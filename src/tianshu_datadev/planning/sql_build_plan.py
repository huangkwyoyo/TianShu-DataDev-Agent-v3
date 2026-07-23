"""SqlBuildPlan——8 Step 类型化 IR + SqlBuildPlanBuilder（确定性）。

每个 step 使用 step_type: Literal["scan"|...] 作为 Pydantic discriminated union 的判别器。
禁止任何自由 SQL 字段（raw_sql / where_sql / join_on: str / expression: str）。

Phase 3B 新增 WindowStep（窗口函数步骤）和 CaseWhenStep 完善。
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, timedelta
from typing import Annotated, Literal, Union

from pydantic import Field

from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
from tianshu_datadev.developer_spec.models import (
    CompareOp,
    DatasetType,
    InputTableDecl,
    LabelAnd,
    LabelCompare,
    LabelDatePartRef,
    LabelIsNotNull,
    LabelIsNull,
    LabelNot,
    LabelOr,
    LabelTypedLiteral,
    OpenQuestion,
    ParsedDeveloperSpec,
    StrictModel,
)

from .models import (
    AggregateSpec,
    AliasExpr,
    ColumnRef,
    DatePartExpression,
    DerivedGroupKey,
    JoinType,
    Predicate,
    PredicateOperator,
    RatioExpr,
    SafeIdentifier,
    SortDirection,
    SortSpec,
    SqlLiteral,
    SqlRawExpression,
    TimeTransformExpr,
    WhenBranch,
    WindowExpr,
    WindowFunction,
)
from .relationship_hypothesis import RelationshipHypothesis
from .temp_table import make_temp_name

# ════════════════════════════════════════════
# 异常类型
# ════════════════════════════════════════════


class DerivedColumnRuleMissingError(Exception):
    """输出列无解析规则异常——禁止未解析输出列回退成物理 ColumnRef。

    触发条件：output_spec 中的列既不是源表物理列，也不是指标/维度/窗口指标输出，
    也不是 label_rules 的 output_column。

    此异常为防御性编程——正常情况下，label_table 预处理应已将所有
    未解析列转换为 label_rules。若此处抛出，说明 Pipeline 门禁未起作用。
    """


# ════════════════════════════════════════════
# 8 Step 类型（strict Pydantic，extra="forbid"）
# ════════════════════════════════════════════


class ScanStep(StrictModel):
    """表扫描步骤——从 SourceManifest 注册的表读取指定列。

    required_columns 不得为空（等于 SELECT *）——强制显式声明所需列。
    partition_filters 用于分区裁剪（如日期分区键）。
    """

    step_type: Literal["scan"] = "scan"
    step_id: str
    table_ref: SafeIdentifier  # SourceManifest 中注册的表引用——SafeIdentifier 防注入
    required_columns: list[ColumnRef]  # 实际需要的列——不得为空
    predicates: list[Predicate] = []  # 扫描阶段可下推的过滤
    partition_filters: list[Predicate] = []  # 分区裁剪过滤
    estimated_row_count: int | None = None  # SourceManifest 提供的近似行数


class FilterStep(StrictModel):
    """过滤步骤——应用 Predicate 条件（等效 WHERE 子句）。"""

    step_type: Literal["filter"] = "filter"
    step_id: str
    predicate: Predicate


class JoinStep(StrictModel):
    """受控 Join 步骤——仅 STRONG/MEDIUM 证据等级的 Join 可进入。

    relationship_ref 指向 RelationshipHypothesis 中的 candidate_id。
    WEAK/NONE Join 被硬门禁拦截，不得到达此步骤。
    """

    step_type: Literal["join"] = "join"
    step_id: str
    right_table_ref: SafeIdentifier  # 被 Join 的右表引用——SafeIdentifier 防注入
    join_type: JoinType = JoinType.INNER
    join_keys: list[tuple[ColumnRef, ColumnRef]] = []  # (left_key, right_key) 对列表
    relationship_ref: str  # 对应 JoinCandidate.candidate_id
    cardinality_hint: str | None = None  # "1:1" | "1:N" | "N:M" | None
    pre_aggregation_allowed: bool = False  # 是否允许 Join 前先聚合大表


class AggregateStep(StrictModel):
    """聚合步骤——GROUP BY + 聚合函数。

    having 为 Predicate 而非字符串，禁止 having_sql。
    """

    step_type: Literal["aggregate"] = "aggregate"
    step_id: str
    group_keys: list[ColumnRef | DatePartExpression | DerivedGroupKey]  # GROUP BY 列或受控派生键
    metrics: list[AggregateSpec]  # 聚合规格
    having: Predicate | None = None  # HAVING 条件（封闭 AST）


class ProjectStep(StrictModel):
    """列投影步骤——选择输出列及其别名。"""

    step_type: Literal["project"] = "project"
    step_id: str
    columns: list[AliasExpr]  # 输出列列表


class CaseWhenStep(StrictModel):
    """CASE WHEN 条件标签步骤——Phase 3B 开放。

    枚举值必须在 DeveloperSpec 中声明，未声明枚举值被拒绝。
    每个 WhenBranch 的 result 值必须来自声明枚举值列表。
    """

    step_type: Literal["case_when"] = "case_when"
    step_id: str
    cases: list[WhenBranch] = []  # CASE WHEN 分支列表
    else_value: SqlLiteral | None = None  # 默认值（ELSE 子句）
    alias: SafeIdentifier = ""  # 输出列别名——SafeIdentifier 防注入（空字符串表示无别名）
    # ── 聚合阶段评估位置（从 CaseWhenDecl.evaluation_phase 传递）──
    evaluation_phase: Literal["pre_aggregate", "post_aggregate"] | None = None


class WindowStep(StrictModel):
    """窗口函数步骤——Phase 3B 新增。

    对当前结果集计算窗口函数，每个 WindowExpr 产生一个带别名的输出列。
    窗口函数白名单（8 种）由 Validator 强制校验。
    """

    step_type: Literal["window"] = "window"
    step_id: str
    window_exprs: list[WindowExpr] = []  # 窗口函数表达式列表


class SortStep(StrictModel):
    """排序步骤——ORDER BY + 可选 LIMIT。

    requires_full_sort 为 True 时表示无 LIMIT 或 LIMIT 极大，
    PerfValidator（Phase 1C）将发出 PERF-005 WARN。
    """

    step_type: Literal["sort"] = "sort"
    step_id: str
    order_by: list[SortSpec]  # 排序列 + 方向
    limit: int | None = None  # 排序后保留行数
    requires_full_sort: bool = False
    estimated_input_rows: int | None = None


class LimitStep(StrictModel):
    """行数限制步骤——LIMIT + OFFSET。"""

    step_type: Literal["limit"] = "limit"
    step_id: str
    limit: int
    offset: int | None = None


class SubqueryStep(StrictModel):
    """子查询步骤——在 FROM 子句中嵌入完整的 SqlBuildPlan（Phase 4.6 Step 2）。

    仅支持 FROM 子句中的派生表子查询，不支持：
    - WHERE 中的关联子查询
    - SELECT 列表中的标量子查询
    - 超过 2 层嵌套

    递归引用 SqlBuildPlan——由 from __future__ import annotations + Pydantic
    ForwardRef 自动解析循环引用。
    """

    step_type: Literal["subquery"] = "subquery"
    step_id: str
    alias: str  # 派生表别名（如 order_agg）
    inner_plan: SqlBuildPlan  # 嵌套的完整 SqlBuildPlan——递归引用
    depth: int = 1  # 嵌套深度（从 1 开始计数，Validator 限制 ≤ 2）


# ════════════════════════════════════════════
# Step 联合类型
# ════════════════════════════════════════════

StepNode = Annotated[
    Union[
        ScanStep,
        FilterStep,
        JoinStep,
        AggregateStep,
        ProjectStep,
        CaseWhenStep,
        WindowStep,
        SortStep,
        LimitStep,
        SubqueryStep,
    ],
    Field(discriminator="step_type"),
]


# ── 日期辅助函数：半开区间日期计算 ──

_YYYYMMDD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_date_only(s: str) -> bool:
    """判断字符串是否为 YYYY-MM-DD 格式（仅日期，无时间组件）。"""
    return bool(_YYYYMMDD_RE.match(s))


def _add_one_day(date_str: str) -> str:
    """对 YYYY-MM-DD 格式日期加一天，返回 ISO 格式字符串。

    在 SqlBuildPlan 构建阶段确定性计算 end_plus_one_day，
    Compiler/Mapper 只渲染 IR，不做边界修正。

    Args:
        date_str: YYYY-MM-DD 格式的日期字符串

    Returns:
        end_plus_one_day 的 ISO 格式字符串（如 "2026-04-01"）

    Raises:
        ValueError: date_str 不是合法的 YYYY-MM-DD 日期
    """
    d = date.fromisoformat(date_str)
    next_day = d + timedelta(days=1)
    return next_day.isoformat()


# ════════════════════════════════════════════
# SqlBuildPlan
# ════════════════════════════════════════════


class SqlBuildPlan(StrictModel):
    """类型安全的 SQL 构建计划——9 Step IR 的有序序列（Phase 4.6 新增 SubqueryStep）。

    steps 的顺序即执行顺序。允许步骤重复（如多个 ScanStep、多个 FilterStep）。
    hypothesis_id 溯源到 RelationshipHypothesis，source_manifest_hash 溯源到 SourceManifest。
    """

    plan_id: str
    spec_hash: str  # 对应 ParsedDeveloperSpec.spec_hash
    hypothesis_id: str | None = None  # 对应 RelationshipHypothesis.hypothesis_id
    source_manifest_hash: str | None = None  # 对应 SourceManifest 的 hash
    steps: list[StepNode] = []  # 有序 step 列表，至少一个
    multi_table: bool = False

    # ── 确定性 ID 生成 ──

    @staticmethod
    def generate_plan_id(spec_hash: str) -> str:
        """基于 spec_hash 的确定性 plan ID。"""
        return f"plan_{spec_hash[:12]}"

    @staticmethod
    def generate_step_id(step_type: str, content: dict) -> str:
        """基于步骤内容的确定性 step ID。

        Args:
            step_type: 步骤类型（scan/filter/join/aggregate/project/case_when/sort/limit）
            content: 步骤的核心字段 dict（用于 hash）

        Returns:
            "step_{type}_{hash前8位}"
        """
        content_str = json.dumps(content, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content_str.encode()).hexdigest()[:8]
        return f"step_{step_type}_{hash_hex}"

    @staticmethod
    def generate_plan_hash(plan: SqlBuildPlan) -> str:
        """计算 SqlBuildPlan 的确定性 hash——用于 Phase 1C Compiler 确定性验证。

        排除 open_questions 等非结构性字段，仅计算 steps 部分。
        """
        # 只 hash 步骤部分（结构字段），排除副作用
        steps_data = []
        for step in plan.steps:
            step_dict = step.model_dump(exclude={"step_id"}, exclude_none=True)
            steps_data.append(step_dict)
        content = json.dumps(steps_data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:16]


# ════════════════════════════════════════════
# SqlBuildPlanBuilder（确定性）
# ════════════════════════════════════════════


class SqlBuildPlanBuilder:
    """Phase 1B 确定性 SqlBuildPlan 构建器。

    构建策略：
    - 单表：Scan → (Filter*) → (Aggregate) → Project → (Sort) → (Limit)
    - 两表 Join：Scan(L) → Scan(R) → (Filter*) → Join → (Aggregate) → Project → (Sort) → (Limit)
    build_from_steps() 按 compute_steps 拆为多个聚合 Plan，通过 _temp 表串联。
    - build() 返回单个 SqlBuildPlan（单步场景），build_multi()/build_from_steps() 返回列表（多步场景）。
    - WEAK/NONE Join 被硬门禁拦截在上层，不会到达此 Builder
    """

    def __init__(self, normalizer: FieldNormalizer | None = None):
        """初始化 Builder。

        Args:
            normalizer: 字段名归一化器，用于 ColumnRef.normalized_name 填充
        """
        self._normalizer = normalizer or FieldNormalizer()

    @staticmethod
    def _has_self_join(hypothesis: RelationshipHypothesis | None) -> bool:
        """检测是否存在自引用 Join——同一张表连接自身。

        Args:
            hypothesis: Join 推测（可含多个候选）

        Returns:
            True 如果至少一个候选的 left_table == right_table
        """
        if hypothesis is None:
            return False
        return any(
            c.left_table == c.right_table
            for c in hypothesis.candidates
        )

    def build(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis | None = None,
    ) -> tuple[SqlBuildPlan, list[OpenQuestion]]:
        """基于 ParsedDeveloperSpec + RelationshipHypothesis 构建 SqlBuildPlan。

        Args:
            spec: 已解析的 DeveloperSpec
            hypothesis: Join 推测（多表时必须提供，且仅含 STRONG/MEDIUM 候选）

        Returns:
            (SqlBuildPlan, list[OpenQuestion])

        Raises:
            ValueError: 多表但无 hypothesis 或 hypothesis.spec_hash 不匹配
        """
        is_multi = len(spec.input_tables) > 1
        is_self_join = self._has_self_join(hypothesis)

        # 校验 hypothesis
        if is_multi and hypothesis is None:
            raise ValueError("多表 spec 必须提供 RelationshipHypothesis")
        if hypothesis and hypothesis.spec_hash != spec.spec_hash:
            raise ValueError("hypothesis.spec_hash 与 spec.spec_hash 不匹配")

        if not is_multi and not is_self_join:
            steps = self._build_single_table(spec)
        else:
            steps = self._build_multi_table(spec, hypothesis)  # type: ignore[arg-type]

        plan = SqlBuildPlan(
            plan_id=SqlBuildPlan.generate_plan_id(spec.spec_hash),
            spec_hash=spec.spec_hash,
            hypothesis_id=hypothesis.hypothesis_id if hypothesis else None,
            source_manifest_hash=hypothesis.source_manifest_hash if hypothesis else None,
            steps=steps,
            multi_table=is_multi or is_self_join,
        )

        return plan, []

    def build_from_steps(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis | None = None,
    ) -> list[SqlBuildPlan]:
        """按 compute_steps 声明构建多步聚合 SqlBuildPlan 链。

        每个 ComputeStep 产生一个 SqlBuildPlan——中间步骤输出 _temp 表，
        后续步骤从 _temp 表读取。最终步骤使用 spec.output_spec。

        Args:
            spec: 已解析的 DeveloperSpec（compute_steps 必须非空）
            hypothesis: 可选的 Join 推测

        Returns:
            按依赖顺序排列的 SqlBuildPlan 列表

        Raises:
            ValueError: compute_steps 为空
        """
        if not spec.compute_steps or len(spec.compute_steps) == 0:
            raise ValueError("compute_steps 为空——应使用 build() 而非 build_from_steps()")

        return self._build_plans_from_compute_steps(spec, hypothesis)

    def _build_plans_from_compute_steps(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis | None = None,
    ) -> list[SqlBuildPlan]:
        """从 compute_steps 构建 SqlBuildPlan 列表——支持线性链和分支 DAG。

        DAG 拓扑排序后逐步骤构建：
        - source="input" → 叶节点 Plan（从源表扫描+聚合）
        - source=单个 step_name → 线性 Plan（从上游 _temp 扫描+聚合）
        - source=[a, b] → 合流 Plan（多 _temp 扫描+Join+聚合）

        Join 键从 spec.joins 中查找——JoinDecl 的 left_table/right_table
        匹配 source 列表中的 step_name 时，使用其 left_key/right_key。
        """
        steps = spec.compute_steps
        chain_id = hashlib.md5(
            "|".join(s.step_name for s in steps).encode()
        ).hexdigest()[:8]

        # ── 1. 拓扑排序 ──
        sorted_steps = self._topo_sort_compute_steps(steps)

        # 构建 JoinDecl 查找表：按 (left, right) 键查找 join keys
        join_key_map: dict[tuple[str, str], tuple[str, str]] = {}
        if spec.joins:
            for j in spec.joins:
                join_key_map[(j.left_table, j.right_table)] = (j.left_key, j.right_key)
                join_key_map[(j.right_table, j.left_table)] = (j.right_key, j.left_key)

        # 记录已产出步骤的 output_alias → 列映射（供下游查找列）
        step_outputs: dict[str, list[ColumnRef]] = {}
        # step_name → SqlBuildPlan（用于按原始顺序返回）
        step_plan_map: dict[str, SqlBuildPlan] = {}
        # step_name → plan_id（供外部查询依赖关系）
        self._step_plan_ids: dict[str, str] = {}

        for idx, cs in enumerate(sorted_steps):
            is_final = (idx == len(sorted_steps) - 1)
            plan_steps: list[StepNode] = []

            # 归一化 source 为列表
            sources: list[str] = (
                cs.source if isinstance(cs.source, list) else [cs.source]
            )

            if len(sources) == 1 and sources[0] == "input":
                if cs.joins:
                    # ── 多源 Join 场景：source="input" + joins → 需 Join 多张源表后再聚合 ──
                    table_map = {t.table_alias: t for t in spec.input_tables}
                    for j_idx, jd in enumerate(cs.joins):
                        left_table = table_map.get(jd.left_table)
                        right_table = table_map.get(jd.right_table)
                        if not left_table or not right_table:
                            raise ValueError(
                                f"compute_step '{cs.step_name}' 的 Join 声明引用了不存在的表: "
                                f"left='{jd.left_table}', right='{jd.right_table}'"
                            )

                        if j_idx == 0:
                            # 第一对 Join：扫描左右表
                            left_cols = self._build_columns_for_input_step_table(
                                cs, left_table, extra_cols=[jd.left_key]
                            )
                            left_scan = ScanStep(
                                step_id=SqlBuildPlan.generate_step_id(
                                    "scan", {"step": cs.step_name, "table": left_table.source_table}
                                ),
                                table_ref=left_table.table_alias,
                                required_columns=left_cols,
                                estimated_row_count=left_table.row_count,
                            )
                            plan_steps.append(left_scan)
                            for f in left_table.filters:
                                plan_steps.append(self._build_filter_step(
                                    f, left_table.table_alias
                                ))

                        right_cols = self._build_columns_for_input_step_table(
                            cs, right_table, extra_cols=[jd.right_key]
                        )
                        right_scan = ScanStep(
                            step_id=SqlBuildPlan.generate_step_id(
                                "scan_r", {"step": cs.step_name, "table": right_table.source_table}
                            ),
                            table_ref=right_table.table_alias,
                            required_columns=right_cols,
                            estimated_row_count=right_table.row_count,
                        )
                        plan_steps.append(right_scan)
                        for f in right_table.filters:
                            plan_steps.append(self._build_filter_step(
                                f, right_table.table_alias
                            ))

                        # JoinStep——JoinType 枚举值兼容（INNER/LEFT/RIGHT 共用 .value）
                        join_step = JoinStep(
                            step_id=SqlBuildPlan.generate_step_id("join", {
                                "step": cs.step_name,
                                "left": jd.left_table,
                                "right": jd.right_table,
                                "left_key": jd.left_key,
                                "right_key": jd.right_key,
                            }),
                            right_table_ref=right_table.table_alias,
                            join_type=JoinType(jd.join_type.value.upper()),
                            join_keys=[(
                                ColumnRef(
                                    table_ref=left_table.table_alias,
                                    column_name=jd.left_key,
                                    normalized_name=self._normalizer.normalize(jd.left_key),
                                ),
                                ColumnRef(
                                    table_ref=right_table.table_alias,
                                    column_name=jd.right_key,
                                    normalized_name=self._normalizer.normalize(jd.right_key),
                                ),
                            )],
                            relationship_ref=(
                                f"compute_steps:{cs.step_name}:"
                                f"{jd.left_table}:{jd.right_table}"
                            ),
                        )
                        plan_steps.append(join_step)
                else:
                    # ── 单表扫描（原有逻辑）──
                    table = self._match_table_for_compute_step(cs, spec.input_tables)
                    scan_cols = self._build_required_columns_from_compute_step(
                        cs, table.table_alias
                    )
                    scan = ScanStep(
                        step_id=SqlBuildPlan.generate_step_id(
                            "scan", {"step": cs.step_name, "table": table.source_table}
                        ),
                        table_ref=table.table_alias,
                        required_columns=scan_cols,
                        estimated_row_count=table.row_count,
                    )
                    plan_steps.append(scan)
                    # 源表预过滤
                    for f in table.filters:
                        plan_steps.append(self._build_filter_step(f, table.table_alias))

            elif len(sources) == 1:
                # ── 线性步骤：从单个上游 _temp 表扫描 ──
                src = sources[0]
                temp_ref = make_temp_name(chain_id, src)
                upstream_cols = step_outputs.get(src, [])
                up_col_map = {c.normalized_name for c in upstream_cols}
                scan_cols: list[ColumnRef] = []
                for gb in cs.group_by:
                    gb_norm = self._normalizer.normalize(gb)
                    if gb_norm in up_col_map:
                        scan_cols.append(ColumnRef(
                            table_ref=temp_ref, column_name=gb,
                            normalized_name=gb_norm,
                        ))
                for m in cs.metrics:
                    if m.input_column:
                        m_norm = self._normalizer.normalize(m.input_column)
                        if m_norm in up_col_map:
                            scan_cols.append(ColumnRef(
                                table_ref=temp_ref, column_name=m.input_column,
                                normalized_name=m_norm,
                            ))
                if not scan_cols:
                    scan_cols = [
                        ColumnRef(
                            table_ref=temp_ref,
                            column_name=c.column_name,
                            normalized_name=c.normalized_name,
                        )
                        for c in upstream_cols
                    ]
                scan = ScanStep(
                    step_id=SqlBuildPlan.generate_step_id(
                        "scan", {"step": cs.step_name, "temp": temp_ref}
                    ),
                    table_ref=temp_ref,
                    required_columns=scan_cols,
                )
                plan_steps.append(scan)

            else:
                # ── 合流步骤：从多个上游 _temp 表扫描 + Join ──
                temp_refs: list[str] = []
                for src in sources:
                    temp_ref = make_temp_name(chain_id, src)
                    temp_refs.append(temp_ref)
                    upstream_cols = step_outputs.get(src, [])
                    # 扫描上游全部列
                    scan_cols = [
                        ColumnRef(
                            table_ref=temp_ref,
                            column_name=c.column_name,
                            normalized_name=c.normalized_name,
                        )
                        for c in upstream_cols
                    ]
                    scan = ScanStep(
                        step_id=SqlBuildPlan.generate_step_id(
                            "scan", {"step": cs.step_name, "temp": temp_ref}
                        ),
                        table_ref=temp_ref,
                        required_columns=scan_cols,
                    )
                    plan_steps.append(scan)

                # 为每对相邻 source 构建 JoinStep
                # 策略：source[0] ⋈ source[1], 结果 ⋈ source[2], ...
                accumulated_ref = temp_refs[0]
                for j_idx in range(1, len(temp_refs)):
                    right_ref = temp_refs[j_idx]
                    left_src = sources[j_idx - 1]
                    right_src = sources[j_idx]

                    # 从 JoinDecl 查找 join keys
                    left_key_raw, right_key_raw = self._find_join_keys(
                        join_key_map, sources, left_src, right_src, step_outputs
                    )

                    # 空键 → CROSS JOIN（跨粒度场景：一侧有 GROUP BY，一侧无）
                    if not left_key_raw and not right_key_raw:
                        join_step = JoinStep(
                            step_id=SqlBuildPlan.generate_step_id("join", {
                                "step": cs.step_name,
                                "left_src": left_src,
                                "right_src": right_src,
                                "type": "CROSS",
                            }),
                            right_table_ref=right_ref,
                            join_type=JoinType.CROSS,
                            join_keys=[],  # CROSS JOIN 无等值键
                            relationship_ref=f"compute_steps:{chain_id}:{left_src}:{right_src}",
                        )
                    else:
                        left_key_norm = self._normalizer.normalize(left_key_raw)
                        right_key_norm = self._normalizer.normalize(right_key_raw)

                        join_step = JoinStep(
                            step_id=SqlBuildPlan.generate_step_id("join", {
                                "step": cs.step_name,
                                "left_src": left_src,
                                "right_src": right_src,
                                "left_key": left_key_raw,
                                "right_key": right_key_raw,
                            }),
                            right_table_ref=right_ref,
                            join_type=JoinType.INNER,
                            join_keys=[(
                                ColumnRef(
                                    table_ref=accumulated_ref,
                                    column_name=left_key_raw,
                                    normalized_name=left_key_norm,
                                ),
                                ColumnRef(
                                    table_ref=right_ref,
                                    column_name=right_key_raw,
                                    normalized_name=right_key_norm,
                                ),
                            )],
                            relationship_ref=f"compute_steps:{chain_id}:{left_src}:{right_src}",
                        )
                    plan_steps.append(join_step)
                    # 累积引用——Join 后左侧视为累积结果
                    # 后续 Join 的 left 侧使用 accumulated_ref

            # ── CaseWhenStep（Phase 6：仅合流步骤 + case_when 声明时）──
            if cs.case_when and cs.case_when.branches:
                # 合流步骤：用第一个源的 _temp 表名作为条件列的 table_ref
                cw_table_ref = ""
                if isinstance(cs.source, list) and len(cs.source) > 0:
                    cw_table_ref = make_temp_name(chain_id, cs.source[0])
                case_step = self._build_case_when_from_decl(
                    cs.case_when, cs.step_name, chain_id, source_table=cw_table_ref,
                )
                plan_steps.append(case_step)

            # ── AggregateStep ──
            # 合流步骤有 case_when 时跳过聚合——CASE WHEN 已替代聚合逻辑
            if cs.metrics and not cs.case_when:
                # 合流步骤（source 为列表）：GROUP BY 各列可能来自不同上游源，
                # 需要按列消歧——仅对重叠列（多个源都有）加表前缀
                if isinstance(cs.source, list) and len(cs.source) > 0:
                    agg = self._build_aggregate_from_compute_step(cs, source_table="")
                    # 构建列名→上游源列表映射
                    col_sources: dict[str, list[str]] = {}
                    for src in sources:
                        for uc in step_outputs.get(src, []):
                            name = uc.column_name
                            if name not in col_sources:
                                col_sources[name] = []
                            col_sources[name].append(src)
                    # 逐列修正 GROUP BY 表前缀——仅重叠列需要消歧
                    def _rebuild_group_key(gk):
                        """修正 GROUP BY 列的表前缀——仅 ColumnRef 需要重映射。"""
                        if isinstance(gk, DerivedGroupKey):
                            return gk  # 派生键无 table_ref，直接透传
                        # ColumnRef 或 DatePartExpression——两者都有 column_name/normalized_name
                        src = gk.column_name
                        return ColumnRef(
                            table_ref=(
                                make_temp_name(chain_id, col_sources[src][0])
                                if len(col_sources.get(src, [])) > 1
                                else ""
                            ),
                            column_name=src,
                            normalized_name=gk.normalized_name,
                        )
                    agg.group_keys = [_rebuild_group_key(gk) for gk in agg.group_keys]
                else:
                    st = "" if isinstance(cs.source, list) else (
                        cs.source if cs.source != "input" else ""
                    )
                    agg = self._build_aggregate_from_compute_step(cs, source_table=st)
                plan_steps.append(agg)

            # ── WindowStep（仅最终步骤，聚合后、投影前）──
            window = None
            if is_final:
                window = self._build_window_step(spec)
                if window:
                    plan_steps.append(window)
                    plan_steps.extend(self._build_post_window_filter_steps(spec))

            # ── ProjectStep ──
            if is_final:
                # 合流步骤：使用第一个源的 _temp 表别名消除列歧义
                proj_table_ref = ""
                if isinstance(cs.source, list) and len(cs.source) > 0:
                    proj_table_ref = make_temp_name(chain_id, cs.source[0])
                project = self._build_project_step(spec, default_table_ref=proj_table_ref)
                # 排除 CaseWhenStep 已产出的列——避免 SELECT 中重复
                if (
                    cs.case_when
                    and cs.case_when.output_column
                    and not spec.ratio_metrics
                ):
                    cw_output = cs.case_when.output_column
                    filtered_cols = [
                        c for c in project.columns
                        if c.alias != cw_output
                    ]
                    project = ProjectStep(
                        step_id=project.step_id,
                        columns=filtered_cols,
                    )
                # 排除 WindowStep 已产出的列——避免 SELECT 中重复
                if window and not spec.ratio_metrics:
                    win_aliases = {str(w.alias) for w in window.window_exprs if w.alias}
                    filtered_cols = [
                        c for c in project.columns
                        if c.alias not in win_aliases
                    ]
                    project = ProjectStep(
                        step_id=project.step_id,
                        columns=filtered_cols,
                    )
                plan_steps.append(project)
            else:
                if not cs.metrics and not cs.case_when:
                    # ── 无聚合步骤（透传导流）：透传所有上游列 ──
                    has_upstream = not (len(sources) == 1 and sources[0] == "input")
                    if has_upstream:
                        # 构建列名→源列表映射，用于检测重叠列（如 borough 存在于多个上游）
                        col_sources: dict[str, list[str]] = {}
                        for src in sources:
                            for uc in step_outputs.get(src, []):
                                name = uc.column_name
                                if name not in col_sources:
                                    col_sources[name] = []
                                col_sources[name].append(src)

                        proj_cols = []
                        seen: set[str] = set()
                        for src in sources:
                            temp_ref = make_temp_name(chain_id, src)
                            for uc in step_outputs.get(src, []):
                                name = uc.column_name
                                normalized = uc.normalized_name or self._normalizer.normalize(name)
                                if normalized not in seen:
                                    seen.add(normalized)
                                    # 重叠列（多个源都有）：用第一个源的 temp_ref 消歧
                                    # 唯一列：不设 table_ref（DuckDB 自动解析）
                                    src_list = col_sources.get(name, [src])
                                    use_ref = (
                                        make_temp_name(chain_id, src_list[0])
                                        if len(src_list) > 1 else ""
                                    )
                                    proj_cols.append(AliasExpr(
                                        expression=ColumnRef(
                                            table_ref=use_ref,
                                            column_name=name,
                                            normalized_name=normalized,
                                        ),
                                        alias=name,
                                    ))
                    else:
                        # source="input" 且无聚合——使用标准透传（仅 group_by）
                        proj_cols = self._build_compute_step_passthrough(cs)
                else:
                    # 有聚合（metrics 或 case_when）：标准透传（group_by + metric aliases）
                    # 合流场景下不设 table_ref——列消歧已在 AggregateStep/CaseWhenStep 的
                    # group_keys 处理中完成，此处的 ProjectStep 读取聚合结果表而非上游临时表
                    proj_cols = self._build_compute_step_passthrough(cs)

                # ── 派生表达式列（如 crash_per_million_trips = total_crashes * 1e6 / total_trip_count）──
                if cs.expressions:
                    for expr in cs.expressions:
                        proj_cols.append(AliasExpr(
                            expression=SqlRawExpression(sql_fragment=expr.expression),
                            alias=SafeIdentifier(expr.name),
                        ))

                plan_steps.append(ProjectStep(
                    step_id=SqlBuildPlan.generate_step_id(
                        "project", {"step": cs.step_name, "intermediate": True}
                    ),
                    columns=proj_cols,
                ))
                # 记录此步骤的产出列供下游 lookup
                step_outputs[cs.step_name] = [
                    ColumnRef(
                        table_ref="",
                        column_name=pc.alias,
                        normalized_name=self._normalizer.normalize(pc.alias),
                    )
                    for pc in proj_cols
                ]

            # ── SortStep + LimitStep（仅最终步骤）──
            if is_final:
                if spec.output_spec.sort:
                    plan_steps.append(self._build_sort_step(spec))
                if spec.output_spec.limit is not None:
                    plan_steps.append(LimitStep(
                        step_id=SqlBuildPlan.generate_step_id(
                            "limit", {"limit": spec.output_spec.limit}
                        ),
                        limit=spec.output_spec.limit,
                    ))

            # ── 组装 Plan ──
            # 使用 step_name 的确定性排序位置（而非拓扑序号）保证 plan_id 确定性
            orig_pos = next(
                i for i, s in enumerate(steps) if s.step_name == cs.step_name
            )
            plan_id = f"plan_{spec.spec_hash[:12]}_{chain_id}_{orig_pos}"
            plan = SqlBuildPlan(
                plan_id=plan_id,
                spec_hash=spec.spec_hash,
                hypothesis_id=hypothesis.hypothesis_id if hypothesis else None,
                source_manifest_hash=hypothesis.source_manifest_hash if hypothesis else None,
                steps=plan_steps,
                multi_table=isinstance(cs.source, list),
            )
            step_plan_map[cs.step_name] = plan
            self._step_plan_ids[cs.step_name] = plan_id

        # 按原始声明顺序返回 plans——Pipeline 通过索引与 compute_steps 配对
        return [step_plan_map[s.step_name] for s in steps]

    @staticmethod
    def _topo_sort_compute_steps(steps) -> list:
        """对 compute_steps 进行 Kahn 拓扑排序——支持多源 DAG。

        source="input" 的步骤入度为 0（根节点），
        source 为 step_name 的步骤依赖上游，
        source 为列表的步骤依赖所有列表中的上游步骤。
        """
        name_to_idx = {s.step_name: i for i, s in enumerate(steps)}
        in_degree: dict[str, int] = {s.step_name: 0 for s in steps}
        dependents: dict[str, list[str]] = {s.step_name: [] for s in steps}

        for s in steps:
            src_list = s.source if isinstance(s.source, list) else [s.source]
            for src in src_list:
                if src != "input" and src in name_to_idx:
                    in_degree[s.step_name] += 1
                    dependents[src].append(s.step_name)

        import heapq
        heap = [n for n, d in in_degree.items() if d == 0]
        heapq.heapify(heap)
        result: list = []
        while heap:
            current = heapq.heappop(heap)
            result.append(name_to_idx[current])
            for dep in dependents.get(current, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    heapq.heappush(heap, dep)

        # 按拓扑顺序返回 ComputeStep 对象
        return [steps[i] for i in result]

    @staticmethod
    def _find_join_keys(
        join_key_map: dict,
        sources: list[str],
        left_src: str,
        right_src: str,
        step_outputs: dict,
    ) -> tuple[str, str]:
        """查找两个源步骤之间的 Join 键。

        优先级：
        1. spec.joins 中显式声明的 JoinDecl（left_table/right_table 匹配 step_name）
           - JoinDecl 的 key 为空字符串时 → 返回 ("", "") 表示 CROSS JOIN
        2. 两个上游步骤的共同 group_by 列（兜底自动推断）
        3. 无共同列 → 返回 ("", "") 表示 CROSS JOIN（跨粒度场景）

        Returns:
            (left_key, right_key) 列名对——均为 "" 时表示 CROSS JOIN
        """
        # 1. 从 JoinDecl 查找
        key = (left_src, right_src)
        if key in join_key_map:
            jk = join_key_map[key]
            # JoinDecl 显式声明空键 → CROSS JOIN
            return jk

        # 2. 兜底：共同 group_by 列自动推断
        left_cols = {c.normalized_name for c in step_outputs.get(left_src, [])}
        right_cols = {c.normalized_name for c in step_outputs.get(right_src, [])}
        common = left_cols & right_cols
        if common:
            col = sorted(common)[0]
            return col, col

        # 3. 无共同列 → CROSS JOIN（跨粒度场景：一侧有 GROUP BY，一侧无）
        return "", ""

    def _expand_metric_to_agg_specs(
        self, m, source_table: str = "",  # MetricDecl
    ) -> list[AggregateSpec]:
        """将一个 MetricDecl（含可选 variants）展开为多个 AggregateSpec。

        基础指标始终产生一个 AggregateSpec（使用 MetricDecl 自身的 alias/filter）。
        每个 MetricVariant 额外产生一个 AggregateSpec（使用 variant 的 alias/filter），
        共享同一基础聚合逻辑（aggregation + input_column）。

        Args:
            m: MetricDecl 实例
            source_table: 源表别名——自引用/多表场景消除列歧义

        Returns:
            展开后的 AggregateSpec 列表——长度 ≥ 1
        """
        specs: list[AggregateSpec] = []
        st = source_table if source_table else None  # 空字符串 → None

        # 规范化 input_column——去除表别名前缀（如 cp.collision_id → collision_id）
        # Planner 有时会在 input_column 前加表别名，但 AggregateSpec.input_column
        # 为 SafeIdentifier（不允许点号），表上下文应由 source_table 字段承载。
        input_col = m.input_column
        if input_col and "." in input_col:
            input_col = input_col.split(".")[-1]

        # 基础指标
        specs.append(AggregateSpec(
            aggregation=m.aggregation,
            input_column=input_col,
            alias=m.alias,
            filter=m.filter,
            input_expression=m.input_expression,
            distinct=m.distinct,
            source_table=st,
        ))

        # 变体——共享基础聚合逻辑，仅替换 filter + alias
        if m.variants:
            for v in m.variants:
                specs.append(AggregateSpec(
                    aggregation=m.aggregation,
                    input_column=input_col,
                    alias=v.alias,
                    filter=v.filter,
                    input_expression=m.input_expression,
                    distinct=m.distinct,
                    source_table=st,
                ))

        return specs

    @staticmethod
    def _match_table_for_compute_step(
        cs,  # ComputeStep
        input_tables: list,
    ) -> InputTableDecl:
        """多表 spec 中，为 compute step 匹配合适的源表。

        根据 step 的 group_by + metric input_column 与各表的声明列名匹配度，
        选择最佳匹配表。当只有一张表时直接返回。
        t.columns 始终为空（仅 key_columns + business_columns 被填充），
        因此不参与匹配。

        Args:
            cs: ComputeStep 实例
            input_tables: 所有输入表列表

        Returns:
            匹配的 InputTableDecl 实例
        """
        if len(input_tables) == 1:
            return input_tables[0]

        # 收集 step 需要的所有列名
        step_cols: set[str] = set(cs.group_by or [])
        for m in cs.metrics:
            if m.input_column:
                step_cols.add(m.input_column)

        if not step_cols:
            return input_tables[0]

        # 按列名匹配度打分——仅使用 key_columns + business_columns
        # t.columns 始终为 []，不参与匹配
        best_table: InputTableDecl = input_tables[0]
        best_score = -1
        for t in input_tables:
            table_cols: set[str] = set()
            for c in t.key_columns:
                table_cols.add(c.column_name)
            for c in t.business_columns:
                table_cols.add(c.column_name)
            score = len(step_cols & table_cols)
            if score > best_score:
                best_score = score
                best_table = t
        return best_table

    def _build_required_columns_from_compute_step(
        self,
        cs,  # ComputeStep
        table_alias: str,
    ) -> list[ColumnRef]:
        """从 ComputeStep 的 metrics + group_by 推断需要的源表列。"""
        seen: set[str] = set()
        cols: list[ColumnRef] = []

        def _add(col_name: str) -> None:
            normalized = self._normalizer.normalize(col_name)
            if normalized not in seen:
                seen.add(normalized)
                cols.append(ColumnRef(
                    table_ref=table_alias,
                    column_name=col_name,
                    normalized_name=normalized,
                ))

        for m in cs.metrics:
            if m.input_column:
                _add(m.input_column)
        for gb in cs.group_by:
            _add(gb)

        return cols

    def _build_columns_for_input_step_table(
        self, cs, table: InputTableDecl, extra_cols: list[str] | None = None,
    ) -> list[ColumnRef]:
        """为 source="input" + joins 场景构建单源表的扫描列。

        从 compute_step 的 group_by + metrics 中筛选出该表拥有的列，
        外加额外的列（如 Join 键）。仅包含 key_columns + business_columns 中声明的列。

        Args:
            cs: ComputeStep 实例
            table: 目标 InputTableDecl
            extra_cols: 额外需要的列名列表（如 Join 键）

        Returns:
            该表需要扫描的 ColumnRef 列表
        """
        # 该表所有已声明列名的归一化集合
        declared: set[str] = set()
        for c in table.key_columns:
            declared.add(c.normalized_name)
        for c in table.business_columns:
            declared.add(c.normalized_name)

        # 从 group_by + metrics 收集所有需要的列
        needed: set[str] = set(cs.group_by or [])
        for m in cs.metrics:
            if m.input_column:
                needed.add(m.input_column)
        if extra_cols:
            for c in extra_cols:
                if c:
                    needed.add(c)

        # 只保留该表拥有的列——用 sorted 保证确定性
        cols: list[ColumnRef] = []
        seen: set[str] = set()
        for col_name in sorted(needed):
            normalized = self._normalizer.normalize(col_name)
            if normalized in declared and normalized not in seen:
                seen.add(normalized)
                cols.append(ColumnRef(
                    table_ref=table.table_alias,
                    column_name=col_name,
                    normalized_name=normalized,
                ))
        return cols

    def _build_aggregate_from_compute_step(self, cs, source_table: str = "") -> AggregateStep:  # ComputeStep
        """从 ComputeStep 构建 AggregateStep。

        Args:
            cs: ComputeStep 实例
            source_table: 源表引用——用于 group_keys 的 table_ref。
                          空字符串表示当前上下文（Join 后或源表扫描后）。
        """
        # 确定 source_table：标量非 input → 用 source 值，列表或 input → 空字符串
        if not source_table:
            if isinstance(cs.source, str) and cs.source != "input":
                source_table = cs.source
        group_cols: list[ColumnRef] = []
        for gb in cs.group_by:
            normalized = self._normalizer.normalize(gb)
            group_cols.append(ColumnRef(
                table_ref=source_table,
                column_name=gb,
                normalized_name=normalized,
            ))

        agg_metrics: list[AggregateSpec] = []
        for m in cs.metrics:
            agg_metrics.extend(self._expand_metric_to_agg_specs(
                m, source_table=source_table,
            ))

        step_id_content = {
            "step": cs.step_name,
            "groups": [g.normalized_name if isinstance(g, ColumnRef) else g.alias for g in group_cols],
            "metrics": [m.alias for m in agg_metrics],
        }
        return AggregateStep(
            step_id=SqlBuildPlan.generate_step_id("aggregate", step_id_content),
            group_keys=group_cols,
            metrics=agg_metrics,
        )

    def _build_case_when_from_decl(
        self,
        case_when,  # CaseWhenDecl
        step_name: str,
        chain_id: str,
        source_table: str = "",  # 合流步骤第一个源的 _temp 表名——消除列歧义
    ) -> CaseWhenStep:
        """从 CaseWhenDecl 构建 CaseWhenStep——将字符串条件转为类型化 Predicate。

        每个 CaseWhenBranchDecl 映射为一个 WhenBranch（condition=Predicate, result=SqlLiteral）。
        source_table 用于消除 Join 后的列歧义——条件列引用左侧 _temp 表的列。

        Args:
            case_when: CaseWhenDecl 声明
            step_name: 所属 ComputeStep 的 step_name
            chain_id: 当前链的确定性 hash ID
            source_table: 左表别名——合流步骤第一个源的 _temp 表引用

        Returns:
            CaseWhenStep IR 节点
        """
        operator_map: dict[str, PredicateOperator] = {
            "=": PredicateOperator.EQ,
            "!=": PredicateOperator.NEQ,
            ">": PredicateOperator.GT,
            "<": PredicateOperator.LT,
            ">=": PredicateOperator.GTE,
            "<=": PredicateOperator.LTE,
            "IN": PredicateOperator.IN,
        }

        branches: list[WhenBranch] = []
        # 用于 step_id 内容的摘要（字符串模式用 when/then，类型化模式用 col/op/val/then）
        branch_summaries: list[dict] = []
        for b in case_when.branches:
            if b.when is not None and b.then is not None:
                # ── 字符串模式：复杂布尔表达式（如 crash_per_million_trips >= 800 OR ...）──
                raw_cond = SqlRawExpression(sql_fragment=b.when)
                branches.append(WhenBranch(
                    raw_condition=raw_cond,
                    result=SqlLiteral(value=b.then),
                ))
                branch_summaries.append({"when": b.when[:60], "then": b.then})
            elif b.condition_column is not None:
                # ── 类型化模式：单列简单比较（如 status = 'VIP'）──
                col_norm = self._normalizer.normalize(b.condition_column)
                op = operator_map.get(b.condition_operator, PredicateOperator.EQ)

                condition = Predicate(
                    left=ColumnRef(
                        table_ref=source_table,  # 合流步骤用左表别名消除列歧义
                        column_name=b.condition_column,
                        normalized_name=col_norm,
                    ),
                    operator=op,
                    right=SqlLiteral(value=b.condition_value),
                )
                branches.append(WhenBranch(
                    condition=condition,
                    result=SqlLiteral(value=b.result_column, is_sql_expr=True),
                ))
                branch_summaries.append({
                    "col": b.condition_column, "op": b.condition_operator,
                    "val": b.condition_value, "then": b.result_column,
                })
            # 忽略两种模式都不匹配的分支（防御性处理）

        # ELSE 默认值
        else_val = None
        if case_when.else_value is not None:
            else_val = SqlLiteral(value=case_when.else_value)

        step_id_content = {
            "step": step_name,
            "branches": branch_summaries,
            "else": case_when.else_value,
            "chain": chain_id,
        }
        return CaseWhenStep(
            step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
            cases=branches,
            else_value=else_val,
            alias=case_when.output_column,
        )

    def _build_compute_step_passthrough(
        self, cs, default_table_ref: str = ""
    ) -> list[AliasExpr]:  # ComputeStep
        """为中间 ComputeStep 构建透传投影——输出 GROUP BY 键 + 所有指标列。

        default_table_ref 用于合流步骤（多源 merge）时消除列歧义。

        下游步骤可通过列名引用这些列作为 input_column 或 group_by。
        """
        proj_cols: list[AliasExpr] = []
        seen: set[str] = set()

        # GROUP BY 键
        for gb in cs.group_by:
            gb_norm = self._normalizer.normalize(gb)
            if gb_norm not in seen:
                seen.add(gb_norm)
                proj_cols.append(AliasExpr(
                    expression=ColumnRef(
                        table_ref=default_table_ref,
                        column_name=gb,
                        normalized_name=gb_norm,
                    ),
                    alias=gb,
                ))

        # 指标
        for m in cs.metrics:
            alias = m.alias or m.metric_name
            alias_norm = self._normalizer.normalize(alias)
            if alias_norm not in seen:
                seen.add(alias_norm)
                proj_cols.append(AliasExpr(
                    expression=ColumnRef(
                        table_ref=default_table_ref,
                        column_name=alias,
                        normalized_name=alias_norm,
                    ),
                    alias=alias,
                ))

        # ── Phase 6：CASE WHEN 产出列 ──
        if cs.case_when and cs.case_when.output_column:
            cw_col = cs.case_when.output_column
            cw_norm = self._normalizer.normalize(cw_col)
            if cw_norm not in seen:
                seen.add(cw_norm)
                proj_cols.append(AliasExpr(
                    expression=ColumnRef(
                        table_ref=default_table_ref,
                        column_name=cw_col,
                        normalized_name=cw_norm,
                    ),
                    alias=cw_col,
                ))

        return proj_cols

    def build_multi(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis,
    ) -> list[SqlBuildPlan]:
        """多表多 Join 场景——每对候选产出独立 SqlBuildPlan。

        按 left→right 链顺序排序候选，每对候选构建一个两表 Join Plan。
        中间 Plan 通过 _temp 表串联——后续 Plan 的左扫描引用前一步的 _temp 产物。
        只有最后一个 Plan 包含聚合+投影+排序+限制步骤。

        Args:
            spec: 已解析的 DeveloperSpec
            hypothesis: Join 推测（含 ≥2 个 STRONG/MEDIUM 候选）

        Returns:
            按链顺序排列的 SqlBuildPlan 列表
        """
        chain = self._sort_candidates_to_chain(hypothesis.candidates)
        # 单候选回退到 build()
        if len(chain) <= 1:
            plan, _ = self.build(spec, hypothesis)
            return [plan]

        plans: list[SqlBuildPlan] = []
        table_map = {t.table_alias: t for t in spec.input_tables}
        chain_id = hashlib.md5(
            "|".join(c.candidate_id for c in chain).encode()
        ).hexdigest()[:8]

        # 收集所有已 Join 的表别名——用于计算中间 Plan 的输出列
        joined_tables: list[str] = []

        for idx, candidate in enumerate(chain):
            is_final = (idx == len(chain) - 1)

            if idx == 0:
                left_source = candidate.left_table
                joined_tables.append(candidate.left_table)
            else:
                left_source = make_temp_name(chain_id, str(idx - 1))

            joined_tables.append(candidate.right_table)

            plan = self._build_chain_step(
                spec=spec,
                hypothesis=hypothesis,
                candidate=candidate,
                table_map=table_map,
                left_source=left_source,
                step_index=idx,
                chain_id=chain_id,
                is_final=is_final,
                joined_tables=list(joined_tables),
            )
            plans.append(plan)

        return plans

    # ── 窗口函数构建 ──

    def _build_window_step(
        self, spec: ParsedDeveloperSpec, table_ref: str = "",
    ) -> WindowStep | None:
        """从 spec.inferred_window_metrics 构建 WindowStep。

        将 SpecEnricher 推断的窗口指标（InferredWindowMetric）转换为
        类型化 WindowExpr 列表，封装为 WindowStep。

        NTILE 的 input_column 为桶数（整数字符串），转换为 SqlLiteral；
        其他函数（LAG/LEAD/SUM_OVER 等）的 input_column 为列名，转换为 ColumnRef；
        ROW_NUMBER/RANK/DENSE_RANK 无参数。

        Args:
            spec: 已解析的 DeveloperSpec（含 inferred_window_metrics）
            table_ref: 源表别名——用于 ColumnRef.table_ref

        Returns:
            WindowStep 或 None（无窗口指标时）
        """
        if not spec.inferred_window_metrics:
            return None

        # 无需参数的窗口函数集合
        _no_arg_functions = frozenset({
            "ROW_NUMBER", "RANK", "DENSE_RANK",
        })
        # NTILE 的 input 为整数 SqlLiteral 而非列引用
        _ntile_functions = frozenset({"NTILE"})

        window_exprs: list[WindowExpr] = []
        for iwm in spec.inferred_window_metrics:
            func_name = iwm.window_function.upper()

            # 跳过不在白名单中的函数名
            if func_name not in WindowFunction.__members__:
                continue

            wf = WindowFunction[func_name]

            # 构建 partition_by ColumnRef 列表
            partition_by: list[ColumnRef] = []
            for p in iwm.partition_by:
                partition_by.append(ColumnRef(
                    table_ref=table_ref,
                    column_name=p,
                    normalized_name=self._normalizer.normalize(p),
                ))

            # 构建 order_by SortSpec 列表——解析 "col DESC" / "col ASC" / "col"
            order_by: list[SortSpec] = []
            for o in iwm.order_by:
                parts = o.strip().split()
                col = parts[0]
                if len(parts) > 1 and parts[1].upper() == "DESC":
                    direction = SortDirection.DESC
                else:
                    direction = SortDirection.ASC
                order_by.append(SortSpec(
                    column=col,
                    direction=direction,
                ))

            # 为 ROW_NUMBER/RANK/DENSE_RANK 自动追加 grain 列作为 tiebreaker——
            # 避免平局时跨引擎（DuckDB vs Spark）ROW_NUMBER 赋值不一致导致物理验证误报。
            # 只在 ORDER BY 和 PARTITION BY 中尚未出现的 grain 列才追加。
            if func_name in ("ROW_NUMBER", "RANK", "DENSE_RANK") and spec.output_spec.grain:
                existing_order_cols = {s.column.lower() for s in order_by}
                partition_cols = {p.column_name.lower() for p in partition_by}
                for grain_col in spec.output_spec.grain:
                    if (
                        grain_col.lower() not in existing_order_cols
                        and grain_col.lower() not in partition_cols
                    ):
                        order_by.append(SortSpec(
                            column=grain_col,
                            direction=SortDirection.ASC,
                        ))
                        existing_order_cols.add(grain_col.lower())

            # 构建 input——根据函数类型选择 ColumnRef 或 SqlLiteral
            win_input: ColumnRef | SqlLiteral | None = None
            if func_name in _ntile_functions:
                # NTILE(n)——桶数为整数 SqlLiteral
                if iwm.input_column:
                    try:
                        n_buckets = int(iwm.input_column)
                    except ValueError:
                        n_buckets = 0
                    win_input = SqlLiteral(value=n_buckets)
            elif func_name not in _no_arg_functions and iwm.input_column:
                # LAG/LEAD/SUM_OVER/AVG_OVER/COUNT_OVER——列引用
                win_input = ColumnRef(
                    table_ref=table_ref,
                    column_name=iwm.input_column,
                    normalized_name=self._normalizer.normalize(iwm.input_column),
                )

            window_exprs.append(WindowExpr(
                function=wf,
                input=win_input,
                partition_by=partition_by,
                order_by=order_by,
                alias=iwm.alias,
            ))

        if not window_exprs:
            return None

        return WindowStep(
            step_id=SqlBuildPlan.generate_step_id("window", {
                "functions": [w.function.value for w in window_exprs],
            }),
            window_exprs=window_exprs,
        )

    def _build_post_window_filter_steps(
        self, spec: ParsedDeveloperSpec,
    ) -> list[FilterStep]:
        """将窗口输出上的封闭比较条件转换为 WindowStep 后的 FilterStep。"""
        window_aliases = {metric.alias for metric in spec.inferred_window_metrics}
        operator_map = {
            CompareOp.EQ: PredicateOperator.EQ,
            CompareOp.NEQ: PredicateOperator.NEQ,
            CompareOp.GT: PredicateOperator.GT,
            CompareOp.GTE: PredicateOperator.GTE,
            CompareOp.LT: PredicateOperator.LT,
            CompareOp.LTE: PredicateOperator.LTE,
        }
        steps: list[FilterStep] = []
        for filter_decl in spec.inferred_post_window_filters:
            if filter_decl.column not in window_aliases:
                continue
            steps.append(FilterStep(
                step_id=SqlBuildPlan.generate_step_id("filter", {
                    "window_column": filter_decl.column,
                    "operator": filter_decl.operator.value,
                    "value": filter_decl.value,
                }),
                predicate=Predicate(
                    left=ColumnRef(
                        table_ref="",
                        column_name=filter_decl.column,
                        normalized_name=self._normalizer.normalize(filter_decl.column),
                    ),
                    operator=operator_map[filter_decl.operator],
                    right=SqlLiteral(value=filter_decl.value),
                ),
            ))
        return steps

    # ── 单表路径 ──

    # ── label_table 支持：CaseWhenStep 生成 ──

    # CompareOp → PredicateOperator 映射表
    _COMPARE_OP_MAP: dict[CompareOp, PredicateOperator] = {
        CompareOp.EQ: PredicateOperator.EQ,
        CompareOp.NEQ: PredicateOperator.NEQ,
        CompareOp.GT: PredicateOperator.GT,
        CompareOp.GTE: PredicateOperator.GTE,
        CompareOp.LT: PredicateOperator.LT,
        CompareOp.LTE: PredicateOperator.LTE,
    }

    def _predicate_from_label_node(
        self, node, table_alias: str,
        derived_expr_map: dict[str, TimeTransformExpr] | None = None,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 LabelPredicateCondition AST 节点转换为 SQL Predicate。

        递归处理 AND/OR/NOT 复合节点——LabelCompare/IsNull/IsNotNull 为叶子节点。
        也支持 dict 类型条件（case_when_rules），自动通过 _predicate_from_dict 处理。

        Args:
            node: LabelPredicateCondition 子类实例或 dict（case_when_rules）
            table_alias: 源表别名——用于 ColumnRef.table_ref
            derived_expr_map: 派生维度名→TimeTransformExpr 映射——节点中的派名列名
                              被解析为 TimeTransformExpr 而非 ColumnRef
            output_names: 聚合输出列名/指标别名集合——条件中引用这些名称时
                          不加表前缀（它们是 SELECT 中的计算列，非源表物理列）

        Returns:
            Predicate——可放入 WhenBranch.condition 或嵌套 Predicate

        Raises:
            ValueError: 遇到不支持的节点类型
        """
        derived_expr_map = derived_expr_map or {}
        output_names = output_names or set()

        # case_when_rules 使用 dict 条件（LLM JSON Schema 兼容格式）
        if isinstance(node, dict):
            return self._predicate_from_dict(
                node, table_alias, derived_expr_map, output_names=output_names,
            )

        if isinstance(node, LabelCompare):
            left = node.left
            # LabelDatePartRef → DatePartExpression（origin/main 新增类型）
            if isinstance(left, LabelDatePartRef):
                col_ref = ColumnRef(
                    table_ref=table_alias,
                    column_name=left.column_name,
                    normalized_name=self._normalizer.normalize(left.column_name),
                )
                date_part = DatePartExpression(
                    part=left.part,
                    column=col_ref,
                )
                op = self._COMPARE_OP_MAP.get(node.op)
                if op is None:
                    raise ValueError(f"不支持的比较操作符: {node.op}")
                sql_lit = self._literal_from_label(node.right)
                return Predicate(left=date_part, operator=op, right=sql_lit)
            # str → derived_expr_map 查找 TimeTransformExpr
            if left in derived_expr_map:
                op = self._COMPARE_OP_MAP.get(node.op)
                if op is None:
                    raise ValueError(f"不支持的比较操作符: {node.op}")
                sql_lit = self._literal_from_label(node.right)
                return Predicate(left=derived_expr_map[left], operator=op, right=sql_lit)
            return self._predicate_from_compare(
                node, table_alias, output_names=output_names,
            )
        elif isinstance(node, LabelIsNull):
            return self._predicate_from_is_null(
                node, table_alias, output_names=output_names,
            )
        elif isinstance(node, LabelIsNotNull):
            return self._predicate_from_is_not_null(
                node, table_alias, output_names=output_names,
            )
        elif isinstance(node, LabelAnd):
            return self._predicate_from_logical(
                node.children, PredicateOperator.AND, table_alias,
                derived_expr_map=derived_expr_map,
                output_names=output_names,
            )
        elif isinstance(node, LabelOr):
            return self._predicate_from_logical(
                node.children, PredicateOperator.OR, table_alias,
                derived_expr_map=derived_expr_map,
                output_names=output_names,
            )
        elif isinstance(node, LabelNot):
            raise ValueError(
                "label_table v1 暂不支持 LabelNot——"
                "LabelNot 应在 Validator 阶段被 NO_LABEL_NOT 检查拒绝，"
                "此处抛出说明门禁未起作用。"
            )
        else:
            raise ValueError(f"不支持的标签谓词节点类型: {type(node).__name__}")

    def _predicate_from_compare(
        self, node: LabelCompare, table_alias: str,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 LabelCompare 转换为 Predicate。

        聚合输出列名（output_names 中的名称）不加表前缀——
        这些是 SELECT 中的计算列，非源表物理列。
        """
        output_names = output_names or set()
        source_column = (
            node.left.column_name
            if isinstance(node.left, LabelDatePartRef)
            else node.left
        )
        normalized = self._normalizer.normalize(source_column)
        # 聚合输出别名不加表前缀——它们在 SELECT 中独立存在
        effective_table_ref = (
            SafeIdentifier(table_alias)
            if source_column not in output_names and normalized not in output_names
            else ""
        )
        col_ref = ColumnRef(
            table_ref=effective_table_ref,
            column_name=SafeIdentifier(source_column),
            normalized_name=SafeIdentifier(normalized),
        )
        left_operand: ColumnRef | DatePartExpression = col_ref
        if isinstance(node.left, LabelDatePartRef):
            left_operand = DatePartExpression(
                part=node.left.part,
                column=col_ref,
            )
        op = self._COMPARE_OP_MAP.get(node.op)
        if op is None:
            raise ValueError(f"不支持的比较操作符: {node.op}")
        sql_lit = self._literal_from_label(node.right)
        return Predicate(left=left_operand, operator=op, right=sql_lit)

    def _predicate_from_is_null(
        self, node: LabelIsNull, table_alias: str,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 LabelIsNull 转换为 Predicate(IS_NULL)。"""
        output_names = output_names or set()
        normalized = self._normalizer.normalize(node.column)
        effective_table_ref = (
            SafeIdentifier(table_alias)
            if node.column not in output_names and normalized not in output_names
            else ""
        )
        col_ref = ColumnRef(
            table_ref=effective_table_ref,
            column_name=SafeIdentifier(node.column),
            normalized_name=SafeIdentifier(normalized),
        )
        return Predicate(left=col_ref, operator=PredicateOperator.IS_NULL)

    def _predicate_from_is_not_null(
        self, node: LabelIsNotNull, table_alias: str,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 LabelIsNotNull 转换为 Predicate(IS_NOT_NULL)。"""
        output_names = output_names or set()
        normalized = self._normalizer.normalize(node.column)
        effective_table_ref = (
            SafeIdentifier(table_alias)
            if node.column not in output_names and normalized not in output_names
            else ""
        )
        col_ref = ColumnRef(
            table_ref=effective_table_ref,
            column_name=SafeIdentifier(node.column),
            normalized_name=SafeIdentifier(normalized),
        )
        return Predicate(left=col_ref, operator=PredicateOperator.IS_NOT_NULL)

    def _predicate_from_logical(
        self,
        children: list,
        operator: PredicateOperator,
        table_alias: str,
        derived_expr_map: dict[str, TimeTransformExpr] | None = None,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 AND/OR 子节点列表递归折叠为嵌套 Predicate。

        两个子节点时：Predicate(left=left, operator=AND/OR, right=right)
        超过两个时：左结合折叠——((a AND b) AND c)

        Args:
            derived_expr_map: 传递给子递归的派生维度映射
            output_names: 聚合输出名称集合——传递给子递归
        """
        if len(children) < 2:
            raise ValueError(
                f"{operator.value} 至少需要 2 个子节点，实际 {len(children)}"
            )
        preds = [
            self._predicate_from_label_node(
                c, table_alias, derived_expr_map, output_names=output_names,
            )
            for c in children
        ]
        # 左结合折叠
        result = preds[0]
        for p in preds[1:]:
            result = Predicate(left=result, operator=operator, right=p)
        return result

    @staticmethod
    def _literal_from_label(lit: LabelTypedLiteral) -> SqlLiteral:
        """将 LabelTypedLiteral 转换为 SqlLiteral。

        LabelTypedLiteral.value 类型为 str|Decimal|bool|None——
        Decimal 转换为 float 以兼容 SqlLiteral 类型约束。
        LLM 可能输出 data_type="number" 但值为 JSON 字符串（如 "2"），
        此时强制按 data_type 转换为数值——避免 SQL 中出现 '2' 代替 2。
        """
        from decimal import Decimal

        raw = lit.value
        if isinstance(raw, Decimal):
            return SqlLiteral(value=float(raw))
        # LLM 经常输出 data_type="number" 但 value 为 JSON 字符串——强制按 data_type 转换
        if lit.data_type == "number" and isinstance(raw, str):
            try:
                return SqlLiteral(value=float(raw))
            except (ValueError, TypeError):
                pass  # 转换失败时保留原始字符串——避免丢失数据
        return SqlLiteral(value=raw)

    @staticmethod
    def _collect_label_condition_columns(node, collected: set[str]) -> None:
        """递归收集 LabelPredicateCondition 树中所有列引用。

        Args:
            node: LabelPredicateCondition 子类实例
            collected: 输出集合——收集到的列名加入此集合
        """
        if isinstance(node, LabelCompare):
            collected.add(
                node.left.column_name
                if isinstance(node.left, LabelDatePartRef)
                else node.left
            )
        elif isinstance(node, (LabelIsNull, LabelIsNotNull)):
            collected.add(node.column)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                SqlBuildPlanBuilder._collect_label_condition_columns(child, collected)
        elif isinstance(node, LabelNot):
            SqlBuildPlanBuilder._collect_label_condition_columns(node.child, collected)

    # ── dict 条件支持（case_when_rules）──

    @staticmethod
    def _literal_from_dict(lit_dict: dict) -> SqlLiteral | list[SqlLiteral] | None:
        """将 LITERAL 类型的 dict 转换为 SqlLiteral 或列表。

        Args:
            lit_dict: 如 {"node_type": "LITERAL", "value": ..., "data_type": ...}

        Returns:
            SqlLiteral（标量）或 list[SqlLiteral]（IN 子句的列表值）
        """
        if not lit_dict:
            return None
        value = lit_dict.get("value")
        if isinstance(value, list):
            return [SqlLiteral(value=v) for v in value]
        from decimal import Decimal
        raw = value
        if isinstance(raw, Decimal):
            return SqlLiteral(value=float(raw))
        data_type = lit_dict.get("data_type", "")
        if data_type == "number" and isinstance(raw, str):
            try:
                return SqlLiteral(value=float(raw))
            except (ValueError, TypeError):
                pass
        return SqlLiteral(value=raw)

    # dict 条件 → PredicateOperator 映射表（支持 CompareOp + IN/NOT_IN/BETWEEN）
    _DICT_COMPARE_OP_MAP: dict[str, PredicateOperator] = {
        "=": PredicateOperator.EQ,
        "!=": PredicateOperator.NEQ,
        ">": PredicateOperator.GT,
        ">=": PredicateOperator.GTE,
        "<": PredicateOperator.LT,
        "<=": PredicateOperator.LTE,
        "IN": PredicateOperator.IN,
        "NOT_IN": PredicateOperator.NOT_IN,
        "BETWEEN": PredicateOperator.BETWEEN,
    }

    def _predicate_from_dict(
        self,
        condition: dict,
        table_alias: str,
        derived_expr_map: dict[str, TimeTransformExpr],
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 dict 类型条件转换为 Predicate——用于 case_when_rules。

        dict 格式与 LabelPredicateCondition 结构一致但使用字符串操作符，
        支持 "IN" 等 CompareOp 枚举范围外的操作符。
        """
        output_names = output_names or set()
        node_type = condition.get("node_type", "")

        if node_type == "COMPARE":
            return self._predicate_from_dict_compare(
                condition, table_alias, derived_expr_map,
                output_names=output_names,
            )
        elif node_type == "IS_NULL":
            return self._predicate_from_dict_null(
                condition, table_alias, derived_expr_map, is_null=True,
                output_names=output_names,
            )
        elif node_type == "IS_NOT_NULL":
            return self._predicate_from_dict_null(
                condition, table_alias, derived_expr_map, is_null=False,
                output_names=output_names,
            )
        elif node_type in ("AND", "OR"):
            op = PredicateOperator.AND if node_type == "AND" else PredicateOperator.OR
            children = condition.get("children", [])
            if len(children) < 2:
                raise ValueError(
                    f"{node_type} 至少需要 2 个子节点，实际 {len(children)}"
                )
            preds = [
                self._predicate_from_dict(
                    c, table_alias, derived_expr_map, output_names=output_names,
                )
                for c in children
            ]
            result = preds[0]
            for p in preds[1:]:
                result = Predicate(left=result, operator=op, right=p)
            return result
        elif node_type == "NOT":
            raise ValueError("case_when_rules 暂不支持 NOT")
        else:
            raise ValueError(f"不支持的 dict 条件 node_type: {node_type}")

    def _predicate_from_dict_compare(
        self,
        condition: dict,
        table_alias: str,
        derived_expr_map: dict[str, TimeTransformExpr],
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 COMPARE 类型的 dict 条件转换为 Predicate。"""
        output_names = output_names or set()
        left_name = condition.get("left", "")

        # 左操作数——派生维度别名解析为 TimeTransformExpr
        if left_name in derived_expr_map:
            left = derived_expr_map[left_name]
        else:
            normalized = self._normalizer.normalize(left_name)
            # 聚合输出别名不加表前缀
            effective_table_ref = (
                SafeIdentifier(table_alias)
                if left_name not in output_names and normalized not in output_names
                else ""
            )
            left = ColumnRef(
                table_ref=effective_table_ref,
                column_name=SafeIdentifier(left_name),
                normalized_name=SafeIdentifier(normalized),
            )

        # 操作符映射
        op_str = condition.get("op", "")
        op = self._DICT_COMPARE_OP_MAP.get(op_str)
        if op is None:
            raise ValueError(f"不支持的 dict 条件操作符: {op_str}")

        # 右操作数
        right_dict = condition.get("right", {})
        right = self._literal_from_dict(right_dict)

        return Predicate(left=left, operator=op, right=right)

    def _predicate_from_dict_null(
        self,
        condition: dict,
        table_alias: str,
        derived_expr_map: dict[str, TimeTransformExpr],
        is_null: bool,
        output_names: set[str] | None = None,
    ) -> Predicate:
        """将 IS_NULL / IS_NOT_NULL 类型的 dict 条件转换为 Predicate。"""
        output_names = output_names or set()
        col_name = condition.get("column", "")

        if col_name in derived_expr_map:
            left = derived_expr_map[col_name]
        else:
            normalized = self._normalizer.normalize(col_name)
            effective_table_ref = (
                SafeIdentifier(table_alias)
                if col_name not in output_names and normalized not in output_names
                else ""
            )
            left = ColumnRef(
                table_ref=effective_table_ref,
                column_name=SafeIdentifier(col_name),
                normalized_name=SafeIdentifier(normalized),
            )

        op = PredicateOperator.IS_NULL if is_null else PredicateOperator.IS_NOT_NULL
        return Predicate(left=left, operator=op)

    @staticmethod
    def _collect_dict_condition_columns(
        condition: dict,
        derived_expr_map: dict[str, TimeTransformExpr],
        physical_collected: set[str],
    ) -> None:
        """从 dict 条件中提取物理列名——派生维度别名解析为 source_column。

        Args:
            condition: dict 类型条件节点
            derived_expr_map: 派生维度名→TimeTransformExpr 映射
            physical_collected: 输出集合——收集到的物理列名
        """
        node_type = condition.get("node_type", "")
        if node_type == "COMPARE":
            col = condition.get("left", "")
            if col in derived_expr_map:
                physical_collected.add(
                    str(derived_expr_map[col].source_column)
                )
            else:
                physical_collected.add(col)
        elif node_type in ("IS_NULL", "IS_NOT_NULL"):
            col = condition.get("column", "")
            if col in derived_expr_map:
                physical_collected.add(
                    str(derived_expr_map[col].source_column)
                )
            else:
                physical_collected.add(col)
        elif node_type in ("AND", "OR"):
            for child in condition.get("children", []):
                SqlBuildPlanBuilder._collect_dict_condition_columns(
                    child, derived_expr_map, physical_collected,
                )
        elif node_type == "NOT":
            child = condition.get("child", {})
            SqlBuildPlanBuilder._collect_dict_condition_columns(
                child, derived_expr_map, physical_collected,
            )

    @staticmethod
    def _resolve_derived_source_table(
        dd_source_table: str, spec: ParsedDeveloperSpec,
    ) -> str:
        """将 LLM 输出的物理表名映射为表别名。

        RequirementPlanner LLM 输出的 source_table 是物理表名
        （如 gold.fact_trips / gold.dim_taxi_zone），
        但 ColumnRef.table_ref / TimeTransformExpr.source_table 等字段
        需要表别名（如 ft / dtz）。
        此方法通过 spec.input_tables 的 source_table→table_alias 做映射。

        适用范围：DerivedDimensionDecl.source_table 和 DimensionDecl.source_table。
        """
        for t in spec.input_tables:
            if t.source_table == dd_source_table:
                return t.table_alias
        return dd_source_table  # 兜底：直接返回（单表别名即表名场景）

    @staticmethod
    def _resolve_column_source_table(
        column_name: str, spec: ParsedDeveloperSpec,
    ) -> str:
        """从 input_tables 中查找列所属的表别名——多表消歧兜底。

        当 dimension 未声明 source_table 时，通过遍历所有源表的
        key_columns + business_columns 查找列的归属表。
        多表命中时返回第一个匹配的表别名（歧义由上层 ColumnRef 的 AMBIGUOUS_REFERENCE
        机制处理——此处仅提供候选），无匹配时返回空字符串。

        Args:
            column_name: 列名（原始名称）
            spec: 已解析的 DeveloperSpec

        Returns:
            表别名或空字符串
        """
        for t in spec.input_tables:
            all_cols = t.key_columns + t.business_columns
            for c in all_cols:
                if c.column_name == column_name:
                    return t.table_alias
        return ""  # 未找到——由上层兜底

    def _build_case_when_steps(self, spec: ParsedDeveloperSpec) -> list:
        """从 spec.label_rules 和 spec.case_when_rules 生成 CaseWhenStep 列表。

        先处理 label_rules（v4-light 标签表路径），再处理 case_when_rules
        （v3.1 RequirementPlanner 派生 CASE WHEN 路径）。
        每一条规则生成一个独立的 CaseWhenStep——
        放在 AggregateStep（如果有）之后、ProjectStep 之前。

        每个 typed_branch 的 condition（LabelPredicateCondition）被转换为
        Predicate，then_label 被转换为 SqlLiteral。
        case_when_rules 的 condition 为 dict（LLM JSON Schema 兼容），
        通过 _predicate_from_dict 处理，并支持 derived_expr_map 将派生维度别名
        解析为 TimeTransformExpr。

        Args:
            spec: 已解析的 DeveloperSpec

        Returns:
            CaseWhenStep 列表——两个规则列表均为空时返回空列表
        """
        steps: list = []
        table_alias = spec.input_tables[0].table_alias

        # ── 构建"输出名称"集合——CASE WHEN 条件引用这些名称时不应加表前缀 ──
        # 聚合输出别名（如 crash_count、total_killed）在 SELECT 中作为计算列存在，
        # 不是源表物理列——加表前缀会导致 DuckDB Binder Error。
        output_names: set[str] = set()
        for col in spec.output_spec.columns:
            output_names.add(col.name)
        for m in spec.metrics:
            output_names.add(m.alias)

        # ── 构建派生维度别名→TimeTransformExpr 映射（供 Predicate 引用）──
        derived_expr_map: dict[str, TimeTransformExpr] = {
            dd.dimension_name: TimeTransformExpr(
                source_column=SafeIdentifier(dd.source_column),
                source_table=SafeIdentifier(
                    self._resolve_derived_source_table(dd.source_table, spec)
                ),
                time_function=dd.time_function,
            )
            for dd in spec.derived_dimensions
        }

        # ── 已处理的输出列名——label_rules 优先级更高（携带 evaluation_phase），
        #     case_when_rules 中的同名规则应被跳过，避免重复生成 CaseWhenStep。
        seen_aliases: set[str] = set()

        # ── 处理 label_rules（v4-light 标签表路径）──
        #     同时检查 label_rules 内部的重复——同名列只保留第一条。
        #     典型场景：spec 同时包含 pre_aggregate 和 post_aggregate 的
        #     peak_type 规则时，第二条通过 withColumn 覆盖写入是正确行为，
        #     但 SQL 不允许 SELECT 列重名，必须在构建阶段去重。
        for rule in spec.label_rules:
            if rule.output_column in seen_aliases:
                continue
            cases: list[WhenBranch] = []
            for tb in rule.typed_branches:
                predicate = self._predicate_from_label_node(
                    tb.condition, table_alias, derived_expr_map,
                    output_names=output_names,
                )
                result = SqlLiteral(value=tb.then_label)
                cases.append(WhenBranch(condition=predicate, result=result))

            else_val = SqlLiteral(value=rule.else_value)

            step_id_content = {
                "output_column": rule.output_column,
                "branch_count": len(cases),
            }
            step = CaseWhenStep(
                step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
                cases=cases,
                else_value=else_val,
                alias=SafeIdentifier(rule.output_column),
                evaluation_phase=rule.evaluation_phase,  # 从 CaseWhenDecl 传递聚合阶段
            )
            steps.append(step)
            seen_aliases.add(rule.output_column)

        # ── 处理 case_when_rules（v3.1 RequirementPlanner 路径）──
        #     跳过与 label_rules 同名的规则——label_rules 携带 evaluation_phase，
        #     信息更完整，优先保留。
        for rule in spec.case_when_rules:
            if rule.output_column in seen_aliases:
                continue
            cases: list[WhenBranch] = []
            for branch in rule.branches:
                predicate = self._predicate_from_label_node(
                    branch.condition, table_alias, derived_expr_map,
                    output_names=output_names,
                )
                result = SqlLiteral(value=branch.then_value)
                cases.append(WhenBranch(condition=predicate, result=result))

            else_val = SqlLiteral(value=rule.else_value)
            step_id_content = {
                "output_column": rule.output_column,
                "branch_count": len(cases),
            }
            step = CaseWhenStep(
                step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
                cases=cases,
                else_value=else_val,
                alias=SafeIdentifier(rule.output_column),
            )
            steps.append(step)
            seen_aliases.add(rule.output_column)

        return steps

    # ── 单表路径 ──

    def _assert_all_output_columns_resolved(
        self,
        spec: ParsedDeveloperSpec,
    ) -> None:
        """防御性检查——所有输出列必须有解析规则，禁止回退为物理 ColumnRef。

        对所有 dataset_type 执行未解析列检测——在 build 阶段就暴露问题，
        避免到 execute 阶段才以晦涩的 Binder Error 失败。

        复用 _find_unresolved_derived_columns() 避免重复维护六类字段收集逻辑。

        Args:
            spec: 已解析的 DeveloperSpec

        Raises:
            DerivedColumnRuleMissingError: 存在未解析的输出列或作用域约束违反
        """
        from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns

        # 1. 检查未解析输出列（所有 dataset_type 通用）
        unresolved_output = _find_unresolved_derived_columns(spec)

        if unresolved_output:
            # 根据 spec 当前配置给出针对性修复指引
            hints: list[str] = []
            if not spec.metrics:
                hints.append("缺少 metrics——聚合列（COUNT/SUM/AVG 等）需在 spec.metrics 中声明")
            if not spec.dimensions:
                hints.append("缺少 dimensions——分组键列需在 spec.dimensions 中声明")
            if not spec.compute_steps:
                hints.append(
                    "缺少 compute_steps——派生表达式（如 HOUR(pickup_at)）、"
                    "CASE WHEN 分支需在 spec.compute_steps 中声明"
                )
            if spec.dataset_type == DatasetType.LABEL_TABLE and not spec.label_rules:
                hints.append(
                    "缺少 label_rules——label_table 的标签列需通过 label_rules "
                    "或 LLM 标签提取生成"
                )
            hint_text = "；".join(hints) if hints else (
                "请检查 spec.metrics / dimensions / compute_steps / label_rules 配置"
            )

            raise DerivedColumnRuleMissingError(
                f"输出列无解析规则——以下列既不是源表物理列，也不是指标/维度/窗口指标"
                f"/计算步骤/标签规则输出: {unresolved_output}。"
                f"修复指引: {hint_text}。"
                f"这些列将在编译期被当作物理 ColumnRef 生成 SQL，"
                f"执行时会因列不存在而报 Binder Error——"
                f"此处提前阻断以给出清晰错误信息。"
            )

    def _build_single_table(self, spec: ParsedDeveloperSpec) -> list[StepNode]:
        """单表构建管线。

        Scan → Filter* → (pre-aggregate CaseWhen*) → Aggregate
        → (post-aggregate CaseWhen*) → Window → Project → Sort → Limit。

        pre-aggregate CASE WHEN 输出列自动加入 GROUP BY——派生维度在聚合前计算。
        post-aggregate CASE WHEN 在聚合后计算——条件可引用聚合指标。
        """
        steps: list[StepNode] = []
        table = spec.input_tables[0]

        # 收集标签输出列名——这些列由 CaseWhenStep 产生，不应出现在 Scan/Project 中
        label_output_columns: set[str] = {
            rule.output_column for rule in spec.label_rules
        }
        # 收集标签条件中引用的源列——需加入 Scan
        label_source_columns: set[str] = set()
        for rule in spec.label_rules:
            for tb in rule.typed_branches:
                self._collect_label_condition_columns(tb.condition, label_source_columns)

        # 1. ScanStep——构建 required_columns
        scan_cols = self._build_required_columns(table.table_alias, spec, table)
        # 排除标签输出列（它们不是物理列）并追加标签源列
        scan_cols = [
            c for c in scan_cols
            if c.column_name not in label_output_columns
        ]
        # 追加标签条件引用的源列（如果尚未在列表中）
        existing_norm = {c.normalized_name for c in scan_cols}
        for src_col in sorted(label_source_columns):
            norm = self._normalizer.normalize(src_col)
            if norm not in existing_norm:
                existing_norm.add(norm)
                scan_cols.append(ColumnRef(
                    table_ref=SafeIdentifier(table.table_alias),
                    column_name=SafeIdentifier(src_col),
                    normalized_name=SafeIdentifier(norm),
                ))

        # 追加派生维度源列——TimeTransformExpr 需要源列在 Scan 中
        for dd in spec.derived_dimensions:
            if dd.source_table == table.table_alias:
                norm = self._normalizer.normalize(dd.source_column)
                if norm not in existing_norm:
                    existing_norm.add(norm)
                    scan_cols.append(ColumnRef(
                        table_ref=SafeIdentifier(table.table_alias),
                        column_name=SafeIdentifier(dd.source_column),
                        normalized_name=SafeIdentifier(norm),
                    ))

        # 追加 case_when_rules 条件引用的物理列
        if spec.case_when_rules:
            cw_derived_map: dict[str, TimeTransformExpr] = {
                dd.dimension_name: TimeTransformExpr(
                    source_column=SafeIdentifier(dd.source_column),
                    source_table=SafeIdentifier(
                        self._resolve_derived_source_table(dd.source_table, spec)
                    ),
                    time_function=dd.time_function,
                )
                for dd in spec.derived_dimensions
            }
            cw_source_cols: set[str] = set()
            for rule in spec.case_when_rules:
                for branch in rule.branches:
                    self._collect_dict_condition_columns(
                        branch.condition, cw_derived_map, cw_source_cols,
                    )
            for src_col in sorted(cw_source_cols):
                norm = self._normalizer.normalize(src_col)
                if norm not in existing_norm:
                    existing_norm.add(norm)
                    scan_cols.append(ColumnRef(
                        table_ref=SafeIdentifier(table.table_alias),
                        column_name=SafeIdentifier(src_col),
                        normalized_name=SafeIdentifier(norm),
                    ))

        scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan", {"table": table.source_table}),
            table_ref=table.table_alias,
            required_columns=scan_cols,
            estimated_row_count=table.row_count,
        )
        steps.append(scan)

        # 2. FilterSteps——表级预过滤
        for f in table.filters:
            filter_step = self._build_filter_step(f, table.table_alias)
            steps.append(filter_step)

        # 2b. 时间范围过滤——TimeRangeDecl → FilterStep 列表（Phase 5 业务日历，半开区间）
        tr_filters = self._build_time_range_filter(spec, table.table_alias)
        steps.extend(tr_filters)

        # 3. pre-aggregate CaseWhenSteps——派生维度在聚合前计算
        #    输出列自动加入 GROUP BY，条件引用源表物理列（非聚合指标）
        all_case_when_steps = self._build_case_when_steps(spec)
        pre_agg_cw = [s for s in all_case_when_steps
                       if s.evaluation_phase == "pre_aggregate"]
        post_agg_cw = [s for s in all_case_when_steps
                        if s.evaluation_phase != "pre_aggregate"]
        steps.extend(pre_agg_cw)

        # 3a. pre-aggregate CASE WHEN 输出列 → 自动加入 group_by
        pre_agg_cw_aliases: set[str] = {str(s.alias) for s in pre_agg_cw if s.alias}

        # 4. AggregateStep——如果有指标（group_keys 自动包含 pre_agg_cw 输出列）
        if spec.metrics:
            agg = self._build_aggregate_step(
                spec, table.table_alias,
                extra_group_keys=pre_agg_cw_aliases,
            )
            steps.append(agg)

        # 4b. post-aggregate CaseWhenSteps——标签列在聚合后计算（条件可引用聚合指标）
        steps.extend(post_agg_cw)

        # 3c. WindowStep——如果有窗口指标（聚合后、投影前）
        window = self._build_window_step(spec, table.table_alias)
        if window:
            steps.append(window)
            steps.extend(self._build_post_window_filter_steps(spec))

        # ── 防御性检查：label_table 输出列必须有解析规则——禁止回退成物理 ColumnRef ──
        self._assert_all_output_columns_resolved(spec)

        # 4. ProjectStep——输出列（排除窗口函数已产出的别名 + 标签列）
        project = self._build_project_step(spec)
        excluded_aliases: set[str] = set()
        if window:
            excluded_aliases.update(
                str(w.alias) for w in window.window_exprs if w.alias
            )
        excluded_aliases.update(label_output_columns)
        # 排除 case_when_rules 输出列（它们由 CaseWhenStep 产生）
        excluded_aliases.update(
            rule.output_column for rule in spec.case_when_rules
        )
        if excluded_aliases and not spec.ratio_metrics:
            filtered_cols = [
                c for c in project.columns
                if c.alias not in excluded_aliases
            ]
            project = ProjectStep(
                step_id=project.step_id,
                columns=filtered_cols,
            )
        steps.append(project)

        # 5. SortStep——如果有排序声明
        if spec.output_spec.sort:
            sort = self._build_sort_step(spec)
            steps.append(sort)

        # 6. LimitStep——如果有行限制
        if spec.output_spec.limit is not None:
            limit = LimitStep(
                step_id=SqlBuildPlan.generate_step_id("limit", {"limit": spec.output_spec.limit}),
                limit=spec.output_spec.limit,
            )
            steps.append(limit)

        return steps

    # ── 多表路径 ──

    @staticmethod
    def _self_join_aliases(table_alias: str) -> tuple[str, str]:
        """为自引用场景生成左右别名——避免同一物理表名出现两次。

        Args:
            table_alias: 原始表别名

        Returns:
            (left_alias, right_alias) 元组
        """
        return f"{table_alias}_self_left", f"{table_alias}_self_right"

    @staticmethod
    def _assert_degradation_safe(spec: ParsedDeveloperSpec) -> None:
        """校验空 candidates 退化为单表是否安全。

        退化只保留首表扫描——若 output_columns 中存在仅在其他输入表声明的列，
        退化计划必然在运行期 Binder Error，应在构建期显式失败并给出真实原因
        （通常是 Join 候选被安全门禁丢弃，如 dim 表联结键未声明 unique: true）。

        Raises:
            ValueError: 输出列依赖非首表专属列时。
        """
        if len(spec.input_tables) <= 1:
            return
        output_columns = spec.output_spec.columns if spec.output_spec else []
        if not output_columns:
            return

        def _decl_names(table) -> set[str]:
            # 输入表列模型为 ColumnDecl——名称字段是 column_name（区别于输出列的 name）
            return {
                c.column_name
                for c in (table.columns + table.key_columns + table.business_columns)
            }

        first_cols = _decl_names(spec.input_tables[0])
        other_cols: set[str] = set()
        for t in spec.input_tables[1:]:
            other_cols |= _decl_names(t)

        # 仅在其他表声明的输出列——退化后必然找不到（指标别名/计算列不在此列）
        offending = [
            oc.name for oc in output_columns
            if oc.name not in first_cols and oc.name in other_cols
        ]
        if offending:
            raise ValueError(
                f"多表 spec 无可用 Join 候选，无法退化为单表：输出列 {offending} "
                f"仅在非首表中声明。请检查 Join 声明是否被安全门禁丢弃"
                f"（如 dim 表联结键需声明 unique: true）"
            )

    def _build_multi_table(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis,
    ) -> list[StepNode]:
        """两表构建管线。

        Scan → Filter* → Join → (pre-aggregate CaseWhen*) → Aggregate
        → (post-aggregate CaseWhen*) → Window → Project → Sort → Limit。

        pre-aggregate CASE WHEN 输出列自动加入 GROUP BY——在 Join 后、聚合前计算。
        post-aggregate CASE WHEN 在聚合后计算——条件可引用聚合指标。
        """
        steps: list[StepNode] = []
        table_map = {t.table_alias: t for t in spec.input_tables}

        if not hypothesis.candidates:
            # 无候选 Join——退化为单表前必须确认输出不依赖其他表，
            # 否则退化计划会把 dim 表专属列当作首表列扫描，
            # 编译出的 SQL 在运行期以晦涩的 Binder Error 失败
            self._assert_degradation_safe(spec)
            return self._build_single_table(spec)

        join_candidate = hypothesis.candidates[0]
        left_table = table_map[join_candidate.left_table]
        right_table = table_map[join_candidate.right_table]

        # ── 自引用检测：同一张表 Join 自身时需要不同别名 ──
        is_self_join = left_table.table_alias == right_table.table_alias
        if is_self_join:
            left_alias, right_alias = self._self_join_aliases(left_table.table_alias)
        else:
            left_alias = left_table.table_alias
            right_alias = right_table.table_alias

        # 1. ScanStep——左表
        left_cols = self._build_required_columns(left_alias, spec, left_table)
        # 追加左表派生维度源列
        left_existing = {c.normalized_name for c in left_cols}
        for dd in spec.derived_dimensions:
            if dd.source_table == left_alias:
                norm = self._normalizer.normalize(dd.source_column)
                if norm not in left_existing:
                    left_existing.add(norm)
                    left_cols.append(ColumnRef(
                        table_ref=SafeIdentifier(left_alias),
                        column_name=SafeIdentifier(dd.source_column),
                        normalized_name=SafeIdentifier(norm),
                    ))
        left_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan_l", {"table": left_table.source_table}),
            table_ref=left_alias,
            required_columns=left_cols,
            estimated_row_count=left_table.row_count,
        )
        steps.append(left_scan)

        # 2. ScanStep——右表
        right_cols = self._build_required_columns(right_alias, spec, right_table)
        # 追加右表派生维度源列
        right_existing = {c.normalized_name for c in right_cols}
        for dd in spec.derived_dimensions:
            if dd.source_table == right_alias:
                norm = self._normalizer.normalize(dd.source_column)
                if norm not in right_existing:
                    right_existing.add(norm)
                    right_cols.append(ColumnRef(
                        table_ref=SafeIdentifier(right_alias),
                        column_name=SafeIdentifier(dd.source_column),
                        normalized_name=SafeIdentifier(norm),
                    ))
        right_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan_r", {"table": right_table.source_table}),
            table_ref=right_alias,
            required_columns=right_cols,
            estimated_row_count=right_table.row_count,
        )
        steps.append(right_scan)

        # 3. FilterSteps——两表的预过滤（自引用时使用各自别名）
        for f in left_table.filters:
            steps.append(self._build_filter_step(f, left_alias))
        for f in right_table.filters:
            steps.append(self._build_filter_step(f, right_alias))

        # 3b. 时间范围过滤——在 Join 之前下推到左表（Phase 5 业务日历，半开区间）
        tr_filters = self._build_time_range_filter(spec, left_alias)
        steps.extend(tr_filters)

        # 4. JoinStep——基于 JoinCandidate（自引用时使用生成别名）
        join_step = JoinStep(
            step_id=SqlBuildPlan.generate_step_id("join", {
                "left": join_candidate.left_table,
                "right": join_candidate.right_table,
                "left_key": join_candidate.left_key_normalized,
                "right_key": join_candidate.right_key_normalized,
            }),
            right_table_ref=right_alias,
            join_type=join_candidate.join_type,
            join_keys=[
                (
                    ColumnRef(
                        table_ref=left_alias,
                        column_name=join_candidate.left_key,
                        normalized_name=join_candidate.left_key_normalized,
                    ),
                    ColumnRef(
                        table_ref=right_alias,
                        column_name=join_candidate.right_key,
                        normalized_name=join_candidate.right_key_normalized,
                    ),
                )
            ],
            relationship_ref=join_candidate.candidate_id,
            cardinality_hint=None,  # Phase 1B 不推断基数
        )
        steps.append(join_step)

        # 4b. pre-aggregate CaseWhenSteps——派生维度在 Join 后、聚合前计算
        #     输出列自动加入 GROUP BY（多表场景同单表逻辑）
        all_case_when_steps = self._build_case_when_steps(spec)
        pre_agg_cw = [s for s in all_case_when_steps
                       if s.evaluation_phase == "pre_aggregate"]
        post_agg_cw = [s for s in all_case_when_steps
                        if s.evaluation_phase != "pre_aggregate"]
        steps.extend(pre_agg_cw)

        # 5. AggregateStep——如果有指标（多表——group_keys 自动包含 pre_agg_cw 输出列）
        pre_agg_cw_aliases: set[str] = {str(s.alias) for s in pre_agg_cw if s.alias}
        if spec.metrics:
            agg_table_ref = left_alias if is_self_join else ""
            agg = self._build_aggregate_step(
                spec, agg_table_ref,
                extra_group_keys=pre_agg_cw_aliases,
            )
            steps.append(agg)

        # 5b. post-aggregate CaseWhenSteps——标签列在聚合后计算（多表场景）
        steps.extend(post_agg_cw)

        # 5c. WindowStep——如果有窗口指标（聚合后、投影前）
        window = self._build_window_step(spec, "")
        if window:
            steps.append(window)
            steps.extend(self._build_post_window_filter_steps(spec))

        # 6. ProjectStep（排除窗口函数已产出的别名 + CASE WHEN 标签列）
        # 自引用时用左别名消除列歧义——左右表列名完全相同，DuckDB 无法自动解析
        proj_table_ref = left_alias if is_self_join else ""
        project = self._build_project_step(spec, default_table_ref=proj_table_ref)
        excluded_aliases: set[str] = set()
        if window:
            excluded_aliases.update(
                str(w.alias) for w in window.window_exprs if w.alias
            )
        # 排除所有 CASE WHEN 输出列——这些列由 CaseWhenStep 产生，不在源表中
        label_output_columns = {rule.output_column for rule in spec.label_rules}
        excluded_aliases.update(label_output_columns)
        if excluded_aliases and not spec.ratio_metrics:
            filtered_cols = [
                c for c in project.columns
                if c.alias not in excluded_aliases
            ]
            project = ProjectStep(
                step_id=project.step_id,
                columns=filtered_cols,
            )
        steps.append(project)

        # 7. SortStep
        if spec.output_spec.sort:
            sort = self._build_sort_step(spec)
            steps.append(sort)

        # 8. LimitStep
        if spec.output_spec.limit is not None:
            limit = LimitStep(
                step_id=SqlBuildPlan.generate_step_id("limit", {"limit": spec.output_spec.limit}),
                limit=spec.output_spec.limit,
            )
            steps.append(limit)

        return steps

    # ── 链构建辅助（Phase 4.6 多跳 Join） ──

    @staticmethod
    def _sort_candidates_to_chain(candidates):
        """贪心排序候选为线性链——按 left_table→right_table 链接关系排列。

        链断裂或菱形分支的残留候选附在尾部，由 Validator V-009b 拒绝。
        """
        if len(candidates) <= 1:
            return list(candidates)

        chain = [candidates[0]]
        remaining = list(candidates[1:])

        while remaining:
            prev_right = chain[-1].right_table
            next_c = next(
                (c for c in remaining if c.left_table == prev_right), None
            )
            if not next_c:
                break
            chain.append(next_c)
            remaining.remove(next_c)

        # 残留候选附在尾部——菱形场景由 Validator 拒绝
        chain.extend(remaining)
        return chain

    def _build_chain_step(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis,
        candidate,  # JoinCandidate
        table_map: dict,
        left_source: str,
        step_index: int,
        chain_id: str,
        is_final: bool,
        joined_tables: list[str],
    ) -> SqlBuildPlan:
        """构建链中单个步骤的 SqlBuildPlan。

        所有步骤包含：Scan(L) + Scan(R) + Join
        仅最终步骤额外包含：Aggregate + Project + Sort + Limit
        中间步骤额外包含：透传 Project（输出全部列供下游使用）

        Phase 5 新增：自引用检测——链首步 left_table == right_table 时生成不同别名。
        """
        steps: list[StepNode] = []
        right_table = table_map[candidate.right_table]

        # ── 自引用检测：链第一步同表 Join 自身时需要不同别名 ──
        right_alias = right_table.table_alias
        left_alias = left_source  # 默认使用传入的 left_source
        if step_index == 0:
            left_table = table_map.get(candidate.left_table)
            if left_table and left_table.table_alias == right_table.table_alias:
                left_alias, right_alias = self._self_join_aliases(left_table.table_alias)
                # 更新 left_source 以保持一致性
                left_source = left_alias

        # ── 1. ScanStep - 左侧 ──
        if step_index == 0:
            left_table = table_map[candidate.left_table]
            # 使用可能已修改的别名构建 required_columns
            left_cols = self._build_required_columns(left_alias, spec, left_table)
            # 确保 join key 在 required_columns 中
            left_key_norm = self._normalizer.normalize(candidate.left_key)
            if not any(c.normalized_name == left_key_norm for c in left_cols):
                left_cols.append(ColumnRef(
                    table_ref=left_alias,
                    column_name=candidate.left_key,
                    normalized_name=left_key_norm,
                ))
        else:
            # _temp 表——从之前已 Join 表收集全部列作为可用列
            prev_tables = joined_tables[:-1]  # 排除当前右表
            left_cols = self._build_temp_scan_columns(table_map, prev_tables)

        left_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id(
                "scan_l", {"chain": chain_id, "step": step_index, "table": left_source}
            ),
            table_ref=left_source,
            required_columns=left_cols,
        )
        steps.append(left_scan)

        # ── 2. ScanStep - 右侧 ──
        right_cols = self._build_required_columns(right_alias, spec, right_table)
        right_key_norm = self._normalizer.normalize(candidate.right_key)
        if not any(c.normalized_name == right_key_norm for c in right_cols):
            right_cols.append(ColumnRef(
                table_ref=right_alias,
                column_name=candidate.right_key,
                normalized_name=right_key_norm,
            ))

        right_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id(
                "scan_r", {"chain": chain_id, "step": step_index, "table": candidate.right_table}
            ),
            table_ref=right_alias,
            required_columns=right_cols,
        )
        steps.append(right_scan)

        # ── 3. FilterSteps（仅真实表，自引用时使用各自别名）──
        if step_index == 0:
            left_table = table_map[candidate.left_table]
            for f in left_table.filters:
                steps.append(self._build_filter_step(f, left_alias))
        for f in right_table.filters:
            steps.append(self._build_filter_step(f, right_alias))

        # ── 4. JoinStep ──
        join_step = JoinStep(
            step_id=SqlBuildPlan.generate_step_id("join", {
                "left": left_source,
                "right": candidate.right_table,
                "left_key": candidate.left_key_normalized,
                "right_key": candidate.right_key_normalized,
                "chain": chain_id,
                "step": step_index,
            }),
            right_table_ref=right_alias,
            join_type=candidate.join_type,
            join_keys=[(
                ColumnRef(
                    table_ref=left_source,
                    column_name=candidate.left_key,
                    normalized_name=candidate.left_key_normalized,
                ),
                ColumnRef(
                    table_ref=right_alias,
                    column_name=candidate.right_key,
                    normalized_name=candidate.right_key_normalized,
                ),
            )],
            relationship_ref=candidate.candidate_id,
        )
        steps.append(join_step)

        # ── 5. 聚合 + CASE WHEN + 窗口 + 投影 / 透传投影 ──
        if is_final:
            # ── CASE WHEN 步骤（pre/post aggregate 分流）──
            #     链路径也必须支持 label_table 的 CASE WHEN 标签规则，
            #     与 _build_single_table / _build_multi_table 保持一致。
            all_case_when_steps = self._build_case_when_steps(spec)
            pre_agg_cw = [s for s in all_case_when_steps
                           if s.evaluation_phase == "pre_aggregate"]
            post_agg_cw = [s for s in all_case_when_steps
                            if s.evaluation_phase != "pre_aggregate"]
            steps.extend(pre_agg_cw)

            # pre-aggregate CASE WHEN 输出列 → 自动加入 group_by
            pre_agg_cw_aliases: set[str] = {
                str(s.alias) for s in pre_agg_cw if s.alias
            }

            if spec.metrics:
                # 链步骤中数据来自 temp 表（left_source）+ 右表 Join 后。
                # 仅右表别名在 FROM 子句中有效——原始表别名已被 temp 表替代。
                # valid_table_aliases={right_alias}：
                #   列解析到 right_alias → 使用（该表在 FROM 中）
                #   列解析到其他别名 → 回退至 primary_table（即 left_source/temp 表）
                agg = self._build_aggregate_step(
                    spec, left_source,
                    extra_group_keys=pre_agg_cw_aliases,
                    valid_table_aliases={right_alias},
                )
                steps.append(agg)

            # post-aggregate CASE WHEN——标签列在聚合后计算（条件可引用聚合指标）
            steps.extend(post_agg_cw)

            # 窗口函数（聚合后、投影前）
            window = self._build_window_step(spec, left_source)
            if window and not spec.ratio_metrics:
                steps.append(window)
                steps.extend(self._build_post_window_filter_steps(spec))
            # 构建 per-column table_ref 覆盖——
            # 多跳链中左源为 temp 表、右表为新 Join 的维度表。
            # 维度声明的 source_table 匹配 right_alias 时使用右表别名，
            # 其余列默认使用 left_source（temp 表）。
            col_overrides: dict[str, str] = {}
            for d in spec.dimensions:
                if d.source_table and d.source_table == right_alias:
                    col_overrides[d.dimension_name] = right_alias
                elif d.source_table:
                    col_overrides[d.dimension_name] = left_source
            project = self._build_project_step(
                spec, default_table_ref=left_source,
                column_table_overrides=col_overrides,
            )
            # 排除窗口别名——避免 SELECT 中重复
            if window:
                win_aliases = {str(w.alias) for w in window.window_exprs if w.alias}
                filtered_cols = [
                    c for c in project.columns if c.alias not in win_aliases
                ]
                project = ProjectStep(step_id=project.step_id, columns=filtered_cols)
            steps.append(project)
            if spec.output_spec.sort:
                steps.append(self._build_sort_step(spec))
            if spec.output_spec.limit is not None:
                steps.append(LimitStep(
                    step_id=SqlBuildPlan.generate_step_id(
                        "limit", {"limit": spec.output_spec.limit}
                    ),
                    limit=spec.output_spec.limit,
                ))
        else:
            # 中间步骤：透传投影——输出全部已 Join 表的全部列
            proj_cols = self._build_chain_pass_through_columns(table_map, joined_tables)
            steps.append(ProjectStep(
                step_id=SqlBuildPlan.generate_step_id(
                    "project", {"chain": chain_id, "step": step_index, "intermediate": True}
                ),
                columns=proj_cols,
            ))

        plan_id = f"plan_{spec.spec_hash[:12]}_{chain_id}_{step_index}"
        return SqlBuildPlan(
            plan_id=plan_id,
            spec_hash=spec.spec_hash,
            hypothesis_id=hypothesis.hypothesis_id,
            source_manifest_hash=hypothesis.source_manifest_hash,
            steps=steps,
            multi_table=True,
        )

    def _build_temp_scan_columns(
        self, table_map: dict, prev_table_aliases: list[str]
    ) -> list[ColumnRef]:
        """为 _temp 表扫描构建列定义——从之前已 Join 表的声明中收集全部列。"""
        cols: list[ColumnRef] = []
        seen: set[str] = set()
        for alias in prev_table_aliases:
            table = table_map.get(alias)
            if not table:
                continue
            for col_list in [table.columns, table.key_columns, table.business_columns]:
                for col in col_list:
                    if col.normalized_name not in seen:
                        seen.add(col.normalized_name)
                        cols.append(ColumnRef(
                            table_ref=alias,
                            column_name=col.column_name,
                            normalized_name=col.normalized_name,
                        ))
        return cols

    def _build_chain_pass_through_columns(
        self, table_map: dict, joined_tables: list[str]
    ) -> list[AliasExpr]:
        """为中间 Plan 构建透传投影——全部已 Join 表的全部列作为输出。"""
        proj_cols: list[AliasExpr] = []
        seen: set[str] = set()
        for alias in joined_tables:
            table = table_map.get(alias)
            if not table:
                continue
            for col_list in [table.columns, table.key_columns, table.business_columns]:
                for col in col_list:
                    if col.normalized_name not in seen:
                        seen.add(col.normalized_name)
                        proj_cols.append(AliasExpr(
                            expression=ColumnRef(
                                table_ref=alias,
                                column_name=col.column_name,
                                normalized_name=col.normalized_name,
                            ),
                            alias=col.column_name,
                        ))
        return proj_cols

    # ── Step 构建辅助 ──

    def _build_time_range_filter(
        self, spec: ParsedDeveloperSpec, table_alias: str,
    ) -> list[FilterStep]:
        """从 TimeRangeDecl 构建时间范围 FilterStep 列表——支持财年、相对日期和固定起止。

        三种模式（优先级递减）：
        1. relative_range: "last_7d" → [col >= CURRENT_DATE - INTERVAL 7 DAY]（SQL 表达式）
        2. calendar_type: "fiscal_jul"/"fiscal_apr" + fiscal_year
           → [col >= start, col < end+1day]（半开区间）
        3. start + end（默认）
           → YYYY-MM-DD 格式：[col >= start, col < end+1day]（半开区间）
           → 非 YYYY-MM-DD：[col BETWEEN start AND end]（保留原有行为）

        relative_range 与 start/end 互斥——relative_range 优先。

        column_ref 解析优先级：tr.column_ref > 对应 InputTableDecl.time_field。

        半开区间 end+1day 在构建阶段用 date.fromisoformat() + timedelta(days=1)
        确定性计算，Compiler/Mapper 只渲染 IR，不做边界修正。

        Args:
            spec: 已解析的 DeveloperSpec
            table_alias: 表别名（用于 ColumnRef.table_ref）

        Returns:
            FilterStep 列表（无有效配置时为空列表）
        """
        tr = spec.time_range
        if tr is None:
            return []

        # 解析时间列名：优先用 time_range.column_ref，为空时回退到表声明的 time_field
        column_ref = tr.column_ref
        if not column_ref:
            # 从 input_tables 中查找匹配的表，取其 time_field
            for t in spec.input_tables:
                if t.table_alias == table_alias:
                    column_ref = t.time_field or ""
                    break

        if not column_ref:
            # 无法确定时间列——无法构建过滤条件
            return []

        normalized = self._normalizer.normalize(column_ref)

        def _col_ref() -> ColumnRef:
            """构建当前上下文的 ColumnRef。"""
            return ColumnRef(
                table_ref=table_alias,
                column_name=column_ref,
                normalized_name=normalized,
            )

        # ── 模式 1：相对日期范围（relative_range 优先）──
        if tr.relative_range:
            interval_days = {
                "last_7d": 7, "last_30d": 30, "last_90d": 90,
            }
            if tr.relative_range in interval_days:
                days = interval_days[tr.relative_range]
                right_expr = SqlLiteral(
                    value=f"CURRENT_DATE - INTERVAL {days} DAY",
                    is_sql_expr=True,
                )
                return [FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": column_ref,
                        "op": "GTE",
                        "relative_range": tr.relative_range,
                    }),
                    predicate=Predicate(
                        left=_col_ref(),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )]
            elif tr.relative_range == "mtd":
                right_expr = SqlLiteral(
                    value="DATE_TRUNC('month', CURRENT_DATE)",
                    is_sql_expr=True,
                )
                return [FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": column_ref,
                        "op": "GTE",
                        "relative_range": "mtd",
                    }),
                    predicate=Predicate(
                        left=_col_ref(),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )]
            elif tr.relative_range == "ytd":
                right_expr = SqlLiteral(
                    value="DATE_TRUNC('year', CURRENT_DATE)",
                    is_sql_expr=True,
                )
                return [FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": column_ref,
                        "op": "GTE",
                        "relative_range": "ytd",
                    }),
                    predicate=Predicate(
                        left=_col_ref(),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )]

        # ── 模式 2：财年日期计算（半开区间：>= start AND < end+1day）──
        if tr.calendar_type != "calendar" and tr.fiscal_year is not None:
            fy = tr.fiscal_year
            if tr.calendar_type == "fiscal_jul":
                start_date = f"{fy}-07-01"
                end_date = f"{fy + 1}-06-30"
            elif tr.calendar_type == "fiscal_apr":
                start_date = f"{fy}-04-01"
                end_date = f"{fy + 1}-03-31"
            else:
                return []

            # 财年日期始终是 YYYY-MM-DD 格式，确定性计算 end+1day
            end_plus_one = _add_one_day(end_date)
            return [
                FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": column_ref,
                        "op": "GTE",
                        "calendar_type": tr.calendar_type,
                        "fiscal_year": tr.fiscal_year,
                    }),
                    predicate=Predicate(
                        left=_col_ref(),
                        operator=PredicateOperator.GTE,
                        right=SqlLiteral(value=start_date),
                    ),
                ),
                FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": column_ref,
                        "op": "LT",
                        "calendar_type": tr.calendar_type,
                        "fiscal_year": tr.fiscal_year,
                    }),
                    predicate=Predicate(
                        left=_col_ref(),
                        operator=PredicateOperator.LT,
                        right=SqlLiteral(value=end_plus_one),
                    ),
                ),
            ]

        # ── 模式 3：固定起止日期 ──
        if tr.start and tr.end:
            # YYYY-MM-DD 格式 → 半开区间 col >= start AND col < end+1day
            if _is_date_only(tr.start) and _is_date_only(tr.end):
                end_plus_one = _add_one_day(tr.end)
                return [
                    FilterStep(
                        step_id=SqlBuildPlan.generate_step_id("filter", {
                            "table": table_alias,
                            "col": column_ref,
                            "op": "GTE",
                        }),
                        predicate=Predicate(
                            left=_col_ref(),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value=tr.start),
                        ),
                    ),
                    FilterStep(
                        step_id=SqlBuildPlan.generate_step_id("filter", {
                            "table": table_alias,
                            "col": column_ref,
                            "op": "LT",
                        }),
                        predicate=Predicate(
                            left=_col_ref(),
                            operator=PredicateOperator.LT,
                            right=SqlLiteral(value=end_plus_one),
                        ),
                    ),
                ]
            # 非 YYYY-MM-DD 格式（含时间组件）→ 保留 BETWEEN 语义
            return [FilterStep(
                step_id=SqlBuildPlan.generate_step_id("filter", {
                    "table": table_alias,
                    "col": column_ref,
                    "op": "BETWEEN",
                }),
                predicate=Predicate(
                    left=_col_ref(),
                    operator=PredicateOperator.BETWEEN,
                    right=[
                        SqlLiteral(value=tr.start),
                        SqlLiteral(value=tr.end),
                    ],
                ),
            )]

        return []

    def _build_required_columns(
        self, table_alias: str, spec: ParsedDeveloperSpec, table: InputTableDecl,
    ) -> list[ColumnRef]:
        """从 spec 的指标和维度引用中推断需要的列——仅包含属于该表的列。

        构建该表已声明列的归一化集合，所有候选列引用（指标 input_column、
        维度 column_ref、排序列、输出列源列）必须通过此集合过滤，
        防止将其他表的列错误地分配给当前表的 ScanStep。
        """
        # 该表所有已声明列名的归一化集合（来自 key_columns + business_columns + columns）
        declared: set[str] = set()
        for col_list in [table.key_columns, table.business_columns, table.columns]:
            for c in col_list:
                declared.add(self._normalizer.normalize(c.column_name))

        seen: set[str] = set()
        cols: list[ColumnRef] = []

        def _add(col_name: str) -> None:
            normalized = self._normalizer.normalize(col_name)
            if normalized in declared and normalized not in seen:
                seen.add(normalized)
                cols.append(
                    ColumnRef(
                        table_ref=table_alias,
                        column_name=col_name,
                        normalized_name=normalized,
                    )
                )

        # 指标引用——仅保留 input_column 属于该表的指标
        for m in spec.metrics:
            if m.input_column:
                _add(m.input_column)

        # 维度引用——有 source_table 时严格按表过滤，无时靠列名匹配
        for d in spec.dimensions:
            if d.source_table:
                if d.source_table == table_alias:
                    _add(d.column_ref)
            else:
                _add(d.column_ref)

        # 排序引用
        if spec.output_spec.sort:
            for s in spec.output_spec.sort:
                _add(s.column)

        # 输出列的源列——若输出列名匹配维度声明，
        # 使用维度的 column_ref（源列名）而非输出列名（别名）
        dim_col_map: dict[str, str] = {
            d.dimension_name: d.column_ref for d in spec.dimensions
        }
        for col in spec.output_spec.columns:
            source_col = dim_col_map.get(col.name, col.name)
            _add(source_col)

        return cols

    def _build_filter_step(self, filter_decl, table_alias: str) -> FilterStep:
        """从 FilterDecl 构建 FilterStep。"""
        operator_map = {
            "=": PredicateOperator.EQ,
            "!=": PredicateOperator.NEQ,
            ">": PredicateOperator.GT,
            "<": PredicateOperator.LT,
            ">=": PredicateOperator.GTE,
            "<=": PredicateOperator.LTE,
            "IN": PredicateOperator.IN,
            "BETWEEN": PredicateOperator.BETWEEN,
            "IS_NULL": PredicateOperator.IS_NULL,
            "IS_NOT_NULL": PredicateOperator.IS_NOT_NULL,
        }
        op = operator_map.get(filter_decl.operator, PredicateOperator.EQ)
        normalized = self._normalizer.normalize(filter_decl.column_ref)

        right = None
        if filter_decl.value is not None:
            if isinstance(filter_decl.value, list):
                right = [SqlLiteral(value=v) for v in filter_decl.value]
            else:
                right = SqlLiteral(value=filter_decl.value)

        step_id_content = {
            "table": table_alias,
            "col": filter_decl.column_ref,
            "op": str(op.value),
        }
        return FilterStep(
            step_id=SqlBuildPlan.generate_step_id("filter", step_id_content),
            predicate=Predicate(
                left=ColumnRef(
                    table_ref=table_alias,
                    column_name=filter_decl.column_ref,
                    normalized_name=normalized,
                ),
                operator=op,
                right=right,
            ),
        )

    def _build_aggregate_step(
        self, spec: ParsedDeveloperSpec, primary_table: str,
        extra_group_keys: set[str] | None = None,
        valid_table_aliases: set[str] | None = None,
    ) -> AggregateStep:
        """从 spec 构建 AggregateStep——支持派生维度 DerivedGroupKey 和 DatePartExpression。

        Args:
            spec: 已解析的 DeveloperSpec
            primary_table: 主表别名（默认 table_ref，当消歧失败时回退到此值）
            extra_group_keys: pre-aggregate CASE WHEN 派生列名——
                这些列不是源表物理列，由 CaseWhenStep 在聚合前产生，
                需要作为 GROUP BY 键（不带 table_ref）。
            valid_table_aliases: 当前 FROM 子句中有效的表别名集合——
                None 表示所有别名均有效（向后兼容）；
                空集表示仅 primary_table 有效（等效于旧 ignore_source_table=True）；
                非空集时仅接受匹配的解析结果，不匹配则回退至 primary_table。
        """
        # group_keys 从 dimensions 构建
        group_cols: list[ColumnRef | DatePartExpression | DerivedGroupKey] = []
        for d in spec.dimensions:
            if extra_group_keys and d.dimension_name in extra_group_keys:
                # CASE 输出是上游派生列；同名维度不能重新解释为源表物理列。
                continue
            normalized = self._normalizer.normalize(d.column_ref)
            # 多表场景下表别名消歧优先级：
            # 1) LLM 标注的 source_table（解析为别名）
            # 2) 从 input_tables 列声明中扫描（兜底）
            # 3) primary_table（以上均未命中或解析结果不在有效别名集合中）
            # valid_table_aliases 机制替代旧的 ignore_source_table 布尔开关——
            # None=全部有效，空集=仅 primary_table 有效，非空集=仅指定别名有效
            dim_table_ref = primary_table
            if d.source_table:
                resolved = self._resolve_derived_source_table(d.source_table, spec)
                if valid_table_aliases is None or resolved in valid_table_aliases:
                    dim_table_ref = resolved
            if dim_table_ref == primary_table:
                # source_table 未设置或解析结果不在有效别名集合中——
                # 尝试列扫描兜底（多表路径下 primary_table 可能为空或 _temp_ 表）
                resolved = self._resolve_column_source_table(d.column_ref, spec)
                if resolved and (valid_table_aliases is None or resolved in valid_table_aliases):
                    dim_table_ref = resolved
            source = ColumnRef(
                table_ref=dim_table_ref,
                column_name=d.column_ref,
                normalized_name=normalized,
            )
            if d.date_part:
                group_cols.append(
                    DatePartExpression(
                        part=d.date_part,
                        column=source,
                        alias=SafeIdentifier(d.dimension_name),
                    )
                )
            else:
                group_cols.append(source)

        # 派生维度 → DerivedGroupKey（含 TimeTransformExpr）
        # 去重：若 dimensions 中已有同名 DatePartExpression（相同语义、不同模型），
        # DerivedGroupKey 会被 Contract→Mapper→Compiler 链路重复追加为
        # time_transform，导致 Spark 的 groupBy/agg 中出现重复列引用。
        existing_aliases = {
            g.alias
            for g in group_cols
            if isinstance(g, (DatePartExpression, DerivedGroupKey))
        }
        for dd in spec.derived_dimensions:
            if dd.dimension_name in existing_aliases:
                # 去重：dimensions.date_part 已生成同名 DatePartExpression，
                # 但 CASE WHEN 条件中的 TimeTransformExpr 反向查找 alias
                # 需要此条目。保留为影子条目——不参与 SQL/Spark 渲染，
                # 仅供 Contract 提取器的 derived_expr_map 使用。
                dgk = DerivedGroupKey(
                    alias=dd.dimension_name,
                    expr=TimeTransformExpr(
                        source_column=SafeIdentifier(dd.source_column),
                        source_table=SafeIdentifier(
                            self._resolve_derived_source_table(dd.source_table, spec)
                        ),
                        time_function=dd.time_function,
                    ),
                )
                dgk._shadow = True
                group_cols.append(dgk)
                continue
            # 派生维度渲染时解析 source_table——若 valid_table_aliases 约束有效别名集合，
            # 解析结果不在有效集合中时回退至 primary_table。
            _dd_resolved = self._resolve_derived_source_table(dd.source_table, spec)
            if valid_table_aliases is not None and _dd_resolved not in valid_table_aliases:
                _dd_table = primary_table
            else:
                _dd_table = _dd_resolved
            group_cols.append(DerivedGroupKey(
                alias=dd.dimension_name,
                expr=TimeTransformExpr(
                    source_column=SafeIdentifier(dd.source_column),
                    source_table=SafeIdentifier(_dd_table),
                    time_function=dd.time_function,
                ),
            ))

        # 如果 output_spec.grain 提供了额外粒度键
        # 去重逻辑兼容 ColumnRef.normalized_name 和 DerivedGroupKey.alias
        existing_grains = {
            g.normalized_name if isinstance(g, ColumnRef) else g.alias
            for g in group_cols
        }
        for grain_col in spec.output_spec.grain:
            if extra_group_keys and grain_col in extra_group_keys:
                # 同名 CASE 输出由下方派生键分支统一加入，禁止绑定到源表。
                continue
            normalized = self._normalizer.normalize(grain_col)
            if normalized not in existing_grains:
                existing_grains.add(normalized)
                # 检查是否为 date_part 派生列——需 DatePartExpression 而非裸 ColumnRef
                derived = next(
                    (d for d in spec.dimensions if d.dimension_name == grain_col),
                    None,
                )
                if derived and derived.date_part:
                    # LLM 输出的 source_table 可能是物理表名，需映射为表别名。
                    grain_table_ref = primary_table
                    if derived.source_table:
                        grain_table_ref = self._resolve_derived_source_table(
                            derived.source_table, spec
                        )
                    group_cols.append(
                        DatePartExpression(
                            part=derived.date_part,
                            column=ColumnRef(
                                table_ref=grain_table_ref,
                                column_name=derived.column_ref,
                                normalized_name=self._normalizer.normalize(
                                    derived.column_ref
                                ),
                            ),
                            alias=SafeIdentifier(derived.dimension_name),
                        )
                    )
                    continue
                # 多表场景下表别名消歧——grain 列通过以下优先级获知表别名：
                # 1) 匹配 dimension 的 source_table（LLM 已标注）
                # 2) 从 input_tables 的列声明中查找（兜底扫描）
                # 3) primary_table（以上均未命中或解析结果不在有效别名集合中）
                grain_table_ref = primary_table
                if derived and derived.source_table:
                    resolved = self._resolve_derived_source_table(
                        derived.source_table, spec
                    )
                    if valid_table_aliases is None or resolved in valid_table_aliases:
                        grain_table_ref = resolved
                if grain_table_ref == primary_table:
                    # 上述未命中——扫描源表列声明兜底
                    resolved = self._resolve_column_source_table(
                        grain_col, spec,
                    )
                    if resolved and (valid_table_aliases is None or resolved in valid_table_aliases):
                        grain_table_ref = resolved
                group_cols.append(
                    ColumnRef(
                        table_ref=grain_table_ref,
                        column_name=grain_col,
                        normalized_name=normalized,
                    )
                )

        # pre-aggregate CASE WHEN 派生列——这些列在聚合前由 CaseWhenStep 计算，
        # 需要作为 GROUP BY 键（不带 table_ref，因为它们是派生列而非源表物理列）
        def _group_has_key(g, key: str) -> bool:
            """检查分组键是否等于 key——兼容 ColumnRef / DatePartExpression / DerivedGroupKey。"""
            if isinstance(g, ColumnRef):
                return g.column_name == key or g.normalized_name == key
            # DatePartExpression 和 DerivedGroupKey 都有 alias
            return g.alias == key
        if extra_group_keys:
            for key in sorted(extra_group_keys):
                if not any(
                    _group_has_key(g, key)
                    for g in group_cols
                ):
                    normalized = self._normalizer.normalize(key)
                    group_cols.append(ColumnRef(
                        table_ref="",  # 派生列无 table_ref
                        column_name=SafeIdentifier(key),
                        normalized_name=SafeIdentifier(normalized),
                    ))

        # metrics——展开 MetricDecl + variants 为多个 AggregateSpec
        agg_metrics: list[AggregateSpec] = []
        for m in spec.metrics:
            agg_metrics.extend(self._expand_metric_to_agg_specs(
                m, source_table=primary_table,
            ))

        # step_id hash 用 alias 表示派生组键（DerivedGroupKey 无 normalized_name）
        group_key_names = [
            g.normalized_name if isinstance(g, ColumnRef) else g.alias
            for g in group_cols
        ]
        step_id_content = {
            "groups": group_key_names,
            "metrics": [m.alias for m in agg_metrics],
        }
        return AggregateStep(
            step_id=SqlBuildPlan.generate_step_id("aggregate", step_id_content),
            group_keys=group_cols,
            metrics=agg_metrics,
        )

    def _build_project_step(
        self, spec: ParsedDeveloperSpec, default_table_ref: str = "",
        column_table_overrides: dict[str, str] | None = None,
    ) -> ProjectStep:
        """从 spec.output_spec 构建 ProjectStep。

        列解析优先级：
        1. 维度声明匹配——若某输出列的 name 等于某个 dimension 的 dimension_name，
           使用 dimension.column_ref 作为源列名、dimension.source_table 作为 table_ref
        2. column_table_overrides——调用方按列名指定的 table_ref 覆盖（优先级高于维度）
        3. default_table_ref——兜底表别名

        Args:
            spec: 已解析的 DeveloperSpec
            default_table_ref: 列引用的默认表别名——合流步骤中用于消除列歧义
            column_table_overrides: 列名→table_ref 的覆盖映射——多跳链最终步骤使用
        """
        # ── 构建维度名→(源列, 源表) 的映射 ──
        dim_source_map: dict[str, tuple[str, str]] = {}
        for d in spec.dimensions:
            # LLM 输出的 source_table 可能是物理表名，需映射为表别名。
            table_ref = (
                self._resolve_derived_source_table(d.source_table, spec)
                if d.source_table
                else ""
            )
            dim_source_map[d.dimension_name] = (d.column_ref, table_ref)

        overrides = column_table_overrides or {}
        label_output_names = {rule.output_column for rule in spec.label_rules}
        label_output_names.update(
            rule.output_column for rule in spec.case_when_rules
        )
        ratio_map = {
            ratio.output_alias: ratio for ratio in spec.ratio_metrics
        }

        proj_cols: list[AliasExpr] = []
        for col in spec.output_spec.columns:
            col_name = col.name
            ratio = ratio_map.get(col_name)
            if ratio is not None:
                proj_cols.append(AliasExpr(
                    expression=RatioExpr(
                        numerator_alias=ratio.numerator_alias,
                        denominator_alias=ratio.denominator_alias,
                        zero_division=ratio.zero_division,
                        multiplier=ratio.multiplier,
                    ),
                    alias=col_name,
                ))
                continue
            # 解析源列名和表别名
            if col_name in label_output_names:
                source_col = col_name
                table_ref = ""
            elif col_name in dim_source_map:
                source_col, dim_table = dim_source_map[col_name]
                # 维度声明的 source_table 可被 overrides 覆盖
                table_ref = overrides.get(col_name, dim_table or default_table_ref)
            else:
                source_col = col_name
                table_ref = overrides.get(col_name, default_table_ref)
            normalized = self._normalizer.normalize(source_col)
            dimension = next(
                (d for d in spec.dimensions if d.dimension_name == col_name),
                None,
            )
            source_ref = ColumnRef(
                table_ref=table_ref,
                column_name=source_col,
                normalized_name=normalized,
            )
            if dimension and dimension.date_part and spec.metrics:
                # 聚合步骤已经以派生别名输出该分组键，最终投影只引用结果列。
                expression = ColumnRef(
                    table_ref="",
                    column_name=SafeIdentifier(col_name),
                    normalized_name=SafeIdentifier(
                        self._normalizer.normalize(col_name)
                    ),
                )
            else:
                expression = (
                    DatePartExpression(
                        part=dimension.date_part,
                        column=source_ref,
                        alias=SafeIdentifier(col_name),
                    )
                    if dimension and dimension.date_part
                    else source_ref
                )
            proj_cols.append(
                AliasExpr(
                    expression=expression,
                    alias=col_name,
                )
            )

        step_id_content = {"columns": [c.name for c in spec.output_spec.columns]}
        return ProjectStep(
            step_id=SqlBuildPlan.generate_step_id("project", step_id_content),
            columns=proj_cols,
        )

    def _build_sort_step(self, spec: ParsedDeveloperSpec) -> SortStep:
        """从 spec.output_spec.sort 构建 SortStep。"""
        sort_specs: list[SortSpec] = []
        for s in spec.output_spec.sort:
            sort_specs.append(
                SortSpec(
                    column=s.column,
                    direction=s.direction,
                )
            )

        step_id_content = {
            "columns": [s.column for s in sort_specs],
        }
        return SortStep(
            step_id=SqlBuildPlan.generate_step_id("sort", step_id_content),
            order_by=sort_specs,
            requires_full_sort=spec.output_spec.limit is None,
        )
