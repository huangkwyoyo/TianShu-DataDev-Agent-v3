"""SqlBuildPlan——8 Step 类型化 IR + SqlBuildPlanBuilder（确定性）。

每个 step 使用 step_type: Literal["scan"|...] 作为 Pydantic discriminated union 的判别器。
禁止任何自由 SQL 字段（raw_sql / where_sql / join_on: str / expression: str）。

Phase 3B 新增 WindowStep（窗口函数步骤）和 CaseWhenStep 完善。
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal, Union

from pydantic import Field

from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
from tianshu_datadev.developer_spec.models import (
    InferredWindowMetric,
    OpenQuestion,
    ParsedDeveloperSpec,
    StrictModel,
)

from .models import (
    AggregateSpec,
    AliasExpr,
    ColumnRef,
    JoinType,
    Predicate,
    PredicateOperator,
    SafeIdentifier,
    SortDirection,
    SortSpec,
    SqlLiteral,
    WhenBranch,
    WindowExpr,
    WindowFunction,
)
from .relationship_hypothesis import RelationshipHypothesis

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
    group_keys: list[ColumnRef]  # GROUP BY 列
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
                # ── 叶节点：从源表扫描 ──
                table = spec.input_tables[0]
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
                temp_ref = f"_temp_c{chain_id}_{src}"
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
                    temp_ref = f"_temp_c{chain_id}_{src}"
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
                    cw_table_ref = f"_temp_c{chain_id}_{cs.source[0]}"
                case_step = self._build_case_when_from_decl(
                    cs.case_when, cs.step_name, chain_id, source_table=cw_table_ref,
                )
                plan_steps.append(case_step)

            # ── AggregateStep ──
            # 合流步骤有 case_when 时跳过聚合——CASE WHEN 已替代聚合逻辑
            if cs.metrics and not cs.case_when:
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

            # ── ProjectStep ──
            if is_final:
                # 合流步骤：使用第一个源的 _temp 表别名消除列歧义
                proj_table_ref = ""
                if isinstance(cs.source, list) and len(cs.source) > 0:
                    proj_table_ref = f"_temp_c{chain_id}_{cs.source[0]}"
                project = self._build_project_step(spec, default_table_ref=proj_table_ref)
                # 排除 CaseWhenStep 已产出的列——避免 SELECT 中重复
                if cs.case_when and cs.case_when.output_column:
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
                if window:
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
                proj_cols = self._build_compute_step_passthrough(cs)
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

        # 基础指标
        specs.append(AggregateSpec(
            aggregation=m.aggregation,
            input_column=m.input_column,
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
                    input_column=m.input_column,
                    alias=v.alias,
                    filter=v.filter,
                    input_expression=m.input_expression,
                    distinct=m.distinct,
                    source_table=st,
                ))

        return specs

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
            "groups": [g.normalized_name for g in group_cols],
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
        for b in case_when.branches:
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
            # THEN 结果——引用上游分支步骤产出的指标 alias（SQL 表达式直通渲染）
            branches.append(WhenBranch(
                condition=condition,
                result=SqlLiteral(value=b.result_column, is_sql_expr=True),
            ))

        # ELSE 默认值
        else_val = None
        if case_when.else_value is not None:
            else_val = SqlLiteral(value=case_when.else_value)

        step_id_content = {
            "step": step_name,
            "branches": [
                {"col": b.condition_column, "op": b.condition_operator,
                 "val": b.condition_value, "then": b.result_column}
                for b in case_when.branches
            ],
            "else": case_when.else_value,
            "chain": chain_id,
        }
        return CaseWhenStep(
            step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
            cases=branches,
            else_value=else_val,
            alias=case_when.output_column,
        )

    def _build_compute_step_passthrough(self, cs) -> list[AliasExpr]:  # ComputeStep
        """为中间 ComputeStep 构建透传投影——输出 GROUP BY 键 + 所有指标列。

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
                        table_ref="",
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
                        table_ref="",
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
                        table_ref="",
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
                left_source = f"_temp_c{chain_id}_{idx - 1}"

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
        _NO_ARG_FUNCTIONS = frozenset({
            "ROW_NUMBER", "RANK", "DENSE_RANK",
        })
        # NTILE 的 input 为整数 SqlLiteral 而非列引用
        _NTILE_FUNCTIONS = frozenset({"NTILE"})

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

            # 构建 input——根据函数类型选择 ColumnRef 或 SqlLiteral
            win_input: ColumnRef | SqlLiteral | None = None
            if func_name in _NTILE_FUNCTIONS:
                # NTILE(n)——桶数为整数 SqlLiteral
                if iwm.input_column:
                    try:
                        n_buckets = int(iwm.input_column)
                    except ValueError:
                        n_buckets = 0
                    win_input = SqlLiteral(value=n_buckets)
            elif func_name not in _NO_ARG_FUNCTIONS and iwm.input_column:
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

    # ── 单表路径 ──

    def _build_single_table(self, spec: ParsedDeveloperSpec) -> list[StepNode]:
        """单表构建：Scan → (Filter*) → (Aggregate) → (Window) → Project → (Sort) → (Limit)。"""
        steps: list[StepNode] = []
        table = spec.input_tables[0]

        # 1. ScanStep——构建 required_columns
        scan_cols = self._build_required_columns(table.table_alias, spec)
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

        # 2b. 时间范围过滤——TimeRangeDecl → FilterStep（Phase 5 业务日历）
        tr_filter = self._build_time_range_filter(spec, table.table_alias)
        if tr_filter:
            steps.append(tr_filter)

        # 3. AggregateStep——如果有指标
        if spec.metrics:
            agg = self._build_aggregate_step(spec, table.table_alias)
            steps.append(agg)

        # 3b. WindowStep——如果有窗口指标（聚合后、投影前）
        window = self._build_window_step(spec, table.table_alias)
        if window:
            steps.append(window)

        # 4. ProjectStep——输出列（排除窗口函数已产出的别名）
        project = self._build_project_step(spec)
        if window:
            win_aliases = {str(w.alias) for w in window.window_exprs if w.alias}
            filtered_cols = [
                c for c in project.columns
                if c.alias not in win_aliases
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

    def _build_multi_table(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis,
    ) -> list[StepNode]:
        """两表构建：Scan(L) → Scan(R) → (Filter*) → Join → (Aggregate) → (Window) → Project → (Sort) → (Limit)。

        Phase 1B 仅处理第一个 Join 候选的两表场景。
        Phase 5 新增：自引用检测——left_table == right_table 时自动生成不同别名。
        """
        steps: list[StepNode] = []
        table_map = {t.table_alias: t for t in spec.input_tables}

        if not hypothesis.candidates:
            # 无候选 Join——退化为单表处理（取第一个表）
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
        left_cols = self._build_required_columns(left_alias, spec)
        left_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan_l", {"table": left_table.source_table}),
            table_ref=left_alias,
            required_columns=left_cols,
            estimated_row_count=left_table.row_count,
        )
        steps.append(left_scan)

        # 2. ScanStep——右表
        right_cols = self._build_required_columns(right_alias, spec)
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

        # 3b. 时间范围过滤——在 Join 之前下推到左表（Phase 5 业务日历）
        tr_filter = self._build_time_range_filter(spec, left_alias)
        if tr_filter:
            steps.append(tr_filter)

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

        # 5. AggregateStep——如果有指标
        if spec.metrics:
            agg = self._build_aggregate_step(spec, left_alias)
            steps.append(agg)

        # 5b. WindowStep——如果有窗口指标（聚合后、投影前）
        window = self._build_window_step(spec, left_alias)
        if window:
            steps.append(window)

        # 6. ProjectStep（排除窗口函数已产出的别名）
        project = self._build_project_step(spec)
        if window:
            win_aliases = {str(w.alias) for w in window.window_exprs if w.alias}
            filtered_cols = [
                c for c in project.columns
                if c.alias not in win_aliases
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
            left_cols = self._build_required_columns(left_alias, spec)
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
        right_cols = self._build_required_columns(right_alias, spec)
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

        # ── 5. 聚合 + 窗口 + 投影 / 透传投影 ──
        if is_final:
            if spec.metrics:
                agg = self._build_aggregate_step(spec, left_source)
                steps.append(agg)
            # 窗口函数（聚合后、投影前）
            window = self._build_window_step(spec, left_source)
            if window:
                steps.append(window)
            project = self._build_project_step(spec)
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
    ) -> FilterStep | None:
        """从 TimeRangeDecl 构建时间范围 FilterStep——支持财年、相对日期和固定起止。

        三种模式（优先级递减）：
        1. relative_range: "last_7d" → col >= CURRENT_DATE - INTERVAL 7 DAY（SQL 表达式）
        2. calendar_type: "fiscal_jul"/"fiscal_apr" + fiscal_year → col BETWEEN start AND end
        3. start + end（默认）→ col BETWEEN start AND end

        relative_range 与 start/end 互斥——relative_range 优先。

        Args:
            spec: 已解析的 DeveloperSpec
            table_alias: 表别名（用于 ColumnRef.table_ref）

        Returns:
            FilterStep 或 None（spec.time_range 为 None 或无有效配置时）
        """
        tr = spec.time_range
        if tr is None:
            return None

        normalized = self._normalizer.normalize(tr.column_ref)

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
                return FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": tr.column_ref,
                        "op": "GTE",
                        "relative_range": tr.relative_range,
                    }),
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref=table_alias,
                            column_name=tr.column_ref,
                            normalized_name=normalized,
                        ),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )
            elif tr.relative_range == "mtd":
                right_expr = SqlLiteral(
                    value="DATE_TRUNC('month', CURRENT_DATE)",
                    is_sql_expr=True,
                )
                return FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": tr.column_ref,
                        "op": "GTE",
                        "relative_range": "mtd",
                    }),
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref=table_alias,
                            column_name=tr.column_ref,
                            normalized_name=normalized,
                        ),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )
            elif tr.relative_range == "ytd":
                right_expr = SqlLiteral(
                    value="DATE_TRUNC('year', CURRENT_DATE)",
                    is_sql_expr=True,
                )
                return FilterStep(
                    step_id=SqlBuildPlan.generate_step_id("filter", {
                        "table": table_alias,
                        "col": tr.column_ref,
                        "op": "GTE",
                        "relative_range": "ytd",
                    }),
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref=table_alias,
                            column_name=tr.column_ref,
                            normalized_name=normalized,
                        ),
                        operator=PredicateOperator.GTE,
                        right=right_expr,
                    ),
                )

        # ── 模式 2：财年日期计算 ──
        if tr.calendar_type != "calendar" and tr.fiscal_year is not None:
            fy = tr.fiscal_year
            if tr.calendar_type == "fiscal_jul":
                start_date = f"{fy}-07-01"
                end_date = f"{fy + 1}-06-30"
            elif tr.calendar_type == "fiscal_apr":
                start_date = f"{fy}-04-01"
                end_date = f"{fy + 1}-03-31"
            else:
                return None

            return FilterStep(
                step_id=SqlBuildPlan.generate_step_id("filter", {
                    "table": table_alias,
                    "col": tr.column_ref,
                    "op": "BETWEEN",
                    "calendar_type": tr.calendar_type,
                    "fiscal_year": tr.fiscal_year,
                }),
                predicate=Predicate(
                    left=ColumnRef(
                        table_ref=table_alias,
                        column_name=tr.column_ref,
                        normalized_name=normalized,
                    ),
                    operator=PredicateOperator.BETWEEN,
                    right=[
                        SqlLiteral(value=start_date),
                        SqlLiteral(value=end_date),
                    ],
                ),
            )

        # ── 模式 3：固定起止日期（原有逻辑）──
        if tr.start and tr.end:
            return FilterStep(
                step_id=SqlBuildPlan.generate_step_id("filter", {
                    "table": table_alias,
                    "col": tr.column_ref,
                    "op": "BETWEEN",
                }),
                predicate=Predicate(
                    left=ColumnRef(
                        table_ref=table_alias,
                        column_name=tr.column_ref,
                        normalized_name=normalized,
                    ),
                    operator=PredicateOperator.BETWEEN,
                    right=[
                        SqlLiteral(value=tr.start),
                        SqlLiteral(value=tr.end),
                    ],
                ),
            )

        return None

    def _build_required_columns(
        self, table_alias: str, spec: ParsedDeveloperSpec
    ) -> list[ColumnRef]:
        """从 spec 的指标和维度引用中推断需要的列。

        收集所有指标引用（input_column）、维度引用和排序引用，
        构建 ColumnRef 列表。
        """
        seen: set[str] = set()
        cols: list[ColumnRef] = []

        def _add(col_name: str) -> None:
            normalized = self._normalizer.normalize(col_name)
            if normalized not in seen:
                seen.add(normalized)
                cols.append(
                    ColumnRef(
                        table_ref=table_alias,
                        column_name=col_name,
                        normalized_name=normalized,
                    )
                )

        # 指标引用
        for m in spec.metrics:
            if m.input_column:
                _add(m.input_column)

        # 维度引用
        for d in spec.dimensions:
            _add(d.column_ref)

        # 排序引用
        if spec.output_spec.sort:
            for s in spec.output_spec.sort:
                _add(s.column)

        # 输出列的源列
        for col in spec.output_spec.columns:
            _add(col.name)

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
        self, spec: ParsedDeveloperSpec, primary_table: str
    ) -> AggregateStep:
        """从 spec 构建 AggregateStep。"""
        # group_keys 从 dimensions 构建
        group_cols: list[ColumnRef] = []
        for d in spec.dimensions:
            normalized = self._normalizer.normalize(d.column_ref)
            group_cols.append(
                ColumnRef(
                    table_ref=primary_table,
                    column_name=d.column_ref,
                    normalized_name=normalized,
                )
            )

        # 如果 output_spec.grain 提供了额外粒度键
        for grain_col in spec.output_spec.grain:
            normalized = self._normalizer.normalize(grain_col)
            if not any(g.normalized_name == normalized for g in group_cols):
                group_cols.append(
                    ColumnRef(
                        table_ref=primary_table,
                        column_name=grain_col,
                        normalized_name=normalized,
                    )
                )

        # metrics——展开 MetricDecl + variants 为多个 AggregateSpec
        agg_metrics: list[AggregateSpec] = []
        for m in spec.metrics:
            agg_metrics.extend(self._expand_metric_to_agg_specs(
                m, source_table=primary_table,
            ))

        step_id_content = {
            "groups": [g.normalized_name for g in group_cols],
            "metrics": [m.alias for m in agg_metrics],
        }
        return AggregateStep(
            step_id=SqlBuildPlan.generate_step_id("aggregate", step_id_content),
            group_keys=group_cols,
            metrics=agg_metrics,
        )

    def _build_project_step(
        self, spec: ParsedDeveloperSpec, default_table_ref: str = "",
    ) -> ProjectStep:
        """从 spec.output_spec 构建 ProjectStep。

        Args:
            spec: 已解析的 DeveloperSpec
            default_table_ref: 列引用的默认表别名——合流步骤中用于消除列歧义
        """
        proj_cols: list[AliasExpr] = []
        for col in spec.output_spec.columns:
            col_name = col.name
            normalized = self._normalizer.normalize(col_name)
            proj_cols.append(
                AliasExpr(
                    expression=ColumnRef(
                        table_ref=default_table_ref,  # 合流步骤传左表别名消除歧义
                        column_name=col_name,
                        normalized_name=normalized,
                    ),
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
