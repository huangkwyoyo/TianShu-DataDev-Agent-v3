"""SqlBuildPlan——8 Step 类型化 IR + SqlBuildPlanBuilder（Fake，确定性）。

每个 step 使用 step_type: Literal["scan"|...] 作为 Pydantic discriminated union 的判别器。
禁止任何自由 SQL 字段（raw_sql / where_sql / join_on: str / expression: str）。
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal, Union

from pydantic import Field

from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
from tianshu_datadev.developer_spec.models import (
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
    SortSpec,
    SqlLiteral,
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
    table_ref: str  # SourceManifest 中注册的表引用
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
    right_table_ref: str  # 被 Join 的右表引用
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
    """

    step_type: Literal["case_when"] = "case_when"
    step_id: str
    cases: list = []  # Phase 3B 填充 WhenBranch 列表
    else_value: SqlLiteral | None = None
    alias: str = ""


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
        SortStep,
        LimitStep,
    ],
    Field(discriminator="step_type"),
]


# ════════════════════════════════════════════
# SqlBuildPlan
# ════════════════════════════════════════════


class SqlBuildPlan(StrictModel):
    """类型安全的 SQL 构建计划——8 Step IR 的有序序列。

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
# SqlBuildPlanBuilder（Fake，确定性）
# ════════════════════════════════════════════


class SqlBuildPlanBuilder:
    """Phase 1B 确定性 SqlBuildPlan 构建器（Fake 实现）。

    构建策略：
    - 单表：Scan → (Filter*) → (Aggregate) → Project → (Sort) → (Limit)
    - 两表 Join：Scan(L) → Scan(R) → (Filter*) → Join → (Aggregate) → Project → (Sort) → (Limit)
    - WEAK/NONE Join 被硬门禁拦截在上层，不会到达此 Builder

    Phase 1B 仅支持单表和一个 Join 的两表场景。
    """

    def __init__(self, normalizer: FieldNormalizer | None = None):
        """初始化 Builder。

        Args:
            normalizer: 字段名归一化器，用于 ColumnRef.normalized_name 填充
        """
        self._normalizer = normalizer or FieldNormalizer()

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

        # 校验 hypothesis
        if is_multi and hypothesis is None:
            raise ValueError("多表 spec 必须提供 RelationshipHypothesis")
        if hypothesis and hypothesis.spec_hash != spec.spec_hash:
            raise ValueError("hypothesis.spec_hash 与 spec.spec_hash 不匹配")

        if not is_multi:
            steps = self._build_single_table(spec)
        else:
            steps = self._build_multi_table(spec, hypothesis)  # type: ignore[arg-type]

        plan = SqlBuildPlan(
            plan_id=SqlBuildPlan.generate_plan_id(spec.spec_hash),
            spec_hash=spec.spec_hash,
            hypothesis_id=hypothesis.hypothesis_id if hypothesis else None,
            source_manifest_hash=hypothesis.source_manifest_hash if hypothesis else None,
            steps=steps,
            multi_table=is_multi,
        )

        return plan, []

    # ── 单表路径 ──

    def _build_single_table(self, spec: ParsedDeveloperSpec) -> list[StepNode]:
        """单表构建：Scan → (Filter*) → (Aggregate) → Project → (Sort) → (Limit)。"""
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

        # 3. AggregateStep——如果有指标
        if spec.metrics:
            agg = self._build_aggregate_step(spec, table.table_alias)
            steps.append(agg)

        # 4. ProjectStep——输出列
        project = self._build_project_step(spec)
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

    def _build_multi_table(
        self,
        spec: ParsedDeveloperSpec,
        hypothesis: RelationshipHypothesis,
    ) -> list[StepNode]:
        """两表构建：Scan(L) → Scan(R) → (Filter*) → Join → (Aggregate) → Project → (Sort) → (Limit)。

        Phase 1B 仅处理第一个 Join 候选的两表场景。
        """
        steps: list[StepNode] = []
        table_map = {t.table_alias: t for t in spec.input_tables}

        if not hypothesis.candidates:
            # 无候选 Join——退化为单表处理（取第一个表）
            return self._build_single_table(spec)

        join_candidate = hypothesis.candidates[0]
        left_table = table_map[join_candidate.left_table]
        right_table = table_map[join_candidate.right_table]

        # 1. ScanStep——左表
        left_cols = self._build_required_columns(left_table.table_alias, spec)
        left_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan_l", {"table": left_table.source_table}),
            table_ref=left_table.table_alias,
            required_columns=left_cols,
            estimated_row_count=left_table.row_count,
        )
        steps.append(left_scan)

        # 2. ScanStep——右表
        right_cols = self._build_required_columns(right_table.table_alias, spec)
        right_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id("scan_r", {"table": right_table.source_table}),
            table_ref=right_table.table_alias,
            required_columns=right_cols,
            estimated_row_count=right_table.row_count,
        )
        steps.append(right_scan)

        # 3. FilterSteps——两表的预过滤
        for t in [left_table, right_table]:
            for f in t.filters:
                filter_step = self._build_filter_step(f, t.table_alias)
                steps.append(filter_step)

        # 4. JoinStep——基于 JoinCandidate
        join_step = JoinStep(
            step_id=SqlBuildPlan.generate_step_id("join", {
                "left": join_candidate.left_table,
                "right": join_candidate.right_table,
                "left_key": join_candidate.left_key_normalized,
                "right_key": join_candidate.right_key_normalized,
            }),
            right_table_ref=join_candidate.right_table,
            join_type=join_candidate.join_type,
            join_keys=[
                (
                    ColumnRef(
                        table_ref=join_candidate.left_table,
                        column_name=join_candidate.left_key,
                        normalized_name=join_candidate.left_key_normalized,
                    ),
                    ColumnRef(
                        table_ref=join_candidate.right_table,
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
            agg = self._build_aggregate_step(spec, left_table.table_alias)
            steps.append(agg)

        # 6. ProjectStep
        project = self._build_project_step(spec)
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

    # ── Step 构建辅助 ──

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
        for col_name in spec.output_spec.columns:
            _add(col_name)

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

        # metrics
        agg_metrics: list[AggregateSpec] = []
        for m in spec.metrics:
            agg_metrics.append(
                AggregateSpec(
                    aggregation=m.aggregation.value,
                    input_column=m.input_column,
                    alias=m.alias,
                )
            )

        step_id_content = {
            "groups": [g.normalized_name for g in group_cols],
            "metrics": [m.alias for m in agg_metrics],
        }
        return AggregateStep(
            step_id=SqlBuildPlan.generate_step_id("aggregate", step_id_content),
            group_keys=group_cols,
            metrics=agg_metrics,
        )

    def _build_project_step(self, spec: ParsedDeveloperSpec) -> ProjectStep:
        """从 spec.output_spec 构建 ProjectStep。"""
        proj_cols: list[AliasExpr] = []
        for col_name in spec.output_spec.columns:
            normalized = self._normalizer.normalize(col_name)
            proj_cols.append(
                AliasExpr(
                    expression=ColumnRef(
                        table_ref="",  # Project 阶段不再区分表
                        column_name=col_name,
                        normalized_name=normalized,
                    ),
                    alias=col_name,
                )
            )

        step_id_content = {"columns": spec.output_spec.columns}
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
