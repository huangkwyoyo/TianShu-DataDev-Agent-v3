"""SqlBuildPlanValidator——事实源校验 + Join 门禁 + 语义校验 + 架构边界断言。

验证流程（12 项检查）：
1. 空 steps 拒绝
2. 不支持的步骤类型——白名单外 Step 类型一律拒绝（架构边界断言）
3. 多跳 Join 拒绝——单 Plan 内 ≥2 JoinStep → 拒绝（Phase 3C 遗留，Phase 4.6 开放）
4. 表引用校验——所有 ScanStep.table_ref 必须在 SourceManifest 中注册
5. 字段引用校验——所有 ColumnRef 必须在对应表的 columns 中存在
6. Join key 类型兼容——JoinStep 双方字段类型必须兼容
7. WEAK/NONE Join 门禁——不得出现在 JoinStep 中（二次确认）
8. 枚举值校验——CaseWhenStep 枚举值须声明
9. 时间过滤校验——大事实表必须有时间过滤 Predicate
10. 明细查询 LIMIT 校验——无聚合时必须显式 LIMIT
11. 窗口函数校验——白名单 + Frame 合法性 + WHERE 拒绝检测
12. 窗口函数位置校验——窗口函数不得出现在 WHERE/HAVING 子句

返回 (passed: bool, questions: list[OpenQuestion])。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import OpenQuestion, SourceManifest
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinEvidenceLevel,
    RelationshipHypothesis,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    JoinStep,
    ScanStep,
    SqlBuildPlan,
    StepNode,
)

from .models import find_column_type, types_are_compatible

# 大事实表行数阈值——超过此行数视为"大表"
_LARGE_TABLE_ROW_THRESHOLD = 1_000_000  # 100 万行


class SqlBuildPlanValidator:
    """SQL 构建计划验证器——确定性的事实源校验和语义校验。

    Validator 是确定性的——相同输入 → 相同输出。
    所有检查结果记录为 OpenQuestion（blocking=True 时阻断编译）。

    Phase 3B 新增：
    - 窗口函数白名单 + Frame 合法性 + WHERE 拒绝检测
    - CASE WHEN 标签枚举值校验
    """

    def validate(
        self,
        plan: SqlBuildPlan,
        manifest: SourceManifest,
        hypothesis: RelationshipHypothesis | None = None,
        spec: object | None = None,  # ParsedDeveloperSpec——Phase 3B 标签枚举校验
    ) -> tuple[bool, list[OpenQuestion]]:
        """验证 SqlBuildPlan 的正确性和安全性。

        Args:
            plan: 待验证的 SqlBuildPlan
            manifest: 事实源——含注册表、字段、类型信息
            hypothesis: Join 推测——用于 Join 证据等级门禁
            spec: 已解析的 DeveloperSpec（Phase 3B 标签枚举校验）

        Returns:
            (all_passed, open_questions)
            all_passed 为 False 时表示存在 blocking 问题，编译必须中止。
        """
        questions: list[OpenQuestion] = []

        # ── 1. 空 steps 拒绝 ──
        if not plan.steps:
            questions.append(
                OpenQuestion(
                    question_id=f"Q-VAL-EMPTY-{plan.plan_id}",
                    source="validator",
                    field_ref="plan.steps",
                    description="SqlBuildPlan.steps 为空——无法编译",
                    blocking=True,
                )
            )
            return False, questions

        # 构建查询上下文
        ctx = _ValidationContext(plan, manifest, hypothesis)

        # ── 2. 不支持的步骤类型（架构边界断言） ──
        self._validate_unsupported_step_types(ctx, questions)

        # ── 3. 多跳 Join 拒绝（Phase 3C 遗留） ──
        self._validate_multi_hop_join(ctx, questions)

        # ── 4. 表引用校验 ──
        self._validate_table_refs(ctx, questions)

        # ── 5. 字段引用校验 ──
        self._validate_column_refs(ctx, questions)

        # ── 6. Join key 类型兼容 ──
        self._validate_join_key_types(ctx, questions)

        # ── 7. WEAK/NONE Join 门禁（二次确认） ──
        self._validate_join_evidence_gate(ctx, questions)

        # ── 8. 枚举值校验（Phase 3B 实现） ──
        self._validate_enum_values(ctx, questions, spec)

        # ── 9. 时间过滤校验 ──
        self._validate_time_filter(ctx, questions)

        # ── 10. 明细查询 LIMIT 校验 ──
        self._validate_detail_limit(ctx, questions)

        # ── 11. 窗口函数校验（Phase 3B 新增） ──
        self._validate_window_functions(ctx, questions)

        # ── 12. 窗口函数位置校验（Phase 3B 新增） ──
        self._validate_window_position(ctx, questions)

        # ── 13. 粒度完整性校验（Phase 4C 补全——原 known_gap） ──
        self._validate_grain_completeness(ctx, questions, spec)

        # ── 14. 聚合类型声明对比（Phase 4C 补全——原 known_gap） ──
        self._validate_aggregation_declaration(ctx, questions, spec)

        # 有 blocking 问题 → 不通过
        all_passed = not any(q.blocking for q in questions)
        return all_passed, questions

    # ── 检查方法 ──

    def _validate_unsupported_step_types(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """检查所有 step 类型是否在支持的白名单中——架构边界断言。

        Phase 3C 遗留规则：任何不在白名单中的 Step 类型一律拒绝。
        白名单随 Phase 渐进开放——Phase 4.6 计划新增 SubqueryStep。

        当前白名单：ScanStep, FilterStep, JoinStep, AggregateStep,
        ProjectStep, CaseWhenStep, WindowStep, SortStep, LimitStep。
        """
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            CaseWhenStep,
            FilterStep,
            JoinStep,
            LimitStep,
            ProjectStep,
            ScanStep,
            SortStep,
            WindowStep,
        )

        _allowed = (
            ScanStep, FilterStep, JoinStep, AggregateStep,
            ProjectStep, CaseWhenStep, WindowStep, SortStep, LimitStep,
        )

        for step in ctx.plan.steps:
            if not isinstance(step, _allowed):
                step_type_name = type(step).__name__
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-UNSUPPORTED-{step_type_name}",
                        source="validator",
                        field_ref=f"plan.steps.{step.step_id}",
                        description=(
                            f"不支持的步骤类型 '{step_type_name}'——"
                            f"当前白名单: {[t.__name__ for t in _allowed]}。"
                            f"该类型计划在后续 Phase 中开放（详见 Phase 4.6 规划）。"
                        ),
                        blocking=True,
                    )
                )

    def _validate_multi_hop_join(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """检查单 SqlBuildPlan 内 JoinStep 数量——Phase 3C 仅支持单跳 Join。

        V-009d（Phase 4.6 预定义）：同一 SqlBuildPlan 内仅允许一张 JoinStep。
        多跳 Join 应拆入多步 SqlProgram——每步最多一个 JoinStep，
        通过 _temp 表传递中间结果。

        此规则是 Phase 3C 遗留的硬门禁——Phase 4.6 Step 1 实施多跳 Join 时移除。
        """
        join_step_count = len(ctx.join_steps)
        if join_step_count > 1:
            # 收集所有 JoinStep 的 right_table_ref 用于诊断消息
            joined_tables = [s.right_table_ref for s in ctx.join_steps]
            chain = " → ".join(joined_tables)
            questions.append(
                OpenQuestion(
                    question_id=f"Q-VAL-MULTIHOP-{ctx.plan.plan_id}",
                    source="validator",
                    field_ref="plan.steps",
                    description=(
                        f"多跳 Join 不支持——当前 SqlBuildPlan 包含 {join_step_count} 个 "
                        f"JoinStep（右表引用链: {chain}）。"
                        f"当前仅支持单跳 Join（两表关联），多跳 Join 应拆分为 "
                        f"多步 SqlProgram：每步最多一个 JoinStep，通过 _temp 中间表传递结果。"
                        f"Phase 4.6 计划开放多跳 Join（≤5 跳）。"
                    ),
                    blocking=True,
                )
            )

    def _validate_table_refs(self, ctx: _ValidationContext, questions: list[OpenQuestion]) -> None:
        """检查所有 ScanStep.table_ref 在 SourceManifest 中注册。"""
        registered_tables = {t.table_ref for t in ctx.manifest.tables}

        for step in ctx.scan_steps:
            if step.table_ref not in registered_tables:
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-TABLE-{step.step_id}",
                        source="validator",
                        field_ref=f"{step.table_ref}",
                        description=(
                            f"表引用 '{step.table_ref}' 未在 SourceManifest 中注册——"
                            f"已注册表: {sorted(registered_tables)}"
                        ),
                        blocking=True,
                    )
                )

    def _validate_column_refs(self, ctx: _ValidationContext, questions: list[OpenQuestion]) -> None:
        """检查所有 ColumnRef 在对应表的 columns 中存在。"""
        for step in ctx.scan_steps:
            table_cols = ctx.get_table_columns(step.table_ref)
            if table_cols is None:
                continue  # 表引用问题已在 _validate_table_refs 中报告

            # 收集此 step 中所有 column ref
            for col_ref in step.required_columns:
                col_type = find_column_type(
                    table_ref=step.table_ref,
                    column_name=col_ref.column_name,
                    normalized_name=col_ref.normalized_name,
                    columns=table_cols,
                )
                if col_type is None:
                    known_cols = [c.column_name for c in table_cols]
                    questions.append(
                        OpenQuestion(
                            question_id=f"Q-VAL-COL-{step.step_id}-{col_ref.column_name}",
                            source="validator",
                            field_ref=f"{step.table_ref}.{col_ref.column_name}",
                            description=(
                                f"字段 '{step.table_ref}.{col_ref.column_name}' "
                                f"（归一化: {col_ref.normalized_name}）未在 "
                                f"SourceManifest 的 {step.table_ref} 表中找到——"
                                f"已知字段: {known_cols}"
                            ),
                            blocking=True,
                        )
                    )

    def _validate_join_key_types(self, ctx: _ValidationContext, questions: list[OpenQuestion]) -> None:
        """检查 JoinStep 的 join_keys 双方字段类型是否兼容。"""
        for step in ctx.join_steps:
            for left_key, right_key in step.join_keys:
                left_type = ctx.get_column_type(left_key.table_ref, left_key)
                right_type = ctx.get_column_type(step.right_table_ref, right_key)

                if left_type is None or right_type is None:
                    # 字段不存在问题已在 _validate_column_refs 中报告
                    continue

                if not types_are_compatible(left_type, right_type):
                    questions.append(
                        OpenQuestion(
                            question_id=f"Q-VAL-JOINTYPE-{step.step_id}",
                            source="validator",
                            field_ref=f"{left_key.table_ref}.{left_key.column_name} ↔ "
                            f"{step.right_table_ref}.{right_key.column_name}",
                            description=(
                                f"Join key 类型不兼容: "
                                f"{left_key.table_ref}.{left_key.column_name} ({left_type}) ↔ "
                                f"{step.right_table_ref}.{right_key.column_name} ({right_type})——"
                                f"类型 '{left_type}' 与 '{right_type}' 不兼容"
                            ),
                            blocking=True,
                        )
                    )

    def _validate_join_evidence_gate(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """二次确认 WEAK/NONE Join 不出现在 JoinStep 中。

        JoinStep.relationship_ref 应指向 STRONG 或 MEDIUM 证据等级的 JoinCandidate。
        此检查是 Phase 1B 硬门禁的二次确认——如果出现 WEAK/NONE，说明上层过滤有 Bug。
        """
        if ctx.hypothesis is None:
            return

        # 构建 candidate_id → evidence_level 映射
        candidate_levels = {
            c.candidate_id: c.evidence.level if c.evidence else None
            for c in ctx.hypothesis.candidates
        }

        for step in ctx.join_steps:
            ref = step.relationship_ref
            level = candidate_levels.get(ref)

            if level is None:
                # relationship_ref 指向了不存在的候选——可能是硬门禁 Bug
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-GATE-MISSING-{step.step_id}",
                        source="validator",
                        field_ref=ref,
                        description=(
                            f"JoinStep.relationship_ref '{ref}' 在 "
                            f"RelationshipHypothesis 中未找到——可能硬门禁漏拦截"
                        ),
                        blocking=True,
                    )
                )
            elif level in (JoinEvidenceLevel.WEAK, JoinEvidenceLevel.NONE):
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-GATE-{step.step_id}",
                        source="validator",
                        field_ref=ref,
                        description=(
                            f"JoinStep 包含 {level.value} 证据等级的 Join——"
                            f"硬门禁违规。WEAK/NONE Join 不得进入 SqlBuildPlan"
                        ),
                        blocking=True,
                    )
                )

    def _validate_enum_values(
        self,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
        spec: object | None = None,
    ) -> None:
        """检查 CaseWhenStep 的枚举值是否在 DeveloperSpec / SourceManifest 中声明。

        Phase 3B 实现——调用 LabelValidator 进行确定性的枚举值校验。
        """
        from tianshu_datadev.validation.label_validator import validate_label_enums

        # 收集所有 CaseWhenStep
        has_case_when = any(
            isinstance(step, CaseWhenStep) for step in ctx.plan.steps
        )
        if not has_case_when:
            return

        if spec is not None:
            label_questions = validate_label_enums(
                ctx.plan, spec=spec, manifest=ctx.manifest
            )
        else:
            label_questions = validate_label_enums(
                ctx.plan, spec=None, manifest=ctx.manifest
            )
        questions.extend(label_questions)

    def _validate_window_functions(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """校验窗口函数白名单、Frame 合法性和字段引用。

        Phase 3B 新增——调用 WindowValidator 进行窗口函数安全校验。
        """
        from tianshu_datadev.validation.window_validator import (
            validate_window_exprs,
        )

        window_questions = validate_window_exprs(ctx.plan, manifest=ctx.manifest)
        questions.extend(window_questions)

    def _validate_window_position(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """检查窗口函数是否出现在 WHERE / HAVING 子句中。

        Phase 3B 新增——窗口函数只能用于 SELECT 和 ORDER BY。
        """
        from tianshu_datadev.validation.window_validator import (
            validate_window_not_in_where,
        )

        position_questions = validate_window_not_in_where(ctx.plan)
        questions.extend(position_questions)

    # ── Phase 4C 新增：粒度完整性 + 聚合类型声明对比 ──

    def _validate_grain_completeness(
        self,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
        spec: object | None = None,
    ) -> None:
        """检查所有声明的维度列是否出现在 GROUP BY 中。

        从 ParsedDeveloperSpec.dimensions 获取所有声明的维度列引用，
        与计划中 AggregateStep.group_keys 做交集对比。
        缺失的维度列产生 Q-VAL-GRAIN- 拒绝码。

        前置条件：spec 必须提供——若不提供，跳过检查（不误报）。
        """
        if spec is None:
            return
        if not ctx.aggregate_steps:
            return

        # 从 ParsedDeveloperSpec 提取声明的维度列
        declared_dimensions: set[str] = set()
        try:
            dims = getattr(spec, "dimensions", None) or []
            for dim in dims:
                col_ref = getattr(dim, "column_ref", None)
                if col_ref:
                    declared_dimensions.add(col_ref)
        except Exception:
            return  # spec 结构不符预期——跳过，不误报

        if not declared_dimensions:
            return

        # 收集所有 AggregateStep 的 group_keys 中的列名（归一化名）
        for agg_step in ctx.aggregate_steps:
            actual_grain: set[str] = {
                gk.normalized_name for gk in agg_step.group_keys
            }
            missing = declared_dimensions - actual_grain
            if missing:
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"Q-VAL-GRAIN-{agg_step.step_id}"
                        ),
                        source="validator",
                        field_ref=f"{agg_step.step_id}.group_keys",
                        description=(
                            f"分组键不完整——声明维度列 {sorted(missing)} "
                            f"未出现在 GROUP BY 中。"
                            f"当前分组键：{sorted(actual_grain)}"
                        ),
                        blocking=True,
                    )
                )

    def _validate_aggregation_declaration(
        self,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
        spec: object | None = None,
    ) -> None:
        """检查聚合函数类型是否与 DeveloperSpec 声明一致。

        从 ParsedDeveloperSpec.metrics 获取所有声明的指标定义
        （别名 → 聚合类型 + 输入列），与计划中 AggregateStep.metrics
        逐项对比。不匹配产生 Q-VAL-AGG- 拒绝码。

        前置条件：spec 必须提供——若不提供，跳过检查。
        """
        if spec is None:
            return
        if not ctx.aggregate_steps:
            return

        # 从 ParsedDeveloperSpec 提取声明的指标定义
        declared_metrics: dict[str, tuple[str, str | None]] = {}  # alias → (aggregation, input_column)
        try:
            metrics = getattr(spec, "metrics", None) or []
            for m in metrics:
                alias = getattr(m, "alias", None)
                agg_val = getattr(m, "aggregation", None)
                input_col = getattr(m, "input_column", None)
                if alias and agg_val:
                    # AggregationType 是 (str, Enum)——取 .value 得到字符串
                    agg_str = getattr(agg_val, "value", str(agg_val))
                    declared_metrics[alias] = (agg_str, input_col)
        except Exception:
            return  # spec 结构不符预期——跳过

        if not declared_metrics:
            return

        # 收集所有 AggregateStep 的实际聚合指标
        for agg_step in ctx.aggregate_steps:
            for metric in agg_step.metrics:
                alias = metric.alias
                if alias not in declared_metrics:
                    continue  # 未声明的别名——由字段引用校验处理

                declared_agg, declared_col = declared_metrics[alias]
                actual_agg = str(metric.aggregation) if metric.aggregation else ""

                # 对比聚合类型
                if actual_agg != declared_agg:
                    questions.append(
                        OpenQuestion(
                            question_id=(
                                f"Q-VAL-AGG-{agg_step.step_id}-{alias}"
                            ),
                            source="validator",
                            field_ref=f"{agg_step.step_id}.metrics.{alias}",
                            description=(
                                f"聚合函数不匹配——声明为 {declared_agg}({declared_col or '*'})，"
                                f"实际为 {actual_agg}({metric.input_column or '*'})"
                            ),
                            blocking=True,
                        )
                    )
                    continue

                # 对比输入列（当声明了 input_column 且不为空时）
                if declared_col:
                    actual_col = metric.input_column or ""
                    actual_col_normalized = actual_col
                    if actual_col_normalized != declared_col:
                        questions.append(
                            OpenQuestion(
                                question_id=(
                                    f"Q-VAL-AGG-{agg_step.step_id}-{alias}"
                                ),
                                source="validator",
                                field_ref=f"{agg_step.step_id}.metrics.{alias}",
                                description=(
                                    f"聚合输入列不匹配——声明为 "
                                    f"{declared_agg}({declared_col})，"
                                    f"实际为 {actual_agg}({actual_col})"
                                ),
                                blocking=True,
                            )
                        )

    def _validate_time_filter(self, ctx: _ValidationContext, questions: list[OpenQuestion]) -> None:
        """检查大事实表是否包含时间过滤条件。

        规则：role="fact" 且 estimated_row_count > 阈值（100 万行）的表，
        其 ScanStep 或关联的 FilterStep 必须包含对时间字段的过滤。
        """
        for step in ctx.scan_steps:
            table = ctx.get_manifest_table(step.table_ref)
            if table is None:
                continue

            # 只检查事实表
            if table.estimated_row_count is None:
                continue
            if table.estimated_row_count < _LARGE_TABLE_ROW_THRESHOLD:
                continue

            # 检查 ScanStep 自身是否有过滤
            has_time_filter = _has_time_predicate(step.predicates)

            # 检查关联的 FilterStep
            if not has_time_filter:
                for fstep in ctx.filter_steps:
                    if _predicate_refers_to_table(fstep.predicate, step.table_ref):
                        if _has_time_predicate([fstep.predicate]):
                            has_time_filter = True
                            break

            if not has_time_filter:
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-TIME-{step.step_id}",
                        source="validator",
                        field_ref=f"{step.table_ref}",
                        description=(
                            f"大事实表 '{step.table_ref}'（估算 {table.estimated_row_count:,} 行）"
                            f"缺少时间过滤条件——可能触发全表扫描"
                        ),
                        blocking=True,
                    )
                )

    def _validate_detail_limit(self, ctx: _ValidationContext, questions: list[OpenQuestion]) -> None:
        """检查明细查询（无聚合）是否有 LIMIT。

        规则：如果 SqlBuildPlan 不包含 AggregateStep，且不包含 LimitStep，
        则添加 blocking 问题——防止无限制返回明细数据。
        """
        # 有聚合 → 不是明细查询，OK
        if ctx.aggregate_steps:
            return

        # 有 LIMIT 步骤 → OK
        if ctx.limit_steps:
            return

        # 有排序且排序带 limit → OK
        for s in ctx.sort_steps:
            if s.limit is not None and s.limit > 0:
                return

        questions.append(
            OpenQuestion(
                question_id=f"Q-VAL-LIMIT-{ctx.plan.plan_id}",
                source="validator",
                field_ref="plan.steps",
                description=(
                    "明细查询（无聚合）缺少 LIMIT——"
                    "必须显式设置行数限制以防止无限制返回明细数据"
                ),
                blocking=True,
            )
        )


# ════════════════════════════════════════════
# 验证上下文
# ════════════════════════════════════════════


class _ValidationContext:
    """验证上下文——缓存 step 分类和 Manifest 查询结果。"""

    def __init__(
        self,
        plan: SqlBuildPlan,
        manifest: SourceManifest,
        hypothesis: RelationshipHypothesis | None,
    ):
        self.plan = plan
        self.manifest = manifest
        self.hypothesis = hypothesis

        # 按类型分类 steps
        self.scan_steps: list[ScanStep] = []
        self.join_steps: list[JoinStep] = []
        self.filter_steps: list = []
        self.aggregate_steps: list[AggregateStep] = []
        self.sort_steps: list = []
        self.limit_steps: list = []

        for step in plan.steps:
            _classify_step(step, self)

    def get_manifest_table(self, table_ref: str):
        """获取 SourceManifest 中注册的表。"""
        for t in self.manifest.tables:
            if t.table_ref == table_ref:
                return t
        return None

    def get_table_columns(self, table_ref: str) -> list | None:
        """获取注册表的列列表。"""
        table = self.get_manifest_table(table_ref)
        if table is None:
            return None
        return table.columns

    def get_column_type(
        self, table_ref: str, col_ref
    ) -> str | None:
        """获取指定表中字段的数据类型。"""
        columns = self.get_table_columns(table_ref)
        if columns is None:
            return None
        return find_column_type(
            table_ref=table_ref,
            column_name=col_ref.column_name,
            normalized_name=col_ref.normalized_name,
            columns=columns,
        )


def _classify_step(step: StepNode, ctx: _ValidationContext) -> None:
    """将 step 分类到对应的列表。"""
    from tianshu_datadev.planning.sql_build_plan import (
        AggregateStep,
        FilterStep,
        JoinStep,
        LimitStep,
        ScanStep,
        SortStep,
    )

    if isinstance(step, ScanStep):
        ctx.scan_steps.append(step)
    elif isinstance(step, JoinStep):
        ctx.join_steps.append(step)
    elif isinstance(step, FilterStep):
        ctx.filter_steps.append(step)
    elif isinstance(step, AggregateStep):
        ctx.aggregate_steps.append(step)
    elif isinstance(step, SortStep):
        ctx.sort_steps.append(step)
    elif isinstance(step, LimitStep):
        ctx.limit_steps.append(step)
    # Phase 3B 新增步骤类型——不在旧版分类列表中，仅静默跳过
    # CaseWhenStep 和 WindowStep 由专用 Validator 处理


# ════════════════════════════════════════════
# Predicate 分析辅助函数
# ════════════════════════════════════════════


def _has_time_predicate(predicates: list) -> bool:
    """检查谓词列表中是否包含时间相关条件。

    启发式检测：检查谓词的 left 字段是否引用了疑似时间字段名
    （如 date、time、timestamp、dt、ds 等）。
    """
    time_keywords = {
        "date", "time", "timestamp", "datetime", "dt", "ds",
        "event_time", "create_time", "created_at", "updated_at",
        "stat_date", "dt_date", "day", "hour",
    }

    for pred in predicates:
        if _is_time_field(pred, time_keywords):
            return True
    return False


def _is_time_field(pred, time_keywords: set[str]) -> bool:
    """递归检查谓词树中是否包含时间字段引用。"""
    # 检查 left 侧
    left = getattr(pred, "left", None)
    if left is not None:
        col_name = getattr(left, "column_name", "")
        norm_name = getattr(left, "normalized_name", "")
        if any(kw in col_name.lower() for kw in time_keywords):
            return True
        if any(kw in norm_name.lower() for kw in time_keywords):
            return True
        # 递归嵌套 Predicate
        if hasattr(left, "operator"):
            if _is_time_field(left, time_keywords):
                return True

    # 检查 right 侧
    right = getattr(pred, "right", None)
    if right is not None and hasattr(right, "operator"):
        if _is_time_field(right, time_keywords):
            return True

    return False


def _predicate_refers_to_table(pred, table_ref: str) -> bool:
    """检查谓词是否引用了指定表的字段。"""
    left = getattr(pred, "left", None)
    if left is not None:
        left_table = getattr(left, "table_ref", "")
        if left_table == table_ref:
            return True
        if hasattr(left, "operator"):
            if _predicate_refers_to_table(left, table_ref):
                return True
    return False
