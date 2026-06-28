"""SqlBuildPlanValidator——事实源校验 + Join 门禁 + 语义校验。

验证流程（8 项检查）：
1. 空 steps 拒绝
2. 表引用校验——所有 ScanStep.table_ref 必须在 SourceManifest 中注册
3. 字段引用校验——所有 ColumnRef 必须在对应表的 columns 中存在
4. Join key 类型兼容——JoinStep 双方字段类型必须兼容
5. WEAK/NONE Join 门禁——不得出现在 JoinStep 中（二次确认）
6. 枚举值校验——CaseWhenStep 枚举值须声明（Phase 1C 占位）
7. 时间过滤校验——大事实表必须有时间过滤 Predicate
8. LIMIT 存在性——明细查询（无聚合）必须有 LIMIT

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

        # ── 2. 表引用校验 ──
        self._validate_table_refs(ctx, questions)

        # ── 3. 字段引用校验 ──
        self._validate_column_refs(ctx, questions)

        # ── 4. Join key 类型兼容 ──
        self._validate_join_key_types(ctx, questions)

        # ── 5. WEAK/NONE Join 门禁（二次确认） ──
        self._validate_join_evidence_gate(ctx, questions)

        # ── 6. 枚举值校验（Phase 3B 实现） ──
        self._validate_enum_values(ctx, questions, spec)

        # ── 7. 时间过滤校验 ──
        self._validate_time_filter(ctx, questions)

        # ── 8. 明细查询 LIMIT 校验 ──
        self._validate_detail_limit(ctx, questions)

        # ── 9. 窗口函数校验（Phase 3B 新增） ──
        self._validate_window_functions(ctx, questions)

        # ── 10. 窗口函数位置校验（Phase 3B 新增） ──
        self._validate_window_position(ctx, questions)

        # 有 blocking 问题 → 不通过
        all_passed = not any(q.blocking for q in questions)
        return all_passed, questions

    # ── 检查方法 ──

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
