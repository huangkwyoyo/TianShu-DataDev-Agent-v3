"""SqlBuildPlanValidator——事实源校验 + Join 门禁 + 语义校验 + 架构边界断言。

验证流程（15 项检查，Phase 4.6 Step 1/2 完整实施）：
1. 空 steps 拒绝
2. 不支持的步骤类型——白名单外 Step 类型一律拒绝（架构边界断言）
3. 多跳 Join 校验——V-009 四规则体系（Phase 4.6 Step 1 实施）
   V-009a: 每步 evidence_level ≥ MEDIUM（_validate_join_evidence_gate 二次确认）
   V-009b: 右表引用链无循环（validate_multi_hop_chain 跨 SqlProgram 校验）
   V-009c: 多跳深度 ≤ 5（validate_multi_hop_chain 跨 SqlProgram 校验）
   V-009d: 单 Plan 仅允许一张 JoinStep（_validate_multi_hop_join 硬门禁）
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
from tianshu_datadev.planning.models import ColumnRef, RatioExpr
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinEvidenceLevel,
    RelationshipHypothesis,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    JoinStep,
    ProjectStep,
    ScanStep,
    SqlBuildPlan,
    StepNode,
    SubqueryStep,
    WindowStep,
)

# SqlProgram 导入——Phase 4.6 Step 1 多跳链校验
from tianshu_datadev.planning.sql_program import SqlProgram

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

        # ── 3. 多跳 Join 校验——V-009d 单 Plan 硬门禁（Phase 4.6 Step 1） ──
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

        # ── 10. 窗口函数校验（Phase 3B 新增） ──
        self._validate_window_functions(ctx, questions)

        # ── 11. 窗口函数位置校验（Phase 3B 新增） ──
        self._validate_window_position(ctx, questions)

        # ── 13. 粒度完整性校验（Phase 4C 补全——原 known_gap） ──
        self._validate_grain_completeness(ctx, questions, spec)

        # ── 14. 聚合类型声明对比（Phase 4C 补全——原 known_gap） ──
        self._validate_aggregation_declaration(ctx, questions, spec)

        # 聚合后的投影只能引用分组键、聚合结果或后续派生列。
        # 该门禁保护“字段 + 业务描述”场景，避免 Agent 漏维度时生成非法 SQL。
        self._validate_aggregate_projection(ctx, questions)

        # ── 15. 子查询校验——V-010a~e 五规则（Phase 4.6 Step 2） ──
        self._validate_subquery(ctx, questions)

        # 有 blocking 问题 → 不通过
        all_passed = not any(q.blocking for q in questions)
        return all_passed, questions

    def validate_multi_hop_chain(
        self,
        program: SqlProgram,
    ) -> tuple[bool, list[OpenQuestion]]:
        """V-009b + V-009c——跨 SqlProgram 的多跳 Join 链级别校验。

        遍历 SqlProgram 中所有语句的 SqlBuildPlan，提取每个计划中的 JoinStep
        （每计划最多 1 个，由 V-009d 保证），在链级别进行两项检查：

        V-009b 循环检测：右表引用链中不得出现重复表引用。
        V-009c 深度上限：整条链的 JoinStep 总数不得超过 5。

        Args:
            program: 待校验的 SqlProgram

        Returns:
            (all_passed, open_questions)
        """
        questions: list[OpenQuestion] = []

        if not program.statements:
            return True, questions

        # 按拓扑顺序遍历语句，收集 JoinStep 信息
        topo_order = program.topological_order
        if not topo_order:
            # 无拓扑排序时按 statements 原始顺序
            ordered_stmts = program.statements
        else:
            stmt_map = {s.statement_id: s for s in program.statements}
            ordered_stmts = [stmt_map[sid] for sid in topo_order if sid in stmt_map]

        join_chain: list[dict] = []  # [{step_id, right_table_ref, statement_id}, ...]
        seen_right_tables: set[str] = set()

        for stmt in ordered_stmts:
            plan = stmt.plan
            for step in plan.steps:
                if isinstance(step, JoinStep):
                    join_chain.append({
                        "step_id": step.step_id,
                        "right_table_ref": step.right_table_ref,
                        "statement_id": stmt.statement_id,
                    })

        # ── V-009c：深度上限 ≤ 5 ──
        total_hops = len(join_chain)
        if total_hops > 5:
            chain_desc = " → ".join(
                j["right_table_ref"] for j in join_chain
            )
            questions.append(
                OpenQuestion(
                    question_id=f"Q-VAL-MULTIHOP-DEPTH-{program.program_id}",
                    source="validator",
                    field_ref="SqlProgram.statements",
                    description=(
                        f"V-009c MULTI_HOP_DEPTH_EXCEEDED——"
                        f"整条 Join 链包含 {total_hops} 跳（上限为 5 跳）。"
                        f"右表引用链: {chain_desc}。"
                        f"请减少 Join 跳数或将部分关联逻辑拆分为独立程序。"
                    ),
                    blocking=True,
                )
            )

        # ── V-009b：循环检测——右表引用链中不得出现重复 ──
        for j in join_chain:
            right_ref = j["right_table_ref"]
            # 跳过 _temp 中间表引用（由 SqlProgram 串联产生，不算循环）
            if right_ref.startswith("_temp_"):
                continue
            if right_ref in seen_right_tables:
                chain_desc = " → ".join(
                    jj["right_table_ref"] for jj in join_chain
                )
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"Q-VAL-MULTIHOP-CYCLE-{program.program_id}"
                        ),
                        source="validator",
                        field_ref=f"SqlProgram.statements.{j['statement_id']}",
                        description=(
                            f"V-009b AMBIGUOUS_MULTI_HOP——"
                            f"右表 '{right_ref}' 在 Join 链中重复出现。"
                            f"链: {chain_desc}。"
                            f"多跳 Join 链必须为线性——每个右表只能被 JOIN 一次，"
                            f"否则说明 Join 顺序存在歧义（菱形 Join）。"
                        ),
                        blocking=True,
                    )
                )
            seen_right_tables.add(right_ref)

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
            SubqueryStep,
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
                            f"该步骤类型当前未开放，如有需求请联系平台管理员。"
                        ),
                        blocking=True,
                    )
                )

    def _validate_multi_hop_join(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """V-009d 硬门禁——单 SqlBuildPlan 内仅允许一张 JoinStep。

        多跳 Join 通过 SqlProgram 串联实现——每步最多一个 JoinStep，
        通过 _temp 表传递中间结果。链级别的循环检测（V-009b）和深度上限（V-009c）
        由 validate_multi_hop_chain() 在 SqlProgram 级别校验。

        Phase 4.6 Step 1 实施——从 blanket rejection 升级为架构指导错误。
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
                        f"V-009d MULTI_HOP_PER_STEP_EXCEEDED——当前 SqlBuildPlan 包含 "
                        f"{join_step_count} 个 JoinStep（右表引用链: {chain}），"
                        f"超过单 Plan 上限（1 个）。多跳 Join 请使用 SqlProgram 串联——"
                        f"每步一个 JoinStep，通过 _temp 中间表传递结果。链级别深度上限为 5 跳。"
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
                                f"未在 SourceManifest 的 {step.table_ref} 表中找到——"
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
                            f"内部数据异常——Join 关联记录 '{ref}' 缺失，"
                            f"请重新执行关系推理"
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
                            f"关联证据不足（{level.value}）——"
                            f"请确认两表之间是否有明确的关联关系，"
                            f"或在 DeveloperSpec 中显式声明 join_keys"
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

    # ── Phase 4.6 Step 2 新增：子查询五规则校验 ──

    def _validate_subquery(
        self, ctx: _ValidationContext, questions: list[OpenQuestion]
    ) -> None:
        """V-010a~e 子查询专用校验——递归检查所有嵌套 SubqueryStep。

        五条规则：
        V-010a SUBQUERY_DEPTH_CHECK：嵌套深度 <= 2
        V-010c SUBQUERY_FACT_SOURCE_CHECK：内层事实源一致性（递归校验）
        V-010d SUBQUERY_WINDOW_FORBIDDEN：内层不含 WindowStep
        V-010e SUBQUERY_JOIN_FORBIDDEN：内层仅单表（无 JoinStep）

        递归遍历 SqlBuildPlan 的 steps——包括 SubqueryStep 嵌套的内层计划。
        """
        self._validate_subquery_recursive(ctx.plan, ctx, questions, depth=1)

    def _validate_subquery_recursive(
        self,
        plan: SqlBuildPlan,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
        depth: int = 1,
    ) -> None:
        """递归遍历计划中的 SubqueryStep，逐层应用 V-010 规则。

        Args:
            plan: 当前层级的 SqlBuildPlan
            ctx: 验证上下文（manifest 信息用于事实源校验）
            questions: 累积的 OpenQuestion 列表
            depth: 当前递归深度（从 1 开始）
        """
        for step in plan.steps:
            if not isinstance(step, SubqueryStep):
                continue

            sq = step
            inner = sq.inner_plan

            # ── V-010a：深度检查——嵌套不得超过 2 层 ──
            effective_depth = sq.depth
            if effective_depth > 2:
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-SUBQUERY-DEPTH-{sq.step_id}",
                        source="validator",
                        field_ref=f"plan.steps.{sq.step_id}.depth",
                        description=(
                            f"V-010a SUBQUERY_NESTING_TOO_DEEP——"
                            f"子查询嵌套深度为 {effective_depth} 层，"
                            f"超过上限（2 层）。"
                            f"别名: '{sq.alias}'"
                        ),
                        blocking=True,
                    )
                )

            # ── V-010d：内层不得含 WindowStep ──
            inner_win_steps = [
                s for s in inner.steps if isinstance(s, WindowStep)
            ]
            if inner_win_steps:
                win_ids = [ws.step_id for ws in inner_win_steps]
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-SUBQUERY-WINDOW-{sq.step_id}",
                        source="validator",
                        field_ref=f"plan.steps.{sq.step_id}.inner_plan",
                        description=(
                            f"V-010d SUBQUERY_WINDOW_FORBIDDEN——"
                            f"子查询 '{sq.alias}' 的内层计划包含 "
                            f"{len(inner_win_steps)} 个 WindowStep "
                            f"({win_ids})。子查询内禁止窗口函数，"
                            f"窗口操作应在子查询外部进行。"
                        ),
                        blocking=True,
                    )
                )

            # ── V-010e：内层仅单表（无 JoinStep） ──
            inner_join_steps = [
                s for s in inner.steps if isinstance(s, JoinStep)
            ]
            if inner_join_steps:
                join_ids = [js.step_id for js in inner_join_steps]
                questions.append(
                    OpenQuestion(
                        question_id=f"Q-VAL-SUBQUERY-JOIN-{sq.step_id}",
                        source="validator",
                        field_ref=f"plan.steps.{sq.step_id}.inner_plan",
                        description=(
                            f"V-010e SUBQUERY_JOIN_FORBIDDEN——"
                            f"子查询 '{sq.alias}' 的内层计划包含 "
                            f"{len(inner_join_steps)} 个 JoinStep "
                            f"({join_ids})。子查询内仅允许单表扫描，"
                            f"复杂关联请拆分为多步 SqlProgram。"
                        ),
                        blocking=True,
                    )
                )

            # ── V-010c：事实源一致性——递归校验内层计划 ──
            self._check_subquery_fact_source(sq, ctx, questions)

            # ── 递归进入子查询的内层计划 ──
            self._validate_subquery_recursive(
                inner, ctx, questions, depth=sq.depth + 1
            )

    def _check_subquery_fact_source(
        self,
        sq: SubqueryStep,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
    ) -> None:
        """V-010c 事实源一致性——递归校验内层 SqlBuildPlan 的表引用。

        确保内层计划的 ScanStep.table_ref 全部在 SourceManifest 中注册。
        """
        inner = sq.inner_plan
        registered_tables = {t.table_ref for t in ctx.manifest.tables}

        for s in inner.steps:
            if not isinstance(s, ScanStep):
                continue
            # 跳过 _temp 中间表（由 SqlProgram 管理）
            if s.table_ref.startswith("_temp_"):
                continue
            if s.table_ref not in registered_tables:
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"Q-VAL-SUBQUERY-SOURCE-"
                            f"{sq.step_id}-{s.table_ref}"
                        ),
                        source="validator",
                        field_ref=(
                            f"plan.steps.{sq.step_id}"
                            f".inner_plan.{s.step_id}"
                        ),
                        description=(
                            f"V-010c SOURCE_CONFLICT——子查询 "
                            f"'{sq.alias}' 内层 ScanStep "
                            f"'{s.step_id}' 引用了未注册表 "
                            f"'{s.table_ref}'。"
                            f"已注册表: {sorted(registered_tables)}"
                        ),
                        blocking=True,
                    )
                )

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
                    declared_dimensions.add(
                        dim.dimension_name
                        if getattr(dim, "date_part", None)
                        else col_ref
                    )
        except Exception:
            return  # spec 结构不符预期——跳过，不误报

        if not declared_dimensions:
            return

        # 收集所有 AggregateStep 的 group_keys 中的列名（归一化名）
        # 兼容 ColumnRef / DatePartExpression / DerivedGroupKey 三种分组键类型
        for agg_step in ctx.aggregate_steps:
            actual_grain: set[str] = {
                gk.normalized_name if isinstance(gk, ColumnRef) else gk.alias
                for gk in agg_step.group_keys
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

    def _validate_aggregate_projection(
        self,
        ctx: _ValidationContext,
        questions: list[OpenQuestion],
    ) -> None:
        """阻断聚合后投影未分组的原始列。"""
        available: set[str] | None = None

        for step in ctx.plan.steps:
            if isinstance(step, AggregateStep):
                available = set()
                for key in step.group_keys:
                    if isinstance(key, ColumnRef):
                        available.add(key.column_name)
                        available.add(key.normalized_name)
                    else:
                        # DatePartExpression / DerivedGroupKey——只暴露 alias
                        available.add(key.alias)
                available.discard("")
                available.update(metric.alias for metric in step.metrics)
                continue

            if available is None:
                continue

            if isinstance(step, CaseWhenStep):
                if step.alias:
                    available.add(step.alias)
                continue

            if isinstance(step, WindowStep):
                available.update(expr.alias for expr in step.window_exprs)
                continue

            if not isinstance(step, ProjectStep):
                continue

            for column in step.columns:
                expression = column.expression
                if isinstance(expression, RatioExpr):
                    missing = [
                        dependency
                        for dependency in (
                            expression.numerator_alias,
                            expression.denominator_alias,
                        )
                        if dependency not in available
                    ]
                    if missing:
                        questions.append(OpenQuestion(
                            question_id=(
                                f"Q-VAL-RATIO-{step.step_id}-{column.alias}"
                            ),
                            source="validator",
                            field_ref=f"{step.step_id}.columns.{column.alias}",
                            description=(
                                f"比率输出 '{column.alias}' 引用了未定义的聚合后别名 "
                                f"{missing}"
                            ),
                            blocking=True,
                        ))
                    else:
                        available.add(column.alias)
                    continue
                column_name = getattr(expression, "column_name", None)
                normalized_name = getattr(expression, "normalized_name", None)
                if column_name is None:
                    continue
                if (
                    column_name in available
                    or normalized_name in available
                    or column.alias in available
                ):
                    continue
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"Q-VAL-AGG-PROJECT-{step.step_id}-{column.alias}"
                        ),
                        source="validator",
                        field_ref=f"{step.step_id}.columns.{column.alias}",
                        description=(
                            f"聚合后输出列 '{column.alias}' 引用了未分组的原始字段 "
                            f"'{column_name}'。请将该字段加入维度，或将输出改为聚合指标。"
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
                            f"缺少时间过滤条件——可能触发全表扫描。"
                            f"请在 spec 中添加 time_range 声明，例如：\n"
                            f"  time_range:\n"
                            f'    start: "2026-01-01"\n'
                            f'    end: "2026-03-31"'
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

    检测策略（三级）：
    1. 列名含时间关键词（date/time/timestamp/dt 等）
    2. 列名匹配常见时间字段后缀（_at/_date/_time/_ts/_dt）
    3. BETWEEN 谓词的右值为日期格式字符串（如 "2026-01-01"）→ 高度疑似时间过滤
    """
    time_keywords = {
        "date", "time", "timestamp", "datetime", "dt", "ds",
        "event_time", "create_time", "created_at", "updated_at",
        "stat_date", "dt_date", "day", "hour",
    }
    # 常见时间字段后缀——覆盖 pickup_at / dropoff_at / order_date 等
    time_suffixes = ("_at", "_date", "_time", "_ts", "_dt")

    for pred in predicates:
        if _is_time_field(pred, time_keywords, time_suffixes):
            return True
    return False


def _looks_like_date_value(value: str) -> bool:
    """检查字符串是否像日期值（YYYY-MM-DD 格式）。"""
    import re
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', value))


def _is_time_field(pred, time_keywords: set[str], time_suffixes: tuple[str, ...] = ()) -> bool:
    """递归检查谓词树中是否包含时间字段引用。"""
    # 检查 left 侧
    left = getattr(pred, "left", None)
    if left is not None:
        col_name = getattr(left, "column_name", "")
        norm_name = getattr(left, "normalized_name", "")
        col_lower = col_name.lower()
        norm_lower = norm_name.lower()
        # 策略 1：关键词匹配
        if any(kw in col_lower for kw in time_keywords):
            return True
        if any(kw in norm_lower for kw in time_keywords):
            return True

        # 策略 2：时间字段后缀匹配
        if time_suffixes and any(col_lower.endswith(suffix) for suffix in time_suffixes):
            return True
        if time_suffixes and any(norm_lower.endswith(suffix) for suffix in time_suffixes):
            return True
        # 递归嵌套 Predicate
        if hasattr(left, "operator"):
            if _is_time_field(left, time_keywords, time_suffixes):
                return True

    # 检查 right 侧
    right = getattr(pred, "right", None)
    if right is not None:
        # 策略 3：BETWEEN 右值如果是列表且含日期格式字符串 → 时间过滤
        if isinstance(right, list):
            right_values = [getattr(v, "value", v) for v in right]
            if any(_looks_like_date_value(str(v)) for v in right_values):
                return True
        if hasattr(right, "operator"):
            if _is_time_field(right, time_keywords, time_suffixes):
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
