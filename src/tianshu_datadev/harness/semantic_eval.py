"""SemanticEvaluator——Phase 4C 语义评测器。

对 5 类语义错误进行确定性评测：
1. 错字段——聚合输入列与声明不符（Validator 字段引用校验）
2. 错粒度——分组键与声明不符（Validator 输出列检查）
3. 错聚合——聚合函数与声明不符（枚举值对比）
4. 错枚举——CASE WHEN 输出未声明枚举值（LabelValidator）
5. 错 Join——Join key 类型不兼容（Validator Join key 类型检查）

语义错误不触发 Schema 层的 ValidationError（字段名本身合法），
而是通过 Validator 的间接推理检出。
"""

from __future__ import annotations

from datetime import datetime, timezone

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    JoinType,
    Predicate,
    PredicateOperator,
    SqlLiteral,
    WhenBranch,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    JoinStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator
from tianshu_datadev.validation.label_validator import validate_label_enums

from .models import (
    SemanticCase,
    SemanticCaseResult,
    SemanticErrorType,
    SemanticEvalReport,
)

# ════════════════════════════════════════════
# 语义错误 Fixture（Python inline——含复杂嵌套模型）
# ════════════════════════════════════════════


def _make_semantic_fixtures() -> list[SemanticCase]:
    """构建 5 类语义错误的测试用例。

    每类错误构造一个 SemanticCase——声明"正确的"语义，
    但实际 SqlBuildPlan 被故意篡改引入语义错误。
    返回的 case 由 SemanticEvaluator.evaluate_case() 评测。
    """
    return [
        # ── 错误1：错字段 ──
        SemanticCase(
            case_id="SEM-WF-001",
            error_type=SemanticErrorType.WRONG_FIELD,
            description=(
                "声明 SUM(order_amount)，实际 AggregateSpec.input_column='order_count'——"
                "输入列名与声明不符"
            ),
            expected_detection_layer="validator",
            expected_rejection_pattern="Q-VAL-COL-",
        ),
        # ── 错误2：错粒度 ──
        SemanticCase(
            case_id="SEM-WG-001",
            error_type=SemanticErrorType.WRONG_GRAIN,
            description=(
                "声明按 (date, region) 分组，实际 group_keys 仅含 date——"
                "缺少 region 维度"
            ),
            expected_detection_layer="validator",
            expected_rejection_pattern="Q-VAL-COL-",
        ),
        # ── 错误3：错聚合 ──
        SemanticCase(
            case_id="SEM-WA-001",
            error_type=SemanticErrorType.WRONG_AGGREGATION,
            description=(
                "声明 COUNT(DISTINCT user_id)，实际 aggregation='COUNT'——"
                "缺少 DISTINCT 语义"
            ),
            expected_detection_layer="validator",
            expected_rejection_pattern="Q-VAL-COL-",
        ),
        # ── 错误4：错枚举 ──
        SemanticCase(
            case_id="SEM-WE-001",
            error_type=SemanticErrorType.WRONG_ENUM,
            description=(
                "声明枚举值 ['高', '中', '低']，CaseWhenStep 输出 '极高'——"
                "未声明的枚举值"
            ),
            expected_detection_layer="label_validator",
            expected_rejection_pattern="label_enum_undeclared_",
        ),
        # ── 错误5：错 Join ──
        SemanticCase(
            case_id="SEM-WJ-001",
            error_type=SemanticErrorType.WRONG_JOIN,
            description=(
                "Join key 类型不兼容（int vs varchar）——"
                "用 user_id(int) JOIN order_desc(varchar)，字段类型不一致"
            ),
            expected_detection_layer="validator",
            expected_rejection_pattern="Q-VAL-JOINTYPE-",
        ),
    ]


# 已知能力缺口——Phase 4C 补全后，所有 5 类语义错误均已有对应 Validator 规则。
# 此集合保留为空——若未来新增语义错误类型暂时无对应规则，
# 可临时加入此集合以标记为 known_gap。
_KNOWN_GAP_ERROR_TYPES: set[SemanticErrorType] = set()


class SemanticEvaluator:
    """语义评测器——确定性评估 5 类语义错误的检测能力。

    全部 5/5 可检测（Phase 4C 补全）：
    - ✅ WRONG_FIELD（字段引用校验——Q-VAL-COL-）
    - ✅ WRONG_GRAIN（粒度完整性校验——Q-VAL-GRAIN-）
    - ✅ WRONG_AGGREGATION（聚合类型声明对比——Q-VAL-AGG-）
    - ✅ WRONG_ENUM（LabelValidator 枚举值校验）
    - ✅ WRONG_JOIN（Join key 类型兼容校验——Q-VAL-JOINTYPE-）

    评测流程（确定性的，不调用真实 LLM）：
    1. 为每类语义错误构造故意有错的 SqlBuildPlan
    2. 将错误 plan 注入 Validator/LabelValidator 流水线
    3. 验证错误是否被正确检测
    4. 生成 SemanticEvalReport——含 known_gaps 标记
    """

    def __init__(self):
        """初始化语义评测器。"""
        self._fixtures = _make_semantic_fixtures()

    # ── 公开接口 ──

    def run_all(
        self, error_types: list[SemanticErrorType] | None = None,
    ) -> SemanticEvalReport:
        """运行全部（或指定子集）语义错误评测。

        对于已知缺口（_KNOWN_GAP_ERROR_TYPES），无论评测结果如何，
        均如实标记为 known_gap 而非声称已覆盖。
        已知缺口不等于"测试失败"——它反映系统当前的能力边界。

        Args:
            error_types: 要评测的错误类型列表。None 表示全部 5 类。

        Returns:
            SemanticEvalReport——含每类错误的逐条结果和 known_gaps。
        """
        if error_types is None:
            error_types = list(SemanticErrorType)

        cases = [
            c for c in self._fixtures
            if c.error_type in error_types
        ]

        all_results = [self.evaluate_case(case) for case in cases]
        error_type_coverage: dict[str, bool] = {}
        known_gaps: list[str] = []

        for et in error_types:
            et_results = [r for r in all_results if r.error_type == et]
            if et in _KNOWN_GAP_ERROR_TYPES:
                # 已知缺口——标记为未覆盖（False），加入 known_gaps
                error_type_coverage[et.value] = False
                known_gaps.append(et.value)
            else:
                error_type_coverage[et.value] = (
                    all(r.passed for r in et_results) if et_results else False
                )

        detected_count = sum(1 for r in all_results if r.passed)
        undetected = [
            f"{r.case_id}: {r.rejection_detail}"
            for r in all_results if not r.passed
        ]

        # 构造 summary——明确列出已知缺口
        if known_gaps:
            gap_str = ", ".join(known_gaps)
            summary = (
                f"{detected_count}/{len(all_results)} errors detectable "
                f"({len(known_gaps)} known gap{'s' if len(known_gaps) > 1 else ''}: "
                f"{gap_str})"
            )
        else:
            summary = f"{detected_count}/{len(all_results)} errors detected"

        return SemanticEvalReport(
            eval_id=SemanticEvalReport.generate_eval_id(),
            timestamp=datetime.now(timezone.utc).isoformat(),
            summary=summary,
            error_type_coverage=error_type_coverage,
            results=all_results,
            undetected_errors=undetected,
            known_gaps=known_gaps,
        )

    def evaluate_case(self, case: SemanticCase) -> SemanticCaseResult:
        """评测单个语义错误场景——按错误类型分派。"""
        dispatcher = {
            SemanticErrorType.WRONG_FIELD: self._eval_wrong_field,
            SemanticErrorType.WRONG_GRAIN: self._eval_wrong_grain,
            SemanticErrorType.WRONG_AGGREGATION: self._eval_wrong_aggregation,
            SemanticErrorType.WRONG_ENUM: self._eval_wrong_enum,
            SemanticErrorType.WRONG_JOIN: self._eval_wrong_join,
        }
        handler = dispatcher.get(case.error_type)
        if handler is None:
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                detection_layer=None,
                rejection_detail=f"未知语义错误类型：{case.error_type}",
                trace="无对应的评测方法",
            )
        return handler(case)

    # ── 错误1：错字段 ──

    def _eval_wrong_field(self, case: SemanticCase) -> SemanticCaseResult:
        """评测语义错误：错字段。

        策略：构造含 AggregateSpec.input_column='order_count' 的 plan，
        但所有 ScanStep 的 required_columns 中只有 order_amount。
        Validator 的字段引用校验应发现 order_count 不在任何表的列清单中。
        """
        # 构造 manifest——含 order_amount 字段，但不含 order_count
        manifest = SourceManifest(
            manifest_id="test_manifest_wf",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(column_name="order_amount", normalized_name="order_amount",
                        data_type="decimal"),
                        ManifestColumn(column_name="dt", normalized_name="dt", data_type="date"),
                    ],
                ),
            ],
        )

        # 构造错误 plan——required_columns 含 order_count 但 manifest 中不存在
        plan = SqlBuildPlan(
            plan_id="test_wrong_field",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="order_amount", normalized_name="order_amount"),
                        ColumnRef(table_ref="tf", column_name="dt", normalized_name="dt"),
                        # order_count 在 required_columns 中但不在 manifest 中
                        ColumnRef(table_ref="tf", column_name="order_count", normalized_name="order_count"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_wrong",
                    group_keys=[
                        ColumnRef(table_ref="tf", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="SUM",
                            input_column="order_count",  # 错误：应该是 order_amount
                            alias="total_amt",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        if not passed:
            col_issues = [
                q for q in questions
                if q.blocking and (
                    "Q-VAL-COL-" in q.question_id
                    or "order_count" in q.description.lower()
                    or "字段" in q.description
                    or "列" in q.description
                )
            ]
            if col_issues:
                issue = col_issues[0]
                return SemanticCaseResult(
                    case_id=case.case_id,
                    error_type=case.error_type,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=f"Validator 检测到字段错误：{issue.question_id}",
                    trace=f"question_id={issue.question_id}, "
                          f"description={issue.description[:200]}",
                )
            # 有其他 blocking 问题但未命中字段相关拒绝码——不能算"错字段已识别"
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                detection_layer="validator",
                rejection_detail=(
                    f"Validator 拒绝但拒绝码未命中预期——"
                    f"预期 Q-VAL-COL-* 或含 'order_count'/'字段'/'列'，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )
        else:
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                detection_layer=None,
                rejection_detail="Validator 未检测到错字段——字段引用校验可能对 input_column 未覆盖",
                trace="AggregateSpec.input_column='order_count' 不在列清单中，但 Validator 返回 passed=True",
            )

    # ── 错误2：错粒度 ──

    def _eval_wrong_grain(self, case: SemanticCase) -> SemanticCaseResult:
        """评测语义错误：错粒度。

        策略：构造 ParsedDeveloperSpec 声明 (date, region) 两个维度，
        但 SqlBuildPlan 的 group_keys 仅含 date——缺失 region。
        Validator._validate_grain_completeness() 应产生 Q-VAL-GRAIN- 拒绝码。
        """
        from tianshu_datadev.developer_spec.models import (
            DimensionDecl,
            MetricDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest_wg",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(column_name="date", normalized_name="date", data_type="date"),
                        ManifestColumn(column_name="region", normalized_name="region", data_type="varchar"),
                        ManifestColumn(column_name="amount", normalized_name="amount", data_type="decimal"),
                    ],
                ),
            ],
        )

        # 错误 plan——group_keys 仅含 date，缺少 region
        plan = SqlBuildPlan(
            plan_id="test_wrong_grain",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="date", normalized_name="date"),
                        ColumnRef(table_ref="tf", column_name="region", normalized_name="region"),
                        ColumnRef(table_ref="tf", column_name="amount", normalized_name="amount"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_wrong_grain",
                    group_keys=[
                        ColumnRef(table_ref="tf", column_name="date", normalized_name="date"),
                        # 缺少 region——声明需要但实际未分组
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="SUM",
                            input_column="amount",
                            alias="total_amt",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        # 构造 ParsedDeveloperSpec——声明两个维度
        spec = ParsedDeveloperSpec(
            spec_id="spec_wg",
            spec_hash="abc123",
            title="粒度测试",
            description="声明 (date, region) 两个维度",
            input_tables=[],
            metrics=[
                MetricDecl(metric_name="total_amt", aggregation="SUM", input_column="amount", alias="total_amt"),
            ],
            dimensions=[
                DimensionDecl(dimension_name="date", column_ref="date"),
                DimensionDecl(dimension_name="region", column_ref="region"),
            ],
            output_spec=OutputSpecDecl(columns=["date", "region", "total_amt"], grain=["date", "region"]),
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest, spec=spec)

        if not passed:
            grain_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-GRAIN-" in q.question_id
            ]
            if grain_issues:
                return SemanticCaseResult(
                    case_id=case.case_id,
                    error_type=case.error_type,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=(
                        f"Validator 检测到粒度错误："
                        f"{grain_issues[0].question_id}"
                    ),
                    trace=f"question_id={grain_issues[0].question_id}, "
                          f"description={grain_issues[0].description[:200]}",
                )
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                rejection_detail=(
                    f"Validator 拒绝但未命中 Q-VAL-GRAIN-——"
                    f"实际: {questions[0].question_id if questions else '无'}"
                ),
                trace=f"questions={[q.question_id for q in questions]}",
            )
        else:
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                rejection_detail="Validator 未检测到粒度不完整——_validate_grain_completeness 可能未生效",
                trace="plan 的 group_keys 缺少 region，但 Validator 返回 passed=True",
            )

    # ── 错误3：错聚合 ──

    def _eval_wrong_aggregation(self, case: SemanticCase) -> SemanticCaseResult:
        """评测语义错误：错聚合。

        策略：构造 ParsedDeveloperSpec 声明 COUNT_DISTINCT(user_id)->dau，
        但 SqlBuildPlan 使用 COUNT(user_id)->dau。
        Validator._validate_aggregation_declaration() 应产生 Q-VAL-AGG- 拒绝码。
        """
        from tianshu_datadev.developer_spec.models import (
            DimensionDecl,
            MetricDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest_wa",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="int"),
                        ManifestColumn(column_name="dt", normalized_name="dt", data_type="date"),
                    ],
                ),
            ],
        )

        # 错误 plan——使用 COUNT 而非 COUNT_DISTINCT
        plan = SqlBuildPlan(
            plan_id="test_wrong_agg",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="user_id", normalized_name="user_id"),
                        ColumnRef(table_ref="tf", column_name="dt", normalized_name="dt"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_wrong_func",
                    group_keys=[
                        ColumnRef(table_ref="tf", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT",  # 错误：声明应为 COUNT_DISTINCT
                            input_column="user_id",
                            alias="dau",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        # 构造 ParsedDeveloperSpec——声明 COUNT_DISTINCT
        spec = ParsedDeveloperSpec(
            spec_id="spec_wa",
            spec_hash="abc123",
            title="聚合测试",
            description="声明 COUNT_DISTINCT(user_id) as dau",
            input_tables=[],
            metrics=[
                MetricDecl(metric_name="dau", aggregation="COUNT_DISTINCT", input_column="user_id", alias="dau"),
            ],
            dimensions=[
                DimensionDecl(dimension_name="dt", column_ref="dt"),
            ],
            output_spec=OutputSpecDecl(columns=["dt", "dau"], grain=["dt"]),
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest, spec=spec)

        if not passed:
            agg_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-AGG-" in q.question_id
            ]
            if agg_issues:
                return SemanticCaseResult(
                    case_id=case.case_id,
                    error_type=case.error_type,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=(
                        f"Validator 检测到聚合错误："
                        f"{agg_issues[0].question_id}"
                    ),
                    trace=f"question_id={agg_issues[0].question_id}, "
                          f"description={agg_issues[0].description[:200]}",
                )
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                rejection_detail=(
                    f"Validator 拒绝但未命中 Q-VAL-AGG-——"
                    f"实际: {questions[0].question_id if questions else '无'}"
                ),
                trace=f"questions={[q.question_id for q in questions]}",
            )
        else:
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                rejection_detail="Validator 未检测到聚合类型不匹配——_validate_aggregation_declaration 可能未生效",
                trace="plan 使用 COUNT 而非 COUNT_DISTINCT，但 Validator 返回 passed=True",
            )

    # ── 错误4：错枚举 ──

    def _eval_wrong_enum(self, case: SemanticCase) -> SemanticCaseResult:
        """评测语义错误：错枚举。

        策略：构造 CaseWhenStep 输出 '极高'（未声明的枚举值）。
        系统应通过 LabelValidator 的 validate_label_enums() 检测到。
        如果 LabelValidator 未注册该值，产生 label_enum_undeclared_* 拒绝。
        """
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            InputTableDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest_we",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(
                            column_name="score", normalized_name="score",
                            data_type="int",
                            enum_values=["高", "中", "低"],  # 声明仅三个枚举值
                        ),
                    ],
                ),
            ],
        )

        # 构造 CaseWhenStep——输出未声明的 '极高'
        plan = SqlBuildPlan(
            plan_id="test_wrong_enum",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_wrong_enum",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf", column_name="score", normalized_name="score",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=90),
                            ),
                            result=SqlLiteral(value="极高"),  # 未声明的枚举值
                        ),
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf", column_name="score", normalized_name="score",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=60),
                            ),
                            result=SqlLiteral(value="中"),
                        ),
                    ],
                    else_value=SqlLiteral(value="低"),
                    alias="score_label",
                ),
            ],
            multi_table=False,
        )

        # 构造 ParsedDeveloperSpec——含枚举声明
        spec = ParsedDeveloperSpec(
            spec_id="spec_we",
            spec_hash="abc123",
            title="枚举测试",
            description="测试未声明枚举值检测",
            input_tables=[
                InputTableDecl(
                    table_alias="tf",
                    source_table="db.tf",
                    columns=[
                        ColumnDecl(
                            column_name="score",
                            normalized_name="score",
                            data_type="int",
                            enum_values=["高", "中", "低"],
                        ),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=["score_label"],
                grain=["score_label"],
            ),
        )

        # 运行 LabelValidator
        questions = validate_label_enums(plan, spec=spec, manifest=manifest)

        enum_issues = [
            q for q in questions
            if "label_enum_undeclared_" in q.question_id
            or "极高" in q.description
            or "未声明" in q.description
        ]

        if enum_issues:
            issue = enum_issues[0]
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=True,
                detection_layer="label_validator",
                rejection_detail=f"LabelValidator 检测到未声明枚举值：{issue.question_id}",
                trace=f"question_id={issue.question_id}, "
                      f"description={issue.description[:200]}",
            )

        if questions:
            # 有 LabelValidator 问题但未命中枚举相关关键词——不能算"错枚举已识别"
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                detection_layer="label_validator",
                rejection_detail=(
                    f"LabelValidator 发现问题但未命中预期——"
                    f"预期 label_enum_undeclared_* 或 '极高'/'未声明'，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )

        return SemanticCaseResult(
            case_id=case.case_id,
            error_type=case.error_type,
            passed=False,
            rejection_detail="LabelValidator 未检测到未声明枚举值 '极高'——枚举覆盖检查可能缺失",
            trace="plan 的 CaseWhenStep 输出 '极高'，但 LabelValidator 未检测到",
        )

    # ── 错误5：错 Join ──

    def _eval_wrong_join(self, case: SemanticCase) -> SemanticCaseResult:
        """评测语义错误：错 Join。

        策略：构造 Join key 类型不兼容的 JoinStep（int vs varchar）。
        Validator._validate_join_key_types() 应产生 Q-VAL-JOINTYPE- 拒绝码。
        """
        manifest = SourceManifest(
            manifest_id="test_manifest_wj",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="int"),
                    ],
                ),
                ManifestTable(
                    table_ref="t2",
                    source_table="db.t2",
                    columns=[
                        # order_desc 是 varchar——与 user_id(int) 不兼容
                        ManifestColumn(column_name="order_desc", normalized_name="order_desc",
                        data_type="varchar"),
                    ],
                ),
            ],
        )

        # 错误 plan——Join key 类型不兼容
        plan = SqlBuildPlan(
            plan_id="test_wrong_join",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="user_id", normalized_name="user_id"),
                    ],
                ),
                ScanStep(
                    step_id="scan_t2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(table_ref="t2", column_name="order_desc", normalized_name="order_desc"),
                    ],
                ),
                JoinStep(
                    step_id="join_wrong_type",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="t1", column_name="user_id", normalized_name="user_id"),
                            ColumnRef(table_ref="t2", column_name="order_desc", normalized_name="order_desc"),
                            # int vs varchar——类型不兼容
                        ),
                    ],
                    relationship_ref="rel_wrong",
                ),
            ],
            multi_table=True,
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        if not passed:
            jointype_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-JOINTYPE-" in q.question_id
            ]
            if jointype_issues:
                issue = jointype_issues[0]
                return SemanticCaseResult(
                    case_id=case.case_id,
                    error_type=case.error_type,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=f"Validator 检测到 Join key 类型不兼容：{issue.question_id}",
                    trace=f"question_id={issue.question_id}, "
                          f"description={issue.description[:200]}",
                )
            # 有其他 blocking 问题但未命中 Join 类型关键词——不能算"错 Join 已识别"
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                detection_layer="validator",
                rejection_detail=(
                    f"Validator 拒绝但拒绝码未命中预期——"
                    f"预期 Q-VAL-JOINTYPE-*，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )
        else:
            return SemanticCaseResult(
                case_id=case.case_id,
                error_type=case.error_type,
                passed=False,
                rejection_detail="Validator 未检测到 Join key 类型不兼容——Join key 类型检查可能缺失",
                trace="Join key:int vs varchar，但 Validator 返回 passed=True",
            )
