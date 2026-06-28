"""语义评测器测试——5 类语义错误的覆盖测试。

测试策略：通过 SemanticEvaluator 运行全量语义错误评测，
验证每类错误被正确检测。同时直接测试被测系统以验证个别检测路径。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.harness.models import SemanticCaseResult, SemanticErrorType
from tianshu_datadev.harness.semantic_eval import SemanticEvaluator
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

# ════════════════════════════════════════════
# 错误1：错字段
# ════════════════════════════════════════════


class TestSemanticWrongField:
    """语义错误：错字段——聚合输入列与声明不符。"""

    def test_field_mismatch_detected_by_validator(self):
        """SUM(order_count) 中 order_count 不在 manifest 列清单中——Validator 拒绝。

        策略：在 required_columns 中包含 order_count（让 Validator 看到此引用），
        但在 manifest 中不声明此列——触发 Q-VAL-COL- 拒绝。
        """
        manifest = SourceManifest(
            manifest_id="test_manifest_wf",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(column_name="order_amount", normalized_name="order_amount",
                        data_type="decimal"),
                        ManifestColumn(column_name="dt", normalized_name="dt", data_type="date"),
                        # order_count 不在此列表中
                    ],
                ),
            ],
        )

        plan = SqlBuildPlan(
            plan_id="test_wrong_field",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="order_amount",
                            normalized_name="order_amount",
                        ),
                        ColumnRef(table_ref="tf", column_name="dt", normalized_name="dt"),
                        # order_count 在 required_columns 中但不在 manifest 中
                        ColumnRef(
                            table_ref="tf",
                            column_name="order_count",
                            normalized_name="order_count",
                        ),
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
                            input_column="order_count",  # 错误字段
                            alias="total_amt",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        assert passed is False, (
            f"应拒绝错误字段引用，questions={[q.question_id for q in questions]}"
        )
        col_issues = [
            q for q in questions
            if q.blocking and (
                "Q-VAL-COL-" in q.question_id
                or "order_count" in q.description
            )
        ]
        assert len(col_issues) >= 1, (
            f"应有字段相关拒绝码，实际 questions={[q.question_id for q in questions]}"
        )


# ════════════════════════════════════════════
# 错误2：错粒度
# ════════════════════════════════════════════


class TestSemanticWrongGrain:
    """语义错误：错粒度——分组键与声明不符。

    当前状态：known_gap。Validator 无粒度完整性检查规则，
    因此 evaluator 诚实报告 passed=False。
    此测试记录当前能力边界——防止未来误以为已覆盖。
    """

    def test_grain_mismatch_is_known_gap_not_detected(self):
        """错粒度——当前系统无法检测，evaluator 诚实报告 passed=False。

        Validator 的所有 10 项检查均通过（字段均在 manifest 中存在），
        PerfValidator 的 15 条规则也均通过——无规则检查分组键完整性。

        验证：
        1. Validator 级别——所有字段合法，不触发任何拒绝
        2. Evaluator 级别——SemanticEvaluator 诚实返回 passed=False
        3. 报告级别——error_type_coverage[WRONG_GRAIN]=False 且出现在 known_gaps 中
        """
        manifest = SourceManifest(
            manifest_id="test_manifest_wg",
            spec_hash="abc",
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

        plan = SqlBuildPlan(
            plan_id="test_wrong_grain",
            spec_hash="abc",
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

        # ── 层面 1：Validator 级别——所有字段合法，不触发拒绝 ──
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert passed is True, (
            f"当前 Validator 无粒度规则——应返回 passed=True。"
            f"若此断言失败，说明 Validator 新增了粒度检查，"
            f"请同步更新 _KNOWN_GAP_ERROR_TYPES。"
            f"questions={[q.question_id for q in questions]}"
        )

        # ── 层面 2：Evaluator 级别——诚实报告 passed=False ──
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()
        grain_results = [
            r for r in report.results
            if r.error_type == SemanticErrorType.WRONG_GRAIN
        ]
        assert len(grain_results) == 1, "应有 1 个 WRONG_GRAIN 结果"
        assert grain_results[0].passed is False, (
            "错粒度当前无法检测——evaluator 应诚实报告 passed=False。"
            "若此断言失败，说明 Validator 新增了粒度规则，"
            "请将 WRONG_GRAIN 从 _KNOWN_GAP_ERROR_TYPES 移除。"
        )

        # ── 层面 3：报告级别——标记为 known_gap ──
        assert report.error_type_coverage["WRONG_GRAIN"] is False, (
            "WRONG_GRAIN 未被系统覆盖，coverage 应为 False"
        )
        assert "WRONG_GRAIN" in report.known_gaps, (
            "WRONG_GRAIN 应在 known_gaps 中——这是系统能力边界，"
            "不是测试失败"
        )


# ════════════════════════════════════════════
# 错误3：错聚合
# ════════════════════════════════════════════


class TestSemanticWrongAggregation:
    """语义错误：错聚合——聚合函数与声明不符。

    当前状态：known_gap。Validator 无聚合类型声明对比规则，
    因此 evaluator 诚实报告 passed=False。
    PERF-009 仅检测 COUNT_DISTINCT + 无 group_keys 的特例，
    不覆盖 COUNT vs COUNT_DISTINCT 的声明/实现不匹配。
    """

    def test_aggregation_mismatch_is_known_gap_not_detected(self):
        """错聚合——当前系统无法检测，evaluator 诚实报告 passed=False。

        验证：
        1. Validator 级别——字段合法，不触发任何拒绝
        2. Evaluator 级别——SemanticEvaluator 诚实返回 passed=False
        3. 报告级别——error_type_coverage[WRONG_AGGREGATION]=False 且出现在 known_gaps 中
        """
        manifest = SourceManifest(
            manifest_id="test_manifest_wa",
            spec_hash="abc",
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

        plan = SqlBuildPlan(
            plan_id="test_wrong_agg",
            spec_hash="abc",
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

        # ── 层面 1：Validator 级别——字段合法，不触发拒绝 ──
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert passed is True, (
            f"当前 Validator 无聚合类型对比规则——应返回 passed=True。"
            f"若此断言失败，说明 Validator 新增了聚合检查，"
            f"请同步更新 _KNOWN_GAP_ERROR_TYPES。"
            f"questions={[q.question_id for q in questions]}"
        )

        # ── 层面 2：Evaluator 级别——诚实报告 passed=False ──
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()
        agg_results = [
            r for r in report.results
            if r.error_type == SemanticErrorType.WRONG_AGGREGATION
        ]
        assert len(agg_results) == 1, "应有 1 个 WRONG_AGGREGATION 结果"
        assert agg_results[0].passed is False, (
            "错聚合当前无法检测——evaluator 应诚实报告 passed=False。"
            "若此断言失败，说明 Validator/PerfValidator 新增了聚合规则，"
            "请将 WRONG_AGGREGATION 从 _KNOWN_GAP_ERROR_TYPES 移除。"
        )

        # ── 层面 3：报告级别——标记为 known_gap ──
        assert report.error_type_coverage["WRONG_AGGREGATION"] is False, (
            "WRONG_AGGREGATION 未被系统覆盖，coverage 应为 False"
        )
        assert "WRONG_AGGREGATION" in report.known_gaps, (
            "WRONG_AGGREGATION 应在 known_gaps 中——这是系统能力边界，"
            "不是测试失败"
        )


# ════════════════════════════════════════════
# 错误4：错枚举
# ════════════════════════════════════════════


class TestSemanticWrongEnum:
    """语义错误：错枚举——CASE WHEN 输出未声明枚举值。"""

    def test_undeclared_enum_rejected_by_label_validator(self):
        """LabelValidator 拒绝未声明枚举值 '极高'。"""
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            InputTableDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest_we",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="tf",
                    source_table="db.tf",
                    columns=[
                        ManifestColumn(
                            column_name="score", normalized_name="score",
                            data_type="int",
                            enum_values=["高", "中", "低"],
                        ),
                    ],
                ),
            ],
        )

        plan = SqlBuildPlan(
            plan_id="test_wrong_enum",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="score",
                            normalized_name="score",
                        ),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_wrong_enum",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="score",
                                    normalized_name="score",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=90),
                            ),
                            result=SqlLiteral(value="极高"),  # 未声明
                        ),
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="score",
                                    normalized_name="score",
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

        spec = ParsedDeveloperSpec(
            spec_id="spec_we",
            spec_hash="abc",
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

        questions = validate_label_enums(plan, spec=spec, manifest=manifest)

        assert len(questions) >= 1, (
            "应检测到未声明枚举值 '极高'"
        )
        enum_issues = [
            q for q in questions
            if "极高" in q.description
            or "未声明" in q.description
            or "label_enum" in q.question_id
        ]
        assert len(enum_issues) >= 1, (
            f"应有枚举相关拒绝码，实际 questions={[q.question_id for q in questions]}"
        )


# ════════════════════════════════════════════
# 错误5：错 Join
# ════════════════════════════════════════════


class TestSemanticWrongJoin:
    """语义错误：错 Join——Join key 类型不兼容。"""

    def test_wrong_join_key_type_detected(self):
        """int vs varchar Join key 类型不兼容被 Validator 拒绝。"""
        manifest = SourceManifest(
            manifest_id="test_manifest_wj",
            spec_hash="abc",
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
                        ManifestColumn(column_name="order_desc", normalized_name="order_desc",
                        data_type="varchar"),
                    ],
                ),
            ],
        )

        plan = SqlBuildPlan(
            plan_id="test_wrong_join",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(
                            table_ref="t1",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_t2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(
                            table_ref="t2",
                            column_name="order_desc",
                            normalized_name="order_desc",
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_wrong_type",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(
                                table_ref="t1",
                                column_name="user_id",
                                normalized_name="user_id",
                            ),
                            ColumnRef(
                                table_ref="t2",
                                column_name="order_desc",
                                normalized_name="order_desc",
                            ),
                        ),
                    ],
                    relationship_ref="rel_wrong",
                ),
            ],
            multi_table=True,
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        assert passed is False, (
            f"应拒绝类型不兼容的 Join key，questions={[q.question_id for q in questions]}"
        )
        jointype_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-JOINTYPE-" in q.question_id
        ]
        assert len(jointype_issues) >= 1, (
            f"应有 Q-VAL-JOINTYPE- 拒绝码，实际 questions={[q.question_id for q in questions]}"
        )
        # 验证拒绝可追溯
        issue = jointype_issues[0]
        assert issue.question_id, "拒绝应有 question_id (code)"
        assert issue.description, "拒绝应有 description (message)"


# ════════════════════════════════════════════
# 集成测试——SemanticEvaluator.run_all()
# ════════════════════════════════════════════


class TestSemanticEvaluatorIntegration:
    """通过 SemanticEvaluator 运行全量语义错误评测。"""

    def test_run_all_error_types(self):
        """SemanticEvaluator.run_all() 运行全部 5 类语义错误。

        验证：
        - 5 类错误全部在 error_type_coverage 中
        - known_gaps 记录当前能力边界
        - summary 诚实报告 3/5 + 2 known gaps
        """
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()

        # 验证 5 类错误全被覆盖（出现在 error_type_coverage 中）
        for error_type in SemanticErrorType:
            assert error_type.value in report.error_type_coverage, (
                f"错误类型 {error_type.value} 未被覆盖"
            )

        # 验证 known_gaps 存在且含预期的缺口类型
        assert hasattr(report, "known_gaps"), "SemanticEvalReport 应有 known_gaps 字段"
        assert isinstance(report.known_gaps, list), "known_gaps 应为列表"
        assert "WRONG_GRAIN" in report.known_gaps, (
            "WRONG_GRAIN 应在 known_gaps 中——当前 Validator 无粒度检查"
        )
        assert "WRONG_AGGREGATION" in report.known_gaps, (
            "WRONG_AGGREGATION 应在 known_gaps 中——当前 Validator 无聚合类型检查"
        )

        # 验证所有结果有合理的 rejection_detail
        for result in report.results:
            assert isinstance(result, SemanticCaseResult)
            assert result.case_id, "每个结果应有 case_id"

        # 验证汇总信息——诚实报告缺口
        assert report.eval_id, "应生成 eval_id"
        assert "known gaps" in report.summary, (
            f"summary 应明确提到 known gaps。"
            f"实际: {report.summary}"
        )
        assert "3/5" in report.summary, (
            f"summary 应诚实报告 3/5（非 5/5）。"
            f"实际: {report.summary}"
        )

    def test_each_detection_has_code_path_message(self):
        """每次检测有 code、path、message。"""
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()

        for result in report.results:
            if result.passed:
                assert result.rejection_detail, (
                    f"{result.case_id}: 检测到错误但 rejection_detail 为空"
                )
                assert result.detection_layer in (
                    "validator", "label_validator", "perf_validator",
                ), (
                    f"{result.case_id}: detection_layer 无效——"
                    f"'{result.detection_layer}' 非合法检测层"
                )


# ════════════════════════════════════════════
# 负向回归测试——不相关拒绝 ≠ 成功
# ════════════════════════════════════════════


class TestFallbackRejectionIsNotSuccess:
    """负向回归：当 Validator 返回 blocking 但拒绝码不匹配预期时，passed=False。

    这是方案 A 的核心语义属性——消除"不相关拒绝也算成功"的虚高报告。
    """

    def test_wrong_grain_unrelated_column_rejection_not_passed(self):
        """错粒度——Validator 因列缺失拒绝（非粒度问题）→ passed=False。

        构造一个 plan：引用不存在的列 → Validator 返回 Q-VAL-COL-*，
        而不是粒度相关的拒绝。验证 evaluator 不会将其算作"错粒度已识别"。
        """
        from tianshu_datadev.developer_spec.models import (
            ManifestColumn,
            ManifestTable,
            SourceManifest,
        )
        from tianshu_datadev.harness.models import SemanticErrorType
        from tianshu_datadev.harness.semantic_eval import SemanticEvaluator
        from tianshu_datadev.planning.models import AggregateSpec, AggregationType
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            ScanStep,
            SqlBuildPlan,
        )
        from tianshu_datadev.sql.validator import SqlBuildPlanValidator

        # 构造 manifest——含 t1 表，列: id, date, amount
        manifest = SourceManifest(
            manifest_id="test_neg_grain",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(
                            column_name="id", normalized_name="id", data_type="int",
                        ),
                        ManifestColumn(
                            column_name="date", normalized_name="date",
                            data_type="date",
                        ),
                        ManifestColumn(
                            column_name="amount", normalized_name="amount",
                            data_type="decimal",
                        ),
                    ],
                ),
            ],
        )

        # 构造 plan——引用不存在的列 nonexistent_col
        plan = SqlBuildPlan(
            plan_id="test_neg_grain_plan",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(
                            table_ref="t1", column_name="date",
                            normalized_name="date",
                        ),
                        ColumnRef(
                            table_ref="t1",
                            column_name="nonexistent_col",  # 列不存在 → Q-VAL-COL-
                            normalized_name="nonexistent_col",
                        ),
                    ],
                ),
                AggregateStep(
                    step_id="agg_neg",
                    group_keys=[
                        ColumnRef(
                            table_ref="t1", column_name="date",
                            normalized_name="date",
                        ),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation=AggregationType.SUM,
                            input_column="amount",
                            alias="total",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        # 前置条件：Validator 因列缺失而拒绝（非粒度问题）
        assert passed is False, "前置条件失败：Validator 应拒绝此 plan"
        col_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-COL-" in q.question_id
        ]
        assert len(col_issues) >= 1, (
            "前置条件失败：拒绝原因应为 Q-VAL-COL-*（列缺失），"
            f"实际: {[q.question_id for q in questions]}"
        )

        # 验证拒绝码与粒度完全无关
        grain_keywords = ("Q-VAL-GRAIN-", "粒度", "分组键缺少", "分组键不完整")
        grain_issues = [
            q for q in questions
            if q.blocking and any(
                kw in q.question_id or kw in q.description
                for kw in grain_keywords
            )
        ]
        assert len(grain_issues) == 0, (
            "前置条件失败：拒绝不应与粒度相关——"
            f"发现 grain_keyword 匹配: {[q.question_id for q in grain_issues]}"
        )

        # 核心断言：运行 SemanticEvaluator.run_all()
        # WRONG_GRAIN 的 _eval_wrong_grain 方法会构造自己的 plan。
        # 我们验证：当 Validator 因非粒度原因拒绝时，结果中的 passed=False
        # 且 undetected_errors 包含该 case。
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()

        # 查找 WRONG_GRAIN 的所有结果
        grain_results = [
            r for r in report.results
            if r.error_type == SemanticErrorType.WRONG_GRAIN
        ]
        assert len(grain_results) >= 1, "应有至少一个 WRONG_GRAIN 结果"

        # 如果有 passed=False 的 grain 结果，验证它出现在 undetected_errors 中
        failed_grains = [r for r in grain_results if not r.passed]
        if failed_grains:
            for fg in failed_grains:
                assert fg.rejection_detail, (
                    f"{fg.case_id}: passed=False 时 rejection_detail 不应为空"
                )
                # 验证 undetected_errors 包含此 case
                found_in_undetected = any(
                    fg.case_id in ue for ue in report.undetected_errors
                )
                assert found_in_undetected, (
                    f"{fg.case_id}: passed=False 应出现在 "
                    f"undetected_errors 中。"
                    f"undetected_errors={report.undetected_errors}"
                )

    def test_run_all_undetected_errors_includes_only_real_failures(self):
        """run_all() 的 undetected_errors 仅含 passed=False 的 case。

        验证汇总逻辑：undetected_errors 数量等于 passed=False 的结果数量。
        """
        evaluator = SemanticEvaluator()
        report = evaluator.run_all()

        failed_results = [r for r in report.results if not r.passed]
        assert len(report.undetected_errors) == len(failed_results), (
            f"undetected_errors 数量 ({len(report.undetected_errors)}) "
            f"应与 passed=False 结果数量 ({len(failed_results)}) 一致。"
            f"\nundetected_errors={report.undetected_errors}"
            f"\nfailed_results={[r.case_id for r in failed_results]}"
        )
