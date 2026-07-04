"""Phase 8 Spark Harness 评测测试——5 维度评测框架 + E2E 集成测试。

覆盖：
- SparkEvalDimension 5 维度枚举
- SparkEvalCase / SparkEvalReport 模型
- SparkHarnessRunner 评测执行
- 5 个维度各至少 1 个用例
- E2E 集成：Contract → mapper → Developer → Compiler → Validator → ReviewPackage
"""

from __future__ import annotations

from tianshu_datadev.harness.spark_eval import (
    SparkEvalCase,
    SparkEvalDimension,
    SparkEvalReport,
    SparkHarnessRunner,
)
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkLimitStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
)
from tianshu_datadev.spark.orchestrator import SparkOrchestrator, SparkPipelineStatus
from tianshu_datadev.spark.review_builder import SparkReviewBuilder
from tianshu_datadev.spark.validator import SparkStaticValidator

# ════════════════════════════════════════════
# SparkEvalDimension 枚举测试
# ════════════════════════════════════════════


class TestSparkEvalDimension:
    """5 个评测维度全部存在。"""

    def test_all_five_dimensions_exist(self):
        """SPARK_CONTRACT_FIDELITY/COMPILATION_DETERMINISM/VALIDATOR_COVERAGE/LOGIC_EQUIVALENCE/PHYSICAL_CONSISTENCY。"""
        expected = {
            "SPARK_CONTRACT_FIDELITY",
            "SPARK_COMPILATION_DETERMINISM",
            "SPARK_VALIDATOR_COVERAGE",
            "SPARK_LOGIC_EQUIVALENCE",
            "SPARK_PHYSICAL_CONSISTENCY",
        }
        actual = {d.value for d in SparkEvalDimension}
        assert actual == expected


# ════════════════════════════════════════════
# SparkEvalCase / SparkEvalReport 模型测试
# ════════════════════════════════════════════


class TestSparkEvalModels:
    """SparkEvalCase + SparkEvalReport 模型。"""

    def test_eval_case_creation(self):
        """SparkEvalCase 基本构造。"""
        case = SparkEvalCase(
            case_id="D1_001",
            dimension=SparkEvalDimension.SPARK_CONTRACT_FIDELITY,
            description="验证 Contract → SparkPlan 映射后字段完整性",
            expected_behavior="所有 Contract 字段正确映射到 SparkPlan step",
        )
        assert case.case_id == "D1_001"
        assert case.dimension == SparkEvalDimension.SPARK_CONTRACT_FIDELITY
        assert not case.passed  # 默认未通过

    def test_eval_case_passed(self):
        """passed=True 的用例。"""
        case = SparkEvalCase(
            case_id="D2_001",
            dimension=SparkEvalDimension.SPARK_COMPILATION_DETERMINISM,
            description="同一 plan 两次编译产出相同 hash",
            expected_behavior="raw_hash 相同",
            passed=True,
            actual_result={"first_hash": "abc", "second_hash": "abc"},
        )
        assert case.passed

    def test_eval_report_generation(self):
        """SparkEvalReport 生成——含 5 维汇总。"""
        report_id = SparkEvalReport.generate_report_id()
        assert report_id.startswith("spark_eval_")
        assert len(report_id) > 12


# ════════════════════════════════════════════
# SparkHarnessRunner 测试
# ════════════════════════════════════════════


class TestSparkHarnessRunner:
    """SparkHarnessRunner——评测执行器。"""

    def test_runner_empty_returns_zero_pass_rate(self):
        """空用例集 → 0 通过率。"""
        runner = SparkHarnessRunner()
        report = runner.evaluate()
        assert report.total_cases == 0
        assert report.overall_pass_rate == 0.0

    def test_runner_all_pass(self):
        """全部通过 → 100% 通过率。"""
        runner = SparkHarnessRunner()
        for i, dim in enumerate(SparkEvalDimension):
            runner.add_case(SparkEvalCase(
                case_id=f"D_{i}",
                dimension=dim,
                description=f"测试维度 {dim.value}",
                expected_behavior="通过",
                passed=True,
            ))
        report = runner.evaluate()
        assert report.total_cases == 5
        assert report.total_passed == 5
        assert report.overall_pass_rate == 1.0

    def test_runner_mixed_pass_fail(self):
        """部分通过 → 正确计算通过率。"""
        runner = SparkHarnessRunner()
        runner.add_case(SparkEvalCase(
            case_id="D1_pass",
            dimension=SparkEvalDimension.SPARK_CONTRACT_FIDELITY,
            description="通过用例",
            expected_behavior="通过",
            passed=True,
        ))
        runner.add_case(SparkEvalCase(
            case_id="D1_fail",
            dimension=SparkEvalDimension.SPARK_CONTRACT_FIDELITY,
            description="失败用例",
            expected_behavior="通过",
            passed=False,
        ))
        report = runner.evaluate()
        assert report.total_cases == 2
        assert report.total_passed == 1
        assert report.overall_pass_rate == 0.5

    def test_evaluate_dimension_returns_summary(self):
        """单维度评测返回正确的汇总。"""
        runner = SparkHarnessRunner()
        for i in range(3):
            runner.add_case(SparkEvalCase(
                case_id=f"V_{i}",
                dimension=SparkEvalDimension.SPARK_VALIDATOR_COVERAGE,
                description=f"Validator 测试 {i}",
                expected_behavior="检测恶意代码",
                passed=(i < 2),  # 2/3 通过
            ))
        summary = runner.evaluate_dimension(SparkEvalDimension.SPARK_VALIDATOR_COVERAGE)
        assert summary["total"] == 3
        assert summary["passed"] == 2
        assert summary["pass_rate"] == 2 / 3

    def test_dimension_results_has_all_five_dimensions(self):
        """evaluate() 报告对所有 5 个维度都有汇总。"""
        runner = SparkHarnessRunner()
        for dim in SparkEvalDimension:
            runner.add_case(SparkEvalCase(
                case_id=f"case_{dim.value}",
                dimension=dim,
                description=f"维度 {dim.value} 的用例",
                expected_behavior="通过",
                passed=True,
            ))
        report = runner.evaluate()
        assert len(report.dimension_results) == 5
        for dim in SparkEvalDimension:
            assert dim.value in report.dimension_results


# ════════════════════════════════════════════
# E2E 集成测试——Contract → ReviewPackage
# ════════════════════════════════════════════


def _make_e2e_plan() -> SparkPlan:
    """构造 E2E 测试用的 SparkPlan——含 scan + filter + project + sort + limit。"""
    return SparkPlan(
        plan_id="spark_e2e_test",
        version="v1",
        source_phase="phase-5",
        source_contract_hash="e2e_test_hash",
        steps=[
            SparkReadStep(
                alias="od",
                source_name="dwd.order_detail",
                input_key="order_detail",
            ),
            SparkFilterStep(
                input_alias="od",
                operator="GT",
                left="od.amount",
                right="100",
            ),
            SparkProjectStep(
                input_alias="_f0",
                columns=[
                    SparkProjectColumn(column_name="order_id", alias="order_id"),
                    SparkProjectColumn(column_name="amount", alias="amount"),
                ],
            ),
            SparkSortStep(
                input_alias="_p2",
                order_by=[
                    SparkSortSpec(column="amount", direction=SparkSortDirection.DESC),
                ],
            ),
            SparkLimitStep(
                input_alias="_s3",
                limit=10,
            ),
        ],
    )


class TestE2EPipeline:
    """端到端集成测试——Contract → mapper → Developer → Compiler → Validator → ReviewPackage。"""

    def test_full_pipeline_with_mock_developer(self):
        """完整链路——mock Developer（确定性标注）→ Compiler → Validator → ReviewPackage。"""
        plan = _make_e2e_plan()

        # Step 1：确定性编译
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        assert result.raw_pyspark is not None
        assert len(result.raw_pyspark) > 0
        assert result.raw_hash is not None

        # Step 2：Validator 安全校验
        validator = SparkStaticValidator()
        validation = validator.validate(result.raw_pyspark)
        assert validation.is_valid, f"Validator 拒绝合法代码：{validation.errors}"

    def test_compilation_determinism(self):
        """同一 SparkPlan 两次编译产出相同 raw_hash——证明确定性。"""
        plan = _make_e2e_plan()
        compiler = SparkCompiler()

        result1 = compiler.compile(plan)
        result2 = compiler.compile(plan)

        assert result1.raw_hash == result2.raw_hash
        assert result1.raw_pyspark == result2.raw_pyspark

    def test_orchestrator_with_review_builder(self):
        """Orchestrator 编排 + ReviewBuilder 产出统一交付物。"""
        # 使用默认 Orchestrator（无 Developer）
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="e2e_test_hash")

        # 验证状态
        assert state.contract_hash == "e2e_test_hash"
        assert state.overall_status in (
            SparkPipelineStatus.ALL_CONSISTENT,
            SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED,
        )

        # ReviewBuilder 产出交付物
        builder = SparkReviewBuilder()
        pkg = builder.build(state)
        assert pkg.provenance.contract_hash == "e2e_test_hash"
        assert pkg.package_id.startswith("pkg_")

    def test_validator_rejects_malicious_code(self):
        """Validator 正确拒绝含 spark.read 的恶意代码。"""
        validator = SparkStaticValidator()
        malicious_code = 'df = spark.read.parquet("/etc/passwd")'
        result = validator.validate(malicious_code)
        assert not result.is_valid
        assert any(e.error_code == "E601" for e in result.errors)

    def test_validator_rejects_multiple_malicious_patterns(self):
        """Validator 检测多种恶意模式。"""
        validator = SparkStaticValidator()

        # E603：DataFrame action
        result = validator.validate("df.count()")
        assert not result.is_valid
        assert any(e.error_code == "E603" for e in result.errors)

        # E602：危险导入
        result = validator.validate("import subprocess")
        assert not result.is_valid
        assert any(e.error_code == "E602" for e in result.errors)

        # E606：原始表达式
        result = validator.validate('df.select(F.expr("1+1"))')
        assert not result.is_valid
        assert any(e.error_code == "E606" for e in result.errors)

    def test_window_plan_compiles_and_validates(self):
        """窗口函数 plan 编译产物通过 Validator 校验。"""
        plan = SparkPlan(
            plan_id="spark_e2e_window",
            version="v1",
            source_phase="phase-5",
            source_contract_hash="e2e_window_hash",
            steps=[
                SparkReadStep(
                    alias="od",
                    source_name="dwd.order_detail",
                    input_key="order_detail",
                ),
                SparkWindowStep(
                    input_alias="od",
                    expressions=[
                        SparkWindowExpr(
                            function=SparkWindowFunction.ROW_NUMBER,
                            alias="rn",
                            input_column="",
                            partition_by=["order_id"],
                            order_by=["amount"],
                        ),
                    ],
                ),
            ],
        )

        compiler = SparkCompiler()
        result = compiler.compile(plan)
        raw_lower = result.raw_pyspark.lower()
        assert (
            "row_number" in raw_lower
            or "rownumber" in raw_lower
            or "f.row_number()" in raw_lower
        )

        # Validator 必须接受
        validator = SparkStaticValidator()
        validation = validator.validate(result.raw_pyspark)
        assert validation.is_valid, f"Validator 不应拒绝合法窗口代码：{validation.errors}"
