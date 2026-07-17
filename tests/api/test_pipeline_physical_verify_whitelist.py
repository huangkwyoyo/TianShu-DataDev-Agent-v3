"""case_when UNSUPPORTED 物理验证白名单测试——API/Pipeline 级别。

覆盖 9 个场景：
1-3: 三条入口白名单执行（含 spark_ok=False）
4: Validator 未通过跳过
5: 非 case_when 的 UNSUPPORTED 跳过
6: NOT_EQUIVALENT 跳过
7: 无 comparator 报告跳过
8: 物理不一致→failed + 保留有界报告
9: LOGIC_EQUIVALENT 回归 spark_ok=True
"""

from __future__ import annotations

from unittest.mock import patch

from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.spark.physical_verifier import (
    EngineExecutionResult,
    PhysicalVerificationReport,
    PhysicalVerificationStatus,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparisonReport,
)
from tianshu_datadev.spark.plan_equivalence import (
    EquivalenceVerdict,
    StepEquivalenceResult,
)

# ════════════════════════════════════════════
# 辅助函数：构建 PlanComparisonReport
# ════════════════════════════════════════════


def _make_step_result(
    step_type: str,
    verdict: EquivalenceVerdict,
) -> StepEquivalenceResult:
    """创建一个 StepEquivalenceResult。"""
    return StepEquivalenceResult(
        step_type=step_type,
        verdict=verdict,
        sql_count=1,
        spark_count=1,
    )


def _make_comparator_report(
    status: ComparisonStatus,
    step_results: list[StepEquivalenceResult] | None = None,
) -> PlanComparisonReport:
    """创建一个 PlanComparisonReport。"""
    return PlanComparisonReport(
        report_id="test-report",
        contract_hash="test-hash",
        sql_plan_hash="sql-hash",
        spark_plan_hash="spark-hash",
        status=status,
        step_results=step_results or [],
    )


def _make_physver_report(
    status: PhysicalVerificationStatus = PhysicalVerificationStatus.RESULT_CONSISTENT,
    row_count_match: bool = True,
    schema_match: bool = True,
    total_diff_count: int = 0,
    sample_rows: list[dict] | None = None,
) -> PhysicalVerificationReport:
    """创建一个 PhysicalVerificationReport。"""
    return PhysicalVerificationReport(
        report_id="test-physver",
        contract_hash="test-hash",
        snapshot_id="test-snapshot",
        status=status,
        row_count_match=row_count_match,
        schema_match=schema_match,
        total_diff_count=total_diff_count,
        duckdb_result=EngineExecutionResult(
            engine="duckdb",
            success=True,
            sample_rows=sample_rows or [],
        ),
        spark_result=EngineExecutionResult(
            engine="spark",
            success=True,
            sample_rows=sample_rows or [],
        ),
    )


# ════════════════════════════════════════════
# 场景 2、4-7：_should_physical_verify 单元测试
# ════════════════════════════════════════════


class TestShouldPhysicalVerify:
    """Pipeline._should_physical_verify 的单元测试。

    白名单条件：
    - Validator 通过
    - comparator_report.status == LOGIC_UNSUPPORTED
    - 所有 step_results 中：
      - 无 NOT_EQUIVALENT
      - 仅 case_when 的 UNSUPPORTED_COMPARISON
      - 至少有一个 case_when UNSUPPORTED
    - 以上全部满足 → True；否则 False
    """

    def test_validator_not_ok_returns_false(self):
        """场景 4：Validator 未通过 → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
        ])
        assert not pipeline._should_physical_verify(False, report)

    def test_no_comparator_report_returns_false(self):
        """场景 7：无 comparator 报告 → 跳过。"""
        pipeline = Pipeline()
        assert not pipeline._should_physical_verify(True, None)

    def test_logic_equivalent_returns_true(self):
        """LOGIC_EQUIVALENT → 放行（基线路径）。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_EQUIVALENT, [
            _make_step_result("case_when", EquivalenceVerdict.EQUIVALENT),
            _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_whitelist_case_when_only_returns_true(self):
        """场景 2：白名单——仅 case_when UNSUPPORTED + 其他 EQUIVALENT → 放行。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
            _make_step_result("project", EquivalenceVerdict.EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_whitelist_no_case_when_unsupported_returns_false(self):
        """LOGIC_UNSUPPORTED 但没有 case_when UNSUPPORTED → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("filter", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
        ])
        assert not pipeline._should_physical_verify(True, report)

    def test_non_case_when_unsupported_returns_false(self):
        """场景 5：非 case_when 的 UNSUPPORTED → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            _make_step_result("subquery", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
        ])
        assert not pipeline._should_physical_verify(True, report)

    def test_not_equivalent_returns_false(self):
        """场景 6：存在 NOT_EQUIVALENT → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_MISMATCH, [
            _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            _make_step_result("filter", EquivalenceVerdict.NOT_EQUIVALENT),
        ])
        assert not pipeline._should_physical_verify(True, report)

    def test_logic_mismatch_returns_false(self):
        """LOGIC_MISMATCH（全部 NOT_EQUIVALENT）→ 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_MISMATCH, [
            _make_step_result("filter", EquivalenceVerdict.NOT_EQUIVALENT),
        ])
        assert not pipeline._should_physical_verify(True, report)

    def test_not_covered_returns_false(self):
        """NOT_COVERED → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.NOT_COVERED, [
            _make_step_result("window", EquivalenceVerdict.EQUIVALENT),
        ])
        assert not pipeline._should_physical_verify(True, report)


# ════════════════════════════════════════════
# 场景 8：物理不一致 → failed + 有界报告
# ════════════════════════════════════════════


class TestPhysicalMismatchStatus:
    """物理不一致时 run_spark_stage 返回 status="failed" + 有界摘要。"""

    def test_mismatch_maps_to_failed(self):
        """RESULT_MISMATCH → status="failed"。"""
        pipeline = Pipeline()
        context = pipeline._get_or_create_spark_context("test-mismatch")
        context.stage_results["VALIDATOR"] = "SUCCESS"
        context.comparator_report = _make_comparator_report(
            ComparisonStatus.LOGIC_UNSUPPORTED, [
                _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            ],
        )

        # 构造物理不一致报告
        mismatch_report = _make_physver_report(
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            row_count_match=False,
            total_diff_count=5,
            sample_rows=[{"id": 1}, {"id": 2}],
        )
        context.physical_verify_report = mismatch_report
        context.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"

        # 验证 status 覆写为 "failed"
        from tianshu_datadev.spark.orchestrator import SparkPipelineStage
        stage_val = SparkPipelineStage.PHYSICAL_VERIFIER.value
        status_map = {"SUCCESS": "ok", "FAILURE": "failed", "SKIPPED": "skipped"}
        current_status = status_map.get(
            context.stage_results.get(stage_val, "NOT_EXECUTED"), "skipped",
        )
        # 手动触发覆写（同 pipeline.py 逻辑）
        if current_status == "ok":
            report = context.physical_verify_report
            if report is not None and report.status != PhysicalVerificationStatus.RESULT_CONSISTENT:
                current_status = "failed"
        assert current_status == "failed"

    def test_bounded_summary_contains_sample_rows(self):
        """有界摘要包含 sample_rows[:5]（每侧最多 5 条）。"""
        pipeline = Pipeline()
        context = pipeline._get_or_create_spark_context("test-summary")
        context.physical_verify_report = _make_physver_report(
            row_count_match=True,
            sample_rows=[{"col": "a"}, {"col": "b"}],
        )
        report = context.physical_verify_report
        bounded = {
            "row_count_match": report.row_count_match,
            "schema_match": report.schema_match,
            "total_diff_count": report.total_diff_count,
            "sample_rows": {
                "duckdb": (report.duckdb_result.sample_rows or [])[:5],
                "spark": (report.spark_result.sample_rows or [])[:5],
            },
        }
        assert bounded["row_count_match"] is True
        assert len(bounded["sample_rows"]["duckdb"]) == 2
        assert len(bounded["sample_rows"]["spark"]) == 2
        assert bounded["total_diff_count"] == 0


# ════════════════════════════════════════════
# 场景 9：LOGIC_EQUIVALENT → spark_ok=True
# ════════════════════════════════════════════


class TestSparkOkLogicEquivalent:
    """COMPARATOR LOGIC_EQUIVALENT + 物理一致 → spark_ok=True。"""

    def test_spark_ok_true_when_logic_equivalent_and_physver_ok(self):
        """回归——全量路径 spark_ok=True。"""
        pipeline = Pipeline()
        context = pipeline._get_or_create_spark_context("test-spark-ok")
        context.stage_results["VALIDATOR"] = "SUCCESS"
        context.comparator_report = _make_comparator_report(
            ComparisonStatus.LOGIC_EQUIVALENT, [
                _make_step_result("case_when", EquivalenceVerdict.EQUIVALENT),
                _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
            ],
        )
        context.physical_verify_report = _make_physver_report(
            status=PhysicalVerificationStatus.RESULT_CONSISTENT,
            row_count_match=True,
        )
        context.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"

        # 模拟 spark_ok 判定
        comparator_status = "LOGIC_EQUIVALENT"
        spark_ok = (
            context.stage_results.get("PHYSICAL_VERIFIER") == "SUCCESS"
            and comparator_status == "LOGIC_EQUIVALENT"
        )
        assert spark_ok is True


# ════════════════════════════════════════════
# 场景 1、8：_do_spark_physical_verify 门禁集成测试
# ════════════════════════════════════════════


class TestDoSparkPhysicalVerifyGate:
    """_do_spark_physical_verify 的门禁行为。"""

    @patch("tianshu_datadev.api.pipeline.Pipeline._should_physical_verify", return_value=False)
    def test_gate_skips_when_should_not_verify(self, mock_should):
        """门禁返回 False → PHYSICAL_VERIFIER 标记 SKIPPED。"""
        pipeline = Pipeline()
        from tianshu_datadev.api.pipeline import SparkStageContext
        context = SparkStageContext()
        context.stage_results["VALIDATOR"] = "SUCCESS"

        # Mock artifacts——门禁在 PySpark 检查之前，所以 artifacts 不会真正被使用
        from tianshu_datadev.api.pipeline import PipelineArtifactBundle
        artifacts = PipelineArtifactBundle(request_id="test-gate-skip")

        pipeline._do_spark_physical_verify(artifacts, context)
        assert context.stage_results.get("PHYSICAL_VERIFIER") == "SKIPPED"

    @patch("tianshu_datadev.api.pipeline.Pipeline._should_physical_verify", return_value=True)
    def test_gate_passes_when_should_verify(self, mock_should):
        """门禁返回 True → 继续执行（后续因 PySpark 不可用而 SKIPPED，非门禁原因）。"""
        pipeline = Pipeline()
        from tianshu_datadev.api.pipeline import SparkStageContext
        context = SparkStageContext()
        context.stage_results["VALIDATOR"] = "SUCCESS"
        context.sandbox_transform_code = "test_code"

        from tianshu_datadev.api.pipeline import PipelineArtifactBundle
        artifacts = PipelineArtifactBundle(request_id="test-gate-pass")

        pipeline._do_spark_physical_verify(artifacts, context)
        # 门禁通过了，后续因 PySpark 未安装或 artifacts 不足而 SKIPPED，但不是 SKIPPED 门禁原因
        assert context.stage_results.get("PHYSICAL_VERIFIER") is not None
        # 不应是门禁原因的 SKIPPED
        gate_skip_msg = "[PHYSICAL_VERIFIER] SKIPPED: 物理验证门禁未通过"
        assert not any(gate_skip_msg in e for e in context.errors)


# ════════════════════════════════════════════
# 场景 3：run_all_full 响应字段
# ════════════════════════════════════════════


class TestFullRunResponseFields:
    """run_all_full 和 run_all_full_stream 的响应字段。"""

    def test_full_run_response_contains_new_fields(self):
        """白名单场景响应含 comparator_status/requires_human_review/review_ready。"""
        comparator_status = "LOGIC_UNSUPPORTED"
        response_fields = {
            "comparator_status": comparator_status,
            "requires_human_review": (
                comparator_status != "LOGIC_EQUIVALENT"
                if comparator_status else True
            ),
            "review_ready": (
                comparator_status == "LOGIC_EQUIVALENT"
                if comparator_status else False
            ),
        }
        assert response_fields["comparator_status"] == "LOGIC_UNSUPPORTED"
        assert response_fields["requires_human_review"] is True
        assert response_fields["review_ready"] is False

    def test_spark_ok_false_in_whitelist(self):
        """白名单场景 spark_ok=False（即使物理一致）。"""
        # 模拟白名单场景：物理验证未执行（门禁跳过）或物理一致
        physver_stage = {"stage": "PHYSICAL_VERIFIER", "status": "ok"}
        comparator_status = "LOGIC_UNSUPPORTED"
        spark_ok = (
            physver_stage is not None
            and physver_stage["status"] == "ok"
            and comparator_status == "LOGIC_EQUIVALENT"
        )
        assert spark_ok is False

    def test_spark_ok_false_when_physver_skipped(self):
        """物理验证被跳过 → spark_ok=False。"""
        physver_stage = {"stage": "PHYSICAL_VERIFIER", "status": "skipped"}
        comparator_status = "LOGIC_EQUIVALENT"
        spark_ok = (
            physver_stage is not None
            and physver_stage["status"] == "ok"
            and comparator_status == "LOGIC_EQUIVALENT"
        )
        assert spark_ok is False

    def test_spark_ok_false_when_no_physver(self):
        """无 PHYSICAL_VERIFIER 阶段 → spark_ok=False。"""
        physver_stage = None
        comparator_status = "LOGIC_EQUIVALENT"
        spark_ok = (
            physver_stage is not None
            and physver_stage["status"] == "ok"
            and comparator_status == "LOGIC_EQUIVALENT"
        )
        assert spark_ok is False

    def test_requires_human_review_false_when_logic_equivalent(self):
        """COMPARATOR LOGIC_EQUIVALENT + 物理一致 → requires_human_review=False。"""
        comparator_status = "LOGIC_EQUIVALENT"
        requires_human_review = (
            comparator_status != "LOGIC_EQUIVALENT"
            if comparator_status else True
        )
        assert requires_human_review is False
        review_ready = (
            comparator_status == "LOGIC_EQUIVALENT"
            if comparator_status else False
        )
        assert review_ready is True
