"""物理验证门禁测试——API/Pipeline 级别。

_should_physical_verify 门禁逻辑：
- Validator 未通过 / report=None / NOT_EXECUTED → False（阻止物理验证）
- 所有其他 COMPARATOR 状态 → True（物理验证是 ground truth）

覆盖：
- _should_physical_verify 单元测试（7 场景）
- run_spark_stage 入口测试（物理不一致→failed + 有界摘要）
- _do_spark_physical_verify 门禁集成测试
- run_all_full spark_ok 判定测试
"""

from __future__ import annotations

from unittest.mock import patch

from tianshu_datadev.api.pipeline import Pipeline, PipelineArtifactBundle
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

    物理验证是 ground truth——只要 Validator 通过且 Comparator 已执行，
    就应该允许物理验证运行。仅以下情况跳过：
    - Validator 未通过（安全风险）
    - Comparator 报告为 None（无逻辑对比基线）
    - Comparator 状态为 NOT_EXECUTED（逻辑对比未实际执行）
    """

    def test_validator_not_ok_returns_false(self):
        """Validator 未通过 → 跳过。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_EQUIVALENT, [
            _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
        ])
        assert not pipeline._should_physical_verify(False, report)

    def test_no_comparator_report_returns_false(self):
        """无 comparator 报告 → 跳过。"""
        pipeline = Pipeline()
        assert not pipeline._should_physical_verify(True, None)

    def test_not_executed_returns_false(self):
        """NOT_EXECUTED → 跳过（逻辑对比未实际执行）。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.NOT_EXECUTED, [
            _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
        ])
        assert not pipeline._should_physical_verify(True, report)

    def test_logic_equivalent_returns_true(self):
        """LOGIC_EQUIVALENT → 放行（基线路径）。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_EQUIVALENT, [
            _make_step_result("case_when", EquivalenceVerdict.EQUIVALENT),
            _make_step_result("filter", EquivalenceVerdict.EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_logic_unsupported_returns_true(self):
        """LOGIC_UNSUPPORTED → 放行——物理验证是唯一验证手段。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("filter", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_logic_unsupported_with_not_equivalent_returns_true(self):
        """LOGIC_UNSUPPORTED 含 NOT_EQUIVALENT → 仍放行。
        NOT_EQUIVALENT 可能是 UNSUPPORTED 步骤的副作用（如缺失列导致
        project 不等价），物理验证提供 ground truth 确认这些差异的实际影响。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_UNSUPPORTED, [
            _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            _make_step_result("project", EquivalenceVerdict.NOT_EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_logic_mismatch_returns_true(self):
        """LOGIC_MISMATCH → 放行——物理验证确认不等价是否影响实际输出。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.LOGIC_MISMATCH, [
            _make_step_result("filter", EquivalenceVerdict.NOT_EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)

    def test_not_covered_returns_true(self):
        """NOT_COVERED → 放行——物理验证提供兜底保障。"""
        pipeline = Pipeline()
        report = _make_comparator_report(ComparisonStatus.NOT_COVERED, [
            _make_step_result("window", EquivalenceVerdict.EQUIVALENT),
        ])
        assert pipeline._should_physical_verify(True, report)


# ════════════════════════════════════════════
# 场景 8、9：真实 run_spark_stage 入口测试（mock _do_spark_physical_verify）
# ════════════════════════════════════════════


class TestRunSparkStagePhysicalVerify:
    """通过真实 run_spark_stage() 入口测试物理不一致→failed 和有界摘要。"""

    @patch.object(Pipeline, "export_artifacts")
    @patch.object(Pipeline, "_check_stage_dependencies")
    def test_physical_mismatch_status_failed(
        self, mock_check_deps, mock_export,
    ):
        """RESULT_MISMATCH → run_spark_stage 返回 status='failed' + 有界摘要。"""
        mock_export.return_value = PipelineArtifactBundle(request_id="test-entry-mismatch")

        pipeline = Pipeline()
        request_id = "test-entry-mismatch"
        context = pipeline._get_or_create_spark_context(request_id)
        context.stage_results["VALIDATOR"] = "SUCCESS"
        context.comparator_report = _make_comparator_report(
            ComparisonStatus.LOGIC_UNSUPPORTED, [
                _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            ],
        )

        # mock _do_spark_physical_verify：设置状态 + 物理不一致报告
        with patch.object(pipeline, "_do_spark_physical_verify") as mock_do:
            def _set_mismatch(artifacts, ctx):
                ctx.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"
                ctx.physical_verify_report = _make_physver_report(
                    status=PhysicalVerificationStatus.RESULT_MISMATCH,
                    row_count_match=False,
                    total_diff_count=5,
                    sample_rows=[{"id": 1}, {"id": 2}],
                )
            mock_do.side_effect = _set_mismatch

            from tianshu_datadev.spark.orchestrator import SparkPipelineStage
            result = pipeline.run_spark_stage(
                request_id, SparkPipelineStage.PHYSICAL_VERIFIER,
            )

        assert result["status"] == "failed", (
            f"物理不一致时 status 应为 'failed'，实际为 {result['status']}"
        )
        r = result["result"]
        assert r["type"] == "physical_verify"
        assert r["row_count_match"] is False
        assert r["total_diff_count"] == 5
        assert r["sample_rows"]["duckdb"] == [{"id": 1}, {"id": 2}]
        assert r["sample_rows"]["spark"] == [{"id": 1}, {"id": 2}]

    @patch.object(Pipeline, "export_artifacts")
    @patch.object(Pipeline, "_check_stage_dependencies")
    def test_single_engine_result_none_does_not_crash(
        self, mock_check_deps, mock_export,
    ):
        """duckdb_result=None 时有界摘要不会 AttributeError，返回空列表。"""
        mock_export.return_value = PipelineArtifactBundle(request_id="test-entry-none")

        pipeline = Pipeline()
        request_id = "test-entry-none"
        context = pipeline._get_or_create_spark_context(request_id)
        context.stage_results["VALIDATOR"] = "SUCCESS"
        context.comparator_report = _make_comparator_report(
            ComparisonStatus.LOGIC_UNSUPPORTED, [
                _make_step_result("case_when", EquivalenceVerdict.UNSUPPORTED_COMPARISON),
            ],
        )

        with patch.object(pipeline, "_do_spark_physical_verify") as mock_do:
            def _set_none_side(artifacts, ctx):
                ctx.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"
                # duckdb_result=None，仅 spark 侧有结果
                ctx.physical_verify_report = PhysicalVerificationReport(
                    report_id="test-none",
                    contract_hash="test",
                    snapshot_id="test",
                    status=PhysicalVerificationStatus.RESULT_MISMATCH,
                    row_count_match=False,
                    total_diff_count=3,
                    duckdb_result=None,
                    spark_result=EngineExecutionResult(
                        engine="spark", success=True,
                        sample_rows=[{"col": "b"}],
                    ),
                )
            mock_do.side_effect = _set_none_side

            from tianshu_datadev.spark.orchestrator import SparkPipelineStage
            result = pipeline.run_spark_stage(
                request_id, SparkPipelineStage.PHYSICAL_VERIFIER,
            )

        # 不崩溃 + duckdb 侧空列表
        assert result["status"] == "failed"
        r = result["result"]
        assert r["sample_rows"]["duckdb"] == []
        assert r["sample_rows"]["spark"] == [{"col": "b"}]
        assert r["row_count_match"] is False
        assert r["total_diff_count"] == 3


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


class TestPhysicalVerifySafetyHelpers:
    """覆盖物理验证入口曾遗漏的排序键与受控快照路径。"""

    def test_extract_sort_step_order_keys(self):
        """SparkSortStep 应读取 order_by.column，不访问不存在的 columns。"""
        from tianshu_datadev.api.pipeline import _extract_spark_order_keys
        from tianshu_datadev.spark.models import (
            SparkPlan,
            SparkSortDirection,
            SparkSortSpec,
            SparkSortStep,
        )

        plan = SparkPlan(
            plan_id="sort_plan",
            source_contract_hash="contract_hash",
            steps=[SparkSortStep(
                input_alias="f1",
                order_by=[
                    SparkSortSpec(column="borough", direction=SparkSortDirection.ASC),
                    SparkSortSpec(column="trip_count", direction=SparkSortDirection.DESC),
                ],
            )],
        )

        assert _extract_spark_order_keys(plan) == ["borough", "trip_count"]

    def test_duckdb_snapshot_fallback_uses_controlled_builder(self):
        """DuckDB 备选路径必须委托统一快照构建，禁止自行全表导出。"""
        from tianshu_datadev.api.pipeline import SparkStageContext
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            DataTransformContractLite,
        )
        from tianshu_datadev.spark.snapshot import SnapshotManifest

        contract = DataTransformContractLite(
            contract_id="contract_safe_snapshot",
            source_sqlbuildplan_hash="plan_hash",
            input_tables=[ContractInputTable(
                table_ref="ft",
                source_table="gold.fact_trips",
            )],
        )
        artifacts = PipelineArtifactBundle(
            request_id="req_safe_snapshot",
            data_transform_contract=contract,
        )
        manifest = SnapshotManifest(
            snapshot_id="snap_safe",
            contract_hash="hash",
            snapshot_dir="snapshot_dir",
            files=[],
        )
        pipeline = Pipeline(duckdb_path="warehouse.duckdb")
        pipeline._results[artifacts.request_id] = {
            "table_mapping": {"ft": "gold.fact_trips"},
        }

        with patch.object(
            pipeline,
            "_prepare_run_all_snapshot",
            return_value=(manifest, {}),
        ) as prepare:
            result = pipeline._build_snapshot_from_duckdb(
                artifacts,
                SparkStageContext(),
            )

        assert result is manifest
        assert artifacts.snapshot_manifest is manifest
        prepare.assert_called_once_with(
            contract=contract,
            table_mapping={"ft": "gold.fact_trips"},
            table_paths=None,
            spec=None,
        )

    def test_empty_filtered_snapshot_has_explicit_physical_status(self):
        """过滤后空快照必须显式进入人工审查状态，不能当作双引擎一致。"""
        from tianshu_datadev.api.pipeline import SparkStageContext
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            DataTransformContractLite,
        )
        from tianshu_datadev.spark.snapshot import SnapshotEmptyForFilterError

        contract = DataTransformContractLite(
            contract_id="contract_empty_snapshot",
            source_sqlbuildplan_hash="plan_hash",
            input_tables=[ContractInputTable(
                table_ref="ft",
                source_table="gold.fact_trips",
            )],
        )
        artifacts = PipelineArtifactBundle(
            request_id="req_empty_snapshot",
            data_transform_contract=contract,
        )
        pipeline = Pipeline(duckdb_path="warehouse.duckdb")
        pipeline._results[artifacts.request_id] = {}
        context = SparkStageContext()

        with patch.object(
            pipeline,
            "_prepare_run_all_snapshot",
            side_effect=SnapshotEmptyForFilterError(
                "[SNAPSHOT_EMPTY_FOR_FILTER] no rows"
            ),
        ):
            result = pipeline._build_snapshot_from_duckdb(artifacts, context)

        assert result is None
        assert context.stage_results["PHYSICAL_VERIFIER"] == (
            "SNAPSHOT_EMPTY_FOR_FILTER"
        )
        assert any("SNAPSHOT_EMPTY_FOR_FILTER" in error for error in context.errors)


# ════════════════════════════════════════════
# 场景 3：真实 run_all_full 入口 spark_ok 判定测试
# ════════════════════════════════════════════


class TestFullRunResponseSparkOk:
    """通过真实 run_all_full() 入口测试 spark_ok 判定和响应字段。"""

    @staticmethod
    def _mock_stage_results(
        comparator_status: str,
        physver_status: str,
    ) -> list[dict]:
        """构建 6 阶段 run_spark_stage 的 side_effect 返回值。"""
        return [
            {"status": "ok", "result": {"type": "mapper"}, "llm_traces": {}, "errors": []},
            {"status": "ok", "result": {"type": "developer"}, "llm_traces": {}, "errors": []},
            {
                "status": "ok",
                "result": {"type": "compiler", "pyspark_code": "code"},
                "llm_traces": {},
                "errors": [],
            },
            {"status": "ok", "result": {"type": "validator"}, "llm_traces": {}, "errors": []},
            {
                "status": "ok",
                "result": {"type": "comparator", "status": comparator_status},
                "llm_traces": {},
                "errors": [],
            },
            {"status": physver_status, "result": {"type": "physical_verify"}, "llm_traces": {}, "errors": []},
        ]

    @staticmethod
    def _setup_context(pipeline: Pipeline, request_id: str) -> None:
        """预填 SparkStageContext 的关键阶段结果——供 _compute_review_ready 消费。"""
        ctx = pipeline._get_or_create_spark_context(request_id)
        ctx.stage_results.update({
            "MAPPER": "SUCCESS",
            "COMPILER": "SUCCESS",
            "VALIDATOR": "SUCCESS",
            "COMPARATOR": "SUCCESS",
        })

    @patch.object(Pipeline, "run_spark_stage")
    @patch.object(Pipeline, "run_all")
    def test_spark_ok_true_when_logic_equivalent(
        self, mock_run_all, mock_run_spark_stage,
    ):
        """COMPARATOR LOGIC_EQUIVALENT + 物理一致 → spark_ok=True。"""
        mock_run_all.return_value = {
            "request_id": "test-ok",
            "pipeline_error": None,
            "generated_sql": "SELECT 1",
            "llm_traces": {},
        }
        mock_run_spark_stage.side_effect = self._mock_stage_results(
            comparator_status="LOGIC_EQUIVALENT", physver_status="ok",
        )

        pipeline = Pipeline()
        self._setup_context(pipeline, "test-ok")
        result = pipeline.run_all_full("test markdown")

        assert result["spark_ok"] is True, (
            f"LOGIC_EQUIVALENT + 物理一致时 spark_ok 应为 True，"
            f"实际为 {result['spark_ok']}"
        )
        assert result["comparator_status"] == "LOGIC_EQUIVALENT"
        assert result["requires_human_review"] is False
        assert result["review_ready"] is True

    @patch.object(Pipeline, "run_spark_stage")
    @patch.object(Pipeline, "run_all")
    def test_spark_ok_false_whitelist(
        self, mock_run_all, mock_run_spark_stage,
    ):
        """白名单场景（LOGIC_UNSUPPORTED + 物理一致）→ spark_ok=False。"""
        mock_run_all.return_value = {
            "request_id": "test-whitelist",
            "pipeline_error": None,
            "generated_sql": "SELECT 1",
            "llm_traces": {},
        }
        mock_run_spark_stage.side_effect = self._mock_stage_results(
            comparator_status="LOGIC_UNSUPPORTED", physver_status="ok",
        )

        pipeline = Pipeline()
        self._setup_context(pipeline, "test-whitelist")
        result = pipeline.run_all_full("test markdown")

        assert result["spark_ok"] is False, (
            f"白名单场景 spark_ok 应为 False，实际为 {result['spark_ok']}"
        )
        assert result["comparator_status"] == "LOGIC_UNSUPPORTED"
        assert result["requires_human_review"] is True
        assert result["review_ready"] is False

    @patch.object(Pipeline, "run_spark_stage")
    @patch.object(Pipeline, "run_all")
    def test_spark_ok_false_physver_skipped(
        self, mock_run_all, mock_run_spark_stage,
    ):
        """物理验证被跳过 → spark_ok=False。"""
        mock_run_all.return_value = {
            "request_id": "test-skipped",
            "pipeline_error": None,
            "generated_sql": "SELECT 1",
            "llm_traces": {},
        }
        mock_run_spark_stage.side_effect = self._mock_stage_results(
            comparator_status="LOGIC_EQUIVALENT", physver_status="skipped",
        )

        pipeline = Pipeline()
        self._setup_context(pipeline, "test-skipped")
        result = pipeline.run_all_full("test markdown")

        assert result["spark_ok"] is False, (
            f"物理跳过时 spark_ok 应为 False，实际为 {result['spark_ok']}"
        )
