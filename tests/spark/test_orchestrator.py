"""Phase 8 SparkOrchestrator 测试——Pipeline 状态机 + 全链路编排。

覆盖：
- SparkPipelineStage 6 阶段枚举
- SparkPipelineState 状态模型 + 全局状态流转
- SparkOrchestrator 编排逻辑（各阶段调用顺序 + 重试上限）
- 阶段失败 → RepairPlanner 分类 → 重试/人工审查
"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.orchestrator import (
    SparkOrchestrator,
    SparkPipelineStage,
    SparkPipelineState,
    SparkPipelineStatus,
)

# ════════════════════════════════════════════
# SparkPipelineStage 枚举测试
# ════════════════════════════════════════════


class TestSparkPipelineStage:
    """SparkPipelineStage 6 阶段枚举——覆盖全链路。"""

    def test_all_six_stages_exist(self):
        """6 个阶段全部存在：MAPPER/DEVELOPER/COMPILER/VALIDATOR/COMPARATOR/PHYSICAL_VERIFIER。"""
        expected = {
            "MAPPER",
            "DEVELOPER",
            "COMPILER",
            "VALIDATOR",
            "COMPARATOR",
            "PHYSICAL_VERIFIER",
        }
        actual = {s.value for s in SparkPipelineStage}
        assert actual == expected

    def test_stage_order_follows_pipeline_sequence(self):
        """阶段枚举值按 pipeline 执行顺序排列。"""
        stages = list(SparkPipelineStage)
        assert stages[0] == SparkPipelineStage.MAPPER
        assert stages[1] == SparkPipelineStage.DEVELOPER
        assert stages[2] == SparkPipelineStage.COMPILER
        assert stages[3] == SparkPipelineStage.VALIDATOR
        assert stages[4] == SparkPipelineStage.COMPARATOR
        assert stages[5] == SparkPipelineStage.PHYSICAL_VERIFIER


# ════════════════════════════════════════════
# SparkPipelineStatus 枚举测试
# ════════════════════════════════════════════


class TestSparkPipelineStatus:
    """SparkPipelineStatus——4 种全局状态，禁止泛化 PASS。"""

    def test_all_four_statuses_exist(self):
        """4 种全局状态：ALL_CONSISTENT / LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED /
        REPAIR_NEEDED / HUMAN_REVIEW_REQUIRED。"""
        expected = {
            "ALL_CONSISTENT",
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED",
            "REPAIR_NEEDED",
            "HUMAN_REVIEW_REQUIRED",
        }
        actual = {s.value for s in SparkPipelineStatus}
        assert actual == expected

    def test_no_pass_status(self):
        """禁止泛化 PASS 状态——所有状态名精确描述实际结论。"""
        for status in SparkPipelineStatus:
            assert "PASS" not in status.value
            assert status.value != "Go"
            assert status.value != "No-Go"


# ════════════════════════════════════════════
# SparkPipelineState 模型测试
# ════════════════════════════════════════════


class TestSparkPipelineState:
    """SparkPipelineState 状态模型——记录每阶段输入/输出/错误。"""

    def test_initial_state_all_not_executed(self):
        """初始状态——所有阶段标记 NOT_EXECUTED。"""
        state = SparkPipelineState(contract_hash="test_hash")
        assert state.contract_hash == "test_hash"
        for stage in SparkPipelineStage:
            stage_status = state.stage_results.get(stage.value)
            assert stage_status is not None
            assert stage_status == "NOT_EXECUTED"

    def test_stage_result_recording(self):
        """阶段执行后可记录结果状态。"""
        state = SparkPipelineState(contract_hash="test_hash")
        state.record_stage_result(SparkPipelineStage.MAPPER, "SUCCESS")
        assert state.stage_results["MAPPER"] == "SUCCESS"
        assert state.stage_results["DEVELOPER"] == "NOT_EXECUTED"

    def test_overall_status_derivation_all_consistent(self):
        """全部通过 → ALL_CONSISTENT。"""
        state = SparkPipelineState(contract_hash="test_hash")
        # 模拟全部阶段成功
        for stage in SparkPipelineStage:
            state.record_stage_result(stage, "SUCCESS")
        state.derive_overall_status()
        assert state.overall_status == SparkPipelineStatus.ALL_CONSISTENT

    def test_overall_status_derivation_repair_needed(self):
        """任意阶段失败 → REPAIR_NEEDED。"""
        state = SparkPipelineState(contract_hash="test_hash")
        state.record_stage_result(SparkPipelineStage.MAPPER, "SUCCESS")
        state.record_stage_result(SparkPipelineStage.COMPILER, "FAILURE")
        state.derive_overall_status()
        assert state.overall_status == SparkPipelineStatus.REPAIR_NEEDED

    def test_overall_status_derivation_human_review(self):
        """HUMAN_REVIEW 标记 → HUMAN_REVIEW_REQUIRED。"""
        state = SparkPipelineState(contract_hash="test_hash")
        state.record_stage_result(SparkPipelineStage.MAPPER, "SUCCESS")
        state.record_stage_result(SparkPipelineStage.DEVELOPER, "HUMAN_REVIEW")
        state.derive_overall_status()
        assert state.overall_status == SparkPipelineStatus.HUMAN_REVIEW_REQUIRED

    def test_overall_status_logic_consistent_physical_not_executed(self):
        """逻辑链路通过但物理未执行 → LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED。"""
        state = SparkPipelineState(contract_hash="test_hash")
        for stage in [
            SparkPipelineStage.MAPPER,
            SparkPipelineStage.DEVELOPER,
            SparkPipelineStage.COMPILER,
            SparkPipelineStage.VALIDATOR,
            SparkPipelineStage.COMPARATOR,
        ]:
            state.record_stage_result(stage, "SUCCESS")
        # PHYSICAL_VERIFIER 保持 NOT_EXECUTED
        state.derive_overall_status()
        assert state.overall_status == SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED

    def test_derive_overall_status_checks_comparator_report_not_just_stage_result(self):
        """COMPARATOR stage_result=SUCCESS 但 comparator_report.status=LOGIC_MISMATCH
        时，overall_status 不得显示为逻辑一致。

        这是对 RC2 的回归测试——derive_overall_status() 不能仅检查
        stage_results["COMPARATOR"] == "SUCCESS"，
        必须同时验证 comparator_report.status 是否为 LOGIC_EQUIVALENT。
        """
        from tianshu_datadev.spark.plan_comparator import (
            ComparisonStatus,
            PlanComparisonReport,
        )

        state = SparkPipelineState(contract_hash="test_hash")
        # 模拟所有逻辑阶段 SUCCESS（包括 COMPARATOR）
        for stage in [
            SparkPipelineStage.MAPPER,
            SparkPipelineStage.DEVELOPER,
            SparkPipelineStage.COMPILER,
            SparkPipelineStage.VALIDATOR,
            SparkPipelineStage.COMPARATOR,
        ]:
            state.record_stage_result(stage, "SUCCESS")
        # PHYSICAL_VERIFIER 未执行
        state.record_stage_result(SparkPipelineStage.PHYSICAL_VERIFIER, "NOT_EXECUTED")

        # 注入 comparator_report——status 为 LOGIC_MISMATCH
        state.comparator_report = PlanComparisonReport(
            report_id="test_report_mismatch",
            contract_hash="test_hash",
            sql_plan_hash="abc123",
            spark_plan_hash="def456",
            status=ComparisonStatus.LOGIC_MISMATCH,
            step_results=[],
        )

        state.derive_overall_status()

        # 关键断言：comparator_report.status=LOGIC_MISMATCH 时，
        # overall_status 不得为 LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
        # （即不得声称"逻辑一致"）
        logic_consistent_statuses = {
            SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED,
            SparkPipelineStatus.ALL_CONSISTENT,
        }
        assert state.overall_status not in logic_consistent_statuses, (
            f"comparator_report.status=LOGIC_MISMATCH 时 "
            f"overall_status 不得显示为逻辑一致，"
            f"实际 overall_status={state.overall_status}"
        )

    def test_retry_count_tracking(self):
        """返工计数从 0 开始，可递增。"""
        state = SparkPipelineState(contract_hash="test_hash")
        assert state.retry_count == 0
        state.retry_count = 1
        assert state.retry_count == 1

    def test_max_retry_exceeded_forces_human_review(self):
        """超过 MAX_RETRY(2) 后，derive 强制 HUMAN_REVIEW_REQUIRED。"""
        state = SparkPipelineState(contract_hash="test_hash")
        state.retry_count = 2  # 达到上限
        for stage in SparkPipelineStage:
            state.record_stage_result(stage, "SUCCESS")
        state.derive_overall_status()
        # retry_count >= 2 → 即使全部 SUCCESS，也标记 HUMAN_REVIEW
        assert state.overall_status == SparkPipelineStatus.HUMAN_REVIEW_REQUIRED


# ════════════════════════════════════════════
# SparkOrchestrator 编排测试（mock 阶段注入）
# ════════════════════════════════════════════


class TestSparkOrchestrator:
    """SparkOrchestrator 编排——各阶段调用顺序 + 重试 + 错误处理。"""

    def test_orchestrator_creation_with_defaults(self):
        """SparkOrchestrator 可用默认参数创建。"""
        orchestrator = SparkOrchestrator()
        assert orchestrator is not None
        assert orchestrator.MAX_RETRY == 2

    def test_run_returns_pipeline_state(self):
        """run() 返回 SparkPipelineState——含 contract_hash。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        assert isinstance(state, SparkPipelineState)
        assert state.contract_hash == "test_hash"

    def test_run_records_all_stage_results(self):
        """run() 记录全部 6 个阶段的执行结果；不传 sql_plan 时 COMPARATOR 仍为 SKIPPED。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        for stage in SparkPipelineStage:
            assert stage.value in state.stage_results
        # 不传 sql_plan → COMPARATOR 标记 SKIPPED（非 NOT_EXECUTED，因为阶段已执行）
        assert state.stage_results["COMPARATOR"] == "SKIPPED", (
            f"无 sql_plan 时 COMPARATOR 应为 SKIPPED，"
            f"实际 {state.stage_results['COMPARATOR']}，errors={state.errors}"
        )

    def test_run_default_flow_success(self):
        """默认 run（无 contract、无 stage_failures）→ 所有阶段 SKIPPED，
        全局状态 LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED（无失败，但无可执行内容）。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        assert state.overall_status == SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED

    def test_run_with_stage_failure_triggers_repair(self):
        """阶段失败 → 触发 RepairPlanner 分类 → REPAIR_NEEDED。"""
        orchestrator = SparkOrchestrator()
        # 注入一个会在 MAPPER 阶段失败的 callback
        failures: dict[str, str] = {"MAPPER": "映射失败——Contract 不含任何输入表"}
        state = orchestrator.run(
            contract_hash="test_hash",
            stage_failures=failures,
        )
        assert state.stage_results["MAPPER"] == "FAILURE"
        assert state.overall_status == SparkPipelineStatus.REPAIR_NEEDED

    def test_run_retry_count_increments_on_retry(self):
        """返工时 retry_count 递增。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract_hash="test_hash",
            retry_count=1,
        )
        assert state.retry_count == 1

    def test_run_max_retry_forces_human_review(self):
        """retry_count >= MAX_RETRY → 强制 HUMAN_REVIEW_REQUIRED。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract_hash="test_hash",
            retry_count=2,
        )
        assert state.overall_status == SparkPipelineStatus.HUMAN_REVIEW_REQUIRED

    def test_run_exceeding_max_retry_forces_human_review(self):
        """retry_count > MAX_RETRY → 强制 HUMAN_REVIEW_REQUIRED。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract_hash="test_hash",
            retry_count=3,
        )
        assert state.overall_status == SparkPipelineStatus.HUMAN_REVIEW_REQUIRED

    def test_run_preserves_error_messages(self):
        """阶段失败时保留错误信息。"""
        orchestrator = SparkOrchestrator()
        failures = {"COMPILER": "编译错误——不支持的 step 类型"}
        state = orchestrator.run(
            contract_hash="test_hash",
            stage_failures=failures,
        )
        assert len(state.errors) > 0
        assert any("COMPILER" in e for e in state.errors)

    def test_run_without_llm_developer_skips_annotations(self):
        """未注入 SparkDeveloperService 时，DEVELOPER 阶段标记 SKIPPED。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        # 没有 developer → DEVELOPER 阶段跳过
        assert state.stage_results["DEVELOPER"] == "SKIPPED"

    # ── COMPARATOR 集成测试 ──

    def test_comparator_with_sql_plan_compares_instead_of_skip(self):
        """提供 sql_plan + spark_plan → COMPARATOR 真实对比，不再 SKIPPED。"""
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import ScanStep, SqlBuildPlan
        from tianshu_datadev.spark.models import SparkPlan, SparkReadStep

        orchestrator = SparkOrchestrator()
        state = SparkPipelineState(contract_hash="test_hash")

        # 构造最小 SqlBuildPlan
        sql_plan = SqlBuildPlan(
            plan_id="test_sql_plan",
            spec_hash="test_spec",
            steps=[
                ScanStep(
                    step_type="scan",
                    step_id="scan_t",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
        )
        # 构造最小 SparkPlan（单 read step）
        spark_plan = SparkPlan(
            plan_id="test_spark_plan",
            version="v1",
            source_phase="phase-3",
            source_contract_hash="test_hash",
            steps=[SparkReadStep(alias="t", source_name="tbl", input_key="tbl_key")],
        )

        # 注入缓存
        orchestrator._cached_sql_plan = sql_plan
        orchestrator._cached_plan = spark_plan

        # 执行 COMPARATOR
        orchestrator._run_comparator(SparkPipelineStage.COMPARATOR, state)

        # 验证：不再 SKIPPED
        assert state.stage_results["COMPARATOR"] == "SUCCESS", (
            f"预期 SUCCESS，实际 {state.stage_results['COMPARATOR']}，"
            f"errors={state.errors}"
        )
        assert state.comparator_report is not None

    def test_comparator_without_sql_plan_still_skips(self):
        """不设 sql_plan → COMPARATOR 保持 SKIPPED，错误消息包含 SqlBuildPlan。"""
        from tianshu_datadev.spark.models import SparkPlan, SparkReadStep

        orchestrator = SparkOrchestrator()
        state = SparkPipelineState(contract_hash="test_hash")

        # 仅设 spark_plan，不设 sql_plan
        spark_plan = SparkPlan(
            plan_id="test_spark_plan",
            version="v1",
            source_phase="phase-3",
            source_contract_hash="test_hash",
            steps=[SparkReadStep(alias="t", source_name="tbl", input_key="tbl_key")],
        )
        orchestrator._cached_plan = spark_plan
        # _cached_sql_plan 保持 None

        orchestrator._run_comparator(SparkPipelineStage.COMPARATOR, state)

        assert state.stage_results["COMPARATOR"] == "SKIPPED"
        assert any("SqlBuildPlan" in e for e in state.errors), (
            f"错误消息应提及 SqlBuildPlan，实际 errors={state.errors}"
        )

    def test_comparator_without_spark_plan_still_skips(self):
        """设 sql_plan 但不设 spark_plan → COMPARATOR SKIPPED，错误消息包含 SparkPlan。"""
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import ScanStep, SqlBuildPlan

        orchestrator = SparkOrchestrator()
        state = SparkPipelineState(contract_hash="test_hash")

        # 仅设 sql_plan，不设 spark_plan
        sql_plan = SqlBuildPlan(
            plan_id="test_sql_plan",
            spec_hash="test_spec",
            steps=[
                ScanStep(
                    step_type="scan",
                    step_id="scan_t",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
        )
        orchestrator._cached_sql_plan = sql_plan
        # _cached_plan 保持 None

        orchestrator._run_comparator(SparkPipelineStage.COMPARATOR, state)

        assert state.stage_results["COMPARATOR"] == "SKIPPED"
        assert any("SparkPlan" in e for e in state.errors), (
            f"错误消息应提及 SparkPlan，实际 errors={state.errors}"
        )

    def test_consecutive_runs_do_not_leak_cached_plan(self):
        """A 类修复：同一 Orchestrator 连续两次 run()，
        第二次无 contract 但有 sql_plan → COMPARATOR 不得复用第一次的 spark_plan。

        每次 run() 都是独立执行上下文——_cached_plan / _cached_compile_result
        必须在 run() 开头重置为 None，防止上一轮残留泄漏。
        """
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            ContractOutputColumn,
            DataTransformContractV1,
        )
        from tianshu_datadev.planning.sql_build_plan import ScanStep, SqlBuildPlan

        orchestrator = SparkOrchestrator()

        # 构造最小 SqlBuildPlan
        sql_plan = SqlBuildPlan(
            plan_id="sql_leak_test",
            spec_hash="leak_spec",
            steps=[ScanStep(step_type="scan", step_id="scan_t", table_ref="t", required_columns=[])],
        )

        # 构造最小 Contract（驱动 Mapper 产出 SparkPlan）
        contract = DataTransformContractV1(
            contract_id="leak_test_contract",
            source_sqlprogram_hash="leak_test",
            input_tables=[ContractInputTable(table_ref="t", source_table="src.t")],
            output_columns=[ContractOutputColumn(column_name="id", alias="id")],
        )

        # ── 第一次 run：contract + sql_plan → MAPPER SUCCESS + COMPARATOR SUCCESS ──
        state1 = orchestrator.run(contract=contract, sql_plan=sql_plan)
        assert state1.stage_results["MAPPER"] == "SUCCESS", (
            f"第一次 MAPPER 应为 SUCCESS，实际 {state1.stage_results['MAPPER']}"
        )
        assert state1.stage_results["COMPARATOR"] == "SUCCESS", (
            f"第一次 COMPARATOR 应为 SUCCESS，实际 {state1.stage_results['COMPARATOR']}，"
            f"errors={state1.errors}"
        )

        # ── 第二次 run：只有 sql_plan，无 contract ──
        # 正确行为：_cached_plan 已在 run() 开头重置为 None
        # → MAPPER SKIPPED（无 contract）→ COMPARATOR SKIPPED（缺 SparkPlan）
        state2 = orchestrator.run(
            contract_hash="second_run_no_contract",
            sql_plan=sql_plan,
        )
        assert state2.stage_results["MAPPER"] == "SKIPPED", (
            f"第二次 MAPPER 应为 SKIPPED（无 contract），实际 {state2.stage_results['MAPPER']}"
        )
        # 关键断言：不得复用第一次的 SparkPlan
        assert state2.stage_results["COMPARATOR"] == "SKIPPED", (
            f"第二次 COMPARATOR 应为 SKIPPED（无 spark_plan），"
            f"实际 {state2.stage_results['COMPARATOR']}——"
            f"疑似复用了第一次 run 的残留 _cached_plan"
        )
        assert any("SparkPlan" in e for e in state2.errors), (
            f"错误消息应指出缺少 SparkPlan，实际 errors={state2.errors}"
        )


# ════════════════════════════════════════════
# 骨架级 E2E 测试——Orchestrator 真实调用组件
# ════════════════════════════════════════════
#
# R3（Mapper input_alias 空值）已于 2026-07-04 修复——_chain_input_aliases()
# 在 mapper 组装步骤后自动填充线性依赖链。真实 Contract E2E 测试见
# test_real_contract_e2e_mapper_compiler_validator。


def _make_e2e_spark_plan():
    """构造骨架级 E2E 测试用的 SparkPlan——scan + filter + project + sort。"""
    from tianshu_datadev.spark.models import (
        SparkFilterStep,
        SparkPlan,
        SparkProjectColumn,
        SparkProjectStep,
        SparkReadStep,
        SparkSortDirection,
        SparkSortSpec,
        SparkSortStep,
    )

    return SparkPlan(
        plan_id="acceptance_e2e_plan",
        version="v1",
        source_phase="phase-5",
        source_contract_hash="acceptance_test_001",
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
                input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="order_id", alias="order_id"),
                    SparkProjectColumn(column_name="amount", alias="amount"),
                ],
            ),
            SparkSortStep(
                input_alias="od",
                order_by=[
                    SparkSortSpec(column="amount", direction=SparkSortDirection.DESC),
                ],
            ),
        ],
    )


class TestOrchestratorSkeletonE2E:
    """骨架级端到端——Orchestrator 真实调用 mapper → compiler → validator。

    R3（Mapper input_alias 空值）已修复——真实 Contract 可经 mapper 全链路流转。
    Comparator 和 PhysicalVerifier 因依赖外部条件标记 SKIPPED。
    """

    def test_skeleton_e2e_compiler_validator_review_package(self):
        """最小真实链路：SparkPlan → Compiler → Validator → ReviewPackage。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import SparkPlan
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder
        from tianshu_datadev.spark.validator import SparkStaticValidator

        plan = _make_e2e_spark_plan()
        plan_hash = SparkPlan.compute_plan_hash(plan)

        # Step 1：编译
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        assert result.raw_pyspark is not None
        assert len(result.raw_pyspark) > 0
        assert result.raw_hash is not None

        # Step 2：安全校验
        validator = SparkStaticValidator()
        validation = validator.validate(result.raw_pyspark)
        assert validation.is_valid, f"Validator 拒绝合法代码：{validation.errors}"

        # Step 3：构建 PipelineState（模拟 Orchestrator 状态填充）
        from tianshu_datadev.spark.orchestrator import SparkPipelineState

        state = SparkPipelineState(contract_hash="acceptance_test_001")
        state.spark_plan_hash = plan_hash
        state.compiled_code_sha256 = result.raw_hash
        state.record_stage_result(SparkPipelineStage.MAPPER, "SUCCESS")
        state.record_stage_result(SparkPipelineStage.COMPILER, "SUCCESS")
        state.record_stage_result(SparkPipelineStage.VALIDATOR, "SUCCESS")
        state.record_stage_result(SparkPipelineStage.DEVELOPER, "SKIPPED")
        state.record_stage_result(SparkPipelineStage.COMPARATOR, "SKIPPED")
        state.record_stage_result(SparkPipelineStage.PHYSICAL_VERIFIER, "SKIPPED")
        state.derive_overall_status()

        assert state.overall_status == SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED

        # Step 4：构建交付物
        builder = SparkReviewBuilder()
        pkg = builder.build(state)
        assert pkg.package_id.startswith("pkg_")
        assert pkg.provenance.spark_plan_hash == plan_hash
        assert pkg.provenance.compiled_code_sha256 == result.raw_hash
        assert pkg.overall_status == "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"

    def test_skeleton_e2e_hash_determinism(self):
        """同一 SparkPlan 两次编译产出相同 hash——证明确定性。"""
        from tianshu_datadev.spark.compiler import SparkCompiler

        plan = _make_e2e_spark_plan()
        result1 = SparkCompiler().compile(plan)
        result2 = SparkCompiler().compile(plan)
        assert result1.raw_hash == result2.raw_hash

    def test_backward_compat_stage_failures_still_works(self):
        """stage_failures 注入模式仍正常工作——向后兼容。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract_hash="test_hash",
            stage_failures={"COMPILER": "测试注入——编译失败"},
        )
        assert state.stage_results["COMPILER"] == "FAILURE"
        assert state.overall_status == SparkPipelineStatus.REPAIR_NEEDED
        # 无 contract → MAPPER 标记 SKIPPED（无法映射）
        assert state.stage_results["MAPPER"] == "SKIPPED"

    def test_run_without_contract_skips_mapper(self):
        """无 contract 时 MAPPER 标记 SKIPPED——不崩溃。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="no_contract")
        assert state.stage_results["MAPPER"] == "SKIPPED"
        # 后续依赖 Mapper 的阶段也 SKIPPED
        assert state.stage_results["COMPILER"] == "SKIPPED"
        assert state.stage_results["VALIDATOR"] == "SKIPPED"

    def test_stage_failures_takes_priority_over_real_contract(self):
        """stage_failures 注入优先于真实 contract 执行——测试注入模式隔离。"""
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            ContractOutputColumn,
            ContractSort,
            DataTransformContractV1,
        )

        contract_id = DataTransformContractV1.generate_contract_id("acceptance_test_002")
        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash="acceptance_test_002",
            input_tables=[
                ContractInputTable(
                    table_ref="od",
                    source_table="dwd.order_detail",
                    estimated_row_count=1000,
                ),
            ],
            output_columns=[
                ContractOutputColumn(
                    column_name="order_id", alias="order_id", data_type="string",
                ),
            ],
            sort_spec=[ContractSort(column="order_id", direction="ASC")],
        )

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract,
            stage_failures={"MAPPER": "注入失败"},
        )
        # 注入优先——MAPPER 应为 FAILURE 而非 SUCCESS
        assert state.stage_results["MAPPER"] == "FAILURE"
        assert any("注入失败" in e for e in state.errors)

    def test_real_contract_e2e_mapper_compiler_validator(self):
        """R3 收口验证——真实 DataTransformContractV1 经 orchestrator.run(contract=contract)
        完成 MAPPER → COMPILER → VALIDATOR 全链路。

        通过标准：
        - MAPPER / COMPILER / VALIDATOR SUCCESS
        - DEVELOPER / COMPARATOR / PHYSICAL_VERIFIER SKIPPED
        - overall_status = LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
        - spark_plan_hash / compiled_code_sha256 非空（hash 链完整）
        - ReviewPackage 可生成
        """
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            ContractOutputColumn,
            ContractPredicate,
            ContractSort,
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder

        # 构造最小真实 Contract：单表 → 过滤 → 投影 → 排序
        contract_id = DataTransformContractV1.generate_contract_id("acceptance_r3_e2e")
        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash="acceptance_r3_e2e",
            input_tables=[
                ContractInputTable(
                    table_ref="od",
                    source_table="dwd.order_detail",
                    estimated_row_count=1000,
                ),
            ],
            output_columns=[
                ContractOutputColumn(
                    column_name="order_id", alias="order_id", data_type="string",
                ),
                ContractOutputColumn(
                    column_name="amount", alias="amount", data_type="decimal(18,2)",
                ),
            ],
            filters=[
                ContractPredicate(
                    operator="GT", left="od.amount", right="100",
                ),
            ],
            sort_spec=[ContractSort(column="amount", direction="DESC")],
        )

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract=contract)

        # ── 阶段结果验证 ──
        assert state.stage_results["MAPPER"] == "SUCCESS", (
            f"MAPPER 应为 SUCCESS，实际 {state.stage_results['MAPPER']}，"
            f"错误：{state.errors}"
        )
        assert state.stage_results["COMPILER"] == "SUCCESS", (
            f"COMPILER 应为 SUCCESS，实际 {state.stage_results['COMPILER']}，"
            f"错误：{state.errors}"
        )
        assert state.stage_results["VALIDATOR"] == "SUCCESS", (
            f"VALIDATOR 应为 SUCCESS，实际 {state.stage_results['VALIDATOR']}，"
            f"错误：{state.errors}"
        )
        assert state.stage_results["DEVELOPER"] == "SKIPPED"
        assert state.stage_results["COMPARATOR"] == "SKIPPED"
        assert state.stage_results["PHYSICAL_VERIFIER"] == "SKIPPED"

        # ── 全局状态验证 ──
        assert state.overall_status == SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED, (
            f"全局状态应为 LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED，"
            f"实际 {state.overall_status}"
        )

        # ── Hash 链完整性验证 ──
        assert state.spark_plan_hash != "", "spark_plan_hash 不应为空"
        assert state.compiled_code_sha256 != "", "compiled_code_sha256 不应为空"
        assert state.contract_hash != "", "contract_hash 不应为空"

        # ── ReviewPackage 可生成 ──
        builder = SparkReviewBuilder()
        pkg = builder.build(state)
        assert pkg.package_id.startswith("pkg_")
        assert pkg.provenance.spark_plan_hash == state.spark_plan_hash
        assert pkg.provenance.compiled_code_sha256 == state.compiled_code_sha256
        assert pkg.overall_status == "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"

    # ── Phase 9A2: 真实 SQL Pipeline SqlBuildPlan 驱动 COMPARATOR ──

    def test_comparator_with_real_sql_pipeline_plan(self):
        """9A3 集成测试——真实 SQL Pipeline 全链路驱动 COMPARATOR。

        使用 Pipeline.run_all() → export_artifacts() 获取真实 SqlBuildPlan 和
        DataTransformContractLite，经 adapt_lite_to_v1() 适配为 V1 后传入 Orchestrator，
        验证 MAPPER → COMPARATOR 全阶段 SUCCESS。

        与 9A2 版本的关键区别：不再手工构造 DataTransformContractV1 绕过
        export_artifacts() 导出的 contract——而是使用确定性适配层升级 Lite → V1。
        """
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        import os
        import tempfile

        from tianshu_datadev.api.pipeline import Pipeline
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1

        # ── 1. 使用真实 SQL Pipeline 获取 SqlBuildPlan ──
        root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        spec_path = os.path.join(root, "tests", "fixtures", "golden", "golden_passing.md")
        csv_path = os.path.abspath(
            os.path.join(root, "tests", "fixtures", "sql", "test_fact.csv")
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            markdown_text = f.read()

        tmpdir = tempfile.mkdtemp()
        try:
            pipeline = Pipeline(base_output_dir=tmpdir, adapter=None)
            result = pipeline.run_all(
                markdown_text,
                table_mapping={"tf": "test_fact"},
                table_paths={"test_fact": csv_path},
            )
            request_id = result["request_id"]

            bundle = pipeline.export_artifacts(request_id)
            assert bundle is not None, "export_artifacts 不应返回 None"
            assert bundle.sql_build_plan is not None, "真实 SqlBuildPlan 不应为 None"

            # ── 2. 适配 contract（Lite → V1）——不再手工构造 V1 ──
            assert bundle.data_transform_contract is not None, (
                "export_artifacts() 应包含 data_transform_contract"
            )
            v1_contract = adapt_lite_to_v1(bundle.data_transform_contract)

            # ── 3. Orchestrator 使用真实 SqlBuildPlan + 适配后的 V1 contract ──
            orchestrator = SparkOrchestrator()
            state = orchestrator.run(
                contract=v1_contract,
                sql_plan=bundle.sql_build_plan,
            )

            # ── 4. 验证 COMPARATOR 阶段成功 ──
            assert state.stage_results["MAPPER"] == "SUCCESS", (
                f"MAPPER 应为 SUCCESS，实际 {state.stage_results['MAPPER']}，"
                f"errors={state.errors}"
            )
            assert state.stage_results["COMPARATOR"] == "SUCCESS", (
                f"COMPARATOR 应为 SUCCESS（真实 SqlBuildPlan 驱动），"
                f"实际 {state.stage_results['COMPARATOR']}，"
                f"errors={state.errors}"
            )
            assert state.comparator_report is not None, (
                "comparator_report 不应为 None"
            )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════
# Phase 10 Case06——Orchestrator SqlProgram 集成测试
# ════════════════════════════════════════════


class TestOrchestratorSqlProgramIntegration:
    """Orchestrator 接受 SqlProgram 作为 Comparator 输入——多语句 DAG 对比。"""

    def test_comparator_with_sql_program_uses_compare_program(self):
        """注入 SqlProgram → _run_comparator 分派到 compare_program() → SUCCESS。"""
        from tianshu_datadev.planning.models import (
            AliasExpr,
            ColumnRef,
            Predicate,
            PredicateOperator,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            FilterStep,
            ProjectStep,
            ScanStep,
            SqlBuildPlan,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )
        from tianshu_datadev.spark.models import (
            SparkFilterStep,
            SparkPlan,
            SparkProjectColumn,
            SparkProjectStep,
            SparkReadStep,
        )

        orchestrator = SparkOrchestrator()
        state = SparkPipelineState(contract_hash="test_sqlprogram_hash")

        # 构造最小 SqlProgram（单语句——STANDALONE，scan + filter + project）
        sql_plan = SqlBuildPlan(
            plan_id="test_prog_plan",
            spec_hash="test_prog_spec",
            steps=[
                ScanStep(
                    step_type="scan", step_id="scan_t",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                    ],
                ),
                FilterStep(
                    step_type="filter", step_id="filter_001",
                    predicate=Predicate(
                        left=ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                        operator=PredicateOperator.GT,
                        right=ColumnRef(table_ref="t", column_name="threshold", normalized_name="threshold"),
                    ),
                ),
                ProjectStep(
                    step_type="project", step_id="proj_001",
                    columns=[
                        AliasExpr(
                            expression=ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                            alias="id",
                        ),
                    ],
                ),
            ],
        )
        sql_program = SqlProgram(
            program_id=SqlProgram.generate_program_id("test_prog_spec"),
            spec_id="test_prog_spec",
            statements=[
                SqlStatement(
                    statement_id="stmt_0",
                    plan=sql_plan,
                    kind=StatementKind.STANDALONE,
                ),
            ],
            topological_order=["stmt_0"],
        )

        # 构造等价 SparkPlan
        spark_plan = SparkPlan(
            plan_id="test_spark_prog_plan",
            version="v1",
            source_phase="phase-3",
            source_contract_hash="test_prog_spec",
            steps=[
                SparkReadStep(alias="t", source_name="tbl", input_key="tbl_key"),
                SparkFilterStep(input_alias="t", operator="GT", left="id", right="threshold"),
                SparkProjectStep(
                    input_alias="t",
                    columns=[SparkProjectColumn(column_name="id", alias="id")],
                ),
            ],
        )

        # 注入缓存——SqlProgram 而非 SqlBuildPlan
        orchestrator._cached_sql_plan = sql_program
        orchestrator._cached_plan = spark_plan

        # 执行 COMPARATOR
        orchestrator._run_comparator(SparkPipelineStage.COMPARATOR, state)

        # 验证：不再 SKIPPED
        assert state.stage_results["COMPARATOR"] == "SUCCESS", (
            f"预期 SUCCESS，实际 {state.stage_results['COMPARATOR']}，"
            f"errors={state.errors}"
        )
        assert state.comparator_report is not None, (
            "SqlProgram 路径应产出 comparator_report"
        )
        # 逻辑等价——单语句语义与 SparkPlan 一致
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"预期 LOGIC_EQUIVALENT，实际 {state.comparator_report.status}"
        )

    def test_comparator_with_sql_build_plan_still_works(self):
        """SqlBuildPlan 路径向后兼容——注入 SqlBuildPlan 而非 SqlProgram → 走 compare()。"""
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import ScanStep, SqlBuildPlan
        from tianshu_datadev.spark.models import SparkPlan, SparkReadStep

        orchestrator = SparkOrchestrator()
        state = SparkPipelineState(contract_hash="test_backward_hash")

        # 构造 SqlBuildPlan（非 SqlProgram）——与已有测试一致
        sql_plan = SqlBuildPlan(
            plan_id="test_sql_plan_bw",
            spec_hash="test_spec_bw",
            steps=[
                ScanStep(
                    step_type="scan",
                    step_id="scan_t",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
        )
        spark_plan = SparkPlan(
            plan_id="test_spark_plan_bw",
            version="v1",
            source_phase="phase-3",
            source_contract_hash="test_spec_bw",
            steps=[SparkReadStep(alias="t", source_name="tbl", input_key="tbl_key")],
        )

        orchestrator._cached_sql_plan = sql_plan
        orchestrator._cached_plan = spark_plan

        orchestrator._run_comparator(SparkPipelineStage.COMPARATOR, state)

        assert state.stage_results["COMPARATOR"] == "SUCCESS"
        assert state.comparator_report is not None
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT


# ════════════════════════════════════════════
# Phase 8: Pipeline._do_spark_develop 单元测试
# ════════════════════════════════════════════


class TestPipelineSparkDevelop:
    """Pipeline._do_spark_develop 三态覆盖——SUCCESS / FAILURE / SKIPPED。"""

    def test_do_spark_develop_with_service_success(self):
        """注入 mock service 时 DEVELOPER 阶段返回 SUCCESS 且 annotation_result 非空。"""
        from unittest.mock import MagicMock

        from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext
        from tianshu_datadev.spark.annotations import AnnotatedSparkPlan, StepAnnotation, StepIntent
        from tianshu_datadev.spark.developer import SparkDeveloperService

        # 准备
        mock_ann = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[
                StepAnnotation(step_id="SparkReadStep_0", step_index=0, step_type="read",
                               intent=StepIntent.SOURCE, intent_detail="读取数据"),
            ],
        )
        mock_service = MagicMock(spec=SparkDeveloperService)
        mock_service.annotate.return_value = mock_ann

        pipeline = Pipeline(developer_service=mock_service)
        ctx = SparkStageContext()
        ctx.spark_plan = MagicMock()

        # 执行
        pipeline._do_spark_develop(ctx)

        # 验证
        assert ctx.stage_results["DEVELOPER"] == "SUCCESS"
        assert ctx.annotation_result is not None
        assert ctx.annotation_result.annotations[0].step_id == "SparkReadStep_0"
        mock_service.annotate.assert_called_once_with(ctx.spark_plan)

    def test_do_spark_develop_service_failure(self):
        """service 抛异常时 DEVELOPER 标记 FAILURE。"""
        from unittest.mock import MagicMock

        from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext
        from tianshu_datadev.spark.developer import SparkDeveloperService

        mock_service = MagicMock(spec=SparkDeveloperService)
        mock_service.annotate.side_effect = ValueError("API 调用失败")

        pipeline = Pipeline(developer_service=mock_service)
        ctx = SparkStageContext()
        ctx.spark_plan = MagicMock()

        pipeline._do_spark_develop(ctx)

        assert ctx.stage_results["DEVELOPER"] == "FAILURE"
        assert any("[DEVELOPER] 标注异常" in e for e in ctx.errors)

    def test_do_spark_develop_no_service_skips(self):
        """service=None 时 DEVELOPER 标记 SKIPPED。"""
        from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext

        pipeline = Pipeline()  # developer_service=None
        ctx = SparkStageContext()

        pipeline._do_spark_develop(ctx)

        assert ctx.stage_results["DEVELOPER"] == "SKIPPED"
        assert any("未注入" in e for e in ctx.errors)
