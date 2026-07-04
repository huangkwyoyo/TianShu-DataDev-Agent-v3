"""Phase 8 SparkOrchestrator 测试——Pipeline 状态机 + 全链路编排。

覆盖：
- SparkPipelineStage 6 阶段枚举
- SparkPipelineState 状态模型 + 全局状态流转
- SparkOrchestrator 编排逻辑（各阶段调用顺序 + 重试上限）
- 阶段失败 → RepairPlanner 分类 → 重试/人工审查
"""

from __future__ import annotations

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
        """run() 记录全部 6 个阶段的执行结果。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        for stage in SparkPipelineStage:
            assert stage.value in state.stage_results

    def test_run_default_flow_success(self):
        """默认 run（无实际组件注入）→ 所有阶段标记 SUCCESS。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="test_hash")
        assert state.overall_status == SparkPipelineStatus.ALL_CONSISTENT

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
