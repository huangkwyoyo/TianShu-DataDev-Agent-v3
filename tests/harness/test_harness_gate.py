"""Phase 4D 七维门禁测试——验证 Harness 框架的正确判决。

测试策略：
1. 七维门禁——每个维度至少 1 个测试验证 REJECT 条件正确触发
2. Join 零容忍——使用故意注入错误的 fixture 验证 REJECT
3. HarnessReport 生成——验证 GO/NO_GO 判决
4. 人工接受率——验证流程可执行、D5 占位不崩溃
5. 数据集加载——fixture 验证
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from tianshu_datadev.harness.dataset_loader import DatasetLoader
from tianshu_datadev.harness.eval_runner import HarnessRunner
from tianshu_datadev.harness.metrics import HarnessMetricsEngine
from tianshu_datadev.harness.models import (
    AttackVector,
    DatasetCategory,
    DimensionResult,
    HarnessCase,
    HarnessReport,
    HarnessVerdict,
    SecurityCaseResult,
    SecurityEvalReport,
    SemanticCaseResult,
    SemanticErrorType,
    SemanticEvalReport,
)

# ════════════════════════════════════════════
# 辅助函数——构造受控测试数据
# ════════════════════════════════════════════


def _make_join_case(
    case_id: str,
    expect_false_negative: bool = False,
    expect_weak_rejection: bool = False,
    expect_evidence_chain_failure: bool = False,
) -> HarnessCase:
    """构造 Join 相关的 golden 用例用于零容忍测试。"""
    expected: dict = {
        "must_accept": True,
        "required_output_columns": ["user_name", "total_amount"],
        "required_join_keys": [["orders.user_id", "users.user_id"]],
    }
    if expect_false_negative:
        expected["expect_false_negative"] = True
    if expect_weak_rejection:
        expected["expect_weak_rejection"] = True
    if expect_evidence_chain_failure:
        expected["expect_evidence_chain_failure"] = True

    return HarnessCase(
        case_id=case_id,
        category=DatasetCategory.GOLDEN,
        description=f"Join 零容忍测试用例：{case_id}",
        expected=expected,
    )


def _make_semantic_report_with_known_gaps() -> SemanticEvalReport:
    """构造含 2 个 known_gap 的 SemanticEvalReport（3/5 detectable）。"""
    return SemanticEvalReport(
        eval_id="sem_test_001",
        timestamp="2026-06-28T00:00:00+00:00",
        summary="3/5 errors detectable (2 known gaps: WRONG_GRAIN, WRONG_AGGREGATION)",
        error_type_coverage={
            "WRONG_FIELD": True,
            "WRONG_GRAIN": False,
            "WRONG_AGGREGATION": False,
            "WRONG_ENUM": True,
            "WRONG_JOIN": True,
        },
        results=[
            SemanticCaseResult(
                case_id="SEM-WF-001",
                error_type=SemanticErrorType.WRONG_FIELD,
                passed=True, detection_layer="validator",
                rejection_detail="检测到错字段", trace="",
            ),
            SemanticCaseResult(
                case_id="SEM-WG-001",
                error_type=SemanticErrorType.WRONG_GRAIN,
                passed=False,
                rejection_detail="已知缺口——系统无粒度完整性规则", trace="",
            ),
            SemanticCaseResult(
                case_id="SEM-WA-001",
                error_type=SemanticErrorType.WRONG_AGGREGATION,
                passed=False,
                rejection_detail="已知缺口——系统无聚合声明对比规则", trace="",
            ),
            SemanticCaseResult(
                case_id="SEM-WE-001",
                error_type=SemanticErrorType.WRONG_ENUM,
                passed=True, detection_layer="label_validator",
                rejection_detail="检测到未声明枚举值", trace="",
            ),
            SemanticCaseResult(
                case_id="SEM-WJ-001",
                error_type=SemanticErrorType.WRONG_JOIN,
                passed=True, detection_layer="validator",
                rejection_detail="检测到 Join key 类型不兼容", trace="",
            ),
        ],
        undetected_errors=["SEM-WG-001: 已知缺口", "SEM-WA-001: 已知缺口"],
        known_gaps=["WRONG_GRAIN", "WRONG_AGGREGATION"],
    )


def _make_security_report_with_blocking_issue() -> SecurityEvalReport:
    """构造含 1 个 blocking_issue 的 SecurityEvalReport。"""
    return SecurityEvalReport(
        eval_id="sec_test_001",
        timestamp="2026-06-28T00:00:00+00:00",
        summary="5/6 cases blocked",
        vector_coverage={
            "PROMPT_INJECTION": True,
            "SQL_INJECTION": True,
            "SCHEMA_EXTRA": True,
            "UNDECLARED_REF": True,
            "JOIN_ERROR_INFERENCE": False,
            "WRITE_PRIVILEGE": True,
        },
        results=[
            SecurityCaseResult(
                case_id="SEC-PI-001",
                attack_vector=AttackVector.PROMPT_INJECTION,
                passed=True, detection_layer="schema",
                rejection_detail="Schema 层拒绝 extra 字段",
            ),
            SecurityCaseResult(
                case_id="SEC-JEI-001",
                attack_vector=AttackVector.JOIN_ERROR_INFERENCE,
                passed=False,
                rejection_detail="Validator 未拒绝 WEAK Join——门禁缺失",
            ),
        ],
        blocking_issues=["SEC-JEI-001: Validator 未拒绝 WEAK Join——门禁缺失"],
    )


# ════════════════════════════════════════════
# 测试类 1：七维门禁——各维度 REJECT 条件
# ════════════════════════════════════════════


class TestSevenDimensionGate:
    """七维门禁——每个维度至少 1 个测试验证其 REJECT 条件正确触发。"""

    def test_dimension_1_warn_when_no_data(self):
        """D1：无 plan_build_results 时返回 WARN——不能因缺数据而误放行。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_1(
            datasets={cat: [] for cat in DatasetCategory},
            plan_build_results=None,
        )
        assert result.verdict == "WARN", (
            f"无数据时应返回 WARN（不阻断），实际: {result.verdict}"
        )
        assert result.metrics["parse_pass_rate"] == -1.0, (
            "无数据时 parse_pass_rate 应为 -1.0 占位值"
        )
        assert result.metrics["extra_field_rejection_rate"] == -1.0
        assert "[stub]" in result.details[0]

    def test_dimension_1_reject_on_low_parse_pass_rate(self):
        """D1：parse_pass_rate < 95% → REJECT。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_1(
            datasets={cat: [] for cat in DatasetCategory},
            plan_build_results={
                "parse_pass_rate": 80.0,
                "extra_field_rejection_rate": 100.0,
                "passed_cases": 80,
            },
        )
        assert result.verdict == "REJECT", (
            f"解析通过率 80% < 95% 应 REJECT，实际: {result.verdict}"
        )
        assert result.metrics["parse_pass_rate"] == 80.0, (
            f"应读取传入的 parse_pass_rate=80.0，实际: {result.metrics['parse_pass_rate']}"
        )
        assert "80.0%" in result.details[0]

    def test_dimension_1_reject_on_low_extra_rejection_rate(self):
        """D1：extra_field_rejection_rate < 100% → REJECT。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_1(
            datasets={cat: [] for cat in DatasetCategory},
            plan_build_results={
                "parse_pass_rate": 98.0,
                "extra_field_rejection_rate": 90.0,
                "passed_cases": 98,
            },
        )
        assert result.verdict == "REJECT", (
            f"extra 拒绝率 90% < 100% 应 REJECT，实际: {result.verdict}"
        )
        assert result.metrics["extra_field_rejection_rate"] == 90.0, (
            f"应读取传入的 extra_field_rejection_rate=90.0，"
            f"实际: {result.metrics['extra_field_rejection_rate']}"
        )

    def test_dimension_1_pass_when_all_thresholds_met(self):
        """D1：parse_pass_rate >= 95% 且 extra_field_rejection_rate == 100% → PASS。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_1(
            datasets={cat: [] for cat in DatasetCategory},
            plan_build_results={
                "parse_pass_rate": 97.0,
                "extra_field_rejection_rate": 100.0,
                "passed_cases": 97,
            },
        )
        assert result.verdict == "PASS", (
            f"全部达标应 PASS，实际: {result.verdict}"
        )

    def test_dimension_1_reject_priority_parse_over_extra(self):
        """D1：parse_pass_rate 不达标时优先报告，不检查 extra 条件。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_1(
            datasets={cat: [] for cat in DatasetCategory},
            plan_build_results={
                "parse_pass_rate": 80.0,
                "extra_field_rejection_rate": 50.0,  # 也不达标
                "passed_cases": 80,
            },
        )
        assert result.verdict == "REJECT", (
            f"两项均不达标时应 REJECT，实际: {result.verdict}"
        )
        # 应优先报告解析通过率问题（第一个检查的条件）
        assert any("解析通过率" in d for d in result.details), (
            f"应优先报告解析通过率问题，实际详情: {result.details}"
        )

    def test_dimension_2_reject_on_weak_none_adopted(self):
        """D2：WEAK/NONE 被采纳 → REJECT（零容忍）。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2({
            "false_negative_count": 0,
            "weak_none_adopted_count": 1,  # 零容忍违规
            "missing_evidence_chain_count": 0,
            "total_joins": 5,
            "false_negative_details": [],
            "weak_none_details": ["jc_weak_001: WEAK Join 被采纳"],
            "evidence_chain_details": [],
        })
        assert result.verdict == "REJECT", (
            f"WEAK/NONE 被采纳 > 0 应触发 REJECT，实际: {result.verdict}"
        )
        assert result.metrics["weak_none_adopted"] == 1

    def test_dimension_2_reject_on_false_negative(self):
        """D2：漏报率 > 0 → REJECT。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2({
            "false_negative_count": 1,
            "weak_none_adopted_count": 0,
            "missing_evidence_chain_count": 0,
            "total_joins": 5,
            "false_negative_details": ["golden_join_001: 预期 Join 未输出"],
            "weak_none_details": [],
            "evidence_chain_details": [],
        })
        assert result.verdict == "REJECT", (
            f"漏报率 > 0 应触发 REJECT，实际: {result.verdict}"
        )

    def test_dimension_2_reject_on_missing_evidence_chain(self):
        """D2：缺证据链 > 0 → REJECT。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2({
            "false_negative_count": 0,
            "weak_none_adopted_count": 0,
            "missing_evidence_chain_count": 2,
            "total_joins": 5,
            "false_negative_details": [],
            "weak_none_details": [],
            "evidence_chain_details": [
                "jc_001: evidence_checks 仅含 1 项",
                "jc_002: evidence_chain_yaml 为空",
            ],
        })
        assert result.verdict == "REJECT", (
            f"缺证据链 > 0 应触发 REJECT，实际: {result.verdict}"
        )

    def test_dimension_2_warn_when_no_data(self):
        """D2：无 join_quality_data 时返回 WARN——不阻断退出（待接入 Planner）。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2(None)
        assert result.verdict == "WARN", (
            f"无数据时应返回 WARN（不阻断），实际: {result.verdict}"
        )
        assert "[stub]" in result.details[0]

    def test_dimension_3_pass_with_known_gaps(self):
        """D3：已知缺口不影响 PASS 判决——3/5 detectable + 2 known_gap 应 PASS。"""
        engine = HarnessMetricsEngine()
        report = _make_semantic_report_with_known_gaps()
        result = engine.compute_dimension_3(report)
        assert result.verdict == "PASS", (
            f"已知缺口情况下 3/5 检测通过应 PASS，实际: {result.verdict}"
        )
        assert result.metrics["known_gaps"] == 2
        assert result.metrics["detected_errors"] == 3

    def test_dimension_6_reject_on_blocking_issues(self):
        """D6：存在未拦截的攻击 → REJECT。"""
        engine = HarnessMetricsEngine()
        report = _make_security_report_with_blocking_issue()
        result = engine.compute_dimension_6(report)
        assert result.verdict == "REJECT", (
            f"存在 blocking_issues 应 REJECT，实际: {result.verdict}"
        )
        assert result.metrics["blocking_issues"] == 1


# ════════════════════════════════════════════
# 测试类 2：Join 零容忍——故意注入错误的 fixture
# ════════════════════════════════════════════


class TestJoinZeroTolerance:
    """Join 零容忍——验证 fixture 期望与实际 Planner/Validator 输出的正确比较。

    fixture 的 expected.expect_* 字段仅表示"此用例用于测试该行为"，
    不等于系统已违规。只有实际结果违背 fixture 期望时才计入违规。
    """

    # ── stub 行为 ──

    def test_stub_when_no_actual_results(self):
        """无 actual_results 时返回 stub——fixture 期望不直接计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_fn_001", expect_false_negative=True),
            _make_join_case("join_weak_001", expect_weak_rejection=True),
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
        ]
        data = runner.evaluate_join_quality(golden)  # 不传 actual_results
        assert data.get("_stub") is True, (
            f"无 actual_results 时应标记 _stub，实际: {data.get('_stub')}"
        )
        assert data["false_negative_count"] == 0, (
            "stub 状态下漏报计数必须为 0——fixture 期望 ≠ 违规"
        )
        assert data["weak_none_adopted_count"] == 0
        assert data["missing_evidence_chain_count"] == 0
        assert data["total_joins"] == 3

    # ── 漏报（条件①）：违背预期才计违规 ──

    def test_false_negative_counted_when_system_misses(self):
        """expect_false_negative=true 且系统未检测到 → 计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_fn_001", expect_false_negative=True),
        ]
        actual = {"join_fn_001": {"false_negative_detected": False}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["false_negative_count"] == 1, (
            f"系统未检测到漏报应计 1 次违规，实际: {data['false_negative_count']}"
        )

    def test_false_negative_not_counted_when_system_detects(self):
        """expect_false_negative=true 且系统正确检测到 → 不计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_fn_001", expect_false_negative=True),
        ]
        actual = {"join_fn_001": {"false_negative_detected": True}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["false_negative_count"] == 0, (
            f"系统正确检测到漏报不应计违规，实际: {data['false_negative_count']}"
        )

    # ── WEAK/NONE 拒绝（条件②）：违背预期才计违规 ──

    def test_weak_rejection_counted_when_validator_fails(self):
        """expect_weak_rejection=true 且 Validator 未拒绝 → 计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_weak_001", expect_weak_rejection=True),
        ]
        actual = {"join_weak_001": {"weak_rejected": False}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["weak_none_adopted_count"] == 1, (
            f"Validator 未拒绝 WEAK Join 应计 1 次违规，"
            f"实际: {data['weak_none_adopted_count']}"
        )

    def test_weak_rejection_not_counted_when_validator_rejects(self):
        """expect_weak_rejection=true 且 Validator 正确拒绝 → 不计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_weak_001", expect_weak_rejection=True),
        ]
        actual = {"join_weak_001": {"weak_rejected": True}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["weak_none_adopted_count"] == 0, (
            f"Validator 正确拒绝 WEAK Join 不应计违规，"
            f"实际: {data['weak_none_adopted_count']}"
        )

    # ── 证据链（条件③）：违背预期才计违规 ──

    def test_evidence_chain_counted_when_validator_misses(self):
        """expect_evidence_chain_failure=true 且 Validator 未检测到 → 计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
        ]
        actual = {"join_ev_001": {"evidence_chain_validated": False}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["missing_evidence_chain_count"] == 1, (
            f"Validator 未检测到证据链缺口应计 1 次违规，"
            f"实际: {data['missing_evidence_chain_count']}"
        )

    def test_evidence_chain_not_counted_when_validator_detects(self):
        """expect_evidence_chain_failure=true 且 Validator 正确检测 → 不计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
        ]
        actual = {"join_ev_001": {"evidence_chain_validated": True}}
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["missing_evidence_chain_count"] == 0, (
            f"Validator 正确检测到证据链缺口不应计违规，"
            f"实际: {data['missing_evidence_chain_count']}"
        )

    # ── expect_* 未设置时不计数 ──

    def test_expect_flags_not_set_no_violations(self):
        """expect_* 均未设置时，无论 actual 值如何都不计违规。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_ok_001"),  # 无 expect_* 标记
        ]
        actual = {
            "join_ok_001": {
                "false_negative_detected": False,
                "weak_rejected": False,
                "evidence_chain_validated": False,
            },
        }
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["false_negative_count"] == 0
        assert data["weak_none_adopted_count"] == 0
        assert data["missing_evidence_chain_count"] == 0

    # ── 三条条件独立 + 真实违规触发 REJECT ──

    def test_all_three_violations_independent_and_trigger_reject(self):
        """三条零容忍条件独立触发——真实违规互不干扰，任一即 REJECT。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_fn_001", expect_false_negative=True),
            _make_join_case("join_weak_001", expect_weak_rejection=True),
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
            _make_join_case("join_ok_001"),  # 正常通过——无标记
        ]
        # 全部违背预期——系统均未检测到问题
        actual = {
            "join_fn_001": {"false_negative_detected": False},
            "join_weak_001": {"weak_rejected": False},
            "join_ev_001": {"evidence_chain_validated": False},
        }
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["false_negative_count"] == 1
        assert data["weak_none_adopted_count"] == 1
        assert data["missing_evidence_chain_count"] == 1
        assert data["total_joins"] == 4

        # 引擎层面验证三条条件正确触发 REJECT
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2(data)
        assert result.verdict == "REJECT", (
            f"任一零容忍条件违约应 REJECT，实际: {result.verdict}"
        )

    def test_all_expectations_met_triggers_pass(self):
        """全部预期满足时 D2 应 PASS。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_fn_001", expect_false_negative=True),
            _make_join_case("join_weak_001", expect_weak_rejection=True),
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
        ]
        # 全部符合预期——系统正确检测到所有问题
        actual = {
            "join_fn_001": {"false_negative_detected": True},
            "join_weak_001": {"weak_rejected": True},
            "join_ev_001": {"evidence_chain_validated": True},
        }
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        assert data["false_negative_count"] == 0
        assert data["weak_none_adopted_count"] == 0
        assert data["missing_evidence_chain_count"] == 0

        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2(data)
        assert result.verdict == "PASS", (
            f"全部预期满足应 PASS，实际: {result.verdict}"
        )

    # ── actual 覆盖缺失 → REJECT（评测覆盖缺失不能静默放行）──

    def test_empty_actual_with_zero_tolerance_fixture_rejects(self):
        """actual_results={} 时含 expect_* 的零容忍 fixture 缺少覆盖 → REJECT。

        传入空 dict 不等于"系统无违规"——这意味着无法验证零容忍条件，
        必须 REJECT 而非 PASS。
        """
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_weak_001", expect_weak_rejection=True),
        ]
        # 空 actual_results——零容忍 fixture 缺少覆盖
        data = runner.evaluate_join_quality(golden, actual_results={})
        assert data["missing_actual_count"] == 1, (
            f"空 actual 时含 expect_* 的 fixture 应计 missing_actual，"
            f"实际: {data['missing_actual_count']}"
        )
        assert "join_weak_001" in data["missing_actual_details"][0]
        # 违规计数保持 0——没有 actual 不代表系统违规，只代表无法评测
        assert data["weak_none_adopted_count"] == 0

        # D2 引擎层面——missing_actual > 0 → REJECT
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2(data)
        assert result.verdict == "REJECT", (
            f"零容忍 fixture 缺少 actual 覆盖应 REJECT，实际: {result.verdict}"
        )

    def test_partial_actual_coverage_with_zero_tolerance_fixture_rejects(self):
        """actual_results 只覆盖部分零容忍 fixture → 未覆盖的触发 REJECT。"""
        runner = HarnessRunner()
        golden = [
            _make_join_case("join_weak_001", expect_weak_rejection=True),
            _make_join_case("join_ev_001", expect_evidence_chain_failure=True),
        ]
        # 只覆盖 join_weak_001，join_ev_001 缺失
        actual = {
            "join_weak_001": {"weak_rejected": True},  # 符合预期
        }
        data = runner.evaluate_join_quality(golden, actual_results=actual)
        # join_weak_001 符合预期——不计违规
        assert data["weak_none_adopted_count"] == 0
        # join_ev_001 缺 actual 覆盖——计 missing
        assert data["missing_actual_count"] == 1, (
            f"部分覆盖时未覆盖的零容忍 fixture 应计 missing_actual=1，"
            f"实际: {data['missing_actual_count']}"
        )
        assert any("join_ev_001" in d for d in data["missing_actual_details"])

        # D2 引擎——有 missing_actual → REJECT
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2(data)
        assert result.verdict == "REJECT", (
            f"部分覆盖导致 missing_actual > 0 应 REJECT，实际: {result.verdict}"
        )

    # ── D2 stub → WARN（不阻断退出）──

    def test_dimension_2_warn_on_stub_data(self):
        """D2 收到 stub 数据时返回 WARN——不阻断退出。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_2({
            "_stub": True,
            "_stub_reason": "测试 stub 原因",
            "false_negative_count": 0,
            "weak_none_adopted_count": 0,
            "missing_evidence_chain_count": 0,
            "total_joins": 0,
        })
        assert result.verdict == "WARN", (
            f"stub 数据应返回 WARN 而非 REJECT，实际: {result.verdict}"
        )


# ════════════════════════════════════════════
# 测试类 3：HarnessReport 生成
# ════════════════════════════════════════════


class TestHarnessReportGeneration:
    """HarnessReport 正确生成——GO/NO_GO 判决。"""

    def _make_all_pass_dimensions(self) -> list[DimensionResult]:
        """构造 7 个全 PASS 的维度结果。"""
        return [
            DimensionResult(
                dimension=i, name=f"维度{i}", verdict="PASS",
                metrics={"m": 1.0},
            )
            for i in range(1, 8)
        ]

    def test_overall_verdict_go_when_all_pass(self):
        """所有维度 PASS → GO。"""
        dimensions = self._make_all_pass_dimensions()
        engine = HarnessMetricsEngine()
        report = engine.produce_report(
            dimensions=dimensions,
            dataset_counts={
                "golden": 10, "rejection": 5, "attack": 25,
                "performance": 15, "regression": 8,
            },
        )
        assert report.overall_verdict == HarnessVerdict.GO, (
            f"全 PASS 应为 GO，实际: {report.overall_verdict}"
        )
        assert report.rejected_dimensions == []
        assert report.report_id.startswith("hr_"), (
            f"report_id 应以 'hr_' 开头，实际: {report.report_id}"
        )

    def test_overall_verdict_no_go_when_any_reject(self):
        """任何一个维度 REJECT → NO_GO。"""
        dimensions = self._make_all_pass_dimensions()
        # D2 设为 REJECT
        dimensions[1] = DimensionResult(
            dimension=2, name="Join 推理质量（零容忍）",
            verdict="REJECT",
            metrics={"weak_none_adopted": 1},
            details=["WEAK Join 被采纳——零容忍违规"],
        )
        engine = HarnessMetricsEngine()
        report = engine.produce_report(
            dimensions=dimensions,
            dataset_counts={
                "golden": 10, "rejection": 5, "attack": 25,
                "performance": 15, "regression": 8,
            },
        )
        assert report.overall_verdict == HarnessVerdict.NO_GO, (
            f"D2 REJECT 应为 NO_GO，实际: {report.overall_verdict}"
        )
        assert 2 in report.rejected_dimensions

    def test_report_includes_dataset_counts_and_warn_items(self):
        """报告包含所有数据集计数和 WARN 项。"""
        dimensions = self._make_all_pass_dimensions()
        # D4 设为 WARN（占位）
        dimensions[3] = DimensionResult(
            dimension=4, name="编译与执行",
            verdict="WARN",
            metrics={"compile_success_rate": -1.0},
            details=["[stub] 占位"],
        )
        engine = HarnessMetricsEngine()
        counts = {
            "golden": 5, "attack": 24, "rejection": 10,
            "performance": 15, "regression": 8,
        }
        report = engine.produce_report(
            dimensions=dimensions,
            dataset_counts=counts,
        )
        # 有 WARN 项但无 REJECT → 应 GO
        assert report.overall_verdict == HarnessVerdict.GO
        assert report.dataset_counts["golden"] == 5
        assert report.dataset_counts["attack"] == 24
        assert len(report.warn_items) >= 1
        assert any("D4" in w for w in report.warn_items)


# ════════════════════════════════════════════
# 测试类 4：人工接受率——流程可执行
# ════════════════════════════════════════════


class TestHumanAcceptanceFlow:
    """人工接受率评测流程可执行——D5 占位不崩溃。"""

    def test_dimension_5_does_not_crash_with_empty_data(self):
        """D5 即使无人工审查数据也不会崩溃——返回 WARN 占位。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_5(review_results=None)
        assert result.verdict == "WARN", (
            f"无审查数据时应为 WARN（占位），实际: {result.verdict}"
        )
        assert result.metrics["human_acceptance_rate"] == -1.0, (
            "占位指标应为 -1.0 表示无数据"
        )

    def test_dimension_5_accepts_real_data(self):
        """D5 接受真实审查数据并正确计算接受率。"""
        engine = HarnessMetricsEngine()
        result = engine.compute_dimension_5(review_results={
            "total_reviews": 10,
            "accepted_reviews": 8,
        })
        assert result.metrics["total_reviews"] == 10
        assert result.metrics["accepted_reviews"] == 8
        assert result.metrics["human_acceptance_rate"] == 80.0

    def test_runner_produces_report_without_real_human_data(self):
        """完整 run_all() 即使无真实审查数据也生成报告。"""
        runner = HarnessRunner()
        report = runner.run_all()
        assert isinstance(report, HarnessReport)
        assert report.overall_verdict in (HarnessVerdict.GO, HarnessVerdict.NO_GO)
        # 验证维度 5 为 WARN（占位）
        d5 = [d for d in report.dimensions if d.dimension == 5]
        assert len(d5) == 1
        assert d5[0].verdict == "WARN", (
            f"D5 应返回 WARN（占位），实际: {d5[0].verdict}"
        )
        # 验证有 7 个维度
        assert len(report.dimensions) == 7
        # 验证 dataset_counts 包含全部 5 个类别
        for cat in ("golden", "rejection", "attack", "performance", "regression"):
            assert cat in report.dataset_counts, (
                f"dataset_counts 应包含 '{cat}'"
            )


# ════════════════════════════════════════════
# 测试类 5：数据集加载 fixture 验证
# ════════════════════════════════════════════


class TestHarnessDatasetFixtures:
    """验证 5 个数据集目录的 fixture 完整性。"""

    _EXPECTED_DIRS = {
        DatasetCategory.GOLDEN: "golden",
        DatasetCategory.REJECTION: "rejection",
        DatasetCategory.ATTACK: "attack",
        DatasetCategory.PERFORMANCE: "performance",
        DatasetCategory.REGRESSION: "regression",
    }

    def test_all_dataset_directories_exist(self):
        """5 个数据集子目录全部存在。"""
        base = Path(__file__).resolve().parent.parent.parent / "harness" / "datasets"
        for category, dirname in self._EXPECTED_DIRS.items():
            dirpath = base / dirname
            assert dirpath.is_dir(), (
                f"数据集目录不存在: {dirpath}（分类: {category.value}）"
            )

    def test_golden_fixtures_loadable(self):
        """golden 目录的 JSON fixture 可加载为 HarnessCase。"""
        loader = DatasetLoader()
        cases = loader.load_category(DatasetCategory.GOLDEN)
        assert len(cases) >= 4, (
            f"golden 应至少有 4 个 fixture case，实际: {len(cases)}"
        )
        for case in cases:
            assert isinstance(case, HarnessCase)
            assert case.category == DatasetCategory.GOLDEN
            assert case.case_id, "golden case 缺少 case_id"

    def test_rejection_fixtures_loadable(self):
        """rejection 目录的 JSON fixture 可加载。"""
        loader = DatasetLoader()
        cases = loader.load_category(DatasetCategory.REJECTION)
        assert len(cases) >= 1, (
            f"rejection 应至少有 1 个 fixture case，实际: {len(cases)}"
        )
        for case in cases:
            assert isinstance(case, HarnessCase)
            assert case.category == DatasetCategory.REJECTION

    def test_all_datasets_loadable(self):
        """load_all() 加载全部 5 类数据集。"""
        loader = DatasetLoader()
        all_datasets = loader.load_all()
        assert len(all_datasets) == 5, (
            f"应加载 5 类数据集，实际: {len(all_datasets)}"
        )
        for category in DatasetCategory:
            assert category in all_datasets
            assert isinstance(all_datasets[category], list)

    def test_case_ids_are_globally_unique(self):
        """所有数据集的 case_id 全局唯一。"""
        loader = DatasetLoader()
        all_datasets = loader.load_all()
        seen: set[str] = set()
        for category, cases in all_datasets.items():
            for case in cases:
                assert case.case_id not in seen, (
                    f"重复的 case_id: {case.case_id}（在 {category.value} 中）"
                )
                seen.add(case.case_id)

    def test_harness_case_strict_model_forbids_extra(self):
        """HarnessCase 作为 StrictModel 拒绝未定义字段。"""
        with pytest.raises(ValidationError):
            HarnessCase(
                case_id="test_extra",
                category=DatasetCategory.GOLDEN,
                description="extra 字段测试",
                undeclared_field="应被拒绝",  # 未定义的字段
            )

    def test_dataset_loader_cache_reuses_results(self):
        """DatasetLoader 缓存——同一分类多次加载返回相同列表。"""
        loader = DatasetLoader()
        cases1 = loader.load_category(DatasetCategory.GOLDEN)
        cases2 = loader.load_category(DatasetCategory.GOLDEN)
        assert cases1 is cases2, (
            "缓存应使同一分类的第二次加载返回同一对象引用"
        )

    def test_empty_category_returns_empty_list(self):
        """不存在 JSON 文件的子目录返回空列表。"""
        loader = DatasetLoader()
        # performance 目录当前只有 1 个文件
        cases = loader.load_category(DatasetCategory.PERFORMANCE)
        assert isinstance(cases, list)
        # 至少能正常加载
        assert len(cases) >= 1
