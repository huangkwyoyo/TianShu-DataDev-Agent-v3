"""RegressionRunner 测试——加载、执行、报告生成。

验证：
1. 加载 4 份 regression_cases.jsonl——文件可解析且用例存在
2. valid 用例通过 Gateway Schema 校验
3. invalid 用例被正确拒绝且错误模式匹配
4. run_all() 覆盖全部 task/version
"""

from __future__ import annotations

import pytest

from tianshu_datadev.prompts.manager import PromptManager
from tianshu_datadev.regression.runner import RegressionReport, RegressionRunner


@pytest.fixture(scope="module")
def runner() -> RegressionRunner:
    """创建 RegressionRunner——module scope 复用。"""
    prompt_manager = PromptManager()
    return RegressionRunner(prompt_manager=prompt_manager)


# ════════════════════════════════════════════
# 回归用例文件存在性
# ════════════════════════════════════════════


class TestRegressionCasesExist:
    """验证 4 份 regression_cases.jsonl 存在且可解析。"""

    _EXPECTED_TASKS = [
        "developer_spec_parser",
        "relationship_planner",
        "sql_build_planner",
        "sql_program_planner",
    ]

    def test_all_four_tasks_registered(self, runner: RegressionRunner):
        """4 个 task 全部有回归用例目录。"""
        tasks = runner.list_tasks()
        for task in self._EXPECTED_TASKS:
            assert task in tasks, f"缺少回归用例 task: {task}"

    @pytest.mark.parametrize("task", _EXPECTED_TASKS)
    def test_each_task_has_v001_cases(self, runner: RegressionRunner, task: str):
        """每个 task 都有 v001 回归用例。"""
        versions = runner.list_versions(task)
        assert "v001" in versions, f"task='{task}' 缺少 v001 回归用例"

    @pytest.mark.parametrize("task", _EXPECTED_TASKS)
    def test_each_task_loads_at_least_4_cases(self, runner: RegressionRunner, task: str):
        """每个 task 至少 4 个回归用例（覆盖 valid/invalid/边界）。"""
        cases = runner.load_cases(task, "v001")
        # sql_build_planner 有 4 个（移除的 unknown_step_type 由 discriminated union 拦截），
        # 其他 task 有 4-5 个
        min_cases = 4
        assert len(cases) >= min_cases, (
            f"task='{task}' 仅有 {len(cases)} 个回归用例——至少需要 {min_cases} 个"
        )

    @pytest.mark.parametrize("task", _EXPECTED_TASKS)
    def test_each_task_has_valid_and_invalid_cases(self, runner: RegressionRunner, task: str):
        """每个 task 同时有 valid 和 invalid 用例。"""
        cases = runner.load_cases(task, "v001")
        valid_count = sum(1 for c in cases if c.expected_status == "valid")
        invalid_count = sum(1 for c in cases if c.expected_status == "invalid")
        assert valid_count >= 2, f"task='{task}' valid 用例不足（{valid_count}）"
        assert invalid_count >= 1, f"task='{task}' invalid 用例不足（{invalid_count}）"


# ════════════════════════════════════════════
# 回归执行
# ════════════════════════════════════════════


class TestRegressionExecution:
    """执行回归用例——valid 通过 / invalid 拒绝。"""

    _EXPECTED_TASKS = [
        "developer_spec_parser",
        "relationship_planner",
        "sql_build_planner",
        "sql_program_planner",
    ]

    @pytest.mark.parametrize("task", _EXPECTED_TASKS)
    def test_all_cases_pass_for_task(self, runner: RegressionRunner, task: str):
        """某 task 的全部回归用例通过。"""
        report = runner.run(task, "v001")
        assert report.all_passed, (
            f"task='{task}' 回归失败——"
            f"通过={report.passed}/{report.total}，"
            f"失败={report.failed}，异常={report.errors}\n"
            f"失败详情：{report.failures}"
        )

    def test_run_all_returns_all_reports(self, runner: RegressionRunner):
        """run_all() 返回全部 4 份报告。"""
        reports = runner.run_all()
        assert len(reports) == 4, f"应有 4 份报告，实际 {len(reports)}"
        for report in reports:
            assert isinstance(report, RegressionReport)
            assert report.total > 0

    def test_all_reports_pass(self, runner: RegressionRunner):
        """run_all() 全部报告 all_passed=True。"""
        reports = runner.run_all()
        for report in reports:
            assert report.all_passed, (
                f"task='{report.task}' v{report.version} 回归失败：{report.failures}"
            )


# ════════════════════════════════════════════
# 报告属性
# ════════════════════════════════════════════


class TestRegressionReportProperties:
    """RegressionReport 的属性和计算方法。"""

    def test_report_pass_rate_calculation(self, runner: RegressionRunner):
        """通过率计算正确。"""
        report = runner.run("developer_spec_parser", "v001")
        expected_rate = report.passed / max(report.total, 1)
        assert report.pass_rate == pytest.approx(expected_rate)
        if report.all_passed:
            assert report.pass_rate == 1.0

    def test_empty_report_fails(self):
        """total=0 的报告 all_passed=False。"""
        report = RegressionReport(task="empty", version="v001")
        assert not report.all_passed
        assert report.pass_rate == 0.0
