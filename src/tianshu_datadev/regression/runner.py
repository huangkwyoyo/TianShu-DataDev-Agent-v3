"""RegressionRunner——Prompt 回归用例加载、执行和验证。

回归用例集（regression_cases.jsonl）配套 Prompt 模板——
每份 Prompt 模板有对应的回归用例集，确保 Prompt 优化/重构后
LLM 输出的结构化约束力不退化。

工作方式：
1. 加载 regression_cases.jsonl → RegressionCase 列表
2. 通过 FakeLLMAdapter 将期望输出注册为 fixture
3. 通过 LLMGateway.submit() 执行
4. 对比实际 validation_status 与期望状态
5. 生成 RegressionReport
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import LlmRequest
from tianshu_datadev.prompts.manager import PromptManager


class RegressionCase(StrictModel):
    """单个回归用例——从 regression_cases.jsonl 加载。

    每个用例包含：
    - 唯一标识和描述
    - 期望的 LLM 输出（用作 FakeLLMAdapter fixture）
    - 期望的校验状态（valid 通过 / invalid 拒绝）
    - 期望的错误模式（仅 invalid 用例）
    """

    case_id: str
    description: str
    expected_output: dict  # 期望的 LLM 输出——用作 FakeAdapter fixture
    expected_status: Literal["valid", "invalid"]
    expected_error_patterns: list[str] = []  # invalid 用例期望匹配的错误子串


class RegressionReport(StrictModel):
    """回归执行报告——记录每个用例的执行结果。

    summary 字段提供总体统计（通过/失败/错误），
    failures 列表仅包含不通过的用例详情。
    """

    task: str
    version: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0  # 执行异常（非断言失败）
    failures: list[dict] = []  # 失败用例详情

    @property
    def all_passed(self) -> bool:
        """是否全部通过。"""
        return self.failed == 0 and self.errors == 0 and self.total > 0

    @property
    def pass_rate(self) -> float:
        """通过率（0.0 ~ 1.0）。"""
        if self.total == 0:
            return 0.0
        return self.passed / self.total


class RegressionRunner:
    """Prompt 回归用例执行器。

    用法：
        runner = RegressionRunner()
        report = runner.run("developer_spec_parser", "v001")
        if not report.all_passed:
            print(f"回归失败：{report.failures}")

    注意：
    - 回归用例通过 FakeLLMAdapter 执行——不依赖真实 LLM
    - 验证的是 Schema 的结构化约束力，而非 LLM 的语义正确性
    - 每个用例独立执行——前一个失败不影响后续
    """

    # 默认回归用例根目录
    _DEFAULT_CASES_ROOT = "regression_cases"

    def __init__(
        self,
        cases_root: str | None = None,
        prompt_manager: PromptManager | None = None,
    ) -> None:
        """初始化回归执行器。

        Args:
            cases_root: 回归用例根目录——若为 None，使用默认路径
            prompt_manager: PromptManager 实例——若为 None，自动创建
        """
        if cases_root is None:
            cwd = Path.cwd()
            candidate = cwd / self._DEFAULT_CASES_ROOT
            if candidate.is_dir():
                cases_root = str(candidate)
            else:
                this_dir = Path(__file__).resolve().parent.parent.parent.parent
                cases_root = str(this_dir / "regression_cases")

        self._cases_root = Path(cases_root)
        if not self._cases_root.is_dir():
            raise ValueError(f"回归用例根目录不存在：{self._cases_root}")

        self._prompt_manager = prompt_manager or PromptManager()

    @property
    def cases_root(self) -> str:
        """返回回归用例根目录路径。"""
        return str(self._cases_root)

    def run(self, task: str, version: str) -> RegressionReport:
        """执行指定 task/version 的全部回归用例。

        Args:
            task: 任务标识（如 "developer_spec_parser"）
            version: Prompt 版本号（如 "v001"）

        Returns:
            RegressionReport——含总体统计和失败详情

        Raises:
            ValueError: task/version 的回归用例文件不存在
        """
        # ── 加载用例 ──
        cases = self.load_cases(task, version)
        if not cases:
            raise ValueError(
                f"task='{task}' version='{version}' 无回归用例——"
                f"文件为空或不存在"
            )

        # ── 获取 Schema 绑定 ──
        template = self._prompt_manager.get_prompt(task, version)
        schema_name = template.schema_binding.schema_name
        schema_version = template.schema_binding.schema_version

        # ── 初始化报告 ──
        report = RegressionReport(task=task, version=version, total=len(cases))

        # ── 逐用例执行 ──
        for case in cases:
            try:
                # 构造 FakeLLMAdapter——只注册当前用例的期望输出
                adapter = FakeLLMAdapter()
                adapter.register_default_for_task(task=task, output=case.expected_output)

                gateway = LLMGateway(adapter=adapter, prompt_manager=self._prompt_manager)

                request = LlmRequest(
                    request_id=LlmRequest.generate_request_id(),
                    task=task,
                    prompt_version=version,
                    schema_name=schema_name,
                    schema_version=schema_version,
                    temperature=0.0,
                    model="fake",
                )

                response = gateway.submit(request)

                # ── 验证 ──
                actual_status = response.validation_status
                if actual_status != case.expected_status:
                    report.failed += 1
                    report.failures.append({
                        "case_id": case.case_id,
                        "description": case.description,
                        "reason": (
                            f"期望 validation_status='{case.expected_status}'，"
                            f"实际='{actual_status}'——"
                            f"错误：{response.validation_errors}"
                        ),
                    })
                    continue

                # invalid 用例额外验证错误模式
                if case.expected_status == "invalid" and case.expected_error_patterns:
                    errors_text = " ".join(response.validation_errors).lower()
                    for pattern in case.expected_error_patterns:
                        if pattern.lower() not in errors_text:
                            report.failed += 1
                            report.failures.append({
                                "case_id": case.case_id,
                                "description": case.description,
                                "reason": (
                                    f"期望错误包含 '{pattern}'，"
                                    f"实际错误：{response.validation_errors}"
                                ),
                            })
                            break
                    else:
                        report.passed += 1
                else:
                    report.passed += 1

            except Exception as e:
                report.errors += 1
                report.failures.append({
                    "case_id": case.case_id,
                    "description": case.description,
                    "reason": f"执行异常：{type(e).__name__}: {e}",
                })

        return report

    def run_all(self) -> list[RegressionReport]:
        """执行全部已注册 task/version 的回归用例。

        Returns:
            RegressionReport 列表——每个 task/version 一个报告
        """
        reports: list[RegressionReport] = []
        for task in self.list_tasks():
            for version in self.list_versions(task):
                try:
                    report = self.run(task, version)
                    reports.append(report)
                except ValueError:
                    # 跳过无回归用例的 task
                    pass
        return reports

    # ── 加载方法 ──

    def load_cases(self, task: str, version: str) -> list[RegressionCase]:
        """从 JSONL 文件加载回归用例。

        Args:
            task: 任务标识
            version: 版本号

        Returns:
            RegressionCase 列表

        Raises:
            ValueError: 文件不存在或格式错误
        """
        cases_path = self._cases_root / task / f"{version}.jsonl"
        if not cases_path.is_file():
            raise ValueError(f"回归用例文件不存在：{cases_path}")

        cases: list[RegressionCase] = []
        raw_content = cases_path.read_text(encoding="utf-8")
        for line_no, line in enumerate(raw_content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue  # 跳过空行和注释行

            try:
                data = json.loads(stripped)
                case = RegressionCase(
                    case_id=data["case_id"],
                    description=data.get("description", ""),
                    expected_output=data["expected_output"],
                    expected_status=data["expected_status"],
                    expected_error_patterns=data.get("expected_error_patterns", []),
                )
                cases.append(case)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"回归用例文件 '{cases_path}' 第 {line_no} 行 JSON 解析失败：{e}"
                ) from e
            except KeyError as e:
                raise ValueError(
                    f"回归用例文件 '{cases_path}' 第 {line_no} 行缺少必填字段：{e}"
                ) from e

        return cases

    def list_tasks(self) -> list[str]:
        """列出所有有回归用例的 task。

        Returns:
            task 名称列表（按字母序）
        """
        tasks: list[str] = []
        for entry in sorted(self._cases_root.iterdir()):
            if entry.is_dir():
                tasks.append(entry.name)
        return sorted(tasks)

    def list_versions(self, task: str) -> list[str]:
        """列出某 task 的所有回归用例版本。

        Args:
            task: 任务标识

        Returns:
            版本号列表（如 ["v001"]）
        """
        task_dir = self._cases_root / task
        if not task_dir.is_dir():
            return []

        versions: list[str] = []
        for entry in sorted(task_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".jsonl":
                versions.append(entry.stem)

        return sorted(versions)
