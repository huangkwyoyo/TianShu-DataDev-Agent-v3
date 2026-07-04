"""Phase 8 Spark Harness——Spark 路径 5 维度评测框架。

5 个维度：
1. SPARK_CONTRACT_FIDELITY：Contract → SparkPlan 映射精确性
2. SPARK_COMPILATION_DETERMINISM：同一 SparkPlan 多次编译产出相同 hash
3. SPARK_VALIDATOR_COVERAGE：Validator 对恶意代码的检测率
4. SPARK_LOGIC_EQUIVALENCE：SQL ↔ Spark 逻辑链路对比
5. SPARK_PHYSICAL_CONSISTENCY：双引擎物理结果一致性

评测器不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# SparkEvalDimension——5 维度枚举
# ════════════════════════════════════════════


class SparkEvalDimension(str, Enum):
    """Spark 路径 5 个评测维度。"""

    SPARK_CONTRACT_FIDELITY = "SPARK_CONTRACT_FIDELITY"        # 维度 1：映射精确性
    SPARK_COMPILATION_DETERMINISM = "SPARK_COMPILATION_DETERMINISM"  # 维度 2：编译确定性
    SPARK_VALIDATOR_COVERAGE = "SPARK_VALIDATOR_COVERAGE"      # 维度 3：校验覆盖率
    SPARK_LOGIC_EQUIVALENCE = "SPARK_LOGIC_EQUIVALENCE"        # 维度 4：逻辑等价性
    SPARK_PHYSICAL_CONSISTENCY = "SPARK_PHYSICAL_CONSISTENCY"  # 维度 5：物理一致性


# ════════════════════════════════════════════
# SparkEvalCase——单个评测用例
# ════════════════════════════════════════════


class SparkEvalCase(StrictModel):
    """单个 Spark 评测用例——含输入、预期行为和实际结果。"""

    case_id: str                                   # 用例唯一标识
    dimension: SparkEvalDimension                  # 所属评测维度
    description: str                               # 人类可读的评测场景描述
    expected_behavior: str                         # 预期行为描述
    actual_result: dict = Field(default_factory=dict)   # 实际评测数据
    passed: bool = False                           # 是否通过


# ════════════════════════════════════════════════
# SparkEvalReport——评测报告
# ════════════════════════════════════════════════


class SparkEvalReport(StrictModel):
    """Spark 路径 5 维度评测报告——汇总所有维度的评测结果。"""

    report_id: str                                          # 报告唯一标识
    dimensions: list[SparkEvalDimension] = Field(            # 评测维度列表
        default_factory=lambda: list(SparkEvalDimension),
    )
    cases: list[SparkEvalCase] = Field(default_factory=list)  # 全部用例
    dimension_results: dict[str, dict] = Field(              # 每维度汇总
        default_factory=dict,
    )
    total_passed: int = 0                                     # 通过数
    total_cases: int = 0                                      # 总用例数
    overall_pass_rate: float = 0.0                            # 总通过率

    @staticmethod
    def generate_report_id() -> str:
        """生成确定性报告 ID。"""
        payload = {"phase": "8", "type": "spark_eval"}
        content = json.dumps(payload, sort_keys=True)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"spark_eval_{hash_hex}"


# ════════════════════════════════════════════
# SparkHarnessRunner
# ════════════════════════════════════════════


class SparkHarnessRunner:
    """Spark 路径 5 维度评测执行器。

    评测器不修改被测系统——只读取、验证、报告。

    使用方式：
        runner = SparkHarnessRunner()
        runner.add_case(case)
        report = runner.evaluate()
    """

    def __init__(self) -> None:
        """初始化评测执行器。"""
        self._cases: list[SparkEvalCase] = []

    def add_case(self, case: SparkEvalCase) -> None:
        """添加评测用例。

        Args:
            case: 单个 SparkEvalCase
        """
        self._cases.append(case)

    def evaluate(self) -> SparkEvalReport:
        """执行全部评测用例并产出报告。

        Returns:
            SparkEvalReport——含 5 维度汇总结果
        """
        report_id = SparkEvalReport.generate_report_id()

        # 按维度分组
        dimension_results: dict[str, dict] = {}
        for dim in SparkEvalDimension:
            dim_cases = [c for c in self._cases if c.dimension == dim]
            passed = sum(1 for c in dim_cases if c.passed)
            total = len(dim_cases)
            dimension_results[dim.value] = {
                "dimension_name": dim.value,
                "passed": passed,
                "total": total,
                "pass_rate": passed / total if total > 0 else 0.0,
                "cases": [c.case_id for c in dim_cases],
            }

        total_passed = sum(1 for c in self._cases if c.passed)
        total_cases = len(self._cases)

        return SparkEvalReport(
            report_id=report_id,
            cases=self._cases,
            dimension_results=dimension_results,
            total_passed=total_passed,
            total_cases=total_cases,
            overall_pass_rate=total_passed / total_cases if total_cases > 0 else 0.0,
        )

    def evaluate_dimension(
        self,
        dimension: SparkEvalDimension,
    ) -> dict:
        """评测单个维度——返回该维度的汇总结果。

        Args:
            dimension: 目标评测维度

        Returns:
            含 passed/total/pass_rate 的汇总 dict
        """
        dim_cases = [c for c in self._cases if c.dimension == dimension]
        passed = sum(1 for c in dim_cases if c.passed)
        total = len(dim_cases)
        return {
            "dimension": dimension.value,
            "passed": passed,
            "total": total,
            "pass_rate": passed / total if total > 0 else 0.0,
        }
