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
    """单个 Spark 评测用例——含输入、预期行为和实际结果。

    Phase 9A3 新增 developer_spec_md 字段——供自动驱动器模式使用。
    提供 developer_spec_md 的用例在 evaluate() 时自动走 Pipeline → Orchestrator 全链路；
    未提供的用例回退到被动模式（读取预置的 case.passed）。
    """

    case_id: str                                   # 用例唯一标识
    dimension: SparkEvalDimension                  # 所属评测维度
    description: str                               # 人类可读的评测场景描述
    expected_behavior: str                         # 预期行为描述
    actual_result: dict = Field(default_factory=dict)   # 实际评测数据
    passed: bool = False                           # 是否通过
    developer_spec_md: str = ""                    # 9A3 新增：完整 DeveloperSpec Markdown 文本


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

    两种模式：
    - **自动模式（Phase 9A3 新增）**：注入 Pipeline + Orchestrator 后，
      evaluate() 对每个含 developer_spec_md 的 case 自动执行全链路——
      Pipeline.run_all() → export_artifacts() → adapt_lite_to_v1() →
      Orchestrator.run() → 读取 stage_results 自动判定 passed。
    - **被动模式（向后兼容）**：evaluate(passive=True) 仅聚合预置的 case.passed，
      与 Phase 8 行为一致。

    使用方式：
        # 自动模式
        runner = SparkHarnessRunner(pipeline=pipeline, orchestrator=orchestrator)
        runner.add_case(case)  # case 需含 developer_spec_md
        report = runner.evaluate()

        # 被动模式
        runner = SparkHarnessRunner()
        runner.add_case(case)  # case.passed 已预置
        report = runner.evaluate(passive=True)
    """

    def __init__(
        self,
        pipeline: object | None = None,        # Pipeline | None
        orchestrator: object | None = None,    # SparkOrchestrator | None
    ) -> None:
        """初始化评测执行器。

        Args:
            pipeline: Pipeline 实例——提供时启用自动模式（evaluate() 自动执行全链路）。
            orchestrator: SparkOrchestrator 实例——与 pipeline 配合使用。
        """
        self._cases: list[SparkEvalCase] = []
        self._pipeline = pipeline
        self._orchestrator = orchestrator

    def add_case(self, case: SparkEvalCase) -> None:
        """添加评测用例。

        Args:
            case: 单个 SparkEvalCase
        """
        self._cases.append(case)

    def evaluate(self, passive: bool = False) -> SparkEvalReport:
        """执行全部评测用例并产出报告。

        两种模式：
        - 自动模式（passive=False 且注入 pipeline + orchestrator）：
          对每个含 developer_spec_md 的 case 自动执行 Pipeline → Orchestrator 全链路，
          从 SparkPipelineState.stage_results 自动判定 passed。
        - 被动模式（passive=True 或未注入 pipeline）：
          仅聚合预置的 case.passed（Phase 8 行为，向后兼容）。

        Args:
            passive: True 时强制被动模式（仅聚合预置 passed），忽略 pipeline/orchestrator。

        Returns:
            SparkEvalReport——含 5 维度汇总结果
        """
        # ── 自动模式：Pipeline → Orchestrator 全链路 ──
        if not passive and self._pipeline is not None and self._orchestrator is not None:
            self._execute_auto_mode()

        # ── 聚合报告（自动/被动共用）──
        report_id = SparkEvalReport.generate_report_id()

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

    def _execute_auto_mode(self) -> None:
        """自动模式——对每个含 developer_spec_md 的 case 执行全链路评测。

        对每个 case：
        1. Pipeline.run_all(markdown_text) → request_id
        2. Pipeline.export_artifacts(request_id) → bundle
        3. adapt_lite_to_v1(bundle.data_transform_contract) → V1 contract
        4. Orchestrator.run(contract=v1, sql_plan=bundle.sql_build_plan) → state
        5. 从 state.stage_results 自动判定 case.passed

        不含 developer_spec_md 的 case 保持原样（被动模式兼容）。
        Pipeline 执行异常时 case 标记为 failed，错误信息写入 actual_result。
        """
        import tempfile

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1

        for case in self._cases:
            if not case.developer_spec_md:
                # 无 DeveloperSpec 文本——保持被动模式
                continue

            tmpdir = tempfile.mkdtemp()
            try:
                # 从 actual_result 读取可选的 table_paths / table_mapping
                table_paths: dict = case.actual_result.get("table_paths", {})
                table_mapping: dict = case.actual_result.get("table_mapping", {})

                # Step 1: Pipeline 执行
                pipeline = self._pipeline
                result = pipeline.run_all(
                    case.developer_spec_md,
                    table_paths=table_paths,
                    table_mapping=table_mapping,
                )
                request_id = result["request_id"]

                # Step 2: 导出中间产物
                bundle = pipeline.export_artifacts(request_id)
                if bundle is None:
                    case.passed = False
                    case.actual_result["error"] = "export_artifacts 返回 None"
                    continue

                # Step 3: 适配 contract（Lite → V1）
                raw_contract = bundle.data_transform_contract
                if raw_contract is None:
                    case.passed = False
                    case.actual_result["error"] = (
                        "data_transform_contract 为 None——"
                        "Pipeline 未产出 contract"
                    )
                    continue

                from tianshu_datadev.artifacts.models import DataTransformContractV1
                if isinstance(raw_contract, DataTransformContractV1):
                    v1_contract = raw_contract
                else:
                    v1_contract = adapt_lite_to_v1(raw_contract)

                # Step 4: Orchestrator 执行
                orchestrator = self._orchestrator
                state = orchestrator.run(
                    contract=v1_contract,
                    sql_plan=bundle.sql_build_plan,
                )

                # Step 5: 自动判定 passed——基于 stage_results
                stage_results = dict(state.stage_results)
                case.actual_result["stage_results"] = stage_results
                case.actual_result["overall_status"] = (
                    state.overall_status.value
                    if hasattr(state.overall_status, "value")
                    else str(state.overall_status)
                )

                # 判定规则：MAPPER + COMPILER + VALIDATOR + COMPARATOR 均为 SUCCESS
                # （DEVELOPER 和 PHYSICAL_VERIFIER 可 SKIPPED）
                critical_stages = {
                    "MAPPER", "COMPILER", "VALIDATOR", "COMPARATOR",
                }
                all_critical_ok = all(
                    stage_results.get(s, "NOT_EXECUTED") == "SUCCESS"
                    for s in critical_stages
                )
                case.passed = all_critical_ok

                # 记录 comparator 结果（若存在）
                if state.comparator_report is not None:
                    case.actual_result["comparator_status"] = (
                        state.comparator_report.status.value
                        if hasattr(state.comparator_report.status, "value")
                        else str(state.comparator_report.status)
                    )

            except Exception as e:
                case.passed = False
                case.actual_result["error"] = str(e)
            finally:
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)

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
