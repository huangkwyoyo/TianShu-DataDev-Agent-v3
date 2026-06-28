"""HarnessRunner——Phase 4D 全量评测执行器。

编排全部 7 个维度的评测：
- 维 1（结构化约束力）：加载 golden/rejection 数据集 → 通过 fake Planner 运行
- 维 2（Join 质量）：使用 golden join fixture → 测量 3 个零容忍指标
- 维 3（语义正确性）：直接调用 SemanticEvaluator
- 维 4（编译与执行）：占位——需 Compiler 集成
- 维 5（产品可用性）：占位——需人工审查
- 维 6（安全边界）：直接调用 SecurityEvaluator
- 维 7（运行稳健性）：占位——需多次运行历史

不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

from .dataset_loader import DatasetLoader
from .metrics import HarnessMetricsEngine
from .models import (
    DatasetCategory,
    HarnessCase,
    HarnessReport,
)
from .security_eval import SecurityEvaluator
from .semantic_eval import SemanticEvaluator


class HarnessRunner:
    """Phase 4D 全量 Harness 评测执行器。

    编排所有七个维度的评测，收集数据源，
    通过 HarnessMetricsEngine 计算判决，生成 HarnessReport。
    """

    def __init__(
        self,
        dataset_base_dir: str | None = None,
        attack_dataset_dir: str | None = None,
    ):
        """初始化 HarnessRunner。

        Args:
            dataset_base_dir: 数据集根目录（默认 harness/datasets/）。
            attack_dataset_dir: 攻击数据集目录（默认 SecurityEvaluator 默认路径）。
        """
        self._dataset_loader = DatasetLoader(dataset_base_dir)
        self._metrics_engine = HarnessMetricsEngine()
        self._security_evaluator = SecurityEvaluator(
            attack_dataset_dir=attack_dataset_dir,
        )
        self._semantic_evaluator = SemanticEvaluator()

    def run_security(self):
        """维度 6：运行安全评测——全部 6 种攻击向量。

        Returns:
            SecurityEvalReport
        """
        return self._security_evaluator.run_all()

    def run_semantic(self):
        """维度 3：运行语义评测——全部 5 类语义错误。

        Returns:
            SemanticEvalReport
        """
        return self._semantic_evaluator.run_all()

    def load_datasets(self) -> dict[DatasetCategory, list[HarnessCase]]:
        """加载全部 5 类数据集。

        Returns:
            dict[DatasetCategory, list[HarnessCase]]
        """
        return self._dataset_loader.load_all()

    def evaluate_join_quality(
        self, golden_cases: list[HarnessCase],
        actual_results: dict[str, dict] | None = None,
    ) -> dict:
        """维度 2：Join 质量零容忍评估。

        针对 golden 数据集中的 join fixture 进行三项测量：
        1. 漏报率——预期 Join 未被 Planner 输出
        2. WEAK/NONE 被采纳——SqlBuildPlan 中含非法证据等级
        3. 缺证据链——evidence_checks 不足 3 项或 evidence_chain_yaml 为空

        fixture 的 expected.expect_* 字段仅表示"此用例用于测试该行为"，
        不直接等于违规。只有实际 Planner/Validator 结果与预期不符时，
        才计入违规计数。

        Args:
            golden_cases: golden 分类的 HarnessCase 列表。
            actual_results: Planner/Validator 实际输出，按 case_id 索引。
                结构：{"case_id": {"false_negative_detected": bool,
                "weak_rejected": bool, "evidence_chain_validated": bool}}
                未提供时返回 [stub] 标记，不进行真实评测。

        Returns:
            dict——含三项计数、详情列表和可选的 _stub 标记。
        """
        # 筛选 Join 相关的 golden 用例
        join_cases = [
            c for c in golden_cases
            if "join" in c.case_id.lower()
        ]

        result: dict = {
            "false_negative_count": 0,
            "weak_none_adopted_count": 0,
            "missing_evidence_chain_count": 0,
            "total_joins": len(join_cases),
            "false_negative_details": [],
            "weak_none_details": [],
            "evidence_chain_details": [],
            # 追踪 actual 覆盖缺失——含 expect_* 的零容忍 fixture 缺少覆盖时
            # 不能静默放行，需记录并触发 REJECT
            "missing_actual_count": 0,
            "missing_actual_details": [],
        }

        # 无实际结果时返回 stub——不能因 fixture 期望直接计违规
        if actual_results is None:
            result["_stub"] = True
            result["_stub_reason"] = (
                "缺少 Planner/Validator 实际结果——"
                "无法将 fixture 期望与系统输出对照比较。"
                "接入 actual_results 后自动进入真实评测模式。"
            )
            return result

        # 有实际结果时——逐条比较 fixture 期望 vs 系统实际输出
        for case in join_cases:
            expected = case.expected
            actual = actual_results.get(case.case_id)

            # actual 缺失时检查是否为含 expect_* 的零容忍 fixture——
            # 评测覆盖缺失不能静默跳过，必须记录
            if actual is None:
                # 判断该 case 是否有零容忍期望标记
                has_zero_tolerance = (
                    expected.get("expect_false_negative")
                    or expected.get("expect_weak_rejection")
                    or expected.get("expect_evidence_chain_failure")
                )
                if has_zero_tolerance:
                    # 零容忍 fixture 缺少 actual 覆盖——评测覆盖缺失
                    result["missing_actual_count"] += 1
                    missing_flags = []
                    if expected.get("expect_false_negative"):
                        missing_flags.append("漏报检测")
                    if expected.get("expect_weak_rejection"):
                        missing_flags.append("WEAK 拒绝")
                    if expected.get("expect_evidence_chain_failure"):
                        missing_flags.append("证据链完整性")
                    result["missing_actual_details"].append(
                        f"{case.case_id}: 缺少 actual 覆盖——"
                        f"无法验证 {'/'.join(missing_flags)}"
                    )
                continue

            # ── 条件①：漏报检查 ──
            # fixture 标记 expect_false_negative=true 表示此用例用于测试
            # 系统能否检测到漏报。只有系统未检测到（false_negative_detected
            # 不为 True）时才构成真实违规。
            if expected.get("expect_false_negative"):
                if actual.get("false_negative_detected") is True:
                    # 系统正确检测到漏报——符合预期，不计违规
                    pass
                else:
                    # 系统未检测到漏报——违背预期，计入违规
                    result["false_negative_count"] += 1
                    result["false_negative_details"].append(
                        f"{case.case_id}: 预期 Join 未被 Planner 输出（漏报）——"
                        f"系统未检测到此漏报"
                    )

            # ── 条件②：WEAK/NONE 拒绝检查 ──
            # fixture 标记 expect_weak_rejection=true 表示此用例用于测试
            # Validator 能否拒绝 WEAK 证据等级的 Join。
            # 只有 Validator 未拒绝（weak_rejected 不为 True）时才构成违规。
            if expected.get("expect_weak_rejection"):
                if actual.get("weak_rejected") is True:
                    # Validator 正确拒绝了 WEAK Join——符合预期
                    pass
                else:
                    # Validator 未拒绝——违背预期
                    result["weak_none_adopted_count"] += 1
                    result["weak_none_details"].append(
                        f"{case.case_id}: WEAK/NONE 证据等级的 Join 被错误采纳——"
                        f"Validator 未拦截"
                    )

            # ── 条件③：证据链完整性检查 ──
            # fixture 标记 expect_evidence_chain_failure=true 表示此用例用于
            # 测试 Validator 能否检测证据链不完整。
            # 只有 Validator 未检测到（evidence_chain_validated 不为 True）
            # 时才构成违规。
            if expected.get("expect_evidence_chain_failure"):
                if actual.get("evidence_chain_validated") is True:
                    # Validator 正确检测到证据链不完整——符合预期
                    pass
                else:
                    # Validator 未检测到——违背预期
                    result["missing_evidence_chain_count"] += 1
                    result["evidence_chain_details"].append(
                        f"{case.case_id}: 证据链不完整——"
                        f"evidence_checks 不足 3 项或 evidence_chain_yaml 为空，"
                        f"但系统未检测到此缺口"
                    )

        return result

    def run_all(
        self,
        run_compiler: bool = False,
        run_history: list | None = None,
        review_results: dict | None = None,
    ) -> HarnessReport:
        """运行全量 7 维评测，产生 HarnessReport。

        Args:
            run_compiler: 是否运行编译器检查（默认 False——占位）。
            run_history: 多次运行历史记录（默认 None——占位）。
            review_results: 人工审查结果（默认 None——占位）。

        Returns:
            HarnessReport——含全部 7 维度结果 + HarnessVerdict。
        """
        # 步骤 1：加载全部 5 类数据集
        datasets = self.load_datasets()
        dataset_counts = {
            k.value: len(v) for k, v in datasets.items()
        }

        # 步骤 2：运行安全评测（维度 6）
        security_report = self._security_evaluator.run_all()

        # 步骤 3：运行语义评测（维度 3）
        semantic_report = self._semantic_evaluator.run_all()

        # 步骤 4：收集 Join 质量数据（维度 2）
        golden_cases = datasets.get(DatasetCategory.GOLDEN, [])
        join_quality_data = self.evaluate_join_quality(golden_cases)

        # 步骤 5：计算全部 7 个维度
        dimensions = self._metrics_engine.compute_all(
            datasets=datasets,
            security_report=security_report,
            semantic_report=semantic_report,
            join_quality_data=join_quality_data,
            review_results=review_results,
            run_history=run_history,
        )

        # 步骤 6：生成 HarnessReport
        extra_reports = {
            "security": security_report.model_dump(),
            "semantic": semantic_report.model_dump(),
            "join_quality": join_quality_data,
        }

        report = self._metrics_engine.produce_report(
            dimensions=dimensions,
            dataset_counts=dataset_counts,
            extra_reports=extra_reports,
        )

        return report
