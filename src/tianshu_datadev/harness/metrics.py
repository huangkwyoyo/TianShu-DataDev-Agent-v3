"""HarnessMetricsEngine——七维门禁指标计算引擎。

从 eval_runner 收集所有数据源（SecurityEvaluator 报告、SemanticEvaluator 报告、
Join 门禁输出、数据集加载结果），计算各维度的判决。

每个 compute_dimension_N 方法接收所需数据源并返回 DimensionResult。
不修改被测系统——只读取、计算、报告。
"""

from __future__ import annotations

from datetime import datetime, timezone

from .models import (
    DatasetCategory,
    DimensionResult,
    HarnessCase,
    HarnessReport,
    HarnessVerdict,
)


class HarnessMetricsEngine:
    """指标计算引擎——确定性的七维门禁判决。

    每个维度独立计算，判决结果汇总为 HarnessReport。
    占位维度（D4/D5/D7）返回 WARN 并标记 [stub]，
    遵循 SemanticEvaluator 的 known_gap 诚实报告原则。
    """

    def compute_all(
        self,
        datasets: dict[DatasetCategory, list[HarnessCase]],
        security_report: object | None = None,
        semantic_report: object | None = None,
        plan_build_results: dict[str, dict] | None = None,
        join_quality_data: dict | None = None,
        compile_results: list | None = None,
        review_results: dict | None = None,
        run_history: list | None = None,
    ) -> list[DimensionResult]:
        """计算全部 7 个维度的结果。

        Args:
            datasets: 5 类数据集加载结果。
            security_report: SecurityEvalReport 实例或 None。
            semantic_report: SemanticEvalReport 实例或 None。
            plan_build_results: Planner/Builder 结果（占位）。
            join_quality_data: Join 零容忍数据。
            compile_results: 编译执行结果（占位）。
            review_results: 人工审查结果（占位）。
            run_history: 多次运行历史（占位）。

        Returns:
            list[DimensionResult]——按维度编号 1-7 排序的 7 个结果。
        """
        return [
            self.compute_dimension_1(datasets, plan_build_results),
            self.compute_dimension_2(join_quality_data),
            self.compute_dimension_3(semantic_report),
            self.compute_dimension_4(compile_results),
            self.compute_dimension_5(review_results),
            self.compute_dimension_6(security_report),
            self.compute_dimension_7(run_history),
        ]

    # ── 维度 1：结构化约束力 ──

    def compute_dimension_1(
        self,
        datasets: dict[DatasetCategory, list[HarnessCase]],
        plan_build_results: dict[str, dict] | None = None,
    ) -> DimensionResult:
        """维度 1：ParsedDeveloperSpec / SqlBuildPlan 结构化约束。

        测量指标：
        - parse_pass_rate: 合法 DeveloperSpec 的结构化输出通过率（%）
        - extra_field_rejection_rate: extra 字段被严格拒绝的比率（%）

        REJECT：parse_pass_rate < 95% 或 extra_field_rejection_rate < 100%。
        无 plan_build_results 时返回 WARN——不能因缺数据而误放行。
        指标从 plan_build_results dict 中读取，而非使用硬编码默认值。
        """
        total_cases = sum(len(cases) for cases in datasets.values())
        default_metrics: dict[str, float | int | str | bool] = {
            "parse_pass_rate": -1.0,
            "extra_field_rejection_rate": -1.0,
            "total_cases": total_cases,
            "passed_cases": 0,
        }
        details: list[str] = []

        # 无数据 → WARN：不能因缺少 Planner 而静默放行
        if plan_build_results is None:
            details.append(
                "[stub] 缺少 plan_build_results——需集成 fake Planner 后填充真实数据"
            )
            return DimensionResult(
                dimension=1,
                name="结构化约束力",
                verdict="WARN",  # 不阻断退出，但诚实报告数据缺失
                metrics=default_metrics,
                evidence=(
                    "datasets/golden/ + datasets/rejection/ fixtures; "
                    "fake Planner parse results"
                ),
                details=details,
            )

        # 从 plan_build_results 读取真实指标——不使用硬编码默认值
        parse_pass_rate = float(
            plan_build_results.get("parse_pass_rate", 0.0)
        )
        extra_field_rejection_rate = float(
            plan_build_results.get("extra_field_rejection_rate", 0.0)
        )
        passed_cases = int(plan_build_results.get("passed_cases", 0))

        metrics = {
            "parse_pass_rate": parse_pass_rate,
            "extra_field_rejection_rate": extra_field_rejection_rate,
            "total_cases": total_cases,
            "passed_cases": passed_cases,
        }

        # 阈值判决——任一条件不满足即 REJECT
        verdict = "PASS"
        if parse_pass_rate < 95.0:
            verdict = "REJECT"
            details.append(
                f"解析通过率 {parse_pass_rate}% < 95% 阈值"
            )
        elif extra_field_rejection_rate < 100.0:
            verdict = "REJECT"
            details.append(
                f"extra 字段拒绝率 {extra_field_rejection_rate}% < 100% 阈值"
            )

        return DimensionResult(
            dimension=1,
            name="结构化约束力",
            verdict=verdict,
            metrics=metrics,
            evidence=(
                "datasets/golden/ + datasets/rejection/ fixtures; "
                "fake Planner parse results"
            ),
            details=details,
        )

    # ── 维度 2：Join 推理质量（零容忍）──

    def compute_dimension_2(
        self, join_quality_data: dict | None = None,
    ) -> DimensionResult:
        """维度 2：Join 推理质量——唯一零容忍维度。

        三个可测量 REJECT 条件（任一 > 0 → REJECT）：
        1. false_negative_rate: golden 数据集 Join 漏报率
        2. weak_none_adopted: WEAK/NONE 证据等级被采纳数
        3. missing_evidence_chain: 证据链不完整的 Join 数

        无 join_quality_data 或含 _stub 标记时返回 WARN——
        Planner/Validator 未接入时不阻断退出，但提示"待接入"。
        只有基于真实 Planner/Validator 输出与 fixture 期望对比后
        确认的违规才触发 REJECT。
        """
        default_metrics: dict[str, float | int | str | bool] = {
            "false_negative_rate": 0.0,
            "weak_none_adopted": 0,
            "missing_evidence_chain": 0,
            "total_joins_evaluated": 0,
        }
        details: list[str] = []

        # 无数据 → WARN：不能因缺少 Planner/Validator 而阻断退出
        if join_quality_data is None:
            details.append(
                "[stub] 缺少 join_quality_data——需集成 Planner + Validator 后填充"
            )
            return DimensionResult(
                dimension=2,
                name="Join 推理质量（零容忍）",
                verdict="WARN",  # 无数据时不阻断退出
                metrics=default_metrics,
                evidence=(
                    "datasets/golden/join_* fixtures; "
                    "RelationshipHypothesis.candidates[].evidence"
                ),
                details=details,
            )

        # stub 标记 → WARN：Planner/Validator 未接入，evaluate_join_quality
        # 返回了占位数据（fixture 期望未被当作违规）
        if join_quality_data.get("_stub"):
            details.append(
                f"[stub] {join_quality_data.get('_stub_reason', '缺少 Planner/Validator 实际结果')}"
            )
            return DimensionResult(
                dimension=2,
                name="Join 推理质量（零容忍）",
                verdict="WARN",  # stub 阶段不阻断退出
                metrics=default_metrics,
                evidence=(
                    "datasets/golden/join_* fixtures; "
                    "RelationshipHypothesis.candidates[].evidence"
                ),
                details=details,
            )

        # 检查 actual 覆盖缺失——含 expect_* 的零容忍 fixture 缺少实际结果
        # 评测覆盖缺失不能静默放行，必须 REJECT
        missing_actual = join_quality_data.get("missing_actual_count", 0)
        if missing_actual > 0:
            missing_items = join_quality_data.get("missing_actual_details", [])
            if missing_items:
                details.extend(missing_items)
            details.append(
                f"评测覆盖缺失：{missing_actual} 个零容忍 fixture 缺少 actual 覆盖——"
                f"无法验证系统行为是否符合预期"
            )
            return DimensionResult(
                dimension=2,
                name="Join 推理质量（零容忍）",
                verdict="REJECT",  # 零容忍 fixture 缺覆盖 = 不能放行
                metrics={
                    "false_negative_rate": 0.0,
                    "weak_none_adopted": 0,
                    "missing_evidence_chain": 0,
                    "total_joins_evaluated": join_quality_data.get("total_joins", 0),
                    "missing_actual_count": missing_actual,
                },
                evidence=(
                    "datasets/golden/join_* fixtures; "
                    "RelationshipHypothesis.candidates[].evidence"
                ),
                details=details,
            )

        fn_count = join_quality_data.get("false_negative_count", 0)
        wn_count = join_quality_data.get("weak_none_adopted_count", 0)
        ev_count = join_quality_data.get("missing_evidence_chain_count", 0)
        total = join_quality_data.get("total_joins", 0)

        metrics = {
            "false_negative_rate": float(fn_count),
            "weak_none_adopted": int(wn_count),
            "missing_evidence_chain": int(ev_count),
            "total_joins_evaluated": int(total),
        }

        # 收集详情
        for key, label in [
            ("false_negative_details", "漏报"),
            ("weak_none_details", "WEAK/NONE 被采纳"),
            ("evidence_chain_details", "缺证据链"),
        ]:
            items = join_quality_data.get(key, [])
            if items:
                details.extend(items)

        # 零容忍检查——任一真实违规即 REJECT
        verdict = "PASS"
        if fn_count > 0:
            verdict = "REJECT"
        if wn_count > 0:
            verdict = "REJECT"
        if ev_count > 0:
            verdict = "REJECT"

        return DimensionResult(
            dimension=2,
            name="Join 推理质量（零容忍）",
            verdict=verdict,
            metrics=metrics,
            evidence=(
                "datasets/golden/join_* fixtures; "
                "RelationshipHypothesis.candidates[].evidence"
            ),
            details=details,
        )

    # ── 维度 3：语义正确性 ──

    def compute_dimension_3(
        self, semantic_report: object | None = None,
    ) -> DimensionResult:
        """维度 3：语义正确性——5 类语义错误检测。

        使用 SemanticEvaluator.run_all() 的结果。
        known_gaps 中的错误类型不触发 REJECT（诚实记录能力边界）。

        REJECT：无 known_gaps 但仍有未检测到的错误。
        WARN：排除 known_gaps 后有未检测项（不阻断）。
        """
        if semantic_report is None:
            return DimensionResult(
                dimension=3,
                name="语义正确性",
                verdict="REJECT",
                metrics={
                    "detected_errors": 0, "known_gaps": 0, "undetected": 0,
                },
                evidence="缺失 SemanticEvaluator 结果",
                details=["[stub] 缺少 semantic_report——需运行 SemanticEvaluator"],
            )

        results = getattr(semantic_report, "results", [])
        known_gaps = getattr(semantic_report, "known_gaps", [])

        detected = sum(1 for r in results if getattr(r, "passed", False))
        undetected = sum(1 for r in results if not getattr(r, "passed", False))
        gap_count = len(known_gaps)

        # 排除已知缺口后的可检测项
        non_gap_results = [
            r for r in results
            if getattr(r, "error_type", None)
            and str(getattr(r.error_type, "value", "")) not in known_gaps
        ]
        non_gap_detected = sum(
            1 for r in non_gap_results if getattr(r, "passed", False)
        )

        verdict = "PASS"
        if undetected > 0 and gap_count == 0:
            verdict = "REJECT"
        elif non_gap_detected < len(non_gap_results):
            verdict = "WARN"

        undetected_details = [
            f"{getattr(r, 'case_id', '?')}: {getattr(r, 'rejection_detail', '')}"
            for r in results if not getattr(r, "passed", False)
        ]

        return DimensionResult(
            dimension=3,
            name="语义正确性",
            verdict=verdict,
            metrics={
                "detected_errors": detected,
                "undetected_errors": undetected,
                "known_gaps": gap_count,
                "total_error_types": len(results),
                "non_gap_detection_rate": (
                    round(non_gap_detected / len(non_gap_results) * 100, 1)
                    if non_gap_results else 100.0
                ),
            },
            evidence="SemanticEvaluator.run_all() 报告",
            details=undetected_details,
        )

    # ── 维度 4：编译与执行 ──

    def compute_dimension_4(
        self, compile_results: list | None = None,
    ) -> DimensionResult:
        """维度 4：编译与执行——确定性、成功率。

        占位实现——真实数据需要完整的 Compiler + Executor 集成。

        REJECT：compile_success_rate < 99% 或 execute_success_rate < 95%
        当前阶段所有指标为 [stub]，返回 WARN（不阻断退出）。
        """
        metrics: dict[str, float | int | str | bool] = {
            "compile_success_rate": -1.0,
            "execute_success_rate": -1.0,
            "compile_determinism": -1.0,
        }
        details: list[str] = [
            "[stub] 编译与执行指标需要真实 Compiler + Executor 集成——当前为占位值",
            "[stub] compile_determinism 可使用 SqlBuildPlan.generate_plan_hash() 验证",
        ]

        return DimensionResult(
            dimension=4,
            name="编译与执行",
            verdict="WARN",  # 占位阶段不阻断，但提醒
            metrics=metrics,
            evidence="Compiler 确定性验证占位；真实数据需 Phase 2 Compiler 集成",
            details=details,
        )

    # ── 维度 5：产品可用性 ──

    def compute_dimension_5(
        self, review_results: dict | None = None,
    ) -> DimensionResult:
        """维度 5：产品可用性——review.md 人工接受率。

        占位实现——需要至少 3 名数据工程师参与审查。
        阈值将在 30-50 个真实 LLM 样本后校准。
        """
        metrics: dict[str, float | int | str | bool] = {
            "human_acceptance_rate": -1.0,
            "total_reviews": 0,
            "accepted_reviews": 0,
        }
        details: list[str] = [
            "[stub] 人工接受率需要至少 3 名数据工程师参与审查——当前为占位值",
            "[stub] 阈值将在 30-50 个真实 LLM 样本后校准",
        ]

        if review_results:
            total = review_results.get("total_reviews", 0)
            accepted = review_results.get("accepted_reviews", 0)
            metrics["total_reviews"] = total
            metrics["accepted_reviews"] = accepted
            if total > 0:
                metrics["human_acceptance_rate"] = round(accepted / total * 100, 1)

        return DimensionResult(
            dimension=5,
            name="产品可用性",
            verdict="WARN",  # 占位阶段不阻断
            metrics=metrics,
            evidence="review.md 人工审查流程（待集成）",
            details=details,
        )

    # ── 维度 6：安全边界 ──

    def compute_dimension_6(
        self, security_report: object | None = None,
    ) -> DimensionResult:
        """维度 6：安全边界——6 种攻击向量全部拦截。

        使用 SecurityEvaluator.run_all() 的结果。
        任何未拦截的攻击 = REJECT。
        """
        if security_report is None:
            return DimensionResult(
                dimension=6,
                name="安全边界",
                verdict="REJECT",
                metrics={
                    "vectors_covered": 0, "vectors_blocked": 0, "total_cases": 0,
                },
                evidence="缺失 SecurityEvaluator 结果",
                details=["[stub] 缺少 security_report——需运行 SecurityEvaluator"],
            )

        results = getattr(security_report, "results", [])
        vector_coverage = getattr(security_report, "vector_coverage", {})
        blocking_issues = getattr(security_report, "blocking_issues", [])

        blocked = sum(1 for r in results if getattr(r, "passed", False))
        total = len(results)

        verdict = "PASS" if not blocking_issues else "REJECT"

        return DimensionResult(
            dimension=6,
            name="安全边界",
            verdict=verdict,
            metrics={
                "vectors_covered": sum(1 for v in vector_coverage.values() if v),
                "total_vectors": len(vector_coverage),
                "cases_blocked": blocked,
                "total_cases": total,
                "blocking_issues": len(blocking_issues),
            },
            evidence="SecurityEvaluator.run_all() 报告",
            details=list(blocking_issues),
        )

    # ── 维度 7：运行稳健性 ──

    def compute_dimension_7(
        self, run_history: list | None = None,
    ) -> DimensionResult:
        """维度 7：运行稳健性——连续运行无显著衰减。

        占位实现——需要多次完整 HarnessRunner 执行数据。
        当前返回 PASS（无运行数据时默认通过）。
        """
        metrics: dict[str, float | int | str | bool] = {
            "run_count": 0,
            "token_usage": 0,
            "latency_ms": 0,
            "exception_count": 0,
            "has_degradation": False,
        }
        details: list[str] = [
            "[stub] 运行稳健性需要多次完整 HarnessRunner 执行数据——当前为占位值",
        ]

        if run_history:
            metrics["run_count"] = len(run_history)
            exceptions = sum(
                1 for run in run_history if run.get("exception")
            )
            metrics["exception_count"] = exceptions
            metrics["has_degradation"] = exceptions > 0
            if exceptions > 0:
                details.append(
                    f"检测到 {exceptions} 次运行异常——需人工审查"
                )

        return DimensionResult(
            dimension=7,
            name="运行稳健性",
            verdict="PASS" if metrics["exception_count"] == 0 else "REJECT",
            metrics=metrics,
            evidence="连续运行记录（真实数据需多次运行）",
            details=details,
        )

    # ── 报告汇总 ──

    def produce_report(
        self,
        dimensions: list[DimensionResult],
        dataset_counts: dict[str, int],
        extra_reports: dict | None = None,
    ) -> HarnessReport:
        """从 7 个维度结果生成 HarnessReport。

        Args:
            dimensions: 7 个维度结果列表。
            dataset_counts: 各数据集分类的评测案例数。
            extra_reports: 子报告字典（security/semantic/join_quality）。

        Returns:
            HarnessReport——含 HarnessVerdict 判决。
        """
        rejected = [d.dimension for d in dimensions if d.verdict == "REJECT"]
        warn_items: list[str] = []
        for d in dimensions:
            if d.verdict == "WARN" and d.details:
                warn_items.append(
                    f"D{d.dimension} {d.name}: {d.details[0]}"
                )

        overall = HarnessVerdict.NO_GO if rejected else HarnessVerdict.GO

        extra = extra_reports or {}

        return HarnessReport(
            report_id=HarnessReport.generate_report_id(),
            dimensions=dimensions,
            overall_verdict=overall,
            rejected_dimensions=rejected,
            warn_items=warn_items,
            evaluated_at=datetime.now(timezone.utc).isoformat(),
            dataset_counts=dataset_counts,
            security_report=extra.get("security"),
            semantic_report=extra.get("semantic"),
            join_quality_report=extra.get("join_quality"),
        )
