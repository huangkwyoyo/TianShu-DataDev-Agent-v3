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
    无数据时返回 WARN 并标记 [stub]（诚实报告原则），
    有数据时执行真实阈值判决（REJECT/PASS）。
    D4/D5/D7 已从占位 stub 补全为真实指标计算（Phase 4D 补全）。
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

    # D4 门禁阈值（来自 Phase 4D 规划文档）
    _D4_COMPILE_SUCCESS_MIN: float = 99.0       # 编译成功率最低阈值（%）
    _D4_EXECUTE_SUCCESS_MIN: float = 95.0       # 执行成功率最低阈值（%）
    _D4_DETERMINISM_MIN: float = 100.0          # 编译确定性最低阈值（%）

    def compute_dimension_4(
        self, compile_results: dict | None = None,
    ) -> DimensionResult:
        """维度 4：编译与执行——确定性、成功率。

        测量指标（来自 Compiler + Executor 集成）：
        - compile_success_rate: SqlBuildPlan → CompiledSql 成功率（%）
        - execute_success_rate: CompiledSql → ExecutionTrace 成功率（%）
        - compile_determinism: 同一 SqlBuildPlan 两次编译 hash 一致率（%）

        REJECT 条件：
        - compile_success_rate < 99%
        - execute_success_rate < 95%
        - compile_determinism < 100%

        无 compile_results 时返回 WARN（占位——不阻断退出）。
        """
        # 无数据 → WARN：不能因缺少 Compiler 集成而静默放行
        if compile_results is None:
            return DimensionResult(
                dimension=4,
                name="编译与执行",
                verdict="WARN",
                metrics={
                    "compile_success_rate": -1.0,
                    "execute_success_rate": -1.0,
                    "compile_determinism": -1.0,
                    "total_plans": 0,
                    "compiled_count": 0,
                    "executed_count": 0,
                },
                evidence="Compiler 集成待接入——使用 HarnessRunner.run_compiler_checks() 填充数据",
                details=[
                    "[stub] 编译与执行指标需要真实 Compiler + Executor 集成——当前为占位值",
                    "[stub] 接入 DuckDbSqlCompiler + DuckDBExecutor 后自动进入真实评测模式",
                ],
            )

        # 从 compile_results 提取指标
        total = compile_results.get("total_plans", 0)
        compiled = compile_results.get("compiled_count", 0)
        executed = compile_results.get("executed_count", 0)
        deterministic = compile_results.get("deterministic_count", 0)

        compile_rate = round(compiled / total * 100, 1) if total > 0 else 0.0
        execute_rate = round(executed / compiled * 100, 1) if compiled > 0 else 0.0
        determinism_rate = round(deterministic / total * 100, 1) if total > 0 else 0.0

        metrics: dict[str, float | int | str | bool] = {
            "compile_success_rate": compile_rate,
            "execute_success_rate": execute_rate,
            "compile_determinism": determinism_rate,
            "total_plans": total,
            "compiled_count": compiled,
            "executed_count": executed,
            "deterministic_count": deterministic,
        }

        failures = compile_results.get("failures", [])
        details: list[str] = []

        # 逐条件检查——任一不达标即 REJECT
        reject_reasons: list[str] = []

        if compile_rate < self._D4_COMPILE_SUCCESS_MIN:
            reject_reasons.append(
                f"编译成功率 {compile_rate}% < {self._D4_COMPILE_SUCCESS_MIN}%——"
                f"{total - compiled} 个 plan 编译失败"
            )
        if execute_rate < self._D4_EXECUTE_SUCCESS_MIN:
            reject_reasons.append(
                f"执行成功率 {execute_rate}% < {self._D4_EXECUTE_SUCCESS_MIN}%——"
                f"{compiled - executed} 个 SQL 执行失败"
            )
        if determinism_rate < self._D4_DETERMINISM_MIN:
            reject_reasons.append(
                f"编译确定性 {determinism_rate}% < {self._D4_DETERMINISM_MIN}%——"
                f"{total - deterministic} 个 plan 两次编译 hash 不一致"
            )

        if reject_reasons:
            details.extend(reject_reasons)
            # 附加失败详情（最多 10 条，避免报告过长）
            for f in failures[:10]:
                details.append(
                    f"  {f.get('plan_id', '?')}: {f.get('error', '未知错误')}"
                )
            if len(failures) > 10:
                details.append(f"  ... 及其他 {len(failures) - 10} 条失败记录")

        verdict = "REJECT" if reject_reasons else "PASS"

        return DimensionResult(
            dimension=4,
            name="编译与执行",
            verdict=verdict,
            metrics=metrics,
            evidence=compile_results.get(
                "evidence",
                "Compiler + Executor 集成运行结果",
            ),
            details=details or [
                f"全部 {total} 个 plan 编译/执行通过，确定性 100%——D4 门禁 PASS"
            ],
        )

    # ── 维度 5：产品可用性 ──

    # D5 门禁阈值（来自 Phase 4D 规划文档——需 30-50 个真实样本后校准）
    _D5_ACCEPTANCE_MIN: float = 70.0            # 人工接受率最低阈值（%）
    _D5_MIN_REVIEWERS: int = 3                  # 最少审查人数

    def compute_dimension_5(
        self, review_results: dict | None = None,
    ) -> DimensionResult:
        """维度 5：产品可用性——review.md 人工接受率。

        测量指标：
        - human_acceptance_rate: 人工审查接受率（%）
        - total_reviews / accepted_reviews: 审查总数与接受数
        - reviewer_count: 参与审查的人数

        REJECT 条件：
        - human_acceptance_rate < 70%（阈值待 30-50 样本后校准）
        - reviewer_count < 3（审查人数不足——样本偏差风险）

        无 review_results 或 total_reviews == 0 时返回 WARN（占位——不阻断退出）。
        """
        # 无数据 → WARN：不能因缺少人工审查而静默放行
        if review_results is None:
            return DimensionResult(
                dimension=5,
                name="产品可用性",
                verdict="WARN",
                metrics={
                    "human_acceptance_rate": -1.0,
                    "total_reviews": 0,
                    "accepted_reviews": 0,
                    "reviewer_count": 0,
                },
                evidence="人工审查流程待集成——需至少 3 名数据工程师参与",
                details=[
                    "[stub] 人工接受率需要至少 3 名数据工程师参与审查——当前为占位值",
                    "[stub] 阈值将在 30-50 个真实 LLM 样本后校准",
                ],
            )

        total = review_results.get("total_reviews", 0)
        accepted = review_results.get("accepted_reviews", 0)
        reviewer_count = review_results.get("reviewer_count", 0)

        # 无有效审查数据 → WARN
        if total == 0:
            return DimensionResult(
                dimension=5,
                name="产品可用性",
                verdict="WARN",
                metrics={
                    "human_acceptance_rate": -1.0,
                    "total_reviews": 0,
                    "accepted_reviews": 0,
                    "reviewer_count": reviewer_count,
                },
                evidence="审查数据为空——total_reviews == 0",
                details=[
                    "[stub] 审查数据为空——需填充真实人工审查结果",
                ],
            )

        acceptance_rate = round(accepted / total * 100, 1)
        metrics: dict[str, float | int | str | bool] = {
            "human_acceptance_rate": acceptance_rate,
            "total_reviews": total,
            "accepted_reviews": accepted,
            "reviewer_count": reviewer_count,
        }

        details: list[str] = []
        reject_reasons: list[str] = []

        # 条件 1：审查人数不足 → 样本偏差风险（REJECT）
        if reviewer_count < self._D5_MIN_REVIEWERS:
            reject_reasons.append(
                f"审查人数 {reviewer_count} < {self._D5_MIN_REVIEWERS}——"
                f"样本偏差风险，审查结果不可靠"
            )

        # 条件 2：接受率低于阈值 → REJECT
        if acceptance_rate < self._D5_ACCEPTANCE_MIN:
            reject_reasons.append(
                f"人工接受率 {acceptance_rate}% < {self._D5_ACCEPTANCE_MIN}%——"
                f"{total - accepted}/{total} 个审查被拒绝"
            )

        if reject_reasons:
            details.extend(reject_reasons)
            # 附加拒绝明细（如有）
            rejected_items = review_results.get("rejected_items", [])
            for item in rejected_items[:10]:
                details.append(
                    f"  {item.get('case_id', '?')}: {item.get('reason', '未说明')}"
                )

        verdict = "REJECT" if reject_reasons else "PASS"

        return DimensionResult(
            dimension=5,
            name="产品可用性",
            verdict=verdict,
            metrics=metrics,
            evidence=review_results.get(
                "evidence",
                "review.md 人工审查流程",
            ),
            details=details or [
                f"人工接受率 {acceptance_rate}%（{accepted}/{total}），"
                f"{reviewer_count} 人参与审查——D5 门禁 PASS"
            ],
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

    # D7 门禁阈值
    _D7_TOKEN_DRIFT_MAX: float = 50.0           # token 消耗漂移上限（%——相对首次运行）
    _D7_LATENCY_DRIFT_MAX: float = 100.0        # 延迟漂移上限（%——相对首次运行，即 2x）
    _D7_MIN_RUNS_FOR_TREND: int = 3             # 趋势分析最少运行次数

    def compute_dimension_7(
        self, run_history: list | None = None,
    ) -> DimensionResult:
        """维度 7：运行稳健性——连续运行无显著衰减。

        测量指标（来自多次完整 HarnessRunner 执行）：
        - run_count: 运行次数
        - token_usage 趋势：token 消耗是否持续增长（>50% 漂移 = 异常）
        - latency 趋势：延迟是否持续增长（>2x 漂移 = 异常）
        - exception_count: 异常运行次数
        - has_degradation: 是否存在显著退化

        REJECT 条件：
        - exception_count > 0（任何异常运行）
        - token 漂移 > 50%（相对首次运行的平均值）
        - 延迟漂移 > 100%（相对首次运行的平均值，即 2x）

        无 run_history 时返回 WARN（占位——不阻断退出）。
        """
        # 无数据 → WARN：不能因缺少运行历史而静默放行
        if run_history is None or len(run_history) == 0:
            return DimensionResult(
                dimension=7,
                name="运行稳健性",
                verdict="WARN",
                metrics={
                    "run_count": 0,
                    "avg_token_usage": -1,
                    "avg_latency_ms": -1,
                    "token_drift_pct": 0.0,
                    "latency_drift_pct": 0.0,
                    "exception_count": 0,
                    "has_degradation": False,
                },
                evidence="运行历史待收集——需多次完整 HarnessRunner 执行数据",
                details=[
                    "[stub] 运行稳健性需要多次完整 HarnessRunner 执行数据——当前为占位值",
                ],
            )

        run_count = len(run_history)
        exceptions = sum(
            1 for run in run_history if run.get("exception")
        )

        # 提取每轮的 token 和延迟数据
        tokens = [
            run.get("token_usage", 0)
            for run in run_history
            if not run.get("exception")  # 排除异常运行
        ]
        latencies = [
            run.get("latency_ms", 0)
            for run in run_history
            if not run.get("exception")
        ]

        avg_tokens = round(sum(tokens) / len(tokens)) if tokens else 0
        avg_latency = round(sum(latencies) / len(latencies)) if latencies else 0

        details: list[str] = []
        reject_reasons: list[str] = []

        # 条件 1：异常运行 → REJECT
        if exceptions > 0:
            reject_reasons.append(
                f"检测到 {exceptions}/{run_count} 次运行异常——"
                f"异常率 {round(exceptions / run_count * 100, 1)}%"
            )
            # 列出异常详情
            for i, run in enumerate(run_history):
                if run.get("exception"):
                    details.append(
                        f"  运行 #{i + 1}: {run.get('exception')}"
                    )

        # 条件 2：token 漂移检测（需至少 _D7_MIN_RUNS_FOR_TREND 次运行）
        token_drift_pct = 0.0
        if len(tokens) >= self._D7_MIN_RUNS_FOR_TREND and tokens[0] > 0:
            # 比较后半段与前半段的平均值——检测持续增长趋势
            mid = len(tokens) // 2
            first_half_avg = sum(tokens[:mid]) / mid
            second_half_avg = sum(tokens[mid:]) / (len(tokens) - mid)
            if first_half_avg > 0:
                token_drift_pct = round(
                    (second_half_avg - first_half_avg) / first_half_avg * 100, 1
                )
            if token_drift_pct > self._D7_TOKEN_DRIFT_MAX:
                reject_reasons.append(
                    f"Token 消耗漂移 {token_drift_pct}% > {self._D7_TOKEN_DRIFT_MAX}%——"
                    f"前半段均值 {round(first_half_avg)} → 后半段均值 {round(second_half_avg)}"
                )

        # 条件 3：延迟漂移检测
        latency_drift_pct = 0.0
        if len(latencies) >= self._D7_MIN_RUNS_FOR_TREND and latencies[0] > 0:
            mid = len(latencies) // 2
            first_half_avg = sum(latencies[:mid]) / mid
            second_half_avg = sum(latencies[mid:]) / (len(latencies) - mid)
            if first_half_avg > 0:
                latency_drift_pct = round(
                    (second_half_avg - first_half_avg) / first_half_avg * 100, 1
                )
            if latency_drift_pct > self._D7_LATENCY_DRIFT_MAX:
                reject_reasons.append(
                    f"延迟漂移 {latency_drift_pct}% > {self._D7_LATENCY_DRIFT_MAX}%——"
                    f"前半段均值 {round(first_half_avg)}ms → 后半段均值 {round(second_half_avg)}ms"
                )

        has_degradation = len(reject_reasons) > 0

        if not reject_reasons and run_count > 0:
            details.append(
                f"{run_count} 次运行均无异常——"
                f"平均 token {avg_tokens}，平均延迟 {avg_latency}ms"
            )
            if len(tokens) >= self._D7_MIN_RUNS_FOR_TREND:
                details.append(
                    f"Token 漂移 {token_drift_pct}%（阈值 {self._D7_TOKEN_DRIFT_MAX}%），"
                    f"延迟漂移 {latency_drift_pct}%（阈值 {self._D7_LATENCY_DRIFT_MAX}%）"
                )

        verdict = "REJECT" if reject_reasons else "PASS"

        return DimensionResult(
            dimension=7,
            name="运行稳健性",
            verdict=verdict,
            metrics={
                "run_count": run_count,
                "avg_token_usage": avg_tokens,
                "avg_latency_ms": avg_latency,
                "token_drift_pct": token_drift_pct,
                "latency_drift_pct": latency_drift_pct,
                "exception_count": exceptions,
                "has_degradation": has_degradation,
            },
            evidence=f"连续 {run_count} 次运行记录",
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
