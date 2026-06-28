"""PerfValidator——Phase 4B 性能契约验证器。

15 条 PERF 规则——REJECT（硬门禁）/ WARN（软规则）/ PERF_FEEDBACK（执行反馈）三分流。
REJECT 违反后阻断编译，WARN 违反后记录但不阻断，PERF_FEEDBACK 进入 artifact。

规则来源：docs/roadmap/phase-4b-perf-and-compiler-pass.md § 15 条 PERF 规则。
扩展自 Phase 1C 的 8 条规则。
"""

from __future__ import annotations

from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    JoinStep,
    JoinType,
    LimitStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    WindowStep,
)

from .models import (
    PerfCheckResult,
    PerfRule,
    PerfSeverity,
    PerfValidationResult,
    types_are_compatible,
)

# ════════════════════════════════════════════
# 阈值常量
# ════════════════════════════════════════════

# 大事实表行数阈值——超过此行数视为"大表"
_LARGE_FACT_ROW_THRESHOLD = 1_000_000  # 100 万行
# 明细查询行数阈值——超过此行数的无聚合查询视为明细查询
_DETAIL_ROW_THRESHOLD = 100_000  # 10 万行
# 复杂 SQL 步骤数阈值——超过此步骤数建议拆分中间表
_COMPLEX_STEP_THRESHOLD = 8
# 慢 SQL 阈值（毫秒）——超过此时长的执行视为慢查询
_SLOW_QUERY_MS_THRESHOLD = 5_000  # 5 秒


# ════════════════════════════════════════════
# PERF 规则注册表（15 条）
# ════════════════════════════════════════════


def _build_perf_rules() -> list[PerfRule]:
    """构建 15 条 PERF 规则的注册表。

    规则按 check_category 分组：
    - column_selection：列选择（PERF-001）
    - filtering：过滤条件（PERF-002, PERF-003）
    - table_selection：表选择（PERF-004）
    - join：Join 相关（PERF-005, PERF-006, PERF-007, PERF-010）
    - aggregation：聚合相关（PERF-009）
    - sorting：排序相关（PERF-011）
    - window：窗口函数（PERF-012）
    - optimization：优化建议（PERF-013, PERF-014）
    - execution：执行反馈（PERF-015）
    """
    return [
        # ── 列选择 ──
        PerfRule(
            rule_id="PERF-001",
            description="禁止 SELECT *——必须显式声明业务需要字段",
            severity=PerfSeverity.REJECT,
            condition="ScanStep.required_columns 为空或未显式声明",
            check_category="column_selection",
        ),
        # ── 过滤条件 ──
        PerfRule(
            rule_id="PERF-002",
            description="查询大事实表时必须添加时间范围过滤",
            severity=PerfSeverity.REJECT,
            condition="大事实表（>1M 行）的 ScanStep 无时间过滤 predicate 且无 partition_filters",
            check_category="filtering",
        ),
        PerfRule(
            rule_id="PERF-003",
            description="时间过滤使用 >= start AND < end——禁止 WHERE 左侧套函数",
            severity=PerfSeverity.REJECT,
            condition="时间字段在谓词左侧被函数包裹（如 DATE(ts) >= '...'）",
            check_category="filtering",
        ),
        # ── 表选择 ──
        PerfRule(
            rule_id="PERF-004",
            description="优先使用已给出的汇总表/DWS 表——不能默认扫描 fact 明细",
            severity=PerfSeverity.WARN,
            condition="存在汇总表/DWS 但计划扫描了明细 fact 表",
            check_category="table_selection",
        ),
        # ── Join 相关 ──
        PerfRule(
            rule_id="PERF-005",
            description="大表 Join 大表前必须先过滤、再按业务粒度聚合",
            severity=PerfSeverity.WARN,
            condition="两个 >1M 行表 Join，但 Join 前无 FilterStep 或预聚合",
            check_category="join",
        ),
        PerfRule(
            rule_id="PERF-006",
            description="Join key 类型必须一致——禁止 Join 条件临时 CAST",
            severity=PerfSeverity.REJECT,
            condition="JoinStep.join_keys 双方字段类型不在同一兼容组",
            check_category="join",
        ),
        PerfRule(
            rule_id="PERF-007",
            description="Join key 必须有业务含义证据——维表 Join 前检查唯一性",
            severity=PerfSeverity.WARN,
            condition="Join key 缺乏业务含义证据（无 relationship_ref 或 cardinality_hint）",
            check_category="join",
        ),
        PerfRule(
            rule_id="PERF-008",
            description="明细查询必须带 LIMIT——离线生成结果表除外",
            severity=PerfSeverity.REJECT,
            condition="无聚合步骤且无 LimitStep 的查询，且估算行数 > 10 万",
            check_category="output",
        ),
        # ── 聚合相关 ──
        PerfRule(
            rule_id="PERF-009",
            description="禁止无理由 DISTINCT *——DISTINCT 必须指定具体列",
            severity=PerfSeverity.REJECT,
            condition="AggregateStep 使用了 COUNT_DISTINCT 且无 group_keys（等效 DISTINCT *）",
            check_category="aggregation",
        ),
        # ── Join 类型 ──
        PerfRule(
            rule_id="PERF-010",
            description="禁止无理由 CROSS JOIN——Join 必须有明确 Join 条件",
            severity=PerfSeverity.REJECT,
            condition="JoinStep.join_type=CROSS 或 join_keys 为空且非 CROSS",
            check_category="join",
        ),
        # ── 排序相关 ──
        PerfRule(
            rule_id="PERF-011",
            description="ORDER BY 只允许最终展示层或窗口必要位置",
            severity=PerfSeverity.WARN,
            condition="SortStep 出现在非最终位置（非最后一步且后无 LimitStep）",
            check_category="sorting",
        ),
        # ── 窗口函数 ──
        PerfRule(
            rule_id="PERF-012",
            description="窗口函数前必须尽可能缩小数据范围",
            severity=PerfSeverity.WARN,
            condition="WindowStep 前无聚合或过滤缩小数据量",
            check_category="window",
        ),
        # ── 优化建议 ──
        PerfRule(
            rule_id="PERF-013",
            description="高频指标建议沉淀为汇总表——避免重复计算",
            severity=PerfSeverity.WARN,
            condition="聚合指标重复出现在多个 SqlBuildPlan 中",
            check_category="optimization",
        ),
        PerfRule(
            rule_id="PERF-014",
            description="复杂 SQL 允许拆分 _temp 中间表验证中间行数",
            severity=PerfSeverity.WARN,
            condition="SqlBuildPlan 步骤数 > 8 且未使用 _temp 中间表",
            check_category="optimization",
        ),
        # ── 执行反馈 ──
        PerfRule(
            rule_id="PERF-015",
            description="慢 SQL 必须基于真实执行计划优化",
            severity=PerfSeverity.PERF_FEEDBACK,
            condition="SQL 执行耗时 > 5s 且无 EXPLAIN 反馈记录",
            check_category="execution",
        ),
    ]


# ════════════════════════════════════════════
# PerfValidator
# ════════════════════════════════════════════


class PerfValidator:
    """Phase 4B 性能契约验证器——15 条规则 + 三分流。

    REJECT → 阻断编译（硬门禁）
    WARN → 记录到结果但不阻断（软规则）
    PERF_FEEDBACK → 进入 artifact（执行计划反馈）

    Validator 是确定性的——相同输入 → 相同输出。
    LLM 不做性能决策——所有规则由代码确定性地执行。
    """

    def __init__(self):
        """初始化 PerfValidator，加载 15 条规则注册表。"""
        self._rules = _build_perf_rules()

    @property
    def rules(self) -> list[PerfRule]:
        """返回已注册的 PERF 规则列表（只读）。"""
        return list(self._rules)

    def validate(
        self,
        plan: SqlBuildPlan,
        partitioned_tables: set[str] | None = None,
        fact_tables: set[str] | None = None,
        summary_tables: set[str] | None = None,
        column_types: dict[str, dict[str, str]] | None = None,
        execution_stats: dict | None = None,
    ) -> PerfValidationResult:
        """对 SqlBuildPlan 执行全部 15 条 PERF 规则。

        Args:
            plan: 待验证的 SqlBuildPlan
            partitioned_tables: 已知分区表的 table_ref 集合
            fact_tables: 已知大事实表的 table_ref 集合
            summary_tables: 已知汇总表/DWS 表的 table_ref 集合
            column_types: {table_ref: {column_name: data_type}} 列类型信息
            execution_stats: 执行统计（execution_time_ms, row_count 等）——PERF-015 用

        Returns:
            PerfValidationResult——聚合全部 15 条规则的检查结果
        """
        check_results: list[PerfCheckResult] = []

        # ── column_selection ──
        check_results.append(self._check_perf001(plan))

        # ── filtering ──
        check_results.append(self._check_perf002(plan, fact_tables, partitioned_tables))
        check_results.append(self._check_perf003(plan))

        # ── table_selection ──
        check_results.append(self._check_perf004(plan, summary_tables))

        # ── join ──
        check_results.append(self._check_perf005(plan, fact_tables))
        check_results.append(self._check_perf006(plan, column_types))
        check_results.append(self._check_perf007(plan))
        check_results.append(self._check_perf010(plan))

        # ── output ──
        check_results.append(self._check_perf008(plan))

        # ── aggregation ──
        check_results.append(self._check_perf009(plan))

        # ── sorting ──
        check_results.append(self._check_perf011(plan))

        # ── window ──
        check_results.append(self._check_perf012(plan))

        # ── optimization ──
        check_results.append(self._check_perf013(plan))
        check_results.append(self._check_perf014(plan))

        # ── execution ──
        check_results.append(self._check_perf015(plan, execution_stats))

        # 分类汇总
        reject_violations = [
            r for r in check_results
            if not r.passed and r.severity == PerfSeverity.REJECT
        ]
        warnings = [
            r for r in check_results
            if not r.passed and r.severity == PerfSeverity.WARN
        ]
        feedbacks = [
            r for r in check_results
            if not r.passed and r.severity == PerfSeverity.PERF_FEEDBACK
        ]
        all_reject_passed = len(reject_violations) == 0

        return PerfValidationResult(
            plan_id=plan.plan_id,
            all_reject_passed=all_reject_passed,
            check_results=check_results,
            reject_violations=reject_violations,
            warnings=warnings,
            feedbacks=feedbacks,
        )

    # ════════════════════════════════════════
    # 规则实现
    # ════════════════════════════════════════

    def _check_perf001(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-001: 禁止 SELECT *——必须显式声明业务需要字段 → REJECT。

        检查：所有 ScanStep 的 required_columns 不得为空。
        空 required_columns 等效于 SELECT *，被硬门禁拦截。
        """
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]

        for scan in scan_steps:
            if not scan.required_columns:
                return PerfCheckResult(
                    rule_id="PERF-001",
                    passed=False,
                    severity=PerfSeverity.REJECT,
                    message=(
                        f"表 '{scan.table_ref}' 的 required_columns 为空——"
                        f"等效于 SELECT *，必须显式声明所需列"
                    ),
                    flagged_items=[f"table={scan.table_ref}"],
                )

        # 检查是否有 ScanStep 声明了全表列（required_columns 覆盖全部 source 列）
        # 此处保守处理——只要 required_columns 非空即视为显式声明

        return PerfCheckResult(
            rule_id="PERF-001",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="所有 ScanStep 均显式声明了 required_columns",
        )

    def _check_perf002(
        self,
        plan: SqlBuildPlan,
        fact_tables: set[str] | None = None,
        partitioned_tables: set[str] | None = None,
    ) -> PerfCheckResult:
        """PERF-002: 大事实表必须添加时间范围过滤 → REJECT。

        检查：对于 fact_tables 中注册的大事实表，
        ScanStep 必须有 partition_filters 或时间相关的 predicates。

        若 fact_tables 为 None，使用 estimated_row_count > _LARGE_FACT_ROW_THRESHOLD 推断。
        """
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        time_keywords = {"dt", "date", "ds", "hour", "day", "month",
                         "stat_date", "dt_date", "timestamp", "time", "ts"}

        for scan in scan_steps:
            # 判断是否为大事实表
            is_fact = False
            if fact_tables is not None:
                is_fact = scan.table_ref in fact_tables
            elif scan.estimated_row_count is not None:
                is_fact = scan.estimated_row_count > _LARGE_FACT_ROW_THRESHOLD

            if not is_fact:
                continue

            # 检查是否有分区过滤
            if scan.partition_filters:
                continue  # 有显式分区过滤 → OK

            # 检查 predicates 中是否包含时间相关过滤——仅限属于当前扫描表的列
            has_time_filter = False
            scan_table = str(scan.table_ref)
            for pred in scan.predicates:
                if _predicate_contains_keywords(pred, time_keywords, scan_table):
                    has_time_filter = True
                    break

            # 也检查 FilterStep——同样仅限引用当前扫描表的过滤条件
            if not has_time_filter:
                filter_steps = [s for s in plan.steps
                                if hasattr(s, "step_type") and s.step_type == "filter"]
                for fstep in filter_steps:
                    if hasattr(fstep, "predicate"):
                        if _predicate_contains_keywords(
                            fstep.predicate, time_keywords, scan_table,
                        ):
                            has_time_filter = True
                            break

            if not has_time_filter:
                return PerfCheckResult(
                    rule_id="PERF-002",
                    passed=False,
                    severity=PerfSeverity.REJECT,
                    message=(
                        f"大事实表 '{scan.table_ref}'（估算 {scan.estimated_row_count or '?'} 行）"
                        f"缺少时间范围过滤——必须添加时间过滤条件避免全表扫描"
                    ),
                    flagged_items=[f"table={scan.table_ref}"],
                )

        return PerfCheckResult(
            rule_id="PERF-002",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="所有大事实表均包含时间过滤",
        )

    def _check_perf003(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-003: 时间过滤使用 >= AND < ——禁止 WHERE 左侧套函数 → REJECT。

        检查：时间字段在谓词左侧时，不得被函数包裹（如 DATE(ts) >= '...' 应改为 ts >= '...' AND ts < '...'）。
        此检查覆盖 Predicate 树中的所有时间相关比较。
        """
        # 收集所有 Predicate（包括 ScanStep.predicates 和 FilterStep.predicate）
        all_predicates = []
        for step in plan.steps:
            if isinstance(step, ScanStep):
                all_predicates.extend(step.predicates)
            if hasattr(step, "step_type") and step.step_type == "filter":
                if hasattr(step, "predicate"):
                    all_predicates.append(step.predicate)

        time_functions = {"date", "datediff", "date_trunc", "date_part",
                          "year", "month", "day", "hour", "minute", "second",
                          "extract", "to_date", "to_timestamp", "strftime"}

        for pred in all_predicates:
            violations = _find_function_on_left(pred, time_functions)
            if violations:
                return PerfCheckResult(
                    rule_id="PERF-003",
                    passed=False,
                    severity=PerfSeverity.REJECT,
                    message=(
                        f"时间字段被函数包裹——{violations[0]}。"
                        f"应使用 >= start AND < end 的半开区间模式，"
                        f"避免在 WHERE 左侧对字段套函数（会阻止索引/分区裁剪）"
                    ),
                    flagged_items=violations,
                )

        return PerfCheckResult(
            rule_id="PERF-003",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="时间过滤未在左侧使用函数包裹",
        )

    def _check_perf004(
        self,
        plan: SqlBuildPlan,
        summary_tables: set[str] | None = None,
    ) -> PerfCheckResult:
        """PERF-004: 优先使用汇总表/DWS 表——不能默认扫描 fact 明细 → WARN。

        当 summary_tables 提供了可用的汇总表，但计划仍扫描了明细表时发出告警。
        若 summary_tables 为 None 则跳过此检查。
        """
        if summary_tables is None or not summary_tables:
            return PerfCheckResult(
                rule_id="PERF-004",
                passed=True,
                severity=PerfSeverity.WARN,
                message="PERF-004 跳过——未提供汇总表信息",
            )

        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        fact_scans = [
            s for s in scan_steps
            if s.table_ref not in summary_tables
            and s.estimated_row_count is not None
            and s.estimated_row_count > _LARGE_FACT_ROW_THRESHOLD
        ]

        if fact_scans:
            flagged = [f"table={s.table_ref}" for s in fact_scans]
            return PerfCheckResult(
                rule_id="PERF-004",
                passed=False,
                severity=PerfSeverity.WARN,
                message=(
                    f"扫描了 {len(fact_scans)} 个大明细表但存在可用汇总表——"
                    f"建议评估是否可用汇总表替代明细扫描以提升性能"
                ),
                flagged_items=flagged,
            )

        return PerfCheckResult(
            rule_id="PERF-004",
            passed=True,
            severity=PerfSeverity.WARN,
            message="已使用汇总表或无可用的汇总表替代方案",
        )

    def _check_perf005(
        self,
        plan: SqlBuildPlan,
        fact_tables: set[str] | None = None,
    ) -> PerfCheckResult:
        """PERF-005: 大表 Join 大表前必须先过滤、再按业务粒度聚合 → WARN。

        检查：两个大表（>1M 行）Join 时，Join 前是否有 FilterStep 或预聚合设置。
        """
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]
        if not join_steps:
            return PerfCheckResult(
                rule_id="PERF-005",
                passed=True,
                severity=PerfSeverity.WARN,
                message="无 Join 步骤，PERF-005 不适用",
            )

        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        scan_rows = {s.table_ref: s.estimated_row_count or 0 for s in scan_steps}

        # 判断是否为大表（通过 fact_tables 或估算行数）
        def _is_large_table(table_ref: str) -> bool:
            if fact_tables is not None:
                return table_ref in fact_tables
            return scan_rows.get(table_ref, 0) > _LARGE_FACT_ROW_THRESHOLD

        for join_step in join_steps:
            # 检查 Join 双方是否都是大表
            right_ref = join_step.right_table_ref
            # 左表从 ScanStep 中推断——此处简化处理：检查是否有两个大 ScanStep
            large_scans = [s for s in scan_steps if _is_large_table(s.table_ref)]
            if len(large_scans) < 2:
                continue

            if not join_step.pre_aggregation_allowed:
                return PerfCheckResult(
                    rule_id="PERF-005",
                    passed=False,
                    severity=PerfSeverity.WARN,
                    message=(
                        "大表 Join 前未启用预聚合（pre_aggregation_allowed=False）——"
                        "建议在 Join 前先对事实表按业务粒度聚合以减少 Join 数据量"
                    ),
                    flagged_items=[
                        f"join_right={right_ref}",
                        f"large_tables={[s.table_ref for s in large_scans]}",
                    ],
                )

        return PerfCheckResult(
            rule_id="PERF-005",
            passed=True,
            severity=PerfSeverity.WARN,
            message="大表 Join 预聚合设置合理或无可优化场景",
        )

    def _check_perf006(
        self,
        plan: SqlBuildPlan,
        column_types: dict[str, dict[str, str]] | None = None,
    ) -> PerfCheckResult:
        """PERF-006: Join key 类型必须一致——禁止临时 CAST → REJECT。

        检查：JoinStep.join_keys 双方字段的数据类型必须在同一兼容组。
        若 column_types 为 None，使用启发式检查（列名后缀推断）。

        注意：此规则与 SqlBuildPlanValidator 的 Join key 类型校验互补——
        Validator 做事实源校验（类型必须来自 SourceManifest），
        PerfValidator 做性能校验（类型不兼容会导致隐式 CAST 降低性能）。
        """
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]

        for join_step in join_steps:
            for left_key, right_key in join_step.join_keys:
                left_type = None
                right_type = None

                if column_types is not None:
                    left_table_types = column_types.get(left_key.table_ref, {})
                    right_table_types = column_types.get(right_key.table_ref, {})
                    left_type = left_table_types.get(
                        left_key.normalized_name or left_key.column_name
                    )
                    right_type = right_table_types.get(
                        right_key.normalized_name or right_key.column_name
                    )

                if left_type and right_type:
                    if not types_are_compatible(left_type, right_type):
                        return PerfCheckResult(
                            rule_id="PERF-006",
                            passed=False,
                            severity=PerfSeverity.REJECT,
                            message=(
                                f"Join key 类型不兼容："
                                f"'{left_key.table_ref}.{left_key.column_name}' "
                                f"({left_type}) ↔ "
                                f"'{right_key.table_ref}.{right_key.column_name}' "
                                f"({right_type})——禁止临时 CAST，应统一 Join key 类型"
                            ),
                            flagged_items=[
                                f"left={left_key.table_ref}.{left_key.column_name}:{left_type}",
                                f"right={right_key.table_ref}.{right_key.column_name}:{right_type}",
                            ],
                        )
                else:
                    # 无类型信息时使用启发式检查
                    left_name = left_key.column_name.lower()
                    right_name = right_key.column_name.lower()
                    id_hints = ("id", "key", "code", "num", "no", "sk")
                    text_hints = ("name", "text", "desc", "type", "status")

                    left_is_id = any(left_name.endswith(h) or left_name == h for h in id_hints)
                    right_is_text = any(right_name.endswith(h) or right_name == h for h in text_hints)
                    right_is_id = any(right_name.endswith(h) or right_name == h for h in id_hints)
                    left_is_text = any(left_name.endswith(h) or left_name == h for h in text_hints)

                    if (left_is_id and right_is_text) or (right_is_id and left_is_text):
                        return PerfCheckResult(
                            rule_id="PERF-006",
                            passed=False,
                            severity=PerfSeverity.REJECT,
                            message=(
                                f"Join key 可能类型不兼容（启发式推断）："
                                f"'{left_key.column_name}'（疑似数值型）↔ "
                                f"'{right_key.column_name}'（疑似字符型）——"
                                f"禁止临时 CAST，应在 SourceManifest 中统一类型"
                            ),
                            flagged_items=[
                                f"left={left_key.table_ref}.{left_key.column_name}",
                                f"right={right_key.table_ref}.{right_key.column_name}",
                            ],
                        )

        return PerfCheckResult(
            rule_id="PERF-006",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="所有 Join key 类型兼容",
        )

    def _check_perf007(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-007: Join key 必须有业务含义证据——维表 Join 前检查唯一性 → WARN。

        检查：JoinStep 是否缺少 relationship_ref 或 cardinality_hint——
        无证据的 Join 可能产生重复数据或性能问题。
        """
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]

        for join_step in join_steps:
            if not join_step.relationship_ref:
                return PerfCheckResult(
                    rule_id="PERF-007",
                    passed=False,
                    severity=PerfSeverity.WARN,
                    message=(
                        f"JoinStep '{join_step.step_id}' 缺少 relationship_ref——"
                        f"Join key 无业务含义证据，建议在 RelationshipHypothesis 中补充证据"
                    ),
                    flagged_items=[
                        f"step={join_step.step_id}",
                        f"right_table={join_step.right_table_ref}",
                    ],
                )
            if join_step.cardinality_hint is None:
                return PerfCheckResult(
                    rule_id="PERF-007",
                    passed=False,
                    severity=PerfSeverity.WARN,
                    message=(
                        f"JoinStep '{join_step.step_id}' 缺少 cardinality_hint——"
                        f"建议在 Join 前检查维表唯一性以避免重复"
                    ),
                    flagged_items=[
                        f"step={join_step.step_id}",
                        f"right_table={join_step.right_table_ref}",
                    ],
                )

        return PerfCheckResult(
            rule_id="PERF-007",
            passed=True,
            severity=PerfSeverity.WARN,
            message="所有 Join key 均有业务含义证据",
        )

    def _check_perf008(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-008: 明细查询必须带 LIMIT——离线生成结果表除外 → REJECT。

        检查：无聚合步骤（AggregateStep）且无 LimitStep 的查询，
        且估算行数 > _DETAIL_ROW_THRESHOLD 时触发。
        """
        has_aggregate = any(isinstance(s, AggregateStep) for s in plan.steps)
        if has_aggregate:
            return PerfCheckResult(
                rule_id="PERF-008",
                passed=True,
                severity=PerfSeverity.REJECT,
                message="查询包含聚合步骤，PERF-008 不适用",
            )

        has_limit = any(isinstance(s, LimitStep) for s in plan.steps)
        if has_limit:
            return PerfCheckResult(
                rule_id="PERF-008",
                passed=True,
                severity=PerfSeverity.REJECT,
                message="查询已包含 LIMIT",
            )

        # 估算总行数
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        max_rows = max(
            (s.estimated_row_count or 0 for s in scan_steps),
            default=0,
        )

        if max_rows > _DETAIL_ROW_THRESHOLD:
            return PerfCheckResult(
                rule_id="PERF-008",
                passed=False,
                severity=PerfSeverity.REJECT,
                message=(
                    f"明细查询（无聚合）缺少 LIMIT——估算行数 {max_rows:,} > "
                    f"{_DETAIL_ROW_THRESHOLD:,}，必须添加 LIMIT 限制返回行数。"
                    f"离线生成结果表时可通过参数豁免此规则"
                ),
                flagged_items=[f"estimated_rows={max_rows}"],
            )

        return PerfCheckResult(
            rule_id="PERF-008",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="明细查询行数可控或已包含 LIMIT",
        )

    def _check_perf009(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-009: 禁止无理由 DISTINCT * → REJECT。

        检查：AggregateStep 使用了 COUNT_DISTINCT 但无 group_keys——
        这等效于 DISTINCT * 或对整个表去重，通常不合理。
        """
        agg_steps = [s for s in plan.steps if isinstance(s, AggregateStep)]

        for agg in agg_steps:
            for metric in agg.metrics:
                if metric.aggregation and metric.aggregation.value == "COUNT_DISTINCT":
                    if not agg.group_keys:
                        return PerfCheckResult(
                            rule_id="PERF-009",
                            passed=False,
                            severity=PerfSeverity.REJECT,
                            message=(
                                f"AggregateStep '{agg.step_id}' 使用 COUNT_DISTINCT "
                                f"但无 group_keys——等效于 DISTINCT *，"
                                f"必须指定具体去重列或添加分组键"
                            ),
                            flagged_items=[
                                f"step={agg.step_id}",
                                f"metric={metric.alias}",
                            ],
                        )

        return PerfCheckResult(
            rule_id="PERF-009",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="无 DISTINCT * 模式",
        )

    def _check_perf010(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-010: 禁止无理由 CROSS JOIN → REJECT。

        检查：JoinStep.join_type=CROSS 或 join_keys 为空时触发。
        CROSS JOIN 产生笛卡尔积，通常为开发错误。
        """
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]

        for join_step in join_steps:
            if join_step.join_type == JoinType.CROSS:
                return PerfCheckResult(
                    rule_id="PERF-010",
                    passed=False,
                    severity=PerfSeverity.REJECT,
                    message=(
                        f"JoinStep '{join_step.step_id}' 使用了 CROSS JOIN——"
                        f"笛卡尔积会产生极大中间结果集，必须提供明确的 Join 条件"
                    ),
                    flagged_items=[f"step={join_step.step_id}"],
                )
            if not join_step.join_keys:
                return PerfCheckResult(
                    rule_id="PERF-010",
                    passed=False,
                    severity=PerfSeverity.REJECT,
                    message=(
                        f"JoinStep '{join_step.step_id}' 缺少 join_keys——"
                        f"Join 必须提供明确的 Join 键，无 Join 键等效于 CROSS JOIN"
                    ),
                    flagged_items=[f"step={join_step.step_id}"],
                )

        return PerfCheckResult(
            rule_id="PERF-010",
            passed=True,
            severity=PerfSeverity.REJECT,
            message="无 CROSS JOIN 或 Join 键缺失",
        )

    def _check_perf011(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-011: ORDER BY 只允许最终展示层或窗口必要位置 → WARN。

        检查：SortStep 是否位于非最终位置（不是最后一步或倒数第二步），
        且后续无 LimitStep。中间排序通常无用且浪费资源。
        """
        sort_steps = [
            (i, s) for i, s in enumerate(plan.steps)
            if isinstance(s, SortStep)
        ]
        if not sort_steps:
            return PerfCheckResult(
                rule_id="PERF-011",
                passed=True,
                severity=PerfSeverity.WARN,
                message="无排序步骤，PERF-011 不适用",
            )

        total_steps = len(plan.steps)
        has_limit = any(isinstance(s, LimitStep) for s in plan.steps)

        for idx, sort_step in sort_steps:
            # 排序在最后 2 步内 → OK
            if idx >= total_steps - 2:
                continue
            # 排序后有 LimitStep → OK
            if has_limit:
                continue

            return PerfCheckResult(
                rule_id="PERF-011",
                passed=False,
                severity=PerfSeverity.WARN,
                message=(
                    f"SortStep '{sort_step.step_id}' 位于步骤 {idx+1}/{total_steps}——"
                    f"非最终位置且无后续 LIMIT，中间排序通常无用。"
                    f"建议将 ORDER BY 移至最终展示层或仅在窗口必要位置使用"
                ),
                flagged_items=[
                    f"step={sort_step.step_id}",
                    f"position={idx+1}/{total_steps}",
                ],
            )

        return PerfCheckResult(
            rule_id="PERF-011",
            passed=True,
            severity=PerfSeverity.WARN,
            message="排序位置合理（最终展示层或有 LIMIT 保护）",
        )

    def _check_perf012(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-012: 窗口函数前必须尽可能缩小数据范围 → WARN。

        检查：WindowStep 前是否有 AggregateStep 或 FilterStep 缩小数据量。
        无预缩小步骤时，窗口函数处理全量数据可能导致性能问题。
        """
        window_steps = [s for s in plan.steps if isinstance(s, WindowStep)]
        if not window_steps:
            return PerfCheckResult(
                rule_id="PERF-012",
                passed=True,
                severity=PerfSeverity.WARN,
                message="无窗口函数步骤，PERF-012 不适用",
            )

        # 找到第一个 WindowStep 的位置
        first_window_idx = None
        for i, s in enumerate(plan.steps):
            if isinstance(s, WindowStep):
                first_window_idx = i
                break

        if first_window_idx is None:
            return PerfCheckResult(
                rule_id="PERF-012",
                passed=True,
                severity=PerfSeverity.WARN,
                message="无窗口函数步骤",
            )

        # 检查窗口前是否有聚合或过滤
        has_pre_narrow = False
        for i in range(first_window_idx):
            step = plan.steps[i]
            if isinstance(step, (AggregateStep,)) or (
                hasattr(step, "step_type") and step.step_type == "filter"
            ):
                has_pre_narrow = True
                break

        if not has_pre_narrow:
            return PerfCheckResult(
                rule_id="PERF-012",
                passed=False,
                severity=PerfSeverity.WARN,
                message=(
                    "WindowStep 前无聚合或过滤步骤——"
                    "窗口函数可能处理全量数据。建议在窗口计算前先通过"
                    "聚合（GROUP BY）或过滤（WHERE）缩小数据范围"
                ),
                flagged_items=[
                    f"window_at_step={first_window_idx+1}/{len(plan.steps)}",
                ],
            )

        return PerfCheckResult(
            rule_id="PERF-012",
            passed=True,
            severity=PerfSeverity.WARN,
            message="窗口函数前已缩小数据范围",
        )

    def _check_perf013(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-013: 高频指标建议沉淀为汇总表 → WARN。

        检查：AggregateStep 中是否有重复的聚合模式（如同一指标多次出现）。
        当前实现为启发式——检查是否包含典型的高频指标模式。
        未来可通过跨计划指标频率统计增强。
        """
        agg_steps = [s for s in plan.steps if isinstance(s, AggregateStep)]
        if not agg_steps:
            return PerfCheckResult(
                rule_id="PERF-013",
                passed=True,
                severity=PerfSeverity.WARN,
                message="无聚合步骤，PERF-013 不适用",
            )

        # 收集所有聚合指标
        all_metrics = []
        for agg in agg_steps:
            for m in agg.metrics:
                all_metrics.append(f"{m.aggregation}:{m.input_column}")

        # 检查是否有复用的指标模式
        from collections import Counter
        metric_counts = Counter(all_metrics)

        # 如果是单计划的单次聚合，不触发（无法判断是否为高频）
        if len(all_metrics) <= 3:
            return PerfCheckResult(
                rule_id="PERF-013",
                passed=True,
                severity=PerfSeverity.WARN,
                message="聚合指标数量较少，当前不触发高频建议",
            )

        # 启发式：聚合指标 ≥ 5 个时提示可沉淀
        if len(all_metrics) >= 5:
            return PerfCheckResult(
                rule_id="PERF-013",
                passed=False,
                severity=PerfSeverity.WARN,
                message=(
                    f"检测到 {len(all_metrics)} 个聚合指标——"
                    f"若这些指标为高频查询（如日活、GMV 等），"
                    f"建议沉淀为 DWS 汇总表以避免重复计算"
                ),
                flagged_items=[m for m, c in metric_counts.items() if c >= 2],
            )

        return PerfCheckResult(
            rule_id="PERF-013",
            passed=True,
            severity=PerfSeverity.WARN,
            message="聚合指标未触发高频建议阈值",
        )

    def _check_perf014(self, plan: SqlBuildPlan) -> PerfCheckResult:
        """PERF-014: 复杂 SQL 允许拆分 _temp 中间表验证中间行数 → WARN。

        检查：SqlBuildPlan 步骤数 > _COMPLEX_STEP_THRESHOLD 时，
        建议拆分为 _temp 中间表以便验证每步行数。
        """
        step_count = len(plan.steps)

        if step_count <= _COMPLEX_STEP_THRESHOLD:
            return PerfCheckResult(
                rule_id="PERF-014",
                passed=True,
                severity=PerfSeverity.WARN,
                message=f"步骤数 {step_count} ≤ {_COMPLEX_STEP_THRESHOLD}，无需拆分",
            )

        # 检查是否已有 _temp 表引用（表名含 _temp）
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        has_temp = any("_temp" in s.table_ref.lower() for s in scan_steps)

        if has_temp:
            return PerfCheckResult(
                rule_id="PERF-014",
                passed=True,
                severity=PerfSeverity.WARN,
                message="已使用 _temp 中间表",
            )

        return PerfCheckResult(
            rule_id="PERF-014",
            passed=False,
            severity=PerfSeverity.WARN,
            message=(
                f"SqlBuildPlan 包含 {step_count} 个步骤（> {_COMPLEX_STEP_THRESHOLD}）——"
                f"建议拆分为 _temp 中间表，便于验证每步中间行数并定位性能瓶颈"
            ),
            flagged_items=[f"step_count={step_count}"],
        )

    def _check_perf015(
        self,
        plan: SqlBuildPlan,
        execution_stats: dict | None = None,
    ) -> PerfCheckResult:
        """PERF-015: 慢 SQL 必须基于真实执行计划优化 → PERF_FEEDBACK。

        此规则不阻断编译——它检测到慢 SQL 时仅产生反馈，
        建议运行 EXPLAIN 或 EXPLAIN ANALYZE 获取执行计划。
        LLM 不参与性能决策。

        若 execution_stats 为 None，跳过此检查。
        """
        if execution_stats is None:
            return PerfCheckResult(
                rule_id="PERF-015",
                passed=True,
                severity=PerfSeverity.PERF_FEEDBACK,
                message="PERF-015 跳过——未提供执行统计信息",
            )

        execution_time_ms = execution_stats.get("execution_time_ms", 0)
        if execution_time_ms <= _SLOW_QUERY_MS_THRESHOLD:
            return PerfCheckResult(
                rule_id="PERF-015",
                passed=True,
                severity=PerfSeverity.PERF_FEEDBACK,
                message=f"执行耗时 {execution_time_ms}ms ≤ {_SLOW_QUERY_MS_THRESHOLD}ms，非慢查询",
            )

        return PerfCheckResult(
            rule_id="PERF-015",
            passed=False,
            severity=PerfSeverity.PERF_FEEDBACK,
            message=(
                f"慢 SQL 检测——执行耗时 {execution_time_ms}ms > "
                f"{_SLOW_QUERY_MS_THRESHOLD}ms。"
                f"建议运行 EXPLAIN ANALYZE 获取执行计划并基于反馈优化"
            ),
            flagged_items=[
                f"execution_time_ms={execution_time_ms}",
                f"plan_id={plan.plan_id}",
            ],
        )


# ════════════════════════════════════════════
# Predicate 辅助函数（PERF 规则公用）
# ════════════════════════════════════════════


def _predicate_contains_keywords(
    pred, keywords: set[str], table_ref: str | None = None,
) -> bool:
    """递归检查谓词树中是否引用了指定关键词的字段。

    用于 PERF-002 的时间过滤检测。

    当 table_ref 不为 None 时，仅检查 ColumnRef.table_ref 匹配的列引用——
    防止其他表的时间过滤误放行当前事实表（PERF-002 跨表绕过修复）。
    AND/OR 嵌套时 table_ref 参数穿透递归，不会在中间层丢失。
    """
    left = getattr(pred, "left", None)
    if left is not None:
        col_name = getattr(left, "column_name", "")
        if col_name:
            # left 是 ColumnRef——先检查表归属，再匹配关键词
            if table_ref is None or str(left.table_ref) == table_ref:
                norm_name = getattr(left, "normalized_name", "")
                if any(kw in col_name.lower() for kw in keywords):
                    return True
                if any(kw in norm_name.lower() for kw in keywords):
                    return True
        if hasattr(left, "operator"):
            # left 是嵌套 Predicate（AND/OR）——递归携带 table_ref
            if _predicate_contains_keywords(left, keywords, table_ref):
                return True

    right = getattr(pred, "right", None)
    if right is not None and hasattr(right, "operator"):
        # right 是嵌套 Predicate（AND/OR）——递归携带 table_ref
        if _predicate_contains_keywords(right, keywords, table_ref):
            return True

    return False


def _find_function_on_left(pred, functions: set[str]) -> list[str]:
    """递归检查谓词树中是否存在左侧字段被函数包裹的模式。

    返回违规描述的列表。用于 PERF-003 的时间函数检测。

    检测模式：FUNC(column) OP value，其中 FUNC 在 functions 集合中。
    通过检查 Predicate 的 left 是否为嵌套 Predicate（表示函数调用）。
    """
    violations: list[str] = []

    left = getattr(pred, "left", None)
    if left is not None:
        # 检查 left 是否为嵌套 Predicate（可能是函数调用）
        if hasattr(left, "operator"):
            # left 是嵌套谓词——检查其 left 是否为 ColumnRef
            inner_left = getattr(left, "left", None)
            if inner_left is not None and hasattr(inner_left, "column_name"):
                # 检查 operator 是否暗示函数调用
                op_str = str(getattr(left, "operator", ""))
                if any(f in op_str.lower() for f in functions):
                    violations.append(
                        f"{op_str}({inner_left.column_name})——"
                        f"不应在 WHERE 左侧对字段套函数"
                    )
            # 递归检查嵌套
            violations.extend(_find_function_on_left(left, functions))

    right = getattr(pred, "right", None)
    if right is not None and hasattr(right, "operator"):
        violations.extend(_find_function_on_left(right, functions))

    return violations
