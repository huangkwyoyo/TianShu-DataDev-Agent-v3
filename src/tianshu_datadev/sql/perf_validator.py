"""PerfValidator——Phase 1C 性能契约验证器。

8 条 PERF 规则：4 条 REJECT（硬门禁）+ 4 条 WARN（软规则）。
REJECT 违反后阻断编译，WARN 违反后记录到验证结果但不阻断。

规则来源：docs/roadmap/phase-1c-compilation-execution.md § PerfContract。
"""

from __future__ import annotations

from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    JoinStep,
    LimitStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)

from .models import (
    PerfRule,
    PerfRuleLevel,
    PerfValidationResult,
)

# ════════════════════════════════════════════
# PERF 规则注册表
# ════════════════════════════════════════════


def _build_perf_rules() -> list[PerfRule]:
    """构建 8 条 PERF 规则的注册表。"""
    return [
        PerfRule(
            rule_id="PERF-001",
            description="无 LIMIT 的全量扫描且估算行数 > 10M",
            level=PerfRuleLevel.REJECT,
            condition="ScanStep.estimated_row_count > 10,000,000 且计划中无 LimitStep",
        ),
        PerfRule(
            rule_id="PERF-002",
            description="Join 键双方类型不一致（如 int ↔ varchar）",
            level=PerfRuleLevel.REJECT,
            condition="JoinStep.join_keys 双方字段类型不在同一兼容组",
        ),
        PerfRule(
            rule_id="PERF-003",
            description="窗口函数违反白名单",
            level=PerfRuleLevel.REJECT,
            condition="Phase 3B 生效，Phase 1C no-op",
        ),
        PerfRule(
            rule_id="PERF-004",
            description="分区过滤键未在 WHERE 中出现且表为分区表",
            level=PerfRuleLevel.REJECT,
            condition="ScanStep.partition_filters 为空且表有分区键声明",
        ),
        PerfRule(
            rule_id="PERF-005",
            description="无 LIMIT 的排序且 estimated_input_rows > 1M",
            level=PerfRuleLevel.WARN,
            condition="SortStep.requires_full_sort=True 且 estimated_input_rows > 1,000,000",
        ),
        PerfRule(
            rule_id="PERF-006",
            description="聚合前行数 > 10M 且 group_keys > 5",
            level=PerfRuleLevel.WARN,
            condition="大表聚合且分组维度过多",
        ),
        PerfRule(
            rule_id="PERF-007",
            description="SELECT *（required_columns 为空或等于全表列）",
            level=PerfRuleLevel.WARN,
            condition="ScanStep.required_columns 为空",
        ),
        PerfRule(
            rule_id="PERF-008",
            description="Join 前可预聚合但 pre_aggregation_allowed=False",
            level=PerfRuleLevel.WARN,
            condition="大事实表 Join 且 pre_aggregation_allowed=False",
        ),
    ]


# ════════════════════════════════════════════
# PerfValidator
# ════════════════════════════════════════════


class PerfValidator:
    """Phase 1C 性能契约验证器——硬规则（REJECT）阻断 / 软规则（WARN）记录。

    Validator 是确定性的——相同输入 → 相同输出。
    REJECT 规则全部通过 + 编译器可以继续编译。
    """

    def __init__(self):
        """初始化 PerfValidator，加载 8 条规则注册表。"""
        self._rules = _build_perf_rules()

    @property
    def rules(self) -> list[PerfRule]:
        """返回已注册的 PERF 规则列表（只读）。"""
        return list(self._rules)

    def validate(
        self,
        plan: SqlBuildPlan,
        partitioned_tables: set[str] | None = None,
    ) -> tuple[bool, list[PerfValidationResult]]:
        """对 SqlBuildPlan 执行全部 8 条 PERF 规则。

        Args:
            plan: 待验证的 SqlBuildPlan
            partitioned_tables: 已知分区表的 table_ref 集合——用于 PERF-004 检查。
                               为 None 时 PERF-004 跳过（向后兼容）。

        Returns:
            (all_reject_passed, results)
            all_reject_passed: 所有 REJECT 规则是否全部通过
            results: 每条规则的验证结果（含 WARN）
        """
        results: list[PerfValidationResult] = []

        # PERF-001: 无 LIMIT 全量扫描且估算行数 > 10M
        results.append(self._check_perf001(plan))

        # PERF-002: Join 键类型不一致
        results.append(self._check_perf002(plan))

        # PERF-003: 窗口函数白名单（Phase 1C no-op）
        results.append(self._check_perf003(plan))

        # PERF-004: 分区过滤缺失
        results.append(self._check_perf004(plan, partitioned_tables))

        # PERF-005: 无 LIMIT 排序 + 大输入
        results.append(self._check_perf005(plan))

        # PERF-006: 大聚合 + 多 group_keys
        results.append(self._check_perf006(plan))

        # PERF-007: SELECT *
        results.append(self._check_perf007(plan))

        # PERF-008: 预聚合未启用
        results.append(self._check_perf008(plan))

        # 所有 REJECT 规则通过 → 可继续
        all_reject_passed = all(
            r.passed
            for r in results
            if r.level == PerfRuleLevel.REJECT
        )
        return all_reject_passed, results

    # ── 各规则检查 ──

    def _check_perf001(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-001: 无 LIMIT 的全量扫描且估算行数 > 10M → REJECT。"""
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]
        has_limit = any(isinstance(s, LimitStep) for s in plan.steps)

        if has_limit:
            return PerfValidationResult(
                rule_id="PERF-001",
                passed=True,
                level=PerfRuleLevel.REJECT,
                message="存在 LIMIT，全量扫描风险已控制",
            )

        for scan in scan_steps:
            if scan.estimated_row_count is not None and scan.estimated_row_count > 10_000_000:
                return PerfValidationResult(
                    rule_id="PERF-001",
                    passed=False,
                    level=PerfRuleLevel.REJECT,
                    message=(
                        f"表 '{scan.table_ref}' 估算行数 {scan.estimated_row_count:,} > 10M，"
                        f"且无 LIMIT——全量扫描被阻断"
                    ),
                )

        return PerfValidationResult(
            rule_id="PERF-001",
            passed=True,
            level=PerfRuleLevel.REJECT,
            message="无超过 10M 行的全量扫描",
        )

    def _check_perf002(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-002: Join 键双方类型不一致 → REJECT。"""
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]

        for join_step in join_steps:
            for left_key, right_key in join_step.join_keys:
                # 从 ColumnRef 无法直接获取类型——类型兼容检查在 Validator 中完成
                # 此处检查键名是否暗示了类型不兼容（如一个叫 *_id, 一个叫 *_name）
                left_name = left_key.column_name.lower()
                right_name = right_key.column_name.lower()

                # 启发式：一个暗示 ID（整数），另一个暗示 name/text（字符串）
                id_hints = ("id", "key", "code", "num", "no")
                text_hints = ("name", "text", "desc", "type", "status")

                left_is_id = any(left_name.endswith(h) or left_name == h for h in id_hints)
                right_is_text = any(right_name.endswith(h) or right_name == h for h in text_hints)
                right_is_id = any(right_name.endswith(h) or right_name == h for h in id_hints)
                left_is_text = any(left_name.endswith(h) or left_name == h for h in text_hints)

                if (left_is_id and right_is_text) or (right_is_id and left_is_text):
                    return PerfValidationResult(
                        rule_id="PERF-002",
                        passed=False,
                        level=PerfRuleLevel.REJECT,
                        message=(
                            f"Join 键可能类型不兼容: "
                            f"'{left_key.column_name}' ↔ '{right_key.column_name}'——"
                            f"一个暗示数值型，另一个暗示字符型"
                        ),
                    )

        return PerfValidationResult(
            rule_id="PERF-002",
            passed=True,
            level=PerfRuleLevel.REJECT,
            message="Join 键类型兼容",
        )

    def _check_perf003(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-003: 窗口函数违反白名单 → REJECT（Phase 1C no-op）。"""
        # Phase 1C: SqlBuildPlan 不支持窗口函数，此规则注册但不触发
        return PerfValidationResult(
            rule_id="PERF-003",
            passed=True,
            level=PerfRuleLevel.REJECT,
            message="PERF-003 窗口函数白名单检查——Phase 1C no-op（无窗口函数支持）",
        )

    def _check_perf004(
        self,
        plan: SqlBuildPlan,
        partitioned_tables: set[str] | None = None,
    ) -> PerfValidationResult:
        """PERF-004: 分区过滤键缺失且表声明了分区字段 → REJECT。

        检查逻辑：
        1. 若 partitioned_tables 为 None——未提供分区表信息，跳过检查（通过）
        2. 若 ScanStep.table_ref 在 partitioned_tables 中：
           a. 若 scan.partition_filters 非空 → 通过
           b. 若 scan.predicates 中包含疑似分区字段的过滤 → 通过
           c. 否则 → REJECT（分区表缺少分区过滤）

        分区字段启发式：列名含 dt/date/ds/hour/day/partition 关键词。
        """
        if partitioned_tables is None:
            return PerfValidationResult(
                rule_id="PERF-004",
                passed=True,
                level=PerfRuleLevel.REJECT,
                message="PERF-004 跳过——未提供分区表信息",
            )

        if not partitioned_tables:
            return PerfValidationResult(
                rule_id="PERF-004",
                passed=True,
                level=PerfRuleLevel.REJECT,
                message="无已注册的分区表",
            )

        # 分区字段关键词
        partition_keywords = {
            "dt", "date", "ds", "hour", "day", "month",
            "stat_date", "dt_date", "partition", "part",
        }

        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]

        for scan in scan_steps:
            if scan.table_ref not in partitioned_tables:
                continue

            # 检查 ScanStep 自身的分区过滤
            if scan.partition_filters:
                continue  # 有显式分区过滤 → OK

            # 检查 predicates 中是否包含分区字段
            has_partition_filter = False
            for pred in scan.predicates:
                if _predicate_refers_to_keywords(pred, partition_keywords):
                    has_partition_filter = True
                    break

            # 也检查关联的 FilterStep
            if not has_partition_filter:
                filter_steps = [
                    s for s in plan.steps
                    if hasattr(s, "step_type") and s.step_type == "filter"
                ]
                for fstep in filter_steps:
                    if hasattr(fstep, "predicate"):
                        # 检查谓词是否引用此分区表且含分区字段
                        if _predicate_refers_to_table(
                            fstep.predicate, scan.table_ref
                        ) and _predicate_refers_to_keywords(
                            fstep.predicate, partition_keywords
                        ):
                            has_partition_filter = True
                            break

            if not has_partition_filter:
                return PerfValidationResult(
                    rule_id="PERF-004",
                    passed=False,
                    level=PerfRuleLevel.REJECT,
                    message=(
                        f"分区表 '{scan.table_ref}' 缺少分区过滤条件——"
                        f"必须对分区键进行过滤以避免全分区扫描"
                    ),
                )

        return PerfValidationResult(
            rule_id="PERF-004",
            passed=True,
            level=PerfRuleLevel.REJECT,
            message="所有分区表均包含分区过滤",
        )

    def _check_perf005(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-005: 无 LIMIT 排序且 estimated_input_rows > 1M → WARN。"""
        sort_steps = [s for s in plan.steps if isinstance(s, SortStep)]

        for sort_step in sort_steps:
            if sort_step.requires_full_sort and sort_step.estimated_input_rows is not None:
                if sort_step.estimated_input_rows > 1_000_000:
                    return PerfValidationResult(
                        rule_id="PERF-005",
                        passed=False,
                        level=PerfRuleLevel.WARN,
                        message=(
                            f"SortStep 需要全排序且估算输入行数 "
                            f"{sort_step.estimated_input_rows:,} > 1M——建议添加 LIMIT"
                        ),
                    )

        return PerfValidationResult(
            rule_id="PERF-005",
            passed=True,
            level=PerfRuleLevel.WARN,
            message="排序性能风险可控",
        )

    def _check_perf006(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-006: 聚合前行数 > 10M 且 group_keys > 5 → WARN。"""
        agg_steps = [s for s in plan.steps if isinstance(s, AggregateStep)]
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]

        if not agg_steps:
            return PerfValidationResult(
                rule_id="PERF-006",
                passed=True,
                level=PerfRuleLevel.WARN,
                message="无聚合步骤，PERF-006 不适用",
            )

        for scan in scan_steps:
            if scan.estimated_row_count is not None and scan.estimated_row_count > 10_000_000:
                for agg in agg_steps:
                    if len(agg.group_keys) > 5:
                        return PerfValidationResult(
                            rule_id="PERF-006",
                            passed=False,
                            level=PerfRuleLevel.WARN,
                            message=(
                                f"表 '{scan.table_ref}' 估算行数 {scan.estimated_row_count:,} > 10M，"
                                f"且聚合 group_keys={len(agg.group_keys)} > 5——"
                                f"建议减少分组维度或预聚合"
                            ),
                        )

        return PerfValidationResult(
            rule_id="PERF-006",
            passed=True,
            level=PerfRuleLevel.WARN,
            message="聚合性能风险可控",
        )

    def _check_perf007(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-007: SELECT *（required_columns 为空）→ WARN。"""
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]

        for scan in scan_steps:
            if not scan.required_columns:
                return PerfValidationResult(
                    rule_id="PERF-007",
                    passed=False,
                    level=PerfRuleLevel.WARN,
                    message=(
                        f"表 '{scan.table_ref}' 的 required_columns 为空——"
                        f"等效于 SELECT *，应在 IR 中显式声明所需列"
                    ),
                )

        return PerfValidationResult(
            rule_id="PERF-007",
            passed=True,
            level=PerfRuleLevel.WARN,
            message="所有 ScanStep 均显式声明了 required_columns",
        )

    def _check_perf008(self, plan: SqlBuildPlan) -> PerfValidationResult:
        """PERF-008: Join 前可预聚合但 pre_aggregation_allowed=False → WARN。"""
        join_steps = [s for s in plan.steps if isinstance(s, JoinStep)]
        scan_steps = [s for s in plan.steps if isinstance(s, ScanStep)]

        if not join_steps:
            return PerfValidationResult(
                rule_id="PERF-008",
                passed=True,
                level=PerfRuleLevel.WARN,
                message="无 Join 步骤，PERF-008 不适用",
            )

        for scan in scan_steps:
            # 检查大表 Join 且未启用预聚合
            if scan.estimated_row_count is not None and scan.estimated_row_count > 1_000_000:
                for join_step in join_steps:
                    if not join_step.pre_aggregation_allowed:
                        return PerfValidationResult(
                            rule_id="PERF-008",
                            passed=False,
                            level=PerfRuleLevel.WARN,
                            message=(
                                f"大表 '{scan.table_ref}'（{scan.estimated_row_count:,} 行）Join 前"
                                f"可预聚合但 pre_aggregation_allowed=False——"
                                f"建议评估是否可在 Join 前先聚合"
                            ),
                        )

        return PerfValidationResult(
            rule_id="PERF-008",
            passed=True,
            level=PerfRuleLevel.WARN,
            message="Join 预聚合设置合理或无可优化场景",
        )


# ════════════════════════════════════════════
# Predicate 辅助函数（PERF-004 分区过滤检查用）
# ════════════════════════════════════════════


def _predicate_refers_to_keywords(pred, keywords: set[str]) -> bool:
    """递归检查谓词树中是否引用了指定关键词的字段。"""
    left = getattr(pred, "left", None)
    if left is not None:
        col_name = getattr(left, "column_name", "")
        norm_name = getattr(left, "normalized_name", "")
        if any(kw in col_name.lower() for kw in keywords):
            return True
        if any(kw in norm_name.lower() for kw in keywords):
            return True
        if hasattr(left, "operator"):
            if _predicate_refers_to_keywords(left, keywords):
                return True

    right = getattr(pred, "right", None)
    if right is not None and hasattr(right, "operator"):
        if _predicate_refers_to_keywords(right, keywords):
            return True

    return False


def _predicate_refers_to_table(pred, table_ref: str) -> bool:
    """检查谓词是否引用了指定表的字段。"""
    left = getattr(pred, "left", None)
    if left is not None:
        left_table = getattr(left, "table_ref", "")
        if left_table == table_ref:
            return True
        if hasattr(left, "operator"):
            if _predicate_refers_to_table(left, table_ref):
                return True
    return False
