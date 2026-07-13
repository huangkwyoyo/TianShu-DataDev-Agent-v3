"""CRE v2 原型——判定层。

模块职责：
- CanonicalComparisonResult 数据模型
- DecisionEngine：判定矩阵输出结论
"""

from __future__ import annotations

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.cre_alignment import AlignmentResult, BucketResult
from tianshu_datadev.spark.cre_comparison import (
    CellComparisonResult,
    ComparisonRowResult,
)
from tianshu_datadev.spark.cre_encoding import (
    _TOLERANCE_RULE_DESC,
    CreConfig,
    NotToleratedReason,
    ToleratedDifferenceWarning,
    ToleratedFieldDetail,
    _normalize_name,
    _type_family,
)

# ════════════════════════════════════════════
# CanonicalComparisonResult——完整比较结果
# ════════════════════════════════════════════


class CanonicalComparisonResult(StrictModel):
    """CRE 原型比较结果——独立于生产 verify()。

    status : "CONSISTENT" | "CONSISTENT_WITH_WARN" | "MISMATCH" | "HUMAN_REVIEW"
    """
    status: str = ""

    total_rows_duckdb: int = 0
    total_rows_spark: int = 0
    aligned_rows: int = 0
    rows_missing_in_duckdb: int = 0
    rows_missing_in_spark: int = 0
    duplicate_keys: bool = False

    exact_match_rows: int = 0
    tolerance_match_rows: int = 0

    bucket_count: int = 0
    mismatched_buckets: list[int] = Field(default_factory=list)
    mismatched_bucket_count: int = 0

    warnings: list[ToleratedDifferenceWarning] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    decision_reason: str = ""


# ════════════════════════════════════════════
# DecisionEngine——判定矩阵
# ════════════════════════════════════════════


class DecisionEngine:
    """判定引擎——根据判定矩阵输出结论。

    决策优先级（要求 3/4）：
    1. 缺少 Contract/primary_keys/timezone → HUMAN_REVIEW
    2. 行数不匹配 / 重复键 → MISMATCH / HUMAN_REVIEW
    3. INT/BOOL/DATE/STRING 差异 → MISMATCH（无容差规则）
    4. 超容差差异（OUT_OF_TOLERANCE）→ MISMATCH
    5. 未知原因 / 无规则 → MISMATCH / HUMAN_REVIEW
    6. 全量 exact match → CONSISTENT
    7. 全量在容差内 → CONSISTENT_WITH_WARN（ratio 仅影响诊断，不改变判定）
    """

    @classmethod
    def decide(
        cls,
        alignment: AlignmentResult,
        row_comparisons: list[tuple[dict, dict, ComparisonRowResult]],
        bucket_result: BucketResult,
        config: CreConfig,
    ) -> CanonicalComparisonResult:
        result = CanonicalComparisonResult(
            total_rows_duckdb=(
                len(alignment.aligned_pairs) + len(alignment.duckdb_only)
            ),
            total_rows_spark=(
                len(alignment.aligned_pairs) + len(alignment.spark_only)
            ),
            aligned_rows=len(alignment.aligned_pairs),
            rows_missing_in_duckdb=len(alignment.spark_only),
            rows_missing_in_spark=len(alignment.duckdb_only),
            duplicate_keys=alignment.duplicate_keys,
            bucket_count=bucket_result.num_buckets,
            mismatched_buckets=bucket_result.mismatched_buckets,
            mismatched_bucket_count=len(bucket_result.mismatched_buckets),
        )

        # ── Gate 1: 缺少 Contract / primary_keys / timezone ──
        if not config.output_columns:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                "缺少 Contract output_columns——无法确定列顺序和类型规则"
            )
            return result

        if not config.primary_keys:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                "缺少 Contract 权威主键（primary_keys）——无法完成行对齐"
            )
            return result

        has_ts_no_tz = any(
            _type_family(col.data_type or "") == "TIMESTAMP"
            for col in config.output_columns
        ) and not config.timezone
        if has_ts_no_tz:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                "Contract 包含 timestamp 列但未配置 timezone——"
                "无法确定时区，禁止自动判定"
            )
            return result

        # ── Gate 2: 对齐失败（含 NULL 主键 / schema 差异）──
        if alignment.error_message and not alignment.duplicate_keys:
            # schema 差异（要求 3）——含 "必需列" 或 "额外列"
            err_msg = alignment.error_message or ""
            if "必需列" in err_msg or "额外列" in err_msg:
                result.status = "MISMATCH"
                result.decision_reason = f"Contract 列定义与实际数据不匹配：{alignment.error_message}"
                return result
            # 其他对齐失败 → HUMAN_REVIEW
            result.status = "HUMAN_REVIEW"
            result.decision_reason = f"行对齐失败：{alignment.error_message}"
            return result

        # ── Gate 3: 重复键 → HUMAN_REVIEW ──
        if alignment.duplicate_keys:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                f"发现重复主键——无法自动对齐：{alignment.error_message}"
            )
            return result

        # ── Gate 4: 行数不匹配 → MISMATCH ──
        if alignment.duckdb_only or alignment.spark_only:
            missing_d = len(alignment.duckdb_only)
            missing_s = len(alignment.spark_only)
            result.status = "MISMATCH"
            result.decision_reason = (
                f"行数不匹配——DuckDB 额外 {missing_d} 行，"
                f"Spark 额外 {missing_s} 行。"
                f"DuckDB 行数={result.total_rows_duckdb}，"
                f"Spark 行数={result.total_rows_spark}"
            )
            return result

        # ── 分析对齐行比较结果 ──
        exact_count = 0
        all_cells: list[CellComparisonResult] = []
        for _d, _s, rr in row_comparisons:
            if rr.exact_match:
                exact_count += 1
            else:
                all_cells.extend(rr.cell_results)

        result.exact_match_rows = exact_count
        result.tolerance_match_rows = len(row_comparisons) - exact_count

        # 分类差异
        within_count = sum(
            1 for c in all_cells if c.reason == NotToleratedReason.WITHIN_TOLERANCE
        )
        out_count = sum(
            1 for c in all_cells if c.reason == NotToleratedReason.OUT_OF_TOLERANCE
        )
        int_mismatch = [c for c in all_cells if c.reason == NotToleratedReason.INTEGER_MISMATCH]
        str_mismatch = [c for c in all_cells if c.reason == NotToleratedReason.STRING_MISMATCH]
        bool_mismatch = [c for c in all_cells if c.reason == NotToleratedReason.BOOL_MISMATCH]
        date_mismatch = [c for c in all_cells if c.reason == NotToleratedReason.DATE_MISMATCH]
        ts_mismatch = [c for c in all_cells if c.reason == NotToleratedReason.TIMESTAMP_MISMATCH]
        unknown_cause = [c for c in all_cells if c.reason == NotToleratedReason.UNKNOWN_CAUSE]
        rules_unknown = [c for c in all_cells if c.reason == NotToleratedReason.RULES_UNKNOWN]
        no_type_info = [c for c in all_cells if c.reason == NotToleratedReason.NO_TYPE_INFO]

        # ── Gate 5: 整数/字符串/布尔/日期/时间戳差异 → MISMATCH ──
        non_tolerance_mismatches = (
            int_mismatch + str_mismatch + bool_mismatch + date_mismatch + ts_mismatch
        )
        if non_tolerance_mismatches:
            categories = set(c.reason.value for c in non_tolerance_mismatches)
            result.status = "MISMATCH"
            result.decision_reason = (
                f"存在不允许容差的列类型差异：{sorted(categories)}。"
                f"共 {len(non_tolerance_mismatches)} 处差异"
            )
            return result

        # ── Gate 6: 超容差数值差异 → MISMATCH ──
        if out_count > 0:
            result.status = "MISMATCH"
            result.decision_reason = (
                f"存在 {out_count} 处超容差数值差异——"
                f"isclose(rel_tol={config.float_rel_tolerance}, "
                f"abs_tol={config.float_abs_tolerance}) 不满足"
            )
            return result

        # ── Gate 7: 未知原因 / 无规则 → MISMATCH / HUMAN_REVIEW ──
        if unknown_cause:
            result.status = "MISMATCH"
            result.decision_reason = (
                f"存在 {len(unknown_cause)} 处未知原因差异——无法自动判定"
            )
            return result

        # 区分 NaN/Inf HUMAN_REVIEW 与其他 RULES_UNKNOWN
        nan_human_review = [
            c for c in rules_unknown
            if "HUMAN_REVIEW" in c.description and ("NaN" in c.description or "Inf" in c.description)
        ]
        if nan_human_review:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                f"存在 {len(nan_human_review)} 处特殊浮点值（NaN/Inf）差异——"
                f"环境清单策略为 HUMAN_REVIEW，无法自动判定"
            )
            return result

        other_rules_unknown = [c for c in rules_unknown if c not in nan_human_review]
        if other_rules_unknown:
            result.status = "MISMATCH"
            result.decision_reason = (
                f"存在 {len(other_rules_unknown)} 处无对应比较规则的列类型差异"
            )
            return result

        if no_type_info:
            result.status = "HUMAN_REVIEW"
            result.decision_reason = (
                f"Contract 中 {len(no_type_info)} 列缺少 data_type——"
                f"无法确定比较规则"
            )
            return result

        # ── 全部通过 → CONSISTENT / CONSISTENT_WITH_WARN ──
        if within_count == 0:
            result.status = "CONSISTENT"
            result.decision_reason = (
                f"双引擎 {exact_count} 行逐行 exact match——无需容差比较"
            )
        else:
            # 全量容差比较后一致（要求 3：不因 ratio>5% 改 HUMAN_REVIEW）
            # 计算受影响唯一行数比例（非单元格数）
            affected_rows_set: set[int] = set()
            for idx, (_d, _s, rr) in enumerate(row_comparisons):
                if any(c.reason == NotToleratedReason.WITHIN_TOLERANCE for c in rr.cell_results):
                    affected_rows_set.add(idx)
            affected_row_count = len(affected_rows_set)
            affected_row_ratio = affected_row_count / max(len(row_comparisons), 1)
            severe = "高" if affected_row_ratio > 0.05 else "低"

            # 构建 WARN
            warn = cls._build_warning(all_cells, config, row_comparisons)
            result.warnings = [warn] if warn else []

            result.status = "CONSISTENT_WITH_WARN"
            result.decision_reason = (
                f"全部 {within_count} 处差异在已批准容差内——"
                f"判定为一致，产生 WARN。"
                f"受影响唯一行数={affected_row_count}/{len(row_comparisons)}"
                f"（{affected_row_ratio:.2%}，严重度={severe}），"
                f"不影响判定结论"
            )

        return result

    @classmethod
    def _build_warning(
        cls,
        all_cells: list[CellComparisonResult],
        config: CreConfig,
        row_comparisons: list[tuple[dict, dict, ComparisonRowResult]],
    ) -> ToleratedDifferenceWarning | None:
        """从容差内差异构建警告。"""
        within = [c for c in all_cells if c.reason == NotToleratedReason.WITHIN_TOLERANCE]
        if not within:
            return None

        # 计算受影响唯一行数
        affected_row_idx: set[int] = set()
        for idx, (_d, _s, rr) in enumerate(row_comparisons):
            for cell in rr.cell_results:
                if cell.reason == NotToleratedReason.WITHIN_TOLERANCE:
                    affected_row_idx.add(idx)
                    break
        affected_row_count = len(affected_row_idx)
        affected_row_ratio = min(
            affected_row_count / max(len(row_comparisons), 1), 1.0
        )

        # 按列分组
        col_groups: dict[str, list[CellComparisonResult]] = {}
        for c in within:
            col_groups.setdefault(c.column, []).append(c)

        details: list[ToleratedFieldDetail] = []
        for col, cells in col_groups.items():
            abs_errs = [c.abs_error for c in cells if c.abs_error is not None]
            rel_errs = [c.rel_error for c in cells if c.rel_error is not None]

            # 确定 data_type
            dt = ""
            for col_def in config.output_columns:
                cn = _normalize_name(col_def.column_name)
                if cn == _normalize_name(col):
                    dt = col_def.data_type or ""
                    break

            family = _type_family(dt)
            rule = _TOLERANCE_RULE_DESC.get(family, "(unknown rule)")

            details.append(ToleratedFieldDetail(
                column=col,
                data_type=dt,
                rule_applied=rule,
                affected_cell_count=len(cells),
                total_rows=len(row_comparisons),
                affected_ratio=len(cells) / max(len(row_comparisons), 1),
                max_abs_error=max(abs_errs) if abs_errs else None,
                max_rel_error=max(rel_errs) if rel_errs else None,
                samples=[{
                    "duckdb_value": c.duckdb_value,
                    "spark_value": c.spark_value,
                } for c in cells[:10]],
            ))

        return ToleratedDifferenceWarning(
            action="PASS_WITH_WARN",
            tolerated_ratio=affected_row_ratio,
            affected_row_count=affected_row_count,
            affected_cell_count=len(within),
            field_details=details,
            total_comparison_rows=len(row_comparisons),
        )
