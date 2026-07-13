"""CRE v2 原型——容差比较层。

模块职责：
- CellComparisonResult / ComparisonRowResult 数据模型
- ToleranceComparator：列级容差感知比较器
"""

from __future__ import annotations

import math
import re
import zoneinfo
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.cre_encoding import (
    CreConfig,
    CREEncoder,
    EnvironmentManifest,
    NotToleratedReason,
    SpecialFloatStrategy,
    _normalize_name,
    _type_family,
)

# ════════════════════════════════════════════
# CellComparisonResult——单细胞比较结果
# ════════════════════════════════════════════


class CellComparisonResult(StrictModel):
    """单个单元格的跨引擎比较结果。"""
    column: str = ""
    data_type: str = ""
    reason: NotToleratedReason = NotToleratedReason.UNKNOWN_CAUSE
    duckdb_value: str = ""
    spark_value: str = ""
    abs_error: float | None = None
    rel_error: float | None = None
    description: str = ""


# ════════════════════════════════════════════
# ComparisonRowResult——单行比较结果
# ════════════════════════════════════════════


class ComparisonRowResult(StrictModel):
    """单行对齐后的比较结果。"""
    exact_match: bool = False
    cell_results: list[CellComparisonResult] = Field(default_factory=list)


# ════════════════════════════════════════════
# ToleranceComparator——列级容差比较
# ════════════════════════════════════════════


class ToleranceComparator:
    """列级容差感知比较器。

    同一 key 对齐后，逐列执行容差比较（而非基于 row digest 分桶配对）。

    容差规则：
    - FLOAT/DOUBLE  : math.isclose(rel_tol=1e-9, abs_tol=1e-12)
                       NaN/±Infinity 由 EnvironmentManifest 策略决定
    - DECIMAL       : Decimal.quantize(Contract.scale) 后精确 ==
    - INT/BOOL/DATE : 精确 ==（无容差）
    - VARCHAR       : str 精确 ==（无容差）
    - TIMESTAMP     : 精确 ==（时区由 CreConfig.timezone 保证）
    - 未知类型/无 data_type → 标记 NO_TYPE_INFO / RULES_UNKNOWN
    """

    @staticmethod
    def _get_type_family(data_type: str | None) -> str:
        if not data_type:
            return "UNKNOWN"
        return _type_family(data_type)

    @staticmethod
    def _compare_special_float(
        dv: float, sv: float,
        dv_nan: bool, sv_nan: bool,
        dv_inf: bool, sv_inf: bool,
        manifest: EnvironmentManifest,
    ) -> CellComparisonResult:
        """按 EnvironmentManifest 策略比较特殊浮点值（NaN/±Infinity）。"""
        # ── NaN ──
        if dv_nan or sv_nan:
            strategy = manifest.nan_handling
            both_nan = dv_nan and sv_nan
            if both_nan and strategy == SpecialFloatStrategy.EQUAL:
                return CellComparisonResult(
                    reason=NotToleratedReason.WITHIN_TOLERANCE,
                    duckdb_value=str(dv) if not dv_nan else "NaN",
                    spark_value=str(sv) if not sv_nan else "NaN",
                    description="双引擎均为 NaN，策略=EQUAL",
                )
            if strategy == SpecialFloatStrategy.EQUAL:
                return CellComparisonResult(
                    reason=NotToleratedReason.OUT_OF_TOLERANCE,
                    duckdb_value=str(dv),
                    spark_value=str(sv),
                    description="NaN vs 非 NaN，策略=EQUAL 但值不同",
                )
            if strategy == SpecialFloatStrategy.MISMATCH:
                return CellComparisonResult(
                    reason=NotToleratedReason.OUT_OF_TOLERANCE,
                    duckdb_value=str(dv),
                    spark_value=str(sv),
                    description="NaN 出现，策略=MISMATCH",
                )
            # HUMAN_REVIEW
            return CellComparisonResult(
                reason=NotToleratedReason.RULES_UNKNOWN,
                duckdb_value=str(dv),
                spark_value=str(sv),
                description="NaN 出现但策略=HUMAN_REVIEW/UNKNOWN——无法自动判定",
            )

        # ── ±Infinity ──
        if dv_inf or sv_inf:
            # 区分正负 Inf
            dv_pos_inf = dv_inf and dv > 0
            dv_neg_inf = dv_inf and dv < 0
            sv_pos_inf = sv_inf and sv > 0
            sv_neg_inf = sv_inf and sv < 0

            # 正 Inf
            if dv_pos_inf or sv_pos_inf:
                strategy = manifest.pos_inf_handling
                both_pos_inf = dv_pos_inf and sv_pos_inf
                if both_pos_inf and strategy == SpecialFloatStrategy.EQUAL:
                    return CellComparisonResult(
                        reason=NotToleratedReason.WITHIN_TOLERANCE,
                        duckdb_value="+Inf" if dv_pos_inf else str(dv),
                        spark_value="+Inf" if sv_pos_inf else str(sv),
                        description="双引擎均为 +Inf，策略=EQUAL",
                    )
                if strategy == SpecialFloatStrategy.MISMATCH:
                    return CellComparisonResult(
                        reason=NotToleratedReason.OUT_OF_TOLERANCE,
                        duckdb_value=str(dv),
                        spark_value=str(sv),
                        description="+Inf 出现，策略=MISMATCH",
                    )
                if strategy == SpecialFloatStrategy.EQUAL:
                    return CellComparisonResult(
                        reason=NotToleratedReason.OUT_OF_TOLERANCE,
                        duckdb_value=str(dv),
                        spark_value=str(sv),
                        description="+Inf vs 非 +Inf，策略=EQUAL 但值不同",
                    )
                # HUMAN_REVIEW
                return CellComparisonResult(
                    reason=NotToleratedReason.RULES_UNKNOWN,
                    duckdb_value=str(dv),
                    spark_value=str(sv),
                    description="+Inf 出现但策略=HUMAN_REVIEW/UNKNOWN——无法自动判定",
                )

            # 负 Inf
            strategy = manifest.neg_inf_handling
            both_neg_inf = dv_neg_inf and sv_neg_inf
            if both_neg_inf and strategy == SpecialFloatStrategy.EQUAL:
                return CellComparisonResult(
                    reason=NotToleratedReason.WITHIN_TOLERANCE,
                    duckdb_value="-Inf" if dv_neg_inf else str(dv),
                    spark_value="-Inf" if sv_neg_inf else str(sv),
                    description="双引擎均为 -Inf，策略=EQUAL",
                )
            if strategy == SpecialFloatStrategy.MISMATCH:
                return CellComparisonResult(
                    reason=NotToleratedReason.OUT_OF_TOLERANCE,
                    duckdb_value=str(dv),
                    spark_value=str(sv),
                    description="-Inf 出现，策略=MISMATCH",
                )
            if strategy == SpecialFloatStrategy.EQUAL:
                return CellComparisonResult(
                    reason=NotToleratedReason.OUT_OF_TOLERANCE,
                    duckdb_value=str(dv),
                    spark_value=str(sv),
                    description="-Inf vs 非 -Inf，策略=EQUAL 但值不同",
                )
            return CellComparisonResult(
                reason=NotToleratedReason.RULES_UNKNOWN,
                duckdb_value=str(dv),
                spark_value=str(sv),
                description="-Inf 出现但策略=HUMAN_REVIEW/UNKNOWN——无法自动判定",
            )

        # 不应到达——应已经被外层过滤
        return CellComparisonResult(
            reason=NotToleratedReason.UNKNOWN_CAUSE,
            duckdb_value=str(dv),
            spark_value=str(sv),
        )

    @classmethod
    def compare_row(
        cls,
        duckdb_row: dict[str, Any],
        spark_row: dict[str, Any],
        config: CreConfig,
        encoder: CREEncoder,
    ) -> ComparisonRowResult:
        """比较一行对齐的双引擎数据。

        先尝试 exact row digest 匹配（最快路径），
        不匹配时逐列执行容差比较。
        """
        # Step 1: exact row digest？
        d_bytes = encoder.encode_row(duckdb_row)
        s_bytes = encoder.encode_row(spark_row)
        if d_bytes == s_bytes:
            return ComparisonRowResult(exact_match=True)

        # Step 2: 逐列容差比较
        norm_d = {_normalize_name(k): v for k, v in duckdb_row.items()}
        norm_s = {_normalize_name(k): v for k, v in spark_row.items()}

        cell_results: list[CellComparisonResult] = []
        for col_def in config.output_columns:
            norm_name = _normalize_name(col_def.column_name)
            d_val = norm_d.get(norm_name)
            s_val = norm_s.get(norm_name)

            result = cls._compare_cell(
                d_val, s_val, col_def.data_type or "", config,
            )
            if result is not None:
                # 设置 column 名称
                result.column = col_def.column_name
                result.data_type = col_def.data_type or ""
                cell_results.append(result)

        return ComparisonRowResult(exact_match=False, cell_results=cell_results)

    @classmethod
    def _compare_cell(
        cls,
        duckdb_val: Any,
        spark_val: Any,
        data_type: str,
        config: CreConfig,
    ) -> CellComparisonResult | None:
        """比较单个单元格。返回 None 表示两值等价。"""
        # NULL vs NULL → 等价
        if duckdb_val is None and spark_val is None:
            return None
        # NULL vs 有值 → UNKNOWN_CAUSE 差异
        if duckdb_val is None or spark_val is None:
            return CellComparisonResult(
                reason=NotToleratedReason.UNKNOWN_CAUSE,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        family = cls._get_type_family(data_type)

        # ── 无 data_type ──
        if family == "UNKNOWN" and not data_type:
            return CellComparisonResult(
                reason=NotToleratedReason.NO_TYPE_INFO,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )
        if family == "UNKNOWN":
            return CellComparisonResult(
                reason=NotToleratedReason.RULES_UNKNOWN,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── FLOAT / DOUBLE：math.isclose + 特殊浮点值处理 ──
        if family in ("FLOAT", "DOUBLE"):
            try:
                dv = float(duckdb_val)
                sv = float(spark_val)

                # 检查 NaN / ±Infinity
                dv_nan = math.isnan(dv)
                sv_nan = math.isnan(sv)
                dv_inf = math.isinf(dv)
                sv_inf = math.isinf(sv)
                has_special = dv_nan or sv_nan or dv_inf or sv_inf

                if has_special:
                    manifest = config.environment_manifest
                    if manifest is None:
                        return CellComparisonResult(
                            reason=NotToleratedReason.RULES_UNKNOWN,
                            duckdb_value=str(duckdb_val),
                            spark_value=str(spark_val),
                            description="环境清单缺失——无法判定 NaN/Inf",
                        )
                    return cls._compare_special_float(
                        dv, sv, dv_nan, sv_nan, dv_inf, sv_inf, manifest,
                    )

                if math.isclose(
                    dv, sv,
                    rel_tol=config.float_rel_tolerance,
                    abs_tol=config.float_abs_tolerance,
                ):
                    return CellComparisonResult(
                        reason=NotToleratedReason.WITHIN_TOLERANCE,
                        duckdb_value=str(duckdb_val),
                        spark_value=str(spark_val),
                        abs_error=abs(dv - sv),
                        rel_error=abs(dv - sv) / max(abs(dv), abs(sv), 1e-300),
                    )
                else:
                    return CellComparisonResult(
                        reason=NotToleratedReason.OUT_OF_TOLERANCE,
                        duckdb_value=str(duckdb_val),
                        spark_value=str(spark_val),
                        abs_error=abs(dv - sv),
                        rel_error=abs(dv - sv) / max(abs(dv), abs(sv), 1e-300),
                    )
            except (ValueError, TypeError):
                return CellComparisonResult(
                    reason=NotToleratedReason.UNKNOWN_CAUSE,
                    duckdb_value=str(duckdb_val),
                    spark_value=str(spark_val),
                )

        # ── DECIMAL：quantize 后精确 == ──
        if family == "DECIMAL":
            m = re.match(
                r"(decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
                data_type.strip().lower(),
            )
            if m:
                scale = int(m.group(3))
                qs = "0." + "0" * scale if scale > 0 else "1"
                quant = Decimal(qs)
                try:
                    dd = Decimal(str(duckdb_val)).quantize(quant, rounding=ROUND_HALF_UP)
                    sd = Decimal(str(spark_val)).quantize(quant, rounding=ROUND_HALF_UP)
                    if dd == sd:
                        return CellComparisonResult(
                            reason=NotToleratedReason.WITHIN_TOLERANCE,
                            duckdb_value=str(duckdb_val),
                            spark_value=str(spark_val),
                        )
                    else:
                        return CellComparisonResult(
                            reason=NotToleratedReason.OUT_OF_TOLERANCE,
                            duckdb_value=str(duckdb_val),
                            spark_value=str(spark_val),
                            abs_error=float(dd - sd),
                        )
                except Exception:
                    return CellComparisonResult(
                        reason=NotToleratedReason.UNKNOWN_CAUSE,
                        duckdb_value=str(duckdb_val),
                        spark_value=str(spark_val),
                    )
            # 有 type_family=DECIMAL 但无法解析 p/s
            return CellComparisonResult(
                reason=NotToleratedReason.RULES_UNKNOWN,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── 整数类型：精确 ==（无容差）──
        if family in ("INT8", "INT16", "INT32", "INT64"):
            try:
                if int(duckdb_val) == int(spark_val):
                    return None
                else:
                    return CellComparisonResult(
                        reason=NotToleratedReason.INTEGER_MISMATCH,
                        duckdb_value=str(duckdb_val),
                        spark_value=str(spark_val),
                    )
            except (ValueError, TypeError):
                return CellComparisonResult(
                    reason=NotToleratedReason.UNKNOWN_CAUSE,
                    duckdb_value=str(duckdb_val),
                    spark_value=str(spark_val),
                )

        # ── BOOLEAN：精确 ==（拒绝 str("false")→True 陷阱）──
        if family == "BOOLEAN":
            def _to_bool(v: Any) -> bool | None:
                """严格布尔解析——只接受 bool、int(0/1)、白名单字符串。"""
                if isinstance(v, bool):
                    return v
                if isinstance(v, int):
                    if v in (0, 1):
                        return bool(v)
                    return None  # 非 0/1 整数视为非法
                if isinstance(v, str):
                    vs = v.strip().lower()
                    if vs in ("true", "1", "yes", "t"):
                        return True
                    if vs in ("false", "0", "no", "f"):
                        return False
                    return None  # 非法字符串
                return None

            db = _to_bool(duckdb_val)
            sb = _to_bool(spark_val)
            if db is None or sb is None:
                return CellComparisonResult(
                    reason=NotToleratedReason.UNKNOWN_CAUSE,
                    duckdb_value=str(duckdb_val),
                    spark_value=str(spark_val),
                )
            if db == sb:
                return None
            return CellComparisonResult(
                reason=NotToleratedReason.BOOL_MISMATCH,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── VARCHAR：精确 str == ──
        if family == "VARCHAR":
            if str(duckdb_val) == str(spark_val):
                return None
            return CellComparisonResult(
                reason=NotToleratedReason.STRING_MISMATCH,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── DATE：精确 == ──
        if family == "DATE":
            import datetime as _dt
            try:
                def _to_days(v):
                    if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
                        return (v - _dt.date(1970, 1, 1)).days
                    return int(v)
                if _to_days(duckdb_val) == _to_days(spark_val):
                    return None
            except Exception:
                pass
            return CellComparisonResult(
                reason=NotToleratedReason.DATE_MISMATCH,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── TIMESTAMP：精确 ==（时区由 CreConfig.timezone 保证）──
        if family == "TIMESTAMP":
            import datetime as _dt
            tz_str = config.timezone
            try:
                def _check_dst(v: _dt.datetime) -> None:
                    """检查 DST 歧义——fold=0 和 fold=1 的 UTC 应一致。"""
                    if v.tzinfo is not None:
                        return
                    if not tz_str:
                        return
                    tz = zoneinfo.ZoneInfo(tz_str)
                    u0 = v.replace(tzinfo=tz, fold=0).astimezone(_dt.timezone.utc)
                    u1 = v.replace(tzinfo=tz, fold=1).astimezone(_dt.timezone.utc)
                    if u0 != u1:
                        raise ValueError(
                            f"DST 歧义：{v.isoformat()} 在 '{tz_str}' 中不唯一"
                        )

                def _to_micros(v):
                    if isinstance(v, _dt.datetime):
                        e = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
                        if v.tzinfo is None:
                            if not tz_str:
                                raise ValueError("未配置 timezone")
                            _check_dst(v)
                            tz = zoneinfo.ZoneInfo(tz_str)
                            v = v.replace(tzinfo=tz)
                        v_utc = v.astimezone(_dt.timezone.utc)
                        return int((v_utc - e).total_seconds() * 1_000_000)
                    return int(v)
                if _to_micros(duckdb_val) == _to_micros(spark_val):
                    return None
            except Exception:
                pass
            return CellComparisonResult(
                reason=NotToleratedReason.TIMESTAMP_MISMATCH,
                duckdb_value=str(duckdb_val),
                spark_value=str(spark_val),
            )

        # ── COMPLEX 或其他：无规则 ──
        return CellComparisonResult(
            reason=NotToleratedReason.RULES_UNKNOWN,
            duckdb_value=str(duckdb_val),
            spark_value=str(spark_val),
        )
