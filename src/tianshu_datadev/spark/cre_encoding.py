"""CRE v2 原型——编码层。

模块职责：
- 类型枚举（NotToleratedReason）
- 列映射辅助函数（_normalize_name, _type_family, _TOLERANCE_RULE_DESC）
- CRE 编码器（CREEncoder）

共享模型（EnvironmentManifest / CreShadowReport / CreConfig 等）已移到
cre_models.py 中立模块——消除 cre_encoding ↔ physical_verifier 循环依赖。
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import re
import struct
import zoneinfo
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Any

# 共享模型——从中立模块 cre_models 导入，消除 cre_encoding ↔ physical_verifier 循环依赖
from tianshu_datadev.cre_models import (  # noqa: I001
    CreConfig,
    CreShadowReport,
    CreShadowStatus,
    CreShadowWarning,
    DecimalStrategy,
    EnvironmentManifest,
    NormalizationColumn,
    NullStrategy,
    SpecialFloatStrategy,
    ToleratedDifferenceWarning,
    ToleratedFieldDetail,
)

# 重新导出——保持向后兼容，使外部导入 from cre_encoding import ... 仍然有效
__all__ = [
    "NotToleratedReason",
    "_normalize_name",
    "_type_family",
    "_TOLERANCE_RULE_DESC",
    "CREEncoder",
    # 重新导出共享模型（向后兼容）
    "CreConfig",
    "CreShadowReport",
    "CreShadowStatus",
    "CreShadowWarning",
    "DecimalStrategy",
    "EnvironmentManifest",
    "NormalizationColumn",
    "NullStrategy",
    "SpecialFloatStrategy",
    "ToleratedFieldDetail",
    "ToleratedDifferenceWarning",
]

# ════════════════════════════════════════════
# NotToleratedReason——不可容忍原因枚举
# ════════════════════════════════════════════


class NotToleratedReason(str, Enum):
    """不可容忍差异的原因分类。"""
    WITHIN_TOLERANCE = "WITHIN_TOLERANCE"                # 在容差内
    OUT_OF_TOLERANCE = "OUT_OF_TOLERANCE"                # 超容差
    STRING_MISMATCH = "STRING_MISMATCH"                  # 字符串不精确匹配
    INTEGER_MISMATCH = "INTEGER_MISMATCH"                # 整数不精确匹配
    BOOL_MISMATCH = "BOOL_MISMATCH"                      # 布尔值不匹配
    DATE_MISMATCH = "DATE_MISMATCH"                      # 日期不精确匹配
    TIMESTAMP_MISMATCH = "TIMESTAMP_MISMATCH"            # 时间戳不精确匹配
    RULES_UNKNOWN = "RULES_UNKNOWN"                      # 无匹配规则
    NO_TYPE_INFO = "NO_TYPE_INFO"                        # 缺少 data_type
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"                  # 额外/缺失列
    ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"            # 行数不一致
    PK_MISMATCH = "PK_MISMATCH"                          # 主键值不匹配
    DUPLICATE_KEYS = "DUPLICATE_KEYS"                    # 重复主键
    UNKNOWN_CAUSE = "UNKNOWN_CAUSE"                      # 无法分类


# ════════════════════════════════════════════
# 列映射辅助
# ════════════════════════════════════════════


def _normalize_name(name: str) -> str:
    """列名归一化——去除表前缀、转小写、去空格。"""
    if "." in name:
        name = name.split(".")[-1]
    return name.strip().lower()


def _type_family(data_type: str) -> str:
    """从 data_type 字符串推断类型族。

    返回值：BOOLEAN / INT8 / INT16 / INT32 / INT64 / FLOAT / DOUBLE
            DECIMAL / VARCHAR / DATE / TIMESTAMP / COMPLEX / UNKNOWN
    """
    dt = data_type.strip().lower()

    # 布尔
    if dt in ("boolean", "bool"):
        return "BOOLEAN"
    # 整型
    if dt in ("tinyint", "int8", "byte"):
        return "INT8"
    if dt in ("smallint", "int16", "short"):
        return "INT16"
    if dt in ("int", "integer", "int32", "signed"):
        return "INT32"
    if dt in ("bigint", "int64", "long"):
        return "INT64"
    # 浮点
    if dt in ("float", "real", "float4"):
        return "FLOAT"
    if dt in ("double", "float8"):
        return "DOUBLE"
    # Decimal
    if dt.startswith("decimal") or dt.startswith("numeric"):
        return "DECIMAL"
    # 字符串
    if dt in ("varchar", "string", "text", "char"):
        return "VARCHAR"
    # 日期时间
    if dt in ("date",):
        return "DATE"
    if dt in ("timestamp", "datetime", "timestamptz", "timestamp with time zone"):
        return "TIMESTAMP"
    # 复杂类型
    if dt.startswith("array") or dt.startswith("struct") or dt.startswith("map"):
        return "COMPLEX"
    # 无匹配
    return "UNKNOWN"


# 容差规则描述（模块级——DecisionEngine 和 ToleranceComparator 共用）
_TOLERANCE_RULE_DESC: dict[str, str] = {
    "FLOAT": "math.isclose(rel_tol=1e-9, abs_tol=1e-12)",
    "DOUBLE": "math.isclose(rel_tol=1e-9, abs_tol=1e-12)",
    "DECIMAL": "Decimal.quantize(Contract.scale)",
}


# ════════════════════════════════════════════
# CREEncoder——类型感知的行编码器
# ════════════════════════════════════════════


class CREEncoder:
    """将单行 dict 编码为确定性的字节序列。

    编码格式：
      [4B]  magic        "CRE2" (0x43524532)
      [2B]  column_count  uint16 BE
      [4B]  total_length  uint32 BE（含本字段之前的所有字节）
      for each column in Contract output_columns 顺序:
        [1B] type_tag
        [N B] value_bytes

    类型标签（type_tag）：
      0x00 = NULL（任意类型）
      0x01 = BOOLEAN
      0x02 = INT8         0x03 = INT16
      0x04 = INT32        0x05 = INT64
      0x06 = FLOAT32      0x07 = FLOAT64
      0x08 = DECIMAL(p,s)
      0x09 = VARCHAR
      0x0A = DATE
      0x0B = TIMESTAMP
    """

    _MAGIC = b"CRE2"

    def __init__(self, config: CreConfig):
        self._config = config
        # 归一化列名 → NormalizationColumn 映射
        self._col_map: dict[str, NormalizationColumn] = {}
        for col in config.output_columns:
            self._col_map[_normalize_name(col.column_name)] = col

        # 验证时间戳支持
        self._has_timestamp = any(
            _type_family(col.data_type or "") == "TIMESTAMP"
            for col in config.output_columns
        )
        if self._has_timestamp and not config.timezone:
            raise ValueError(
                "Contract 包含 timestamp 列但未配置 timezone——"
                "无法确定时区，禁止编码"
            )

    @staticmethod
    def _encode_bool(value: Any) -> bytes:
        """编码布尔值——只接受 bool、int(0/1)、或白名单字符串。

        白名单（大小写不敏感）：
        - True: "true", "1", "yes", "t"
        - False: "false", "0", "no", "f"
        - 其他字符串 → ValueError
        - int 仅接受 0=False 和 1=True，其他整数 → ValueError
        """
        if isinstance(value, bool):
            return b"\x01" + (b"\x01" if value else b"\x00")
        if isinstance(value, int):
            if value not in (0, 1):
                raise ValueError(
                    f"不支持的布尔整数值：{value}——"
                    f"仅接受 0(False) 和 1(True)"
                )
            return b"\x01" + (b"\x01" if value else b"\x00")
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "1", "yes", "t"):
                return b"\x01" + b"\x01"
            if v in ("false", "0", "no", "f"):
                return b"\x01" + b"\x00"
            raise ValueError(
                f"不支持的布尔字符串值：'{value}'——"
                f"仅接受 true/false/1/0/yes/no/t/f（大小写不敏感）"
            )
        raise ValueError(
            f"不支持的布尔值类型：{type(value)}:{value}——"
            f"仅接受 bool、int(0/1)、或白名单字符串"
        )

    @staticmethod
    def _encode_int8(value: Any) -> bytes:
        return b"\x02" + int(int(value)).to_bytes(1, "big", signed=True)

    @staticmethod
    def _encode_int16(value: Any) -> bytes:
        return b"\x03" + int(int(value)).to_bytes(2, "big", signed=True)

    @staticmethod
    def _encode_int32(value: Any) -> bytes:
        return b"\x04" + int(int(value)).to_bytes(4, "big", signed=True)

    @staticmethod
    def _encode_int64(value: Any) -> bytes:
        return b"\x05" + int(int(value)).to_bytes(8, "big", signed=True)

    @staticmethod
    def _encode_float32(value: Any) -> bytes:
        return b"\x06" + struct.pack(">f", float(value))

    @staticmethod
    def _encode_float64(value: Any) -> bytes:
        return b"\x07" + struct.pack(">d", float(value))

    @staticmethod
    def _encode_decimal(value: Any, data_type: str) -> bytes:
        """编码 Decimal——按 Contract scale 量化后编码 unscaled 值。

        编码格式：
          [1B] type_tag=0x08
          [1B] precision
          [1B] scale
          [1B] unscaled_byte_length（无歧义——解码器知悉精准长度边界）
          [N B] unscaled_value（big-endian signed, variable length）

        校验：
        - precision 和 scale 在合理范围内（1<=p<=38, 0<=s<=p）
        - 量化后的 unscaled_value 不超过 10^p - 1（precision 约束）
        - 正负数边界：negative zero 归一化为 b"\\x00"
        """
        m = re.match(
            r"(decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
            data_type.strip().lower(),
        )
        if not m:
            raise ValueError(
                f"无法从 data_type='{data_type}' 解析 Decimal precision/scale"
            )
        precision = int(m.group(2))
        scale = int(m.group(3))

        # 校验 precision/scale 范围
        if not (1 <= precision <= 38):
            raise ValueError(
                f"Decimal precision={precision} 超出范围 [1, 38]——"
                f"data_type='{data_type}'"
            )
        if not (0 <= scale <= precision):
            raise ValueError(
                f"Decimal scale={scale} 超出范围 [0, precision={precision}]——"
                f"data_type='{data_type}'"
            )

        d = Decimal(str(value))
        quantize_str = "0." + "0" * scale if scale > 0 else "1"
        d = d.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        unscaled = int(d * Decimal(10) ** scale)

        # 校验 unscaled 值不超过 precision 约束
        max_unscaled = Decimal(10) ** precision
        abs_unscaled = abs(Decimal(unscaled))
        if abs_unscaled >= max_unscaled:
            raise ValueError(
                f"Decimal 值 {value} 量化后 unscaled={unscaled} 超出 "
                f"precision={precision} 约束（最大 {max_unscaled - 1}）——"
                f"data_type='{data_type}'"
            )

        if unscaled == 0:
            unscaled_bytes = b"\x00"
        else:
            byte_len = (unscaled.bit_length() + 8) // 8
            unscaled_bytes = unscaled.to_bytes(byte_len, "big", signed=True)

        # 验证编码的正负数边界
        if len(unscaled_bytes) > 255:
            raise ValueError(
                f"Decimal unscaled_value 编码长度 {len(unscaled_bytes)} "
                f"超过 255 字节上限——无法编码"
            )

        return (
            b"\x08"
            + precision.to_bytes(1, "big")
            + scale.to_bytes(1, "big")
            + bytes([len(unscaled_bytes)])
            + unscaled_bytes
        )

    @staticmethod
    def _encode_varchar(value: Any) -> bytes:
        encoded = str(value).encode("utf-8")
        return b"\x09" + len(encoded).to_bytes(4, "big") + encoded

    @staticmethod
    def _encode_date(value: Any) -> bytes:
        if isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
            epoch = _dt.date(1970, 1, 1)
            days = (value - epoch).days
        elif isinstance(value, str):
            d = _dt.date.fromisoformat(value)
            epoch = _dt.date(1970, 1, 1)
            days = (d - epoch).days
        elif isinstance(value, (int, float)):
            days = int(value)
        else:
            raise ValueError(f"不支持的 date 值类型：{type(value)}:{value}")
        return b"\x0a" + days.to_bytes(4, "big", signed=True)

    @staticmethod
    def _check_dst_ambiguity(dt_naive: _dt.datetime, tz_str: str) -> None:
        """检查 naive datetime 在目标时区是否存在 DST 歧义。

        通过比较 fold=0 和 fold=1 两种解释对应的 UTC 时间是否一致来判断。
        不一致表示该 wall clock 时间在 DST 过渡期间不唯一（重叠/不存在）。

        Raises:
            ValueError: 存在 DST 歧义，需人工介入
        """
        tz = zoneinfo.ZoneInfo(tz_str)
        dt_fold0 = dt_naive.replace(tzinfo=tz, fold=0)
        dt_fold1 = dt_naive.replace(tzinfo=tz, fold=1)
        utc0 = dt_fold0.astimezone(_dt.timezone.utc)
        utc1 = dt_fold1.astimezone(_dt.timezone.utc)
        if utc0 != utc1:
            raise ValueError(
                f"Timestamp {dt_naive.isoformat()} 在时区 '{tz_str}' 中存在 "
                f"DST 歧义——无法唯—确定 UTC 时间，需人工介入"
            )

    def _encode_timestamp(self, value: Any) -> bytes:
        """编码 timestamp——使用 CreConfig.timezone 进行时区转换。

        流程：
        1. 若 value 有时区信息 → 转换到 UTC
        2. 若 value 为 naive datetime → 按配置时区 localize，再转 UTC
        3. 若为 int/float → 直接视为 UTC 微秒数

        禁止把 naive datetime 默认视为 UTC。
        DST 歧义检测：重叠/不存在时间 → ValueError。
        """
        tz_str = self._config.timezone
        epoch = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)

        if isinstance(value, _dt.datetime):
            if value.tzinfo is None:
                # naive datetime → 按配置时区 localize（禁止默认 UTC）
                if not tz_str:
                    raise ValueError(
                        "Timestamp 列为 naive datetime 但未配置 timezone——"
                        "无法确定时区，禁止自动编码"
                    )
                # DST 歧义检测
                self._check_dst_ambiguity(value, tz_str)
                tz = zoneinfo.ZoneInfo(tz_str)
                value = value.replace(tzinfo=tz)
            # 转换到 UTC 计算微秒
            value_utc = value.astimezone(_dt.timezone.utc)
            micros = int((value_utc - epoch).total_seconds() * 1_000_000)
        elif isinstance(value, str):
            dt_val = _dt.datetime.fromisoformat(value)
            if dt_val.tzinfo is None:
                if not tz_str:
                    raise ValueError(
                        "Timestamp 字符串为 naive datetime 但未配置 timezone——"
                        "无法确定时区，禁止自动编码"
                    )
                # DST 歧义检测
                self._check_dst_ambiguity(dt_val, tz_str)
                tz = zoneinfo.ZoneInfo(tz_str)
                dt_val = dt_val.replace(tzinfo=tz)
            value_utc = dt_val.astimezone(_dt.timezone.utc)
            micros = int((value_utc - epoch).total_seconds() * 1_000_000)
        elif isinstance(value, (int, float)):
            micros = int(value)
        else:
            raise ValueError(f"不支持的 timestamp 值类型：{type(value)}")
        return b"\x0b" + micros.to_bytes(8, "big", signed=True)

    def encode_value(self, value: Any, data_type: str | None) -> bytes:
        """将单个值按 data_type 编码为 CRE 字节序列。"""
        if value is None:
            return b"\x00"  # type_tag=0x00 NULL

        family = _type_family(data_type or "") if data_type else "UNKNOWN"

        if family == "BOOLEAN":
            return self._encode_bool(value)
        elif family == "INT8":
            return self._encode_int8(value)
        elif family == "INT16":
            return self._encode_int16(value)
        elif family == "INT32":
            return self._encode_int32(value)
        elif family == "INT64":
            return self._encode_int64(value)
        elif family == "FLOAT":
            return self._encode_float32(value)
        elif family == "DOUBLE":
            return self._encode_float64(value)
        elif family == "DECIMAL":
            return self._encode_decimal(value, data_type or "")
        elif family == "VARCHAR":
            return self._encode_varchar(value)
        elif family == "DATE":
            return self._encode_date(value)
        elif family == "TIMESTAMP":
            return self._encode_timestamp(value)
        elif family == "COMPLEX":
            raise ValueError(
                f"不支持的数据类型族 COMPLEX——无比较规则。"
                f"data_type='{data_type}'"
            )
        else:
            raise ValueError(
                f"不支持的 data_type='{data_type}'（family={family}）——"
                f"缺少类型映射规则"
            )

    def encode_row(self, row: dict[str, Any]) -> bytes:
        """将整行按 Contract 列顺序编码为完整的 CRE 字节序列。

        含 magic header + column_count + total_length + 各列编码。

        Raises:
            ValueError: Contract output_columns 为空，或某列缺少必要信息
        """
        col_count = len(self._config.output_columns)
        if col_count == 0:
            raise ValueError("Contract output_columns 为空，无法编码")

        norm_row = {_normalize_name(k): v for k, v in row.items()}

        payload_parts: list[bytes] = []
        for col_def in self._config.output_columns:
            norm_name = _normalize_name(col_def.column_name)
            value = norm_row.get(norm_name)
            encoded = self.encode_value(value, col_def.data_type)
            payload_parts.append(encoded)

        payload = b"".join(payload_parts)
        header = self._MAGIC + col_count.to_bytes(2, "big")
        total_length = len(header) + 4 + len(payload)
        header += total_length.to_bytes(4, "big")

        return header + payload

    def encode_pk_bytes(self, row: dict[str, Any]) -> bytes:
        """仅编码主键列为字节序列——用于对齐和分桶。

        编码格式（轻量，无 magic）：
          for each primary key column in Contract order:
            [1B] type_tag
            [N B] value_bytes
        """
        pk_cols = self._config.primary_keys
        if not pk_cols:
            raise ValueError("CreConfig.primary_keys 为空——无法编码主键")

        norm_row = {_normalize_name(k): v for k, v in row.items()}
        parts: list[bytes] = []
        for pk_name in pk_cols:
            col_def = self._col_map.get(pk_name)
            if col_def is None:
                raise ValueError(
                    f"主键列 '{pk_name}' 不在 Contract output_columns 中"
                )
            value = norm_row.get(pk_name)
            parts.append(self.encode_value(value, col_def.data_type))

        return b"".join(parts)

    def pk_digest(self, row: dict[str, Any]) -> str:
        """计算行的主键 digest（SHA-256）。"""
        return hashlib.sha256(self.encode_pk_bytes(row)).hexdigest()
