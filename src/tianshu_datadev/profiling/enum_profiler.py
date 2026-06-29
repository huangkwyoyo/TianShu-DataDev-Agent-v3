"""EnumProfiler——枚举值自动检测核心。

从采样数据自动判定字段是否属于枚举值字段（Flag/Status/Code 三类），
输出分层置信度（CERTAIN/HIGH/MEDIUM/LOW/NOT_ENUM）。

不依赖数据库——接收已采样的列值列表，保持确定性和可测试性。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .models import (
    EnumConfidenceTier,
    EnumDetectionResult,
    EnumFieldClass,
    EnumProfile,
)

# ════════════════════════════════════════════
# 字段名信号词典
# ════════════════════════════════════════════

# Flag 字段名模式——列名暗示标志位
_FLAG_NAME_PATTERNS: list[str] = [
    "is_*", "has_*", "flag_*", "*_flag", "*_yn",
    "是否*", "*_是否",
]

# Status 字段名模式——列名暗示状态
_STATUS_NAME_PATTERNS: list[str] = [
    "*_status", "*_state", "status_*", "*_phase", "*_stage",
    "状态", "*_状态",
]

# Code 字段名模式——列名暗示分类代码
_CODE_NAME_PATTERNS: list[str] = [
    "*_type", "*_code", "*_category", "*_class", "*_kind", "*_level",
    "type_*", "code_*",
    "类型", "分类", "级别",
]

# 特殊排除字段名——不应被检测为 Code 的字段
_EXCLUSION_NAME_PATTERNS: list[str] = [
    # 时间维度
    "*year*", "*_yr", "*_yr_*", "年份", "年度",
    "*month*", "*_mon", "*_mon_*", "月份", "月",
    # 金额/数量
    "*amount*", "*_amt", "*price*", "*_price",
    "*qty*", "*quantity*", "*count*", "*_cnt",
    "金额", "数量", "价格",
    # 年龄/天数
    "*age*", "*_days", "*day_*",
]

# Status 词典——典型状态词
_STATUS_DICT: frozenset[str] = frozenset({
    "approved", "pending", "active", "inactive",
    "complete", "completed", "draft", "closed", "open",
    "done", "failed", "success", "processing", "processed",
    "submitted", "received", "shipped", "delivered",
    "enabled", "disabled", "valid", "invalid",
    "new", "in_progress", "in-progress", "in progress",
    "on_hold", "on-hold", "on hold", "cancelled", "canceled",
})

# ════════════════════════════════════════════
# Flag 值模式
# ════════════════════════════════════════════

_FLAG_VALUE_SETS: list[frozenset[str]] = [
    frozenset({"0", "1"}),
    frozenset({"0", "1", "true", "false"}),  # 允许小写 true/false
    frozenset({"y", "n"}),
    frozenset({"yes", "no"}),
    frozenset({"true", "false"}),
    frozenset({"是", "否"}),
]

# ════════════════════════════════════════════
# 正则模式
# ════════════════════════════════════════════

# Status 值模式：首字母大写 + 可选空格/连字符分隔多词
_STATUS_VALUE_RE = re.compile(
    r"^[A-Z][a-zA-Z]*([\s\-][A-Z][a-zA-Z]*)*$"
)

# Code 值模式：纯数字（2-6 位）或大写字母缩写（2-6 位）
_CODE_NUMERIC_RE = re.compile(r"^\d{1,6}$")
_CODE_ALPHA_RE = re.compile(r"^[A-Z]{2,6}$")

# 阈值常量
_MAX_FLAG_DISTINCT = 2
_MAX_STATUS_DISTINCT = 30
_MAX_CODE_DISTINCT = 50
_MAX_DISTINCT_RATIO_STATUS = 0.1
_MAX_DISTINCT_RATIO_CODE = 0.05
_MIN_SAMPLE_FOR_RATIO = 10  # ratio 检查生效的最低样本量——低于此值不检查 ratio
_RATIO_HIGH = 0.9
_RATIO_MEDIUM = 0.8
_RATIO_LOW = 0.6
_MIN_STATUS_LEN = 3
_MAX_STATUS_LEN = 25


@dataclass
class ColumnSample:
    """单列采样数据——传递给 EnumProfiler 的最小输入。

    不依赖 DB 连接——调用方负责执行 SELECT DISTINCT + LIMIT 并填充数据。
    """

    table_ref: str
    column_name: str
    normalized_name: str
    values: list[Any] = field(default_factory=list)


class EnumProfiler:
    """枚举值自动检测器——从采样数据判定字段枚举类型和置信度。

    使用方式：
        samples = [ColumnSample(table_ref="tf", column_name="status", ...)]
        result = EnumProfiler().profile(samples)
        for p in result.profiles:
            if p.tier in (EnumConfidenceTier.CERTAIN, EnumConfidenceTier.HIGH):
                print(p.detected_values)

    确定性保证：相同输入 → 相同输出。不涉及随机采样或模型推理。
    """

    def profile(self, samples: list[ColumnSample]) -> EnumDetectionResult:
        """对一组列采样执行枚举检测。

        Args:
            samples: 列采样列表——每个 ColumnSample 包含列元信息和采样值

        Returns:
            EnumDetectionResult——含每个字段的 EnumProfile
        """
        from datetime import datetime, timezone

        profiles: list[EnumProfile] = []
        tables: set[str] = set()

        for sample in samples:
            tables.add(sample.table_ref)
            profile = self._profile_one(sample)
            profiles.append(profile)

        return EnumDetectionResult(
            profiles=profiles,
            sampled_tables=sorted(tables),
            sample_timestamp=datetime.now(timezone.utc).isoformat(),
        )

    # ── 单列检测 ──

    def _profile_one(self, sample: ColumnSample) -> EnumProfile:
        """对单个列执行枚举检测——按 Flag → Status → Code 顺序尝试。"""
        # 过滤掉 None 值——不影响模式检测
        clean_values = [v for v in sample.values if v is not None]
        total = len(sample.values)
        distinct = len(set(str(v) for v in clean_values))

        # 全 NULL 列——跳过
        if not clean_values:
            return EnumProfile(
                table_ref=sample.table_ref,
                column_name=sample.column_name,
                normalized_name=sample.normalized_name,
                tier=EnumConfidenceTier.NOT_ENUM,
                distinct_count=0,
                total_sampled=total,
                exclusions=["all_null"],
            )

        # 特殊排除检查——必须在模式匹配之前执行
        exclusion = _check_exclusion(sample.normalized_name)
        if exclusion:
            return EnumProfile(
                table_ref=sample.table_ref,
                column_name=sample.column_name,
                normalized_name=sample.normalized_name,
                tier=EnumConfidenceTier.NOT_ENUM,
                distinct_count=distinct,
                total_sampled=total,
                exclusions=[exclusion],
            )

        # 按优先级依次检测：Flag → Status → Code
        # 若任一检测器返回非 None 结果（含 NOT_ENUM），即停止——
        # 字段名信号已确认此列属于某类别，即使数据不匹配也不穿透到下一类别。
        for detector in [_detect_flag, _detect_status, _detect_code]:
            profile = detector(
                clean_values, distinct, total,
                sample.table_ref, sample.column_name,
                sample.normalized_name,
            )
            if profile is not None:
                return profile

        # 所有检测器均未匹配（全部返回 None）——NOT_ENUM
        return EnumProfile(
            table_ref=sample.table_ref,
            column_name=sample.column_name,
            normalized_name=sample.normalized_name,
            tier=EnumConfidenceTier.NOT_ENUM,
            distinct_count=distinct,
            total_sampled=total,
        )


# ════════════════════════════════════════════
# 检测器函数
# ════════════════════════════════════════════


def _detect_flag(
    clean_values: list[Any],
    distinct: int,
    total: int,
    table_ref: str,
    column_name: str,
    normalized_name: str,
) -> EnumProfile | None:
    """检测 Flag（标志位）。

    Flag 是二值判定——满足必达条件即为 CERTAIN。
    若字段名明确暗示为 Flag 但数据不匹配 → 返回 NOT_ENUM（不继续探测其他类别）。
    """
    has_name_signal = _match_field_name(normalized_name, _FLAG_NAME_PATTERNS)

    if distinct > _MAX_FLAG_DISTINCT:
        # 字段名明确是 Flag 但 distinct 超限 → 拒绝（不穿透到 Status/Code）
        if has_name_signal:
            return EnumProfile(
                table_ref=table_ref,
                column_name=column_name,
                normalized_name=normalized_name,
                tier=EnumConfidenceTier.NOT_ENUM,
                distinct_count=distinct,
                total_sampled=total,
                exclusions=["flag_name_mismatch:distinct>2"],
            )
        return None

    str_values = {str(v).lower().strip() for v in clean_values}

    # 检查是否匹配任一 Flag 值集合
    for flag_set in _FLAG_VALUE_SETS:
        if str_values.issubset(flag_set):
            signals: list[str] = [f"flag_pattern:{sorted(str_values)}"]
            tier = EnumConfidenceTier.CERTAIN

            if not has_name_signal:
                tier = EnumConfidenceTier.LOW
                signals.append("no_field_name_signal")
            else:
                signals.append("field_name:flag")

            return EnumProfile(
                table_ref=table_ref,
                column_name=column_name,
                normalized_name=normalized_name,
                field_class=EnumFieldClass.FLAG,
                detected_values=sorted(str_values),
                distinct_count=distinct,
                total_sampled=total,
                tier=tier,
                pattern_match_ratio=1.0,
                signals=signals,
            )

    # 字段名暗示 Flag 但值不匹配任何 Flag 模式 → 拒绝
    if has_name_signal:
        return EnumProfile(
            table_ref=table_ref,
            column_name=column_name,
            normalized_name=normalized_name,
            tier=EnumConfidenceTier.NOT_ENUM,
            distinct_count=distinct,
            total_sampled=total,
            exclusions=["flag_name_mismatch:value_not_flag"],
        )

    return None


def _detect_status(
    clean_values: list[Any],
    distinct: int,
    total: int,
    table_ref: str,
    column_name: str,
    normalized_name: str,
) -> EnumProfile | None:
    """检测 Status（状态码）。"""
    # 低基数列筛选（ratio 仅在大样本时生效——小样本允许高比例）
    if distinct > _MAX_STATUS_DISTINCT:
        return None
    if total >= _MIN_SAMPLE_FOR_RATIO and distinct / total > _MAX_DISTINCT_RATIO_STATUS:
        return None

    str_values = [str(v).strip() for v in clean_values]
    unique_values = sorted(set(str_values))

    # 值平均长度检查
    avg_len = sum(len(v) for v in unique_values) / len(unique_values)
    if avg_len < _MIN_STATUS_LEN or avg_len > _MAX_STATUS_LEN:
        return None

    # 全大写短缩写排除——PDR/ADR/OTH 等典型 Code 不应误判为 Status
    # 规则：若 ≥80% 值为 2-4 字符全大写 → 更可能是 Code
    short_upper_count = sum(
        1 for v in unique_values
        if 2 <= len(v) <= 4 and v.isupper() and v.isalpha()
    )
    if short_upper_count / len(unique_values) >= 0.8:
        return None

    # 模式匹配率
    match_count = sum(1 for v in unique_values if _STATUS_VALUE_RE.match(v))
    ratio = match_count / len(unique_values) if unique_values else 0

    if ratio < _RATIO_LOW:
        return None

    # 收集信号
    signals: list[str] = []
    has_name_signal = _match_field_name(normalized_name, _STATUS_NAME_PATTERNS)
    if has_name_signal:
        signals.append("field_name:status")

    # 词典命中检查
    dict_hits = [v for v in unique_values if v.lower().replace("-", "_") in _STATUS_DICT]
    has_dict_hit = len(dict_hits) > 0
    if has_dict_hit:
        signals.append(f"dict:hit:{dict_hits[0]}")

    # 分层判定
    tier = _classify_tier(ratio, has_name_signal, has_dict_hit, total)

    return EnumProfile(
        table_ref=table_ref,
        column_name=column_name,
        normalized_name=normalized_name,
        field_class=EnumFieldClass.STATUS,
        detected_values=unique_values,
        distinct_count=distinct,
        total_sampled=total,
        tier=tier,
        pattern_match_ratio=ratio,
        signals=signals,
    )


def _detect_code(
    clean_values: list[Any],
    distinct: int,
    total: int,
    table_ref: str,
    column_name: str,
    normalized_name: str,
) -> EnumProfile | None:
    """检测 Code（分类代码）。

    Code 最易误判——年份/月份/金额字段必须被排除。
    """
    # distinct ≥ 3（Code 不同于 Flag 的 2 值）
    if distinct < 3:
        return None
    if distinct > _MAX_CODE_DISTINCT:
        return None
    if total >= _MIN_SAMPLE_FOR_RATIO and distinct / total > _MAX_DISTINCT_RATIO_CODE:
        return None

    str_values = [str(v).strip() for v in clean_values]
    unique_values = sorted(set(str_values))

    # 模式匹配——纯数字或大写字母缩写
    match_count = 0
    for v in unique_values:
        if _CODE_NUMERIC_RE.match(v) or _CODE_ALPHA_RE.match(v):
            match_count += 1

    ratio = match_count / len(unique_values) if unique_values else 0

    if ratio < _RATIO_LOW:
        return None

    # 收集信号
    signals: list[str] = []
    has_name_signal = _match_field_name(normalized_name, _CODE_NAME_PATTERNS)
    if has_name_signal:
        signals.append("field_name:code")

    # 统一长度检查（±1）
    lengths = {len(v) for v in unique_values}
    has_uniform_length = len(lengths) <= 2 and max(lengths) - min(lengths) <= 1
    if has_uniform_length:
        signals.append("uniform_length")

    # Code 没有词典命中概念——字段名信号是主要交叉验证

    # 分层判定（Code 用 has_name_signal 作为关键信号）
    tier = _classify_tier(ratio, has_name_signal, has_dict_hit=False, total=total)

    return EnumProfile(
        table_ref=table_ref,
        column_name=column_name,
        normalized_name=normalized_name,
        field_class=EnumFieldClass.CODE,
        detected_values=unique_values,
        distinct_count=distinct,
        total_sampled=total,
        tier=tier,
        pattern_match_ratio=ratio,
        signals=signals,
    )


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _match_field_name(normalized_name: str, patterns: list[str]) -> bool:
    """检查字段名是否匹配任一模式。

    支持 * 通配符：前缀、后缀、精确匹配。
    """
    name_lower = normalized_name.lower()
    for pattern in patterns:
        if pattern == name_lower:
            return True
        if pattern.startswith("*") and pattern.endswith("*"):
            # *contains*
            if pattern[1:-1] in name_lower:
                return True
        elif pattern.startswith("*"):
            # *suffix
            if name_lower.endswith(pattern[1:]):
                return True
        elif pattern.endswith("*"):
            # prefix*
            if name_lower.startswith(pattern[:-1]):
                return True
    return False


def _check_exclusion(normalized_name: str) -> str | None:
    """检查字段名是否命中特殊排除规则。

    Returns:
        排除规则描述字符串，未命中返回 None
    """
    # 年份/月份/金额/数量等排除
    for pattern in _EXCLUSION_NAME_PATTERNS:
        if _match_field_name(normalized_name, [pattern]):
            return f"excluded:{pattern}"
    return None


def _classify_tier(
    ratio: float,
    has_name_signal: bool,
    has_dict_hit: bool,
    total: int,
) -> EnumConfidenceTier:
    """根据信号积分判定置信度分层。

    不封顶小样本——tier 如实反映信号强度，
    采样不足的降级由 LabelValidator 集成层负责。
    """
    if ratio >= _RATIO_HIGH and has_name_signal:
        return EnumConfidenceTier.HIGH
    elif ratio >= _RATIO_MEDIUM and (has_name_signal or has_dict_hit):
        return EnumConfidenceTier.MEDIUM
    elif ratio >= _RATIO_LOW or (has_name_signal and ratio < _RATIO_MEDIUM):
        return EnumConfidenceTier.LOW
    else:
        return EnumConfidenceTier.NOT_ENUM
