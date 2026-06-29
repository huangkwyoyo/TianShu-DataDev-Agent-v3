"""枚举值自动检测数据模型——Phase 3B.1。

所有模型继承 StrictModel（extra="forbid"），确保序列化一致性。
"""

from __future__ import annotations

from enum import Enum

from tianshu_datadev.developer_spec.models import StrictModel


class EnumConfidenceTier(str, Enum):
    """枚举检测置信度分层——替代单点浮点数。

    分层依据：信号积分制——综合模式匹配率 + 字段名信号 + 特殊排除。
    """

    CERTAIN = "certain"    # Flag 必达条件全满足，或 DeveloperSpec 显式声明
    HIGH = "high"          # 模式匹配 ≥ 0.9 + 字段名信号命中 + 未被排除
    MEDIUM = "medium"      # 模式匹配 ≥ 0.8 + (字段名信号 或 词典命中)
    LOW = "low"            # 模式匹配 ≥ 0.6，或字段名信号命中但模式 < 0.8
    NOT_ENUM = "not_enum"  # 未通过筛选 或 被特殊排除


class EnumFieldClass(str, Enum):
    """枚举值字段分类。"""

    FLAG = "flag"        # 0/1、Y/N、YES/NO
    STATUS = "status"    # 固定英文短语
    CODE = "code"        # 字母缩写或纯数字


class EnumProfile(StrictModel):
    """单个字段的枚举值检测结果。

    不设 confidence: float——改用 tier 分层 + pattern_match_ratio 诊断值。
    signals 和 exclusions 记录命中/排除的信号，便于调试和人工审查。
    """

    table_ref: str
    column_name: str
    normalized_name: str
    field_class: EnumFieldClass | None = None  # None 表示 tier=NOT_ENUM
    detected_values: list[str] = []             # 检测到的所有枚举值
    distinct_count: int = 0                     # distinct 数
    total_sampled: int = 0                      # 采样行数
    tier: EnumConfidenceTier = EnumConfidenceTier.NOT_ENUM
    pattern_match_ratio: float = 0.0            # 模式匹配率（诊断用，非决策用）
    signals: list[str] = []                     # 命中的信号标签
    exclusions: list[str] = []                  # 触发的排除规则（空=未被排除）


class EnumDetectionResult(StrictModel):
    """一次 profiling 的完整结果。"""

    profiles: list[EnumProfile] = []
    sampled_tables: list[str] = []    # 实际采样了哪些表
    sample_timestamp: str = ""        # 采样时间戳
