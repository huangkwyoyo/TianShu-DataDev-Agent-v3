"""RelationshipValidator——确定性证据评级器。

LLM/Fake 只能提候选——等级由 Validator 确定性计算。
四级级联规则：STRONG → MEDIUM → WEAK → NONE，匹配即终止。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .relationship_hypothesis import EvidenceAction, JoinEvidenceLevel


@dataclass
class JoinSafetyTableInfo:
    """LEFT JOIN 安全门禁所需的表唯一性元数据。

    由 _build_join_safety_info 从 SourceManifest 构建，
    传给 check_left_join_safety 做唯一性判断。
    """
    unique_keys: list[list[str]] = field(default_factory=list)
    """已声明的唯一键组（已归一化）。[] 表示"已查询但无声明"，区别于 None 表示"未查询"。"""
    role: str | None = None
    """表角色——"fact" | "dim" | None。"""
    key_column_names_normalized: list[str] = field(default_factory=list)
    """key_columns 列名（已归一化），用于判断 join key 是否属于维度键。"""


class RelationshipValidator:
    """确定性 Join 证据评级器。

    评级规则（优先级从高到低，匹配即终止）：
    1. STRONG: (显式声明 OR FK约束) AND 类型兼容
    2. MEDIUM: 归一化名匹配 AND 类型兼容 AND (唯一索引 OR 高去重率)
    3. WEAK:   (编辑距离≤2 OR 别名匹配) AND 类型兼容
    4. NONE:   以上都不满足

    所有规则是确定性的——相同输入永远产生相同输出。
    不依赖 LLM、数据库查询或外部状态。
    """

    def rate(
        self,
        *,
        has_explicit_decl: bool = False,
        has_fk_constraint: bool = False,
        names_normalized_match: bool = False,
        name_edit_distance: int | None = None,
        name_alias_match: bool = False,
        types_compatible: bool = True,
        has_unique_index: bool = False,
        has_high_distinct_ratio: bool = False,
    ) -> tuple[JoinEvidenceLevel, EvidenceAction]:
        """根据输入信号计算证据等级与动作。

        Args:
            has_explicit_decl: DeveloperSpec 中程序员显式声明了此 Join
            has_fk_constraint: SchemaRegistry 中存在外键约束
            names_normalized_match: 归一化后字段名完全匹配
            name_edit_distance: 归一化后字段名的编辑距离（None 表示未计算或超过阈值）
            name_alias_match: 归一化后别名匹配（如 cust_id ↔ customer_id）
            types_compatible: 双方字段类型兼容（如 int↔bigint，varchar↔text）
            has_unique_index: 至少一方有唯一索引
            has_high_distinct_ratio: 快照采样显示右表键去重率高

        Returns:
            (JoinEvidenceLevel, EvidenceAction) 二元组
        """
        # 规则 1: STRONG——显式声明或外键约束 + 类型兼容
        # 类型兼容是必须的前置条件——不兼容的 FK 是数据质量问题，不能假定正确
        if (has_explicit_decl or has_fk_constraint) and types_compatible:
            return (JoinEvidenceLevel.STRONG, EvidenceAction.AUTO_ADOPT)

        # 规则 2: MEDIUM——字段名归一化匹配 + 类型兼容 + 索引佐证
        if names_normalized_match and types_compatible and (has_unique_index or has_high_distinct_ratio):
            return (JoinEvidenceLevel.MEDIUM, EvidenceAction.HUMAN_CONFIRM)

        # 规则 3: WEAK——仅字段名相似（编辑距离或别名）+ 类型兼容
        # 但没有任何约束/索引/声明佐证
        edit_close = name_edit_distance is not None and name_edit_distance <= 2
        if (edit_close or name_alias_match) and types_compatible:
            return (JoinEvidenceLevel.WEAK, EvidenceAction.REJECT_BLOCKING)

        # 规则 4: NONE——无任何有意义信号
        return (JoinEvidenceLevel.NONE, EvidenceAction.REJECT_SILENT)

    def generate_detail(
        self,
        level: JoinEvidenceLevel,
        has_explicit_decl: bool,
        has_fk_constraint: bool,
        names_normalized_match: bool,
        name_edit_distance: int | None,
        name_alias_match: bool,
        types_compatible: bool,
        has_unique_index: bool,
        has_high_distinct_ratio: bool,
    ) -> str:
        """生成人类可读的评级理由。"""
        signals = []
        if has_explicit_decl:
            signals.append("显式Join声明")
        if has_fk_constraint:
            signals.append("FK约束")
        if names_normalized_match:
            signals.append("字段名归一化匹配")
        if name_edit_distance is not None:
            signals.append(f"编辑距离={name_edit_distance}")
        if name_alias_match:
            signals.append("别名匹配")
        if not types_compatible:
            signals.append("类型不兼容")
        if has_unique_index:
            signals.append("唯一索引")
        if has_high_distinct_ratio:
            signals.append("高去重率")

        if level == JoinEvidenceLevel.STRONG:
            return f"STRONG：{' + '.join(signals[:2])}，类型兼容——自动采纳"
        elif level == JoinEvidenceLevel.MEDIUM:
            idx = "唯一索引" if has_unique_index else "高去重率"
            return f"MEDIUM：字段名匹配 + 类型兼容 + {idx}——需人工确认"
        elif level == JoinEvidenceLevel.WEAK:
            reason = "编辑距离接近" if name_edit_distance else "别名匹配"
            return f"WEAK：{reason} + 类型兼容，无约束佐证——阻断拒绝"
        else:
            if not types_compatible:
                return "NONE：类型不兼容——静默忽略"
            return "NONE：无任何匹配信号——静默忽略"

    # ── LEFT JOIN 唯一性安全门禁（V2：预归一化 + JoinSafetyTableInfo + 分层文案）──

    def check_left_join_safety(
        self,
        right_table_unique_keys: list[list[str]] | None,
        right_join_key_normalized: str,
        right_join_safety_info: JoinSafetyTableInfo | None = None,
    ) -> tuple[bool, str | None]:
        """检查 LEFT JOIN 右表联结键是否有唯一性保证。

        右表联结键不唯一时，LEFT JOIN 会产生静默笛卡尔积——
        左表行被复制、度量值膨胀。此方法是安全门禁：
        无唯一性证据时返回 unsafe，由调用方生成 blocking OpenQuestion。

        Phase 1 仅支持单列联结键。复合键去重延后到 Phase 2。

        V2 变更：
        - right_join_key_normalized 已由调用方经 FieldNormalizer 归一化传入
        - unique_keys 已由 Builder 预归一化
        - Validator 内部不做任何 .lower() ——接收即假定已归一化
        - right_join_safety_info 提供 role/key_column_names 用于增强阻断文案

        Args:
            right_table_unique_keys: 右表的 unique_keys 列表（已归一化）。None 表示未查询。
            right_join_key_normalized: 右表联结键——已由调用方经 FieldNormalizer 归一化传入。
            right_join_safety_info: 右表的完整安全信息（含 role + key_column_names_normalized），
                                    用于生成更精准的阻断提示。None 时退化为 V1 行为。

        Returns:
            (is_safe, description) 二元组。
            is_safe=True 时 description 为 None。
            is_safe=False 时 description 为阻断理由。
        """
        # 无任何唯一性声明 → unsafe
        if not right_table_unique_keys:
            return self._build_unsafe_result_no_unique_keys(
                right_join_key_normalized, right_join_safety_info
            )

        # 检查是否有唯一键组覆盖 join key（Phase 1 仅支持单列键）
        for key_group in right_table_unique_keys:
            if right_join_key_normalized in key_group and len(key_group) == 1:
                return (True, None)

        # 有唯一键声明但不覆盖当前联结键 → unsafe
        declared = "; ".join(", ".join(g) for g in right_table_unique_keys)
        return (
            False,
            f"LEFT JOIN 右表联结键 '{right_join_key_normalized}' 不被任何唯一键覆盖。"
            f"右表已声明唯一键：[{declared}]，"
            f"均不包含 '{right_join_key_normalized}'。"
            f"若该键有重复值，将导致静默笛卡尔积。"
            f"请确认联结键选择正确，或为 '{right_join_key_normalized}' 声明唯一性。",
        )

    def _build_unsafe_result_no_unique_keys(
        self,
        right_join_key_normalized: str,
        safety_info: JoinSafetyTableInfo | None,
    ) -> tuple[bool, str]:
        """构建"无唯一性声明"的阻断结果——dim 表生成增强文案。

        策略：
        - role=dim 且 join_key ∈ key_column_names_normalized → 增强文案（提示声明 unique: true 或 unique_keys）
        - 其他 → 通用阻断文案
        """
        # 场景 2：dim 表 + join_key 属于 key_column_names_normalized → 增强文案
        if (
            safety_info is not None
            and safety_info.role == "dim"
            and right_join_key_normalized in safety_info.key_column_names_normalized
        ):
            return (
                False,
                f"dim 表的 key_column '{right_join_key_normalized}' 未声明唯一性。"
                f"请在 DeveloperSpec 中为该列添加 'unique: true'，"
                f"或在 source_tables 对应条目中声明 "
                f"unique_keys: [['{right_join_key_normalized}']]。"
                f"若该键确实有重复值，请提供去重策略说明。",
            )
        # 场景 1：通用阻断——无任何唯一性声明
        return (
            False,
            f"LEFT JOIN 右表 '{right_join_key_normalized}' 无唯一性保证："
            f"右表未声明 primary_key 且 unique_keys 为空。"
            f"若该键有重复值，将导致静默笛卡尔积、左表度量值膨胀。"
            f"请在 DeveloperSpec 中为右表声明 unique_keys，或提供去重策略说明。",
        )
