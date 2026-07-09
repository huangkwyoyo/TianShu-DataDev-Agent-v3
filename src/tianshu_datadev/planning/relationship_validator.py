"""RelationshipValidator——确定性证据评级器。

LLM/Fake 只能提候选——等级由 Validator 确定性计算。
四级级联规则：STRONG → MEDIUM → WEAK → NONE，匹配即终止。
"""

from __future__ import annotations

from .relationship_hypothesis import EvidenceAction, JoinEvidenceLevel


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

    # ── LEFT JOIN 唯一性安全门禁 ──

    def check_left_join_safety(
        self,
        right_table_unique_keys: list[list[str]] | None,
        right_join_key: str,
    ) -> tuple[bool, str | None]:
        """检查 LEFT JOIN 右表联结键是否有唯一性保证。

        右表联结键不唯一时，LEFT JOIN 会产生静默笛卡尔积——
        左表行被复制、度量值膨胀。此方法是安全门禁：
        无唯一性证据时返回 unsafe，由调用方生成 blocking OpenQuestion。

        Phase 1 仅支持单列联结键。复合键去重延后到 Phase 2。

        Args:
            right_table_unique_keys: 右表的 unique_keys 列表（来自 ManifestTable），
                                     每个元素是一组列名。None 表示无任何唯一性声明。
            right_join_key: 右表联结键的原始字段名（非归一化）。

        Returns:
            (is_safe, description) 二元组。
            is_safe=True 时 description 为 None。
            is_safe=False 时 description 为阻断理由。
        """
        # 无任何唯一性声明 → unsafe
        if not right_table_unique_keys:
            return (
                False,
                f"LEFT JOIN 右表 '{right_join_key}' 无唯一性保证："
                f"右表未声明 primary_key 且 ManifestTable.unique_keys 为空。"
                f"若该键有重复值，将导致静默笛卡尔积、左表度量值膨胀。"
                f"请在 DeveloperSpec 中为右表声明 unique_keys，或提供去重策略说明。",
            )

        # 归一化比较——大小写不敏感
        right_key_lower = right_join_key.lower()

        for key_group in right_table_unique_keys:
            group_lower = [k.lower() for k in key_group]
            # Phase 1：仅支持单列唯一键精确覆盖
            if right_key_lower in group_lower and len(key_group) == 1:
                return (True, None)

        # 有 unique_keys 声明但不覆盖当前联结键 → unsafe
        declared = "; ".join(", ".join(g) for g in right_table_unique_keys)
        return (
            False,
            f"LEFT JOIN 右表联结键 '{right_join_key}' 不被任何唯一键覆盖。"
            f"右表已声明唯一键：[{declared}]，"
            f"均不包含 '{right_join_key}'。"
            f"若该键有重复值，将导致静默笛卡尔积。"
            f"请确认联结键选择正确，或为 '{right_join_key}' 声明唯一性。",
        )
