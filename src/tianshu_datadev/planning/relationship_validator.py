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
