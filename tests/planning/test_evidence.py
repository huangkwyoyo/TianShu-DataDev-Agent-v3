"""测试 RelationshipValidator 证据等级判定 + WEAK/NONE 硬门禁。"""


from tianshu_datadev.planning.relationship_hypothesis import (
    EvidenceAction,
    JoinEvidenceLevel,
)
from tianshu_datadev.planning.relationship_validator import RelationshipValidator


class TestEvidenceValidatorStrong:
    """STRONG 证据等级测试。"""

    def test_strong_explicit_decl(self):
        """显式声明 + 类型兼容 → STRONG / AUTO_ADOPT。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=True,
            has_fk_constraint=False,
            names_normalized_match=True,
            name_edit_distance=0,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.STRONG
        assert action == EvidenceAction.AUTO_ADOPT

    def test_strong_fk_constraint(self):
        """FK 约束 + 类型兼容 → STRONG / AUTO_ADOPT。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=True,
            names_normalized_match=True,
            name_edit_distance=0,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.STRONG
        assert action == EvidenceAction.AUTO_ADOPT

    def test_strong_rejected_type_mismatch(self):
        """显式声明但类型不兼容 → 不进入 STRONG（至少应进入 WEAK 或 NONE）。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=True,
            has_fk_constraint=False,
            names_normalized_match=True,
            name_edit_distance=0,
            name_alias_match=False,
            types_compatible=False,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        # 类型不兼容时，即使显式声明也不应 STRONG
        assert level != JoinEvidenceLevel.STRONG


class TestEvidenceValidatorMedium:
    """MEDIUM 证据等级测试。"""

    def test_medium_unique_index(self):
        """归一化名匹配 + 类型兼容 + 唯一索引 → MEDIUM / HUMAN_CONFIRM。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=True,
            name_edit_distance=0,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=True,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.MEDIUM
        assert action == EvidenceAction.HUMAN_CONFIRM

    def test_medium_high_distinct(self):
        """归一化名匹配 + 类型兼容 + 高去重率 → MEDIUM / HUMAN_CONFIRM。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=True,
            name_edit_distance=0,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=True,
        )
        assert level == JoinEvidenceLevel.MEDIUM
        assert action == EvidenceAction.HUMAN_CONFIRM


class TestEvidenceValidatorWeak:
    """WEAK 证据等级测试。"""

    def test_weak_edit_distance_one(self):
        """编辑距离 1 + 类型兼容 → WEAK / REJECT_BLOCKING。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=1,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.WEAK
        assert action == EvidenceAction.REJECT_BLOCKING

    def test_weak_edit_distance_two(self):
        """编辑距离 2 + 类型兼容 → WEAK / REJECT_BLOCKING。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=2,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.WEAK
        assert action == EvidenceAction.REJECT_BLOCKING

    def test_weak_alias_match(self):
        """别名匹配 + 类型兼容 → WEAK / REJECT_BLOCKING。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=None,
            name_alias_match=True,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.WEAK
        assert action == EvidenceAction.REJECT_BLOCKING


class TestEvidenceValidatorNone:
    """NONE 证据等级测试。"""

    def test_none_no_signals(self):
        """无任何信号 → NONE / REJECT_SILENT。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=None,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.NONE
        assert action == EvidenceAction.REJECT_SILENT

    def test_none_edit_distance_too_high(self):
        """编辑距离 5——远超阈值，应落入 NONE。"""
        validator = RelationshipValidator()
        level, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=5,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert level == JoinEvidenceLevel.NONE
        assert action == EvidenceAction.REJECT_SILENT


class TestHardGate:
    """WEAK/NONE 硬门禁——不得进入 SqlBuildPlan JoinStep。"""

    def test_weak_action_is_reject_blocking(self):
        """WEAK → REJECT_BLOCKING——调用方必须看到明确的拒绝信号。"""
        validator = RelationshipValidator()
        _, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=1,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert action == EvidenceAction.REJECT_BLOCKING

    def test_none_action_is_reject_silent(self):
        """NONE → REJECT_SILENT——静默忽略，不浪费审查带宽。"""
        validator = RelationshipValidator()
        _, action = validator.rate(
            has_explicit_decl=False,
            has_fk_constraint=False,
            names_normalized_match=False,
            name_edit_distance=None,
            name_alias_match=False,
            types_compatible=True,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )
        assert action == EvidenceAction.REJECT_SILENT

    def test_strong_action_is_auto_adopt(self):
        """STRONG → AUTO_ADOPT——进入 SqlBuildPlan。"""
        validator = RelationshipValidator()
        _, action = validator.rate(
            has_explicit_decl=True,
            types_compatible=True,
        )
        assert action == EvidenceAction.AUTO_ADOPT

    def test_medium_action_is_human_confirm(self):
        """MEDIUM → HUMAN_CONFIRM——进入 SqlBuildPlan 但产生 OpenQuestion。"""
        validator = RelationshipValidator()
        _, action = validator.rate(
            names_normalized_match=True,
            types_compatible=True,
            has_unique_index=True,
        )
        assert action == EvidenceAction.HUMAN_CONFIRM
