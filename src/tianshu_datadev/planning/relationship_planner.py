"""FakeRelationshipPlanner——Phase 1B 确定性 Join 推测器。

仅从 DeveloperSpec 显式 Join 声明生成候选（不推断），
调用 RelationshipValidator 定级，过滤 WEAK/NONE，生成证据链 YAML。
不依赖 LLM——Phase 4 替换为真实 LLM Planner。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
from tianshu_datadev.developer_spec.models import JoinDecl, OpenQuestion, ParsedDeveloperSpec, SourceManifest

from .models import JoinType
from .relationship_hypothesis import (
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipEvidence,
    RelationshipHypothesis,
)
from .relationship_validator import RelationshipValidator


class FakeRelationshipPlanner:
    """Phase 1B 确定性 Join 推测器（Fake 实现）。

    行为：
    1. 从 DeveloperSpec.joins 提取显式声明的 Join
    2. 对每个 Join 调用 FieldNormalizer 归一化键名
    3. 调用 RelationshipValidator 确定性定级
    4. STRONG/MEDIUM → 加入 hypothesis.candidates
    5. WEAK/NONE → 生成 OpenQuestion，不加入 candidates（硬门禁）
    6. 生成可渲染的 YAML 证据链文本
    """

    def __init__(
        self,
        validator: RelationshipValidator | None = None,
        normalizer: FieldNormalizer | None = None,
    ):
        """初始化 Fake Planner。

        Args:
            validator: 证据评级器，None 使用默认 RelationshipValidator
            normalizer: 字段名归一化器，None 使用默认 FieldNormalizer
        """
        self._validator = validator or RelationshipValidator()
        self._normalizer = normalizer or FieldNormalizer()

    def plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest | None = None,
    ) -> tuple[RelationshipHypothesis, list[OpenQuestion]]:
        """基于 DeveloperSpec 构建 RelationshipHypothesis。

        Phase 1B 仅处理显式 Join 声明——不进行字段名匹配推理。
        manifest 参数保留接口，供 Phase 4 使用。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 可选的 SourceManifest（Phase 1B 不使用，保留接口）

        Returns:
            (RelationshipHypothesis, list[OpenQuestion])
        """
        open_questions: list[OpenQuestion] = []
        candidates: list[JoinCandidate] = []

        # 从显式声明提取候选
        if spec.joins:
            for join_decl in spec.joins:
                candidate = self._build_candidate(join_decl, spec)
                open_q = self._rate_and_decide(candidate)
                if open_q:
                    open_questions.append(open_q)
                else:
                    # STRONG/MEDIUM 且无额外问题的加入 candidates
                    candidates.append(candidate)

        multi_table = len(spec.input_tables) > 1

        hypothesis = RelationshipHypothesis(
            hypothesis_id=RelationshipHypothesis.generate_hypothesis_id(spec.spec_hash),
            spec_hash=spec.spec_hash,
            source_manifest_hash=manifest.spec_hash if manifest else None,
            candidates=candidates,
            multi_table=multi_table,
        )

        return hypothesis, open_questions

    # ── 内部方法 ──

    def _build_candidate(self, join_decl: JoinDecl, spec: ParsedDeveloperSpec) -> JoinCandidate:
        """将 DeveloperSpec JoinDecl 转换为 JoinCandidate。"""
        left_normalized = self._normalizer.normalize(join_decl.left_key)
        right_normalized = self._normalizer.normalize(join_decl.right_key)

        return JoinCandidate(
            candidate_id=JoinCandidate.generate_candidate_id(
                join_decl.left_table,
                join_decl.right_table,
                join_decl.left_key,
                join_decl.right_key,
            ),
            left_table=join_decl.left_table,
            right_table=join_decl.right_table,
            left_key=join_decl.left_key,
            right_key=join_decl.right_key,
            left_key_normalized=left_normalized,
            right_key_normalized=right_normalized,
            join_type=self._map_join_type(join_decl),
        )

    def _rate_and_decide(self, candidate: JoinCandidate) -> OpenQuestion | None:
        """对候选调用 Validator 定级，填充 evidence，按等级决定去向。

        Returns:
            OpenQuestion（WEAK/NONE 时）或 None（STRONG/MEDIUM 通过时）。
        """
        # 判断字段名归一化后是否匹配
        names_match = candidate.left_key_normalized == candidate.right_key_normalized

        # 计算编辑距离
        edit_dist = self._edit_distance(
            candidate.left_key_normalized,
            candidate.right_key_normalized,
        )

        # 判断别名匹配（编辑距离不为0但归一化后仍不同 → 检查是否仅差别名）
        alias_match = not names_match and edit_dist is not None and edit_dist <= 2

        # 类型兼容性——Phase 1B 默认兼容（DeveloperSpec 显式声明视为程序员保证了兼容性）
        types_compatible = True

        # 调用 Validator 定级
        level, action = self._validator.rate(
            has_explicit_decl=True,  # Phase 1B 只处理显式声明
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,  # Phase 1B 不使用 schema_registry
            has_high_distinct_ratio=False,
        )

        # 构建证据检查列表
        evidence_checks = self._build_checks(
            names_match=names_match,
            edit_dist=edit_dist,
            types_compatible=types_compatible,
            has_explicit_decl=True,
        )

        # 生成可读理由
        detail = self._validator.generate_detail(
            level=level,
            has_explicit_decl=True,
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )

        # 构建证据记录
        evidence = RelationshipEvidence(
            evidence_id=f"ev_{candidate.candidate_id}",
            level=level,
            action=action,
            left_table=candidate.left_table,
            right_table=candidate.right_table,
            left_key_raw=candidate.left_key,
            right_key_raw=candidate.right_key,
            left_key_normalized=candidate.left_key_normalized,
            right_key_normalized=candidate.right_key_normalized,
            evidence_checks=evidence_checks,
            detail=detail,
        )
        # 生成可渲染证据链 YAML
        evidence.generate_evidence_chain_yaml()

        # 填入 evidence
        object.__setattr__(candidate, "evidence", evidence)

        # WEAK/NONE 硬门禁——生成 OpenQuestion 阻断
        if level in (JoinEvidenceLevel.WEAK, JoinEvidenceLevel.NONE):
            blocking = level == JoinEvidenceLevel.WEAK
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"证据等级 {level.value}——{detail}"
                ),
                blocking=blocking,
            )

        # MEDIUM → OpenQuestion（非阻断）
        if level == JoinEvidenceLevel.MEDIUM:
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"证据等级 MEDIUM——需人工确认"
                ),
                blocking=False,
            )

        # STRONG → 无需 open_question
        return None

    # ── 辅助 ──

    @staticmethod
    def _map_join_type(join_decl: JoinDecl) -> JoinType:
        """将 developer_spec JoinDecl.join_type 映射到 planning JoinType。"""
        # JoinDecl.join_type 是 JoinTypeEnum（developer_spec），映射到 JoinType（planning）
        mapping = {
            "INNER": JoinType.INNER,
            "LEFT": JoinType.LEFT,
            "RIGHT": JoinType.RIGHT,
            "FULL": JoinType.FULL,
        }
        return mapping.get(str(join_decl.join_type.value), JoinType.INNER)

    @staticmethod
    def _edit_distance(a: str, b: str) -> int:
        """计算两个字符串的莱文斯坦编辑距离。"""
        if len(a) < len(b):
            a, b = b, a
        # a 是较长的字符串
        if len(b) == 0:
            return len(a)
        # 使用两行滚动数组节省内存
        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a, 1):
            curr = [i]
            for j, cb in enumerate(b, 1):
                if ca == cb:
                    curr.append(prev[j - 1])
                else:
                    curr.append(1 + min(prev[j], curr[-1], prev[j - 1]))
            prev = curr
        return prev[-1]

    @staticmethod
    def _build_checks(
        names_match: bool,
        edit_dist: int | None,
        types_compatible: bool,
        has_explicit_decl: bool,
    ) -> list[str]:
        """构建证据检查列表——记录每条检查的结果。"""
        checks: list[str] = []
        if has_explicit_decl:
            checks.append("developer_declared: FOUND")
        else:
            checks.append("developer_declared: NOT_FOUND")

        if names_match:
            checks.append("field_name_match: MATCH")
        elif edit_dist is not None and edit_dist <= 2:
            checks.append(f"field_name_similarity: PARTIAL (edit_distance={edit_dist})")
        else:
            checks.append("field_name_match: MISMATCH")

        if types_compatible:
            checks.append("type_compatibility: MATCH")
        else:
            checks.append("type_compatibility: MISMATCH")

        checks.append("unique_index: NOT_CHECKED")
        checks.append("foreign_key: NOT_CHECKED")
        return checks
