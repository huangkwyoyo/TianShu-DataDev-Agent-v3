"""Join 推测模型——JoinCandidate / RelationshipEvidence / RelationshipHypothesis。

LLM/Fake 只能提候选，证据等级由 RelationshipValidator 确定性计算。
WEAK/NONE Join 硬门禁——不得进入 SqlBuildPlan 的 JoinStep。
"""

from __future__ import annotations

import hashlib
from enum import Enum

from tianshu_datadev.developer_spec.models import StrictModel

from .models import JoinType

# ════════════════════════════════════════════
# 证据等级与动作枚举
# ════════════════════════════════════════════


class JoinEvidenceLevel(str, Enum):
    """Join 证据等级——由 RelationshipValidator 确定性计算。

    STRONG: (显式声明 OR FK约束) AND 类型兼容 → AUTO_ADOPT
    MEDIUM: 归一化名匹配 AND 类型兼容 AND (唯一索引 OR 高去重率) → HUMAN_CONFIRM
    WEAK:   (编辑距离≤2 OR 别名匹配) AND 类型兼容 → REJECT_BLOCKING
    NONE:   无任何证据 → REJECT_SILENT
    """

    STRONG = "STRONG"
    MEDIUM = "MEDIUM"
    WEAK = "WEAK"
    NONE = "NONE"


class EvidenceAction(str, Enum):
    """证据动作——基于等级的自动决策。

    AUTO_ADOPT:      STRONG → 自动采纳，不进入 open_questions
    HUMAN_CONFIRM:   MEDIUM → 采纳但输出到 open_questions 供人工审查
    REJECT_BLOCKING: WEAK → 拒绝进入 SqlBuildPlan，进入 open_questions(blocking=true)
    REJECT_SILENT:   NONE → 拒绝，仅记录 evidence_log，不浪费审查带宽
    """

    AUTO_ADOPT = "AUTO_ADOPT"
    HUMAN_CONFIRM = "HUMAN_CONFIRM"
    REJECT_BLOCKING = "REJECT_BLOCKING"
    REJECT_SILENT = "REJECT_SILENT"


# ════════════════════════════════════════════
# 核心模型
# ════════════════════════════════════════════


class RelationshipEvidence(StrictModel):
    """单条 Join 证据记录——逐条检查结果 + 可渲染证据链 YAML。

    evidence_chain_yaml 字段保存完整的 YAML 格式证据链文本，
    供人工审查时直接渲染，不依赖外部工具。
    """

    evidence_id: str
    level: JoinEvidenceLevel
    action: EvidenceAction
    left_table: str  # 左表引用（table_alias）
    right_table: str  # 右表引用（table_alias）
    left_key_raw: str  # 左键原始字段名
    right_key_raw: str  # 右键原始字段名
    left_key_normalized: str  # 左键归一化后字段名
    right_key_normalized: str  # 右键归一化后字段名
    evidence_checks: list[str] = []  # 通过/失败的检查项列表（如 "field_name_match: MATCH"）
    detail: str = ""  # 人类可读的评级理由
    evidence_chain_yaml: str = ""  # 可渲染的完整证据链 YAML 文本

    def generate_evidence_chain_yaml(self) -> str:
        """生成可渲染的 YAML 格式证据链文本。

        供人工审查时直接渲染，包含左右字段详情、逐条证据检查结果和最终等级/动作。
        """
        checks_yaml = "\n".join(f"    - {c}" for c in self.evidence_checks)
        yaml_text = f"""join_candidate:
  evidence_id: "{self.evidence_id}"
  left_table: "{self.left_table}"
  right_table: "{self.right_table}"
  left_field:
    raw: "{self.left_key_raw}"
    normalized: "{self.left_key_normalized}"
  right_field:
    raw: "{self.right_key_raw}"
    normalized: "{self.right_key_normalized}"
  evidence_checks:
{checks_yaml}
  level: "{self.level.value}"
  action: "{self.action.value}"
  detail: "{self.detail}"
"""
        # 缓存到字段中，确保 idempotent
        object.__setattr__(self, "evidence_chain_yaml", yaml_text)
        return yaml_text


class JoinCandidate(StrictModel):
    """Join 候选——由 FakeRelationshipPlanner 提出，含 Validator 计算的证据等级。

    候选提出者（LLM/Fake）填写表引用和键名，
    Validator 确定性填入 evidence（等级 + 动作 + 证据链）。
    WEAK/NONE 候选被 FakeRelationshipPlanner 过滤——
    不进入 RelationshipHypothesis.candidates，而是进入 open_questions。
    """

    candidate_id: str
    left_table: str  # 左表引用（table_alias，对应 SourceManifest.table_ref）
    right_table: str  # 右表引用
    left_key: str  # 左键原始字段名
    right_key: str  # 右键原始字段名
    left_key_normalized: str  # 归一化后
    right_key_normalized: str  # 归一化后
    join_type: JoinType = JoinType.INNER
    evidence: RelationshipEvidence | None = None  # Validator 确定性填入

    # ── 确定性 ID 生成 ──

    @staticmethod
    def generate_candidate_id(
        left_table: str, right_table: str, left_key: str, right_key: str
    ) -> str:
        """基于内容的确定性候选 ID——相同键 → 相同 ID。"""
        content = f"{left_table}.{left_key}->{right_table}.{right_key}"
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:8]
        return f"jc_{hash_hex}"


class RelationshipHypothesis(StrictModel):
    """Join 推测——所有通过 Validator（STRONG/MEDIUM）的候选 Join 集合。

    WEAK/NONE 候选被过滤——不在此列表中，而是记录在 open_questions 中。
    source_manifest_hash 用于溯源，保证推测与 SourceManifest 的一致性。
    """

    hypothesis_id: str
    spec_hash: str  # 对应 ParsedDeveloperSpec.spec_hash
    source_manifest_hash: str | None = None  # 对应 SourceManifest 的 hash
    candidates: list[JoinCandidate] = []  # 仅 STRONG + MEDIUM
    multi_table: bool = False  # spec.input_tables 数量 > 1
    # WEAK/NONE 被过滤但应记录在 open_questions 中，不在此列表

    # ── 确定性 ID 生成 ──

    @staticmethod
    def generate_hypothesis_id(spec_hash: str) -> str:
        """基于 spec_hash 的确定性 hypothesis ID。"""
        return f"hyp_{spec_hash[:12]}"
