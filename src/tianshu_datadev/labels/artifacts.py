"""标签子系统 Artifact 与校验报告模型。

Artifact 记录标签提取/Promotion 全过程的溯源信息。
ValidationReport 记录六项确定性检查的结果。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    LabelRuleProposal,
    StrictModel,
)


class LabelValidationCheck(StrictModel):
    """单条校验检查项——Validator 六项检查的原子结果。"""

    check_name: str  # 检查项名称（如 FIELD_EXISTS / TYPE_COMPATIBLE / LABEL_DOMAIN）
    passed: bool  # 本项是否通过
    level: str  # 严重级别：BLOCKING / HUMAN_REVIEW / WARN
    detail: str = ""  # 人类可读的检查详情


class LabelValidationReport(StrictModel):
    """单条 LabelRuleProposal 的校验报告。

    v4-light 最终版：passed=True 要求 blocking_errors 和 human_review_items 均为空。
    """

    proposal_id: str  # 对应 LabelRuleProposal.proposal_id
    passed: bool  # len(blocking_errors)==0 and len(human_review_items)==0
    checks: list[LabelValidationCheck] = []  # 逐项检查结果
    blocking_errors: list[str] = []  # BLOCKING 级别的错误描述——非空必导致 passed=False
    human_review_items: list[str] = []  # 需要人工审核的项目——非空也导致 passed=False
    warnings: list[str] = []  # WARN 级别的警告——不影响 passed


class LabelExtractionArtifact(StrictModel):
    """标签提取阶段的溯源 Artifact——记录 LLM 调用的完整上下文。

    包含输入参数、LLM 返回的快照以及系统生成的源信息。
    """

    artifact_id: str  # 唯一标识
    source_spec_hash: str  # 来源 Spec 的哈希——用于关联到具体 Spec 版本
    extraction_time: str  # ISO 8601 提取时间戳——系统生成
    llm_model: str  # 使用的 LLM 模型名称（或 "fake"）
    llm_prompt_version: str  # Prompt 模板版本
    llm_temperature: float  # LLM 温度参数
    unresolved_columns: list[str] = []  # 请求提取的未解析派生列名列表
    raw_proposals: list[LabelRuleProposal] = []  # LLM 返回的原始规则候选（已包装系统字段）
    prompt_snapshot: str = ""  # 实际发送给 LLM 的完整 Prompt 快照（用于调试/审计）


class LabelPromotionArtifact(StrictModel):
    """标签提升阶段的溯源 Artifact——记录 Proposal → CaseWhenDecl 的转换过程。

    包含验证结果、被拒绝的提案以及需要人工审核的标记。
    """

    artifact_id: str  # 唯一标识
    parent_spec_hash: str  # 源 Spec 的哈希——保留不变，用于关联原始 Spec
    new_spec_hash: str  # 提升后的 Spec 哈希——当前与 parent_spec_hash 相同
    promoted_rules_hash: str = ""  # 提升规则的独立哈希——从 promoted_rules 列表计算
    promotion_time: str  # ISO 8601 提升时间戳
    extraction_artifact_id: str  # 关联的 LabelExtractionArtifact.artifact_id
    promoted_rules: list[CaseWhenDecl] = []  # 成功提升的 CaseWhenDecl 列表
    validation_reports: list[LabelValidationReport] = []  # 所有规则的校验报告
    rejected_proposals: list[str] = []  # 被拒绝的 proposal_id 列表
    human_review_required: bool = False  # 是否需要人工审核
