"""标签提升——Proposal → CaseWhenDecl（双空阻断 + 必需字段检查）。

v4-light 最终版：Promotion 要求 blocking_errors 和 human_review_items 均为空。
额外安全性：所有分支 evidence 必须非空。

CaseWhenDecl 接口使用规则：
- typed_branches：从 Proposal 的结构化 condition 生成——这是编译器的真实输入
- branches（CaseWhenBranchDecl 列表）：留空——不把 evidence 写入自由字符串条件
  evidence 仅保存在 Artifact 中供人审追溯，不进入编译器
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    LabelPredicateBranch,
    LabelRuleProposal,
)
from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
    LabelValidationReport,
)


def _compute_promoted_rules_hash(rules: list[CaseWhenDecl]) -> str:
    """计算提升规则列表的确定性哈希——用于变更检测与追溯。

    将每个 CaseWhenDecl 的关键字段序列化为 JSON 后做 SHA256——
    仅含 output_column / typed_branches / else_value，不含时间戳等非确定性字段。

    Args:
        rules: 成功提升的 CaseWhenDecl 列表

    Returns:
        12 位十六进制哈希字符串
    """
    if not rules:
        return ""
    # 仅取确定性字段——排除 branches（始终为空列表）
    payload = [
        {
            "output_column": r.output_column,
            "else_value": r.else_value,
            "typed_branches": [
                {
                    "condition": tb.condition.model_dump(mode="json"),
                    "then_label": tb.then_label,
                }
                for tb in r.typed_branches
            ],
        }
        for r in rules
    ]
    full_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return full_hash[:12]


class Promotion:
    """标签提升器——将验证通过的 Proposal 转换为 CaseWhenDecl。

    双空阻断：blocking_errors 和 human_review_items 均为空才提升。
    额外安全性检查：每个分支的 evidence 必须非空。

    只生成 typed_branches（结构化条件）——
    branches（CaseWhenBranchDecl）留空，不把 evidence 写入自由字符串条件。
    """

    def promote(
        self,
        spec_hash: str,
        proposals: list[LabelRuleProposal],
        reports: list[LabelValidationReport],
        extraction_artifact: LabelExtractionArtifact,
    ) -> tuple[list[CaseWhenDecl], LabelPromotionArtifact]:
        """将验证通过的 Proposal 提升为 CaseWhenDecl。

        Args:
            spec_hash: 源 Spec 的哈希
            proposals: 系统包装后的标签规则候选列表
            reports: 对应的校验报告列表
            extraction_artifact: 标签提取阶段的溯源 Artifact

        Returns:
            (promoted_rules, artifact)
        """
        promoted: list[CaseWhenDecl] = []
        rejected_ids: list[str] = []
        human_review_required = False

        for proposal, report in zip(proposals, reports):
            # v4-light 最终版：双空阻断
            if not report.passed:
                rejected_ids.append(proposal.proposal_id)
                if report.human_review_items:
                    human_review_required = True
                continue

            # 额外安全性：每个分支的 evidence 必须非空
            empty_evidence = [
                b.then_label for b in proposal.branches if not b.evidence
            ]
            if empty_evidence:
                rejected_ids.append(proposal.proposal_id)
                human_review_required = True
                continue

            # 只生成 typed_branches——不把 evidence 写入自由字符串条件
            # evidence 保留在 Artifact 中供人审追溯，不进入编译器
            typed_branches: list[LabelPredicateBranch] = []
            for branch in proposal.branches:
                typed_branches.append(LabelPredicateBranch(
                    condition=branch.condition,
                    then_label=branch.then_label,
                ))

            case_when = CaseWhenDecl(
                branches=[],  # 留空——不生成自由字符串条件
                else_value=proposal.else_value,
                output_column=proposal.output_column,
                typed_branches=typed_branches,
            )
            promoted.append(case_when)

        new_spec_hash = spec_hash  # 提升不改变 spec_hash——label_rules 附加到现有 spec
        # 计算提升规则的独立哈希——用于变更检测与追溯
        promoted_rules_hash = _compute_promoted_rules_hash(promoted)

        artifact = LabelPromotionArtifact(
            artifact_id=f"prom_{uuid.uuid4().hex[:12]}",
            parent_spec_hash=spec_hash,
            new_spec_hash=new_spec_hash,
            promoted_rules_hash=promoted_rules_hash,
            promotion_time=datetime.now(timezone.utc).isoformat(),
            extraction_artifact_id=extraction_artifact.artifact_id,
            promoted_rules=promoted,
            validation_reports=reports,
            rejected_proposals=rejected_ids,
            human_review_required=human_review_required,
        )

        return promoted, artifact
