"""标签提升——Proposal → CaseWhenDecl（双空阻断 + 必需字段检查）。

v4-light 最终版：Promotion 要求 blocking_errors 和 human_review_items 均为空。
额外安全性：所有分支 evidence 必须非空。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    CaseWhenBranchDecl,
    LabelBranchProposal,
    LabelPredicateBranch,
    LabelRuleProposal,
)
from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
    LabelValidationReport,
)


class Promotion:
    """标签提升器——将验证通过的 Proposal 转换为 CaseWhenDecl。

    双空阻断：blocking_errors 和 human_review_items 均为空才提升。
    额外安全性检查：每个分支的 evidence 必须非空。
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

            # 转换 branches→typed_branches + CaseWhenBranchDecl
            typed_branches: list[LabelPredicateBranch] = []
            case_branches: list[CaseWhenBranchDecl] = []
            for branch in proposal.branches:
                typed_branches.append(LabelPredicateBranch(
                    condition=branch.condition,
                    then_label=branch.then_label,
                ))
                case_branches.append(CaseWhenBranchDecl(
                    condition_text=branch.evidence,
                    then_value=branch.then_label,
                ))

            case_when = CaseWhenDecl(
                branches=case_branches,
                else_value=proposal.else_value,
                output_column=proposal.output_column,
                typed_branches=typed_branches,
            )
            promoted.append(case_when)

        new_spec_hash = spec_hash  # 提升不改变 spec_hash——label_rules 附加到现有 spec

        artifact = LabelPromotionArtifact(
            artifact_id=f"prom_{uuid.uuid4().hex[:12]}",
            parent_spec_hash=spec_hash,
            new_spec_hash=new_spec_hash,
            promotion_time=datetime.now(timezone.utc).isoformat(),
            extraction_artifact_id=extraction_artifact.artifact_id,
            promoted_rules=promoted,
            validation_reports=reports,
            rejected_proposals=rejected_ids,
            human_review_required=human_review_required,
        )

        return promoted, artifact
