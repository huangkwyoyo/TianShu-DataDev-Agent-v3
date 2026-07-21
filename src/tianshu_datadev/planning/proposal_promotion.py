"""ProposalPromotion——将校验通过的 Proposal 写入 ParsedDeveloperSpec。

核心原则：仅追加不覆盖。程序员手写字段优先级最高——
如果 spec 已有值，proposal 不能覆盖。

Promotion 规则：
1. dimensions：proposal 非空且 spec 为空时写入
2. derived_dimensions：proposal 非空时追加到 spec（同名不重复追加）
3. metrics：proposal 非空且 spec 为空时写入
4. case_when_rules：proposal 非空时追加到 spec（同 output_column 不重复追加）
"""

from tianshu_datadev.developer_spec.models import (
    ParsedDeveloperSpec,
    RequirementProposal,
)


class ProposalPromotion:
    """将校验通过的 Proposal 确定性写入 ParsedDeveloperSpec。

    仅追加不覆盖——已有内容的字段拒绝被 proposal 覆盖。
    """

    def promote(
        self,
        proposal: RequirementProposal,
        spec: ParsedDeveloperSpec,
    ) -> ParsedDeveloperSpec:
        """将 Proposal 内容 Promotion 到 Spec（仅追加不覆盖）。

        Args:
            proposal: 已校验通过的 Proposal
            spec: 原始 ParsedDeveloperSpec

        Returns:
            修改后的 ParsedDeveloperSpec 副本
        """
        # 浅拷贝——Pydantic model_copy 返回新实例
        result = spec.model_copy(deep=False)

        # ── dimensions：仅当 proposal 非空且 spec 为空时写入 ──
        if proposal.dimensions and not result.dimensions:
            result.dimensions = list(proposal.dimensions)

        # ── derived_dimensions：非空时追加，同名不重复 ──
        if proposal.derived_dimensions:
            existing_names = {d.dimension_name for d in result.derived_dimensions}
            for dd in proposal.derived_dimensions:
                if dd.dimension_name not in existing_names:
                    result.derived_dimensions.append(dd)
                    existing_names.add(dd.dimension_name)

        # ── metrics：仅当 proposal 非空且 spec 为空时写入 ──
        if proposal.metrics and not result.metrics:
            result.metrics = list(proposal.metrics)

        # ── case_when_rules：非空时追加，同 output_column 不重复 ──
        if proposal.case_when_rules:
            existing_cols = {r.output_column for r in result.case_when_rules}
            for rule in proposal.case_when_rules:
                if rule.output_column not in existing_cols:
                    result.case_when_rules.append(rule)
                    existing_cols.add(rule.output_column)

        return result
