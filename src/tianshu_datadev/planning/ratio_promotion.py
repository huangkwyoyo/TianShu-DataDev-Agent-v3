"""比率候选提升——只做类型化字段复制，不参与语义判断。"""

from tianshu_datadev.developer_spec.models import RatioDecl, RatioProposal


class RatioPromotion:
    """将已通过确定性校验的 RatioProposal 提升为正式声明。"""

    @staticmethod
    def promote(proposal: RatioProposal) -> RatioDecl:
        """复制封闭字段，禁止生成或解析自由表达式。"""
        return RatioDecl(
            output_alias=proposal.output_alias,
            numerator_alias=proposal.numerator_alias,
            denominator_alias=proposal.denominator_alias,
            zero_division=proposal.zero_division,
            multiplier=proposal.multiplier,
        )
