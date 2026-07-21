"""ProposalPromotion 全场景测试——验证仅追加不覆盖语义。"""

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    CaseWhenBranch,
    CaseWhenRule,
    ColumnDecl,
    DatasetType,
    DerivedDimensionDecl,
    DimensionDecl,
    InputTableDecl,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    RequirementProposal,
)
from tianshu_datadev.planning.proposal_promotion import ProposalPromotion


class TestProposalPromotion:
    """ProposalPromotion 全场景测试。"""

    def _make_empty_spec(self) -> ParsedDeveloperSpec:
        """构造一个全部业务字段为空的 spec。"""
        return ParsedDeveloperSpec(
            spec_id="spec-001",
            spec_hash="abc123",
            title="测试需求",
            description="测试描述",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[
                        ColumnDecl(
                            column_name="borough",
                            normalized_name="borough",
                            data_type="varchar",
                        ),
                        ColumnDecl(
                            column_name="amount",
                            normalized_name="amount",
                            data_type="decimal",
                        ),
                    ],
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="total_amount"),
                ],
                grain=["borough"],
            ),
            metrics=[],
            dimensions=[],
        )

    def _make_proposal_with_data(self) -> RequirementProposal:
        """构造一个含全部字段的 proposal。"""
        return RequirementProposal(
            proposal_id="prop-001",
            spec_hash="abc123",
            dimensions=[
                DimensionDecl(
                    dimension_name="borough",
                    column_ref="borough",
                    source_table="ft",
                ),
            ],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="hour",
                    source_column="created_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="total_amount",
                    aggregation=AggregationType.SUM,
                    input_column="amount",
                    alias="total_amount",
                ),
            ],
            case_when_rules=[
                CaseWhenRule(
                    output_column="borough_label",
                    branches=[
                        CaseWhenBranch(
                            condition={
                                "node_type": "COMPARE",
                                "left": "borough",
                                "op": "EQ",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": "Manhattan",
                                    "data_type": "string",
                                },
                            },
                            then_value="城区",
                        ),
                    ],
                    else_value="其他",
                ),
            ],
        )

    # ════════════════════════════════════════
    # 场景 1：正常 Promotion——全部字段写入空 spec
    # ════════════════════════════════════════
    def test_full_promotion_to_empty_spec(self):
        """正常 Promotion——proposal 全部字段写入空 spec 的业务字段。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()

        result = prompter.promote(proposal, spec)

        # dimensions 应写入
        assert len(result.dimensions) == 1
        assert result.dimensions[0].dimension_name == "borough"

        # derived_dimensions 应写入
        assert len(result.derived_dimensions) == 1
        assert result.derived_dimensions[0].dimension_name == "hour"

        # metrics 应写入
        assert len(result.metrics) == 1
        assert result.metrics[0].alias == "total_amount"

        # case_when_rules 应写入
        assert len(result.case_when_rules) == 1
        assert result.case_when_rules[0].output_column == "borough_label"

        # 原 spec 不变（浅拷贝独立）
        assert len(spec.dimensions) == 0

    # ════════════════════════════════════════
    # 场景 2：不覆盖已有 dimensions
    # ════════════════════════════════════════
    def test_does_not_overwrite_existing_dimensions(self):
        """spec 已有维度时 proposal 维度被忽略。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        # spec 已有手写维度
        spec.dimensions.append(
            DimensionDecl(
                dimension_name="borough",
                column_ref="borough",
            ),
        )

        result = prompter.promote(proposal, spec)

        # dimensions 应保持不变（只有手写的那条）
        assert len(result.dimensions) == 1
        assert result.dimensions[0].dimension_name == "borough"
        # derived_dimensions、metrics、case_when_rules 不受影响
        assert len(result.derived_dimensions) == 1
        assert len(result.metrics) == 1
        assert len(result.case_when_rules) == 1

    # ════════════════════════════════════════
    # 场景 3：不覆盖已有 metrics
    # ════════════════════════════════════════
    def test_does_not_overwrite_existing_metrics(self):
        """spec 已有指标时 proposal 指标被忽略。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        # spec 已有手写指标
        spec.metrics.append(
            MetricDecl(
                metric_name="total_amount",
                aggregation=AggregationType.SUM,
                input_column="amount",
                alias="total_amount",
            ),
        )

        result = prompter.promote(proposal, spec)

        # metrics 应保持不变（只有手写的那条）
        assert len(result.metrics) == 1
        assert result.metrics[0].metric_name == "total_amount"
        # dimensions、derived_dimensions、case_when_rules 正常写入
        assert len(result.dimensions) == 1
        assert len(result.derived_dimensions) == 1
        assert len(result.case_when_rules) == 1

    # ════════════════════════════════════════
    # 场景 4：派生维度去重
    # ════════════════════════════════════════
    def test_dedup_derived_dimensions(self):
        """同名派生维度不重复追加。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        # spec 已有同名派生维度
        spec.derived_dimensions.append(
            DerivedDimensionDecl(
                dimension_name="hour",
                source_column="created_at",
                source_table="ft",
                time_function="HOUR",
            ),
        )

        result = prompter.promote(proposal, spec)

        # 同名不重复追加——还是只有原来的 1 条
        assert len(result.derived_dimensions) == 1
        assert result.derived_dimensions[0].dimension_name == "hour"

    def test_dedup_derived_dimensions_partial(self):
        """同名去重不影响其他不同名派生维度的追加。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        spec.derived_dimensions.append(
            DerivedDimensionDecl(
                dimension_name="hour",
                source_column="created_at",
                source_table="ft",
                time_function="HOUR",
            ),
        )
        # proposal 再加一条不同的派生维度
        proposal.derived_dimensions.append(
            DerivedDimensionDecl(
                dimension_name="minute",
                source_column="created_at",
                source_table="ft",
                time_function="HOUR",
            ),
        )

        result = prompter.promote(proposal, spec)

        # hour 保持原值，minute 追加
        assert len(result.derived_dimensions) == 2
        names = {d.dimension_name for d in result.derived_dimensions}
        assert names == {"hour", "minute"}

    # ════════════════════════════════════════
    # 场景 5：CASE WHEN 去重
    # ════════════════════════════════════════
    def test_dedup_case_when_rules(self):
        """同 output_column 的 CASE WHEN 规则不重复追加。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        # spec 已有同 output_column 的规则
        spec.case_when_rules.append(
            CaseWhenRule(
                output_column="borough_label",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "COMPARE",
                            "left": "borough",
                            "op": "EQ",
                            "right": {
                                "node_type": "LITERAL",
                                "value": "Brooklyn",
                                "data_type": "string",
                            },
                        },
                        then_value="布鲁克林",
                    ),
                ],
                else_value="默认",
            ),
        )

        result = prompter.promote(proposal, spec)

        # 同 output_column 不重复追加——只有原来的 1 条
        assert len(result.case_when_rules) == 1
        assert result.case_when_rules[0].output_column == "borough_label"
        # 内容保持原来的
        assert result.case_when_rules[0].branches[0].then_value == "布鲁克林"

    def test_dedup_case_when_rules_partial(self):
        """同 output_column 去重不影响其他不同名的规则追加。"""
        prompter = ProposalPromotion()
        proposal = self._make_proposal_with_data()
        spec = self._make_empty_spec()
        spec.case_when_rules.append(
            CaseWhenRule(
                output_column="borough_label",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "COMPARE",
                            "left": "borough",
                            "op": "EQ",
                            "right": {
                                "node_type": "LITERAL",
                                "value": "Brooklyn",
                                "data_type": "string",
                            },
                        },
                        then_value="布鲁克林",
                    ),
                ],
                else_value="默认",
            ),
        )
        # proposal 再加一条不同的规则
        proposal.case_when_rules.append(
            CaseWhenRule(
                output_column="risk_label",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "COMPARE",
                            "left": "amount",
                            "op": "GT",
                            "right": {
                                "node_type": "LITERAL",
                                "value": 1000,
                                "data_type": "number",
                            },
                        },
                        then_value="高风险",
                    ),
                ],
                else_value="低风险",
            ),
        )

        result = prompter.promote(proposal, spec)

        # borough_label 不重复，risk_label 追加
        assert len(result.case_when_rules) == 2
        cols = {r.output_column for r in result.case_when_rules}
        assert cols == {"borough_label", "risk_label"}

    # ════════════════════════════════════════
    # 场景 6：空 Proposal 返回原 spec 不变
    # ════════════════════════════════════════
    def test_empty_proposal_returns_spec_unchanged(self):
        """空 Proposal 返回原 spec 不变。"""
        prompter = ProposalPromotion()
        proposal = RequirementProposal(
            proposal_id="prop-empty",
            spec_hash="abc123",
        )
        spec = self._make_empty_spec()

        result = prompter.promote(proposal, spec)

        # 所有字段保持空
        assert len(result.dimensions) == 0
        assert len(result.derived_dimensions) == 0
        assert len(result.metrics) == 0
        assert len(result.case_when_rules) == 0
        # 非业务字段不受影响
        assert result.spec_id == "spec-001"
        assert result.title == "测试需求"

    # ════════════════════════════════════════
    # 场景 7：空 proposal 非空字段不影响有内容的 spec
    # ════════════════════════════════════════
    def test_empty_proposal_does_not_clear_existing(self):
        """空 Proposal 不应清空 spec 已有内容。"""
        prompter = ProposalPromotion()
        proposal = RequirementProposal(
            proposal_id="prop-empty",
            spec_hash="abc123",
        )
        spec = self._make_empty_spec()
        spec.dimensions.append(
            DimensionDecl(
                dimension_name="borough",
                column_ref="borough",
            ),
        )

        result = prompter.promote(proposal, spec)

        # 已有内容不应被清空
        assert len(result.dimensions) == 1
