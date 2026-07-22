"""ProposalValidator 全检查项测试（V1-V11）。"""


from tianshu_datadev.developer_spec.models import (
    AggregationType,
    CaseWhenBranch,
    CaseWhenRule,
    ColumnDecl,
    DatasetType,
    DerivedDimensionDecl,
    DimensionDecl,
    InputTableDecl,
    ManifestColumn,
    ManifestTable,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    RequirementProposal,
    SourceManifest,
)
from tianshu_datadev.planning.proposal_validator import ProposalValidator


class TestProposalValidator:
    """ProposalValidator 全检查项测试。"""

    def _make_proposal(self, **overrides) -> RequirementProposal:
        """构造标准 Proposal——默认通过全部校验。"""
        defaults = {
            "proposal_id": "test-001",
            "spec_hash": "abc123",
            "dimensions": [
                DimensionDecl(
                    dimension_name="borough",
                    column_ref="borough",
                    source_table="ft",
                ),
            ],
            "derived_dimensions": [
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            "metrics": [
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            "case_when_rules": [
                CaseWhenRule(
                    output_column="peak_type",
                    branches=[
                        CaseWhenBranch(
                            condition={
                                "node_type": "COMPARE",
                                "left": "pickup_hour",
                                "op": "IN",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": [7, 8, 9],
                                    "data_type": "number",
                                },
                            },
                            then_value="高峰",
                        ),
                    ],
                    else_value="平峰",
                ),
            ],
        }
        defaults.update(overrides)
        return RequirementProposal(**defaults)

    def _make_spec(self) -> ParsedDeveloperSpec:
        """构造标准 Spec。"""
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
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ColumnDecl(
                            column_name="borough",
                            normalized_name="borough",
                            data_type="varchar",
                        ),
                    ],
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                    OutputColumnDecl(name="peak_type"),
                ],
                grain=["borough", "pickup_hour"],
            ),
            metrics=[],
            dimensions=[],
        )

    def _make_manifest(self) -> SourceManifest:
        """构造标准 SourceManifest。"""
        return SourceManifest(
            manifest_id="manifest_001",
            spec_hash="manifest_001",
            tables=[
                ManifestTable(
                    table_ref="ft",
                    source_table="fact_table",
                    columns=[
                        ManifestColumn(
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ManifestColumn(
                            column_name="borough",
                            normalized_name="borough",
                            data_type="varchar",
                        ),
                    ],
                ),
            ],
        )

    # ════════════════════════════════════════
    # V1: dimension column_ref 存在于 SourceManifest
    # ════════════════════════════════════════
    def test_v1_unknown_column_ref(self):
        """V1: dimension 引用了 SourceManifest 中不存在的列则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(dimensions=[
            DimensionDecl(
                dimension_name="unknown_col",
                column_ref="nonexistent",
                source_table="ft",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V1" for q in questions)

    # ════════════════════════════════════════
    # V2: derived_dimension source_column 存在于 SourceManifest
    # ════════════════════════════════════════
    def test_v2_unknown_source_column(self):
        """V2: derived_dimension 的 source_column 不在 manifest 中则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(derived_dimensions=[
            DerivedDimensionDecl(
                dimension_name="bad_col",
                source_column="no_such_column",
                source_table="ft",
                time_function="HOUR",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V2" for q in questions)

    # ════════════════════════════════════════
    # V3: time_function 白名单
    # ════════════════════════════════════════
    def test_v3_invalid_time_function(self):
        """V3: 不支持的时间函数则阻断。

        注：DerivedDimensionDecl.time_function 在 Schema 层已是 Literal["HOUR"]，
        此处使用 model_construct 绕过 Pydantic 校验，验证 Validator 纵深防御。
        """
        validator = ProposalValidator()
        proposal = self._make_proposal()
        # model_construct 不触发 Literal 校验，可由 Validator 拦截
        proposal.derived_dimensions = [
            DerivedDimensionDecl.model_construct(
                dimension_name="pickup_day",
                source_column="pickup_at",
                source_table="ft",
                time_function="DAY",
            ),
        ]
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V3" for q in questions)

    # ════════════════════════════════════════
    # V4: metric alias 非空
    # ════════════════════════════════════════
    def test_v4_empty_metric_alias(self):
        """V4: 指标 alias 为空则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(metrics=[
            MetricDecl(
                metric_name="test_metric",
                aggregation=AggregationType.COUNT,
                alias="",  # 空 alias
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V4" for q in questions)

    # ════════════════════════════════════════
    # V5: metric alias 唯一性
    # ════════════════════════════════════════
    def test_v5_duplicate_metric_alias(self):
        """V5: alias 重复则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(metrics=[
            MetricDecl(
                metric_name="count_a",
                aggregation=AggregationType.COUNT,
                alias="dup_alias",
            ),
            MetricDecl(
                metric_name="count_b",
                aggregation=AggregationType.COUNT,
                alias="dup_alias",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V5" for q in questions)

    # ════════════════════════════════════════
    # V6: derived_dimension dimension_name 唯一性
    # ════════════════════════════════════════
    def test_v6_duplicate_derived_dimension_name(self):
        """V6: derived_dimension 名称重复则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(derived_dimensions=[
            DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_at",
                source_table="ft",
                time_function="HOUR",
            ),
            DerivedDimensionDecl(
                dimension_name="pickup_hour",  # 重复
                source_column="pickup_at",
                source_table="ft",
                time_function="HOUR",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V6" for q in questions)

    # ════════════════════════════════════════
    # V7: CASE WHEN branches 非空
    # ════════════════════════════════════════
    def test_v7_empty_branches(self):
        """V7: CASE WHEN 分支列表为空则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[],
                else_value="默认值",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V7" for q in questions)

    # ════════════════════════════════════════
    # V8: CASE WHEN else_value 非空
    # ════════════════════════════════════════
    def test_v8_missing_else(self):
        """V8: CASE WHEN ELSE 为空则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "COMPARE",
                            "left": "pickup_hour",
                            "op": "GT",
                            "right": {
                                "node_type": "LITERAL",
                                "value": 7,
                                "data_type": "number",
                            },
                        },
                        then_value="高峰",
                    ),
                ],
                else_value="",  # 空 ELSE
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V8" for q in questions)

    # ════════════════════════════════════════
    # V9: CASE WHEN condition 列引用存在性
    # ════════════════════════════════════════
    def test_v9_unknown_condition_ref(self):
        """V9: CASE WHEN 条件引用了不存在的名称则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "COMPARE",
                            "left": "nonexistent_col",
                            "op": "GT",
                            "right": {
                                "node_type": "LITERAL",
                                "value": 0,
                                "data_type": "number",
                            },
                        },
                        then_value="有效",
                    ),
                ],
                else_value="无效",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V9" for q in questions)

    def test_v9_refers_to_derived_dimension(self):
        """V9: 条件引用已声明的 derived_dimension 应通过。"""
        validator = ProposalValidator()
        # 默认 proposal 的 CASE WHEN 引用了 pickup_hour（derived_dimension）
        proposal = self._make_proposal()
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        # 不涉及 V9 错误
        assert not any(q.question_id == "V9" for q in questions)

    # ════════════════════════════════════════
    # V10b: LabelNot 节点拒绝
    # ════════════════════════════════════════
    def test_v10b_rejects_label_not(self):
        """V10b: CASE WHEN 条件含 NOT 节点则阻断。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "NOT",
                            "child": {
                                "node_type": "IS_NULL",
                                "column": "borough",
                            },
                        },
                        then_value="有值",
                    ),
                ],
                else_value="无值",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V10b" for q in questions)

    def test_v10b_allows_is_not_null(self):
        """V10b: IS_NOT_NULL 不含 NOT 节点，应通过。"""
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "IS_NOT_NULL",
                            "column": "borough",
                        },
                        then_value="有值",
                    ),
                ],
                else_value="无值",
            ),
        ])
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert valid
        assert not any(q.question_id == "V10b" for q in questions)

    # ════════════════════════════════════════
    # V11: 与程序员手写字段冲突
    # ════════════════════════════════════════
    def test_v11_conflict_with_declared_dimension(self):
        """V11: proposal 维度与 spec 中手写维度名称冲突则阻断。"""
        validator = ProposalValidator()
        spec = self._make_spec()
        # 给 spec 加一个手写维度
        spec.dimensions.append(
            DimensionDecl(
                dimension_name="borough",
                column_ref="borough",
            ),
        )
        proposal = self._make_proposal()
        valid, questions = validator.validate(
            proposal, spec, self._make_manifest()
        )
        assert not valid
        assert any(q.question_id == "V11" for q in questions)

    # ════════════════════════════════════════
    # 正常通过
    # ════════════════════════════════════════
    def test_valid_proposal_passes(self):
        """标准 Proposal 应通过全部校验。"""
        validator = ProposalValidator()
        proposal = self._make_proposal()
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert valid
        assert len(questions) == 0

    # ════════════════════════════════════════
    # 混合多问题场景
    # ════════════════════════════════════════
    def test_multiple_violations(self):
        """多个问题同时存在时应报告所有问题。"""
        validator = ProposalValidator()
        # 构造带多种违规的 proposal
        proposal = self._make_proposal(
            dimensions=[
                DimensionDecl(
                    dimension_name="bad_dim",
                    column_ref="no_such_column",
                    source_table="ft",
                ),
            ],
            metrics=[],
            case_when_rules=[
                CaseWhenRule(
                    output_column="peak_type",
                    branches=[],
                    else_value="",
                ),
            ],
        )
        # 使用 model_construct 绕过 Literal 校验设置非法 time_function
        proposal.derived_dimensions = [
            DerivedDimensionDecl.model_construct(
                dimension_name="bad_derived",
                source_column="no_such_column",
                source_table="ft",
                time_function="YEAR",
            ),
        ]
        valid, questions = validator.validate(
            proposal, self._make_spec(), self._make_manifest()
        )
        assert not valid
        # 应至少报告 V1, V2, V3, V7, V8 五个问题
        question_ids = {q.question_id for q in questions}
        assert "V1" in question_ids
        assert "V2" in question_ids
        assert "V3" in question_ids
        assert "V7" in question_ids
        assert "V8" in question_ids
