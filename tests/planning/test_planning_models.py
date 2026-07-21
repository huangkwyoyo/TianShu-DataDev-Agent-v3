"""测试 planning 模块模型的 Schema 严格性 + 禁止自由 SQL 字段。"""


import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    ColumnRef,
    JoinType,
    Predicate,
    PredicateOperator,
    SqlLiteral,
)
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinCandidate,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)

# ── 辅助 ──

def _make_col(table: str = "t", col: str = "c") -> ColumnRef:
    """创建测试用 ColumnRef。"""
    return ColumnRef(table_ref=table, column_name=col, normalized_name=col)


def _make_pred() -> Predicate:
    """创建测试用 Predicate。"""
    return Predicate(
        left=_make_col("t", "amt"),
        operator=PredicateOperator.GT,
        right=SqlLiteral(value=100),
    )


# ════════════════════════════════════════════
# Schema 严格性
# ════════════════════════════════════════════


class TestSchemaStrictness:
    """extra="forbid" 拒绝未知字段。"""

    def test_column_ref_rejects_extra(self):
        """ColumnRef 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            ColumnRef(
                table_ref="t", column_name="c", normalized_name="c",
                extra_field="x",
            )

    def test_predicate_rejects_extra(self):
        """Predicate 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            Predicate(
                left=_make_col(), operator=PredicateOperator.EQ,
                raw_sql="1=1",
            )

    def test_join_candidate_rejects_extra(self):
        """JoinCandidate 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            JoinCandidate(
                candidate_id="jc_01",
                left_table="t1", right_table="t2",
                left_key="a", right_key="b",
                left_key_normalized="a", right_key_normalized="b",
                join_on="t1.a = t2.b",
            )

    def test_sql_build_plan_rejects_extra(self):
        """SqlBuildPlan 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            SqlBuildPlan(
                plan_id="p1", spec_hash="abc",
                steps=[], raw_sql="SELECT * FROM t",
            )

    def test_scan_step_extra_rejected(self):
        """ScanStep 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            ScanStep(
                step_id="s1", table_ref="t",
                required_columns=[_make_col()],
                where_sql="WHERE 1=1",
            )


# ════════════════════════════════════════════
# Step 类型判别
# ════════════════════════════════════════════


class TestStepTypeLiterals:
    """验证每个 Step 的 step_type Literal 值。"""

    def test_scan_step_type(self):
        s = ScanStep(step_id="s", table_ref="t", required_columns=[_make_col()])
        assert s.step_type == "scan"

    def test_filter_step_type(self):
        s = FilterStep(step_id="s", predicate=_make_pred())
        assert s.step_type == "filter"

    def test_join_step_type(self):
        s = JoinStep(
            step_id="s", right_table_ref="t2",
            join_type=JoinType.INNER,
            relationship_ref="jc_01",
        )
        assert s.step_type == "join"

    def test_aggregate_step_type(self):
        s = AggregateStep(step_id="s", group_keys=[], metrics=[])
        assert s.step_type == "aggregate"

    def test_project_step_type(self):
        s = ProjectStep(step_id="s", columns=[])
        assert s.step_type == "project"

    def test_case_when_step_type(self):
        s = CaseWhenStep(step_id="s")
        assert s.step_type == "case_when"

    def test_sort_step_type(self):
        s = SortStep(step_id="s", order_by=[])
        assert s.step_type == "sort"

    def test_limit_step_type(self):
        s = LimitStep(step_id="s", limit=10)
        assert s.step_type == "limit"


# ════════════════════════════════════════════
# Predicate 嵌套与特殊操作符
# ════════════════════════════════════════════


class TestPredicateAdvanced:
    """Predicate 嵌套 AND/OR 和特殊操作符。"""

    def test_predicate_nested_and(self):
        """AND 嵌套——left 和 right 都是 Predicate。"""
        inner_left = Predicate(
            left=_make_col("t", "amt"),
            operator=PredicateOperator.GT,
            right=SqlLiteral(value=100),
        )
        inner_right = Predicate(
            left=_make_col("t", "status"),
            operator=PredicateOperator.EQ,
            right=SqlLiteral(value="active"),
        )
        outer = Predicate(
            left=inner_left,
            operator=PredicateOperator.AND,
            right=inner_right,
        )
        assert outer.operator == PredicateOperator.AND
        assert isinstance(outer.left, Predicate)
        assert isinstance(outer.right, Predicate)

    def test_predicate_in_with_list(self):
        """IN 操作符时 right 为 list[SqlLiteral]。"""
        p = Predicate(
            left=_make_col("t", "status"),
            operator=PredicateOperator.IN,
            right=[SqlLiteral(value="active"), SqlLiteral(value="pending")],
        )
        assert isinstance(p.right, list)
        assert len(p.right) == 2

    def test_predicate_is_null_no_right(self):
        """IS_NULL 时 right 为 None。"""
        p = Predicate(
            left=_make_col("t", "deleted_at"),
            operator=PredicateOperator.IS_NULL,
        )
        assert p.right is None


# ================================================
# v4-light 最终版: DatasetType + LabelPredicateNode + 根条件约束
# ================================================

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    CompareOp,
    DatasetType,
    LabelAnd,
    LabelBranchProposal,
    LabelBranchProposalOutput,
    LabelCompare,
    LabelDomain,
    # v4-light 最终版: LLM 输出层 + 系统层标签模型
    LabelDomainOutput,
    LabelIsNotNull,
    LabelIsNull,
    LabelNot,
    LabelOr,
    LabelPredicateBranch,
    LabelPredicateCondition,
    LabelPredicateNode,
    LabelRuleProposal,
    LabelRuleProposalOutput,
    LabelTypedLiteral,
)


class TestDatasetType:
    def test_serialize_label_table(self):
        assert DatasetType.LABEL_TABLE.value == "label_table"

    def test_default_unspecified(self):
        assert DatasetType.UNSPECIFIED.value == "unspecified"


class TestLabelPredicateNodeDiscriminator:
    """8 子类 discriminator 联合 AST。"""

    def test_compare_node(self):
        node = LabelCompare(
            left="distance_miles", op=CompareOp.LTE,
            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
        )
        assert node.node_type == "COMPARE"

    def test_is_null_node(self):
        node = LabelIsNull(column="distance_miles")
        assert node.node_type == "IS_NULL"

    def test_is_not_null_node(self):
        node = LabelIsNotNull(column="distance_miles")
        assert node.node_type == "IS_NOT_NULL"

    def test_and_node(self):
        node = LabelAnd(children=[
            LabelCompare(left="a", op=CompareOp.GT,
                        right=LabelTypedLiteral(value=Decimal("0"), data_type="number")),
            LabelCompare(left="a", op=CompareOp.LT,
                        right=LabelTypedLiteral(value=Decimal("10"), data_type="number")),
        ])
        assert node.node_type == "AND"
        assert len(node.children) == 2

    def test_or_node(self):
        node = LabelOr(children=[
            LabelIsNull(column="x"),
            LabelCompare(left="y", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value=True, data_type="boolean")),
        ])
        assert node.node_type == "OR"

    def test_not_node(self):
        node = LabelNot(child=LabelIsNull(column="x"))
        assert node.node_type == "NOT"

    def test_nested_and_or(self):
        """AND(OR(...), COMPARE) 嵌套。"""
        node = LabelAnd(children=[
            LabelOr(children=[
                LabelCompare(left="a", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value="x", data_type="string")),
                LabelCompare(left="a", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value="y", data_type="string")),
            ]),
            LabelIsNotNull(column="b"),
        ])
        assert node.node_type == "AND"


class TestLabelPredicateConditionRootConstraint:
    """v4-light 最终版: LabelPredicateCondition 仅允许 6 种根节点类型。
    LITERAL/COLUMN_REF 不可作为 WHEN 根条件。"""

    def test_compare_is_valid_root(self):
        """COMPARE 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "COMPARE", "left": "col",
            "op": "=",
            "right": {"node_type": "LITERAL", "value": "test", "data_type": "string"},
        })
        assert node.node_type == "COMPARE"

    def test_is_null_is_valid_root(self):
        """IS_NULL 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "IS_NULL", "column": "col",
        })
        assert node.node_type == "IS_NULL"

    def test_and_is_valid_root(self):
        """AND 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "AND", "children": [
                {"node_type": "COMPARE", "left": "a", "op": ">",
                 "right": {"node_type": "LITERAL", "value": 0, "data_type": "number"}},
            ],
        })
        assert node.node_type == "AND"

    def test_literal_rejected_as_root(self):
        """LITERAL 不可作根条件——Pydantic discriminator 拒绝。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        with pytest.raises(ValidationError):
            adapter.validate_python({
                "node_type": "LITERAL", "value": "short", "data_type": "string",
            })

    def test_column_ref_rejected_as_root(self):
        """COLUMN_REF 不可作根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        with pytest.raises(ValidationError):
            adapter.validate_python({
                "node_type": "COLUMN_REF", "column_name": "col",
            })

    def test_label_predicate_node_still_allows_literal(self):
        """LabelPredicateNode（完整 AST）仍允许 LITERAL/COLUMN_REF——
        仅 LabelPredicateCondition 限制了根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateNode)
        node = adapter.validate_python({
            "node_type": "LITERAL", "value": 5, "data_type": "number",
        })
        assert node.node_type == "LITERAL"


# ================================================
# v4-light 最终版: LLM 输出 Schema 与系统模型分离 + 必需字段强制
# ================================================


class TestLabelDomainOutput:
    """LLM 输出的标签值域——不含系统字段。"""

    def test_llm_output_no_system_fields(self):
        domain = LabelDomainOutput(
            values=["unknown", "short", "medium", "long"],
            source_evidence="分为四类",
            is_exhaustive=True,
            completeness_evidence="以上四类覆盖全部",
        )
        assert "domain_id" not in LabelDomainOutput.model_fields


class TestLabelRuleProposalOutput:
    """LLM 输出不含 proposal_id/source_spec_hash。"""

    def test_forbidden_system_fields(self):
        output = LabelRuleProposalOutput(
            output_column="distance_category",
            branches=[
                LabelBranchProposalOutput(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomainOutput(values=["short", "long"]),
        )
        assert "proposal_id" not in LabelRuleProposalOutput.model_fields
        assert "source_spec_hash" not in LabelRuleProposalOutput.model_fields

    def test_literal_root_condition_rejected_in_branch(self):
        """LITERAL 不可作 LabelBranchProposalOutput 的 condition。"""
        with pytest.raises(ValidationError):
            LabelBranchProposalOutput(
                condition=LabelTypedLiteral(value="short", data_type="string"),
                then_label="short",
                evidence="非法根条件",
            )


class TestSystemModelRequiredFields:
    """系统模型——else_value/label_domain/evidence 均为必需。"""

    def test_else_value_required(self):
        """else_value 为必填 str——不可为 None 或缺失。"""
        with pytest.raises(ValidationError):
            LabelRuleProposal(
                proposal_id="p1", source_spec_hash="h",
                output_column="distance_category",
                branches=[
                    LabelBranchProposal(
                        condition=LabelCompare(
                            left="distance_miles", op=CompareOp.LTE,
                            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                        ),
                        then_label="short",
                        evidence="<=2 -> short",
                    ),
                ],
                # else_value 缺失 → ValidationError
            )

    def test_label_domain_required(self):
        """label_domain 为必填 LabelDomain——不可为 None 或缺失。"""
        with pytest.raises(ValidationError):
            LabelRuleProposal(
                proposal_id="p1", source_spec_hash="h",
                output_column="distance_category",
                branches=[
                    LabelBranchProposal(
                        condition=LabelCompare(
                            left="distance_miles", op=CompareOp.LTE,
                            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                        ),
                        then_label="short",
                        evidence="<=2 -> short",
                    ),
                ],
                else_value="long",
                # label_domain 缺失 → ValidationError
            )

    def test_evidence_required_in_branch(self):
        """evidence 为必填 str——空字符串导致 Promotion 拒绝。"""
        # Pydantic 层允许空字符串（非 None），但 Promotion 检查非空
        branch = LabelBranchProposal(
            condition=LabelCompare(
                left="distance_miles", op=CompareOp.LTE,
                right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
            ),
            then_label="short",
            evidence="",  # 空字符串——Promotion 阶段拒绝
        )
        assert branch.evidence == ""

    def test_system_model_has_id_and_domain_fields(self):
        """系统层含 proposal_id/source_spec_hash/label_domain/else_value。"""
        proposal = LabelRuleProposal(
            proposal_id="sys_gen_001",
            source_spec_hash="hash_abc",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(
                domain_id="dom_001",
                values=["short", "long"],
                source_evidence="原文分类",
            ),
        )
        assert proposal.proposal_id == "sys_gen_001"
        assert proposal.else_value == "long"
        assert proposal.label_domain.values == ["short", "long"]


# ================================================
# Task 12: Builder——_predicate_from_label_node() + _build_case_when_steps()
# ================================================


class TestPredicateFromLabelNode:
    """验证 LabelPredicateCondition AST → Predicate 转换。"""

    def test_compare_to_predicate(self):
        """LabelCompare(LTE)→Predicate。"""
        from decimal import Decimal

        from tianshu_datadev.developer_spec.models import (
            CompareOp,
            LabelCompare,
            LabelTypedLiteral,
        )
        from tianshu_datadev.planning.models import PredicateOperator
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        builder = SqlBuildPlanBuilder()
        node = LabelCompare(
            left="distance_miles", op=CompareOp.LTE,
            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
        )
        pred = builder._predicate_from_label_node(node, "tf")
        assert pred.operator == PredicateOperator.LTE
        assert pred.left.column_name == "distance_miles"
        assert pred.right.value == 2.0  # Decimal→float

    def test_is_null_to_predicate(self):
        """LabelIsNull→Predicate(IS_NULL)。"""
        from tianshu_datadev.developer_spec.models import LabelIsNull
        from tianshu_datadev.planning.models import PredicateOperator
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        builder = SqlBuildPlanBuilder()
        node = LabelIsNull(column="distance_miles")
        pred = builder._predicate_from_label_node(node, "tf")
        assert pred.operator == PredicateOperator.IS_NULL
        assert pred.left.column_name == "distance_miles"

    def test_and_nested_to_predicate(self):
        """LabelAnd(COMPARE, IS_NULL)→嵌套 Predicate。"""
        from decimal import Decimal

        from tianshu_datadev.developer_spec.models import (
            CompareOp,
            LabelAnd,
            LabelCompare,
            LabelIsNull,
            LabelTypedLiteral,
        )
        from tianshu_datadev.planning.models import PredicateOperator
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        builder = SqlBuildPlanBuilder()
        node = LabelAnd(children=[
            LabelCompare(
                left="distance_miles", op=CompareOp.GT,
                right=LabelTypedLiteral(value=Decimal("0"), data_type="number"),
            ),
            LabelIsNull(column="flag"),
        ])
        pred = builder._predicate_from_label_node(node, "tf")
        assert pred.operator == PredicateOperator.AND
        # 左结合折叠：left 是第一个 Compare，right 是 IS_NULL
        assert pred.left.operator == PredicateOperator.GT
        assert pred.right.operator == PredicateOperator.IS_NULL

    def test_not_to_predicate(self):
        """LabelNot → ValueError——label_table v1 暂不支持 LabelNot。"""
        import pytest

        from tianshu_datadev.developer_spec.models import LabelIsNull, LabelNot
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        builder = SqlBuildPlanBuilder()
        node = LabelNot(child=LabelIsNull(column="flag"))
        with pytest.raises(ValueError, match="LabelNot"):
            builder._predicate_from_label_node(node, "tf")


class TestBuildCaseWhenSteps:
    """验证 spec.label_rules → CaseWhenStep 列表生成。"""

    def test_single_rule_generates_one_step(self):
        """一条 CaseWhenDecl→一个 CaseWhenStep。"""
        from decimal import Decimal

        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl,
            ColumnDecl,
            CompareOp,
            DatasetType,
            InputTableDecl,
            LabelCompare,
            LabelTypedLiteral,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="t", description="",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="distance_miles",
                                   normalized_name="distance_miles",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="distance_category", type="string")],
                grain=[],
            ),
            time_range=None,
        )
        spec.label_rules.append(CaseWhenDecl(
            output_column="distance_category",
            else_value="long",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                ),
            ],
        ))

        builder = SqlBuildPlanBuilder()
        steps = builder._build_case_when_steps(spec)
        assert len(steps) == 1
        step = steps[0]
        assert step.step_type == "case_when"
        assert step.alias == "distance_category"
        assert len(step.cases) == 1
        assert step.cases[0].result.value == "short"
        assert step.else_value.value == "long"

    def test_build_single_table_includes_case_when(self):
        """_build_single_table 为 label_table spec 生成 CaseWhenStep。"""
        from decimal import Decimal

        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl,
            ColumnDecl,
            CompareOp,
            DatasetType,
            InputTableDecl,
            LabelCompare,
            LabelTypedLiteral,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep, SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="t", description="",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="distance_miles",
                                   normalized_name="distance_miles",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="distance_category", type="string")],
                grain=[],
            ),
            time_range=None,
        )
        spec.label_rules.append(CaseWhenDecl(
            output_column="distance_category",
            else_value="unknown",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                ),
            ],
        ))

        builder = SqlBuildPlanBuilder()
        steps = builder._build_single_table(spec)
        case_when_steps = [s for s in steps if isinstance(s, CaseWhenStep)]
        assert len(case_when_steps) == 1, (
            f"应有 1 个 CaseWhenStep，实际 step_types={[s.step_type for s in steps]}"
        )
        # 验证 Scan 不包含标签列
        from tianshu_datadev.planning.sql_build_plan import ScanStep
        scan = next(s for s in steps if isinstance(s, ScanStep))
        scan_col_names = [c.column_name for c in scan.required_columns]
        assert "distance_category" not in scan_col_names, (
            f"标签输出列不应出现在 Scan 中: {scan_col_names}"
        )
        assert "distance_miles" in scan_col_names, (
            f"标签条件源列应在 Scan 中: {scan_col_names}"
        )


# ================================================
# Task 12.1-2: DerivedColumnRuleMissingError 防御性检查
# ================================================


class TestDerivedColumnRuleMissingError:
    """label_table 输出列无解析规则 → DerivedColumnRuleMissingError——禁止回退为物理 ColumnRef。"""

    def test_unresolved_output_column_raises_error(self):
        """LABEL_TABLE 输出列无解析规则 → DerivedColumnRuleMissingError。"""
        import pytest

        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DatasetType,
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            DerivedColumnRuleMissingError,
            SqlBuildPlanBuilder,
        )

        # 构造 LABEL_TABLE spec，输出列 "unknown_col" 不是物理列也没有 label_rules
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="t", description="",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="col1", normalized_name="col1",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="unknown_col", type="string")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        with pytest.raises(DerivedColumnRuleMissingError, match="unknown_col"):
            builder._build_single_table(spec)

    def test_detail_table_unresolved_raises_error(self):
        """DETAIL_TABLE 输出列无解析规则 → DerivedColumnRuleMissingError——所有类型统一门禁。"""
        import pytest

        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DatasetType,
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            DerivedColumnRuleMissingError,
            SqlBuildPlanBuilder,
        )

        # 构造 DETAIL_TABLE spec——输出列 unknown_col 无解析规则（不是物理列也没有其他声明）
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h2", title="t", description="",
            dataset_type=DatasetType.DETAIL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="col1", normalized_name="col1",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="unknown_col", type="string")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        # 所有类型统一门禁——未解析列提前阻断，避免到 execute 阶段才报 Binder Error
        with pytest.raises(DerivedColumnRuleMissingError, match="unknown_col"):
            builder._build_single_table(spec)

    def test_detail_table_resolved_passes(self):
        """DETAIL_TABLE 输出列均为物理列 → 防御检查通过（所有类型统一门禁）。"""
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DatasetType,
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        # 构造 DETAIL_TABLE spec——输出列 col1 是源表物理列
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h2r", title="t", description="",
            dataset_type=DatasetType.DETAIL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="col1", normalized_name="col1",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="col1", type="double")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        plan = builder._build_single_table(spec)
        assert plan is not None

    def test_builder_bypass_multi_table_raises_error(self):
        """Builder 直接调用绕过 Pipeline——多表 LABEL_TABLE → scope 检查抛异常。"""
        import pytest

        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DatasetType,
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            DerivedColumnRuleMissingError,
            SqlBuildPlanBuilder,
        )

        # 构造多表 LABEL_TABLE spec——绕过 Pipeline 直接调 Builder
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h3", title="t", description="",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="t1", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="col1", normalized_name="col1",
                                   data_type="double"),
                    ],
                    key_columns=[], business_columns=[],
                ),
                InputTableDecl(
                    table_alias="t2", source_table="dim",
                    columns=[
                        ColumnDecl(column_name="col2", normalized_name="col2",
                                   data_type="string"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="col1", type="double")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        with pytest.raises(DerivedColumnRuleMissingError, match="单表"):
            builder._build_single_table(spec)
