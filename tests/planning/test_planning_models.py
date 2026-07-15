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
    DatasetType, CompareOp,
    LabelColumnRef, LabelTypedLiteral,
    LabelCompare, LabelIsNull, LabelIsNotNull,
    LabelAnd, LabelOr, LabelNot,
    LabelPredicateNode, LabelPredicateCondition,
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
