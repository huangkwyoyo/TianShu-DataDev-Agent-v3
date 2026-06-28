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
