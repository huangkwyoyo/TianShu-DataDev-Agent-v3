"""管线测试——_find_unresolved_derived_columns()、_prepare_spec_for_planning()。"""

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    ColumnDecl,
    CompareOp,
    DatasetType,
    InputTableDecl,
    LabelCompare,
    LabelPredicateBranch,
    LabelTypedLiteral,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns


def _make_spec(output_cols, source_cols=None):
    """构造最小 ParsedDeveloperSpec——用于 resolver 单元测试。"""
    cols = source_cols if source_cols is not None else output_cols
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="测试", description="",
        dataset_type=DatasetType.UNSPECIFIED,
        input_tables=[InputTableDecl(
            table_alias="t", source_table="test",
            columns=[ColumnDecl(column_name=c, normalized_name=c) for c in cols],
            key_columns=[], business_columns=[],
        )],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name=c, type="string") for c in output_cols],
            grain=[],
        ),
        time_range=None,
    )


class TestFindUnresolvedDerivedColumns:

    def test_all_physical_returns_empty(self):
        spec = _make_spec(["col1", "col2"])
        assert _find_unresolved_derived_columns(spec) == []

    def test_derived_column_detected(self):
        spec = _make_spec(["col1", "derived_col"], source_cols=["col1"])
        unresolved = _find_unresolved_derived_columns(spec)
        assert "derived_col" in unresolved
        assert "col1" not in unresolved

    def test_all_derived(self):
        spec = _make_spec(["label_a", "label_b"], source_cols=["col1", "col2"])
        unresolved = _find_unresolved_derived_columns(spec)
        assert sorted(unresolved) == ["label_a", "label_b"]

    def test_label_rule_output_excluded(self):
        spec = _make_spec(["distance_category"], source_cols=["distance_miles"])
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
        unresolved = _find_unresolved_derived_columns(spec)
        assert "distance_category" not in unresolved
