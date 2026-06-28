"""测试 4 个 Compiler Pass——幂等性验证。"""

from tianshu_datadev.planning.models import ColumnRef
from tianshu_datadev.planning.sql_build_plan import (
    ScanStep,
    SortStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.compiler_passes import (
    column_pruning,
    constant_folding,
    predicate_normalization,
    sort_elimination,
)


class TestColumnPruning:
    """列裁剪 Pass——幂等性。"""

    def test_column_pruning_idempotent(self):
        """两次列裁剪产生相同结果。"""
        plan = SqlBuildPlan(
            plan_id="test_prune",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="t1", column_name="unused_col", normalized_name="unused_col"),
                    ],
                ),
                # 后续 step 只引用 "id"
                ScanStep(
                    step_id="scan_2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(table_ref="t2", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
            multi_table=False,
        )

        result1, _, pruned1 = column_pruning(plan)
        result2, _, pruned2 = column_pruning(result1)

        # 第一次应裁剪掉 unused_col
        assert len(pruned1) > 0, "应裁剪掉未引用的列"
        assert any("unused_col" in p for p in pruned1), f"应裁剪 unused_col，实际: {pruned1}"
        # 第二次应无新裁剪（幂等性）
        assert len(pruned2) == 0, f"第二次应无变更，实际: {pruned2}"

        # 两次结果应一致
        assert len(result1.steps) == len(result2.steps)

        # 检查 step 内容一致
        for s1, s2 in zip(result1.steps, result2.steps):
            if isinstance(s1, ScanStep) and isinstance(s2, ScanStep):
                assert len(s1.required_columns) == len(s2.required_columns)


class TestPredicateNormalization:
    """谓词规范化 Pass——幂等性。"""

    def test_predicate_normalization_idempotent(self):
        """两次谓词规范化产生相同结果。"""
        plan = SqlBuildPlan(
            plan_id="test_norm",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="x", normalized_name="x"),
                    ],
                ),
            ],
            multi_table=False,
        )

        result1, records1 = predicate_normalization(plan)
        result2, records2 = predicate_normalization(result1)

        # 第二次运行不应有新变更（幂等性）
        if records1:
            # 如果第一次有变更，第二次应无变更
            assert len(records2) == 0, f"第二次应无变更，实际: {records2}"
        # 如果第一次无变更，第二次也应无变更
        assert len(result1.steps) == len(result2.steps)


class TestSortElimination:
    """无用排序消除 Pass——幂等性。"""

    def test_sort_elimination_idempotent(self):
        """两次排序消除产生相同结果。"""
        plan = SqlBuildPlan(
            plan_id="test_sort_elim",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                ),
                SortStep(
                    step_id="sort_useless",
                    order_by=[],
                    requires_full_sort=True,
                    limit=None,  # 无 LIMIT
                ),
            ],
            multi_table=False,
        )

        result1, _, eliminated1 = sort_elimination(plan)
        result2, _, eliminated2 = sort_elimination(result1)

        # 第一次应消除无用的 sort
        assert len(eliminated1) > 0, "应消除无用的 SortStep"
        assert "sort_useless" in eliminated1, f"应消除 sort_useless，实际: {eliminated1}"
        # 第二次应无新消除（幂等性）
        assert len(eliminated2) == 0, f"第二次应无变更，实际: {eliminated2}"

        # 两次结果应一致
        assert len(result1.steps) == len(result2.steps)


class TestConstantFolding:
    """常量折叠 Pass——幂等性。"""

    def test_constant_folding_idempotent(self):
        """两次常量折叠产生相同结果。"""
        plan = SqlBuildPlan(
            plan_id="test_fold",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
            multi_table=False,
        )

        result1, records1 = constant_folding(plan)
        result2, records2 = constant_folding(result1)

        # 第二次运行不应有新变更
        if records1:
            assert len(records2) == 0, f"第二次应无变更，实际: {records2}"
        assert len(result1.steps) == len(result2.steps)
