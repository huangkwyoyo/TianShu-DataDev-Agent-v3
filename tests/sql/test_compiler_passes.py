"""测试 4 个 Compiler Pass——直接调用 + 幂等性 warp 验证。

Phase 4B 核心要求：Compiler Pass 必须是幂等的——
相同 SqlBuildPlan 经任意次数运行产生相同输出。
本文件融合了原 tests/perf/test_compiler_pass_idempotent.py 的哈希验证变体。
"""

from tianshu_datadev.planning.models import ColumnRef, Predicate, PredicateOperator, SqlLiteral
from tianshu_datadev.planning.sql_build_plan import ScanStep, SortStep, SqlBuildPlan
from tianshu_datadev.sql.compiler_passes import (
    column_pruning,
    constant_folding,
    predicate_normalization,
    sort_elimination,
    verify_all_passes_idempotent,
    verify_column_pruning_idempotent,
    verify_constant_folding_idempotent,
    verify_predicate_normalization_idempotent,
    verify_sort_elimination_idempotent,
)


def _make_test_plan() -> SqlBuildPlan:
    """构造一个包含多种优化机会的 SqlBuildPlan——用于幂等验证。"""
    return SqlBuildPlan(
        plan_id="test_idempotent_001",
        spec_hash="abc123",
        steps=[
            ScanStep(
                step_id="scan_t1",
                table_ref="t1",
                required_columns=[
                    ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ColumnRef(table_ref="t1", column_name="name", normalized_name="name"),
                    ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                    ColumnRef(table_ref="t1", column_name="unused_col", normalized_name="unused_col"),
                ],
            ),
            SortStep(
                step_id="sort_no_limit",
                order_by=[],
                requires_full_sort=True,
                limit=None,
            ),
        ],
        multi_table=False,
    )


class TestColumnPruning:
    """列裁剪 Pass——幂等性：直接测试 + 最小计划变体 + warp 验证。"""

    def test_column_pruning_idempotent(self):
        """两次列裁剪产生相同结果——内部状态断言。"""
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
        assert len(pruned1) > 0, "应裁剪掉未引用的列"
        assert any("unused_col" in p for p in pruned1), f"应裁剪 unused_col，实际: {pruned1}"
        assert len(pruned2) == 0, f"第二次应无变更，实际: {pruned2}"
        assert len(result1.steps) == len(result2.steps)
        for s1, s2 in zip(result1.steps, result2.steps):
            if isinstance(s1, ScanStep) and isinstance(s2, ScanStep):
                assert len(s1.required_columns) == len(s2.required_columns)

    def test_column_pruning_idempotent_hash(self):
        """列裁剪两次运行 → 输出 hash 一致（warp 验证）。"""
        ok, detail = verify_column_pruning_idempotent(_make_test_plan())
        assert ok, detail

    def test_column_pruning_no_change_on_minimal_plan(self):
        """列裁剪对最小计划无变更 → 两次输出一致。"""
        plan = SqlBuildPlan(
            plan_id="test_minimal",
            spec_hash="abc",
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
        ok, detail = verify_column_pruning_idempotent(plan)
        assert ok, detail


class TestPredicateNormalization:
    """谓词规范化 Pass——幂等性：直接测试 + BETWEEN 变体 + warp 验证。"""

    def test_predicate_normalization_idempotent(self):
        """两次谓词规范化产生相同结果——内部状态断言。"""
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
        if records1:
            assert len(records2) == 0, f"第二次应无变更，实际: {records2}"
        assert len(result1.steps) == len(result2.steps)

    def test_predicate_normalization_idempotent_hash(self):
        """谓词规范化两次运行 → 输出 hash 一致（warp 验证）。"""
        ok, detail = verify_predicate_normalization_idempotent(_make_test_plan())
        assert ok, detail

    def test_predicate_norm_with_between(self):
        """包含 BETWEEN 谓词的计划 → 规范化后幂等。"""
        plan = SqlBuildPlan(
            plan_id="test_between",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                    predicates=[
                        Predicate(
                            left=ColumnRef(
                                table_ref="t1", column_name="dt", normalized_name="dt",
                            ),
                            operator=PredicateOperator.BETWEEN,
                            right=[
                                SqlLiteral(value="2026-01-01"),
                                SqlLiteral(value="2026-01-31"),
                            ],
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        ok, detail = verify_predicate_normalization_idempotent(plan)
        assert ok, detail


class TestSortElimination:
    """无用排序消除 Pass——幂等性：直接测试 + warp 验证。"""

    def test_sort_elimination_idempotent(self):
        """两次排序消除产生相同结果——内部状态断言。"""
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
                    limit=None,
                ),
            ],
            multi_table=False,
        )
        result1, _, eliminated1 = sort_elimination(plan)
        result2, _, eliminated2 = sort_elimination(result1)
        assert len(eliminated1) > 0, "应消除无用的 SortStep"
        assert "sort_useless" in eliminated1, f"应消除 sort_useless，实际: {eliminated1}"
        assert len(eliminated2) == 0, f"第二次应无变更，实际: {eliminated2}"
        assert len(result1.steps) == len(result2.steps)

    def test_sort_elimination_idempotent_hash(self):
        """排序消除两次运行 → 输出 hash 一致（warp 验证）。"""
        ok, detail = verify_sort_elimination_idempotent(_make_test_plan())
        assert ok, detail


class TestConstantFolding:
    """常量折叠 Pass——幂等性：直接测试 + warp 验证。"""

    def test_constant_folding_idempotent(self):
        """两次常量折叠产生相同结果——内部状态断言。"""
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
        if records1:
            assert len(records2) == 0, f"第二次应无变更，实际: {records2}"
        assert len(result1.steps) == len(result2.steps)

    def test_constant_folding_idempotent_hash(self):
        """常量折叠两次运行 → 输出 hash 一致（warp 验证）。"""
        ok, detail = verify_constant_folding_idempotent(_make_test_plan())
        assert ok, detail


class TestAllPassesIdempotent:
    """全部 Pass 幂等批量验证。"""

    def test_all_passes_idempotent(self):
        """verify_all_passes_idempotent 对测试计划全部通过。"""
        results = verify_all_passes_idempotent(_make_test_plan())
        for pass_name, ok, detail in results:
            assert ok, f"{pass_name} 幂等验证失败: {detail}"
        assert len(results) == 4
