"""Compiler Pass 幂等验证测试——4 个 Pass 各运行两次应输出相同 hash。

Phase 4B 核心要求：Compiler Pass 必须是幂等的——
相同 SqlBuildPlan 经任意次数运行产生相同输出。
"""

from __future__ import annotations

from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    PredicateOperator,
    SqlLiteral,
)
from tianshu_datadev.planning.sql_build_plan import (
    ScanStep,
    SortStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.compiler_passes import (
    constant_folding,
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
        hypothesis_id="hyp_001",
        source_manifest_hash="src_man_001",
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


class TestColumnPruningIdempotent:
    """列裁剪幂等测试。"""

    def test_column_pruning_idempotent(self):
        """运行列裁剪两次 → 输出 hash 一致。"""
        plan = _make_test_plan()
        ok, detail = verify_column_pruning_idempotent(plan)
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


class TestPredicateNormalizationIdempotent:
    """谓词规范化幂等测试。"""

    def test_predicate_normalization_idempotent(self):
        """运行谓词规范化两次 → 输出 hash 一致。"""
        plan = _make_test_plan()
        ok, detail = verify_predicate_normalization_idempotent(plan)
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


class TestSortEliminationIdempotent:
    """无用排序消除幂等测试。"""

    def test_sort_elimination_idempotent(self):
        """运行排序消除两次 → 输出 hash 一致。"""
        plan = _make_test_plan()
        ok, detail = verify_sort_elimination_idempotent(plan)
        assert ok, detail

    def test_sort_elimination_removes_useless_sort(self):
        """无 LIMIT 的 SortStep 被消除——首次消除后二次运行无变更。"""
        plan = SqlBuildPlan(
            plan_id="test_sort_elim",
            spec_hash="abc",
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
        # 运行一次消除
        plan_a, _, _ = sort_elimination(plan)
        # 二次消除——应无变更
        plan_b, _, _ = sort_elimination(plan_a)
        hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
        hash_b = SqlBuildPlan.generate_plan_hash(plan_b)
        assert hash_a == hash_b, f"hash_a={hash_a} != hash_b={hash_b}"


class TestConstantFoldingIdempotent:
    """常量折叠幂等测试。"""

    def test_constant_folding_idempotent(self):
        """运行常量折叠两次 → 输出 hash 一致。"""
        plan = _make_test_plan()
        ok, detail = verify_constant_folding_idempotent(plan)
        assert ok, detail

    def test_constant_fold_on_noop_plan(self):
        """对无优化机会的计划常量折叠——两次运行不变，验证幂等。"""
        plan = SqlBuildPlan(
            plan_id="test_noop",
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
        # 连续两次常量折叠——应得到相同结果
        plan_a, _ = constant_folding(plan)
        plan_b, _ = constant_folding(plan_a)
        hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
        hash_b = SqlBuildPlan.generate_plan_hash(plan_b)
        assert hash_a == hash_b, (
            f"常量折叠幂等失败: hash_a={hash_a}, hash_b={hash_b}"
        )


class TestAllPassesIdempotent:
    """全部 Pass 幂等验证——批量运行。"""

    def test_all_passes_idempotent(self):
        """verify_all_passes_idempotent 对测试计划全部通过。"""
        plan = _make_test_plan()
        results = verify_all_passes_idempotent(plan)
        for pass_name, ok, detail in results:
            assert ok, f"{pass_name} 幂等验证失败: {detail}"
        assert len(results) == 4
