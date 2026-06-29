"""子查询（SubqueryStep）校验测试——Phase 4.6 Step 2。

验证 SqlBuildPlanValidator 的 V-010a~e 五规则 + Compiler 递归渲染：
- V-010a SUBQUERY_DEPTH_CHECK：嵌套深度 ≤ 2
- V-010d SUBQUERY_WINDOW_FORBIDDEN：内层不含 WindowStep
- V-010e SUBQUERY_JOIN_FORBIDDEN：内层仅单表
- V-010c SOURCE_CONFLICT：内层事实源一致性
- 确定性 hash：相同子查询两次编译一致

同时验证 Validator 白名单已包含 SubqueryStep。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    JoinType,
    SortDirection,
    SortSpec,
    WindowExpr,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    JoinStep,
    LimitStep,
    ScanStep,
    SqlBuildPlan,
    SubqueryStep,
    WindowStep,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

# ════════════════════════════════════════════
# 辅助工厂函数
# ════════════════════════════════════════════


def _make_manifest() -> SourceManifest:
    """构造 3 表 SourceManifest——o（事实）+ p（维表）+ u（用户）。"""
    return SourceManifest(
        manifest_id="subquery_test_manifest",
        spec_hash="subquery_test",
        tables=[
            ManifestTable(
                table_ref="o",
                source_table="dwd.order_detail",
                columns=[
                    ManifestColumn(
                        column_name="product_id",
                        normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="amount",
                        normalized_name="amount",
                        data_type="decimal",
                    ),
                    ManifestColumn(
                        column_name="order_time",
                        normalized_name="order_time",
                        data_type="timestamp",
                    ),
                ],
            ),
            ManifestTable(
                table_ref="p",
                source_table="dim.product_info",
                columns=[
                    ManifestColumn(
                        column_name="product_id",
                        normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="product_name",
                        normalized_name="product_name",
                        data_type="varchar",
                    ),
                    ManifestColumn(
                        column_name="category",
                        normalized_name="category",
                        data_type="varchar",
                    ),
                ],
            ),
            ManifestTable(
                table_ref="u",
                source_table="dwd.user_info",
                columns=[
                    ManifestColumn(
                        column_name="user_id",
                        normalized_name="user_id",
                        data_type="bigint",
                    ),
                ],
            ),
        ],
    )


def _make_single_table_inner_plan() -> SqlBuildPlan:
    """构造内层子查询计划——单表 Scan + 聚合（合法）。"""
    return SqlBuildPlan(
        plan_id="inner_single",
        spec_hash="test",
        steps=[
            ScanStep(
                step_id="scan_o",
                table_ref="o",
                required_columns=[
                    ColumnRef(
                        table_ref="o",
                        column_name="product_id",
                        normalized_name="product_id",
                    ),
                    ColumnRef(
                        table_ref="o",
                        column_name="amount",
                        normalized_name="amount",
                    ),
                ],
            ),
            AggregateStep(
                step_id="agg_inner",
                group_keys=[
                    ColumnRef(
                        table_ref="o",
                        column_name="product_id",
                        normalized_name="product_id",
                    ),
                ],
                metrics=[
                    AggregateSpec(
                        aggregation="SUM",
                        input_column="amount",
                        alias="daily_amount",
                    ),
                ],
            ),
            LimitStep(step_id="limit_100", limit=100),
        ],
    )


def _make_join_inner_plan() -> SqlBuildPlan:
    """构造内层子查询计划——含 JoinStep（非法——V-010e 应拒绝）。"""
    return SqlBuildPlan(
        plan_id="inner_join",
        spec_hash="test",
        steps=[
            ScanStep(
                step_id="scan_o",
                table_ref="o",
                required_columns=[
                    ColumnRef(
                        table_ref="o",
                        column_name="product_id",
                        normalized_name="product_id",
                    ),
                ],
            ),
            ScanStep(
                step_id="scan_p",
                table_ref="p",
                required_columns=[
                    ColumnRef(
                        table_ref="p",
                        column_name="product_id",
                        normalized_name="product_id",
                    ),
                ],
            ),
            JoinStep(
                step_id="join_o_p",
                right_table_ref="p",
                join_type=JoinType.INNER,
                join_keys=[
                    (
                        ColumnRef(
                            table_ref="o",
                            column_name="product_id",
                            normalized_name="product_id",
                        ),
                        ColumnRef(
                            table_ref="p",
                            column_name="product_id",
                            normalized_name="product_id",
                        ),
                    ),
                ],
                relationship_ref="rel_o_p",
            ),
            LimitStep(step_id="limit_100", limit=100),
        ],
    )


def _make_window_inner_plan() -> SqlBuildPlan:
    """构造内层子查询计划——含 WindowStep（非法——V-010d 应拒绝）。"""
    return SqlBuildPlan(
        plan_id="inner_window",
        spec_hash="test",
        steps=[
            ScanStep(
                step_id="scan_o",
                table_ref="o",
                required_columns=[
                    ColumnRef(
                        table_ref="o",
                        column_name="product_id",
                        normalized_name="product_id",
                    ),
                    ColumnRef(
                        table_ref="o",
                        column_name="amount",
                        normalized_name="amount",
                    ),
                ],
            ),
            WindowStep(
                step_id="win_rank",
                window_exprs=[
                    WindowExpr(
                        function="ROW_NUMBER",
                        partition_by=[
                            ColumnRef(
                                table_ref="o",
                                column_name="product_id",
                                normalized_name="product_id",
                            ),
                        ],
                        order_by=[
                            SortSpec(
                                column="amount",
                                direction=SortDirection.DESC,
                            ),
                        ],
                        alias="rn",
                    ),
                ],
            ),
            LimitStep(step_id="limit_100", limit=100),
        ],
    )


def _make_empty_inner_plan() -> SqlBuildPlan:
    """构造空 steps 的内层计划（非法——应触发 Validator 拒绝）。"""
    return SqlBuildPlan(
        plan_id="inner_empty",
        spec_hash="test",
        steps=[],
    )


# ════════════════════════════════════════════
# 黄金路径——FROM 子查询通过
# ════════════════════════════════════════════


class TestSubqueryGolden:
    """FROM 子句派生表子查询——合法场景全部通过。"""

    def test_from_subquery_passes_validation(self):
        """派生表子查询（单表聚合）→ 通过 V-010 五规则校验。"""
        inner = _make_single_table_inner_plan()
        sq = SubqueryStep(
            step_id="sq_order_agg",
            alias="order_agg",
            inner_plan=inner,
            depth=1,
        )

        outer = SqlBuildPlan(
            plan_id="outer_golden",
            spec_hash="test",
            steps=[
                sq,
                ScanStep(
                    step_id="scan_p",
                    table_ref="p",
                    required_columns=[
                        ColumnRef(
                            table_ref="p",
                            column_name="product_id",
                            normalized_name="product_id",
                        ),
                        ColumnRef(
                            table_ref="p",
                            column_name="product_name",
                            normalized_name="product_name",
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_sq_p",
                    right_table_ref="p",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(
                                table_ref="order_agg",
                                column_name="product_id",
                                normalized_name="product_id",
                            ),
                            ColumnRef(
                                table_ref="p",
                                column_name="product_id",
                                normalized_name="product_id",
                            ),
                        ),
                    ],
                    relationship_ref="rel_sq_p",
                ),
                AggregateStep(
                    step_id="agg_outer",
                    group_keys=[
                        ColumnRef(
                            table_ref="p",
                            column_name="product_name",
                            normalized_name="product_name",
                        ),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="SUM",
                            input_column="daily_amount",
                            alias="total_amount",
                        ),
                    ],
                ),
                LimitStep(step_id="limit_50", limit=50),
            ],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert passed, (
            f"FROM 子查询黄金路径应通过校验，实际 blocking: "
            f"{[(q.question_id, q.description[:80]) for q in questions if q.blocking]}"
        )

    def test_depth_2_subquery_passes(self):
        """2 层嵌套子查询（边界值）通过校验。"""
        inner_l2 = _make_single_table_inner_plan()
        sq_l2 = SubqueryStep(
            step_id="sq_l2",
            alias="level_2",
            inner_plan=inner_l2,
            depth=2,
        )

        outer_l1 = SqlBuildPlan(
            plan_id="outer_l1",
            spec_hash="test",
            steps=[
                sq_l2,
                AggregateStep(
                    step_id="agg_l1",
                    group_keys=[],
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT",
                            input_column="daily_amount",
                            alias="cnt",
                        ),
                    ],
                ),
                LimitStep(step_id="limit_10", limit=10),
            ],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer_l1, manifest)

        assert not any(
            "SUBQUERY-DEPTH" in q.question_id
            for q in questions
        ), (
            f"2 层嵌套不应触发深度拒绝: "
            f"{[q.description[:80] for q in questions if 'DEPTH' in q.question_id]}"
        )

    def test_subquery_compiles_correct_sql(self):
        """验证子查询编译器输出正确的 SQL 结构。"""
        inner = _make_single_table_inner_plan()
        sq = SubqueryStep(
            step_id="sq_1",
            alias="order_agg",
            inner_plan=inner,
            depth=1,
        )
        outer = SqlBuildPlan(
            plan_id="outer_compile_test",
            spec_hash="test",
            steps=[sq],
            multi_table=True,
        )

        compiler = DuckDbSqlCompiler()
        sql = compiler.compile(outer)

        # SQL 必须包含子查询结构
        assert "(" in sql.sql, "子查询 SQL 必须包含括号"
        assert ") AS order_agg" in sql.sql, "子查询必须包含派生表别名"
        # 禁止 CTE
        assert "WITH " not in sql.sql.upper().split("SELECT")[0], (
            "子查询不得生成 CTE (WITH ... AS)"
        )

    def test_subquery_deterministic_hash(self):
        """相同子查询两次编译→相同 SQL + 相同 SHA-256。"""
        inner = _make_single_table_inner_plan()
        sq = SubqueryStep(
            step_id="sq_1",
            alias="order_agg",
            inner_plan=inner,
            depth=1,
        )
        outer = SqlBuildPlan(
            plan_id="outer_hash_test",
            spec_hash="test",
            steps=[sq],
            multi_table=True,
        )

        compiler = DuckDbSqlCompiler()
        result1 = compiler.compile(outer)
        result2 = compiler.compile(outer)

        assert result1.sql == result2.sql, (
            f"两次编译 SQL 不一致:\n---1---\n{result1.sql}\n---2---\n{result2.sql}"
        )
        assert result1.sql_sha256 == result2.sql_sha256, (
            f"两次编译 SHA-256 不一致: "
            f"{result1.sql_sha256} vs {result2.sql_sha256}"
        )


# ════════════════════════════════════════════
# V-010a 深度超限拒绝
# ════════════════════════════════════════════


class TestSubqueryDepthReject:
    """V-010a——嵌套深度超过 2 层拒绝。"""

    def test_depth_3_subquery_rejected(self):
        """3 层嵌套子查询→ SUBQUERY_NESTING_TOO_DEEP。"""
        inner_l3 = _make_single_table_inner_plan()
        sq_l3 = SubqueryStep(
            step_id="sq_l3",
            alias="level_3",
            inner_plan=inner_l3,
            depth=3,  # 超过上限
        )

        outer = SqlBuildPlan(
            plan_id="outer_depth3",
            spec_hash="test",
            steps=[
                sq_l3,
                LimitStep(step_id="limit_10", limit=10),
            ],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed, "3 层嵌套子查询应被拒绝"
        depth_qs = [
            q for q in questions if "SUBQUERY-DEPTH" in q.question_id
        ]
        assert len(depth_qs) == 1, (
            f"应有 1 个 SUBQUERY-DEPTH 问题，实际: "
            f"{[q.question_id for q in questions]}"
        )
        assert depth_qs[0].blocking is True
        assert "SUBQUERY_NESTING_TOO_DEEP" in depth_qs[0].description
        assert "3" in depth_qs[0].description

    def test_depth_4_subquery_rejected(self):
        """4 层嵌套→ SUBQUERY_NESTING_TOO_DEEP（>2 均拒绝）。"""
        inner_l4 = _make_single_table_inner_plan()
        sq_l4 = SubqueryStep(
            step_id="sq_l4",
            alias="level_4",
            inner_plan=inner_l4,
            depth=4,
        )

        outer = SqlBuildPlan(
            plan_id="outer_depth4",
            spec_hash="test",
            steps=[sq_l4, LimitStep(step_id="limit_10", limit=10)],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed
        depth_qs = [
            q for q in questions if "SUBQUERY-DEPTH" in q.question_id
        ]
        assert len(depth_qs) == 1
        assert "4" in depth_qs[0].description


# ════════════════════════════════════════════
# V-010d 窗口禁止拒绝
# ════════════════════════════════════════════


class TestSubqueryWindowForbidden:
    """V-010d——子查询内层含 WindowStep 拒绝。"""

    def test_window_in_subquery_rejected(self):
        """内层计划含 WindowStep→ SUBQUERY_WINDOW_FORBIDDEN。"""
        inner = _make_window_inner_plan()
        sq = SubqueryStep(
            step_id="sq_win",
            alias="ranked",
            inner_plan=inner,
            depth=1,
        )

        outer = SqlBuildPlan(
            plan_id="outer_win_reject",
            spec_hash="test",
            steps=[sq, LimitStep(step_id="limit_10", limit=10)],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed, "子查询内含 WindowStep 应被拒绝"
        win_qs = [
            q for q in questions if "SUBQUERY-WINDOW" in q.question_id
        ]
        assert len(win_qs) == 1, (
            f"应有 1 个 SUBQUERY-WINDOW 问题，实际: "
            f"{[q.question_id for q in questions]}"
        )
        assert win_qs[0].blocking is True
        assert "SUBQUERY_WINDOW_FORBIDDEN" in win_qs[0].description
        assert "WindowStep" in win_qs[0].description


# ════════════════════════════════════════════
# V-010e Join 禁止拒绝
# ════════════════════════════════════════════


class TestSubqueryJoinForbidden:
    """V-010e——子查询内层含 JoinStep 拒绝。"""

    def test_join_in_subquery_rejected(self):
        """内层计划含 JoinStep→ SUBQUERY_JOIN_FORBIDDEN。"""
        inner = _make_join_inner_plan()
        sq = SubqueryStep(
            step_id="sq_join",
            alias="joined",
            inner_plan=inner,
            depth=1,
        )

        outer = SqlBuildPlan(
            plan_id="outer_join_reject",
            spec_hash="test",
            steps=[sq, LimitStep(step_id="limit_10", limit=10)],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed, "子查询内含 JoinStep 应被拒绝"
        join_qs = [
            q for q in questions if "SUBQUERY-JOIN" in q.question_id
        ]
        assert len(join_qs) == 1, (
            f"应有 1 个 SUBQUERY-JOIN 问题，实际: "
            f"{[q.question_id for q in questions]}"
        )
        assert join_qs[0].blocking is True
        assert "SUBQUERY_JOIN_FORBIDDEN" in join_qs[0].description
        assert "JoinStep" in join_qs[0].description


# ════════════════════════════════════════════
# V-010c 事实源一致性拒绝
# ════════════════════════════════════════════


class TestSubquerySourceConflict:
    """V-010c——子查询内层引用未注册表拒绝。"""

    def test_unregistered_table_in_subquery_rejected(self):
        """内层 ScanStep 引用未注册表→ SOURCE_CONFLICT。"""
        inner = SqlBuildPlan(
            plan_id="inner_bad_source",
            spec_hash="test",
            steps=[
                ScanStep(
                    step_id="scan_unknown",
                    table_ref="non_existent_table",
                    required_columns=[
                        ColumnRef(
                            table_ref="non_existent_table",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
                LimitStep(step_id="limit_10", limit=10),
            ],
        )
        sq = SubqueryStep(
            step_id="sq_bad_src",
            alias="bad_source",
            inner_plan=inner,
            depth=1,
        )

        outer = SqlBuildPlan(
            plan_id="outer_bad_src",
            spec_hash="test",
            steps=[sq, LimitStep(step_id="limit_10", limit=10)],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed, "子查询引用未注册表应被拒绝"
        src_qs = [
            q for q in questions if "SUBQUERY-SOURCE" in q.question_id
        ]
        assert len(src_qs) == 1, (
            f"应有 1 个 SUBQUERY-SOURCE 问题，实际: "
            f"{[q.question_id for q in questions]}"
        )
        assert src_qs[0].blocking is True
        assert "SOURCE_CONFLICT" in src_qs[0].description
        assert "non_existent_table" in src_qs[0].description


# ════════════════════════════════════════════
# 组合场景
# ════════════════════════════════════════════


class TestSubqueryCombined:
    """同时触发多条 V-010 规则的组合场景。"""

    def test_depth_and_window_both_rejected(self):
        """3 层嵌套 + 内层含 WindowStep→同时触发 DEPTH + WINDOW 拒绝。"""
        inner = _make_window_inner_plan()
        sq = SubqueryStep(
            step_id="sq_both",
            alias="bad_depth_and_win",
            inner_plan=inner,
            depth=3,
        )

        outer = SqlBuildPlan(
            plan_id="outer_both",
            spec_hash="test",
            steps=[sq, LimitStep(step_id="limit_10", limit=10)],
            multi_table=True,
        )

        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(outer, manifest)

        assert not passed
        question_ids = {q.question_id for q in questions}
        assert any(
            "SUBQUERY-DEPTH" in qid for qid in question_ids
        ), f"缺少 DEPTH 问题: {question_ids}"
        assert any(
            "SUBQUERY-WINDOW" in qid for qid in question_ids
        ), f"缺少 WINDOW 问题: {question_ids}"
