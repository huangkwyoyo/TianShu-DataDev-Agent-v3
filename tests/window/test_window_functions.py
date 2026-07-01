"""窗口函数完整测试——Phase 3B WindowExpr 白名单 + 拒绝路径。

覆盖：
- 8 种窗口函数合法用例各至少 1 个
- 非法窗口函数名拒绝（Pydantic 层 + Validator 层）
- 窗口函数嵌套拒绝（类型系统阻止）
- 窗口函数出现在 WHERE 拒绝
- Frame 合法性校验
- 确定性编译
"""

from __future__ import annotations

import typing

import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    ColumnRef,
    FrameBoundary,
    FrameBoundaryKind,
    Predicate,
    PredicateOperator,
    SortSpec,
    SqlLiteral,
    WindowExpr,
    WindowFrame,
    WindowFrameType,
    WindowFunction,
)
from tianshu_datadev.planning.sql_build_plan import (
    FilterStep,
    ScanStep,
    SqlBuildPlan,
    WindowStep,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.validation.window_validator import (
    validate_window_exprs,
    validate_window_not_in_where,
)

# ── 辅助函数 ──


def _make_base_scan() -> ScanStep:
    """创建基础 ScanStep——测试通用。"""
    return ScanStep(
        step_id="scan_1",
        table_ref="tf",
        required_columns=[
            ColumnRef(table_ref="tf", column_name="id", normalized_name="id"),
            ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
            ColumnRef(table_ref="tf", column_name="salary", normalized_name="salary"),
            ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
        ],
    )


def _make_window_plan(window_exprs: list[WindowExpr]) -> SqlBuildPlan:
    """创建含 WindowStep 的 SqlBuildPlan。"""
    return SqlBuildPlan(
        plan_id="test_window",
        spec_hash="win_hash_001",
        steps=[
            _make_base_scan(),
            WindowStep(
                step_id="win_1",
                window_exprs=window_exprs,
            ),
        ],
    )


# ════════════════════════════════════════════
# 8 种窗口函数合法用例
# ════════════════════════════════════════════


class TestWindowFunctionWhitelist:
    """8 种窗口函数白名单——合法用例测试。"""

    def test_row_number_valid(self):
        """ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)。"""
        wexpr = WindowExpr(
            function=WindowFunction.ROW_NUMBER,
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="DESC")],
            alias="rn",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0, f"ROW_NUMBER 应合法: {questions}"

    def test_rank_valid(self):
        """RANK() OVER (ORDER BY salary DESC)。"""
        wexpr = WindowExpr(
            function=WindowFunction.RANK,
            order_by=[SortSpec(column="salary", direction="DESC")],
            alias="rank_val",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_dense_rank_valid(self):
        """DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary)。"""
        wexpr = WindowExpr(
            function=WindowFunction.DENSE_RANK,
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="ASC")],
            alias="dr",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_lag_valid(self):
        """LAG(salary) OVER (PARTITION BY dept ORDER BY dt)。"""
        wexpr = WindowExpr(
            function=WindowFunction.LAG,
            input=ColumnRef(
                table_ref="tf", column_name="salary", normalized_name="salary"
            ),
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="ASC")],
            alias="prev_salary",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_lead_valid(self):
        """LEAD(salary) OVER (PARTITION BY dept ORDER BY dt)。"""
        wexpr = WindowExpr(
            function=WindowFunction.LEAD,
            input=ColumnRef(
                table_ref="tf", column_name="salary", normalized_name="salary"
            ),
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="ASC")],
            alias="next_salary",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_sum_over_valid(self):
        """SUM(amt) OVER (PARTITION BY dept ORDER BY dt ROWS UNBOUNDED PRECEDING)。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="cum_sum",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_avg_over_valid(self):
        """AVG(amt) OVER (PARTITION BY dept)。"""
        wexpr = WindowExpr(
            function=WindowFunction.AVG_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            alias="avg_amt",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_count_over_valid(self):
        """COUNT(*) OVER (PARTITION BY dept)。"""
        wexpr = WindowExpr(
            function=WindowFunction.COUNT_OVER,
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            alias="cnt",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_all_nine_functions_compilable(self):
        """全部 9 种窗口函数可通过编译。"""
        all_funcs = [
            WindowExpr(
                function=WindowFunction.ROW_NUMBER,
                partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                order_by=[SortSpec(column="salary", direction="DESC")],
                alias="rn",
            ),
            WindowExpr(
                function=WindowFunction.RANK,
                order_by=[SortSpec(column="salary", direction="DESC")],
                alias="rk",
            ),
            WindowExpr(
                function=WindowFunction.DENSE_RANK,
                order_by=[SortSpec(column="salary", direction="DESC")],
                alias="dr",
            ),
            WindowExpr(
                function=WindowFunction.NTILE,
                input=SqlLiteral(value=4),
                order_by=[SortSpec(column="salary", direction="DESC")],
                alias="nt",
            ),
            WindowExpr(
                function=WindowFunction.LAG,
                input=ColumnRef(table_ref="tf", column_name="salary", normalized_name="salary"),
                order_by=[SortSpec(column="salary", direction="ASC")],
                alias="lag_sal",
            ),
            WindowExpr(
                function=WindowFunction.LEAD,
                input=ColumnRef(table_ref="tf", column_name="salary", normalized_name="salary"),
                order_by=[SortSpec(column="salary", direction="ASC")],
                alias="lead_sal",
            ),
            WindowExpr(
                function=WindowFunction.SUM_OVER,
                input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                alias="sum_amt",
            ),
            WindowExpr(
                function=WindowFunction.AVG_OVER,
                input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                alias="avg_amt",
            ),
            WindowExpr(
                function=WindowFunction.COUNT_OVER,
                partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                alias="cnt_dept",
            ),
        ]
        # 每个函数单独编译——验证都能通过
        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        for i, wexpr in enumerate(all_funcs):
            plan = _make_window_plan([wexpr])
            questions = validate_window_exprs(plan)
            assert len(questions) == 0, (
                f"函数 {wexpr.function.value} 校验失败: {questions}"
            )
            compiled = compiler.compile(plan)
            assert compiled.sql, f"函数 {wexpr.function.value} 编译失败"
            assert "OVER" in compiled.sql.upper(), (
                f"函数 {wexpr.function.value} 编译结果缺少 OVER"
            )


# ════════════════════════════════════════════
# 非法窗口函数名拒绝
# ════════════════════════════════════════════


class TestWindowFunctionRejection:
    """窗口函数拒绝路径测试。"""

    def test_invalid_function_name_rejected_by_pydantic(self):
        """非法函数名在 Pydantic 层被拦截。"""
        with pytest.raises(ValidationError):
            WindowExpr(
                function="PERCENT_RANK",  # 不在白名单中
                partition_by=[],
                order_by=[],
                alias="pr",
            )

    def test_invalid_function_name_string_rejected(self):
        """任意字符串函数名被 Pydantic 枚举拒绝。"""
        invalid_names = ["MEDIAN", "PERCENTILE", "FIRST_VALUE", "LAST_VALUE", "CUME_DIST"]
        for name in invalid_names:
            with pytest.raises(ValidationError):
                WindowExpr(
                    function=name,
                    partition_by=[],
                    order_by=[],
                    alias="bad",
                )

    def test_ranking_function_with_frame_rejected(self):
        """ROW_NUMBER 等排序函数带 frame 被 Validator 拒绝。"""
        ranking_funcs = [
            WindowFunction.ROW_NUMBER,
            WindowFunction.RANK,
            WindowFunction.DENSE_RANK,
            WindowFunction.NTILE,
            WindowFunction.LAG,
            WindowFunction.LEAD,
        ]
        for func in ranking_funcs:
            wexpr = WindowExpr(
                function=func,
                order_by=[SortSpec(column="salary", direction="DESC")],
                frame=WindowFrame(
                    frame_type=WindowFrameType.ROWS,
                    start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                    end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                ),
                alias="bad",
            )
            plan = _make_window_plan([wexpr])
            questions = validate_window_exprs(plan)
            assert len(questions) >= 1, (
                f"函数 {func.value} 带 frame 应被拒绝"
            )


# ════════════════════════════════════════════
# 窗口函数位置拒绝
# ════════════════════════════════════════════


class TestWindowPositionRejection:
    """窗口函数不能出现在 WHERE / HAVING 中。"""

    def test_window_alias_in_where_rejected(self):
        """窗口函数别名出现在 FilterStep（等效 WHERE）中被拒绝。"""
        plan = SqlBuildPlan(
            plan_id="test_where_win",
            spec_hash="win_hash_001",
            steps=[
                _make_base_scan(),
                WindowStep(
                    step_id="win_1",
                    window_exprs=[
                        WindowExpr(
                            function=WindowFunction.ROW_NUMBER,
                            order_by=[SortSpec(column="salary", direction="DESC")],
                            alias="rn",
                        )
                    ],
                ),
                # 试图在 WHERE 中引用窗口函数别名
                FilterStep(
                    step_id="filter_rn",
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref="", column_name="rn", normalized_name="rn"
                        ),
                        operator=PredicateOperator.GT,
                        right=SqlLiteral(value=1),
                    ),
                ),
            ],
        )
        questions = validate_window_not_in_where(plan)
        assert len(questions) >= 1, (
            f"窗口函数别名 'rn' 在 FilterStep 中应被拒绝: {questions}"
        )

    def test_no_window_aliases_no_errors(self):
        """无窗口函数的 plan 不产生 WHERE 拒绝。"""
        plan = _make_window_plan([])
        questions = validate_window_not_in_where(plan)
        assert len(questions) == 0


# ════════════════════════════════════════════
# WindowFrame 合法性
# ════════════════════════════════════════════


class TestWindowFrameValidation:
    """WindowFrame 合法性校验。"""

    def test_valid_rows_frame_passes(self):
        """合法 ROWS frame 通过。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.N_PRECEDING, offset=3),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="sum_3rows",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_valid_range_frame_passes(self):
        """合法 RANGE frame 通过。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.RANGE,
                start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="range_sum",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) == 0

    def test_n_boundary_without_offset_rejected(self):
        """N_PRECEDING 无 offset 被拒绝。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.N_PRECEDING, offset=None),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="bad_frame",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) >= 1

    def test_negative_offset_rejected(self):
        """负 offset 被拒绝。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.N_PRECEDING, offset=-5),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="neg_frame",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) >= 1

    def test_start_unbounded_following_rejected(self):
        """start 为 UNBOUNDED_FOLLOWING 被拒绝。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_FOLLOWING),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="bad_start",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) >= 1

    def test_end_unbounded_preceding_rejected(self):
        """end 为 UNBOUNDED_PRECEDING 被拒绝。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            order_by=[SortSpec(column="salary", direction="ASC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                end=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
            ),
            alias="bad_end",
        )
        plan = _make_window_plan([wexpr])
        questions = validate_window_exprs(plan)
        assert len(questions) >= 1


# ════════════════════════════════════════════
# 窗口函数嵌套拒绝
# ════════════════════════════════════════════


class TestWindowNestingRejection:
    """窗口函数嵌套——类型系统阻止。"""

    def test_window_expr_input_is_not_window_expr(self):
        """WindowExpr.input 类型为 ColumnRef | SqlLiteral | None，
        不是 WindowExpr——因此嵌套窗口函数在 Schema 层已阻止。
        """
        # 验证类型注解——WindowExpr.input 不接受 WindowExpr
        hints = typing.get_type_hints(WindowExpr)
        input_type = hints.get("input")
        input_str = str(input_type)
        # input 类型不应包含 WindowExpr
        assert "ColumnRef" in input_str or "SqlLiteral" in input_str, (
            f"WindowExpr.input 应只接受 ColumnRef 或 SqlLiteral，"
            f"实际类型: {input_str}"
        )
        # 不应接受 WindowExpr（防止嵌套）
        assert "WindowExpr" not in input_str, (
            f"WindowExpr.input 不应接受 WindowExpr（防止嵌套），"
            f"实际类型: {input_str}"
        )


# ════════════════════════════════════════════
# 确定性编译
# ════════════════════════════════════════════


class TestWindowCompilerDeterminism:
    """窗口函数确定性编译——相同 plan 两次编译结果一致。"""

    def test_same_window_plan_same_sql_and_hash(self):
        """相同窗口函数 plan 两次编译——SQL 和 SHA-256 一致。"""
        wexpr = WindowExpr(
            function=WindowFunction.ROW_NUMBER,
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="DESC")],
            alias="rn",
        )
        plan = _make_window_plan([wexpr])

        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        result1 = compiler.compile(plan)
        result2 = compiler.compile(plan)

        assert result1.sql == result2.sql, (
            f"相同 plan 两次编译 SQL 应一致\n"
            f"第一次: {result1.sql}\n"
            f"第二次: {result2.sql}"
        )
        assert result1.sql_sha256 == result2.sql_sha256, (
            "相同 plan 两次编译 SHA-256 应一致"
        )

    def test_window_with_case_when_deterministic(self):
        """窗口函数 + CASE WHEN 组合——确定性编译。"""
        from tianshu_datadev.planning.models import WhenBranch
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep

        plan = SqlBuildPlan(
            plan_id="test_combined",
            spec_hash="win_hash_001",
            steps=[
                _make_base_scan(),
                CaseWhenStep(
                    step_id="case_1",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="salary",
                                    normalized_name="salary",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=5000),
                            ),
                            result=SqlLiteral(value="高"),
                        )
                    ],
                    else_value=SqlLiteral(value="低"),
                    alias="salary_level",
                ),
                WindowStep(
                    step_id="win_1",
                    window_exprs=[
                        WindowExpr(
                            function=WindowFunction.ROW_NUMBER,
                            partition_by=[
                                ColumnRef(
                                    table_ref="tf",
                                    column_name="dept",
                                    normalized_name="dept",
                                )
                            ],
                            order_by=[SortSpec(column="salary", direction="DESC")],
                            alias="rn",
                        )
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        result1 = compiler.compile(plan)
        result2 = compiler.compile(plan)

        assert result1.sql == result2.sql
        assert result1.sql_sha256 == result2.sql_sha256
        # 验证 SQL 包含 CASE WHEN 和 OVER
        assert "CASE" in result1.sql
        assert "OVER" in result1.sql
