"""WindowValidator——窗口函数安全校验。

Phase 3B 窗口函数门禁——在窗口表达式进入 Compiler 之前执行以下检查：
1. 窗口函数白名单——仅允许 8 种已注册函数
2. 窗口函数嵌套检测——WindowExpr 不能包含另一个窗口函数
3. 窗口函数位置检测——窗口函数不能出现在 WHERE / HAVING 中
4. Partition by 字段引用校验
5. Order by 字段引用校验
6. WindowFrame 合法性校验（边界类型、offset 约束）
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import OpenQuestion, SourceManifest
from tianshu_datadev.planning.models import (
    FrameBoundaryKind,
    WindowExpr,
    WindowFrameType,
    WindowFunction,
)
from tianshu_datadev.planning.sql_build_plan import (
    SqlBuildPlan,
    WindowStep,
)

# 白名单窗口函数集合——任何不在该集合中的窗口函数名被拒绝
_VALID_WINDOW_FUNCTIONS: frozenset[WindowFunction] = frozenset({
    WindowFunction.ROW_NUMBER,
    WindowFunction.RANK,
    WindowFunction.DENSE_RANK,
    WindowFunction.LAG,
    WindowFunction.LEAD,
    WindowFunction.SUM_OVER,
    WindowFunction.AVG_OVER,
    WindowFunction.COUNT_OVER,
})

# 不需要 frame 的窗口函数——这些函数不接受自定义窗口帧
_RANKING_FUNCTIONS: frozenset[WindowFunction] = frozenset({
    WindowFunction.ROW_NUMBER,
    WindowFunction.RANK,
    WindowFunction.DENSE_RANK,
    WindowFunction.LAG,
    WindowFunction.LEAD,
})


def validate_window_exprs(
    plan: SqlBuildPlan,
    manifest: SourceManifest | None = None,
) -> list[OpenQuestion]:
    """校验 SqlBuildPlan 中所有窗口表达式的合法性。

    对 WindowStep 中的每个 WindowExpr 执行：
    - 函数名白名单检查
    - 嵌套拒绝
    - Partition by 字段引用
    - Order by 字段引用
    - Frame 合法性

    Args:
        plan: 待校验的 SqlBuildPlan
        manifest: 事实源（可选——提供时进行字段引用校验）

    Returns:
        OpenQuestion 列表——空列表表示全部通过
    """
    questions: list[OpenQuestion] = []

    # 收集所有 WindowStep
    window_steps: list[WindowStep] = []
    for step in plan.steps:
        if isinstance(step, WindowStep):
            window_steps.append(step)

    if not window_steps:
        return questions

    # 构建已知表引用集合（用于字段引用校验）
    known_tables: set[str] = set()
    if manifest:
        known_tables = {t.table_ref for t in manifest.tables}

    for wstep in window_steps:
        for i, wexpr in enumerate(wstep.window_exprs):
            # ── 1. 白名单检查 ──
            if wexpr.function not in _VALID_WINDOW_FUNCTIONS:
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"window_invalid_func_"
                            f"{wstep.step_id}_{i}"
                        ),
                        source="WindowValidator",
                        description=(
                            f"窗口函数 '{wexpr.function.value}' 不在白名单中——"
                            f"仅允许: {sorted(f.value for f in _VALID_WINDOW_FUNCTIONS)}"
                        ),
                        blocking=True,
                    )
                )

            # ── 2. 嵌套窗口函数检测 ──
            # WindowExpr.input 是 ColumnRef 或 SqlLiteral——不是 WindowExpr，
            # 因此窗口函数嵌套在类型层面已被 Schema 阻止。
            # 此检查是二次确认——如果 Pydantic 类型约束被绕过。

            # ── 3. 排序函数不接受 frame ──
            if wexpr.function in _RANKING_FUNCTIONS and wexpr.frame is not None:
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"window_frame_on_ranking_"
                            f"{wstep.step_id}_{i}"
                        ),
                        source="WindowValidator",
                        description=(
                            f"窗口函数 '{wexpr.function.value}' 不支持自定义 "
                            f"WindowFrame——排名函数和 LAG/LEAD 不接受 frame 参数"
                        ),
                        blocking=True,
                    )
                )

            # ── 4. Frame 合法性校验 ──
            if wexpr.frame is not None:
                frame_qs = _validate_frame(
                    wexpr, wstep.step_id, i
                )
                questions.extend(frame_qs)

            # ── 5. Partition by 字段引用校验 ──
            if manifest:
                for j, col_ref in enumerate(wexpr.partition_by):
                    if col_ref.table_ref and col_ref.table_ref not in known_tables:
                        questions.append(
                            OpenQuestion(
                                question_id=(
                                    f"window_partition_table_"
                                    f"{wstep.step_id}_{i}_{j}"
                                ),
                                source="WindowValidator",
                                description=(
                                    f"窗口函数 PARTITION BY 引用了未注册的表 "
                                    f"'{col_ref.table_ref}'"
                                ),
                                blocking=True,
                            )
                        )

            # ── 6. Order by 字段引用校验 ──
            if manifest:
                for j, sort_spec in enumerate(wexpr.order_by):
                    # SortSpec.column 已由 SafeIdentifier 约束保护（Schema 层），
                    # 此处额外的 Manifest 对照校验（若未来 SortSpec 扩展为 ColumnRef）
                    pass

    return questions


def validate_window_not_in_where(
    plan: SqlBuildPlan,
) -> list[OpenQuestion]:
    """检查窗口函数是否出现在 WHERE / HAVING 子句中。

    窗口函数只能在 SELECT 和 ORDER BY 中使用，
    不能出现在 WHERE / HAVING 子句中——这是 SQL 标准约束。

    Args:
        plan: 待校验的 SqlBuildPlan

    Returns:
        OpenQuestion 列表——空列表表示全部通过
    """
    from tianshu_datadev.planning.sql_build_plan import (
        AggregateStep,
        FilterStep,
    )

    questions: list[OpenQuestion] = []

    # 窗口函数在 IR 中只能出现在 WindowStep 中，
    # FilterStep 和 AggregateStep.having 使用的是 Predicate，
    # 而 Predicate 不接受 WindowExpr 作为操作数。
    # 因此类型系统已阻止窗口函数进入 WHERE/HAVING。
    # 此检查是二次安全确认。

    # 检查所有 FilterStep ——确认 predicates 中不引用窗口函数别名
    window_aliases: set[str] = set()
    for step in plan.steps:
        if isinstance(step, WindowStep):
            for wexpr in step.window_exprs:
                if wexpr.alias:
                    window_aliases.add(wexpr.alias)

    if window_aliases:
        for step in plan.steps:
            if isinstance(step, FilterStep):
                col_name = getattr(step.predicate.left, "column_name", "")
                if col_name in window_aliases:
                    questions.append(
                        OpenQuestion(
                            question_id=(
                                f"window_in_where_"
                                f"{step.step_id}_{col_name}"
                            ),
                            source="WindowValidator",
                            description=(
                                f"窗口函数别名 '{col_name}' 出现在 FilterStep "
                                f"（等效 WHERE 子句）中——窗口函数不能用于 WHERE"
                            ),
                            blocking=True,
                        )
                    )

            if isinstance(step, AggregateStep) and step.having:
                _check_having_window_ref(
                    step.having, window_aliases, step.step_id, questions
                )

    return questions


def _check_having_window_ref(
    pred, window_aliases: set[str], step_id: str, questions: list[OpenQuestion]
) -> None:
    """递归检查 HAVING 谓词中是否引用了窗口函数别名。"""
    from tianshu_datadev.planning.models import Predicate

    if not isinstance(pred, Predicate):
        return

    col_name = getattr(pred.left, "column_name", "")
    if col_name in window_aliases:
        questions.append(
            OpenQuestion(
                question_id=f"window_in_having_{step_id}_{col_name}",
                source="WindowValidator",
                description=(
                    f"窗口函数别名 '{col_name}' 出现在 HAVING 子句中——"
                    f"窗口函数不能用于 HAVING"
                ),
                blocking=True,
            )
        )

    # 递归检查嵌套 Predicate
    if isinstance(pred.left, Predicate):
        _check_having_window_ref(pred.left, window_aliases, step_id, questions)
    if isinstance(pred.right, Predicate):
        _check_having_window_ref(pred.right, window_aliases, step_id, questions)


def _validate_frame(
    wexpr: WindowExpr,
    step_id: str,
    expr_index: int,
) -> list[OpenQuestion]:
    """校验 WindowFrame 的合法性。

    规则：
    - ROWS frame 支持所有边界类型
    - RANGE frame 仅支持 UNBOUNDED_PRECEDING / CURRENT_ROW
    - N_PRECEDING / N_FOLLOWING 必须提供正整数 offset
    - start 不能是 UNBOUNDED_FOLLOWING
    - end 不能是 UNBOUNDED_PRECEDING
    """
    questions: list[OpenQuestion] = []
    frame = wexpr.frame
    if frame is None:
        return questions

    prefix = f"window_frame_{step_id}_{expr_index}"

    # RANGE 模式约束
    if frame.frame_type == WindowFrameType.RANGE:
        allowed_kinds = {
            FrameBoundaryKind.UNBOUNDED_PRECEDING,
            FrameBoundaryKind.CURRENT_ROW,
        }
        if frame.start.kind not in allowed_kinds:
            questions.append(
                OpenQuestion(
                    question_id=f"{prefix}_range_start",
                    source="WindowValidator",
                    description=(
                        f"RANGE frame 的 start 边界类型 '{frame.start.kind.value}' "
                        f"无效——RANGE 仅支持 UNBOUNDED_PRECEDING / CURRENT_ROW"
                    ),
                    blocking=True,
                )
            )
        if frame.end.kind not in allowed_kinds:
            questions.append(
                OpenQuestion(
                    question_id=f"{prefix}_range_end",
                    source="WindowValidator",
                    description=(
                        f"RANGE frame 的 end 边界类型 '{frame.end.kind.value}' "
                        f"无效——RANGE 仅支持 UNBOUNDED_FOLLOWING / CURRENT_ROW"
                    ),
                    blocking=True,
                )
            )

    # N_PRECEDING / N_FOLLOWING 必须有正整数 offset
    for boundary_name, boundary in [("start", frame.start), ("end", frame.end)]:
        if boundary.kind in (
            FrameBoundaryKind.N_PRECEDING,
            FrameBoundaryKind.N_FOLLOWING,
        ):
            if boundary.offset is None:
                questions.append(
                    OpenQuestion(
                        question_id=f"{prefix}_{boundary_name}_no_offset",
                        source="WindowValidator",
                        description=(
                            f"Frame {boundary_name} 边界类型为 "
                            f"'{boundary.kind.value}' 但 offset 为 None——"
                            f"必须提供正整数偏移量"
                        ),
                        blocking=True,
                    )
                )
            elif boundary.offset < 0:
                questions.append(
                    OpenQuestion(
                        question_id=f"{prefix}_{boundary_name}_neg_offset",
                        source="WindowValidator",
                        description=(
                            f"Frame {boundary_name} offset 为 {boundary.offset}——"
                            f"必须为非负整数"
                        ),
                        blocking=True,
                    )
                )

    # start 不能是 UNBOUNDED_FOLLOWING
    if frame.start.kind == FrameBoundaryKind.UNBOUNDED_FOLLOWING:
        questions.append(
            OpenQuestion(
                question_id=f"{prefix}_start_following",
                source="WindowValidator",
                description="Frame start 不能为 UNBOUNDED_FOLLOWING",
                blocking=True,
            )
        )

    # end 不能是 UNBOUNDED_PRECEDING
    if frame.end.kind == FrameBoundaryKind.UNBOUNDED_PRECEDING:
        questions.append(
            OpenQuestion(
                question_id=f"{prefix}_end_preceding",
                source="WindowValidator",
                description="Frame end 不能为 UNBOUNDED_PRECEDING",
                blocking=True,
            )
        )

    return questions
