"""ReviewBuilder——从 SqlBuildPlan 确定性抽取审查材料。

Phase 3B 审查包生成器——提取标签规则、窗口表达式和字段血缘，
输出人类可读的 ReviewReport。

抽取是确定性的——相同 SqlBuildPlan → 相同 ReviewReport。
"""

from __future__ import annotations

import hashlib

from tianshu_datadev.developer_spec.models import (
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    WindowExpr,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    ProjectStep,
    ScanStep,
    SqlBuildPlan,
    WindowStep,
)

from .models import (
    FieldLineageEntry,
    LabelRuleEntry,
    ReviewReport,
    WindowExprEntry,
)


class ReviewBuilder:
    """Phase 3B 审查包构建器——从 SqlBuildPlan 确定性生成审查报告。

    审查报告包含三个维度：
    1. 标签规则——CASE WHEN 条件、结果和枚举值覆盖
    2. 窗口表达式——函数、分区键、排序键和帧
    3. 字段血缘——从源表字段到输出列的完整转换链
    """

    def build(
        self,
        plan: SqlBuildPlan,
        spec: ParsedDeveloperSpec | None = None,
        manifest: SourceManifest | None = None,
    ) -> ReviewReport:
        """从 SqlBuildPlan 构建审查报告。

        Args:
            plan: 待审查的 SqlBuildPlan
            spec: 已解析的 DeveloperSpec（提供枚举值声明）
            manifest: 事实源（提供字段来源信息）

        Returns:
            ReviewReport——含标签规则、窗口表达式和字段血缘
        """
        # 收集声明枚举值（用于标记标签规则的声明范围）
        declared_enums = self._collect_declared_enums(spec, manifest)

        # 1. 提取标签规则
        label_rules = self._extract_label_rules(plan, declared_enums)

        # 2. 提取窗口表达式
        window_exprs = self._extract_window_exprs(plan)

        # 3. 构建字段血缘
        field_lineage = self._extract_field_lineage(plan)

        # 4. 收集源表
        source_tables = self._extract_source_tables(plan)

        # 5. 收集输出列
        output_columns = self._extract_output_columns(plan)

        return ReviewReport(
            plan_id=plan.plan_id,
            label_rules=label_rules,
            window_exprs=window_exprs,
            field_lineage=field_lineage,
            source_tables=source_tables,
            output_columns=output_columns,
        )

    def build_summary(self, report: ReviewReport) -> str:
        """生成审查报告的人类可读摘要。

        Args:
            report: ReviewReport

        Returns:
            Markdown 格式的审查摘要
        """
        lines: list[str] = []
        lines.append(f"# 审查报告：{report.plan_id}")
        lines.append("")

        # ── 标签规则 ──
        if report.label_rules:
            lines.append("## 标签规则（CASE WHEN）")
            lines.append("")
            for rule in report.label_rules:
                lines.append(f"### {rule.alias or rule.step_id}")
                for branch in rule.branches:
                    lines.append(f"- {branch}")
                lines.append(f"- ELSE → {rule.else_value}")
                if rule.enum_values_declared:
                    lines.append(
                        f"- 声明枚举值: {', '.join(rule.enum_values_declared)}"
                    )
                lines.append("")
        else:
            lines.append("## 标签规则")
            lines.append("")
            lines.append("（无 CASE WHEN 标签规则）")
            lines.append("")

        # ── 窗口表达式 ──
        if report.window_exprs:
            lines.append("## 窗口函数")
            lines.append("")
            for we in report.window_exprs:
                lines.append(f"### {we.alias} (`{we.function}`)")
                if we.input_column:
                    lines.append(f"- 输入列: `{we.input_column}`")
                if we.partition_by:
                    lines.append(f"- PARTITION BY: {', '.join(f'`{c}`' for c in we.partition_by)}")
                if we.order_by:
                    lines.append(f"- ORDER BY: {', '.join(we.order_by)}")
                if we.frame:
                    lines.append(f"- Frame: {we.frame}")
                lines.append("")
        else:
            lines.append("## 窗口函数")
            lines.append("")
            lines.append("（无窗口函数）")
            lines.append("")

        # ── 字段血缘 ──
        if report.field_lineage:
            lines.append("## 字段血缘")
            lines.append("")
            lines.append("| 源表 | 源字段 | 转换 | 输出别名 |")
            lines.append("|------|--------|------|----------|")
            for entry in report.field_lineage:
                transforms = " → ".join(entry.transformations) if entry.transformations else "-"
                lines.append(
                    f"| {entry.source_table} | {entry.source_column} "
                    f"| {transforms} | {entry.output_alias} |"
                )
            lines.append("")

        # ── 源表 ──
        lines.append("## 源表")
        lines.append("")
        for t in report.source_tables:
            lines.append(f"- `{t}`")
        lines.append("")

        # ── 输出列 ──
        lines.append("## 输出列")
        lines.append("")
        for col in report.output_columns:
            lines.append(f"- `{col}`")

        return "\n".join(lines)

    # ── 抽取方法 ──

    def _extract_label_rules(
        self,
        plan: SqlBuildPlan,
        declared_enums: list[str],
    ) -> list[LabelRuleEntry]:
        """从 SqlBuildPlan 提取所有 CASE WHEN 标签规则。"""
        rules: list[LabelRuleEntry] = []

        for step in plan.steps:
            if isinstance(step, CaseWhenStep):
                branches: list[str] = []
                for branch in step.cases:
                    cond_desc = _describe_predicate(branch.condition)
                    result_desc = _describe_literal(branch.result)
                    branches.append(f"WHEN {cond_desc} THEN {result_desc}")

                else_desc = (
                    _describe_literal(step.else_value)
                    if step.else_value is not None
                    else "NULL"
                )

                rules.append(
                    LabelRuleEntry(
                        step_id=step.step_id,
                        alias=step.alias,
                        branches=branches,
                        else_value=else_desc,
                        enum_values_declared=list(declared_enums),
                    )
                )

        return rules

    def _extract_window_exprs(
        self,
        plan: SqlBuildPlan,
    ) -> list[WindowExprEntry]:
        """从 SqlBuildPlan 提取所有窗口函数表达式。"""
        entries: list[WindowExprEntry] = []

        for step in plan.steps:
            if isinstance(step, WindowStep):
                for wexpr in step.window_exprs:
                    func_name = (
                        wexpr.function.value
                        if hasattr(wexpr.function, "value")
                        else str(wexpr.function)
                    )

                    # 输入列
                    input_col = ""
                    if wexpr.input is not None:
                        if hasattr(wexpr.input, "column_name"):
                            input_col = wexpr.input.column_name
                        elif hasattr(wexpr.input, "value"):
                            input_col = str(wexpr.input.value)

                    # PARTITION BY
                    partition_cols = [
                        c.column_name for c in wexpr.partition_by
                    ]

                    # ORDER BY
                    order_cols = [
                        _format_sort_spec(s) for s in wexpr.order_by
                    ]

                    # Frame
                    frame_desc = ""
                    if wexpr.frame is not None:
                        frame_type = (
                            wexpr.frame.frame_type.value
                            if hasattr(wexpr.frame.frame_type, "value")
                            else str(wexpr.frame.frame_type)
                        )
                        start_desc = _describe_boundary(wexpr.frame.start)
                        end_desc = _describe_boundary(wexpr.frame.end)
                        frame_desc = f"{frame_type} BETWEEN {start_desc} AND {end_desc}"

                    entries.append(
                        WindowExprEntry(
                            step_id=step.step_id,
                            function=func_name,
                            input_column=input_col,
                            partition_by=partition_cols,
                            order_by=order_cols,
                            frame=frame_desc,
                            alias=wexpr.alias,
                        )
                    )

        # 也从 ProjectStep 的 AliasExpr 中提取窗口表达式
        for step in plan.steps:
            if isinstance(step, ProjectStep):
                for ae in step.columns:
                    if isinstance(ae.expression, WindowExpr):
                        wexpr = ae.expression
                        func_name = (
                            wexpr.function.value
                            if hasattr(wexpr.function, "value")
                            else str(wexpr.function)
                        )
                        input_col = ""
                        if wexpr.input is not None:
                            if hasattr(wexpr.input, "column_name"):
                                input_col = wexpr.input.column_name
                        partition_cols = [c.column_name for c in wexpr.partition_by]
                        order_cols = [_format_sort_spec(s) for s in wexpr.order_by]
                        frame_desc = ""
                        if wexpr.frame is not None:
                            frame_type_val = (
                                wexpr.frame.frame_type.value
                                if hasattr(wexpr.frame.frame_type, "value")
                                else str(wexpr.frame.frame_type)
                            )
                            frame_desc = _format_frame_desc(
                                frame_type_val, wexpr.frame
                            )

                        entries.append(
                            WindowExprEntry(
                                step_id=step.step_id,
                                function=func_name,
                                input_column=input_col,
                                partition_by=partition_cols,
                                order_by=order_cols,
                                frame=frame_desc,
                                alias=wexpr.alias,
                            )
                        )

        return entries

    def _extract_field_lineage(
        self,
        plan: SqlBuildPlan,
    ) -> list[FieldLineageEntry]:
        """从 SqlBuildPlan 构建字段血缘——从源表到输出的转换链。

        追踪规则：
        - ScanStep.required_columns → 源字段
        - AggregateStep → 聚合转换
        - CaseWhenStep → CASE WHEN 转换
        - WindowStep → 窗口函数转换
        - ProjectStep → 最终输出别名
        """
        entries: list[FieldLineageEntry] = []

        # 收集各步骤产生的转换
        lineage_map: dict[str, FieldLineageEntry] = {}

        # 从 ScanStep 收集源字段
        for step in plan.steps:
            if isinstance(step, ScanStep):
                for col in step.required_columns:
                    key = col.normalized_name or col.column_name
                    lineage_map[key] = FieldLineageEntry(
                        source_table=step.table_ref,
                        source_column=col.column_name,
                        transformations=[],
                        output_alias=col.column_name,
                    )

        # 聚合转换
        for step in plan.steps:
            if isinstance(step, AggregateStep):
                for m in step.metrics:
                    input_col = m.input_column or "*"
                    key = m.alias
                    if input_col in lineage_map:
                        entry = lineage_map[input_col]
                        lineage_map[key] = FieldLineageEntry(
                            source_table=entry.source_table,
                            source_column=entry.source_column,
                            transformations=entry.transformations + [f"{m.aggregation}()"],
                            output_alias=m.alias,
                        )
                    else:
                        lineage_map[key] = FieldLineageEntry(
                            source_table="",
                            source_column=input_col,
                            transformations=[f"{m.aggregation}()"],
                            output_alias=m.alias,
                        )

        # CASE WHEN 转换
        for step in plan.steps:
            if isinstance(step, CaseWhenStep) and step.alias:
                # 从第一个分支的 condition 推断源字段
                source_col = ""
                source_table = ""
                if step.cases:
                    first_cond = step.cases[0].condition
                    source_col = _extract_column_from_predicate(first_cond)
                    if source_col and source_col in lineage_map:
                        source_table = lineage_map[source_col].source_table

                lineage_map[step.alias] = FieldLineageEntry(
                    source_table=source_table,
                    source_column=source_col,
                    transformations=["CASE WHEN"],
                    output_alias=step.alias,
                )

        # 窗口函数转换
        for step in plan.steps:
            if isinstance(step, WindowStep):
                for wexpr in step.window_exprs:
                    func_name = (
                        wexpr.function.value
                        if hasattr(wexpr.function, "value")
                        else str(wexpr.function)
                    )
                    source_col = ""
                    source_table = ""
                    if wexpr.input is not None and hasattr(wexpr.input, "column_name"):
                        source_col = wexpr.input.column_name
                        if source_col in lineage_map:
                            source_table = lineage_map[source_col].source_table

                    lineage_map[wexpr.alias] = FieldLineageEntry(
                        source_table=source_table,
                        source_column=source_col,
                        transformations=[f"{func_name}() OVER"],
                        output_alias=wexpr.alias,
                    )

        # 投影步骤——收集最终输出列
        for step in plan.steps:
            if isinstance(step, ProjectStep):
                for ae in step.columns:
                    entry = lineage_map.get(ae.alias)
                    if entry:
                        entries.append(entry)
                    elif hasattr(ae.expression, "column_name"):
                        col_name = ae.expression.column_name
                        if col_name in lineage_map:
                            entries.append(lineage_map[col_name])

        # 如果没有 ProjectStep，输出所有已追踪的字段
        if not entries:
            entries = list(lineage_map.values())

        return entries

    def _extract_source_tables(self, plan: SqlBuildPlan) -> list[str]:
        """提取 SqlBuildPlan 引用的所有源表。"""
        tables: list[str] = []
        seen: set[str] = set()

        for step in plan.steps:
            if isinstance(step, ScanStep):
                if step.table_ref not in seen:
                    seen.add(step.table_ref)
                    tables.append(step.table_ref)

        return tables

    def _extract_output_columns(self, plan: SqlBuildPlan) -> list[str]:
        """提取 SqlBuildPlan 的最终输出列名。"""
        columns: list[str] = []

        for step in plan.steps:
            if isinstance(step, ProjectStep):
                for ae in step.columns:
                    columns.append(ae.alias or str(ae.expression))
                return columns

        # 没有 ProjectStep 时收集所有步骤产出的别名
        for step in plan.steps:
            if isinstance(step, CaseWhenStep) and step.alias:
                columns.append(step.alias)
            elif isinstance(step, WindowStep):
                for wexpr in step.window_exprs:
                    if wexpr.alias:
                        columns.append(wexpr.alias)
            elif isinstance(step, AggregateStep):
                for m in step.metrics:
                    columns.append(m.alias)
                for gk in step.group_keys:
                    # 兼容 ColumnRef / DatePartExpression / DerivedGroupKey
                    columns.append(
                        gk.column_name if isinstance(gk, ColumnRef) else gk.alias
                    )

        return columns

    @staticmethod
    def _collect_declared_enums(
        spec: ParsedDeveloperSpec | None,
        manifest: SourceManifest | None,
    ) -> list[str]:
        """收集所有已声明的枚举值——用于审查报告展示。

        Returns:
            去重排序的枚举值列表
        """
        values: set[str] = set()

        if spec:
            for table in spec.input_tables:
                for col in table.columns:
                    if col.enum_values:
                        values.update(col.enum_values)

        if manifest:
            for table in manifest.tables:
                for col in table.columns:
                    if col.enum_values:
                        values.update(col.enum_values)

        return sorted(values)

    @staticmethod
    def compute_review_hash(report: ReviewReport) -> str:
        """计算审查报告的确定性 hash。

        Args:
            report: ReviewReport

        Returns:
            SHA-256 hex 前 16 位
        """
        content = report.model_dump_json(exclude_none=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _describe_predicate(pred: Predicate) -> str:
    """将 Predicate 描述为人类可读的字符串。"""
    left = pred.left
    left_str = ""
    if hasattr(left, "table_ref") and hasattr(left, "column_name"):
        left_str = f"{left.table_ref}.{left.column_name}" if left.table_ref else left.column_name
    elif hasattr(left, "operator"):
        left_str = f"({_describe_predicate(left)})"
    else:
        left_str = str(left)

    op = pred.operator.value if hasattr(pred.operator, "value") else str(pred.operator)

    right = pred.right
    right_str = ""
    if right is None:
        right_str = ""
    elif isinstance(right, list):
        right_str = ", ".join(_describe_literal(r) for r in right)
    elif hasattr(right, "value"):
        right_str = _describe_literal(right)
    elif hasattr(right, "operator"):
        right_str = f"({_describe_predicate(right)})"
    else:
        right_str = str(right)

    if op in ("IS_NULL",):
        return f"{left_str} IS NULL"
    if op in ("IS_NOT_NULL",):
        return f"{left_str} IS NOT NULL"
    if op in ("IN", "NOT_IN"):
        return f"{left_str} {op.replace('_', ' ')} ({right_str})"
    if op in ("BETWEEN",):
        return f"{left_str} BETWEEN {right_str}"

    return f"{left_str} {op} {right_str}"


def _describe_literal(lit) -> str:
    """描述 SqlLiteral 为人类可读字符串。"""
    if lit is None:
        return "NULL"
    v = lit.value if hasattr(lit, "value") else lit
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)


def _describe_boundary(boundary) -> str:
    """描述 FrameBoundary 为人类可读字符串。"""
    kind = boundary.kind
    kind_value = kind.value if hasattr(kind, "value") else str(kind)

    kind_map = {
        "CURRENT_ROW": "CURRENT ROW",
        "UNBOUNDED_PRECEDING": "UNBOUNDED PRECEDING",
        "UNBOUNDED_FOLLOWING": "UNBOUNDED FOLLOWING",
        "N_PRECEDING": f"{boundary.offset} PRECEDING",
        "N_FOLLOWING": f"{boundary.offset} FOLLOWING",
    }
    return kind_map.get(kind_value, str(kind_value))


def _extract_column_from_predicate(pred: Predicate) -> str:
    """从 Predicate 的 left 侧提取源字段名。"""
    left = pred.left
    if left is None:
        return ""
    if hasattr(left, "column_name"):
        return left.column_name
    if hasattr(left, "operator"):
        return _extract_column_from_predicate(left)
    return ""


def _format_sort_spec(s) -> str:
    """格式化 SortSpec 为人类可读字符串。"""
    direction = s.direction.value if hasattr(s.direction, "value") else str(s.direction)
    return f"{s.column} {direction}"


def _format_frame_desc(frame_type: str, frame) -> str:
    """格式化 WindowFrame 为人类可读描述。"""
    start_desc = _describe_boundary(frame.start)
    end_desc = _describe_boundary(frame.end)
    return f"{frame_type} BETWEEN {start_desc} AND {end_desc}"
