"""派生列解析器——检测 output_spec 中无法匹配源表物理列的派生列。

独立于 Parser——在 SpecEnricher 之后、Label Extractor 之前调用。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec
    from tianshu_datadev.manifest.models import SourceManifest


def _find_unresolved_derived_columns(
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest | None = None,
) -> list[str]:
    """查找 output_spec 中无法匹配源表物理列的派生列名。

    匹配策略（按优先级）：
    1. 源表物理列（input_tables 中各表的 columns）
    2. 指标输出名（metrics）
    3. 维度输出名（dimensions）
    4. 窗口指标输出名（inferred_window_metrics）
    5. compute_steps 输出名
    6. 已有 label_rules 输出名

    Args:
        spec: 已解析的 DeveloperSpec
        manifest: 可选的 SourceManifest——其 schema 字段可补充列名

    Returns:
        未能匹配到任何源列的输出列名列表——这些列需要 Label Extractor 解析
    """
    # 收集所有已知列名
    known: set[str] = set()

    # 1. 源表物理列
    for table in spec.input_tables:
        for col in table.columns:
            known.add(col.column_name)
            known.add(col.normalized_name)

    # 2. 指标输出名
    for metric in spec.metrics:
        known.add(metric.alias)

    # 3. 维度输出名
    for dim in spec.dimensions:
        known.add(dim.dimension_name)

    # 4. 窗口指标
    for wm in spec.inferred_window_metrics:
        known.add(wm.name)

    # 5. compute_steps 输出列名——从 metrics/expressions/case_when 收集
    if spec.compute_steps:
        for step in spec.compute_steps:
            for m in step.metrics:
                known.add(m.alias)
            for expr in step.expressions:
                known.add(expr.name)
            if step.case_when is not None:
                known.add(step.case_when.output_column)

    # 6. 已有 label_rules 的输出列名
    for rule in spec.label_rules:
        known.add(rule.output_column)

    # 7. Manifest schema（如果有）
    if manifest is not None:
        for schema_col in manifest.schema or []:
            known.add(schema_col.get("name", ""))
            known.add(schema_col.get("normalized_name", ""))

    # 移除空字符串
    known.discard("")

    # 找出不在已知列中的输出列
    unresolved: list[str] = []
    for col in spec.output_spec.columns:
        if col.name not in known:
            unresolved.append(col.name)

    return unresolved
