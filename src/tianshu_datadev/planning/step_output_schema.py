"""StepOutputSchema——ComputeStep 输出列的类型与唯一键元数据。

与 step_outputs (dict[str, list[ColumnRef]]) 并列存在——
step_outputs 供血缘追踪使用（不改），StepOutputSchema 供 Validator 校验使用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ComputeStep
    from tianshu_datadev.developer_spec.source_manifest import SourceManifest


@dataclass
class StepOutputSchema:
    """一个 ComputeStep 的输出列类型与唯一键元数据。

    columns: {normalized_name: type_string | None}
        None = UNKNOWN——无法从源表或聚合函数推导类型。
        UNKNOWN 类型参与 Join 时必须阻断。

    unique_keys: 派生唯一键组——来自 Aggregate 的 group_by 列。
        每组是一个列名列表，表示一组复合唯一键。
        单列 group_by → [[col]]，多列 → [[col1, col2]]。
    """

    columns: dict[str, str | None] = field(default_factory=dict)
    unique_keys: list[list[str]] = field(default_factory=list)


def compute_output_schema(
    cs: ComputeStep,
    step_schemas: dict[str, StepOutputSchema],
    manifest: SourceManifest | None,
    normalizer,  # FieldNormalizer
) -> StepOutputSchema:
    """从 ComputeStep 声明推导其输出列的 StepOutputSchema。

    类型推导规则（按优先级）：
    1. GROUP BY 列——类型来自源表 manifest 或上游 step_schemas
    2. 指标别名——类型由聚合函数推导（COUNT→bigint, AVG→double 等）
    3. CASE WHEN 输出列——固定 varchar（CASE WHEN 始终返回字符串标签）
    4. Expression 输出列——本轮移除（HUMAN_REVIEW），不在此推导

    无法确定类型时设为 None（UNKNOWN）——禁止默认 varchar/double。
    """
    columns: dict[str, str | None] = {}
    unique_keys: list[list[str]] = []

    # ── 构建源表列→类型查找表 ──
    manifest_cols: dict[str, str] = {}  # {normalized_name: type}
    if manifest:
        for table in manifest.tables:
            for col in table.columns:
                manifest_cols[normalizer.normalize(col.column_name)] = col.column_type

    # ── 1. GROUP BY 列——类型来自源表或上游 ──
    for gb in cs.group_by:
        gb_norm = normalizer.normalize(gb)
        gb_type: str | None = None

        # 从上游步骤查找
        if isinstance(cs.source, str) and cs.source != "input":
            upstream = step_schemas.get(cs.source)
            if upstream:
                gb_type = upstream.columns.get(gb_norm)
        elif isinstance(cs.source, list):
            # 多源——尝试从第一个有该列的源获取类型
            for src_name in cs.source:
                upstream = step_schemas.get(src_name)
                if upstream and gb_norm in upstream.columns:
                    gb_type = upstream.columns[gb_norm]
                    break

        # 从 manifest 查找
        if gb_type is None:
            gb_type = manifest_cols.get(gb_norm)
            # 仍为 None → UNKNOWN（不设默认值）

        columns[gb_norm] = gb_type

    # ── 2. 指标别名——类型由聚合函数推导 ──
    for m in cs.metrics:
        alias = m.alias or m.metric_name
        if not alias:
            continue
        alias_norm = normalizer.normalize(alias)
        agg_str = m.aggregation.value if hasattr(m.aggregation, "value") else str(m.aggregation)

        if agg_str in ("COUNT", "COUNT_DISTINCT"):
            columns[alias_norm] = "bigint"
        elif agg_str == "AVG":
            columns[alias_norm] = "double"
        elif agg_str == "SUM":
            # SUM 类型继承 input_column 类型——从 manifest 或上游查找
            sum_type: str | None = None
            if m.input_column:
                input_norm = normalizer.normalize(m.input_column)
                sum_type = manifest_cols.get(input_norm)
                if sum_type is None and isinstance(cs.source, str) and cs.source != "input":
                    upstream = step_schemas.get(cs.source)
                    if upstream:
                        sum_type = upstream.columns.get(input_norm)
            columns[alias_norm] = sum_type  # None=UNKNOWN
        elif agg_str in ("MIN", "MAX"):
            # MIN/MAX 继承 input_column 类型
            minmax_type: str | None = None
            if m.input_column:
                input_norm = normalizer.normalize(m.input_column)
                minmax_type = manifest_cols.get(input_norm)
                if minmax_type is None and isinstance(cs.source, str) and cs.source != "input":
                    upstream = step_schemas.get(cs.source)
                    if upstream:
                        minmax_type = upstream.columns.get(input_norm)
            columns[alias_norm] = minmax_type
        else:
            columns[alias_norm] = None  # UNKNOWN

    # ── 3. CASE WHEN 输出列——固定 varchar ──
    if cs.case_when and cs.case_when.output_column:
        cw_norm = normalizer.normalize(cs.case_when.output_column)
        columns[cw_norm] = "varchar"

    # ── 4. Expression 输出列——本轮不处理（已移除或 HUMAN_REVIEW）──
    # 不在 columns 中添加任何 expression 输出列

    # ── 5. 派生 unique_keys——Aggregate 的 group_by 形成 ──
    if cs.group_by:
        group_norms = [normalizer.normalize(gb) for gb in cs.group_by]
        unique_keys.append(group_norms)

    return StepOutputSchema(columns=columns, unique_keys=unique_keys)
