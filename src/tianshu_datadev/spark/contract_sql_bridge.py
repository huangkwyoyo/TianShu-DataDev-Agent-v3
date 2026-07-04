"""Contract → SqlBuildPlan 桥接模块——C3 PlanComparator 双管线闭环。

该模块提供 DataTransformContractV1 → SqlBuildPlan step 列表的确定性映射。
是 C3（PlanComparator——SQL ↔ Spark 逻辑对比）最小闭环的关键一环。

映射规则（与 Mapper 的 SparkPlan 映射对称）：
- input_tables → ScanStep（每表一个）
- filters → FilterStep
- join_relationships → JoinStep
- aggregations + grouping_keys → AggregateStep
- case_when_labels → CaseWhenStep（标签级，不含完整谓词）
- output_columns → ProjectStep
- sort_spec → SortStep
- limit_spec → LimitStep

已知限制：
- window_specs 不映射——Phase 7B Comparator 不启用 window 对比
- case_when 仅映射标签级结构，完整谓词表达式在后续 Phase 覆盖
- 该桥接是确定性轻量映射，不涉及 SQL pipeline 的 SpecEnricher 推测逻辑

后续若 SQL pipeline 提供正式的 Contract → SqlBuildPlan 路径，可直接替换本模块的映射逻辑。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.artifacts.models import DataTransformContractV1


def contract_to_sql_steps(
    contract: DataTransformContractV1,
) -> list:
    """从 DataTransformContractV1 确定性构造 SqlBuildPlan step 列表。

    该函数将 Contract 中各个结构化字段一一映射为 SqlBuildPlan 的各类 Step，
    作为 SQL 管线的输入供 PlanComparator 与 Mapper 产出的 SparkPlan 对比。

    映射规则说明：
    - input_tables 中的每个表生成一个 ScanStep
    - filters 中的每个谓词生成一个 FilterStep（谓词左右值从点分隔符解析）
    - join_relationships 中的每个关系生成一个 JoinStep
    - aggregations + grouping_keys 组合生成一个 AggregateStep
    - case_when_labels 中的每个标签规格生成一个 CaseWhenStep（仅标签级占位）
    - output_columns 生成一个 ProjectStep
    - sort_spec 生成一个 SortStep
    - limit_spec 生成一个 LimitStep

    参数:
        contract: DataTransformContractV1 实例，包含输入表、过滤条件、
                  关联关系、聚合分组、CASE WHEN、输出列、排序、限制等字段。

    返回:
        SqlBuildPlan step 列表（元素为 ScanStep / FilterStep / JoinStep /
        AggregateStep / CaseWhenStep / ProjectStep / SortStep / LimitStep）。

    防御行为:
        contract.input_tables 为空时直接返回空列表——说明没有数据源，无法构建有意义的 plan。

    已知限制:
        window_specs 不做映射——当前 Phase 阶段 Comparator 仅启用 8 种类型。
    """
    from tianshu_datadev.planning.models import (
        AggregateSpec,
        AggregationType,
        AliasExpr,
        ColumnRef,
        Predicate,
        PredicateOperator,
        SortDirection,
        SortSpec,
        SqlLiteral,
        WhenBranch,
    )
    from tianshu_datadev.planning.sql_build_plan import (
        AggregateStep,
        CaseWhenStep,
        FilterStep,
        JoinStep,
        LimitStep,
        ProjectStep,
        ScanStep,
        SortStep,
    )

    # 防御检查：无输入表时直接返回空列表
    if not contract.input_tables:
        return []

    steps: list = []

    # 1. input_tables → ScanStep
    for tbl in contract.input_tables:
        steps.append(ScanStep(
            step_type="scan",
            step_id=f"scan_{tbl.table_ref}",
            table_ref=tbl.table_ref,
            required_columns=[],  # 测试级——Comparator 不逐列对比 required_columns
        ))

    # 2. filters → FilterStep
    for i, f in enumerate(contract.filters):
        left_parts = f.left.split(".", 1)
        left_col = left_parts[1] if len(left_parts) > 1 else left_parts[0]
        left_table = left_parts[0] if len(left_parts) > 1 else ""
        steps.append(FilterStep(
            step_type="filter",
            step_id=f"filter_{i:03d}",
            predicate=Predicate(
                left=ColumnRef(table_ref=left_table, column_name=left_col, normalized_name=left_col),
                operator=PredicateOperator(f.operator),
                right=SqlLiteral(value=f.right.strip("'").strip('"')),
            ),
        ))

    # 3. join_relationships → JoinStep
    for j in contract.join_relationships:
        steps.append(JoinStep(
            step_type="join",
            step_id=f"join_{j.join_id}",
            right_table_ref=j.right_table,
            join_type=j.join_type,
            join_keys=[(
                ColumnRef(table_ref=j.left_table, column_name=j.left_key, normalized_name=j.left_key),
                ColumnRef(table_ref=j.right_table, column_name=j.right_key, normalized_name=j.right_key),
            )],
            relationship_ref=j.join_id,
        ))

    # 4. aggregations + grouping_keys → AggregateStep
    if contract.aggregations:
        agg_metrics = []
        for agg in contract.aggregations:
            agg_type = AggregationType(agg.function) if agg.function else AggregationType.COUNT
            # 从 "od.amount" 提取列名 "amount"
            raw_col = agg.input_column or ""
            col_name = raw_col.split(".", 1)[1] if "." in raw_col else raw_col
            agg_metrics.append(AggregateSpec(
                aggregation=agg_type,
                input_column=col_name,
                alias=agg.alias,
            ))
        group_keys = []
        for gk in contract.grouping_keys:
            parts = gk.split(".", 1)
            col = parts[1] if len(parts) > 1 else parts[0]
            tbl = parts[0] if len(parts) > 1 else ""
            group_keys.append(ColumnRef(table_ref=tbl, column_name=col, normalized_name=col))
        steps.append(AggregateStep(
            step_type="aggregate",
            step_id="agg_001",
            group_keys=group_keys,
            metrics=agg_metrics,
        ))

    # 5. case_when_labels → CaseWhenStep（标签级——完整谓词在后续 Phase 做）
    if contract.case_when_labels:
        for cw in contract.case_when_labels:
            cases = []
            for label in cw.labels:
                cases.append(WhenBranch(
                    condition=Predicate(
                        left=ColumnRef(table_ref="", column_name="", normalized_name=""),
                        operator=PredicateOperator.EQ,
                        right=SqlLiteral(value=""),
                    ),
                    result=SqlLiteral(value=label),
                ))
            steps.append(CaseWhenStep(
                step_type="case_when",
                step_id=f"cw_{cw.statement_id}",
                cases=cases,
                else_value=SqlLiteral(value=cw.else_label) if cw.else_label else None,
                alias=cw.output_alias,
            ))

    # 6. output_columns → ProjectStep
    if contract.output_columns:
        proj_cols = []
        for oc in contract.output_columns:
            col_ref = ColumnRef(
                table_ref="", column_name=oc.column_name,
                normalized_name=oc.column_name,
            )
            proj_cols.append(AliasExpr(expression=col_ref, alias=oc.alias))
        steps.append(ProjectStep(
            step_type="project",
            step_id="proj_001",
            columns=proj_cols,
        ))

    # 7. sort_spec → SortStep
    if contract.sort_spec:
        sort_specs = []
        for s in contract.sort_spec:
            sort_specs.append(SortSpec(
                column=s.column,
                direction=SortDirection(s.direction),
            ))
        steps.append(SortStep(
            step_type="sort",
            step_id="sort_001",
            order_by=sort_specs,
        ))

    # 8. limit_spec → LimitStep
    if contract.limit_spec:
        steps.append(LimitStep(
            step_type="limit",
            step_id="limit_001",
            limit=contract.limit_spec.limit,
        ))

    return steps
