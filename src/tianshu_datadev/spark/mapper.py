"""Phase 5 SparkPlanMapper——DataTransformContractV1 → SparkPlan 确定性映射。

映射规则：
1. ContractInputTable → SparkReadStep（每个输入表一个 ReadStep）
2. ContractPredicate → SparkFilterStep（每个过滤条件一个 FilterStep）
3. ContractJoin → SparkJoinStep（每个 Join 关系一个 JoinStep）
4. ContractAggregation + grouping_keys → SparkAggregateStep（合并为一个聚合步骤）
5. ContractOutputColumn → SparkProjectStep（合并为一个投影步骤）
6. CaseWhenLabelSpec → SparkCaseWhenStep（每个标签一个 CaseWhenStep）
7. WindowSpecSummary → SparkWindowStep（合并为一个窗口步骤）
8. ContractSort → SparkSortStep
9. ContractLimit → SparkLimitStep

所有映射均为确定性——相同 Contract → 相同 SparkPlan → 相同 hash。
不读取 SQL 文本，不读取 SqlBuildPlan，不调用 LLM。
"""

from __future__ import annotations

from tianshu_datadev.artifacts.models import (
    CaseWhenLabelSpec,
    ContractAggregation,
    ContractInputTable,
    ContractJoin,
    ContractLimit,
    ContractOutputColumn,
    ContractSort,
    DataTransformContractV1,
    WindowSpecSummary,
)

from .models import (
    ContractGap,
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkCaseWhenBranch,
    SparkCaseWhenStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkJoinType,
    SparkLimitStep,
    SparkPlan,
    SparkPlanMappingResult,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
    UnsupportedPattern,
)

# ── 映射函数名到枚举的查找表 ──

_AGG_FUNCTION_MAP: dict[str, SparkAggFunction] = {
    "COUNT": SparkAggFunction.COUNT,
    "COUNT_DISTINCT": SparkAggFunction.COUNT_DISTINCT,
    "SUM": SparkAggFunction.SUM,
    "AVG": SparkAggFunction.AVG,
    "MIN": SparkAggFunction.MIN,
    "MAX": SparkAggFunction.MAX,
}

_WINDOW_FUNCTION_MAP: dict[str, SparkWindowFunction] = {
    "ROW_NUMBER": SparkWindowFunction.ROW_NUMBER,
    "RANK": SparkWindowFunction.RANK,
    "DENSE_RANK": SparkWindowFunction.DENSE_RANK,
    "NTILE": SparkWindowFunction.NTILE,
    "LAG": SparkWindowFunction.LAG,
    "LEAD": SparkWindowFunction.LEAD,
    "SUM_OVER": SparkWindowFunction.SUM_OVER,
    "AVG_OVER": SparkWindowFunction.AVG_OVER,
    "COUNT_OVER": SparkWindowFunction.COUNT_OVER,
}

_JOIN_TYPE_MAP: dict[str, SparkJoinType] = {
    "INNER": SparkJoinType.INNER,
    "LEFT": SparkJoinType.LEFT,
    "RIGHT": SparkJoinType.RIGHT,
    "FULL": SparkJoinType.FULL,
    "inner": SparkJoinType.INNER,
    "left": SparkJoinType.LEFT,
    "right": SparkJoinType.RIGHT,
    "full": SparkJoinType.FULL,
}

_SORT_DIRECTION_MAP: dict[str, SparkSortDirection] = {
    "ASC": SparkSortDirection.ASC,
    "DESC": SparkSortDirection.DESC,
    "asc": SparkSortDirection.ASC,
    "desc": SparkSortDirection.DESC,
}


def map_contract_to_spark_plan(
    contract: DataTransformContractV1,
) -> SparkPlanMappingResult:
    """将 DataTransformContractV1 确定性映射为 SparkPlan IR。

    这是 Phase 5 的核心映射函数——它是纯函数，相同输入产生相同输出。
    不读取 SQL 文本或 SqlBuildPlan。

    Args:
        contract: 从 SqlProgram 确定性抽取的 DataTransformContractV1

    Returns:
        SparkPlanMappingResult——成功时 spark_plan 非空，失败时记录 unsupported/gaps
    """
    unsupported: list[UnsupportedPattern] = []
    gaps: list[ContractGap] = []
    warnings: list[str] = []

    # 计算 contract hash（用于 plan_id 派生）
    contract_hash = DataTransformContractV1.compute_contract_hash(contract)

    # ── Step 1：输入表 → ReadStep ──
    if not contract.input_tables:
        gaps.append(
            ContractGap(
                gap_id="gap_input_tables",
                contract_field="input_tables",
                missing_info="Contract 不含任何输入表",
                severity="BLOCKING",
            )
        )

    read_steps = _map_input_tables(contract.input_tables)

    # ── Step 2：过滤条件 → FilterStep ──
    filter_steps = _map_filters(contract.filters, unsupported)

    # ── Step 3：Join 关系 → JoinStep ──
    join_steps = _map_joins(contract.join_relationships, unsupported)

    # ── Step 4：聚合 → AggregateStep ──
    agg_result = _map_aggregations(
        contract.aggregations, contract.grouping_keys, unsupported, gaps,
    )
    if isinstance(agg_result, ContractGap):
        gaps.append(agg_result)
        agg_steps = []
    elif isinstance(agg_result, UnsupportedPattern):
        unsupported.append(agg_result)
        agg_steps = []
    else:
        agg_steps = agg_result

    # ── Step 5：输出列 → ProjectStep ──
    project_result = _map_output_columns(contract.output_columns, gaps)
    if isinstance(project_result, ContractGap):
        gaps.append(project_result)
        project_steps = []
    else:
        project_steps = project_result

    # ── Step 6：CASE WHEN 标签 → CaseWhenStep ──
    case_when_steps = _map_case_when(contract.case_when_labels)

    # ── Step 7：窗口函数 → WindowStep ──
    window_result = _map_windows(contract.window_specs, unsupported)
    if isinstance(window_result, UnsupportedPattern):
        unsupported.append(window_result)
        window_steps = []
    else:
        window_steps = window_result

    # ── Step 8：排序 → SortStep ──
    sort_steps = _map_sort(contract.sort_spec)

    # ── Step 9：行限制 → LimitStep ──
    limit_steps = _map_limit(contract.limit_spec)

    # ── 检查是否有 BLOCKING gap 或 unsupported ──
    blocking_gaps = [g for g in gaps if g.severity == "BLOCKING"]
    success = len(unsupported) == 0 and len(blocking_gaps) == 0

    if not success:
        return SparkPlanMappingResult(
            success=False,
            spark_plan=None,
            unsupported=unsupported,
            gaps=gaps,
            warnings=warnings,
        )

    # ── 组装 SparkPlan ──
    plan_id = SparkPlan.generate_plan_id(contract_hash)

    # 步骤顺序：Read → Filter → Join → Aggregate → Window → CaseWhen → Project → Sort → Limit
    steps: list = []
    steps.extend(read_steps)
    steps.extend(filter_steps)
    steps.extend(join_steps)
    steps.extend(agg_steps)
    steps.extend(window_steps)
    steps.extend(case_when_steps)
    steps.extend(project_steps)
    steps.extend(sort_steps)
    steps.extend(limit_steps)

    spark_plan = SparkPlan(
        plan_id=plan_id,
        version="v1",
        source_phase="phase-5",
        source_contract_hash=contract_hash,
        source_contract_version="v1",
        steps=steps,
        write_mode=(
            "overwrite_partition"
            if contract.write_spec and contract.write_spec.get("type") == "partition_overwrite"
            else None
        ),
    )

    # WARN 级别 gap 进入 warnings
    warn_gaps = [g for g in gaps if g.severity == "WARN"]
    for g in warn_gaps:
        warnings.append(f"WARN: {g.contract_field} — {g.missing_info}")

    return SparkPlanMappingResult(
        success=True,
        spark_plan=spark_plan,
        unsupported=[],
        gaps=warn_gaps,
        warnings=warnings,
    )


# ════════════════════════════════════════════
# 各字段映射辅助函数
# ════════════════════════════════════════════


def _map_input_tables(
    input_tables: list[ContractInputTable],
) -> list[SparkReadStep]:
    """将 Contract 的 input_tables 映射为 ReadStep 列表。

    每个输入表生成一个独立的 ReadStep。
    物理路径不存放在 SparkPlan 中——由 SnapshotManifest 在 Phase 7 管理。

    Args:
        input_tables: Contract 中的输入表列表

    Returns:
        SparkReadStep 列表——每个输入表一个
    """
    steps: list[SparkReadStep] = []
    for it in input_tables:
        steps.append(
            SparkReadStep(
                alias=it.table_ref,
                source_name=it.source_table,
                input_key=it.table_ref,
                required_columns=[],  # Phase 5 暂不填充，Phase 7 从 Contract input_columns 填充
                estimated_row_count=it.estimated_row_count,
            )
        )
    return steps


def _map_filters(
    filters: list,
    unsupported: list[UnsupportedPattern],
) -> list[SparkFilterStep]:
    """将 Contract 的 filters 映射为 FilterStep 列表。

    每个 ContractPredicate 映射为一个 SparkFilterStep。
    结构化三元组 (left, operator, right) 直传。

    Args:
        filters: ContractPredicate 列表
        unsupported: 累积的 UnsupportedPattern 列表

    Returns:
        SparkFilterStep 列表
    """
    steps: list[SparkFilterStep] = []
    for i, f in enumerate(filters):
        # 操作符必须在白名单内
        operator = f.operator.upper()
        if operator not in _VALID_FILTER_OPERATORS:
            unsupported.append(
                UnsupportedPattern(
                    pattern_id=f"unsup_filter_{i}",
                    contract_field=f"filters[{i}].operator",
                    reason=f"不支持的操作符：{f.operator}",
                    suggested_workaround="使用 GT / EQ / AND / IN / IS_NULL 等标准操作符",
                )
            )
            continue

        # 推断 input_alias——从 left 操作数中提取表别名
        input_alias = _extract_table_alias(f.left)

        steps.append(
            SparkFilterStep(
                input_alias=input_alias,
                operator=operator,
                left=f.left,
                right=f.right,
            )
        )
    return steps


def _map_joins(
    joins: list[ContractJoin],
    unsupported: list[UnsupportedPattern],
) -> list[SparkJoinStep]:
    """将 Contract 的 join_relationships 映射为 JoinStep 列表。

    每个 ContractJoin 映射为一个 SparkJoinStep。
    WEAK/NONE 等级的 Join 不会出现在 Contract 中（已在 Phase 1B 被拒绝）。

    Args:
        joins: ContractJoin 列表
        unsupported: 累积的 UnsupportedPattern 列表

    Returns:
        SparkJoinStep 列表
    """
    steps: list[SparkJoinStep] = []
    for i, j in enumerate(joins):
        # Join 类型必须在白名单内
        join_type = _JOIN_TYPE_MAP.get(j.join_type)
        if join_type is None:
            unsupported.append(
                UnsupportedPattern(
                    pattern_id=f"unsup_join_{i}",
                    contract_field=f"join_relationships[{i}].join_type",
                    reason=f"不支持的 Join 类型：{j.join_type}",
                    suggested_workaround="使用 INNER / LEFT / RIGHT / FULL",
                )
            )
            continue

        steps.append(
            SparkJoinStep(
                left_alias=j.left_table,
                right_alias=j.right_table,
                left_key=j.left_key,
                right_key=j.right_key,
                join_type=join_type,
                evidence_chain=j.evidence_chain,
            )
        )
    return steps


def _map_aggregations(
    aggregations: list[ContractAggregation],
    grouping_keys: list[str],
    unsupported: list[UnsupportedPattern],
    gaps: list[ContractGap],
) -> list[SparkAggregateStep] | ContractGap | UnsupportedPattern:
    """将 Contract 的 aggregations 和 grouping_keys 映射为 AggregateStep。

    聚合本质上是一个步骤——所有 metrics 合并到一个 SparkAggregateStep 中。
    若没有聚合，返回空列表（不是错误——明细查询无聚合）。

    Args:
        aggregations: ContractAggregation 列表
        grouping_keys: 分组键列表
        unsupported: 累积的 UnsupportedPattern 列表
        gaps: 累积的 ContractGap 列表

    Returns:
        SparkAggregateStep 列表（0 或 1 个），或 ContractGap/UnsupportedPattern
    """
    if not aggregations:
        return []

    # 检查聚合函数是否在白名单内
    unknown_funcs: list[str] = []
    for a in aggregations:
        if a.function.upper() not in _AGG_FUNCTION_MAP:
            unknown_funcs.append(a.function)

    if unknown_funcs:
        unsupported.append(
            UnsupportedPattern(
                pattern_id="unsup_agg_func",
                contract_field="aggregations",
                reason=f"不支持的聚合函数：{', '.join(unknown_funcs)}",
                suggested_workaround="使用 COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT",
            )
        )
        return unsupported[-1]

    # 检查 grouping_keys 是否为空（COUNT(*) 可以不用分组键，但 Phase 5 WARN）
    if not grouping_keys:
        gaps.append(
            ContractGap(
                gap_id="gap_agg_no_group",
                contract_field="grouping_keys",
                missing_info="聚合操作未指定分组键——结果将聚合为单行",
                severity="WARN",
            )
        )

    # 推断 input_alias——从第一个聚合的 input_column 中提取
    input_alias = _infer_input_alias_from_aggregations(aggregations)

    metrics = [
        SparkAggregateSpec(
            function=_AGG_FUNCTION_MAP[a.function.upper()],
            input_column=a.input_column,
            alias=a.alias,
        )
        for a in aggregations
    ]

    return [
        SparkAggregateStep(
            input_alias=input_alias,
            group_keys=grouping_keys,
            metrics=metrics,
        )
    ]


def _map_output_columns(
    output_columns: list[ContractOutputColumn],
    gaps: list[ContractGap],
) -> list[SparkProjectStep] | ContractGap:
    """将 Contract 的 output_columns 映射为 ProjectStep。

    所有输出列合并为一个投影步骤。

    Args:
        output_columns: ContractOutputColumn 列表
        gaps: 累积的 ContractGap 列表

    Returns:
        SparkProjectStep 列表（0 或 1 个），或 ContractGap
    """
    if not output_columns:
        gap = ContractGap(
            gap_id="gap_no_output",
            contract_field="output_columns",
            missing_info="Contract 不含任何输出列",
            severity="BLOCKING",
        )
        gaps.append(gap)
        return gap

    # 推断 input_alias——所有输出列的来源表取并集
    input_alias = _infer_input_alias_from_columns(output_columns)

    columns = [
        SparkProjectColumn(
            column_name=oc.column_name,
            alias=oc.alias,
        )
        for oc in output_columns
    ]

    return [
        SparkProjectStep(
            input_alias=input_alias,
            columns=columns,
        )
    ]


def _map_case_when(
    case_when_labels: list[CaseWhenLabelSpec],
) -> list[SparkCaseWhenStep]:
    """将 Contract 的 case_when_labels 映射为 CaseWhenStep 列表。

    每个 CaseWhenLabelSpec 映射为一个 SparkCaseWhenStep。

    Args:
        case_when_labels: CaseWhenLabelSpec 列表

    Returns:
        SparkCaseWhenStep 列表
    """
    steps: list[SparkCaseWhenStep] = []
    for cwl in case_when_labels:
        branches = [SparkCaseWhenBranch(label=label) for label in cwl.labels]
        steps.append(
            SparkCaseWhenStep(
                input_alias="",  # 由 statement_id 推断，Phase 5 暂空
                output_alias=cwl.output_alias,
                branches=branches,
                else_value=cwl.else_label,
            )
        )
    return steps


def _map_windows(
    window_specs: list[WindowSpecSummary],
    unsupported: list[UnsupportedPattern],
) -> list[SparkWindowStep] | UnsupportedPattern:
    """将 Contract 的 window_specs 映射为 WindowStep。

    所有窗口表达式合并为一个步骤。

    Args:
        window_specs: WindowSpecSummary 列表
        unsupported: 累积的 UnsupportedPattern 列表

    Returns:
        SparkWindowStep 列表（0 或 1 个），或 UnsupportedPattern
    """
    if not window_specs:
        return []

    # 检查窗口函数是否在白名单内
    unknown_funcs: list[str] = []
    for ws in window_specs:
        if ws.function.upper() not in _WINDOW_FUNCTION_MAP:
            unknown_funcs.append(ws.function)

    if unknown_funcs:
        unsupported.append(
            UnsupportedPattern(
                pattern_id="unsup_window_func",
                contract_field="window_specs",
                reason=f"不支持的窗口函数：{', '.join(unknown_funcs)}",
                suggested_workaround="使用 ROW_NUMBER / RANK / LAG / LEAD / SUM_OVER / AVG_OVER / COUNT_OVER",
            )
        )
        return unsupported[-1]

    # 从第一个 window_spec 推断 input_alias
    input_alias = ""
    if window_specs and window_specs[0].statement_id:
        input_alias = window_specs[0].statement_id

    expressions = [
        SparkWindowExpr(
            function=_WINDOW_FUNCTION_MAP[ws.function.upper()],
            alias=ws.alias,
            partition_by=ws.partition_by,
            order_by=ws.order_by,
        )
        for ws in window_specs
    ]

    return [
        SparkWindowStep(
            input_alias=input_alias,
            expressions=expressions,
        )
    ]


def _map_sort(
    sort_spec: list[ContractSort] | None,
) -> list[SparkSortStep]:
    """将 Contract 的 sort_spec 映射为 SortStep。

    Args:
        sort_spec: ContractSort 列表或 None

    Returns:
        SparkSortStep 列表（0 或 1 个）
    """
    if not sort_spec:
        return []

    order_by = [
        SparkSortSpec(
            column=s.column,
            direction=_SORT_DIRECTION_MAP.get(s.direction, SparkSortDirection.ASC),
        )
        for s in sort_spec
    ]

    return [
        SparkSortStep(
            input_alias="",  # Phase 5 暂空，Phase 6 编译时填充
            order_by=order_by,
        )
    ]


def _map_limit(
    limit_spec: ContractLimit | None,
) -> list[SparkLimitStep]:
    """将 Contract 的 limit_spec 映射为 LimitStep。

    Args:
        limit_spec: ContractLimit 或 None

    Returns:
        SparkLimitStep 列表（0 或 1 个）
    """
    if limit_spec is None:
        return []

    return [
        SparkLimitStep(
            input_alias="",  # Phase 5 暂空
            limit=limit_spec.limit,
            offset=limit_spec.offset,
        )
    ]


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════

# 过滤操作符白名单
_VALID_FILTER_OPERATORS: set[str] = {
    "GT", "GTE", "LT", "LTE", "EQ", "NEQ",
    "AND", "OR", "NOT",
    "IN", "NOT_IN",
    "IS_NULL", "IS_NOT_NULL",
    "BETWEEN",
    "LIKE",
}


def _extract_table_alias(operand_str: str) -> str:
    """从操作数字符串中提取表别名。

    例如："od.user_id" → "od"，"user_id" → ""。

    Args:
        operand_str: 操作数字符串表示（如 "od.user_id"）

    Returns:
        表别名，或空字符串
    """
    if "." in operand_str:
        return operand_str.split(".")[0]
    return ""


def _infer_input_alias_from_aggregations(
    aggregations: list[ContractAggregation],
) -> str:
    """从聚合列表中推断 input_alias。

    取第一个非空的 input_column 的表别名部分。

    Args:
        aggregations: ContractAggregation 列表

    Returns:
        表别名，或空字符串
    """
    for a in aggregations:
        if a.input_column and "." in a.input_column:
            return a.input_column.split(".")[0]
    return ""


def _infer_input_alias_from_columns(
    columns: list[ContractOutputColumn],
) -> str:
    """从输出列列表中推断 input_alias。

    取第一个含表别名的 column_name。

    Args:
        columns: ContractOutputColumn 列表

    Returns:
        表别名，或空字符串
    """
    for oc in columns:
        if "." in oc.column_name:
            return oc.column_name.split(".")[0]
    return ""
