"""Phase 5 PlanEquivalence 规则——SqlBuildPlan step 与 SparkPlan step 的结构等价判定。

这是在 Phase 5 定义、Phase 7 PlanComparator 消费的规则集。
每条规则对比一种 step 类型，判定两个 IR 节点的业务语义是否等价。

规则设计原则：
- 确定性：相同输入 → 相同 verdict
- 字段归一化：column_name 对比使用归一化名（去表别名前缀、驼峰转下划线）
- 不比较 SQL 文本或 Spark 代码文本——只比较结构化 IR
- UNSUPPORTED_COMPARISON 表示该 step 类型暂不支持等价对比（如 SubqueryStep）
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel


class EquivalenceVerdict(str, Enum):
    """单步等价性判定结果。"""

    EQUIVALENT = "EQUIVALENT"               # 两者在业务语义上等价
    NOT_EQUIVALENT = "NOT_EQUIVALENT"       # 两者在业务语义上不等价
    UNSUPPORTED_COMPARISON = "UNSUPPORTED"  # 该 step 类型暂不支持等价对比


class StepEquivalenceResult(StrictModel):
    """单步类型对比结果。"""

    step_type: str  # step 类型名（如 "scan" / "filter"）
    verdict: EquivalenceVerdict  # 判定结果
    sql_count: int = 0  # SQL 侧该类型 step 数量
    spark_count: int = 0  # Spark 侧该类型 step 数量
    detail: str = ""  # 差异描述（NOT_EQUIVALENT 时非空）


class PlanEquivalenceResult(StrictModel):
    """两个完整 Plan 的结构等价对比结果。

    Phase 7 PlanComparator 消费此结果做 Go/No-Go 判定。
    """

    sql_plan_hash: str  # SQL 侧 SqlBuildPlan hash（或 SqlProgram hash）
    spark_plan_hash: str  # Spark 侧 SparkPlan hash
    step_results: list[StepEquivalenceResult] = Field(default_factory=list)
    overall_verdict: EquivalenceVerdict = EquivalenceVerdict.UNSUPPORTED_COMPARISON
    unsupported_types: list[str] = Field(default_factory=list)  # 两侧中不支持对比的 step 类型
    extra_sql_types: list[str] = Field(default_factory=list)     # SQL 侧有但 Spark 侧无的 step 类型
    extra_spark_types: list[str] = Field(default_factory=list)   # Spark 侧有但 SQL 侧无的 step 类型


# ════════════════════════════════════════════
# 字段名归一化
# ════════════════════════════════════════════


def normalize_field_name(name: str) -> str:
    """将字段名归一化以进行跨 IR 对比。

    规则：
    1. 去除表别名前缀（"od.user_id" → "user_id"）
    2. 大写统一为小写（归一化后 SQL/Spark 侧字段引用不区分大小写）
    3. 去除首尾空格

    Args:
        name: 原始字段名（可能含表别名前缀）

    Returns:
        归一化后的字段名
    """
    # 去除表别名前缀
    if "." in name:
        name = name.split(".")[-1]
    # 标准化空格和大小写
    return name.strip().lower()


# ════════════════════════════════════════════
# 单步对比函数
# ════════════════════════════════════════════


def _extract_column_name(col: Any) -> str:
    """统一提取列名——兼容 ColumnRef dict（SQL 侧）和纯字符串（Spark 侧）。

    ColumnRef dict 优先取 normalized_name，其次 column_name。
    纯字符串直接归一化。
    提取失败返回空字符串。
    """
    if isinstance(col, dict):
        name = col.get("normalized_name") or col.get("column_name", "")
        return normalize_field_name(str(name)) if name else ""
    return normalize_field_name(str(col))


def compare_scan_steps(
    sql_scans: list[dict[str, Any]],
    spark_reads: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL ScanStep 与 Spark ReadStep 的结构等价性。

    等价条件：
    1. 数量相同
    2. 每个表的 source_table / alias 对应（归一化对比）

    Args:
        sql_scans: SQL 侧 ScanStep 的 model_dump 列表
        spark_reads: Spark 侧 SparkReadStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_scans)
    spark_count = len(spark_reads)

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="scan",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"输入表数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 收集并排序表别名
    sql_aliases = sorted([
        normalize_field_name(s.get("table_ref", ""))
        for s in sql_scans
    ])
    spark_aliases = sorted([
        normalize_field_name(r.get("alias", ""))
        for r in spark_reads
    ])

    if sql_aliases != spark_aliases:
        return StepEquivalenceResult(
            step_type="scan",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"输入表别名不一致：SQL 侧 {sql_aliases}，Spark 侧 {spark_aliases}",
        )

    # ── 按 alias 分组收集列集合——全局 set 会丢失多表同名列信息 ──
    def _collect_scan_columns(
        scans: list[dict[str, Any]],
        alias_key: str,
        cols_key: str,
    ) -> dict[str, set[str]]:
        """按 alias 分组收集列集合。"""
        result: dict[str, set[str]] = {}
        for s in scans:
            alias = normalize_field_name(s.get(alias_key, ""))
            cols: set[str] = set()
            for c in (s.get(cols_key, []) or []):
                name = _extract_column_name(c)
                if name:
                    cols.add(name)
            if cols:
                result[alias] = cols
        return result

    sql_cols_by_alias = _collect_scan_columns(
        sql_scans, "table_ref", "required_columns",
    )
    spark_cols_by_alias = _collect_scan_columns(
        spark_reads, "alias", "required_columns",
    )

    # 仅在两侧共有的 alias 上对比（单侧有列集合不构成差异）
    common_aliases = set(sql_cols_by_alias) & set(spark_cols_by_alias)
    for alias in sorted(common_aliases):
        if sql_cols_by_alias[alias] != spark_cols_by_alias[alias]:
            only_sql = sql_cols_by_alias[alias] - spark_cols_by_alias[alias]
            only_spark = spark_cols_by_alias[alias] - sql_cols_by_alias[alias]
            return StepEquivalenceResult(
                step_type="scan",
                verdict=EquivalenceVerdict.NOT_EQUIVALENT,
                sql_count=sql_count,
                spark_count=spark_count,
                detail=(
                    f"表 '{alias}' 读取列集合不一致："
                    f"仅在 SQL 侧 {only_sql or '无'}，"
                    f"仅在 Spark 侧 {only_spark or '无'}"
                ),
            )

    return StepEquivalenceResult(
        step_type="scan",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_filter_steps(
    sql_filters: list[dict[str, Any]],
    spark_filters: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL FilterStep 与 Spark FilterStep 的结构等价性。

    等价条件：
    1. 数量相同
    2. 每个过滤条件的 (left, operator, right) 三元组等价（归一化后）

    Args:
        sql_filters: SQL 侧 FilterStep.predicates 的 model_dump 列表
        spark_filters: Spark 侧 SparkFilterStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_filters)
    spark_count = len(spark_filters)

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="filter",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"过滤条件数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 归一化并排序
    sql_normalized = sorted([
        (
            normalize_field_name(f.get("left", "")),
            f.get("operator", "").upper(),
            normalize_field_name(f.get("right", "")),
        )
        for f in sql_filters
    ])
    spark_normalized = sorted([
        (
            normalize_field_name(f.get("left", "")),
            f.get("operator", "").upper(),
            normalize_field_name(f.get("right", "")),
        )
        for f in spark_filters
    ])

    if sql_normalized != spark_normalized:
        return StepEquivalenceResult(
            step_type="filter",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"过滤条件不一致：SQL 侧 {sql_normalized}，Spark 侧 {spark_normalized}",
        )

    return StepEquivalenceResult(
        step_type="filter",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_join_steps(
    sql_joins: list[dict[str, Any]],
    spark_joins: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL JoinStep 与 Spark JoinStep 的结构等价性。

    等价条件：
    1. 数量相同
    2. 每个 Join 的 (left_table, right_table, left_key, right_key, join_type) 等价（归一化后）

    Args:
        sql_joins: SQL 侧 JoinStep 的 model_dump 列表
        spark_joins: Spark 侧 SparkJoinStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_joins)
    spark_count = len(spark_joins)

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="join",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"Join 数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 归一化并排序
    sql_normalized = sorted([
        (
            normalize_field_name(j.get("left_table_ref", "")),
            normalize_field_name(j.get("right_table_ref", "")),
            normalize_field_name(j.get("left_key", "")),
            normalize_field_name(j.get("right_key", "")),
            (j.get("join_type", "") or "").upper(),
        )
        for j in sql_joins
    ])
    spark_normalized = sorted([
        (
            normalize_field_name(j.get("left_alias", "")),
            normalize_field_name(j.get("right_alias", "")),
            normalize_field_name(j.get("left_key", "")),
            normalize_field_name(j.get("right_key", "")),
            (j.get("join_type", "") or "").upper(),
        )
        for j in spark_joins
    ])

    if sql_normalized != spark_normalized:
        return StepEquivalenceResult(
            step_type="join",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"Join 规格不一致——"
                f"SQL 侧: {sql_normalized}，"
                f"Spark 侧: {spark_normalized}"
            ),
        )

    return StepEquivalenceResult(
        step_type="join",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_aggregate_steps(
    sql_aggs: list[dict[str, Any]],
    spark_aggs: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL AggregateStep 与 Spark AggregateStep 的结构等价性。

    等价条件：
    1. 数量相同（通常为 1）
    2. group_keys 集合一致（归一化后）
    3. metrics 数量一致，每个 metric 的 (function, input_column, alias) 等价

    Args:
        sql_aggs: SQL 侧 AggregateStep 的 model_dump 列表
        spark_aggs: Spark 侧 SparkAggregateStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_aggs)
    spark_count = len(spark_aggs)

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"聚合步骤数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    sql_agg = sql_aggs[0]
    spark_agg = spark_aggs[0]

    # 对比分组键——去重后比较（SQL 侧 _normalize_dag_steps 已去重，Spark 侧 Mapper 可能含重复）
    sql_groups = sorted(set(normalize_field_name(g) for g in sql_agg.get("group_keys", [])))
    spark_groups = sorted(set(normalize_field_name(g) for g in spark_agg.get("group_keys", [])))

    if sql_groups != spark_groups:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"分组键不一致：SQL 侧 {sql_groups}，Spark 侧 {spark_groups}",
        )

    # 对比聚合指标
    sql_metrics = sql_agg.get("metrics", [])
    spark_metrics = spark_agg.get("metrics", [])

    if len(sql_metrics) != len(spark_metrics):
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"聚合指标数量不一致：SQL 侧 {len(sql_metrics)} 个，Spark 侧 {len(spark_metrics)} 个",
        )

    sql_metric_specs = sorted([
        (
            m.get("function", "").upper(),
            normalize_field_name(m.get("input_column", "") or ""),
            normalize_field_name(m.get("alias", "")),
        )
        for m in sql_metrics
    ])
    spark_metric_specs = sorted([
        (
            str(m.get("function", "")).upper(),
            normalize_field_name(m.get("input_column", "") or ""),
            normalize_field_name(m.get("alias", "")),
        )
        for m in spark_metrics
    ])

    if sql_metric_specs != spark_metric_specs:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"聚合指标规格不一致——"
                f"SQL 侧: {sql_metric_specs}，"
                f"Spark 侧: {spark_metric_specs}"
            ),
        )

    return StepEquivalenceResult(
        step_type="aggregate",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_project_steps(
    sql_projects: list[dict[str, Any]],
    spark_projects: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL ProjectStep 与 Spark ProjectStep 的结构等价性。

    等价条件：输出列名和别名集合一致（归一化后）。

    Args:
        sql_projects: SQL 侧 ProjectStep 的 model_dump 列表
        spark_projects: Spark 侧 SparkProjectStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_projects)
    spark_count = len(spark_projects)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="project",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    # 收集所有输出列（可能有多个 ProjectStep）
    sql_cols: set[tuple[str, str]] = set()
    for p in sql_projects:
        for col in p.get("columns", []) or []:
            sql_cols.add((
                normalize_field_name(col.get("column_name", "")),
                normalize_field_name(col.get("alias", "")),
            ))

    spark_cols: set[tuple[str, str]] = set()
    for p in spark_projects:
        for col in p.get("columns", []) or []:
            spark_cols.add((
                normalize_field_name(col.get("column_name", "")),
                normalize_field_name(col.get("alias", "")),
            ))

    if sql_cols != spark_cols:
        return StepEquivalenceResult(
            step_type="project",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"输出列不一致——仅在 SQL 侧：{sql_cols - spark_cols}，"
                f"仅在 Spark 侧：{spark_cols - sql_cols}"
            ),
        )

    return StepEquivalenceResult(
        step_type="project",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_case_when_steps(
    sql_case_whens: list[dict[str, Any]],
    spark_case_whens: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL CaseWhenStep 与 Spark CaseWhenStep 的结构等价性。

    等价条件：
    1. 数量相同
    2. 每个 CASE WHEN 的 output_alias、branch_count、labels 集合一致

    Args:
        sql_case_whens: SQL 侧 CaseWhenStep 的 model_dump 列表
        spark_case_whens: Spark 侧 SparkCaseWhenStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_case_whens)
    spark_count = len(spark_case_whens)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="case_when",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="case_when",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"CASE WHEN 数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 对比每个 CASE WHEN
    for i, (sql_cw, spark_cw) in enumerate(zip(sql_case_whens, spark_case_whens)):
        sql_labels = sorted([normalize_field_name(label) for label in (sql_cw.get("labels", []) or [])])
        spark_labels = sorted([
            normalize_field_name(b.get("label", ""))
            for b in (spark_cw.get("branches", []) or [])
        ])

        if sql_labels != spark_labels:
            return StepEquivalenceResult(
                step_type="case_when",
                verdict=EquivalenceVerdict.NOT_EQUIVALENT,
                sql_count=sql_count,
                spark_count=spark_count,
                detail=(
                    f"CASE WHEN[{i}] 标签值不一致："
                    f"SQL 侧 {sql_labels}，Spark 侧 {spark_labels}"
                ),
            )

        sql_else = normalize_field_name(sql_cw.get("default_value", "") or "")
        spark_else = normalize_field_name(spark_cw.get("else_value", "") or "")
        if sql_else != spark_else:
            return StepEquivalenceResult(
                step_type="case_when",
                verdict=EquivalenceVerdict.NOT_EQUIVALENT,
                sql_count=sql_count,
                spark_count=spark_count,
                detail=f"CASE WHEN[{i}] ELSE 值不一致：SQL 侧 '{sql_else}'，Spark 侧 '{spark_else}'",
            )

    return StepEquivalenceResult(
        step_type="case_when",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_window_steps(
    sql_windows: list[dict[str, Any]],
    spark_windows: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL WindowStep 与 Spark WindowStep 的结构等价性。

    等价条件：
    1. 数量相同（通常为 1）
    2. 每个窗口表达式的 (function, alias, partition_by, order_by) 等价

    Args:
        sql_windows: SQL 侧 WindowStep 的 model_dump 列表
        spark_windows: Spark 侧 SparkWindowStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_windows)
    spark_count = len(spark_windows)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"窗口步骤数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 收集所有窗口表达式（跨多个 WindowStep 聚合）
    sql_exprs: set[tuple[str, ...]] = set()
    for w in sql_windows:
        for expr in w.get("window_exprs", []) or w.get("expressions", []):
            func = expr.get("function", "").upper()
            alias = normalize_field_name(expr.get("alias", ""))
            partition = tuple(sorted([
                normalize_field_name(p) for p in (expr.get("partition_by", []) or [])
            ]))
            order = tuple(sorted([
                normalize_field_name(o) for o in (expr.get("order_by", []) or [])
            ]))
            sql_exprs.add((func, alias, partition, order))

    spark_exprs: set[tuple[str, ...]] = set()
    for w in spark_windows:
        for expr in w.get("expressions", []):
            func = str(expr.get("function", "")).upper()
            alias = normalize_field_name(expr.get("alias", ""))
            partition = tuple(sorted([
                normalize_field_name(p) for p in (expr.get("partition_by", []) or [])
            ]))
            order = tuple(sorted([
                normalize_field_name(o) for o in (expr.get("order_by", []) or [])
            ]))
            spark_exprs.add((func, alias, partition, order))

    if sql_exprs != spark_exprs:
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"窗口表达式不一致——仅在 SQL 侧：{sql_exprs - spark_exprs}，"
                f"仅在 Spark 侧：{spark_exprs - sql_exprs}"
            ),
        )

    return StepEquivalenceResult(
        step_type="window",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_sort_steps(
    sql_sorts: list[dict[str, Any]],
    spark_sorts: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL SortStep 与 Spark SortStep 的结构等价性。

    等价条件：排序规格一致。

    Args:
        sql_sorts: SQL 侧 SortStep 的 model_dump 列表
        spark_sorts: Spark 侧 SparkSortStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_sorts)
    spark_count = len(spark_sorts)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="sort",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="sort",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"排序步骤数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 收集所有排序规格
    sql_keys: list[tuple[str, str, str]] = []
    for s in sql_sorts:
        for item in s.get("order_by", []) or []:
            sql_keys.append((
                normalize_field_name(item.get("column", "")),
                (item.get("direction", "asc") or "asc").upper(),
                (item.get("null_order", "last") or "last").upper(),
            ))

    spark_keys: list[tuple[str, str, str]] = []
    for s in spark_sorts:
        for item in s.get("order_by", []) or []:
            spark_keys.append((
                normalize_field_name(item.get("column", "")),
                (item.get("direction", "asc") or "asc").upper(),
                "LAST",  # SparkSortSpec 无 null_order 字段。Spark SortOrder 默认：ASC→NULLS LAST，DESC→NULLS FIRST。此处容缺默认 "LAST"（大写，与 SQL .upper() 一致），仅当 SQL 显式 NULLS FIRST 时触发 NOT_EQUIVALENT
            ))

    if sql_keys != spark_keys:
        return StepEquivalenceResult(
            step_type="sort",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"排序规格不一致：SQL 侧 {sql_keys}，Spark 侧 {spark_keys}",
        )

    return StepEquivalenceResult(
        step_type="sort",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


def compare_limit_steps(
    sql_limits: list[dict[str, Any]],
    spark_limits: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL LimitStep 与 Spark LimitStep 的结构等价性。

    等价条件：limit 值和 offset 值一致。

    Args:
        sql_limits: SQL 侧 LimitStep 的 model_dump 列表
        spark_limits: Spark 侧 SparkLimitStep 的 model_dump 列表

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_limits)
    spark_count = len(spark_limits)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="limit",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    # SQL 侧可能有多个 LimitStep（多语句），取最终语句的
    sql_limit_val = sql_limits[-1].get("limit", 0) if sql_limits else None
    spark_limit_val = spark_limits[-1].get("limit", 0) if spark_limits else None

    sql_offset = sql_limits[-1].get("offset") if sql_limits else None
    spark_offset = spark_limits[-1].get("offset") if spark_limits else None

    if sql_limit_val != spark_limit_val or sql_offset != spark_offset:
        return StepEquivalenceResult(
            step_type="limit",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"行限制不一致：SQL 侧 LIMIT {sql_limit_val} OFFSET {sql_offset}，"
                f"Spark 侧 LIMIT {spark_limit_val} OFFSET {spark_offset}"
            ),
        )

    return StepEquivalenceResult(
        step_type="limit",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )


# ════════════════════════════════════════════
# 完整 Plan 对比入口
# ════════════════════════════════════════════

# 所有支持的 step 对比函数注册表——key 为规范化后的类型名
_STEP_COMPARATORS: dict[str, Any] = {
    "scan": compare_scan_steps,    # SQL: scan → Spark: read
    "read": compare_scan_steps,    # SQL: scan → Spark: read（同一条规则）
    "filter": compare_filter_steps,
    "join": compare_join_steps,
    "aggregate": compare_aggregate_steps,
    "project": compare_project_steps,
    "case_when": compare_case_when_steps,
    "window": compare_window_steps,
    "sort": compare_sort_steps,
    "limit": compare_limit_steps,
}

# SQL step type → 规范化类型名映射（SQL 侧和 Spark 侧使用不同命名）
_SQL_TYPE_TO_NORMALIZED: dict[str, str] = {
    "scan": "scan",
    "read": "scan",       # Spark 侧的 read 等价于 SQL 侧的 scan
    "filter": "filter",
    "join": "join",
    "aggregate": "aggregate",
    "project": "project",
    "case_when": "case_when",
    "window": "window",
    "sort": "sort",
    "limit": "limit",
}

# 无等价对比规则的 step 类型。
# 与 PlanComparator._NOT_YET_COVERED_TYPES 的区别：
#   - 此集合：对比规则不存在（如 subquery——Spark 侧无对应类型，无法设计规则）
#   - _NOT_YET_COVERED_TYPES：规则已存在但本 Phase 未启用（如 Phase 7B 的 window）
_NO_EQUIVALENCE_RULE_TYPES: set[str] = {"subquery"}


def compare_plans(
    sql_steps: list[dict[str, Any]],
    spark_steps: list[dict[str, Any]],
    sql_plan_hash: str = "",
    spark_plan_hash: str = "",
) -> PlanEquivalenceResult:
    """对比 SQL 侧 SqlBuildPlan 与 Spark 侧 SparkPlan 的结构等价性。

    这是 Phase 7 PlanComparator 的入口函数。

    Args:
        sql_steps: SQL 侧所有 step 的 model_dump 列表（按类型分组后）
        spark_steps: Spark 侧所有 step 的 model_dump 列表（按类型分组后）
        sql_plan_hash: SQL 侧 plan 的 SHA-256（用于追踪）
        spark_plan_hash: Spark 侧 plan 的 SHA-256（用于追踪）

    Returns:
        PlanEquivalenceResult——包含每个 step 类型的对比结果和 overall_verdict
    """
    # 按类型分组——先归一化类型名再分组（SQL scan ↔ Spark read）
    sql_by_type: dict[str, list[dict[str, Any]]] = {}
    for s in sql_steps:
        stype = s.get("step_type", s.get("type", ""))
        if hasattr(stype, "value"):
            stype = stype.value
        # 归一化类型名
        stype = _SQL_TYPE_TO_NORMALIZED.get(stype, stype)
        sql_by_type.setdefault(stype, []).append(s)

    spark_by_type: dict[str, list[dict[str, Any]]] = {}
    for s in spark_steps:
        stype = s.get("step_type", "")
        if hasattr(stype, "value"):
            stype = stype.value
        # 归一化类型名
        stype = _SQL_TYPE_TO_NORMALIZED.get(stype, stype)
        spark_by_type.setdefault(stype, []).append(s)

    # 收集所有 step 类型
    all_types = set(sql_by_type.keys()) | set(spark_by_type.keys())

    step_results: list[StepEquivalenceResult] = []
    unsupported_types: list[str] = []
    extra_sql_types: list[str] = []
    extra_spark_types: list[str] = []

    for stype in sorted(all_types):
        # stype 已经在上面的分组逻辑中归一化过（scan/read 统一为 scan）
        sql_steps_of_type = sql_by_type.get(stype, [])
        spark_steps_of_type = spark_by_type.get(stype, [])

        if stype in _NO_EQUIVALENCE_RULE_TYPES:
            unsupported_types.append(stype)
            step_results.append(StepEquivalenceResult(
                step_type=stype,
                verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
                detail=f"'{stype}' 无等价对比规则（_NO_EQUIVALENCE_RULE_TYPES）",
            ))
            continue

        if stype not in _STEP_COMPARATORS:
            unsupported_types.append(stype)
            step_results.append(StepEquivalenceResult(
                step_type=stype,
                verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
                detail=f"未知 step 类型 '{stype}'——不在 _STEP_COMPARATORS 注册表中",
            ))
            continue

        if not sql_steps_of_type and spark_steps_of_type:
            extra_spark_types.append(stype)
            step_results.append(
                StepEquivalenceResult(
                    step_type=stype,
                    verdict=EquivalenceVerdict.NOT_EQUIVALENT,
                    sql_count=0,
                    spark_count=len(spark_steps_of_type),
                    detail=f"Spark 侧有 {stype} 但 SQL 侧无",
                )
            )
            continue

        if sql_steps_of_type and not spark_steps_of_type:
            extra_sql_types.append(stype)
            step_results.append(
                StepEquivalenceResult(
                    step_type=stype,
                    verdict=EquivalenceVerdict.NOT_EQUIVALENT,
                    sql_count=len(sql_steps_of_type),
                    spark_count=0,
                    detail=f"SQL 侧有 {stype} 但 Spark 侧无",
                )
            )
            continue

        # 执行对比——stype 已归一化
        comparator = _STEP_COMPARATORS[stype]
        result = comparator(sql_steps_of_type, spark_steps_of_type)
        step_results.append(result)

    # 计算 overall_verdict
    not_equivalent = [
        r for r in step_results
        if r.verdict == EquivalenceVerdict.NOT_EQUIVALENT
    ]

    if unsupported_types:
        overall = EquivalenceVerdict.UNSUPPORTED_COMPARISON
    elif not_equivalent or extra_sql_types or extra_spark_types:
        overall = EquivalenceVerdict.NOT_EQUIVALENT
    else:
        overall = EquivalenceVerdict.EQUIVALENT

    return PlanEquivalenceResult(
        sql_plan_hash=sql_plan_hash,
        spark_plan_hash=spark_plan_hash,
        step_results=step_results,
        overall_verdict=overall,
        unsupported_types=unsupported_types,
        extra_sql_types=extra_sql_types,
        extra_spark_types=extra_spark_types,
    )
