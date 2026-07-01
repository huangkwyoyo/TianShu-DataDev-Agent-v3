"""SpecEnricher——基于规则的指标/维度推断层（Fake 实现）+ LLM 推断层（Phase 4）。

位于 Parser 与 Builder 之间：
  Parser → SpecEnricher → Builder → Validator → Compiler → Executor → ...

职责边界（仅推断，不修改）：
- 从业务描述中推断程序员未显式声明的指标（MetricDecl）
- 推断窗口函数指标（InferredWindowMetric）
- 推断计算指标（InferredComputedMetric）
- 不推断 JOIN 关系——那是 RelationshipPlanner 的职责
- 不修改程序员已手写的 metrics——显式声明优先级最高

FakeSpecEnricher（Phase 1）：纯规则匹配，不调用 LLM。
SpecEnricher（Phase 4）：LLM 驱动，Prompt 嵌入 8 条硬约束。
"""

from __future__ import annotations

import json
import re
import time
from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    ComputeStep,
    EnrichedSpec,
    InferredComputedMetric,
    InferredWindowMetric,
    JoinDecl,
    JoinTypeEnum,
    MetricDecl,
    MetricFilterDecl,
    ParsedDeveloperSpec,
    SourceManifest,
)

if TYPE_CHECKING:
    from tianshu_datadev.llm.adapters.base import ProviderAdapter

# ════════════════════════════════════════════
# 规则推断——中文关键词 → 聚合函数映射
# ════════════════════════════════════════════

# 模式：(正则, 聚合类型, 是否需要 distinct)
_AGGREGATION_PATTERNS: list[tuple[re.Pattern, AggregationType, bool]] = [
    # 去重计数（优先匹配——"去重XX数"比"XX数"更具体）
    (re.compile(r"去重|独立(的)?|不重复(的)?"), AggregationType.COUNT_DISTINCT, False),
    # 平均——必须在 SUM 之前，否则"平均XX金额"中的"金额"会先匹配 SUM
    (re.compile(r"(平均|均值|人均|户均|日均|月均)"), AggregationType.AVG, False),
    # 最大值——必须在 SUM 之前
    (re.compile(r"(最大|最高|峰值|极大)"), AggregationType.MAX, False),
    # 最小值——必须在 SUM 之前
    (re.compile(r"(最小|最低|谷值|极小)"), AggregationType.MIN, False),
    # 求和
    (re.compile(r"(总|合[计记]|求和|累[计记]|金额|销售额|收入|支出|费用|成本)"), AggregationType.SUM, False),
    # 计数（最通用——放在最后，避免误匹配）
    (re.compile(r"(数|数量|个数|次数|笔数|条数|人次|PV|UV|访问量|浏览量)"), AggregationType.COUNT, False),
]

# 条件聚合关键词——"有XX的"、"含XX的"、"状态为XX的" 等
_CONDITIONAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"(有|含|带有|包含|具备)([^的，。,.\s]+)的"),
    re.compile(r"([^\s，。,]+)(状态|类型|标志|标签)(为|是|等于)([^\s，。,]+)"),
    re.compile(r"(满足|符合)([^\s，。,]+)(条件|要求)"),
    re.compile(r"([^\s，。,]+)(的)([^\s，。,]+)"),  # "有标准罚款的车牌数"
]

# 比率关键词
_RATIO_PATTERNS: list[re.Pattern] = [
    re.compile(r"(率|比|占比|比例|百分比|覆盖率|渗透率|转化率|合格率)"),
]

# 窗口/排名关键词
_WINDOW_PATTERNS: list[re.Pattern] = [
    re.compile(r"(排名|排行|名次|前\d+|TOP\s*\d+|top\s*\d+)"),
    re.compile(r"(累计|累加|累积)"),
    re.compile(r"(同比|环比|去年同期|上月同期)"),
]

# ════════════════════════════════════════════
# Description 解析——仅处理机械 SQL 签名，不碰自然语言
# ════════════════════════════════════════════
#
# 设计原则（防膨胀）：
# - 只匹配 SQL 函数语法：COUNT(*)、SUM(col)、COUNT(DISTINCT col) 等
# - 不解析中文条件（"仅含"、"状态为"等）→ 那是 LLM SpecEnricher 的职责
# - 正则只覆盖确定性、跨语言不变的 SQL 模式
# - 新增模式必须满足"无需理解语义即可匹配"的标准

# 聚合函数解析：FUNC(arg) 或 FUNC(DISTINCT arg)
_DESC_AGG_RE = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT|MEDIAN|STDDEV|PERCENTILE)"
    r"\s*\(\s*(DISTINCT\s+)?([^)]*)\s*\)",
    re.IGNORECASE,
)

# 窗口函数 OVER(...) 语法——纯机械匹配
# 捕获组: (1)函数名 (2)参数 (3)PARTITION BY 子句 (4)ORDER BY 子句
_DESC_WINDOW_RE = re.compile(
    r"\b(ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD|NTILE|SUM|AVG|COUNT)"
    r"\s*\(\s*([^)]*)\s*\)\s*OVER\s*\(\s*"
    r"(?:PARTITION\s+BY\s+([^)]+?))?"
    r"(?:\s*ORDER\s+BY\s+([^)]+?))?"
    r"\s*\)",
    re.IGNORECASE,
)

# 比率/表达式模式：identifier / identifier
_DESC_RATIO_RE = re.compile(
    r"\b(\w+)\s*/\s*(\w+)\b",
)

# 不支持函数——返回 None，标记低置信度
_UNSUPPORTED_FUNCTIONS = frozenset(
    {"MEDIAN", "STDDEV", "PERCENTILE", "PERCENTILE_CONT", "PERCENTILE_DISC"}
)


def _parse_description_to_metric(col) -> MetricDecl | None:
    """从 description 中提取 SQL 聚合函数签名（纯机械解析）。

    只处理在 description 中显式写出 SQL 函数的情况：
    - "COUNT(*)" → MetricDecl(COUNT, None)
    - "COUNT(DISTINCT plate_id)" → MetricDecl(COUNT_DISTINCT, plate_id)
    - "SUM(amount)" → MetricDecl(SUM, amount)
    - "MEDIAN(amount)" → None（不支持函数，走人工）

    不处理：
    - 中文条件过滤（"仅含 STANDARD"等）→ LLM SpecEnricher 负责
    - 无 SQL 签名的自然语言描述 → 返回 None，由上层兜底

    Args:
        col: OutputColumnDecl 实例

    Returns:
        MetricDecl 或 None
    """
    if not col.description:
        return None

    desc = col.description.strip()

    agg_match = _DESC_AGG_RE.search(desc)
    if not agg_match:
        return None

    func_name = agg_match.group(1).upper()
    has_distinct = agg_match.group(2) is not None and "DISTINCT" in agg_match.group(2).upper()
    arg = agg_match.group(3).strip() if agg_match.group(3) else ""

    # 不支持函数 → None
    if func_name in _UNSUPPORTED_FUNCTIONS:
        return None

    # 函数名 → AggregationType
    func_map = {
        "COUNT": AggregationType.COUNT,
        "SUM": AggregationType.SUM,
        "AVG": AggregationType.AVG,
        "MIN": AggregationType.MIN,
        "MAX": AggregationType.MAX,
        "COUNT_DISTINCT": AggregationType.COUNT_DISTINCT,
    }
    if has_distinct and func_name == "COUNT":
        agg_type = AggregationType.COUNT_DISTINCT
    else:
        agg_type = func_map.get(func_name)
        if agg_type is None:
            return None

    # input_column vs input_expression
    input_column = None
    input_expression = None
    if arg and arg != "*":
        # 包含运算符 → 表达式；否则 → 列名
        if any(op in arg for op in ("*", "/", "+", "-")):
            input_expression = arg
        else:
            input_column = arg

    return MetricDecl(
        metric_name=col.name,
        aggregation=agg_type,
        input_column=input_column,
        alias=col.name,
        filter=None,  # 不由 regex 推断——LLM 负责
        input_expression=input_expression,
        distinct=has_distinct and func_name != "COUNT",
    )


def _parse_description_to_computed(col) -> InferredComputedMetric | None:
    """从 description 解析计算指标（比率等）。

    如 "fined_plate_count / unique_plate_count，范围 [0, 1]"
    → InferredComputedMetric(expression="fined_plate_count / unique_plate_count", ...)

    Args:
        col: OutputColumnDecl 实例

    Returns:
        InferredComputedMetric 或 None
    """
    if not col.description:
        return None

    desc = col.description.strip()
    ratio_match = _DESC_RATIO_RE.search(desc)
    if not ratio_match:
        return None

    left = ratio_match.group(1)
    right = ratio_match.group(2)
    expression = f"{left} / {right}"

    return InferredComputedMetric(
        metric_name=col.name,
        expression=expression,
        depends_on=[left, right],
        alias=col.name,
        confidence="high",
    )


def _parse_description_to_window(col) -> InferredWindowMetric | None:
    """从 description 解析窗口指标。

    如 "ROW_NUMBER() OVER (PARTITION BY issue_date ORDER BY cnt DESC)"
    → InferredWindowMetric(window_function="ROW_NUMBER", ...)

    Args:
        col: OutputColumnDecl 实例

    Returns:
        InferredWindowMetric 或 None
    """
    if not col.description:
        return None

    desc = col.description.strip()
    win_match = _DESC_WINDOW_RE.search(desc)
    if not win_match:
        return None

    func_name = win_match.group(1).upper()
    input_col = win_match.group(2).strip() if win_match.group(2) else ""
    partition_raw = win_match.group(3).strip() if win_match.group(3) else ""
    order_raw = win_match.group(4).strip() if win_match.group(4) else ""

    partition_by = [p.strip() for p in partition_raw.split(",")] if partition_raw else []
    order_by = [o.strip() for o in order_raw.split(",")] if order_raw else []

    return InferredWindowMetric(
        metric_name=col.name,
        window_function=func_name,
        input_column=input_col,
        partition_by=partition_by,
        order_by=order_by,
        alias=col.name,
        confidence="high",
    )


def _find_matching_columns(
    keyword: str,
    manifest: SourceManifest,
    exclude_columns: set[str] | None = None,
    metric_name: str = "",
) -> list[str]:
    """在 manifest 的所有表中查找与关键词匹配的列名。

    使用 metric_name 作为辅助信号提高匹配精度——例如 "uv"（独立访客数）
    应优先匹配 user_id 而非 stat_date。

    Args:
        keyword: 中文关键词（如 "金额"）
        manifest: 源数据清单
        exclude_columns: 排除已使用的列名
        metric_name: 指标别名（如 "uv"），用于消歧义

    Returns:
        匹配的列名列表（按优先级排序，去重）
    """
    exclude = exclude_columns or set()
    scored: list[tuple[int, str]] = []  # (优先级分数, 列名)
    seen: set[str] = set()

    # 英文关键词映射——中文 → 常见英文列名
    cn_to_en: dict[str, list[str]] = {
        "金额": ["amount", "amt", "price", "fee", "money", "revenue", "sales"],
        "数量": ["quantity", "qty", "count", "num", "cnt"],
        "用户": ["user_id", "uid", "user", "member_id"],
        "订单": ["order_id", "oid", "order"],
        "时间": ["time", "date", "dt", "event_time", "create_time", "update_time"],
        "状态": ["status", "state", "sts"],
        "类型": ["type", "category", "kind"],
        "名称": ["name", "title", "label"],
        "车牌": ["plate_id", "plate_no", "license_plate"],
        "罚款": ["fine_amount", "fine_status", "penalty"],
        "日期": ["date", "dt", "stat_date", "ds"],
        "页面": ["page_id", "page_url", "page"],
        "事件": ["event_type", "event", "action"],
    }

    # 指标别名 → 优先列类型的启发式映射——用于消歧义
    metric_name_lower = metric_name.lower()
    alias_hints: dict[str, list[str]] = {
        "pv": ["event", "page", "id"],
        "uv": ["user_id", "uid", "user", "member_id"],
        "fined": ["plate_id", "fine_status", "fine_amount"],
        "plate": ["plate_id", "plate_no"],
        "amount": ["amount", "price", "fee"],
        "revenue": ["amount", "revenue", "sales"],
        "count": ["id", "order_id"],
    }

    # 从 metric_name 提取提示关键词
    hint_categories: list[str] = []
    for hint_key, hint_cols in alias_hints.items():
        if hint_key in metric_name_lower:
            hint_categories.extend(hint_cols)

    for table in manifest.tables:
        for col in table.columns:
            if col.column_name in seen or col.column_name in exclude:
                continue
            col_lower = col.column_name.lower()

            # 计算优先级分数：匹配 hint → 高分（10），仅匹配 cn_to_en → 低分（1）
            score = 0
            if hint_categories and col_lower in hint_categories:
                score = 10
            for en_keywords in cn_to_en.values():
                if col_lower in en_keywords:
                    score = max(score, 1)  # 至少 1 分
                    break

            if score > 0:
                seen.add(col.column_name)
                scored.append((score, col.column_name))

    # 按分数降序排列——高分列优先
    scored.sort(key=lambda x: x[0], reverse=True)
    return [col for _, col in scored]


def _infer_aggregation_type(
    description: str,
    metric_name: str,
) -> tuple[AggregationType, bool]:
    """从描述文本推断聚合类型。

    Args:
        description: 业务描述文本
        metric_name: 指标名（如 "fined_plate_count"）

    Returns:
        (AggregationType, needs_distinct)
    """
    # 合并描述和指标名进行匹配
    combined = f"{metric_name} {description}"

    for pattern, agg_type, needs_distinct in _AGGREGATION_PATTERNS:
        if pattern.search(combined):
            return agg_type, needs_distinct

    # ── 英文名称推断——中文关键词未命中时的兜底 ──
    name_lower = metric_name.lower()
    # "unique" / "distinct" / "dedup" → COUNT_DISTINCT
    if any(kw in name_lower for kw in ("unique", "distinct", "dedup", "去重")):
        return AggregationType.COUNT_DISTINCT, False
    # "avg" / "average" / "mean" → AVG
    if any(kw in name_lower for kw in ("avg", "average", "mean", "平均")):
        return AggregationType.AVG, False
    # "max" / "maximum" / "highest" → MAX
    if any(kw in name_lower for kw in ("max", "maximum", "highest", "最大", "最高")):
        return AggregationType.MAX, False
    # "min" / "minimum" / "lowest" → MIN
    if any(kw in name_lower for kw in ("min", "minimum", "lowest", "最小", "最低")):
        return AggregationType.MIN, False
    # "sum" / "total" / "amount" → SUM
    if any(kw in name_lower for kw in ("sum", "total", "amount", "revenue", "sales")):
        return AggregationType.SUM, False

    # 默认：COUNT
    return AggregationType.COUNT, False


def _infer_filter_condition(
    description: str,
) -> MetricFilterDecl | None:
    """从描述文本推断过滤条件。

    Args:
        description: 业务描述文本

    Returns:
        MetricFilterDecl 或 None（无法推断时）
    """
    for pattern in _CONDITIONAL_PATTERNS:
        match = pattern.search(description)
        if match:
            groups = match.groups()
            # 尝试从匹配组中提取列名和值
            # 这是一个启发式方法——实际 LLM 版本会更准确
            return None  # 规则推断无法可靠推断 filter 的具体列和值

    return None


class FakeSpecEnricher:
    """Phase 1 确定性指标推断器——基于规则匹配，不调用 LLM。

    行为：
    1. 检查 spec.metrics 是否已覆盖 output_columns 中的所有指标列
    2. 对缺失的指标列，尝试从业务描述中推断聚合函数
    3. 匹配 manifest 中的列名
    4. 产出 EnrichedSpec（inferred_metrics 填充推断结果）

    不修改原始 spec.metrics——程序员手写声明优先级最高。

    Phase 5 新增：跨粒度依赖检测——InferredComputedMetric.depends_on 引用
    不同粒度的指标时，自动生成 compute_steps + JoinDecl 拆分 DAG。
    """

    def _detect_cross_grain_dependency(
        self,
        spec: ParsedDeveloperSpec,
        inferred_computed: list[InferredComputedMetric],
        manifest: SourceManifest | None = None,
    ) -> tuple[list[ComputeStep], list[JoinDecl]]:
        """检测跨粒度 ComputedMetric → 生成 compute_steps + JoinDecl。

        当 InferredComputedMetric.depends_on 引用了一个不在当前 grain 的指标时，
        自动拆分 DAG：
        - Step A：分组聚合（输出 grain 的 GROUP BY）
        - Step B：全局聚合（无 GROUP BY——全局汇总）
        - Step C：source=[A, B]，跨粒度 Join + 计算比率

        检测启发式：
        - depends_on 中的指标不在已声明 metrics 中 → 需要独立的全局聚合步骤
        - 或 expression 含 "/" 且分母含 "total"/"global"/"overall" 关键词

        Args:
            spec: 已解析的 DeveloperSpec
            inferred_computed: SpecEnricher 推断的计算指标列表

        Returns:
            (compute_steps, join_decls)——空列表表示无跨粒度依赖
        """
        # grain 可为空列表（全局聚合），用 is None 而非 falsy 检查
        if not inferred_computed:
            return [], []

        grain = spec.output_spec.grain
        grain_key = "_".join(grain) if grain else "global"
        declared_aliases: set[str] = {m.alias for m in spec.metrics}

        steps: list[ComputeStep] = []
        joins: list[JoinDecl] = []

        for cm in inferred_computed:
            if not cm.depends_on or len(cm.depends_on) < 2:
                continue

            # 检查 depends_on 是否引用了未声明的指标（跨粒度信号）
            missing_deps = [
                d for d in cm.depends_on if d not in declared_aliases
            ]
            if not missing_deps:
                # 所有依赖都已声明——可能不是跨粒度场景
                continue

            # 为每个缺失的依赖创建全局聚合步骤（无 GROUP BY）
            global_step_names: list[str] = []
            for dep_alias in missing_deps:
                step_name = f"global_{dep_alias}"
                global_step_names.append(step_name)

                # ── 推断缺失 dep 的输入列和聚合类型 ──
                # 基础：从现有指标复刻（向后兼容）
                dep_input_col = None
                dep_agg_type = AggregationType.SUM
                for m in spec.metrics:
                    if m.input_column:
                        dep_input_col = m.input_column
                        dep_agg_type = m.aggregation
                        break

                # 增强：从 dep 名称推断更精确的聚合类型（如 "unique_xxx"→COUNT_DISTINCT）
                dep_inferred_type, dep_needs_distinct = _infer_aggregation_type(
                    dep_alias, dep_alias,
                )
                if dep_needs_distinct or dep_inferred_type == AggregationType.COUNT_DISTINCT:
                    dep_agg_type = AggregationType.COUNT_DISTINCT
                elif dep_inferred_type != AggregationType.COUNT:
                    # 非默认类型（如 AVG/SUM/MIN/MAX）→ 使用推断值
                    dep_agg_type = dep_inferred_type

                # manifest 兜底——仅在无现有指标时启用
                if dep_input_col is None and manifest:
                    matched = _find_matching_columns(
                        dep_alias, manifest,
                        metric_name=dep_alias,
                    )
                    dep_input_col = matched[0] if matched else None

                steps.append(ComputeStep(
                    step_name=step_name,
                    source="input",
                    group_by=[],  # 全局聚合——无 GROUP BY
                    metrics=[
                        MetricDecl(
                            metric_name=dep_alias,
                            aggregation=dep_agg_type,
                            input_column=dep_input_col,
                            alias=dep_alias,
                        ),
                    ],
                    output_alias=step_name,
                ))

            # Step A：分组聚合（按输出 grain）
            grouped_step_name = f"grouped_{grain_key}"
            grouped_metrics = [
                MetricDecl(
                    metric_name=m.metric_name,
                    aggregation=m.aggregation,
                    input_column=m.input_column,
                    alias=m.alias,
                )
                for m in spec.metrics
            ]
            steps.append(ComputeStep(
                step_name=grouped_step_name,
                source="input",
                group_by=list(grain),
                metrics=grouped_metrics,
                output_alias=grouped_step_name,
            ))

            # Step C：合流步骤——跨粒度 Join + 计算比率
            merge_sources = [grouped_step_name] + global_step_names
            merge_step_name = f"merged_{cm.alias}"

            # 收集上游依赖列作为 group_by（跨粒度 Join 后每行唯一）
            # 含 grain 列 + depends_on 中的所有列——确保 GROUP BY 合法
            merge_group_by = list(grain)
            for dep_alias in cm.depends_on:
                if dep_alias not in merge_group_by:
                    merge_group_by.append(dep_alias)

            # 合流步骤的指标——计算比率表达式（SUM of unique row = passthrough）
            merge_metrics = [
                MetricDecl(
                    metric_name=cm.alias,
                    aggregation=AggregationType.SUM,
                    input_expression=cm.expression,
                    alias=cm.alias,
                ),
            ]

            steps.append(ComputeStep(
                step_name=merge_step_name,
                source=merge_sources,
                group_by=merge_group_by,
                metrics=merge_metrics,
                output_alias=merge_step_name,
            ))

            # JoinDecl——跨粒度场景使用 CROSS JOIN（全局表仅一行）
            # 首个合并对：grouped + first global
            for gsn in global_step_names:
                joins.append(JoinDecl(
                    left_table=grouped_step_name,
                    right_table=gsn,
                    left_key="",  # CROSS JOIN——无等值键
                    right_key="",
                    join_type=JoinTypeEnum.INNER,  # Builder 将转为 CROSS
                ))

        return steps, joins

    def _detect_conditional_branch(
        self,
        spec: ParsedDeveloperSpec,
        inferred_metrics: list[MetricDecl],
    ) -> tuple[list[ComputeStep], list[JoinDecl]]:
        """检测条件分支语义——生成 compute_steps + CASE WHEN 合并步骤。

        检测启发式：
        1. spec 描述含条件关键词（"XX客户" + 不同行为模式）
        2. spec.metrics 含 variants（不同 filter 条件）→ 每 variant 一个分支
        3. output_columns 中有一个"标签"类列（如 value_level）→ 合并步骤用 CASE WHEN

        生成的 DAG 结构：
        - 分支步骤（每 variant 一个）：source="input"，按 variant.filter 过滤 + 聚合
        - 合并步骤：source=[branch_1, ..., branch_n]，CASE WHEN 选择结果
        - JoinDecl：分支间用 grain 键做 FULL OUTER JOIN

        Args:
            spec: 已解析的 DeveloperSpec
            inferred_metrics: SpecEnricher 推断的指标列表

        Returns:
            (compute_steps, join_decls)——空列表表示未检测到条件分支
        """
        from tianshu_datadev.developer_spec.models import CaseWhenBranchDecl, CaseWhenDecl

        if not spec.metrics:
            return [], []

        # 收集含 variants 的指标——这些是条件分支的信号
        branched_metrics = [m for m in spec.metrics if m.variants and len(m.variants) > 0]
        if not branched_metrics:
            return [], []

        grain = spec.output_spec.grain
        grain_key = "_".join(grain) if grain else "default"
        steps: list[ComputeStep] = []
        joins: list[JoinDecl] = []

        # 每个 variant 生成一个分支步骤
        branch_names: list[str] = []
        case_branches: list[CaseWhenBranchDecl] = []

        for bm in branched_metrics:
            for vi, variant in enumerate(bm.variants):
                branch_name = f"branch_{variant.alias}"
                branch_names.append(branch_name)

                # 构建分支的过滤指标——仅含基础聚合 + variant filter
                branch_metric = MetricDecl(
                    metric_name=variant.alias,
                    aggregation=bm.aggregation,
                    input_column=bm.input_column,
                    alias=variant.alias,
                    filter=variant.filter,
                    input_expression=bm.input_expression,
                    distinct=bm.distinct,
                )

                steps.append(ComputeStep(
                    step_name=branch_name,
                    source="input",
                    group_by=list(grain),
                    metrics=[branch_metric],
                    output_alias=branch_name,
                ))

                # CASE WHEN 分支——条件来自 variant filter
                if variant.filter:
                    case_branches.append(CaseWhenBranchDecl(
                        condition_column=variant.filter.column,
                        condition_operator=variant.filter.operator,
                        condition_value=variant.filter.value,
                        result_column=variant.alias,
                    ))

        if not branch_names or len(branch_names) < 2:
            return [], []

        # 合并步骤——source 为所有分支
        merge_name = "merge_conditional"
        case_when = CaseWhenDecl(
            branches=case_branches,
            else_value=None,  # 无 ELSE → NULL
            output_column=branched_metrics[0].alias if branched_metrics else "merged_value",
        )

        steps.append(ComputeStep(
            step_name=merge_name,
            source=branch_names,
            group_by=list(grain),
            metrics=[],  # CASE WHEN 替代聚合——不声明 metrics
            case_when=case_when,
            output_alias=merge_name,
        ))

        # JoinDecl——相邻分支间用 grain 键 FULL OUTER JOIN
        for i in range(len(branch_names) - 1):
            left_br = branch_names[i]
            right_br = branch_names[i + 1]
            # 使用首个 grain 列作为 Join 键
            join_key = grain[0] if grain else "id"
            joins.append(JoinDecl(
                left_table=left_br,
                right_table=right_br,
                left_key=join_key,
                right_key=join_key,
                join_type=JoinTypeEnum.LEFT,
            ))

        return steps, joins

    def enrich(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> EnrichedSpec:
        """执行规则推断，返回 EnrichedSpec。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单（含列名和类型信息）

        Returns:
            EnrichedSpec——original_spec + 推断结果
        """
        start_time = time.time()
        inferred_metrics: list[MetricDecl] = []
        inferred_window: list[InferredWindowMetric] = []
        inferred_computed: list[InferredComputedMetric] = []

        # 收集已声明的指标 alias 和使用的列
        declared_aliases: set[str] = {m.alias for m in spec.metrics}
        declared_columns: set[str] = set()
        for m in spec.metrics:
            if m.input_column:
                declared_columns.add(m.input_column)

        # 收集 output_columns 中的指标列（非维度列、非 grain 列）
        grain_set: set[str] = set(spec.output_spec.grain)
        output_metric_cols: list[OutputColumnDecl] = [
            c for c in spec.output_spec.columns
            if c.name not in grain_set and c.name not in declared_aliases
        ]

        # 对每个未声明的输出指标列，尝试推断
        from tianshu_datadev.developer_spec.models import OutputColumnDecl
        for col in output_metric_cols:
            col_name = col.name

            # 优先从 description 解析——结构化 DSL 比规则推断更可靠
            if col.description:
                parsed = _parse_description_to_metric(col)
                if parsed:
                    inferred_metrics.append(parsed)
                    continue

            # 兜底：从描述文本推断聚合类型
            agg_type, needs_distinct = _infer_aggregation_type(
                spec.description, col_name
            )

            # 尝试匹配 manifest 中的输入列——metric_name 用于消歧义
            matched_cols = _find_matching_columns(
                col_name, manifest, declared_columns, metric_name=col_name,
            )

            input_col = matched_cols[0] if matched_cols else None

            # 尝试推断过滤条件
            filter_cond = _infer_filter_condition(spec.description)

            inferred_metrics.append(
                MetricDecl(
                    metric_name=col_name,
                    aggregation=agg_type,
                    input_column=input_col,
                    alias=col_name,
                    filter=filter_cond,
                    input_expression=None,
                    distinct=needs_distinct,
                )
            )

        # 检测比率类指标——优先从 description 解析
        for col in output_metric_cols:
            if col.description:
                computed = _parse_description_to_computed(col)
                if computed:
                    inferred_computed.append(computed)
                    continue
            # 兜底：关键词匹配
            for pattern in _RATIO_PATTERNS:
                if pattern.search(col.name) or pattern.search(spec.description):
                    inferred_computed.append(
                        InferredComputedMetric(
                            metric_name=col.name,
                            expression="",
                            depends_on=[],
                            alias=col.name,
                            confidence="low",
                        )
                    )
                    break

        # 检测窗口/排名类指标——优先从 description 解析
        for col in output_metric_cols:
            if col.description:
                window = _parse_description_to_window(col)
                if window:
                    inferred_window.append(window)
        # 兜底：关键词匹配
        for pattern in _WINDOW_PATTERNS:
            if pattern.search(spec.description):
                break

        # ── Phase 5：跨粒度依赖检测 ──
        cross_grain_steps, cross_grain_joins = \
            self._detect_cross_grain_dependency(spec, inferred_computed, manifest)

        # ── Phase 6：条件分支检测 ──
        branch_steps, branch_joins = \
            self._detect_conditional_branch(spec, inferred_metrics)

        elapsed_ms = int((time.time() - start_time) * 1000)

        # ── 合并所有生成的 compute_steps + joins（跨粒度 + 条件分支）──
        all_generated_steps = list(cross_grain_steps) + list(branch_steps)
        all_generated_joins = list(cross_grain_joins) + list(branch_joins)

        metadata = {
            "source": "FakeSpecEnricher",
            "method": "rule_based",
            "inference_time_ms": elapsed_ms,
            "total_inferred": len(inferred_metrics)
            + len(inferred_window)
            + len(inferred_computed),
        }
        if all_generated_steps:
            metadata["generated_compute_steps"] = [
                s.model_dump() for s in all_generated_steps
            ]
        if all_generated_joins:
            metadata["generated_joins"] = [
                j.model_dump() for j in all_generated_joins
            ]

        return EnrichedSpec(
            original_spec=spec,
            inferred_metrics=inferred_metrics,
            inferred_window_metrics=inferred_window,
            inferred_computed_metrics=inferred_computed,
            enrichment_metadata=metadata,
        )


# ════════════════════════════════════════════
# LLM Prompt 模板——Phase 4 启用
# ════════════════════════════════════════════

_METRIC_INFERENCE_SYSTEM_PROMPT = """你是数据仓库指标推断专家。你的任务是阅读业务描述，
推断程序员可能需要的聚合指标，并输出严格的 JSON 结构。

════════════════════════════════════
硬约束（违反任何一条都是错误）
════════════════════════════════════

H1. 列名只能从提供的 [Table Schemas] 中选择，禁止编造不存在的列名。
    如果你需要的列不在 Schema 中，设置 confidence=low 并标注缺少的列。

H2. 聚合函数只能是以下 6 种之一：
    COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT
    不要使用 STDDEV、MEDIAN、PERCENTILE 等未支持的函数。

H3. filter.column 必须存在于同一张源表中，禁止跨表引用过滤列。
    如果你不确定过滤列属于哪张表，不要添加 filter。

H4. 不要推断 JOIN 关系——这不是你的职责。
    JOIN 由 RelationshipPlanner 独立处理，你的推断会被独立验证。
    你只需要关注单表的聚合指标。

H5. 不能修改 [Existing Metrics] 中程序员已手写的条目。
    程序员显式声明 > LLM 推断。如果已有手写指标覆盖了某个输出列，
    不要重复推断。

H6. 不能修改 target_grain（输出粒度）。
    粒度决定了 GROUP BY 键，修改它等于改变整个聚合的维度。
    你推断的指标必须与当前粒度兼容。

H7. 不确定时设置 confidence=low，不要猜测。
    confidence 取值：high（有明确列对应）、medium（列存在但语义模糊）、
    low（列不确定或需人工确认）。low 置信度的结果会生成 HumanResolution 问题。

H8. 你只接收 schema 信息（列名 + 类型 + 描述），不接收数据样本。
    不要要求或期望看到实际数据值。

════════════════════════════════════
推断规则
════════════════════════════════════

- "总数/数量/计数/个数/次数" → COUNT
- "去重XX数/独立XX数/不重复XX数" → COUNT_DISTINCT
- "求和/总额/合计/累计" → SUM
- "平均/均值/人均" → AVG
- "最大/最高/峰值" → MAX
- "最小/最低/谷值" → MIN
- "有XX的/含XX的/状态为/满足XX条件" → 添加 filter（条件聚合）
- "XX率/XX比/XX占比" → 标记为比率指标（computed metric），列出依赖
- "排名/TOP N/前N名" → 标记为窗口指标
- 多字段计算（如 "单价×数量"）→ 使用 input_expression

════════════════════════════════════
输出格式
════════════════════════════════════

严格按以下 JSON Schema 输出，禁止多余文本或解释：

{
  "inferred_metrics": [
    {
      "metric_name": "指标名",
      "aggregation": "COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT",
      "input_column": "列名（从 Table Schemas 中选择）",
      "alias": "输出别名",
      "filter": null | {"column": "过滤列", "operator": "eq|neq|gt|gte|lt|lte|in|is_null|is_not_null", "value": "过滤值"},
      "input_expression": null | "表达式（如 quantity*unit_price）",
      "distinct": false | true,
      "confidence": "high|medium|low",
      "reasoning": "推断依据（一句话）"
    }
  ],
  "inferred_window_metrics": [
    {
      "metric_name": "窗口指标名",
      "window_function": "ROW_NUMBER|RANK|DENSE_RANK|SUM|AVG|LAG|LEAD",
      "input_column": "输入列名",
      "partition_by": ["分区列"],
      "order_by": ["排序列 DESC"],
      "alias": "输出别名",
      "confidence": "high|medium|low",
      "reasoning": "推断依据"
    }
  ],
  "inferred_computed_metrics": [
    {
      "metric_name": "计算指标名",
      "expression": "表达式（如 fined_count / total_count）",
      "depends_on": ["依赖的指标 alias"],
      "alias": "输出别名",
      "confidence": "high|medium|low",
      "reasoning": "推断依据"
    }
  ]
}"""


# ════════════════════════════════════════════
# LLM 输出 JSON Schema——传给 AnthropicAdapter 做 structured output
# ════════════════════════════════════════════

_METRIC_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "inferred_metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric_name": {
                        "type": "string",
                        "description": "指标名——可用中文或英文",
                    },
                    "aggregation": {
                        "type": "string",
                        "enum": ["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"],
                        "description": "聚合函数——6 种之一",
                    },
                    "input_column": {
                        "type": ["string", "null"],
                        "description": "输入列名——必须从 Table Schemas 中选择",
                    },
                    "alias": {
                        "type": "string",
                        "description": "输出别名",
                    },
                    "filter": {
                        "type": ["object", "null"],
                        "properties": {
                            "column": {"type": "string"},
                            "operator": {
                                "type": "string",
                                "enum": ["eq", "neq", "gt", "gte", "lt", "lte", "in", "is_null", "is_not_null"],
                            },
                            "value": {"type": "string"},
                        },
                        "required": ["column", "operator", "value"],
                        "additionalProperties": False,
                    },
                    "input_expression": {
                        "type": ["string", "null"],
                        "description": "表达式——如 quantity*unit_price",
                    },
                    "distinct": {
                        "type": "boolean",
                        "description": "是否去重",
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "置信度：high(明确列对应)/medium(语义模糊)/low(需人工确认)",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "一句话推断依据",
                    },
                },
                "required": ["metric_name", "aggregation", "alias", "confidence"],
                "additionalProperties": False,
            },
        },
        "inferred_window_metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string"},
                    "window_function": {
                        "type": "string",
                        "enum": ["ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
                                 "LAG", "LEAD", "SUM_OVER", "AVG_OVER", "COUNT_OVER"],
                    },
                    "input_column": {"type": "string"},
                    "partition_by": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "order_by": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "alias": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "reasoning": {"type": "string"},
                },
                "required": ["metric_name", "window_function", "alias", "confidence"],
                "additionalProperties": False,
            },
        },
        "inferred_computed_metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string"},
                    "expression": {"type": "string", "description": "表达式——如 fined_count / total_count"},
                    "depends_on": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "依赖的指标 alias 列表",
                    },
                    "alias": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "reasoning": {"type": "string"},
                },
                "required": ["metric_name", "expression", "alias", "confidence"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["inferred_metrics", "inferred_window_metrics", "inferred_computed_metrics"],
    "additionalProperties": False,
}


# ════════════════════════════════════════════
# 窗口函数白名单 + 表达式安全校验常量
# ════════════════════════════════════════════

# 合法窗口函数名集合——与 planning.models.WindowFunction 枚举保持一致
_VALID_WINDOW_FUNCTIONS: frozenset[str] = frozenset({
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
    "LAG", "LEAD", "SUM_OVER", "AVG_OVER", "COUNT_OVER",
})

# 旧名→新名映射——兼容历史 JSON Schema 中的命名（SUM→SUM_OVER, AVG→AVG_OVER）
_WINDOW_FUNCTION_ALIASES: dict[str, str] = {
    "SUM": "SUM_OVER",
    "AVG": "AVG_OVER",
}

# expression 字段中禁止出现的 SQL 特殊字符——与 _PHYSICAL_TABLE_NAME_FORBIDDEN 同策略
_FORBIDDEN_EXPRESSION_CHARS: frozenset[str] = frozenset({";", "'", '"', "`"})

# expression 字段中禁止出现的 SQL 注释/注入模式
_FORBIDDEN_EXPRESSION_PATTERNS: tuple[str, ...] = ("--", "/*")


class SpecEnricher:
    """Phase 4 LLM 指标推断器——调用 LLM 从业务描述推断指标。

    使用嵌入 8 条硬约束的 System Prompt + JSON Schema 约束输出。
    需要注入 ProviderAdapter，Phase 4 装配。

    与 FakeSpecEnricher 接口完全一致，可在 Pipeline 中直接替换。
    """

    def __init__(self, adapter: ProviderAdapter | None = None):
        """初始化 LLM 推断器。

        Args:
            adapter: LLM Provider 适配器，Phase 4 注入。
                     None 时退化为 FakeSpecEnricher（纯规则推断）。
        """
        self._adapter = adapter
        self._fake = FakeSpecEnricher()

    def enrich(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> EnrichedSpec:
        """执行 LLM 推断，返回 EnrichedSpec。

        adapter=None → 退化为 FakeSpecEnricher（纯规则推断）。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            EnrichedSpec——original_spec + 推断结果
        """
        if self._adapter is None:
            return self._fake.enrich(spec, manifest)

        # Phase 4：构建 Prompt → 调用 LLM → 解析 JSON → 校验 → 返回 EnrichedSpec
        return self._llm_enrich(spec, manifest)

    def apply_enrichment(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> ParsedDeveloperSpec:
        """应用 enrich 结果——将推断指标合并到 spec 中。

        程序员手写的 metrics 优先级最高（不可覆盖），
        仅追加 inferred_metrics 中不与现有 alias 冲突的条目。

        也合入跨粒度 compute_steps + JoinDecl + 窗口指标。

        Args:
            spec: 原始 DeveloperSpec
            manifest: 源数据清单

        Returns:
            增强后的 ParsedDeveloperSpec（如无变更则返回原对象）
        """
        enriched: EnrichedSpec = self.enrich(spec, manifest)

        # ── 合并推断指标 ──
        declared_aliases = {m.alias for m in spec.metrics}
        new_metrics = [
            m for m in enriched.inferred_metrics
            if m.alias not in declared_aliases
        ]
        combined_metrics = list(spec.metrics) + new_metrics

        # ── 合并跨粒度 compute_steps + joins ──
        meta = enriched.enrichment_metadata
        generated_steps_data = meta.get("generated_compute_steps", [])
        generated_joins_data = meta.get("generated_joins", [])

        combined_steps = list(spec.compute_steps) if spec.compute_steps else []
        combined_joins = list(spec.joins) if spec.joins else []

        if generated_steps_data:
            for sd in generated_steps_data:
                combined_steps.append(ComputeStep(**sd))
        if generated_joins_data:
            for jd in generated_joins_data:
                combined_joins.append(JoinDecl(**jd))

        # ── 合并窗口指标 ──
        new_window_metrics = list(enriched.inferred_window_metrics)

        # 仅当有实际变更时才更新
        needs_update = bool(
            new_metrics or generated_steps_data or generated_joins_data
            or new_window_metrics
        )
        if not needs_update:
            return spec

        update_dict: dict = {"metrics": combined_metrics}
        if combined_steps:
            update_dict["compute_steps"] = combined_steps
        if combined_joins:
            update_dict["joins"] = combined_joins
        if new_window_metrics:
            update_dict["inferred_window_metrics"] = new_window_metrics

        return spec.model_copy(update=update_dict)

    def _build_context(self, spec: ParsedDeveloperSpec, manifest: SourceManifest) -> dict:
        """构建 LLM 调用的 Context 部分——不包含数据样本（遵守 H8）。

        Returns:
            可序列化的 Context dict
        """
        # Table Schemas——仅包含结构信息
        tables_info: list[dict] = []
        for table in manifest.tables:
            cols_info: list[dict] = []
            for col in table.columns:
                cols_info.append({
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                })
            tables_info.append({
                "table_ref": table.table_ref,
                "source_table": str(table.source_table) if table.source_table else None,
                "columns": cols_info,
                "estimated_row_count": table.estimated_row_count,
            })

        # 已有指标——不可覆盖（H5）
        existing_metrics: list[dict] = []
        for m in spec.metrics:
            existing_metrics.append({
                "metric_name": m.metric_name,
                "aggregation": m.aggregation.value,
                "input_column": m.input_column,
                "alias": m.alias,
            })

        return {
            "table_schemas": tables_info,
            "existing_metrics": existing_metrics,
            "target_grain": spec.output_spec.grain,
            "output_columns": [c.model_dump() for c in spec.output_spec.columns],
            "business_description": spec.description,
            "spec_title": spec.title,
        }

    def _llm_enrich(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> EnrichedSpec:
        """Phase 4：实际调用 LLM 进行推断。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            EnrichedSpec
        """
        context = self._build_context(spec, manifest)
        raw: dict = {"inferred_metrics": [], "inferred_window_metrics": [], "inferred_computed_metrics": []}

        try:
            raw = self._adapter.invoke(
                system_message=_METRIC_INFERENCE_SYSTEM_PROMPT,
                user_message=json.dumps(context, ensure_ascii=False),
                json_schema=_METRIC_JSON_SCHEMA,
                model="",
                temperature=0.1,
            )
        except Exception:
            # LLM 调用失败 → 退化为规则推断，不阻断流程
            return self._fake.enrich(spec, manifest)

        return self._parse_llm_response(raw, spec)

    def _parse_llm_response(
        self,
        raw: dict,
        spec: ParsedDeveloperSpec,
    ) -> EnrichedSpec:
        """解析 LLM 返回的 JSON 并校验。

        校验步骤（对应硬约束）：
        1. 列名必须在 manifest 中存在（H1）
        2. 聚合函数必须是 AggregationType 枚举值（H2）
        3. filter.column 必须存在（H3）
        4. 不能修改已有 metrics（H5）

        Args:
            raw: LLM 返回的原始 JSON
            spec: 原始 DeveloperSpec

        Returns:
            校验后的 EnrichedSpec
        """
        inferred_metrics: list[MetricDecl] = []
        inferred_window: list[InferredWindowMetric] = []
        inferred_computed: list[InferredComputedMetric] = []

        # 解析指标
        for item in raw.get("inferred_metrics", []):
            try:
                agg = AggregationType(item["aggregation"])
            except (KeyError, ValueError):
                continue  # H2：非法聚合函数，丢弃

            filter_decl = None
            if item.get("filter"):
                try:
                    filter_decl = MetricFilterDecl(
                        column=item["filter"]["column"],
                        operator=item["filter"]["operator"],
                        value=str(item["filter"]["value"]),
                    )
                except Exception:
                    pass  # H3：filter 不合法，丢弃 filter 保留指标

            inferred_metrics.append(
                MetricDecl(
                    metric_name=item.get("metric_name", ""),
                    aggregation=agg,
                    input_column=item.get("input_column"),
                    alias=item.get("alias", ""),
                    filter=filter_decl,
                    input_expression=item.get("input_expression"),
                    distinct=item.get("distinct", False),
                )
            )

        # 解析窗口指标
        for item in raw.get("inferred_window_metrics", []):
            wf = item.get("window_function", "")
            # 旧名兼容映射——SUM→SUM_OVER, AVG→AVG_OVER
            if wf in _WINDOW_FUNCTION_ALIASES:
                wf = _WINDOW_FUNCTION_ALIASES[wf]
            # 白名单校验——非法窗口函数静默丢弃
            if wf not in _VALID_WINDOW_FUNCTIONS:
                continue
            inferred_window.append(
                InferredWindowMetric(
                    metric_name=item.get("metric_name", ""),
                    window_function=wf,
                    input_column=item.get("input_column", ""),
                    partition_by=item.get("partition_by", []),
                    order_by=item.get("order_by", []),
                    alias=item.get("alias", ""),
                    confidence=item.get("confidence", "medium"),
                )
            )

        # 解析计算指标
        for item in raw.get("inferred_computed_metrics", []):
            expr = item.get("expression", "")
            # 安全检查——含 SQL 注入字符的表达式静默丢弃
            if any(c in expr for c in _FORBIDDEN_EXPRESSION_CHARS):
                continue
            if any(p in expr for p in _FORBIDDEN_EXPRESSION_PATTERNS):
                continue
            inferred_computed.append(
                InferredComputedMetric(
                    metric_name=item.get("metric_name", ""),
                    expression=expr,
                    depends_on=item.get("depends_on", []),
                    alias=item.get("alias", ""),
                    confidence=item.get("confidence", "medium"),
                )
            )

        return EnrichedSpec(
            original_spec=spec,
            inferred_metrics=inferred_metrics,
            inferred_window_metrics=inferred_window,
            inferred_computed_metrics=inferred_computed,
            enrichment_metadata={
                "source": "SpecEnricher",
                "method": "llm",
                "raw_response_keys": list(raw.keys()),
            },
        )
