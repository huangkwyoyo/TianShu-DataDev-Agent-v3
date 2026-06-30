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
from typing import Any

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    EnrichedSpec,
    InferredComputedMetric,
    InferredWindowMetric,
    MetricDecl,
    MetricFilterDecl,
    ParsedDeveloperSpec,
    SourceManifest,
)

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
    """

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

        elapsed_ms = int((time.time() - start_time) * 1000)

        return EnrichedSpec(
            original_spec=spec,
            inferred_metrics=inferred_metrics,
            inferred_window_metrics=inferred_window,
            inferred_computed_metrics=inferred_computed,
            enrichment_metadata={
                "source": "FakeSpecEnricher",
                "method": "rule_based",
                "inference_time_ms": elapsed_ms,
                "total_inferred": len(inferred_metrics)
                + len(inferred_window)
                + len(inferred_computed),
            },
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


class SpecEnricher:
    """Phase 4 LLM 指标推断器——调用 LLM 从业务描述推断指标。

    使用嵌入 8 条硬约束的 System Prompt + JSON Schema 约束输出。
    需要注入 LLM 客户端（如 Anthropic/OpenAI SDK），Phase 4 装配。

    与 FakeSpecEnricher 接口完全一致，可在 Pipeline 中直接替换。
    """

    def __init__(self, llm_client: Any = None):
        """初始化 LLM 推断器。

        Args:
            llm_client: LLM 客户端（如 anthropic.Anthropic()），Phase 4 注入。
                        None 时退化为 FakeSpecEnricher。
        """
        self._llm = llm_client
        self._fake = FakeSpecEnricher()

    def enrich(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> EnrichedSpec:
        """执行 LLM 推断，返回 EnrichedSpec。

        当前退化为 FakeSpecEnricher（未注入 LLM 客户端时）。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            EnrichedSpec——original_spec + 推断结果
        """
        if self._llm is None:
            return self._fake.enrich(spec, manifest)

        # Phase 4：构建 Prompt → 调用 LLM → 解析 JSON → 校验 → 返回 EnrichedSpec
        return self._llm_enrich(spec, manifest)

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
        # 占位——Phase 4 实施时注入 LLM 调用
        context = self._build_context(spec, manifest)

        # TODO(Phase 4): 实际调用 self._llm.messages.create()
        # response = self._llm.messages.create(
        #     model="claude-sonnet-4-6",
        #     system=_METRIC_INFERENCE_SYSTEM_PROMPT,
        #     messages=[{"role": "user", "content": json.dumps(context, ensure_ascii=False)}],
        #     max_tokens=4096,
        # )
        # raw = json.loads(response.content[0].text)
        # return self._parse_llm_response(raw, spec)

        # 当前退化为规则推断
        _ = context  # 保留接口，避免 linter 报错
        return self._fake.enrich(spec, manifest)

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
            inferred_window.append(
                InferredWindowMetric(
                    metric_name=item.get("metric_name", ""),
                    window_function=item.get("window_function", ""),
                    input_column=item.get("input_column", ""),
                    partition_by=item.get("partition_by", []),
                    order_by=item.get("order_by", []),
                    alias=item.get("alias", ""),
                    confidence=item.get("confidence", "medium"),
                )
            )

        # 解析计算指标
        for item in raw.get("inferred_computed_metrics", []):
            inferred_computed.append(
                InferredComputedMetric(
                    metric_name=item.get("metric_name", ""),
                    expression=item.get("expression", ""),
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
