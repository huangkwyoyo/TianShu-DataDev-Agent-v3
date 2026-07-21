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
import logging
import re
import time
import uuid
import warnings
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    CaseWhenDecl,
    CompareOp,
    ComputeStep,
    DatasetType,
    DimensionDecl,
    EnrichedSpec,
    InferredComputedMetric,
    InferredWindowMetric,
    JoinDecl,
    JoinTypeEnum,
    LegacyDescriptionDSLWarning,
    MetricDecl,
    MetricFilterDecl,
    OpenQuestion,
    OutputColumnDecl,
    ParsedDeveloperSpec,
    PostWindowFilterDecl,
    SourceManifest,
)
from tianshu_datadev.sql.expression_guard import validate_input_expression

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import (
        LabelRuleProposal,
        LabelRuleProposalOutput,
    )
    from tianshu_datadev.llm.adapters.base import ProviderAdapter

logger = logging.getLogger(__name__)

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

_WINDOW_ALIAS_RE = re.compile(r"^\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
_POST_WINDOW_FILTER_RE = re.compile(
    r"\b(?:WHERE|QUALIFY)\s+([A-Za-z_][A-Za-z0-9_]*)\s*"
    r"(<=|>=|!=|=|<|>)\s*(-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
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
            # 双重校验：入站层（禁止字符） + 编译器层（白名单 + SQL 关键字拒绝）
            is_valid, _ = validate_input_expression(arg, mode="silent")
            is_valid_c, _ = validate_input_expression(arg, mode="compiler")
            if is_valid and is_valid_c:
                input_expression = arg
        else:
            # 基础入站校验——不接受禁止字符，防御深度不下降
            is_valid, _ = validate_input_expression(arg, mode="silent")
            if is_valid:
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


def _parse_business_description_windows(
    spec: ParsedDeveloperSpec,
) -> tuple[list[InferredWindowMetric], list[PostWindowFilterDecl]]:
    """机械提取正文中明确写出的窗口表达式及其外层比较条件。

    这里只识别封闭的函数、标识符、比较符和数值，不接受自由 SQL。
    自然语言需求由真实 SpecEnricher Agent 输出同一结构。
    """
    output_names = {col.name for col in spec.output_spec.columns}
    windows: list[InferredWindowMetric] = []

    for match in _DESC_WINDOW_RE.finditer(spec.description):
        alias_match = _WINDOW_ALIAS_RE.match(spec.description[match.end():])
        if not alias_match:
            continue
        alias = alias_match.group(1)
        if alias not in output_names:
            continue

        function = match.group(1).upper()
        function = _WINDOW_FUNCTION_ALIASES.get(function, function)
        input_column = match.group(2).strip() if match.group(2) else ""
        partition_raw = match.group(3).strip() if match.group(3) else ""
        order_raw = match.group(4).strip() if match.group(4) else ""
        windows.append(InferredWindowMetric(
            metric_name=alias,
            window_function=function,
            input_column=input_column,
            partition_by=[p.strip() for p in partition_raw.split(",") if p.strip()],
            order_by=[o.strip() for o in order_raw.split(",") if o.strip()],
            alias=alias,
            confidence="high",
        ))

    window_aliases = {window.alias for window in windows}
    filters: list[PostWindowFilterDecl] = []
    for match in _POST_WINDOW_FILTER_RE.finditer(spec.description):
        column, raw_operator, raw_value = match.groups()
        if column not in window_aliases:
            continue
        value: int | float
        value = float(raw_value) if "." in raw_value else int(raw_value)
        filters.append(PostWindowFilterDecl(
            column=column,
            operator=CompareOp(raw_operator),
            value=value,
        ))

    return windows, filters


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

    # ── 英文指标名直接匹配：当中文关键词映射无法匹配时，
    # 对英文指标名进行前缀剥离 + 单词重叠匹配——
    # 例如 "avg_distance_miles" → 剥离 "avg_" → "distance_miles" 精确匹配源列
    if not scored and metric_name_lower:
        # 常见聚合前缀——用于剥离以获取核心列名
        _known_prefixes = [
            "avg_", "average_", "total_", "sum_", "count_", "max_", "min_",
            "anomaly_", "distinct_", "dedup_", "unique_",
        ]
        core = metric_name_lower
        for prefix in _known_prefixes:
            if core.startswith(prefix):
                core = core[len(prefix):]
                break
        metric_words = set(metric_name_lower.split("_"))
        core_words = set(core.split("_"))

        for table in manifest.tables:
            for col in table.columns:
                if col.column_name in seen or col.column_name in exclude:
                    continue
                col_lower = col.column_name.lower()
                col_words = set(col_lower.split("_"))
                score = 0

                # 策略1: 剥离前缀后精确匹配（最高分）
                if core == col_lower:
                    score = 25
                # 策略2: 核心名单词与列名单词重叠——优先于全名单词重叠
                # 例如 "passengers" vs "passenger_count" → 子串包含计分
                elif core_words and col_words:
                    for cw in core_words:
                        for mw in col_words:
                            if cw == mw:
                                score += 8  # 精确单词匹配
                            elif len(cw) > 3 and len(mw) > 3 and (cw in mw or mw in cw):
                                score += 4  # 模糊单词匹配（如 passenger≈passengers）
                # 策略3: 全名单词重叠（最低分）——仅在核心名匹配无结果时使用
                if score == 0 and metric_words and col_words:
                    overlap = len(metric_words & col_words)
                    if overlap > 0:
                        score = overlap * 2

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

    优先检查英文指标名中的聚合关键词（更精确），
    未命中时回退到中文正则匹配（覆盖中文指标名和描述文本）。

    Args:
        description: 业务描述文本
        metric_name: 指标名（如 "fined_plate_count"）

    Returns:
        (AggregationType, needs_distinct)
    """
    name_lower = metric_name.lower()

    # ── 优先英文名称推断——比中文正则匹配更精确，
    # 避免描述文本中的"收入"等关键词使所有指标都被误判为 SUM
    if any(kw in name_lower for kw in ("unique", "distinct", "dedup", "去重")):
        return AggregationType.COUNT_DISTINCT, False
    if any(kw in name_lower for kw in ("avg", "average", "mean", "平均")):
        return AggregationType.AVG, False
    if any(kw in name_lower for kw in ("max", "maximum", "highest", "最大", "最高")):
        return AggregationType.MAX, False
    if any(kw in name_lower for kw in ("min", "minimum", "lowest", "最小", "最低")):
        return AggregationType.MIN, False
    if any(kw in name_lower for kw in ("sum", "total", "amount", "revenue", "sales")):
        return AggregationType.SUM, False
    # "count" / "cnt" → COUNT——覆盖 trip_count、anomaly_trip_count 等
    if any(kw in name_lower for kw in ("count", "cnt")):
        return AggregationType.COUNT, False
    # "count" 是默认值，不在此匹配——让中文正则有机会匹配更具体的模式
    # （如"去重XX数"→COUNT_DISTINCT），未命中时默认为 COUNT

    # ── 中文正则兜底——匹配中文指标名和描述文本 ──
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

    def _infer_dimensions(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest | None,
        excluded_output_names: set[str] | None = None,
    ) -> list[DimensionDecl]:
        """推断输出列到源列的维度映射。

        当输出列名在源表中找不到精确匹配时，通过子串匹配 + JOIN 上下文
        自动推断 column_ref 和 source_table。这在多表 JOIN 场景中尤为关键——
        源表列名相同但输出需要重命名（如 tz_pu.zone_name → pickup_zone_name）。

        推断规则：
        1. 输出列名精确匹配源列 → 跳过（无需维度声明）
        2. 输出列名包含某源列名 → 候选
        3. 单一候选 → 直接采纳
        4. 多候选 → 通过 JOIN key 关键词消歧义

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            推断的 DimensionDecl 列表
        """
        # ── 收集所有源表列 → (table_alias, column_name, data_type) ──
        # 从 spec.input_tables 的显式声明列构建——不使用 manifest.tables，
        # 因为 build_manifest_from_spec 会将 output_spec 的列名（如 pickup_zone_name）
        # 加入每个 ManifestTable，导致"精确匹配"误判并跳过维度推断。
        all_source_cols: dict[str, list[tuple[str, str, str]]] = {}
        for table in spec.input_tables:
            alias = table.table_alias
            for col_list in [table.columns, table.key_columns, table.business_columns]:
                for col in col_list:
                    cn = col.column_name
                    if cn not in all_source_cols:
                        all_source_cols[cn] = []
                    all_source_cols[cn].append((alias, cn, col.data_type or "unknown"))

        # ── 构建 JOIN key 上下文（右表别名 → 左表 join key）──
        join_keys: dict[str, str] = {}
        if spec.joins:
            for join in spec.joins:
                join_keys[join.right_table] = join.left_key

        dimensions: list[DimensionDecl] = []
        declared_dimensions = {d.dimension_name for d in spec.dimensions}
        excluded = excluded_output_names or set()

        for col in spec.output_spec.columns:
            col_name = col.name
            if col_name in declared_dimensions or col_name in excluded:
                continue

            # 精确源列也是维度候选。若它不是指标/窗口输出，就必须进入
            # dimensions，AggregateStep 才会把它纳入 GROUP BY。
            if col_name in all_source_cols:
                exact_candidates = all_source_cols[col_name]
                exact_tables = {item[0] for item in exact_candidates}
                if len(exact_tables) == 1:
                    alias, src_col, _ = exact_candidates[0]
                    dimensions.append(DimensionDecl(
                        dimension_name=col_name,
                        column_ref=src_col,
                        source_table=alias,
                    ))
                continue

            # ── 子串匹配：源列名出现在输出列名中 ──
            candidates: list[tuple[str, str, str]] = []
            for src_col_name, entries in all_source_cols.items():
                if src_col_name in col_name:
                    candidates.extend(
                        (alias, src_col_name, dtype) for alias, _cn, dtype in entries
                    )

            if not candidates:
                continue  # 无法推断，跳过

            if len(candidates) == 1:
                # 唯一匹配——直接采纳
                alias, src_col, _ = candidates[0]
                dimensions.append(DimensionDecl(
                    dimension_name=col_name,
                    column_ref=src_col,
                    source_table=alias,
                ))
            else:
                # 多候选——通过 JOIN key 关键词消歧义
                best = self._disambiguate_by_join(col_name, candidates, join_keys)
                if best:
                    alias, src_col, _ = best
                    dimensions.append(DimensionDecl(
                        dimension_name=col_name,
                        column_ref=src_col,
                        source_table=alias,
                    ))

        return dimensions

    @staticmethod
    def _disambiguate_by_join(
        col_name: str,
        candidates: list[tuple[str, str, str]],
        join_keys: dict[str, str],
    ) -> tuple[str, str, str] | None:
        """通过 JOIN key 关键词匹配消歧义。

        策略：
        1. 检查列名是否包含 table alias 的片段
        2. 检查 JOIN key 中的关键词是否出现在列名中
           （如 pickup_zone_name 匹配 pickup_location_id 中的 "pickup"）

        Args:
            col_name: 输出列名
            candidates: [(table_alias, source_col, data_type), ...]
            join_keys: {right_table_alias → left_join_key}

        Returns:
            最佳候选 (table_alias, source_col, data_type) 或 None
        """
        col_lower = col_name.lower()
        best_score = 0
        best_candidate = None

        for alias, src_col, dtype in candidates:
            score = 0
            # 检查列名是否包含 table alias 的片段
            alias_lower = alias.lower()
            # 将 alias 拆分为 token（如 tz_pu → ["tz", "pu"]）
            alias_tokens = alias_lower.replace("_", " ").split()
            for token in alias_tokens:
                if len(token) >= 3 and token in col_lower:
                    score += 3

            # 检查 JOIN key 中的关键词
            if alias in join_keys:
                join_key = join_keys[alias].lower()
                # 提取 JOIN key 中的语义 token（去掉 _id, _key 等后缀）
                key_tokens = join_key.replace("_id", "").replace("_key", "").split("_")
                for token in key_tokens:
                    if len(token) >= 3 and token in col_lower:
                        score += 5

            if score > best_score:
                best_score = score
                best_candidate = (alias, src_col, dtype)

        return best_candidate if best_score > 0 else None

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

        # 模板无需逐列声明语义：明确写在业务正文中的窗口表达式可确定性提取；
        # 自然语言窗口需求由生产环境的 SpecEnricher Agent 输出相同结构。
        for col in spec.output_spec.columns:
            window = col.window_hint
            if window is None and col.description:
                window = _parse_description_to_window(col)
            if window is not None:
                inferred_window.append(window)
        business_windows, post_window_filters = \
            _parse_business_description_windows(spec)
        known_window_aliases = {window.alias for window in inferred_window}
        for window in business_windows:
            if window.alias not in known_window_aliases:
                inferred_window.append(window)
                known_window_aliases.add(window.alias)

        # 收集已声明的指标 alias 和使用的列
        declared_aliases: set[str] = {m.alias for m in spec.metrics}
        declared_columns: set[str] = set()
        for m in spec.metrics:
            if m.input_column:
                declared_columns.add(m.input_column)

        # 若指标列表显式为空且 dataset_type 为 label_table / detail_table——
        # 程序员明确声明了"无需聚合"，跳过兜底 SUM 推断。
        # aggregate_table / unspecified 即使 metrics 为空也应走兜底推断，
        # 因为这些类型的 Spec 期望从描述文本自动推断聚合指标。
        # 但结构化 hint（metric_hint/computed_hint/window_hint）在所有类型中仍需处理。
        _no_aggregation_types = {DatasetType.LABEL_TABLE, DatasetType.DETAIL_TABLE}
        metrics_explicitly_empty = (
            len(spec.metrics) == 0
            and spec.dataset_type in _no_aggregation_types
        )

        # 先识别维度，再判断剩余输出是否为指标。源表中存在的普通字段
        # 不能因为未写入 dimensions 就被默认推断成 SUM。
        semantic_aliases = declared_aliases | known_window_aliases | {
            col.name for col in spec.output_spec.columns
            if col.metric_hint or col.computed_hint
        }
        inferred_dimensions = self._infer_dimensions(
            spec, manifest, excluded_output_names=semantic_aliases,
        )
        dimension_names = {d.dimension_name for d in spec.dimensions}
        dimension_names.update(d.dimension_name for d in inferred_dimensions)

        # 收集 output_columns 中的指标列（非维度列、非 grain 列）
        grain_set: set[str] = set(spec.output_spec.grain)
        output_metric_cols: list[OutputColumnDecl] = [
            c for c in spec.output_spec.columns
            if c.name not in grain_set and c.name not in declared_aliases
            and c.name not in dimension_names
            and c.name not in known_window_aliases
        ]

        # 对每个未声明的输出指标列，尝试推断
        for col in output_metric_cols:
            col_name = col.name

            # 优先使用结构化 hint（推荐方式）
            if col.metric_hint:
                inferred_metrics.append(col.metric_hint)
                continue

            # 有 window_hint 或 computed_hint 的列不推断为聚合指标
            if col.window_hint or col.computed_hint:
                continue

            # 次优：从旧 description DSL 解析（兼容模式，触发迁移警告）
            if col.description:
                warnings.warn(
                    f"列 '{col_name}' 通过旧 description DSL 推断指标 "
                    f"（\"{col.description[:80]}{'...' if len(col.description) > 80 else ''}\"），"
                    f"请迁移到 metric_hint 结构化字段",
                    LegacyDescriptionDSLWarning,
                    stacklevel=2,
                )
                parsed = _parse_description_to_metric(col)
                if parsed:
                    inferred_metrics.append(parsed)
                    continue

            # 兜底：从描述文本推断聚合类型
            # 若 metrics 显式声明为空（label_table / detail_table），
            # 跳过兜底推断——透传列不应被 SUM 误判
            if metrics_explicitly_empty:
                continue
            agg_type, needs_distinct = _infer_aggregation_type(
                spec.description, col_name
            )

            # 尝试匹配 manifest 中的输入列——metric_name 用于消歧义
            matched_cols = _find_matching_columns(
                col_name, manifest, declared_columns, metric_name=col_name,
            )

            input_col = matched_cols[0] if matched_cols else None

            # COUNT(*) 是唯一允许无输入列的聚合。其他聚合缺少字段时保持未解析，
            # 交给 Agent/HumanReview，而不是制造 SUM(*) 之类的无效计划。
            if input_col is None and agg_type != AggregationType.COUNT:
                continue

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

        # 检测比率类指标——优先使用结构化 hint
        for col in output_metric_cols:
            # 优先使用结构化 hint（推荐方式）
            if col.computed_hint:
                inferred_computed.append(col.computed_hint)
                continue
            # 次优：从旧 description DSL 解析（兼容模式）
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

        # ── Phase 5：跨粒度依赖检测 ──
        cross_grain_steps, cross_grain_joins = \
            self._detect_cross_grain_dependency(spec, inferred_computed, manifest)

        # ── Phase 6：条件分支检测 ──
        branch_steps, branch_joins = \
            self._detect_conditional_branch(spec, inferred_metrics)

        elapsed_ms = int((time.time() - start_time) * 1000)

        # ── 合并所有生成的 compute_steps + joins（跨粒度 + 条件分支）──
        all_generated_steps = (
            list(cross_grain_steps) + list(branch_steps)
        )
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
            inferred_post_window_filters=post_window_filters,
            inferred_computed_metrics=inferred_computed,
            inferred_dimensions=inferred_dimensions,
            enrichment_metadata=metadata,
        )


# ════════════════════════════════════════════
# LLM Prompt 模板——Phase 4 启用
# ════════════════════════════════════════════

_METRIC_INFERENCE_SYSTEM_PROMPT = """你是数据开发规格分析 Agent。你的任务是阅读程序员提供的
字段、业务描述和源表 Schema，将输出列分类为维度、聚合指标、计算指标或窗口指标，
并输出严格的 JSON 结构。程序员不需要为每个输出列重复填写结构化提示。

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
    你可以分类多表输出字段，但不能自行创建或修改表间关系。

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

H9. 普通源字段应输出为 inferred_dimensions，禁止把 varchar 等维度列推断成 SUM。
    非 COUNT 聚合必须提供 input_column 或受控 input_expression。

H10. 窗口结果上的 TopN/排名过滤只能输出 inferred_post_window_filters，
     column 必须引用本次 inferred_window_metrics 的 alias，禁止输出 SQL 片段。

H11. CASE WHEN 推断——当业务描述中包含分类/分段/标签定义时
     （如 "高峰定义：7-10、17-20 为高峰，其余为平峰"），
     输出 inferred_case_when 数组。每一条规则包含：
     - output_column：目标 varchar 输出列名
     - branches：WHEN-THEN 分支列表，每个分支含 condition/then_label/evidence
     - else_value：ELSE 默认值

     条件必须使用 LabelPredicateCondition 类型化 AST——
     仅允许 6 种根节点类型（node_type 字段区分）：
     - COMPARE: {"node_type":"COMPARE","left":"列名","op":"=|!=|>|>=|<|<=",
                 "right":{"node_type":"LITERAL","value":值,"data_type":"number|string|boolean|null"}}
     - IS_NULL: {"node_type":"IS_NULL","column":"列名"}
     - IS_NOT_NULL: {"node_type":"IS_NOT_NULL","column":"列名"}
     - AND: {"node_type":"AND","children":[条件1,条件2,...]}（至少2子节点）
     - OR: {"node_type":"OR","children":[条件1,条件2,...]}（至少2子节点）
     - NOT: {"node_type":"NOT","child":条件}（注意：label_table v1 暂不支持 NOT）

     铁律：
     a) 禁止使用自由字符串 when/then SQL！条件必须是结构化 AST
     b) THEN/ELSE 输出真实值（如 "高峰"），禁止携带 SQL 引号（如 "'高峰'"）
     c) evidence 必须从业务描述原文中提取，用于后续锚定验证
     d) 列名只能从提供的 Table Schemas 中选择（同 H1）
     e) 不确定时不要输出——错误的 CASE WHEN 比缺失更严重
     f) evaluation_phase 判定规则（必填）：
        - 若 CASE 输出列被 dimensions 或 grain 引用：pre_aggregate
        - 若 CASE 条件引用的列全部来自源表物理列（非聚合指标）：pre_aggregate
        - 若 CASE 条件引用了聚合指标（如 SUM(xxx) 的结果列）：post_aggregate
        - 不确定时设置 evaluation_phase=null——系统将转为 HUMAN_REVIEW
     g) 派生维度列（如 pickup_hour/pickup_date/peak_type）通常为 pre_aggregate

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
      "filter": (null
                 | {"column": "过滤列",
                    "operator": "eq|neq|gt|gte|lt|lte|in|is_null|is_not_null",
                    "value": "过滤值"}),
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
  "inferred_post_window_filters": [
    {"column": "窗口输出 alias", "operator": "<=|<|=|!=|>|>=", "value": 10}
  ],
  "inferred_dimensions": [
    {
      "dimension_name": "输出列名",
      "column_ref": "Table Schemas 中的源列名",
      "source_table": "源表 table_ref"
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
                                "enum": [
                                    "eq", "neq", "gt", "gte", "lt", "lte",
                                    "in", "is_null", "is_not_null",
                                ],
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
        "inferred_dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string"},
                    "column_ref": {"type": "string"},
                    "source_table": {"type": ["string", "null"]},
                },
                "required": ["dimension_name", "column_ref", "source_table"],
                "additionalProperties": False,
            },
        },
        "inferred_post_window_filters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "operator": {
                        "type": "string",
                        "enum": ["=", "!=", ">", ">=", "<", "<="],
                    },
                    "value": {"type": "number"},
                },
                "required": ["column", "operator", "value"],
                "additionalProperties": False,
            },
        },
        "inferred_case_when": {
            "type": "array",
            "description": "CASE WHEN 分类规则——从业务描述推断的结构化标签分支。"
                           "仅当描述中包含明确的分类/分段/标签定义时才输出。"
                           "条件必须使用 LabelPredicateCondition 类型化 AST（6 种根节点），"
                           "禁止自由字符串 when/then SQL。",
            "items": {
                "type": "object",
                "properties": {
                    "output_column": {
                        "type": "string",
                        "description": "CASE WHEN 输出列名——"
                                       "必须是 output_spec.columns 中的 varchar/string 列",
                    },
                    "branches": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "condition": {
                                    "$ref": "#/$defs/LabelPredicateCondition",
                                    "description": "WHEN 条件——必须为 6 种根节点类型之一",
                                },
                                "then_label": {
                                    "type": "string",
                                    "description": "THEN 结果值——真实标签值（如 '高峰'），不含 SQL 引号",
                                },
                                "evidence": {
                                    "type": "string",
                                    "description": "原文证据——从业务描述中提取的支持文本，用于后续锚定验证",
                                },
                            },
                            "required": ["condition", "then_label", "evidence"],
                            "additionalProperties": False,
                        },
                    },
                    "else_value": {
                        "type": "string",
                        "description": "ELSE 默认值——真实标签值（如 '平峰'），不含 SQL 引号",
                    },
                    "label_domain": {
                        "type": "object",
                        "description": "标签值域——可选，未提供时自动从 branches+else_value 提取",
                        "properties": {
                            "values": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "所有可能的标签值",
                            },
                            "source_evidence": {"type": "string"},
                            "is_exhaustive": {"type": "boolean"},
                            "completeness_evidence": {"type": "string"},
                        },
                        "required": ["values"],
                        "additionalProperties": False,
                    },
                    "evaluation_phase": {
                        "type": ["string", "null"],
                        "enum": ["pre_aggregate", "post_aggregate", None],
                        "description": "CASE WHEN 的聚合阶段评估位置。"
                                       "pre_aggregate：派生维度——在 GROUP BY 前计算，"
                                       "输出列自动加入 group_by。"
                                       "post_aggregate：标签列——在 GROUP BY 后计算，"
                                       "条件可引用聚合指标。null：无法判定——将产生 HUMAN_REVIEW。",
                    },
                },
                "required": ["output_column", "branches", "else_value"],
                "additionalProperties": False,
            },
        },
    },
    "required": [
        "inferred_metrics", "inferred_window_metrics", "inferred_computed_metrics",
        "inferred_dimensions", "inferred_post_window_filters",
    ],
    "additionalProperties": False,
    "$defs": {
        "LabelTypedLiteral": {
            "type": "object",
            "description": "类型化字面量——不可作为 WHEN 根条件",
            "properties": {
                "node_type": {"const": "LITERAL"},
                "value": {
                    "description": "字面量值——可以是字符串、数字、布尔值或 null"
                },
                "data_type": {
                    "type": "string",
                    "enum": ["string", "number", "boolean", "null"],
                    "description": "字面量的真实 Python 类型",
                },
            },
            "required": ["node_type", "value", "data_type"],
            "additionalProperties": False,
        },
        "LabelPredicateCondition": {
            "description": "标签谓词条件 AST——仅 6 种根节点类型。"
                           "LITERAL/COLUMN_REF 不可作为根条件，LLM 若输出则 Schema 层拒绝。",
            "oneOf": [
                {
                    "type": "object",
                    "description": "二元比较——left OP right",
                    "properties": {
                        "node_type": {"const": "COMPARE"},
                        "left": {"type": "string", "description": "左操作数列名"},
                        "op": {
                            "type": "string",
                            "enum": ["=", "!=", ">", ">=", "<", "<="],
                            "description": "比较操作符",
                        },
                        "right": {
                            "$ref": "#/$defs/LabelTypedLiteral",
                            "description": "右操作数——类型化字面量",
                        },
                    },
                    "required": ["node_type", "left", "op", "right"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "description": "IS NULL 检查",
                    "properties": {
                        "node_type": {"const": "IS_NULL"},
                        "column": {"type": "string", "description": "待检查的列名"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "description": "IS NOT NULL 检查",
                    "properties": {
                        "node_type": {"const": "IS_NOT_NULL"},
                        "column": {"type": "string", "description": "待检查的列名"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "description": "逻辑 AND——至少 2 个子节点",
                    "properties": {
                        "node_type": {"const": "AND"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/LabelPredicateCondition"},
                            "description": "AND 子条件列表",
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "description": "逻辑 OR——至少 2 个子节点",
                    "properties": {
                        "node_type": {"const": "OR"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/LabelPredicateCondition"},
                            "description": "OR 子条件列表",
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "description": "逻辑 NOT——单子节点。注意：label_table v1 暂不支持 NOT 根条件，"
                                   "Validator 会拒绝。",
                    "properties": {
                        "node_type": {"const": "NOT"},
                        "child": {
                            "$ref": "#/$defs/LabelPredicateCondition",
                            "description": "被取反的子条件",
                        },
                    },
                    "required": ["node_type", "child"],
                    "additionalProperties": False,
                },
            ],
        },
    },
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

# expression 字段安全校验——统一使用 expression_guard 共享模块
# 禁止字符和模式定义见 tianshu_datadev.sql.expression_guard


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

    @staticmethod
    def _collect_condition_columns(
        condition, columns: set[str],
    ) -> None:
        """递归收集 LabelPredicateCondition AST 中引用的所有列名。

        用于 evaluation_phase 上下文判定——判断条件是否引用聚合指标。

        Args:
            condition: LabelPredicateCondition 根节点
            columns: 输出集合——收集到的列名追加到此集合
        """
        node_type = getattr(condition, "node_type", None)
        if node_type is None:
            return
        if node_type == "COMPARE":
            if hasattr(condition, "left") and condition.left:
                columns.add(str(condition.left))
        elif node_type in ("IS_NULL", "IS_NOT_NULL"):
            if hasattr(condition, "column") and condition.column:
                columns.add(str(condition.column))
        elif node_type in ("AND", "OR"):
            for child in getattr(condition, "children", []) or []:
                SpecEnricher._collect_condition_columns(child, columns)
        elif node_type == "NOT":
            child = getattr(condition, "child", None)
            if child is not None:
                SpecEnricher._collect_condition_columns(child, columns)

    @staticmethod
    def _resolve_evaluation_phase(
        cw: CaseWhenDecl,
        spec: ParsedDeveloperSpec,
    ) -> Literal["pre_aggregate", "post_aggregate"] | None:
        """根据 spec 上下文判定 CASE WHEN 的 evaluation_phase。

        在 LLM 未提供 evaluation_phase 时进行确定性回退判定——
        仅根据 spec 中已声明的 dimensions / grain / metrics / manifest 列，
        不猜测、不推断。

        Args:
            cw: 待判定的 CaseWhenDecl
            spec: 已解析的 DeveloperSpec

        Returns:
            "pre_aggregate" / "post_aggregate" / None（无法判定——需 HUMAN_REVIEW）
        """
        # 规则 1：输出列被 dimension 或 grain 引用 → pre_aggregate
        dim_names = {d.dimension_name for d in spec.dimensions}
        grain_names = set(spec.output_spec.grain or [])
        if cw.output_column in dim_names or cw.output_column in grain_names:
            return "pre_aggregate"

        # 规则 2：收集 CASE WHEN 条件中引用的列名
        condition_cols: set[str] = set()
        for tb in cw.typed_branches:
            SpecEnricher._collect_condition_columns(tb.condition, condition_cols)

        # 规则 3：条件引用了聚合指标别名 → post_aggregate
        metric_aliases = {m.alias for m in spec.metrics}
        if condition_cols & metric_aliases:
            return "post_aggregate"

        # 规则 4：条件列全是源表物理列 → pre_aggregate（派生维度场景）
        manifest_cols: set[str] = set()
        for table in spec.input_tables:
            for col in table.columns:
                manifest_cols.add(col.column_name.lower())
            for col in table.key_columns:
                manifest_cols.add(col.column_name.lower())
            for col in table.business_columns:
                manifest_cols.add(col.column_name.lower())

        if condition_cols and all(
            c.lower() in manifest_cols for c in condition_cols
        ):
            return "pre_aggregate"

        # 无法判定 → None（转为 OpenQuestion）
        return None

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

        # ── 合并窗口指标与窗口后过滤 ──
        existing_window_aliases = {m.alias for m in spec.inferred_window_metrics}
        new_window_metrics = [
            m for m in enriched.inferred_window_metrics
            if m.alias not in existing_window_aliases
        ]
        combined_window_metrics = list(spec.inferred_window_metrics) + new_window_metrics

        existing_post_filter_keys = {
            (f.column, f.operator, f.value)
            for f in spec.inferred_post_window_filters
        }
        new_post_window_filters = [
            f for f in enriched.inferred_post_window_filters
            if (f.column, f.operator, f.value) not in existing_post_filter_keys
        ]
        combined_post_window_filters = (
            list(spec.inferred_post_window_filters) + new_post_window_filters
        )

        # ── 合并推断的维度映射 ──
        # 仅追加 spec 中未显式声明的维度（程序员手写优先级最高）
        declared_dim_names = {d.dimension_name for d in spec.dimensions}
        new_dimensions = [
            d for d in enriched.inferred_dimensions
            if d.dimension_name not in declared_dim_names
        ]
        combined_dimensions = list(spec.dimensions) + new_dimensions

        # ── 合并 CASE WHEN 标签规则（H11）──
        case_when_data: list[dict] = meta.get("case_when_rules", [])
        new_case_when: list[CaseWhenDecl] = []
        existing_label_cols = {r.output_column for r in spec.label_rules}
        # 从 parser 层传递的 unresolved_case_when（校验失败 / AST 非法等）
        unresolved_cw: list[dict] = list(meta.get("unresolved_case_when", []))
        for cw_dict in case_when_data:
            try:
                cw = CaseWhenDecl(**cw_dict)
                if cw.output_column in existing_label_cols:
                    continue
                # ── evaluation_phase 上下文判定 ──
                # LLM 可能返回 null——此时根据 spec.dimensions / grain / metrics
                # 进行确定性回退判定。无法判定时产生 OpenQuestion。
                if cw.evaluation_phase is None:
                    resolved = self._resolve_evaluation_phase(cw, spec)
                    if resolved is not None:
                        cw.evaluation_phase = resolved
                    else:
                        unresolved_cw.append({
                            "output_column": cw.output_column,
                            "reason": (
                                "CASE WHEN evaluation_phase 无法判定——"
                                "输出列未被 dimensions/grain 引用，"
                                "且条件列无法确认为纯源表物理列或纯聚合指标"
                            ),
                            "blocking_errors": [],
                            "human_review_items": [
                                "evaluation_phase 需人工确认——"
                                "请明确该 CASE WHEN 应在聚合前还是聚合后计算"
                            ],
                        })
                        continue
                new_case_when.append(cw)
                existing_label_cols.add(cw.output_column)
            except Exception as exc:
                logger.warning("CASE WHEN 规则反序列化失败: %s", exc)
        combined_label_rules = list(spec.label_rules) + new_case_when

        # ── 未解析的 CASE WHEN 列 → OpenQuestion ──
        new_open_questions: list[OpenQuestion] = []
        for uc in unresolved_cw:
            q = OpenQuestion(
                question_id=f"cw_{uuid.uuid4().hex[:12]}",
                source="spec_enricher",
                field_ref=uc.get("output_column"),
                description=f"CASE WHEN 推断未完成——{uc.get('reason', '未知原因')}。"
                            f"blocking_errors={uc.get('blocking_errors', [])}, "
                            f"human_review_items={uc.get('human_review_items', [])}",
                blocking=False,
            )
            new_open_questions.append(q)
        combined_open_questions = list(spec.open_questions) + new_open_questions

        # 仅当有实际变更时才更新
        needs_update = bool(
            new_metrics or generated_steps_data or generated_joins_data
            or new_window_metrics or new_post_window_filters or new_dimensions
            or new_case_when or new_open_questions
        )
        if not needs_update:
            return spec

        update_dict: dict = {"metrics": combined_metrics}
        if combined_steps:
            update_dict["compute_steps"] = combined_steps
        if combined_joins:
            update_dict["joins"] = combined_joins
        if new_window_metrics:
            update_dict["inferred_window_metrics"] = combined_window_metrics
        if new_post_window_filters:
            update_dict["inferred_post_window_filters"] = combined_post_window_filters
        if new_dimensions:
            update_dict["dimensions"] = combined_dimensions
        if new_case_when:
            update_dict["label_rules"] = combined_label_rules
        if new_open_questions:
            update_dict["open_questions"] = combined_open_questions

        return spec.model_copy(update=update_dict)

    @staticmethod
    def _wrap_case_when_proposal(
        llm_output: LabelRuleProposalOutput,
        source_spec_hash: str,
    ) -> LabelRuleProposal | None:
        """将 LLM 输出的 LabelRuleProposalOutput 包装为系统级 LabelRuleProposal。

        注入 proposal_id/source_spec_hash，并为空的 label_domain 自动提取值域。

        Args:
            llm_output: LLM 直接产出的标签规则
            source_spec_hash: 源 Spec 哈希

        Returns:
            系统包装的 LabelRuleProposal——失败时返回 None
        """
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal,
            LabelDomain,
            LabelRuleProposal,
        )

        proposal_id = f"prop_{uuid.uuid4().hex[:12]}"

        # 包装 LabelDomain——未提供时自动从 branches + else_value 提取值域
        if llm_output.label_domain is None or not llm_output.label_domain.values:
            domain_values = [b.then_label for b in llm_output.branches]
            if llm_output.else_value not in domain_values:
                domain_values.append(llm_output.else_value)
            domain = LabelDomain(
                domain_id=f"dom_{uuid.uuid4().hex[:12]}",
                values=domain_values,
            )
        else:
            domain = LabelDomain(
                domain_id=f"dom_{uuid.uuid4().hex[:12]}",
                values=llm_output.label_domain.values,
                source_evidence=llm_output.label_domain.source_evidence,
                is_exhaustive=llm_output.label_domain.is_exhaustive,
                completeness_evidence=llm_output.label_domain.completeness_evidence,
            )

        # 包装分支——evidence 非空检查
        branches: list[LabelBranchProposal] = []
        for b in llm_output.branches:
            branches.append(LabelBranchProposal(
                condition=b.condition,
                then_label=b.then_label,
                evidence=b.evidence or "",
            ))

        try:
            return LabelRuleProposal(
                proposal_id=proposal_id,
                source_spec_hash=source_spec_hash,
                output_column=llm_output.output_column,
                branches=branches,
                else_value=llm_output.else_value,
                label_domain=domain,
            )
        except Exception as exc:
            logger.warning("包装 LabelRuleProposal 失败: %s", exc)
            return None

    def _build_context(self, spec: ParsedDeveloperSpec, manifest: SourceManifest) -> dict:
        """构建 LLM 调用的 Context 部分——不包含数据样本（遵守 H8）。

        Returns:
            可序列化的 Context dict
        """
        # Table Schemas——仅包含结构信息
        tables_info: list[dict] = []
        for table in manifest.tables:
            cols_info: list[dict] = []
            declared_input_columns = {
                column.column_name
                for input_table in spec.input_tables
                if input_table.table_alias == table.table_ref
                for group in (
                    input_table.columns,
                    input_table.key_columns,
                    input_table.business_columns,
                )
                for column in group
            }
            for col in table.columns:
                if declared_input_columns and col.column_name not in declared_input_columns:
                    continue
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
        raw: dict = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
        }

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
        inferred_dimensions: list[DimensionDecl] = []
        inferred_post_window_filters: list[PostWindowFilterDecl] = []

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

            # 安全校验——LLM 产出的 input_expression 含注入字符或 SQL 关键字时静默丢弃
            raw_input_expr = item.get("input_expression")
            if raw_input_expr:
                is_valid, _ = validate_input_expression(raw_input_expr, mode="silent")
                is_valid_c, _ = validate_input_expression(raw_input_expr, mode="compiler")
                if not is_valid or not is_valid_c:
                    logger.warning(
                        "LLM 产出 input_expression '%s' 未通过安全校验（silent=%s, compiler=%s），已丢弃",
                        raw_input_expr, is_valid, is_valid_c,
                    )
                    raw_input_expr = None  # 校验失败时静默丢弃表达式

            input_column = item.get("input_column")
            if (
                agg != AggregationType.COUNT
                and input_column is None
                and raw_input_expr is None
            ):
                logger.warning(
                    "LLM 产出聚合 %s(%s) 缺少输入列，已拒绝",
                    agg.value,
                    item.get("alias", ""),
                )
                continue

            inferred_metrics.append(
                MetricDecl(
                    metric_name=item.get("metric_name", ""),
                    aggregation=agg,
                    input_column=input_column,
                    alias=item.get("alias", ""),
                    filter=filter_decl,
                    input_expression=raw_input_expr,
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
                    input_column=item.get("input_column") or "",
                    partition_by=item.get("partition_by", []),
                    order_by=item.get("order_by", []),
                    alias=item.get("alias", ""),
                    confidence=item.get("confidence", "medium"),
                )
            )

        # 解析维度，并限定为输出列与已声明源字段的交集
        output_names = {col.name for col in spec.output_spec.columns}
        source_columns: dict[str, set[str]] = {}
        for table in spec.input_tables:
            columns = {
                col.column_name
                for group in (table.columns, table.key_columns, table.business_columns)
                for col in group
            }
            source_columns[table.table_alias] = columns

        for item in raw.get("inferred_dimensions", []):
            dimension_name = item.get("dimension_name", "")
            column_ref = item.get("column_ref", "")
            source_table = item.get("source_table")
            if dimension_name not in output_names:
                continue
            if source_table:
                if column_ref not in source_columns.get(source_table, set()):
                    continue
            else:
                candidates = [
                    alias for alias, columns in source_columns.items()
                    if column_ref in columns
                ]
                if len(candidates) != 1:
                    continue
                source_table = candidates[0]
            inferred_dimensions.append(DimensionDecl(
                dimension_name=dimension_name,
                column_ref=column_ref,
                source_table=source_table,
            ))

        # 窗口后过滤只能引用本次已验证的窗口 alias
        window_aliases = {window.alias for window in inferred_window}
        for item in raw.get("inferred_post_window_filters", []):
            if item.get("column") not in window_aliases:
                continue
            try:
                inferred_post_window_filters.append(PostWindowFilterDecl(
                    column=item["column"],
                    operator=CompareOp(item["operator"]),
                    value=item["value"],
                ))
            except (KeyError, TypeError, ValueError):
                continue

        # 解析计算指标
        for item in raw.get("inferred_computed_metrics", []):
            expr = item.get("expression", "")
            # 双重校验：入站层 + 编译器层（白名单正则 + SQL 关键字拒绝）
            is_valid, _ = validate_input_expression(expr, mode="silent")
            is_valid_c, _ = validate_input_expression(expr, mode="compiler")
            if not is_valid or not is_valid_c:
                if not is_valid:
                    logger.warning(
                        "LLM 产出计算表达式 '%s' 含禁止字符/模式，已丢弃",
                        expr,
                    )
                else:
                    logger.warning(
                        "LLM 产出计算表达式 '%s' 含 SQL 关键字或非法字符（compiler 拒绝），已丢弃",
                        expr,
                    )
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

        # ── 解析 CASE WHEN 规则（H11）──
        case_when_rules: list[dict] = []
        unresolved_case_when: list[dict] = []

        raw_case_when = raw.get("inferred_case_when", [])
        if raw_case_when:
            from tianshu_datadev.developer_spec.models import (
                LabelBranchProposalOutput,
                LabelDomainOutput,
                LabelRuleProposalOutput,
            )
            from tianshu_datadev.labels.artifacts import LabelExtractionArtifact
            from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator
            from tianshu_datadev.labels.promotion import Promotion

            validator = LabelRuleValidator()
            promoter = Promotion()

            for item in raw_case_when:
                output_col = item.get("output_column", "unknown")
                # 尝试解析 LLM 输出为 LabelRuleProposalOutput——
                # Pydantic discriminator 自动拒绝 LITERAL/COLUMN_REF 根条件
                try:
                    branches_output = [
                        LabelBranchProposalOutput(
                            condition=b["condition"],
                            then_label=b["then_label"],
                            evidence=b.get("evidence", ""),
                        )
                        for b in item.get("branches", [])
                    ]
                    domain_output = None
                    if item.get("label_domain"):
                        domain_output = LabelDomainOutput(
                            values=item["label_domain"].get("values", []),
                            source_evidence=item["label_domain"].get("source_evidence", ""),
                            is_exhaustive=item["label_domain"].get("is_exhaustive", False),
                            completeness_evidence=item["label_domain"].get("completeness_evidence", ""),
                        )
                    llm_output = LabelRuleProposalOutput(
                        output_column=item["output_column"],
                        branches=branches_output,
                        else_value=item["else_value"],
                        label_domain=domain_output,
                    )
                except Exception as exc:
                    logger.warning(
                        "CASE WHEN 规则解析失败（条件 AST 非法或结构不完整）——%s: %s",
                        output_col, exc,
                    )
                    unresolved_case_when.append({
                        "output_column": output_col,
                        "reason": f"条件 AST 非法: {exc}",
                    })
                    continue

                # 包装为 LabelRuleProposal——注入系统字段
                proposal = self._wrap_case_when_proposal(llm_output, spec.spec_hash)
                if proposal is None:
                    unresolved_case_when.append({
                        "output_column": output_col,
                        "reason": "包装失败——缺少必需字段",
                    })
                    continue

                # 确定性校验——六项检查（FIELD_EXISTS / TYPE_COMPATIBLE / OPERATOR_VALID
                # / AST_VALID / LABEL_DOMAIN / COVERAGE / NO_LABEL_NOT）
                report = validator.validate(proposal, spec)
                if not report.passed:
                    logger.warning(
                        "CASE WHEN 规则校验未通过——%s: blocking=%s, human_review=%s",
                        proposal.output_column,
                        report.blocking_errors,
                        report.human_review_items,
                    )
                    unresolved_case_when.append({
                        "output_column": proposal.output_column,
                        "blocking_errors": report.blocking_errors,
                        "human_review_items": report.human_review_items,
                    })
                    continue

                # 双空门禁通过 → 提升为 CaseWhenDecl
                extraction_artifact = LabelExtractionArtifact(
                    artifact_id=f"ext_{uuid.uuid4().hex[:12]}",
                    source_spec_hash=spec.spec_hash,
                    extraction_time=datetime.now(timezone.utc).isoformat(),
                    llm_model="",
                    llm_prompt_version="",
                    llm_temperature=0.1,
                    unresolved_columns=[],
                    raw_proposals=[proposal],
                    prompt_snapshot="",
                )
                promoted, prom_artifact = promoter.promote(
                    spec.spec_hash, [proposal], [report], extraction_artifact,
                )
                if promoted:
                    for cw in promoted:
                        # 从 LLM 输出注入 evaluation_phase——
                        # Promotion 不负责阶段判定，阶段信息来自 H11 LLM 输出
                        cw.evaluation_phase = item.get("evaluation_phase")
                        case_when_rules.append(cw.model_dump(mode="json"))
                if prom_artifact.human_review_required:
                    unresolved_case_when.append({
                        "output_column": proposal.output_column,
                        "reason": "人工审核需要——Promotion 阶段标记",
                        "rejected_proposals": prom_artifact.rejected_proposals,
                    })

        # Agent 可以解释业务语义，但不应独占显然的字段事实判断。
        # 输出列与唯一源字段同名时，确定性补为维度，避免模型漏项导致
        # 聚合查询丢失 GROUP BY。指标、窗口和计算列始终优先排除。
        semantic_aliases = {
            metric.alias for metric in spec.metrics
        } | {
            metric.alias for metric in inferred_metrics
        } | {
            metric.alias for metric in inferred_window
        } | {
            metric.alias for metric in inferred_computed
        }
        existing_dimension_names = {
            dimension.dimension_name for dimension in spec.dimensions
        } | {
            dimension.dimension_name for dimension in inferred_dimensions
        }
        for dimension in self._fake._infer_dimensions(
            spec,
            None,
            excluded_output_names=semantic_aliases,
        ):
            if dimension.dimension_name not in existing_dimension_names:
                inferred_dimensions.append(dimension)
                existing_dimension_names.add(dimension.dimension_name)

        return EnrichedSpec(
            original_spec=spec,
            inferred_metrics=inferred_metrics,
            inferred_window_metrics=inferred_window,
            inferred_post_window_filters=inferred_post_window_filters,
            inferred_computed_metrics=inferred_computed,
            inferred_dimensions=inferred_dimensions,
            enrichment_metadata={
                "source": "SpecEnricher",
                "method": "llm",
                "raw_response_keys": list(raw.keys()),
                "case_when_rules": case_when_rules,
                "unresolved_case_when": unresolved_case_when,
                "inferred_case_when_count": len(case_when_rules),
            },
        )
