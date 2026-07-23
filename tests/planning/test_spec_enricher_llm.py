"""SpecEnricher._parse_llm_response() Fixture 测试。

验证 LLM JSON 输出 → EnrichedSpec 的解析和校验逻辑。
覆盖全部 5 项校验规则：H2(聚合函数枚举)、H3(filter 合法性)、
窗口/计算指标解析、空列表/缺键容错、混合合法/非法。

所有测试使用 JSON fixture 文件模拟 LLM 输出，不依赖网络或 API Key。
"""

from __future__ import annotations

import json
import os

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    ColumnDecl,
    DimensionDecl,
    EnrichedSpec,
    InputTableDecl,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.planning.spec_enricher import SpecEnricher

# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _read_fixture(name: str) -> dict:
    """读取 llm_responses/enricher/ 下的 JSON fixture。"""
    path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "llm_responses", "enricher", name,
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_minimal_spec() -> ParsedDeveloperSpec:
    """构造最小合法 ParsedDeveloperSpec——用于测试 _parse_llm_response。

    包含一张表 orders(id, amount, status, user_id, order_date)，
    一个已有指标 total_amount。
    """
    return ParsedDeveloperSpec(
        spec_id="test_spec",
        spec_hash="abc123",
        title="测试需求书",
        description="测试用的最小 DeveloperSpec",
        input_tables=[
            InputTableDecl(
                table_alias="orders",
                source_table="test.orders",
                columns=[
                    ColumnDecl(column_name="id", normalized_name="id", data_type="int"),
                    ColumnDecl(column_name="amount", normalized_name="amount", data_type="decimal"),
                    ColumnDecl(column_name="status", normalized_name="status", data_type="varchar"),
                    ColumnDecl(column_name="user_id", normalized_name="user_id", data_type="int"),
                    ColumnDecl(column_name="order_date", normalized_name="order_date", data_type="date"),
                ],
            ),
        ],
        metrics=[
            MetricDecl(
                metric_name="total_amount",
                aggregation=AggregationType.SUM,
                input_column="amount",
                alias="total_amount",
            ),
        ],
        dimensions=[
            DimensionDecl(dimension_name="stat_date", column_ref="order_date"),
        ],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name="total_amount")],
            grain=["stat_date"],
        ),
    )


# ════════════════════════════════════════════
# 测试类
# ════════════════════════════════════════════


class TestSpecEnricherParseLLM:
    """验证 _parse_llm_response 对各种 LLM JSON 输出的处理。"""

    # ── 正常路径 ──

    def test_parse_normal_all_three_types(self):
        """正常 JSON——三种推断类型全部正确解析。"""
        enricher = SpecEnricher()
        raw = _read_fixture("normal.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert isinstance(result, EnrichedSpec)
        assert result.original_spec == spec
        # 指标
        assert len(result.inferred_metrics) == 2
        assert result.inferred_metrics[0].metric_name == "total_amount"
        assert result.inferred_metrics[0].aggregation == AggregationType.SUM
        assert result.inferred_metrics[0].input_column == "amount"
        assert result.inferred_metrics[1].metric_name == "order_count"
        assert result.inferred_metrics[1].aggregation == AggregationType.COUNT
        assert result.inferred_metrics[1].input_column is None  # COUNT(*)
        # 窗口指标
        assert len(result.inferred_window_metrics) == 1
        assert result.inferred_window_metrics[0].metric_name == "amount_rank"
        assert result.inferred_window_metrics[0].window_function == "RANK"
        assert result.inferred_window_metrics[0].partition_by == ["user_id"]
        assert result.inferred_window_metrics[0].order_by == ["amount DESC"]
        # 计算指标
        assert len(result.inferred_computed_metrics) == 1
        assert result.inferred_computed_metrics[0].metric_name == "conversion_rate"
        assert result.inferred_computed_metrics[0].depends_on == ["paid_count", "total_count"]
        # 元数据
        assert result.enrichment_metadata["source"] == "SpecEnricher"
        assert result.enrichment_metadata["method"] == "llm"

    # ── H2：非法聚合函数 → 丢弃 ──

    def test_parse_rejects_invalid_aggregation(self):
        """aggregation="MEDIAN" 不在枚举中——静默丢弃该项。"""
        enricher = SpecEnricher()
        raw = _read_fixture("invalid_aggregation.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        # MEDIAN 应被丢弃，SUM 应保留
        assert len(result.inferred_metrics) == 1
        assert result.inferred_metrics[0].metric_name == "total_amount"
        assert result.inferred_metrics[0].aggregation == AggregationType.SUM

    def test_parse_rejects_missing_aggregation(self):
        """aggregation 字段完全缺失——静默丢弃。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [
                {
                    "metric_name": "bad_metric",
                    "input_column": "amount",
                    "alias": "bad_metric",
                    "confidence": "medium",
                }
            ],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_metrics) == 0

    # ── H3：合法 filter → 保留 ──

    def test_parse_valid_filter_preserved(self):
        """合法 filter（字段存在 + 结构完整）——保留 filter。"""
        enricher = SpecEnricher()
        raw = _read_fixture("invalid_filter.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_metrics) == 3
        # 第一个指标——合法 filter
        assert result.inferred_metrics[0].metric_name == "active_orders"
        assert result.inferred_metrics[0].filter is not None
        assert result.inferred_metrics[0].filter.column == "status"
        assert result.inferred_metrics[0].filter.operator == "eq"

    # ── H3：filter 结构不完整 → 丢弃 filter，保留指标 ──

    def test_parse_drops_malformed_filter_keeps_metric(self):
        """filter 缺少 operator 和 value——丢弃 filter，保留指标。"""
        enricher = SpecEnricher()
        raw = _read_fixture("invalid_filter.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        # 第三个指标——filter 结构不完整，应丢弃 filter
        malformed = result.inferred_metrics[2]
        assert malformed.metric_name == "malformed_filter"
        assert malformed.filter is None, "不合法的 filter 应被丢弃"

    # ── 窗口指标解析 ──

    def test_parse_window_metrics_only(self):
        """仅含窗口指标——正确解析。"""
        enricher = SpecEnricher()
        raw = _read_fixture("window_metrics.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_metrics) == 0
        assert len(result.inferred_computed_metrics) == 0
        assert len(result.inferred_window_metrics) == 2
        assert result.inferred_window_metrics[0].window_function == "ROW_NUMBER"
        assert result.inferred_window_metrics[1].window_function == "LAG"

    def test_parse_window_metrics_default_empty_lists(self):
        """窗口指标缺少 partition_by/order_by——使用默认空列表。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "simple_rank",
                    "window_function": "ROW_NUMBER",
                    "input_column": "amount",
                    "alias": "simple_rank",
                    "confidence": "medium",
                }
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 1
        assert result.inferred_window_metrics[0].partition_by == []
        assert result.inferred_window_metrics[0].order_by == []

    # ── 计算指标解析 ──

    def test_parse_computed_metrics_only(self):
        """仅含计算指标——正确解析。"""
        enricher = SpecEnricher()
        raw = _read_fixture("computed_metrics.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_metrics) == 0
        assert len(result.inferred_window_metrics) == 0
        assert len(result.inferred_computed_metrics) == 2
        assert result.inferred_computed_metrics[0].metric_name == "avg_order_amount"
        assert result.inferred_computed_metrics[0].expression == "total_amount / order_count"

    def test_parse_computed_metrics_default_depends_on(self):
        """计算指标缺少 depends_on——使用默认空列表。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "simple_ratio",
                    "expression": "a / b",
                    "alias": "simple_ratio",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 1
        assert result.inferred_computed_metrics[0].depends_on == []

    # ── 空列表 ──

    def test_parse_empty_all_lists(self):
        """所有列表为空——返回空 EnrichedSpec。"""
        enricher = SpecEnricher()
        raw = _read_fixture("empty_all.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert result.inferred_metrics == []
        assert result.inferred_window_metrics == []
        assert result.inferred_computed_metrics == []
        assert result.original_spec == spec

    # ── 混合合法/非法 ──

    def test_parse_mixed_valid_invalid(self):
        """混合合法/非法——合法保留，非法丢弃，旧名映射。"""
        enricher = SpecEnricher()
        raw = _read_fixture("mixed_valid_invalid.json")
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        # PERCENTILE 被丢弃，保留 SUM 和 COUNT
        assert len(result.inferred_metrics) == 2
        metric_names = [m.metric_name for m in result.inferred_metrics]
        assert "total_amount" in metric_names
        assert "order_count" in metric_names
        assert "invalid_agg" not in metric_names
        # 窗口指标：PERCENT_RANK 被白名单拒绝，SUM→SUM_OVER 映射后保留，DENSE_RANK 保留
        assert len(result.inferred_window_metrics) == 2
        window_names = [m.metric_name for m in result.inferred_window_metrics]
        assert "valid_rank" in window_names
        assert "legacy_sum" in window_names
        assert "invalid_window_func" not in window_names
        # 验证旧名映射：legacy_sum 的 window_function 应为 SUM_OVER
        legacy = next(m for m in result.inferred_window_metrics if m.metric_name == "legacy_sum")
        assert legacy.window_function == "SUM_OVER"
        # 计算指标：SQL 注入表达式被拒绝，保留合法表达式
        assert len(result.inferred_computed_metrics) == 1
        assert result.inferred_computed_metrics[0].metric_name == "conversion_rate"

    # ── 缺少键 ──

    def test_parse_missing_keys_returns_empty_lists(self):
        """JSON 不含任何指标键——对应列表为空。"""
        enricher = SpecEnricher()
        raw: dict = {}
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert result.inferred_metrics == []
        assert result.inferred_window_metrics == []
        assert result.inferred_computed_metrics == []

    # ════════════════════════════════════════════
    # 窗口函数白名单校验（Phase 5 新增）
    # ════════════════════════════════════════════

    def test_parse_window_function_rejects_percent_rank(self):
        """PERCENT_RANK 不在白名单——静默丢弃。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "bad_rank",
                    "window_function": "PERCENT_RANK",
                    "input_column": "amount",
                    "alias": "bad_rank",
                    "confidence": "low",
                }
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 0

    def test_parse_window_function_rejects_empty_string(self):
        """空字符串不在白名单——静默丢弃。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "empty_func",
                    "window_function": "",
                    "input_column": "amount",
                    "alias": "empty_func",
                    "confidence": "low",
                }
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 0

    def test_parse_window_function_maps_sum_to_sum_over(self):
        """旧名 SUM → 映射为 SUM_OVER。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "running_total",
                    "window_function": "SUM",
                    "input_column": "amount",
                    "partition_by": ["user_id"],
                    "order_by": ["order_date"],
                    "alias": "running_total",
                    "confidence": "high",
                }
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 1
        assert result.inferred_window_metrics[0].window_function == "SUM_OVER"

    def test_parse_window_function_maps_avg_to_avg_over(self):
        """旧名 AVG → 映射为 AVG_OVER。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "moving_avg",
                    "window_function": "AVG",
                    "input_column": "amount",
                    "partition_by": ["user_id"],
                    "order_by": ["order_date"],
                    "alias": "moving_avg",
                    "confidence": "high",
                }
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 1
        assert result.inferred_window_metrics[0].window_function == "AVG_OVER"

    def test_parse_window_function_allows_all_nine_valid(self):
        """全部 9 种合法窗口函数——全部通过。"""
        enricher = SpecEnricher()
        valid_funcs = [
            "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
            "LAG", "LEAD", "SUM_OVER", "AVG_OVER", "COUNT_OVER",
        ]
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": f"wf_{wf}",
                    "window_function": wf,
                    "input_column": "amount",
                    "alias": f"wf_{wf}",
                    "confidence": "high",
                }
                for wf in valid_funcs
            ],
            "inferred_computed_metrics": [],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_window_metrics) == 9
        passed_funcs = {m.window_function for m in result.inferred_window_metrics}
        assert passed_funcs == set(valid_funcs)

    # ════════════════════════════════════════════
    # expression 安全校验（Phase 5 新增）
    # ════════════════════════════════════════════

    def test_parse_rejects_sql_injection_semicolon(self):
        """expression 含分号——拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "injected",
                    "expression": "1; DROP TABLE users; --",
                    "alias": "injected",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    def test_parse_rejects_sql_injection_quote(self):
        """expression 含单引号——拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "injected",
                    "expression": "x' OR '1'='1",
                    "alias": "injected",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    def test_parse_rejects_sql_injection_backtick(self):
        """expression 含反引号——拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "injected",
                    "expression": "x`; DELETE FROM orders`",
                    "alias": "injected",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    def test_parse_rejects_sql_comment_double_dash(self):
        """expression 含 SQL 注释 -- ——拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "injected",
                    "expression": "a / b -- 注释掉后半段",
                    "alias": "injected",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    def test_parse_rejects_sql_comment_slash_star(self):
        """expression 含 SQL 注释 /* ——拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "injected",
                    "expression": "a /* 恶意注释 */ / b",
                    "alias": "injected",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    def test_parse_allows_legitimate_expression(self):
        """合法算术表达式——通过。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "ratio",
                    "expression": "paid_count / total_count",
                    "alias": "ratio",
                    "confidence": "high",
                },
                {
                    "metric_name": "revenue",
                    "expression": "quantity * unit_price",
                    "alias": "revenue",
                    "confidence": "high",
                },
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 2
        assert result.inferred_computed_metrics[0].expression == "paid_count / total_count"
        assert result.inferred_computed_metrics[1].expression == "quantity * unit_price"

    def test_parse_rejects_empty_expression(self):
        """空 expression——被拒绝（expression 应有实际内容）。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "no_expr",
                    "expression": "",
                    "alias": "no_expr",
                    "confidence": "low",
                }
            ],
        }
        spec = _build_minimal_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert len(result.inferred_computed_metrics) == 0

    # ════════════════════════════════════════════
    # CASE WHEN 推断测试（H11——Phase 8 替代）
    # ════════════════════════════════════════════

    def _build_case_when_spec(self) -> ParsedDeveloperSpec:
        """构造含 varchar 输出列和业务描述的 Spec——用于 CASE WHEN 测试。

        含 pickup_hour (int) 列，业务描述含 "高峰定义：7-10、17-20 为高峰，其余为平峰"。
        """
        return ParsedDeveloperSpec(
            spec_id="test_cw",
            spec_hash="cw123",
            title="高峰时段标签",
            description="高峰定义：7-10、17-20 为高峰，其余为平峰",
            input_tables=[
                InputTableDecl(
                    table_alias="trips",
                    source_table="test.trips",
                    columns=[
                        ColumnDecl(column_name="pickup_at", normalized_name="pickup_at",
                                   data_type="timestamp"),
                        ColumnDecl(column_name="pickup_hour", normalized_name="pickup_hour",
                                   data_type="int"),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="peak_type", type="varchar")],
                grain=["pickup_hour"],
            ),
        )

    # ── 正常路径：多分支 + AND/OR ──

    def test_case_when_multi_branch_and_or(self):
        """多分支 CASE WHEN 含 AND/OR——解析成功，case_when_rules 含 1 条规则。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "OR",
                                "children": [
                                    {
                                        "node_type": "AND",
                                        "children": [
                                            {
                                                "node_type": "COMPARE",
                                                "left": "pickup_hour",
                                                "op": ">=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 7,
                                                    "data_type": "number",
                                                },
                                            },
                                            {
                                                "node_type": "COMPARE",
                                                "left": "pickup_hour",
                                                "op": "<=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 10,
                                                    "data_type": "number",
                                                },
                                            },
                                        ],
                                    },
                                    {
                                        "node_type": "AND",
                                        "children": [
                                            {
                                                "node_type": "COMPARE",
                                                "left": "pickup_hour",
                                                "op": ">=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 17,
                                                    "data_type": "number",
                                                },
                                            },
                                            {
                                                "node_type": "COMPARE",
                                                "left": "pickup_hour",
                                                "op": "<=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 20,
                                                    "data_type": "number",
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                            "then_label": "高峰",
                            "evidence": "高峰定义：7-10、17-20 为高峰，其余为平峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert result.enrichment_metadata["inferred_case_when_count"] == 1
        assert len(result.enrichment_metadata["case_when_rules"]) == 1
        cw = result.enrichment_metadata["case_when_rules"][0]
        assert cw["output_column"] == "peak_type"
        assert cw["else_value"] == "平峰"
        assert len(cw["typed_branches"]) == 1
        assert cw["typed_branches"][0]["then_label"] == "高峰"

    # ── 非法列：FIELD_EXISTS 阻断 ──

    def test_case_when_invalid_column_rejected(self):
        """CASE WHEN 引用不存在的列 nonexistent_col——Validator FIELD_EXISTS 阻断。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "nonexistent_col",
                                "op": ">=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": 7,
                                    "data_type": "number",
                                },
                            },
                            "then_label": "高峰",
                            "evidence": "高峰定义：超过7为高峰，其余为平峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        result = enricher._parse_llm_response(raw, spec)

        # 规则应被拒绝——FIELD_EXISTS 阻断
        assert result.enrichment_metadata["inferred_case_when_count"] == 0
        assert len(result.enrichment_metadata["case_when_rules"]) == 0
        assert len(result.enrichment_metadata["unresolved_case_when"]) == 1
        assert result.enrichment_metadata["unresolved_case_when"][0]["output_column"] == "peak_type"
        ucw_str = str(result.enrichment_metadata["unresolved_case_when"][0])
        assert "nonexistent_col" in ucw_str or "未知列" in ucw_str

    # ── 非法谓词：LITERAL 根条件被 Pydantic discriminator 拒绝 ──

    def test_case_when_literal_root_rejected(self):
        """CASE WHEN 使用 LITERAL 作为根条件——Pydantic discriminator 拒绝。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "LITERAL",
                                "value": True,
                                "data_type": "boolean",
                            },
                            "then_label": "高峰",
                            "evidence": "硬编码分支",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        result = enricher._parse_llm_response(raw, spec)

        # 规则应被拒绝——LITERAL 不在 LabelPredicateCondition 的 6 种根类型中
        assert result.enrichment_metadata["inferred_case_when_count"] == 0
        assert len(result.enrichment_metadata["case_when_rules"]) == 0
        assert len(result.enrichment_metadata["unresolved_case_when"]) >= 1
        assert "AST" in str(
            result.enrichment_metadata["unresolved_case_when"][0].get("reason", "")
        )

    # ── LLM 返回空：无 inferred_case_when ──

    def test_case_when_empty_response(self):
        """LLM 未返回 inferred_case_when——metadata 含空列表和 count=0。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
        }
        spec = self._build_case_when_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert result.enrichment_metadata["inferred_case_when_count"] == 0
        assert result.enrichment_metadata["case_when_rules"] == []
        assert result.enrichment_metadata["unresolved_case_when"] == []

    # ── AND 条件（多谓词组合）──

    def test_case_when_and_condition(self):
        """CASE WHEN 含 AND 条件——解析成功，条件保留为 AND 节点。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "AND",
                                "children": [
                                    {
                                        "node_type": "COMPARE",
                                        "left": "pickup_hour",
                                        "op": ">=",
                                        "right": {
                                            "node_type": "LITERAL",
                                            "value": 7,
                                            "data_type": "number",
                                        },
                                    },
                                    {
                                        "node_type": "IS_NOT_NULL",
                                        "column": "pickup_hour",
                                    },
                                ],
                            },
                            "then_label": "高峰",
                            "evidence": "高峰定义：7-10、17-20 为高峰，其余为平峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        result = enricher._parse_llm_response(raw, spec)

        assert result.enrichment_metadata["inferred_case_when_count"] == 1
        cw = result.enrichment_metadata["case_when_rules"][0]
        assert len(cw["typed_branches"]) == 1
        # 验证条件为 AND 节点
        cond = cw["typed_branches"][0]["condition"]
        assert cond["node_type"] == "AND"
        assert len(cond["children"]) == 2

    # ── 集成测试：apply_enrichment 合并 CASE WHEN 到 spec.label_rules ──

    def test_apply_enrichment_merges_case_when_to_label_rules(self):
        """apply_enrichment 将 case_when_rules 合并到 spec.label_rules。"""
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "pickup_hour",
                                "op": ">=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": 7,
                                    "data_type": "number",
                                },
                            },
                            "then_label": "高峰",
                            "evidence": "高峰定义：7-10、17-20 为高峰，其余为平峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        # mock enrich 方法，直接返回解析后的 EnrichedSpec
        class _MockEnricher(SpecEnricher):
            def enrich(self, spec2, manifest):
                return self._parse_llm_response(raw, spec2)

        from tianshu_datadev.developer_spec.models import (
            ManifestColumn,
            ManifestTable,
            SourceManifest,
        )
        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="cw123",
            tables=[
                ManifestTable(
                    table_ref="trips",
                    source_table="test.trips",
                    columns=[
                        ManifestColumn(column_name="pickup_at", normalized_name="pickup_at",
                                       data_type="timestamp"),
                        ManifestColumn(column_name="pickup_hour", normalized_name="pickup_hour",
                                       data_type="int"),
                    ],
                ),
            ],
        )

        mock_enricher = _MockEnricher()
        result_spec = mock_enricher.apply_enrichment(spec, manifest)

        # 验证 label_rules 已合并
        assert len(result_spec.label_rules) == 1
        rule = result_spec.label_rules[0]
        assert rule.output_column == "peak_type"
        assert rule.else_value == "平峰"
        assert len(rule.typed_branches) == 1
        assert rule.typed_branches[0].then_label == "高峰"

    # ── 未覆盖输出列 → OpenQuestion ──

    def test_case_when_unresolved_creates_open_question(self):
        """CASE WHEN 校验未通过时——apply_enrichment 生成 OpenQuestion。"""
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "bad_column",
                                "op": ">=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": 7,
                                    "data_type": "number",
                                },
                            },
                            "then_label": "高峰",
                            "evidence": "高峰定义：超过7为高峰，其余为平峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
        }
        spec = self._build_case_when_spec()

        class _MockEnricher2(SpecEnricher):
            def enrich(self, spec3, manifest):
                return self._parse_llm_response(raw, spec3)

        from tianshu_datadev.developer_spec.models import (
            ManifestColumn,
            ManifestTable,
            SourceManifest,
        )
        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="cw123",
            tables=[
                ManifestTable(
                    table_ref="trips",
                    source_table="test.trips",
                    columns=[
                        ManifestColumn(column_name="pickup_at", normalized_name="pickup_at",
                                       data_type="timestamp"),
                        ManifestColumn(column_name="pickup_hour", normalized_name="pickup_hour",
                                       data_type="int"),
                    ],
                ),
            ],
        )

        mock_enricher = _MockEnricher2()
        result_spec = mock_enricher.apply_enrichment(spec, manifest)

        # 规则未通过，不应进入 label_rules
        assert len(result_spec.label_rules) == 0
        # 应生成 OpenQuestion
        assert len(result_spec.open_questions) >= 1
        q = result_spec.open_questions[0]
        assert q.source == "spec_enricher"
        assert q.field_ref == "peak_type"
        assert "CASE WHEN" in q.description
        assert not q.blocking


class TestResolveEvaluationPhase:
    """验证 _resolve_evaluation_phase 确定性回退判定逻辑。"""

    def test_pre_aggregate_when_condition_cols_are_physical(self):
        """条件列全是源表物理列 → pre_aggregate（派生维度场景）。"""
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl,
            LabelIsNotNull,
            LabelPredicateBranch,
        )

        spec = _build_minimal_spec()
        cw = CaseWhenDecl(
            output_column="peak_type",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelIsNotNull(column="amount"),
                    then_label="高峰",
                ),
            ],
            else_value="平峰",
        )
        result = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert result == "pre_aggregate"

    def test_post_aggregate_when_condition_refs_metric(self):
        """条件引用聚合指标别名 → post_aggregate。"""
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl,
            LabelIsNotNull,
            LabelPredicateBranch,
        )

        spec = _build_minimal_spec()
        cw = CaseWhenDecl(
            output_column="amount_level",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelIsNotNull(column="total_amount"),
                    then_label="高",
                ),
            ],
            else_value="低",
        )
        result = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert result == "post_aggregate"

    def test_pre_aggregate_when_output_in_grain(self):
        """输出列在 grain 中 → pre_aggregate（规则 1 优先）。"""
        from tianshu_datadev.developer_spec.models import CaseWhenDecl

        spec = _build_minimal_spec()
        cw = CaseWhenDecl(
            output_column="stat_date",
            else_value="unknown",
        )
        result = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert result == "pre_aggregate"

    def test_pre_aggregate_when_output_in_dimensions(self):
        """输出列在 dimensions 中 → pre_aggregate（规则 1）。"""
        from tianshu_datadev.developer_spec.models import CaseWhenDecl

        spec = _build_minimal_spec()
        # stat_date 已在 dimensions 中（见 _build_minimal_spec）
        cw = CaseWhenDecl(
            output_column="stat_date",
        )
        result = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert result == "pre_aggregate"

    def test_none_when_cannot_determine(self):
        """条件列既不是物理列也不是聚合指标 → None（HUMAN_REVIEW）。"""
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl,
            LabelIsNotNull,
            LabelPredicateBranch,
        )

        spec = _build_minimal_spec()
        cw = CaseWhenDecl(
            output_column="custom_label",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelIsNotNull(column="unknown_col"),
                    then_label="是",
                ),
            ],
            else_value="否",
        )
        result = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert result is None
