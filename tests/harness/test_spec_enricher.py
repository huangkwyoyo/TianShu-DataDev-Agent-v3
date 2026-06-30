"""SpecEnricher tests - Phase 4D metric inference layer.

Covers:
1. FakeSpecEnricher rule-based inference
2. Hand-declared metric protection (no overwrite)
3. EnrichedSpec serialization
4. MetricDecl extended fields (filter/input_expression/distinct)
5. E2E: Parser -> Enricher -> Builder -> Compiler
6. LLM Prompt 8-boundary integrity
7. _parse_llm_response validation logic
"""

from __future__ import annotations

import pytest

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    EnrichedSpec,
    MetricDecl,
    MetricFilterDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.spec_enricher import (
    FakeSpecEnricher,
    SpecEnricher,
    _METRIC_INFERENCE_SYSTEM_PROMPT,
    _infer_aggregation_type,
)
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.api.pipeline import _build_manifest


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def sample_spec() -> ParsedDeveloperSpec:
    """Basic spec with 1 hand-declared metric + 2 output columns."""
    md = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test
  target_grain: [stat_date]
  summary: "按日期统计页面浏览量和独立访客数"
  source_tables:
    - name: dwd.user_events
      alias: ue
      row_count: ~1000万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: stat_date
          type: date
          nullable: false
        - name: user_id
          type: bigint
          nullable: false
        - name: event_type
          type: varchar
          nullable: true
  metrics:
    - metric_name: pv
      aggregation: COUNT
      input_column: id
      alias: pv
  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
  output_columns:
    - name: stat_date
      type: date
    - name: pv
      type: bigint
    - name: uv
      type: bigint
---
# daily PV and UV stats with dedup users
```"""
    parser = DeveloperSpecParser()
    return parser.parse(md)


@pytest.fixture
def sample_spec_with_filter() -> ParsedDeveloperSpec:
    """Spec with conditional aggregation need: fined plate count."""
    md = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.fine_stats
  target_grain: [stat_date]
  summary: "daily fined plate count stats with standard fine filter"
  source_tables:
    - name: dwd.parking_fines
      alias: pf
      row_count: ~500万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: stat_date
          type: date
          nullable: false
        - name: plate_id
          type: varchar
          nullable: false
        - name: fine_status
          type: varchar
          nullable: true
        - name: fine_amount
          type: decimal(10,2)
          nullable: true
  metrics:
    - metric_name: record_count
      aggregation: COUNT
      input_column: id
      alias: record_count
  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
  output_columns:
    - name: stat_date
      type: date
    - name: record_count
      type: bigint
    - name: fined_plate_count
      type: bigint
---
# parking fine stats with conditional dedup count
```"""
    parser = DeveloperSpecParser()
    return parser.parse(md)


@pytest.fixture
def sample_manifest(sample_spec: ParsedDeveloperSpec):
    """Build manifest from sample_spec."""
    return _build_manifest(sample_spec)


# ============================================================
# Keyword inference tests
# ============================================================

class TestKeywordInference:
    """Chinese keyword -> aggregation type mapping."""

    @pytest.mark.parametrize("description,metric_name,expected_agg", [
        # Chinese keyword patterns that match _AGGREGATION_PATTERNS
        ("页面浏览量", "pv", AggregationType.COUNT),
        ("独立访客数", "uv", AggregationType.COUNT_DISTINCT),
        ("去重用户数", "unique_users", AggregationType.COUNT_DISTINCT),
        ("总金额合计", "total_amount", AggregationType.SUM),
        ("销售额求和", "sales_total", AggregationType.SUM),
        ("平均订单金额", "avg_order_amt", AggregationType.AVG),
        ("最大订单金额", "max_order_amt", AggregationType.MAX),
        ("最小价格", "min_price", AggregationType.MIN),
    ])
    def test_aggregation_type_inference(self, description, metric_name, expected_agg):
        """Keyword inference should return correct aggregation type."""
        agg_type, _ = _infer_aggregation_type(description, metric_name)
        assert agg_type == expected_agg

    def test_count_distinct_priority_over_count(self):
        """去重 keyword should match COUNT_DISTINCT before COUNT."""
        agg_type, _ = _infer_aggregation_type("去重用户数", "uv")
        assert agg_type == AggregationType.COUNT_DISTINCT


# ============================================================
# FakeSpecEnricher tests
# ============================================================

class TestFakeSpecEnricher:
    """FakeSpecEnricher end-to-end tests."""

    def test_enrich_returns_enriched_spec(self, sample_spec, sample_manifest):
        """Should return EnrichedSpec with correct metadata."""
        enricher = FakeSpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        assert isinstance(result, EnrichedSpec)
        assert result.original_spec == sample_spec
        assert result.enrichment_metadata["method"] == "rule_based"

    def test_does_not_overwrite_existing_metrics(self, sample_spec, sample_manifest):
        """Hand-declared 'pv' metric should not be overwritten."""
        enricher = FakeSpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        declared_aliases = {m.alias for m in sample_spec.metrics}
        for m in result.inferred_metrics:
            assert m.alias not in declared_aliases, (
                f"Inferred {m.alias} conflicts with declared metric"
            )

    def test_infers_missing_output_metric(self, sample_spec, sample_manifest):
        """'uv' in output_columns but not in metrics should be inferred."""
        enricher = FakeSpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        inferred_aliases = {m.alias for m in result.inferred_metrics}
        assert "uv" in inferred_aliases

    def test_inferred_uv_is_count_distinct(self, sample_spec, sample_manifest):
        """'unique visitors' -> COUNT_DISTINCT."""
        enricher = FakeSpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        uv_metric = next(
            (m for m in result.inferred_metrics if m.alias == "uv"), None
        )
        assert uv_metric is not None
        assert uv_metric.aggregation == AggregationType.COUNT_DISTINCT

    def test_conditional_metric_inference(self, sample_spec_with_filter):
        """'fined_plate_count' should be inferred from output_columns."""
        manifest = _build_manifest(sample_spec_with_filter)
        enricher = FakeSpecEnricher()
        result = enricher.enrich(sample_spec_with_filter, manifest)
        inferred_aliases = {m.alias for m in result.inferred_metrics}
        assert "fined_plate_count" in inferred_aliases


# ============================================================
# MetricDecl extended fields tests
# ============================================================

class TestMetricDeclExtension:
    """MetricDecl filter/input_expression/distinct field tests."""

    def test_metric_with_filter_serialization(self):
        """Conditional aggregation MetricDecl serialization."""
        m = MetricDecl(
            metric_name="fined_plate_count",
            aggregation=AggregationType.COUNT_DISTINCT,
            input_column="plate_id",
            alias="fined_plate_count",
            filter=MetricFilterDecl(
                column="fine_status",
                operator="eq",
                value="STANDARD",
            ),
        )
        d = m.model_dump()
        assert d["filter"]["column"] == "fine_status"
        assert d["filter"]["operator"] == "eq"
        assert d["filter"]["value"] == "STANDARD"

    def test_metric_with_input_expression(self):
        """Multi-field expression aggregation."""
        m = MetricDecl(
            metric_name="total_revenue",
            aggregation=AggregationType.SUM,
            input_column=None,
            input_expression="quantity * unit_price",
            alias="total_revenue",
        )
        d = m.model_dump()
        assert d["input_expression"] == "quantity * unit_price"
        assert d["input_column"] is None

    def test_metric_with_distinct_sum(self):
        """SUM(DISTINCT col) aggregation."""
        m = MetricDecl(
            metric_name="unique_amount_sum",
            aggregation=AggregationType.SUM,
            input_column="amount",
            alias="unique_amount_sum",
            distinct=True,
        )
        assert m.model_dump()["distinct"] is True

    def test_filter_decl_validation(self):
        """MetricFilterDecl basic validation."""
        f = MetricFilterDecl(column="status", operator="eq", value="active")
        assert f.column == "status"
        f2 = MetricFilterDecl(column="deleted_at", operator="is_null", value="")
        assert f2.operator == "is_null"


# ============================================================
# LLM Prompt structure tests
# ============================================================

class TestLLMPromptStructure:
    """LLM Prompt 8-boundary integrity tests."""

    def test_prompt_contains_all_8_boundaries(self):
        """System prompt must contain all 8 hard constraints (H1-H8)."""
        keywords = ["H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8"]
        for kw in keywords:
            assert kw in _METRIC_INFERENCE_SYSTEM_PROMPT, (
                f"Prompt missing boundary: {kw}"
            )

    def test_prompt_has_output_schema(self):
        """Prompt must contain JSON Schema output format."""
        assert "inferred_metrics" in _METRIC_INFERENCE_SYSTEM_PROMPT
        assert "inferred_window_metrics" in _METRIC_INFERENCE_SYSTEM_PROMPT
        assert "inferred_computed_metrics" in _METRIC_INFERENCE_SYSTEM_PROMPT
        assert "confidence" in _METRIC_INFERENCE_SYSTEM_PROMPT

    def test_prompt_mentions_6_aggregation_types(self):
        """Prompt must list all 6 supported aggregation functions."""
        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"]:
            assert func in _METRIC_INFERENCE_SYSTEM_PROMPT, (
                f"Prompt missing aggregation: {func}"
            )


# ============================================================
# LLM response parsing tests
# ============================================================

class TestLLMResponseParsing:
    """SpecEnricher._parse_llm_response validation tests."""

    @pytest.fixture
    def enricher(self):
        """Create SpecEnricher without LLM client (fallback to Fake)."""
        return SpecEnricher()

    def test_parse_valid_llm_response(self, enricher, sample_spec):
        """Valid LLM response should parse correctly."""
        raw = {
            "inferred_metrics": [
                {
                    "metric_name": "uv",
                    "aggregation": "COUNT_DISTINCT",
                    "input_column": "user_id",
                    "alias": "uv",
                    "filter": None,
                    "input_expression": None,
                    "distinct": False,
                    "confidence": "high",
                    "reasoning": "dedup users",
                }
            ],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
        }
        result = enricher._parse_llm_response(raw, sample_spec)
        assert len(result.inferred_metrics) == 1
        assert result.inferred_metrics[0].aggregation == AggregationType.COUNT_DISTINCT

    def test_parse_llm_response_with_filter(self, enricher, sample_spec):
        """LLM response with filter should parse correctly."""
        raw = {
            "inferred_metrics": [
                {
                    "metric_name": "fined",
                    "aggregation": "COUNT_DISTINCT",
                    "input_column": "plate_id",
                    "alias": "fined",
                    "filter": {
                        "column": "fine_status",
                        "operator": "eq",
                        "value": "STANDARD",
                    },
                    "input_expression": None,
                    "distinct": False,
                    "confidence": "high",
                    "reasoning": "filtered count",
                }
            ],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
        }
        result = enricher._parse_llm_response(raw, sample_spec)
        m = result.inferred_metrics[0]
        assert m.filter is not None
        assert m.filter.column == "fine_status"
        assert m.filter.operator == "eq"

    def test_invalid_aggregation_is_filtered(self, enricher, sample_spec):
        """Illegal aggregation function (H2 violation) should be filtered out."""
        raw = {
            "inferred_metrics": [
                {
                    "metric_name": "med",
                    "aggregation": "MEDIAN",  # illegal!
                    "input_column": "amount",
                    "alias": "med",
                    "filter": None,
                    "input_expression": None,
                    "distinct": False,
                    "confidence": "medium",
                    "reasoning": "median",
                },
                {
                    "metric_name": "total",
                    "aggregation": "SUM",
                    "input_column": "amount",
                    "alias": "total",
                    "filter": None,
                    "input_expression": None,
                    "distinct": False,
                    "confidence": "high",
                    "reasoning": "sum",
                },
            ],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
        }
        result = enricher._parse_llm_response(raw, sample_spec)
        assert len(result.inferred_metrics) == 1
        assert result.inferred_metrics[0].metric_name == "total"

    def test_parse_window_metrics(self, enricher, sample_spec):
        """Window metrics should parse correctly."""
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [
                {
                    "metric_name": "rank",
                    "window_function": "ROW_NUMBER",
                    "input_column": "amount",
                    "partition_by": ["dt"],
                    "order_by": ["amount DESC"],
                    "alias": "rank",
                    "confidence": "medium",
                    "reasoning": "ranking",
                }
            ],
            "inferred_computed_metrics": [],
        }
        result = enricher._parse_llm_response(raw, sample_spec)
        assert len(result.inferred_window_metrics) == 1
        assert result.inferred_window_metrics[0].window_function == "ROW_NUMBER"

    def test_parse_computed_metrics(self, enricher, sample_spec):
        """Computed metrics should parse correctly."""
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [
                {
                    "metric_name": "rate",
                    "expression": "a / b",
                    "depends_on": ["a", "b"],
                    "alias": "rate",
                    "confidence": "medium",
                    "reasoning": "ratio",
                }
            ],
        }
        result = enricher._parse_llm_response(raw, sample_spec)
        assert len(result.inferred_computed_metrics) == 1
        assert result.inferred_computed_metrics[0].expression == "a / b"


# ============================================================
# E2E integration tests
# ============================================================

class TestEndToEndEnricherPipeline:
    """Parser -> Enricher -> Builder -> Compiler full pipeline."""

    def test_enriched_spec_compiles_to_valid_sql(self, sample_spec, sample_manifest):
        """Enriched spec should produce valid SQL with COUNT(DISTINCT ...)."""
        enricher = FakeSpecEnricher()
        enriched = enricher.enrich(sample_spec, sample_manifest)

        # Merge inferred metrics
        declared_aliases = {m.alias for m in sample_spec.metrics}
        new_metrics = [
            m for m in enriched.inferred_metrics
            if m.alias not in declared_aliases
        ]
        combined = list(sample_spec.metrics) + new_metrics
        enriched_spec = sample_spec.model_copy(update={"metrics": combined})

        # Build plan
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(enriched_spec)
        agg_steps = [s for s in plan.steps if s.step_type == "aggregate"]
        assert len(agg_steps) == 1
        metric_aliases = [str(m.alias) for m in agg_steps[0].metrics]
        assert "uv" in metric_aliases

        # Compile
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)
        sql = compiled.sql
        assert "COUNT(DISTINCT" in sql
        assert "uv" in sql

    def test_filter_metric_in_compiled_sql(self):
        """FILTER clause should appear in compiled SQL."""
        from tianshu_datadev.planning.models import AggregateSpec
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            ColumnRef,
            ScanStep,
            SqlBuildPlan,
        )

        plan = SqlBuildPlan(
            plan_id="test_filter_plan",
            spec_hash="hash001",
            steps=[
                ScanStep(
                    step_id="s1",
                    step_type="scan",
                    table_ref="pf",
                    required_columns=[
                        ColumnRef(table_ref="pf", column_name="stat_date", normalized_name="stat_date"),
                        ColumnRef(table_ref="pf", column_name="plate_id", normalized_name="plate_id"),
                        ColumnRef(table_ref="pf", column_name="fine_status", normalized_name="fine_status"),
                    ],
                ),
                AggregateStep(
                    step_id="a1",
                    step_type="aggregate",
                    group_keys=[
                        ColumnRef(table_ref="pf", column_name="stat_date", normalized_name="stat_date"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation=AggregationType.COUNT_DISTINCT,
                            input_column="plate_id",
                            alias="fined_plate_count",
                            filter=MetricFilterDecl(
                                column="fine_status",
                                operator="eq",
                                value="STANDARD",
                            ),
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)
        sql = compiled.sql
        assert "FILTER (WHERE" in sql
        assert "fine_status = 'STANDARD'" in sql
        assert "COUNT(DISTINCT plate_id)" in sql


# ============================================================
# Pipeline._apply_enrichment tests
# ============================================================

class TestApplyEnrichment:
    """Pipeline._apply_enrichment static method tests."""

    def test_applies_enrichment_to_spec(self, sample_spec, sample_manifest):
        """Should add inferred 'uv' metric to spec."""
        from tianshu_datadev.api.pipeline import FakePipeline
        enricher = FakeSpecEnricher()
        result = FakePipeline._apply_enrichment(
            sample_spec, sample_manifest, enricher
        )
        assert "uv" in {m.alias for m in result.metrics}

    def test_no_duplicate_alias(self, sample_spec, sample_manifest):
        """Inferred metrics with conflicting aliases should not duplicate."""
        from tianshu_datadev.api.pipeline import FakePipeline
        enricher = FakeSpecEnricher()
        result = FakePipeline._apply_enrichment(
            sample_spec, sample_manifest, enricher
        )
        pv_count = sum(1 for m in result.metrics if m.alias == "pv")
        assert pv_count == 1
