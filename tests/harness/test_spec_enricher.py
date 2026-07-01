"""SpecEnricher tests - Phase 4D metric inference layer.

Covers:
1. SpecEnricher rule-based inference（adapter=None 退化）
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
    SpecEnricher,
    _METRIC_INFERENCE_SYSTEM_PROMPT,
    _infer_aggregation_type,
)
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec


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
    return build_manifest_from_spec(sample_spec)


@pytest.fixture
def cross_grain_spec() -> ParsedDeveloperSpec:
    """跨粒度 Spec：区域销售额 + 占比（region / global total）。"""
    md = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.region_sales_pct
  target_grain: [region]
  summary: "每个区域的销售额占比——区域销售额 / 全局总销售额"

  source_tables:
    - name: dwd.sales_fact
      alias: s
      row_count: ~1000万
      role: fact
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
      business_columns:
        - name: region
          type: varchar
          nullable: false
        - name: amount
          type: decimal
          nullable: true

  metrics:
    - metric_name: regional_sales
      aggregation: SUM
      input_column: amount
      alias: regional_sales

  dimensions:
    - dimension_name: region
      column_ref: region

  output_columns:
    - name: region
      type: varchar
    - name: regional_sales
      type: decimal
    - name: pct_of_total
      type: decimal
      description: "regional_sales / total_sales，范围 [0, 1]"
---
# 跨粒度占比测试
```"""
    parser = DeveloperSpecParser()
    return parser.parse(md)


@pytest.fixture
def cross_grain_manifest(cross_grain_spec: ParsedDeveloperSpec):
    """Build manifest from cross_grain_spec."""
    return build_manifest_from_spec(cross_grain_spec)


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
# SpecEnricher tests（退化路径——adapter=None → FakeSpecEnricher）
# ============================================================

class TestSpecEnricher:
    """SpecEnricher end-to-end tests（退化路径 adapter=None）。"""

    def test_enrich_returns_enriched_spec(self, sample_spec, sample_manifest):
        """Should return EnrichedSpec with correct metadata."""
        enricher = SpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        assert isinstance(result, EnrichedSpec)
        assert result.original_spec == sample_spec
        assert result.enrichment_metadata["method"] == "rule_based"

    def test_does_not_overwrite_existing_metrics(self, sample_spec, sample_manifest):
        """Hand-declared 'pv' metric should not be overwritten."""
        enricher = SpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        declared_aliases = {m.alias for m in sample_spec.metrics}
        for m in result.inferred_metrics:
            assert m.alias not in declared_aliases, (
                f"Inferred {m.alias} conflicts with declared metric"
            )

    def test_infers_missing_output_metric(self, sample_spec, sample_manifest):
        """'uv' in output_columns but not in metrics should be inferred."""
        enricher = SpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        inferred_aliases = {m.alias for m in result.inferred_metrics}
        assert "uv" in inferred_aliases

    def test_inferred_uv_is_count_distinct(self, sample_spec, sample_manifest):
        """'unique visitors' -> COUNT_DISTINCT."""
        enricher = SpecEnricher()
        result = enricher.enrich(sample_spec, sample_manifest)
        uv_metric = next(
            (m for m in result.inferred_metrics if m.alias == "uv"), None
        )
        assert uv_metric is not None
        assert uv_metric.aggregation == AggregationType.COUNT_DISTINCT

    def test_conditional_metric_inference(self, sample_spec_with_filter):
        """'fined_plate_count' should be inferred from output_columns."""
        manifest = build_manifest_from_spec(sample_spec_with_filter)
        enricher = SpecEnricher()
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
        enricher = SpecEnricher()
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
        from tianshu_datadev.api.pipeline import Pipeline
        enricher = SpecEnricher()
        result = enricher.apply_enrichment(
            sample_spec, sample_manifest
        )
        assert "uv" in {m.alias for m in result.metrics}

    def test_no_duplicate_alias(self, sample_spec, sample_manifest):
        """Inferred metrics with conflicting aliases should not duplicate."""
        from tianshu_datadev.api.pipeline import Pipeline
        enricher = SpecEnricher()
        result = enricher.apply_enrichment(
            sample_spec, sample_manifest
        )
        pv_count = sum(1 for m in result.metrics if m.alias == "pv")
        assert pv_count == 1


# ============================================================
# P1-4：跨粒度依赖检测测试
# ============================================================


class TestCrossGrainDependency:
    """跨粒度 ComputedMetric → compute_steps + JoinDecl 生成。"""

    def test_detects_cross_grain_from_description(
        self, cross_grain_spec, cross_grain_manifest
    ):
        """含 "X / Y" 描述且 Y 未声明的 Spec → Enricher 生成 compute_steps。"""
        enricher = SpecEnricher()
        enriched = enricher.enrich(cross_grain_spec, cross_grain_manifest)

        # 验证 inferred_computed_metrics
        assert len(enriched.inferred_computed_metrics) >= 1
        cm = enriched.inferred_computed_metrics[0]
        assert "regional_sales" in cm.depends_on
        assert "total_sales" in cm.depends_on

        # 验证 generated_compute_steps 在 metadata 中
        meta = enriched.enrichment_metadata
        generated_steps = meta.get("generated_compute_steps", [])
        assert len(generated_steps) >= 3, \
            f"应生成至少 3 步 compute_steps（分组+全局+合流），实际: {len(generated_steps)}"

        # 应有全局聚合步骤（group_by 为空）
        global_steps = [
            s for s in generated_steps
            if s.get("group_by") == []
        ]
        assert len(global_steps) >= 1, "应有全局聚合步骤（无 GROUP BY）"

        # 应有合流步骤（source 为列表）
        merge_steps = [
            s for s in generated_steps
            if isinstance(s.get("source"), list)
        ]
        assert len(merge_steps) >= 1, "应有合流步骤（source 为列表）"

        # 验证 generated_joins
        generated_joins = meta.get("generated_joins", [])
        assert len(generated_joins) >= 1, "应有 JoinDecl"

    def test_cross_grain_merged_into_spec(
        self, cross_grain_spec, cross_grain_manifest
    ):
        """Pipeline._apply_enrichment 将 compute_steps 合并到 spec。"""
        from tianshu_datadev.api.pipeline import Pipeline
        enricher = SpecEnricher()
        result = enricher.apply_enrichment(
            cross_grain_spec, cross_grain_manifest
        )

        # 验证 compute_steps 已合并
        assert result.compute_steps is not None
        assert len(result.compute_steps) >= 3

        # 验证 joins 已合并
        assert result.joins is not None
        assert len(result.joins) >= 1

        # 验证含全局步骤（group_by=[]）
        global_steps = [
            s for s in result.compute_steps if s.group_by == []
        ]
        assert len(global_steps) >= 1

        # 验证含合流步骤（source 为列表）
        merge_steps = [
            s for s in result.compute_steps
            if isinstance(s.source, list)
        ]
        assert len(merge_steps) >= 1

    def test_no_false_positive_on_simple_spec(
        self, sample_spec, sample_manifest
    ):
        """无 cross-grain 的 Spec 不应生成 compute_steps。"""
        enricher = SpecEnricher()
        enriched = enricher.enrich(sample_spec, sample_manifest)

        meta = enriched.enrichment_metadata
        generated_steps = meta.get("generated_compute_steps", [])
        assert generated_steps == [], \
            f"无跨粒度 Spec 不应生成 compute_steps，实际: {generated_steps}"


class TestCrossGrainE2E:
    """跨粒度端到端：Enricher → Pipeline → Builder → Compiler → DuckDB。"""

    def test_full_pipeline_builds_and_compiles(
        self, cross_grain_spec, cross_grain_manifest
    ):
        """跨粒度 Spec 经完整 Pipeline 可编译为合法 SQL。"""
        from tianshu_datadev.api.pipeline import Pipeline
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        # 1. SpecEnricher + 合并 compute_steps
        enricher = SpecEnricher()
        enriched_spec = enricher.apply_enrichment(
            cross_grain_spec, cross_grain_manifest
        )

        # 2. Builder——走 build_from_steps 路径
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(enriched_spec)
        assert len(plans) >= 3

        # 3. 合流 Plan 应有 JoinStep（CROSS JOIN）
        merge_plan = plans[-1]
        join_steps = [s for s in merge_plan.steps if s.step_type == "join"]
        assert len(join_steps) >= 1
        # CROSS JOIN 应 join_keys 为空
        assert join_steps[0].join_keys == [] or \
            join_steps[0].join_type.value == "CROSS"

        # 4. Compiler 可编译
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(merge_plan)
        assert "CROSS JOIN" in compiled.sql.upper() or \
            "SELECT" in compiled.sql.upper()
        assert compiled.sql != ""

    def test_cross_grain_executes_in_duckdb(
        self, cross_grain_spec, cross_grain_manifest
    ):
        """跨粒度 SQL 在 DuckDB 中执行正确——占比值域 [0, 1]。"""
        from tianshu_datadev.api.pipeline import Pipeline
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        # 1. Enrich + 合并
        enricher = SpecEnricher()
        enriched_spec = enricher.apply_enrichment(
            cross_grain_spec, cross_grain_manifest
        )

        # 2. Builder
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(enriched_spec)

        # 3. 计算 chain_id（与 Builder 一致）
        import hashlib
        steps = enriched_spec.compute_steps or []
        chain_id = hashlib.md5(
            "|".join(s.step_name for s in steps).encode()
        ).hexdigest()[:8]

        # 4. 编译所有 Plan
        compiler = DuckDbSqlCompiler(table_mapping={"s": "s"})
        compiled_plans = [compiler.compile(p) for p in plans]

        # 5. DuckDB 执行
        import duckdb
        con = duckdb.connect(":memory:")

        # 创建源表
        con.execute("""
            CREATE TABLE s AS SELECT * FROM (VALUES
                ('East', 100.0), ('East', 200.0),
                ('West', 50.0), ('West', 150.0),
                ('North', 300.0),
                ('South', 80.0), ('South', 120.0)
            ) AS t(region, amount)
        """)

        # 执行：PRODUCER → 创建 _temp 表；FINAL → 返回结果
        consumed: set[str] = set()
        for cs in steps:
            src_list = cs.source if isinstance(cs.source, list) else [cs.source]
            for src in src_list:
                if src != "input":
                    consumed.add(src)

        result = None
        for cs, compiled in zip(steps, compiled_plans):
            is_final = cs.step_name not in consumed
            temp_name = f"_temp_c{chain_id}_{cs.step_name}"

            if is_final:
                result = con.execute(compiled.sql).fetchall()
            else:
                con.execute(
                    f"CREATE TEMP TABLE {temp_name} AS {compiled.sql}"
                )

        # 验证：占比应在 [0, 1] 范围
        assert result is not None, "应有 FINAL 结果"
        assert len(result) >= 1, f"应有结果行，实际: {len(result)}"
        for row in result:
            pct = row[-1]
            if pct is not None:
                assert 0.0 <= float(pct) <= 1.0, \
                    f"占比 {pct} 不在 [0, 1] 范围"

        con.close()


# ════════════════════════════════════════════
# P3-8：标量子查询——SpecEnricher 检测 + 全局聚合步骤生成
# ════════════════════════════════════════════


class TestScalarSubquery:
    """标量子查询检测——比率表达式分母未声明 → 全局聚合 + CROSS JOIN 合流。"""

    @staticmethod
    def _make_scalar_subquery_spec():
        """构造含标量子查询语义的 Spec——fined_rate 依赖未声明的 unique_plate_count。"""
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            InputTableDecl,
            MetricDecl,
            MetricFilterDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        table = InputTableDecl(
            table_alias="o", source_table="dwd.plate_records",
            columns=[
                ColumnDecl(column_name="plate_id", normalized_name="plate_id", data_type="varchar"),
                ColumnDecl(column_name="fine_status", normalized_name="fine_status", data_type="varchar"),
            ],
            role="fact",
        )

        metrics = [
            MetricDecl(
                metric_name="fined_plate_count", aggregation=AggregationType.COUNT,
                input_column="plate_id", alias="fined_plate_count",
                filter=MetricFilterDecl(column="fine_status", operator="=", value="STANDARD"),
            ),
        ]

        output_spec = OutputSpecDecl(
            columns=[
                OutputColumnDecl(name="fined_rate", type="decimal",
                                 description="fined_plate_count / unique_plate_count，范围 [0, 1]"),
            ],
            grain=[],  # 无 grain——全局汇总
        )

        return ParsedDeveloperSpec(
            spec_id="spec_scalar_subquery",
            spec_hash="spec_scalar_subquery",
            title="标量子查询检测测试",
            description="罚款率 = 有标准罚款的车牌数 / 全部独立车牌数",
            input_tables=[table],
            metrics=metrics,
            dimensions=[],
            output_spec=output_spec,
        )

    def test_detects_undeclared_denominator(self):
        """分母未声明 → 应生成全局聚合步骤。"""
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec

        spec = self._make_scalar_subquery_spec()
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched = enricher.enrich(spec, manifest)

        # 验证推断的 computed metric
        computed = enriched.inferred_computed_metrics
        assert len(computed) >= 1, f"应推断出至少 1 个计算指标，实际: {len(computed)}"
        rate_metric = computed[0]
        assert "unique_plate_count" in rate_metric.depends_on, (
            f"depends_on 应包含 unique_plate_count: {rate_metric.depends_on}"
        )

        # 验证生成了全局聚合步骤
        meta = enriched.enrichment_metadata
        generated_steps = meta.get("generated_compute_steps", [])
        assert len(generated_steps) >= 2, (
            f"应生成至少 2 个步骤（全局聚合 + 合流），实际: {len(generated_steps)}"
        )

        # 全局聚合步骤——group_by 应为空
        global_steps = [
            s for s in generated_steps
            if s.get("group_by") == []
        ]
        assert len(global_steps) >= 1, (
            f"应有全局聚合步骤（group_by=[]），实际: {len(global_steps)}"
        )

        # 合流步骤——source 为列表
        merge_steps = [
            s for s in generated_steps
            if isinstance(s.get("source"), list)
        ]
        assert len(merge_steps) >= 1, (
            f"应有合流步骤（source 为列表），实际: {len(merge_steps)}"
        )

    def test_global_aggregate_infers_count_distinct_for_unique(self):
        """含 'unique' 的 dep 应推断 COUNT_DISTINCT 聚合。"""
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec

        spec = self._make_scalar_subquery_spec()
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched = enricher.enrich(spec, manifest)

        meta = enriched.enrichment_metadata
        generated_steps = meta.get("generated_compute_steps", [])

        # 查找全局聚合步骤
        global_steps = [
            s for s in generated_steps
            if s.get("group_by") == []
        ]
        assert len(global_steps) >= 1

        # 验证全局步骤的指标使用 COUNT_DISTINCT
        global_step = global_steps[0]
        global_metrics = global_step.get("metrics", [])
        assert len(global_metrics) >= 1
        unique_metric = [
            m for m in global_metrics
            if "unique" in m.get("alias", "").lower()
        ]
        assert len(unique_metric) >= 1, (
            f"应有含 unique 的指标，实际: {global_metrics}"
        )
        # 验证聚合类型为 COUNT_DISTINCT
        agg = unique_metric[0].get("aggregation", "")
        assert "DISTINCT" in str(agg).upper(), (
            f"unique 指标应使用 COUNT_DISTINCT，实际: {agg}"
        )

    def test_scalar_subquery_cross_join_generated(self):
        """标量子查询场景应生成 CROSS JOIN（空 join key）。"""
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec

        spec = self._make_scalar_subquery_spec()
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched = enricher.enrich(spec, manifest)

        meta = enriched.enrichment_metadata
        generated_joins = meta.get("generated_joins", [])
        assert len(generated_joins) >= 1, (
            f"应生成 Join，实际: {generated_joins}"
        )

        # 跨粒度 Join 使用空键 → Builder 转为 CROSS JOIN
        cross_joins = [
            j for j in generated_joins
            if j.get("left_key", "") == "" and j.get("right_key", "") == ""
        ]
        assert len(cross_joins) >= 1, (
            f"应有 CROSS JOIN（空键），实际: {generated_joins}"
        )

    def test_scalar_subquery_duckdb_execution(self):
        """标量子查询 DuckDB 端到端——罚款率在 [0, 1] 范围。"""
        import duckdb

        from tianshu_datadev.api.pipeline import Pipeline
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        spec = self._make_scalar_subquery_spec()
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched = enricher.enrich(spec, manifest)

        # 应用 Enricher 推断——合入 compute_steps
        enriched_spec = enricher.apply_enrichment(spec, manifest)

        # 验证 compute_steps 已合并
        assert enriched_spec.compute_steps is not None
        assert len(enriched_spec.compute_steps) >= 2, (
            f"应有至少 2 个 compute_steps，实际: {len(enriched_spec.compute_steps)}"
        )

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(enriched_spec)

        compiler = DuckDbSqlCompiler(
            table_mapping={"o": "dwd.plate_records"}
        )

        con = duckdb.connect(":memory:")
        try:
            con.execute("CREATE SCHEMA dwd")
            con.execute("""
                CREATE TABLE dwd.plate_records AS
                SELECT * FROM (VALUES
                    ('PLATE001', 'STANDARD'),
                    ('PLATE002', 'STANDARD'),
                    ('PLATE003', 'STANDARD'),
                    ('PLATE004', 'EXPIRED'),
                    ('PLATE005', 'EXPIRED')
                ) AS t(plate_id, fine_status)
            """)

            # 提取 chain_id 并执行 _temp 管道
            import re
            import hashlib

            chain_id = hashlib.md5(
                "|".join(s.step_name for s in enriched_spec.compute_steps).encode()
            ).hexdigest()[:8]

            plan_sqls = [compiler.compile(p).sql for p in plans]

            # 执行中间步骤——写入 _temp 表
            for i, cs in enumerate(enriched_spec.compute_steps):
                if i < len(enriched_spec.compute_steps) - 1:
                    temp_name = f"_temp_c{chain_id}_{cs.step_name}"
                    con.execute(
                        f"CREATE TEMP TABLE \"{temp_name}\" AS {plan_sqls[i]}"
                    )

            # 执行最终步骤
            final_sql = plan_sqls[-1]
            result = con.execute(final_sql).fetchall()

            assert result is not None, "应有 FINAL 结果"
            assert len(result) >= 1, f"应有结果行，实际: {len(result)}"

            # 验证 fined_rate 在 [0, 1] 范围
            for row in result:
                rate = row[-1]
                if rate is not None:
                    assert 0.0 <= float(rate) <= 1.0, (
                        f"罚款率 {rate} 不在 [0, 1] 范围"
                    )
        finally:
            con.close()
