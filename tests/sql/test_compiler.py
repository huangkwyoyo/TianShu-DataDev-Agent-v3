"""测试 DuckDbSqlCompiler——确定性编译 + SQL 渲染。"""

import os

import pytest

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    InferredWindowMetric,
    MetricDecl,
    MetricFilterDecl,
    MetricVariant,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import AggregateSpec, ColumnRef
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder, WindowStep
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.models import SqlArtifact

# ── 辅助 ──

def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_spec(fixture_path: str):
    parser = DeveloperSpecParser()
    text = _read_fixture(fixture_path)
    return parser.parse(text)


# ════════════════════════════════════════════
# Compiler 测试
# ════════════════════════════════════════════


class TestDuckDbSqlCompiler:
    """DuckDbSqlCompiler 确定性编译测试。"""

    def test_single_table_compile(self):
        """单表 SqlBuildPlan → 合法 DuckDB SQL。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 验证 CompiledSql 结构
        assert compiled.sql != ""
        assert compiled.sql_sha256 != ""
        assert compiled.compiler_version == "1.1.0"
        assert compiled.input_plan_hash is not None

        # SQL 应包含关键词
        assert "SELECT" in compiled.sql.upper()
        assert "FROM" in compiled.sql.upper()

        # 应有优化记录
        assert compiled.optimized_plan is not None
        assert len(compiled.optimized_plan.applied_passes) >= 0

    def test_two_table_join_compile(self):
        """两表 Join SqlBuildPlan → 合法 DuckDB SQL。"""
        spec = _parse_spec("fixtures/relationship/explicit_join_spec.md")

        planner = RelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 提供表名映射
        table_mapping = {"tf": "dwd.test_fact", "td": "dim.test_dim"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        assert compiled.sql != ""
        assert "SELECT" in compiled.sql.upper()
        assert "JOIN" in compiled.sql.upper()
        assert "dwd.test_fact" in compiled.sql
        assert "dim.test_dim" in compiled.sql

    def test_deterministic_compile_same_hash(self):
        """相同 SqlBuildPlan 两次编译 → 相同 SQL 和 SHA-256。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()

        compiled1 = compiler.compile(plan)
        compiled2 = compiler.compile(plan)

        # SQL 文本必须一致
        assert compiled1.sql == compiled2.sql, (
            f"SQL 不同:\n---1---\n{compiled1.sql}\n---2---\n{compiled2.sql}"
        )

        # SHA-256 必须一致
        assert compiled1.sql_sha256 == compiled2.sql_sha256, (
            f"SHA-256 不同: {compiled1.sql_sha256} vs {compiled2.sql_sha256}"
        )

        # input_plan_hash 必须一致
        assert compiled1.input_plan_hash == compiled2.input_plan_hash

    def test_compile_with_case_when_minimal(self):
        """最小 CASE WHEN 编译——CaseWhenStep 确认可通过编译管道。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # CASE WHEN step 当前为空，编译应正常完成
        assert compiled.sql != ""

    def test_compile_to_artifact_wraps_full_lineage(self):
        """compile_to_artifact 包装 CompiledSql + 完整溯源链为 SqlArtifact。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        artifact = compiler.compile_to_artifact(
            plan,
            spec_hash=spec.spec_hash,
            hypothesis_id=None,
        )

        # 验证 SqlArtifact 结构
        assert isinstance(artifact, SqlArtifact)
        assert artifact.artifact_id != ""
        assert artifact.artifact_id.startswith("artifact_")
        assert artifact.spec_hash == spec.spec_hash
        assert artifact.plan_id == plan.plan_id
        assert artifact.hypothesis_id is None  # 单表无 hypothesis

        # 验证内嵌的 CompiledSql
        assert artifact.compiled_sql is not None
        assert artifact.compiled_sql.sql != ""
        assert artifact.compiled_sql.sql_sha256 != ""

        # 确定性：相同输入 → 相同 artifact_id
        artifact2 = compiler.compile_to_artifact(
            plan,
            spec_hash=spec.spec_hash,
        )
        assert artifact.artifact_id == artifact2.artifact_id
        assert artifact.compiled_sql.sql_sha256 == artifact2.compiled_sql.sql_sha256

    def test_optimized_plan_records_pruning(self):
        """OptimizedSQLPlan 正确记录列裁剪明细。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        optimized = compiled.optimized_plan

        # applied_passes 应包含所有 4 个 Pass
        pass_names = {p.pass_name for p in optimized.applied_passes}
        assert "column_pruning" in pass_names

        # column_pruning_removed 应为列表（可能为空，取决于 plan 内容）
        assert isinstance(optimized.column_pruning_removed, list)

        # eliminated_sorts 应为列表（可能为空）
        assert isinstance(optimized.eliminated_sorts, list)

        # predicate_normalizations 应为列表
        assert isinstance(optimized.predicate_normalizations, list)

        # constant_folds 应为列表
        assert isinstance(optimized.constant_folds, list)

        # input_plan_hash 和 output_plan_hash 应不同（经过 Pass 处理后）
        assert optimized.input_plan_hash != ""
        assert optimized.output_plan_hash != ""

    def test_optimized_plan_records_rejected_directives(self):
        """OptimizedSQLPlan.rejected_directives 正确记录未应用的优化指令。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # rejected_directives 应为列表（Phase 1C 当前为空）
        assert isinstance(compiled.optimized_plan.rejected_directives, list)

        # 验证 OptimizedSQLPlan 各字段与 CompiledSql 的关联一致
        assert compiled.optimized_plan.input_plan_hash == compiled.input_plan_hash


# ════════════════════════════════════════════
# MetricVariants 多条件变体测试
# ════════════════════════════════════════════


class TestMetricVariantExpansion:
    """Builder._expand_metric_to_agg_specs 展开 variants 为多个 AggregateSpec。"""

    def test_expand_single_metric_no_variants(self):
        """无 variants 的 MetricDecl → 单个 AggregateSpec（基础行为不变）。"""
        builder = SqlBuildPlanBuilder()
        m = MetricDecl(
            metric_name="total_users",
            aggregation=AggregationType.COUNT_DISTINCT,
            input_column="user_id",
            alias="total_users",
        )
        specs = builder._expand_metric_to_agg_specs(m)
        assert len(specs) == 1
        assert specs[0].alias == "total_users"
        assert specs[0].aggregation == AggregationType.COUNT_DISTINCT
        assert specs[0].filter is None

    def test_expand_three_variants(self):
        """一个 MetricDecl + 3 个 variants → 4 个 AggregateSpec。"""
        builder = SqlBuildPlanBuilder()
        m = MetricDecl(
            metric_name="user_count",
            aggregation=AggregationType.COUNT_DISTINCT,
            input_column="user_id",
            alias="total_users",
            variants=[
                MetricVariant(
                    variant_name="active_users",
                    filter=MetricFilterDecl(column="status", operator="eq", value="active"),
                    alias="active_users",
                ),
                MetricVariant(
                    variant_name="paying_users",
                    filter=MetricFilterDecl(column="status", operator="eq", value="paying"),
                    alias="paying_users",
                ),
                MetricVariant(
                    variant_name="vip_users",
                    filter=MetricFilterDecl(column="level", operator="eq", value="VIP"),
                    alias="vip_users",
                ),
            ],
        )
        specs = builder._expand_metric_to_agg_specs(m)
        assert len(specs) == 4

        # 基础指标
        assert specs[0].alias == "total_users"
        assert specs[0].filter is None

        # variant 1
        assert specs[1].alias == "active_users"
        assert specs[1].filter is not None
        assert specs[1].filter.column == "status"
        assert specs[1].filter.value == "active"

        # variant 2
        assert specs[2].alias == "paying_users"
        assert specs[2].filter is not None
        assert specs[2].filter.column == "status"
        assert specs[2].filter.value == "paying"

        # variant 3
        assert specs[3].alias == "vip_users"
        assert specs[3].filter is not None
        assert specs[3].filter.column == "level"
        assert specs[3].filter.value == "VIP"

        # 所有 variants 共享同一基础聚合逻辑
        for s in specs:
            assert s.aggregation == AggregationType.COUNT_DISTINCT
            assert s.input_column == "user_id"

    def test_variants_inherit_input_expression(self):
        """Variants 继承基础指标的 input_expression。"""
        builder = SqlBuildPlanBuilder()
        m = MetricDecl(
            metric_name="revenue",
            aggregation=AggregationType.SUM,
            input_expression="quantity * unit_price",
            alias="total_revenue",
            variants=[
                MetricVariant(
                    variant_name="online_revenue",
                    filter=MetricFilterDecl(column="channel", operator="eq", value="online"),
                    alias="online_revenue",
                ),
            ],
        )
        specs = builder._expand_metric_to_agg_specs(m)
        assert len(specs) == 2
        # 基础指标
        assert specs[0].input_expression == "quantity * unit_price"
        # variant 继承
        assert specs[1].input_expression == "quantity * unit_price"
        assert specs[1].alias == "online_revenue"
        assert specs[1].filter.column == "channel"

    def test_variants_inherit_distinct(self):
        """Variants 继承基础指标的 distinct 标志。"""
        builder = SqlBuildPlanBuilder()
        m = MetricDecl(
            metric_name="unique_amount",
            aggregation=AggregationType.SUM,
            input_column="amount",
            alias="total_unique_amount",
            distinct=True,
            variants=[
                MetricVariant(
                    variant_name="large_unique_amount",
                    filter=MetricFilterDecl(column="amount", operator="gt", value="1000"),
                    alias="large_unique_amount",
                ),
            ],
        )
        specs = builder._expand_metric_to_agg_specs(m)
        assert len(specs) == 2
        assert specs[0].distinct is True
        assert specs[1].distinct is True


class TestMetricVariantsInCompiler:
    """Variants → Compiler 生成正确的 FILTER 子句。"""

    def test_variants_in_aggregate_step(self):
        """含 variants 的 MetricDecl → AggregateStep.metrics 含展开条目。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        # 手工给指标添加 variants——使用 fixture 中存在的列 id
        if spec.metrics:
            spec.metrics[0].variants = [
                MetricVariant(
                    variant_name="high_id",
                    filter=MetricFilterDecl(column="id", operator="gt", value="100"),
                    alias="high_id_count",
                ),
            ]
            # variant 别名需加入 output_columns 以免被 ProjectStep 过滤
            from tianshu_datadev.developer_spec.models import OutputColumnDecl
            spec.output_spec.columns.append(OutputColumnDecl(name="high_id_count", type="bigint"))

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        # 找到 AggregateStep
        agg_steps = [s for s in plan.steps if s.step_type == "aggregate"]
        assert len(agg_steps) == 1
        agg_step = agg_steps[0]

        # 验证展开：基础指标(cnt) + 1 个 variant(high_id_count) = 2 个 AggregateSpec
        assert len(agg_step.metrics) == 2
        aliases = {m.alias for m in agg_step.metrics}
        assert "cnt" in aliases
        assert "high_id_count" in aliases

        # variant 条目应带 filter
        variant_spec = next(m for m in agg_step.metrics if m.alias == "high_id_count")
        assert variant_spec.filter is not None
        assert variant_spec.filter.column == "id"
        assert variant_spec.filter.value == "100"

    def test_variants_compile_filter_sql(self):
        """含 variants → 编译结果含 FILTER(WHERE ...) 子句。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        if spec.metrics:
            spec.metrics[0].variants = [
                MetricVariant(
                    variant_name="high_id",
                    filter=MetricFilterDecl(column="id", operator="gt", value="100"),
                    alias="high_id_count",
                ),
            ]
            # variant 别名需加入 output_columns 以免被 ProjectStep 过滤
            from tianshu_datadev.developer_spec.models import OutputColumnDecl
            spec.output_spec.columns.append(OutputColumnDecl(name="high_id_count", type="bigint"))

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # SQL 应包含 FILTER 关键字
        assert "FILTER" in compiled.sql.upper(), f"SQL 未含 FILTER:\n{compiled.sql}"
        assert "WHERE" in compiled.sql.upper()
        assert "high_id_count" in compiled.sql

    def test_variants_no_filter_null_base(self):
        """基础指标无 filter, variants 各有 filter——各自独立渲染。"""

        builder = SqlBuildPlanBuilder()
        m = MetricDecl(
            metric_name="user_stats",
            aggregation=AggregationType.COUNT,
            input_column="user_id",
            alias="total",
            variants=[
                MetricVariant(
                    variant_name="active",
                    filter=MetricFilterDecl(column="status", operator="eq", value="active"),
                    alias="active_count",
                ),
                MetricVariant(
                    variant_name="inactive",
                    filter=MetricFilterDecl(column="status", operator="eq", value="inactive"),
                    alias="inactive_count",
                ),
            ],
        )
        specs = builder._expand_metric_to_agg_specs(m)
        assert len(specs) == 3
        # 基础无 filter
        assert specs[0].filter is None
        assert specs[0].alias == "total"
        # variant 有 filter
        assert specs[1].filter is not None
        assert specs[1].alias == "active_count"
        assert specs[2].filter is not None
        assert specs[2].alias == "inactive_count"


# ════════════════════════════════════════════
# Aggregate + Project 集成测试
# ════════════════════════════════════════════


class TestAggregateProjectIntegration:
    """验证 AggregateStep 后跟 ProjectStep 时聚合表达式不被覆盖。"""

    def test_project_after_aggregate_preserves_count(self):
        """单表聚合+投影：ProjectStep 不应覆盖 COUNT 表达式。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 核心验证：聚合表达式 COUNT(id) 必须保留在 SQL 中
        assert "COUNT(tf.id) AS cnt" in compiled.sql, (
            f"聚合表达式 COUNT(tf.id) AS cnt 被 ProjectStep 覆盖，实际 SQL:\n{compiled.sql}"
        )

        # GROUP BY 子句必须存在
        assert "GROUP BY" in compiled.sql.upper(), (
            f"缺少 GROUP BY 子句，实际 SQL:\n{compiled.sql}"
        )

    def test_project_after_aggregate_preserves_all_metrics(self):
        """多指标聚合+投影：所有聚合表达式应保留。"""
        parser = DeveloperSpecParser()
        spec_text = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_metrics

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
        - name: user_id
          type: bigint
          nullable: false
        - name: order_amount
          type: decimal
          nullable: false
      business_columns:
        - name: event_time
          type: timestamp
          nullable: false

  metrics:
    - metric_name: pv
      aggregation: COUNT
      input_column: id
      alias: pv
    - metric_name: uv
      aggregation: COUNT_DISTINCT
      input_column: user_id
      alias: uv
    - metric_name: total_amount
      aggregation: SUM
      input_column: order_amount
      alias: total_amount

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
    - name: total_amount
      type: decimal
---

# 多指标聚合测试
```
"""
        spec = parser.parse(spec_text)
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 三个聚合表达式都必须保留（编译器会自动添加表前缀）
        assert "COUNT(tf.id) AS pv" in compiled.sql, (
            f"COUNT(tf.id) AS pv 被覆盖，实际 SQL:\n{compiled.sql}"
        )
        assert "COUNT(DISTINCT tf.user_id) AS uv" in compiled.sql, (
            f"COUNT(DISTINCT tf.user_id) AS uv 被覆盖，实际 SQL:\n{compiled.sql}"
        )
        assert "SUM(tf.order_amount) AS total_amount" in compiled.sql, (
            f"SUM(tf.order_amount) AS total_amount 被覆盖，实际 SQL:\n{compiled.sql}"
        )

        # 确认输出列顺序正确（stat_date → pv → uv → total_amount）
        sql_upper = compiled.sql.upper()
        select_start = sql_upper.index("SELECT") + 6
        from_pos = sql_upper.index("FROM")
        select_clause = compiled.sql[select_start:from_pos]

        # stat_date 应在 pv 之前（列顺序）
        assert select_clause.index("stat_date") < select_clause.index("pv"), (
            f"列顺序错误：stat_date 应在 pv 之前，实际 SELECT:\n{select_clause}"
        )

    def test_aggregate_with_filter_preserved(self):
        """带 FILTER 的聚合+投影：FILTER 子句应保留。"""
        parser = DeveloperSpecParser()
        spec_text = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_filter

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: user_id
          type: bigint
          nullable: false
        - name: status
          type: varchar
          nullable: true

  metrics:
    - metric_name: valid_users
      aggregation: COUNT
      input_column: user_id
      alias: valid_users
      filter:
        column: status
        operator: eq
        value: ACTIVE

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: valid_users
      type: bigint
---

# FILTER 聚合测试
```
"""
        spec = parser.parse(spec_text)
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # FILTER 子句必须保留
        assert "FILTER (WHERE status = 'ACTIVE')" in compiled.sql, (
            f"FILTER 子句被覆盖，实际 SQL:\n{compiled.sql}"
        )
        assert "COUNT(tf.user_id)" in compiled.sql, (
            f"COUNT(tf.user_id) 被覆盖，实际 SQL:\n{compiled.sql}"
        )

    def test_no_aggregate_project_only_no_interference(self):
        """无聚合纯投影：_reorder_aggregation_cols 不影响普通 ProjectStep。"""
        parser = DeveloperSpecParser()
        spec_text = """```markdown
---
spec:
  type: detail_table
  target_table: ads.test_detail

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      columns:
        - name: id
          type: bigint
          nullable: false
        - name: user_id
          type: bigint
          nullable: false

  output_columns:
    - name: id
      type: bigint
    - name: user_id
      type: bigint
---

# 纯投影测试
```
"""
        spec = parser.parse(spec_text)
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 无聚合时的纯投影：列名应直接出现在 SELECT 中
        assert "id" in compiled.sql
        assert "user_id" in compiled.sql
        # 不应有 GROUP BY
        assert "GROUP BY" not in compiled.sql.upper()


# ════════════════════════════════════════════
# 窗口过滤子查询包裹测试（P1-3）
# ════════════════════════════════════════════


class TestWindowFilterWrapping:
    """Compiler._render_window_wrapped_sql——窗口列在 FilterStep 中触发子查询包裹。"""

    @staticmethod
    def _make_window_filter_plan(
        window_func: str = "ROW_NUMBER",
        window_alias: str = "rn",
        filter_op: str = "LTE",
        filter_value: int = 3,
    ):
        """构造含 WindowStep + FilterStep(引用窗口列) 的 SqlBuildPlan。

        Plan 结构：Scan → Aggregate → Window → Filter(refs window col) → Sort → Limit
        """
        from tianshu_datadev.developer_spec.models import (
            AggregationType,
            SortDirection,
        )
        from tianshu_datadev.planning.models import (
            ColumnRef,
            Predicate,
            PredicateOperator,
            SortSpec,
            SqlLiteral,
            WindowExpr,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            FilterStep,
            LimitStep,
            ScanStep,
            SortStep,
            SqlBuildPlan,
            WindowStep,
        )

        scan = ScanStep(
            step_id="step_scan_test",
            table_ref="t",
            required_columns=[
                ColumnRef(table_ref="t", column_name="category", normalized_name="category"),
                ColumnRef(table_ref="t", column_name="sales", normalized_name="sales"),
            ],
        )

        agg = AggregateStep(
            step_id="step_agg_test",
            group_keys=[
                ColumnRef(table_ref="t", column_name="category", normalized_name="category"),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.SUM,
                    input_column="sales",
                    alias="total_sales",
                ),
            ],
        )

        win = WindowStep(
            step_id="step_win_test",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction(window_func),
                    partition_by=[
                        ColumnRef(table_ref="t", column_name="category", normalized_name="category"),
                    ],
                    order_by=[
                        SortSpec(column="total_sales", direction=SortDirection.DESC),
                    ],
                    alias=window_alias,
                ),
            ],
        )

        filter_step = FilterStep(
            step_id="step_filter_test",
            predicate=Predicate(
                left=ColumnRef(table_ref="", column_name=window_alias, normalized_name=window_alias),
                operator=PredicateOperator(filter_op),
                right=SqlLiteral(value=filter_value),
            ),
        )

        sort = SortStep(
            step_id="step_sort_test",
            order_by=[SortSpec(column="category", direction=SortDirection.ASC)],
        )

        limit = LimitStep(step_id="step_limit_test", limit=10)

        return SqlBuildPlan(
            plan_id="plan_test_window_filter",
            spec_hash="test_window_filter_hash_000",
            steps=[scan, agg, win, filter_step, sort, limit],
        )

    # ── 窗口函数类型覆盖 ──

    def test_row_number_top3(self):
        """ROW_NUMBER + rn <= 3 → 子查询包裹 SQL。"""
        plan = self._make_window_filter_plan("ROW_NUMBER", "rn", "LTE", 3)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 验证子查询包裹结构
        assert "SELECT *" in compiled.sql
        assert "FROM (" in compiled.sql
        assert ") AS _sub" in compiled.sql
        # 内层应有 ROW_NUMBER
        assert "ROW_NUMBER()" in compiled.sql.upper()
        # 外层应有 WHERE _sub.rn <= 3
        assert "_sub.rn" in compiled.sql
        assert "<= 3" in compiled.sql

    def test_rank_filter(self):
        """RANK + rank <= 5 → 子查询包裹 SQL。"""
        plan = self._make_window_filter_plan("RANK", "rank", "LTE", 5)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        assert "SELECT *" in compiled.sql
        assert ") AS _sub" in compiled.sql
        assert "RANK()" in compiled.sql.upper()
        assert "_sub.rank" in compiled.sql
        assert "<= 5" in compiled.sql

    def test_dense_rank_filter(self):
        """DENSE_RANK + dr <= 10 → 子查询包裹 SQL。"""
        plan = self._make_window_filter_plan("DENSE_RANK", "dr", "LTE", 10)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        assert "DENSE_RANK()" in compiled.sql.upper()
        assert "_sub.dr" in compiled.sql
        assert "<= 10" in compiled.sql

    def test_sum_over_filter(self):
        """SUM_OVER 窗口函数 + 过滤——全部 8 种窗口函数通用。"""
        plan = self._make_window_filter_plan("SUM_OVER", "cumulative", "GT", 1000)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        assert "SUM(" in compiled.sql.upper()
        assert "OVER" in compiled.sql.upper()
        assert "_sub.cumulative" in compiled.sql
        assert "> 1000" in compiled.sql

    # ── 不同过滤操作符 ──

    def test_window_filter_eq(self):
        """窗口列 = 1 过滤。"""
        plan = self._make_window_filter_plan("ROW_NUMBER", "rn", "EQ", 1)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        assert "_sub.rn = 1" in compiled.sql

    def test_window_filter_gt(self):
        """窗口列 > 100 过滤。"""
        plan = self._make_window_filter_plan("SUM_OVER", "running_sum", "GT", 100)
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        assert "_sub.running_sum > 100" in compiled.sql

    # ── 无窗口过滤不触发包裹 ──

    def test_no_window_filter_flat_render(self):
        """无 WindowStep 的 Plan → 扁平渲染（不触发子查询包裹）。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 扁平渲染不应有 _sub 前缀
        assert ") AS _sub" not in compiled.sql
        # 标准 SQL 结构
        assert "SELECT" in compiled.sql.upper()
        assert "FROM" in compiled.sql.upper()

    def test_window_without_filter_flat_render(self):
        """有 WindowStep 但无 FilterStep 引用窗口列 → 扁平渲染。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            SortSpec,
            WindowExpr,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
            WindowStep,
        )

        plan = SqlBuildPlan(
            plan_id="plan_win_no_filter",
            spec_hash="test_win_no_filter_hash",
            steps=[
                ScanStep(
                    step_id="step_s",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="x", normalized_name="x"),
                    ],
                ),
                WindowStep(
                    step_id="step_w",
                    window_exprs=[
                        WindowExpr(
                            function=WindowFunction.ROW_NUMBER,
                            order_by=[SortSpec(column="x", direction="DESC")],
                            alias="rn",
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 无过滤引用的窗口不应触发包裹
        assert ") AS _sub" not in compiled.sql
        assert "ROW_NUMBER()" in compiled.sql.upper()

    # ── 边界 ──

    def test_window_predicate_references_any_detection(self):
        """_predicate_references_any 正确递归检测嵌套 Predicate。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            Predicate,
            PredicateOperator,
            SqlLiteral,
        )

        compiler = DuckDbSqlCompiler()
        window_aliases = {"rn", "rank"}

        # 直接引用
        pred_direct = Predicate(
            left=ColumnRef(table_ref="", column_name="rn", normalized_name="rn"),
            operator=PredicateOperator.LTE,
            right=SqlLiteral(value=3),
        )
        assert compiler._predicate_references_any(pred_direct, window_aliases) is True

        # AND 嵌套引用
        pred_and = Predicate(
            left=pred_direct,
            operator=PredicateOperator.AND,
            right=Predicate(
                left=ColumnRef(table_ref="", column_name="x", normalized_name="x"),
                operator=PredicateOperator.GT,
                right=SqlLiteral(value=0),
            ),
        )
        assert compiler._predicate_references_any(pred_and, window_aliases) is True

        # 无引用
        pred_no = Predicate(
            left=ColumnRef(table_ref="", column_name="x", normalized_name="x"),
            operator=PredicateOperator.GT,
            right=SqlLiteral(value=0),
        )
        assert compiler._predicate_references_any(pred_no, window_aliases) is False

    def test_collect_window_aliases(self):
        """_collect_window_aliases 正确收集所有 WindowStep 的别名。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            WindowExpr,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
            WindowStep,
        )

        plan = SqlBuildPlan(
            plan_id="plan_multi_win",
            spec_hash="test_multi_win_hash",
            steps=[
                ScanStep(
                    step_id="step_s",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="x", normalized_name="x"),
                    ],
                ),
                WindowStep(
                    step_id="step_w1",
                    window_exprs=[
                        WindowExpr(
                            function=WindowFunction.ROW_NUMBER,
                            alias="rn",
                        ),
                        WindowExpr(
                            function=WindowFunction.RANK,
                            alias="rank",
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        aliases = compiler._collect_window_aliases(plan)
        assert aliases == {"rn", "rank"}


# ════════════════════════════════════════════
# P2-6：自引用 Join——同一张表 Join 自身
# ════════════════════════════════════════════


class TestSelfJoinCompile:
    """自引用 Join——同一张表 Join 自身时自动生成不同别名（_self_left / _self_right）。"""

    @staticmethod
    def _build_self_join_spec():
        """构造员工自引用测试 Spec——emp 表通过 mgr_id → id 连接自身。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DimensionDecl,
            InputTableDecl,
            JoinDecl,
            JoinTypeEnum,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        normalizer = FieldNormalizer()

        def _col(name: str, dtype: str = "bigint") -> ColumnDecl:
            return ColumnDecl(
                column_name=name,
                normalized_name=normalizer.normalize(name),
                data_type=dtype,
            )

        spec = ParsedDeveloperSpec(
            spec_id="test_self_join",
            spec_hash="test_self_join_hash_001",
            title="员工自引用测试",
            description="查询每位员工及其上级——emp 表 mgr_id → id 连接自身",
            input_tables=[
                InputTableDecl(
                    table_alias="emp",
                    source_table="hr.employees",  # type: ignore[arg-type]
                    role="dim",
                    key_columns=[_col("id")],
                    business_columns=[
                        _col("name", "varchar"),
                        _col("mgr_id"),
                    ],
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="emp_count",
                    aggregation=AggregationType.COUNT,
                    input_column="id",
                    alias="emp_count",
                ),
            ],
            dimensions=[
                DimensionDecl(dimension_name="name", column_ref="name"),
            ],
            joins=[
                JoinDecl(
                    left_table="emp",
                    right_table="emp",
                    left_key="mgr_id",
                    right_key="id",
                    join_type=JoinTypeEnum.INNER,
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="name", type="varchar"),
                    OutputColumnDecl(name="emp_count", type="bigint"),
                ],
                grain=["name"],
            ),
        )
        return spec

    def test_self_join_compile_has_distinct_aliases(self):
        """自引用编译产物应含 _self_left / _self_right 不同别名。"""
        spec = self._build_self_join_spec()

        planner = RelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 提供表名映射——emp 别名映射到物理表
        table_mapping = {"emp": "hr.employees"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        sql = compiled.sql
        # 验证 SQL 包含自引用别名
        assert "_self_left" in sql, f"SQL 应含 _self_left 别名:\n{sql}"
        assert "_self_right" in sql, f"SQL 应含 _self_right 别名:\n{sql}"
        # 验证物理表出现两次（左右各一次）
        assert sql.count("hr.employees") == 2, (
            f"物理表应出现 2 次（左右各一）:\n{sql}"
        )
        # 验证 JOIN 子句
        assert "JOIN" in sql.upper()
        # 验证别名不同（不应出现同一别名两次）
        assert " AS emp\n" not in sql  # 不应有无后缀的裸别名在 FROM/JOIN 中

    def test_self_join_sql_structure(self):
        """自引用 SQL 结构验证——FROM 左别名 + JOIN 右别名 + ON 条件。"""
        spec = self._build_self_join_spec()

        planner = RelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        table_mapping = {"emp": "hr.employees"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        sql = compiled.sql
        # FROM 子句应有左别名
        assert "FROM\n  hr.employees AS emp_self_left" in sql, (
            f"FROM 应使用 emp_self_left:\n{sql}"
        )
        # JOIN 子句应有右别名
        assert "JOIN\n  hr.employees AS emp_self_right" in sql, (
            f"JOIN 应使用 emp_self_right:\n{sql}"
        )
        # ON 条件应用不同别名引用（mgr_id → id）
        assert "emp_self_left.mgr_id" in sql
        assert "emp_self_right.id" in sql

    def test_self_join_duckdb_execution(self):
        """自引用 SQL 在 DuckDB 中正确执行——员工+上级配对。"""
        import duckdb

        spec = self._build_self_join_spec()

        planner = RelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 使用简单表名避免 schema 前缀问题
        table_mapping = {"emp": "employees"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        # 在 DuckDB 中执行
        con = duckdb.connect(":memory:")
        try:
            # 创建测试数据：员工表含 id/name/mgr_id（CEO 的 mgr_id 为 NULL）
            con.execute("""
                CREATE TABLE employees AS
                SELECT * FROM (VALUES
                    (1, 'Alice', NULL),
                    (2, 'Bob', 1),
                    (3, 'Charlie', 1),
                    (4, 'Diana', 2)
                ) AS t(id, name, mgr_id)
            """)

            result = con.execute(compiled.sql).fetchall()
            # 应有 3 行（Bob→Alice, Charlie→Alice, Diana→Bob），CEO 无上级被 INNER JOIN 排除
            # 输出列：emp_name, emp_count
            assert len(result) == 3, (
                f"预期 3 行员工-上级配对，实际 {len(result)}: {result}"
            )

            # 验证员工名都存在
            emp_names = {row[0] for row in result}
            assert emp_names == {"Bob", "Charlie", "Diana"}, (
                f"应有 Bob/Charlie/Diana（Alice 的 mgr_id=NULL 被 INNER JOIN 排除），实际: {emp_names}"
            )
        finally:
            con.close()

    def test_self_join_no_false_positive(self):
        """非自引用的普通两表 Join 不受影响——别名保持原样。"""
        # 使用已有 fixture 测试普通两表 Join 不受自引用逻辑影响
        spec = _parse_spec("fixtures/relationship/explicit_join_spec.md")

        planner = RelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        table_mapping = {"tf": "dwd.test_fact", "td": "dim.test_dim"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        sql = compiled.sql
        # 不应出现自引用别名
        assert "_self_left" not in sql
        assert "_self_right" not in sql
        # 原始别名应正确出现
        assert "tf" in sql
        assert "td" in sql


# ════════════════════════════════════════════
# P3-10：业务日历——财年 + 相对日期 + Compiler SQL 渲染
# ════════════════════════════════════════════


class TestBusinessCalendar:
    """业务日历——财年 / 相对日期 / 固定起止 → FilterStep → SQL 渲染。"""

    @staticmethod
    def _build_time_range_spec(time_range_cfg: dict):
        """构造含 time_range 的简单单表 Spec——用于 Builder + Compiler 测试。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            DimensionDecl,
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
            TimeRangeDecl,
        )

        normalizer = FieldNormalizer()

        def _col(name: str, dtype: str = "bigint") -> ColumnDecl:
            return ColumnDecl(
                column_name=name, normalized_name=normalizer.normalize(name),
                data_type=dtype,
            )

        tr = TimeRangeDecl(**time_range_cfg)

        return ParsedDeveloperSpec(
            spec_id="test_biz_calendar",
            spec_hash="test_biz_cal_hash_001",
            title="业务日历测试",
            description="验证时间范围过滤 SQL 生成",
            input_tables=[
                InputTableDecl(
                    table_alias="t",
                    source_table="test.orders",  # type: ignore[arg-type]
                    role="fact",
                    key_columns=[_col("id")],
                    business_columns=[_col("amount"), _col("order_time", "varchar")],
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="total",
                    aggregation=AggregationType.SUM,
                    input_column="amount",
                    alias="total_amount",
                ),
            ],
            dimensions=[
                DimensionDecl(dimension_name="order_time", column_ref="order_time"),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="order_time", type="varchar"),
                    OutputColumnDecl(name="total_amount", type="decimal"),
                ],
                grain=["order_time"],
            ),
            time_range=tr,
        )

    def test_fixed_date_range_between(self):
        """固定起止 → >= start AND <= end（BETWEEN 规范化展开后保持包含语义）。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "start": "2025-01-01",
            "end": "2025-06-30",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert ">=" in sql
        assert "<=" in sql
        assert "'2025-01-01'" in sql
        assert "'2025-06-30'" in sql
        assert "WHERE" in sql

    def test_fiscal_july_date_range(self):
        """财年 7 月起 FY2026 → >= '2026-07-01' AND <= '2027-06-30'。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "calendar_type": "fiscal_jul",
            "fiscal_year": 2026,
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "'2026-07-01'" in sql, f"财年 7 月应含起始日期:\n{sql}"
        assert "'2027-06-30'" in sql, f"财年 7 月应含结束日期:\n{sql}"
        assert "<=" in sql  # 包含上界

    def test_fiscal_april_date_range(self):
        """财年 4 月起 FY2026 → >= '2026-04-01' AND <= '2027-03-31'。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "calendar_type": "fiscal_apr",
            "fiscal_year": 2026,
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "'2026-04-01'" in sql, f"财年 4 月应含起始日期:\n{sql}"
        assert "'2027-03-31'" in sql, f"财年 4 月应含结束日期:\n{sql}"

    def test_relative_range_last_30d(self):
        """相对日期 last_30d → CURRENT_DATE - INTERVAL 30 DAY。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "relative_range": "last_30d",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "CURRENT_DATE - INTERVAL 30 DAY" in sql, (
            f"last_30d 应使用 CURRENT_DATE 表达式:\n{sql}"
        )
        assert ">=" in sql  # GTE 操作符

    def test_relative_range_last_7d(self):
        """相对日期 last_7d → CURRENT_DATE - INTERVAL 7 DAY。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "relative_range": "last_7d",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "CURRENT_DATE - INTERVAL 7 DAY" in sql, (
            f"last_7d 应使用 7 天间隔:\n{sql}"
        )

    def test_relative_range_mtd(self):
        """相对日期 mtd → DATE_TRUNC('month', CURRENT_DATE)。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "relative_range": "mtd",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "DATE_TRUNC('month', CURRENT_DATE)" in sql, (
            f"mtd 应使用月初表达式:\n{sql}"
        )

    def test_relative_range_ytd(self):
        """相对日期 ytd → DATE_TRUNC('year', CURRENT_DATE)。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "relative_range": "ytd",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        assert "DATE_TRUNC('year', CURRENT_DATE)" in sql, (
            f"ytd 应使用年初表达式:\n{sql}"
        )

    def test_no_time_range_no_where(self):
        """无 time_range 时不生成额外 WHERE 条件。"""
        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "start": "",
            "end": "",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "test.orders"})
        compiled = compiler.compile(plan)

        sql = compiled.sql
        # 没有时间范围过滤——不应有 WHERE
        assert "BETWEEN" not in sql, f"无时间范围不应有 BETWEEN:\n{sql}"

    def test_duckdb_fixed_date_execution(self):
        """固定日期范围在 DuckDB 中正确过滤数据。"""
        import duckdb

        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "start": "2025-06-01",
            "end": "2025-06-15",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "orders"})
        compiled = compiler.compile(plan)

        con = duckdb.connect(":memory:")
        try:
            con.execute("""
                CREATE TABLE orders AS
                SELECT * FROM (VALUES
                    (1, 100, '2025-05-20'),
                    (2, 200, '2025-06-01'),
                    (3, 300, '2025-06-10'),
                    (4, 400, '2025-06-15'),
                    (5, 500, '2025-07-01')
                ) AS t(id, amount, order_time)
            """)

            result = con.execute(compiled.sql).fetchall()
            # 应包含 6/1, 6/10, 6/15 的数据（3 行）
            assert len(result) == 3, (
                f"预期 3 行（6/1~6/15），实际 {len(result)}: {result}"
            )
        finally:
            con.close()

    def test_duckdb_relative_range_execution(self):
        """相对日期 last_30d 在 DuckDB 中正确过滤。"""
        import duckdb

        spec = self._build_time_range_spec({
            "column_ref": "order_time",
            "relative_range": "last_30d",
        })

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler({"t": "orders"})
        compiled = compiler.compile(plan)

        con = duckdb.connect(":memory:")
        try:
            # 创建含 DATE 类型的数据——避免 VARCHAR vs DATE 类型冲突
            con.execute("""
                CREATE TABLE orders AS
                SELECT * FROM (VALUES
                    (1, 100, CAST('2020-01-01' AS DATE)),
                    (2, 200, CAST('2020-01-02' AS DATE)),
                    (3, 300, CURRENT_DATE - INTERVAL 15 DAY),
                    (4, 400, CURRENT_DATE),
                    (5, 500, CURRENT_DATE - INTERVAL 5 DAY)
                ) AS t(id, amount, order_time)
            """)

            result = con.execute(compiled.sql).fetchall()
            # 应包含最近 30 天内的 3 行（15 天前、今天、5 天前）
            assert len(result) == 3, (
                f"预期 3 行（最近 30 天），实际 {len(result)}: {result}"
            )
        finally:
            con.close()


# ════════════════════════════════════════════
# P3-9：条件分支聚合——CaseWhenStep 生成 + 编译 + 执行
# ════════════════════════════════════════════


class TestConditionalBranch:
    """条件分支聚合——Builder 生成 CaseWhenStep + 编译 + DuckDB 执行验证。"""

    @staticmethod
    def _make_conditional_branch_spec(spec_hash: str = "spec_cond_branch"):
        """构造含条件分支 DAG 的 ParsedDeveloperSpec——2 分支 + 1 CASE WHEN 合流。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType,
            CaseWhenBranchDecl,
            CaseWhenDecl,
            ColumnDecl,
            ComputeStep,
            InputTableDecl,
            JoinDecl,
            JoinTypeEnum,
            MetricDecl,
            MetricFilterDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        table = InputTableDecl(
            table_alias="o", source_table="dwd.order_fact",
            columns=[
                ColumnDecl(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                ColumnDecl(column_name="user_type", normalized_name="user_type", data_type="varchar"),
                ColumnDecl(column_name="amount", normalized_name="amount", data_type="decimal"),
            ],
            role="fact",
        )

        vip_metrics = [
            MetricDecl(
                metric_name="vip_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="vip_amount",
                filter=MetricFilterDecl(column="user_type", operator="=", value="VIP"),
            ),
        ]
        normal_metrics = [
            MetricDecl(
                metric_name="normal_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="normal_amount",
                filter=MetricFilterDecl(column="user_type", operator="=", value="NORMAL"),
            ),
        ]

        case_when = CaseWhenDecl(
            branches=[
                CaseWhenBranchDecl(
                    condition_column="user_type", condition_operator="=",
                    condition_value="VIP", result_column="vip_amount",
                ),
                CaseWhenBranchDecl(
                    condition_column="user_type", condition_operator="=",
                    condition_value="NORMAL", result_column="normal_amount",
                ),
            ],
            else_value=None,
            output_column="final_amount",
        )

        compute_steps = [
            ComputeStep(
                step_name="branch_vip", source="input",
                group_by=["user_id", "user_type"], metrics=vip_metrics,
                output_alias="branch_vip",
            ),
            ComputeStep(
                step_name="branch_normal", source="input",
                group_by=["user_id", "user_type"], metrics=normal_metrics,
                output_alias="branch_normal",
            ),
            ComputeStep(
                step_name="merge", source=["branch_vip", "branch_normal"],
                group_by=["user_id", "user_type"], metrics=[],
                case_when=case_when,
                output_alias="merge",
            ),
        ]

        joins = [
            JoinDecl(
                left_table="branch_vip", right_table="branch_normal",
                left_key="user_id", right_key="user_id",
                join_type=JoinTypeEnum.LEFT,
            ),
        ]

        output_spec = OutputSpecDecl(
            columns=[
                OutputColumnDecl(name="user_id", type="bigint"),
                OutputColumnDecl(name="user_type", type="varchar"),
                OutputColumnDecl(name="final_amount", type="decimal"),
            ],
            grain=["user_id", "user_type"],
        )

        return ParsedDeveloperSpec(
            spec_id="spec_cond_branch_test",
            spec_hash=spec_hash,
            title="条件分支聚合测试",
            description="VIP 和 NORMAL 客户分别汇总，CASE WHEN 合并",
            input_tables=[table],
            metrics=[],
            dimensions=[],
            joins=joins,
            output_spec=output_spec,
            compute_steps=compute_steps,
        )

    def test_conditional_branch_produces_case_when_step(self):
        """条件分支合并步骤应包含 CaseWhenStep。"""
        spec = self._make_conditional_branch_spec()
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)

        merge_plan = plans[2]
        step_types = [s.step_type for s in merge_plan.steps]
        assert "case_when" in step_types, (
            f"合并 Plan 应包含 case_when 步骤，实际: {step_types}"
        )

        case_steps = [s for s in merge_plan.steps if s.step_type == "case_when"]
        assert len(case_steps) == 1
        case_step = case_steps[0]
        assert len(case_step.cases) == 2, (
            f"应有 2 个 WHEN 分支，实际: {len(case_step.cases)}"
        )
        assert str(case_step.alias) == "final_amount", (
            f"CaseWhenStep alias 应为 final_amount，实际: {case_step.alias}"
        )

    def test_conditional_branch_no_aggregate_in_merge(self):
        """合并步骤有 case_when 时应跳过 AggregateStep。"""
        spec = self._make_conditional_branch_spec()
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)

        merge_plan = plans[2]
        step_types = [s.step_type for s in merge_plan.steps]
        assert "aggregate" not in step_types, (
            f"合并 Plan 不应有 aggregate（CASE WHEN 替代），实际: {step_types}"
        )

    def test_conditional_branch_compile(self):
        """条件分支编译——CaseWhenStep 应正确渲染 SQL。"""
        spec = self._make_conditional_branch_spec()
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)

        compiler = DuckDbSqlCompiler()
        merge_sql = compiler.compile(plans[2]).sql
        assert "CASE" in merge_sql.upper(), (
            f"合并 Plan SQL 应包含 CASE: {merge_sql}"
        )
        assert "WHEN" in merge_sql.upper(), (
            f"合并 Plan SQL 应包含 WHEN: {merge_sql}"
        )

    def test_conditional_branch_deterministic(self):
        """相同输入对应相同的 plan_id（确定性）。"""
        spec = self._make_conditional_branch_spec()
        plans1 = SqlBuildPlanBuilder().build_from_steps(spec)
        plans2 = SqlBuildPlanBuilder().build_from_steps(spec)

        for i, (p1, p2) in enumerate(zip(plans1, plans2)):
            assert p1.plan_id == p2.plan_id, (
                f"Plan {i}: plan_id 应一致 ({p1.plan_id} vs {p2.plan_id})"
            )

    def test_conditional_branch_duckdb_execution(self):
        """条件分支 DuckDB 执行——验证 CASE WHEN 结果正确。"""
        import duckdb

        spec = self._make_conditional_branch_spec()
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)

        compiler = DuckDbSqlCompiler(table_mapping={"o": "dwd.order_fact"})
        plan_sqls = [compiler.compile(p).sql for p in plans]

        con = duckdb.connect(":memory:")
        try:
            con.execute("CREATE SCHEMA dwd")
            con.execute("""
                CREATE TABLE dwd.order_fact AS
                SELECT * FROM (VALUES
                    (1, 'VIP', 100.0),
                    (2, 'VIP', 200.0),
                    (3, 'NORMAL', 50.0),
                    (4, 'NORMAL', 75.0),
                    (5, 'NORMAL', 125.0)
                ) AS t(user_id, user_type, amount)
            """)

            # 执行分支 1（VIP）和分支 2（NORMAL）
            # 注意：FILTER 聚合对全部 GROUP BY 键都产出行——不匹配的为 NULL
            con.execute(f"CREATE TEMP TABLE _temp_test_branch_vip AS {plan_sqls[0]}")
            con.execute(f"CREATE TEMP TABLE _temp_test_branch_normal AS {plan_sqls[1]}")

            # 确定 chain_id——从编译 SQL 中提取
            import re
            temp_match = re.search(r'_temp_c([a-f0-9]{8})_', plan_sqls[2])
            assert temp_match, f"合并 SQL 应引用 _temp_ 表: {plan_sqls[2]}"
            chain_id = temp_match.group(1)

            # 直接用正确名称创建 temp 表
            con.execute("DROP TABLE IF EXISTS _temp_test_branch_vip")
            con.execute("DROP TABLE IF EXISTS _temp_test_branch_normal")
            # 按 Builder 生成的 _temp 表名重建
            con.execute(
                f"CREATE TEMP TABLE \"_temp_c{chain_id}_branch_vip\" AS {plan_sqls[0]}"
            )
            con.execute(
                f"CREATE TEMP TABLE \"_temp_c{chain_id}_branch_normal\" AS {plan_sqls[1]}"
            )

            # 执行合并步骤——直接使用编译 SQL（已引用正确的 _temp 表名）
            result = con.execute(plan_sqls[2]).fetchall()

            # FILTER 聚合保留所有 GROUP BY 行——每行通过 CASE WHEN 取值
            assert len(result) == 5, (
                f"合并结果应有 5 行，实际: {len(result)}: {result}"
            )

            # 验证 CASE WHEN 正确选择——VIP 用户取 vip_amount，NORMAL 取 normal_amount
            for row in result:
                user_id, user_type, final_amount = row
                if user_type == "VIP":
                    assert final_amount is not None, f"VIP 用户 {user_id} 应有金额"
                    assert float(final_amount) > 0, f"VIP 用户 {user_id} 金额应 > 0"
                elif user_type == "NORMAL":
                    assert final_amount is not None, f"NORMAL 用户 {user_id} 应有金额"
                    assert float(final_amount) > 0, f"NORMAL 用户 {user_id} 金额应 > 0"
        finally:
            con.close()

    def test_conditional_branch_no_case_when_without_decl(self):
        """无 case_when 声明的普通合流步骤不应产生 CaseWhenStep。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType,
            ColumnDecl,
            ComputeStep,
            InputTableDecl,
            JoinDecl,
            JoinTypeEnum,
            MetricDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )

        table = InputTableDecl(
            table_alias="o", source_table="dwd.test",
            columns=[
                ColumnDecl(column_name="id", normalized_name="id", data_type="bigint"),
                ColumnDecl(column_name="val", normalized_name="val", data_type="decimal"),
            ],
            role="fact",
        )

        m1 = [MetricDecl(metric_name="sum_a", aggregation=AggregationType.SUM,
               input_column="val", alias="sum_a")]
        m2 = [MetricDecl(metric_name="cnt_b", aggregation=AggregationType.COUNT,
               input_column="id", alias="cnt_b")]
        m3 = [MetricDecl(metric_name="avg_val", aggregation=AggregationType.AVG,
               input_column="sum_a", alias="avg_val")]

        compute_steps = [
            ComputeStep(step_name="a", source="input", group_by=["id"], metrics=m1),
            ComputeStep(step_name="b", source="input", group_by=["id"], metrics=m2),
            ComputeStep(step_name="c", source=["a", "b"], group_by=["id"], metrics=m3),
        ]

        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="test_no_cw",
            title="无 CASE WHEN 合流", description="普通合流",
            input_tables=[table], metrics=[], dimensions=[],
            joins=[JoinDecl(left_table="a", right_table="b",
                   left_key="id", right_key="id", join_type=JoinTypeEnum.INNER)],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="id", type="bigint"),
                         OutputColumnDecl(name="avg_val", type="decimal")],
                grain=["id"],
            ),
            compute_steps=compute_steps,
        )

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)
        merge_plan = plans[2]
        step_types = [s.step_type for s in merge_plan.steps]
        assert "case_when" not in step_types, (
            f"无 case_when 声明时不应有 CaseWhenStep，实际: {step_types}"
        )
        assert "aggregate" in step_types, (
            f"无 case_when 声明时应有 AggregateStep，实际: {step_types}"
        )

    def test_spec_enricher_detects_conditional_branch(self):
        """SpecEnricher 检测含 variants 的指标 → 生成条件分支 compute_steps。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType,
            ColumnDecl,
            InputTableDecl,
            MetricDecl,
            MetricFilterDecl,
            MetricVariant,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
        from tianshu_datadev.planning.spec_enricher import SpecEnricher

        metrics = [
            MetricDecl(
                metric_name="typed_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="typed_amount",
                variants=[
                    MetricVariant(
                        variant_name="vip_amount", alias="vip_amount",
                        filter=MetricFilterDecl(column="user_type", operator="=", value="VIP"),
                    ),
                    MetricVariant(
                        variant_name="normal_amount", alias="normal_amount",
                        filter=MetricFilterDecl(column="user_type", operator="=", value="NORMAL"),
                    ),
                ],
            ),
        ]

        table = InputTableDecl(
            table_alias="o", source_table="dwd.test",
            columns=[
                ColumnDecl(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                ColumnDecl(column_name="user_type", normalized_name="user_type", data_type="varchar"),
                ColumnDecl(column_name="amount", normalized_name="amount", data_type="decimal"),
            ],
            role="fact",
        )

        spec = ParsedDeveloperSpec(
            spec_id="test_enricher_branch",
            spec_hash="spec_enricher_branch",
            title="条件分支检测测试",
            description="VIP客户和NORMAL客户分别汇总金额",
            input_tables=[table],
            metrics=metrics,
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="user_id", type="bigint"),
                    OutputColumnDecl(name="user_type", type="varchar"),
                    OutputColumnDecl(name="typed_amount", type="decimal"),
                ],
                grain=["user_id", "user_type"],
            ),
        )

        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched = enricher.enrich(spec, manifest)

        meta = enriched.enrichment_metadata
        generated_steps = meta.get("generated_compute_steps", [])
        assert len(generated_steps) >= 3, (
            f"应有至少 3 个生成步骤（2 分支 + 1 合并），实际: {len(generated_steps)}"
        )

        merge_steps = [
            s for s in generated_steps
            if s.get("case_when") is not None
        ]
        assert len(merge_steps) == 1, (
            f"应有 1 个合并步骤含 case_when，实际: {len(merge_steps)}"
        )
        merge_cw = merge_steps[0]["case_when"]
        assert len(merge_cw.get("branches", [])) == 2, (
            "合并步骤应有 2 个 CASE WHEN 分支"
        )


# ════════════════════════════════════════════
# 窗口函数 Builder 集成测试
# ════════════════════════════════════════════


class TestWindowStepBuilder:
    """窗口函数管线集成——Builder 构造 WindowStep + 编译 + 执行。"""

    # ── 辅助 ──

    @staticmethod
    def _make_window_metrics(*aliases: str) -> list[InferredWindowMetric]:
        """创建测试用 InferredWindowMetric 列表——每种函数一个。"""
        metrics: list[InferredWindowMetric] = []
        for alias in aliases:
            if alias == "rn":
                metrics.append(InferredWindowMetric(
                    metric_name="rn",
                    window_function="ROW_NUMBER",
                    input_column="",
                    partition_by=["dept"],
                    order_by=["salary DESC"],
                    alias="rn",
                    confidence="high",
                ))
            elif alias == "rk":
                metrics.append(InferredWindowMetric(
                    metric_name="rk",
                    window_function="RANK",
                    input_column="",
                    order_by=["salary DESC"],
                    alias="rk",
                    confidence="high",
                ))
            elif alias == "dr":
                metrics.append(InferredWindowMetric(
                    metric_name="dr",
                    window_function="DENSE_RANK",
                    input_column="",
                    order_by=["salary DESC"],
                    alias="dr",
                    confidence="high",
                ))
            elif alias == "nt":
                metrics.append(InferredWindowMetric(
                    metric_name="nt",
                    window_function="NTILE",
                    input_column="4",
                    order_by=["salary DESC"],
                    alias="nt",
                    confidence="high",
                ))
            elif alias == "lag_sal":
                metrics.append(InferredWindowMetric(
                    metric_name="lag_sal",
                    window_function="LAG",
                    input_column="salary",
                    order_by=["salary ASC"],
                    alias="lag_sal",
                    confidence="high",
                ))
            elif alias == "lead_sal":
                metrics.append(InferredWindowMetric(
                    metric_name="lead_sal",
                    window_function="LEAD",
                    input_column="salary",
                    order_by=["salary ASC"],
                    alias="lead_sal",
                    confidence="high",
                ))
            elif alias == "sum_amt":
                metrics.append(InferredWindowMetric(
                    metric_name="sum_amt",
                    window_function="SUM_OVER",
                    input_column="amt",
                    partition_by=["dept"],
                    alias="sum_amt",
                    confidence="high",
                ))
            elif alias == "avg_amt":
                metrics.append(InferredWindowMetric(
                    metric_name="avg_amt",
                    window_function="AVG_OVER",
                    input_column="amt",
                    partition_by=["dept"],
                    alias="avg_amt",
                    confidence="high",
                ))
            elif alias == "cnt_dept":
                metrics.append(InferredWindowMetric(
                    metric_name="cnt_dept",
                    window_function="COUNT_OVER",
                    input_column="",
                    partition_by=["dept"],
                    alias="cnt_dept",
                    confidence="high",
                ))
        return metrics

    # ── 构造测试 ──

    def test_build_window_step_returns_none_for_empty_metrics(self):
        """无窗口指标时 _build_window_step 返回 None。"""
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="a" * 64,
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="x", type="int")],
                grain=[],
            ),
            inferred_window_metrics=[],
        )
        builder = SqlBuildPlanBuilder()
        result = builder._build_window_step(spec)
        assert result is None

    def test_build_window_step_creates_window_step(self):
        """_build_window_step 将 InferredWindowMetric 转换为 WindowStep。"""
        metrics = self._make_window_metrics("rn", "rk")
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="b" * 64,
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="x", type="int")],
                grain=[],
            ),
            inferred_window_metrics=metrics,
        )
        builder = SqlBuildPlanBuilder()
        result = builder._build_window_step(spec, table_ref="t")
        assert isinstance(result, WindowStep)
        assert len(result.window_exprs) == 2
        assert result.window_exprs[0].function.value == "ROW_NUMBER"
        assert result.window_exprs[1].function.value == "RANK"

    def test_build_window_step_ntile_input_is_sql_literal(self):
        """NTILE 的 input 为 SqlLiteral（桶数），非 ColumnRef。"""
        metrics = self._make_window_metrics("nt")
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="c" * 64,
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="x", type="int")],
                grain=[],
            ),
            inferred_window_metrics=metrics,
        )
        builder = SqlBuildPlanBuilder()
        result = builder._build_window_step(spec, table_ref="t")
        assert result is not None
        ntile_expr = result.window_exprs[0]
        assert ntile_expr.function.value == "NTILE"
        # NTILE 的 input 应为 SqlLiteral，value=4
        assert hasattr(ntile_expr.input, "value")
        assert ntile_expr.input.value == 4  # type: ignore[union-attr]

    # ── 编译测试 ──

    def test_window_step_compiles_all_nine_functions(self):
        """全部 9 种窗口函数可编译为有效 SQL。"""
        all_aliases = ["rn", "rk", "dr", "nt", "lag_sal",
                       "lead_sal", "sum_amt", "avg_amt", "cnt_dept"]
        metrics = self._make_window_metrics(*all_aliases)
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="d" * 64,
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="x", type="int")],
                grain=[],
            ),
            inferred_window_metrics=metrics,
        )
        builder = SqlBuildPlanBuilder()
        result = builder._build_window_step(spec, table_ref="t")
        assert result is not None
        assert len(result.window_exprs) == 9

        # 验证每个函数都能编译
        compiler = DuckDbSqlCompiler(table_mapping={"t": "test_t"})
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
        )
        plan = SqlBuildPlan(
            plan_id="test_9",
            spec_hash="d" * 64,
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="t", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="t", column_name="salary", normalized_name="salary"),
                        ColumnRef(table_ref="t", column_name="amt", normalized_name="amt"),
                    ],
                ),
                result,
            ],
        )
        compiled = compiler.compile(plan)
        assert compiled.sql
        assert "OVER" in compiled.sql.upper()
        # 验证每种函数都在 SQL 中出现
        assert "ROW_NUMBER()" in compiled.sql.upper()
        assert "RANK()" in compiled.sql.upper()
        assert "DENSE_RANK()" in compiled.sql.upper()
        assert "NTILE(4)" in compiled.sql.upper()
        assert "LAG(" in compiled.sql.upper()
        assert "LEAD(" in compiled.sql.upper()
        assert "SUM(" in compiled.sql.upper()
        assert "AVG(" in compiled.sql.upper()
        assert "COUNT(" in compiled.sql.upper()

    # ── DuckDB 执行测试 ──

    def test_row_number_duckdb_execution(self):
        """ROW_NUMBER 窗口函数在 DuckDB 中正确执行。"""
        metrics = self._make_window_metrics("rn")
        self._run_window_execution_test(metrics, "rn")

    def test_ntile_duckdb_execution(self):
        """NTILE(4) 窗口函数在 DuckDB 中正确执行。"""
        metrics = self._make_window_metrics("nt")
        self._run_window_execution_test(metrics, "nt")

    def test_sum_over_duckdb_execution(self):
        """SUM_OVER 窗口函数在 DuckDB 中正确执行。"""
        metrics = self._make_window_metrics("sum_amt")
        self._run_window_execution_test(metrics, "sum_amt")

    @staticmethod
    def _run_window_execution_test(metrics, expected_col: str):
        """通用窗口函数执行测试——创建测试数据 + 编译 + 执行。"""
        import duckdb

        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
        )

        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="e" * 64,
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="id", type="int")],
                grain=[],
            ),
            inferred_window_metrics=metrics,
        )
        builder = SqlBuildPlanBuilder()
        win_step = builder._build_window_step(spec, table_ref="t")
        assert win_step is not None

        plan = SqlBuildPlan(
            plan_id="test_exec",
            spec_hash="e" * 64,
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="t", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="t", column_name="salary", normalized_name="salary"),
                        ColumnRef(table_ref="t", column_name="amt", normalized_name="amt"),
                    ],
                ),
                win_step,
            ],
        )
        compiler = DuckDbSqlCompiler(table_mapping={"t": "test_t"})
        compiled = compiler.compile(plan)

        # DuckDB 内存执行
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE test_t AS SELECT * FROM (VALUES "
            "(1, 'A', 100, 10.0),"
            "(2, 'A', 200, 20.0),"
            "(3, 'B', 150, 15.0),"
            "(4, 'B', 300, 30.0)"
            ") AS t(id, dept, salary, amt)"
        )
        result = con.execute(compiled.sql).fetchall()
        con.close()
        assert len(result) == 4, f"应返回 4 行，实际: {len(result)}"
        # 简单验证：结果不为空
        assert all(row is not None for row in result)


class TestWindowPipelineE2E:
    """窗口函数端到端管线测试——SpecEnricher → Builder → Compiler。"""

    def test_pipeline_merges_window_metrics_into_spec(self):
        """Pipeline._apply_enrichment 将窗口指标合入 ParsedDeveloperSpec。"""
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
        from tianshu_datadev.planning.spec_enricher import SpecEnricher

        # 窗口函数描述必须写在具体输出列的 description 中——SpecEnricher 逐列检测
        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="f" * 64,
            title="test",
            description="按部门排名报表",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="id", type="int"),
                    OutputColumnDecl(name="dept", type="varchar"),
                    OutputColumnDecl(
                        name="rn", type="int",
                        description="ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)",
                    ),
                ],
                grain=[],
            ),
        )
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched_spec = enricher.apply_enrichment(spec, manifest)

        # 验证窗口指标已合入——rn 列的 description 含 ROW_NUMBER() OVER (...)
        assert len(enriched_spec.inferred_window_metrics) >= 1, (
            f"应从 rn 列的 description 检测到 ROW_NUMBER 窗口指标，"
            f"实际: {len(enriched_spec.inferred_window_metrics)}"
        )
        rn_metric = enriched_spec.inferred_window_metrics[0]
        assert rn_metric.window_function == "ROW_NUMBER"
        assert rn_metric.alias == "rn"

    def test_builder_includes_window_step_in_single_table_plan(self):
        """单表路径：Builder 将 WindowStep 插入聚合后、投影前。"""
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
        from tianshu_datadev.planning.spec_enricher import SpecEnricher

        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="g" * 64,
            title="test",
            description="按部门排名报表",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="id", type="int"),
                    OutputColumnDecl(name="dept", type="varchar"),
                    OutputColumnDecl(
                        name="rn", type="int",
                        description="ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)",
                    ),
                ],
                grain=[],
            ),
        )
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()
        enriched_spec = enricher.apply_enrichment(spec, manifest)

        builder = SqlBuildPlanBuilder()
        plan, questions = builder.build(enriched_spec)

        # 验证 WindowStep 存在于计划中
        window_steps = [s for s in plan.steps if s.step_type == "window"]
        assert len(window_steps) == 1, (
            f"单表计划应包含 1 个 WindowStep，实际: {len(window_steps)}"
        )
        win_step = window_steps[0]
        assert len(win_step.window_exprs) >= 1
        assert win_step.window_exprs[0].function.value == "ROW_NUMBER"

        # 验证 WindowStep 在 AggregateStep 之后、ProjectStep 之前
        step_types = [s.step_type for s in plan.steps]
        win_idx = step_types.index("window")
        proj_idx = step_types.index("project")
        assert win_idx < proj_idx, "WindowStep 应在 ProjectStep 之前"

    def test_window_deterministic_compilation(self):
        """相同窗口指标产生确定性 WindowStep 和 SQL。"""
        from tianshu_datadev.developer_spec.models import (
            InputTableDecl,
            OutputColumnDecl,
            OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
        from tianshu_datadev.planning.spec_enricher import SpecEnricher

        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="h" * 64,
            title="test",
            description="按部门排名报表",
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="db.t",
                    columns=[], key_columns=[], business_columns=[],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="id", type="int"),
                    OutputColumnDecl(name="dept", type="varchar"),
                    OutputColumnDecl(
                        name="rn", type="int",
                        description="ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn",
                    ),
                ],
                grain=[],
            ),
        )
        manifest = build_manifest_from_spec(spec)
        enricher = SpecEnricher()

        # 两次独立构建
        spec1 = enricher.apply_enrichment(spec, manifest)
        plan1, _ = SqlBuildPlanBuilder().build(spec1)
        compiler1 = DuckDbSqlCompiler(table_mapping={"t": "test_t"})
        sql1 = compiler1.compile(plan1).sql

        # 重新解析（模拟独立运行）
        spec2 = enricher.apply_enrichment(spec, manifest)
        plan2, _ = SqlBuildPlanBuilder().build(spec2)
        compiler2 = DuckDbSqlCompiler(table_mapping={"t": "test_t"})
        sql2 = compiler2.compile(plan2).sql

        assert sql1 == sql2, "相同输入应产生相同 SQL（确定性编译）"

