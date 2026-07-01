"""Pipeline——确定性串联全部组件的执行流水线。

所有步骤使用确定性实现，不需要真实 LLM 或生产数据库。
每次调用独立创建组件实例，无状态泄漏。
API 只返回 artifact 引用和结构化摘要。
"""

from __future__ import annotations

import hashlib

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.packager import PackageInputs, ReviewPackageBuilder
from tianshu_datadev.developer_spec.models import (
    EnrichedSpec,
    FieldSource,
    ManifestColumn,
    ManifestTable,
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.cross_validator import cross_validate
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import SqlArtifact
from tianshu_datadev.sql.validator import SqlBuildPlanValidator


def _summarize_open_questions(
    questions: list,
) -> list[dict]:
    """将 OpenQuestion 列表转换为 API 摘要格式。"""
    return [
        {
            "question_id": q.question_id,
            "source": q.source,
            "description": q.description,
            "blocking": q.blocking,
        }
        for q in questions
    ]


def _summarize_warnings(warnings: list) -> list[dict]:
    """将 ParseWarning 列表转换为 API 摘要格式。"""
    result = []
    for w in warnings:
        severity = w.severity.value if hasattr(w.severity, "value") else str(w.severity)
        result.append({
            "warning_id": w.warning_id,
            "message": w.message,
            "severity": severity,
        })
    return result


def _build_manifest(spec: ParsedDeveloperSpec) -> SourceManifest:
    """从 ParsedDeveloperSpec 构建 SourceManifest——涵盖所有列引用。

    不仅包含 input_tables 中显式声明的列，还从 metrics、dimensions、
    output_spec 中提取被引用但未显式声明的列（以 "varchar" 类型补充）。

    与 tests/sql/test_pipeline_e2e.py 中的 _build_manifest 逻辑一致。
    """
    tables: list[ManifestTable] = []
    for t in spec.input_tables:
        seen: set[str] = set()
        cols: list[ManifestColumn] = []

        def _add(col_name: str) -> None:
            """添加列（去重），从原始声明中查找类型信息。"""
            if col_name in seen:
                return
            seen.add(col_name)
            dtype = "varchar"
            for src_list in [t.columns, t.key_columns, t.business_columns]:
                for c in src_list:
                    if c.column_name == col_name:
                        dtype = c.data_type or "varchar"
                        break
            cols.append(
                ManifestColumn(
                    column_name=col_name,
                    normalized_name=col_name.lower(),
                    data_type=dtype,
                    nullable=True,
                    source=FieldSource.DEVELOPER_SPEC,
                )
            )

        # 从显式声明的列开始
        for c in t.columns + t.key_columns + t.business_columns:
            _add(c.column_name)

        # 从指标引用中提取
        for m in spec.metrics:
            if m.input_column:
                _add(m.input_column)

        # 从维度引用中提取
        for d in spec.dimensions:
            _add(d.column_ref)

        # 从输出列提取
        for col in spec.output_spec.columns:
            _add(col.name)

        # 从排序列提取
        if spec.output_spec.sort:
            for s in spec.output_spec.sort:
                _add(s.column)

        tables.append(
            ManifestTable(
                table_ref=t.table_alias,
                source_table=t.source_table,
                columns=cols,
                estimated_row_count=t.row_count,
            )
        )
    return SourceManifest(
        manifest_id=f"manifest_{spec.spec_hash[:12]}",
        spec_hash=spec.spec_hash,
        tables=tables,
    )


def _auto_table_mapping(spec: ParsedDeveloperSpec) -> dict[str, str]:
    """从 DeveloperSpec 的 source_tables 自动构建 table_mapping（别名 → 物理表名）。

    当 API 请求未显式提供 table_mapping 时，用此函数补齐，
    确保编译器能将 table_ref（如 "ue"）解析为物理表名（如 "dwd.user_events"）。

    Args:
        spec: 已解析的 DeveloperSpec

    Returns:
        {alias: physical_table_name} 映射字典
    """
    mapping: dict[str, str] = {}
    for t in spec.input_tables:
        if t.table_alias and t.source_table:
            mapping[t.table_alias] = str(t.source_table)
    return mapping


class Pipeline:
    """执行流水线——确定性串联全部 6 个组件。

    工作流程：
      parse_only: Parser → 摘要
      build_plan:  Parser → Builder → Validator → 摘要
      execute:     Parser → Builder → Validator → Compiler → Executor → 摘要
      run_all:     Parser → Builder → Validator → Compiler → Executor → Contract → Packager → 摘要
      get_package: 内存存储 → 摘要

    内部维护 _results 和 _packages 字典作为临时存储。
    每次 API 调用独立创建组件实例，无状态泄漏。
    """

    # ── 预设 DeveloperSpec 模板 ──────────────────────────

    TEMPLATES: list[dict] = [
        {
            "template_id": "tpl_aggregation",
            "name": "汇总表",
            "description": "单表聚合——按维度分组统计指标，如日活、销售额汇总",
            "category": "aggregation",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: aggregate_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.metrics_daily\n"
                "  target_grain: [stat_date]\n"
                '  summary: "按日期汇总核心指标"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.user_events\n"
                "      alias: ue\n"
                "      row_count: ~1000万\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: event_time\n"
                "      key_columns:\n"
                "        - name: id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: event_time\n"
                "          type: timestamp\n"
                "          nullable: false\n"
                "        - name: user_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "        - name: event_type\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "\n"
                "  metrics:\n"
                "    - metric_name: pv\n"
                "      aggregation: COUNT  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: id\n"
                "      alias: pv\n"
                "    - metric_name: uv\n"
                "      aggregation: COUNT_DISTINCT  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: user_id\n"
                "      alias: uv\n"
                "\n"
                "  dimensions:\n"
                "    - dimension_name: stat_date\n"
                "      column_ref: stat_date\n"
                "\n"
                "  output_columns:\n"
                "    - name: stat_date\n"
                "      type: date\n"
                "    - name: pv\n"
                "      type: bigint\n"
                "    - name: uv\n"
                "      type: bigint\n"
                "---\n"
                "\n"
                "# 汇总表模板\n"
                "\n"
                "## 业务目标\n"
                "按日期统计 PV 和 UV，产出日报表。\n"
                "```\n"
            ),
        },
        {
            "template_id": "tpl_label_table",
            "name": "标签表",
            "description": "CASE WHEN 分类打标——按条件对数据进行分类标签加工",
            "category": "label",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: label_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.user_labels\n"
                "  target_grain: [user_id]\n"
                '  summary: "用户价值分层标签加工"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.user_orders\n"
                "      alias: uo\n"
                "      row_count: ~500万\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: order_time\n"
                "      key_columns:\n"
                "        - name: user_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: order_amount\n"
                "          type: decimal(18,2)\n"
                "          nullable: true\n"
                "        - name: order_time\n"
                "          type: timestamp\n"
                "          nullable: false\n"
                "\n"
                "  metrics:\n"
                "    - metric_name: total_amount\n"
                "      aggregation: SUM  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: order_amount\n"
                "      alias: total_amount\n"
                "    - metric_name: order_cnt\n"
                "      aggregation: COUNT  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: user_id\n"
                "      alias: order_cnt\n"
                "\n"
                "  dimensions:\n"
                "    - dimension_name: user_id\n"
                "      column_ref: user_id\n"
                "\n"
                "  output_columns:\n"
                "    - name: user_id\n"
                "      type: bigint\n"
                "    - name: total_amount\n"
                "      type: decimal(18,2)\n"
                "    - name: order_cnt\n"
                "      type: bigint\n"
                "    - name: value_level\n"
                "      type: varchar\n"
                "---\n"
                "\n"
                "# 标签表模板\n"
                "\n"
                "## 业务目标\n"
                "按用户汇总消费金额和订单数，输出价值分层标签。\n"
                "```\n"
            ),
        },
        {
            "template_id": "tpl_multi_step",
            "name": "多步骤加工",
            "description": "多表 Join + 聚合——两表关联后分组统计，产出宽表",
            "category": "multi_step",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: aggregate_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.order_analysis\n"
                "  target_grain: [order_date, category]\n"
                '  summary: "订单品类分析——订单表关联商品维度表"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.orders\n"
                "      alias: o\n"
                "      row_count: ~2000万\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: order_date\n"
                "      key_columns:\n"
                "        - name: order_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: order_date\n"
                "          type: date\n"
                "          nullable: false\n"
                "        - name: order_amount\n"
                "          type: decimal(18,2)\n"
                "          nullable: true\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "    - name: dim.product\n"
                "      alias: p\n"
                "      row_count: ~10万\n"
                "      role: dim  # fact（事实表）| dim（维度表）\n"
                "      key_columns:\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: category\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "        - name: product_name\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "\n"
                "  metrics:\n"
                "    - metric_name: order_cnt\n"
                "      aggregation: COUNT  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: order_id\n"
                "      alias: order_cnt\n"
                "    - metric_name: total_amount\n"
                "      aggregation: SUM  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: order_amount\n"
                "      alias: total_amount\n"
                "\n"
                "  dimensions:\n"
                "    - dimension_name: order_date\n"
                "      column_ref: order_date\n"
                "    - dimension_name: category\n"
                "      column_ref: category\n"
                "\n"
                "  joins:\n"
                "    - left_table: o\n"
                "      right_table: p\n"
                "      left_key: product_id\n"
                "      right_key: product_id\n"
                "      join_type: INNER  # INNER（内连接）| LEFT（左连接）| RIGHT（右连接）| FULL（全连接）\n"
                "\n"
                "  output_columns:\n"
                "    - name: order_date\n"
                "      type: date\n"
                "    - name: category\n"
                "      type: varchar\n"
                "    - name: order_cnt\n"
                "      type: bigint\n"
                "    - name: total_amount\n"
                "      type: decimal(18,2)\n"
                "---\n"
                "\n"
                "# 订单品类分析\n"
                "\n"
                "## 业务目标\n"
                "关联订单事实表和商品维度表，按日期和品类统计订单量和金额。\n"
                "```\n"
            ),
        },
        {
            "template_id": "tpl_two_table_join",
            "name": "两表 Join",
            "description": "两表关联——事实表关联维度表，展开宽表字段，不做聚合",
            "category": "join",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: detail_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.order_detail_wide\n"
                "  target_grain: [order_id]\n"
                '  summary: "订单明细宽表——关联商品维度，展开品类和名称"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.orders\n"
                "      alias: o\n"
                "      row_count: ~2000万\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: order_date\n"
                "      key_columns:\n"
                "        - name: order_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: order_date\n"
                "          type: date\n"
                "          nullable: false\n"
                "        - name: order_amount\n"
                "          type: decimal(18,2)\n"
                "          nullable: true\n"
                "        - name: user_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "    - name: dim.product\n"
                "      alias: p\n"
                "      row_count: ~10万\n"
                "      role: dim  # fact（事实表）| dim（维度表）\n"
                "      key_columns:\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: product_name\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "        - name: category\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "        - name: brand\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "\n"
                "  joins:\n"
                "    - left_table: o\n"
                "      right_table: p\n"
                "      left_key: product_id\n"
                "      right_key: product_id\n"
                "      join_type: LEFT  # INNER（内连接）| LEFT（左连接）| RIGHT（右连接）| FULL（全连接）\n"
                "\n"
                "  output_columns:\n"
                "    - name: order_id\n"
                "      type: bigint\n"
                "    - name: order_date\n"
                "      type: date\n"
                "    - name: order_amount\n"
                "      type: decimal(18,2)\n"
                "    - name: user_id\n"
                "      type: bigint\n"
                "    - name: product_name\n"
                "      type: varchar\n"
                "    - name: category\n"
                "      type: varchar\n"
                "    - name: brand\n"
                "      type: varchar\n"
                "---\n"
                "\n"
                "# 订单明细宽表\n"
                "\n"
                "## 业务目标\n"
                "关联订单事实表和商品维度表，展开商品名称、品类、品牌等维度属性，\n"
                "产出订单明细宽表供下游分析使用。\n"
                "```\n"
            ),
        },
        {
            "template_id": "tpl_window_topn",
            "name": "窗口 TopN",
            "description": "窗口函数排名——ROW_NUMBER/RANK 分组排序取 TopN，如各品类销售额 Top10 商品",
            "category": "window",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: aggregate_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.category_top10_product\n"
                "  target_grain: [category, product_id]\n"
                '  summary: "各品类销售额 Top10 商品排名"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.order_items\n"
                "      alias: oi\n"
                "      row_count: ~5000万\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: order_date\n"
                "      key_columns:\n"
                "        - name: item_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: order_date\n"
                "          type: date\n"
                "          nullable: false\n"
                "        - name: sale_amount\n"
                "          type: decimal(18,2)\n"
                "          nullable: true\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "    - name: dim.product\n"
                "      alias: p\n"
                "      row_count: ~10万\n"
                "      role: dim  # fact（事实表）| dim（维度表）\n"
                "      key_columns:\n"
                "        - name: product_id\n"
                "          type: bigint\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: category\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "        - name: product_name\n"
                "          type: varchar\n"
                "          nullable: true\n"
                "\n"
                "  metrics:\n"
                "    - metric_name: total_sales\n"
                "      aggregation: SUM  # COUNT（计数）| SUM（求和）| AVG（平均）| MIN（最小）| MAX（最大）| COUNT_DISTINCT（去重计数）\n"
                "      input_column: sale_amount\n"
                "      alias: total_sales\n"
                "\n"
                "  dimensions:\n"
                "    - dimension_name: category\n"
                "      column_ref: category\n"
                "    - dimension_name: product_id\n"
                "      column_ref: product_id\n"
                "\n"
                "  joins:\n"
                "    - left_table: oi\n"
                "      right_table: p\n"
                "      left_key: product_id\n"
                "      right_key: product_id\n"
                "      join_type: INNER  # INNER（内连接）| LEFT（左连接）| RIGHT（右连接）| FULL（全连接）\n"
                "\n"
                "  output_columns:\n"
                "    - name: category\n"
                "      type: varchar\n"
                "    - name: product_id\n"
                "      type: bigint\n"
                "    - name: product_name\n"
                "      type: varchar\n"
                "    - name: total_sales\n"
                "      type: decimal(18,2)\n"
                "    - name: rank_in_category\n"
                "      type: int\n"
                "---\n"
                "\n"
                "# 各品类销售额 Top10 商品\n"
                "\n"
                "## 业务目标\n"
                "按品类分组，计算每个商品的销售额汇总，使用 ROW_NUMBER 窗口函数\n"
                "按销售额降序排名，取各品类 Top 10 商品。\n"
                "\n"
                "## 窗口函数说明\n"
                "使用 ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_sales DESC) 排名，\n"
                "外层 WHERE rank_in_category <= 10 取 TopN。\n"
                "```\n"
            ),
        },
        {
            "template_id": "tpl_empty",
            "name": "自定义空模板",
            "description": "空白模板——从零开始编写 DeveloperSpec，适合自定义需求",
            "category": "empty",
            "markdown_template": (
                "```markdown\n"
                "---\n"
                "spec:\n"
                "  type: aggregate_table  # aggregate_table（汇总表）| detail_table（明细表）| label_table（标签表）\n"
                "  target_table: ads.目标表名\n"
                "  target_grain: [维度列1, 维度列2]\n"
                '  summary: "一句话描述业务目标"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.源表名\n"
                "      alias: 别名（两个字母）\n"
                "      row_count: ~估算行数\n"
                "      role: fact  # fact（事实表）| dim（维度表）\n"
                "      time_field: 时间字段名      # 如有\n"
                "      key_columns:\n"
                "        - name: 主键列名\n"
                "          type: 类型              # bigint | varchar | ...\n"
                "          nullable: false\n"
                "      business_columns:\n"
                "        - name: 业务列名\n"
                "          type: 类型\n"
                "          nullable: true\n"
                "\n"
                "  metrics:                       # 指标声明（可选）\n"
                "    # - metric_name: 指标名\n"
                "    #   aggregation: SUM（求和）| COUNT（计数）| COUNT_DISTINCT（去重计数）| AVG（平均）| MAX（最大）| MIN（最小）\n"
                "    #   input_column: 输入列名\n"
                "    #   alias: 输出别名\n"
                "\n"
                "  dimensions:                    # 维度声明（可选）\n"
                "    # - dimension_name: 维度名\n"
                "    #   column_ref: 列引用\n"
                "\n"
                "  joins:                         # Join 声明（可选，多表时填写）\n"
                "    # - left_table: 左表别名\n"
                "    #   right_table: 右表别名\n"
                "    #   left_key: 左键列名\n"
                "    #   right_key: 右键列名\n"
                "    #   join_type: INNER（内连接）| LEFT（左连接）| RIGHT（右连接）| FULL（全连接）\n"
                "\n"
                "  output_columns:                # 输出列定义\n"
                "    # - name: 列名\n"
                "    #   type: 类型\n"
                "---\n"
                "\n"
                "# 标题\n"
                "\n"
                "## 业务目标\n"
                "在此描述业务需求和分析目标。\n"
                "\n"
                "## 数据说明\n"
                "在此补充数据源、字段含义、业务口径等说明。\n"
                "```\n"
            ),
        },
    ]

    def __init__(self, base_output_dir: str = "generated/review_packages"):
        """初始化流水线。

        Args:
            base_output_dir: ReviewPackage 输出根目录
        """
        self._base_output_dir = base_output_dir
        self._results: dict[str, dict] = {}  # request_id → 内部产物
        self._packages: dict[str, object] = {}  # request_id → ReviewPackageManifest
        # 多表时生成 Join 推测——llm_client=None 退化为纯显式声明模式
        self._relationship_planner = RelationshipPlanner()
        self._spec_enricher = SpecEnricher()  # 指标推断（LLM 驱动，llm_client=None 退化规则匹配）

    @staticmethod
    def _apply_enrichment(spec: ParsedDeveloperSpec, manifest: SourceManifest, enricher) -> ParsedDeveloperSpec:
        """应用 SpecEnricher 推断——将推断指标合并到 spec 中。

        程序员手写的 metrics 优先级最高（不可覆盖），
        仅追加 inferred_metrics 中不与现有 alias 冲突的条目。

        Phase 5 新增：合入跨粒度 compute_steps + JoinDecl——
        SpecEnricher._detect_cross_grain_dependency 产出放入 enrichment_metadata，
        此处解码并合并到 spec.compute_steps / spec.joins。

        Args:
            spec: 原始 DeveloperSpec
            manifest: 源数据清单
            enricher: SpecEnricher 实例

        Returns:
            增强后的 ParsedDeveloperSpec
        """
        from tianshu_datadev.developer_spec.models import ComputeStep, JoinDecl

        enriched: EnrichedSpec = enricher.enrich(spec, manifest)

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

    @staticmethod
    def _gen_request_id(spec: ParsedDeveloperSpec) -> str:
        """从 spec_hash 生成确定性 request_id。"""
        return f"req_{spec.spec_hash[:12]}"

    def _enrich_and_plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
        table_mapping: dict | None = None,
    ) -> tuple[ParsedDeveloperSpec, object | None, list, dict]:
        """统一入口：SpecEnricher → RelationshipPlanner → 交叉验证。

        消除 5 个入口点中重复的 15 行代码块。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单
            table_mapping: 表名映射（None 时自动推断）

        Returns:
            (spec, hypothesis, extra_questions, table_mapping)
        """
        # 自动表映射
        if not table_mapping:
            table_mapping = _auto_table_mapping(spec)

        # SpecEnricher：从业务描述推断缺失指标
        spec = self._apply_enrichment(spec, manifest, self._spec_enricher)

        # RelationshipPlanner：多表时生成 Join 推测
        extra_questions: list = []
        hypothesis = None
        if len(spec.input_tables) > 1:
            hypothesis, extra_questions = self._relationship_planner.plan(spec, manifest)

        # 交叉验证——指标推断 vs Join 推断一致性检查
        if hypothesis:
            xv_questions = cross_validate(spec, hypothesis, manifest)
            extra_questions.extend(xv_questions)

        return spec, hypothesis, extra_questions, table_mapping or {}

    # ── 错误处理辅助方法 ─────────────────────────────────

    @staticmethod
    def _capture_error(stage: str, exc: Exception) -> dict:
        """将异常封装为结构化错误信息。

        Args:
            stage: 失败阶段标识（parser/enrich/build/compile/execute）
            exc: 捕获的异常

        Returns:
            含 stage、error_type、error_message 的 dict
        """
        return {
            "stage": stage,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

    @staticmethod
    def _build_pipeline_stages(
        failed_stage: str,
        error_info: dict | None = None,
        all_stages: list[str] | None = None,
    ) -> list[dict]:
        """构建流水线阶段状态列表——失败阶段之前为 ok，自身为 failed，之后为 skipped。

        Args:
            failed_stage: 失败的阶段标识
            error_info: 失败阶段的错误详情（可选，合并到 failed 条目）
            all_stages: 完整阶段列表（默认 5 阶段，run_all 用 7 阶段）

        Returns:
            阶段状态列表，前端据此渲染指示灯
        """
        if all_stages is None:
            all_stages = ["parser", "enrich", "build", "compile", "execute"]
        stages = []
        for s in all_stages:
            if s == failed_stage:
                entry: dict = {"stage": s, "status": "failed"}
                if error_info:
                    entry.update(error_info)
                stages.append(entry)
            elif all_stages.index(s) < all_stages.index(failed_stage):
                stages.append({"stage": s, "status": "ok"})
            else:
                stages.append({"stage": s, "status": "skipped"})
        return stages

    @staticmethod
    def _stage_name_cn(stage: str) -> str:
        """返回阶段的中文名称。"""
        _names = {
            "parser": "解析",
            "enrich": "增强",
            "build": "构建",
            "compile": "编译",
            "execute": "执行",
            "contract": "契约",
            "package": "打包",
        }
        return _names.get(stage, stage)

    # ── 公共方法 ──────────────────────────────────────────

    def parse_only(self, markdown_text: str) -> dict:
        """仅解析 DeveloperSpec——返回 SpecParseResponse 的 dict。

        解析失败时返回 200 + pipeline_error，保留错误信息供前端展示。

        Args:
            markdown_text: DeveloperSpec Markdown 全文

        Returns:
            符合 SpecParseResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        # ── Stage: parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
        except Exception as e:
            print(f"[Pipeline] parse_only: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {"parsed_spec": spec}

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "spec_hash": spec.spec_hash,
            "title": spec.title,
            "table_count": len(spec.input_tables),
            "metric_count": len(spec.metrics),
            "dimension_count": len(spec.dimensions),
            "has_joins": bool(spec.joins),
            "has_time_range": spec.time_range is not None,
            "open_question_count": len(spec.open_questions),
            "warning_count": len(spec.parse_warnings),
            "open_questions": _summarize_open_questions(spec.open_questions),
            "parse_warnings": _summarize_warnings(spec.parse_warnings),
        }

    def build_plan(self, markdown_text: str, table_mapping: dict[str, str] | None = None) -> dict:
        """解析 + 构建 SqlBuildPlan + Validator 验证——返回 PlanResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = _build_manifest(spec)
        except Exception as e:
            print(f"[Pipeline] build_plan: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            print(f"[Pipeline] build_plan: enrich 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            self._results[request_id] = {"parsed_spec": spec, "manifest": manifest}
            error_info = self._capture_error("enrich", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("enrich", error_info),
            }

        # ── Stage 3: Build + Validate ──
        plan = None
        plan_questions: list = []
        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径：每步独立聚合 Plan，_temp 串联 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径：每对候选独立 Plan，_temp 串联 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate(plan, manifest)
                sql_program = self._build_sql_program(plan, spec.spec_hash)

        except Exception as e:
            print(f"[Pipeline] build_plan: build 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            self._results[request_id] = partial
            error_info = self._capture_error("build", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": plan.plan_id if plan is not None else "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("build", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "table_mapping": table_mapping or {},
        }

        all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)
        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "step_count": len(plan.steps),
            "step_types": [s.step_type for s in plan.steps],
            "multi_table": plan.multi_table,
            "validation_passed": passed,
            "open_questions": _summarize_open_questions(all_questions),
        }

    def execute(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """全流程：解析 → 构建 → 验证 → 编译 → 执行——返回 ExecuteResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。
        成功路径返回值结构不变。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（传给 Compiler）
            table_paths: 物理表名 → CSV 文件路径（传给 Executor）

        Returns:
            符合 ExecuteResponse 结构的 dict，失败时额外含 pipeline_error + pipeline_stages
        """
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = _build_manifest(spec)
        except Exception as e:
            print(f"[Pipeline] execute: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            print(f"[Pipeline] execute: enrich 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            self._results[request_id] = {"parsed_spec": spec, "manifest": manifest}
            error_info = self._capture_error("enrich", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("enrich", error_info),
            }

        # ── Stage 3-5: Build → Compile → Execute（按分支） ──
        # 跨阶段变量——初始化为 None，按阶段赋值
        plan = None
        compiled = None
        program_artifact = None
        all_questions: list = []
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                _chain_passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = program_result.results[-1]
                trace = last_result.trace
                summary = last_result.summary
                compiled = program_artifact.compiled.statements[-1]
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                _chain_passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = program_result.results[-1]
                trace = last_result.trace
                summary = last_result.summary
                compiled = program_artifact.compiled.statements[-1]
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                # Validator 验证（非阻断——记录问题供排查）
                validator = SqlBuildPlanValidator()
                _passed, val_questions = validator.validate(plan, manifest)
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                compiled = compiler.compile(plan)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                trace, summary = execute_executor.execute(compiled)

        except Exception as e:
            # ── 错误处理：日志 + 保留已完成产物 + 返回部分结果 ──
            print(f"[Pipeline] execute: {stage} 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            # 保存已完成产物供事后查询
            partial: dict = {
                "parsed_spec": spec,
                "manifest": manifest,
            }
            if plan is not None:
                partial["plan"] = plan
            if compiled is not None:
                partial["compiled"] = compiled
            elif program_artifact is not None:
                partial["program_artifact"] = program_artifact
            partial["table_mapping"] = table_mapping or {}
            self._results[request_id] = partial

            # 提取部分可用字段——根据已完成的阶段
            _plan_id = plan.plan_id if plan is not None else ""
            _sql_sha256 = ""
            _compiler_ver = ""
            if compiled is not None:
                _sql_sha256 = getattr(compiled, "sql_sha256", "")
                _compiler_ver = getattr(compiled, "compiler_version", "")
            elif program_artifact is not None:
                try:
                    _sql_sha256 = program_artifact.compiled.statements[-1].sql_sha256
                except (IndexError, AttributeError):
                    pass
                _compiler_ver = getattr(program_artifact, "compiler_version", "")

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "sql_sha256": _sql_sha256,
                "compiler_version": _compiler_ver,
                "execution_trace": None,
                "result_summary": None,
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
            }

        # ── 成功路径——现有逻辑不变 ──
        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled,
            "trace": trace,
            "summary": summary,
            "table_mapping": table_mapping or {},
        }

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": trace.status.value if hasattr(trace.status, "value") else str(trace.status),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "sql_sha256": compiled.sql_sha256,
            "compiler_version": compiled.compiler_version,
            "open_questions": _summarize_open_questions(all_questions),
        }

    @staticmethod
    def _build_sql_program(plan: SqlBuildPlan, spec_hash: str) -> SqlProgram:
        """从单个 SqlBuildPlan 构建最小 SqlProgram（单语句 STANDALONE）。

        这是 Pipeline 自动化多语句构建的基础——
        当前将单 plan 包装为单语句 SqlProgram，
        未来多语句拆分逻辑在此扩展（如按 _temp 依赖拆分）。

        Args:
            plan: SqlBuildPlan 实例
            spec_hash: 对应 DeveloperSpec 的 SHA-256

        Returns:
            含单个 STANDALONE 语句的 SqlProgram
        """
        stmt = SqlStatement(
            statement_id=plan.plan_id,
            plan=plan,
            kind=StatementKind.STANDALONE,
        )
        builder = SqlProgramBuilder()
        return builder.build_from_statements(
            statements=[stmt],
            spec_hash=spec_hash,
            final_output=plan.plan_id,
        )

    @staticmethod
    def _build_sql_program_from_chain(
        plans: list[SqlBuildPlan], spec_hash: str, chain_id: str
    ) -> SqlProgram:
        """从多 Plan 链构建 SqlProgram——每步 PRODUCER/FINAL，通过 _temp 串联。

        中间 Plan 标记为 PRODUCER，产生 _temp 中间表供下游消费。
        最终 Plan 标记为 FINAL，产生最终输出。
        """
        statements: list[SqlStatement] = []
        for idx, plan in enumerate(plans):
            is_final = (idx == len(plans) - 1)
            produces = None if is_final else f"_temp_c{chain_id}_{idx}"
            depends_on = [plans[idx - 1].plan_id] if idx > 0 else []

            stmt = SqlStatement(
                statement_id=plan.plan_id,
                plan=plan,
                kind=StatementKind.FINAL if is_final else StatementKind.PRODUCER,
                depends_on=depends_on,
                produces=produces,
            )
            statements.append(stmt)

        builder = SqlProgramBuilder()
        return builder.build_from_statements(
            statements=statements,
            spec_hash=spec_hash,
            final_output=plans[-1].plan_id,
        )

    @staticmethod
    def _build_sql_program_from_compute_steps(
        plans: list[SqlBuildPlan],
        spec,  # ParsedDeveloperSpec（含 compute_steps）
        chain_id: str,
    ) -> SqlProgram:
        """从 ComputeSteps Plan 链构建 SqlProgram——使用 step_name 命名 _temp 表。

        与 _build_sql_program_from_chain 的区别：
        - 使用 spec.compute_steps 的 step_name 命名 _temp 表（而非 idx）
        - 确保 produces 与 Builder 中 ScanStep 的 table_ref 一致
        - 支持 DAG 依赖——合流步骤 depends_on 包含所有上游 plan_id
        """
        steps = spec.compute_steps or []
        # 构建 step_name → plan 映射（含 plan_id）
        step_plan_map: dict[str, SqlBuildPlan] = {}
        for cs, plan in zip(steps, plans):
            step_plan_map[cs.step_name] = plan

        statements: list[SqlStatement] = []
        # 跟踪哪些步骤被其他步骤依赖——不被依赖的为 FINAL
        consumed: set[str] = set()
        for cs in steps:
            src_list = cs.source if isinstance(cs.source, list) else [cs.source]
            for src in src_list:
                if src != "input" and src in step_plan_map:
                    consumed.add(src)

        for cs, plan in zip(steps, plans):
            is_final = cs.step_name not in consumed
            src_list = cs.source if isinstance(cs.source, list) else [cs.source]

            # 计算依赖——所有非 input 的上游 step
            depends_on: list[str] = []
            for src in src_list:
                if src != "input" and src in step_plan_map:
                    depends_on.append(step_plan_map[src].plan_id)

            # 使用 step_name 命名 _temp 表——与 Builder 中 ScanStep.table_ref 一致
            produces = (
                None
                if is_final
                else f"_temp_c{chain_id}_{cs.step_name}"
            )

            stmt = SqlStatement(
                statement_id=plan.plan_id,
                plan=plan,
                kind=StatementKind.FINAL if is_final else StatementKind.PRODUCER,
                depends_on=depends_on,
                produces=produces,
            )
            statements.append(stmt)

        # final_output 为所有 FINAL 语句的最后一个
        final_plans = [
            s for s in statements if s.kind == StatementKind.FINAL
        ]
        final_output = final_plans[-1].statement_id if final_plans else statements[-1].statement_id

        builder = SqlProgramBuilder()
        return builder.build_from_statements(
            statements=statements,
            spec_hash=spec.spec_hash,
            final_output=final_output,
        )

    def run_all(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """全流程 + ReviewPackage 打包——返回 RunAllResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。
        7 阶段：parser → enrich → build → compile → execute → contract → package。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 RunAllResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        _RUN_ALL_STAGES = ["parser", "enrich", "build", "compile", "execute", "contract", "package"]

        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = _build_manifest(spec)
        except Exception as e:
            print(f"[Pipeline] run_all: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info, _RUN_ALL_STAGES),
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            print(f"[Pipeline] run_all: enrich 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            self._results[request_id] = {"parsed_spec": spec, "manifest": manifest}
            error_info = self._capture_error("enrich", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("enrich", error_info, _RUN_ALL_STAGES),
            }

        # ── Stage 3-7: Build → Compile → Execute → Contract → Package ──
        plan = None
        compiled_sql = None
        program_artifact = None
        artifact = None
        contract = None
        package_manifest = None
        trace = None
        summary = None
        execution_trace = None
        plan_questions: list = []
        val_questions: list = []
        passed = False
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = executor.execute_program(
                    program_artifact.compiled
                )
                execution_trace = program_result.results[-1].trace if program_result.results else None

                stage = "contract"
                extractor = DataTransformContractExtractor()
                contract = extractor.extract_v1(sql_program)

                stage = "package"
                request_id = self._gen_request_id(spec)
                package_inputs = PackageInputs(
                    request_id=request_id,
                    original_spec_md=markdown_text,
                    parsed_spec=spec.model_dump(),
                    source_manifest=manifest.model_dump(),
                    sql_build_plan=plan.model_dump(),
                    sql_artifact=SqlArtifact(
                        artifact_id=SqlArtifact.generate_artifact_id(
                            plan.plan_id, program_artifact.compiler_version
                        ),
                        compiled_sql=compiled_sql,
                        spec_hash=spec.spec_hash,
                        plan_id=plan.plan_id,
                    ).model_dump(),
                    execution_trace=execution_trace.model_dump() if execution_trace else {},
                    result_summary=(
                        program_result.results[-1].summary.model_dump()
                        if program_result and program_result.results else {}
                    ),
                    data_transform_contract=contract.model_dump(),
                    open_questions=[],
                    validation_questions=[],
                    perf_results=[],
                    retry_count=0,
                )
                packager = ReviewPackageBuilder()
                package_manifest = packager.build(package_inputs)
                self._results[request_id] = {
                    "package": package_manifest,
                    "sql_artifact": SqlArtifact(
                        artifact_id=SqlArtifact.generate_artifact_id(
                            plan.plan_id, program_artifact.compiler_version
                        ),
                        compiled_sql=compiled_sql,
                        spec_hash=spec.spec_hash,
                        plan_id=plan.plan_id,
                    ),
                    "contract": contract,
                    "plan": plan,
                    "parsed_spec": spec,
                    "manifest": manifest,
                    "table_mapping": table_mapping or {},
                }

                # ComputeSteps 路径独立返回
                return {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "plan_id": plan.plan_id,
                    "validation_passed": passed,
                    "execution_status": execution_trace.status if execution_trace else "not_executed",
                    "row_count": execution_trace.row_count if execution_trace else 0,
                    "elapsed_ms": execution_trace.execution_time_ms if execution_trace else 0,
                    "open_questions": _summarize_open_questions(
                        list(plan_questions) + list(val_questions) + list(extra_questions)
                    ),
                    "contract_id": contract.contract_id,
                    "package_id": package_manifest.package_id,
                    "contract": contract.model_dump() if hasattr(contract, "model_dump") else {},
                    "compiled": compiled_sql,
                    "package_manifest": package_manifest.model_dump(
                        exclude_none=True
                    ) if hasattr(package_manifest, "model_dump") else {},
                }

            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = self._build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                trace = program_result.results[-1].trace
                summary = program_result.results[-1].summary
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate(plan, manifest)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                artifact = compiler.compile_to_artifact(plan, spec.spec_hash)
                compiled_sql = artifact.compiled_sql

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                trace, summary = execute_executor.execute(compiled_sql)

                sql_program = self._build_sql_program(plan, spec.spec_hash)

            # ── 公共阶段：Contract + Package（非 ComputeSteps 路径） ──
            stage = "contract"
            contract_extractor = DataTransformContractExtractor()
            if len(sql_program.statements) > 1:
                contract = contract_extractor.extract_v1(sql_program)
            else:
                contract = contract_extractor.extract(plan)

            stage = "package"
            request_id = self._gen_request_id(spec)
            packager = ReviewPackageBuilder(self._base_output_dir)
            package_inputs = PackageInputs(
                request_id=request_id,
                original_spec_md=markdown_text,
                parsed_spec=spec.model_dump(),
                source_manifest=manifest.model_dump(),
                sql_build_plan=plan.model_dump(),
                sql_artifact=(
                    artifact.model_dump()
                    if artifact is not None
                    else program_artifact.model_dump()
                ),
                execution_trace=trace.model_dump(),
                result_summary=summary.model_dump(),
                data_transform_contract=contract.model_dump(),
                open_questions=[q.model_dump() for q in spec.open_questions + plan_questions + extra_questions],
                validation_questions=[q.model_dump() for q in val_questions],
                perf_results=[],
                retry_count=0,
            )
            package_manifest = packager.build(package_inputs)

        except Exception as e:
            print(f"[Pipeline] run_all: {stage} 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            # 保存已完成产物
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            if compiled_sql is not None:
                partial["compiled"] = compiled_sql
            elif program_artifact is not None:
                partial["program_artifact"] = program_artifact
            elif artifact is not None:
                partial["artifact"] = artifact
            if contract is not None:
                partial["contract"] = contract
            partial["table_mapping"] = table_mapping or {}
            self._results[request_id] = partial

            _plan_id = plan.plan_id if plan is not None else ""
            _contract_id = contract.contract_id if contract is not None else ""
            _package_id = package_manifest.package_id if package_manifest is not None else ""

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "validation_passed": passed,
                "execution_status": "not_executed",
                "row_count": 0,
                "elapsed_ms": 0,
                "open_questions": [],
                "contract_id": _contract_id,
                "package_id": _package_id,
                "contract": contract.model_dump() if contract is not None and hasattr(contract, "model_dump") else {},
                "compiled": compiled_sql,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info, _RUN_ALL_STAGES),
            }

        # ── 成功路径（非 ComputeSteps） ──
        self._results[request_id] = {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled_sql,
            "trace": trace,
            "summary": summary,
            "table_mapping": table_mapping or {},
        }
        self._packages[request_id] = package_manifest

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "package_id": package_manifest.package_id,
            "package_dir": f"{self._base_output_dir}/{request_id}",
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": trace.status.value if hasattr(trace.status, "value") else str(trace.status),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "open_questions": _summarize_open_questions(
                list(plan_questions) + list(val_questions) + list(extra_questions)
            ),
            "artifact_count": len(package_manifest.artifacts),
        }

    def get_package(self, request_id: str) -> dict | None:
        """获取已打包的 ReviewPackageManifest。

        Args:
            request_id: 请求唯一标识

        Returns:
            符合 PackageResponse 结构的 dict，不存在时返回 None
        """
        manifest = self._packages.get(request_id)
        if manifest is None:
            return None
        return {
            "request_id": manifest.request_id,
            "package_id": manifest.package_id,
            "created_at": manifest.created_at,
            "artifacts": [a.model_dump() for a in manifest.artifacts],
            "artifact_count": len(manifest.artifacts),
            "spec_hash": manifest.spec_hash,
            "retry_count": manifest.retry_count,
        }

    # ── Phase 4.5B 前端 SPA 专用方法 ──────────────────────

    def get_templates(self) -> list[dict]:
        """获取预设的 DeveloperSpec 模板列表。

        Returns:
            模板定义列表（不含 markdown_template 时的精简版用于列表展示）
        """
        return [
            {
                "template_id": t["template_id"],
                "name": t["name"],
                "description": t["description"],
                "category": t["category"],
            }
            for t in self.TEMPLATES
        ]

    def get_template(self, template_id: str) -> dict | None:
        """获取指定模板的完整定义（含 markdown_template）。

        Args:
            template_id: 模板唯一标识

        Returns:
            完整模板定义 dict，不存在时返回 None
        """
        for t in self.TEMPLATES:
            if t["template_id"] == template_id:
                return dict(t)
        return None

    @staticmethod
    def _step_to_summary(step) -> dict:
        """将单个 SqlBuildPlan step 转换为前端可用的摘要。

        根据 step_type 提取关键信息生成人类可读的描述。
        """
        desc_parts = []
        stype = step.step_type
        if stype == "scan":
            cols = [c.column_name for c in step.required_columns[:5]]
            more = f" +{len(step.required_columns) - 5}" if len(step.required_columns) > 5 else ""
            desc_parts.append(f"扫描表 {step.table_ref}，读取列: {', '.join(cols)}{more}")
        elif stype == "filter":
            desc_parts.append(f"过滤: {step.predicate.operator}")
        elif stype == "join":
            keys = [f"{lk.column_name}={rk.column_name}" for lk, rk in step.join_keys]
            desc_parts.append(f"Join {step.right_table_ref} ({step.join_type}) ON {', '.join(keys)}")
        elif stype == "aggregate":
            gk = [k.column_name for k in step.group_keys]
            ms = [m.alias for m in step.metrics]
            desc_parts.append(f"按 {', '.join(gk)} 分组，聚合: {', '.join(ms)}")
        elif stype == "project":
            cols = [a.alias for a in step.columns[:5]]
            if len(step.columns) > 5:
                cols.append(f"+{len(step.columns) - 5}")
            desc_parts.append(f"投影列: {', '.join(cols)}")
        elif stype == "sort":
            sc = [f"{s.column} {s.direction}" for s in step.sort_keys]
            desc_parts.append(f"排序: {', '.join(sc)}")
        elif stype == "limit":
            desc_parts.append(f"限制行数: {step.limit_count}")
        elif stype == "case_when":
            desc_parts.append(f"CASE WHEN 分支数: {len(step.branches)}")
        else:
            desc_parts.append(f"步骤类型: {stype}")
        return {
            "step_type": stype,
            "step_id": step.step_id,
            "description": "；".join(desc_parts) if desc_parts else stype,
        }

    @staticmethod
    def _extract_join_evidence(plan) -> list[dict]:
        """从 SqlBuildPlan 的 join_hypothesis 中提取 Join 证据。

        Args:
            plan: SqlBuildPlan 实例

        Returns:
            JoinEvidenceItem dict 列表
        """
        evidence_list = []
        if not hasattr(plan, "join_hypothesis") or plan.join_hypothesis is None:
            return evidence_list
        hypothesis = plan.join_hypothesis
        if not hasattr(hypothesis, "candidates"):
            return evidence_list
        for candidate in hypothesis.candidates:
            item = {
                "evidence_id": getattr(candidate, "candidate_id", ""),
                "level": _safe_enum_value(candidate, "level"),
                "action": _safe_enum_value(candidate, "action"),
                "left_table": getattr(candidate, "left_table", ""),
                "right_table": getattr(candidate, "right_table", ""),
                "left_key_raw": getattr(candidate, "left_key_raw", ""),
                "right_key_raw": getattr(candidate, "right_key_raw", ""),
                "left_key_normalized": getattr(candidate, "left_key_normalized", ""),
                "right_key_normalized": getattr(candidate, "right_key_normalized", ""),
                "evidence_checks": list(getattr(candidate, "evidence_checks", [])),
                "detail": getattr(candidate, "detail", ""),
                "evidence_chain_yaml": getattr(candidate, "evidence_chain_yaml", ""),
            }
            evidence_list.append(item)
        return evidence_list

    @staticmethod
    def _build_file_tree(artifacts: list) -> list[dict]:
        """从 artifact 清单构建文件树结构。

        将扁平的 artifact 路径列表转换为嵌套树结构供前端渲染。

        Args:
            artifacts: Artifact 模型列表（每项含 path、sha256 属性）

        Returns:
            树节点 dict 列表
        """
        # 按路径分组构建树
        tree_root: dict[str, dict] = {}

        for a in artifacts:
            path = getattr(a, "path", "")
            sha = getattr(a, "sha256", "")
            if not path:
                continue
            parts = path.replace("\\", "/").split("/")
            current = tree_root
            for i, part in enumerate(parts):
                if part not in current:
                    is_file = (i == len(parts) - 1)
                    current[part] = {
                        "name": part,
                        "path": "/".join(parts[: i + 1]),
                        "kind": "file" if is_file else "directory",
                        "sha256": sha if is_file else None,
                        "_children": {},
                    }
                node = current[part]
                if i < len(parts) - 1:
                    current = node["_children"]
                else:
                    # 文件节点：更新 sha256
                    node["sha256"] = sha

        def _to_list(node_dict: dict) -> list[dict]:
            """将内部 dict 树转换为有序列表，去除 _children 内部键。"""
            result = []
            for name, node in sorted(node_dict.items()):
                children = _to_list(node.pop("_children", {}))
                node["children"] = children
                result.append(node)
            return result

        return _to_list(tree_root)

    def parse_rich(self, markdown_text: str) -> dict:
        """前端专用：完整解析 DeveloperSpec——返回 SpecRichResponse dict。

        包含全部结构化解析结果：表、字段、指标、维度、Join、时间范围等。
        解析失败时返回 200 + pipeline_error。

        Args:
            markdown_text: DeveloperSpec Markdown 全文

        Returns:
            符合 SpecRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        # ── Stage: parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
        except Exception as e:
            print(f"[Pipeline] parse_rich: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {"parsed_spec": spec}

        # 构建表声明摘要
        tables = []
        for t in spec.input_tables:
            tables.append({
                "table_alias": t.table_alias,
                "source_table": str(t.source_table),
                "row_count": t.row_count,
                "role": t.role,
                "column_count": len(t.columns) + len(t.key_columns) + len(t.business_columns),
                "has_time_field": t.time_field is not None,
                "has_partition": t.partition_field is not None,
            })

        # 构建 Join 声明摘要
        joins = []
        for j in (spec.joins or []):
            joins.append({
                "left_table": j.left_table,
                "right_table": j.right_table,
                "left_key": j.left_key,
                "right_key": j.right_key,
                "join_type": _safe_enum_value(j, "join_type"),
            })

        # 构建时间范围摘要
        time_range = None
        if spec.time_range:
            time_range = {
                "column_ref": spec.time_range.column_ref,
                "start": spec.time_range.start,
                "end": spec.time_range.end,
                "inclusive": spec.time_range.inclusive,
            }

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "spec_hash": spec.spec_hash,
            "title": spec.title,
            "description": spec.description,
            "tables": tables,
            "metrics": [
                {"metric_name": m.metric_name, "aggregation": _safe_enum_value(m, "aggregation"),
                 "input_column": m.input_column, "alias": m.alias}
                for m in spec.metrics
            ],
            "dimensions": [
                {"dimension_name": d.dimension_name, "column_ref": d.column_ref}
                for d in spec.dimensions
            ],
            "joins": joins,
            "time_range": time_range,
            "output_spec": {
                "columns": [c.model_dump() for c in spec.output_spec.columns],
                "grain": spec.output_spec.grain,
                "sort_columns": [s.column for s in (spec.output_spec.sort or [])],
                "limit": spec.output_spec.limit,
            },
            "open_questions": _summarize_open_questions(spec.open_questions),
            "parse_warnings": _summarize_warnings(spec.parse_warnings),
        }

    def build_plan_rich(
        self, markdown_text: str, table_mapping: dict[str, str] | None = None,
    ) -> dict:
        """前端专用：解析 + 构建 Plan + 提取 Join 证据——返回 PlanRichResponse dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = _build_manifest(spec)
        except Exception as e:
            print(f"[Pipeline] build_plan_rich: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            print(f"[Pipeline] build_plan_rich: enrich 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            self._results[request_id] = {"parsed_spec": spec, "manifest": manifest}
            error_info = self._capture_error("enrich", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("enrich", error_info),
            }

        # ── Stage 3: Build + Validate ──
        plan = None
        try:
            builder = SqlBuildPlanBuilder()
            plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

            validator = SqlBuildPlanValidator()
            passed, val_questions = validator.validate(plan, manifest)
        except Exception as e:
            print(f"[Pipeline] build_plan_rich: build 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            self._results[request_id] = partial
            error_info = self._capture_error("build", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": plan.plan_id if plan is not None else "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("build", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "table_mapping": table_mapping or {},
        }

        all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

        # 提取步骤摘要
        steps = [self._step_to_summary(s) for s in plan.steps]

        # 提取 Join 证据
        join_evidence = self._extract_join_evidence(plan)

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "step_count": len(plan.steps),
            "step_types": [s.step_type for s in plan.steps],
            "steps": steps,
            "multi_table": plan.multi_table,
            "validation_passed": passed,
            "open_questions": _summarize_open_questions(all_questions),
            "join_evidence": join_evidence,
        }

    def execute_rich(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """前端专用：全流程编译+执行——返回 ExecuteRichResponse dict（含 SQL 文本）。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 ExecuteRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = _build_manifest(spec)
        except Exception as e:
            print(f"[Pipeline] execute_rich: parser 阶段失败 - {type(e).__name__}: {e}")
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            print(f"[Pipeline] execute_rich: enrich 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            self._results[request_id] = {"parsed_spec": spec, "manifest": manifest}
            error_info = self._capture_error("enrich", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("enrich", error_info),
            }

        # ── Stage 3-5: Build → Compile → Execute ──
        plan = None
        compiled = None
        all_questions: list = []
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()
            plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

            validator = SqlBuildPlanValidator()
            _passed, val_questions = validator.validate(plan, manifest)
            all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

            stage = "compile"
            compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
            compiled = compiler.compile(plan)

            stage = "execute"
            executor = DuckDBExecutor(table_paths=table_paths or {})
            trace, summary = executor.execute(compiled)

        except Exception as e:
            print(f"[Pipeline] execute_rich: {stage} 阶段失败 - {type(e).__name__}: {e}")
            request_id = self._gen_request_id(spec)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            if compiled is not None:
                partial["compiled"] = compiled
            partial["table_mapping"] = table_mapping or {}
            self._results[request_id] = partial

            _plan_id = plan.plan_id if plan is not None else ""
            _sql_sha256 = getattr(compiled, "sql_sha256", "") if compiled is not None else ""
            _compiler_ver = getattr(compiled, "compiler_version", "") if compiled is not None else ""

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "generated_sql": getattr(compiled, "sql", "") if compiled is not None else "",
                "sql_sha256": _sql_sha256,
                "compiler_version": _compiler_ver,
                "execution_trace": None,
                "result_summary": None,
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
            }

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
        }

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "generated_sql": compiled.sql,
            "sql_sha256": compiled.sql_sha256,
            "compiler_version": compiled.compiler_version,
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": _safe_enum_value(trace, "status"),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "open_questions": _summarize_open_questions(all_questions),
        }

    def get_package_rich(self, request_id: str) -> dict | None:
        """前端专用：获取 ReviewPackage 文件树——返回 PackageRichResponse dict。

        Args:
            request_id: 请求唯一标识

        Returns:
            符合 PackageRichResponse 结构的 dict，不存在时返回 None
        """
        manifest = self._packages.get(request_id)
        if manifest is None:
            return None
        file_tree = self._build_file_tree(manifest.artifacts)
        return {
            "request_id": manifest.request_id,
            "package_id": manifest.package_id,
            "created_at": manifest.created_at,
            "artifact_count": len(manifest.artifacts),
            "spec_hash": manifest.spec_hash,
            "retry_count": manifest.retry_count,
            "file_tree": file_tree,
        }


def _safe_enum_value(obj, attr: str) -> str:
    """安全获取枚举属性的字符串值——兼容 Enum 和普通属性。"""
    val = getattr(obj, attr, "")
    if hasattr(val, "value"):
        return val.value
    return str(val)
