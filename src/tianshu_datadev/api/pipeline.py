"""FakePipeline——确定性串联全部组件的假执行流水线。

所有步骤使用 Fake/确定性实现，不需要真实 LLM 或生产数据库。
每次调用独立创建组件实例，无状态泄漏。
API 只返回 artifact 引用和结构化摘要。
"""

from __future__ import annotations

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.packager import PackageInputs, ReviewPackageBuilder
from tianshu_datadev.developer_spec.models import (
    FieldSource,
    ManifestColumn,
    ManifestTable,
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
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
        for col_name in spec.output_spec.columns:
            _add(col_name)

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


class FakePipeline:
    """假执行流水线——确定性串联全部 6 个组件。

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
                "---\n"
                "spec:\n"
                "  type: aggregate_table\n"
                "  target_table: ads.metrics_daily\n"
                "  target_grain: [stat_date]\n"
                '  summary: "按日期汇总核心指标"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.user_events\n"
                "      alias: ue\n"
                "      row_count: ~1000万\n"
                "      role: fact\n"
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
                "      aggregation: COUNT\n"
                "      input_column: id\n"
                "      alias: pv\n"
                "    - metric_name: uv\n"
                "      aggregation: COUNT_DISTINCT\n"
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
            ),
        },
        {
            "template_id": "tpl_label_table",
            "name": "标签表",
            "description": "CASE WHEN 分类打标——按条件对数据进行分类标签加工",
            "category": "label",
            "markdown_template": (
                "---\n"
                "spec:\n"
                "  type: label_table\n"
                "  target_table: ads.user_labels\n"
                "  target_grain: [user_id]\n"
                '  summary: "用户价值分层标签加工"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.user_orders\n"
                "      alias: uo\n"
                "      row_count: ~500万\n"
                "      role: fact\n"
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
                "      aggregation: SUM\n"
                "      input_column: order_amount\n"
                "      alias: total_amount\n"
                "    - metric_name: order_cnt\n"
                "      aggregation: COUNT\n"
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
            ),
        },
        {
            "template_id": "tpl_multi_step",
            "name": "多步骤加工",
            "description": "多表 Join + 聚合——两表关联后分组统计，产出宽表",
            "category": "multi_step",
            "markdown_template": (
                "---\n"
                "spec:\n"
                "  type: aggregate_table\n"
                "  target_table: ads.order_analysis\n"
                "  target_grain: [order_date, category]\n"
                '  summary: "订单品类分析——订单表关联商品维度表"\n'
                "\n"
                "  source_tables:\n"
                "    - name: dwd.orders\n"
                "      alias: o\n"
                "      row_count: ~2000万\n"
                "      role: fact\n"
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
                "      role: dim\n"
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
                "      aggregation: COUNT\n"
                "      input_column: order_id\n"
                "      alias: order_cnt\n"
                "    - metric_name: total_amount\n"
                "      aggregation: SUM\n"
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
                "      join_type: INNER\n"
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

    @staticmethod
    def _gen_request_id(spec: ParsedDeveloperSpec) -> str:
        """从 spec_hash 生成确定性 request_id。"""
        return f"req_{spec.spec_hash[:12]}"

    # ── 公共方法 ──────────────────────────────────────────

    def parse_only(self, markdown_text: str) -> dict:
        """仅解析 DeveloperSpec——返回 SpecParseResponse 的 dict。

        Args:
            markdown_text: DeveloperSpec Markdown 全文

        Returns:
            符合 SpecParseResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        request_id = self._gen_request_id(spec)
        # 存储完整产物供后续步骤使用
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

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        manifest = _build_manifest(spec)

        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        validator = SqlBuildPlanValidator()
        passed, val_questions = validator.validate(plan, manifest)

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "table_mapping": table_mapping or {},
        }

        all_questions = list(plan_questions) + list(val_questions)
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

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（传给 Compiler）
            table_paths: 物理表名 → CSV 文件路径（传给 Executor）

        Returns:
            符合 ExecuteResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        manifest = _build_manifest(spec)

        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        # Validator 验证（非阻断——记录问题供排查）
        validator = SqlBuildPlanValidator()
        _passed, val_questions = validator.validate(plan, manifest)
        all_questions = list(plan_questions) + list(val_questions)

        compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
        compiled = compiler.compile(plan)

        executor = DuckDBExecutor(table_paths=table_paths or {})
        trace, summary = executor.execute(compiled)

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

    def run_all(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """全流程 + ReviewPackage 打包——返回 RunAllResponse 的 dict。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 RunAllResponse 结构的 dict
        """
        # 1. 解析
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)

        # 2. 构建 SourceManifest
        manifest = _build_manifest(spec)

        # 3. 构建 SqlBuildPlan
        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        # 4. Validator 验证
        validator = SqlBuildPlanValidator()
        passed, val_questions = validator.validate(plan, manifest)

        # 5. 编译
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
        artifact = compiler.compile_to_artifact(plan, spec.spec_hash)

        # 6. 执行（dry_run）
        executor = DuckDBExecutor(table_paths=table_paths or {})
        trace, summary = executor.execute(artifact.compiled_sql)

        # 7. 构建 SqlProgram + 条件选择 Contract 版本
        #    多语句 SqlProgram → extract_v1()（含 step_dag / temp_tables 等 v1 字段）
        #    单语句 SqlProgram → extract()（lite，Phase 2 兼容）
        sql_program = self._build_sql_program(plan, spec.spec_hash)
        contract_extractor = DataTransformContractExtractor()
        if len(sql_program.statements) > 1:
            contract = contract_extractor.extract_v1(sql_program)
        else:
            contract = contract_extractor.extract(plan)

        # 8. 打包 ReviewPackage
        request_id = self._gen_request_id(spec)
        packager = ReviewPackageBuilder(self._base_output_dir)
        package_inputs = PackageInputs(
            request_id=request_id,
            original_spec_md=markdown_text,
            parsed_spec=spec.model_dump(),
            source_manifest=manifest.model_dump(),
            sql_build_plan=plan.model_dump(),
            sql_artifact=artifact.model_dump(),
            execution_trace=trace.model_dump(),
            result_summary=summary.model_dump(),
            data_transform_contract=contract.model_dump(),
            open_questions=[q.model_dump() for q in spec.open_questions + plan_questions],
            validation_questions=[q.model_dump() for q in val_questions],
            perf_results=[],
            retry_count=0,
        )
        package_manifest = packager.build(package_inputs)

        # 存储
        self._results[request_id] = {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": artifact.compiled_sql,
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
            cols = [a.alias for a in step.aliases[:5]]
            if len(step.aliases) > 5:
                cols.append(f"+{len(step.aliases) - 5}")
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

        Args:
            markdown_text: DeveloperSpec Markdown 全文

        Returns:
            符合 SpecRichResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
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
                "columns": spec.output_spec.columns,
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

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanRichResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        manifest = _build_manifest(spec)

        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        validator = SqlBuildPlanValidator()
        passed, val_questions = validator.validate(plan, manifest)

        request_id = self._gen_request_id(spec)
        self._results[request_id] = {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "table_mapping": table_mapping or {},
        }

        all_questions = list(plan_questions) + list(val_questions)

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

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 ExecuteRichResponse 结构的 dict
        """
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        manifest = _build_manifest(spec)

        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        validator = SqlBuildPlanValidator()
        _passed, val_questions = validator.validate(plan, manifest)
        all_questions = list(plan_questions) + list(val_questions)

        compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
        compiled = compiler.compile(plan)

        executor = DuckDBExecutor(table_paths=table_paths or {})
        trace, summary = executor.execute(compiled)

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
