"""tests/api/test_pipeline_error.py——Pipeline 逐层容错测试。

验证 execute() 在各阶段失败时：
  1. 返回 200 + pipeline_error（不崩溃）
  2. 已完成产物保留在 self._results 中
  3. pipeline_stages 正确标记各阶段状态
  4. 成功路径不含 pipeline_error 字段
  5. RUNTIME_FAIL 执行阻断——不返回 execution_trace，不含 package_id
"""

import pytest

from tianshu_datadev.sql.models import ExecutionStatus, ExecutionTrace, ResultSummary


class TestPipelineErrorHandling:
    """Pipeline.execute() 各阶段失败时的容错行为。"""

    def test_parser_failure_returns_pipeline_error(self, pipeline, golden_spec):
        """空 markdown → parser 失败 → 200 + pipeline_error。"""
        result = pipeline.execute("")
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "parser"
        assert result["pipeline_error"]["error_type"] == "ParseError"
        # pipeline_stages 结构正确
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["parser"] == "failed"
        assert stages["enrich"] == "skipped"
        assert stages["build"] == "skipped"
        assert stages["compile"] == "skipped"
        assert stages["execute"] == "skipped"

    def test_enrich_failure_preserves_parse_result(self, pipeline, golden_spec):
        """enrich 阶段失败 → 保留解析结果 + 返回错误。"""
        original = pipeline._enrich_and_plan

        def _failing(*args, **kwargs):
            raise RuntimeError("模拟 enrich 阶段失败")

        pipeline._enrich_and_plan = _failing
        try:
            result = pipeline.execute(golden_spec)
        finally:
            pipeline._enrich_and_plan = original

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "enrich"
        assert result["pipeline_error"]["error_type"] == "RuntimeError"
        assert "模拟 enrich 阶段失败" in result["pipeline_error"]["error_message"]
        # pipeline_stages 结构
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["parser"] == "ok"
        assert stages["enrich"] == "failed"
        assert stages["build"] == "skipped"
        # 已完成产物已保存
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved

    def test_build_failure_preserves_enrich_result(self, pipeline, golden_spec):
        """build 阶段失败 → 保留 spec + manifest + 返回 plan_id 为空。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        original_builder = pipeline_mod.SqlBuildPlanBuilder

        class FailingBuilder:
            def build(self, spec, hypothesis=None):
                raise ValueError("模拟 build 阶段失败")

        pipeline_mod.SqlBuildPlanBuilder = FailingBuilder
        try:
            result = pipeline.execute(golden_spec)
        finally:
            pipeline_mod.SqlBuildPlanBuilder = original_builder

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "build"
        assert result["plan_id"] == ""  # plan 未构建
        # 已完成产物已保存
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved

    def test_execute_success_path_complete(self, pipeline, golden_spec_passing):
        """execute() 成功路径——不含 pipeline_error/stages，产物完整。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.execute(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        # 成功路径不含错误标记
        assert "pipeline_error" not in result
        assert "pipeline_stages" not in result
        # 成功路径包含执行结果
        assert "request_id" in result
        assert "execution_trace" in result
        # 编译产物完整性
        assert result["sql_sha256"]
        assert result["compiler_version"]
        # 中间产物保留到 _results
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "plan" in saved
        assert "compiled" in saved

    def test_parser_error_structure(self, pipeline):
        """parser 失败时返回结构的完整性验证。"""
        result = pipeline.execute("")
        # 必要字段存在
        assert "request_id" in result
        assert "pipeline_error" in result
        assert "pipeline_stages" in result
        # pipeline_error 字段完整
        pe = result["pipeline_error"]
        assert "stage" in pe
        assert "error_type" in pe
        assert "error_message" in pe
        assert pe["stage"] == "parser"
        # pipeline_stages 有 6 个阶段（含 validate）
        assert len(result["pipeline_stages"]) == 6
        for s in result["pipeline_stages"]:
            assert "stage" in s
            assert "status" in s
            assert s["status"] in ("ok", "failed", "skipped")
        # 失败阶段应包含错误详情
        failed_stage = next(s for s in result["pipeline_stages"] if s["status"] == "failed")
        assert "error_type" in failed_stage
        assert "error_message" in failed_stage

    def test_partial_sql_sha256_on_compile_failure(self, pipeline, golden_spec_passing):
        """compile 失败时 sql_sha256 应为空字符串。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        original_compiler = pipeline_mod.DuckDbSqlCompiler

        class FailingCompiler:
            def __init__(self, table_mapping=None):
                pass

            def compile(self, plan):
                raise RuntimeError("模拟编译失败")

        pipeline_mod.DuckDbSqlCompiler = FailingCompiler
        try:
            result = pipeline.execute(golden_spec_passing)
        finally:
            pipeline_mod.DuckDbSqlCompiler = original_compiler

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "compile"
        # 编译失败 → sql_sha256 为空（未产出编译结果）
        assert result["sql_sha256"] == ""
        assert result["compiler_version"] == ""
        # plan_id 已生成
        assert result["plan_id"] != ""
        # 保留 plan
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "plan" in saved

    def test_request_id_preserved_across_stages(self, pipeline, golden_spec):
        """request_id 在各失败路径中保持一致——确定性生成。"""
        # parser 失败
        result1 = pipeline.execute("")
        # enrich 失败
        original = pipeline._enrich_and_plan

        def _failing(*args, **kwargs):
            raise RuntimeError("x")

        pipeline._enrich_and_plan = _failing
        try:
            result2 = pipeline.execute(golden_spec)
        finally:
            pipeline._enrich_and_plan = original

        # parser 失败时 request_id 为空
        assert result1["request_id"] == ""
        # enrich 失败时 request_id 非空
        assert result2["request_id"] != ""
        assert result2["request_id"].startswith("req_")

    # ── Phase 4.7: Execute 阶段 RUNTIME_FAIL 阻断 ──

    def test_execute_runtime_fail_blocks_and_preserves(self, pipeline, golden_spec_passing):
        """execute() RUNTIME_FAIL → 返回 pipeline_error + 中间产物保留到 _results。"""
        import tianshu_datadev.api.pipeline as pipeline_mod

        class FailingExecutor:
            """模拟执行失败——返回 RUNTIME_FAIL 而非抛异常。"""
            def execute(self, compiled):
                trace = ExecutionTrace(
                    trace_id="trace_fail_e",
                    plan_id=compiled.input_plan_hash,
                    engine="duckdb", generated_sql=compiled.sql,
                    status=ExecutionStatus.RUNTIME_FAIL,
                    row_count=0, execution_time_ms=10.0,
                    error_message="模拟执行失败",
                )
                summary = ResultSummary(
                    summary_id="summary_fail_e",
                    trace_id="trace_fail_e",
                    engine="duckdb", columns=[], column_types=[],
                    row_count=0, null_counts={}, numeric_sums={}, sample_rows=[],
                )
                return trace, summary

        original = pipeline_mod.DuckDBExecutor
        pipeline_mod.DuckDBExecutor = lambda **kw: FailingExecutor()
        try:
            result = pipeline.execute(golden_spec_passing)
        finally:
            pipeline_mod.DuckDBExecutor = original

        # ── 阻断：返回 pipeline_error ──
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "execute"
        assert result["pipeline_error"]["error_type"] == "ExecutionFailed"
        # 编译已成功→sql_sha256 非空（compile 阶段已完成）
        assert result["sql_sha256"] != ""
        # 不应有 execution_trace 和 result_summary（阻断不返回）
        assert result["execution_trace"] is None
        assert result["result_summary"] is None
        # pipeline_stages 标记 execute=failed
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["execute"] == "failed"

        # ── 中间产物保留到 _results ──
        request_id = result["request_id"]
        assert request_id in pipeline._results
        saved = pipeline._results[request_id]
        assert "parsed_spec" in saved
        assert "plan" in saved
        assert "trace" in saved
        assert saved["trace"].error_message == "模拟执行失败"

    # ── Phase 4.7b: execute_rich RUNTIME_FAIL 阻断 ──

    def test_execute_rich_runtime_fail_blocks_and_preserves(self, pipeline, golden_spec_passing):
        """execute_rich() RUNTIME_FAIL → 返回 pipeline_error + 中间产物保留到 _results。"""
        import tianshu_datadev.api.pipeline as pipeline_mod

        class FailingExecutor:
            """模拟执行失败——返回 RUNTIME_FAIL 而非抛异常。"""
            def execute(self, compiled):
                trace = ExecutionTrace(
                    trace_id="trace_rich_fail",
                    plan_id=compiled.input_plan_hash,
                    engine="duckdb", generated_sql=compiled.sql,
                    status=ExecutionStatus.RUNTIME_FAIL,
                    row_count=0, execution_time_ms=10.0,
                    error_message="execute_rich 模拟执行失败",
                )
                summary = ResultSummary(
                    summary_id="summary_rich_fail",
                    trace_id="trace_rich_fail",
                    engine="duckdb", columns=[], column_types=[],
                    row_count=0, null_counts={}, numeric_sums={}, sample_rows=[],
                )
                return trace, summary

        original = pipeline_mod.DuckDBExecutor
        pipeline_mod.DuckDBExecutor = lambda **kw: FailingExecutor()
        try:
            result = pipeline.execute_rich(golden_spec_passing)
        finally:
            pipeline_mod.DuckDBExecutor = original

        # ── 阻断：返回 pipeline_error ──
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "execute"
        assert result["pipeline_error"]["error_type"] == "ExecutionFailed"
        assert "execute_rich 模拟执行失败" in result["pipeline_error"]["error_message"]
        # 编译已成功→sql_sha256 和 generated_sql 非空
        assert result["sql_sha256"] != ""
        assert result["generated_sql"] != ""
        assert result["compiler_version"] != ""
        # 不应有 execution_trace 和 result_summary（阻断不返回）
        assert result["execution_trace"] is None
        assert result["result_summary"] is None
        # 链路状态字段——验证已通过，执行阶段失败
        assert result["validation_passed"] is True
        # pipeline_stages 标记 execute=failed
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["execute"] == "failed"
        assert stages["compile"] == "ok"

        # ── 中间产物保留到 _results ──
        request_id = result["request_id"]
        assert request_id in pipeline._results
        saved = pipeline._results[request_id]
        assert "parsed_spec" in saved
        assert "manifest" in saved
        assert "plan" in saved
        assert "compiled" in saved
        assert "trace" in saved
        assert "summary" in saved
        assert saved["trace"].error_message == "execute_rich 模拟执行失败"


class TestPipelineStagesHelper:
    """_build_pipeline_stages 和 _capture_error 辅助方法单元测试。"""

    def test_build_pipeline_stages_parser_failed(self, pipeline):
        """parser 失败——其余全部 skipped。"""
        error_info = pipeline._capture_error("parser", ValueError("test"))
        stages = pipeline._build_pipeline_stages("parser", error_info)
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses == {
            "parser": "failed",
            "enrich": "skipped",
            "build": "skipped",
            "validate": "skipped",
            "compile": "skipped",
            "execute": "skipped",
        }

    def test_build_pipeline_stages_compile_failed(self, pipeline):
        """compile 失败——parser+enrich+build+validate 为 ok，compile failed，execute skipped。"""
        error_info = pipeline._capture_error("compile", RuntimeError("err"))
        stages = pipeline._build_pipeline_stages("compile", error_info)
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses == {
            "parser": "ok",
            "enrich": "ok",
            "build": "ok",
            "validate": "ok",
            "compile": "failed",
            "execute": "skipped",
        }

    def test_capture_error_structure(self, pipeline):
        """_capture_error 返回完整的三字段结构。"""
        info = pipeline._capture_error("build", ValueError("测试错误"))
        assert info["stage"] == "build"
        assert info["error_type"] == "ValueError"
        assert info["error_message"] == "测试错误"

    def test_stage_name_cn_mapping(self, pipeline):
        """_stage_name_cn 返回正确中文映射。"""
        assert pipeline._stage_name_cn("parser") == "解析"
        assert pipeline._stage_name_cn("enrich") == "增强"
        assert pipeline._stage_name_cn("build") == "构建"
        assert pipeline._stage_name_cn("compile") == "编译"
        assert pipeline._stage_name_cn("execute") == "执行"
        assert pipeline._stage_name_cn("contract") == "契约"
        assert pipeline._stage_name_cn("package") == "打包"
        assert pipeline._stage_name_cn("unknown") == "unknown"

    def test_build_pipeline_stages_8_stages(self, pipeline):
        """run_all 的 8 阶段列表——contract 失败时前 6 阶段为 ok。"""
        run_all_stages = [
            "parser", "enrich", "build", "validate",
            "compile", "execute", "contract", "package",
        ]
        error_info = pipeline._capture_error("contract", RuntimeError("打包前契约抽取失败"))
        stages = pipeline._build_pipeline_stages("contract", error_info, run_all_stages)
        assert len(stages) == 8
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses == {
            "parser": "ok",
            "enrich": "ok",
            "build": "ok",
            "validate": "ok",
            "compile": "ok",
            "execute": "ok",
            "contract": "failed",
            "package": "skipped",
        }

    def test_build_pipeline_stages_package_failed(self, pipeline):
        """package 失败——前 7 阶段为 ok。"""
        run_all_stages = [
            "parser", "enrich", "build", "validate",
            "compile", "execute", "contract", "package",
        ]
        error_info = pipeline._capture_error("package", RuntimeError("打包失败"))
        stages = pipeline._build_pipeline_stages("package", error_info, run_all_stages)
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses["contract"] == "ok"
        assert statuses["package"] == "failed"


class TestPipelineValidationBlocking:
    """Validator 阻断行为——blocking 问题必须在 compile 前中止流水线。"""

    def test_execute_blocks_on_validation_failure(self, pipeline, golden_spec):
        """execute() 在 Validator 返回 blocking 问题时中止——不执行 SQL。"""
        result = pipeline.execute(golden_spec)
        # 应返回 pipeline_error 而非 execution_trace
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "validate"
        assert result["pipeline_error"]["error_type"] == "ValidationBlocked"
        assert "阻塞问题" in result["pipeline_error"]["error_message"]
        # 不应有编译/执行产物
        assert result["execution_trace"] is None
        assert result["result_summary"] is None
        assert result["sql_sha256"] == ""
        # validation_passed 为 False
        assert result["validation_passed"] is False
        # pipeline_stages 标记 validate 为 failed
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["validate"] == "failed"
        assert stages["compile"] == "skipped"
        assert stages["execute"] == "skipped"
        # 已完成产物已保存供诊断
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved
        assert "plan" in saved

    def test_run_all_blocks_on_validation_failure(self, pipeline, golden_spec):
        """run_all() 在 Validator blocking 时中止——不生成 Package。"""
        result = pipeline.run_all(golden_spec)
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "validate"
        assert result["pipeline_error"]["error_type"] == "ValidationBlocked"
        # 不应有执行和打包产物
        assert result["execution_status"] == "not_executed"
        # pipeline_stages 使用 8 阶段，validate 为 failed
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert len(result["pipeline_stages"]) == 8
        assert stages["validate"] == "failed"
        assert stages["compile"] == "skipped"
        assert stages["contract"] == "skipped"

    def test_open_questions_include_blocking_details(self, pipeline, golden_spec):
        """阻断响应的 open_questions 包含具体的 blocking 问题。"""
        result = pipeline.execute(golden_spec)
        questions = result["open_questions"]
        blocking = [q for q in questions if q["blocking"]]
        assert len(blocking) > 0, "应至少有一个 blocking 问题"
        # 每个 blocking 问题有 question_id + description
        for q in blocking:
            assert "question_id" in q
            assert "description" in q
            assert q["blocking"] is True


# ════════════════════════════════════════════════
# Phase 9A1: Pipeline.export_artifacts() 测试
# ════════════════════════════════════════════════


class TestPipelineExportArtifacts:
    """Pipeline.export_artifacts()——从 _results 缓存导出中间产物。"""

    def test_export_after_run_all_returns_bundle(self, pipeline, golden_spec_passing):
        """run_all 成功后 export_artifacts 返回非空 PipelineArtifactBundle。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]

        bundle = pipeline.export_artifacts(request_id)

        # bundle 不为 None
        assert bundle is not None
        # request_id 一致
        assert bundle.request_id == request_id
        # spec_hash 非空
        assert bundle.spec_hash != ""
        # SqlBuildPlan 已缓存——应非空
        assert bundle.sql_build_plan is not None
        # DataTransformContract 已缓存——应非空（9A2 桥接替换的关键输入）
        assert bundle.data_transform_contract is not None
        assert bundle.data_transform_contract.contract_id != ""
        assert len(bundle.data_transform_contract.input_tables) > 0
        # CompiledSql 已缓存——应非空
        assert bundle.compiled_sql is not None
        # ExecutionTrace 已缓存——应非空
        assert bundle.execution_trace is not None
        # ResultSummary 已缓存——应非空
        assert bundle.result_summary is not None

    def test_export_unknown_request_id_returns_none(self, pipeline):
        """未执行过的 request_id 返回 None。"""
        bundle = pipeline.export_artifacts("req_nonexistent")
        assert bundle is None

    def test_export_after_ttl_expiry_returns_none(self, pipeline, golden_spec_passing):
        """TTL 过期清理后 export_artifacts 返回 None。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]

        # 设置 TTL 为负值——使 _purge_expired 的 now - ts > -1 恒成立，立即清理所有条目
        pipeline._ttl_seconds = -1
        # export_artifacts 内部调用 _purge_expired——应清理并返回 None
        bundle = pipeline.export_artifacts(request_id)
        assert bundle is None

    def test_export_after_execute_returns_bundle(self, pipeline, golden_spec_passing):
        """execute() 成功后 export_artifacts 同样可导出产物。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.execute(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]

        bundle = pipeline.export_artifacts(request_id)

        assert bundle is not None
        assert bundle.sql_build_plan is not None
        assert bundle.compiled_sql is not None
        assert bundle.execution_trace is not None
        assert bundle.result_summary is not None

    def test_export_after_build_plan_has_plan_but_no_execution(self, pipeline, golden_spec_passing):
        """build_plan 只到 plan 阶段——compiled/trace/summary 应为 None。"""
        result = pipeline.build_plan(golden_spec_passing)
        request_id = result["request_id"]

        bundle = pipeline.export_artifacts(request_id)

        assert bundle is not None
        assert bundle.sql_build_plan is not None
        # build_plan 不执行编译和执行——这些字段应为 None
        assert bundle.compiled_sql is None
        assert bundle.execution_trace is None
        assert bundle.result_summary is None

    def test_export_bundle_spec_hash_matches_parsed_spec(self, pipeline, golden_spec_passing):
        """bundle.spec_hash 与 _results 中 ParsedDeveloperSpec.spec_hash 一致。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]

        bundle = pipeline.export_artifacts(request_id)
        saved = pipeline._results[request_id]
        parsed_spec = saved["parsed_spec"]

        assert bundle.spec_hash == parsed_spec.spec_hash

    def test_export_bundle_plan_id_matches(self, pipeline, golden_spec_passing):
        """bundle.sql_build_plan.plan_id 与 run_all 返回的 plan_id 一致。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]

        bundle = pipeline.export_artifacts(request_id)

        assert bundle.sql_build_plan.plan_id == result["plan_id"]
