"""tests/api/test_pipeline_error.py——Pipeline 逐层容错测试。

验证 execute() 在各阶段失败时：
  1. 返回 200 + pipeline_error（不崩溃）
  2. 已完成产物保留在 self._results 中
  3. pipeline_stages 正确标记各阶段状态
  4. 成功路径不含 pipeline_error 字段
"""

import pytest

from tianshu_datadev.api.pipeline import Pipeline


# ── 阶段中文名映射 ──

STAGE_CN = {
    "parser": "解析",
    "enrich": "增强",
    "build": "构建",
    "compile": "编译",
    "execute": "执行",
}


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

    def test_success_path_has_no_pipeline_error(self, pipeline, golden_spec):
        """成功路径 → 不含 pipeline_error 字段——依赖 DuckDB。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        result = pipeline.execute(golden_spec)
        assert "pipeline_error" not in result
        assert "request_id" in result
        assert "execution_trace" in result
        # 成功路径不返回 pipeline_stages
        assert "pipeline_stages" not in result

    def test_pipeline_stages_all_ok_on_success(self, pipeline, golden_spec):
        """成功路径验证——产物包含所有阶段数据。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        result = pipeline.execute(golden_spec)
        # 成功路径不含 pipeline_error / pipeline_stages
        assert "pipeline_error" not in result
        assert "pipeline_stages" not in result
        # 结果完整性
        assert result["sql_sha256"]
        assert result["compiler_version"]
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
        # pipeline_stages 有 5 个阶段
        assert len(result["pipeline_stages"]) == 5
        for s in result["pipeline_stages"]:
            assert "stage" in s
            assert "status" in s
            assert s["status"] in ("ok", "failed", "skipped")
        # 失败阶段应包含错误详情
        failed_stage = next(s for s in result["pipeline_stages"] if s["status"] == "failed")
        assert "error_type" in failed_stage
        assert "error_message" in failed_stage

    def test_partial_sql_sha256_on_compile_failure(self, pipeline, golden_spec):
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
            result = pipeline.execute(golden_spec)
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
            "compile": "skipped",
            "execute": "skipped",
        }

    def test_build_pipeline_stages_compile_failed(self, pipeline):
        """compile 失败——parser+enrich+build 为 ok，compile failed，execute skipped。"""
        error_info = pipeline._capture_error("compile", RuntimeError("err"))
        stages = pipeline._build_pipeline_stages("compile", error_info)
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses == {
            "parser": "ok",
            "enrich": "ok",
            "build": "ok",
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

    def test_build_pipeline_stages_7_stages(self, pipeline):
        """run_all 的 7 阶段列表——contract 失败时前 5 阶段为 ok。"""
        run_all_stages = ["parser", "enrich", "build", "compile", "execute", "contract", "package"]
        error_info = pipeline._capture_error("contract", RuntimeError("打包前契约抽取失败"))
        stages = pipeline._build_pipeline_stages("contract", error_info, run_all_stages)
        assert len(stages) == 7
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses == {
            "parser": "ok",
            "enrich": "ok",
            "build": "ok",
            "compile": "ok",
            "execute": "ok",
            "contract": "failed",
            "package": "skipped",
        }

    def test_build_pipeline_stages_package_failed(self, pipeline):
        """package 失败——前 6 阶段为 ok。"""
        run_all_stages = ["parser", "enrich", "build", "compile", "execute", "contract", "package"]
        error_info = pipeline._capture_error("package", RuntimeError("打包失败"))
        stages = pipeline._build_pipeline_stages("package", error_info, run_all_stages)
        statuses = {s["stage"]: s["status"] for s in stages}
        assert statuses["contract"] == "ok"
        assert statuses["package"] == "failed"
