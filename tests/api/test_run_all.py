"""tests/api/test_run_all.py——POST /api/run-all 测试。"""

import os

import pytest

_CSV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)


class TestRunAll:
    """POST /api/run-all——全流程+打包 → RunAllResponse 摘要。"""

    def test_run_all_success(self, client, golden_spec_passing):
        """全流程成功——需要 DuckDB 和 CSV fixture。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["package_id"].startswith("pkg_")
        assert data["artifact_count"] > 0
        assert "execution_trace" in data
        assert "result_summary" in data
        # 统一的链路状态字段——调用方单点判断
        assert "validation_passed" in data
        assert "open_questions" in data

    def test_run_all_invalid_spec(self, client):
        """无效输入 → 200 + pipeline_error（Pipeline 内部捕获，8 阶段）。"""
        resp = client.post("/api/run-all", json={"markdown_text": ""})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        # run_all 使用 8 阶段（含 validate）
        assert len(data["pipeline_stages"]) == 8
        # 验证 contract/package 在 7 阶段中
        stage_names = [s["stage"] for s in data["pipeline_stages"]]
        assert "contract" in stage_names
        assert "package" in stage_names

    def test_run_all_success_no_pipeline_error(self, client, golden_spec_passing):
        """成功全流程 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "pipeline_stages" not in data
        assert data["package_id"].startswith("pkg_")
        # 成功路径应包含链路状态字段
        assert "validation_passed" in data
        assert "open_questions" in data

    def test_run_all_build_failure(self, pipeline, golden_spec_passing):
        """run_all build 阶段失败 → 保留 spec + manifest。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        original_builder = pipeline_mod.SqlBuildPlanBuilder

        class FailingBuilder:
            def build(self, spec, hypothesis=None):
                raise ValueError("模拟 run_all build 失败")

        pipeline_mod.SqlBuildPlanBuilder = FailingBuilder
        try:
            result = pipeline.run_all(golden_spec_passing)
        finally:
            pipeline_mod.SqlBuildPlanBuilder = original_builder

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "build"
        # 8 阶段（含 validate）
        assert len(result["pipeline_stages"]) == 8
        # 产物已保存
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved

    def test_run_all_execute_failure_blocks_package(self, pipeline, golden_spec_passing):
        """run_all() Executor 返回 RUNTIME_FAIL → 阻断，不含 package_id。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        from tianshu_datadev.sql.models import ExecutionStatus, ExecutionTrace, ResultSummary

        class FailingExecutor:
            """模拟执行失败——RUNTIME_FAIL，不抛异常。"""
            def execute(self, compiled):
                trace = ExecutionTrace(
                    trace_id="trace_fail_r",
                    plan_id=compiled.input_plan_hash,
                    engine="duckdb", generated_sql=compiled.sql,
                    status=ExecutionStatus.RUNTIME_FAIL,
                    row_count=0, execution_time_ms=5.0,
                    error_message="模拟 run_all 执行失败",
                )
                summary = ResultSummary(
                    summary_id="summary_fail_r",
                    trace_id="trace_fail_r",
                    engine="duckdb", columns=[], column_types=[],
                    row_count=0, null_counts={}, numeric_sums={}, sample_rows=[],
                )
                return trace, summary

        original = pipeline_mod.DuckDBExecutor
        pipeline_mod.DuckDBExecutor = lambda **kw: FailingExecutor()
        try:
            result = pipeline.run_all(golden_spec_passing)
        finally:
            pipeline_mod.DuckDBExecutor = original

        # 阻断：返回 pipeline_error
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "execute"
        # 不应有 package_id
        assert "package_id" not in result or result.get("package_id") == ""
        # 8 阶段，execute=failed, contract=skipped, package=skipped
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert len(result["pipeline_stages"]) == 8
        assert stages["execute"] == "failed"
        assert stages["contract"] == "skipped"
        assert stages["package"] == "skipped"
