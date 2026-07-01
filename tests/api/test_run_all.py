"""tests/api/test_run_all.py——POST /api/run-all 测试。"""

import os

import pytest

_CSV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)


class TestRunAll:
    """POST /api/run-all——全流程+打包 → RunAllResponse 摘要。"""

    def test_run_all_success(self, client, golden_spec):
        """全流程成功——需要 DuckDB 和 CSV fixture。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec,
            "table_paths": {"test_fact": _CSV_PATH},
        })
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["package_id"].startswith("pkg_")
        assert data["artifact_count"] > 0
        assert "execution_trace" in data
        assert "result_summary" in data

    def test_run_all_invalid_spec(self, client):
        """无效输入 → 200 + pipeline_error（Pipeline 内部捕获，7 阶段）。"""
        resp = client.post("/api/run-all", json={"markdown_text": ""})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        # run_all 使用 7 阶段
        assert len(data["pipeline_stages"]) == 7
        # 验证 contract/package 在 7 阶段中
        stage_names = [s["stage"] for s in data["pipeline_stages"]]
        assert "contract" in stage_names
        assert "package" in stage_names

    def test_run_all_success_no_pipeline_error(self, client, golden_spec):
        """成功全流程 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec,
            "table_paths": {"test_fact": _CSV_PATH},
        })
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "pipeline_stages" not in data
        assert data["package_id"].startswith("pkg_")

    def test_run_all_build_failure(self, pipeline, golden_spec):
        """run_all build 阶段失败 → 保留 spec + manifest。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        original_builder = pipeline_mod.SqlBuildPlanBuilder

        class FailingBuilder:
            def build(self, spec, hypothesis=None):
                raise ValueError("模拟 run_all build 失败")

        pipeline_mod.SqlBuildPlanBuilder = FailingBuilder
        try:
            result = pipeline.run_all(golden_spec)
        finally:
            pipeline_mod.SqlBuildPlanBuilder = original_builder

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "build"
        # 7 阶段
        assert len(result["pipeline_stages"]) == 7
        # 产物已保存
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved
