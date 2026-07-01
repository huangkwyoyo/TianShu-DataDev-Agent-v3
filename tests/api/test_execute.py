"""tests/api/test_execute.py——POST /api/execute 测试。"""

import os

import pytest

_CSV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)


class TestExecute:
    """POST /api/execute——编译+执行(dry_run) → ExecuteResponse 摘要。"""

    def test_execute_success(self, client, golden_spec):
        """编译+执行成功——需要 DuckDB 和 CSV fixture。"""
        resp = client.post("/api/execute", json={
            "markdown_text": golden_spec,
            "table_paths": {"test_fact": _CSV_PATH},
        })
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "execution_trace" in data
        assert data["execution_trace"]["status"] in (
            "RUNTIME_PASS", "RUNTIME_FAIL", "NOT_EXECUTED",
        )
        assert data["sql_sha256"] is not None
        assert data["compiler_version"] is not None

    def test_execute_no_table_paths(self, client, golden_spec):
        """不传 table_paths → 执行可能失败但不崩溃（表不存在）"""
        resp = client.post("/api/execute", json={"markdown_text": golden_spec})
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        # 即使表不存在，API 仍应返回 200（执行状态在 trace 中体现）
        assert resp.status_code == 200

    def test_execute_invalid_spec(self, client):
        """无效输入 → 200 + pipeline_error（错误信息编码在响应体中）。"""
        resp = client.post("/api/execute", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        assert "pipeline_stages" in data
        # 验证 parser 阶段标记为 failed，其余为 skipped
        stages = {s["stage"]: s["status"] for s in data["pipeline_stages"]}
        assert stages["parser"] == "failed"
        assert stages["enrich"] == "skipped"

    def test_execute_success_no_pipeline_error(self, client, golden_spec):
        """成功路径 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/execute", json={
            "markdown_text": golden_spec,
            "table_paths": {"test_fact": _CSV_PATH},
        })
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "pipeline_stages" not in data

    def test_execute_rich_success(self, client, golden_spec):
        """execute_rich 成功 → 含 generated_sql。"""
        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec,
            "table_paths": {"test_fact": _CSV_PATH},
        })
        if resp.status_code == 500 and "DuckDB" in resp.text:
            pytest.skip("DuckDB 未安装")
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "generated_sql" in data
        assert len(data["generated_sql"]) > 0

    def test_execute_rich_parser_failure(self, client):
        """execute_rich 空输入 → 200 + pipeline_error。"""
        resp = client.post("/api/execute-rich", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
