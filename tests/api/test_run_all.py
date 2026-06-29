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
        """无效输入 → 422。"""
        resp = client.post("/api/run-all", json={"markdown_text": ""})
        assert resp.status_code == 422, f"期望 422，实际 {resp.status_code}: {resp.text}"
