"""tests/api/test_package.py——GET /api/package/{request_id} 测试。"""


class TestPackage:
    """GET /api/package/{request_id}——获取 ReviewPackage manifest。"""

    def test_get_package_success(self, client, pipeline, golden_spec_passing):
        """先 run-all 再获取 package → 200。"""
        import os
        csv_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
        )
        # 通过 pipeline 直接执行 run_all（不经过 HTTP 层）
        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        request_id = result["request_id"]
        # 通过 HTTP 层获取 package
        resp = client.get(f"/api/package/{request_id}")
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["package_id"].startswith("pkg_")
        assert data["artifact_count"] > 0
        assert isinstance(data["artifacts"], list)

    def test_get_package_not_found(self, client):
        """不存在的 request_id → 404。"""
        resp = client.get("/api/package/nonexistent_req_id")
        assert resp.status_code == 404
        data = resp.json()
        assert data["error_code"] == "NOT_FOUND"
