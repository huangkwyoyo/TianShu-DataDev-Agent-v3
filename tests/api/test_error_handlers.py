"""tests/api/test_error_handlers.py——结构化错误格式测试。"""


class TestErrorHandlers:
    """验证所有错误路径返回统一 ErrorDetail 格式。"""

    def test_parse_error_structure(self, client):
        """ParseError → 200 + pipeline_error（Pipeline 内部捕获）。"""
        text = "```markdown\n---\ninvalid: [yaml\n---\n```"
        resp = client.post("/api/spec/parse", json={"markdown_text": text})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        assert "error_type" in data["pipeline_error"]
        assert "error_message" in data["pipeline_error"]

    def test_validation_error_structure(self, client):
        """空输入 → 200 + pipeline_error（Pipeline 内部捕获 ParseError）。"""
        resp = client.post("/api/spec/parse", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"

    def test_extra_field_rejected(self, client):
        """请求体含未声明字段 → 422（StrictModel extra="forbid" 行为）。"""
        resp = client.post("/api/spec/parse", json={
            "markdown_text": "test",
            "unknown_field": "should be rejected",
        })
        # Pydantic extra="forbid" 在 FastAPI 请求校验层就拒绝
        assert resp.status_code == 422

    def test_not_found_structure(self, client):
        """404 响应也使用统一 ErrorDetail 格式。"""
        resp = client.get("/api/package/nonexistent_id_xyz")
        assert resp.status_code == 404
        data = resp.json()
        assert data["error_code"] == "NOT_FOUND"
        assert "message" in data
        assert data["field_ref"] == "request_id"
