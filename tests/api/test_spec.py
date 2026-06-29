"""tests/api/test_spec.py——POST /api/spec/parse 测试。"""


class TestSpecParse:
    """POST /api/spec/parse——解析 DeveloperSpec → 结构化摘要。"""

    def test_parse_success(self, client, golden_spec):
        """正常解析 golden fixture → 200 + 结构化摘要。"""
        resp = client.post("/api/spec/parse", json={"markdown_text": golden_spec})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["spec_id"].startswith("spec_")
        assert data["table_count"] >= 1
        assert data["metric_count"] >= 1
        assert data["dimension_count"] >= 1
        # 此 fixture 无 time_range → warning_count > 0
        assert data["warning_count"] > 0

    def test_parse_empty_text(self, client):
        """空 markdown_text → 422 + 结构化错误。"""
        resp = client.post("/api/spec/parse", json={"markdown_text": ""})
        assert resp.status_code == 422, f"期望 422，实际 {resp.status_code}"
        data = resp.json()
        assert data["error_code"] is not None
        assert data["message"] is not None

    def test_parse_missing_field(self, client):
        """缺少 markdown_text 字段 → 422。"""
        resp = client.post("/api/spec/parse", json={})
        assert resp.status_code == 422

    def test_parse_invalid_yaml(self, client):
        """YAML 格式错误的文本 → 422 + ParseError 结构。"""
        text = "```markdown\n---\ninvalid: [yaml\n---\n```"
        resp = client.post("/api/spec/parse", json={"markdown_text": text})
        assert resp.status_code == 422, f"期望 422，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["error_code"] is not None
