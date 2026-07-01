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
        """空 markdown_text → 200 + pipeline_error（Pipeline 内部捕获）。"""
        resp = client.post("/api/spec/parse", json={"markdown_text": ""})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"

    def test_parse_missing_field(self, client):
        """缺少 markdown_text 字段 → 422（Pydantic 请求校验层）。"""
        resp = client.post("/api/spec/parse", json={})
        assert resp.status_code == 422

    def test_parse_invalid_yaml(self, client):
        """YAML 格式错误的文本 → 200 + pipeline_error（Pipeline 内部捕获）。"""
        text = "```markdown\n---\ninvalid: [yaml\n---\n```"
        resp = client.post("/api/spec/parse", json={"markdown_text": text})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"

    def test_parse_success_no_pipeline_error(self, client, golden_spec):
        """成功解析 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/spec/parse", json={"markdown_text": golden_spec})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "pipeline_stages" not in data
        assert data["spec_id"].startswith("spec_")

    def test_parse_rich_failure(self, client):
        """parse_rich 空输入 → 200 + pipeline_error。"""
        resp = client.post("/api/spec/parse-rich", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        assert len(data["pipeline_stages"]) == 5

    def test_parse_rich_success_no_pipeline_error(self, client, golden_spec):
        """parse_rich 成功 → 不含 pipeline_error。"""
        resp = client.post("/api/spec/parse-rich", json={"markdown_text": golden_spec})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "title" in data
