"""tests/api/test_plan.py——POST /api/plan 测试。"""


class TestPlan:
    """POST /api/plan——解析+构建+验证 → Plan 摘要。"""

    def test_plan_success(self, client, golden_spec):
        """正常构建 → 200 + Plan 摘要。"""
        resp = client.post("/api/plan", json={"markdown_text": golden_spec})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["plan_id"].startswith("plan_")
        assert data["step_count"] >= 1
        assert isinstance(data["step_types"], list)
        assert isinstance(data["validation_passed"], bool)

    def test_plan_with_table_mapping(self, client, golden_spec):
        """带 table_mapping 的正常构建。"""
        resp = client.post("/api/plan", json={
            "markdown_text": golden_spec,
            "table_mapping": {"tf": "test_fact"},
        })
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"

    def test_plan_invalid_spec(self, client):
        """无效输入 → 200 + pipeline_error（Pipeline 内部捕获）。"""
        resp = client.post("/api/plan", json={"markdown_text": "无 fenced block 的内容"})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"

    def test_plan_empty_text(self, client):
        """空输入 → 200 + pipeline_error。"""
        resp = client.post("/api/plan", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"

    def test_plan_success_no_pipeline_error(self, client, golden_spec):
        """成功构建 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/plan", json={"markdown_text": golden_spec})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "pipeline_stages" not in data
        assert data["plan_id"].startswith("plan_")

    def test_plan_rich_success(self, client, golden_spec):
        """build_plan_rich 成功 → 含 join_evidence。"""
        resp = client.post("/api/plan-rich", json={"markdown_text": golden_spec})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        assert "join_evidence" in data
        assert data["plan_id"].startswith("plan_")

    def test_plan_rich_parser_failure(self, client):
        """build_plan_rich 空输入 → 200 + pipeline_error。"""
        resp = client.post("/api/plan-rich", json={"markdown_text": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
