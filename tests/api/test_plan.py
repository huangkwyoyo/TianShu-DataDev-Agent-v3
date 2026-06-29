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
        """无效输入 → 422 + 结构化错误。"""
        resp = client.post("/api/plan", json={"markdown_text": "无 fenced block 的内容"})
        assert resp.status_code == 422, f"期望 422，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["error_code"] is not None

    def test_plan_empty_text(self, client):
        """空输入 → 422。"""
        resp = client.post("/api/plan", json={"markdown_text": ""})
        assert resp.status_code == 422
