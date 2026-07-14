"""tests/api/test_spark_verify.py——POST /api/spark/verify 端点测试。

覆盖：
1. 正常流程——run_all → spark/verify → 200 + 6 阶段 + review_ready=True
2. 无效 request_id → 404 SPARK_ARTIFACTS_NOT_FOUND
3. artifacts 不完整（仅 build_plan 路径）→ 422 SPARK_ARTIFACTS_INCOMPLETE
4. 阶段失败——contract 为 None → 422
"""

from __future__ import annotations

import pytest

# 用于 run_all DuckDB 执行的 CSV fixture 路径

class TestSparkVerifySuccess:
    """正常流程——run_all 产出 artifacts 后 spark/verify 返回完整结果。"""

    def test_spark_verify_full_chain_returns_200(self, client, golden_spec_passing, csv_path):
        """run_all → spark/verify → 200 + 6 阶段 + review_ready=True。"""

        # ── Step 1: 先执行全流程 Run-All ──
        resp_run = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp_run.status_code == 200, (
            f"run-all 应返回 200，实际 {resp_run.status_code}: {resp_run.text}"
        )
        run_result = resp_run.json()
        request_id = run_result["request_id"]
        assert request_id, "run-all 应返回非空 request_id"

        # ── Step 2: 触发 Spark 验证 ──
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 200, (
            f"spark/verify 应返回 200，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()

        # ── 验证响应结构 ──
        assert data["request_id"] == request_id
        assert len(data["spark_stages"]) == 6, (
            f"spark_stages 应有 6 个阶段，实际 {len(data['spark_stages'])}"
        )
        # 验证阶段名完整
        stage_names = {s["stage"] for s in data["spark_stages"]}
        expected_stages = {
            "MAPPER", "DEVELOPER", "COMPILER", "VALIDATOR",
            "COMPARATOR", "PHYSICAL_VERIFIER",
        }
        assert stage_names == expected_stages, (
            f"spark_stages 阶段名应为 {expected_stages}，实际 {stage_names}"
        )
        # 验证 status 值合法
        for s in data["spark_stages"]:
            assert s["status"] in ("ok", "failed", "skipped"), (
                f"阶段 {s['stage']} status 应为 ok/failed/skipped，实际 {s['status']}"
            )
        # 验证关键字段存在
        assert "overall_status" in data
        assert "comparator_status" in data
        assert "review_ready" in data
        # 单表路径 MAPPER/COMPILER/VALIDATOR 应为 SUCCESS → review_ready=True
        assert data["review_ready"] is True, (
            f"review_ready 应为 True，实际 {data['review_ready']}。"
            f"overall_status={data.get('overall_status')}, "
            f"stages={[(s['stage'], s['status']) for s in data['spark_stages']]}"
        )
        assert data["package_id"].startswith("pkg_"), (
            f"package_id 应以 pkg_ 开头，实际 {data['package_id']}"
        )

class TestSparkVerifyErrors:
    """错误路径——404 / 422。"""

    def test_invalid_request_id_returns_404(self, client, csv_path):
        """不存在的 request_id → 404 SPARK_ARTIFACTS_NOT_FOUND。"""
        resp = client.post("/api/spark/verify", json={
            "request_id": "req_nonexistent_12345",
        })
        assert resp.status_code == 404, (
            f"应返回 404，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_NOT_FOUND"
        assert "不存在" in data["message"] or "已过期" in data["message"]

    def test_incomplete_artifacts_returns_422(self, client, golden_spec_passing, csv_path):
        """仅 build_plan（无 contract）→ 422 SPARK_ARTIFACTS_INCOMPLETE。"""
        # ── 先执行 build_plan（不产生 contract）──
        resp_plan = client.post("/api/plan", json={
            "markdown_text": golden_spec_passing,
        })
        assert resp_plan.status_code == 200
        plan_result = resp_plan.json()
        request_id = plan_result["request_id"]

        # ── 触发 Spark 验证 ──
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422, (
            f"应返回 422，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_INCOMPLETE"
        assert "data_transform_contract" in data["message"]

    def test_stage_failure_returns_422_when_contract_none(
        self, client, golden_spec_passing, csv_path,
    ):
        """contract 为 None → 422 SPARK_ARTIFACTS_INCOMPLETE。

        通过替换 Pipeline._results 中的 contract 为 None 来模拟。
        """

        # ── Step 1: 正常 run-all ──
        resp_run = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp_run.status_code == 200
        request_id = resp_run.json()["request_id"]

        # ── Step 2: 注入损坏的 contract 到 _results ──
        pipeline = client.app.state.pipeline
        saved = pipeline._results.get(request_id)
        assert saved is not None, "_results 中应有该 request_id 的数据"
        # 将 contract 替换为 None——模拟缺失场景
        saved["contract"] = None

        # ── Step 3: 触发 Spark 验证（应因 contract 为 None 而失败）──
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422, (
            f"contract 为 None 应触发 422，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_INCOMPLETE"
