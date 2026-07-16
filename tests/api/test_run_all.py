"""tests/api/test_run_all.py——POST /api/run-all 测试。"""

# 两表 LEFT JOIN 但 dim 表联结键未声明 unique: true——
# 触发 Q-JOIN-SAFETY blocking OpenQuestion（LEFT JOIN 唯一性安全门禁）
_SPEC_LEFT_JOIN_NO_UNIQUE = """# 门禁泄漏回归用例

```markdown
---
spec:
  type: detail_table
  target_table: ads.gate_leak_regression
  target_grain: [order_id]
  summary: "两表 LEFT JOIN 无 unique 声明——应在 validate 阶段阻断"

  source_tables:
    - name: fact_orders
      alias: fo
      row_count: 1000
      role: fact
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
      business_columns:
        - name: product_code
          type: varchar
          nullable: false
        - name: amount
          type: decimal(12,2)
          nullable: true

    - name: dim_product
      alias: dp
      row_count: 50
      role: dim
      key_columns:
        - name: product_code
          type: varchar
          nullable: false
      business_columns:
        - name: product_name
          type: varchar
          nullable: true

  metrics: []
  limit: 100

  dimensions:
    - dimension_name: order_id
      column_ref: order_id

  joins:
    - left_table: fo
      right_table: dp
      left_key: product_code
      right_key: product_code
      join_type: LEFT

  output_columns:
    - name: order_id
      type: bigint
    - name: product_name
      type: varchar
---

# 门禁泄漏回归用例
```
"""


class TestRunAllBlockingQuestionGate:
    """run_all 对 enrich 阶段 blocking OpenQuestion 的门禁——

    回归背景：LEFT JOIN 唯一性门禁产生 blocking Q-JOIN-SAFETY 后，
    候选被丢弃但流水线未阻断，静默退化为单表计划，
    最终在 execute 阶段以晦涩的 Binder Error 暴露（NYC Case03/04 存量失败根因）。
    """

    def test_blocking_extra_question_blocks_at_validate(self, pipeline):
        """blocking extra_questions 应在 validate 阶段阻断，不得进入 compile/execute。"""
        result = pipeline.run_all(_SPEC_LEFT_JOIN_NO_UNIQUE)

        # 阻断：validation_passed=False + ValidationBlocked
        assert result["validation_passed"] is False
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "validate"
        assert result["pipeline_error"]["error_type"] == "ValidationBlocked"

        # blocking 问题必须出现在 open_questions 中（用户可见的真实原因）
        blocking_qs = [q for q in result["open_questions"] if q["blocking"]]
        assert len(blocking_qs) >= 1
        assert any("unique" in q["description"] for q in blocking_qs)

        # 阶段状态：validate failed，后续全部 skipped
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert stages["validate"] == "failed"
        assert stages["compile"] == "skipped"
        assert stages["execute"] == "skipped"


class TestRunAll:
    """POST /api/run-all——全流程+打包 → RunAllResponse 摘要。"""

    def test_run_all_success(self, client, golden_spec_passing, csv_path):
        """全流程成功——需要 DuckDB 和 CSV fixture。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["package_id"].startswith("pkg_")
        assert data["artifact_count"] > 0
        assert "execution_trace" in data
        assert "result_summary" in data
        # 统一的链路状态字段——调用方单点判断
        assert "validation_passed" in data
        assert "open_questions" in data

    def test_run_all_invalid_spec(self, client, csv_path):
        """无效输入 → 200 + pipeline_error（Pipeline 内部捕获，8 阶段）。"""
        resp = client.post("/api/run-all", json={"markdown_text": ""})
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "pipeline_error" in data
        assert data["pipeline_error"]["stage"] == "parser"
        # run_all 使用 8 阶段（含 validate）
        assert len(data["pipeline_stages"]) == 8
        # 验证 contract/package 在 7 阶段中
        stage_names = [s["stage"] for s in data["pipeline_stages"]]
        assert "contract" in stage_names
        assert "package" in stage_names

    def test_run_all_success_no_pipeline_error(self, client, golden_spec_passing, csv_path):
        """成功全流程 → 不含 pipeline_error 字段。"""
        resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_error" not in data
        # 成功路径应包含 pipeline_stages（8 阶段全部 ok），供流式进度使用
        assert "pipeline_stages" in data
        assert len(data["pipeline_stages"]) == 8
        assert all(s["status"] == "ok" for s in data["pipeline_stages"])
        stage_names = [s["stage"] for s in data["pipeline_stages"]]
        assert stage_names == ["parser", "enrich", "build", "validate", "compile", "execute", "contract", "package"]
        assert data["package_id"].startswith("pkg_")
        # 成功路径应包含链路状态字段
        assert "validation_passed" in data
        assert "open_questions" in data

    def test_run_all_build_failure(self, pipeline, golden_spec_passing):
        """run_all build 阶段失败 → 保留 spec + manifest。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        original_builder = pipeline_mod.SqlBuildPlanBuilder

        class FailingBuilder:
            def build(self, spec, hypothesis=None):
                raise ValueError("模拟 run_all build 失败")

        pipeline_mod.SqlBuildPlanBuilder = FailingBuilder
        try:
            result = pipeline.run_all(golden_spec_passing)
        finally:
            pipeline_mod.SqlBuildPlanBuilder = original_builder

        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "build"
        # 8 阶段（含 validate）
        assert len(result["pipeline_stages"]) == 8
        # 产物已保存
        assert result["request_id"] in pipeline._results
        saved = pipeline._results[result["request_id"]]
        assert "parsed_spec" in saved
        assert "manifest" in saved

    def test_run_all_execute_failure_blocks_package(self, pipeline, golden_spec_passing):
        """run_all() Executor 返回 RUNTIME_FAIL → 阻断，不含 package_id。"""
        import tianshu_datadev.api.pipeline as pipeline_mod
        from tianshu_datadev.sql.models import ExecutionStatus, ExecutionTrace, ResultSummary

        class FailingExecutor:
            """模拟执行失败——RUNTIME_FAIL，不抛异常。"""
            def execute(self, compiled):
                trace = ExecutionTrace(
                    trace_id="trace_fail_r",
                    plan_id=compiled.input_plan_hash,
                    engine="duckdb", generated_sql=compiled.sql,
                    status=ExecutionStatus.RUNTIME_FAIL,
                    row_count=0, execution_time_ms=5.0,
                    error_message="模拟 run_all 执行失败",
                )
                summary = ResultSummary(
                    summary_id="summary_fail_r",
                    trace_id="trace_fail_r",
                    engine="duckdb", columns=[], column_types=[],
                    row_count=0, null_counts={}, numeric_sums={}, sample_rows=[],
                )
                return trace, summary

        original = pipeline_mod.DuckDBExecutor
        pipeline_mod.DuckDBExecutor = lambda **kw: FailingExecutor()
        try:
            result = pipeline.run_all(golden_spec_passing)
        finally:
            pipeline_mod.DuckDBExecutor = original

        # 阻断：返回 pipeline_error
        assert "pipeline_error" in result
        assert result["pipeline_error"]["stage"] == "execute"
        # 不应有 package_id
        assert "package_id" not in result or result.get("package_id") == ""
        # 8 阶段，execute=failed, contract=skipped, package=skipped
        stages = {s["stage"]: s["status"] for s in result["pipeline_stages"]}
        assert len(result["pipeline_stages"]) == 8
        assert stages["execute"] == "failed"
        assert stages["contract"] == "skipped"
        assert stages["package"] == "skipped"
