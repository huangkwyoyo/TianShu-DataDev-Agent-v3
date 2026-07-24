"""Run-All 流式响应中的 LLM 追踪序列化回归测试。"""

from __future__ import annotations

import json

from tianshu_datadev.api.pipeline import Pipeline


def _stage_results(trace: dict) -> list[dict]:
    """构造六个 Spark 阶段的确定性返回值。"""
    common = {"status": "ok", "llm_traces": trace, "errors": []}
    return [
        {**common, "result": {"type": "mapper"}},
        {**common, "result": {"type": "developer"}},
        {
            **common,
            "result": {
                "type": "compiler",
                "pyspark_code": "def transform(): pass",
                "standalone_pyspark": "print('spark')",
            },
        },
        {**common, "result": {"type": "validator"}},
        {
            **common,
            "result": {
                "type": "comparator",
                "status": "LOGIC_EQUIVALENT",
            },
        },
        {
            **common,
            "result": {
                "type": "physical_verify",
                "verification_status": "RESULT_CONSISTENT",
                "duckdb_row_count": 45,
                "spark_row_count": 45,
                "row_count_match": True,
                "schema_match": True,
                "total_diff_count": 0,
            },
        },
    ]


def test_run_all_stream_serializes_llm_trace_and_emits_complete_result(
    monkeypatch,
) -> None:
    """LlmTraceNode 不得阻断 done 事件及前端六个结果区所需字段。"""
    pipeline = Pipeline()
    request_id = "req_trace_stream"
    pipeline._record_trace(
        request_id,
        "spec_enricher",
        model="test-model",
        token_usage={
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
        },
        latency_ms=12,
        status="valid",
    )
    trace = pipeline._get_llm_traces(request_id)
    assert trace is not None

    spec_result = {
        "request_id": request_id,
        "spec_id": "spec_trace",
        "spec_hash": "a" * 64,
        "title": "追踪序列化",
        "description": "验证 Run-All 完整响应。",
        "tables": [],
        "metrics": [],
        "dimensions": [],
        "joins": [],
        "time_range": None,
        "output_spec": {
            "columns": ["result_value"],
            "grain": [],
            "sort_columns": [],
            "limit": None,
        },
        "open_questions": [],
        "parse_warnings": [],
    }
    sql_result = {
        "request_id": request_id,
        "pipeline_error": None,
        "pipeline_stages": [{"stage": "package", "status": "ok"}],
        "generated_sql": "SELECT 1 AS result_value",
        "spec_id": "spec_trace",
        "plan_id": "plan_trace",
        "package_id": "package_trace",
        "spec_result": spec_result,
        "steps": [
            {
                "step_type": "project",
                "step_id": "project_1",
                "description": "输出结果",
            },
        ],
        "join_evidence": [],
        "llm_traces": trace,
    }
    monkeypatch.setattr(pipeline, "run_all", lambda *args, **kwargs: sql_result)

    stage_results = iter(_stage_results(trace))
    monkeypatch.setattr(
        pipeline,
        "run_spark_stage",
        lambda *args, **kwargs: next(stage_results),
    )
    context = pipeline._get_or_create_spark_context(request_id)
    context.stage_results.update({
        "MAPPER": "SUCCESS",
        "COMPILER": "SUCCESS",
        "VALIDATOR": "SUCCESS",
        "COMPARATOR": "SUCCESS",
    })

    events = [
        json.loads(line)
        for line in pipeline.run_all_full_stream("test markdown")
    ]

    assert not any(event.get("error_code") == "SERIALIZATION_ERROR" for event in events)
    done = next(event for event in events if event["event"] == "done")
    result = done["result"]
    assert result["spec_result"] == spec_result
    assert result["steps"][0]["step_type"] == "project"
    assert result["generated_sql"] == "SELECT 1 AS result_value"
    assert result["pyspark_code"] == "def transform(): pass"
    assert result["llm_traces"]["spec_enricher"]["model"] == "test-model"

