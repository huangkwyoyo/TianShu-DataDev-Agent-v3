"""Phase 4.5A — REST API 路由处理器。

5 个端点，全部通过 Pipeline 编排：
  POST /api/spec/parse      — 解析 DeveloperSpec
  POST /api/plan             — 解析 + 构建 SqlBuildPlan + 验证
  POST /api/execute          — 全流程编译+执行（dry_run）
  GET  /api/package/{id}     — 获取 ReviewPackage manifest
  POST /api/run-all          — 全流程+打包

Phase 4.5B 新增前端 SPA 专用端点：
  GET  /api/templates        — 获取模板列表
  GET  /api/templates/{id}   — 获取指定模板详情
  GET  /api/health           — 健康检查
  POST /api/spec/parse-rich  — 富解析（含完整结构化结果）
  POST /api/plan-rich        — 富 Plan（含步骤详情+Join 证据）
  POST /api/execute-rich     — 富 Execute（含 SQL 文本）
  GET  /api/package-rich/{id}— 富 Package（含文件树）

Batch 3 — 监控端点：
  GET  /api/monitor/config   — 前端查询监控状态
  POST /api/monitor/browser-event — 浏览器安全上报
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

if TYPE_CHECKING:
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage

from .models import (
    ExecuteRequest,
    ParseSpecRequest,
    PlanRequest,
    RunAllRequest,
    SparkStageItem,
    SparkStageRequest,  # 新增
    SparkVerifyRequest,
    SparkVerifyResponse,
)

api_router = APIRouter(prefix="/api")


@api_router.post("/spec/parse")
async def parse_spec(request: Request, body: ParseSpecRequest):
    """解析 DeveloperSpec——返回结构化摘要。

    不返回完整 ParsedDeveloperSpec 对象，仅返回 table_count、
    metric_count 等元信息和 OpenQuestion/Warning 摘要。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.parse_only(body.markdown_text)
    return result


@api_router.post("/plan")
async def build_plan(request: Request, body: PlanRequest):
    """解析 + 构建 SqlBuildPlan + Validator 验证——返回 Plan 摘要。

    返回 plan_id、step 类型列表、验证结果和 OpenQuestion 摘要。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.build_plan(body.markdown_text, body.table_mapping)
    return result


@api_router.post("/execute")
async def execute_pipeline(request: Request, body: ExecuteRequest):
    """全流程编译+执行（dry_run）——返回执行摘要。

    dry_run 始终为 true——不提供生产执行入口。
    返回 execution_trace 摘要和 result_summary（不含 sample_rows）。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.execute(body.markdown_text, body.table_mapping, body.table_paths)
    return result


@api_router.get("/package/{request_id}")
async def get_package(request: Request, request_id: str):
    """获取 ReviewPackage manifest——返回 artifact 引用列表。

    仅返回文件路径和 SHA-256 引用，不返回文件内容。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.get_package(request_id)
    if result is None:
        return JSONResponse(
            status_code=404,
            content={
                "error_code": "NOT_FOUND",
                "message": f"request_id '{request_id}' 对应的 package 不存在",
                "field_ref": "request_id",
            },
        )
    return result


@api_router.post("/run-all")
async def run_all(request: Request, body: RunAllRequest):
    """全流程一键执行 + ReviewPackage 打包——返回完整摘要。

    串联 Parser → Builder → Validator → Compiler → Executor → Contract → Packager。
    dry_run 始终为 true——不提供生产写入开关。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.run_all(body.markdown_text, body.table_mapping, body.table_paths)
    return result


@api_router.post("/run-all-full")
async def run_all_full(request: Request, body: RunAllRequest):
    """全流程 SQL + Spark 管线——后端轻量编排，复用现有 dispatcher。

    串联：
    1. SQL 管线 8 阶段（parser → package）
    2. Spark 管线 6 阶段（MAPPER → PHYSICAL_VERIFIER）

    返回聚合 FullRunResponse：SQL 代码、PySpark 代码、双管线阶段状态、LLM 追踪。
    前端只需一次请求。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.run_all_full(body.markdown_text, body.table_mapping, body.table_paths)
    return result


@api_router.post("/run-all-full/stream")
async def run_all_full_stream(request: Request, body: RunAllRequest):
    """全流程 SQL + Spark 管线——NDJSON 流式进度推送。

    返回 application/x-ndjson 流，每行一个 JSON 事件：
    - stage: 阶段进度（pipeline + stage + status + duration_ms）
    - done: 全流程完成（含完整 FullRunResponse）
    - fatal: 致命错误
    - heartbeat: 心跳（保持连接）

    前端通过 fetch + ReadableStream 逐行消费，实时更新进度面板。
    连接断开时后台继续执行——结果通过 done 事件的 result 字段返回。
    """
    pipeline = request.app.state.pipeline
    return StreamingResponse(
        pipeline.run_all_full_stream(
            body.markdown_text, body.table_mapping, body.table_paths,
        ),
        media_type="application/x-ndjson",
        headers={
            "X-Accel-Buffering": "no",   # 禁用 nginx 缓冲
            "Cache-Control": "no-cache",  # 禁用缓存
        },
    )


@api_router.post("/run-all-rich")
async def run_all_rich(request: Request, body: RunAllRequest):
    """前端专用：全流程一键执行+富结果——返回 RunAllRichResponse。

    一步获得 PlanRich + ExecuteRich + PackageRich 的全部信息：
    步骤摘要、Join 证据、SQL 文本、执行追踪、文件树。
    前端无需分两次请求（execute-rich + package-rich）。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.run_all_rich(body.markdown_text, body.table_mapping, body.table_paths)
    return result


# ════════════════════════════════════════════
# Phase 4.5B — 前端 SPA 专用端点
# ════════════════════════════════════════════


@api_router.get("/templates")
async def list_templates(request: Request):
    """获取 DeveloperSpec 模板列表——返回模板元信息（不含 markdown_template）。"""
    pipeline = request.app.state.pipeline
    templates = pipeline.get_templates()
    return {"templates": templates, "count": len(templates)}


@api_router.get("/templates/{template_id}")
async def get_template(request: Request, template_id: str):
    """获取指定模板的完整定义——含 markdown_template 正文。"""
    pipeline = request.app.state.pipeline
    template = pipeline.get_template(template_id)
    if template is None:
        return JSONResponse(
            status_code=404,
            content={
                "error_code": "NOT_FOUND",
                "message": f"模板 '{template_id}' 不存在",
                "field_ref": "template_id",
            },
        )
    return template


@api_router.get("/health")
async def health_check(request: Request):
    """API 健康检查——返回服务状态和版本信息。"""
    return {
        "status": "ok",
        "version": "0.1.0",
        "pipeline_ready": request.app.state.pipeline is not None,
    }


@api_router.post("/spec/parse-rich")
async def parse_spec_rich(request: Request, body: ParseSpecRequest):
    """前端专用：完整解析 DeveloperSpec——返回 SpecRichResponse。

    包含全部结构化解析结果：表、字段、指标、维度、Join、时间范围等，
    供前端渲染结构化预览面板和 OpenQuestion 面板。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.parse_rich(body.markdown_text)
    return result


@api_router.post("/plan-rich")
async def build_plan_rich(request: Request, body: PlanRequest):
    """前端专用：构建 Plan + 提取 Join 证据——返回 PlanRichResponse。

    包含步骤详情列表和 Join 推理证据链（STRONG/MEDIUM/WEAK/NONE），
    供前端渲染 SqlBuildPlan 步骤面板和 Join 证据面板。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.build_plan_rich(body.markdown_text, body.table_mapping)
    return result


@api_router.post("/execute-rich")
async def execute_pipeline_rich(request: Request, body: ExecuteRequest):
    """前端专用：全流程编译+执行——返回 ExecuteRichResponse（含 SQL 文本）。

    返回生成的 SQL 全文和执行结果，供前端渲染 SQL 展示面板。
    dry_run 始终为 true——不提供生产执行入口。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.execute_rich(body.markdown_text, body.table_mapping, body.table_paths)
    return result


@api_router.get("/artifacts/{request_id}/status")
async def get_artifacts_status(request: Request, request_id: str):
    """检查指定 request_id 的 artifacts 是否就绪——供前端 Spark 按钮 gating 使用。

    返回 artifacts_ready 标记 + 已就绪的产物类型列表。
    artifacts_ready=true 表示可安全触发 Spark MAPPER 阶段。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.check_artifacts_status(request_id)
    return result


@api_router.get("/package-rich/{request_id}")
async def get_package_rich(request: Request, request_id: str):
    """前端专用：获取 ReviewPackage 文件树——返回 PackageRichResponse。

    返回文件树结构供前端渲染 Review Package 文件浏览器。
    """
    pipeline = request.app.state.pipeline
    result = pipeline.get_package(request_id, rich=True)
    if result is None:
        return JSONResponse(
            status_code=404,
            content={
                "error_code": "NOT_FOUND",
                "message": f"request_id '{request_id}' 对应的 package 不存在",
                "field_ref": "request_id",
            },
        )
    return result


# ════════════════════════════════════════════
# Spark 管线验证端点
# ════════════════════════════════════════════


@api_router.post("/spark/verify")
async def spark_verify(request: Request, body: SparkVerifyRequest):
    """触发 Spark 管线验证——返回 6 阶段结果 + REVIEW_READY 判定。

    处理流程：
    1. Pipeline.export_artifacts(request_id) → 提取 SqlBuildPlan + Contract
    2. adapt_lite_to_v1() → 将 Lite 契约升级为 V1
    3. SparkOrchestrator.run(contract=v1, sql_plan=sql_build_plan) → 执行全链路
    4. SparkReviewBuilder.build(state) → REVIEW_READY 判定
    5. 将 SparkPipelineState.stage_results 映射为前端 status 字符串

    错误码：
    - SPARK_ARTIFACTS_NOT_FOUND (404)：request_id 对应的 artifacts 不存在或已过期
    - SPARK_ARTIFACTS_INCOMPLETE (422)：sql_build_plan 或 data_transform_contract 为 None
    - SPARK_VERIFY_FAILED (500)：Orchestrator 执行过程中发生未预期异常
    """
    # 映射 SparkPipelineState 值 → 前端 status
    _status_map = {
        "SUCCESS": "ok",
        "FAILURE": "failed",
        "HUMAN_REVIEW": "failed",
        "SKIPPED": "skipped",
        "NOT_EXECUTED": "skipped",
    }

    pipeline = request.app.state.pipeline

    # ── Step 1: 导出 artifacts ──
    bundle = pipeline.export_artifacts(body.request_id)
    if bundle is None:
        return JSONResponse(
            status_code=404,
            content={
                "error_code": "SPARK_ARTIFACTS_NOT_FOUND",
                "message": (
                    f"request_id '{body.request_id}' 对应的 artifacts 不存在或已过期。"
                    f"请先执行全流程 Run-All 生成 artifacts。"
                ),
                "field_ref": "request_id",
            },
        )

    # ── Step 2: 校验 artifacts 完整性 ──
    if bundle.sql_build_plan is None or bundle.data_transform_contract is None:
        missing_parts: list[str] = []
        if bundle.sql_build_plan is None:
            missing_parts.append("sql_build_plan")
        if bundle.data_transform_contract is None:
            missing_parts.append("data_transform_contract")
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "SPARK_ARTIFACTS_INCOMPLETE",
                "message": (
                    f"request_id '{body.request_id}' 的 artifacts 不完整："
                    f"缺少 {', '.join(missing_parts)}。"
                    f"请使用全流程 Run-All（而非仅 build_plan 或 execute）生成完整 artifacts。"
                ),
                "field_ref": "request_id",
            },
        )

    # ── Step 3: Contract 适配（Lite → V1）──
    try:
        from tianshu_datadev.artifacts.models import DataTransformContractV1
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder

        raw_contract = bundle.data_transform_contract
        if isinstance(raw_contract, DataTransformContractV1):
            v1_contract = raw_contract
        else:
            v1_contract = adapt_lite_to_v1(raw_contract)

        # ── Step 4: 执行 Spark Orchestrator ──
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=v1_contract,
            sql_plan=bundle.sql_build_plan,
        )

        # ── Step 5: REVIEW_READY 判定 ──
        builder = SparkReviewBuilder()
        pkg = builder.build(state)

        # ── Step 6: 映射阶段状态 → 前端格式 ──
        spark_stages: list[SparkStageItem] = []
        for stage_name, result in state.stage_results.items():
            spark_stages.append(SparkStageItem(
                stage=stage_name,
                status=_status_map.get(result, "skipped"),
            ))

        # ── Step 7: 构造响应 ──
        return SparkVerifyResponse(
            request_id=body.request_id,
            spark_stages=spark_stages,
            overall_status=pkg.overall_status,
            comparator_status=pkg.comparator_status,
            review_ready=pkg.review_ready,
            package_id=pkg.package_id,
            errors=list(state.errors),
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "SPARK_VERIFY_FAILED",
                "message": f"Spark 管线验证执行异常：{e}",
                "field_ref": None,
            },
        )


# ════════════════════════════════════════════
# Spark 阶段独立触发端点（Phase: spark-stage-independent）
# ════════════════════════════════════════════


def _handle_spark_stage(
    request: Request,
    request_id: str,
    stage: "SparkPipelineStage",
):
    """Spark 阶段统一处理——参数校验、异常转换、调用 dispatcher。

    捕获 SparkDependencyMissingError → 422，
    其他异常 → 500。
    """
    from tianshu_datadev.api.pipeline import SparkDependencyMissingError

    pipeline = request.app.state.pipeline
    try:
        return pipeline.run_spark_stage(request_id, stage)
    except SparkDependencyMissingError as e:
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "SPARK_DEPENDENCY_MISSING",
                "message": str(e),
                "field_ref": e.stage.value if e.stage else None,
                "missing_dependencies": e.missing,
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "SPARK_STAGE_FAILED",
                "message": f"Spark 阶段 {stage.value} 执行异常：{e}",
                "field_ref": stage.value,
            },
        )


@api_router.post("/spark/map")
async def spark_map(request: Request, body: SparkStageRequest):
    """Spark MAPPER 阶段——Contract → SparkPlan 映射。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.MAPPER)


@api_router.post("/spark/develop")
async def spark_develop(request: Request, body: SparkStageRequest):
    """Spark DEVELOPER 阶段——LLM 语义标注（可选）。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.DEVELOPER)


@api_router.post("/spark/compile")
async def spark_compile(request: Request, body: SparkStageRequest):
    """Spark COMPILER 阶段——SparkPlan → PySpark DSL 编译。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPILER)


@api_router.post("/spark/validate")
async def spark_validate(request: Request, body: SparkStageRequest):
    """Spark VALIDATOR 阶段——PySpark DSL 静态安全校验。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.VALIDATOR)


@api_router.post("/spark/compare")
async def spark_compare(request: Request, body: SparkStageRequest):
    """Spark COMPARATOR 阶段——SQL ↔ Spark 逻辑链路对比。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPARATOR)


@api_router.post("/spark/physical-verify")
async def spark_physical_verify(request: Request, body: SparkStageRequest):
    """Spark PHYSICAL_VERIFIER 阶段——双引擎物理结果对比。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.PHYSICAL_VERIFIER)


# ════════════════════════════════════════════
# Batch 3 — 监控端点
# ════════════════════════════════════════════

# 速率限制状态（按 run_id）
_rate_limit_state: dict[str, list[float]] = defaultdict(list)
_total_count_state: dict[str, int] = defaultdict(int)


@api_router.get("/monitor/config")
def monitor_config(request: Request):
    """前端查询监控状态——返回 enabled、run_id、monitor_token、限流阈值。

    监控未启用时返回 {"enabled": False}，前端据此隐藏监控面板。
    """
    collector = getattr(request.app.state, "monitor_collector", None)
    if collector is None or not getattr(collector, "enabled", True):
        return {"enabled": False}
    return {
        "enabled": True,
        "run_id": collector.run_id,
        "monitor_token": os.environ.get("TIANSHU_MONITOR_TOKEN", ""),
        "rate_limit_per_minute": 20,
        "max_total_events": 200,
    }


@api_router.post("/monitor/browser-event", status_code=204)
async def browser_event(payload: dict, request: Request):
    """浏览器安全上报——6 层安全校验链后写入浏览器事件。

    安全校验链（任一失败返回 403/404）：
    1. 监控未启用 → 404
    2. Origin 白名单（127.0.0.1:5173, localhost:5173）→ 403
    3. monitor_token 缺失或不匹配 → 403
    4. run_id 与 TIANSHU_RUN_ID 不匹配 → 403
    5. 速率限制（每分钟 20 条）→ 429
    6. 总量限制（总共 200 条）→ 429
    7. 请求体 > 4KB → 413
    """
    collector = getattr(request.app.state, "monitor_collector", None)
    if collector is None or not getattr(collector, "enabled", True):
        raise HTTPException(status_code=404)  # 不暴露原因

    # ── 1. Origin 白名单 ──
    origin = request.headers.get("Origin", "")
    if origin not in ("http://127.0.0.1:5173", "http://localhost:5173"):
        raise HTTPException(status_code=403)

    # ── 2. Token 校验 ──
    token = payload.get("monitor_token", "")
    if not token or token != os.environ.get("TIANSHU_MONITOR_TOKEN", ""):
        raise HTTPException(status_code=403)

    # ── 3. Run ID 校验 ──
    run_id = payload.get("run_id", "")
    if run_id != os.environ.get("TIANSHU_RUN_ID", ""):
        raise HTTPException(status_code=403)

    # ── 4. 请求体大小限制 ──
    body_str = json.dumps(payload)
    if len(body_str.encode()) > 4096:
        raise HTTPException(status_code=413)

    # ── 5. 速率限制——每分钟 20 条 ──
    now = time.time()
    window = [t for t in _rate_limit_state[run_id] if now - t < 60]
    if len(window) >= 20:
        raise HTTPException(status_code=429)
    window.append(now)
    _rate_limit_state[run_id] = window

    # ── 6. 总量限制——200 条 ──
    if _total_count_state[run_id] >= 200:
        raise HTTPException(status_code=429)
    _total_count_state[run_id] += 1

    # ── 7. 黑名单检查（对原始 payload 检查，禁止上报敏感字段）──
    forbidden = {"request_body", "response_body", "headers", "authorization", "cookie"}
    for key in forbidden:
        if key in payload:
            raise HTTPException(status_code=400)

    # ── 8. 白名单字段过滤（移除未在允许列表中的字段）──
    allowed = {
        "event_type", "timestamp", "run_id", "monitor_token",
        "api_path", "api_status", "api_duration_ms",
        "error_type", "error_message", "stack_frames", "url",
    }
    for key in list(payload.keys()):
        if key not in allowed:
            del payload[key]

    collector.log_browser_event(payload)
    return Response(status_code=204)
