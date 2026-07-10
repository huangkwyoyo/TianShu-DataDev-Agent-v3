"""FastAPI 应用工厂——创建 TianShu DataDev Agent 内部 API 实例。

用法:
    app = create_app()
    # 开发服务器: uvicorn tianshu_datadev.api.app:create_app --reload

Phase 4.5B 新增静态文件服务——挂载 frontend/dist 为 SPA 根路径。
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from tianshu_datadev.monitor.lifespan import monitor_lifespan
from tianshu_datadev.monitor.middleware import MonitorMiddleware

from .error_handlers import register_error_handlers
from .pipeline import Pipeline
from .routes import api_router


def _discover_csv_fixtures() -> dict[str, str]:
    """自动发现 tests/fixtures/ 目录下的 CSV fixture 文件。

    以文件名（不含扩展名）为 key、绝对路径为 value，构建 DuckDB 所需的
    table_paths 映射。

    仅在 TIANSHU_E2E_MODE=true 时由 create_app() 调用，
    生产路径不触发此函数——避免测试数据泄漏到生产环境。

    扫描范围：tests/fixtures/ 及所有子目录中的 *.csv 文件。

    Returns:
        {表名: CSV 绝对路径} 映射字典——目录不存在时返回空字典
    """
    import glob

    # 从当前文件位置推导仓库根目录（app.py → api → tianshu_datadev → src → repo_root）
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures")
    if not os.path.isdir(fixtures_dir):
        return {}

    mapping: dict[str, str] = {}
    for csv_file in glob.glob(
        os.path.join(fixtures_dir, "**", "*.csv"), recursive=True
    ):
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        mapping[table_name] = os.path.abspath(csv_file)
    return mapping


def _discover_nyc_duckdb() -> str | None:
    """自动发现 NYC 出租车数据仓库 DuckDB 文件。

    按优先级依次尝试以下路径，返回第一个存在的文件绝对路径。
    若都不存在，返回 None——Pipeline 将以纯 in-memory 模式运行。

    Returns:
        数据库文件绝对路径或 None
    """
    candidate_paths = [
        os.path.join(
            "D:\\", "ProgramData", "Datawarehouse",
            "纽约市城市交通", "nyc_transport.duckdb",
        ),
    ]
    for p in candidate_paths:
        if os.path.isfile(p):
            return os.path.abspath(p)
    return None


def _inject_snapshot_deps(pipeline: Pipeline, fixture_paths: dict[str, str]) -> None:
    """仅在存在显式配置/default_table_paths 时注入 SnapshotBuilder + SnapshotSourceProvider。

    白名单仅来自显式发现的 CSV fixture 文件——禁止自动扫描目录全量加入。
    不满足条件时不做任何操作，PHYSICAL_VERIFIER 将返回 SNAPSHOT_NOT_READY。

    Args:
        pipeline: Pipeline 实例（已创建，含 default_table_paths）
        fixture_paths: 表名 → CSV 绝对路径映射（来自 _discover_csv_fixtures）
    """
    if not fixture_paths:
        return

    from tianshu_datadev.spark.snapshot import (
        SnapshotBuilder,
        SnapshotSourceProvider,
        SnapshotSourceType,
    )

    # 使用 fixtures 根目录作为 base_path——子目录中的文件由 execute_rich 主路径处理
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures")

    provider = SnapshotSourceProvider(
        provider_id="e2e_csv_fixtures",
        source_type=SnapshotSourceType.LOCAL_FIXTURE,
        connection_alias="local",
        allowlisted_tables=sorted(fixture_paths.keys()),
        base_path=fixtures_dir,
        description="E2E 测试 CSV fixture 文件——白名单来自显式发现结果",
    )
    # 使用临时目录作为快照输出根目录——避免累积磁盘文件
    import tempfile as _tempfile
    output_dir = _tempfile.mkdtemp(prefix="tianshu_snapshots_")
    builder = SnapshotBuilder(output_dir=output_dir)
    pipeline.inject_snapshot_deps(builder, provider)


def create_app(pipeline: Pipeline | None = None) -> FastAPI:
    """创建 FastAPI 应用实例。

    Args:
        pipeline: 可选的 Pipeline 实例（测试时可注入 mock）。
                  若为 None，使用默认 Pipeline。
                  CSV fixture 自动发现仅在 TIANSHU_E2E_MODE=true 时启用。

    Returns:
        配置完成的 FastAPI 应用
    """
    import logging

    from tianshu_datadev.config import load_dotenv
    from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter
    from tianshu_datadev.prompts.manager import PromptManager
    from tianshu_datadev.spark.developer import SparkDeveloperService

    logger = logging.getLogger(__name__)

    # ── Phase 8: 加载 .env 环境变量 ──
    load_dotenv()

    # ── Phase 8: 创建 SparkDeveloperService（API Key preflight）──
    spark_developer_service = None
    api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            adapter = AnthropicAdapter()
            prompt_manager = PromptManager()
            spark_developer_service = SparkDeveloperService.from_provider_adapter(
                adapter, prompt_manager, max_llm_retries=1
            )
            logger.info("SparkDeveloperService 初始化成功——DEVELOPER 阶段将调用 DeepSeek API")
        except Exception as exc:
            logger.warning(
                "SparkDeveloperService 创建失败（key 存在但初始化异常），"
                "DEVELOPER 阶段将标记 SKIPPED: %s", exc
            )
    else:
        logger.info(
            "未检测到 DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY——"
            "SparkDeveloperService 跳过，DEVELOPER 阶段将标记 SKIPPED"
        )

    # 监控系统自动启用——仅当 TIANSHU_RUN_ID 未设置时自动生成
    # 环境变量存在但为空字符串 → 显式禁用（get_collector 返回 NullCollector）
    # 环境变量不存在 → 自动生成 run_id（默认启用）
    if "TIANSHU_RUN_ID" not in os.environ:
        from datetime import datetime
        os.environ["TIANSHU_RUN_ID"] = datetime.now().strftime("%Y%m%d-%H%M%S")

    app = FastAPI(
        title="TianShu DataDev Agent API",
        version="0.1.0",
        description="内部交互验证口——不对外暴露，不做生产执行。",
        lifespan=monitor_lifespan,
    )

    # CORS 中间件——允许本地开发
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # HTTP 监控中间件——记录请求事件，异常时 bare raise
    app.add_middleware(MonitorMiddleware)

    # 注入流水线——未显式传入时仅在 E2E 测试模式下自动发现 CSV fixture 文件
    # 生产路径不扫描 tests/fixtures/，避免测试数据泄漏到生产环境
    if pipeline is None:
        # 自动发现 NYC 数据仓库 DuckDB 文件
        db_path = _discover_nyc_duckdb()
        _e2e_val = os.environ.get("TIANSHU_E2E_MODE")
        if _e2e_val == "true":
            fixture_paths = _discover_csv_fixtures()
            pipeline = Pipeline(
                default_table_paths=fixture_paths,
                duckdb_path=db_path,
                developer_service=spark_developer_service,
            )
            # ── 注入 SnapshotBuilder + SnapshotSourceProvider（仅当存在显式配置时）──
            # 白名单仅来自显式发现的 CSV fixture 文件，禁止自动扫描目录全量加入
            _inject_snapshot_deps(pipeline, fixture_paths)
        else:
            pipeline = Pipeline(
                duckdb_path=db_path,
                developer_service=spark_developer_service,
            )
    app.state.pipeline = pipeline
    app.state.spark_developer_service = spark_developer_service

    # 注册异常处理器
    register_error_handlers(app)

    # 注册 API 路由
    app.include_router(api_router)

    # Phase 4.5B：挂载前端 SPA 静态文件
    _mount_spa(app)

    return app


def _mount_spa(app: FastAPI) -> None:
    """挂载前端 SPA 静态文件目录。

    查找顺序：
    1. frontend/dist/（Vite 构建输出）
    2. web/（备选静态目录）

    若两者都不存在，跳过挂载（纯 API 模式）。
    """
    import_paths = [
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "frontend", "dist"),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "web"),
    ]

    spa_dir = None
    for p in import_paths:
        abs_path = os.path.abspath(p)
        if os.path.isdir(abs_path):
            spa_dir = abs_path
            break

    if spa_dir is None:
        return

    # 挂载静态文件（不含 index.html——需单独处理 SPA fallback）
    assets_dir = os.path.join(spa_dir, "assets") if "assets" in os.listdir(spa_dir) else None
    if assets_dir and os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="spa_assets")

    # SPA fallback：非 /api 请求返回 index.html
    from fastapi.responses import HTMLResponse

    index_path = os.path.join(spa_dir, "index.html")
    if not os.path.isfile(index_path):
        return

    # 读取 index.html 内容用于 SPA fallback
    with open(index_path, "r", encoding="utf-8") as f:
        index_html = f.read()

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str):
        """SPA fallback——非 /api 请求返回 index.html。"""
        # /api 开头的路径由 api_router 处理（已在上方注册）
        # 其余路径返回 index.html 供前端路由接管
        return HTMLResponse(content=index_html)
