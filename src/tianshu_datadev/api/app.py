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

from .error_handlers import register_error_handlers
from .pipeline import Pipeline
from .routes import api_router


def create_app(pipeline: Pipeline | None = None) -> FastAPI:
    """创建 FastAPI 应用实例。

    Args:
        pipeline: 可选的 Pipeline 实例（测试时可注入 mock）。
                  若为 None，使用默认 Pipeline。

    Returns:
        配置完成的 FastAPI 应用
    """
    app = FastAPI(
        title="TianShu DataDev Agent API",
        version="0.1.0",
        description="内部交互验证口——不对外暴露，不做生产执行。",
    )

    # CORS 中间件——允许本地开发
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 注入流水线
    app.state.pipeline = pipeline or Pipeline()

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
