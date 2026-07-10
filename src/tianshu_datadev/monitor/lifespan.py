"""监控生命周期——绑定到 FastAPI app lifespan。"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from tianshu_datadev.monitor import get_collector

logger = logging.getLogger(__name__)


@asynccontextmanager
async def monitor_lifespan(app: FastAPI):
    """监控生命周期——在 FastAPI 启动时初始化采集器，关闭时 flush 并清理。

    startup：
        - 使用 logs/monitor 目录创建采集器（根据 TIANSHU_RUN_ID 自动选择）
        - 绑定到 app.state.monitor_collector
    shutdown：
        - flush 队列（超时 10 秒）
        - 标记 run_complete 并 close()
    """
    # ── startup ──
    log_dir = Path("logs/monitor")
    collector = get_collector(log_dir)
    app.state.monitor_collector = collector
    logger.info(
        "监控采集器已初始化——enabled=%s",
        getattr(collector, "enabled", False),
    )
    yield
    # ── shutdown ──
    try:
        collector.flush_completed = collector.flush(timeout=10.0)
    except Exception:
        logger.warning("监控 flush 失败", exc_info=True)
    finally:
        collector.run_complete = True
        collector.close()
