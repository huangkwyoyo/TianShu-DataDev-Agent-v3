"""监控生命周期——绑定到 FastAPI app lifespan。"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from tianshu_datadev.monitor import cleanup, get_collector

logger = logging.getLogger(__name__)

# 日志目录——JSONL 与人类可读文本日志分离
_JSONL_LOG_DIR = Path("logs/monitor")
_TEXT_LOG_DIR = Path("logs/monitor-text")
_KEEP_GROUPS = 50


@asynccontextmanager
async def monitor_lifespan(app: FastAPI):
    """监控生命周期——在 FastAPI 启动时初始化采集器，关闭时 flush 并清理。

    startup：
        - 对 JSONL 和文本日志目录执行轮转清理（各保留最近 50 组）
        - 使用分离的目录创建采集器（根据 TIANSHU_RUN_ID 自动选择）
        - 绑定到 app.state.monitor_collector
    shutdown：
        - flush 队列（超时 10 秒）
        - 标记 run_complete 并 close()
    """
    # ── startup ──
    run_id = ""
    try:
        import os
        run_id = os.environ.get("TIANSHU_RUN_ID", "")
        if run_id:
            # 对两个目录分别执行轮转清理
            cleaned_jsonl = cleanup(_JSONL_LOG_DIR, run_id, keep_groups=_KEEP_GROUPS)
            cleaned_text = cleanup(_TEXT_LOG_DIR, run_id, keep_groups=_KEEP_GROUPS)
            if cleaned_jsonl or cleaned_text:
                logger.info(
                    "日志轮转——JSONL 清理 %d 组，文本日志清理 %d 组",
                    cleaned_jsonl, cleaned_text,
                )
    except Exception:
        logger.warning("日志轮转清理失败", exc_info=True)

    collector = get_collector(_JSONL_LOG_DIR, text_log_dir=_TEXT_LOG_DIR)
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
