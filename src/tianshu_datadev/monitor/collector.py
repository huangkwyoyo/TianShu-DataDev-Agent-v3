"""监控采集器——线程安全单写者 RunLogCollector。

内部使用 queue.Queue + 单消费者线程写 JSONL 文件。
emit() 非阻塞入队，队列满时丢弃并计数。
"""

import json
import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from tianshu_datadev.monitor.models import (
    BrowserEvent,
    MonitorEvent,
    ResourceSample,
    StageEvent,
)
from tianshu_datadev.monitor.renderer import LogRenderer
from tianshu_datadev.monitor.sanitizer import Sanitizer

logger = logging.getLogger(__name__)


class NullCollector:
    """无 TIANSHU_RUN_ID 时的空操作采集器。"""

    enabled: bool = False
    run_id: str = ""

    def emit(self, event: MonitorEvent) -> None:
        pass

    def stage(
        self,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ) -> "StageContext":
        """返回空操作的 StageContext。"""
        return StageContext(self, "", "", "", None)

    def log_resource_sample(self, sample: ResourceSample) -> None:
        pass

    def log_browser_event(self, payload: dict) -> None:
        pass

    def flush(self, timeout: float = 5.0) -> bool:
        return True

    def close(self) -> None:
        pass


class RunLogCollector:
    """线程安全单写者——唯一写入 _events.jsonl 的组件。

    内部使用 queue.Queue + 单消费者线程写文件。
    emit() 非阻塞 put_nowait——队列满时 dropped_event_count += 1。
    close() 原子输出 *_collector_status.json。
    """

    def __init__(
        self, log_dir: Path, run_id: str,
        text_log_dir: Path | None = None,
        max_queue: int = 10000,
    ):
        self._log_dir = log_dir
        self._text_log_dir = text_log_dir if text_log_dir is not None else log_dir
        self.run_id = run_id
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self.dropped_event_count: int = 0
        self.flush_completed: bool = False
        self.run_complete: bool = False
        self._running: bool = True

        # 创建 JSONL 日志目录并打开文件
        self._log_dir.mkdir(parents=True, exist_ok=True)
        file_path = self._log_dir / f"tianshu_run_{run_id}_events.jsonl"
        self._file: TextIO = open(file_path, "w", encoding="utf-8")

        # 人类可读文本日志——写入独立目录
        self._text_log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self._text_log_dir / f"tianshu_run_{run_id}_events.log"
        self._text_file: TextIO = open(log_path, "w", encoding="utf-8")

        # 启动 writer 消费者线程
        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name=f"monitor-writer-{run_id}"
        )
        self._writer_thread.start()

    def _writer_loop(self) -> None:
        """消费者线程主循环——从队列取事件，先写 JSONL 再写文本日志。"""
        while self._running or not self._queue.empty():
            try:
                event = self._queue.get(timeout=0.5)
                # JSONL 写入（不变）
                json_line = event.model_dump_json() + "\n"
                self._file.write(json_line)
                self._file.flush()
                # 文本日志写入（新增）
                event_dict = json.loads(event.model_dump_json())
                text_output = LogRenderer.format_event(event_dict)
                if text_output:
                    self._text_file.write(text_output + "\n")
                    self._text_file.flush()
            except queue.Empty:
                continue
            except Exception as exc:
                logging.warning("监控写入失败: %s", exc)

    def emit(self, event: MonitorEvent) -> None:
        """非阻塞入队——队列满时 dropped_event_count += 1 + logging.warning。"""
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped_event_count += 1
            logging.warning("监控队列满，事件丢弃")

    def stage(
        self,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ) -> "StageContext":
        """返回 StageContext 上下文管理器。"""
        stage_run_id = f"stage_{node}_{uuid.uuid4().hex[:8]}"
        return StageContext(
            collector=self,
            stage_run_id=stage_run_id,
            node=node,
            artifact_request_id=artifact_request_id,
            parent_stage_run_id=parent_stage_run_id,
        )

    def log_resource_sample(self, sample: ResourceSample) -> None:
        """写入资源采样（sanitize 后 emit）。"""
        Sanitizer.validate_event(sample)
        self.emit(sample)

    def log_browser_event(self, payload: dict) -> None:
        """写入浏览器事件。"""
        event = BrowserEvent(**payload)
        Sanitizer.validate_event(event)
        self.emit(event)

    def flush(self, timeout: float = 5.0) -> bool:
        """排空队列——等待 writer 线程处理完当前队列。

        设置 self.flush_completed。返回是否排空。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._queue.empty():
                self.flush_completed = True
                return True
            time.sleep(0.05)
        return False

    def close(self) -> None:
        """停止 writer 线程 → 关闭文件 → 原子输出 *_collector_status.json。"""
        self._running = False
        self._writer_thread.join(timeout=5.0)
        self._file.close()
        self._text_file.close()
        self.run_complete = True

        # 原子输出状态文件
        status = {
            "run_id": self.run_id,
            "dropped_event_count": self.dropped_event_count,
            "flush_completed": self.flush_completed,
            "run_complete": True,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        status_path = (
            self._log_dir / f"tianshu_run_{self.run_id}_collector_status.json"
        )
        tmp_path = (
            self._log_dir / f"tianshu_run_{self.run_id}_collector_status.tmp"
        )
        tmp_path.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
        tmp_path.rename(status_path)


@dataclass
class StageContext:
    """阶段上下文管理器——进入写 started，退出写 completed/failed。"""

    collector: "RunLogCollector"
    stage_run_id: str
    node: str
    artifact_request_id: str
    parent_stage_run_id: str | None
    _started_at: float = field(default_factory=time.time)
    _result: dict = field(default_factory=dict)

    def set_result(self, **kwargs) -> None:
        """设置 artifact_path、artifact_sha256、row_count 等。"""
        self._result.update(kwargs)

    def __enter__(self) -> "StageContext":
        self.collector.emit(
            StageEvent(
                run_id=self.collector.run_id,
                stage_run_id=self.stage_run_id,
                node=self.node,
                artifact_request_id=self.artifact_request_id,
                parent_stage_run_id=self.parent_stage_run_id,
                status="started",
            )
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_ms = int((time.time() - self._started_at) * 1000)
        if exc_type is None:
            self.collector.emit(
                StageEvent(
                    run_id=self.collector.run_id,
                    stage_run_id=self.stage_run_id,
                    node=self.node,
                    artifact_request_id=self.artifact_request_id,
                    parent_stage_run_id=self.parent_stage_run_id,
                    status="completed",
                    duration_ms=duration_ms,
                    **self._result,
                )
            )
        else:
            self.collector.emit(
                StageEvent(
                    run_id=self.collector.run_id,
                    stage_run_id=self.stage_run_id,
                    node=self.node,
                    artifact_request_id=self.artifact_request_id,
                    parent_stage_run_id=self.parent_stage_run_id,
                    status="failed",
                    duration_ms=duration_ms,
                    error_type=exc_type.__name__,
                    error_message=Sanitizer.sanitize_error_message(str(exc_val)),
                    stack_frames=Sanitizer.sanitize_traceback(exc_tb),
                )
            )
        return False  # 关键——不吞异常


def get_collector(
    log_dir: Path | None = None,
    text_log_dir: Path | None = None,
) -> "RunLogCollector | NullCollector":
    """工厂——根据 TIANSHU_RUN_ID 环境变量返回对应采集器。

    Args:
        log_dir: JSONL 日志目录，默认 logs/monitor。
        text_log_dir: 人类可读文本日志目录，默认与 log_dir 相同。
    """
    run_id = os.environ.get("TIANSHU_RUN_ID", "").strip()
    if not run_id:
        return NullCollector()
    if log_dir is None:
        log_dir = Path("logs/monitor")
    if text_log_dir is None:
        text_log_dir = log_dir
    return RunLogCollector(log_dir, run_id, text_log_dir=text_log_dir)
