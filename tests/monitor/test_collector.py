"""测试监控采集器——NullCollector、RunLogCollector、StageContext、get_collector。"""

import json
import os
import queue
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from tianshu_datadev.monitor import (
    NullCollector,
    RunLogCollector,
    StageContext,
    get_collector,
)
from tianshu_datadev.monitor.models import StageEvent


class TestGetCollector:
    """get_collector 工厂测试。"""

    def test_get_collector_returns_null_when_no_env_var(self):
        """无 TIANSHU_RUN_ID 时返回 NullCollector。"""
        with patch.dict(os.environ, {}, clear=True):
            collector = get_collector()
            assert isinstance(collector, NullCollector)

    def test_get_collector_returns_run_collector_when_env_var_set(self):
        """设置 TIANSHU_RUN_ID 后返回 RunLogCollector。"""
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test-run-001"}):
                collector = get_collector(Path(tmp))
                assert isinstance(collector, RunLogCollector)
                assert collector.run_id == "test-run-001"
                collector.close()


class TestNullCollector:
    """NullCollector 空操作行为测试。"""

    def test_null_collector_enabled_is_false(self):
        """NullCollector.enabled 为 False。"""
        collector = NullCollector()
        assert collector.enabled is False

    def test_null_collector_stage_is_noop(self):
        """NullCollector.stage() with 块正常通过无副作用。"""
        collector = NullCollector()
        with collector.stage("test-node", "req-001"):
            pass  # 不应抛异常
        # 再次验证——嵌套场景
        with collector.stage("node-a", "req-001", parent_stage_run_id="parent-001"):
            pass

    def test_null_collector_all_methods_noop(self):
        """所有方法为空操作，不抛异常。"""
        collector = NullCollector()
        from tianshu_datadev.monitor.models import (
            MonitorEvent,
            ProcessMetrics,
            ResourceSample,
        )

        class TestEvent(MonitorEvent):
            event_type: str = "test"

        collector.emit(TestEvent(run_id="", event_type="test"))
        collector.log_resource_sample(
            ResourceSample(
                run_id="",
                processes=[
                    ProcessMetrics(
                        pid=1, name="p", cpu_percent=0.0, rss_mb=0.0, vms_mb=0.0, num_threads=1
                    )
                ],
            )
        )
        collector.log_browser_event(
            {"run_id": "", "api_path": "/test", "api_status": 200}
        )
        assert collector.flush() is True
        collector.close()  # 不抛异常


def _read_events(tmp_path: Path, run_id: str) -> list[dict]:
    """从 JSONL 文件中读取事件列表。"""
    file_path = tmp_path / f"tianshu_run_{run_id}_events.jsonl"
    events = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def _create_collector(tmp_path: Path, run_id: str = "test-run", **kwargs):
    """创建 RunLogCollector 的辅助函数（已设置 env var）。"""
    with patch.dict(os.environ, {"TIANSHU_RUN_ID": run_id}):
        return RunLogCollector(tmp_path, run_id, **kwargs)


class TestRunLogCollector:
    """RunLogCollector 核心功能测试。"""

    def test_emit_writes_to_jsonl(self):
        """emit 后 JSONL 文件含正确事件行。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-emit")
            event = StageEvent(
                run_id="test-emit",
                stage_run_id="stage-001",
                node="extract",
                status="started",
            )
            collector.emit(event)
            collector.flush()
            collector.close()

            events = _read_events(Path(tmp), "test-emit")
            assert len(events) == 1
            assert events[0]["event_type"] == "stage"
            assert events[0]["stage_run_id"] == "stage-001"
            assert events[0]["node"] == "extract"
            assert events[0]["status"] == "started"

    def test_stage_context_writes_started_and_completed(self):
        """正常完成写 started+completed 两条。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-stage-ok")
            with collector.stage("transform", "req-001"):
                pass
            collector.flush()
            collector.close()

            events = _read_events(Path(tmp), "test-stage-ok")
            assert len(events) == 2
            assert events[0]["status"] == "started"
            assert events[0]["node"] == "transform"
            assert events[1]["status"] == "completed"
            assert events[1]["node"] == "transform"
            assert isinstance(events[1]["duration_ms"], int)

    def test_stage_context_writes_failed_on_exception(self):
        """异常写 failed + error_type。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-stage-fail")
            try:
                with collector.stage("load", "req-002"):
                    raise ValueError("数据校验失败")
            except ValueError:
                pass
            collector.flush()
            collector.close()

            events = _read_events(Path(tmp), "test-stage-fail")
            assert len(events) == 2
            assert events[0]["status"] == "started"
            assert events[1]["status"] == "failed"
            assert events[1]["error_type"] == "ValueError"
            assert "数据校验失败" in events[1]["error_message"]
            assert isinstance(events[1]["stack_frames"], list)
            assert len(events[1]["stack_frames"]) >= 1

    def test_stage_context_does_not_swallow_exception(self):
        """异常原样传播——__exit__ 返回 False。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-no-swallow")
            with pytest.raises(RuntimeError, match="预期异常"):
                with collector.stage("validate", "req-003"):
                    raise RuntimeError("预期异常")
            collector.flush()
            collector.close()

    def test_stage_context_set_result(self):
        """set_result 的 artifact_path/SHA-256 出现在 completed 事件中。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-set-result")
            with collector.stage("extract", "req-004") as ctx:
                ctx.set_result(
                    artifact_path="/data/output.parquet",
                    artifact_sha256="abc123def456",
                    row_count=12345,
                )
            collector.flush()
            collector.close()

            events = _read_events(Path(tmp), "test-set-result")
            assert len(events) == 2
            completed = events[1]
            assert completed["status"] == "completed"
            assert completed["artifact_path"] == "/data/output.parquet"
            assert completed["artifact_sha256"] == "abc123def456"
            assert completed["row_count"] == 12345

    def test_queue_full_drops_and_counts(self):
        """队列满时 dropped_event_count 递增。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-queue-full", max_queue=1)
            # 先等 writer 线程启动
            time.sleep(0.1)
            # 写 100 个事件——大部分应被丢弃
            for i in range(100):
                collector.emit(
                    StageEvent(
                        run_id="test-queue-full",
                        stage_run_id=f"stage-{i}",
                        node="test",
                        status="started",
                    )
                )
            # 等待 writer 处理完已有事件
            collector.flush(timeout=3.0)
            collector.close()

            assert collector.dropped_event_count > 0, (
                f"期待丢弃计数 > 0，实际 {collector.dropped_event_count}"
            )
            events = _read_events(Path(tmp), "test-queue-full")
            total_count = len(events) + collector.dropped_event_count
            assert total_count == 100, (
                f"写入 {len(events)} + 丢弃 {collector.dropped_event_count} = "
                f"{total_count}，应等于 100"
            )

    def test_flush_returns_true_when_queue_empty(self):
        """flush 排空返回 True。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-flush-true")
            collector.emit(
                StageEvent(
                    run_id="test-flush-true",
                    stage_run_id="stage-001",
                    node="test",
                    status="started",
                )
            )
            result = collector.flush(timeout=3.0)
            assert result is True
            assert collector.flush_completed is True
            collector.close()

    def test_flush_timeout_returns_false(self):
        """未排空超时返回 False。"""

        # 使用 writer 有延迟的子类模拟慢写入
        class SlowWriterCollector(RunLogCollector):
            """Writer 启动时延迟 0.5 秒，用于模拟慢队列消费。"""

            def _writer_loop(self):
                time.sleep(0.5)
                super()._writer_loop()

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test-flush-timeout"}):
                collector = SlowWriterCollector(Path(tmp), "test-flush-timeout", max_queue=1)
                # 让 writer 先开始 sleep
                time.sleep(0.05)
                # 发送一个事件——writer 在 sleep，队列非空
                collector.emit(
                    StageEvent(
                        run_id="test-flush-timeout",
                        stage_run_id="stage-001",
                        node="test",
                        status="started",
                    )
                )
                # 短超时 flush——writer 还在 sleep，队列未排空
                result = collector.flush(timeout=0.1)
                assert result is False, "短超时时应返回 False"
                # 正常关闭
                collector.close()

    def test_close_writes_collector_status_json(self):
        """close() 原子输出 *_collector_status.json。"""
        with tempfile.TemporaryDirectory() as tmp:
            collector = _create_collector(Path(tmp), "test-close-status")
            collector.emit(
                StageEvent(
                    run_id="test-close-status",
                    stage_run_id="stage-001",
                    node="test",
                    status="started",
                )
            )
            collector.flush()
            collector.close()

            status_path = Path(tmp) / "tianshu_run_test-close-status_collector_status.json"
            tmp_path = Path(tmp) / "tianshu_run_test-close-status_collector_status.tmp"

            # 验证状态文件存在且 tmp 文件不存在（原子重命名）
            assert status_path.is_file(), "状态文件应存在"
            assert not tmp_path.exists(), "临时文件应已被重命名"

            # 验证状态内容
            with open(status_path, "r", encoding="utf-8") as f:
                status = json.load(f)

            assert status["run_id"] == "test-close-status"
            assert status["run_complete"] is True
            assert status["flush_completed"] is True
            assert status["dropped_event_count"] == 0
            assert "closed_at" in status
            # 验证 closed_at 是 ISO 8601 格式（含时区）
            assert "+" in status["closed_at"] or status["closed_at"].endswith("Z")

    def test_run_id_never_generated_internally(self):
        """run_id 永远从 env var 读取，不自生成。"""
        # case 1: 设置环境变量——从 env 读取
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "from-env-var"}):
                collector = get_collector(Path(tmp))
                assert isinstance(collector, RunLogCollector)
                assert collector.run_id == "from-env-var"
                collector.close()

        # case 2: 未设置环境变量——返回 NullCollector
        with patch.dict(os.environ, {}, clear=True):
            collector = get_collector()
            assert isinstance(collector, NullCollector)

        # case 3: 空字符串视为未设置
        with patch.dict(os.environ, {"TIANSHU_RUN_ID": ""}):
            collector = get_collector()
            assert isinstance(collector, NullCollector)

        # case 4: 空白字符串视为未设置
        with patch.dict(os.environ, {"TIANSHU_RUN_ID": "  "}):
            collector = get_collector()
            assert isinstance(collector, NullCollector)
