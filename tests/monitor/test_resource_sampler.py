"""测试 ResourceSampler——后台进程树资源采样器。

Batch 6 —— 独立进程树资源采样器 ResourceSampler 的单元测试。
使用 MockCollector 避免文件 IO。
"""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# psutil 是 monitor 可选依赖（pyproject.toml [project.optional-dependencies] monitor）
# 若未安装，整个测试模块静默跳过——ResourceSampler 当前不提供无 psutil 降级路径
psutil = pytest.importorskip("psutil", reason="psutil 未安装——执行 `pip install .[monitor]` 启用资源采样测试")

from tianshu_datadev.monitor.models import ResourceSample  # noqa: E402
from tianshu_datadev.monitor.resource_sampler import ResourceSampler  # noqa: E402

# ═══════════════════════════════════════════════════════════
# 辅助：MockCollector——轻量替代 RunLogCollector
# ═══════════════════════════════════════════════════════════


class MockCollector:
    """轻量测试替身——记录 log_resource_sample 调用。"""

    def __init__(self):
        self.samples: list[ResourceSample] = []
        self.enabled = True
        self.run_id = "test-run-1"

    def log_resource_sample(self, sample: ResourceSample) -> None:
        """记录 ResourceSample 供断言。"""
        self.samples.append(sample)

    def emit(self, event) -> None:
        pass

    def stage(self, node: str, artifact_request_id: str, parent_stage_run_id: str | None = None):
        """返回空的 stage 上下文管理器。"""
        _ctx = MagicMock()
        _ctx.__enter__ = MagicMock(return_value=_ctx)
        _ctx.__exit__ = MagicMock(return_value=False)
        return _ctx

    def log_browser_event(self, payload: dict) -> None:
        pass

    def flush(self, timeout: float = 5.0) -> bool:
        return True

    def close(self) -> None:
        pass


# ═══════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════


class TestResourceSampler:
    """ResourceSampler 后台进程树资源采样器测试。"""

    # ── 辅助 ──────────────────────────────────────────────

    @staticmethod
    def _make_sampler(
        collector=None,
        run_id: str = "test-run-1",
        interval: float = 5.0,
    ) -> ResourceSampler:
        """创建带有默认参数的 ResourceSampler 实例。"""
        if collector is None:
            collector = MockCollector()
        return ResourceSampler(
            log_dir=Path("/tmp"),
            run_id=run_id,
            collector=collector,
            interval=interval,
        )

    @staticmethod
    def _make_fake_process(
        pid: int = 12345,
        name: str = "python.exe",
        cpu: float = 10.0,
        rss_bytes: int = 100 * 1024 * 1024,
        vms_bytes: int = 200 * 1024 * 1024,
        num_threads: int = 4,
        cmdline: list[str] | None = None,
    ) -> MagicMock:
        """创建模拟的 psutil.Process 实例。"""
        mock_proc = MagicMock(spec=psutil.Process)
        mock_proc.pid = pid
        mock_proc.name.return_value = name
        mock_proc.cpu_percent.return_value = cpu
        mock_mem = MagicMock()
        mock_mem.rss = rss_bytes
        mock_mem.vms = vms_bytes
        mock_proc.memory_info.return_value = mock_mem
        mock_proc.num_threads.return_value = num_threads
        mock_proc.cmdline.return_value = cmdline or ["python", "script.py"]
        return mock_proc

    # ── Test 1：基本采样 ──────────────────────────────────

    def test_sample_collects_python_processes(self):
        """验证 _sample() 返回的 ResourceSample 包含至少一个 Python 进程。

        使用真实 psutil 调用——当前测试进程本身就是 Python 进程。
        """
        sampler = self._make_sampler()
        sample = sampler._sample()

        assert isinstance(sample, ResourceSample)
        assert len(sample.processes) > 0, "采样应返回至少一个进程"

        # 至少有一个 Python 进程（当前测试进程本身）
        python_procs = [p for p in sample.processes if "python" in p.name.lower()]
        assert len(python_procs) >= 1, (
            f"应包含至少一个 Python 进程，实际进程名: {[p.name for p in sample.processes]}"
        )

        # 验证 ProcessMetrics 字段完整性
        for proc in sample.processes:
            assert isinstance(proc.pid, int)
            assert isinstance(proc.name, str) and proc.name
            assert isinstance(proc.cpu_percent, float)
            assert isinstance(proc.rss_mb, float) and proc.rss_mb >= 0
            assert isinstance(proc.vms_mb, float) and proc.vms_mb >= 0
            assert isinstance(proc.num_threads, int) and proc.num_threads > 0

    # ── Test 2：活跃阶段设置 ──────────────────────────────

    def test_set_active_stages_updates_internal_state(self):
        """验证 set_active_stages 正确更新采样器内部活跃阶段状态。"""
        collector = MockCollector()
        sampler = self._make_sampler(collector=collector)

        # 设置活跃阶段
        sampler.set_active_stages({"stage_1", "stage_2"})

        # 验证采样器内部状态已更新
        with sampler._lock:
            assert sampler._active_stage_run_ids == {"stage_1", "stage_2"}
            assert sampler._observed_stages == {"stage_1", "stage_2"}

        # 第二次设置应正确替换并累积
        sampler.set_active_stages({"stage_3", "stage_4"})
        with sampler._lock:
            assert sampler._active_stage_run_ids == {"stage_3", "stage_4"}
            assert sampler._observed_stages == {"stage_1", "stage_2", "stage_3", "stage_4"}

        # 验证 _sample() 可以读取活跃阶段（不崩溃）
        sample = sampler._sample()
        assert isinstance(sample, ResourceSample)
        assert len(sample.processes) > 0

    # ── Test 3：命令行截断 ────────────────────────────────

    def test_cmdline_truncated_to_200_chars(self):
        """命令行 >200 字符时截断。"""
        # 构造长命令行（超过 200 字符）
        long_cmdline = ["python", "script.py"] + ["--verbose"] * 30
        raw = " ".join(long_cmdline)
        assert len(raw) > 200, "测试用例应超过 200 字符"

        result = ResourceSampler._sanitize_cmdline(long_cmdline)

        # 结果 = 200 字符 + "..."（3 字符）
        assert len(result) == 203, f"截断后长度应为 203，实际为 {len(result)}: {result!r}"
        assert result.endswith("..."), "截断应追加 '...'"
        # 前 200 字符应与原始字符串前 200 字符一致
        assert result[:200] == raw[:200], "截断应保留前 200 字符"

    # ── Test 4：敏感参数过滤 ──────────────────────────────

    @pytest.mark.parametrize(
        "cmdline, expected_part",
        [
            (["node", "app.js", "--password", "secret123"], "--password ***"),
            (["java", "-jar", "app.jar", "--token=abc123"], "--token=***"),
            (["python", "script.py", "--key", "mykey"], "--key ***"),
            (["cmd", "--password", "p1", "--token", "t1", "--key", "k1"],
             "--password *** --token *** --key ***"),
            (["cmd", "--password=secret"], "--password=***"),
        ],
    )
    def test_cmdline_filters_password_token_key(self, cmdline, expected_part):
        """过滤 --password/--token/--key 参数值。"""
        result = ResourceSampler._sanitize_cmdline(cmdline)
        assert "***" in result, f"应包含 '***': {result}"
        assert "secret123" not in result, "密码值不应明文出现"
        assert "abc123" not in result, "token 值不应明文出现"
        assert "mykey" not in result, "key 值不应明文出现"
        assert "p1" not in result, "密码值不应明文出现"
        assert expected_part in result, f"应包含 {expected_part!r}: {result}"

    # ── Test 5：异常容错 ──────────────────────────────────

    def test_sample_failure_does_not_throw(self):
        """psutil 异常时返回空 ResourceSample，不抛异常。"""
        collector = MockCollector()
        sampler = self._make_sampler(collector=collector)

        # Mock：psutil.Process() 抛出 NoSuchProcess
        with patch("psutil.Process", side_effect=psutil.NoSuchProcess(99999)):
            with patch("psutil.process_iter", return_value=[]):
                # 不应抛出任何异常
                sample = sampler._sample()

        assert isinstance(sample, ResourceSample)
        assert len(sample.processes) == 0, "异常时应返回空进程列表"
        assert sample.run_id == "test-run-1"

    # ── Test 6：峰值命名 ──────────────────────────────────

    def test_metrics_named_peak_observed(self):
        """start/stop 周期后验证 peak_observed_* 命名。"""
        collector = MockCollector()
        sampler = self._make_sampler(collector=collector, interval=0.1)

        # 先采集一个正常样本，积累一些峰值
        sampler._sample()

        # 启动后台线程
        sampler.start()
        # 给线程一个机会采样
        time.sleep(0.3)
        # 停止——触发峰值样本写入
        sampler.stop()

        # 查找最后一次样本（峰值样本应该是最后一次）
        assert len(collector.samples) >= 1, "collector 应收到至少一个样本"

        # 峰值样本是最后一次写入的
        peak_sample = collector.samples[-1]

        # 验证 peak_sample 中包含 peak_observed_* 命名的条目
        peak_names = [p.name for p in peak_sample.processes]
        expected_peaks = {
            "peak_observed_cpu_percent",
            "peak_observed_rss_mb",
            "peak_observed_vms_mb",
            "peak_observed_num_processes",
        }
        for name in expected_peaks:
            assert name in peak_names, (
                f"峰值样本应包含 {name}，实际峰值: {peak_names}"
            )

        # 验证峰值条目的 pid 均为 0（合成条目）
        for p in peak_sample.processes:
            assert p.pid == 0, f"峰值条目 pid 应为 0，实际 {p.pid}"

    # ── Test 7：峰值聚合 ──────────────────────────────────

    def test_peak_aggregation_across_run(self):
        """整个 run 的峰值聚合正确。"""
        collector = MockCollector()
        sampler = self._make_sampler(collector=collector)

        # 第一轮：低指标
        low_proc = self._make_fake_process(
            pid=1001, name="python.exe",
            cpu=10.0, rss_bytes=100 * 1024 * 1024, vms_bytes=200 * 1024 * 1024,
            num_threads=4,
        )
        with patch("psutil.Process", return_value=low_proc):
            with patch("psutil.process_iter", return_value=[]):
                sampler._sample()

        assert sampler._peak_metrics["peak_observed_cpu_percent"] == 10.0
        assert sampler._peak_metrics["peak_observed_rss_mb"] == pytest.approx(100.0)
        assert sampler._peak_metrics["peak_observed_vms_mb"] == pytest.approx(200.0)
        assert sampler._peak_metrics["peak_observed_num_processes"] == 1

        # 第二轮：高指标（应保留峰值）
        # 注：使用不同 PID 绕过 _proc_cache——缓存会复用首轮的 mock Process 对象，
        # 导致 cpu_percent 返回缓存的旧值而非新 mock 的值
        high_proc = self._make_fake_process(
            pid=2001, name="python.exe",
            cpu=50.0, rss_bytes=300 * 1024 * 1024, vms_bytes=500 * 1024 * 1024,
            num_threads=8,
        )
        with patch("psutil.Process", return_value=high_proc):
            with patch("psutil.process_iter", return_value=[]):
                sampler._sample()

        # 峰值应取最大值
        assert sampler._peak_metrics["peak_observed_cpu_percent"] == 50.0, (
            f"CPU 峰值应为 50.0，实际 {sampler._peak_metrics['peak_observed_cpu_percent']}"
        )
        assert sampler._peak_metrics["peak_observed_rss_mb"] == pytest.approx(300.0), (
            f"RSS 峰值应为 300，实际 {sampler._peak_metrics['peak_observed_rss_mb']}"
        )
        assert sampler._peak_metrics["peak_observed_vms_mb"] == pytest.approx(500.0), (
            f"VMS 峰值应为 500，实际 {sampler._peak_metrics['peak_observed_vms_mb']}"
        )
        assert sampler._peak_metrics["peak_observed_num_processes"] == 1, (
            f"进程数峰值应为 1，实际 {sampler._peak_metrics['peak_observed_num_processes']}"
        )

        # 第三轮：低指标（峰值不应降低）
        low_proc2 = self._make_fake_process(
            pid=3001, name="python.exe",
            cpu=5.0, rss_bytes=50 * 1024 * 1024, vms_bytes=100 * 1024 * 1024,
            num_threads=2,
        )
        with patch("psutil.Process", return_value=low_proc2):
            with patch("psutil.process_iter", return_value=[]):
                sampler._sample()

        # 峰值仍应保持最大值
        assert sampler._peak_metrics["peak_observed_cpu_percent"] == 50.0
        assert sampler._peak_metrics["peak_observed_rss_mb"] == pytest.approx(300.0)
        assert sampler._peak_metrics["peak_observed_vms_mb"] == pytest.approx(500.0)

        # 停止——验证峰值样本写入
        sampler.stop()

        # 最后一个样本应是峰值样本
        peak_sample = collector.samples[-1]
        peak_map = {p.name: p for p in peak_sample.processes}

        assert "peak_observed_cpu_percent" in peak_map
        assert peak_map["peak_observed_cpu_percent"].cpu_percent == 50.0

        assert "peak_observed_rss_mb" in peak_map
        assert peak_map["peak_observed_rss_mb"].rss_mb == pytest.approx(300.0)

        assert "peak_observed_vms_mb" in peak_map
        assert peak_map["peak_observed_vms_mb"].vms_mb == pytest.approx(500.0)

        assert "peak_observed_num_processes" in peak_map
        assert peak_map["peak_observed_num_processes"].num_threads == 1

    # ── Test 8：线程生命周期 ───────────────────────────────

    def test_start_stop_lifecycle(self):
        """start/stop 线程生命周期正确。"""
        collector = MockCollector()
        sampler = self._make_sampler(collector=collector, interval=0.1)

        # 初始状态：线程不存在
        assert sampler._thread is None

        # start() 后：线程存在且存活
        sampler.start()
        assert sampler._thread is not None
        assert sampler._thread.is_alive(), "采样线程应存活"
        assert sampler._thread.daemon is True, "采样线程应是 daemon 线程"
        assert f"resource-sampler-{sampler.run_id}" in sampler._thread.name, (
            f"线程名应包含 {sampler.run_id}"
        )

        # 重复 start() 不应创建新线程
        thread_id = id(sampler._thread)
        sampler.start()  # 应输出 warning，不创建新线程
        assert id(sampler._thread) == thread_id, "重复 start() 不应创建新线程"

        # stop() 后：线程停止
        sampler.stop()
        assert sampler._thread is None, "stop() 后 _thread 应被置为 None"

        # 连续 stop() 不崩溃
        sampler.stop()  # 应不报错

        # 第二次 start/stop 周期应正常
        sampler.start()
        assert sampler._thread is not None
        assert sampler._thread.is_alive()
        sampler.stop()
        assert sampler._thread is None
