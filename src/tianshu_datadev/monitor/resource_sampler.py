"""后台线程资源采样器——定期采集进程树指标并写入监控日志。

Batch 6 —— ResourceSampler：独立的后台线程资源采样器，使用 psutil 采集进程树指标。
"""

import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

import psutil

from tianshu_datadev.monitor.models import ProcessMetrics, ResourceSample

if TYPE_CHECKING:
    from tianshu_datadev.monitor.collector import RunLogCollector

logger = logging.getLogger(__name__)

# 命令行截断长度
_CMDLINE_MAX_LEN = 200
# 敏感参数前缀列表——匹配后替换下一个参数值
_SENSITIVE_PARAM_PREFIXES = ("--password", "--token", "--key")


class ResourceSampler:
    """后台线程资源采样器——定期采集进程树指标并写入监控日志。"""

    def __init__(
        self,
        log_dir: Path,
        run_id: str,
        collector: "RunLogCollector",
        interval: float = 5.0,
    ):
        """
        Args:
            log_dir: 监控日志目录（用于调试输出）。
            run_id: 当前运行 ID。
            collector: RunLogCollector 实例（用于 log_resource_sample）。
            interval: 采样间隔（秒），默认 5.0。
        """
        self._log_dir = log_dir
        self.run_id = run_id
        self._collector = collector
        self._interval = interval

        # 内部状态
        self._active_stage_run_ids: set[str] = set()
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        # 整个 run 的峰值聚合
        self._peak_metrics: dict[str, float | int] = {
            "peak_observed_cpu_percent": 0.0,
            "peak_observed_rss_mb": 0.0,
            "peak_observed_vms_mb": 0.0,
            "peak_observed_num_processes": 0,
        }

    # ── 生命周期 ─────────────────────────────────────────────

    def start(self) -> None:
        """启动后台采样线程（daemon=True）。"""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("ResourceSampler 线程已在运行")
            return
        self._running.set()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name=f"resource-sampler-{self.run_id}",
        )
        self._thread.start()
        logger.info("ResourceSampler 已启动，间隔 %.1f 秒", self._interval)

    def stop(self) -> None:
        """停止后台线程并等待 join(timeout=10)。"""
        self._running.clear()
        if self._thread is not None:
            self._thread.join(timeout=10.0)
            if self._thread.is_alive():
                logger.warning("ResourceSampler 线程未在 10 秒内退出")
            self._thread = None
        # 写入最终峰值样本
        self._write_peak_sample()
        logger.info(
            "ResourceSampler 峰值摘要: cpu=%.1f%%, rss=%.1fMB, vms=%.1fMB, procs=%d",
            self._peak_metrics["peak_observed_cpu_percent"],
            self._peak_metrics["peak_observed_rss_mb"],
            self._peak_metrics["peak_observed_vms_mb"],
            self._peak_metrics["peak_observed_num_processes"],
        )

    # ── 线程安全接口 ─────────────────────────────────────────

    def set_active_stages(self, stage_run_ids: set[str]) -> None:
        """更新活跃阶段列表——线程安全。"""
        with self._lock:
            self._active_stage_run_ids = set(stage_run_ids)

    # ── 内部：后台线程主循环 ─────────────────────────────────

    def _run_loop(self) -> None:
        """后台线程主循环——定期采样。

        异常保护：整个 _sample() 调用包裹在 try/except 中，
        异常时 logging.warning 不崩溃。
        分段 sleep（每次 0.1s）以便及时响应 stop()。
        """
        while self._running.is_set():
            try:
                sample = self._sample()
                self._collector.log_resource_sample(sample)
            except Exception as exc:
                logger.warning("资源采样异常: %s", exc)
            # 分段 sleep——在 interval 期间仍可响应 running 变 False
            for _ in range(int(self._interval * 10)):
                if not self._running.is_set():
                    return
                time.sleep(0.1)

    # ── 核心：进程树采样 ─────────────────────────────────────

    def _sample(self) -> ResourceSample:
        """采集一次进程树指标。

        采集范围：
        - 当前 Python 进程（os.getpid()）及其子进程
        - 所有 Node.js 进程（node.exe / node）
        - 所有 Spark JVM 进程（java.exe / java，命令行含 spark-submit 或 SparkSubmit）

        Returns:
            ResourceSample 实例。
        """
        collected_pids: set[int] = set()
        process_metrics: list[ProcessMetrics] = []

        # ── 当前 Python 进程 + 子进程 ──
        try:
            current = psutil.Process()
            all_procs: list[psutil.Process] = [current]
            try:
                all_procs.extend(current.children(recursive=True))
            except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
                logger.warning("无法获取子进程列表: %s", exc)

            for proc in all_procs:
                pid = proc.pid
                if pid in collected_pids:
                    continue
                metrics = self._collect_process_metrics(proc)
                if metrics is not None:
                    process_metrics.append(metrics)
                    collected_pids.add(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            logger.warning("无法访问当前进程: %s", exc)

        # ── Node.js 和 Spark JVM 进程（全系统扫描） ──
        try:
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    pid = proc.info["pid"]
                    if pid in collected_pids:
                        continue
                    name = (proc.info["name"] or "").lower()
                    is_node = "node" in name
                    is_java = "java" in name
                    if not is_node and not is_java:
                        continue
                    # Java 进程仅保留 Spark 相关（命令行含 spark-submit 或 SparkSubmit）
                    if is_java:
                        raw_cmdline = proc.info["cmdline"]
                        cmdline_str = " ".join(raw_cmdline or [])
                        if "spark" not in cmdline_str.lower():
                            continue

                    p = psutil.Process(pid)
                    metrics = self._collect_process_metrics(p)
                    if metrics is not None:
                        process_metrics.append(metrics)
                        collected_pids.add(pid)

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception as exc:
                    logger.warning("采样进程异常 (pid=%s): %s", proc.info.get("pid"), exc)
        except Exception as exc:
            logger.warning("系统进程迭代异常: %s", exc)

        # ── 更新峰值聚合 ──
        num_processes = len(process_metrics)
        total_cpu = sum(p.cpu_percent for p in process_metrics)
        total_rss = sum(p.rss_mb for p in process_metrics)
        total_vms = sum(p.vms_mb for p in process_metrics)
        self._peak_metrics["peak_observed_cpu_percent"] = max(
            self._peak_metrics["peak_observed_cpu_percent"], total_cpu  # type: ignore[arg-type]
        )
        self._peak_metrics["peak_observed_rss_mb"] = max(
            self._peak_metrics["peak_observed_rss_mb"], total_rss  # type: ignore[arg-type]
        )
        self._peak_metrics["peak_observed_vms_mb"] = max(
            self._peak_metrics["peak_observed_vms_mb"], total_vms  # type: ignore[arg-type]
        )
        self._peak_metrics["peak_observed_num_processes"] = max(
            self._peak_metrics["peak_observed_num_processes"], num_processes  # type: ignore[arg-type]
        )

        return ResourceSample(
            run_id=self.run_id,
            processes=process_metrics,
        )

    # ── 辅助：单进程指标采集 ─────────────────────────────────

    def _collect_process_metrics(self, proc: psutil.Process) -> ProcessMetrics | None:
        """采集单个进程的指标。

        Args:
            proc: psutil.Process 实例。

        Returns:
            ProcessMetrics 实例；采集失败（权限不足、进程已退出）时返回 None。
        """
        try:
            cpu = proc.cpu_percent(interval=None)
            mem = proc.memory_info()
            p_name = proc.name() or ""
            num_threads = proc.num_threads()

            # 命令行处理（仅用于 debug 日志输出，模型不包含 cmdline 字段）
            try:
                raw_cmdline = proc.cmdline()
                sanitized = self._sanitize_cmdline(raw_cmdline or [])
                logger.debug("进程 pid=%d name=%s cmdline=%s", proc.pid, p_name, sanitized)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            return ProcessMetrics(
                pid=proc.pid,
                name=p_name,
                cpu_percent=cpu,
                rss_mb=mem.rss / (1024 * 1024),
                vms_mb=mem.vms / (1024 * 1024),
                num_threads=num_threads,
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            logger.warning("无法采集进程 pid=%d 指标: %s", proc.pid, exc)
            return None

    # ── 辅助：峰值样本写入 ───────────────────────────────────

    def _write_peak_sample(self) -> None:
        """写入峰值聚合样本到 collector。每个峰值指标作为独立 ProcessMetrics 条目。"""
        peak_processes: list[ProcessMetrics] = []
        for key, value in self._peak_metrics.items():
            metrics_kwargs: dict = {"pid": 0, "name": key}
            if key == "peak_observed_cpu_percent":
                metrics_kwargs.update(
                    cpu_percent=float(value), rss_mb=0.0, vms_mb=0.0, num_threads=0,
                )
            elif key == "peak_observed_rss_mb":
                metrics_kwargs.update(
                    cpu_percent=0.0, rss_mb=float(value), vms_mb=0.0, num_threads=0,
                )
            elif key == "peak_observed_vms_mb":
                metrics_kwargs.update(
                    cpu_percent=0.0, rss_mb=0.0, vms_mb=float(value), num_threads=0,
                )
            elif key == "peak_observed_num_processes":
                metrics_kwargs.update(
                    cpu_percent=0.0, rss_mb=0.0, vms_mb=0.0, num_threads=int(value),
                )
            peak_processes.append(ProcessMetrics(**metrics_kwargs))

        sample = ResourceSample(
            run_id=self.run_id,
            processes=peak_processes,
        )
        self._collector.log_resource_sample(sample)

    # ── 辅助：命令行清理 ─────────────────────────────────────

    @staticmethod
    def _sanitize_cmdline(cmdline: list[str]) -> str:
        """清理命令行字符串：过滤敏感参数值 + 超长截断。

        敏感参数过滤规则：
        - 匹配 --password、--token、--key（含 --xxx=value 和 --xxx value 两种形式）
        - 参数值替换为 "***"

        截断规则：
        - 拼接后长度 > 200 字符时截断为前 200 字符 + "..."

        Args:
            cmdline: 命令行参数列表（如 ["python", "script.py", "--password", "secret"]）。

        Returns:
            处理后的命令行字符串。
        """
        parts = list(cmdline)
        i = 0
        while i < len(parts):
            part = parts[i]
            # 匹配敏感参数前缀
            is_sensitive = any(
                part.startswith(prefix) for prefix in _SENSITIVE_PARAM_PREFIXES
            )
            if is_sensitive:
                if "=" in part:
                    # --password=secret123 → --password=***
                    key_part = part.split("=", 1)[0]
                    parts[i] = key_part + "=***"
                elif i + 1 < len(parts):
                    # --password secret123 → --password ***
                    parts[i + 1] = "***"
                    i += 1  # 跳过已替换的值
            i += 1

        cmdline_str = " ".join(parts)

        if len(cmdline_str) > _CMDLINE_MAX_LEN:
            cmdline_str = cmdline_str[:_CMDLINE_MAX_LEN] + "..."

        return cmdline_str
