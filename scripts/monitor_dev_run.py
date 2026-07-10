"""监控模式统一启动脚本——生成 run_id → 轮转 → 启动采样器 → 启动前后端 → 优雅退出。

用法:
    python scripts/monitor_dev_run.py          # 全量启动（监控模式）

行为序列（严格按顺序）:
    1. 生成 run_id = YYYYMMDD-HHMMSS
    2. 生成 monitor_token = secrets.token_hex(16)
    3. 创建 logs/monitor/ 目录
    4. 设置环境变量 TIANSHU_RUN_ID 和 TIANSHU_MONITOR_TOKEN
    5. 调用 rotation.cleanup(log_dir, run_id) ← 启动时轮转
    6. 创建 collector = get_collector(log_dir)
    7. 创建 ResourceSampler(log_dir, run_id, collector, interval=5.0)
    8. 调用 sampler.start()
    9. 启动 Backend subprocess
    10. 启动 Frontend subprocess
    11. 健康检查轮询（超时 30s）
    12. 打印启动信息
    13. 注册信号处理器，等待退出信号
    14. 优雅关闭：终止前端 → 终止后端 → 停止采样器 → 生成 summary.json → 退出时轮转
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import TextIO

from tianshu_datadev.monitor.collector import RunLogCollector, get_collector
from tianshu_datadev.monitor.resource_sampler import ResourceSampler
from tianshu_datadev.monitor.rotation import cleanup

# 项目根目录——脚本在 scripts/ 下，父目录即为项目根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR = _PROJECT_ROOT / "logs" / "monitor"

# 前端目录
_FRONTEND_DIR = _PROJECT_ROOT / "frontend"

# 后端健康检查 URL
_BACKEND_HEALTH_URL = "http://127.0.0.1:8000/api/health"
# 前端健康检查 URL
_FRONTEND_HEALTH_URL = "http://127.0.0.1:5173/"

# 健康检查超时（秒）
_HEALTH_TIMEOUT = 30.0
# 健康检查重试间隔（秒）
_HEALTH_INTERVAL = 1.0

logger = logging.getLogger(__name__)


def _generate_run_id() -> str:
    """生成 run_id = YYYYMMDD-HHMMSS，碰撞时追加 _{random_hex_4}。

    碰撞检测：检查 logs/monitor/ 下是否存在同名文件前缀。
    这是线程安全的——同一台机器上几乎不可能在 1 秒内启动两次。

    Returns:
        唯一且可排序的 run_id 字符串。
    """
    now = datetime.now()
    base = now.strftime("%Y%m%d-%H%M%S")
    run_id = base

    # 碰撞检测——检查是否有同前缀的日志文件
    if _LOG_DIR.is_dir():
        for fpath in _LOG_DIR.iterdir():
            if fpath.name.startswith(f"tianshu_run_{run_id}"):
                # 追加 4 位随机 hex
                run_id = f"{base}_{secrets.token_hex(2)}"
                break
    return run_id


def _setup_logging(log_dir: Path, run_id: str) -> logging.Logger:
    """配置日志输出——同时写入文件和 stderr。

    文件路径: logs/monitor/tianshu_run_{run_id}_script.log
    stderr 输出 INFO 级别，文件输出 DEBUG 级别。

    Args:
        log_dir: 日志目录路径。
        run_id: 当前运行 ID（用于日志文件名）。

    Returns:
        配置完成的 logger 实例。
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"tianshu_run_{run_id}_script.log"

    # 文件 handler——DEBUG 级别
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="w")
    fh.setLevel(logging.DEBUG)

    # stderr handler——INFO 级别
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)

    # 格式
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    # 配置根 logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    # 清除已有 handlers（避免重复添加）
    root_logger.handlers.clear()
    root_logger.addHandler(fh)
    root_logger.addHandler(sh)

    return logging.getLogger(__name__)


def _create_log_dir() -> Path:
    """创建监控日志目录。

    Returns:
        日志目录 Path 实例。
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    return _LOG_DIR


def _open_log_file(run_id: str, suffix: str) -> tuple[ Path, TextIO ]:
    """打开日志文件。

    Args:
        run_id: 当前 run_id。
        suffix: 日志文件后缀（如 backend、frontend）。

    Returns:
        (文件路径, 文件对象) 元组。
    """
    path = _LOG_DIR / f"tianshu_run_{run_id}_{suffix}.log"
    fh = open(path, "w", encoding="utf-8")
    return path, fh


def _health_check_backend(timeout: float = _HEALTH_TIMEOUT) -> bool:
    """轮询后端健康检查直到通过或超时。

    GET /api/health → 期望 {"status": "ok"}

    Args:
        timeout: 超时秒数。

    Returns:
        是否在超时前通过检查。
    """
    import json as _json
    import urllib.request as urllib_req

    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        try:
            req = urllib_req.Request(_BACKEND_HEALTH_URL)
            with urllib_req.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    body = resp.read().decode("utf-8", errors="replace")
                    data = _json.loads(body)
                    if data.get("status") == "ok":
                        return True
                    last_error = f"状态非 ok: {body[:100]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(_HEALTH_INTERVAL)
    logger.error("后端健康检查超时（%s 秒），最后错误: %s", timeout, last_error[:200])
    return False


def _health_check_frontend(timeout: float = _HEALTH_TIMEOUT) -> bool:
    """轮询前端健康检查直到通过或超时。

    GET / → 期望 HTTP 200

    Args:
        timeout: 超时秒数。

    Returns:
        是否在超时前通过检查。
    """
    import urllib.request as urllib_req

    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        try:
            req = urllib_req.Request(_FRONTEND_HEALTH_URL)
            with urllib_req.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    return True
                last_error = f"状态码: {resp.status}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(_HEALTH_INTERVAL)
    logger.error("前端健康检查超时（%s 秒），最后错误: %s", timeout, last_error[:200])
    return False


def _write_summary(
    log_dir: Path,
    run_id: str,
    collector: "RunLogCollector",
    backend_exit_code: int | None,
    frontend_exit_code: int | None,
) -> Path:
    """写入 summary.json 文件。

    Args:
        log_dir: 日志目录。
        run_id: 当前运行 ID。
        collector: RunLogCollector 实例。
        backend_exit_code: 后端进程退出码。
        frontend_exit_code: 前端进程退出码。

    Returns:
        summary.json 路径。
    """
    summary = {
        "run_id": run_id,
        "dropped_event_count": collector.dropped_event_count,
        "flush_completed": collector.flush_completed,
        "run_complete": collector.run_complete,
        "backend_exit_code": backend_exit_code,
        "frontend_exit_code": frontend_exit_code,
        "monitor_version": "3.0",
        "finished_at": datetime.now().isoformat(),
    }
    summary_path = log_dir / f"tianshu_run_{run_id}_summary.json"
    # 原子写入——先写 tmp 再 rename
    tmp_path = log_dir / f"tianshu_run_{run_id}_summary.tmp"
    tmp_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.rename(summary_path)
    return summary_path


def _check_ports_available() -> list[str]:
    """检查 8000 和 5173 端口是否可用。

    使用 urllib 快速探测——端口忙碌时给出清晰提示。

    Returns:
        被占用的端口名列表（如 ["backend (8000)"]）。
    """
    import urllib.error as urllib_err
    import urllib.request as urllib_req

    busy: list[str] = []
    for port, name, url in [
        (8000, "backend (8000)", "http://127.0.0.1:8000/api/health"),
        (5173, "frontend (5173)", "http://127.0.0.1:5173/"),
    ]:
        try:
            req = urllib_req.Request(url)
            with urllib_req.urlopen(req, timeout=1):
                busy.append(f"{name} 端口 {port}")
        except urllib_err.HTTPError:
            busy.append(f"{name} 端口 {port}")  # HTTP响应=端口已占用
        except urllib_err.URLError:
            pass  # 端口空闲（连接被拒绝）
        except OSError:
            busy.append(f"{name} 端口 {port}")
        except Exception:
            pass  # 超时或无响应——端口可能被占用也可能未就绪
    return busy


def main() -> None:
    """监控模式启动入口——生成 run_id → 轮转 → 启动采样器 → 启动前后端 → 优雅退出。"""
    # ── 强制 UTF-8 输出 ──
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    # ── 端口预检查 ──
    busy_ports = _check_ports_available()
    if busy_ports:
        msg = (
            f"端口被占用: {', '.join(busy_ports)}。\n"
            f"建议运行 ./dev-reload.sh 清理旧进程后重试。"
        )
        print(f"[错误] {msg}", file=sys.stderr)
        sys.exit(1)

    # ════════ 阶段 1-2：生成 run_id + token ════════
    run_id = _generate_run_id()
    monitor_token = secrets.token_hex(16)

    # ════════ 阶段 3：创建日志目录 ════════
    log_dir = _create_log_dir()

    # ════════ 配置日志 ════════
    _setup_logging(log_dir, run_id)
    logger.info("监控运行启动——run_id=%s", run_id)

    # ════════ 阶段 4-5：设置环境变量 + 启动时轮转 ════════
    os.environ["TIANSHU_RUN_ID"] = run_id
    os.environ["TIANSHU_MONITOR_TOKEN"] = monitor_token

    cleanup(log_dir, run_id)
    logger.info("启动时轮转完成")

    # ════════ 阶段 6-8：创建 collector + sampler ════════
    collector = get_collector(log_dir)
    if not isinstance(collector, RunLogCollector):
        logger.error("get_collector 返回了 NullCollector——TIANSHU_RUN_ID 未正确设置")
        sys.exit(1)

    sampler = ResourceSampler(log_dir, run_id, collector, interval=5.0)
    sampler.start()
    logger.info("ResourceSampler 已启动")

    # ════════ 阶段 9：启动后端 ════════
    backend_log_path, backend_log_fp = _open_log_file(run_id, "backend")
    backend_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "tianshu_datadev.api.app:create_app",
         "--factory",
         "--host", "127.0.0.1", "--port", "8000"],
        stdout=backend_log_fp,
        stderr=subprocess.STDOUT,
    )
    backend_log_fp.close()
    logger.info("后端已启动——PID %d，日志 → %s", backend_proc.pid, backend_log_path)

    # ════════ 阶段 10：启动前端 ════════
    frontend_log_path, frontend_log_fp = _open_log_file(run_id, "frontend")
    # Windows 上 npx 是 npx.cmd 批处理文件，需要 shell=True
    # Unix 上 shell=True 不影响——npx 本身不需要 shell
    frontend_proc = subprocess.Popen(
        ["npx", "vite", "--host", "127.0.0.1", "--port", "5173"],
        cwd=str(_FRONTEND_DIR),
        stdout=frontend_log_fp,
        stderr=subprocess.STDOUT,
        shell=True,
    )
    frontend_log_fp.close()
    logger.info("前端已启动——PID %d，日志 → %s", frontend_proc.pid, frontend_log_path)

    # ════════ 阶段 11-12：健康检查 + 打印启动信息 ════════
    backend_ok = _health_check_backend()
    frontend_ok = _health_check_frontend()

    if not backend_ok:
        logger.error("后端健康检查失败——请检查日志")
        # 清理已启动的资源
        frontend_proc.terminate()
        backend_proc.terminate()
        sampler.stop()
        sys.exit(1)

    if not frontend_ok:
        logger.error("前端健康检查失败——请检查日志")
        frontend_proc.terminate()
        backend_proc.terminate()
        sampler.stop()
        sys.exit(1)

    logger.info("=" * 50)
    logger.info("监控运行已就绪")
    logger.info("  run_id:    %s", run_id)
    logger.info("  后端:      http://127.0.0.1:8000")
    logger.info("  前端:      http://127.0.0.1:5173")
    logger.info("  日志目录:  %s", log_dir)
    logger.info("=" * 50)

    # ════════ 阶段 13：等待退出信号 ════════
    shutdown_event = threading.Event()

    def _signal_handler(signum: int, frame) -> None:
        """信号处理器——设置 shutdown 事件，等待优雅退出。"""
        signame = signal.strsignal(signum) if hasattr(signal, "strsignal") else str(signum)
        logger.info("收到信号 %s，开始优雅关闭...", signame)
        shutdown_event.set()

    # 跨平台信号注册
    signal.signal(signal.SIGINT, _signal_handler)
    # SIGTERM 在 Windows 上不可用
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)
    # Windows: SIGBREAK (Ctrl+Break)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _signal_handler)

    # 等待 shutdown 信号或子进程退出
    try:
        # 轮询子进程和 shutdown_event
        while not shutdown_event.is_set():
            # 检查子进程是否已退出
            backend_ret = backend_proc.poll()
            frontend_ret = frontend_proc.poll()
            if backend_ret is not None or frontend_ret is not None:
                logger.warning(
                    "子进程提前退出——backend=%s, frontend=%s",
                    backend_ret, frontend_ret,
                )
                break
            shutdown_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        # 兜底：即使 signal handler 未触发也处理 Ctrl+C
        logger.info("捕获 KeyboardInterrupt，开始优雅关闭...")
        shutdown_event.set()

    # ════════ 阶段 14-16：优雅关闭 ════════
    logger.info("正在终止前端（PID %d）...", frontend_proc.pid)
    frontend_proc.terminate()
    try:
        frontend_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logger.warning("前端未在 5 秒内退出，强制终止")
        frontend_proc.kill()
        frontend_proc.wait(timeout=2)

    logger.info("正在终止后端（PID %d）...", backend_proc.pid)
    backend_proc.terminate()
    try:
        backend_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logger.warning("后端未在 10 秒内退出，强制终止")
        backend_proc.kill()
        backend_proc.wait(timeout=2)

    # 停止资源采样器
    sampler.stop()
    logger.info("ResourceSampler 已停止")

    # 收集退出码
    backend_exit_code = backend_proc.returncode
    frontend_exit_code = frontend_proc.returncode

    # 写入 summary.json
    summary_path = _write_summary(
        log_dir, run_id, collector,
        backend_exit_code, frontend_exit_code,
    )
    logger.info("summary 已写入 → %s", summary_path)

    # 退出时轮转
    cleanup(log_dir, run_id)
    logger.info("退出时轮转完成")

    logger.info(
        "监控运行结束——run_id=%s，日志保存在 logs/monitor/",
        run_id,
    )


if __name__ == "__main__":
    main()
