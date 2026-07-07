"""dev-reload——git pull 后自动重启前后端开发服务器。

用法：
    python scripts/dev_reload.py              # 全量——清缓存 + 重启前后端
    python scripts/dev_reload.py --backend    # 仅后端
    python scripts/dev_reload.py --frontend   # 仅前端
    python scripts/dev_reload.py --no-kill    # 跳过终止，仅清理缓存 + 补启动缺失服务

入口脚本 dev-reload.sh 等效于：
    cd "$(dirname "$0")" && python scripts/dev_reload.py "$@"
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def clean_pyc(project_root: Path) -> int:
    """清理全项目 __pycache__ 目录和 *.pyc 文件。

    返回删除的目录/文件总数。
    """
    count = 0
    # 删除 __pycache__ 目录树
    for d in project_root.rglob("__pycache__"):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
            count += 1
    # 删除根级 *.pyc（不在 __pycache__ 内的）
    for f in project_root.rglob("*.pyc"):
        try:
            f.unlink()
            count += 1
        except OSError:
            pass
    return count


def parse_netstat_listeners(port: int) -> list[int]:
    """解析 netstat -ano 输出，返回指定端口上所有 LISTENING 状态的 PID。

    Args:
        port: 目标端口号

    Returns:
        PID 列表（去重排序）
    """
    try:
        out = subprocess.check_output(
            "netstat -ano", shell=True, text=True, timeout=5
        )
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return []

    pids: list[int] = []
    port_str = f":{port}"
    for line in out.splitlines():
        if port_str not in line or "LISTENING" not in line:
            continue
        parts = line.strip().split()
        if not parts:
            continue
        try:
            pid = int(parts[-1])
            if pid > 0 and pid not in pids:
                pids.append(pid)
        except ValueError:
            pass
    return sorted(pids)


def get_process_command_line(pid: int) -> str:
    """通过 wmic 获取 Windows 进程的完整命令行。

    Args:
        pid: 进程 ID

    Returns:
        命令行字符串；失败返回空字符串
    """
    try:
        out = subprocess.check_output(
            f"wmic process where ProcessId={pid} get CommandLine /format:list",
            shell=True, text=True, timeout=5,
        )
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("CommandLine="):
                return line.split("=", 1)[1].strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
        pass
    return ""


def check_whitelist(pid: int, port: int, project_root: Path) -> tuple[bool, str]:
    """检查端口上的 PID 是否在可终止白名单内。

    白名单规则：
    - 端口 8000：命令行含 "uvicorn" 且含 "tianshu_datadev"
    - 端口 5173：命令行含 "vite"/"npm"/"node" 且路径在项目 frontend/ 目录下
    - 其他端口：一律拒绝

    Args:
        pid: 进程 ID
        port: 监听端口
        project_root: 项目根目录

    Returns:
        (allowed, reason)——allowed=False 时 reason 含拒绝原因
    """
    cmdline = get_process_command_line(pid)
    if not cmdline:
        return False, (
            f"PID {pid} 命令行无法获取（可能已退出或权限不足）"
        )

    # 标准化路径分隔符（wmic 可能返回不同格式）
    cmdline_normalized = cmdline.replace("\\", "/").lower()
    frontend_dir = str(project_root / "frontend").replace("\\", "/").lower()

    if port == 8000:
        if "uvicorn" in cmdline_normalized and "tianshu_datadev" in cmdline_normalized:
            return True, ""
        return False, (
            f"端口 8000 PID {pid} 非本项目进程："
            f"命令行 = {cmdline[:200]}"
        )

    if port == 5173:
        is_node_tool = any(
            kw in cmdline_normalized for kw in ["vite", "npm", "node"]
        )
        in_project = frontend_dir in cmdline_normalized
        if is_node_tool and in_project:
            return True, ""
        return False, (
            f"端口 5173 PID {pid} 非本项目前端进程（"
            f"node_tool={is_node_tool}, in_project={in_project}）："
            f"命令行 = {cmdline[:200]}"
        )

    return False, f"未配置端口 {port} 的白名单规则"


def kill_process(pid: int) -> bool:
    """通过 taskkill /F 终止 Windows 进程。

    Args:
        pid: 进程 ID

    Returns:
        是否成功终止（taskkill 返回码为 0）
    """
    try:
        result = subprocess.run(
            f"taskkill /F /PID {pid}",
            shell=True, capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def stop_service(port: int, project_root: Path) -> tuple[int, list[str]]:
    """停止指定端口上的本项目 dev 进程。

    流程：netstat → 获取 PID → 白名单检查 → taskkill。

    Args:
        port: 目标端口
        project_root: 项目根目录

    Returns:
        (killed_count, errors)——errors 非空时调用方应终止脚本
    """
    errors: list[str] = []
    killed = 0

    pids = parse_netstat_listeners(port)
    if not pids:
        return 0, []

    for pid in pids:
        allowed, reason = check_whitelist(pid, port, project_root)
        if not allowed:
            errors.append(reason)
            continue
        if kill_process(pid):
            killed += 1
        else:
            errors.append(f"终止 PID {pid} 失败（taskkill 返回非零）")

    return killed, errors


def wait_for_health(
    url: str,
    check_fn,
    timeout: float = 15.0,
    interval: float = 1.0,
) -> bool:
    """轮询 HTTP 健康检查，直到通过或超时。

    Args:
        url: 健康检查 URL
        check_fn: 判定函数 (status_code: int, body: str, headers: dict) -> bool
        timeout: 超时秒数
        interval: 重试间隔秒数

    Returns:
        是否在超时前通过检查
    """
    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                headers = dict(resp.headers)
                if check_fn(resp.status, body, headers):
                    return True
        except Exception as exc:
            last_error = str(exc)
        time.sleep(interval)
    # 超时——打印最后错误以辅助排查
    if last_error:
        print(f"  [调试] 最后一次请求失败：{last_error[:200]}")
    return False


def _check_backend_health(status: int, body: str, headers: dict) -> bool:
    """后端健康检查：200 + body 含 'ok'（不区分大小写）。"""
    return status == 200 and "ok" in body.lower()


def _check_frontend_health(status: int, body: str, headers: dict) -> bool:
    """前端健康检查：200 + Content-Type 含 text/html。"""
    return status == 200 and "text/html" in headers.get("content-type", "").lower()


def start_backend(project_root: Path, log_dir: Path) -> subprocess.Popen:
    """启动 uvicorn 后端开发服务器（后台进程）。

    日志写入 log_dir / backend.log。
    """
    log_file = log_dir / "backend.log"
    log_fh = open(log_file, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "tianshu_datadev.api.app:create_app",
            "--factory",
            "--host", "127.0.0.1",
            "--port", "8000",
            "--reload",
        ],
        cwd=str(project_root),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    return proc


def start_frontend(project_root: Path, log_dir: Path) -> subprocess.Popen:
    """启动 Vite 前端开发服务器（后台进程）。

    在 frontend/ 目录下执行 npx vite，日志写入 log_dir / frontend.log。
    """
    log_file = log_dir / "frontend.log"
    log_fh = open(log_file, "w", encoding="utf-8")
    frontend_dir = project_root / "frontend"
    proc = subprocess.Popen(
        ["npx", "vite", "--host", "127.0.0.1", "--port", "5173"],
        cwd=str(frontend_dir),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    return proc


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。

    供 main() 使用，同时导出供单元测试验证参数组合。

    注意：--backend 和 --frontend 使用 store_false + 反向 dest，
    使得默认无参数时两项均为 True（全量处理），
    传参时排除另一项。
    """
    parser = argparse.ArgumentParser(
        description="dev-reload——git pull 后重启前后端开发服务器",
    )
    parser.add_argument(
        "--backend", action="store_false", dest="frontend",
        help="仅处理后端（端口 8000）",
    )
    parser.add_argument(
        "--frontend", action="store_false", dest="backend",
        help="仅处理前端（端口 5173）",
    )
    parser.add_argument(
        "--no-kill", action="store_true",
        help="跳过终止步骤，仅清理缓存 + 补启动缺失服务",
    )
    # 默认无参数 = 全量（前后端都处理）
    parser.set_defaults(backend=True, frontend=True)
    return parser


def main() -> None:
    """入口——按 CLI 参数执行清理 + 重启 + 健康检查。"""
    parser = build_parser()
    args = parser.parse_args()

    # 默认无参数 = 全量（前后端都处理）
    if not args.backend and not args.frontend:
        args.backend = True
        args.frontend = True

    project_root = Path(__file__).resolve().parent.parent
    log_dir = project_root / "logs" / "dev"
    log_dir.mkdir(parents=True, exist_ok=True)

    # ════════ 1. 清理 .pyc 缓存 ════════
    n = clean_pyc(project_root)
    print(f"[清理] 已删除 {n} 个 __pycache__ / *.pyc 文件")

    # ════════ 2-3. 识别 + 终止旧进程 ════════
    if not args.no_kill:
        all_errors: list[str] = []

        if args.backend:
            killed, errors = stop_service(8000, project_root)
            print(f"[终止] 后端端口 8000：终止 {killed} 个进程")
            all_errors.extend(errors)

        if args.frontend:
            killed, errors = stop_service(5173, project_root)
            print(f"[终止] 前端端口 5173：终止 {killed} 个进程")
            all_errors.extend(errors)

        # 白名单拒绝 → 安全退出
        if all_errors:
            print("\n[安全闸门] 以下进程不在白名单内，拒绝终止：\n")
            for e in all_errors:
                print(f"  ❌ {e}")
            print(
                "\n请手动检查端口占用情况后重试。\n"
                "提示：如果确认这些进程可终止，可先手动 taskkill 后再运行本脚本。"
            )
            sys.exit(1)

    # 给进程退出留时间
    if not args.no_kill:
        time.sleep(1.5)

    # ════════ 4. 启动服务 ════════
    backend_ok = True
    frontend_ok = True

    if args.backend:
        existing = parse_netstat_listeners(8000)
        if existing:
            print(f"[跳过] 后端已在端口 8000 运行 (PID {existing})")
        else:
            proc = start_backend(project_root, log_dir)
            print(f"[启动] 后端 PID {proc.pid}，日志 → {log_dir / 'backend.log'}")

    if args.frontend:
        existing = parse_netstat_listeners(5173)
        if existing:
            print(f"[跳过] 前端已在端口 5173 运行 (PID {existing})")
        else:
            proc = start_frontend(project_root, log_dir)
            print(f"[启动] 前端 PID {proc.pid}，日志 → {log_dir / 'frontend.log'}")

    # ════════ 5. 健康检查 ════════
    if args.backend:
        print("[验证] 等待后端就绪（超时 15s）...")
        backend_ok = wait_for_health(
            "http://127.0.0.1:8000/api/health",
            _check_backend_health,
            timeout=15.0, interval=1.0,
        )
        if backend_ok:
            print("[验证] 后端 ✓  http://127.0.0.1:8000/api/health")
        else:
            print("[验证] 后端 ✗  健康检查超时，请检查 logs/dev/backend.log")

    if args.frontend:
        print("[验证] 等待前端就绪（超时 10s）...")
        frontend_ok = wait_for_health(
            "http://127.0.0.1:5173/",
            _check_frontend_health,
            timeout=10.0, interval=0.5,
        )
        if frontend_ok:
            print("[验证] 前端 ✓  http://127.0.0.1:5173/")
        else:
            print("[验证] 前端 ✗  健康检查超时，请检查 logs/dev/frontend.log")

    # ════════ 6. 摘要 ════════
    print("\n━━━ dev-reload 完成 ━━━")
    print(f"  后端: {'✅' if backend_ok else '❌'} http://127.0.0.1:8000")
    print(f"  前端: {'✅' if frontend_ok else '❌'} http://127.0.0.1:5173")
    print(f"  日志: {log_dir}")

    if not backend_ok or not frontend_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
