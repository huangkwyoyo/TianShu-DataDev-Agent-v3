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


def parse_netstat_listeners(port: int, validate_liveness: bool = False) -> list[int]:
    """解析 netstat -ano 输出，返回指定端口上所有 LISTENING 状态的 PID。

    Windows 上 netstat 可能返回已退出进程的僵死 PID（TCP 表残留）。
    当 validate_liveness=True 时，对每个 PID 调用 _process_exists() 过滤
    僵死进程——这能防止脚本因 stale netstat 记录而误判"已在运行"跳过重启。

    Args:
        port: 目标端口号
        validate_liveness: True 时过滤掉已不存在的进程

    Returns:
        PID 列表（去重排序，且通过存活校验）
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

    result = sorted(pids)

    # 过滤僵死 PID——netstat 可能残留已退出进程的 TCP 表项
    if validate_liveness and result:
        alive = [p for p in result if _process_exists(p)]
        zombies = [p for p in result if p not in alive]
        if zombies:
            print(f"  [检测] 端口 {port} 发现僵死 PID {zombies}（netstat 残留，已自动忽略）")
        return alive

    return result


def get_process_command_line(pid: int) -> str:
    """通过 wmic 或 PowerShell 获取 Windows 进程的完整命令行。

    优先使用 wmic（Windows 10），不可用时回退到 PowerShell Get-CimInstance
    （Windows 11 已弃用 wmic）。

    Args:
        pid: 进程 ID

    Returns:
        命令行字符串；失败返回空字符串
    """
    # 首选 wmic（Windows 10）——抑制 stderr 避免"wmic 不是内部命令"噪音
    try:
        out = subprocess.check_output(
            f"wmic process where ProcessId={pid} get CommandLine /format:list",
            shell=True, text=True, timeout=5,
            stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("CommandLine="):
                return line.split("=", 1)[1].strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
        pass

    # 回退：PowerShell Get-CimInstance（Windows 11，wmic 已弃用）
    try:
        ps_cmd = (
            "powershell -NoProfile -Command "
            f"\"Get-CimInstance Win32_Process -Filter 'ProcessId={pid}' "
            "| Select-Object -ExpandProperty CommandLine\""
        )
        out = subprocess.check_output(
            ps_cmd, shell=True, text=True, timeout=10,
        )
        result = out.strip()
        if result:
            return result
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
        pass

    return ""


def _process_exists(pid: int) -> bool:
    """检查 PID 对应的进程是否仍然存活。

    通过 PowerShell Get-Process 判断——比 wmic/CimInstance
    更轻量，且能区分"进程不存在"和"权限不足"。

    Args:
        pid: 进程 ID

    Returns:
        True 如果进程存在
    """
    try:
        subprocess.check_output(
            f"powershell -NoProfile -Command \"Get-Process -Id {pid}\"",
            shell=True, text=True, timeout=5,
            stderr=subprocess.DEVNULL,
        )
        return True
    except subprocess.CalledProcessError:
        return False
    except (subprocess.TimeoutExpired, OSError):
        # 超时或系统错误时保守返回 True（宁可拒绝也不误杀）
        return True


def _get_process_start_time(pid: int) -> float | None:
    """获取 Windows 进程的启动时间（Unix timestamp）。

    通过 PowerShell 输出 ISO 8601 格式（ToString('o')），该格式仅含 ASCII
    字符，不依赖系统区域设置——避免中文 Windows 上日期字符串乱码问题。

    Args:
        pid: 进程 ID

    Returns:
        进程启动时间的 Unix timestamp；失败返回 None
    """
    try:
        # ISO 8601 格式（如 "2026-07-07T22:16:02.0000000+08:00"）
        # 仅含 ASCII 字符，不受系统区域/编码影响
        ps_cmd = (
            "powershell -NoProfile -Command "
            f"\"Get-Process -Id {pid} -ErrorAction SilentlyContinue | "
            "ForEach-Object { $_.StartTime.ToString('o') }\""
        )
        out = subprocess.check_output(
            ps_cmd, shell=True, text=True, timeout=10,
            stderr=subprocess.DEVNULL,
        )
        dt_str = out.strip()
        if not dt_str:
            return None
        # 解析 ISO 8601 格式
        from datetime import datetime
        try:
            # Python 3.7+ 支持 ISO 8601 解析（含时区）
            dt = datetime.fromisoformat(dt_str)
            return dt.timestamp()
        except ValueError:
            # 降级：去掉纳秒部分重试
            if "." in dt_str and "+" in dt_str:
                try:
                    main, rest = dt_str.split(".", 1)
                    tz_pos = rest.index("+") if "+" in rest else rest.index("-")
                    clean = main + rest[tz_pos:]
                    dt = datetime.fromisoformat(clean)
                    return dt.timestamp()
                except (ValueError, IndexError):
                    pass
            return None
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
        return None


def _check_source_freshness(project_root: Path, pids: list[int]) -> bool:
    """检查源码是否比运行中的服务进程更新。

    若 src/ 下任何 .py 文件的 mtime 晚于所有服务进程的启动时间，
    说明 StatReload 未能触发重载，源码修改未生效——需要强制重启。

    这是 Windows Git Bash 下 uvicorn --reload 文件监听不可靠的补偿措施。

    Args:
        project_root: 项目根目录
        pids: 当前在监听端口的存活进程 PID 列表

    Returns:
        True 如果所有进程都已加载最新源码（无需重启）
        False 如果源码更新但进程未重载（需要强制重启）
    """
    if not pids:
        return True  # 无进程在运行，谈不上"新鲜度"

    # 获取 src/ 下最新 .py 文件的 mtime
    src_dir = project_root / "src"
    if not src_dir.is_dir():
        return True

    latest_source_mtime = 0.0
    for py_file in src_dir.rglob("*.py"):
        try:
            mtime = py_file.stat().st_mtime
            if mtime > latest_source_mtime:
                latest_source_mtime = mtime
        except OSError:
            continue

    if latest_source_mtime == 0.0:
        return True  # 无源文件可检查

    # 获取所有进程的启动时间，取最早的（最保守）
    all_started_before_source = True
    for pid in pids:
        start_time = _get_process_start_time(pid)
        if start_time is None:
            # 无法获取启动时间——保守认为进程是旧的，需要重启
            all_started_before_source = False
            break
        if start_time < latest_source_mtime:
            # 进程在源码修改之前启动 → StatReload 未触发 → 旧代码在跑
            all_started_before_source = False
            break

    return all_started_before_source


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
        # 可能是僵尸 PID（netstat 残留但进程已退出）
        if not _process_exists(pid):
            return True, ""  # 进程已死，视为可安全清理
        return False, (
            f"PID {pid} 命令行无法获取（进程仍存活但权限不足）"
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

    进程不存在（taskkill 报"没有找到进程"）时也返回 True——
    表示该 PID 已不存在，目标状态已达成。

    Args:
        pid: 进程 ID

    Returns:
        是否成功终止（或进程本就不存在）
    """
    try:
        result = subprocess.run(
            f"taskkill /F /PID {pid}",
            shell=True, capture_output=True, text=True, timeout=10,
        )
        # taskkill 成功（returncode 0）或进程不存在都视为成功
        if result.returncode == 0:
            return True
        if result.stderr and "没有找到" in result.stderr:
            return True  # 进程已不存在
        if result.stdout and "没有找到" in result.stdout:
            return True
        return False
    except (subprocess.TimeoutExpired, OSError):
        return False


def _find_uvicorn_workers(
    project_root: Path,
    known_parent_pids: set[int] | None = None,
) -> list[int]:
    """查找所有属于本项目的 uvicorn worker 进程（不通过端口，通过命令行匹配）。

    Windows 上 uvicorn reloader 的 worker 子进程通过 multiprocessing.spawn 启动，
    命令行不含 "uvicorn" 或项目名，netstat 查不到。此函数分两个阶段补漏：

    阶段 1：直接匹配 reloader/server 进程（命令行含 uvicorn + 项目名）
    阶段 2：检测孤儿 worker——命令行含 multiprocessing.spawn + spawn_main
            且 parent_pid 在 known_parent_pids 中（关联到本项目端口），父进程已死

    Args:
        project_root: 项目根目录
        known_parent_pids: 已知的本项目 reloader PID 集合（含僵尸 PID）——
                           仅其孤儿子进程会被阶段 2 匹配，避免误杀其他应用的 worker

    Returns:
        应被终止的 PID 列表
    """
    worker_pids: list[int] = []
    try:
        # PowerShell 脚本——避免 shell=True 的嵌套引号转义问题
        ps_script = (
            "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
            "Select-Object ProcessId, CommandLine | "
            "ForEach-Object { Write-Output \"$($_.ProcessId)|$($_.CommandLine)\" }"
        )
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", ps_script],
            text=True, timeout=15,
            stderr=subprocess.DEVNULL,
        )
        repo_name = project_root.name.lower()
        import re

        for line in out.splitlines():
            line = line.strip()
            if not line or "|" not in line:
                continue
            try:
                pid_str, cmdline = line.split("|", 1)
                pid = int(pid_str.strip())
                cmd_lower = cmdline.lower()
                # 阶段 1：匹配 uvicorn reloader/server 进程
                if "uvicorn" in cmd_lower and (
                    repo_name in cmd_lower or "tianshu" in cmd_lower
                ):
                    worker_pids.append(pid)
                    continue

                # 阶段 2：检测孤儿 multiprocessing.spawn worker
                # 命令行格式示例：
                # python.exe -c "from multiprocessing.spawn import spawn_main;
                #   spawn_main(parent_pid=23832, pipe_handle=412)" --multiprocessing-fork
                if "multiprocessing.spawn" in cmd_lower and "spawn_main" in cmd_lower:
                    match = re.search(r"parent_pid=(\d+)", cmdline)
                    if match:
                        parent_pid = int(match.group(1))
                        # 安全检查：仅当 parent_pid 关联到本项目端口时才处理
                        # 避免误杀其他应用的 multiprocessing worker
                        if known_parent_pids and parent_pid not in known_parent_pids:
                            continue
                        if not _process_exists(parent_pid):
                            # 父进程已死 → 孤儿 worker → 加入清理列表
                            worker_pids.append(pid)
            except (ValueError, IndexError):
                continue
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
        pass
    return worker_pids


def stop_service(port: int, project_root: Path) -> tuple[int, list[str]]:
    """停止指定端口上的本项目 dev 进程。

    四层清理策略（解决 Windows 上 uvicorn worker 不随 reloader 退出的问题）：
    1. netstat LISTENING → 存活校验 → 白名单检查 → taskkill
    2. 扫描 multiprocessing.spawn 孤儿 worker（parent_pid 关联到本端口）
    3. 二次验证——等待后重新检查端口 + 孤儿 worker
    4. 兜底——再次扫描孤儿 worker（解决 reloader 被杀前瞬间 spawn 的竞态）

    Args:
        port: 目标端口
        project_root: 项目根目录

    Returns:
        (killed_count, errors)——errors 非空时调用方应终止脚本
    """
    errors: list[str] = []
    killed = 0

    # ════ 第 1 层：netstat 端口监听进程 ════
    # 启用存活校验——过滤掉 Windows netstat 的僵死 TCP 表项
    pids = parse_netstat_listeners(port, validate_liveness=True)
    all_port_pids = set(parse_netstat_listeners(port, validate_liveness=False))
    zombies = all_port_pids - set(pids)
    if zombies:
        print(f"  [检测] 端口 {port} 发现僵死 PID {sorted(zombies)}（netstat 残留，已自动忽略）")

    for pid in pids:
        allowed, reason = check_whitelist(pid, port, project_root)
        if not allowed:
            errors.append(reason)
            continue
        if kill_process(pid):
            killed += 1
        else:
            errors.append(f"终止 PID {pid} 失败（taskkill 返回非零）")

    # ════ 第 2 层：扫描 orphan worker 进程 ════
    # uvicorn reloader 的 worker 子进程通过 multiprocessing.spawn 启动，
    # 不绑定端口、命令行不含项目名。通过 parent_pid 关联到本端口的 PID 来识别。
    # all_port_pids 包含僵尸 PID——即使 reloader 已死，其孤儿 worker 的 parent_pid
    # 仍在 all_port_pids 中，确保能被匹配到。
    worker_pids = _find_uvicorn_workers(project_root, known_parent_pids=all_port_pids)
    for pid in worker_pids:
        if pid in pids:
            continue  # 已处理过
        if kill_process(pid):
            killed += 1
        else:
            errors.append(f"终止 worker PID {pid} 失败（taskkill 返回非零）")

    # ════ 第 3 层：二次验证——等待后重新检查 ════
    if killed > 0:
        time.sleep(1.0)
        # 3a. 重新检查端口监听进程
        remaining = parse_netstat_listeners(port, validate_liveness=True)
        for pid in remaining:
            allowed, reason = check_whitelist(pid, port, project_root)
            if not allowed:
                errors.append(reason)
                continue
            if kill_process(pid):
                killed += 1
            else:
                errors.append(f"终止残留 PID {pid} 失败（taskkill 返回非零）")

    # ════ 第 4 层：兜底——再次扫描孤儿 worker ════
    # 解决竞态：reloader 在被杀前瞬间 spawn 了新的 worker
    # 此时新 worker 的 parent_pid 可能在第 2 层时仍存活（reloader 尚未被完全杀死），
    # 需要等 reloader 确认死亡后重新扫描
    orphan_pids = _find_uvicorn_workers(project_root, known_parent_pids=all_port_pids)
    for pid in orphan_pids:
        if pid in pids or pid in worker_pids:
            continue  # 已处理过
        if kill_process(pid):
            killed += 1
        else:
            errors.append(f"终止孤儿 worker PID {pid} 失败（taskkill 返回非零）")

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
                headers = {k.lower(): v for k, v in resp.headers.items()}
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
    log_fh.close()  # 子进程已通过 dup2 获得自己的 fd，可安全关闭父进程句柄
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
        shell=True,  # Windows 上 npx 是 npx.cmd 批处理文件，必须 shell=True
    )
    log_fh.close()  # 子进程已通过 dup2 获得自己的 fd，可安全关闭父进程句柄
    return proc


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。

    供 main() 使用，同时导出供单元测试验证参数组合。

    注意：--backend/--frontend 使用 action="store_false" + 反向 dest 实现
    "无参数=全量"的默认行为——默认值 store_false 使得无参数时两者均为 True。
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
    # 强制 UTF-8 输出——Windows Git Bash 默认 GBK 可能导致编码崩溃
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    parser = build_parser()
    args = parser.parse_args()

    # 默认无参数 = 全量（前后端都处理）
    if not args.backend and not args.frontend:
        args.backend = True
        args.frontend = True

    project_root = Path(__file__).resolve().parent.parent
    log_dir = project_root / "logs" / "dev"
    log_dir.mkdir(parents=True, exist_ok=True)

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
                print(f"  [FAIL] {e}")
            print(
                "\n请手动检查端口占用情况后重试。\n"
                "提示：如果确认这些进程可终止，可先手动 taskkill 后再运行本脚本。"
            )
            sys.exit(1)

    # 给进程退出留时间
    if not args.no_kill:
        time.sleep(1.5)

    # ════════ 清理 .pyc 缓存 ════════
    # 必须在进程终止后运行——旧进程可能锁定 __pycache__ 文件导致 rmtree 静默失败
    n = clean_pyc(project_root)
    print(f"[清理] 已删除 {n} 个 __pycache__ / *.pyc 文件")

    # ════════ 4. 启动服务 ════════
    backend_ok = True
    frontend_ok = True

    if args.backend:
        # 用存活校验读取端口占用——过滤僵死 PID（netstat TCP 表残留）
        existing = parse_netstat_listeners(8000, validate_liveness=True)
        if existing:
            # 检查源码新鲜度——补偿 Windows 下 StatReload 文件监听不可靠
            if not args.no_kill and not _check_source_freshness(project_root, existing):
                print(
                    "[检测] 后端源码已更新但进程未重载"
                    "（Windows StatReload 限制），强制重启..."
                )
                # 终止旧进程后重新启动
                killed_count = 0
                for pid in existing:
                    if kill_process(pid):
                        killed_count += 1
                if killed_count > 0:
                    print(f"  [终止] 强制终止 {killed_count} 个过期进程")
                    time.sleep(1.5)
                proc = start_backend(project_root, log_dir)
                print(f"[启动] 后端 PID {proc.pid}，日志 → {log_dir / 'backend.log'}")
            else:
                if args.no_kill and not _check_source_freshness(project_root, existing):
                    print(
                        "[警告] 后端源码已更新但进程未重载——"
                        "当前运行的是旧代码！建议去掉 --no-kill 重新执行。"
                    )
                print(f"[跳过] 后端已在端口 8000 运行 (PID {existing})")
        else:
            proc = start_backend(project_root, log_dir)
            print(f"[启动] 后端 PID {proc.pid}，日志 → {log_dir / 'backend.log'}")

    if args.frontend:
        existing = parse_netstat_listeners(5173, validate_liveness=True)
        if existing:
            need_restart = False
            if not args.no_kill:
                # 前端源码新鲜度——检查 frontend/src/ 目录
                frontend_src = project_root / "frontend" / "src"
                if frontend_src.is_dir():
                    latest_fe_mtime = 0.0
                    for f in frontend_src.rglob("*"):
                        if not f.is_file():
                            continue
                        try:
                            mtime = f.stat().st_mtime
                            if mtime > latest_fe_mtime:
                                latest_fe_mtime = mtime
                        except OSError:
                            continue
                    if latest_fe_mtime > 0.0:
                        for pid in existing:
                            start_time = _get_process_start_time(pid)
                            if start_time is not None and start_time < latest_fe_mtime:
                                need_restart = True
                                break
            if need_restart:
                print(
                    "[检测] 前端源码已更新但进程未重载"
                    "（Windows HMR 限制），强制重启..."
                )
                for pid in existing:
                    kill_process(pid)
                time.sleep(1.5)
                proc = start_frontend(project_root, log_dir)
                print(f"[启动] 前端 PID {proc.pid}，日志 → {log_dir / 'frontend.log'}")
            else:
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
            print("[验证] 后端 [OK]  http://127.0.0.1:8000/api/health")
        else:
            print("[验证] 后端 [FAIL]  健康检查超时，请检查 logs/dev/backend.log")

    if args.frontend:
        print("[验证] 等待前端就绪（超时 10s）...")
        frontend_ok = wait_for_health(
            "http://127.0.0.1:5173/",
            _check_frontend_health,
            timeout=10.0, interval=0.5,
        )
        if frontend_ok:
            print("[验证] 前端 [OK]  http://127.0.0.1:5173/")
        else:
            print("[验证] 前端 [FAIL]  健康检查超时，请检查 logs/dev/frontend.log")

    # ════════ 6. 摘要 ════════
    print("\n=== dev-reload 完成 ===")
    print(f"  后端: {'[OK]' if backend_ok else '[FAIL]'} http://127.0.0.1:8000")
    print(f"  前端: {'[OK]' if frontend_ok else '[FAIL]'} http://127.0.0.1:5173")
    print(f"  日志: {log_dir}")

    if not backend_ok or not frontend_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
