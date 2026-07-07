# dev-reload——git pull 后自动重启前后端服务 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供 `./dev-reload.sh` 一键脚本，使 Agent 在 `git pull` / `git checkout` 后可靠地重启前后端开发服务器，确保最新代码生效。

**Architecture:** `dev-reload.sh` → `scripts/dev_reload.py`（Python 核心）→ 清理 .pyc → netstat 解析端口 → 白名单检查命令行 → taskkill 终止 → subprocess.Popen 启动 uvicorn + vite → HTTP 健康检查轮询。

**Tech Stack:** Python 3.12 (subprocess + urllib.request)，Bash（入口），Windows netstat + taskkill + wmic

## 全局约束

- 不修改 `package.json`、`pyproject.toml` 或任何业务代码
- 不集成 CI——纯本地开发工具
- 不处理 Docker 或远程环境
- 杀进程必须通过白名单检查——端口 8000 要求 `uvicorn` + `tianshu_datadev`；端口 5173 要求 `vite`/`npm`/`node` **且**路径在项目 `frontend/` 目录
- 白名单外的进程 → 打印 PID/命令行/端口，exit 1，不强行终止
- 所有代码注释使用中文
- `logs/dev/` 目录纳入 `.gitignore`

---

### Task 1: 单元测试——netstat 解析、白名单、参数组合

**文件:**
- Create: `tests/scripts/__init__.py`
- Create: `tests/scripts/test_dev_reload.py`

**接口:**
- Produces: 12 个测试函数，覆盖 `parse_netstat_listeners()`、`check_whitelist()`、`clean_pyc()`、CLI 参数解析
- 所有测试直接调用 `scripts.dev_reload` 模块中的纯函数（不含 main/进程操作）

- [ ] **Step 1: 创建 `tests/scripts/__init__.py`**

```bash
mkdir -p tests/scripts
```

```python
# tests/scripts/__init__.py
```

- [ ] **Step 2: 创建 `tests/scripts/test_dev_reload.py`**

```python
"""dev_reload.py 单元测试——netstat 解析、白名单、pyc 清理、参数组合。

不测试启动/终止/sleep 等副作用操作——这些由集成环境验证。
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# 将 scripts/ 加入路径以导入被测模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))
import dev_reload


# ════════════════════════════════════════════
# netstat 输出解析
# ════════════════════════════════════════════


class TestParseNetstatListeners:
    """parse_netstat_listeners(port) 解析 netstat -ano 输出。"""

    SAMPLE_OUTPUT = """
Active Connections

  Proto  Local Address          Foreign Address        State           PID
  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING       1236
  TCP    127.0.0.1:8000         0.0.0.0:0              LISTENING       10228
  TCP    127.0.0.1:8000         0.0.0.0:0              LISTENING       9999
  TCP    [::1]:5173             [::]:0                 LISTENING       22192
  TCP    127.0.0.1:5173         0.0.0.0:0              LISTENING       22192
  TCP    127.0.0.1:3306         0.0.0.0:0              LISTENING       4500
"""

    def test_extracts_pids_for_port_8000(self):
        """8000 端口返回所有 LISTENING PID。"""
        with patch("subprocess.check_output", return_value=self.SAMPLE_OUTPUT):
            pids = dev_reload.parse_netstat_listeners(8000)
        assert sorted(pids) == [9999, 10228]

    def test_extracts_pids_for_port_5173(self):
        """5173 端口返回所有 LISTENING PID（含 IPv6）。"""
        with patch("subprocess.check_output", return_value=self.SAMPLE_OUTPUT):
            pids = dev_reload.parse_netstat_listeners(5173)
        assert 22192 in pids

    def test_empty_when_no_listeners(self):
        """无监听时返回空列表。"""
        with patch("subprocess.check_output", return_value="Active Connections\n"):
            pids = dev_reload.parse_netstat_listeners(9999)
        assert pids == []

    def test_ignores_non_listening_entries(self):
        """非 LISTENING 状态的行不提取 PID。"""
        output = """Active Connections
  TCP    127.0.0.1:8000         0.0.0.0:0              ESTABLISHED     5555
"""
        with patch("subprocess.check_output", return_value=output):
            pids = dev_reload.parse_netstat_listeners(8000)
        assert pids == []


# ════════════════════════════════════════════
# 白名单检查
# ════════════════════════════════════════════


class TestCheckWhitelistBackend:
    """端口 8000 白名单：命令行必须含 uvicorn 且含 tianshu_datadev。"""

    PROJECT_ROOT = Path("D:/Project/TianShu-DataDev-Agent-v3")

    def test_allows_uvicorn_with_tianshu(self):
        """uvicorn + tianshu_datadev → allowed。"""
        with patch.object(
            dev_reload, "get_process_command_line",
            return_value="python -m uvicorn tianshu_datadev.api.app:create_app --factory --host 127.0.0.1 --port 8000 --reload",
        ):
            allowed, reason = dev_reload.check_whitelist(12345, 8000, self.PROJECT_ROOT)
        assert allowed is True
        assert reason == ""

    def test_rejects_unknown_process_on_8000(self):
        """非 uvicorn 进程 → rejected + 含 PID 和命令行。"""
        with patch.object(
            dev_reload, "get_process_command_line",
            return_value="java -jar some-service.jar",
        ):
            allowed, reason = dev_reload.check_whitelist(12345, 8000, self.PROJECT_ROOT)
        assert allowed is False
        assert "12345" in reason

    def test_rejects_empty_command_line(self):
        """命令行获取失败 → rejected + 说明原因。"""
        with patch.object(dev_reload, "get_process_command_line", return_value=""):
            allowed, reason = dev_reload.check_whitelist(12345, 8000, self.PROJECT_ROOT)
        assert allowed is False
        assert "无法获取" in reason


class TestCheckWhitelistFrontend:
    """端口 5173 白名单：vite/npm/node 且路径在项目 frontend/ 目录。"""

    PROJECT_ROOT = Path("D:/Project/TianShu-DataDev-Agent-v3")

    def test_allows_vite_in_frontend_dir(self):
        """vite 且路径含 frontend → allowed。"""
        cmdline = (
            "node D:\\Project\\TianShu-DataDev-Agent-v3\\frontend\\node_modules\\.bin\\vite"
            " --host 127.0.0.1 --port 5173"
        )
        with patch.object(dev_reload, "get_process_command_line", return_value=cmdline):
            allowed, reason = dev_reload.check_whitelist(22192, 5173, self.PROJECT_ROOT)
        assert allowed is True
        assert reason == ""

    def test_allows_node_in_frontend_dir(self):
        """node 且 cwd 在 frontend 目录 → allowed。"""
        cmdline = (
            r'C:\Program Files\nodejs\node.exe  '
            r'D:\Project\TianShu-DataDev-Agent-v3\frontend\node_modules\.bin\vite.js'
        )
        with patch.object(dev_reload, "get_process_command_line", return_value=cmdline):
            allowed, reason = dev_reload.check_whitelist(22192, 5173, self.PROJECT_ROOT)
        assert allowed is True
        assert reason == ""

    def test_rejects_node_outside_frontend_dir(self):
        """node 不在 frontend 目录 → rejected（边界——保护非本项目 Node 服务）。"""
        cmdline = "node C:\\OtherProject\\server.js"
        with patch.object(dev_reload, "get_process_command_line", return_value=cmdline):
            allowed, reason = dev_reload.check_whitelist(22192, 5173, self.PROJECT_ROOT)
        assert allowed is False
        assert "5173" in reason  # 报告中含端口号

    def test_rejects_unknown_process_on_5173(self):
        """非 Node 进程 → rejected。"""
        with patch.object(
            dev_reload, "get_process_command_line",
            return_value="nginx.exe",
        ):
            allowed, reason = dev_reload.check_whitelist(22192, 5173, self.PROJECT_ROOT)
        assert allowed is False


class TestCheckWhitelistUnknownPort:
    """未知端口一律拒绝。"""

    PROJECT_ROOT = Path("D:/Project/TianShu-DataDev-Agent-v3")

    def test_rejects_unknown_port(self):
        """端口 3000 不在白名单中 → rejected。"""
        with patch.object(
            dev_reload, "get_process_command_line",
            return_value="node server.js",
        ):
            allowed, reason = dev_reload.check_whitelist(5555, 3000, self.PROJECT_ROOT)
        assert allowed is False
        assert "3000" in reason


# ════════════════════════════════════════════
# pyc 清理
# ════════════════════════════════════════════


class TestCleanPyc:
    """clean_pyc(project_root) 清理 __pycache__ 和 *.pyc。"""

    def test_removes_pycache_dirs(self):
        """删除 __pycache__ 目录。"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pycache = root / "__pycache__"
            pycache.mkdir()
            (pycache / "test.pyc").write_text("dummy")
            count = dev_reload.clean_pyc(root)
            assert count >= 1
            assert not pycache.exists()

    def test_removes_pyc_files(self):
        """删除根级 *.pyc 文件。"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pyc_file = root / "module.pyc"
            pyc_file.write_text("dummy")
            count = dev_reload.clean_pyc(root)
            assert count >= 1
            assert not pyc_file.exists()

    def test_ignores_non_pyc_files(self):
        """.py 文件不受影响。"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            py_file = root / "module.py"
            py_file.write_text("print('hello')")
            dev_reload.clean_pyc(root)
            assert py_file.exists()


# ════════════════════════════════════════════
# CLI 参数组合
# ════════════════════════════════════════════


class TestCliArgs:
    """argparse 参数解析——默认值 + 组合。"""

    def _parse(self, argv: list[str]) -> argparse.Namespace:
        return dev_reload.build_parser().parse_args(argv)

    def test_default_is_full(self):
        """无参数 → backend=True, frontend=True, no_kill=False。"""
        args = self._parse([])
        assert args.backend is True
        assert args.frontend is True
        assert args.no_kill is False

    def test_backend_only(self):
        """--backend → 仅后端。"""
        args = self._parse(["--backend"])
        assert args.backend is True
        assert args.frontend is False
        assert args.no_kill is False

    def test_frontend_only(self):
        """--frontend → 仅前端。"""
        args = self._parse(["--frontend"])
        assert args.backend is False
        assert args.frontend is True
        assert args.no_kill is False

    def test_no_kill(self):
        """--no-kill → 跳过终止。"""
        args = self._parse(["--no-kill"])
        assert args.backend is True
        assert args.frontend is True
        assert args.no_kill is True

    def test_combined_backend_no_kill(self):
        """--backend --no-kill 组合。"""
        args = self._parse(["--backend", "--no-kill"])
        assert args.backend is True
        assert args.frontend is False
        assert args.no_kill is True


# ════════════════════════════════════════════
# stop_service 白名单拒绝场景
# ════════════════════════════════════════════


class TestStopServiceRejectsUnknown:
    """stop_service 遇到白名单外进程 → 返回 errors 而非崩溃。"""

    PROJECT_ROOT = Path("D:/Project/TianShu-DataDev-Agent-v3")

    def test_reports_unknown_as_error(self):
        """白名单外进程 → killed=0，errors 非空。"""
        with patch.object(dev_reload, "parse_netstat_listeners", return_value=[6666]):
            with patch.object(
                dev_reload, "get_process_command_line",
                return_value="SomeOtherApp.exe",
            ):
                killed, errors = dev_reload.stop_service(8000, self.PROJECT_ROOT)
        assert killed == 0
        assert len(errors) == 1
        assert "6666" in errors[0]
```

- [ ] **Step 3: 运行测试——应全部 FAIL（被测模块不存在）**

```bash
python -m pytest tests/scripts/test_dev_reload.py -v --tb=short 2>&1 | tail -30
```

Expected: 全部 16 个测试 FAIL（`ModuleNotFoundError: No module named 'dev_reload'`）

- [ ] **Step 4: Commit**

```bash
git add tests/scripts/__init__.py tests/scripts/test_dev_reload.py
git commit -m "test: dev_reload 单元测试——netstat 解析 + 白名单 + pyc 清理 + CLI 参数（RED）"
```

---

### Task 2: `scripts/dev_reload.py` 核心脚本

**文件:**
- Create: `scripts/__init__.py`
- Create: `scripts/dev_reload.py`

**接口:**
- Consumes: 无（独立脚本，仅依赖 Python 标准库）
- Produces: `parse_netstat_listeners(port) -> list[int]`、`get_process_command_line(pid) -> str`、`check_whitelist(pid, port, project_root) -> tuple[bool, str]`、`kill_process(pid) -> bool`、`stop_service(port, project_root) -> tuple[int, list[str]]`、`wait_for_health(url, check_fn, timeout, interval) -> bool`、`start_backend(project_root, log_dir) -> Popen`、`start_frontend(project_root, log_dir) -> Popen`、`clean_pyc(project_root) -> int`、`build_parser() -> ArgumentParser`、`main()`

- [ ] **Step 1: 创建 `scripts/__init__.py`**

```python
# scripts/__init__.py
```

- [ ] **Step 2: 创建 `scripts/dev_reload.py`**

```python
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
    """
    parser = argparse.ArgumentParser(
        description="dev-reload——git pull 后重启前后端开发服务器",
    )
    parser.add_argument(
        "--backend", action="store_true",
        help="仅处理后端（端口 8000）",
    )
    parser.add_argument(
        "--frontend", action="store_true",
        help="仅处理前端（端口 5173）",
    )
    parser.add_argument(
        "--no-kill", action="store_true",
        help="跳过终止步骤，仅清理缓存 + 补启动缺失服务",
    )
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
```

- [ ] **Step 3: 运行单元测试——应全部 PASS**

```bash
python -m pytest tests/scripts/test_dev_reload.py -v --tb=short 2>&1 | tail -30
```

Expected: 16 passed

- [ ] **Step 4: 运行全量回归测试**

```bash
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

Expected: 全部已有测试通过 + 16 新测试，零退化

- [ ] **Step 5: Commit**

```bash
git add scripts/__init__.py scripts/dev_reload.py
git commit -m "feat: dev_reload.py 核心脚本——清理 + 白名单杀进程 + 启动 + 健康检查"
```

---

### Task 3: 基础设施——`dev-reload.sh` + `.gitignore` + `logs/dev/`

**文件:**
- Create: `dev-reload.sh`
- Modify: `.gitignore`
- Create: `logs/dev/.gitkeep`

**接口:**
- Consumes: `scripts/dev_reload.py`（Task 2 产出）
- Produces: 用户可执行的 `./dev-reload.sh` 入口 + 日志目录

- [ ] **Step 1: 创建 `dev-reload.sh`**

```bash
#!/usr/bin/env bash
# dev-reload——git pull 后自动重启前后端开发服务器
#
# 用法：
#   ./dev-reload.sh                  # 全量——清缓存 + 重启前后端
#   ./dev-reload.sh --backend        # 仅后端
#   ./dev-reload.sh --frontend       # 仅前端
#   ./dev-reload.sh --no-kill        # 跳过终止步骤（仅限手动诊断场景）
#
# 禁止在 git pull / git checkout 后使用 --no-kill。
#
# 核心逻辑在 scripts/dev_reload.py 中，本脚本仅负责切换到项目根目录。

set -euo pipefail

cd "$(dirname "$0")"
python scripts/dev_reload.py "$@"
```

- [ ] **Step 2: 设置可执行权限**

```bash
chmod +x dev-reload.sh
```

- [ ] **Step 3: 修改 `.gitignore`——添加 `logs/dev/`**

在 `.gitignore` 末尾（`llm_reports/` 之后）追加：

```
# dev-reload 启动日志
logs/dev/
```

- [ ] **Step 4: 创建 `logs/dev/.gitkeep`**

```bash
mkdir -p logs/dev
```

```gitkeep
# .gitkeep——保持 logs/dev/ 目录在 git 中存在（日志文件被 .gitignore 排除）
```

- [ ] **Step 5: 验证脚本语法**

```bash
bash -n dev-reload.sh && echo "语法检查通过"
```

Expected: `语法检查通过`

- [ ] **Step 6: Commit**

```bash
git add dev-reload.sh .gitignore logs/dev/.gitkeep
git commit -m "feat: dev-reload.sh 入口 + .gitignore logs/dev/ + .gitkeep"
```

---

### Task 4: CLAUDE.md——新增"git pull 后强制重启规范"

**文件:**
- Modify: `CLAUDE.md`（在第 13-27 行 `.pyc 缓存清除` 节之后插入新节）

**接口:**
- Consumes: `dev-reload.sh`（Task 3 产出）
- Produces: Agent 行为规范——`git pull` 后必须执行 `./dev-reload.sh`

- [ ] **Step 1: 在 `.pyc 缓存清除` 节之后插入新节**

在 `CLAUDE.md` 第 27 行（`.pyc 缓存清除` 节结束的 `` ``` `` 之后、`## CodeGraph 使用策略` 之前）插入：

```markdown

## git pull 后强制重启规范

**Windows Git Bash 环境下，`git pull` 后 Vite HMR 和 uvicorn --reload
文件监听不可靠，必须执行 `./dev-reload.sh` 确保最新代码生效。**

**Agent 行为规范：**
- `git pull` 或 `git checkout` 完成后，必须立即执行 `./dev-reload.sh`
- 脚本失败时不得跳过——输出包含端口、PID、命令行、日志路径，据此排查
- 成功后直接报告结果，无需再问"需要重启吗"
- 仅需重启一端时用 `--frontend` 或 `--backend`
- **禁止在 `git pull` / `git checkout` 后使用 `--no-kill`**——该参数仅限手动诊断"补启动缺失服务"场景
```

- [ ] **Step 2: 验证 CLAUDE.md 结构完整**

```bash
grep -n "^## " CLAUDE.md
```

Expected: 输出含 `git pull 后强制重启规范` 节，位于 `.pyc 缓存清除` 和 `CodeGraph 使用策略` 之间

- [ ] **Step 3: 最终全量回归**

```bash
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -10
python -m ruff check scripts/
```

Expected: 全量测试通过 + ruff 零告警

- [ ] **Step 4: 提交所有变更**

```bash
git add CLAUDE.md
git status
```

确认无遗漏文件后，统一标记任务完成。

---

## 验证清单

完成所有 Task 后：

```bash
# 1. 全量 pytest
python -m pytest tests/ -v --tb=short

# 2. ruff 零告警
python -m ruff check scripts/

# 3. Bash 语法检查
bash -n dev-reload.sh

# 4. 脚本帮助信息
python scripts/dev_reload.py --help

# 5. 确认产物文件存在
ls -la dev-reload.sh scripts/dev_reload.py tests/scripts/test_dev_reload.py
```
