"""dev_reload.py 单元测试——netstat 解析、白名单、pyc 清理、参数组合。

不测试启动/终止/sleep 等副作用操作——这些由集成环境验证。
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

# 将 scripts/ 加入路径以导入被测模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))
import dev_reload  # noqa: I001 — 需在 sys.path.insert 之后导入


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

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

    def test_allows_uvicorn_with_tianshu(self):
        """uvicorn + tianshu_datadev → allowed。"""
        with patch.object(
            dev_reload, "get_process_command_line",
            return_value=(
                "python -m uvicorn tianshu_datadev.api.app:create_app "
                "--factory --host 127.0.0.1 --port 8000 --reload"
            ),
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
        """命令行获取失败但进程仍存活 → rejected + 说明原因。"""
        with patch.object(dev_reload, "get_process_command_line", return_value=""):
            with patch.object(dev_reload, "_process_exists", return_value=True):
                allowed, reason = dev_reload.check_whitelist(12345, 8000, self.PROJECT_ROOT)
        assert allowed is False
        assert "无法获取" in reason


class TestCheckWhitelistFrontend:
    """端口 5173 白名单：vite/npm/node 且路径在项目 frontend/ 目录。"""

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

    def test_allows_vite_in_frontend_dir(self):
        """vite 且路径含 frontend → allowed。"""
        cmdline = (
            f"node {self.PROJECT_ROOT / 'frontend' / 'node_modules' / '.bin' / 'vite'}"
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
            fr'{self.PROJECT_ROOT}\frontend\node_modules\.bin\vite.js'
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
        assert "5173" in reason


class TestCheckWhitelistUnknownPort:
    """未知端口一律拒绝。"""

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

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

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

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
