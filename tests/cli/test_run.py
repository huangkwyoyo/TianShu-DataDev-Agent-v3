"""tests/cli/test_run.py——tianshu run 子命令测试。"""

import json
import subprocess
import sys

import pytest


class TestCliRun:
    """tianshu run <file>——全流程执行+打包。"""

    def test_run_success(self, temp_spec_file):
        """全流程执行成功 → JSON + 退出码 0。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "run", temp_spec_file],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0 and "DuckDB" in result.stderr:
            pytest.skip("DuckDB 未安装")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        data = json.loads(result.stdout)
        assert data["status"] == "success"
        assert data["command"] == "run"
        assert data["result"]["package_id"].startswith("pkg_")

    def test_run_invalid_file(self):
        """无效文件 → 退出码非 0。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "run", "bad.md"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode != 0
        data = json.loads(result.stderr)
        assert data["status"] == "error"
