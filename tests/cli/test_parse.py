"""tests/cli/test_parse.py——tianshu parse 子命令测试。"""

import json
import os
import subprocess
import sys

# Windows 下强制子进程输出 UTF-8，避免 GBK/UTF-8 乱码
_ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}


def _parse_error_json(stderr: str) -> dict:
    """从 stderr 中提取最后一行 JSON 错误响应。

    pipeline 内部可能通过 logging 输出诊断信息到 stderr，
    _fail() 输出的 JSON 始终在最后一行——取最后一行解析。
    """
    lines = [line for line in stderr.strip().split("\n") if line.strip()]
    if not lines:
        raise ValueError("stderr 为空，无法提取 JSON 错误响应")
    return json.loads(lines[-1])


class TestCliParse:
    """tianshu parse <file>——解析 DeveloperSpec 文件。"""

    def test_parse_file_success(self, temp_spec_file):
        """解析有效文件 → 输出 JSON + 退出码 0。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "parse", temp_spec_file],
            capture_output=True, text=True, encoding="utf-8", timeout=30, env=_ENV,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        data = json.loads(result.stdout)
        assert data["status"] == "success"
        assert data["command"] == "parse"
        assert data["result"]["spec_id"].startswith("spec_")
        assert data["result"]["table_count"] >= 1

    def test_parse_file_not_found(self):
        """文件不存在 → 退出码非 0 + stderr JSON。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "parse", "nonexistent.md"],
            capture_output=True, text=True, encoding="utf-8", timeout=30, env=_ENV,
        )
        assert result.returncode != 0
        data = _parse_error_json(result.stderr)
        assert data["status"] == "error"
        assert data["error"]["error_code"] == "FILE_NOT_FOUND"

    def test_parse_empty_file(self, temp_empty_file):
        """空文件 → 退出码非 0 + 结构化错误。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "parse", temp_empty_file],
            capture_output=True, text=True, encoding="utf-8", timeout=30, env=_ENV,
        )
        assert result.returncode != 0
        data = _parse_error_json(result.stderr)
        assert data["status"] == "error"
