"""tests/cli/test_parse.py——tianshu parse 子命令测试。"""

import json
import subprocess
import sys


class TestCliParse:
    """tianshu parse <file>——解析 DeveloperSpec 文件。"""

    def test_parse_file_success(self, temp_spec_file):
        """解析有效文件 → 输出 JSON + 退出码 0。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "parse", temp_spec_file],
            capture_output=True, text=True, timeout=30,
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
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode != 0
        data = json.loads(result.stderr)
        assert data["status"] == "error"
        assert data["error"]["error_code"] == "FILE_NOT_FOUND"

    def test_parse_empty_file(self, temp_empty_file):
        """空文件 → 退出码非 0 + 结构化错误。"""
        result = subprocess.run(
            [sys.executable, "-m", "tianshu_datadev.cli.main", "parse", temp_empty_file],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode != 0
        data = json.loads(result.stderr)
        assert data["status"] == "error"
