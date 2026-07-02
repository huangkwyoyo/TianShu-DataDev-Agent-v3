"""CLI 测试共享 fixtures——临时 DeveloperSpec 文件。"""

import os

import pytest

# 项目根目录
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def _read_fixture(filename: str) -> str:
    """读取测试 fixture 文件。"""
    path = os.path.join(_ROOT, "tests", "fixtures", "golden", filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def temp_spec_file(tmp_path):
    """创建临时 DeveloperSpec 文件——内容来自 golden fixture。"""
    content = _read_fixture("golden_passing.md")
    f = tmp_path / "spec.md"
    f.write_text(content, encoding="utf-8")
    return str(f)


@pytest.fixture
def temp_empty_file(tmp_path):
    """创建临时空文件。"""
    f = tmp_path / "empty.md"
    f.write_text("", encoding="utf-8")
    return str(f)
