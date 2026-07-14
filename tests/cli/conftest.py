"""CLI 测试共享 fixtures——临时 DeveloperSpec 文件。"""

import os

import pytest

# 项目根目录
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


from tests._test_utils import read_fixture


@pytest.fixture
def temp_spec_file(tmp_path):
    """创建临时 DeveloperSpec 文件——内容来自 golden fixture。"""
    content = read_fixture("fixtures/golden/golden_passing.md")
    f = tmp_path / "spec.md"
    f.write_text(content, encoding="utf-8")
    return str(f)


@pytest.fixture
def temp_empty_file(tmp_path):
    """创建临时空文件。"""
    f = tmp_path / "empty.md"
    f.write_text("", encoding="utf-8")
    return str(f)


@pytest.fixture
def test_fact_csv_path():
    """返回 test_fact.csv 的绝对路径。"""
    return os.path.join(_ROOT, "tests", "fixtures", "sql", "test_fact.csv")
