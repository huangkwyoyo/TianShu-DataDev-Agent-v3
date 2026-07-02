"""API 测试共享 fixtures——TestClient + Pipeline + golden_spec。"""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from tianshu_datadev.api.app import create_app
from tianshu_datadev.api.pipeline import Pipeline

# 项目根目录（相对于本文件）
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def _read_fixture(filename: str) -> str:
    """读取测试 fixture 文件。"""
    path = os.path.join(_ROOT, "tests", "fixtures", "golden", filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def pipeline():
    """创建真实 Pipeline 实例——使用临时目录避免污染。"""
    return Pipeline(base_output_dir=tempfile.mkdtemp())


@pytest.fixture
def client(pipeline):
    """创建 FastAPI TestClient——注入 Pipeline。"""
    app = create_app(pipeline=pipeline)
    return TestClient(app)


@pytest.fixture
def golden_spec():
    """读取 golden fixture——golden_no_time_range.md（会触发 Validator 阻断）。"""
    return _read_fixture("golden_no_time_range.md")


@pytest.fixture
def golden_spec_passing():
    """读取 golden fixture——golden_passing.md（行数低于阈值，可通过 Validator 校验）。"""
    return _read_fixture("golden_passing.md")
