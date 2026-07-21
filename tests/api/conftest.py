"""API 测试共享 fixtures——TestClient + Pipeline + golden_spec。"""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from tests._test_utils import read_fixture as _read_fixture
from tianshu_datadev.api.app import create_app
from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter

# 项目根目录（相对于本文件）
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture
def pipeline():
    """创建真实 Pipeline 实例——使用临时目录避免污染，测试结束后清理。"""
    tmpdir = tempfile.mkdtemp()
    yield Pipeline(base_output_dir=tmpdir)
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(pipeline):
    """创建 FastAPI TestClient——注入 Pipeline。"""
    app = create_app(pipeline=pipeline)
    return TestClient(app)


@pytest.fixture
def golden_spec():
    """读取 golden fixture——golden_no_time_range.md（会触发 Validator 阻断）。"""
    return _read_fixture("fixtures/golden/golden_no_time_range.md")


@pytest.fixture
def csv_path():
    """返回 test_fact.csv 的绝对路径——消除 5 处 _CSV_PATH 重复定义。"""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "sql", "test_fact.csv",
    )


@pytest.fixture
def golden_spec_passing():
    """读取 golden fixture——golden_passing.md（行数低于阈值，可通过 Validator 校验）。"""
    return _read_fixture("fixtures/golden/golden_passing.md")


@pytest.fixture
def fake_requirement_adapter():
    """FakeLLMAdapter——用于 RequirementPlanner 管线集成测试。"""
    return FakeLLMAdapter()
