"""pytest 全局配置——slow 测试门控 + Windows 临时目录隔离。

默认跳过标记为 @pytest.mark.slow 的测试（需要真实 PySpark 子进程执行）。
使用 --run-slow 选项显式运行。

Windows 下系统 Temp 目录可能被其他进程锁定，导致 tmp_path fixture 初始化失败
（PermissionError in pytest_asyncio plugin）。通过设置 basetemp 为项目本地目录来隔离。
"""

import os
import sys

import pytest


def pytest_addoption(parser):
    """注册 --run-slow 和 --run-harness 命令行选项。"""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="运行需要真实 PySpark 子进程的慢速集成测试",
    )
    parser.addoption(
        "--run-harness",
        action="store_true",
        default=False,
        help="运行需要真实 LLM API Key 的 Harness 冒烟测试",
    )


def pytest_configure(config):
    """注册 slow 标记描述 + Windows 临时目录隔离。"""
    config.addinivalue_line(
        "markers",
        "slow: 需要真实 PySpark 子进程执行的慢速集成测试",
    )
    config.addinivalue_line(
        "markers",
        "harness: 需要真实 LLM API Key 的 Harness 冒烟测试（需 --run-harness 启用）",
    )

    # Windows 下使用项目本地临时目录，避免系统 Temp 目录被锁
    if sys.platform == "win32" and not config.option.basetemp:
        # pyproject.toml 的 basetemp 配置项在某些 pytest 版本中不被识别
        # 通过 hook 设置确保生效
        # 使用 PID 子目录避免并行 pytest 进程互相清理/锁定目录
        root_dir = config.rootpath
        basetemp = str(root_dir / ".pytest_tmp" / str(os.getpid()))
        os.makedirs(basetemp, exist_ok=True)
        config.option.basetemp = basetemp


def pytest_collection_modifyitems(config, items):
    """默认跳过 slow 和 harness 测试——除非显式传入对应选项。"""
    # --run-slow 门控
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="需要 --run-slow 选项启用真实 PySpark 执行")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # --run-harness 门控——默认跳过真实 LLM 冒烟测试
    if not config.getoption("--run-harness"):
        skip_harness = pytest.mark.skip(reason="需要 --run-harness 选项启用真实 LLM 调用")
        for item in items:
            if "harness" in item.keywords:
                item.add_marker(skip_harness)
