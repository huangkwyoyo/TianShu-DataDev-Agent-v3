"""pytest 全局配置——slow 测试门控。

默认跳过标记为 @pytest.mark.slow 的测试（需要真实 PySpark 子进程执行）。
使用 --run-slow 选项显式运行。
"""

import pytest


def pytest_addoption(parser):
    """注册 --run-slow 命令行选项。"""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="运行需要真实 PySpark 子进程的慢速集成测试",
    )


def pytest_configure(config):
    """注册 slow 标记描述。"""
    config.addinivalue_line(
        "markers",
        "slow: 需要真实 PySpark 子进程执行的慢速集成测试",
    )


def pytest_collection_modifyitems(config, items):
    """默认跳过 slow 测试——除非显式传入 --run-slow。"""
    if config.getoption("--run-slow"):
        return
    skip_slow = pytest.mark.skip(reason="需要 --run-slow 选项启用真实 PySpark 执行")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
