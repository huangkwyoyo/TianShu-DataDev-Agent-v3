"""验证所有子包可以正常导入。

Phase 0 最基础的健康检查——确保包结构正确。
"""

import importlib


# 所有预期存在的子包
EXPECTED_SUBPACKAGES = [
    "tianshu_datadev",
    "tianshu_datadev.ir",
    "tianshu_datadev.orchestration",
    "tianshu_datadev.sql",
    "tianshu_datadev.spark",
    "tianshu_datadev.execution",
    "tianshu_datadev.validation",
    "tianshu_datadev.artifacts",
    "tianshu_datadev.llm",
]


def test_package_imports():
    """验证所有子包可导入且 __version__ 存在。"""
    for pkg_name in EXPECTED_SUBPACKAGES:
        mod = importlib.import_module(pkg_name)
        assert mod is not None, f"无法导入 {pkg_name}"


def test_root_package_version():
    """验证根包版本信息存在。"""
    import tianshu_datadev

    assert tianshu_datadev.__version__ is not None
    assert isinstance(tianshu_datadev.__version__, str)
    assert len(tianshu_datadev.__version__) > 0
