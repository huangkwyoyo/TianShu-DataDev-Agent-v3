"""测试公共工具函数——消除跨文件 _read_fixture 等重复定义。"""

import os


def read_fixture(path: str) -> str:
    """读取 tests/ 下的 fixture 文件。path 相对于 tests/ 目录。

    示例：
        text = read_fixture("fixtures/golden/golden_passing.md")
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")

    原 18 个文件中各自定义的 _read_fixture 统一收敛到此函数。
    """
    abs_path = os.path.join(os.path.dirname(__file__), path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()
