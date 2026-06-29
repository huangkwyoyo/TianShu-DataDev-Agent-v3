"""全局配置加载器——启动时自动读取 .env 文件，无需 python-dotenv 依赖。

在项目入口处调用 load_dotenv() 即可将 .env 中的变量注入 os.environ。
AnthropicAdapter 和其他组件通过 os.environ.get() 读取配置。

用法：
    from tianshu_datadev.config import load_dotenv
    load_dotenv()  # 读取项目根目录的 .env 文件
"""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(env_path: str | None = None) -> bool:
    """加载 .env 文件中的环境变量——简单实现，零外部依赖。

    格式：每行 KEY=VALUE，忽略空行和 # 开头的注释行。
    引号包裹的值自动去除外层引号。

    Args:
        env_path: .env 文件路径——若为 None，自动查找项目根目录

    Returns:
        True 如果找到并加载了 .env 文件，False 如果文件不存在
    """
    if env_path is None:
        # 从当前文件位置向上查找项目根目录
        this_dir = Path(__file__).resolve().parent.parent.parent
        env_path = str(this_dir / ".env")

    env_file = Path(env_path)
    if not env_file.is_file():
        return False

    loaded_count = 0
    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()

        # 跳过空行和注释行
        if not stripped or stripped.startswith("#"):
            continue

        # 解析 KEY=VALUE
        if "=" not in stripped:
            continue

        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip()

        # 去除外层引号（单引号或双引号）
        if len(value) >= 2:
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]

        # 仅当环境变量未设置时才从 .env 加载（不覆盖已有值）
        if key not in os.environ and value:
            os.environ[key] = value
            loaded_count += 1

    return True
