#!/usr/bin/env bash
# dev-reload——git pull 后自动重启前后端开发服务器
#
# 用法：
#   ./dev-reload.sh                  # 全量——清缓存 + 重启前后端
#   ./dev-reload.sh --backend        # 仅后端
#   ./dev-reload.sh --frontend       # 仅前端
#   ./dev-reload.sh --no-kill        # 跳过终止步骤（仅限手动诊断场景）
#
# 禁止在 git pull / git checkout 后使用 --no-kill。
#
# 核心逻辑在 scripts/dev_reload.py 中，本脚本仅负责切换到项目根目录。

set -euo pipefail

# 确保 Python 子进程使用 UTF-8 输出（Windows Git Bash 默认 GBK 会导致 emoji 编码崩溃）
export PYTHONIOENCODING=utf-8

cd "$(dirname "$0")"

# 使用系统 Python（D:\Program Files\Python312），绕过 .venv 自动激活
# 避免因 .venv 未安装 pyspark 导致物理验证跳过
SYSTEM_PYTHON="/d/Program Files/Python312/python"
if [ ! -f "$SYSTEM_PYTHON" ]; then
    # fallback：如果系统 Python 路径不存在，使用 PATH 上的 python
    SYSTEM_PYTHON="python"
fi
"$SYSTEM_PYTHON" scripts/dev_reload.py "$@"
