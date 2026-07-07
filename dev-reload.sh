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

cd "$(dirname "$0")"
python scripts/dev_reload.py "$@"
