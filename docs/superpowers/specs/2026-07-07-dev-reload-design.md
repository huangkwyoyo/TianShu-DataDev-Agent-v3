# dev-reload——git pull 后自动重启前后端服务

> 文档版本：2026-07-07 | 状态：设计定稿
> 分类：C 类——Agent 行为规范 + 运维脚本

## 问题

Windows Git Bash 环境下，`git pull` 后文件监听器不可靠：

| 现象 | 根因 |
|------|------|
| Vite HMR 不触发 | Git Bash 的文件变更事件不被 Node.js fs.watch 可靠捕获 |
| uvicorn `--reload` 不触发 | 同上，且 `.pyc` 缓存可能覆盖源码 |
| 旧进程残留 | `taskkill` 在 Git Bash 下编码损坏；`kill -9` 对部分 Windows 进程 Access Denied |
| 新增文件（新组件）不被检测 | HMR 增删文件事件比修改事件更不可靠 |

## 目标

`git pull` / `git checkout` 完成后，Agent 执行一条命令即可确保前后端服务加载最新代码，
用户不再需要手动排查"为什么页面没变"。

## 架构

```
git pull 完成
    │
    ▼
Agent 执行 ./dev-reload.sh [--backend] [--frontend] [--no-kill]
    │
    ▼
scripts/dev_reload.py
    │
    ├─ 1. 清理 __pycache__ / *.pyc（全项目）
    ├─ 2. netstat 解析 :8000/:5173 PID
    ├─ 3. 查询命令行 → 白名单判定
    │      ├─ 白名单内 → taskkill /F /PID
    │      └─ 白名单外 → 打印 PID/命令行/端口，exit 1
    ├─ 4. subprocess.Popen 启动 uvicorn + vite
    │      日志 → logs/dev/backend.log / logs/dev/frontend.log
    ├─ 5. 健康检查轮询
    │      后端：GET /api/health → 200 + body 含 "ok"
    │      前端：GET / → 200 + text/html
    └─ 6. 输出摘要（成功/失败 + 端口 + PID + 日志路径）
```

## 文件清单

| 文件 | 用途 |
|------|------|
| `dev-reload.sh` | Bash 入口——`cd` 到项目根再调 Python |
| `scripts/dev_reload.py` | 核心逻辑——清理、识别、白名单、终止、启动、验证 |
| `CLAUDE.md` | 新增"git pull 后强制重启规范"节 |
| `logs/dev/` | 启动日志目录（`.gitignore` 需纳入） |

## 安全设计：进程白名单

**不按端口无条件强杀**。端口 8000/5173 上的 PID 必须先通过白名单检查：

| 端口 | 允许终止的命令行特征 |
|------|---------------------|
| 8000 | 含 `uvicorn` 且含 `tianshu_datadev` |
| 5173 | 含 `vite` 或含 `node` |

若端口被未知程序占用，脚本 **失败退出**，打印 PID、命令行、端口，不强行终止——保护本机其他服务。

## CLI 参数

| 参数 | 行为 |
|------|------|
| （无） | 全量——清缓存 + 杀+启前后端 |
| `--backend` | 仅处理后端（8000） |
| `--frontend` | 仅处理前端（5173） |
| `--no-kill` | 跳过终止步骤，仅清理缓存 + 启动缺失的服务 |

## 健康检查规格

| 服务 | 端点 | 成功条件 | 超时 | 重试间隔 |
|------|------|---------|------|---------|
| 后端 | `http://127.0.0.1:8000/api/health` | 200 + body 含 `"ok"` | 15s | 1s |
| 前端 | `http://127.0.0.1:5173/` | 200 + Content-Type 含 `text/html` | 10s | 0.5s |

仅检查端口监听不够——端口打开不等于新代码已加载、HTTP 可响应。

## CLAUDE.md 修改

在现有 `.pyc 缓存清除` 节之后插入：

```markdown
## git pull 后强制重启规范

**Windows Git Bash 环境下，`git pull` 后 Vite HMR 和 uvicorn --reload
文件监听不可靠，必须执行 `./dev-reload.sh` 确保最新代码生效。**

**Agent 行为规范：**
- `git pull` 或 `git checkout` 完成后，必须立即执行 `./dev-reload.sh`
- 脚本失败时不得跳过——输出包含端口、PID、命令行、日志路径，据此排查
- 成功后直接报告结果，无需再问"需要重启吗"
- 仅需重启一端时用 `--frontend` 或 `--backend`
- 如只需确保缺失服务启动（不杀现有进程），加 `--no-kill`
```

## 不做什么

- 不修改 `package.json` 或 `pyproject.toml`
- 不集成 CI（纯本地开发工具）
- 不处理 Docker 或远程环境
- 不在 `.gitignore` 之外写文件（`logs/dev/` 需加入 `.gitignore`）

## 验收标准

1. `git pull` 后执行 `./dev-reload.sh`，`http://localhost:5173` 加载最新页面
2. 端口 8000 被其他非本项目程序占用时，脚本失败退出并报告 PID/命令行
3. `./dev-reload.sh --backend` 仅重启后端，前端不受影响
4. `./dev-reload.sh --no-kill` 在进程已运行时不会误杀
5. Python 测试基线零退化（脚本本身无测试要求——运维工具，非业务代码）
