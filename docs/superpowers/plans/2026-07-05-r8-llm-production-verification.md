# R8：LLM 生产环境 API key 配置 + 持续验证链路——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 LLM Gateway 的真实 Provider（DeepSeek/Anthropic）配置安全的手动验证链路——在不污染默认 pytest（FakeAdapter）的前提下，提供一条显式开关控制的真实 LLM 端到端验证路径。

**Architecture:** 现有 `scripts/real_llm_regression.py` 已具备完整的 Prompt→Adapter→Schema 校验链路（覆盖 4 个 task × 8+ 用例）。R8 的核心工作是：加一把"必须显式拧开才能点火"的安全开关（`TIANSHU_RUN_REAL_LLM=1`），提供 `.env.example` 说明密钥从哪读取，输出 Harness 兼容的结构化结果，然后更新文档消除 R8 残留风险。

**Tech Stack:** Python 3.12+, DeepSeek API (Anthropic Messages 兼容端点), httpx, 现有 LLMGateway + AnthropicAdapter + PromptManager

## 全局约束

- **不得**把 API key 写入代码、日志、测试快照、文档或 git 历史
- **不得**默认在 pytest 中调用真实 LLM——所有 pytest 继续使用 FakeLLMAdapter
- **不得**让真实 LLM 输出绕过 Schema 校验——Gateway 的 `_validate_against_schema()` 是必经路径
- **不得**修改 SQL/Spark Pipeline 业务语义
- **不得**进入 Case 06 / SqlProgram DAG 开发
- **不得**把 FakeAdapter 结果冒充真实 LLM 验证——两者必须明确区分
- **只修改** `scripts/real_llm_regression.py`、新建 `.env.example`、更新风险文档和状态文档
- **不修改** `src/tianshu_datadev/llm/` 下的任何代码（Gateway / Adapter / Models 均不变）
- **不修改** `tests/` 下的任何测试代码

---

### Task 1: 安全开关——`TIANSHU_RUN_REAL_LLM` 环境变量门禁

**Files:**
- Modify: `scripts/real_llm_regression.py:1-10, 735-784`

**Interfaces:**
- Consumes: 现有 `run_real_llm_regression()` 函数签名不变
- Produces: `TIANSHU_RUN_REAL_LLM` 环境变量检查——未设置或不为 `"1"` 时脚本拒绝执行，exit code=2

- [ ] **Step 1: 在 CLI 入口添加环境变量门禁**

在 `if __name__ == "__main__":` 块的 `argparse` 之前插入安全检查——这是脚本的第一行实际执行逻辑，确保在任何导入或网络调用之前就被拦截：

```python
if __name__ == "__main__":
    import argparse
    import os
    import sys

    # ── 安全门禁：必须显式设置 TIANSHU_RUN_REAL_LLM=1 才能执行真实调用 ──
    # 此检查放在最前面——在任何网络调用之前拦截。
    # 默认 pytest 使用 FakeLLMAdapter，不会触发此脚本；
    # 只有当用户手动执行此脚本时，才需要此环境变量。
    if os.environ.get("TIANSHU_RUN_REAL_LLM") != "1":
        print(
            "错误：真实 LLM 调用需要显式授权。\n"
            "请设置环境变量 TIANSHU_RUN_REAL_LLM=1 后再执行。\n"
            "示例：TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py\n"
            "\n"
            "此门禁确保：\n"
            "1. 真实 LLM 调用不会在 pytest 中意外触发（pytest 使用 FakeLLMAdapter）\n"
            "2. API key 不会被无意识地消费\n"
            "3. 每次真实调用都是有意的、可审计的操作",
            file=sys.stderr,
        )
        sys.exit(2)
```

- [ ] **Step 2: 在脚本顶部的 docstring 中更新用法说明**

将文件头部的用法说明从：
```
用法：
    python scripts/real_llm_regression.py
```
改为：
```
用法：
    TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py [--model MODEL] [--task TASK] [--json]
```

- [ ] **Step 3: 验证门禁生效**

```bash
# 不设环境变量时脚本应拒绝执行
python scripts/real_llm_regression.py; echo "exit code: $?"
```
期望：输出错误提示到 stderr，exit code=2

```bash
# 设为 0 也应拒绝（只有 "1" 才是合法值）
TIANSHU_RUN_REAL_LLM=0 python scripts/real_llm_regression.py; echo "exit code: $?"
```
期望：exit code=2

```bash
# 设为 1 才能通过门禁（但缺少 API key 会在 Adapter 层报错——这是预期行为）
TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py; echo "exit code: $?"
```
期望：通过门禁，进入 Adapter 初始化（然后因无 API key 报错——验证门禁正确放行）

- [ ] **Step 4: Commit**

```bash
git add scripts/real_llm_regression.py
git commit -m "feat(r8): 添加 TIANSHU_RUN_REAL_LLM 环境变量门禁——防止真实 LLM 意外调用

- 脚本入口处检查 TIANSHU_RUN_REAL_LLM=1，否则拒绝执行（exit code=2）
- 更新文件头部用法说明，标注必须显式设置环境变量
- pytest 默认使用 FakeLLMAdapter，不受此门禁影响"
```

---

### Task 2: 安全文档——`.env.example` + 密钥配置说明

**Files:**
- Create: `.env.example`

**Interfaces:**
- Consumes: 现有 `AnthropicAdapter.__init__()` 的 env var 读取逻辑（`DEEPSEEK_API_KEY`, `ANTHROPIC_API_KEY`, `DEEPSEEK_BASE_URL`, `DEEPSEEK_MODEL`）
- Produces: `.env.example`——不含任何真实密钥的配置模板

- [ ] **Step 1: 创建 `.env.example`**

```ini
# ============================================================
# TianShu DataDev Agent v3 — 环境变量配置模板
# ============================================================
# 复制此文件为 .env 并填入真实值。
# .env 已加入 .gitignore——不会被 git 追踪。
#
# 警告：绝对不要把真实 API key 写入此文件或提交到 git！
# ============================================================

# --- LLM Provider 配置（DeepSeek / Anthropic）---
# DeepSeek API key（通过 Anthropic Messages 兼容端点调用）
# 获取方式：https://platform.deepseek.com/api_keys
DEEPSEEK_API_KEY=sk-your-deepseek-api-key-here

# Anthropic API key（备选——如果直接使用 Anthropic API）
# ANTHROPIC_API_KEY=sk-ant-your-anthropic-api-key-here

# DeepSeek API 基础 URL（默认已指向 DeepSeek Anthropic 端点）
# DEEPSEEK_BASE_URL=https://api.deepseek.com/anthropic

# 默认模型（deepseek-v4-pro / deepseek-chat 等）
# DEEPSEEK_MODEL=deepseek-v4-pro

# --- 真实 LLM 验证开关 ---
# 仅当设置为 1 时，scripts/real_llm_regression.py 才会执行真实 LLM 调用。
# 此变量不设或设为其他值时，脚本拒绝执行。
# pytest 不受此变量影响——pytest 始终使用 FakeLLMAdapter。
# TIANSHU_RUN_REAL_LLM=1
```

- [ ] **Step 2: 确认 .gitignore 已包含 .env**

```bash
grep -n "\.env" .gitignore || echo "需要在 .gitignore 中添加 .env"
```

如果 `.env` 不在 `.gitignore` 中，添加一行 `.env`。

- [ ] **Step 3: 确认 .env 不在 git 追踪中**

```bash
git ls-files .env
```
期望：无输出（说明 .env 未被 git 追踪）

- [ ] **Step 4: Commit**

```bash
git add .env.example
git commit -m "feat(r8): 添加 .env.example 密钥配置模板

- 列出所有 LLM Provider 相关环境变量（DEEPSEEK_API_KEY 等）
- 说明 TIANSHU_RUN_REAL_LLM 开关的用途
- 每一行都是占位符——不含任何真实密钥
- 用户复制为 .env 后填入真实值即可使用"
```

---

### Task 3: 验证结果输出——Harness 兼容的结构化报告

**Files:**
- Modify: `scripts/real_llm_regression.py:670-705`（汇总部分）和新增 `--output` 参数

**Interfaces:**
- Consumes: 现有 `run_real_llm_regression()` 返回的 `summary: dict`
- Produces: JSON 报告文件——格式兼容 Harness 结果聚合器，含 provider / model / timestamp / per-case 明细

- [ ] **Step 1: 在 CLI 中添加 `--output` 参数**

在 `if __name__ == "__main__":` 的 argparse 部分，`--json` 参数之后添加：

```python
    parser.add_argument(
        "--output",
        default="",
        help="将验证结果写入指定 JSON 文件（Harness 兼容格式）",
    )
```

- [ ] **Step 2: 在 `run_real_llm_regression()` 末尾添加报告文件写入逻辑**

在 `if __name__ == "__main__":` 块中，`summary = run_real_llm_regression(...)` 之后、`sys.exit(...)` 之前添加：

```python
    # ── 输出 Harness 兼容的 JSON 报告 ──
    if args.output:
        import datetime
        report = {
            "harness": "real_llm_verification",
            "provider": adapter.provider_name(),
            "model": args.model or adapter._model,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "environment": {
                "has_api_key": bool(adapter._api_key),
                "base_url": adapter._base_url,
            },
            "summary": {
                "total": summary["total"],
                "passed": summary["passed"],
                "failed": summary["failed"],
                "errors": summary["errors"],
                "pass_rate": summary["pass_rate"],
                "total_tokens": summary["total_tokens"],
                "total_latency_ms": summary["total_latency_ms"],
            },
            "per_task": {},
            "failures": [
                r for r in summary["results"]
                if r["status"] != "passed"
            ],
        }
        # 按 task 分组统计
        for task_name in REGRESSION_CASES:
            task_results = [r for r in summary["results"] if r["task"] == task_name]
            task_passed = sum(1 for r in task_results if r["status"] == "passed")
            report["per_task"][task_name] = {
                "total": len(task_results),
                "passed": task_passed,
                "pass_rate": task_passed / max(len(task_results), 1),
            }
        # 写入文件
        import json as json_module
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json_module.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\n报告已写入：{output_path}", flush=True)
```

- [ ] **Step 3: 将真实 LLM 报告目录加入 .gitignore**

```bash
# 确认 .gitignore 中有 llm_reports/ 或类似目录
grep "llm_reports" .gitignore || echo "llm_reports/" >> .gitignore
```

- [ ] **Step 4: 验证 `--output` 功能（用门禁拦截的路径测试，不需要真实 API key）**

```bash
# 不设 TIANSHU_RUN_REAL_LLM 时不应写入文件（门禁拦截在 argparse 之后、output 写入之前）
python scripts/real_llm_regression.py --output /tmp/test_r8_report.json 2>&1; echo "exit: $?"
```
期望：exit code=2，不写入任何文件

- [ ] **Step 5: Commit**

```bash
git add scripts/real_llm_regression.py .gitignore
git commit -m "feat(r8): 添加 --output 参数——输出 Harness 兼容的结构化验证报告

- JSON 报告含 provider / model / timestamp / per-task 统计 / 失败详情
- 格式兼容 Harness 结果聚合器
- llm_reports/ 目录加入 .gitignore——真实 LLM 输出不进入 git"
```

---

### Task 4: 验证——全量回归 + FakeAdapter 隔离确认

**Files:**
- 不修改任何文件——仅执行验证命令

**Interfaces:**
- Consumes: 现有测试套件（`tests/llm/`, `tests/api/`, `tests/spark/`, `tests/harness/`）
- Produces: 测试结果——确认零退化，FakeAdapter 仍然是默认

- [ ] **Step 1: 运行全量后端测试**

```bash
python -m pytest tests/llm/ tests/api/ tests/spark/ tests/harness/ -q
```
期望：全部通过，零退化

- [ ] **Step 2: 确认 LLM 测试仅使用 FakeAdapter——grep 验证无真实 Adapter 导入**

```bash
# 检查 tests/ 目录下是否有任何真实 AnthropicAdapter 导入
grep -r "AnthropicAdapter" tests/ && echo "FAIL: 测试中不应导入真实 Adapter" || echo "PASS: 测试中无真实 Adapter 导入"
```

- [ ] **Step 3: 确认 TIANSHU_RUN_REAL_LLM 不在任何 pytest 配置中**

```bash
grep -r "TIANSHU_RUN_REAL_LLM" pyproject.toml pytest.ini setup.cfg conftest.py tests/ 2>/dev/null && echo "FAIL: 开关不应出现在测试配置中" || echo "PASS: 开关不出现在测试配置中"
```

- [ ] **Step 4: ruff 检查**

```bash
python -m ruff check src/ tests/ scripts/
```
期望：零告警

- [ ] **Step 5: git diff 检查**

```bash
git diff --check
```
期望：无空白警告

- [ ] **Step 6: 记录验证基线**

在 commit message 中记录当前测试数量。
确认命令：
```bash
python -m pytest tests/llm/ tests/api/ tests/spark/ tests/harness/ -q 2>&1 | tail -1
```

---

### Task 5: 文档更新——R8 风险消除 + 状态仪表盘刷新

**Files:**
- Modify: `docs/risks/phase-6-8-known-risks.md:238-241`（R8 状态行）
- Modify: `docs/current-state-and-verification-status.md:53-54`（R8 残留风险行 + Phase 进度矩阵）

**Interfaces:**
- Consumes: 无
- Produces: 更新后的风险文档和状态仪表盘

- [ ] **Step 1: 更新 `docs/current-state-and-verification-status.md`**

将第 54 行从：
```
| R8 | LLM 生产环境持续验证未配置 | C | 待 API key |
```
改为：
```
| R8 | ~~LLM 生产环境持续验证未配置~~ | 已消除 | 2026-07-05——`scripts/real_llm_regression.py` + `TIANSHU_RUN_REAL_LLM=1` 门禁就绪 |
```

同时将第 94 行的"下一步方向"中：
```
2. **生产环境 LLM 验证**——API key 配置 + 持续验证链路
```
改为：
```
2. **生产环境 LLM 验证**——已就绪（R8），`TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py --output llm_reports/verify_$(date +%Y%m%d).json`
```

- [ ] **Step 2: 更新 `docs/risks/phase-6-8-known-risks.md`**

在 R8 相关段落末尾追加 R8 消除记录（寻找 R8 最后一次出现的位置）：

```markdown
---
## R8 消除验证 ✅ 2026-07-05

- **风险等级**：C（配置级——非开发密集型）
- **完成时间**：2026-07-05
- **影响范围**：
  - `scripts/real_llm_regression.py`——添加 `TIANSHU_RUN_REAL_LLM=1` 安全门禁 + `--output` Harness 兼容报告
  - `.env.example`——新建密钥配置模板（不含真实密钥）
  - `docs/current-state-and-verification-status.md`——R8 标记为已消除
- **产出**：
  - 真实 LLM 验证链路就绪：`TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py`
  - 覆盖 4 个 task（developer_spec_parser / relationship_planner / sql_build_planner / sql_program_planner）× 8+ 用例
  - 验证最小链路：Prompt 模板 → AnthropicAdapter.invoke() → 原始 JSON → Gateway._validate_against_schema() → validation_status
  - 结果输出为 Harness 兼容 JSON 报告（含 provider / model / per-task 通过率 / token 用量 / 失败详情）
- **安全边界**：
  - ✅ API key 仅从环境变量 `DEEPSEEK_API_KEY` 或 `ANTHROPIC_API_KEY` 读取——不存在于代码中
  - ✅ `.env.example` 仅含占位符——不含真实密钥
  - ✅ 默认 pytest 继续使用 `FakeLLMAdapter`——不受 `TIANSHU_RUN_REAL_LLM` 影响
  - ✅ 真实 LLM 输出必经 `LLMGateway._validate_against_schema()` Schema 校验——不可绕过
  - ✅ 真实 LLM 调用必须有 `TIANSHU_RUN_REAL_LLM=1`——不设则脚本拒绝执行
  - ✅ `llm_reports/` 加入 `.gitignore`——真实 LLM 输出不进入版本控制
- **不可碰边界守住了**：
  - ✅ 未修改 `src/tianshu_datadev/llm/` 任何代码
  - ✅ 未修改 `tests/` 任何测试代码
  - ✅ 未修改 SQL/Spark Pipeline 业务语义
  - ✅ 未将 API key 写入代码、日志或文档
  - ✅ 未让 FakeAdapter 结果冒充真实 LLM 验证
- **如何使用（非技术人员版）**：
  1. 从 DeepSeek 平台获取 API key：https://platform.deepseek.com/api_keys
  2. 复制 `.env.example` 为 `.env`，将 `sk-your-deepseek-api-key-here` 替换为真实 key
  3. 运行：`TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py`
  4. 脚本会调用真实 DeepSeek API，验证 4 个 Prompt 模板的结构化输出约束力
  5. 结果输出到终端——如果全部通过（PASS），说明 LLM 能正确产出符合 Schema 的 JSON
  6. 这不是自动测试——是手动触发的验证命令，需要你每次明确设置 `TIANSHU_RUN_REAL_LLM=1`
- **残留风险**：无——R8 已消除。后续若需持续验证（CI/CD 中的周期性真实 LLM 调用），可基于此脚本添加 cron/定时任务。
- **状态**：✅ 已消除
```

- [ ] **Step 3: Commit**

```bash
git add docs/current-state-and-verification-status.md docs/risks/phase-6-8-known-risks.md
git commit -m "docs(r8): R8 风险消除——LLM 生产环境验证链路就绪

- R8 从残留风险表移至已消除
- 更新状态仪表盘，标注 TIANSHU_RUN_REAL_LLM=1 手动验证命令
- 详细记录安全边界、不可碰边界、使用说明"
```

---

## 验收命令

实施完成后，按顺序执行以下验收：

### 1. 后端全量回归（确认零退化 + FakeAdapter 仍是默认）

```bash
python -m pytest tests/llm/ tests/api/ tests/spark/ tests/harness/ -q
```

### 2. 代码质量检查

```bash
python -m ruff check src/ tests/ scripts/
git diff --check
```

### 3. 安全门禁验证（不需要 API key）

```bash
# 不设环境变量——应拒绝执行
python scripts/real_llm_regression.py; echo "exit: $?"

# 设为 0——应拒绝执行
TIANSHU_RUN_REAL_LLM=0 python scripts/real_llm_regression.py; echo "exit: $?"

# 设为 1——应通过门禁（然后因无 API key 报 AdapterError——预期行为）
TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py; echo "exit: $?"
```

### 4. 真实 LLM 验证（需要 API key——仅手动执行）

```bash
# 前提：.env 中已配置 DEEPSEEK_API_KEY
TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py --output llm_reports/verify_20260705.json
```

### 5. 密钥安全检查

```bash
# 确认无 API key 泄漏到代码中
grep -r "sk-" --include="*.py" --include="*.md" --include="*.yaml" --include="*.json" src/ tests/ scripts/ docs/ 2>/dev/null | grep -v ".env.example" | grep -v "site-packages" && echo "FAIL" || echo "PASS"
```

---

## A/B/C 分类

| 分类 | 内容 |
|:----:|------|
| **A** | Task 1（安全门禁——阻断级安全）——`TIANSHU_RUN_REAL_LLM=1` 门禁是硬安全边界 |
| **B** | Task 3（Harness 报告输出）——功能增强，非阻断 |
| **B** | Task 5（文档更新）——状态同步 |
| **C** | Task 2（`.env.example`）——文档/模板，无代码逻辑影响 |

## 修改范围

| 文件 | 操作 | 风险 |
|------|:----:|:----:|
| `scripts/real_llm_regression.py` | 修改——添加门禁 + `--output` 参数 | 低（仅在脚本入口+末尾添加逻辑，不改变核心验证流程） |
| `.env.example` | 新建 | 零（纯文档模板） |
| `.gitignore` | 可能追加 `llm_reports/` | 零 |
| `docs/current-state-and-verification-status.md` | 修改——R8 状态行 | 零（文档） |
| `docs/risks/phase-6-8-known-risks.md` | 追加 R8 消除记录 | 零（文档） |

**不修改的文件：**
- `src/tianshu_datadev/llm/` ——所有 LLM 基础设施代码不变
- `tests/` ——所有测试代码不变
- `src/tianshu_datadev/prompts/` ——Prompt 模板不变
- `src/tianshu_datadev/regression/` ——RegressionRunner 不变

## 密钥安全边界

| 边界 | 实现 |
|------|------|
| API key 读取位置 | 仅从环境变量 `DEEPSEEK_API_KEY` 或 `ANTHROPIC_API_KEY` 读取——由 `AnthropicAdapter.__init__()` 已有的 `os.environ.get()` 保证 |
| API key 不在代码中 | `.env.example` 仅含占位符 `sk-your-deepseek-api-key-here`——无真实值 |
| API key 不进入 git | `.env` 已在 `.gitignore` 中 |
| API key 不进入日志 | `AnthropicAdapter.invoke()` 不打印 API key——仅错误消息中包含 "API key" 文字提示 |
| API key 不进入测试快照 | pytest 使用 `FakeLLMAdapter`——不接触真实 API |
| 真实调用不可意外触发 | `TIANSHU_RUN_REAL_LLM=1` 是硬门禁——脚本入口第一行就检查 |

## 残留风险（R8 完成后）

无新增残留风险。R8 消除后，项目仅剩：
- **R7**：真实业务样本缺失——NYC Case 01 已完成（1/6），剩余 5/6 待业务方提供

## 可进入下一阶段

R8 完成后，建议进入：
1. **R7 推进**——若业务方已提供更多 NYC 样本（Case 02-05），推进 9A4 批次
2. **SqlProgram 多语句 DAG**——若需解锁 Case 06 跨域融合（C 类阻塞，需 SqlProgram 多语句 DAG 支持多步管线间的依赖编排）

---

## 非技术人员摘要

这一步是在确认"系统真的能安全调用外部 AI 服务"，但密钥不能进代码，真实调用也不能变成默认测试。

具体来说：
- **密钥从哪里来**：你从 AI 服务商（如 DeepSeek）获取一串密钥，放在一个叫 `.env` 的文件里（这个文件不会被上传到代码仓库）
- **怎么验证**：运行一行命令 `TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py`——系统会调用真实 AI 服务，让它按照我们定义好的规则输出结果，然后检查结果格式是否正确
- **为什么不默认运行**：因为 AI 服务按调用次数收费——我们只在需要验证的时候手动运行，平时的自动化测试用模拟数据
- **安全在哪**：命令前面的 `TIANSHU_RUN_REAL_LLM=1` 是一个"钥匙"——不拧这把钥匙，命令就拒绝执行，防止误操作
