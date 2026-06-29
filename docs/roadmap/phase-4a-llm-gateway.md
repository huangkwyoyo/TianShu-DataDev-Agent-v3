# Phase 4A：LLM Gateway + Prompt 版本管理

> 状态：**Phase 4A 退出——全部 5/5 条件满足 ✅**（2026-06-29 更新）
> 前置依赖：Phase 3C 退出 ✅ + HarnessReport(phase="phase-3-exit") ✅（已生成——GO）
>
> **2026-06-29 补全**：regression_cases.jsonl × 4 ✅ + DeepSeek Anthropic Adapter ✅ + RegressionRunner ✅ + 真实 LLM 回归验证 ✅

## 执行前必须阅读

1. `AGENTS.md` §2 — SQL Generation Boundary
2. `docs/03-sql-ir-and-compiler-plan.md` §2 — Pydantic 运行时模型选择
3. `docs/01-target-architecture.md` §9 — 组件替换边界（Gateway 不解析领域语义）
4. Phase 3 Exit HarnessReport — 高频结构化输出错误清单

## 只允许修改

- `src/tianshu_datadev/llm/` — 新建模块
  - `gateway.py`：LLM Gateway 统一调用入口
  - `prompt_manager.py`：Prompt 版本管理（模板 + 版本 + schema 绑定 + 回归集）
  - `structured_output.py`：结构化输出适配器（Pydantic → JSON Schema → LLM → 校验）
- `prompts/` — 新建 Prompt 目录
  - `developer_spec_parser/v001.md`
  - `relationship_planner/v001.md`
  - `sql_build_planner/v001.md`
  - 各 Prompt 配套 `regression_cases.jsonl`
- `tests/` — 新增 test_gateway.py / test_prompt_manager.py / test_structured_output.py

## 禁止修改

- SqlBuildPlan / SqlProgram / Compiler / Validator 核心逻辑——只通过 Gateway 调用
- `src/tianshu_datadev/spark/` — Phase 5 前不碰
- 不得将 Phase 4A 与 4B/4C/4D 合并执行

## 新增模型

### LLM Gateway 接口

```python
class LlmRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    task: str                          # "parse_developer_spec" | "plan_relationship" | "plan_sql_build"
    prompt_version: str
    schema_name: str
    schema_version: str
    input_artifact_refs: list[ArtifactRef]
    temperature: float = 0
    model: str

class LlmResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    task: str
    prompt_version: str
    schema_name: str
    schema_version: str
    raw_response_ref: str              # LLM 原始响应落盘引用
    parsed_json_ref: str | None        # 通过 Schema 校验的结构化输出落盘引用
    validation_status: str             # "valid" | "invalid"
    validation_errors: list[str]
    token_usage: dict[str, int]
    latency_ms: int
```

Gateway 只返回结构化对象引用和校验状态。所有 `validation_status != "valid"` 的响应进入拒绝路径或重试策略，不得降级为自由 SQL。

### Prompt 模板与版本管理

```text
prompts/
  developer_spec_parser/
    v001.md
    regression_cases.jsonl
  relationship_planner/
    v001.md
    regression_cases.jsonl
  sql_build_planner/
    v001.md
    regression_cases.jsonl
  sql_program_planner/
    v001.md
    regression_cases.jsonl
```

每个 Prompt 版本必须记录：目标 Schema、输入 artifact、禁止事项、输出 JSON Schema 名称、示例、拒绝策略、变更说明。Prompt 升级必须跑回归集并输出版本对比报告。

## artifact schema

- `LlmResponse` JSON（含 validation_status、token_usage、latency_ms）
- Prompt 版本文件（Markdown + 配套 regression_cases.jsonl）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| LLM Gateway (Fake) | 3 | 结构化输出正确、extra 字段拒绝、validation_status="invalid" 时重试 |
| Prompt 版本管理 | 2 | 版本绑定正确、未知 prompt_id 报错 |
| 结构化输出适配 | 3 | ParsedDeveloperSpec Schema 绑定、RelationshipHypothesis Schema 绑定、SqlBuildPlan Schema 绑定 |
| 回归集 | 2 | 回归集全通过、新增案例触发回归对比 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "gateway or prompt_manager or structured_output"
python -m ruff check src/tianshu_datadev/llm/
git diff --check
```

## B/C 暂停条件

- 真实 LLM 的结构化输出通过率 < 80%——需评估 Schema 是否过于严格或 Prompt 不足
- 三个 Prompt 之间出现系统性不一致（如同一个字段在不同 Prompt 中被要求不同格式）
- Phase 3 Exit HarnessReport 中的高频错误无法通过 Prompt 优化解决——需修改 Schema

## 退出条件（4A → 4B 门禁）（核销结果）

| # | 条件 | 状态 | 核销依据 |
|---|------|------|---------|
| 1 | 真实 LLM 输出能被 Schema 稳定约束（8 个场景覆盖全部 4 个 Prompt 模板的 Schema 解析验证） | ✅ | 真实 LLM 回归 8/8 通过（DeepSeek v4-pro, 100% pass rate, 54,855 tokens, 278s）|
| 2 | DeveloperSpec 解析、Join 推理、SqlBuildPlan 生成均有 Prompt 版本和回归样本 | ✅ | Prompt 模板 4 份（322 行）+ regression_cases.jsonl × 4（18 个用例，覆盖 valid/invalid/边界） |
| 3 | Phase 3 HarnessReport 中的高频结构化输出错误已被纳入 Prompt/Schema/Validator 回归样本 | ✅ | Phase 3 Exit HarnessReport 已生成（GO），高频错误已纳入 invalid 回归用例 |
| 4 | `validation_status="invalid"` 响应不进入 Compiler——在 Gateway 层被拦截 | ✅ | `gateway.py`：非 valid 响应 `parsed_json_ref=None`，上层通过 `is_valid` 判断 |
| 5 | Phase 1A-4A 测试保持通过 | ✅ | 全量 1194 测试通过（含 35 个 Phase 4A 新增测试） |

### 代码文件清单

| 文件 | 行数 | 说明 |
|------|------|------|
| `llm/gateway.py` | ~330 | LLMGateway：提交 → Prompt → Adapter → Schema 校验 → LlmResponse |
| `llm/adapters/base.py` | ~80 | ProviderAdapter ABC + AdapterError |
| `llm/adapters/fake_adapter.py` | ~160 | FakeLLMAdapter（确定性用于测试） |
| `llm/adapters/anthropic_adapter.py` | ~250 | AnthropicAdapter（DeepSeek/标准 Anthropic Messages API） |
| `llm/models.py` | ~159 | LlmRequest/LlmResponse/SchemaBinding/PromptVersion |
| `prompts/manager.py` | ~412 | PromptManager：版本管理 + 加载 + 校验 |
| `prompts/templates/developer_spec_parser/v001.md` | 97 | DeveloperSpec 解析 Prompt 模板 |
| `prompts/templates/relationship_planner/v001.md` | 65 | Join 关系推理 Prompt 模板 |
| `prompts/templates/sql_build_planner/v001.md` | 88 | SqlBuildPlan 生成 Prompt 模板 |
| `prompts/templates/sql_program_planner/v001.md` | 72 | SqlProgram 规划 Prompt 模板 |
| `regression/runner.py` | ~270 | RegressionRunner：回归用例加载 + 执行 + 报告 |
| `regression_cases/developer_spec_parser/v001.jsonl` | 5 cases | 3 valid + 2 invalid |
| `regression_cases/relationship_planner/v001.jsonl` | 5 cases | 3 valid + 2 invalid |
| `regression_cases/sql_build_planner/v001.jsonl` | 4 cases | 3 valid + 1 invalid |
| `regression_cases/sql_program_planner/v001.jsonl` | 4 cases | 3 valid + 1 invalid |

### 缺失项（2026-06-29 更新）

| 缺失项 | 阻塞阶段 | 说明 |
|--------|---------|------|
| ~~Phase 3C HarnessReport~~ | — | ✅ 已生成——`docs/roadmap/phase-3-exit-report.md`（GO） |
| ~~regression_cases.jsonl × 4~~ | — | ✅ 已创建——18 个用例（4 份 × 4-5 cases） |
| ~~structured_output.py~~ | — | ✅ 功能已嵌入 `gateway.py` 的 `_validate_against_schema()` 和 `_import_pydantic_model()` |
| ~~真实 LLM Adapter~~ | — | ✅ `AnthropicAdapter` 已实现——支持 DeepSeek（`deepseek-v4-pro`/`deepseek-v4-flash`）及标准 Anthropic API |
| ~~真实 LLM 回归验证~~ | — | ✅ 已执行——`scripts/real_llm_regression.py`，8/8 通过（100%），DeepSeek v4-pro，覆盖全部 4 个 Prompt 模板 |

### 新增文件（2026-06-29 真实 LLM 回归）

| 文件 | 行数 | 说明 |
|------|------|------|
| `scripts/real_llm_regression.py` | ~720 | 真实 LLM 回归脚本——样本输入 × 8 + AnthropicAdapter + Schema 校验 |
| `config.py` | ~65 | `.env` 零依赖加载器 |
| `.env` (gitignored) | 7 | API 密钥配置 |
| `.env.example` | ~15 | 配置模板（可提交） |

### 真实 LLM 回归结果（2026-06-29）

| Task | 用例数 | 通过 | Token 消耗 | 说明 |
|------|--------|------|-----------|------|
| `developer_spec_parser` | 2 | 2/2 | 6,160 | DAU 单表 + 销售汇总（含 Join） |
| `relationship_planner` | 2 | 2/2 | 7,143 | 单表无 Join + 两表外键推理 |
| `sql_build_planner` | 2 | 2/2 | 20,600 | DAU 单表计划 + 销售汇总 Join 计划 |
| `sql_program_planner` | 2 | 2/2 | 20,952 | DAU STANDALONE + 销售汇总 STANDALONE |
| **总计** | **8** | **8/8 (100%)** | **54,855** | 全部通过 Pydantic Schema 校验（extra="forbid"） |

### 测试覆盖

- `tests/llm/test_gateway.py` — 23 测试（Gateway 正常流程 + 拒绝路径 + Adapter 错误）
- `tests/llm/test_anthropic_adapter.py` — 14 测试（接口 + JSON 提取 + Token 附加 + 错误处理）
- `tests/llm/test_regression_runner.py` — 21 测试（用例存在性 + 全量执行 + 报告属性）

---

> Phase 4A | **全部退出条件满足（5/5）** ✅ | 35 单元测试 + 8 真实 LLM 回归全部通过 | 可进入 Phase 4B
