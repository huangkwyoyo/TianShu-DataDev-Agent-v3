# Phase 4A：LLM Gateway + Prompt 版本管理

> 状态：待实施
> 前置依赖：Phase 3C 退出 + HarnessReport(phase="phase-3-exit")

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

## 退出条件（4A → 4B 门禁）

1. 真实 LLM 输出能被 Schema 稳定约束（10 个基础 DeveloperSpec 通过解析、规划、编译和执行）
2. DeveloperSpec 解析、Join 推理、SqlBuildPlan 生成均有 Prompt 版本和回归样本
3. Phase 3 `HarnessReport(phase="phase-3-exit")` 中的高频结构化输出错误已被纳入 Prompt/Schema/Validator 回归样本
4. `validation_status="invalid"` 响应不进入 Compiler——在 Gateway 层被拦截
5. Phase 1A-3C 测试保持通过

---

> Phase 4A | 待实施 | 前置：Phase 3C + HarnessReport(phase="phase-3-exit") | 下一阶段：Phase 4B
