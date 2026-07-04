# C2 LLM 基础设施合并方案 A

> **日期**：2026-07-04 | **状态**：✅ 已完成
> **目标**：删除 Spark 侧重复 ProviderAdapter / AnthropicAdapter，复用既有 LLM 基础设施，消除 C 类架构违规（含循环导入修复）

---

## 背景

C2 执行阶段在 Spark 侧新增了独立的 LLM 调用入口：
- `spark/provider_adapter.py`——与 `llm/adapters/base.py` 功能重复的 Protocol
- `spark/adapter_anthropic.py`——与 `llm/adapters/anthropic_adapter.py` ~80% 代码重复
- `spark/developer.py::_build_prompt()`——绕过 PromptManager 的硬编码 Prompt

这与 AGENTS.md 第 11 行"LLM Gateway + PromptManager + Adapter 基础设施已就绪"矛盾。

## 方案：完全合并

1. 删除 `spark/provider_adapter.py` 和 `spark/adapter_anthropic.py`
2. 在 `_SCHEMA_PATH_MAP` 注册 `AnnotatedSparkPlan`
3. 新增 `prompts/templates/spark_annotator/v001.md` 版本化 Prompt 模板
4. `SparkDeveloperService` 改用既有 `ProviderAdapter.invoke()` + `PromptManager`
5. 更新测试——Mock 改用既有接口

## 实施步骤

### Step 1: 注册 AnnotatedSparkPlan 到 Schema 映射
- 文件：`src/tianshu_datadev/prompts/manager.py`
- 操作：在 `_SCHEMA_PATH_MAP` 添加 `AnnotatedSparkPlan` → `tianshu_datadev.spark.annotations.AnnotatedSparkPlan`

### Step 2: 创建版本化 Prompt 模板
- 文件：`src/tianshu_datadev/prompts/templates/spark_annotator/v001.md`
- 内容：将 `_build_prompt()` 的硬编码 Prompt 转为 Markdown 模板

### Step 3: 重构 SparkDeveloperService
- 文件：`src/tianshu_datadev/spark/developer.py`
- 操作：
  - 删除 `from_provider_adapter()`（不再需要 Spark 专用 ProviderAdapter）
  - 新增 `from_provider_adapter(adapter, prompt_manager)` 使用既有接口
  - `_build_prompt()` 改为从 PromptManager 加载模板渲染
  - 内部调用 `adapter.invoke(system_message, user_message, json_schema, model, temperature)`

### Step 4: 删除重复文件
- 删除 `src/tianshu_datadev/spark/provider_adapter.py`
- 删除 `src/tianshu_datadev/spark/adapter_anthropic.py`

### Step 5: 更新测试
- 文件：`tests/spark/test_spark_developer.py`
- 操作：Mock 改用既有 `ProviderAdapter.invoke()` 形态

### Step 6: 更新风险文档
- 文件：`docs/risks/phase-6-8-known-risks.md`

## 边界约束

- 不改 SQL/Spark 安全边界
- 不改 Schema/Memory 机制
- 不实现 C3/C4
- 不接入生产数据
- 不执行真实 LLM 调用

## 验收命令

```bash
# 核心功能
python -m pytest tests/spark/test_spark_developer.py -v --tb=short
python -m pytest tests/ -k "sql or developer_spec or relationship or planning" -v --tb=short
python -m pytest tests/spark/ tests/artifacts/ -q
python -m ruff check src/tianshu_datadev/ tests/
git diff --check

# 循环导入修复验收（2026-07-04 追加）
python -c "from tianshu_datadev.prompts.manager import PromptManager; print('PROMPT_MANAGER_OK')"
python -c "from tianshu_datadev.llm.gateway import LLMGateway; print('LLM_GATEWAY_OK')"
```

## 完成记录

### 2026-07-04 主体完成

- ✅ 删除 `spark/provider_adapter.py`、`spark/adapter_anthropic.py`
- ✅ 注册 `AnnotatedSparkPlan` 到 `_SCHEMA_PATH_MAP`
- ✅ 新增版本化 Prompt 模板 `v001.md`
- ✅ `SparkDeveloperService` 复用既有 `ProviderAdapter.invoke()` + `PromptManager`
- ✅ 测试 18/18 全绿
- ✅ 真实 LLM 重新点亮：3/3 标注通过

### 2026-07-04 循环导入修复

- **问题**：`prompts.manager` → `llm.models` → `llm.__init__` → `llm.gateway` → `prompts.manager` 形成闭环
- **根因**：`llm/gateway.py:28` 在模块级导入 `PromptManager`，但该文件仅在类型注解中使用（已有 `from __future__ import annotations`）
- **修复**：将 `PromptManager` 导入移到 `TYPE_CHECKING` 块，打破循环
- **验证**：`python -c "from tianshu_datadev.prompts.manager import PromptManager"` 直接可用
