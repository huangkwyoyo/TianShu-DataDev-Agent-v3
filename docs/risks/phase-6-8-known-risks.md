# Phase 6-8 已知风险登记

> 日期：2026-07-04 | 最后更新：2026-07-04 C3/C4 业务集成第二轮——桥接生产化 + P0 覆盖确认
> 状态：C1 已点亮（11/11），C2 架构已收口（复用 llm.adapters + PromptManager，mock 路径可回归），C3 测试级已点亮（桥接生产化），C4 P0 已点亮（D1/D2/D3/D5）

---

## C1: 真实 Spark 物理验证 ✅ 已点亮

- **风险等级**：已消除（2026-07-04 点亮）
- **发现阶段**：Phase 7B | **点亮阶段**：业务集成执行第一轮
- **影响范围**：`tests/spark/test_physical_verifier.py::TestRealSparkExecution` 全部 11 个参数化用例
  - scan / filter / project / sort / limit / aggregate / join / case_when / window_row_number / window_sum_over / window_rank
- **环境信息**：
  - PySpark 4.1.2 | Python 3.12.10 | Windows 11 Pro
  - Java: OpenJDK 64-Bit Server VM (build 25.442-b08)
  - SparkSession: `local[1]` 模式，创建和销毁正常
- **验收证据**（2026-07-04）：
  - 69/69 全部通过（含 11/11 `TestRealSparkExecution` 参数化用例 + 58 个 DuckDB 安全/mock 测试）
  - 全量回归 521 passed, 11 skipped，零退化
  - 双引擎（DuckDB ↔ PySpark）结果一致性 100%
- **验收命令**：`pytest tests/spark/test_physical_verifier.py -v --run-slow --tb=short`
- **PYSPARK_PYTHON 可复现性**：
  - 当前环境 PySpark 启动时有 `python3` 未找到警告（PySpark 默认查找 `python3`，Windows 上仅有 `python`）
  - **不影响功能**——PySpark 自动回退到 `python`，SparkSession 正常创建
  - **推荐设置**（消除警告）：`set PYSPARK_PYTHON=python` + `set PYSPARK_DRIVER_PYTHON=python`（Windows cmd）
  - 已在测试命令中通过内联环境变量 `PYSPARK_PYTHON=python PYSPARK_DRIVER_PYTHON=python` 消除警告
- **状态**：✅ 已点亮——11/11 真实 Spark 物理验证通过，C1 风险消除

---

## C2: LLM 基础设施架构收口 ✅ 已收口

- **风险等级**：已消除（2026-07-04 架构收口）
- **发现阶段**：Phase 8 | **点亮阶段**：业务集成执行 C2 | **收口阶段**：C2 架构边界合并
- **影响范围**：`src/tianshu_datadev/spark/developer.py::SparkDeveloperService`
- **问题背景**（2026-07-04）：
  - C2 初始实现引入了 Spark 专用 LLM 调用入口——`spark/provider_adapter.py`（ProviderAdapter Protocol）和 `spark/adapter_anthropic.py`（AnthropicAdapter）
  - 与既有 LLM 基础设施（`llm/adapters/base.py::ProviderAdapter`、`llm/adapters/anthropic_adapter.py::AnthropicAdapter`）功能重复 ~80%
  - `_build_prompt()` 绕过 PromptManager 硬编码 Prompt，无法版本管理和审计
  - 违反 AGENTS.md §11 "LLM Gateway + PromptManager + Adapter 基础设施已就绪"
- **收口方案（方案 A——完全合并）**：
  - ✅ 删除 `src/tianshu_datadev/spark/provider_adapter.py`（重复的 Protocol + ProviderConfig + ProviderError）
  - ✅ 删除 `src/tianshu_datadev/spark/adapter_anthropic.py`（重复的 httpx AnthropicAdapter）
  - ✅ 在 `prompts/manager.py::_SCHEMA_PATH_MAP` 注册 `AnnotatedSparkPlan`
  - ✅ 新增 `prompts/templates/spark_annotator/v001.md`——版本化 Prompt 模板（Markdown + YAML frontmatter）
  - ✅ `SparkDeveloperService.from_provider_adapter()` 改用既有 `llm.adapters.base.ProviderAdapter.invoke()` + `PromptManager`
  - ✅ Prompt 安全保证不变——模板不含 SQL 关键字，不引用 DeveloperSpec/SqlBuildPlan
  - ✅ 集成测试 18/18 全绿（Mock 改用既有接口）
- **架构对齐后的 LLM 调用链路**：
  ```
  SparkDeveloperService.from_provider_adapter(adapter, prompt_manager)
    → prompt_manager.get_prompt("spark_annotator", "v001")    # 版本化 Prompt 模板
    → adapter.invoke(system_message, user_message, json_schema, model, temperature)  # 既有统一接口
    → AnnotatedSparkPlan.model_validate(raw_output)            # Pydantic 校验
    → AnnotationValidator.validate()                           # 确定性业务校验
  ```
- **验收证据**（2026-07-04 收口 + 重新点亮）：
  - ✅ 重复文件已删除：`spark/provider_adapter.py`、`spark/adapter_anthropic.py`
  - ✅ 版本化 Prompt 模板：`prompts/templates/spark_annotator/v001.md`（含元数据 + 禁止事项）
  - ✅ Schema 映射注册：`AnnotatedSparkPlan` 已加入 `_SCHEMA_PATH_MAP`
  - ✅ 集成测试 18/18 全绿（含 PromptManager 模板渲染安全测试）
  - ✅ **结构化路径已点亮**：3/3 mock 标注通过（source/clean/shape），step_id 全正确，AnnotationValidator 零拒绝
  - ⚠️ **真实 LLM 开发环境验证通过**：DeepSeek (`deepseek-v4-pro`) 一次性验证 3/3 标注正确——生产环境持续验证链路待 API key 配置后启用
  - ✅ ruff 零告警
- **残留风险**：
  - ~~`llm` ↔ `prompts` 存在潜在循环导入~~ → **已于 2026-07-04 修复**：`llm/gateway.py` 中 `PromptManager` 改为 `TYPE_CHECKING` 延迟导入（模块已有 `from __future__ import annotations`，运行时无需实际类）。`python -c "from tianshu_datadev.prompts.manager import PromptManager"` 直接可用，不再需要先 import `tianshu_datadev.llm`。
- **状态**：✅ 已收口——C2 架构风险消除（含循环导入修复），Spark 管线 LLM 调用复用既有统一基础设施。结构化路径（mock）可回归验证；真实 LLM 开发环境一次性验证通过（DeepSeek 3/3），生产环境持续验证待 API key 配置

---

## C3: Comparator 真实逻辑对比

- **风险等级**：B（测试级已点亮，生产 SQL pipeline 串联待完成）
- **发现阶段**：Phase 8 全局验收 | **进化阶段**：C3/C4 业务集成第二轮（桥接生产化）
- **影响范围**：`src/tianshu_datadev/spark/plan_comparator.py::PlanComparator.compare()` + `src/tianshu_datadev/spark/contract_sql_bridge.py`
- **当前状态**：
  - `PlanComparator` 接口已完整实现，9 种 step 对比规则已就绪——26/26 测试全绿
  - `contract_to_sql_steps()` 桥接函数已生产化——从测试文件提升到 `spark/contract_sql_bridge.py`（完整类型注释 + 防御检查 + 单元测试）
  - 双管线集成测试 `test_contract_to_spark_via_mapper_then_compare_all_eight_types`：同一 Contract → Mapper 产出 SparkPlan + 桥接产出 SqlBuildPlan → Comparator 对比——8 种 step 类型全部 LOGIC_EQUIVALENT
  - Orchestrator 中 COMPARATOR 阶段仍标记 SKIPPED——桥接函数已就绪，但 Orchestrator 尚未集成（需要 sql_plan 参数传入 run() 入口）
- **阻塞项**：
  - 生产级 SQL pipeline 串联（Contract → SpecEnricher → SqlBuildPlanBuilder）属于 SQL pipeline 范围——不在 Spark pipeline 范围内
  - 桥接函数 `contract_to_sql_steps()` 提供确定性替代路径——在 SQL pipeline 就绪前可启用 Orchestrator COMPARATOR 阶段
- **下一轮行动**：
  1. Orchestrator 集成：`run()` 方法接收可选的 `sql_plan: SqlBuildPlan` 参数 → COMPARATOR 阶段接入桥接产出
  2. 桥接函数在 SQL pipeline `SpecEnricher → SqlBuildPlanBuilder` 就绪后替换
- **影响评估**：不影响骨架级验收——桥接函数提供确定性对比能力，测试级 C3 已点亮
- **处置建议**：Orchestrator 集成在下一轮业务集成中执行——修改量小（~20 行），风险低

---

## C4: Harness 真实样本评测

- **风险等级**：B（P0 维度已点亮，P1 维度阻塞于 C3 生产串联，Harness Runner 为结果聚合器）
- **发现阶段**：Phase 8 全局验收 | **进化阶段**：C3/C4 业务集成第二轮（P0 覆盖确认）
- **影响范围**：`src/tianshu_datadev/harness/spark_eval.py::SparkHarnessRunner`
- **当前状态**：
  - 5 维度评测框架已定义（CONTRACT_FIDELITY / COMPILATION_DETERMINISM / VALIDATOR_COVERAGE / LOGIC_EQUIVALENCE / PHYSICAL_CONSISTENCY）
  - **P0 已点亮**（26/26 测试全绿）：
    - D1 CONTRACT_FIDELITY：真实 Mapper 执行 → step 数量/类型/别名校验（2 个 EvalCase）
    - D2 COMPILATION_DETERMINISM：真实 Compiler 3 次编译 → raw_hash 全等（2 个 EvalCase）
    - D3 VALIDATOR_COVERAGE：真实 Validator E601-E608 错误码检测（2 个 EvalCase）
    - D5 PHYSICAL_CONSISTENCY：Compiler 产物 → Validator 前置条件验证 + C1 证据引用（3 个 EvalCase）
  - **P1 未点亮**：D4 LOGIC_EQUIVALENCE——阻塞于 C3 生产 SQL pipeline 串联（见 C3 阻塞项）
  - `SparkHarnessRunner.evaluate()` 当前为结果聚合器——统计预置 `case.passed` 布尔值，不自动执行评测逻辑
  - 评测逻辑在测试代码中手动执行（Mapper/Compiler/Validator），结果填入 EvalCase 后交 Runner 聚合
- **影响评估**：P0 维度（4/5）不依赖外部环境、不依赖 C3——测试级已点亮。Harness Runner 升级为自动评测驱动器属于 Phase 9+ 范围
- **处置建议**：P0 维度已有测试覆盖——风险等级从 C（延期）降为 B（核心能力已就绪，D4 等 C3）

---

## R3: Mapper ProjectStep input_alias 空值 Gap（已修复 ✅）

- **风险等级**：已消除（2026-07-04 修复）
- **发现阶段**：Phase 8 全局验收 Task 2
- **修复阶段**：R3 收口（业务集成前置准备）
- **影响范围**：`src/tianshu_datadev/spark/mapper.py`
- **问题描述**：
  - Mapper 对 `ProjectStep`、`CaseWhenStep`、`SortStep`、`LimitStep` 产出空的 `input_alias`
  - 导致 Compiler 的 `SparkCodeRenderer.validate_identifier()` 拒绝（正则 `^[a-zA-Z_][a-zA-Z0-9_]*$` 不匹配空字符串）
- **修复方案**：
  - 新增 `_chain_input_aliases(steps)` ——遍历已排序步骤列表，为每个步骤的 `input_alias` 填入前驱步骤的编译器输出别名
  - 新增 `_get_step_output_alias(step, index)` ——返回编译器将赋予给定步骤的输出变量名（与 compiler.py out_alias 命名规则严格一致）
  - 在 `map_contract_to_spark_plan()` 中，SparkPlan 构造前调用 `_chain_input_aliases(steps)`
- **验收证据**：
  - `test_input_alias_chain_populated_for_linear_steps` PASSED（简单线性 Contract）
  - `test_input_alias_chain_full_contract_no_empty_aliases` PASSED（完整 9 种 step）
  - `test_real_contract_e2e_mapper_compiler_validator` PASSED（真实 Contract 全链路）
  - 全量回归 521 passed, 11 skipped，零退化
- **状态**：✅ 已修复并回归通过

---

## R4: PhysicalVerifier SKIPPED 语义修正（已修复）

- **风险等级**：已消除
- **发现阶段**：Phase 8 全局验收 Task 2
- **问题描述**：`derive_overall_status()` 中 PHYSICAL_VERIFIER 的 SKIPPED 被视同"已执行"，导致全 SKIPPED 状态误判为 ALL_CONSISTENT
- **修复**：`physical_executed` 条件从 `!= "NOT_EXECUTED"` 改为 `not in ("NOT_EXECUTED", "SKIPPED")`
- **状态**：已修复并回归通过

---

## 风险矩阵

| 编号 | 等级 | 阻塞骨架验收？ | 阻塞业务集成？ | 处置时机 |
|------|------|:---:|:---:|------|
| C1 | 已消除 | — | — | 2026-07-04 点亮（11/11 真实 Spark 通过） |
| C2 | 已消除 | — | — | 2026-07-04 架构收口 + 循环导入修复（PromptManager 可直接导入） |
| C3 | B-测试级已点亮 | 否 | 否 | 桥接已生产化——Orchestrator 集成下一轮 |
| C4 | B-P0 已点亮 | 否 | 否（P0）| D4 等 C3 生产串联——P0 已覆盖 |
| R3 | 已消除 | — | — | 2026-07-04 已修复 |
| R4 | 已消除 | — | — | — |
