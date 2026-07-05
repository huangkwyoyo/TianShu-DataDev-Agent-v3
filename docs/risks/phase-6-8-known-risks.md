# Phase 6-8 已知风险登记

> 日期：2026-07-05 | 最后更新：2026-07-05 Phase 9A5 REVIEW_READY 终验收完成——全链路 Pipeline → ReviewPackage 审查级闭环
> 状态：C1-C4 全部点亮，9A1-9A3 + 9A5 已完成（9A4 阻塞-待业务方），629 passed / 11 skipped

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

- **风险等级**：A（已点亮——桥接生产化 + Orchestrator 集成完成）
- **发现阶段**：Phase 8 全局验收 | **点亮阶段**：C3/C4 业务集成第三轮（Orchestrator COMPARATOR 集成）
- **影响范围**：`src/tianshu_datadev/spark/plan_comparator.py` + `contract_sql_bridge.py` + `orchestrator.py`
- **当前状态**：
  - `PlanComparator` 接口已完整实现，9 种 step 对比规则已就绪——30/30 测试全绿
  - `contract_to_sql_steps()` 桥接函数已生产化到 `spark/contract_sql_bridge.py`
  - **Orchestrator COMPARATOR 已集成**：`run()` 接收可选 `sql_plan: SqlBuildPlan` 参数
    - 提供 `sql_plan` + `spark_plan`（Mapper 产出）→ 真实调用 `PlanComparator.compare()` → 记录 `comparator_report`
    - 缺一 → SKIPPED，错误消息精确指出缺失项
  - 31/31 orchestrator 测试全绿（含 3 个 COMPARATOR 集成测试）
- **残留风险**：
  - 桥接函数 `contract_to_sql_steps()` 是确定性映射——不经过 SQL pipeline 的 SpecEnricher 推测逻辑
  - 在 SQL pipeline（SpecEnricher → SqlBuildPlanBuilder）正式就绪前，桥接函数提供等效替代
- **下一轮行动**：
  - 桥接函数在 SQL pipeline 就绪后替换为 `SqlBuildPlanBuilder` 产出
- **状态**：✅ 已点亮——C3 风险消除（Orchestrator 集成完成）

---

## C4: Harness 真实样本评测

- **风险等级**：B（P0+P1 桥接级已全覆盖——D4 桥接级验证点亮，完整 SQL Pipeline 生产级验证待后续 Phase）
- **发现阶段**：Phase 8 全局验收 | **进化阶段**：C3/C4 业务集成第四轮（D4 桥接级点亮）
- **影响范围**：`src/tianshu_datadev/harness/spark_eval.py::SparkHarnessRunner`
- **当前状态**：
  - 5 维度评测框架已定义（CONTRACT_FIDELITY / COMPILATION_DETERMINISM / VALIDATOR_COVERAGE / LOGIC_EQUIVALENCE / PHYSICAL_CONSISTENCY）
  - **P0 已点亮**（26/26 测试全绿）：
    - D1 CONTRACT_FIDELITY：真实 Mapper 执行 → step 数量/类型/别名校验（2 个 EvalCase）
    - D2 COMPILATION_DETERMINISM：真实 Compiler 3 次编译 → raw_hash 全等（2 个 EvalCase）
    - D3 VALIDATOR_COVERAGE：真实 Validator E601-E608 错误码检测（2 个 EvalCase）
    - D5 PHYSICAL_CONSISTENCY：Compiler 产物 → Validator 前置条件验证 + C1 证据引用（3 个 EvalCase）
  - **D4 LOGIC_EQUIVALENCE 桥接级已点亮**（2026-07-04，3 个 EvalCase）：
    - 同一 DataTransformContractV1 → contract_to_sql_steps() + Mapper → PlanComparator
    - 8 种 step 类型全等价验证（scan/filter/project/sort/limit/aggregate/join/case_when）
    - 人为不一致检测验证（LOGIC_MISMATCH 正确识别）
    - 最小 Contract 边界验证
    - **注意**：这是桥接级验证——使用确定性桥接函数 `contract_to_sql_steps()` 而非完整 SQL Pipeline（SpecEnricher → SqlBuildPlanBuilder）。它验证的核心命题是"同一份结构化合同两边生成结果是否对得上"，不是完整 SQL Pipeline 的生产级验收。
  - `SparkHarnessRunner.evaluate()` 当前为结果聚合器——统计预置 `case.passed` 布尔值，不自动执行评测逻辑
  - 评测逻辑在测试代码中手动执行（Mapper/Compiler/Validator/Comparator），结果填入 EvalCase 后交 Runner 聚合
- **影响评估**：P0+P1 全 5 维度桥接级已覆盖（27/27 测试全绿，含 3 个 D4 桥接测试）。完整 SQL Pipeline 生产级验收 + Harness Runner 自动评测驱动器属于 Phase 9+ 范围
- **处置建议**：风险等级维持 B——D4 桥接级已点亮，剩余风险在 SQL Pipeline 生产级串联（非桥接）

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
| C3 | 已消除 | — | — | 2026-07-04 点亮（桥接生产化 + Orchestrator 集成） |
| C4 | B-D4 桥接级已点亮 | 否 | 否 | D4 桥接级已点亮——完整 SQL Pipeline 生产级验证待后续 Phase |
| R3 | 已消除 | — | — | 2026-07-04 已修复 |
| R4 | 已消除 | — | — | — |
| 9A1 | B-低风险 | 否 | 否 | 2026-07-05 已完成——PipelineArtifactBundle + export_artifacts() 就绪 |
| 9A2 | B-低风险 | 否 | 否 | 2026-07-05 已完成——桥接函数标记 deprecated + 真实 SqlBuildPlan 驱动 COMPARATOR |
| 9A3 | B-低风险 | 否 | 否 | 2026-07-05 已完成——Lite→V1 适配层 + Harness 自动驱动器 |
| 9A5 | B-低风险 | 否 | 否 | 2026-07-05 已完成——ReviewPackage 增强 + REVIEW_READY 判定 + 端到端闭环验证 |

---
## Phase 9A3: Harness 自动驱动器 + Lite→V1 适配收口 ✅ 已完成

- **风险等级**：B（低风险——纯数据流适配 + Harness 升级，不改核心推理链路）
- **完成时间**：2026-07-05
- **影响范围**：
  - `src/tianshu_datadev/spark/contract_adapter.py`——**新建**：`adapt_lite_to_v1()` 确定性适配函数
  - `src/tianshu_datadev/harness/spark_eval.py`——`SparkEvalCase` 新增 `developer_spec_md` 字段 + `SparkHarnessRunner` 升级为自动/被动双模式
  - `tests/spark/test_spark_eval.py`——新增 `TestContractAdapter`（3 测试）+ `TestC4AutoDrive`（4 测试）
  - `tests/spark/test_orchestrator.py`——`test_comparator_with_real_sql_pipeline_plan` 改用适配器
- **产出**：
  - `adapt_lite_to_v1(lite)` → `DataTransformContractV1`：无损确定性适配，14 个公共字段直接复制，5 个 V1 独有字段填入安全默认值
  - `SparkHarnessRunner(pipeline, orchestrator)`：注入 Pipeline + Orchestrator 后 `evaluate()` 自动执行全链路
  - `evaluate(passive=True)`：向后兼容——仅聚合预置 `case.passed`
- **验收证据**（2026-07-05）：
  - 新增 7 个测试全绿（3 适配器 + 4 自动驱动器）
  - 全量回归 617 passed, 11 skipped，零退化（+7 vs 610 基线）
  - ruff 零告警，git diff --check 干净
- **核心突破**：
  - **Lite/V1 收口**：9A2 的手工 V1 构造已全部替换为 `adapt_lite_to_v1(bundle.data_transform_contract)`
  - **自动驱动器就绪**：`TestC4AutoDrive.test_auto_drive_full_pipeline_mapper_to_comparator` 证明 Harness 可自动完成全链路评测
  - **测试零手工 V1 绕过**：`test_orchestrator.py` 和 `test_spark_eval.py` 中所有涉及真实 Pipeline 的测试均使用适配器
- **不可碰边界守住了**：
  - ✅ 未修改 DataTransformContract schema（Lite / V1 模型零改动）
  - ✅ 未修改 SQL Pipeline 的 SpecEnricher / RelationshipPlanner / SqlBuildPlanBuilder
  - ✅ 未修改 PlanComparator 核心判定规则
  - ✅ 未删除 `contract_to_sql_steps()`（保持 deprecated）
  - ✅ 未接入真实 LLM / 生产数据 / Spark 物理执行
- **残留风险**：
  - `adapt_lite_to_v1()` 的 V1 独有字段（case_when_labels / window_specs）默认为空——当真实 Pipeline 产出多语句 SqlProgram 时，`extract_v1()` 路径直接产出 V1，不需要适配器。适配器仅用于单表非 ComputeSteps 路径
  - Harness 自动驱动器的 table_paths 通过 `case.actual_result` 传递——未来可升级为正式字段
- **下一轮行动**：9A4 真实业务样本验证（阻塞-待业务方）或 9A5 REVIEW_READY 终验收
- **状态**：✅ 已完成——Lite/V1 适配收口 + Harness 自动驱动器就绪

---
## Phase 9A5: REVIEW_READY 终验收 + 审查级闭环 ✅ 已完成

- **风险等级**：B（低风险——模型增强 + 判定逻辑 + 端到端验证，不改核心推理链路）
- **完成时间**：2026-07-05
- **影响范围**：
  - `src/tianshu_datadev/spark/review_package.py`——`SparkReviewPackage` 新增 `stage_results` / `comparator_status` / `review_ready` 字段
  - `src/tianshu_datadev/spark/review_builder.py`——`SparkReviewBuilder.build()` 自动填充 9A5 字段 + 新增 `build_review_ready()` 显式 REVIEW_READY 装配 + `_compute_review_ready()` 判定方法
  - `tests/spark/test_review_package.py`——新增 `TestReviewReady`（8 测试）：字段存在性/判定逻辑/外部报告/确定性/边界条件
  - `tests/spark/test_spark_eval.py`——新增 `TestC4ReviewReady`（4 测试）：全链路闭环/确定性/缺失 contract/完整 provenance
- **产出**：
  - `SparkReviewPackage.review_ready: bool`：REVIEW_READY 判定——MAPPER + COMPILER + VALIDATOR + COMPARATOR 均为 SUCCESS + comparator 为 LOGIC_EQUIVALENT
  - `SparkReviewPackage.stage_results: dict`：透传全部 6 阶段执行结果
  - `SparkReviewPackage.comparator_status: str`：透传 ComparisonStatus 值
  - `SparkReviewBuilder._compute_review_ready()`：确定性判定方法——关键阶段全 SUCCESS + 对比器 LOGIC_EQUIVALENT → True
  - `SparkReviewBuilder.build_review_ready()`：接受外部 comparator_report 的显式装配入口
- **REVIEW_READY 判定规则**：
  1. MAPPER + COMPILER + VALIDATOR + COMPARATOR 均为 SUCCESS（DEVELOPER / PHYSICAL_VERIFIER 可 SKIPPED）
  2. comparator_status 为 LOGIC_EQUIVALENT 或 NOT_COVERED（有空对比报告时）
  3. 以上两条同时满足 → `review_ready = True`
- **验收证据**（2026-07-05）：
  - 新增 12 个测试全绿（8 模型/判定 + 4 端到端）
  - 全量回归 629 passed, 11 skipped，零退化（+12 vs 617 基线）
  - ruff 零告警，git diff --check 干净
  - 端到端验证：`test_review_ready_e2e_full_chain` 证明 DeveloperSpec → Pipeline → Orchestrator → ReviewPackage → REVIEW_READY=True 全链路闭环可复现
- **核心突破**：
  - **审查级闭环就绪**：从原始 DeveloperSpec 到 REVIEW_READY 判定的全自动化链路已打通
  - **判定确定性**：同一合约多次构建产出一致 package_id + review_ready 判定
  - **向后兼容**：`build()` 旧接口不变——已有 14 个 ReviewPackage 测试零退化
- **不可碰边界守住了**：
  - ✅ 未修改 DataTransformContract schema
  - ✅ 未修改 SQL Pipeline 的 SpecEnricher / RelationshipPlanner / SqlBuildPlanBuilder
  - ✅ 未修改 PlanComparator 核心判定规则
  - ✅ 未删除 `contract_to_sql_steps()`
  - ✅ 未接入真实 LLM / 生产数据 / Spark 物理执行
  - ✅ 未将 REVIEW_READY 表述为生产上线批准
- **REVIEW_READY 的含义（非技术语言）**：
  - **代表**：所有自动化检查已通过，材料（SqlBuildPlan + Contract + SparkPlan + PlanComparisonReport + ReviewPackage）已完整组装，可进入人工代码审查
  - **不代表**：生产上线批准、业务逻辑正确性验证、性能 SLA 承诺、安全合规认证
- **残留风险**：
  - R7：真实业务样本缺失——9A4 阻塞于业务方，当前所有测试使用手工构造样本
  - R8：LLM 生产环境持续验证未配置——Fake Adapter 覆盖全部 pytest
  - R10：Snapshot Builder 未集成到 REVIEW_READY 流程——Snapshot Builder 有独立可调用接口但 `Pipeline.run_all()` 未调用
- **下一轮行动**：Phase 9A 全部子阶段（9A1-9A3 + 9A5）已完成，9A4 继续阻塞于业务方。可进入 Phase 9B 或更高级别的集成验证
- **状态**：✅ 已完成——REVIEW_READY 审查级闭环就绪

---
## Phase 9A2: 桥接函数替换 ✅ 已完成

- **风险等级**：B（低风险——新增集成测试，不改变现有逻辑路径）
- **完成时间**：2026-07-05
- **影响范围**：
  - `src/tianshu_datadev/spark/contract_sql_bridge.py`——`contract_to_sql_steps()` 标记 `@deprecated`
  - `tests/spark/test_orchestrator.py`——新增 `test_comparator_with_real_sql_pipeline_plan`
  - `tests/spark/test_spark_eval.py`——新增 `test_d4_with_real_sql_pipeline_plan`
- **产出**：
  - Orchestrator COMPARATOR 可接收真实 SQL Pipeline 产出的 SqlBuildPlan（通过 `export_artifacts().sql_build_plan`）
  - D4 LOGIC_EQUIVALENCE 新增生产级验证用例（真实 SqlBuildPlan 驱动——而非桥接函数）
  - 桥接函数保留为向后兼容 fallback，已有 3 个桥接级 D4 测试不受影响
- **验收证据**（2026-07-05）：
  - 新增 2 个集成测试全绿（Orchestrator + 真实 SqlBuildPlan、D4 + 真实 SqlBuildPlan）
  - 既有桥接级 D4 测试 3/3 全绿（向后兼容）
  - 全量回归通过，ruff 零告警
- **关键发现**：
  - `DataTransformContractLite`（`extract(plan)` 产出）不能被 Mapper 直接消费——Mapper 需要 `DataTransformContractV1`
  - 9A2 集成测试中手动构造 `DataTransformContractV1` 供 Mapper 使用，真实 SqlBuildPlan 供 Comparator 使用
  - 这是 9A3 需要处理的适配问题——Lite → V1 转换或 Mapper 扩展
- **不可碰边界守住了**：
  - ✅ 未删除 `contract_to_sql_steps()`
  - ✅ 未修改 `Orchestrator.run()` / `_run_comparator()` 逻辑
  - ✅ 未修改 `PlanComparator.compare()` 接口
  - ✅ 未修改 `SqlBuildPlanBuilder` 构建逻辑
  - ✅ 未接入真实 LLM / 生产数据
- **残留风险**：
  - ~~`DataTransformContractLite` 与 Mapper 的类型不兼容~~ → **已于 9A3 收口**：`adapt_lite_to_v1()` 确定性适配层就绪
  - 桥接函数仍被 3 个已有 D4 测试使用——后续 Phase 可迁移到真实 Pipeline
- **下一轮行动**：已完成——9A3 Harness Runner 自动驱动器完成
- **状态**：✅ 已完成——真实 SqlBuildPlan 驱动 COMPARATOR 就绪

---
## Phase 9A1: SQL Pipeline 中间产物导出 ✅ 已完成

- **风险等级**：B（低风险——纯数据流改造，新增导出方法，不改现有执行逻辑）
- **完成时间**：2026-07-05
- **影响范围**：`src/tianshu_datadev/api/pipeline.py`（新增 `PipelineArtifactBundle` 模型 + `Pipeline.export_artifacts()` 方法）
- **产出**：
  - `PipelineArtifactBundle`——含 request_id / spec_hash / sql_build_plan / data_transform_contract / compiled_sql / execution_trace / result_summary 的结构化导出包
  - `Pipeline.export_artifacts(request_id) -> PipelineArtifactBundle | None`——从 `_results` 内存缓存导出中间产物
- **验收证据**（2026-07-05）：
  - 新增 7 个测试全绿（含 run_all 导出 / 未知 request_id / TTL 过期 / execute 导出 / build_plan 部分字段 / spec_hash 一致性 / plan_id 一致性）
  - 全量回归 608 passed, 11 skipped，零退化
  - ruff 零告警，git diff --check 干净
- **字段覆盖**：
  - `sql_build_plan`：所有 Pipeline 路径（build_plan/execute/run_all）均产出
  - `compiled_sql`：execute/run_all 单表路径产出
  - `execution_trace` / `result_summary`：execute/run_all 成功路径产出
  - `data_transform_contract`：仅 run_all ComputeSteps 路径产出（其余路径为 None）
- **不可碰边界守住了**：
  - ✅ 未修改 `run_all()` / `execute()` / `build_plan()` 的现有执行逻辑
  - ✅ 未修改 `_results` 的写入时机和内容
  - ✅ 未新增文件 I/O
  - ✅ 未引入真实 LLM / 生产数据 / 凭据
- **残留风险**：
  - ~~`data_transform_contract` 在 non-ComputeSteps 路径为 `DataTransformContractLite`，Mapper 需要 `DataTransformContractV1`~~ → **已于 9A3 收口**：`adapt_lite_to_v1()` 确定性适配层就绪
- **下一轮行动**：已完成——9A2 桥接函数替换 + 9A3 适配收口完成
- **状态**：✅ 已完成——9A1 中间产物导出就绪
