# C2 文档收口 + C3/C4 前置澄清与执行方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**日期：** 2026-07-04 | **状态：** 方案中

**目标：** 收口 C2 后遗留的文档表述问题，明确 C3/C4 每项前置条件的满足状态，输出可执行的 C3/C4 点亮计划或阻塞报告。

**架构：** 本轮分两个 Phase——Phase A 做纯文档层面的 C2 表述修正（2 个文件），Phase B 做 C3/C4 前置条件逐项核查并输出执行方案。不写新功能代码。

**基线：** 527 passed, 11 skipped, ruff clean, C1 已点亮, C2 已收口（含循环导入修复）。

---

## 全局约束

- **允许**：修正 `docs/risks/phase-6-8-known-risks.md` 中 C2 的"真实 LLM 已点亮"过度表述
- **允许**：同步 `docs/superpowers/plans/03-business-integration-round1.md` 中已过时的 C2 描述
- **允许**：调查 C3/C4 前置条件（代码阅读 + 接口分析），输出执行方案
- **禁止**：接入真实 LLM、使用生产数据、写生产库
- **禁止**：绕过 Validator / Comparator / Executor
- **禁止**：改变 SQL/Spark 安全边界、Schema/Memory/Prompt 机制
- **禁止**：恢复 `spark/provider_adapter.py` 或 `spark/adapter_anthropic.py`
- **禁止**：实现 C3/C4 代码（只规划，不实现）

---

## 能力清单

### ✅ 本轮之前已完成

| 能力 | 证据 |
|------|------|
| C1 真实 Spark 点亮 | 11/11 TestRealSparkExecution passed |
| C2 架构合并 | `spark/provider_adapter.py` + `spark/adapter_anthropic.py` 已删除 |
| C2 版本化 Prompt | `prompts/templates/spark_annotator/v001.md` 已创建 |
| C2 循环导入修复 | `python -c "from tianshu_datadev.prompts.manager import PromptManager"` 直接可用 |
| C2 真实 LLM 验证（Dev 环境） | DeepSeek (`deepseek-v4-pro`) 3/3 标注通过——开发环境一次性验证 |
| SQL 管线 SqlBuildPlan 产出 | `SqlBuildPlanBuilder` 已实现，90+ 测试全覆盖 |
| PlanComparator 9 种规则 | `test_plan_comparator.py` 测试已全绿 |
| Harness 5 维度框架 | `SparkEvalCase` + `SparkEvalDimension` + `SparkHarnessRunner` 已定义 |

### ⚠️ 本轮要做

| 项目 | 类型 | 说明 |
|------|------|------|
| C2 文档表述修正 | A 类小修 | 2 个文件的"真实 LLM 已点亮"措辞精确化 |
| 03-business-integration-round1.md 同步 | A 类小修 | C2 已从"待实现"变为"已完成" |
| C3 前置条件核查 | 方案输出 | SqlBuildPlan 可用性 + SparkPlan 可用性对照 |
| C4 前置条件核查 | 方案输出 | 5 维度逐项可用/阻塞判定 |

### ❌ 本轮不做

| 项目 | 原因 |
|------|------|
| C3 Comparator 代码实现 | 只规划，不实现 |
| C4 Harness 代码实现 | 只规划，不实现 |
| 真实 LLM 生产验证 | 需生产环境 API key |

---

## Task 1: C2 文档表述收口——修正"真实 LLM 已点亮"过度表述

**背景**：C2 架构合并后，使用开发环境的 DeepSeek API key（`deepseek-v4-pro`）完成了一次性的 LLM 验证（3/3 标注通过）。但该验证为开发环境一次性行为，不应表述为"真实 LLM 已点亮"——后者暗示生产环境持续性验证链路已就绪。应将措辞精确化为"结构化路径已点亮（mock 可回归），真实 LLM 开发环境一次性验证通过，生产 LLM 持续验证待 API key"。

### Step 1: 修正 `docs/risks/phase-6-8-known-risks.md`

**文件**：`docs/risks/phase-6-8-known-risks.md`

**位置 1**：第 63 行——C2 验收证据列表

修改前：
```markdown
  - ✅ **真实 LLM 重新点亮**：3/3 标注通过（source/clean/shape），step_id 全正确，AnnotationValidator 零拒绝
```

修改后：
```markdown
  - ✅ **结构化路径已点亮**：3/3 mock 标注通过（source/clean/shape），step_id 全正确，AnnotationValidator 零拒绝
  - ⚠️ **真实 LLM 开发环境验证通过**：DeepSeek (`deepseek-v4-pro`) 一次性验证 3/3 标注正确——生产环境持续验证链路待 API key 配置后启用
```

**位置 2**：第 67 行——C2 状态行

修改前：
```markdown
- **状态**：✅ 已收口并重新点亮——C2 架构风险消除（含循环导入修复），Spark 管线 LLM 调用复用既有统一基础设施，PromptManager 可作为公共入口直接导入
```

修改后：
```markdown
- **状态**：✅ 已收口——C2 架构风险消除（含循环导入修复），Spark 管线 LLM 调用复用既有统一基础设施。结构化路径（mock）可回归验证；真实 LLM 开发环境一次性验证通过（DeepSeek 3/3），生产环境持续验证待 API key 配置
```

**位置 3**：第 4 行——文档头部状态摘要

修改前：
```markdown
> 状态：C1 已点亮（11/11），C2 架构已收口（复用 llm.adapters + PromptManager），C3/C4 等待前置条件
```

修改后：
```markdown
> 状态：C1 已点亮（11/11），C2 架构已收口（复用 llm.adapters + PromptManager，mock 路径可回归），C3/C4 等待前置条件
```

### Step 2: 同步 `docs/superpowers/plans/03-business-integration-round1.md`

**文件**：`docs/superpowers/plans/03-business-integration-round1.md`

该文档记录的是 Round 1 执行情况。Round 1 时 C2 还处于"方案制定"阶段，但该文档的前瞻性描述（"下一轮执行""待实现"）已过时——C2 已完成。应在文档末尾追加"后续更新"章节，而不修改原文档的历史记录部分。

**追加内容（在文档末尾 `~~~` 之后）**：

```markdown
---

## 后续更新（2026-07-04）

### C2 已完成（2026-07-04）

本文档 §Task 2 中制定的 C2 ProviderAdapter 接入方案已被**方案 A（完全合并）**替代：

- **原方案**（`04-c2-provider-adapter-plan.md`）：新建 `spark/provider_adapter.py` + `spark/adapter_anthropic.py`
- **实际执行**（`05-c2-llm-boundary-consolidation.md`）：删除重复文件，复用既有 `llm/adapters/base.py::ProviderAdapter` + `PromptManager`
- **原因**：C2 初始实现引入了与既有 LLM 基础设施 ~80% 重复的 Spark 专用 Adapter——属 C 类架构违规，需合并而非新建
- **完成状态**：
  - ✅ 重复文件已删除：`spark/provider_adapter.py`、`spark/adapter_anthropic.py`
  - ✅ 版本化 Prompt 模板：`prompts/templates/spark_annotator/v001.md`
  - ✅ 循环导入修复：`llm/gateway.py` → `TYPE_CHECKING` 延迟导入
  - ✅ 结构化路径 18/18 测试全绿（mock 可回归）
  - ⚠️ 真实 LLM 开发环境一次性验证通过（DeepSeek `deepseek-v4-pro` 3/3）——生产环境持续验证待 API key

### 原 §"是否可继续 C2？" 更新

- ~~C2 Task 1-4 实现~~ → **已完成**（通过方案 A 完全合并）
- ~~C2 mock 路径全通后，等待 API key~~ → **mock 路径已全通**，真实 LLM 开发环境验证已通过
- **当前状态**：C2 全部阻塞项已消除，可进入 C3/C4 评估

### 原 §残留风险 更新

| 风险 | 原状态 | 当前状态 |
|------|--------|---------|
| C2 方案未实现 | B-待执行 | ✅ 已完成（通过方案 A） |
| C2 真实 LLM 验证 | C-环境依赖 | ⚠️ 开发环境已验证，生产环境待 API key |
```

---

## Task 2: C3/C4 前置条件逐项核查

### 2.1 C3 Comparator——前置条件调查结果

**C3 核心问题**：同一 `DataTransformContractV1` → 同时产出 `SqlBuildPlan`（SQL 管线）和 `SparkPlan`（Spark 管线）→ 送入 `PlanComparator.compare()`。

**逐项核查：**

| 前置条件 | 状态 | 证据 |
|---------|------|------|
| `SqlBuildPlan` 模型已定义 | ✅ 已就绪 | `src/tianshu_datadev/planning/sql_build_plan.py:204` |
| `SqlBuildPlanBuilder` 可构造 SqlBuildPlan | ✅ 已就绪 | `SqlBuildPlanBuilder.build_from_steps()` 已实现，90+ 测试覆盖 |
| `PlanComparator.compare()` 接口 | ✅ 已就绪 | `src/tianshu_datadev/spark/plan_comparator.py`，9 种 step 规则 |
| 同一 Contract → SqlBuildPlan 的代码路径 | ✅ 存在 | SQL 管线：`DataTransformContractV1` → `SpecEnricher` → `RelationshipPlanner` → `SqlBuildPlanBuilder` |
| 同一 Contract → SparkPlan 的代码路径 | ✅ 存在 | Spark 管线：`DataTransformContractV1` → `ContractToSparkMapper` → `SparkPlan` |
| **同一 Contract → 同时产出两者的 E2E 路径** | ⚠️ **未接通** | 两条管线独立运行，无统一的"同一 Contract 同时驱动两条管线"的编排节点 |
| Comparator 测试中 SqlBuildPlan + SparkPlan 同步构造 | ✅ 存在 | `tests/spark/test_plan_comparator.py`——手工构造两者后 compare() |

**阻塞判定**：C3 的前置条件**技术上已全部满足**——SQL 管线可产出 SqlBuildPlan，Spark 管线可产出 SparkPlan，PlanComparator 已就绪。阻塞点在于**编排层面**：没有一条代码路径能对同一 Contract 同时驱动两条管线并将产出送入 Comparator。

**点亮路径**（不在本轮实现）：

1. 在 `SparkOrchestrator` 或新增的 Comparator 集成测试中：
   - 输入：一个 `DataTransformContractV1`
   - SQL 管线：`Contract → SpecEnricher → SqlBuildPlanBuilder → SqlBuildPlan`
   - Spark 管线：`Contract → ContractToSparkMapper → SparkPlan`
   - 输出：`PlanComparator.compare(spark_plan, sql_plan)`
2. 验收：9 种 step 类型的 SQL ↔ Spark 语义等价性全部 PASS

**最低可行点亮**：选择一个已同时覆盖两套管线的 Contract（如 `tests/spark/test_plan_comparator.py` 中手工构造的），写一个集成测试串联 `SqlBuildPlanBuilder.build_from_steps()` 和 `ContractToSparkMapper.map_contract_to_spark_plan()` 并送入 `PlanComparator.compare()`。

### 2.2 C4 Harness——前置条件逐维调查结果

**C4 核心问题**：5 维度评测框架已定义，但 `SparkHarnessRunner.evaluate()` 当前是结果聚合器（统计预置 `case.passed` 布尔值），不执行真实编译/验证/对比。

**逐维核查：**

#### D1: CONTRACT_FIDELITY（Contract → SparkPlan 字段完整性）

| 前置条件 | 状态 | 说明 |
|---------|------|------|
| Mapper 可运行 | ✅ | `ContractToSparkMapper.map_contract_to_spark_plan()` 已就绪 |
| 样本 Contract | ⚠️ 需准备 | 需 1 个覆盖全部 9 种 step 的 Contract fixture |
| 评判逻辑 | ⚠️ 需实现 | 需定义"字段完整性"的具体检查项（step 数量、类型、别名等） |

**可点亮性**：✅ **可立即点亮**——Mapper 已就绪，只需准备样本 Contract 和定义评判规则。不依赖 C1/C3。

#### D2: COMPILATION_DETERMINISM（同一 Plan 多次编译相同 hash）

| 前置条件 | 状态 | 说明 |
|---------|------|------|
| Compiler 可运行 | ✅ | `SparkPlanCompiler.compile()` 已就绪 |
| 样本 SparkPlan | ⚠️ 需准备 | 需 1 个含 5+ step 的 SparkPlan |
| 评判逻辑 | ⚠️ 需实现 | 3 次编译 → raw_hash 全等 |

**可点亮性**：✅ **可立即点亮**——Compiler 已就绪，不依赖 C1/C3。

#### D3: VALIDATOR_COVERAGE（Validator 对恶意代码的检测率）

| 前置条件 | 状态 | 说明 |
|---------|------|------|
| Validator 已实现 | ✅ | `SparkCodeValidator` 已就绪，E601-E608 错误码完整 |
| 恶意代码样本集 | ⚠️ 需系统整理 | 现有 `test_spark_validator.py` 已有部分覆盖，需归入 Harness 框架 |
| 评判逻辑 | ⚠️ 需实现 | 注入恶意代码 → 验证 error_code 正确拒绝 |

**可点亮性**：✅ **可立即点亮**——Validator 已就绪且有现成测试可迁移。不依赖 C1/C3。

#### D4: LOGIC_EQUIVALENCE（SQL ↔ Spark 逻辑对比）

| 前置条件 | 状态 | 说明 |
|---------|------|------|
| C3 Comparator 就绪 | ⚠️ 需 C3 先点亮 | 见 §2.1 C3 前置条件 |
| 同一 Contract 的双管线产出 | ⚠️ 需 C3 先点亮 | 同上 |

**可点亮性**：❌ **阻塞于 C3**——C3 点亮后 D4 可直接跟进。

#### D5: PHYSICAL_CONSISTENCY（双引擎物理结果一致）

| 前置条件 | 状态 | 说明 |
|---------|------|------|
| C1 已点亮 | ✅ | 11/11 TestRealSparkExecution passed |
| 样本 Contract | ⚠️ 需准备 | 需 1 个覆盖 scan+filter+project 的 Contract fixture |

**可点亮性**：✅ **可立即点亮**——C1 已点亮，DuckDB ↔ PySpark 对比链路已验证。

#### C4 汇总

| 维度 | 可点亮性 | 阻塞项 | 点亮优先级 |
|------|:---:|------|:---:|
| D1 CONTRACT_FIDELITY | ✅ 可立即点亮 | 样本 Contract + 评判规则 | **P0** |
| D2 COMPILATION_DETERMINISM | ✅ 可立即点亮 | 样本 SparkPlan + 评判规则 | **P0** |
| D3 VALIDATOR_COVERAGE | ✅ 可立即点亮 | 恶意代码样本整理 | **P0** |
| D4 LOGIC_EQUIVALENCE | ❌ 阻塞于 C3 | C3 Comparator 未点亮 | P1 |
| D5 PHYSICAL_CONSISTENCY | ✅ 可立即点亮 | 样本 Contract | **P0** |

**关键发现**：5 个维度中 4 个可以立即点亮（D1/D2/D3/D5）——不依赖外部环境，不依赖 SQL pipeline，不依赖真实 LLM。D4 需等 C3 先点亮。

---

## Task 3: C3/C4 执行方案

### 3.1 C3 点亮计划（3 步，预计 1 轮执行）

**前置条件**：本 Task 不在本轮实现——仅输出执行计划供后续使用。

1. **Step 1**：选择一个已同时覆盖 SQL 和 Spark 管线的 Contract fixture（如 `acceptance_r3_e2e`）
2. **Step 2**：在 `tests/spark/test_plan_comparator.py` 中新增集成测试：
   - `test_contract_to_sql_and_spark_then_compare()`：同一 Contract → SQL 管线产出 `SqlBuildPlan` + Spark 管线产出 `SparkPlan` → `PlanComparator.compare()` → 验证 9 种 step 全部等价
3. **Step 3**：若 Comparator 发现不等价 → 分类（语义差异 / 数据差异 / 接口差异），记录为已知差异或修复

**验收**：
```bash
pytest tests/spark/test_plan_comparator.py -v --tb=short
# 预期：全部 PASS，含新增的 E2E 集成测试
```

### 3.2 C4 点亮计划（4 步，预计 2 轮执行）

**前置条件**：本 Task 不在本轮实现——仅输出执行计划供后续使用。

**第一轮（P0 维度：D1/D2/D3/D5——不依赖 C3）：**

1. **Step 1（D1）**：准备 1 个 9-step Contract fixture → 定义 CONTRACT_FIDELITY 评判规则（step 数量匹配、类型匹配、别名匹配）→ 接入 `SparkHarnessRunner`
2. **Step 2（D2）**：准备 1 个 5+ step SparkPlan → 定义 COMPILATION_DETERMINISM 评判规则（3 次编译 raw_hash 全等）→ 接入 `SparkHarnessRunner`
3. **Step 3（D3）**：整理 `test_spark_validator.py` 中已有的恶意代码用例 → 归入 Harness EvalCase 格式 → 定义 VALIDATOR_COVERAGE 评判规则（error_code 匹配）→ 接入 `SparkHarnessRunner`
4. **Step 4（D5）**：基于 C1 已验证的 11 个用例 → 选出 1 个（scan+filter+project）→ 接入 Harness EvalCase 格式 → 定义 PHYSICAL_CONSISTENCY 评判规则（DuckDB ↔ PySpark 结果一致）

**第二轮（P1 维度：D4——依赖 C3 先点亮）：**

5. **Step 5（D4）**：C3 点亮后 → 使用同一 Comparator 集成测试的 Contract → 接入 Harness → 定义 LOGIC_EQUIVALENCE 评判规则

**验收**：
```bash
pytest tests/spark/test_spark_eval.py -v --tb=short
# 预期：4/5 维度 PASS（第一轮），5/5 维度 PASS（C3 点亮后第二轮）
```

---

## 验收命令

```bash
# 1. 核心导入验证（循环导入修复持续有效）
python -c "from tianshu_datadev.prompts.manager import PromptManager; print('PROMPT_MANAGER_OK')"
python -c "from tianshu_datadev.llm.gateway import LLMGateway; print('LLM_GATEWAY_OK')"

# 2. Spark Developer 测试
python -m pytest tests/spark/test_spark_developer.py -v --tb=short

# 3. 全量回归
python -m pytest tests/spark/ tests/artifacts/ -q

# 4. Lint
python -m ruff check src/tianshu_datadev/ tests/

# 5. Git diff
git diff --check
```

**预期结果**：
- `PROMPT_MANAGER_OK` + `LLM_GATEWAY_OK`
- `test_spark_developer.py`：18 passed
- 全量回归：527 passed, 11 skipped，零退化
- ruff：All checks passed!
- git diff --check：clean（仅 CRLF 警告）

---

## 退出标准

- [ ] `docs/risks/phase-6-8-known-risks.md`：C2 的"真实 LLM 已点亮"修正为精确表述
- [ ] `docs/superpowers/plans/03-business-integration-round1.md`：追加 C2 已完成状态更新
- [ ] C3 前置条件逐项核查完成，点亮路径已定义
- [ ] C4 5 维度逐项核查完成，P0/P1 分轮执行计划已定义
- [ ] 全量回归 527 passed, 11 skipped
- [ ] ruff 零告警

---

## 是否可进入 C3/C4 执行阶段？

**是——C3 和 C4（P0 维度）均具备执行条件。** 具体而言：

| 项目 | 可执行性 | 说明 |
|------|:---:|------|
| **C3 Comparator** | ✅ 可执行 | SqlBuildPlan + SparkPlan 均可产出，PlanComparator 已就绪——只需串联 E2E 测试 |
| **C4 D1/D2/D3/D5** | ✅ 可执行 | 4/5 维度不依赖外部环境，样本可用现有 Contract fixture |
| **C4 D4** | ⚠️ 等 C3 | C3 点亮后 D4 可立即跟进 |

**建议执行顺序**：
1. 本轮（Phase A）：完成 C2 文档收口（Task 1）
2. 下一轮：C3 点亮（PlanComparator 同 Contract 双管线集成测试）
3. 再下一轮：C4 P0 维度点亮（D1/D2/D3/D5），D4 在 C3 后跟进

---

## A/B/C 分类汇总

| 分类 | 内容 | 处置 |
|------|------|------|
| **A（本轮修复）** | C2 文档"真实 LLM 已点亮"过度表述修正 | 2 个文件精确化措辞 |
| **A（本轮修复）** | 03-business-integration-round1.md C2 状态同步 | 追加"后续更新"章节 |
| **B（本轮方案）** | C3 Comparator 前置条件核查 + 点亮计划 | 核查完成，3 步点亮路径已定义 |
| **B（本轮方案）** | C4 Harness 5 维度逐项核查 + 分轮计划 | 核查完成，4/5 可立即点亮 |
| **B（本轮方案）** | 本方案文档 | `06-c3-c4-prerequisite-clarification.md` |
| **C（不实现）** | C3 代码实现 | 下一轮执行 |
| **C（不实现）** | C4 代码实现 | 下一轮执行（P0 优先，D4 等 C3） |
| **C（不实现）** | 真实 LLM 生产验证 | 需生产环境 API key |

---

## 修改范围

| 文件 | 操作 | 说明 |
|------|------|------|
| `docs/risks/phase-6-8-known-risks.md` | 修改 | C2"真实 LLM 已点亮"→精确表述（3 处） |
| `docs/superpowers/plans/03-business-integration-round1.md` | 修改 | 追加"后续更新"章节，同步 C2 已完成状态 |
| `docs/superpowers/plans/06-c3-c4-prerequisite-clarification.md` | **新建** | 本文档——C3/C4 前置澄清与执行方案 |

---

## 非技术人员解释

**这轮做了什么？**

C2（AI 大脑接口）的架构改造已经全部完成——重复零件删掉了，线路统一了，还修好了一个"先有鸡还是先有蛋"的死锁问题。但这轮主要是"改标签"：

1. **把墙上的"已通电"标签换成更诚实的说法**：之前用开发环境的临时电池点亮过一次（3 盏灯全亮），但真正的生产电源还没接上。现在标签写的是"测试电池验证通过——等正式电源接入"。

2. **更新了旧图纸**：第一轮的记录里写着"C2 方案待实现"，现在补上了"已完成（但走的是合并方案，不是新建方案）"的脚注——后面来的师傅不会以为 C2 还没做。

3. **检查了第三台（C3）和第四台（C4）机器的零件清单**：
   - **C3（双线对比机）**：两边（SQL 线和 Spark 线）的零件都在，对比仪也装好了——就差把两根线同时插上同一个原料口跑一次。技术上完全可行，下轮就能做。
   - **C4（5 维度质检台）**：5 个检测位中 4 个马上就能用（原料检查、稳定性检查、安全门检查、物理一致性检查），不依赖任何外部条件。只有 1 个（逻辑等价性检查）需要等 C3 先跑通。

**总结**：所有阻塞都解除了——没有"缺零件""缺工具""缺权限"的问题。下一轮可以直接开始点亮 C3（双线对比）和 C4 的 4 个检测位。
