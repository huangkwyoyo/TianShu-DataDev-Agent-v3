# Phase 9A：生产级串联方案——从桥接验证到生产级 SQL Pipeline 升级

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**日期：** 2026-07-05 | **状态：** ✅ 全部完成（9A1-9A3 + 9A4 NYC 01-06 + 9A5 均已完成并回归通过）

**目标：** 将 D4 LOGIC_EQUIVALENCE 从"确定性桥接函数"升级为"真实 SQL Pipeline 全链路串联"，同时将 Harness Runner 从结果聚合器升级为自动评测驱动器。本阶段为实施计划文档——不进入代码实现。

**架构：** 5 个子阶段拆分——9A1（SQL Pipeline 中间产物导出）→ 9A2（桥接函数替换为真实 SqlBuildPlan）→ 9A3（Harness Runner 自动驱动器）→ 9A4（真实业务样本端到端验证）→ 9A5（REVIEW_READY 终验 + 风险收口）。每个子阶段独立可测、独立可回滚。

**基线：** 552 passed / 11 skipped，ruff 零告警，C1-C4 桥接级全部点亮。

---

## 桥接级 vs 生产级 SQL Pipeline 验证的差异

| 维度 | 桥接级（当前） | 生产级（目标） |
|------|---------------|---------------|
| **SqlBuildPlan 来源** | `contract_to_sql_steps()`——从 Contract 逆向确定性映射 | `Pipeline.run_all()`——DeveloperSpec → Parser → SpecEnricher → SqlBuildPlanBuilder |
| **Join 推理** | 无——Contract 中的 join_relationships 已固化 | RelationshipPlanner——LLM 提候选 → Validator 定级 STRONG/MEDIUM/WEAK/NONE |
| **指标推断** | 无——Contract 中的 aggregations 已固化 | SpecEnricher——从业务描述推断聚合函数 + 窗口指标 + 计算指标 |
| **字段归一化** | 简单点分隔符拆分（`od.amount` → `amount`） | FieldNormalizer——大小写统一 + 驼峰转下划线 + 别名字典 |
| **Validator 门禁** | 无——桥接步骤跳过 Validator | SqlBuildPlanValidator——类型安全检查 + PerfContract REJECT/WARN |
| **Contract 流向** | Contract → 桥接函数 → steps | DeveloperSpec → Pipeline → SqlBuildPlan → ContractExtractor → Contract |
| **Harness 评测** | 测试代码手动执行 Mapper/Compiler/Comparator，结果填入 `case.passed` | `HarnessRunner.evaluate()` 自动调用 Orchestrator，读取 `StageResult` 判定 |
| **双向追溯** | 无——桥接产物不可追溯回原始 DeveloperSpec | SqlBuildPlan.hash → Contract.hash → SparkPlan.hash → Comparator.report——全链路可追溯 |

**一句话总结**：桥接级验证了"同一份结构化合同两边生成结果是否对得上"，生产级验证了"从原始项目书到 SQL + Spark 双管线最终产物的全链路一致性"。

---

## A/B/C 风险分类

| 编号 | 子阶段 | 等级 | 说明 |
|:----:|--------|:----:|------|
| 9A1 | SQL Pipeline 中间产物导出 | A | 纯数据流改造——Pipeline 已有的 SqlBuildPlan + Contract 内部缓存，仅需暴露导出接口 |
| 9A2 | 桥接函数替换 | B | 替换 `contract_to_sql_steps()` 为真实 SqlBuildPlan，涉及 Orchestrator 的 sql_plan 注入路径 |
| 9A3 | Harness Runner 自动驱动器 | B | 从 `case.passed` 布尔聚合升级为自动执行 Orchestrator + 读取 `StageResult` |
| 9A4 | 真实业务样本验证 | C | 依赖业务方提供 6 个企业场景的 DeveloperSpec + 快照数据，不在本阶段控制范围内 |
| 9A5 | REVIEW_READY 终验收 | B | Snapshot Builder + 双引擎 Executor + 自动交叉验证全串联 |
| — | 全局约束 | — | 不得改 Spark Validator/SQL 安全边界、不引入真实 LLM/生产数据/凭据（9A4 除外，且仅样本数据） |

---

## 全局约束

- **允许**：修改 `api/pipeline.py`（仅新增导出方法，不改现有逻辑）
- **允许**：修改 `spark/orchestrator.py`（替换 `contract_to_sql_steps()` 调用路径）
- **允许**：修改 `harness/spark_eval.py`（升级 `evaluate()` 为自动驱动器）
- **允许**：修改 `spark/contract_sql_bridge.py`（标记为 deprecated，保留兼容）
- **允许**：新增 `tests/spark/` 下的测试（每个子阶段独立测试文件或扩展现有文件）
- **允许**：更新 `docs/risks/phase-6-8-known-risks.md`（风险等级随子阶段推进更新）
- **禁止**：删除 `contract_to_sql_steps()`（保留向后兼容——已有 3 个 D4 测试依赖它）
- **禁止**：修改 `spark/validator.py` 的安全边界（AST 硬门禁 E601-E608 白名单）
- **禁止**：修改 `sql/compiler.py` 的 SQL 生成逻辑
- **禁止**：修改 `planning/sql_build_plan.py` 的 SqlBuildPlanBuilder 构建逻辑
- **禁止**：引入真实 LLM 调用（pytest 使用 Fake Adapter）
- **禁止**：引入生产数据源凭据或生产数据库连接
- **测试策略**：每个子阶段独立 TDD——先写失败测试，再实现，再回归全量
- **测试基线**：每个子阶段完成后运行 `python -m pytest tests/spark/ tests/artifacts/ -q`，零退化
- **回滚方式**：每个子阶段独立分支（`feature/spark_first`），merge 前可独立 revert
- **文档**：所有注释使用中文

---

## 子阶段拆分

### 9A1：SQL Pipeline 中间产物导出（A 类——数据流改造）

**目标**：`Pipeline.run_all()` 内部已产生 SqlBuildPlan + DataTransformContractV1 + SqlArtifact 等中间产物，当前仅缓存于 `self._results` 内存字典中。本阶段新增 `export_artifacts(request_id)` 方法，将指定 request_id 的中间产物序列化为结构化 dict——供后续 Orchestrator 和 Harness Runner 消费。

**输入 artifact**：
- `DeveloperSpec`（Markdown 文本）→ `Pipeline.run_all(markdown_text)`
- `Pipeline._results[request_id]`（内存缓存）→ `{"parsed_spec", "manifest", "plan", "compiled", "trace", "summary", "contract"}`

**输出 artifact**：
- `PipelineArtifactBundle`（新 Pydantic 模型）：
  ```python
  class PipelineArtifactBundle(StrictModel):
      request_id: str
      spec_hash: str
      sql_build_plan: SqlBuildPlan          # 真实 SQL Pipeline 产出的 plan
      data_transform_contract: DataTransformContractV1  # 从 plan 确定性抽取
      compiled_sql: CompiledSql             # Compiler 产物
      execution_trace: ExecutionTrace       # DuckDB 执行追踪
      result_summary: ResultSummary         # 执行结果摘要
  ```

**修改范围**：
- `src/tianshu_datadev/api/pipeline.py`：新增 `export_artifacts(request_id) -> PipelineArtifactBundle | None`
- `tests/spark/test_spark_eval.py` 或新文件 `tests/test_pipeline_export.py`：新增导出测试

**不可碰边界**：
- 不改 `Pipeline.run_all()` / `execute()` / `build_plan()` 的现有逻辑
- 不改 `_results` 的写入时机和内容
- 不新增文件 I/O（纯内存 → dict 映射）

**测试策略**：
1. 单表 DeveloperSpec → `run_all()` → `export_artifacts()` → 验证 bundle 中各字段非空、hash 一致
2. 多表 + Join DeveloperSpec → 同上，额外验证 `data_transform_contract.join_relationships` 非空
3. 未执行 `run_all()` 的 request_id → 返回 None
4. TTL 过期清理后 → 返回 None

**验收命令**：
```bash
python -m pytest tests/test_pipeline_export.py -v --tb=short  # 新测试
python -m pytest tests/spark/ tests/artifacts/ -q              # 全量回归
python -m ruff check src/tianshu_datadev/api/ tests/
git diff --check
```

**回滚方式**：`export_artifacts()` 为纯新增方法，无现有逻辑修改——直接 revert。

---

### 9A2：桥接函数替换为真实 SqlBuildPlan（B 类——Orchestrator 注入路径变更）

**目标**：Orchestrator 的 COMPARATOR 阶段当前从 `contract_to_sql_steps()` 获取 SqlBuildPlan step 列表，替换为从真实 SQL Pipeline（`Pipeline.run_all()` → `export_artifacts()`）获取的 SqlBuildPlan。将桥接函数保留为 fallback 路径（`deprecated` 标记）。

**输入 artifact**（来自 9A1）：
- `PipelineArtifactBundle.sql_build_plan`——真实 SQL Pipeline 产出的 SqlBuildPlan 实例

**输出 artifact**：
- `PlanComparisonReport`——与现有格式一致，但 sql_plan 来源从桥接函数切换为真实 Pipeline

**修改范围**：
- `src/tianshu_datadev/spark/contract_sql_bridge.py`：函数 docstring 加 `@deprecated` 标记，说明替换路径
- `src/tianshu_datadev/spark/orchestrator.py`：`run()` 方法的 `sql_plan` 参数不变，但在 `test_spark_eval.py` 中修改调用方——传入 `bundle.sql_build_plan` 而非 `contract_to_sql_steps(contract)`
- `tests/spark/test_orchestrator.py`：新增集成测试——用真实 SQL Pipeline 产出的 SqlBuildPlan 驱动 COMPARATOR
- `tests/spark/test_spark_eval.py`：D4 测试的 sql_plan 来源从 `contract_to_sql_steps` 切换为 `Pipeline.run_all() → export_artifacts()`

**不可碰边界**：
- 不删除 `contract_to_sql_steps()`（已有 3 个 D4 测试依赖它）
- 不改 `PlanComparator.compare()` 接口和逻辑
- 不改 `Orchestrator._run_comparator()` 的内部实现
- 不改 `SqlBuildPlanBuilder` 构建逻辑
- 不引入真实 LLM——`Pipeline` 的 `adapter=None`（Fake 模式），SpecEnricher 走纯规则推断

**测试策略**：
1. 新建 `test_orchestrator_with_real_sql_pipeline`：单表 DeveloperSpec → Pipeline.run_all() → export_artifacts() → Orchestrator.run(contract, sql_plan=bundle.sql_build_plan) → COMPARATOR SUCCESS
2. 新建 `test_d4_with_real_sql_pipeline`：同上，纳入 Harness 5 维度报告，D4 的 case.passed 由 Orchestrator StageResult 决定而非手动填入
3. 向后兼容：`contract_to_sql_steps()` 仍可正常调用（已有 3 个测试不受影响）
4. 回归全量：552 passed 基线不变

**验收命令**：
```bash
python -m pytest tests/spark/test_orchestrator.py tests/spark/test_spark_eval.py -v --tb=short
python -m pytest tests/spark/ tests/artifacts/ -q
python -m ruff check src/tianshu_datadev/spark/ tests/spark/
git diff --check
```

**回滚方式**：`Orchestrator.run()` 的 `sql_plan` 参数接口不变——调用方切回 `contract_to_sql_steps(contract)` 即可回到桥接级。

---

### 9A3：Harness Runner 自动评测驱动器（B 类——被动聚合 → 主动评测）

**目标**：`SparkHarnessRunner.evaluate()` 当前仅统计预置的 `case.passed` 布尔值，不自动执行任何评测逻辑。升级为——`runner.add_case(case)` 后，`runner.evaluate()` 自动调用 `Orchestrator.run()` → 读取 `SparkPipelineState.stage_results` → 自动判定 `passed`。

**关键设计决策**：
- Harness Runner 注入 `Pipeline` + `Orchestrator` 实例，而非自己实现评测逻辑
- 评测判定规则（确定性）：
  - D1（CONTRACT_FIDELITY）：`state.stage_results["MAPPER"] == "SUCCESS"`
  - D2（COMPILATION_DETERMINISM）：`state.stage_results["COMPILER"] == "SUCCESS"` + 三次编译 raw_hash 全等
  - D3（VALIDATOR_COVERAGE）：`state.stage_results["VALIDATOR"] == "SUCCESS"` 或 FAILURE 时错误码符合预期
  - D4（LOGIC_EQUIVALENCE）：`state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT`
  - D5（PHYSICAL_CONSISTENCY）：`state.stage_results["PHYSICAL_VERIFIER"] == "SUCCESS"`
- Harness Runner 不替代测试框架——它产出结构化报告，测试代码断言报告内容

**输入 artifact**：
- `SparkEvalCase`（增强：新增 `developer_spec_md` 字段——完整 DeveloperSpec Markdown 文本）
- `Pipeline` 实例（Fake 模式）
- `Orchestrator` 实例

**输出 artifact**：
- `SparkEvalReport`（增强：每 case 新增 `stage_results` 字段——从 Orchestrator 透传的阶段结果，供测试断言）

**修改范围**：
- `src/tianshu_datadev/harness/spark_eval.py`：
  - `SparkEvalCase` 新增 `developer_spec_md: str = ""` 可选字段
  - `SparkHarnessRunner` 新增 `__init__(self, pipeline: Pipeline, orchestrator: SparkOrchestrator)`
  - `evaluate()` 改为自动执行：对每个 case → Pipeline.run_all(developer_spec_md) → export_artifacts() → Orchestrator.run(contract, sql_plan) → 读取 stage_results 判定 passed
  - 保留手动模式：`evaluate(passive=True)` 恢复旧行为（仅聚合 `case.passed`）
- `tests/spark/test_spark_eval.py`：新增自动驱动器测试 + 被动模式向后兼容测试

**不可碰边界**：
- 不改 `SparkEvalReport` 的公共字段结构（仅增加，不删除）
- 不改 `Orchestrator.run()` 和 `Pipeline.run_all()` 的接口
- 不引入真实 LLM、生产数据、真实 Spark（Orchestrator 的 PHYSICAL_VERIFIER 仍 SKIPPED）

**测试策略**：
1. 自动模式：单表 DeveloperSpec → HarnessRunner.evaluate() → 报告 total_passed >= 4（MAPPER + COMPILER + VALIDATOR + COMPARATOR 至少 SUCCESS）
2. 被动模式：旧 `case.passed = True` → `evaluate(passive=True)` → 通过率 100%（与旧行为一致）
3. D4 自动判定：有 sql_plan 时 COMPARATOR SUCCESS → passed=True；缺 sql_plan 时 → passed=False
4. 回归全量：552 passed 基线不变

**验收命令**：
```bash
python -m pytest tests/spark/test_spark_eval.py -v --tb=short
python -m pytest tests/spark/ tests/artifacts/ -q
python -m ruff check src/tianshu_datadev/harness/ tests/spark/
git diff --check
```

**回滚方式**：`evaluate(passive=True)` 保留旧行为——调用方切换参数即可回到桥接级聚合模式。

---

### 9A4：真实业务样本端到端验证（C 类——需业务方配合）

**前置条件（阻塞项）**：
1. **真实 DeveloperSpec 样本**：6 个企业场景（单表聚合/两表 Join/多表 Join + 聚合/窗口 TopN/CASE 标签分类/多步骤加工）的完整 DeveloperSpec Markdown 项目书
2. **快照数据**：对应的 DuckDB 可读 CSV/Parquet 数据样本（关系一致、不可变）
3. **预期结果**：每条 SQL 的"正确输出"——供 Comparator 对照

**当前状态**：三项均缺失。所有 552 个测试使用手工构造 Contract/Mock 数据。本子阶段为**阻塞-待业务方**，本轮计划仅定义验收标准。

**目标**（解阻塞后）：
- 每场景一条端到端测试：DeveloperSpec → Pipeline.run_all() → export_artifacts() → HarnessRunner.evaluate() → 5 维度报告
- 5 维度报告中至少 D1/D2/D4 通过（D3 因样本数据不含恶意代码不适用，D5 因快照环境不适用）

**不可碰边界**：
- 业务数据不进入 git（.gitignore 配置 `tests/data/business_samples/`）
- 业务样本仅用于 pytest，不进入 Harness 长期存储
- 不因样本缺失阻塞 9A1/9A2/9A3 的推进

**修改范围**：
- `tests/data/business_samples/`（新建目录，.gitignore）
- `tests/spark/test_business_e2e.py`（新建，参数化 6 场景）

---

### 9A5：REVIEW_READY 终验收 + 风险收口（B 类——全链路串联 + Snapshot Builder 集成）

**目标**：在 9A1-9A4 基础上，将 Snapshot Builder + 双引擎 Executor + 自动交叉验证串联为一条 `Pipeline.run_all()` 调用即可触发的全链路流程。终点状态为 `REVIEW_READY`（材料完整，可进入人工代码审查）。

**前置条件**：
- 9A1（Pipeline 中间产物导出）已完成
- 9A2（桥接函数替换）已完成
- 9A3（Harness Runner 自动驱动器）已完成

**输入 artifact**：
- DeveloperSpec Markdown + 快照数据路径（table_paths）

**输出 artifact**：
- `ReviewPackage`（增强：新增 `spark_orchestrator_state` 字段——含 PlanComparisonReport + SparkPipelineState）
- 验证状态：`REVIEW_READY`——当 SQL Pipeline 和 Spark Pipeline 均 `ALL_CONSISTENT` 时

**修改范围**：
- `src/tianshu_datadev/api/pipeline.py`：`run_all()` 的可选 `spark_orchestrator` 注入——提供时在 Contract + Package 之后执行 Spark 管线
- `src/tianshu_datadev/spark/review_package.py`：增强 `SparkReviewPackage` 模型——新增 `orchestrator_state: SparkPipelineState`
- `tests/test_review_ready_e2e.py`（新建）：端到端 REVIEW_READY 验收测试

**不可碰边界**：
- 不改 `Pipeline.run_all()` 的现有逻辑路径（Spark 管线为可选附加步骤）
- 不改 Snapshot Builder 的数据源连接逻辑
- 不引入生产数据源凭据

**验收命令**：
```bash
python -m pytest tests/test_review_ready_e2e.py -v --tb=short
python -m pytest tests/ -q
python -m ruff check src/ tests/
git diff --check
```

---

## 建议阶段划分与执行顺序

```
Phase 9A（本轮计划——不实现代码）
    │
    ├─ 9A1: SQL Pipeline 中间产物导出（A 类）
    │   依赖：无（Pipeline 已有内部缓存）
    │   产出：PipelineArtifactBundle + export_artifacts()
    │   风险：低——纯数据流改造
    │
    ├─ 9A2: 桥接函数替换为真实 SqlBuildPlan（B 类）
    │   依赖：9A1（需要 PipelineArtifactBundle）
    │   产出：Orchestrator 使用真实 SqlBuildPlan 驱动 COMPARATOR
    │   风险：中——替换核心对比链路，需向后兼容
    │
    ├─ 9A3: Harness Runner 自动驱动器（B 类）
    │   依赖：9A2（需要真实 SqlBuildPlan 供自动判定）
    │   产出：evaluate() 自动执行 Orchestrator + 读取 StageResult
    │   风险：中——改变 Harness 执行模型，被动模式保留
    │
    ├─ 9A4: 真实业务样本（C 类——阻塞-待业务方）
    │   依赖：9A3（需要自动驱动器跑样本）+ 业务方提供样本
    │   产出：6 条端到端企业场景测试
    │   风险：高——不具自主可控性
    │
    └─ 9A5: REVIEW_READY 终验收（B 类）
        依赖：9A2 + 9A3（桥接替换 + 自动驱动器）
        产出：全链路 Pipeline.run_all() → REVIEW_READY
        风险：中——跨度大，涉及 Snapshot + 双引擎
```

**建议执行策略**：
1. **9A1 先行**（A 类，低风险，可立即开工）
2. **9A2 + 9A3 并行规划、串行执行**（B 类，9A3 依赖 9A2 的接口但可提前设计）
3. **9A4 不阻塞 9A5**——9A5 可用手工构造样本先完成 REVIEW_READY 流程验证
4. **每子阶段独立分支 + 独立 review + 独立回归**

---

## 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R5 | `contract_to_sql_steps()` 桥接函数与真实 SqlBuildPlan 可能存在结构差异（如 required_columns 空列表 vs 完整列清单）→ Comparator 对比结果可能变化 | B | 9A2 执行时逐项对照，差异记录到风险登记表 |
| R6 | Harness Runner 自动驱动器依赖 Pipeline + Orchestrator 实例注入——若注入失败或超时，evaluate() 需优雅降级 | B | 9A3 实现 try/except + SKIPPED 标记，不崩溃 |
| R7 | 真实业务样本缺失——9A4 完全阻塞于业务方 | C | 不阻塞 9A1/9A2/9A3/9A5，手工构造样本先行 |
| R8 | LLM 生产环境持续验证未配置 | C | 不阻塞任何 9A 子阶段，Fake Adapter 覆盖全部 pytest |
| R9 | `Pipeline.adapter=None` 时 SpecEnricher 走 FakeSpecEnricher（纯规则），与真实 LLM 推断结果可能存在差异——生产级 Pipeline 需要 adapter 注入 | C | 9A1-9A3 均使用 Fake 模式；真实 LLM 验证待 API key 后独立进行 |
| R10 | Snapshot Builder 关系一致快照抽取尚未与双引擎 Executor 串联——9A5 REVIEW_READY 需要此能力 | B | 9A5 前置条件——需先确认 Snapshot Builder 接口是否可直接调用 |

---

## 非技术解释

> **为什么本轮先写计划而不是直接改代码？**
>
> 现在的系统像一个"所有零件都单独跑通了，连起来也做了桥接测试"的发动机——我们知道火花塞能点火、油泵能供油、活塞能运动，桥接线也证明了两边的零件能对上。但真正启动发动机需要把所有零件按生产流程串联起来：从油箱（DeveloperSpec）→ 油泵（Parser）→ 喷油嘴（SpecEnricher）→ 点火（SqlBuildPlanBuilder）→ 燃烧室（Compiler）→ 排气（Executor），然后自动检测每个环节是否正常工作。
>
> 这一步会动到主干流程，涉及 5 个子系统。如果直接开工，改一处可能引发三处连锁故障。先画施工图（本计划），再按图一个子系统一个子系统地升级，每个升级完立即验证，比一次性大拆大建更稳。这个施工图也让我们清楚哪些是我们可以自己做的（9A1/9A2/9A3/9A5），哪些需要等业务方配合（9A4），不会一头扎进去才发现卡在半路。

---

## 计划文件路径

`docs/superpowers/plans/07-phase-9a-production-pipeline-plan.md`

## 是否建议进入 Phase 9B 实施

**建议先评审本计划**，确认以下问题后再进入实施：

1. **9A1→9A2→9A3→9A5 的执行顺序是否合理？** 是否有其他依赖关系遗漏？
2. **9A4（真实业务样本）的阻塞风险**：如果业务方无法在短期内提供样本，是否接受用"手工构造样本 + REVIEW_READY 流程验证"作为 Phase 9 的退出标准？
3. **R10（Snapshot Builder 串联）**：Snapshot Builder 当前是否已有可直接调用的接口？如果没有，是否需要在 9A5 之前新增一个子阶段？

评审通过后，建议用 `superpowers:subagent-driven-development` 按 9A1 → 9A2 → 9A3 → 9A5 的顺序逐子阶段执行，每阶段独立 review + 独立回归。
