# 复用与迁移地图 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 目标

审计三个现有项目的可复用资产，制定 DeveloperSpec-first 迁移策略，记录历史教训避免重蹈覆辙。

本文只列迁移关系，不修改代码。

## 2. 现有项目审计摘要

### 2.1 TianShu（数据仓库）

| 维度 | 评估 |
|------|------|
| `contracts/*.yml` | **可复用** — 表结构、字段定义、关系约束已在此处定义 |
| 数据血缘 | 不直接复用，仅参考 |
| ETL 流程 | 不直接复用 |
| 代码风格 | 不涉及 |

**复用决定**：`contracts/*.yml` 通过 git submodule 或 symlink 引用；可选项 SchemaRegistry 以此为基础构建。其余不复用。

### 2.2 TianShu-Text2SQL-Agent（Text2SQL 项目）

| 维度 | 评估 |
|------|------|
| 安全检查逻辑 | **算法可参考**，不直接复用代码 |
| IR 设计 | 有参考价值，但需从 RequirementIR 路线改为 DeveloperSpec 路线 |
| 测试用例 | 部分可参考思路 |
| 其余代码 | 不复用 |

**经验教训**：
- `ir.py` 中 45% 字段是非核心附加属性，导致结构臃肿
- `AgentResponse` 18 个字段中 44% 是附加状态，非核心输出
- 6 层安全检查虽全面但过度设计，增加维护成本
- Prompt 与业务逻辑耦合过紧

### 2.3 TianShu Data Dev Agent（v2 / legacy）

| 维度 | 评估 |
|------|------|
| Spark 多 Agent 设计 | **算法可参考**，但需简化角色数量 |
| 文件输出打包 | 不直接复用 |
| 执行引擎 | DuckDB + PySpark 可沿用，但封装方式重新设计 |
| 其余代码 | 不复用 |

**经验教训**：
- 12 层防御体系增加了 200% 的维护成本，实际收益有限
- SubIntent 设计缺乏严格的字段契约——已被 SqlBuildPlan / SqlProgram 替代
- 交叉验证逻辑混入 LLM 判定，削弱了确定性
- 测试膨胀到 200+ 用例，大量低价值 case 增加了 CI 时间

## 3. 修订后的复用策略

| 资产类型 | 策略 | 说明 |
|----------|------|------|
| `contracts/*.yml` | **直接引用** | 通过 git submodule 或路径引用，确保与 TianShu 一致；作为 SchemaRegistry 物理元数据源 |
| 安全检查算法 | **参考后重写** | 理解 legacy 的安全逻辑，用更精简的方式实现 |
| IR / SqlBuildPlan 设计 | **参考后重写** | 只保留核心字段，去除 LLM 评分等附加属性；采用 DeveloperSpec-first 全新链路 |
| 测试思路 | **参考** | 针对确定性逻辑设计新用例，优先 table-driven |
| 代码/实现 | **不复用** | 全部重新实现，避免继承 legacy 的技术债 |
| Prompt 模板 | **重新设计** | 基于 v3 的角色隔离原则和 DeveloperSpec-first 链路重新编写 |

## 4. 旧项目代码膨胀的教训

### 4.1 ir.py 45% 非核心字段

legacy 的 IR 类包含大量非必要的 LLM 元数据，如：
- `llm_confidence_score` — LLM 评分不应进入结构化数据
- `alternative_interpretations` — 不应在生产 IR 中保留
- `raw_llm_response` — 原始响应单独存储，不在 IR 内

**v3 方案**：每个结构化数据类型（ParsedDeveloperSpec、RelationshipHypothesis、SqlBuildPlan、SqlProgram）定义严格字段契约，只包含业务必要的属性。LLM 置信度仅作为 Harness 诊断元数据，不进入业务 Schema。

### 4.2 AgentResponse 18 字段 44% 附加

legacy 的 AgentResponse 包含 token 统计、耗时、模型版本等运行时信息。

**v3 方案**：运行时元数据由 Harness 收集，不进业务数据结构。provenance.yml 记录模型版本和哈希。

### 4.3 过度防御

legacy 的 6 层 SQL 安全检查和 12 层 Spark 安全检查覆盖了大量低风险场景，维护成本高。

**v3 方案**：
- SQL 安全：SQL 由 Python Compiler 确定性生成，SqlBuildPlan 无自由 SQL 字段——安全边界在 Schema 层保证，无需逐层重复检查。
- Spark 安全：使用 Static Validator 做 AST 白名单检查 + 模块导入禁止 + 入口点强制。
- 不做逐层重复防御。

## 5. 各模块迁移路径（DeveloperSpec-first）

| 旧概念 / 模块 | 迁移方式 | 新概念 / 模块 | 目标 Phase |
|---------------|----------|---------------|------------|
| RequirementIR | **删除**——替换为 ParsedDeveloperSpec | ParsedDeveloperSpec（确定性 Parser 输出） | Phase 1A |
| SubIntent | **删除**——替换为 RelationshipHypothesis + SqlBuildPlan | RelationshipHypothesis（Join 推理 + 证据定级）+ SqlBuildPlan（8 step 类型化计划） | Phase 1B |
| SQLPlan / PhysicalPlan | **删除**——替换为 SqlProgram + Compiler | SqlProgram（多语句 DAG）+ Compiler（确定性渲染） | Phase 1B / 1C / 3A |
| Fact Catalog Adapter | **删除**——替换为 SourceManifest + optional SchemaRegistry | SourceManifest（表字段事实追踪，三来源标记）+ SchemaRegistry（可选物理元数据补充） | Phase 1A |
| planning_table / G2/G3 指标绑定 | **删除**——DeveloperSpec 中程序员直接声明 metrics/dimensions | MetricDecl / DimensionDecl（DeveloperSpec 内嵌声明） | Phase 1A |
| TransformationContract | **重命名并扩展**——三级递进 | DataTransformContract-lite（Phase 2 单语句）→ DataTransformContract v1（Phase 3 Exit 多语句） | Phase 2 / 3 Exit |
| MergePlan | **删除**——替换为 SqlProgram DAG | SqlProgram DAG 依赖 + 拓扑排序 | Phase 3A |
| CTEPlan | **删除且永不实现**——以 SqlProgram + _temp 替代 | SqlProgram._temp 中间表 | Phase 3A |
| Contract 引用（git submodule） | **保留**——作为 SchemaRegistry 基础 | SchemaRegistry 物理元数据源 | Phase 0 |
| SQL Compiler | **重写**——新 IR 结构，但保留 sqlglot Renderer 思路 | Compiler（输入 SqlBuildPlan/SqlProgram，输出 DuckDB SQL） | Phase 1C |
| SQL Validator | **重写**——新增 WEAK/NONE 硬门禁、SOURCE_CONFLICT 检查 | Validator（事实源校验 + Join 证据门禁 + 语义校验 + PerfValidator） | Phase 1C |
| Spark 多 Agent | **后移并重写**——Phase 5-6，输入改为 DataTransformContract v1 | SparkDeveloper（标注）+ SparkCompiler（代码生成）+ Static Validator（AST 门禁） | Phase 5-6 |
| 交叉验证 | **后移并重写**——Phase 7，新增逻辑+物理双链验证 | PlanComparator + PhysicalVerifier + RepairPlanner（双链验证） | Phase 7 |
| 返工机制 | **后移并重写**——Phase 7-8，保持 2 轮上限 | DifferenceAnalyst + RepairPlanner | Phase 7-8 |
| Code Review Package | **重写**——新目录结构，DeveloperSpec-first 命名 | Code Review Package | Phase 2 |
| Harness | **重写**——新增 Join 推理质量"零容忍"维度 | Harness（七维门禁） | Phase 4 |
| LangGraph 编排 | **重写**——节点按新链路重排 | LangGraph（薄编排层，DeveloperSpec-first 节点） | Phase 8 |
| 前端工作台 | **重写**——Phase 4.5 内部验证口取代 Phase 5 前端 | 内部交互验证口（DeveloperSpec 编辑器 + 模板按钮 + 结构化解析预览 + OpenQuestion 面板） | Phase 4.5 |
| src/tianshu_datadev/ir/protocols.py | **标记 deprecated**——Phase 0.5 不修改；Phase 1A 新建 Pydantic 模型模块替代 | 新 Pydantic 模型（`developer_spec/`、`planning/`、`sql/`） | Phase 1A-1C |

## 6. 不复用的风险

| 风险 | 说明 | 缓解 |
|------|------|------|
| 重复踩坑 | 新实现可能重复旧 bug | 记录 legacy 常见 bug 清单，编写对应测试 |
| 时间成本 | 全新实现不等于节省时间 | 复用设计思路而不复用代码，平衡质量与成本 |
| 兼容遗漏 | 可能遗漏 legacy 中的边缘 case | 参考 legacy 测试边界设计新 golden/拒绝 fixture |
| 旧路线误导 | 旧文档残留可能误导 Claude Code 执行 | Phase 0.5 全局 rg 检查确保旧术语全部迁移或归档 |

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | Phase 1A 迁移依据
