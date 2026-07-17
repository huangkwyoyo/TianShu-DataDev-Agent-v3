# Phase 0 / 0.5：项目启动与架构契约校正

> ⚠️ 本文为历史完成阶段记录，不代表当前下一步。
> **状态：✅ 已完成（2026-06-26）**
> Phase 0.5 已于 2026-06-26 完成文档迁移与路线图统一，所有规划文档已切换为 DeveloperSpec-first 路线。

## Phase 0 已完成

- 创建项目骨架、核心文档、Protocol 探索模型和 22 个基础测试。
- 完成 TianShu、Text2SQL Agent 和 legacy Data Dev Agent 复用审计。
- 明确新项目不复制旧项目实现，不继承发布和物化体系。

Phase 0 产物是探索基线，不是最终运行时契约。`src/tianshu_datadev/ir/protocols.py` 中的自由字符串和宽泛 Protocol 不得直接进入 Phase 1 实现。

## Phase 0.5 目标（已完成）

Phase 0.5 已将项目从 Text2SQL-first / Fact Catalog-first 路线迁移为 DeveloperSpec-first 路线：

1. SqlBuildPlan / SqlProgram 使用类型化 step AST，禁止 LLM 间接生成 SQL 片段。
2. LLM 结构化输出使用严格 Pydantic / JSON Schema，而非仅靠 Protocol。
3. PySpark 固定为 `transform(inputs, params) -> DataFrame` 纯转换函数。
4. 多表样本使用关系一致快照，不按表独立 LIMIT。
5. 验证采用精确状态，`RESULT_CONSISTENT` 不等于生产正确。
6. Graph State 只保存 artifact 引用、哈希、状态和摘要。
7. 表字段事实源是 SourceManifest / SchemaRegistry，不建设可写 Domain Memory 或独立 Engineering Memory。
8. Phase 1A-8 路线按 DeveloperSpec → ParsedDeveloperSpec → SourceManifest → RelationshipHypothesis → SqlBuildPlan → SqlProgram → DataTransformContract → Spark-first 依次推进。

## Phase 0.5 交付物

- README.md、AGENTS.md、docs/00-09 全部迁移为 DeveloperSpec-first。
- docs/roadmap/ 下 18 个 Phase 文件全部按新命名规范创建或重写。
- 旧 roadmap 文件已删除（Git 保留完整历史，无需本地归档）。
- 全局 rg 检查通过——旧路线术语（RequirementIR / SubIntent / Fact Catalog Adapter）零残留。

## 本阶段不做

- 不修改 Python IR 实现。
- 不实现 SQL 编译、Spark 生成、执行器或 LangGraph。
- 不接真实 LLM、数据库或前端。
- 不新增针对文档措辞的 pytest。

## 验收

- 核心规划和 Phase 1A-8 路线无相互冲突。
- AGENTS 和 README 与目标架构一致。
- 现有 22 个测试保持通过。
- ruff 现有问题被明确记录并留给独立 A 类小修。
- Phase 1A 能依据文档制定独立实现计划。

---

> Phase 0 完成；Phase 0.5 文档校正完成 | 2026-06-26 | 历史阶段——当前下一步为 Phase 1A
