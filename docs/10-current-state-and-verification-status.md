# 项目当前状态与验证进度 — TianShu DataDev Agent v3

> 文档版本：2026-07-04 | 最后更新：2026-07-04 C4 D4 桥接级点亮
> 本文是项目当前实施状态的**唯一权威文档**。各 Phase 设计文档（docs/00-09、docs/roadmap/）描述的是目标设计，实际建成状态以本文为准。

## 1. Phase 进度矩阵

| Phase | 名称 | 设计 | 实现 | 测试 | 备注 |
|:-----:|------|:---:|:---:|:---:|------|
| 0.5 | DeveloperSpec-first 架构校正 | ✅ | ✅ | ✅ | 文档迁移 + 路线图统一 |
| 1A/1B/1C | SQL 管线（输入→推理→编译） | ✅ | ✅ | ✅ | SQL-first v1.0 基础 |
| 2 | Code Review Package v1 | ✅ | ✅ | ✅ | |
| 3A/3B/3C | SqlProgram + 窗口 + 写入 | ✅ | ✅ | ✅ | |
| 4A-4D | SQL-first v1.0 硬化 | ✅ | ✅ | ✅ | LLM Gateway + Harness 七维 |
| 4.5 | 内部交互验证口 | ✅ | ✅ | ✅ | CLI + REST API |
| 4.6 | 复杂 SQL 渐进开放 | ✅ | ✅ | ✅ | 多跳 Join + FROM 子查询 |
| 5 | Spark-ready 契约 | ✅ | ✅ | ✅ | DataTransformContract + SparkPlan IR |
| 6A | scan/filter/project/sort/limit | ✅ | ✅ | ✅ | 编译 + Validator 全错误码 |
| 6B | aggregate/join/case_when | ✅ | ✅ | ✅ | 编译扩展 |
| 6C | window + 帧边界 | ✅ | ✅ | ✅ | 含 RepairPlanner |
| 7A | 逻辑链路 + Snapshot | ✅ | ✅ | ✅ | PlanComparator 9 种 step |
| 7B | 物理链路——双引擎验证 | ✅ | ✅ | ✅ | 11/11 真实 Spark 通过 |
| 7C | 物理链路扩展 + 安全加固 | ✅ | ✅ | ✅ | 窗口双引擎 + SQL 加固 |
| 8 | 编排硬化 + Harness | ✅ | ✅ | ✅ | Orchestrator + Review Package + 5 维度 |

**当前测试基线**：552 passed / 11 skipped（全量回归，零退化，ruff 零告警）

## 2. C1-C4 业务集成验证

| 编号 | 内容 | 风险等级 | 状态 | 证据 |
|:----:|------|:--------:|:----:|------|
| C1 | 真实 Spark 物理验证 | 已消除 | ✅ 11/11 通过 | PySpark 4.1.2，DuckDB ↔ PySpark 一致性 100% |
| C2 | LLM 基础设施架构收口 | 已消除 | ✅ 收口完成 | 重复文件已删除，18/18 测试全绿，DeepSeek 3/3 验证 |
| C3 | Comparator 真实逻辑对比 | 已消除 | ✅ 桥接+集成 | 30/30 测试全绿，Orchestrator COMPARATOR 集成 |
| C4 | Harness 5 维度评测 | B（D4 桥接级） | ✅ 全 5 维度 | D1/D2/D3/D4/D5 共 27/27 测试全绿 |

**D4 重要说明**：D4 LOGIC_EQUIVALENCE 当前为**桥接级验证**——使用确定性桥接函数 `contract_to_sql_steps()` 将 Contract 映射为 SqlBuildPlan，而非经过 SQL Pipeline 的 SpecEnricher → SqlBuildPlanBuilder 完整链路。它验证的核心命题是"同一份结构化合同两边生成结果是否对得上"，不是完整 SQL Pipeline 的生产级验收。

## 3. 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R5 | 桥接函数替代完整 SQL Pipeline——D4 需从桥接级升级到生产级 | B | Phase 9+ |
| R6 | Harness Runner 为结果聚合器——需升级为自动评测驱动器 | B | Phase 9+ |
| R7 | 真实业务样本缺失——所有测试使用手工构造 Contract | B | 待业务方提供 |
| R8 | LLM 生产环境持续验证未配置——DeepSeek 开发环境一次性通过 | C | 待 API key |

## 4. 当前架构全景

```
DeveloperSpec (.md 项目书)
    │
    ├─ SQL 管线（确定性，生产可用）
    │   Parser → SourceManifest → SqlBuildPlan → Compiler → DuckDB → Review Package
    │
    └─ Spark 管线（确定性 + 桥接验证）
        DataTransformContract → Mapper → SparkPlan → Compiler → Validator
                                    │                      │
                                    └── PlanComparator ────┘  ← 双管线逻辑对比
                                         PhysicalVerifier      ← 双引擎物理对比
                                         Orchestrator          ← 6 阶段编排
                                         Harness 5 维度        ← 评测框架
```

## 5. 下一步方向（Phase 9+）

1. **SQL Pipeline 生产级串联**——桥接函数替换为 SpecEnricher → SqlBuildPlanBuilder 真实产出
2. **Harness Runner 自动评测驱动器**——从手动填入 `case.passed` 升级为自动执行+判定
3. **真实业务样本端到端验证**——6 个企业场景的 DeveloperSpec → 双管线全链路
4. **生产环境 LLM 验证**——API key 配置 + 持续验证链路
5. **REVIEW_READY 最终验收**——Snapshot Builder + 双引擎 Executor + 自动交叉验证全串联

## 6. 关键文档索引

| 文档 | 用途 |
|------|------|
| `docs/00-product-charter.md` | 产品愿景和验收标准 |
| `docs/01-target-architecture.md` | 目标架构（设计参考） |
| `docs/04-spark-multi-agent-plan.md` | Phase 6 Spark DSL 设计 |
| `docs/05-cross-validation-and-repair-plan.md` | Phase 7 双链验证设计 |
| `docs/06-langgraph-orchestration-plan.md` | Phase 8 编排设计 |
| `docs/09-test-strategy.md` | 测试策略 |
| `docs/risks/phase-6-8-known-risks.md` | C1-C4 风险详细登记 |
| `docs/roadmap/` | 各 Phase 实施路线图 |
| `docs/superpowers/specs/` | Phase 6-8 完整设计+实施计划 |
