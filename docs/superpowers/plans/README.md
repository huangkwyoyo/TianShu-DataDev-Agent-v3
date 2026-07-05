# 方案书索引

> 最后更新：2026-07-05 | 当前阶段：Phase 9B-9A3 完成 → 9A4 真实业务样本 或 9A5 REVIEW_READY

## 执行链路（按序号阅读）

```
01-phase-6-8-global-acceptance      02-business-integration-prep       03-business-integration-round1
Phase 6-8 骨架验收                   业务集成前置准备                      业务集成执行第一轮
✅ 已完成                            ✅ 已完成                            ✅ 已完成
                                                                             │
                                                                             └──→ 04-c2-provider-adapter-plan
                                                                                  C2 ProviderAdapter 实现
                                                                                  ✅ 已完成
                                                                                       │
                                                                                       └──→ 05-c2-llm-boundary-consolidation
                                                                                            C2 架构边界收口
                                                                                            ✅ 已完成
                                                                                                 │
                                                                                                 └──→ 06-c3-c4-prerequisite-clarification
                                                                                                      C3/C4 前置澄清
                                                                                                      ✅ 已完成
                                                                                                           │
                                                                                                           └──→ 07-phase-9a-production-pipeline-plan
                                                                                                                Phase 9A 生产级串联方案
                                                                                                                9A1 ✅ → 9A2 ✅ → 9A3 ✅
```

## 文档清单

| 序号 | 文件 | 阶段 | 状态 | 一句话 |
|:---:|------|------|:---:|------|
| 01 | `01-phase-6-8-global-acceptance.md` | Phase 6-8 验收 | ✅ | Orchestrator 骨架串联 + 组件复核 + C 类风险登记 |
| 02 | `02-business-integration-prep.md` | 前置准备 | ✅ | R3 收口 + C1-C4 验收路径矩阵定义 |
| 03 | `03-business-integration-round1.md` | 执行第一轮 | ✅ | C1 点亮（11/11）+ C2 方案启动 |
| 04 | `04-c2-provider-adapter-plan.md` | C2 详细方案 | ✅ | ProviderAdapter + AnthropicAdapter + 集成测试（已完成） |
| 05 | `05-c2-llm-boundary-consolidation.md` | C2 架构收口 | ✅ | 删除重复文件 + 复用统一 LLM 基础设施 |
| 06 | `06-c3-c4-prerequisite-clarification.md` | C3/C4 前置澄清 | ✅ | 文档表述收口 + C3/C4 执行方案 |
| **07** | **`07-phase-9a-production-pipeline-plan.md`** | **Phase 9A 计划** | **📋** | **桥接级→生产级升级方案——5 子阶段拆分** |
| — | `_archived/sql-temp-table-comments.md` | 早期 | 📦 | SQL-first Pipeline 历史文档，与 Spark 路径无关 |

## 给审核人的阅读路径

- **10 分钟了解现状** → `README.md`（本文）→ `07-phase-9a-production-pipeline-plan.md` 的"非技术解释"和"A/B/C 风险分类"
- **30 分钟理解全链路** → 按 01→02→03→05→06→07 顺序阅读每份文档的"能力清单"和"执行记录"
- **接手 Phase 9A 开发** → 读 `07-phase-9a-production-pipeline-plan.md`，5 个子阶段带完整接口定义

## 关键里程碑

| 日期 | 事件 | 证据 |
|------|------|------|
| 2026-07-04 | R3 Mapper input_alias 修复 | 真实 Contract E2E 全链路 PASSED |
| 2026-07-04 | Phase 6-8 骨架验收完成 | 521 passed, 11 skipped |
| 2026-07-04 | C1 真实 Spark 点亮 | 11/11 双引擎（DuckDB ↔ PySpark 4.1.2）100% 一致 |
| 2026-07-04 | C2 架构收口 + 循环导入修复 | 重复文件删除 + PromptManager 可直接导入 |
| 2026-07-04 | C3 Comparator 桥接级点亮 | Orchestrator COMPARATOR 集成，30/30 测试全绿 |
| 2026-07-04 | C4 D4 桥接级点亮 | Harness 5 维度全绿（27/27），含 3 个 D4 桥接测试 |
| 2026-07-05 | Phase 9A 生产级计划制定 | 5 子阶段拆分，A/B/C 分类，接口定义完整 |
| 2026-07-05 | **9A1 SQL Pipeline 中间产物导出** | PipelineArtifactBundle + export_artifacts()，7 测试全绿 |
| 2026-07-05 | **9A2 桥接函数替换** | contract_to_sql_steps 标记 deprecated + 真实 SqlBuildPlan 驱动 COMPARATOR，+2 集成测试全绿，610 passed/11 skipped |
| 2026-07-05 | **9A3 Harness 自动驱动器 + Lite→V1 适配收口** | adapt_lite_to_v1() + HarnessRunner 双模式，7 测试全绿，617 passed/11 skipped |
| ⬜ | 9A4 真实业务样本 | 待业务方提供样本 |
| ⬜ | 9A5 REVIEW_READY 终验收 | 待 9A3 完成后执行 |
