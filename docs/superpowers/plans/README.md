# 方案书索引

> 最后更新：2026-07-04 | 当前阶段：03 业务集成执行第一轮（C1 ✅ C2 📋）

## 执行链路（按序号阅读）

```
01-phase-6-8-global-acceptance      02-business-integration-prep       03-business-integration-round1
Phase 6-8 骨架验收                   业务集成前置准备                      业务集成执行第一轮
✅ 已完成                            ✅ 已完成                            ✅ 本轮完成
                                                                             │
                                                                             └──→ 04-c2-provider-adapter-plan
                                                                                  C2 ProviderAdapter 实现
                                                                                  ⬜ 待执行（4 个 Task）
```

## 文档清单

| 序号 | 文件 | 阶段 | 状态 | 一句话 |
|:---:|------|------|:---:|------|
| 01 | `01-phase-6-8-global-acceptance.md` | Phase 6-8 验收 | ✅ | Orchestrator 骨架串联 + 组件复核 + C 类风险登记 |
| 02 | `02-business-integration-prep.md` | 前置准备 | ✅ | R3 收口 + C1-C4 验收路径矩阵定义 |
| **03** | **`03-business-integration-round1.md`** | **执行第一轮** | ✅ | **C1 点亮（11/11）+ C2 方案启动** |
| 04 | `04-c2-provider-adapter-plan.md` | C2 详细方案 | ⬜ | ProviderAdapter + AnthropicAdapter + 集成测试（4 Task） |
| — | `_archived/sql-temp-table-comments.md` | 早期 | 📦 | SQL-first Pipeline 历史文档，与 Spark 路径无关 |

## 给审核人的阅读路径

- **10 分钟了解现状** → `README.md`（本文）→ `03-business-integration-round1.md` 末尾"非技术人员解释"
- **30 分钟理解全链路** → 按 01→02→03 顺序阅读每份文档的"能力清单"和"执行记录"
- **接手开发** → 读 `04-c2-provider-adapter-plan.md`，4 个 Task 带完整代码

## 关键里程碑

| 日期 | 事件 | 证据 |
|------|------|------|
| 2026-07-04 | R3 Mapper input_alias 修复 | 真实 Contract E2E 全链路 PASSED |
| 2026-07-04 | Phase 6-8 骨架验收完成 | 521 passed, 11 skipped |
| 2026-07-04 | C1 真实 Spark 点亮 | 11/11 双引擎（DuckDB ↔ PySpark 4.1.2）100% 一致 |
| 2026-07-04 | C2 ProviderAdapter 方案 | 4 个 Task 已定义，待实现 |
| ⬜ | C2 实现 | 见 04 |
| ⬜ | C3 Comparator | 等待 SQL pipeline |
| ⬜ | C4 Harness 样本 | 等待业务方 |
