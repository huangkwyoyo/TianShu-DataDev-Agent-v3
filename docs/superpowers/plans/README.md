# 方案书索引

> 最后更新：2026-07-17 | 当前阶段：Phase 9A-9C + label_table v1 完成
>
> 所有已完成的计划保留为历史参考，最新状态以 `docs/current-state-and-verification-status.md` 为准。

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
                                                                                                                9A1 ✅ → 9A2 ✅ → 9A3 ✅ → 9A5 ✅
                                                                                                                （9A4 NYC 01-06 已完成）
```

## 文档清单

### 主线方案（按编号顺序阅读）

| 序号 | 文件 | 阶段 | 状态 | 一句话 |
|:---:|------|------|:---:|------|
| 01 | `01-phase-6-8-global-acceptance.md` | Phase 6-8 验收 | ✅ | Orchestrator 骨架串联 + 组件复核 + C 类风险登记 |
| 02 | `02-business-integration-prep.md` | 前置准备 | ✅ | R3 收口 + C1-C4 验收路径矩阵定义 |
| 03 | `03-business-integration-round1.md` | 执行第一轮 | ✅ | C1 点亮（11/11）+ C2 方案启动 |
| 04 | `04-c2-provider-adapter-plan.md` | C2 详细方案 | ✅ | ProviderAdapter + AnthropicAdapter + 集成测试 |
| 05 | `05-c2-llm-boundary-consolidation.md` | C2 架构收口 | ✅ | 删除重复文件 + 复用统一 LLM 基础设施 |
| 06 | `06-c3-c4-prerequisite-clarification.md` | C3/C4 前置澄清 | ✅ | 文档表述收口 + C3/C4 执行方案 |
| 07 | `07-phase-9a-production-pipeline-plan.md` | Phase 9A 计划 | ✅ | 桥接级→生产级升级方案——5 子阶段均已实现 |

### 特性方案（按日期排序）

| 文件 | 关键内容 | 状态 |
|------|----------|:----:|
| `2026-07-05-case06-b-class-closure.md` | Case06 B 类设计修复 | ✅ |
| `2026-07-05-case06-comparator-gap-closure.md` | Case06 Comparator 缺口收口 | ✅ |
| `2026-07-05-final-hardening.md` | 核心平台最终硬化 | ✅ |
| `2026-07-05-phase-9b-frontend-regression-observability.md` | 前端回归 + 可观测性 | ✅ |
| `2026-07-05-phase-9b-p0-snapshot-pipeline-integration.md` | Snapshot Builder 集成 | ✅ |
| `2026-07-05-phase-9c-e2e-implementation.md` | DOM E2E 测试 | ✅ |
| `2026-07-05-r8-llm-production-verification.md` | LLM 生产验证 | ✅ |
| `2026-07-05-spark-pipeline-frontend-integration.md` | Spark 前端集成 | ✅ |
| `2026-07-05-sqlprogram-multi-statement-dag-case06.md` | SqlProgram DAG Case06 | ✅ |
| `2026-07-06-spark-comparator-content-alignment-plan.md` | Comparator 内容对齐 | ✅ |
| `2026-07-06-spark-stage-independent-and-llm-traces.md` | Spark 阶段独立 + 追踪 | ✅ |
| `2026-07-07-dev-reload.md` | dev-reload 脚本 | ✅ |
| `2026-07-07-spark-compiler-llm-annotation-injection-plan.md` | LLM 注释注入 | ✅ |
| `2026-07-07-spark-developer-service-injection-plan.md` | SparkDeveloper 服务注入 | ✅ |
| `2026-07-08-comparator-gap-fix.md` | Comparator 缺口修复 | ✅ |
| `2026-07-09-comparator-crcs-v2-revision.md` | CRCS v2 修订 | ✅ |
| `2026-07-09-comparator-window-aggregate-fix.md` | 窗口/聚合 Comparator 修复 | ✅ |
| `2026-07-09-left-join-safety-gate-v2.md` | Left Join 安全门 v2 | ✅ |
| `2026-07-09-snapshot-inputs-key-alias-fix.md` | Snapshot Key 别名修复 | ✅ |
| `2026-07-10-full-pipeline-monitoring-plan.md` | 全管线监控 | ✅ |
| `2026-07-10-monitor-human-readable-log-plan.md` | 可读日志监控 | ✅ |
| `2026-07-13-cre-v3-cdp-implementation.md` | CRE v3 CDP 实现 | ✅ |
| `2026-07-14-code-download-buttons.md` | 代码下载按钮 | ✅ |
| `2026-07-14-run-all-progress-streaming.md` | Run-All 流式进度 | ✅ |
| `2026-07-15-label-table-implementation.md` | label_table v1 完整管线 | ✅ |

### 已归档

| 文件 | 状态 |
|------|:----:|
| `_archived/sql-temp-table-comments.md` | 📦 SQL-first Pipeline 历史文档 |

## 关键里程碑

| 日期 | 事件 | 证据 |
|------|------|------|
| 2026-07-04 | Phase 6-8 骨架验收完成 | 521 passed, 11 skipped |
| 2026-07-04 | C1 真实 Spark 点亮 | 11/11 双引擎 100% 一致 |
| 2026-07-05 | Phase 9A 全链路闭环 | 9A1-9A3 + 9A5 完成，629 passed |
| 2026-07-05 | Phase 9B 前端回归 | R11/R15 消除 |
| 2026-07-05 | Phase 9C DOM E2E | 6/6 Playwright 通过 |
| 2026-07-06 | Case06 双链 LOGIC_EQUIVALENT | xfail 转正，三层剥离 |
| 2026-07-13 | CRE shadow 物理验证可用 | 三条 Pipeline 证据通过 |
| 2026-07-14 | Run-All 流式进度 + 代码下载按钮 | 前端体验改进 |
| **2026-07-16** | **label_table v1 管线完成** | **Parser→Extractor→Validator→Promotion→Builder→Compiler 全链路，90 测试全绿** |
