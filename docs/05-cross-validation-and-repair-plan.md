# Phase 7 双链验证与修复 — TianShu DataDev Agent v3

> 文档版本：2026-07-03 设计完成版 | 实施状态：已完成（2026-07-04）。当前项目状态见 `docs/current-state-and-verification-status.md`
> 完整设计：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §2
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 7A/7B/7C

## 当前状态

**设计已完成。** Phase 7 的双链验证架构、Snapshot 安全模型、Comparator 状态体系、RepairPlanner 路由已产出。不再等待 Phase 4 退出——Phase 4 已完成。

## 架构摘要

- **Snapshot Builder**：Contract → 不可变 Parquet 快照，`SnapshotSourceProvider` 白名单数据源
- **PlanComparator（逻辑链路）**：SQL Plan ↔ Spark Plan 结构等价对比，封装 Phase 5 `plan_equivalence.py`
- **PhysicalVerifier（物理链路）**：同一快照上 DuckDB + Spark 双引擎执行，`ResultCanonicalizer` 规范化后对比
- **RepairPlanner**：只输出 RepairAction（MAPPER_BUG / COMPILER_BUG / VALIDATOR_GAP / SNAPSHOT_ISSUE / BUSINESS_SEMANTIC），不直接修改任何 Plan
- **状态命名**：`LOGIC_EQUIVALENT` / `RESULT_CONSISTENT` / `NOT_EXECUTED` / `HUMAN_REVIEW`（禁止 "PASS"）

---

> 本文已从占位更新。详细设计、模型定义、硬约束、实施步骤见上述 superpowers/specs 文档。
