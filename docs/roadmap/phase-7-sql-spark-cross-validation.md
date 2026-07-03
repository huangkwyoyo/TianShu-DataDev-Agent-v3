# Phase 7：SQL/Spark 双链验证

> 状态：设计完成，待实施 | 设计文档：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md`
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md`
> 前置依赖：Phase 6 受控 PySpark DSL

## 架构概要

```
                       DataTransformContractV1
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        SqlBuildPlan     SparkPlan      Snapshot Builder
        (SQL 侧)         (Spark 侧)     (不可变 Parquet)
              │               │               │
              └───────┬───────┘               │
                      ▼                ┌──────┴──────┐
              PlanComparator          ▼             ▼
              （逻辑链路）        DuckDB Exec   Spark Exec
                      │               │             │
                      ▼               └──────┬──────┘
              PlanComparisonReport          ▼
                      │              PhysicalVerifier
                      │              （物理链路）
                      ▼                     │
              UnifiedVerificationReport ←───┘
                      │
                      ▼
              REVIEW_READY / REPAIR_NEEDED / HUMAN_REVIEW
```

**双链验证**：逻辑链路（PlanComparator 对比 SQL Plan ↔ Spark Plan 结构等价）+ 物理链路（同一份不可变 Snapshot 上 DuckDB + Spark 双引擎执行后对比结果）。逻辑通过不能替代物理通过。

## 关键组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `SnapshotBuilder` | `snapshot.py` | Contract → 不可变 Parquet 快照；`SnapshotSourceProvider` 白名单数据源 |
| `SnapshotManifest` | `snapshot.py` | 快照清单，snapshot_id = sha256(contract_hash + source_manifest_hash + sampling_spec + env_fingerprint) |
| `PlanComparator` | `plan_comparator.py` | 封装 Phase 5 `plan_equivalence.py`，SQL ↔ Spark 结构等价对比 |
| `PhysicalVerifier` | `physical_verifier.py` | 双引擎执行 + `ResultCanonicalizer` 规范化 + 结果对比 |
| `ResultCanonicalizer` | `physical_verifier.py` | 排序/去重/NULL/NaN/Decimal/TZ 统一策略 |
| `RepairPlanner` | `repair_planner.py` | 只输出 RepairAction，不直接修改任何 Plan |
| `LocalSparkExecutor` | `executor.py` | 子进程隔离执行 PySpark DSL（安全声明：受控开发验证口） |

## 状态命名（禁止 "PASS" / "Go" / "No-Go"）

| 状态 | 含义 |
|------|------|
| `LOGIC_EQUIVALENT` | SQL ↔ Spark 结构完全等价 |
| `LOGIC_MISMATCH` | 结构不等价 |
| `LOGIC_UNSUPPORTED` | 存在不支持对比的 step 类型 |
| `RESULT_CONSISTENT` | 双引擎结果一致 |
| `RESULT_MISMATCH` | 结果不一致 |
| `NOT_EXECUTED` | 尚未执行（禁止泛化 PASS） |
| `HUMAN_REVIEW` | 需人工审查 |

## RepairPlanner 路由

| RepairAction.category | 处理方式 |
|----------------------|---------|
| `MAPPER_BUG` | 路由回 mapper.py 修复（工程团队） |
| `COMPILER_BUG` | 路由回 Compiler 修复（工程团队） |
| `VALIDATOR_GAP` | 路由回 Validator 规则修复（工程团队） |
| `SNAPSHOT_ISSUE` | 重新生成快照 |
| `BUSINESS_SEMANTIC` | HUMAN_REVIEW 或回 SQL-first Planner |

**RepairPlanner 不直接修改 Contract、SqlBuildPlan、SparkPlan、PySpark 代码。** 返工最多 2 轮。

## 硬约束（C 类）

1. Snapshot Builder 只能读 `SnapshotSourceProvider` 白名单数据源
2. 同一份快照供 DuckDB 和 Spark 分别读取
3. `NOT_EXECUTED` 禁止泛化 PASS——未覆盖 step 类型必须明确标记
4. 物理未覆盖 → `NOT_EXECUTED` 或 `HUMAN_REVIEW`
5. 返工上限 2 轮 → 自动 HUMAN_REVIEW

---

> 详细设计见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §2
> 实施步骤见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 7A/7B/7C
