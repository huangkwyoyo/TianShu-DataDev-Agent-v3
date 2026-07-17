# Phase 8：Spark-first 编排硬化

> 状态：**已完成 ✅**（2026-07-04） | 设计文档：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` | 当前项目状态见 `docs/current-state-and-verification-status.md`
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md`
> 前置依赖：Phase 7 SQL/Spark 双链验证

## 架构概要

```
Contract → mapper → SparkDeveloperService（LLM 封装，StructuredOutput）
                 → SparkCompiler → Static Validator
                 → PlanComparator → PhysicalVerifier
                 → RepairPlanner（如需要）
                 → SparkReviewBuilder → SparkReviewPackage
                 → Harness（Spark 5 维度评测）
```

**Orchestrator LLM 边界**：SparkOrchestrator 不直接访问模型、不构造 Prompt、不解析 LLM 自由文本。只调用已封装、结构化输出、带 AnnotationValidator 的 SparkDeveloperService。

## 关键组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `SparkOrchestrator` | `orchestrator.py` | 编排全链路，返工最多 2 轮 → HUMAN_REVIEW |
| `SparkDeveloperService` | `developer.py` | LLM 标注封装，StructuredOutput + AnnotationValidator |
| `SparkReviewPackage` | `review_package.py` | 统一交付物：SparkPlan + annotations + compiled code + verification + cross_reference + provenance |
| `CrossReference` | `review_package.py` | 跨引擎对照——使用 sql_artifact_id / sql_step_id 引用，不含 SQL 文本 |
| `SparkProvenance` | `review_package.py` | 完整 hash 链：contract_hash → spark_plan_hash → annotation_hash → compiled_code_sha256 → snapshot_id |
| `SparkReviewBuilder` | `review_builder.py` | 构建 SparkReviewPackage |
| Harness Spark 维度 | `harness/spark_eval.py` | 5 维度评测 |

## Pipeline 全局状态

```
SQL_REVIEW_READY                       # SQL-only 通过
SQL_REVIEW_READY_SPARK_NOT_EXECUTED     # SQL 通过，Spark 未执行
SQL_REVIEW_READY_SPARK_HUMAN_REVIEW     # SQL 通过，Spark 需人工审查
SQL_SPARK_REVIEW_READY                 # SQL + Spark 全部通过
```

Spark 链路失败不影响 SQL 侧——SQL Review Package 正常产出。

## Harness Spark 5 维度

| 维度 | 说明 |
|------|------|
| `SPARK_CONTRACT_FIDELITY` | SparkPlan 与 Contract 的一致性 |
| `SPARK_COMPILATION_DETERMINISM` | 同一 Contract 两次编译 → 相同 raw 代码 |
| `SPARK_VALIDATOR_COVERAGE` | Static Validator 对 8 种错误码的拦截覆盖率 |
| `SPARK_LOGIC_EQUIVALENCE` | SQL/Spark PlanComparator 结构一致 |
| `SPARK_PHYSICAL_CONSISTENCY` | 双引擎同快照结果一致 |

## 硬约束（C 类）

1. Orchestrator 不直接访问模型、不构造 Prompt、不解析 LLM 自由文本
2. CrossReference 不含 SQL 文本——只用 sql_artifact_id / sql_step_id
3. AnnotationWarning 不触发自动返工
4. SparkDeveloper 调用失败 → HUMAN_REVIEW，不重试
5. 返工上限 2 轮，AnnotationWarning 不计入返工计数

## Memory 边界

本项目不建设独立 Engineering Memory。失败沉淀走 Harness 回归集、确定性规则、Schema/Contract 标注和 Prompt/Harness 版本化评测记录。

---

> 详细设计见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §3
> 实施步骤见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 8
