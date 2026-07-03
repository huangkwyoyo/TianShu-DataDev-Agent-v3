# Phase 8 Spark-first 编排硬化 — TianShu DataDev Agent v3

> 文档版本：2026-07-03 设计完成版
> 完整设计：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §3
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 8

## 当前状态

**设计已完成。** Phase 8 的编排架构、Review Package 模型、CrossReference 设计、Harness 5 维度已产出。不再等待 Phase 4 退出——Phase 4 已完成。

## 架构摘要

- **SparkOrchestrator**：编排全链路，不直接访问模型/构造 Prompt/解析 LLM 自由文本
- **SparkDeveloperService**：LLM 标注封装，StructuredOutput + AnnotationValidator
- **SparkReviewPackage**：统一交付物——SparkPlan + annotations + compiled code + verification + cross_reference + provenance hash 链
- **CrossReference**：跨引擎对照——使用 sql_artifact_id / sql_step_id 引用，不含 SQL 文本
- **Harness**：Spark 5 维度评测（CONTRACT_FIDELITY / COMPILATION_DETERMINISM / VALIDATOR_COVERAGE / LOGIC_EQUIVALENCE / PHYSICAL_CONSISTENCY）

## Memory 边界

本项目不建设独立 Engineering Memory。失败沉淀走 Harness 回归集、确定性规则、Schema/Contract 标注。

---

> 本文已从占位更新。详细设计、模型定义、硬约束、实施步骤见上述 superpowers/specs 文档。
