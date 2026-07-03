# Phase 6 受控 PySpark DSL — TianShu DataDev Agent v3

> 文档版本：2026-07-03 设计完成版
> 完整设计：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §1
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 6A/6B/6C

## 当前状态

**设计已完成。** Phase 6 的完整设计、模型定义、编译器架构、安全渲染规则、Static Validator 错误码体系已产出。不再等待 Phase 4 退出——Phase 4 已完成。

## 架构摘要

- **mapper.py**：唯一 Contract → SparkPlan 结构生成路径（确定性，Phase 5 交付）
- **SparkDeveloper（LLM）**：只做语义标注（StepAnnotation + AnnotationWarning），不增删改 step
- **SparkCompiler**：确定性 PySpark DSL 生成，固定入口 `transform(inputs, params) -> DataFrame`
- **SparkCodeRenderer**：安全渲染——所有值来自封闭枚举/白名单，禁止字符串拼接
- **Static Validator**：AST call-chain 分类，8 种错误码（E601-E608）

## 难度分组

- Phase 6A：scan / filter / project / sort / limit
- Phase 6B：aggregate / join / case_when
- Phase 6C：window（含帧边界）

---

> 本文已从占位更新。详细设计、模型定义、硬约束、实施步骤见上述 superpowers/specs 文档。
