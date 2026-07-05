# Phase 6：受控 PySpark DSL 生成

> 状态：已完成（2026-07-04） | 设计文档：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` | 当前项目状态见 `docs/current-state-and-verification-status.md`
> 实施计划：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md`
> 前置依赖：Phase 5 SparkPlan IR + mapper.py 确定性映射 ✅

## 架构概要

```
DataTransformContractV1 → mapper.py → baseline SparkPlan（结构，确定性）
                                     → SparkDeveloper（LLM，只做标注，不增删改 step）
                                     → AnnotatedSparkPlan → AnnotationValidator
                                     → SparkCompiler（确定性 PySpark DSL 生成）
                                     → SparkCodeRenderer（安全渲染，禁止字符串拼接）
                                     → Static Validator（AST 硬门禁，8 种错误码 E601-E608）
```

**核心原则**：LLM 不直接生成 PySpark 代码。mapper.py 是唯一 Contract → SparkPlan 结构生成路径。Compiler 确定性生成代码，所有代码片段通过 Renderer 封闭枚举/白名单渲染。

## 关键组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `StepAnnotation` / `AnnotatedSparkPlan` | `annotations.py` | 标注模型，step_id 主键，annotation 数量 == steps 数量 |
| `AnnotationValidator` | `annotations.py` | 确定性校验：数量匹配、step_id 有效、无重复 |
| `SparkCompiler` | `compiler.py` | 9 种 step → PySpark DSL，固定入口 `transform(inputs, params) -> DataFrame` |
| `SparkCodeRenderer` | `renderer.py` | 安全渲染——所有值来自封闭枚举/白名单，禁止裸 f-string |
| `SparkStaticValidator` | `validator.py` | AST call-chain 分类，8 种错误码（E601-E608），预留 ExecutionSafetyProbe |
| `SparkDeveloperService` | `developer.py` | LLM 封装（Phase 8 实现），StructuredOutput + AnnotationValidator |

## 难度分组

```
Phase 6A: scan / filter / project / sort / limit     ← 先做
Phase 6B: aggregate / join / case_when               ← 后做
Phase 6C: window（含帧边界）                          ← 最后
```

## 硬约束（C 类）

1. mapper.py 是唯一 Contract → SparkPlan 路径
2. SparkDeveloper 只做标注，不增删改 step
3. 删除 annotation 后执行代码完全等价
4. `inputs["{source_name}"]` 禁止 `spark.read` / `spark.table`
5. Compiled code 不含 SQL 文本
6. 所有代码片段过 Renderer，禁止直接字符串拼接

## 注释格式（5 行固定）

```
# Step: <label>
# Intent: <业务意图，含下游消费者>
# Operation: <操作简述>
# Inputs: <输入表列表>
# Output: <输出表名/别名>
```

不含 SQL 文本。跨引擎对照通过 CrossReference（sql_artifact_id / sql_step_id）完成。

---

> 详细设计见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §1
> 实施步骤见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-implementation-plan.md` Phase 6A/6B/6C
