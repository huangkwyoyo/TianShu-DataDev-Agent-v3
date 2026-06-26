# Phase 5：SparkPlan IR + DataTransformContract v1 消费

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 状态：占位——Phase 4 退出后重写
> 前置依赖：Phase 4 退出 + Phase 4.5 内部验证口验证通过

## 当前占位概要

### 目标

1. 设计 SparkPlan IR——Spark 侧的类型化中间表示，从 DataTransformContract v1 确定性映射
2. 实现 SQL step → Spark step 的映射规则（ScanStep→read.parquet、FilterStep→filter、JoinStep→join 等）
3. PlanEquivalence 规则——表达 SqlBuildPlan step 与 SparkPlan step 的结构等价判断标准
4. DataTransformContract v1 是 Spark 侧的唯一业务输入——SparkDeveloper 不读取 DeveloperSpec

### 前置依赖

- DataTransformContract v1（Phase 3 Exit）
- Harness 七维门禁通过（Phase 4）
- 内部验证口确认 SQL-first 链路稳定（Phase 4.5）

### 能力白名单（预期）

- ScanStep → `spark.read.parquet(path)`
- FilterStep → `df.filter(condition)`
- JoinStep → `df.join(other, on=keys, how=join_type)`
- AggregateStep → `df.groupBy(keys).agg(*metrics)`
- ProjectStep → `df.select(*columns)`
- CaseWhenStep → `F.when(cond, result).otherwise(else_val)`
- SortStep → `df.orderBy(*sorts)`
- LimitStep → `df.limit(n)`

### 禁止事项

- SparkPlan IR 不得包含自由代码片段
- 映射必须是确定性的——相同 DataTransformContract v1 产生相同 SparkPlan

### 验收标准骨架

1. DataTransformContract v1 可确定性映射为 SparkPlan IR
2. SQL step → Spark step 映射覆盖全部 8 种 step
3. PlanEquivalence 规则定义完整（待 Phase 7 实现 Comparator）

---

> Phase 5 | 占位 | Phase 4 退出后由实施 Prompt 重写
