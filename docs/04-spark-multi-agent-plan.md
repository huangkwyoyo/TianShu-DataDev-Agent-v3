# Phase 6 受控 PySpark DSL — TianShu DataDev Agent v3

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版（占位）

## 1. 当前状态

**等待 Phase 4 退出。** Phase 6 的详细实施规格——包括具体 Prompt、DSL 语法细节、Static Validator 规则、SparkReviewer/Tester 的完整交互协议——必须在 Phase 4 SQL-first v1.0 硬化完成、Harness 七维门禁通过、内部验证口确认链路稳定后才能确定。

## 2. 前置依赖

- Phase 4 SQL-first v1.0 硬化退出
- Phase 5 SparkPlan IR + DataTransformContract v1 消费
- Phase 4.5 内部验证口确认 SQL-first 链路稳定

## 3. 能力白名单（预期）

SparkDeveloper 只能生成以下受控 PySpark 操作：

| 操作 | PySpark API | 来源 |
|------|------------|------|
| scan | `df.select(*columns)` / 从 inputs 读取 | DataTransformContract v1 |
| filter | `df.filter(condition)` | 封闭 Predicate AST |
| join | `df.join(other, on=keys, how=join_type)` | JoinSpec |
| aggregate | `df.groupBy(keys).agg(*metrics)` | AggregateSpec |
| project | `df.select(*columns)` / `df.withColumn(name, expr)` | ProjectSpec |
| case_when | `F.when(cond, result).otherwise(else_val)` | CaseWhenStep |
| window | `F.row_number() / F.rank() / F.lag() / ...` | WindowExpr（白名单 8 种） |

## 4. 禁止事项

- 禁止任意 PySpark API（不在白名单的拒绝）
- 禁止 Action：`collect`、`count`、`toPandas`、`foreach`、`head`、`take` 等
- 禁止 Sink：`write`、`save`、`saveAsTable`、`insertInto` 等
- 禁止 UDF：Python UDF、pandas UDF、任意序列化执行
- 禁止 `spark.table()`、`spark.read`、自行创建 SparkSession
- 禁止网络、文件系统、进程、线程、动态执行、反射、`eval`/`exec`/`compile`
- 禁止 SparkDeveloper 查看 SQL 文本或 SqlBuildPlan 实现
- SparkDeveloper 只读 DataTransformContract v1，不从 DeveloperSpec 重新推理业务逻辑

## 5. 验收标准骨架

1. Developer 稳定输出符合入口契约的代码 artifact
2. Static Validator 拒绝未注入数据源、Action、Sink、UDF、网络、文件和动态执行
3. Reviewer 输出结构化 Finding 和 Directive，不直接替代最终代码
4. Tester 代码经过与业务代码同等级的安全校验
5. 合法代码能在真实本地 Spark 隔离环境读取注入快照并返回 DataFrame
6. 每轮代码、Prompt、模型、契约和执行环境均可追溯

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 占位——Phase 4 退出后重写
