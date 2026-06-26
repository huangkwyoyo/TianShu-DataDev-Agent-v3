# Phase 6：受控 PySpark DSL + Static Validator

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 状态：占位——Phase 4 退出后重写
> 前置依赖：Phase 5 SparkPlan IR + DataTransformContract v1 消费

## 当前占位概要

### 目标

1. SparkDeveloper（LLM）读 DataTransformContract v1，生成受控 PySpark 纯转换函数
2. Static Validator 做 AST 白名单检查——默认拒绝未知语法
3. SparkReviewer 输出结构化 ReviewFinding 与 OptimizationDirective
4. SparkTester 输出 TestPlan 和测试代码（测试代码同样经过 Static Validator 和隔离执行）

### 前置依赖

- DataTransformContract v1（Phase 5 已验证可映射为 SparkPlan IR）
- SparkDeveloper 从 DataTransformContract 翻译，不独立读取 DeveloperSpec

### 能力白名单

- scan / filter / join / aggregate / project / case_when / window
- 纯转换函数入口：`transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame`
- 只读取 `inputs` 中契约声明的数据源

### 禁止事项

- 禁止 `spark.table`、`spark.read`、自行创建 SparkSession
- 禁止 Action（collect/count/toPandas/foreach）、Sink（write/save/saveAsTable/insertInto）
- 禁止 UDF（Python UDF / pandas UDF / 任意序列化执行）
- 禁止网络、文件系统、数据库连接、进程、线程、动态执行、反射
- 禁止 `eval`、`exec`、`compile`、动态导入和任意模块导入
- 禁止 SparkDeveloper 查看 SQL 文本或 SqlBuildPlan 实现

### 验收标准骨架

1. Developer 稳定输出符合入口契约的代码 artifact
2. Static Validator 拒绝未注入数据源、Action、Sink、UDF、网络、文件和动态执行
3. Reviewer 输出结构化 Finding 和 Directive，不直接替代最终代码
4. Tester 代码经过与业务代码同等级的安全校验

---

> Phase 6 | 占位 | Phase 4 退出后由实施 Prompt 重写
