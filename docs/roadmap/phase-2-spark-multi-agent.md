# Phase 2：受控PySpark多角色纵向切片

## 目标

使用Fake LLM Adapter先打通SparkDeveloper、SparkReviewer和SparkTester的结构化角色契约，并在真实本地Spark隔离环境执行受控`transform(inputs, params)`函数。

## 输入

- Phase 1的RequirementIR、SubIntent和TransformationContract。
- 版本化快照fixture及源Schema。
- 角色Prompt和输出JSON Schema。

## 前置条件

- Phase 1.2 的 PerfContract 和 `get_prompt_hints()` 机制已完成——Spark 分支的性能约束独立实现，不读取 SQL 侧的 PerfRule 列表。

## 交付物

1. SparkCodeArtifact、ReviewResult、ReviewFinding、OptimizationDirective和TestPlan模型。
2. SparkDeveloper Fake Adapter和统一LLM Gateway接口。
3. AST-based Spark Static Validator。
4. SparkReviewer结构化审查流程，Reviewer不直接覆盖最终代码。
5. SparkTester及Test Static Validator。
6. 独立进程的本地Spark Executor和EnvironmentManifest。

## 固定流程

```text
Developer → Validator → Reviewer
→ REVISE时Developer修订 → Validator
→ Tester → Test Validator → Test Runner
→ Spark Executor → ExecutionTrace + ResultSummary
```

## 禁止

- 真实模型API作为本阶段必需依赖。
- `spark.table`、Spark read、Action、Sink、UDF、网络、文件系统和动态执行。
- Spark代码查看SQL文本或SQLPlan。
- Reviewer或Tester宣布验证通过。
- LangGraph和自动返工。

## 验收

1. 合法代码只读取注入的inputs并返回一个DataFrame。
2. 危险代码和测试代码在执行前被拒绝。
3. Reviewer输出Finding/Directive，Developer修订后重新校验。
4. 真实本地Spark黄金路径运行成功；环境不可用时为`NOT_EXECUTED`。
5. 所有artifact记录模型占位标识、Prompt、Schema、代码和环境哈希。
6. 累计pytest目标50-65个。

## 下一阶段依赖

Phase 3使用本阶段的Spark ExecutionTrace/ResultSummary和Phase 1的SQL结果进行确定性交叉验证。

---

> Phase 0.5 校正 | 2026-06-22
