# Phase 1：类型化SQL纵向切片

## 目标

完成一条不接真实LLM的确定性纵向路径：项目书fixture → 严格RequirementIR → SubIntent → TransformationContract → 类型化SQLPlan → Python编译SQL → DuckDB冻结快照执行 → ExecutionTrace与ResultSummary。

## 前置条件

- Phase 0.5文档校正已批准。
- 明确TianShu Fact Catalog读取边界。
- 选择Pydantic严格模型和SQL AST/Renderer方案。
- `docs/10-performance-contract.md` 已定义，明确 REJECT/WARN 两级门禁策略和编译器优化 pass 清单——本阶段只预留接口，Phase 1.2 实现规则。

## 交付物

1. 严格Pydantic模型：RequirementIR、SubIntent、TransformationContract、SQLPlan及表达式AST。
2. Fact Catalog Adapter：读取TianShu指标、语义、表字段和Join白名单。
3. Deterministic Fake Planner：不用真实LLM即可生成受控SQLPlan fixture。
4. SQL Validator（含语义校验逻辑 + `register_perf_rules()` 性能门禁注册表接口）与 SQL Compiler（含 `register_passes()` 编译优化 pass 管道接口，Phase 1.2 注入具体规则和 pass）。
5. 单表快照fixture、DuckDB Executor、ExecutionTrace和ResultSummary。
6. MergePlan契约，但本阶段只验证不兼容场景，不实现任意多表合并。

## 支持范围

- 单Gold表。
- 最多一个白名单Join。
- 注册指标和维度。
- 类型化过滤、日期范围、分组、聚合、排序和limit。
- COUNT、SUM、AVG、MIN、MAX、COUNT DISTINCT。

## 禁止

- 真实LLM调用。
- 任意SQL字符串字段。
- 开窗函数和`WindowExpr`。
- 多跳Join、任意子查询和自由函数。
- CTE、DDL、DML和多段SQL脚本。
- Spark、LangGraph、Memory、前端和生产数据。

开窗函数进入Phase 1.5。Phase 1遇到排名、累计、LAG/LEAD、分区TopN等窗口需求时，只能返回`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。

## 验收

1. JSON Schema拒绝自由SQL、额外字段和未注册引用。
2. 同一SQLPlan产生字节一致SQL和哈希。
3. 单表与一个受控Join黄金用例在DuckDB运行成功。
4. 不支持需求返回明确状态，不退化为自由文本。
5. 累计pytest目标30-40个。

## 下一阶段依赖

Phase 1.2 使用本阶段的 Validator 注册表接口和 Compiler pass 管道接口，注入性能门禁规则和编译优化 pass。

Phase 1.5 复用本阶段的 RequirementIR、SubIntent、TransformationContract、Fact Catalog Adapter 和 SQL Compiler 骨架，新增 `WindowExpr`、`WindowSpec` 和窗口拒绝路径。

Phase 2 复用 RequirementIR、SubIntent、TransformationContract 和 artifact 模型，不读取 SQL 文本。

---

> Phase 0.5 校正 | 2026-06-22
