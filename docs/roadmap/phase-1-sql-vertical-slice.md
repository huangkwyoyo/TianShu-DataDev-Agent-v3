# Phase 1：类型化SQL纵向切片

## 目标

完成一条不接真实LLM的确定性纵向路径：项目书fixture → 严格RequirementIR → SubIntent → TransformationContract → 类型化SQLPlan → Python编译SQL → DuckDB冻结快照执行 → ExecutionTrace与ResultSummary。

## 前置条件

- Phase 0.5文档校正已批准。
- 明确TianShu Fact Catalog读取边界。
- 选择Pydantic严格模型和SQL AST/Renderer方案。

## 交付物

1. 严格Pydantic模型：RequirementIR、SubIntent、TransformationContract、SQLPlan及表达式AST。
2. Fact Catalog Adapter：读取TianShu指标、语义、表字段和Join白名单。
3. Deterministic Fake Planner：不用真实LLM即可生成受控SQLPlan fixture。
4. SQL Compiler与Validator。
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
- 多跳Join、任意子查询和自由函数。
- Spark、LangGraph、Memory、前端和生产数据。

## 验收

1. JSON Schema拒绝自由SQL、额外字段和未注册引用。
2. 同一SQLPlan产生字节一致SQL和哈希。
3. 单表与一个受控Join黄金用例在DuckDB运行成功。
4. 不支持需求返回明确状态，不退化为自由文本。
5. 累计pytest目标30-40个。

## 下一阶段依赖

Phase 2复用RequirementIR、SubIntent、TransformationContract和artifact模型，不读取SQL文本。

---

> Phase 0.5 校正 | 2026-06-22
