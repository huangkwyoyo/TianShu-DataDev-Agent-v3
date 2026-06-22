# SQL IR 和编译器计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 目标

建立三层、可校验、可序列化的SQL IR。LLM只能选择经过事实源注册的类型化节点，Python编译器负责生成全部SQL语法。

## 2. 运行时模型选择

Phase 0中的`Protocol`只用于接口探索，不能作为LLM结构化输出和持久化契约。Phase 1必须使用Pydantic模型或等价的严格Schema，并满足：

- `extra="forbid"`，拒绝未知字段。
- Enum限制操作符、Join类型、聚合函数和排序方向。
- 字段引用使用注册ID或完全限定ColumnRef，不接受自由字符串表达式。
- 可生成JSON Schema供LLM structured output使用。
- 可序列化、反序列化并保持语义不变。

## 3. 三层IR

### 3.1 RequirementIR

业务需求层只描述“要什么”，不描述SQL实现：

```text
request_id
metric_ids[]
dimension_ids[]
time_range
filters[]
output_grain
expected_columns[]
human_review_points[]
project_spec_ref
```

`time_range`、`filters`必须是类型化对象。原始项目书通过artifact引用保存，不直接塞入IR或Graph State。

### 3.2 SubIntent / LogicalPlan

SubIntent按planning_table、可用指标表达式和输出粒度拆分：

```text
sub_intent_id
request_id
planning_table_id
metric_ids[]
dimension_ids[]
filter_specs[]
output_grain
business_keys[]
merge_group
source_contract_refs[]
```

拆分前必须验证：

1. 指标存在于TianShu事实源。
2. 每个指标有可用G3或G2绑定。
3. 维度和日期字段可从规划表或白名单Join获得。
4. 多SubIntent存在可证明的合并键和兼容粒度。

### 3.3 SQLPlan / PhysicalPlan

SQLPlan只包含封闭的类型化节点：

```python
class ColumnRef:
    table_id: str
    column_id: str

class Literal:
    value: str | int | float | bool | date | datetime | None
    data_type: str

class Predicate:
    left: ColumnRef
    operator: PredicateOperator
    right: ColumnRef | Literal | list[Literal]

class JoinSpec:
    right_table_id: str
    join_type: JoinType
    conditions: list[Predicate]
    relationship_ref: str

class AggregateSpec:
    function: AggregateFunction
    input: ColumnRef | None
    alias: str
    distinct: bool
    metric_ref: str

class SortSpec:
    column: ColumnRef
    direction: SortDirection
    null_order: NullOrder
```

SQLPlan组合以上节点，并声明select、joins、predicates、grouping、aggregates、having、ordering和limit。

## 4. 明确禁止的IR形态

以下字段或同义字段不得出现：

- `where_clauses: list[str]`
- `join_on: str`
- `expression: str`
- `aggregation_expr: str`
- `having_sql: str`
- `raw_sql`
- 任意SQL函数调用字符串

如果需求无法由当前表达式节点表示，Planner必须返回`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`，不能退化为自由文本SQL。

## 5. 事实源解析

LLM输出的是逻辑ID，Fact Resolver确定性绑定到物理对象：

```text
metric_id → metric definition → G3物理列 / G2聚合表达式
dimension_id → semantic dimension → 物理列
relationship_ref → Join白名单 → 左右键和基数
planning_table_id → Gold表
```

编译器不得接受未解析的表名、字段名、指标名和Join关系。

## 6. SQL Compiler

编译流程：

```text
SQLPlan Schema Validation
→ Fact Resolution
→ Semantic Validation
→ SQL AST Construction
→ sqlglot / 受控Renderer输出DuckDB SQL
→ SQL AST Safety Validation
→ Artifact写入与哈希
```

同一个规范化SQLPlan和编译器版本必须产生字节一致的SQL。

## 7. 支持范围

Phase 1只支持：

- Gold层单表查询。
- 白名单中的一个受控Join。
- SELECT、WHERE、GROUP BY、HAVING、ORDER BY、LIMIT。
- 已注册的COUNT、SUM、AVG、MIN、MAX和COUNT DISTINCT。
- 明确类型的日期范围和比较谓词。

窗口函数、子查询、多跳Join和复杂表达式在后续按黄金用例逐项开放，不能因为LLM能生成就默认支持。

## 8. MergePlan

跨表多指标拆分后，由确定性MergePlanner生成MergePlan。自动合并必须同时满足：

- 输出粒度兼容。
- 合并键在每个结果中存在。
- 期望基数已声明。
- 重复键策略明确。
- 列冲突策略明确。

不满足时进入`HUMAN_REVIEW`，不得由pandas默认merge推断。

## 9. 错误与状态

| 状态 | 含义 |
|------|------|
| `PLAN_VALIDATED` | 结构和事实源校验通过 |
| `PLAN_REJECTED` | 非法字段、未注册引用或不支持表达式 |
| `COMPILED` | SQL artifact已确定性生成 |
| `RUNTIME_PASS` | DuckDB在冻结快照上执行成功 |
| `HUMAN_REVIEW` | 无法确定规划、绑定或合并语义 |

LLM的`confidence`只能作为诊断元数据，不得参与安全判定、执行许可和自动通过。

## 10. Phase 1验收标准

1. LLM输出Schema中不存在自由SQL片段字段。
2. 非法表、列、指标、Join和操作符在编译前被拒绝。
3. 相同SQLPlan重复编译产生相同SQL和哈希。
4. 单表和一个白名单Join黄金用例可在DuckDB快照上运行。
5. 不支持场景明确拒绝，不使用字符串逃生口。
6. SQLPlan、SQL artifact和ExecutionTrace可追溯到事实源和版本。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 1 实施依据
