# SQL IR 和编译器计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 目标

建立三层、可校验、可序列化的SQL IR。LLM只能选择经过事实源注册的类型化节点，Python编译器负责生成全部SQL语法。

SQLPlan不是“长SQL字符串拼接器”，而是按阶段开放的多层SQL AST。Phase 1只实现受控`QueryPlan + SelectNode`纵向切片；开窗函数进入Phase 1.5；CTE、子查询、DDL和DML在更后续阶段按黄金用例逐项开放。

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

Phase 1的SQLPlan组合以上节点，并声明select、joins、predicates、grouping、aggregates、having、ordering和limit。

### 3.4 多层SQL AST

后续复杂SQL能力必须通过多层AST表达，不得回退为SQL文本：

```text
SqlProgram
├── QueryPlan
│   ├── SelectNode
│   ├── CTEPlan[]
│   ├── Predicate / Expression AST
│   ├── JoinSpec[]
│   ├── AggregateSpec[]
│   ├── WindowExpr[]
│   └── SortSpec[]
├── InsertPlan
├── CreateTablePlan
└── UnsupportedPlan / HumanReviewPlan
```

`SqlProgram`是可审查的SQL产物容器，不代表可以执行生产写入。Phase 1只允许`QueryPlan`中不含CTE、子查询和开窗函数的`SelectNode`子集。任何未开放节点必须在Schema或Validator阶段拒绝。

### 3.5 WindowExpr / WindowSpec

开窗函数在Phase 1.5开放，LLM只能输出结构化窗口AST，不能输出`OVER (...)`文本。

```python
class WindowExpr:
    function: WindowFunction
    input: ColumnRef | Literal | None
    partition_by: list[ColumnRef]
    order_by: list[SortSpec]
    frame: WindowFrame | None
    alias: str
    metric_ref: str | None

class WindowFrame:
    frame_type: WindowFrameType  # ROWS | RANGE
    start: FrameBoundary
    end: FrameBoundary

class FrameBoundary:
    kind: FrameBoundaryKind
    offset: int | None
```

Phase 1.5首批窗口函数白名单：

- `ROW_NUMBER`
- `RANK`
- `DENSE_RANK`
- `LAG`
- `LEAD`
- `SUM_OVER`
- `AVG_OVER`
- `COUNT_OVER`

禁止任意窗口函数名、嵌套窗口函数、窗口函数出现在WHERE、窗口函数内自由表达式、窗口函数与任意子查询组合。无法表达的窗口需求必须进入`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。

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
→ Perf Validation（Phase 1.2：REJECT 阻断 / WARN 记录）
→ SQL AST Construction
→ Compiler Passes（Phase 1.2：列裁剪 / 谓词规范化 / 无用排序消除 / 常量折叠）
→ sqlglot / 受控Renderer输出DuckDB SQL
→ SQL AST Safety Validation
→ Artifact写入与哈希
```

同一个规范化SQLPlan和编译器版本必须产生字节一致的SQL。

## 7. 支持范围

### 7.1 Phase 1支持范围

Phase 1只支持：

- Gold层单表查询。
- 白名单中的一个受控Join。
- SELECT、WHERE、GROUP BY、HAVING、ORDER BY、LIMIT。
- 已注册的COUNT、SUM、AVG、MIN、MAX和COUNT DISTINCT。
- 明确类型的日期范围和比较谓词。

Phase 1禁止窗口函数、CTE、子查询、多跳Join、DDL、DML和复杂表达式。遇到窗口需求时返回`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。

### 7.2 Phase 1.5开窗函数支持范围

Phase 1.5支持通过`WindowExpr`表达以下黄金场景：

- 分组内排序取TopN。
- 按日期累计`SUM_OVER`。
- 按业务键分区计算`ROW_NUMBER`。
- 使用`LAG`或`LEAD`生成环比字段。
- 分区内`AVG_OVER`和`COUNT_OVER`窗口指标。

每个窗口能力必须同时交付Schema、Validator、Compiler、测试和拒绝路径。相同Window SQLPlan重复编译必须产生字节一致SQL和哈希。

### 7.3 后续复杂SQL开放规则

CTE、子查询、多跳Join、DDL和DML在Phase 4及以后按黄金用例逐项开放。每开放一种SQL能力，必须满足：

1. 新增严格Pydantic模型或等价JSON Schema，并设置`extra="forbid"`。
2. Validator校验事实源、类型、作用域、引用关系和禁止字段。
3. Compiler确定性渲染SQL，不接受字符串片段。
4. Safety Validation二次确认没有自由SQL逃生口。
5. 测试覆盖合法黄金路径、非法字段、未注册引用、不支持语义和确定性哈希。
6. 无法表达时返回`PLAN_REJECTED`、`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。
7. Artifact记录AST、compiler version、fact catalog hash和schema version。

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
