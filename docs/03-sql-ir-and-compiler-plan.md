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
    # 优化元数据：无 LIMIT 且输入估算大时，Validator 可触发 PERF-005 WARN
    estimated_input_rows: int | None = None
```

Phase 1的SQLPlan组合以上节点，并声明select、joins、predicates、grouping、aggregates、having、ordering和limit。

### 3.3.1 节点优化元数据字段

为支撑确定性 PerfValidator 和 Compiler Pass 的优化决策，SQLPlan 各节点携带以下优化元数据。这些字段由 SQL Planner（LLM）填充初始估算值，由 Fact Resolver 补充事实源信息，最终由 PerfValidator 校验：

**ScanNode 优化字段：**

```python
class ScanNode:
    table_ref: str                          # 事实源注册表ID
    required_columns: list[ColumnRef]       # 实际需要的列——不得为空或等于 SELECT *
    predicates: list[Predicate]             # 扫描阶段可下推的过滤
    partition_filters: list[Predicate]      # 可作用于 Parquet 分区的过滤（如日期分区键）
    estimated_row_count: int | None         # 事实源提供的近似行数，用于基数判断
```

`partition_filters` 将时间范围过滤与 Parquet 分区裁剪直接关联——Compiler 可在渲染时生成分区感知 SQL，避免全表扫描。Fact Resolver 负责校验分区列是否真实存在。

**JoinNode 优化字段：**

```python
class JoinNode:
    left: QueryPlanNode
    right: QueryPlanNode
    right_table_ref: str
    join_type: JoinType
    join_keys: list[tuple[ColumnRef, ColumnRef]]
    relationship_ref: str
    cardinality_hint: str | None            # 事实源声明的基数关系："1:1" | "1:N" | "N:M" | None
    pre_aggregation_allowed: bool = False   # 目标粒度允许在 Join 前先对大表聚合
```

`pre_aggregation_allowed` 直接支撑 PERF-008 规则——当目标粒度（如 zone/day）低于事实表明细粒度（如 trip_id），且业务语义允许时，Planner 可将此字段设为 True，Validator 据此判断是否需要 WARN。

**SortNode 优化字段：**

```python
class SortNode:
    order_by: list[SortSpec]
    limit: int | None
    requires_full_sort: bool = False        # True 表示无 LIMIT 或 LIMIT 极大，需全量排序
    estimated_input_rows: int | None = None
```

`requires_full_sort` 由 Planner 根据是否有 LIMIT 及 LIMIT 大小设置。当 `requires_full_sort=True` 且 `estimated_input_rows` 超过阈值时，PerfValidator（PERF-005）发出 WARN。

**WindowNode 优化字段（Phase 1.5 生效）：**

```python
class WindowNode:
    partition_by: list[ColumnRef]
    order_by: list[SortSpec]
    frame: WindowFrame | None
    window_exprs: list[WindowExpr]
    estimated_partition_size: int | None    # 估算的每分区平均行数
```

`estimated_partition_size` 支撑 Phase 1.5 的窗口性能规则（PERF-009/010）——分区过大时触发 WARN 或 REJECT。

以上优化字段均为可选（默认 None/False），不影响 Phase 1 功能正确性，但为 Phase 1.2 及后续阶段的性能门禁提供 IR 层可表达、可校验的结构化载体。Planner Prompt 要求 LLM 填充这些字段的初始值，Fact Resolver 和 PerfValidator 负责最终校验和决策。

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

### 6.1 SQL 侧是否需要独立 LLM Performance Reviewer？

本项目在 Spark 侧设有 `SparkReviewer`（LLM）输出结构化 `OptimizationDirective`，但 SQL 侧**不设独立的 LLM SQL Performance Reviewer 节点**。这不是遗漏，而是基于以下设计权衡：

1. **SQL 语义空间比 PySpark 窄得多**。SQLPlan 是封闭类型化 AST——列裁剪、谓词规范化、无用排序消除和常量折叠这四类优化已被确定性 Compiler Pass 全覆盖。LLM 再审查同一份 SQLPlan，要么复读已有规则，要么引入不可证伪的"风格建议"。

2. **LLM 不应做性能决策**（AGENTS.md §2）。如果让 LLM 审查 SQLPlan 并提出 `OptimizationDirective`，就赋予了它"判断什么是慢查询"的权力——这正是本项目刻意避免的。SparkReviewer 的存在是因为 PySpark 代码空间更广、静态规则覆盖不全，需要 LLM 辅助发现模式问题（如笛卡尔积、数据倾斜风险），但其 Directive 仍需经过确定性 Validator 才能落地。

3. **SQL Planner 自身已承担"生成时即优化"的职责**。PerfRule 注册表的 `get_prompt_hints()` 将优化方向注入 Planner Prompt，Planner 在生成 SQLPlan 时就必须遵循这些原则——不需要事后由另一个 LLM 再审查一遍。

4. **如果未来需要**（如 Phase 4+ 出现复杂 CTE/子查询场景），可以启用 LLM SQL Reviewer，但其输出仍限于 `OptimizationDirective`，落地必须回到 SQLPlan，且重新经过 Schema / Semantic / Perf / Safety 全套校验。届时只需在编译流程中 **Perf Validation 之后、Compiler Passes 之前** 增加一个可选节点即可——当前架构已为此预留空间。

综上：**SQL 侧的优化走"Planner Prompt 软约束 + IR 元数据表达 + PerfValidator 硬门禁 + Compiler Pass 确定改写"四层闭环，不需要 LLM 中间审查**。这与 Codex 的 C 方案（IR 级优化规则体系为核心）完全一致——Codex 的 B 方案（LLM Reviewer）在 C 方案足够强时可以省略。

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
