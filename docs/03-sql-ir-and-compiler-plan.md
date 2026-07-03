# SQL IR 和编译器计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 目标

建立两层、可校验、可序列化的 SQL IR。LLM 只能输出经过 Schema 约束的类型化节点（SqlBuildPlan / SqlProgram），Python Compiler 负责生成全部 SQL 语法。

SqlBuildPlan 不是"长 SQL 字符串拼接器"，而是按阶段开放的封闭类型化 AST。Phase 1B 实现 SqlBuildPlan 8 step 单语句；Phase 3A 实现 SqlProgram 多语句 DAG；窗口函数和 CASE 标签进入 Phase 3B；CTE 永不实现（以 SqlProgram + _temp 替代）。

## 2. 运行时模型选择

Phase 0 中的 `Protocol` 只用于接口探索，不能作为 LLM 结构化输出和持久化契约。Phase 1 必须使用 Pydantic 模型或等价的严格 JSON Schema，并满足：

- `extra="forbid"`，拒绝未知字段。
- Enum 限制操作符、Join 类型、聚合函数、排序方向和证据等级。
- 字段引用使用注册 ID 或完全限定 ColumnRef，不接受自由字符串表达式。
- 可生成 JSON Schema 供 LLM structured output 使用。
- 可序列化、反序列化并保持语义不变。

以下类型必须使用严格 Pydantic 模型实现：

| 模型 | Phase | 关键约束 |
|------|-------|----------|
| `ParsedDeveloperSpec` | 1A | `extra="forbid"`；`open_questions` 中每个 OpenQuestion 必须包含 `blocking: bool` |
| `OpenQuestion` | 1A | `question_id`、`source`（parser/source_manifest/relationship）、`blocking`、`resolution: None | HumanResolution` |
| `SourceConflict` | 1A | `field_ref`、`developer_spec_value`、`schema_registry_value`、`conflict_type`（TYPE_MISMATCH/ENUM_MISMATCH/UNIQUENESS_MISMATCH/MISSING_IN_REGISTRY） |
| `SourceManifest` | 1A | 每个字段标记 `source: developer_spec | schema_registry | snapshot_profile` |
| `RelationshipHypothesis` | 1B | `join_candidates: list[JoinCandidate]`，每个候选带 `evidence: list[EvidenceItem]` 和 `level: EvidenceLevel`（STRONG/MEDIUM/WEAK/NONE） |
| `EvidenceItem` | 1B | `evidence_type`、`result`（MATCH/MISMATCH/FOUND/NOT_FOUND）、`detail: str` |
| `SqlBuildPlan` | 1B | `steps: list[StepNode]`，至少一个 step；step 类型从 8 种 step 枚举 |
| `StepNode` | 1B | 区分联合类型：`ScanStep | FilterStep | JoinStep | AggregateStep | ProjectStep | CaseWhenStep | SortStep | LimitStep` |
| `SqlProgram` | 3A | `steps: list[SqlBuildPlan]`、`dag: dict[str, list[str]]`、`temp_tables: list[TempTableSpec]`、`topological_order: list[str]` |
| `DataTransformContract` | 2 (lite) / 3 Exit (v1) | 从 SqlBuildPlan / SqlProgram 确定性抽取；不包含实现代码字段 |

## 3. IR 结构

旧架构的三层 IR（RequirementIR → SubIntent/LogicalPlan → SQLPlan/PhysicalPlan）被替换为以下结构：

### 3.1 ParsedDeveloperSpec（输入层）

DeveloperSpec Parser 将 Markdown + YAML-like 项目书确定性解析为结构化 ParsedDeveloperSpec：

```text
spec_id
spec_hash: str                          # normalized_spec_hash——相同输入两次解析 hash 一致
title: str
description: str
input_tables:
  - table_alias: str
    source_table: str                   # 物理表名
    columns: list[ColumnDecl]           # 程序员声明的列（名称、类型、枚举值、nullable、unique）
    filters: list[FilterDecl]           # 表级预过滤
metrics: list[MetricDecl]               # 程序员声明的指标（名称、聚合函数、输入列、别名）
dimensions: list[DimensionDecl]         # 维度声明
joins: list[JoinDecl] | None            # 程序员显式声明的 Join 关系（可选——缺失时由 RelationshipHypothesis 推理）
time_range: TimeRangeDecl | None
output_spec: OutputSpecDecl
open_questions: list[OpenQuestion]      # Parser 无法确定的问题（blocking=true 时阻断后续流程）
parse_warnings: list[ParseWarning]      # 非阻断警告
```

`time_range`、`filters`、`joins` 必须是类型化对象。原始 Markdown 项目书通过 artifact 引用保存，不直接塞入 IR 或 Graph State。

**字段名归一化**：Parser 将所有字段名归一化（大小写统一、驼峰转下划线、常见别名字典替换、去除非字母数字字符），归一化后的字段名存储在 ParsedDeveloperSpec 中，原始字段名保留在 `raw_field_name` 字段供追溯。

### 3.2 RelationshipHypothesis + SqlBuildPlan（推理与计划层）

#### 3.2.1 RelationshipHypothesis

RelationshipHypothesis Planner（LLM）读取 ParsedDeveloperSpec + SourceManifest，输出 Join 候选推理：

```text
hypothesis_id
spec_id
source_manifest_hash
join_candidates:
  - candidate_id: str
    left_table: str
    right_table: str
    left_key: ColumnRef                # 归一化后的字段引用
    right_key: ColumnRef
    join_type: JoinType
    cardinality_hint: str | None       # "1:1" | "1:N" | "N:M" | None
    evidence: list[EvidenceItem]       # 逐条证据
    level: EvidenceLevel               # STRONG | MEDIUM | WEAK | NONE（由 Validator 确定性填入）
    action: EvidenceAction             # AUTO_ADOPT | HUMAN_CONFIRM | REJECT_BLOCKING（由 Validator 确定性填入）
```

**三层分工**：
1. **LLM 提候选**——Planner 读取 ParsedDeveloperSpec 和 SourceManifest，提出可能的 Join 候选（不填 level 和 action）。
2. **Validator 确定性定级**——对每个候选，逐条检查证据并填入 level 和 action。规则是确定性的——相同输入产生相同定级。
3. **人工确认中低置信**——MEDIUM 等级的 Join（以及程序员从 WEAK 升级的 Join）进入 `open_questions` 供人工审查。

**证据类型**（Validator 检查的维度）：

| 证据类型 | 检查内容 |
|----------|----------|
| `field_name_match` | 归一化后字段名完全匹配 |
| `field_name_similarity` | 归一化后编辑距离 ≤ 2 或常见别名匹配 |
| `type_compatibility` | 双方字段类型兼容（int ↔ bigint、varchar ↔ text 视为兼容，int ↔ varchar 视为不兼容） |
| `foreign_key` | SchemaRegistry 中存在外键约束 |
| `unique_index` | 至少一方有唯一索引 |
| `developer_declared` | DeveloperSpec 中程序员显式声明了此 Join |
| `column_statistics` | 快照采样数据支持（如右表键去重率高）——仅作为辅助证据，不单独决定等级 |

**证据等级硬门禁**：
- WEAK 和 NONE 等级在任何情况下不得进入 SqlBuildPlan 的 JoinSpec。
- Validator 未拦截视为 Bug——Phase 4 Harness "零容忍"维度检查：WEAK/NONE 被采纳 = REJECT。
- NONE 不进入 open_questions（不浪费审查带宽）；WEAK 进入 open_questions(blocking=true)，程序员可显式升级为 MEDIUM。

**证据链模板**：每个 Join 候选必须输出完整证据链（参见 `docs/01-target-architecture.md` §2.4）。

#### 3.2.2 SqlBuildPlan

SqlBuildPlan Planner（LLM）读取 ParsedDeveloperSpec + SourceManifest + RelationshipHypothesis（仅 STRONG 和 MEDIUM 等级的 Join），输出类型化 SqlBuildPlan：

```text
plan_id
spec_id
hypothesis_id
source_manifest_hash
steps: list[StepNode]                   # 有序 step 列表，至少一个
```

**8 种 Step 的封闭类型定义**：

```python
class ScanStep:
    table_ref: str                      # SourceManifest 中注册的表 ID
    required_columns: list[ColumnRef]   # 实际需要的列——不得为空或等于 SELECT *
    predicates: list[Predicate]         # 扫描阶段可下推的过滤
    partition_filters: list[Predicate]  # 可作用于 Parquet 分区的过滤（如日期分区键）
    estimated_row_count: int | None     # SourceManifest 提供的近似行数

class FilterStep:
    predicate: Predicate                # 封闭 AST，非字符串

class JoinStep:
    right_table_ref: str
    join_type: JoinType
    join_keys: list[tuple[ColumnRef, ColumnRef]]
    relationship_ref: str               # 对应 RelationshipHypothesis 中的 candidate_id
    cardinality_hint: str | None        # "1:1" | "1:N" | "N:M" | None
    pre_aggregation_allowed: bool = False

class AggregateStep:
    group_keys: list[ColumnRef]
    metrics: list[AggregateSpec]
    having: Predicate | None

class ProjectStep:
    columns: list[ColumnRef | AliasExpr]

class CaseWhenStep:                     # Phase 3B 开放
    cases: list[WhenBranch]
    else_value: Literal | None
    alias: str

class SortStep:
    order_by: list[SortSpec]
    limit: int | None
    requires_full_sort: bool = False    # True 表示无 LIMIT 或 LIMIT 极大
    estimated_input_rows: int | None = None

class LimitStep:
    limit: int
    offset: int | None = None
```

**基础类型**：

```python
class ColumnRef:
    table_ref: str                      # 表引用（来自 SourceManifest）
    column_name: str                    # 原始字段名（非归一化）
    normalized_name: str                # 归一化字段名

class Literal:
    value: str | int | float | bool | None
    data_type: str

class Predicate:
    left: ColumnRef | Predicate         # Predicate 支持嵌套（AND/OR/NOT）
    operator: PredicateOperator
    right: ColumnRef | Literal | list[Literal] | None  # IS_NULL/IS_NOT_NULL 时 right 为 None

class AggregateSpec:
    function: AggregateFunction         # COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT
    input: ColumnRef | None             # COUNT(*) 时 input 为 None
    alias: str
    distinct: bool = False

class SortSpec:
    column: ColumnRef
    direction: SortDirection            # ASC | DESC
    null_order: NullOrder               # FIRST | LAST

class AliasExpr:
    expression: ColumnRef | AggregateSpec | CaseWhenStep
    alias: str

class WhenBranch:
    condition: Predicate
    result: Literal
```

**预聚合优化字段**：`JoinStep.pre_aggregation_allowed` 支撑性能规则——当目标粒度低于事实表细粒度且业务语义允许时，Planner 可设为 True，允许 Compiler 在 Join 前先对大表聚合。此字段不影响功能正确性，仅影响 Compiler Pass 行为。

**SortStep 优化字段**：`requires_full_sort` 由 Planner 设置，当为 True 且 `estimated_input_rows` 超过阈值时，PerfValidator 发出 PERF-005 WARN。

### 3.3 SqlProgram + Compiler（多语句程序与编译层）

#### 3.3.1 SqlProgram

Phase 3A 引入 SqlProgram——将多个 SqlBuildPlan 编排为多语句 DAG：

```text
program_id
spec_id
steps: list[SqlBuildPlan]               # 有序执行单元列表
dag: dict[str, list[str]]               # step_id → 依赖的 step_id 列表
temp_tables: list[TempTableSpec]        # _temp 中间表声明
topological_order: list[str]            # 确定性拓扑排序结果
final_output: str | None               # 最终输出 step_id（None 表示程序以写入结束）
```

```python
class TempTableSpec:
    temp_id: str                        # 如 "_temp_aggregated_orders"
    produced_by: str                    # 生产者 step_id
    consumed_by: list[str]              # 消费者 step_id 列表
    schema: list[ColumnRef]             # 中间表列定义
    cleanup_after: str                  # "program_end"（程序结束后清理）
```

**DAG 约束**：
- 每个 step 的 `depends_on` 只能引用同一 SqlProgram 内的其他 step_id。
- 循环依赖在 Validator 阶段被拒绝（错误码 `CIRCULAR_DEPENDENCY`）。
- 拓扑排序使用 Kahn 算法，同级节点按 step_id 字典序打破平局——确定性保证。
- _temp 表不得跨越 SqlProgram 边界——不同 SqlProgram 之间通过 DataTransformContract 传递规格。

#### 3.3.2 不实现 CTEPlan

CTE（Common Table Expression）不进入本 IR。理由：
1. CTE 引入嵌套作用域，破坏 step 的扁平可审查性。
2. SqlProgram + _temp 已覆盖所有 CTE 使用场景——每个 CTE 等效为一个 _temp 中间表 + producer step。
3. 避免 Compiler 需同时处理 CTE 作用域遮蔽、CTE 内 JOIN CTE、递归 CTE 等边缘情况。
4. 语义等价：`WITH cte AS (...) SELECT ... FROM cte` 等效于 `CREATE TEMP TABLE _temp_cte AS ...; SELECT ... FROM _temp_cte`。

任何 CTE 需求在 Validator 阶段返回 `UNSUPPORTED_PLAN`。

### 3.4 多层 SQL AST（更新）

后续复杂 SQL 能力通过多层 AST 表达，不回退为 SQL 文本：

```text
SqlProgram
├── SqlBuildPlan[]                     # 每个 step 是一个独立 SqlBuildPlan
│   ├── ScanStep
│   ├── FilterStep
│   ├── JoinStep
│   ├── AggregateStep
│   ├── ProjectStep
│   ├── CaseWhenStep                   # Phase 3B
│   ├── SortStep
│   └── LimitStep
├── TempTableSpec[]                    # _temp 中间表规格
├── DAG 依赖图
└── 拓扑排序结果
```

**CTEPlan 不存在**。子查询不在 Phase 1-3 范围内（后续阶段如需开放，必须按成套规则交付：Schema + Validator + Compiler + 测试 + 拒绝路径）。

### 3.5 WindowExpr / WindowSpec（Phase 3B）

开窗函数在 Phase 3B 开放，LLM 只能输出结构化窗口 AST，不能输出 `OVER (...)` 文本：

```python
class WindowExpr:
    function: WindowFunction
    input: ColumnRef | Literal | None
    partition_by: list[ColumnRef]
    order_by: list[SortSpec]
    frame: WindowFrame | None
    alias: str

class WindowFrame:
    frame_type: WindowFrameType         # ROWS | RANGE
    start: FrameBoundary
    end: FrameBoundary

class FrameBoundary:
    kind: FrameBoundaryKind             # CURRENT_ROW | UNBOUNDED_PRECEDING | UNBOUNDED_FOLLOWING | N_PRECEDING | N_FOLLOWING
    offset: int | None
```

Phase 3B 窗口函数白名单（8 种）：

- `ROW_NUMBER`
- `RANK`
- `DENSE_RANK`
- `LAG`
- `LEAD`
- `SUM_OVER`
- `AVG_OVER`
- `COUNT_OVER`

禁止任意窗口函数名、嵌套窗口函数、窗口函数出现在 WHERE 子句、窗口函数内自由表达式、窗口函数与子查询组合。无法表达的窗口需求必须进入 `UNSUPPORTED_PLAN` 或 `HUMAN_REVIEW`。

## 4. 明确禁止的 IR 形态

以下字段或同义字段不得出现在 SqlBuildPlan / SqlProgram 的任何节点中：

- `where_clauses: list[str]`
- `where_sql: str`
- `join_on: str`
- `expression: str`
- `aggregation_expr: str`
- `having_sql: str`
- `raw_sql`
- 任意 SQL 函数调用字符串
- 未注册的表名、字段名、Join 关系

如果需求无法由当前表达式节点表示，Planner 必须返回 `UNSUPPORTED_PLAN` 或 `HUMAN_REVIEW`，不能退化为自由文本 SQL。

## 5. SourceManifest：事实源解析

SourceManifest 是表字段事实的唯一追踪点，替代旧架构的 Fact Catalog Adapter：

```text
SourceManifest
├── tables[]
│   ├── table_ref: str                  # 注册表 ID
│   ├── source_table: str               # 物理表名
│   ├── columns[]
│   │   ├── column_name: str            # 原始字段名
│   │   ├── normalized_name: str        # 归一化字段名
│   │   ├── data_type: str
│   │   ├── nullable: bool
│   │   ├── unique: bool | None
│   │   ├── enum_values: list[str] | None
│   │   └── source: FieldSource         # developer_spec | schema_registry | snapshot_profile
│   ├── primary_key: list[str] | None
│   ├── foreign_keys: list[ForeignKeyRef] | None
│   └── estimated_row_count: int | None
├── conflicts: list[SourceConflict]      # SOURCE_CONFLICT 条目
└── anomalies: list[SourceAnomaly]       # SOURCE_ANOMALY 条目
```

**解析流程**：
1. 从 ParsedDeveloperSpec 提取程序员声明的表/字段/类型 → 标记 `source=developer_spec`。
2. Optional SchemaRegistry 补充物理表元数据（字段类型、nullable、主键、外键、行数估算）→ 标记 `source=schema_registry`。
3. Optional 快照采样补充统计特征（去重率、NULL 比例、值分布）→ 标记 `source=snapshot_profile`。
4. 冲突检测：同一字段在 developer_spec 和 schema_registry 中值不一致 → 输出 `SOURCE_CONFLICT`。

**SchemaRegistry 定位**：只补充 developer_spec 中缺失的字段信息（如程序员未声明的 nullable），不静默覆盖程序员已声明的值。冲突时双方值均保留，由程序员裁决。

Compiler 不得接受未在 SourceManifest 中注册的表名、字段名和 Join 关系。

## 6. SQL Compiler

编译流程：

```text
SqlBuildPlan / SqlProgram Schema Validation
  → SourceManifest Fact Resolution（绑定所有 table_ref、column_ref、relationship_ref）
  → Semantic Validation（Join key 类型一致、未声明引用拒绝、WEAK/NONE Join 门禁）
  → Perf Validation（Phase 1C：REJECT 阻断 / WARN 记录）
  → SQL AST Construction
  → Compiler Passes（列裁剪、谓词规范化、无用排序消除、常量折叠）
  → sqlglot / 受控 Renderer 输出 DuckDB SQL
  → SQL AST Safety Validation（二次确认无自由 SQL 逃生口）
  → Artifact 写入与哈希
```

同一个规范化 SqlBuildPlan 和编译器版本必须产生字节一致的 SQL 和相同 SHA-256。

### 6.1 Compiler Pass（优化管道）

每个 Pass 必须是幂等的——重复运行不改变结果：

1. **列裁剪**：移除 ScanStep 中未被后续 step 引用的 `required_columns`。
2. **谓词规范化**：`BETWEEN` → `>= AND <`；`DATE() = '2025-01-01'` → `>= '2025-01-01' AND < '2025-01-02'`；`strftime` → 等价范围表达式；移除恒真/恒假条件。
3. **无用排序消除**：无 LIMIT 且输出不依赖顺序的 SortStep → 移除。
4. **常量折叠**：`1 + 2` → `3`；`TRUE AND x` → `x`；`x IS NOT NULL AND x IS NOT NULL` → `x IS NOT NULL`。

每个 Pass 的优化决策记录在 OptimizedSQLPlan 中，包含输入/输出 AST 片段和应用的规则列表。

### 6.2 为何 SQL 侧不设独立 LLM Performance Reviewer

SQL 侧的优化走 **"Planner Prompt 软约束 + IR 元数据表达 + PerfValidator 硬门禁 + Compiler Pass 确定改写"四层闭环**，不需要 LLM 中间审查：

1. SQL 语义空间比 PySpark 窄得多——SqlBuildPlan 是封闭类型化 AST，四类优化已被确定性 Compiler Pass 全覆盖。LLM 再审查要么复读已有规则，要么引入不可证伪的"风格建议"。
2. LLM 不应做性能决策（AGENTS.md §2）——让 LLM 判断"什么是慢查询"会赋予它不应有的决策权。
3. SqlBuildPlan Planner 自身已承担"生成时即优化"的职责——PerfRule 注册表的 Prompt 提示将优化方向注入 Planner Prompt。
4. 如果未来需要（如 Phase 4+ 出现复杂场景），LLM SQL Reviewer 可在 Perf Validation 之后、Compiler Passes 之前插入——架构已预留空间。

## 7. 支持范围

### 7.1 Phase 1B 支持范围

Phase 1B 只支持：
- 单表查询 + 白名单中的一个受控 Join（STRONG 或 MEDIUM 等级）。
- 8 种 step：ScanStep、FilterStep、JoinStep、AggregateStep、ProjectStep、SortStep、LimitStep（CaseWhenStep 延后到 Phase 3B）。
- 已注册的 COUNT、SUM、AVG、MIN、MAX 和 COUNT DISTINCT 聚合函数。
- 明确类型的日期范围和比较谓词。

Phase 1B 禁止窗口函数、CTE、子查询、多跳 Join、DDL、DML、CASE WHEN 和复杂表达式。遇到不支持需求时返回 `UNSUPPORTED_PLAN` 或 `HUMAN_REVIEW`。

### 7.2 Phase 3B 窗口函数与 CASE 标签支持范围

Phase 3B 通过 `WindowExpr` 和 `CaseWhenStep` 表达以下场景：

- 分组内排序取 TopN（ROW_NUMBER + SortStep + FilterStep）。
- 按日期累计 SUM_OVER。
- 按业务键分区计算 ROW_NUMBER。
- 使用 LAG/LEAD 生成环比字段。
- 分区内 AVG_OVER 和 COUNT_OVER 窗口指标。
- CASE WHEN 标签分类（枚举值必须在 DeveloperSpec 中声明，未声明枚举值被拒绝）。

每个窗口和 CASE 能力必须同时交付 Schema、Validator、Compiler、测试和拒绝路径。相同 SqlBuildPlan 重复编译必须产生字节一致 SQL 和哈希。

### 7.3 后续复杂 SQL 开放规则

子查询和多跳 Join 已纳入专属 Phase 4.6 规划（参见 docs/roadmap/phase-4-6-complex-sql-opening.md）。DDL/DML 在 Phase 4 及以后按黄金用例逐项开放。每开放一种 SQL 能力，必须满足以下全部条件：

1. 新增严格 Pydantic 模型或等价 JSON Schema，`extra="forbid"`。
2. Validator 校验 SourceManifest 注册、类型、引用关系和禁止字段。
3. Compiler 确定性渲染 SQL，不接受字符串片段。
4. Safety Validation 二次确认无自由 SQL 逃生口。
5. 测试覆盖合法黄金路径、非法字段、未注册引用、不支持语义和确定性哈希。
6. 无法表达时返回 `PLAN_REJECTED`、`UNSUPPORTED_PLAN` 或 `HUMAN_REVIEW`。
7. Artifact 记录 AST、compiler version、source_manifest_hash 和 schema version。

## 8. 错误与状态

| 状态 | 含义 |
|------|------|
| `SPEC_PARSED` | DeveloperSpec 已解析为 ParsedDeveloperSpec |
| `SOURCE_RESOLVED` | SourceManifest 已完成事实源绑定 |
| `HYPOTHESIS_RATED` | RelationshipHypothesis 已完成证据定级 |
| `PLAN_VALIDATED` | SqlBuildPlan / SqlProgram 结构和事实源校验通过 |
| `PLAN_REJECTED` | 非法字段、未注册引用、WEAK/NONE Join 或不受支持表达式 |
| `COMPILED` | SQL artifact 已确定性生成 |
| `EXECUTION_PASS` | DuckDB 在冻结快照上执行成功 |
| `HUMAN_REVIEW` | 无法确定规划、绑定、Join 或合并语义 |

LLM 的 `confidence` 只能作为诊断元数据，不得参与安全判定、执行许可和自动通过。

## 9. DataTransformContract 抽取

DataTransformContract 从已验证的 SqlBuildPlan（Phase 2 lite）/ SqlProgram（Phase 3 Exit v1）确定性抽取：

**lite（Phase 2）**：从单个 SqlBuildPlan 抽取——输入表/字段、过滤条件、Join 关系、聚合定义、输出列和类型、排序、行限制。不包含 SQL 代码。

**v1（Phase 3 Exit）**：从 SqlProgram 抽取——lite 全部内容 + 多步 DAG 依赖图 + _temp 中间表规格 + CASE WHEN 标签规则 + 窗口函数规格 + FinalWritePlan 写入方案。不包含 SQL 代码和 SqlBuildPlan 实现细节。

抽取是确定性的——相同 SqlBuildPlan / SqlProgram 产生相同 DataTransformContract 和相同哈希。Spark 侧只读取 DataTransformContract 作为业务规格输入。

## 10. Phase 1 验收标准

1. LLM 输出 Schema 中不存在自由 SQL 片段字段。
2. 非法表、列、Join 和操作符在编译前被拒绝（Validator 阶段）。
3. WEAK/NONE Join 被硬门禁拦截，任何时候不得进入 SqlBuildPlan。
4. 相同 SqlBuildPlan 重复编译产生相同 SQL 和 SHA-256。
5. 单表和一个白名单 Join 黄金用例可在 DuckDB 快照上运行。
6. 不支持场景明确拒绝，不使用字符串逃生口。
7. SqlBuildPlan、SQL artifact 和 ExecutionTrace 可追溯到 DeveloperSpec、SourceManifest 和编译器版本。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | Phase 1B/1C 实施依据
