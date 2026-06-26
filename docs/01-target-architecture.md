# 目标架构 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 架构原则

1. PySpark 是主要代码产物，SQL 是独立参考实现和验证手段。
2. SQL 由 SqlBuildPlan / SqlProgram 经确定性 Compiler 生成，LLM 不生成 SQL 文本或片段。
3. PySpark 由 LLM 生成，但必须满足纯转换函数、安全校验和真实执行契约。
4. SQL 与 Spark 共享 DataTransformContract 和冻结快照，不共享实现代码。
5. Comparator 只证明样本一致性；人负责业务审查和上线决策。
6. LangGraph 是薄编排层，业务节点是普通 Python 函数。

## 2. 总体数据流

```text
DeveloperSpec (.md 项目书，Markdown 正文 + YAML-like metadata block)
  → DeveloperSpec Parser（确定性解析 + 允许/禁止宽松封闭表）
  → ParsedDeveloperSpec + open_questions

ParsedDeveloperSpec
  → SourceManifest（表字段事实追踪：developer_spec / schema_registry / snapshot_profile）
  → RelationshipHypothesis（Join 推理：LLM 提候选 → Validator 证据定级 → 人工确认中低置信）
  → SqlBuildPlan / SqlProgram（受控 SQL 构建计划，8 step 类型化 DAG）
  → SQL Validator → Compiler（确定性渲染 + 优化 Pass）→ DuckDB Executor
  → SQL Code Review Package（供程序员审查）

[Spark-first v2.0]
  → DataTransformContract（从已验证 SqlBuildPlan 确定性抽取，三级递进）
  → SparkDeveloper → Static Validator → 受控 PySpark DSL
  → 双链验证（PlanEquivalence：SqlBuildPlan vs ExtractedSparkPlan + ResultComparator）
  → 人工审查
```

Snapshot Builder 必须在双引擎执行前完成。两个 Executor 只能读取同一个 snapshot_id。

### 2.1 SourceManifest 与 SchemaRegistry 冲突策略

SourceManifest 追踪每个表字段的事实来源，标记为以下三类之一：

| 来源标记 | 含义 |
|----------|------|
| `developer_spec` | 程序员在 DeveloperSpec 中显式声明的字段类型、枚举值、唯一性 |
| `schema_registry` | 从 SchemaRegistry（物理表元数据）补充的字段信息 |
| `snapshot_profile` | 从冻结快照采样推断的字段统计特征（仅用于参考，不参与决策） |

**冲突规则**：当 DeveloperSpec 中程序员已声明的值（如字段类型、枚举值、唯一性声明）与 SchemaRegistry 物理事实不一致时：

1. **双方值均输出为 `SOURCE_CONFLICT`**，进入 `open_questions`，默认 `blocking=true`。
2. **SchemaRegistry 不得静默覆盖 DeveloperSpec**——程序员声明优先保留，但物理事实也必须可见。
3. 程序员在审查 `open_questions` 时裁决——可以修正 DeveloperSpec、接受物理事实、或标记为已知差异。
4. 物理事实异常（如 SchemaRegistry 中不存在 DeveloperSpec 引用的表/字段）输出 `SOURCE_ANOMALY`，进入审查包 WARN。

此策略确保：程序员对数据口径的声明不会被机器静默篡改，同时物理现实不会被隐藏。

### 2.2 RelationshipHypothesis 三层分工与证据等级硬门禁

Join 推理是 SQL 生成中最易出错的环节。本架构将 Join 推理拆分为三层，每层由不同角色执行：

| 层 | 角色 | 职责 |
|----|------|------|
| 候选提出 | LLM（RelationshipHypothesis Planner） | 读取 ParsedDeveloperSpec + SourceManifest，提出 Join 候选（左右表、关联键、Join 类型、推定基数） |
| 证据定级 | Validator（确定性代码） | 对每个 Join 候选，逐条检查证据并定级：STRONG / MEDIUM / WEAK / NONE |
| 人工确认 | 程序员 | 审查 MEDIUM / WEAK 置信度的 Join，在 open_questions 中确认或拒绝 |

**证据等级定义**：

| 等级 | 条件 | 行为 |
|------|------|------|
| `STRONG` | 程序员在 DeveloperSpec 中显式声明了 Join 关系 + 关联键；或 SourceManifest 中存在匹配的外键约束 + 双方字段类型一致 | 自动采纳，进入 SqlBuildPlan JoinSpec |
| `MEDIUM` | 字段名归一化后匹配 + 类型兼容 + 至少一方有索引或唯一性证据，但缺乏显式声明或外键约束 | 自动采纳，但输出到 open_questions 供人工审查 |
| `WEAK` | 仅字段名相似（编辑距离或常见别名匹配）+ 类型兼容，但无任何约束、索引或声明佐证 | **拒绝进入 SqlBuildPlan**，输出到 open_questions(blocking=true)，由程序员显式确认后可升级为 MEDIUM |
| `NONE` | 无任何证据——字段名不匹配、无约束、无声明 | **拒绝进入 SqlBuildPlan**，不进入 open_questions（不浪费审查带宽），仅在 evidence_log 中记录 |

**硬门禁**：WEAK 和 NONE 等级的 Join 在任何情况下不得进入 SqlBuildPlan 的 JoinSpec。Validator 未拦截视为 Bug（Phase 4 Harness "零容忍"维度：WEAK/NONE 被采纳 = REJECT）。

### 2.3 字段名归一化规则

在 Join 推理和 SourceManifest 字段匹配中，双方字段名必须先经过确定性归一化再比较：

1. **大小写统一**：全部转为小写。
2. **驼峰转下划线**：`userId` → `user_id`，`OrderID` → `order_id`。
3. **常见别名字典**：维护一份版本化的别名字典（如 `cust_id` ↔ `customer_id`、`amt` ↔ `amount`、`dt` ↔ `date`），归一化时查表替换。
4. **去除前缀**：去除常见表名前缀（如 `user_` 前缀在 `users` 表中可配置去除：`user_name` → `name`）。
5. **去除非字母数字**：去掉空格、下划线以外的特殊字符，多个下划线合并为一个。

归一化后的字段名用于匹配，但 **SqlBuildPlan 中保留原始字段名**——归一化仅用于推理和校验阶段。

### 2.4 证据链路模板

每个 Join 推理必须输出完整证据链，格式如下：

```yaml
join_hypothesis:
  id: "join_001"
  left_table: "orders"
  right_table: "customers"
  left_field:
    raw: "cust_id"
    normalized: "customer_id"
    source: "developer_spec"       # 字段来源
    type: "bigint"
    nullable: false
  right_field:
    raw: "customer_id"
    normalized: "customer_id"
    source: "schema_registry"
    type: "bigint"
    nullable: false
  evidence:
    - type: "field_name_match"      # 归一化后完全匹配
      result: "MATCH"
    - type: "type_compatibility"    # 双方均为 bigint
      result: "MATCH"
    - type: "foreign_key"           # SchemaRegistry 中存在 FK 约束
      result: "FOUND"
      detail: "orders.cust_id → customers.customer_id"
    - type: "developer_declared"    # DeveloperSpec 中显式声明
      result: "NOT_FOUND"
  level: "STRONG"                   # 3/4 证据通过，含外键约束
  action: "AUTO_ADOPT"              # 自动采纳
```

WEAK 等级的 Join 证据链可能只有一条 `field_name_similarity` 且 `edit_distance >= 1`——此时 action 为 `REJECT_BLOCKING`，Join 不进入 SqlBuildPlan。

## 3. SQL IR：两层结构

旧架构的三层 IR（RequirementIR → SubIntent → SQLPlan）被替换为两层：

| 层 | 作用 | 对应 Phase |
|----|------|------------|
| SqlBuildPlan | 单语句受控 SQL 构建计划，使用 8 种类型化 step 表达单条 SELECT 的完整语义 | Phase 1B |
| SqlProgram | 多语句 DAG，将多个 SqlBuildPlan 编排为有序执行单元，管理 _temp 中间表生命周期 | Phase 3A |

两层之间不插入任何"LogicalPlan"中间层——SqlBuildPlan 已经是逻辑与物理的统一表达。

### 3.1 SqlBuildPlan 最小 step 范围

SqlBuildPlan 必须使用以下 8 种封闭类型化 step，不接受自由 SQL 片段：

| Step | 说明 | 关键字段 |
|------|------|----------|
| `ScanStep` | 扫描表/视图 | `table_ref`、`required_columns`、`partition_filters`、`estimated_row_count` |
| `FilterStep` | 行过滤 | `predicate: Predicate`（封闭 AST，非字符串） |
| `JoinStep` | 受控关联 | `right_table_ref`、`join_type`、`join_keys`、`relationship_ref`、`cardinality_hint` |
| `AggregateStep` | 分组聚合 | `group_keys`、`metrics: list[AggregateSpec]`、`having: Predicate | None` |
| `ProjectStep` | 列投影 | `columns: list[ColumnRef | AliasExpr]` |
| `CaseWhenStep` | 条件标签 | `cases: list[WhenBranch]`、`else_value`、`alias`（Phase 3B 开放） |
| `SortStep` | 排序 | `order_by: list[SortSpec]`、`limit: int | None` |
| `LimitStep` | 行数限制 | `limit: int`、`offset: int | None` |

每个 step 的字段均为封闭类型化对象（`ColumnRef`、`Predicate`、`JoinSpec`、`AggregateSpec`、`SortSpec`、`Literal` 等）。任何 `where_sql`、`join_on: str`、`expression: str`、`raw_sql` 字段都属于越界。

### 3.2 SqlProgram + _temp + DAG + 拓扑排序

Phase 3A 引入 SqlProgram——将多个 SqlBuildPlan 编排为多语句 DAG：

```text
SqlProgram
├── steps: list[SqlBuildPlan]           # 有序执行单元
├── dag: Mapping[str, list[str]]        # step_id → 依赖的 step_id 列表
├── temp_tables: list[TempTableSpec]    # _temp 中间表声明
│   ├── temp_id: str                    # 如 _temp_aggregated_orders
│   ├── produced_by: str                # 生产者 step_id
│   └── consumed_by: list[str]          # 消费者 step_id 列表
├── topological_order: list[str]        # 确定性拓扑排序结果
└── final_query: str | None             # 最终输出 step_id（None 表示程序以写入结束）
```

**DAG 依赖规则**：
- 每个 step 声明的 `depends_on` 只能引用同一 SqlProgram 内的其他 step。
- 循环依赖在 Validator 阶段被拒绝（`CIRCULAR_DEPENDENCY`）。
- 拓扑排序是确定性的——相同 DAG 产生相同执行顺序。

**_temp 中间表生命周期**：
- `_temp_*` 前缀表是 SqlProgram 内部中间表，不对外暴露。
- 每个 _temp 表由唯一 producer step 创建，被一个或多个 consumer step 读取。
- 程序执行完毕后，所有 _temp 表在 cleanup 阶段删除。
- _temp 表不得跨越 SqlProgram 边界——不同 SqlProgram 之间通过 DataTransformContract 传递规格，不通过临时表共享数据。

### 3.3 不实现 CTEPlan

CTE（Common Table Expression）不进入本架构。理由：

1. CTE 引入嵌套作用域，破坏 SqlBuildPlan 的扁平可审查性。
2. SqlProgram + _temp 已覆盖多步构建场景——每个中间结果物化为 _temp 表，依赖关系显式声明在 DAG 中。
3. CTE 与 _temp 同时存在会增加 Compiler、Validator 和执行器的复杂度（需处理作用域遮蔽、CTE 内 JOIN CTE 等边缘情况）。
4. 所有需要 CTE 表达的场景均可等效转换为 SqlProgram DAG——语义等价，且更易审查。

任何 CTE 需求在 Validator 阶段返回 `UNSUPPORTED_PLAN`。

## 4. DataTransformContract：三级递进

DataTransformContract 是 SQL 侧向 Spark 侧传递的权威业务规格，从已验证的 SqlBuildPlan 确定性抽取。它不包含 SQL 代码、不包含 SqlBuildPlan 实现细节，仅表达"数据要做什么变换"。

三级递进：

| 级别 | 交付 Phase | 覆盖范围 | 内容 |
|------|------------|----------|------|
| **DataTransformContract-lite** | Phase 2 | 单语句 SqlBuildPlan | 输入表/字段、过滤、Join 关系、聚合定义、输出列和类型、排序、行限制 |
| **DataTransformContract v1** | Phase 3 Exit | SqlProgram 多语句 DAG | lite 全部内容 + 多步依赖图 + _temp 中间表规格 + CASE 标签规则 + 窗口函数规格 + 受控写入方案 |
| **Phase 5 消费 v1** | Phase 5 | 完整 v1 | SparkPlan IR 从 v1 确定性映射；v1 是 Spark 侧的唯一业务输入——SparkDeveloper 不读取 DeveloperSpec |

```text
contract_id
source_sqlbuildplan_hash       # 来源 SqlBuildPlan / SqlProgram 的 SHA-256
input_tables[]                 # 输入表名和字段列表
input_columns[]                # 实际使用的列
join_relationships[]           # Join 关系（左右表、键、类型、基数提示）
filters[]                      # 过滤条件（封闭 Predicate AST）
aggregations[]                 # 聚合定义（函数、输入列、别名、distinct）
grouping_keys[]                # 分组键
case_when_labels[]             # CASE 标签规则（Phase 3B，v1 新增）
window_specs[]                 # 窗口函数规格（Phase 3B，v1 新增）
output_columns[]               # 输出列名和类型
output_grain                   # 输出粒度
write_spec                     # 写入方案（Phase 3C，v1 新增）：分区键、overwrite 范围、禁止操作列表
business_keys[]                # 业务键（用于快照构建锚点）
semantic_policy_ref            # 语义兼容策略引用
source_contract_hashes[]       # 上游 SqlBuildPlan 哈希链
```

**设计理由（约束 > 冗余）**：Spark 侧从 DataTransformContract 翻译生成 PySpark DSL，而非独立读取 DeveloperSpec 让 LLM 重新推理。理由：

1. **验证投入不浪费**——SQL 侧已通过 Validator、Executor、Comparator 验证的业务理解，Spark 侧直接复用，不需要再次承担 Join 推理错误、字段类型误判、过滤条件遗漏的风险。
2. **单一事实源**——DataTransformContract 是 SQL/Spark 共同的业务规格；两份独立理解会引入"两个 LLM 一致地误解需求"的隐蔽风险。
3. **可审查性**——人工审查者只需检查 DataTransformContract 是否正确表达了 DeveloperSpec 意图，不需要比较两套独立的 LLM 推理结果。
4. **SparkDeveloper 的 LLM 只做引擎翻译**——从封闭类型化的业务规格映射到 PySpark DSL，不重新判断业务语义。

## 5. SqlProgram DAG 依赖与拓扑排序

（本节替换旧架构的"跨 SubIntent 合并"——旧架构通过 MergePlan 合并多个 SubIntent 的结果；新架构中多语句直接由 SqlProgram DAG 编排。）

### 5.1 DAG 依赖模型

SqlProgram 内 steps 之间的依赖通过 `depends_on` 显式声明：

```text
step_1: 从 orders 表扫描，过滤 date >= '2025-01-01'
step_2: 从 customers 表扫描
step_3: 依赖 [step_1, step_2]，Join orders 与 customers
step_4: 依赖 [step_3]，按 zone 聚合 SUM(amount)
step_5: 依赖 [step_4]，按 total_amount DESC 排序 + LIMIT 10
```

对应 DAG：`step_1 → step_3 → step_4 → step_5`，`step_2 → step_3`。

### 5.2 拓扑排序确定性

Validator 对 DAG 执行拓扑排序，规则：
- 使用 Kahn 算法，同时从所有入度为 0 的节点开始。
- 同级节点按 `step_id` 字典序打破平局——保证相同 DAG 产生相同顺序。
- 循环依赖被检测并拒绝（`CIRCULAR_DEPENDENCY`）。

### 5.3 自动合并的边界

当两个 step 输出相同粒度、兼容列且需要合并时，由 SqlProgram 显式插入一个 `union_step`（而非旧架构的隐式 MergePlan）：
- 输出粒度不兼容 → 拒绝，进入 `HUMAN_REVIEW`。
- 合并键在任一结果中缺失 → 拒绝。
- 列冲突无明确策略 → 拒绝。
- 期望基数未声明 → 允许但记录 WARN。

## 6. Artifact 优先的 Graph State

LangGraph State 只保存小型结构化字段，语义更新为 DeveloperSpec-first 命名：

```python
class GraphState(TypedDict):
    request_id: str
    developer_spec_ref: str                     # DeveloperSpec artifact 引用
    parsed_developer_spec_ref: str | None       # ParsedDeveloperSpec artifact 引用
    source_manifest_ref: str | None             # SourceManifest artifact 引用
    relationship_hypothesis_refs: list[str]      # RelationshipHypothesis artifact 引用列表
    sql_build_plan_refs: list[str]              # SqlBuildPlan artifact 引用列表
    sql_program_ref: str | None                 # SqlProgram artifact 引用
    data_transform_contract_refs: list[str]      # DataTransformContract artifact 引用
    snapshot_manifest_ref: str | None
    sql_artifact_refs: list[str]
    spark_artifact_refs: list[str]
    test_artifact_refs: list[str]
    execution_trace_refs: list[str]
    result_summary_refs: list[str]
    comparison_report_ref: str | None
    diagnosis_ref: str | None
    repair_directive_ref: str | None
    retry_count: int
    assurance_level: str
    final_status: str
```

禁止在 State 中保存 DataFrame、完整结果集、完整代码、完整 DeveloperSpec 正文、凭据或无限聊天历史。具体内容落盘后通过路径和 SHA-256 引用。

## 7. 执行环境边界

- Snapshot Builder 只读访问开发数据源。
- DuckDB 与 Spark 在隔离环境读取 Parquet 快照。
- EnvironmentManifest 固定引擎版本、时区、ANSI 模式、大小写、Decimal 和 NaN 策略。
- SQL、Spark 代码和 LLM 生成的测试代码必须先静态验证再执行。
- 每次执行设置超时、内存、CPU、行数和输出大小限制。
- Executor 不拥有生产凭据和写入目标。

## 8. Code Review Package

```text
generated/review_packages/{request_id}/
├── developer_spec/              # DeveloperSpec 原文与解析结果
├── source_manifest/             # SourceManifest + SOURCE_CONFLICT 记录
├── plans/                       # SqlBuildPlan + SqlProgram
├── contracts/                   # DataTransformContract（lite 或 v1）
├── sql/                         # 编译产物 SQL + OptimizedSQLPlan
├── spark/                       # PySpark DSL 代码
├── tests/                       # 测试代码
├── snapshots/snapshot_manifest.yml
├── traces/                      # ExecutionTrace + ResultSummary
├── reports/                     # Comparator 报告 + 差异诊断
├── lineage/source_refs.yml
├── provenance.yml
└── review.md
```

`provenance.yml` 至少记录：代码哈希、Prompt 版本、模型标识、事实源哈希（SourceManifest + SchemaRegistry）、快照哈希、执行环境指纹和返工轮次。

## 9. 组件替换边界

- 所有业务节点接收结构化输入并返回结构化输出，可脱离 LangGraph 测试。
- LLM 调用统一通过 Gateway，但 Gateway 不解析领域语义。
- Storage 只负责 artifact 持久化和哈希，不参与状态判定。
- Validator 和 Comparator 是确定性模块，不依赖 LLM。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | Phase 1A 前置事实源
