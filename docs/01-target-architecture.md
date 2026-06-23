# 目标架构 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 架构原则

1. PySpark是主要代码产物，SQL是独立参考实现和验证手段。
2. SQL由类型化SQLPlan确定性编译，LLM不生成SQL文本或片段。
3. PySpark由LLM生成，但必须满足纯转换函数、安全校验和真实执行契约。
4. SQL与Spark共享TransformationContract和冻结快照，不共享实现代码。
5. Comparator只证明样本一致性；人负责业务审查和上线决策。
6. LangGraph是薄编排层，业务节点是普通Python函数。

## 2. 总体数据流

```text
ProjectSpec artifact
  → RequirementIR
  → Human confirmation gate
  → SubIntent[]
  → TransformationContract[]
  → RelationalSnapshotManifest
      ├─ SQLPlan → SQL Compiler → SQL Validator → DuckDB Executor
      └─ SparkDeveloper → Spark Validator → SparkReviewer
           → SparkDeveloper revision → SparkTester → Test Validator
           → Spark Executor
  → ResultNormalizer
  → DeterministicComparator
      ├─ CONSISTENT_SAMPLE → REVIEW_READY packaging
      └─ DIFFERENT → DifferenceAnalyst → RepairPlanner
           → retry (最多2轮) / HUMAN_REVIEW
```

Snapshot Builder必须在双引擎执行前完成。两个Executor只能读取同一个snapshot_id。

## 3. 三层SQL IR

| 层 | 作用 | 是否允许自由表达式 |
|----|------|--------------------|
| RequirementIR | 表达业务目标、指标、维度、过滤、时间与粒度 | 否 |
| SubIntent / LogicalPlan | 按planning_table和可合并粒度拆分业务任务 | 否 |
| SQLPlan / PhysicalPlan | 绑定物理表列、Join、谓词、聚合与排序 | 否 |

SQLPlan使用`ColumnRef`、`Literal`、`Predicate`、`JoinSpec`、`AggregateSpec`、`SortSpec`等封闭类型。任何`where_sql`、`join_on: str`、`expression: str`都属于越界。

SQLPlan在实现上按阶段扩展为多层SQL AST。Phase 1只允许`QueryPlan + SelectNode`的受控子集；Phase 1.5新增`WindowExpr`和`WindowSpec`；CTE、子查询、DDL和DML必须在后续阶段以独立Schema、Validator、Compiler、测试和拒绝路径成套开放。未开放节点不得通过自由SQL字段绕过。

## 4. TransformationContract

每个SubIntent在进入双分支前必须生成同一份TransformationContract：

```text
contract_id
sub_intent_id
input_tables[]
input_columns[]
metric_bindings[]
join_paths[]
filters[]
grouping_keys[]
output_columns[]
output_types[]
output_grain
business_keys[]
semantic_policy_ref
source_contract_hashes[]
```

TransformationContract是两种实现的共同业务规格，不含SQL或Spark代码。

## 5. 跨SubIntent合并

多SubIntent不得直接按一个字符串`merge_key`合并。必须存在MergePlan：

```text
merge_plan_id
input_summary_refs[]
join_keys[]
expected_cardinality
join_type
grain_compatibility
column_conflict_policy
missing_key_policy
output_schema
```

粒度不兼容、Join基数未知或业务键不唯一时，禁止自动合并并进入`HUMAN_REVIEW`。

## 6. Artifact优先的Graph State

LangGraph State只保存小型结构化字段：

```python
class GraphState(TypedDict):
    request_id: str
    project_spec_ref: str
    requirement_ir_ref: str | None
    sub_intent_refs: list[str]
    transformation_contract_refs: list[str]
    snapshot_manifest_ref: str | None
    sql_plan_refs: list[str]
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

禁止在State中保存DataFrame、完整结果集、完整代码、完整原始项目书或无限聊天历史。具体内容落盘后通过路径和SHA-256引用。

## 7. 执行环境边界

- Snapshot Builder只读访问开发数据源。
- DuckDB与Spark在隔离环境读取Parquet快照。
- EnvironmentManifest固定引擎版本、时区、ANSI模式、大小写、Decimal和NaN策略。
- SQL、Spark代码和LLM生成的测试代码必须先静态验证再执行。
- 每次执行设置超时、内存、CPU、行数和输出大小限制。
- Executor不拥有生产凭据和写入目标。

## 8. Code Review Package

```text
generated/review_packages/{request_id}/
├── requirement/
├── contracts/
├── plans/
├── sql/
├── spark/
├── tests/
├── snapshots/snapshot_manifest.yml
├── traces/
├── reports/
├── lineage/source_refs.yml
├── provenance.yml
└── review.md
```

`provenance.yml`至少记录：代码哈希、Prompt版本、模型标识、事实源哈希、快照哈希、执行环境指纹和返工轮次。

## 9. 组件替换边界

- 所有业务节点接收结构化输入并返回结构化输出，可脱离LangGraph测试。
- LLM调用统一通过Gateway，但Gateway不解析领域语义。
- Storage只负责artifact持久化和哈希，不参与状态判定。
- Validator和Comparator是确定性模块，不依赖LLM。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 1 前置事实源
