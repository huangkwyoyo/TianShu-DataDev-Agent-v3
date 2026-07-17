# Phase 5：SparkPlan IR + DataTransformContractV1 确定性映射

> 状态：**已完成 ✅（2026-06-29）**
> 前置依赖：Phase 4.6 退出 ✅ | Phase 4.5 内部验证口 ✅ | Harness 七维门禁 ✅

## 交付物

### 1. SparkPlan IR 模型（`src/tianshu_datadev/spark/models.py`）

定义 Spark 侧的类型化中间表示——9 种 step 类型 + 顶层容器 + UNSUPPORTED_PATTERN / CONTRACT_GAP。

| Step 类型 | 对应 SQL Step | 说明 |
|-----------|--------------|------|
| `SparkReadStep` | ScanStep | `{alias} = inputs["{source_name}"]`（物理路径在 SnapshotManifest） |
| `SparkFilterStep` | FilterStep | `df.filter(condition)` |
| `SparkJoinStep` | JoinStep | `df.join(other, on=keys, how=join_type)` |
| `SparkAggregateStep` | AggregateStep | `df.groupBy(keys).agg(*metrics)` |
| `SparkProjectStep` | ProjectStep | `df.select(*columns)` |
| `SparkCaseWhenStep` | CaseWhenStep | `F.when(cond, result).otherwise(else_val)` |
| `SparkWindowStep` | WindowStep | `F.row_number().over(Window.partitionBy(...))` |
| `SparkSortStep` | SortStep | `df.orderBy(*sorts)` |
| `SparkLimitStep` | LimitStep | `df.limit(n)` |

所有模型继承 `StrictModel`（`extra="forbid"`），不包含 SQL 文本字段。
`SparkPlan` 支持确定性 hash（`compute_plan_hash()`）。

### 2. 确定性映射器（`src/tianshu_datadev/spark/mapper.py`）

`map_contract_to_spark_plan()` —— DataTransformContractV1 → SparkPlan 的纯函数映射。

- 9 种 step 类型全覆盖
- 聚合函数白名单：COUNT / COUNT_DISTINCT / SUM / AVG / MIN / MAX
- 窗口函数白名单：ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD / SUM_OVER / AVG_OVER / COUNT_OVER
- Join 类型白名单：INNER / LEFT / RIGHT / FULL
- 不支持的函数 → `UnsupportedPattern`；缺失必要信息 → `ContractGap`（BLOCKING / WARN）
- 相同 Contract → 相同 SparkPlan → 相同 hash

### 3. PlanEquivalence 规则（`src/tianshu_datadev/spark/plan_equivalence.py`）

9 条 step 类型对比规则——Phase 7 PlanComparator 消费。

| 对比函数 | 对比内容 |
|----------|----------|
| `compare_scan_steps` | 输入表别名的集合等价 |
| `compare_filter_steps` | (left, operator, right) 三元组等价 |
| `compare_join_steps` | (left/right table, key, type) 五元组等价 |
| `compare_aggregate_steps` | group_keys + metrics 集合等价（顺序无关） |
| `compare_project_steps` | 输出列 (column_name, alias) 集合等价 |
| `compare_case_when_steps` | labels 集合 + else_value 等价 |
| `compare_window_steps` | (function, alias, partition_by, order_by) 等价 |
| `compare_sort_steps` | (column, direction) 列表等价 |
| `compare_limit_steps` | limit + offset 值等价 |

`compare_plans()` 入口函数支持 SQL/Spark 侧 step type 名自动归一化（scan↔read）。

### 4. 测试（`tests/spark/test_spark_plan.py`）

45 个测试覆盖：
- 12 个模型创建 + 严格性 + hash 确定性
- 10 个映射器（golden 全 9 种 step、确定性、空输入拒绝、不支持函数、write_spec）
- 3 个字段名归一化
- 20 个 PlanEquivalence（每种 step 类型的等价/不等价 + 完整 plan 对比）

## 验收命令

```bash
python -m pytest tests/spark/ -q
python -m ruff check src/tianshu_datadev/spark/ tests/spark/
```

## 不支持的模式（Phase 6/7 按需处理）

| 模式 | 状态 | 原因 |
|------|------|------|
| 相关子查询 | UNSUPPORTED | SubqueryStep Phase 4.6 仅支持 FROM 子查询 |
| CTE（WITH ... AS） | 永久禁止 | AGENTS.md 架构决策——用 SqlProgram + _temp 替代 |
| CROSS JOIN | UNSUPPORTED | 无证据链的笛卡尔积 |
| 自定义窗口帧 | UNSUPPORTED | Phase 5 仅保留函数名/分区/排序，不保留帧边界 |
| 未知聚合/窗口函数 | CONTRACT_GAP (BLOCKING) | 不在白名单内的函数名 |

## 禁止事项

- SparkPlan 包含自由代码片段
- 映射依赖 LLM（必须是确定性纯函数）
- SparkDeveloper 读取 SQL 文本或 SqlBuildPlan
- SparkPlan 节点超出 SQL 侧已验证节点范围

---

> Phase 5 | 已完成 | 2026-06-29
