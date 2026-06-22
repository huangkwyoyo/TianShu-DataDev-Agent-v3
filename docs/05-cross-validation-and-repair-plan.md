# 交叉验证和修复计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 目标

让DuckDB SQL和PySpark在同一个关系一致快照上真实执行，经统一语义规范化后进行确定性比较。比较结果只证明样本实现一致，不证明需求理解、全量性能或生产行为正确。

## 2. 关系一致快照

多表需求禁止对每张表独立`LIMIT N`。Snapshot Builder必须：

1. 从SubIntent确定锚点事实表、时间范围和业务键。
2. 采用固定seed和确定性哈希选择锚点键。
3. 依据Join白名单级联抽取所需维表和关联事实行。
4. 保留重复键、NULL键和未匹配键的真实分布。
5. 对每张表写Parquet、Schema和内容哈希。
6. 写入抽取SQL/规则、事实源版本和EnvironmentManifest引用。

```text
snapshots/{snapshot_id}/
├── tables/{logical_table_id}.parquet
├── schemas/{logical_table_id}.json
├── relational_keys.yml
├── extraction_plan.yml
├── environment.yml
└── snapshot_manifest.yml
```

两个Executor必须校验同一个`snapshot_id`和manifest hash。

## 3. EnvironmentManifest

必须固定并记录：

- DuckDB、Spark、Python、PyArrow版本。
- session timezone。
- Spark ANSI模式和case sensitivity。
- Decimal精度与舍入策略。
- NaN、NULL、日期和时间戳策略。
- locale、字符串规范化和排序规则。
- Comparator版本和容差配置。

## 4. SemanticCompatibilityPolicy

在实现某种操作前，必须声明DuckDB与Spark的等价策略。Spark与SQL对NULL等行为**存在明确语义差异**——不能直接比较原始结果，必须经Normalizer逐项处理。

| 语义 | 必须明确的规则 |
|------|----------------|
| NULL | 比较、Join、聚合和排序中的NULL处理（详见下方 NULL 差异表） |
| NaN | 与NULL区分、排序和聚合处理（详见下方 NaN 差异表） |
| Decimal | 精度、scale、溢出和舍入 |
| 时间 | 时区、日期截断、夏令时和边界包含性 |
| 字符串 | 大小写、空白、编码和排序 |
| 除法 | 整数/小数除法和除零行为 |
| Join | NULL键、重复键和期望基数 |
| 聚合 | DISTINCT、空集合和近似函数 |
| 行集合 | set、multiset或ordered sequence语义 |

### 4.1 DuckDB vs Spark NULL 语义差异（必须逐项归一化）

两个引擎都遵循 SQL 标准的多数规则，但以下关键行为差异**不能**通过原始结果直接比较来消除：

| 操作 | DuckDB 行为 | Spark 行为 | 归一化策略 |
|------|------------|-----------|-----------|
| **NULL 排序** | 默认 `NULLS FIRST`（升序） | 默认 `NULLS LAST`（升序） | Normalizer 必须显式指定 `NULLS LAST` 后再比较，或按业务键排序消除不确定性 |
| **NULL = NULL** | 返回 `NULL`（非 TRUE） | 返回 `NULL`（非 TRUE） | 一致——但 `WHERE col = NULL` 在两个引擎中都不返回行。Comparator 不能使用等值比较来判断 NULL 匹配 |
| **GROUP BY NULL** | NULL 归为一组 | NULL 归为一组 | 一致——但 Normalizer 必须用 `COALESCE` 或显式 NULL 标记统一两组输出的表示 |
| **SUM(全NULL列)** | 返回 `NULL` | 返回 `NULL` | 一致——但 `SUM` + `COALESCE(0)` 在两个引擎中行为不同，因 COALESCE 评估时机取决于执行计划 |
| **Window 分区中 NULL** | `PARTITION BY NULL` 将所有行归入同一分区 | 同 DuckDB | 一致——但 `ORDER BY NULL` 在不同分区中的排序不稳定性可能导致行顺序差异 |
| **NULL 在 JOIN 键** | NULL 键不匹配任何行（含另一个 NULL） | 同 DuckDB | 一致——但 LEFT JOIN 保留左表 NULL 键行，右表列填 NULL；Comparator 必须用业务键 multiset 而非位置比较 |
| **IS NULL vs = NULL** | `IS NULL` 检测 NULL，`= NULL` 返回 NULL | 同 DuckDB | 一致——但 Normalizer 必须在比较前将 NULL 标记为 `__NULL__` 哨兵值，禁止直接用 `=` 判断 |
| **COALESCE 短路** | 从左到右返回第一个非 NULL | 同 DuckDB | 一致——但 `COALESCE(Spark_UDF(), default)` 这类场景不适用于纯转换函数契约 |

**关键结论**：NULL 语义差异不能靠一句"声明等价策略"解决。Normalizer 必须在比较前完成：(1) 显式 NULL 排序方向、(2) NULL 哨兵值替换、(3) 排除基于 `=` 的 NULL 匹配。

### 4.2 DuckDB vs Spark NaN 语义差异

| 操作 | DuckDB 行为 | Spark 行为 | 归一化策略 |
|------|------------|-----------|-----------|
| **NaN 存在性** | DuckDB 不区分 NaN 和 NULL——所有特殊浮点值（NaN/+Inf/-Inf）在 DuckDB 中视为 NULL | Spark 区分 NaN、+Inf、-Inf 和 NULL，`isNaN()` 可单独检测 | Normalizer 必须在 Spark 侧将 NaN 显式替换为 `__NaN__` 哨兵值，DuckDB 侧 NaN 已是 NULL |
| **NaN 聚合** | NaN 作为 NULL 被忽略 | `SUM` 包含 NaN 时结果可能是 NaN | 聚合前必须由 Normalizer 统一 NaN → NULL 映射 |
| **NaN 排序** | NaN 作为 NULL 参与排序 | NaN 排序位置引擎特定（通常大于所有非 NULL 值） | Normalizer 必须将 NaN 归一化为统一哨兵后再排序 |

**关键结论**：DuckDB 根本没有 NaN 概念。Spark 侧的 NaN 必须在进入 Comparator 之前显式替换——否则会因"DuckDB 报 NULL 计数、Spark 另有 NaN 计数"而反复产生 DIFFERENT。

## 5. ResultNormalizer

Comparator之前统一执行：

- 列名和列顺序映射到TransformationContract。
- 类型映射到规范化类型系统。
- 时间转换到声明时区。
- Decimal按策略量化。
- NaN与NULL保留区别。
- 按业务键确定性排序；无业务键时使用规范化行哈希排序。
- 使用multiset计数保留重复行语义。

Normalizer不得修改业务值来“消除差异”。

## 6. 确定性比较维度

1. 输出列和顺序。
2. 规范化数据类型。
3. 行数。
4. 业务键集合与重复次数。
5. 规范化multiset行内容。
6. 每列NULL和NaN数量。
7. 数值汇总及容差。
8. 分类值分布摘要。
9. 内容摘要哈希。
10. TransformationContract不变量和MergePlan约束。

每个维度输出`MATCH`、`DIFFERENT`、`NOT_APPLICABLE`或`UNSUPPORTED`，不能把未执行项当作匹配。

## 7. 精确状态

| 状态 | 含义 |
|------|------|
| `NOT_EXECUTED` | 至少一个引擎没有产生可比较结果 |
| `RUNTIME_PASS` | 单个引擎在当前快照运行成功 |
| `DIFFERENT` | 至少一个必需维度不一致 |
| `UNSUPPORTED_SEMANTICS` | 当前兼容策略无法证明等价 |
| `CONSISTENT_SAMPLE` | 两个结果在当前快照和全部必需维度上一致 |
| `REVIEW_READY` | 样本一致且审查材料、来源和不确定项完整 |
| `HUMAN_REVIEW` | 自动诊断或返工无法继续 |

禁止使用泛化`PASS`表示上述状态。

## 8. 一致但仍可能错误

SQL和Spark可能共同误解RequirementIR。为降低相关错误：

- 人工确认RequirementIR和TransformationContract。
- Spark角色不得查看SQL实现。
- Harness维护人工标注的黄金结果和语义不变量。
- TestPlan包含行数边界、守恒关系、单调性、唯一性等属性测试。
- 对关键黄金用例进行变异测试，确保Comparator和测试能发现故意错误。

## 9. 差异诊断和返工

```text
DIFFERENT
→ Deterministic DifferenceClassifier
→ DifferenceAnalyst LLM
→ RepairPlanner LLM
→ RepairDirective
   ├─ SQL_PLAN
   ├─ SPARK_CODE
   ├─ BOTH
   ├─ REQUIREMENT
   └─ HUMAN_REVIEW
→ 对应生成节点
→ Validator
→ Executor
→ Comparator
```

DifferenceAnalyst只能读取结构化差异、契约、trace摘要和必要的脱敏样本，不读取无限日志或生产数据。

SQL返工只能生成新的SQLPlan；Spark返工由Developer根据OptimizationDirective生成新代码。每次返工产生新artifact和哈希，旧版本保留在repair history中。

## 10. 返工上限

- `retry_count`初始为0。
- 最多执行2次自动修订。
- 未知根因、事实源缺失、Requirement需要改变或两轮后仍不一致，立即进入`HUMAN_REVIEW`。
- LLM不能延长重试上限，也不能覆盖Comparator结论。

## 11. Phase 3/4验收标准

1. 多表快照保持白名单关系和锚点键一致。
2. SQL/Spark环境配置和输入快照可复现。
3. NULL、NaN、Decimal、时间、重复行和Join基数具有明确策略。
4. 任一引擎未运行时不能产生`CONSISTENT_SAMPLE`。
5. 一致结果只升级到`CONSISTENT_SAMPLE`，材料完整后才为`REVIEW_READY`。
6. 差异诊断不改变确定性比较结果。
7. 两轮返工上限和`HUMAN_REVIEW`路由可确定性测试。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 3/4 实施依据
