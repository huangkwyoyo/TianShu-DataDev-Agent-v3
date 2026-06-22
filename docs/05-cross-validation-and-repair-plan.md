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

在实现某种操作前，必须声明DuckDB与Spark的等价策略：

| 语义 | 必须明确的规则 |
|------|----------------|
| NULL | 比较、Join、聚合和排序中的NULL处理 |
| NaN | 与NULL区分、排序和聚合处理 |
| Decimal | 精度、scale、溢出和舍入 |
| 时间 | 时区、日期截断、夏令时和边界包含性 |
| 字符串 | 大小写、空白、编码和排序 |
| 除法 | 整数/小数除法和除零行为 |
| Join | NULL键、重复键和期望基数 |
| 聚合 | DISTINCT、空集合和近似函数 |
| 行集合 | set、multiset或ordered sequence语义 |

未进入兼容矩阵的函数和操作不得自动标记一致，必须返回`UNSUPPORTED_SEMANTICS`或`HUMAN_REVIEW`。

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
