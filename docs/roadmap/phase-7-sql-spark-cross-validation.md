# Phase 7：SQL/Spark 双链验证 + 交叉验证

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 状态：占位——Phase 4 退出后重写
> 前置依赖：Phase 6 受控 PySpark DSL

## 当前占位概要

### 目标

双链验证：
1. **逻辑链路**（PlanEquivalence）：SqlBuildPlan step 与 ExtractedSparkPlan step 的结构等价比较
2. **物理链路**（ResultComparator）：SQL 和 Spark 在同一冻结快照上的执行结果比较

### Comparator 状态枚举

| 状态 | 含义 |
|------|------|
| `NOT_EXECUTED` | 至少一个引擎没有产生可比较结果 |
| `RUNTIME_PASS` | 单个引擎在当前快照运行成功 |
| `DIFFERENT` | 至少一个必需维度不一致 |
| `UNSUPPORTED_SEMANTICS` | 当前兼容策略无法证明等价 |
| `CONSISTENT_SAMPLE` | 两个结果在当前快照和全部必需维度上一致 |
| `REVIEW_READY` | 样本一致且审查材料完整 |

### 同快照约束

- SQL 与 Spark 必须读取同一个关系一致、不可变的 Parquet 快照
- 多表快照使用锚点键和 Join 白名单级联抽取，禁止各表独立 LIMIT

### RepairPlanner 路由

```text
DIFFERENT
→ Deterministic DifferenceClassifier
→ DifferenceAnalyst LLM（只读结构化差异，不读生产数据）
→ RepairPlanner LLM
→ RepairDirective
   ├─ SQL_PLAN        → SQL Planner 返工
   ├─ SPARK_CODE      → SparkDeveloper 返工
   ├─ BOTH            → 双方同时返工
   ├─ REQUIREMENT     → DeveloperSpec 需修正
   └─ HUMAN_REVIEW    → 自动无法继续
→ 每次返工产生新 artifact 和哈希
```

### 返工上限

- `retry_count` 初始为 0，最多 2 轮自动返工
- 未知根因、事实源缺失、需求变化或超限 → `HUMAN_REVIEW`
- LLM 不能延长重试上限，不能覆盖 Comparator 结论

### 验收标准骨架

1. SQL/Spark 读取同一快照
2. Comparator 产生精确状态
3. 返工最多 2 轮
4. 无法确定时进入 HUMAN_REVIEW

---

> Phase 7 | 占位 | Phase 4 退出后由实施 Prompt 重写
