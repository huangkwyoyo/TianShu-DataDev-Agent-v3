# Phase 7 双链验证与修复 — TianShu DataDev Agent v3

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版（占位）

## 1. 当前状态

**等待 Phase 4 退出。** Phase 7 的详细实施规格——包括具体 Comparator 实现、差异定位算法、Normalizer 逐项规则——必须在 Phase 4 硬化完成、SQL/Spark 双链的真实差异模式和误报率已知后才能确定。

## 2. 前置依赖

- Phase 6 受控 PySpark DSL 退出
- DataTransformContract v1 作为 SQL/Spark 共同业务规格
- Harness 七维门禁通过（尤其是 Join 推理质量维度）

## 3. Comparator 状态枚举

| 状态 | 含义 | 不代表 |
|------|------|--------|
| `NOT_EXECUTED` | 至少一个必需执行没有结果 | — |
| `RUNTIME_PASS` | 单引擎在当前快照运行成功 | 不代表双引擎一致 |
| `DIFFERENT` | 必需比较维度不一致 | — |
| `UNSUPPORTED_SEMANTICS` | 当前兼容策略不能证明等价 | — |
| `CONSISTENT_SAMPLE` | 当前快照和比较维度一致 | **不代表业务绝对正确、全量一致或生产就绪** |
| `REVIEW_READY` | 材料完整，可进入人工代码审查 | 不代表获准上线 |
| `HUMAN_REVIEW` | 自动化无法安全继续 | — |

禁止使用泛化 `PASS` 表示上述状态。

## 4. 核心约束

- SQL 与 Spark 必须读取同一个关系一致、不可变的 Parquet 快照
- 多表快照使用锚点键和 Join 白名单级联抽取，禁止各表独立 LIMIT
- 两个 Executor 必须校验同一个 `snapshot_id` 和 manifest hash
- Comparator 是确定性模块，LLM 不能决定验证是否通过
- `CONSISTENT_SAMPLE` ≠ 正确——SQL 和 Spark 可能基于同一个错误的 DeveloperSpec"一致地算错"

## 5. RepairPlanner 路由

```text
DIFFERENT
→ Deterministic DifferenceClassifier
→ DifferenceAnalyst LLM
→ RepairPlanner LLM
→ RepairDirective
   ├─ SQL_PLAN        → 返回 SQL Planner（生成新 SqlBuildPlan）
   ├─ SPARK_CODE      → 返回 SparkDeveloper
   ├─ BOTH            → 双方同时返工
   ├─ REQUIREMENT     → DeveloperSpec 需修正
   └─ HUMAN_REVIEW    → 自动诊断或返工无法继续
```

**返工上限**：`retry_count` 初始为 0，最多 2 轮自动修订。未知根因、事实源缺失、需求变化或超限 → `HUMAN_REVIEW`。

## 6. 双链验证

- **逻辑链路**（PlanEquivalence）：SqlBuildPlan step vs ExtractedSparkPlan step 结构等价比较
- **物理链路**（ResultComparator）：SQL 和 Spark 执行结果的 10 维度规范化比较

## 7. 验收标准骨架

1. 多表快照保持白名单关系和锚点键一致
2. SQL/Spark 环境配置和输入快照可复现
3. NULL、NaN、Decimal、时间、重复行和 Join 基数具有明确归一化策略
4. 任一引擎未运行时不能产生 `CONSISTENT_SAMPLE`
5. 差异诊断不改变确定性比较结果
6. 两轮返工上限和 `HUMAN_REVIEW` 路由可确定性测试

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 占位——Phase 4 退出后重写
