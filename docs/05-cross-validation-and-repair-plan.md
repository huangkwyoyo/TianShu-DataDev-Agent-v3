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

## 8. 人工 Review Feedback 返工入口

> 本节定义人工 Review 不通过后的结构化反馈与返工路由边界。与 §5 RepairPlanner（自动 Comparator 差异修复）互补——§5 覆盖自动发现的双链不一致，本节覆盖人工发现的逻辑/事实/需求错误。两者共享同一 `retry_count` 上限和 `HUMAN_REVIEW` 退出路由。

### 8.1 与自动返工的区别

| 维度 | 自动返工（§5） | 人工 Review 返工（§8） |
|------|---------------|---------------------|
| 触发源 | Comparator 输出 `DIFFERENT` | 人工审查 Code Review Package 发现问题 |
| 输入 artifact | DifferenceReport | ReviewFeedback |
| 能发现的问题 | SQL/Spark 双链不一致 | 两个引擎一致地理解错了需求、事实源缺失、Join 逻辑错误、性能隐患 |
| 路由依据 | DifferenceClassifier + RepairPlanner | `target`（机器路由主字段）+ `finding_type`（细分原因） |

### 8.2 ReviewFeedback artifact 最小字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `request_id` | str | 本次返工请求唯一标识 |
| `review_package_id` | str | 被审查的 Code Review Package ID |
| `developer_spec_hash` | str | 审查时使用的 DeveloperSpec 版本 |
| `source_manifest_hash` | str | 审查时使用的 SourceManifest 版本 |
| `sql_build_plan_hash` | str | 被审查的 SqlBuildPlan 版本 |
| `sql_artifact_hash` | str | 被审查的 SQL artifact 版本 |
| `target` | enum | 机器路由主字段：`REQUIREMENT` / `SQL_PLAN` / `COMPILER_BUG` / `SOURCE_FACT` / `HUMAN_REVIEW` |
| `finding_type` | str | 细分原因（如 `wrong_metric`、`missing_nullable`、`join_type_error`、`perf_missing_broadcast`） |
| `comment` | str | 人工审查意见 |
| `suggested_resolution` | str? | 可选，建议的修复方向 |

`target` 是路由主字段，`finding_type` 不参与路由决策——避免自由文本型字段变成隐式路由条件。

### 8.3 路由表

| `target` | 返工入口 | 说明 |
|----------|---------|------|
| `REQUIREMENT` | DeveloperSpec / Parser / Planner | 需求理解错误，修改 DeveloperSpec 或补 HumanResolution，重新走全链路 |
| `SOURCE_FACT` | SourceManifest / SchemaRegistry / open_questions | 表字段事实缺失或冲突，补全事实源后重新抽取 Contract |
| `SQL_PLAN` | SqlBuildPlan 重新生成 | SQL 计划错误或 Join 关系错误；Join 问题进入 RelationshipHypothesis 重新定级，不靠 Memory；禁止直接改 SQL 文本 |
| `COMPILER_BUG` | Compiler 修复 + 回归测试 | 确定性 Compiler 渲染错误，修 Compiler 并加 regression fixture |
| `HUMAN_REVIEW` | **停止自动返工** | 反馈无法结构化、证据不足、需求变化不明确或目标无法归类 |

性能问题不进入 ReviewFeedback 路由——经人工确认后直接补强 PerfValidator / Compiler Pass / Optimizer 确定性规则。

### 8.4 核心约束

- ReviewFeedback 是**版本化 artifact**，不是 Memory。通过 artifact 引用 + hash + checkpoint + retry_count 关联历史上下文。
- Agent 读取上一版 DeveloperSpec、SourceManifest、SqlBuildPlan、ReviewFeedback，生成新版 artifact。"在原有基础上修改"不依赖长期学习 Memory。
- 每次返工重新经过 Validator、Executor、Comparator，与自动返工共享同一 `retry_count` 上限（最多 2 轮）。
- 可复现 Review 经验沉淀为 regression fixture、Validator/Compiler 规则、Schema/Contract 标注或 Prompt/Harness 回归样本，不进入运行时可检索 Memory。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 占位——Phase 4 退出后重写
