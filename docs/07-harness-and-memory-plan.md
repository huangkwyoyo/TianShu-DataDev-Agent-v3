# Phase 4 Harness + 回归 / 规则 / Schema 标注 — TianShu DataDev Agent v3

> 文档版本：2026-07-13 更新版
> 状态：Phase 4 已退出，Harness 七维门禁已落地，Phase 8 Spark 5 维度评测已完成。当前测试基线 2568 passed / 24 skipped / 10 预存失败。

## 1. 当前状态

**Phase 4 已退出。** Harness 评测系统已实现，七维门禁可用。本文保留 Harness 设计参考；Spark 扩展维度设计见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §3.3。

## 2. Harness 七维门禁定义

| 维度 | 名称 | REJECT 条件 |
|------|------|-------------|
| 1 | 结构化输出通过率 | 合法 DeveloperSpec 的结构化输出通过率 < 95% |
| 2 | Join 推理质量（零容忍） | 漏报率 > 0 **或** WEAK/NONE 被采纳 > 0 **或** 缺失证据链 > 0 |
| 3 | SQL 编译成功率 | 合法 SqlBuildPlan 的编译成功率 < 99% |
| 4 | SQL 执行成功率 | 编译产物在 DuckDB 快照上的执行成功率 < 95% |
| 5 | 拒绝准确性 | 不支持场景正确拒绝率 < 90% |
| 6 | 安全门禁 | 六种攻击向量任一漏报 |
| 7 | 确定性 | 相同输入两次生成 SqlBuildPlan 哈希不同 |

## 3. 评测数据集目录结构

```text
harness/datasets/
├── golden/                        # 黄金 DeveloperSpec → 预期 SqlBuildPlan
├── rejection/                     # 应被拒绝的非法输入
├── attack/                        # 六种攻击向量
│   ├── attack_001_prompt_injection.md
│   ├── attack_002_sql_injection.md
│   ├── attack_003_schema_breakout.md
│   ├── attack_004_undeclared_ref.md
│   ├── attack_005_join_error.md
│   └── attack_006_write_escalation.md
├── performance/                   # 性能边界（15 条 PERF 规则）
└── regression/                    # 回归用例
```

## 4. 失败沉淀：回归 / 规则 / Schema 标注（不做独立 Engineering Memory）

**本项目不建设独立 Engineering Memory。** 失败、经验与模型行为变化不进入运行时可检索 Memory。可复现工程失败按以下路径沉淀：

| 失败类型 | 落点 | 示例 |
|---------|------|------|
| LLM 对某类 DeveloperSpec 稳定产出错误 IR | `harness/datasets/regression/` + pytest | JOIN 类型、输出粒度、拒绝路径 |
| 可自动检测的低效或危险执行模式 | Optimizer / Compiler / Validator 确定性规则 | broadcast、filter pushdown、全表排序 WARN |
| 事实源缺失或语义标注不足 | SchemaRegistry / SourceManifest / DataTransformContract 字段 | nullable、枚举、唯一性、时区策略 |
| Prompt 暗示不足导致结构化输出不稳定 | Prompt 回归样本 + Harness 指标 | structured output extra 字段、缺证据链 |

- **事实源隔离**：表、字段、Join 和业务口径的事实源是 SourceManifest / SchemaRegistry / Contract——禁止用 Memory 覆盖或补写事实源。
- **无自写入**：禁止建立可半自动写入的 `memory/domain` 来补充或覆盖事实源。

## 5. Harness 非运行时依赖

- Harness 是离线质量工程系统，不得被产品运行时 `import`
- Harness 数据库和日志独立于产品运行时的 artifact store
- 评测结果可生成 HarnessReport，但不参与自动路由、自动通过或自动上线决策

## 6. Phase 8 Spark 扩展维度（已完成）

Phase 8 在已有 SQL Harness 基础上新增 5 个 Spark 专属评测维度（已落地，详见 `docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md` §3.3）：

| 维度 | 说明 |
|------|------|
| `SPARK_CONTRACT_FIDELITY` | SparkPlan 与 Contract 的一致性 |
| `SPARK_COMPILATION_DETERMINISM` | 同一 Contract 两次编译 → 相同 raw 代码 |
| `SPARK_VALIDATOR_COVERAGE` | Static Validator 对 8 种错误码的拦截覆盖率 |
| `SPARK_LOGIC_EQUIVALENCE` | SQL/Spark PlanComparator 结构一致 |
| `SPARK_PHYSICAL_CONSISTENCY` | 双引擎同快照结果一致 |

## 7. 验收标准

1. Harness 不被产品运行时 import
2. 模型输出评测不依赖全文快照
3. 能区分 `RESULT_CONSISTENT`（样本一致）、黄金正确率和人工接受率
4. Domain Knowledge 只有一个事实源——SourceManifest / SchemaRegistry
5. 失败案例沉淀路径完整（回归 / 规则 / Schema 标注 / Prompt 回归），不存在独立 Memory 写入流程
6. Harness case、Prompt 版本、模型版本和失败沉淀落点均可追溯

---

> 2026-07-03 更新：移除占位标记，Phase 4 已退出。Spark 扩展维度设计见 superpowers/specs。
