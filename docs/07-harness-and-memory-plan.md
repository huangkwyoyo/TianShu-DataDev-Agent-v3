# Phase 4 Harness + Phase 8 Memory — TianShu DataDev Agent v3

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版（占位）

## 1. 当前状态

**等待 Phase 4 退出。** 本文覆盖 Phase 4 的 Harness 评测系统和 Phase 8 的 Engineering Memory。具体的 Harness 实现代码、指标阈值、评测数据集——必须在 Phase 4 真实 LLM 硬化过程中校准后才能确定。

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

## 4. Memory 禁令

- **Memory 不覆盖 DeveloperSpec / SourceManifest / SchemaRegistry**——表、字段、Join 和业务口径的事实源是 SourceManifest / SchemaRegistry，不属于可写 Domain Memory
- Engineering Memory 在 Phase 8 前不参与运行时路由
- 只有满足以下条件的经验才能进入 Engineering Memory：
  1. 来源于可复现失败案例
  2. 有人工批准记录
  3. 记录适用范围、反例、来源 artifact 和失效条件
  4. 版本化并可撤销
- 禁止建立可半自动写入的 `memory/domain` 来补充或覆盖事实源

## 5. Harness 非运行时依赖

- Harness 是离线质量工程系统，不得被产品运行时 `import`
- Harness 数据库和日志独立于产品运行时的 artifact store
- 评测结果可生成 HarnessReport，但不参与自动路由、自动通过或自动上线决策

## 6. 验收标准骨架

1. Harness 不被产品运行时 import
2. 模型输出评测不依赖全文快照
3. 能区分样本一致率（CONSISTENT_SAMPLE）、黄金正确率和人工接受率
4. Domain Knowledge 只有一个事实源——SourceManifest / SchemaRegistry
5. Engineering Memory 不能未经人工批准自动写入或影响运行时
6. 每次检索和 Prompt/模型版本均可追溯

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 占位——Phase 4 退出后重写
