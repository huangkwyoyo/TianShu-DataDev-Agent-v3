# 产品宪章 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 产品目标

TianShu DataDev Agent v3 是 **AI 辅助数据开发工具**。它接收数据开发项目书，生成达到“开发审查级”的 SQL、PySpark DataFrame DSL、测试与验证材料，最终交付 Code Review Package。

最终产物是代码，不是生产数据。系统不自动部署、不写生产库，也不替代程序员审批。

## 2. “开发审查级”的精确定义

只有同时满足以下条件，产物才能标记为 `REVIEW_READY`：

1. 项目书已转换为结构化 RequirementIR，并经事实源校验。
2. SQL 由类型化 SQLPlan 经 Python 确定性编译生成。
3. PySpark 代码满足受控纯转换函数契约。
4. SQL 与 Spark 均在同一个冻结快照上真实执行。
5. 确定性 Comparator 给出 `CONSISTENT_SAMPLE`。
6. 所有 WARN、不支持语义和人工确认项已写入审查材料。

`REVIEW_READY` 只表示可以进入代码审查，不表示业务绝对正确、全量性能合格或生产就绪。

### 2.1 方案可行性前提

本方案能得到可用 PySpark DSL，但前提是**四根支柱同时成立**：

1. **受控代码生成**——LLM 输出受 TransformationContract 和 Static Validator 严格约束，禁止自由脚本。
2. **真实 Spark 执行**——代码在隔离 Sandbox 中读取注入快照并真实运行，不依赖模拟或静态分析替代。
3. **同源快照**——SQL 和 Spark 读取同一份冻结 Parquet，禁止分别抽样。
4. **确定性交叉验证**——Comparator 由代码逻辑判定一致性，不由 LLM 裁决。

**单靠 Developer、Reviewer、Tester 三个 LLM 角色不够。** LLM 加速开发，Validator、Executor 和 Comparator 才保证可用。LLM 建议、机器裁决、人拍板——三者边界不可混淆。

## 3. AssuranceLevel

| 等级 | 含义 | 不代表什么 |
|------|------|------------|
| `DRAFT` | 代码已生成，尚未验证 | 不代表可运行 |
| `STATIC_VALIDATED` | 静态契约和安全检查通过 | 不代表运行成功 |
| `RUNTIME_PASS` | 对应引擎在冻结样本上运行成功 | 不代表双引擎一致 |
| `CONSISTENT_SAMPLE` | SQL/Spark 在样本和已比较维度上一致 | 不代表业务正确或全量一致 |
| `REVIEW_READY` | 材料完整，可以进入人工代码审查 | 不代表获准上线 |
| `HUMAN_REVIEW` | 自动化无法继续或存在不确定项 | 不代表失败代码可接受 |

禁止使用一个通用 `PASS` 覆盖以上不同含义。

## 4. 核心流水线

```text
项目书
→ Requirement Analyzer
→ RequirementIR 人工确认点
→ SubIntent 拆分
→ TransformationContract
→ 关系一致快照
→ SQL / Spark 双分支
→ 真实双引擎样本执行
→ 确定性交叉验证
→ LLM 差异诊断与有限返工
→ Code Review Package
→ 人工审查
```

## 5. 双分支独立性

- SQL 分支读取 `SubIntent + TransformationContract + TianShu事实源`，LLM只生成类型化SQLPlan。
- Spark 分支读取相同业务契约，但不得查看SQL文本或SQLPlan实现细节。
- 两个分支共享业务目标和输入Schema，不共享彼此代码。
- 两个分支结果一致只能证明样本实现一致，不能排除共同误解需求。

因此，RequirementIR必须在高成本生成和执行之前设置人工确认点；Harness还必须维护人工标注的黄金需求和结果不变量。

## 6. 模块职责

| 模块 | 职责 |
|------|------|
| Requirement Analyzer | 项目书转为结构化RequirementIR |
| Fact Validator | 校验指标、表、字段、Join和语义来源 |
| SubIntent Decomposer | 按planning_table和可合并粒度拆分需求 |
| TransformationContract Builder | 固定输入、输出Schema、粒度、指标和语义 |
| SQL Planner | 只输出类型化SQLPlan |
| SQL Compiler | 确定性编译DuckDB SQL |
| SparkDeveloper | 生成受控PySpark纯转换函数 |
| SparkReviewer | 输出ReviewFinding与OptimizationDirective |
| SparkTester | 输出TestPlan和不可信测试代码 |
| Snapshot Builder | 构建关系一致、不可变的Parquet快照 |
| Executors | 在隔离环境真实执行SQL和Spark |
| Normalizer / Comparator | 规范化并确定性判断结果一致性 |
| DifferenceAnalyst | 解释差异，不决定验证结论 |
| RepairPlanner | 输出结构化RepairDirective |
| LangGraph | 编排节点、分支、重试、checkpoint和人工中断 |
| Packager | 生成Code Review Package |

## 7. 明确不做什么

- 不自动上线或自动批准代码。
- 不写生产库，不生成生产数据。
- 不把样本一致称为生产正确。
- 不让LLM直接生成或修改SQL文本。
- 不让LLM决定验证是否通过。
- 不在小样本上宣称Spark全量性能合格。
- 不让Memory覆盖TianShu contracts、meta或数据库设计事实源。
- 不建设生产调度、生产写入和发布审批系统。

## 8. 主要风险与控制

| 风险 | 控制 |
|------|------|
| LLM通过字符串字段间接写SQL | SQLPlan使用封闭的类型化表达式AST |
| SQL和Spark一致地误解需求 | Requirement确认点、黄金用例和语义不变量 |
| 跨表独立LIMIT破坏关联 | 锚点键驱动的关系一致快照 |
| SQL/Spark语义差异造成误报 | EnvironmentManifest与SemanticCompatibilityPolicy |
| Reviewer优化改变业务语义 | Reviewer只输出指令，Developer修订后重新验证 |
| LLM测试代码执行任意操作 | 测试代码同样经过AST校验并隔离执行 |
| Memory污染后续决策 | 初期不让长期Memory参与运行时路由 |

## 9. v1.0验收标准

1. 至少覆盖单表、受控双表Join、聚合和窗口函数的黄金项目书。
2. SQLPlan不包含任何自由SQL片段。
3. PySpark只能通过受控`transform(inputs, params)`入口运行。
4. SQL/Spark读取同一关系一致快照，并记录环境、代码和契约哈希。
5. Comparator产生精确状态，LLM不能覆盖结论。
6. 返工最多2轮，无法确定时进入`HUMAN_REVIEW`。
7. Code Review Package可追溯到需求、事实源、模型、Prompt、快照和执行环境。
8. 主pytest套件保持高价值、快速；Prompt与模型质量进入Harness。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 1 前置事实源
