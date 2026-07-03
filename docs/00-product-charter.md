# 产品宪章 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 产品目标

TianShu DataDev Agent v3 是 **AI 辅助数据开发工具**，面向程序员 / 数据工程师。它接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书（Markdown 正文 + YAML-like metadata block），生成达到"开发审查级"的 SQL、PySpark DataFrame DSL、测试与验证材料，最终交付 Code Review Package。

最终产物是代码，不是生产数据。系统不自动部署、不写生产库，也不替代程序员审批。

## 2. "开发审查级"的精确定义

只有同时满足以下条件，产物才能标记为 `REVIEW_READY`：

1. DeveloperSpec 已由确定性 Parser 解析为结构化 ParsedDeveloperSpec，并经 SourceManifest 校验。
2. SQL 由类型化 SqlBuildPlan / SqlProgram 经 Python 确定性编译生成。
3. PySpark 代码满足受控纯转换函数契约。
4. SQL 与 Spark 均在同一个冻结快照上真实执行。
5. 确定性 Comparator 给出 `RESULT_CONSISTENT`（双引擎结果一致）或进入 `HUMAN_REVIEW`。
6. 所有 WARN、不支持语义和人工确认项已写入审查材料。

`REVIEW_READY` 只表示可以进入代码审查，不表示业务绝对正确、全量性能合格或生产就绪。

### 2.1 方案可行性前提

本方案能得到可用 PySpark DSL，但前提是**四根支柱同时成立**：

1. **受控代码生成**——LLM 输出受 DataTransformContract 和 Static Validator 严格约束，禁止自由脚本。
2. **真实 Spark 执行**——代码在隔离 Sandbox 中读取注入快照并真实运行，不依赖模拟或静态分析替代。
3. **同源快照**——SQL 和 Spark 读取同一份冻结 Parquet，禁止分别抽样。
4. **确定性交叉验证**——Comparator 由代码逻辑判定一致性，不由 LLM 裁决。

**单靠 Developer、Reviewer、Tester 三个 LLM 角色不够。** LLM 加速开发，Validator、Executor 和 Comparator 才保证可用。LLM 建议、机器裁决、人拍板——三者边界不可混淆。

## 3. AssuranceLevel

| 等级 | 含义 | 不代表什么 |
|------|------|------------|
| `DRAFT` | 代码已生成，尚未验证 | 不代表可运行 |
| `STATIC_VALIDATED` | 静态契约和安全检查通过 | 不代表运行成功 |
| `EXECUTION_PASS` | 对应引擎在冻结样本上运行成功 | 不代表双引擎一致 |
| `RESULT_CONSISTENT` | SQL/Spark 在样本和已比较维度上一致 | 不代表业务正确或全量一致 |
| `LOGIC_EQUIVALENT` | SQL Plan ↔ Spark Plan 结构完全等价 | 不代表结果一致 |
| `REVIEW_READY` | 材料完整，可以进入人工代码审查 | 不代表获准上线 |
| `HUMAN_REVIEW` | 自动化无法继续或存在不确定项 | 不代表失败代码可接受 |

禁止使用一个通用 `PASS` 覆盖以上不同含义。

## 4. 核心流水线

```text
DeveloperSpec (.md 项目书)
→ DeveloperSpec Parser（确定性解析 + 宽松/禁止封闭表）
→ ParsedDeveloperSpec + open_questions
→ SourceManifest（表字段事实追踪 + optional SchemaRegistry 补充）
→ RelationshipHypothesis（LLM 提 Join 候选 → Validator 定级 → 人工确认中低置信）
→ SqlBuildPlan / SqlProgram（受控 SQL 构建计划）
→ SQL Validator → Compiler（确定性渲染）→ DuckDB Executor
→ SQL Code Review Package

[Spark-first v2.0]
→ DataTransformContract（从已验证 SqlBuildPlan 确定性抽取，三级递进）
→ mapper.py → baseline SparkPlan（确定性，唯一结构路径）
→ SparkDeveloper（LLM 只做语义标注）→ SparkCompiler（确定性 PySpark DSL 生成）
→ Static Validator（AST 硬门禁）
→ 双链验证（PlanComparator + PhysicalVerifier）
→ 人工审查
```

## 5. 双分支独立性

- SQL 分支读取 `ParsedDeveloperSpec + SourceManifest`，LLM 在证据约束下推理 RelationshipHypothesis 和 SqlBuildPlan。
- Spark 分支读取 `DataTransformContract`（从已验证 SqlBuildPlan 确定性抽取），但不得查看 SQL 文本或 SqlBuildPlan 实现细节。
- 两个分支共享同一份经过验证的业务理解，不共享彼此代码。
- 两个分支结果一致只能证明样本实现一致，不能排除共同误解需求。

因此，DeveloperSpec 中的 Join 推理必须展示完整证据链并经 Validator 证据定级；Harness 还必须维护人工标注的黄金需求和结果不变量。

## 6. 模块职责

| 模块 | 职责 |
|------|------|
| DeveloperSpec Parser | 确定性解析 Markdown + YAML-like 项目书，输出 ParsedDeveloperSpec + open_questions |
| SourceManifest | 追踪表字段事实来源（developer_spec / schema_registry / snapshot_profile），冲突输出 SOURCE_CONFLICT |
| RelationshipHypothesis Planner | LLM 提 Join 候选，Validator 确定性定级（STRONG/MEDIUM/WEAK/NONE） |
| SqlBuildPlan Planner | LLM 输出类型化 SqlBuildPlan（8 step），不含自由 SQL 字符串 |
| SqlProgram Builder | 多语句 DAG + _temp 中间表 + 拓扑排序 |
| SQL Validator | 校验表字段存在、Join key 类型一致、证据等级硬门禁 |
| SQL Compiler | 确定性编译 DuckDB SQL + 优化 Pass（列裁剪、谓词规范化、无用排序消除、常量折叠） |
| PerfValidator | 性能门禁——REJECT 阻断 / WARN 记录 |
| SparkDeveloper | 读 DataTransformContract + baseline SparkPlan，只输出语义标注（StepAnnotation），不增删改 step |
| SparkCompiler | 确定性 PySpark DSL 生成——SparkPlan → 代码，所有片段过 Renderer 封闭枚举/白名单 |
| SparkCodeRenderer | 安全渲染——禁止字符串拼接，所有值来自封闭模型/枚举/白名单 |
| SparkStaticValidator | AST call-chain 硬门禁——8 种错误码（E601-E608），默认拒绝 |
| Snapshot Builder | 构建关系一致、不可变的 Parquet 快照 |
| Executors | 在隔离环境真实执行 SQL 和 Spark |
| Normalizer / Comparator | 规范化并确定性判断结果一致性 |
| PlanComparator | SQL 侧 SqlBuildPlan vs Spark 侧 SparkPlan 结构等价比较（封装 plan_equivalence.py） |
| DifferenceAnalyst | 解释差异，不决定验证结论 |
| RepairPlanner | 输出结构化 RepairDirective |
| LangGraph | 编排节点、分支、重试、checkpoint 和人工中断 |
| Packager | 生成 Code Review Package |

## 7. 明确不做什么

- 不自动上线或自动批准代码。
- 不写生产库，不生成生产数据。
- 不把样本一致称为生产正确。
- 不让 LLM 直接生成或修改 SQL 文本。
- 不让 LLM 决定验证是否通过。
- 不在小样本上宣称 Spark 全量性能合格。
- 不让 SchemaRegistry 静默覆盖 DeveloperSpec 中程序员已声明的字段类型、枚举值或唯一性——冲突输出 SOURCE_CONFLICT，由程序员裁决。
- 不建设独立 Engineering Memory；不让 Memory 覆盖 SourceManifest 或 SchemaRegistry。
- 不把业务人员自然语言问数作为产品主入口。
- 不建设生产调度、生产写入和发布审批系统。

## 8. 主要风险与控制

| 风险 | 控制 |
|------|------|
| LLM 通过字符串字段间接写 SQL | SqlBuildPlan 使用封闭的类型化 step AST，禁止 raw_sql/where_sql/join_on/expression |
| 复杂 SQL 能力被一次性放开 | 窗口函数、CASE 标签、多语句程序、受控写入按阶段逐项开放，每项必须配套 Schema、Validator、Compiler、测试和拒绝路径（CTE 永不实现，以 SqlProgram + _temp 替代） |
| SQL 和 Spark 一致地误解需求 | RequirementIR 人工确认点已替换为：① DeveloperSpec 程序员事实声明 + open_questions；② Join 推理证据链 + Validator 定级 + 人工确认中低置信；③ Harness 黄金用例和语义不变量 |
| DeveloperSpec 输入不稳定——程序员写法过散，Parser 无法稳定输出 | Parser 允许/禁止宽松封闭表（6 项允许 + 7 项禁止），golden/拒绝 fixture 全覆盖，normalized_spec_hash 确定性验证 |
| Join 推理错误——LLM 推断错误关联，后续验证未必能发现 | 三层分工（LLM 提候选 → Validator 定级 → 人工确认），WEAK/NONE 硬门禁不得进入 SqlBuildPlan，Phase 4 Harness Join 推理质量"零容忍"维度 |
| DeveloperSpec 与 SchemaRegistry 事实冲突——程序员声明与物理实际不一致 | SOURCE_CONFLICT 输出双方值进入 open_questions，默认 blocking，由程序员裁决；SOURCE_ANOMALY 进入审查包 WARN |
| 跨表独立 LIMIT 破坏关联 | 锚点键驱动的关系一致快照 |
| SQL/Spark 语义差异造成误报 | EnvironmentManifest 与 SemanticCompatibilityPolicy |
| Reviewer 优化改变业务语义 | Reviewer 只输出指令，Developer 修订后重新验证 |
| LLM 测试代码执行任意操作 | 测试代码同样经过 AST 校验并隔离执行 |
| Memory 污染后续决策 | 不建设独立 Engineering Memory；失败沉淀走回归 / 规则 / Schema / Prompt 回归 |
| Spark 侧独立读取 DeveloperSpec 导致验证投入浪费 | SparkDeveloper 读 DataTransformContract（从已验证 SqlBuildPlan 确定性抽取），只做引擎翻译不做二次理解 |

## 9. v1.0 验收标准

1. 至少覆盖单表、受控 Join、聚合、CASE 标签和窗口函数的黄金 DeveloperSpec 用例。
2. SqlBuildPlan / SqlProgram 不包含任何自由 SQL 片段。
3. PySpark 只能通过受控 `transform(inputs, params)` 入口运行。
4. SQL/Spark 读取同一关系一致快照，并记录环境、代码、契约和 Prompt 版本哈希。
5. Comparator 产生精确状态，LLM 不能覆盖结论。
6. 返工最多 2 轮，无法确定时进入 `HUMAN_REVIEW`。
7. Code Review Package 可追溯到 DeveloperSpec、SourceManifest、模型、Prompt、快照和执行环境。
8. 主 pytest 套件保持高价值、快速；Prompt 与模型质量进入 Harness。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | Phase 1A 前置事实源
