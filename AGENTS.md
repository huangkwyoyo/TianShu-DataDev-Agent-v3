# AGENTS.md — TianShu DataDev Agent v3

> 项目宪法。所有 Agent、LLM 角色和自动化工具必须遵守本文件。

## 1. System Role

TianShu DataDev Agent v3 是 AI 辅助数据开发工具。它接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书，生成 SQL、PySpark、测试和 Code Review Package；最终产物是代码，不是生产数据。

系统不自动上线、不写生产库、不生成生产数据。人是最终代码审查者和上线决策者。

当前阶段：`Phase 3C 收尾 → Phase 4A 基础设施就绪`。核心流水线（Parser/Planner/Validator/Compiler/Executor/Packager）已完成，多语句 SqlProgram + _temp 生命周期 + 窗口函数 + 受控写入审查 + CompilerBackend 接口 + DataTransformContract v1 均已实现；LLM Gateway + PromptManager + Adapter 基础设施已就绪。Phase 3 Exit HarnessReport 已生成（GO），Phase 3→4 门禁解除。详见 `docs/roadmap/` 各 Phase 核销结果。

## 2. SQL Generation Boundary

- 输入是 DeveloperSpec（Markdown 正文 + YAML-like metadata block），经 Parser 确定性解析为 ParsedDeveloperSpec。
- LLM 只输出严格的 ParsedDeveloperSpec、RelationshipHypothesis、SqlBuildPlan 和 SqlProgram。
- SqlBuildPlan 必须使用封闭类型的 ScanStep、FilterStep、JoinStep、AggregateStep、ProjectStep、CaseWhenStep、SortStep、LimitStep。
- 禁止 `where_sql`、`join_on: str`、`expression: str`、`raw_sql` 及其他自由 SQL 片段。
- SQL 只能由 Python 确定性编译器生成。
- SQL 修复只能生成新 SqlBuildPlan，禁止直接修改 SQL 文本。
- 表、字段和 Join 必须来自 SourceManifest；optional SchemaRegistry 只补充缺失信息，禁止静默覆盖 DeveloperSpec 中程序员已声明的值——冲突时输出 SOURCE_CONFLICT。
- 不支持的表达式必须拒绝或进入 `HUMAN_REVIEW`，不能使用字符串逃生口。
- Join 推理遵循三层分工：LLM 提候选 → Validator 确定性定级（STRONG/MEDIUM/WEAK/NONE）→ 人工确认中低置信。WEAK/NONE 硬门禁——不得进入 SqlBuildPlan 的 JoinSpec。
- 性能门禁由确定性 PerfValidator 执行——硬规则（REJECT）阻断流水线，软规则（WARN）记录到 ExecutionTrace。LLM 不做性能决策。
- SQL 编译器在渲染前运行轻量优化 pass（列裁剪、谓词规范化、无用排序消除、常量折叠），优化必须是幂等的——相同 SqlBuildPlan 两次编译产生相同 SQL 和哈希。

## 3. Spark Generation Boundary

LLM 可以生成 PySpark，但只能生成以下纯转换入口：

```python
def transform(
    inputs: Mapping[str, DataFrame],
    params: TransformParams,
) -> DataFrame:
    ...
```

强制要求：

- SparkDeveloper 读 DataTransformContract（从已验证 SqlBuildPlan 确定性抽取）作为权威规格输入，不从 DeveloperSpec 重新推理业务逻辑。
- 只读取 `inputs` 中契约声明的数据源。
- 禁止 `spark.table`、`spark.read` 和自行创建 SparkSession。
- 禁止 Action、Sink、UDF、网络、文件系统、进程、线程、动态执行和任意模块导入。
- 返回且仅返回一个 DataFrame。
- 不查看 SQL 文本或 SqlBuildPlan 实现。
- 所有代码和测试代码都是不可信 artifact，必须先静态验证再隔离执行。

角色职责：

- SparkDeveloper 生成或修订代码。
- SparkReviewer 输出 ReviewFinding 和 OptimizationDirective，不直接替换最终代码。
- SparkTester 输出 TestPlan 和测试代码，不参与业务实现，也不能宣布测试通过。

## 4. Execution Boundary

- Snapshot Builder 只读访问开发数据源。
- SQL 与 Spark 必须读取同一个关系一致、不可变的 Parquet 快照。
- 多表快照使用锚点键和 Join 白名单级联抽取，禁止各表独立 LIMIT。
- Validator 先于 Executor。
- Executor 运行在隔离环境，受超时、CPU、内存、网络、工作目录和输出大小限制。
- Agent 环境不包含生产凭据和生产写权限。
- EnvironmentManifest 必须记录引擎版本、时区、ANSI、大小写、Decimal、NULL 和 NaN 策略。

## 5. Validation Boundary

LLM 不能决定验证通过。确定性 Comparator 产生以下精确状态：

| 状态 | 含义 |
|------|------|
| `NOT_EXECUTED` | 至少一个必需执行没有结果 |
| `RUNTIME_PASS` | 单引擎在当前快照运行成功 |
| `DIFFERENT` | 必需比较维度不一致 |
| `UNSUPPORTED_SEMANTICS` | 当前兼容策略不能证明等价 |
| `CONSISTENT_SAMPLE` | 当前快照和比较维度一致 |
| `REVIEW_READY` | 材料完整，可进入人工代码审查 |
| `HUMAN_REVIEW` | 自动化无法安全继续 |

禁止使用泛化 `PASS` 表示业务正确、全量一致、生产性能或上线批准。

## 6. Repair Boundary

- DifferenceAnalyst 只解释结构化差异，不修改 Comparator 结论。
- RepairPlanner 只能输出 SQL_PLAN、SPARK_CODE、BOTH、REQUIREMENT 或 HUMAN_REVIEW。
- SQL_PLAN 返回 SQL Planner；SPARK_CODE 返回 SparkDeveloper。
- 每次修订必须重新经过 Validator、Executor 和 Comparator。
- 最多 2 轮自动返工；UNKNOWN、事实源缺失、需求变化或超限进入 `HUMAN_REVIEW`。
- 人工 Review 不通过时，提交结构化 **ReviewFeedback** artifact（非 Memory），至少包含：
  `request_id`、`review_package_id`、`developer_spec_hash`、`source_manifest_hash`、
  `sql_build_plan_hash`、`sql_artifact_hash`、`target`、`finding_type`、`comment`、`suggested_resolution`。
- `target` 是机器路由主字段（`REQUIREMENT` | `SQL_PLAN` | `COMPILER_BUG` | `SOURCE_FACT` | `HUMAN_REVIEW`），
  `finding_type` 是细分原因。`target=HUMAN_REVIEW` 时停止自动返工。
- 不同 `target` 的返工入口：
  - `REQUIREMENT` → 修改 DeveloperSpec 或补 HumanResolution，重新从 Parser/Planner 走
  - `SOURCE_FACT` → 更新 SourceManifest / SchemaRegistry / open_questions
  - `SQL_PLAN` → 生成新 SqlBuildPlan（禁止直接改 SQL 文本）；Join 问题进入 RelationshipHypothesis 重新定级，不靠 Memory
  - `COMPILER_BUG` → 修 Compiler，并加回归测试
  - `HUMAN_REVIEW` → 反馈无法结构化、证据不足或需求变化不明确，停止自动返工
- "在原有基础上修改"靠 artifact 引用 + hash + checkpoint + retry_count，不靠 Memory。
  Agent 读取上一版 DeveloperSpec、SourceManifest、SqlBuildPlan、ReviewFeedback，生成新版 artifact。
- 可复现 Review 经验沉淀为 regression fixture、Validator/Compiler 规则、
  Schema/Contract 标注或 Prompt/Harness 回归样本，不进入运行时可检索 Memory。

## 7. LangGraph Boundary

- LangGraph 只编排节点、分支、路由、checkpoint、retry_count 和人工中断。
- 业务节点必须是可脱离 LangGraph 调用的普通 Python 服务。
- Graph State 只保存 artifact 引用、哈希、状态和小型摘要。
- 禁止在 State 中保存 DataFrame、完整结果集、完整代码、项目书正文、凭据或无限聊天历史。
- 条件路由只能读取结构化确定性状态，不能依赖 LLM 自由文本或置信度。

## 8. Data Contracts

- Phase 1 运行时契约使用严格 Pydantic 模型或等价 JSON Schema，拒绝额外字段。
- DataTransformContract 是 SQL/Spark 共同业务规格，从已验证 SqlBuildPlan 确定性抽取，不包含实现代码。三级递进：lite（Phase 2 单语句）→ v1（Phase 3 Exit，+SqlProgram DAG +窗口 +CASE +受控写入）→ Phase 5 消费 v1。
- SqlProgram 多语句合并使用 DAG 依赖和确定性拓扑排序，不引入 CTE 嵌套作用域。
- Code Review Package 记录事实源、代码、Prompt、模型、快照、环境和 Comparator 版本哈希。

## 9. Harness and Memory

- pytest 覆盖确定性逻辑、安全边界和少量黄金路径。
- Prompt、模型、规模、成本、人工接受率和返工效果进入独立 Harness。
- Harness 不得成为产品运行时依赖。
- Run State 由 checkpoint 和 artifact store 管理，不是长期学习 Memory。
- **本项目不建设独立 Engineering Memory。** 失败、经验与模型行为变化不进入运行时可检索 Memory。
- 沉淀路径：
  1. Harness 回归样本（`harness/datasets/regression/` + pytest）
  2. Validator / Compiler / Optimizer 确定性规则
  3. SchemaRegistry / Contract 显式标注（nullable、枚举、唯一性、时区策略等）
  4. Prompt/Harness 版本化评测记录
- 表、字段、Join 和业务口径的事实源是 SourceManifest / SchemaRegistry / Contract——禁止用 Memory 覆盖或补写事实源。

## 10. Testing Policy

- 测试保护独立风险，不追求数量或覆盖率目标。
- 优先表驱动测试，禁止为枚举组合、标准库行为和文档措辞创建重复测试。
- LLM 真实调用进入 Harness；pytest 使用确定性 Fake Adapter。
- 每阶段运行 `pytest`、`ruff` 和 `git diff --check`。
- Phase 0 已有 22 个测试，Phase 1 前不得继续增加低价值 Protocol 反射测试。

## 11. Code Style

- 所有代码注释和 docstring 使用中文。
- 注释解释"为什么"，不复述代码。
- 文件按职责拆分，避免重新形成大型扁平模块。
