# AGENTS.md — TianShu DataDev Agent v3

> 项目宪法。所有Agent、LLM角色和自动化工具必须遵守本文件。

## 1. System Role

TianShu DataDev Agent v3是AI辅助数据开发工具。它生成SQL、PySpark、测试和Code Review Package；最终产物是代码，不是生产数据。

系统不自动上线、不写生产库、不生成生产数据。人是最终代码审查者和上线决策者。

当前阶段：`Phase 0.5 — 架构契约校正`。本阶段只调整规划，不实现Phase 1代码。

## 2. SQL Generation Boundary

- LLM只输出严格的RequirementIR、SubIntent和SQLPlan。
- SQLPlan必须使用封闭类型的ColumnRef、Literal、Predicate、JoinSpec、AggregateSpec和SortSpec。
- 禁止`where_sql`、`join_on: str`、`expression: str`、`raw_sql`及其他自由SQL片段。
- SQL只能由Python确定性编译器生成。
- SQL修复只能生成新SQLPlan，禁止直接修改SQL文本。
- 表、字段、指标和Join必须来自TianShu contracts、meta和数据库设计事实源。
- 不支持的表达式必须拒绝或进入`HUMAN_REVIEW`，不能使用字符串逃生口。
- 性能门禁由确定性 PerfValidator 执行——硬规则（REJECT）阻断流水线，软规则（WARN）记录到 ExecutionTrace。LLM 不做性能决策。
- SQL 编译器在渲染前运行轻量优化 pass（列裁剪、谓词规范化、无用排序消除、常量折叠），优化必须是幂等的——相同 SQLPlan 两次编译产生相同 SQL 和哈希。

## 3. Spark Generation Boundary

LLM可以生成PySpark，但只能生成以下纯转换入口：

```python
def transform(
    inputs: Mapping[str, DataFrame],
    params: TransformParams,
) -> DataFrame:
    ...
```

强制要求：

- 只读取`inputs`中契约声明的数据源。
- 禁止`spark.table`、`spark.read`和自行创建SparkSession。
- 禁止Action、Sink、UDF、网络、文件系统、进程、线程、动态执行和任意模块导入。
- 返回且仅返回一个DataFrame。
- 不查看SQL文本或SQLPlan实现。
- 所有代码和测试代码都是不可信artifact，必须先静态验证再隔离执行。

角色职责：

- SparkDeveloper生成或修订代码。
- SparkReviewer输出ReviewFinding和OptimizationDirective，不直接替换最终代码。
- SparkTester输出TestPlan和测试代码，不参与业务实现，也不能宣布测试通过。

## 4. Execution Boundary

- Snapshot Builder只读访问开发数据源。
- SQL与Spark必须读取同一个关系一致、不可变的Parquet快照。
- 多表快照使用锚点键和Join白名单级联抽取，禁止各表独立LIMIT。
- Validator先于Executor。
- Executor运行在隔离环境，受超时、CPU、内存、网络、工作目录和输出大小限制。
- Agent环境不包含生产凭据和生产写权限。
- EnvironmentManifest必须记录引擎版本、时区、ANSI、大小写、Decimal、NULL和NaN策略。

## 5. Validation Boundary

LLM不能决定验证通过。确定性Comparator产生以下精确状态：

| 状态 | 含义 |
|------|------|
| `NOT_EXECUTED` | 至少一个必需执行没有结果 |
| `RUNTIME_PASS` | 单引擎在当前快照运行成功 |
| `DIFFERENT` | 必需比较维度不一致 |
| `UNSUPPORTED_SEMANTICS` | 当前兼容策略不能证明等价 |
| `CONSISTENT_SAMPLE` | 当前快照和比较维度一致 |
| `REVIEW_READY` | 材料完整，可进入人工代码审查 |
| `HUMAN_REVIEW` | 自动化无法安全继续 |

禁止使用泛化`PASS`表示业务正确、全量一致、生产性能或上线批准。

## 6. Repair Boundary

- DifferenceAnalyst只解释结构化差异，不修改Comparator结论。
- RepairPlanner只能输出SQL_PLAN、SPARK_CODE、BOTH、REQUIREMENT或HUMAN_REVIEW。
- SQL_PLAN返回SQL Planner；SPARK_CODE返回SparkDeveloper。
- 每次修订必须重新经过Validator、Executor和Comparator。
- 最多2轮自动返工；UNKNOWN、事实源缺失、需求变化或超限进入`HUMAN_REVIEW`。

## 7. LangGraph Boundary

- LangGraph只编排节点、分支、路由、checkpoint、retry_count和人工中断。
- 业务节点必须是可脱离LangGraph调用的普通Python服务。
- Graph State只保存artifact引用、哈希、状态和小型摘要。
- 禁止在State中保存DataFrame、完整结果集、完整代码、项目书正文、凭据或无限聊天历史。
- 条件路由只能读取结构化确定性状态，不能依赖LLM自由文本或置信度。

## 8. Data Contracts

- Phase 1运行时契约使用严格Pydantic模型或等价JSON Schema，拒绝额外字段。
- TransformationContract是SQL/Spark共同业务规格，不包含实现代码。
- 多SubIntent合并必须使用MergePlan，明确键、粒度、基数、Join类型和冲突策略。
- Code Review Package记录事实源、代码、Prompt、模型、快照、环境和Comparator版本哈希。

## 9. Harness and Memory

- pytest覆盖确定性逻辑、安全边界和少量黄金路径。
- Prompt、模型、规模、成本、人工接受率和返工效果进入独立Harness。
- Harness不得成为产品运行时依赖。
- Run State由checkpoint和artifact store管理，不是长期学习Memory。
- Engineering Memory在Phase 6前不参与运行时；写入必须可复现且经人工批准。
- 指标、表、字段、Join和业务口径属于TianShu Fact Catalog，不属于可写Domain Memory。

## 10. Testing Policy

- 测试保护独立风险，不追求数量或覆盖率目标。
- 优先表驱动测试，禁止为枚举组合、标准库行为和文档措辞创建重复测试。
- LLM真实调用进入Harness；pytest使用确定性Fake Adapter。
- 每阶段运行`pytest`、`ruff`和`git diff --check`。
- Phase 0已有22个测试，Phase 1前不得继续增加低价值Protocol反射测试。

## 11. Code Style

- 所有代码注释和docstring使用中文。
- 注释解释“为什么”，不复述代码。
- 文件按职责拆分，避免重新形成大型扁平模块。
