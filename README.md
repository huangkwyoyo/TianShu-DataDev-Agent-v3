# TianShu DataDev Agent v3

AI 辅助数据开发工具：接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书，生成 SQL、测试、验证材料和 Code Review Package。最终目标覆盖受控 PySpark DSL 和 SQL/Spark 双链验证。

最终产物是代码和审查材料，不是生产数据。系统不自动上线、不写生产库。

## 当前状态

- 当前阶段：**Phase 9A-9C + label_table v1 完成**——C1-C4 已消除，label_table v1 管线已交付，前端 E2E 6/6 通过。
- Phase 0–4.6（SQL 链路）全部退出——含 DeveloperSpec Parser、SqlBuildPlan、Validator、Compiler、Executor、Code Review Package、Harness 七维门禁、复杂 SQL（多跳 Join + 子查询）。
- **Phase 6–9 已完成**——受控 PySpark DSL（10 种 step）、SQL/Spark 双链验证（逻辑+物理）、编排硬化 + Harness 5 维度评测、前端回归 + 可观测性、DOM E2E 交互测试已全部实现并回归通过。
- **label_table v1 已完成（2026-07-16）**——Parser→LlmLabelExtractor→Validator→Promotion→Builder(CaseWhenStep)→Compiler 全链路，90 测试全绿。
- 项目当前状态详见 **`docs/current-state-and-verification-status.md`**（Phase 进度矩阵、业务集成验证状态、测试基线、残留风险、下一步方向）。
- 文档入口详见 **`docs/README.md`**。

### 路线图状态

Phase 6-8 的 roadmap 文件已从占位更新为设计摘要，完整设计见 `docs/superpowers/specs/`。

| Phase | 文件 | 状态 |
|-------|------|------|
| 5 | `phase-5-spark-ready-contract-and-sparkplan.md` | ✅ 已完成（2026-06-29） |
| 6 | `phase-6-controlled-pyspark-dsl.md` | ✅ 已完成（2026-07-04） |
| 7 | `phase-7-sql-spark-cross-validation.md` | ✅ 已完成（2026-07-04） |
| 8 | `phase-8-spark-first-orchestration-hardening.md` | ✅ 已完成（2026-07-04） |
| C1-C4 | `docs/risks/phase-6-8-known-risks.md` | ✅ Phase 6-8 历史验收记录——已冻结，当前风险以 current-state §3/§3.5 为准 |

**实施状态**：Phase 0-9 已完成。C1-C4 业务集成全部点亮，前端 E2E 6/6 通过。详见 `docs/current-state-and-verification-status.md`。

## 目标流程

```text
DeveloperSpec (.md 项目书，Markdown 正文 + YAML-like metadata block)
→ ParsedDeveloperSpec（系统结构化理解 + open_questions，含 label_table 类型识别）
→ SourceManifest（表字段事实追踪，optional SchemaRegistry 补充）
→ RelationshipHypothesis（Join 推理 + 证据定级：强/中/弱/无）
→ [label_table 分支] LlmLabelExtractor → LabelRuleValidator → Promotion → Builder 追加 CaseWhenStep
→ SqlBuildPlan / SqlProgram（受控 SQL 构建计划，10 step 类型化 DAG）
→ SQL Validator → Compiler（确定性渲染）→ DuckDB Executor
→ SQL Code Review Package（供程序员审查）

[Spark-first v2.0]
→ DataTransformContract（从已验证 SqlBuildPlan 确定性抽取）
→ mapper.py → baseline SparkPlan（确定性，唯一结构路径）
→ SparkDeveloper（LLM 只做标注，不增删改 step）
→ SparkCompiler（确定性 PySpark DSL 生成）+ SparkCodeRenderer（安全渲染）
→ Static Validator（AST 硬门禁）
→ SQL/Spark 双链验证（PlanComparator + PhysicalVerifier）
```

## 关键边界

### SQL

- LLM 不生成 SQL 文本或 SQL 片段。
- LLM 只输出严格类型化 SqlBuildPlan（10 种封闭 step 类型）。
- Python 编译器确定性生成 SQL；相同 SqlBuildPlan 两次编译产生相同 SQL 和哈希。
- 禁止 `raw_sql`、`where_sql`、`join_on: str`、`expression: str`。
- 表、字段和 Join 必须来自 SourceManifest；SchemaRegistry 只补充不覆盖——冲突输出 SOURCE_CONFLICT。

### PySpark

PySpark 只能以受控纯转换函数形式生成：

```python
def transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame:
    ...
```

代码只读取注入的 `inputs`，禁止自行读取数据（`spark.read`、`spark.table`）、Action、写入、UDF、网络、文件系统和动态执行。

**生成链路**：`mapper.py` 是唯一 Contract → SparkPlan 结构生成路径。SparkDeveloper（LLM）只做语义标注，不增删改 step、不直接输出代码。Compiler 确定性生成 PySpark DSL，所有代码片段通过 Renderer 封闭枚举/白名单渲染。Validator 做 AST call-chain 硬门禁。

SparkDeveloper / SparkCompiler / SparkPlan 生成链路只读 DataTransformContractV1 和 baseline SparkPlan；Phase 7 验证层可读取 SqlBuildPlan 的结构化 artifact 用于对比，但不得把 SQL 文本提供给 SparkDeveloper 或 SparkCompiler。

### 验证

SQL 与 Spark 读取同一个关系一致冻结快照（SnapshotManifest）。逻辑链路（PlanComparator：SqlBuildPlan ↔ SparkPlan 结构等价对比）和物理链路（PhysicalVerifier：同一快照上 DuckDB + Spark 双引擎执行结果对比）双链验证。状态使用 `LOGIC_EQUIVALENT` / `RESULT_CONSISTENT` / `NOT_EXECUTED` / `HUMAN_REVIEW`，禁止泛化 "PASS"。未覆盖 step 类型必须明确标记 `NOT_EXECUTED`。

### LangGraph

编排层负责编排、分支、checkpoint、重试和人工中断。SparkOrchestrator 不直接访问模型、不构造 Prompt、不解析 LLM 自由文本——只调用已封装的 SparkDeveloperService。业务逻辑是普通 Python 服务；Graph State 只保存 artifact 引用、哈希、状态和摘要。

### Memory 边界

本项目不建设独立 Engineering Memory。失败案例沉淀进入 Harness 回归集、确定性 Validator / Compiler / Optimizer 规则、SchemaRegistry / Contract 显式标注和 Prompt/Harness 版本化评测记录。运行时路由、规划与生成不读取长期 Memory。事实源只有 SourceManifest / SchemaRegistry / Contract。

## 规划文档

核心文档入口：

- **`docs/README.md`** — 文档索引与分类入口（推荐从此开始）
- `docs/current-state-and-verification-status.md` — 当前实施状态（唯一权威）
- `AGENTS.md` — 项目宪法
- `docs/00-product-charter.md` 至 `docs/09-test-strategy.md` — 架构与设计参考
- `docs/superpowers/specs/` — 各特性完整设计文档
- `docs/superpowers/plans/` — 方案书索引
- `docs/examples/` — DeveloperSpec 示例（汇总表/标签表/多步骤加工）

## 目录

```text
src/tianshu_datadev/
├── developer_spec/   # DeveloperSpec Parser、ParsedDeveloperSpec、SourceManifest
├── planning/         # RelationshipHypothesis、SqlBuildPlan、SqlProgram
├── sql/              # SQL Validator、确定性 Compiler、PerfContract
├── spark/            # SparkDeveloper、Static Validator、SparkReviewer
├── execution/        # 快照、DuckDB 和 Spark 隔离执行
├── validation/       # 规范化、Comparator、PlanEquivalence
├── orchestration/    # LangGraph 薄编排层
├── artifacts/        # Code Review Package
└── llm/              # LLM Gateway、Prompt 版本管理
```

## 开发命令

```powershell
pip install -e ".[dev]"
python -m pytest tests -q
python -m ruff check .
```

## 已知质量状态

- pytest：2818 collected（2026-07-17 基线）。非 Spark/非 Harness 子集：1629 passed / 6 skipped / 2 xfailed。Spark 全量需 `--run-slow` + PySpark。
- ruff/tsc/build：零告警。
- 详情见 `docs/current-state-and-verification-status.md`。
