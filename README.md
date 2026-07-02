# TianShu DataDev Agent v3

AI 辅助数据开发工具：接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书，生成 SQL、测试、验证材料和 Code Review Package。最终目标覆盖受控 PySpark DSL 和 SQL/Spark 双链验证。

最终产物是代码和审查材料，不是生产数据。系统不自动上线、不写生产库。

## 当前状态

- 当前阶段：**Phase 5 已完成**——SparkPlan IR + DataTransformContractV1 确定性映射已落地。
- Phase 0–4.6（SQL 链路）全部退出——含 DeveloperSpec Parser、SqlBuildPlan、Validator、Compiler、Executor、Code Review Package、Harness 七维门禁、复杂 SQL（多跳 Join + 子查询）。
- Phase 6–8 为远期规划占位，待 Phase 5 的实际行为数据驱动下一轮细化。
- 下一阶段：Phase 6 — 受控 PySpark DSL（远期）。

### 路线图占位策略

Phase 6-8 的 roadmap 文件为**远期规划占位**，仅含方向性骨架，不包含详细模型/测试/退出条件。Phase 5 已完工（SparkPlan IR + 确定性映射器 + Contract 协议）。

| Phase | 文件 | 状态 |
|-------|------|------|
| 5 | `phase-5-spark-ready-contract-and-sparkplan.md` | ✅ 已完成（2026-06-29） |
| 6 | `phase-6-controlled-pyspark-dsl.md` | 占位——Phase 5 退出后重写 |
| 7 | `phase-7-sql-spark-cross-validation.md` | 占位——Phase 6 退出后重写 |
| 8 | `phase-8-spark-first-orchestration-hardening.md` | 占位——Phase 7 退出后重写 |

**当前可执行的完整规格到 Phase 5**（含 Phase 1A–4.6 SQL 链路 + Phase 5 SparkPlan IR + 确定性映射）。Phase 6+ 的详细设计与实施取决于 Spark 链路的实际行为数据。

## 目标流程

```text
DeveloperSpec (.md 项目书，Markdown 正文 + YAML-like metadata block)
→ ParsedDeveloperSpec（系统结构化理解 + open_questions）
→ SourceManifest（表字段事实追踪，optional SchemaRegistry 补充）
→ RelationshipHypothesis（Join 推理 + 证据定级：强/中/弱/无）
→ SqlBuildPlan / SqlProgram（受控 SQL 构建计划，8 step 类型化 DAG）
→ SQL Validator → Compiler（确定性渲染）→ DuckDB Executor
→ SQL Code Review Package（供程序员审查）

[Spark-first v2.0]
→ DataTransformContract（从已验证 SqlBuildPlan 确定性抽取）
→ SparkDeveloper → Static Validator → 受控 PySpark DSL
→ SQL/Spark 双链验证（PlanEquivalence + ResultComparator）
```

## 关键边界

### SQL

- LLM 不生成 SQL 文本或 SQL 片段。
- LLM 只输出严格类型化 SqlBuildPlan（scan/filter/join/aggregate/project/case_when/sort/limit）。
- Python 编译器确定性生成 SQL；相同 SqlBuildPlan 两次编译产生相同 SQL 和哈希。
- 禁止 `raw_sql`、`where_sql`、`join_on: str`、`expression: str`。
- 表、字段和 Join 必须来自 SourceManifest；SchemaRegistry 只补充不覆盖——冲突输出 SOURCE_CONFLICT。

### PySpark

PySpark 只能以受控纯转换函数形式生成：

```python
def transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame:
    ...
```

代码只读取注入的 inputs，禁止自行读取数据、Action、写入、UDF、网络、文件系统和动态执行。SparkDeveloper 读 DataTransformContract（不从 DeveloperSpec 重新推理业务逻辑）。Reviewer 只输出修订指令，最终修订仍由 Developer 完成；测试代码同样需要安全校验和隔离执行。

### 验证

SQL 与 Spark 读取同一个关系一致冻结快照。确定性 Comparator 可以产生 `CONSISTENT_SAMPLE`，但该状态只说明样本一致，不代表业务绝对正确、全量性能合格或获准上线。逻辑链路（PlanEquivalence：SqlBuildPlan vs ExtractedSparkPlan）和物理链路（ResultComparator）双链验证。

### LangGraph

LangGraph 只负责编排、分支、checkpoint、重试和人工中断。业务逻辑是普通 Python 服务；Graph State 只保存 artifact 引用、哈希、状态和摘要。

### Memory 边界

本项目不建设独立 Engineering Memory。失败案例沉淀进入 Harness 回归集、确定性 Validator / Compiler / Optimizer 规则、SchemaRegistry / Contract 显式标注和 Prompt/Harness 版本化评测记录。运行时路由、规划与生成不读取长期 Memory。事实源只有 SourceManifest / SchemaRegistry / Contract。

## 规划文档

核心事实源：

- `docs/00-product-charter.md` — 产品宪章
- `docs/01-target-architecture.md` — 目标架构
- `docs/02-reuse-and-migration-map.md` — 复用与迁移地图
- `docs/03-sql-ir-and-compiler-plan.md` — SQL IR 与编译器计划
- `docs/04-spark-multi-agent-plan.md` — 受控 PySpark DSL（占位）
- `docs/05-cross-validation-and-repair-plan.md` — 双链验证（占位）
- `docs/06-langgraph-orchestration-plan.md` — 编排（占位）
- `docs/07-harness-and-memory-plan.md` — Harness + Memory（占位）
- `docs/08-frontend-workbench-plan.md` — 内部交互验证口
- `docs/09-test-strategy.md` — 测试策略
- `docs/examples/` — DeveloperSpec 示例（汇总表/标签表/多步骤加工）
- `docs/roadmap/phase-0-5-developer-spec-architecture-migration.md` 至 `phase-8-spark-first-orchestration-hardening.md`（18 个 Phase 文件）

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

- pytest：~1488 个用例（覆盖 SQL IR、Compiler、Validator、Pipeline API、SparkPlan 映射、Harness 等全部链路）。
- Phase 0.5 已退出——文档与路线图统一完毕。
- 已有 ruff 问题将在独立 A 类小修中处理。
