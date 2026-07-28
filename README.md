# TianShu DataDev Agent v3

AI 辅助数据开发工具。接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书，经 **确定性管线 + LLM 增强** 生成 SQL、PySpark DSL、双链验证报告和 Code Review Package。

> **最终产物是代码和审查材料，不是生产数据。系统不自动上线、不写生产库。**

---

## 快速开始

```bash
pip install -e ".[dev]"
./dev-reload.sh          # 启动前后端（详见下方"开发命令"）
# 浏览器打开 http://127.0.0.1:5173
```

---

## 当前状态

**Capability Complete** — SQL→PySpark 全量 10 种 step 双链验证已贯通，管线已合入 `main`。

| 维度 | 状态 |
| --- | :---: |
| SQL 管线（Parse→Plan→Validate→Compile→Execute） | ✅ |
| PySpark DSL（10 种封闭 step 类型：scan/filter/project/aggregate/join/case_when/window/sort/limit/subquery） | ✅ |
| SQL/Spark 逻辑等价对比（PlanComparator，含窗口合并、多分支、Snapshot 桥接） | ✅ |
| 物理双引擎验证（DuckDB ↔ Spark，含浮点容差 + CRE 编码比较体系） | ✅ |
| label_table v1 完整管线（Parser→LlmLabelExtractor→Validator→Promotion→Builder CaseWhenStep→Compiler） | ✅ |
| RatioExpr 全链路（RatioProposal→Validator→Decl→Expr→Compiler→Contract→Spark） | ✅ |
| RequirementPlanner v3.1（TimeTransformExpr + UncertaintyEntry 路由） | ✅ |
| ComputeSteps Builder 双能力扩展（case_when+metrics 共存 + 混合源 Join + 两跳桥接） | ✅ |
| SparkPlan 多分支 DAG（branches + typed_branches 替换 SqlRawExpression） | ✅ |
| Snapshot 桥接 + 前端增强（LLM 追踪、物理验证对比、Run-All 面板三层兜底） | ✅ |
| NYC 业务案例 01-06 全量 SQL+Spark LOGIC_EQUIVALENT 验证 | ✅ |
| C1-C4 业务集成风险已全部消除 | ✅ |

> **详细进度矩阵、测试基线、残留风险 → `docs/current-state-and-verification-status.md`**

---

## 架构

### 数据流

```text
DeveloperSpec (.md 项目书，Markdown + YAML metadata)
  │
  ▼
ParsedDeveloperSpec（确定性 Parser → 结构化理解）
  │  ├─ RequirementPlanner（LLM：补充维度/指标/CASE WHEN）
  │  ├─ SpecEnricher（LLM：比率表达式、窗口帧边界）
  │  └─ LabelExtractor（label_table 类型：标签规则提取 + 校验 + 提升）
  ▼
SqlBuildPlan / SqlProgram（10 种封闭 step 类型 DAG）
  │  └─ ComputeStepValidator（符号解析、类型兼容、基数安全）
  ▼
┌─ SQL Validator → Compiler → Executor → SQL Code Review Package
│     （确定性渲染，禁止 raw_sql / where_sql / expression: str）
│
└─ Spark（从头路径）
      │
      ├─ DataTransformContract（从已验证 SqlBuildPlan 确定性抽取）
      ├─ mapper.py → baseline SparkPlan（确定性，唯一结构路径）
      ├─ SparkDeveloper（LLM 只做语义标注，不增删改 step）
      ├─ SparkCompiler + Renderer（确定性生成 PySpark DSL）
      ├─ Validator（AST call-chain 硬门禁）
      └─ SQL/Spark 双链验证
           ├─ 逻辑等价（PlanComparator：SqlBuildPlan ↔ SparkPlan）
           └─ 物理一致（PhysicalVerifier：DuckDB ↔ PySpark 同一快照）
```

### 关键原则

| 原则 | 说明 |
|--- | ---|
| **LLM 不生成 SQL/代码** | LLM 只输出结构化声明（ParsedDeveloperSpec、SqlBuildPlan、SparkPlan 标注）。代码由 Python 确定性编译器生成。 |
| **封闭 step 类型** | SqlBuildPlan：10 种封闭类型；SparkPlan：scan/filter/project/aggregate/join/case_when/window/sort/limit。禁止 `raw_sql`、`expression: str` 逃生口。 |
| **双链验证** | 逻辑（PlanComparator 结构等价）+ 物理（DuckDB+Spark 同一快照双引擎执行对比）。状态使用 `LOGIC_EQUIVALENT` / `RESULT_CONSISTENT` / `NOT_EXECUTED` / `HUMAN_REVIEW`。 |
| **依赖确定性** | 表/字段/Join 必须来自 SourceManifest；SchemaRegistry 只补充不覆盖；冲突 → SOURCE_CONFLICT。 |
| **编排薄层** | LangGraph 只做编排/分支/checkpoint/重试，不接触模型、不构造 Prompt、不解析 LLM 自由文本。 |

---

## 目录结构

```text
src/tianshu_datadev/
├── developer_spec/     # Parser、ParsedDeveloperSpec、SourceManifest
├── planning/           # RelationshipHypothesis、SqlBuildPlan、SqlProgram
│   ├── requirement_planner.py   # v3.1：LLM 基础声明生成
│   ├── spec_enricher.py         # 窗口帧/比率/标签增强
│   └── sql_build_plan.py        # Builder（含 CaseWhenStep、ComputeSteps）
├── labels/             # LabelExtractor、Validator、Promotion（label_table 类型）
├── sql/                # Validator、确定性 Compiler
├── spark/              # mapper、Developer（LLM 标注）、Compiler、Validator、Reviewer
├── execution/          # 快照、DuckDB/Spark 隔离执行
├── validation/         # PlanComparator、PhysicalVerifier、CRE
├── orchestration/      # LangGraph 薄编排层
├── artifacts/          # Code Review Package、Contract Extractor
└── llm/                # LLM Gateway、Prompt 版本管理、调用追踪

frontend/src/
├── App.tsx             # 主应用状态机 & 面板布局
├── api/client.ts       # API 客户端 + NDJSON 流式消费
└── components/         # SpecEditor、ParsePreview、PlanStepsPanel、SqlDisplay、
                        # LlmTracePanel、RunProgressPanel、SparkStageButtons 等
```

---

## 文档

| 入口 | 说明 |
|--- | ---|
| **`docs/README.md`** | 文档索引与分类入口（推荐从这里开始） |
| **`docs/current-state-and-verification-status.md`** | 当前实施状态的唯一权威文档 |
| **`AGENTS.md`** | 项目宪法——所有 Agent 必须遵守 |
| `docs/00-product-charter.md` ~ `docs/09-test-strategy.md` | 架构与设计参考 |
| `docs/superpowers/specs/` | 各特性完整设计文档 |
| `docs/superpowers/plans/` | 方案书索引 |
| `docs/examples/` | DeveloperSpec 示例（汇总表/标签表/多步骤加工） |

---

## 开发命令

```bash
# ── 安装 ──
pip install -e ".[dev]"

# ── 重启服务（Windows Git Bash 下唯一入口）──
./dev-reload.sh               # 前后端全重启
./dev-reload.sh --backend     # 仅后端
./dev-reload.sh --frontend    # 仅前端

# ── 测试 ──
python -m pytest tests/ -q    # 非 Spark/非 Harness 子集
python -m pytest tests/ --run-slow   # 含 Spark（需 PySpark）

# ── 代码检查 ──
ruff check src/ tests/
npx tsc --noEmit              # TypeScript 类型检查

# ── 前端单独 ──
cd frontend && npm run dev    # 开发模式（Vite HMR）
```

---

## 测试基线

> 数据口径：2026-07-26（近两周新增大量测试：ComputeSteps 扩展、RatioExpr、RequirementPlanner v3.1、集成测试等，测试计数持续增长）。

- Ruff / tsc / build：**零告警**
- 详见 `docs/current-state-and-verification-status.md` §1

---

## 许可

MIT License
