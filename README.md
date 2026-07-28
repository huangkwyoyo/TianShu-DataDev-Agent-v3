# TianShu DataDev Agent v3

AI 辅助数据开发工具。程序员用**半自然语言 + 半结构化 Markdown** 编写需求说明书（DeveloperSpec），系统经混合管线（确定性引擎 + LLM 增强）自动生成 SQL、PySpark DSL、双引擎验证报告和代码审查包。

> **最终产物是代码和审查材料，不是生产数据。系统不自动上线、不写生产库。人是最终代码审查者和上线决策者。**

---

## 1. 项目简介

TianShu DataDev Agent 解决"从中文业务需求到可信数据代码"之间的翻译鸿沟。传统做法是程序员读懂业务描述后手写 SQL/Spark，再逐条验证——效率低且易遗漏边界。本工具将这一过程半自动化：

- **输入**：程序员写的半结构化工单（DeveloperSpec），包含业务描述、表映射、指标/维度声明和 YAML metadata
- **处理**：确定性 Parser → LLM 补充推理 → 受控 SqlBuildPlan → 确定性代码生成 → 双链验证
- **输出**：SQL 代码、PySpark DSL 代码、逻辑等价证明、物理验证报告、Code Review Package

技术栈：Python 3.12+、FastAPI、Pydantic v2、DuckDB、PySpark 4.x、React + TypeScript、LangGraph。

---

## 2. 项目目标

1. **半结构化输入 → 可信代码**：程序员用自然语言 + 表格描述业务逻辑，系统生成经双引擎验证的生产级 SQL 和 PySpark
2. **LLM 辅助，而非 LLM 生成**：LLM 只输出结构化声明，代码由确定性编译器生成——杜绝不可控的 SQL 片段
3. **SQL/Spark 双链验证**：同一业务规格使用两种引擎独立生成代码、独立执行、自动比对——发现差异而不是掩盖差异
4. **人机协作兜底**：LLM 低置信、能力边界内的事情自动路由到人工审核（HUMAN_REVIEW），不静默绕过

---

## 3. 核心能力

| 能力 | 说明 |
| --- | :---: |
| **DeveloperSpec 解析** | Parser 将半结构化 Markdown 确定性解析为 ParsedDeveloperSpec（表、指标、维度、Join、时间范围） |
| **RequirementPlanner v3.1** | LLM 从业务描述补充维度/指标/CASE WHEN 规则，支持 TimeTransformExpr + UncertaintyEntry 路由 |
| **SpecEnricher** | LLM 补充 RatioExpr（比率表达式）、窗口帧边界、evaluation_phase 回填 |
| **label_table v1 完整管线** | LlmLabelExtractor → Validator → Promotion → Builder CaseWhenStep 完整端到端 |
| **SqlBuildPlan / SqlProgram** | 10 种封闭 step 类型 DAG：Scan、Filter、Project、Aggregate、Join、CaseWhen、Window、Sort、Limit、Subquery |
| **ComputeSteps Builder** | case_when + metrics 可共存，支持混合源 Join + 两跳桥接 JOIN |
| **SQL 确定性编译器** | 相同 SqlBuildPlan 始终生成相同 SQL + 哈希。禁止 `raw_sql`、`expression: str` |
| **PySpark DSL** | 10 种对应 step，经 mapper→Developer→Compiler→Renderer→Validator 确定性生成 |
| **SparkPlan 多分支 DAG** | branches 字段将 ComputeSteps 编译为独立 DataFrame 分支 |
| **PlanComparator 逻辑对比** | SqlBuildPlan ↔ SparkPlan 结构等价，含窗口合并、三层剥离 |
| **PhysicalVerifier 物理对比** | DuckDB ↔ PySpark 同一快照双引擎执行 + CRE 编码比较体系 |
| **Code Review Package** | 完整溯源链 + 阶段结果 + 对比器状态 + REVIEW_READY 判定 |
| **前端工作台** | 模板选择、MD 编辑器、阶段执行、Run-All 全流程、LLM 追踪面板、双引擎对比摘要 |

> **全部 20+ 项子能力已在 Phase 0.5-9C + label_table v1 + 扩展阶段完成。详见 `docs/current-state-and-verification-status.md`。**

---

## 4. 非目标（明确不做什么）

| 事项 | 说明 |
| --- | --- |
| ❌ **不生成生产数据** | 系统产物是代码和审查材料，不写入生产库 |
| ❌ **不上线** | 无 CI/CD 部署、无调度执行、无生产凭据 |
| ❌ **不做 LLM 代码生成** | LLM 不写 SQL/PySpark 文本，只输出结构化声明 |
| ❌ **不做通用代码生成** | 仅覆盖数据分析场景（SQL + PySpark），不做通用软件工程 |
| ❌ **不建 Memory 系统** | 失败案例不进 Memory，而是沉淀为回归测试/Validator 规则/Harness 样例 |
| ❌ **不维护设计文档** | 文档条目已全部完成并归档保留。实施状态以 current-state.md 为准 |
| ❌ **不做运行时学习** | 运行时不读取长期 Memory。事实源仅 SourceManifest / SchemaRegistry / Contract |

---

## 5. 整体架构

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
└─ Spark（从头路径，不读 SQL 文本）
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

### 关键设计原则

| 原则 | 说明 |
| --- | --- |
| **LLM 不生成 SQL/代码** | LLM 只输出结构化声明。代码由 Python 确定性编译器生成。 |
| **封闭 step 类型** | 10 种封闭类型，禁止 `raw_sql`、`expression: str` 逃生口 |
| **双链验证** | 逻辑结构等价 + 物理引擎执行结果一致性，状态精确（禁止泛化 PASS） |
| **依赖确定性** | 表/字段/Join 必须来自 SourceManifest；SchemaRegistry 只补充不覆盖 |
| **编排薄层** | LangGraph 只做编排/分支/checkpoint/重试，不构造 Prompt、不解析自由文本 |

---

## 6. Agent 工作流程

```text
1. 程序员编写 DeveloperSpec（.md 文件，含 YAML metadata）
2. Parser 确定性解析 → ParsedDeveloperSpec + open_questions
3. RequirementPlanner（LLM）补充维度、指标、CASE WHEN 声明
4. SpecEnricher（LLM）补充 RatioExpr、窗口帧边界、evaluation_phase
5. LabelExtractor（LLM，仅 LABEL_TABLE 类型）提取标签规则
6. ComputeStepValidator 五项确定性校验
7. Builder 编译为 SqlBuildPlan DAG
8. SQL Compiler 确定性生成 SQL → DuckDB 执行
9. DataTransformContract 确定性抽取 → mapper → SparkPlan → SparkCompiler
10. PlanComparator 逻辑等价对比
11. PhysicalVerifier 物理双引擎结果对比
12. SparkReviewBuilder 构建完整审查包（review_ready 判定）
```

整个流程可通过前端 Run-All 一键执行，也可逐步执行（Parse → Plan → Execute → Spark 各阶段）。

---

## 7. 数据架构

### 快照（Snapshot）

- SQL 与 Spark 读取**同一个关系一致、不可变的 Parquet 快照**
- Snapshot Builder 使用锚点键和 Join 白名单级联抽取
- 多表快照禁止各表独立 LIMIT

### 契约链

```text
ParsedDeveloperSpec → SqlBuildPlan → DataTransformContract → SparkPlan
     （业务语义）      （SQL 执行计划）      （独立规格契约）       （Spark 执行计划）
```

- DataTransformContract 从已验证 SqlBuildPlan **确定性抽取**，不包含实现代码
- 各阶段通过 artifact hash 追溯

### 状态机

验证状态使用精确枚举，禁止泛化 PASS：

| 逻辑对比 | 物理对比 |
| --- | --- |
| `LOGIC_EQUIVALENT` | `RESULT_CONSISTENT` |
| `LOGIC_MISMATCH` | `RESULT_MISMATCH` |
| `LOGIC_UNSUPPORTED` | `UNSUPPORTED_SEMANTICS` |
| `NOT_EXECUTED` | `NOT_EXECUTED` |
| — | `HUMAN_REVIEW` |
| — | `EXECUTION_ERROR` |

---

## 8. LLM 使用边界

LLM 在系统中的角色被严格限定：

| 允许 | 禁止 |
| --- | --- |
| 输出 ParsedDeveloperSpec 声明 | 生成 SQL 文本或片段 |
| 输出 RelationshipHypothesis（Join 候选） | 输出 `where_sql`、`join_on: str`、`expression: str` |
| 输出 SqlBuildPlan（结构化 step） | 直接改 SQL |
| 输出 SparkPlan 语义标注 | 增删改 SparkPlan step |
| 输出 RatioProposal | 携带自由 SQL 表达式 |
| 输出 LabelDomainOutput（提取标签规则） | 绕过 Validator/Compiler |

**全部代码必须由 Python 确定性编译器生成。** LLM 的输出是结构化数据——经过 Pydantic 校验后，才能进入下一阶段。

---

## 9. Harness / Eval 体系

| 维度 | 说明 |
| --- | :---: |
| D1 安全 | 拒绝越权 SQL 和 PySpark 代码 |
| D2 语义 | 拒绝语义不等价代码 |
| D3 功能 | 正确代码应成功执行 |
| D4 修复 | 修复代码不引入新问题 |
| D5 完整性 | 代码结构完整性门禁 |

- Harness 是独立评测框架，不作为产品运行时依赖
- pytest 覆盖确定性逻辑、安全边界、黄金路径
- LLM 真实调用进入 Harness；pytest 使用确定性 Fake Adapter
- 失败案例沉淀为回归测试（`tests/` + `harness/datasets/regression/`）和 Validator/Compiler 确定性规则

---

## 10. 项目结构

```text
src/tianshu_datadev/
├── developer_spec/           # Parser、ParsedDeveloperSpec、SourceManifest
├── planning/                 # RequirementPlanner、SpecEnricher、
│                               RelationshipHypothesis、SqlBuildPlan、SqlProgram
├── labels/                   # LabelExtractor、Validator、Promotion
├── sql/                      # Validator、确定性 Compiler
├── spark/                    # mapper、Developer、Compiler、Validator、Reviewer
├── execution/                # 快照、DuckDB/Spark 隔离执行
├── validation/               # PlanComparator、PhysicalVerifier、CRE
├── orchestration/            # LangGraph 薄编排层
├── artifacts/                # Code Review Package、Contract Extractor
└── llm/                      # LLM Gateway、Prompt 版本管理、调用追踪

frontend/src/
├── App.tsx                   # 主应用状态机 & 面板布局
├── api/client.ts             # API 客户端 + NDJSON 流式消费
└── components/               # SpecEditor、ParsePreview、PlanStepsPanel、
                                SqlDisplay、LlmTracePanel、RunProgressPanel 等

docs/                         # 完整文档体系（详见 §15）
├── README.md                 # 文档索引入口
├── current-state-and-verification-status.md  # 当前实施状态（唯一权威）
├── examples/                 # DeveloperSpec 示例
└── superpowers/
    ├── specs/                # 各特性设计文档
    └── plans/                # 方案书索引
```

---

## 11. 快速开始

```bash
# 安装
pip install -e ".[dev]"
cd frontend && npm install && cd ..

# 启动服务（Windows Git Bash 下统一入口）
./dev-reload.sh               # 前后端全重启
# 浏览器打开 http://127.0.0.1:5173

# 选择模板 → 编辑 DeveloperSpec → 单击"Run All"查看全流程输出
```

---

## 12. 配置说明

### API Key

label_table 类型和 RequirementPlanner 需要 LLM Adapter：

```bash
export LLM_API_KEY="sk-..."       # DeepSeek / 其他兼容 API
export LLM_ENDPOINT="https://..."  # 自定义端点（可选）
```

> 无 API Key 时，label_table 请求返回 CONFIG_ERROR，SparkDeveloperService 标记为 SKIPPED。pytest 使用 FakeAdapter 不依赖 Key。

### PySpark

- PySpark 4.1.2 需安装在系统 Python 环境
- Java 17+ 可用；`JAVA_HOME` 指向 JDK 8 不影响 PySpark 4.x 运行
- `dev-reload.sh` 使用系统 Python（非虚拟环境）

### 数据源映射

通过前端 tablePaths 配置或 API 参数传入表路径映射。

---

## 13. 测试与验收

| 命令 | 说明 |
| --- | --- |
| `python -m pytest tests/ -q` | 非 Spark/非 Harness 子集（~1600+ tests） |
| `python -m pytest tests/ --run-slow` | 含 Spark 全量（需 PySpark 环境） |
| `ruff check src/ tests/` | Python lint |
| `npx tsc --noEmit` | TypeScript 类型检查 |
| `git diff --check` | whitespace 检查 |

**当前基线**（2026-07-26）：

- Ruff / tsc / build：**零告警**
- 前次 pytest 全量采集：**2818 tests collected**（近两周大幅增长后待重采集）
- CRE 核心：125 passed / 7 skipped
- Physical Verifier（含 CRE shadow）：191 passed / 11 skipped
- 前端 E2E：6/6 Playwright 测试通过

---

## 14. 开发流程

```text
1. 修改源码（src/ 或 frontend/src/）
2. 运行 pytest + ruff + tsc 验证
3. 执行 ./dev-reload.sh 重启服务
4. 浏览器验证效果（Ctrl+Shift+R 强制刷新）
5. 原子提交（一个 commit 一个逻辑改动）
```

> **Windows Git Bash 下 Vite HMR 和 uvicorn --reload 不可靠。任何修改后如需验证，必须通过 `./dev-reload.sh` 重启。**

---

## 15. 文档体系

| 文档 | 说明 |
| --- | --- |
| **`docs/README.md`** | 文档索引与分类入口 |
| **`docs/current-state-and-verification-status.md`** | 当前实施状态的唯一权威文档 |
| **`AGENTS.md`** | 项目宪法——所有 Agent 必须遵守 |
| `docs/00-product-charter.md` ~ `09-test-strategy.md` | 架构与设计参考 |
| `docs/pipeline_主链路详解_20260702_2140.md` | SQL 管线内部实现 |
| `docs/superpowers/specs/` | 各特性完整设计（label_table、CRE、Planner 等） |
| `docs/superpowers/plans/README.md` | 方案书索引 |
| `docs/examples/` | DeveloperSpec 示例（汇总表/标签表/多步骤加工） |

---

## 16. 当前状态

**Capability Complete** — 全部核心能力已实现并通过验证。

### 能力矩阵

| 维度 | 状态 |
| --- | :---: |
| SQL 管线（Parse→Plan→Validate→Compile→Execute） | ✅ |
| PySpark DSL 全 10 种 step | ✅ |
| SQL/Spark 逻辑等价对比（PlanComparator） | ✅ |
| 物理双引擎验证（DuckDB ↔ Spark + CRE） | ✅ |
| label_table v1 完整管线 | ✅ |
| RatioExpr 全链路 | ✅ |
| RequirementPlanner v3.1 | ✅ |
| ComputeSteps Builder 双能力扩展 | ✅ |
| SparkPlan 多分支 DAG | ✅ |
| Snapshot 桥接 + 前端面板增强 | ✅ |
| NYC 业务案例 01-06 全量验证 | ✅ |
| C1-C4 业务集成风险 | ✅ 已消除 |

### 残留风险

| 编号 | 说明 | 等级 |
| --- | --- | :---: |
| R9 | Case 05 Window 规范化差异（DuckDB ↔ Spark 帧边界行为差异） | **C（保守阻断）** |
| R-CRE-Golden | Golden Registry 为空 | 低（非阻断） |
| R-CRE-Null | `null_strategy` 始终 UNKNOWN | 低（非阻断） |
| R-LT-1 | CASE WHEN condition 语义等价对比未实现（设计取舍） | **B（按需建设）** |
| R-LT-3 | condition ColumnRef 表别名依赖（多表场景） | **B（当前被阻断）** |

> 详见 `docs/current-state-and-verification-status.md` §3。

---

## 17. Roadmap

全部 Phase 0.5-9C 及扩展功能（label_table v1、RatioExpr、RequirementPlanner v3.1、ComputeSteps 扩展、SparkPlan 多分支、Snapshot 桥接）**已全部完成并合入 main**。

### 后续方向

- 测试基线重采集
- CRE 门禁切换（Golden Registry 填充、NULL strategy 处理）
- CASE WHEN condition 等价比较（按需建设）
- `_temp_` 前缀检测统一（低优先级维护债）
- 生产环境 LLM 验证
- 物理验证溢出降级精度验证

---

## 18. 风险与约束

### 架构约束

- 表/字段/Join 必须来自 SourceManifest——不来自 LLM Memory
- Join 推理：LLM 提候选 → Validator 确定性定级 → WEAK/NONE 硬阻断
- 性能门禁：确定性 PerfValidator 执行，LLM 不做性能决策
- SQL 优化：幂等优化 pass，相同 SqlBuildPlan 两次编译相同哈希

### 环境依赖

- Windows 开发环境（`dev-reload.sh` 基于 Windows Git Bash）
- PySpark 4.1.2 需要系统 Python 安装 + Java 17
- LLM API Key 是 label_table 和 SparkDeveloper 的前置条件（无 Key 时以 CONFIG_ERROR 或 SKIPPED 安全降级）

### 已知局限

- CASE WHEN condition（谓词条件）语义等价对比当前为 UNSUPPORTED——仅结构骨架已验证
- Case 05 窗口帧边界规范化差异属于引擎行为差异，非代码 bug
- 系统所有文档条目已全部完成实施，不再维护新的设计文档

---

## 19. FAQ

**Q：LLM 生成 SQL 吗？**
A：不。LLM 只输出 ParsedDeveloperSpec、SqlBuildPlan、SparkPlan 标注等结构化数据。SQL 和 PySpark 代码由 Python 确定性编译器生成。完全相同的输入始终产生完全相同的输出。

**Q：系统能替代程序员吗？**
A：不能。系统定位是"辅助工具"——将半结构化需求翻译为可信代码供审查。人是最终审查者和上线决策者。

**Q：为什么 SQL 和 Spark 要做两遍？**
A：双链验证的核心逻辑：两种独立引擎从同一业务规格出发，用各自方式生成代码并执行，然后自动比对结果。一致则增强信心，不一致则发现差异。单一引擎无法发现自身错误。

**Q：验证状态为什么不用 PASS/FAIL？**
A：业务正确性不是自动系统能判定的。LOGIC_EQUIVALENT 不代表业务逻辑正确，RESULT_CONSISTENT 不代表满足业务需求。精确状态语言避免虚假安全感。

**Q：没有 API Key 能跑吗？**
A：能。pytest 使用 FakeAdapter 不依赖 Key。前端可以执行 Parse/Plan/Execute（这些阶段使用确定性 Fallback）。label_table 和 SparkDeveloper 需要 Key 时返回明确的配置错误提示。

**Q：系统能处理多大数据量？**
A：Snapshot Builder 面向分析型工作负载（Parquet 快照），不做 OLTP 或流处理。物理验证受制于 DuckDB 和 PySpark 的执行环境。

---

## 许可

MIT License
