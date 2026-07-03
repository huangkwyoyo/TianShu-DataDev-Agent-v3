# TianShu DataDev Agent v3 工程术语表

本文解释本项目当前阶段的常见工程术语。每个术语按九件事说明：

1. **术语名称**：一句话解释
2. **是什么**：用中文说清楚它是什么
3. **解决什么问题**：为什么项目里需要它
4. **在当前项目中的位置**：列出可能相关的目录或文件，如无则写"待实现"
5. **输入是什么**：它接收什么
6. **输出是什么**：它产出什么
7. **出错会导致什么风险**：如果它设计不好，会造成什么问题
8. **简单例子**：结合真实业务场景举例
9. **Owner 审查时应该问什么**：2-3 个项目 Owner 可以用来审查的问题

---

## 1. DataDev Agent

**一句话解释**：接收程序员编写的半自然语言 + 半结构化 DeveloperSpec 项目书，生成 SQL、PySpark、测试和 Code Review Package 的 AI 辅助数据开发工具。

**是什么**

DataDev Agent 是本项目的核心产品。它不面向业务人员的自然语言问数，而是面向数据开发程序员——程序员用结构化的 DeveloperSpec（Markdown + YAML-like 元数据块）描述数据开发需求，Agent 经过解析→增强→构建→验证→编译→执行→契约→打包的确定性流水线，产出可审查的 SQL artifact、Spark artifact、测试和 Code Review Package。系统不自动上线、不写生产库、不生成生产数据。

流水线有两个入口方法：`execute()` 执行前五个阶段（解析→增强→构建→编译→执行），产出 SQL 执行结果；`run_all()` 走完整八个阶段，额外产出 DataTransformContract 和 Code Review Package。两个方法内部都有三条代码路径——ComputeSteps（分步计算）、多跳链、单 SQL——每条路径在 Build 阶段的处理方式不同，但最终都经过相同的 Validator 门禁和 RUNTIME_FAIL 执行检查。

**解决什么问题**

把数据开发中重复性的 SQL/PySpark 代码生成标准化——程序员只需描述"要什么"，Agent 负责生成合规代码。同时通过硬门禁（Validator/PerfValidator/WriteValidator）避免 LLM 生成不可控的自由 SQL 片段。

**在当前项目中的位置**

- `src/tianshu_datadev/api/pipeline.py` — Pipeline 完整链路编排（类名 `Pipeline`，约 1400 行）
- `src/tianshu_datadev/developer_spec/` — 输入解析
- `src/tianshu_datadev/planning/` — 规划与推理
- `src/tianshu_datadev/sql/` — SQL 编译与执行
- `src/tianshu_datadev/artifacts/` — 产物打包
- `AGENTS.md` — 系统边界（产品宪法）

**输入是什么**

程序员编写的 DeveloperSpec 项目书（Markdown 正文 + YAML-like 元数据块，含表声明、字段声明、Join 声明、指标定义、维度定义、时间范围、输出规格）。

**输出是什么**

DataTransformContract（业务规格） + CompiledSql / SqlProgramArtifact（SQL 产物） + ExecutionTrace（执行追踪） + ReviewPackageManifest（Code Review Package）。

**出错会导致什么风险**

如果 Agent 的 LLM 组件绕过 Validator 直接生成 SQL，将失去编译前安全门禁——可能生成破坏性操作或引用未注册的表/字段。如果 Pipeline 的有状态存储泄漏，多次调用之间可能混淆 artifact 引用。

**简单例子**

程序员提交一份 DeveloperSpec 描述"日活用户聚合表——从订单事实表按日期+用户去重计数"，Agent 走完整链路：Parser 解析→Enricher 增强→Builder 生成 SqlBuildPlan（ScanStep + AggregateStep + SortStep）→Validator 校验→Compiler 生成 `SELECT stat_date, COUNT(DISTINCT user_id) AS dau FROM ... GROUP BY stat_date ORDER BY stat_date DESC`→Executor 在 DuckDB 快照上执行→Contract 抽取→Packager 打包 ReviewPackage。

**Owner 审查时应该问什么**

1. "能否向我证明，当前系统中没有任何一条 SQL 是 LLM 直接输出并执行的？"
2. "如果程序员在 DeveloperSpec 中声明了一个不存在的表名，链路会在哪一步停下？停下的输出是什么？"
3. "Pipeline 的 _results 字典在不同 request_id 之间的隔离性如何？执行和 run_all 产物的 TTL 过期策略是否一致？"

---

## 2. DeveloperSpec

**一句话解释**：程序员编写的半结构化数据开发需求书，是 Agent 的权威业务输入。

**是什么**

DeveloperSpec 是程序员用 Markdown 正文 + YAML-like 元数据块编写的需求文档。元数据块包含：目标表名、粒度、源表声明（含 key_columns / business_columns / role / time_field）、指标定义（aggregation + input_column）、维度定义、Join 声明（join_keys + join_type）、时间范围、输出列规格。正文部分是纯 Markdown 叙述，描述业务目标和特殊说明。

**解决什么问题**

用结构化声明替代自然语言问数——关键信息（表名、字段名、Join 键、聚合类型）由程序员显式声明，不给 LLM 自由推测空间。避免自然语言问数中的歧义、口径冲突和表选择错误。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py` — ParsedDeveloperSpec 模型
- `src/tianshu_datadev/developer_spec/parser.py` — DeveloperSpecParser 解析器
- `tests/fixtures/golden/` — 6 个黄金用例（golden_no_time_range.md 等）
- `tests/fixtures/reject/` — 6 个拒绝用例

**输入是什么**

无——DeveloperSpec 本身是程序员手写的输入文件。进入系统后由 DeveloperSpecParser 解析。

**输出是什么**

被解析为 ParsedDeveloperSpec（严格 Pydantic 模型），包含：source_tables、metrics、dimensions、relationships、time_range、output_columns 等结构化字段。

**出错会导致什么风险**

如果 DeveloperSpec 的元数据块语法过于宽松，程序员可能写出 Parser 无法解析的内容——导致解析失败或被静默跳过。如果元数据块允许 `raw_sql` 等自由字段，会破坏 SQL Generation Boundary 的安全红线。

**简单例子**

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.dau_daily
  target_grain: [stat_date]
  source_tables:
    - name: dwd.order_fact
      alias: o
      role: fact
      time_field: order_time
      key_columns:
        - name: order_id
          type: bigint
      business_columns:
        - name: user_id
          type: bigint
  metrics:
    - metric_name: dau
      aggregation: COUNT_DISTINCT
      input_column: o.user_id
  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
  time_range:
    range: last_7_days
    time_field: o.order_time
---
# 日活用户聚合

统计最近 7 天每天的去重活跃用户数。
```

**Owner 审查时应该问什么**

1. "有哪些字段是程序员必须在 DeveloperSpec 中显式声明的？有哪些可以由 SchemaRegistry 补充？"
2. "如果把元数据块中的 table name 改成不存在的表，Parser 在哪个阶段发现？发现后的行为是什么？"
3. "DeveloperSpec 正文部分（Markdown 叙述）在解析后还有用吗？还是只用于人类阅读？"

---

## 3. ParsedDeveloperSpec

**一句话解释**：DeveloperSpec 经 Parser 确定性解析后的结构化 Pydantic 模型，extra="forbid"。

**是什么**

ParsedDeveloperSpec 是系统的第一层 IR（中间表示）。它将程序员手写的半结构化文本转换为严格的结构化对象。所有字段都来自元数据块，Parser 不进行任何 LLM 推理——只做确定性的文本解析和字段映射。关键设计约束：extra="forbid"，任何未定义的字段出现在元数据块中都会被拒绝。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py:422` — ParsedDeveloperSpec 定义
- `src/tianshu_datadev/developer_spec/parser.py:117` — DeveloperSpecParser.parse()

**输入是什么**

DeveloperSpec 原始文本（Markdown + YAML-like 元数据块字符串）。

**输出是什么**

ParsedDeveloperSpec 实例：包含 spec_id（确定性 hash）、spec_type、target_table、source_tables（list[InputTableDecl]）、metrics（list[MetricDecl]）、dimensions（list[DimensionDecl]）、relationships（list[JoinDecl]）、time_range（TimeRangeDecl）、output_columns（list[OutputSpecDecl]）、open_questions（list[OpenQuestion]）、parse_warnings（list[ParseWarning]）。

**出错会导致什么风险**

如果 Parser 允许未注册字段通过（extra="ignore"），程序员可能在 DeveloperSpec 中写入无效字段而不知情。如果 Parser 的列名推断逻辑有 bug，可能导致字段映射错误→Validator 拒绝合法字段或允许非法字段。

**简单例子**

解析"日活用户聚合"DeveloperSpec → ParsedDeveloperSpec(target_table="ads.dau_daily", source_tables=[InputTableDecl(name="dwd.order_fact", alias="o")], metrics=[MetricDecl(metric_name="dau", aggregation=COUNT_DISTINCT, input_column="o.user_id")], ...)。

**Owner 审查时应该问什么**

1. "Parser 生成 spec_id 用的是哪几个字段的哈希？修改 DeveloperSpec 正文（Markdown 叙述）会改变 spec_id 吗？"
2. "如果程序员在元数据块中声明了未定义的字段，Parser 是静默忽略还是报错？"

---

## 4. StrictModel

**一句话解释**：项目所有 Pydantic 模型的基类，强制 extra="forbid"，禁用字段名和类型推断。

**是什么**

StrictModel 继承自 `pydantic.BaseModel`，设置 `model_config = ConfigDict(extra="forbid", strict=True, frozen=False)`。项目中的所有 Schema 类（从 ParsedDeveloperSpec 到 SqlBuildPlan 到 HarnessReport）都继承 StrictModel。extra="forbid" 确保任何额外字段都会触发 ValidationError——这是安全边界的第一道防线。

**解决什么问题**

防止 LLM 或程序员在 JSON/Schema 中塞入未定义的字段，防止 Pydantic 的类型强制转换（如 str → int），确保所有数据流转在严格约束内。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py:23` — StrictModel 基类定义

**输入是什么**

无——StrictModel 是基类，被所有 Schema 类继承。

**输出是什么**

一个配置了 extra="forbid" + strict=True 的 BaseModel 子类。

**出错会导致什么风险**

如果 StrictModel 的 extra 被设为 "ignore" 或 "allow"，LLM 输出的 JSON 中可能包含未定义字段——这些字段会绕过 Schema 约束，可能被下游组件误用。如果 strict=False，Pydantic 可能将 "123" 强制转为 123，掩盖类型错误。

**简单例子**

```python
class MySchema(StrictModel):
    name: str
    count: int

# ✅ 合法
MySchema(name="test", count=42)

# ❌ 拒绝——extra 字段
MySchema(name="test", count=42, unknown_field="oops")  # → ValidationError

# ❌ 拒绝——类型不匹配（strict=True 时）
MySchema(name="test", count="not_a_number")  # → ValidationError
```

**Owner 审查时应该问什么**

1. "项目中是否存在任何不继承 StrictModel 的 Pydantic 模型？如果有，为什么？"
2. "strict=True 对 enum 字段的影响是什么？'duckdb' str 能通过 Literal['duckdb'] 校验吗？"

---

## 5. SourceManifest

**一句话解释**：事实源注册表——记录所有可用的表、列、类型、行数、键和外键引用。

**是什么**

SourceManifest 是数据的"户口本"。它从 DeveloperSpec 的 source_tables 声明中确定性构建，也可通过 SchemaRegistry（可选的外部类型/枚举补充）扩展。每个表记录 alias、物理表名、所有列（key_columns + business_columns）的类型和可空性、估算行数、外键引用。它是 SqlBuildPlan 中所有 ScanStep.table_ref 和 ColumnRef 的引用来源。

**解决什么问题**

Validator 用 SourceManifest 校验 SqlBuildPlan 中所有表引用和字段引用——任何未在 SourceManifest 中注册的表或列在编译前被拒绝。避免 LLM 编造表名和字段名。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py:443` — SourceManifest 定义
- `src/tianshu_datadev/developer_spec/source_manifest.py:73` — SourceManifestBuilder
- `src/tianshu_datadev/developer_spec/source_manifest.py:34` — SchemaRegistry Protocol

**输入是什么**

ParsedDeveloperSpec 的 source_tables 声明 + optional SchemaRegistry（Protocol——外部类型/枚举补全）。

**输出是什么**

SourceManifest 实例：包含 `tables: dict[str, ManifestTable]`（key=alias），每个 ManifestTable 含 key_columns、business_columns、row_count、foreign_keys。

**出错会导致什么风险**

如果 SourceManifest 中缺少程序员声明的列（构建时丢失），Validator 会错误地拒绝合法引用。如果 SchemaRegistry 静默覆盖了程序员声明的值（如类型冲突），可能产生 SOURCE_CONFLICT 未被记录到 open_questions。

**简单例子**

DeveloperSpec 声明了 `source_tables: [{name: "dwd.order_fact", alias: "o", key_columns: [{name: "order_id", type: "bigint"}], business_columns: [{name: "user_id", type: "bigint"}]}]` → SourceManifest.tables["o"] = ManifestTable(physical_name="dwd.order_fact", columns={"order_id": ManifestColumn(type="bigint"), "user_id": ManifestColumn(type="bigint")}, row_count=None)。

**Owner 审查时应该问什么**

1. "如果 SchemaRegistry 返回的类型与 DeveloperSpec 中程序员声明的类型不一致，SourceManifestBuilder 如何处理？"
2. "SourceManifest 中的 foreign_keys 是否影响 Join 推理？RelationshipPlanner 是否依赖它？"

---

## 6. RelationshipHypothesis

**一句话解释**：Join 关系的证据推理结果——对每对可能的 Join 关系进行 STRONG/MEDIUM/WEAK/NONE 四级定级。

**是什么**

RelationshipHypothesis 是 SqlBuildPlan 中 Join 推理的中间产物。它包含一组 JoinCandidate，每个候选记录 left_table、right_table、join_keys、evidence_level（STRONG/MEDIUM/WEAK/NONE）以及支撑该定级的 evidence 列表。三层分工：LLM 提候选 → Validator 确定性定级 → 人工确认中低置信。WEAK/NONE 是硬门禁——任何时候不得进入 SqlBuildPlan。

**解决什么问题**

防止不可靠的 Join 推理进入 SQL 编译——只有当两表之间的关系有充分证据（程序员显式声明 join_keys + 双方字段类型兼容）时，才能定级为 STRONG/MEDIUM。自动 Join 推理（模糊匹配列名）最多到 MEDIUM。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/relationship_hypothesis.py` — 完整定义
  - `JoinEvidenceLevel` — STRONG/MEDIUM/WEAK/NONE
  - `RelationshipEvidence` — 证据项
  - `JoinCandidate` — 候选 Join
  - `RelationshipHypothesis` — 假设集合
- `src/tianshu_datadev/planning/relationship_planner.py` — FakeRelationshipPlanner
- `src/tianshu_datadev/planning/relationship_validator.py` — RelationshipValidator（确定性定级）

**输入是什么**

ParsedDeveloperSpec 的 relationships 声明 + SourceManifest 的类型信息。

**输出是什么**

RelationshipHypothesis 实例：含 `candidates: list[JoinCandidate]`，每个 JoinCandidate 有 evidence_level 和 evidence 详情。

**出错会导致什么风险**

如果 WEAK/NONE Join 被错误定级为 MEDIUM 并进入 SqlBuildPlan，可能生成语义错误的跨表关联（如用户 ID 关联到订单金额）。如果 Validator 过度严格，STRONG 证据被降级为 MEDIUM 导致不必要的人工审查。

**简单例子**

DeveloperSpec 声明 `relationships: [{left_table: "u", right_table: "o", join_keys: [[u.user_id, o.user_id]], join_type: inner}]` → RelationshipPlanner 生成 JoinCandidate(candidate_id="jc_xxx", left_table="u", right_table="o", join_keys=[(ColumnRef("u.user_id"), ColumnRef("o.user_id"))]) → RelationshipValidator 检查双方类型均为 bigint→兼容→定级 STRONG→Join 可进入 SqlBuildPlan。

**Owner 审查时应该问什么**

1. "如果程序员在 DeveloperSpec 中声明了 join_keys=[(u.user_id, o.amount)]，但 user_id 是 bigint，amount 是 decimal——Validator 如何处理？"
2. "WEAK/NONE 的 Join 被硬门禁拦截时，OpenQuestion 中是如何描述的？程序员看到什么？"
3. "三层分工中 LLM 提候选这一步，在 Pipeline 中是如何模拟的？"

---

## 7. SqlBuildPlan

**一句话解释**：类型化的 SQL 构建计划——用 9 种封闭 Step 类型描述查询逻辑，禁止自由 SQL 片段。

**是什么**

SqlBuildPlan 是系统的第二层 IR。它由一组有序的 StepNode 组成，每个步骤通过 discriminated union（判别器 step_type）确定类型。当前支持 9 种 Step：ScanStep、FilterStep、JoinStep、AggregateStep、ProjectStep、CaseWhenStep、WindowStep、SortStep、LimitStep。关键设计约束：禁止 `raw_sql`、`where_sql`、`join_on: str`、`expression: str` 及其他自由 SQL 片段字段——所有 SQL 逻辑必须由确定性 Compiler 从 Step 节点生成。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/sql_build_plan.py:181` — SqlBuildPlan 定义
- `src/tianshu_datadev/planning/sql_build_plan.py:44-176` — 9 个 Step 类
- `src/tianshu_datadev/planning/sql_build_plan.py:237` — SqlBuildPlanBuilder（Fake，确定性）
- `src/tianshu_datadev/planning/models.py` — ColumnRef/Predicate/AggregateSpec/WindowExpr/SqlLiteral 等原子类型

**输入是什么**

ParsedDeveloperSpec + SourceManifest + RelationshipHypothesis（经由 SqlBuildPlanBuilder 组装）。

**输出是什么**

SqlBuildPlan 实例：含 `plan_id`、`steps: list[StepNode]`、`source_manifest_hash`、`relationship_hypothesis_hash`。多次构建相同输入 → 相同 plan_id（确定性哈希）。

**出错会导致什么风险**

如果 Step 类的字段允许自由 SQL 字符串（如 FilterStep 接受 `where_sql: str`），LLM 可能绕过 Compiler 直接注入 SQL 文本——这会让安全门禁体系崩溃。如果 steps 的顺序不符合 DAG 依赖（如聚合在 Join 前），Compiler 可能生成非法 SQL 或运行时失败。

**简单例子**

"日活用户聚合"需求 → SqlBuildPlan(steps=[ScanStep(table_ref="o", required_columns=[...]), AggregateStep(group_keys=[stat_date], agg_funcs=[AggregateSpec(func=COUNT_DISTINCT, input=ColumnRef("o.user_id"))]), SortStep(sort_keys=[SortSpec(column=stat_date, direction=DESC)])])。

**Owner 审查时应该问什么**

1. "SqlBuildPlan 的 9 个 Step 类型中，哪些是 Phase 1-3 可用、哪些是 Phase 4+ 才开放的？"
2. "如果需要在 SqlBuildPlan 中新增第 10 种 Step（如 UNION），需要改哪些文件？有哪些硬性规则？"
3. "plan_id 的哈希计算覆盖了 steps 的哪些字段？改变 step_id 会改变 plan_id 吗？"

---

## 8. SqlBuildPlanValidator

**一句话解释**：确定性的计划验证器——在编译前校验事实源引用、Join 门禁、禁则规则等 8 项检查。

**是什么**

SqlBuildPlanValidator 是编译前的最后一道结构安全门禁。它接收 SqlBuildPlan + SourceManifest + RelationshipHypothesis，执行 8 项确定性检查：空步骤拒绝、表引用校验、字段引用校验、Join key 类型兼容、WEAK/NONE Join 门禁、枚举值校验（Phase 1C）、时间过滤校验（大表必须有时间条件）、LIMIT 存在性（无聚合明细查询）。返回 passed: bool + questions: list[OpenQuestion]。

**解决什么问题**

确保 SqlBuildPlan 中的每个表引用和字段引用都在 SourceManifest 中注册——杜绝 LLM 编造表名/字段名。确保不可靠 Join（WEAK/NONE）不进入编译器。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/validator.py:38` — SqlBuildPlanValidator

**输入是什么**

SqlBuildPlan + SourceManifest + RelationshipHypothesis。

**输出是什么**

`(passed: bool, questions: list[OpenQuestion])`。passed=True 表示所有 blocking 检查通过。questions 中 blocking=True 的项标记阻断原因。

**出错会导致什么风险**

如果 Validator 漏检了一个假字段引用（如字段在 SourceManifest 中存在但类型不兼容的 Join key），Compiler 可能生成运行时失败的 SQL。如果 Validator 的 WEAK/NONE 门禁被绕过，不可靠 Join 直接进入 SQL——可能产生笛卡尔积或语义错误的结果。

**简单例子**

SqlBuildPlan 中 AggregateStep 引用 `ColumnRef("o.non_existent_column")` → Validator 查询 SourceManifest.tables["o"].columns → non_existent_column 未注册 → 生成 blocking OpenQuestion("Q-VAL-COL-xxx", "列 non_existent_column 未在源表 o 中注册") → passed=False。

**Owner 审查时应该问什么**

1. "Validator 的 8 项检查的执行顺序是否重要？如果第 3 项（字段引用）失败，第 7 项（时间过滤）还会执行吗？"
2. "Validator 对 WEAK/NONE Join 的处理：是直接 REJECT 还是生成 HUMAN_REVIEW？如果程序员确认了 WEAK Join，如何绕过门禁？"

---

## 9. SqlProgram

**一句话解释**：多语句 SQL 程序——用 DAG 依赖图 + 有序 SqlStatement 列表替代 CTE嵌套作用域。

**是什么**

SqlProgram 是系统的多语句容器，替代传统 SQL 的 CTE（WITH ... AS ...）。它包含一组 SqlStatement（每个 statement 有自己的 SqlBuildPlan）+ 一个依赖 DAG（dict[str, list[str]]，记录 statement_id 之间的依赖关系）+ 确定性拓扑排序。每个 statement 可以声明中间表（_temp 命名空间）。编译时按拓扑序依次执行每个 statement，_temp 表在 statement 间传递数据。

**解决什么问题**

CTE 引入嵌套作用域，破坏 SqlBuildPlan 的扁平可审查性。SqlProgram 用平面 DAG + _temp 表实现等价语义——每个 statement 独立可审查，依赖关系显式可检。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/sql_program.py:82` — SqlProgram 定义
- `src/tianshu_datadev/planning/sql_program.py:463` — SqlProgramBuilder（多语句组装）
- `src/tianshu_datadev/planning/sql_program.py:42` — StatementKind（TEMP/CREATE/INSERT）

**输入是什么**

一组 SqlBuildPlan（每个 statement 独立） + 依赖声明。

**输出是什么**

SqlProgram 实例：含 `program_id`、`statements: list[SqlStatement]`、`dag: dict[str, list[str]]`、`temp_tables: list[TempTableSpec]`、`topological_order: list[str]`。

**出错会导致什么风险**

如果 DAG 出现环，拓扑排序失败→程序无法编译。如果 _temp 表名冲突（两个 statement 声明了相同 _temp 名），后执行的会覆盖前的数据——可能导致下游 statement 读到错误数据。

**简单例子**

三表 Join（用户→订单→商品）拆分为 2 步 SqlProgram：
- Statement 1: u JOIN o → 写入 `_temp_step1_user_order`
- Statement 2: `_temp_step1_user_order` JOIN p → 最终写入 ads.target_table
- DAG: {"stmt_001": [], "stmt_002": ["stmt_001"]}
- 拓扑序: ["stmt_001", "stmt_002"]

**Owner 审查时应该问什么**

1. "SqlProgram 的 DAG 有环检测吗？如果 statements 中引入了循环依赖，会在哪一步报错？"
2. "_temp 表的生命周期如何管理？所有 _temp 是 session 级还是全局级？"

---

## 10. CTE → _temp 替代

**一句话解释**：CTE（Common Table Expression）永不实现——用 SqlProgram + _temp 中间表等价覆盖。

**是什么**

本项目在设计上放弃 CTE 语法（`WITH cte AS (...) SELECT ... FROM cte`），用 SqlProgram 的多语句 _temp 表方案替代。语义等价已证明：`WITH cte AS (SELECT ...) SELECT ... FROM cte` 等效于 `CREATE TEMP TABLE _temp_cte AS SELECT ...; SELECT ... FROM _temp_cte`。Validator 对任何 CTE 尝试返回 UNSUPPORTED_PLAN。

**解决什么问题**

CTE 引入嵌套作用域——内层 CTE 的列名、作用域与外层隔离，审查时需要在脑中展开嵌套。平面 _temp 表方案每步独立、可单独审查、DAG 依赖显式可见。

**在当前项目中的位置**

- `AGENTS.md:116` — 核心声明
- `docs/00-product-charter.md:118`
- `docs/01-target-architecture.md §3.3`
- `docs/02-reuse-and-migration-map.md:102`
- `docs/03-sql-ir-and-compiler-plan.md §3.3.2`

**输入是什么**

不适用——这是一个设计决策。

**输出是什么**

不适用。

**出错会导致什么风险**

如果未来开发者不了解此设计决策，试图在 Compiler 中支持 CTE——会破坏 SqlBuildPlan 的平面可审查性，引入嵌套作用域。

**简单例子**

传统 SQL（被禁止）：`WITH daily_orders AS (SELECT date, COUNT(*) AS cnt FROM orders GROUP BY date) SELECT * FROM daily_orders WHERE cnt > 100`

本项目等价方案（允许）：`CREATE TEMP TABLE _temp_daily_orders AS SELECT date, COUNT(*) AS cnt FROM orders GROUP BY date; SELECT * FROM _temp_daily_orders WHERE cnt > 100`

**Owner 审查时应该问什么**

1. "是否有任何场景是 _temp 方案不能等价覆盖 CTE 的？如递归 CTE？"
2. "如果新加入的团队成员试图在 Compiler 中实现 CTE 支持，代码审查时会在哪些位置被拦截？"

---

## 11. SafeIdentifier

**一句话解释**：SQL 标识符安全类型——只接受 ASCII 字母数字下划线组合，拒绝特殊字符和中文列名。

**是什么**

SafeIdentifier 是一个约束类型（Pydantic `Annotated[str, AfterValidator(...)]`），强制 SQL 标识符（表名、列名、别名）符合 `^[A-Za-z_][A-Za-z0-9_]*` 正则。这是 SQL 注入防护的第一道防线——任何不符合此模式的标识符都会被拒绝，包括中文列名（设计决定）。

**解决什么问题**

防止 SQL 注入通过表名/列名进入 SQL 文本。中文列名虽然在某些数据库合法，但在本项目中被明确拒绝——编译器不做 Unicode 标识符的引用/转义。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/models.py` — SafeIdentifier 定义
- 所有 ColumnRef、table_ref 字段均使用 SafeIdentifier

**输入是什么**

一个字符串标识符（表名、列名、别名）。

**输出是什么**

通过验证的字符串（与原字符串相同但类型标记为 SafeIdentifier）。

**出错会导致什么风险**

如果 SafeIdentifier 约束被绕过（如使用 str 类型替代 SafeIdentifier），`table_ref` 可能包含 `"; DROP TABLE xxx; --"` 等注入片段——这些会在 Compiler 渲染 SQL 时直接拼接进 SQL 文本。

**简单例子**

```python
# ✅ 合法
SafeIdentifier("user_id")       # → "user_id"
SafeIdentifier("_temp_step1")   # → "_temp_step1"

# ❌ 拒绝
SafeIdentifier("用户ID")         # → ValidationError（含中文字符）
SafeIdentifier("1table")        # → ValidationError（数字开头）
SafeIdentifier("user-id")       # → ValidationError（含连字符）
```

**Owner 审查时应该问什么**

1. "项目中是否存在任何不使用 SafeIdentifier 的 SQL 标识符字段？"
2. "如果数据库实际表名包含特殊字符（如 `order-detail`），如何在不破坏 SafeIdentifier 约束的前提下处理？"

---

## 12. DuckDbSqlCompiler

**一句话解释**：确定性 SQL 编译器——从 SqlBuildPlan/SqlProgram 生成 DuckDB 方言 SQL，不接受自由 SQL 字符串。

**是什么**

DuckDbSqlCompiler 是 sql_plan_to_sql() 的实现。它遍历 SqlBuildPlan 的 steps 列表，按步骤类型调用对应的渲染方法（`_render_scan()`、`_render_join()`、`_render_aggregate()` 等），生成完整的 DuckDB SQL 语句。关键约束：相同 SqlBuildPlan 重复编译必须产生字节一致的 SQL 和相同 SHA-256。Compiler 在渲染前运行 4 个优化 pass（列裁剪、谓词规范化、无用排序消除、常量折叠），优化必须是幂等的。

Phase 4D 扩展：
- **FILTER 子句支持**：`_render_metric_filter()` 将 MetricFilterDecl 渲染为 `FILTER (WHERE ...)` 子句——支持 8 种操作符（eq/neq/gt/gte/lt/lte/in/is_null/is_not_null），值统一加单引号
- **input_expression 支持**：`"quantity * unit_price"` 等多字段表达式直接透传为聚合输入
- **distinct 标志**：`SUM(DISTINCT col)` 渲染支持（distinct=True 时添加 DISTINCT 修饰）
- **聚合后投影重排**：`_reorder_aggregation_cols()` 在 AggregateStep 后跟 ProjectStep 时，按 ProjectStep 定义的列顺序重排聚合输出——不覆盖表达式本体，仅调整顺序
- **Join 右表去重**：FROM 子句自动排除已在 JOIN 子句中出现的右表，避免 `FROM a, b JOIN b` 的重复引用

**在当前项目中的位置**

- `src/tianshu_datadev/sql/compiler.py:61` — DuckDbSqlCompiler
- `src/tianshu_datadev/sql/compiler_backend.py:23` — CompilerBackend ABC（抽象接口）
- `src/tianshu_datadev/sql/compiler_passes.py` — 4 个优化 Pass
- `src/tianshu_datadev/sql/models.py:76` — CompiledSql 产出模型

**输入是什么**

SqlBuildPlan 或 SqlProgram。

**输出是什么**

CompiledSql（单语句）或 ProgramCompiledSql（多语句）。包含 compiled_sql: str、sql_hash: str、compiler_version: str、applied_passes: list[str]、optimized_plan（优化后的 SQL 计划）。

**出错会导致什么风险**

如果 Compiler 的渲染逻辑在相同输入下产生不同 SQL 文本（如 SQL 关键字大小写不一致、列顺序不确定），sql_hash 会变化——破坏可复现性和审计追踪。如果优化 Pass 不幂等（每次运行优化结果不同），同样破坏哈希稳定性。

**简单例子**

SqlBuildPlan(steps=[ScanStep(table_ref="o", required_columns=[ColumnRef("o.user_id"), ColumnRef("o.order_time")]), AggregateStep(group_keys=[...], agg_funcs=[AggregateSpec(aggregation=COUNT_DISTINCT, input_column="o.user_id", alias="dau")]), SortStep(sort_keys=[SortSpec(column=stat_date, direction=DESC)])]) → Compiler 渲染为 `SELECT DATE_TRUNC('day', o.order_time) AS stat_date, COUNT(DISTINCT o.user_id) AS dau FROM dwd.order_fact AS o WHERE o.order_time >= '2026-06-22' AND o.order_time < '2026-06-29' GROUP BY DATE_TRUNC('day', o.order_time) ORDER BY stat_date DESC`。

Phase 4D 示例——条件聚合：MetricDecl(filter=MetricFilterDecl(column="fine_status", operator="eq", value="STANDARD")) → 渲染为 `COUNT(DISTINCT plate_id) FILTER (WHERE fine_status = 'STANDARD') AS fined_count`。

**Owner 审查时应该问什么**

1. "如何验证'相同 SqlBuildPlan 重复编译产生相同 SQL 和 SHA-256'？有对应的测试吗？"
2. "Compiler 的 4 个优化 Pass 中，哪些是默认开启的？哪些可以关闭？"
3. "如果未来需要支持 Spark SQL 方言（如 `DATE_TRUNC` vs `TRUNC`），CompilerBackend 如何切换？"
4. "Phase 4D 的 FILTER 子句渲染中，value 统一加单引号——数字类型的过滤值（如 amount > 1000）会产生类型转换问题吗？"

---

## 13. CompilerBackend

**一句话解释**：SQL 编译器后端的抽象接口——当前实现 DuckDB，Phase 5+ 实现 Spark SQL。

**是什么**

CompilerBackend 是一个 ABC（抽象基类），定义 `compile(plan) -> CompilerOutput` 和 `dialect() -> str` 两个接口。当前唯一实现是 DuckDBBackend（封装 DuckDbSqlCompiler）。Phase 5+ 将实现 SparkSQLBackend，同一份 SqlBuildPlan 可编译为 DuckDB SQL 和 Spark SQL 两种方言。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/compiler_backend.py:23` — CompilerBackend ABC
- `src/tianshu_datadev/sql/compiler_backend.py:52` — DuckDBBackend

**输入是什么**

SqlBuildPlan 或 SqlProgram。

**输出是什么**

CompilerOutput（含 compiled_sql、dialect、compiler_version）。

**出错会导致什么风险**

如果两个后端的编译结果在语义上不一致（如 DuckDB 的 `DATE_TRUNC('day', ts)` vs Spark SQL 的 `TRUNC(ts, 'DD')`），跨引擎验证（Phase 7）可能误报 DIFFERENT。

**简单例子**

```python
backend = DuckDBBackend()
output = backend.compile(sql_build_plan)
assert output.dialect == "duckdb"
# Phase 5:
backend = SparkSQLBackend()
output = backend.compile(sql_build_plan)
assert output.dialect == "spark_sql"
```

**Owner 审查时应该问什么**

1. "CompilerBackend 的 dialect() 返回值是用于展示还是用于运行时路由？"
2. "DuckDBBackend 和未来 SparkSQLBackend 之间有哪些语法差异是 CompilerBackend 接口无法覆盖的？"

---

## 14. PerfValidator

**一句话解释**：确定性的性能门禁——硬规则阻断慢查询模式，软规则记录到 ExecutionTrace。

**是什么**

PerfValidator 执行确定性的查询性能规则检查。规则分为 HARD（违反→REJECT 阻断）和 SOFT（违反→WARN 记录到 ExecutionTrace）。Phase 1C 已实现 8 条规则，Phase 4B 规划扩展至 15 条。LLM 不参与性能决策——所有规则是确定性代码。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/perf_validator.py:188` — PerfValidator
- `src/tianshu_datadev/sql/models.py:174-248` — PerfRuleLevel/PerfSeverity/PerfRule/PerfCheckResult/PerfValidationResult

**输入是什么**

SqlBuildPlan + SourceManifest（含 row_count 估算）。

**输出是什么**

PerfValidationResult：含 checks: list[PerfCheckResult](每项的 rule_id、passed、severity、detail)。

**出错会导致什么风险**

如果 HARD 规则太弱（如大表无 LIMIT 产生 WARN 而非 REJECT），全表扫描可能进入执行阶段耗尽内存。如果 SOFT 规则太强（如把小表也强制要求时间过滤），可能过度拒绝合法查询。

**简单例子**

大事实表（row_count > 100万）的 ScanStep 没有 time_range 过滤 → PerfValidator 规则 "LARGE_TABLE_NO_TIME_FILTER" 触发 REJECT → 编译被阻断。

**Owner 审查时应该问什么**

1. "HARD 和 SOFT 规则的划分标准是什么？有没有可能某条规则一开始是 SOFT 后来升级为 HARD？"
2. "PerfValidator 的规则是否依赖 row_count 的准确性？如果 row_count 是估算值，不同的估算结果会导致不同判决吗？"

---

## 15. WriteValidator

**一句话解释**：写入方案的安全审查器——只允许日期分区 overwrite，拒绝全表/无分区/UPDATE/DELETE/MERGE。

**是什么**

WriteValidator 对 FinalWritePlan 执行 10 项安全检查，确保只允许受控的日期分区 overwrite 写入方案。最终写入方案作为审查材料输出（FinalWritePlan），不实际执行到生产数据库。拒绝操作包括：全表 overwrite、无分区 overwrite、UPDATE/DELETE/MERGE、INSERT INTO（非 overwrite）。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/write_validator.py:47` — WriteValidator（10 项安全检查）
- `src/tianshu_datadev/sql/write_plan.py:269` — FinalWritePlan 模型
- `src/tianshu_datadev/sql/write_plan_builder.py:26` — FinalWritePlanBuilder

**输入是什么**

SqlProgram + WritePlan 声明（target_table、partition_keys、overwrite_mode、partition_values）。

**输出是什么**

`(passed: bool, write_plan: FinalWritePlan | None, errors: list[WriteValidationCheck])`。

**出错会导致什么风险**

如果 WriteValidator 误允许全表 overwrite——覆盖生产表全部数据不可恢复。如果误允许 UPDATE/DELETE——生产表部分数据丢失且无审计记录。

**简单例子**

FinalWritePlan(target_table="ads.dau_daily", partition_keys=["stat_date"], overwrite_mode="partition", partition_values={"stat_date": "2026-06-29"}) → WriteValidator 确认：① 分区键存在 ② overwrite_mode="partition" ③ 非全表操作 → passed=True。

**Owner 审查时应该问什么**

1. "WriteValidator 允许的最大写入范围是什么？如果程序员要求在无分区的表上 overwrite，如何处理？"
2. "FinalWritePlan 作为审查材料——是纯文本描述还是结构化 JSON？人工审查者需要核对哪些内容？"

---

## 16. DuckDBExecutor

**一句话解释**：隔离环境中的 DuckDB 只读执行器——在冻结快照（CSV/Parquet）上运行编译后的 SQL。

**是什么**

DuckDBExecutor 接收 CompiledSql + 数据源路径映射（table_paths: dict[str, str]），在 DuckDB 内存数据库中注册表、执行 SQL、返回 DataFrame 和 ExecutionTrace。执行环境受隔离限制——不包含生产凭据、不连接生产数据库。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/executor.py:28` — DuckDBExecutor
- `src/tianshu_datadev/sql/models.py:124` — ExecutionTrace
- `src/tianshu_datadev/sql/models.py:146` — ResultSummary

**输入是什么**

CompiledSql + table_paths: dict[str, str]（表名→CSV/Parquet 文件路径）。

**输出是什么**

ExecutionTrace（含 status、row_count、elapsed_ms、error）+ ResultSummary（含 column_names、sample_rows(前 5 行)、shape）。

**出错会导致什么风险**

如果 table_paths 映射错误（如事实表和维表互换路径），执行成功但结果语义错误——Comparator 在后续阶段应能检测。如果 DuckDB 版本变化导致相同 SQL 相同数据不同结果（罕见但可能），破坏可复现性。

**简单例子**

CompiledSql(compiled_sql="SELECT ... FROM dwd.order_fact WHERE ...") + table_paths={"dwd.order_fact": "tests/fixtures/sql/test_fact.csv"} → Executor 在 DuckDB 中 `CREATE TABLE dwd.order_fact AS SELECT * FROM 'tests/fixtures/sql/test_fact.csv'`→执行 SQL→返回 100 行 DataFrame。

**Owner 审查时应该问什么**

1. "Executor 的超时限制是多少？内存限制是多少？谁设置这些限制？"
2. "如果执行 SQL 中包含 `CREATE TABLE AS`（_temp 表），Executor 如何处理？是允许还是拒绝？"

---

## 17. Pipeline（流水线编排器）

**一句话解释**：串联 Parser → Enricher → Builder → Validator → Compiler → Executor → Contract → Packager 八阶段，提供 execute() 和 run_all() 两个入口，失败时保留中间产物并返回 pipeline_error。

**是什么**

Pipeline 是系统的核心编排组件（类名 `Pipeline`，定义在 `src/tianshu_datadev/api/pipeline.py`）。它接收 DeveloperSpec 文本，按阶段依次调用各组件，返回结构化结果。在不接真实 LLM 的情况下，Planner 组件使用 Fake 实现（确定性规则代码替代 LLM 推理）。内部维护 `_results: dict[str, dict]` 和 `_timestamps: dict[str, float]` 作为请求级临时存储，带 TTL 过期自动清理（`_purge_expired()`）。

Pipeline 有两个核心入口方法：

- **`execute()`**（parser → enrich → build → compile → execute）：五阶段，产出 SQL 编译和执行结果。Validator 门禁内嵌在 build 阶段——blocking 问题直接阻断，不进入编译。
- **`run_all()`**（parser → enrich → build → validate → compile → execute → contract → package）：八阶段，在 execute 基础上追加 DataTransformContract 抽取和 ReviewPackage 打包。

两个方法内部都有**三条代码路径**，根据输入特征自动选择：
1. **ComputeSteps 路径**——spec 中声明了 compute_steps，走 `build_from_steps → build_sql_program_from_compute_steps → compile_program → execute_program`
2. **多跳链路径**——hypothesis 含多个候选 Join，走 `build_multi → build_sql_program_from_chain → compile_program → execute_program`
3. **单 SQL 路径**——标准场景，走 `build → compile → execute`

**统一的错误处理**：三条路径共享相同的错误出口——Validator blocking 时返回 `validation_passed=false` + 具体问题列表；SQL 执行失败（RUNTIME_FAIL）时返回 `pipeline_error` + `pipeline_stages` 阶段状态；异常捕获时保留已完成产物到 `_results` 供事后查询。所有响应（成功和失败）都包含 `validation_passed` 和 `open_questions` 字段，调用方无需猜测链路状态。

**在当前项目中的位置**

- `src/tianshu_datadev/api/pipeline.py` — Pipeline 类实现（~1400 行）
- `src/tianshu_datadev/api/routes.py` — REST API 路由调用 Pipeline
- `src/tianshu_datadev/api/models.py` — ExecuteResponse / RunAllResponse 响应模型

**输入是什么**

DeveloperSpec 文本 + optional table_mapping（别名→物理表名，传给 Compiler）+ optional table_paths（表名→CSV 路径，传给 Executor）。

**输出是什么**

取决于调用方法：parse_only()→SpecParseResponse、build_plan()→PlanResponse、execute()→ExecuteResponse（含 validation_passed + execution_trace + result_summary）、run_all()→RunAllResponse（含 validation_passed + contract_id + package_id + execution_trace + result_summary + open_questions）、get_package()→PackageResponse。失败响应均为 HTTP 200 + pipeline_error + pipeline_stages。

**出错会导致什么风险**

如果 Pipeline 的 _results 字典在不同 request_id 之间没有正确隔离（如 key 冲突），后一次请求可能读到前一次请求的 artifact。如果三条代码路径返回的字段集合不一致——调用方（前端/CLI）会因字段缺失而误判链路状态。当前已通过统一 Response 模型和每条路径显式返回所有字段来解决。

**简单例子**

```python
pipeline = Pipeline()
result = pipeline.execute(developer_spec_text)
# → ExecuteResponse(validation_passed=True, execution_trace={...}, result_summary={...})

result2 = pipeline.run_all(developer_spec_text)
# → RunAllResponse(validation_passed=True, contract_id="...", package_id="...", ...)
```

**Owner 审查时应该问什么**

1. "Pipeline 的三条代码路径（ComputeSteps / 多跳链 / 单 SQL）中，ComputeSteps 路径的返回值与其他两条路径的差异是什么？这些差异是否已记录在 RunAllResponse 模型中？"
2. "如果 Validator 在 build 阶段产生 blocking 问题——execute() 和 run_all() 返回的 response 中分别包含哪些字段？前端如何区分'校验失败'和'执行失败'？"

---

## 18. DataTransformContract（lite / V1）

**一句话解释**：从已验证 SqlBuildPlan/SqlProgram 确定性抽取的业务规格——SQL 和 Spark 的共同输入，不包含实现代码。

**是什么**

DataTransformContract 是业务口径的权威文档。它从已验证的 SqlBuildPlan（lite 版）或 SqlProgram（V1 版）中确定性抽取——包含输入表、字段、Join 关系、聚合定义、输出列规格，但不包含 SQL 代码和实现细节。分两级递进：
- **lite（Phase 2）**：从单个 SqlBuildPlan 抽取——输入表/字段、过滤条件、Join 关系、聚合定义、输出列和类型、排序、行限制
- **V1（Phase 3 Exit）**：从 SqlProgram 抽取——lite 全部 + step_dag + temp_tables + case_when_labels + window_specs + write_spec

**解决什么问题**

为 SQL 和 Spark 提供单一事实源——SparkDeveloper 只读 DataTransformContract 作为业务规格输入，不从 DeveloperSpec 重新推理业务逻辑。相同 SqlBuildPlan/SqlProgram 抽取相同 Contract 和相同哈希。

**在当前项目中的位置**

- `src/tianshu_datadev/artifacts/models.py:104` — DataTransformContractLite
- `src/tianshu_datadev/artifacts/models.py:182` — DataTransformContractV1
- `src/tianshu_datadev/artifacts/contract_extractor.py:41` — DataTransformContractExtractor

**输入是什么**

已验证的 SqlBuildPlan（lite）/ SqlProgram（V1）。

**输出是什么**

DataTransformContractLite 或 DataTransformContractV1 实例——纯业务规格，无 SQL 代码。

**出错会导致什么风险**

如果 Contract 抽取非确定性——相同 SqlBuildPlan 产生不同 Contract→Spark 侧基于不同规格生成代码→跨引擎验证失败。如果 Contract 漏抽了关键字段——SparkDeveloper 基于不完整规格生成代码→产出 DataFrame 列不匹配。

**简单例子**

SqlProgram（2 步三表 Join）→ extract_v1() → DataTransformContractV1(contract_id="...", source_sqlprogram_hash="...", step_dag={"stmt_001": [], "stmt_002": ["stmt_001"]}, temp_tables=[TempTableSpec(...)], window_specs=[], write_spec=FinalWritePlan(...))。

**Owner 审查时应该问什么**

1. "Contract 的 hash 是否只依赖 SqlBuildPlan 的结构字段？还是也包括 step_id 这种非语义字段？"
2. "如果 SparkDeveloper 使用的 Contract 版本与 SQL Compiler 使用的 SqlBuildPlan 版本不一致，会发生什么？"

---

## 19. ReviewPackage

**一句话解释**：Code Review 的完整材料包——包含事实源、代码、Prompt、模型、快照、环境和 Comparator 版本哈希。

**是什么**

ReviewPackage 是提交给人工审查者的完整材料。它不是简单的"代码 + 测试"，而是包含：DeveloperSpec 原文、ParsedDeveloperSpec、SourceManifest、SqlBuildPlan/SqlProgram、编译后的 SQL、ExecutionTrace、DataTransformContract、以及所有涉及的事实源哈希、编译器版本、快照信息。ReviewPackageManifest 是清单索引。

**在当前项目中的位置**

- `src/tianshu_datadev/artifacts/models.py:279` — ReviewPackageManifest
- `src/tianshu_datadev/artifacts/models.py:303` — HumanReviewItem
- `src/tianshu_datadev/artifacts/models.py:336` — ReviewFeedback
- `src/tianshu_datadev/artifacts/packager.py:36` — ReviewPackageBuilder

**输入是什么**

PackageInputs：含 developer_spec、parsed_spec、source_manifest、sql_plan、compiled_sql、execution_trace、contract。

**输出是什么**

ReviewPackageManifest：含 package_id、所有输入 artifact 的哈希、status、created_at。

**出错会导致什么风险**

如果 ReviewPackage 缺少关键哈希（如 source_manifest_hash），人工审查者无法验证代码是否基于正确的数据源生成。如果 ReviewFeedback 的 target 字段路由错误——REQUIREMENT 问题被路由到 COMPILER_BUG——返工入口错误导致问题无法修复。

**简单例子**

Pipeline.run_all() → 自动打包 → ReviewPackageManifest(package_id="pkg_xxx", status="READY_FOR_REVIEW", artifact_refs=[ArtifactRef(ref_type="parsed_spec", hash="..."), ArtifactRef(ref_type="sql_artifact", hash="..."), ...])。

**Owner 审查时应该问什么**

1. "ReviewPackageManifest 中哪些字段是人工审查者必须核对的？哪些是机器自动填充仅供参考的？"
2. "ReviewFeedback 的 target 字段有哪 5 个合法值？每个 target 对应的返工入口是什么？"

---

## 20. Repair Boundary（返工边界）

**一句话解释**：最多 2 轮自动返工——每次返工必须经过完整的 Validator/Executor/Comparator 链，超过轮次进入 HUMAN_REVIEW。

**是什么**

Repair Boundary 定义了代码修复的自动化边界：当 DifferenceAnalyst 发现 SQL/Spark 差异时，RepairPlanner 输出修复指令（SQL_PLAN / SPARK_CODE / BOTH / REQUIREMENT / HUMAN_REVIEW），目标组件重新生成产物，重新通过 Validator/Executor/Comparator。最多 2 轮自动返工，UNKNOWN、事实源缺失、需求变化或超过轮次进入 HUMAN_REVIEW。

**解决什么问题**

防止自动化修复系统陷入无限循环或在不具备足够信息时强行修复。强制结构化 ReviewFeedback 而不是"用 Memory 记录上次改了什么"。

**在当前项目中的位置**

- `AGENTS.md §6` — Repair Boundary 完整定义
- `src/tianshu_datadev/ir/protocols.py:61` — RepairTarget（REQUIREMENT/SQL_PLAN/COMPILER_BUG/SOURCE_FACT/HUMAN_REVIEW）
- `src/tianshu_datadev/artifacts/models.py:336` — ReviewFeedback

**输入是什么**

Comparator 产生的差异报告 + 当前轮次的 artifact 引用。

**输出是什么**

RepairDirective：含 target（路由） + suggested_resolution（建议修复方案） + retry_count。

**出错会导致什么风险**

如果返工轮次计数器被重置（如 Agent 重启丢失状态），可能超出 2 轮限制。如果 ReviewFeedback 的 target=HUMAN_REVIEW 但 Agent 仍继续返工——越过人工审查直接修改代码。

**简单例子**

第一轮 SQL 执行成功但结果行数错误 → Comparator 返回 DIFFERENT → DifferenceAnalyst 分析 → RepairPlanner 输出 target=SQL_PLAN → SqlBuildPlanBuilder 生成新版 SqlBuildPlan（retry_count=1）→ 重新 Validator/Compiler/Executor → 通过 → 完成。

**Owner 审查时应该问什么**

1. "retry_count 计数器存储在哪里？Agent 重启后计数会重置吗？"
2. "如果不问原因直接删除 ReviewFeedback 字段，返工链路还能正常工作吗？"

---

## 21. OpenQuestion

**一句话解释**：验证过程中发现的需要程序员确认的问题——blocking 问题阻断编译，非 blocking 作为警告。

**是什么**

OpenQuestion 是 Validator/Parser/Planner 在不确定时向程序员提出的结构化问题。包含 question_id、question 文本、blocking: bool（True=阻断编译）、context 字典（相关字段/表/证据）。程序员通过 HumanResolution 回答 OpenQuestion。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py:339` — OpenQuestion 定义
- `src/tianshu_datadev/developer_spec/models.py:330` — HumanResolution 定义

**输入是什么**

发现不确定情况时由 Validator/Parser/Planner 生成。

**输出是什么**

传递给程序员的结构化问题——如果 blocking=True，编译暂停直到程序员提供 HumanResolution。

**出错会导致什么风险**

如果 blocking 问题被当作非 blocking 跳过——编译在信息不足时继续，可能生成错误 SQL。如果 OpenQuestion 的 context 不完整——程序员无法理解决策困境，回答错误导致后续链路偏差。

**简单例子**

Validator 发现 Join 的两个字段类型不完全匹配：`u.user_id: bigint` vs `o.user_id: integer` → 生成 OpenQuestion("Q-VAL-JOIN-xxx", "Join 键 u.user_id(bigint) 与 o.user_id(integer) 类型不完全一致——是否确认兼容？", blocking=True, context={"left_col": "u.user_id", "left_type": "bigint", "right_col": "o.user_id", "right_type": "integer"})。

**Owner 审查时应该问什么**

1. "OpenQuestion 的 blocking 判定逻辑是确定性的还是依赖 LLM？如果是后者，如何保证一致性？"
2. "如果一个 blocking OpenQuestion 在 10 分钟内没被回答——系统是等待、超时还是降级？"

---

## 22. SQL Generation Boundary（SQL 生成边界）

**一句话解释**：核心安全红线——LLM 只输出结构化计划，SQL 只能由确定性 Compiler 生成，禁止自由 SQL 片段。

**是什么**

SQL Generation Boundary 是项目最重要的架构约束，在 AGENTS.md §2 中定义。它规定了 8 条硬性规则：① LLM 只输出 ParsedDeveloperSpec/RelationshipHypothesis/SqlBuildPlan/SqlProgram ② SqlBuildPlan 使用封闭 Step 类型 ③ 禁止 raw_sql/where_sql/join_on: str/expression: str ④ SQL 只能由 Python 确定性编译器生成 ⑤ SQL 修复只能生成新 SqlBuildPlan ⑥ 表字段和 Join 必须来自 SourceManifest ⑦ 不支持表达式必须拒绝或 HUMAN_REVIEW ⑧ 性能门禁由 PerfValidator 执行。

**解决什么问题**

防止 LLM"幻觉"出 SQL 代码被执行——LLM 在生成 SQL 方面能力很强但也极不稳定。将 LLM 的能力限制在"结构化理解"，将 SQL 生成交给确定性规则代码。

**在当前项目中的位置**

- `AGENTS.md §2` — 完整边界定义
- 所有 Step 类不包含字符串 SQL 字段
- 所有 Compiler 不接受 SQL 字符串输入

**输入是什么**

不适用——这是一个架构约束。

**输出是什么**

不适用。

**出错会导致什么风险**

如果在任意一个 Step 类中添加 `raw_sql: str` 字段——LLM 可以将任意 SQL 注入该字段，绕过所有类型安全和引用校验。如果 Compiler 接受 SQL 字符串片段作为输入——丧失了"相同输入→相同输出"的确定性保证。

**简单例子**

- ✅ 合法：`FilterStep(step_id="f1", predicate=Predicate(column=ColumnRef("o.amount"), operator=PredicateOperator.GT, value=SqlLiteral("0")))`
- ❌ 非法：`FilterStep(step_id="f1", where_sql="o.amount > 0 AND o.status = 'valid'")` ← 即使能跑也必须拒绝

**Owner 审查时应该问什么**

1. "在项目的任何 .py 文件中，搜 `raw_sql`、`where_sql`、`join_on`、`expression: str`——结果为 0 吗？"
2. "如果 LLM Gateway 返回的 JSON 中包含一个 `sql` 字段，Gateway 会如何处理？"

---

## 23. Validation Boundary（验证边界）

**一句话解释**：LLM 不能决定验证通过——所有验证由确定性 Comparator/Validator 执行，产出一个精细状态（非简单 PASS/FAIL）。

**是什么**

Validation Boundary 规定了 7 个精细验证状态：NOT_EXECUTED、RUNTIME_PASS、DIFFERENT、UNSUPPORTED_SEMANTICS、CONSISTENT_SAMPLE、REVIEW_READY、HUMAN_REVIEW。禁止使用泛化 PASS 表示业务正确、全量一致、生产性能或上线批准。

**在当前项目中的位置**

- `AGENTS.md §5` — Validation Boundary 定义
- 各 Validator/Comparator 组件产出这些状态

**输入是什么**

编译产物 + 执行结果 + 预期业务规格。

**输出是什么**

7 个精细状态之一——具体状态而非泛化 PASS/FAIL。

**出错会导致什么风险**

如果使用泛化 PASS 表示"一切 OK"——遗漏了采样不足、快照过期、语义不支持等隐藏问题。如果 DIFFERENT 被降级为 WARN——差异被掩盖，跨引擎验证形同虚设。

**简单例子**

SQL 在 DuckDB 执行成功但 Spark 端不支持相同语法 → Comparator 产出 UNSUPPORTED_SEMANTICS（而非 FAIL）→ 表明语义等价无法在当前兼容策略下证明。

**Owner 审查时应该问什么**

1. "什么条件下系统会产出 CONSample_CONSISTENT 而非 PASS？两者在人工审查流程中的区别是什么？"
2. "这 7 个状态中哪些是终端状态（不再自动返工），哪些会触发 Repair？"

---

## 24. HarnessReport

**一句话解释**：Phase 退出门禁的结构化评测报告——含多维度评测和 GO/NO_GO 总判决。

**是什么**

HarnessReport 是 Phase 门禁的标准化评测输出。它包含 phase 标识、dimensions: list[DimensionResult]（每维度独立判决 PASS/REJECT/WARN/INFO + 指标 + 详情）、overall_verdict: HarnessVerdict（GO/NO_GO）。Phase 3 Exit Report（`phase-3-exit-report.md`）是第一个实例——5 维度评测全部 PASS/INFO，总判决 GO。

**在当前项目中的位置**

- `src/tianshu_datadev/harness/models.py:201` — HarnessReport 定义
- `src/tianshu_datadev/harness/models.py:185` — DimensionResult
- `src/tianshu_datadev/harness/models.py:178` — HarnessVerdict（GO/NO_GO）
- `scripts/phase3_exit_eval.py` — Phase 3 Exit HarnessReport 生成脚本
- `docs/roadmap/phase-3-exit-report.md` — Phase 3 Exit 报告（归档）

**输入是什么**

评测脚本 + 被评测的代码基线。

**输出是什么**

HarnessReport 实例（可序列化为 JSON）+ Markdown 归档报告。

**出错会导致什么风险**

如果 HarnessReport 的维度判决作假（如 Parser 实际只能解析 60% 但报告写 100%），Phase 门禁形同虚设——后续 Phase 在不可靠的基础上继续建造。如果 GO/NO_GO 判定未覆盖全部基线维度（如漏检 CTE 拒绝是否生效），关键边界问题被带进下一 Phase。

**简单例子**

Phase 3 Exit HarnessReport(phase="phase-3-exit", overall_verdict=GO, dimensions=[DimensionResult(dimension=1, name="Schema 可生成性基线", verdict=PASS, metrics={"parse_pass_rate": 100.0}), ...])。

**Owner 审查时应该问什么**

1. "HarnessReport 的 GO/NO_GO 判定是否可以有人工覆盖？如果可以，覆盖记录在哪里？"
2. "下一个 Phase（4A）的 HarnessReport 会用同一套脚本生成吗？还是每个 Phase 有独立的评测脚本？"

---

## 25. Harness（评测框架）

**一句话解释**：独立于产品运行时的评测体系——覆盖安全、语义、回归、Prompt 评测，pytest 是执行载体。

**是什么**

Harness 是评测框架的总称，包含：
- **SecurityEvaluator**：安全攻击向量测试（SQL 注入、旁路注入、自由 SQL 检测）
- **SemanticEvaluator**：语义错误类型测试（错粒度、错聚合、错 Join）
- **HarnessRunner**：编排各 Evaluator，生成 HarnessReport
- **HarnessMetricsEngine**：指标计算引擎
- **DatasetLoader**：加载 `harness/datasets/` 下的评测数据集

Harness 不得成为产品运行时依赖——仅用于开发和 CI 评测。

**在当前项目中的位置**

- `src/tianshu_datadev/harness/` — 完整评测框架
  - `models.py` — HarnessReport/DimensionResult/SecurityCase/SemanticCase
  - `eval_runner.py` — HarnessRunner
  - `metrics.py` — HarnessMetricsEngine
  - `security_eval.py` — SecurityEvaluator
  - `semantic_eval.py` — SemanticEvaluator
  - `dataset_loader.py` — DatasetLoader
- `harness/datasets/` — 评测数据集（attack/sql_injection.json 等）

**输入是什么**

评测数据集 + 被评测的组件（Parser/Planner/Validator/Compiler）。

**输出是什么**

HarnessReport（可作为 Phase 门禁依据）。

**出错会导致什么风险**

如果 Harness 评测脚本依赖运行时数据库——CI 环境可能无法运行，评测变成"某台机器上才能跑"。如果评测数据集覆盖不全——某些高风险场景（如间接 SQL 注入）未被测试，产生假安全。

**简单例子**

SecurityEvaluator 加载 `harness/datasets/attack/sql_injection.json` → 对每个 case 构造含注入的 DeveloperSpec → 验证 Parser/Validator/Compiler 是否正拒绝或转义 → 生成 SecurityCaseResult → 汇总为 SecurityEvalReport。

**Owner 审查时应该问什么**

1. "Harness 评测是每次 commit 都跑，还是只在 Phase 退出时跑？"
2. "安全评测数据集（sql_injection.json）中有多少个 case？覆盖了哪些注入向量？"

---

## 26. 黄金用例（Golden Fixture）与回归样本

**一句话解释**：预期应通过的固定测试案例——记录输入 DeveloperSpec 和预期行为，用于每次变更后检测行为漂移。

**是什么**

Golden Fixture 是 `tests/fixtures/golden/` 目录下的 6 个 DeveloperSpec 文件，每个对应 Parser 的一种"允许宽松"场景：无时间范围、无输出排序、无显式 Join、类型从 Registry 推断、额外 Markdown 文本、中文列注释。它们被 `test_pipeline_e2e.py` 等 E2E 测试引用，验证完整流水线的正确性。

回归样本是 `harness/datasets/regression/` 和 `prompts/templates/**/regression_cases.jsonl` 中的案例——记录此前发现并修复的错误，每次变更必须重跑。

**在当前项目中的位置**

- `tests/fixtures/golden/` — 6 个 Golden Fixture
- `tests/fixtures/reject/` — 6 个 Reject Fixture
- `harness/datasets/regression/` — 回归数据集（待补充）
- `prompts/templates/**/regression_cases.jsonl` — Prompt 回归案例（待创建）

**输入是什么**

无——fixture 是静态 Markdown/YAML/JSON 文件。

**输出是什么**

测试运行时产出 PASS/FAIL——fixture 的预期行为与当前系统实际行为的比对结果。

**出错会导致什么风险**

Fixture 覆盖不全会导致关键场景未被测试——Prompt 修改后某些拒绝路径失效而未被发现。Fixture 过期（预期值本身已不符合当前安全策略）产生假 PASS——给变更开绿灯但实际已经过时。

**简单例子**

`golden_no_time_range.md`（Parser 宽松1：无时间范围）→ Parser 应正常生成 ParsedDeveloperSpec + 添加 W002 parse_warning → Planner → Validator → Compiler 应正常生成 SQL 但 PerfValidator 可能触发时间过滤缺失警告。

**Owner 审查时应该问什么**

1. "当前 6 个 golden fixture 中，哪几个需要完整 E2E 链路？哪几个是纯 Parser 级别的？"
2. "如果新增一种 SQL 模式（如子查询），需要增加几个 golden fixture？至少几个 reject fixture？"

---

## 27. Gate（门禁体系）

**一句话解释**：每个 Phase 的退出/准入检查点——控制下一 Phase 能否启动的决策依据。

**是什么**

Gate（门禁）是 Phase 间的前进控制机制。当前项目有：
- **Phase 门禁**：每个 Phase 的"退出条件"表格（如 Phase 3C 的 6 条退出条件、Phase 4A 的 5 条）
- **Harness 门禁**：Phase 4D 规划的七维门禁（安全/语义/性能/契约/回归/合规/可追溯）
- **Join 门禁**：WEAK/NONE 硬门禁——任何时候不进入 SqlBuildPlan
- **B/C 暂停条件**：每个 Phase 的"停止继续往下做"的条件

**解决什么问题**

防止在不满足前置条件时推进——比如 Phase 4A 的 HarnessReport 缺失时不能真正开始 LLM 集成。

**在当前项目中的位置**

- 各 `docs/roadmap/phase-*.md` — 退出条件表格 + B/C 暂停条件
- `AGENTS.md §5` — Validation Boundary 状态机
- `src/tianshu_datadev/planning/relationship_validator.py` — Join 门禁

**输入是什么**

当前 Phase 的交付物 + HarnessReport。

**输出是什么**

GO/NO_GO 判决——GO = 下一 Phase 可启动，NO_GO = 必须补齐缺失项。

**出错会导致什么风险**

如果门禁流于形式（如退出条件都标 ✅ 但实际未验证）——后续 Phase 在不稳定的基础上建造，返工成本指数增长。如果门禁过于宽松（如 HarnessReport 的 GO 没有要求所有维度 PASS）——关键边界问题被带进下一 Phase。

**简单例子**

Phase 3C → Phase 4A 门禁：5/6 退出条件满足，缺 HarnessReport → 门禁状态为 "⚠️ 阻塞" → 补齐后变为 ✅ → Phase 4A 启动。

**Owner 审查时应该问什么**

1. "每个 Phase 的退出条件是谁核销的？核销记录保存在哪里？"
2. "如果某个退出条件被标记为 ✅，但实际交付物后来被删除——门禁会自动降级为 ⚠️ 吗？"

---

## 28. Phase 3 Exit（Phase 3 退出）

**一句话解释**：SQL-first v1.0 的核心交付里程碑——Schema 可生成性 + Contract v1 + SqlProgram + 边界清单 + 测试基线。

**是什么**

Phase 3 Exit 是项目的一个重要里程碑，标志着 SQL-first 流水线的核心能力已就绪。它包含 5 项基线评测：
1. Schema 可生成性基线（6/6 golden fixture 解析通过）
2. DataTransformContract v1 覆盖度（5/5 v1 专属字段 + extract_v1()）
3. SqlProgram + _temp 多语句 Compiler 覆盖率
4. 已知不支持的 SQL 模式清单（5 项文档化）
5. Phase 4 硬化的输入基线（1123 测试、已知缺口）

HarnessReport(phase="phase-3-exit") 记录这些基线，并作为 Phase 4A 的前置依赖。

**在当前项目中的位置**

- `scripts/phase3_exit_eval.py` — 评测脚本
- `docs/roadmap/phase-3-exit-report.md` — Markdown 归档报告
- `docs/roadmap/phase-3-exit-report.json` — JSON 结构化报告

**输入是什么**

代码基线 + 测试基线。

**输出是什么**

HarnessReport + Markdown 报告。

**出错会导致什么风险**

如果 Phase 3 Exit 基线不准确（如溢报了未实现的功能为"已完成"），Phase 4 的硬化工作量和难度将被低估。

**简单例子**

Phase 3 Exit Report D4（不支持模式清单）记录了 5 项边界：CTE（永不实现）、子查询（Phase 4+开放，需 7 项成套规则）、多跳 Join（同上）、窗口+子查询组合（Phase 3B 禁止）、DDL/DML（FinalWritePlan 受控替代）。

**Owner 审查时应该问什么**

1. "Phase 3 Exit 之后，哪些能力是'完成'、哪些是'已知不支持'、哪些是'Phase 4+ 规划中'？"
2. "Phase 3 Exit 的 5 项基线评测是在什么环境下运行的？不同环境重跑结果一致吗？"

---

## 29. LLM Gateway

**一句话解释**：LLM 调用的统一入口——提交请求→加载 Prompt→调用 Adapter→Schema 校验→返回 LlmResponse。

**是什么**

LLMGateway 是 LLM 调用的唯一入口。它接收 LlmRequest（含 task、prompt_version、schema_name、input_artifact_refs），经过：加载 Prompt 模板 → 构建 messages → 调用 ProviderAdapter → 解析 LLM 原始响应 → Pydantic Schema 校验 → 返回 LlmResponse（含 validation_status、parsed_json_ref、token_usage、latency_ms）。Gateway 只返回结构化对象引用和校验状态。validation_status="invalid" 的响应进入拒绝路径或重试策略，不得降级为自由 SQL。

**在当前项目中的位置**

- `src/tianshu_datadev/llm/gateway.py:31` — LLMGateway
- `src/tianshu_datadev/llm/models.py:31-70` — LlmRequest/LlmResponse
- `src/tianshu_datadev/llm/adapters/base.py:32` — ProviderAdapter ABC
- `src/tianshu_datadev/llm/adapters/fake_adapter.py:22` — FakeLLMAdapter

**输入是什么**

LlmRequest：含 task（任务类型）、prompt_version、schema_name、input_artifact_refs、temperature、model。

**输出是什么**

LlmResponse：含 validation_status（valid/invalid）、parsed_json_ref、validation_errors、token_usage、latency_ms。

**出错会导致什么风险**

如果 Gateway 在 validation_status="invalid" 时不返回 parsed_json_ref=None（而返回 LLM 原始 JSON）。如果 FakeLLMAdapter 的确定性不保证相同输入→相同输出——测试不可复现。

**简单例子**

```python
gateway = LLMGateway(prompt_manager, adapter=FakeLLMAdapter())
response = gateway.submit(LlmRequest(
    task="parse_developer_spec",
    prompt_version="v001",
    schema_name="ParsedDeveloperSpec",
    input_artifact_refs=[ArtifactRef(ref_type="developer_spec", ref_id="...")]
))
assert response.validation_status == "valid"
```

**Owner 审查时应该问什么**

1. "如果 Gateway 调用真实 LLM 且 Schema 校验失败——Gateway 会重试几次？重试策略是什么？"
2. "FakeLLMAdapter 是如何保证'确定性'的？是硬编码返回值还是基于输入 hash 生成？"

---

## 30. PromptManager / PromptTemplate

**一句话解释**：Prompt 模板的版本管理——每个 Prompt 模板绑定版本号、目标 Schema 和回归案例集。

**是什么**

PromptManager 管理 `prompts/templates/` 目录下的 Prompt 模板文件。每个模板包含：角色定义、任务说明、输入格式、输出 JSON Schema 引用、禁止行为、示例。模板绑定版本号（v001/v002...）和目标 Pydantic Schema。Prompt 升级必须跑回归集并输出版本对比报告。

**在当前项目中的位置**

- `src/tianshu_datadev/prompts/manager.py:58` — PromptManager
- `src/tianshu_datadev/prompts/manager.py:41` — PromptTemplate（StrictModel）
- `prompts/templates/` — 4 个模板文件（developer_spec_parser / relationship_planner / sql_build_planner / sql_program_planner）

**输入是什么**

Prompt 模板加载时：任务名 + 版本号 → 从文件系统加载 Markdown 模板。

**输出是什么**

PromptTemplate 实例：含模板文本、目标 Schema 名、版本号、回归案例路径。

**出错会导致什么风险**

如果 Prompt 模板与目标 Schema 不同步（如 Schema 新增字段但 Prompt 模板未更新）——LLM 可能不输出该字段或输出错误格式，导致 Schema 校验失败率上升。如果 Prompt 升级没有跑回归集——已知 fixed 的问题可能重新出现。

**简单例子**

```python
manager = PromptManager()
template = manager.load("developer_spec_parser", "v001")
# template.prompt_text → Markdown 文本
# template.target_schema → "ParsedDeveloperSpec"
# template.regression_path → "prompts/templates/developer_spec_parser/regression_cases.jsonl"
```

**Owner 审查时应该问什么**

1. "Prompt 模板中'禁止输出可执行 SQL'这句话出现了几次？是否在所有的 planner prompt 中都包含了？"
2. "如果修改 Prompt 模板后不跑回归集——测试能否发现？"

---

## 31. FakeLLMAdapter

**一句话解释**：LLM 的确定性模拟适配器——返回硬编码的结构化输出，用于测试和离线验证。

**是什么**

FakeLLMAdapter 实现了 ProviderAdapter 接口，但不调用任何真实 LLM。它根据 task 和 schema_name 返回预定义的结构化 JSON 响应——保证相同输入→相同输出（确定性）。这使得测试不依赖外部服务和网络，且可复现。

**在当前项目中的位置**

- `src/tianshu_datadev/llm/adapters/fake_adapter.py:22` — FakeLLMAdapter
- `src/tianshu_datadev/llm/adapters/base.py:32` — ProviderAdapter ABC

**输入是什么**

prompt_messages（list[dict]）+ model + temperature。

**输出是什么**

原始 JSON 字符串（模拟 LLM 返回值）——后续由 Gateway 进行 Schema 校验和解析。

**出错会导致什么风险**

如果 FakeLLMAdapter 返回的 JSON 不符合目标 Schema——测试中的 Schema 校验会失败，但这是 Fake 数据错误而非 Schema 错误——需要区分。如果 FakeLLMAdapter 的返回值在不同测试用例之间共享可变状态——前一个测试可能影响后一个测试的输出。

**简单例子**

```python
adapter = FakeLLMAdapter()
response = adapter.call([
    {"role": "system", "content": "..."},
    {"role": "user", "content": "Parse this DeveloperSpec: ..."}
])
# response → '{"spec_type": "aggregate_table", "target_table": "ads.dau_daily", ...}'
```

**Owner 审查时应该问什么**

1. "FakeLLMAdapter 返回的 JSON 是固定的还是基于输入动态生成的？如果是固定的——能覆盖多少个不同的 DeveloperSpec 变体？"
2. "如果真实 LLM 的行为发生变化（如新增了输出字段），FakeLLMAdapter 应该同步更新吗？谁负责？"

---

## 32. 子查询与多跳 Join 边界

**一句话解释**：Phase 1-3 不支持，Phase 4+ 按黄金用例逐项开放——每个模式须满足 7 项成套交付规则后单独放行。

**是什么**

子查询和多跳 Join 是 Phase 3 Exit Report D4 中记录的 5 项不支持模式之一。两者的工程路径不同：
- **多跳 Join**（3+ 表关联）：现有 JoinStep 串联 + SqlProgram 多步 + _temp 传递——平面 DAG，工程难度低
- **子查询**（FROM 派生表）：需新增 SubqueryStep + 递归 SqlBuildPlan 嵌套 + 递归 Validator + 递归 Compiler——嵌套作用域，工程难度高

两者开放须满足 7 项规则：新增 Schema + Validator + Compiler + Safety + 测试 + 拒绝路径 + Artifact。每项规则已具象化为 checklist（详见补充文档）。

**在当前项目中的位置**

- `docs/roadmap/subquery-multihop-join-boundary_20260629_1500.md` — 边界补充文档（454 行）
- `docs/03-sql-ir-and-compiler-plan.md §7.3` — 7 项开放规则（原始抽象版）
- `docs/roadmap/phase-3-exit-report.md` D4 — 原始声明

**输入是什么**

不适用——当前为规划阶段。实施时需：黄金用例 DeveloperSpec + 新增 Schema 定义。

**输出是什么**

不适用。

**出错会导致什么风险**

如果在 Validator 未更新时开放子查询——子查询内的字段引用绕过事实源校验（因为当前 Validator 不知道 SubqueryStep 的存在）。如果在 Compiler 未添加深度检查时开放嵌套子查询——深层嵌套的 SQL 可能包含不可审查的复杂逻辑。

**简单例子**

当前边界：DeveloperSpec 声明 3 个表的关联 → Validator 在 SqlBuildPlan 中发现只有 1 个 JoinStep（两表）→ 第三个表引用未被注册 → 拒绝（但拒绝原因是"表未注册"而非"多跳 Join 不支持"——拒绝信息不准确）。

规划中（Phase 4F Step 1）：DeveloperSpec 声明 3 个表 → SqlProgram 生成 2 步——每步 1 个 JoinStep → Validator 分别校验每步的事实源引用→通过。

**Owner 审查时应该问什么**

1. "子查询和多跳 Join——哪一个先开？为什么？依据是什么？"
2. "如果程序员在 DeveloperSpec 中写了一个合法的子查询需求但当前不支持——系统给程序员的信息是什么？是'不支持子查询'还是'不支持 XXX 表'？"

---

## 33. JoinEvidenceLevel

**一句话解释**：Join 关系的 4 级置信度——STRONG/MEDIUM/WEAK/NONE，WEAK 和 NONE 是硬门禁，任何时候不得进入 SqlBuildPlan。

**是什么**

JoinEvidenceLevel 是 RelationshipHypothesis 对每对 Join 关系的置信度分级：
- **STRONG**：程序员显式声明了所有 join_keys 且双方字段类型兼容
- **MEDIUM**：至少一个 join_key 来自自动推断（列名匹配），但类型兼容
- **WEAK**：证据不充分——只有列名相似或模糊匹配，需程序员确认
- **NONE**：无任何证据——未找到两表之间的关联字段

STRONG 和 MEDIUM 可通过 Join 门禁进入 SqlBuildPlan（MEDIUM 需人工确认），WEAK 和 NONE 在任何情况下不得进入。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/relationship_hypothesis.py:21` — JoinEvidenceLevel 枚举
- `src/tianshu_datadev/planning/relationship_validator.py` — 确定性定级逻辑
- `AGENTS.md:23` — "WEAK/NONE 硬门禁"

**输入是什么**

JoinCandidate 的证据列表 + SourceManifest 类型信息。

**输出是什么**

JoinEvidenceLevel 枚举值 + 定级理由（evidence 列表）。

**出错会导致什么风险**

如果 WEAK Join 被错误定级为 MEDIUM 并进入 SqlBuildPlan——Join 键字段类型可能不兼容，运行时失败或产生语义错误。如果 STRONG Join 被降级为 MEDIUM——触发不必要的人工审查，影响自动化率。

**简单例子**

```python
# STRONG 证据
RelationshipEvidence(kind="EXPLICIT_JOIN_KEY", detail="程序员声明 join_keys=[[u.user_id, o.user_id]]", strength=1.0)
RelationshipEvidence(kind="TYPE_MATCH", detail="双方字段类型均为 bigint", strength=1.0)
# → 综合定级 STRONG → 直接进入 SqlBuildPlan

# WEAK 证据
RelationshipEvidence(kind="COLUMN_NAME_GUESS", detail="user_id 在两个表中都存在但未声明显式 join_keys", strength=0.3)
# → 综合定级 WEAK → 被 Join 门禁拦截
```

**Owner 审查时应该问什么**

1. "STRONG 和 MEDIUM 的阈值在代码中是硬编码的吗？可以调整吗？"
2. "如果同一个 Join 关系有多个 evidence，其中一些指向 MEDIUM、一些指向 STRONG——最终定级是什么？规则是什么？"

---

## 34. WindowStep（窗口函数步骤）

**一句话解释**：窗口函数的类型化表达——支持 ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD + FrameSpec，禁止任意外部函数名。

**是什么**

WindowStep 是 SqlBuildPlan 的第 7 种 Step 类型（Phase 3B 新增）。它通过 WindowExpr 表达窗口计算：function（白名单枚举：ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD/SUM/AVG/COUNT）、partition_by、order_by、frame（ROWS/RANGE/GROUPS）。禁止：任意函数名、嵌套窗口函数、窗口函数出现在 WHERE 子句、窗口函数内自由表达式、窗口函数与子查询组合。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/sql_build_plan.py:120` — WindowStep 定义
- `src/tianshu_datadev/planning/models.py:101-233` — WindowFunction/WindowFrameType/FrameBoundaryKind/WindowFrame/WindowExpr
- `src/tianshu_datadev/validation/window_validator.py` — WindowValidator
- `docs/roadmap/phase-3b-window-functions.md` — Phase 3B 窗口函数规划

**输入是什么**

WindowExpr（含 function、partition_by、order_by、frame）。

**输出是什么**

编译后的窗口函数 SQL 片段（如 `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time DESC)`）。

**出错会导致什么风险**

如果允许任意函数名——LLM 可能生成 `MY_CUSTOM_FUNC() OVER (...)`，Compiler 无法验证——运行时失败或更糟，执行了非预期的 UDF。如果窗口函数+子查询组合被允许——破坏 Phase 3B 的禁止规则，引入嵌套作用域。

**简单例子**

"每个用户最近 3 笔订单"需求 → WindowStep(function=ROW_NUMBER, partition_by=[ColumnRef("o.user_id")], order_by=[SortSpec(column=ColumnRef("o.order_time"), direction=DESC)]) → Compiler 渲染为 `ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.order_time DESC) AS rn`。

**Owner 审查时应该问什么**

1. "WindowStep 的函数白名单在哪定义的？如果要新增 PERCENT_RANK——需要改哪些文件？"
2. "窗口函数 + CASE WHEN 组合（如按条件窗口）——当前支持吗？"

---

## 35. CaseWhenStep（CASE WHEN 步骤）

**一句话解释**：CASE WHEN 标签分类的类型化表达——所有枚举值必须在 DeveloperSpec 中声明，未声明枚举值被拒绝。

**是什么**

CaseWhenStep 是 SqlBuildPlan 的第 6 种 Step 类型。它通过一组 WhenBranch 表达 CASE WHEN 逻辑：每个 WhenBranch 含 condition（Predicate）+ result（ColumnRef 或 SqlLiteral）。关键约束：所有可能的输出标签值必须在 DeveloperSpec 中预先声明——未声明的枚举值在 Validator 阶段被拒绝。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/sql_build_plan.py:106` — CaseWhenStep 定义
- `src/tianshu_datadev/planning/models.py:236-254` — AliasExpr/WhenBranch
- `src/tianshu_datadev/validation/label_validator.py` — LabelValidator（枚举值校验）

**输入是什么**

WhenBranch 列表 + 枚举声明列表。

**输出是什么**

编译后的 CASE WHEN SQL 片段。

**出错会导致什么风险**

如果允许未声明的枚举值通过——LLM 可能编造标签名（如 `CASE WHEN amount > 1000 THEN 'high' ELSE 'unknown' END` 但 'unknown' 未定义）——下游消费者依赖枚举值做后续处理，未知标签会导致逻辑断裂。

**简单例子**

DeveloperSpec 声明 `case_labels: ["high", "medium", "low"]` → CaseWhenStep(when_branches=[WhenBranch(condition=Predicate(column=ColumnRef("o.amount"), operator=GT, value=SqlLiteral("1000")), result="high"), ...]) → Validator 确认所有 result 值在 ["high", "medium", "low"] 中 → 通过。

**Owner 审查时应该问什么**

1. "如果 CASE WHEN 的 ELSE 分支产生了一个未声明的标签——Validator 如何处理？"
2. "LabelValidator 是在编译前还是编译后运行？如果标签声明在 DataTransformContract 中——Validator 何时拿到声明列表？"

---

## 36. 枚举自动检测（Enum Profiling）

**一句话解释**：从数据快照自动推断列的真实枚举值——用于 CASE WHEN 标签声明和类枚举字段识别。

**是什么**

EnumProfiler 是 Phase 3B.1 新增的数据画像组件。它读取 DuckDB 快照数据，对表的指定列执行 `SELECT DISTINCT` 采样，返回 EnumProfile（含 distinct_values、null_count、total_rows、confidence_tier）。产出用于：CASE WHEN 标签声明的自动补全、Review Report 中的枚举字段自动标注。

**在当前项目中的位置**

- `src/tianshu_datadev/profiling/enum_profiler.py:122` — EnumProfiler
- `src/tianshu_datadev/profiling/models.py:34-70` — EnumProfile/EnumDetectionResult/EnumConfidenceTier/EnumFieldClass
- `docs/roadmap/phase-3b1-enum-auto-detection.md` — Phase 3B.1 规划

**输入是什么**

表名 + 列名 + DuckDB 连接。

**输出是什么**

EnumProfile：含 distinct_values、null_count、total_rows、confidence_tier（HIGH/MEDIUM/LOW/UNKNOWN）、field_class（TRUE_ENUM/PSEUDO_ENUM/FREE_TEXT/IDENTIFIER/UNKNOWN）。

**出错会导致什么风险**

如果 EnumProfiler 在大型表上未做采样限制——全表扫描可能耗尽内存或超时。如果 confidence_tier 低了但被当作 HIGH 使用——假枚举值列表不完整，导致合法标签被拒绝。

**简单例子**

订单表的 status 列 → `SELECT DISTINCT status FROM dwd.order_fact` → ['pending', 'confirmed', 'shipped', 'cancelled'] → EnumProfile(distinct_values=4, total_rows=1000000, confidence_tier=HIGH, field_class=TRUE_ENUM)。

**Owner 审查时应该问什么**

1. "EnumProfiler 的采样上限是多少？如果 distinct 值超过上限——怎么处理？"
2. "confidence_tier 的 HIGH/MEDIUM/LOW 阈值是如何划分的？"

---

## 37. FakeRelationshipPlanner

**一句话解释**：Join 关系的确定性推理器——从 DeveloperSpec 的 relationships 声明确定性构建 JoinCandidate，不依赖 LLM。

**是什么**

FakeRelationshipPlanner 是 RelationshipPlanner 的确定性实现。它从 ParsedDeveloperSpec 的 relationships 声明中提取 Join 信息，为每对关系创建 JoinCandidate，并用 SourceManifest 的类型信息填充 evidence。在 Pipeline 中替代 LLM 版的 RelationshipPlanner。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/relationship_planner.py:23` — FakeRelationshipPlanner

**输入是什么**

ParsedDeveloperSpec 的 relationships 字段 + SourceManifest。

**输出是什么**

RelationshipHypothesis（含 JoinCandidate 列表）。

**出错会导致什么风险**

如果 FakeRelationshipPlanner 没有验证 join_keys 中的字段是否在 SourceManifest 中存在——不存在的字段引用进入 JoinCandidate → 在后续 RelationshipValidator 中才发现 → 错误发现太晚。

**简单例子**

DeveloperSpec 声明 `relationships: [{left_table: "u", right_table: "o", join_keys: [[u.user_id, o.user_id]]}]` → FakeRelationshipPlanner.plan() → JoinCandidate(candidate_id="...", left_table="u", right_table="o", join_keys=[(ColumnRef("u.user_id"), ColumnRef("o.user_id"))], evidence=[])。

**Owner 审查时应该问什么**

1. "FakeRelationshipPlanner 和 LLM RelationshipPlanner 的接口是否完全相同？能否热替换？"
2. "FakeRelationshipPlanner 如何处理无显式 join_keys 声明的场景？"

---

## 38. SqlProgramBuilder

**一句话解释**：多语句 SqlProgram 的确定性构建器——从一组 SqlBuildPlan 组装为有依赖 DAG 的完整程序。

**是什么**

SqlProgramBuilder 接收一组 SqlBuildPlan（或从 DeveloperSpec + SourceManifest 确定性生成），构建 SqlProgram：为每个 SqlBuildPlan 创建 SqlStatement、确定 statement 间的依赖关系（DAG）、分配 _temp 表名（防冲突）、执行拓扑排序、产出完整 SqlProgram。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/sql_program.py:463` — SqlProgramBuilder
- `src/tianshu_datadev/planning/sql_program.py:82` — SqlProgram 定义

**输入是什么**

一组 SqlBuildPlan + 依赖声明。

**输出是什么**

SqlProgram 实例。

**出错会导致什么风险**

如果 DAG 构建出环——拓扑排序失败，程序无法编译。如果 _temp 表名在两个独立 statement 中重复——后执行的覆盖前者数据，造成静默数据错误。

**简单例子**

```python
builder = SqlProgramBuilder()
program = builder.build_from_statements([sql_build_plan_1, sql_build_plan_2])
# program.statements[0] → stmt_001 (含 SqlBuildPlan_1)
# program.statements[1] → stmt_002 (含 SqlBuildPlan_2, 依赖 stmt_001)
# program.topological_order → ["stmt_001", "stmt_002"]
```

**Owner 审查时应该问什么**

1. "SqlProgramBuilder 是如何检测 statement 之间依赖的？是通过 _temp 表引用还是程序员显式声明？"
2. "如果两个 statement 之间没有依赖——它们的执行顺序是并行的还是串行的？"

---

## 39. DataTransformContractExtractor

**一句话解释**：从已验证 SqlBuildPlan/SqlProgram 确定性抽取 DataTransformContract——用于 SQL/Spark 共享的业务规格。

**是什么**

DataTransformContractExtractor 提供了两个关键方法：
- `extract_lite(plan: SqlBuildPlan) -> DataTransformContractLite`（Phase 2）
- `extract_v1(program: SqlProgram) -> DataTransformContractV1`（Phase 3 Exit）

抽取是纯函数——相同输入产生相同 Contract 和相同哈希。V1 相比 Lite 新增：step_dag、temp_tables、case_when_labels、window_specs、write_spec。

**在当前项目中的位置**

- `src/tianshu_datadev/artifacts/contract_extractor.py:41` — DataTransformContractExtractor

**输入是什么**

SqlBuildPlan（lite）或 SqlProgram（v1）。

**输出是什么**

DataTransformContractLite 或 DataTransformContractV1。

**出错会导致什么风险**

如果 Contract 抽取丢失了关键字段（如 temp_tables 规格）——SparkDeveloper 基于不完整规格生成代码。如果同一 SqlProgram 两次抽取产生不同 Contract——破坏确定性契约，跨引擎验证假失败。

**简单例子**

```python
extractor = DataTransformContractExtractor()
contract = extractor.extract_v1(sql_program)
# contract.step_dag → {"stmt_001": [], "stmt_002": ["stmt_001"]}
# contract.temp_tables → [TempTableSpec(name="_temp_step1", ...)]
# contract.case_when_labels → [CaseWhenLabelSpec(...)]
```

**Owner 审查时应该问什么**

1. "Contract 抽取是完全确定性的吗？如果 SqlProgram 的 step_id 变化但结构不变，Contract hash 会变吗？"
2. "Contract 中哪些字段来自 DeveloperSpec、哪些来自 SqlBuildPlan/SqlProgram？"

---

## 40. ReviewPackageBuilder

**一句话解释**：Code Review 材料打包器——将全链路 artifact 组装为 ReviewPackageManifest。

**是什么**

ReviewPackageBuilder 接收 PackageInputs（含 DeveloperSpec、ParsedDeveloperSpec、SourceManifest、SqlBuildPlan/SqlProgram、CompiledSql、ExecutionTrace、DataTransformContract），生成 ReviewPackageManifest：记录 package_id、所有 artifact 的哈希引用、status、created_at。

**在当前项目中的位置**

- `src/tianshu_datadev/artifacts/packager.py:36` — ReviewPackageBuilder
- `src/tianshu_datadev/artifacts/models.py:279` — ReviewPackageManifest
- `src/tianshu_datadev/artifacts/models.py:372` — PackageInputs

**输入是什么**

PackageInputs：含全链路 artifact 引用。

**输出是什么**

ReviewPackageManifest：含 package_id + artifact 哈希清单 + status。

**出错会导致什么风险**

如果 PackageInputs 中某个 artifact 的哈希与实际内容不一致——人工审查者核对时无法验证代码基于正确的源数据生成。如果 package_id 的生成策略依赖时间戳——同一输入不同时间产生不同 package_id，破坏可复现性。

**简单例子**

```python
builder = ReviewPackageBuilder()
manifest = builder.build(PackageInputs(
    developer_spec_text="...",
    parsed_spec_hash="...",
    source_manifest_hash="...",
    sql_plan_hash="...",
    sql_artifact_hash="...",
    execution_trace=execution_trace,
    contract_hash="..."
))
# manifest.package_id → "pkg_xxx"
# manifest.status → "READY_FOR_REVIEW"
# manifest.artifact_refs → [ArtifactRef(...), ...]
```

**Owner 审查时应该问什么**

1. "ReviewPackageManifest 的 package_id 是根据哪些字段生成的哈希？"
2. "如果 Raw LLM Response 还没有结构化输出——它在 ReviewPackage 中是什么状态？"

---

## 41. ExecutionTrace

**一句话解释**：SQL 执行的完整追踪记录——含状态、行数、耗时、错误和优化前后 SQL。

**是什么**

ExecutionTrace 是每条 SQL 执行的审计记录。它包含：status（ExecutionStatus 枚举）、row_count、elapsed_ms、error（如有）、single_statement_compiled_sql、optimized_sql_before_execution、source_anomalies。不包含完整结果集——只记录摘要。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/models.py:124` — ExecutionTrace
- `src/tianshu_datadev/sql/models.py:19` — ExecutionStatus（NOT_EXECUTED/RUNTIME_PASS/RUNTIME_FAILED/TIMEOUT）
- `src/tianshu_datadev/sql/models.py:429` — StatementExecutionResult（多语句每条的执行追踪）
- `src/tianshu_datadev/sql/models.py:437` — ProgramExecutionResult（多语句总执行追踪）

**输入是什么**

CompiledSql + DuckDB 执行结果。

**输出是什么**

ExecutionTrace 实例。

**出错会导致什么风险**

如果 ExecutionTrace 记录了错误的 row_count——后续 Comparator 的 DIFFERENT 判断可能误报。如果 error 字段未正确填充——执行失败但原因不明，无法进入 Repair 链路。

**简单例子**

```python
trace = executor.execute(compiled_sql, table_paths)
# trace.status → RUNTIME_PASS
# trace.row_count → 7
# trace.elapsed_ms → 45.2
# trace.error → None
```

**Owner 审查时应该问什么**

1. "ExecutionTrace 中的 single_statement_compiled_sql 和 optimized_sql_before_execution 有什么区别？"
2. "如果 SQL 执行超时——ExecutionTrace 的 status 是什么？error 字段记录了什么？"

---

## 42. Compiler Pass（编译器优化遍历）

**一句话解释**：SQL 编译前的 4 个优化 Pass——列裁剪、谓词规范化、无用排序消除、常量折叠，必须幂等。

**是什么**

Compiler Pass 是在 SQL 渲染前按顺序执行的一组优化遍历：
1. **列裁剪**：移除 ScanStep 中未被后续步骤引用的列
2. **谓词规范化**：将 Predicate 表达式树规范化为 CNF（合取范式）
3. **无用排序消除**：移除被后续 SortStep 覆盖的排序操作
4. **常量折叠**：编译期计算常量表达式

每个 Pass 必须是幂等的——多次执行相同 Step 产生相同结果。Phase 4B 规划在真实 LLM 输出的 SqlBuildPlan 上验证这些 Pass 的稳定性。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/compiler_passes.py` — 4 个 Compiler Pass
- `src/tianshu_datadev/sql/models.py:33` — CompilerPassRecord（单个 Pass 的执行记录）
- `src/tianshu_datadev/sql/models.py:60` — OptimizedSQLPlan（优化后的 SQL 计划）

**输入是什么**

原始 SqlBuildPlan/SqlProgram。

**输出是什么**

优化后的 SqlBuildPlan/SqlProgram + CompilerPassRecord 列表。

**出错会导致什么风险**

如果某个 Pass 不幂等——相同 SqlBuildPlan 多次编译产生不同 SQL 文本和哈希，破坏确定性编译器的核心承诺。如果列裁剪错误地移除了 Join key——Compiler 渲染出非法 SQL。

**简单例子**

SqlBuildPlan 的 ScanStep 声明了 10 个 required_columns，但整个计划中只有 3 个被后续 FilterStep/AggregateStep 引用 → 列裁剪 Pass 移除 7 个未使用的列 → 减少扫描数据量 → 节省 IO。

**Owner 审查时应该问什么**

1. "Compiler Pass 的执行顺序重要吗？交换 Pass 1 和 Pass 3 的执行顺序会产生不同结果吗？"
2. "每个 Pass 的幂等性有被测试覆盖吗？测试是如何验证幂等性的？"

---

## 43. FinalWritePlan

**一句话解释**：受控写入方案的审查材料——只允许日期分区 overwrite，作为人工审查的依据不实际执行。

**是什么**

FinalWritePlan 是从 SqlProgram 中提取的写入方案规格。它声明：目标表、分区键、overwrite 模式（仅 "partition"）、分区值、验证检查列表。它是审查材料——不在 Agent 环境中实际执行写入生产库。WriteValidator 对其执行 10 项安全检查。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/write_plan.py:269` — FinalWritePlan
- `src/tianshu_datadev/sql/write_plan.py:53` — WriteValidationCheck
- `src/tianshu_datadev/sql/write_plan.py:138` — PartitionOverwriteSpec
- `src/tianshu_datadev/sql/write_plan_builder.py:26` — FinalWritePlanBuilder

**输入是什么**

SqlProgram + 写入声明（target_table、partition_keys、overwrite_mode、partition_values）。

**输出是什么**

FinalWritePlan 实例：含 write_plan_id、target_table、partition_keys、overwrite_mode、partition_values、validation_checks、review_material（审查说明文本）。

**出错会导致什么风险**

如果 WriteValidator 的 10 项检查漏检了一项（如分区键声明了但实际表不存在该分区）——写入方案审查通过但人工执行时失败。如果 review_material 内容不充分——人工审查者无法理解决策上下文。

**简单例子**

```python
write_plan = FinalWritePlan(
    write_plan_id="wp_xxx",
    program_id="sp_xxx",
    target_table="ads.dau_daily",
    partition_keys=["stat_date"],
    overwrite_mode="partition",
    partition_values={"stat_date": "2026-06-29"},
    validation_checks=[WriteValidationCheck(check_id="wc_001", check_type="PARTITION_EXISTS", passed=True, detail="分区 stat_date 在目标表元数据中已注册")],
    review_material="向目标日期分区 ads.dau_daily/stat_date=2026-06-29 执行 INSERT OVERWRITE。请人工确认分区值正确后执行。"
)
```

**Owner 审查时应该问什么**

1. "FinalWritePlan 的 review_material 是机器自动生成的还是需要人工补充？"
2. "如果目标表的实际分区是 'dt' 而非 'stat_date'——WriteValidator 能发现吗？在哪个检查项中被拦？"

---

## 44. SchemaRegistry

**一句话解释**：可选的类型/枚举补充接口——补充 SourceManifest 中缺失的列类型和枚举值，禁止静默覆盖程序员声明的值。

**是什么**

SchemaRegistry 是一个 Protocol（非实现），定义了外部类型/枚举补充的接口。当程序员在 DeveloperSpec 中未声明列类型时，SchemaRegistry 可以提供补全信息。关键约束：SchemaRegistry 只能补充缺失信息，禁止静默覆盖程序员已声明的值——冲突时输出 SOURCE_CONFLICT。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/source_manifest.py:34` — SchemaRegistry Protocol

**输入是什么**

表名 + 列名 → 可选的外部元数据查询。

**输出是什么**

ManifestColumn（含 type、nullable、comment）或 None（表示未知）。

**出错会导致什么风险**

如果 SchemaRegistry 静默覆盖了程序员声明的类型（如程序员说 `amount: decimal(18,2)`，Registry 返回 `float`）——Compiler 使用错误类型 → SQL 结果精度丢失 → 难以定位根因。如果 Registry 查询超时——SourceManifestBuilder 可能需要等待或降级。

**简单例子**

DeveloperSpec 中声明了 `business_columns: [{name: "status", type: null}]` → SchemaRegistry.resolve("dwd.order_fact", "status") → ManifestColumn(type="varchar", nullable=false) → 补全后的 SourceManifest 中 status 列为 varchar。

**Owner 审查时应该问什么**

1. "SchemaRegistry 目前有实现吗？还是只是 Protocol 定义？"
2. "SOURCE_CONFLICT 被记录到 OpenQuestion 还是直接拒绝？"

---

## 45. FieldNormalizer

**一句话解释**：列名的确定性规范化器——将驼峰、下划线、中文列名统一为 snake_case ASCII 格式。

**是什么**

FieldNormalizer 是 DeveloperSpec 解析流程中的预处理组件。它接收程序员手写的列名（可能包含中文、驼峰、大小写不一致），将列名规范化为统一的 snake_case ASCII 格式（如 `用户ID`→`user_id`、`OrderAmount`→`order_amount`）。规范化后的列名通过 SafeIdentifier 校验，确保与 SourceManifest 一致。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/field_normalizer.py:24` — FieldNormalizer
- `src/tianshu_datadev/developer_spec/field_normalizer.py:14` — NormalizationConfig

**输入是什么**

原始列名字符串 + NormalizationConfig。

**输出是什么**

规范化后的列名字符串。

**出错会导致什么风险**

如果规范化规则过于激进——`userId` 和 `user_id` 被规范化为相同名称但实际是不同的列——造成列名冲突或错误的引用。如果中文列名在规范化后与另一个 ASCII 列名冲突——无法区分两列。

**简单例子**

```python
normalizer = FieldNormalizer()
normalizer.normalize("用户ID")        # → "user_id"
normalizer.normalize("OrderAmount")  # → "order_amount"
normalizer.normalize("stat_date")    # → "stat_date"（已经是 snake_case，不变）
```

**Owner 审查时应该问什么**

1. "规范化映射表是可配置的还是硬编码的？如果新增一种命名约定（如匈牙利命名法）——如何扩展？"
2. "中文列名 `金额` 在规范化后变成什么？是否可能与 `amount` 冲突？"

---

## 46. REST API（Phase 4.5 内部交互验证口）

**一句话解释**：通过 REST API + CLI 暴露核心流水线——用于内部开发者交互验证和集成测试。

**是什么**

REST API 是 Phase 4.5 规划的对外接口层。它通过 FastAPI 暴露 5 个端点（POST /api/spec/parse、POST /api/plan、POST /api/execute、POST /api/run-all、GET /api/package/{request_id}），由 Pipeline 确定性响应。CLI 工具（tianshu parse/run/package）通过 argparse 子命令提供等价的命令行入口。不涉及前端、不做生产执行入口。

**在当前项目中的位置**

- `src/tianshu_datadev/api/app.py` — FastAPI 工厂
- `src/tianshu_datadev/api/routes.py` — 5 个路由处理器
- `src/tianshu_datadev/api/models.py` — Request/Response 模型
- `src/tianshu_datadev/api/pipeline.py` — Pipeline 编排器
- `src/tianshu_datadev/api/error_handlers.py` — 结构化错误处理
- `src/tianshu_datadev/cli/main.py` — CLI 入口
- `docs/roadmap/phase-4-5-internal-workbench.md` — Phase 4.5 规划

**输入是什么**

HTTP 请求（JSON body）或 CLI 参数。

**输出是什么**

JSON 响应（SpecParseResponse/PlanResponse/ExecuteResponse/RunAllResponse/PackageResponse）+ 结构化错误（ParseError → 422）。

**出错会导致什么风险**

如果 API 绕过 Pipeline 直接调用组件——失去编排层的请求隔离和状态管理。如果 CLI 和 REST API 同输入不同输出——"CLI 和 Web 同输入同输出"的退出条件不满足。

**简单例子**

```bash
# CLI
tianshu parse golden_no_time_range.md
# → {"status": "SPEC_PARSED", "spec_id": "...", "parse_warnings": [{"code": "W002", "message": "..."}], ...}

# REST API
curl -X POST http://localhost:8000/api/spec/parse -d '{"developer_spec_text": "..."}'
# → 200 {"status": "SPEC_PARSED", "spec_id": "...", ...}
```

**Owner 审查时应该问什么**

1. "REST API 和 CLI 是否共享同一个 Pipeline 实例？还是每次请求创建新实例？"
2. "如果 ParseError 包含中文消息——API 的 422 响应中 error_code 是中文还是英文？"

---

## 47. Spark Generation Boundary（Spark 生成边界）

**一句话解释**：Spark 代码只能生成纯转换入口函数 `transform(inputs, params) -> DataFrame`，禁止 Action/Sink/UDF/网络/文件系统/动态执行。

**是什么**

Spark Generation Boundary 定义了 PySpark 代码的生成边界。LLM 可以生成 PySpark，但只能生成一个特定签名的函数：`transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame`。强制约束：只读取 `inputs` 中契约声明的数据源、禁止 `spark.table`/`spark.read`/创建 SparkSession、禁止 Action（如 `collect()`/`show()`/`write`）/Sink/UDF/网络/文件系统/进程/线程/动态执行和任意模块导入。返回且仅返回一个 DataFrame。

**在当前项目中的位置**

- `AGENTS.md §3` — Spark Generation Boundary 完整定义
- `src/tianshu_datadev/spark/` — Spark 相关代码（Phase 5 前不碰）
- `docs/roadmap/phase-5-spark-ready-contract-and-sparkplan.md` — Phase 5 规划

**输入是什么**

DataTransformContract（从已验证 SqlBuildPlan 确定性抽取）+ inputs（Mapping[str, DataFrame]）+ params（TransformParams）。

**输出是什么**

一个 DataFrame——纯转换结果，不包含任何 Action。

**出错会导致什么风险**

如果生成的代码中包含 `spark.table("production_db.secret_table")`——绕过输入契约，越权访问未授权的数据源。如果生成的代码中包含 `df.write.save("s3://...")`——将数据写入未授权的外部存储。

**简单例子**

```python
# ✅ 合法：纯转换函数
def transform(
    inputs: Mapping[str, DataFrame],
    params: TransformParams,
) -> DataFrame:
    df = inputs["order_fact"]
    result = df.groupBy("stat_date").agg(F.countDistinct("user_id").alias("dau"))
    return result

# ❌ 非法：使用了 spark.table（绕过契约）
def transform(inputs, params):
    df = spark.table("production_db.order_fact")  # ← 红线
    ...
```

**Owner 审查时应该问什么**

1. "Spark Code 的静态验证是如何实现的？在代码生成后、执行前有 AST 级别的检查吗？"
2. "如果生成的代码中 import 了 `requests` 库——静态验证能发现吗？"

---

## 48. ReviewFeedback

**一句话解释**：结构化的人工审查反馈——至少包含 request_id、artifact 哈希、target（路由主字段）、finding_type（细分原因）、suggested_resolution。

**是什么**

ReviewFeedback 是人工审查者对 ReviewPackage 的反馈格式。它不是自由文本——必须包含：request_id、review_package_id、developer_spec_hash、source_manifest_hash、sql_build_plan_hash、sql_artifact_hash、target（路由主字段：REQUIREMENT/SQL_PLAN/COMPILER_BUG/SOURCE_FACT/HUMAN_REVIEW）、finding_type（细分原因）、comment、suggested_resolution。target=HUMAN_REVIEW 时停止自动返工。

**在当前项目中的位置**

- `src/tianshu_datadev/artifacts/models.py:336` — ReviewFeedback 定义
- `AGENTS.md §6` — Repair Boundary 中的 ReviewFeedback 规范

**输入是什么**

人工审查者的结构化反馈（通过 API 或 CLI 提交）。

**输出是什么**

ReviewFeedback 实例——被 Agent 的 Repair 链路消费。

**出错会导致什么风险**

如果 target 字段路由错误——REQUIREMENT 问题被路由到 COMPILER_BUG→修改 Compiler 无法修复需求错误。如果缺少 artifact 哈希——无法确认反馈针对的是哪个版本的产物。

**简单例子**

```python
feedback = ReviewFeedback(
    request_id="req_xxx",
    review_package_id="pkg_xxx",
    developer_spec_hash="abc123",
    source_manifest_hash="def456",
    sql_build_plan_hash="ghi789",
    sql_artifact_hash="jkl012",
    target="SQL_PLAN",
    finding_type="MISSING_FILTER",
    comment="缺少对 cancelled 状态订单的过滤——开发需求书中要求排除已取消订单。",
    suggested_resolution="在 SqlBuildPlan 中新增 FilterStep，过滤 status != 'cancelled'。"
)
```

**Owner 审查时应该问什么**

1. "target 字段有 5 个合法值——每个 target 对应的返工入口在代码中是 if-else 还是策略模式？"
2. "如果人工审查者的 ReviewFeedback 缺少 artifact 哈希——系统是拒绝还是容错？"

---

## 50. SpecEnricher / FakeSpecEnricher

**一句话解释**：位于 Parser 与 Builder 之间的指标推断层——从业务描述中推断程序员未显式声明的聚合/窗口/计算指标，不修改程序员手写声明。

**是什么**

SpecEnricher 是 Parser → Builder 链路的中间组件。它接收 ParsedDeveloperSpec + SourceManifest，对 output_columns 中未被 spec.metrics 覆盖的指标列进行推断。分两层实现：
- **FakeSpecEnricher（Phase 1）**：纯规则匹配——中文关键词→聚合函数映射（"去重用户数"→COUNT_DISTINCT）、description 结构化 DSL 解析（"COUNT(*)"、"SUM(amount)"）、列名匹配（中文→英文列名映射表）
- **SpecEnricher（Phase 4）**：LLM 驱动——嵌入 8 条硬约束的 System Prompt（列名只能从 manifest 选、聚合函数 6 种白名单、不推断 JOIN、不修改手写声明等），未注入 LLM 时退化为 FakeSpecEnricher

核心设计原则：不修改程序员手写的 metrics——显式声明优先级 > LLM 推断（H5 硬约束）。

**解决什么问题**

程序员在 DeveloperSpec 中只声明核心指标定义时，SpecEnricher 自动补充缺失的指标声明——减轻手写负担。规则版（Fake）用于离线验证，LLM 版（Phase 4）用于灵活场景。

**在当前项目中的位置**

- `src/tianshu_datadev/planning/spec_enricher.py:388` — FakeSpecEnricher
- `src/tianshu_datadev/planning/spec_enricher.py:622` — SpecEnricher（LLM 骨架）
- `src/tianshu_datadev/planning/spec_enricher.py:40-73` — 6 类规则模式（聚合/条件/比率/窗口/函数描述解析）
- `tests/harness/test_spec_enricher.py` — 574 行测试

**输入是什么**

ParsedDeveloperSpec（含 output_columns 的业务描述）+ SourceManifest。

**输出是什么**

EnrichedSpec——原始 spec 原封不动保留，推断结果放在独立字段（inferred_metrics / inferred_window_metrics / inferred_computed_metrics）。

**出错会导致什么风险**

如果规则版推断出错误的聚合类型（如"金额"推断为 SUM 但实际是 COUNT）——SQL 语义错误且无明显信号（因为推断字段没有 HumanResolution 机制）。如果 LLM 版违反 H1（编造列名）——生成的 BUG AggregateSpec 通过 Builder 进入 SqlBuildPlan。

**简单例子**

output_columns: `[{name: "dau", description: "COUNT(DISTINCT user_id)"}]` 但 spec.metrics 中无 dau 定义 → FakeSpecEnricher 从 description 解析出 COUNT_DISTINCT(user_id) → 填充 inferred_metrics=[MetricDecl(aggregation=COUNT_DISTINCT, input_column="user_id")]。

**Owner 审查时应该问什么**

1. "FakeSpecEnricher 和 SpecEnricher 的接口是否可以互换？在 Pipeline 中如何热切换？"
2. "推断结果的 confidence 字段在 Fake 版中是 hardcoded 还是动态计算？"
3. "如果 spec.metrics 已覆盖了所有 output_columns——SpecEnricher 还做什么？"

---

## 51. EnrichedSpec

**一句话解释**：SpecEnricher 产出的丰富化规格——原始 ParsedDeveloperSpec + 三组推断指标 + 丰富化元数据，Builder 消费此产出生成 SqlBuildPlan。

**是什么**

EnrichedSpec 是 SpecEnricher 输出的 Pydantic StrictModel，设计约束：不修改原始 spec，所有推断结果放在独立字段。
- `original_spec`：原始 ParsedDeveloperSpec（不变——优先级最高）
- `inferred_metrics: list[MetricDecl]`：推断的简单聚合指标
- `inferred_window_metrics: list[InferredWindowMetric]`：推断的窗口函数指标（需 WindowStep 承接）
- `inferred_computed_metrics: list[InferredComputedMetric]`：推断的计算指标（比率/表达式，聚合后计算）
- `enrichment_metadata: dict`：来源、耗时 ms、推断总数

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py` — EnrichedSpec 定义

**输入是什么**

ParsedDeveloperSpec + SourceManifest（由 SpecEnricher 消费后产出 EnrichedSpec）。

**输出是什么**

EnrichedSpec——Builder 消费 original_spec 和 inferred_* 三组字段，生成 SqlBuildPlan。

**出错会导致什么风险**

如果 EnrichedSpec 的 inferred 字段与 original_spec 冲突（如同名指标两个定义）——Builder 需要明确的合并规则（手写优先级 > 推断），否则产生重复 AggregateSpec。

**简单例子**

```python
EnrichedSpec(
    original_spec=parsed_spec,
    inferred_metrics=[MetricDecl(metric_name="dau", aggregation=COUNT_DISTINCT, input_column="user_id")],
    enrichment_metadata={"source": "FakeSpecEnricher", "method": "rule_based", "total_inferred": 1}
)
```

**Owner 审查时应该问什么**

1. "Builder 消费 EnrichedSpec 时，original_spec.metrics 和 inferred_metrics 中的同名指标如何合并？"
2. "inferred_computed_metrics 最终在 Compiler 中渲染成什么 SQL 片段？"

---

## 52. OutputColumnDecl

**一句话解释**：输出列声明——包含名称、类型和可选的 SQL 语义描述（结构化 DSL），SpecEnricher 的 description parser 从中提取指标定义。

**是什么**

OutputColumnDecl 是 OutputSpecDecl 的列级单元。Phase 4D 升级后 `columns` 从 `list[str]` 变为 `list[OutputColumnDecl]`，每列可附带：
- `name`：输出列名
- `type`：输出列类型（date / bigint / decimal / varchar）
- `description`：结构化 DSL——"COUNT(*)"（简单聚合）、"COUNT(DISTINCT user_id)"（去重聚合）、"fined_count / total_count"（计算指标）、"ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC)"（窗口函数）

支持向后兼容：YAML 中写字符串 "col_name" 自动转为 OutputColumnDecl(name="col_name")，通过 field_validator _coerce_columns 实现。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py` — OutputColumnDecl 定义
- `src/tianshu_datadev/developer_spec/models.py` — OutputSpecDecl._coerce_columns（list[str]→list[OutputColumnDecl] 向后兼容）
- `src/tianshu_datadev/planning/spec_enricher.py:114-184` — _parse_description_to_metric() 解析入口
- `src/tianshu_datadev/planning/spec_enricher.py:187-217` — _parse_description_to_computed()
- `src/tianshu_datadev/planning/spec_enricher.py:220-256` — _parse_description_to_window()

**输入是什么**

程序员在 DeveloperSpec YAML 元数据块中声明的输出列规格。

**输出是什么**

OutputColumnDecl 实例——被 Builder 消费生成 ProjectStep 和 AggregateStep 的列引用。

**出错会导致什么风险**

如果 description 中的 SQL 签名与实际列引用不一致（如 "COUNT(DISTINCT user_id)" 但 user_id 不在 manifest 中）——推断出的 MetricDecl 在 Validator 阶段被拒绝。如果 description 为空——SpecEnricher 纯靠规则推断聚合类型，置信度较低。

**简单例子**

```yaml
columns:
  - name: dau
    type: bigint
    description: "COUNT(DISTINCT user_id)"
  - name: stat_date
    type: date
```

**Owner 审查时应该问什么**

1. "OutputColumnDecl 的 description 字段如果写了一个不支持的函数（如 MEDIAN）——SpecEnricher 如何处理？"
2. "向后兼容的 list[str] → list[OutputColumnDecl] 转换是在 Parser 层还是 Pydantic 的 field_validator 层做的？"

---

## 53. MetricDecl 扩展（Phase 4D）

**一句话解释**：指标声明新增条件聚合、表达式聚合、去重标志——支持 FILTER (WHERE ...) 子句和多字段表达式，扩展 SQL 聚合表达能力。

**是什么**

Phase 4D 为 MetricDecl 新增三个可选字段：
- `filter: MetricFilterDecl | None`：条件聚合过滤条件——如 `COUNT(DISTINCT plate_id) FILTER (WHERE fine_status = 'STANDARD')`。MetricFilterDecl 含 column、operator（8 种白名单）、value
- `input_expression: str | None`：多字段表达式——如 `"quantity * unit_price"`，编译时直接透传
- `distinct: bool`：去重聚合标志——如 `SUM(DISTINCT amount)`

这些字段在 SqlBuildPlanBuilder（透传至 AggregateSpec）和 DuckDbSqlCompiler（渲染为 SQL FILTER 子句）中同步更新。

**解决什么问题**

覆盖 Phase 1-3 无法表达的业务场景：条件聚合（"有标准罚款的车牌数"）、表达式聚合（"金额×数量求和"）、去重求和（"去重金额汇总"）。避免程序员使用自由 SQL 片段来绕过。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py` — MetricFilterDecl + MetricDecl.filter/input_expression/distinct
- `src/tianshu_datadev/planning/sql_build_plan.py:576-592` — Builder 透传至 AggregateSpec
- `src/tianshu_datadev/sql/compiler.py:439-529` — Compiler 渲染 FILTER 子句（_render_metric_filter）
- `src/tianshu_datadev/planning/models.py` — AggregateSpec.filter/input_expression/distinct

**输入是什么**

程序员在 DeveloperSpec 中声明的 MetricDecl 中的 filter/input_expression/distinct 字段。

**输出是什么**

编译后的 SQL——含 FILTER (WHERE ...) 子句、DISTINCT 修饰、表达式替代列引用。

**出错会导致什么风险**

如果 MetricFilterDecl.operator 不受限制——程序员或 LLM 可注入任意操作符。如果 filter.value 不做类型处理（Compiler 统一加单引号）——数字过滤值（如 amount > 1000）可能生成 `amount > '1000'`（隐式类型转换，性能下降）。

**简单例子**

`MetricDecl(metric_name="fined_count", aggregation=COUNT_DISTINCT, input_column="plate_id", filter=MetricFilterDecl(column="fine_status", operator="eq", value="STANDARD"))` → Compiler 渲染为 `COUNT(DISTINCT plate_id) FILTER (WHERE fine_status = 'STANDARD') AS fined_count`。

**Owner 审查时应该问什么**

1. "MetricFilterDecl 的 operator 白名单在哪里定义？新增一个操作符需要改哪些文件？"
2. "input_expression 是否有安全审查机制（如禁止包含子查询）？谁负责这个审查？"

---

## 54. SparkPlan IR

**一句话解释**：Spark 侧的类型化中间表示——9 种封闭 Step 类型（与 SqlBuildPlan 对称），从 DataTransformContractV1 确定性映射，不包含 SQL 文本或 PySpark 代码。

**是什么**

SparkPlan IR 是 Phase 5 的核心交付物。它定义 9 种 SparkStep 类型（READ/FILTER/JOIN/AGGREGATE/PROJECT/CASE_WHEN/WINDOW/SORT/LIMIT），每种是结构化 Pydantic StrictModel。SparkPlan 从 DataTransformContractV1 确定性映射（通过 mapper.py），不读取 SQL 文本或 SqlBuildPlan。相同 Contract → 相同 SparkPlan → 相同 plan_hash。步骤按固定顺序组装：Read → Filter → Join → Aggregate → Window → CaseWhen → Project → Sort → Limit。

**解决什么问题**

为 SQL 和 Spark 提供可等价对比的对称 IR——Phase 7 PlanComparator 通过对比 SqlBuildPlan 和 SparkPlan 的结构等价性（而非对比 SQL 文本和 PySpark 文本），实现跨引擎正确性验证，不受执行顺序差异影响。

**在当前项目中的位置**

- `src/tianshu_datadev/spark/models.py:261` — SparkPlan 顶层容器
- `src/tianshu_datadev/spark/models.py:29-41` — SparkStepType 枚举（9 种）
- `src/tianshu_datadev/spark/models.py:88-241` — 9 个 SparkStep 类（SparkReadStep 等）
- `src/tianshu_datadev/spark/mapper.py:98` — map_contract_to_spark_plan() 入口
- `src/tianshu_datadev/spark/models.py:282-321` — generate_plan_id() / compute_plan_hash()
- `src/tianshu_datadev/spark/__init__.py` — 模块导出
- `tests/spark/test_spark_plan.py` — SparkPlan 测试

**输入是什么**

DataTransformContractV1（从 SqlProgram 确定性抽取的业务规格，含 input_tables、filters、join_relationships、aggregations、output_columns 等）。

**输出是什么**

SparkPlan——含 plan_id（从 contract_hash 派生 SHA-256[:12]）、version（"v1"）、steps（list[SparkStep]）、source_contract_hash、write_mode。

**出错会导致什么风险**

如果 SparkPlan 的 step 枚举与 SqlBuildPlan 不同步（如 SQL 侧新增 SubqueryStep 但 Spark 侧无对应映射）——PlanEquivalence 判定为 UNSUPPORTED_COMPARISON，覆盖不全。如果 Contract→SparkPlan 映射非确定性——相同 Contract 不同映射结果导致 Phase 7 跨验证假失败。

**简单例子**

ContractInputTable(table_ref="od") → SparkPlanMapper → SparkReadStep(alias="od", source_path="order_daily/", format="parquet")。ContractAggregation(function="COUNT_DISTINCT", input_column="user_id") → SparkAggregateStep(metrics=[SparkAggregateSpec(function=COUNT_DISTINCT, input_column="user_id")])。

**Owner 审查时应该问什么**

1. "SparkPlan 和 SqlBuildPlan 的 step 类型数量是否完全对标？各自有哪些对方没有的类型？"
2. "SparkPlan 的 plan_hash 和 SqlBuildPlan 的 plan_id 计算方式是否可跨 IR 对比？"

---

## 55. SparkPlanMapper

**一句话解释**：DataTransformContractV1 → SparkPlan 的确定性映射函数——9 类 Contract 信息通过 9 条规则代码映射为 9 种 SparkStep，纯函数无副作用、无 LLM 调用。

**是什么**

SparkPlanMapper 定义 `map_contract_to_spark_plan(contract) → SparkPlanMappingResult`。每条映射规则是确定性代码：ContractInputTable → SparkReadStep（path 约定 `{table_name}/`）、ContractPredicate → SparkFilterStep（结构化三元组直传）、ContractJoin → SparkJoinStep（evidence_chain 直传）、ContractAggregation + grouping_keys → SparkAggregateStep（合并为一个步骤）、ContractOutputColumn → SparkProjectStep、CaseWhenLabelSpec → SparkCaseWhenStep、WindowSpecSummary → SparkWindowStep、ContractSort → SparkSortStep、ContractLimit → SparkLimitStep。步骤按固定顺序组装，BLOCKING gap 或 unsupported pattern 存在时 success=False。

**解决什么问题**

确保 SQL 和 Spark 从同一份业务规格（DataTransformContractV1）出发——SparkDeveloper 不读取 DeveloperSpec 或 SqlBuildPlan，避免两套独立的业务理解产生口径不一致。SparkPlanMapper 是 SQL 和 Spark 之间的"翻译层"。

**在当前项目中的位置**

- `src/tianshu_datadev/spark/mapper.py:98` — map_contract_to_spark_plan() 主函数
- `src/tianshu_datadev/spark/mapper.py:237-604` — 9 个 _map_* 辅助函数
- `src/tianshu_datadev/spark/mapper.py:57-95` — 5 个映射查表（_AGG_FUNCTION_MAP 等）
- `tests/spark/test_spark_plan.py` — 映射测试

**输入是什么**

已验证的 DataTransformContractV1 实例。

**输出是什么**

SparkPlanMappingResult——success=True 时 spark_plan 非空、unsupported/gaps 为空；success=False 时记录所有阻断项。

**出错会导致什么风险**

如果映射规则遗漏了某个 Contract 字段（如 window_specs 的帧定义）——SparkPlan 丢失窗口帧信息。如果步骤顺序错误（如 Aggregate 在 Join 前）——Phase 6 SparkDeveloper 生成调用链错误的 PySpark 代码。

**简单例子**

```python
result = map_contract_to_spark_plan(contract)
assert result.success == True
assert len(result.spark_plan.steps) > 0  # 至少 1 个 ReadStep + 其他
```

**Owner 审查时应该问什么**

1. "如果 Contract 的 output_columns 为空——mapper 返回什么状态？"
2. "BLOCKING 级别和 WARN 级别的 ContractGap 有什么区别？在 Pipeline 中分别如何处理？"

---

## 56. PlanEquivalence（计划等价判定）

**一句话解释**：SqlBuildPlan step 与 SparkPlan step 的结构等价判定规则集——通过归一化字段名对比每种 step 类型的业务语义，替代 SQL/Spark 文本对比。

**是什么**

PlanEquivalence 定义一组确定性对比函数（每种 step 类型一个），入口为 `compare_plans(sql_steps, spark_steps) → PlanEquivalenceResult`。所有对比前执行字段名归一化（`normalize_field_name`: 去表别名前缀、统一小写），再对比结构化字段：
- scan：source_table/alias 数量+集合一致
- filter：(left, operator, right) 三元组等价
- join：(left_table, right_table, left_key, right_key, join_type) 等价
- aggregate：group_keys 集合 + metrics 的 (function, input_column, alias) 等价
- project：输出列名和别名集合一致
- case_when：output_alias、label 集合、else 值一致
- window：(function, alias, partition_by, order_by) 等价
- sort：排序规格一致
- limit：limit/offset 值一致

产出三个判定级别：EQUIVALENT、NOT_EQUIVALENT、UNSUPPORTED_COMPARISON。暂不支持 subquery 类型的对比。

**解决什么问题**

替代 SQL 文本和 PySpark 代码的直接对比——文本对比无法处理执行顺序差异（如 Filter 下推导致 SQL/Spark 步骤顺序不同但语义等价）。结构化 IR 对比只关心业务语义，不关心执行顺序。

**在当前项目中的位置**

- `src/tianshu_datadev/spark/plan_equivalence.py:759` — compare_plans() 入口
- `src/tianshu_datadev/spark/plan_equivalence.py:87-718` — 9 个 step 对比函数
- `src/tianshu_datadev/spark/plan_equivalence.py:61-79` — normalize_field_name()
- `src/tianshu_datadev/spark/plan_equivalence.py:726-756` — _STEP_COMPARATORS 注册表 + _UNSUPPORTED_STEP_TYPES
- `docs/05-cross-validation-and-repair-plan.md` — 跨验证规划

**出错会导致什么风险**

如果 normalize_field_name 处理不当（如 "o.user_id" 归一化为 "user_id" 但 "user_id" 本身是另一个字段）——假 EQUIVALENT。如果 SQL 和 Spark 步长不对等（如 SQL 用 2 个 FilterStep，Spark 合并为 1 个）——NOT_EQUIVALENT 但实际语义等价。

**简单例子**

SQL 侧：ScanStep("od") + AggregateStep(group_keys=["stat_date"], metrics=[COUNT(DISTINCT user_id)]) → Spark 侧：SparkReadStep(alias="od") + SparkAggregateStep(group_keys=["stat_date"], metrics=[COUNT_DISTINCT(user_id)]) → compare_aggregate_steps 判定 EQUIVALENT。

**Owner 审查时应该问什么**

1. "PlanEquivalence 的 UNSUPPORTED_COMPARISON 在什么条件下触发？"
2. "如果 SQL 侧有排序但 Spark 侧没有——compare_plans() 的 overall_verdict 是什么？"

---

## 57. UnsupportedPattern / ContractGap / SparkPlanMappingResult

**一句话解释**：Contract → SparkPlan 映射中的三种阻断/警告产出——记录哪些数据变换模式无法映射、Contract 缺什么信息、映射的完整状态。

**是什么**

三种阻断模型配合使用：
- **UnsupportedPattern**：Contract 中某些模式无法映射为 SparkStep。当前不支持的模式：相关子查询、CTE、自定义窗口帧（非默认帧）、CROSS JOIN（无证据笛卡尔积）。含 pattern_id、contract_field、reason、suggested_workaround
- **ContractGap**：Contract 缺失映射所需的必要信息。如 output_columns 为空、aggregations 中的 function 不在白名单内、grouping_keys 为空做无分组聚合。severity=BLOCKING（阻断）或 WARN（警告）
- **SparkPlanMappingResult**：映射的完整状态——success（BLOCKING gap 或 unsupported 存在时=False）、spark_plan（成功时非空）、unsupported/gaps/warnings

**解决什么问题**

为 Phase 7 跨引擎验证提供阻断原因的结构化记录——人工审查者可以明确知道哪些模式无法映射及原因。

**在当前项目中的位置**

- `src/tianshu_datadev/spark/models.py:329-372` — 三种阻断模型定义
- `src/tianshu_datadev/spark/mapper.py` — 各 _map_* 函数中触发阻断条件

**简单例子**

```python
# 映射失败：Contract 无输入表
SparkPlanMappingResult(
    success=False, spark_plan=None,
    unsupported=[],
    gaps=[ContractGap(gap_id="gap_input_tables", contract_field="input_tables",
                       missing_info="Contract 不含任何输入表", severity="BLOCKING")],
    warnings=[]
)
```

**Owner 审查时应该问什么**

1. "UnsupportedPattern 和 ContractGap 的本质区别和触发条件各是什么？"
2. "如果 Phase 5 新增了对子查询的映射支持——需要修改哪些文件？"

---

## 58. ExpressionGuard（表达式安全守卫）

**一句话解释**：表达式安全校验模块——为 `input_expression` / `expression` 字段提供入站拒绝和编译白名单双闸门防护，防止成为 SQL 注入逃生口。

**是什么**

ExpressionGuard 是 Phase 4D 新增的安全组件（`src/tianshu_datadev/sql/expression_guard.py`）。它通过两道独立闸门保护表达式字段：

1. **入站侧闸门**（`mode="strict"` / `mode="silent"`）：拒绝含 `;`、`'`、`"`、`` ` `` 等禁止字符和 `--`、`/*` 等 SQL 注释模式的表达式
2. **编译器侧闸门**（`mode="compiler"`）：白名单正则 `^[\w一-鿿.+*\-/()%\s]+$`——只允许列引用 + 算术运算符 + 数字字面量 + 括号 + 空格，额外禁止 SELECT/INSERT/UPDATE/DELETE/DROP 等 SQL 关键字子串

两闸门独立运作，任一失效仍有另一道拦截。

**解决什么问题**

MetricDecl 的 `input_expression` 字段（Phase 4D 新增）允许程序员输入 `"quantity * unit_price"` 等多字段表达式——如果不做安全校验，该字段可能被注入 SQL 代码片段。ExpressionGuard 确保表达式字段不会成为 SQL Generation Boundary 的安全漏洞。

**在当前项目中的位置**

- `src/tianshu_datadev/sql/expression_guard.py:19` — `validate_input_expression()` 主函数
- `src/tianshu_datadev/sql/expression_guard.py:26-31` — 禁止字符和禁止模式常量
- `src/tianshu_datadev/sql/expression_guard.py:43-45` — 编译器白名单正则
- `src/tianshu_datadev/sql/expression_guard.py:49-52` — 编译器端禁止的关键词列表

**输入是什么**

`(expression: str, mode: Literal["strict", "silent", "compiler"])`——待校验的表达式字符串 + 校验模式。

**输出是什么**

`(is_valid: bool, error_message: str)`——`is_valid=True` 表示表达式通过当前闸门校验；`is_valid=False` 附带具体拒绝原因（如"表达式包含禁止字符：`;`"）。

**出错会导致什么风险**

如果 ExpressionGuard 的入站侧闸门失效（如模式取 `"silent"` 但下游未检查返回值）——含注入字符的表达式通过入站侧到达编译器。如果编译器侧白名单正则过于宽松（允许了 SQL 关键字）——表达式可能包含 SELECT 等操作。两道闸门中任一道失效，另一道仍能拦截。

**简单例子**

```python
from tianshu_datadev.sql.expression_guard import validate_input_expression

# ✅ 合法表达式
validate_input_expression("quantity * unit_price", mode="strict")
# → (True, "")

# ❌ 含注入字符
validate_input_expression("column; DROP TABLE orders", mode="strict")
# → (False, "表达式包含禁止字符：;")

# ❌ 含 SQL 关键字（编译器侧）
validate_input_expression("amount + SELECT_ME", mode="compiler")
# → (False, "表达式包含禁止的 SQL 关键字子串")
```

**Owner 审查时应该问什么**

1. "ExpressionGuard 的三模式（strict/silent/compiler）分别被谁调用？silent 模式的下游是否检查返回值？"
2. "编译器侧白名单正则是否覆盖了所有可能的 SQL 注入向量？如何验证？"

---

## 59. Spec Schema（规格模式）

**一句话解释**：ParsedDeveloperSpec 的字段体系——定义了程序员需求书可表达的业务语义边界，是 Builder 能生成什么 Plan 的上限。

**是什么**

Spec Schema 是 `ParsedDeveloperSpec` 及其子模型（`InputTableDecl`、`MetricDecl`、`DimensionDecl`、`JoinDecl`、`OutputSpecDecl`、`TimeRangeDecl` 等）共同构成的字段体系。它是 Parser 的输出格式、Builder 的输入格式——处于管线中"自然语言理解"与"代码生成"的分界线上。关键定位：

- **上游（Parser）**：LLM 推理从自然语言需求书中提取结构化字段，填入 Spec Schema
- **下游（Builder）**：硬编码规则读取 Spec Schema 字段，映射为 SqlBuildPlan 的 10 种 Step IR
- **Spec Schema ≠ Step IR**：Spec Schema 描述"要算什么"（业务语义），Step IR 描述"怎么算"（执行计划）

当前 Spec Schema 假设"一次聚合 + 一条 Join 链"的线性计算模型，不支持多步中间聚合、多分支并行计算、跨粒度依赖等 DAG 计算场景。

**解决什么问题**

为 Parser（LLM）和 Builder（硬编码）提供明确的接口契约——Parser 知道要产出什么字段，Builder 知道能从哪些字段读取业务意图。Spec Schema 的表达能力 = 系统能自动生成 SQL 的业务场景上限。

**在当前项目中的位置**

- `src/tianshu_datadev/developer_spec/models.py:535` — ParsedDeveloperSpec 顶层定义
- `src/tianshu_datadev/developer_spec/models.py:269` — MetricDecl（指标声明）
- `src/tianshu_datadev/developer_spec/models.py:339` — DimensionDecl（维度声明）
- `src/tianshu_datadev/developer_spec/models.py:346` — JoinDecl（Join 声明）
- `src/tianshu_datadev/developer_spec/models.py:356` — TimeRangeDecl（时间范围）
- `src/tianshu_datadev/developer_spec/models.py:376` — InputTableDecl（源表声明）
- `src/tianshu_datadev/developer_spec/models.py:393` — OutputColumnDecl（输出列声明）
- `src/tianshu_datadev/developer_spec/models.py:411` — OutputSpecDecl（输出规格）
- `src/tianshu_datadev/developer_spec/models.py:288` — InferredWindowMetric（窗口指标推断）
- `src/tianshu_datadev/developer_spec/models.py:305` — InferredComputedMetric（计算指标推断）
- `src/tianshu_datadev/developer_spec/models.py:319` — EnrichedSpec（丰富化规格）

**输入是什么**

不适用——Spec Schema 是类型定义，不是运行时组件。它的实例由 Parser 填充。

**输出是什么**

不适用——它是 Builder 的输入契约。

**出错会导致什么风险**

如果 Spec Schema 表达能力不足——程序员的需求书中合法的业务过程无法用任何字段组合表达 → Parser 无法产出完整结构化 Spec → Builder 缺失关键信息 → 生成的 SQL 不完整或语义错误。如果 Spec Schema 过于复杂——Parser（LLM）难以稳定产出符合 Schema 的 JSON → Schema 校验失败率升高 → 自动化率下降。

**简单例子**

当前 Spec Schema 能表达的："用户表 Join 订单表，按天汇总订单金额"——`metrics: [{aggregation: SUM, input_column: "amount"}]` + `dimensions: [{column_ref: "stat_date"}]`。

当前 Spec Schema 不能表达的："先按天汇总订单金额，再按月对日汇总求平均"——无法声明"第一步的产出是第二步的输入"。

**Owner 审查时应该问什么**

1. "ParsedDeveloperSpec 的字段总数是多少？其中哪些字段是程序员手写的、哪些是 SpecEnricher 推断的、哪些是 Builder 自己计算的？"
2. "如果要新增一个 Spec 字段（如 intermediate_aggregations），从定义到 Builder 消费，需要改哪些文件？"
3. "Spec Schema 和 Step IR（10 种 StepNode）之间的映射关系是否文档化了？还是只在 Builder 代码中隐式存在？"

---

## 60. ComputeStep（计算步骤声明）

**一句话解释**：Spec Schema 的新增字段——在 DeveloperSpec 中声明分步计算，每步定义输入来源、聚合逻辑和输出别名，替代当前扁平的 `metrics` 列表。

**是什么**

ComputeStep 是 Spec Schema 扩展的核心概念。它将当前"一个 metrics 列表 + 一个 dimensions 列表 = 一层聚合"的线性模型，扩展为可声明多步计算的 DAG 模型。每个 ComputeStep 包含：

- `step_name`：步骤名称（在 Spec 内唯一，用于下游引用）
- `source`：输入来源——`"input"`（源表直接计算）或 `step_name`（引用前面步骤的产出）
- `group_by`：此步骤的 GROUP BY 列
- `metrics`：此步骤的聚合指标
- `output_alias`：此步骤产出的中间表别名（供后续步骤 FROM 引用）

多个 ComputeStep 构成一个 DAG——后续步骤通过 `source` 引用前面步骤，Builder 据此生成 SqlProgram 的 statement 依赖链。

**解决什么问题**

覆盖阻断级缺陷 P0-1（多步聚合链）："先按天汇总→再按月求平均→最后 Join 用户表"这类需求可以声明为 3 个 ComputeStep，Builder 生成 3 个 SqlStatement 的链。

**在当前项目中的位置**

- **待实现**——当前 Spec Schema 中不存在此字段
- `src/tianshu_datadev/developer_spec/models.py` — 新增 ComputeStep 模型
- `src/tianshu_datadev/planning/sql_build_plan.py` — Builder 新增 `_build_from_compute_steps()`

**输入是什么**

程序员在 DeveloperSpec 中声明的分步计算规格（由 Parser 解析填充）。如果程序员未显式声明，由 SpecEnricher（LLM）从自然语言业务描述推断。

**输出是什么**

ComputeStep 列表——被 Builder 消费，生成对应数量的 SqlStatement（每个 step → 一个 statement，含 AggregateStep + ProjectStep）。

**出错会导致什么风险**

如果 ComputeStep 的 `source` 引用形成环——Builder 无法生成合法 SqlProgram → 拓扑排序失败 → 编译阻断。如果 `source` 引用了不存在的 step_name——类似缺失依赖，Validator 在 DAG 校验阶段拦截。

**简单例子**

```yaml
compute_steps:
  - step_name: daily_agg
    source: input
    group_by: [dt, user_id]
    metrics:
      - metric_name: daily_amount
        aggregation: SUM
        input_column: amount
    output_alias: daily_summary

  - step_name: monthly_avg
    source: daily_agg
    group_by: [month, user_id]
    metrics:
      - metric_name: avg_daily_amount
        aggregation: AVG
        input_column: daily_amount
    output_alias: monthly_summary
```

**Owner 审查时应该问什么**

1. "ComputeStep 的 source 除了引用 input 和其他 step_name，还能引用什么？"
2. "如果两个 ComputeStep 的 source 相同（都引用 daily_agg），Builder 如何处理？会产生重复计算吗？"

---

## 61. Spec DAG（规格依赖图）

**一句话解释**：多个 ComputeStep 之间的依赖关系图——描述中间计算结果如何被下游步骤引用，Builder 据此生成 SqlProgram 的 statement 依赖链。

**是什么**

Spec DAG 不是独立的数据结构，而是从 ComputeStep 列表的 `source` 字段推导出的有向无环图。节点 = ComputeStep + 源表，边 = "A 的产出被 B 引用"。与 SqlProgram 的 statement DAG 一一对应——每个 ComputeStep 生成一个 SqlStatement，Spec DAG 的边转换为 statement 的 `depends_on`。

关键约束：
- 必须是 DAG（无环）——拓扑排序可解
- 每个步骤的 `source` 必须已声明（无悬空引用）
- 最终输出步骤通过 OutputSpecDecl 指定

**解决什么问题**

为 Builder 提供明确的依赖信息——Builder 不需要重新推理步骤间的数据流，直接从 Spec DAG 生成 SqlProgram 的 statement 依赖链。

**在当前项目中的位置**

- **待实现**——依赖 ComputeStep 引入后自动由 Builder 推导
- `src/tianshu_datadev/planning/sql_build_plan.py:316` — build_multi() 可扩展为消费 Spec DAG

**输入是什么**

ComputeStep 列表——Builder 读取每个步骤的 `source` 字段，构建邻接表。

**输出是什么**

拓扑排序后的步骤执行顺序 + 中间表命名映射。

**出错会导致什么风险**

如果 Spec DAG 存在环——Builder 在拓扑排序时检测到循环依赖 → 必须抛出明确错误（类似 SqlProgram 的 CIRCULAR_DEPENDENCY）。如果 Spec DAG 存在孤立节点（无下游引用且非最终输出）——警告但允许（可能是冗余计算）。

**简单例子**

```
源表(orders) → daily_agg → monthly_avg → final_join(users)
                      ↘
                  weekly_avg ────────────→ final_join(users)
```

这个 DAG 有分支（daily_agg 被两个下游引用），Builder 生成 3 个 PRODUCER statement + 1 个 FINAL statement。

**Owner 审查时应该问什么**

1. "Spec DAG 和 SqlProgram 的 statement DAG 是否保证一一对应？在什么情况下会不对应？"
2. "如何处理菱形依赖（Diamond Dependency）——两个中间步骤被同一个下游步骤引用？"

---

## 62. PipelineStageIndicator（流水线阶段指示灯）

**一句话解释**：前端右上角的可折叠组件——展示流水线各阶段（解析→增强→构建→验证→编译→执行→契约→打包）的实时状态，失败时展开显示错误详情。

**是什么**

PipelineStageIndicator 是前端工作台（`App.tsx`）header 右侧的 React 组件。它以圆点 + 状态文字展示当前流水线执行状态：全部成功显示绿色 ✅ + "全部成功"，某阶段失败显示红色 ❌ + "XX失败"，处理中显示加载动画。点击可展开下拉面板，逐阶段列出各阶段状态（ok / failed / skipped），失败阶段附带 error_type 和 error_message。阶段覆盖：parser→enrich→build→validate→compile→execute→contract→package（execute 方法只走前 6 个阶段，run_all 走满 8 个）。数据来源：后端任何 200 响应中的 `pipeline_error` + `pipeline_stages` 字段——由 `App.tsx` 的 `runAction()` wrapper 自动提取。

**解决什么问题**

让用户在不打开浏览器 DevTools 或查看原始 JSON 响应的情况下，一眼看到流水线执行到哪个阶段、哪个阶段失败、失败原因是什么。结合 `ErrorDisplay`（错误条）和 `OpenQuestionPanel`（问题列表）形成三层反馈：阶段状态 → 错误类型 → 具体问题。

**在当前项目中的位置**

- `frontend/src/components/PipelineStageIndicator.tsx` — 组件实现（~113 行）
- `frontend/src/App.tsx:227-230` — header 右侧集成
- `frontend/src/App.tsx:103-106` — `runAction()` 自动提取 `pipeline_error` + `pipeline_stages`
- `src/tianshu_datadev/api/pipeline.py` — `_build_pipeline_stages()` 生成阶段状态列表

**输入是什么**

Props：`stages: StageInfo[]`（每项含 stage 英文名、status: ok/failed/skipped、可选的 error_type/error_message）+ `error: PipelineError | null`（含 stage、error_type、error_message）。

**输出是什么**

右上角可折叠 UI：折叠态——彩色圆点 + 状态摘要文字（如"验证失败"）；展开态——阶段列表（每行含状态图标 + 中文名 + 错误类型）+ 错误详情区域。

**出错会导致什么风险**

如果 `STAGE_CN` 映射缺少新增的阶段名——该阶段显示原始英文名而非中文名，用户可能不理解。如果 `pipeline_stages` 长度因 execute() 和 run_all() 不同而变化但前端未适配——用户可能看到不完整的阶段列表。当前前端通过自动检测阶段数量来兼容两种方法。

**简单例子**

用户在编辑器中输入 DeveloperSpec → 点击"全流程 Run-All" → Validator 发现大表缺少时间过滤（blocking 问题）→ 后端返回 200 + `validation_passed=false` + `pipeline_error`(stage="validate") + `pipeline_stages`(validate=failed) → 右上角圆点变红，显示"验证失败" → 用户点击展开 → 看到：解析 ✅ → 增强 ✅ → 构建 ✅ → 验证 ❌ ValidationBlocked → 编译 ⏭️ → 执行 ⏭️ → 契约 ⏭️ → 打包 ⏭️ → 下方 ErrorDisplay 显示红色卡片"[ValidationBlocked] — 管线阻断" → OpenQuestionPanel 列出具体 blocking 问题。

**Owner 审查时应该问什么**

1. "PipelineStageIndicator 的阶段列表是否区分 execute()（6 阶段）和 run_all()（8 阶段）？前端如何知道当前请求走的是哪个方法？"
2. "如果后端返回的 pipeline_error 中 error_message 包含中文或特殊字符——前端在 Windows 终端下的渲染是否正确？"

---

## 63. 项目缩写速查

| 缩写 | 全称 | 含义 |
|------|------|------|
| **DS** | DeveloperSpec | 程序员编写的半结构化开发需求书 |
| **PDS** | ParsedDeveloperSpec | Parser 解析后的结构化 IR（第一层） |
| **SM** | SourceManifest | 事实源注册表——表/列/类型/行数 |
| **RH** | RelationshipHypothesis | Join 关系的证据推理与定级结果 |
| **SBP** | SqlBuildPlan | 类型化 SQL 构建计划（第二层 IR） |
| **SP** | SqlProgram | 多语句 SQL 程序（含 DAG + _temp） |
| **DTC** | DataTransformContract | 业务规格——从 SBP/SP 确定性抽取 |
| **V1** | Contract V1 | Phase 3 Exit 交付——全字段 Contract |
| **ET** | ExecutionTrace | SQL 执行的完整追踪记录 |
| **FWP** | FinalWritePlan | 受控写入方案（分区 overwrite 审查材料） |
| **WV** | WriteValidator | 写入安全审查器——10 项检查 |
| **PV** | PerfValidator | 性能门禁——硬/软规则 |
| **SBPV** | SqlBuildPlanValidator | 计划验证器——8 项检查 |
| **JEL** | JoinEvidenceLevel | Join 置信度——STRONG/MEDIUM/WEAK/NONE |
| **LLM-GW** | LLM Gateway | LLM 统一调用入口 |
| **PM** | PromptManager | Prompt 模板版本管理 |
| **FLA** | FakeLLMAdapter | LLM 确定性模拟适配器 |
| **FP** | Pipeline（原 FakePipeline） | 不依赖真实 LLM 的完整流水线编排器 |
| **RP** | ReviewPackage | Code Review 材料包 |
| **RF** | ReviewFeedback | 结构化人工审查反馈 |
| **CB** | CompilerBackend | 编译器后端抽象接口 |
| **DDBC** | DuckDbSqlCompiler | DuckDB 方言 SQL 编译器 |
| **EG** | ExpressionGuard | 表达式安全校验——双闸门（入站拒绝 + 编译白名单） |
| **OQ** | OpenQuestion | 需程序员确认的结构化问题 |
| **HR** | HumanResolution | 程序员对 OpenQuestion 的回答或 HumanReview 判定 |
| **BC** | B/C 暂停条件 | Phase 的"停止继续往下做"的条件 |
| **HR** | HarnessReport | Phase 门禁评测报告 |
| **SE** | SpecEnricher | 指标推断层（规则/LLM 双模） |
| **ES** | EnrichedSpec | 丰富化规格（含推断指标） |
| **OCD** | OutputColumnDecl | 输出列声明（含 DSL 描述） |
| **MFD** | MetricFilterDecl | 条件聚合过滤声明（FILTER WHERE） |
| **SPK** | SparkPlan | Spark 侧类型化中间表示 |
| **SPM** | SparkPlanMapper | Contract→SparkPlan 确定性映射 |
| **PEQ** | PlanEquivalence | SQL/Spark 结构等价判定 |
| **UP** | UnsupportedPattern | 无法映射的变换模式 |
| **CG** | ContractGap | Contract 缺失的必要信息 |
| **SS** | Spec Schema | ParsedDeveloperSpec 字段体系——Builder 输入契约 |
| **CS** | ComputeStep | 分步计算声明——Spec 扩展核心字段 |
| **SDAG** | Spec DAG | 规格依赖图——从 ComputeStep.source 推导 |
| **PSI** | PipelineStageIndicator | 流水线阶段指示灯——前端右上角可折叠组件 |

---

> 本文基于项目代码基线（2026-07-03）更新，覆盖 63 个核心工程术语。Pipeline 条目已从 FakePipeline 迁移至当前 Pipeline 类——含 execute/run_all 两条入口、三条代码路径、RUNTIME_FAIL 阻断和统一 validation_passed 响应。新增 ExpressionGuard（§58）表达式安全守卫条目。
> 每个术语遵循"九件事"说明格式：名称→是什么→解决什么问题→项目位置→输入→输出→风险→例子→审查问题。
> 参考：[[AGENTS.md]] | [[03-sql-ir-and-compiler-plan]] | [[01-target-architecture]] | 各 Phase Roadmap 文档
> 关联文档：[[subquery-multihop-join-boundary_20260629_1500]] | [[phase-3-exit-report]]
