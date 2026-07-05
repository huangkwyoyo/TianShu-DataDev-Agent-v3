# 测试策略 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 目标

测试用于保护高风险契约和关键行为，不追求数量。pytest 覆盖确定性逻辑和少量隔离集成；Prompt、模型、规模和性能评测进入 Harness。

## 2. 当前基线

Phase 0 实际已有 22 个 pytest 用例，超过原定 `≤10` 预算。进入 Phase 1 前不继续为 Protocol 属性和枚举组合增加测试；在具体 Pydantic 模型落地后，合并或删除低价值反射测试。

测试预算是评审阈值，不是为了达标而漏测安全边界。超过预算必须说明新增用例保护了哪项独立风险。

## 3. 分阶段预算

| 阶段 | 累计目标 | 重点 |
|------|----------|------|
| Phase 0.5 | 维持 22，不新增文档措辞测试 | 契约校正，不改实现 |
| Phase 1A | 30-40 | DeveloperSpec Parser、ParsedDeveloperSpec Schema、SourceManifest、SOURCE_CONFLICT、golden/拒绝 fixture |
| Phase 1B | 45-55 | RelationshipHypothesis 证据等级判定、字段名归一化、SqlBuildPlan 8 step Schema、WEAK/NONE 硬门禁 |
| Phase 1C | 55-65 | SQL Validator、Compiler 确定性、PerfContract REJECT/WARN 门禁、Compiler Pass 幂等、DuckDB Executor |
| Phase 2 | 65-80 | Code Review Package artifact schema、DataTransformContract-lite 抽取、provenance.yml、review.md 可读性 |
| Phase 3A | 75-90 | SqlProgram 多语句 DAG、_temp 生命周期、拓扑排序、循环依赖拒绝 |
| Phase 3B | 85-100 | CaseWhenStep、LabelRule 枚举覆盖、WindowExpr 白名单 8 种、窗口函数拒绝路径 |
| Phase 3C | 90-105 | FinalWritePlan、分区 overwrite 审查材料、非分区覆盖拒绝、CompilerBackend 接口 |
| Phase 4 | 100-130 | 真实 LLM 结构化输出、Harness 七维门禁、15 条 PERF 规则、安全评测六种攻击向量 |
| Phase 4.5 | 110-140 | REST API 请求/响应 Schema、CLI 确定性、Web 前端输入校验 |
| Phase 5 | 115-145 | SparkPlan IR Schema、DataTransformContract v1、SQL step 到 Spark step 映射、PlanEquivalence 规则 |
| Phase 6 | 125-160 | SparkDeveloper 语义标注、SparkCompiler 确定性代码生成、SparkCodeRenderer 安全渲染、Static Validator AST 硬门禁（E601-E608） |
| Phase 7 | 135-175 | PlanEquivalenceComparator、Snapshot Builder 关系一致抽取、ResultComparator 10 维度、差异诊断路由 |
| Phase 8 | 140-190 | LangGraph 编排壳、Graph State 边界、返工上限、Spark Harness、前端 Spark-first 视图 |

## 4. pytest 覆盖范围

- Pydantic/JSON Schema 拒绝非法和额外字段。
- DeveloperSpec Parser 允许/禁止宽松封闭表（golden + rejection fixture）。
- SqlBuildPlan / SqlProgram 无自由 SQL 逃生口。
- RelationshipHypothesis 证据等级正确判定和分流。
- SQL 编译确定性和 SourceManifest 事实源拒绝。
- Spark AST 安全、入口契约和隔离 Executor。
- 测试代码安全校验。
- 关系一致快照和哈希。
- NULL、NaN、Decimal、时间、重复行和 Join 基数规范化。
- Comparator 精确状态与 PlanEquivalence。
- LangGraph 确定性路由、重试预算和恢复。
- 3 至 8 条高价值端到端黄金 DeveloperSpec 用例。

## 5. 不进入普通 pytest 的内容

- LLM 全文输出稳定性。
- Prompt 和模型版本排名。
- 大规模 DeveloperSpec 组合。
- Spark 全量性能和资源压测。
- 人工代码质量评分。
- 生产数据和生产连接测试。

这些进入 Harness 或独立环境测试。

## 6. 测试设计规则

1. 一个测试保护一个独立风险，不为每个 Enum 值机械复制。
2. 优先使用表驱动测试合并同类非法输入。
3. 不测试 Python 标准库、dataclass/Enum 自身行为和私有实现细节。
4. 不对文档句子、完整 LLM 文本和大段生成代码做脆弱快照。
5. 安全测试覆盖攻击类别和绕过路径，而不是只测关键词。
6. 真实 DuckDB/Spark 集成测试使用小型版本化快照，不 mock 核心执行语义。
7. LLM Gateway 在单元测试中使用确定性 Fake Adapter；真实模型放 Harness。
8. 每个 E2E 用例必须同时声明业务价值和它替代的低层重复测试。

## 7. 各阶段测试重点

### Phase 1A

- DeveloperSpec Parser golden fixture 全部通过（6 项允许宽松）。
- DeveloperSpec Parser rejection fixture 全部正确拒绝（7 项禁止宽松）。
- ParsedDeveloperSpec、OpenQuestion、ParseWarning、SourceConflict 严格 Schema——extra 字段拒绝。
- `normalized_spec_hash` 确定性：相同输入两次解析 hash 一致。
- SourceManifest 字段来源标记正确（developer_spec / schema_registry / snapshot_profile）。
- SchemaRegistry 不可静默覆盖 DeveloperSpec 声明——SOURCE_CONFLICT 正确输出。
- REQUIRED 字段缺失生成 OpenQuestion(blocking=true)。

### Phase 1B

- RelationshipHypothesis 证据等级判定规则：STRONG/MEDIUM 自动采纳，WEAK/NONE 被拒绝。
- WEAK/NONE Join 被 Validator 拦截，不得进入 SqlBuildPlan 的 JoinSpec。
- 字段名归一化规则正确（大小写统一、驼峰转下划线、常见别名字典）。
- 每个 Join 输出完整证据链模板。
- SqlBuildPlan 8 step Schema（scan/filter/join/aggregate/project/case_when/sort/limit）——extra 字段拒绝。
- `raw_sql`、`where_sql`、`join_on: str`、`expression: str` 字段不存在或被拒绝。
- Fake Planner 确定性：相同 SourceManifest 两次生成相同 SqlBuildPlan。

### Phase 1C

- SQL Validator 正确拒绝未声明表字段、Join key 类型不一致、时间字段无过滤。
- Compiler 确定性：相同 SqlBuildPlan 两次编译产生相同 SQL 和 SHA-256。
- PerfContract REJECT 规则（PERF-001/002/004）违反后阻断 Compiler。
- PerfContract WARN 规则（PERF-005/006/007/008）违反后记录不阻断。
- PERF-003 窗口函数规则注册但 no-op（Phase 3B 生效）。
- Compiler Pass 幂等：列裁剪、谓词规范化（BETWEEN/DATE()=/strftime → >= AND <）、无用排序消除、常量折叠。
- OptimizedSQLPlan 正确记录优化链和 rejected_directives。
- DuckDB Executor 正确执行 Compiler 产物并输出 ExecutionTrace + ResultSummary。

### Phase 2

- Code Review Package 目录结构完整，artifact hash 可复现。
- DataTransformContract-lite 从 SqlBuildPlan 确定性抽取（不依赖 Phase 3A SqlProgram）。
- provenance.yml 记录所有模型版本和输入 hash。
- review.md 可被不熟悉系统的数据工程师读懂。
- 非法输入生成拒绝报告，不生成不完整审查包。

### Phase 3A

- SqlProgram 多语句 DAG 依赖正确——两步聚合、多表串联、扇出扇入。
- 循环依赖被拒绝。
- _temp 中间表生命周期：创建、使用、清理。
- 拓扑排序确定性。
- 多语句 Executor：失败语句阻断后续，cleanup 正确执行。

### Phase 3B

- CaseWhenStep 标签枚举覆盖检查——枚举值不在 DeveloperSpec 声明中被拒绝。
- WindowExpr 白名单 8 种函数通过，非法函数被拒绝。
- WindowFrame 非法参数和窗口函数嵌套被拒绝。
- 窗口函数非法位置（WHERE 子句）被拒绝。

### Phase 3C

- FinalWritePlan 日期分区 overwrite 方案正确生成。
- 全表 overwrite、无分区 overwrite、UPDATE/DELETE/MERGE 被拒绝。
- CompilerBackend 抽象接口占位就绪。

### Phase 4

- 真实 LLM 结构化输出通过率可测量。
- Harness 七维度门禁 REJECT 项全部通过。
- 15 条 PERF 规则 REJECT/WARN/PERF_FEEDBACK 分流正确。
- 六种攻击向量（Prompt 注入、SQL 注入、Schema extra 突破、未声明引用、Join 错误推理、写入越权）全部拦截。
- Join 推理质量"零容忍"维度：高风险漏报率 = 0、WEAK/NONE 被采纳 = REJECT、缺证据链 = REJECT。

### Phase 4.5

- REST API 请求/响应 Schema 正确校验。
- CLI 和 Web 同输入同输出。
- 非法输入展示结构化拒绝原因，不崩溃。

### Phase 5-8

Phase 5-8 已全部实施并回归通过（2026-07-04）。当前测试基线：552 passed / 11 skipped。详细状态见 `docs/current-state-and-verification-status.md`。

- Phase 5：PlanEquivalence 规则、DataTransformContract v1——已完成。
- Phase 6：受控 PySpark DSL（9 种 step 编译）、Static Validator（E601-E608 全错误码）——已完成。
- Phase 7：PlanComparator（9 种 step 逻辑对比）、PhysicalVerifier（DuckDB ↔ PySpark）、Snapshot Builder——已完成。
- Phase 8：Orchestrator（6 阶段编排）、SparkReviewPackage、Harness 5 维度评测——已完成。

## 8. 质量门

每个阶段至少运行：

```powershell
python -m pytest tests -q
python -m ruff check .
git diff --check
```

阶段报告同时记录 Harness 基线是否变化，但 Harness 失败不得被 pytest 数量掩盖。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 全阶段测试事实源
