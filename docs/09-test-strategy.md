# 测试策略 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 目标

测试用于保护高风险契约和关键行为，不追求数量。pytest覆盖确定性逻辑和少量隔离集成；Prompt、模型、规模和性能评测进入Harness。

## 2. 当前基线

Phase 0实际已有22个pytest用例，超过原定`≤10`预算。进入Phase 1前不继续为Protocol属性和枚举组合增加测试；在具体Pydantic模型落地后，合并或删除低价值反射测试。

测试预算是评审阈值，不是为了达标而漏测安全边界。超过预算必须说明新增用例保护了哪项独立风险。

## 3. 分阶段预算

| 阶段 | 累计目标 | 重点 |
|------|----------|------|
| Phase 0.5 | 维持22，不新增文档措辞测试 | 契约校正，不改实现 |
| Phase 1 | 30-40 | 类型化IR、事实解析、SQL编译和DuckDB黄金路径 |
| Phase 1.2 | 45-55 | 性能契约注册表、REJECT/WARN 门禁规则、编译优化 pass 确定性 |
| Phase 1.5 | 55-65 | WindowExpr、WindowFrame、TopN、累计、LAG/LEAD、窗口性能规则和拒绝路径 |
| Phase 2 | 65-80 | Spark纯函数契约、AST安全、测试代码隔离和真实Spark运行 |
| Phase 3 | 80-100 | 关系快照、语义规范化、Comparator和MergePlan |
| Phase 4 | 90-115 | LangGraph路由、checkpoint、两轮返工和人工中断 |
| v1.0 | 100-150 | 前端/API边界和少量全链路黄金用例 |

## 4. pytest覆盖范围

- Pydantic/JSON Schema拒绝非法和额外字段。
- SQLPlan无自由SQL逃生口。
- SQL编译确定性和事实源拒绝。
- Spark AST安全、入口契约和隔离Executor。
- 测试代码安全校验。
- 关系一致快照和哈希。
- NULL、NaN、Decimal、时间、重复行和Join基数规范化。
- Comparator精确状态与MergePlan。
- LangGraph确定性路由、重试预算和恢复。
- 3至8条高价值端到端黄金项目书。

## 5. 不进入普通pytest的内容

- LLM全文输出稳定性。
- Prompt和模型版本排名。
- 大规模项目书组合。
- Spark全量性能和资源压测。
- 人工代码质量评分。
- 生产数据和生产连接测试。

这些进入Harness或独立环境测试。

## 6. 测试设计规则

1. 一个测试保护一个独立风险，不为每个Enum值机械复制。
2. 优先使用表驱动测试合并同类非法输入。
3. 不测试Python标准库、dataclass/Enum自身行为和私有实现细节。
4. 不对文档句子、完整LLM文本和大段生成代码做脆弱快照。
5. 安全测试覆盖攻击类别和绕过路径，而不是只测关键词。
6. 真实DuckDB/Spark集成测试使用小型版本化快照，不mock核心执行语义。
7. LLM Gateway在单元测试中使用确定性Fake Adapter；真实模型放Harness。
8. 每个E2E用例必须同时声明业务价值和它替代的低层重复测试。

## 7. 各阶段测试重点

### Phase 1

- RequirementIR、SubIntent、SQLPlan严格Schema。
- `where_sql`、`join_on`和自由表达式字段被拒绝。
- 单表及一个白名单Join黄金编译与执行。
- 未注册指标、列和Join拒绝。
- MergePlan不兼容粒度进入人工审查。

### Phase 1.2

- PerfContract 注册表完整性：`rule_id` 唯一性、`get_prompt_hints()` 非空、`get_rules_by_severity()` 正确过滤。
- REJECT 规则（PERF-001/002/004）通过和拒绝路径：fact 表时间过滤、Join key 类型、时间字段函数包裹。
- WARN 规则（PERF-005/006/007/008）通过和警告路径：明细 LIMIT、GROUP BY 基数、汇总表优先、Join 前聚合。
- PERF-003 注册但 no-op，测试推迟到 Phase 1.5。
- Compiler Pass 确定性：相同 SQLPlan 两次编译产生相同 SQL 和 SHA-256。
- 谓词规范化：`BETWEEN` / `DATE() =` / `strftime` 改写为标准 `>= AND <`。
- 门禁集成：REJECT 阻断 Compiler，WARN 不阻断。

### Phase 1.5

- `WindowExpr`和`WindowFrame`严格Schema。
- `over_sql`、`window_sql`、`expression: str`和额外字段被拒绝。
- 未注册分区键、排序键、输入列和指标被拒绝。
- `ROW_NUMBER`分区TopN、`SUM_OVER`日期累计、`LAG`或`LEAD`环比黄金路径。
- 非法frame、缺失`order_by`和窗口函数非法位置进入拒绝状态。
- 相同Window SQLPlan重复编译产生相同SQL和哈希。

### Phase 2

- `transform(inputs, params)`唯一入口。
- 禁止`spark.table`、read、Action、Sink、UDF、网络、文件和动态执行。
- Reviewer只输出Finding/Directive。
- Developer修订后重新验证。
- Tester代码也被拦截和隔离执行。
- 真实本地Spark运行一条黄金路径。

### Phase 3

- 多表锚点键级联抽样。
- Snapshot hash和EnvironmentManifest。
- 类型、NULL、NaN、Decimal、时间、multiset和容差。
- `NOT_EXECUTED`不能升级为一致。
- `CONSISTENT_SAMPLE`不等于`REVIEW_READY`。

### Phase 4

- 条件边只读取确定性状态。
- SQL修复只回到SQLPlan。
- Spark修复回到Developer。
- 0、1、2轮返工和超限人工审查。
- checkpoint恢复不重复副作用。

## 8. 质量门

每个阶段至少运行：

```powershell
python -m pytest tests -q
python -m ruff check .
git diff --check
```

阶段报告同时记录Harness基线是否变化，但Harness失败不得被pytest数量掩盖。

---

> Phase 0.5 校正 | 2026-06-22 | 全阶段测试事实源
