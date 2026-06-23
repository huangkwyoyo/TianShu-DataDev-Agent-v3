# Phase 1.5：Window Function SQLPlan扩展

## 目标

在Phase 1类型化SQL纵向切片稳定后，开放受控开窗函数能力。LLM可以生成含窗口语义的结构化SQLPlan，但不得生成`OVER (...)`、窗口函数调用或任何SQL文本片段。

## 前置条件

- Phase 1已完成严格SQLPlan Schema、Fact Catalog Adapter、SQL Compiler、DuckDB Executor和黄金路径。
- SQL Compiler已经具备确定性渲染、SQL AST Safety Validation和artifact哈希。
- TransformationContract能够声明窗口指标所需的输入列、分区键、排序键和输出列。
- **Phase 1.2 的 PerfContract 注册表、性能门禁规则和编译优化 pass 已完成**——本阶段通过 `register_perf_rules()` 追加 4 条窗口相关性能规则（PERF-003 窗口排序检查 + Phase 1.5 新增 3 条）。

## AST契约

Phase 1.5新增以下封闭节点：

```text
WindowExpr
├── function: WindowFunction
├── input: ColumnRef | Literal | None
├── partition_by: list[ColumnRef]
├── order_by: list[SortSpec]
├── frame: WindowFrame | None
├── alias: str
└── metric_ref: str | None

WindowFrame
├── frame_type: ROWS | RANGE
├── start: FrameBoundary
└── end: FrameBoundary

FrameBoundary
├── kind: UNBOUNDED_PRECEDING | PRECEDING | CURRENT_ROW | FOLLOWING | UNBOUNDED_FOLLOWING
└── offset: int | None
```

`WindowExpr`只能出现在`SelectNode`输出表达式或受控派生列中。禁止将窗口函数放入`WHERE`，禁止嵌套窗口函数，禁止窗口函数引用未注册字段。

## 支持函数白名单

首批只开放：

- `ROW_NUMBER`
- `RANK`
- `DENSE_RANK`
- `LAG`
- `LEAD`
- `SUM_OVER`
- `AVG_OVER`
- `COUNT_OVER`

其他窗口函数、任意函数名和自由表达式均进入`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。

## 黄金场景

Phase 1.5只承诺覆盖以下高价值场景：

- 分组内排序取TopN。
- 按日期累计`SUM_OVER`。
- 按业务键分区计算`ROW_NUMBER`。
- 使用`LAG`或`LEAD`生成环比字段。
- 分区内`AVG_OVER`和`COUNT_OVER`窗口指标。

## 禁止场景

- 递归、嵌套或多层窗口表达式。
- 窗口函数出现在`WHERE`、`JOIN ON`或任意自由谓词中。
- 窗口函数与任意子查询、CTE、DDL、DML组合。
- 未声明`order_by`的排名、偏移和累计窗口。
- 未注册字段、未注册指标或未声明分区键。
- 任意`window_sql`、`over_sql`、`expression: str`或`raw_sql`字段。

## Validator规则

Validator必须在编译前完成：

1. 拒绝Schema额外字段和自由SQL字段。
2. 校验`WindowFunction`属于白名单。
3. 校验`partition_by`、`order_by`和`input`均来自事实源绑定列。
4. 校验排名、偏移和累计函数的`order_by`存在且稳定。
5. 校验`WindowFrame`边界合法，禁止负数offset和反向frame。
6. 校验窗口输出alias不与已有输出列冲突。
7. 校验窗口表达式不出现在禁止位置。
8. 校验窗口相关性能门禁规则通过 Phase 1.2 的 PerfContract 注册表执行——PERF-003（窗口排序检查，本阶段由 no-op 变为生效）、PERF-009（窗口禁止位置）、PERF-010（嵌套窗口）、PERF-011（无分区警告）。

## Compiler规则

Compiler只从`WindowExpr`确定性渲染DuckDB SQL：

```text
WindowExpr
→ WindowSpec normalization
→ DuckDB SQL AST node
→ Renderer输出SQL
→ SQL AST Safety Validation
→ artifact hash
```

相同规范化SQLPlan、Fact Catalog版本和compiler版本必须产生字节一致SQL与哈希。

## 测试策略

Phase 1.5新增测试只保护窗口函数独立风险，不为每个枚举机械复制。

必须覆盖：

- Schema拒绝`over_sql`、`window_sql`、`expression: str`和额外字段。
- 未注册分区键、排序键、输入列和指标被拒绝。
- `ROW_NUMBER`分区TopN黄金路径。
- `SUM_OVER`日期累计黄金路径。
- `LAG`或`LEAD`环比黄金路径。
- 非法frame、缺失`order_by`和窗口函数非法位置进入拒绝状态。
- 窗口性能门禁规则（PERF-003/009/010/011）的通过和拒绝路径。
- 相同Window SQLPlan重复编译产生相同SQL和哈希。

## 验收标准

1. SQLPlan Schema中不存在任何窗口SQL字符串字段。
2. `ROW_NUMBER`、`RANK`、`DENSE_RANK`、`LAG`、`LEAD`、`SUM_OVER`、`AVG_OVER`和`COUNT_OVER`可通过AST表达。
3. 非法窗口函数名、非法frame、未注册字段和缺失`order_by`在编译前被拒绝。
4. 相同Window SQLPlan重复编译产生字节一致SQL和哈希。
5. 至少覆盖TopN、累计、LAG/LEAD三类黄金用例。
6. 不支持的窗口语义进入`UNSUPPORTED_PLAN`或`HUMAN_REVIEW`。
7. Spark分支仍不得查看SQL文本或SQLPlan实现。
8. 窗口相关性能门禁规则通过 Phase 1.2 PerfContract 接口注册并生效。

## 下一阶段依赖

Phase 2复用窗口增强后的TransformationContract和输出Schema，但SparkDeveloper仍只读取业务契约，不读取SQLPlan或SQL文本。双引擎窗口语义差异在Phase 3通过SemanticCompatibilityPolicy和Comparator处理。

---

> Phase 0.5 校正 | 2026-06-23 | Phase 1.5 实施依据
