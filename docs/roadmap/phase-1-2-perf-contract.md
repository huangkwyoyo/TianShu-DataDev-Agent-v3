# Phase 1.2：SQL 性能契约与编译优化

## 目标

在 Phase 1 类型化 SQL 纵向切片稳定后，建立 SQLPlan 的性能门禁体系和编译器轻量优化管道。每条性能规则通过 Python 函数实现、Pydantic 注册表统一管理——不引入 DSL 或规则引擎。门禁分两级：硬规则 REJECT（阻断流水线），软规则 WARN（记录但不阻断）。

## 前置条件

- Phase 1 已完成严格 Pydantic 模型、Fact Catalog Adapter、SQL Validator（含 `register_perf_rules()` 接口）和 SQL Compiler（含 `register_passes()` 接口）。
- Phase 1 已预留 `PerfRule` 和 `CompilerPass` 注册表接口。
- `docs/10-performance-contract.md` 已定义 REJECT/WARN 两级门禁策略和编译器优化 pass 清单。

## 交付物

1. **PerfContract 注册表** — `PerfRule` / `PerfSeverity` / `PerfCheckResult` / `PerfValidationResult` Pydantic 模型，`get_prompt_hints()` 自动生成 LLM 方向性原则。
2. **性能门禁规则（8 条）** — 在 `perf_rules.py` 中实现，通过 `register_perf_rules()` 注入 Phase 1 的 SqlValidator。其中 7 条本阶段可执行（3 REJECT + 4 WARN），1 条（PERF-003 窗口排序）no-op 等待 Phase 1.5。
3. **编译优化 Pass（4 个）** — 列裁剪、谓词规范化、无用排序消除、常量折叠，通过 `register_passes()` 注入 Compiler。
4. **性能契约文档** — `docs/10-performance-contract.md`，将 DuckDB 优化手册中 LLM 需要知晓的提炼为方向性原则，Validator 需要检查的编码为规则表。
5. **测试** — PerfContract 完整性、每条规则通过/拒绝路径、Compiler Pass 确定性、门禁集成测试。

## 支持范围

### 门禁规则

| ID | 规则名 | 级别 | 说明 |
|----|--------|------|------|
| PERF-001 | `fact_table_requires_time_filter` | REJECT | fact 表查询必须包含时间范围过滤 |
| PERF-002 | `join_key_type_mismatch` | REJECT | Join 键类型必须一致 |
| PERF-003 | `window_missing_order_by` | REJECT | 窗口函数必须有 ORDER BY（本阶段 no-op，Phase 1.5 生效） |
| PERF-004 | `time_field_wrapped_in_function` | REJECT | WHERE 左侧时间字段不得被函数包裹 |
| PERF-005 | `detail_query_missing_limit` | WARN | 明细查询缺少 LIMIT |
| PERF-006 | `group_by_excessive_cardinality` | WARN | GROUP BY 字段超过 5 个 |
| PERF-007 | `fact_scan_prefer_summary` | WARN | 可用 dws 汇总表但选择了 fact 明细 |
| PERF-008 | `join_before_aggregation` | WARN | 大表 Join 后聚合建议先分别聚合 |

### 编译优化 Pass

1. **列裁剪** — 只输出 SQLPlan 实际引用的列，移除冗余 PROJECTION。
2. **谓词规范化** — `BETWEEN`、`DATE() =`、`strftime` 等模式统一改写为 `>= start AND < end` 范围形式。
3. **无用排序消除** — 移除非最终层的 ORDER BY 子句。
4. **常量折叠** — 编译时计算常量表达式。

## 禁止

- 不修改 Phase 1 已定义的 SQLPlan Schema 结构。
- 不引入 Cost-based Optimizer（CBO）、表统计信息或直方图。
- 不引入声明式规则 DSL 或 YAML 规则引擎——规则用 Python 实现。
- 不做运行时 profiling 数据反馈闭环（Phase 6/7）。
- 不影响 Spark 分支的性能约束逻辑（Phase 2 独立处理）。
- 不用 LLM 做性能决策——所有门禁规则和优化 pass 都是确定性的。

## 验收

1. `get_prompt_hints()` 从 PERF_RULES 注册表自动生成 LLM 方向性原则，无需手动维护两份内容。
2. 3 条 Phase 1.2 可执行 REJECT 规则（PERF-001/002/004）违反后返回 `PLAN_REJECTED`，SQLPlan 不进入 Compiler；PERF-003 注册但 no-op。
3. 4 条 WARN 规则违反后记录到 ExecutionTrace.perf_warnings，不阻断流水线。
4. 相同 SQLPlan 经 Compiler Passes 后两次编译产生字节一致 SQL 和 SHA-256。
5. 谓词规范化 pass 将 `BETWEEN` / `DATE() =` / `strftime` 改写为标准 `>= AND <` 形式。
6. 新规则注册只需：实现一个 Python 函数 + 在 PERF_RULES 追加一行 PerfRule 条目。
7. 累计 pytest 测试数（含 Phase 1）达到 45–55。

## 下一阶段依赖

Phase 1.5 通过相同的 `register_perf_rules()` 接口追加 4 条窗口相关性能规则（PERF-003 由 no-op 变为生效，新增 PERF-009/010/011）。Phase 2 复用 PerfContract 模型和 `get_prompt_hints()` 机制，Spark 分支的性能约束独立实现。

---

> Phase 1.2 规划 | 2026-06-23 | 实施依据为 `docs/superpowers/plans/2026-06-23-phase-1-2-perf-contract-design.md`
