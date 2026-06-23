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
4. **OptimizedSQLPlan 谱系模型** — 记录优化前后的 SQLPlan 血缘关系，支撑差异诊断和审计回溯。Pydantic 模型包含：
   ```python
   class OptimizedSQLPlan(BaseModel):
       plan_id: str                          # 当前优化后 SQLPlan 的唯一标识
       parent_plan_id: str                   # 优化前 SQLPlan 的标识
       optimization_round: int               # 优化轮次（从 1 开始）
       applied_directives: list[str]         # 已应用的优化指令 rule_id 列表
       rejected_directives: list[RejectedDirective]  # 被拒绝的优化指令及拒绝原因
       plan_sha256: str                      # 优化后 SQLPlan 的 SHA-256
       parent_sha256: str                    # 优化前 SQLPlan 的 SHA-256
       optimizer_version: str                # Compiler Pass 版本标识
       optimization_notes: str | None        # 额外说明（如为何某 directive 被拒绝）

   class RejectedDirective(BaseModel):
       rule_id: str                          # 被拒绝的规则 ID
       target_node_id: str                   # 目标节点
       reason: str                           # 拒绝原因（如"粒度不兼容"）
       rejected_by: str                      # 拒绝者（如"SemanticValidator"）
   ```
   每个优化后的 SQLPlan 必须持有对父计划的引用，形成完整优化链——从 Initial SQLPlan 到最终 Compiler 输出的每一步都可追溯。
5. **性能契约文档** — `docs/10-performance-contract.md`，将 DuckDB 优化手册中 LLM 需要知晓的提炼为方向性原则，Validator 需要检查的编码为规则表。
6. **测试** — PerfContract 完整性、每条规则通过/拒绝路径、Compiler Pass 确定性、门禁集成测试、OptimizedSQLPlan 谱系完整性。

## 支持范围

### 门禁规则

| ID | 规则名 | 级别 | 自动应用 | 说明 |
|----|--------|------|----------|------|
| PERF-001 | `fact_table_requires_time_filter` | REJECT | N/A（阻断） | fact 表查询必须包含时间范围过滤 |
| PERF-002 | `join_key_type_mismatch` | REJECT | N/A（阻断） | Join 键类型必须一致 |
| PERF-003 | `window_missing_order_by` | REJECT | N/A（阻断） | 窗口函数必须有 ORDER BY（本阶段 no-op，Phase 1.5 生效） |
| PERF-004 | `time_field_wrapped_in_function` | REJECT | N/A（阻断） | WHERE 左侧时间字段不得被函数包裹 |
| PERF-005 | `detail_query_missing_limit` | WARN | 否 | 明细查询缺少 LIMIT |
| PERF-006 | `group_by_excessive_cardinality` | WARN | 否 | GROUP BY 字段超过 5 个 |
| PERF-007 | `fact_scan_prefer_summary` | WARN | 否 | 可用 dws 汇总表但选择了 fact 明细 |
| PERF-008 | `join_before_aggregation` | WARN | 是（条件） | 大表 Join 后聚合建议先分别聚合 |

### 优化自动应用三级分类

性能规则不仅区分 REJECT/WARN 两级门禁，还区分优化是否可自动应用。这一分类直接映射到 `PerfRule.auto_apply_allowed` 字段，由确定性 Rewriter 在执行前判断：

| 分类 | `auto_apply_allowed` | 含义 | 示例 |
|------|---------------------|------|------|
| **可自动改写** | `True` | 规则触发后，确定性 Compiler Pass 可直接改写 SQLPlan，无需人工确认 | 列裁剪、谓词规范化（`BETWEEN` → `>= AND <`）、常量折叠、无用排序消除 |
| **谨慎自动改写** | `"conditional"` | 规则触发后，需同时满足附加条件（如 `pre_aggregation_allowed=True`、粒度兼容、Fact Resolver 确认基数）才可自动改写；否则记录 WARN 进入 ExecutionTrace | Join 前预聚合（PERF-008）——需 `JoinNode.pre_aggregation_allowed=True` 且输出粒度低于明细粒度 |
| **仅审查不自动改写** | `False` | 规则触发后只记录到 ExecutionTrace / ReviewReport，不自动修改 SQLPlan。改写必须由 Planner 或人工决定 | 明细缺少 LIMIT（PERF-005）、高基数 GROUP BY（PERF-006）、汇总表替代建议（PERF-007） |

设计原则：

- `auto_apply_allowed=True` 的规则必须满足**语义不变性**——改写前后的结果在业务上完全等价。所有 Compiler Pass 默认属于此类。
- `auto_apply_allowed="conditional"` 的规则改写**可能改变粒度或基数**，必须由 Fact Resolver 和 Semantic Validator 二次确认条件后，才允许 Rewriter 自动应用。
- `auto_apply_allowed=False` 的规则**不自动改写**——优化方向是建议性的，可能涉及表选择、业务口径等人判因素。

PerfRule Pydantic 模型：

```python
class PerfRule(BaseModel):
    rule_id: str                            # 如 "PERF-001"
    name: str                               # 规则名
    severity: PerfSeverity                  # REJECT | WARN
    category: PerfCategory                  # scan_pruning | predicate_pushdown | join_cardinality | ...
    description: str                        # 人类可读说明
    auto_apply_allowed: bool | Literal["conditional"]  # 是否可自动改写
    auto_apply_condition: str | None = None # conditional 时的附加条件说明
    check_function: str                     # 校验函数名（如 "check_fact_table_requires_time_filter"）
    applies_to_phase: str                   # 生效阶段，如 "1.2"
```

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

## EXPLAIN ANALYZE 反馈接口占位

运行时 profiling 反馈闭环不在本阶段实现（推迟到 Phase 6/7），但 Phase 1.2 预留以下接口骨架，避免后续阶段接入时大改 Compiler 和 Validator：

```python
class RuntimePerfReport(BaseModel):
    """DuckDB EXPLAIN / EXPLAIN ANALYZE 的结构化摘要——Phase 6/7 接入"""
    plan_id: str                              # 对应 SQLPlan 标识
    sql_artifact_sha256: str                  # 执行的 SQL artifact 哈希
    engine: str = "duckdb"
    engine_version: str
    snapshot_id: str
    explain_tree_json: str | None = None      # EXPLAIN 输出的 JSON 表示
    total_elapsed_ms: int | None = None       # 总执行耗时
    scanned_rows: int | None = None           # 扫描行数
    peak_memory_bytes: int | None = None      # 峰值内存
    operator_profiles: list[dict] | None = None  # 各算子的执行统计

class PerfRuleFeedback(BaseModel):
    """将 EXPLAIN 结果反馈到规则体系的记录——Phase 6/7 接入"""
    runtime_report_ref: str                   # RuntimePerfReport 引用
    rule_id: str                              # 相关规则 ID
    static_prediction: str                    # 静态阶段判断（REJECT / WARN / PASS）
    runtime_observation: str                  # 实际运行观察
    discrepancy: str | None = None            # 静态预测与运行时的不一致说明
    suggested_rule_change: str | None = None  # 建议的规则调整
```

Phase 1.2 只定义模型骨架和文件落盘路径（`generated/traces/runtime/`），不实现执行、收集和反馈逻辑。Compiler 和 PerfValidator 在架构上已预留 `runtime_feedback: RuntimePerfReport | None` 参数，当前调用方传入 `None` 即可。

## 验收

1. `get_prompt_hints()` 从 PERF_RULES 注册表自动生成 LLM 方向性原则，无需手动维护两份内容。
2. 3 条 Phase 1.2 可执行 REJECT 规则（PERF-001/002/004）违反后返回 `PLAN_REJECTED`，SQLPlan 不进入 Compiler；PERF-003 注册但 no-op。
3. 4 条 WARN 规则违反后记录到 ExecutionTrace.perf_warnings，不阻断流水线；其中 PERF-008 在满足 `pre_aggregation_allowed=True` 时可由 Compiler Pass 自动改写。
4. 相同 SQLPlan 经 Compiler Passes 后两次编译产生字节一致 SQL 和 SHA-256。
5. 谓词规范化 pass 将 `BETWEEN` / `DATE() =` / `strftime` 改写为标准 `>= AND <` 形式。
6. 新规则注册只需：实现一个 Python 函数 + 在 PERF_RULES 追加一行 PerfRule 条目（含 `auto_apply_allowed` 字段）。
7. OptimizedSQLPlan 正确记录 `parent_plan_id`、`applied_directives`、`rejected_directives` 和 `optimization_round`，形成完整优化链。
8. `RuntimePerfReport` 和 `PerfRuleFeedback` Pydantic 模型定义完成，Compiler 和 Validator 已预留 `runtime_feedback` 参数。
9. 累计 pytest 测试数（含 Phase 1）达到 45–55。

## 下一阶段依赖

Phase 1.5 通过相同的 `register_perf_rules()` 接口追加 4 条窗口相关性能规则（PERF-003 由 no-op 变为生效，新增 PERF-009/010/011）。Phase 2 复用 PerfContract 模型和 `get_prompt_hints()` 机制，Spark 分支的性能约束独立实现。

---

> Phase 1.2 规划 | 2026-06-23 | 实施依据为 `docs/superpowers/plans/2026-06-23-phase-1-2-perf-contract-design.md`
