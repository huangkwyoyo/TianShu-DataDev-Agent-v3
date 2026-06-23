# Phase 1.2：SQL 性能契约与编译优化 — 设计文档

> 文档状态：设计稿 | 2026-06-23 | 待审批后转入 writing-plans

## 1. 背景与问题

### 1.1 现状

TianShu DataDev Agent v3 的核心安全边界是：**LLM 不直接生成 SQL 文本，SQL 由 Python 确定性编译器渲染**。现有路线图按 SQL 能力线性展开——Phase 1（单表+Join）→ Phase 1.5（开窗）→ Phase 2（Spark）→ Phase 3（双引擎验证）。

每个 Phase 的交付物定义（文档 03 §7.3）已规定了"每开放一种 SQL 能力，必须配套 Schema + Validator + Compiler + 测试 + 拒绝路径"。但这条规则只覆盖了**语义正确性**（表/列/指标是否存在、Join 路径是否合法），没有覆盖**性能安全性**（是否扫描了过多数据、Join 基数是否失控、窗口排序是否缺少约束）。

### 1.2 触发条件

用户提供了一份 DuckDB SQL 优化手册（25 条规则），覆盖了扫描优化、Join 优化、聚合优化、窗口优化、表设计等维度。需要将这些规则融入 IR 架构。

### 1.3 已确认的设计决策

| # | 决策 | 结论 |
|---|------|------|
| 1 | 范围 | 路线图级重规划——新增独立 Phase 1.2，调整后续阶段依赖 |
| 2 | 定位 | Phase 1.2 插在 Phase 1 与 Phase 1.5 之间 |
| 3 | 门禁策略 | 两级分类：硬规则 REJECT（阻断流水线），软规则 WARN（记录但不阻断） |
| 4 | 编译器深度 | 轻量规范化：列裁剪、谓词规范化、无用排序消除、常量折叠 |
| 5 | 架构模式 | 混合架构（Python 实现规则 + Pydantic 注册表管理元数据） |

## 2. 路线图变更

### 2.1 调整后的路线图

```
Phase 0.5  架构契约校正（当前）
    ↓
Phase 1    类型化 SQL 纵向切片（单表+Join，不含开窗）
    │       新增交付：Validator 注册表接口 + Compiler pass 管道接口
    │       状态：SQLPlan Schema 天然禁止 SELECT *、CROSS JOIN
    ↓
Phase 1.2  SQL 性能契约与编译优化  ← 新增
    │       交付：PerfContract 注册表、8 条性能门禁规则、4 个编译优化 pass
    │       依赖：Phase 1 的严格 Pydantic 模型和 Validator/Compiler 骨架
    ↓
Phase 1.5  Window Function SQLPlan 扩展
    │       增量：PerfContract 新增 3 条窗口相关性能规则
    │       前置条件增加：Phase 1.2 的 PerfContract 已完成
    ↓
Phase 2    受控 PySpark 多角色切片（不变）
    ↓
Phase 3    双引擎验证（不变）
    ↓
  ...
```

### 2.2 现有文档变更清单

| 文件 | 变更类型 | 变更内容 |
|------|----------|----------|
| `docs/roadmap/phase-1-sql-vertical-slice.md` | 修改 | 交付物增加"Validator 注册表接口"和"Compiler pass 管道接口"；前置条件增加"性能契约已定义" |
| `docs/roadmap/phase-1-5-window-sqlplan.md` | 修改 | 前置条件增加"Phase 1.2 PerfContract 已完成"；交付物增加"窗口相关性能规则 3 条" |
| `docs/roadmap/phase-2-spark-multi-agent.md` | 修改 | 前置条件增加"Phase 1.2 性能门禁不阻塞 Python→SQL 路径" |
| `docs/roadmap/phase-1-2-perf-contract.md` | **新增** | Phase 1.2 完整路线图（交付物、禁止事项、验收标准） |
| `docs/09-test-strategy.md` | 修改 | 测试预算表增加 Phase 1.2（累计 45-55）；Phase 1.2 测试重点 |
| `docs/03-sql-ir-and-compiler-plan.md` | 修改 | §6 编译流程图增加 PerfValidator 和 Compiler Passes 两个节点 |
| `AGENTS.md` | 修改 | §2 SQL Generation Boundary 增加性能门禁条款 |

## 3. 架构设计

### 3.1 整体数据流

```
                    ┌─────────────────────────┐
                    │  SQL Planner (LLM)        │
                    │                          │
                    │  system prompt 包含:       │
                    │  get_prompt_hints() 输出   │
                    │  (8 条方向性原则)          │
                    └────────────┬────────────┘
                                 │ SQLPlan (Pydantic, extra="forbid")
                                 ▼
              ┌──────────────────────────────────────┐
              │  Phase 1: Schema Validation           │
              │  Pydantic extra="forbid" 拒绝未知字段  │
              │  天然禁止: SELECT *, CROSS JOIN,       │
              │  自由 SQL 字符串                       │
              └──────────────────┬───────────────────┘
                                 │ SQLPlan √
                                 ▼
              ┌──────────────────────────────────────┐
              │  Phase 1: Semantic Validator          │
              │  事实源绑定、引用完整性、Join 白名单    │
              └──────────────────┬───────────────────┘
                                 │ SQLPlan √
                                 ▼
 ┌───────────────────────────────────────────────────────────┐
 │  Phase 1.2: PerfValidator                                  │
 │                                                            │
 │  for rule in PERF_RULES:                                   │
 │      result = rule.check(sqlplan, fact_catalog)            │
 │      if result.severity == REJECT and not result.passed:   │
 │          → PLAN_REJECTED（阻断流水线）                      │
 │      if result.severity == WARN and not result.passed:     │
 │          → 记录到 ExecutionTrace.perf_warnings（不阻断）    │
 │                                                            │
 │  输出: (SQLPlan, PerfValidationResult)                      │
 └──────────────────────────┬────────────────────────────────┘
                             │ SQLPlan √（可能带 WARN）
                             ▼
 ┌───────────────────────────────────────────────────────────┐
 │  Phase 1 + 1.2: Compiler                                   │
 │                                                            │
 │  SQLPlan → sqlglot AST                                     │
 │    → Pass 1: 列裁剪（只输出引用列）                         │
 │    → Pass 2: 谓词规范化（>= start AND < end）              │
 │    → Pass 3: 无用排序消除（非最终层 ORDER BY 移除）         │
 │    → Pass 4: 常量折叠                                     │
 │    → Renderer 输出 DuckDB SQL 字符串                       │
 └──────────────────────────┬────────────────────────────────┘
                             │ 优化后 SQL 字符串
                             ▼
 ┌───────────────────────────────────────────────────────────┐
 │  Phase 1: Safety Validation                                │
 │  二次扫描生成的 SQL AST，确认无自由 SQL 逃生口              │
 └───────────────────────────────────────────────────────────┘
```

### 3.2 设计原则

1. **规则用 Python 实现，元数据用 Pydantic 注册表管理**——不建 DSL，不引入规则引擎。每条规则的检查逻辑是纯 Python 函数，但规则的名称、级别、消息、提示通过 `PerfRule` 模型集中注册。
2. **PerfValidator 是附加校验层，不修改 SQLPlan**——它只读 SQLPlan 并返回检查结果，不做 AST 变换。
3. **Compiler Passes 是 AST → AST 变换，必须是幂等的**——同一个 SQLPlan 两次编译必须产生相同的优化后 SQL 和哈希。
4. **注册表是单一事实源**——`PERF_RULES` 列表同时驱动 Validator（检查逻辑）、Prompt 生成（`get_prompt_hints()`）、文档生成和测试参数化。

## 4. 核心组件设计

### 4.1 PerfContract（性能契约注册表）

**文件位置**：`src/tianshu_datadev/sql/contracts/perf_contract.py`

```python
"""SQL 性能契约——性能门禁规则注册表与元数据"""

from enum import Enum
from typing import Callable
from pydantic import BaseModel

class PerfSeverity(str, Enum):
    """性能规则的严重级别"""
    REJECT = "REJECT"   # 硬门禁——违反后阻断流水线，SQLPlan 不进入 Compiler
    WARN = "WARN"       # 软警告——记录到 ExecutionTrace，不阻断流水线

class PerfRule(BaseModel):
    """单条性能规则的完整定义"""
    model_config = {"arbitrary_types_allowed": True}

    rule_id: str                          # 唯一标识，如 "PERF-001"
    name: str                            # 规则简称，如 "fact_table_requires_time_filter"
    severity: PerfSeverity               # REJECT | WARN
    message: str                         # 违反时的人类可读消息
    hint: str                            # 给 LLM 的方向性建议（一句话中文）
    applies_to: list[str]                # 适用的 SQLPlan 类型标签，如 ["QueryPlan", "SelectNode"]
    check_fn: Callable  # (SQLPlan, FactCatalog) -> PerfCheckResult

class PerfCheckResult(BaseModel):
    """单条规则的检查结果"""
    rule_id: str
    rule_name: str
    passed: bool
    severity: PerfSeverity
    detail: str | None = None           # 违反时的具体信息
    suggestion: str | None = None       # 修复建议

class PerfValidationResult(BaseModel):
    """一次性能门禁检查的完整结果"""
    plan_id: str
    rejected: list[PerfCheckResult] = []   # REJECT 且未通过的规则
    warned: list[PerfCheckResult] = []     # WARN 且未通过的规则
    passed: list[str] = []                 # 通过的规则 ID

    @property
    def is_blocked(self) -> bool:
        """是否有 REJECT 级规则未通过"""
        return len(self.rejected) > 0

    @property
    def has_warnings(self) -> bool:
        """是否有 WARN 级规则未通过"""
        return len(self.warned) > 0

def get_prompt_hints(rules: list[PerfRule] | None = None) -> str:
    """从规则注册表提取 LLM 方向性原则（用于 SQL Planner system prompt）"""
    if rules is None:
        rules = PERF_RULES
    lines = []
    for rule in rules:
        prefix = "【必须】" if rule.severity == PerfSeverity.REJECT else "【建议】"
        lines.append(f"{prefix} {rule.hint}")
    return "\n".join(lines)

def get_rules_by_severity(severity: PerfSeverity) -> list[PerfRule]:
    """按级别过滤规则"""
    return [r for r in PERF_RULES if r.severity == severity]
```

### 4.2 PerfValidator（性能门禁）

**文件位置**：集成到 `src/tianshu_datadev/sql/validator/` 中，规则实现在 `perf_rules.py`。

Phase 1 的 `SqlValidator` 预留两个钩子：

```python
# Phase 1 实现
class SqlValidator:
    """SQLPlan 校验器——语义校验 + 性能门禁的统一入口"""
    def __init__(self, fact_catalog: FactCatalog):
        self._fact_catalog = fact_catalog
        self._semantic_rules: list = []
        self._perf_rules: list[PerfRule] = []

    def register_perf_rules(self, rules: list[PerfRule]) -> None:
        """注册性能门禁规则——Phase 1.2 调用此方法注入规则"""
        self._perf_rules.extend(rules)

    def validate(self, sqlplan: SQLPlan) -> ValidationResult:
        # 1. 语义校验
        semantic_result = self._validate_semantic(sqlplan)
        if semantic_result.is_blocked:
            return semantic_result

        # 2. 性能门禁校验
        perf_result = self._validate_performance(sqlplan)
        return ValidationResult(
            plan_id=sqlplan.plan_id,
            semantic=semantic_result,
            performance=perf_result,
        )

    def _validate_performance(self, sqlplan: SQLPlan) -> PerfValidationResult:
        results = PerfValidationResult(plan_id=sqlplan.plan_id)
        for rule in self._perf_rules:
            try:
                check = rule.check_fn(sqlplan, self._fact_catalog)
            except Exception as exc:
                # 规则自身异常不阻断——降级为 WARN 并记录异常信息
                check = PerfCheckResult(
                    rule_id=rule.rule_id, rule_name=rule.name,
                    passed=False, severity=PerfSeverity.WARN,
                    detail=f"规则执行异常: {exc}",
                )
            if not check.passed:
                if rule.severity == PerfSeverity.REJECT:
                    results.rejected.append(check)
                else:
                    results.warned.append(check)
            else:
                results.passed.append(rule.rule_id)
        return results
```

### 4.3 Compiler Passes（编译优化）

**文件位置**：`src/tianshu_datadev/sql/compiler/passes.py`

```python
"""轻量编译优化 pass——AST → AST 的确定性变换"""

from abc import ABC, abstractmethod
import sqlglot.expressions as exp

class CompilerPass(ABC):
    """编译优化 pass 基类"""
    @abstractmethod
    def apply(self, ast: exp.Expression) -> exp.Expression:
        """对 sqlglot AST 应用优化变换，返回变换后的 AST"""
        ...

class ColumnPruningPass(CompilerPass):
    """列裁剪——移除 SQLPlan 未引用的列。只输出 SELECT 中实际需要的列。"""
    def apply(self, ast: exp.Expression) -> exp.Expression:
        # 实现：遍历 AST，移除未被 SELECT/WHERE/JOIN/GROUP BY 引用的列
        ...

class PredicateNormalizePass(CompilerPass):
    """谓词规范化——时间范围统一展开为 >= start AND < end 形式。"""
    def apply(self, ast: exp.Expression) -> exp.Expression:
        # 实现：识别 BETWEEN / DATE() = '...' 等模式，重写为标准范围形式
        ...

class DeadOrderByEliminationPass(CompilerPass):
    """无用排序消除——移除中间 CTE/子查询中不影响最终结果的 ORDER BY。"""
    def apply(self, ast: exp.Expression) -> exp.Expression:
        # 实现：识别非最外层的 ORDER BY 子句并移除
        ...

class ConstantFoldingPass(CompilerPass):
    """常量折叠——在编译时计算常量表达式（如 DATE '2024-01-01' + INTERVAL 1 DAY）。"""
    def apply(self, ast: exp.Expression) -> exp.Expression:
        # 实现：使用 sqlglot 的 constant folding 能力或自行实现
        ...
```

Phase 1 的 `SqlCompiler` 预留 pass 管道：

```python
class SqlCompiler:
    def __init__(self, fact_catalog: FactCatalog):
        self._fact_catalog = fact_catalog
        self._passes: list[CompilerPass] = []

    def register_passes(self, passes: list[CompilerPass]) -> None:
        """注册编译优化 pass——Phase 1.2 调用"""
        self._passes.extend(passes)

    def compile(self, sqlplan: SQLPlan) -> CompiledSql:
        # 1. SQLPlan → sqlglot AST
        ast = self._build_ast(sqlplan)
        # 2. 运行优化 pass 管道
        for p in self._passes:
            try:
                ast = p.apply(ast)
            except Exception:
                # 优化 pass 失败不阻断——跳过该 pass
                continue
        # 3. Renderer 输出 SQL
        sql = ast.sql(dialect="duckdb")
        # 4. Safety Validation
        self._safety_check(ast)
        # 5. 生成 artifact 与哈希
        return CompiledSql(sql=sql, ast=ast, hash=self._hash(sql))
```

## 5. 首批规则目录（Phase 1.2：8 条）

### 5.1 REJECT 级规则（4 条）

| ID | 规则名 | 检查内容 | 违反消息 |
|----|--------|----------|----------|
| PERF-001 | `fact_table_requires_time_filter` | SQLPlan 的 `primary_table` 以 `fact_` 开头时，`predicates` 中必须包含至少一个时间范围条件 | "事实表 {table} 的查询缺少时间范围过滤，必须添加 WHERE 时间条件" |
| PERF-002 | `join_key_type_mismatch` | JoinSpec 左键和右键的事实源类型必须一致 | "Join 键 '{left}' ({type_l}) 与 '{right}' ({type_r}) 类型不一致，禁止隐式 CAST" |
| PERF-003 | `window_missing_order_by` | 排名类（ROW_NUMBER/RANK/DENSE_RANK）、偏移类（LAG/LEAD）、累计类（SUM_OVER/AVG_OVER）窗口函数的 WindowExpr 必须有 order_by。**Phase 1.2 注册但为 no-op（SQLPlan 尚无 WindowExpr 节点），Phase 1.5 WindowExpr 落地后自动生效** | "窗口函数 {func} 必须有 ORDER BY 子句，否则结果不确定" |
| PERF-004 | `time_field_wrapped_in_function` | Predicate 的 left 为时间字段时，不得被函数调用包裹（如 `DATE(col)`、`strftime(col)`） | "WHERE 条件中时间字段 {col} 被函数包裹，会阻止分区裁剪，请改用 >= AND < 范围过滤" |

### 5.2 WARN 级规则（4 条）

| ID | 规则名 | 检查内容 | 违反消息 |
|----|--------|----------|----------|
| PERF-005 | `detail_query_missing_limit` | 无聚合函数（COUNT/SUM/AVG/...）的查询必须带 LIMIT | "明细查询缺少 LIMIT，建议限制返回行数" |
| PERF-006 | `group_by_excessive_cardinality` | GROUP BY 字段数超过 5 个时警告 | "GROUP BY 包含 {n} 个字段，粒度可能过细，确认业务需求" |
| PERF-007 | `fact_scan_prefer_summary` | 聚合查询的 primary_table 为 fact 表，但存在对应的 dws 汇总表可以满足需求 | "聚合查询建议使用 {dws_table} 汇总表，而非扫描 {fact_table} 明细表" |
| PERF-008 | `join_before_aggregation` | SQLPlan 同时包含 JoinSpec 和 AggregateSpec，且 Join 的右表也是大表时建议先聚合再 Join | "大表 Join 后聚合可能导致中间结果膨胀，建议先按目标粒度分别聚合再 Join" |

### 5.3 Phase 1.5 增量规则（3 条，预留）

| ID | 规则名 | 检查内容 | 违反消息 |
|----|--------|----------|----------|
| PERF-009 | `window_in_forbidden_position` | WindowExpr 不得出现在 WHERE/JOIN ON/HAVING 条件中 | "窗口函数不能出现在 WHERE 或 JOIN ON 条件中" |
| PERF-010 | `nested_window_function` | 禁止一个 WindowExpr 的 input 引用另一个 WindowExpr | "不支持嵌套窗口函数" |
| PERF-011 | `window_partition_by_unbounded` | partition_by 为空且表超过阈值行数时警告 | "窗口函数缺少 PARTITION BY，将在全表上排序" |

## 6. 状态流转与错误处理

### 6.1 PerfValidator 决策树

```
SQLPlan 进入 PerfValidator
│
├─ 逐条执行 PERF_RULES（先 REJECT 级，再 WARN 级）
│
├─ 任何 REJECT 级规则未通过
│   → 状态: PLAN_REJECTED（复用文档 03 §9 已有状态）
│   → SQLPlan 不进入 Compiler
│   → 返回全部 PerfCheckResult（含 detail + suggestion）
│   → 调用方决定：退回 SQL Planner 修正 或 转入 HUMAN_REVIEW
│
├─ 全部 REJECT 级通过，部分 WARN 级未通过
│   → 状态: PLAN_VALIDATED
│   → SQLPlan 正常进入 Compiler
│   → WARN 结果写入 ExecutionTrace.perf_warnings
│   → Code Review Package 汇总所有 WARN
│
└─ 全部规则通过
    → 状态: PLAN_VALIDATED
    → perf_warnings 为空列表
```

### 6.2 与现有状态枚举的关系

**不新增状态枚举**。PerfValidator 复用以下已有状态：

| 场景 | 状态 | 来源 |
|------|------|------|
| 硬规则违反 | `PLAN_REJECTED` | 文档 03 §9 |
| 软规则违反 | `PLAN_VALIDATED` + trace 记录 | 复用；WARN 不改变状态 |
| 规则执行异常 | 降级为 `WARN`，不阻断 | 防御性设计 |

### 6.3 ExecutionTrace 扩展

Phase 1 落地 ExecutionTrace 为 Pydantic 模型时直接包含此字段：

```python
class ExecutionTrace(BaseModel):
    # ... Phase 1 现有字段 ...
    perf_warnings: list[PerfCheckResult] = []  # Phase 1.2 新增
```

### 6.4 Compiler Pass 的错误处理

优化 pass **不允许失败**。策略：

```python
for p in self._passes:
    try:
        ast = p.apply(ast)
    except PassNotApplicable:
        # 跳过，不改变 AST，不记录错误
        continue
```

## 7. 测试策略

### 7.1 测试预算

Phase 1.2 新增约 15-20 个测试用例，累计目标从 Phase 1 的 30-40 提升到 **45-55**。

### 7.2 必须覆盖

**PerfContract 完整性**（3 个）
- `PERF_RULES` 列表中每条规则都有唯一的 `rule_id`
- `get_prompt_hints()` 为非空字符串
- `get_rules_by_severity()` 按级别正确过滤

**REJECT 级规则**（每条至少 2 个：通过 + 拒绝）
- PERF-001：fact 表带/不带时间过滤 → 通过/PLAN_REJECTED
- PERF-002：Join key 类型一致/不一致 → 通过/PLAN_REJECTED
- PERF-003：窗口函数有/无 ORDER BY → 通过/PLAN_REJECTED（**测试推迟到 Phase 1.5**——Phase 1.2 的 SQLPlan 尚无 WindowExpr 节点，规则注册但 no-op）
- PERF-004：时间字段被/不被函数包裹 → 通过/PLAN_REJECTED

**WARN 级规则**（每条至少 2 个：通过 + 警告）
- PERF-005：明细查询有/无 LIMIT → 通过/WARN
- PERF-006：GROUP BY ≤5 / >5 字段 → 通过/WARN
- PERF-007：使用 dws / 使用 fact → 通过/WARN
- PERF-008：Join 前已聚合 / 未聚合 → 通过/WARN

**Compiler Pass 确定性**（2 个）
- 相同 SQLPlan 两次编译产生相同 SQL 字符串和 SHA-256
- 谓词规范化：`BETWEEN` / `DATE() = '...'` / `strftime` 三种模式均被改写为 `>= AND <`

**门禁集成**（1-2 个）
- REJECT 规则失败后，Compiler 不被调用
- WARN 规则失败后，Compiler 正常执行且结果哈希一致

### 7.3 不进入 pytest 的内容

- 规则的具体实现正确性由集成测试覆盖，不对每条规则的内部逻辑写单元测试
- 不测试 LLM 是否遵循了 `get_prompt_hints()` 的建议
- 不测试规则自身的性能开销

## 8. Phase 1.2 交付物清单

| # | 交付物 | 文件 | 类型 |
|---|--------|------|------|
| 1 | PerfContract 注册表 | `src/tianshu_datadev/sql/contracts/perf_contract.py` | 新增 |
| 2 | 性能门禁规则（8 条） | `src/tianshu_datadev/sql/validator/perf_rules.py` | 新增 |
| 3 | 编译优化 Pass（4 个） | `src/tianshu_datadev/sql/compiler/passes.py` | 新增 |
| 4 | Validator 注册表接口 | `src/tianshu_datadev/sql/validator/__init__.py` | 修改（Phase 1 配合） |
| 5 | Compiler Pass 管道接口 | `src/tianshu_datadev/sql/compiler/__init__.py` | 修改（Phase 1 配合） |
| 6 | ExecutionTrace perf_warnings | `src/tianshu_datadev/ir/` | 修改（Phase 1 配合） |
| 7 | PerfContract 测试 | `tests/test_perf_contract.py` | 新增 |
| 8 | 性能门禁规则测试 | `tests/test_perf_rules.py` | 新增 |
| 9 | 编译优化 Pass 测试 | `tests/test_compiler_passes.py` | 新增 |
| 10 | Phase 1.2 路线图 | `docs/roadmap/phase-1-2-perf-contract.md` | 新增 |
| 11 | 性能规范文档 | `docs/10-performance-contract.md` | 新增 |
| 12 | 受影响文档更新 | 见 §2.2 变更清单 | 修改 |

## 9. Phase 1.2 禁止事项

- 不修改 Phase 1 已定义的 SQLPlan Schema 结构
- 不引入 Cost-based Optimizer（CBO）、表统计信息或直方图
- 不引入声明式规则 DSL 或 YAML 规则引擎
- 不做运行时 profiling 数据反馈闭环（Phase 6/7 的事）
- 不影响 Spark 分支的性能约束逻辑（Phase 2 独立处理）
- 不用 LLM 做性能决策——所有门禁规则和优化 pass 都是确定性的

## 10. Phase 1.2 验收标准

1. `get_prompt_hints()` 可从 PERF_RULES 注册表自动生成 LLM 方向性原则列表，无需手动维护两份内容
2. 3 条 Phase 1.2 可执行的 REJECT 级规则（PERF-001/002/004）违反后返回 `PLAN_REJECTED`；PERF-003 已注册但 no-op，Phase 1.5 WindowExpr 落地后生效
3. 4 条 WARN 级规则违反后记录到 ExecutionTrace，不阻断流水线
4. 相同 SQLPlan 经过 Compiler Passes 后两次编译产生字节一致的 SQL 和 SHA-256 哈希
5. 谓词规范化 pass 将 `BETWEEN`、`DATE() =`、`strftime` 三种模式改写为标准 `>= AND <` 形式
6. 累计 pytest 测试数（含 Phase 1）达到 45-55
7. 新规则注册只需：实现一个 Python 函数 + 在 PERF_RULES 列表追加一行 PerfRule 条目
8. Phase 1.5 窗口规则可通过相同的 `register_perf_rules()` 接口追加

## 11. 待决策项

以下问题留到 writing-plans 阶段或实施过程中决策：

1. **PERF-007（fact vs dws 选择）的实现方式**：需要 Fact Catalog 提供"表级别映射"（如 `gold.fact_trips` → `gold.dws_daily_trip_summary`）。这个映射是硬编码在代码中，还是从 TianShu 事实源读区？
2. **PERF-008（大表检测）的阈值**：什么算"大表"？行数阈值从哪来——Fact Catalog 元数据、Snapshot 采样、还是配置常量？
3. **Compiler Pass 的执行顺序**：4 个 pass 之间是否有依赖关系（如常量折叠应在谓词规范化之前），还是独立可重排？

---

> 设计完成 | 2026-06-23 | 待审批后转入 superpowers:writing-plans
