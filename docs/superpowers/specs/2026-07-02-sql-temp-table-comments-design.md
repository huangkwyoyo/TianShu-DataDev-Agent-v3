# SQL Pipeline 临时表注释块——设计文档

> 状态：待审 | 日期：2026-07-02

---

## 1. 目标

SQL-first Pipeline 最终输出的代码中，每一个临时表前面带一段结构化注释，描述该段代码的操作和意图。最终输出语句前也带注释。注释块串联后，代码呈现清晰的处理过程和目标结果。

## 2. 输出规格

### 2.1 单语句（STANDALONE）

```sql
-- Step: Standalone Query: plan_a1b2c3d4
-- Intent: 单语句直接生成目标查询结果。
-- Operation: 从 orders 扫描 5 个字段，过滤条件：status = 'paid'，按 product_id, dt 分组，聚合 3 个指标（total_amount, order_count, avg_price），输出 5 列。
-- Inputs: orders
-- Output: (直接返回)

SELECT ...
```

### 2.2 多语句链（PRODUCER → FINAL）

```sql
-- Step: _temp_c7f3a1b2_0
-- Intent: 生成第 1 步中间结果，供下一步处理使用。
-- Operation: 从 orders 扫描 5 个字段，按 product_id, dt 分组，聚合 2 个指标（daily_sales, daily_orders）。
-- Inputs: orders
-- Output: _temp_c7f3a1b2_0

CREATE TEMP TABLE _temp_c7f3a1b2_0 AS
SELECT ...

-- Step: Final Output: result
-- Intent: 生成多步骤处理链的最终结果。
-- Operation: 从 _temp_c7f3a1b2_0 扫描 3 个字段，按 product_id 排序，输出 3 列。
-- Inputs: _temp_c7f3a1b2_0
-- Output: (最终结果集)

SELECT ...
```

### 2.3 ComputeSteps DAG

```sql
-- Step: daily_user_agg_temp
-- Intent: 生成用户日粒度中间结果，供后续月度汇总使用。下游消费者：monthly_user_agg
-- Operation: 从 dwd_user_order 扫描 5 个字段，按 user_id, dt 分组，聚合 2 个指标（order_count, pay_amount）。
-- Inputs: dwd_user_order
-- Output: daily_user_agg_temp

CREATE TEMP TABLE daily_user_agg_temp AS
SELECT ...

-- Step: monthly_user_agg_temp
-- Intent: 生成用户月度汇总中间结果，供最终输出使用。下游消费者：final_output
-- Operation: 从 daily_user_agg_temp 扫描 3 个字段，按 user_id, yyyyMM 分组，聚合 2 个指标（monthly_orders, monthly_pay）。
-- Inputs: daily_user_agg_temp
-- Output: monthly_user_agg_temp

CREATE TEMP TABLE monthly_user_agg_temp AS
SELECT ...

-- Step: Final Output: ads_user_month_summary
-- Intent: 生成用户月度汇总结果表，本步骤用于生成项目书声明的最终输出结果。
-- Operation: 从 monthly_user_agg_temp 扫描 4 个字段，输出 4 列。
-- Inputs: monthly_user_agg_temp
-- Output: ads_user_month_summary partition dt=yyyyMMdd

SELECT ...
```

### 2.4 硬约束

- 每个 `CREATE TEMP TABLE` 前必须有完整的 5 行注释块
- FINAL / STANDALONE 的 `Output`：有 `final_output_target` 时写真实目标，否则写 `(最终结果集)` / `(直接返回)`
- 注释块与 SQL 语句之间空一行
- 多语句 SQL 文件末尾包含 `cleanup_sql`（`DROP TABLE IF EXISTS`），与上方 SQL 空一行分隔

---

## 3. 数据模型变更

### 3.1 新增字段

```python
class SqlStatement(StrictModel):
    statement_id: str
    plan: SqlBuildPlan
    kind: StatementKind
    depends_on: list[str] = []
    produces: str | None = None
    intent: str | None = None  # 新增——Builder 填写的业务意图描述


class SqlProgram(StrictModel):
    program_id: str
    spec_id: str
    statements: list[SqlStatement]
    temp_tables: list[TempTableSpec] = []
    topological_order: list[str] = []
    final_output: str | None = None
    final_output_target: str | None = None  # 新增——FINAL 的真实输出目标（表名+分区等）
```

### 3.2 `PackageInputs` 新增

```python
class PackageInputs:
    ...
    sql_artifact: dict              # 单语句 SqlArtifact.model_dump()（向后兼容）
    sql_program: dict | None = None           # 新增——SqlProgram.model_dump()，含 intent
    sql_program_artifact: dict | None = None  # 新增——SqlProgramArtifact.model_dump()
```

---

## 4. 职责边界

```
Builder 产出                          Compiler 产出
───────────                          ─────────────
SqlStatement.intent (意图)     →     操作描述（从优化后 plan.steps 提取）
  "生成用户日粒度中间结果，              "从 dwd_user_order 扫描 5 个字段，
   供后续月度汇总使用"                  按 user_id, dt 分组，聚合 2 个指标"

                                  →  输入表（从 ScanStep/JoinStep 提取）
                                  →  输出表（从 statement.produces / program.final_output_target 取）
                                  →  拼接完整注释块，prepend 到 SQL 前
```

| # | 不可改变的边界 | 原因 |
|---|---------------|------|
| 1 | Compiler 不访问 `ParsedDeveloperSpec` | 只能从 `SqlStatement.intent` + `plan.steps` 提取信息，不反向读 spec |
| 2 | Builder 不深入 `plan.steps` 提取操作细节 | 操作提取是 Compiler 的职责 |
| 3 | `SqlProgram` 不引入编译相关结构体 | 仅 `SqlStatement` 加 `intent`，不新增 `StatementComment` 模型 |
| 4 | Packager 不反解析 SQL 注释 | 读取 `sql_program.statements[].intent` |
| 5 | `compute_sql_hash()` 函数签名不变 | 注释改变 SQL 文本，hash 自动变化——这是期望行为 |

---

## 5. Builder 层——intent 填写

### 5.1 `build_from_compute_steps(plans, spec, chain_id)`

可生成较完整 intent——有 `spec.compute_steps`、`output_alias`、`step_name`。

```python
# PRODUCER
intent = (
    f"生成{cs.step_name}中间结果，"
    f"供后续{', '.join(consumers) if consumers else '下游'}使用。"
    + (f"下游消费者：{', '.join(consumers)}" if consumers else "")
)

# FINAL
intent = f"本步骤用于生成项目书声明的最终输出结果。"

# final_output_target：从 spec.output_spec.table_name + 分区信息生成
final_output_target = self._derive_final_target(spec.output_spec)
```

### 5.2 `build_chain(plans, spec_hash, chain_id)`

不改签名——仅生成通用 intent。

```python
# PRODUCER
intent = f"生成第 {idx+1} 步中间结果，供下一步处理使用。"

# FINAL
intent = "生成多步骤处理链的最终结果。"
```

### 5.3 `build_single(plan, spec_hash)`

不改签名——仅生成通用 intent。

```python
intent = "单语句直接生成目标查询结果。"
```

### 5.4 措辞规范

- 使用"本步骤用于……"而非"已满足……"（避免暗示业务正确性已证明）
- 不使用绝对化措辞（"完全"、"精确"、"100%"）

---

## 6. Compiler 层

### 6.1 编译架构分层（关键修正）

当前 `compile()` 和 `compile_program()` 存在职责冲突——必须拆分为三层：

```
_compile_core(plan) → CoreCompileResult(raw_sql, optimized_plan, optimized_sql_plan)
        │
        ├─ compile(plan)
        │     用 _compile_core() → 基于优化后 plan 生成 STANDALONE 注释 → CompiledSql
        │     （不经过 _render_statement_comment()，使用独立 _render_standalone_comment()）
        │
        └─ compile_program(program)
              对每条语句：_compile_core(stmt.plan) → 按 StatementKind 包装
              → 基于 stmt + program + 优化后 plan 调用 _render_statement_comment()
              → prepend 上下文注释块 → ProgramCompiledSql
```

其中 `CoreCompileResult`：

```python
@dataclass(frozen=True)
class CoreCompileResult:
    raw_sql: str                      # 优化后的裸 SQL（无注释，无 CREATE TEMP TABLE 包装）
    optimized_plan: SqlBuildPlan      # 优化后的 SqlBuildPlan（供注释渲染使用）
    optimized_sql_plan: OptimizedSQLPlan  # 优化 Pass 记录（供调试/审计）
```

**`compile(plan)` 的 STANDALONE 注释路径与 `compile_program()` 不同**：前者没有 `SqlStatement` 和 `SqlProgram` 上下文，因此不经过 `_render_statement_comment()`。使用独立方法 `_render_standalone_comment(plan, optimized_plan) -> str`，其内部构造临时 `SqlStatement(kind=STANDALONE)` + 空 `SqlProgram` 的 transient 对象，仅用于驱动注释渲染，不暴露到 artifact。两者共享 `_render_comment_line()` / `_derive_operation_description()` / `_derive_input_tables()` 等底层 helper。

**注释必须 prepend 到整条最终 SQL 的最前面。对于 PRODUCER/CONSUMER：先完成 `CREATE TEMP TABLE ... AS raw_sql` 包装，再把注释块加到包装后整条 SQL 的最前面。禁止把注释插入 `CREATE TEMP TABLE ... AS` 与 `SELECT` 之间。**

调用顺序：

```text
1. _compile_core(plan)        → raw_sql（无注释，优化后的裸 SELECT）
2. 如果 PRODUCER/CONSUMER produces → 包装 CREATE TEMP TABLE AS raw_sql
3. _render_statement_comment() → 基于 stmt 上下文生成注释块
4. prepend 注释块 → 最终 SQL
5. compute_sql_hash()          → 基于最终 SQL（含注释）
```

### 6.2 注释安全清洗

```python
import re

@staticmethod
def _render_comment_line(label: str, value: str) -> str:
    """安全渲染单行注释——清洗控制字符、换行、注释破坏序列。

    规则：
    1. 替换 CR/LF 为空格
    2. 移除 C0 控制字符（0x00-0x1F，除 \\t 外）
    3. 连续 "--" 替换为 "- -"（防止注释提前终止）
    4. 首尾空白 trim
    5. 统一前缀 "-- {label}: "
    """
    cleaned = str(value)
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
    cleaned = re.sub(r"--+", "- -", cleaned)
    cleaned = cleaned.strip()
    return f"-- {label}: {cleaned}"
```

所有注释行通过此方法生成，**禁止直接 f-string 拼接 `intent` 或其他外部来源字符串**。

### 6.3 `_render_statement_comment()` ——生成完整注释块

```python
def _render_statement_comment(
    self, stmt: SqlStatement, program: SqlProgram, optimized_plan: SqlBuildPlan,
) -> str:
    """从 intent + 优化后 plan 生成 5 行注释块。

    Args:
        stmt: 当前语句（含 intent）
        program: 所属 SqlProgram（取 final_output_target）
        optimized_plan: 优化后的 SqlBuildPlan（保证注释与最终 SQL 一致）

    Returns:
        完整的 5 行注释块字符串
    """
    # ── Step 标签 ──
    if stmt.kind == StatementKind.STANDALONE:
        step_label = f"Standalone Query: {stmt.plan.plan_id[:8]}"
    elif stmt.kind == StatementKind.FINAL:
        step_label = f"Final Output: {program.final_output_target or 'result'}"
    else:
        step_label = stmt.produces or stmt.statement_id

    # ── Intent ──
    intent = stmt.intent or "（无描述）"

    # ── Operation ──
    operation = self._derive_operation_description(optimized_plan)

    # ── Inputs ──
    inputs = self._derive_input_tables(optimized_plan)

    # ── Output ──
    if stmt.kind == StatementKind.FINAL:
        output_str = program.final_output_target or "(最终结果集)"
    elif stmt.kind == StatementKind.STANDALONE:
        output_str = "(直接返回)"
    else:
        output_str = stmt.produces or "(中间结果)"

    lines = [
        self._render_comment_line("Step", step_label),
        self._render_comment_line("Intent", intent),
        self._render_comment_line("Operation", operation),
        self._render_comment_line("Inputs", inputs),
        self._render_comment_line("Output", output_str),
    ]
    return "\n".join(lines)
```

### 6.4 Operation 描述提取——从优化后 plan.steps

```python
def _derive_operation_description(self, plan: SqlBuildPlan) -> str:
    """从优化后的 plan.steps 生成中文操作描述短语串。

    按 step 顺序提取，连成一句话。跳过信息量低的 step。
    """
    parts: list[str] = []
    for step in plan.steps:
        desc = self._describe_single_step(step)
        if desc:
            parts.append(desc)
    if not parts:
        return "（无操作描述）"
    return "，".join(parts) + "。"

def _describe_single_step(self, step) -> str | None:
    """单 step → 中文短语。"""
    if isinstance(step, ScanStep):
        n = len(step.required_columns) if step.required_columns else 0
        table = step.table_ref
        return f"从 {table} 扫描 {n} 个字段" if n else f"从 {table} 扫描"
    elif isinstance(step, FilterStep):
        return "过滤条件：{简述}"  # 从 predicate 取关键信息
    elif isinstance(step, JoinStep):
        keys = ", ".join(f"{lk.column_name}={rk.column_name}" for lk, rk in step.join_keys)
        return f"与 {step.right_table_ref} 按 {keys} 关联"
    elif isinstance(step, AggregateStep):
        keys = ", ".join(gk.column_name for gk in step.group_keys)
        n_metrics = len(step.metrics)
        return f"按 {keys} 分组，聚合 {n_metrics} 个指标"
    elif isinstance(step, WindowStep):
        aliases = ", ".join(wexpr.alias for wexpr in step.window_exprs if wexpr.alias)
        return f"计算窗口函数：{aliases}" if aliases else "计算窗口函数"
    elif isinstance(step, ProjectStep):
        n = len(step.columns)
        return f"输出 {n} 列" if n else None
    elif isinstance(step, SortStep):
        cols = ", ".join(s.column for s in step.order_by)
        return f"按 {cols} 排序"
    elif isinstance(step, LimitStep):
        return f"限制 {step.limit} 行"
    elif isinstance(step, CaseWhenStep):
        return f"计算 {step.alias} 分类标签" if step.alias else "计算分类标签"
    return None

def _derive_input_tables(self, plan: SqlBuildPlan) -> str:
    """从 plan.steps 提取输入表名（ScanStep + JoinStep 去重）。"""
    tables: list[str] = []
    seen: set[str] = set()
    for step in plan.steps:
        if isinstance(step, ScanStep):
            if step.table_ref not in seen:
                tables.append(step.table_ref)
                seen.add(step.table_ref)
        elif isinstance(step, JoinStep):
            if step.right_table_ref not in seen:
                tables.append(step.right_table_ref)
                seen.add(step.right_table_ref)
    return ", ".join(tables) if tables else "（无输入表）"
```

### 6.5 COMPILER_VERSION

```python
COMPILER_VERSION = "1.1.0"  # 1.0.0 → 1.1.0：注释块进入 CompiledSql.sql
```

---

## 7. Packager 层

### 7.1 `_write_sql()` ——完整多语句 SQL

```python
def _write_sql(self, package_dir, inputs):
    sql_parts = []
    if inputs.sql_program_artifact:
        # 多语句：按 statement_order 拼接所有语句 SQL
        compiled = inputs.sql_program_artifact.get("compiled", {})
        statements = compiled.get("statements", [])
        for compiled_sql in statements:
            sql_parts.append(compiled_sql.get("sql", ""))
        # cleanup SQL
        cleanup = compiled.get("cleanup_sql", [])
        if cleanup:
            sql_parts.append("")
            sql_parts.extend(cleanup)
    else:
        # 单语句：向后兼容
        sql_parts.append(inputs.sql_artifact.get("compiled_sql", {}).get("sql", ""))
    sql_content = "\n\n".join(p for p in sql_parts if p)
    ...
```

### 7.2 Provenance 同步

`generate_provenance()` 新增字段：

```yaml
# 单语句（不变）
sql_artifact_hash: "abc123..."

# 多语句（新增）
compiled_program_sha256: "def456..."       # 完整 SQL 文件的 SHA-256
statement_sql_sha256:                      # 逐语句 SHA-256 列表
  - statement_id: "stmt_agg"
    sql_sha256: "ghi789..."
  - statement_id: "stmt_output"
    sql_sha256: "jkl012..."
```

**hash 一致性保障**：Provenance 中 `compiled_program_sha256` 必须与 `_write_sql()` 实际写入文件的 SHA-256 一致。建议把 SQL 拼接逻辑抽为 `_assemble_full_sql(sql_program_artifact) -> str`，Packager 和 Provenance 共用。

### 7.3 `review.md` 复用 intent

```python
# review_md 生成时，从 sql_program.statements 读取 intent
for stmt in sql_program.get("statements", []):
    intent = stmt.get("intent")
    if intent:
        lines.append(f"- **{stmt['statement_id']}**：{intent}")
```

**不反解析 SQL 注释——直接从模型字段读取。**

---

## 8. Pipeline 调用点变更

三条路径均显式设置新字段——ComputeSteps/多跳链路径传入实际值，单表路径传入 `None`：

```python
# ComputeSteps 路径 / 多跳链路径
package_inputs = PackageInputs(
    ...
    sql_artifact=...,                     # 保持（向后兼容）
    sql_program=sql_program.model_dump(),          # 新增
    sql_program_artifact=program_artifact.model_dump(),  # 新增
)

# 单表路径
package_inputs = PackageInputs(
    ...
    sql_artifact=artifact.model_dump(),   # 保持
    sql_program=None,                     # 新增——单表路径不传
    sql_program_artifact=None,            # 新增
)
```

---

## 9. 验收标准

| # | 验收项 | 验收方式 |
|---|--------|---------|
| 1 | 每个 `CREATE TEMP TABLE` 前有完整 5 行注释块 | 单元测试：`compile_program()` → 断言 `compiled.statements[i].sql` 以 `-- Step:` 开头 |
| 2 | FINAL 语句前有注释块，`Step` 行含 `Final Output:` | 同上 |
| 3 | STANDALONE 单语句前有注释块 | `compile()` → 断言 SQL 以注释块开头 |
| 4 | 注释块完整性——5 行不缺 | 正则断言 `-- Step: .*\n-- Intent: .*\n-- Operation: .*\n-- Inputs: .*\n-- Output: .*` |
| 5 | `SqlStatement.intent` 可被 ReviewPackage 直接读取（不反解析 SQL） | `review.md` 内容含 intent 文本，且 packager 代码中无 SQL 正则 |
| 6 | SQL hash 随注释变化（期望行为） | 加注释前后 `compute_sql_hash()` 结果不同 |
| 7 | 现有测试回归——SQL 字符串断言已更新 | `pytest` 全量：0 regression |
| 8 | `build_single` / `build_chain` 生成通用 intent | 断言 intent 非 None 且以 `"本步骤用于"` 或 `"生成"` 等通用措辞开头 |
| 9 | `build_from_compute_steps` intent 含下游消费者 | 断言 PRODUCER intent 含消费者 statement_id |
| 10 | 注释安全清洗：控制字符、`--`、换行被规整 | 传入含 `\n`、`\r`、`--` 的 intent → 断言输出不含控制字符和连续 `--` |
| 11 | Operation 描述基于优化后 plan | 构造含将被列裁剪的列的 plan → 断言注释中列数 = 优化后列数 |
| 12 | FINAL Output 当有 `final_output_target` 时写真实目标 | 构造含 `final_output_target` 的 SqlProgram → 断言注释 Output 行含真实目标 |
| 13 | Provenance 中 `compiled_program_sha256` = `_write_sql()` 写入的文件 SHA-256 | 端到端：包构建 → 读回文件算 hash → 与 provenance 字段对比 |

---

## 10. 实施计划——可追溯 / 可观测 / 可审计 / 可回归

| 步骤 | 文件 | 改动 | commit message |
|------|------|------|----------------|
| 10.1 | `sql_program.py` | `SqlStatement.intent` + `SqlProgram.final_output_target` 字段新增 | `feat(sql_program): SqlStatement 新增 intent 字段，SqlProgram 新增 final_output_target` |
| 10.2 | `sql_program.py` | Builder 三个方法的 intent 生成逻辑；`build_from_compute_steps()` 填写 `final_output_target`（`build_chain()` / `build_single()` 默认 `None`） | `feat(sql_program): Builder 构建时填写 intent 和 final_output_target` |
| 10.3 | `compiler.py` | `COMPILER_VERSION` → `"1.1.0"` | `chore(compiler): COMPILER_VERSION 升级至 1.1.0（注释块进入 CompiledSql）` |
| 10.4 | `compiler.py` | `_compile_core()` 抽取——纯编译（无注释） | `refactor(compiler): 抽取 _compile_core()，分离注释渲染与编译核心` |
| 10.5 | `compiler.py` | `_render_comment_line()` 安全清洗 helper | `feat(compiler): 新增注释安全清洗 _render_comment_line()` |
| 10.6 | `compiler.py` | `_render_statement_comment()` + `_derive_operation_description()` + `_derive_input_tables()` | `feat(compiler): 实现注释块渲染——Step/Intent/Operation/Inputs/Output` |
| 10.7 | `compiler.py` | `compile()` 调用注释渲染（Pass 后，hash 前） | `feat(compiler): compile() 单语句 prepend STANDALONE 注释` |
| 10.8 | `compiler.py` | `compile_program()` 调用注释渲染（逐语句上下文注释） | `feat(compiler): compile_program() 逐语句 prepend 上下文注释` |
| 10.9 | `models.py`（artifacts） | `PackageInputs` 新增 `sql_program` + `sql_program_artifact` | `feat(packager): PackageInputs 新增 sql_program / sql_program_artifact 字段` |
| 10.10 | `packager.py` | `_write_sql()` 完整多语句 SQL 拼接 + 抽 `_assemble_full_sql()` 共用 | `feat(packager): _write_sql() 输出完整多语句 SQL` |
| 10.11 | `packager.py` | `generate_provenance()` 新增 `compiled_program_sha256` + `statement_sql_sha256` | `feat(packager): provenance 记录完整多语句 SQL hash 和逐语句 hash` |
| 10.12 | `packager.py` | `review.md` 生成复用 `sql_program.statements[].intent` | `feat(packager): review.md 复用 SqlStatement.intent 生成步骤说明` |
| 10.13 | `pipeline.py` | 三条路径显式设置 `PackageInputs` 的 `sql_program` + `sql_program_artifact`（单表路径传 `None`） | `feat(pipeline): run_all() 传入 SqlProgram 元数据至 PackageInputs` |
| 10.14 | 现有测试 | 更新因注释插入变化的 SQL 字符串断言 | `test: 更新注释插入后的 SQL 断言` |
| 10.15 | `tests/sql/test_compiler_comment.py` | 新增 13 个测试用例——覆盖 §9 全部验收项 | `test: 新增编译器注释块测试套件（13 tests）` |

**提交方式**：每步一个 commit。最终 `pytest` 全量通过后合并。

---

## 11. 测试覆盖清单（新增文件）

`tests/sql/test_compiler_comment.py`：

| 测试函数 | 覆盖验收项 |
|----------|-----------|
| `test_comment_producer_has_full_block` | #1 |
| `test_comment_final_has_block` | #2 |
| `test_comment_standalone_has_block` | #3 |
| `test_comment_block_five_lines_complete` | #4 |
| `test_intent_accessible_from_review_md` | #5 |
| `test_sql_hash_differs_with_comment` | #6 |
| `test_build_single_intent_not_none` | #8 |
| `test_build_chain_intent_not_none` | #8 |
| `test_build_from_compute_steps_intent_includes_downstream` | #9 |
| `test_comment_sanitization_control_chars` | #10 |
| `test_comment_sanitization_double_dash` | #10 |
| `test_comment_sanitization_newlines` | #10 |
| `test_operation_based_on_optimized_plan` | #11 |
| `test_final_output_with_real_target` | #12 |
| `test_provenance_hash_matches_file_content` | #13 |
| `test_multi_statement_sql_concatenation` | 补充 |

---

## 12. 变更影响分析

| 影响范围 | 程度 | 说明 |
|---------|------|------|
| `SqlStatement` / `SqlProgram` 模型 | 低 | 新增 optional 字段，向后兼容 |
| Compiler 内部架构 | 中 | `_compile_core()` 抽离——不改变外部 API |
| `PackageInputs` | 低 | 新增 optional 字段 |
| Packager SQL 输出 | 中 | 多语句场景输出完整 SQL，单语句不变 |
| Provenance | 低 | 新增可选字段 |
| 现有测试 | 中 | SQL 字符串断言需更新 |
| Pipeline API 响应 | 低 | `compiled` 字段内容变化（SQL 含注释），结构不变 |
