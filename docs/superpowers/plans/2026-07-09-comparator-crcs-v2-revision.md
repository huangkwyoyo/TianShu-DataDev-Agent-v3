# COMPARATOR 机制 CRCS v2.0 修复计划（修订版 v2）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** 修复 COMPARATOR 机制中 3 个 C 类验证边界缺陷和 1 个 B 类逻辑缺陷，补全测试矩阵以证明"不再误判等价"。

**Architecture:** 见 `src/tianshu_datadev/spark/plan_comparator.py`（扁平化+步骤分类）和 `src/tianshu_datadev/spark/plan_equivalence.py`（9 条对比规则）。

**Tech Stack:** Python 3.12+, Pydantic, SparkPlan IR, SqlBuildPlan IR

---

## Global Constraints

- Comparator 是 SQL/Spark 双链验证的核心边界——每个修复必须考虑验证误判风险
- `UNSUPPORTED_COMPARISON` 不可比较字段必须返回显式拒绝状态，不得静默判 EQUIVALENT
- 状态传播链：`EquivalenceVerdict.UNSUPPORTED_COMPARISON` → `PlanComparator._map_status()` 输出 `ComparisonStatus.LOGIC_UNSUPPORTED` → Pipeline mapping `"HUMAN_REVIEW"` → `SparkReviewBuilder._compute_review_ready()` 返回 `False`
- **C 类验收测试必须使用 `PlanComparator.compare()` + 真实 `SqlBuildPlan`/`SparkPlan` 模型实例**，dict 层单测仅做补充
- 每个 C 类任务完成后必须经过 code review 方可进入下一个任务
- `_flatten_*_step` 方法是 SQL/Spark 两侧的公共正规化层，**不得通过 `pop()` 移除对方侧 `compare_*_steps` 仍依赖的键**

---

## Phase 1：Comparator 语义设计确认（C类）

### 决策项 D2：Spark 侧统一接入 `_normalize_step_dict`

**当前：**
```python
# plan_comparator.py:1212-1221 —— Spark 侧不经过扁平化
@staticmethod
def _extract_spark_step_data(spark_plan: SparkPlan) -> list[dict[str, Any]]:
    return [step.model_dump(mode="json", exclude_none=True) for step in spark_plan.steps]

# plan_comparator.py:645-661 —— SQL 侧经过扁平化
@staticmethod
def _extract_sql_step_data(sql_plan: SqlBuildPlan) -> list[dict[str, Any]]:
    steps = []
    for step in sql_plan.steps:
        step_dict = step.model_dump(mode="json", exclude_none=True)
        step_dict = PlanComparator._normalize_step_dict(step_dict)  # ✅
        ...
        steps.append(step_dict)
    return steps
```

**决策：复用同一个 `_normalize_step_dict`。** 需验证 Spark 侧 `model_dump` 输出的各步骤键名兼容性。

| 步骤 | Spark 侧键名 | 兼容性 | 调整 |
|------|-------------|--------|------|
| join | `step_type="read"` → `_normalize_step_dict` 对 `read` 无操作 → 原样返回 | ✅ | `compare_plans` 在 `_SQL_TYPE_TO_NORMALIZED` 中将 `"read"` 归一化为 `"scan"` |
| filter | `step_type="filter"`, `left`/`operator`/`right` 已在顶层 | ✅ | `_flatten_filter_step` pop `predicate` 返回空 dict，不覆盖已有字段 |
| project | `step_type="project"`, `columns` 已为 `[{column_name, alias}]` 无嵌套 `expression` | ✅ | 无 `"expression"` 键 → `col.get("expression", {})` 返回空 dict |
| aggregate | `step_type="aggregate"`, `group_keys` 为字符串列表，`metrics.function` 直接用 | ✅ | `"aggregation" in flat_m` 为 False → 不触发重命名 |
| **join** | `step_type="join"`, **`left_alias`/`right_alias`** | **⚠️** | `_flatten_join_step` 从 `join_keys` 读取，pop 不到 → 走兼容分支 |
| **window** | `step_type="window"`, `frame_type`/`frame_start`/`frame_end` 分离字符串 | **⚠️⚠️** | `_flatten_window_step` 需在 `expressions` block 处理 frame 分离字段 |
| sort | `step_type="sort"`, `order_by` 已扁平 | ✅ | 无操作 |
| limit | `step_type="limit"`, `limit`/`offset` 在顶层 | ✅ | 无操作 |
| **case_when** | `step_type="case_when"`, **`branches`** 而非 `cases` | **⚠️** | `_flatten_case_when_step` pop `cases` 时 Spark dict 中无此键 → 不执行 labels 提取；但 `compare_case_when_steps` 从 Spark 侧读 `branches` 而非 `labels`，所以 labels 为空不会影响 Spark 侧 |

**关键约束：`_flatten_join_step` 不得 `pop` Spark 侧 `left_alias`/`right_alias`。**

```python
# _flatten_join_step 中 Spark 兼容分支（NOT pop）：
if "left_table_ref" not in result or not result.get("left_table_ref"):
    result["left_table_ref"] = str(result.get("left_alias", ""))    # .get() 不 pop
if "right_table_ref" not in result or not result.get("right_table_ref"):
    result["right_table_ref"] = str(result.get("right_alias", ""))  # .get() 不 pop
# compare_join_steps 仍从 left_alias/right_alias 读取 Spark 侧数据
```

### 决策项 F1：Window 统一可比较结构

**当前 frame 字段差异：**

| 侧 | 序列化格式 | 示例 |
|----|-----------|------|
| SQL（`WindowFrame` dict） | `"frame": {"frame_type": "ROWS", "start": {"kind": "UNBOUNDED_PRECEDING", ...}, "end": {"kind": "CURRENT_ROW", ...}}` | `_render_frame_boundary` 处理 `FrameBoundary dict` |
| Spark（`SparkWindowExpr` 分离字段） | `"frame_type": "rows", "frame_start": "unbounded_preceding", "frame_end": "current_row"` | **纯字符串**，直接 `.upper()` 即可 |

**决策：在 `_flatten_window_step` 的 `expressions` block 中检测 `frame_type` 键，就地合并。不得 `del` 任何不存在的键。**

```python
# _flatten_expr_frame(expr: dict) → 在 expressions block 和 window_exprs block 中复用
def _flatten_expr_frame(expr: dict) -> None:
    """统一窗口表达式中的 frame 字段为合并字符串格式。"""

    # Spark 分离格式：frame_type/frame_start/frame_end 是纯字符串
    frame_type = expr.pop("frame_type", None)
    if frame_type is not None:
        ft = str(frame_type).upper()
        fs = str(expr.pop("frame_start", "unbounded_preceding")).upper()
        fe = str(expr.pop("frame_end", "current_row")).upper()
        expr["frame"] = f"{ft}:{fs}:{fe}"
        return

    # SQL WindowFrame dict 格式
    frame = expr.pop("frame", None)
    if isinstance(frame, dict):
        ft = str(frame.get("frame_type", "RANGE")).upper()
        fs = PlanComparator._render_frame_boundary(frame.get("start", {}))
        fe = PlanComparator._render_frame_boundary(frame.get("end", {}))
        expr["frame"] = f"{ft}:{fs}:{fe}"
```

### 决策项 D1：CASE WHEN condition 比较策略

**当前：** `_flatten_case_when_step` 丢弃 condition。`compare_case_when_steps` 不比较 condition。

**决策：Option A（降级 UNSUPPORTED_COMPARISON）作为安全基线。**

`_flatten_case_when_step` 必须保留 `has_conditions` 和 `condition_comparison_supported` 标记：

```python
# _flatten_case_when_step 中 labels 提取后追加：
# 检测 condition 存在性（保留标记供 compare_case_when_steps 消费）
has_conditions = False
if isinstance(raw_cases, list):
    for c in raw_cases:
        if isinstance(c, dict):
            cond = c.get("condition")  # Predicate | None
            raw_cond = c.get("raw_condition")  # SqlRawExpression | None
            if cond is not None or raw_cond is not None:
                has_conditions = True
                break
result["has_conditions"] = has_conditions
result["condition_comparison_supported"] = False  # Phase 2 将改为 True
```

`compare_case_when_steps` 消费此标记：

```python
# 在 labels/default/alias 均等价之后、return EQUIVALENT 之前：
sql_has_cond = sql_cw.get("has_conditions", False)
spark_has_cond = spark_cw.get("has_conditions", False)
# 额外检测 Spark 侧 branches 中的 condition（model_dump 后仍保留）
if not spark_has_cond:
    for b in (spark_cw.get("branches", []) or []):
        if isinstance(b, dict) and b.get("condition") is not None:
            spark_has_cond = True
            break

if sql_has_cond or spark_has_cond:
    return StepEquivalenceResult(
        step_type="case_when",
        verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
        sql_count=sql_count,
        spark_count=spark_count,
        detail="CASE WHEN 存在 condition 但 compare_case_when_steps 暂不支持 condition 对比，需人工审核",
    )
```

---

## Phase 2：代码实施（按 Task 拆分）

### Task 1 [C]: `_extract_spark_step_data` 接入 `_normalize_step_dict` + join 兼容

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:1212-1221, 894-931`

**Interfaces:**
- Consumes: `SparkPlan.steps` (list of Pydantic step models)
- Produces: `list[dict[str, Any]]` — 与 SQL 侧一致的结构，各步骤经 `_normalize_step_dict`
- Consumes: `SparkJoinStep` 使用 `left_alias`/`right_alias` 而非 `left_table_ref`/`right_table_ref`

- [ ] **Step 1: 修改 `_extract_spark_step_data`**

```python
@staticmethod
def _extract_spark_step_data(spark_plan: SparkPlan) -> list[dict[str, Any]]:
    """从 SparkPlan 提取结构化 step 数据并通过 _normalize_step_dict 扁平化。

    统一路径：Spark 侧与 SQL 侧均经过 _normalize_step_dict 扁平化，
    确保两侧的 filter/project/join/aggregate/case_when/window 字段
    在进入 compare_plans 前已归一化为相同格式。
    """
    steps: list[dict[str, Any]] = []
    for step in spark_plan.steps:
        step_dict = step.model_dump(mode="json", exclude_none=True)
        step_dict = PlanComparator._normalize_step_dict(step_dict)  # 新增：统一扁平化
        steps.append(step_dict)
    return steps
```

SQL 侧的参考（仅对比，不改动）：
```python
@staticmethod
def _extract_sql_step_data(sql_plan: SqlBuildPlan) -> list[dict[str, Any]]:
    steps = []
    for step in sql_plan.steps:
        step_dict = step.model_dump(mode="json", exclude_none=True)
        step_dict = PlanComparator._normalize_step_dict(step_dict)
        PlanComparator._flatten_steps(step, steps)
        steps.append(step_dict)
    return steps
```

- [ ] **Step 2: `_flatten_join_step` 增加 Spark 侧键名兼容（NOT pop）**

在 `_flatten_join_step`（`plan_comparator.py:894-931`）中现有 `join_keys` 提取逻辑之后，增加兼容分支：

```python
# 现有逻辑：从 join_keys 提取（SQL 侧键名）
join_keys = step_dict.pop("join_keys", [])
result = dict(step_dict)

if join_keys and len(join_keys) > 0:
    first_key = join_keys[0]
    if isinstance(first_key, list) and len(first_key) >= 2:
        left_col, right_col = first_key[0], first_key[1]
        if isinstance(left_col, dict):
            result["left_table_ref"] = left_col.get("table_ref", "")
            result["left_key"] = (
                left_col.get("normalized_name") or left_col.get("column_name", "")
            )
        if isinstance(right_col, dict):
            result["right_key"] = (
                right_col.get("normalized_name") or right_col.get("column_name", "")
            )

# ← 新增：Spark 侧键名兼容（仅 .get()，不 pop——compare_join_steps 仍读 left_alias/right_alias）
if "left_table_ref" not in result or not result.get("left_table_ref"):
    result["left_table_ref"] = str(result.get("left_alias", ""))
if "right_table_ref" not in result or not result.get("right_table_ref"):
    result["right_table_ref"] = str(result.get("right_alias", ""))

# 防御性默认值
result.setdefault("left_key", "")
result.setdefault("right_key", "")
```

- [ ] **Step 3: 单元测试——验证 `_extract_spark_step_data` 产出扁平化 dict**

```python
# tests/spark/test_plan_comparator.py
def test_spark_step_data_goes_through_normalize(self):
    """Spark 侧 step 数据经过 _normalize_step_dict 扁平化。"""
    spark_plan = _make_spark_plan([
        SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="od",
            operator="GT", left="amount", right="threshold",
        ),
    ])
    steps = PlanComparator._extract_spark_step_data(spark_plan)
    # 验证 filter step 被扁平化（left/operator/right 在顶层）
    assert len(steps) == 1
    assert steps[0]["left"] == "amount"
    assert steps[0]["operator"] == "GT"
    assert steps[0]["right"] == "threshold"
```

- [ ] **Step 4: 运行 `pytest tests/spark/test_plan_comparator.py -v` 验证通过**

- [ ] **Step 5: Commit**

```
git add src/tianshu_datadev/spark/plan_comparator.py tests/spark/test_plan_comparator.py
git commit -m "fix(comparator): Spark 侧接入 _normalize_step_dict——D2/C 类"
```

- [ ] **Step 6: Code review**

Dispatch code reviewer 子代理审查本 Task 变更。

---

### Task 2 [C]: `_flatten_window_step` frame 字段统一合并

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:1033-1152`
- Test: `tests/spark/test_plan_comparator.py`（新增管道级集成测试）

- [ ] **Step 1: 实现 `_flatten_expr_frame` 工具方法**

```python
@staticmethod
def _flatten_expr_frame(expr: dict) -> None:
    """统一窗口表达式中的 frame 字段为合并字符串格式 "type:start:end"。

    处理两种格式：
    1. Spark 分离格式：frame_type（字符串）+ frame_start（字符串）+ frame_end（字符串）
    2. SQL WindowFrame dict 格式：frame 为 {"frame_type": ..., "start": {...}, "end": {...}}

    Spark 的 frame_start/frame_end 是纯字符串（如 "unbounded_preceding"），
    不是 FrameBoundary dict，直接 .upper() 即可。
    SQL 侧的 FrameBoundary 是 dict（含 kind/offset），需 _render_frame_boundary 渲染。
    """
    # Spark 分离格式
    frame_type = expr.pop("frame_type", None)
    if frame_type is not None:
        ft = str(frame_type).upper()
        fs = str(expr.pop("frame_start", "unbounded_preceding")).upper()
        fe = str(expr.pop("frame_end", "current_row")).upper()
        expr["frame"] = f"{ft}:{fs}:{fe}"
        return

    # SQL WindowFrame dict 格式
    frame = expr.pop("frame", None)
    if isinstance(frame, dict):
        ft = str(frame.get("frame_type", "RANGE")).upper()
        fs = PlanComparator._render_frame_boundary(frame.get("start", {}))
        fe = PlanComparator._render_frame_boundary(frame.get("end", {}))
        expr["frame"] = f"{ft}:{fs}:{fe}"
```

- [ ] **Step 2: 在 `window_exprs` block 中调用 `_flatten_expr_frame`**

```python
# plan_comparator.py:1094-1130 —— 在 window_exprs 每个 expr 的 partition/order/input 处理后调用
flat_exprs = []
for expr in raw_exprs:
    flat_expr = dict(expr) if isinstance(expr, dict) else expr
    if isinstance(flat_expr, dict):
        # ... 现有 partition_by/order_by/input 扁平化逻辑 ...
        PlanComparator._flatten_expr_frame(flat_expr)  # ← 新增
    flat_exprs.append(flat_expr)
result["window_exprs"] = flat_exprs
```

- [ ] **Step 3: 在 `expressions` block 中调用 `_flatten_expr_frame` + 补 `input_column` 扁平化**

```python
# plan_comparator.py:1132-1150 —— expressions block
raw_exprs2 = result.get("expressions", []) or []
if raw_exprs2:
    flat_exprs2 = []
    for expr in raw_exprs2:
        flat_expr = dict(expr) if isinstance(expr, dict) else expr
        if isinstance(flat_expr, dict):
            # 现有 partition_by/order_by 扁平化逻辑保持不变
            raw_p = flat_expr.pop("partition_by", []) or []
            flat_expr["partition_by"] = [...]
            raw_o = flat_expr.pop("order_by", []) or []
            flat_expr["order_by"] = [...]  # _render_sort_spec

            # ← 新增：input_column 扁平化（Spark 侧 input_column 可能已为字符串）
            raw_input_expr = flat_expr.pop("input", None)
            if raw_input_expr is not None and isinstance(raw_input_expr, dict):
                name = raw_input_expr.get("normalized_name", "") or raw_input_expr.get("column_name", "")
                flat_expr["input_column"] = normalize_field_name(str(name))
            elif flat_expr.get("input_column") is not None:
                # 字符串格式的 input_column 无需进一步处理
                pass

            PlanComparator._flatten_expr_frame(flat_expr)  # ← 新增
        flat_exprs2.append(flat_expr)
    result["expressions"] = flat_exprs2
```

- [ ] **Step 4: 管道级集成测试（C类验收标准）**

所有测试使用 `PlanComparator.compare()` + 真实模型：

```python
# tests/spark/test_plan_comparator.py
class TestPlanComparatorWindowEquivalence:
    """Window 逻辑等价性对比——全管道集成测试（真实模型）。"""

    def test_window_frame_equivalent(self):
        """SQL WindowFrame dict + Spark frame_type/frame_start/frame_end → frame 合并后等价。"""
        from tianshu_datadev.planning.models import (
            WindowExpr, WindowFrame, FrameBoundary, FrameBoundaryKind,
            ColumnRef, SortSpec, SortDirection, WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr, SparkWindowFunction, SparkSortSpec, SparkSortDirection,
        )

        # SQL 侧：使用 WindowFrame dict
        sql_window = WindowStep(
            step_type="window", step_id="step_win_001",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM, alias="total",
                    input=ColumnRef(table_ref="od", column_name="amount", normalized_name="amount"),
                    partition_by=[ColumnRef(table_ref="od", column_name="dept_id", normalized_name="dept_id")],
                    order_by=[SortSpec(column="salary", direction=SortDirection.ASC)],
                    frame=WindowFrame(
                        frame_type="ROWS",
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：使用分离 frame 字符串字段
        spark_window = SparkWindowStep(
            step_type=SparkStepType.WINDOW,
            input_alias="od",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM, alias="total",
                    input_column="amount",
                    partition_by=["dept_id"],
                    order_by=["salary ASC"],  # _render_sort_spec 插入 null_order→"salary ASC FIRST"
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"相同 frame 应等价，实际 status={report.status}, "
            f"results={[(r.step_type, r.verdict.value) for r in report.step_results]}"
        )

    def test_window_frame_diff_not_equivalent(self):
        """SQL ROWS vs Spark RANGE → LOGIC_MISMATCH。"""
        # 与 test_window_frame_equivalent 类似，但 Spark 侧 frame_type="RANGE"
        ...

    def test_window_order_reversed_not_equivalent(self):
        """ORDER BY salary DESC, name ASC vs name ASC, salary DESC → LOGIC_MISMATCH。"""
        ...

    def test_window_input_column_diff_not_equivalent(self):
        """SUM(amount) vs SUM(discount) → LOGIC_MISMATCH。"""
        ...

    def test_window_full_equivalent(self):
        """完整 window（partition + order + frame + input）→ LOGIC_EQUIVALENT。"""
        ...

    def test_window_no_frame_no_partition(self):
        """无 frame、无 partition 的 ROW_NUMBER → 不崩溃，等价。"""
        ...
```

- [ ] **Step 5: 运行 `pytest tests/spark/test_plan_comparator.py::TestPlanComparatorWindowEquivalence -v` 验证通过**

- [ ] **Step 6: 运行全量 `pytest tests/spark/ -v` 验证无回归**

- [ ] **Step 7: Commit**

```
git add src/tianshu_datadev/spark/plan_comparator.py tests/spark/test_plan_comparator.py
git commit -m "fix(comparator): Window frame 字段统一合并——F1/C 类"
```

- [ ] **Step 8: Code review**

---

### Task 3 [C]: CASE WHEN condition 降级 UNSUPPORTED_COMPARISON

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:974-1006`（`_flatten_case_when_step`）
- Modify: `src/tianshu_datadev/spark/plan_equivalence.py:516-604`（`compare_case_when_steps`）
- Test: `tests/spark/test_spark_plan.py` + `tests/spark/test_plan_comparator.py`

- [ ] **Step 1: `_flatten_case_when_step` 保留 condition 检测标记**

```python
# _flatten_case_when_step 中 labels 提取后追加：
# 检测 condition 存在性（保留标记供 compare_case_when_steps 消费）
# WhenBranch 的 condition 为 Predicate | None，raw_condition 为 SqlRawExpression | None
has_conditions = False
if isinstance(raw_cases, list):
    for c in raw_cases:
        if isinstance(c, dict):
            cond = c.get("condition")     # Predicate dict | None
            raw_cond = c.get("raw_condition")  # SqlRawExpression dict | None
            if cond is not None or raw_cond is not None:
                has_conditions = True
                break
result["has_conditions"] = has_conditions
result["condition_comparison_supported"] = False
```

- [ ] **Step 2: `compare_case_when_steps` 消费 `has_conditions` 标记，不等价时降级**

```python
# compare_case_when_steps：在 labels/default/alias 均等价之后、return EQUIVALENT 之前插入：

# 检测 condition 是否存在（标记由 _flatten_case_when_step 写入）
sql_has_cond = sql_cw.get("has_conditions", False)
spark_has_cond = spark_cw.get("has_conditions", False)

# 额外检测 Spark 侧 branches 中的 condition
if not spark_has_cond:
    for b in (spark_cw.get("branches", []) or []):
        if isinstance(b, dict) and b.get("condition") is not None:
            spark_has_cond = True
            break

if sql_has_cond or spark_has_cond:
    return StepEquivalenceResult(
        step_type="case_when",
        verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
        sql_count=sql_count,
        spark_count=spark_count,
        detail=(
            f"CASE WHEN[{i}] 存在 condition 但 compare_case_when_steps "
            f"暂不支持 condition 对比（仅比较 labels/default/alias），需人工审核"
        ),
    )
```

- [ ] **Step 3: 管道级集成测试（C类验收标准）**

```python
# tests/spark/test_plan_comparator.py — 真实 model 管道
def test_case_when_condition_triggers_unsupported(self):
    """CASE WHEN 含 condition → LOGIC_UNSUPPORTED。"""
    from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
    from tianshu_datadev.planning.models import (
        WhenBranch, Predicate, PredicateOperator, ColumnRef, SqlLiteral,
    )

    cond = Predicate(
        left=ColumnRef(table_ref="od", column_name="amount",
                       normalized_name="amount"),
        operator=PredicateOperator.GT,
        right=SqlLiteral(value="100", is_sql_expr=False),
    )
    sql_cw = CaseWhenStep(
        step_type="case_when", step_id="step_cw_001",
        cases=[
            WhenBranch(condition=cond, result=SqlLiteral(value="high", is_sql_expr=False)),
        ],
        else_value=SqlLiteral(value="low", is_sql_expr=False),
        alias="level",
    )
    sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_cw])

    # Spark 侧：相等的 labels
    spark_cw = SparkCaseWhenStep(
        step_type=SparkStepType.CASE_WHEN,
        input_alias="od", output_alias="level",
        branches=[SparkCaseWhenBranch(label="high")],
        else_value="low",
    )
    spark_plan = _make_spark_plan([_make_spark_read_step(), spark_cw])

    comparator = PlanComparator()
    report = comparator.compare(sql_plan, spark_plan)

    # condition 虽存在但 labels 相同 → UNSUPPORTED_COMPARISON（非 EQUIVALENT）
    cw_results = [r for r in report.step_results if r.step_type == "case_when"]
    assert len(cw_results) > 0
    assert cw_results[0].verdict == EquivalenceVerdict.UNSUPPORTED_COMPARISON

def test_case_when_no_condition_still_equivalent(self):
    """无 condition 的 CASE WHEN（仅 labels）→ 不变，仍为 EQUIVALENT。"""
    # labels/default/alias 都相同 → EQUIVALENT（不受本次修改影响）
    ...
```

- [ ] **Step 4: dict 层补充单测（非验收路径，仅辅助）**

```python
# tests/spark/test_spark_plan.py — 现有 compare_case_when_steps 测试
def test_case_when_condition_diff_same_labels(self):
    """相同 labels 不同 condition → UNSUPPORTED_COMPARISON。"""
    sql_cw = [{"labels": ["high"], "default_value": "low", "alias": "level",
               "has_conditions": True, "condition_comparison_supported": False}]
    spark_cw = [{"branches": [{"label": "high"}], "else_value": "low",
                 "output_alias": "level"}]
    result = compare_case_when_steps(sql_cw, spark_cw)
    assert result.verdict == EquivalenceVerdict.UNSUPPORTED_COMPARISON
```

- [ ] **Step 5: 运行全量 `pytest tests/spark/ -v` 验证通过**

- [ ] **Step 6: Commit + Code review**

```
git add src/tianshu_datadev/spark/plan_comparator.py src/tianshu_datadev/spark/plan_equivalence.py tests/spark/test_spark_plan.py tests/spark/test_plan_comparator.py
git commit -m "fix(comparator): CASE WHEN condition 降级 UNSUPPORTED_COMPARISON——D1/C 类"
```

---

### Task 4 [B]: Filter 右侧谓词 tree 修复

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:837-838`
- Test: `tests/spark/test_plan_comparator.py`

**说明：** B 类——局部逻辑修复，方案已明确，无需设计确认。

```python
# 当前（plan_comparator.py:837-838）：
right_val = predicate.get("right", "")
if isinstance(right_val, dict):
    right_val = PlanComparator._column_ref_to_string(right_val)

# 修改为：先检测是否为 Nesting Predicate tree
right_val = predicate.get("right", "")
if isinstance(right_val, dict):
    if PlanComparator._is_predicate_tree(right_val):
        # 嵌套谓词 tree → 递归渲染（复用 _render_predicate_tree）
        right_val = PlanComparator._render_predicate_tree(right_val)
    else:
        # ColumnRef 或 SqlLiteral（原逻辑）
        right_val = PlanComparator._column_ref_to_string(right_val)
```

**测试（管道级）：** 使用 `PlanComparator.compare()` + 真实 FilterStep（AND 组合时右侧为嵌套谓词 tree）。

同 `isinstance(left_val, dict)` 分支（`plan_comparator.py:823-831`）已正确使用 `_is_predicate_tree`。但实际路径是 left 为 ColumnRef、right 为嵌套 tree 的 AND/OR 组合时才触发——当前代码的 `isinstance(right_val, dict)` 分支未做 `_is_predicate_tree` 检测。

---

### Task 5 [B]: 多 aggregate 回归测试

**Files:**
- Test: `tests/spark/test_plan_comparator.py`

B 类——回归测试，已有修复代码，只补测试。

```python
def test_aggregate_multi_step_not_crash(self):
    """多 aggregate step 通过 PlanComparator.compare_program 时不崩溃。"""
    from tianshu_datadev.planning.sql_build_plan import AggregateStep

    sql_agg_1 = AggregateStep(
        step_type="aggregate", step_id="agg_001",
        group_keys=[ColumnRef(table_ref="od", column_name="dept_id", normalized_name="dept_id")],
        metrics=[AggregateSpec(aggregation=AggregationType.COUNT, input=ColumnRef(
            table_ref="od", column_name="id", normalized_name="id"), alias="cnt")],
    )
    sql_agg_2 = AggregateStep(
        step_type="aggregate", step_id="agg_002",
        group_keys=[ColumnRef(table_ref="od", column_name="region", normalized_name="region")],
        metrics=[AggregateSpec(aggregation=AggregationType.SUM, input=ColumnRef(
            table_ref="od", column_name="amount", normalized_name="amount"), alias="total")],
    )
    sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_agg_1, sql_agg_2])

    # Spark 侧：单 aggregate
    spark_agg = SparkAggregateStep(
        step_type=SparkStepType.AGGREGATE, input_alias="od",
        group_keys=["dept_id"],
        metrics=[SparkAggregateSpec(function=SparkAggFunction.COUNT, input_column="id", alias="cnt")],
    )
    spark_plan = _make_spark_plan([_make_spark_read_step(), spark_agg])

    comparator = PlanComparator()
    report = comparator.compare(sql_plan, spark_plan)

    # 不得崩溃，应有结构化结果
    agg_results = [r for r in report.step_results if r.step_type == "aggregate"]
    assert len(agg_results) > 0
    assert agg_results[0].verdict in (
        EquivalenceVerdict.NOT_EQUIVALENT,
        EquivalenceVerdict.UNSUPPORTED_COMPARISON,
    ), f"多 aggregate 不得崩溃或误判 EQUIVALENT, 实际={agg_results[0].verdict}"
```

---

## 执行顺序

```
Task 1 [C]: _extract_spark_step_data 接入归一化 + join 兼容 ──→ Code Review
    ↓ (review 通过)
Task 2 [C]: Window frame 字段合并 ──→ Code Review
    ↓ (review 通过)
Task 3 [C]: CASE WHEN condition 降级 UNSUPPORTED ──→ Code Review
    ↓ (review 通过)
Task 4 [B]: Filter 右侧谓词 tree ──→ 无需 review（B 类局部修复）
    ↓
Task 5 [B]: 多 aggregate 回归测试 ──→ 无需 review
    ↓
全量 pytest 验证：600 passed / 11 skipped
```

---

## 状态传播验证（最终验证项）

- [ ] `compare_case_when_steps` 返回 `UNSUPPORTED_COMPARISON` → `PlanEquivalenceResult.overall_verdict` 为 `UNSUPPORTED_COMPARISON`
- [ ] `PlanComparator._map_status(UNSUPPORTED_COMPARISON)` → `ComparisonStatus.LOGIC_UNSUPPORTED`
- [ ] Pipeline: `{"LOGIC_UNSUPPORTED": "HUMAN_REVIEW"}`
- [ ] `SparkReviewBuilder._compute_review_ready(comparator_status="LOGIC_UNSUPPORTED")` → `False`
- [ ] Window frame diff 场景：`compare_window_steps` 返回 `NOT_EQUIVALENT` → `PlanComparator.compare()` 输出 `LOGIC_MISMATCH`
