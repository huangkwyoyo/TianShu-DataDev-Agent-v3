# Comparator Window + Aggregate 修复计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) 或 superpowers:executing-plans 按任务逐步执行。步骤使用 `- [ ]` 跟踪进度。

**目标:** 修复 Comparator 中 window 对比崩溃、漏检字段、order_by 顺序丢弃，以及 aggregate assert 生产崩溃

**架构:**
- `plan_comparator.py:_normalize_step_dict` 新增 `_flatten_window_step`，将 SQL 侧 `list[ColumnRef]` 和 `list[SortSpec]` dict 扁平化为字符串，消除 `normalize_field_name` 的 `AttributeError` 崩溃
- `plan_equivalence.py:compare_window_steps` 扩展对比元组，加入 `input_column`、`frame`、保序 `order_by`
- `plan_equivalence.py:compare_aggregate_steps` 将 `assert len == 1` 替换为结构化 NOT_EQUIVALENT 返回

**Tech Stack:** Python, tianshu_datadev Spark equivalence comparison

## Global Constraints

- 所有代码注释使用中文
- 字段名使用 `normalize_field_name` 归一化（去表前缀、小写、去空格）
- `EquivalenceVerdict.UNSUPPORTED_COMPARISON` 仅对确实无法设计规则的 step 类型使用（如 subquery），window 有规则则不应使用
- `StepEquivalenceResult.detail` 在 NOT_EQUIVALENT 时必须描述具体差异
- 测试使用 pytest，`test_plan_comparator.py` 和 `test_spark_plan.py` 窗口对比测试各一个

---

### Task 1: 新增 `_flatten_window_step`（PlanComparator）

**触发条件：** C-1 崩溃根因——`_normalize_step_dict:706` 对 window step 直接返回 `step_dict`，但 SQL 侧 `WindowStep.model_dump()` 后 `partition_by: list[ColumnRef]` 和 `order_by: list[SortSpec]` 是 dict 列表。`compare_window_steps` 调用 `normalize_field_name(p)` 时 `p.strip()` 对 dict 抛出 `AttributeError`。

**修复：** 新增 `_flatten_window_step` 将嵌套的 ColumnRef/SortSpec dict 转换为字符串，与 filter/project/aggregate 等步的扁平化模式一致。

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:706`
- Test: `tests/spark/test_plan_comparator.py`

- [ ] **Step 1: 读取现有 flatten 函数了解模式**

读取 `_flatten_aggregate_step` 和 `_flatten_filter_step` 了解扁平化模式——将 `step_dict.pop("field")` 处理后重新赋值。

- [ ] **Step 2: 新增 `_flatten_window_step` 静态方法**

```python
@staticmethod
def _flatten_window_step(step_dict: dict[str, Any]) -> dict[str, Any]:
    """扁平化 WindowStep——将 ColumnRef/SortSpec 归一化为字符串。

    SQL 侧 WindowStep.model_dump() 后：
    - partition_by: list[ColumnRef] → 每个 ColumnRef 有 normalized_name/column_name/table_ref
    - order_by: list[SortSpec] → 每个 SortSpec 有 column/direction/null_order
    - frame: WindowFrame | None

    扁平化规则：
    - partition_by: ColumnRef → normalized_name 字符串（经 normalize_field_name）
    - order_by: SortSpec → "column direction null_order" 字符串（经 normalize_field_name），
      保留 direction/null_order 顺序语义
    - frame: WindowFrame dict → "frame_type:start:end" 规范字符串
    """
    result = dict(step_dict)

    # 扁平化 partition_by
    raw_partition = result.pop("partition_by", []) or []
    flattened_partition = []
    for p in raw_partition:
        if isinstance(p, dict):
            # ColumnRef dict → 提取 normalized_name
            name = p.get("normalized_name", "") or p.get("column_name", "")
            flattened_partition.append(normalize_field_name(str(name)))
        else:
            flattened_partition.append(normalize_field_name(str(p)))
    result["partition_by"] = flattened_partition

    # 扁平化 order_by
    raw_order = result.pop("order_by", []) or []
    flattened_order = []
    for o in raw_order:
        if isinstance(o, dict):
            # SortSpec dict → "column direction null_order"
            col = normalize_field_name(str(o.get("column", "")))
            direction = str(o.get("direction", "ASC")).upper()
            null_order = str(o.get("null_order", "LAST")).upper()
            flattened_order.append(f"{col} {direction} {null_order}")
        else:
            flattened_order.append(normalize_field_name(str(o)))
    result["order_by"] = flattened_order

    # 扁平化 input_column（ColumnRef → 字符串）
    raw_input = result.pop("input", None)
    if raw_input and isinstance(raw_input, dict):
        if "normalized_name" in raw_input or "column_name" in raw_input:
            name = raw_input.get("normalized_name", "") or raw_input.get("column_name", "")
            result["input_column"] = normalize_field_name(str(name))
        elif "value" in raw_input:
            # SqlLiteral → 直接取 value
            result["input_column"] = str(raw_input["value"])

    # 扁平化 frame
    raw_frame = result.pop("frame", None)
    if raw_frame and isinstance(raw_frame, dict):
        frame_type = str(raw_frame.get("frame_type", "RANGE")).upper()
        start = raw_frame.get("start", {})
        end = raw_frame.get("end", {})
        start_str = PlanComparator._render_frame_boundary(start)
        end_str = PlanComparator._render_frame_boundary(end)
        result["frame"] = f"{frame_type}:{start_str}:{end_str}"

    # 保留原有 window_exprs 键（用于 _extract_sql_step_data 匹配）
    # 但每个 expr 中的 partition_by/order_by 也需扁平化
    raw_exprs = result.get("window_exprs", []) or []
    if raw_exprs:
        flat_exprs = []
        for expr in raw_exprs:
            flat_expr = dict(expr) if isinstance(expr, dict) else expr
            if isinstance(flat_expr, dict):
                # 扁平化 partition_by
                raw_p = flat_expr.pop("partition_by", []) or []
                flat_expr["partition_by"] = [
                    normalize_field_name(str(
                        p.get("normalized_name", "") or p.get("column_name", "") or str(p)
                    )) if isinstance(p, dict) else normalize_field_name(str(p))
                    for p in raw_p
                ]
                # 扁平化 order_by
                raw_o = flat_expr.pop("order_by", []) or []
                flat_expr["order_by"] = [
                    PlanComparator._render_sort_spec(o) if isinstance(o, dict) else normalize_field_name(str(o))
                    for o in raw_o
                ]
                # 扁平化 input
                raw_input_expr = flat_expr.pop("input", None)
                if raw_input_expr and isinstance(raw_input_expr, dict):
                    if "normalized_name" in raw_input_expr or "column_name" in raw_input_expr:
                        name = raw_input_expr.get("normalized_name", "") or raw_input_expr.get("column_name", "")
                        flat_expr["input_column"] = normalize_field_name(str(name))
                    elif "value" in raw_input_expr:
                        flat_expr["input_column"] = str(raw_input_expr["value"])
                # 扁平化 frame
                raw_frame_expr = flat_expr.pop("frame", None)
                if raw_frame_expr and isinstance(raw_frame_expr, dict):
                    ft = str(raw_frame_expr.get("frame_type", "RANGE")).upper()
                    st = PlanComparator._render_frame_boundary(raw_frame_expr.get("start", {}))
                    et = PlanComparator._render_frame_boundary(raw_frame_expr.get("end", {}))
                    flat_expr["frame"] = f"{ft}:{st}:{et}"
            flat_exprs.append(flat_expr)
        result["window_exprs"] = flat_exprs

    # 同样扁平化 expressions（Spark 侧兼容）
    raw_exprs2 = result.get("expressions", []) or []
    if raw_exprs2:
        flat_exprs2 = []
        for expr in raw_exprs2:
            flat_expr = dict(expr) if isinstance(expr, dict) else expr
            if isinstance(flat_expr, dict):
                raw_p = flat_expr.pop("partition_by", []) or []
                flat_expr["partition_by"] = [
                    normalize_field_name(str(p)) if isinstance(p, dict) else normalize_field_name(str(p))
                    for p in raw_p
                ]
                raw_o = flat_expr.pop("order_by", []) or []
                flat_expr["order_by"] = [
                    PlanComparator._render_sort_spec(o) if isinstance(o, dict) else normalize_field_name(str(o))
                    for o in raw_o
                ]
            flat_exprs2.append(flat_expr)
        result["expressions"] = flat_exprs2

    return result
```

- [ ] **Step 3: 新增辅助方法 `_render_frame_boundary` 和 `_render_sort_spec`**

```python
@staticmethod
def _render_frame_boundary(boundary: dict) -> str:
    """将 FrameBoundary dict 渲染为规范字符串。

    FrameBoundary 格式：{"kind": "UNBOUNDED_PRECEDING", "offset": None}
    """
    kind = str(boundary.get("kind", "")).upper()
    offset = boundary.get("offset")
    if offset is not None:
        return f"{kind}({offset})"
    return kind

@staticmethod
def _render_sort_spec(spec: dict) -> str:
    """将 SortSpec dict 渲染为规范字符串。

    SortSpec 格式：{"column": "amount", "direction": "ASC", "null_order": "LAST"}
    输出： "amount ASC LAST"
    """
    col = normalize_field_name(str(spec.get("column", "")))
    direction = str(spec.get("direction", "ASC")).upper()
    null_order = str(spec.get("null_order", "LAST")).upper()
    return f"{col} {direction} {null_order}"
```

- [ ] **Step 4: 在 `_normalize_step_dict` 中注册 window 扁平化**

```python
# 在 _normalize_step_dict 中，case_when 分支后新增：
if step_type == "window":
    return PlanComparator._flatten_window_step(step_dict)
# 删除注释中的 "window 的对比字段已在顶层"
```

- [ ] **Step 5: 编写 window 扁平化的单元测试**

在 `TestPlanComparatorStepExtraction` 中新增测试类：

```python
def test_flatten_window_step(self):
    """WindowStep 中 ColumnRef/SortSpec → 扁平化为字符串。"""
    from tianshu_datadev.planning.models import (
        ColumnRef, SortSpec, SortDirection, NullOrder,
        WindowExpr, WindowFunction,
    )

    step_dict = {
        "step_type": "window",
        "step_id": "win_001",
        "window_exprs": [
            WindowExpr(
                function=WindowFunction.ROW_NUMBER,
                alias="rn",
                partition_by=[
                    ColumnRef(
                        table_ref="od", column_name="dept_id", normalized_name="dept_id",
                    ),
                ],
                order_by=[
                    SortSpec(column="salary", direction=SortDirection.DESC,
                             null_order=NullOrder.LAST),
                ],
            ).model_dump(mode="json", exclude_none=True)
        ],
    }

    result = PlanComparator._flatten_window_step(step_dict)
    exprs = result.get("window_exprs", [])
    assert len(exprs) == 1
    assert exprs[0]["partition_by"] == ["dept_id"]
    # order_by 应包含 direction 和 null_order
    assert "salary desc last" in str(exprs[0]["order_by"]).lower()
```

- [ ] **Step 6: 运行测试确认通过**

Run: `pytest tests/spark/test_plan_comparator.py::TestPlanComparatorStepExtraction::test_flatten_window_step -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/spark/plan_comparator.py tests/spark/test_plan_comparator.py
git commit -m "fix: _flatten_window_step 消除 window 对比的 dict 崩溃
C-1 根因：_normalize_step_dict 对 window 直接返回原始 dict，
compare_window_steps 调用 normalize_field_name(p) 时 p 为 dict → .strip() AttributeError。
新增 _flatten_window_step 将 ColumnRef/SortSpec 提取为字符串。
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: 修复 `compare_window_steps`——扩展对比元组、保序 order_by、修复崩溃

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_equivalence.py:602-683`
- Test: `tests/spark/test_spark_plan.py`

- [ ] **Step 1: 读取当前 `compare_window_steps` 源码**

确认当前实现位置 line 602-683，理解以下需要修改的点：
1. partition_by 的 `normalize_field_name(p)` 现在接收字符串（扁平化后），不再崩溃
2. order_by 的 `sorted()` 丢弃顺序——必须去除 sorted()，保留原序
3. 对比元组缺少 `input_column` 和 `frame`

- [ ] **Step 2: 重写 `compare_window_steps`——扩展对比元组、保序、修复崩溃**

```python
def compare_window_steps(
    sql_windows: list[dict[str, Any]],
    spark_windows: list[dict[str, Any]],
) -> StepEquivalenceResult:
    """对比 SQL WindowStep 与 Spark WindowStep 的结构等价性。

    等价条件：
    1. 数量相同
    2. 每个窗口表达式的 (function, alias, input_column, partition_by, order_by, frame) 等价
    3. order_by 保持顺序（ORDER BY a,b ≠ ORDER BY b,a）

    Args:
        sql_windows: SQL 侧 WindowStep 的 model_dump 列表（已扁平化）
        spark_windows: Spark 侧 SparkWindowStep 的 model_dump 列表（已扁平化）

    Returns:
        StepEquivalenceResult
    """
    sql_count = len(sql_windows)
    spark_count = len(spark_windows)

    if sql_count == 0 and spark_count == 0:
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.EQUIVALENT,
            sql_count=0,
            spark_count=0,
        )

    if sql_count != spark_count:
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=f"窗口步骤数量不一致：SQL 侧 {sql_count} 个，Spark 侧 {spark_count} 个",
        )

    # 收集所有窗口表达式
    def _extract_exprs(windows: list[dict], field_key: str) -> list[tuple]:
        """从 window steps 中提取表达式元组。"""
        exprs = []
        for w in windows:
            for expr in w.get(field_key, []) or w.get("expressions", []):
                func = str(expr.get("function", "")).upper()
                alias = normalize_field_name(expr.get("alias", ""))
                # input_column（窗口函数的输入列：SUM/AVG/COUNT/LAG/LEAD 需要，排名函数不需要）
                input_col = normalize_field_name(expr.get("input_column", "") or "")
                # partition_by——去重但保序（两边独立排序后再比）
                partition = tuple(sorted([
                    normalize_field_name(str(p)) for p in (expr.get("partition_by", []) or [])
                ]))
                # order_by——保留原始顺序（ORDER BY a,b ≠ ORDER BY b,a）
                # 已扁平化为字符串列表，每个元素含 direction 和 null_order
                order = tuple(
                    normalize_field_name(str(o)) for o in (expr.get("order_by", []) or [])
                )
                # frame——窗口帧边界（ROWS/RANGE BETWEEN ... AND ...）
                frame_raw = expr.get("frame", "")
                frame = normalize_field_name(str(frame_raw)) if frame_raw else ""
                exprs.append((func, alias, input_col, partition, order, frame))
        return exprs

    sql_exprs = _extract_exprs(sql_windows, "window_exprs")
    spark_exprs = _extract_exprs(spark_windows, "expressions")

    # 比较方式：先规范化每组——partition 已排序可比较，order 保留原序
    sql_set: set[tuple] = set(sql_exprs)
    spark_set: set[tuple] = set(spark_exprs)

    if sql_set != spark_set:
        only_sql = sql_set - spark_set
        only_spark = spark_set - sql_set
        return StepEquivalenceResult(
            step_type="window",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"窗口表达式不一致——仅在 SQL 侧：{list(only_sql)}，"
                f"仅在 Spark 侧：{list(only_spark)}"
            ),
        )

    return StepEquivalenceResult(
        step_type="window",
        verdict=EquivalenceVerdict.EQUIVALENT,
        sql_count=sql_count,
        spark_count=spark_count,
    )
```

- [ ] **Step 3: 运行现有测试确认基线通过**

Run: `pytest tests/spark/test_spark_plan.py::TestSparkPlanComparison::test_window_equivalent tests/spark/test_spark_plan.py::TestSparkPlanComparison::test_window_not_equivalent -v`
Expected: PASS

- [ ] **Step 4: 编写非空 window 的端到端对比测试**

在 `test_spark_plan.py` 中新增测试：

```python
def test_window_partition_order_equivalent(self):
    """相同 partition_by + order_by（含 direction）→ 等价。"""
    win_expr = {
        "function": "ROW_NUMBER", "alias": "rn",
        "partition_by": ["dept_id"], "order_by": ["salary DESC LAST"],
    }
    sql_windows = [{"window_exprs": [win_expr]}]
    spark_windows = [{"expressions": [win_expr]}]
    result = compare_window_steps(sql_windows, spark_windows)
    assert result.verdict == EquivalenceVerdict.EQUIVALENT

def test_window_order_reversed_not_equivalent(self):
    """ORDER BY salary DESC, name ASC vs ORDER BY name ASC, salary DESC → NOT_EQUIVALENT。"""
    sql_win = {
        "window_exprs": [
            {"function": "RANK", "alias": "rk",
             "partition_by": ["dept_id"],
             "order_by": ["salary DESC LAST", "name ASC LAST"]},
        ],
    }
    spark_win = {
        "window_exprs": [
            {"function": "RANK", "alias": "rk",
             "partition_by": ["dept_id"],
             "order_by": ["name ASC LAST", "salary DESC LAST"]},
        ],
    }
    result = compare_window_steps([sql_win], [spark_win])
    assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

def test_window_input_column_diff_not_equivalent(self):
    """SUM(amount) vs SUM(discount) → NOT_EQUIVALENT。"""
    sql_win = {
        "window_exprs": [
            {"function": "SUM", "alias": "total",
             "input_column": "amount",
             "partition_by": ["dept_id"]},
        ],
    }
    spark_win = {
        "window_exprs": [
            {"function": "SUM", "alias": "total",
             "input_column": "discount",
             "partition_by": ["dept_id"]},
        ],
    }
    result = compare_window_steps([sql_win], [spark_win])
    assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

def test_window_frame_diff_not_equivalent(self):
    """ROWS vs RANGE → NOT_EQUIVALENT。"""
    sql_win = {
        "window_exprs": [
            {"function": "SUM", "alias": "total",
             "input_column": "amount",
             "partition_by": ["dept_id"],
             "order_by": ["salary ASC LAST"],
             "frame": "ROWS:UNBOUNDED_PRECEDING:CURRENT_ROW"},
        ],
    }
    spark_win = {
        "window_exprs": [
            {"function": "SUM", "alias": "total",
             "input_column": "amount",
             "partition_by": ["dept_id"],
             "order_by": ["salary ASC LAST"],
             "frame": "RANGE:UNBOUNDED_PRECEDING:CURRENT_ROW"},
        ],
    }
    result = compare_window_steps([sql_win], [spark_win])
    assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

def test_window_multi_expr_partial_mismatch(self):
    """多个 window expr 中一个不一致 → 检测到 NOT_EQUIVALENT。"""
    sql_wins = [{
        "window_exprs": [
            {"function": "ROW_NUMBER", "alias": "rn",
             "partition_by": ["dept_id"], "order_by": ["salary DESC LAST"]},
            {"function": "SUM", "alias": "total", "input_column": "amount",
             "partition_by": ["dept_id"]},
        ],
    }]
    spark_wins = [{
        "window_exprs": [
            {"function": "ROW_NUMBER", "alias": "rn",
             "partition_by": ["dept_id"], "order_by": ["salary DESC LAST"]},
            {"function": "AVG", "alias": "total", "input_column": "amount",
             "partition_by": ["dept_id"]},
        ],
    }]
    result = compare_window_steps(sql_wins, spark_wins)
    assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT
```

- [ ] **Step 5: 运行新增测试确认通过**

Run: `pytest tests/spark/test_spark_plan.py::TestSparkPlanComparison -v`
Expected: 全部 PASS（含新增 5 个测试）

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/spark/plan_equivalence.py tests/spark/test_spark_plan.py
git commit -m "fix: compare_window_steps 扩展 input_column/frame/保序 order_by
C-2: 补齐 input_column 和 frame 字段到对比元组
C-3: 移除 order_by 的 sorted()——ORDER BY a,b ≠ ORDER BY b,a
同时消除 C-1 崩溃（配合 _flatten_window_step 确保输入为字符串而非 dict）
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: 修复 `compare_aggregate_steps`——替换 assert 为结构化结果

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_equivalence.py:373-379`
- Test: `tests/spark/test_spark_plan.py`（或 test_plan_comparator.py）

- [ ] **Step 1: 读取当前 assert 逻辑**

确认位置 `plan_equivalence.py:376`。现有断言：
```python
assert len(sql_aggs) == len(spark_aggs) == 1, (
    f"compare_aggregate_steps 假设每侧最多 1 个 aggregate step，"
    f"实际 SQL={len(sql_aggs)}, Spark={len(spark_aggs)}"
)
```

- [ ] **Step 2: 替换为条件返回 NOT_EQUIVALENT**

当 `len(sql_aggs) != 1` 或 `len(spark_aggs) != 1` 时，返回 `NOT_EQUIVALENT` 而非崩溃：

```python
if len(sql_aggs) != len(spark_aggs):
    # 数量已在前面检查过，这里主要处理 != 1 的情况
    # 当两侧都不为 1 但数量相等时，说明有多个 aggregate step
    # 当前设计不支持多 aggregate 对比
    return StepEquivalenceResult(
        step_type="aggregate",
        verdict=EquivalenceVerdict.NOT_EQUIVALENT,
        sql_count=len(sql_aggs),
        spark_count=len(spark_aggs),
        detail=(
            f"compare_aggregate_steps 暂不支持多 aggregate 对比，"
            f"实际 SQL={len(sql_aggs)}, Spark={len(spark_aggs)}"
        ),
    )
```

- [ ] **Step 3: 编写多 aggregate 对比测试**

在 test_spark_plan.py 中新增：

```python
def test_aggregate_multi_step_not_equivalent(self):
    """多 aggregate step 时不崩溃，返回 NOT_EQUIVALENT。"""
    sql_aggs = [
        {"group_keys": ["a"], "metrics": [{"function": "COUNT", "alias": "cnt_a"}]},
        {"group_keys": ["b"], "metrics": [{"function": "SUM", "alias": "sum_b"}]},
    ]
    spark_aggs = [
        {"group_keys": ["a"], "metrics": [{"function": "COUNT", "alias": "cnt_a"}]},
        {"group_keys": ["b"], "metrics": [{"function": "SUM", "alias": "sum_b"}]},
    ]
    result = compare_aggregate_steps(sql_aggs, spark_aggs)
    # 不崩溃，返回 NOT_EQUIVALENT（暂不支持多 aggregate 对比）
    assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/spark/test_spark_plan.py::TestSparkPlanComparison::test_aggregate_multi_step_not_equivalent -v`
Expected: PASS（不崩溃）

Run: `pytest tests/spark/test_spark_plan.py::TestSparkPlanComparison -v`
Expected: 现有 aggregate 测试仍正常通过

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/spark/plan_equivalence.py tests/spark/test_spark_plan.py
git commit -m "fix: compare_aggregate_steps 替换 assert 为 NOT_EQUIVALENT 返回
C-4: assert len==1 在生产路径可达（target_grain=None + 不同粒度多 aggregate），
改为返回 NOT_EQUIVALENT 而非 AssertionError 崩溃
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: 运行全部测试确认无回归

- [ ] **Step 1: 清除 pyc 缓存**

```bash
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null
```

- [ ] **Step 2: 运行全部 spark 相关测试**

Run: `pytest tests/spark/ -v --tb=short 2>&1 | tail -50`
Expected: 无新增失败

- [ ] **Step 3: 如失败，逐一修复后重新运行**

- [ ] **Step 4: 提交最终 commit**

```bash
git add -A
git commit -m "chore: Comparator window+aggregate 修复后全部测试通过
- C-1: _flatten_window_step 消除 dict→.strip() AttributeError 崩溃
- C-2: compare_window_steps 补充 input_column 和 frame 对比
- C-3: 移除 order_by sorted()，保留 ORDER BY 顺序语义
- C-4: compare_aggregate_steps assert→NOT_EQUIVALENT，消除生产崩溃
- 新增 6 个测试覆盖非空 window、order 反转、input_column 差异、
  frame 差异、多 aggregate 等场景
Co-Authored-By: Claude <noreply@anthropic.com>"
```
