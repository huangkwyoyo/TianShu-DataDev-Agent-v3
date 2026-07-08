# COMPARATOR 机制 8 个缺陷修复——设计方案

> 文档版本：2026-07-08 | 基于系统性调试 Phase 1-3 的根因分析 + 3 轮方案审查

## 背景

COMPARATOR（SQL ↔ Spark 逻辑链路对比）与 PHYSICAL_VERIFIER（双引擎物理结果对比）之间存在 8 个覆盖缺口。当前 PHYSICAL_VERIFIER 为 COMPARATOR 的多个盲区做了事实上的兜底，但这是靠"物理执行发现差异"而非"逻辑结构提前预警"。如果物理验证环境不可用（PySpark 未安装），这些缺陷会变成未检测到的正确性问题。

## 全局约束

- 生产代码仅限 3 个文件：`plan_equivalence.py`、`plan_comparator.py`、`pipeline.py`。测试文件按需新增/修改
- 不破坏 9 个单类型对比函数的现有实现
- 非嵌套 filter / 非 window 的现有测试预期不变
- 所有注释和测试使用中文

---

## 缺陷 1 🔴：步骤执行顺序不检查

**根因**：`compare_plans()` 按类型分组后 sorted 集合对比，完全丢弃步骤顺序。

**方案**：新增 `check_order` 参数，在分组对比之后生成类型顺序签名。

**改动文件**：`plan_equivalence.py`（`compare_plans`）+ `plan_comparator.py`（`compare` / `compare_program` 调用方）

**实现要点**：

1. `compare_plans(sql_steps, spark_steps, ..., check_order=True)` 新增参数
2. 当 `check_order=True` 时：从原始 step 列表按顺序提取归一化类型名，生成签名列表。两侧严格相等 → 通过；不等 → 新增 `StepEquivalenceResult(step_type="order", verdict=NOT_EQUIVALENT)`
3. 单 `SqlBuildPlan` 路径（`compare()`）：`check_order=True`（默认）
4. `SqlProgram` 路径（`compare_program()`）：内部调用 `compare_plans(..., check_order=False)`——DAG 扁平化 + `_normalize_dag_steps` 合并 aggregate/project 后，步骤顺序已不是原始执行顺序，全局签名会误伤
5. 签名中的类型名使用 `_SQL_TYPE_TO_NORMALIZED` 归一化后的名称（scan/read 统一为 scan）

**测试**：构造 SQL Plan `[scan, filter, sort]` vs Spark Plan `[scan, sort, filter]` → `NOT_EQUIVALENT`，step_type="order"

---

## 缺陷 2 🔴：嵌套 AND/OR 谓词扁平化丢失

**根因**：`Predicate.left` 可以是 `ColumnRef | Predicate`（嵌套），但 `_flatten_filter_step` 的 `_column_ref_to_string` 只处理 ColumnRef 和 SqlLiteral dict，遇到嵌套 Predicate dict 时返回空字符串。

**方案**：结构判别 + 递归规范字符串渲染 + `compare_filter_steps` 特殊路径。

**改动文件**：`plan_comparator.py`（`_flatten_filter_step` + 新增 `_is_predicate_tree` + `_render_predicate_tree`）+ `plan_equivalence.py`（`compare_filter_steps` 归一化逻辑）

**实现要点**：

### 结构判别

```python
def _is_predicate_tree(d: dict) -> bool:
    """含 left + operator 键 → Predicate tree（嵌套）；否则是 ColumnRef/SqlLiteral"""
    return "left" in d and "operator" in d
```

ColumnRef dict 特征：`normalized_name` / `column_name` / `table_ref`
嵌套 Predicate dict 特征：`left` / `operator` / `right`

### `_flatten_filter_step` 分支

```python
if isinstance(left_val, dict) and _is_predicate_tree(left_val):
    rendered = _render_predicate_tree(predicate)
    result["left"] = rendered
    result["operator"] = "PREDICATE_TREE"
    result["right"] = ""
else:
    # 原路径不变——非嵌套 ColumnRef/SqlLiteral
```

### `_render_operand(value) -> str`——统一操作数渲染

叶子节点的 left/right 可能是 ColumnRef dict、SqlLiteral dict、list（IN/BETWEEN）、None（IS_NULL），需统一渲染为规范字符串，避免 Python dict/list repr 泄漏：

```python
def _render_operand(value: Any) -> str:
    """将操作数统一渲染为规范字符串——消除 SQL/Spark 序列化差异。"""
    if value is None:
        return "<NULL>"              # IS_NULL / IS_NOT_NULL
    if isinstance(value, dict):
        if "normalized_name" in value or "column_name" in value:
            # ColumnRef → 归一化字段名
            return normalize_field_name(
                value.get("normalized_name") or value.get("column_name", "")
            )
        if "value" in value:
            # SqlLiteral → 提取值
            return str(value["value"])
        # 其他 dict（防御）→ JSON 稳定序列化
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, list):
        # IN / BETWEEN 右值列表 → 递归 + 排序
        rendered = sorted(_render_operand(v) for v in value)
        return "[" + ",".join(rendered) + "]"
    return str(value)
```

### `_render_predicate_tree(predicate_dict) -> str`

- 叶子节点（left 和 right 都不是 Predicate tree）：通过 `_render_operand` 渲染 left/right，输出 `(rendered_left operator rendered_right)`
- AND 节点：递归渲染子树，按字母序排序后 `" AND "` 拼接，外层括号包裹
- OR 节点：同上，`" OR "` 拼接——OR 也有可交换性
- NOT 节点：单子树，不排序（无交换性）
- 顶层包裹 `(rendered)`
- 示例：`OR(AND(a GT 1, b LT 10), EQ(c, 0))` → `"((a GT 1 AND b LT 10) OR c EQ 0)"`
- IS_NULL 示例：`NOT(IS_NULL(status))` → `"(NOT (status IS_NULL <NULL>))"`

### `compare_filter_steps` 归一化

```python
def _normalize_filter_tuple(f: dict) -> tuple:
    if f.get("operator", "").upper() == "PREDICATE_TREE":
        # 规范字符串已在渲染时预归一化，直接对比，不走 normalize_field_name
        return (f.get("left", ""), "PREDICATE_TREE", "")
    return (
        normalize_field_name(f.get("left", "")),
        f.get("operator", "").upper(),
        normalize_field_name(f.get("right", "")),
    )
```

**关键**：双端配合——`_render_predicate_tree` 叶子节点预归一化 + `compare_filter_steps` 跳过二次归一化。否则 `normalize_field_name` 遇到 `table.column` 中的 `.` 会 `split(".")[-1]` 截断整个表达式。

**测试**：构造嵌套谓词 `OR(AND(a>1, b<10), EQ(c,0))` → 正确渲染并对比

---

## 缺陷 3 🟡：window 标记 NOT_COVERED 但规则已实现

**根因**：`compare_window_steps` 在 `plan_equivalence.py:523-604` 已完整实现（分区键、排序键、窗口函数名、别名），但 `PlanComparator` 仍将其标记为 NOT_COVERED。

**方案**：将 `"window"` 从 `_NOT_YET_COVERED_TYPES` 移到 `_PHASE_7B_ENABLED_TYPES`。

**改动文件**：`plan_comparator.py`

**实现要点**：
- `_PHASE_7B_ENABLED_TYPES` 加入 `"window"`，注释从"8 种"改为"9 种"
- `_NOT_YET_COVERED_TYPES` 移除 `"window"`，只剩 `"subquery"`
- `"subquery"` 保持 NOT_COVERED——Spark 侧无 SubqueryStep 对应类型

**测试**：包含 window 的 Plan 对比 → `LOGIC_EQUIVALENT`（非 NOT_COVERED）

---

## 缺陷 4 🟡：Scan 对比不检查列集合

**根因**：`compare_scan_steps()` 只对比表别名数量和名称，忽略 `required_columns`。

**方案**：按 alias 分组对比列集合——全局 set 会丢失多表同名列信息（`a.id` 和 `b.id` 都坍缩为 `id`）。

**改动文件**：`plan_equivalence.py`（`compare_scan_steps` + 新增 `_extract_column_name` helper）

**实现要点**：

### Helper：统一提取列名

```python
def _extract_column_name(col: Any) -> str:
    """兼容 ColumnRef dict（SQL 侧）和纯字符串（Spark 侧）。"""
    if isinstance(col, dict):
        return normalize_field_name(
            col.get("normalized_name") or col.get("column_name", "")
        )
    return normalize_field_name(str(col))
```

### 按 alias 分组对比

```python
# {alias: set(columns)}
sql_cols_by_alias = {}
for s in sql_scans:
    alias = normalize_field_name(s.get("table_ref", ""))
    cols = {_extract_column_name(c) for c in (s.get("required_columns", []) or [])}
    cols.discard("")  # 去掉提取失败的
    if cols:
        sql_cols_by_alias[alias] = cols

spark_cols_by_alias = {}
for r in spark_reads:
    alias = normalize_field_name(r.get("alias", ""))
    cols = {_extract_column_name(c) for c in (r.get("required_columns", []) or [])}
    cols.discard("")
    if cols:
        spark_cols_by_alias[alias] = cols

# 只在两侧共有的 alias 上对比
common = set(sql_cols_by_alias) & set(spark_cols_by_alias)
for alias in sorted(common):
    if sql_cols_by_alias[alias] != spark_cols_by_alias[alias]:
        # → NOT_EQUIVALENT
```

**边界**：
- 任一侧 `required_columns` 为空 → 跳过列对比（向后兼容 Phase 5）
- 仅两侧都非空的 alias 参与对比——单侧有列集合不构成差异

**测试**：SQL 读 5 列、Spark 读 3 列（同 alias）→ `NOT_EQUIVALENT`

---

## 缺陷 5 🟡：`_do_spark_compare` 硬编码 SUCCESS

**根因**：`pipeline.py:2777-2778` 无论 `report.status` 是什么都写 `"SUCCESS"`，掩盖阶段级真实状态。

**方案**：状态映射表，非 SUCCESS/FAILURE 的枚举映射为 `HUMAN_REVIEW` 或 `SKIPPED`。

**改动文件**：`pipeline.py`（`_do_spark_compare`）

**实现要点**：

```python
_status_map = {
    ComparisonStatus.LOGIC_EQUIVALENT: "SUCCESS",
    ComparisonStatus.LOGIC_MISMATCH: "FAILURE",
    ComparisonStatus.LOGIC_UNSUPPORTED: "HUMAN_REVIEW",
    ComparisonStatus.NOT_COVERED: "HUMAN_REVIEW",
    ComparisonStatus.NOT_EXECUTED: "SKIPPED",
}
context.stage_results["COMPARATOR"] = _status_map.get(report.status, "HUMAN_REVIEW")
```

详细状态保留在 `context.comparator_report.status`，`derive_overall_status` 消费。

**无需改 `derive_overall_status`**——它已经正确检查 `comparator_report.status`。

**测试**：验证 `LOGIC_MISMATCH` → stage_result="FAILURE"，`LOGIC_UNSUPPORTED` → stage_result="HUMAN_REVIEW"

---

## 缺陷 6 🟡：未知 step 类型静默跳过

**根因**：`compare_plans()` 中 `_UNSUPPORTED_STEP_TYPES` 和 `_STEP_COMPARATORS` 外的类型只 `continue`，不产生 `StepEquivalenceResult`。

**方案**：两类都补 `StepEquivalenceResult(verdict=UNSUPPORTED_COMPARISON)`。

**改动文件**：`plan_equivalence.py`（`compare_plans`）

**实现要点**：

```python
if stype in _NO_EQUIVALENCE_RULE_TYPES:
    unsupported_types.append(stype)
    step_results.append(StepEquivalenceResult(
        step_type=stype,
        verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
        detail=f"'{stype}' 无等价对比规则",
    ))
    continue

if stype not in _STEP_COMPARATORS:
    unsupported_types.append(stype)
    step_results.append(StepEquivalenceResult(
        step_type=stype,
        verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
        detail=f"未知 step 类型 '{stype}'——不在 _STEP_COMPARATORS 注册表中",
    ))
    continue
```

**测试**：包含 subquery 的 Plan → step_results 中有 subquery 的 UNSUPPORTED_COMPARISON 条目

---

## 缺陷 7 🟢：Sort 不对比 null_order

**根因**：`compare_sort_steps()` 只对比 `(column, direction)`，不对比 `null_order`。

**确认**：Spark `SparkSortSpec` 只有 `column` + `direction`，无 `null_order` 字段。SQL `SortSpec` 有 `null_order: NullOrder = NullOrder.LAST`。

**方案**：sort key 元组增加第三元素，Spark 侧默认 `"LAST"`（大写，与 SQL `.upper()` 一致）。

**改动文件**：`plan_equivalence.py`（`compare_sort_steps`）

**实现要点**：

```python
# SQL 侧——从 SortSpec 提取 null_order
sql_keys.append((
    normalize_field_name(item.get("column", "")),
    (item.get("direction", "asc") or "asc").upper(),
    (item.get("null_order", "last") or "last").upper(),
))

# Spark 侧——SparkSortSpec 无 null_order，默认 LAST
spark_keys.append((
    normalize_field_name(item.get("column", "")),
    (item.get("direction", "asc") or "asc").upper(),
    "LAST",  # 大写，与 SQL .upper() 一致
))
```

**行为**：SQL 默认 `NULLS LAST` → 与 Spark 默认一致 → EQUIVALENT。SQL 写 `NULLS FIRST` → 与 Spark `LAST` 不一致 → NOT_EQUIVALENT（正确暴露缺口，而非误报）。

**测试**：SQL `ORDER BY col NULLS FIRST` vs Spark 默认 → `NOT_EQUIVALENT`

---

## 缺陷 8 🟢：术语不一致

**根因**：`plan_equivalence.py` 的 `_UNSUPPORTED_STEP_TYPES` 与 `PlanComparator._NOT_YET_COVERED_TYPES` 对 subquery 用不同术语描述同一情况。

**方案**：重命名为 `_NO_EQUIVALENCE_RULE_TYPES`，加注释说明两者关系。

**改动文件**：`plan_equivalence.py`

**实现要点**：

```python
# 无等价对比规则的 step 类型。
# 与 PlanComparator._NOT_YET_COVERED_TYPES 的区别：
#   - 此集合：对比规则不存在（如 subquery——Spark 侧无对应类型，无法设计规则）
#   - _NOT_YET_COVERED_TYPES：规则已存在但本 Phase 未启用（如 Phase 7B 的 window）
_NO_EQUIVALENCE_RULE_TYPES: set[str] = {"subquery"}
```

---

## 影响面汇总

| 缺陷 | 文件 | 改动量（估） | 需新增测试 |
|:---:|------|:---:|:---:|
| 1 | `plan_equivalence.py` + `plan_comparator.py` | ~25 行 | 顺序不一致 → NOT_EQUIVALENT |
| 2 | `plan_comparator.py` + `plan_equivalence.py` | ~60 行 | 嵌套 AND/OR → 正确渲染对比 |
| 3 | `plan_comparator.py` | 2 行 | window → LOGIC_EQUIVALENT |
| 4 | `plan_equivalence.py` | ~25 行 | 同 alias 列集合不一致 → 检测 |
| 5 | `pipeline.py` | 10 行 | 各 ComparisonStatus 映射正确 |
| 6 | `plan_equivalence.py` | 10 行 | subquery 产生 step_result 条目 |
| 7 | `plan_equivalence.py` | 2 行 | NULLS FIRST vs LAST → NOT_EQUIVALENT |
| 8 | `plan_equivalence.py` | 3 行 | 无需新测试（纯重命名） |

**总改动**：~170 行，3 个生产文件 + 测试文件。预计新增至少 7 个聚焦测试用例（order、嵌套 AND/OR、scan 列集合按 alias、comparator status 映射、subquery step_result、null_order、window enabled），避免压缩风险覆盖。

---

## 验证

```bash
# 全量回归
pytest tests/spark/ -v

# ruff 零告警
python -m ruff check src/tianshu_datadev/spark/plan_equivalence.py \
    src/tianshu_datadev/spark/plan_comparator.py \
    src/tianshu_datadev/api/pipeline.py

# git diff 格式检查
git diff --check
```
