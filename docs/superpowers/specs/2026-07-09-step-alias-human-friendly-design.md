# 数据流式步骤别名——设计规格

> 版本：2026-07-09 | 状态：设计完成

## 问题

Spark 编译器为中间步骤生成 `_f{n}`、`_j{n}`、`_a{n}` 等序号别名，后续步骤引用 `_j3`、`_a4` 时无法从名称判断数据来源，管线可读性差。

## 目标

用数据流式命名替换序号别名，使生成的 PySpark 代码接近人手写的风格——变量名表达"这个 DataFrame 是什么状态"而非"这个步骤是什么类型"。

## 别名生成规则

| 步骤类型 | 规则 | 示例 |
|---------|------|------|
| **Read** | 直接使用 `step.alias`（即 `table_ref`） | `od`, `fact_orders`, `ft` |
| **Filter** | `{input}_filtered` | `ft_filtered` |
| **Sort** | `{input}_sorted` | `ft_filtered_sorted` |
| **Limit** | `{input}_top_{n}` | `ft_filtered_sorted_top_100` |
| **Join** | `{left}_with_{right}` | `od_with_ft` |
| **Aggregate**（有 metric + grain） | `{metric}_by_{grain}` | `revenue_by_day` |
| **Aggregate**（无 metric/grain） | `{input}_aggregated` | `ft_aggregated` |
| **Project**（中间） | `{input}_selected` | `od_with_ft_selected` |
| **Project**（最后） | `{input}_output` | `od_with_ft_selected_output` |
| **Window** | `{input}_windowed` | `ft_sorted_windowed` |
| **CaseWhen** | `{input}_with_{output_alias}` | `ft_with_trip_category` |

### 唯一性

同名追加 `_2`, `_3` 后缀：`ft_filtered`, `ft_filtered_2`, `ft_filtered_3`

### 截断

过长别名保留头尾，中间用 `__` 连接，总长不超过 **48 字符**：
`revenue_by_day_and_region_for_active_customers` → `revenue_by_day_and_region__customers`

### 标识符安全

所有别名经 `validate_identifier` 处理：仅保留 `[a-zA-Z0-9_]`，首字符必须为字母或下划线。

---

## 实现结构

### 新增文件

**`src/tianshu_datadev/spark/_alias_generator.py`**（~80 行）

```python
"""步骤别名生成器——数据流式命名。

compiler.py 和 mapper.py 共享此模块，确保别名规则一致。
"""

from .models import (
    SparkReadStep, SparkFilterStep, SparkJoinStep, SparkAggregateStep,
    SparkProjectStep, SparkSortStep, SparkLimitStep,
    SparkWindowStep, SparkCaseWhenStep,
)

_MAX_ALIAS_LEN = 48


def generate_step_alias(
    step,
    used_aliases: set[str] | None = None,
    is_last_project: bool = False,
) -> str:
    """为步骤生成数据流式输出别名。

    Args:
        step: SparkPlan 步骤实例
        used_aliases: 已用别名集合（用于冲突检测和递增后缀）
        is_last_project: 仅对 ProjectStep 有效——是否为管线中最后一个投影步骤

    Returns:
        符合 Python 标识符规则的别名
    """
    used = used_aliases or set()

    if isinstance(step, SparkReadStep):
        base = step.alias
    elif isinstance(step, SparkFilterStep):
        base = f"{step.input_alias}_filtered"
    elif isinstance(step, SparkSortStep):
        base = f"{step.input_alias}_sorted"
    elif isinstance(step, SparkLimitStep):
        base = f"{step.input_alias}_top_{step.limit}"
    elif isinstance(step, SparkJoinStep):
        base = f"{step.left_alias}_with_{step.right_alias}"
    elif isinstance(step, SparkAggregateStep):
        base = _agg_alias(step)
    elif isinstance(step, SparkProjectStep):
        suffix = "_output" if is_last_project else "_selected"
        base = f"{step.input_alias}{suffix}"
    elif isinstance(step, SparkWindowStep):
        base = f"{step.input_alias}_windowed"
    elif isinstance(step, SparkCaseWhenStep):
        base = f"{step.input_alias}_with_{step.output_alias}"
    else:
        base = f"step_output"

    base = _sanitize(base)
    base = _truncate(base, _MAX_ALIAS_LEN)

    # 唯一性处理
    alias = base
    n = 2
    while alias in used:
        alias = f"{base}_{n}"
        n += 1

    used.add(alias)
    return alias


def _agg_alias(step: SparkAggregateStep) -> str:
    """聚合步骤别名：{metric}_by_{grain} 或 {input}_aggregated。"""
    if step.metrics and step.group_keys:
        metric = step.metrics[0].alias or step.metrics[0].function.value
        grain = "_".join(step.group_keys[:2])  # 最多 2 个 grain 维度
        return f"{metric}_by_{grain}"
    return f"{step.input_alias}_aggregated"


def _sanitize(name: str) -> str:
    """清理为合法 Python 标识符：仅保留字母数字下划线。"""
    result = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if result and result[0].isdigit():
        result = "_" + result
    return result or "step_output"


def _truncate(name: str, max_len: int) -> str:
    """过长别名保留头尾，中间用 __ 连接。"""
    if len(name) <= max_len:
        return name
    head = name[:max_len // 2 + max_len % 2]
    tail = name[-max_len // 2 + 2:]
    return f"{head}__{tail}"
```

### 修改文件

#### `compiler.py`

1. 导入 `generate_step_alias`
2. `CompileState` 新增 `used_aliases: set[str]` 字段
3. 主编译循环预计算"最后一个 ProjectStep 的索引"
4. 7 个编译方法中 `out_alias = f"_{prefix}{index}"` 替换为 `generate_step_alias(step, state.used_aliases, is_last_project)`

具体位置：
- `_compile_filter` (line 318): `out_alias = f"_f{index}"` → 新调用
- `_compile_project` (line 368): `out_alias = f"_p{index}"` → 新调用 + `is_last_project` 判断
- `_compile_sort` (line 404): `out_alias = f"_s{index}"` → 新调用
- `_compile_limit` (line 437): `out_alias = f"_l{index}"` → 新调用
- `_compile_join` (line 465): `out_alias = f"_j{index}"` → 新调用
- `_compile_aggregate` (line 496): `out_alias = f"_a{index}"` → 新调用
- `_compile_case_when` (line 550): `out_alias = f"_c{index}"` → 新调用
- `_compile_window` (line 694): `out_alias = f"_w{index}"` → 新调用

#### `mapper.py`

- `_get_step_output_alias()` 改为委托 `generate_step_alias()`，保持向后兼容
- 注意：mapper 调用时没有 `used_aliases` 集合和 `is_last_project` 标记，需要适配

---

## 测试

### 新测试文件

**`tests/spark/test_alias_generator.py`**

| 测试 | 场景 |
|------|------|
| `test_read_alias_is_table_ref` | ReadStep 别名 = table_ref |
| `test_filter_alias_is_input_filtered` | FilterStep 别名 = `{input}_filtered` |
| `test_join_alias_is_left_with_right` | JoinStep 别名 = `{left}_with_{right}` |
| `test_agg_with_metric_and_grain` | 有 metric + group_keys → `{metric}_by_{grain}` |
| `test_agg_without_grain_fallback` | 无 group_keys → `{input}_aggregated` |
| `test_agg_without_metric_fallback` | 无 metrics → `{input}_aggregated` |
| `test_project_last_is_output` | is_last_project=True → `{input}_output` |
| `test_project_middle_is_selected` | is_last_project=False → `{input}_selected` |
| `test_window_alias_is_windowed` | WindowStep → `{input}_windowed` |
| `test_case_when_alias` | CaseWhenStep → `{input}_with_{output_alias}` |
| `test_limit_alias` | LimitStep → `{input}_top_{n}` |
| `test_sort_alias` | SortStep → `{input}_sorted` |
| `test_conflict_appends_suffix` | 同名 → 追加 `_2`, `_3` |
| `test_truncate_long_alias` | 超长别名截断为 head__tail |
| `test_sanitize_special_chars` | 特殊字符替换为下划线 |

### 更新现有测试

- `tests/spark/test_compiler.py`：硬编码 `_f0`/`_j1`/`_a2` 的断言改为匹配新规则
- `tests/spark/test_mapper.py`：同上

---

## 不变约束

- **不修改 Contract 模型**——不碰 `artifacts/models.py`
- **不修改 SparkPlan 步骤模型**——不碰 `spark/models.py`（别名生成仅依赖已有字段）
- **不影响 DuckDB 管线**——仅 Spark 编译器使用
- **向后兼容**——Snapshot 中的 `_inputs_index.json` 的 key 来自 ReadStep.alias（= table_ref），不受影响

---

## 示例对比

### 修改前

```python
od = inputs["orders"]
ft = inputs["fact_trips"]
_f2 = ft.filter(F.col("amount") > 100)
_f3 = _f2.filter(F.col("status") != "CANCELLED")
_j4 = od.join(_f3, on=od.id == _f3.order_id, how="inner")
_a5 = _j4.groupBy("day").agg(F.sum("amount").alias("revenue"))
_p6 = _a5.select(F.col("day"), F.col("revenue"))
```

### 修改后

```python
od = inputs["orders"]
ft = inputs["fact_trips"]
ft_filtered = ft.filter(F.col("amount") > 100)
ft_filtered_2 = ft_filtered.filter(F.col("status") != "CANCELLED")
od_with_ft_filtered_2 = od.join(ft_filtered_2, on=od.id == ft_filtered_2.order_id, how="inner")
revenue_by_day = od_with_ft_filtered_2.groupBy("day").agg(F.sum("amount").alias("revenue"))
revenue_by_day_output = revenue_by_day.select(F.col("day"), F.col("revenue"))
```
