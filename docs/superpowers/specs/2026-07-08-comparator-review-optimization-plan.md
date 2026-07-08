# COMPARATOR 审查缺陷优化计划

> 版本：2026-07-08 | 基于审查报告 2026-07-08-comparator-gap-fix-design.md 修复后的二次审查
> 当前测试基线：583 passed / 11 skipped / 0 failed（全量），plan_comparator 专项 55 passed

---

## 概述

在 8 个缺陷修复完成后进行了深度代码审查，发现 2 个 Critical、5 个 Important、4 个 Minor 问题。
本计划覆盖 **Critical + Important** 共 7 个问题（Minor 不纳入本轮修改）。

---

## CR-01：case_when 不对比 alias/output_alias

### 1. 问题分类

**逻辑漏检——COMPARATOR 核心语义对比缺失维度。**

### 2. 可复现事实与证据

**证据 A**：`compare_case_when_steps()`（`plan_equivalence.py:538-568`）在逐元素循环中对比了：
- labels → branches[].label（正确）
- default_value → else_value（正确）
- **未对比：alias → output_alias（缺失）**

**证据 B**：同级对比函数全部检查了 alias：
- `compare_project_steps`（line 465-471）：检查 `(column_name, alias)` 二元组
- `compare_window_steps`（line 617, 630）：检查 `(func, alias, partition, order)` 四元组

**复现步骤**：
```python
# SQL 侧 alias="label"
CaseWhenStep(cases=[...], else_value=SqlLiteral("other"), alias="label")
# Spark 侧 output_alias="wrong_label"
SparkCaseWhenStep(branches=[...], else_value="other", output_alias="wrong_label")
# → comparator.compare() 返回 LOGIC_EQUIVALENT（错误）
```

**证据 C**：现有测试 `test_case_when_equivalent`（test_plan_comparator.py:1091）中 SQL 和 Spark 的 alias/output_alias 恰好都是 `"label"`，因此目前通过——但这个测试**不能**检测 alias 不一致。

### 3. 正确行为

当 `sql_cw.alias != spark_cw.output_alias` 时，`compare_case_when_steps` 应返回 `NOT_EQUIVALENT`，detail 中明确写出两边的别名差异。

### 4. 不可改变的边界

- 不能改变 labels/branches 的对比逻辑（工作正常）
- 不能改变 else_value/default_value 的对比逻辑（工作正常）
- 不能改变 `normalize_field_name()` 的大小写归一化行为
- 允许 alias 为 `None` 或空字符串——两边都为空时视为等价（与 project/window 行为一致）

### 5. 本轮允许做到哪一步

1. 在 `compare_case_when_steps` 的逐元素循环中增加 alias 对比（约 6 行代码）
2. 新增 1 个测试：`test_case_when_alias_mismatch`——SQL alias="label"，Spark output_alias="wrong_label" → LOGIC_MISMATCH
3. 新增 1 个测试：`test_case_when_alias_both_empty`——两侧 alias 均为空字符串 → LOGIC_EQUIVALENT（边界行为）

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q -k "case_when"  # 所有 case_when 相关测试通过
pytest tests/spark/ -q  # 全量回归零退化
python -m ruff check src/tianshu_datadev/spark/plan_equivalence.py  # 零告警
```

---

## CR-02：compare_aggregate_steps 只对比 [0]

### 1. 问题分类

**结构性风险——多 aggregate 场景静默跳检。**

### 2. 可复现事实与证据

**证据 A**：`compare_aggregate_steps()`（`plan_equivalence.py:371-372`）在数量检查通过后：
```python
sql_agg = sql_aggs[0]    # 只取第一个
spark_agg = spark_aggs[0] # 只取第一个
```
如果 `len(sql_aggs) == len(spark_aggs) == 2`，第 2 个 aggregate 被完全忽略。

**证据 B**：当前调用路径中不会触发——单 SqlBuildPlan 最多 1 个 aggregate，DAG 归一化将多语句 aggregate 按 grain 合并。但代码中没有:
- 断言 `len == 1`
- 遍历全部元素的循环
- 注释说明为何只取 `[0]` 是安全的

**复现步骤**（构造性）：
```python
# 两侧各传入 2 个 aggregate（通过 _normalize_dag_steps 合并失败场景）
sql_aggs = [agg_a1, agg_a2]  # agg_a1 group_keys=["region"], agg_a2 group_keys=["city"]
spark_aggs = [agg_a1, agg_a2_wrong]  # agg_a2_wrong group_keys=["wrong"]
# → 数量检查通过（2==2），但只对比 agg_a1 vs agg_a1 → EQUIVALENT
# → agg_a2_wrong 的错误被静默放过
```

### 3. 正确行为

选择方案 A（Fail-Fast）：
- 在 `sql_count > 0` 时加 `assert len(sql_aggs) == len(spark_aggs) == 1`，明确暴露违反假设的调用
- 这比遍历循环更合适——因为 DAG 归一化确保每侧最多 1 个 aggregate，遍历循环是没有真实测试场景的死代码

### 4. 不可改变的边界

- 不能改变 DAG 归一化的合并逻辑（工作正常）
- 不能改变 group_keys/metrics 的对比逻辑（工作正常）
- 不能改变 `normalize_field_name()` 的行为
- assert 仅限 `compare_aggregate_steps` 内部，不影响 `compare_plans()` 的 try/except 外层

### 5. 本轮允许做到哪一步

1. 在 `compare_aggregate_steps` 中，`sql_count > 0` 路径（line 371 之后）增加 fail-fast 断言
2. 新增注释说明为何只取 `[0]`（解释 DAG 归一化保证）
3. 不新增测试——这是防御性断言，测试的是"假设不变"而非新功能

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q  # 全量通过，断言不会触发
pytest tests/spark/ -q  # 全量回归零退化
python -m ruff check src/tianshu_datadev/spark/plan_equivalence.py  # 零告警
```

---

## IM-01：_flatten_steps 未使用 mode='json'

### 1. 问题分类

**代码一致性问题——与其他提取方法序列化方式不一致。**

### 2. 可复现事实与证据

**证据 A**：`_flatten_steps`（`plan_comparator.py:669`）：
```python
step_dict = step.model_dump(exclude_none=True)  # 无 mode='json'
```

**证据 B**：`_extract_sql_step_data` 和 `_extract_spark_step_data` 都使用：
```python
step.model_dump(mode="json", exclude_none=True)
```

**证据 C**：inner_steps 直接 append 到 accumulator，未经 `_normalize_step_dict` 扁平化（line 674-675）。

**影响分析**：当前 subquery 在 `_NO_EQUIVALENCE_RULE_TYPES` 中，inner steps 不会被加入 covered 集——所以功能上暂不出错。但如果未来 subquery 规则被启用，枚举值序列化不一致 + 未扁平化会导致对比失效。

### 3. 正确行为

`_flatten_steps` 应使用 `mode="json"` 并调用 `_normalize_step_dict`，与 `_extract_sql_step_data`/`_extract_spark_step_data` 保持一致。

### 4. 不可改变的边界

- 不能改变 `_NO_EQUIVALENCE_RULE_TYPES` 的值
- 不能改变 subquery 的当前分类行为
- `_normalize_step_dict` 必须是幂等的（重复调用不产生副作用）

### 5. 本轮允许做到哪一步

1. 将 `step.model_dump(exclude_none=True)` 改为 `step.model_dump(mode="json", exclude_none=True)`
2. 将 inner_step 添加包装为 `_normalize_step_dict(inner_step)` 后再 append
3. 不新增专门测试——当前无 subquery 测试场景，修改仅保证一致性

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q  # 确认零回归
python -m ruff check src/tianshu_datadev/spark/plan_comparator.py  # 零告警
```

---

## IM-02：BETWEEN 右值正则无法处理含空格 datetime

### 1. 问题分类

**边界条件遗漏——正则捕获组无法处理带空格的值。**

### 2. 可复现事实与证据

**证据 A**：`_normalize_between_right_string`（`plan_comparator.py:1050`）：
```python
values = re.findall(r"value['\"]?\s*[:=]\s*['\"]?([^'\",}\s)]+)", right_str)
```
捕获组 `[^'\",}\s)]+` 的 `\s` 会排除所有空白字符。

**证据 B**：当前所有测试中的 BETWEEN 值均为纯数值（如 `"20260101"`）或纯字符串（如 `"a"`, `"z"`），没有带空格的值。

**复现步骤**：
```python
right_str = "[ContractPredicate(value='2026-01-01 00:00:00'), ContractPredicate(value='2026-01-31 23:59:59')]"
# re.findall 结果：['2026-01-01', '2026-01-31']  ← 截断了时间部分
# 正确结果应为：['2026-01-01 00:00:00', '2026-01-31 23:59:59']
```

**证据 C**：`_normalize_between_right_string` 仅处理 Spark 侧（Mapper 产出的 repr 字符串），SQL 侧通过 `_normalize_between_list` 处理（不受影响）。

### 3. 正确行为

放宽捕获组，允许含空格的字符串值。最小改动：将 `\s` 从排除字符类中移除，改为允许空格：
```python
values = re.findall(r"value['\"]?\s*[:=]\s*['\"]?([^'\"},)]+)", right_str)
```
（仅移除 `\s`——空白在 value 捕获中天然由引号或逗号边界终止）

### 4. 不可改变的边界

- 不能改变 `_normalize_between_list` 的逻辑（SQL 侧，工作正常）
- 不能改变 BETWEEN 右值的不排序语义（`sort_list=False`）
- 不能改变纯数值 BETWEEN 的现有行为

### 5. 本轮允许做到哪一步

1. 修改正则捕获组：`[^'\",}\s)]+` → `[^'\",})]+`（移除 `\s`）
2. 新增 1 个测试：`test_between_normalization_with_datetime_spaces`——验证带空格时间戳值的 BETWEEN 右值提取
3. 新增 1 个测试：`test_between_datetime_full_comparison`——端到端验证带空格时间戳的 BETWEEN filter 双向等价

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q -k "between"  # 所有 BETWEEN 相关测试通过
pytest tests/spark/ -q  # 全量回归零退化
python -m ruff check src/tianshu_datadev/spark/plan_comparator.py  # 零告警
```

---

## IM-03：IN/NOT_IN/LIKE/IS_NULL/IS_NOT_NULL 无测试覆盖

### 1. 问题分类

**测试覆盖缺口——谓词操作符渲染路径未经验证。**

### 2. 可复现事实与证据

**证据 A**：当前测试覆盖的操作符——GT、LT、EQ、BETWEEN、NOT、AND、OR、PREDICATE_TREE。

**证据 B**：当前测试**未覆盖**的操作符——IN、NOT_IN、LIKE、IS_NULL、IS_NOT_NULL。

**证据 C**：这些操作符的渲染逻辑**已在代码中实现**——通过 `_render_operand` + `_render_predicate_tree` 回调链处理，但无测试证明其正确性：
- IN 列表排序：`["c","a","b"]` → `"[a,b,c]"`（sort_list=True）
- NOT_IN 同上 + 否定语义
- LIKE 模式匹配：`(col LIKE "%pattern%")`
- IS_NULL 单目操作符：`right=None` → `"<NULL>"`
- IS_NOT_NULL：同上 + 否定

### 3. 正确行为

为每个未覆盖的操作符新增至少 1 个双向等价测试，关键验证点：
- IN：多元素列表排序后规范字符串与 Spark 侧一致
- IS_NULL：`None` → `"<NULL>"` 渲染与 Spark 侧一致
- LIKE：字符串模式保留原样

### 4. 不可改变的边界

- 不改变 `_render_operand` 的排序逻辑
- 不改变 `_render_predicate_tree` 的递归结构
- IN/NOT_IN 的列表排序行为必须保持（可交换性）
- IS_NULL 的 `<NULL>` 占位符不能改成其他值

### 5. 本轮允许做到哪一步

新增 3 个测试：
1. `test_filter_in_not_in_equivalent`——IN 和 NOT_IN 双向等价
2. `test_filter_is_null_equivalent`——IS_NULL/IS_NOT_NULL 双向等价
3. `test_filter_like_equivalent`——LIKE 操作符双向等价

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q -k "filter"  # 所有 filter 相关测试通过
pytest tests/spark/ -q  # 全量回归零退化
python -m ruff check tests/spark/test_plan_comparator.py  # 零告警
```

---

## IM-04：_map_comparator_status 缺失参数类型标注

### 1. 问题分类

**类型安全性——签名为 `Any` 允许非法输入。**

### 2. 可复现事实与证据

**证据 A**：`pipeline.py:2746`：
```python
def _map_comparator_status(status) -> str:
```
`status` 参数无类型标注。

**证据 B**：测试 `test_comparator_status_unknown_fallback` 显式传入了 `None` 和字符串，并标注了 `# type: ignore[arg-type]` ——说明类型检查器会拒绝合法签名。

**证据 C**：函数内部已有防御兜底 `_status_map.get(status, "HUMAN_REVIEW")`，运行时不会崩溃。

### 3. 正确行为

签名改为 `def _map_comparator_status(status: ComparisonStatus) -> str:`，防御兜底保留（处理运行时的意外 None 值），但静态类型禁止新调用方传非 ComparisonStatus 值。

### 4. 不可改变的边界

- 不改变 `_status_map` 的映射关系
- 不改变防御兜底返回 `"HUMAN_REVIEW"` 的行为
- 测试中的 `# type: ignore[arg-type]` 必须保留——这是显式的边界测试

### 5. 本轮允许做到哪一步

1. 修改函数签名为 `def _map_comparator_status(status: ComparisonStatus) -> str:`
2. 测试文件不做任何改动（已有的 `# type: ignore[arg-type]` 保持不变）

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q -k "status"  # 状态映射测试通过
python -m ruff check src/tianshu_datadev/api/pipeline.py  # 零告警
```

---

## IM-05：多个 filter step 不检查内部条件顺序

### 1. 问题分类

**文档/注释缺失——设计选择未记录，易被误判为缺陷。**

### 2. 可复现事实与证据

**证据 A**：`compare_filter_steps`（`plan_equivalence.py:241-243`）将所有 filter 条件通过 `sorted()` 排序后对比集合，不保留原始顺序。

**证据 B**：这是利用 AND/OR 可交换性的有意设计——`WHERE a>1 AND b<10` 与 `WHERE b<10 AND a>1` 语义等价。

**证据 C**：步骤间顺序由 `check_order=True`（步骤类型签名）保护——但代码中无注释说明这个分工。

### 3. 正确行为

在 `compare_filter_steps` 的条件收集代码上方增加注释，说明 sorted 是有意为之（利用可交换性），步骤间顺序由 `check_order` 参数保护。

### 4. 不可改变的边界

- 不改变 sorted() 的对比逻辑
- 不改变 check_order 的行为

### 5. 本轮允许做到哪一步

1. 在 `compare_filter_steps` 中 filter 条件收集处增加 1 行注释
2. 不新增测试

### 6. 验收方式

```bash
pytest tests/spark/test_plan_comparator.py -q  # 确认零回归
python -m ruff check src/tianshu_datadev/spark/plan_equivalence.py  # 零告警
```

---

## 修复任务汇总

| 编号 | 问题简述 | 改动文件 | 改动量 | 新增测试 |
|:----:|---------|---------|:-----:|:------:|
| CR-01 | case_when 不对比 alias | `plan_equivalence.py` | ~6 行 | +2 |
| CR-02 | aggregate 只取 [0] | `plan_equivalence.py` | ~3 行 | 0 |
| IM-01 | _flatten_steps 缺 mode='json' | `plan_comparator.py` | ~3 行 | 0 |
| IM-02 | BETWEEN 正则无法处理空格 | `plan_comparator.py` | ~2 行 | +2 |
| IM-03 | IN/LIKE/IS_NULL 无测试 | `test_plan_comparator.py` | ~80 行 | +3 |
| IM-04 | _map_comparator_status 缺类型 | `pipeline.py` | ~1 行 | 0 |
| IM-05 | filter sorted 缺注释 | `plan_equivalence.py` | ~1 行 | 0 |

**总计**：3 个生产文件、1 个测试文件，约 96 行改动，新增 7 个测试用例。

---

## 全局验收

```bash
# 最终全量回归
pytest tests/spark/ -q

# 零告警
python -m ruff check src/tianshu_datadev/spark/plan_equivalence.py \
  src/tianshu_datadev/spark/plan_comparator.py \
  src/tianshu_datadev/api/pipeline.py \
  tests/spark/test_plan_comparator.py

# git diff 无异常
git diff --check
```
