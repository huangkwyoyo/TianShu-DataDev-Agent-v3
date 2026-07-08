# Task 2 报告：scan 列集合按 alias 分组对比（缺陷 4）

## 状态

**DONE**

## 提交

```
2ffa1ef fix(comparator): scan 列集合按 alias 分组对比
```

## 测试结果

### Step 2：失败确认（TDD 红）

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestPlanComparatorScanEquivalence::test_scan_columns_mismatch_detected_by_alias -v
```
**FAILED** — 现有代码未检查 required_columns，SQL 侧 3 列 vs Spark 侧 2 列仍返回 EQUIVALENT（错误），验证了缺陷存在。

### Step 5：实现后测试通过

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestPlanComparatorScanEquivalence -v
```
**3 passed** — 原有 2 个测试（等价、别名不等）+ 新增 1 个（列集合不等）全部通过。

### 回归测试

```bash
python -m pytest tests/spark/test_plan_comparator.py -v
```
**46 passed** — 全部 46 个测试通过，未破坏 filter/project/sort/limit/aggregate/join/case_when/not_covered/混合场景/多语句/归一化 等现有逻辑。

## 变更内容

### `src/tianshu_datadev/spark/plan_equivalence.py`

- 新增 `_extract_column_name(col) -> str`：统一提取列名，兼容 SQL 侧 ColumnRef dict（优先取 `normalized_name`）和 Spark 侧纯字符串
- `compare_scan_steps()`：在别名对比通过后、EQUIVALENT return 之前，插入按 alias 分组的列集合对比逻辑：
  - 内部函数 `_collect_scan_columns` 按 alias 收集 `set[str]`
  - 仅在两侧共有的 alias 上对比
  - 不一致时返回 `NOT_EQUIVALENT`，detail 含仅在 SQL/Spark 侧的列名
  - 任一侧 `required_columns` 为空时跳过该 alias 的列对比（向后兼容）

### `tests/spark/test_plan_comparator.py`

- 新增 `test_scan_columns_mismatch_detected_by_alias`：SQL 侧 3 列（含 status）、Spark 侧 2 列（缺 status），同 alias "od" —— 验证返回 NOT_EQUIVALENT 且 detail 包含 "status"

## 关注点

无。
