# Spark 双链 Comparator 内容级对齐——设计文档

> 状态：设计确认 | 日期：2026-07-06
> 目标：消除 SQL DAG 扁平化 vs Spark Mapper 之间的 scan/join/aggregate 内容级差异，使 Case 06 达到 LOGIC_EQUIVALENT

## 一、问题定义

Case 06（NYC 区域安全合规画像）使用 7 步 SqlProgram DAG，`_temp_*` 临时表串联多步独立聚合→Join→计算→标签。`PlanComparator.compare_program()` 将多语句扁平化为单一 step 列表后与 Mapper 产出的 SparkPlan 对比，当前判 `LOGIC_MISMATCH`。

根因：三层内容级差异——扁平化后的 SQL 侧 step 仍携带 DAG 内部管道信息（`_temp_*` join、不同粒度的 aggregate、可能的 `_temp_*` scan），而 Spark 侧 Mapper 从 Contract 生成的是纯业务级 step。

## 二、总体策略："三层剥离"

```
SQL Program DAG
    │
    ▼ _flatten_sql_program_steps()
    │
    ├─ 剥离层 1：过滤 _temp_* scan（已实现）
    ├─ 剥离层 2：过滤 _temp_* join（10.x-A，新增）
    │
    ▼ _normalize_dag_steps()
    │
    ├─ 剥离层 3：aggregate 同粒度合并 + target_grain 过滤（10.x-B1 + B2）
    │
    ▼
业务级可比 step 列表 ──► compare_plans() ──► LOGIC_EQUIVALENT
```

## 三、执行步骤

### Step 0：增强 diagnostic detail

**文件**：`src/tianshu_datadev/spark/plan_equivalence.py`

**改动**：在 `compare_join_steps`、`compare_aggregate_steps`、`compare_scan_steps` 的 `NOT_EQUIVALENT` detail 中输出两侧归一化后的值，使 mismatch 可直接定位。

```python
# compare_join_steps detail 增强示例
detail=(
    f"Join 规格不一致——"
    f"SQL 侧: {sql_normalized}，"
    f"Spark 侧: {spark_normalized}"
)
```

**测试**：无需新增——现有测试的 detail 字段更丰富即为验证。

---

### Step A：过滤 _temp_* join（10.x-A）

**文件**：`src/tianshu_datadev/spark/plan_comparator.py`
**位置**：`_flatten_sql_program_steps()`，紧接现有 `_temp_` scan 过滤

**改动**：

```python
# 现有：过滤 _temp_* scan
if step_type == "scan":
    table_ref = step_dict.get("table_ref", "")
    if isinstance(table_ref, str) and table_ref.startswith("_temp_"):
        continue  # 跳过 _temp_ 中间表 scan

# 新增：过滤 _temp_* join——DAG 内部管道 join，Spark 侧无对应
if step_type == "join":
    lt = step_dict.get("left_table_ref", "")
    rt = step_dict.get("right_table_ref", "")
    if (isinstance(lt, str) and lt.startswith("_temp_")) or \
       (isinstance(rt, str) and rt.startswith("_temp_")):
        continue
```

**约束**：
- 不修改 `compare()` 方法（单 plan 路径）
- 不新增报告字段（审计追踪通过测试注释覆盖）
- 不影响源表 join（如 `tz ↔ zts`）

**测试**（`tests/spark/test_plan_comparator.py`）：
- `test_temp_join_filtered_from_compare_program`：含 `_temp_* ↔ _temp_*` join 的 SqlProgram，扁平化后 join 列表为空
- `test_source_join_preserved_in_compare_program`：`tz ↔ zts` 的源表 join 仍保留且参与对比

---

### Step C：验证/修复 Contract 不抽取 _temp_* scan（10.x-C）

**文件**：`src/tianshu_datadev/artifacts/contract_extractor.py`
**位置**：`_extract_scan_v1()` 或 `extract_v1()` 主循环

**验证**：检查 `_extract_scan_v1()` 是否对 `table_ref.startswith("_temp_")` 有过滤

**如未过滤则改动**：

```python
# 在 _extract_scan_v1() 中新增 _temp_ 前缀守卫
def _extract_scan_v1(self, step, input_tables, input_columns, seen_tables, seen_columns):
    # _temp_* 表是 DAG 内部管道——不进入 Contract
    if step.table_ref.startswith("_temp_"):
        return
    # ... 原有逻辑
```

或在 `extract_v1()` 遍历循环中加守卫：

```python
if isinstance(step, ScanStep):
    if step.table_ref.startswith("_temp_"):
        continue  # DAG 内部临时表——不进入 Contract
    self._extract_scan_v1(...)
```

**约束**：
- 过滤位置与 Comparator 的 `_temp_` scan 过滤对称
- 不影响源表 scan 的提取
- Contract 的语义：只描述"业务输入"，不暴露 DAG 实现细节

**测试**（`tests/artifacts/test_contract_extractor.py`）：
- `test_contract_input_tables_excludes_temp`：从含 `_temp_*` scan 的 SqlProgram 提取 Contract，断言 input_tables 不含 `_temp_` 前缀

---

### Step B1：aggregate 按 grain 分组合并（10.x-B1）

**文件**：`src/tianshu_datadev/spark/plan_comparator.py`
**位置**：`_normalize_dag_steps()`

**改动**：将当前"无条件合并所有 aggregate"改为"按 `group_keys` 签名分组合并"

```python
# 按 group_keys 签名分组——禁止跨粒度硬合并
agg_groups: dict[tuple, dict] = {}
for step in sql_steps:
    if step.get("step_type") == "aggregate":
        gk_tuple = tuple(sorted(step.get("group_keys", [])))
        if gk_tuple not in agg_groups:
            agg_groups[gk_tuple] = {
                "group_keys": list(gk_tuple),
                "metrics": [],
                "seen_aliases": set(),
            }
        for m in step.get("metrics", []):
            alias = m.get("alias", "")
            if alias not in agg_groups[gk_tuple]["seen_aliases"]:
                agg_groups[gk_tuple]["seen_aliases"].add(alias)
                agg_groups[gk_tuple]["metrics"].append(m)

# 将分组后的 aggregate 插入结果
for gk_tuple, agg_data in agg_groups.items():
    merged_agg = {
        "step_type": "aggregate",
        "group_keys": agg_data["group_keys"],
        "metrics": agg_data["metrics"],
    }
    # 找到合适的插入位置（最后一个 scan/filter/join/read 之后）
    insert_pos = 0
    for i, s in enumerate(result):
        if s.get("step_type") in ("scan", "filter", "join", "read"):
            insert_pos = i + 1
    result.insert(insert_pos, merged_agg)
```

**Case 06 效果**：
- `[borough]` → 6 metrics（statement 0 + 2 合并）
- `[violation_county]` → 2 metrics（statement 1 独立保留）

**测试**（`tests/spark/test_plan_comparator.py`）：
- `test_aggregate_same_grain_merged`：多个同 `[borough]` aggregate → 合并为 1 个
- `test_aggregate_different_grain_kept_separate`：`[borough]` + `[violation_county]` → 产出 2 个独立 aggregate

---

### Step B2：target_grain 过滤——只保留最终业务粒度（10.x-B2）

**文件**：`src/tianshu_datadev/spark/plan_comparator.py`

**改动点 1**：`_normalize_dag_steps` 新增 `target_grain` 可选参数

```python
@staticmethod
def _normalize_dag_steps(
    sql_steps: list[dict[str, Any]],
    target_grain: list[str] | None = None,  # 新增：目标粒度
) -> list[dict[str, Any]]:
```

在 B1 分组后，若 `target_grain` 非空，只保留 group_keys 与 target_grain 集合相等的 aggregate 组：

```python
# B2：target_grain 过滤——只保留最终业务粒度
if target_grain is not None:
    target_set = set(target_grain)
    filtered_groups = {}
    for gk_tuple, agg_data in agg_groups.items():
        if set(gk_tuple) == target_set:
            filtered_groups[gk_tuple] = agg_data
    agg_groups = filtered_groups
```

**改动点 2**：`compare_program` 新增 `target_grain` 参数并透传

```python
def compare_program(
    self,
    sql_program: SqlProgram,
    spark_plan: SparkPlan,
    annotations: list | None = None,
    warnings: list[AnnotationWarning] | None = None,
    enabled_step_types: set[str] | None = None,
    target_grain: list[str] | None = None,  # 新增
) -> PlanComparisonReport:
    # ...
    sql_steps_data = self._normalize_dag_steps(sql_steps_data, target_grain=target_grain)
```

**改动点 3**：`SparkOrchestrator.run()` 从 Contract 提取 `grouping_keys` 并传入

```python
# orchestrator.py 中调用 compare_program 时
target_grain = contract.grouping_keys if hasattr(contract, 'grouping_keys') else None
report = self.comparator.compare_program(
    sql_program, spark_plan,
    target_grain=target_grain,
)
```

**约束**：
- `target_grain=None` 时行为与 B1 完全一致（向后兼容）
- 过滤的是非目标粒度 aggregate——`[violation_county]` 是 DAG 内部实现细节

**测试**（`tests/spark/test_plan_comparator.py`）：
- `test_target_grain_filters_irrelevant_aggregate`：传入 `target_grain=["borough"]`，`[violation_county]` aggregate 被过滤
- `test_target_grain_none_preserves_all`：`target_grain=None` 保留所有 aggregate 组

---

### Step D：Case 06 xfail 转正（10.x-D）

**文件**：`tests/api/test_nyc_business_case.py`

**改动**：
- `test_spark_orchestrator_logic_equivalence`：移除 `@pytest.mark.xfail` 装饰器，`strict=True` 断言改为正常 `assert`
- 确认 `test_spark_comparator_report_not_mismatch` 测试不再需要（或其语义调整为严格等价断言）

**测试**：Step D 本身即为测试——Case 06 全链路 `LOGIC_EQUIVALENT`

---

## 四、不变更的组件

| 组件 | 理由 |
|------|------|
| `plan_equivalence.py` 的 9 条对比规则 | 规则本身正确——问题在输入数据未对齐 |
| `Contract` 模型 | 不引入 `_temp_` 相关字段 |
| `Mapper` | 不改变其从 Contract 生成 SparkPlan 的逻辑 |
| `PlanComparisonReport` 模型 | 本 Phase 不新增 `normalization_notes` 字段 |
| `compare()` 方法（单 plan 路径） | 所有改动仅影响 `compare_program()` 多语句路径 |

## 五、不可触碰边界

1. **CTE 禁止**——不变
2. **raw_sql 禁止**——不变
3. **不修改 SqlProgram IR Schema**
4. **不修改 Compiler/Executor 核心逻辑**
5. **不引入新 step 类型**
6. **Contract 不承载 DAG 内部管道信息**
7. **compare() 单 plan 路径零退化**

## 六、测试清单

| 步骤 | 测试 | 文件 |
|:--:|------|------|
| 0 | 现有测试的 mismatch detail 更可读 | 无需新增 |
| A | `test_temp_join_filtered_from_compare_program` | `test_plan_comparator.py` |
| A | `test_source_join_preserved_in_compare_program` | `test_plan_comparator.py` |
| C | `test_contract_input_tables_excludes_temp` | `test_contract_extractor.py` |
| B1 | `test_aggregate_same_grain_merged` | `test_plan_comparator.py` |
| B1 | `test_aggregate_different_grain_kept_separate` | `test_plan_comparator.py` |
| B2 | `test_target_grain_filters_irrelevant_aggregate` | `test_plan_comparator.py` |
| B2 | `test_target_grain_none_preserves_all` | `test_plan_comparator.py` |
| D | `test_spark_orchestrator_logic_equivalence` 转正 | `test_nyc_business_case.py` |

## 七、回归范围

```bash
# 核心变更文件测试
python -m pytest tests/spark/test_plan_comparator.py tests/artifacts/test_contract_extractor.py -q

# Case 06 集成测试
python -m pytest tests/api/test_nyc_business_case.py -q

# 全量回归（零退化基线：≥ 853 passed）
python -m pytest tests/ -q

# 代码质量
python -m ruff check src/ tests/
```

## 八、残留风险

| 编号 | 说明 | 等级 | 处置 |
|:--:|------|:--:|------|
| R-CA-1 | B2 的 `target_grain` 过滤假设所有非目标粒度 aggregate 都是 DAG 内部实现——对 Case 06 成立，对更复杂的多输出粒度场景需重新评估 | C | 后续 Phase 扩展 target_grain 为 target_grains（支持多输出粒度） |
| R-CA-2 | `_temp_` 前缀是 DAG 内部管道的充分非必要条件——若未来临时表命名规则变化，过滤规则需同步更新 | B | 测试覆盖 `_temp_` 前缀过滤，改名会触发测试失败 |
| R-CA-3 | Step 4（all_three_join）的 join 在 builder 输出中缺失——这是 builder bug，非 Comparator 问题。当前 Case 06 不影响对齐，但新 case 中可能暴露导致业务语义不完整 | **中高** | **B 类设计修复项**：独立排查 builder 的 join 生成逻辑，建立单独的修复任务——不阻塞本 Phase 合并，但应在下一轮迭代中优先处理 |
