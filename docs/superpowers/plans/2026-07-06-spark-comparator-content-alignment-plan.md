# Spark 双链 Comparator 内容级对齐——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 SQL DAG 扁平化 vs Spark Mapper 之间的 scan/join/aggregate 内容级差异，使 Case 06 `compare_program()` 判定 `LOGIC_EQUIVALENT`

**Architecture:** "三层剥离"——在 `_flatten_sql_program_steps()` 过滤 `_temp_*` scan/join（DAG 内部管道），在 `_normalize_dag_steps()` 做 grain-aware aggregate 合并 + target_grain 过滤，使 SQL 侧扁平化后的 step 列表与 Mapper 从 Contract 生成的 SparkPlan 在业务语义级对齐

**Tech Stack:** Python 3.12+ / Pydantic StrictModel / pytest / 现有 PlanComparator + ContractExtractor + SparkOrchestrator

## Global Constraints

- **不修改 `compare()` 方法（单 plan 路径）**——所有改动仅影响 `compare_program()` 多语句路径
- **不修改 Contract 模型**——不引入 `_temp_` 相关字段
- **不修改 Mapper**——不改变其从 Contract 生成 SparkPlan 的逻辑
- **不修改 PlanComparisonReport 模型**——本 Phase 不新增 `normalization_notes` 字段
- **不修改 SqlProgram IR Schema / Compiler / Executor**
- **compare() 单 plan 路径零退化**
- **Case 06 回归零退化（SQL 管线 7 个测试全绿）**
- **所有代码注释必须使用中文**

---

### Task 1: 增强 diagnostic detail（Step 0）

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_equivalence.py:258-265`

**Interfaces:**
- Consumes: 现有的 `compare_join_steps()` NOT_EQUIVALENT 分支
- Produces: 更可读的 detail 消息——输出两侧归一化后的值

- [ ] **Step 1: 增强 compare_join_steps 的 NOT_EQUIVALENT detail**

当前 `compare_join_steps` 在 join 规格不匹配时只输出 `"Join 规格不一致"`，无法定位具体差异。改为输出两侧归一化后的 join tuple 列表。

在 `src/tianshu_datadev/spark/plan_equivalence.py` 第 258-265 行，将：

```python
    if sql_normalized != spark_normalized:
        return StepEquivalenceResult(
            step_type="join",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail="Join 规格不一致",
        )
```

替换为：

```python
    if sql_normalized != spark_normalized:
        return StepEquivalenceResult(
            step_type="join",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"Join 规格不一致——"
                f"SQL 侧: {sql_normalized}，"
                f"Spark 侧: {spark_normalized}"
            ),
        )
```

- [ ] **Step 2: 增强 compare_aggregate_steps 的 metrics 不匹配 detail**

当前 `compare_aggregate_steps` 第 359-366 行在 metrics 不匹配时只输出 `"聚合指标规格不一致"`。改为输出两侧具体的 metric specs。

在 `src/tianshu_datadev/spark/plan_equivalence.py` 第 359-366 行，将：

```python
    if sql_metric_specs != spark_metric_specs:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail="聚合指标规格不一致",
        )
```

替换为：

```python
    if sql_metric_specs != spark_metric_specs:
        return StepEquivalenceResult(
            step_type="aggregate",
            verdict=EquivalenceVerdict.NOT_EQUIVALENT,
            sql_count=sql_count,
            spark_count=spark_count,
            detail=(
                f"聚合指标规格不一致——"
                f"SQL 侧: {sql_metric_specs}，"
                f"Spark 侧: {spark_metric_specs}"
            ),
        )
```

- [ ] **Step 3: 运行现有测试确认 detail 增强无破坏**

```bash
python -m pytest tests/spark/test_plan_comparator.py -q
```

预期：全部通过（detail 字段变更不影响 verdict 判定逻辑）。

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/spark/plan_equivalence.py
git commit -m "feat: 增强 compare_join_steps / compare_aggregate_steps mismatch detail——输出两侧归一化值"
```

---

### Task 2: 过滤 _temp_* join——DAG 内部管道 join 不参与对比（Step A）

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:437-443`
- Create: `tests/spark/test_plan_comparator.py`（新增 2 个测试）

**Interfaces:**
- Consumes: `_flatten_sql_program_steps()` 产出的 step_dict（`_normalize_step_dict` 已处理）
- Produces: 过滤掉 `left_table_ref` 或 `right_table_ref` 以 `_temp_` 开头的 join step

- [ ] **Step 1: 编写 RED 测试——_temp_ join 被过滤**

在 `tests/spark/test_plan_comparator.py` 的 `TestPlanComparatorMultiStatementFlatten` 类末尾新增：

```python
    def test_temp_join_filtered_from_compare_program(self):
        """_temp_* 表之间的 join 应从扁平化结果中过滤——DAG 内部管道 join。"""
        from tianshu_datadev.planning.sql_build_plan import (
            JoinStep,
            ScanStep,
            JoinType,
        )
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # 构造含 _temp_* join 的 SqlProgram
        stmt_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_t1",
                table_ref="_temp_c0_trip_agg",
                required_columns=[
                    ColumnRef(table_ref="_temp_c0_trip_agg", column_name="borough", normalized_name="borough"),
                ],
            ),
            ScanStep(
                step_type="scan", step_id="scan_t2",
                table_ref="_temp_c0_crash_agg",
                required_columns=[
                    ColumnRef(table_ref="_temp_c0_crash_agg", column_name="borough", normalized_name="borough"),
                ],
            ),
            JoinStep(
                step_type="join", step_id="join_temp",
                right_table_ref="_temp_c0_crash_agg",
                join_type=JoinType.LEFT,
                join_keys=[(
                    ColumnRef(table_ref="_temp_c0_trip_agg", column_name="borough", normalized_name="borough"),
                    ColumnRef(table_ref="_temp_c0_crash_agg", column_name="borough", normalized_name="borough"),
                )],
            ),
        ])

        sql_program = self._make_minimal_sql_program([
            self._make_statement("stmt_0", stmt_plan, kind=StatementKind.PRODUCER),
        ])

        comparator = PlanComparator()
        flattened = comparator._flatten_sql_program_steps(sql_program)

        # _temp_ scan 被过滤 + _temp_ join 被过滤 → 结果为空
        join_steps = [s for s in flattened if s.get("step_type") == "join"]
        scan_steps = [s for s in flattened if s.get("step_type") == "scan"]
        assert len(join_steps) == 0, (
            f"_temp_* join 应被过滤，实际保留 {len(join_steps)} 个"
        )
        assert len(scan_steps) == 0, (
            f"_temp_* scan 应被过滤，实际保留 {len(scan_steps)} 个"
        )
```

- [ ] **Step 2: 运行 RED 测试——验证失败**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestPlanComparatorMultiStatementFlatten::test_temp_join_filtered_from_compare_program -v
```

预期：FAIL——`_temp_*` join 未被过滤，`len(join_steps) == 1`

- [ ] **Step 3: 在 _flatten_sql_program_steps() 中新增 _temp_ join 过滤**

在 `src/tianshu_datadev/spark/plan_comparator.py` 第 437-441 行（现有 `_temp_` scan 过滤块）之后，新增 join 过滤：

```python
                # 过滤 _temp_* scan：DAG 内部管道——Spark 侧通过变量传递 DataFrame，
                # 不存在临时表概念，这些 scan 不应参与对比
                step_type = step_dict.get("step_type", "")
                if step_type == "scan":
                    table_ref = step_dict.get("table_ref", "")
                    if isinstance(table_ref, str) and table_ref.startswith("_temp_"):
                        continue  # 跳过 _temp_ 中间表 scan

                # 过滤 _temp_* join：DAG 内部管道 join——_temp_ 表之间的
                # 关联是 DAG 实现细节，Spark 侧 Mapper 从 Contract 生成的
                # SparkPlan 不包含这些中间表 join
                if step_type == "join":
                    lt = step_dict.get("left_table_ref", "")
                    rt = step_dict.get("right_table_ref", "")
                    if (isinstance(lt, str) and lt.startswith("_temp_")) or \
                       (isinstance(rt, str) and rt.startswith("_temp_")):
                        continue  # 跳过 _temp_ 中间表 join
```

- [ ] **Step 4: 运行测试验证通过**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestPlanComparatorMultiStatementFlatten::test_temp_join_filtered_from_compare_program -v
```

预期：PASS

- [ ] **Step 5: 编写 GREEN 测试——源表 join 保留**

在同一测试类中新增：

```python
    def test_source_join_preserved_in_compare_program(self):
        """源表之间的 join（非 _temp_*）应保留并参与对比。"""
        from tianshu_datadev.planning.sql_build_plan import (
            JoinStep,
            ScanStep,
            JoinType,
        )
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # 构造含源表 join（tz ↔ zts）的 SqlProgram
        stmt_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_tz",
                table_ref="tz",
                required_columns=[
                    ColumnRef(table_ref="tz", column_name="location_id", normalized_name="location_id"),
                ],
            ),
            ScanStep(
                step_type="scan", step_id="scan_zts",
                table_ref="zts",
                required_columns=[
                    ColumnRef(table_ref="zts", column_name="pickup_location_id", normalized_name="pickup_location_id"),
                ],
            ),
            JoinStep(
                step_type="join", step_id="join_tz_zts",
                right_table_ref="zts",
                join_type=JoinType.LEFT,
                join_keys=[(
                    ColumnRef(table_ref="tz", column_name="location_id", normalized_name="location_id"),
                    ColumnRef(table_ref="zts", column_name="pickup_location_id", normalized_name="pickup_location_id"),
                )],
            ),
        ])

        sql_program = self._make_minimal_sql_program([
            self._make_statement("stmt_0", stmt_plan, kind=StatementKind.PRODUCER),
        ])

        comparator = PlanComparator()
        flattened = comparator._flatten_sql_program_steps(sql_program)

        join_steps = [s for s in flattened if s.get("step_type") == "join"]
        assert len(join_steps) == 1, (
            f"源表 join 应保留，实际 {len(join_steps)} 个"
        )
        # 验证保留的 join 是源表 join，不是 _temp_ join
        assert "tz" in join_steps[0].get("left_table_ref", ""), (
            f"保留的 join 应引用源表 tz，实际={join_steps[0]}"
        )
```

- [ ] **Step 6: 运行两个测试确认通过 + 回归**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestPlanComparatorMultiStatementFlatten -v
python -m pytest tests/spark/test_plan_comparator.py -q
```

预期：TestPlanComparatorMultiStatementFlatten 2 个新测试 PASS；全部 20+ 个测试 PASS

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/spark/plan_comparator.py tests/spark/test_plan_comparator.py
git commit -m "feat: _flatten_sql_program_steps 过滤 _temp_* join——DAG 内部管道 join 不参与业务级对比"
```

---

### Task 3: Contract 不抽取 _temp_* scan（Step C）

**Files:**
- Modify: `src/tianshu_datadev/artifacts/contract_extractor.py:409-413`
- Modify: `tests/artifacts/test_contract_extractor.py`（新增 1 个测试）

**Interfaces:**
- Consumes: `extract_v1()` 遍历 SqlProgram 所有 statement 的 ScanStep
- Produces: Contract input_tables 不含 `_temp_` 前缀的表

- [ ] **Step 1: 编写 RED 测试**

在 `tests/artifacts/test_contract_extractor.py` 末尾新增：

```python
    def test_contract_input_tables_excludes_temp(self):
        """Contract V1 的 input_tables 不应包含 _temp_* 中间表——DAG 内部管道。"""
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
            ProjectStep,
        )
        from tianshu_datadev.planning.models import ColumnRef, AliasExpr
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )
        from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor

        # 构造含 _temp_* scan 的 2 语句 SqlProgram
        stmt_0_plan = SqlBuildPlan(
            plan_id="plan_0", spec_hash="test_hash",
            steps=[
                ScanStep(step_type="scan", step_id="scan_fc",
                         table_ref="fc", required_columns=[
                             ColumnRef(table_ref="fc", column_name="borough", normalized_name="borough"),
                         ]),
                ProjectStep(step_type="project", step_id="proj_0", columns=[
                    AliasExpr(expression=ColumnRef(table_ref="", column_name="borough", normalized_name="borough"),
                              alias="borough"),
                ]),
            ],
        )
        stmt_1_plan = SqlBuildPlan(
            plan_id="plan_1", spec_hash="test_hash",
            steps=[
                ScanStep(step_type="scan", step_id="scan_temp",
                         table_ref="_temp_abc123_crash_boro_agg", required_columns=[
                             ColumnRef(table_ref="_temp_abc123_crash_boro_agg",
                                       column_name="borough", normalized_name="borough"),
                         ]),
                ProjectStep(step_type="project", step_id="proj_1", columns=[
                    AliasExpr(expression=ColumnRef(table_ref="", column_name="borough", normalized_name="borough"),
                              alias="borough"),
                ]),
            ],
        )
        sql_program = SqlProgram(
            program_id="test_prog", spec_id="test_spec",
            statements=[
                SqlStatement(statement_id="stmt_0", plan=stmt_0_plan, kind=StatementKind.PRODUCER),
                SqlStatement(statement_id="stmt_1", plan=stmt_1_plan,
                            kind=StatementKind.CONSUMER, depends_on=["stmt_0"]),
            ],
            topological_order=["stmt_0", "stmt_1"],
        )

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        input_refs = {t.table_ref for t in contract.input_tables}
        assert "fc" in input_refs, "源表 fc 应出现在 input_tables"
        assert not any(ref.startswith("_temp_") for ref in input_refs), (
            f"_temp_* 表不应进入 Contract input_tables，实际={input_refs}"
        )
```

- [ ] **Step 2: 运行 RED 测试——验证失败**

```bash
python -m pytest tests/artifacts/test_contract_extractor.py::TestContractExtractor::test_contract_input_tables_excludes_temp -v
```

注意：测试类名需根据实际测试文件中的类名调整。先运行确认 RED。

```bash
python -m pytest tests/artifacts/test_contract_extractor.py -k "test_contract_input_tables_excludes_temp" -v
```

预期：FAIL——`_temp_abc123_crash_boro_agg` 出现在 input_tables 中

- [ ] **Step 3: 在 extract_v1() 中过滤 _temp_* scan**

在 `src/tianshu_datadev/artifacts/contract_extractor.py` 第 410-413 行，将：

```python
                if isinstance(step, ScanStep):
                    self._extract_scan(
                        step, input_tables, input_columns, seen_tables, seen_columns,
                    )
```

替换为：

```python
                if isinstance(step, ScanStep):
                    # _temp_* 表是 DAG 内部管道——不进入 Contract
                    if step.table_ref.startswith("_temp_"):
                        continue
                    self._extract_scan(
                        step, input_tables, input_columns, seen_tables, seen_columns,
                    )
```

- [ ] **Step 4: 运行测试验证通过 + 全量回归**

```bash
python -m pytest tests/artifacts/test_contract_extractor.py -k "test_contract_input_tables_excludes_temp" -v
python -m pytest tests/artifacts/test_contract_extractor.py -q
```

预期：新测试 PASS；全量回归 PASS

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/artifacts/contract_extractor.py tests/artifacts/test_contract_extractor.py
git commit -m "feat: ContractExtractor.extract_v1 过滤 _temp_* scan——DAG 内部管道不进入 Contract"
```

---

### Task 4: aggregate 按 grain 分组合并（Step B1）

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:447-521`（`_normalize_dag_steps` 方法）
- Modify: `tests/spark/test_plan_comparator.py:1736-1751`（更新 `test_merges_multiple_aggregates`）

**Interfaces:**
- Consumes: `_flatten_sql_program_steps()` 产出的扁平化步骤列表
- Produces: 按 group_keys 签名分组合并后的 aggregate 列表——同粒度合并，不同粒度独立

**注意**：此任务会改变 `test_merges_multiple_aggregates` 的行为——原来 `[borough]` 和 `[violation_county]` 两个不同粒度的 aggregate 被合并为 1 个，B1 后应保持 2 个独立。

- [ ] **Step 1: 更新现有测试——反映新的 grain-aware 行为**

将 `tests/spark/test_plan_comparator.py` 第 1736-1751 行的测试从"无条件合并"改为"同粒度合并、不同粒度独立"：

```python
    def test_merges_multiple_aggregates(self):
        """同粒度 aggregate 合并，不同粒度 aggregate 保持独立。"""
        steps = [
            {"step_type": "scan", "table_ref": "fc"},
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "SUM", "alias": "total_injured"}]},
            {"step_type": "aggregate", "group_keys": ["violation_county"],
             "metrics": [{"function": "SUM", "alias": "total_violations"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        # B1：同粒度 [borough] 合并为 1 个（2 metrics），[violation_county] 独立 1 个
        assert len(agg_steps) == 2, f"预期 2 个 aggregate（不同粒度独立），实际 {len(agg_steps)}"

        # 收集所有 group_keys 集合
        all_groups = [tuple(sorted(s["group_keys"])) for s in agg_steps]
        assert ("borough",) in all_groups, "应保留 [borough] 粒度的 aggregate"
        assert ("violation_county",) in all_groups, "应保留 [violation_county] 粒度的 aggregate"

        # [borough] aggregate 应有 2 个 metrics（合并自两个同粒度 aggregate）
        borough_agg = [s for s in agg_steps if s["group_keys"] == ["borough"]][0]
        assert len(borough_agg["metrics"]) == 2, (
            f"[borough] aggregate 应有 2 个 metrics，实际 {len(borough_agg['metrics'])}"
        )

        # [violation_county] aggregate 应有 1 个 metric
        vc_agg = [s for s in agg_steps if s["group_keys"] == ["violation_county"]][0]
        assert len(vc_agg["metrics"]) == 1
```

- [ ] **Step 2: 运行测试确认 RED**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestNormalizeDagSteps::test_merges_multiple_aggregates -v
```

预期：FAIL——`len(agg_steps) == 1`（旧行为），期望 2

- [ ] **Step 3: 添加新测试——同粒度合并 + 不同粒度独立**

在 `TestNormalizeDagSteps` 类中新增：

```python
    def test_aggregate_same_grain_merged(self):
        """多个同 [borough] aggregate → 合并为 1 个，metrics 去重合并。"""
        steps = [
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "input_column": "crash_id", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "SUM", "input_column": "persons_injured", "alias": "total_injured"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        assert len(agg_steps) == 1
        assert agg_steps[0]["group_keys"] == ["borough"]
        assert len(agg_steps[0]["metrics"]) == 2
        aliases = {m["alias"] for m in agg_steps[0]["metrics"]}
        assert aliases == {"total_crashes", "total_injured"}

    def test_aggregate_different_grain_kept_separate(self):
        """[borough] 和 [violation_county] 不同粒度 → 各自独立，不合并。"""
        steps = [
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["violation_county"],
             "metrics": [{"function": "SUM", "alias": "total_violations"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        assert len(agg_steps) == 2, (
            f"不同粒度应保持独立，实际合并为 {len(agg_steps)} 个"
        )
        gk_sets = {tuple(sorted(s["group_keys"])) for s in agg_steps}
        assert gk_sets == {("borough",), ("violation_county",)}
```

- [ ] **Step 4: 重写 _normalize_dag_steps 的 aggregate 合并逻辑**

将 `src/tianshu_datadev/spark/plan_comparator.py` 第 447-521 行的整个 `_normalize_dag_steps` 方法替换为重写版本。

同时更新 `compare_program` 中对 `_normalize_dag_steps` 的调用（第 311 行），先不传 `target_grain`（Task 5 再加）。

完整新方法：

```python
    @staticmethod
    def _normalize_dag_steps(
        sql_steps: list[dict[str, Any]],
        target_grain: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """将 DAG 扁平化产生的多个同类型 step 合并为单一步骤。

        合并规则：
        1. aggregate：按 group_keys 签名分组合并——同粒度合并，不同粒度独立
        2. project：合并所有 columns（去重按 alias）
        3. 其他类型（scan/filter/join/case_when/sort/limit）：保持原样
        4. 若提供 target_grain，只保留 group_keys 签名匹配的 aggregate 组

        此归一化使 SQL DAG 的多语句结构与 Mapper 从平铺 Contract
        生成的单 aggregate/单 project 结构对齐。

        Args:
            sql_steps: _flatten_sql_program_steps() 产出的扁平化步骤
            target_grain: 可选——目标粒度（如 ["borough"]），
                          非空时只保留匹配的 aggregate 组

        Returns:
            归一化后的步骤列表
        """
        result: list[dict[str, Any]] = []
        proj_columns: list[dict[str, Any]] = []
        seen_proj_aliases: set[str] = set()
        has_project = False

        # aggregate 按 group_keys 签名分组合并——禁止跨粒度硬合并
        agg_groups: dict[tuple, dict] = {}
        for step in sql_steps:
            stype = step.get("step_type", "")
            if stype == "aggregate":
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
            elif stype == "project":
                has_project = True
                for col in step.get("columns", []):
                    alias = col.get("alias", "")
                    if alias not in seen_proj_aliases:
                        seen_proj_aliases.add(alias)
                        proj_columns.append(col)
            else:
                result.append(step)

        # B2：target_grain 过滤——只保留最终业务粒度 aggregate
        if target_grain is not None:
            target_set = set(target_grain)
            agg_groups = {
                gk: data for gk, data in agg_groups.items()
                if set(gk) == target_set
            }

        # 将分组后的 aggregate 插入 result（放在 scan/filter/join/read 之后）
        for gk_tuple, agg_data in agg_groups.items():
            merged_agg = {
                "step_type": "aggregate",
                "group_keys": agg_data["group_keys"],
                "metrics": agg_data["metrics"],
            }
            # 找到合适的插入位置
            insert_pos = 0
            for i, s in enumerate(result):
                if s.get("step_type") in ("scan", "filter", "join", "read"):
                    insert_pos = i + 1
            result.insert(insert_pos, merged_agg)

        # 将合并后的 project 追加到末尾
        if has_project:
            result.append({
                "step_type": "project",
                "columns": proj_columns,
            })

        return result
```

- [ ] **Step 5: 运行测试验证通过 + 回归**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestNormalizeDagSteps -v
python -m pytest tests/spark/test_plan_comparator.py -q
```

预期：TestNormalizeDagSteps 4 个测试 PASS；全部测试 PASS（零退化）

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/spark/plan_comparator.py tests/spark/test_plan_comparator.py
git commit -m "feat: _normalize_dag_steps grain-aware aggregate 合并——同粒度合并，不同粒度独立 + target_grain 参数骨架"
```

---

### Task 5: target_grain 过滤 + Orchestrator 透传（Step B2）

**Files:**
- Modify: `src/tianshu_datadev/spark/plan_comparator.py:276-311`（`compare_program` 签名 + 调用点）
- Modify: `src/tianshu_datadev/spark/orchestrator.py:256-258, 431-447`（缓存 contract + 透传 target_grain）
- Create: `tests/spark/test_plan_comparator.py`（新增 2 个 B2 测试）

**Interfaces:**
- Consumes: `compare_program()` 新增 `target_grain` 参数；`Orchestrator.run()` 持有的 Contract.grouping_keys
- Produces: `target_grain=None` 向后兼容；传入时过滤非目标粒度 aggregate

- [ ] **Step 1: 编写 RED 测试——target_grain 过滤**

在 `tests/spark/test_plan_comparator.py` 的 `TestNormalizeDagSteps` 类中新增：

```python
    def test_target_grain_filters_irrelevant_aggregate(self):
        """target_grain=["borough"] 时，[violation_county] aggregate 应被过滤。"""
        steps = [
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["violation_county"],
             "metrics": [{"function": "SUM", "alias": "total_violations"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps, target_grain=["borough"])
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        assert len(agg_steps) == 1, (
            f"target_grain 过滤后应仅剩 1 个 aggregate，实际 {len(agg_steps)}"
        )
        assert agg_steps[0]["group_keys"] == ["borough"]
        assert len(agg_steps[0]["metrics"]) == 1

    def test_target_grain_none_preserves_all(self):
        """target_grain=None 时保留所有 aggregate 组——向后兼容。"""
        steps = [
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["violation_county"],
             "metrics": [{"function": "SUM", "alias": "total_violations"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps, target_grain=None)
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        assert len(agg_steps) == 2, (
            f"target_grain=None 应保留所有 aggregate，实际 {len(agg_steps)}"
        )
```

- [ ] **Step 2: 运行测试——B2 功能已在 Task 4 中实现，应直接 PASS**

```bash
python -m pytest tests/spark/test_plan_comparator.py::TestNormalizeDagSteps::test_target_grain_filters_irrelevant_aggregate tests/spark/test_plan_comparator.py::TestNormalizeDagSteps::test_target_grain_none_preserves_all -v
```

预期：PASS（B2 逻辑已在 Task 4 的 `_normalize_dag_steps` 重写中实现）

- [ ] **Step 3: 在 compare_program() 中新增 target_grain 参数并透传**

在 `src/tianshu_datadev/spark/plan_comparator.py` 第 276-283 行，修改 `compare_program` 签名：

```python
    def compare_program(
        self,
        sql_program: SqlProgram,
        spark_plan: SparkPlan,
        annotations: list | None = None,     # noqa: ARG002 保留接口，Phase 8 消费
        warnings: list[AnnotationWarning] | None = None,
        enabled_step_types: set[str] | None = None,
        target_grain: list[str] | None = None,  # 新增：目标粒度——用于过滤 DAG 中间粒度 aggregate
    ) -> PlanComparisonReport:
```

在同一方法内第 311 行，将 `_normalize_dag_steps` 调用改为透传 `target_grain`：

```python
        sql_steps_data = self._normalize_dag_steps(sql_steps_data, target_grain=target_grain)
```

- [ ] **Step 4: 在 Orchestrator 中缓存 contract 并透传 target_grain**

**4a. 缓存 contract**：在 `src/tianshu_datadev/spark/orchestrator.py` 第 223-225 行，新增 `_cached_contract`：

```python
        self._cached_plan = None           # SparkPlan | None
        self._cached_sql_plan = None       # SqlBuildPlan | None
        self._cached_compile_result = None  # SparkCompileResult | None
        self._cached_contract = None        # DataTransformContractV1 | None（新增）
```

在第 258 行后新增 contract 缓存：

```python
        self._cached_sql_plan = sql_plan
        self._cached_contract = contract   # 缓存 contract——供 COMPARATOR 阶段提取 target_grain
```

**4b. 在 _run_comparator 中提取 target_grain 并透传**：在 `src/tianshu_datadev/spark/orchestrator.py` 第 444-447 行，修改：

```python
                if isinstance(self._cached_sql_plan, SqlProgram):
                    # 从 Contract 提取 target_grain——用于过滤 DAG 中间粒度 aggregate
                    target_grain = None
                    if self._cached_contract is not None and hasattr(
                        self._cached_contract, "grouping_keys"
                    ):
                        target_grain = (
                            self._cached_contract.grouping_keys
                            if self._cached_contract.grouping_keys
                            else None
                        )
                    report = comparator.compare_program(
                        self._cached_sql_plan, self._cached_plan,
                        target_grain=target_grain,
                    )
```

- [ ] **Step 5: 运行回归确认无退化**

```bash
python -m pytest tests/spark/test_plan_comparator.py -q
python -m pytest tests/spark/test_orchestrator.py -q
```

预期：全部 PASS

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/spark/plan_comparator.py src/tianshu_datadev/spark/orchestrator.py tests/spark/test_plan_comparator.py
git commit -m "feat: compare_program + Orchestrator 透传 target_grain——Contract grouping_keys 驱动 DAG aggregate 粒度过滤"
```

---

### Task 6: Case 06 xfail 转正（Step D）

**Files:**
- Modify: `tests/api/test_nyc_business_case.py:1203-1243`

**Interfaces:**
- Consumes: Task 2 + 3 + 4 + 5 的全部改动
- Produces: `test_spark_orchestrator_logic_equivalence` 从 xfail 转为 PASS

- [ ] **Step 1: 移除 xfail 装饰器**

在 `tests/api/test_nyc_business_case.py` 第 1203-1211 行，移除 `@pytest.mark.xfail` 装饰器及其参数，保留测试方法体不变。

将：

```python
    @pytest.mark.xfail(
        reason="已知限制：B 类收口已完成（比率计算/CASE WHEN/Comparator 归一化），"
               "DAG 归一化（_normalize_dag_steps）有效——aggregate 3→1、project 7→1，"
               "步数差异已消除。但 scan/join/aggregate 的内容级差异"
               "（_temp_* 表引用 vs Mapper 别名）导致仍为 LOGIC_MISMATCH。"
               "需独立 Phase 引入 plan 级别内容对齐（scan 别名重映射、join 引用归一化）。"
               "一旦内容级对齐完成，此 xfail 应自然转正。",
        strict=True,
    )
    def test_spark_orchestrator_logic_equivalence(self, nyc06_spec_md, nyc06_csv_paths):
```

改为：

```python
    def test_spark_orchestrator_logic_equivalence(self, nyc06_spec_md, nyc06_csv_paths):
        """多语句 DAG Spark Orchestrator 逻辑等价判定——显式断言 comparator_report.status。

        内容级对齐（Phase 10.x）完成后，三层剥离（_temp_ scan/join 过滤 +
        grain-aware aggregate 合并 + target_grain 过滤）使 SQL DAG 扁平化结果
        与 Mapper SparkPlan 在业务语义级对齐，Comparator 应判 LOGIC_EQUIVALENT。
        """
```

- [ ] **Step 2: 运行 Case 06 Spark 双链测试——开启新纪元**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SparkDualChain::test_spark_orchestrator_logic_equivalence -v
```

预期：PASS——`ComparisonStatus.LOGIC_EQUIVALENT`

如果仍 FAIL，检查 comparator_report.status 的实际值，根据 diagnostic detail（Task 1 增强）定位残留差异。

- [ ] **Step 3: 运行 Case 06 SQL 管线回归——确保零退化**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SqlPipeline -v
```

预期：7 个测试全部 PASS

- [ ] **Step 4: 全量回归**

```bash
python -m pytest tests/spark/ tests/artifacts/ tests/api/ -q
python -m pytest tests/ -q
python -m ruff check src/ tests/
```

预期：≥ 853 passed，ruff clean

- [ ] **Step 5: Commit**

```bash
git add tests/api/test_nyc_business_case.py
git commit -m "feat: Case 06 Spark 双链 Comparator LOGIC_EQUIVALENT——内容级对齐完成，xfail 转正"
```

---

## 执行顺序

```
Task 1 (diagnostic detail) → Task 2 (_temp_ join) → Task 3 (Contract _temp_ scan)
    → Task 4 (grain-aware aggregate) → Task 5 (target_grain + Orchestrator)
    → Task 6 (xfail 转正)
```

每个 Task 独立可验证、独立可 commit。Task 2-5 有依赖关系（后一个依赖前一个的产出），但不影响独立测试验证。

## 验收命令

```bash
# 1. 核心变更文件测试
python -m pytest tests/spark/test_plan_comparator.py tests/artifacts/test_contract_extractor.py -q

# 2. Case 06 集成测试——关键验收
python -m pytest tests/api/test_nyc_business_case.py -q

# 3. 全量回归
python -m pytest tests/ -q

# 4. 代码质量
python -m ruff check src/ tests/
```

**最终预期**：
- `test_spark_orchestrator_logic_equivalence` → **PASS**（`LOGIC_EQUIVALENT`）
- 全量 ≥ 853 passed / 11 skipped / 0 xfailed / 0 xpassed
- Ruff clean
