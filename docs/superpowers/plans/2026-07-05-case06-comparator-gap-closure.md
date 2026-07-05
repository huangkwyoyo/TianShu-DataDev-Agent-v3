# Case06 SqlProgram 多语句 DAG Spark Comparator 缺口收口——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐 Case06（SqlProgram 7 步 DAG）的 Spark Comparator 双链验证，建立 `TestNYCCase06SparkDualChain` 测试类，明确 xfail 转正与保留策略。

**Architecture:** 核心改造在 `PlanComparator`——新增 `compare_program()` 方法支持将 SqlProgram 的所有非 _temp_ step 扁平化为单一步骤列表后与 SparkPlan 对比。`Orchestrator` 和 `Pipeline.export_artifacts()` 做最小扩展以透传 SqlProgram。不改动 Mapper、Compiler、Validator、Comparator 核心判定规则。

**Tech Stack:** Python 3.12, Pydantic (StrictModel), PySpark 4.1.2, DuckDB

## Global Constraints

- 禁止引入 CTE（`WITH ... AS`）
- 禁止 raw SQL 逃生口
- 禁止绕过 SqlProgram / `_temp_*` 临时表机制
- 禁止削弱 SQL 安全校验、Validator、Comparator 状态语义
- 禁止修改真实 LLM / R8 逻辑
- 禁止修改 `DataTransformContract` schema（V1 / Lite 模型零改动）
- 禁止删除 `contract_to_sql_steps()`（保持 deprecated）
- 禁止修改 `plan_equivalence.py` 中 8 条对比规则的核心判定逻辑
- 所有代码注释使用中文
- Case06 的 `compute_ratios`（比率计算）和 `risk_label`（CASE WHEN）属于 B 类遗留——本轮**不实现**，仅做 Comparator 框架就绪

---

## 背景分析

### 现状

| 组件 | 当前能力 | Case06 涉及 |
|------|----------|:----------:|
| SqlProgram | 7 语句 DAG，`_temp_*` 串联，拓扑排序，DAG 校验 | ✅ |
| `extract_v1()` | SqlProgram → DataTransformContractV1（含 step_dag/temp_tables） | ✅ |
| Mapper | ContractV1 → SparkPlan（仅消费 case_when_labels/window_specs，不消费 step_dag/temp_tables） | ✅ |
| `PlanComparator.compare()` | 接受单一 `SqlBuildPlan` + `SparkPlan` | ❌ |
| `Orchestrator._run_comparator()` | 传递单一 `_cached_sql_plan` | ❌ |
| `Pipeline.export_artifacts()` | 仅导出 `plans[-1]`（最后一步的 plan） | ❌ |
| NYC 测试 | Case 01-05 有 `TestNYCCaseXXSparkDualChain`，Case06 无 | ❌ |

### 根因

`PlanComparator.compare(sql_plan: SqlBuildPlan, spark_plan: SparkPlan)` 的契约是"单 Plan 对单 Plan"，但 SqlProgram 多语句 DAG 是 **N 个 SqlBuildPlan → 1 个 SparkPlan**（Mapper 消费聚合后的 V1 Contract 产出单个 SparkPlan）。

当前 Comparator 拿到的是 FINAL statement 的 SqlBuildPlan（risk_label，step 7），仅覆盖最后一步，前 6 步的聚合/Join/分组逻辑完全被跳过。

### 关键设计约束

- **_temp 表扫描不属于业务语义**：SQL 侧多语句通过 `_temp_*` 表传递数据，这些 scan 是执行引擎的内部管道——Spark 侧不需要它们（PySpark 在单个脚本中通过变量传递 DataFrame）。
- **V1 Contract 是双方共享的"真值源"**：`extract_v1(sql_program)` 聚合了所有 7 个 statement 的语义信息（输入表、列、Join、聚合、分组、输出列、sort/limit）。Mapper 消费此 Contract 产出 SparkPlan。
- **扁平化对比是正确路径**：将 SqlProgram 中所有 statement 的 SqlBuildPlan step 全部提取、过滤 `_temp_` scan、合并为一个 step 列表，然后与 SparkPlan step 列表对比。

---

## 修改范围

| 文件 | 操作 | 改动量 |
|------|:----:|:------:|
| `src/tianshu_datadev/spark/plan_comparator.py` | 修改 | +40 行——新增 `compare_program()` + `_flatten_sql_program_steps()` |
| `src/tianshu_datadev/spark/orchestrator.py` | 修改 | ~5 行——`_cached_sql_plan` 类型扩展为 `SqlBuildPlan \| SqlProgram` |
| `src/tianshu_datadev/api/pipeline.py` | 修改 | ~5 行——`export_artifacts()` 新增 `sql_program` 字段 |
| `tests/spark/test_plan_comparator.py` | 修改 | +80 行——多语句扁平化测试 |
| `tests/spark/test_orchestrator.py` | 修改 | +30 行——Comparator 接受 SqlProgram 测试 |
| `tests/api/test_nyc_business_case.py` | 修改 | +80 行——新增 `TestNYCCase06SparkDualChain` 类 + xfail 状态调整 |

---

## 任务分解

### Task 1: PlanComparator 新增 `compare_program()`——多语句扁平化对比

**目标**：使 PlanComparator 能处理 SqlProgram（多语句 DAG）与 SparkPlan 的对比。

**设计说明**：
- `compare(sql_plan, spark_plan)` 保持不变——单 Plan 对比
- 新增 `compare_program(sql_program, spark_plan)` —— 多语句对比入口
- `_flatten_sql_program_steps(sql_program)` 将所有 SqlStatement 中的 SqlBuildPlan step 全量提取，过滤以下 step：
  1. `scan` step 中 `table_ref` 以 `_temp_` 开头的（DAG 内部管道——Spark 侧无对应）
  2. 其余 `scan`（CSV/源表）保留——双侧应一致
  3. `filter`/`project`/`aggregate`/`join`/`sort`/`limit`/`case_when`/`window`——全保留
- 扁平化后的步骤列表传给现有 `compare_plans()` 核心引擎——**零改动**

**修改文件**：`src/tianshu_datadev/spark/plan_comparator.py`

**Interface 设计**：

```python
# PlanComparator 新增方法
def compare_program(
    self,
    sql_program: SqlProgram,
    spark_plan: SparkPlan,
    annotations: list | None = None,
    warnings: list[AnnotationWarning] | None = None,
    enabled_step_types: set[str] | None = None,
) -> PlanComparisonReport:
    """多语句 SqlProgram ↔ SparkPlan 逻辑对比入口。

    将所有 SqlStatement 的 SqlBuildPlan steps 扁平化为单一步骤列表，
    过滤 _temp_ 表 scan（内部管道——Spark 侧无对应），然后委托给核心
    compare_plans() 引擎执行等价对比。

    这是 Case06 等 ComputeSteps 路径的唯一对比入口。
    """

@staticmethod
def _flatten_sql_program_steps(
    sql_program: SqlProgram,
) -> list[dict[str, Any]]:
    """从 SqlProgram 扁平化所有 step 数据。

    规则：
    1. 遍历所有 SqlStatement.plan.steps
    2. 跳过 scan step 中 table_ref 以 _temp_ 开头的（DAG 内部管道）
    3. 保留所有源表 scan 和所有语义 step
    4. 不做归一化——归一化在 compare() 路径中已有
    """
```

**验收标准**：
- 对 Case06 7 步 DAG 的 SqlProgram + 对应 SparkPlan 调用 `compare_program()` 后：
  - 状态不是 `NOT_COVERED`（所有 8 种 step 类型均在 enabled 范围内）
  - 状态不是 `LOGIC_MISMATCH`（SQL 和 Spark 侧的语义一致）
  - `_temp_*` scan 从 step_results 中排除
  - 对比覆盖：源表 scan（5 个 CSV）× filter × aggregate（3 步）× join（3 步 LEFT）× project

---

### Task 2: Orchestrator 扩展——接受 SqlProgram 作为 Comparator 输入

**目标**：使 `Orchestrator.run()` 的 `sql_plan` 参数接受 `SqlBuildPlan | SqlProgram`。

**修改文件**：
- `src/tianshu_datadev/spark/orchestrator.py`：`run()` 签名 + `_run_comparator()` 分发逻辑

**改动要点**：
- `run(sql_plan: SqlBuildPlan | SqlProgram | None = None)`——扩展类型
- `_cached_sql_plan` 类型同步扩展
- `_run_comparator()` 内部分发：
  - `isinstance(cached, SqlProgram)` → `comparator.compare_program(cached, spark_plan)`
  - `isinstance(cached, SqlBuildPlan)` → `comparator.compare(cached, spark_plan)`（保持不变）

**验收标准**：
- Orchestrator 能接收 SqlProgram 并调用 `compare_program()`
- 接收 SqlBuildPlan 时行为不变（向后兼容）
- 31 个已有 Orchestrator 测试零退化

---

### Task 3: Pipeline.export_artifacts() 暴露 SqlProgram

**目标**：使 NYC 测试能通过 `export_artifacts()` 获取 SqlProgram（当前仅导出 `sql_build_plan = plans[-1]`）。

**修改文件**：`src/tianshu_datadev/api/pipeline.py`

**改动要点**：
- `PipelineArtifactBundle` 新增 `sql_program: SqlProgram | None = None`
- `export_artifacts()` 在 ComputeSteps 路径下从缓存填充 `sql_program`
- 非 ComputeSteps 路径保持 `None`

**验收标准**：
- Case06 `run_all()` → `export_artifacts()` 后 `bundle.sql_program` 非空
- `bundle.sql_program.statements` 含 7 个 statement
- 已有测试零退化

---

### Task 4: 新增 Comparator 多语句扁平化单元测试

**目标**：在 `test_plan_comparator.py` 中新增多语句 SqlProgram → 扁平化 step 的对比测试。

**修改文件**：`tests/spark/test_plan_comparator.py`

**测试用例**：

1. **`test_flatten_sql_program_excludes_temp_table_scans`**：
   - 构造一个 3 语句 SqlProgram（2 个 PRODUCER + 1 个 FINAL），含 2 个 `_temp_*` scan
   - 调用 `_flatten_sql_program_steps()` → 确认 `_temp_*` scan 被排除
   - 确认源表 scan 和语义 step 保留

2. **`test_flatten_sql_program_preserves_all_semantic_steps`**：
   - 构造含 filter/aggregate/join/project 的 SqlProgram
   - 确认所有语义 step 类型和数量在扁平化后完整保留

3. **`test_compare_program_vs_spark_plan_equivalent`**：
   - 构造最小 2 语句 SqlProgram（scan + filter → aggregate → project）
   - 构造等价 SparkPlan
   - 调用 `compare_program()` → LOGIC_EQUIVALENT

4. **`test_compare_program_vs_spark_plan_mismatch`**：
   - 同上但修改 SparkPlan（移除一个 filter）
   - 调用 `compare_program()` → LOGIC_MISMATCH

---

### Task 5: 新增 Orchestrator + SqlProgram 集成测试

**目标**：验证 Orchestrator 完整链路（Mapper + Compiler + Validator + Comparator）接受 SqlProgram。

**修改文件**：`tests/spark/test_orchestrator.py`

**测试用例**：

1. **`test_comparator_with_sql_program_uses_compare_program`**：
   - 构造最小 SqlProgram（2 语句）+ 对应 SparkPlan（通过真实 Mapper 产出）
   - 注入 `stage_failures` 跳过除 COMPARATOR 外的所有阶段
   - 断言 `state.comparator_report is not None`
   - 断言 `state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT`

2. **`test_comparator_with_sql_build_plan_still_works`**（向后兼容）：
   - 用已有的单 SqlBuildPlan + SparkPlan 调用 Orchestrator
   - 确认行为与改造前一致（COMPARATOR SUCCESS + LOGIC_EQUIVALENT）

---

### Task 6: New——新增 `TestNYCCase06SparkDualChain` 测试类

**目标**：在 `test_nyc_business_case.py` 中补齐 Case06 的 Spark 双链验证测试。

**修改文件**：`tests/api/test_nyc_business_case.py`

**测试用例**：

#### 6a. 主测试——双链逻辑等价（xfail，严格模式）

```python
class TestNYCCase06SparkDualChain:
    """NYC 案例 06——多语句 DAG 跨域融合 Spark 双管线逻辑验证。"""

    @pytest.mark.xfail(
        reason="已知限制：compute_ratios（比率计算）和 risk_label（CASE WHEN）未实现，"
               "导致 Comparator 在 project 步骤上列数不一致——SQL 侧 FINAL 语句缺少 "
               "crash_per_million_trips/violation_per_thousand_trips/safety_risk_level 三列。"
               "待 B 类遗留收口后转正。",
        strict=True,  # strict=True：xfail 的原因是真实缺陷，不是环境问题
    )
    def test_spark_orchestrator_logic_equivalence(self, nyc06_spec_md, nyc06_csv_paths):
        """多语句 DAG Spark Orchestrator 逻辑等价判定——显式断言 comparator_report.status。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        # 获取 SqlProgram 和 V1 Contract（ComputeSteps 路径直接产 V1）
        sql_program = bundle.sql_program
        contract_v1 = bundle.data_transform_contract

        assert sql_program is not None, "SqlProgram 不应为空"
        assert contract_v1 is not None, "Contract 不应为空"

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract_v1, sql_plan=sql_program,
        )

        # ① comparator_report 必须非空
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )
        # ② 严格断言——应为 LOGIC_EQUIVALENT（当前 xfail）
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"Case 06 逻辑对比应判定为等价，"
            f"实际 status={state.comparator_report.status}"
        )
        # ③ overall_status 一致性
        assert state.overall_status.value in {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
        }
```

**断言策略**：
- `strict=True`（不是 `strict=False`）——表示此 xfail 的原因是**真实功能缺失**（比率计算/CASE WHEN），一旦功能实现，测试应自然通过。若测试意外通过（XPASS），说明功能已就绪但 xfail 标记未更新——这是正确的告警信号。
- `comparator_report.status` 严格断言 `LOGIC_EQUIVALENT`（与 Case 01-04 一致，非 Case 05 的宽松断言）

#### 6b. 辅助测试——Comparator 非空 + 状态非 MISMATCH（防御性，不 xfail）

```python
    def test_spark_comparator_report_not_mismatch(self, nyc06_spec_md, nyc06_csv_paths):
        """Comparator 不应报告 LOGIC_MISMATCH——未覆盖部分应标记 NOT_COVERED 而非 MISMATCH。

        此测试是防御性的——即使比率计算/CASE WHEN 导致列数不一致，
        Comparator 也不应误报为逻辑矛盾。当前步骤 1-5 的语义对比应等价。
        """
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=bundle.data_transform_contract, sql_plan=bundle.sql_program,
        )

        assert state.comparator_report is not None
        # 关键防御：不得为 LOGIC_MISMATCH
        assert state.comparator_report.status != ComparisonStatus.LOGIC_MISMATCH, (
            f"Comparator 不应报告逻辑矛盾，"
            f"实际 status={state.comparator_report.status}，"
            f"step_results={[(r.step_type, r.verdict.value) for r in state.comparator_report.step_results]}"
        )
        # 接受 NOT_COVERED（因 project 列数差异）或 LOGIC_EQUIVALENT
        assert state.comparator_report.status in {
            ComparisonStatus.LOGIC_EQUIVALENT,
            ComparisonStatus.NOT_COVERED,
        }, f"意外状态: {state.comparator_report.status}"
```

#### 6c. 辅助测试——Contract V1 导出验证

```python
    def test_contract_v1_is_extracted_from_run_all(self, nyc06_spec_md, nyc06_csv_paths):
        """Case06 run_all() 应直接产出 DataTransformContractV1（非 Lite）。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        assert bundle.data_transform_contract is not None
        contract = bundle.data_transform_contract
        # ComputeSteps 路径应直接产 V1
        from tianshu_datadev.artifacts.models import DataTransformContractV1
        assert isinstance(contract, DataTransformContractV1), (
            f"应为 DataTransformContractV1，实际={type(contract).__name__}"
        )
```

---

### Task 7: xfail 状态审计——现有 3 个 Case06 xfail 的转正/保留判定

**文件**：`tests/api/test_nyc_business_case.py`

#### 7a. `test_run_all_produces_borough_results`（line 1093）

- **当前标记**：`xfail(strict=False, reason="比率计算+CASE WHEN 未实现")`
- **判定**：**转正** → 移除 xfail
  - 理由：步骤 1-5（crash_boro_agg → all_three_join）已通过 `run_all()` 执行，输出应包含 5 个 borough 的基本指标（total_trip_count/total_crashes/total_injured/total_killed/total_violations/avg_daily_fine）。比率列和风险等级列可能为 NULL 或缺失，但核心聚合逻辑已验证通过——此 xfail 的 `strict=False` 本意是"可能通过也可能不通过"，当前应已稳定通过。
- **改动**：移除 `@pytest.mark.xfail` 装饰器，保留断言逻辑

#### 7b. `test_safety_risk_level_values_valid`（line 1130）

- **当前标记**：`xfail(strict=False, reason="CASE WHEN 未实现")`
- **判定**：**保留 xfail，升级为 `strict=True`**
  - 理由：`safety_risk_level` 列因 CASE WHEN 未实现，值不可预测（可能为 NULL 或硬编码占位）。这是真实的功能缺失，一旦 CASE WHEN 实现，测试应自然通过。
- **改动**：将 `strict=False` 改为 `strict=True`

#### 7c. `test_temp_tables_cleaned_after_execution`（line 1174）

- **当前标记**：`xfail(strict=False, reason="无法从外部连接检查内部临时表")`
- **判定**：**保留 xfail，`strict=False` 不变**
  - 理由：这是测试基础设施问题，不是功能缺陷。DuckDBExecutor 的 `finally` 块保证 `_temp_` 表的 DROP 清理，但外部新建 `:memory:` 连接无法检查。需要 Pipeline 暴露 `cleanup_status` 后才能直接断言。这是 C 类改进——优先级低。
- **改动**：无（保持不变）

---

### Task 8: 文档更新

**文件**：`docs/risks/phase-6-8-known-risks.md`

- `Case06-Comparator` 风险状态从 🔴 更新为 🟡（框架就绪，因 B 类遗留 block 严格断言）
- 新增 `Case06-Comparator` 子条目：记录 xfail 转正情况和残留风险

**文件**：`docs/current-state-and-verification-status.md`

- Phase 进度矩阵 Case 06 状态更新
- 测试基线更新

---

## xfail 转正/保留汇总

| 测试 | 当前 | 新状态 | 理由 |
|------|:----:|:------:|------|
| `test_run_all_produces_borough_results` | xfail (strict=False) | **转正** ✅ | 步骤 1-5 已稳定通过，比率/CASE WHEN 不影响基本聚合 |
| `test_safety_risk_level_values_valid` | xfail (strict=False) | xfail (strict=True) 🟡 | CASE WHEN 未实现——是真实功能缺失 |
| `test_temp_tables_cleaned_after_execution` | xfail (strict=False) | xfail (strict=False) 🟡 | 测试基础设施限制，非功能缺陷 |
| `test_spark_orchestrator_logic_equivalence` (新增) | — | xfail (strict=True) 🟡 | 比率/CASE WHEN B 类遗留——待收口后转正 |
| `test_spark_comparator_report_not_mismatch` (新增) | — | **通过** ✅ | 防御性测试——不可 xfail |
| `test_contract_v1_is_extracted_from_run_all` (新增) | — | **通过** ✅ | Contract V1 提取已就绪 |

---

## 测试策略

### 验收命令

```bash
# 1. Case06 SQL 管线（确保改造不破坏已有路径）
python -m pytest tests/api/test_nyc_business_case.py -q

# 2. Comparator + Orchestrator 全量
python -m pytest tests/spark/test_plan_comparator.py tests/spark/test_orchestrator.py -q

# 3. 全量后端回归
python -m pytest tests/api/ tests/spark/ -q

# 4. 代码质量
python -m ruff check src/ tests/

# 5. Git 干净
git diff --check
```

### 预期测试基线变化

| 指标 | 改造前 | 改造后 |
|------|:-----:|:-----:|
| passed | 839 | ~850（+10~12 新测试 + 1 xfail 转正） |
| skipped | 11 | 11（不变） |
| xfailed | 3 | 5（+2 新 xfail，-1 转正，+1 strict 升级） |
| XPASS 告警 | 0 | 0（strict=True 确保不静默通过） |

---

## A/B/C 分类

| 类别 | 内容 | 状态 |
|:----:|------|:----:|
| **A** | PlanComparator `compare_program()` 多语句扁平化 + Pipeline 暴露 SqlProgram | 本轮实施 |
| **A** | `TestNYCCase06SparkDualChain` 测试类框架（含防御性非 xfail 测试） | 本轮实施 |
| **A** | xfail 状态审计——1 个转正 + 2 个保留 + 标记升级 | 本轮实施 |
| **B** | `compute_ratios` 比率计算功能实现 | **不实施**——后续 Phase |
| **B** | `risk_label` CASE WHEN 输出功能实现 | **不实施**——后续 Phase |
| **B** | `test_spark_orchestrator_logic_equivalence` xfail→pass（待 B 类功能收口） | **不实施**——后续 Phase |
| **C** | `test_temp_tables_cleaned_after_execution`——Pipeline 暴露 cleanup_status | 待后续 Phase |
| **C** | Case 05 Comparator 窗口函数 NOT_COVERED → LOGIC_EQUIVALENT | 独立计划 |

---

## 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R7-Case06-B | 比率计算/CASE WHEN B 类遗留——block `test_spark_orchestrator_logic_equivalence` 严格断言 | B | 后续 Phase，当前 xfail(strict=True) |
| R7-Case06-Comp | Case06 Comparator 框架就绪但严格断言 xfail——仅防御性测试通过 | B | 双链验证框架已建立，不可绕过；功能就绪后自然转正 |
| R7-Case06-C | cleanup_status 暴露 + violation_county 通用化 | C | 后续 Phase |

---

## 是否允许进入实现

**允许。** 本轮计划已明确：

1. **不碰 B 类功能缺失**（比率计算/CASE WHEN）——仅建立 Comparator 框架
2. **不修改任何安全链路**（Validator、Comparator 状态语义、_temp_ 机制）
3. **不引入 CTE、raw SQL 逃生口**
4. **xfail 策略明确**：严格断言 xfail(`strict=True`) 确保功能就绪时 XPASS 告警
5. **A 类改造确定性高**：`compare_program()` 是纯数据流转换——扁平化步骤列表后委托给已有 `compare_plans()` 引擎

**核心平台完成版判定不变**——R7 从 B 类降至 B 类框架就绪（Comparator 已接入，严格断言被功能缺失 block），但 R9（Case05 窗口函数）仍未消除。

---

## 附录：关键技术细节

### SqlProgram 扁平化规则

```
SqlProgram (7 statements)
  stmt[0] crash_boro_agg:  scan(fc) → filter → aggregate → project
  stmt[1] parking_boro_agg: scan(dps) → filter → aggregate → project
  stmt[2] trip_boro_agg: scan(tz, zts) → filter → join → aggregate → project
  stmt[3] trip_crash_join: scan(_temp_trip_boro_agg) → scan(_temp_crash_boro_agg) → join → project
  stmt[4] all_three_join: scan(_temp_trip_crash_join) → scan(_temp_parking_boro_agg) → join → project
  stmt[5] compute_ratios: scan(_temp_all_three_join) → project  ← 比率计算未实现
  stmt[6] risk_label: scan(_temp_compute_ratios) → case_when → project  ← CASE WHEN 未实现
                                        ↑
                                    此 plan 是 export_artifacts 导出的 plans[-1]
```

扁平化后（过滤 `_temp_*` scan）：
```
scan(fc) → filter → aggregate → project      ← stmt[0]
scan(dps) → filter → aggregate → project     ← stmt[1]
scan(tz, zts) → filter → join → aggregate → project  ← stmt[2]
join → project                                 ← stmt[3] (_temp_ scan 已过滤)
join → project                                 ← stmt[4] (_temp_ scan 已过滤)
project                                        ← stmt[5] (_temp_ scan 已过滤)
case_when → project                            ← stmt[6] (_temp_ scan 已过滤)
```

与 SparkPlan（Mapper 从 V1 Contract 产出）对比：
```
read(fc) → filter → aggregate → project
read(dps) → filter → aggregate → project
read(tz, zts) → filter → join → aggregate → project
join → project
join → project
project
case_when → project
```

结论：逻辑结构一致——扁平化对比可行。

### 为什么 `extract_v1()` 不消费 `step_dag` / `temp_tables`

Mapper 将 V1 Contract 视为**聚合后的业务语义描述**而非执行蓝图。它不需要知道 SQL 侧的 _temp_ 表拓扑——它只需要知道：有哪些输入表、哪些 Join、哪些聚合、最终输出列。PySpark 通过变量传递 DataFrame（`df1 → df2 → df3`），天然不需要临时表。

### 防御性测试定位

`test_spark_comparator_report_not_mismatch` 的作用：即使步骤 1-5 的 projector (stmt[0]-[4]) 列数与 Spark 侧不一致（因 compute_ratios 列未生成），Comparator 也不应报 `LOGIC_MISMATCH`——它应标记 `NOT_COVERED`（部分步骤未覆盖对比）或 `LOGIC_EQUIVALENT`（已覆盖步骤全等价）。这个防御性测试确保 Comparator 状态语义**不被未来的 regressions 泛化为 MISMATCH**。
