# Phase 6-8 全局验收计划

> **For agentic workers:** 本计划涉及两个子任务（组件复核 + Orchestrator 骨架串联），建议直接手动执行——复核任务只需阅读+记录，改造任务只涉及 2 个文件。
> **状态：** ✅ 已完成（2026-07-04）。后续进展见本文末尾"后续状态"章节。

**目标：** 完成 Phase 6-8 骨架级端到端验收（Contract → Mapper → Compiler → Validator → ReviewPackage），复核各组件测试覆盖与残留风险，登记 C 类环境依赖风险。

**架构：** 让 Orchestrator 真实调用 mapper / compiler / validator 三个已有组件实例，形成一条可运行的骨架链路。Comparator 和 PhysicalVerifier 因依赖外部条件（SqlBuildPlan、Spark 环境），在当前验收中标记 SKIPPED 并登记原因。

**基线：** 513 passed, 11 skipped, ruff clean, git clean。

---

## Global Constraints

- 不接入真实 LLM（Developer 阶段标记 SKIPPED）
- 不强制点亮真实 Spark slow tests（11 个 skipped 保持 skipped）
- 不引入生产数据（仅使用测试 fixture 中的 Contract）
- 不改变 SQL/Spark 安全边界
- 不新增 Memory 机制
- 不修改 mapper.py / compiler.py / validator.py 的已有接口签名
- Orchestrator 改造只涉及 `orchestrator.py` 和 `test_orchestrator.py`

---

## 能力清单（诚实声明，写入最终报告）

### ✅ 已完成事实（组件独立验证通过）

| 组件 | 证据 | 覆盖规模 |
|------|------|---------|
| Mapper | `test_spark_plan.py` 全绿 | Contract → SparkPlan（9 种 step 映射） |
| Compiler | `test_spark_compiler.py` 全绿 | 9 种 step → PySpark DSL（含窗口函数帧边界） |
| Renderer | `test_renderer.py` 全绿 | 安全渲染 + 恶意输入拒绝 |
| Validator | `test_spark_validator.py` 全绿 | E601-E608 错误码全量覆盖 |
| PhysicalVerifier (DuckDB) | `test_physical_verifier.py` 全绿 | DuckDB 侧安全校验 + 结果对比 |
| RepairPlanner | `test_repair_planner.py` 全绿 | 5 种 RepairAction 分类 |
| Snapshot | `test_snapshot.py` 全绿 | Snapshot 构建 + Manifest 完整性 |
| Annotations | `test_annotations.py` 全绿 | StepAnnotation 模型 + AnnotationValidator |
| PlanComparator | `test_plan_comparator.py` 全绿 | 9 种 step 逻辑对比 |

### ⚠️ 骨架级能力（接口对接完成，但未真实全链路串联）

| 能力 | 实际状态 | 差距 |
|------|---------|------|
| Orchestrator 编排 | 状态机骨架——`run()` 不实例化任何组件 | **本轮改造目标**：真实调用 mapper/compiler/validator |
| Developer 标注 | Mock callable 注入——Prompt 安全构造已验证 | 真实 LLM 未接入（C 类风险） |
| Harness 评测 | 结果聚合器——统计 `case.passed` 布尔值 | 不执行真实编译/验证 |
| ReviewPackage | 数据模型完整 + ReviewBuilder 确定性构建 | 已可用 |

### ❌ Mock / Skipped / Deferred（诚实登记）

| 项目 | 状态 | 原因 | 分类 |
|------|------|------|------|
| 11 个真实 Spark 用例 | `SKIPPED`（需 `--run-slow` + PySpark 环境） | 本地无 Spark 运行时 | C 类环境依赖 |
| Developer LLM 接入 | Mock callable | ProviderAdapter 未接入 | C 类延期 |
| Comparator 真实对比 | 未执行 | 需要 SqlBuildPlan（SQL pipeline 产出） | 骨架级——接口已对接 |
| PhysicalVerifier Spark 侧 | Mock | 需要 Spark 子进程 | C 类环境依赖 |
| Harness 真实样本评测 | 未执行 | 需要业务样本集 | C 类延期 |

---

## Task 1: 组件测试覆盖复核（A 类——只读复核，不改代码）

**目标：** 逐组件 review 已有测试覆盖，确认无遗漏的边界/异常场景，输出复核记录。

**复核清单：**

- [ ] **Mapper**（`tests/spark/test_spark_plan.py`）
  - 9 种 step 映射是否每种至少 1 个正向用例？
  - 异常路径：Contract 无输入表、空 window_specs 等是否有覆盖？
  - 确定性：同一 Contract 两次映射产出相同 hash？

- [ ] **Compiler**（`tests/spark/test_spark_compiler.py`）
  - 9 种 step 编译是否每种至少 1 个正向用例？
  - 窗口函数 9 种（ROW_NUMBER/RANK/DENSE_RANK/NTILE/LAG/LEAD/SUM_OVER/AVG_OVER/COUNT_OVER）各至少 1 个？
  - 异常路径：NTILE 缺 input_column、LAG 缺 input_column 是否正确抛出？
  - 确定性：同一 plan 两次编译产出相同 raw_hash？

- [ ] **Renderer**（`tests/spark/test_renderer.py`）
  - SQL 注入拒绝、非法标识符拒绝是否有覆盖？
  - 帧边界 snake_case→camelCase 映射是否正确？

- [ ] **Validator**（`tests/spark/test_spark_validator.py`）
  - E601-E608 每种错误码至少 1 个触发用例？
  - 合法 PySpark DSL 不误杀？

- [ ] **PhysicalVerifier**（`tests/spark/test_physical_verifier.py`）
  - DuckDB 安全校验（字符串剥离、CTE 禁止、多语句拒绝、关键词黑名单）是否每种至少 1 个？
  - DuckDB 结果对比（scan/filter/project/sort/limit/aggregate/join/case_when/window）是否覆盖？

- [ ] **RepairPlanner**（`tests/spark/test_repair_planner.py`）
  - 5 种 RepairAction 分类是否每种至少 1 个？

- [ ] **Snapshot**（`tests/spark/test_snapshot.py`）
  - Snapshot 构建 + Manifest hash 确定性 + 完整性校验？

**复核输出格式（每组件）：**
```
| 组件 | 正向用例 | 异常用例 | 确定性用例 | 缺口 |
|------|---------|---------|-----------|------|
| Mapper | N | N | N | 无 / 描述缺口 |
```

**这一步不改代码。** 如发现缺口，记录到风险清单，不在本轮修复（除非是 A 类阻塞——如安全校验漏检）。

---

## Task 2: Orchestrator 骨架串联改造（B 类——最小改造）

**目标：** 让 `SparkOrchestrator.run()` 真实调用 mapper → compiler → validator，产出含真实 hash 的 PipelineState，最终通过 ReviewBuilder 构建 ReviewPackage。

**修改范围：** 仅 2 个文件

| 文件 | 操作 | 说明 |
|------|------|------|
| `src/tianshu_datadev/spark/orchestrator.py` | **修改** | `run()` 真实调用组件 |
| `tests/spark/test_orchestrator.py` | **修改** | 新增骨架级 E2E 测试，保留现有 22 个状态机测试 |

### Step 1: 改造 `SparkOrchestrator.run()`

**改造前（当前代码）：**
```python
def run(self, contract_hash, stage_failures=None, retry_count=0):
    failures = stage_failures or {}
    state = SparkPipelineState(contract_hash=contract_hash, retry_count=retry_count)
    # 只检查 stage_failures dict，不调用任何组件
    for stage in SparkPipelineStage:
        if stage.value in failures:
            state.record_stage_result(stage, "FAILURE")
        elif stage == SparkPipelineStage.DEVELOPER and self._developer_service is None:
            state.record_stage_result(stage, "SKIPPED")
        else:
            state.record_stage_result(stage, "SUCCESS")
    state.derive_overall_status()
    return state
```

**改造后（新增 `_execute_stage` 分发）：**

`run()` 签名改为接受 `DataTransformContractV1`（而非仅 `contract_hash`），保留 `stage_failures` 用于测试注入：

```python
def run(
    self,
    contract: DataTransformContractV1 | None = None,
    contract_hash: str = "",
    stage_failures: dict[str, str] | None = None,
    retry_count: int = 0,
) -> SparkPipelineState:
```

核心逻辑：
1. 如果 `stage_failures` 非空 → 使用注入失败模式（保持现有行为，向后兼容）
2. 如果 `contract` 非空 → 真实执行 MAPPER / COMPILER / VALIDATOR 阶段
3. DEVELOPER 阶段：有 developer_service → 调用；无 → SKIPPED
4. COMPARATOR 阶段：需要 SqlBuildPlan → 无则 SKIPPED
5. PHYSICAL_VERIFIER 阶段：需要 Spark 环境 → 无则 SKIPPED

**`_execute_stage` 分发逻辑（伪代码）：**

```python
def _execute_stage(self, stage, state, contract, stage_failures):
    failures = stage_failures or {}
    
    # 测试注入优先
    if stage.value in failures:
        state.record_stage_result(stage, "FAILURE")
        state.errors.append(f"[{stage.value}] {failures[stage.value]}")
        return
    
    if stage == SparkPipelineStage.MAPPER:
        if contract is None:
            state.record_stage_result(stage, "SKIPPED")
            return
        result = map_contract_to_spark_plan(contract)
        if result.success and result.spark_plan is not None:
            state.spark_plan_hash = SparkPlan.compute_plan_hash(result.spark_plan)
            state.record_stage_result(stage, "SUCCESS")
            # 缓存 SparkPlan 供后续阶段使用
            self._cached_plan = result.spark_plan
        else:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[MAPPER] 映射失败：{result.gaps}")
    
    elif stage == SparkPipelineStage.DEVELOPER:
        if self._developer_service is None:
            state.record_stage_result(stage, "SKIPPED")
        else:
            # 真实调用 Developer（需要 SparkPlan）
            ...
    
    elif stage == SparkPipelineStage.COMPILER:
        plan = getattr(self, "_cached_plan", None)
        if plan is None:
            state.record_stage_result(stage, "SKIPPED")
            return
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        state.compiled_code_sha256 = result.raw_hash
        state.record_stage_result(stage, "SUCCESS")
        self._cached_compile_result = result
    
    elif stage == SparkPipelineStage.VALIDATOR:
        compile_result = getattr(self, "_cached_compile_result", None)
        if compile_result is None:
            state.record_stage_result(stage, "SKIPPED")
            return
        validator = SparkStaticValidator()
        validation = validator.validate(compile_result.raw_pyspark)
        if validation.is_valid:
            state.record_stage_result(stage, "SUCCESS")
        else:
            state.record_stage_result(stage, "FAILURE")
            for e in validation.errors:
                state.errors.append(f"[VALIDATOR] {e.error_code}: {e.detail}")
    
    elif stage in (SparkPipelineStage.COMPARATOR, SparkPipelineStage.PHYSICAL_VERIFIER):
        # 骨架级——标记 SKIPPED 并说明原因
        reason = "需要 SqlBuildPlan（SQL pipeline）" if stage == SparkPipelineStage.COMPARATOR else "需要 Spark 运行时环境"
        state.record_stage_result(stage, "SKIPPED")
        state.errors.append(f"[{stage.value}] SKIPPED: {reason}")
```

### Step 2: 新增骨架级 E2E 测试

在 `tests/spark/test_orchestrator.py` 新增 `TestOrchestratorSkeletonE2E` 类：

```python
class TestOrchestratorSkeletonE2E:
    """骨架级端到端——Orchestrator 真实调用 mapper → compiler → validator。"""

    def test_skeleton_e2e_contract_to_review_package(self):
        """最小真实链路：Contract → Mapper → Compiler → Validator → ReviewPackage。"""
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            ContractOutputColumn,
            ContractPredicate,
            ContractSort,
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder

        # 构造最小 Contract——scan + filter + project + sort
        contract = DataTransformContractV1(
            contract_id="acceptance_test_001",
            version="v1",
            input_tables=[
                ContractInputTable(
                    table_name="dwd.order_detail",
                    alias="od",
                    input_key="order_detail",
                ),
            ],
            predicates=[],
            output_columns=[
                ContractOutputColumn(
                    column_name="order_id",
                    alias="order_id",
                    source_table="od",
                ),
                ContractOutputColumn(
                    column_name="amount",
                    alias="amount",
                    source_table="od",
                ),
            ],
            sorts=[
                ContractSort(column="amount", direction="DESC"),
            ],
        )

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract=contract, contract_hash="acceptance_test_001")

        # 验证阶段结果
        assert state.stage_results["MAPPER"] == "SUCCESS"
        assert state.stage_results["COMPILER"] == "SUCCESS"
        assert state.stage_results["VALIDATOR"] == "SUCCESS"
        assert state.stage_results["DEVELOPER"] == "SKIPPED"  # 无 Developer 注入
        assert state.stage_results["COMPARATOR"] == "SKIPPED"  # 无 SqlBuildPlan
        assert state.stage_results["PHYSICAL_VERIFIER"] == "SKIPPED"  # 无 Spark

        # 验证 hash 链——Mapper 和 Compiler 产出真实 hash
        assert state.spark_plan_hash != "", "Mapper 应产出真实 plan hash"
        assert state.compiled_code_sha256 != "", "Compiler 应产出真实代码 hash"

        # 验证全局状态——逻辑链路通过但物理未执行
        assert state.overall_status == SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED

        # ReviewBuilder 产出统一交付物
        builder = SparkReviewBuilder()
        pkg = builder.build(state)
        assert pkg.package_id.startswith("pkg_")
        assert pkg.provenance.contract_hash == "acceptance_test_001"
        assert pkg.provenance.spark_plan_hash == state.spark_plan_hash
        assert pkg.provenance.compiled_code_sha256 == state.compiled_code_sha256
        assert pkg.overall_status == "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"

    def test_skeleton_e2e_hash_determinism(self):
        """同一 Contract 两次 run 产出相同 hash 链——证明确定性。"""
        ...

    def test_skeleton_e2e_validator_rejects_bad_compile(self):
        """Validator 阶段正确拒绝含恶意代码的编译产物。"""
        ...

    def test_backward_compat_stage_failures_still_works(self):
        """stage_failures 注入模式仍正常工作——向后兼容。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract_hash="test",
            stage_failures={"COMPILER": "测试注入"},
        )
        assert state.stage_results["COMPILER"] == "FAILURE"
        assert state.overall_status == SparkPipelineStatus.REPAIR_NEEDED

    def test_run_without_contract_skips_mapper(self):
        """无 contract 时 MAPPER 标记 SKIPPED——不崩溃。"""
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="no_contract")
        assert state.stage_results["MAPPER"] == "SKIPPED"
```

### Step 3: 现有 22 个状态机测试必须全绿

`stage_failures` 注入模式保持向后兼容——现有测试不需要任何修改。

---

## Task 3: C 类风险登记（不改代码，写入文档）

将以下内容写入 `docs/risks/phase-6-8-known-risks.md`：

### C1: 真实 Spark 物理验证（11 个 skipped 用例）

- **风险等级**：C（环境依赖）
- **影响范围**：`test_physical_verifier.py::TestRealSparkExecution` 全部 11 个参数化用例
- **触发条件**：`pytest --run-slow` + 本地安装 PySpark + Java 8+
- **当前状态**：DuckDB 侧验证通过，Spark 侧 mock 验证通过，但双引擎真实对比未执行
- **登记策略**：不阻塞全局验收；进入业务集成前需在目标环境中点亮

### C2: LLM ProviderAdapter 接入

- **风险等级**：C（延期实现）
- **影响范围**：`SparkDeveloperService` 当前使用 mock callable
- **当前状态**：Prompt 安全构造已验证（不含 SQL 文本），AnnotationValidator 校验已就绪，只差 ProviderAdapter 接入
- **登记策略**：不阻塞全局验收；业务集成前需确认 LLM 接入方案并完成集成测试

### C3: Comparator 真实逻辑对比

- **风险等级**：骨架级（接口已对接，数据未通）
- **影响范围**：`PlanComparator.compare()` 需要 SqlBuildPlan
- **当前状态**：接口已就绪，Phase 6-8 计划中 Comparator 属于逻辑链路组件，需 SQL pipeline 产出 SqlBuildPlan
- **登记策略**：不阻塞全局验收；业务集成时随 SQL 链路一起验收

### C4: Harness 真实样本评测

- **风险等级**：C（延期实现）
- **影响范围**：`SparkHarnessRunner` 当前是结果聚合器
- **当前状态**：5 维度框架已定义，评测用例需填充真实业务样本
- **登记策略**：不阻塞全局验收；业务集成前需准备至少 5 个业务样本（每维度 1 个）

---

## 验收命令

```bash
# 1. 骨架级 E2E 测试（含新增）
python -m pytest tests/spark/test_orchestrator.py -v --tb=short

# 2. 全量 Spark 测试（462 passed, 11 skipped——skipped 数不变）
python -m pytest tests/spark/ -v --tb=short

# 3. Artifacts 测试（51 passed——不变）
python -m pytest tests/artifacts/ -v --tb=short

# 4. ruff 检查
python -m ruff check src/tianshu_datadev/spark/orchestrator.py tests/spark/test_orchestrator.py

# 5. git diff 检查
git diff --check
```

**预期结果：**
- 骨架级 E2E 测试：4 个新测试全绿
- 全量回归：≥513 passed（含新增），skipped 保持 11
- ruff：零告警
- git diff：clean

---

## 风险登记输出

| 风险编号 | 分类 | 描述 | 处置 |
|---------|------|------|------|
| C1 | C-环境依赖 | 11 个真实 Spark 用例 skipped | 业务集成前在目标环境点亮 |
| C2 | C-延期 | LLM ProviderAdapter 未接入 | 业务集成前确认方案 |
| C3 | 骨架级 | Comparator 需 SqlBuildPlan | 随 SQL 链路一起验收 |
| C4 | C-延期 | Harness 真实样本评测 | 业务集成前准备样本集 |
| R1 | A-已消除 | Orchestrator 不真实调用组件 | 本轮改造消除 |
| R2 | B-已识别 | Phase 8 报告 "全链路完整证明" 表述偏强 | 本报告已修正为 "骨架级端到端验收" |

---

## 退出标准

- [ ] Orchestrator 真实调用 mapper → compiler → validator（产出真实 hash）
- [ ] 骨架级 E2E 测试通过（Contract → PipelineState → ReviewPackage）
- [ ] 现有 22 个状态机测试零退化
- [ ] `stage_failures` 注入模式向后兼容
- [ ] 组件复核记录完整（每个组件有正向/异常/确定性覆盖统计）
- [ ] C 类风险文档已写入 `docs/risks/`
- [ ] 全量回归 ≥513 passed, 11 skipped
- [ ] ruff 零告警

### 通过后是否可进入业务集成前置准备？

**条件性可以。** 满足以下全部条件时允许进入业务集成前置准备：

1. 骨架级 E2E 验收通过（本计划全部退出条件满足）
2. C1-C4 风险已登记，且业务集成计划中明确每个风险的处置方案
3. 业务集成不要求真实 Spark 环境或真实 LLM（或已为这些依赖准备环境）

**不建议**直接进入生产业务集成。应先完成：
- 业务集成前置准备（确认目标环境、数据源、LLM 接入方案）
- 然后再执行业务集成（含真实 Spark + LLM 点亮）

---

## A/B/C 分类汇总

| 分类 | 内容 | 处置 |
|------|------|------|
| **A** | 组件测试覆盖缺口（复核中发现） | 记录，如非安全相关不在本轮修复 |
| **B** | Orchestrator 骨架串联——`run()` 真实调用 mapper/compiler/validator | **本轮执行** |
| **B** | Phase 8 报告 "全链路完整证明" 表述修正 | 本报告已修正 |
| **C** | 真实 Spark 11 用例 | 登记为环境依赖，业务集成前点亮 |
| **C** | LLM ProviderAdapter 接入 | 登记为延期，业务集成前完成 |
| **C** | Harness 真实样本评测 | 登记为延期，业务集成前准备 |
| **C** | Comparator 真实对比 | 登记为骨架级，随 SQL 链路验收 |

---

## 非技术人员解释

**之前 Phase 8 做了什么？**
团队造好了所有零件（编译器、安全检查器、质量对比器）并给每个零件单独做了质量验证。Phase 8 画了一张"流水线图纸"（Orchestrator），标明了零件应该按什么顺序组装，但还没有真正把它们装在一起试运行。

**这次全局验收要做什么？**
把图纸变成一条能转动的流水线——让原料（Contract）真的经过"加工"（Mapper → Compiler）和"质检"（Validator），最后产出一张带完整追溯标签的"合格证"（ReviewPackage）。这条流水线现在还缺两个环节：一个是需要另一条生产线（SQL pipeline）配合的"双线对比站"（Comparator），一个是需要特殊设备（Spark 环境）的"压力测试站"（PhysicalVerifier）。这两个环节的接口已经预留好了，但目前先跳过，等设备到位再接入。

**为什么这样做最稳妥？**
因为只改一张图纸（Orchestrator），不动任何已经验证过的零件。流水线转起来之后如果有问题，一定是装配方式的问题，不会是零件本身的问题——排查范围极小，风险可控。

---

## 后续状态（2026-07-04 更新）

> 本章节在全局验收完成后追加，记录项目推进过程中原计划中各风险项的消解情况。

### 进展时间线

| 日期 | 阶段 | 关键成果 | 方案书 |
|------|------|---------|--------|
| 2026-07-04 | Phase 6-8 全局验收 | Orchestrator 骨架串联，521 passed, 11 skipped | 本文档 |
| 2026-07-04 | R3 收口 | Mapper input_alias 空值修复，真实 Contract E2E 全链路通过 | — |
| 2026-07-04 | 业务集成前置准备 | R3 状态同步 + C1-C4 验收路径矩阵 | `02-business-integration-prep.md` |
| 2026-07-04 | **业务集成执行第一轮** | **C1 点亮（11/11）+ C2 接入方案** | `03-business-integration-round1.md` |

### 原风险项当前状态

| 编号 | 原状态 | 当前状态 | 说明 |
|------|--------|---------|------|
| C1 | C-环境依赖 | **✅ 已消除** | 11/11 真实 Spark 双引擎物理验证通过（PySpark 4.1.2） |
| C2 | C-延期 | **📋 方案已定** | ProviderAdapter 接入方案已制定，4 个 Task 待实现 |
| C3 | 骨架级 | 骨架级 | 等待 SQL pipeline 就绪 |
| C4 | C-延期 | C-延期 | 等待业务方提供样本 |
| R1 | A-已消除 | ✅ | Orchestrator 已真实调用组件 |
| R2 | B-已识别 | ✅ | 表述已修正为"骨架级端到端验收" |
| R3 | — | ✅ | Mapper input_alias 已修复 |
| R4 | — | ✅ | PhysicalVerifier SKIPPED 语义已修正 |

### 当前基线

- **全量回归：** 521 passed, 11 skipped（零退化）
- **C1 物理验证：** 69/69 passed（含 11/11 真实 Spark 双引擎对比，PySpark 4.1.2）
- **ruff：** 零告警
