# CASE WHEN condition UNSUPPORTED 物理验证白名单实施计划

> **C 类架构风险** — 设计已批准，实施中不新增状态枚举、验证 API、前端组件
> **设计文档**: `docs/case_when物理验证白名单设计_20260717_0918.md`

**目标**：Validator 通过 + Comparator 仅 case_when-condition UNSUPPORTED（其他 step 全部 EQUIVALENT）时，放行物理验证作为人审证据。不改架构边界、状态流转、REVIEW_READY 判定。

**改动文件**：`src/tianshu_datadev/api/pipeline.py`（1 文件核心 + 测试）

## Global Constraints

- 不新增 PhysicalVerificationStatus 枚举值
- 不新增 PhysicalVerifier 方法
- `_should_physical_verify` 返回 bool，不预留字符串模式
- 白名单场景固定 spark_ok=False、requires_human_review=True、review_ready=False
- 物理不一致阶段状态为 "failed"，不是 "completed"
- sample_rows 最多 5 条/侧
- 所有断言必须包含中文错误消息
- 所有注释使用中文

---

### Task 1: 核心门禁 + 报告保存 + run_spark_stage 有界响应

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`
  - `SparkStageContext` 新增 `physical_verify_report` 字段（约行 4509）
  - Pipeline 类新增 `_should_physical_verify()` 静态方法
  - `_do_spark_physical_verify` 入口增加门禁检查、保存报告到 context
  - `run_spark_stage` PHYSICAL_VERIFIER 结果返回有界摘要
  - 物理不一致时 status→"failed"

**接口产出：** `_should_physical_verify(validator_ok: bool, comparator_report: PlanComparisonReport | None) -> bool`
**接口产出：** `SparkStageContext.physical_verify_report: PhysicalVerificationReport | None`
**接口产出：** `run_spark_stage` PHYSICAL_VERIFIER result dict 含 status/row_count_match/schema_match/total_diff_count/sample_rows

- [ ] **Step 1: SparkStageContext 新增字段**

在 `SparkStageContext`（约行 4509）的 `cre_shadow_report` 字段后新增：

```python
physical_verify_report: "PhysicalVerificationReport | None" = None
```

需要 import PhysicalVerificationReport（或在 `"..."` 字符串注解中延迟解析）。

- [ ] **Step 2: 新增 _should_physical_verify 静态方法**

在 Pipeline 类中（靠近 `_do_spark_physical_verify` 之前或之后）新增：

```python
@staticmethod
def _should_physical_verify(
    validator_ok: bool,
    comparator_report: "PlanComparisonReport | None",
) -> bool:
    """判定 PHYSICAL_VERIFIER 是否应执行。"""
    if not validator_ok:
        return False
    if comparator_report is None:
        return False
    # LOGIC_EQUIVALENT → 正常执行
    if comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT:
        return True
    # 白名单：仅 case_when UNSUPPORTED + 其他 step 全部 EQUIVALENT
    if comparator_report.status == ComparisonStatus.LOGIC_UNSUPPORTED:
        has_case_when_unsupported = False
        for r in comparator_report.step_results:
            if r.verdict == EquivalenceVerdict.NOT_EQUIVALENT:
                return False
            if r.verdict == EquivalenceVerdict.UNSUPPORTED_COMPARISON:
                if r.step_type != "case_when":
                    return False
                has_case_when_unsupported = True
        return has_case_when_unsupported
    return False
```

引入 `ComparisonStatus` 和 `EquivalenceVerdict`（已在文件顶部或延迟导入中可用）。
注意：`PlanComparisonReport`、`ComparisonStatus`、`EquivalenceVerdict` 的导入方式需参照文件已有的导入模式。

- [ ] **Step 3: _do_spark_physical_verify 入口加门禁**

在 `_do_spark_physical_verify` 方法（约行 4139）的 Step 1（try import pyspark）之前插入：

```python
# ── 门禁检查 ──
validator_ok = context.stage_results.get("VALIDATOR") == "SUCCESS"
if not self._should_physical_verify(validator_ok, context.comparator_report):
    context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
    context.errors.append(
        "[PHYSICAL_VERIFIER] SKIPPED: 物理验证门禁未通过"
        "（Validator 未通过，或 Comparator 不满足白名单条件）"
    )
    return
```

- [ ] **Step 4: 验证完成后保存报告到 context**

在 `_do_spark_physical_verify` 末尾，`verify()` 调用完成后保存报告。找到 verify() 调用处（约行 4300+），在 try 块内 `verify()` 返回后追加：

```python
# ── 保存完整物理验证报告到上下文 ──
context.physical_verify_report = report
```

- [ ] **Step 5: run_spark_stage 有界摘要**

修改 `run_spark_stage` 的 PHYSICAL_VERIFIER 结果构建（约行 3588-3608）：

```python
# PHYSICAL_VERIFIER——无论 ok/skipped/failed 都返回结果消息
if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
    if current_status == "ok":
        report = context.physical_verify_report
        result = {
            "type": "physical_verify",
            "status": "ok",
            "skipped": False,
            "row_count_match": report.row_count_match if report else None,
            "schema_match": report.schema_match if report else None,
            "total_diff_count": report.total_diff_count if report else None,
            "sample_rows": {
                "duckdb": (report.duckdb_result.sample_rows or [])[:5],
                "spark": (report.spark_result.sample_rows or [])[:5],
            } if report else None,
        }
    else:
        # 收集跳过/失败原因
        verify_errors = [
            e.split("] ", 1)[1] if "] " in e else e
            for e in context.errors
            if e.startswith("[PHYSICAL_VERIFIER]")
        ]
        reason = verify_errors[0] if verify_errors else "物理验证阶段未执行"
        result = {
            "type": "physical_verify",
            "message": reason,
            "skipped": current_status == "skipped",
            "errors": verify_errors,
        }
```

同时需要在 `run_spark_stage` 中**物理不一致时 status→"failed"**。当前 status_map 是：

```python
status_map = {
    "SUCCESS": "ok",
    "FAILURE": "failed",
    "SKIPPED": "skipped",
    "NOT_EXECUTED": "skipped",
}
```

物理验证 `stage_results` 中为 `"SUCCESS"` 但物理不一致时，需要覆写 status 为 `"failed"`。在 `current_status` 赋值后（约行 3488-3490），对 PHYSICAL_VERIFIER stage 增加：

```python
# PHYSICAL_VERIFIER 特殊处理：物理不一致时标记 failed
if stage == SparkPipelineStage.PHYSICAL_VERIFIER and current_status == "ok":
    report = context.physical_verify_report
    if report is not None and not report.row_count_match:
        current_status = "failed"
```

- [ ] **Step 6: 确认现有 import 可用**

确保 `PhysicalVerificationReport`、`PhysicalVerificationStatus` 等类型可被 `SparkStageContext` 和 `run_spark_stage` 使用。如果当前 pipeline.py 顶部没有导入，使用字符串向前引用（`"PhysicalVerificationReport | None"`）避免循环导入。

- [ ] **Step 7: 运行现有测试确认无损**

```bash
python -m pytest tests/api/test_spark_stage_independent.py -x -q 2>&1 | tail -5
python -m pytest tests/api/test_pipeline_stream.py -x -q 2>&1 | tail -5
```

- [ ] **Step 8: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: case_when UNSUPPORTED 物理验证白名单——核心门禁 + 有界摘要

- SparkStageContext 新增 physical_verify_report 字段
- Pipeline._should_physical_verify() 统一 bool 门禁
- _do_spark_physical_verify 入口门禁 + 报告保存
- run_spark_stage 返回有界摘要（含 sample_rows[:5]）
- 物理不一致映射为 failed 状态"
```

---

### Task 2: run_all_full / run_all_full_stream 入口改造

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`
  - `run_all_full`: 简化 PHYSICAL_VERIFIER 逻辑，修正 spark_ok 条件
  - `run_all_full_stream`: 删除重复 COMPARATOR 门禁
  - 两个入口增加 comparator_status/requires_human_review/review_ready 字段

**依赖：** Task 1 完成（`_should_physical_verify` 已存在、`_do_spark_physical_verify` 有门禁）

- [ ] **Step 1: run_all_full 简化**

当前 `run_all_full`（约行 2226-2244）有独立的 PHYSICAL_VERIFIER Validator 门禁：

```python
if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
    validator_passed = any(...)
    if not validator_passed:
        # skip + break
    # execute
    spark_stages.append(...)
    if current_status == "ok":
        spark_ok = True
```

改造为：删掉 validator_passed 检查（门禁已在 `_do_spark_physical_verify` 中），只保留阶段结果记录：

```python
if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
    spark_stages.append({
        "stage": stage_val, "status": current_status,
        "errors": current_errors,
    })
    continue
```

注意：`run_all_full` 的 `spark_ok` 汇总（行 2251-2254）修正为：

```python
# 判断整体 Spark 管线是否通过（物理一致 + COMPARATOR LOGIC_EQUIVALENT）
physver_stage = next(
    (s for s in spark_stages if s["stage"] == "PHYSICAL_VERIFIER"), None,
)
spark_ok = (
    physver_stage is not None
    and physver_stage["status"] == "ok"
    and comparator_status == "LOGIC_EQUIVALENT"
)
```

- [ ] **Step 2: run_all_full 响应增加字段**

在 `run_all_full` 返回值（约行 2256）增加：

```python
"comparator_status": comparator_status,
"requires_human_review": comparator_status != "LOGIC_EQUIVALENT" if comparator_status else True,
"review_ready": comparator_status == "LOGIC_EQUIVALENT" if comparator_status else False,
```

- [ ] **Step 3: run_all_full_stream 删除 COMPARATOR 门禁**

当前 SSE 流（约行 2488-2501）有独立的 COMPARATOR 门禁：

```python
if comparator_status and comparator_status != "LOGIC_EQUIVALENT":
    # skip + break
```

删除此段（门禁已在 `_do_spark_physical_verify` 中）。物理验证阶段直接走到执行/记录逻辑。

- [ ] **Step 4: run_all_full_stream 响应增加字段**

在 SSE 的 `full_result`（约行 2528-2544）追加：

```python
"comparator_status": comparator_status,
"requires_human_review": comparator_status != "LOGIC_EQUIVALENT" if comparator_status else True,
"review_ready": comparator_status == "LOGIC_EQUIVALENT" if comparator_status else False,
```

- [ ] **Step 5: 运行测试确认无损**

```bash
python -m pytest tests/api/ -x -q --timeout=60 2>&1 | tail -5
```

- [ ] **Step 6: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: case_when 白名单——run_all_full/SSE 入口改造

- run_all_full 简化 PHYSICAL_VERIFIER 逻辑
- spark_ok 条件收紧（需 comparator_status==LOGIC_EQUIVALENT）
- run_all_full_stream 删除重复 COMPARATOR 门禁
- 两入口增加 comparator_status/requires_human_review/review_ready
- 白名单场景固定 spark_ok=False, requires_human_review=True"
```

---

### Task 3: API/Pipeline 级别测试

**Files:**
- Create: `tests/api/test_pipeline_physical_verify_whitelist.py`

**依赖：** Task 1+2 完成

测试使用 mock（mock PhysicalVerifier.verify() 的返回值），不依赖真实 PySpark。

- [ ] **Step 1: 创建测试文件**

```python
"""case_when UNSUPPORTED 物理验证白名单测试——API/Pipeline 级别。

覆盖 9 个场景：
1-3: 三条入口白名单执行
4: Validator 未通过跳过
5: 非 case_when 的 UNSUPPORTED 跳过
6: NOT_EQUIVALENT 跳过
7: 无 comparator 报告跳过
8: 物理不一致→failed + 保留报告
9: LOGIC_EQUIVALENT 回归 spark_ok=True
"""
```

关键 mock：模拟 `PlanComparisonReport` 和 `PhysicalVerificationReport`，模拟 comparator 返回特定状态组合。

- [ ] **Step 2: 验收**

```bash
python -m pytest tests/api/test_pipeline_physical_verify_whitelist.py -x -v 2>&1 | tail -30
```

- [ ] **Step 3: 提交**

```bash
git add tests/api/test_pipeline_physical_verify_whitelist.py
git commit -m "test: case_when 物理验证白名单——9 场景 API/Pipeline 测试"
```
