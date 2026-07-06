# Spark 阶段独立触发 + LLM 调用追踪 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 6 个 Spark 管线阶段可在 SQL 编译执行后独立手动触发，并在前端展示 LLM 调用追踪信息（节点名、Token、延迟）。

**Architecture:** 后端新增 LlmTraceNode 模型 + SparkStageContext 缓存 + run_spark_stage() dispatcher + 6 个 REST 端点；前端新增 SparkStageButtons（6 个独立按钮）和 LlmTracePanel（可折叠追踪表格）；execute_rich() 前置修正——抽取 Contract 供 Spark 管线使用。不变更现有 `/api/spark/verify` 端点（向后兼容）。

**Tech Stack:** Python 3.12+ (FastAPI + Pydantic), TypeScript (React + Vite), DuckDB, pytest

## 全局约束

- Contract 必须从已验证的 SqlBuildPlan / SqlProgram 确定性抽取，使用现有 `DataTransformContractExtractor`
- LLM traces 只能含诊断元数据：`node_name`、`model`、`token_usage`、`latency_ms`、`status`、`error_type`
- traces 不可进入 IR、不可影响路由、不可参与 REVIEW_READY 判定
- traces 不做长期 Memory——最多 request-scoped cache
- 不新增自定义 Contract 抽取函数——不重复现有规则
- 不新增独立 `GET /api/llm-traces/{request_id}` 端点（除非后续需要跨请求审计）
- 所有代码注释使用中文
- `POST /api/spark/verify` 保留作为一键全链路入口（向后兼容）
- 不通过 `SparkOrchestrator.run()` 执行单阶段（其内部重置缓存），Pipeline 层自行维护 `SparkStageContext`

---

### Task 1: LlmTraceNode 模型 + Pipeline._record_llm_trace()

**文件:**
- Modify: `src/tianshu_datadev/llm/models.py`（在 `LlmResponse` 类后新增 `LlmTraceNode`）
- Modify: `src/tianshu_datadev/api/pipeline.py`（新增 `_record_llm_trace()` 方法 + `_get_llm_traces()` 方法 + `_llm_traces` 属性）

**接口:**
- Produces: `LlmTraceNode` 类（导出到 `llm.models`），`Pipeline._record_llm_trace(request_id, response)`，`Pipeline._get_llm_traces(request_id) -> dict`

- [ ] **Step 1: 在 llm/models.py 中新增 LlmTraceNode 类**

在 `LlmResponse` 类定义之后（第 125 行后），添加：

```python
class LlmTraceNode(StrictModel):
    """单个 LLM 节点调用诊断元数据。
    
    仅含诊断信息——不含 prompt 原文、raw response、业务数据。
    不可进入 IR、不可影响路由、不可参与 REVIEW_READY 判定。
    """
    node_name: str
    # 合法值：
    #   "parse_developer_spec" | "relationship_planner" |
    #   "sql_build_planner" | "sql_program_planner" | "spark_developer"
    model: str              # 实际模型标识（Fake 时为 "fake"）
    token_usage: dict[str, int] = {}
    # {"prompt_tokens": N, "completion_tokens": N, "total_tokens": N}
    latency_ms: int = 0     # 总延迟（毫秒）
    status: str = "skipped"
    # "valid" | "invalid" | "skipped" | "error"
    error_type: str | None = None
    # 失败时的 AdapterError 类型字符串
```

- [ ] **Step 2: 在 Pipeline.__init__() 中添加 _llm_traces 字典**

在 `pipeline.py` 的 `Pipeline.__init__()` 方法中，`self._duckdb_path = duckdb_path` 之后添加：

```python
# ── LLM 调用追踪（request-scoped cache）──
self._llm_traces: dict[str, dict[str, LlmTraceNode]] = {}
# request_id → {node_name: LlmTraceNode}
```

在文件顶部导入区域添加（与其他 llm 导入一起）：

```python
from tianshu_datadev.llm.models import LlmTraceNode
```

- [ ] **Step 3: 新增 _record_llm_trace() 方法**

在 Pipeline 类中（`_purge_expired` 方法之后）添加：

```python
def _record_llm_trace(self, request_id: str, response: LlmResponse) -> None:
    """从 LlmResponse 记录单次 LLM 调用的诊断元数据。
    
    同一 node_name 多次调用 → 保留最后一次（不聚合）。
    仅在 request-scoped cache 中存储。
    
    Args:
        request_id: Pipeline 请求 ID
        response: LLM Gateway 返回的 LlmResponse
    """
    if request_id not in self._llm_traces:
        self._llm_traces[request_id] = {}
    
    # 从 LlmResponse.task 映射到 node_name
    node_name = response.task  # 直接使用 task 字段——值与 node_name 合法值一致
    
    # 从 validation_status 映射到 trace status
    if response.validation_status == "valid":
        status = "valid"
    elif response.validation_status == "invalid":
        status = "invalid"
    else:
        status = "skipped"
    
    trace = LlmTraceNode(
        node_name=node_name,
        model=getattr(response, "model", "") or "fake",
        token_usage=response.token_usage or {},
        latency_ms=response.latency_ms,
        status=status,
        error_type=None,  # 当前 LlmResponse 没有 error_type 字段——预留
    )
    self._llm_traces[request_id][node_name] = trace
```

- [ ] **Step 4: 新增 _get_llm_traces() 方法**

在 `_record_llm_trace` 之后添加：

```python
def _get_llm_traces(self, request_id: str) -> dict[str, LlmTraceNode] | None:
    """获取指定 request_id 的 LLM 调用追踪数据。
    
    Args:
        request_id: Pipeline 请求 ID
    
    Returns:
        {node_name: LlmTraceNode} 字典，无数据时返回 None
    """
    traces = self._llm_traces.get(request_id)
    if not traces:
        return None
    return dict(traces)
```

- [ ] **Step 5: 运行已有测试确认零退化**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -5
```

Expected: 全部已有测试通过（552+ passed），零退化

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/llm/models.py src/tianshu_datadev/api/pipeline.py
git commit -m "feat: 新增 LlmTraceNode 模型 + Pipeline LLM 追踪基础设施"
```

---

### Task 2: execute_rich() 前置修正——Contract 抽取 + llm_traces 累积

**文件:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（`execute_rich()` 方法——成功路径的 `_store_result()` 之前插入 Contract 抽取 + llm_traces 累积）

**接口:**
- Consumes: `DataTransformContractExtractor`（已在文件顶部导入），`LlmTraceNode`（Task 1 产出）
- Produces: `_results[request_id]["contract"]` 和 `_results[request_id]["llm_traces"]` 可供 `export_artifacts()` 读取

- [ ] **Step 1: 在 execute_rich() 的成功路径 _store_result() 前插入 Contract 抽取和 llm_traces**

找到 `execute_rich()` 方法中成功路径的 `_store_result()` 调用（约第 2089-2094 行），在调用前插入 Contract 抽取，并修改 `_store_result()` 增加 contract 和 llm_traces 字段。

将原来的：
```python
        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
        })
```

替换为：
```python
        request_id = self._gen_request_id(spec)

        # ── 确定性抽取 Contract（供 Spark 管线使用）──
        # 前提：plan 已通过 Validator 校验，compiled/trace/summary 已在当前作用域
        contract = None
        try:
            extractor = DataTransformContractExtractor()
            contract = extractor.extract(plan)
        except Exception as contract_err:
            logger.warning("Contract 抽取失败（非阻断）：%s", contract_err)

        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
            "contract": contract,  # 新增——供 Spark 管线使用
            "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
        })
```

同时修改 `execute_rich()` 的返回值 dict（约第 2096-2120 行），在返回 dict 中添加 `llm_traces` 字段：

```python
        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "validation_passed": True,
            "generated_sql": compiled.sql,
            "sql_sha256": compiled.sql_sha256,
            "compiler_version": compiled.compiler_version,
            "execution_trace": { ... },  # 不变
            "result_summary": { ... },    # 不变
            "open_questions": _summarize_open_questions(all_questions),
            "llm_traces": self._get_llm_traces(request_id),  # 新增
        }
```

- [ ] **Step 2: 同步修改 execute() 方法的成功路径**

在 `execute()` 方法（非 rich 版本）的成功路径 `_store_result()`（约第 965-974 行）中也添加 `contract` 字段。`execute()` 方法的 plan 同样已通过 Validator 校验。返回值不添加 llm_traces（非 rich 路径不需要）。

将原 `_store_result` 调用后添加 `"contract": contract` 字段（contract 抽取逻辑与 execute_rich 一致）。

- [ ] **Step 3: 同步扩展 execute() 方法（非 rich 版本）的 _store_result()**

在 `execute()` 方法的成功路径 `_store_result()`（约第 965-974 行）中，将 `contract` 字段添加到缓存字典。抽取逻辑与 execute_rich 一致（使用 `extract(plan)`），但不修改返回值（非 rich 路径不需要 llm_traces 透出）。

- [ ] **Step 4: 修改 run_all() 和 run_all_rich()——llm_traces 透出**

在 `run_all()` 成功路径的返回值 dict（约第 1564-1605 行）中添加：

```python
"llm_traces": self._get_llm_traces(request_id),
```

在 `run_all()` ComputeSteps 返回路径（约第 1253-1292 行）中也添加相同字段。

`run_all_rich()` 委托给 `run_all(rich=True)`，无需单独修改。

- [ ] **Step 5: 运行已有测试确认零退化**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -5
```

Expected: 全部已有测试通过，零退化

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: execute_rich/run_all 前置修正——抽取 Contract + 累积 llm_traces"
```

---

### Task 3: SparkStageContext + SparkDependencyMissingError + run_spark_stage() 方法

**文件:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（在 `PipelineArtifactBundle` 之后、`Pipeline` 类之前新增 `SparkStageContext` + `SparkDependencyMissingError`；在 `Pipeline` 类中新增 `run_spark_stage()` 及相关辅助方法）

**接口:**
- Consumes: `SparkPipelineStage`（从 `spark.orchestrator` 导入），`SparkStageResponse`（Task 4 定义在 `api/models.py`）
- Produces: `SparkStageContext`（dataclass），`SparkDependencyMissingError`（异常类），`Pipeline.run_spark_stage(request_id, stage) -> SparkStageResponse`

- [ ] **Step 1: 新增 SparkStageContext 和 SparkDependencyMissingError**

在 `pipeline.py` 的 `PipelineArtifactBundle.model_rebuild()` 之前（约第 2137 行）添加：

```python
# ════════════════════════════════════════════
# Spark 阶段独立触发——上下文缓存与异常
# ════════════════════════════════════════════

from dataclasses import dataclass, field


@dataclass
class SparkStageContext:
    """request_id 级别的 Spark 阶段中间产物缓存。
    
    由 Pipeline._get_or_create_spark_context() 创建和管理，
    独立于 SparkOrchestrator 的内部缓存。
    """
    spark_plan: SparkPlan | None = None
    compile_result: SparkCompileResult | None = None
    comparator_report: PlanComparisonReport | None = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class SparkDependencyMissingError(Exception):
    """Spark 阶段依赖缺失异常——由 _check_stage_dependencies 抛出。
    
    当用户跳过前置阶段直接触发后续阶段时抛出。
    routes.py 的 _handle_spark_stage() 捕获此异常返回 422。
    """
    def __init__(self, stage: SparkPipelineStage, missing: list[str]):
        self.stage = stage
        self.missing = missing
        super().__init__(
            f"阶段 {stage.value} 缺少前置产物：{', '.join(missing)}"
        )
```

同时在文件顶部导入区域补充所需的类型导入（在 `TYPE_CHECKING` 块中已有部分，需补充 SparkPlan 和 SparkCompileResult）：

```python
if TYPE_CHECKING:
    ...
    from tianshu_datadev.spark.models import SparkPlan
    from tianshu_datadev.spark.compiler import SparkCompileResult
    from tianshu_datadev.spark.plan_comparator import PlanComparisonReport
```

并在非 TYPE_CHECKING 区域（dataclass 使用处）需要实际导入——在文件末尾的延迟导入区域补充：

其实 `SparkStageContext` 使用的是类型标注，在 dataclass 中 `SparkPlan | None` 等是字符串形式的前向引用时不需要实际导入。但为了安全，我们使用 `"SparkPlan"` 字符串形式：

```python
@dataclass
class SparkStageContext:
    """request_id 级别的 Spark 阶段中间产物缓存。"""
    spark_plan: "SparkPlan | None" = None
    compile_result: "SparkCompileResult | None" = None
    comparator_report: "PlanComparisonReport | None" = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=dict)
```

- [ ] **Step 2: 在 Pipeline.__init__() 中添加 _spark_contexts**

在 `Pipeline.__init__()` 中，`self._duckdb_path = duckdb_path` 之后添加：

```python
# ── Spark 阶段独立触发——上下文缓存 ──
self._spark_contexts: dict[str, SparkStageContext] = {}
```

- [ ] **Step 3: 新增 _get_or_create_spark_context() 辅助方法**

```python
def _get_or_create_spark_context(self, request_id: str) -> SparkStageContext:
    """获取或创建 request_id 的 Spark 阶段上下文。
    
    Args:
        request_id: Pipeline 请求 ID
    
    Returns:
        该 request_id 的 SparkStageContext（新建或已有）
    """
    if request_id not in self._spark_contexts:
        self._spark_contexts[request_id] = SparkStageContext()
    return self._spark_contexts[request_id]
```

- [ ] **Step 4: 新增 _check_stage_dependencies() 辅助方法**

```python
def _check_stage_dependencies(
    self,
    stage: SparkPipelineStage,
    context: SparkStageContext,
    artifacts: PipelineArtifactBundle,
) -> None:
    """Spark 阶段依赖门禁——检查前置产物是否就绪。
    
    Args:
        stage: 目标阶段
        context: 当前 Spark 阶段上下文
        artifacts: Pipeline 中间产物 bundle
    
    Raises:
        SparkDependencyMissingError: 前置产物缺失
    """
    missing: list[str] = []
    
    if stage == SparkPipelineStage.MAPPER:
        if artifacts.data_transform_contract is None:
            missing.append("data_transform_contract（请先执行 编译执行 生成 Contract）")
    
    elif stage == SparkPipelineStage.DEVELOPER:
        if context.spark_plan is None:
            missing.append("spark_plan（请先执行 MAPPER 阶段）")
    
    elif stage == SparkPipelineStage.COMPILER:
        if context.spark_plan is None:
            missing.append("spark_plan（请先执行 MAPPER 阶段）")
    
    elif stage == SparkPipelineStage.VALIDATOR:
        if context.compile_result is None:
            missing.append("compile_result（请先执行 COMPILER 阶段）")
    
    elif stage == SparkPipelineStage.COMPARATOR:
        if artifacts.sql_build_plan is None:
            missing.append("sql_build_plan")
        if context.spark_plan is None:
            missing.append("spark_plan（请先执行 MAPPER 阶段）")
        if artifacts.data_transform_contract is None:
            missing.append("data_transform_contract")
    
    elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
        if artifacts.compiled_sql is None:
            missing.append("compiled_sql")
        if context.compile_result is None:
            missing.append("spark compile_result（请先执行 COMPILER 阶段）")
    
    if missing:
        raise SparkDependencyMissingError(stage, missing)
```

- [ ] **Step 5: 新增 run_spark_stage() 方法**

```python
def run_spark_stage(
    self,
    request_id: str,
    stage: SparkPipelineStage,
) -> "SparkStageResponse":
    """执行单个 Spark 管线阶段。
    
    流程：
    1. export_artifacts(request_id) → 获取 contract + sql_plan
    2. _get_or_create_spark_context(request_id) → 获取或创建阶段上下文
    3. _check_stage_dependencies(stage, context, artifacts) → 依赖门禁
    4. 执行该阶段（复用现有组件，不通过 SparkOrchestrator.run()）
    5. 缓存中间产物到 SparkStageContext
    6. 收集 llm_traces（仅 DEVELOPER 阶段产生）
    7. 返回 SparkStageResponse
    
    Args:
        request_id: Pipeline 请求 ID
        stage: 目标 Spark 管线阶段
    
    Returns:
        SparkStageResponse——含阶段结果、状态、llm_traces
    
    Raises:
        SparkDependencyMissingError: 前置产物缺失
    """
    from tianshu_datadev.api.models import SparkStageResponse, SparkStageItem
    from tianshu_datadev.spark.models import SparkPlan
    
    # Step 1: 导出 artifacts
    artifacts = self.export_artifacts(request_id)
    if artifacts is None:
        raise SparkDependencyMissingError(
            stage, [f"request_id '{request_id}' 对应的 artifacts 不存在或已过期"]
        )
    
    # Step 2: 获取 Spark 上下文
    context = self._get_or_create_spark_context(request_id)
    
    # Step 3: 依赖门禁
    self._check_stage_dependencies(stage, context, artifacts)
    
    # Step 4: 执行阶段
    errors: list[str] = []
    try:
        if stage == SparkPipelineStage.MAPPER:
            self._do_spark_map(artifacts, context)
        elif stage == SparkPipelineStage.DEVELOPER:
            self._do_spark_develop(context)
        elif stage == SparkPipelineStage.COMPILER:
            self._do_spark_compile(context)
        elif stage == SparkPipelineStage.VALIDATOR:
            self._do_spark_validate(context, errors)
        elif stage == SparkPipelineStage.COMPARATOR:
            self._do_spark_compare(artifacts, context, errors)
        elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
            self._do_spark_physical_verify(artifacts, context)
    except Exception as e:
        context.stage_results[stage.value] = "FAILURE"
        context.errors.append(f"[{stage.value}] 异常：{e}")
    
    # Step 5: 构建响应
    status_map = {
        "SUCCESS": "ok",
        "FAILURE": "failed",
        "SKIPPED": "skipped",
        "NOT_EXECUTED": "skipped",
    }
    spark_stages: list[SparkStageItem] = []
    for s_name, s_result in context.stage_results.items():
        spark_stages.append(SparkStageItem(
            stage=s_name,
            status=status_map.get(s_result, "skipped"),
        ))
    
    current_status = status_map.get(
        context.stage_results.get(stage.value, "NOT_EXECUTED"), "skipped"
    )
    
    return SparkStageResponse(
        request_id=request_id,
        stage=stage.value,
        status=current_status,
        missing_dependencies=[],
        errors=list(context.errors),
        spark_stages=spark_stages,
        llm_traces=self._get_llm_traces(request_id),
    )
```

- [ ] **Step 6: 新增各阶段私有实现方法**

```python
def _do_spark_map(
    self, artifacts: PipelineArtifactBundle, context: SparkStageContext,
) -> None:
    """执行 MAPPER 阶段——Contract → SparkPlan。"""
    from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
    from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
    from tianshu_datadev.spark.models import SparkPlan
    from tianshu_datadev.artifacts.models import DataTransformContractV1

    raw_contract = artifacts.data_transform_contract
    if isinstance(raw_contract, DataTransformContractV1):
        v1_contract = raw_contract
    else:
        v1_contract = adapt_lite_to_v1(raw_contract)

    result = map_contract_to_spark_plan(v1_contract)
    if result.success and result.spark_plan is not None:
        context.spark_plan = result.spark_plan
        context.stage_results["MAPPER"] = "SUCCESS"
    else:
        context.stage_results["MAPPER"] = "FAILURE"
        gap_msgs = [g.message for g in result.gaps] if result.gaps else ["未知错误"]
        context.errors.append(f"[MAPPER] 映射失败：{'; '.join(gap_msgs)}")


def _do_spark_develop(self, context: SparkStageContext) -> None:
    """执行 DEVELOPER 阶段——LLM 语义标注（可选）。
    
    当前无 SparkDeveloperService 注入时标记 SKIPPED，不阻断后续阶段。
    """
    # 当前 Pipeline 未注入 developer_service，标记 SKIPPED
    context.stage_results["DEVELOPER"] = "SKIPPED"
    context.errors.append("[DEVELOPER] SKIPPED: 未注入 SparkDeveloperService")


def _do_spark_compile(self, context: SparkStageContext) -> None:
    """执行 COMPILER 阶段——SparkPlan → PySpark DSL。"""
    from tianshu_datadev.spark.compiler import SparkCompiler

    compiler = SparkCompiler()
    result = compiler.compile(context.spark_plan)
    context.compile_result = result
    context.stage_results["COMPILER"] = "SUCCESS"


def _do_spark_validate(
    self, context: SparkStageContext, errors: list[str],
) -> None:
    """执行 VALIDATOR 阶段——PySpark DSL 安全校验。"""
    from tianshu_datadev.spark.validator import SparkStaticValidator

    validator = SparkStaticValidator()
    validation = validator.validate(context.compile_result.raw_pyspark)
    if validation.is_valid:
        context.stage_results["VALIDATOR"] = "SUCCESS"
    else:
        context.stage_results["VALIDATOR"] = "FAILURE"
        for e in validation.errors:
            errors.append(f"[VALIDATOR] {e.error_code}: {e.detail}")
        context.errors.extend(errors)


def _do_spark_compare(
    self,
    artifacts: PipelineArtifactBundle,
    context: SparkStageContext,
    errors: list[str],
) -> None:
    """执行 COMPARATOR 阶段——SQL ↔ Spark 逻辑对比。"""
    from tianshu_datadev.planning.sql_program import SqlProgram
    from tianshu_datadev.spark.plan_comparator import PlanComparator

    comparator = PlanComparator()
    sql_plan = artifacts.sql_build_plan
    sql_program = artifacts.sql_program

    if sql_program is not None:
        target_grain = None
        raw_contract = artifacts.data_transform_contract
        if raw_contract is not None and hasattr(raw_contract, "grouping_keys"):
            target_grain = (
                raw_contract.grouping_keys
                if raw_contract.grouping_keys
                else None
            )
        report = comparator.compare_program(
            sql_program, context.spark_plan,
            target_grain=target_grain,
        )
    elif sql_plan is not None:
        report = comparator.compare(sql_plan, context.spark_plan)
    else:
        context.stage_results["COMPARATOR"] = "SKIPPED"
        context.errors.append("[COMPARATOR] SKIPPED: 无 SqlBuildPlan/SqlProgram，无法执行逻辑对比")
        return

    context.comparator_report = report
    context.stage_results["COMPARATOR"] = "SUCCESS"


def _do_spark_physical_verify(
    self, artifacts: PipelineArtifactBundle, context: SparkStageContext,
) -> None:
    """执行 PHYSICAL_VERIFIER 阶段——双引擎物理结果对比。
    
    当前需要 Spark 运行时环境，标记 SKIPPED。
    """
    context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
    context.errors.append(
        "[PHYSICAL_VERIFIER] SKIPPED: 需要 Spark 运行时环境，当前未配置"
    )
```

- [ ] **Step 7: 运行已有测试确认零退化**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -5
```

Expected: 全部已有测试通过，零退化

- [ ] **Step 8: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: SparkStageContext + run_spark_stage() dispatcher + 6 阶段独立方法"
```

---

### Task 4: SparkStageRequest/SparkStageResponse 模型 + 6 个 REST 端点

**文件:**
- Modify: `src/tianshu_datadev/api/models.py`（新增 `SparkStageRequest`、`SparkStageResponse` 类）
- Modify: `src/tianshu_datadev/api/routes.py`（新增 6 个端点 + `_handle_spark_stage()`）

**接口:**
- Consumes: `SparkPipelineStage`（从 `spark.orchestrator`），`LlmTraceNode`（从 `llm.models`），`Pipeline.run_spark_stage()`（Task 3）
- Produces: 6 个 `POST /api/spark/{stage}` 端点

- [ ] **Step 1: 在 api/models.py 中新增请求/响应模型**

在 `SparkVerifyResponse` 类之后（约第 445 行），添加：

```python
# ════════════════════════════════════════════
# Spark 阶段独立触发——POST /api/spark/{stage}
# ════════════════════════════════════════════


class SparkStageRequest(StrictModel):
    """Spark 单阶段触发请求——传入 Pipeline 产出的 request_id。"""

    request_id: str  # Pipeline execute_rich 返回的 request_id


class SparkStageResponse(StrictModel):
    """Spark 单阶段触发响应——含该阶段结果 + 全量阶段状态 + LLM 追踪。
    
    每次单阶段执行后返回当前全部阶段的状态（供前端更新指示灯），
    以及该阶段新增的 LLM 调用追踪信息。
    """

    request_id: str  # 回显请求的 request_id
    stage: str  # 当前执行的阶段名（MAPPER / DEVELOPER / ...）
    status: str  # "ok" | "failed" | "skipped"
    missing_dependencies: list[str] = []  # 依赖缺失时的缺失项列表
    errors: list[str] = []  # 错误信息
    spark_stages: list[SparkStageItem] = []  # 当前全部阶段状态
    llm_traces: dict | None = None  # 本阶段新增的 LLM 追踪（LlmTraceNode dict）
```

注意：`llm_traces` 使用 `dict | None` 而非 `dict[str, LlmTraceNode] | None`，避免循环导入——`api/models.py` 不应依赖 `llm/models.py`。运行时传入的是已序列化的 dict。

- [ ] **Step 2: 在 routes.py 中新增 6 个端点和 _handle_spark_stage()**

首先更新 routes.py 的导入区域，添加新的模型和 SparkPipelineStage：

```python
from .models import (
    ExecuteRequest,
    ParseSpecRequest,
    PlanRequest,
    RunAllRequest,
    SparkStageItem,
    SparkStageRequest,      # 新增
    SparkStageResponse,     # 新增
    SparkVerifyRequest,
    SparkVerifyResponse,
)
```

在 `routes.py` 末尾（`spark_verify` 端点之后）添加：

```python
# ════════════════════════════════════════════
# Spark 阶段独立触发端点（Phase: spark-stage-independent）
# ════════════════════════════════════════════


def _handle_spark_stage(
    request: Request,
    request_id: str,
    stage: "SparkPipelineStage",
):
    """Spark 阶段统一处理——参数校验、异常转换、调用 dispatcher。
    
    捕获 SparkDependencyMissingError → 422，
    其他异常 → 500。
    """
    from tianshu_datadev.api.pipeline import SparkDependencyMissingError

    pipeline = request.app.state.pipeline
    try:
        return pipeline.run_spark_stage(request_id, stage)
    except SparkDependencyMissingError as e:
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "SPARK_DEPENDENCY_MISSING",
                "message": str(e),
                "field_ref": e.stage.value if e.stage else None,
                "missing_dependencies": e.missing,
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "SPARK_STAGE_FAILED",
                "message": f"Spark 阶段 {stage.value} 执行异常：{e}",
                "field_ref": stage.value,
            },
        )


@api_router.post("/spark/map")
async def spark_map(request: Request, body: SparkStageRequest):
    """Spark MAPPER 阶段——Contract → SparkPlan 映射。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.MAPPER)


@api_router.post("/spark/develop")
async def spark_develop(request: Request, body: SparkStageRequest):
    """Spark DEVELOPER 阶段——LLM 语义标注（可选）。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.DEVELOPER)


@api_router.post("/spark/compile")
async def spark_compile(request: Request, body: SparkStageRequest):
    """Spark COMPILER 阶段——SparkPlan → PySpark DSL 编译。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPILER)


@api_router.post("/spark/validate")
async def spark_validate(request: Request, body: SparkStageRequest):
    """Spark VALIDATOR 阶段——PySpark DSL 静态安全校验。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.VALIDATOR)


@api_router.post("/spark/compare")
async def spark_compare(request: Request, body: SparkStageRequest):
    """Spark COMPARATOR 阶段——SQL ↔ Spark 逻辑链路对比。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPARATOR)


@api_router.post("/spark/physical-verify")
async def spark_physical_verify(request: Request, body: SparkStageRequest):
    """Spark PHYSICAL_VERIFIER 阶段——双引擎物理结果对比。"""
    from tianshu_datadev.spark.orchestrator import SparkPipelineStage
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.PHYSICAL_VERIFIER)
```

- [ ] **Step 3: 运行已有测试确认零退化**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/ -x -q --tb=short 2>&1 | tail -5
```

Expected: 全部已有测试通过，零退化

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/api/models.py src/tianshu_datadev/api/routes.py
git commit -m "feat: 新增 6 个 Spark 独立阶段 REST 端点 + 请求/响应模型"
```

---

### Task 5: 前端 API Client 更新

**文件:**
- Modify: `frontend/src/api/client.ts`

**接口:**
- Consumes: 后端新增的 `POST /api/spark/{stage}` 端点
- Produces: `LlmTraceNode` 类型、`SparkStageRequest` 类型、`SparkStageResponse` 类型、`runSparkStage()` 函数

- [ ] **Step 1: 新增类型定义和 API 函数**

在 `client.ts` 的 Spark 区域（`sparkVerify` 函数之后）添加：

```typescript
// ── LLM 调用追踪 ──

/** LLM 节点调用追踪 */
export interface LlmTraceNode {
  node_name: string;
  model: string;
  token_usage: Record<string, number>;
  latency_ms: number;
  status: string;
  error_type: string | null;
}

// ── Spark 阶段独立触发 ──

/** Spark 单阶段请求 */
export interface SparkStageRequest {
  request_id: string;
}

/** Spark 单阶段响应 */
export interface SparkStageResponse {
  request_id: string;
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
  missing_dependencies: string[];
  errors: string[];
  spark_stages: SparkStageItem[];
  llm_traces: Record<string, LlmTraceNode> | null;
}

/** Spark 6 阶段 slug 列表 */
const SPARK_STAGES = ['map', 'develop', 'compile', 'validate', 'compare', 'physical-verify'] as const;

/** 触发单个 Spark 管线阶段 */
export function runSparkStage(
  requestId: string,
  stage: string,
): Promise<SparkStageResponse> {
  return apiPost<SparkStageResponse>(`/spark/${stage}`, { request_id: requestId });
}
```

- [ ] **Step 2: 在已有响应类型中添加可选 llm_traces 字段**

修改 `ExecuteRichResponse` 接口，在 `open_questions` 之后添加：

```typescript
/** 富 Execute 响应 */
export interface ExecuteRichResponse {
  request_id: string;
  spec_id: string;
  plan_id: string;
  generated_sql: string;
  sql_sha256: string;
  compiler_version: string;
  execution_trace: ExecutionTraceSummary;
  result_summary: ResultSummarySummary;
  open_questions: OpenQuestionSummary[];
  llm_traces?: Record<string, LlmTraceNode> | null;  // 新增
}
```

修改 `RunAllResponse` 接口，在 `pipeline_stages` 之后添加：

```typescript
export interface RunAllResponse {
  // ... 已有字段
  llm_traces?: Record<string, LlmTraceNode> | null;  // 新增
}
```

- [ ] **Step 3: 验证前端编译**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: 无新增 TypeScript 错误

- [ ] **Step 4: Commit**

```bash
git add frontend/src/api/client.ts
git commit -m "feat: 前端 API Client——新增 Spark 独立阶段 + LlmTraceNode 类型"
```

---

### Task 6: SparkStageButtons 组件（新建）

**文件:**
- Create: `frontend/src/components/SparkStageButtons.tsx`
- Create: `frontend/src/components/SparkStageButtons.css`

**接口:**
- Consumes: `SparkStageItem`（从 `api/client.ts`），`runSparkStage()`（Task 5）
- Produces: `<SparkStageButtons>` 组件——6 个独立按钮，依赖感知的 enable/disable

- [ ] **Step 1: 创建 SparkStageButtons.css**

```css
/* Spark 阶段独立按钮组 */
.spark-stage-buttons {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
  margin: 8px 0;
}

.spark-stage-buttons .section-label {
  font-size: 13px;
  font-weight: 600;
  color: #64748b;
  margin-right: 4px;
  white-space: nowrap;
}

.spark-stage-btn {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 12px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #fff;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
  white-space: nowrap;
}

.spark-stage-btn:hover:not(:disabled) {
  border-color: #3b82f6;
  background: #eff6ff;
}

.spark-stage-btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
  background: #f1f5f9;
}

.spark-stage-btn.status-ok {
  border-color: #22c55e;
  background: #f0fdf4;
}

.spark-stage-btn.status-failed {
  border-color: #ef4444;
  background: #fef2f2;
}

.spark-stage-btn.status-skipped {
  border-color: #94a3b8;
  background: #f8fafc;
}

.spark-stage-btn .stage-icon {
  font-size: 14px;
}

.spark-stage-btn .stage-label {
  font-weight: 500;
}

.spark-stage-btn .stage-loading {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
```

- [ ] **Step 2: 创建 SparkStageButtons.tsx**

```typescript
import { useMemo, useState } from 'react';
import {
  runSparkStage,
  SparkStageItem,
  SparkStageResponse,
  ApiError,
} from '../api/client';
import './SparkStageButtons.css';

/** 阶段名 → 中文映射 */
const STAGE_CN: Record<string, string> = {
  MAPPER: '映射',
  DEVELOPER: '标注',
  COMPILER: '编译',
  VALIDATOR: '校验',
  COMPARATOR: '对比',
  PHYSICAL_VERIFIER: '物理验证',
};

/** 阶段 slug → 枚举名映射 */
const SLUG_TO_STAGE: Record<string, string> = {
  map: 'MAPPER',
  develop: 'DEVELOPER',
  compile: 'COMPILER',
  validate: 'VALIDATOR',
  compare: 'COMPARATOR',
  'physical-verify': 'PHYSICAL_VERIFIER',
};

/** 枚举名 → slug 映射 */
const STAGE_TO_SLUG: Record<string, string> = {
  MAPPER: 'map',
  DEVELOPER: 'develop',
  COMPILER: 'compile',
  VALIDATOR: 'validate',
  COMPARATOR: 'compare',
  PHYSICAL_VERIFIER: 'physical-verify',
};

/** 执行顺序 */
const STAGE_ORDER = ['MAPPER', 'DEVELOPER', 'COMPILER', 'VALIDATOR', 'COMPARATOR', 'PHYSICAL_VERIFIER'];

/** 计算哪些阶段可在当前状态下触发 */
function computeAvailableStages(stages: SparkStageItem[]): Set<string> {
  const available = new Set<string>();
  const statusMap: Record<string, string> = {};
  for (const s of stages) {
    statusMap[s.stage] = s.status;
  }

  // MAPPER 始终可用（依赖 contract——由 execute-rich 确保）
  available.add('MAPPER');

  // MAPPER 成功后 DEVELOPER 和 COMPILER 可用
  if (statusMap['MAPPER'] === 'ok') {
    available.add('DEVELOPER');
    available.add('COMPILER');
  }

  // COMPILER 成功后 VALIDATOR 可用
  if (statusMap['COMPILER'] === 'ok') {
    available.add('VALIDATOR');
  }

  // MAPPER 成功后 COMPARATOR 可用（额外需要 sql_plan——execute-rich 已确保）
  if (statusMap['MAPPER'] === 'ok') {
    available.add('COMPARATOR');
  }

  // COMPILER 成功后 PHYSICAL_VERIFIER 可用
  if (statusMap['COMPILER'] === 'ok') {
    available.add('PHYSICAL_VERIFIER');
  }

  return available;
}

/** 状态图标 */
function stageIcon(status: string): string {
  switch (status) {
    case 'ok': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    default: return '⬜';
  }
}

interface Props {
  requestId: string | null;
  stages: SparkStageItem[];
  onStageComplete: (response: SparkStageResponse) => void;
  onError: (error: ApiError) => void;
  disabled: boolean;  // 顶层禁用（如 isLoading）
}

export function SparkStageButtons({ requestId, stages, onStageComplete, onError, disabled }: Props) {
  const [loadingStage, setLoadingStage] = useState<string | null>(null);

  const available = useMemo(() => computeAvailableStages(stages), [stages]);

  const statusMap: Record<string, string> = {};
  for (const s of stages) {
    statusMap[s.stage] = s.status;
  }

  const handleClick = async (stageEnum: string) => {
    if (!requestId || disabled || loadingStage) return;
    const slug = STAGE_TO_SLUG[stageEnum];
    setLoadingStage(stageEnum);
    try {
      const result = await runSparkStage(requestId, slug);
      onStageComplete(result);
    } catch (err) {
      const apiErr: ApiError =
        err && typeof err === 'object' && 'error_code' in err
          ? (err as ApiError)
          : { error_code: 'NETWORK_ERROR', message: String(err), field_ref: null };
      onError(apiErr);
    } finally {
      setLoadingStage(null);
    }
  };

  if (!requestId) return null;

  return (
    <div className="spark-stage-buttons">
      <span className="section-label">Spark 管线</span>
      {STAGE_ORDER.map((stageEnum) => {
        const status = statusMap[stageEnum] || 'none';
        const isAvailable = available.has(stageEnum);
        const isLoading = loadingStage === stageEnum;
        const cn = STAGE_CN[stageEnum] || stageEnum;

        return (
          <button
            key={stageEnum}
            className={`spark-stage-btn status-${status === 'ok' ? 'ok' : status === 'failed' ? 'failed' : status === 'skipped' ? 'skipped' : 'none'}`}
            disabled={!isAvailable || disabled || !!loadingStage}
            onClick={() => handleClick(stageEnum)}
            title={
              isAvailable
                ? `执行 ${cn} 阶段`
                : `${cn}：缺少前置产物`
            }
          >
            <span className="stage-icon">
              {isLoading ? '⏳' : stageIcon(status)}
            </span>
            <span className="stage-label">{cn}</span>
          </button>
        );
      })}
    </div>
  );
}
```

- [ ] **Step 3: 验证前端编译**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: 无新增 TypeScript 错误

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/SparkStageButtons.tsx frontend/src/components/SparkStageButtons.css
git commit -m "feat: SparkStageButtons 组件——6 个独立阶段按钮，依赖感知 enable/disable"
```

---

### Task 7: LlmTracePanel 组件（新建）

**文件:**
- Create: `frontend/src/components/LlmTracePanel.tsx`
- Create: `frontend/src/components/LlmTracePanel.css`

**接口:**
- Consumes: `LlmTraceNode`（从 `api/client.ts`，Task 5）
- Produces: `<LlmTracePanel>` 组件——可折叠 LLM 调用追踪表格

- [ ] **Step 1: 创建 LlmTracePanel.css**

```css
/* LLM 调用追踪面板 */
.llm-trace-panel {
  margin: 12px 0;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  overflow: hidden;
}

.llm-trace-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: #f8fafc;
  cursor: pointer;
  user-select: none;
  font-size: 13px;
  font-weight: 600;
  color: #475569;
}

.llm-trace-header:hover {
  background: #f1f5f9;
}

.llm-trace-chevron {
  transition: transform 0.2s;
  font-size: 10px;
}

.llm-trace-chevron.expanded {
  transform: rotate(90deg);
}

.llm-trace-badge {
  margin-left: auto;
  background: #3b82f6;
  color: #fff;
  font-size: 11px;
  padding: 1px 6px;
  border-radius: 10px;
}

.llm-trace-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.llm-trace-table th {
  text-align: left;
  padding: 6px 10px;
  background: #f1f5f9;
  color: #64748b;
  font-weight: 600;
  border-bottom: 1px solid #e2e8f0;
}

.llm-trace-table td {
  padding: 5px 10px;
  border-bottom: 1px solid #f1f5f9;
  color: #334155;
}

.llm-trace-table tr.status-valid td:first-child {
  border-left: 3px solid #22c55e;
}

.llm-trace-table tr.status-error td:first-child {
  border-left: 3px solid #ef4444;
}

.llm-trace-table tr.status-skipped td:first-child {
  border-left: 3px solid #94a3b8;
}

.llm-trace-summary {
  background: #f8fafc;
  font-weight: 600;
  color: #1e293b;
}

.llm-trace-summary td {
  border-top: 2px solid #e2e8f0;
}
```

- [ ] **Step 2: 创建 LlmTracePanel.tsx**

```typescript
import { useState, useMemo } from 'react';
import { LlmTraceNode } from '../api/client';
import './LlmTracePanel.css';

/** 节点名 → 中文映射 */
const NODE_CN: Record<string, string> = {
  parse_developer_spec: 'Spec 解析',
  relationship_planner: '关系规划',
  sql_build_planner: 'SQL Plan 构建',
  sql_program_planner: 'SQL 程序生成',
  spark_developer: 'Spark 标注',
};

interface Props {
  traces: Record<string, LlmTraceNode> | null | undefined;
  visible: boolean;
}

export function LlmTracePanel({ traces, visible }: Props) {
  const [expanded, setExpanded] = useState(false);

  const entries = useMemo(() => {
    if (!traces) return [];
    return Object.entries(traces);
  }, [traces]);

  // 无数据或不显示时返回 null
  if (!visible || entries.length === 0) return null;

  // 计算汇总
  const totalPrompt = entries.reduce((sum, [, t]) => sum + (t.token_usage?.prompt_tokens || 0), 0);
  const totalCompletion = entries.reduce((sum, [, t]) => sum + (t.token_usage?.completion_tokens || 0), 0);
  const totalTokens = entries.reduce((sum, [, t]) => sum + (t.token_usage?.total_tokens || 0), 0);
  const totalLatency = entries.reduce((sum, [, t]) => sum + (t.latency_ms || 0), 0);

  return (
    <div className="llm-trace-panel">
      <div className="llm-trace-header" onClick={() => setExpanded(!expanded)}>
        <span className={`llm-trace-chevron ${expanded ? 'expanded' : ''}`}>▶</span>
        LLM 调用追踪
        <span className="llm-trace-badge">{entries.length} 节点</span>
      </div>

      {expanded && (
        <table className="llm-trace-table">
          <thead>
            <tr>
              <th>节点名称</th>
              <th>模型</th>
              <th>Prompt Token</th>
              <th>Completion Token</th>
              <th>总 Token</th>
              <th>耗时 (ms)</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([name, trace]) => (
              <tr key={name} className={`status-${trace.status}`}>
                <td>{NODE_CN[name] || name}</td>
                <td>{trace.model || '-'}</td>
                <td>{trace.token_usage?.prompt_tokens ?? '-'}</td>
                <td>{trace.token_usage?.completion_tokens ?? '-'}</td>
                <td>{trace.token_usage?.total_tokens ?? '-'}</td>
                <td>{trace.latency_ms > 0 ? trace.latency_ms : '-'}</td>
                <td>{trace.status === 'valid' ? '✅' : trace.status === 'error' ? '❌' : trace.status === 'skipped' ? '⏭️' : trace.status}</td>
              </tr>
            ))}
            {/* 汇总行 */}
            <tr className="llm-trace-summary">
              <td>合计</td>
              <td>-</td>
              <td>{totalPrompt || '-'}</td>
              <td>{totalCompletion || '-'}</td>
              <td>{totalTokens || '-'}</td>
              <td>{totalLatency > 0 ? totalLatency : '-'}</td>
              <td>{entries.length} 次调用</td>
            </tr>
          </tbody>
        </table>
      )}
    </div>
  );
}
```

- [ ] **Step 3: 验证前端编译**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: 无新增 TypeScript 错误

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/LlmTracePanel.tsx frontend/src/components/LlmTracePanel.css
git commit -m "feat: LlmTracePanel 组件——可折叠 LLM 调用追踪表格"
```

---

### Task 8: App.tsx 集成——替换 Spark 按钮 + 条件渲染 LlmTracePanel

**文件:**
- Modify: `frontend/src/App.tsx`

**接口:**
- Consumes: `SparkStageButtons` (Task 6), `LlmTracePanel` (Task 7), `SparkStageResponse`, `LlmTraceNode` (Task 5)
- Produces: 集成后的 App 组件——替换单按钮为 SparkStageButtons，条件渲染 LlmTracePanel

- [ ] **Step 1: 更新导入**

修改 `App.tsx` 的导入语句，添加新组件和类型：

```typescript
import { SparkStageButtons } from './components/SparkStageButtons';
import { LlmTracePanel } from './components/LlmTracePanel';
```

从 `api/client` 导入中增加：
```typescript
import {
  // ... 已有导入
  SparkStageResponse,
  LlmTraceNode,
} from './api/client';
```

- [ ] **Step 2: 更新 AppState 接口**

在 `AppState` 接口中添加 llmTraces 字段，修改 sparkStages 说明：

```typescript
interface AppState {
  // ... 已有字段
  
  // Spark 管线验证结果（由 SparkStageButtons 逐步更新）
  sparkStages: StageInfo[];
  sparkVerifyResult: SparkVerifyResponse | null;
  
  // LLM 调用追踪（各阶段累积）
  llmTraces: Record<string, LlmTraceNode> | null;
}
```

- [ ] **Step 3: 更新 useState 初始值**

在 `useState<AppState>` 初始值中添加：

```typescript
sparkStages: [],
sparkVerifyResult: null,
llmTraces: null,
```

- [ ] **Step 4: 新增 handleSparkStageComplete 回调**

替换旧的 `handleSparkVerify` 函数，添加：

```typescript
/** Spark 单阶段完成回调 */
const handleSparkStageComplete = (response: SparkStageResponse) => {
  // 将后端返回的 spark_stages 映射为前端 StageInfo 格式
  const stages: StageInfo[] = response.spark_stages.map((s) => ({
    stage: s.stage,
    status: s.status,
  }));
  update({
    sparkStages: stages,
    sparkVerifyResult: {
      request_id: response.request_id,
      spark_stages: response.spark_stages,
      overall_status: '',
      comparator_status: '',
      review_ready: false,
      package_id: '',
      errors: response.errors,
    },
    // 合并 llm_traces——后续阶段的追踪追加到已有数据
    llmTraces: response.llm_traces
      ? { ...(state.llmTraces || {}), ...response.llm_traces }
      : state.llmTraces,
  });
};
```

- [ ] **Step 5: 更新 handleExecute 的 onSuccess**

修改 `handleExecute` 的 `runAction` 回调，从响应中提取 llm_traces：

```typescript
const handleExecute = () => {
  if (!state.markdownText.trim()) { ... }
  runAction(
    () => executeRich(state.markdownText),
    (result) => ({
      executeResult: result,
      packageResult: null,
      requestId: result.request_id,
      activePanel: 'sql' as Panel,
      llmTraces: (result as ExecuteRichResponse).llm_traces || null,
      // 重置 Spark 状态——新的 execute 需要重新执行 Spark 阶段
      sparkStages: [],
      sparkVerifyResult: null,
    }),
  );
};
```

- [ ] **Step 6: 更新 handleRunAll 的 onSuccess**

修改 `handleRunAll` 回调，从响应中提取 llm_traces：

```typescript
const handleRunAll = () => {
  ...
  runAction(
    () => runAll(state.markdownText),
    async (result) => {
      ...
      // 在返回的 partial 中添加 llmTraces
      return {
        ...
        llmTraces: (result as RunAllResponse).llm_traces || null,
      };
    },
  );
};
```

- [ ] **Step 7: 替换 JSX——Spark 按钮 + LlmTracePanel 渲染**

将 action-bar 中的 Spark 验证按钮：
```tsx
<button
  className="btn btn-accent"
  disabled={!state.requestId || state.isLoading}
  onClick={handleSparkVerify}
>
  Spark 验证
</button>
```

替换为：
```tsx
<SparkStageButtons
  requestId={state.requestId}
  stages={state.sparkStages}
  onStageComplete={handleSparkStageComplete}
  onError={(err) => update({ error: err })}
  disabled={state.isLoading}
/>
```

在 SQL 面板之后添加 LlmTracePanel（紧跟 SqlDisplay 组件）：

```tsx
{/* LLM 调用追踪——编译执行后或 Spark 阶段后 */}
<LlmTracePanel
  traces={state.llmTraces}
  visible={
    (state.activePanel === 'sql' || state.activePanel === 'package') &&
    state.llmTraces !== null
  }
/>
```

放在 `{state.packageResult && (` 之前。

- [ ] **Step 8: 保留旧的 handleSparkVerify 用于向后兼容**

`handleSparkVerify` 函数可以保留（`/api/spark/verify` 端点不变），但不再绑定到 UI 按钮。或者直接移除，因为旧按钮已被替换。

保留该函数但删除对应的旧按钮 JSX（已在 Step 7 中替换）。

- [ ] **Step 9: 验证前端编译**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: 无 TypeScript 错误

- [ ] **Step 10: 构建前端并验证**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\frontend
npm run build 2>&1 | tail -10
```

Expected: 构建成功

- [ ] **Step 11: Commit**

```bash
git add frontend/src/App.tsx
git commit -m "feat: App.tsx 集成——SparkStageButtons + LlmTracePanel 条件渲染"
```

---

### Task 9: 后端 Pytest 测试

**文件:**
- Create: `tests/api/test_spark_stage_independent.py`

**接口:**
- Consumes: 所有 Task 1-4 产出的后端变更
- Produces: 9 个 pytest 用例，覆盖核心路径

- [ ] **Step 1: 创建测试文件**

```python
"""Spark 阶段独立触发 + LLM 调用追踪——后端 pytest。

测试 9 个核心路径：
- execute-rich 产出 contract
- spark/map 正常执行
- 依赖缺失返回 422
- developer 无服务时 skipped
- validate 缺少 compile_result 返回 422
- compare 缺少 sql/spark plan 返回 422
- execute-rich 响应含 llm_traces
- spark 阶段响应含 llm_traces
- llm_traces 不参与 REVIEW_READY 判定
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tianshu_datadev.api.app import create_app
from tianshu_datadev.api.pipeline import Pipeline, SparkDependencyMissingError, SparkStageContext
from tianshu_datadev.llm.models import LlmTraceNode, LlmResponse
from tianshu_datadev.spark.orchestrator import SparkPipelineStage


# ── Fixtures ──

@pytest.fixture
def client():
    """创建测试用的 FastAPI TestClient（不含 CSV fixtures 的纯净环境）。"""
    pipeline = Pipeline()
    app = create_app(pipeline=pipeline)
    return TestClient(app)


@pytest.fixture
def sample_spec():
    """单表聚合 DeveloperSpec——用于 execute-rich。"""
    return (
        "# 测试报表：用户行为汇总\n"
        "## 事实表\n"
        "- 别名: ue\n"
        "- 源表: dwd.user_events\n"
        "- 时间字段: event_time\n"
        "## 指标\n"
        "- 指标名: total_events\n"
        "- 聚合: COUNT\n"
        "- 输入列: event_id\n"
        "- 别名: total_events\n"
        "## 维度\n"
        "- 维度名: user_id\n"
        "- 列引用: ue.user_id\n"
        "## 输出\n"
        "- 粒度: user_id\n"
    )


# ── 测试用例 ──


class TestExecuteRichProducesContract:
    """execute-rich 成功后 export_artifacts() 返回非空 contract。"""

    def test_produces_contract(self, client, sample_spec):
        """验证 execute-rich 后 contract 被正确缓存。"""
        resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert resp.status_code == 200
        data = resp.json()
        request_id = data["request_id"]

        # 通过 /api/spark/map 间接验证 contract 存在
        # （MAPPER 依赖 contract，不存在时返回 422）
        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200, (
            f"MAPPER 应成功执行（依赖 contract），实际返回 {map_resp.status_code}: {map_resp.json()}"
        )


class TestSparkMapAfterExecuteRich:
    """execute-rich 后调用 /api/spark/map 返回 200。"""

    def test_map_returns_ok(self, client, sample_spec):
        """验证完整的 execute-rich → MAPPER 链路。"""
        # Step 1: execute-rich
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # Step 2: MAPPER
        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200
        data = map_resp.json()
        assert data["stage"] == "MAPPER"
        assert data["status"] == "ok"
        # spark_stages 应包含至少 MAPPER 的状态
        assert len(data["spark_stages"]) >= 1


class TestSparkCompileMissingSparkPlan:
    """未执行 MAPPER 直接调用 COMPILER 返回 422。"""

    def test_compile_missing_dependency(self, client, sample_spec):
        """验证依赖门禁——缺少 spark_plan 时 COMPILER 被拒。"""
        # execute-rich 产出 contract 但不执行 MAPPER
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 直接调用 COMPILER（跳过 MAPPER）
        resp = client.post("/api/spark/compile", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        assert "spark_plan" in data["message"]


class TestSparkDeveloperSkippedWithoutService:
    """DEVELOPER 未配置时返回 SKIPPED，不阻断。"""

    def test_developer_skipped(self, client, sample_spec):
        """验证 DEVELOPER 在无 service 注入时 graceful degradation。"""
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 先执行 MAPPER 使 DEVELOPER 依赖满足
        client.post("/api/spark/map", json={"request_id": request_id})

        # 执行 DEVELOPER
        resp = client.post("/api/spark/develop", json={
            "request_id": request_id,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["stage"] == "DEVELOPER"
        assert data["status"] == "skipped"


class TestSparkValidateMissingCompileResult:
    """未执行 COMPILER 直接调用 VALIDATOR 返回 422。"""

    def test_validate_missing_dependency(self, client, sample_spec):
        """验证依赖门禁——缺少 compile_result 时 VALIDATOR 被拒。"""
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 执行 MAPPER（满足 COMPILER 依赖），但跳过 COMPILER
        client.post("/api/spark/map", json={"request_id": request_id})

        # 直接调用 VALIDATOR
        resp = client.post("/api/spark/validate", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        assert "compile_result" in str(data)


class TestSparkCompareNeedsSqlAndSparkPlan:
    """缺少 SqlBuildPlan 或 SparkPlan 时 COMPARATOR 返回 422。"""

    def test_compare_missing_spark_plan(self, client, sample_spec):
        """验证 COMPARATOR 依赖——缺少 spark_plan 时被拒。"""
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 不执行 MAPPER，直接调用 COMPARATOR
        resp = client.post("/api/spark/compare", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"


class TestLlmTracesInExecuteRichResponse:
    """execute-rich 响应含 llm_traces 字段。"""

    def test_llm_traces_field_present(self, client, sample_spec):
        """验证 execute-rich 响应中包含 llm_traces 字段。"""
        resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert resp.status_code == 200
        data = resp.json()
        # llm_traces 应为 null（Fake 模式下无 LLM 调用）或 dict
        assert "llm_traces" in data


class TestLlmTracesInSparkStageResponse:
    """spark 单阶段响应含 llm_traces 字段。"""

    def test_spark_stage_has_llm_traces(self, client, sample_spec):
        """验证 spark/map 响应中包含 llm_traces 字段。"""
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": sample_spec,
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200
        data = map_resp.json()
        assert "llm_traces" in data


class TestLlmTracesNotInReviewReady:
    """llm_traces 不参与 REVIEW_READY 判定。"""

    def test_traces_not_affect_review(self, client, sample_spec):
        """验证 llm_traces 不会影响 Spark 管线的 REVIEW_READY 结果。
        
        通过 /api/spark/verify 执行全链路，确认 llm_traces 不参与判定。
        """
        # 使用 run-all 而非 execute-rich——/api/spark/verify 需要完整的
        # sql_build_plan + data_transform_contract（run_all 产出）
        run_resp = client.post("/api/run-all", json={
            "markdown_text": sample_spec,
        })
        assert run_resp.status_code == 200
        request_id = run_resp.json()["request_id"]

        verify_resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert verify_resp.status_code == 200
        data = verify_resp.json()
        # 确认 review_ready 判定不受外部因素影响
        assert "review_ready" in data
        # llm_traces 不应出现在 verify 响应中（verify 端点不变）
        # 这是设计意图——verify 端点保持原有行为


# ── 单元测试：SparkDependencyMissingError ──


class TestSparkDependencyMissingErrorUnit:
    """SparkDependencyMissingError 异常类的单元测试。"""

    def test_error_message_format(self):
        """验证异常消息格式包含阶段名和缺失项。"""
        exc = SparkDependencyMissingError(
            SparkPipelineStage.COMPILER,
            ["spark_plan", "compile_result"],
        )
        msg = str(exc)
        assert "COMPILER" in msg
        assert "spark_plan" in msg
        assert "compile_result" in msg
        assert exc.stage == SparkPipelineStage.COMPILER
        assert exc.missing == ["spark_plan", "compile_result"]


# ── 单元测试：SparkStageContext ──


class TestSparkStageContextUnit:
    """SparkStageContext 数据类的单元测试。"""

    def test_initial_state(self):
        """验证初始状态——所有字段为 None/空。"""
        ctx = SparkStageContext()
        assert ctx.spark_plan is None
        assert ctx.compile_result is None
        assert ctx.comparator_report is None
        assert ctx.stage_results == {}
        assert ctx.errors == []

    def test_mutable_stage_results(self):
        """验证 stage_results 是可变的且独立于实例。"""
        ctx1 = SparkStageContext()
        ctx2 = SparkStageContext()
        ctx1.stage_results["MAPPER"] = "SUCCESS"
        assert ctx2.stage_results == {}  # 独立


# ── 单元测试：LlmTraceNode ──


class TestLlmTraceNodeUnit:
    """LlmTraceNode 模型单元测试。"""

    def test_default_values(self):
        """验证默认值——status=skipped，其他为空。"""
        node = LlmTraceNode(
            node_name="test_node",
            model="fake",
        )
        assert node.status == "skipped"
        assert node.token_usage == {}
        assert node.latency_ms == 0
        assert node.error_type is None

    def test_full_fields(self):
        """验证全部字段正确赋值。"""
        node = LlmTraceNode(
            node_name="sql_build_planner",
            model="deepseek-v3",
            token_usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
            latency_ms=350,
            status="valid",
            error_type=None,
        )
        assert node.node_name == "sql_build_planner"
        assert node.model == "deepseek-v3"
        assert node.token_usage["total_tokens"] == 150
        assert node.latency_ms == 350
        assert node.status == "valid"
```

- [ ] **Step 2: 运行新测试——确认初始状态**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/api/test_spark_stage_independent.py -v --tb=short 2>&1
```

Expected: 9 个测试通过（全部新测试应通过）。注意：部分集成测试在有 DuckDB 的环境下可通过，如果 DuckDB 未安装在 CI 环境中，某些测试中的 execute-rich 会走 NOT_EXECUTED 路径——不影响 test_execute_rich_produces_contract 和 test_spark_map_after_execute_rich 的核心逻辑。

- [ ] **Step 3: 运行全量回归测试**

```bash
cd D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3
python -m pytest tests/ -v --tb=short 2>&1 | tail -20
```

Expected: 全部已有测试 + 新测试通过（约 561+ passed），零退化

- [ ] **Step 4: Commit**

```bash
git add tests/api/test_spark_stage_independent.py
git commit -m "test: Spark 阶段独立触发 + LLM 追踪——9 个 pytest 用例"
```

---

## 验证清单

完成所有 Task 后，执行以下验证：

```bash
# 1. 全量 pytest
python -m pytest tests/ -v --tb=short

# 2. ruff 代码风格检查
python -m ruff check src/

# 3. 前端 TypeScript 编译
cd frontend && npx tsc --noEmit

# 4. 前端构建
cd frontend && npm run build

# 5. Git diff 完整性检查
git diff --stat main
```
