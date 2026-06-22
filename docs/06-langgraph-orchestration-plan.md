# LangGraph 编排计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 定位

LangGraph只提供有状态编排、并行分支、条件路由、checkpoint、重试预算和人工中断。所有领域逻辑、编译、安全、执行和比较均由可独立调用的普通Python服务完成。

Phase 1-3先实现并验证普通Python节点；Phase 4才接入LangGraph图。不得为了框架提前改变领域接口。

## 2. 图结构

```text
validate_project_spec
→ analyze_requirement
→ validate_requirement
→ requirement_confirmation_gate
→ decompose_sub_intents
→ build_transformation_contracts
→ build_relational_snapshot
→ fan_out_sub_intents
    ├─ plan_sql → compile_sql → validate_sql → execute_duckdb
    └─ develop_spark → validate_spark → review_spark
         → revise_spark → validate_spark → generate_tests
         → validate_tests → run_tests → execute_spark
→ normalize_results
→ compare_results
    ├─ CONSISTENT_SAMPLE → build_review_package
    ├─ DIFFERENT → classify_difference → analyze_difference
    │    → plan_repair → route_repair → retry / HUMAN_REVIEW
    └─ NOT_EXECUTED / UNSUPPORTED → HUMAN_REVIEW
```

## 3. Artifact优先State

```python
class GraphState(TypedDict):
    request_id: str
    run_id: str
    project_spec_ref: str
    requirement_ir_ref: str | None
    requirement_confirmed: bool
    sub_intent_refs: list[str]
    transformation_contract_refs: list[str]
    merge_plan_ref: str | None
    snapshot_manifest_ref: str | None
    sql_plan_refs: list[str]
    sql_artifact_refs: list[str]
    spark_artifact_refs: list[str]
    test_artifact_refs: list[str]
    execution_trace_refs: list[str]
    result_summary_refs: list[str]
    comparison_report_ref: str | None
    diagnosis_ref: str | None
    repair_directive_ref: str | None
    repair_history_refs: list[str]
    retry_count: int
    max_retries: int
    assurance_level: str
    final_status: str
    human_review_reasons: list[str]
```

State禁止包含：

- pandas或Spark DataFrame。
- 完整结果集、完整代码和完整项目书正文。
- LLM原始响应或无限消息历史。
- 生产数据、凭据和任意不可序列化对象。

## 4. 节点接口

业务服务不接收整个GraphState，只接收所需的明确输入：

```python
def compile_sql(plan: SQLPlan, catalog: FactCatalog) -> SQLArtifact:
    ...

def execute_spark(
    code: SparkCodeArtifact,
    snapshot: RelationalSnapshotManifest,
    environment: EnvironmentManifest,
) -> ExecutionTrace:
    ...
```

LangGraph adapter负责从State解析引用、调用服务、持久化输出并返回最小State更新。这样业务模块可脱离LangGraph测试和复用。

## 5. 路由事实源

条件边只能读取确定性字段：

- Schema validation status。
- Validator status。
- Executor status。
- Comparator verdict。
- retry_count。
- requirement_confirmed。
- RepairDirective target。

禁止根据LLM自然语言、置信度或Reviewer评分直接决定执行、通过或上线。

## 6. 并发与SubIntent

- SubIntent可并发处理，但每个SubIntent使用独立artifact命名空间。
- Snapshot必须在分支执行前完成并冻结。
- fan-in前验证所有必需SubIntent均产生结果。
- MergePlan在合并前确定性校验粒度、键和基数。
- 任一必需SubIntent失败时，不得把部分结果包装成完整成功。

## 7. Checkpoint与恢复

- checkpoint保存State和artifact索引，不复制artifact正文。
- 每次LLM调用、编译、执行和比较前后记录幂等operation_id。
- 恢复时先校验artifact哈希和EnvironmentManifest，失效则从最近安全节点重跑。
- Executor、LLM调用和Storage写入必须设计为可重试或显式不可重试。
- checkpoint设大小、数量和保留期限上限。

## 8. 人工中断

以下节点可触发interrupt：

- RequirementIR确认。
- 事实源或Join不完整。
- 不支持语义。
- 两轮返工仍不一致。
- 测试代码或Spark代码被安全Validator拒绝。
- Spark运行环境不可用。

恢复后产生新的人工决策artifact，禁止直接修改历史State掩盖原结论。

## 9. 可观测性

每个节点记录：node_id、operation_id、输入artifact哈希、输出artifact哈希、开始/结束时间、模型和Prompt版本、token/延迟、状态与错误分类。日志不能替代ExecutionTrace或Harness评测数据。

## 10. Phase 4验收标准

1. 普通Python服务在不安装LangGraph时仍可独立测试。
2. State序列化后不包含DataFrame、代码正文和结果集。
3. SQL/Spark分支并行且使用同一snapshot manifest。
4. 条件路由只依赖结构化确定性状态。
5. retry_count最多2，UNKNOWN直接进入`HUMAN_REVIEW`。
6. checkpoint恢复不重复提交已完成的副作用。
7. 人工中断与恢复保留完整审计链。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 4 实施依据
