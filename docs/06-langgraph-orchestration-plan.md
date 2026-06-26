# Phase 8 LangGraph 编排 — TianShu DataDev Agent v3

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版（占位）

## 1. 当前状态

**等待 Phase 4 退出。** Phase 8 的详细实施规格——包括具体节点实现、路由条件代码、checkpoint 恢复逻辑——必须在 Phase 5-7 各节点作为普通 Python 服务验证稳定后才能确定。

## 2. 前置依赖

- Phase 5-7 全部退出（SparkPlan IR、受控 PySpark DSL、双链验证）
- 所有业务节点可作为普通 Python 函数独立调用和测试

## 3. 定位

LangGraph 只提供有状态编排、并行分支、条件路由、checkpoint、重试预算和人工中断。所有领域逻辑、编译、安全、执行和比较均由可独立调用的普通 Python 服务完成。

## 4. Graph State 最小约束

Graph State 只保存：
- artifact 引用（路径 + SHA-256）
- 哈希值（developer_spec_hash、source_manifest_hash、sql_build_plan_hash 等）
- 状态枚举（NOT_EXECUTED / RUNTIME_PASS / DIFFERENT / CONSISTENT_SAMPLE / REVIEW_READY / HUMAN_REVIEW）
- retry_count、assurance_level、final_status

Graph State **禁止**保存：
- pandas 或 Spark DataFrame
- 完整结果集、完整代码、完整 DeveloperSpec 正文
- LLM 原始响应或无限消息历史
- 生产数据、凭据和任意不可序列化对象

## 5. 业务节点接口

业务服务不接收整个 GraphState，只接收所需的明确输入：

```python
def compile_sql(plan: SqlBuildPlan, manifest: SourceManifest) -> CompilerOutput: ...

def execute_spark(
    code: SparkCodeArtifact,
    snapshot: SnapshotManifest,
    environment: EnvironmentManifest,
) -> ExecutionTrace: ...
```

LangGraph adapter 负责从 State 解析引用、调用服务、持久化输出并返回最小 State 更新。

## 6. checkpoint / retry / 人工中断

- checkpoint 保存 State 和 artifact 索引，不复制 artifact 正文
- 恢复时先校验 artifact 哈希和 EnvironmentManifest，失效则从最近安全节点重跑
- 每次 LLM 调用、编译、执行和比较前后记录幂等 operation_id
- `retry_count` 最多 2，UNKNOWN 直接进入 `HUMAN_REVIEW`
- 人工中断保留完整审计链——恢复后产生新的人工决策 artifact，不覆盖历史 State

## 7. 条件路由

条件边只能读取确定性结构化字段：
- Schema validation status
- Validator status（含 WEAK/NONE 门禁判定）
- Executor status
- Comparator verdict
- retry_count

禁止根据 LLM 自然语言、置信度或 Reviewer 评分直接决定执行、通过或上线。

## 8. 验收标准骨架

1. 普通 Python 服务在不安装 LangGraph 时仍可独立测试
2. State 序列化后不包含 DataFrame、代码正文和结果集
3. SQL/Spark 分支并行且使用同一 snapshot manifest
4. 条件路由只依赖结构化确定性状态
5. retry_count 最多 2，UNKNOWN 直接进入 `HUMAN_REVIEW`
6. checkpoint 恢复不重复提交已完成的副作用
7. 人工中断与恢复保留完整审计链

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | 占位——Phase 4 退出后重写
