# LangGraph 编排计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

定义 LangGraph 在系统中的精确使用边界，明确其职责和禁止事项，确保编排层不越界执行业务逻辑。

## 2. LangGraph 的 12 项职责

| # | 职责 | 说明 |
|---|------|------|
| 1 | 节点编排 | 定义节点执行顺序和依赖关系 |
| 2 | 状态传递 | Graph State 在不同节点间传递 |
| 3 | SQL/Spark 并行分支 | 同时调度 SQL 和 PySpark 执行 |
| 4 | 条件路由 | 根据 PASS/FAIL 结果路由到不同路径 |
| 5 | Checkpoint | 周期性保存状态，支持恢复 |
| 6 | retry_count 追踪 | 记录当前重试次数 |
| 7 | 最大返工次数控制 | retry_count >= max_retries 时终止返工 |
| 8 | 人工中断信号 | 侦听外部中断信号 |
| 9 | 执行轨迹记录 | 记录每个节点的输入输出 |
| 10 | 超时控制 | 每个节点有独立超时限制 |
| 11 | 失败处理 | 节点异常时的 Fallback 策略 |
| 12 | 日志和可观测性 | 状态变更日志输出 |

## 3. LangGraph 的 8 项禁止

| # | 禁止事项 | 原因 |
|---|----------|------|
| 1 | 不得拼接 SQL 字符串 | 拼接 SQL 属于编译器职责，不应在编排层处理 |
| 2 | 不得进行 SQL 安全判定 | 安全由 SQLPlan 契约保证，不需运行时判定 |
| 3 | 不得进行 Spark 安全判定 | 安全由 Static Validator 负责 |
| 4 | 不得判断表和字段真实性 | 真实性由 Contract 引用保证 |
| 5 | 不得定义指标计算逻辑 | 指标计算是业务逻辑，不在编排层 |
| 6 | 不得判定结果一致性最终结论 | 最终结论由 Comparator 决定 |
| 7 | 不得自动批准代码 | 代码批准是人工审查环节 |
| 8 | 不得触发自动上线 | 自动上线不在本系统范围 |

## 4. Graph State 字段定义

```python
from typing import TypedDict, Any, Optional
from pandas import DataFrame

class GraphState(TypedDict):
    """LangGraph 全局状态"""
    # --- 输入 ---
    project_doc: str                       # 原始项目书文本
    session_id: str                        # 会话唯一标识

    # --- 需求阶段 ---
    requirement_ir: Optional[dict]         # 结构化需求（RequirementIR）
    sub_intents: Optional[list[dict]]      # SubIntent 列表

    # --- SQL 分支 ---
    sql_plan: Optional[dict]               # SQLPlan
    sql_code: Optional[str]                # 编译后的 SQL 字符串
    sql_result: Optional[Any]              # DuckDB 执行结果（DataFrame）
    sql_execution_success: Optional[bool]  # SQL 执行是否成功

    # --- PySpark 分支 ---
    spark_code: Optional[str]              # PySpark 代码
    spark_review: Optional[list[dict]]     # Reviewer 审查意见
    spark_test_code: Optional[str]         # 测试代码
    spark_result: Optional[Any]            # PySpark 执行结果（DataFrame）
    spark_execution_success: Optional[bool]  # Spark 执行是否成功

    # --- 交叉验证 ---
    comparison_result: Optional[dict]      # 9 个维度比较结果
    comparison_pass: Optional[bool]        # 是否通过比较

    # --- 差异诊断 ---
    diagnosis: Optional[str]               # LLM 差异诊断文本

    # --- 修复 ---
    repair_directive: Optional[dict]       # RepairDirective
    retry_count: int                       # 当前返工次数（从 0 开始）
    max_retries: int                       # 最大返工次数（固定值 2）

    # --- 输出 ---
    final_report: Optional[dict]           # Code Review Package 索引
    final_status: Optional[str]            # PASS / FAIL / HUMAN_REVIEW
```

## 5. 节点定义与执行顺序

```
input_validation → requirement_analysis → sub_intent_splitting
    │
    ├── sql_branch ───────────────────────┐
    │   sql_plan_generation               │
    │   → sql_compilation                 │
    │   → sql_execution                   │
    │                                     │
    ├── spark_branch ─────────────────────┤
    │   spark_development                 │
    │   → spark_review                    │
    │   → spark_test_generation           │
    │   → spark_execution                 │
    │                                     │
    ├── snapshot_build ───────────────────┤
    │   (在 sql 和 spark 执行前完成)      │
    │                                     │
    └── cross_validation ─────────────────┤
        → comparison                      │
        → (PASS) → report_packaging       │
        → (FAIL) → difference_analysis    │
        → repair_planning                 │
        → (retry) → sql_branch + spark_branch  │
        → (max retries) → human_review_marker  │
```

## 6. 所有业务节点是可脱离 LangGraph 单独测试的普通 Python 函数

每个节点的实现都遵循统一签名：

```python
# 所有节点函数的签名
def node_function(state: GraphState) -> dict:
    """
    业务节点函数。
    输入：当前 GraphState
    输出：要更新的字段字典
    """
    # 这里是纯业务逻辑，不依赖 LangGraph
    ...
    return {"field_name": new_value}
```

这意味着：
- 每个节点可以在单元测试中独立测试
- 不需要启动 LangGraph 即可测试业务逻辑
- 节点可以轻松替换或重新排序

## 7. 检查点与恢复

- LangGraph checkpoint 每执行一个节点后保存
- 支持从上次 checkpoint 恢复执行
- checkpoint 存储在本地文件系统（`./checkpoints/{session_id}/`）

## 8. 测试边界

| 测试类型 | 覆盖内容 |
|----------|----------|
| 正常流水线 | 项目书 → Code Review Package 完整路径 |
| 条件路由 | PASS → Package, FAIL → Repair |
| 返工限制 | retry_count 达到 max_retries 后进入 HUMAN_REVIEW |
| 人工中断 | 中断信号 → 当前节点完成后暂停 |
| 节点独立测试 | 每个节点函数单独调用验证 |
| 状态传递 | 验证 state 更新是否正确 |
| 异常恢复 | 节点抛出异常 → Graph 捕获并处理 |

## 9. 风险

| 风险 | 缓解 |
|------|------|
| LangGraph 版本升级导致不兼容 | Lock 依赖版本，抽象接口层 |
| Checkpoint 数据膨胀 | 限制 checkpoint 保留数量，设置大小上限 |
| 状态字段污染 | 每个节点只返回需要更新的字段 |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
