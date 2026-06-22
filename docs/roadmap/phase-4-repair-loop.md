# Phase 4：LangGraph差异诊断与有限返工

## 目标

在Phase 1-3普通Python节点稳定后，用LangGraph薄编排层串联条件路由、checkpoint、人工中断和最多2轮返工。

## 输入

- ComparisonReport。
- SQLPlan、SparkCodeArtifact和TransformationContract引用。
- ExecutionTrace摘要。
- retry_count和artifact哈希。

## 交付物

1. Artifact优先GraphState和Storage Adapter。
2. DifferenceClassifier确定性差异分类。
3. DifferenceAnalyst Fake/LLM Adapter及结构化Diagnosis。
4. RepairPlanner及RepairDirective。
5. SQL_PLAN、SPARK_CODE、BOTH、REQUIREMENT、HUMAN_REVIEW条件路由。
6. Checkpoint、幂等operation_id、恢复和人工interrupt。
7. repair_history artifacts。

## 路由规则

- `CONSISTENT_SAMPLE`且材料完整 → `REVIEW_READY`。
- `DIFFERENT`且retry_count < 2 → DifferenceAnalyst与RepairPlanner。
- `SQL_PLAN` → 新SQLPlan，重新编译、验证和执行。
- `SPARK_CODE` → Developer修订，重新走Reviewer/Tester/Validator/Executor。
- `REQUIREMENT`、`UNKNOWN`、事实源缺失或retry_count达到2 → `HUMAN_REVIEW`。
- `NOT_EXECUTED`和`UNSUPPORTED_SEMANTICS`不得自动返工为成功。

## 禁止

- LangGraph节点内部实现SQL编译、安全检查或Comparator。
- State保存DataFrame、代码正文、结果集或原始LLM响应。
- LLM修改Comparator状态或延长重试预算。
- SQL返工直接修改SQL文本。
- Reviewer直接绕过Developer替换最终Spark代码。

## 验收

1. 业务服务可脱离LangGraph测试。
2. State只包含引用、哈希、状态和小型摘要。
3. 0、1、2轮返工与超限人工审查路由正确。
4. checkpoint恢复不重复已完成的执行副作用。
5. 每轮新旧artifact、Prompt、模型和诊断可追溯。
6. 累计pytest目标80-105个。

## 后续依赖

Phase 5前端展示结构化状态和artifact；Phase 6 Harness统计返工效果；Phase 7才接真实LLM并设置质量门。

---

> Phase 0.5 校正 | 2026-06-22
