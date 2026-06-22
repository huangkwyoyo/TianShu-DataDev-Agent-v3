# Phase 5：前端项目书工作台

## 目标

提供项目书输入、Requirement确认、artifact查看、验证差异、返工历史和Review Package下载的工程工作台。

## 交付物

- FastAPI只读/受控操作API。
- 项目书与RequirementIR确认界面。
- SubIntent、SQLPlan、TransformationContract和MergePlan查看器。
- SQL、Spark和测试artifact查看器。
- DAG状态、ExecutionTrace和快照/环境信息。
- 精确验证状态与差异查看器。
- HUMAN_REVIEW暂停/恢复与Review Package下载。

## 禁止

- 在线任意编辑并执行代码。
- 用PASS/FAIL简化全部状态。
- 生产部署、生产连接、生产调度和上线审批。
- 在浏览器保存凭据或完整结果集。

## 验收

1. 用户必须确认RequirementIR后才能进入生成流程。
2. UI正确展示`CONSISTENT_SAMPLE`与`REVIEW_READY`的差别。
3. 只展示受限差异样本，不传输完整DataFrame。
4. 人工停止和恢复保留审计artifact。
5. 下载包与artifact manifest哈希一致。

---

> Phase 0.5 校正 | 2026-06-22
