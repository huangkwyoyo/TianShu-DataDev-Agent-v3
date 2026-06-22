# Phase 6：Harness与受控Engineering Memory

## 目标

扩展离线评测体系，并从可复现、人工确认的失败案例建立受控Engineering Memory。Harness和Memory均不成为运行时事实源。

## 交付物

- EvalCase Schema和版本化评测集。
- Requirement、SQL、Spark、一致性、返工、成本和人工接受率报告。
- 相关错误与变异检出率评测。
- Engineering Memory候选、人工批准、版本、撤销和适用范围机制。

## 禁止

- 自动把历史项目书或LLM诊断写成长时记忆。
- 建立可写Domain Memory覆盖TianShu事实源。
- 默认把Engineering Memory注入运行时Prompt。
- 让Harness结果改变产品运行状态。
- 引入生产数据库作为Memory依赖。

## 验收

1. Harness可独立运行且产品不import Harness模块。
2. 能区分样本一致率、黄金正确率和人工接受率。
3. Engineering Memory只接受可复现且人工批准的条目。
4. Memory条目有来源、版本、反例、适用范围和撤销记录。
5. Phase 6结束时长期Memory仍可完全关闭而不影响主流程。

---

> Phase 0.5 校正 | 2026-06-22
