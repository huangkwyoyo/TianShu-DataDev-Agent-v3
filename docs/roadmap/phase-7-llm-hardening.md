# Phase 7：真实LLM接入与v1.0硬化

## 目标

在确定性节点、真实双引擎执行和Harness基线稳定后，接入真实LLM Provider，验证质量、成本、失败恢复和安全边界，形成开发审查级v1.0。

## 交付物

- 多Provider LLM Gateway、结构化输出、超时、限流和降级。
- 各角色Prompt/Schema版本管理。
- LangGraph checkpoint和幂等恢复硬化。
- Prompt注入、Schema逃逸、恶意Spark和测试代码安全评测。
- v1.0 Code Review Package与验收报告。

## 质量门

- RequirementIR人工标注准确率达到项目设定阈值。
- SQLPlan Schema和事实源违规拒绝率达到阈值。
- Spark静态安全和真实运行成功率达到阈值。
- `CONSISTENT_SAMPLE`、黄金正确率和Human acceptance分别报告。
- 两轮返工必须提高正确率，而不是只提高一致率。
- token、延迟和成本满足预算。

阈值必须来自Phase 6基线和人工决策，不在本规划中编造固定数字。

## 禁止

- 自动上线、生产部署、生产调度和生产写入。
- LLM直接生成SQL或覆盖Comparator结论。
- 用样本一致率替代黄金正确率和人工接受率。
- 因真实模型接入而放宽Validator、Executor和人审边界。
- 把API Key、生产数据或完整LLM日志写入Review Package和Memory。

## 验收

1. 真实模型不可用时确定性模块和Fake Adapter测试仍可运行。
2. 结构化输出异常、超时、限流和中断可恢复或进入人工审查。
3. 所有Prompt、模型、Schema、代码、快照和环境版本可追溯。
4. 安全攻击和语义差异用例在Harness中有可重复结果。
5. v1.0最终状态是`REVIEW_READY`或`HUMAN_REVIEW`，不是自动上线批准。

---

> Phase 0.5 校正 | 2026-06-22
