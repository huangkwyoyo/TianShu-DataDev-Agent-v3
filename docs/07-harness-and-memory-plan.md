# Harness 和 Memory 计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 原则

Harness是离线质量工程系统，应从早期记录基线，但不能成为产品运行时依赖。Memory不是越早越好；在积累真实失败样本和人工确认规则前，长期Memory不得参与自动路由或代码生成事实判断。

## 2. Harness分层

### 2.1 Fast deterministic suite

进入普通CI，覆盖：IR Schema、SQL编译黄金用例、Validator、Normalizer、Comparator、状态路由和少量E2E。

### 2.2 Model evaluation suite

独立运行，覆盖：

- RequirementIR和SubIntent人工标注准确率。
- Spark语法、静态安全和真实运行成功率。
- SQL/Spark `CONSISTENT_SAMPLE`比例。
- 一轮、二轮返工成功率和错误修复率。
- Human acceptance和`REVIEW_READY`接受率。
- Unsupported/refusal准确率。
- Prompt/模型版本、token、延迟和成本。
- 相关错误率：两个分支结果一致但黄金答案错误的比例。
- 测试变异检出率：生成测试能否发现故意植入的错误。

### 2.3 Environment suite

独立Spark/DuckDB环境运行，验证版本、时区、Decimal、NULL、NaN和执行隔离，不放入每次提交的快速CI。

## 3. EvalCase契约

每个评测用例至少包含：

```text
case_id
project_spec_ref
fact_catalog_version
expected_requirement_ir_ref
expected_sub_intents_ref
expected_invariants[]
golden_result_ref
supported_semantics[]
expected_outcome
human_labels
```

禁止只对比LLM全文快照。评测应比较结构化字段、可执行结果和人工标注。

## 4. Run State不是长期Memory

单次运行状态由LangGraph checkpoint和artifact store管理，包含引用和状态，不称为“学习记忆”。运行完成后按保留策略归档或清理。

Run State不得保存完整DataFrame、生产数据、凭据和无限聊天记录。

## 5. Engineering Memory

只有满足以下条件的经验才能进入Engineering Memory：

1. 来源于可复现失败案例。
2. 有人工批准记录。
3. 记录适用范围、反例、来源artifact和失效条件。
4. 版本化并可撤销。

Phase 6前，Engineering Memory只用于Harness分析，不自动注入运行时Prompt。Phase 7若启用检索，必须记录命中条目、版本和对输出的影响，并允许关闭。

## 6. Domain Knowledge边界

指标、表、字段、Join和业务口径不是可学习Memory，而是只读Fact Catalog：

```text
TianShu contracts/meta/database design
→ Fact Catalog Adapter
→ versioned catalog snapshot
→ Requirement/Plan validators
```

禁止建立可半自动写入的`memory/domain`来补充或覆盖事实源。缺失知识必须走TianShu变更流程。

## 7. Prompt与模型版本

每个LLM artifact必须记录role、model_id、provider、prompt_version、schema_version、input artifact hashes、token与延迟。Harness基于这些字段进行可归因对比，不从自由日志猜测版本。

## 8. 数据安全

- Harness用例必须脱敏、版本化并声明来源许可。
- DifferenceAnalyst只读取必要差异摘要和受限样本。
- LLM原始响应进入受限日志，不进入Memory或Code Review Package正文。
- Memory、Harness和日志均不得保存凭据和生产连接信息。

## 9. 阶段安排

| 阶段 | Harness | Memory |
|------|---------|--------|
| Phase 1 | SQL黄金用例与基线格式 | 仅Run State引用 |
| Phase 2 | Spark生成、Validator和运行基线 | 不启用长期Memory |
| Phase 3 | 双引擎一致率和语义矩阵 | 不启用长期Memory |
| Phase 4 | 返工成功率、错误修复率 | 仅收集候选经验 |
| Phase 6 | 扩充模型评测和报告 | 人工批准Engineering Memory |
| Phase 7 | 模型A/B与稳定性门槛 | 可选、可审计检索 |

## 10. 验收标准

1. Harness不被产品运行时import。
2. 模型输出评测不依赖全文快照。
3. 能区分样本一致率、黄金正确率和人工接受率。
4. Domain Knowledge只有一个TianShu事实源。
5. Engineering Memory不能未经人工批准自动写入或影响运行时。
6. 每次检索和Prompt/模型版本均可追溯。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 6/7 实施依据
