# 前端工作台计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 定位

前端是程序员使用的数据开发工作台，不是营销页、生产调度台或在线数据编辑器。Phase 5才实现前端；Phase 1-4通过CLI/API验证核心能力。

## 2. 核心工作流

1. 输入或上传项目书。
2. 预览RequirementIR、事实源匹配和不确定项。
3. 人工确认或退回RequirementIR。
4. 查看SubIntent、TransformationContract和MergePlan。
5. 查看SQLPlan、SQL、Spark和测试artifacts。
6. 查看真实执行、快照、环境和交叉验证状态。
7. 查看DifferenceAnalysis和RepairHistory。
8. 在`HUMAN_REVIEW`节点停止、补充需求或重新运行。
9. 下载Code Review Package。

## 3. 状态展示

UI必须展示精确状态：`DRAFT`、`STATIC_VALIDATED`、`RUNTIME_PASS`、`DIFFERENT`、`UNSUPPORTED_SEMANTICS`、`CONSISTENT_SAMPLE`、`REVIEW_READY`和`HUMAN_REVIEW`。

禁止只用绿色PASS/红色FAIL掩盖未执行、样本一致和人工审查的差别。每个状态提供简短边界说明。

## 4. 页面

- Project Spec：项目书输入、版本和提交。
- Requirement Review：结构化需求、事实源和人工确认。
- Plan：SubIntent、TransformationContract、SQLPlan和MergePlan。
- Artifacts：SQL、Spark、测试、Prompt/模型/代码哈希。
- Execution：DAG、snapshot_id、EnvironmentManifest和ExecutionTrace。
- Comparison：规范化维度、差异行和Comparator结论。
- Repair：诊断、RepairDirective、新旧artifact diff和轮次。
- Review Package：完整性检查和下载。

## 5. 人工操作边界

允许：确认需求、停止、重试、补充事实源变更请求、标记需要修改、下载材料。

禁止：在线绕过Validator执行代码、直接修改Comparator结论、批准上线、配置生产连接和触发生产任务。

## 6. 数据与安全

- API返回artifact摘要和按需分页内容，不返回完整DataFrame。
- 差异样本默认脱敏并限制行数。
- 前端不保存LLM API Key和数据源凭据。
- 所有人工动作产生审计artifact，不直接篡改历史State。

## 7. 验收

1. 项目书到Requirement确认的工作流可用。
2. 能区分所有精确验证状态。
3. 能追溯代码、事实源、快照、环境和返工版本。
4. `HUMAN_REVIEW`可以暂停和恢复。
5. 下载包与后端artifact manifest一致。
6. UI不存在生产部署和在线任意代码执行入口。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 5 实施依据
