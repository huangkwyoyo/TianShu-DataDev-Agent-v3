# Phase 8：Spark-first 编排硬化

> ⚠️ 本文为占位文档。Phase 4 退出后，必须基于 SQL-first v1.0 的真实 Harness 报告、人工接受率和试用反馈重写本文，才能启动本 Phase 的实施。

> 状态：占位——Phase 4 退出后重写
> 前置依赖：Phase 7 SQL/Spark 双链验证

## 当前占位概要

### 目标

1. LangGraph 编排层接入完整 DeveloperSpec-first 链路
2. Graph State 只存 artifact 引用、哈希、状态和摘要
3. 业务节点是可脱离 LangGraph 调用的普通 Python 函数
4. Memory 在 Phase 8 前不参与运行时路由

### Graph State 约束

- 禁止保存 DataFrame、完整结果集、完整代码、完整 DeveloperSpec 正文
- 禁止保存凭据或无限聊天历史
- 条件路由只能读取结构化确定性状态，不能依赖 LLM 自由文本或置信度

### checkpoint / retry / 人工中断

- checkpoint 保存 State 和 artifact 索引，不复制 artifact 正文
- 恢复时先校验 artifact 哈希和 EnvironmentManifest
- 返工上限 2 轮，超限进入 HUMAN_REVIEW
- 人工中断保留完整审计链

### Memory 禁令

- Engineering Memory 在 Phase 8 前不参与运行时
- 写入必须可复现且经人工批准
- 表、字段、Join 和业务口径的事实源是 SourceManifest / SchemaRegistry——不属于可写 Domain Memory

### 验收标准骨架

1. LangGraph interrupt/resume 可复现
2. Spark HarnessVerdict 正确
3. 业务节点可脱离 LangGraph 独立测试

---

> Phase 8 | 占位 | Phase 4 退出后由实施 Prompt 重写
