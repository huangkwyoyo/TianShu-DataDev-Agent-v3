# Phase 4：LangGraph 修复循环

## 目标

构建基于 LangGraph 的自动化修复循环，在双引擎验证（Phase 3）发现结果差异时，由 DifferenceAnalyst 分析差异原因并生成 RepairDirective，触发最多 2 轮返工尝试。若 2 轮后仍未通过验证，则标记为 HUMAN_REVIEW 并挂起。LangGraph 在此作为薄编排层，不包含业务逻辑。

## 输入

- **ValidationReport**（来自 Phase 3）：包含双引擎验证的 9 个维度比较结果和差异明细
- **当前代码版本**：引发差异的 SQL 或 Spark 代码（带版本标记）

## 输出

| 产出 | 说明 |
|------|------|
| **LangGraph 图定义** | 薄编排层，定义节点、边和状态流转逻辑 |
| **DifferenceAnalyst** | 分析 ValidationReport 中的差异，推断根因，输出结构化分析报告 |
| **RepairDirective** | 包含 5 个目标修复方向的指令，指导 Phase 1 或 Phase 2 重新生成 |
| **LangGraph 状态机** | 管理 RepairAttempt 计数、流转状态（PASS / REPAIR / HUMAN_REVIEW） |
| **HumanReviewRequest** | 当修复轮次耗尽时生成的挂起请求，包含完整上下文 |

## 模块职责

- **langgraph_graph/**：定义 LangGraph 编排图
  - 节点：`analyze_difference`、`generate_repair_directive`、`apply_repair`、`re_validate`
  - 边：条件边控制流转（PASS -> 结束；FAIL 且轮次 < 2 -> 重新分析；FAIL 且轮次 >= 2 -> HUMAN_REVIEW）
  - 状态：记录当前轮次（repair_attempt）、差异摘要、当前代码版本
  - LangGraph 仅做流程控制，不包含领域逻辑
- **difference_analyst/**：接收 ValidationReport，分析差异维度的具体表现，推断根因类型：
  - SCHEMA_MISMATCH（字段名或类型不一致）
  - NULL_MISMATCH（空值处理不一致）
  - JOIN_MISMATCH（JOIN 逻辑差异导致行数不同）
  - FILTER_MISMATCH（过滤条件语义差异）
  - AGG_MISMATCH（聚合计算差异）
  - ORDER_MISMATCH（排序结果不一致）
  - UNKNOWN（无法推断根因）
  - DifferenceAnalyst **只输出分析报告**，不决定 PASS 或 FAIL（验证结果由 Phase 3 决定）
- **repair_directive_generator/**：根据 DifferenceAnalyst 的输出生成 RepairDirective，包含 5 个目标：
  1. **修复方向**：明确指定修改哪个引擎的代码（SQL / Spark / 两者）
  2. **差异定位**：指出差异发生的具体代码位置或逻辑片段
  3. **预期修正**：描述期望的正确行为
  4. **约束条件**：修复时必须满足的不变条件（如 Schema 一致、JOIN 路径不变）
  5. **验证要求**：修复后需要重新通过的验证维度清单
- **human_review_handler/**：管理 HUMAN_REVIEW 状态的请求，收集完整上下文（ValidationReport + 修复历史 + 最终代码版本），生成可读的审查报告

## 明确不做什么

- 不自行修改代码（DifferenceAnalyst 只分析，不修改）
- 不包含业务逻辑（LangGraph 只做编排）
- 不决定最终 PASS/FAIL（由验证阶段决定）
- 不涉及前端展示
- 不替代 Phase 1/2 的代码生成能力

## 契约

- **LangGraph 状态契约**：状态对象必须包含 `repair_attempt: int`、`status: Literal["PASS", "REPAIR", "HUMAN_REVIEW"]`、`validation_reports: list[ValidationReport]`
- **RepairDirective 契约**：5 个字段（direction、location、expected_fix、constraints、verification_items）均为必填
- **轮次上限**：`MAX_REPAIR_ATTEMPTS = 2`（常量，可在配置中调整）

## 风险

| 风险 | 缓解措施 |
|------|----------|
| DifferenceAnalyst 根因推断不准确 | 保留 UNKNOWN 类型，该类型直接转向 HUMAN_REVIEW 而非自动修复 |
| 自动修复引入新问题 | LangGraph 限制最多 2 轮，每轮后必须重新验证 |
| LangGraph 自身异常导致无限循环 | 状态机内置最大轮次检查，到达上限自动终止 |
| HUMAN_REVIEW 请求堆积无人处理 | 请求生成格式化的通知信息，包含完整上下文便于快速决策 |

## 验收标准

1. [ ] LangGraph 图定义完成，包含 analyze、generate、apply、re-validate 四个节点和条件边
2. [ ] 状态机正确管理 repair_attempt 计数，2 轮后自动进入 HUMAN_REVIEW
3. [ ] DifferenceAnalyst 能正确推断至少 5 种差异根因类型（SCHEMA、NULL、JOIN、FILTER、AGG）
4. [ ] RepairDirective 包含完整的 5 个目标字段
5. [ ] PASS 状态下直接结束，不触发任何修复
6. [ ] HUMAN_REVIEW 请求包含完整的上下文（ValidationReport + 修复历史 + 最终代码）
7. [ ] DifferenceAnalyst 对 UNKNOWN 类型不自动修复，直接转 HUMAN_REVIEW

## 测试边界

- **测试范围**：LangGraph 状态流转、DifferenceAnalyst 根因推断、RepairDirective 生成、HumanReviewRequest 格式
- **不测试**：实际代码修改效果（由 Phase 1/2 测试覆盖）、前端展示
- **隔离要求**：使用 Mock ValidationReport 和 Mock Phase 1/2 代码生成器，不依赖真实编译和执行
- **异常测试**：ValidationReport 为空、所有维度 PASS、所有维度 FAIL、连续 2 轮修复失败

## 与其他阶段的依赖

- **依赖 Phase 3**：依赖 ValidationReport 作为分析输入
- **依赖 Phase 1/2**：依赖 SQL 编译器和 Spark 代码生成器来执行修复
- **被 Phase 5/7 依赖**：前端需要展示 HUMAN_REVIEW 状态；v1.0 验收需要评估修复循环的通过率

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
