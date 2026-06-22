# 产品宪章 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

构建一个 **AI 辅助数据开发工具**，核心目标是生成达到 **"开发审查级"** 的 PySpark DataFrame DSL 代码。

"开发审查级"意味着：生成的代码质量足以提交给程序员进行 code review 和上线决策——包含：
- 结构清晰、遵循最佳实践的 PySpark 代码
- 通过 SQL/Spark 双引擎交叉验证确保逻辑正确
- 附带 Python 测试代码
- 附带完整的执行追踪和验证报告
- 附带供审查者阅读的 Code Review Package

**SQL 是验证手段，不是最终交付物**。DuckDB SQL 用于生成参考结果，与 PySpark 输出交叉验证——确保 PySpark 代码逻辑正确。最终交付的是 PySpark 代码 + 测试 + 审查材料。

**最终产物是可交付的 Code Review Package**，而非直接上线执行。

## 2. 核心流水线

项目书 → 需求分析 → SubIntent 拆分 → SQL / Spark 双分支 → 同源样本执行 → 确定性交叉验证 → LLM 差异诊断 → 返工 → Code Review Package → 人工审查

| 阶段 | 说明 |
|------|------|
| 项目书 | 用户以自然语言描述分析目标 |
| 需求分析 | 将项目书解析为结构化 RequirementIR |
| SubIntent 拆分 | 将需求分解为原子可执行的 SubIntent |
| SQL / Spark 双分支 | 两条独立路径并行生成 SQL 和 PySpark 代码 |
| 同源样本执行 | 基于同一份 Parquet 快照分别执行 |
| 确定性交叉验证 | 比较两路执行结果，判定一致性 |
| LLM 差异诊断 | LLM 仅解释差异原因，不判定 PASS/FAIL |
| 返工 | 根据诊断结果自动修改 SQLPlan 或 Spark 代码 |
| Code Review Package | 打包代码、执行结果、验证报告供人工审查 |
| 人工审查 | 工程师确认后可选上线 |

## 3. 核心约束

1. **LLM 不直接生成 SQL 字符串** — SQL 必须由 Python 确定性编译器从 SQLPlan 生成
2. **LLM 不能决定 PASS** — 交叉验证 PASS 只能由确定性 Comparator 给出
3. **SQL 必须由 Python 编译器确定性生成** — 同一 SQLPlan 始终生成同一 SQL
4. **所有代码必须经过人工审查** 才能进入生产
5. **表名、字段名、Join 关系必须来自 TianShu 事实源**，不允许 LLM 猜测
6. **同源数据快照** — SQL 和 PySpark 使用同一份 Parquet 数据

## 4. 模块职责

| 模块 | 职责 |
|------|------|
| 需求分析器 | 解析项目书 → RequirementIR |
| SubIntent 拆分器 | 拆分需求 → SubIntent 列表 |
| SQLPlan 生成器 (LLM) | SubIntent → SQLPlan |
| Python SQL 编译器 | SQLPlan → DuckDB SQL |
| Spark Developer (LLM) | SubIntent → PySpark DSL |
| Spark Reviewer (LLM) | 审查 PySpark 代码 |
| Spark Tester (LLM) | 生成测试规格和测试代码 |
| Snapshot Builder | 构建同源 Parquet 快照 |
| SQL Executor | 在 DuckDB 上执行 SQL |
| Spark Executor | 在 PySpark 上执行代码 |
| Comparator | 确定性比较两路执行结果 |
| Difference Analyst (LLM) | 解释差异原因（不判定 PASS） |
| Repair Planner (LLM) | 根据诊断生成修复计划 |
| Report Packager | 打包 Code Review Package |
| LangGraph 编排层 | 编排上述所有模块 |

## 5. 明确不做什么

- 不自动上线代码
- 不直接写入生产数据库
- 不生成生产级数据
- 不替代人工代码审查
- 不管理用户权限和认证
- 不提供生产调度能力
- 不覆盖数据质量监控
- 不做数据血缘追踪

## 6. 外部依赖

- **TianShu 数据仓库**：表结构、字段定义、表关系等事实源信息
- **Parquet 文件存储**：用于构建同源快照
- **DuckDB**：本地 SQL 执行引擎
- **PySpark (本地模式)**：本地 Spark 代码执行

## 7. 风险

| 风险 | 缓解措施 |
|------|----------|
| LLM 生成不合规的 SQLPlan | SQLPlan 字段契约严格约束，编译器拒绝非法输入 |
| 双分支执行结果不一致 | 确定性比较 + LLM 差异诊断 + 返工机制 |
| 测试覆盖不足 | Harness 持续评估质量指标 |
| 状态累积和 "遗忘" | Run Memory 结构化存储，每轮重置 |
| 用户拒绝所有自动生成代码 | 系统保证人工审查是最终环节 |

## 8. 验收标准

1. Phase 0：流水线框架搭建完成，端到端核心路径走通
2. Phase 1：SQL 分支可完整运行，PySpark 分支可完整运行
3. Phase 2：交叉验证 + 返工机制可用
4. Phase 3：Code Review Package 完整可用，Harness 体系就绪
5. v1.0：全功能可用，覆盖 80-150 个测试用例

## 9. 不支持的功能（Phases 之外）

- Web UI 仪表盘（Phase 0 不实现）
- 实时流数据处理
- 多用户协作
- 生产调度集成

## 10. 与现有项目的边界

- 本项目的 **Contract 目录** 可引用 TianShu 的 `contracts/*.yml`
- 本项目的 **架构和设计** 吸取了三个 legacy 项目的经验教训
- 本项目的 **代码** 不直接复用 legacy 项目，仅参考算法

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
