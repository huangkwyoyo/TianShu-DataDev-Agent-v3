# Harness 和 Memory 计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

定义 Harness 评测框架和 Memory 存储机制。Harness 负责离线质量评估，Memory 负责结构化信息存储。两者职责严格分离，不交叉。

## 2. Harness 职责

Harness 是一个独立于生产系统的评测框架，用于持续评估系统质量。

### 2.1 Harness 评测项

| # | 评测项 | 说明 | 评测方式 |
|---|--------|------|----------|
| 1 | Prompt 回归 | 各 LLM 角色 Prompt 变更后输出是否稳定 | 对比历史输出记录 |
| 2 | IR 准确率 | RequirementIR 是否准确反映项目书 | 人工标注 + 自动化检查 |
| 3 | SQL 编译黄金测试 | SQLPlan → 编译 → 执行 是否符合预期 | 预置黄金测试集 |
| 4 | Spark 代码质量评测 | 生成的 PySpark 是否符合代码规范 | 静态分析 + 人工评分 |
| 5 | SQL/Spark 一致率 | 双分支交叉验证通过率 | 多轮运行统计 |
| 6 | 返工成功率 | 自动修复后通过交叉验证的比例 | 多轮运行统计 |
| 7 | Token 和延迟统计 | 每轮推理的 Token 消耗和耗时 | 自动统计 |
| 8 | 模型版本对比 | 不同模型或版本的效果对比 | A/B 测试 |

### 2.2 Harness 测试数据

- 测试用例保存在 `harness/evals/` 目录
- 每个用例包含：项目书文本、预期输出、标注信息
- 用例来源：真实历史项目、人工构造、边缘场景

### 2.3 Harness 不能成为生产运行时依赖

- Harness 不随产品部署
- Harness 不参与生产推理流程
- Harness 运行不影响生产状态
- Harness 的数据不用于生产决策

## 3. 三种 Memory 的严格分离

### 3.1 Run Memory（会话内存）

**用途**：记录单次推理运行期间的所有状态和数据。

**内容**：
- 原始项目书
- RequirementIR
- SubIntent 列表
- SQLPlan
- 编译后的 SQL
- PySpark 代码
- 执行结果
- 交叉验证结果
- 差异诊断
- 修复记录
- Code Review Package

**生命周期**：单次推理开始创建，推理结束清除。

**存储位置**：`memory/runs/{session_id}/`

### 3.2 Engineering Memory（工程内存）

**用途**：记录开发调试过程中累积的经验和模式。

**内容**：
- 常见失败模式记录
- Prompt 调优历史
- 编译器修复记录
- 测试失败归因

**生命周期**：长期存储，人工管理。

**存储位置**：`memory/engineering/`

### 3.3 Domain Memory（领域内存）

**用途**：记录业务领域相关的知识。

**内容**：
- 指标定义
- 常用的过滤条件
- 业务规则
- 表关系补充信息

**生命周期**：长期存储，随 Contract 更新而更新。

**存储位置**：`memory/domain/`

### 3.4 三类 Memory 对比

| 特性 | Run Memory | Engineering Memory | Domain Memory |
|------|-----------|-------------------|---------------|
| 自动写入 | 是 | 半自动（人工确认） | 半自动 |
| 自动清除 | 推理结束即清除 | 否 | 否 |
| 用于生产推理 | 是（当前会话） | 否 | 否 |
| 用于 Harness | 可导出 | 可引用 | 可引用 |
| 大小限制 | 严格限制 | 宽松 | 宽松 |

## 4. Memory 的五条禁止事项

| # | 禁止事项 | 原因 |
|---|----------|------|
| 1 | Memory 不得包含 LLM 原始响应 | 原始响应应保存到日志系统，不进入结构化 Memory |
| 2 | Memory 不得包含 Token 统计 | Token 统计属于运行时监控，由 Harness 收集 |
| 3 | Memory 不得用于一次性决策 | 每个推理周期的状态在 Run Memory 中独立存储 |
| 4 | Engineering Memory 不得自动写入生产状态 | Engineering Memory 仅供参考，不直接影响推理 |
| 5 | Domain Memory 不得替代 Contract 引用 | Domain Memory 仅是补充，表结构等事实以 Contract 为准 |

## 5. Harness 输出格式

```
harness/reports/
├── {eval_name}_{YYYYMMDD}_{HHmmss}/
│   ├── report.json          # 结构化评测结果
│   ├── report.md            # 可读报表
│   └── artifacts/           # 附带产物（执行日志、失败案例等）
```

## 6. 测试边界

| 测试类型 | 覆盖内容 |
|----------|----------|
| Harness 独立 | Harness 不依赖生产代码运行 |
| Run Memory 隔离 | 不同 session 互不干扰 |
| Run Memory 清除 | 推理结束后目录被清除 |
| Engineering Memory 写入 | 人工确认后才写入 |
| Domain Memory 引用 | 不引用已删除的 Domain Memory |

## 7. 风险

| 风险 | 缓解 |
|------|------|
| Harness 误报 | 人工审核评测结果，允许调整标注 |
| Memory 膨胀 | Run Memory 限制大小，Engineering/Domain Memory 人工管理 |
| Harness 与生产耦合 | 严格的代码隔离，不共享运行时模块 |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
