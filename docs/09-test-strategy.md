# 测试策略 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

定义分阶段测试策略，控制测试用例数量增长，避免重蹈 legacy 项目测试膨胀的覆辙。

## 2. 分阶段测试上限

| Phase | 测试用例上限 | 说明 |
|-------|-------------|------|
| Phase 0 | ≤ 10 | 核心模块单元测试 + 1 条端到端黄金路径 |
| Phase 1 | ≤ 30 | 新增 SQL 分支和 Spark 分支测试 |
| Phase 2 | ≤ 50 | 新增交叉验证和返工测试 |
| Phase 3 | ≤ 80 | 新增 Harness 和 Memory 测试 |
| v1.0 | 80-150 | 最终稳定版本 |

### 2.1 上限管理原则

- 每个阶段结束时评估测试用例数量
- 超过上限需要评审：确认用例是否有必要，是否能合并
- 低价值用例（测试 trivial 逻辑、重复覆盖）应删除而非保留

## 3. 测试范围

### 3.1 使用 pytest 测试的领域

| 领域 | 说明 | 用例占比 |
|------|------|----------|
| 确定性业务逻辑 | 编译器、Comparator、Validator、Snapshot Builder | 40% |
| 契约 | 各 IR 类的字段约束、类型校验、序列化/反序列化 | 15% |
| 安全边界 | Static Validator 规则、非法输入处理 | 15% |
| 关键状态转换 | Graph State 更新、条件路由、返工计数 | 10% |
| 端到端黄金路径 | 少量完整流水线测试 | 10% |
| 其他 | 工具函数、异常处理 | 10% |

### 3.2 不在 pytest 中测试的领域

| 领域 | 测试方式 | 原因 |
|------|----------|------|
| Prompt 质量 | Harness / evals | 大规模用例，不适合 CI 中的 pytest |
| LLM 输出稳定性 | Harness / evals | 需要统计分析，不适合确定性断言 |
| 性能 | 独立性能测试 | 需要专门工具和环境 |
| 大规模数据 | 独立测试 | 不适用于单元测试 |

## 4. 大规模 Prompt 用例存放位置

```
harness/evals/
├── prompt_regression/          # Prompt 回归测试用例
├── ir_accuracy/                # IR 准确率评测用例
├── sql_golden/                 # SQL 编译黄金测试用例
├── spark_quality/              # Spark 代码质量评测用例
├── cross_validation/           # 交叉验证一致率评测用例
└── repair_success_rate/        # 返工成功率评测用例
```

每个 evals 目录下的用例是结构化 JSON/YAML 文件，不是 pytest 用例。

## 5. 禁止事项

| # | 禁止事项 | 原因 |
|---|----------|------|
| 1 | 禁止测试 LLM 调用的返回内容 | LLM 输出不确定，无法通过确定性断言验证 |
| 2 | 禁止使用 LLM 作为测试 Orcale | 测试必须确定性，LLM 输出不能作为判断依据 |
| 3 | 禁止测试外部依赖的不可控行为 | TianShu 表结构变更等外部因素不纳入 pytest |
| 4 | 禁止为覆盖率工具写测试 | 覆盖率是参考指标，不追目标值 |
| 5 | 禁止测试私有方法 | 只测试公有接口 |

## 6. 与 Legacy 项目测试膨胀的对比

| 指标 | Legacy (v2) | v3 目标 |
|------|------------|---------|
| 测试用例总数 | 200+ | 80-150 (v1.0) |
| 端到端测试 | 30+ | 10-15 |
| LLM 相关测试 | 40+ | 0（全部移入 Harness） |
| 安全检查测试 | 50+ | 15-20（更精简） |
| CI 运行时间 | 30+ 分钟 | ≤ 15 分钟 |

## 7. 测试用例结构

```python
# 测试文件命名：test_{module_name}.py

# 每个测试函数结构：
def test_{scenario}():
    """简短的中文描述测试场景"""
    # Arrange：准备输入
    # Act：执行被测函数
    # Assert：验证输出

    # 不使用 mock 覆盖 LLM 调用
    # 不使用 mock 覆盖外部数据源（使用测试用 Contract）
```

## 8. 测试数据管理

- 测试用的 Contract 文件存放在 `tests/fixtures/contracts/`
- 测试用的 Parquet 文件存放在 `tests/fixtures/snapshots/`
- 测试数据应该是静态的、版本化的

## 9. 各阶段测试重点

### Phase 0（≤10）

| 模块 | 用例数 | 内容 |
|------|--------|------|
| SQLPlan 字段契约 | 2 | 合法/非法输入 |
| SQL 编译器 | 3 | 基本 SELECT、JOIN、GROUP BY |
| Graph State | 2 | 状态创建、更新 |
| 端到端 | 1 | 完整流水线（使用小样本） |
| 其他 | 2 | 工具函数 |

### Phase 1（新增 ≤20）

| 模块 | 新增用例 | 内容 |
|------|----------|------|
| SQLPlan 生成 | 3 | 各种 SubIntent 到 SQLPlan 的映射 |
| SQL 编译器扩展 | 5 | HAVING、子查询、复杂 JOIN |
| PySpark Static Validator | 5 | 安全规则全覆盖 |
| SparkDeveloper | 2 | SubIntent 到 PySpark 的基本映射 |
| 更多端到端 | 5 | SQL 分支和 Spark 分支独立端到端 |

### Phase 2（新增 ≤20）

| 模块 | 新增用例 | 内容 |
|------|----------|------|
| Comparator | 8 | 9 维度全覆盖 |
| RepairDirective | 4 | 5 个目标的映射逻辑 |
| 返工流程 | 5 | 0/1/2/3 轮逻辑 |
| 条件路由 | 3 | PASS/FAIL/HUMAN_REVIEW |

### Phase 3（新增 ≤30）

| 模块 | 新增用例 | 内容 |
|------|----------|------|
| Code Review Package | 5 | 打包内容完整性 |
| Harness 接口 | 5 | 各评测项接口测试 |
| Memory 管理 | 5 | 读写清除逻辑 |
| 更多端到端 | 10 | 覆盖更多场景 |
| 安全边界 | 5 | 新增的边界 case |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
