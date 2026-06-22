# AGENTS.md — TianShu DataDev Agent v3

> 项目宪法。所有 Agent、LLM 角色和自动化工具必须遵守本文档的边界定义。

---

## 1. 系统角色

TianShu DataDev Agent v3 是 **AI 辅助数据开发工具**。

**核心目标**：生成达到 **"开发审查级"** 的 PySpark 代码——即代码质量足以提交给程序员进行 code review 和上线决策，而非仅作原型参考。

- 接收结构化或自然语言"数据开发项目书"
- 生成 PySpark DataFrame DSL 代码（主产物）、SQL 验证代码、Python 测试、交叉验证报告、差异诊断与返工记录
- 通过 SQL/Spark 双引擎交叉验证确保代码逻辑正确
- 通过 SparkReviewer + SparkTester 双角色确保代码质量达到审查标准
- 最终产物是 **Code Review Package**——包含代码、测试、验证报告和审查摘要
- **不自动上线**，不直接写生产库，不自动部署
- **人是最终代码审查者和上线决策者**

---

## 2. 生成边界（Generation Boundaries）

### SQL 分支

- **LLM 不能直接生成或修改 SQL 字符串**
- LLM 只能输出结构化 `RequirementIR`、`SubIntent` 和 `SQLPlan`
- SQL 必须由 Python 编译器确定性生成（`sql_gen.py`）
- SQL 修复必须修改 `SQLPlan`，再重新编译——禁止直接修改 SQL 文本
- 所有表名、字段名、指标名和 JOIN 路径必须来自 TianShu 事实源（`contracts/`）
- 禁止编造表、字段、指标或 JOIN 路径

### Spark 分支

- LLM（SparkDeveloper）可直接生成 PySpark DataFrame DSL
- 生成的代码属于 **不可信草案**
- 必须经过 SparkReviewer 检查和 Spark Static Validator 安全检查后才能执行
- SparkTester 生成测试代码，不参与业务代码生成

### 通用规则

- **LLM 不能决定 PASS**——PASS 只能由确定性 Comparator 产生
- DifferenceAnalyst 只能解释差异、建议修复方向，不能把不一致标记为 PASS

---

## 3. 执行边界（Execution Boundaries）

- **只读开发环境**——不连接生产库
- **同源固定样本**——SQL 和 Spark 必须读取同一版本的 Parquet 快照
- **Validator 先于 Executor**——任何代码进入样本执行前，必须通过安全检查
- **最多 2 轮自动返工**——超过后强制进入 HUMAN_REVIEW
- 样本执行使用 `LIMIT` 上限，不做全量扫描

---

## 4. 编排边界（Orchestration Boundaries）

- **LangGraph 只是薄编排层**——负责节点编排、条件路由、checkpoint、retry_count
- **所有业务节点必须是可脱离 LangGraph 单独测试的普通 Python 函数**
- LangGraph 不得负责：SQL 拼接、安全判定、字段真实性判断、指标定义、结果一致性最终判定、自动批准代码
- Graph State 只保存结构化状态和 artifact 引用——不保存完整数据集，不无限保存聊天历史

---

## 5. 人审边界（Human Boundary）

- **Agent 不自动上线**——Agent 只能输出 Code Review Package
- **人是最终代码审查者**——上线决定始终在人
- 以下情况强制 HUMAN_REVIEW：
  - 自动返工超过 2 轮
  - 无法确定修复目标（RepairDirective 不明确）
  - 事实源不完整（缺少表定义、JOIN 路径或指标契约）
  - 交叉验证持续不一致
- Agent 只能写入 `PENDING_REVIEW` 状态，不能写入 `APPROVED`

---

## 6. 测试策略（Testing Policy）

- **pytest 只覆盖确定性业务逻辑**：契约、安全边界、关键状态转换、少量端到端黄金路径
- **Prompt 用例和模型组合放到 harness/evals**，不得转成 pytest 参数组合
- v1.0 主测试集目标：80–150 个高价值测试
- 禁止：为每个枚举组合写重复测试、为文档措辞写测试、重复测试 Python 标准库行为、把模型输出全文快照作为单元测试

---

## 7. 失败策略（Failure Policy）

所有产出必须归属以下状态之一：

| 状态 | 含义 | 产生方 |
|------|------|--------|
| `PASS` | 确定性验证通过 | 只能由确定性 Comparator 产生 |
| `FAIL` | 安全检查或执行失败 | Validator / Executor |
| `DIFFERENT` | SQL/Spark 结果不一致 | 确定性 Comparator |
| `RETRY` | 进入返工循环 | LangGraph 编排层 |
| `HUMAN_REVIEW` | 需要人工审查 | 编排层或 RepairPlanner |

- `PASS` 不能由 LLM 产生
- 不一致结果不能被 LLM 覆盖为 PASS
- 返工必须遵循 RepairDirective，不能盲目重试

---

## 8. Memory 分类

1. **Run Memory**：当前项目书执行状态，由 LangGraph checkpoint 管理，执行结束后可丢弃
2. **Engineering Memory**：历史错误模式、修复经验、Prompt 版本和评测结果，存于 `harness/reports/`
3. **Domain Memory**：指标、表字段、JOIN、业务口径——**只能来自 TianShu 事实源**（`contracts/` 或 TianShu 项目的 `docs/warehouse/database_design/`）

禁止：
- 把完整聊天记录无限注入 Prompt
- 把未验证 LLM 结论写成事实
- 把代码执行结果直接沉淀为长期规则
- 在 Memory 中保存凭据和生产数据
- 让 Memory 覆盖 contracts 事实源

---

## 9. 代码规范

- 所有代码注释和 docstring 使用**中文**
- 注释解释"为什么"而非"是什么"
- 函数/类使用简短的中文 docstring 说明用途

---

## 10. 项目文件结构

```
TianShu-DataDev-Agent-v3/
├── AGENTS.md                    # 本文件——项目宪法
├── README.md                    # 项目概述
├── pyproject.toml               # 包配置
├── docs/                        # 规划文档
│   └── roadmap/                 # 分阶段路线图
├── contracts/                   # TianShu 事实源引用
├── src/tianshu_datadev/         # 核心 Python 包
│   ├── ir/                      # IR 数据结构与 Protocol
│   ├── orchestration/           # LangGraph 图定义
│   ├── sql/                     # SQL 编译器
│   ├── spark/                   # Spark 相关
│   ├── execution/               # DuckDB/Spark 执行器
│   ├── validation/              # 验证与交叉验证
│   ├── artifacts/               # Code Review Package 生成
│   └── llm/                     # LLM 角色 Prompt 与 Schema
├── tests/                       # pytest 测试
├── evals/                       # 评估数据
├── harness/                     # 工程评测系统
├── fixtures/                    # 测试 fixture
└── generated/                   # 生成的 Review Package 输出
```

---

## 11. 当前状态

- **Phase**: 0 — 项目骨架搭建
- **分支**: `feature/data-dev-agent-v3-bootstrap`
- **下一阶段**: Phase 1 — 单项目书 SQL 纵向切片
