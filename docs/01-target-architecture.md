# 目标架构 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

定义 TianShu DataDev Agent v3 的系统架构。核心设计原则：**PySpark 代码是主产物，SQL 是验证手段**。

- SQL 分支：确定性生成 DuckDB SQL → 产生参考结果 → 用于交叉验证 PySpark 代码的正确性
- PySpark 分支：三个 LLM 角色协作 → 生成达到"开发审查级"的 DataFrame DSL 代码 → 最终交付物
- 双引擎交叉验证：同一快照上比较 SQL 和 PySpark 输出 → 确保 PySpark 逻辑正确

## 2. 双分支架构总览

```
项目书
  │
  ▼
RequirementIR
  │
  ▼
SubIntent 列表
  │
  ├── SQL 分支 ────────────────────────────────┐
  │   SubIntent → SQLPlan → Python 编译器     │
  │   → DuckDB SQL → DuckDB 执行              │
  │                                            │
  ├── PySpark 分支 ────────────────────────────┤
  │   SparkDeveloper → SparkReviewer          │
  │   → SparkTester → PySpark 执行            │
  │                                            │
  ├── 同源 Parquet 快照 ───────────────────────┤
  │   SQL 和 PySpark 使用同一快照              │
  │                                            │
  └── 交叉验证 ────────────────────────────────┘
      9 个确定性维度比较 → LLM 差异诊断
      → 返工 → Code Review Package
```

## 3. 五个 LLM 角色隔离

| 角色 | 职责 | 输入 | 输出 |
|------|------|------|------|
| SparkDeveloper | 将 SubIntent 转为 PySpark DSL | SubIntent | PySpark 代码 |
| SparkReviewer | 审查代码安全、性能、正确性 | PySpark 代码 | Review 意见 |
| SparkTester | 为 PySpark 代码生成测试 | PySpark 代码 | 测试规格 + 测试代码 |
| DifferenceAnalyst | 解释交叉验证差异（不判定） | 差异报告 | 诊断文本 |
| RepairPlanner | 基于诊断制定修复方案 | 诊断文本 | RepairDirective |

**隔离方式**：同一 LLM 模型，不同 Prompt + 不同输出 Schema，不共享上下文窗口。

## 4. LangGraph 编排层

### 4.1 使用边界

LangGraph 仅用于**编排控制流**，包括：

- 节点调度和状态传递
- SQL / Spark 并行分支执行
- 条件路由（PASS → Package / FAIL → Repair）
- Checkpoint 持久化
- retry_count 追踪
- 最大返工次数控制
- 人工中断信号侦听
- 执行轨迹记录

### 4.2 明确禁止 LangGraph 执行

1. 不得在 LangGraph 内拼接 SQL 字符串
2. 不得在 LangGraph 内进行 SQL 安全判定
3. 不得在 LangGraph 内进行 Spark 安全判定
4. 不得在 LangGraph 内判断表和字段的真实性
5. 不得在 LangGraph 内定义指标计算逻辑
6. 不得在 LangGraph 内判定结果一致性最终结论
7. 不得在 LangGraph 内自动批准代码
8. 不得在 LangGraph 内触发自动上线

### 4.3 Graph State 字段定义

```python
class GraphState(TypedDict):
    project_doc: str                    # 原始项目书文本
    requirement_ir: dict                 # 结构化需求
    sub_intents: list[dict]              # SubIntent 列表
    sql_plan: dict | None                # SQL 分支的 SQLPlan
    sql_code: str | None                 # 编译后的 SQL
    sql_result: DataFrame | None         # SQL 执行结果
    spark_code: str | None               # PySpark 代码
    spark_review: list[str] | None       # Reviewer 意见
    spark_test: str | None               # 测试代码
    spark_result: DataFrame | None       # Spark 执行结果
    comparison_result: dict | None       # 比较结果（9 个维度）
    diagnosis: str | None                # LLM 差异诊断
    retry_count: int                     # 当前已返工次数
    max_retries: int                     # 最大返工次数
    final_report: dict | None            # Code Review Package
```

## 5. 同源 Parquet 快照机制

1. **Snapshot Builder** 从 TianShu 数据源读取表数据
2. 输出：Parquet 文件 + schema.json + manifest.yml + SHA-256 校验
3. SQL 分支和 PySpark 分支都使用同一份快照
4. 快照不可变，在同一推理周期内不重建

## 6. Code Review Package 输出目录

```
code_review_package/
├── 01_requirement/          # 项目书和 RequirementIR
├── 02_sub_intent/           # SubIntent 列表
├── 03_sql_plan/             # SQLPlan
├── 04_sql_code/             # 编译后的 SQL
├── 05_spark_code/           # PySpark 代码
├── 06_execution_results/    # 双分支执行结果
├── 07_comparison/           # 交叉验证报告
├── 08_diagnosis/            # LLM 差异诊断
└── 09_summary/              # 汇总报告和审批单
```

## 7. 非功能性目标

| 维度 | 目标 |
|------|------|
| 容错 | 单个节点失败可重试，LangGraph 支持 checkpoint |
| 可观测 | 每个节点有输入输出日志，执行轨迹可回放 |
| 安全性 | Spark 代码通过 Static Validator 安全检查 |
| 确定性 | SQL 编译器输入输出一一对应 |
| 隔离性 | LLM 角色间上下文不共享 |

## 8. 架构边界约束

- 所有模块是可脱离 LangGraph 单独调用的普通 Python 函数
- LLM 调用必须通过统一 Gateway 层（支持 retry、fallback、token 计数）
- 文件 I/O 统一通过 Storage 抽象层（支持本地 / S3 切换）

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
