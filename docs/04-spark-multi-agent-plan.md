# Spark 多 Agent 计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 架构契约校正版

## 1. 目标

让LLM生成“开发审查级”PySpark DataFrame DSL，同时把自由代码限制在可静态检查、可隔离执行、可复现的纯转换函数中。

## 2. PySpark生成硬契约

唯一业务入口：

```python
from collections.abc import Mapping
from pyspark.sql import DataFrame

def transform(
    inputs: Mapping[str, DataFrame],
    params: TransformParams,
) -> DataFrame:
    """基于已注入输入构造纯DataFrame转换。"""
    ...
```

强制规则：

1. 只读取`inputs`中TransformationContract声明的数据源。
2. 禁止`spark.table()`、`spark.read`和`SparkSession.builder`。
3. 禁止`.write`、`save`、`saveAsTable`、`insertInto`和流式Sink。
4. 禁止文件系统、网络、数据库连接、进程、线程和环境变量访问。
5. 禁止`eval`、`exec`、`compile`、动态导入、反射和任意模块导入。
6. 禁止Python UDF、pandas UDF和任意序列化执行；优先使用PySpark内置函数。
7. 禁止`collect`、`count`、`toPandas`、`foreach`等Action。
8. 返回且仅返回一个DataFrame，不缓存全局状态，不修改输入对象。
9. 表、字段、Join、指标、输出Schema和粒度必须来自TransformationContract。
10. 代码不得查看SQL文本或SQLPlan，以保留实现独立性。

## 3. Artifact契约

SparkCodeArtifact至少包含：

```text
artifact_id
sub_intent_id
transformation_contract_ref
code_ref
code_sha256
entrypoint = transform
model_id
prompt_version
generation_round
allowed_imports[]
declared_inputs[]
expected_output_schema
static_validation_status
```

代码正文落盘，Graph State只保存artifact引用。

## 4. 三个LLM角色

**核心设计原则**：三个LLM角色的价值是加速开发——Developer生成草稿、Reviewer发现质量问题、Tester生成测试。但**真正保证产物可用的是它们身后每一步的确定性Validator和Executor**。LLM建议、机器裁决、人拍板——三者边界不可混淆。

### 4.1 SparkDeveloper

输入：RequirementIR摘要、SubIntent、TransformationContract、源Schema、安全契约、上一轮OptimizationDirective。

输出：SparkCodeArtifact草案和结构化GenerationNotes。

Developer不读取SQL代码、SQLPlan、SQL执行结果或Reviewer内部推理。

### 4.2 SparkReviewer

Reviewer不直接重写最终代码，输出：

```text
ReviewResult
├── findings[]
│   ├── category
│   ├── severity
│   ├── evidence_location
│   └── explanation
├── optimization_directives[]
│   ├── target
│   ├── required_change
│   ├── semantic_invariants[]
│   └── expected_effect
└── recommendation = ACCEPT | REVISE | HUMAN_REVIEW
```

Reviewer重点检查：

- Join条件、声明基数和重复行风险。
- 聚合粒度、非聚合列和指标口径。
- NULL、NaN、日期、时区、Decimal和类型转换。
- 不必要Shuffle、笛卡尔积、宽依赖、数据倾斜和重复计算风险。
- 输出Schema与TransformationContract一致性。

`ACCEPT`不是验证通过，只表示Reviewer未发现需要修订的问题。

### 4.3 SparkTester

输入最终SparkCodeArtifact、TransformationContract和测试快照Schema，输出TestPlan与测试代码artifact。

TestPlan必须声明：输入fixture、预期不变量、边界条件、oracle来源和测试目的。测试代码同样是不可信代码，必须经过独立AST校验、导入白名单和隔离执行。

Tester和测试代码不能自行宣布业务正确；测试结果由确定性Test Runner产生。

## 5. 固定角色顺序

```text
SparkDeveloper
→ Spark Static Validator
→ SparkReviewer
→ REVISE时返回SparkDeveloper
→ Spark Static Validator
→ SparkTester
→ Test Static Validator
→ Deterministic Test Runner
→ Spark Sample Executor
```

任何代码修订后必须从静态校验重新开始。Reviewer不能绕过Developer直接把新代码送入执行器。

## 6. Static Validator

Validator采用Python AST并默认拒绝未知语法。它至少验证：

- 顶层结构只允许白名单import、`TransformParams`和`transform`定义。
- 入口签名、输入参数和返回约束正确。
- 属性和方法调用属于PySpark DataFrame/Functions白名单。
- 禁止名称、模块、Action、Sink和动态执行。
- `inputs[...]`键来自TransformationContract。
- 字段引用来自源Schema。
- 代码大小、AST深度和链式调用长度受限。

Reviewer是质量辅助，Validator才是执行前安全边界。

## 7. Runtime Sandbox

- 使用独立进程或容器运行，不在Agent主进程`exec`代码。
- 使用固定Spark版本和EnvironmentManifest。
- 禁止网络并限制工作目录、CPU、内存、超时和输出大小。
- 只注入冻结快照对应的DataFrame字典。
- Executor负责触发Action并提取ResultSummary；生成代码本身不得触发Action。
- 运行失败产生ExecutionTrace，不自动修改代码。

## 8. 性能审查边界

样本运行不能证明生产性能。Reviewer可以标注Shuffle、Broadcast候选、Skew和缓存风险；系统可以保存`explain`摘要，但只能产生`PERFORMANCE_REVIEW_REQUIRED`，不能据此称为生产性能通过。

## 9. Phase 2验收标准

1. Developer稳定输出符合入口契约的代码artifact。
2. Validator拒绝未注入数据源、Action、Sink、UDF、网络、文件和动态执行。
3. Reviewer输出结构化Finding和Directive，不直接替代最终代码。
4. Tester代码经过与业务代码同等级的安全校验。
5. 合法代码能在真实本地Spark隔离环境读取注入快照并返回DataFrame。
6. 每轮代码、Prompt、模型、契约和执行环境均可追溯。
7. Spark不可用时状态为`NOT_EXECUTED`，不得伪装为运行成功。

---

> Phase 0.5 校正 | 2026-06-22 | Phase 2 实施依据
