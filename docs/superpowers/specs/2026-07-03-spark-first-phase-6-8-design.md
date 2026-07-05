# Spark-first Phase 6-8 完整设计方案

> 状态：已完成（Phase 6-8 全部实施通过）| 日期：2026-07-03
> 前置：Phase 5（SparkPlan IR + mapper.py + plan_equivalence.py）已完成，45 个测试通过

---

## 0. 总体架构与核心边界

### 0.1 数据流

```
                     DataTransformContractV1（SQL/Spark 唯一共享契约）
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
   mapper.py（确定性）  SQL Compiler（已有）  Snapshot Builder
         │                  │                  │
         ▼                  ▼                  ▼
   SparkPlan (baseline)  DuckDB SQL（基准）  immutable.parquet（不可变快照）
         │                                     │
         ▼                               ┌─────┴─────┐
   SparkDeveloper（LLM 标注）            ▼           ▼
         │                          DuckDB Exec   Spark Exec
         ▼                               │           │
   AnnotatedSparkPlan                    └─────┬─────┘
         │                                     ▼
         ▼                              ResultComparator
   SparkCompiler（确定性代码生成）       （物理链路）
         │
         ▼
   Static Validator（硬门禁）
         │
    ┌────┴────┐
    ▼         ▼
PlanComparator  PhysicalVerifier
（逻辑链路）     （物理链路）
    │         │
    └────┬────┘
         ▼
UnifiedVerificationReport
         │
         ▼
REVIEW_READY / REPAIR_NEEDED / HUMAN_REVIEW
         │
         ▼
Spark Review Package
```

### 0.2 核心硬边界（C 类，不可违反）

| # | 硬边界 | 说明 |
|---|--------|------|
| 1 | **SparkDeveloper / SparkCompiler / SparkPlan 生成链路只读 DataTransformContractV1 和 baseline SparkPlan** | Phase 7 验证层可读取 SqlBuildPlan 的结构化 artifact 用于对比，但不得把 SQL 文本提供给 SparkDeveloper 或 SparkCompiler |
| 2 | **mapper.py 是唯一 Contract → SparkPlan 路径** | 不允许任何旁路直接生成 SparkPlan 结构 |
| 3 | **SparkDeveloper（LLM）只做标注** | 不增删改 step、不修改字段/条件/join/聚合/窗口/排序 |
| 4 | **删除 annotation 后执行代码完全等价** | `compile_raw(baseline) == compile_raw(annotated.baseline)` |
| 5 | **Compiled code 不含 SQL 文本** | 注释块 5 行固定格式，跨引擎对照放 Review Package 的 CrossReference 区域 |
| 6 | **PySpark DSL 只接受 `inputs[source_name]`** | 禁止 `spark.read`、`spark.table`、SparkSession 自行创建 |
| 7 | **Snapshot Builder 白名单数据源** | 只能读 SnapshotSourceProvider 显式配置的 dev/test/fixture 数据源 |
| 8 | **RepairPlanner 不直接修改任何 Plan** | 只输出 RepairAction；SparkPlan 修复回 mapper.py；业务语义问题回 SQL-first Planner 或 HUMAN_REVIEW |
| 9 | **NOT_EXECUTED 禁止泛化 PASS** | 未覆盖的 step 类型必须明确标记 |

### 0.3 Orchestrator LLM 调用边界

```
SparkOrchestrator：
  - 不直接访问模型（ProviderAdapter / LLM Client）
  - 不直接构造 Prompt
  - 不解析 LLM 自由文本输出
  - 可以调用已封装、结构化输出、带 AnnotationValidator 的 SparkDeveloperService
  - 只接收通过 AnnotationValidator 校验的 AnnotatedSparkPlan
  - SparkDeveloper 调用失败 → HUMAN_REVIEW，不重试
```

### 0.4 AnnotationWarning 传播规则

```
AnnotationWarning 只进入：
  - Review Package（annotation_warnings 区域）
  - UnifiedVerificationReport（携带但不影响 verdict）
  - Harness（Spark 评测维度）

AnnotationWarning 不得：
  - 直接改变 SparkPlan 结构
  - 改变 Compiler 输出
  - 改变 PlanComparator 结论
  - 改变 PhysicalVerifier 结论
  - 触发自动返工
```

---

## 1. Phase 6：受控 PySpark DSL 生成（难度分组迭代）

### 1.1 新增模型：`annotations.py`

```python
# spark/annotations.py

class StepIntent(str, Enum):
    """步骤意图分类。"""
    SOURCE = "source"        # 数据读取
    CLEAN = "clean"          # 数据清洗/过滤
    RELATE = "relate"        # 表关联
    SUMMARIZE = "summarize"  # 聚合汇总
    LABEL = "label"          # 分类打标 (CASE WHEN)
    RANK = "rank"            # 窗口排名
    SHAPE = "shape"          # 投影/排序/截断（最终整形）


class StepAnnotation(StrictModel):
    """单个 step 的语义标注——不修改 SparkPlan step 的任何字段。

    step_id 为主键（对应 baseline.steps[i].step_id），step_index 仅为展示字段。
    """

    step_id: str                     # 主键——对应 baseline.steps[i].step_id
    step_index: int                  # 展示字段——由构建时自动填充
    step_type: str                   # 冗余校验
    intent: StepIntent               # 意图分类
    intent_detail: str               # 中文业务意图描述（≤120 字）
    operation_summary: str           # 中文操作简述
    downstream_step_ids: list[str] = Field(default_factory=list)  # 下游消费者 step_id
    review_flags: list[str] = Field(default_factory=list)         # 疑点标签


class AnnotationWarning(StrictModel):
    """SparkDeveloper 发现的语义疑点——只能进入 Review/Repair/Harness。

    禁止：直接修改 SparkPlan、Compiler 输出、Comparator 结论。
    """

    warning_id: str
    step_id: str | None = None       # 关联的 step_id，可为 None（全局疑点）
    severity: Literal["INFO", "WARN", "REVIEW"] = "WARN"
    category: str                    # "semantic_mismatch" / "missing_filter" / "ambiguous_join" / ...
    description: str
    suggestion: str | None = None


class AnnotatedSparkPlan(StrictModel):
    """标注后的 SparkPlan——baseline SparkPlan + 标注层。

    约束：
    1. annotations 数量 == baseline.steps 数量（一一对应）
    2. 删除全部 annotations 后，Compiler 产出等价代码
    3. annotations 不参与 SparkPlan.compute_plan_hash()
    """

    plan_id: str
    baseline: SparkPlan               # mapper.py 产出的原始 SparkPlan（只读）
    annotations: list[StepAnnotation] # 每个 step 一条标注
    warnings: list[AnnotationWarning] # LLM 发现的疑点（不进执行路径）
    annotator_version: str = "v1"
    annotation_hash: str              # 标注层确定性 hash


class AnnotationValidator:
    """确定性标注校验器——检查 LLM 标注是否违反边界约束。

    规则：
    - annotation 数量 != steps 数量 → VALIDATION_ERROR（阻断编译）
    - step_id 不在 baseline 中 → VALIDATION_ERROR
    - step_id 重复 → VALIDATION_ERROR
    - REVIEW 级别 warning → 标记 HumanReviewSuggested（不阻断）
    """
```

### 1.2 annotation_hash 规范

```python
@staticmethod
def compute_annotation_hash(annotated: AnnotatedSparkPlan) -> str:
    """计算标注层确定性 SHA-256。

    包含：annotations(按 step_id 排序)、warnings(按 warning_id 排序)、
          annotator_version、baseline.plan_id
    不包含：时间戳、step_index（展示字段）、baseline 内部结构
    """
    data = {
        "plan_id": annotated.baseline.plan_id,
        "annotator_version": annotated.annotator_version,
        "annotations": sorted(
            [a.model_dump(exclude={"step_index"}) for a in annotated.annotations],
            key=lambda a: a["step_id"],
        ),
        "warnings": sorted(
            [w.model_dump() for w in annotated.warnings],
            key=lambda w: w["warning_id"],
        ),
    }
    content = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()
```

### 1.3 SparkDeveloper 协议

```
输入（只读）：
  - DataTransformContractV1      # 业务语义上下文
  - SparkPlan（mapper.py 产出）  # baseline 结构

禁止访问：
  - ParsedDeveloperSpec          # 不能独立读项目书
  - SqlBuildPlan                 # 不能看 SQL Plan
  - SQL 文本                     # 不能看生成的 SQL

输出：
  - list[StepAnnotation]         # 每个 step 一条标注（数量 == steps 数量）
  - list[AnnotationWarning]      # 疑点列表

行为约束：
  - 不得增删 step、修改字段、条件、join、聚合、窗口、排序
  - 发现语义疑点 → 输出 AnnotationWarning，不改 Plan
  - Prompt 包含 JSON Schema 做 StructuredOutput 强制约束
```

### 1.4 SparkCompiler：确定性 PySpark DSL 生成

参照 SQL Compiler 的 `_compile_core()` + 注释渲染分层架构。

```python
# spark/compiler.py

@dataclass(frozen=True)
class SparkCompileResult:
    raw_pyspark: str          # 无注释的纯 PySpark DSL 代码（执行以 raw 为准）
    annotated_pyspark: str    # 带结构化注释的代码（仅供人审展示）
    raw_hash: str             # raw_pyspark 的 SHA-256


class SparkCompiler:
    """确定性 PySpark DSL 编译器——SparkPlan → PySpark 代码。

    不访问：DeveloperSpec、SqlBuildPlan、SQL 文本、LLM。
    所有代码片段通过 SparkCodeRenderer 生成——禁止直接 f-string 拼接。

    生成代码固定入口：
        def transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame:
    Phase 6A 可以不使用 params，但接口必须保留。
    """

    COMPILER_VERSION = "1.0.0"
```

**9 种 step → PySpark 代码生成规则**（经过 Renderer 安全渲染）：

| Step 类型 | PySpark 代码模板 |
|-----------|-----------------|
| `SparkReadStep` | `{alias} = inputs["{source_name}"]` |
| `SparkFilterStep` | `{out} = {input}.filter(F.col("{left}") {op} {right})` |
| `SparkJoinStep` | `{out} = {left}.join({right}, on=[...], how="{type}")` |
| `SparkAggregateStep` | `{out} = {input}.groupBy(*[...]).agg(*[...])` |
| `SparkProjectStep` | `{out} = {input}.select(...)` |
| `SparkCaseWhenStep` | `{out} = {input}.withColumn("{alias}", F.when(...).otherwise(...))` |
| `SparkWindowStep` | `{out} = {input}.withColumn("{alias}", F.{func}().over(window_spec))` |
| `SparkSortStep` | `{out} = {input}.orderBy(*[...])` |
| `SparkLimitStep` | `{out} = {input}.limit({n})` |

**关键约束**：
- `SparkReadStep` 生成 `inputs["{source_name}"]`——不允许 `spark.read.parquet()`
- `source_name` 是 inputs dict 的 key，`alias` 是代码变量名，二者可相同但不混用
- 物理路径在 `SnapshotManifest`，不在 `SparkPlan` / `SparkReadStep`
- `SparkReadStep` 只保留逻辑字段：`alias`、`source_name`、`input_key`

### 1.5 SparkCodeRenderer：安全渲染规则

```python
# spark/renderer.py

class SparkCodeRenderer:
    """PySpark DSL 安全渲染器——所有代码片段必须通过本渲染器生成。

    规则：
    1. 变量名——必须匹配 SafeIdentifier 正则（字母开头，字母数字下划线）
    2. 列名——来自封闭模型字段（ColumnRef.column_name），经反引号转义
    3. 字面量——按类型渲染（str → 单引号包围并转义，int/float → 直接渲染）
    4. 函数名——来自 SparkAggFunction / SparkWindowFunction 枚举
    5. Join how——来自 SparkJoinType 枚举
    6. Sort direction——来自 SparkSortDirection 枚举
    7. 禁止直接拼接表达式字符串
    """

    _SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    # 所有 render_* 方法都来自封闭模型或白名单
    # 所有 render_* 方法都有单元测试和恶意输入拒绝测试
```

### 1.6 结构化注释格式（5 行固定）

```python
# Step: daily_user_agg_temp（索引 3/7）
# Intent: 生成用户日粒度中间结果，供后续月度汇总使用。下游消费者：monthly_user_agg_temp
# Operation: 从 dwd_user_order 扫描 5 个字段，按 user_id, dt 分组，聚合 2 个指标
# Inputs: dwd_user_order
# Output: daily_user_agg_temp

daily_user_agg_temp = (
    dwd_user_order
    .groupBy("user_id", "dt")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("pay_amount").alias("pay_amount"),
    )
)
```

**约束**：
- 5 行固定格式：Step / Intent / Operation / Inputs / Output
- **不含 SQL 文本**（等价 SQL 不放注释，放 Review Package 的 CrossReference 区域）
- 注释安全清洗（控制字符、`--`、换行）通过 `_render_comment_line()` 统一处理
- 注释块与代码之间空一行

### 1.7 Static Validator

```python
# spark/validator.py

class SparkValidationError(StrictModel):
    error_code: str          # "E601" / "E602" / ...
    line_number: int
    category: str            # "FORBIDDEN_API" / "UNSAFE_IMPORT" / ...
    detail: str
    suggestion: str | None


class SparkStaticValidator:
    """Phase 6 Static Validator——AST 白名单 + 语义约束。

    基于 AST call-chain 分类（不是简单的字符串匹配）：
    - F.count(...) → 允许（聚合函数）
    - df.count()  → 禁止（DataFrame action）
    - Window.orderBy(...) → 允许
    预留 ExecutionSafetyProbe 接口（Phase 7 接入）。
    """
```

**错误码体系**：

| 错误码 | 类别 | 示例 |
|--------|------|------|
| E601 | FORBIDDEN_API | `spark.table(...)` 被检测到 |
| E602 | UNSAFE_IMPORT | `import subprocess` 被检测到 |
| E603 | ACTION_NOT_ALLOWED | `.collect()` 被检测到 |
| E604 | SINK_NOT_ALLOWED | `.write.parquet(...)` 被检测到 |
| E605 | UDF_NOT_ALLOWED | `@udf` 装饰器被检测到 |
| E606 | RAW_EXPRESSION | `F.expr("1=1")` 被检测到 |
| E607 | UNKNOWN_FUNCTION | 不在白名单内的函数调用 |
| E608 | DYNAMIC_EXEC | `eval()` / `exec()` 被检测到 |

### 1.8 Phase 6A/6B/6C 边界

```
Phase 6A（scan/filter/project/sort/limit）：
  - Compiler 实现 5 种 step 编译
  - SparkDeveloper 实现对应 5 种标注
  - Validator 全量规则定义，6A 只测 5 种 AST
  - 测试：编译 + 注释 + 校验 + Renderer 安全测试

Phase 6B（aggregate/join/case_when）：
  - Compiler 增加 3 种
  - 测试追加

Phase 6C（window）：
  - Compiler 增加 window + 帧边界校验
  - 测试追加
```

**skip/xfail 策略**：6B/6C 未实现测试用 skip/xfail 占位，必须标记 phase；不计入 Phase 6A 完成度。进入对应 Phase 后转为普通断言测试。Phase 6A Exit 只统计 6A 范围测试。

---

## 2. Phase 7：SQL/Spark 双链验证

### 2.1 Snapshot Builder

```python
# spark/snapshot.py

class SnapshotSourceProvider(StrictModel):
    """快照数据源配置——Snapshot Builder 只能读取此清单内的数据源。"""

    provider_id: str
    source_type: Literal["local_fixture", "dev_warehouse", "test_dataset"]
    connection_alias: str            # 受控连接别名（不是真实凭据）
    allowlisted_tables: list[str]    # 完全限定表名精确匹配，禁止通配/正则/模糊


class SamplingSpec(StrictModel):
    """采样策略——确保同一 Contract 可回归复现。"""

    mode: Literal["full", "random", "stratified", "head"]
    limit: int | None = None
    seed: int | None = None
    strata_keys: list[str] = Field(default_factory=list)
    anchor_keys: list[str] = Field(default_factory=list)


class SnapshotFile(StrictModel):
    source_name: str                # inputs key
    file_path: str
    format: str = "parquet"
    row_count: int
    file_sha256: str


class SnapshotManifest(StrictModel):
    """不可变快照清单。快照 ID 确定性生成，created_at 仅元数据。"""

    snapshot_id: str                # sha256(contract_hash + source_manifest_hash + sampling_spec + env_fingerprint)
    contract_hash: str
    created_at: str                 # ISO 8601，仅元数据，不参与 snapshot_id 计算
    snapshot_dir: str
    files: list[SnapshotFile]
    snapshot_sha256: str
    source_provider_id: str
    source_type: str                # "local_fixture" / "dev_warehouse"
    sampling_spec: SamplingSpec
    deidentification: str | None    # "none" / "masked_pii" / "synthetic"


class SnapshotBuilder:
    """从 Contract 生成不可变 Parquet 快照。

    安全边界：
    - 只能读 SnapshotSourceProvider 白名单内的数据源
    - 禁止从 Contract 字段直接推导生产连接
    - Manifest 记录完整数据源追溯链
    - 快照目录生成后为只读
    - DuckDB 和 Spark 都从同一目录读取
    """
```

### 2.2 PlanComparator：逻辑链路

封装 Phase 5 `plan_equivalence.py` 的 9 条对比规则和 `compare_plans()` 入口。

```python
# spark/plan_comparator.py

class ComparisonStatus(str, Enum):
    LOGIC_EQUIVALENT = "LOGIC_EQUIVALENT"       # SQL ↔ Spark 结构完全等价
    LOGIC_MISMATCH = "LOGIC_MISMATCH"           # 结构不等价
    LOGIC_UNSUPPORTED = "LOGIC_UNSUPPORTED"     # 存在不支持对比的 step 类型
    NOT_EXECUTED = "NOT_EXECUTED"               # 尚未执行对比


class PlanComparisonReport(StrictModel):
    report_id: str
    contract_hash: str
    sql_plan_hash: str
    spark_plan_hash: str
    status: ComparisonStatus
    step_results: list[StepEquivalenceResult]   # 复用 Phase 5 模型
    unsupported_types: list[str]
    uncovered_step_types: list[str]             # NOT_EXECUTED 的 step 类型
    annotation_warnings: list[AnnotationWarning] # 携带但不影响 verdict


class PlanComparator:
    """SQL Plan ↔ Spark Plan 逻辑链路对比器。"""

    def compare(
        self,
        sql_plan: SqlBuildPlan,
        spark_plan: SparkPlan,
        annotations: list[StepAnnotation] | None = None,
        warnings: list[AnnotationWarning] | None = None,
        enabled_step_types: set[str] | None = None,
    ) -> PlanComparisonReport:
        """执行逻辑链路对比。不在 enabled_step_types 内的类型 → NOT_EXECUTED。"""
```

### 2.3 PhysicalVerifier：物理链路

```python
# spark/physical_verifier.py

class PhysicalVerificationStatus(str, Enum):
    RESULT_CONSISTENT = "RESULT_CONSISTENT"     # 双引擎结果一致
    RESULT_MISMATCH = "RESULT_MISMATCH"         # 结果不一致
    EXECUTION_FAILED = "EXECUTION_FAILED"       # 执行失败
    NOT_EXECUTED = "NOT_EXECUTED"               # 尚未执行物理验证
    HUMAN_REVIEW = "HUMAN_REVIEW"


class ResultCanonicalizer:
    """结果规范化器——统一双引擎输出格式再做对比。

    规则：
    - 按输出列顺序 + 主键/稳定排序键排序
    - 无排序键 → 标记 UNSUPPORTED_SEMANTICS，进入 HUMAN_REVIEW
    - 统一 NULL 表示、NaN 处理、Decimal scale、timestamp timezone、大小写策略
    """


class PhysicalVerifier:
    """物理链路验证器——同一份快照，两个引擎，对比结果。

    前置条件：逻辑链路 LOGIC_EQUIVALENT。
    未覆盖 step 类型 → NOT_EXECUTED。
    禁止泛化 PASS。
    """


class SnapshotCatalogBinder:
    """DuckDB 快照绑定器——执行前将 SnapshotManifest.files 注册为 DuckDB 视图。
    禁止 DuckDB 读取原始表——只能读快照目录中的 Parquet 文件。
    """
```

### 2.4 统一验证报告

```python
# spark/verification_report.py

class VerificationOverallStatus(str, Enum):
    ALL_CONSISTENT = "ALL_CONSISTENT"
    LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED = "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"
    LOGIC_CONSISTENT_PHYSICAL_MISMATCH = "LOGIC_CONSISTENT_PHYSICAL_MISMATCH"
    LOGIC_MISMATCH = "LOGIC_MISMATCH"
    REPAIR_NEEDED = "REPAIR_NEEDED"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"


class UnifiedVerificationReport(StrictModel):
    """Phase 7 统一验证报告——Phase 8 编排层消费。"""

    report_id: str
    contract_hash: str
    snapshot_id: str | None
    logic_status: ComparisonStatus
    logic_detail: PlanComparisonReport
    physical_status: PhysicalVerificationStatus
    physical_detail: PhysicalVerificationReport | None
    overall_status: VerificationOverallStatus
    requires_human_review: bool
    repair_attempts_remaining: int = 2
```

### 2.5 状态流转规则

```
逻辑链路:
  LOGIC_EQUIVALENT  → 进入物理链路
  LOGIC_MISMATCH    → REPAIR_NEEDED（返工 ≤2 轮）
  LOGIC_UNSUPPORTED → HUMAN_REVIEW

物理链路（仅当逻辑 LOGIC_EQUIVALENT）:
  快照完整性校验  → 失败 → EXECUTION_FAILED → HUMAN_REVIEW
  RESULT_CONSISTENT → REVIEW_READY
  RESULT_MISMATCH   → REPAIR_NEEDED（返工 ≤2 轮）
  EXECUTION_FAILED  → HUMAN_REVIEW

返工上限:
  累计返工 ≥ 2 轮 → HUMAN_REVIEW（禁止无限返工循环）

NOT_EXECUTED 传播规则:
  逻辑链路 NOT_EXECUTED → 物理链路自动 NOT_EXECUTED
  禁止将 NOT_EXECUTED 泛化为 PASS
  必须明确标记哪些 step 类型未被覆盖
```

### 2.6 RepairPlanner

```python
# spark/repair_planner.py

class RepairAction(StrictModel):
    action_id: str
    category: str  # "MAPPER_BUG" / "COMPILER_BUG" / "VALIDATOR_GAP" / "SNAPSHOT_ISSUE" / "BUSINESS_SEMANTIC"
    step_id: str | None
    issue: str
    suggested_fix: str


class RepairPlanner:
    """修复规划器——只输出结构化修复建议，不直接修改任何 Plan。

    硬边界：
    - 不直接修改 Contract、SqlBuildPlan、SparkPlan、PySpark 代码
    - 只输出 RepairAction
    - MAPPER_BUG / COMPILER_BUG / VALIDATOR_GAP → 路由回工程修复
    - BUSINESS_SEMANTIC → 默认 HUMAN_REVIEW 或回 SQL-first Planner
    - SparkPlan 结构修复 → 唯一允许路径是回到 mapper.py 修复 + 重新生成
    """
```

### 2.7 Local Spark Executor

```python
# spark/executor.py

class LocalSparkExecutor:
    """本地 Spark 执行器——在本地 Spark 上执行 PySpark DSL 代码。

    安全声明：exec(pyspark_code) 是受控开发验证口——代码已经过
    Compiler 确定性生成 + Static Validator AST 硬门禁。这不是通用安全沙箱。

    正式边界：子进程隔离（subprocess + preexec_fn）、临时工作目录（tempfile.mkdtemp）、
    资源限制（resource.setrlimit）、网络禁用（--no-network Spark 配置）、
    输出大小硬限制（默认 10MB）、执行超时、完整执行日志 + 环境 Manifest。
    """

    def execute(
        self,
        pyspark_code: str,
        inputs: dict[str, DataFrame],
        params: TransformParams | None = None,
    ) -> ExecutionResult:
        """执行 PySpark DSL 代码。
        生成代码固定入口：def transform(inputs, params) -> DataFrame
        """
```

---

## 3. Phase 8：Spark-first 编排硬化

### 3.1 SparkOrchestrator

```python
# spark/orchestrator.py

class SparkPipelineState(str, Enum):
    INIT = "INIT"
    SPARK_PLAN_MAPPED = "SPARK_PLAN_MAPPED"
    ANNOTATED = "ANNOTATED"
    COMPILED = "COMPILED"
    VALIDATED = "VALIDATED"
    SNAPSHOT_READY = "SNAPSHOT_READY"
    LOGIC_VERIFIED = "LOGIC_VERIFIED"
    PHYSICAL_VERIFIED = "PHYSICAL_VERIFIED"
    REVIEW_READY = "REVIEW_READY"
    REPAIR_NEEDED = "REPAIR_NEEDED"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    FAILED = "FAILED"


class SparkOrchestrator:
    """Spark-first 路径编排器。

    LLM 调用边界：不直接访问模型、不构造 Prompt、不解析 LLM 自由文本。
    可以调用已封装、结构化输出、带 AnnotationValidator 的 SparkDeveloperService。
    只接收通过 AnnotationValidator 校验的 AnnotatedSparkPlan。

    返工策略：
    - 累计 2 轮 → HUMAN_REVIEW
    - AnnotationWarning 不触发返工
    """
```

**Repair 路由**：

```
RepairPlanner 输出 RepairAction：
  MAPPER_BUG → 路由回 mapper.py 修复（工程团队）
  COMPILER_BUG → 路由回 Compiler 修复（工程团队）
  VALIDATOR_GAP → 路由回 Validator 规则修复（工程团队）
  SNAPSHOT_ISSUE → 重新生成快照
  BUSINESS_SEMANTIC → HUMAN_REVIEW 或回 SQL-first Planner 重新生成
```

### 3.2 Spark Review Package

```python
# spark/review_package.py

class CrossReferenceEntry(StrictModel):
    step_id: str
    contract_field: str              # 来源 Contract 字段
    spark_representation: str         # PySpark 代码片段
    business_description: str         # 从 Contract 派生的业务描述
    sql_artifact_id: str | None = None  # SQL 侧引用 ID，不是 SQL 文本
    sql_step_id: str | None = None      # SQL 侧 step 引用 ID
    derivation: Literal["contract-derived"] = "contract-derived"


class CrossReference(StrictModel):
    """跨引擎对照——不含 SQL 文本，只记录 Contract 字段和引用 ID。"""
    entries: list[CrossReferenceEntry]


class SparkReviewPackage(StrictModel):
    """Spark-first Code Review Package——统一交付物。"""

    package_id: str
    contract_hash: str

    # Spark 产物
    spark_plan: SparkPlan              # mapper 产出的 baseline
    annotations: list[StepAnnotation]  # SparkDeveloper 标注
    compiled_code: str                 # 含注释版本（仅供人审展示）
    compiled_code_raw: str            # 执行版本（Validator/执行/hash 以此为准）

    # 验证结果
    validation: SparkValidationResult
    verification: UnifiedVerificationReport

    # 跨引擎对照（contract-derived，不含 SQL 文本）
    cross_reference: CrossReference

    # 审查辅助
    annotation_warnings: list[AnnotationWarning]
    open_questions: list[OpenQuestion]

    # Hash 链
    provenance: SparkProvenance


class SparkProvenance(StrictModel):
    contract_hash: str
    spark_plan_hash: str
    annotation_hash: str
    compiled_code_sha256: str
    compiled_code_raw_sha256: str
    snapshot_id: str | None
    snapshot_manifest_hash: str | None
    verification_report_id: str | None
```

### 3.3 Harness 扩展

已有 SQL Harness（七维评测框架）新增 Spark 专属维度：

| 维度 | 说明 |
|------|------|
| `SPARK_CONTRACT_FIDELITY` | SparkPlan 与 Contract 的一致性 |
| `SPARK_COMPILATION_DETERMINISM` | 同一 Contract 两次编译 → 相同 raw 代码 |
| `SPARK_VALIDATOR_COVERAGE` | Static Validator 对 8 种错误码的拦截覆盖率 |
| `SPARK_LOGIC_EQUIVALENCE` | SQL/Spark PlanComparator 结构一致 |
| `SPARK_PHYSICAL_CONSISTENCY` | 双引擎同快照结果一致 |

### 3.4 Pipeline 集成

已有 Pipeline 条件性接入 Spark 链路，全局结果状态：

```
SQL_REVIEW_READY                           # SQL-only 通过
SQL_REVIEW_READY_SPARK_NOT_EXECUTED         # SQL 通过，Spark 未执行
SQL_REVIEW_READY_SPARK_HUMAN_REVIEW         # SQL 通过，Spark 需人工审查
SQL_SPARK_REVIEW_READY                     # SQL + Spark 全部通过
```

Spark 链路失败不影响 SQL 侧——SQL Review Package 正常产出。

### 3.5 Phase 6-8 实施节奏

```
阶段       范围                               验收标准
──────────────────────────────────────────────────────────
Phase 6A   scan/filter/project/sort/limit     编译 5 种 + Validator 全错误码 + 注释块
Phase 7A   逻辑链路（5 种）+ Snapshot 最小快照 LOGIC_EQUIVALENT + 完整性校验
Phase 6B   aggregate/join/case_when           编译扩展 3 种
Phase 7B   物理链路最小验证                    双引擎执行 + RESULT_CONSISTENT
Phase 6C   window                             窗口编译 + 帧边界
Phase 7C   物理链路扩展                        窗口双引擎验证
Phase 8    编排 + Review Package + Harness     完整链路 + Spark 5 维度评测
```

---

## 4. 风险清单

| 风险 | 类别 | 缓解措施 |
|------|------|---------|
| `exec(pyspark_code)` 安全边界 | C | 明确不是安全沙箱；正式边界含子进程隔离/网络禁用/资源限制 |
| Snapshot 数据源污染 | C | SnapshotSourceProvider 白名单；禁止从 Contract 推导生产连接 |
| LLM 标注幻觉修改 Plan | C | StructuredOutput + AnnotationValidator + step_id 强校验 |
| RepairPlanner 直接改 Plan | C | 只输出 RepairAction；路由回 mapper.py 或 HUMAN_REVIEW |
| NOT_EXECUTED 泛化 PASS | C | 状态机强制 NOT_EXECUTED → HUMAN_REVIEW，Step-by-step 标记 |
| SparkDeveloper 读取 SQL | C | 硬约束：只能读 Contract + SparkPlan，Prompt 不含 SQL 文本 |
| 注释含 SQL 文本 | C | 固定 5 行格式；CrossReference 只放引用 ID |
| Renderer 字符串拼接注入 | C | 所有片段过 SparkCodeRenderer 封闭枚举白名单 |
| Orchestrator 直接调 LLM | C | 只调 SparkDeveloperService；不构造 Prompt/不访问模型 |
| 返工无限循环 | B | 最多 2 轮 → HUMAN_REVIEW |
| step_index 错位 | B | step_id 主键 + AnnotationValidator 校验 |
| 双引擎结果伪差异 | B | ResultCanonicalizer + 统一 NULL/NaN/Decimal/TZ 策略 |
| skip/xfail 伪装完成 | B | 标记 phase，不计入 Exit 验收 |

---

## 5. Phase 6-8 文件结构总览

```
src/tianshu_datadev/spark/
├── __init__.py                # 已有
├── models.py                  # 已有——SparkPlan IR
├── mapper.py                  # 已有——Contract → SparkPlan
├── plan_equivalence.py        # 已有——结构等价规则（Phase 7 消费）
│
├── annotations.py             # Phase 6 新增——标注模型 + AnnotationValidator
├── compiler.py                # Phase 6 新增——SparkCompiler（确定性代码生成）
├── renderer.py                # Phase 6 新增——SparkCodeRenderer（安全渲染）
├── validator.py               # Phase 6 新增——SparkStaticValidator（AST + 语义）
├── developer.py               # Phase 6 新增——SparkDeveloperService（LLM 封装）
│
├── snapshot.py                # Phase 7 新增——SnapshotBuilder + SnapshotSourceProvider
├── plan_comparator.py         # Phase 7 新增——PlanComparator
├── physical_verifier.py       # Phase 7 新增——PhysicalVerifier + ResultCanonicalizer
├── verification_report.py     # Phase 7 新增——UnifiedVerificationReport
├── repair_planner.py          # Phase 7 新增——RepairPlanner
├── executor.py                # Phase 7 新增——LocalSparkExecutor
│
├── orchestrator.py            # Phase 8 新增——SparkOrchestrator
├── review_package.py          # Phase 8 新增——SparkReviewPackage + CrossReference
└── review_builder.py          # Phase 8 新增——SparkReviewBuilder

src/tianshu_datadev/harness/
└── spark_eval.py              # Phase 8 新增——Spark 评测维度

tests/spark/
├── test_spark_plan.py              # 已有（45 tests）
├── test_annotations.py             # Phase 6 新增
├── test_renderer.py                # Phase 6 新增——含恶意输入拒绝
├── test_spark_compiler.py          # Phase 6 新增——6A 5 种，其余 skip/xfail
├── test_spark_compiler_comment.py  # Phase 6 新增——注释块格式
├── test_spark_validator.py         # Phase 6 新增——AST call-chain
├── test_spark_developer.py         # Phase 6 新增——prompt regression
├── test_snapshot.py                # Phase 7 新增
├── test_plan_comparator.py         # Phase 7 新增
├── test_physical_verifier.py       # Phase 7 新增
├── test_repair_planner.py          # Phase 7 新增
├── test_spark_executor.py          # Phase 7 新增
├── test_orchestrator.py            # Phase 8 新增
├── test_review_package.py          # Phase 8 新增
└── test_spark_e2e.py               # Phase 8 新增
```

---

> 设计完成。本方案是 Phase 6-8 的完整设计，经过多轮 CRCS 审查，所有 C 类硬边界已在设计层确认；实施阶段必须通过测试、扫描和 Harness 逐项验收。
>
> 关联设计文档：
> - `docs/superpowers/specs/2026-07-02-sql-temp-table-comments-design.md`（SQL 注释格式——Spark 侧对齐参考）
> - Phase 5: `src/tianshu_datadev/spark/models.py`, `mapper.py`, `plan_equivalence.py`
