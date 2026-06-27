"""DEPRECATED — Phase 1 及以后使用 strict Pydantic 模型（extra="forbid"）替代。

Phase 0 只定义最基础的 Protocol 接口和状态枚举。
这些接口规定了各模块之间的契约——不包含实现代码。

迁移规划：
- ParsedDeveloperSpec → tianshu_datadev.developer_spec.models.ParsedDeveloperSpec
- SourceManifest → tianshu_datadev.developer_spec.models.SourceManifest
- 后续 Protocol 将在对应 Phase 中被严格 Pydantic 模型替换

不要删除此文件——Phase 0 的 22 个测试依赖它。
"""

from __future__ import annotations

import warnings

warnings.warn(
    "ir.protocols is deprecated. Use strict Pydantic models from developer_spec.models instead.",
    DeprecationWarning,
    stacklevel=2,
)

# ── 以下为 Phase 0 原始代码，未修改 ──

# noqa: E402 — 以下导入在 DeprecationWarning 之后，因为 warning 必须在导入其他模块前执行
from enum import Enum  # noqa: E402
from typing import Protocol, runtime_checkable  # noqa: E402

# =============================================================================
# 状态枚举
# =============================================================================


class RequestStatus(str, Enum):
    """一次数据开发请求的生命周期状态。"""

    DRAFT = "DRAFT"  # 项目书已接收，尚未开始处理
    ANALYZING = "ANALYZING"  # Requirement Analyzer 正在解析
    DECOMPOSING = "DECOMPOSING"  # SubIntent Decomposer 正在拆分
    EXECUTING_SQL = "EXECUTING_SQL"  # SQL 分支执行中
    EXECUTING_SPARK = "EXECUTING_SPARK"  # Spark 分支执行中
    VALIDATING = "VALIDATING"  # 交叉验证进行中
    DIAGNOSING = "DIAGNOSING"  # DifferenceAnalyst 分析差异
    RETRYING = "RETRYING"  # 返工循环中
    HUMAN_REVIEW = "HUMAN_REVIEW"  # 等待人工审查
    COMPLETED = "COMPLETED"  # 流程正常结束
    FAILED = "FAILED"  # 无法继续，需人工介入


class StepStatus(str, Enum):
    """单个步骤的执行状态。"""

    PASS = "PASS"  # 确定性验证通过——只能由确定性 Comparator 产生
    FAIL = "FAIL"  # 安全检查或执行失败
    DIFFERENT = "DIFFERENT"  # SQL/Spark 结果不一致
    NOT_EXECUTED = "NOT_EXECUTED"  # 步骤未执行（如 Spark 不可用）
    SKIPPED = "SKIPPED"  # 步骤被跳过（不需要执行）


class RepairTarget(str, Enum):
    """修复目标——规定返工应该修改什么。"""

    SQL_PLAN = "SQL_PLAN"  # 修改 SQLPlan 后重新编译 SQL
    SPARK_CODE = "SPARK_CODE"  # 修改 PySpark 代码
    BOTH = "BOTH"  # SQL 和 Spark 都需要修改
    REQUIREMENT = "REQUIREMENT"  # 需求本身有问题，需要澄清
    HUMAN_REVIEW = "HUMAN_REVIEW"  # 无法自动确定修复目标


# =============================================================================
# 核心 IR Protocol 接口
# =============================================================================


@runtime_checkable
class RequirementIR(Protocol):
    """解析后的项目书——结构化需求表示。

    Requirement Analyzer LLM 的输出。包含从项目书中提取的指标、
    维度、过滤条件、时间范围和预期输出粒度。
    """

    request_id: str  # 请求唯一标识
    metrics: list[str]  # 指标名称列表（必须来自 TianShu metric_contract）
    dimensions: list[str]  # 维度/分组字段列表
    time_range: str | None  # 时间范围的自然语言描述（待 SubIntent 解析为具体日期）
    filters: list[dict]  # 过滤条件列表
    grain: str | None  # 预期输出粒度（如 "daily"、"zone"）
    raw_spec: str  # 原始项目书文本，用于追溯


@runtime_checkable
class SubIntent(Protocol):
    """拆解后的子意图——每个 SubIntent 对应一个 planning_table 的查询。

    SubIntent Decomposer 的输出。跨表多指标需求被拆分为多个 SubIntent，
    每个 SubIntent 独立规划 SQLPlan 和执行。
    """

    sub_intent_id: str  # 子意图唯一标识
    parent_request_id: str  # 关联的请求 ID
    metrics: list[str]  # 本子意图要查询的指标
    planning_table: str  # 主查询表（来自 TianShu 语义层）
    time_range: dict | None  # 解析后的时间范围（start、end）
    dimensions: list[str]  # 分组维度
    status: str  # 子意图处理状态（RequestStatus 值）


@runtime_checkable
class SQLPlan(Protocol):
    """SQL 执行计划——LLM 输出、Python 编译器确定性生成 SQL 的中间表示。

    SQLPlan 由 LLM（SQL Planner）生成，由 Python sql_gen 模块编译为
    可执行的 SQL 字符串。LLM 不直接生成 SQL 字符串。
    """

    plan_id: str  # 计划唯一标识
    sub_intent_id: str  # 关联的子意图 ID
    primary_table: str  # 主查询表
    joins: list[dict]  # JOIN 计划列表（table、on、type）
    where_clauses: list[str]  # WHERE 子句列表
    group_by: list[str]  # GROUP BY 字段列表
    order_by: list[str]  # ORDER BY 字段列表
    aggregations: list[dict]  # 聚合表达式列表（expr、alias）
    limit: int | None  # 结果行数上限
    confidence: float  # LLM 对计划的置信度（0.0–1.0）


@runtime_checkable
class ExecutionTrace(Protocol):
    """单次执行的完整追踪记录。

    记录 SQL 或 Spark 执行的全过程，用于交叉验证阶段的问题定位。
    """

    trace_id: str  # 追踪唯一标识
    plan_id: str  # 关联的执行计划 ID
    engine: str  # 执行引擎："duckdb" 或 "spark"
    generated_code: str  # 实际执行的代码（SQL 字符串或 PySpark DSL）
    status: str  # 执行状态（StepStatus 值）
    row_count: int  # 返回行数
    execution_time_ms: float  # 执行耗时（毫秒）
    error_message: str | None  # 执行失败时的错误信息


@runtime_checkable
class ResultSummary(Protocol):
    """结构化执行结果摘要——用于交叉验证比对的标准化格式。

    将 DuckDB 和 Spark 的执行结果转换为统一格式后进行比较。
    """

    summary_id: str  # 摘要唯一标识
    trace_id: str  # 关联的执行追踪 ID
    engine: str  # 执行引擎
    columns: list[str]  # 输出列名
    column_types: list[str]  # 规范化后的列类型
    row_count: int  # 行数
    null_counts: dict[str, int]  # 逐列空值计数
    numeric_sums: dict[str, float]  # 数值列合计
    sample_rows: list[list]  # 前 N 行抽样数据


@runtime_checkable
class CrossValidationResult(Protocol):
    """确定性交叉验证结果——比较 SQL 和 Spark 的执行结果。

    Comparator 是确定性的——不依赖 LLM。PASS 只能由本结果产生。
    """

    validation_id: str  # 验证唯一标识
    request_id: str  # 关联的请求 ID
    sql_summary_id: str  # SQL 结果摘要 ID
    spark_summary_id: str  # Spark 结果摘要 ID
    status: str  # 验证状态（StepStatus 值）
    comparisons: list[dict]  # 逐维度比较结果列表
    # 每个比较项包含：dimension（维度名）、match（是否一致）、detail（详情）
    # 比较维度至少包含：列名、数据类型、行数、空值计数、数值合计、样本行


@runtime_checkable
class RepairDirective(Protocol):
    """修复指令——由 DifferenceAnalyst 或 RepairPlanner 产生。

    规定下一轮返工应该修改什么、为什么修改。
    """

    directive_id: str  # 指令唯一标识
    validation_id: str  # 关联的交叉验证结果 ID
    target: str  # 修复目标（RepairTarget 值）
    reason: str  # 修复原因——LLM 对差异的分析
    suggestions: list[str]  # 具体修复建议列表
    retry_count: int  # 当前返工轮次（从 1 开始）


@runtime_checkable
class MergedResult(Protocol):
    """多 SubIntent 结果合并后的统一结果。

    当需求被拆分为多个 SubIntent 时，各自独立执行后将结果合并。
    """

    merge_id: str  # 合并唯一标识
    request_id: str  # 关联的请求 ID
    merge_key: str | None  # 合并键（如 "date"、"zone_id"）
    row_count: int  # 合并后的总行数
    source_summary_ids: list[str]  # 各子结果摘要 ID 列表
    status: str  # 合并状态（StepStatus 值）


# =============================================================================
# Spark 侧 Protocol 接口
# =============================================================================


@runtime_checkable
class TransformationContract(Protocol):
    """PySpark 转换的结构化契约——LLM 必须遵守的输入/输出/安全边界。

    契约声明了 transform() 函数允许使用的表、字段、Join 路径、
    指标定义和输出 Schema。Spark Static Validator 用此契约校验生成代码。
    """

    contract_id: str  # 契约唯一标识
    sub_intent_id: str  # 关联的子意图 ID
    declared_inputs: list[str]  # 允许从 inputs 读取的源表名列表——禁止读取列表外的表
    field_schemas: dict[str, list[str]]  # 每个源表的字段白名单（表名→字段名列表）
    join_paths: list[dict]  # 允许的 JOIN 路径（left_table/right_table/on/type）
    metrics: list[dict]  # 指标定义列表（name/expr/output_alias）
    output_schema: list[dict]  # 输出列定义（name/type/nullable/description）
    grain: str | None  # 预期输出粒度（如 "daily"、"zone"）——禁止随意改变粒度
    allowed_imports: list[str]  # 允许导入的模块白名单（如 "pyspark.sql.functions"）
    params_schema: dict | None  # TransformParams 的 JSON Schema——约束 LLM 可用的参数


@runtime_checkable
class TransformParams(Protocol):
    """传递给 transform() 的运行时参数。

    参数必须来自 TransformationContract 中声明的 params_schema——
    LLM 不能自行决定参数结构。参数值在编译/执行时由系统注入，不由 LLM 指定。
    """

    # Protocol 不约束具体字段——具体字段由 TransformationContract.params_schema 声明
    # 此处只标记类型身份，实际校验由 JSON Schema 和 Static Validator 完成


@runtime_checkable
class SparkCodeArtifact(Protocol):
    """PySpark 代码生成产物——SparkDeveloper 的输出。

    包含经过静态验证和 Reviewer 检查的代码及其完整溯源信息。
    代码正文落盘为独立文件，Graph State 只保存本 artifact 引用。
    """

    artifact_id: str  # 产物唯一标识
    sub_intent_id: str  # 关联的子意图 ID
    contract_ref: str  # 关联的 TransformationContract.contract_id
    code_ref: str  # 代码文件路径（相对于 generated/ 目录）
    code_sha256: str  # 代码正文的 SHA-256 哈希——用于完整性校验
    entrypoint: str  # 入口函数名，固定为 "transform"
    model_id: str  # 生成此代码的 LLM 模型标识
    prompt_version: str  # 使用的 Prompt 模板版本
    generation_round: int  # 生成轮次（首次=1，每轮返工递增）
    allowed_imports: list[str]  # 实际允许的导入白名单
    declared_inputs: list[str]  # 代码声明的输入表名——必须 ⊆ contract.declared_inputs
    expected_output_schema: list[dict]  # 预期输出 Schema（从 contract 复制）
    static_validation_status: str  # 静态验证状态（StepStatus 值）
