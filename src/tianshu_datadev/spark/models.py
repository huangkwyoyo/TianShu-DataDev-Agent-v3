"""Phase 5 SparkPlan IR——Spark 侧的类型化中间表示。

SparkPlan 从 DataTransformContractV1 确定性映射，不读取 SQL 文本或 SqlBuildPlan。
每个 step 类型对应一种 PySpark DataFrame 操作，映射规则在 mapper.py 中实现。

设计原则：
- 确定性：相同 Contract → 相同 SparkPlan → 相同 hash
- 无自由代码：所有 step 均为结构化 Pydantic 对象
- 白名单：仅支持 9 种 step 类型
- 不包含 SQL 文本、DeveloperSpec 引用、运行时上下文
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Literal

from pydantic import Field

from tianshu_datadev.artifacts.models import CaseWhenCondition
from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# SparkPlan 顶层容器
# ════════════════════════════════════════════


class SparkStepType(str, Enum):
    """SparkPlan 支持的 step 类型——白名单，不可扩展。"""

    READ = "read"           # spark.read.parquet(path)
    FILTER = "filter"       # df.filter(condition)
    JOIN = "join"           # df.join(other, on=keys, how=join_type)
    AGGREGATE = "aggregate" # df.groupBy(keys).agg(*metrics)
    PROJECT = "project"     # df.select(*columns)
    CASE_WHEN = "case_when" # F.when(cond, result).otherwise(else_val)
    WINDOW = "window"       # F.row_number().over(Window.partitionBy(...).orderBy(...))
    SORT = "sort"           # df.orderBy(*sorts)
    LIMIT = "limit"         # df.limit(n)


class SparkJoinType(str, Enum):
    """Spark Join 类型白名单——与 SQL Join 类型一一对应。"""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class SparkSortDirection(str, Enum):
    """排序方向白名单。"""

    ASC = "asc"
    DESC = "desc"


class SparkAggFunction(str, Enum):
    """聚合函数白名单——对应 DataTransformContractV1 中的 function 字段。"""

    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


class SparkWindowFunction(str, Enum):
    """窗口函数白名单——Phase 3B 已定义的 9 种。"""

    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    NTILE = "NTILE"
    LAG = "LAG"
    LEAD = "LEAD"
    SUM_OVER = "SUM_OVER"
    AVG_OVER = "AVG_OVER"
    COUNT_OVER = "COUNT_OVER"


# ════════════════════════════════════════════
# SparkPlan Step 类型
# ════════════════════════════════════════════


class SparkReadStep(StrictModel):
    """Spark 读取步骤——从 DataTransformContractV1.input_tables 映射。

    对应 SQL 侧的 ScanStep。
    物理路径不存放在 SparkPlan 中，由 SnapshotManifest 管理。
    """

    step_type: SparkStepType = SparkStepType.READ
    alias: str  # DataFrame 变量名（如 "od"）
    source_name: str  # inputs dict 的 key（替换旧 source_path，物理路径在 SnapshotManifest）
    input_key: str  # 对应 ContractInputTable 的唯一标识
    required_columns: list[str] = Field(default_factory=list)  # 需要的列（Phase 5 暂空）
    estimated_row_count: int | None = None  # 估算行数


class SparkFilterStep(StrictModel):
    """Spark 过滤步骤——从 DataTransformContractV1.filters 映射。

    对应 SQL 侧的 FilterStep。
    过滤条件以结构化三元组 (left, operator, right) 表达，不含自由文本。
    """

    step_type: SparkStepType = SparkStepType.FILTER
    input_alias: str  # 输入 DataFrame 别名
    operator: str  # 操作符，如 "GT" / "EQ" / "AND" / "IN" / "IS_NULL"
    left: str  # 左操作数（列引用）
    right: str  # 右操作数（字面量或列引用）


class SparkJoinStep(StrictModel):
    """Spark Join 步骤——从 DataTransformContractV1.join_relationships 映射。

    对应 SQL 侧的 JoinStep。
    包含完整证据链——Join 的理由和置信度是可审查的。
    """

    step_type: SparkStepType = SparkStepType.JOIN
    left_alias: str  # 左 DataFrame 别名
    right_alias: str  # 右 DataFrame 别名
    left_key: str  # 左 Join 键（原始字段名）
    right_key: str  # 右 Join 键（原始字段名）
    join_type: SparkJoinType  # INNER / LEFT / RIGHT / FULL
    evidence_chain: dict = Field(default_factory=dict)  # 完整证据链（从 ContractJoin 直传）


class SparkAggregateStep(StrictModel):
    """Spark 聚合步骤——从 DataTransformContractV1.aggregations + grouping_keys 映射。

    对应 SQL 侧的 AggregateStep。
    """

    step_type: SparkStepType = SparkStepType.AGGREGATE
    input_alias: str  # 输入 DataFrame 别名
    group_keys: list[str] = []  # 分组键（归一化字段名列表）
    metrics: list[SparkAggregateSpec] = []  # 聚合指标


class SparkAggregateSpec(StrictModel):
    """单个聚合指标——映射 ContractAggregation。"""

    function: SparkAggFunction  # 聚合函数
    input_column: str | None = None  # 输入列（COUNT(*) 时为 None）
    alias: str  # 输出列别名


class SparkProjectStep(StrictModel):
    """Spark 投影步骤——从 DataTransformContractV1.output_columns 映射。

    对应 SQL 侧的 ProjectStep。
    """

    step_type: SparkStepType = SparkStepType.PROJECT
    input_alias: str  # 输入 DataFrame 别名
    columns: list[SparkProjectColumn] = []  # 投影列


class SparkProjectColumn(StrictModel):
    """单个投影列——映射 ContractOutputColumn。"""

    column_name: str  # 源列名
    alias: str  # 输出别名


class SparkCaseWhenStep(StrictModel):
    """Spark CASE WHEN 步骤——从 DataTransformContractV1.case_when_labels 映射。

    对应 SQL 侧的 CaseWhenStep。
    每个 CaseWhenLabelSpec 生成一个 withColumn 链。
    """

    step_type: SparkStepType = SparkStepType.CASE_WHEN
    input_alias: str  # 输入 DataFrame 别名
    output_alias: str  # 输出列别名
    branches: list[SparkCaseWhenBranch] = []  # WHEN 分支列表
    else_value: str | None = None  # ELSE 默认值


class SparkCaseWhenBranch(StrictModel):
    """单个 CASE WHEN 分支。

    Phase 5 仅保留标签值——完整谓词还原在 Phase 7 PlanEquivalence 时做。
    Phase 6B 新增 condition_column / condition_value 支持编译期代码生成。
    Phase 10 新增 condition: CaseWhenCondition——结构化 Predicate AST，
             从 Contract 提取器的 _predicate_to_case_when_condition() 生成。
             condition_column / condition_value 保留向后兼容（展示/审查用途），
             编译器优先使用 condition，为 None 时抛出 RenderError 阻断。
    """

    label: str  # 标签值
    condition_column: str = ""  # Phase 6B：条件列名（保留兼容，不进 compiler 可执行路径）
    condition_value: str = ""   # Phase 6B：条件值（保留兼容，不进 compiler 可执行路径）
    condition: CaseWhenCondition | None = None  # Phase 10：结构化条件 AST


class SparkWindowStep(StrictModel):
    """Spark 窗口函数步骤——从 DataTransformContractV1.window_specs 映射。

    对应 SQL 侧的 WindowStep。
    """

    step_type: SparkStepType = SparkStepType.WINDOW
    input_alias: str  # 输入 DataFrame 别名
    expressions: list[SparkWindowExpr] = []  # 窗口表达式列表


class SparkWindowExpr(StrictModel):
    """单个窗口函数表达式——映射 WindowSpecSummary。

    frame_type / frame_start / frame_end 控制窗口帧边界（ROWS/RANGE BETWEEN）。
    针对不同窗口函数的默认帧语义：
    - ROW_NUMBER / RANK / DENSE_RANK / NTILE：默认 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    - LAG / LEAD：忽略帧（单行偏移）
    - SUM_OVER / AVG_OVER / COUNT_OVER：默认 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    """

    function: SparkWindowFunction  # 窗口函数名
    alias: str  # 输出列别名
    input_column: str | None = None  # 输入列名（LAG/LEAD/SUM_OVER/AVG_OVER/COUNT_OVER 需要；排名函数不需要）
    partition_by: list[str] = []  # 分区键列名列表
    order_by: list[str] = []  # 排序键列名列表
    # 帧边界配置（可选——默认使用函数类型对应的标准帧）
    frame_type: str = "rows"  # "rows" | "range"
    frame_start: str = "unbounded_preceding"  # 帧起始边界
    frame_end: str = "current_row"  # 帧结束边界


class SparkSortStep(StrictModel):
    """Spark 排序步骤——从 DataTransformContractV1.sort_spec 映射。

    对应 SQL 侧的 SortStep。
    """

    step_type: SparkStepType = SparkStepType.SORT
    input_alias: str  # 输入 DataFrame 别名
    order_by: list[SparkSortSpec] = []  # 排序规格列表


class SparkSortSpec(StrictModel):
    """单个排序规格——映射 ContractSort。"""

    column: str  # 排序列名
    direction: SparkSortDirection  # ASC / DESC


class SparkLimitStep(StrictModel):
    """Spark 行限制步骤——从 DataTransformContractV1.limit_spec 映射。

    对应 SQL 侧的 LimitStep。
    """

    step_type: SparkStepType = SparkStepType.LIMIT
    input_alias: str  # 输入 DataFrame 别名
    limit: int  # 最大行数
    offset: int | None = None  # 偏移量（Phase 5 保留，默认 None）


# ════════════════════════════════════════════
# SparkPlan 顶层模型
# ════════════════════════════════════════════

# SparkStep 联合类型——所有可能的 step 类型
SparkStep = (
    SparkReadStep
    | SparkFilterStep
    | SparkJoinStep
    | SparkAggregateStep
    | SparkProjectStep
    | SparkCaseWhenStep
    | SparkWindowStep
    | SparkSortStep
    | SparkLimitStep
)


class SparkPlan(StrictModel):
    """SparkPlan IR——Spark 侧的类型化中间表示。

    从 DataTransformContractV1 确定性映射生成。
    不包含 SQL 文本、PySpark 代码、DeveloperSpec 引用。
    相同 Contract → 相同 SparkPlan → 相同 plan_hash。

    这是 Phase 5 的核心交付物——SparkPlan IR Schema。
    Phase 6 的 SparkDeveloper 消费此 IR 生成 PySpark 代码。
    Phase 7 的 PlanComparator 消费此 IR 做结构等价对比。
    """

    plan_id: str  # 确定性 ID（从 contract_hash 派生）
    version: str = "v1"  # SparkPlan IR 版本
    source_phase: str = "phase-5"  # 来源阶段
    source_contract_hash: str  # 来源 DataTransformContractV1 的 hash
    source_contract_version: str = "v1"  # 来源 Contract 的版本
    steps: list[SparkStep] = Field(default_factory=list)
    # ── 写入占位（Phase 6 开放）──
    write_mode: str | None = None  # "overwrite_partition" / None（Phase 5 不实现写入）

    @staticmethod
    def generate_plan_id(contract_hash: str) -> str:
        """基于 contract_hash 的确定性 plan ID。

        Args:
            contract_hash: DataTransformContractV1 的 SHA-256

        Returns:
            "spark_{hash[:12]}" 格式的 plan_id
        """
        hash_hex = hashlib.sha256(
            f"sparkplan_v1:{contract_hash}".encode()
        ).hexdigest()[:12]
        return f"spark_{hash_hex}"

    @staticmethod
    def compute_plan_hash(plan: SparkPlan) -> str:
        """计算 SparkPlan 的确定性 SHA-256。

        排除 plan_id（由 contract_hash 派生），仅计算业务字段。
        相同 steps → 相同 hash。

        Args:
            plan: 已构建的 SparkPlan

        Returns:
            64 字符十六进制 SHA-256
        """
        data = plan.model_dump(
            exclude={"plan_id"},
            exclude_none=True,
        )
        # 确保 steps 列表按 step_type 排序——同内容不同顺序 → 相同 hash
        if "steps" in data and isinstance(data["steps"], list):
            data["steps"] = sorted(
                data["steps"],
                key=lambda s: json.dumps(s, sort_keys=True, default=str),
            )
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()


# ════════════════════════════════════════════
# UNSUPPORTED_PATTERN / CONTRACT_GAP 模型
# ════════════════════════════════════════════


class UnsupportedPattern(StrictModel):
    """数据变换模式无法映射为 SparkPlan step 时输出的阻断信息。

    当前不支持的模式（Phase 5）：
    - 相关子查询
    - CTE
    - 自定义窗口帧（非默认帧）
    - CROSS JOIN（无证据的笛卡尔积）
    """

    pattern_id: str  # 唯一标识
    contract_field: str  # 无法映射的 Contract 字段
    reason: str  # 为什么无法映射
    suggested_workaround: str | None = None  # 建议的替代方案


class ContractGap(StrictModel):
    """Contract 中缺失映射所需的必要信息。

    例如：
    - output_columns 为空
    - join_relationships 缺少 evidence_chain
    - aggregations 中的 function 不在白名单内
    """

    gap_id: str  # 唯一标识
    contract_field: str  # 缺失信息的字段
    missing_info: str  # 缺失什么
    severity: Literal["BLOCKING", "WARN"] = "BLOCKING"  # 阻断级别


class SparkPlanMappingResult(StrictModel):
    """Contract → SparkPlan 映射的完整结果。

    成功时：spark_plan 非空，unsupported 和 gaps 为空。
    失败时：spark_plan 为 None，unsupported 和 gaps 记录所有阻断项。
    部分成功：spark_plan 非空但 warnings 列表非空（WARN 级别 gap）。
    """

    success: bool  # 映射是否成功（BLOCKING gap 或 unsupported 存在时 = False）
    spark_plan: SparkPlan | None = None  # 成功时非空
    unsupported: list[UnsupportedPattern] = Field(default_factory=list)
    gaps: list[ContractGap] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
