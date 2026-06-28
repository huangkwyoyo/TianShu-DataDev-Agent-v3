"""Phase 2 数据模型——Code Review Package、DataTransformContract-lite、ReviewFeedback。

所有模型继承 StrictModel（extra="forbid"），拒绝未知字段。
DataTransformContract-lite 从 SqlBuildPlan 确定性抽取，不包含 SQL 代码字段。
ReviewFeedback 的 target 是机器路由主字段，finding_type 是细分原因。
"""

from __future__ import annotations

import hashlib
import json

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# DataTransformContract-lite
# ════════════════════════════════════════════


class ContractInputTable(StrictModel):
    """Contract 中的输入表——精简自 ScanStep。"""

    table_ref: str  # 表别名（来自 SqlBuildPlan）
    source_table: str  # 物理表名
    estimated_row_count: int | None = None  # 估算行数


class ContractColumn(StrictModel):
    """Contract 中的列——精简自 ColumnRef。"""

    column_name: str  # 原始字段名
    normalized_name: str  # 归一化字段名
    data_type: str  # 推断或声明的字段类型
    table_ref: str  # 所属表别名


class ContractJoin(StrictModel):
    """Contract 中的 Join 关系——精简自 JoinStep + RelationshipEvidence。"""

    join_id: str  # 来自 JoinCandidate.candidate_id
    left_table: str  # 左表别名
    right_table: str  # 右表别名
    left_key: str  # 左键原始字段名
    right_key: str  # 右键原始字段名
    join_type: str  # INNER / LEFT / RIGHT / FULL
    evidence_chain: dict = {}  # 完整证据链（来自 RelationshipEvidence 序列化）
    level: str  # STRONG / MEDIUM（WEAK/NONE 不进入 Contract）


class ContractPredicate(StrictModel):
    """Contract 中的过滤条件——人类可读的谓词描述。

    不包含嵌套 AST，仅保留表达式的结构化摘要。
    """

    expression: str  # 人类可读的谓词描述，如 "tf.amount > 0"
    operator: str  # 操作符，如 "GT" / "EQ" / "AND"
    left: str  # 左操作数描述
    right: str  # 右操作数描述


class ContractAggregation(StrictModel):
    """Contract 中的聚合定义——精简自 AggregateSpec。"""

    function: str  # COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT
    input_column: str | None = None  # None 表示 COUNT(*)
    alias: str  # 输出别名


class ContractOutputColumn(StrictModel):
    """Contract 中的输出列——精简自 ProjectStep。"""

    column_name: str  # 列名
    alias: str  # 输出别名
    data_type: str | None = None  # 推断的数据类型


class ContractSort(StrictModel):
    """Contract 中的排序规格——精简自 SortSpec。"""

    column: str  # 排序列名
    direction: str  # ASC / DESC


class ContractLimit(StrictModel):
    """Contract 中的行限制——精简自 LimitStep。"""

    limit: int  # 最大行数
    offset: int | None = None  # 偏移量


class DataTransformContractLite(StrictModel):
    """DataTransformContract-lite——从 SqlBuildPlan 确定性抽取的业务规格。

    version 固定为 "lite"，source_phase 固定为 "phase-2"。
    不包含 SQL 代码、SqlBuildPlan 实现细节或自由文本字段。
    相同 SqlBuildPlan → 相同 Contract → 相同 hash。

    Phase 2 仅支持单语句 SqlBuildPlan（不依赖 SqlProgram）。
    """

    contract_id: str  # 确定性 ID
    version: str = "lite"  # 固定为 lite
    source_phase: str = "phase-2"  # 来源阶段
    source_sqlbuildplan_hash: str  # 来源 SqlBuildPlan 的 SHA-256
    input_tables: list[ContractInputTable] = []  # 输入表
    input_columns: list[ContractColumn] = []  # 实际使用的列
    join_relationships: list[ContractJoin] = []  # Join 关系（含证据链）
    filters: list[ContractPredicate] = []  # 过滤条件
    aggregations: list[ContractAggregation] = []  # 聚合定义
    grouping_keys: list[str] = []  # 分组键（归一化字段名列表）
    output_columns: list[ContractOutputColumn] = []  # 输出列
    output_grain: list[str] = []  # 输出粒度（归一化字段名列表）
    sort_spec: list[ContractSort] | None = None  # 排序规格
    limit_spec: ContractLimit | None = None  # 行限制
    business_keys: list[str] = []  # 业务键（从 dimensions + grain 推导）
    semantic_policy_ref: str = ""  # 语义策略引用（Phase 2 固定为空）

    @staticmethod
    def generate_contract_id(plan_hash: str) -> str:
        """基于 plan_hash 的确定性 contract ID。"""
        hash_hex = hashlib.sha256(
            f"dtc_lite:{plan_hash}".encode()
        ).hexdigest()[:12]
        return f"dtc_lite_{hash_hex}"

    @staticmethod
    def compute_contract_hash(contract: DataTransformContractLite) -> str:
        """计算 contract 的确定性 SHA-256。

        排除 contract_id（由 plan_hash 派生），仅计算业务字段。
        """
        data = contract.model_dump(
            exclude={"contract_id"},
            exclude_none=True,
        )
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()


# ════════════════════════════════════════════
# ReviewPackage 模型
# ════════════════════════════════════════════


class ArtifactRef(StrictModel):
    """artifact 引用——路径 + SHA-256，用于 ReviewPackageManifest。"""

    path: str  # 包内相对路径（如 "sql/main.sql"）
    sha256: str  # 文件内容 SHA-256


class ValidationSummaryArtifact(StrictModel):
    """验证摘要 artifact——汇总 Validator + PerfValidator 的全部结果。

    不保存完整结果集，仅保存摘要和统计数据。
    """

    validation_id: str  # 唯一标识
    plan_id: str  # 关联的 SqlBuildPlan.plan_id
    validator_passed: bool  # SqlBuildPlanValidator 是否全部通过
    validator_questions: list[dict] = []  # OpenQuestion 列表（序列化）
    perf_all_passed: bool  # PerfValidator REJECT 规则全部通过
    perf_results: list[dict] = []  # PerfValidationResult 列表（序列化）
    blocking_count: int = 0  # 阻断项数量
    warning_count: int = 0  # 警告项数量

    @staticmethod
    def generate_validation_id(plan_id: str) -> str:
        """基于 plan_id 的确定性 validation ID。"""
        hash_hex = hashlib.sha256(
            f"validation:{plan_id}".encode()
        ).hexdigest()[:12]
        return f"val_{hash_hex}"


class ReviewPackageManifest(StrictModel):
    """Code Review Package 清单——记录所有 artifact 的引用和 hash。"""

    request_id: str  # 请求唯一标识
    package_id: str  # 包唯一标识
    created_at: str  # ISO 时间戳
    artifacts: list[ArtifactRef] = []  # 所有 artifact 的引用列表
    spec_hash: str = ""  # DeveloperSpec SHA-256
    source_manifest_hash: str = ""  # SourceManifest SHA-256
    sql_build_plan_hash: str = ""  # SqlBuildPlan SHA-256
    sql_artifact_hash: str = ""  # SqlArtifact SHA-256
    data_transform_contract_hash: str = ""  # DataTransformContract SHA-256
    provenance_hash: str = ""  # provenance.yml SHA-256
    retry_count: int = 0  # 返工轮次

    @staticmethod
    def generate_package_id(request_id: str) -> str:
        """基于 request_id 的确定性 package ID。"""
        hash_hex = hashlib.sha256(
            f"pkg:{request_id}".encode()
        ).hexdigest()[:12]
        return f"pkg_{hash_hex}"


class HumanReviewItem(StrictModel):
    """人工审查清单项——供 review.md 渲染和数据工程师审查使用。"""

    item_id: str  # 唯一标识
    category: str  # 分类：join_evidence / time_filter / enum_values / open_question / performance
    description: str  # 人类可读的审查项描述
    severity: str  # 严重程度：blocking / warning / info
    related_artifact: str | None = None  # 关联的 artifact 路径


# ════════════════════════════════════════════
# ReviewFeedback
# ════════════════════════════════════════════

# target 合法值集合——机器路由主字段
VALID_REVIEW_TARGETS: frozenset[str] = frozenset({
    "REQUIREMENT",   # → 修改 DeveloperSpec，重新走 Parser/Planner
    "SQL_PLAN",      # → 生成新 SqlBuildPlan（禁止直接改 SQL 文本）
    "COMPILER_BUG",  # → 修 Compiler 并加回归测试
    "SOURCE_FACT",   # → 更新 SourceManifest / SchemaRegistry / open_questions
    "HUMAN_REVIEW",  # → 停止自动返工，需人工介入
})

# target → 返工入口路由表
REVIEW_ROUTING_TABLE: dict[str, str] = {
    "REQUIREMENT": "修改 DeveloperSpec 或补 HumanResolution，重新从 Parser/Planner 走",
    "SQL_PLAN": "生成新 SqlBuildPlan（禁止直接改 SQL 文本）；Join 问题进入 RelationshipHypothesis 重新定级",
    "COMPILER_BUG": "修 Compiler 并加回归测试",
    "SOURCE_FACT": "更新 SourceManifest / SchemaRegistry / open_questions",
    "HUMAN_REVIEW": "反馈无法结构化、证据不足或需求变化不明确，停止自动返工",
}


class ReviewFeedback(StrictModel):
    """结构化 Review 反馈——人工审查不通过时的返工输入。

    target 是机器路由主字段（REQUIREMENT/SQL_PLAN/COMPILER_BUG/SOURCE_FACT/HUMAN_REVIEW），
    finding_type 是细分原因，不参与路由。
    target=HUMAN_REVIEW 时停止自动返工。

    返工不靠 Memory，靠 artifact 引用 + hash + checkpoint + retry_count。
    """

    model_config = {"extra": "forbid"}  # type: ignore[assignment]

    request_id: str  # 请求唯一标识
    review_package_id: str  # 审查包 ID
    developer_spec_hash: str  # DeveloperSpec SHA-256
    source_manifest_hash: str  # SourceManifest SHA-256
    sql_build_plan_hash: str  # SqlBuildPlan SHA-256
    sql_artifact_hash: str  # SqlArtifact SHA-256
    target: str  # 机器路由主字段——必须是 VALID_REVIEW_TARGETS 之一
    finding_type: str  # 细分原因——不参与路由
    comment: str  # 人类可读的审查意见
    suggested_resolution: str  # 建议的解决方案

    @staticmethod
    def validate_target(target: str) -> bool:
        """验证 target 是否为合法值。"""
        return target in VALID_REVIEW_TARGETS


# ════════════════════════════════════════════
# PackageInputs——组装器输入
# ════════════════════════════════════════════


class PackageInputs(StrictModel):
    """组装 Code Review Package 所需的全部输入。

    所有输入在组装前必须通过 hash 一致性验证。
    不保存完整结果集——ExecutionTrace 只存 row_count，
    ResultSummary 只存 sample_rows（前 20 行）。
    """

    request_id: str  # 请求唯一标识
    original_spec_md: str  # 原始 DeveloperSpec Markdown 文本
    parsed_spec: dict  # ParsedDeveloperSpec 序列化 dict
    source_manifest: dict  # SourceManifest 序列化 dict
    hypothesis: dict | None = None  # RelationshipHypothesis 序列化 dict（单表时为 None）
    sql_build_plan: dict  # SqlBuildPlan 序列化 dict
    sql_artifact: dict  # SqlArtifact 序列化 dict（含 CompiledSql）
    execution_trace: dict | None = None  # ExecutionTrace 序列化 dict
    result_summary: dict | None = None  # ResultSummary 序列化 dict
    data_transform_contract: dict  # DataTransformContractLite 序列化 dict
    open_questions: list[dict] = []  # 来自 Parser/SourceManifest 的 OpenQuestion 列表
    validation_questions: list[dict] = []  # 来自 Validator 的 OpenQuestion 列表
    perf_results: list[dict] = []  # PerfValidationResult 序列化列表
    retry_count: int = 0  # 返工轮次
