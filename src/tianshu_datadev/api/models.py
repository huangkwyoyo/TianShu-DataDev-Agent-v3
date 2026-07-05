"""Phase 4.5A — REST API 请求/响应 Pydantic 模型。

所有模型继承 StrictModel（extra="forbid"），拒绝未知字段。
API 响应只返回结构化摘要和 artifact 引用，不返回完整内部对象。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import StrictModel

# 统一错误响应
# ════════════════════════════════════════════


class ErrorDetail(StrictModel):
    """结构化错误详情——所有 4xx/5xx 响应的统一格式。"""

    error_code: str  # 错误码（如 "E001"、"VALIDATION_ERROR"、"NOT_FOUND"）
    message: str  # 人类可读的错误描述
    field_ref: str | None = None  # 引发错误的字段引用（可选）


# ════════════════════════════════════════════
# 请求体
# ════════════════════════════════════════════


class ParseSpecRequest(StrictModel):
    """POST /api/spec/parse 请求体。"""

    markdown_text: str  # DeveloperSpec Markdown 全文


class PlanRequest(StrictModel):
    """POST /api/plan 请求体。"""

    markdown_text: str  # DeveloperSpec Markdown 全文
    table_mapping: dict[str, str] | None = None  # table_ref → 物理表名（可选）


class ExecuteRequest(StrictModel):
    """POST /api/execute 请求体。

    dry_run 始终为 true——不提供生产执行入口。
    """

    markdown_text: str  # DeveloperSpec Markdown 全文
    table_mapping: dict[str, str] | None = None  # table_ref → 物理表名
    table_paths: dict[str, str] | None = None  # 物理表名 → CSV 文件路径


class RunAllRequest(StrictModel):
    """POST /api/run-all 请求体——全流程一键执行。

    dry_run 始终为 true——不提供生产写入开关。
    """

    markdown_text: str  # DeveloperSpec Markdown 全文
    table_mapping: dict[str, str] | None = None  # table_ref → 物理表名
    table_paths: dict[str, str] | None = None  # 物理表名 → CSV 文件路径


# ════════════════════════════════════════════
# 响应体——摘要模型
# ════════════════════════════════════════════


class OpenQuestionSummary(StrictModel):
    """OpenQuestion 摘要——不返回 resolution 等内部字段。"""

    question_id: str
    source: str
    description: str
    blocking: bool


class ParseWarningSummary(StrictModel):
    """ParseWarning 摘要。"""

    warning_id: str
    message: str
    severity: str


class SpecParseResponse(StrictModel):
    """POST /api/spec/parse 响应——仅摘要，不含完整 ParsedDeveloperSpec。"""

    request_id: str  # 请求唯一标识（基于 spec_hash）
    spec_id: str  # DeveloperSpec 标识
    spec_hash: str  # 规范化哈希
    title: str  # 标题
    table_count: int  # 输入表数量
    metric_count: int  # 指标数量
    dimension_count: int  # 维度数量
    has_joins: bool  # 是否有显式 Join 声明
    has_time_range: bool  # 是否有时间范围声明
    open_question_count: int  # OpenQuestion 数量
    warning_count: int  # ParseWarning 数量
    open_questions: list[OpenQuestionSummary] = []  # OpenQuestion 摘要列表
    parse_warnings: list[ParseWarningSummary] = []  # ParseWarning 摘要列表


class PlanResponse(StrictModel):
    """POST /api/plan 响应——Plan 摘要 + Validator 校验结果。"""

    request_id: str  # 请求唯一标识
    spec_id: str  # DeveloperSpec 标识
    plan_id: str  # SqlBuildPlan 标识
    step_count: int  # Step 总数
    step_types: list[str]  # 步骤类型列表 ["scan", "filter", ...]
    multi_table: bool  # 是否多表
    validation_passed: bool  # Validator 是否通过
    open_questions: list[OpenQuestionSummary] = []  # Validator/Builder 问题摘要


class ExecutionTraceSummary(StrictModel):
    """ExecutionTrace 摘要——不包含完整 generated_sql。"""

    trace_id: str
    status: str  # ExecutionStatus 字符串值
    row_count: int
    execution_time_ms: float
    error_message: str | None = None


class ResultSummarySummary(StrictModel):
    """ResultSummary 的摘要——不含 sample_rows（不返回完整结果集）。"""

    summary_id: str
    columns: list[str]  # 列名列表
    column_types: list[str]  # 列类型列表
    row_count: int  # 结果行数
    null_counts: dict[str, int]  # 各列 NULL 计数
    numeric_sums: dict[str, float]  # 数值列求和


class ExecuteResponse(StrictModel):
    """POST /api/execute 响应——编译+执行摘要。

    成功路径返回 execution_trace + result_summary；
    失败路径（Validator 阻断 / 编译失败 / RUNTIME_FAIL）返回 pipeline_error + pipeline_stages，
    且 execution_trace / result_summary 为 None。
    """

    request_id: str  # 请求唯一标识
    spec_id: str  # DeveloperSpec 标识
    plan_id: str  # SqlBuildPlan 标识
    execution_trace: ExecutionTraceSummary | None = None  # 执行追踪摘要（失败时为 None）
    result_summary: ResultSummarySummary | None = None  # 结果摘要（失败时为 None）
    sql_sha256: str  # 生成的 SQL 哈希（确定性校验用）
    compiler_version: str  # Compiler 版本
    validation_passed: bool = False  # Validator 是否通过（透传给调用方判断链路状态）
    open_questions: list[OpenQuestionSummary] = []  # Builder/Validator 问题摘要
    pipeline_error: dict | None = None  # 失败时的错误信息（stage + error_type + error_message）
    pipeline_stages: list[dict] = []  # 失败时的各阶段状态标记


class PackageResponse(StrictModel):
    """GET /api/package/{request_id} 响应——ReviewPackage 摘要。"""

    request_id: str  # 请求唯一标识
    package_id: str  # Package 标识
    created_at: str  # 创建时间戳
    artifacts: list[dict]  # Artifact 引用列表 [{"path": "...", "sha256": "..."}]
    artifact_count: int  # Artifact 总数
    spec_hash: str  # DeveloperSpec 哈希
    retry_count: int  # 返工轮次


class RunAllResponse(StrictModel):
    """POST /api/run-all 响应——全流程一键执行结果摘要。

    成功路径返回 execution_trace + result_summary + package_id；
    失败路径（Validator 阻断 / RUNTIME_FAIL / 编译失败）返回 pipeline_error + pipeline_stages，
    且 execution_trace / result_summary 为 None、package_id 为空字符串。
    """

    request_id: str  # 请求唯一标识
    spec_id: str  # DeveloperSpec 标识
    plan_id: str  # SqlBuildPlan 标识
    package_id: str = ""  # ReviewPackage 标识（失败时为空字符串）
    package_dir: str = ""  # Package 输出目录（失败时为空字符串）
    execution_trace: ExecutionTraceSummary | None = None  # 执行追踪摘要（失败时为 None）
    result_summary: ResultSummarySummary | None = None  # 结果摘要（失败时为 None）
    artifact_count: int = 0  # Package 中 artifact 总数
    validation_passed: bool = False  # Validator 是否通过（透传给调用方判断链路状态）
    open_questions: list[OpenQuestionSummary] = []  # Builder/Validator 问题摘要
    pipeline_error: dict | None = None  # 失败时的错误信息（stage + error_type + error_message）
    pipeline_stages: list[dict] = []  # 失败时的各阶段状态标记


# ════════════════════════════════════════════
# Phase 4.5B — 前端 SPA 专用响应模型
# ════════════════════════════════════════════


class TemplateItem(StrictModel):
    """DeveloperSpec 模板定义——供前端模板选择器使用。"""

    template_id: str  # 模板唯一标识
    name: str  # 模板名称（如 "汇总表"）
    description: str  # 模板用途说明
    category: str  # 分类："aggregation" | "label" | "multi_step"
    markdown_template: str  # 预填的 DeveloperSpec Markdown 全文


class TemplateListResponse(StrictModel):
    """GET /api/templates 响应——模板列表。"""

    templates: list[TemplateItem]
    count: int


class JoinEvidenceItem(StrictModel):
    """Join 推理证据条目——供前端 Join 推理面板展示 STRONG/MEDIUM/WEAK/NONE。"""

    evidence_id: str  # 证据唯一标识
    level: str  # STRONG | MEDIUM | WEAK | NONE
    action: str  # AUTO_ADOPT | HUMAN_CONFIRM | REJECT_BLOCKING | REJECT_SILENT
    left_table: str  # 左表别名
    right_table: str  # 右表别名
    left_key_raw: str  # 左键原始字段名
    right_key_raw: str  # 右键原始字段名
    left_key_normalized: str  # 左键归一化字段名
    right_key_normalized: str  # 右键归一化字段名
    evidence_checks: list[str] = []  # 逐条检查结果
    detail: str = ""  # 评级理由
    evidence_chain_yaml: str = ""  # 完整证据链 YAML 文本


class TableDeclSummary(StrictModel):
    """表声明摘要——供前端解析预览使用。"""

    table_alias: str  # 表别名
    source_table: str  # 物理表名
    row_count: int | None = None  # 预估行数
    role: str | None = None  # fact | dim
    column_count: int = 0  # 声明列数
    has_time_field: bool = False  # 是否有时间字段
    has_partition: bool = False  # 是否有分区字段


class JoinDeclSummary(StrictModel):
    """Join 声明摘要——供前端解析预览使用。"""

    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: str


class MetricDeclSummary(StrictModel):
    """指标声明摘要。"""

    metric_name: str
    aggregation: str
    input_column: str | None = None
    alias: str


class DimensionDeclSummary(StrictModel):
    """维度声明摘要。"""

    dimension_name: str
    column_ref: str


class OutputSpecSummary(StrictModel):
    """输出规格摘要。"""

    columns: list[str]
    grain: list[str]
    sort_columns: list[str] = []
    limit: int | None = None


class TimeRangeSummary(StrictModel):
    """时间范围摘要。"""

    column_ref: str
    start: str
    end: str
    inclusive: bool = True


class SpecRichResponse(StrictModel):
    """前端解析预览完整响应——含全部结构化解析结果。

    比 SpecParseResponse 更丰富，包含表、字段、指标、维度、
    Join、时间范围等完整声明，供前端渲染结构化预览面板。
    """

    request_id: str
    spec_id: str
    spec_hash: str
    title: str
    description: str
    tables: list[TableDeclSummary] = []
    metrics: list[MetricDeclSummary] = []
    dimensions: list[DimensionDeclSummary] = []
    joins: list[JoinDeclSummary] = []
    time_range: TimeRangeSummary | None = None
    output_spec: OutputSpecSummary
    open_questions: list[OpenQuestionSummary] = []
    parse_warnings: list[ParseWarningSummary] = []


class PlanStepSummary(StrictModel):
    """SqlBuildPlan 单个步骤摘要——供前端逐步骤展示。"""

    step_type: str  # scan | filter | join | aggregate | project | case_when | sort | limit
    step_id: str
    description: str  # 人类可读的步骤描述


class PlanRichResponse(StrictModel):
    """前端 Plan 面板完整响应——含步骤列表 + Join 证据链。"""

    request_id: str
    spec_id: str
    plan_id: str
    step_count: int
    step_types: list[str]
    steps: list[PlanStepSummary] = []  # 逐步骤详情
    multi_table: bool
    validation_passed: bool
    open_questions: list[OpenQuestionSummary] = []
    join_evidence: list[JoinEvidenceItem] = []  # Join 推理证据


class ExecuteRichResponse(StrictModel):
    """前端 Execute 面板完整响应——含 SQL 文本和执行结果。

    成功路径返回 execution_trace + result_summary + generated_sql；
    失败路径（Validator 阻断 / RUNTIME_FAIL / 编译失败）返回 pipeline_error + pipeline_stages，
    且 execution_trace / result_summary 为 None。
    """

    request_id: str
    spec_id: str
    plan_id: str
    generated_sql: str  # 实际执行的 SQL 文本（失败时为空字符串）
    sql_sha256: str
    compiler_version: str
    execution_trace: ExecutionTraceSummary | None = None  # 执行追踪摘要（失败时为 None）
    result_summary: ResultSummarySummary | None = None  # 结果摘要（失败时为 None）
    validation_passed: bool = False  # Validator 是否通过（透传给调用方判断链路状态）
    open_questions: list[OpenQuestionSummary] = []
    pipeline_error: dict | None = None  # 失败时的错误信息（stage + error_type + error_message）
    pipeline_stages: list[dict] = []  # 失败时的各阶段状态标记


class ArtifactTreeNode(StrictModel):
    """Review Package 文件树节点——供前端文件树渲染。"""

    name: str  # 文件名或目录名
    path: str  # 相对路径
    kind: str  # "file" | "directory"
    sha256: str | None = None  # 文件 SHA-256（目录为 None）
    children: list["ArtifactTreeNode"] = []  # 子节点（仅目录）


class PackageRichResponse(StrictModel):
    """前端 Package 面板完整响应——含文件树结构。"""

    request_id: str
    package_id: str
    created_at: str
    artifact_count: int
    spec_hash: str
    retry_count: int
    file_tree: list[ArtifactTreeNode] = []  # 文件树根节点列表


class RunAllRichResponse(StrictModel):
    """前端全流程一键执行富响应——合并 PlanRich + ExecuteRich + PackageRich 全部信息。

    前端一次 POST /api/run-all-rich 即可获得：
    - 步骤摘要 + Join 证据（PlanRichResponse）
    - SQL 文本 + 执行追踪（ExecuteRichResponse）
    - 文件树（PackageRichResponse）
    - 基础结果（RunAllResponse）
    """

    request_id: str
    spec_id: str
    plan_id: str
    package_id: str
    package_dir: str
    execution_trace: ExecutionTraceSummary
    result_summary: ResultSummarySummary
    artifact_count: int
    open_questions: list[OpenQuestionSummary] = []
    # 富 Execute 字段
    generated_sql: str = ""
    sql_sha256: str = ""
    compiler_version: str = ""
    # 富 Plan 字段
    steps: list[PlanStepSummary] = []
    join_evidence: list[JoinEvidenceItem] = []
    # 富 Package 字段
    file_tree: list[ArtifactTreeNode] = []


class HealthResponse(StrictModel):
    """GET /api/health 响应——API 健康检查。"""

    status: str  # "ok" | "degraded"
    version: str
    pipeline_ready: bool


# ════════════════════════════════════════════
# Spark 管线验证——POST /api/spark/verify
# ════════════════════════════════════════════


class SparkVerifyRequest(StrictModel):
    """POST /api/spark/verify 请求体——传入 Pipeline 产出的 request_id。"""

    request_id: str  # Pipeline run_all 返回的 request_id


class SparkStageItem(StrictModel):
    """Spark 管线单个阶段结果——供前端 PipelineStageIndicator 渲染。"""

    stage: str  # 阶段名（MAPPER / DEVELOPER / COMPILER / VALIDATOR / COMPARATOR / PHYSICAL_VERIFIER）
    status: str  # 阶段状态（"ok" / "failed" / "skipped"）


class SparkVerifyResponse(StrictModel):
    """POST /api/spark/verify 响应——Spark 管线 6 阶段结果 + REVIEW_READY 判定。

    成功路径返回 spark_stages + review_ready；
    失败路径（artifacts 缺失/不完整/执行异常）通过 HTTP 错误码返回。
    """

    request_id: str  # 回显请求的 request_id
    spark_stages: list[SparkStageItem] = []  # Spark 6 阶段结果列表
    overall_status: str = ""  # SparkPipelineStatus 字符串值
    comparator_status: str = ""  # 对比器状态字符串
    review_ready: bool = False  # REVIEW_READY 判定——所有关键阶段通过的标志
    package_id: str = ""  # SparkReviewPackage ID
    errors: list[str] = []  # 错误信息列表（成功时为空）
