/** TianShu DataDev Agent — 前端 API 客户端（仅调用内部 API，不写生产库） */

const BASE = '/api';

/** 通用 API 响应类型 */
export interface ApiError {
  error_code: string;
  message: string;
  field_ref: string | null;
}

/** 模板摘要 */
export interface TemplateSummary {
  template_id: string;
  name: string;
  description: string;
  category: string;
}

/** 模板完整定义 */
export interface TemplateFull extends TemplateSummary {
  markdown_template: string;
}

/** 模板列表响应 */
export interface TemplateListResponse {
  templates: TemplateSummary[];
  count: number;
}

/** OpenQuestion 摘要 */
export interface OpenQuestionSummary {
  question_id: string;
  source: string;
  description: string;
  blocking: boolean;
}

/** ParseWarning 摘要 */
export interface ParseWarningSummary {
  warning_id: string;
  message: string;
  severity: string;
}

/** 表声明摘要 */
export interface TableDeclSummary {
  table_alias: string;
  source_table: string;
  row_count: number | null;
  role: string | null;
  column_count: number;
  has_time_field: boolean;
  has_partition: boolean;
}

/** Join 声明摘要 */
export interface JoinDeclSummary {
  left_table: string;
  right_table: string;
  left_key: string;
  right_key: string;
  join_type: string;
}

/** 指标声明摘要 */
export interface MetricDeclSummary {
  metric_name: string;
  aggregation: string;
  input_column: string | null;
  alias: string;
}

/** 维度声明摘要 */
export interface DimensionDeclSummary {
  dimension_name: string;
  column_ref: string;
}

/** 时间范围摘要 */
export interface TimeRangeSummary {
  column_ref: string;
  start: string;
  end: string;
  inclusive: boolean;
}

/** 输出规格摘要 */
export interface OutputSpecSummary {
  columns: string[];
  grain: string[];
  sort_columns: string[];
  limit: number | null;
}

/** 富解析响应 */
export interface SpecRichResponse {
  request_id: string;
  spec_id: string;
  spec_hash: string;
  title: string;
  description: string;
  tables: TableDeclSummary[];
  metrics: MetricDeclSummary[];
  dimensions: DimensionDeclSummary[];
  joins: JoinDeclSummary[];
  time_range: TimeRangeSummary | null;
  output_spec: OutputSpecSummary;
  open_questions: OpenQuestionSummary[];
  parse_warnings: ParseWarningSummary[];
}

/** Plan 步骤摘要 */
export interface PlanStepSummary {
  step_type: string;
  step_id: string;
  description: string;
}

/** Join 证据条目 */
export interface JoinEvidenceItem {
  evidence_id: string;
  level: string;
  action: string;
  left_table: string;
  right_table: string;
  left_key_raw: string;
  right_key_raw: string;
  left_key_normalized: string;
  right_key_normalized: string;
  evidence_checks: string[];
  detail: string;
  evidence_chain_yaml: string;
}

/** 富 Plan 响应 */
export interface PlanRichResponse {
  request_id: string;
  spec_id: string;
  plan_id: string;
  step_count: number;
  step_types: string[];
  steps: PlanStepSummary[];
  multi_table: boolean;
  validation_passed: boolean;
  open_questions: OpenQuestionSummary[];
  join_evidence: JoinEvidenceItem[];
}

/** 执行追踪摘要 */
export interface ExecutionTraceSummary {
  trace_id: string;
  status: string;
  row_count: number;
  execution_time_ms: number;
  error_message: string | null;
}

/** 结果摘要 */
export interface ResultSummarySummary {
  summary_id: string;
  columns: string[];
  column_types: string[];
  row_count: number;
  null_counts: Record<string, number>;
  numeric_sums: Record<string, number>;
}

/** 富 Execute 响应 */
export interface ExecuteRichResponse {
  request_id: string;
  spec_id: string;
  plan_id: string;
  generated_sql: string;
  sql_sha256: string;
  compiler_version: string;
  execution_trace: ExecutionTraceSummary;
  result_summary: ResultSummarySummary;
  open_questions: OpenQuestionSummary[];
  llm_traces?: Record<string, LlmTraceNode> | null;  // LLM 调用追踪（可选）
}

/** 文件树节点 */
export interface ArtifactTreeNode {
  name: string;
  path: string;
  kind: 'file' | 'directory';
  sha256: string | null;
  children: ArtifactTreeNode[];
}

/** 富 Package 响应 */
export interface PackageRichResponse {
  request_id: string;
  package_id: string;
  created_at: string;
  artifact_count: number;
  spec_hash: string;
  retry_count: number;
  file_tree: ArtifactTreeNode[];
}

/** 健康检查响应 */
export interface HealthResponse {
  status: string;
  version: string;
  pipeline_ready: boolean;
}

/** 基础解析响应（Phase 4.5A 兼容） */
export interface SpecParseResponse {
  request_id: string;
  spec_id: string;
  spec_hash: string;
  title: string;
  table_count: number;
  metric_count: number;
  dimension_count: number;
  has_joins: boolean;
  has_time_range: boolean;
  open_question_count: number;
  warning_count: number;
  open_questions: OpenQuestionSummary[];
  parse_warnings: ParseWarningSummary[];
}

/** RunAll 响应（Phase 4.5A 兼容） */
export interface RunAllResponse {
  request_id: string;
  spec_id: string;
  plan_id: string;
  package_id: string;
  package_dir: string;
  execution_trace: ExecutionTraceSummary | null;
  result_summary: ResultSummarySummary | null;
  artifact_count: number;
  validation_passed: boolean;
  open_questions: OpenQuestionSummary[];
  pipeline_error?: { stage: string; error_type: string; error_message: string } | null;
  pipeline_stages?: { stage: string; status: string; error_type?: string; error_message?: string }[];
  llm_traces?: Record<string, LlmTraceNode> | null;  // LLM 调用追踪（可选）
}

/** 通用错误：提取 API 错误信息 */
async function handleError(resp: Response): Promise<ApiError> {
  try {
    const body = await resp.json();
    return {
      error_code: body.error_code || 'UNKNOWN',
      message: body.message || resp.statusText,
      field_ref: body.field_ref || null,
    };
  } catch {
    return {
      error_code: 'PARSE_ERROR',
      message: `HTTP ${resp.status}: ${resp.statusText}`,
      field_ref: null,
    };
  }
}

/** API 包装器——统一错误处理 */
async function apiGet<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE}${path}`);
  if (!resp.ok) {
    const err = await handleError(resp);
    throw err;
  }
  return resp.json();
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const err = await handleError(resp);
    throw err;
  }
  return resp.json();
}

// ── 公开 API 方法 ──

/** 获取模板列表 */
export function fetchTemplates(): Promise<TemplateListResponse> {
  return apiGet('/templates');
}

/** 获取指定模板完整定义 */
export function fetchTemplate(templateId: string): Promise<TemplateFull> {
  return apiGet(`/templates/${templateId}`);
}

/** 健康检查 */
export function healthCheck(): Promise<HealthResponse> {
  return apiGet('/health');
}

/** 富解析——完整结构化预览 */
export function parseSpecRich(markdownText: string): Promise<SpecRichResponse> {
  return apiPost('/spec/parse-rich', { markdown_text: markdownText });
}

/** 基础解析——Phase 4.5A 兼容 */
export function parseSpec(markdownText: string): Promise<SpecParseResponse> {
  return apiPost('/spec/parse', { markdown_text: markdownText });
}

/** 富 Plan——含步骤详情+Join 证据 */
export function buildPlanRich(
  markdownText: string,
  tableMapping?: Record<string, string>,
): Promise<PlanRichResponse> {
  return apiPost('/plan-rich', {
    markdown_text: markdownText,
    table_mapping: tableMapping || null,
  });
}

/** 富 Execute——含 SQL 文本 */
export function executeRich(
  markdownText: string,
  tableMapping?: Record<string, string>,
  tablePaths?: Record<string, string>,
): Promise<ExecuteRichResponse> {
  return apiPost('/execute-rich', {
    markdown_text: markdownText,
    table_mapping: tableMapping || null,
    table_paths: tablePaths || null,
  });
}

/** 全流程一键执行 */
export function runAll(
  markdownText: string,
  tableMapping?: Record<string, string>,
  tablePaths?: Record<string, string>,
): Promise<RunAllResponse> {
  return apiPost('/run-all', {
    markdown_text: markdownText,
    table_mapping: tableMapping || null,
    table_paths: tablePaths || null,
  });
}

/** 富 Package——含文件树 */
export function getPackageRich(requestId: string): Promise<PackageRichResponse> {
  return apiGet(`/package-rich/${requestId}`);
}

// ── Spark 管线验证 ──

/** Spark 单个阶段结果 */
export interface SparkStageItem {
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
}

/** Spark 验证响应 */
export interface SparkVerifyResponse {
  request_id: string;
  spark_stages: SparkStageItem[];
  overall_status: string;
  comparator_status: string;
  review_ready: boolean;
  package_id: string;
  errors: string[];
}

/** 触发 Spark 管线验证——传入 Pipeline Run-All 产出的 request_id */
export function sparkVerify(requestId: string): Promise<SparkVerifyResponse> {
  return apiPost<SparkVerifyResponse>('/spark/verify', { request_id: requestId });
}

// ── LLM 调用追踪 ──

/** LLM 节点调用追踪 */
export interface LlmTraceNode {
  node_name: string;
  model: string;
  token_usage: Record<string, number>;
  latency_ms: number;
  status: string;
  error_type: string | null;
}

// ── Spark 阶段独立触发 ──

/** Spark 单阶段请求 */
export interface SparkStageRequest {
  request_id: string;
}

/** Spark 单阶段响应 */
export interface SparkStageResponse {
  request_id: string;
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
  missing_dependencies: string[];
  errors: string[];
  spark_stages: SparkStageItem[];
  llm_traces: Record<string, LlmTraceNode> | null;
  result?: SparkStageResult | null;
}

/** Spark 阶段特有结果 */
export interface SparkStageResult {
  type: 'mapper' | 'developer' | 'compiler' | 'validator' | 'comparator' | 'physical_verify';
  steps?: { step_type: string; description: string }[];
  step_count?: number;
  plan_id?: string;
  message?: string;
  pyspark_code?: string;          // 编译器产的带注释 PySpark DSL 代码（transform 函数）
  standalone_pyspark?: string;    // 独立可执行脚本（含 SparkSession 引导、spark.read.csv）
  raw_hash?: string;
  is_valid?: boolean;
  status?: string;
  step_results?: { step_type: string; verdict: string }[];
  unsupported_types?: string[];
  uncovered_step_types?: string[];
  errors?: string[];
  skipped?: boolean;  // true 表示该阶段因环境/配置原因被跳过
  // ── Phase 8: DEVELOPER 阶段标注输出 ──
  annotations?: {
    step_id: string;
    intent: string;
    intent_detail: string;
    operation_summary: string;
  }[];
  annotation_count?: number;
  warnings?: {
    warning_id: string;
    severity: string;
    description: string;
  }[];
}

/** Spark 6 阶段 slug 列表 */
const SPARK_STAGES = ['map', 'develop', 'compile', 'validate', 'compare', 'physical-verify'] as const;

/** 触发单个 Spark 管线阶段 */
export function runSparkStage(
  requestId: string,
  stage: string,
): Promise<SparkStageResponse> {
  return apiPost<SparkStageResponse>(`/spark/${stage}`, { request_id: requestId });
}
