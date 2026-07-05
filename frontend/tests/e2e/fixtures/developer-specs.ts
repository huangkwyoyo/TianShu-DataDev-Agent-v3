/**
 * Phase 9C E2E 测试数据——两类分离：
 * 1. 模板路径用例——通过 TemplateSelector 真实选择（仅用于 UI 验证）
 * 2. 手工编辑路径用例——在 SpecEditor 中粘贴（用于数据流验证）
 *
 * 注意：后端生产模板引用 dwd.* 表，测试数据库只有 test_fact。
 * 模板路径测试只验证 UI 选择行为，不执行 Run-All。
 */

/** 模板名称——与后端 templates.py 中的 name 字段严格一致 */
export const TEMPLATE_NAMES = {
  /** 汇总表模板——单表聚合，后端 template_id: tpl_aggregation */
  SUMMARY: '汇总表',
} as const;

/**
 * 有效手工 Spec——引用 test_fact 表，使用 Parser 所需的 fenced code block 格式。
 * 注意：E2E 环境中未配置 table_paths，Run-All 会在 execute 阶段因 DuckDB 找不到
 * test_fact 表而返回 pipeline_error，但 request_id 仍被正确设置。
 */
export const MANUAL_SUMMARY_SPEC = '```markdown\n' +
'---\n' +
'spec:\n' +
'  type: aggregate_table\n' +
'  target_table: test.aggregated\n' +
'  target_grain: [stat_date]\n' +
'  summary: "手工 Spec——按日期分组计数"\n' +
'  source_tables:\n' +
'    - name: test_fact\n' +
'      alias: tf\n' +
'      row_count: ~10万\n' +
'      role: fact\n' +
'      time_field: event_time\n' +
'      key_columns:\n' +
'        - name: id\n' +
'          type: bigint\n' +
'          nullable: false\n' +
'      business_columns:\n' +
'        - name: event_time\n' +
'          type: timestamp\n' +
'          nullable: false\n' +
'  metrics:\n' +
'    - metric_name: cnt\n' +
'      aggregation: COUNT\n' +
'      input_column: id\n' +
'      alias: cnt\n' +
'  dimensions:\n' +
'    - dimension_name: stat_date\n' +
'      column_ref: stat_date\n' +
'  output_columns:\n' +
'    - name: stat_date\n' +
'      type: date\n' +
'    - name: cnt\n' +
'      type: bigint\n' +
'---\n' +
'# 用户行为汇总表\n' +
'## 业务目标\n' +
'测试手工 Spec——按日期统计事件数。\n' +
'```\n';

/**
 * 无效 Spec——不包含 fenced code block，触发 Parser 阶段的 ParseError。
 * 用于错误展示路径测试（PipelineStageIndicator dot-error 态）。
 */
export const INVALID_SPEC = '# 无效项目书\n' +
'## 数据源\n' +
'- not_existing_table\n' +
'## 输出\n' +
'- 明细表：读取不存在的数据源\n';
