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
 * 有效手工 Spec——引用 test_fact 表，确保在测试数据库中可执行
 * 对应后端 tests/fixtures/sql/test_fact.csv
 */
export const MANUAL_SUMMARY_SPEC = `
# 用户行为汇总表
## 数据源
- test_fact
## 输出
- 汇总表：每日用户行为汇总，按日期和事件类型分组，计数去重用户
`;

/**
 * 无效 Spec——引用不存在的数据源，触发 RUNTIME_FAIL 错误
 * 用于错误展示路径测试
 */
export const INVALID_SPEC = `
# 无效项目书
## 数据源
- not_existing_table
## 输出
- 明细表：读取不存在的数据源
`;
