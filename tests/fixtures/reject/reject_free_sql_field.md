# 禁止宽松 7：raw_sql、where_sql、expression: str 字段出现

> 应抛出 ParseError(E007)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试自由 SQL 字段被拒绝"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: amount
          type: decimal
          nullable: false

  metrics:
    - metric_name: total
      aggregation: SUM
      input_column: amount
      alias: total

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: total
      type: decimal
      raw_sql: "SELECT * FROM somewhere"
---

# 测试：自由 SQL 字段

## 业务目标
output_columns 中的 raw_sql 字段应触发 E007。

```
