# 禁止宽松 6：输出列列表为空

> 应抛出 ParseError(E006)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试空输出列"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false

  metrics:
    - metric_name: cnt
      aggregation: COUNT
      input_column: id
      alias: cnt

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns: []

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：空输出列

```
