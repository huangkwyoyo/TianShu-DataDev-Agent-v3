# 允许宽松 2：时间范围未指定

> Parser 生成 W002 警告，不阻断

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试时间范围未指定时的 W002 警告"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      time_field: event_time
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: event_time
          type: timestamp
          nullable: false

  metrics:
    - metric_name: cnt
      aggregation: COUNT
      input_column: id
      alias: cnt

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: cnt
      type: bigint
---

# 测试：时间范围未指定

## 业务目标
源表有时间字段但未指定时间范围，Parser 应生成 W002 警告。

```
