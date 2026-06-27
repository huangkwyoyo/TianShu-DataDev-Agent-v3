# 允许宽松 3：Join 未显式声明

> Parser 不要求，留给 RelationshipHypothesis 推理

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试 Join 未显式声明——Parser 不拒绝"

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
    - name: dim.test_dim
      alias: td
      row_count: ~1000
      role: dim
      key_columns:
        - name: dim_id
          type: bigint
          nullable: false

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: amount
      alias: total_amount

  dimensions:
    - dimension_name: dim_id
      column_ref: dim_id

  output_columns:
    - name: dim_id
      type: bigint
    - name: total_amount
      type: decimal

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：Join 未显式声明

## 业务目标
不声明 joins，由 RelationshipHypothesis 推理。

```
