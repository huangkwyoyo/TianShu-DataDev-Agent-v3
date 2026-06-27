# 禁止宽松 4：两个表使用相同别名

> 应抛出 ParseError(E005)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试重复表别名"

  source_tables:
    - name: dwd.test_fact
      alias: t
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
    - name: dim.test_dim
      alias: t
      row_count: ~1000
      role: dim
      key_columns:
        - name: dim_id
          type: bigint
          nullable: false

  metrics:
    - metric_name: cnt
      aggregation: COUNT
      input_column: id
      alias: cnt

  dimensions:
    - dimension_name: dim_id
      column_ref: dim_id

  output_columns:
    - name: cnt
      type: bigint

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：重复表别名

## 业务目标
两个表都用了 't' 作为别名，应被拒绝。

```
