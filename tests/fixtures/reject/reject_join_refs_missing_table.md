# 禁止宽松 5：Join 声明引用了不存在的表别名

> 应抛出 ParseError(E005)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试 Join 引用不存在表"

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

  joins:
    - left_table: tf
      right_table: nonexistent_table
      left_key: id
      right_key: id
      join_type: LEFT

  output_columns:
    - name: cnt
      type: bigint

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：Join 引用不存在表

## 业务目标
Join 的 right_table 'nonexistent_table' 不在 source_tables 中。

```
