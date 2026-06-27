# 禁止宽松 3：指标引用了不在任何 input_table 中的字段

> 应抛出 ParseError(E004)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试指标引用未声明字段"

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
    - metric_name: wrong_metric
      aggregation: SUM
      input_column: nonexistent_column
      alias: wrong_metric

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: wrong_metric
      type: decimal

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：指标引用未声明字段

## 业务目标
Parser 应拒绝 nonexistent_column 的引用。

```
