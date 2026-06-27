# 允许宽松 1：字段类型未声明

> Parser 不拒绝，由 SourceManifest 从 SchemaRegistry 补充

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试字段类型未声明时的允许宽松行为"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          nullable: false
          unique: true
        - name: stat_date
          nullable: false
      business_columns:
        - name: amount
        - name: status

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: amount
      alias: total_amount

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: total_amount
      type: decimal

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：字段类型未声明

## 业务目标
验证 amount 和 status 字段的类型未声明（W001），
由 SchemaRegistry 补充。

```
