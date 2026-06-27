# 允许宽松 4：输出排序未声明

> Parser 生成 W004 警告，不拒绝

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试输出排序未声明时的 W004 警告"

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

  output_columns:
    - name: stat_date
      type: date
    - name: cnt
      type: bigint

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：输出排序未声明

## 业务目标
不声明 sort，Parser 应生成 W004 警告。

```
