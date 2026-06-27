# 允许宽松 6：字段注释中存在中文

> 归一化正常处理，注释不影响解析

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [用户ID, 日期]
  summary: "测试中文列注释"

  source_tables:
    - name: dwd.用户行为表
      alias: ua
      row_count: ~5000万
      role: fact
      key_columns:
        - name: 用户ID
          type: bigint
          nullable: false
          description: 用户唯一标识
      business_columns:
        - name: 订单金额
          type: decimal
          nullable: false
          description: 订单金额（元）
        - name: 下单时间
          type: timestamp
          nullable: false
          description: 用户下单的时间戳

  metrics:
    - metric_name: 总金额
      aggregation: SUM
      input_column: 订单金额
      alias: total_amount

  dimensions:
    - dimension_name: 用户ID
      column_ref: 用户ID

  output_columns:
    - name: 用户ID
      type: bigint
    - name: 总金额
      type: decimal

  time_range:
    column_ref: 下单时间
    start: "2025-01-01"
    end: "2025-01-31"
---

# 中文注释测试

## 业务目标
验证 Parser 能正确处理中文字段名和注释。

```
