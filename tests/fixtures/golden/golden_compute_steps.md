# ComputeSteps 多步聚合链——黄金用例

> 验证 compute_steps 声明的两步聚合链：daily_sum → monthly_avg，通过 _temp 表串联

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.monthly_avg_daily
  target_grain: [month, user_id]
  summary: "先按天汇总订单金额，再按月对天汇总求平均"

  source_tables:
    - name: dwd.order_fact
      alias: o
      row_count: ~500万
      role: fact
      time_field: order_time
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
      business_columns:
        - name: user_id
          type: bigint
          nullable: false
        - name: amount
          type: decimal
          nullable: true
        - name: order_time
          type: timestamp
          nullable: false

  compute_steps:
    - step_name: daily_agg
      source: input
      group_by: [dt, user_id]
      metrics:
        - metric_name: daily_amount
          aggregation: SUM
          input_column: amount
          alias: daily_amount
      output_alias: daily_summary

    - step_name: monthly_avg
      source: daily_agg
      group_by: [month, user_id]
      metrics:
        - metric_name: avg_daily_amount
          aggregation: AVG
          input_column: daily_amount
          alias: avg_daily_amount
      output_alias: monthly_summary

  output_columns:
    - name: month
      type: varchar
    - name: user_id
      type: bigint
    - name: avg_daily_amount
      type: decimal
---
# 多步聚合链测试

先按天汇总订单金额(daily_agg)，再按月对天汇总求平均(monthly_avg)，两步通过 _temp 表串联。
```