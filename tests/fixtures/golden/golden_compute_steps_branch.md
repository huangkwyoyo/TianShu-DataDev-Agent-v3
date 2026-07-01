# ComputeSteps 多分支聚合合流——黄金用例

> 验证 compute_steps 声明的双分支并行聚合 + Join 合流：两路各自聚合后 Join 合并

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.user_daily_avg_order_value
  target_grain: [dt, user_id]
  summary: "双分支聚合合流——先并行计算每人每日消费总额和订单数，再 Join 计算平均订单金额"

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
    - step_name: daily_amount_sum
      source: input
      group_by: [dt, user_id]
      metrics:
        - metric_name: daily_total_amount
          aggregation: SUM
          input_column: amount
          alias: daily_total_amount
      output_alias: amount_summary

    - step_name: daily_order_count
      source: input
      group_by: [dt, user_id]
      metrics:
        - metric_name: daily_order_cnt
          aggregation: COUNT
          input_column: order_id
          alias: daily_order_cnt
      output_alias: count_summary

    - step_name: merged_daily
      source: [daily_amount_sum, daily_order_count]
      group_by: [dt, user_id]
      metrics:
        - metric_name: avg_order_value
          aggregation: AVG
          input_column: daily_total_amount
          alias: avg_order_value
      output_alias: merged_summary

  joins:
    - left_table: daily_amount_sum
      right_table: daily_order_count
      left_key: user_id
      right_key: user_id
      join_type: INNER

  output_columns:
    - name: dt
      type: varchar
    - name: user_id
      type: bigint
    - name: avg_order_value
      type: decimal
---
# 双分支聚合合流测试

两个分支并行聚合（daily_amount_sum + daily_order_count），
合流步骤（merged_daily）Join 两个中间 _temp 表后做最终聚合。
```
