# NYC 按行程来源每日聚合——真实业务样本

> 数据源：NYC TLC 2026 Q1 行程记录（gold.fact_trips）
> 样本行数：~10,000 行分层抽样（fhvhv/yellow/fhv/green）
> 本文件用于 Phase 9A4 真实业务样本端到端验证

```markdown
---
spec:
  type: aggregate_table
  target_table: gold.dws_trip_source_daily
  target_grain: [trip_source, pickup_date_key]
  summary: "按行程来源统计每日行程数、收入总额与平均费用"

  source_tables:
    - name: fact_trips_sample
      alias: ft
      row_count: ~1万
      role: fact
      time_field: pickup_at
      key_columns:
        - name: trip_id
          type: varchar
          nullable: false
      business_columns:
        - name: trip_source
          type: varchar
          nullable: false
        - name: pickup_date_key
          type: integer
          nullable: false
        - name: total_amount
          type: decimal(12,2)
          nullable: true
        - name: fare_amount
          type: decimal(12,2)
          nullable: true
        - name: passenger_count
          type: bigint
          nullable: true
        - name: distance_miles
          type: double
          nullable: true

  time_range:
    column_ref: pickup_date_key
    start: "20260101"
    end: "20260331"

  metrics:
    - metric_name: trip_count
      aggregation: COUNT
      input_column: trip_id
      alias: trip_count
    - metric_name: total_revenue
      aggregation: SUM
      input_column: total_amount
      alias: total_revenue
    - metric_name: avg_fare
      aggregation: AVG
      input_column: fare_amount
      alias: avg_fare
    - metric_name: total_passengers
      aggregation: SUM
      input_column: passenger_count
      alias: total_passengers
    - metric_name: avg_distance
      aggregation: AVG
      input_column: distance_miles
      alias: avg_distance

  dimensions:
    - dimension_name: trip_source
      column_ref: trip_source
    - dimension_name: pickup_date_key
      column_ref: pickup_date_key

  output_columns:
    - name: trip_source
      type: varchar
    - name: pickup_date_key
      type: integer
    - name: trip_count
      type: bigint
    - name: total_revenue
      type: decimal(18,2)
    - name: avg_fare
      type: decimal(12,2)
    - name: total_passengers
      type: bigint
    - name: avg_distance
      type: double
---

# 按行程来源每日聚合

## 业务目标
按行程来源类型（yellow/green/fhv/fhvhv）和日期聚合，统计每日行程量、总营收、平均车费、总载客数和平均距离。

## 业务口径
- `trip_count`：该来源该日所有行程计数
- `total_revenue`：total_amount 求和，不含 NULL 行
- `avg_fare`：fare_amount 均值，不含 NULL 行
- `total_passengers`：passenger_count 求和，NULL 不计入
- `avg_distance`：distance_miles 均值，不含 NULL 行

## 数据说明
- 数据来自 gold.fact_trips，约 8032 万行
- fhvhv 占 78%、yellow 14%、fhv 8%、green 0.2%
- 测试使用分层抽样 CSV 样本（~10,000 行）
```
