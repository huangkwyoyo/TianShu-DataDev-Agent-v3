# NYC Borough 日期维度聚合——真实业务样本（Case 04）

> 数据源：NYC TLC 2026 Q1 行程 + taxi_zone 维度表
> 本文件用于 Phase 9A4 真实业务样本端到端验证——Case 04
>
> **B 类已知限制**：
> (1) 3 表 JOIN 链在 build_multi 中间投影有列缺失；
> (2) week_start_date 计算列需要 date_trunc 表达式支持。
> 当前使用 2 表 JOIN (ft + tz) + pickup_date_key 维度替代原始 3 表口径。

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.dws_borough_daily_trip_stats
  target_grain: [borough, pickup_date_key, trip_source]
  summary: "按 Borough + 日期聚合行程指标（Join 区域维度后分组统计）"

  source_tables:
    - name: fact_trips_sample
      alias: ft
      row_count: 3000
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
        - name: pickup_location_id
          type: integer
          nullable: true
        - name: total_amount
          type: decimal(12,2)
          nullable: true
        - name: distance_miles
          type: double
          nullable: true
        - name: passenger_count
          type: bigint
          nullable: true

    - name: dim_taxi_zone
      alias: tz
      row_count: 265
      role: dim
      key_columns:
        - name: location_id
          type: integer
          nullable: false
          unique: true
      business_columns:
        - name: borough
          type: varchar
          nullable: false
        - name: zone_name
          type: varchar
          nullable: false

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
    - metric_name: avg_distance
      aggregation: AVG
      input_column: distance_miles
      alias: avg_distance
    - metric_name: total_passengers
      aggregation: SUM
      input_column: passenger_count
      alias: total_passengers

  dimensions:
    - dimension_name: borough
      column_ref: borough
    - dimension_name: pickup_date_key
      column_ref: pickup_date_key
    - dimension_name: trip_source
      column_ref: trip_source

  joins:
    - left_table: ft
      right_table: tz
      left_key: pickup_location_id
      right_key: location_id
      join_type: LEFT

  output_columns:
    - name: borough
      type: varchar
    - name: pickup_date_key
      type: integer
    - name: trip_source
      type: varchar
    - name: trip_count
      type: bigint
    - name: total_revenue
      type: decimal(18,2)
    - name: avg_distance
      type: double
    - name: total_passengers
      type: bigint
---

# Borough 日期维度行程聚合分析

## 业务目标
将行程事实表与区域维度表关联，按行政区、日期和行程来源分组统计核心运营指标。

## 关联说明
- LEFT JOIN dim_taxi_zone ON pickup_location_id = location_id 获取 borough
- 按 borough + pickup_date_key + trip_source 三维度分组聚合
- 原始 Case 04 含 3 表 JOIN + week_start_date，当前为 2 表简化版

## 数据说明
- 数据来自 gold.fact_trips + dim_taxi_zone
- 测试使用 CSV 样本
```
