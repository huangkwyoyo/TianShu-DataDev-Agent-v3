# NYC Borough Top5 上车区域——真实业务样本（Case 05）

> 数据源：NYC TLC 2026 Q1 行程 + taxi_zone 维度表
> 本文件用于 Phase 9A4 真实业务样本端到端验证——Case 05
>
> **注意**：使用 ROW_NUMBER 窗口函数 + INNER JOIN dim_taxi_zone。
> rank_by_revenue（第二个排名）和 WHERE rank<=5 的 TopN 过滤为 B 类已知限制。

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.borough_top5_pickup_zones
  target_grain: [borough, pickup_location_id, zone_name]
  summary: "各 Borough 上车量最高的 Top 5 区域——使用 ROW_NUMBER 窗口函数分组排序"

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
        - name: pickup_date_key
          type: integer
          nullable: false
        - name: pickup_location_id
          type: integer
          nullable: true
        - name: total_amount
          type: decimal(12,2)
          nullable: true

    - name: dim_taxi_zone
      alias: tz
      row_count: 265
      role: dim
      key_columns:
        - name: location_id
          type: integer
          nullable: false
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

  dimensions:
    - dimension_name: borough
      column_ref: borough
    - dimension_name: pickup_location_id
      column_ref: pickup_location_id
    - dimension_name: zone_name
      column_ref: zone_name

  joins:
    - left_table: ft
      right_table: tz
      left_key: pickup_location_id
      right_key: location_id
      join_type: INNER

  output_columns:
    - name: borough
      type: varchar
    - name: pickup_location_id
      type: integer
    - name: zone_name
      type: varchar
    - name: trip_count
      type: bigint
    - name: total_revenue
      type: decimal(18,2)
    - name: rank_by_count
      type: integer
      window_hint:
        metric_name: rank_by_count
        window_function: ROW_NUMBER
        input_column: ""
        partition_by: [borough]
        order_by: [trip_count DESC]
        alias: rank_by_count
        confidence: high
---

# 各行政区上车量 Top 5 区域排名

## 业务目标
按 Borough 分组统计每个上车区域的行程数和总收入，使用 ROW_NUMBER 窗口函数按 trip_count 降序排名。

## 窗口函数说明
- ROW_NUMBER() OVER (PARTITION BY borough ORDER BY trip_count DESC) AS rank_by_count

## 已知限制
- rank_by_revenue（第二个窗口排名）+ WHERE rank_by_count <= 5 TopN 过滤为 B 类限制
- 当前验证单一 ROW_NUMBER 窗口函数在 JOIN 后的 Pipeline 集成

## 数据说明
- INNER JOIN dim_taxi_zone 排除无区域信息的行程
- 测试使用 CSV 样本
```
