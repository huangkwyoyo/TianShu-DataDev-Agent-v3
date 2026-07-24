# case04 双分支事故/行程聚合 → 合流打标

> 验证 SparkPlan branches 合流——双分支并行聚合后 Join 合流，产出区域事故风险热力标签

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.zone_crash_risk_hotspot
  target_grain: [borough]
  summary: "双分支事故/行程聚合 → 合流打标——验证 SparkPlan branches 合流"

  time_range:
    start: "2026-03-25"
    end: "2026-03-31"

  source_tables:
    - name: silver.crash_detail
      alias: cd
      row_count: ~166万
      role: fact
      key_columns:
        - name: crash_id
          type: bigint
          nullable: false
          unique: true
      business_columns:
        - name: crash_at
          type: timestamp
          nullable: true
        - name: borough
          type: varchar
          nullable: true
        - name: persons_injured
          type: integer
          nullable: true
        - name: persons_killed
          type: integer
          nullable: true
        - name: is_location_missing
          type: boolean
          nullable: false

    - name: silver.trip_detail
      alias: td
      row_count: ~8032万
      role: fact
      key_columns:
        - name: trip_id
          type: varchar
          nullable: false
          unique: true
      business_columns:
        - name: pickup_at
          type: timestamp
          nullable: true
        - name: pickup_location_id
          type: integer
          nullable: true
        - name: fare_amount
          type: decimal(12,2)
          nullable: true
        - name: is_location_missing
          type: boolean
          nullable: false
        - name: is_distance_outlier
          type: boolean
          nullable: false

    - name: silver.taxi_zone
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
          nullable: true

  compute_steps:
    # ── 分支 A：事故聚合 ──
    - step_name: crash_agg
      source: input
      group_by:
        - borough
      metrics:
        - metric_name: crash_count
          aggregation: COUNT
          input_column: crash_id
          alias: crash_count
        - metric_name: total_injured
          aggregation: SUM
          input_column: persons_injured
          alias: total_injured
      output_alias: crash_agg

    # ── 分支 B：行程聚合（含 taxi_zone Join）──
    - step_name: trip_agg
      source: input
      joins:
        - left_table: td
          right_table: tz
          left_key: pickup_location_id
          right_key: location_id
          join_type: INNER
      group_by:
        - borough
      metrics:
        - metric_name: total_trips
          aggregation: COUNT
          input_column: trip_id
          alias: total_trips
      output_alias: trip_agg

    # ── 合流：事故 + 行程 → 打标 ──
    - step_name: risk_label
      source: [crash_agg, trip_agg]
      joins:
        - left_table: crash_agg
          right_table: trip_agg
          left_key: borough
          right_key: borough
          join_type: INNER
      group_by:
        - borough
      metrics:
        - metric_name: crash_count
          aggregation: SUM
          input_column: crash_count
          alias: crash_count
        - metric_name: total_trips
          aggregation: SUM
          input_column: total_trips
          alias: total_trips
      case_when:
        output_column: risk_level
        evaluation_phase: post_aggregate
        typed_branches:
          - condition:
              node_type: AND
              children:
                - node_type: COMPARE
                  left: crash_count
                  op: ">="
                  right:
                    node_type: LITERAL
                    value: "5"
                    data_type: string
                - node_type: COMPARE
                  left: total_trips
                  op: ">="
                  right:
                    node_type: LITERAL
                    value: "1000"
                    data_type: string
            then_label: "高危优先"
          - condition:
              node_type: COMPARE
              left: crash_count
              op: ">="
              right:
                node_type: LITERAL
                value: "5"
                data_type: string
            then_label: "高事故低流量"
          - condition:
              node_type: COMPARE
              left: total_trips
              op: ">="
              right:
                node_type: LITERAL
                value: "1000"
                data_type: string
            then_label: "高流量低事故"
        else_value: "常规巡查"
      output_alias: risk_label

  output_columns:
    - name: borough
      type: varchar
    - name: crash_count
      type: bigint
    - name: total_injured
      type: bigint
    - name: total_trips
      type: bigint
    - name: risk_level
      type: varchar
---
```

## 多步 DAG 说明

本案使用 3 步 SqlProgram DAG：
1. crash_agg：事故数据按 borough 预聚合（COUNT crash_id + SUM persons_injured）
2. trip_agg：行程数据按 borough 聚合（JOIN taxi_zone 获取 borough，COUNT trip_id）
3. risk_label：合流 Join + CASE WHEN 风险等级打标
