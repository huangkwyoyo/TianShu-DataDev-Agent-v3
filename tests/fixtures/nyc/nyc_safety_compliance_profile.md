# 区域安全合规画像——Case 06 跨域融合

> 数据源：NYC TLC 2026 Q1 行程记录 + NYC 机动车碰撞事故数据 + 停车违章罚单数据
> 本文件用于 Phase 9A4 真实业务样本端到端验证——Case 06 多步 DAG 跨域融合

```markdown
---
spec:
  type: label_table
  target_table: ads.zone_safety_compliance_profile
  target_grain: [borough]
  summary: "区域安全合规画像——融合停车违章频率与事故严重度，产出区域风险等级标签"

  source_tables:
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

    - name: fact_trips_sample
      alias: zts
      row_count: 5000
      role: fact
      key_columns:
        - name: trip_id
          type: varchar
          nullable: false
      time_field: pickup_at
      business_columns:
        - name: pickup_location_id
          type: integer
          nullable: false
        - name: total_amount
          type: decimal(12,2)
          nullable: true
        - name: fare_amount
          type: decimal(12,2)
          nullable: true

    - name: fact_crashes_sample
      alias: fc
      row_count: 30
      role: fact
      time_field: crash_date_key
      key_columns:
        - name: crash_id
          type: bigint
          nullable: false
      business_columns:
        - name: borough
          type: varchar
          nullable: false
        - name: persons_injured
          type: integer
          nullable: false
        - name: persons_killed
          type: integer
          nullable: false
        - name: contributing_factor_1
          type: varchar
          nullable: true

    - name: dws_daily_parking_summary
      alias: dps
      row_count: 20
      role: fact
      time_field: date_key
      key_columns:
        - name: date_key
          type: integer
          nullable: false
      business_columns:
        - name: violation_county
          type: varchar
          nullable: false
        - name: violation_count
          type: bigint
          nullable: false
        - name: standard_fine_total
          type: decimal(18,2)
          nullable: false

    - name: dim_violation_type
      alias: vt
      row_count: 100
      role: dim
      key_columns:
        - name: violation_code
          type: varchar
          nullable: false
      business_columns:
        - name: violation_description
          type: varchar
          nullable: true

  # 分步计算声明——7 步 DAG（CTE 禁止：所有多步依赖使用 _temp_* 临时表）
  compute_steps:
    - step_name: crash_boro_agg
      source: input
      output_alias: crash_boro_agg
      description: "事故数据按 borough 预聚合——fact_crashes 仅有 borough 文本字段，无法直接键值 JOIN"
      group_by: [borough]
      metrics:
        - metric_name: total_crashes
          aggregation: COUNT
          input_column: crash_id
          alias: total_crashes
        - metric_name: total_injured
          aggregation: SUM
          input_column: persons_injured
          alias: total_injured
        - metric_name: total_killed
          aggregation: SUM
          input_column: persons_killed
          alias: total_killed
      time_range:
        column_ref: crash_date_key
        start: "20120101"
        end: "20261231"

    - step_name: parking_boro_agg
      source: input
      output_alias: parking_boro_agg
      description: "违章数据按 violation_county 代码映射到 borough 后聚合"
      group_by: [violation_county]
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: violation_count
          alias: total_violations
        - metric_name: avg_daily_fine
          aggregation: AVG
          input_column: standard_fine_total
          alias: avg_daily_fine

    - step_name: trip_boro_agg
      source: input
      output_alias: trip_boro_agg
      description: "行程数据——dim_taxi_zone JOIN fact_trips_sample 后按 borough 聚合"
      group_by: [borough]
      joins:
        - left_table: tz
          right_table: zts
          left_key: location_id
          right_key: pickup_location_id
          join_type: LEFT
      metrics:
        - metric_name: total_trip_count
          aggregation: COUNT
          input_column: trip_id
          alias: total_trip_count
        - metric_name: total_fare
          aggregation: SUM
          input_column: total_amount
          alias: total_fare

    - step_name: trip_crash_join
      source: [trip_boro_agg, crash_boro_agg]
      output_alias: trip_crash_join
      description: "行程 borough 聚合 LEFT JOIN 事故 borough 聚合——borough 字符串匹配（MEDIUM 证据）"
      group_by: [borough]
      joins:
        - left_table: _temp_trip_boro_agg
          right_table: _temp_crash_boro_agg
          left_key: borough
          right_key: borough
          join_type: LEFT

    - step_name: all_three_join
      source: [trip_crash_join, parking_boro_agg]
      output_alias: all_three_join
      description: "合并停车违章聚合——violation_county 代码经 CASE WHEN 映射为 borough 后关联"
      group_by: [borough]
      joins:
        - left_table: _temp_trip_crash_join
          right_table: _temp_parking_boro_agg
          left_key: borough
          right_key: borough
          join_type: LEFT

    - step_name: compute_ratios
      source: [all_three_join]
      output_alias: compute_ratios
      description: "归一化指标计算——每百万行程事故率、每千行程违章率"
      group_by: [borough]
      expressions:
        - name: crash_per_million_trips
          expression: "total_crashes * 1000000.0 / NULLIF(total_trip_count, 0)"
          type: double
        - name: violation_per_thousand_trips
          expression: "total_violations * 1000.0 / NULLIF(total_trip_count, 0)"
          type: double

    - step_name: risk_label
      source: [compute_ratios]
      output_alias: risk_label
      description: "CASE WHEN 风险等级标签 + 最终输出"
      case_when:
        output_column: safety_risk_level
        branches:
          - when: "crash_per_million_trips >= 800 OR violation_per_thousand_trips >= 15"
            then: "高风险"
          - when: "crash_per_million_trips < 300 AND violation_per_thousand_trips < 5"
            then: "低风险"
        else_label: "中风险"

  output_columns:
    - name: borough
      type: varchar
    - name: total_trip_count
      type: bigint
    - name: total_crashes
      type: bigint
    - name: total_injured
      type: bigint
    - name: total_killed
      type: bigint
    - name: total_violations
      type: bigint
    - name: avg_daily_fine
      type: decimal(18,2)
    - name: crash_per_million_trips
      type: double
    - name: violation_per_thousand_trips
      type: double
    - name: safety_risk_level
      type: varchar
---
```

## 多步 DAG 说明
本案使用 7 步 SqlProgram DAG（CTE 禁止——所有多步依赖使用 _temp_* 临时表）：
1. crash_boro_agg：事故数据按 borough 预聚合（fact_crashes 仅有 borough 文本字段）
2. parking_boro_agg：违章数据按 county 代码预聚合
3. trip_boro_agg：行程数据按 borough 聚合（JOIN tz + zts）
4. trip_crash_join：行程 LEFT JOIN 事故（borough 字符串匹配——MEDIUM 证据）
5. all_three_join：合并违章数据
6. compute_ratios：归一化指标计算
7. risk_label：CASE WHEN 风险等级标签

## 硬编码阈值（替代 PERCENTILE_CONT——本轮不实现）
- crash_per_million_trips >= 800 → 高事故密度
- violation_per_thousand_trips >= 15 → 高违章密度
- 两项均低于中位数估计（300 / 5）→ 低风险
- 一项高于上四分位估计（800 / 15）→ 高风险
- 两项均高于上四分位估计 → extreme

## 已知限制
- top_crash_factor 留空——ROW_NUMBER 子查询本轮不实现
- PERCENTILE_CONT 由硬编码阈值替代
- violation_county 代码映射为 5 个已知代码（QN/BK/NY/BX/ST）
