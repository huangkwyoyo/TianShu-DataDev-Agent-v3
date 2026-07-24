# case05 违章聚合 → borough 映射 → 效能打标

> 验证三 Transform 线性链——违章数据先按代码×日期聚合，再映射到 borough，最后产出执法效能标签

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.borough_enforcement_scorecard
  target_grain: [borough]
  summary: "违章聚合 → borough 映射 → 效能打标——三 Transform 线性链"

  time_range:
    start: "2026-03-25"
    end: "2026-03-31"

  source_tables:
    - name: gold.fact_parking_violations
      alias: fv
      row_count: ~958万
      role: fact
      key_columns:
        - name: violation_id
          type: bigint
          nullable: false
          unique: true
      business_columns:
        - name: issue_date_key
          type: integer
          nullable: false
        - name: violation_code
          type: varchar
          nullable: true
        - name: violation_county
          type: varchar
          nullable: true
        - name: registration_state
          type: varchar
          nullable: true
        - name: standard_fine_amount
          type: decimal(12,2)
          nullable: true
        - name: is_duplicate_summons
          type: boolean
          nullable: true

    - name: gold.dim_date
      alias: dd
      row_count: ~1.1万
      role: dim
      key_columns:
        - name: date_key
          type: integer
          nullable: false
          unique: true
      business_columns:
        - name: date
          type: timestamp
          nullable: false

  compute_steps:
    # ── T1：违章代码×日期聚合 ──
    - step_name: daily_violation
      source: input
      joins:
        - left_table: fv
          right_table: dd
          left_key: issue_date_key
          right_key: date_key
          join_type: INNER
      group_by:
        - violation_county
        - violation_code
        - date
      metrics:
        - metric_name: daily_count
          aggregation: COUNT
          input_column: violation_id
          alias: daily_count
        - metric_name: daily_fine
          aggregation: SUM
          input_column: standard_fine_amount
          alias: daily_fine
      output_alias: daily_violation

    # ── T2：county→borough 映射 + 重聚合 ──
    - step_name: borough_score
      source: daily_violation
      group_by:
        - violation_county
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: daily_count
          alias: total_violations
        - metric_name: total_fine
          aggregation: SUM
          input_column: daily_fine
          alias: total_fine
        - metric_name: code_count
          aggregation: COUNT_DISTINCT
          input_column: violation_code
          alias: code_count
      case_when:
        output_column: borough
        evaluation_phase: pre_aggregate
        typed_branches:
          - condition:
              node_type: COMPARE
              left: violation_county
              op: "="
              right:
                node_type: LITERAL
                value: "NY"
                data_type: string
            then_label: "曼哈顿"
          - condition:
              node_type: COMPARE
              left: violation_county
              op: "="
              right:
                node_type: LITERAL
                value: "K"
                data_type: string
            then_label: "布鲁克林"
          - condition:
              node_type: COMPARE
              left: violation_county
              op: "="
              right:
                node_type: LITERAL
                value: "Q"
                data_type: string
            then_label: "皇后区"
          - condition:
              node_type: COMPARE
              left: violation_county
              op: "="
              right:
                node_type: LITERAL
                value: "BX"
                data_type: string
            then_label: "布朗克斯"
          - condition:
              node_type: COMPARE
              left: violation_county
              op: "="
              right:
                node_type: LITERAL
                value: "R"
                data_type: string
            then_label: "史坦顿岛"
        else_value: "未知区域"
      output_alias: borough_score

    # ── T3：效能打标（纯透传 + CASE WHEN）──
    - step_name: enforcement_label
      source: borough_score
      group_by:
        - borough
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: total_violations
          alias: total_violations
        - metric_name: total_fine
          aggregation: SUM
          input_column: total_fine
          alias: total_fine
      case_when:
        output_column: enforcement_level
        evaluation_phase: post_aggregate
        typed_branches:
          - condition:
              node_type: COMPARE
              left: total_violations
              op: ">="
              right:
                node_type: LITERAL
                value: "10000"
                data_type: string
            then_label: "高效"
          - condition:
              node_type: COMPARE
              left: total_violations
              op: ">="
              right:
                node_type: LITERAL
                value: "5000"
                data_type: string
            then_label: "良好"
        else_value: "待提升"
      output_alias: enforcement_label

  output_columns:
    - name: borough
      type: varchar
    - name: total_violations
      type: bigint
    - name: total_fine
      type: decimal(18,2)
    - name: code_count
      type: bigint
    - name: enforcement_level
      type: varchar
---
```

## 多步 DAG 说明

本案使用 3 步 SqlProgram DAG：
1. daily_violation：违章数据按 county×code×date 预聚合（JOIN dim_date 过滤时间窗口）
2. borough_score：county→borough 映射（pre_agg CASE WHEN）+ 重聚合（SUM + COUNT_DISTINCT）
3. enforcement_label：按 borough 二次聚合 + 效能分级标签（post_agg CASE WHEN）
