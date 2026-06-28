# 测试：显式 Join 声明 → STRONG 证据等级

> 期望：Parser 解析后，FakeRelationshipPlanner 提取显式 Join 声明，
> 生成 STRONG JoinCandidate，进入 SqlBuildPlan JoinStep。

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "两表 Join 测试——显式声明 INNER JOIN"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: amount
          type: decimal
          nullable: false
        - name: dim_id
          type: bigint
          nullable: false

    - name: dim.test_dim
      alias: td
      row_count: ~1000
      role: dim
      key_columns:
        - name: dim_id
          type: bigint
          nullable: false
      business_columns:
        - name: dim_name
          type: varchar
          nullable: false

  joins:
    - left_table: tf
      right_table: td
      left_key: dim_id
      right_key: dim_id
      join_type: INNER

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: amount
      alias: total_amount

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: dim_name
      type: varchar
    - name: total_amount
      type: decimal

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-06-01"
---

# 业务目标

计算按日期和维度聚合的指标总和。
使用 dwd.test_fact 表与 dim.test_dim 表 INNER JOIN。
```
