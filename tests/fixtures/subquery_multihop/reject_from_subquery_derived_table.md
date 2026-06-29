# 派生表子查询拒绝（FROM 子查询）

> Phase 3C 不支持——应被架构边界断言拒绝
> 预期拒绝码：ParseError E009（不支持的 SQL 模式）或 Validator UNSUPPORTED_STEP_TYPE

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.order_agg_daily
  target_grain: [stat_date]
  summary: "派生表子查询——FROM 子句中引用子查询聚合结果"

  source_tables:
    - name: dwd.order_detail
      alias: o
      row_count: ~2000万
      role: fact
      time_field: order_time
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
        - name: user_id
          type: bigint
          nullable: false
      business_columns:
        - name: amount
          type: decimal
          nullable: false
        - name: order_time
          type: timestamp
          nullable: false

  # 声明"先聚合再引用"——语义上等价于 FROM (SELECT ...) AS agg
  # 当前架构不支持派生表子查询，应被拒绝
  intermediate_tables:
    - alias: order_agg
      source: |
        SELECT user_id, SUM(amount) AS total_amount, COUNT(*) AS order_cnt
        FROM o
        GROUP BY user_id

  metrics:
    - metric_name: avg_order_amount
      aggregation: AVG
      input_column: total_amount
      alias: avg_order_amount

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: avg_order_amount
      type: decimal
---

# 测试：派生表子查询拒绝

## 业务目标
`intermediate_tables` 声明了"先聚合再引用"的中间步骤，
语义等价于 `FROM (SELECT ...) AS agg`。
当前 Phase 3C 不支持派生表子查询——应被拒绝。

## 预期行为
- Parser：`intermediate_tables` 不在支持的语法中 → ParseError E009
- **或** Planner：无法构建对应的 SqlBuildPlan（缺少 SubqueryStep）
- **或** Validator：检测到不支持的步骤类型 → UNSUPPORTED_STEP_TYPE
```
