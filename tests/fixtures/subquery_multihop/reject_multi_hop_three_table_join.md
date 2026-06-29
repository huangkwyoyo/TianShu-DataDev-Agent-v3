# 多跳 Join 拒绝（三表关联：user → order → product）

> Phase 3C 不支持——应被 Validator MULTI_HOP_JOIN_CHECK 拒绝
> 预期拒绝码：Q-VAL-MULTIHOP

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.user_order_product_daily
  target_grain: [stat_date]
  summary: "三表多跳 Join——user 关联 order，order 再关联 product"

  source_tables:
    - name: dwd.user_info
      alias: u
      row_count: ~500万
      role: fact
      time_field: reg_date
      key_columns:
        - name: user_id
          type: bigint
          nullable: false
      business_columns:
        - name: user_name
          type: varchar
          nullable: true

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
        - name: product_id
          type: bigint
          nullable: false
      business_columns:
        - name: amount
          type: decimal
          nullable: false

    - name: dim.product_info
      alias: p
      row_count: ~10万
      role: dimension
      key_columns:
        - name: product_id
          type: bigint
          nullable: false
      business_columns:
        - name: product_name
          type: varchar
          nullable: true

  # 两个 Join——构成多跳 Join（u → o → p）
  joins:
    - left_table: u
      right_table: o
      left_key: user_id
      right_key: user_id
      join_type: inner

    - left_table: o
      right_table: p
      left_key: product_id
      right_key: product_id
      join_type: left

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: amount
      alias: total_amount

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
    - dimension_name: product_name
      column_ref: product_name

  output_columns:
    - name: stat_date
      type: date
    - name: product_name
      type: varchar
    - name: total_amount
      type: decimal
---

# 测试：多跳 Join 拒绝

## 业务目标
三表关联（user → order → product）构成多跳 Join。
当前 Phase 3C 仅支持单跳 Join（两表关联），多跳 Join 应被 Validator 拒绝。

## 预期行为
- Parser：可能正常解析（三个源表均声明）
- Planner：可能构建出含多个 JoinStep 的 SqlBuildPlan
- **Validator**：检测到 >1 JoinStep → 返回 MULTI_HOP_JOIN 拒绝（blocking=True）
```
