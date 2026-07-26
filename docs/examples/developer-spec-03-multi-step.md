# DeveloperSpec 示例三：多步骤加工

> **当前 Parser 版本**：v4（2026-07-26）——本示例的 YAML 格式与当前 `ParsedDeveloperSpec` Parser 兼容。
> 注意：多步骤加工在 YAML 中使用 `compute_steps` 字段声明分步 DAG，本示例展示的是描述性多步骤场景。
>
> 来源：主规划书附录 A.3
> 场景：按商品类目和月份统计销售额、客单价、复购率，需要三步中间加工

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.category_monthly_summary
  target_grain: [stat_month, category_id]
  partition_field: stat_month
  summary: "按商品类目和月份统计销售额、客单价、复购率，需要三步中间加工"

  source_tables:
    - name: dwd.order_detail
      alias: od
      row_count: ~5000万
      partition_field: dt
      time_field: order_time
      role: fact
      key_columns:
        - name: order_id
          type: string
          nullable: false
          unique: true
        - name: user_id
          type: bigint
          nullable: false
        - name: product_id
          type: string
          nullable: false
      business_columns:
        - name: order_amount
          type: decimal(18,2)
          nullable: false
        - name: order_status
          type: string
          nullable: false
          enum: [paid, unpaid, cancelled]
        - name: order_time
          type: timestamp
          nullable: false

    - name: dim.product_info
      alias: pi
      row_count: ~50万
      role: dim
      description: 商品维表
      key_columns:
        - name: product_id
          type: string
          nullable: false
          unique: true
      business_columns:
        - name: category_id
          type: string
          nullable: false
        - name: category_name
          type: string
          nullable: false

  output_columns:
    - name: stat_month
      type: string
    - name: category_id
      type: string
    - name: category_name
      type: string
    - name: total_sales
      type: decimal(18,2)
      description: 销售额
    - name: order_count
      type: bigint
    - name: user_count
      type: bigint
      description: 购买用户数（去重）
    - name: avg_order_value
      type: decimal(18,2)
      description: 客单价 = total_sales / order_count
    - name: repurchase_rate
      type: decimal(5,4)
      description: 复购率 = 购买≥2次的用户数 / user_count

  compute_steps:
    # Step1：订单月度宽表——Join od + pi，按 order_id 透传不做实质聚合
    - step_name: step1_monthly_orders
      source: input
      joins:
        - left_table: od
          right_table: pi
          left_key: product_id
          right_key: product_id
          join_type: INNER
      group_by:
        - order_id
        - user_id
        - category_id
        - category_name
        - order_amount
        - stat_month
      metrics: []
      output_alias: step1_monthly_orders_temp

    # Step2：用户月度购买统计——按 user + month + category 聚合
    - step_name: step2_user_stats
      source: step1_monthly_orders
      group_by:
        - user_id
        - stat_month
        - category_id
      metrics:
        - metric_name: order_count_per_user
          aggregation: COUNT
          input_column: order_id
          alias: order_count_per_user
        - metric_name: total_amount_per_user
          aggregation: SUM
          input_column: order_amount
          alias: total_amount_per_user
      output_alias: step2_user_category_stats_temp

    # Step3：类目月度汇总 + 复购率——最终聚合 + 计算比率
    - step_name: step3_category_summary
      source: step2_user_stats
      group_by:
        - stat_month
        - category_id
        - category_name
      metrics:
        - metric_name: total_sales
          aggregation: SUM
          input_column: total_amount_per_user
          alias: total_sales
        - metric_name: order_count
          aggregation: SUM
          input_column: order_count_per_user
          alias: order_count
        - metric_name: user_count
          aggregation: COUNT_DISTINCT
          input_column: user_id
          alias: user_count
        - metric_name: repeat_users
          aggregation: COUNT_DISTINCT
          input_column: user_id
          alias: repeat_users
          filter:
            column: order_count_per_user
            operator: gte
            value: "2"
      expressions:
        - name: avg_order_value
          expression: "total_sales / NULLIF(order_count, 0)"
          type: decimal
        - name: repurchase_rate
          expression: "CAST(repeat_users AS double) / NULLIF(user_count, 0)"
          type: decimal
      output_alias: final_category_summary
---

# 类目月度汇总表（多步骤加工）

## 业务目标
按商品类目和月份统计：销售额、客单价、复购率。
需三步中间加工，每一步均可独立验证中间行数。

## 加工步骤

### Step1：订单月度宽表（_temp）
- 从 dwd.order_detail 过滤已支付 + 指定月份
- 关联 dim.product_info 获取 category_id、category_name
- 输出：order_id、user_id、category_id、category_name、order_amount、stat_month
- 中间工作表：step1_monthly_orders_temp

### Step2：用户月度购买统计（_temp）
- 基于 step1_monthly_orders_temp
- 按 user_id + stat_month + category_id 聚合：
  - order_count_per_user = COUNT(order_id)
  - total_amount_per_user = SUM(order_amount)
- 中间工作表：step2_user_category_stats_temp

### Step3：类目月度汇总 + 复购率（最终结果）
- 基于 step2_user_category_stats_temp
- 按 stat_month + category_id + category_name 汇总：
  - total_sales = SUM(total_amount_per_user)
  - order_count = SUM(order_count_per_user)
  - user_count = COUNT(DISTINCT user_id)
  - repeat_users = COUNT(DISTINCT CASE WHEN order_count_per_user >= 2 THEN user_id END)
  - avg_order_value = total_sales / order_count
  - repurchase_rate = repeat_users / user_count

## 中间表清单
| 中间表 | 来源 | 粒度 | 预估行数 |
|--------|------|------|----------|
| step1_monthly_orders_temp | od + pi | order_id | ~500万/月 |
| step2_user_category_stats_temp | step1 | user_id + stat_month + category_id | ~200万/月 |

## 数据质量关注点
- 每步中间表输出行数应与预估量级一致，差异 >20% 标 WARN
- 复购率计算中 user_count=0 时需处理除零
- product_id 关联不上的订单需标记（维表覆盖率检查）

## 验证要求
- Step1 行数 ≤ 源表过滤后行数（Join 维表不应膨胀）
- Step2 行数 = Step1 按 user_id+stat_month+category_id 的去重数
- 复购率范围 [0, 1]
```

---

> DeveloperSpec 示例 | 多步骤加工 | 可用作 Phase 4.5 模板按钮"多步骤 SqlProgram"
