# DeveloperSpec 示例二：标签表

> 来源：主规划书附录 A.2
> 场景：基于用户近90天行为生成价值等级标签和活跃度标签

```markdown
---
spec:
  type: label_table
  target_table: dim.user_behavior_label
  target_grain: [user_id]
  partition_field: stat_month
  summary: "基于用户近90天行为生成价值等级标签和活跃度标签"

  source_tables:
    - name: dwd.user_behavior
      alias: ub
      row_count: ~2亿
      partition_field: dt
      time_field: behavior_time
      description: 用户行为明细表
      role: fact
      key_columns:
        - name: user_id
          type: bigint
          nullable: false
      business_columns:
        - name: behavior_type
          type: string
          nullable: false
          enum: [page_view, add_cart, place_order, pay_success, refund]
          description: 行为类型枚举
        - name: behavior_time
          type: timestamp
          nullable: false
        - name: order_amount
          type: decimal(18,2)
          nullable: true
          description: 仅 pay_success 时有值

  output_columns:
    - name: user_id
      type: bigint
    - name: value_level
      type: string
      enum: [high, mid, low, inactive]
      description: |
        价值等级——基于近90天订单总额：
        - high：≥10000元
        - mid：[1000, 10000)
        - low：(0, 1000)
        - inactive：0元
    - name: activity_score
      type: int
      description: 近30天活跃天数（有任意行为的天数）

  label_rules:
    - field: value_level
      source_fields: [order_amount]
      logic: CASE WHEN 分段
    - field: activity_score
      source_fields: [behavior_time]
      logic: COUNT(DISTINCT date(behavior_time)) WHERE behavior_time >= date_sub(current_date, 30)
---

# 用户行为标签表

## 业务目标
为每个用户生成价值等级和活跃度两个标签，用于运营人群圈选。
- 价值等级：按近90天订单总额分4档
- 活跃度：近30天有行为的天数

## 加工步骤
1. 从 dwd.user_behavior 读取近90天数据
2. 按 user_id 聚合：
   - total_amount = SUM(order_amount) WHERE behavior_type = 'pay_success'
   - active_days_30d = COUNT(DISTINCT date(behavior_time))
     WHERE behavior_time >= date_sub(current_date, 30)
3. CASE WHEN 分段生成 value_level
4. 输出 user_id + value_level + activity_score

## 标签枚举验证
- behavior_type 五个枚举值需覆盖验证
- value_level 四个枚举值 CASE 分支需完整
- NULL order_amount 在非 pay_success 行为中为正常情况

## 数据质量关注点
- 大表（~2亿行），近90天数据量约 5000-8000万，必须指定时间范围
- pay_success 行为的 order_amount 不应为 NULL——如有 NULL 需标记异常
```

---

> DeveloperSpec 示例 | 标签表 | 可用作 Phase 4.5 模板按钮"CASE 标签分类"
