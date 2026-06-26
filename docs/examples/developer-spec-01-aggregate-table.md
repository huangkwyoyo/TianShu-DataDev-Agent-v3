# DeveloperSpec 示例一：汇总表/宽表

> 来源：主规划书附录 A.1
> 场景：按日期和区域统计活跃用户数、订单金额，计算金额降序排名

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.user_region_daily
  target_grain: [stat_date, region_code]
  partition_field: stat_date
  summary: "按日期和区域统计活跃用户数、订单金额，计算金额降序排名"

  source_tables:
    - name: dwd.order_detail
      alias: od
      row_count: ~5000万
      partition_field: dt
      time_field: order_time
      description: 订单明细事实表，一笔订单一行
      role: fact
      key_columns:
        - name: order_id
          type: string
          nullable: false
          unique: true
        - name: user_id
          type: bigint
          nullable: false
          description: 用户ID
      business_columns:
        - name: region_code
          type: string
          nullable: true
          enum: [CN-N, CN-E, CN-S, CN-W, CN-C]
        - name: order_amount
          type: decimal(18,2)
          nullable: false
          description: 订单金额（元）
        - name: order_status
          type: string
          nullable: false
          enum: [paid, unpaid, cancelled]
        - name: order_time
          type: timestamp
          nullable: false

    - name: dim.region_info
      alias: ri
      row_count: ~500
      role: dim
      description: 区域维表
      key_columns:
        - name: region_code
          type: string
          nullable: false
          unique: true
      business_columns:
        - name: region_name
          type: string
          nullable: false

  output_columns:
    - name: stat_date
      type: date
    - name: region_code
      type: string
    - name: region_name
      type: string
    - name: active_users
      type: bigint
      description: 活跃用户数 COUNT(DISTINCT user_id)
    - name: total_order_amount
      type: decimal(18,2)
      description: 订单总金额 SUM(order_amount)
    - name: amount_rank
      type: int
      description: 金额降序排名 ROW_NUMBER

  write_strategy:
    type: partition_overwrite
    partition_format: yyyyMMdd
---

# 用户区域日度汇总表

## 业务目标
按日期和区域统计活跃用户数（去重用户）、订单总金额，
按金额降序计算排名，供运营日报 Top10 区域展示使用。

## 加工步骤
1. 从 dwd.order_detail 过滤已支付订单 `order_status = 'paid'`
2. 按 order_time 截取日期为 stat_date
3. 关联 dim.region_info，获取 region_name
4. 按 stat_date + region_code + region_name 汇总：
   - active_users = COUNT(DISTINCT user_id)
   - total_order_amount = SUM(order_amount)
5. ROW_NUMBER() OVER (PARTITION BY stat_date ORDER BY total_order_amount DESC) 排名

## 关联推理提示
- od.region_code → ri.region_code（事实表区域编码 → 维表区域编码）
- 维表 region_info 行数少（~500），使用全量最新数据 Join

## 数据质量关注点
- order_detail 大表（~5000万行），必须先过滤 paid 状态和时间范围再 Join 维表
- order_status 枚举值覆盖：需确认是否有 paid/unpaid/cancelled 以外的值
- 大表禁止 SELECT *，只选择所需字段

## 验证要求
- 输出行数与源表时间范围内 (日期 × 区域) 组合数一致
- 同一天内各区域 amount_rank 从 1 连续递增，无跳号
```

---

> DeveloperSpec 示例 | 汇总表/宽表 | 可用作 Phase 4.5 模板按钮"单表聚合/Join 汇总"
