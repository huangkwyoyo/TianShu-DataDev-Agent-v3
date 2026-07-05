# NYC 停车违章明细宽表——真实业务样本

> 数据源：NYC 2026 财年停车违章记录（gold.fact_parking_violations + gold.dim_violation_type）
> 样本行数：~5,000 行随机抽样 + 100 行完整字典
> 本文件用于 Phase 9A4 真实业务样本端到端验证——Case 03

```markdown
---
spec:
  type: detail_table
  target_table: ads.parking_violation_detail_wide
  target_grain: [summons_number]
  summary: "停车违章明细宽表——关联违章代码字典，展开违章描述和标准罚款金额"

  source_tables:
    - name: fact_parking_violations_sample
      alias: pv
      row_count: 5000
      role: fact
      time_field: issue_date_key
      key_columns:
        - name: violation_id
          type: bigint
          nullable: false
      business_columns:
        - name: summons_number
          type: varchar
          nullable: false
        - name: violation_code
          type: varchar
          nullable: false
        - name: plate_id
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
        - name: standard_fine_amount
          type: decimal(12,2)
          nullable: true
        - name: penalty_amount
          type: decimal(12,2)
          nullable: true

  metrics: []
  limit: 3000

  dimensions:
    - dimension_name: summons_number
      column_ref: summons_number

  joins:
    - left_table: pv
      right_table: vt
      left_key: violation_code
      right_key: violation_code
      join_type: LEFT
      comment: "违章代码关联违章类型维度表——LEFT JOIN 保证违章记录不丢失"

  output_columns:
    - name: summons_number
      type: varchar
      comment: 传票号
    - name: violation_description
      type: varchar
      comment: 违章描述（来自字典，human-readable）
    - name: plate_id
      type: varchar
      comment: 车牌号
    - name: registration_state
      type: varchar
      comment: 注册州
    - name: is_duplicate_summons
      type: boolean
      comment: 重复传票标记
---

# 停车违章明细宽表

## 业务目标
将停车违章事实表与违章代码维度表关联，展开人类可读的违章描述，产出完整的违章明细宽表。

## 关联说明
- 事实表约 958 万行，维度表约 100 行
- LEFT JOIN 以事实表为主——违章记录不因代码未收录而丢失
- 测试使用 ~5,000 行随机抽样

## 数据说明
- 数据来自 gold.fact_parking_violations + gold.dim_violation_type
- 测试使用 CSV 样本（5,000 行事实 + 100 行维度）
```
