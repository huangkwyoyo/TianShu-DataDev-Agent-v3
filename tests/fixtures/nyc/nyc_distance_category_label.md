# NYC 行程距离分类标签——真实业务样本

> 数据源：NYC TLC 2026 Q1 行程记录（gold.fact_trips）
> 样本行数：~2,549 行分层抽样（fhvhv/yellow/fhv/green）
> 本文件用于 Phase 9A4 真实业务样本端到端验证——Case 02
>
> **B 类已知限制**：`distance_category`（CASE WHEN）和 `source_region_pair`（CONCAT）
> 两类计算列当前 Pipeline 的 ProjectStep 仅支持 ColumnRef/WindowExpr 表达式，
> 尚不支持 CASE WHEN 或字符串拼接。待 Phase 9A4 后续硬化。详见交接报告。

```markdown
---
spec:
  type: label_table
  target_table: gold.dws_trip_distance_label
  target_grain: [trip_id]
  summary: "按行驶距离对 NYC 行程进行短途/中途/长途三级分类打标"

  source_tables:
    - name: fact_trips_sample
      alias: ft
      row_count: ~1万
      role: fact
      time_field: pickup_at
      key_columns:
        - name: trip_id
          type: varchar
          nullable: false
      business_columns:
        - name: trip_source
          type: varchar
          nullable: false
        - name: distance_miles
          type: double
          nullable: true
        - name: total_amount
          type: decimal(12,2)
          nullable: true
        - name: passenger_count
          type: bigint
          nullable: true
        - name: pickup_location_id
          type: integer
          nullable: true
        - name: dropoff_location_id
          type: integer
          nullable: true

  time_range:
    column_ref: pickup_date_key
    start: "20260101"
    end: "20260331"

  metrics: []

  limit: 3000

  dimensions:
    - dimension_name: trip_id
      column_ref: trip_id

  output_columns:
    - name: trip_id
      type: varchar
      comment: 行程唯一标识
    - name: trip_source
      type: varchar
      comment: 行程来源
    - name: distance_miles
      type: double
      comment: 原始行驶距离
    - name: total_amount
      type: decimal(12,2)
      comment: 行程总金额
    - name: passenger_count
      type: bigint
      comment: 乘客数
---

# 行程距离分类标签

## 业务目标
为每笔行程打上距离分类标签（短途/中途/长途/未知），支持运营按距离维度拆解收入构成和区域热度。

## 分类规则（原始业务口径）
使用 CASE WHEN 表达式：

| 条件 | 标签 |
|------|------|
| `distance_miles IS NULL OR is_distance_outlier = true` | `unknown` |
| `distance_miles <= 2` | `short` |
| `distance_miles > 2 AND distance_miles <= 10` | `medium` |
| `distance_miles > 10` | `long` |

## 当前 Pipeline 能力
- 本 DevSpec 验证 label_table 类型（metrics: []）的透传列输出
- `distance_category`（CASE WHEN）和 `source_region_pair`（CONCAT）为 B 类已知限制
- 待 CaseWhenStep 与字符串表达式支持后补齐

## 数据说明
- 数据来自 gold.fact_trips，约 8032 万行
- 测试使用分层抽样 CSV 样本（~2,549 行）
```
