# 允许宽松 5：Markdown 正文中有额外非结构化说明

> Parser 保留在 description 中，不拒绝

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试额外 Markdown 文本"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~5000万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false

  metrics:
    - metric_name: cnt
      aggregation: COUNT
      input_column: id
      alias: cnt

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: cnt
      type: bigint

  time_range:
    column_ref: stat_date
    start: "2025-01-01"
    end: "2025-01-31"
---

# 测试：额外的 Markdown 说明

## 业务目标
这是一段很长的业务说明，包含各种细节。

## 加工步骤
1. 第一步：做某件事
2. 第二步：做另一件事
   - 注意事项 A
   - 注意事项 B

## 数据质量关注点
- 关注点 1：NULL 值处理
- 关注点 2：枚举值覆盖

> 这是一段引用的提示信息

## 验证要求
- 验证项 1
- 验证项 2

```
