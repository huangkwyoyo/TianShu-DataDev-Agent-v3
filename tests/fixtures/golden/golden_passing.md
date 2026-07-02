# Golden — 基础聚合（可通过 Validator 校验）

> 行数低于 100 万阈值，不会触发时间过滤阻断

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试基础聚合流程——事实表行数低于阈值，可通过全链路校验"

  source_tables:
    - name: dwd.test_fact
      alias: tf
      row_count: ~10万
      role: fact
      time_field: event_time
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: event_time
          type: timestamp
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
---

# 测试：基础聚合流程

## 业务目标
计算每日聚合指标。事实表行数 ~10 万，低于 100 万阈值，不触发时间过滤阻断。
```
