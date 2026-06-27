# 禁止宽松 2：input_tables 为空数组

> 应抛出 ParseError(E002)

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_daily
  target_grain: [stat_date]
  summary: "测试空 input_tables"

  source_tables: []

  output_columns:
    - name: stat_date
      type: date
---
```

```
