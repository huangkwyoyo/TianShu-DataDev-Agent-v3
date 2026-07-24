# e2e_case04 事故按 borough 聚合——单步 ComputeStep DAG

> 验证 ComputeSteps 聚合 Pipeline——事故数据按 borough 聚合输出

```markdown
---
spec:
  type: aggregate_table
  target_table: e2e_crash_summary
  target_grain: [borough]
  summary: "e2e 验证——事故数据按 borough 聚合"

  source_tables:
    - name: crash_detail
      alias: cd
      role: fact
      key_columns:
        - name: crash_id
          type: bigint
          nullable: false
      business_columns:
        - name: borough
          type: varchar
          nullable: true
        - name: persons_injured
          type: integer
          nullable: true

  compute_steps:
    # ── 单步：事故按 borough 聚合 ──
    - step_name: crash_agg
      source: input
      group_by:
        - borough
      metrics:
        - metric_name: crash_count
          aggregation: COUNT
          input_column: crash_id
          alias: crash_count
        - metric_name: total_injured
          aggregation: SUM
          input_column: persons_injured
          alias: total_injured
      output_alias: crash_agg

  output_columns:
    - name: borough
      type: varchar
    - name: crash_count
      type: bigint
    - name: total_injured
      type: bigint
---
```
