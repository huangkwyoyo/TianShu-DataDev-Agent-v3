# e2e_case05 违章按 violation_code 聚合——单步 ComputeStep DAG

> 验证 ComputeSteps 聚合 Pipeline——违章数据按 violation_code 聚合输出

```markdown
---
spec:
  type: aggregate_table
  target_table: e2e_violation_summary
  target_grain: [violation_code]
  summary: "e2e 验证——违章数据按 violation_code 聚合"

  source_tables:
    - name: fact_parking_violations
      alias: fv
      role: fact
      key_columns:
        - name: violation_id
          type: bigint
          nullable: false
      business_columns:
        - name: violation_code
          type: varchar
          nullable: true
        - name: standard_fine_amount
          type: decimal(12,2)
          nullable: true

  compute_steps:
    # ── 单步：违章按 violation_code 聚合 ──
    - step_name: viol_agg
      source: input
      group_by:
        - violation_code
      metrics:
        - metric_name: total_violations
          aggregation: COUNT
          input_column: violation_id
          alias: total_violations
        - metric_name: total_fine
          aggregation: SUM
          input_column: standard_fine_amount
          alias: total_fine
      output_alias: viol_agg

  output_columns:
    - name: violation_code
      type: varchar
    - name: total_violations
      type: bigint
    - name: total_fine
      type: double
---
```
