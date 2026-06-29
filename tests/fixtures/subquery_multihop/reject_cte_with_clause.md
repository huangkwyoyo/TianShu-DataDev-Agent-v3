# CTE（WITH 子句）拒绝——永不实现

> Phase 3C 不支持——CTE 永不实现，等价替代方案为 SqlProgram + _temp
> 预期拒绝：ParseError（WITH 语法不在支持的 YAML 结构中）或 Validator UNSUPPORTED_PLAN

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.cte_test_daily
  target_grain: [stat_date]
  summary: "使用 CTE（WITH 子句）描述变换——CTE 永不实现，应被拒绝"

  # CTE 语法——不在 DeveloperSpec 支持的 YAML 结构中
  # 当前架构等价替代：SqlProgram + _temp 中间表 + DAG 拓扑排序
  with:
    - name: order_agg
      query: |
        SELECT user_id, SUM(amount) AS total_amount
        FROM dwd.order_detail
        WHERE order_time >= '2026-01-01'
        GROUP BY user_id

    - name: user_with_order
      query: |
        SELECT u.user_name, oa.total_amount
        FROM dwd.user_info u
        JOIN order_agg oa ON u.user_id = oa.user_id

  source_tables:
    - name: dwd.user_info
      alias: u
      row_count: ~500万
      role: fact
      time_field: reg_date
      key_columns:
        - name: user_id
          type: bigint
          nullable: false
      business_columns:
        - name: user_name
          type: varchar
          nullable: true

  metrics:
    - metric_name: user_count
      aggregation: COUNT
      input_column: user_id
      alias: user_count

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: user_count
      type: bigint
---

# 测试：CTE（WITH 子句）拒绝

## 业务目标
`with` 子句声明了 CTE 变换链。
CTE 永不实现——等价替代方案为：
1. CTE → SqlProgram 多步骤（每步一个 SqlBuildPlan）
2. 步骤间通过 `_temp` 中间表传递结果
3. DAG 拓扑排序决定执行顺序

## 预期行为
- Parser：`with` 不在 DeveloperSpec 支持的顶层字段中 → **ParseError** 或 WARN + 忽略
- Validator：若计划中包含 CTE 引用 → UNSUPPORTED_PLAN

## 架构约束
- AGENTS.md §2 硬性规则：LLM 只输出结构化计划，SQL 只能由确定性 Compiler 生成
- CTE 引入嵌套作用域，破坏 SqlBuildPlan 的扁平可审查性
```
