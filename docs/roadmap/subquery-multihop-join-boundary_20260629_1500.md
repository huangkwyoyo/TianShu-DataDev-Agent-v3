# 子查询 & 多跳 Join 边界补充文档

> 状态：**规划补全**（2026-06-29）
> 前置：Phase 3 Exit HarnessReport D4 边界声明（`phase-3-exit-report.md:88-98`）
> 关联：[[phase-3c-controlled-ddl-dml-and-compiler-interface]] | [[phase-3-exit-report]] | [[03-sql-ir-and-compiler-plan]]

---

## 1. 背景：现有记录的成熟度差异

Phase 3 Exit Report D4（"已知不支持的 SQL 模式清单"）对 5 项边界模式做了声明性记录。各模式的文档成熟度差异显著：

| 模式 | 文档状态 | 成熟度 | 差距 |
|------|---------|--------|------|
| **CTE** | ✅ 5 文档交叉引用，Validator 强制拒绝，语义等价于 SqlProgram+_temp 已证明 | ⭐⭐⭐⭐⭐ | 无 |
| **DDL/DML** | ✅ WriteValidator 10 项检查，FinalWritePlan 受控审查 | ⭐⭐⭐⭐ | 基本无 |
| **窗口+子查询组合** | ✅ Phase 3B 明确禁止，WindowExpr 不接受嵌套 | ⭐⭐⭐⭐ | 基本无 |
| **子查询** | ⚠️ 仅声明"Phase 1-3 不支持，Phase 4+ 逐项开放" | ⭐⭐ | **见本文 §2–6** |
| **多跳 Join** | ⚠️ 仅声明"Phase 1-3 不支持，Phase 4+ 逐项开放" | ⭐⭐ | **见本文 §2–6** |

子查询和多跳 Join 的边界声明停留在"占位"级别——远未达到 CTE 的等价证明 + 5 文档交叉引用标准。且两者共享同一套抽象的 7 项交付规则，实际工程路径截然不同。

---

## 2. 缺口清单（5 项）

### 缺口 1：无具体 Phase 归属 🔴

`03-sql-ir-and-compiler-plan.md §7.3` 承诺"Phase 4 及以后按黄金用例逐项开放"。现已规划的 Phase 4A–4.5 覆盖范围如下：

| Phase | 主要内容 | 是否涉及子查询/多跳 Join |
|-------|---------|-------------------------|
| 4A | LLM Gateway + Prompt 管理 | ❌ |
| 4B | PerfValidator + Compiler Pass | ❌ |
| 4C | 安全/语义评测器 | ❌ |
| 4D | Harness 七维门禁 | ❌ |
| 4.5 | 内部交互验证口 (REST API + CLI) | ❌ |

**每个 Phase 4X 文档均不含"子查询"或"多跳 Join"关键词。** 开放承诺悬空，没有落地的 Phase 归属。

> **建议**：在 Phase 4D 之后、Phase 5 之前新增 **Phase 4.6 "复杂 SQL 模式渐进开放"**，作为子查询和多跳 Join 的专属 Phase。

### 缺口 2：无黄金用例 🔴

`tests/fixtures/golden/` 目录 6 个 fixture 全部为单表或两表单跳 Join 场景：

```
golden_no_time_range.md           → 单表聚合（无时间范围）
golden_no_output_sort.md          → 单表聚合（无排序）
golden_extra_markdown_text.md     → 单表聚合（额外 Markdown 文本）
golden_chinese_column_comments.md → 单表聚合（中文列注释）
golden_type_inferred_from_registry.md → 单表 + Registry 类型推断
golden_no_explicit_joins.md       → 两表 Join（隐式声明，需 Planner）
```

**没有一个 fixture 覆盖子查询或多跳 Join。** 7 项规则第 5 条要求"测试覆盖合法黄金路径"——连用例都没定义，何时满足无法衡量。

> **建议**：至少 3 个 golden fixture（见 §4）。

### 缺口 3：Validator 无模式专用检测规则 🟡

当前 `SqlBuildPlanValidator.validate()`（`src/tianshu_datadev/sql/validator.py`）的 8 项检查不包含：

- 子查询 AST 节点检测（因为 SqlBuildPlan 没有 SubqueryStep）
- 多跳 Join 计数检查（JoinStep 每次只接受一个 right_table_ref）
- 合法子查询 vs 不支持的子查询的粒度区分

真要开放时，Validator 需要从"我不知道子查询存在"变成"我精确知道子查询在哪里、它是否超越了允许的边界"。

### 缺口 4：替代方案描述不区分工程难度 🟡

Phase 3 Exit Report 中两者写法高度相似，但工程本质不同：

| 维度 | 子查询 | 多跳 Join |
|------|--------|----------|
| 本质 | **嵌套作用域**——在 SELECT/FROM/WHERE 中嵌入完整查询 | **平面 DAG**——多个 JoinStep 串联，无嵌套 |
| 当前等效方案 | 无（需新建 SubqueryStep / 嵌套 SqlBuildPlan） | 可拆分为多步 SqlProgram，每步一个 JoinStep + _temp 传递 |
| Schema 改动 | 需要新增字段或递归引用（SqlBuildPlan → 含子 SqlBuildPlan） | JoinStep 已是平面结构，只需允许多个 JoinStep 串联 |
| Validator 改动 | 需递归校验嵌套 SqlBuildPlan 的事实源一致性 | 只需累加右表引用校验（现有逻辑基本够用） |
| Compiler 改动 | 需支持递归渲染子查询 SQL 文本 | 多步 SqlProgram 已支持（每步独立编译） |
| 安全风险 | 🔴 嵌套 SQL 片段可能藏匿注入——需双重校验 | 🟡 每步平面校验，无新攻击面 |

**两者不应共享同一套模糊的 7 项规则。** 多跳 Join 的工程难度明显低于子查询，应优先开放。

### 缺口 5：7 项规则缺少具象验收 checklist 🟡

当前 `03-sql-ir-and-compiler-plan.md:426-434` 的 7 项规则是抽象原则，没有具象化为可计数的交付物清单。见 §5 的具象化展开。

---

## 3. Phase 归属建议：新增 Phase 4.6

### 建议时间线

```
Phase 4D (Harness Gate) → Phase 4.5 (Internal Workbench) → Phase 4.6 (复杂 SQL 渐进开放) → Phase 5 (Spark)
```

### Phase 4.6 两步渐进

**Step 1 — 多跳 Join（优先级高，难度低）**

| 维度 | 说明 |
|------|------|
| 开放范围 | 2–3 跳 Join（3–4 表关联），每步一个 JoinStep |
| 先决条件 | RelationshipHypothesis 为每对表关系独立定级 |
| 校验规则 | 所有 JoinStep 的 evidence_level ≥ MEDIUM，不得出现 WEAK/NONE |
| 编译策略 | 多步 SqlProgram 串联——每步 JOIN 结果写入 _temp，下一步从 _temp 读取 |
| 预估工时 | Schema 微调（0 行改动）+ Validator 新增 1 条规则（~20 行）+ Compiler 无需改动 + 测试（~80 行）|

**Step 2 — 子查询（优先级中，难度高）**

| 维度 | 说明 |
|------|------|
| 开放范围 | FROM 子句中的派生表子查询（`SELECT ... FROM (SELECT ...) AS t`）|
| 禁止范围 | WHERE 中的关联子查询、SELECT 列表中的标量子查询（这些需更晚评估）|
| 先决条件 | 多跳 Join 已稳定 + 子查询物化策略已验证 |
| Schema 改动 | 新增 `SubqueryStep`——包含一个嵌套 `SqlBuildPlan`（递归引用）|
| 校验规则 | 递归 Validator 校验嵌套 SqlBuildPlan + 禁止超过 2 层嵌套 |
| 编译策略 | 子查询编译为 `(...)` 内联 SQL 片段，不单独创建 _temp |
| 预估工时 | Schema 新增 SubqueryStep（~40 行）+ Validator 新增 3 条规则（~80 行）+ Compiler 递归渲染（~50 行）+ 测试（~120 行）|

---

## 4. 黄金用例定义

### Fixture 1：多跳 Join（3 表关联）

**文件**：`tests/fixtures/subquery_multihop/multihop_three_table_join.md`

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.user_order_product_daily
  target_grain: [stat_date, user_id]
  summary: "三表关联——用户(主) + 订单(事实) + 商品(维表)"

  source_tables:
    - name: dim.user
      alias: u
      role: main
      key_columns:
        - name: user_id
          type: bigint
          nullable: false
      business_columns:
        - name: user_name
          type: varchar
          nullable: false

    - name: dwd.order_fact
      alias: o
      role: fact
      time_field: order_time
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
      business_columns:
        - name: user_id
          type: bigint
          nullable: false
        - name: product_id
          type: bigint
          nullable: false
        - name: amount
          type: decimal(18,2)
        - name: order_time
          type: timestamp
          nullable: false

    - name: dim.product
      alias: p
      role: dim
      key_columns:
        - name: product_id
          type: bigint
          nullable: false
      business_columns:
        - name: product_name
          type: varchar
        - name: category
          type: varchar

  relationships:
    - left_table: u
      right_table: o
      join_keys: [[u.user_id, o.user_id]]
      join_type: inner
    - left_table: o
      right_table: p
      join_keys: [[o.product_id, p.product_id]]
      join_type: inner

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: o.amount
      alias: total_amount
    - metric_name: order_cnt
      aggregation: COUNT
      input_column: o.order_id
      alias: order_cnt

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
    - dimension_name: user_id
      column_ref: u.user_id
    - dimension_name: user_name
      column_ref: u.user_name
    - dimension_name: category
      column_ref: p.category

  time_range:
    range: last_7_days
    time_field: o.order_time

  output_columns:
    - name: stat_date
      type: date
    - name: user_id
      type: bigint
    - name: user_name
      type: varchar
    - name: category
      type: varchar
    - name: total_amount
      type: decimal(18,2)
    - name: order_cnt
      type: bigint
---

# 多跳 Join 黄金用例：三表关联

## 业务目标

用户维度表 JOIN 订单事实表 JOIN 商品维表，按日期/用户/品类聚合金额和订单数。

## 预期行为

1. Parser 解析 3 个 source_tables，生成 3 个 SourceTableDecl。
2. Planner 识别 2 个 relationships，生成 2 个 RelationshipHypothesis。
3. Validator 对每对 Join 独立定级——都应是 STRONG（显式声明所有 join_keys）。
4. SqlProgram 包含 2 个步骤——每步一个 JoinStep + AggStep：
   - Step 1: u JOIN o → 写入 `_temp_step1_user_order`
   - Step 2: `_temp_step1_user_order` JOIN p → 聚合 → 输出到 `ads.user_order_product_daily`
5. Compiler 生成 2 条 SQL 语句，无 CTE。
6. DuckDB 执行——3 表联合快照一致。
```

### Fixture 2：FROM 子句中的合法子查询

**文件**：`tests/fixtures/subquery_multihop/from_subquery_derived_table.md`

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.top_category_daily
  target_grain: [stat_date, category]
  summary: "先聚合订单再 JOIN 商品——FROM 子查询物化后关联维表"

  source_tables:
    - name: dim.product
      alias: p
      role: dim
      key_columns:
        - name: product_id
          type: bigint
          nullable: false
      business_columns:
        - name: product_name
          type: varchar
        - name: category
          type: varchar

    - name: dwd.order_fact
      alias: o
      role: fact
      time_field: order_time
      key_columns:
        - name: order_id
          type: bigint
          nullable: false
      business_columns:
        - name: product_id
          type: bigint
          nullable: false
        - name: amount
          type: decimal(18,2)
        - name: order_time
          type: timestamp
          nullable: false

  # 子查询：先对订单表按 product_id 预聚合，得到派生表 order_agg
  subquery:
    - alias: order_agg
      source_tables: [o]
      grain: [o.product_id]
      metrics:
        - metric_name: daily_amount
          aggregation: SUM
          input_column: o.amount
          alias: daily_amount
      time_range:
        range: last_1_day
        time_field: o.order_time

  # 外层查询：派生表 JOIN 维表
  relationships:
    - left_table: order_agg
      right_table: p
      join_keys: [[order_agg.product_id, p.product_id]]
      join_type: inner

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: order_agg.daily_amount
      alias: total_amount

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date
    - dimension_name: category
      column_ref: p.category

  output_columns:
    - name: stat_date
      type: date
    - name: category
      type: varchar
    - name: total_amount
      type: decimal(18,2)
---

# 子查询黄金用例：FROM 派生表

## 业务目标

先对订单事实表按 product_id 预聚合（子查询），再 JOIN 商品维表获取品类名，最后按品类汇总。

## 预期行为

1. Parser 解析 1 个 subquery 声明，生成 SubqueryDecl。
2. Planner 将 subquery 转换为 SubqueryStep（嵌套 SqlBuildPlan）。
3. 内层 SqlBuildPlan 仅含 ScanStep(o) + AggregateStep（GROUP BY product_id, SUM amount）。
4. 外层 SqlBuildPlan 含 ScanStep(order_agg) + JoinStep(p) + AggregateStep。
5. Validator 递归校验内层和外层的事实源一致性。
6. Compiler 渲染为 `SELECT ... FROM (SELECT product_id, SUM(amount) AS daily_amount FROM dwd.order_fact WHERE ... GROUP BY product_id) AS order_agg JOIN dim.product p ON ...`。
7. 内层嵌套 ≤ 2 层——通过深度检查。
```

### Fixture 3（拒绝路径）：嵌套超过 2 层的子查询

**文件**：`tests/fixtures/subquery_multihop/reject_nested_subquery_depth_3.md`

预期：Validator 返回 `PLAN_REJECTED`，原因为 `SUBQUERY_NESTING_TOO_DEEP`。

```markdown
---
# 三层子查询嵌套——须被拒绝
spec:
  ...
  subquery:
    - alias: level_1
      subquery:
        - alias: level_2
          subquery:
            - alias: level_3  # ← 第 3 层：拒绝
              ...
---
```

---

## 5. 7 项交付规则具象化

以下将 `03-sql-ir-and-compiler-plan.md §7.3` 的抽象规则展开为可计数的交付物清单。

### 多跳 Join（Step 1）—— 交付 checklist

| # | 抽象规则 | 具象交付物 | 预估行数 |
|---|---------|-----------|---------|
| 1 | 新增严格 Pydantic 模型 | 无 Schema 改动——现有 JoinStep 已支持多表串联。仅需在 SqlProgram 级别允许多个 JoinStep 步骤 | 0 |
| 2 | Validator 校验 | 新增规则 #9 `MULTI_HOP_JOIN_CHECK`：每个 JoinStep 的 `relationship_ref` 独立定级 ≥ MEDIUM；所有 JoinStep 的右表引用无重复（防自循环 Join）；右表不得为已 JOIN 过的表（防菱形 Join 歧义）| ~30 |
| 3 | Compiler 确定性渲染 | 现有 Compiler 已支持多步 SqlProgram 连续编译——每步独立输出 SQL。无需改动 | 0 |
| 4 | Safety Validation 二次确认 | WriteValidator 确认 _temp 表名不冲突、DAG 无环、中间 _temp 步骤数 ≤ 5 | ~25 |
| 5 | 测试覆盖 | golden_fixture × 1（三表 Join 通过）、reject_fixture × 2（菱形 Join 拒绝、WEAK Join 拒绝）、hash 稳定性测试 × 1 | ~80 |
| 6 | PLAN_REJECTED 拒绝路径 | 菱形 Join → `AMBIGUOUS_MULTI_HOP`；WEAK Join → `JOIN_EVIDENCE_TOO_WEAK`；超过 5 跳 → `MULTI_HOP_DEPTH_EXCEEDED` | ~15 |
| 7 | Artifact 记录 | SqlBuildPlan hash、source_manifest_hash、compiler version 均已在现有 SqlProgram 架构中记录 | 0 |

### 子查询（Step 2）—— 交付 checklist

| # | 抽象规则 | 具象交付物 | 预估行数 |
|---|---------|-----------|---------|
| 1 | 新增严格 Pydantic 模型 | 新增 `SubqueryStep(StrictModel)`：`step_type: Literal["subquery"]`、`alias: SafeIdentifier`、`inner_plan: SqlBuildPlan`（递归引用——需 `from __future__ import annotations` + `ForwardRef`）| ~35 |
| 2 | Validator 校验 | 新增 3 条规则：`SUBQUERY_DEPTH_CHECK`（嵌套层数 ≤ 2）、`SUBQUERY_COLUMN_LEAK_CHECK`（内层输出列必须全部被外层引用或拒绝）、`SUBQUERY_FACT_SOURCE_CHECK`（递归校验内层 SqlBuildPlan 的事实源一致性）| ~80 |
| 3 | Compiler 确定性渲染 | `DuckDbSqlCompiler` 新增 `_render_subquery_step()`——递归调用 `compile_plan(inner_plan)` 生成 `(...)` 片段，确保内层别名唯一 | ~50 |
| 4 | Safety Validation 二次确认 | 确认子查询内层不含 WindowStep（Phase 3B 禁止规则）、内层不含 JoinStep（子查询限单表——复杂 JOIN 应拆为多步 SqlProgram）、内层 WHERE 不含关联引用外层列 | ~40 |
| 5 | 测试覆盖 | golden_fixture × 1（FROM 子查询通过）、reject_fixture × 3（嵌套超 2 层拒绝、子查询内窗口拒绝、子查询内 CTE 拒绝）、hash 稳定性测试 × 1 | ~120 |
| 6 | PLAN_REJECTED 拒绝路径 | 超 2 层 → `SUBQUERY_NESTING_TOO_DEEP`；子查询内含窗口 → `SUBQUERY_WINDOW_FORBIDDEN`；子查询内含 CTE → `SUBQUERY_CTE_FORBIDDEN`；关联子查询 → `CORRELATED_SUBQUERY_UNSUPPORTED` | ~20 |
| 7 | Artifact 记录 | SqlBuildPlan hash 需递归覆盖内层计划 + 新增 `subquery_depth` 字段 + `inner_plan_hash` | ~15 |

---

## 6. 架构约束（不可突破）

以下约束来自 AGENTS.md 和产品宪章，子查询/多跳 Join 开放不得违反：

| 约束 | 来源 | 对子查询/多跳 Join 的影响 |
|------|------|--------------------------|
| **禁止自由 SQL 片段** | `AGENTS.md:18` | 子查询不能使用 `subquery_sql: str` 逃生口——必须通过嵌套 SqlBuildPlan 表达 |
| **禁止 CTE 嵌套** | `AGENTS.md:116` | 子查询编译时不得生成 `WITH ... AS (...)`——使用直接嵌套 `(...)` 或 _temp 物化 |
| **禁止 WEAK/NONE Join 进入计划** | `AGENTS.md:23` | 多跳 Join 每一步的 evidence_level 必须 ≥ MEDIUM——一个 WEAK 步骤阻断整条链 |
| **LLM 不决定验证通过** | `AGENTS.md §5` | 子查询深度、Join 跳数的上限由 Validator 硬编码检查，不由 LLM 置信度决定 |
| **SQL 修复只能生成新 SqlBuildPlan** | `AGENTS.md:20` | 子查询或 Join 链有问题时，返工入口是重新生成 SqlBuildPlan，不能原地修补 SQL 文本 |
| **窗口+子查询组合仍禁止** | Phase 3B 退出条件 | 即使子查询开放，`WindowStep` 不得嵌套在 `SubqueryStep` 内部——两者互斥 |

---

## 7. 文档交叉引用更新清单

本文补充后，以下文档需要相应更新（标注引用来源）：

| 文档 | 更新内容 | 紧迫度 |
|------|---------|--------|
| `AGENTS.md §2` | 新增子查询嵌套深度上限（≤2 层）、多跳 Join 跳数上限（≤5 跳） | 中（随 Phase 4.6 实施） |
| `docs/03-sql-ir-and-compiler-plan.md §7.3` | 替换抽象 7 项规则为本文 §5 的具象 checklist | 中（随 Phase 4.6 实施） |
| `docs/roadmap/phase-3-exit-report.md` D4 | 补充引用到本文 | 低（可随时更新） |
| `docs/roadmap/` Phase 4.6 新建 | 本文即 Phase 4.6 的基础文档 | **当前** |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 新增 `SubqueryStep` 类定义 | 实施时 |
| `src/tianshu_datadev/sql/validator.py` | 新增 4 条子查询/多跳检测规则 | 实施时 |
| `src/tianshu_datadev/sql/compiler.py` | 新增 `_render_subquery_step()` 递归方法 | 实施时 |

---

## 8. 验收标准（Phase 4.6 就绪条件）

子查询/多跳 Join 边界从"占位声明"升级为"可实施规划"需满足：

- [ ] 本文（补充文档）已纳入项目文档索引
- [ ] `docs/roadmap/phase-4-6-complex-sql-opening.md` 已基于本文创建为正式 Phase 规划文档
- [ ] 3 个 golden/reject fixture 文件已创建（`tests/fixtures/subquery_multihop/`）
- [ ] Phase 4.6 估算工时已产出（多跳 Join ~150 行 + 子查询 ~360 行）
- [ ] 子查询 vs 多跳 Join 的差异化策略已在 AGENTS.md 中体现

---

> 本文为 `phase-3-exit-report.md` D4 维度的扩展补充——将占位声明升级为具象可执行规划。
> 关联文档：[[03-sql-ir-and-compiler-plan]] §7.3 | [[phase-3-exit-report]] D4 | [[AGENTS.md]] §2
> 外接参考：`Ai Learning/Data Dev Agent知识积累/新架构详解_逻辑链路与物理链路_20260626_1730.md`
