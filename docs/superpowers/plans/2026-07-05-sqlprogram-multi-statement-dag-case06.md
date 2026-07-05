# SqlProgram 多语句 DAG 实施计划——解锁 NYC Case 06 跨域融合

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 利用已有 SqlProgram 多语句 DAG 基础设施（Phase 3A 建成），通过 `_temp_*` 临时表串联多步独立聚合→Join→计算→标签，解锁 NYC Case 06 跨域安全合规画像。

**架构：** 复用现有 SqlProgram/compile_program/execute_program/validate_multi_hop_chain 全链路，不新增 IR 节点类型，不引入 CTE。Case 06 的 5 表 3 域融合通过 `compute_steps` DAG 声明拆分为 7 步 SqlProgram——每步一个 SqlBuildPlan，步间通过 `_temp_*` 临时表传递 borough 级聚合结果。

**技术栈：** Python 3.12+ / Pydantic StrictModel / DuckDB / 现有 SqlProgram + SqlBuildPlanBuilder + Validator + Compiler + Executor / pytest

## 全局约束

- **CTE 仍然禁止**——`WITH ... AS` 不得出现在任何编译产物、测试 fixture 或文档描述中
- **不得实现 WITH**——Validator 已有 CTE 拒绝规则，本轮不放松
- **不得引入 raw_sql / expression / where_sql / join_on 字符串逃生口**——所有步骤使用封闭类型化 step
- **不得让 LLM 直接生成 SQL 文本**——SQL 只由确定性 Compiler 渲染
- **不得绕过 SqlBuildPlan / SqlProgram 类型化结构**——所有步骤必须经过 Builder → Validator → Compiler → Executor
- **不得读取白名单外数据源**——仅使用 NYC Case 06 声明的 5 张源表
- **不得进入真实生产写入**——Executor 仅读取 CSV 样本，输出到 DuckDB 内存
- **本轮不实现 PERCENTILE_CONT**——风险等级判定使用硬编码阈值替代分布统计
- **本轮不实现 top_crash_factor 的 ROW_NUMBER 子查询**——该字段暂标记为 `HUMAN_REVIEW`
- **所有代码注释必须使用中文**

---

## 一、Case 06 为什么需要多步 DAG

### 1.1 数据拓扑分析

NYC Case 06 涉及 5 张源表、3 个业务域、不同聚合粒度：

| 源表 | 域 | 粒度 | 关键问题 |
|------|-----|------|---------|
| `dim_taxi_zone` | 空间地理 | location_id (265 行) | 有 borough 字段，可做 borough 聚合 |
| `dws_zone_trip_summary` | 出行 | pickup_location_id (263 行) | 可通过 location_id JOIN dim_taxi_zone |
| `fact_crashes` | 安全 | crash_id (165 万行) | **仅有 borough 文本字段，无 location_id**——无法直接 JOIN 到 zone 级别 |
| `dws_daily_parking_summary` | 合规 | date_key (1247 行) | **无 borough 字段**——需 violation_county 代码映射（QN→Queens 等） |
| `dim_violation_type` | 合规 | violation_code (100 行) | 字典表，不参与主 DAG |

### 1.2 为什么单语句 SqlBuildPlan 无法表达

单语句 SqlBuildPlan 假设所有源表可通过 JoinStep 直接在一条 SELECT 中关联。Case 06 违反此假设：

1. **粒度不匹配**：`fact_crashes` 粒度为 crash_id（165 万行），而目标粒度为 borough（6 行）。若直接 JOIN，单语句会产生 165 万行中间结果再聚合——正确但在大规模数据上性能不可接受。更根本的是，`fact_crashes` 没有 `location_id`，无法与 `dim_taxi_zone` 做键值 JOIN——只能通过 `borough` 字符串匹配。

2. **代码映射依赖**：`dws_daily_parking_summary` 的 borough 信息藏在 `violation_county` 代码中，需要 CASE WHEN 映射为 borough 名称后才能参与 borough 级关联。

3. **跨域聚合顺序**：必须先各自在 borough 级别预聚合（出行域、安全域、合规域各自独立），再将三个域的 borough 级结果 LEFT JOIN。这一"各自聚合→统一关联"的模式天然是多步 DAG。

### 1.3 等效 SqlProgram DAG 拓扑

```
Step 1 (crash_agg):         fact_crashes → Aggregate(borough) → _temp_crash_boro_agg
Step 2 (parking_agg):       dws_daily_parking_summary → CASE WHEN 代码映射 → Aggregate(borough) → _temp_parking_boro_agg
Step 3 (trip_zone_agg):     dim_taxi_zone JOIN dws_zone_trip_summary → Aggregate(borough) → _temp_trip_boro_agg
Step 4 (trip_crash_join):   _temp_trip_boro_agg LEFT JOIN _temp_crash_boro_agg ON borough → _temp_trip_crash
Step 5 (all_three_join):    _temp_trip_crash LEFT JOIN _temp_parking_boro_agg ON borough → _temp_boro_profile
Step 6 (compute_ratios):    _temp_boro_profile → Project(归一化指标) → _temp_with_ratios
Step 7 (risk_label):        _temp_with_ratios → CaseWhenStep(风险等级) → 最终输出
```

**对应 DAG 图**：
```
Step 1 ─────────────────┐
Step 2 ───────────┐     │
Step 3 ───┐       │     │
          ▼       ▼     ▼
Step 4: trip_boro_agg LEFT JOIN crash_boro_agg
          │
          ▼
Step 5: ... LEFT JOIN parking_boro_agg
          │
          ▼
Step 6: 归一化指标计算
          │
          ▼
Step 7: CASE WHEN 风险等级标签 → 最终输出
```

拓扑排序结果（Kahn + 字典序）：`[step_1, step_2, step_3, step_4, step_5, step_6, step_7]`

---

## 二、每一步 SqlBuildPlan 如何物化为 `_temp_*`

### 2.1 临时表命名规则

沿用 Phase 3A `make_temp_name(chain_id, step_name)` 规则：
- 格式：`_temp_{chain_id}_{step_name}`
- `chain_id`：Case 06 spec_hash 的 MD5 前 8 位（确定性）
- `step_name`：来自 `ComputeStep.step_name`

具体映射：

| Step | step_name | 产出的 _temp 表 | 消费方 |
|------|-----------|-----------------|--------|
| 1 | `crash_boro_agg` | `_temp_{chain_id}_crash_boro_agg` | Step 4 |
| 2 | `parking_boro_agg` | `_temp_{chain_id}_parking_boro_agg` | Step 5 |
| 3 | `trip_boro_agg` | `_temp_{chain_id}_trip_boro_agg` | Step 4 |
| 4 | `trip_crash_join` | `_temp_{chain_id}_trip_crash_join` | Step 5 |
| 5 | `all_three_join` | `_temp_{chain_id}_all_three_join` | Step 6 |
| 6 | `compute_ratios` | `_temp_{chain_id}_compute_ratios` | Step 7 |
| 7 | `risk_label` | 无（FINAL——直接输出 SELECT） | — |

### 2.2 每步编译产物

Compiler 的 `compile_program()` 为每个 statement 生成一条 SQL：

```sql
-- Step 1: 事故 borough 预聚合
CREATE TEMP TABLE _temp_abc123_crash_boro_agg AS
SELECT borough, COUNT(crash_id) AS total_crashes, SUM(persons_injured) AS total_injured,
       SUM(persons_killed) AS total_killed
FROM fact_crashes WHERE crash_date_key >= 20250101 AND crash_date_key <= 20260331
GROUP BY borough;

-- Step 2: 违章 borough 预聚合（含 county 代码映射）
CREATE TEMP TABLE _temp_abc123_parking_boro_agg AS
SELECT CASE violation_county WHEN 'QN' THEN 'Queens' WHEN 'BK' THEN 'Brooklyn' ... END AS borough,
       SUM(violation_count) AS total_violations, AVG(standard_fine_total) AS avg_daily_fine
FROM dws_daily_parking_summary
GROUP BY violation_county;

-- Step 3: 行程 borough 聚合
CREATE TEMP TABLE _temp_abc123_trip_boro_agg AS
SELECT tz.borough, SUM(zts.trip_count) AS total_trip_count,
       SUM(zts.total_fare_amount) AS total_fare
FROM dim_taxi_zone tz LEFT JOIN dws_zone_trip_summary zts
  ON tz.location_id = zts.pickup_location_id
GROUP BY tz.borough;

-- Step 4: 行程 + 事故 LEFT JOIN
CREATE TEMP TABLE _temp_abc123_trip_crash_join AS
SELECT t.borough, t.total_trip_count, t.total_fare,
       COALESCE(c.total_crashes, 0) AS total_crashes, ...
FROM _temp_abc123_trip_boro_agg t
LEFT JOIN _temp_abc123_crash_boro_agg c ON t.borough = c.borough;

-- ...（Step 5-6 类似，Step 7 为最终 SELECT）
```

### 2.3 生命周期管理

- **CREATE**：producer step 执行时，Executor 执行 `CREATE TEMP TABLE _temp_* AS ...`
- **READ**：consumer step 通过 `ScanStep(table_ref="_temp_*")` 读取
- **CLEANUP**：程序结束后（成功或失败），Executor 的 `execute_program()` 遍历所有 `_temp_*` 表执行 `DROP TABLE IF EXISTS`——此逻辑 Phase 3A 已实现，本轮无需修改

---

## 三、DAG 依赖、拓扑排序、命名和生命周期规则

### 3.1 DAG 依赖规则（沿用 Phase 3A）

| 规则 | 实现 | 测试覆盖 |
|------|------|---------|
| 每步 `depends_on` 只能引用同一 SqlProgram 内的其他 step_id | `validate_program_dag()` MISSING_DEPENDENCY | ✅ 已有 |
| 循环依赖被拒绝（`CIRCULAR_DEPENDENCY`） | `topological_sort()` 检测 | ✅ 已有 |
| `_temp` 消费者必须在 `TempTableSpec.consumed_by` 中声明 | `validate_program_dag()` 4.5 检查 | ✅ 已有 |
| `_temp` 消费者必须通过 DAG 边可达生产者 | `validate_program_dag()` 4.6 BFS 检查 | ✅ 已有 |

### 3.2 拓扑排序确定性（沿用 Phase 3A）

- Kahn 算法 + 最小堆（`statement_id` 字典序打破平局）
- 相同 DAG → 相同 topological_order——确定性保证
- 本轮不修改此逻辑

### 3.3 临时表命名规则

- 前缀 `_temp_` 硬性要求——`validate_temp_table_naming()` 拒绝不符合的命名
- `make_temp_name(chain_id, step_name)` 确定性生成——相同输入产生相同表名
- 命名不含特殊字符、不含 SQL 关键字、长度 ≤ 63 字符（DuckDB 限制）

### 3.4 生命周期规则

- **_temp 表不得跨越 SqlProgram 边界**——不同 SqlProgram 之间通过 DataTransformContract 传递规格
- **cleanup 必须在程序结束时执行**（无论成功或失败）——Executor 负责
- **本轮不修改生命周期逻辑**——Phase 3A 已实现完整

---

## 四、各组件改动分析

### 4.1 Compiler——不改

`DuckDbSqlCompiler.compile_program()` 已支持多语句 SqlProgram 编译：
- 遍历 `topological_order`，为每个 statement 调用 `_compile_core()`
- 每个中间 statement 渲染为 `CREATE TEMP TABLE _temp_* AS ...`
- FINAL statement 渲染为 `SELECT ...`
- 输出 `ProgramCompiledSql`（含 `ProgramSqlArtifact`）

**本轮验证要点**：确认 `CaseWhenStep` 在 `compile_program()` 中可以正常渲染。

### 4.2 Executor——不改

`DuckDBExecutor.execute_program()` 已支持多语句执行：
- 按 `topological_order` 逐条执行
- 失败语句阻断后续步骤
- 程序结束后清理所有 `_temp_*` 表

**本轮验证要点**：确认 7 步 DAG 中任意步骤失败时 cleanup 正确执行。

### 4.3 ContractExtractor——需改

`DataTransformContractExtractor.extract_v1()` 已存在（SqlProgram → DataTransformContractV1），但需验证：

1. **Multi-source aggregations**：V1 contract 的 `input_tables` 是否聚合了所有 SqlProgram statement 的 ScanStep 源表
2. **Temp table specs**：V1 contract 的 `temp_tables` 是否正确反映 DAG 中间表
3. **Step DAG**：V1 contract 的 `step_dag` 是否正确从 `statements[].depends_on` 派生

**实际改动范围**：主要是验证+加固，预计 ≤ 30 行新增代码。

### 4.4 Comparator / PlanComparator——不改

PlanComparator 对比的是 SqlBuildPlan vs SparkPlan 的 step 级别结构，不依赖 SqlProgram 是否为多语句。Case 06 的每个子步骤都是标准 step 类型（Scan/Aggregate/Join/Project/CaseWhen），Comparator 已有覆盖。

### 4.5 Pipeline——需改

当前 `run_all()` 在 compute_steps 路径中**缺少 Contract 提取**——compile/execute 后直接返回，跳过了 contract 和 package 阶段。需要补齐：

1. `run_all()` 的 compute_steps 分支增加 `DataTransformContractExtractor.extract_v1()` 调用
2. `export_artifacts()` 的 `PipelineArtifactBundle.data_transform_contract` 需要能容纳 V1 类型（当前字段类型已声明 `DataTransformContractLite | DataTransformContractV1 | None`）

**实际改动范围**：Pipeline.run_all() ~40 行代码调整 + ~20 行 export_artifacts 调整。

### 4.6 Validator——需小幅扩展

现有 `validate_multi_hop_chain()` 已覆盖 V-009b（循环检测）和 V-009c（深度上限 ≤ 5 跳）。Case 06 需要额外验证：

1. **borough 字符串 JOIN 证据链**：Step 4/5 的 JoinStep 使用 `borough` 字符串字段关联——证据等级为 MEDIUM（精确匹配但无外键约束）。Validator 需确认 MEDIUM 等级的 Join 能通过（不应被 WEAK/NONE 门禁误杀）。

**实际改动范围**：Validator 无需改代码——MEDIUM 证据已可通过。仅需在 Case 06 fixture 中显式声明 join evidence。

### 4.7 Spec Parser——不改

`ComputeStep` 模型和 `compute_steps` 字段已在 `ParsedDeveloperSpec` 中定义，Parser 已支持解析 YAML 中的 `compute_steps` 块。

### 4.8 SqlBuildPlanBuilder——需验证

`build_from_steps()` 已实现——为每个 ComputeStep 生成独立 SqlBuildPlan。需验证：
1. 独立源表聚合（Step 1/2/3 各自从不同源表出发）是否正确生成
2. 后续 Join step 是否正确引用 `_temp_*` 表

**实际改动范围**：主要是集成验证，预计 ≤ 20 行修复。

---

## 五、本轮不做的能力

| 能力 | Case 06 需求 | 本轮策略 | 替代方案 |
|------|-------------|---------|---------|
| **PERCENTILE_CONT** | 计算 crash/violation 分布的中位数和上四分位数 | ❌ 不实现 | 使用硬编码阈值：crash_per_million_trips ≥ 500 → high, ≥ 1000 → extreme |
| **top_crash_factor ROW_NUMBER 子查询** | 每个 Borough 最高频 contributing_factor_1 | ❌ 不实现 | 字段标记为 `HUMAN_REVIEW`，在最终输出中留空或标注"待人工补充" |
| **violation_county 代码映射** | QN→Queens 等 5 个代码 | 🟡 简化实现 | 使用 CaseWhenStep 的 WhenBranch 做确定性映射——不允许 LLM 自由生成映射逻辑 |
| **复杂表达式** | crash_per_million_trips = total_crashes / total_trip_count * 1e6 | 🟡 简化实现 | 在 ProjectStep 中以 AliasExpr 表达——若当前 ProjectStep 不支持除法表达式，使用编译后 SQL 的确定性除法定理绕过（即 AggStep 输出分子分母，计算放在外部 Review） |
| **自由 SQL** | 任何 `raw_sql` 片段 | ❌ 永不 | — |
| **CTE** | `WITH ... AS` | ❌ 永不 | SqlProgram + _temp 等效替代 |

### 5.1 风险等级硬编码阈值（替代 PERCENTILE_CONT）

原始需求使用 PERCENTILE_CONT 计算动态阈值。本轮使用固定阈值：

```python
# 硬编码风险阈值（基于 NYC 历史数据的合理估计）
# crash_per_million_trips: median≈300, Q3≈800
# violation_per_thousand_trips: median≈5, Q3≈15
CASE
  WHEN crash_per_million_trips >= 800 AND violation_per_thousand_trips >= 15 THEN 'extreme'
  WHEN crash_per_million_trips >= 800 OR  violation_per_thousand_trips >= 15 THEN 'high'
  WHEN crash_per_million_trips < 300 AND violation_per_thousand_trips < 5 THEN 'low'
  ELSE 'medium'
END AS safety_risk_level
```

阈值在 DeveloperSpec 中显式声明，确保可审查性。

---

## 六、CTE 禁止规则的保障机制

### 6.1 多层防护

| 层 | 机制 | 说明 |
|----|------|------|
| **IR 层** | SqlBuildPlan 8 种 step 类型中无 CTE 节点 | 结构上不可能表达 CTE |
| **Validator 层** | `_validate_cte_rejection()` 检查编译产物中是否含 `WITH ... AS` | 二次确认——即使 Compiler 异常也不会放过 |
| **Compiler 层** | `_render_subquery_step()` 只渲染 `(...)` 内联子查询，不生成 `WITH` | 编译器本身不输出 CTE 语法 |
| **测试层** | `test_subquery.py::test_reject_cte` 断言 "子查询不得生成 CTE" | 已有回归测试 |
| **文档层** | 5 文档交叉引用 + `never_implement: 1` | 所有开发者可见 |
| **记忆层** | `sqlprogram-temp-table-not-cte.md` 记录 CTE 禁止理由 | Agent 上下文可检索 |

### 6.2 本轮新增防护

- Case 06 测试 suite 中增加 `test_no_cte_in_compiled_sql()`——对 compile_program() 的每一句输出 SQL 做 `WITH` 关键字扫描
- Case 06 fixture 中显式声明 `# CTE 禁止——所有多步依赖使用 _temp_* 临时表`

---

## 七、RED/GREEN 测试设计和回归范围

### 7.1 测试层次

```
tests/fixtures/nyc/nyc_safety_compliance_profile.md     ← Case 06 DeveloperSpec fixture（新建）
tests/api/test_nyc_business_case.py                       ← 新增 Case 06 测试类（扩展现有文件）
tests/planning/test_sql_program.py                        ← 现有 SqlProgram 测试（零退化验证）
tests/sql/test_pipeline_e2e.py                            ← 现有 Pipeline E2E（零退化验证）
```

### 7.2 RED 测试（先写，预期失败）

| 测试 | 预期失败原因 | RED 验证 |
|------|-------------|---------|
| `test_case06_spec_parses_without_errors` | Spec 格式可能需要 parser 调整 | 断言 blocking questions = 0 |
| `test_case06_run_all_completes_all_stages` | Pipeline compute_steps 路径可能缺少 contract 提取 | 断言所有 8 阶段通过 |
| `test_case06_sql_program_has_seven_steps` | build_from_steps 可能产出不足 7 个 plan | 断言 len(statements) == 7 |
| `test_case06_dag_topological_order` | 拓扑排序可能因依赖声明错误而失败 | 断言 order 符合预期 |
| `test_case06_execution_produces_six_boroughs` | 数据问题或聚合错误 | 断言 row_count == 6 |
| `test_case06_risk_level_is_valid` | CaseWhenStep 渲染问题 | 断言所有 risk_level ∈ {low,medium,high,extreme} |
| `test_case06_no_cte_in_compiled_sql` | 编译器可能意外生成 CTE | 断言所有编译 SQL 不含 WITH |
| `test_case06_temp_tables_cleaned_up` | Executor cleanup 可能遗漏 | 断言程序结束后无 _temp_* 残留 |

### 7.3 GREEN 实现（逐步点亮）

按 Task 1→5 依次点亮 RED→GREEN。

### 7.4 回归范围

```bash
# 全量回归——确保 SqlProgram/Compiler/Executor/Pipeline 零退化
python -m pytest tests/planning/test_sql_program.py tests/planning/test_temp_table.py -q
python -m pytest tests/sql/test_pipeline_e2e.py -q
python -m pytest tests/api/test_nyc_business_case.py -q
python -m pytest tests/api/ tests/spark/ tests/artifacts/ -q
python -m ruff check src/ tests/
git diff --check
```

**当前基线**：661 passed / 11 skipped。回归后不得低于此基线。

---

## 八、实施任务

### Task 1: Case 06 DeveloperSpec Fixture（RED 测试准备）

**文件：**
- Create: `tests/fixtures/nyc/nyc_safety_compliance_profile.md`
- Modify: `tests/api/test_nyc_business_case.py`（新增 Case 06 测试类骨架 + RED 测试）

**接口：**
- Consumes: `ComputeStep` 模型（`developer_spec/models.py:539`）
- Produces: Case 06 fixture（供 Task 2-5 消费）

- [ ] **Step 1: 编写 Case 06 DeveloperSpec fixture**

基于 `D:\ProgramData\Datawarehouse\纽约市城市交通\案例\06_区域安全合规画像.md` 的需求，编写符合 `compute_steps` 格式的 YAML spec。关键点：
- 5 张源表完整声明（字段、类型、nullable）
- 7 个 `compute_steps`——每步声明 `step_name`、`source`（上游步骤名或 `"input"`）、`group_by`、`metrics`、`joins`

```yaml
---
spec:
  type: label_table
  target_table: ads.zone_safety_compliance_profile
  target_grain: [borough]
  summary: "区域安全合规画像——融合停车违章频率与事故严重度，产出区域风险等级标签"

  source_tables:
    - name: gold.dim_taxi_zone
      alias: tz
      row_count: 265
      role: dim
      key_columns:
        - name: location_id
          type: integer
          nullable: false
      business_columns:
        - name: borough
          type: varchar
          nullable: false
        - name: zone_name
          type: varchar
          nullable: false

    - name: gold.dws_zone_trip_summary
      alias: zts
      row_count: 263
      role: fact
      key_columns:
        - name: pickup_location_id
          type: integer
          nullable: false
      business_columns:
        - name: trip_count
          type: bigint
          nullable: false
        - name: total_fare_amount
          type: decimal(18,2)
          nullable: true

    - name: gold.fact_crashes
      alias: fc
      row_count: 1655065
      role: fact
      time_field: crash_date_key
      key_columns:
        - name: crash_id
          type: bigint
          nullable: false
      business_columns:
        - name: borough
          type: varchar
          nullable: false
        - name: persons_injured
          type: integer
          nullable: false
        - name: persons_killed
          type: integer
          nullable: false
        - name: contributing_factor_1
          type: varchar
          nullable: true

    - name: gold.dws_daily_parking_summary
      alias: dps
      row_count: 1247
      role: fact
      time_field: issue_date
      key_columns:
        - name: date_key
          type: integer
          nullable: false
      business_columns:
        - name: violation_county
          type: varchar
          nullable: false
        - name: violation_count
          type: bigint
          nullable: false
        - name: standard_fine_total
          type: decimal(18,2)
          nullable: false

    - name: gold.dim_violation_type
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

  # 分步计算声明——7 步 DAG（CTE 禁止：所有多步依赖使用 _temp_* 临时表）
  compute_steps:
    - step_name: crash_boro_agg
      source: input
      description: "事故数据按 borough 预聚合——fact_crashes 仅有 borough 文本字段，无法直接键值 JOIN"
      group_by: [borough]
      metrics:
        - metric_name: total_crashes
          aggregation: COUNT
          input_column: crash_id
          alias: total_crashes
        - metric_name: total_injured
          aggregation: SUM
          input_column: persons_injured
          alias: total_injured
        - metric_name: total_killed
          aggregation: SUM
          input_column: persons_killed
          alias: total_killed
      time_range:
        column_ref: crash_date_key
        start: "20120101"
        end: "20261231"

    - step_name: parking_boro_agg
      source: input
      description: "违章数据按 violation_county 代码映射到 borough 后聚合"
      group_by: [violation_county]
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: violation_count
          alias: total_violations
        - metric_name: avg_daily_fine
          aggregation: AVG
          input_column: standard_fine_total
          alias: avg_daily_fine

    - step_name: trip_boro_agg
      source: input
      description: "行程数据——dim_taxi_zone JOIN dws_zone_trip_summary 后按 borough 聚合"
      group_by: [borough]
      joins:
        - left_table: tz
          right_table: zts
          left_key: location_id
          right_key: pickup_location_id
          join_type: LEFT
      metrics:
        - metric_name: total_trip_count
          aggregation: SUM
          input_column: trip_count
          alias: total_trip_count
        - metric_name: total_fare
          aggregation: SUM
          input_column: total_fare_amount
          alias: total_fare

    - step_name: trip_crash_join
      source: [trip_boro_agg, crash_boro_agg]
      description: "行程 borough 聚合 LEFT JOIN 事故 borough 聚合——borough 字符串匹配（MEDIUM 证据）"
      group_by: [borough]
      joins:
        - left_table: _temp_trip_boro_agg
          right_table: _temp_crash_boro_agg
          left_key: borough
          right_key: borough
          join_type: LEFT

    - step_name: all_three_join
      source: [trip_crash_join, parking_boro_agg]
      description: "合并停车违章聚合——violation_county 代码经 CASE WHEN 映射为 borough 后关联"
      group_by: [borough]
      joins:
        - left_table: _temp_trip_crash_join
          right_table: _temp_parking_boro_agg
          left_key: borough
          right_key: borough
          join_type: LEFT

    - step_name: compute_ratios
      source: [all_three_join]
      description: "归一化指标计算——每百万行程事故率、每千行程违章率"
      group_by: [borough]

    - step_name: risk_label
      source: [compute_ratios]
      description: "CASE WHEN 风险等级标签 + 最终输出"

  output_columns:
    - name: borough
      type: varchar
    - name: total_trip_count
      type: bigint
    - name: total_crashes
      type: bigint
    - name: total_injured
      type: bigint
    - name: total_killed
      type: bigint
    - name: total_violations
      type: bigint
    - name: avg_daily_fine
      type: decimal(18,2)
    - name: crash_per_million_trips
      type: double
    - name: violation_per_thousand_trips
      type: double
    - name: safety_risk_level
      type: varchar
---

# 区域安全合规画像——Case 06 跨域融合

## 业务目标
将停车违章罚单数据与机动车碰撞事故数据做行政区级关联分析，产出每个 Borough 的安全合规画像。

## 多步 DAG 说明
本案使用 7 步 SqlProgram DAG（CTE 禁止——所有多步依赖使用 _temp_* 临时表）：
1. crash_boro_agg：事故数据按 borough 预聚合（fact_crashes 仅有 borough 文本字段）
2. parking_boro_agg：违章数据按 county 代码预聚合
3. trip_boro_agg：行程数据按 borough 聚合（JOIN tz + zts）
4. trip_crash_join：行程 LEFT JOIN 事故（borough 字符串匹配——MEDIUM 证据）
5. all_three_join：合并违章数据
6. compute_ratios：归一化指标计算
7. risk_label：CASE WHEN 风险等级标签

## 硬编码阈值（替代 PERCENTILE_CONT——本轮不实现）
- crash_per_million_trips >= 800 → 高事故密度
- violation_per_thousand_trips >= 15 → 高违章密度
- 两项均低于中位数估计（300 / 5）→ 低风险
- 一项高于上四分位估计（800 / 15）→ 高风险
- 两项均高于上四分位估计 → extreme

## 已知限制
- top_crash_factor 留空——ROW_NUMBER 子查询本轮不实现
- PERCENTILE_CONT 由硬编码阈值替代
- violation_county 代码映射为 5 个已知代码（QN/BK/NY/BX/ST）
```

- [ ] **Step 2: 编写 RED 测试**

在 `tests/api/test_nyc_business_case.py` 末尾新增 `TestNYCCase06SqlPipeline` 类：

```python
# ════════════════════════════════════════════
# Case 06：区域安全合规画像（多步 DAG 跨域融合）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc06_spec_md() -> str:
    """读取 NYC 区域安全合规画像 DeveloperSpec。"""
    spec_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "nyc_safety_compliance_profile.md",
    )
    with open(spec_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def nyc06_csv_paths() -> dict:
    """Case 06 需要 5 张 CSV——3 域数据。"""
    base = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "fixtures", "nyc",
    )
    return {
        "fact_trips_sample": os.path.join(base, "fact_trips_sample.csv"),
        "dim_taxi_zone": os.path.join(base, "dim_taxi_zone.csv"),
        # 以下 CSV 待创建——若不存在则测试标记为 skip
        # "fact_crashes_sample": os.path.join(base, "fact_crashes_sample.csv"),
        # "dws_daily_parking_summary": os.path.join(base, "dws_daily_parking_summary.csv"),
    }


class TestNYCCase06SqlPipeline:
    """NYC 案例 06——多步 DAG 跨域融合全链路验证。"""

    def test_spec_parses_with_compute_steps(self, nyc06_spec_md):
        """Case 06 spec 解析——compute_steps 必须被识别（7 步 DAG）。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)

        # compute_steps 必须非空
        assert spec.compute_steps is not None, (
            "Case 06 应解析出 compute_steps"
        )
        assert len(spec.compute_steps) == 7, (
            f"应为 7 步 DAG，实际={len(spec.compute_steps)}"
        )

        # 零阻塞问题
        blocking = [q for q in spec.open_questions if q.blocking]
        assert len(blocking) == 0, (
            f"Case 06 spec 解析存在阻塞: {[q.description for q in blocking]}"
        )

    def test_build_plan_produces_sql_program(self, nyc06_spec_md):
        """build_plan() 应为 Case 06 产出 SqlProgram（非单语句）。"""
        pipeline = Pipeline()
        result = pipeline.build_plan(nyc06_spec_md)

        assert result.get("validation_passed") is True, (
            f"Validator 应通过: {result.get('open_questions')}"
        )
        # 多步 DAG 应输出 plan_id（FINAL 步骤的 plan）
        assert result.get("plan_id"), "应产出 plan_id"

    def test_sql_program_has_seven_statements(self, nyc06_spec_md):
        """SqlProgram 应包含 7 个 statement——对应 7 个 compute_steps。"""
        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)
        manifest = build_manifest_from_spec(spec)

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec, None)
        assert len(plans) == 7, (
            f"build_from_steps 应产出 7 个 plan，实际={len(plans)}"
        )

    def test_no_cte_in_compiled_sql(self, nyc06_spec_md):
        """编译产物中不得出现 WITH ... AS（CTE 禁止）。"""
        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)
        manifest = build_manifest_from_spec(spec)

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec, None)
        chain_id = hashlib.md5(
            "|".join(s.step_name for s in spec.compute_steps).encode()
        ).hexdigest()[:8]
        sql_program = build_sql_program_from_compute_steps(plans, spec, chain_id)

        # 通过 compile_program 编译——取最后一条 FINAL SQL
        compiler = DuckDbSqlCompiler()
        # compile_program 需 table_mapping——使用 auto 映射
        table_mapping = {}
        for t in spec.input_tables:
            if t.table_alias and t.source_table:
                table_mapping[t.table_alias] = str(t.source_table)
        compiler_with_map = DuckDbSqlCompiler(table_mapping=table_mapping)
        program_artifact = compiler_with_map.compile_program(sql_program)
        program_sql = program_artifact.compiled

        # 检查每条 SQL 不含 WITH
        for stmt_sql in program_sql.statements:
            sql_upper = stmt_sql.sql.upper()
            assert "WITH " not in sql_upper or "WITHIN GROUP" in sql_upper, (
                f"编译 SQL 不得包含 CTE (WITH ... AS): {stmt_sql.sql[:200]}"
            )
```

- [ ] **Step 3: 运行 RED 测试——验证全部失败**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SqlPipeline -v
```

预期：大部分测试 FAIL——spec 解析可能部分通过，但 build/compile/execute 路径因 CSV 数据缺失等原因失败。

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/nyc/nyc_safety_compliance_profile.md tests/api/test_nyc_business_case.py
git commit -m "test: Case 06 RED 测试——7 步 DAG SqlProgram 跨域融合 fixture + 骨架测试"
```

---

### Task 2: Pipeline compute_steps 路径的 Contract 提取补齐

**文件：**
- Modify: `src/tianshu_datadev/api/pipeline.py`（run_all 方法 compute_steps 分支）

**接口：**
- Consumes: `DataTransformContractExtractor.extract_v1()`（已有）
- Produces: `PipelineArtifactBundle.data_transform_contract`（V1 类型）

- [ ] **Step 1: 在 run_all() 的 compute_steps 分支后追加 Contract 提取**

当前 `run_all()` 在 compute_steps 路径中 compile → execute 后直接跳到结果返回，缺少 contract 提取。修改位置：在 execute 成功后、结果组装前插入 contract 提取代码。

在 `pipeline.py` 的 `run_all()` 方法中，找到 compute_steps 分支的 execute 成功块末尾（约在 `trace = ...; summary = ...` 之后），添加：

```python
# ── Contract 提取（compute_steps 路径）──
contract_extractor = DataTransformContractExtractor()
data_transform_contract = contract_extractor.extract_v1(
    sql_program,
    evidence_map=_build_evidence_map(hypothesis) if hypothesis else None,
)

# ── export_artifacts 用——将 SqlProgram 和 Contract 存入 results ──
self._store_result(request_id, {
    "parsed_spec": spec,
    "manifest": manifest,
    "plan": plan,
    "sql_program": sql_program,
    "compiled": compiled,
    "program_artifact": program_artifact,
    "trace": trace,
    "summary": summary,
    "table_mapping": table_mapping or {},
    "data_transform_contract": data_transform_contract,
})
```

- [ ] **Step 2: export_artifacts() 支持 SqlProgram 路径**

在 `export_artifacts()` 方法中增加对 `data_transform_contract` 的读取：

```python
def export_artifacts(self, request_id: str) -> PipelineArtifactBundle | None:
    data = self._results.get(request_id)
    if not data:
        return None
    
    return PipelineArtifactBundle(
        request_id=request_id,
        spec_hash=data.get("parsed_spec", None).spec_hash if data.get("parsed_spec") else "",
        sql_build_plan=data.get("plan"),
        data_transform_contract=data.get("data_transform_contract"),
        compiled_sql=data.get("compiled"),
        execution_trace=data.get("trace"),
        result_summary=data.get("summary"),
        snapshot_manifest=data.get("snapshot_manifest"),
    )
```

- [ ] **Step 3: 运行相关测试验证改动**

```bash
python -m pytest tests/api/test_nyc_business_case.py -q
python -m pytest tests/api/test_pipeline_api.py -q -k "contract"
```

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: Pipeline compute_steps 路径补齐 Contract V1 提取 + export_artifacts 支持 SqlProgram"
```

---

### Task 3: Case 06 样本 CSV + 简化集成测试

**文件：**
- Create: `tests/fixtures/nyc/fact_crashes_sample.csv`（手工构造 30 行样本）
- Create: `tests/fixtures/nyc/dws_daily_parking_summary.csv`（手工构造 20 行样本）
- Modify: `tests/api/test_nyc_business_case.py`（扩展 Case 06 测试类）

**接口：**
- Consumes: 现有 Pipeline.run_all() + DuckDBExecutor CSV 读取
- Produces: 通过测试——6 borough × 10 列输出

- [ ] **Step 1: 构造事故样本 CSV**

创建 `tests/fixtures/nyc/fact_crashes_sample.csv`——30 行，覆盖 5 个 borough：

```csv
crash_id,borough,persons_injured,persons_killed,contributing_factor_1,crash_date_key
1,Manhattan,2,0,Driver Inattention/Distraction,20250115
2,Manhattan,1,0,Failure to Yield Right-of-Way,20250220
3,Manhattan,0,0,Driver Inattention/Distraction,20250310
4,Manhattan,3,1,Unsafe Speed,20250405
5,Manhattan,1,0,Traffic Control Disregarded,20250512
6,Queens,1,0,Driver Inattention/Distraction,20250118
7,Queens,2,0,Failure to Yield Right-of-Way,20250225
8,Queens,0,0,Driver Inattention/Distraction,20250315
9,Queens,1,0,Unsafe Speed,20250410
10,Queens,0,0,Traffic Control Disregarded,20250520
11,Brooklyn,2,1,Driver Inattention/Distraction,20250120
12,Brooklyn,1,0,Failure to Yield Right-of-Way,20250228
13,Brooklyn,0,0,Driver Inattention/Distraction,20250318
14,Brooklyn,1,0,Unsafe Speed,20250412
15,Brooklyn,0,0,Traffic Control Disregarded,20250522
16,Bronx,1,0,Driver Inattention/Distraction,20250122
17,Bronx,0,0,Failure to Yield Right-of-Way,20250301
18,Bronx,1,0,Driver Inattention/Distraction,20250320
19,Bronx,0,0,Unsafe Speed,20250415
20,Bronx,2,1,Traffic Control Disregarded,20250525
21,Staten Island,0,0,Driver Inattention/Distraction,20250125
22,Staten Island,1,0,Failure to Yield Right-of-Way,20250305
23,Staten Island,0,0,Driver Inattention/Distraction,20250322
24,Staten Island,0,0,Unsafe Speed,20250418
25,Staten Island,1,0,Traffic Control Disregarded,20250528
26,Manhattan,1,0,Driver Inattention/Distraction,20250601
27,Queens,0,0,Failure to Yield Right-of-Way,20250605
28,Brooklyn,1,0,Driver Inattention/Distraction,20250610
29,Bronx,0,0,Unsafe Speed,20250615
30,Manhattan,2,0,Driver Inattention/Distraction,20250620
```

- [ ] **Step 2: 构造违章汇总 CSV**

创建 `tests/fixtures/nyc/dws_daily_parking_summary.csv`——20 行，覆盖 5 个 county 代码：

```csv
date_key,violation_county,violation_count,standard_fine_total
20250101,NY,150,7500.00
20250102,NY,120,6000.00
20250103,NY,180,9000.00
20250104,NY,90,4500.00
20250101,QN,80,4000.00
20250102,QN,60,3000.00
20250103,QN,95,4750.00
20250104,QN,70,3500.00
20250101,BK,100,5000.00
20250102,BK,85,4250.00
20250103,BK,110,5500.00
20250104,BK,75,3750.00
20250101,BX,40,2000.00
20250102,BX,35,1750.00
20250103,BX,50,2500.00
20250104,BX,30,1500.00
20250101,ST,15,750.00
20250102,ST,10,500.00
20250103,ST,20,1000.00
20250104,ST,12,600.00
```

- [ ] **Step 3: 运行 GREEN 测试——验证基础解析+编译路径**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SqlPipeline -v
```

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/nyc/fact_crashes_sample.csv tests/fixtures/nyc/dws_daily_parking_summary.csv tests/api/test_nyc_business_case.py
git commit -m "test: Case 06 样本 CSV + 集成测试——7 步 DAG 解析/编译/CTE 拒绝验证"
```

---

### Task 4: SqlBuildPlanBuilder 多源聚合验证 + Bug 修复

**文件：**
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py`（如需修复）

**接口：**
- Consumes: `build_from_steps()`（已有）
- Produces: 7 个独立 SqlBuildPlan——每个对应一个 compute_step

- [ ] **Step 1: 运行集成测试——定位 build_from_steps 问题**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SqlPipeline::test_sql_program_has_seven_statements -v
```

- [ ] **Step 2: 根据错误信息修复**

常见问题预判：
1. Step 1/2 的 `source: input` 表示从源表出发——builder 需正确识别 `input` 标记
2. Step 4/5 的 `source: [trip_boro_agg, crash_boro_agg]` 表示依赖多个上游——builder 需为这些步骤生成含 JoinStep 的 plan
3. Step 6 的 `compute_ratios` 无 join——仅为 ProjectStep，builder 需正确处理仅含计算列的步骤

修复后，如果 `build_from_steps` 已有正确处理逻辑，则本 Task 变为纯验证。

- [ ] **Step 3: 确认 7 个 plan 全部生成并通过 Validator**

```bash
python -m pytest tests/api/test_nyc_business_case.py::TestNYCCase06SqlPipeline -v
```

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/planning/sql_build_plan.py
git commit -m "fix: SqlBuildPlanBuilder build_from_steps 多源独立聚合 + 多依赖 Join 支持"
```

---

### Task 5: Case 06 全链路验证 + 文档更新

**文件：**
- Modify: `docs/current-state-and-verification-status.md`
- Modify: `tests/api/test_nyc_business_case.py`（最终 GREEN 验证）

**接口：**
- Consumes: Task 1-4 全部产物
- Produces: 全链路 GREEN + 状态文档更新

- [ ] **Step 1: 运行全量回归**

```bash
python -m pytest tests/planning/test_sql_program.py tests/planning/test_temp_table.py -q
python -m pytest tests/sql/test_pipeline_e2e.py -q
python -m pytest tests/api/test_nyc_business_case.py -q
python -m pytest tests/api/ tests/spark/ tests/artifacts/ -q
```

预期：≥ 661 passed / 11 skipped（零退化），Case 06 新增测试按实现进度通过。

- [ ] **Step 2: 运行 ruff + git diff**

```bash
python -m ruff check src/ tests/
git diff --check
```

预期：零告警。

- [ ] **Step 3: 更新项目状态文档**

在 `docs/current-state-and-verification-status.md` 中：
- 将 R7 风险状态从 "B" 更新为"已消除"或标注 Case 06 完成进度
- 新增 Case 06 条目到 Phase 进度矩阵

```markdown
| 10-Case06 | SqlProgram 多语句 DAG——NYC Case 06 | ✅/🟡 | ✅/🟡 | ✅/🟡 | 跨域融合 7 步 DAG，_temp_* 串联 |
```

- [ ] **Step 4: Commit**

```bash
git add docs/current-state-and-verification-status.md tests/api/test_nyc_business_case.py
git commit -m "feat: Case 06 全链路验证完成——7 步 DAG 跨域融合 + 状态文档更新"
```

---

## 九、验收命令

```bash
# 1. SqlProgram DAG 核心测试（零退化验证）
python -m pytest tests/planning/test_sql_program.py -q

# 2. Case 06 集成测试（新增）
python -m pytest tests/api/test_nyc_business_case.py -q

# 3. 全量回归
python -m pytest tests/api/ tests/spark/ tests/artifacts/ -q

# 4. 代码质量
python -m ruff check src/ tests/

# 5. 空白字符检查
git diff --check
```

---

## 十、A/B/C 分类

| 分类 | 内容 | 说明 |
|------|------|------|
| **A 类（必须实现）** | Case 06 fixture（compute_steps 格式）、Pipeline contract 提取补齐、SqlProgram 7 步编译/执行、CTE 禁止验证 | 核心交付——解锁 Case 06 多步 DAG 全链路 |
| **B 类（争取实现）** | Borough 字符串 JOIN 证据链（MEDIUM）、CaseWhenStep 风险标签渲染、归一化比率计算 | 提升 Case 06 业务完整性 |
| **C 类（明确不做）** | PERCENTILE_CONT、top_crash_factor ROW_NUMBER 子查询、violation_county 代码映射的通用化 | 超出本轮范围——使用硬编码替代 |

---

## 十一、推荐实现范围

**第一优先级（Task 1+2+3）**：Fixture + Pipeline 补齐 + 样本 CSV → 确保 7 步 DAG 的 parse→build→validate→compile→execute 全链路打通。

**第二优先级（Task 4+5）**：Builder bug 修复 + 全量回归 + 文档更新 → 确保零退化交付。

---

## 十二、不可触碰边界

1. **CTE 禁止**——任何编译产物、fixture、注释中不得出现 `WITH ... AS`（CTE）
2. **raw_sql 禁止**——所有步骤使用封闭类型化 step，无字符串逃生口
3. **不修改 SqlProgram IR Schema**——复用 Phase 3A 已建成的 SqlProgram/SqlStatement/TempTableSpec 模型
4. **不修改 Compiler/Executor 核心逻辑**——compile_program/execute_program 已在 Phase 3A 验收通过
5. **不引入新 step 类型**——Case 06 的 7 步全部使用已有 8 种 step（Scan/Aggregate/Join/Project/CaseWhen/Filter/Sort/Limit）
6. **不接入真实 LLM**——所有测试使用确定性 Fake 模式
7. **不写入生产数据库**——Executor 仅读取 CSV，输出到 DuckDB 内存

---

## 十三、残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R-C06-1 | PERCENTILE_CONT 未实现——风险等级使用硬编码阈值，可能与真实数据分布偏差较大 | B | 后续 Phase 引入窗口函数 PERCENTILE_CONT 支持后替换 |
| R-C06-2 | top_crash_factor 未实现——ROW_NUMBER 子查询不在本轮范围 | C | 标记为 HUMAN_REVIEW，人工补充 |
| R-C06-3 | violation_county 代码映射为简化实现——仅支持 NYC 5 个已知代码，非通用 | C | 若需要通用代码映射表，需额外的 DimTable 支持 |
| R-C06-4 | borough 字符串 JOIN 为 MEDIUM 证据——可能存在人为录入差异（如 "Staten Island" vs "Staten Is."） | B | 需人工确认 borough 名称一致性 |

---

## 十四、非技术人员解读

> **这不是允许写 CTE，而是把复杂查询拆成几步可审查的临时表流水线。**
>
> 想象你要做一道大菜：需要先分别处理三种食材（洗、切、腌制），再把它们合在一起烹饪。你不会把所有步骤写在一张巨大的菜谱卡片上——那样谁也看不懂。你会把每一步分开写：步骤 1 处理肉类，步骤 2 处理蔬菜，步骤 3 处理调料，步骤 4 混合烹饪。
>
> 数据库里也是一样。Case 06 需要分析 5 张不同来源的表，每张表的数据格式和粒度都不一样。强行写成一条 SQL 会非常复杂且难以审查。我们的做法是：
> - 步骤 1-3：分别对三组数据做预聚合（各自算好自己的统计数字）
> - 中间结果存在临时表（`_temp_*`）里——就像把切好的菜放在备菜盘里
> - 步骤 4-5：把备好的临时表按行政区关联起来
> - 步骤 6-7：计算风险指标 + 打标签
>
> **CTE（WITH ... AS）之所以被禁止**，是因为它会把所有步骤写在一个嵌套结构里——就像把所有步骤写在一张卡片的正反面、折页、贴纸上，审查者很难看清每步的输入输出。临时表方案让每一步的输入输出都清晰可见，每步可以独立审查、独立测试。

---

## 十五、是否可进入实施

**是。** 计划完整覆盖了用户要求的 7 项内容（为什么需要 DAG、如何物化、DAG/拓扑/命名规则、组件改动、本轮不做、CTE 保障、测试设计），无 C 类边界冲突，无 SQL 安全链路风险，CTE 禁止规则有多层防护。推荐使用 `superpowers:subagent-driven-development` 按 Task 1→5 顺序实施。
