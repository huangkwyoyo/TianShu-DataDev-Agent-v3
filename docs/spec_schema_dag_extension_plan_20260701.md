# Spec Schema 多步 DAG 计算扩展方案

## 一、CRCS 分类

**B 类 — DESIGN-REVIEW（设计方案评审）**

| 判定维度 | 结论 |
|----------|------|
| 是否纯机械改动？ | 否——涉及新增 Pydantic 模型、Builder 新分支、异常路径设计 |
| 是否影响架构边界？ | 不触及 SQL Generation Boundary / Validation Boundary / SafeIdentifier |
| 是否有多种可行方案？ | 是——Schema 字段命名、粒度（全量 vs 渐进）、向后兼容策略 |
| 是否需要非 AI 人员理解？ | 是——影响需求书的可表达范围，需产品/数据开发确认 |

不属于 A 类（非纯机械改动），也不属于 C 类（不改变安全红线），归入 **B 类**。

---

## 二、可复现的事实与证据

### 2.1 当前 Spec Schema 字段清单（逐项核实）

```python
# 来源：src/tianshu_datadev/developer_spec/models.py

ParsedDeveloperSpec:
  ├── spec_id: str                    # spec_{spec_hash[:12]}
  ├── spec_hash: str                  # 确定性哈希
  ├── title: str
  ├── description: str                # YAML summary + Markdown 正文
  ├── input_tables: list[InputTableDecl]    # 源表声明
  ├── metrics: list[MetricDecl]             # ← 扁平列表，单层聚合
  ├── dimensions: list[DimensionDecl]       # ← 扁平列表，与 metrics 配对
  ├── joins: list[JoinDecl] | None
  ├── time_range: TimeRangeDecl | None
  ├── output_spec: OutputSpecDecl
  ├── open_questions: list[OpenQuestion]
  └── parse_warnings: list[ParseWarning]
```

### 2.2 阻断级证据——当前无法表达的场景

**证据 1：多步聚合链无字段可填**

业务需求："先按天汇总订单金额，再按月对日汇总求平均，最后 Join 到用户表"

```yaml
# 当前 Spec 无法表达——metrics 只有一个列表，无法声明步骤依赖
metrics:
  - metric_name: daily_amount    # 第一步
    aggregation: SUM
    input_column: amount
  - metric_name: avg_daily_amount # 第二步——但 input_column 是第一步的产出
    aggregation: AVG              # "daily_amount" 不是源表字段，无法从 input_tables 引用
    input_column: daily_amount
```

Parser 报错路径：`Validator → 字段 daily_amount 未在 SourceManifest 中注册 → blocking OpenQuestion`

**证据 2：多分支聚合 + 合流无字段可填**

业务需求："A 表按地区汇总销售额，B 表按地区汇总退货额，两结果按地区 Join 计算退货率"

当前 `metrics` + `dimensions` 只能描述"对 A 和 B 做某一组聚合"，无法描述"先各自聚合，再 Join 两个聚合结果"。因为所有 metrics 共享同一组 dimensions，且 Join 只能发生在源表之间（通过 JoinDecl），不能 Join 两个聚合结果。

**证据 3：当前 Builder 的消费模式**

```
source: src/tianshu_datadev/planning/sql_build_plan.py

_build_single_table()      → 读取 spec.metrics → 生成 1 个 AggregateStep → 1 个 ProjectStep
_build_multi_table()       → 读取 spec.metrics → 生成 1 个 AggregateStep → 1 个 ProjectStep
_build_chain_step()        → 读取 spec.metrics → 最终步骤 1 个 AggregateStep，中间步骤透传
```

三条路径都假设 **1 次聚合**，没有任何代码路径能处理"第二次聚合的 input_column 是第一次聚合的产出"。

### 2.3 测试基线

- 当前 1312 个测试全部通过
- `test_multihop_chain.py` 的 4 个集成测试覆盖了 2 表链 Join，但聚合始终来自 `spec.metrics`
- 无任何测试覆盖"分步聚合"

---

## 三、正确的行为

本次修改完成后，以下三条路径应全部可用：

### 路径 A：单步聚合（向后兼容，现有行为不变）

```yaml
# 传统写法——不使用 compute_steps，走原有 metrics + dimensions
metrics:
  - metric_name: dau
    aggregation: COUNT_DISTINCT
    input_column: user_id
dimensions:
  - dimension_name: stat_date
    column_ref: order_time
```

Builder 走 `_build_single_table()` / `_build_multi_table()` ——**行为与修改前完全一致**。

### 路径 B：多步聚合链（新增能力）

```yaml
compute_steps:
  - step_name: daily
    source: input                      # 从源表直接计算
    group_by: [dt, user_id]
    metrics:
      - metric_name: daily_amount
        aggregation: SUM
        input_column: amount
    output_alias: _daily

  - step_name: monthly
    source: daily                      # 引用 daily 步骤的产出
    group_by: [month, user_id]
    metrics:
      - metric_name: avg_daily_amount
        aggregation: AVG
        input_column: daily_amount     # daily_amount 是 daily 步骤产出的列
    output_alias: _monthly
```

Builder 行为：
1. 构建 Spec DAG：`input → daily → monthly`
2. `daily` step → ScanStep(源表) + AggregateStep + ProjectStep(透传) → SqlStatement(PRODUCER, produces="_daily")
3. `monthly` step → ScanStep(_daily) + AggregateStep + ProjectStep → SqlStatement(PRODUCER, produces="_monthly")
4. 如果 monthly 是最后一步且 output_spec 声明了最终列 → SqlStatement(FINAL)
5. 组装为 SqlProgram，编译执行

### 路径 C：多分支聚合 + 合流（新增能力）

```yaml
compute_steps:
  - step_name: sales_by_region
    source: input
    group_by: [region]
    metrics:
      - metric_name: total_sales
        aggregation: SUM
        input_column: amount
    output_alias: _sales

  - step_name: returns_by_region
    source: input
    group_by: [region]
    metrics:
      - metric_name: total_returns
        aggregation: SUM
        input_column: return_amount
    output_alias: _returns

  - step_name: final
    source: [sales_by_region, returns_by_region]  # 两个输入 → Join
    group_by: [region]
    metrics:
      - metric_name: return_rate
        aggregation: DIVIDE
        input_column: "total_returns / total_sales"  # 表达式聚合
    # Join 信息通过 JoinDecl 或自动推断
```

Builder 行为：
1. 构建 Spec DAG：`input → sales_by_region ↘` 和 `input → returns_by_region ↘` → `final`
2. 两个独立 PRODUCER statement（可并行执行）
3. final step → ScanStep(_sales) + ScanStep(_returns) + JoinStep + AggregateStep → SqlStatement(FINAL)

---

## 四、当前修改不可触碰的边界

| 边界 | 约束 | 本次如何遵守 |
|------|------|-------------|
| SQL Generation Boundary | LLM 不生成 SQL 文本，SQL 只能由 Compiler 确定性渲染 | 新字段仍是结构化声明（step_name/group_by/metrics），不引入 raw_sql/expression: str |
| SafeIdentifier | 所有 table_ref/alias/column_name 须通过 `^[A-Za-z_][A-Za-z0-9_]*` 校验 | output_alias 使用 SafeIdentifier 类型 |
| StrictModel (extra="forbid") | 不接受未定义字段 | ComputeStep 继承 StrictModel，extra="forbid" |
| _FORBIDDEN_SQL_FIELDS | 禁止 raw_sql/where_sql/join_on: str/expression: str/aggregation_expr/having_sql | ComputeStep.metrics 复用 MetricDecl（其字段已在禁止检查白名单中），不新增自由 SQL 字段 |
| 确定性编译 | 相同 Spec → 相同 Plan → 相同 SQL | step_name 确定性排序，Spec DAG 拓扑排序使用 Kahn 算法（同 SqlProgram） |
| 10 Step IR 封闭性 | 不新增第 11 种 Step | 多步计算通过 SqlProgram 多 statement 实现（每个 statement 用现有 10 Step），不扩 Step 类型 |
| CTE 禁止 | CTE 永不实现 | 中间步骤通过 _temp 表传递（同 SqlProgram 现有机制） |
| Validator 防线 | 所有表引用/字段引用必须通过 SourceManifest 校验 | 中间步骤的 input_column 引用前面步骤产出时，产出列自动注册到 TempTableSpec |
| 现有测试不退化 | 1312 个现有测试保持通过 | compute_steps 为可选字段（None 默认），现有 fixture 不填 → 走原路径 |

---

## 五、当前修改要做到哪一步

**第一阶段（本次）：P0 缺陷——多步聚合链**

只做最小可验证的增量：

```
新增模型（models.py）:
  └── ComputeStep(StrictModel)      # 步骤声明
  └── ParsedDeveloperSpec 新增字段:
        └── compute_steps: list[ComputeStep] | None = None  # 可选，默认走原路径

扩展 Builder（sql_build_plan.py）:
  └── _build_from_compute_steps()   # 新方法——消费 compute_steps
  └── build() 增加分支:
        if spec.compute_steps and len(spec.compute_steps) > 0:
            return self._build_from_compute_steps(spec, hypothesis)
        else:
            # 走原有 _build_single_table / _build_multi_table

扩展 Pipeline（pipeline.py）:
  └── 无需改动——Spec DAG 产出的 SqlProgram 已能被 compile_program() + execute_program() 消费
```

### 不做的事情（明确声明）

| 不做 | 理由 |
|------|------|
| 不实现多分支合流（P0-2） | 需要 Join 两个 _temp 表——依赖 ComputeStep.source 支持 list（多输入），独立做 |
| 不实现 Top-N per group（P1-3） | 需要 FilterStep 支持过滤窗口函数结果——是 Step IR 扩展，不是 Spec Schema 扩展 |
| 不实现跨粒度指标依赖（P1-4） | 需要在不同粒度的聚合结果间 Join——属于 P0-2 子集 |
| 不实现 SpecEnricher 自动推断 ComputeStep | 本次只做 Schema + Builder + 手写 Spec 验证，LLM 推断留到下一轮 |
| 不修改 Compiler / Validator / PerfValidator | 中间步骤编译复用现有编译路径，temp_table 校验复用 SqlProgram 现有机制 |
| 不改 API routes / CLI | Schema 向后兼容，现有端点不变 |

### 涉及文件清单

| 文件 | 改动类型 | 预计行数 |
|------|----------|----------|
| `src/tianshu_datadev/developer_spec/models.py` | 新增 ComputeStep + ParsedDeveloperSpec 加字段 | ~50 |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 新增 _build_from_compute_steps() + build() 分支 | ~150 |
| `tests/sql/test_compute_steps.py` | 新增测试 | ~200 |
| `tests/fixtures/golden/golden_compute_steps.md` | 新增黄金用例 | ~40 |

---

## 六、以什么方式验收

### 6.1 自动化测试（CI 门禁）

| 测试层 | 测试内容 | 数量 |
|--------|----------|------|
| 单元测试 | ComputeStep Pydantic 校验：合法/非法字段、source 引用不存在 step_name、output_alias 合法性 | 8+ |
| 单元测试 | Spec DAG 拓扑排序：线性链、菱形依赖、环检测、孤立节点 | 6+ |
| 集成测试 | 2 步聚合链：daily→monthly，验证 SqlProgram 含 2 statement + depends_on 正确 | 3+ |
| 集成测试 | 3 步聚合链：daily→monthly→final Join，验证最终 SQL 执行结果正确 | 2+ |
| 回归测试 | 现有 1312 测试全部通过，compute_steps=None 时行为不变 | 1312 |
| 黄金用例 | 新增 golden_compute_steps.md → Pipeline.run_all() → 验证 Contract + SQL + ExecutionTrace | 1 |

### 6.2 验收标准

```
☐ 1312 个现有测试零退化
☐ 新增 20+ 测试全部通过
☐ 2 步聚合链（daily→monthly）SQL 在 DuckDB 上执行通过，结果正确
☐ 相同 compute_steps 两次 build() 产生相同 plan_id（确定性）
☐ compute_steps 中存在环时 Builder 抛出 CIRCULAR_DEPENDENCY（而非静默错误）
☐ compute_steps 中 source 引用不存在 step_name 时 Builder 抛出明确错误
```

### 6.3 手工验证

```bash
# 1. 用新黄金用例跑完整 Pipeline
tianshu run tests/fixtures/golden/golden_compute_steps.md

# 2. 确认中间 _temp 表在 ExecutionTrace 中可见
# 3. 确认最终输出列与 OutputSpecDecl 一致
# 4. 确认 SqlProgram 的 temp_tables 包含所有 output_alias

# 5. 回归——用旧黄金用例确认行为不变
tianshu run tests/fixtures/golden/golden_no_time_range.md
# → 应与修改前结果一致
```

---

## 七、不懂 AI 视角的解释

### 给产品经理 / 数据开发 TL

**现在的问题：**

想象你有一张"订单表"，你要做一个报表：先按天汇总销售额，再按月对天汇总求平均。这是两步计算——第一步的产出是第二步的输入。

当前系统的"需求书模板"（Spec Schema）只能描述"一步计算"：选择源表、选择字段、选一个聚合方式、选一个分组方式。你没办法在需求书里写"第二步的输入是第一步的产出"——因为模板里就没有这个空可以填。

结果就是：系统要么拒绝你的需求（"字段 daily_amount 不存在"），要么你只能自己手动写 SQL 绕过系统——这恰好是系统设计要避免的事情。

**这次要改什么：**

在需求书模板里新增一个叫 `compute_steps` 的章节——你可以像写菜谱一样，一步步声明计算的顺序：

```
第 1 步：从订单表按天汇总 → 产出叫 daily_summary
第 2 步：从 daily_summary 按月求平均 → 产出叫 monthly_summary
第 3 步：monthly_summary Join 用户表 → 最终输出
```

系统读取这些步骤，自动生成对应的 SQL 语句链（中间结果用临时表传递）。第 2 步的输入就是第 1 步的临时表，不需要程序员手动管理。

**为什么不能一步到位解决所有问题：**

当前发现了 10 个类似的表达能力缺口——多步计算、多路并行、Top-N 排名、跨粒度占比、自关联……。一次全改的风险太大：改多了可能影响现有的正常功能，测试也跟不上。

所以这次只改**最常见的场景**——"先算 A，再基于 A 算 B"——这是数据仓库日常开发中最频繁出现的模式。其他场景在后续迭代中逐步覆盖。

**改了之后，对使用者有什么影响：**

- **旧的需求书**：完全不受影响——`compute_steps` 是一个可选的新章节，不写就走原来的流程
- **新的需求书**：如果你需要分步计算，就写 `compute_steps`；如果只有一步，继续用原来的 `metrics` + `dimensions`
- **输出的 SQL**：多步计算会生成多条 SQL（每步一条），通过临时表串联——执行器自动按顺序执行

**为什么不让 LLM 自己发挥：**

系统的核心安全策略是"LLM 理解需求，但代码由规则生成"。如果让 LLM 直接写 SQL，可能写出删表、查错字段、引用不存在的表等危险操作。这次改动遵循同一策略——LLM 负责理解"业务需要分几步算"，填入结构化模板，然后由确定性代码生成安全 SQL。

---

## 附录 A：ComputeStep 模型设计草案

```python
class ComputeStepSource(str, Enum):
    """计算步骤的输入来源类型。"""
    INPUT = "input"       # 源表直接计算
    STEP = "step"         # 引用另一个 ComputeStep 的产出


class ComputeStep(StrictModel):
    """一次分步计算声明——单步聚合逻辑 + 输入来源 + 输出别名。

    多个 ComputeStep 构成计算 DAG。每个步骤等价于：
    SELECT group_by_cols, agg_func(input_column) AS alias
    FROM <source> GROUP BY group_by_cols
    """
    step_name: str                          # 步骤唯一名称（Spec 内不重复）
    source: str                             # "input" 或其它 step_name
    group_by: list[str]                     # GROUP BY 列名列表（来自 source 的列）
    metrics: list[MetricDecl]               # 此步骤的聚合指标（复用已有模型）
    output_alias: str                       # 产出别名——Builder 据此命名 _temp 表


class ParsedDeveloperSpec(StrictModel):
    # ... 现有字段保持不变 ...

    # Phase 5+ 新增：分步计算声明
    compute_steps: list[ComputeStep] | None = None  # None/空列表 → 走原路径
```

## 附录 B：Builder 扩展入口伪代码

```python
def build(self, spec, hypothesis=None):
    # 新增分支：如果声明了 compute_steps，走 DAG 路径
    if spec.compute_steps and len(spec.compute_steps) > 0:
        return self._build_from_compute_steps(spec, hypothesis)

    # 原有路径——不变
    is_multi = len(spec.input_tables) > 1
    if not is_multi:
        steps = self._build_single_table(spec)
    else:
        steps = self._build_multi_table(spec, hypothesis)
    # ... 组装 SqlBuildPlan，返回 (plan, [])
```

---

> **文档元信息**
> - 创建时间：2026-07-01
> - CRCS 分类：B 类 — DESIGN-REVIEW
> - 关联术语：[[datadev_engineering_glossary_20260629_1600]] §59-61（Spec Schema / ComputeStep / Spec DAG）
> - 依赖文档：[[AGENTS.md]] | [[01-target-architecture]] | [[spec_schema_defects_analysis_20260701]]
