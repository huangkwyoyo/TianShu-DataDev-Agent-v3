# RequirementPlanner 设计文档 v3.1

> 状态：待评审。未经批准不得开始编码。
>
> v3.1 修订：执行顺序反转（Planner 先于 SpecEnricher，禁止事后清空字段）/
> TimeTransformExpr 全链路补齐（SQL Predicate / Contract alias / Spark CASE / lite→v1 / SQL+Spark Comparator）/
> 修正伪代码（SparkAggregateStep 匹配真实模型）/ Validator 阻断传播 / NOT vs IS_NOT_NULL 澄清 / 文件清单修正

## 1. 架构与权威链路

```
Parser → SourceManifest
       → _prepare_spec_for_planning()           ← label_table 预处理（不变，先执行）
       → _enrich_and_plan():
            │
            ├─ 1. RequirementPlanner             ← 有 Adapter 时**先执行**
            │      ↓ Validator → Promotion       ← 推断基础维度/派生维度/基础指标/CASE WHEN
            │      ↓ 写入 spec 正式字段           ← Planner 产出直接落地到 spec
            │
            ├─ 2. SpecEnricher                   ← **后执行**——完整 scope（无 downstream 限制）
            │      ↓ apply_enrichment()           ← 自然只补充 Planner 未覆盖的字段
            │                                      ← 窗口/计算指标/跨粒度/窗口后过滤/维度映射
            │
            ├─ 3. 统一 unresolved 检查            ← Planner + SpecEnricher 都跑完后
            │      ↓ 仍 unresolved + 无 Key → ConfigError
            │
            └─ 4. RelationshipPlanner
       → Build → Validate → Compile → Execute
```

**权威优先级（冲突时严格按此顺序）：**

1. 程序员显式声明（YAML 中已有的 `dimensions` / `metrics` / `case_when_rules`）
2. `ProposalPromotion` 写入的结果（经验证的 `RequirementProposal`）——Planner 产出
3. `SpecEnricher.apply_enrichment()` 补充结果（完整 scope——窗口/计算指标/跨粒度/维度映射/窗口后过滤）

**关键设计决策——执行顺序反转（v3 → v3.1）：**

| 方面 | v3（旧） | v3.1（新） |
|------|---------|-----------|
| 执行顺序 | SpecEnricher(downstream) → Planner | Planner → SpecEnricher(full) |
| 隔离机制 | `scope="downstream"` + 事后清空 `inferred_metrics`/`inferred_dimensions` | 自然隔离——Planner 先填，SpecEnricher 后补，不冲突 |
| SpecEnricher scope | 人为裁剪 | 完整能力——`apply_enrichment` 只追加不覆盖，Planner 已填的字段自然跳过 |
| 无 Adapter 行为 | SpecEnricher(downstream) → unresolved → ConfigError | SpecEnricher(full) → unresolved → 仍 ConfigError |
| 优势 | — | 无"事后清空字段"的脆弱 hack；SpecEnricher 保留完整推断能力 |

**职责边界：**

| 组件 | 负责 | 约束 |
|------|------|------|
| RequirementPlanner | 基础维度、派生维度(HOUR)、基础指标、类型化 CASE WHEN | 仅输出 `RequirementPlannerOutput`，不直接改 Spec |
| ProposalValidator | 结构校验、字段事实、函数白名单、依赖提取、冲突检测 | 确定性代码，不调 LLM，不做语义推断 |
| ProposalPromotion | 将校验通过的 Proposal 写入 Spec 正式字段 | 写入 `dimensions`/`derived_dimensions`/`metrics`/`case_when_rules` |
| SpecEnricher | **完整 scope**：窗口、计算指标、跨粒度、维度映射、窗口后过滤、基础指标/维度（补充 Planner 遗漏） | 仅追加不覆盖——`apply_enrichment` 的 declared_aliases/declared_dim_names 检查自然保护 Planner 产出 |

---

## 2. 数据模型

### 2.1 模型放置规则

| 模型 | 文件 | 理由 |
|------|------|------|
| `DerivedGroupKey` | `planning/models.py` | SQL IR 层概念——与 `ColumnRef`、`AggregateStep` 同级 |
| `TimeTransformExpr` | `planning/models.py` | 可复用封闭标量表达式——SQL IR 层 |
| `DerivedDimensionDecl` | `developer_spec/models.py` | 开发者声明——与 `DimensionDecl` 同级 |
| `CaseWhenRule`, `CaseWhenBranch` | `developer_spec/models.py` | 业务规则声明 |
| `UncertaintyEntry` | `developer_spec/models.py` | LLM 输出模型 |
| `RequirementPlannerOutput` | `developer_spec/models.py` | LLM 输出模型 |
| `RequirementProposal` | `developer_spec/models.py` | 系统 Artifact |
| `ContractTimeTransform` | `artifacts/models.py` | Contract 层——与 `ContractAggregation` 同级 |
| `SparkTimeTransformExpr` | `spark/models.py` | Spark IR 层——与 `SparkAggregateStep` 同级 |

### 2.2 TimeTransformExpr — 可复用封闭标量表达式

```python
# planning/models.py — 新增

class TimeTransformExpr(StrictModel):
    """封闭时间变换表达式——HOUR(source_table.source_column)。

    可复用：DerivedGroupKey 持有它，Predicate.left 允许引用它，
    SQL SELECT / GROUP BY / CASE WHEN 共享 _render_time_transform() 渲染器。
    禁止转为 SqlRawExpression——Compiler 直接渲染为 HOUR(col) 或 hour(col)。
    """
    source_column: SafeIdentifier
    source_table: SafeIdentifier
    time_function: Literal["HOUR"]  # MVP 仅 HOUR
```

### 2.3 DerivedGroupKey — 持有 alias + TimeTransformExpr

```python
# planning/models.py — 新增

class DerivedGroupKey(StrictModel):
    """派生分组键——alias + TimeTransformExpr 的绑定。

    在 AggregateStep.group_keys 中使用。
    alias 是聚合后的列引用名——CASE WHEN 和 Project 通过此名引用。
    """
    alias: str
    expr: TimeTransformExpr
```

### 2.4 AggregateStep.group_keys 扩展

```python
# planning/models.py — 修改（现有文件，新增 union 类型）

class AggregateStep(StrictModel):
    step_type: Literal["aggregate"] = "aggregate"
    step_id: str
    group_keys: list[ColumnRef | DerivedGroupKey]  # ← 扩展 union
    metrics: list[AggregateSpec]
    having: Predicate | None = None
```

### 2.5 Predicate.left 扩展——允许 TimeTransformExpr

```python
# planning/models.py — 修改

class Predicate(StrictModel):
    left: ColumnRef | Predicate | TimeTransformExpr  # ← 扩展：CASE WHEN 条件可引用派生表达式
    operator: PredicateOperator
    right: ColumnRef | Predicate | SqlLiteral | list[SqlLiteral] | None = None
```

### 2.6 Developer Spec 模型

```python
# developer_spec/models.py — 新增（所有列表使用 Field(default_factory=list)）

class DerivedDimensionDecl(StrictModel):
    """派生维度——LLM 输出格式，Promotion 后成为 DerivedGroupKey 的输入。"""
    dimension_name: str
    source_column: str
    source_table: str
    time_function: Literal["HOUR"]

class CaseWhenBranch(StrictModel):
    """类型化 CASE WHEN 分支。"""
    condition: LabelPredicateCondition
    then_value: str

class CaseWhenRule(StrictModel):
    """CASE WHEN 规则——对应一条 CaseWhenStep。"""
    output_column: str
    branches: list[CaseWhenBranch] = Field(default_factory=list)
    else_value: str = ""

class UncertaintyEntry(StrictModel):
    """LLM 不确定项。"""
    field_ref: str
    description: str
    candidates: list[str] = Field(default_factory=list)

class RequirementPlannerOutput(StrictModel):
    """LLM 原始输出——所有列表使用 default_factory=list。"""
    dimensions: list[DimensionDecl] = Field(default_factory=list)
    derived_dimensions: list[DerivedDimensionDecl] = Field(default_factory=list)
    metrics: list[MetricDecl] = Field(default_factory=list)
    case_when_rules: list[CaseWhenRule] = Field(default_factory=list)
    uncertainties: list[UncertaintyEntry] = Field(default_factory=list)

class RequirementProposal(StrictModel):
    """系统 Artifact——元数据全部由系统生成。"""
    proposal_id: str
    spec_hash: str
    dimensions: list[DimensionDecl] = Field(default_factory=list)
    derived_dimensions: list[DerivedDimensionDecl] = Field(default_factory=list)
    metrics: list[MetricDecl] = Field(default_factory=list)
    case_when_rules: list[CaseWhenRule] = Field(default_factory=list)
    uncertainties: list[UncertaintyEntry] = Field(default_factory=list)
    llm_model: str = ""
    inference_time_ms: int = 0
    total_inferred: int = 0
```

### 2.7 ParsedDeveloperSpec 扩展

```python
# developer_spec/models.py — ParsedDeveloperSpec 新增字段

class ParsedDeveloperSpec(StrictModel):
    # ... 已有字段 ...
    derived_dimensions: list[DerivedDimensionDecl] = Field(default_factory=list)
    case_when_rules: list[CaseWhenRule] = Field(default_factory=list)
```

### 2.8 入站拒绝 vs Validator 检查

| 层级 | 机制 | 拒绝条件 | 测试方式 |
|------|------|---------|---------|
| **JSON Schema** | `predicate_root` 不含 `NOT` node_type | LLM 输出含 `{"node_type":"NOT",...}` → Adapter 层 structured output 拒绝 | JSON Schema 单元测试 |
| **Pydantic 入站** | `StrictModel` + `additionalProperties=false` | 未知字段、缺失必填字段、类型错误、枚举越界、NOT 节点（LabelNot 不在 LabelPredicateCondition union 中） | 模型单元测试——构造非法 JSON 断言 `ValidationError` |
| **Validator 检查** | `ProposalValidator.validate()` | 字段不存在于 SourceManifest、函数不在白名单、冲突检测、uncertainty 阻断 | Validator 单元测试——传入合法模型但业务上无效的数据 |

**NOT vs IS_NOT_NULL 澄清（v3.1 修正）：**

- **`NOT`** = `LabelNot` 谓词节点——逻辑否定包装器（`NOT(child_condition)`）。在 JSON Schema 的 `predicate_root` 中已排除（oneOf 不含 NOT），LLM 无法输出。Pydantic 入站时 `LabelPredicateCondition` union 不含 `LabelNot`，直接拒绝。Validator V10b 提供第三层防御。
- **`IS_NOT_NULL`** = 合法的叶子比较节点（`column IS NOT NULL`）。JSON Schema 的 `predicate_leaf` 包含此 node_type。**不在拒绝范围内**——这是有效的 CASE WHEN 条件。
- **`IN`** = 合法的比较操作符（`column IN (v1, v2, ...)`）。Prompt H4 约束 LLM 不使用，但 JSON Schema 的 `COMPARE` 节点支持 `op: "IN"`。验收时不单独测试 IN 阻断——由 Prompt 约束保证，Validator 不做额外拒绝。

---

## 3. 全链路贯通

### 3.1 SQL Builder——`_build_aggregate_step`

```python
# sql_build_plan.py — _build_aggregate_step 修订

def _build_aggregate_step(self, spec, primary_table: str) -> AggregateStep:
    group_keys: list[ColumnRef | DerivedGroupKey] = []

    # 基础维度 → ColumnRef
    for d in spec.dimensions:
        normalized = self._normalizer.normalize(d.column_ref)
        group_keys.append(ColumnRef(
            table_ref=d.source_table or primary_table,
            column_name=d.column_ref,
            normalized_name=normalized,
        ))

    # 派生维度 → DerivedGroupKey（含 TimeTransformExpr）
    for dd in spec.derived_dimensions:
        group_keys.append(DerivedGroupKey(
            alias=dd.dimension_name,
            expr=TimeTransformExpr(
                source_column=SafeIdentifier(dd.source_column),
                source_table=SafeIdentifier(dd.source_table),
                time_function=dd.time_function,
            ),
        ))

    # grain 补充
    for grain_col in spec.output_spec.grain:
        normalized = self._normalizer.normalize(grain_col)
        existing = {
            g.normalized_name if isinstance(g, ColumnRef) else g.alias
            for g in group_keys
        }
        if normalized not in existing:
            group_keys.append(ColumnRef(
                table_ref=primary_table,
                column_name=grain_col,
                normalized_name=normalized,
            ))

    # metrics 不变
    ...

    return AggregateStep(
        step_id=...,
        group_keys=group_keys,
        metrics=agg_metrics,
    )
```

### 3.2 SQL Builder——`_build_case_when_steps`（扩展读取 case_when_rules）

```python
# sql_build_plan.py — _build_case_when_steps 修订

def _build_case_when_steps(self, spec: ParsedDeveloperSpec) -> list:
    steps = []
    table_alias = spec.input_tables[0].table_alias

    # 构建 derived alias → TimeTransformExpr 映射——供 Predicate 引用
    derived_expr_map: dict[str, TimeTransformExpr] = {
        dd.dimension_name: TimeTransformExpr(
            source_column=SafeIdentifier(dd.source_column),
            source_table=SafeIdentifier(dd.source_table),
            time_function=dd.time_function,
        )
        for dd in spec.derived_dimensions
    }

    # 处理 label_rules（已有路径，不变）
    for rule in spec.label_rules:
        ...

    # 处理 case_when_rules（新路径）
    for rule in spec.case_when_rules:
        cases: list[WhenBranch] = []
        for branch in rule.branches:
            predicate = self._predicate_from_label_node(
                branch.condition, table_alias, derived_expr_map,
            )
            result = SqlLiteral(value=branch.then_value)
            cases.append(WhenBranch(condition=predicate, result=result))

        else_val = SqlLiteral(value=rule.else_value)
        steps.append(CaseWhenStep(
            step_id=SqlBuildPlan.generate_step_id("case_when", {
                "output_column": rule.output_column,
                "branch_count": len(cases),
            }),
            cases=cases,
            else_value=else_val,
            alias=SafeIdentifier(rule.output_column),
        ))

    return steps
```

**Predicate 防御性拒绝 LabelNot：**

```python
# sql_build_plan.py — _predicate_from_label_node 已有此检查，保持不变

def _predicate_from_label_node(self, node, table_alias, derived_expr_map=None):
    ...
    elif isinstance(node, LabelNot):
        raise ValueError(
            "label_table v1 暂不支持 LabelNot——"
            "LabelNot 应在 JSON Schema / Pydantic 入站阶段被拒绝，"
            "此处抛出说明前两层门禁未起作用。"
        )
```

### 3.3 SQL Compiler——`_render_aggregate` 只返回 SELECT 表达式

```python
# sql/compiler.py — _render_aggregate 修订
# 现有接口：返回 SELECT 列的字符串列表（供 _render_flat_sql 使用）
# 修订：新增 TimeTransformExpr 处理

def _render_aggregate(self, step: AggregateStep) -> list[str]:
    cols: list[str] = []

    # Group key 表达式 → SELECT 列
    for gk in step.group_keys:
        if isinstance(gk, DerivedGroupKey):
            # 复用同一渲染器——SELECT 用 "AS alias"
            rendered = self._render_time_transform(gk.expr)
            cols.append(f"{rendered} AS {gk.alias}")
        elif isinstance(gk, ColumnRef):
            if gk.table_ref:
                cols.append(f"{gk.table_ref}.{gk.column_name}")
            else:
                cols.append(gk.column_name)

    # 聚合指标
    for m in step.metrics:
        cols.append(self._render_aggregate_spec(m))

    return cols
```

### 3.4 SQL Compiler——`_render_flat_sql` 的 GROUP BY 分支

```python
# sql/compiler.py — _render_flat_sql 中 AggregateStep 处理修订

elif isinstance(step, AggregateStep):
    # SELECT 列——_render_aggregate 返回（含 TimeTransformExpr AS alias）
    agg_cols = self._render_aggregate(step)
    if agg_cols:
        select_cols = agg_cols
        has_aggregation = True

    # GROUP BY——显式处理 DerivedGroupKey
    for gk in step.group_keys:
        if isinstance(gk, DerivedGroupKey):
            # 复用同一渲染器——GROUP BY 不带 "AS alias"
            group_by_parts.append(
                self._render_time_transform(gk.expr)
            )
        elif isinstance(gk, ColumnRef):
            if gk.table_ref:
                group_by_parts.append(f"{gk.table_ref}.{gk.column_name}")
            else:
                group_by_parts.append(gk.column_name)

    # HAVING 不变
    ...
```

### 3.5 共享渲染器——`_render_time_transform`

```python
# sql/compiler.py — 新增静态方法

@staticmethod
def _render_time_transform(expr: TimeTransformExpr) -> str:
    """TimeTransformExpr → SQL 函数调用字符串。

    SELECT / GROUP BY / CASE WHEN 共享此渲染器。
    """
    return f"{expr.time_function}({expr.source_table}.{expr.source_column})"
```

### 3.6 SQL Predicate 渲染——Contract 提取器的 TimeTransformExpr 处理（v3.1 补齐）

```python
# artifacts/contract_extractor.py — _render_operand 修订

@staticmethod
def _render_operand(operand) -> str:
    """将 Predicate 的操作数渲染为人类可读字符串。"""
    if operand is None:
        return "None"
    # ColumnRef
    if hasattr(operand, "table_ref") and hasattr(operand, "column_name"):
        table = operand.table_ref
        col = operand.column_name
        if table:
            return f"{table}.{col}"
        return col
    # TimeTransformExpr（v3.1 新增）
    if hasattr(operand, "time_function") and hasattr(operand, "source_column"):
        return f"{operand.time_function}({operand.source_table}.{operand.source_column})"
    # SqlLiteral
    if hasattr(operand, "value"):
        v = operand.value
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return f"'{v}'"
        return str(v)
    # 嵌套 Predicate——递归渲染
    if hasattr(operand, "left") and hasattr(operand, "operator"):
        return DataTransformContractExtractor._render_operand(operand)
    return str(operand)
```

### 3.7 Spark Compiler——`groupBy(F.hour(...).alias(...)).agg(...)`

```python
# spark/compiler.py — _compile_aggregate 修订

def _compile_aggregate(self, resolved, step_id, index, total):
    step = resolved.step
    input_alias = resolved.input_vars[0]
    out_alias = resolved.output_var

    # Group key 表达式——含 time transforms
    group_parts: list[str] = []
    select_parts: list[str] = []

    # 普通 group_keys（list[str]）
    for gk in step.group_keys:
        col_str = self.renderer.render_column(gk)
        group_parts.append(col_str)
        select_parts.append(col_str)

    # time_transforms（list[SparkTimeTransformExpr]）——v3.1 新增
    for tt in step.time_transforms:
        # F.hour(F.col("ft.pickup_at")).alias("pickup_hour")
        func = tt.time_function  # "hour"
        src = f'F.col("{tt.source_table}.{tt.source_column}")'
        group_parts.append(f'F.{func}({src}).alias("{tt.alias}")')
        # 聚合后只引用 F.col(alias)，禁止再次访问 source_column
        select_parts.append(f'F.col("{tt.alias}")')

    # 聚合指标
    agg_parts = [...]
    for m in step.metrics:
        ...
        agg_parts.append(f'{agg_expr}.alias("{m.alias}")')
    select_parts.extend([f'F.col("{m.alias}")' for m in step.metrics])

    group_str = ", ".join(group_parts)
    agg_str = ", ".join(agg_parts)
    select_str = ", ".join(select_parts)

    raw = (
        f"{out_alias} = {input_alias}"
        f".groupBy({group_str})"
        f".agg({agg_str})"
        f".select({select_str})"
    )
    ...
```

**关键约束：聚合后只引用 `F.col(alias)`，禁止再次访问 `F.hour(F.col("source"))`。**

### 3.8 Spark CASE WHEN——TimeTransformExpr 条件渲染（v3.1 补齐）

CASE WHEN 在聚合后执行。派生维度的条件引用的是聚合后的 alias（如 `pickup_hour`），而非原始时间表达式。Spark 编译器无需修改——`_render_case_when_condition` 已正确工作：

```python
# spark/compiler.py — _render_case_when_condition（现有逻辑，无需修改）

# 当 CASE WHEN 条件引用 pickup_hour 时，Contract 提取器将其渲染为
# CaseWhenCondition(operator="IN", normalized_name="pickup_hour", value=[7,8,9,10,...])
# 编译器渲染为：F.col("pickup_hour").isin(7, 8, 9, 10, ...)
# 因为聚合后的 select() 已经输出了 pickup_hour 列，CASE WHEN 直接引用即可
```

**关键设计点：** CASE WHEN 条件中的 TimeTransformExpr 在 SQL 侧渲染为 `HOUR(ft.pickup_at)`（因为 SQL 允许在 CASE WHEN 中重复表达式），但在 Spark 侧，聚合后的 select 已输出 alias，CASE WHEN 条件直接引用 alias。Contract 提取器负责在提取时做这个转换——见 §3.11。

### 3.9 Contract 不变量

```python
# artifacts/models.py — 新增

class ContractTimeTransform(StrictModel):
    """Contract 侧时间变换——禁止 dict 逃生口。"""
    type: Literal["time_transform"] = "time_transform"
    source_column: str
    source_table: str
    time_function: str   # "HOUR"
    alias: str           # 输出别名——与 grouping_keys 中的逻辑名对应
```

**Contract 不变量规则：**

```
grouping_keys = ["pickup_hour", "borough"]     ← 逻辑 grain（纯名列表）
time_transforms = [{alias:"pickup_hour", ...}]  ← 变换详情

Mapper 规则：
  - grouping_keys 中的名与 time_transforms[].alias 同名时：
    → 用 transform 替换普通 key，禁止重复分组
  - 不同名时：保持为普通 ColumnRef 分组
```

### 3.10 Contract 提取器修订——alias 规范化（v3.1 补齐）

```python
# artifacts/contract_extractor.py — _extract_aggregate 修订

@staticmethod
def _extract_aggregate(step: AggregateStep):
    """从 AggregateStep 提取聚合、分组键、业务键和时间变换。"""
    aggs: list[ContractAggregation] = []
    groups: list[str] = []
    biz_keys: list[str] = []
    time_transforms: list[ContractTimeTransform] = []

    # 聚合指标
    for m in step.metrics:
        aggs.append(ContractAggregation(
            function=m.aggregation if isinstance(m.aggregation, str) else m.aggregation,
            input_column=m.input_column,
            alias=m.alias,
        ))

    # 分组键
    for gk in step.group_keys:
        if isinstance(gk, DerivedGroupKey):
            # 派生维度：用 alias 作为 grouping_key 名（alias 已是规范化名）
            groups.append(gk.alias)
            time_transforms.append(ContractTimeTransform(
                source_column=gk.expr.source_column,
                source_table=gk.expr.source_table,
                time_function=gk.expr.time_function,
                alias=gk.alias,
            ))
        elif isinstance(gk, ColumnRef):
            # 普通维度：用 normalized_name
            groups.append(gk.normalized_name)
            biz_keys.append(gk.normalized_name)

    return aggs, groups, biz_keys, time_transforms
```

**关键修正（v3.1）：** v3 设计使用 `gk.normalized_name` 但 `DerivedGroupKey` 没有 `normalized_name` 字段——它只有 `alias`。修正为 `gk.alias`，alias 由 Builder 在创建 DerivedGroupKey 时使用 `dd.dimension_name`（已归一化）填充。

### 3.11 Contract 提取器——CASE WHEN 条件的 TimeTransformExpr→alias 转换（v3.1 补齐）

```python
# artifacts/contract_extractor.py — _predicate_to_case_when_condition 修订

@staticmethod
def _predicate_to_case_when_condition(
    predicate: Predicate,
    derived_expr_map: dict[str, TimeTransformExpr] | None = None,
) -> CaseWhenCondition:
    """将 SQL Predicate AST 转换为 CaseWhenCondition。

    v3.1 新增：当 Predicate.left 是 TimeTransformExpr 时，
    通过 derived_expr_map 反查其 alias，Condition 引用 alias 而非原始表达式。
    这确保 Spark CASE WHEN 在聚合后正确引用列名。
    """
    derived_expr_map = derived_expr_map or {}

    # 构建 TimeTransformExpr → alias 反向映射
    expr_to_alias: dict[tuple[str, str, str], str] = {}
    for alias, expr in derived_expr_map.items():
        key = (expr.source_table, expr.source_column, expr.time_function)
        expr_to_alias[key] = alias

    left = predicate.left
    if isinstance(left, TimeTransformExpr):
        # 反查 alias——CASE WHEN 条件引用聚合后的 alias
        key = (left.source_table, left.source_column, left.time_function)
        alias = expr_to_alias.get(key)
        if alias is None:
            raise ValueError(
                f"TimeTransformExpr {left.time_function}({left.source_table}."
                f"{left.source_column}) 在 derived_expr_map 中无对应 alias"
            )
        # 使用 alias 作为条件引用名
        col_name = alias
    elif isinstance(left, ColumnRef):
        col_name = left.normalized_name
    else:
        col_name = str(left)

    op = predicate.operator.value if hasattr(predicate.operator, "value") else str(predicate.operator)

    # ... 其余逻辑不变
```

### 3.12 Spark Mapper——Contract 不变量应用

```python
# spark/mapper.py — _map_aggregations 修订

def _map_aggregations(
    aggregations: list[ContractAggregation],
    grouping_keys: list[str],
    time_transforms: list[ContractTimeTransform] | None = None,
    unsupported: list | None = None,
    gaps: list | None = None,
) -> list[SparkAggregateStep] | ContractGap | UnsupportedPattern:
    """将 Contract 的 aggregations 和 grouping_keys 映射为 AggregateStep。"""
    unsupported = unsupported or []
    gaps = gaps or []

    if not aggregations:
        return []

    # ... 聚合函数白名单检查不变 ...

    # 构建 alias → transform 查找表
    transform_map = {tt.alias: tt for tt in (time_transforms or [])}

    spark_group_keys: list[str] = []
    spark_time_transforms: list[SparkTimeTransformExpr] = []

    for key in grouping_keys:
        if key in transform_map:
            # 同名 → 用 transform 替换普通 key，禁止重复分组
            tt = transform_map[key]
            spark_time_transforms.append(SparkTimeTransformExpr(
                source_column=tt.source_column,
                source_table=tt.source_table,
                time_function=tt.time_function.lower(),  # HOUR → hour
                alias=tt.alias,
            ))
        else:
            spark_group_keys.append(key)

    metrics = [
        SparkAggregateSpec(
            function=_AGG_FUNCTION_MAP[a.function.upper()],
            input_column=a.input_column,
            alias=a.alias,
        )
        for a in aggregations
    ]

    return [SparkAggregateStep(
        input_alias="",
        group_keys=spark_group_keys,
        metrics=metrics,
        time_transforms=spark_time_transforms,
    )]
```

### 3.13 Spark 侧模型（匹配真实代码结构）

```python
# spark/models.py — 新增 SparkTimeTransformExpr + SparkAggregateStep 扩展

class SparkTimeTransformExpr(StrictModel):
    """Spark 侧时间变换表达式——从 ContractTimeTransform 确定性映射。"""
    source_column: str
    source_table: str
    time_function: str   # "hour"（已小写）
    alias: str

# SparkAggregateStep 新增字段（现有模型，新增 time_transforms）
class SparkAggregateStep(StrictModel):
    """Spark 聚合步骤——从 DataTransformContractV1.aggregations + grouping_keys 映射。"""
    step_type: SparkStepType = SparkStepType.AGGREGATE
    input_alias: str  # 输入 DataFrame 别名
    group_keys: list[str] = []  # 分组键（归一化字段名列表）——现有字段
    metrics: list[SparkAggregateSpec] = []  # 聚合指标——现有字段
    time_transforms: list[SparkTimeTransformExpr] = Field(default_factory=list)  # v3.1 新增
```

### 3.14 lite→v1 Adapter——time_transforms 透传（v3.1 补齐）

```python
# artifacts/models.py — DataTransformContractLite 新增字段

class DataTransformContractLite(StrictModel):
    # ... 已有字段 ...
    grouping_keys: list[str] = []
    # v3.1 新增
    time_transforms: list[ContractTimeTransform] = []

# artifacts/models.py — DataTransformContractV1 新增字段

class DataTransformContractV1(StrictModel):
    # ... 已有字段 ...
    grouping_keys: list[str] = []
    # v3.1 新增
    time_transforms: list[ContractTimeTransform] = []

# spark/contract_adapter.py — adapt_lite_to_v1 修订

def adapt_lite_to_v1(
    lite: DataTransformContractLite | DataTransformContractV1,
) -> DataTransformContractV1:
    # 幂等透传
    if isinstance(lite, DataTransformContractV1):
        return lite

    program_id = lite.source_sqlbuildplan_hash
    contract_id = DataTransformContractV1.generate_contract_id(program_id)

    return DataTransformContractV1(
        # ... 已有字段 ...
        grouping_keys=lite.grouping_keys,
        time_transforms=getattr(lite, "time_transforms", None) or [],  # v3.1 新增
        # ...
    )
```

### 3.15 SQL Comparator——DerivedGroupKey 扁平化（v3.1 补齐）

```python
# spark/plan_comparator.py — _flatten_aggregate_step 修订

@staticmethod
def _flatten_aggregate_step(step_dict: dict[str, Any]) -> dict[str, Any]:
    """扁平化 AggregateStep——group_keys ColumnRef/DerivedGroupKey → 字符串列表。

    v3.1 新增：DerivedGroupKey 提取 alias 作为 group key 名，
    并提取 time_transforms 列表供对比函数使用。
    """
    result = dict(step_dict)

    # 扁平化 group_keys: ColumnRef dict / DerivedGroupKey dict → 字符串
    raw_groups = result.get("group_keys", [])
    time_transforms: list[dict[str, Any]] = []
    if raw_groups:
        flat_groups: list[str] = []
        for g in raw_groups:
            if isinstance(g, dict):
                # DerivedGroupKey 特征：有 alias + expr（含 time_function）
                if "alias" in g and "expr" in g:
                    flat_groups.append(str(g["alias"]))
                    expr = g["expr"]
                    time_transforms.append({
                        "source_column": str(expr.get("source_column", "")),
                        "source_table": str(expr.get("source_table", "")),
                        "time_function": str(expr.get("time_function", "")),
                        "alias": str(g["alias"]),
                    })
                else:
                    # ColumnRef
                    flat_groups.append(
                        str(g.get("normalized_name") or g.get("column_name", ""))
                    )
            else:
                flat_groups.append(str(g))
        result["group_keys"] = flat_groups
        if time_transforms:
            result["time_transforms"] = time_transforms

    # 扁平化 metrics: aggregation → function（不变）
    # ...

    return result
```

### 3.16 Spark Comparator——time_transforms 扁平化（v3.1 补齐）

Spark 侧的 `SparkAggregateStep` 已经有 `time_transforms: list[SparkTimeTransformExpr]` 字段（v3.1 新增），`model_dump(mode="json")` 会自然将其序列化。`_flatten_aggregate_step` 对 Spark 侧的处理：`group_keys` 已是 `list[str]`（无需扁平化），但需要确保 `time_transforms` 字段出现在输出中：

```python
# spark/plan_comparator.py — _extract_spark_step_data 无需修改
# SparkAggregateStep.model_dump(mode="json") 已包含 time_transforms 字段
# _flatten_aggregate_step 检测到 group_keys 元素是字符串（非 dict）时直接保留
```

**对比函数修订：** `compare_aggregate_steps` 需要新增 time_transforms 对比逻辑——比较两侧的 `time_transforms` 列表（按 alias 匹配，比较 source_column/source_table/time_function）。

---

## 4. Scan 处理——source_column 必须包含

### 4.1 单表路径

```python
# sql_build_plan.py — _build_single_table 修订

def _build_single_table(self, spec):
    # 收集派生维度的源列
    derived_source_columns: set[tuple[str, str]] = set()  # (table, column)
    for dd in spec.derived_dimensions:
        derived_source_columns.add((dd.source_table, dd.source_column))

    # 收集 case_when_rules 条件引用的列
    case_when_source_columns: set[str] = set()
    for rule in spec.case_when_rules:
        for branch in rule.branches:
            self._collect_label_condition_columns(branch.condition,
                                                   case_when_source_columns)

    # Scan 构建——追加源列
    scan_cols = self._build_required_columns(table.table_alias, spec, table)
    existing_norm = {c.normalized_name for c in scan_cols}
    for table_ref, col_name in sorted(derived_source_columns):
        norm = self._normalizer.normalize(col_name)
        if norm not in existing_norm:
            existing_norm.add(norm)
            scan_cols.append(ColumnRef(
                table_ref=SafeIdentifier(table_ref),
                column_name=SafeIdentifier(col_name),
                normalized_name=SafeIdentifier(norm),
            ))
    for src_col in sorted(case_when_source_columns):
        norm = self._normalizer.normalize(src_col)
        if norm not in existing_norm:
            existing_norm.add(norm)
            scan_cols.append(ColumnRef(
                table_ref=SafeIdentifier(table.table_alias),
                column_name=SafeIdentifier(src_col),
                normalized_name=SafeIdentifier(norm),
            ))
```

### 4.2 多表路径——`_build_multi_table` 精确路径

```python
# sql_build_plan.py — _build_multi_table 修订

def _build_multi_table(self, spec, hypothesis):
    """
    多表构建路径：
    1. 每个 input_table → 独立 ScanStep
    2. 派生维度按 DerivedDimensionDecl.source_table 将源列加入对应 Scan
    3. case_when_rules 条件列按源表归属加入对应 Scan
    4. Join 步骤按 hypothesis.candidates 生成
    5. Join 后 → Aggregate（group_keys 保留表引用，DerivedGroupKey 有 source_table）
    6. CaseWhenSteps → Project
    """
    steps: list[StepNode] = []

    # 1. 按表分组——派生维度源列分配到对应 Scan
    table_scans: dict[str, ScanStep] = {}
    for table in spec.input_tables:
        scan = self._build_scan_for_table(table, spec)
        table_scans[table.table_alias] = scan

    # 2. 追加派生维度源列到对应 Scan
    for dd in spec.derived_dimensions:
        if dd.source_table in table_scans:
            scan = table_scans[dd.source_table]
            self._ensure_column_in_scan(scan, dd.source_column, dd.source_table)

    # 3. 追加 case_when 条件列到对应 Scan
    for rule in spec.case_when_rules:
        for branch in rule.branches:
            self._distribute_condition_columns_to_scans(
                branch.condition, table_scans, spec,
            )

    # 4. 添加所有 Scan
    for table in spec.input_tables:
        steps.append(table_scans[table.table_alias])

    # 5. Join 步骤
    for candidate in hypothesis.candidates:
        steps.append(self._build_join_step(candidate))

    # 6. Aggregate——Join 后保留表引用
    if spec.metrics:
        agg = self._build_aggregate_step(spec, primary_table="")
        steps.append(agg)

    # 7. CaseWhenSteps——引用 aggregate 后的 group key alias
    case_when_steps = self._build_case_when_steps(spec)
    steps.extend(case_when_steps)

    # 8. Project + Sort + Limit
    ...

    return steps
```

---

## 5. Pipeline 集成

### 5.1 执行顺序（v3.1 反转）

```
_parse_and_enrich():
  1. Parser → spec, manifest
  2. _prepare_spec_for_planning(spec)            ← label_table 预处理，不变，先执行
  3. _enrich_and_plan(spec, manifest):           ← 修订
       a. RequirementPlanner                      ← 有 Adapter 时先执行
            → Validator → Promotion              ← 写入基础维度/派生维度/指标/CASE WHEN
       b. SpecEnricher (完整 scope)               ← 后执行——窗口/计算指标/跨粒度/维度/窗口后过滤
            → apply_enrichment()                  ← 自然只追加 Planner 未覆盖的字段
       c. 统一 unresolved 检查                    ← 仍未解析 → ConfigError
       d. RelationshipPlanner
```

### 5.2 `_enrich_and_plan()` 修订（v3.1）

```python
# api/pipeline.py — _enrich_and_plan 修订

def _enrich_and_plan(
    self,
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest,
    table_mapping: dict | None = None,
) -> tuple[ParsedDeveloperSpec, RelationshipHypothesis | None, list[OpenQuestion], dict[str, str]]:
    if not table_mapping:
        table_mapping = _auto_table_mapping(spec)

    extra_questions: list[OpenQuestion] = []

    # ── 1. RequirementPlanner：有 Adapter 时先执行（v3.1 反转）──
    if (self._adapter is not None
            and spec.dataset_type != DatasetType.LABEL_TABLE):
        unresolved_before = _find_unresolved_derived_columns(spec)
        if unresolved_before:
            spec, planner_questions = self._run_requirement_planner(spec, manifest)
            extra_questions.extend(planner_questions)

    # ── 2. SpecEnricher：完整 scope，后执行（v3.1：无 scope 限制）──
    if spec.dataset_type != DatasetType.LABEL_TABLE:
        spec = self._spec_enricher.apply_enrichment(spec, manifest)
        # apply_enrichment 的 declared_aliases / declared_dim_names 检查
        # 自然保护 Planner 产出不被覆盖——无需 downstream_scope hack

    # ── 3. 统一 unresolved 检查 ──
    unresolved_after = _find_unresolved_derived_columns(spec)
    if unresolved_after:
        if self._adapter is None:
            raise ConfigError(
                "以下输出列无法解析且无 LLM Adapter 可用："
                f"{unresolved_after}"
            )
        else:
            raise ConfigError(
                f"RequirementPlanner + SpecEnricher 执行后仍存在未解析输出列: "
                f"{unresolved_after}"
            )

    # ── 4. RelationshipPlanner ──
    hypothesis = None
    if len(spec.input_tables) > 1:
        hypothesis, rel_questions = self._relationship_planner.plan(spec, manifest)
        extra_questions.extend(rel_questions)
        if hypothesis:
            xv_questions = cross_validate(spec, hypothesis, manifest)
            extra_questions.extend(xv_questions)

    return spec, hypothesis, extra_questions, table_mapping or {}
```

### 5.3 `_run_requirement_planner()` —— 返回 questions + 阻断传播（v3.1 修正）

```python
def _run_requirement_planner(
    self, spec: ParsedDeveloperSpec, manifest: SourceManifest,
) -> tuple[ParsedDeveloperSpec, list[OpenQuestion]]:
    """RequirementPlanner → Validator → Promotion。

    Returns:
        (spec, questions)——questions 由调用方合并到 _enrich_and_plan 返回值。

    阻断传播规则（v3.1 明确）：
        - Validator 返回 valid=False → 不执行 Promotion → 返回原 spec + questions
        - questions 中包含 level="blocking" 的项 → 调用方应阻断后续流程
        - 当前设计中：valid=False 时 spec 保持原样 → unresolved 检查捕获 →
          ConfigError 阻断（不依赖调用方手动检查 question level）
    """
    t0 = time.monotonic()

    # 1. LLM 调用
    planner_output = self._requirement_planner.plan(spec, manifest)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    # 2. 包装为 RequirementProposal
    proposal = RequirementProposal(
        proposal_id=_gen_uuid(),
        spec_hash=spec.spec_hash,
        dimensions=planner_output.dimensions,
        derived_dimensions=planner_output.derived_dimensions,
        metrics=planner_output.metrics,
        case_when_rules=planner_output.case_when_rules,
        uncertainties=planner_output.uncertainties,
        llm_model=self._adapter.model,
        inference_time_ms=elapsed_ms,
        total_inferred=(len(planner_output.dimensions)
                        + len(planner_output.derived_dimensions)
                        + len(planner_output.metrics)
                        + len(planner_output.case_when_rules)),
    )

    # 3. Validator——确定性检查
    valid, questions = self._proposal_validator.validate(proposal, spec, manifest)
    if not valid:
        # 阻断：不执行 Promotion，spec 保持原样
        # 调用方的 unresolved 检查会捕获未解析列 → ConfigError
        return spec, questions

    # 4. Promotion——写入 Spec 正式字段
    spec = self._proposal_promotion.promote(proposal, spec)

    return spec, questions
```

**阻断传播流程（v3.1 明确）：**

```
Validator 返回 valid=False
  → _run_requirement_planner 返回 (原 spec, questions)
  → _enrich_and_plan 中 spec 未被修改（Promotion 未执行）
  → SpecEnricher 运行（可能补充部分字段）
  → unresolved 检查仍失败 → ConfigError
  → questions 通过 _enrich_and_plan 返回值传给上层
```

### 5.4 SpecEnricher——移除 downstream_scope（v3.1）

v3.1 不再需要 `scope` 参数。SpecEnricher 始终以完整能力运行。隔离由执行顺序保证：

```python
# api/pipeline.py — _enrich_and_plan 中 SpecEnricher 调用（无 scope 参数）

spec = self._spec_enricher.apply_enrichment(spec, manifest)
# apply_enrichment 内部逻辑不变：
# - declared_aliases = {m.alias for m in spec.metrics}
# - new_metrics = [m for m in enriched.inferred_metrics if m.alias not in declared_aliases]
# - declared_dim_names = {d.dimension_name for d in spec.dimensions}
# - new_dimensions = [d for d in enriched.inferred_dimensions if d.dimension_name not in declared_dim_names]
# Planner 先执行 → spec 已有 dimensions/metrics/case_when_rules
# → SpecEnricher 的追加逻辑自然跳过已存在字段
```

---

## 6. ProposalValidator

```python
class ProposalValidator:
    """确定性校验——不调 LLM，不做语义推断。"""

    def validate(
        self,
        proposal: RequirementProposal,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[bool, list[OpenQuestion]]:
        """执行全部检查项，返回 (is_valid, questions)。

        valid=False 时，questions 中包含 level="blocking" 的项。
        调用方不应直接修改 spec——Promotion 仅在 valid=True 时执行。
        """
```

| # | 检查 | 阻断 | 说明 |
|---|------|------|------|
| V1 | `DimensionDecl.column_ref` 在 SourceManifest 中存在 | True | 列名必须来自源表 |
| V2 | `DerivedDimensionDecl.source_column` 在 SourceManifest 中存在 | True | 源列必须存在 |
| V3 | `DerivedDimensionDecl.time_function` 在 `{"HOUR"}` 中 | True | MVP 仅 HOUR |
| V4 | `MetricDecl.aggregation` 在 AggregationType 枚举中 | True | 聚合函数白名单 |
| V5 | `MetricDecl.input_column` (非 COUNT(*)) 在 SourceManifest 中存在 | True | 输入列必须存在 |
| V6 | `MetricDecl.filter.column` 在源表中存在 | True | 过滤列必须存在 |
| V7 | `CaseWhenRule.branches` 非空 | True | 空分支无意义 |
| V8 | `CaseWhenRule.else_value` 非空 | True | 缺少 ELSE 不可执行 |
| V9 | `CaseWhenRule.output_column` 在 `spec.output_spec.columns` 中存在 | True | 输出列必须声明 |
| V10 | `CaseWhenBranch.condition` 中所有列引用存在 | True | 条件列必须可解析 |
| V10b | `CaseWhenBranch.condition` 不含 `LabelNot` 节点 | True | 三层防御最内层（JSON Schema → Pydantic → Validator） |
| V11 | 与程序员显式声明的 alias 冲突检测 | True | 不可覆盖手写字段 |
| V12 | `uncertainties` 阻断判定 | 按规则 | 高置信度 uncertainty → blocking |
| V13 | 多表 CASE 条件只引用单个已验证的 group key/派生维度 | True | MVP 不跨多表条件 |

---

## 7. ProposalPromotion

```python
class ProposalPromotion:
    def promote(
        self,
        proposal: RequirementProposal,
        spec: ParsedDeveloperSpec,
    ) -> ParsedDeveloperSpec:
        """将校验通过的 Proposal 写入 Spec 正式字段。

        隔离机制：仅追加不覆盖——declared_names 检查确保程序员手写字段不被覆盖。
        Planner 先执行 → Promotion 先写入 → SpecEnricher 后执行时自然跳过已存在字段。
        """
        update = {}

        # dimensions
        declared_names = {d.dimension_name for d in spec.dimensions}
        new_dims = [d for d in proposal.dimensions
                    if d.dimension_name not in declared_names]
        if new_dims:
            update["dimensions"] = list(spec.dimensions) + new_dims

        # derived_dimensions → spec.derived_dimensions
        declared_derived = {d.dimension_name for d in spec.derived_dimensions}
        new_derived = [d for d in proposal.derived_dimensions
                       if d.dimension_name not in declared_derived]
        if new_derived:
            update["derived_dimensions"] = list(spec.derived_dimensions) + new_derived

        # metrics
        declared_aliases = {m.alias for m in spec.metrics}
        new_metrics = [m for m in proposal.metrics
                       if m.alias not in declared_aliases]
        if new_metrics:
            update["metrics"] = list(spec.metrics) + new_metrics

        # case_when_rules → spec.case_when_rules（正式字段，不包装 ComputeStep）
        declared_outputs = {r.output_column for r in spec.case_when_rules}
        new_rules = [r for r in proposal.case_when_rules
                     if r.output_column not in declared_outputs]
        if new_rules:
            update["case_when_rules"] = list(spec.case_when_rules) + new_rules

        return spec.model_copy(update=update) if update else spec
```

---

## 8. LLM Prompt 设计

### System Prompt

```markdown
你是数据开发规格分析 Agent。阅读程序员提供的业务描述和源表 Schema，
输出结构化的维度、派生维度、指标和 CASE WHEN 规则。

════════════════════════════════════════
硬约束
════════════════════════════════════════

H1. 列名只能从 [Table Schemas] 中选择，禁止编造。

H2. 聚合函数只能是：COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT

H3. 时间函数只能是：HOUR
    不要使用 DAY、MONTH、YEAR、DAY_OF_WEEK、DATE_TRUNC、EXTRACT 等。

H4. CASE WHEN 条件必须使用类型化 Predicate 树。
    禁止输出 when/then 字符串模式。
    条件只能使用 COMPARE / IS_NULL / IS_NOT_NULL / AND / OR。
    不要使用 NOT 节点——用反向比较操作符（!=、IS_NULL vs IS_NOT_NULL）代替。
    THEN 值为纯字符串字面量。

H5. 不确定时写入 uncertainties。只写 field_ref + description + candidates。
    不写 category——阻断级别由系统确定性规则决定。

H6. 不要覆盖 [Existing Declarations] 中程序员已手写的字段。

H7. label_table 类型不在你的职责范围——返回全空输出。

H8. 窗口函数、比率指标、跨粒度依赖不在你的职责范围——
    不给这些字段生成任何输出，也不生成 uncertainty。
```

### JSON Schema——predicate_root 不含 NOT

```json
{
  "$defs": {
    "literal": {
      "type": "object",
      "properties": {
        "node_type": {"const": "LITERAL"},
        "value": {},
        "data_type": {"type": "string", "enum": ["string", "number", "boolean", "null"]}
      },
      "required": ["node_type", "value", "data_type"],
      "additionalProperties": false
    },
    "predicate_leaf": {
      "oneOf": [
        {
          "type": "object",
          "properties": {
            "node_type": {"const": "COMPARE"},
            "left": {"type": "string"},
            "op": {"type": "string", "enum": ["=", "!=", ">", ">=", "<", "<=", "IN", "NOT_IN"]},
            "right": {"$ref": "#/$defs/literal"}
          },
          "required": ["node_type", "left", "op", "right"],
          "additionalProperties": false
        },
        {
          "type": "object",
          "properties": {
            "node_type": {"const": "IS_NULL"},
            "column": {"type": "string"}
          },
          "required": ["node_type", "column"],
          "additionalProperties": false
        },
        {
          "type": "object",
          "properties": {
            "node_type": {"const": "IS_NOT_NULL"},
            "column": {"type": "string"}
          },
          "required": ["node_type", "column"],
          "additionalProperties": false
        }
      ]
    },
    "predicate_root": {
      "oneOf": [
        {"$ref": "#/$defs/predicate_leaf"},
        {
          "type": "object",
          "properties": {
            "node_type": {"const": "AND"},
            "children": {"type": "array", "minItems": 2, "items": {"$ref": "#/$defs/predicate_root"}}
          },
          "required": ["node_type", "children"],
          "additionalProperties": false
        },
        {
          "type": "object",
          "properties": {
            "node_type": {"const": "OR"},
            "children": {"type": "array", "minItems": 2, "items": {"$ref": "#/$defs/predicate_root"}}
          },
          "required": ["node_type", "children"],
          "additionalProperties": false
        }
      ]
    }
  }
}
```

**关键设计决策：** JSON Schema 的 `predicate_root` 不含 `NOT` node_type。LLM 在 structured output 模式下无法输出 NOT 节点。Pydantic 入站时 `LabelPredicateCondition` union 不含 `LabelNot`。Validator V10b 提供第三层防御。三层防御确保 NOT 节点无法进入系统。

`IS_NOT_NULL` 是合法的叶子比较节点（`column IS NOT NULL`），不是逻辑否定——它存在于 `predicate_leaf` 中，不被任何一层拒绝。

---

## 9. CASE WHEN 执行位置

```
ads.hourly_borough_trip_ranking (aggregate_table):

Scan(ft: pickup_at, pickup_location_id, ...)
Scan(tz: borough, service_zone, ...)
  → Join(ft ⋈ tz)
  → Aggregate
      SELECT HOUR(ft.pickup_at) AS pickup_hour,   ← TimeTransformExpr → SELECT
             tz.borough,
             COUNT(*) AS trip_count,
             AVG(ft.distance_miles) AS avg_distance_miles,
             ...
      GROUP BY HOUR(ft.pickup_at), tz.borough     ← TimeTransformExpr → GROUP BY（同一渲染器）
  → CaseWhenStep
      CASE WHEN HOUR(ft.pickup_at) IN (7,8,9,10,17,18,19,20)  ← Predicate.left=TimeTransformExpr
           THEN '高峰' ELSE '平峰'
      END AS peak_type
  → Project（最终输出列）
```

**Spark 侧对应：**

```python
df.groupBy(
    F.hour(F.col("ft.pickup_at")).alias("pickup_hour"),  # ← 聚合前 transform
    F.col("borough")
).agg(
    F.count(F.lit(1)).alias("trip_count"),
    ...
).select(
    F.col("pickup_hour"),    # ← 聚合后只引用 alias，禁止 F.hour(...)
    F.col("borough"),
    ...
).withColumn(
    "peak_type",
    F.when(F.col("pickup_hour").isin(7,8,9,10,17,18,19,20), F.lit("高峰"))
     .otherwise(F.lit("平峰"))
)
# 注意：Spark CASE WHEN 引用的是聚合后的 alias "pickup_hour"，
# 不是 F.hour(...)。Contract 提取器在 _predicate_to_case_when_condition 中
# 将 TimeTransformExpr 转换为 alias 引用（见 §3.11）。
```

---

## 10. 完整文件变更清单（v3.1 修正）

### 源码文件（18 个）

| 操作 | 文件 | 说明 |
|------|------|------|
| **修改** | `planning/models.py` | +`TimeTransformExpr`、+`DerivedGroupKey`；`AggregateStep.group_keys` 扩展 union；`Predicate.left` 扩展 union |
| **修改** | `developer_spec/models.py` | +`DerivedDimensionDecl`、`CaseWhenBranch`、`CaseWhenRule`、`UncertaintyEntry`、`RequirementPlannerOutput`、`RequirementProposal`；`ParsedDeveloperSpec` +`derived_dimensions` +`case_when_rules` |
| **新建** | `planning/requirement_planner.py` | `RequirementPlanner` + Prompt + JSON Schema（~200 行） |
| **新建** | `planning/proposal_validator.py` | `ProposalValidator`——含 V10b/V13（~150 行） |
| **新建** | `planning/proposal_promotion.py` | `ProposalPromotion`——写入 `case_when_rules`（~90 行） |
| **修改** | `planning/sql_build_plan.py` | `_build_aggregate_step` 生成 `DerivedGroupKey`；`_build_case_when_steps` 扩展读取 `case_when_rules`；Scan 追加 `source_column`；`_build_multi_table` 精确路径 |
| **修改** | `sql/compiler.py` | +`_render_time_transform`；`_render_aggregate` 新增 `TimeTransformExpr` 分支；`_render_flat_sql` GROUP BY 处理 `DerivedGroupKey` |
| **修改** | `artifacts/models.py` | +`ContractTimeTransform`；`DataTransformContractLite` +`time_transforms`；`DataTransformContractV1` +`time_transforms` |
| **修改** | `artifacts/contract_extractor.py` | `_extract_aggregate` 序列化 `time_transforms`（用 `alias` 而非 `normalized_name`）；`_render_operand` 处理 `TimeTransformExpr`；`_predicate_to_case_when_condition` 转换 `TimeTransformExpr`→alias |
| **修改** | `spark/models.py` | +`SparkTimeTransformExpr`；`SparkAggregateStep` +`time_transforms` 字段 |
| **修改** | `spark/mapper.py` | `_map_aggregations` 新增 `time_transforms` 参数——同名 alias 用 transform 替换 plain key |
| **修改** | `spark/compiler.py` | `_compile_aggregate` 渲染 `time_transforms`——groupBy alias → select 只引用 alias |
| **修改** | `spark/contract_adapter.py` | `adapt_lite_to_v1` 透传 `time_transforms` 字段 |
| **修改** | `spark/plan_comparator.py` | `_flatten_aggregate_step` 处理 `DerivedGroupKey`（SQL 侧）+ `SparkTimeTransformExpr`（Spark 侧）；`compare_aggregate_steps` 新增 `time_transforms` 对比 |
| **修改** | `api/pipeline.py` | `_enrich_and_plan` 执行顺序反转：Planner → SpecEnricher；`_run_requirement_planner` 返回 questions |
| **修改** | `planning/__init__.py` | 导出新组件 |
| **修改** | `api/app.py` | `create_app()` 注入 `RequirementPlanner` |
| **删除** | `planning/spec_enricher.py` | 移除 `apply_enrichment` 的 `scope` 参数（v3.1 不再需要） |

### 测试文件（8 个）

| 操作 | 文件 | 说明 |
|------|------|------|
| **新建** | `tests/planning/test_time_transform_expr.py` | `TimeTransformExpr` SQL+Spark 编译；`Predicate.left` 引用；SELECT/GROUP BY/CASE WHEN 共享渲染器 |
| **新建** | `tests/planning/test_proposal_validator.py` | Validator 全检查项——含 V10b `LabelNot` 拒绝、V13 多表约束 |
| **新建** | `tests/planning/test_proposal_promotion.py` | Promotion→`case_when_rules` 字段 |
| **新建** | `tests/planning/test_contract_time_transform.py` | `ContractTimeTransform`→Mapper→Comparator 全链路 + lite→v1 透传 |
| **新建** | `tests/planning/test_requirement_planner_e2e.py` | FakeAdapter→Planner→Validator→Promotion→Builder→CaseWhenStep |
| **修改** | `tests/api/conftest.py` | 注入 FakeRequirementAdapter |
| **修改** | `tests/sql/test_compiler.py` | `DerivedGroupKey` 编译回归 |
| **修改** | `tests/spark/test_spark_compiler.py` | `SparkAggregateStep`+`time_transforms` 编译回归 |

---

## 11. MVP 验收标准

### 11.1 通过条件

| 输出列 | 应解析为 |
|--------|---------|
| `pickup_hour` | `DerivedDimensionDecl(HOUR, pickup_at, ft)` → `DerivedGroupKey(alias="pickup_hour", expr=TimeTransformExpr(...))` |
| `peak_type` | `CaseWhenRule` → `CaseWhenStep`——条件引用 `TimeTransformExpr` 在 `Predicate.left` |

**Scan 验证：`pickup_at` 在 `ft` 的 `ScanStep.required_columns` 中。**

**SQL 验证：**
- `SELECT HOUR(ft.pickup_at) AS pickup_hour, ...`
- `GROUP BY HOUR(ft.pickup_at), ...`
- `CASE WHEN HOUR(ft.pickup_at) IN (...) THEN '高峰' ...`——同一 `_render_time_transform()` 渲染

**Spark 验证：**
- `F.hour(F.col("ft.pickup_at")).alias("pickup_hour")` 在 groupBy 中
- `F.col("pickup_hour")` 在 select 中——聚合后禁止 `F.hour(...)`
- `F.when(F.col("pickup_hour").isin(7,8,9,10,17,18,19,20), F.lit("高峰")).otherwise(F.lit("平峰"))`——引用 alias 而非重算

**Contract 验证：**
- `grouping_keys = ["pickup_hour", "borough"]`
- `time_transforms = [{alias:"pickup_hour", source_column:"pickup_at", source_table:"ft", ...}]`
- Mapper 看到同名 → 用 transform 替换 plain key
- lite→v1 适配器透传 `time_transforms` 字段

**Comparator 验证：**
- SQL 侧 `_flatten_aggregate_step` 从 `DerivedGroupKey` 提取 `alias` + `time_transforms`
- Spark 侧 `_flatten_aggregate_step` 从 `SparkTimeTransformExpr` 提取 `time_transforms`
- `compare_aggregate_steps` 对比两侧 `time_transforms` 一致

### 11.2 阻断场景

| 场景 | 预期结果 | 阻断层 |
|------|---------|--------|
| 未知列名 | Validator V1/V2/V5 → blocking=True | Validator |
| CASE WHEN 无 ELSE | Validator V8 → blocking=True | Validator |
| CASE WHEN 空 branches | Validator V7 → blocking=True | Validator |
| 时间函数不是 HOUR | Validator V3 → blocking=True | Validator |
| CASE WHEN 条件含 `LabelNot` 节点 | JSON Schema 不含 NOT → Adapter structured output 拒绝 → Pydantic 入站拒绝 → Validator V10b 三层防御 | JSON Schema / Pydantic / Validator |
| CASE WHEN 条件含 `IS_NOT_NULL`（合法） | 正常通过——`IS_NOT_NULL` 是叶子比较节点，不是 `LabelNot` | 无阻断 |
| 多表 CASE 引用多个 group key | Validator V13 → blocking=True | Validator |
| 完整显式 Spec 无 Key | 确定性运行继续——无 unresolved 列 → 跳过 Planner → SpecEnricher 正常运行 | Pipeline |
| 无 Adapter + unresolved 列 | SpecEnricher 后仍 unresolved → ConfigError | Pipeline |
| LLM 输出含未知字段 | Pydantic 入站拒绝 → `ValidationError` | Pydantic |
| 无 API Key + 有 unresolved | `_enrich_and_plan` 中 `self._adapter is None` → ConfigError | Pipeline |

### 11.3 回归验证

- 现有全部测试通过（601 passed）
- label_table 路径不受影响
- Ruff 零告警
- `SparkAggregateStep.time_transforms=[]` 时编译结果与现有基线一致

---

## 12. 不实施清单

- DAY / MONTH / YEAR / DAY_OF_WEEK 时间函数
- 多表 CASE WHEN 引用多个 group key（V13 阻断）
- 任意算术表达式（raw expression）
- 窗口函数推断、比率指标推断、跨粒度依赖检测（这些是 SpecEnricher 的职责，不在 Planner 范围）
- label_table 的 RequirementPlanner 处理
- 生产环境 Fake 回退规则
- 新状态机 / LangGraph 集成
- 通用 SQL 表达式解析器
- 聚合前 ProjectStep 中间层
- ComputeStep 包装 CASE WHEN
- `LabelNot` 谓词节点（三层防御拒绝）
- 子查询层
- SpecEnricher `scope` 参数（v3.1 移除——执行顺序保证隔离）

---

## 附录 A：v3 → v3.1 变更摘要

| # | 变更 | v3 | v3.1 |
|---|------|-----|------|
| 1 | **执行顺序** | SpecEnricher(downstream) → Planner | Planner → SpecEnricher(full) |
| 2 | **隔离机制** | `scope="downstream"` + 事后清空字段 | 自然隔离——Planner 先填，SpecEnricher 仅追加 |
| 3 | **SQL Predicate 渲染** | 未覆盖 | `_render_operand` 处理 TimeTransformExpr |
| 4 | **Contract alias** | 错用 `normalized_name` | 修正为 `DerivedGroupKey.alias` |
| 5 | **Spark CASE WHEN** | 未明确 | Contract 提取器将 TimeTransformExpr→alias；编译器无需修改 |
| 6 | **lite→v1 adapter** | 未覆盖 | `DataTransformContractLite/V1` +`time_transforms`；`adapt_lite_to_v1` 透传 |
| 7 | **SQL Comparator** | 未覆盖 | `_flatten_aggregate_step` 处理 DerivedGroupKey→alias + time_transforms |
| 8 | **Spark Comparator** | 未覆盖 | `_flatten_aggregate_step` 处理 SparkTimeTransformExpr；`compare_aggregate_steps` 对比 |
| 9 | **SparkAggregateStep** | 伪代码不匹配真实模型 | 匹配真实字段 `group_keys: list[str]` + 新增 `time_transforms` |
| 10 | **Validator 阻断传播** | 未明确流程 | 明确：valid=False → Promotion 不执行 → spec 不变 → unresolved→ConfigError |
| 11 | **NOT/IN 澄清** | NOT 和 IS_NOT_NULL 混淆 | 明确：NOT=LabelNot（拒绝），IS_NOT_NULL=叶子比较（合法），IN 由 Prompt 约束 |
| 12 | **文件清单** | `planning/models.py` 标为"新建" | 修正为"修改"（现有文件）；新增 `contract_adapter.py` 修改项 |
