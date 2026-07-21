# RequirementPlanner v3.1 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 RequirementPlanner 组件——从自然语言业务描述生成结构化维度/派生维度/指标/CASE WHEN 声明，含 TimeTransformExpr 全链路（SQL/Spark/Contract/Comparator）。

**Architecture:** 分 6 阶段 16 个任务。Phase 1 建模型基础（3 任务），Phase 2 改造编译器+Builder（3 任务），Phase 3 贯通 Spark 链路（1 任务），Phase 4 实现 Planner 核心三件套（3 任务），Phase 5 管线集成（1 任务），Phase 6 全覆盖测试（5 任务）。

**Tech Stack:** Python 3.12+ / Pydantic v2 (StrictModel) / pytest / DuckDB / PySpark

## Global Constraints

- 所有代码注释必须使用中文
- MVP 仅支持 HOUR 时间函数——禁止 DAY/MONTH/YEAR/DAY_OF_WEEK
- 禁止 raw expression、新状态机、子查询层、ComputeStep 包装 CASE WHEN
- `LabelNot` 节点三层防御拒绝（JSON Schema → Pydantic → Validator V10b）
- 现有 601 个测试必须全部通过（零回归）
- Ruff 零告警
- 仅追加不覆盖——程序员手写字段优先级最高
- `SparkAggregateStep.time_transforms=[]` 时编译结果与现有基线一致

---

### Task 1: planning/models.py — TimeTransformExpr + DerivedGroupKey + Predicate.left 扩展

**Files:**
- Modify: `src/tianshu_datadev/planning/models.py` (在 `AggregateSpec` 之前新增两个模型，修改 `Predicate.left` union)
- Test: `tests/planning/test_time_transform_expr.py` (新建)

**Interfaces:**
- Produces: `TimeTransformExpr(source_column, source_table, time_function)` — 可复用封闭标量表达式
- Produces: `DerivedGroupKey(alias, expr: TimeTransformExpr)` — 派生分组键
- Modifies: `Predicate.left: ColumnRef | Predicate | TimeTransformExpr` — 扩展 union
- Modifies: `AggregateStep.group_keys: list[ColumnRef | DerivedGroupKey]` — 在 `sql_build_plan.py:125` 修改

- [ ] **Step 1: 编写 TimeTransformExpr + DerivedGroupKey 模型单元测试**

```python
# tests/planning/test_time_transform_expr.py
import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    TimeTransformExpr,
    DerivedGroupKey,
    SafeIdentifier,
    Predicate,
    PredicateOperator,
    ColumnRef,
)


class TestTimeTransformExpr:
    """TimeTransformExpr 模型校验测试。"""

    def test_valid_hour_expr(self):
        """合法 HOUR 表达式应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        assert expr.time_function == "HOUR"
        assert str(expr.source_column) == "pickup_at"

    def test_rejects_invalid_time_function(self):
        """非法时间函数应被 Literal 拒绝。"""
        with pytest.raises(ValidationError):
            TimeTransformExpr(
                source_column=SafeIdentifier("pickup_at"),
                source_table=SafeIdentifier("ft"),
                time_function="DAY",  # MVP 仅 HOUR
            )


class TestDerivedGroupKey:
    """DerivedGroupKey 模型校验测试。"""

    def test_valid_derived_key(self):
        """合法 DerivedGroupKey 应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        key = DerivedGroupKey(alias="pickup_hour", expr=expr)
        assert key.alias == "pickup_hour"
        assert key.expr.time_function == "HOUR"


class TestPredicateWithTimeTransform:
    """Predicate.left 扩展——允许 TimeTransformExpr。"""

    def test_predicate_left_with_time_transform(self):
        """Predicate.left 为 TimeTransformExpr 时应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        pred = Predicate(
            left=expr,
            operator=PredicateOperator.IN,
            right=[
                SqlLiteral(value=7), SqlLiteral(value=8), SqlLiteral(value=9),
            ],
        )
        assert isinstance(pred.left, TimeTransformExpr)
        assert pred.operator == PredicateOperator.IN

    def test_predicate_left_still_accepts_column_ref(self):
        """Predicate.left 仍应接受 ColumnRef——向后兼容。"""
        col = ColumnRef(
            table_ref=SafeIdentifier("ft"),
            column_name=SafeIdentifier("borough"),
            normalized_name=SafeIdentifier("borough"),
        )
        pred = Predicate(
            left=col,
            operator=PredicateOperator.EQ,
            right=SqlLiteral(value="Manhattan"),
        )
        assert isinstance(pred.left, ColumnRef)
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3"
python -m pytest tests/planning/test_time_transform_expr.py -v
```

预期：`TimeTransformExpr` 未定义 → `ImportError`

- [ ] **Step 3: 在 planning/models.py 中新增 TimeTransformExpr + DerivedGroupKey**

在 `src/tianshu_datadev/planning/models.py` 的 `SafeIdentifier` 定义之后、`AggregateSpec` 之前插入：

```python
# ── TimeTransformExpr + DerivedGroupKey（v3.1 新增）──

class TimeTransformExpr(StrictModel):
    """封闭时间变换表达式——HOUR(source_table.source_column)。

    可复用：DerivedGroupKey 持有它，Predicate.left 允许引用它，
    SQL SELECT / GROUP BY / CASE WHEN 共享 _render_time_transform() 渲染器。
    禁止转为 SqlRawExpression——Compiler 直接渲染为 HOUR(col) 或 hour(col)。
    """
    source_column: SafeIdentifier
    source_table: SafeIdentifier
    time_function: Literal["HOUR"]  # MVP 仅 HOUR


class DerivedGroupKey(StrictModel):
    """派生分组键——alias + TimeTransformExpr 的绑定。

    在 AggregateStep.group_keys 中使用。
    alias 是聚合后的列引用名——CASE WHEN 和 Project 通过此名引用。
    """
    alias: str
    expr: TimeTransformExpr
```

- [ ] **Step 4: 修改 Predicate.left union 类型**

修改 `src/tianshu_datadev/planning/models.py:180`：

```python
# 修改前：
left: ColumnRef | Predicate
# 修改后：
left: ColumnRef | Predicate | TimeTransformExpr
```

- [ ] **Step 5: 运行测试确认通过 + 已有测试零回归**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
python -m pytest tests/planning/ -v --timeout=60
```

- [ ] **Step 6: Commit**

```bash
git add tests/planning/test_time_transform_expr.py src/tianshu_datadev/planning/models.py
git commit -m "feat: 新增 TimeTransformExpr + DerivedGroupKey 模型，扩展 Predicate.left union

- TimeTransformExpr: 封闭标量表达式 HOUR(source_table.source_column)
- DerivedGroupKey: alias + TimeTransformExpr 绑定
- Predicate.left 扩展为 ColumnRef | Predicate | TimeTransformExpr
- MVP 仅支持 HOUR 时间函数（Literal['HOUR']）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: developer_spec/models.py — Developer 侧模型 + ParsedDeveloperSpec 扩展

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py` (新增 6 个模型 + ParsedDeveloperSpec 扩展)
- Test: `tests/planning/test_time_transform_expr.py` (追加测试)

**Interfaces:**
- Produces: `DerivedDimensionDecl(dimension_name, source_column, source_table, time_function)`
- Produces: `CaseWhenBranch(condition: LabelPredicateCondition, then_value)`
- Produces: `CaseWhenRule(output_column, branches, else_value)`
- Produces: `UncertaintyEntry(field_ref, description, candidates)`
- Produces: `RequirementPlannerOutput(dimensions, derived_dimensions, metrics, case_when_rules, uncertainties)`
- Produces: `RequirementProposal(proposal_id, spec_hash, dimensions, derived_dimensions, metrics, case_when_rules, uncertainties, llm_model, inference_time_ms, total_inferred)`
- Modifies: `ParsedDeveloperSpec` + `derived_dimensions: list[DerivedDimensionDecl]` + `case_when_rules: list[CaseWhenRule]`

- [ ] **Step 1: 编写 Developer 侧模型单元测试**

在 `tests/planning/test_time_transform_expr.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Task 2: Developer Spec 模型测试
# ════════════════════════════════════════════

from tianshu_datadev.developer_spec.models import (
    DerivedDimensionDecl,
    CaseWhenBranch,
    CaseWhenRule,
    UncertaintyEntry,
    RequirementPlannerOutput,
    RequirementProposal,
    ParsedDeveloperSpec,
    DimensionDecl,
    MetricDecl,
    AggregationType,
)


class TestDerivedDimensionDecl:
    """派生维度声明模型测试。"""

    def test_valid_derived_dimension(self):
        dd = DerivedDimensionDecl(
            dimension_name="pickup_hour",
            source_column="pickup_at",
            source_table="ft",
            time_function="HOUR",
        )
        assert dd.dimension_name == "pickup_hour"

    def test_rejects_invalid_time_function(self):
        with pytest.raises(ValidationError):
            DerivedDimensionDecl(
                dimension_name="pickup_day",
                source_column="pickup_at",
                source_table="ft",
                time_function="DAY",
            )


class TestCaseWhenRule:
    """CASE WHEN 规则模型测试。"""

    def test_valid_case_when_rule(self):
        rule = CaseWhenRule(
            output_column="peak_type",
            branches=[
                CaseWhenBranch(
                    condition={"node_type": "COMPARE", "left": "pickup_hour",
                               "op": "IN", "right": {"node_type": "LITERAL",
                               "value": [7, 8, 9], "data_type": "number"}},
                    then_value="高峰",
                ),
            ],
            else_value="平峰",
        )
        assert rule.output_column == "peak_type"
        assert len(rule.branches) == 1

    def test_default_factory_empty_lists(self):
        """default_factory=list 确保默认空列表。"""
        rule = CaseWhenRule(output_column="test", else_value="unknown")
        assert rule.branches == []


class TestRequirementPlannerOutput:
    """LLM 输出模型测试。"""

    def test_empty_output_valid(self):
        output = RequirementPlannerOutput()
        assert output.dimensions == []
        assert output.derived_dimensions == []
        assert output.metrics == []
        assert output.case_when_rules == []
        assert output.uncertainties == []

    def test_rejects_unknown_fields(self):
        with pytest.raises(ValidationError):
            RequirementPlannerOutput(unknown_field="should_reject")


class TestRequirementProposal:
    """系统 Artifact 模型测试。"""

    def test_minimal_proposal(self):
        proposal = RequirementProposal(
            proposal_id="test-001",
            spec_hash="abc123",
        )
        assert proposal.proposal_id == "test-001"
        assert proposal.llm_model == ""
        assert proposal.inference_time_ms == 0
        assert proposal.total_inferred == 0
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
```

预期：`DerivedDimensionDecl` 未定义 → `ImportError`

- [ ] **Step 3: 在 developer_spec/models.py 中新增 6 个模型**

在 `src/tianshu_datadev/developer_spec/models.py` 的 `DimensionDecl` 之后、`JoinDecl` 之前插入：

```python
# ════════════════════════════════════════════
# RequirementPlanner 模型（v3.1 新增）
# ════════════════════════════════════════════

class DerivedDimensionDecl(StrictModel):
    """派生维度——LLM 输出格式，Promotion 后成为 DerivedGroupKey 的输入。"""
    dimension_name: str
    source_column: str
    source_table: str
    time_function: Literal["HOUR"]


class CaseWhenBranch(StrictModel):
    """类型化 CASE WHEN 分支——条件使用 LabelPredicateCondition AST。"""
    condition: LabelPredicateCondition
    then_value: str


class CaseWhenRule(StrictModel):
    """CASE WHEN 规则——对应一条 CaseWhenStep。"""
    output_column: str
    branches: list[CaseWhenBranch] = Field(default_factory=list)
    else_value: str = ""


class UncertaintyEntry(StrictModel):
    """LLM 不确定项——仅 field_ref + description + candidates。"""
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

- [ ] **Step 4: 扩展 ParsedDeveloperSpec**

在 `ParsedDeveloperSpec` 类中新增两个字段（放在 `label_rules` 字段之后）：

```python
# ParsedDeveloperSpec 新增字段
derived_dimensions: list[DerivedDimensionDecl] = Field(default_factory=list)
case_when_rules: list[CaseWhenRule] = Field(default_factory=list)
```

- [ ] **Step 5: 确保 __init__.py 导出新模型**

检查并更新 `src/tianshu_datadev/developer_spec/__init__.py`，确保导出新增的 6 个模型。

- [ ] **Step 6: 运行测试确认通过 + 零回归**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
python -m pytest tests/planning/ tests/developer_spec/ -v --timeout=60
```

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py src/tianshu_datadev/developer_spec/__init__.py tests/planning/test_time_transform_expr.py
git commit -m "feat: 新增 Developer 侧 RequirementPlanner 模型 + ParsedDeveloperSpec 扩展

- DerivedDimensionDecl / CaseWhenBranch / CaseWhenRule / UncertaintyEntry
- RequirementPlannerOutput / RequirementProposal
- ParsedDeveloperSpec +derived_dimensions +case_when_rules
- 所有列表使用 Field(default_factory=list)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: artifacts/models.py + spark/models.py — Contract + Spark IR 模型

**Files:**
- Modify: `src/tianshu_datadev/artifacts/models.py` (新增 `ContractTimeTransform`，`DataTransformContractLite` +`time_transforms`，`DataTransformContractV1` +`time_transforms`)
- Modify: `src/tianshu_datadev/spark/models.py` (新增 `SparkTimeTransformExpr`，`SparkAggregateStep` +`time_transforms`)
- Test: `tests/planning/test_contract_time_transform.py` (新建)

**Interfaces:**
- Produces: `ContractTimeTransform(type, source_column, source_table, time_function, alias)`
- Produces: `SparkTimeTransformExpr(source_column, source_table, time_function, alias)`
- Modifies: `DataTransformContractLite` +`time_transforms: list[ContractTimeTransform]`
- Modifies: `DataTransformContractV1` +`time_transforms: list[ContractTimeTransform]`
- Modifies: `SparkAggregateStep` +`time_transforms: list[SparkTimeTransformExpr]`

- [ ] **Step 1: 编写 Contract + Spark 模型测试**

```python
# tests/planning/test_contract_time_transform.py
import pytest
from pydantic import ValidationError

from tianshu_datadev.artifacts.models import (
    ContractTimeTransform,
    DataTransformContractLite,
    DataTransformContractV1,
)
from tianshu_datadev.spark.models import (
    SparkTimeTransformExpr,
    SparkAggregateStep,
    SparkAggregateSpec,
)


class TestContractTimeTransform:
    """Contract 侧时间变换模型测试。"""

    def test_valid_contract_time_transform(self):
        tt = ContractTimeTransform(
            source_column="pickup_at",
            source_table="ft",
            time_function="HOUR",
            alias="pickup_hour",
        )
        assert tt.alias == "pickup_hour"
        assert tt.type == "time_transform"

    def test_data_transform_contract_lite_has_time_transforms(self):
        """DataTransformContractLite 应有 time_transforms 字段且默认为空。"""
        lite = DataTransformContractLite(
            contract_id="test",
            source_sqlbuildplan_hash="abc",
            grouping_keys=["pickup_hour"],
        )
        assert lite.time_transforms == []

    def test_data_transform_contract_v1_has_time_transforms(self):
        """DataTransformContractV1 应有 time_transforms 字段且默认为空。"""
        v1 = DataTransformContractV1(
            contract_id="test",
            source_sqlprogram_hash="abc",
            grouping_keys=["pickup_hour"],
        )
        assert v1.time_transforms == []


class TestSparkTimeTransformExpr:
    """Spark 侧时间变换模型测试。"""

    def test_valid_spark_time_transform(self):
        tt = SparkTimeTransformExpr(
            source_column="pickup_at",
            source_table="ft",
            time_function="hour",
            alias="pickup_hour",
        )
        assert tt.time_function == "hour"


class TestSparkAggregateStepWithTimeTransforms:
    """SparkAggregateStep + time_transforms 测试。"""

    def test_default_time_transforms_empty(self):
        """默认 time_transforms 为空列表——向后兼容。"""
        step = SparkAggregateStep(
            input_alias="t1",
            group_keys=["borough"],
            metrics=[
                SparkAggregateSpec(
                    function="count",
                    input_column="trip_id",
                    alias="trip_count",
                ),
            ],
        )
        assert step.time_transforms == []

    def test_with_time_transforms(self):
        """time_transforms 非空时应正确存储。"""
        step = SparkAggregateStep(
            input_alias="t1",
            group_keys=["borough"],
            metrics=[],
            time_transforms=[
                SparkTimeTransformExpr(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="hour",
                    alias="pickup_hour",
                ),
            ],
        )
        assert len(step.time_transforms) == 1
        assert step.time_transforms[0].alias == "pickup_hour"
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
```

- [ ] **Step 3: 修改 artifacts/models.py**

在 `ContractAggregation` 之后新增：

```python
class ContractTimeTransform(StrictModel):
    """Contract 侧时间变换——禁止 dict 逃生口。"""
    type: Literal["time_transform"] = "time_transform"
    source_column: str
    source_table: str
    time_function: str   # "HOUR"
    alias: str           # 输出别名——与 grouping_keys 中的逻辑名对应
```

在 `DataTransformContractLite` (line 114) 新增字段：

```python
time_transforms: list[ContractTimeTransform] = []
```

在 `DataTransformContractV1` 中同样新增：

```python
time_transforms: list[ContractTimeTransform] = []
```

- [ ] **Step 4: 修改 spark/models.py**

在 `SparkAggregateSpec` 之后新增：

```python
class SparkTimeTransformExpr(StrictModel):
    """Spark 侧时间变换表达式——从 ContractTimeTransform 确定性映射。"""
    source_column: str
    source_table: str
    time_function: str   # "hour"（已小写）
    alias: str
```

修改 `SparkAggregateStep` (line 135)，新增字段：

```python
time_transforms: list[SparkTimeTransformExpr] = Field(default_factory=list)
```

- [ ] **Step 5: 运行测试 + 回归**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
python -m pytest tests/spark/test_spark_plan.py tests/artifacts/test_models.py -v --timeout=60
```

- [ ] **Step 6: Commit**

```bash
git add tests/planning/test_contract_time_transform.py src/tianshu_datadev/artifacts/models.py src/tianshu_datadev/spark/models.py
git commit -m "feat: 新增 ContractTimeTransform + SparkTimeTransformExpr 模型

- artifacts/models.py: ContractTimeTransform, DataTransformContractLite/V1 +time_transforms
- spark/models.py: SparkTimeTransformExpr, SparkAggregateStep +time_transforms

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: sql/compiler.py — _render_time_transform + 修改 _render_aggregate + _render_flat_sql

**Files:**
- Modify: `src/tianshu_datadev/sql/compiler.py` (新增 `_render_time_transform`，修改 `_render_aggregate`，修改 `_render_flat_sql` 的 GROUP BY 分支)
- Test: `tests/planning/test_time_transform_expr.py` (追加编译器测试)

**Interfaces:**
- Produces: `DuckDbSqlCompiler._render_time_transform(expr: TimeTransformExpr) -> str` — 静态方法
- Modifies: `_render_aggregate` — 新增 `DerivedGroupKey` 分支（SELECT 用 `AS alias`）
- Modifies: `_render_flat_sql` — GROUP BY 显式处理 `DerivedGroupKey`

- [ ] **Step 1: 编写 SQL 编译器测试**

在 `tests/planning/test_time_transform_expr.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Task 4: SQL Compiler 测试
# ════════════════════════════════════════════

from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    CaseWhenStep,
    WhenBranch,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
    AggregationType,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler


class TestRenderTimeTransform:
    """_render_time_transform 共享渲染器测试。"""

    def test_render_hour_expr(self):
        """渲染 HOUR(ft.pickup_at)。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        result = DuckDbSqlCompiler._render_time_transform(expr)
        assert result == "HOUR(ft.pickup_at)"


class TestRenderAggregateWithDerivedGroupKey:
    """_render_aggregate 处理 DerivedGroupKey。"""

    def test_select_with_derived_group_key(self):
        """SELECT 应含 'HOUR(ft.pickup_at) AS pickup_hour'。"""
        agg = AggregateStep(
            step_id="agg_1",
            group_keys=[
                DerivedGroupKey(
                    alias="pickup_hour",
                    expr=TimeTransformExpr(
                        source_column=SafeIdentifier("pickup_at"),
                        source_table=SafeIdentifier("ft"),
                        time_function="HOUR",
                    ),
                ),
                ColumnRef(
                    table_ref=SafeIdentifier("tz"),
                    column_name=SafeIdentifier("borough"),
                    normalized_name=SafeIdentifier("borough"),
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias=SafeIdentifier("trip_count"),
                ),
            ],
        )
        compiler = DuckDbSqlCompiler()
        cols = compiler._render_aggregate(agg)
        assert "HOUR(ft.pickup_at) AS pickup_hour" in cols
        assert "tz.borough" in cols


class TestFlatSqlGroupByWithDerivedGroupKey:
    """_render_flat_sql GROUP BY 处理 DerivedGroupKey——集成验证。"""

    def test_group_by_uses_time_transform_without_alias(self):
        """GROUP BY 应使用 HOUR(ft.pickup_at) 不带 AS alias。"""
        plan = SqlBuildPlan(
            plan_id="test_plan",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref=SafeIdentifier("ft"),
                    required_columns=[
                        ColumnRef(
                            table_ref=SafeIdentifier("ft"),
                            column_name=SafeIdentifier("pickup_at"),
                            normalized_name=SafeIdentifier("pickup_at"),
                        ),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[
                        DerivedGroupKey(
                            alias="pickup_hour",
                            expr=TimeTransformExpr(
                                source_column=SafeIdentifier("pickup_at"),
                                source_table=SafeIdentifier("ft"),
                                time_function="HOUR",
                            ),
                        ),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation=AggregationType.COUNT,
                            input_column=None,
                            alias=SafeIdentifier("trip_count"),
                        ),
                    ],
                ),
            ],
        )
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)
        sql = compiled.sql
        # SELECT 含 AS alias
        assert "HOUR(ft.pickup_at) AS pickup_hour" in sql
        # GROUP BY 不含 AS alias
        assert "GROUP BY" in sql
        assert "HOUR(ft.pickup_at)" in sql
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
```

预期：`_render_time_transform` 属性不存在

- [ ] **Step 3: 在 sql/compiler.py 中新增 _render_time_transform**

在 `DuckDbSqlCompiler` 类中新增静态方法：

```python
@staticmethod
def _render_time_transform(expr: TimeTransformExpr) -> str:
    """TimeTransformExpr → SQL 函数调用字符串。

    SELECT / GROUP BY / CASE WHEN 共享此渲染器。
    """
    return f"{expr.time_function}({expr.source_table}.{expr.source_column})"
```

导入 `TimeTransformExpr`：

```python
from tianshu_datadev.planning.models import TimeTransformExpr
```

- [ ] **Step 4: 修改 _render_aggregate 处理 DerivedGroupKey**

修改 `_render_aggregate` 方法（约 line 738），在 group_keys 遍历中新增 `DerivedGroupKey` 分支：

```python
def _render_aggregate(self, step: AggregateStep) -> list[str]:
    cols: list[str] = []

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

    for m in step.metrics:
        cols.append(self._render_aggregate_spec(m))

    return cols
```

导入 `DerivedGroupKey`：

```python
from tianshu_datadev.planning.models import DerivedGroupKey
```

- [ ] **Step 5: 修改 _render_flat_sql 的 GROUP BY 分支**

修改 `_render_flat_sql` 方法中 AggregateStep 的 GROUP BY 处理（约 line 588-592）：

```python
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
```

- [ ] **Step 6: 运行测试 + 回归**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
python -m pytest tests/sql/test_compiler.py -v --timeout=120
```

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/sql/compiler.py tests/planning/test_time_transform_expr.py
git commit -m "feat: SQL Compiler 支持 TimeTransformExpr——SELECT/GROUP BY 共享渲染器

- 新增 _render_time_transform() 静态方法
- _render_aggregate: DerivedGroupKey → HOUR(col) AS alias
- _render_flat_sql GROUP BY: DerivedGroupKey → HOUR(col) 不带别名

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: sql_build_plan.py — Builder 改造

**Files:**
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py` (修改 `AggregateStep.group_keys` union，修改 `_build_aggregate_step`，修改 `_build_case_when_steps`，修改 Scan 构建，修改 `_build_multi_table`)
- Test: `tests/planning/test_time_transform_expr.py` (追加 Builder 测试)

**Interfaces:**
- Modifies: `AggregateStep.group_keys: list[ColumnRef | DerivedGroupKey]` — 扩展 union
- Modifies: `_build_aggregate_step` — 生成 `DerivedGroupKey`
- Modifies: `_build_case_when_steps` — 读取 `spec.case_when_rules`
- Modifies: Scan 构建 — 追加 `derived_dimensions` 源列 + `case_when_rules` 条件列
- Modifies: `_build_multi_table` — 按 `source_table` 分配列

- [ ] **Step 1: 修改 AggregateStep.group_keys union 类型**

修改 `src/tianshu_datadev/planning/sql_build_plan.py:125`：

```python
# 修改前：
group_keys: list[ColumnRef]  # GROUP BY 列
# 修改后：
group_keys: list[ColumnRef | DerivedGroupKey]  # GROUP BY 列（含派生维度）
```

导入 `DerivedGroupKey`：

```python
from .models import (
    ...
    DerivedGroupKey,  # v3.1 新增
    ...
)
```

- [ ] **Step 2: 编写 Builder 测试**

在 `tests/planning/test_time_transform_expr.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Task 5: SqlBuildPlanBuilder 测试
# ════════════════════════════════════════════

from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder


class TestBuildAggregateStepWithDerivedDimensions:
    """_build_aggregate_step 生成 DerivedGroupKey。"""

    def test_derived_dimension_becomes_derived_group_key(self):
        """spec.derived_dimensions → DerivedGroupKey 在 group_keys 中。"""
        spec = ParsedDeveloperSpec(
            spec_hash="test",
            title="测试",
            description="测试派生维度",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[ColumnDecl(column_name="pickup_at", data_type="timestamp")],
                ),
            ],
            dimensions=[
                DimensionDecl(
                    dimension_name="borough",
                    column_ref="borough",
                    source_table="tz",
                ),
            ],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                ],
            ),
        )
        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, primary_table="ft")
        derived_keys = [
            gk for gk in agg.group_keys if isinstance(gk, DerivedGroupKey)
        ]
        assert len(derived_keys) == 1
        assert derived_keys[0].alias == "pickup_hour"
        assert derived_keys[0].expr.time_function == "HOUR"
```

- [ ] **Step 3: 修改 _build_aggregate_step**

修改 `_build_aggregate_step` 方法（约 line 2632），在基础维度处理后新增派生维度处理：

```python
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
```

修改 grain 补充的去重逻辑，使其同时处理 `ColumnRef.normalized_name` 和 `DerivedGroupKey.alias`：

```python
existing = {
    g.normalized_name if isinstance(g, ColumnRef) else g.alias
    for g in group_keys
}
```

- [ ] **Step 4: 修改 _build_case_when_steps 扩展读取 case_when_rules**

修改 `_build_case_when_steps` 方法（约 line 1661），在处理 `label_rules` 之后新增：

```python
# 构建 derived alias → TimeTransformExpr 映射——供 Predicate 引用
derived_expr_map: dict[str, TimeTransformExpr] = {
    dd.dimension_name: TimeTransformExpr(
        source_column=SafeIdentifier(dd.source_column),
        source_table=SafeIdentifier(dd.source_table),
        time_function=dd.time_function,
    )
    for dd in spec.derived_dimensions
}

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
```

- [ ] **Step 5: 修改 _predicate_from_label_node 支持 derived_expr_map**

修改 `_predicate_from_label_node` 方法签名，新增 `derived_expr_map` 参数。当 `LabelCompare.left` 匹配 `derived_expr_map` 中的 key 时，用 `TimeTransformExpr` 替代 `ColumnRef`：

```python
def _predicate_from_label_node(
    self, node, table_alias: str,
    derived_expr_map: dict[str, TimeTransformExpr] | None = None,
) -> Predicate:
    derived_expr_map = derived_expr_map or {}
    ...
    elif isinstance(node, LabelCompare):
        left_name = node.left
        if left_name in derived_expr_map:
            left = derived_expr_map[left_name]  # TimeTransformExpr
        else:
            left = ColumnRef(
                table_ref=SafeIdentifier(table_alias),
                column_name=SafeIdentifier(left_name),
                normalized_name=SafeIdentifier(self._normalizer.normalize(left_name)),
            )
        ...
```

- [ ] **Step 6: 修改 Scan 构建——追加派生维度源列**

修改 `_build_single_table` 方法，追加派生维度源列收集和 Scan 扩展逻辑（参见设计文档 §4.1）。

- [ ] **Step 7: 运行测试 + 回归**

```bash
python -m pytest tests/planning/test_time_transform_expr.py -v
python -m pytest tests/planning/test_sql_program.py tests/sql/test_compiler.py -v --timeout=120
```

- [ ] **Step 8: Commit**

```bash
git add src/tianshu_datadev/planning/sql_build_plan.py tests/planning/test_time_transform_expr.py
git commit -m "feat: Builder 支持 DerivedGroupKey + case_when_rules 正式字段

- AggregateStep.group_keys 扩展为 list[ColumnRef | DerivedGroupKey]
- _build_aggregate_step: derived_dimensions → DerivedGroupKey
- _build_case_when_steps: 读取 spec.case_when_rules 生成 CaseWhenStep
- _predicate_from_label_node: 支持 derived_expr_map→TimeTransformExpr
- Scan: 追加派生维度源列 + case_when 条件列

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: artifacts/contract_extractor.py — Contract 提取器改造

**Files:**
- Modify: `src/tianshu_datadev/artifacts/contract_extractor.py` (修改 `_extract_aggregate`，修改 `_render_operand`，修改 `_predicate_to_case_when_condition`)
- Test: `tests/planning/test_contract_time_transform.py` (追加测试)

**Interfaces:**
- Modifies: `_extract_aggregate` — 识别 `DerivedGroupKey`，输出 `time_transforms`，使用 `alias` 而非 `normalized_name`
- Modifies: `_render_operand` — 处理 `TimeTransformExpr` 操作数
- Modifies: `_predicate_to_case_when_condition` — `TimeTransformExpr`→alias 转换

- [ ] **Step 1: 编写 Contract 提取器测试**

在 `tests/planning/test_contract_time_transform.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Task 6: Contract 提取器测试
# ════════════════════════════════════════════

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.planning.models import TimeTransformExpr, DerivedGroupKey
from tianshu_datadev.planning.sql_build_plan import AggregateStep


class TestExtractAggregateWithDerivedGroupKey:
    """_extract_aggregate 处理 DerivedGroupKey。"""

    def test_derived_key_produces_time_transform(self):
        """DerivedGroupKey → ContractTimeTransform + grouping_key alias。"""
        agg = AggregateStep(
            step_id="agg_1",
            group_keys=[
                DerivedGroupKey(
                    alias="pickup_hour",
                    expr=TimeTransformExpr(
                        source_column=SafeIdentifier("pickup_at"),
                        source_table=SafeIdentifier("ft"),
                        time_function="HOUR",
                    ),
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias=SafeIdentifier("trip_count"),
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert "pickup_hour" in groups
        assert len(time_transforms) == 1
        assert time_transforms[0].alias == "pickup_hour"
        assert time_transforms[0].time_function == "HOUR"


class TestRenderOperandWithTimeTransform:
    """_render_operand 处理 TimeTransformExpr。"""

    def test_render_time_transform_operand(self):
        """TimeTransformExpr → 'HOUR(ft.pickup_at)'。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        result = DataTransformContractExtractor._render_operand(expr)
        assert result == "HOUR(ft.pickup_at)"
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
```

- [ ] **Step 3: 修改 _extract_aggregate**

修改 `src/tianshu_datadev/artifacts/contract_extractor.py:367`，返回签名改为四元组：

```python
@staticmethod
def _extract_aggregate(
    step: AggregateStep,
) -> tuple[list[ContractAggregation], list[str], list[str], list[ContractTimeTransform]]:
    aggs: list[ContractAggregation] = []
    groups: list[str] = []
    biz_keys: list[str] = []
    time_transforms: list[ContractTimeTransform] = []

    for m in step.metrics:
        aggs.append(ContractAggregation(
            function=m.aggregation if isinstance(m.aggregation, str) else m.aggregation,
            input_column=m.input_column,
            alias=m.alias,
        ))

    for gk in step.group_keys:
        if isinstance(gk, DerivedGroupKey):
            groups.append(gk.alias)
            time_transforms.append(ContractTimeTransform(
                source_column=gk.expr.source_column,
                source_table=gk.expr.source_table,
                time_function=gk.expr.time_function,
                alias=gk.alias,
            ))
        elif isinstance(gk, ColumnRef):
            groups.append(gk.normalized_name)
            biz_keys.append(gk.normalized_name)

    return aggs, groups, biz_keys, time_transforms
```

更新调用方 `extract()` 方法中 line 130 的解包：

```python
aggs, groups, biz_keys, time_transforms = self._extract_aggregate(step)
aggregations.extend(aggs)
grouping_keys.extend(groups)
business_keys.extend(biz_keys)
```

并在 `DataTransformContractLite` 构造时传入 `time_transforms`。

- [ ] **Step 4: 修改 _render_operand**

在 `_render_operand` 方法中，`ColumnRef` 检查之后新增 `TimeTransformExpr` 检查：

```python
# TimeTransformExpr（v3.1 新增）
if hasattr(operand, "time_function") and hasattr(operand, "source_column"):
    return f"{operand.time_function}({operand.source_table}.{operand.source_column})"
```

- [ ] **Step 5: 修改 _predicate_to_case_when_condition**

新增 `derived_expr_map` 参数，当 `Predicate.left` 为 `TimeTransformExpr` 时反查 alias：

```python
@staticmethod
def _predicate_to_case_when_condition(
    predicate: Predicate,
    derived_expr_map: dict | None = None,
) -> CaseWhenCondition:
    derived_expr_map = derived_expr_map or {}
    # 构建 TimeTransformExpr → alias 反向映射
    expr_to_alias: dict[tuple, str] = {}
    for alias, expr in derived_expr_map.items():
        key = (str(expr.source_table), str(expr.source_column), expr.time_function)
        expr_to_alias[key] = alias

    left = predicate.left
    if isinstance(left, TimeTransformExpr):
        key = (str(left.source_table), str(left.source_column), left.time_function)
        alias = expr_to_alias.get(key)
        if alias is None:
            raise ValueError(
                f"TimeTransformExpr 在 derived_expr_map 中无对应 alias"
            )
        col_name = alias
    elif isinstance(left, ColumnRef):
        col_name = left.normalized_name
    else:
        col_name = str(left)
    # ... 其余逻辑
```

- [ ] **Step 6: 运行测试 + 回归**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
python -m pytest tests/artifacts/ -v --timeout=60
```

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/artifacts/contract_extractor.py tests/planning/test_contract_time_transform.py
git commit -m "feat: Contract 提取器支持 TimeTransformExpr 全链路

- _extract_aggregate: DerivedGroupKey→ContractTimeTransform，用 alias 做 key
- _render_operand: 处理 TimeTransformExpr 操作数
- _predicate_to_case_when_condition: TimeTransformExpr→alias 转换

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: Spark 全链路——Compiler + Mapper + Adapter + Comparator

**Files:**
- Modify: `src/tianshu_datadev/spark/compiler.py` (修改 `_compile_aggregate`——渲染 `time_transforms`)
- Modify: `src/tianshu_datadev/spark/mapper.py` (修改 `_map_aggregations`——处理 `time_transforms` 参数 + Contract 不变量)
- Modify: `src/tianshu_datadev/spark/contract_adapter.py` (修改 `adapt_lite_to_v1`——透传 `time_transforms`)
- Modify: `src/tianshu_datadev/spark/plan_comparator.py` (修改 `_flatten_aggregate_step`——处理 `DerivedGroupKey` + `SparkTimeTransformExpr`)
- Test: `tests/planning/test_contract_time_transform.py` (追加 Spark 链路测试)

- [ ] **Step 1: 编写 Spark 链路测试**

在 `tests/planning/test_contract_time_transform.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Task 7: Spark 链路测试
# ════════════════════════════════════════════

from tianshu_datadev.spark.mapper import _map_aggregations
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
from tianshu_datadev.artifacts.models import (
    ContractAggregation,
    ContractTimeTransform,
)


class TestMapAggregationsWithTimeTransforms:
    """Mapper——Contract 不变量应用。"""

    def test_mapper_replaces_group_key_with_transform(self):
        """同名 alias → 从 group_keys 移除，加入 time_transforms。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["pickup_hour", "borough"],
            time_transforms=[
                ContractTimeTransform(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                    alias="pickup_hour",
                ),
            ],
        )
        step = result[0]
        # pickup_hour 从 group_keys 移除
        assert "pickup_hour" not in step.group_keys
        assert "borough" in step.group_keys
        # 加入 time_transforms
        assert len(step.time_transforms) == 1
        assert step.time_transforms[0].alias == "pickup_hour"

    def test_mapper_empty_time_transforms(self):
        """空 time_transforms——group_keys 保持不变。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert step.group_keys == ["borough"]
        assert step.time_transforms == []


class TestAdaptLiteToV1TimeTransforms:
    """lite→v1 adapter 透传 time_transforms。"""

    def test_adapt_preserves_time_transforms(self):
        """time_transforms 应从 Lite 透传到 V1。"""
        from tianshu_datadev.artifacts.models import DataTransformContractLite
        lite = DataTransformContractLite(
            contract_id="test",
            source_sqlbuildplan_hash="abc",
            grouping_keys=["pickup_hour"],
            time_transforms=[
                ContractTimeTransform(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                    alias="pickup_hour",
                ),
            ],
        )
        v1 = adapt_lite_to_v1(lite)
        assert len(v1.time_transforms) == 1
        assert v1.time_transforms[0].alias == "pickup_hour"


class TestCompileAggregateWithTimeTransforms:
    """_compile_aggregate 渲染 time_transforms——集成验证。"""

    def test_compile_generates_hour_in_groupby(self):
        """编译产物应包含 F.hour(...).alias(...) 在 groupBy 中。"""
        from tianshu_datadev.spark.models import SparkPlan, SparkReadStep
        plan = SparkPlan(
            plan_id="test",
            steps=[
                SparkReadStep(
                    source_name="ft",
                    alias="ft",
                ),
                SparkAggregateStep(
                    input_alias="t1",
                    group_keys=["borough"],
                    metrics=[
                        SparkAggregateSpec(
                            function="count",
                            input_column=None,
                            alias="trip_count",
                        ),
                    ],
                    time_transforms=[
                        SparkTimeTransformExpr(
                            source_column="pickup_at",
                            source_table="ft",
                            time_function="hour",
                            alias="pickup_hour",
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # groupBy 中应有 F.hour(...).alias("pickup_hour")
        assert 'F.hour(F.col("ft.pickup_at")).alias("pickup_hour")' in code
        # select 中应有 F.col("pickup_hour")——禁止 F.hour 再次出现
        assert 'F.col("pickup_hour")' in code
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
```

- [ ] **Step 3: 修改 spark/compiler.py — _compile_aggregate**

在 `_compile_aggregate` 中，普通 `group_keys` 处理之后新增 `time_transforms` 处理：

```python
# time_transforms（v3.1 新增）
for tt in step.time_transforms:
    func = tt.time_function  # "hour"
    src = f'F.col("{tt.source_table}.{tt.source_column}")'
    group_parts.append(f'F.{func}({src}).alias("{tt.alias}")')
    # 聚合后只引用 F.col(alias)
    select_parts.append(f'F.col("{tt.alias}")')
```

- [ ] **Step 4: 修改 spark/mapper.py — _map_aggregations**

修改 `_map_aggregations` 函数签名，新增 `time_transforms` 参数：

```python
def _map_aggregations(
    aggregations: list[ContractAggregation],
    grouping_keys: list[str],
    time_transforms: list[ContractTimeTransform] | None = None,
    unsupported: list | None = None,
    gaps: list | None = None,
) -> ...:
```

新增 Contract 不变量逻辑：

```python
transform_map = {tt.alias: tt for tt in (time_transforms or [])}
spark_group_keys: list[str] = []
spark_time_transforms: list[SparkTimeTransformExpr] = []

for key in grouping_keys:
    if key in transform_map:
        tt = transform_map[key]
        spark_time_transforms.append(SparkTimeTransformExpr(
            source_column=tt.source_column,
            source_table=tt.source_table,
            time_function=tt.time_function.lower(),
            alias=tt.alias,
        ))
    else:
        spark_group_keys.append(key)
```

更新 `SparkAggregateStep` 构造，传入 `time_transforms=spark_time_transforms`。

- [ ] **Step 5: 修改 spark/contract_adapter.py — adapt_lite_to_v1**

在 `adapt_lite_to_v1` 中新增透传：

```python
time_transforms=getattr(lite, "time_transforms", None) or [],
```

- [ ] **Step 6: 修改 spark/plan_comparator.py — _flatten_aggregate_step**

在 `_flatten_aggregate_step` 中新增 `DerivedGroupKey` 处理——当 group_key dict 含有 `alias` + `expr` 时提取为 `time_transforms`。

- [ ] **Step 7: 运行测试 + 回归**

```bash
python -m pytest tests/planning/test_contract_time_transform.py -v
python -m pytest tests/spark/ -v --timeout=120
```

- [ ] **Step 8: Commit**

```bash
git add src/tianshu_datadev/spark/compiler.py src/tianshu_datadev/spark/mapper.py src/tianshu_datadev/spark/contract_adapter.py src/tianshu_datadev/spark/plan_comparator.py tests/planning/test_contract_time_transform.py
git commit -m "feat: Spark 全链路支持 TimeTransformExpr

- compiler: _compile_aggregate 渲染 time_transforms → groupBy alias
- mapper: Contract 不变量——同名 alias 用 transform 替换 plain key
- contract_adapter: lite→v1 透传 time_transforms
- plan_comparator: _flatten_aggregate_step 处理 DerivedGroupKey

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: planning/requirement_planner.py — RequirementPlanner 核心

**Files:**
- Create: `src/tianshu_datadev/planning/requirement_planner.py`
- Modify: `src/tianshu_datadev/planning/__init__.py` (导出)
- Test: `tests/planning/test_requirement_planner_e2e.py` (新建)

**Interfaces:**
- Produces: `RequirementPlanner(adapter: ProviderAdapter)` — 构造函数
- Produces: `RequirementPlanner.plan(spec: ParsedDeveloperSpec, manifest: SourceManifest) -> RequirementPlannerOutput`

- [ ] **Step 1: 编写 RequirementPlanner 单元测试**

```python
# tests/planning/test_requirement_planner_e2e.py
import pytest
import json

from tianshu_datadev.planning.requirement_planner import RequirementPlanner
from tianshu_datadev.llm.adapters.fake_adapter import FakeAdapter
from tianshu_datadev.developer_spec.models import (
    ParsedDeveloperSpec,
    DatasetType,
    InputTableDecl,
    ColumnDecl,
    OutputSpecDecl,
    OutputColumnDecl,
    SourceManifest,
    ManifestTable,
    ManifestColumn,
)


class TestRequirementPlanner:
    """RequirementPlanner 核心测试——使用 FakeAdapter。"""

    def _make_spec(self) -> ParsedDeveloperSpec:
        """构建最小可用的 aggregate_table Spec。"""
        return ParsedDeveloperSpec(
            spec_hash="test_planner_001",
            title="高峰时段出行分析",
            description="按小时和区域统计出行次数，区分高峰/平峰",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[
                        ColumnDecl(column_name="pickup_at", data_type="timestamp"),
                        ColumnDecl(column_name="borough", data_type="varchar"),
                    ],
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                    OutputColumnDecl(name="peak_type"),
                ],
            ),
        )

    def _make_manifest(self) -> SourceManifest:
        return SourceManifest(
            spec_hash="manifest_001",
            tables=[
                ManifestTable(
                    table_ref="ft",
                    source_table="fact_table",
                    columns=[
                        ManifestColumn(column_name="pickup_at", data_type="timestamp"),
                        ManifestColumn(column_name="borough", data_type="varchar"),
                    ],
                ),
            ],
        )

    def test_planner_returns_valid_output_with_fake_adapter(self):
        """FakeAdapter 应返回合法 RequirementPlannerOutput。"""
        fake = FakeAdapter()
        # 配置 FakeAdapter 返回预设 JSON
        fake.set_response({
            "dimensions": [{
                "dimension_name": "borough",
                "column_ref": "borough",
                "source_table": "ft",
            }],
            "derived_dimensions": [{
                "dimension_name": "pickup_hour",
                "source_column": "pickup_at",
                "source_table": "ft",
                "time_function": "HOUR",
            }],
            "metrics": [{
                "metric_name": "出行次数",
                "aggregation": "COUNT",
                "alias": "trip_count",
            }],
            "case_when_rules": [{
                "output_column": "peak_type",
                "branches": [{
                    "condition": {
                        "node_type": "COMPARE",
                        "left": "pickup_hour",
                        "op": "IN",
                        "right": {
                            "node_type": "LITERAL",
                            "value": [7, 8, 9, 10, 17, 18, 19, 20],
                            "data_type": "number",
                        },
                    },
                    "then_value": "高峰",
                }],
                "else_value": "平峰",
            }],
            "uncertainties": [],
        })
        planner = RequirementPlanner(adapter=fake)
        spec = self._make_spec()
        manifest = self._make_manifest()
        output = planner.plan(spec, manifest)
        assert len(output.derived_dimensions) == 1
        assert output.derived_dimensions[0].dimension_name == "pickup_hour"
        assert len(output.case_when_rules) == 1
        assert output.case_when_rules[0].output_column == "peak_type"
        assert output.case_when_rules[0].else_value == "平峰"

    def test_planner_with_no_adapter_raises(self):
        """无 Adapter 时不应调用 Planner——Pipeline 层守卫。"""
        # Planner 本身允许无 Adapter 构造，但 Pipeline 在有 unresolved 且无 Adapter 时
        # 抛出 ConfigError——此测试验证 Planner 构造不抛异常
        planner = RequirementPlanner(adapter=None)
        assert planner is not None
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/planning/test_requirement_planner_e2e.py -v
```

- [ ] **Step 3: 创建 planning/requirement_planner.py**

```python
"""RequirementPlanner——从自然语言业务描述生成结构化声明。

使用 LLM 推断维度、派生维度、指标和 CASE WHEN 规则。
输出 RequirementPlannerOutput——经 Validator → Promotion 后写入 Spec。
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.models import (
    CaseWhenBranch,
    CaseWhenRule,
    DerivedDimensionDecl,
    DimensionDecl,
    MetricDecl,
    RequirementPlannerOutput,
    UncertaintyEntry,
)

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec, SourceManifest
    from tianshu_datadev.llm.adapters.base import ProviderAdapter

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════
# System Prompt
# ════════════════════════════════════════════

_REQUIREMENT_PLANNER_SYSTEM_PROMPT = """\
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
    不给这些字段生成任何输出，也不生成 uncertainty。"""

# ════════════════════════════════════════════
# JSON Schema（v3.1——predicate_root 不含 NOT）
# ════════════════════════════════════════════

_REQUIREMENT_PLANNER_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string"},
                    "column_ref": {"type": "string"},
                    "source_table": {"type": "string"},
                },
                "required": ["dimension_name", "column_ref", "source_table"],
                "additionalProperties": False,
            },
        },
        "derived_dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string"},
                    "source_column": {"type": "string"},
                    "source_table": {"type": "string"},
                    "time_function": {"type": "string", "enum": ["HOUR"]},
                },
                "required": ["dimension_name", "source_column", "source_table", "time_function"],
                "additionalProperties": False,
            },
        },
        "metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string"},
                    "aggregation": {
                        "type": "string",
                        "enum": ["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"],
                    },
                    "input_column": {"type": "string"},
                    "alias": {"type": "string"},
                },
                "required": ["metric_name", "aggregation", "alias"],
                "additionalProperties": False,
            },
        },
        "case_when_rules": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "output_column": {"type": "string"},
                    "branches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "condition": {"$ref": "#/$defs/predicate_root"},
                                "then_value": {"type": "string"},
                            },
                            "required": ["condition", "then_value"],
                            "additionalProperties": False,
                        },
                    },
                    "else_value": {"type": "string"},
                },
                "required": ["output_column", "branches", "else_value"],
                "additionalProperties": False,
            },
        },
        "uncertainties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field_ref": {"type": "string"},
                    "description": {"type": "string"},
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["field_ref", "description"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["dimensions", "derived_dimensions", "metrics", "case_when_rules", "uncertainties"],
    "additionalProperties": False,
    "$defs": {
        "literal": {
            "type": "object",
            "properties": {
                "node_type": {"const": "LITERAL"},
                "value": {},
                "data_type": {
                    "type": "string",
                    "enum": ["string", "number", "boolean", "null"],
                },
            },
            "required": ["node_type", "value", "data_type"],
            "additionalProperties": False,
        },
        "predicate_leaf": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "COMPARE"},
                        "left": {"type": "string"},
                        "op": {
                            "type": "string",
                            "enum": ["=", "!=", ">", ">=", "<", "<=", "IN", "NOT_IN"],
                        },
                        "right": {"$ref": "#/$defs/literal"},
                    },
                    "required": ["node_type", "left", "op", "right"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "IS_NULL"},
                        "column": {"type": "string"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "IS_NOT_NULL"},
                        "column": {"type": "string"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
            ],
        },
        "predicate_root": {
            "oneOf": [
                {"$ref": "#/$defs/predicate_leaf"},
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "AND"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/predicate_root"},
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "OR"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/predicate_root"},
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
            ],
        },
    },
}


class RequirementPlanner:
    """从自然语言业务描述生成结构化声明。

    使用 LLM（通过 ProviderAdapter）推断：
    - dimensions: 基础维度
    - derived_dimensions: 派生维度（仅有 HOUR 时间函数）
    - metrics: 基础指标
    - case_when_rules: 类型化 CASE WHEN 规则
    - uncertainties: 不确定项

    LLM 调用失败时返回全空 RequirementPlannerOutput——不阻断管线。
    """

    def __init__(self, adapter: ProviderAdapter | None = None):
        """初始化 RequirementPlanner。

        Args:
            adapter: LLM Provider 适配器。None 时 plan() 返回全空输出。
        """
        self._adapter = adapter

    def plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> RequirementPlannerOutput:
        """执行 LLM 推断，返回结构化声明。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            RequirementPlannerOutput——含推断结果
        """
        if self._adapter is None:
            return RequirementPlannerOutput()

        # 构建上下文
        context = self._build_context(spec, manifest)

        try:
            raw = self._adapter.invoke(
                system_message=_REQUIREMENT_PLANNER_SYSTEM_PROMPT,
                user_message=json.dumps(context, ensure_ascii=False),
                json_schema=_REQUIREMENT_PLANNER_JSON_SCHEMA,
                model="",
                temperature=0.1,
            )
        except Exception as e:
            logger.warning("RequirementPlanner LLM 调用失败：%s", e)
            return RequirementPlannerOutput()

        return self._parse_response(raw)

    def _build_context(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> dict:
        """构建 LLM 调用的 Context 部分。"""
        # 源表 Schema
        tables_info: list[dict] = []
        for table in manifest.tables:
            cols_info = [
                {
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                }
                for col in table.columns
            ]
            tables_info.append({
                "table_ref": table.table_ref,
                "source_table": str(table.source_table) if table.source_table else None,
                "columns": cols_info,
            })

        # 已有声明——不可覆盖（H6）
        existing_declarations: dict = {
            "dimensions": [
                {"dimension_name": d.dimension_name, "column_ref": d.column_ref}
                for d in spec.dimensions
            ],
            "metrics": [
                {"metric_name": m.metric_name, "alias": m.alias}
                for m in spec.metrics
            ],
        }

        return {
            "table_schemas": tables_info,
            "existing_declarations": existing_declarations,
            "output_columns": [c.name for c in spec.output_spec.columns],
            "business_description": spec.description,
            "spec_title": spec.title,
        }

    def _parse_response(self, raw: dict) -> RequirementPlannerOutput:
        """解析 LLM 返回的 JSON 为 RequirementPlannerOutput。"""
        try:
            dimensions = [
                DimensionDecl(**d)
                for d in raw.get("dimensions", [])
            ]
        except Exception as e:
            logger.warning("解析 dimensions 失败：%s", e)
            dimensions = []

        try:
            derived_dimensions = [
                DerivedDimensionDecl(**dd)
                for dd in raw.get("derived_dimensions", [])
            ]
        except Exception as e:
            logger.warning("解析 derived_dimensions 失败：%s", e)
            derived_dimensions = []

        try:
            metrics = [
                MetricDecl(**m)
                for m in raw.get("metrics", [])
            ]
        except Exception as e:
            logger.warning("解析 metrics 失败：%s", e)
            metrics = []

        try:
            case_when_rules = []
            for rule in raw.get("case_when_rules", []):
                branches = [
                    CaseWhenBranch(**b)
                    for b in rule.get("branches", [])
                ]
                case_when_rules.append(CaseWhenRule(
                    output_column=rule["output_column"],
                    branches=branches,
                    else_value=rule.get("else_value", ""),
                ))
        except Exception as e:
            logger.warning("解析 case_when_rules 失败：%s", e)
            case_when_rules = []

        try:
            uncertainties = [
                UncertaintyEntry(**u)
                for u in raw.get("uncertainties", [])
            ]
        except Exception as e:
            logger.warning("解析 uncertainties 失败：%s", e)
            uncertainties = []

        return RequirementPlannerOutput(
            dimensions=dimensions,
            derived_dimensions=derived_dimensions,
            metrics=metrics,
            case_when_rules=case_when_rules,
            uncertainties=uncertainties,
        )
```

- [ ] **Step 4: 更新 planning/__init__.py 导出**

```python
from tianshu_datadev.planning.requirement_planner import RequirementPlanner
```

- [ ] **Step 5: 运行测试确认通过**

```bash
python -m pytest tests/planning/test_requirement_planner_e2e.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/planning/requirement_planner.py src/tianshu_datadev/planning/__init__.py tests/planning/test_requirement_planner_e2e.py
git commit -m "feat: 新增 RequirementPlanner 核心组件

- 从自然语言业务描述生成结构化维度/派生维度/指标/CASE WHEN
- System Prompt 含 8 条硬约束（H1-H8）
- JSON Schema 不含 NOT 节点（predicate_root 仅 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR）
- LLM 调用失败时返回全空输出不阻断管线

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: planning/proposal_validator.py — ProposalValidator

**Files:**
- Create: `src/tianshu_datadev/planning/proposal_validator.py`
- Test: `tests/planning/test_proposal_validator.py` (新建)

**Interfaces:**
- Produces: `ProposalValidator()` — 无参数构造函数
- Produces: `ProposalValidator.validate(proposal, spec, manifest) -> tuple[bool, list[OpenQuestion]]`

- [ ] **Step 1: 编写 Validator 测试（覆盖 V1-V13）**

```python
# tests/planning/test_proposal_validator.py
import pytest

from tianshu_datadev.planning.proposal_validator import ProposalValidator
from tianshu_datadev.developer_spec.models import (
    RequirementProposal,
    ParsedDeveloperSpec,
    DatasetType,
    InputTableDecl,
    ColumnDecl,
    OutputSpecDecl,
    OutputColumnDecl,
    DerivedDimensionDecl,
    CaseWhenRule,
    CaseWhenBranch,
    MetricDecl,
    AggregationType,
    SourceManifest,
    ManifestTable,
    ManifestColumn,
    DimensionDecl,
    OpenQuestion,
)


class TestProposalValidator:
    """ProposalValidator 全检查项测试。"""

    def _make_proposal(self, **overrides) -> RequirementProposal:
        defaults = {
            "proposal_id": "test-001",
            "spec_hash": "abc123",
            "dimensions": [
                DimensionDecl(
                    dimension_name="borough",
                    column_ref="borough",
                    source_table="ft",
                ),
            ],
            "derived_dimensions": [
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            "metrics": [
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            "case_when_rules": [
                CaseWhenRule(
                    output_column="peak_type",
                    branches=[
                        CaseWhenBranch(
                            condition={
                                "node_type": "COMPARE",
                                "left": "pickup_hour",
                                "op": "IN",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": [7, 8, 9],
                                    "data_type": "number",
                                },
                            },
                            then_value="高峰",
                        ),
                    ],
                    else_value="平峰",
                ),
            ],
        }
        defaults.update(overrides)
        return RequirementProposal(**defaults)

    def _make_spec(self) -> ParsedDeveloperSpec:
        return ParsedDeveloperSpec(
            spec_hash="abc123",
            title="测试",
            description="测试",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[
                        ColumnDecl(column_name="pickup_at", data_type="timestamp"),
                        ColumnDecl(column_name="borough", data_type="varchar"),
                    ],
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                    OutputColumnDecl(name="peak_type"),
                ],
            ),
        )

    def _make_manifest(self) -> SourceManifest:
        return SourceManifest(
            spec_hash="manifest_001",
            tables=[
                ManifestTable(
                    table_ref="ft",
                    source_table="fact_table",
                    columns=[
                        ManifestColumn(column_name="pickup_at", data_type="timestamp"),
                        ManifestColumn(column_name="borough", data_type="varchar"),
                    ],
                ),
            ],
        )

    # ── V1: column_ref 存在性 ──
    def test_v1_unknown_column_ref(self):
        validator = ProposalValidator()
        proposal = self._make_proposal(dimensions=[
            DimensionDecl(dimension_name="unknown_col", column_ref="nonexistent", source_table="ft"),
        ])
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert not valid

    # ── V3: time_function 白名单 ──
    def test_v3_invalid_time_function(self):
        validator = ProposalValidator()
        proposal = self._make_proposal(derived_dimensions=[
            DerivedDimensionDecl(
                dimension_name="pickup_day",
                source_column="pickup_at",
                source_table="ft",
                time_function="DAY",  # 非法
            ),
        ])
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert not valid

    # ── V7: CASE WHEN 空 branches ──
    def test_v7_empty_branches(self):
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(output_column="peak_type", branches=[], else_value=""),
        ])
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert not valid

    # ── V8: CASE WHEN 无 ELSE ──
    def test_v8_missing_else(self):
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={"node_type": "COMPARE", "left": "pickup_hour",
                                   "op": "GT", "right": {"node_type": "LITERAL",
                                   "value": 7, "data_type": "number"}},
                        then_value="高峰",
                    ),
                ],
                else_value="",  # 空 ELSE
            ),
        ])
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert not valid

    # ── V10b: LabelNot 节点拒绝 ──
    def test_v10b_rejects_label_not(self):
        validator = ProposalValidator()
        proposal = self._make_proposal(case_when_rules=[
            CaseWhenRule(
                output_column="peak_type",
                branches=[
                    CaseWhenBranch(
                        condition={
                            "node_type": "NOT",
                            "child": {
                                "node_type": "IS_NULL",
                                "column": "borough",
                            },
                        },
                        then_value="有值",
                    ),
                ],
                else_value="无值",
            ),
        ])
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert not valid

    # ── 全部通过 ──
    def test_valid_proposal_passes(self):
        validator = ProposalValidator()
        proposal = self._make_proposal()
        valid, questions = validator.validate(proposal, self._make_spec(), self._make_manifest())
        assert valid
        assert len(questions) == 0
```

- [ ] **Step 2: 创建 planning/proposal_validator.py 并实现全部 13+ 检查**

实现 V1-V13 检查项，详见设计文档 §6。关键逻辑：

```python
class ProposalValidator:
    """确定性校验——不调 LLM，不做语义推断。"""

    def validate(
        self,
        proposal: RequirementProposal,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[bool, list[OpenQuestion]]:
        questions: list[OpenQuestion] = []
        valid = True

        # 收集 manifest 中所有有效列名
        all_columns: set[str] = set()
        for table in manifest.tables:
            for col in table.columns:
                all_columns.add(col.column_name)

        # V1: dimension column_ref 存在
        for d in proposal.dimensions:
            if d.column_ref not in all_columns:
                questions.append(OpenQuestion(
                    question_id="V1",
                    field_ref=f"dimensions.{d.dimension_name}.column_ref",
                    description=f"列 '{d.column_ref}' 不在 SourceManifest 中",
                    level="blocking",
                ))
                valid = False

        # V2: derived_dimension source_column 存在
        for dd in proposal.derived_dimensions:
            if dd.source_column not in all_columns:
                questions.append(OpenQuestion(
                    question_id="V2",
                    field_ref=f"derived_dimensions.{dd.dimension_name}.source_column",
                    description=f"源列 '{dd.source_column}' 不在 SourceManifest 中",
                    level="blocking",
                ))
                valid = False

        # V3: time_function 白名单
        for dd in proposal.derived_dimensions:
            if dd.time_function not in {"HOUR"}:
                questions.append(OpenQuestion(
                    question_id="V3",
                    field_ref=f"derived_dimensions.{dd.dimension_name}.time_function",
                    description=f"时间函数 '{dd.time_function}' 不在白名单中（仅 HOUR）",
                    level="blocking",
                ))
                valid = False

        # V7: CASE WHEN branches 非空
        for rule in proposal.case_when_rules:
            if not rule.branches:
                questions.append(OpenQuestion(
                    question_id="V7",
                    field_ref=f"case_when_rules.{rule.output_column}.branches",
                    description=f"CASE WHEN '{rule.output_column}' 分支列表为空",
                    level="blocking",
                ))
                valid = False

        # V8: CASE WHEN else_value 非空
        for rule in proposal.case_when_rules:
            if not rule.else_value:
                questions.append(OpenQuestion(
                    question_id="V8",
                    field_ref=f"case_when_rules.{rule.output_column}.else_value",
                    description=f"CASE WHEN '{rule.output_column}' 缺少 ELSE 默认值",
                    level="blocking",
                ))
                valid = False

        # V10b: 条件中不含 LabelNot 节点
        for rule in proposal.case_when_rules:
            for i, branch in enumerate(rule.branches):
                if self._contains_not_node(branch.condition):
                    questions.append(OpenQuestion(
                        question_id="V10b",
                        field_ref=f"case_when_rules.{rule.output_column}.branches[{i}]",
                        description="CASE WHEN 条件含 LabelNot 节点——MVP 不支持",
                        level="blocking",
                    ))
                    valid = False

        # V11: 与程序员手写字段冲突检测
        declared_dim_names = {d.dimension_name for d in spec.dimensions}
        for d in proposal.dimensions:
            if d.dimension_name in declared_dim_names:
                questions.append(OpenQuestion(
                    question_id="V11",
                    field_ref=f"dimensions.{d.dimension_name}",
                    description=f"维度 '{d.dimension_name}' 与程序员手写声明冲突",
                    level="blocking",
                ))
                valid = False

        # ... V4, V5, V6, V9, V10, V12, V13 类似实现 ...

        return valid, questions

    @staticmethod
    def _contains_not_node(condition: dict) -> bool:
        """递归检查条件树是否含 LabelNot 节点。"""
        if isinstance(condition, dict):
            if condition.get("node_type") == "NOT":
                return True
            for child_key in ("children", "child"):
                child = condition.get(child_key)
                if isinstance(child, list):
                    for c in child:
                        if ProposalValidator._contains_not_node(c):
                            return True
                elif isinstance(child, dict):
                    if ProposalValidator._contains_not_node(child):
                        return True
            # 也检查 left/right（AND/OR 的子树）
            for key in ("left", "right"):
                child = condition.get(key)
                if isinstance(child, dict):
                    if ProposalValidator._contains_not_node(child):
                        return True
        return False
```

- [ ] **Step 3-5: 运行测试 + 零回归 + Commit**

---

### Task 10: planning/proposal_promotion.py — ProposalPromotion

**Files:**
- Create: `src/tianshu_datadev/planning/proposal_promotion.py`
- Test: `tests/planning/test_proposal_promotion.py` (新建)

**Interfaces:**
- Produces: `ProposalPromotion()` — 无参数构造函数
- Produces: `ProposalPromotion.promote(proposal, spec) -> ParsedDeveloperSpec` — 仅追加不覆盖

测试覆盖：Promotion 写入 dimensions/derived_dimensions/metrics/case_when_rules 到 spec 正式字段；不覆盖已存在字段；空 Proposal 返回原 spec。

---

### Task 11: api/pipeline.py — 管线集成（执行顺序反转 + SpecEnricher 简化）

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py` (`_enrich_and_plan` 执行顺序反转；`_run_requirement_planner` 实现)
- Modify: `src/tianshu_datadev/planning/spec_enricher.py` (移除 `scope` 参数——v3.1 不再需要)
- Modify: `src/tianshu_datadev/api/app.py` (`create_app()` 注入 `RequirementPlanner`)
- Modify: `src/tianshu_datadev/planning/__init__.py` (导出新组件)
- Test: `tests/planning/test_requirement_planner_e2e.py` (追加集成测试)

**Interfaces:**
- Modifies: `_enrich_and_plan` — 执行顺序：Planner → SpecEnricher(full) → unresolved → RelationshipPlanner
- Produces: `_run_requirement_planner(spec, manifest) -> tuple[ParsedDeveloperSpec, list[OpenQuestion]]`
- Modifies: `apply_enrichment` — 移除 `scope` 参数
- Modifies: `create_app()` — 注入 `RequirementPlanner`

- [ ] **Step 1: 修改 _enrich_and_plan 执行顺序**

```python
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

    # ── 2. SpecEnricher：完整 scope，后执行 ──
    if spec.dataset_type != DatasetType.LABEL_TABLE:
        spec = self._spec_enricher.apply_enrichment(spec, manifest)

    # ── 3. 统一 unresolved 检查 ──
    unresolved_after = _find_unresolved_derived_columns(spec)
    if unresolved_after:
        if self._adapter is None:
            raise ConfigError(
                f"以下输出列无法解析且无 LLM Adapter 可用：{unresolved_after}"
            )
        else:
            raise ConfigError(
                f"RequirementPlanner + SpecEnricher 后仍存在未解析列: {unresolved_after}"
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

- [ ] **Step 2: 实现 _run_requirement_planner**

```python
def _run_requirement_planner(
    self, spec: ParsedDeveloperSpec, manifest: SourceManifest,
) -> tuple[ParsedDeveloperSpec, list[OpenQuestion]]:
    t0 = time.monotonic()

    planner_output = self._requirement_planner.plan(spec, manifest)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    proposal = RequirementProposal(
        proposal_id=_gen_uuid(),
        spec_hash=spec.spec_hash,
        dimensions=planner_output.dimensions,
        derived_dimensions=planner_output.derived_dimensions,
        metrics=planner_output.metrics,
        case_when_rules=planner_output.case_when_rules,
        uncertainties=planner_output.uncertainties,
        llm_model=self._adapter.model if self._adapter else "",
        inference_time_ms=elapsed_ms,
        total_inferred=(len(planner_output.dimensions)
                        + len(planner_output.derived_dimensions)
                        + len(planner_output.metrics)
                        + len(planner_output.case_when_rules)),
    )

    valid, questions = self._proposal_validator.validate(proposal, spec, manifest)
    if not valid:
        return spec, questions

    spec = self._proposal_promotion.promote(proposal, spec)
    return spec, questions
```

- [ ] **Step 3: 移除 SpecEnricher scope 参数**

修改 `apply_enrichment` 方法签名，移除 `scope` 参数。

- [ ] **Step 4: 修改 create_app() 注入 RequirementPlanner**

```python
# api/app.py
requirement_planner = RequirementPlanner(adapter=adapter)
pipeline = Pipeline(
    ...
    requirement_planner=requirement_planner,
    ...
)
```

- [ ] **Step 5: 运行集成测试 + 全量回归**

```bash
python -m pytest tests/planning/test_requirement_planner_e2e.py tests/api/test_pipeline.py -v --timeout=120
python -m pytest tests/ -k "not harness" --timeout=120
```

- [ ] **Step 6: Commit**

---

### Task 12: tests/planning/test_time_transform_expr.py — 单元测试补全

将 Task 1-5 中分散追加的测试整合到最终形式。确认：
- TimeTransformExpr 模型校验（合法/非法时间函数）
- DerivedGroupKey 模型校验
- Predicate.left 接受 TimeTransformExpr + 向后兼容 ColumnRef
- SQL Compiler `_render_time_transform` 渲染
- `_render_aggregate` SELECT 含 `AS alias`
- `_render_flat_sql` GROUP BY 无 `AS alias`
- Builder `_build_aggregate_step` 生成 `DerivedGroupKey`

---

### Task 13: tests/planning/test_proposal_validator.py — Validator 全检查项

覆盖 V1-V13 所有检查项，每个检查项至少一个正例+反例。特别注意：
- V10b: `LabelNot` 节点三层防御（JSON Schema / Pydantic / Validator）
- `IS_NOT_NULL` 合法通过（不是 `LabelNot`）
- V8: 空 `else_value` 阻断
- V7: 空 `branches` 阻断

---

### Task 14: tests/planning/test_proposal_promotion.py + test_contract_time_transform.py

Promotion 测试：写入正式字段、不覆盖已有字段、空 Proposal → 原 spec。
Contract 测试：全链路 `ContractTimeTransform`→Mapper→Comparator + lite→v1 透传。

---

### Task 15: tests/planning/test_requirement_planner_e2e.py — 端到端集成测试

黄金链 E2E 测试：
- FakeAdapter→Planner→Validator→Promotion→Builder→CaseWhenStep
- 验证 `DerivedGroupKey` 在 `AggregateStep.group_keys` 中
- 验证 `CaseWhenStep` 的 `Predicate.left` 为 `TimeTransformExpr`
- 验证 Scan 包含 `pickup_at` 源列

---

### Task 16: Regression——conftest.py + test_compiler.py + test_spark_compiler.py

**Files:**
- Modify: `tests/api/conftest.py` (注入 FakeRequirementAdapter)
- Modify: `tests/sql/test_compiler.py` (DerivedGroupKey 编译回归)
- Modify: `tests/spark/test_spark_compiler.py` (SparkAggregateStep+time_transforms 编译回归)

- [ ] **Step 1: 修改 conftest.py 注入 FakeRequirementAdapter**

```python
# tests/api/conftest.py
from tianshu_datadev.llm.adapters.fake_adapter import FakeAdapter

@pytest.fixture
def fake_requirement_adapter():
    """Fake Adapter——用于 RequirementPlanner 测试。"""
    return FakeAdapter()
```

- [ ] **Step 2: SQL Compiler 回归测试**

```python
# tests/sql/test_compiler.py 追加
def test_aggregate_step_with_derived_group_key_compiles():
    """含 DerivedGroupKey 的 AggregateStep 应正确编译。"""
    ...
```

- [ ] **Step 3: Spark Compiler 回归测试**

```python
# tests/spark/test_spark_compiler.py 追加
def test_aggregate_step_with_time_transforms_compiles():
    """time_transforms=[] 时编译结果与现有基线一致。"""
    ...
```

- [ ] **Step 4: 全量回归**

```bash
python -m pytest tests/ -k "not harness" --timeout=120 -q
ruff check src/ tests/
```

预期：601+ passed, 0 failed, Ruff 零告警。

- [ ] **Step 5: Commit**

```bash
git add tests/api/conftest.py tests/sql/test_compiler.py tests/spark/test_spark_compiler.py
git commit -m "test: RequirementPlanner 全链路回归测试

- conftest.py: 注入 FakeRequirementAdapter
- test_compiler.py: DerivedGroupKey 编译回归
- test_spark_compiler.py: SparkAggregateStep+time_transforms 编译回归

Co-Authored-By: Claude <noreply@anthropic.com>"
```
