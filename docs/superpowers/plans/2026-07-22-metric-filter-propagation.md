# MetricFilterDecl Filter 全链路传播修复

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** 修复 `AggregateSpec.filter` (MetricFilterDecl) 在 Contract 提取边界丢失，导致 Spark 生成的聚合代码缺少 FILTER 条件，物理验证 RESULT_MISMATCH。

**Architecture:** 数据流 5 个断点一次性打通——Contract 模型 + Spark 模型新增 `filter` 字段，提取器 + Mapper 透传，Compiler 生成 Spark `F.when(cond, val)` 条件聚合表达式。

**Tech Stack:** Python 3.12, Pydantic v2 (StrictModel), PySpark, pytest

## Global Constraints

- `MetricFilterDecl` 模型定义不可修改——字段 `column`, `operator`, `value` 保持原样
- SQL 编译器路径不可修改——它直接从 `AggregateSpec.filter` 读取，需验证不退化
- 新增字段必须设 `None` 默认值——保持向后兼容，不影响所有现有调用点
- `MetricFilterDecl` 作为跨层类型在 Contract/Spark 模型中复用——不序列化为 dict
- Spark Compiler 生成 `F.when(condition, value)` 包装——利用 PySpark 自动忽略 NULL 的语义等价于 SQL FILTER
- 代码注释使用中文
- 提交消息格式：`fix: <描述>`，每个逻辑改动一个 commit
- 每条 metric filter 的 `operator` 值使用 MetricFilterDecl 定义的精确拼写：`"eq"`, `"neq"`, `"gt"`, `"gte"`, `"lt"`, `"lte"`, `"in"`, `"is_null"`, `"is_not_null"`
- Spark 渲染 map 必须覆盖所有 9 种 operator——默认分支抛 `ValueError`，不做静默回退

---

### Task 1: 打通 MetricFilterDecl filter 全链路（5 处修改 + 3 层测试）

**Files:**
- Modify: `src/tianshu_datadev/artifacts/models.py:83-88`
- Modify: `src/tianshu_datadev/artifacts/contract_extractor.py:404-411`
- Modify: `src/tianshu_datadev/spark/models.py:157-162`
- Modify: `src/tianshu_datadev/spark/mapper.py:486-493`
- Modify: `src/tianshu_datadev/spark/compiler.py:455-467`
- Modify: `tests/planning/test_contract_time_transform.py`（追加测试类）
- Create: `tests/spark/test_metric_filter_propagation.py`（Mapper + Compiler 测试）

**Interfaces:**
- Consumes: `MetricFilterDecl` from `tianshu_datadev.developer_spec.models`（字段 `column`, `operator`, `value`）
- Consumes: `AggregateSpec.filter: MetricFilterDecl | None` from `tianshu_datadev.planning.models`
- Produces: `ContractAggregation.filter: MetricFilterDecl | None = None`
- Produces: `SparkAggregateSpec.filter: MetricFilterDecl | None = None`
- Produces: `_compile_aggregate` 中 `F.when(cond, val)` 条件聚合

- [ ] **Step 1: ContractAggregation 新增 filter 字段**

编辑 `src/tianshu_datadev/artifacts/models.py:83-88`，在 `ContractAggregation` 中新增 `filter` 字段：

```python
from tianshu_datadev.developer_spec.models import MetricFilterDecl

class ContractAggregation(StrictModel):
    """Contract 中的聚合定义——精简自 AggregateSpec。"""

    function: str  # COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT
    input_column: str | None = None  # None 表示 COUNT(*)
    alias: str  # 输出别名
    filter: MetricFilterDecl | None = None  # 条件聚合 FILTER (WHERE ...)
```

确认 `MetricFilterDecl` 的 import 已存在于文件顶部或新增。

- [ ] **Step 2: _extract_aggregate 透传 m.filter**

编辑 `src/tianshu_datadev/artifacts/contract_extractor.py:404-411`，透传 `filter=m.filter`：

```python
        for m in step.metrics:
            aggs.append(
                ContractAggregation(
                    function=m.aggregation if isinstance(m.aggregation, str) else m.aggregation,
                    input_column=m.input_column,
                    alias=m.alias,
                    filter=m.filter,  # 透传条件聚合 FILTER
                )
            )
```

- [ ] **Step 3: SparkAggregateSpec 新增 filter 字段**

编辑 `src/tianshu_datadev/spark/models.py:157-162`，新增 `filter` 字段：

```python
from tianshu_datadev.developer_spec.models import MetricFilterDecl

class SparkAggregateSpec(StrictModel):
    """单个聚合指标——映射 ContractAggregation。"""

    function: SparkAggFunction  # 聚合函数
    input_column: str | None = None  # 输入列（COUNT(*) 时为 None）
    alias: str  # 输出列别名
    filter: MetricFilterDecl | None = None  # 条件聚合 FILTER (WHERE ...)
```

- [ ] **Step 4: _map_aggregations 映射 a.filter**

编辑 `src/tianshu_datadev/spark/mapper.py:486-493`，映射 `filter=a.filter`：

```python
    metrics = [
        SparkAggregateSpec(
            function=_AGG_FUNCTION_MAP[a.function.upper()],
            input_column=a.input_column,
            alias=a.alias,
            filter=a.filter,  # 透传条件聚合 FILTER
        )
        for a in aggregations
    ]
```

- [ ] **Step 5: _compile_aggregate 生成条件聚合表达式**

编辑 `src/tianshu_datadev/spark/compiler.py:455-467`，在渲染聚合指标时处理 `m.filter`。

核心思路：PySpark 无 SQL FILTER 语法，用 `F.when(condition, value).alias("col")` 包装。`F.when` 不匹配时返回 NULL，聚合函数自动忽略 NULL，等价于 SQL FILTER 语义。

操作符映射（与 SQL compiler `_render_metric_filter` 一致）：
```
eq → ==, neq → !=, gt → >, gte → >=, lt → <, lte → <=,
in → .isin(...), is_null → .isNull(), is_not_null → .isNotNull()
```

修改 `_compile_aggregate` 中的聚合指标循环：

```python
        for m in step.metrics:
            fn_name = self.renderer.render_agg_function(m.function)
            if m.input_column:
                col_ref = self.renderer.render_column(m.input_column)
                inner = col_ref
            else:
                # COUNT(*) → F.lit(1)
                inner = "F.lit(1)"
            # 条件聚合 FILTER——F.when(condition, inner) 包装
            if m.filter:
                cond = self._render_metric_filter_spark(m.filter)
                inner = f"F.when({cond}, {inner})"
            agg_expr = f"{fn_name}({inner})"
            alias = self.renderer.validate_identifier(
                m.alias, "AggregateSpec.alias"
            )
            agg_parts.append(f'{agg_expr}.alias("{alias}")')
```

在 `SparkCompiler` 类中新增 `_render_metric_filter_spark` 方法：

```python
    def _render_metric_filter_spark(self, filter_decl) -> str:
        """渲染 MetricFilterDecl 为 PySpark 条件表达式——用于 F.when()。
        
        操作符映射与 SQL compiler _render_metric_filter 语义一致：
          eq → ==, neq → !=, gt → >, gte → >=, lt → <, lte → <=,
          in → .isin(...), is_null → .isNull(), is_not_null → .isNotNull()
        """
        col = f'F.col("{filter_decl.column}")'
        op = filter_decl.operator
        val = filter_decl.value
        
        if op == "eq":
            return f'{col} == F.lit("{val}")'
        elif op == "neq":
            return f'{col} != F.lit("{val}")'
        elif op == "gt":
            return f'{col} > F.lit("{val}")'
        elif op == "gte":
            return f'{col} >= F.lit("{val}")'
        elif op == "lt":
            return f'{col} < F.lit("{val}")'
        elif op == "lte":
            return f'{col} <= F.lit("{val}")'
        elif op == "in":
            # value 是逗号分隔的字符串，拆分为列表
            items = [f'F.lit("{v.strip()}")' for v in val.split(",")]
            return f'{col}.isin({", ".join(items)})'
        elif op == "is_null":
            return f'{col}.isNull()'
        elif op == "is_not_null":
            return f'{col}.isNotNull()'
        else:
            raise ValueError(f"不支持的 MetricFilterDecl 操作符: {op!r}")
```

- [ ] **Step 6: Contract 提取器测试——filter 透传**

在 `tests/planning/test_contract_time_transform.py` 末尾追加测试类：

```python
class TestExtractAggregateWithMetricFilter:
    """_extract_aggregate 透传 MetricFilterDecl filter。"""

    def test_metric_filter_propagated_to_contract_aggregation(self):
        """带 filter 的 AggregateSpec → ContractAggregation.filter 非空。"""
        from tianshu_datadev.developer_spec.models import MetricFilterDecl
        
        agg = AggregateStep(
            step_id="agg_filter",
            group_keys=["borough"],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="anomaly_trip_count",
                    filter=MetricFilterDecl(
                        column="is_time_anomaly",
                        operator="eq",
                        value="true",
                    ),
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert len(aggs) == 1
        assert aggs[0].filter is not None
        assert aggs[0].filter.column == "is_time_anomaly"
        assert aggs[0].filter.operator == "eq"
        assert aggs[0].filter.value == "true"

    def test_metric_filter_none_when_not_set(self):
        """无 filter 的 AggregateSpec → ContractAggregation.filter 为 None。"""
        agg = AggregateStep(
            step_id="agg_no_filter",
            group_keys=["borough"],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="trip_count",
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert len(aggs) == 1
        assert aggs[0].filter is None
```

- [ ] **Step 7: Mapper + Compiler 测试**

创建 `tests/spark/test_metric_filter_propagation.py`：

```python
"""MetricFilterDecl filter 全链路传播测试——Mapper + Compiler。"""

import pytest
from tianshu_datadev.artifacts.models import ContractAggregation
from tianshu_datadev.developer_spec.models import MetricFilterDecl
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.mapper import _map_aggregations
from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkPlan,
    SparkReadStep,
)


class TestMapAggregationsWithFilter:
    """Mapper——Contract→Spark filter 映射。"""

    def test_filter_propagated_to_spark_aggregate_spec(self):
        """带 filter 的 ContractAggregation → SparkAggregateSpec.filter 正确映射。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(
                    function="COUNT",
                    alias="anomaly_trip_count",
                    filter=MetricFilterDecl(
                        column="is_time_anomaly",
                        operator="eq",
                        value="true",
                    ),
                ),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert len(step.metrics) == 1
        assert step.metrics[0].filter is not None
        assert step.metrics[0].filter.column == "is_time_anomaly"
        assert step.metrics[0].filter.operator == "eq"

    def test_filter_none_when_not_set(self):
        """无 filter 的 ContractAggregation → SparkAggregateSpec.filter=None。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert step.metrics[0].filter is None


class TestCompileAggregateWithFilter:
    """Compiler——Spark filter 代码生成。"""

    def test_compile_eq_filter_generates_when(self):
        """eq filter → F.when(F.col("col") == F.lit("val"), ...)"""
        plan = SparkPlan(
            plan_id="test_filter",
            source_contract_hash="abc",
            steps=[
                SparkReadStep(source_name="ft", alias="ft", input_key="ft"),
                SparkAggregateStep(
                    input_alias="ft",
                    group_keys=["borough"],
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias="anomaly_trip_count",
                            filter=MetricFilterDecl(
                                column="is_time_anomaly",
                                operator="eq",
                                value="true",
                            ),
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # 应包含 F.when(condition, F.lit(1)) 条件聚合
        assert "F.when(" in code
        assert 'F.col("is_time_anomaly")' in code
        assert "anomaly_trip_count" in code

    def test_compile_no_filter_no_when(self):
        """无 filter 的聚合不生成 F.when。"""
        plan = SparkPlan(
            plan_id="test_no_filter",
            source_contract_hash="abc",
            steps=[
                SparkReadStep(source_name="ft", alias="ft", input_key="ft"),
                SparkAggregateStep(
                    input_alias="ft",
                    group_keys=["borough"],
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias="trip_count",
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # 无 filter 时不生成 F.when
        assert "F.when(" not in code
        assert "trip_count" in code
```

- [ ] **Step 8: 运行测试验证**

```bash
# Contract 提取器测试
pytest tests/planning/test_contract_time_transform.py::TestExtractAggregateWithMetricFilter -xvs

# Mapper + Compiler 测试
pytest tests/spark/test_metric_filter_propagation.py -xvs

# 全量回归测试
pytest tests/ -x --tb=short
```

预期：新增测试全部 PASS，无回归失败。

- [ ] **Step 9: Ruff 检查**

```bash
ruff check src/tianshu_datadev/artifacts/models.py src/tianshu_datadev/artifacts/contract_extractor.py src/tianshu_datadev/spark/models.py src/tianshu_datadev/spark/mapper.py src/tianshu_datadev/spark/compiler.py tests/spark/test_metric_filter_propagation.py tests/planning/test_contract_time_transform.py
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "fix: MetricFilterDecl filter 全链路传播——Contract/Spark 模型+提取器+Mapper+Compiler"
```
