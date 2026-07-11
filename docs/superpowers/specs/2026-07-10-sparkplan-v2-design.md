# SparkPlan v2 架构设计 **[SUPERSEDED]**

> **此文档已被 `2026-07-11-sparkplan-minimal-alias-fix.md` 取代。**
> v2 完整架构被否决——实际实施的是最小别名修复方案。
> 保留此文档仅供历史参考。

> 消除 mapper/compiler/test 三套别名状态机冗余，将依赖解析 + 命名 + 唯一性统一到单一入口 `finalize_spark_plan()`。

## 目录

1. [数据模型——DraftSparkPlan → FinalizedSparkPlan](#section-1-数据模型)
2. [finalize_spark_plan() 算法](#section-2-finalize_spark_plan-算法)
3. [Compiler 简化](#section-3-compiler-简化)
4. [v1→v2 迁移——可信来源重建](#section-4-v1v2-迁移)
5. [测试策略](#section-5-测试策略)

---

## Section 1：数据模型

### 1.1 核心架构

```
DraftSparkPlan（别名未分配）
    │
    └─ finalize_spark_plan()
        │
        └─ FinalizedSparkPlan（output_alias 必填，frozen）
```

- **DraftSparkPlan**：Mapper 产出，节点无 `output_alias`
- **FinalizedSparkPlan**：`finalize_spark_plan()` 产出，所有节点 `output_alias` 已分配且冻结
- 两者共享 `SparkOperationV2` tagged union + `ScalarExpression` 封闭 AST

### 1.2 DraftSparkPlan

```python
class DraftSparkPlan(StrictModel):
    """别名未分配的 Spark 执行计划——Mapper 产出。"""

    source_contract_hash: str
    """来源 DataTransformContract 的 SHA-256。"""

    source_sqlprogram_hash: str
    """来源 SqlProgram 的 SHA-256。"""

    origin_contract_version: str = "v2"
    """Contract 来源版本——"v1" 表示经 migrate_v1_to_draft 迁移。"""

    steps: tuple[DraftSparkNode, ...]
    """按构造顺序排列的节点——尚未经拓扑排序。"""

    output_node_id: str
    """输出节点 ID——DAG 的汇点。"""

    params_schema: dict[str, str] = {}
    """运行时参数声明——参数名 → 类型（"date" | "timestamp"）。
    来自 Contract.params_schema——Compiler 用于渲染 cast。
    """
```

### 1.3 FinalizedSparkPlan

```python
class FinalizedSparkPlan(StrictModel):
    """冻结的 Spark 执行计划——所有节点已有 output_alias。"""

    model_config = ConfigDict(frozen=True)

    source_contract_hash: str
    source_sqlprogram_hash: str
    origin_contract_version: str
    steps: tuple[FinalizedSparkNode, ...]
    """按规范拓扑序排列的节点——顺序由 _canonical_topological_sort 确定。"""
    output_node_id: str
    params_schema: dict[str, str] = {}

    @model_validator(mode="after")
    def _verify_canonical_topological_order(self) -> "FinalizedSparkPlan":
        """每次反序列化时校验 steps 符合规范拓扑序。"""
        expected = _canonical_topological_sort(list(self.steps))
        actual = [n.node_id for n in self.steps]
        if actual != expected:
            raise ValueError(
                f"steps 顺序不符合规范拓扑序：期望 {expected}，实际 {actual}"
            )
        return self
```

### 1.4 DraftSparkNode / FinalizedSparkNode

```python
class DraftSparkNode(StrictModel):
    """别名未分配的 Plan 节点。"""
    node_id: str
    input_node_ids: tuple[str, ...]
    operation: SparkOperationV2


class FinalizedSparkNode(StrictModel):
    """别名已分配的 Plan 节点——由 finalize_spark_plan() 产出。"""
    model_config = ConfigDict(frozen=True)
    node_id: str
    input_node_ids: tuple[str, ...]
    operation: SparkOperationV2
    output_alias: str  # ← 必填，finalize 时分配
```

### 1.5 SparkOperationV2——9 种 Tagged Union

```python
SparkOperationV2 = (
    ReadOpV2
    | FilterOpV2
    | ProjectOpV2
    | SortOpV2
    | LimitOpV2
    | JoinOpV2
    | AggregateOpV2
    | CaseWhenOpV2
    | WindowOpV2
)
```

每种 Operation 均为 `ConfigDict(frozen=True)`，所有集合字段为 `tuple`。

**ReadOpV2**：
```python
class ReadOpV2(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_name: str       # inputs dict 的 key
    input_key: str         # Contract 中声明的 input key
```

**FilterOpV2**：
```python
class FilterOpV2(BaseModel):
    model_config = ConfigDict(frozen=True)
    predicate: Predicate   # Predicate AST——非自由字符串
```

**JoinOpV2**：
```python
class JoinOpV2(BaseModel):
    model_config = ConfigDict(frozen=True)
    left_key: ColumnRef
    right_key: ColumnRef
    join_type: JoinType
```

其余 6 种 Operation 同理——字段从 v1 的 `operator/left/right: str` 提升为结构化 AST。

### 1.6 ScalarExpression——封闭 AST

```python
ScalarExpression = LiteralValue | ColumnRef | RuntimeParameterRef | DateAdd | DateSub
```

**禁止 SQL 转义**：无 `is_sql_expr`、无自由字符串表达式。

```python
class LiteralValue(BaseModel):
    model_config = ConfigDict(frozen=True)
    value: int | float | str | bool

class ColumnRef(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_input_key: str   # lineage 解析用——渲染时不使用
    column_name: str

class RuntimeParameterRef(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str               # 对应 Contract.params_schema 中声明的参数

class DateAdd(BaseModel):
    model_config = ConfigDict(frozen=True)
    base: ColumnRef | RuntimeParameterRef
    interval: int
    unit: str = "DAY"       # 当前仅支持 DAY

class DateSub(BaseModel):
    model_config = ConfigDict(frozen=True)
    base: ColumnRef | RuntimeParameterRef
    interval: int
    unit: str = "DAY"
```

### 1.7 Predicate AST

```python
class Predicate(BaseModel):
    model_config = ConfigDict(frozen=True)
    operator: PredicateOperator
    left: ScalarExpression
    right: tuple[ScalarExpression, ...]  # 空用于 IS_NULL/IS_NOT_NULL
```

`PredicateOperator` 覆盖：`AND/OR/NOT/EQ/NEQ/GT/GTE/LT/LTE/IS_NULL/IS_NOT_NULL/IN/NOT_IN/BETWEEN/LIKE`。

**IN/NOT_IN 和 LIKE 的操作数类型在 Predicate Schema 层阻断**——不进入 Compiler 猜测。

### 1.8 RuntimeParameterRef 端到端

```
Contract.params_schema 声明允许的参数
    │
    ├─ EnvironmentBinding 绑定值（ISO 8601 + 强制时区偏移）
    │
    └─ RuntimeParameterRef(name="start_date") 引用
        │
        └─ Compiler 渲染 → F.lit(params["start_date"]).cast("date")
```

### 1.9 validate_dag()——三道校验

```text
Phase A: 基础结构——重复 node_id / 缺失依赖 / 入度校验
Phase B: 环路检测——Kahn 算法
Phase C: Join lineage——键的 source_input_key ∈ 对应输入的递归 lineage；
         lineage 有交集 → 通过（桥接表场景），交由 JoinSafetyValidator 处理
```

### 1.10 从 v1 模型中删除的项

`_sanitize`、`_truncate`、`_make_unique`、`_compute_stem`、`AliasState`/`AliasTracker`、`SourceAliasMap`/`SourceAliasBinding`、`mapping_hash`、`_find_output_chain_project`、Python 关键字检查、`isidentifier`、`_chain_input_aliases`（mapper）、`_CompileState.latest_alias`（compiler）、`generate_step_alias`（旧入口）、`ReadOpV2.source_alias`、`validate_source_key_alignment`（跨引擎）。

### 1.11 node_id 确定性规则

- 基于 `operation` 字段内容 + `input_node_ids`（**保持原始顺序**——Join 的左右语义依赖此顺序）
- 碰撞以错误拒绝，不追加后缀

---

## Section 2：finalize_spark_plan() 算法

### 2.1 三个核心函数

```python
def assign_source_aliases(steps: tuple[DraftSparkNode, ...]) -> dict[str, str]:
    """为 Read 节点分配 t1、t2...——按 source_input_key 字典序。"""

def _canonical_topological_sort(steps: list[DraftSparkNode]) -> list[str]:
    """Kahn + heapq——确定性拓扑序，返回 node_id 列表。"""

def finalize_spark_plan(draft: DraftSparkPlan) -> FinalizedSparkPlan:
    """单一入口：校验 DAG → 分配别名 → 拓扑排序 → 冻结输出。"""
```

### 2.2 序号别名规则

| 节点类型 | 别名格式 | 排序依据 |
|---------|---------|---------|
| ReadOpV2 | `t1`, `t2`, `t3`... | `source_input_key` 字典序 |
| 非 Read | `f1`, `f2`, `f3`... | 规范拓扑序中的位置 |

**不生成人类语义别名**——无 `_filtered`、`_with_`、`_by_`、`_output` 等后缀。

### 2.3 finalize_spark_plan() 单循环实现

```python
def finalize_spark_plan(draft: DraftSparkPlan) -> FinalizedSparkPlan:
    validate_dag(draft)

    # 分配 source aliases
    t_aliases = assign_source_aliases(draft.steps)

    # 规范拓扑序
    sorted_ids = _canonical_topological_sort(list(draft.steps))

    # 单循环构建 FinalizedSparkNode
    finalized_nodes: list[FinalizedSparkNode] = []
    for node_id in sorted_ids:
        node = _find_node(draft.steps, node_id)
        if isinstance(node.operation, ReadOpV2):
            alias = t_aliases[node.node_id]
        else:
            alias = f"f{_next_f_counter()}"  # 按拓扑序递增

        finalized_nodes.append(FinalizedSparkNode(
            node_id=node.node_id,
            input_node_ids=node.input_node_ids,
            operation=node.operation,
            output_alias=alias,
        ))

    return FinalizedSparkPlan(
        source_contract_hash=draft.source_contract_hash,
        source_sqlprogram_hash=draft.source_sqlprogram_hash,
        origin_contract_version=draft.origin_contract_version,
        steps=tuple(finalized_nodes),
        output_node_id=draft.output_node_id,
        params_schema=draft.params_schema,
    )
```

### 2.4 已删除的冗余存储

- 无 `SourceAliasMap`/`SourceAliasBinding`——t_aliases 是纯 `dict[str, str]`
- 无 `mapping_hash`——不需要
- 无 `finalize` 第二参数——不需要嵌入副本
- 无碰撞检测——tN/fN 永不冲突

---

## Section 3：Compiler 简化

### 3.1 compile() 最简结构

```python
def compile(
    self,
    plan: FinalizedSparkPlan,
    annotations: list | None = None,
) -> SparkCompileResult:
    """编译 FinalizedSparkPlan 为 PySpark DSL 代码。"""
    raw_lines: list[str] = []
    comment_lines: list[str] = []

    imports = self.renderer.render_imports()
    signature = self.renderer.render_function_signature()

    ann_map = _build_annotation_map(annotations)

    # 构建只读符号表 + 参数类型表
    symbols: dict[str, str] = {n.node_id: n.output_alias for n in plan.steps}
    param_types: dict[str, str] = self._build_param_types(plan.params_schema)

    # 防御性检查——防止先引用后定义
    seen: set[str] = set()
    for node in plan.steps:
        for nid in node.input_node_ids:
            if nid not in seen:
                raise RenderError(
                    f"节点 {node.node_id} 引用了未定义的输入 {nid}"
                )
        seen.add(node.node_id)

    for i, node in enumerate(plan.steps):
        input_aliases = tuple(symbols[nid] for nid in node.input_node_ids)
        ctx = RenderContext(
            symbols=symbols,
            relation_alias=input_aliases[0] if input_aliases else "",
            param_types=param_types,
        )
        raw, comment = self._compile_node(node, ctx, node.node_id, i, len(plan.steps))

        annotation = ann_map.get(node.node_id)
        if annotation is not None:
            comment = self._enhance_comment_with_annotation(comment, annotation)

        raw_lines.append(raw)
        if comment:
            comment_lines.append(comment)

    last_var = symbols[plan.output_node_id]

    body_raw = "\n".join(f"    {line}" for line in raw_lines)
    body_raw += f"\n    return {last_var}"
    raw_pyspark = f"{imports}\n\n\n{signature}\n{body_raw}\n"

    if comment_lines:
        body_annotated = "\n".join(
            f"    {line}" for line in self._interleave_comments(raw_lines, comment_lines)
        )
    else:
        body_annotated = "\n".join(f"    {line}" for line in raw_lines)
    body_annotated += f"\n    return {last_var}"
    annotated_pyspark = f"{imports}\n\n\n{signature}\n{body_annotated}\n"

    raw_hash = hashlib.sha256(raw_pyspark.encode()).hexdigest()
    self._verify_no_comment_injection(raw_pyspark, annotated_pyspark)

    return SparkCompileResult(
        raw_pyspark=raw_pyspark,
        annotated_pyspark=annotated_pyspark,
        raw_hash=raw_hash,
        step_ids=[node.node_id for node in plan.steps],
    )
```

**新增辅助函数**：
- `_build_param_types(params_schema)`——将 `{"start_date": "date", "end_date": "timestamp"}` 展平为 `dict[str, str]`，仅保留 `"date"` 和 `"timestamp"` 类型，其余忽略
- `_interleave_comments(raw_lines, comment_lines)`——按索引交替插入注释行到对应代码行上方：对于每对 `(raw, comment)`，若 comment 非空则先输出 `# comment` 行再输出 `raw`，否则仅输出 `raw`

**删除项**：`_CompileState` dataclass、`annotated_lines` 局部 list、`step_counter`/`next_step_id()`、`latest_alias`/`used_aliases`/`last_project_idx`。

### 3.2 RenderContext

```python
@dataclass(frozen=True)
class RenderContext:
    """只读渲染上下文——每个节点编译前创建一次。"""
    symbols: dict[str, str]        # node_id → output_alias
    relation_alias: str            # 当前操作的主输入 DataFrame 别名
    param_types: dict[str, str]    # 参数名 → "date" | "timestamp"
```

### 3.3 _compile_node() 分发

```python
def _compile_node(self, node, ctx, step_id, index, total):
    """根据 Operation 类型分发——九种独立语义边界。"""
    op = node.operation
    if isinstance(op, ReadOpV2):      return self._compile_read(node, ctx, ...)
    if isinstance(op, FilterOpV2):    return self._compile_filter(node, ctx, ...)
    if isinstance(op, ProjectOpV2):   return self._compile_project(node, ctx, ...)
    if isinstance(op, SortOpV2):      return self._compile_sort(node, ctx, ...)
    if isinstance(op, LimitOpV2):     return self._compile_limit(node, ctx, ...)
    if isinstance(op, JoinOpV2):      return self._compile_join(node, ctx, ...)
    if isinstance(op, AggregateOpV2): return self._compile_aggregate(node, ctx, ...)
    if isinstance(op, CaseWhenOpV2):  return self._compile_case_when(node, ctx, ...)
    if isinstance(op, WindowOpV2):    return self._compile_window(node, ctx, ...)
    raise RenderError(f"未知 Operation 类型：{type(op).__name__}")
```

**九种方法不合并**——每种是不同的受控语义边界。

### 3.4 各 _compile_* 统一模式

```python
def _compile_xxx(self, node, ctx, step_id, index, total):
    op = node.operation
    input_alias = ctx.relation_alias            # 单输入——Join 除外
    out_alias = node.output_alias                # 直接读取——不调用 generate_step_alias()
    # ... 渲染操作特定代码 ...
    raw = f"{out_alias} = {input_alias}.xxx(...)"
    comment = self._build_comment_block(...)
    return raw, comment
```

Join 特殊处理——使用 `ctx.symbols[input_node_ids[0]]` 和 `ctx.symbols[input_node_ids[1]]` 分别获取左右别名。

Read 特殊处理——无输入依赖，`input_node_ids` 为空时抛 `RenderError`（非 `assert`）。

### 3.5 _render_scalar_expr()

```python
def _render_scalar_expr(self, expr: ScalarExpression, ctx: RenderContext) -> str:
    if isinstance(expr, LiteralValue):
        return self.renderer.render_literal(expr.value)
    if isinstance(expr, ColumnRef):
        return f"{ctx.relation_alias}['{expr.column_name}']"
    if isinstance(expr, RuntimeParameterRef):
        key = self.renderer.render_dict_key(expr.name)
        base = f"params[{key}]"
        param_type = ctx.param_types.get(expr.name)
        if param_type == "date":
            return f"F.lit({base}).cast('date')"
        if param_type == "timestamp":
            return f"F.lit({base}).cast('timestamp')"
        return f"F.lit({base})"
    if isinstance(expr, DateAdd):
        base = self._render_scalar_expr(expr.base, ctx)
        return f"F.date_add({base}, {expr.interval})"
    if isinstance(expr, DateSub):
        base = self._render_scalar_expr(expr.base, ctx)
        return f"F.date_sub({base}, {expr.interval})"
    raise RenderError(f"未知 ScalarExpression 类型：{type(expr).__name__}")
```

**约束**：
- `ColumnRef`——`column_name` 已由 Schema 校验，无 SQL 注入
- `RuntimeParameterRef`——安全键访问 `params['key']`，无字符串拼接
- `DateAdd`/`DateSub`——仅 `F.date_add`/`F.date_sub`，`interval` 为整数，`unit` 当前仅 `DAY`
- `IN`/`NOT_IN`/`LIKE` 的操作数类型在 Predicate Schema 层阻断

### 3.6 最终对比

| 维度 | v1 | v2 |
|------|----|----|
| 编译状态 | `_CompileState` dataclass（5 字段） | `raw_lines` + `comment_lines` 两个局部 list |
| step_id | `step_counter` 自增生成 | `node.node_id` |
| 输入解析 | `latest_alias.get()` 运行时状态机 | `symbols[nid]` 只读字典 + `RenderContext` |
| 列引用 | `f"{alias}['{col}']"` 拼接 | `ctx.relation_alias`——编译前一次确定 |
| 参数渲染 | 无 | `F.lit(params["key"]).cast(type)` |
| annotated | `annotated_lines` 并行追踪 | `comment_lines + raw_lines` 直接组装 |
| 拓扑序保证 | 无 | `model_validator` 反序列化 + Compiler `seen` 防御 |
| 错误类型 | `AssertionError` | `RenderError` |
| step_ids | `state.step_ids` 逐个追加 | 返回时直接取 `[n.node_id for n in plan.steps]` |

---

## Section 4：v1→v2 迁移

### 4.1 核心原则

v1 `SparkPlan` 不作为迁移数据源——它只是"指针"。v1 的 `operator/left/right: str` 自由字符串不可靠。正确做法：从已验证的结构化产物完整重建 v2 Plan。

```
v1 SparkPlan（仅作指针）
    │
    ├─ source_contract_hash ──→ DataTransformContractV1 artifact（读取+校验）
    │                               │
    │                               └─ source_sqlprogram_hash ──→ SqlProgram artifact
    │                                                                   │
    │                                                                   └─ SqlBuildPlan artifacts
    │                                                                         │
    │                                                                         └─ _derive_contract_v2()
    │                                                                               │
    │                                                                               └─ map_contract_v2_to_draft()
    │                                                                                     │
    │                                                                                     └─ finalize_spark_plan()
    │
    └─ 任一环节失败 → HumanReviewRequired
```

### 4.2 迁移入口

```python
def rebuild_v2_from_trusted_sources(
    v1_plan: SparkPlan,
    artifact_store: ArtifactStore,
) -> FinalizedSparkPlan | HumanReviewRequired:
    """从可信来源完整重建 v2 FinalizedSparkPlan。"""
    contract_v1 = _read_and_verify_contract_v1(v1_plan.source_contract_hash, artifact_store)
    if isinstance(contract_v1, HumanReviewRequired):
        return contract_v1

    sqlprogram = _read_and_verify_sqlprogram(contract_v1.source_sqlprogram_hash, artifact_store)
    if isinstance(sqlprogram, HumanReviewRequired):
        return sqlprogram

    build_plan = _read_and_verify_build_plan(sqlprogram, artifact_store)
    if isinstance(build_plan, HumanReviewRequired):
        return build_plan

    contract_v2 = _derive_contract_v2(build_plan, contract_v1)
    draft = map_contract_v2_to_draft(contract_v2)
    return finalize_spark_plan(draft)
```

### 4.3 三级校验

每级校验：artifact 存在 → hash 匹配 → verified=True。任一失败 → `HumanReviewRequired`。

### 4.4 map_contract_v2_to_draft()

从 DataTransformContractV2 构建 DraftSparkPlan——纯结构化映射，不涉及字符串解析或别名推断。此函数也是 v2 新建 Plan 的正常入口。

### 4.5 HumanReviewRequired

```python
@dataclass(frozen=True)
class HumanReviewRequired:
    reason: str
    failed_at: str  # "contract_v1" | "sqlprogram" | "build_plan" | "derive_contract_v2"
    expected_hash: str | None = None
    actual_hash: str | None = None
    plan_id: str = ""
```

### 4.6 v1 Plan 两条路径

```
v1 SparkPlan (version="v1")
    ├─ 路径 A: v1 Compiler（向后兼容，不受影响）
    └─ 路径 B: rebuild_v2_from_trusted_sources() → v2 Compiler
```

### 4.7 删除清单

| 删除项 | 原因 |
|--------|------|
| `_convert_step` 及九种 `_convert_*` | v1 Step 不作为数据源 |
| `_parse_v1_predicate_string` / `_parse_v1_operand` | 禁止字符串解析 |
| `_make_node_id` / `get_v1_step_key` | 禁止旧别名推断 |
| v1 `alias` → v2 `node_id` 映射 | 无此需求 |
| `output_node_id` 反向猜测 | 从 BuildPlan DAG 拓扑确定 |
| 半迁移/静默降级 | 任何失败 → `HumanReviewRequired` |

---

## Section 5：测试策略

### 5.1 现有测试处理

| 文件 | 处理 | 原因 |
|------|------|------|
| `test_alias_generator.py` | **保留，加 LEGACY 注释** | v1 Compiler 仍存活——下线时一并移除 |
| `test_spark_compiler.py` | 保留——不改 | v1 路径 A 不受影响 |
| `test_spark_plan.py` | 保留——不改 | v1 Mapper/Plan 测试不受影响 |

### 5.2 新增文件

**一个文件**：`tests/spark/test_spark_plan_v2.py`

五个测试组，约 25 条参数化用例。

### 5.3 测试组一：finalize_spark_plan——表驱动

参数化覆盖：
- 单 Read、双 Read（按 source_input_key 字典序）
- Read→Filter 线性链
- Read→Filter→Filter 连续（t1→f1→f2→f3）
- Read←Read→Join 分支

每组验证 `output_alias` 符合 tN/fN 规则，且所有别名不重复。

### 5.4 测试组二：拓扑序与 DAG 校验——表驱动

参数化覆盖：
- 线性链保持顺序
- 分支 DAG 两次 finalize 结果完全一致
- 环路拒绝
- 缺失依赖拒绝
- Join lineage 键不在对应输入 lineage 中 → 拒绝

### 5.5 测试组三：集成链——Mapper→finalize→Compiler→ast.parse

**唯一一条集成测试**：构造 Draft → `finalize_spark_plan()` → `SparkCompiler.compile()` → `ast.parse()`。

验证：
- Python `ast` 解析通过——所有变量先定义后引用
- `return` 指向 `output_node_id` 对应 alias

**不手写模拟 symbols/latest_alias**——所有别名由 `finalize_spark_plan()` 真实分配。

### 5.6 测试组四：Predicate/Scalar 安全渲染——参数化

参数化覆盖：
- `RuntimeParameterRef` 安全键访问 + date cast
- `ColumnRef` 安全列名
- `DateAdd`/`DateSub` 渲染为 `F.date_add`/`F.date_sub`
- 未定义依赖 → `RenderError`

### 5.7 测试组五：迁移——参数化失败 + 一个集成成功

**参数化失败**（6 条）：
- ContractV1 / SqlProgram / SqlBuildPlan 各 2 条（缺失 + hash 不匹配）
- 每条断言 `failed_at` 指向正确环节

**集成成功**（1 条）：
- 构造完整有效产物链 → `FinalizedSparkPlan` → 可编译

### 5.8 验证命令

```bash
pytest tests/spark/ -x --tb=short
python -m ruff check .
git diff --check
```

---

## 全局约束

- 不修改 v1 models——`SparkPlan` 等保持原样
- 不移除 v1 Compiler——新旧并行，按 `plan.version` 分发
- 不静默降级——任何解析失败 → `MigrationError` 或 `HumanReviewRequired`
- 所有注释使用中文
- `./dev-reload.sh` 是重启的唯一入口
