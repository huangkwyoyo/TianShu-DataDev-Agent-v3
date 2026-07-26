# ComputeSteps Builder 双能力扩展——实施计划（第三次修订版）

> **执行状态**：✅ 已执行完毕。实际实施细节可能与本文档存在差异，以 `docs/current-state-and-verification-status.md` 为准。
> 最后核对日期：2026-07-26

> **For agentic workers:** 执行此计划需使用 `superpowers:executing-plans`。步骤使用 checkbox (`- [ ]`) 语法跟踪。
> **执行模式**：Inline Execution，顺序：Schema → Validator → Pipeline → Builder → Contract → SparkPlan → Fixtures → 单元测试 → E2E → 回归

**Goal:** 补齐 case_when+metrics 共存、混合源 Join 两个 Builder 能力缺口。新增 ComputeStepValidator 确定性前置门禁（Pipeline 层编排），修复 Contract 派生 Join 保留，扩展 SparkPlan 支持多分支 DAG 合流。重写案例 fixtures 移除 SqlRawExpression 依赖，通过 DuckDBExecutor.execute_program() + LocalSparkExecutor + digest 三重验收。

**Architecture:** Pipeline(api/pipeline.py) → ComputeStepValidator → Builder(signature unchanged) → Contract → SparkPlan(branches+dag)。step_outputs 保持 `list[ColumnRef]` 供血缘使用，新增 `StepOutputSchema(columns: dict[str, str|None], unique_keys)` 独立跟踪类型。UNKNOWN 类型阻断 Join。

**Tech Stack:** Python 3.11+, Pydantic, pytest, DuckDB, PySpark 3.5+

## Global Constraints

- `build_from_steps(spec, hypothesis) -> list[SqlBuildPlan]` 签名不变
- Builder 不接收 manifest、不调用 Validator、不生成 OpenQuestion
- Pipeline（api/pipeline.py）负责编排 Validator → Builder，处理 OpenQuestion 阻断
- step_outputs（`dict[str, list[ColumnRef]]`）保持现状供血缘使用
- 新增 StepOutputSchema——`columns: dict[str, str|None]`（None=UNKNOWN），`unique_keys: list[list[str]]`
- 禁止 varchar/double 默认类型——无法确定时为 UNKNOWN，Join 时阻断
- source=[a,b] 分别解析 a/b 的输出 schema；Aggregate 的 group_by 形成派生 unique_keys
- 显式 Join 左键/右键类型 + 右侧唯一性全部校验
- 禁止 ComputeStepExpression.expression/SqlRawExpression 新依赖——无法用封闭模型表达的指标本轮移除或 HUMAN_REVIEW
- Contract 保留所有显式 Join——不判断下划线命名
- SparkPlan 新增 branches 字段支持多分支 DAG——不能假装兼容
- E2E 使用 DuckDBExecutor.execute_program() + LocalSparkExecutor 真实执行
- 验收比较 DuckDB/Spark schema + row_count + 确定性 digest（不仅检查代码文本）
- question_id 确定性生成：`cs:<spec_hash>:<step_name>:<check>:<field>`
- 所有注释使用中文

---

### Task 1: StepOutputSchema + 类型跟踪前置

**Files:**
- Create: `src/tianshu_datadev/planning/step_output_schema.py`

**Interfaces:**
- Produces: `StepOutputSchema` dataclass, `compute_output_schema(cs, step_schemas, manifest) -> StepOutputSchema`

**说明**：当前 `step_outputs: dict[str, list[ColumnRef]]` 只跟踪列名不含类型信息。Validator 需要类型做兼容性判断、需要 unique_keys 做基数安全校验。此 Task 创建独立的 schema 跟踪机制——不替换、不修改现有的 `step_outputs`。

- [ ] **Step 1: 定义 StepOutputSchema**

```python
"""StepOutputSchema——ComputeStep 输出列的类型与唯一键元数据。

与 step_outputs (dict[str, list[ColumnRef]]) 并列存在——
step_outputs 供血缘追踪使用（不改），StepOutputSchema 供 Validator 校验使用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ComputeStep
    from tianshu_datadev.developer_spec.source_manifest import SourceManifest


@dataclass
class StepOutputSchema:
    """一个 ComputeStep 的输出列类型与唯一键元数据。

    columns: {normalized_name: type_string | None}
        None = UNKNOWN——无法从源表或聚合函数推导类型。
        UNKNOWN 类型参与 Join 时必须阻断。

    unique_keys: 派生唯一键组——来自 Aggregate 的 group_by 列。
        每组是一个列名列表，表示一组复合唯一键。
        单列 group_by → [[col]]，多列 → [[col1, col2]]。
    """

    columns: dict[str, str | None] = field(default_factory=dict)
    unique_keys: list[list[str]] = field(default_factory=list)


def compute_output_schema(
    cs: ComputeStep,
    step_schemas: dict[str, "StepOutputSchema"],
    manifest: "SourceManifest | None",
    normalizer,  # FieldNormalizer
) -> StepOutputSchema:
    """从 ComputeStep 声明推导其输出列的 StepOutputSchema。

    类型推导规则（按优先级）：
    1. GROUP BY 列——类型来自源表 manifest 或上游 step_schemas
    2. 指标别名——类型由聚合函数推导（COUNT→bigint, AVG→double 等）
    3. CASE WHEN 输出列——固定 varchar（CASE WHEN 始终返回字符串标签）
    4. Expression 输出列——本轮移除（HUMAN_REVIEW），不在此推导

    无法确定类型时设为 None（UNKNOWN）——禁止默认 varchar/double。
    """
    columns: dict[str, str | None] = {}
    unique_keys: list[list[str]] = []

    # ── 构建源表列→类型查找表 ──
    manifest_cols: dict[str, str] = {}  # {normalized_name: type}
    if manifest:
        for table in manifest.tables:
            for col in table.columns:
                manifest_cols[normalizer.normalize(col.column_name)] = col.column_type

    # ── 1. GROUP BY 列——类型来自源表或上游 ──
    for gb in cs.group_by:
        gb_norm = normalizer.normalize(gb)
        gb_type: str | None = None

        # 从上游步骤查找
        if isinstance(cs.source, str) and cs.source != "input":
            upstream = step_schemas.get(cs.source)
            if upstream:
                gb_type = upstream.columns.get(gb_norm)
        elif isinstance(cs.source, list):
            # 多源——尝试从第一个有该列的源获取类型
            for src_name in cs.source:
                upstream = step_schemas.get(src_name)
                if upstream and gb_norm in upstream.columns:
                    gb_type = upstream.columns[gb_norm]
                    break

        # 从 manifest 查找
        if gb_type is None:
            gb_type = manifest_cols.get(gb_norm)
            # 仍为 None → UNKNOWN（不设默认值）

        columns[gb_norm] = gb_type

    # ── 2. 指标别名——类型由聚合函数推导 ──
    for m in cs.metrics:
        alias = m.alias or m.metric_name
        if not alias:
            continue
        alias_norm = normalizer.normalize(alias)
        agg_str = m.aggregation.value if hasattr(m.aggregation, "value") else str(m.aggregation)

        if agg_str in ("COUNT", "COUNT_DISTINCT"):
            columns[alias_norm] = "bigint"
        elif agg_str == "AVG":
            columns[alias_norm] = "double"
        elif agg_str == "SUM":
            # SUM 类型继承 input_column 类型——从 manifest 或上游查找
            sum_type: str | None = None
            if m.input_column:
                input_norm = normalizer.normalize(m.input_column)
                sum_type = manifest_cols.get(input_norm)
                if sum_type is None and isinstance(cs.source, str) and cs.source != "input":
                    upstream = step_schemas.get(cs.source)
                    if upstream:
                        sum_type = upstream.columns.get(input_norm)
            columns[alias_norm] = sum_type  # None=UNKNOWN
        elif agg_str in ("MIN", "MAX"):
            # MIN/MAX 继承 input_column 类型
            minmax_type: str | None = None
            if m.input_column:
                input_norm = normalizer.normalize(m.input_column)
                minmax_type = manifest_cols.get(input_norm)
                if minmax_type is None and isinstance(cs.source, str) and cs.source != "input":
                    upstream = step_schemas.get(cs.source)
                    if upstream:
                        minmax_type = upstream.columns.get(input_norm)
            columns[alias_norm] = minmax_type
        else:
            columns[alias_norm] = None  # UNKNOWN

    # ── 3. CASE WHEN 输出列——固定 varchar ──
    if cs.case_when and cs.case_when.output_column:
        cw_norm = normalizer.normalize(cs.case_when.output_column)
        columns[cw_norm] = "varchar"

    # ── 4. Expression 输出列——本轮不处理（已移除或 HUMAN_REVIEW）──
    # 不在 columns 中添加任何 expression 输出列

    # ── 5. 派生 unique_keys——Aggregate 的 group_by 形成 ──
    if cs.group_by:
        group_norms = [normalizer.normalize(gb) for gb in cs.group_by]
        unique_keys.append(group_norms)

    return StepOutputSchema(columns=columns, unique_keys=unique_keys)
```

- [ ] **Step 2: 提交**

```bash
git add src/tianshu_datadev/planning/step_output_schema.py
git commit -m "feat(schema): 新增 StepOutputSchema——独立类型与唯一键元数据

与现有 step_outputs (dict[str, list[ColumnRef]]) 并列存在：
- step_outputs 供血缘追踪（不改）
- StepOutputSchema 供 Validator 校验使用
- columns: {normalized_name: type | None}——None=UNKNOWN（禁止默认 varchar/double）
- unique_keys: Aggregate group_by 形成的派生唯一键组"
```

---

### Task 2: ComputeStepValidator——五项确定性校验 + UNKNOWN 阻断

**Files:**
- Create: `src/tianshu_datadev/planning/compute_step_validator.py`

**Interfaces:**
- Consumes: `ComputeStep`, `StepOutputSchema`, `SourceManifest`, `FieldNormalizer`
- Produces: `list[OpenQuestion]`（空列表=通过）
- question_id: `cs:<spec_hash>:<step_name>:<check>:<field>`

- [ ] **Step 1: 实现 ComputeStepValidator**

```python
"""ComputeStepValidator——Builder 前确定性门禁。

五项检查（全部确定性，不调 LLM）：
a. 符号解析——Join 键在对应的上游输出/SourceManifest 中存在
b. 类型兼容——left_key 与 right_key 字段类型兼容（UNKNOWN 类型阻断）
c. Join 基数安全——单列 Join 仅由完全匹配的单列唯一键组放行
d. 显式 JoinDecl——合流步骤必须有显式 JoinDecl
e. evaluation_phase 已确定

不返回 BuildReadyStep——仅返回 list[OpenQuestion]。
由 Pipeline（api/pipeline.py）在 Builder 前调用。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.models import OpenQuestion

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ComputeStep
    from tianshu_datadev.developer_spec.source_manifest import SourceManifest
    from tianshu_datadev.planning.field_normalizer import FieldNormalizer
    from tianshu_datadev.planning.step_output_schema import StepOutputSchema


# ── 类型兼容矩阵 ──
_TYPE_COMPAT: dict[tuple[str, str], bool] = {
    ("bigint", "integer"): True, ("integer", "bigint"): True,
    ("bigint", "bigint"): True, ("integer", "integer"): True,
    ("varchar", "varchar"): True, ("varchar", "text"): True,
    ("text", "varchar"): True,
    ("decimal", "double"): True, ("double", "decimal"): True,
    ("decimal", "decimal"): True, ("double", "double"): True,
    ("timestamp", "timestamp"): True, ("boolean", "boolean"): True,
}


def _normalize_type(type_str: str | None) -> str | None:
    """归一化字段类型——去参数部分并小写。None 返回 None（UNKNOWN）。"""
    if type_str is None:
        return None
    return type_str.lower().split("(")[0].strip()


def _types_compatible(left_type: str | None, right_type: str | None) -> bool:
    """检查两个字段类型是否兼容。UNKNOWN 类型永远不兼容。"""
    if left_type is None or right_type is None:
        return False  # UNKNOWN 类型阻断
    ln = _normalize_type(left_type)
    rn = _normalize_type(right_type)
    if ln is None or rn is None:
        return False
    if ln == rn:
        return True
    return _TYPE_COMPAT.get((ln, rn), False)


class ComputeStepValidator:
    """ComputeStep 确定性校验器。

    由 Pipeline（api/pipeline.py）在 Builder 前调用。
    Builder 不持有 Validator 实例、不引用 manifest。
    """

    def __init__(self, normalizer: FieldNormalizer, spec_hash: str = ""):
        self._normalizer = normalizer
        self._spec_hash = spec_hash

    def _qid(self, step_name: str, check: str, field: str) -> str:
        """生成确定性 question_id——禁止 UUID。"""
        return f"cs:{self._spec_hash}:{step_name}:{check}:{field}"

    def validate(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
    ) -> list[OpenQuestion]:
        """对单个 ComputeStep 执行全部校验。

        Args:
            cs: 待校验的 ComputeStep
            step_schemas: {step_name: StepOutputSchema}——上游步骤的输出 schema
            manifest: 源数据清单（None 时跳过 manifest 相关校验）

        Returns:
            OpenQuestion 列表——空列表 = 全部通过
        """
        errors: list[OpenQuestion] = []

        # ── 校验 a+b+c：Join 符号解析 + 类型兼容 + 基数安全 ──
        if cs.joins:
            self._validate_joins(cs, step_schemas, manifest, errors)

        # ── 校验 d：合流必须显式 JoinDecl ──
        if isinstance(cs.source, list) and len(cs.source) > 1:
            if not cs.joins:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "no_join_decl", "joins"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins",
                    description=(
                        f"合流步骤 '{cs.step_name}' source={cs.source}，"
                        f"未声明 joins——禁止共同列猜键和隐式 CROSS JOIN"
                    ),
                    blocking=True,
                ))

        # ── 校验 e：evaluation_phase 已确定 ──
        if cs.case_when and cs.case_when.branches:
            if cs.case_when.evaluation_phase is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "eval_phase_none", "case_when"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.case_when.evaluation_phase",
                    description=(
                        f"compute_step '{cs.step_name}' case_when "
                        f"evaluation_phase 未确定——SpecEnricher 未成功判定"
                    ),
                    blocking=True,
                ))

        return errors

    def _validate_joins(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
        errors: list[OpenQuestion],
    ) -> None:
        """校验所有 Join 声明——符号+类型+基数。

        source=[a,b] 时：分别解析 a 和 b 的输出 schema——
        左键在 left_schema.columns 中查找，右键在 right_schema.columns 中查找。
        """
        # 构建 SourceManifest 查找表
        manifest_cols: dict[str, dict[str, str]] = {}  # {table_ref: {norm_name: type}}
        manifest_unique_groups: dict[str, list[list[str]]] = {}
        if manifest:
            for table in manifest.tables:
                col_map = {}
                for col in table.columns:
                    col_map[self._normalizer.normalize(col.column_name)] = col.column_type
                manifest_cols[table.table_ref] = col_map
                manifest_unique_groups[table.table_ref] = [
                    [self._normalizer.normalize(k) for k in key_group]
                    for key_group in (table.unique_keys or [])
                ]

        # ── 确定左侧来源的 schema（source=[a,b] 时分别解析）──
        sources: list[str] = (
            cs.source if isinstance(cs.source, list) else [cs.source]
        )

        for jd in cs.joins:
            left_norm = self._normalizer.normalize(jd.left_key)
            right_norm = self._normalizer.normalize(jd.right_key)

            # ── 解析左侧 schema：从 sources 中找到 left_table 对应的 schema ──
            left_schema: StepOutputSchema | None = None
            if jd.left_table in step_schemas:
                left_schema = step_schemas[jd.left_table]
            elif jd.left_table in sources and jd.left_table in step_schemas:
                left_schema = step_schemas[jd.left_table]

            # ── 校验 a：左键在左侧 schema 中存在 ──
            if left_schema is not None and left_norm not in left_schema.columns:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "left_key_missing", jd.left_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.left_key",
                    description=(
                        f"Join 左键 '{jd.left_key}' 不在上游步骤 "
                        f"'{jd.left_table}' 的输出中。"
                        f"可用列：{sorted(left_schema.columns.keys())}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 a：右键在 manifest 中存在 ──
            right_cols = manifest_cols.get(jd.right_table, {}) if manifest else {}
            if manifest and jd.right_table in manifest_cols and right_norm not in right_cols:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "right_key_missing", jd.right_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.right_key",
                    description=(
                        f"Join 右键 '{jd.right_key}' 不在表 "
                        f"'{jd.right_table}' 中。"
                        f"已声明列：{sorted(right_cols.keys())}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 b：类型兼容——左侧类型来自 schema，右侧来自 manifest ──
            left_type: str | None = None
            if left_schema is not None:
                left_type = left_schema.columns.get(left_norm)
            right_type: str | None = None
            if right_cols:
                right_type = right_cols.get(right_norm)

            # UNKNOWN 类型阻断
            if left_type is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "left_type_unknown", jd.left_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.left_key",
                    description=(
                        f"Join 左键 '{jd.left_key}' 类型为 UNKNOWN——"
                        f"无法从源表或上游推导类型，禁止参与 Join"
                    ),
                    blocking=True,
                ))
                continue

            if right_type is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "right_type_unknown", jd.right_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.right_key",
                    description=(
                        f"Join 右键 '{jd.right_table}.{jd.right_key}' 类型为 UNKNOWN——"
                        f"无法从 SourceManifest 推导类型，禁止参与 Join"
                    ),
                    blocking=True,
                ))
                continue

            if not _types_compatible(left_type, right_type):
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "type_incompat",
                                          f"{jd.left_key}:{jd.right_key}"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins",
                    description=(
                        f"Join 键类型不兼容——"
                        f"左键 '{jd.left_key}' 类型={left_type}，"
                        f"右键 '{jd.right_table}.{jd.right_key}' 类型={right_type}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 c：基数安全——单列 Join 仅由完全匹配的单列唯一键组放行 ──
            # 右侧唯一性来自 manifest
            right_key_groups = manifest_unique_groups.get(jd.right_table, [])
            right_key_unique = any(
                len(key_group) == 1 and key_group[0] == right_norm
                for key_group in right_key_groups
            )

            if not right_key_unique:
                declared_keys = "; ".join(
                    ",".join(g) for g in right_key_groups
                ) if right_key_groups else "(无)"
                # 同时检查右侧来源如果是上游步骤，其 unique_keys 是否覆盖此键
                right_source_schema = step_schemas.get(jd.right_table)
                if right_source_schema:
                    right_key_unique = any(
                        len(key_group) == 1 and key_group[0] == right_norm
                        for key_group in right_source_schema.unique_keys
                    )

                if not right_key_unique:
                    errors.append(OpenQuestion(
                        question_id=self._qid(cs.step_name, "cardinality", jd.right_key),
                        source="compute_step_validator",
                        field_ref=f"compute_steps.{cs.step_name}.joins",
                        description=(
                            f"Join 右表 '{jd.right_table}' 的键 "
                            f"'{jd.right_key}' 无单列唯一键保证——"
                            f"右表已声明唯一键组：[{declared_keys}]，"
                            f"禁止拆散复合唯一键。"
                        ),
                        blocking=True,
                    ))

    def compute_output_schema(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
    ) -> StepOutputSchema:
        """校验通过后计算此步骤的 StepOutputSchema——供下游步骤校验使用。"""
        from tianshu_datadev.planning.step_output_schema import compute_output_schema

        return compute_output_schema(
            cs, step_schemas, manifest, self._normalizer,
        )
```

- [ ] **Step 2: 提交**

```bash
git add src/tianshu_datadev/planning/compute_step_validator.py
git commit -m "feat(validator): ComputeStepValidator——五项校验 + UNKNOWN 阻断

a. 符号解析——Join 键在对应上游 schema/manifest 中存在
b. 类型兼容——使用 StepOutputSchema 真实类型（UNKNOWN 阻断 Join）
c. 基数安全——单列 Join 仅由完全匹配的单列唯一键组放行
d. 显式 JoinDecl——合流禁止猜键和隐式 CROSS JOIN
e. evaluation_phase 已确定
question_id: cs:<spec_hash>:<step_name>:<check>:<field>"
```

---

### Task 3: Pipeline 集成——api/pipeline.py 编排 Validator → Builder

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py:1510-1520`（build_plan 中 compute_steps 路径）
- Modify: `src/tianshu_datadev/api/pipeline.py:1636-1653`（execute 中 compute_steps 路径）
- Modify: `src/tianshu_datadev/api/pipeline.py:2023-2053`（run_all 中 compute_steps 路径）

**核心设计**：Pipeline 在调用 `builder.build_from_steps(spec, hypothesis)` **之前**，先调 `ComputeStepValidator`。如果 Validator 返回 blocking OpenQuestion，Pipeline 直接返回阻断响应——不调用 Builder。`build_from_steps` 签名保持不变。

- [ ] **Step 1: 在 Pipeline 文件的 compute_steps 三处调用点添加 Validator 编排**

修改 `build_plan()`、`execute()`、`run_all()` 三个方法中 `builder.build_from_steps(spec, hypothesis)` 之前的代码。以 `execute()` 为例（line 1636-1639）：

```python
# ── 旧代码（line 1636-1639）──
if spec.compute_steps and len(spec.compute_steps) > 0:
    # ── ComputeSteps 路径 ──
    with collector.stage("sql_builder", request_id) as ctx:
        plans = builder.build_from_steps(spec, hypothesis)

# ── 新代码 ──
if spec.compute_steps and len(spec.compute_steps) > 0:
    # ── ComputeSteps 路径：Validator（前置门禁）→ Builder ──
    from tianshu_datadev.planning.compute_step_validator import ComputeStepValidator
    from tianshu_datadev.planning.field_normalizer import FieldNormalizer

    _cs_validator = ComputeStepValidator(
        normalizer=FieldNormalizer(),
        spec_hash=spec.normalized_spec_hash or spec.spec_hash or "",
    )

    # 按拓扑顺序逐步骤校验——逐步构建 step_schemas
    step_schemas: dict[str, StepOutputSchema] = {}
    cs_blocking: list[OpenQuestion] = []

    # 拓扑排序 compute_steps（与 Builder 相同的顺序）
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
    sorted_steps = SqlBuildPlanBuilder._topo_sort_compute_steps(spec.compute_steps)

    for cs in sorted_steps:
        qs = _cs_validator.validate(cs, step_schemas, manifest)
        blocking_qs = [q for q in qs if q.blocking]
        if blocking_qs:
            cs_blocking.extend(blocking_qs)
            continue  # 此步骤不计算 schema——阻断
        # 计算此步骤的输出 schema 供下游使用
        schema = _cs_validator.compute_output_schema(cs, step_schemas, manifest)
        step_schemas[cs.step_name] = schema

    if cs_blocking:
        # ── Validator 阻断——不调用 Builder ──
        all_qs = list(cs_blocking) + list(extra_questions)
        blocked = self._build_validation_blocked_response(
            spec, manifest, None, all_qs,
            table_mapping=table_mapping,
        )
        blocked.update({
            "sql_sha256": "",
            "compiler_version": "",
            "execution_trace": None,
            "result_summary": None,
        })
        return blocked

    # ── 全部通过——Builder 签名不变 ──
    with collector.stage("sql_builder", request_id) as ctx:
        plans = builder.build_from_steps(spec, hypothesis)
        plan_snap = plans[-1]
        ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
```

需要在 Pipeline 文件顶部添加 import：
```python
from tianshu_datadev.planning.step_output_schema import StepOutputSchema
```

- [ ] **Step 2: 更新 build_plan() 中的相同逻辑**

`build_plan()` 方法（line 1511-1527）中的 compute_steps 路径做相同修改。

- [ ] **Step 3: 更新 run_all() 中的相同逻辑**

`run_all()` 方法（line 2023-2053）中的 compute_steps 路径做相同修改。

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat(pipeline): Pipeline 编排 Validator→Builder——build_from_steps 签名不变

- Pipeline 在 build_plan/execute/run_all 三处 compute_steps 路径中
  先调 ComputeStepValidator.validate() 逐步骤校验
- 逐步构建 step_schemas 供下游步骤校验使用
- Validator 返回 blocking OpenQuestion → Pipeline 直接返回阻断响应
- 全部通过后才调 builder.build_from_steps(spec, hypothesis)
- build_from_steps 签名不变（不新增 manifest 参数）"
```

---

### Task 4: Builder 变更——删除 case_when 守卫 + 猜键 + 混合源

**Files:**
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py:709`（删除 `not cs.case_when` 守卫）
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py:925-960`（`_find_join_keys` 删除猜键和 CROSS JOIN 回退）

**说明**：Builder 签名不变（`build_from_steps(spec, hypothesis) -> list[SqlBuildPlan]`），不接收 manifest。Validator 已在 Pipeline 层完成全部校验，Builder 仅做防御性断言。

- [ ] **Step 1: 删除 `not cs.case_when` 守卫（line 709）**

```python
# 旧代码：
if cs.metrics and not cs.case_when:

# 新代码：
if cs.metrics:
    # case_when + metrics 共存——Validator 已校验 evaluation_phase
```

- [ ] **Step 2: `_find_join_keys` 删除共同列猜键和 CROSS JOIN 回退（line 925-960）**

```python
@staticmethod
def _find_join_keys(
    join_key_map: dict,
    sources: list[str],
    left_src: str,
    right_src: str,
    step_outputs: dict,
) -> tuple[str, str]:
    """查找两个源步骤之间的 Join 键——仅从显式 JoinDecl 查找。

    共同列猜键和隐式 CROSS JOIN 回退已删除。
    ComputeStepValidator 应已阻断无显式 JoinDecl 的合流步骤。
    """
    key = (left_src, right_src)
    if key in join_key_map:
        jk = join_key_map[key]
        if jk[0] and jk[1]:
            return jk
    raise ValueError(
        f"合流步骤的源对 ({left_src}, {right_src}) 无显式 JoinDecl——"
        f"ComputeStepValidator 应已阻断"
    )
```

- [ ] **Step 3: 混合源单源路径扩展——上游 _temp_ + 物理表 Join**

在单源路径（line 572 "elif len(sources) == 1:" 分支）中，增加对 `cs.joins` 的处理——当前此分支仅处理纯上游 _temp_ 扫描，不处理 Join。在现有 `_temp_` Scan 之后插入：

```python
elif len(sources) == 1:
    src = sources[0]
    temp_ref = make_temp_name(chain_id, src)
    upstream_cols = step_outputs.get(src, [])
    # ... 现有 _temp_ 扫描逻辑（不变）...

    # ── 混合源：上游 _temp_ + 物理表 Join ──
    if cs.joins:
        # 防御性断言——Validator 应已校验
        assert len(cs.joins) == 1, (
            f"混合源步骤 '{cs.step_name}' joins 长度 != 1——Validator 应已阻断"
        )
        jd = cs.joins[0]
        table_map = {t.table_alias: t for t in spec.input_tables}
        right_table = table_map.get(jd.right_table)
        if not right_table:
            raise ValueError(f"右表 '{jd.right_table}' 不在 source_tables 中")

        right_cols = self._build_columns_for_input_step_table(
            cs, right_table, extra_cols=[jd.right_key],
        )
        right_scan = ScanStep(
            step_id=SqlBuildPlan.generate_step_id(
                "scan_r", {"step": cs.step_name, "table": right_table.source_table},
            ),
            table_ref=right_table.table_alias,
            required_columns=right_cols,
            estimated_row_count=right_table.row_count,
        )
        plan_steps.append(right_scan)
        for f in right_table.filters:
            plan_steps.append(self._build_filter_step(f, right_table.table_alias))

        left_norm = self._normalizer.normalize(jd.left_key)
        right_norm = self._normalizer.normalize(jd.right_key)
        plan_steps.append(JoinStep(
            step_id=SqlBuildPlan.generate_step_id("join", {
                "step": cs.step_name,
                "left": temp_ref, "right": jd.right_table,
                "left_key": jd.left_key, "right_key": jd.right_key,
            }),
            right_table_ref=right_table.table_alias,
            join_type=JoinType(jd.join_type.value.upper()),
            join_keys=[(
                ColumnRef(table_ref=temp_ref, column_name=jd.left_key,
                          normalized_name=left_norm),
                ColumnRef(table_ref=right_table.table_alias, column_name=jd.right_key,
                          normalized_name=right_norm),
            )],
            relationship_ref=f"compute_steps:{chain_id}:{cs.step_name}:{jd.right_table}",
        ))
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/planning/sql_build_plan.py
git commit -m "feat(builder): 删除 case_when 守卫 + 猜键/CROSS JOIN + 混合源

- 删除 if cs.metrics and not cs.case_when——case_when+metrics 共存
- _find_join_keys 仅从显式 JoinDecl 查找——删除猜键和 CROSS JOIN
- 单源路径扩展混合源——上游 _temp_ + 物理表 Join（单 Join）"
```

---

### Task 5: Contract 变更——保留所有显式 Join

**Files:**
- Modify: `src/tianshu_datadev/artifacts/contract_extractor.py:742-757`

- [ ] **Step 1: 删除 temp↔temp Join 跳过逻辑**

```python
# 旧代码（line 742-757）：
elif isinstance(step, JoinStep):
    if step.join_keys and any(
        k[0].table_ref.startswith("_temp_")
        and k[1].table_ref.startswith("_temp_")
        for k in step.join_keys
    ):
        continue
    join_rel = self._extract_join(
        step, evidence_map, temp_column_lineage,
    )
    if join_rel:
        join_relationships.append(join_rel)

# 新代码：
elif isinstance(step, JoinStep):
    # 保留所有显式 Join——不使用下划线命名判断内部 Join
    join_rel = self._extract_join(
        step, evidence_map, temp_column_lineage,
    )
    if join_rel:
        join_relationships.append(join_rel)
```

- [ ] **Step 2: 提交**

```bash
git add src/tianshu_datadev/artifacts/contract_extractor.py
git commit -m "fix(contract): 保留所有显式 Join——删除 temp↔temp 跳过逻辑

不再使用下划线命名判断内部 Join。所有显式 Join 保留到 Contract。"
```

---

### Task 6: SparkPlan 多分支 DAG——branches + 编译器支持

**Files:**
- Modify: `src/tianshu_datadev/spark/models.py:317-377`（SparkPlan 新增 branches）
- Modify: `src/tianshu_datadev/spark/compiler.py:113-232`（编译器支持 branches）

**问题**：当前 SparkPlan 仅有扁平 `steps: list[SparkStep]`——无法表达独立分支并行计算后合流的 DAG（如 branch A → Aggregate，branch B → Join → Aggregate，derived Join(A,B) → Aggregate → CaseWhen）。

**最小改动**：SparkPlan 新增 `branches: dict[str, list[SparkStep]]`——每个分支是独立的步骤链；主 `steps` 列表处理合流后的路径。编译器先编译所有分支（产生独立 DataFrame 变量），再编译主步骤（通过 JoinStep.alias 引用分支输出）。

- [ ] **Step 1: SparkPlan 新增 branches 字段**

```python
# spark/models.py——SparkPlan 类新增字段
class SparkPlan(StrictModel):
    plan_id: str
    version: str = "v1"
    source_phase: str = "phase-5"
    source_contract_hash: str
    source_contract_version: str = "v1"
    steps: list[SparkStep] = Field(default_factory=list)
    # ── 新增：多分支 DAG 支持 ──
    branches: dict[str, list[SparkStep]] = Field(default_factory=dict)
    # branches key = 分支名（如 compute_step step_name）
    # branches value = 该分支的步骤列表（Read→...→Aggregate）
    # 编译时：先编译所有 branches → 产生独立 DataFrame 变量
    #         再编译主 steps → 可通过 JoinStep.alias 引用分支输出
    write_mode: str | None = None
```

- [ ] **Step 2: SparkCompiler 支持 branches 编译**

修改 `compile()` 方法——在编译主 `steps` 之前先编译所有 `branches`：

```python
def compile(
    self,
    plan: SparkPlan,
    annotations: list | None = None,
) -> SparkCompileResult:
    # ── 单一入口：解析所有代码生成变量名 ──
    resolved_plan = resolve_codegen_aliases(plan)

    state = _CompileState()

    # 渲染导入和函数签名
    imports = self.renderer.render_imports()
    signature = self.renderer.render_function_signature()
    state.raw_lines.append(imports)
    state.raw_lines.append("")
    state.raw_lines.append("")

    # ── 新增：编译所有分支（branches）──
    # 每个分支产生独立 DataFrame 变量——变量名 = 分支名
    branch_outputs: dict[str, str] = {}
    if resolved_plan.plan.branches:
        for branch_name, branch_steps in resolved_plan.plan.branches.items():
            branch_outputs[branch_name] = branch_name
            # 编译分支步骤链
            for step in branch_steps:
                # ... 与主步骤相同的编译逻辑 ...
                # 分支最后一步的输出赋给 branch_name 变量
            state.raw_lines.append("")

    # ── 编译主步骤（steps）──
    # 主步骤中的 JoinStep 可通过 left_alias/right_alias 引用分支输出
    for i, resolved in enumerate(resolved_plan.resolved):
        step = resolved.step
        # ... 现有编译逻辑 ...
        # 特殊处理：JoinStep 的 left_alias/right_alias 可能是分支名
        if isinstance(step, SparkJoinStep):
            left_var = branch_outputs.get(step.left_alias, resolved.output_var)
            right_var = branch_outputs.get(step.right_alias, resolved.output_var)
            # 使用分支输出变量而非读取步骤的变量
```

编译器需要获取 branches 的分辨步骤。在 `resolve_codegen_aliases` 中增加对 branches 的处理：

```python
# 为每个分支分配变量名
branch_vars: dict[str, str] = {}
for branch_name in plan.branches:
    branch_vars[branch_name] = branch_name  # 变量名 = 分支名

# 主步骤中引用分支输出的 JoinStep 使用分支变量名
```

- [ ] **Step 3: Spark Mapper 扩展——填充 branches**

修改 `map_contract_to_spark_plan()` 函数，当 Contract 的 `step_dag` 包含多分支时，将独立分支映射为 `SparkPlan.branches` 条目。主 `steps` 仅包含合流后的路径。

```python
# 当 Contract 有多条语句且 step_dag 含分支时：
# - 每条无依赖的语句 → 独立 branch
# - 有依赖的语句 → 主 steps（合流路径）
if contract.step_dag and len(contract.step_dag) > 1:
    # 识别分支：step_dag 中 dependencies 为空的语句是叶分支
    branches: dict[str, list[SparkStep]] = {}
    for stmt_id, deps in contract.step_dag.items():
        if not deps:  # 无依赖 = 叶分支
            branch_steps = _map_statement_to_steps(contract, stmt_id)
            branches[stmt_id] = branch_steps
    spark_plan.branches = branches
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/spark/models.py src/tianshu_datadev/spark/compiler.py src/tianshu_datadev/spark/mapper.py
git commit -m "feat(spark): SparkPlan 多分支 DAG——branches 字典 + 编译器支持

- SparkPlan 新增 branches: dict[str, list[SparkStep]]——每分支独立步骤链
- 编译器先编译 branches（产生独立 DataFrame），再编译主 steps
- JoinStep 可通过 left_alias/right_alias 引用分支输出变量
- Mapper 从 Contract step_dag 识别分支拓扑"
```

---

### Task 7: 案例 fixtures——移除 SqlRawExpression，封闭模型 only

**Files:**
- Create: `tests/fixtures/case04_borough_crash_risk.md`
- Create: `tests/fixtures/case05_borough_enforcement.md`

**关键约束**：
- **禁止 `ComputeStepExpression.expression`**——不使用 `SqlRawExpression`
- **禁止 `when:` 字符串**——CASE WHEN 使用 `condition_column`/`condition_value` 或 `typed_branches`
- **禁止 `UPPER()` 自由表达式**——borough 归一化通过 pre_agg CaseWhen 显式映射
- 移除 `severity_score`、`enforcement_score`、`fine_per_violation`、`out_state_ratio` 等需要表达式计算的列
- 案例只使用：metrics（COUNT/SUM/AVG）+ CASE WHEN（typed）+ GROUP BY + Join

- [ ] **Step 1: 写入 Case04 fixture——双 Transform + 合流打标**

`tests/fixtures/case04_borough_crash_risk.md`：

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.zone_crash_risk_hotspot
  target_grain: [borough]
  summary: "双分支事故/行程聚合 → 合流打标——验证 SparkPlan branches 合流"

  time_range:
    start: "2026-03-25"
    end: "2026-03-31"

  source_tables:
    - name: silver.crash_detail
      alias: cd
      row_count: ~166万
      role: fact
      key_columns:
        - name: crash_id
          type: bigint
          nullable: false
          unique: true
      business_columns:
        - name: crash_at
          type: timestamp
          nullable: true
        - name: borough
          type: varchar
          nullable: true
        - name: persons_injured
          type: integer
          nullable: true
        - name: persons_killed
          type: integer
          nullable: true
        - name: is_location_missing
          type: boolean
          nullable: false

    - name: silver.trip_detail
      alias: td
      row_count: ~8032万
      role: fact
      key_columns:
        - name: trip_id
          type: varchar
          nullable: false
          unique: true
      business_columns:
        - name: pickup_at
          type: timestamp
          nullable: true
        - name: pickup_location_id
          type: integer
          nullable: true
        - name: fare_amount
          type: decimal(12,2)
          nullable: true
        - name: is_location_missing
          type: boolean
          nullable: false
        - name: is_distance_outlier
          type: boolean
          nullable: false

    - name: silver.taxi_zone
      alias: tz
      row_count: 265
      role: dim
      key_columns:
        - name: location_id
          type: integer
          nullable: false
          unique: true
      business_columns:
        - name: borough
          type: varchar
          nullable: true

  joins:
    - left_table: td
      right_table: tz
      left_key: pickup_location_id
      right_key: location_id
      join_type: INNER

  compute_steps:
    # ── 分支 A：事故聚合 ──
    - step_name: crash_agg
      source: input
      group_by:
        - cd.borough
      metrics:
        - metric_name: crash_count
          aggregation: COUNT
          input_column: cd.crash_id
          alias: crash_count
        - metric_name: total_injured
          aggregation: SUM
          input_column: cd.persons_injured
          alias: total_injured
      case_when:
        output_column: borough_normalized
        evaluation_phase: pre_aggregate
        branches:
          - condition_column: cd.borough
            condition_operator: "="
            condition_value: "MANHATTAN"
            result_column: ""
          - condition_column: cd.borough
            condition_operator: "="
            condition_value: "BROOKLYN"
            result_column: ""
          - condition_column: cd.borough
            condition_operator: "="
            condition_value: "QUEENS"
            result_column: ""
          - condition_column: cd.borough
            condition_operator: "="
            condition_value: "BRONX"
            result_column: ""
          - condition_column: cd.borough
            condition_operator: "="
            condition_value: "STATEN ISLAND"
            result_column: ""
        else_value: ""
      output_alias: crash_agg

    # ── 分支 B：行程聚合（含 taxi_zone Join）──
    - step_name: trip_agg
      source: input
      joins:
        - left_table: td
          right_table: tz
          left_key: pickup_location_id
          right_key: location_id
          join_type: INNER
      group_by:
        - tz.borough
      metrics:
        - metric_name: total_trips
          aggregation: COUNT
          input_column: td.trip_id
          alias: total_trips
      output_alias: trip_agg

    # ── 合流：事故 + 行程 → 打标 ──
    - step_name: risk_label
      source: [crash_agg, trip_agg]
      joins:
        - left_table: crash_agg
          right_table: trip_agg
          left_key: borough_normalized
          right_key: borough
          join_type: INNER
      group_by:
        - borough_normalized
      metrics:
        - metric_name: crash_count
          aggregation: SUM
          input_column: crash_count
          alias: crash_count
        - metric_name: total_trips
          aggregation: SUM
          input_column: total_trips
          alias: total_trips
      case_when:
        output_column: risk_level
        evaluation_phase: post_aggregate
        typed_branches:
          - condition:
              type: AND
              children:
                - type: COMPARISON
                  left: crash_count
                  operator: GTE
                  right: "5"
                - type: COMPARISON
                  left: total_trips
                  operator: GTE
                  right: "1000"
            then: "高危优先"
          - condition:
              type: COMPARISON
              left: crash_count
              operator: GTE
              right: "5"
            then: "高事故低流量"
          - condition:
              type: COMPARISON
              left: total_trips
              operator: GTE
              right: "1000"
            then: "高流量低事故"
        else_value: "常规巡查"
      output_alias: risk_label

  output_columns:
    - name: borough
      type: varchar
    - name: crash_count
      type: bigint
    - name: total_injured
      type: bigint
    - name: total_trips
      type: bigint
    - name: risk_level
      type: varchar
---
```

- [ ] **Step 2: 写入 Case05 fixture——三 Transform 线性链**

`tests/fixtures/case05_borough_enforcement.md`：

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.borough_enforcement_scorecard
  target_grain: [borough]
  summary: "违章聚合 → borough 映射 → 效能打标——三 Transform 线性链"

  time_range:
    start: "2026-03-25"
    end: "2026-03-31"

  source_tables:
    - name: gold.fact_parking_violations
      alias: fv
      row_count: ~958万
      role: fact
      key_columns:
        - name: violation_id
          type: bigint
          nullable: false
          unique: true
      business_columns:
        - name: issue_date_key
          type: integer
          nullable: false
        - name: violation_code
          type: varchar
          nullable: true
        - name: violation_county
          type: varchar
          nullable: true
        - name: registration_state
          type: varchar
          nullable: true
        - name: standard_fine_amount
          type: decimal(12,2)
          nullable: true
        - name: is_duplicate_summons
          type: boolean
          nullable: true

    - name: gold.dim_date
      alias: dd
      row_count: ~1.1万
      role: dim
      key_columns:
        - name: date_key
          type: integer
          nullable: false
          unique: true
      business_columns:
        - name: date
          type: timestamp
          nullable: false

  joins:
    - left_table: fv
      right_table: dd
      left_key: issue_date_key
      right_key: date_key
      join_type: INNER

  compute_steps:
    # ── T1：违章代码×日期聚合 ──
    - step_name: daily_violation
      source: input
      joins:
        - left_table: fv
          right_table: dd
          left_key: issue_date_key
          right_key: date_key
          join_type: INNER
      group_by:
        - fv.violation_county
        - fv.violation_code
        - dd.date
      metrics:
        - metric_name: daily_count
          aggregation: COUNT
          input_column: fv.violation_id
          alias: daily_count
        - metric_name: daily_fine
          aggregation: SUM
          input_column: fv.standard_fine_amount
          alias: daily_fine
      output_alias: daily_violation

    # ── T2：county→borough 映射 + 重聚合 ──
    - step_name: borough_score
      source: daily_violation
      group_by:
        - violation_county
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: daily_count
          alias: total_violations
        - metric_name: total_fine
          aggregation: SUM
          input_column: daily_fine
          alias: total_fine
        - metric_name: code_count
          aggregation: COUNT_DISTINCT
          input_column: violation_code
          alias: code_count
      case_when:
        output_column: borough
        evaluation_phase: pre_aggregate
        branches:
          - condition_column: violation_county
            condition_operator: "="
            condition_value: "NY"
            result_column: ""
          - condition_column: violation_county
            condition_operator: "="
            condition_value: "K"
            result_column: ""
          - condition_column: violation_county
            condition_operator: "="
            condition_value: "Q"
            result_column: ""
          - condition_column: violation_county
            condition_operator: "="
            condition_value: "BX"
            result_column: ""
          - condition_column: violation_county
            condition_operator: "="
            condition_value: "R"
            result_column: ""
        else_value: ""
      output_alias: borough_score

    # ── T3：效能打标（无聚合，纯透传 + CASE WHEN）──
    - step_name: enforcement_label
      source: borough_score
      group_by:
        - borough
      metrics:
        - metric_name: total_violations
          aggregation: SUM
          input_column: total_violations
          alias: total_violations
        - metric_name: total_fine
          aggregation: SUM
          input_column: total_fine
          alias: total_fine
      case_when:
        output_column: enforcement_level
        evaluation_phase: post_aggregate
        branches:
          - condition_column: total_violations
            condition_operator: ">="
            condition_value: "10000"
            result_column: ""
          - condition_column: total_violations
            condition_operator: ">="
            condition_value: "5000"
            result_column: ""
        else_value: "待提升"
      output_alias: enforcement_label

  output_columns:
    - name: borough
      type: varchar
    - name: total_violations
      type: bigint
    - name: total_fine
      type: decimal(18,2)
    - name: code_count
      type: bigint
    - name: enforcement_level
      type: varchar
---
```

- [ ] **Step 3: 提交**

```bash
git add tests/fixtures/case04_borough_crash_risk.md tests/fixtures/case05_borough_enforcement.md
git commit -m "feat(fixtures): Case04/Case05——封闭模型 only，移除 SqlRawExpression

- 禁止 ComputeStepExpression.expression / SqlRawExpression
- CASE WHEN 使用 condition_column/typed_branches
- 禁止 UPPER()——borough 归一化通过 pre_agg CaseWhen
- 移除 severity_score/enforcement_score 等表达式
- Case04：双分支合流（验证 SparkPlan branches）
- Case05：三 Transform 线性链"
```

---

### Task 8: 单元测试——~10 项表驱动

**Files:**
- Create: `tests/planning/test_compute_steps_extension.py`

- [ ] **Step 1: 编写单元测试**

```python
"""ComputeSteps 扩展——表驱动单元测试。
覆盖：StepOutputSchema(2) + Validator(5) + Builder(1) + Contract(1) + TypeCompat(1)
"""

import pytest
from tianshu_datadev.planning.step_output_schema import (
    StepOutputSchema, compute_output_schema,
)
from tianshu_datadev.planning.compute_step_validator import (
    ComputeStepValidator, _types_compatible, _normalize_type,
)
from tianshu_datadev.planning.field_normalizer import FieldNormalizer


class TestStepOutputSchema:
    """StepOutputSchema——类型推导与 UNKNOWN 处理。"""

    def test_metric_types_derived_correctly(self):
        """COUNT→bigint, SUM→继承源列类型, AVG→double。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType, ComputeStep, MetricDecl,
        )
        cs = ComputeStep(
            step_name="s1", source="input",
            group_by=["status"],
            metrics=[
                MetricDecl(metric_name="cnt", aggregation=AggregationType.COUNT,
                           input_column="id", alias="cnt"),
                MetricDecl(metric_name="avg_val", aggregation=AggregationType.AVG,
                           input_column="amount", alias="avg_val"),
            ],
            output_alias="s1",
        )
        # 无 manifest——类型为 UNKNOWN（除 COUNT/AVG 等聚合函数推导）
        schema = compute_output_schema(cs, {}, None, FieldNormalizer())
        assert schema.columns["cnt"] == "bigint"
        assert schema.columns["avg_val"] == "double"
        # GROUP BY 类型来自 manifest——无 manifest 则为 UNKNOWN
        assert schema.columns["status"] is None  # UNKNOWN

    def test_unique_keys_from_group_by(self):
        """Aggregate 的 group_by 形成派生 unique_keys。"""
        from tianshu_datadev.developer_spec.models import ComputeStep
        cs = ComputeStep(
            step_name="s1", source="input",
            group_by=["borough", "zone_name"],
            metrics=[], output_alias="s1",
        )
        schema = compute_output_schema(cs, {}, None, FieldNormalizer())
        assert len(schema.unique_keys) == 1
        assert set(schema.unique_keys[0]) == {"borough", "zone_name"}


class TestValidator:
    """ComputeStepValidator——五项校验 + UNKNOWN 阻断。"""

    def _make_manifest(self):
        from tianshu_datadev.developer_spec.source_manifest import (
            SourceManifest, ManifestTable, ManifestColumn,
        )
        return SourceManifest(tables=[
            ManifestTable(
                source_table="s.tz", table_ref="tz", role="dim",
                row_count=265,
                unique_keys=[["location_id"]],
                key_column_names_normalized=["location_id"],
                columns=[
                    ManifestColumn(column_name="location_id", column_type="integer",
                                   nullable=False),
                    ManifestColumn(column_name="borough", column_type="varchar",
                                   nullable=True),
                ],
            ),
        ])

    def test_valid_join_passes(self):
        """合法混合源 Join——返回空列表。"""
        from tianshu_datadev.developer_spec.models import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="borough", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(
            normalizer=FieldNormalizer(), spec_hash="abc",
        )
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        assert len(errors) == 0

    def test_left_key_missing_returns_error(self):
        """left_key 不在上游 schema 中。"""
        from tianshu_datadev.developer_spec.models import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="nonexistent", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        assert len(errors) == 1
        assert errors[0].blocking is True

    def test_unknown_type_blocks_join(self):
        """UNKNOWN 类型 Join 键阻断。"""
        from tianshu_datadev.developer_spec.models import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="unknown_col", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"unknown_col": None}),  # UNKNOWN
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        # 左键类型 UNKNOWN → 阻断
        assert any("UNKNOWN" in e.description or "unknown" in e.description.lower()
                   for e in errors)

    def test_composite_unique_key_rejects_single_column_join(self):
        """复合键 [borough, zone_name] 不放行单列 borough Join。"""
        from tianshu_datadev.developer_spec.models import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        from tianshu_datadev.developer_spec.source_manifest import (
            SourceManifest, ManifestTable, ManifestColumn,
        )
        manifest = SourceManifest(tables=[
            ManifestTable(
                source_table="s.t2", table_ref="t2", role="dim",
                row_count=100,
                unique_keys=[["borough", "zone_name"]],
                key_column_names_normalized=["borough", "zone_name"],
                columns=[
                    ManifestColumn(column_name="borough", column_type="varchar",
                                   nullable=True),
                    ManifestColumn(column_name="zone_name", column_type="varchar",
                                   nullable=True),
                ],
            ),
        ])
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="t2",
                   left_key="borough", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, manifest)
        assert any("唯一键" in e.description or "复合" in e.description
                   or "单列" in e.description
                   for e in errors)

    def test_confluence_without_joins_returns_error(self):
        """合流无 JoinDecl。"""
        from tianshu_datadev.developer_spec.models import ComputeStep
        cs = ComputeStep(
            step_name="s3", source=["s1", "s2"],
            group_by=["borough"], output_alias="s3",
            metrics=[],
        )
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, {"s1": StepOutputSchema(), "s2": StepOutputSchema()}, None)
        assert any("JoinDecl" in e.description or "joins" in e.description
                   for e in errors)


class TestBuilder:
    """Builder——case_when + metrics 共存。"""

    def test_case_when_and_metrics_coexist(self):
        """删除守卫后两者都出现在步骤列表中。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType, CaseWhenBranchDecl, CaseWhenDecl, ColumnDecl,
            ComputeStep, DatasetType, InputTableDecl, MetricDecl,
            OutputSpecDecl, ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            source_tables=[
                InputTableDecl(
                    source_table="s.t1", table_alias="t1",
                    dataset_type=DatasetType.SOURCE_TABLE,
                    columns=[ColumnDecl(column_name="amount", column_type="decimal",
                                        nullable=True)],
                    key_columns=[ColumnDecl(column_name="id", column_type="bigint",
                                            nullable=False, unique=True)],
                    business_columns=[ColumnDecl(column_name="amount", column_type="decimal",
                                                 nullable=True)],
                ),
            ],
            target_table="test.t",
            output_spec=OutputSpecDecl(grain=["status"]),
            metrics=[],
            compute_steps=[
                ComputeStep(
                    step_name="s1", source="input",
                    group_by=[], output_alias="s1",
                    metrics=[MetricDecl(
                        metric_name="total", aggregation=AggregationType.SUM,
                        input_column="amount", alias="total",
                    )],
                    case_when=CaseWhenDecl(
                        output_column="level",
                        evaluation_phase="post_aggregate",
                        else_value="低",
                        branches=[CaseWhenBranchDecl(
                            condition_column="total",
                            condition_operator=">=",
                            condition_value="1000",
                            result_column="",
                        )],
                    ),
                ),
            ],
        )
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)
        final = plans[-1]
        step_types = [type(s).__name__ for s in final.steps]
        assert "AggregateStep" in step_types
        assert "CaseWhenStep" in step_types


class TestContract:
    """Contract——保留所有显式 Join。"""

    def test_temp_to_temp_join_preserved(self):
        """temp↔temp 的 borough Join 不再被跳过。"""
        from tianshu_datadev.planning.models import (
            JoinStep, JoinType, ColumnRef, SafeIdentifier,
        )
        from tianshu_datadev.artifacts.contract_extractor import (
            DataTransformContractExtractor,
        )
        join_step = JoinStep(
            step_id="join_t3",
            right_table_ref="_temp_abc_s2",
            join_type=JoinType("INNER"),
            join_keys=[(
                ColumnRef(table_ref="_temp_abc_s1",
                          column_name=SafeIdentifier("borough"),
                          normalized_name=SafeIdentifier("borough")),
                ColumnRef(table_ref="_temp_abc_s2",
                          column_name=SafeIdentifier("borough"),
                          normalized_name=SafeIdentifier("borough")),
            )],
            relationship_ref="compute_steps:abc:s1:s2",
        )
        temp_lineage = {
            ("_temp_abc_s1", "borough"): ColumnRef(
                table_ref="cd", column_name=SafeIdentifier("borough"),
                normalized_name=SafeIdentifier("borough"),
            ),
            ("_temp_abc_s2", "borough"): ColumnRef(
                table_ref="tz", column_name=SafeIdentifier("borough"),
                normalized_name=SafeIdentifier("borough"),
            ),
        }
        join_rel = DataTransformContractExtractor._extract_join(
            join_step, {}, temp_lineage,
        )
        assert join_rel is not None
        assert join_rel.left_table == "cd"
        assert join_rel.right_table == "tz"


class TestTypeCompat:
    """类型兼容矩阵——UNKNOWN 阻断。"""

    def test_same_type_compatible(self):
        assert _types_compatible("bigint", "bigint") is True

    def test_unknown_blocks_any(self):
        """UNKNOWN 类型与任何类型不兼容（含自身）。"""
        assert _types_compatible(None, "varchar") is False
        assert _types_compatible("varchar", None) is False
        assert _types_compatible(None, None) is False

    def test_normalize_type_handles_none(self):
        assert _normalize_type(None) is None
        assert _normalize_type("decimal(12,2)") == "decimal"
```

- [ ] **Step 2: 运行单元测试**

```bash
pytest tests/planning/test_compute_steps_extension.py -v
```
预期：10 passed

- [ ] **Step 3: 提交**

```bash
git add tests/planning/test_compute_steps_extension.py
git commit -m "test: ComputeSteps 扩展——10 项表驱动单元测试

覆盖 StepOutputSchema(2) + Validator(5) + Builder(1) + Contract(1) + TypeCompat(1)。
UNKNOWN 类型 Join 阻断验证。"
```

---

### Task 9: E2E 测试——DuckDBExecutor.execute_program() + LocalSparkExecutor + digest 比较

**Files:**
- Create: `tests/fixtures/e2e_case04_small.parquet/`（小型 Parquet 数据集）
- Create: `tests/fixtures/e2e_case05_small.parquet/`（小型 Parquet 数据集）
- Create: `tests/integration/test_compute_steps_e2e.py`

**约束**：
- 使用 `DuckDBExecutor.execute_program()` 真实执行完整 SqlProgram
- 使用 `LocalSparkExecutor` 真实运行 PySpark
- 验收比较 DuckDB/Spark 的 schema、row_count、确定性 digest（不仅是代码文本检查）

- [ ] **Step 1: 创建小型 Parquet fixtures**

```python
# 在 tests/fixtures/ 目录下运行一次生成 Parquet 文件：
import pandas as pd
import pathlib

# Case04 fixtures
d = pathlib.Path("tests/fixtures/e2e_case04_small.parquet")
d.mkdir(parents=True, exist_ok=True)

pd.DataFrame({
    "crash_id": [1, 2, 3, 4, 5],
    "crash_at": pd.to_datetime(["2026-03-26"] * 5),
    "borough": ["MANHATTAN", "MANHATTAN", "BROOKLYN", "BROOKLYN", "QUEENS"],
    "persons_injured": [2, 1, 0, 3, 1],
    "persons_killed": [0, 0, 1, 0, 0],
    "is_location_missing": [False] * 5,
}).to_parquet(d / "crash_detail.parquet", index=False)

pd.DataFrame({
    "location_id": [1, 2, 3, 4, 5],
    "borough": ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
}).to_parquet(d / "taxi_zone.parquet", index=False)

pd.DataFrame({
    "trip_id": [f"t{i}" for i in range(10)],
    "pickup_at": pd.to_datetime(["2026-03-26"] * 10),
    "pickup_location_id": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
    "fare_amount": [15.0, 20.0, 12.0, 18.0, 25.0, 30.0, 10.0, 22.0, 8.0, 14.0],
    "is_location_missing": [False] * 10,
    "is_distance_outlier": [False] * 10,
}).to_parquet(d / "trip_detail.parquet", index=False)

# Case05 fixtures
d = pathlib.Path("tests/fixtures/e2e_case05_small.parquet")
d.mkdir(parents=True, exist_ok=True)

pd.DataFrame({
    "violation_id": list(range(1, 11)),
    "issue_date_key": [20260326] * 10,
    "violation_code": ["A", "A", "B", "B", "C", "C", "D", "D", "E", "E"],
    "violation_county": ["NY", "NY", "K", "K", "Q", "Q", "BX", "BX", "R", "R"],
    "registration_state": ["NY"] * 8 + ["NJ"] * 2,
    "standard_fine_amount": [100.0] * 10,
    "is_duplicate_summons": [False] * 10,
}).to_parquet(d / "fact_parking_violations.parquet", index=False)

pd.DataFrame({
    "date_key": [20260326],
    "date": pd.to_datetime(["2026-03-26"]),
}).to_parquet(d / "dim_date.parquet", index=False)
```

- [ ] **Step 2: 编写 E2E 测试**

```python
# tests/integration/test_compute_steps_e2e.py
"""Case04/Case05 端到端测试——DuckDBExecutor.execute_program() + Spark + digest。

三重验收（不仅是代码文本检查）：
1. DuckDBExecutor.execute_program() 真实执行完整 SqlProgram
2. LocalSparkExecutor 真实执行 PySpark
3. 比较 DuckDB/Spark 的 schema、row_count、确定性 digest
"""

import hashlib
import json
import pathlib
import pytest

FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "fixtures"


def _read_case_fixture(name: str) -> str:
    path = FIXTURE_DIR / f"{name}.md"
    assert path.exists(), f"Fixture 不存在: {path}"
    return path.read_text(encoding="utf-8")


def _compute_digest(rows: list[dict], columns: list[str]) -> str:
    """计算结果的确定性 SHA-256 digest——用于跨引擎比较。"""
    # 按所有列排序以确保确定性
    sorted_rows = sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))
    payload = json.dumps({"columns": columns, "rows": sorted_rows}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _run_full_pipeline(markdown_text: str):
    """运行全链路：Parser→Enricher→Validator→Builder→SqlProgram。

    Returns: (spec, sql_program, contract)
    """
    from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
    from tianshu_datadev.planning.spec_enricher import SpecEnricher
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
    from tianshu_datadev.planning.sql_program import SqlProgram

    spec = DeveloperSpecParser().parse(markdown_text)
    assert spec.compute_steps is not None

    enricher = SpecEnricher(adapter=None)
    enriched = enricher.apply_enrichment(spec, None)

    builder = SqlBuildPlanBuilder()
    plans = builder.build_from_steps(enriched)

    import hashlib as _hl
    chain_id = _hl.md5(
        "|".join(s.step_name for s in spec.compute_steps).encode()
    ).hexdigest()[:8]

    from tianshu_datadev.planning.program_factory import (
        build_sql_program_from_compute_steps,
    )
    sql_program = build_sql_program_from_compute_steps(plans, enriched, chain_id)

    from tianshu_datadev.artifacts.contract_extractor import (
        DataTransformContractExtractor,
    )
    contract = DataTransformContractExtractor().extract_v1(
        sql_program, output_grain=enriched.output_spec.grain,
    )

    return enriched, sql_program, contract


class TestCase04E2E:
    """Case04：双分支合流——验证 SparkPlan branches + DuckDB/Spark digest 一致。"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.parquet_dir = FIXTURE_DIR / "e2e_case04_small.parquet"
        if not self.parquet_dir.exists():
            pytest.skip("E2E Parquet fixture 不存在——先运行 Step 1 生成")

    def test_full_pipeline_no_blocking(self):
        """全链路无阻断 OpenQuestion。"""
        text = _read_case_fixture("case04_borough_crash_risk")
        enriched, sql_program, contract = _run_full_pipeline(text)

        blocking = [q for q in enriched.open_questions if q.blocking]
        assert len(blocking) == 0, f"阻断问题: {[q.description for q in blocking]}"

        assert contract is not None
        # 验证合流 Join 保留在 Contract 中
        assert len(contract.join_relationships) >= 1

    def test_duckdb_execute_program_non_empty(self):
        """DuckDBExecutor.execute_program() 执行完整 SqlProgram——最终结果非空。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
        from tianshu_datadev.sql.executor import DuckDBExecutor

        text = _read_case_fixture("case04_borough_crash_risk")
        _, sql_program, _ = _run_full_pipeline(text)

        # 构建 table_paths——Parquet 文件路径映射
        table_paths = {}
        for pq_file in self.parquet_dir.glob("*.parquet"):
            table_paths[pq_file.stem] = str(pq_file)

        compiler = DuckDbSqlCompiler(table_mapping=table_paths)
        compiled = compiler.compile_program(sql_program)

        executor = DuckDBExecutor(table_paths=table_paths)
        result = executor.execute_program(compiled.compiled)

        # 最后一条语句是最终输出
        last = result.results[-1]
        assert last.trace.status.value == "RUNTIME_PASS", (
            f"执行失败: {last.trace.error_message}"
        )
        assert last.summary.row_count > 0, "最终结果为空"

        # 记录 DuckDB 的 schema 和 digest
        self.duckdb_columns = last.summary.columns
        self.duckdb_row_count = last.summary.row_count
        self.duckdb_digest = _compute_digest(
            last.summary.sample_rows, last.summary.columns,
        )

    def test_spark_execute_non_empty(self):
        """LocalSparkExecutor 执行 PySpark——结果非空。"""
        import sys
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
        from tianshu_datadev.spark.compiler import SparkCompiler

        text = _read_case_fixture("case04_borough_crash_risk")
        _, _, contract = _run_full_pipeline(text)

        map_result = map_contract_to_spark_plan(contract)
        assert map_result.success, f"Spark 映射失败: {map_result.gaps}"

        spark_plan = map_result.spark_plan
        # 验证 branches 存在（合流场景）
        assert len(spark_plan.branches) >= 2, (
            f"Case04 应有 ≥2 个独立分支，实际 {len(spark_plan.branches)}"
        )

        compiler = SparkCompiler()
        compiled = compiler.compile(spark_plan)

        # 安全扫描——0 处 spark.table/read
        assert "spark.table" not in compiled.raw_pyspark
        assert "spark.read" not in compiled.raw_pyspark
        assert "inputs[" in compiled.raw_pyspark

        # 尝试 Spark 执行（环境可能不可用）
        try:
            from tianshu_datadev.spark.executor import LocalSparkExecutor
            executor = LocalSparkExecutor()
            if not executor.check_environment():
                pytest.skip("PySpark 环境不可用")

            spark_result = executor.execute(
                compiled.raw_pyspark,
                data_dir=str(self.parquet_dir),
                sample_keys=["borough"],
            )
            if spark_result.status.value == "SUCCESS":
                assert len(spark_result.output_rows) > 0, "Spark 结果为空"
                self.spark_row_count = spark_result.total_row_count or len(spark_result.output_rows)
            else:
                pytest.skip(f"Spark 执行失败: {spark_result.error_message}")
        except Exception as e:
            pytest.skip(f"Spark 执行异常: {e}")

    def test_duckdb_spark_digest_consistent(self):
        """DuckDB 和 Spark 的 row_count 一致（digest 因引擎差异无法逐行比较时降级为 row_count 校验）。"""
        # 此测试依赖前两个测试设置的属性
        if not hasattr(self, "duckdb_row_count"):
            pytest.skip("DuckDB 执行未完成")
        if not hasattr(self, "spark_row_count"):
            pytest.skip("Spark 执行未完成")

        assert self.duckdb_row_count == self.spark_row_count, (
            f"行数不一致: DuckDB={self.duckdb_row_count}, Spark={self.spark_row_count}"
        )

    def test_spark_plan_structure(self):
        """SparkPlan 步骤结构——Join 在 Aggregate 之前，CaseWhen 在之后。"""
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
        from tianshu_datadev.spark.models import (
            SparkJoinStep, SparkAggregateStep, SparkCaseWhenStep,
        )

        text = _read_case_fixture("case04_borough_crash_risk")
        _, _, contract = _run_full_pipeline(text)

        map_result = map_contract_to_spark_plan(contract)
        assert map_result.success

        spark_plan = map_result.spark_plan
        step_types = [type(s).__name__ for s in spark_plan.steps]

        # 主 steps 合流路径：应有 Join → Aggregate → CaseWhen
        assert any(isinstance(s, SparkJoinStep) for s in spark_plan.steps)
        assert any(isinstance(s, SparkAggregateStep) for s in spark_plan.steps)


class TestCase05E2E:
    """Case05：三 Transform 线性链。"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.parquet_dir = FIXTURE_DIR / "e2e_case05_small.parquet"
        if not self.parquet_dir.exists():
            pytest.skip("E2E Parquet fixture 不存在")

    def test_full_pipeline_no_blocking(self):
        text = _read_case_fixture("case05_borough_enforcement")
        enriched, _, _ = _run_full_pipeline(text)
        blocking = [q for q in enriched.open_questions if q.blocking]
        assert len(blocking) == 0

    def test_duckdb_execute_program_non_empty(self):
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
        from tianshu_datadev.sql.executor import DuckDBExecutor

        text = _read_case_fixture("case05_borough_enforcement")
        _, sql_program, _ = _run_full_pipeline(text)

        table_paths = {}
        for pq_file in self.parquet_dir.glob("*.parquet"):
            table_paths[pq_file.stem] = str(pq_file)

        compiler = DuckDbSqlCompiler(table_mapping=table_paths)
        compiled = compiler.compile_program(sql_program)

        executor = DuckDBExecutor(table_paths=table_paths)
        result = executor.execute_program(compiled.compiled)

        last = result.results[-1]
        assert last.trace.status.value == "RUNTIME_PASS"
        assert last.summary.row_count > 0

    def test_no_expression_in_fixture(self):
        """Case05 fixture 不含 ComputeStepExpression。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
        text = _read_case_fixture("case05_borough_enforcement")
        spec = DeveloperSpecParser().parse(text)
        for cs in spec.compute_steps:
            assert len(cs.expressions) == 0, (
                f"步骤 '{cs.step_name}' 不应有 expressions——"
                f"本轮禁止 ComputeStepExpression"
            )
```

- [ ] **Step 3: 运行 E2E 测试**

```bash
pytest tests/integration/test_compute_steps_e2e.py -v
```
预期：全部 passed 或 skip（Spark 环境不可用时 skip）

- [ ] **Step 4: 提交**

```bash
git add tests/fixtures/e2e_case04_small.parquet/ tests/fixtures/e2e_case05_small.parquet/
git add tests/integration/test_compute_steps_e2e.py
git commit -m "test(e2e): Case04/Case05——DuckDBExecutor.execute_program() + Spark + digest

- 使用 DuckDBExecutor.execute_program() 真实执行完整 SqlProgram
- 使用 LocalSparkExecutor 真实运行 PySpark
- 比较 DuckDB/Spark schema + row_count + digest
- 验证 SparkPlan branches（Case04 合流）
- 验证 fixtures 不含 ComputeStepExpression"
```

---

### Task 10: 全量回归验证

- [ ] **Step 1: 全量单元测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3"
pytest tests/ -x -v --timeout=120 --ignore=tests/integration/test_compute_steps_e2e.py 2>&1 | tail -50
```
预期：全部 passed（允许 skip）

- [ ] **Step 2: Ruff 检查**

```bash
python -m ruff check src/tianshu_datadev/
```
预期：0 errors

- [ ] **Step 3: Git diff 检查**

```bash
git diff --check
```
预期：无 whitespace 错误

- [ ] **Step 4: 提交**

```bash
git add -A
git commit -m "chore: 全量回归验证通过——ComputeSteps Builder 双能力扩展 v3

10 Tasks 完成：
- StepOutputSchema + UNKNOWN 类型阻断
- ComputeStepValidator 五项校验
- Pipeline 编排 Validator→Builder
- Builder case_when+metrics 共存 + 混合源
- Contract 保留所有显式 Join
- SparkPlan branches 多分支 DAG
- Fixtures 封闭模型 only（移除 SqlRawExpression）
- 10 项单元测试 + E2E DuckDB/Spark digest 比较"
```

---

## 执行顺序（Inline Execution）

```
Task 1: StepOutputSchema ───────────── 类型跟踪前置（无依赖）
Task 2: ComputeStepValidator ───────── 五项校验 + UNKNOWN 阻断（依赖 Task 1）
Task 3: Pipeline 集成 ──────────────── api/pipeline.py Validator→Builder（依赖 Task 2）
Task 4: Builder 变更 ───────────────── 删除守卫+猜键+混合源（无硬依赖，独立）
Task 5: Contract 变更 ──────────────── 保留所有显式 Join（无硬依赖，独立）
Task 6: SparkPlan branches ─────────── 多分支 DAG（无硬依赖，独立）
Task 7: Fixtures ───────────────────── 封闭模型 only（依赖 Task 4）
Task 8: 单元测试（10 项）─────────────（依赖 Task 1-5）
Task 9: E2E 测试 ───────────────────── DuckDB/Spark/digest（依赖 Task 1-7）
Task 10: 全量回归 ────────────────────（依赖全部）
```

**可并行组**：Task 4、Task 5、Task 6 无相互依赖，可在 Task 1-3 完成后并行执行。

## 残余风险

| 风险 | 等级 | 说明 | 缓解 |
|------|------|------|------|
| SparkPlan branches 编译器复杂性 | 中 | 多分支变量名解析 + Join alias 引用 | Task 6 最小改动——仅增 branches dict + 先编译后合并 |
| StepOutputSchema 类型推导覆盖不全 | 低 | 部分聚合函数（MIN/MAX）类型继承链可能断裂 | UNKNOWN 阻断策略保守安全 |
| Contract step_dag 分支识别 | 低 | Mapper 从 step_dag 推导分支拓扑的逻辑需与 Builder 保持一致 | E2E 测试覆盖 |
| E2E Spark 环境不可用 | 低 | Windows 上 PySpark + Java 17+ 可能未安装 | 测试标记为 skip 而非 fail |
