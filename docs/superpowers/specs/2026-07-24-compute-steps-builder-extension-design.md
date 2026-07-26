# ComputeSteps Builder 双能力扩展——C 类架构变更设计（修订版）

> **实施状态**：✅ 已实施完成（2026-07）。实际实施新增了 spec 中未包含的两跳桥接 JOIN，以 `docs/current-state-and-verification-status.md` 为准。
> 最后核对日期：2026-07-26

> **分类**：C 类（架构风险）
> **目标**：补齐 Builder 两个能力缺口——case_when + metrics 共存、上游步骤 + 物理表混合 Join——并修复两份多 Transform 案例的 YAML 格式和业务逻辑错误，使其通过 Parser→Validator→Builder→Contract→Spark 全链路验收。
> **关联 Spec**：case04（双 Transform 事故风险评估）、case05（三 Transform 违章执法归因）
> **修订**：2026-07-24，按 10 项收敛要求重写——删除 RawExpression 依赖、Validator 前置整合、Contract/Spark 派生 Join 修正、测试收敛。

---

## 一、职责分层架构（修订）

```
YAML 原始 dict
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 1: Parser                                         │
│                                                         │
│  校验范围：                                              │
│  • step_name 唯一性、source 引用存在性、无自引用/循环     │
│  • evaluation_phase 枚举值校验                           │
│  • else_value 按 key 存在性读取（不误吞 0/False/""）     │
│  • Join 结构：left_key/right_key 非空、right_table 存在   │
│  • ★ 混合源 joins 长度 ≤ 1（本轮硬限制）                 │
│  • 不校验：上游输出字段、SourceManifest 字段、类型、基数   │
│                                                         │
│  输出：ComputeStep（evaluation_phase 透传，None 合法）    │
└─────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 2: SpecEnricher                                   │
│                                                         │
│  对 evaluation_phase=None 的 CaseWhenDecl：               │
│  • 调用 _resolve_evaluation_phase(cw, spec)              │
│  • 判定成功 → 设置 phase                                 │
│  • 判定失败 → 生成 OpenQuestion(blocking=True)            │
│  • 已标记 pre/post 的规则保持不变                         │
│  • ★ 若表达式引用无法由封闭模型表达 → HUMAN_REVIEW        │
│    （不扩 AST，不引入 SqlRawExpression 新依赖）           │
└─────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 3: ComputeStepValidator（★ 新增——Builder 前门禁）  │
│                                                         │
│  输入：ComputeStep[] + step_outputs + SourceManifest     │
│  输出：BuildReadySteps | OpenQuestion[]                  │
│                                                         │
│  校验项（全部确定性，不调 LLM）：                          │
│  a. 符号解析——left_key ∈ step_outputs[source]，          │
│     right_key ∈ SourceManifest.tables[alias].columns     │
│  b. 类型兼容——left_key 与 right_key 字段类型兼容          │
│     （int↔bigint✅, varchar↔text✅, int↔varchar❌）       │
│  c. Join 基数安全——                                     │
│     • INNER JOIN：右表键无唯一性 → fan-out 阻断           │
│     • LEFT JOIN：右表键无唯一性 → 左表行膨胀阻断           │
│     • 两种 Join 分别定义 fan-out 规则，禁止复用            │
│  d. 显式 JoinDecl 门禁——合流步骤必须有 joins 声明，       │
│     禁止共同列猜键，禁止隐式 CROSS JOIN                   │
│  e. evaluation_phase 已确定——None → 阻断                  │
│  f. 派生表达式可表达性——引用的列都在上游输出中             │
│                                                         │
│  Builder 不接收 SourceManifest，不生成 OpenQuestion。      │
│  Builder 只对未验证输入防御性抛错（ValueError）。          │
└─────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 4: Builder                                        │
│                                                         │
│  • 拒绝 unresolved evaluation_phase → ValueError         │
│  • 按 phase 排序：pre_agg CaseWhen → Aggregate           │
│    → post_agg CaseWhen → Expression Projection           │
│  • 混合源：扫描上游 _temp_ + 扫描物理表 + Join + Agg      │
│  • 中间输出列完整收集                                     │
│  • ★ 不接收 SourceManifest，不调用 Validator              │
│  • ★ 不生成 OpenQuestion——无效输入直接 ValueError         │
└─────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 5: Contract / Spark（★ 修订重点）                   │
│                                                         │
│  • Contract 保留派生表 Join——temp↔temp 的 Join           │
│    若包含业务语义列（非流水线内部键），不得跳过             │
│  • Spark 物理表编译为 inputs["table_alias"]              │
│  • 禁止 spark.table() / spark.read()                    │
└─────────────────────────────────────────────────────────┘
```

---

## 二、Parser 变更

**文件**：`src/tianshu_datadev/developer_spec/parser.py`

### 2a. `_parse_case_when_raw`——evaluation_phase + else_value

```python
# 校验 evaluation_phase 枚举值
_phase = raw_cw.get("evaluation_phase")
if _phase is not None and _phase not in ("pre_aggregate", "post_aggregate"):
    raise ParseError(
        ParseErrorCode.E001_YAML_PARSE_FAILED,
        f"compute_step '{step_name}' 的 case_when.evaluation_phase "
        f"取值 '{_phase}' 非法——仅允许 pre_aggregate 或 post_aggregate",
    )

# else_value 按 key 存在性读取（不误吞 0/False/""）
_else = raw_cw.get("else_value")
if _else is None:
    _else = raw_cw.get("else_label")

return CaseWhenDecl(
    branches=branches,
    else_value=_else,
    output_column=raw_cw.get("output_column", ""),
    evaluation_phase=_phase,  # None 合法——留给 SpecEnricher 判定
)
```

### 2b. `_parse_compute_steps`——Join 校验 + 混合源 joins 长度限制

```python
if source_raw != "input" and step_joins:
    # ★ 混合源本轮最多一个上游 + 一个物理表
    if len(step_joins) > 1:
        raise ParseError(
            ParseErrorCode.E001_YAML_PARSE_FAILED,
            f"compute_step '{step_name}' 的 joins 长度为 {len(step_joins)}——"
            f"混合源本轮仅支持一个上游步骤 + 一个物理表 Join，"
            f"多跳链不在当前范围",
        )

    # Join key 非空校验
    for jd in step_joins:
        if not jd.left_key or not jd.right_key:
            raise ParseError(
                ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                f"compute_step '{step_name}' 的 Join 声明必须提供 "
                f"left_key 和 right_key——不支持隐式键推断",
            )

    # right_table 必须存在于 source_tables
    declared_aliases = {t.table_alias for t in input_tables}
    for jd in step_joins:
        if jd.right_table not in declared_aliases:
            raise ParseError(
                ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                f"compute_step '{step_name}' 的 Join 右表 "
                f"'{jd.right_table}' 不在 source_tables 声明中——"
                f"已声明表别名：{sorted(declared_aliases)}",
            )
```

### 2c. 合流步骤强制显式 JoinDecl

```python
# source 为列表（多源合流）时，必须有 spec.joins 中对应的显式声明
if isinstance(source_raw, list) and len(source_raw) > 1:
    # 检查 spec.joins 是否覆盖了每对合流源
    step_name_pairs = [
        (source_raw[i], source_raw[i + 1])
        for i in range(len(source_raw) - 1)
    ]
    declared_pairs = {
        (j.left_table, j.right_table) for j in spec_raw.get("joins", [])
    }
    for pair in step_name_pairs:
        if pair not in declared_pairs:
            raise ParseError(
                ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                f"合流步骤 '{step_name}' 的源对 {pair} 没有显式 JoinDecl——"
                f"禁止共同列猜键和隐式 CROSS JOIN。"
                f"请在 spec.joins 中声明 left_table='{pair[0]}', "
                f"right_table='{pair[1]}' 及对应的 left_key/right_key",
            )
```

### Parser 单元测试（表驱动，6 项）

| # | 测试场景 | 输入 | 预期 |
|---|---------|------|------|
| P1 | evaluation_phase 合法值 | `"pre_aggregate"` | 透传 |
| P2 | evaluation_phase 非法值 | `"invalid"` | ParseError(E001) |
| P3 | else_value=0（整数零） | `{"else_value": 0}` | `else_value="0"` |
| P4 | else_value 缺失，else_label 存在 | `{"else_label": "默认"}` | 回退到 else_label |
| P5 | 混合源 joins 长度 > 1 | `joins: [{...}, {...}]` | ParseError(E001) |
| P6 | 合流无显式 JoinDecl | `source: [a,b]`, joins 空 | ParseError(E002) |

---

## 三、SpecEnricher 变更

**文件**：`src/tianshu_datadev/planning/spec_enricher.py`

### 3a. compute_steps 路径 evaluation_phase 判定

与旧版相同——对 `spec.compute_steps` 中每个步骤的 `case_when` 调用 `_resolve_evaluation_phase_for_step`。

### 3b. 封闭表达式模型边界

```python
def _check_expression_references(
    self, cs: ComputeStep, step_outputs: dict[str, set[str]],
) -> list[str]:
    """检查 ComputeStep 的 expressions 引用的列是否全部可解析。

    封闭表达式模型仅支持引用：
    1. 同步骤的指标 alias
    2. 同步骤的 GROUP BY 列
    3. 上游步骤的 step_outputs 中的列

    若引用无法解析的列 → 产生 HUMAN_REVIEW，不在此轮扩展 AST。
    """
    unresolved: list[str] = []
    available = set()
    for m in cs.metrics:
        alias = m.alias or m.metric_name
        if alias:
            available.add(self._normalizer.normalize(alias))
    for gb in cs.group_by:
        available.add(self._normalizer.normalize(gb))
    if cs.source in step_outputs:
        available.update(step_outputs[cs.source])

    for expr in (cs.expressions or []):
        # 提取表达式中的标识符（简单启发式——不解析完整 AST）
        refs = _extract_identifiers(expr.expression)
        for ref in refs:
            if self._normalizer.normalize(ref) not in available:
                unresolved.append(
                    f"expression '{expr.name}' 引用 '{ref}'——"
                    f"不在同步骤指标/GROUP BY 或上游输出中"
                )
    return unresolved
```

### SpecEnricher 单元测试（表驱动，3 项）

| # | 测试场景 | 预期 |
|---|---------|------|
| E1 | CaseWhen 输出列在 group_by → pre_aggregate | phase="pre_aggregate" |
| E2 | CaseWhen 条件引用同步骤 metric → post_aggregate | phase="post_aggregate" |
| E3 | 表达式引用无法解析的列 → OpenQuestion | blocking=True |

---

## 四、ComputeStepValidator（★ 新增——Builder 前确定性门禁）

**文件**：`src/tianshu_datadev/planning/compute_step_validator.py`（新建）

### 4a. 调用位置与返回接口

**调用位置**：在 `sql_build_plan.py` 的 `build_from_steps` 入口，Builder 构建任何 Plan 之前：

```python
# 在 build_from_steps 中，遍历 ComputeStep 构建 Plan 之前：
from .compute_step_validator import ComputeStepValidator

_validator = ComputeStepValidator(normalizer=self._normalizer)

for cs in topo_sorted:
    # ... 构建 step_outputs 查找表 ...

    # ── 确定性门禁（在 Scan/Join/Agg 构建之前）──
    build_ready, errors = _validator.validate(
        cs=cs,
        step_outputs=step_outputs,
        manifest=manifest,
        input_tables=spec.input_tables,
        chain_id=chain_id,
    )
    if errors:
        open_questions.extend(errors)
        continue  # 跳过此步骤，不构建 Plan

    # ... 继续构建 Scan/Join/Agg ...
```

**返回接口**：

```python
@dataclass
class BuildReadyStep:
    """通过校验的步骤——Builder 可直接使用。"""
    cs: ComputeStep
    resolved_joins: list[ResolvedJoin]  # 已解析符号 + 已验证类型的 Join

@dataclass
class ResolvedJoin:
    """一个已验证的 Join——符号已解析，类型兼容，基数安全。"""
    join_decl: JoinDecl
    left_type: str   # 左键字段类型（来自 step_outputs 元数据）
    right_type: str  # 右键字段类型（来自 SourceManifest）
    types_compatible: bool
    right_key_unique: bool  # 右键在 SourceManifest 中有唯一性保证
    right_table_role: str   # "fact" | "dim"
```

### 4b. 五项确定性校验

```python
class ComputeStepValidator:
    """ComputeStep 确定性校验器——Builder 前门禁。

    所有校验是确定性的——相同输入永远产生相同输出。
    不调用 LLM、不访问数据库、不依赖外部状态。
    """

    def validate(
        self,
        cs: ComputeStep,
        step_outputs: dict[str, set[str]],
        manifest: SourceManifest | None,
        input_tables: list[InputTableDecl],
        chain_id: str,
    ) -> tuple[BuildReadyStep | None, list[OpenQuestion]]:
        """对单个 ComputeStep 执行全部校验。

        Returns:
            (BuildReadyStep, []) —— 通过全部校验
            (None, [OpenQuestion, ...]) —— 校验失败，每个错误一个 OpenQuestion
        """
        errors: list[OpenQuestion] = []

        # ── 校验 a：符号解析 ──
        resolved_joins = self._validate_join_symbols(cs, step_outputs, manifest, errors)

        # ── 校验 b：类型兼容 ──
        if resolved_joins:
            self._validate_type_compatibility(cs, resolved_joins, manifest, errors)

        # ── 校验 c：Join 基数安全 ──
        if resolved_joins:
            self._validate_join_cardinality(cs, resolved_joins, manifest, errors)

        # ── 校验 d：显式 JoinDecl 门禁 ──
        self._validate_explicit_join_decl(cs, errors)

        # ── 校验 e：evaluation_phase 已确定 ──
        if cs.case_when and cs.case_when.branches:
            if cs.case_when.evaluation_phase is None:
                errors.append(OpenQuestion(
                    question_id=f"eval_phase:{cs.step_name}",
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.case_when.evaluation_phase",
                    description=(
                        f"compute_step '{cs.step_name}' 的 case_when "
                        f"evaluation_phase 未确定——SpecEnricher 未成功判定"
                    ),
                    blocking=True,
                ))

        if errors:
            return None, errors

        return BuildReadyStep(cs=cs, resolved_joins=resolved_joins or []), []
```

### 4c. 类型兼容判定（使用 SourceManifest 真实字段类型）

```python
# 类型兼容矩阵——来自 SourceManifest 声明的字段类型
_TYPE_COMPAT_MATRIX: dict[tuple[str, str], bool] = {
    ("bigint", "integer"): True,
    ("integer", "bigint"): True,
    ("bigint", "bigint"): True,
    ("integer", "integer"): True,
    ("varchar", "varchar"): True,
    ("varchar", "text"): True,
    ("text", "varchar"): True,
    ("decimal", "double"): True,
    ("double", "decimal"): True,
    ("timestamp", "timestamp"): True,
    ("boolean", "boolean"): True,
    # 默认：不同类型 → False
}

def _check_types_compatible(self, left_type: str, right_type: str) -> bool:
    """检查两个字段类型是否兼容。"""
    left_norm = left_type.lower().split("(")[0].strip()  # decimal(12,2) → decimal
    right_norm = right_type.lower().split("(")[0].strip()
    if left_norm == right_norm:
        return True
    return _TYPE_COMPAT_MATRIX.get((left_norm, right_norm), False)
```

### 4d. LEFT vs INNER fan-out 规则（分别定义）

```python
def _validate_join_cardinality(
    self,
    cs: ComputeStep,
    resolved_joins: list[ResolvedJoin],
    manifest: SourceManifest,
    errors: list[OpenQuestion],
) -> None:
    """Join 基数安全——LEFT 与 INNER 分别定义 fan-out 规则。

    INNER JOIN：右表键不唯一时，左表匹配行被复制到每个右表匹配行
      → 若右表键无唯一性保证，阻断
    LEFT JOIN：右表键不唯一时，左表每行被复制到每个右表匹配行
      → 若右表键无唯一性保证，阻断（行膨胀）
    """
    for rj in resolved_joins:
        if rj.right_key_unique:
            continue  # 安全

        join_type = rj.join_decl.join_type.value.upper()
        if join_type == "INNER":
            errors.append(OpenQuestion(
                question_id=f"cardinality:{cs.step_name}:{rj.join_decl.right_table}",
                source="compute_step_validator",
                field_ref=f"compute_steps.{cs.step_name}.joins",
                description=(
                    f"INNER JOIN 右表 '{rj.join_decl.right_table}' 的键 "
                    f"'{rj.join_decl.right_key}' 无唯一性保证——"
                    f"左表匹配行将被复制，产生 fan-out 行膨胀。"
                    f"请在 source_tables 中为该列声明 unique: true"
                ),
                blocking=True,
            ))
        elif join_type == "LEFT":
            errors.append(OpenQuestion(
                question_id=f"cardinality:{cs.step_name}:{rj.join_decl.right_table}",
                source="compute_step_validator",
                field_ref=f"compute_steps.{cs.step_name}.joins",
                description=(
                    f"LEFT JOIN 右表 '{rj.join_decl.right_table}' 的键 "
                    f"'{rj.join_decl.right_key}' 无唯一性保证——"
                    f"左表行可能被膨胀。"
                    f"请在 source_tables 中为该列声明 unique: true"
                ),
                blocking=True,
            ))
```

### 4e. 禁止共同列猜键 + 隐式 CROSS JOIN

在 `_validate_explicit_join_decl` 中：

```python
def _validate_explicit_join_decl(
    self, cs: ComputeStep, errors: list[OpenQuestion],
) -> None:
    """合流步骤必须有显式 JoinDecl——禁止共同列猜键和隐式 CROSS JOIN。"""
    if isinstance(cs.source, list) and len(cs.source) > 1:
        if not cs.joins:
            errors.append(OpenQuestion(
                question_id=f"no_join_decl:{cs.step_name}",
                source="compute_step_validator",
                field_ref=f"compute_steps.{cs.step_name}.joins",
                description=(
                    f"合流步骤 '{cs.step_name}' 的 source 为列表 "
                    f"'{cs.source}'，但未声明 joins——"
                    f"所有 DAG 合流必须显式声明 JoinDecl，"
                    f"禁止共同列猜键和隐式 CROSS JOIN"
                ),
                blocking=True,
            ))
```

### ComputeStepValidator 单元测试（表驱动，6 项）

| # | 测试场景 | 输入 | 预期 |
|---|---------|------|------|
| V1 | left_key 在上游输出中存在，右表键唯一 | 合法混合源 | BuildReadyStep 非空 |
| V2 | left_key 不在上游输出中 | 不存在的列名 | OpenQuestion(blocking=True) |
| V3 | varchar↔bigint 类型不兼容 | left_type=varchar, right_type=bigint | OpenQuestion(blocking=True) |
| V4 | INNER JOIN 右表键无唯一性 | unique_keys=[] | OpenQuestion(blocking=True) |
| V5 | LEFT JOIN 右表键无唯一性 | unique_keys=[] | OpenQuestion(blocking=True) |
| V6 | 合流 source=[a,b]，joins 为空 | 无 JoinDecl | OpenQuestion(blocking=True) |

---

## 五、Builder 变更

**文件**：`src/tianshu_datadev/planning/sql_build_plan.py`

### 5a. 删除 `not cs.case_when` 守卫 + phase 排序

统一方法 `_build_case_when_and_aggregate_for_step`（与旧版设计相同），替换 line 709 的 `if cs.metrics and not cs.case_when:` 守卫。

### 5b. 删除 `_find_join_keys` 的共同列猜键和 CROSS JOIN 回退

```python
@staticmethod
def _find_join_keys(
    join_key_map: dict,
    sources: list[str],
    left_src: str,
    right_src: str,
    step_outputs: dict,
) -> tuple[str, str]:
    """查找两个源步骤之间的 Join 键。

    ★ 修订：仅从显式 JoinDecl 查找。删除共同列猜键和隐式 CROSS JOIN 回退。
    """
    key = (left_src, right_src)
    if key in join_key_map:
        jk = join_key_map[key]
        if jk[0] and jk[1]:  # 两个 key 都非空
            return jk
    # 无显式声明 → 调用方应在 Validator 阶段已被阻断
    raise ValueError(
        f"合流步骤的源对 ({left_src}, {right_src}) 无显式 JoinDecl——"
        f"应在 ComputeStepValidator 阶段被阻断"
    )
```

### 5c. 混合源单源路径扩展（限制单 Join）

在单源路径的 `_temp_` 扫描之后插入物理表 Join（与旧版设计相同，但增加 `len(cs.joins) == 1` 断言）：

```python
elif len(sources) == 1 and cs.joins:
    # 混合源：上游 _temp_ + 一个物理表
    assert len(cs.joins) == 1, (
        f"混合源步骤 '{cs.step_name}' 的 joins 长度不为 1——"
        f"Parser 应已拒绝"
    )
    jd = cs.joins[0]
    # ... 扫描右表 + JoinStep（与旧版 6c 相同）...
```

### 5d. Builder 不接收 SourceManifest

Builder 方法签名保持不变——不新增 `manifest` 参数。所有类型/基数/符号校验已在 `ComputeStepValidator` 中完成。Builder 仅对未验证状态做防御性断言：

```python
# 防御性检查（不应触发——Validator 应已拦截）
if cs.case_when and cs.case_when.evaluation_phase is None:
    raise ValueError(
        f"compute_step '{cs.step_name}' 的 case_when "
        f"evaluation_phase 未确定——ComputeStepValidator 应已阻断"
    )
```

### Builder 单元测试（表驱动，3 项）

| # | 测试场景 | 预期步骤顺序 |
|---|---------|------------|
| B1 | 单源 + metrics + post_agg CaseWhen | Scan(_temp_) → Aggregate → CaseWhen → Project |
| B2 | 单源 + metrics + pre_agg CaseWhen | Scan(_temp_) → CaseWhen → Aggregate(group_by 含 cw 列) |
| B3 | 混合源 + joins + metrics | Scan(_temp_) → Scan(物理表) → Join → Aggregate |

---

## 六、Contract 与 Spark——派生 Join 保留与 inputs 编译（★ 修订重点）

### 6a. 问题诊断：当前 Contract 跳过 temp↔temp Join

**文件**：`src/tianshu_datadev/artifacts/contract_extractor.py:742-751`

当前守卫：

```python
elif isinstance(step, JoinStep):
    if step.join_keys and any(
        k[0].table_ref.startswith("_temp_")
        and k[1].table_ref.startswith("_temp_")
        for k in step.join_keys
    ):
        continue  # ← 跳过两个 _temp_ 间的 Join
```

**问题**：Case04 T3 将 `borough_crash_stats`（_temp_）和 `borough_trip_stats`（_temp_）按 `borough` Join——这是**业务语义 Join**，不应被跳过。跳过会导致 Contract 丢失此 Join，Spark 无法正确恢复多步骤 DAG 的数据流。

**修正**：区分"流水线内部键"（_temp_ 名称本身、row_id 等）与"业务语义键"（borough、date 等有业务含义的列）。判断依据——Join key 是否出现在上游步骤的 `group_by` 或 `output_columns` 声明中。

```python
elif isinstance(step, JoinStep):
    # ★ 修订：仅当 Join key 是纯流水线内部键时才跳过
    # 业务语义键（borough、date 等）必须保留
    if step.join_keys and any(
        k[0].table_ref.startswith("_temp_")
        and k[1].table_ref.startswith("_temp_")
        for k in step.join_keys
    ):
        # 检查是否所有 join key 都是流水线内部键
        if self._is_internal_only_join(step, temp_column_lineage):
            continue  # 纯 DAG 编排 Join，跳过
        # 否则：业务语义 Join，保留

@staticmethod
def _is_internal_only_join(
    step: JoinStep,
    temp_column_lineage: dict[tuple[str, str], ColumnRef],
) -> bool:
    """判断 Join 是否仅为流水线内部编排（无业务语义）。

    流水线内部键特征：
    - 键名在 temp_column_lineage 中追溯到 row_id / _rownum 等系统列
    - 或键名以 _ 开头（内部约定）
    """
    for left_key, right_key in step.join_keys:
        left_resolved = DataTransformContractExtractor._resolve_column_lineage(
            left_key, temp_column_lineage
        )
        right_resolved = DataTransformContractExtractor._resolve_column_lineage(
            right_key, temp_column_lineage
        )
        # 若解析后的列名以 _ 开头 → 内部键
        left_name = str(left_resolved.column_name)
        right_name = str(right_resolved.column_name)
        if not (left_name.startswith("_") and right_name.startswith("_")):
            return False  # 至少有一对业务键
    return True
```

### 6b. Spark 编译——物理表必须编译为 `inputs["table_alias"]`

**当前代码**：`SparkCompiler._compile_read`（compiler.py:246-256）

```python
def _compile_read(self, resolved, step_id, index, total):
    step = resolved.step
    out_alias = resolved.output_var
    key_str = self.renderer.render_dict_key(step.source_name)
    source_alias = self.renderer.validate_identifier(step.alias, "ReadStep.alias")
    raw = f'{out_alias} = inputs[{key_str}].alias("{source_alias}")'
```

**验证点**：
- `step.source_name` = Contract `input_tables[].source_table` —— 如 `gold.fact_parking_violations`
- `step.alias` = Contract `input_tables[].table_ref` —— 如 `fv`
- `step.input_key` = Contract `input_tables[].table_ref` —— 如 `fv`

**确认**：物理表始终编译为 `inputs["gold.fact_parking_violations"].alias("fv")`——从不使用 `spark.table()` 或 `spark.read()`。Spark Executor 的安全扫描（executor.py:94-96）已硬编码拒绝 `spark.read` 和 `spark.table`。

**新增验收点**：Case04/Case05 生成的 PySpark 代码中，0 处出现 `spark.table` 或 `spark.read`，所有物理表引用均为 `inputs["..."]`。

### 6c. Spark Mapper 多步骤 DAG 的派生 Join 恢复

**链路**：

```
Contract.step_dag: {
    "stmt_zone_crash_stats": [],
    "stmt_zone_risk_assessment": ["stmt_zone_crash_stats"],
}

Contract.join_relationships: [
    ContractJoin(
        left_table="",        # ← _resolve_column_lineage 追溯到原始源表
        right_table="td",     # ← trip_detail 物理表
        left_key="borough",
        right_key="pickup_location_id",  # 通过 taxi_zone 间接关联
    )
]

↓ SparkMapper._map_joins()

SparkJoinStep(
    left_alias="",            # ← 空表示上游步骤输出
    right_alias="td",
    left_key="borough",
    right_key="pickup_location_id",
)

↓ SparkCompiler._compile_join()

# 生成：tN = t_left.join(t_td, ...)
```

**关键**：Contract 的 `_resolve_column_lineage` 将 `_temp_` 表引用解析为原始源表列引用。`left_table=""` 表示"来自上游步骤的派生列"——Spark 编译时由 `_chain_input_aliases` 解析为正确的前驱 DataFrame 变量名。

### 6d. Spark 验收标准（门禁）

生成的 PySpark 代码必须满足：

| 检查项 | 方法 | 预期 |
|--------|------|------|
| 0 处 `spark.table()` | `grep -c "spark\.table"` | 0 |
| 0 处 `spark.read()` | `grep -c "spark\.read"` | 0 |
| 所有读操作使用 `inputs["..."]` | `grep -c 'inputs\['` | ≥ input_tables 数量 |
| 派生 Join 保留 | Contract join_relationships 长度 | ≥ 合流步骤数 |

---

## 七、SqlProgram——无需变更

**文件**：`src/tianshu_datadev/planning/sql_program.py`

当前 `depends_on` 推导逻辑仅跟踪 `ComputeStep.source` 中的 step_name 引用。物理表依赖不出现在 `depends_on` 中——正确行为，无需变更。

---

## 八、案例数据边界与 Spec 重设计（★ 修订）

### 8a. 数据域关键事实

经 DuckDB 实测确认：

| 数据域 | 示例值 | 结论 |
|--------|--------|------|
| `violation_county` | `NY`, `K`, `BK`, `Q`, `Bronx`, `Kings`, `q`, `QNS` | 大小写混杂的 county 编码 |
| `taxi_zone.borough` | `Manhattan`, `Brooklyn`, `Queens` | 首字母大写的 borough 全名 |
| `crash_detail.borough` | `MANHATTAN`, `BROOKLYN` | 全大写 borough 名 |
| `trip_detail.pickup_location_id` | `188`, `242` | 整数，匹配 `taxi_zone.location_id` |

**核心事实**：三个 borough 相关字段分属三种不同值域，任意两者都不能直接等值 Join。

### 8b. Case04 修订：双 Transform——borough 粒度事故×行程风险评估

**关键约束**：
- ★ Case04 T3 必须在 Spec 内确定性统一 borough 值域——`crash_detail.borough` 全大写（`MANHATTAN`），`taxi_zone.borough` 首字母大写（`Manhattan`）——不统一则 T3 Join 零行
- 方案：T1 输出 borough 时通过 UPPER 归一化，T2 同样 UPPER 归一化，T3 Join 键统一为大写

**修订后拓扑**：

```
T1: crash_detail → borough_crash_stats
    源: source="input"（单表 crash_detail）
    GROUP BY UPPER(cd.borough) AS borough
    ← borough 归一化为全大写
    ┌─────────────────────────────────────────┐
    │ 过滤:                                    │
    │   cd.crash_at BETWEEN '2026-03-25'      │
    │                    AND '2026-03-31'      │
    │   cd.is_location_missing = FALSE         │
    │   cd.borough IS NOT NULL                 │
    │ metrics:                                │
    │   crash_count       COUNT(crash_id)     │
    │   total_injured     SUM(persons_injured)│
    │   total_killed      SUM(persons_killed) │
    │   ped_injured       SUM(pedestrians_    │
    │                     injured)            │
    │   cyclist_injured   SUM(cyclist_injured)│
    │ expressions:                            │
    │   severity_score    (total_injured*1.0  │
    │                     +total_killed*10.0) │
    │                     / crash_count       │
    │   ped_cyclist_injured                   │
    │     ped_injured + cyclist_injured       │
    └─────────────────────────────────────────┘
         ↓

T2: trip_detail + taxi_zone → borough_trip_stats
    源: source="input" + joins(taxi_zone)
    JOIN td.pickup_location_id = tz.location_id
    GROUP BY UPPER(tz.borough) AS borough
    ← borough 归一化为全大写（与 T1 对齐）
    ┌─────────────────────────────────────────┐
    │ 过滤:                                    │
    │   td.pickup_at BETWEEN '2026-03-25'     │
    │                    AND '2026-03-31'      │
    │   td.is_location_missing = FALSE         │
    │   td.is_distance_outlier = FALSE         │
    │ metrics:                                │
    │   total_trips        COUNT(trip_id)     │
    │   avg_fare           AVG(fare_amount)   │
    └─────────────────────────────────────────┘
         ↓                    ↓
         └────────┬───────────┘
                  ↓
T3: borough_crash_stats + borough_trip_stats → borough_risk_assessment
    源: source=[borough_crash_stats, borough_trip_stats]
    joins: [{left_table: borough_crash_stats, right_table: borough_trip_stats,
             left_key: borough, right_key: borough, join_type: INNER}]
    ← ★ 显式 JoinDecl——borough 已统一为大写，Join 安全
    group_by: [borough]
    ← 验证：case_when + metrics 共存
    ┌─────────────────────────────────────────┐
    │ metrics:                                │
    │   crash_count       SUM(T1.crash_count) │
    │   severity_score    SUM(T1.severity_...)│
    │   total_trips       SUM(T2.total_trips) │
    │ case_when(post_aggregate):              │
    │   output_column: risk_level             │
    │   evaluation_phase: post_aggregate      │
    │   branches:                             │
    │     when: severity_score>=2 AND         │
    │           total_trips>=10000            │
    │       then: "高危优先"                   │
    │     when: severity_score>=2             │
    │       then: "高事故低流量"               │
    │     when: total_trips>=10000            │
    │       then: "高流量低事故"               │
    │     when: severity_score>=1             │
    │       then: "中等监控"                   │
    │     else_value: "常规巡查"               │
    └─────────────────────────────────────────┘
```

**验收**：T3 产出 5 行（5 个 borough），`risk_level` 正确分类，非零行。

### 8c. Case05 修订：三 Transform——违章执法效能 borough 级归因

**关键约束**：
- ★ 删除未实现的 Rank 输出——不在 `output_columns` 和 T3 中声明任何 Window/Rank
- `violation_county` 与 `taxi_zone.borough` 值域不交 → 通过 pre_agg CaseWhen 映射
- `daily_out_state_count` 使用变体条件 FILTER `registration_state != 'NY'`

**修订后拓扑**：

```
T1: fact_parking_violations + dim_date → daily_violation_stats
    源: source="input" + joins(dim_date)
    JOIN fv.issue_date_key = dd.date_key
    GROUP BY fv.violation_county, fv.violation_code, dd.date
    粒度: county + code + date
    ┌─────────────────────────────────────────┐
    │ 过滤:                                    │
    │   dd.date BETWEEN '2026-03-25'          │
    │              AND '2026-03-31'            │
    │   fv.violation_county IS NOT NULL        │
    │   fv.violation_code IS NOT NULL          │
    │   fv.is_duplicate_summons = FALSE        │
    │ metrics:                                │
    │   daily_violation_count                  │
    │     COUNT(violation_id)                  │
    │   daily_fine_total                       │
    │     SUM(standard_fine_amount)            │
    │   daily_unique_plates                    │
    │     COUNT_DISTINCT(plate_id)             │
    │   daily_out_state_count                  │
    │     COUNT(violation_id)                  │
    │     FILTER registration_state != 'NY'    │
    └─────────────────────────────────────────┘
         ↓

T2: daily_violation_stats → borough_enforcement_score
    源: source=T1
    ← 验证：pre_agg CaseWhen + metrics + expressions 三者共存
    ┌─────────────────────────────────────────┐
    │ case_when(pre_aggregate):               │
    │   output_column: borough                │
    │   evaluation_phase: pre_aggregate       │
    │   branches:                             │
    │     when: violation_county IN           │
    │           ('NY','MN','NYC','MAN')        │
    │       then: "MANHATTAN"                 │
    │     ... (其余 borough 同理)              │
    │     else_value: ""                      │
    │                                         │
    │ GROUP BY borough（pre_agg 输出列自动加入）│
    │ 过滤: borough != ""                      │
    │ metrics:                                │
    │   total_violations                      │
    │     SUM(daily_violation_count)          │
    │   total_fine_amount                     │
    │     SUM(daily_fine_total)               │
    │   active_day_count                      │
    │     COUNT_DISTINCT(date)                │
    │   unique_violation_codes                │
    │     COUNT_DISTINCT(violation_code)      │
    │ expressions:                            │
    │   avg_daily_violations =                │
    │     total_violations / active_day_count │
    │   enforcement_score =                   │
    │     (total_violations * 0.3             │
    │      + total_fine_amount * 0.0001       │
    │      + unique_violation_codes * 50) / 3 │
    └─────────────────────────────────────────┘
         ↓

T3: borough_enforcement_score → enforcement_label
    源: source=T2
    粒度: borough（透传）
    ★ 无 Window/Rank——删除标题声称的"排名"
    ┌─────────────────────────────────────────┐
    │ group_by: [borough]                     │
    │ metrics: []  ← 无聚合，透传 T2 全部列     │
    │ case_when(post_aggregate):              │
    │   output_column: enforcement_level      │
    │   evaluation_phase: post_aggregate      │
    │   branches:                             │
    │     when: enforcement_score >= 10000    │
    │       then: "高能效"                     │
    │     when: enforcement_score >= 5000     │
    │       then: "中能效"                     │
    │     when: enforcement_score >= 2000     │
    │       then: "低能效"                     │
    │     else_value: "待提升"                 │
    └─────────────────────────────────────────┘
```

**验收**：T3 产出 5 行（5 个 borough），`enforcement_level` 正确分类，非零行。无 Rank 输出列。

### 8d. 案例 YAML 格式修正清单

| 错误 | 案例文件使用 | 正确字段 |
|------|-------------|---------|
| 表达式字段名 | `formula:` | `expression:` |
| CASE WHEN 条件字段 | `condition:` | `when:` |
| else_value 缩进 | 缩进在 `branches` 内 | 与 `branches` 同级 |

---

## 九、变更文件清单

| 文件 | 操作 | 变更范围 |
|------|------|---------|
| `src/tianshu_datadev/developer_spec/parser.py` | 修改 | evaluation_phase + else_value + Join 校验 + 合流强制 JoinDecl + 混合源 joins 长度=1 |
| `src/tianshu_datadev/planning/spec_enricher.py` | 修改 | compute_steps 路径 evaluation_phase 判定 + 表达式引用可解析性检查 |
| `src/tianshu_datadev/planning/compute_step_validator.py` | **新建** | 五项确定性校验——符号解析、类型兼容、基数安全、显式 JoinDecl、evaluation_phase |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 修改 | 删除 `not cs.case_when` 守卫、删除共同列猜键/CROSS JOIN 回退、混合源扩展、Builder 不接收 SourceManifest |
| `src/tianshu_datadev/artifacts/contract_extractor.py` | 修改 | 派生表业务语义 Join 保留（不再全跳过 temp↔temp Join） |
| `src/tianshu_datadev/planning/relationship_validator.py` | 不修改 | 现有接口由 ComputeStepValidator 调用 |
| `src/tianshu_datadev/planning/sql_program.py` | 不修改 | depends_on 逻辑无需变更 |
| `D:\ProgramData\Datawarehouse\纽约市城市交通\案例\case04_*.md` | 重写 | 双 Transform + borough 归一化 |
| `D:\ProgramData\Datawarehouse\纽约市城市交通\案例\case05_*.md` | 重写 | 三 Transform + 删除 Rank TODO |
| `tests/planning/test_compute_steps_extension.py` | 新建 | ~15 项表驱动单元测试 |
| `tests/integration/test_case04_case05_e2e.py` | 新建 | 2 项端到端测试 |

---

## 十、测试矩阵（收敛后：15 + 2）

### 表驱动单元测试（~15 项）

| # | 层 | 测试场景 | 预期 |
|---|----|---------|------|
| 1 | Parser | evaluation_phase 合法/非法/未声明 | 透传/ParseError/None |
| 2 | Parser | else_value key-based 读取（0/""/缺失） | 正确值 |
| 3 | Parser | 混合源 joins 长度 > 1 | ParseError |
| 4 | Parser | 合流无显式 JoinDecl | ParseError |
| 5 | Enricher | CaseWhen 输出在 group_by → pre_aggregate | phase 正确 |
| 6 | Enricher | CaseWhen 条件引用 metric → post_aggregate | phase 正确 |
| 7 | Enricher | 表达式引用无法解析的列 | OpenQuestion |
| 8 | Validator | left_key 在上游输出中存在 | BuildReadyStep 非空 |
| 9 | Validator | left_key 不存在 | OpenQuestion(blocking) |
| 10 | Validator | 类型不兼容（varchar↔bigint） | OpenQuestion(blocking) |
| 11 | Validator | INNER JOIN 右表键无唯一性 | OpenQuestion(blocking) |
| 12 | Validator | LEFT JOIN 右表键无唯一性 | OpenQuestion(blocking) |
| 13 | Validator | 合流无显式 JoinDecl | OpenQuestion(blocking) |
| 14 | Builder | case_when + metrics 共存（删除守卫后） | AggregateStep 构建 |
| 15 | Builder | pre_agg CaseWhen 输出列进入 group_by | group_keys 含 cw 列 |

### 端到端集成测试（2 项）

| # | 案例 | 链路 | 验证点 |
|---|------|------|--------|
| E2E1 | Case04 | YAML→Parser→Enricher→Validator→Builder→SQL→Contract→Spark | 5 行非空 + risk_level 正确 + 物理表编译为 inputs["..."] + 0 处 spark.table/read |
| E2E2 | Case05 | YAML→Parser→Enricher→Validator→Builder→SQL→Contract→Spark | 5 行非空 + enforcement_level 正确 + 派生 Join 保留 + 0 处 spark.table/read |

---

## 十一、实施顺序

```
Task 1: Parser 变更（evaluation_phase + else_value + Join 校验 + 合流强制 JoinDecl）
Task 2: SpecEnricher compute_steps 路径（evaluation_phase + 表达式引用检查）
Task 3: ComputeStepValidator（新建——五项确定性校验，Builder 前门禁）
Task 4: Builder 变更（删除守卫 + 删除猜键/CROSS JOIN + 混合源 + 防御性断言）
Task 5: Contract 派生 Join 保留（temp↔temp 业务语义 Join 不跳过）
Task 6: 案例文件重写（Case04 + Case05——borough 归一化 + 删除 Rank + YAML 修正）
Task 7: 表驱动单元测试（~15 项）
Task 8: 端到端测试（Case04 + Case05 SQL→Contract→Spark）
Task 9: 全量回归验证（pytest + ruff）
```

---

## 十二、残余风险

| 风险 | 等级 | 说明 | 缓解 |
|------|------|------|------|
| Pass-through 步骤（T3 无 metrics）| 中 | 不自动携带上游全部列，仅输出 group_by + case_when 列 | 案例 T3 设计为子集输出 |
| Contract 派生 Join 保留的边界 | 低 | `_is_internal_only_join` 的启发式可能漏判 | 两个 e2e 测试覆盖 |
| 多跳 Join 链 + 混合源 | 低 | 当前仅支持单 Join（joins 长度=1） | Parser 硬限制 |
| 三表及以上 JOIN | 中 | 当前 SqlBuildPlan 仅支持到两表 | 本次不处理 |
| Window/RANK | 低 | 不在本次范围 | 案例已删除 Rank 输出 |
| Spark Mapper 空 table_ref | 低 | 血缘解析后 left_table 可能为空 | e2e 测试覆盖 |

---

## 十三、全局约束

- 不修改 Contract / SQL Compiler / Spark Compiler 的核心编译逻辑——除非回归测试证明存在独立缺陷
- 不新增自由字符串 SQL/PySpark，所有条件通过 LabelPredicateCondition AST
- 不削弱现有安全门禁（Validator 7 项检查 + Promotion 双空门禁）
- 禁止 Compiler 猜测或绕过 Validator
- 所有注释使用中文
- THEN/ELSE 输出真实值，禁止携带 SQL 引号
- **Builder 不接收 SourceManifest，不生成 OpenQuestion**——所有校验前移到 ComputeStepValidator
- **派生表达式复用现有封闭模型，不扩 AST，无法表达→HUMAN_REVIEW**
- **物理表编译为 inputs["table_alias"]，禁止 spark.table/read**
- **所有 DAG 合流必须有显式 JoinDecl，禁止猜键和隐式 CROSS JOIN**
- **混合源本轮最多一个上游步骤 + 一个物理表**
