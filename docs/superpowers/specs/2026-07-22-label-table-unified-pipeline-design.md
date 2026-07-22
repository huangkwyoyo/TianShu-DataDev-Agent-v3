# Label Table 统一管线——设计方案

> **状态**：修订中（第三版——基于现有代码事实校准）
> **日期**：2026-07-22
> **触发**：label_table v1 单表门禁拒绝三表 JOIN 场景——`label_table v1 仅支持单表——当前 spec 包含 3 张表: ['fc', 'dd', 'tz']`

---

## 1. 目标与范围

### 1.1 目标

将 label_table 从独立 Planner 分支统一到标准管线中。label_table 应只是输出类型和附加验证策略，不应成为单独的 Planner/Enricher 分支。所有 dataset_type（detail_table、aggregate_table、label_table）通过同一管线。

### 1.2 范围

| 包含 | 排除 |
|------|------|
| 删除 v1 单表/非聚合门禁（`label_scope.py`） | MODE 聚合支持（独立变更） |
| 统一管线：所有 dataset_type 走同一 Planner/Enricher | RIGHT JOIN 反向 Join 语义（独立变更） |
| `UncertaintyEntry` 增加 `output_column` + `output_kind` | 新的中间数据模型 |
| Planner 删除 H7 + 修改 H5 + 新增 H9 | 跨 dataset_type 的通用 ComputeStep 重构 |
| RequirementPlanner 为 derived dimension/metric/case_when 唯一主要推断者 | **第二套 Join 排序或 IR——禁止重新实现** |
| LabelExtractor 仅兜底处理 Planner 标记 LABEL + unresolved + Planner 未生成 case_when_rules 的列 | |
| SpecEnricher 不复用同列重复生成规则 | |
| 同列规则冲突 → blocking OpenQuestion（禁止 logger.error 后继续） | |
| 合法三表 JOIN+聚合+标签全管线成功——**本轮正式验收** | |
| 仅对真实暴露的 Contract、聚合、CaseWhen、SQL/Spark 映射缺陷做最小修复 | |

### 1.3 现有代码事实——多跳 Join 链已存在

**以下能力已在代码中实现，不需要"Phase B 新增"**：

| 组件 | 位置 | 实际行为 |
|------|------|----------|
| `SqlBuildPlan.build_multi()` | `sql_build_plan.py:1304` | 多表多 Join 场景——`_sort_candidates_to_chain()` 排序候选 → 每对候选构建独立 `SqlBuildPlan` → `_temp` 表串联 |
| `_sort_candidates_to_chain()` | `sql_build_plan.py:2510` | 贪心排序候选为线性链——按 left_table→right_table 链接关系排列。链断裂或菱形分支的残留候选附在尾部，由 Validator V-009b 拒绝 |
| `_build_chain_step()` | `sql_build_plan.py:2535` | 构建链中单个步骤——Scan(L)+Scan(R)+Join，仅最终步骤含 Aggregate+Project+Sort+Limit |
| `build_sql_program_from_chain()` | `program_factory.py:28` | 多 Plan 线性链 → SqlProgram——委托至 `SqlProgramBuilder.build_chain()` |
| `SqlBuildPlanValidator.validate_multi_hop_chain()` | `sql/validator.py:148` | V-009b 循环检测 + V-009c 深度上限 ≤ 5——已在 Pipeline 三处调用点执行 |
| Pipeline 多跳路径 | `pipeline.py:1328-1342`, `1500-1506`, `2052-2069` | `len(hypothesis.candidates) > 1` 时自动走 `build_multi()` → `build_sql_program_from_chain()` → `validate_multi_hop_chain()` |

**结论**：当前唯一阻止三表 label_table 的是 `validate_label_table_v1_scope()` 中的单表门禁（`label_scope.py:39-43`）。删除此门禁后，现有多跳链应自动处理 N 表 JOIN。本轮工作验证此假设，仅在发现真实缺陷时做最小修复。

### 1.4 Join 安全边界（利用现有门禁，不新增）

| 边界 | 执行者 | 机制 |
|------|--------|------|
| 断开图（disconnected graph） | `_sort_candidates_to_chain()` → `validate_multi_hop_chain()` V-009b | 贪心链排序残留候选 → V-009b 菱形拒绝 |
| LEFT JOIN 方向 | `RelationshipPlanner` + `CrossValidator` | LEFT JOIN 右表必须唯一——交换左右方向导致语义错误，确定性阻断 |
| 多对多 / 缺少唯一性证据 | `RelationshipPlanner` / `CrossValidator` | 多对多 cardinality → 阻断 OpenQuestion |
| 同级候选排序 | `_sort_candidates_to_chain()` | 仅在语义等价时按 `candidate_id` 字典序确定性排序——不等价的同级候选形成菱形，由 V-009b 阻断 |

### 1.5 关键约束

- label_table 不新增 dataset_type
- 不删除现有安全验证（Validator 七项检查全部保留）
- Planner 的 `output_kind` 仅负责路由和诊断，不能决定验证通过——CASE AST、字段、聚合、Join 和 Builder 能力仍由确定性组件门禁
- **禁止重新实现第二套 Join 排序或 IR**——优先复用并修正现有多跳链
- 保持最小范围——不新增模型和架构层

---

## 2. 架构概览

### 2.1 统一管线执行顺序

```text
Parser → SourceManifest
  → RequirementPlanner          ← 所有类型统一（删除 label_table 跳过）
  → SpecEnricher                ← 所有类型统一（删除 label_table 跳过）
  → _prepare_labels             ← 合并候选 + Extractor + Validator + Promotion
  → 唯一一次 unresolved 检查     ← 移到这里，LabelExtractor 有机会兜底
  → RelationshipPlanner         ← 多表时自动走现有多跳链
  → CrossValidator
  → SqlBuildPlan                ← 单表 build() / 多表 build_multi() → SqlProgram
```

### 2.2 标签规则责任链（3 层——简化后）

RequirementPlanner 是 derived dimension、metric、case_when 的**唯一主要推断者**。SpecEnricher 不得对 Planner 已覆盖列重复生成规则。LabelExtractor 仅兜底处理 Planner 明确标记为 LABEL、仍 unresolved、**且 Planner 未生成 case_when_rules** 的列。

| 层 | 组件 | 触发条件 | 产出类型 |
|----|------|----------|----------|
| 1 | 显式 `label_rules`（程序员手写 YAML） | spec.label_rules 非空 | `CaseWhenDecl` |
| 2 | `RequirementPlanner` → `case_when_rules` | 所有 dataset_type——Planner 从业务描述推断 | `CaseWhenRule` → Validator → `CaseWhenDecl` |
| 3 | `LabelExtractor`（最后兜底） | 仅 label_table + Planner 标记 output_kind=LABEL + unresolved + **Planner 未生成 case_when_rules** | `LabelRuleProposal` → LabelRuleValidator → Promotion → `CaseWhenDecl` |

**LabelExtractor 跳过条件**（任一满足即跳过）：

1. Planner 已生成该列的 `case_when_rules`（`output_column` 已存在于 `spec.case_when_rules`）
2. Planner 未标记为 `output_kind=LABEL`
3. 列已解析（不在 `_find_unresolved_derived_columns` 结果中）

**SpecEnricher 规则**：`inferred_case_when` 不得对已有 `case_when_rules` 或 `label_rules` 覆盖的 `output_column` 重复生成。Planner 结果优先——Enricher 仅补充 Planner 未涉及的列。

### 2.3 两条独立验证路径

```
路径 A: RequirementPlanner → CaseWhenRule
  → ProposalValidator
  → ProposalPromotion → spec.case_when_rules

路径 B: LabelExtractor → LabelRuleProposal
  → LabelRuleValidator（七项检查）
  → Promotion → CaseWhenDecl → spec.label_rules
```

两条路径按 `output_column` 做覆盖冲突检测。同列出现两条不同规则 → blocking OpenQuestion，不混用三种模型。

### 2.4 Planner output_kind 的职责边界

`output_kind` 仅负责**路由**（决定未解析列交给哪个下游组件）和**诊断**（进入 Review artifact 供人工审查）。它**不能**决定验证通过——以下门禁始终由确定性组件执行：

| 门禁 | 执行者 | 依据 |
|------|--------|------|
| CASE 条件 AST 合法性 | `ProposalValidator` / `LabelRuleValidator` | Predicate 树结构验证 |
| 字段存在性 | `LabelRuleValidator._check_field_exists` | SourceManifest |
| 聚合函数白名单 | `ProposalValidator`（Schema enum 约束） | COUNT\|SUM\|AVG\|MIN\|MAX\|COUNT_DISTINCT |
| JOIN 安全性 | `RelationshipPlanner` / `CrossValidator` | 多对多检测、LEFT JOIN 右表唯一性 |
| 多跳链 | `SqlBuildPlanValidator.validate_multi_hop_chain()` | V-009b 循环检测 + V-009c 深度上限 |

---

## 3. 数据模型变更

### 3.1 `UncertaintyEntry` 修正

```python
class UncertaintyEntry(StrictModel):
    """LLM 不确定项——分类 + 路由信息。

    output_column 是路由主键——管线仅读取此字段决定后续处理路径。
    field_ref 仅用于诊断日志，不得解析字符串猜测 output_column。
    缺少 output_column 时直接按 UNKNOWN 处理。
    """

    field_ref: str
    """诊断标识——可能是 "case_when_rules.parse_error.risk_level" 等路径形式，
    仅用于日志和 artifact 审查，不作为路由依据。"""

    output_column: str | None = None
    """路由主键——输出列名。Planner 必须填写此字段，管线据此匹配。
    为 None 时一律按 UNKNOWN 处理。"""

    output_kind: Literal["LABEL", "METRIC", "DERIVED_DIMENSION", "UNKNOWN"] = "UNKNOWN"
    """Planner 对该列业务性质的判断。分类规则（互斥——每列只能匹配一条）：

    LABEL:
      由条件分支产出的有限值分类。输出值取决于 WHEN/THEN 逻辑判断，
      而非对源字段的确定性函数变换。示例：risk_level（CASE WHEN score>...）、
      peak_type（CASE WHEN hour BETWEEN...）。
      关键特征：输出值不能仅通过一个确定性函数从单一源列推导。

    DERIVED_DIMENSION:
      对源字段做直接确定性变换（单输入→单输出，无分支）。
      示例：pickup_hour = HOUR(pickup_at)、pickup_date = DATE(pickup_at)。
      关键特征：一个输入列 + 一个确定性函数 → 一个输出值。

    METRIC:
      聚合或聚合后数值计算。依赖 GROUP BY 后的 COUNT/SUM/AVG 等。
      示例：avg_fare、total_trips、conversion_rate。

    UNKNOWN:
      无法判断——系统将阻断并请求人工裁决。"""

    description: str
    candidates: list[str] = Field(default_factory=list)
```

### 3.2 `ParsedDeveloperSpec` 新增字段

```python
class ParsedDeveloperSpec(StrictModel):
    # ... 现有字段 ...
    uncertainties: list[UncertaintyEntry] = Field(default_factory=list)
```

### 3.3 已有字段（不变）

`RequirementPlannerOutput.uncertainties` 和 `RequirementProposal.uncertainties` 均已有 `list[UncertaintyEntry]` 类型（models.py:625, 636），无需修改。

---

## 4. 管线重排

### 4.1 `_enrich_and_plan`——删除 label_table 跳过

```diff
- if (self._requirement_planner is not None
-         and spec.dataset_type != DatasetType.LABEL_TABLE):
+ if self._requirement_planner is not None:
      spec, planner_questions = self._run_requirement_planner(spec, manifest)
      ...

- if spec.dataset_type != DatasetType.LABEL_TABLE:
-     spec = self._spec_enricher.apply_enrichment(spec, manifest)
+ spec = self._spec_enricher.apply_enrichment(spec, manifest)
```

### 4.2 插入 `_prepare_labels`

在 Planner + Enricher 之后、unresolved 检查之前：

```python
# ── 标签规则处理——合并候选 + Extractor + Validator + Promotion
spec = self._prepare_labels(spec, manifest)

# ── 唯一一次 unresolved 检查
unresolved_after = _find_unresolved_derived_columns(spec)
if unresolved_after:
    raise UnresolvedDerivedColumnsError(...)
```

### 4.3 `_parse_and_enrich`——删除提前调用

```diff
  manifest = build_manifest_from_spec(spec)
- spec = self._prepare_spec_for_planning(spec)
```

### 4.4 `_prepare_labels`——完整实现（修正 I1）

```python
def _prepare_labels(
    self, spec: ParsedDeveloperSpec, manifest: SourceManifest,
) -> ParsedDeveloperSpec:
    """统一标签规则处理——在 Planner/Enricher 之后执行。

    两条独立路径：
    A) case_when_rules → ProposalValidator → spec.case_when_rules（已在 Planner 中处理）
    B) LabelExtractor Proposal → LabelRuleValidator → Promotion → spec.label_rules

    LabelExtractor 仅在以下条件全部满足时调用（修正 I1）：
    1. dataset_type == LABEL_TABLE
    2. Planner 标记 output_kind=LABEL
    3. 列仍 unresolved
    4. Planner 未生成该列的 case_when_rules

    最后做覆盖冲突检查——同 output_column 两条规则 → blocking OpenQuestion。
    """
    # ── 路径 A：case_when_rules 已由 Planner 写入——无需额外处理

    # ── 路径 B：LabelExtractor fallback（严格兜底条件）
    if spec.dataset_type == DatasetType.LABEL_TABLE:
        unresolved = _find_unresolved_derived_columns(spec)
        if unresolved:
            # 获取 Planner 已覆盖的输出列集合
            planner_covered_cols = {r.output_column for r in spec.case_when_rules}
            planner_covered_cols.update(r.output_column for r in spec.label_rules)

            # 仅处理 Planner 明确标记为 LABEL、仍 unresolved、且 Planner 未生成规则的列
            label_candidates = [
                col for col in unresolved
                if _get_output_kind(col, spec.uncertainties) == "LABEL"
                and col not in planner_covered_cols
            ]
            if label_candidates:
                if self._label_extractor is None:
                    raise LabelTableConfigError(
                        "label_table 需要 LlmLabelExtractor，但未配置——"
                        "请设置 DEEPSEEK_API_KEY 环境变量"
                    )
                proposals, extraction_artifact = self._label_extractor.extract(
                    spec, label_candidates,
                )
                if proposals:
                    validator = LabelRuleValidator()
                    reports = [validator.validate(p, spec) for p in proposals]
                    promoter = Promotion()
                    promoted, promotion_artifact = promoter.promote(
                        spec.spec_hash, proposals, reports, extraction_artifact,
                    )
                    spec = spec.model_copy(update={
                        "label_rules": spec.label_rules + promoted,
                    })
                    request_id = self._gen_request_id(spec)
                    self._label_artifacts[request_id] = {
                        "extraction": extraction_artifact,
                        "promotion": promotion_artifact,
                    }

    # ── 覆盖冲突检查——同 output_column 两条规则 → blocking OpenQuestion
    conflict_questions = _check_label_rule_conflicts(spec)
    if conflict_questions:
        spec = spec.model_copy(update={
            "open_questions": spec.open_questions + conflict_questions,
        })

    # ── label_table 门禁：至少一个合法标签列（检查 label_rules 和 case_when_rules）
    if spec.dataset_type == DatasetType.LABEL_TABLE:
        has_labels = bool(spec.label_rules) or bool(spec.case_when_rules)
        if not has_labels:
            raise LabelTableConfigError(
                "label_table 至少需要一个合法标签列——"
                "label_rules 和 case_when_rules 均为空"
            )

    return spec
```

**I1 修正要点**：`label_candidates` 过滤新增 `col not in planner_covered_cols`——Planner 已生成 `case_when_rules` 时，LabelExtractor 不调用。Planner 只标记 LABEL 且仍未解析时，LabelExtractor 才兜底。

### 4.5 `_get_output_kind`——使用 output_column 路由

```python
def _get_output_kind(
    column_name: str,
    uncertainties: list[UncertaintyEntry],
) -> str:
    """查询 Planner 对未解析列的分类。默认返回 "UNKNOWN"。

    路由规则：
    1. 精确匹配 output_column（路由主键）
    2. output_column 为 None → 跳过
    3. 不解析 field_ref 字符串
    4. 无匹配 → "UNKNOWN"
    """
    for u in uncertainties:
        if u.output_column is not None and u.output_column == column_name:
            return u.output_kind
    return "UNKNOWN"
```

### 4.6 `_check_label_rule_conflicts`——同列冲突 → blocking OpenQuestion

禁止基于 `evaluation_phase` 猜测规则来源。同一 `output_column` 在 `label_rules` 和 `case_when_rules` 中同时存在 → 一律 blocking OpenQuestion。

```python
def _check_label_rule_conflicts(spec: ParsedDeveloperSpec) -> list[OpenQuestion]:
    """检查 label_rules 和 case_when_rules 的 output_column 冲突。

    同一 output_column 出现两条不同规则 → blocking OpenQuestion。
    不根据 evaluation_phase 猜测来源——每条冲突都必须人工裁决。
    """
    label_cols = {r.output_column for r in spec.label_rules}
    cw_cols = {r.output_column for r in spec.case_when_rules}
    overlap = label_cols & cw_cols

    if not overlap:
        return []

    return [
        OpenQuestion(
            question_id=f"LABEL_CONFLICT_{col}",
            source="label_conflict",
            field_ref=col,
            description=(
                f"输出列 '{col}' 在 label_rules 和 case_when_rules "
                f"中存在两条不同规则——需人工裁决保留哪一条"
            ),
            blocking=True,
        )
        for col in sorted(overlap)
    ]
```

### 4.7 uncertainties 透传——确定性合并，不整体覆盖

`ProposalPromotion.promote()` 和 `Pipeline._run_requirement_planner()` 中，uncertainties 写入 spec 时按 `(output_column, field_ref)` 组合键合并去重。新项覆盖同键旧项，保留其他旧项。不得整体覆盖已有诊断。

```python
def _merge_uncertainties(
    existing: list[UncertaintyEntry],
    incoming: list[UncertaintyEntry],
) -> list[UncertaintyEntry]:
    """确定性合并 uncertainties——按 (output_column, field_ref) 去重。

    新项覆盖同键旧项（Planner 最新输出优先），保留其他旧项。
    不整体覆盖——避免丢弃与其他组件写入的诊断信息。
    """
    if not incoming:
        return list(existing)

    merged: dict[tuple[str | None, str], UncertaintyEntry] = {}
    for u in existing:
        key = (u.output_column, u.field_ref)
        merged[key] = u
    for u in incoming:
        key = (u.output_column, u.field_ref)
        merged[key] = u  # 同键覆盖

    return list(merged.values())


def _apply_uncertainties_to_spec(
    spec: ParsedDeveloperSpec,
    uncertainties: list[UncertaintyEntry],
) -> ParsedDeveloperSpec:
    """确定性合并 Planner 分类结果到 spec。

    即使 Validator 失败也保留——artifact 审查需要分类证据。
    使用 _merge_uncertainties 而非整体覆盖。
    """
    if not uncertainties:
        return spec
    merged = _merge_uncertainties(spec.uncertainties, uncertainties)
    return spec.model_copy(update={"uncertainties": merged})
```

### 4.8 `_run_requirement_planner`——修正

```python
def _run_requirement_planner(
    self, spec: ParsedDeveloperSpec, manifest: SourceManifest,
) -> tuple[ParsedDeveloperSpec, list[OpenQuestion]]:
    # ... Planner 执行 + Proposal 构建不变 ...

    # CASE WHEN 解析失败仍阻断，但 uncertainties 不丢失
    case_when_parse_errors = _extract_case_when_parse_errors(planner_output)
    if case_when_parse_errors:
        spec = _apply_uncertainties_to_spec(spec, planner_output.uncertainties)
        return spec, case_when_parse_errors

    valid, questions = self._proposal_validator.validate(proposal, spec, manifest)
    if not valid:
        # Validator 失败时仅合并 uncertainties，不写入 dimensions/metrics/case_when_rules
        spec = _apply_uncertainties_to_spec(spec, proposal.uncertainties)
        return spec, questions

    spec = self._proposal_promotion.promote(proposal, spec)
    return spec, questions
```

### 4.9 SpecEnricher——不重复生成 Planner 已覆盖列

在 `apply_enrichment` 的 H11 `inferred_case_when` 合并阶段，新增去重检查：

```python
# SpecEnricher 不重复生成 Planner 已覆盖的列
planner_covered_cols = {r.output_column for r in spec.case_when_rules}
planner_covered_cols.update(r.output_column for r in spec.label_rules)

for cw in inferred_case_when:
    if cw.output_column in planner_covered_cols:
        # Planner 已生成该列规则——跳过，不重复
        continue
    # ... 正常合并 ...
```

---

## 5. Planner 集成

### 5.1 Prompt 变更

**删除 H7**：
```diff
- H7. label_table 类型不在你的职责范围——返回全空输出。
```

**修改 H5——增加 output_kind 和 output_column 指令**：
```diff
- H5. 不确定时写入 uncertainties。只写 field_ref + description + candidates。
-     不写 category——阻断级别由系统确定性规则决定。
+ H5. 不确定时写入 uncertainties。每条包含：
+     - field_ref: 诊断标识（自由文本，仅用于日志）
+     - output_column: 输出列名（路由主键——必须填写，否则按 UNKNOWN 处理）
+     - description: 为什么不确定
+     - candidates: 可能的解析方案（可为空列表）
+     - output_kind: 该列的业务性质——LABEL | METRIC | DERIVED_DIMENSION | UNKNOWN
+       判断依据（互斥——每列只能匹配一条）：
+       · LABEL: 条件分支产出的有限值分类（如 risk_level, peak_type, 安全等级）
+         特征：输出依赖 WHEN/THEN 逻辑，不能仅用确定性函数从单一源列推导
+       · METRIC: 聚合或聚合后数值计算（如 avg_xxx, total_xxx, rate）
+       · DERIVED_DIMENSION: 对源字段做直接确定性变换（如 HOUR(pickup_at) → pickup_hour）
+         特征：单输入 + 确定性函数 → 单输出，无分支
+       · UNKNOWN: 完全无法判断——系统将阻断并请求人工裁决
```

**新增 H9——白名单外聚合识别**：
```text
H9. 遇到白名单外聚合函数（如 MODE、MEDIAN、STDDEV 等）时：
    不要尝试构造 MetricDecl——Schema 会拒绝。
    输出一条 uncertainty：output_kind=METRIC，output_column=目标列名，
    description 说明"聚合函数 MODE 不在白名单 COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT 中"。
```

### 5.2 JSON Schema 变更

uncertainties items 增加 `output_column` 和 `output_kind`：

```json
"uncertainties": {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "field_ref": {"type": "string"},
            "output_column": {"type": ["string", "null"]},
            "output_kind": {
                "type": "string",
                "enum": ["LABEL", "METRIC", "DERIVED_DIMENSION", "UNKNOWN"]
            },
            "description": {"type": "string"},
            "candidates": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["field_ref", "output_column", "output_kind", "description"],
        "additionalProperties": false
    }
}
```

---

## 6. 删除清单

| # | 文件 | 删除内容 | 原因 |
|---|------|----------|------|
| 1 | `labels/label_scope.py` | 整个文件（`validate_label_table_v1_scope` 函数，lines 22-60） | 单表 + 非聚合门禁不再需要 |
| 2 | `api/pipeline.py` | `_prepare_spec_for_planning` 整个方法（lines 454-553） | 替换为 `_prepare_labels` |
| 3 | `api/pipeline.py:765` | `spec = self._prepare_spec_for_planning(spec)` 调用 | 执行位置从 Parser 后移到 Planner+Enricher 后 |
| 4 | `api/pipeline.py:648` | `if spec.dataset_type != DatasetType.LABEL_TABLE:` 跳过 RequirementPlanner | 所有类型统一执行 Planner |
| 5 | `api/pipeline.py:655` | `if spec.dataset_type != DatasetType.LABEL_TABLE:` 跳过 SpecEnricher | 所有类型统一执行 Enricher |
| 6 | `planning/requirement_planner.py:59` | `H7. label_table 类型不在你的职责范围——返回全空输出` | 所有类型统一由 Planner 覆盖 |
| 7 | `planning/sql_build_plan.py:2054-2065` | 重复的 label_table 作用域门禁 | 管线层已统一处理 |

**验证命令**：
```bash
grep -rn "validate_label_table_v1_scope\|_prepare_spec_for_planning\|不在你的职责范围" src/  # 应返回空
```

---

## 7. 保留清单

| # | 组件 | 说明 |
|---|------|------|
| 1 | `LabelRuleValidator` 七项检查 | FIELD_EXISTS / TYPE_COMPATIBLE / OPERATOR_VALID / AST_VALID / LABEL_DOMAIN / COVERAGE / NO_LABEL_NOT——全部不变 |
| 2 | `LabelExtractor.extract()` | 不变——输入从"所有未解析列"变为"仅 Planner 标记 LABEL + unresolved + Planner 未生成 case_when_rules" |
| 3 | `Promotion.promote()` | 不变——仅新增 uncertainties 透传 |
| 4 | `_find_unresolved_derived_columns` | 保留作为便捷包装——内部逻辑不变 |
| 5 | `_extract_case_when_parse_errors` | 保留——CASE 解析错误仍阻断，不交给 Extractor |
| 6 | extraction/promotion artifact 保存路径 | 迁移到 `_prepare_labels` 中，artifact 结构不变 |
| 7 | CASE 解析错误→阻断 | 不变——解析失败仍阻断，Extractor 不重猜 |
| 8 | `Contract` + `ContractExtractor` | 不变——`CaseWhenLabelSpec.evaluation_phase` 已完成 |
| 9 | `SqlBuildPlan._build_single_table` / `_build_multi_table` | 不变——pre/post aggregate CaseWhenStep 分流已完成 |
| 10 | `SparkMapper._map_case_when` | 不变——Spark 端分流已完成 |
| 11 | `SqlBuildPlan.build_multi()` + `_sort_candidates_to_chain()` + `_build_chain_step()` | 不变——现有多跳链机制完全保留 |
| 12 | `build_sql_program_from_chain()` + `SqlProgramBuilder.build_chain()` | 不变——多 Plan → SqlProgram 串联不变 |
| 13 | `SqlBuildPlanValidator.validate_multi_hop_chain()` | 不变——V-009b 循环检测 + V-009c 深度上限不变 |

---

## 8. 残余风险

| # | 风险 | 等级 | 触发条件 | 缓解措施 |
|---|------|------|----------|----------|
| R1 | LLM 未填写 `output_column` | 中 | Planner 输出 `output_column: null` | `_get_output_kind` 自动按 UNKNOWN 处理，不路由到 Extractor |
| R2 | LLM 分类错误（LABEL ↔ DERIVED_DIMENSION 混淆） | 中 | Planner 误判列的业务性质 | 分类证据写入 Review artifact；Extractor 不处理 DERIVED_DIMENSION 列，最终 unresolved 检查阻断 |
| R3 | Planner 首次处理 label_table 产出异常 | 低 | 删除 H7 后行为变化 | Fake Adapter 集成测试覆盖 |
| R4 | 非 label_table spec 意外触发 Extractor | 低 | dataset_type 判断遗漏 | `_prepare_labels` 入口处 dataset_type 门禁 |
| R5 | SpecEnricher 仍对 Planner 已覆盖列重复生成 | 低 | 去重检查遗漏 | Section 4.9 去重逻辑 + 集成测试覆盖 |
| R6 | 三表 Join 链 + CASE WHEN 暴露 Contract 或 Spark 映射缺陷 | 中 | 多跳链 + pre-aggregate CASE WHEN 组合此前未经 label_table 路径测试 | 三表 fixture 全链路物理验证覆盖——发现即修复，不新增架构层 |

---

## 9. 测试矩阵

### 9.1 文件 1：`tests/planning/test_uncertainty_routing.py`——表驱动单元测试

模型 + Schema + 路由 + 合并 + 失败时保留：

| # | 场景 | 输入 | 预期 |
|---|------|------|------|
| 1 | UncertaintyEntry 默认值 | `UncertaintyEntry(field_ref="x", description="...")` | `output_kind=="UNKNOWN"`, `output_column is None` |
| 2 | output_kind 枚举约束 | `output_kind="INVALID"` | Pydantic ValidationError |
| 3 | _get_output_kind 精确匹配 output_column | `u.output_column="risk_label"`, `u.output_kind="LABEL"` | 查询 `"risk_label"` → `"LABEL"` |
| 4 | _get_output_kind output_column=None 时跳过 | `u.output_column=None`, `u.output_kind="LABEL"` | 查询任意 → `"UNKNOWN"` |
| 5 | _get_output_kind 不解析 field_ref | `u.field_ref="case_when.parse_error.x"`, `u.output_column=None` | 查询 `"x"` → `"UNKNOWN"` |
| 6 | ProposalPromotion 透传 uncertainties | `proposal.uncertainties=[entry]` → promote | `result.uncertainties` 包含 entry |
| 7 | _merge_uncertainties 同键覆盖异键保留 | existing `[{oc:"a",fr:"x"}, {oc:"b",fr:"y"}]`，incoming `[{oc:"a",fr:"x"}]` | 合并后 `[{oc:"a",fr:"x"(新值)}, {oc:"b",fr:"y"(保留)}]`——共 2 条 |
| 8 | _apply_uncertainties_to_spec 空列表 | `uncertainties=[]` | 返回原 spec（不触发 model_copy） |
| 9 | JSON Schema uncertainties 含 output_column + output_kind | Schema 验证 | required 含 `output_column` 和 `output_kind` |
| 10 | CASE 解析失败 uncertainty 带 output_column 仍阻断 | `field_ref="case_when_rules.parse_error.peak_type"`, `output_column="peak_type"` | `_extract_case_when_parse_errors` 返回阻断 OpenQuestion（不交给 Extractor） |
| 11 | 同列 label_rules + case_when_rules 冲突 → blocking | `label_rules=[{output_column:"x"}]`, `case_when_rules=[{output_column:"x"}]` | `_check_label_rule_conflicts` 返回 1 个 blocking OpenQuestion |

### 9.2 文件 2：`tests/pipeline/test_label_table_unified.py`——统一管线集成测试

全部使用 Fake Adapter，不依赖真实 LLM。

**Phase A 验收**：

| # | 场景 | 预期 |
|---|------|------|
| I1 | 合法单表标签全管线成功——**Planner 生成 case_when_rules 时 LabelExtractor 不调用** | Planner 推断 dims/metrics/case_when_rules → Enricher 正常 → `_prepare_labels` 检测 Planner 已覆盖 → **LabelExtractor 未调用** → label_rules + case_when_rules 非空 |
| I2 | 合法两表 JOIN+聚合+标签全管线成功 | Parser → Planner → Enricher → Labels → Relationship → Builder → **成功** |
| I3 | **合法三表 JOIN+聚合+标签全管线成功——本轮正式验收** | 3 表、2 条 JoinCandidate → Parser → Planner → Relationship → **build_multi()** → **SqlProgram** → Validator → Contract → **SQL/Spark compile 成功** → **同一快照物理验证通过（DuckDB == Spark）** |
| I4 | 多对多或无唯一性证据的 Join 确定性阻断 | Parser → Planner → Enricher → Labels 通过 → Relationship/CrossValidator 检测多对多 → **阻断 OpenQuestion** |
| I5 | 不支持 MODE 聚合 → OpenQuestion | Planner 产出 `output_kind=METRIC` uncertainty → 不调用 Extractor → **阻断 OpenQuestion** |
| I6 | LABEL 才调用 Extractor——Planner 未生成规则时兜底 | `output_kind=LABEL` + unresolved + Planner 未生成 case_when_rules → Extractor 被调用；`output_kind=METRIC` → 跳过 |
| I7 | UNKNOWN 不调用 Extractor | `output_kind=UNKNOWN` → unresolved 检查阻断，Extractor 不执行 |
| I8 | 非 label_table 的 Planner CASE WHEN 正常工作 | detail_table → Planner 生成 case_when_rules → SpecEnricher 不重复覆盖 |
| I9 | SpecEnricher 不复用 Planner 已覆盖列 | spec.case_when_rules 已有 `peak_type` → Enricher H11 跳过 `peak_type` |
| I10 | SQL 与 Spark 从同一已验证 Contract 生成 | Contract 提取后 SQL 和 Spark 均成功编译——物理验证通过 |

**I3 合法三表 fixture**（本轮正式验收）：

```yaml
dataset_type: label_table
input_tables:
  - name: trips
    key_columns: [trip_id]
    columns: [trip_id, pickup_location_id, dropoff_location_id, pickup_at, fare_amount, passenger_count, trip_distance]
  - name: zones
    key_columns: [location_id]
    columns: [location_id, borough, zone_name]
  - name: weather
    key_columns: [weather_id]
    columns: [weather_id, pickup_date, weather_condition, temp_high, temp_low]
joins:
  - left: trips.pickup_location_id
    right: zones.location_id     # Join 1——唯一键
  - left: trips.pickup_date
    right: weather.pickup_date   # Join 2——唯一键
metrics:
  - name: 总行程数
    aggregation: COUNT
    alias: trip_count
  - name: 平均费用
    aggregation: AVG
    input_column: fare_amount
    alias: avg_fare
  - name: 平均距离
    aggregation: AVG
    input_column: trip_distance
    alias: avg_distance
dimensions:
  - dimension_name: borough
    column_ref: zones.borough
  - dimension_name: weather_condition
    column_ref: weather.weather_condition
output_spec:
  columns: [borough, weather_condition, trip_count, avg_fare, avg_distance, risk_label]
  grain: [borough, weather_condition]
label_rules: []
# 业务描述：按 weather_condition 和 avg_fare 定义 risk_label（高风险/中风险/低风险）
```

**I3 验收链完整路径**：

```text
Parser（三表 YAML 解析成功）
  → SourceManifest（三表 schema 合并成功）
  → RequirementPlanner（推断 dims/metrics/case_when_rules, output_kind=LABEL for risk_label）
  → SpecEnricher（补充上下文，不重复 Planner 已覆盖列）
  → _prepare_labels（Planner 已覆盖 risk_label → LabelExtractor 跳过）
  → unresolved 检查（全部解析 → 通过）
  → RelationshipPlanner（两条 JoinCandidate: trips↔zones, trips↔weather）
  → CrossValidator（1:1 + 唯一性证据 → 通过）
  → SqlBuildPlan.build_multi()（_sort_candidates_to_chain → 2 步链 → 2 SqlBuildPlan）
  → build_sql_program_from_chain()（_temp 串联 → SqlProgram）
  → SqlBuildPlanValidator.validate_multi_hop_chain()（V-009b 循环检测 + V-009c 深度 ≤ 5 → 通过）
  → ContractExtractor（提取 SQL + Spark Contract → CaseWhenLabelSpec 含 evaluation_phase）
  → SQL Compile（DuckDB 执行成功，返回 N 行）
  → Spark Compile（PySpark 执行成功，返回 N 行）
  → 物理验证（DuckDB == Spark——行数一致、schema 一致、值一致）
```

**I2 合法两表 fixture**：

```yaml
dataset_type: label_table
input_tables:
  - name: trips
    key_columns: [trip_id]
    columns: [trip_id, pickup_location_id, pickup_at, fare_amount, passenger_count]
  - name: zones
    key_columns: [location_id]
    columns: [location_id, borough]
joins:
  - left: trips.pickup_location_id
    right: zones.location_id     # 唯一 JOIN 键——1:1
metrics:
  - name: 总行程数
    aggregation: COUNT
    alias: trip_count
  - name: 平均费用
    aggregation: AVG
    input_column: fare_amount
    alias: avg_fare
dimensions:
  - dimension_name: borough
    column_ref: zones.borough
output_spec:
  columns: [borough, trip_count, avg_fare, risk_label]
  grain: [borough]
label_rules: []
```

---

## 10. 执行顺序

1. 数据模型变更：`UncertaintyEntry` 增加 `output_column` + `output_kind`；`ParsedDeveloperSpec` 增加 `uncertainties`
2. RequirementPlanner Prompt + JSON Schema 变更：删除 H7、修改 H5、新增 H9
3. 管线重排：删除 label_table 跳过 → 插入 `_prepare_labels` → 删除 `_prepare_spec_for_planning`
4. I1 修正：`_prepare_labels` 中 LabelExtractor 跳过条件——Planner 已生成 case_when_rules → 不调用
5. uncertainties 透传：`_merge_uncertainties` + `_apply_uncertainties_to_spec` + `ProposalPromotion` 集成
6. SpecEnricher 去重：Planner 已覆盖列不重复生成
7. `_check_label_rule_conflicts` 改为 blocking OpenQuestion
8. 删除：`label_scope.py` + `_prepare_spec_for_planning` + sql_build_plan 重复门禁
9. 三表 fixture 全链路验证：通过现有多跳链验证 JOIN+聚合+标签 → 发现真实缺陷即修复
10. 单元测试：`test_uncertainty_routing.py`（11 个用例）
11. 集成测试：`test_label_table_unified.py`（10 个用例——含三表全链路）
12. 全量回归验证：`pytest` + `ruff check` + `git diff --check`
