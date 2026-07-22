# Label Table 统一管线——设计方案

> **状态**：已确认，待进入 writing-plans
> **日期**：2026-07-22
> **触发**：label_table v1 单表门禁拒绝三表 JOIN 场景——`label_table v1 仅支持单表——当前 spec 包含 3 张表: ['fc', 'dd', 'tz']`

---

## 1. 目标与范围

### 1.1 目标

将 label_table 从独立 Planner 分支统一到标准管线中。label_table 应只是输出类型和附加验证策略，不应成为单独的 Planner/Enricher 分支。所有 dataset_type（detail_table、aggregate_table、label_table）通过同一管线。

将工作拆为两个连续最小阶段：

- **Phase A**（本次 spec）：统一 label_table 与标准 Planner/Validator 管线——删除 v1 门禁、所有类型共享 Planner/Enricher、LabelExtractor 仅兜底处理 Planner 标记为 LABEL 的列
- **Phase B**（后续 spec）：基于现有 `JoinStep`/`SqlPlan` DAG 支持确定性 N 表 Join 链——不再受两表上限限制

### 1.2 Phase A 范围

| 包含 | 排除 |
|------|------|
| 删除 v1 单表/非聚合门禁（`label_scope.py`） | N 表 Join 链（Phase B） |
| 统一管线：所有 dataset_type 走同一 Planner/Enricher | MODE 聚合支持（独立变更） |
| `UncertaintyEntry` 增加 `output_column` + `output_kind` | RIGHT JOIN 反向 Join 语义（独立变更） |
| Planner 删除 H7 + 修改 H5 + 新增 H9 | 新的中间数据模型 |
| RequirementsPlanner 为 derived dimension/metric/case_when 唯一主要推断者 | 跨 dataset_type 的通用 ComputeStep 重构 |
| LabelExtractor 仅处理 Planner 标记为 LABEL 且仍 unresolved 的列 | |
| SpecEnricher 不复用同列重复生成规则 | |
| 同列规则冲突 → blocking OpenQuestion（禁止 logger.error 后继续） | |

### 1.3 Phase B 预览（后续 spec）

当前 `_build_multi_table`（sql_build_plan.py:2323）仅使用 `hypothesis.candidates[0]`——只处理第一个 JoinCandidate。但 IR 层已具备 N 表能力：

- `SqlPlan.steps: list[StepNode]` 支持任意数量的 `ScanStep` + `JoinStep` 组合
- `JoinStep`（line 104-118）含 `right_table_ref`、`join_keys`、`relationship_ref`——每次调用一个
- `RelationshipHypothesis.candidates: list[JoinCandidate]` 可承载多条 Join 边
- `StepNode` Union（line 217-231）不需要新增类型

Phase B 仅需：
1. 从 `candidates` 确定性推导 Join 顺序（以事实表为锚、拓扑展开）
2. 遍历 candidates 构建 Scan → Join → Scan → Join → ... 链
3. 所有现有 Validator/CrossValidator 门禁对每个 JoinCandidate 单独执行

不新增自由 SQL、ComputeStep 或另一套 IR——直接复用现有模型。

### 1.4 关键约束

- label_table 不新增 dataset_type
- 不删除现有安全验证（Validator 七项检查全部保留）
- Planner 的 `output_kind` 仅负责路由和诊断，不能决定验证通过——CASE AST、字段、聚合、Join 和 Builder 能力仍由确定性组件门禁
- 保持最小范围——Phase A 不新增三表 DAG、MODE、RIGHT JOIN 或新的中间模型

---

## 2. 架构概览

### 2.1 统一管线执行顺序

```text
Parser → SourceManifest
  → RequirementPlanner          ← 所有类型统一（删除 label_table 跳过）
  → SpecEnricher                ← 所有类型统一（删除 label_table 跳过）
  → _prepare_labels             ← 合并候选 + Extractor + Validator + Promotion
  → 唯一一次 unresolved 检查     ← 移到这里，LabelExtractor 有机会兜底
  → RelationshipPlanner
  → CrossValidator
  → SqlBuildPlan
```

### 2.2 标签规则责任链（3 层——简化后）

RequirementPlanner 是 derived dimension、metric、case_when 的**唯一主要推断者**。SpecEnricher 不得对 Planner 已覆盖列重复生成规则。LabelExtractor 仅兜底处理 Planner 明确标记为 LABEL 且仍 unresolved 的列。

| 层 | 组件 | 触发条件 | 产出类型 |
|----|------|----------|----------|
| 1 | 显式 `label_rules`（程序员手写 YAML） | spec.label_rules 非空 | `CaseWhenDecl` |
| 2 | `RequirementPlanner` → `case_when_rules` | 所有 dataset_type——Planner 从业务描述推断 | `CaseWhenRule` → Validator → `CaseWhenDecl` |
| 3 | `LabelExtractor`（最后兜底） | 仅 label_table + Planner 标记 output_kind=LABEL + 前 2 层未覆盖 | `LabelRuleProposal` → LabelRuleValidator → Promotion → `CaseWhenDecl` |

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
| Builder 能力（Phase A） | `SqlBuildPlan` | 两表上限（Phase B 解除） |

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

### 4.4 `_prepare_labels`——完整实现

```python
def _prepare_labels(
    self, spec: ParsedDeveloperSpec, manifest: SourceManifest,
) -> ParsedDeveloperSpec:
    """统一标签规则处理——在 Planner/Enricher 之后执行。

    两条独立路径：
    A) case_when_rules → ProposalValidator → spec.case_when_rules（已在 Planner 中处理）
    B) LabelExtractor Proposal → LabelRuleValidator → Promotion → spec.label_rules

    最后做覆盖冲突检查——同 output_column 两条规则 → blocking OpenQuestion。
    """
    # ── 路径 A：case_when_rules 已由 Planner 写入——无需额外处理

    # ── 路径 B：LabelExtractor fallback（仅 label_table + 仅 LABEL 分类列）
    if spec.dataset_type == DatasetType.LABEL_TABLE:
        unresolved = _find_unresolved_derived_columns(spec)
        if unresolved:
            # 仅处理 Planner 明确标记为 LABEL 且 output_column 有效的列
            label_candidates = [
                col for col in unresolved
                if _get_output_kind(col, spec.uncertainties) == "LABEL"
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
        # 冲突阻断——不抛异常，让后续 open_questions 检查统一处理

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

    # 以 (output_column, field_ref) 为键构建映射——新项覆盖旧项
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
# planner_covered_cols 更新为 Planner 已覆盖的列集合

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

## 6. Phase B：N 表 Join 链设计预览

### 6.1 现有模型充分性

Phase B 不需要新增 IR 类型。现有模型已具备 N 表能力：

| 模型 | 位置 | N 表能力 |
|------|------|----------|
| `StepNode` Union | sql_build_plan.py:217-231 | 支持任意数量的 `ScanStep`、`JoinStep` 组合 |
| `JoinStep` | sql_build_plan.py:104-118 | `right_table_ref` + `join_keys` + `relationship_ref`——一次 Join |
| `SqlPlan.steps` | sql_build_plan.py:280 | `list[StepNode]`——无数量限制 |
| `RelationshipHypothesis.candidates` | relationship_hypothesis.py:144 | `list[JoinCandidate]`——可承载 N-1 条边 |

当前仅 `_build_multi_table`（line 2346）使用 `candidates[0]` 导致两表硬限制。

### 6.2 改动点

1. **Join 顺序推导**：从 `candidates` 中确定 Join 链顺序——以事实表为锚点，按 Join 键连通性拓扑展开，同级按 `candidate_id` 字典序打破平局
2. **Scan → Join 链构建**：对每个 candidate 追加 `ScanStep(right_table)` → `JoinStep(chain, right_table)` 到 steps 列表
3. **门禁不变**：每个 `JoinCandidate` 独立走 `CrossValidator`——多对多或无唯一性证据的 Join 被确定性阻断
4. **聚合/标签不变**：Join 链完成后，pre-aggregate CaseWhenSteps → AggregateStep → post-aggregate CaseWhenSteps 逻辑不变

### 6.3 确定性 Join 顺序算法

```
输入: primary_table（事实表）, candidates（JoinCandidate 列表）
输出: 有序 Join 步骤列表

1. joined = {primary_table}
2. ordered = []
3. remaining = sorted(candidates, key=lambda c: c.candidate_id)
4. while remaining:
      for each c in remaining:
          if c.left_table in joined and c.right_table not in joined:
              ordered.append(c)
              joined.add(c.right_table)
              remaining.remove(c)
              break
          elif c.right_table in joined and c.left_table not in joined:
              ordered.append(c)  # 交换左右
              joined.add(c.left_table)
              remaining.remove(c)
              break
      else:
          # 无 progress——剩余候选形成孤立子图
          # 选取剩余中第一个未连接表作为新锚点
          ...
5. return ordered
```

---

## 7. 删除清单

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

## 8. 保留清单

| # | 组件 | 说明 |
|---|------|------|
| 1 | `LabelRuleValidator` 七项检查 | FIELD_EXISTS / TYPE_COMPATIBLE / OPERATOR_VALID / AST_VALID / LABEL_DOMAIN / COVERAGE / NO_LABEL_NOT——全部不变 |
| 2 | `LabelExtractor.extract()` | 不变——输入从"所有未解析列"变为"仅 Planner 标记 LABEL + 仍 unresolved" |
| 3 | `Promotion.promote()` | 不变——仅新增 uncertainties 透传 |
| 4 | `_find_unresolved_derived_columns` | 保留作为便捷包装——内部逻辑不变 |
| 5 | `_extract_case_when_parse_errors` | 保留——CASE 解析错误仍阻断，不交给 Extractor |
| 6 | extraction/promotion artifact 保存路径 | 迁移到 `_prepare_labels` 中，artifact 结构不变 |
| 7 | CASE 解析错误→阻断 | 不变——解析失败仍阻断，Extractor 不重猜 |
| 8 | `Contract` + `ContractExtractor` | 不变——`CaseWhenLabelSpec.evaluation_phase` 已完成 |
| 9 | `SqlBuildPlan._build_single_table` / `_build_multi_table` | 不变——pre/post aggregate CaseWhenStep 分流已完成 |
| 10 | `SparkMapper._map_case_when` | 不变——Spark 端分流已完成 |

---

## 9. 残余风险

| # | 风险 | 等级 | 触发条件 | 缓解措施 |
|---|------|------|----------|----------|
| R1 | LLM 未填写 `output_column` | 中 | Planner 输出 `output_column: null` | `_get_output_kind` 自动按 UNKNOWN 处理，不路由到 Extractor |
| R2 | LLM 分类错误（LABEL ↔ DERIVED_DIMENSION 混淆） | 中 | Planner 误判列的业务性质 | 分类证据写入 Review artifact；Extractor 不处理 DERIVED_DIMENSION 列，最终 unresolved 检查阻断 |
| R3 | Planner 首次处理 label_table 产出异常 | 低 | 删除 H7 后行为变化 | Fake Adapter 集成测试覆盖 |
| R4 | 非 label_table spec 意外触发 Extractor | 低 | dataset_type 判断遗漏 | `_prepare_labels` 入口处 dataset_type 门禁 |
| R5 | SpecEnricher 仍对 Planner 已覆盖列重复生成 | 低 | 去重检查遗漏 | Section 4.9 去重逻辑 + 集成测试覆盖 |

---

## 10. 测试矩阵

### 10.1 文件 1：`tests/planning/test_uncertainty_routing.py`——表驱动单元测试

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

### 10.2 文件 2：`tests/pipeline/test_label_table_unified.py`——统一管线集成测试

全部使用 Fake Adapter，不依赖真实 LLM。

**Phase A 验收**：

| # | 场景 | 预期 |
|---|------|------|
| I1 | 合法单表标签全管线成功 | Planner 推断 dims/metrics → Enricher 正常 → Extractor 兜底 LABEL 列 → label_rules 非空 |
| I2 | 合法两表 JOIN+聚合+标签全管线成功 | Parser → Planner → Enricher → Labels → Relationship → Builder → **成功** |
| I3 | 多对多或无唯一性证据的 Join 确定性阻断 | Parser → Planner → Enricher → Labels 通过 → Relationship/CrossValidator 检测多对多 → **阻断 OpenQuestion** |
| I4 | 不支持 MODE 聚合 → OpenQuestion | Planner 产出 `output_kind=METRIC` uncertainty → 不调用 Extractor → **阻断 OpenQuestion** |
| I5 | LABEL 才调用 Extractor | `output_kind=LABEL` → Extractor 被调用；`output_kind=METRIC` → 跳过 |
| I6 | UNKNOWN 不调用 Extractor | `output_kind=UNKNOWN` → unresolved 检查阻断，Extractor 不执行 |
| I7 | 非 label_table 的 Planner CASE WHEN 正常工作 | detail_table → Planner 生成 case_when_rules → SpecEnricher 不重复覆盖 |
| I8 | SpecEnricher 不复用 Planner 已覆盖列 | spec.case_when_rules 已有 `peak_type` → Enricher H11 跳过 `peak_type` |
| I9 | SQL 与 Spark 从同一已验证 Contract 生成 | Contract 提取后 SQL 和 Spark 均成功编译——物理验证通过 |

**Phase B 验收**（后续 spec 实施后）：

| # | 场景 | 预期 |
|---|------|------|
| I10 | 合法三表 Join 链+聚合+标签全管线成功 | 3 表、2 条 JoinCandidate → 全管线通过 → SQL/Spark 一致 |

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

## 11. 执行顺序

1. 数据模型变更：`UncertaintyEntry` 增加 `output_column` + `output_kind`；`ParsedDeveloperSpec` 增加 `uncertainties`
2. RequirementPlanner Prompt + JSON Schema 变更：删除 H7、修改 H5、新增 H9
3. 管线重排：删除 label_table 跳过 → 插入 `_prepare_labels` → 删除 `_prepare_spec_for_planning`
4. uncertainties 透传：`_merge_uncertainties` + `_apply_uncertainties_to_spec` + `ProposalPromotion` 集成
5. SpecEnricher 去重：Planner 已覆盖列不重复生成
6. `_check_label_rule_conflicts` 改为 blocking OpenQuestion
7. 删除：`label_scope.py` + `_prepare_spec_for_planning` + sql_build_plan 重复门禁
8. 单元测试：`test_uncertainty_routing.py`（11 个用例）
9. 集成测试：`test_label_table_unified.py`（9 个 Phase A 用例 + 1 个 Phase B 占位）
10. 全量回归验证：`pytest` + `ruff check` + `git diff --check`
