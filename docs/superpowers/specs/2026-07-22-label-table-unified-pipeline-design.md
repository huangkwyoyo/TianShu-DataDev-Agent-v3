# Label Table 统一管线——设计方案

> **状态**：已确认，待进入 writing-plans
> **日期**：2026-07-22
> **触发**：label_table v1 单表门禁拒绝三表 JOIN 场景——`label_table v1 仅支持单表——当前 spec 包含 3 张表: ['fc', 'dd', 'tz']`

---

## 1. 目标与范围

### 1.1 目标

将 label_table 从独立 Planner 分支统一到标准管线中。label_table 应只是输出类型和附加验证策略，不应成为单独的 Planner/Enricher 分支。所有 dataset_type（detail_table、aggregate_table、label_table）通过同一管线。

### 1.2 范围

| 包含 | 排除（独立变更） |
|------|-----------------|
| 删除 v1 单表/非聚合门禁 | 三表及以上 JOIN 的 SqlBuildPlan DAG |
| 统一管线：Planner → Enricher → Labels → 单次 unresolved 检查 | MODE 聚合支持 |
| UncertaintyEntry 增加 output_column + output_kind | RIGHT JOIN 反向 Join 语义 |
| Planner 删除 H7 + 修改 H5 + 新增 H9 | 新的中间数据模型 |
| LabelExtractor 仅处理 LABEL 分类列 | 跨 dataset_type 的通用 ComputeStep 重构 |
| 两条独立 label 路径 + 冲突检测 | |

### 1.3 关键约束

- label_table 不新增 dataset_type
- 不删除现有安全验证（Validator 七项检查全部保留）
- Planner 的 output_kind 仅负责路由和诊断，不能决定验证通过——CASE AST、字段、聚合、Join 和 Builder 能力仍由确定性组件门禁
- 保持最小范围——不新增三表 DAG、MODE、RIGHT JOIN 或新的中间模型

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

### 2.2 标签规则责任链（4 层）

| 层 | 组件 | 触发条件 | 产出类型 |
|----|------|----------|----------|
| 1 | 显式 `label_rules`（程序员手写 YAML） | spec.label_rules 非空 | `CaseWhenDecl` |
| 2 | `RequirementPlanner` → `case_when_rules` | Planner 从业务描述推断 | `CaseWhenRule` → Validator → `CaseWhenDecl` |
| 3 | `SpecEnricher` → `inferred_case_when` | Enricher H11 推断 | `CaseWhenDecl` |
| 4 | `LabelExtractor`（最后兜底） | 仅 label_table + 仅 LABEL 分类 + 前 3 层未覆盖 | `LabelRuleProposal` → Validator → Promotion → `CaseWhenDecl` |

### 2.3 两条独立验证路径

```
路径 A: Planner/Enricher → CaseWhenRule/CaseWhenDecl
  → ProposalValidator（非 label_table spec 的 CASE WHEN）
  → spec.case_when_rules / spec.label_rules

路径 B: LabelExtractor → LabelRuleProposal
  → LabelRuleValidator（七项检查）
  → Promotion → CaseWhenDecl → spec.label_rules
```

两条路径按 `output_column` 做覆盖冲突检测——不混用三种模型（`CaseWhenRule`、`CaseWhenDecl`、`LabelRuleProposal`）。

### 2.4 Planner output_kind 的职责边界

`output_kind` 仅负责**路由**（决定未解析列交给哪个下游组件）和**诊断**（进入 Review artifact 供人工审查）。它**不能**决定验证通过——以下门禁始终由确定性组件执行：

| 门禁 | 执行者 | 依据 |
|------|--------|------|
| CASE 条件 AST 合法性 | `ProposalValidator` / `LabelRuleValidator` | Predicate 树结构验证 |
| 字段存在性 | `LabelRuleValidator._check_field_exists` | SourceManifest |
| 聚合函数白名单 | `ProposalValidator`（Schema enum 约束） | COUNT\|SUM\|AVG\|MIN\|MAX\|COUNT_DISTINCT |
| JOIN 安全性 | `RelationshipPlanner` / `CrossValidator` | 多对多检测、LEFT JOIN 右表唯一性 |
| Builder 能力 | `SqlBuildPlan` | 两表上限、不支持的操作符 |

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
# ── 标签规则处理——合并候选 + LabelExtractor + Validator + Promotion
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

    最后按 output_column 做覆盖冲突检查。
    """
    # ── 路径 A：case_when_rules 已由 Planner/Enricher 写入——无需额外处理

    # ── 路径 B：LabelExtractor fallback（仅 label_table + 仅 LABEL 分类列）
    if spec.dataset_type == DatasetType.LABEL_TABLE:
        unresolved = _find_unresolved_derived_columns(spec)
        if unresolved:
            # 仅处理 output_kind == LABEL 且 output_column 有效的列
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

    # ── 覆盖冲突检查
    _check_label_rule_conflicts(spec)

    # ── label_table 门禁：至少一个合法标签列
    if spec.dataset_type == DatasetType.LABEL_TABLE and not spec.label_rules:
        raise LabelTableConfigError(
            "label_table 至少需要一个合法标签列"
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

### 4.6 `_check_label_rule_conflicts`——冲突检测

```python
def _check_label_rule_conflicts(spec: ParsedDeveloperSpec) -> None:
    """检查 label_rules 和 case_when_rules 的 output_column 冲突。

    - 显式 label_rules 覆盖 case_when_rules → WARN（用户意图明确）
    - 两个 LLM 推断规则冲突 → ERROR（不可静默）
    """
    label_cols = {r.output_column for r in spec.label_rules}
    cw_cols = {r.output_column for r in spec.case_when_rules}
    overlap = label_cols & cw_cols

    if not overlap:
        return

    logger = logging.getLogger(__name__)
    for col in overlap:
        label_rule = next(r for r in spec.label_rules if r.output_column == col)
        if label_rule.evaluation_phase is not None:
            logger.warning(
                "标签规则覆盖——显式 label_rule 覆盖 case_when_rule: "
                "output_column=%s", col,
            )
        else:
            logger.error(
                "标签规则冲突——label_rules 和 case_when_rules 对同一列产生不同规则: "
                "output_column=%s，需人工审查", col,
            )
```

### 4.7 uncertainties 透传——确定性合并，不整体覆盖

`ProposalPromotion.promote()` 和 `Pipeline._run_requirement_planner()` 中，uncertainties 写入 spec 时必须做**确定性合并**——按 `(output_column, field_ref)` 组合键去重。新项覆盖同键旧项，保留其他旧项。不得整体覆盖已有诊断。

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
| 2 | `LabelExtractor.extract()` | 不变——输入从"所有未解析列"变为"仅 LABEL 分类列 + output_column 有效" |
| 3 | `Promotion.promote()` | 不变——仅新增 uncertainties 透传 |
| 4 | `_find_unresolved_derived_columns` | 保留作为便捷包装——内部逻辑不变 |
| 5 | `_extract_case_when_parse_errors` | 保留——CASE 解析错误仍阻断，不交给 Extractor |
| 6 | extraction/promotion artifact 保存路径 | 迁移到 `_prepare_labels` 中，artifact 结构不变 |
| 7 | CASE 解析错误→阻断 | 不变——解析失败仍阻断，Extractor 不重猜 |
| 8 | `Contract` + `ContractExtractor` | 不变——`CaseWhenLabelSpec.evaluation_phase` 已完成 |
| 9 | `SqlBuildPlan._build_single_table` / `_build_multi_table` | 不变——pre/post aggregate CaseWhenStep 分流已完成 |
| 10 | `SparkMapper._map_case_when` | 不变——Spark 端分流已完成 |

---

## 8. 残余风险

| # | 风险 | 等级 | 触发条件 | 缓解措施 |
|---|------|------|----------|----------|
| R1 | LLM 未填写 `output_column` | 中 | Planner 输出 `output_column: null` | `_get_output_kind` 自动按 UNKNOWN 处理，不路由到 Extractor |
| R2 | LLM 分类错误（LABEL ↔ DERIVED_DIMENSION 混淆） | 中 | Planner 误判列的业务性质 | 分类证据写入 Review artifact；Extractor 不处理 DERIVED_DIMENSION 列，最终 unresolved 检查阻断 |
| R3 | Planner 首次处理 label_table 产出异常 | 低 | 删除 H7 后行为变化 | Fake Adapter 集成测试覆盖 |
| R4 | 两条路径产生重复 label_rules | 低 | Planner + Extractor 对同一列产出规则 | `_check_label_rule_conflicts` 检测并去重 |
| R5 | 非 label_table spec 意外触发 Extractor | 低 | dataset_type 判断遗漏 | `_prepare_labels` 入口处 dataset_type 门禁 |
| R6 | 三表及以上 JOIN | 中 | 用户提交三表 label_table spec | SqlBuildPlan 仅支持到两表——三表场景通过 Parser/Planner/Enricher 后由 Builder 能力门禁阻断，不会静默成功 |

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
| 7 | _merge_uncertainties 同键覆盖异键保留 | existing `[{oc: "a", fr: "x"}, {oc: "b", fr: "y"}]`，incoming `[{oc: "a", fr: "x"}]` | 合并后 `[{oc: "a", fr: "x" (新值)}, {oc: "b", fr: "y" (保留)}]`——共 2 条 |
| 8 | _apply_uncertainties_to_spec 空列表 | `uncertainties=[]` | 返回原 spec（不触发 model_copy） |
| 9 | JSON Schema uncertainties 含 output_column + output_kind | Schema 验证 | required 含 `output_column` 和 `output_kind` |
| 10 | CASE 解析失败 uncertainty 带 output_column 仍阻断 | `field_ref="case_when_rules.parse_error.peak_type"`, `output_column="peak_type"` | `_extract_case_when_parse_errors` 返回阻断 OpenQuestion（不交给 Extractor） |

### 9.2 文件 2：`tests/pipeline/test_label_table_unified.py`——统一管线集成测试

全部使用 Fake Adapter，不依赖真实 LLM。

| # | 场景 | 预期 |
|---|------|------|
| I1 | 合法单表标签成功 | Planner + Enricher + Extractor 正常执行，label_rules 非空 |
| I2 | 合法两表聚合标签全管线成功 | **新建合法两表 fixture**（唯一 Join 键 + 已声明字段 + 受支持聚合）→ Parser → Planner → Enricher → Labels → Relationship → Builder → 成功 |
| I3 | 三表 borough Join Spec 被确定性能力门禁阻断 | Parser → Planner → Enricher → Labels 均通过 → 到达 Builder 层因两表上限被**确定性阻断**（不是静默成功） |
| I4 | MODE 被识别为未支持指标 | Planner 输出 `output_kind=METRIC` uncertainty → 不调用 Extractor → 阻断 OpenQuestion |
| I5 | LABEL 才调用 Extractor | Planner 分类 output_kind=LABEL → Extractor 被调用；output_kind=METRIC → 跳过 Extractor |
| I6 | UNKNOWN 不调用 Extractor | output_kind=UNKNOWN → 直接阻断（unresolved 检查），Extractor 不执行 |
| I7 | 非 label_table 的 Planner CASE WHEN 正常工作 | detail_table → Planner 正常生成 case_when_rules → SpecEnricher 正常工作 |

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

**I3 原始三表 Spec 的正确预期**：

原始三表 borough Join Spec（`fc.borough = tz.borough` 多对多 + MODE 聚合 + 未声明 `dd.date`）的验收路径：

1. Parser 阶段——不再被 v1 单表门禁阻断（本次变更的核心目标）
2. Planner 阶段——MODE 被 H9 识别为 `output_kind=METRIC` uncertainty；`dd.date` 被识别为 `output_kind=UNKNOWN` uncertainty
3. Enricher 阶段——正常执行
4. Labels 阶段——仅 LABEL 分类列进入 Extractor
5. Relationship/Validator 阶段——多对多 JOIN 被 CrossValidator 检测
6. Builder 阶段——若前序门禁均未阻断，两表上限门禁最终阻断

**关键验收断言**：不会静默成功——必在 Relationship/Validator/Builder 的确定性门禁层阻断。

---

## 10. 执行顺序

1. 数据模型变更：`UncertaintyEntry` 增加 `output_column` + `output_kind`；`ParsedDeveloperSpec` 增加 `uncertainties`
2. RequirementPlanner Prompt + JSON Schema 变更：删除 H7、修改 H5、新增 H9
3. 管线重排：删除 label_table 跳过 → 插入 `_prepare_labels` → 删除 `_prepare_spec_for_planning`
4. uncertainties 透传：`_merge_uncertainties` + `_apply_uncertainties_to_spec` + `ProposalPromotion` 集成
5. 删除：`label_scope.py` + `_prepare_spec_for_planning` + sql_build_plan 重复门禁
6. 单元测试：`test_uncertainty_routing.py`（10 个用例）
7. 集成测试：`test_label_table_unified.py`（7 个用例）
8. 全量回归验证：`pytest` + `ruff check` + `git diff --check`
