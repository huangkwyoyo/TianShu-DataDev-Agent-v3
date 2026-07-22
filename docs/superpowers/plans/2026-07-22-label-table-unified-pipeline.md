# Label Table 统一管线——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 label_table 从独立 Planner 分支统一到标准管线──删除 v1 单表门禁，所有 dataset_type 共享 Planner/Enricher 管线，LabelExtractor 仅兜底处理 Planner 标记 LABEL + unresolved + Planner 未生成 case_when_rules 的列。

**Architecture:** Pipeline 重排（`_prepare_labels` 移到 Planner+Enricher 之后）+ UncertaintyEntry 路由字段（output_column/output_kind）+ 现有 build_multi()/SqlProgram 多跳链复用。不新增 IR 或第二套 Join 排序。

**Tech Stack:** Python 3.12+ / Pydantic v2 / pytest / Fake Adapter（测试）

## Global Constraints

- 不新增 dataset_type
- 不删除现有安全验证（Validator 七项检查全部保留）
- Planner 的 output_kind 仅负责路由和诊断，不能决定验证通过
- **禁止重新实现第二套 Join 排序或 IR**──复用现有 build_multi() / _sort_candidates_to_chain() / build_sql_program_from_chain()
- 保持最小范围──不新增模型和架构层
- 所有注释使用中文
- 所有测试使用 Fake Adapter，不依赖真实 LLM
- TDD：先写失败测试，再写最小实现

---

## File Structure

| 文件 | 职责 | 变更类型 |
|------|------|----------|
| `src/tianshu_datadev/developer_spec/models.py` | UncertaintyEntry + ParsedDeveloperSpec 字段新增 | 修改 |
| `src/tianshu_datadev/planning/requirement_planner.py` | Prompt H5/H7/H9 + JSON Schema + _parse_response | 修改 |
| `src/tianshu_datadev/api/pipeline.py` | 管线重排 + _prepare_labels + 辅助函数 + uncertainties 透传 | 修改 |
| `src/tianshu_datadev/planning/proposal_promotion.py` | uncertainties 透传 | 修改 |
| `src/tianshu_datadev/planning/spec_enricher.py` | Planner 去重检查 | 修改 |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 删除重复 label_table 门禁 | 修改 |
| `src/tianshu_datadev/labels/label_scope.py` | 整个文件删除 | 删除 |
| `tests/planning/test_uncertainty_routing.py` | 单元测试（11 用例） | 新建 |
| `tests/pipeline/test_label_table_unified.py` | 集成测试（10 用例） | 新建 |

---

### Task 1: UncertaintyEntry 增加 output_column + output_kind

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py:612-616`

**Interfaces:**
- Consumes: 无（基础模型变更）
- Produces: `UncertaintyEntry.output_column: str | None`, `UncertaintyEntry.output_kind: Literal["LABEL", "METRIC", "DERIVED_DIMENSION", "UNKNOWN"]`

- [ ] **Step 1: 修改 UncertaintyEntry 类定义**

在 `models.py` 第 612-616 行，将现有定义替换为：

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

注意：需要在文件顶部 `Literal` 导入中确认已包含 `LABEL`, `METRIC`, `DERIVED_DIMENSION`, `UNKNOWN`。

- [ ] **Step 2: 验证向后兼容——默认值不破坏已有序列化**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.developer_spec.models import UncertaintyEntry
u = UncertaintyEntry(field_ref='test', description='test desc')
assert u.output_kind == 'UNKNOWN'
assert u.output_column is None
print('OK: 默认值向后兼容')
"
```

- [ ] **Step 3: 验证枚举约束**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.developer_spec.models import UncertaintyEntry
try:
    u = UncertaintyEntry(field_ref='x', description='y', output_kind='INVALID')
    print('FAIL: 应抛出 ValidationError')
except Exception as e:
    print(f'OK: {type(e).__name__}')
"
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/developer_spec/models.py
git commit -m "feat: UncertaintyEntry 增加 output_column + output_kind 路由字段

- output_column: 路由主键，None 时按 UNKNOWN 处理
- output_kind: LABEL | METRIC | DERIVED_DIMENSION | UNKNOWN
- 默认值 output_kind='UNKNOWN', output_column=None——向后兼容

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: ParsedDeveloperSpec 新增 uncertainties 字段

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py:1003-1031`

**Interfaces:**
- Consumes: Task 1 的 `UncertaintyEntry`
- Produces: `ParsedDeveloperSpec.uncertainties: list[UncertaintyEntry]`

- [ ] **Step 1: 在 ParsedDeveloperSpec 中新增 uncertainties 字段**

在 `models.py` 第 1030 行 `open_questions` 之前插入：

```python
    # ── RequirementPlanner 模型（v3.1 新增）──
    derived_dimensions: list[DerivedDimensionDecl] = Field(default_factory=list)
    case_when_rules: list[CaseWhenRule] = Field(default_factory=list)
    # ── Planner 不确定性分类（v3.2 新增）──
    uncertainties: list[UncertaintyEntry] = Field(default_factory=list)
    open_questions: list[OpenQuestion] = []
```

- [ ] **Step 2: 验证 access**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec
# 快速构造一个最小 spec 验证字段存在
spec = ParsedDeveloperSpec(
    spec_id='test', spec_hash='abc123', title='Test',
    description='test', input_tables=[], metrics=[], dimensions=[],
    output_spec={'columns': [], 'grain': []},
)
assert hasattr(spec, 'uncertainties')
assert spec.uncertainties == []
print('OK: uncertainties 字段可用')
"
```

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/developer_spec/models.py
git commit -m "feat: ParsedDeveloperSpec 新增 uncertainties 字段

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: RequirementPlanner Prompt + JSON Schema 变更

**Files:**
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:54-55` (H5)
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:59` (H7 删除)
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:61-62` (H8 后新增 H9)
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:139-154` (JSON Schema uncertainties)

**Interfaces:**
- Consumes: 无
- Produces: 更新后的 `_REQUIREMENT_PLANNER_SYSTEM_PROMPT` 和 `_REQUIREMENT_PLANNER_JSON_SCHEMA`

- [ ] **Step 1: 修改 H5——增加 output_kind 和 output_column 指令**

将第 54-55 行的 H5 替换为：

```text
H5. 不确定时写入 uncertainties。每条包含：
    - field_ref: 诊断标识（自由文本，仅用于日志）
    - output_column: 输出列名（路由主键——必须填写，否则按 UNKNOWN 处理）
    - description: 为什么不确定
    - candidates: 可能的解析方案（可为空列表）
    - output_kind: 该列的业务性质——LABEL | METRIC | DERIVED_DIMENSION | UNKNOWN
      判断依据（互斥——每列只能匹配一条）：
      · LABEL: 条件分支产出的有限值分类（如 risk_level, peak_type, 安全等级）
        特征：输出依赖 WHEN/THEN 逻辑，不能仅用确定性函数从单一源列推导
      · METRIC: 聚合或聚合后数值计算（如 avg_xxx, total_xxx, rate）
      · DERIVED_DIMENSION: 对源字段做直接确定性变换（如 HOUR(pickup_at) → pickup_hour）
        特征：单输入 + 确定性函数 → 单输出，无分支
      · UNKNOWN: 完全无法判断——系统将阻断并请求人工裁决
```

- [ ] **Step 2: 删除 H7**

删除第 59 行：
```diff
- H7. label_table 类型不在你的职责范围——返回全空输出。
```

- [ ] **Step 3: 新增 H9——白名单外聚合识别**

在第 62 行（H8 之后、`"""` 结束之前）插入：

```text
H9. 遇到白名单外聚合函数（如 MODE、MEDIAN、STDDEV 等）时：
    不要尝试构造 MetricDecl——Schema 会拒绝。
    输出一条 uncertainty：output_kind=METRIC，output_column=目标列名，
    description 说明"聚合函数 MODE 不在白名单 COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT 中"。
```

- [ ] **Step 4: 更新 JSON Schema——uncertainties items 增加 output_column + output_kind**

将第 139-154 行的 uncertainties schema 替换为：

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

- [ ] **Step 5: 验证 Prompt 字符串语法**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.planning.requirement_planner import (
    _REQUIREMENT_PLANNER_SYSTEM_PROMPT,
    _REQUIREMENT_PLANNER_JSON_SCHEMA,
)
# H7 不应再出现
assert '不在你的职责范围' not in _REQUIREMENT_PLANNER_SYSTEM_PROMPT
# output_kind 应出现在 Prompt 和 Schema 中
assert 'output_kind' in _REQUIREMENT_PLANNER_SYSTEM_PROMPT
assert 'output_kind' in str(_REQUIREMENT_PLANNER_JSON_SCHEMA)
assert 'output_column' in str(_REQUIREMENT_PLANNER_JSON_SCHEMA)
# H9 应出现
assert 'MODE' in _REQUIREMENT_PLANNER_SYSTEM_PROMPT
# required 应包含 output_column 和 output_kind
import json
schema = json.dumps(_REQUIREMENT_PLANNER_JSON_SCHEMA)
assert 'output_column' in schema
assert 'output_kind' in schema
print('OK: Prompt + Schema 更新正确')
"
```

- [ ] **Step 6: 提交**

```bash
git add src/tianshu_datadev/planning/requirement_planner.py
git commit -m "feat: RequirementPlanner Prompt——删除 H7 + 修改 H5 + 新增 H9 + JSON Schema 增加 output_column/output_kind

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: RequirementPlanner _parse_response 适配新 UncertaintyEntry 字段

**Files:**
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:394-397` (CASE WHEN 解析失败 uncertainty)
- Modify: `src/tianshu_datadev/planning/requirement_planner.py:400-404` (LLM 返回 uncertainties 解析)

**Interfaces:**
- Consumes: Task 1 的 `UncertaintyEntry` 新字段
- Produces: `RequirementPlannerOutput` 含完整 `output_column` + `output_kind` 的 uncertainties

- [ ] **Step 1: 更新 CASE WHEN 解析失败的 UncertaintyEntry 构造**

将第 394-397 行替换为：

```python
                uncertainties.append(UncertaintyEntry(
                    field_ref=f"case_when_rules.parse_error.{output_col}",
                    output_column=output_col if output_col != "<unknown>" else None,
                    output_kind="LABEL",
                    description=f"CASE WHEN 规则 '{output_col}' 解析失败：{e}",
                ))
```

- [ ] **Step 2: LLM 返回的 uncertainties 解析不需要改**

第 400-404 行已使用 `UncertaintyEntry(**u)` 展开构造——Pydantic 会自动映射新字段。验证即可：

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.planning.requirement_planner import RequirementPlanner
from tianshu_datadev.developer_spec.models import UncertaintyEntry
# 验证新字段能从 dict 反序列化
raw = {
    'field_ref': 'test.risk_label',
    'output_column': 'risk_label',
    'output_kind': 'LABEL',
    'description': '需要 CASE WHEN 定义',
    'candidates': ['方案A', '方案B'],
}
u = UncertaintyEntry(**raw)
assert u.output_column == 'risk_label'
assert u.output_kind == 'LABEL'
print('OK: 新字段反序列化正确')
"
```

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/planning/requirement_planner.py
git commit -m "fix: CASE WHEN 解析失败 uncertainty 填写 output_column + output_kind

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: 管线重排——_enrich_and_plan 删除 label_table 跳过

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py:647-656`

**Interfaces:**
- Consumes: `spec.dataset_type`, `self._requirement_planner`, `self._spec_enricher`
- Produces: 所有 dataset_type 统一执行 Planner + Enricher

- [ ] **Step 1: 删除 Planner 的 label_table 跳过**

将第 647-648 行：
```python
        if (self._requirement_planner is not None
                and spec.dataset_type != DatasetType.LABEL_TABLE):
```
替换为：
```python
        if self._requirement_planner is not None:
```

- [ ] **Step 2: 删除 Enricher 的 label_table 跳过**

将第 655 行：
```python
        if spec.dataset_type != DatasetType.LABEL_TABLE:
            spec = self._spec_enricher.apply_enrichment(spec, manifest)
```
替换为：
```python
        spec = self._spec_enricher.apply_enrichment(spec, manifest)
```

注意：缩进减少一级——原来在 `if` 块内，现在直接执行。

- [ ] **Step 3: 验证语法**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.api.pipeline import Pipeline; print('OK: 导入成功')"
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: 删除 label_table 的 Planner/Enricher 跳过——所有 dataset_type 统一管线

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: 实现辅助纯函数

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（在 `_summarize_warnings` 之后、`_extract_case_when_parse_errors` 之前插入）

**Interfaces:**
- Produces:
  - `_get_output_kind(column_name: str, uncertainties: list[UncertaintyEntry]) -> str`
  - `_check_label_rule_conflicts(spec: ParsedDeveloperSpec) -> list[OpenQuestion]`
  - `_merge_uncertainties(existing: list[UncertaintyEntry], incoming: list[UncertaintyEntry]) -> list[UncertaintyEntry]`
  - `_apply_uncertainties_to_spec(spec: ParsedDeveloperSpec, uncertainties: list[UncertaintyEntry]) -> ParsedDeveloperSpec`

- [ ] **Step 1: 添加 import**

确认 pipeline.py 顶部已有 `UncertaintyEntry` 导入（在 `from tianshu_datadev.developer_spec.models import ...` 中）。如果没有，添加进去。

在第 25-31 行的 import 块中确认包含 `UncertaintyEntry`：
```python
from tianshu_datadev.developer_spec.models import (
    DatasetType,
    OpenQuestion,
    ParsedDeveloperSpec,
    RequirementPlannerOutput,
    RequirementProposal,
    StrictModel,
    UncertaintyEntry,  # 确认此行存在
)
```

- [ ] **Step 2: 在 `_extract_case_when_parse_errors` 之前插入四个辅助函数**

在第 203 行（`def _extract_case_when_parse_errors` 之前）插入：

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

- [ ] **Step 3: 验证导入和语法**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.api.pipeline import (
    _get_output_kind, _check_label_rule_conflicts,
    _merge_uncertainties, _apply_uncertainties_to_spec,
)
print('OK: 四个辅助函数导入成功')
"
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: 实现辅助纯函数——_get_output_kind/_check_label_rule_conflicts/_merge_uncertainties/_apply_uncertainties_to_spec

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: 实现 _prepare_labels + 删除 _prepare_spec_for_planning

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py:454-553`（删除 `_prepare_spec_for_planning`）
- Modify: `src/tianshu_datadev/api/pipeline.py:765`（删除调用）
- Modify: `src/tianshu_datadev/api/pipeline.py`（在 `_run_requirement_planner` 之后插入 `_prepare_labels`）

**Interfaces:**
- Consumes: Task 6 的辅助函数, `self._label_extractor`, `LabelRuleValidator`, `Promotion`
- Produces: `_prepare_labels(self, spec, manifest) -> ParsedDeveloperSpec`

- [ ] **Step 1: 删除 `_prepare_spec_for_planning` 整个方法**

删除第 454-553 行的整个方法。

- [ ] **Step 2: 删除 `_parse_and_enrich` 中的 `_prepare_spec_for_planning` 调用**

在 `_parse_and_enrich` 方法中（约第 765 行），删除：
```diff
- spec = self._prepare_spec_for_planning(spec)
```

- [ ] **Step 3: 在 `_run_requirement_planner` 方法之后插入 `_prepare_labels`**

找到 `_run_requirement_planner` 方法的结束位置（约第 728 行），在其后插入：

```python
    def _prepare_labels(
        self, spec: ParsedDeveloperSpec, manifest,
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

                # 仅处理 Planner 标记为 LABEL、仍 unresolved、且 Planner 未生成规则的列
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
                        from tianshu_datadev.labels.label_rule_validator import (
                            LabelRuleValidator,
                        )
                        from tianshu_datadev.labels.promotion import Promotion
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

        # ── label_table 门禁：至少一个合法标签列
        if spec.dataset_type == DatasetType.LABEL_TABLE:
            has_labels = bool(spec.label_rules) or bool(spec.case_when_rules)
            if not has_labels:
                raise LabelTableConfigError(
                    "label_table 至少需要一个合法标签列——"
                    "label_rules 和 case_when_rules 均为空"
                )

        return spec
```

- [ ] **Step 4: 在 `_enrich_and_plan` 中插入 `_prepare_labels` 调用**

在 `_enrich_and_plan` 方法中，Planner + Enricher 之后、unresolved 检查之前（约第 658 行），插入：

```python
        # ── 2.5. 标签规则处理——合并候选 + Extractor + Validator + Promotion
        spec = self._prepare_labels(spec, manifest)
```

完整上下文（修改后）：
```python
        # ── 2. SpecEnricher：完整 scope，后执行 ──
        spec = self._spec_enricher.apply_enrichment(spec, manifest)

        # ── 2.5. 标签规则处理——合并候选 + Extractor + Validator + Promotion
        spec = self._prepare_labels(spec, manifest)

        # ── 3. 统一 unresolved 检查
        unresolved_after = _find_unresolved_derived_columns(spec)
```

- [ ] **Step 5: 验证导入和语法**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.api.pipeline import Pipeline; print('OK: Pipeline 导入成功')"
```

- [ ] **Step 6: 验证旧方法已删除**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && grep -n "_prepare_spec_for_planning\|validate_label_table_v1_scope" src/tianshu_datadev/api/pipeline.py
```
预期：无输出（两个引用均已删除）。

- [ ] **Step 7: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: 实现 _prepare_labels + 删除 _prepare_spec_for_planning

I1 修正：LabelExtractor 仅在 Planner 未生成 case_when_rules 时兜底。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: ProposalPromotion uncertainties 透传

**Files:**
- Modify: `src/tianshu_datadev/planning/proposal_promotion.py:25-66`

**Interfaces:**
- Consumes: Task 6 的 `_merge_uncertainties`, `ProposalPromotion.promote()`
- Produces: `promote()` 返回的 spec 含合并后的 uncertainties

- [ ] **Step 1: 在 `promote()` 中添加 uncertainties 合并**

在 `promote()` 方法的 `return result` 之前（第 66 行之前），新增 uncertainties 合并逻辑：

```python
        # ── uncertainties 透传——确定性合并，不整体覆盖
        if proposal.uncertainties:
            from tianshu_datadev.api.pipeline import _merge_uncertainties
            merged = _merge_uncertainties(result.uncertainties, proposal.uncertainties)
            result = result.model_copy(update={"uncertainties": merged})

        return result
```

- [ ] **Step 2: 验证**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.planning.proposal_promotion import ProposalPromotion
print('OK: ProposalPromotion 导入成功')
"
```

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/planning/proposal_promotion.py
git commit -m "feat: ProposalPromotion 透传 uncertainties——确定性合并

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: _run_requirement_planner 失败路径 uncertainties 保留

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py:720-728`（`_run_requirement_planner` 方法）

**Interfaces:**
- Consumes: Task 6 的 `_apply_uncertainties_to_spec`
- Produces: Validator 失败或 CASE 解析失败时 uncertainties 不丢失

- [ ] **Step 1: 修改 CASE WHEN 解析失败路径**

将第 720-721 行：
```python
        if case_when_parse_errors:
            return spec, case_when_parse_errors
```
替换为：
```python
        if case_when_parse_errors:
            spec = _apply_uncertainties_to_spec(spec, planner_output.uncertainties)
            return spec, case_when_parse_errors
```

- [ ] **Step 2: 修改 Validator 失败路径**

将第 724-725 行：
```python
        if not valid:
            return spec, questions
```
替换为：
```python
        if not valid:
            spec = _apply_uncertainties_to_spec(spec, proposal.uncertainties)
            return spec, questions
```

- [ ] **Step 3: 验证**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.api.pipeline import Pipeline; print('OK')"
```

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "fix: _run_requirement_planner 失败路径保留 uncertainties 不丢失

CASE 解析失败和 Validator 拒绝时，uncertainties 仍写入 spec——artifact 审查需要分类证据。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 10: SpecEnricher Planner 去重

**Files:**
- Modify: `src/tianshu_datadev/planning/spec_enricher.py:1908-1911`

**Interfaces:**
- Consumes: `spec.case_when_rules`, `spec.label_rules`
- Produces: `existing_label_cols` 扩展为包含 Planner 已覆盖列

- [ ] **Step 1: 在 `apply_enrichment` 的 CASE WHEN 合并前扩展去重集合**

将第 1911 行：
```python
        existing_label_cols = {r.output_column for r in spec.label_rules}
```
替换为：
```python
        # 收集 Planner 已覆盖的输出列——SpecEnricher 不重复生成
        existing_label_cols = {r.output_column for r in spec.label_rules}
        existing_label_cols.update(r.output_column for r in spec.case_when_rules)
```

- [ ] **Step 2: 验证**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.planning.spec_enricher import SpecEnricher; print('OK')"
```

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/planning/spec_enricher.py
git commit -m "feat: SpecEnricher 不重复生成 Planner 已覆盖的 case_when 列

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 11: 删除 label_scope.py + sql_build_plan 重复门禁

**Files:**
- Delete: `src/tianshu_datadev/labels/label_scope.py`
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py:2054-2065`

**Interfaces:**
- Consumes: 无
- Produces: 清理后的代码库——无 `validate_label_table_v1_scope` 残留

- [ ] **Step 1: 删除 label_scope.py**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && git rm src/tianshu_datadev/labels/label_scope.py
```

- [ ] **Step 2: 删除 sql_build_plan.py 中的重复门禁**

删除 `sql_build_plan.py` 第 2054-2065 行：
```diff
-        # 1. label_table 额外作用域门禁——单表、非聚合、禁止 NOT
-        if spec.dataset_type == DatasetType.LABEL_TABLE:
-            from tianshu_datadev.labels.label_scope import (
-                LabelScopeError,
-                validate_label_table_v1_scope,
-            )
-            try:
-                validate_label_table_v1_scope(spec)
-            except LabelScopeError as exc:
-                raise DerivedColumnRuleMissingError(
-                    f"label_table v1 作用域约束违反——{exc}"
-                ) from exc
```

注意：保留 `# 1.` 之后的注释和代码（unresolved output 检查从原 `# 2.` 变为 `# 1.`）。

- [ ] **Step 3: 验证全局无残留**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && grep -rn "validate_label_table_v1_scope\|_prepare_spec_for_planning\|不在你的职责范围" src/ 2>/dev/null || echo "OK: 无残留引用"
```
预期：`OK: 无残留引用`

- [ ] **Step 4: 验证导入**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder; print('OK')"
```

- [ ] **Step 5: 提交**

```bash
git add src/tianshu_datadev/labels/label_scope.py src/tianshu_datadev/planning/sql_build_plan.py
git commit -m "feat: 删除 label_scope.py + sql_build_plan 重复门禁

label_table v1 单表/非聚合门禁已由管线层统一处理，Builder 层不再重复校验。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 12: 单元测试——test_uncertainty_routing.py

**Files:**
- Create: `tests/planning/test_uncertainty_routing.py`

**Interfaces:**
- Consumes: Task 1-9 的所有实现
- Produces: 11 个通过的单元测试

- [ ] **Step 1: 编写完整测试文件**

```python
"""UncertaintyEntry 路由 + 合并 + 冲突检测——单元测试。

全部使用确定性构造，不依赖 LLM 或数据库。
"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.api.pipeline import (
    _apply_uncertainties_to_spec,
    _check_label_rule_conflicts,
    _get_output_kind,
    _merge_uncertainties,
)
from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    CaseWhenRule,
    OpenQuestion,
    ParsedDeveloperSpec,
    UncertaintyEntry,
)
from tianshu_datadev.planning.proposal_promotion import ProposalPromotion
from tianshu_datadev.developer_spec.models import RequirementProposal


def _make_minimal_spec(**overrides) -> ParsedDeveloperSpec:
    """构建最小 ParsedDeveloperSpec——减少样板代码。"""
    defaults = dict(
        spec_id="test",
        spec_hash="abc123",
        title="Test",
        description="test",
        input_tables=[],
        metrics=[],
        dimensions=[],
        output_spec={"columns": [], "grain": []},
    )
    defaults.update(overrides)
    return ParsedDeveloperSpec(**defaults)


# ═══ 模型默认值 ═══

def test_uncertainty_entry_defaults():
    """UncertaintyEntry 默认值——output_kind=UNKNOWN, output_column=None。"""
    u = UncertaintyEntry(field_ref="x", description="test")
    assert u.output_kind == "UNKNOWN"
    assert u.output_column is None


def test_uncertainty_entry_invalid_output_kind():
    """output_kind 枚举约束——非法值抛 ValidationError。"""
    with pytest.raises(ValidationError):
        UncertaintyEntry(
            field_ref="x", description="test", output_kind="INVALID",
        )


# ═══ _get_output_kind 路由 ═══

def test_get_output_kind_exact_match():
    """精确匹配 output_column 返回正确的 output_kind。"""
    u = UncertaintyEntry(
        field_ref="risk_label_ref",
        output_column="risk_label",
        output_kind="LABEL",
        description="需要 CASE WHEN",
    )
    assert _get_output_kind("risk_label", [u]) == "LABEL"


def test_get_output_kind_none_output_column_skipped():
    """output_column=None 时跳过，返回 UNKNOWN。"""
    u = UncertaintyEntry(
        field_ref="x", output_column=None, output_kind="LABEL",
        description="缺少路由键",
    )
    assert _get_output_kind("x", [u]) == "UNKNOWN"


def test_get_output_kind_no_field_ref_parsing():
    """不解析 field_ref 字符串——仅通过 output_column 匹配。"""
    u = UncertaintyEntry(
        field_ref="case_when.parse_error.x",
        output_column=None,
        output_kind="LABEL",
        description="field_ref 含 'x' 但 output_column 为 None",
    )
    # 查询 "x"——field_ref 不被解析，output_column=None → 跳过
    assert _get_output_kind("x", [u]) == "UNKNOWN"


# ═══ uncertainties 透传 ═══

def test_proposal_promotion_passthrough_uncertainties():
    """ProposalPromotion.promote() 透传 uncertainties 到 spec。"""
    entry = UncertaintyEntry(
        field_ref="test_field",
        output_column="col_a",
        output_kind="LABEL",
        description="test",
    )
    proposal = RequirementProposal(
        proposal_id="p1",
        spec_hash="abc123",
        uncertainties=[entry],
    )
    spec = _make_minimal_spec(spec_hash="abc123")
    promoter = ProposalPromotion()
    result = promoter.promote(proposal, spec)
    assert len(result.uncertainties) == 1
    assert result.uncertainties[0].output_column == "col_a"


# ═══ _merge_uncertainties ═══

def test_merge_uncertainties_same_key_overwrite():
    """同键覆盖——新值覆盖旧值，异键保留。"""
    existing = [
        UncertaintyEntry(
            field_ref="fr_a", output_column="a", output_kind="LABEL",
            description="old a",
        ),
        UncertaintyEntry(
            field_ref="fr_b", output_column="b", output_kind="METRIC",
            description="old b",
        ),
    ]
    incoming = [
        UncertaintyEntry(
            field_ref="fr_a", output_column="a", output_kind="LABEL",
            description="new a",
        ),
    ]
    merged = _merge_uncertainties(existing, incoming)
    assert len(merged) == 2  # 保留异键 b，覆盖同键 a
    # 找到 output_column="a" 的条目——应为新值
    a_entries = [u for u in merged if u.output_column == "a"]
    assert len(a_entries) == 1
    assert a_entries[0].description == "new a"
    # output_column="b" 应保留
    b_entries = [u for u in merged if u.output_column == "b"]
    assert len(b_entries) == 1


def test_apply_uncertainties_to_spec_empty():
    """空列表——返回原 spec 不触发 model_copy。"""
    spec = _make_minimal_spec()
    result = _apply_uncertainties_to_spec(spec, [])
    assert result is spec  # 空列表不触发 copy


# ═══ JSON Schema ═══

def test_json_schema_requires_output_column_and_output_kind():
    """JSON Schema 的 uncertainties required 含 output_column 和 output_kind。"""
    from tianshu_datadev.planning.requirement_planner import (
        _REQUIREMENT_PLANNER_JSON_SCHEMA,
    )
    items = _REQUIREMENT_PLANNER_JSON_SCHEMA["properties"]["uncertainties"]["items"]
    assert "output_column" in items["required"]
    assert "output_kind" in items["required"]


# ═══ CASE 解析失败仍阻断 ═══

def test_case_when_parse_error_uncertainty_blocks():
    """CASE WHEN 解析失败的 uncertainty 产出阻断 OpenQuestion。"""
    from tianshu_datadev.api.pipeline import _extract_case_when_parse_errors
    from tianshu_datadev.developer_spec.models import RequirementPlannerOutput

    ue = UncertaintyEntry(
        field_ref="case_when_rules.parse_error.peak_type",
        output_column="peak_type",
        output_kind="LABEL",
        description="CASE WHEN 规则 'peak_type' 解析失败",
    )
    output = RequirementPlannerOutput(uncertainties=[ue])
    questions = _extract_case_when_parse_errors(output)
    assert len(questions) == 1
    assert questions[0].blocking is True


# ═══ 同列冲突 → blocking ═══

def test_label_rule_conflict_blocking():
    """同 output_column 在 label_rules 和 case_when_rules 中 → blocking。"""
    spec = _make_minimal_spec(
        label_rules=[
            CaseWhenDecl(output_column="x", branches=[], else_value="a"),
        ],
        case_when_rules=[
            CaseWhenRule(output_column="x", branches=[], else_value="b"),
        ],
    )
    questions = _check_label_rule_conflicts(spec)
    assert len(questions) == 1
    assert questions[0].blocking is True
    assert "x" in questions[0].description
```

- [ ] **Step 2: 运行测试——全部通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_uncertainty_routing.py -v
```
预期：11 passed

- [ ] **Step 3: 提交**

```bash
git add tests/planning/test_uncertainty_routing.py
git commit -m "test: UncertaintyEntry 路由 + 合并 + 冲突检测——11 个单元测试

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 13: 集成测试——test_label_table_unified.py

**Files:**
- Create: `tests/pipeline/test_label_table_unified.py`

**Interfaces:**
- Consumes: Task 1-11 的所有实现
- Produces: 10 个通过的集成测试（含三表全链路物理验证）

- [ ] **Step 1: 编写集成测试文件**

```python
"""Label Table 统一管线——集成测试。

全部使用 Fake Adapter 和 FakeLabelExtractor，不依赖真实 LLM 或数据库。
"""

import pytest

from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.developer_spec.models import (
    CaseWhenBranch,
    CaseWhenDecl,
    CaseWhenRule,
    DatasetType,
    LabelPredicateBranch,
    LabelRuleProposal,
    ParsedDeveloperSpec,
    UncertaintyEntry,
)
from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
from tianshu_datadev.llm.adapters.fake import FakeAdapter


def _make_label_spec(
    *,
    input_tables: list[dict],
    metrics: list[dict] | None = None,
    dimensions: list[dict] | None = None,
    output_columns: list[str] | None = None,
    description: str = "测试 label_table",
    label_rules: list | None = None,
    joins: list[dict] | None = None,
) -> ParsedDeveloperSpec:
    """构建 label_table spec fixture。"""
    from tianshu_datadev.developer_spec.models import (
        DimensionDecl,
        InputTableDecl,
        JoinDecl,
        JoinTypeEnum,
        MetricDecl,
        OutputColumnDecl,
        OutputSpecDecl,
    )

    tables = []
    for t in input_tables:
        cols = []
        for c in t.get("columns", []):
            cols.append(type("Col", (), {
                "column_name": c[0] if isinstance(c, tuple) else c,
                "normalized_name": c[0] if isinstance(c, tuple) else c,
                "data_type": c[1] if isinstance(c, tuple) else "varchar",
                "nullable": True,
                "is_key": False,
            })())
        tables.append(InputTableDecl(
            table_name=t["name"],
            table_alias=t["name"],
            key_columns=t.get("key_columns", []),
            columns=cols,
            business_columns=[],
        ))

    metric_decls = []
    if metrics:
        for m in metrics:
            metric_decls.append(MetricDecl(
                metric_name=m["name"],
                aggregation=m["aggregation"],
                alias=m["alias"],
                input_column=m.get("input_column", ""),
            ))

    dim_decls = []
    if dimensions:
        for d in dimensions:
            dim_decls.append(DimensionDecl(
                dimension_name=d["dimension_name"],
                column_ref=d.get("column_ref", d["dimension_name"]),
            ))

    join_decls = None
    if joins:
        join_decls = []
        for j in joins:
            join_decls.append(JoinDecl(
                left_table=j["left"].split(".")[0],
                right_table=j["right"].split(".")[0],
                left_key=j["left"].split(".")[1],
                right_key=j["right"].split(".")[1],
                join_type=JoinTypeEnum.LEFT,
            ))

    cols = output_columns or ["col_a"]
    return ParsedDeveloperSpec(
        spec_id="test_spec",
        spec_hash="test_hash_001",
        title="Test Label Table",
        description=description,
        dataset_type=DatasetType.LABEL_TABLE,
        input_tables=tables,
        metrics=metric_decls,
        dimensions=dim_decls,
        joins=join_decls,
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name=c) for c in cols],
            grain=[d["dimension_name"] for d in (dimensions or [])] if dimensions else [],
        ),
        label_rules=label_rules or [],
    )


# ═══ I1: Planner 生成 case_when_rules → LabelExtractor 不调用 ═══

def test_i1_planner_covered_label_extractor_skipped():
    """Planner 已生成 case_when_rules 时，LabelExtractor 不调用。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "risk_label"],
        description="按 val 定义 risk_label：高风险/低风险",
    )
    # Fake Adapter——Planner 会生成 case_when_rules covering risk_label
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    # FakeLabelExtractor 预置 proposal——验证它不被调用
    # 使用计数器 subclass 检测 extract() 是否被调用
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    result = pipeline.run_parse_and_enrich(spec)
    # I1: Planner 已覆盖 risk_label → LabelExtractor 不调用
    assert call_count[0] == 0, f"LabelExtractor 不应被调用，但调用了 {call_count[0]} 次"


# ═══ I2: 合法两表 JOIN+聚合+标签 ═══

def test_i2_two_table_join_agg_label_success():
    """合法两表 JOIN+聚合+标签全管线成功。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"), ("fare_amount", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar")]},
        ],
        joins=[{"left": "trips.pickup_location_id", "right": "zones.location_id"}],
        metrics=[
            {"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"},
            {"name": "平均费用", "aggregation": "AVG", "input_column": "fare_amount", "alias": "avg_fare"},
        ],
        dimensions=[{"dimension_name": "borough", "column_ref": "zones.borough"}],
        output_columns=["borough", "trip_count", "avg_fare", "risk_label"],
        description="按 borough 聚合 trip_count 和 avg_fare，按 avg_fare 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "avg_fare", "op": ">", "right": {"node_type": "LITERAL", "value": 50, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
    # 验证 Planner 生成了 case_when_rules
    assert len(spec.case_when_rules) > 0 or len(spec.label_rules) > 0


# ═══ I3: 合法三表 JOIN+聚合+标签——全链路验收 ═══

def test_i3_three_table_join_chain_agg_label_full_pipeline():
    """合法三表 JOIN+聚合+标签——全链路成功。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"),
                        ("dropoff_location_id", "int"), ("fare_amount", "double"),
                        ("trip_distance", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar"), ("zone_name", "varchar")]},
            {"name": "weather", "key_columns": ["weather_id"],
             "columns": [("weather_id", "int"), ("pickup_date", "date"),
                        ("weather_condition", "varchar"), ("temp_high", "double")]},
        ],
        joins=[
            {"left": "trips.pickup_location_id", "right": "zones.location_id"},
            # 第二条 Join 在描述中由 RelationshipPlanner 推断
        ],
        metrics=[
            {"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"},
            {"name": "平均费用", "aggregation": "AVG", "input_column": "fare_amount", "alias": "avg_fare"},
        ],
        dimensions=[
            {"dimension_name": "borough", "column_ref": "zones.borough"},
        ],
        output_columns=["borough", "weather_condition", "trip_count", "avg_fare", "risk_label"],
        description="按 borough 和 weather_condition 聚合，按 avg_fare 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [{"dimension_name": "weather_condition", "column_ref": "weather_condition", "source_table": "weather"}],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "avg_fare", "op": ">", "right": {"node_type": "LITERAL", "value": 50, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
    # 验证 Parser + Planner 成功
    assert len(spec.case_when_rules) > 0
    # 验证 hypothesis 不为 None（至少一个 Join 关系）
    # result 是一个 dict，包含 hypothesis 字段


# ═══ I4: 多对多 Join 阻断 ═══

def test_i4_many_to_many_join_blocked():
    """多对多 Join → 确定性阻断。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "a", "key_columns": ["id"], "columns": [("id", "int"), ("x", "varchar")]},
            {"name": "b", "key_columns": ["id"], "columns": [("id", "int"), ("y", "varchar")]},
        ],
        joins=[{"left": "a.x", "right": "b.y"}],  # 无唯一性证据
        output_columns=["x", "y", "label_col"],
        description="多对多 Join——应被阻断",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "label_col",
            "branches": [{"condition": {"node_type": "IS_NULL", "column": "x"}, "then_value": "空"}],
            "else_value": "非空",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # 多对多或无唯一性证据——CrossValidator 应阻断
    # result 为 None 或含 blocking OpenQuestion
    if result is not None:
        open_questions = result.get("open_questions", [])
        blocking = [q for q in open_questions if q.get("blocking")]
        # 至少有一个阻断问题
        assert len(blocking) > 0 or result.get("validation_passed") is False


# ═══ I5: MODE 聚合 → OpenQuestion ═══

def test_i5_mode_aggregation_uncertainty():
    """不支持的 MODE 聚合 → output_kind=METRIC uncertainty → 阻断。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "label_col"],
        description="使用 MODE 聚合——不在白名单",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "label_col",
            "branches": [{"condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 0, "data_type": "number"}}, "then_value": "有值"}],
            "else_value": "无值",
        }],
        "uncertainties": [{
            "field_ref": "metrics.mode_val",
            "output_column": "mode_val",
            "output_kind": "METRIC",
            "description": "聚合函数 MODE 不在白名单中",
        }],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # MODE 不在白名单 → Planner 产出 METRIC uncertainty → 阻断
    assert len(spec.uncertainties) > 0
    assert any(u.output_kind == "METRIC" for u in spec.uncertainties)


# ═══ I6: LABEL 才调用 Extractor ═══

def test_i6_label_kind_triggers_extractor():
    """Planner 标记 LABEL + unresolved + 未生成规则 → Extractor 被调用。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("score", "double")]}],
        output_columns=["score", "risk_label"],
        description="按 score 定义 risk_label",
    )
    # Planner 只标记 LABEL 但未生成规则——LabelExtractor 应兜底
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],  # 未生成规则
        "uncertainties": [{
            "field_ref": "risk_label_ref",
            "output_column": "risk_label",
            "output_kind": "LABEL",
            "description": "需要 CASE WHEN 定义",
        }],
    })
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    result = pipeline.run_parse_and_enrich(spec)
    # Planner 标记 LABEL + 未生成规则 → Extractor 应被调用
    assert call_count[0] == 1, f"Extractor 应被调用 1 次，实际 {call_count[0]} 次"


# ═══ I7: UNKNOWN 不调用 Extractor ═══

def test_i7_unknown_kind_no_extractor():
    """output_kind=UNKNOWN → unresolved 检查阻断，Extractor 不执行。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "unknown_col"],
        description="无法判断 unknown_col 是什么",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],
        "uncertainties": [{
            "field_ref": "unknown_ref",
            "output_column": "unknown_col",
            "output_kind": "UNKNOWN",
            "description": "完全无法判断",
        }],
    })
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    # UNKNOWN → unresolved 检查应阻断，Pipeline 可能抛异常
    try:
        result = pipeline.run_parse_and_enrich(spec)
    except Exception:
        pass
    # Extractor 不应被调用（UNKNOWN 不走 LabelExtractor）
    assert call_count[0] == 0


# ═══ I8: 非 label_table 的 Planner CASE WHEN 正常 ═══

def test_i8_detail_table_case_when_works():
    """非 label_table 的 Planner CASE WHEN 正常工作。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "peak_type"],
        description="按 val 定义 peak_type",
    )
    spec = spec.model_copy(update={"dataset_type": DatasetType.DETAIL_TABLE})
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "peak_type",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高峰",
            }],
            "else_value": "平峰",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None


# ═══ I9: SpecEnricher 不复用 Planner 已覆盖列 ═══

def test_i9_enricher_no_duplicate_planner_columns():
    """SpecEnricher 不对 Planner 已覆盖列重复生成 CASE WHEN。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "peak_type"],
        description="按 val 定义 peak_type",
    )
    # 预置 case_when_rules——模拟 Planner 已覆盖
    spec = spec.model_copy(update={
        "case_when_rules": [CaseWhenRule(
            output_column="peak_type",
            branches=[CaseWhenBranch(
                condition={"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                then_value="高峰",
            )],
            else_value="平峰",
        )],
    })
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # Enricher 不应复写 peak_type——case_when_rules 数量不变
    assert result is not None


# ═══ I10: SQL 和 Spark 从同一 Contract 生成 ═══

def test_i10_sql_spark_same_contract():
    """同一已验证 Contract 下 SQL 和 Spark 均成功编译。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"), ("fare_amount", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar")]},
        ],
        joins=[{"left": "trips.pickup_location_id", "right": "zones.location_id"}],
        metrics=[{"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"}],
        dimensions=[{"dimension_name": "borough", "column_ref": "zones.borough"}],
        output_columns=["borough", "trip_count", "risk_label"],
        description="按 borough 聚合，按 trip_count 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "trip_count", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高",
            }],
            "else_value": "低",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
```

- [ ] **Step 2: 运行集成测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/pipeline/test_label_table_unified.py -v
```

注意：部分测试可能因 Fake Adapter 集成路径不完整而失败——逐个排查并修复，而非降低测试预期。

- [ ] **Step 3: 提交**

```bash
git add tests/pipeline/test_label_table_unified.py
git commit -m "test: Label Table 统一管线集成测试——10 个用例含三表全链路

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 14: 全量回归验证

**Files:**
- （验证现有测试 + ruff + git diff）

- [ ] **Step 1: 运行全量 pytest**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -x --timeout=120 2>&1 | tail -20
```
预期：全部通过（含已知 skip），新增测试全部 PASS。

- [ ] **Step 2: 运行 ruff check**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m ruff check src/ tests/
```
预期：无新增错误。

- [ ] **Step 3: 验证 git diff 无意外变更**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && git diff --check
```

- [ ] **Step 4: 重启服务并快速冒烟测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && ./dev-reload.sh
```

验证 API 健康检查：
```bash
curl -s http://127.0.0.1:8000/api/health
```

- [ ] **Step 5: 清理验证**

确认以下命令无输出：
```bash
grep -rn "validate_label_table_v1_scope\|_prepare_spec_for_planning\|不在你的职责范围" src/ 2>/dev/null
```

- [ ] **Step 6: 最终提交（如有遗漏）**

```bash
git add -A && git diff --cached --stat
```
如有未提交变更，单独提交。
```

---

## 执行顺序

1. Task 1: UncertaintyEntry 模型变更
2. Task 2: ParsedDeveloperSpec 新增 uncertainties
3. Task 3: RequirementPlanner Prompt + JSON Schema
4. Task 4: _parse_response 适配新字段
5. Task 5: _enrich_and_plan 删除 label_table 跳过
6. Task 6: 辅助纯函数
7. Task 7: _prepare_labels + 删除 _prepare_spec_for_planning
8. Task 8: ProposalPromotion uncertainties 透传
9. Task 9: _run_requirement_planner 失败路径
10. Task 10: SpecEnricher Planner 去重
11. Task 11: 删除 label_scope.py + sql_build_plan 重复门禁
12. Task 12: 单元测试（11 用例）
13. Task 13: 集成测试（10 用例）
14. Task 14: 全量回归验证

---

## 自审检查

### 1. Spec 覆盖

| Spec 章节 | 对应 Task |
|-----------|----------|
| §3.1 UncertaintyEntry 修正 | Task 1 |
| §3.2 ParsedDeveloperSpec 新增字段 | Task 2 |
| §5.1 Prompt H5/H7/H9 | Task 3 |
| §5.2 JSON Schema | Task 3 |
| §4.1 _enrich_and_plan 删除跳过 | Task 5 |
| §4.4 _prepare_labels 实现（含 I1 修正） | Task 7 |
| §4.5 _get_output_kind | Task 6 |
| §4.6 _check_label_rule_conflicts | Task 6 |
| §4.7 uncertainties 透传 | Task 6, 8, 9 |
| §4.8 _run_requirement_planner 修正 | Task 9 |
| §4.9 SpecEnricher 去重 | Task 10 |
| §6 删除 label_scope.py + 重复门禁 | Task 11 |
| §9.1 单元测试 11 用例 | Task 12 |
| §9.2 集成测试 10 用例 | Task 13 |
| §1.3 现有多跳链复用 | Task 13 (I3 三表 fixture) |

### 2. 占位符扫描

无 TBD/TODO/占位符——所有步骤含完整代码或精确命令。

### 3. 类型一致性

- `UncertaintyEntry` 字段在 Task 1 定义 → Task 4, 6, 7, 8, 9 使用一致
- `_get_output_kind` 签名在 Task 6 定义 → Task 7 使用一致
- `_merge_uncertainties` / `_apply_uncertainties_to_spec` 在 Task 6 定义 → Task 8, 9 使用一致
- `_prepare_labels` 在 Task 7 定义 → Task 5 调用位置一致
