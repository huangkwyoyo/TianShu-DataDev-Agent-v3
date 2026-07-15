# label_table 类型支持——完整设计书（修订版 v2）

> **状态**：设计阶段，待审批。**不执行。**
>
> 修订记录：
> - v2 (2026-07-15)：9 项修订——硬阻断、溯源分离、discriminator AST、Validator 完善、LabelDomain、
>   _prepare_spec_for_planning()、Builder 真模型对齐、E2E Contract 验收、Fake Adapter

## 1. 问题定义

### 1.1 现象

Template 2（行程距离分类标签）执行 `execute` 阶段失败：

```
Binder Error: Referenced column "distance_category" not found in FROM clause!
Candidate bindings: "distance_miles", "is_distance_outlier", ...
```

### 1.2 根因链（三层缺失）

| 层 | 位置 | 缺失内容 |
|----|------|----------|
| **Parser** | `parser.py:248-262` `ParsedDeveloperSpec()` | YAML `type: label_table` 被读入 `spec_dict` 但从未存入模型——`ParsedDeveloperSpec` 无 `type`/`dataset_type` 字段 |
| **Parser** | `parser.py:238` | Markdown body 中的 CASE WHEN 逻辑存入 `description`（纯文本），未结构化为 `compute_steps` 或 `CaseWhenDecl` |
| **Builder** | `sql_build_plan.py:2216-2244` `_build_project_step()` | 所有 `output_columns` 被当作源表物理列，生成 `ColumnRef(column_name="distance_category")` |

**结果**：`SELECT distance_category FROM gold.fact_trips` → DuckDB 报字段不存在。

### 1.3 本质

`label_table` 类型在管线中**完全未实现**。`CaseWhenDecl`、`CaseWhenBranchDecl`、`ComputeStep.case_when` 等数据结构已存在，`CaseWhenLabelSpec` 在 Spark 管线中有使用，但 SQL 管线缺少从 YAML/Markdown → 结构化标签规则 → SQL CASE WHEN 的完整链路。

**v2 核心修正**：未解析派生输出列必须在 Builder 前**硬阻断**（`DERIVED_COLUMN_RULE_MISSING`），绝不允许静默回退为 `ColumnRef` 引用——回退意味着把未验证的业务逻辑当作物理列，造成静默错误。

---

## 2. 架构概览

### 2.1 修正后管线（v2）

```
自然语言项目书（YAML front matter + Markdown body）
  │
  ▼
Parser（确定性，不调 LLM）
  ├─ 保留 spec_dict["type"] → DatasetType 枚举
  ├─ 新增：ParsedDeveloperSpec.dataset_type 字段
  └─ 产出：ParsedDeveloperSpec
  │
  ▼
SourceManifest 解析
  │
  ▼
SpecEnricher（现有，职责不变）
  ├─ 指标推断、窗口指标、Join 推理
  └─ 产出：增强后 ParsedDeveloperSpec
  │
  ▼
_prepare_spec_for_planning()（新增，共享入口）        ◀── v2 新增
  ├─ 覆盖 plan / execute / run_all / execute_rich 全部入口
  ├─ 调用 _find_unresolved_derived_columns()
  ├─ 未解析列非空 → 触发 LabelExtractor
  └─ 未解析列非空 且 非 LABEL_TABLE → W008 警告 + 仍尝试提取
  │
  ▼
LabelExtractor（新增，LLM）
  ├─ 触发条件：存在"未解析派生输出列"
  ├─ 仅对未解析列调用 LLM
  ├─ LLM 输出：list[LabelRuleProposal]（候选，不可执行）
  ├─ 产出 LabelExtractionArtifact（溯源信息：模型/Prompt/时间）  ◀── v2 新增
  └─ 已有结构化规则 → 跳过 LLM，直接进入 Validator
  │
  ▼
LabelRuleValidator（新增，确定性）                        ◀── v2 大幅完善
  ├─ 字段存在性 + 类型兼容性
  ├─ 操作符合法性
  ├─ 输出类型检查
  ├─ 原文 evidence 锚定验证
  ├─ LabelDomain 提取与验证
  ├─ ELSE 或完整覆盖证明
  ├─ 区间重叠/遗漏检测
  └─ 无法证明 → 阻断 或 HUMAN_REVIEW
  │
  ▼
Promotion（新增，确定性）
  ├─ Proposal → CaseWhenDecl 提升
  ├─ 产出 LabelPromotionArtifact（溯源信息：parent_hash/proposal_id/时间）
  ├─ 重新计算 spec_hash（统一重算，不含溯源信息）
  └─ 语义 Spec 只保存确定性规则
  │
  ▼
Builder（现有，新增 CaseWhenStep 插入逻辑 + 硬阻断）    ◀── v2 硬阻断
  ├─ Scan → Filter → Aggregate? → CaseWhenStep → Project → Sort → Limit
  ├─ 检测 output_column 是否有对应 CaseWhenDecl
  ├─ 有 → 插入 CaseWhenStep；无 → 检查是否源表物理列
  └─ 既无 CaseWhenDecl 也非物理列 → DERIVED_COLUMN_RULE_MISSING 阻断
  │
  ▼
Compiler → Execute（现有，无需改动）
  │
  ▼
Contract 抽取 → SparkCaseWhenStep → SQL/Spark 同快照对比  ◀── v2 新增 E2E
```

### 2.2 核心设计原则

1. **`DatasetType` 做门禁，不驱动 Builder 分叉**：DatasetType 决定验证策略和必需能力检查，Builder 保持统一的 IR 驱动——有 `CaseWhenDecl` 则生成 `CaseWhenStep`，没有就当普通列引用
2. **LLM 产出候选，确定性验证后提升**：LLM 不直接输出可执行 `CaseWhenDecl`，而是输出 `LabelRuleProposal`；只有 `LabelRuleValidator` 验证通过后，才提升为 `CaseWhenDecl`
3. **禁止自由 SQL 字符串**：LLM 只能生成带 discriminator 的封闭 `LabelPredicateNode` 联合 AST，禁止 `when: "distance_miles <= 2"` 和 `raw_condition`
4. **溯源与语义分离**：LLM 模型/Prompt/时间等溯源信息存入独立 `LabelExtractionArtifact`/`LabelPromotionArtifact`；语义 Spec 只保存确定性规则，`spec_hash` 统一重算
5. **未解析列硬阻断**：任何未解析派生输出列必须在 Builder 前阻断（`DERIVED_COLUMN_RULE_MISSING`），禁止回退为 `ColumnRef`——宁可阻断也不要静默错误
6. **共享入口函数**：`_prepare_spec_for_planning()` 覆盖全部 plan/execute/run_all/rich 入口

---

## 3. 数据模型设计

### 3.1 `DatasetType` 枚举

**文件**：`src/tianshu_datadev/developer_spec/models.py`

```python
class DatasetType(str, Enum):
    """数据产品类型——决定验证策略和能力门禁，不驱动 Builder 代码路径分叉。"""
    DETAIL_TABLE = "detail_table"       # 明细表：Scan → Filter → Project
    AGGREGATE_TABLE = "aggregate_table" # 聚合表：Scan → Filter → Aggregate → Project
    LABEL_TABLE = "label_table"         # 标签表：Scan → Filter → CaseWhen → Project
    UNSPECIFIED = "unspecified"         # 过渡期兼容——产生 W007 迁移警告
```

**关键约束**：
- `UNSPECIFIED` 不可静默当 `DETAIL_TABLE` 处理——必须产生 `W007` 迁移警告
- `ParsedDeveloperSpec.dataset_type` 最终目标为必填（Phase 1 允许 `UNSPECIFIED` + 警告，Phase 2 升级为必填）
- 纳入 `_normalized_spec_hash` 计算

### 3.2 `ParsedDeveloperSpec` 改动

**文件**：`src/tianshu_datadev/developer_spec/models.py:724`

```python
class ParsedDeveloperSpec(StrictModel):
    # ... 现有字段保持不变 ...
    dataset_type: DatasetType = DatasetType.UNSPECIFIED  # 新增——数据产品类型
    label_rules: list[CaseWhenDecl] = []                  # 新增——标签规则（已验证，仅含确定性信息）
    # 注意：v1 中的 parent_spec_hash 已移入 LabelPromotionArtifact
```

**溯源信息分离原则**：`ParsedDeveloperSpec` 只保留确定性语义信息。LLM 模型名、Prompt 版本、提取时间、proposal hash 等溯源元信息全部存入独立 Artifact（见 §3.8、§3.9）。

### 3.3 `CompareOp` 枚举

```python
class CompareOp(str, Enum):
    """比较操作符——封闭集合。"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
```

### 3.4 `LabelPredicateNode`——带 discriminator 的封闭联合 AST（v2 重写）

**文件**：`src/tianshu_datadev/developer_spec/models.py`（新建）

**设计原则**：
- 每个 `node_type` 对应一个独立 StrictModel 子类——**禁止** Optional 字段大杂烩
- 使用 Pydantic `Annotated[Union[...], Field(discriminator="node_type")]` 实现封闭联合
- Literal 值使用真实 Python 类型（`str | Decimal | bool | None`），不包装为 TypedLiteral

```python
from decimal import Decimal
from typing import Annotated, Union, Literal
from pydantic import Field


class LabelColumnRef(StrictModel):
    """列引用叶子——引用源表中已声明的字段。"""
    node_type: Literal["COLUMN_REF"] = "COLUMN_REF"
    column_name: str  # 归一化后的列名


class LabelTypedLiteral(StrictModel):
    """类型化字面量——真实 Python 类型，禁止隐式转换。"""
    node_type: Literal["LITERAL"] = "LITERAL"
    value: str | Decimal | bool | None  # 真实类型：字符串/数值/布尔/NULL
    data_type: Literal["string", "number", "boolean", "null"]


class LabelCompare(StrictModel):
    """二元比较：left OP right。"""
    node_type: Literal["COMPARE"] = "COMPARE"
    left: str           # 列名（归一化后）
    op: CompareOp       # 比较操作符
    right: LabelTypedLiteral


class LabelIsNull(StrictModel):
    """IS NULL 检查。"""
    node_type: Literal["IS_NULL"] = "IS_NULL"
    column: str


class LabelIsNotNull(StrictModel):
    """IS NOT NULL 检查。"""
    node_type: Literal["IS_NOT_NULL"] = "IS_NOT_NULL"
    column: str


class LabelAnd(StrictModel):
    """逻辑 AND——至少 2 个子节点。"""
    node_type: Literal["AND"] = "AND"
    children: list["LabelPredicateNode"]


class LabelOr(StrictModel):
    """逻辑 OR——至少 2 个子节点。"""
    node_type: Literal["OR"] = "OR"
    children: list["LabelPredicateNode"]


class LabelNot(StrictModel):
    """逻辑 NOT——单子节点。"""
    node_type: Literal["NOT"] = "NOT"
    child: "LabelPredicateNode"


# ── 带 discriminator 的封闭联合类型 ──

LabelPredicateNode = Annotated[
    Union[
        LabelAnd,
        LabelOr,
        LabelNot,
        LabelCompare,
        LabelIsNull,
        LabelIsNotNull,
        LabelColumnRef,
        LabelTypedLiteral,
    ],
    Field(discriminator="node_type"),
]
```

**Template 2 覆盖性验证**：

| Markdown 条件 | LabelPredicateNode 表达 |
|---------------|------------------------|
| `distance_miles IS NULL` | `LabelIsNull(column="distance_miles")` |
| `is_distance_outlier = true` | `LabelCompare(left="is_distance_outlier", op=CompareOp.EQ, right=LabelTypedLiteral(value=True, data_type="boolean"))` |
| `distance_miles <= 2` | `LabelCompare(left="distance_miles", op=CompareOp.LTE, right=LabelTypedLiteral(value=Decimal("2"), data_type="number"))` |
| `distance_miles > 2 AND distance_miles <= 5` | `LabelAnd(children=[LabelCompare(left="distance_miles", op=CompareOp.GT, right=LabelTypedLiteral(value=Decimal("2"), data_type="number")), LabelCompare(left="distance_miles", op=CompareOp.LTE, right=LabelTypedLiteral(value=Decimal("5"), data_type="number"))])` |
| `IS NULL OR = true`（组合） | `LabelOr(children=[LabelIsNull(column="distance_miles"), LabelCompare(left="is_distance_outlier", op=CompareOp.EQ, right=LabelTypedLiteral(value=True, data_type="boolean"))])` |

**与 v1 的关键差异**：
- v1：单个 `LabelPredicateNode` 类 + 全部 Optional 字段（`left: str | None`, `op: CompareOp | None`, `right: LabelTypedLiteral | None`, `column: str | None`, `literal: LabelTypedLiteral | None`, `column_ref: str | None`）
- v2：8 个独立子类 + discriminator——Pydantic 自动根据 `node_type` 选择正确子类，拒绝非法字段组合
- v2：`LabelTypedLiteral.value` 使用 `str | Decimal | bool | None`（真实 Python 类型），v1 使用 `value: str`（全部字符串化）
- v2：禁止 `when`/`raw_condition` 字符串路径——LLM 只能输出结构化 AST

### 3.5 `LabelRuleProposal`——LLM 候选

**文件**：`src/tianshu_datadev/developer_spec/models.py`（新建）

```python
class LabelBranchProposal(StrictModel):
    """单条 WHEN-THEN 候选——LLM 输出。"""
    condition: LabelPredicateNode    # 结构化谓词（discriminator 联合，非字符串）
    then_label: str                  # 结果标签值
    evidence: str = ""               # LLM 从 Markdown 中引用的原文证据（必须非空）


class LabelRuleProposal(StrictModel):
    """LLM 提取的标签规则候选——不可执行，必须经 LabelRuleValidator 验证后提升。

    一个 Proposal 对应 output_spec.columns 中的一个标签列。
    溯源信息不在本模型中——见 LabelExtractionArtifact。
    """
    proposal_id: str                          # proposal_{sha256[:12]}
    source_spec_hash: str                     # 来源 Spec hash（提取时的快照）
    output_column: str                        # 目标输出列名（对应 output_spec.columns）
    branches: list[LabelBranchProposal]       # WHEN-THEN 候选分支
    else_value: str | None = None             # ELSE 默认值
    # 注意：llm_model / llm_prompt_version 已移入 LabelExtractionArtifact
```

### 3.6 `LabelDomain`——从原文提取的标签值域（v2 新增）

**文件**：`src/tianshu_datadev/developer_spec/models.py`（新建）

```python
class LabelDomain(StrictModel):
    """从 Markdown 原文中提取的标签值域——由 Agent（LabelExtractor）提取，
    由 LabelRuleValidator 确定性验证。

    不要求程序员在 output_columns 中手写 enum——allowed_values 保持可选。
    """
    values: list[str]                         # 标签域中的全部可能值
    source_evidence: str = ""                 # 原文中定义这些值的片段（用于审查锚定）
    is_exhaustive: bool = False               # 原文是否声称该域是完备的
    completeness_evidence: str = ""           # 完备性声明的原文证据
```

### 3.7 `CaseWhenDecl` 改造（v2 精简）

**文件**：`src/tianshu_datadev/developer_spec/models.py:534`

现有字段保留（向后兼容已有 `compute_steps`），新增类型化分支：

```python
class CaseWhenDecl(StrictModel):
    """CASE WHEN 声明——已验证的标签规则（仅含确定性信息）。

    两种使用场景：
    1. compute_steps 合流步骤的 case_when 字段（现有）
    2. ParsedDeveloperSpec.label_rules（新增——LabelExtractor → Validator → Promotion 链路）

    溯源信息（proposal_id、promotion_time）已移入 LabelPromotionArtifact——
    CaseWhenDecl 本身只保留确定性规则，确保 spec_hash 仅依赖语义内容。
    """
    # ── 现有字段保持不变 ──
    branches: list[CaseWhenBranchDecl] = []
    else_value: str | None = None
    output_column: str = ""

    # ── 新增：类型化分支（LLM 提取 + 确定性验证后填充）──
    typed_branches: list[LabelPredicateBranch] = []


class LabelPredicateBranch(StrictModel):
    """已验证的类型化 WHEN-THEN 分支——仅含确定性信息。"""
    condition: LabelPredicateNode
    then_label: str
```

### 3.8 `LabelExtractionArtifact`——提取溯源（v2 新增）

**文件**：`src/tianshu_datadev/labels/artifacts.py`（新建）

```python
class LabelExtractionArtifact(StrictModel):
    """LabelExtractor 阶段的溯源记录——与语义 Spec 分离存储。

    包含 LLM 调用参数、原始候选、提取时间等元信息。
    不进入 spec_hash 计算——仅用于审计追溯和 Harness 回归。
    """
    artifact_id: str                           # extract_{sha256[:12]}
    source_spec_hash: str                      # 提取时的 Spec 快照 hash
    extraction_time: str                       # ISO 8601 时间戳
    llm_model: str                             # 使用的 LLM 模型标识
    llm_prompt_version: str                    # Prompt 模板版本
    llm_temperature: float                     # LLM 温度参数
    unresolved_columns: list[str]              # 触发提取的未解析列
    raw_proposals: list[LabelRuleProposal]     # LLM 原始候选（验证前）
    prompt_snapshot: str = ""                  # 实际发送给 LLM 的 Prompt 文本快照（可选）
```

### 3.9 `LabelPromotionArtifact`——提升溯源（v2 新增）

**文件**：`src/tianshu_datadev/labels/artifacts.py`（新建）

```python
class LabelPromotionArtifact(StrictModel):
    """Promotion 阶段的溯源记录——记录 Proposal → CaseWhenDecl 的转换。

    包含验证报告、父 hash 链、提升时间等审计信息。
    不进入 spec_hash 计算。
    """
    artifact_id: str                           # promote_{sha256[:12]}
    parent_spec_hash: str                      # Promotion 前的原始 spec_hash（溯源链）
    new_spec_hash: str                         # Promotion 后重算的 spec_hash
    promotion_time: str                        # ISO 8601 时间戳
    extraction_artifact_id: str                # 关联的 LabelExtractionArtifact
    promoted_rules: list[CaseWhenDecl]         # 提升后的确定性规则
    validation_reports: list[LabelValidationReport]  # 每个 Proposal 的验证报告
    rejected_proposals: list[str] = []         # 被拒绝的 proposal_id 列表
    human_review_required: bool = False        # 是否有规则需要人工审查
```

---

## 4. 管线阶段设计

### 4.1 管线编排 + 共享入口 `_prepare_spec_for_planning()`（v2 新增）

**文件**：`src/tianshu_datadev/api/pipeline.py`

**问题**：当前 `execute_rich` / `run_all` 各自独立编排管线阶段，LabelExtractor 只在一处插入会导致其他入口遗漏。

**方案**：抽取共享函数 `_prepare_spec_for_planning()`，覆盖全部入口：

```python
def _prepare_spec_for_planning(
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest | None = None,
    label_extractor: LabelExtractor | None = None,
    label_validator: LabelRuleValidator | None = None,
    promoter: Promotion | None = None,
) -> tuple[ParsedDeveloperSpec, LabelExtractionArtifact | None, LabelPromotionArtifact | None]:
    """为 Builder 准备 Spec——在所有 plan/execute/run_all 入口共享调用。

    管线阶段：
    1. 检测未解析派生输出列
    2. 如有未解析列 → 调用 LabelExtractor（LLM）
    3. LabelRuleValidator 确定性验证
    4. Promotion 提升为 CaseWhenDecl
    5. 重新计算 spec_hash

    Returns:
        (增强后 Spec, 提取溯源 Artifact 或 None, 提升溯源 Artifact 或 None)
    """
    unresolved = _find_unresolved_derived_columns(spec, manifest)

    if not unresolved:
        return spec, None, None

    # 非 LABEL_TABLE + 未解析列 → W008 警告
    if spec.dataset_type != DatasetType.LABEL_TABLE:
        logger.warning(f"W008: 检测到未解析派生输出列 {unresolved}，"
                       f"但 dataset_type={spec.dataset_type}，非 label_table")

    # LabelExtractor
    extractor = label_extractor or LabelExtractor()
    proposals, extraction_artifact = extractor.extract(spec, unresolved)

    # LabelRuleValidator
    validator = label_validator or LabelRuleValidator()
    reports = [validator.validate(p, spec) for p in proposals]

    # Promotion
    prom = promoter or Promotion()
    new_spec, promotion_artifact = prom.promote(spec, proposals, reports)

    return new_spec, extraction_artifact, promotion_artifact
```

**调用方**（统一替换为调用此共享函数）：
- `execute_rich()` — 交互式管线
- `run_all()` — 批处理管线
- `plan()` — 规划入口
- 任何其他需要 Spec → Builder 的入口

### 4.2 触发条件——`_find_unresolved_derived_columns()`

**不**使用 `dataset_type == LABEL_TABLE and compute_steps is None`——太粗。

改用**"未解析派生输出列"**检测：

```python
def _find_unresolved_derived_columns(
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest | None = None,
) -> list[str]:
    """找出 output_columns 中既不在源表也不在任何结构化规则中的列。

    已解析条件（任一满足即认为已解析）：
    - 列名存在于任意 input_table 的 columns/key_columns/business_columns 中
    - 列名匹配某个 metric.alias
    - 列名匹配某个 window_metric 的输出
    - 列名匹配某个 compute_step.output_alias / case_when.output_column
    - 列名匹配 spec.label_rules[*].output_column
    - 列名在 SourceManifest 的 table_schemas 中存在（manifest 可用时）

    Returns:
        未解析的输出列名列表——这些列需要 LLM 提取标签规则。
        返回空列表时跳过 LabelExtractor。
    """
    resolved: set[str] = set()

    # 源表字段
    for t in spec.input_tables:
        for c in t.columns:
            resolved.add(c.normalized_name)
        for c in t.key_columns:
            resolved.add(c.normalized_name)
        for c in t.business_columns:
            resolved.add(c.normalized_name)

    # SourceManifest schema（若可用）
    if manifest:
        for ts in manifest.table_schemas:
            for col_name in ts.column_names:
                resolved.add(normalize(col_name))

    # 指标 alias
    for m in spec.metrics:
        if m.alias:
            resolved.add(m.alias)

    # 窗口指标
    for wm in spec.inferred_window_metrics:
        resolved.add(wm.metric_name)

    # compute_steps 产出
    if spec.compute_steps:
        for cs in spec.compute_steps:
            if cs.output_alias:
                resolved.add(cs.output_alias)
            if cs.case_when and cs.case_when.output_column:
                resolved.add(cs.case_when.output_column)

    # 已有 label_rules
    for lr in spec.label_rules:
        if lr.output_column:
            resolved.add(lr.output_column)

    unresolved = []
    for col in spec.output_spec.columns:
        normalized = normalize(col.name)
        if normalized not in resolved:
            unresolved.append(col.name)

    return unresolved
```

### 4.3 LabelExtractor

**文件**：`src/tianshu_datadev/labels/label_extractor.py`（新建）

```
职责：从 Markdown body 中提取 CASE WHEN 标签规则 + LabelDomain
输入：ParsedDeveloperSpec + 未解析列名列表
输出：(list[LabelRuleProposal], LabelExtractionArtifact)
LLM：是
```

**Prompt 设计要点**：
- 输入上下文：Markdown body + 相关 output_column 的 name/type/description + 源表可用字段列表
- 输出格式：严格 JSON Schema（`LabelRuleProposal` 结构 + `LabelDomain` 提取）
- 要求 LLM 输出**带 discriminator 的结构化谓词 AST**（`LabelPredicateNode` 联合类型），不是自由文本
- 每个分支必须附带 `evidence`——**逐字引用** Markdown 原文片段
- LLM 还需从原文中提取 `LabelDomain`——列出所有可能的标签值及原文出处
- 温度设低（0.1~0.2），保证提取一致性
- **禁止** LLM 输出 `when` 字符串或 `raw_condition`——Validator 会拒绝

**LabelDomain 提取要点**：
- Agent 从 Markdown 原文中识别标签值域定义（如 "分为四类：unknown / short / medium / long"）
- 提取为 `LabelDomain.values` + `LabelDomain.source_evidence`
- 若原文明确声明完备（如"以上四类覆盖全部情况"），设置 `is_exhaustive=True`
- **不要求程序员在 output_columns 中补写 enum**——`allowed_values` 保持可选

**错误处理**：
- LLM 返回无法解析的 JSON → 生成 `OpenQuestion(blocking=True)`
- LLM 超时 → 重试一次，仍失败则生成 `OpenQuestion(blocking=True)`
- LLM 输出了字符串条件（`when`/`raw_condition`）而非 discriminator AST → Validator 拒绝
- 提取结果写入 `LabelExtractionArtifact` 供审计

### 4.4 LabelRuleValidator（v2 大幅完善）

**文件**：`src/tianshu_datadev/labels/label_rule_validator.py`（新建）

```
职责：确定性验证 LabelRuleProposal，决定是否可提升为 CaseWhenDecl
输入：LabelRuleProposal + ParsedDeveloperSpec（含源表字段清单）
输出：LabelValidationReport（pass/fail/human_review + 8 项检查详情）
LLM：否——纯确定性逻辑
```

**检查项**（全部 8 项，按阻断级别分级。注：用户需求中"字段与类型"在此拆为 #1 字段存在性 + #2 字段类型兼容性——两者失败码不同（`INVALID_FIELD` vs `TYPE_MISMATCH`），合并会丢失诊断精度）：

| # | 检查项 | 说明 | 失败级别 |
|---|--------|------|----------|
| 1 | **字段存在性** | `condition` 中引用的所有 `column_name`/`column` 字段必须在 source_table 已声明列或 Manifest schema 中存在 | **阻断**——`INVALID_FIELD` |
| 2 | **字段类型兼容性** | `LabelTypedLiteral.data_type` 与源表字段声明的 `data_type` 兼容（number→number, string→string, boolean→boolean）。若源表无类型声明，从 Manifest schema 推断 | **阻断**——`TYPE_MISMATCH` |
| 3 | **操作符合法性** | 所有 `CompareOp` 必须是 `CompareOp` 枚举成员；`node_type` 必须是合法的 discriminator 值；逻辑节点子节点数≥1（AND/OR≥2） | **阻断**——`INVALID_OPERATOR` |
| 4 | **输出类型** | `then_label` 的类型（string/number/boolean）必须与 `output_spec.columns` 中声明的输出列类型一致 | **阻断**——`OUTPUT_TYPE_MISMATCH` |
| 5 | **原文 evidence 锚定** | 每个 `LabelBranchProposal.evidence` 必须非空，且必须能在 Markdown body 中找到对应原文（子串匹配或模糊匹配）。**无法锚定到原文的分支不可信任。** | **阻断**——`EVIDENCE_NOT_ANCHORED` |
| 6 | **标签域验证** | 所有 `then_label` 值必须在 `LabelDomain.values` 内，或补充解释为何超出（如 LLM 扩展）。若 `is_exhaustive=True`，则所有分支的 `then_label` 合集必须覆盖 `LabelDomain.values` | **阻断**——`LABEL_DOMAIN_VIOLATION` |
| 7 | **ELSE 或完整覆盖证明** | 若有 `else_value` → 通过（ELSE 覆盖了剩余情况）。若无 ELSE → 必须提供**完整覆盖证明**：所有分支条件取反后的并集覆盖全集（对于有限枚举域，可通过检查 `then_label` 合集覆盖 `LabelDomain.values` 来近似证明） | **阻断**——`INCOMPLETE_COVERAGE`；若仅疑似不完整 → **HUMAN_REVIEW** |
| 8 | **区间重叠/遗漏检测**（数值型条件） | 对于同一列的多条数值区间条件（如 `<=2`, `>2 AND <=5`, `>5 AND <=10`），检测区间之间是否有重叠或遗漏。方法：将区间端点排序后检查相邻区间是否连续 | 重叠 → **阻断**（`INTERVAL_OVERLAP`）；遗漏 → **HUMAN_REVIEW**（`INTERVAL_GAP`） |

**阻断级别定义**：
- **阻断（BLOCKING）**：Proposal 不可 Promotion，必须由程序员修改或修复后重新提取
- **HUMAN_REVIEW**：Proposal 可以暂存但不可自动 Promotion，需人工在审查界面确认
- **WARN**：非阻断警告，记录但不阻止 Promotion

**返回值**：

```python
class LabelValidationReport(StrictModel):
    proposal_id: str
    passed: bool                              # 全部检查通过（无阻断、无 HUMAN_REVIEW）
    checks: list[LabelValidationCheck]        # 逐项检查结果
    blocking_errors: list[str] = []           # 阻断性错误详情
    human_review_items: list[str] = []         # 需人工审查的项目
    warnings: list[str] = []                  # 非阻断警告
    extracted_label_domain: LabelDomain | None = None  # Validator 验证过的 LabelDomain

class LabelValidationCheck(StrictModel):
    check_name: str  # FIELD_EXISTS / TYPE_COMPATIBLE / OPERATOR_VALID /
                     # OUTPUT_TYPE / EVIDENCE_ANCHORED / LABEL_DOMAIN /
                     # COVERAGE_COMPLETENESS / INTERVAL_OVERLAP / INTERVAL_GAP
    passed: bool
    level: Literal["BLOCKING", "HUMAN_REVIEW", "WARN"]
    detail: str = ""
```

**与现有 `validate_label_enums()` 的关系**：

| 验证器 | 位置 | 职责 |
|--------|------|------|
| `LabelRuleValidator` | Promotion 前 | Proposal → CaseWhenDecl——8 项全面检查 |
| `validate_label_enums()` | Compiler 前 | SqlBuildPlan → 防御性复检——CaseWhenStep 结果枚举与 output_columns enum 一致 |

### 4.5 Promotion（v2 溯源分离）

**文件**：`src/tianshu_datadev/labels/promotion.py`（新建）

```
职责：将验证通过的 LabelRuleProposal 提升为 CaseWhenDecl，产出溯源 Artifact
输入：ParsedDeveloperSpec + list[LabelRuleProposal] + list[LabelValidationReport]
输出：(增强后 ParsedDeveloperSpec, LabelPromotionArtifact)
LLM：否——纯确定性
```

**关键操作**：

1. **构建 CaseWhenDecl**（仅含确定性信息）：
   ```python
   new_rules = []
   for proposal, report in zip(proposals, reports):
       if not report.passed:
           continue  # 未通过的 Proposal 不提升
       typed_branches = [
           LabelPredicateBranch(
               condition=bp.condition,  # LabelPredicateNode（已验证）
               then_label=bp.then_label,
           )
           for bp in proposal.branches
       ]
       new_rules.append(CaseWhenDecl(
           output_column=proposal.output_column,
           typed_branches=typed_branches,
           else_value=proposal.else_value,
           # 注意：不包含 proposal_id/promotion_time——已移入 Artifact
       ))
   ```

2. **生成新 Spec**（不原地修改）：
   ```python
   new_spec = ParsedDeveloperSpec(
       **original_spec.model_dump(),
       label_rules=original_spec.label_rules + new_rules,
   )
   ```

3. **统一重算 spec_hash**——只基于确定性语义字段：
   ```python
   new_hash = _normalized_spec_hash(new_spec)
   object.__setattr__(new_spec, "spec_hash", new_hash)
   object.__setattr__(new_spec, "spec_id", f"spec_{new_hash[:12]}")
   ```

4. **构建 LabelPromotionArtifact**（溯源信息独立存储）：
   ```python
   artifact = LabelPromotionArtifact(
       artifact_id=f"promote_{new_hash[:12]}",
       parent_spec_hash=original_spec.spec_hash,
       new_spec_hash=new_hash,
       promotion_time=datetime.utcnow().isoformat(),
       extraction_artifact_id=extraction_artifact.artifact_id,
       promoted_rules=new_rules,
       validation_reports=reports,
       rejected_proposals=[p.proposal_id for p, r in zip(proposals, reports) if not r.passed],
       human_review_required=any(r.human_review_items for r in reports),
   )
   ```

5. **返回** `(new_spec, artifact)`

**溯源链完整路径**：
```
原 Spec (spec_hash_0)
  → LabelExtractionArtifact (source_spec_hash=spec_hash_0, llm_model, prompt_version)
  → LabelRuleProposal (source_spec_hash=spec_hash_0)
  → LabelRuleValidator → LabelValidationReport
  → LabelPromotionArtifact (parent_spec_hash=spec_hash_0, new_spec_hash=spec_hash_1)
  → 新 Spec (spec_hash_1) —— spec_hash 仅依赖确定性语义
```

### 4.6 Builder 改动（v2 真模型对齐 + 硬阻断）

**文件**：`src/tianshu_datadev/planning/sql_build_plan.py`

**原则**：Builder 保持统一 IR 驱动，不按 `DatasetType` 分叉代码路径。但**未解析列必须硬阻断**。

**真实模型引用**（对齐 `planning/models.py` 实际定义）：

| 模型 | 真实字段 | 说明 |
|------|----------|------|
| `CaseWhenStep` | `cases: list[WhenBranch]` | **不是** `branches` |
| `CaseWhenStep` | `else_value: SqlLiteral \| None` | **不是** `else_result` |
| `CaseWhenStep` | `alias: SafeIdentifier` | 输出列别名 |
| `WhenBranch` | `condition: Predicate \| None` | 结构化条件（用这个） |
| `WhenBranch` | `raw_condition: SqlRawExpression \| None` | 字符串模式——**禁止使用** |
| `WhenBranch` | `result: SqlLiteral` | 结果字面量 |

**改动点**：

#### 4.6.1 `_build_single_table()` 插入 `CaseWhenStep` 列表

在 Aggregate 步骤之后、Project 步骤之前插入（`sql_build_plan.py:1429-1490`）：

```python
def _build_single_table(self, spec, ...):
    steps = []
    steps.append(self._build_scan_step(spec, ...))
    steps.append(self._build_filter_step(spec, ...))

    if spec.metrics:
        steps.append(self._build_aggregate_step(spec, ...))

    # 新增：标签规则 → 多个 CaseWhenStep（每个标签列一个）
    case_when_steps = self._build_case_when_steps(spec)
    steps.extend(case_when_steps)

    steps.append(self._build_project_step(spec, ...))
    steps.append(self._build_sort_step(spec))
    if spec.output_spec.limit:
        steps.append(self._build_limit_step(spec))
    return steps
```

#### 4.6.2 新增 `_build_case_when_steps()`

```python
def _build_case_when_steps(self, spec: ParsedDeveloperSpec) -> list[CaseWhenStep]:
    """从 spec.label_rules 构建 CaseWhenStep 列表。

    每个 label_rule（对应一个标签输出列）生成一个独立的 CaseWhenStep。
    使用 WhenBranch.condition（结构化 Predicate），禁止 raw_condition。
    """
    if not spec.label_rules:
        return []

    steps: list[CaseWhenStep] = []
    for rule in spec.label_rules:
        cases: list[WhenBranch] = []
        for tb in rule.typed_branches:
            predicate = self._predicate_from_label_node(tb.condition)
            cases.append(WhenBranch(
                condition=predicate,                              # 结构化 Predicate——非 raw_condition
                result=SqlLiteral(value=tb.then_label),           # SqlLiteral
            ))

        else_value = None
        if rule.else_value is not None:
            else_value = SqlLiteral(value=rule.else_value)

        step_id_content = {
            "output_column": rule.output_column,
            "branch_count": len(cases),
        }
        steps.append(CaseWhenStep(
            step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
            cases=cases,                 # ← 真字段名：cases（非 branches）
            else_value=else_value,       # ← 真字段名：else_value（非 else_result）
            alias=SafeIdentifier(rule.output_column),  # ← SafeIdentifier 类型
        ))

    return steps
```

`_predicate_from_label_node()` 是 `LabelPredicateNode` AST → Planning `Predicate` AST 的转换函数：
- `LabelAnd` → `Predicate(left=..., operator=PredicateOperator.AND, right=...)`
- `LabelOr` → `Predicate(left=..., operator=PredicateOperator.OR, right=...)`
- `LabelNot` → `Predicate(left=..., operator=PredicateOperator.NOT, right=...)`
- `LabelCompare` → `Predicate(left=ColumnRef(...), operator=PredicateOperator(op), right=SqlLiteral(...))`
- `LabelIsNull` → `Predicate(left=ColumnRef(...), operator=PredicateOperator.IS_NULL, right=None)`
- `LabelIsNotNull` → `Predicate(left=ColumnRef(...), operator=PredicateOperator.IS_NOT_NULL, right=None)`

#### 4.6.3 `_build_project_step()` 改动——硬阻断（v2 关键变更）

```python
def _build_project_step(self, spec, ...):
    """列投影步骤——检测每个输出列是否已解析，未解析列硬阻断。

    v2 变更：禁止回退为 ColumnRef——未解析列必须触发 DERIVED_COLUMN_RULE_MISSING。
    """
    # 收集所有 CaseWhenStep 的输出别名
    case_when_aliases: set[str] = set()
    for lr in spec.label_rules:
        case_when_aliases.add(lr.output_column)

    proj_cols = []
    for col in spec.output_spec.columns:
        normalized = normalize(col.name)

        if normalized in case_when_aliases:
            # 标签列——引用 CaseWhenStep 的输出别名
            proj_cols.append(AliasExpr(
                expression=ColumnRef(
                    table_ref=SafeIdentifier(""),
                    column_name=SafeIdentifier(normalized),
                    normalized_name=SafeIdentifier(normalized),
                ),
                alias=SafeIdentifier(col.name),
            ))
        elif self._is_physical_column(normalized, spec):
            # 源表物理列——保持现有行为
            proj_cols.append(AliasExpr(
                expression=ColumnRef(
                    table_ref=SafeIdentifier(""),
                    column_name=SafeIdentifier(normalized),
                    normalized_name=SafeIdentifier(normalized),
                ),
                alias=SafeIdentifier(col.name),
            ))
        else:
            # v2 硬阻断：既非标签列也非物理列——绝不允许静默回退
            raise DerivedColumnRuleMissing(
                column_name=col.name,
                spec_id=spec.spec_id,
                message=(
                    f"输出列 '{col.name}' 既不在源表字段中，也没有对应的 "
                    f"CaseWhenDecl/label_rule。请检查："
                    f"1) 是否遗漏了 type: label_table 声明？"
                    f"2) Markdown body 中是否定义了该列的计算逻辑？"
                ),
            )
    # ...
```

**`DerivedColumnRuleMissing` 异常定义**：

```python
class DerivedColumnRuleMissing(Exception):
    """未解析派生输出列——硬阻断异常。

    当 output_columns 中的列既不是源表物理列，也没有对应的
    CaseWhenDecl / label_rule / compute_step 时抛出。
    调用方必须将此异常转换为 PipelineError(DERIVED_COLUMN_RULE_MISSING)。
    """
    def __init__(self, column_name: str, spec_id: str, message: str):
        self.column_name = column_name
        self.spec_id = spec_id
        self.error_code = "DERIVED_COLUMN_RULE_MISSING"
        super().__init__(message)
```

**行为规则**：
- **有 CaseWhenDecl** → 生成 `ColumnRef` 引用 `CaseWhenStep` 的输出别名
- **源表物理列** → 保持现有行为（`ColumnRef` 直接引用）
- **两者皆非** → **硬阻断**，抛出 `DerivedColumnRuleMissing`，**禁止回退为 ColumnRef**

### 4.7 `_predicate_from_label_node()` —— AST 转换

**文件**：`src/tianshu_datadev/planning/sql_build_plan.py`（新增方法）

将 discriminator 联合 `LabelPredicateNode` 转换为 Planning 层的 `Predicate` AST。一对一映射：

| LabelPredicateNode 子类 | Planning Predicate |
|-------------------------|-------------------|
| `LabelCompare(left, op, right)` | `Predicate(left=ColumnRef(column_name=left), operator=PredicateOperator(op), right=SqlLiteral(value=right.value))` |
| `LabelIsNull(column)` | `Predicate(left=ColumnRef(column_name=column), operator=PredicateOperator.IS_NULL, right=None)` |
| `LabelIsNotNull(column)` | `Predicate(left=ColumnRef(column_name=column), operator=PredicateOperator.IS_NOT_NULL, right=None)` |
| `LabelAnd(children)` | 递归转换 children → 二元合并为 `Predicate(left=..., operator=PredicateOperator.AND, right=...)` |
| `LabelOr(children)` | 同上，`operator=PredicateOperator.OR` |
| `LabelNot(child)` | `Predicate(left=转换(child), operator=PredicateOperator.NOT, right=None)` |

---

## 5. 触发条件与流程控制（v2 硬阻断更新）

### 5.1 `_prepare_spec_for_planning()` 决策树

```
_prepare_spec_for_planning(spec) 被调用：

1. unresolved = _find_unresolved_derived_columns(spec)
   ├─ unresolved 为空 → 跳过全部标签阶段，直接返回 spec
   └─ unresolved 非空 → 继续

2. 对每个 unresolved 列：
   ├─ 调用 LabelExtractor → LabelRuleProposal + LabelExtractionArtifact
   ├─ 调用 LabelRuleValidator.validate(proposal)
   │   ├─ PASS → Promotion.promote(proposal) → CaseWhenDecl + LabelPromotionArtifact
   │   ├─ HUMAN_REVIEW → 记录到 review 报告，不自动提升
   │   └─ BLOCKING → 记录阻断错误，不提升
   └─ 继续下一个 unresolved 列

3. 所有列处理完毕后：
   ├─ 有 BLOCKING 错误 → PipelineError(DERIVED_COLUMN_RULE_MISSING)，管线中止
   ├─ 有 HUMAN_REVIEW 项 → 进入审查流程，不自动进入 Builder
   └─ 全部 PASS → 进入 Builder（CaseWhenStep 已就绪）

4. Builder._build_project_step():
   ├─ 列有 CaseWhenDecl → 引用 CaseWhenStep 输出别名
   ├─ 列是源表物理列 → 引用 ColumnRef
   └─ 两者皆非 → 硬阻断 DERIVED_COLUMN_RULE_MISSING ← v2 新增
```

### 5.2 与 v1 的关键行为差异

| 场景 | v1 行为 | v2 行为 |
|------|---------|---------|
| 未解析列 + LABEL_TABLE | 尝试提取，失败则 OpenQuestion | 提取 → Validator 8 项检查 → 阻断或提升 |
| 未解析列 + 非 LABEL_TABLE | W008 + 宽松模式（失败不阻断） | W008 + 仍尝试提取，**但 Builder 中硬阻断** |
| Builder 遇到既无规则也非物理的列 | 回退为 ColumnRef | **DERIVED_COLUMN_RULE_MISSING 硬阻断** |
| 提取失败 | OpenQuestion(blocking=True) | 阻断 + 提取信息写入 ExtractionArtifact |
| Validator HUMAN_REVIEW | 不存在此级别 | 暂存 Proposal，不自动 Promotion，等待人工确认 |

---

## 6. 迁移策略

### 6.1 分阶段推进

**Phase 1（当前设计实施）**：
1. 定义 `DatasetType` 枚举，`ParsedDeveloperSpec.dataset_type` 默认 `UNSPECIFIED`
2. 实现 discriminator `LabelPredicateNode` 联合 AST + `LabelRuleProposal` + `LabelDomain` 模型
3. 实现 `LabelExtractionArtifact` + `LabelPromotionArtifact`
4. 实现 `LabelExtractor` + `LabelRuleValidator`（8 项检查）+ `Promotion`（溯源分离）
5. 实现 `_prepare_spec_for_planning()` 共享入口
6. Builder 新增 `_build_case_when_steps()` + `_predicate_from_label_node()`
7. Builder `_build_project_step()` 加入 `DerivedColumnRuleMissing` 硬阻断
8. Parser 读取 `spec_dict["type"]` 映射到 `dataset_type`
9. `UNSPECIFIED` 产生 `W007` 迁移警告
10. 实现 `FakeLabelExtractor`（确定性 Fake Adapter）
11. 更新 Template 2 的 YAML（添加 `type: label_table`）

**Phase 2（后续迭代）**：
1. `dataset_type` 升级为必填（移除 `UNSPECIFIED` 默认值）
2. 更新全部内置模板和 fixture
3. 更新黄金样本
4. 将 `dataset_type` 写入 Code Review Package 和 DataTransformContract
5. Contract 抽取自动包含 `case_when_labels` 字段

### 6.2 向后兼容

- `dataset_type = UNSPECIFIED` 的 Spec 按现有行为处理——但如果存在未解析列，Builder 中**仍硬阻断**（v2 变更）
- 已有 `compute_steps` + `case_when` 的 Spec 不受影响（`CaseWhenDecl` 字段只新增不删除）
- `CaseWhenBranchDecl.when: str` 保留但不用于新链路——LLM 不允许产出字符串模式分支
- `WhenBranch.raw_condition` 保留但不用于新链路——Builder 只用 `condition: Predicate`

---

## 7. 验证与测试清单

### 7.1 单元测试（确定性 Fake Adapter）

**v2 核心原则**：
1. pytest 使用**确定性 Fake Adapter**，真实 LLM 调用进入 Harness。`FakeLabelExtractor` 返回预定义的 `LabelRuleProposal` 列表，不依赖网络或 API Key。
2. **优先合并已有测试文件，避免冗余新文件**——每个新增测试在创建前必须先判定是否能合并到已有测试代码中。判定原则：同一被测模块的测试合并到已有测试文件（如 `LabelPredicateNode` 的 discriminator 测试合并到 `tests/developer_spec/test_models.py`；Builder 硬阻断测试合并到 `tests/planning/test_sql_build_plan.py`；Validator 测试合并到已有的 `tests/labels/test_label_rules.py`）。仅当不存在合适的目标文件时才新建测试文件。

| 测试对象 | 测试内容 | Adapter |
|----------|----------|---------|
| `DatasetType` 枚举 | 序列化/反序列化；`UNSPECIFIED` 默认值行为 | 无（纯模型测试） |
| `LabelPredicateNode` discriminator | 每种 `node_type` 子类的构造/验证；非法 discriminator 拒绝；非法字段组合拒绝；递归嵌套 | 无 |
| `LabelRuleProposal` | JSON Schema 验证；evidence 空字符串拒绝 | 无 |
| `LabelRuleValidator` | 8 项检查逐项 PASS/FAIL/HUMAN_REVIEW；边界条件（空 branches、无 ELSE、区间重叠、区间遗漏、证据无法锚定） | FakeLabelExtractor |
| `LabelDomain` 提取 | Validator 验证 extracted LabelDomain vs then_label 合集 | FakeLabelExtractor |
| `Promotion` | spec_hash 重算正确性；溯源 Artifact 字段填充；不修改原 spec | FakeLabelExtractor |
| `_find_unresolved_derived_columns()` | 物理列/指标/窗口指标/compute_step/label_rule/Manifest schema 各场景 | 无 |
| `_build_case_when_steps()` | 有 label_rules → 生成 CaseWhenStep（`cases`/`else_value`/`alias` 字段正确）；无 label_rules → 空列表；Predicate 结构正确 | FakeLabelExtractor |
| `_predicate_from_label_node()` | 每种 discriminator 子类 → 正确 Predicate；AND/OR 嵌套递归 | 无 |
| `_build_project_step()` 硬阻断 | 有 CaseWhenDecl → 引用 alias；物理列 → ColumnRef；未解析列 → `DerivedColumnRuleMissing` 异常 | FakeLabelExtractor |
| `_prepare_spec_for_planning()` | 无未解析列 → 跳过；有未解析列 → 全流程；非 LABEL_TABLE + 未解析列 → W008 | FakeLabelExtractor |
| Parser `type` 映射 | `"label_table"` → `DatasetType.LABEL_TABLE`；未声明 → `UNSPECIFIED` + W007 | 无 |
| `LabelExtractionArtifact` | 字段完整性；与 Spec 的分离存储 | FakeLabelExtractor |
| `LabelPromotionArtifact` | parent_spec_hash 正确；new_spec_hash 正确；溯源链完整性 | FakeLabelExtractor |

### 7.2 `FakeLabelExtractor` 设计

**文件**：`src/tianshu_datadev/labels/label_extractor.py`

```python
class FakeLabelExtractor:
    """确定性 Fake Adapter——用于 pytest，不调用真实 LLM。

    返回预定义的 LabelRuleProposal 列表，覆盖常见标签提取场景。
    真实 LLM 调用通过 Harness 进行（见 §7.4）。
    """

    def __init__(self, proposals: list[LabelRuleProposal] | None = None):
        """初始化 Fake Adapter。

        Args:
            proposals: 预定义的 Proposal 列表。若为 None，使用内置默认场景。
        """
        self._proposals = proposals or []

    def extract(
        self,
        spec: ParsedDeveloperSpec,
        unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """返回预定义 Proposal——不调 LLM。"""
        artifact = LabelExtractionArtifact(
            artifact_id=f"extract_fake_{spec.spec_hash[:12]}",
            source_spec_hash=spec.spec_hash,
            extraction_time=datetime.utcnow().isoformat(),
            llm_model="fake",
            llm_prompt_version="fake-1.0",
            llm_temperature=0.0,
            unresolved_columns=unresolved_columns,
            raw_proposals=self._proposals,
        )
        return self._proposals, artifact
```

### 7.3 集成测试

| 场景 | 预期行为 | Adapter |
|------|----------|---------|
| Template 2（label_table，Markdown 有 CASE WHEN） | LabelExtractor 提取 → Validator 通过 → Builder 生成 CaseWhenStep（`cases`/`else_value`） → SQL 含 CASE WHEN → DuckDB 执行成功 | FakeLabelExtractor（预填充 Template 2 的正确 Proposal） |
| label_table 但 Markdown 无 CASE WHEN | LabelExtractor 提取失败 → BLOCKING 阻断 | FakeLabelExtractor（返回空 Proposal） |
| detail_table 有未解析派生列 | W008 警告 + 尝试提取，**Builder 硬阻断** | FakeLabelExtractor |
| aggregate_table（有 metrics） | 不触发 LabelExtractor；CaseWhenStep 不插入；现有行为不变 | 无 |
| 已有 compute_steps + case_when 的 Spec | 不触发 LabelExtractor（已解析）；Builder 按现有逻辑处理 | 无 |
| Template 2 + Validator 全部通过 | 全部 Proposal → CaseWhenDecl；PromotionArtifact 溯源完整 | FakeLabelExtractor |
| Template 2 + Validator 发现区间遗漏 | HUMAN_REVIEW，不自动 Promotion | FakeLabelExtractor（故意构造遗漏区间） |
| Template 2 + Validator 证据无法锚定 | BLOCKING 阻断 | FakeLabelExtractor（evidence 为虚假内容） |
| Builder 遇到未解析列 | `DerivedColumnRuleMissing` 异常 → PipelineError | FakeLabelExtractor（Proposal 缺某列） |

### 7.4 Harness 测试——真实 LLM 调用

**原则**：真实 LLM 调用不进入 `pytest`（避免网络依赖和 API 费用），而是进入 Harness（`tests/harness/`）。

| Harness 测试 | 说明 |
|-------------|------|
| `test_label_extractor_real_llm.py` | 使用真实 LLM 从 Template 2 Markdown body 提取 LabelRuleProposal + LabelDomain，验证输出结构合法 |
| `test_label_extractor_evidence_anchored.py` | 验证 LLM 输出的 evidence 确实引用 Markdown 原文 |
| `test_label_extractor_discriminator.py` | 验证 LLM 输出的 LabelPredicateNode 使用正确的 discriminator 子类，不输出字符串条件 |

### 7.5 DataTransformContract → SparkCaseWhenStep → SQL/Spark 同快照一致性（v2 新增）

**端到端验收链路**：

```
Spec (含 label_rules)
  → Builder（SQL 管线）
    → SqlBuildPlan（含 CaseWhenStep.cases，使用 Predicate + SqlLiteral）
      → Compiler → SQL（CASE WHEN ... END AS alias）
        → DuckDB 执行 → 结果快照 A
      → Contract 抽取 → DataTransformContractV1（含 case_when_labels）
        → contract_to_sql_steps() → 重建 SqlBuildPlan（含 CaseWhenStep）
          → Compiler → SQL → DuckDB 执行 → 结果快照 B
        → map_contract_to_spark_plan() → SparkPlan（含 SparkCaseWhenStep）
          → Spark Compiler → PySpark 代码 → Spark 执行 → 结果快照 C

验收条件：快照 A == 快照 B == 快照 C（行集 + 列值一致）
```

**测试场景**：

| 场景 | SQL 快照 | Spark 快照 | Contract 重建 SQL 快照 | 预期 |
|------|----------|------------|----------------------|------|
| Template 2（label_table） | A | C | B | A == B == C |
| label_table + 多标签列 | A | C | B | A == B == C |
| label_table + ELSE 默认值 | A | C | B | A == B == C |
| 非 label_table（无 CASE WHEN） | A | C | B | A == B == C（现有行为不变） |

### 7.6 回归测试基线更新

- 现有 601 个测试全部通过
- 所有已存在模板的 Parse / Plan / Execute 行为不变
- `CaseWhenLabelSpec` 在 Spark 管线中的现有行为不变
- 新增测试预计新增 25-35 个测试用例
- Harness 测试独立于 pytest，通过 `./run_harness.sh` 执行

---

## 8. 影响范围

### 8.1 新增文件

**测试合并原则**：优先合并到已有测试文件，避免冗余。仅当不存在合适的目标文件时才新建测试文件。

| 路径 | 职责 | 合并判定 |
|------|------|----------|
| `src/tianshu_datadev/labels/__init__.py` | 标签子系统入口 | 新模块，必须新建 |
| `src/tianshu_datadev/labels/label_extractor.py` | LLM 提取标签规则候选 + FakeLabelExtractor | 新模块，必须新建 |
| `src/tianshu_datadev/labels/label_rule_validator.py` | 确定性验证 Proposal（8 项检查） | 新模块，必须新建 |
| `src/tianshu_datadev/labels/promotion.py` | Proposal → CaseWhenDecl 提升 + 溯源 Artifact | 新模块，必须新建 |
| `src/tianshu_datadev/labels/artifacts.py` | LabelExtractionArtifact + LabelPromotionArtifact | 新模块，必须新建 |

**测试代码合并策略**（优先合并到已有文件）：

| 测试内容 | 合并目标 | 理由 |
|----------|----------|------|
| `DatasetType` 序列化/反序列化 | `tests/developer_spec/test_models.py`（已有） | 已有 Pydantic 模型测试 |
| `LabelPredicateNode` discriminator 验证 | `tests/developer_spec/test_models.py`（已有） | 已有模型单元测试 |
| `LabelRuleProposal` JSON Schema 验证 | `tests/developer_spec/test_models.py`（已有） | 同上 |
| `LabelRuleValidator` 逐项检查 | `tests/labels/test_label_rules.py`（已有） | 已有 `validate_label_enums()` 测试 |
| `LabelDomain` 提取/验证 | `tests/labels/test_label_rules.py`（已有） | 同上 |
| `Promotion` spec_hash/溯源 | `tests/labels/test_label_rules.py`（已有） | 已有 label 相关测试 |
| `_find_unresolved_derived_columns()` | `tests/developer_spec/test_parser.py`（已有） | 已有 Parser/Spec 测试 |
| `_build_case_when_steps()` | `tests/planning/test_sql_build_plan.py`（已有） | 已有 Builder 步骤测试 |
| `_predicate_from_label_node()` | `tests/planning/test_sql_build_plan.py`（已有） | 同上 |
| `_build_project_step()` 硬阻断 | `tests/planning/test_sql_build_plan.py`（已有） | 同上 |
| `_prepare_spec_for_planning()` | `tests/api/test_pipeline.py`（已有） | 已有管线测试 |
| Template 2 端到端集成 | `tests/sql/test_pipeline_e2e.py`（已有） | 已有 E2E 管线测试 |
| Contract → SQL/Spark 同快照 | `tests/spark/test_plan_comparator_integration.py`（已有） | 已有 Contract/Plan 对比测试 |
| Harness——真实 LLM 提取 | `tests/harness/test_label_extractor_real_llm.py`（**新建**） | Harness 独立于 pytest，无现有文件可合并 |
| Harness——Contract E2E 一致性 | `tests/harness/test_label_contract_e2e.py`（**新建**） | 同上 |

> **仅 2 个 Harness 文件需新建**——其余全部合并到已有测试文件中。

### 8.2 修改文件

| 路径 | 改动内容 |
|------|----------|
| `src/tianshu_datadev/developer_spec/models.py` | 新增 `DatasetType`、`CompareOp`、`LabelColumnRef`、`LabelTypedLiteral`、`LabelCompare`、`LabelIsNull`、`LabelIsNotNull`、`LabelAnd`、`LabelOr`、`LabelNot`、`LabelPredicateNode`（discriminator 联合）、`LabelBranchProposal`、`LabelRuleProposal`、`LabelDomain`、`LabelPredicateBranch`；`ParsedDeveloperSpec` 新增 `dataset_type`、`label_rules`；`CaseWhenDecl` 新增 `typed_branches`（移除 v1 的 proposal_id/promotion_time） |
| `src/tianshu_datadev/developer_spec/parser.py` | `parse()` 读取 `spec_dict["type"]` → `dataset_type`；构造 `ParsedDeveloperSpec` 时传入 |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 新增 `_build_case_when_steps()`（使用 `cases`/`else_value`/`SafeIdentifier`）、`_predicate_from_label_node()`；`_build_single_table()` 插入 CaseWhenStep；`_build_project_step()` 加入硬阻断逻辑；新增 `DerivedColumnRuleMissing` 异常 |
| `src/tianshu_datadev/api/pipeline.py` | 新增 `_prepare_spec_for_planning()` 共享入口；管线编排统一调用 |
| `templates/` 目录下 Template 2 YAML | 添加 `type: label_table` |

### 8.3 不修改但需验证的文件

| 路径 | 原因 |
|------|------|
| `src/tianshu_datadev/planning/models.py`（`CaseWhenStep`/`WhenBranch`/`Predicate`/`SqlLiteral`） | 模型已正确，无需修改——但设计必须对齐其真实字段名 |
| `src/tianshu_datadev/spark/models.py`（`SparkCaseWhenStep`） | Spark 侧已有完整 CASE WHEN 支持 |
| `src/tianshu_datadev/spark/mapper.py`（`_map_case_when`） | 已支持 `CaseWhenLabelSpec → SparkCaseWhenStep` 映射 |
| `src/tianshu_datadev/spark/contract_sql_bridge.py`（`contract_to_sql_steps`） | 已支持 `CaseWhenLabelSpec → CaseWhenStep` 重建 |
| `src/tianshu_datadev/validation/label_validator.py`（`validate_label_enums`） | 防御性复检保留，与 LabelRuleValidator 分层 |
| `src/tianshu_datadev/sql/compiler.py` | CaseWhenStep 编译已实现，无需改动 |

---

## 9. 风险与未决项

### 9.1 已识别风险

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| LLM 提取 CASE WHEN 不稳定 | 中 | 低温 + discriminator Schema 约束 + Validator 8 项检查拦截 |
| LLM 生成的 evidence 与原文匹配但语义不对 | 中 | Validator 锚定检查仅做子串匹配——语义正确性仍需人工审查（HUMAN_REVIEW 路径） |
| `LabelPredicateNode` 表达能力不足 | 低 | 当前 discriminator 联合覆盖 Template 2 全部条件；后续按需扩展 `IN`/`BETWEEN` 子类 |
| `_find_unresolved_derived_columns()` 漏判 | 中 | 集成测试覆盖全部已解析类型；Builder 硬阻断兜底 |
| Promotion 后 spec_hash 变化影响下游（Contract/Review） | 高 | PromotionArtifact 记录 parent_spec_hash 保证溯源；Contract 使用 Promotion 后 hash |
| 硬阻断过于激进——误杀合法的派生列 | 中 | `_find_unresolved_derived_columns()` 覆盖 Manifest schema 作为解析来源；测试覆盖边界场景 |
| Fake Adapter 与真实 LLM 行为不一致 | 中 | Harness 测试验证真实 LLM 输出；Fake Adapter 的数据来自真实 LLM 输出快照 |
| 迁移期 UNSPECIFIED 被静默处理 | 中 | W007 警告不可被静默忽略；日志和前端均展示 |

### 9.2 后续迭代

1. `LabelPredicateNode` 扩展 `LabelIn`、`LabelBetween` 子类（当前需求不需要，但 discriminator 架构预留）
2. `LabelExtractor` 支持多语言 Markdown body（当前仅中文）
3. `LabelRuleValidator` 增加跨列一致性检查（如同一标签表的多个标签列之间无重叠/无遗漏）
4. 前端展示 LabelExtractionArtifact → LabelPromotionArtifact 的完整提升过程
5. Harness 真实 LLM 快照定期更新，确保 Fake Adapter 数据不过时

---

## 10. 决策记录

| 决策点 | 选项 | 选择 | 理由 |
|--------|------|------|------|
| 标签逻辑输入来源 | A) YAML compute_steps / B) LLM 提取 / C) output_columns 声明 | **B → A**：LLM 提取候选 + 确定性验证后提升为结构化 IR | 避免人工翻译负担，同时不引入 LLM 产物直接执行的风险 |
| type 字段建模 | A) Optional[str] / B) DatasetType 枚举 | **B**：正式枚举，最终设为必填，迁移期用 UNSPECIFIED | 语义清晰，后续扩展安全，Validator 可按类型启用检查 |
| Builder 分流 | A) 按类型三分支 / B) 统一 IR 驱动 | **B**：Builder 按 IR 步骤组合生成计划 | 不重复代码，DatasetType 只用于门禁和验证 |
| LabelExtractor 位置 | A) SpecEnricher 增强 / B) 独立管线阶段 / C) Parser 内 | **B**：独立阶段 | 不污染确定性 Parser，不扩大 SpecEnricher 职责 |
| 谓词表达方式 | A) 字符串 when / B) discriminator 封闭联合 AST | **B**：8 子类 discriminator 联合 | 禁止 LLM 输出自由 SQL，关闭注入风险；禁止 Optional 大杂烩 |
| 触发条件 | A) dataset_type + compute_steps 为空 / B) 未解析派生输出列 | **B**：按输出列逐个检测是否已解析 | 更精确，不漏判也不冗余调用 LLM |
| Builder 标签表达 | A) Project 内嵌 CASE / B) CaseWhenStep | **B**：使用现有 CaseWhenStep（`cases`/`else_value`/`SafeIdentifier`） | 复用已有基础设施，Compiler 无需改动 |
| Validator 层级 | A) 单一 Validator / B) 两层 | **B**：LabelRuleValidator（Promotion 前，8 项检查）+ validate_label_enums（Compiler 前） | Proposal 提升和防御性复检职责分离 |
| 未解析列处理 | A) 回退为 ColumnRef / B) 硬阻断 | **B**：DERIVED_COLUMN_RULE_MISSING 硬阻断 | v2 变更——静默回退导致 DuckDB Binder Error，不如及早阻断 |
| 溯源信息存储 | A) 嵌入 CaseWhenDecl / B) 独立 Artifact | **B**：LabelExtractionArtifact + LabelPromotionArtifact | 语义 Spec 只含确定性规则，spec_hash 不受溯源信息影响 |
| 共享入口 | A) 各入口独立编排 / B) _prepare_spec_for_planning() | **B**：共享函数 | 覆盖 plan/execute/run_all/rich，避免只修一处 |
| pytest Adapter | A) Mock LLM / B) FakeLabelExtractor | **B**：确定性 Fake Adapter | 不依赖 Mock 框架，行为可预测；真实 LLM 进 Harness |
| LabelDomain 来源 | A) 程序员手写 enum / B) Agent 从原文提取 | **B**：Agent 提取 + Validator 验证 | 不增加程序员负担，evidence 锚定保证可追溯 |
| E2E 验收 | A) 仅 SQL 管线 / B) SQL + Spark + Contract 重建 | **B**：三路同快照对比 | 确保 Contract 抽取和 Spark 映射的 CASE WHEN 一致性 |
