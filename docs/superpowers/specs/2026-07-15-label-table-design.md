# label_table 类型支持——完整设计书

> **状态**：设计阶段，待审批。**不执行。**

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

---

## 2. 架构概览

### 2.1 修正后管线

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
LabelExtractor（新增，LLM）
  ├─ 触发条件：存在"未解析派生输出列"
  ├─ 仅对未解析列调用 LLM
  ├─ LLM 输出：list[LabelRuleProposal]（候选，不可执行）
  └─ 已有结构化规则 → 跳过 LLM，直接进入 Validator
  │
  ▼
LabelRuleValidator（新增，确定性）
  ├─ 字段存在性检查
  ├─ 类型/操作符校验
  ├─ 分支完整性检查（有无 ELSE/默认值）
  ├─ 枚举一致性检查（then 值 vs output_columns enum）
  └─ 产出：验证结果 + 证据报告
  │
  ▼
Promotion（新增，确定性）
  ├─ Proposal → CaseWhenDecl 提升
  ├─ 重新计算 spec_hash
  ├─ 记录 proposal_hash、parent_spec_hash、prompt/模型版本
  └─ 产出：增强后的 ParsedDeveloperSpec（含 CaseWhenDecl）
  │
  ▼
Builder（现有，新增 CaseWhenStep 插入逻辑）
  ├─ Scan → Filter → Aggregate? → CaseWhenStep → Project → Sort → Limit
  ├─ 检测 output_column 是否有对应 CaseWhenDecl
  └─ 有 → 插入 CaseWhenStep；无 → 保持现有 ColumnRef 行为
  │
  ▼
Compiler → Execute（现有，无需改动）
```

### 2.2 核心设计原则

1. **`DatasetType` 做门禁，不驱动 Builder 分叉**：DatasetType 决定验证策略和必需能力检查，Builder 保持统一的 IR 驱动——有 `CaseWhenDecl` 则生成 `CaseWhenStep`，没有就当普通列引用
2. **LLM 产出候选，确定性验证后提升**：LLM 不直接输出可执行 `CaseWhenDecl`，而是输出 `LabelRuleProposal`；只有 `LabelRuleValidator` 验证通过后，才提升为 `CaseWhenDecl`
3. **禁止自由 SQL 字符串**：LLM 只能生成封闭 `LabelPredicateNode` AST，禁止 `when: "distance_miles <= 2"` 和 `raw_condition`
4. **原 Spec 不原地修改**：通过管线阶段逐步增强，溯源链完整

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

新增字段：

```python
class ParsedDeveloperSpec(StrictModel):
    # ... 现有字段保持不变 ...
    dataset_type: DatasetType = DatasetType.UNSPECIFIED  # 新增——数据产品类型
    label_rules: list[CaseWhenDecl] = []                  # 新增——标签规则（已验证）
    parent_spec_hash: str | None = None                   # 新增——Promotion 前的原始 spec_hash（溯源）
```

### 3.3 `LabelPredicateNode`——封闭谓词 AST

**文件**：`src/tianshu_datadev/developer_spec/models.py`（新建，在 `CaseWhenDecl` 之前）

```python
class CompareOp(str, Enum):
    """比较操作符——封闭集合。"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class LabelTypedLiteral(StrictModel):
    """类型化字面量——禁止隐式类型转换。"""
    value: str
    data_type: Literal["string", "number", "boolean", "null"]


class LabelPredicateNode(StrictModel):
    """封闭标签谓词 AST。

    LLM 只能生成此结构的候选，不允许自由 SQL 字符串。
    覆盖 Template 2 的全部条件需求：
      - IS NULL / IS NOT NULL
      - = true / = false
      - <= 2（数值比较）
      - AND / OR / NOT 组合
    """
    node_type: Literal[
        "AND", "OR", "NOT",
        "COMPARE", "IS_NULL", "IS_NOT_NULL",
        "COLUMN_REF", "TYPED_LITERAL",
    ]

    # ── 逻辑节点（AND / OR / NOT）──
    children: list["LabelPredicateNode"] = []

    # ── COMPARE 节点 ──
    left: str | None = None       # 列名（归一化后）
    op: CompareOp | None = None   # 比较操作符
    right: LabelTypedLiteral | None = None

    # ── IS_NULL / IS_NOT_NULL 节点 ──
    column: str | None = None

    # ── TYPED_LITERAL / COLUMN_REF 叶子 ──
    literal: LabelTypedLiteral | None = None
    column_ref: str | None = None  # COLUMN_REF 的列名


# 支持递归类型引用
LabelPredicateNode.model_rebuild()
```

**Template 2 覆盖性验证**：

| Markdown 条件 | LabelPredicateNode 表达 |
|---------------|------------------------|
| `distance_miles IS NULL` | `{node_type: "IS_NULL", column: "distance_miles"}` |
| `is_distance_outlier = true` | `{node_type: "COMPARE", left: "is_distance_outlier", op: "=", right: {value: "true", data_type: "boolean"}}` |
| `distance_miles <= 2` | `{node_type: "COMPARE", left: "distance_miles", op: "<=", right: {value: "2", data_type: "number"}}` |
| `distance_miles > 2 AND distance_miles <= 5` | `{node_type: "AND", children: [{COMPARE left:"distance_miles" op:">" right:{value:"2"...}}, {COMPARE left:"distance_miles" op:"<=" right:{value:"5"...}}]}` |
| `IS NULL OR = true`（组合） | `{node_type: "OR", children: [{IS_NULL...}, {COMPARE...}]}` |

### 3.4 `LabelRuleProposal`——LLM 候选

**文件**：`src/tianshu_datadev/developer_spec/models.py`（新建）

```python
class LabelBranchProposal(StrictModel):
    """单条 WHEN-THEN 候选——LLM 输出。"""
    condition: LabelPredicateNode    # 结构化谓词（非字符串）
    then_label: str                  # 结果标签值
    evidence: str = ""               # LLM 从 Markdown 中引用的原文证据（用于审查）


class LabelRuleProposal(StrictModel):
    """LLM 提取的标签规则候选——不可执行，必须经 LabelRuleValidator 验证后提升。

    一个 Proposal 对应 output_spec.columns 中的一个标签列。
    """
    proposal_id: str                          # proposal_{sha256[:12]}
    source_spec_hash: str                     # 来源 Spec hash
    output_column: str                        # 目标输出列名（对应 output_spec.columns）
    branches: list[LabelBranchProposal]       # WHEN-THEN 候选分支
    else_value: str | None = None             # ELSE 默认值
    llm_model: str = ""                       # 提取时使用的 LLM 模型（追溯）
    llm_prompt_version: str = ""              # Prompt 模板版本
```

### 3.5 `CaseWhenDecl` 改造

**文件**：`src/tianshu_datadev/developer_spec/models.py:534`

现有字段保留（向后兼容已有 `compute_steps`），新增：

```python
class CaseWhenDecl(StrictModel):
    """CASE WHEN 声明——已验证的标签规则。

    两种使用场景：
    1. compute_steps 合流步骤的 case_when 字段（现有）
    2. ParsedDeveloperSpec.label_rules（新增——LabelExtractor → Validator → Promotion 链路）
    """
    # ── 现有字段保持不变 ──
    branches: list[CaseWhenBranchDecl] = []
    else_value: str | None = None
    output_column: str = ""

    # ── 新增：类型化分支（LLM 提取 + 确定性验证后填充）──
    typed_branches: list[LabelPredicateBranch] = []

    # ── 新增：溯源字段（Promotion 阶段填充）──
    proposal_id: str | None = None       # 来源 Proposal ID（None = 人工编写/模板自带）
    promotion_time: str | None = None    # 提升时间戳（ISO 8601）


class LabelPredicateBranch(StrictModel):
    """已验证的类型化 WHEN-THEN 分支。"""
    condition: LabelPredicateNode
    then_label: str
```

---

## 4. 管线阶段设计

### 4.1 管线编排

**文件**：`src/tianshu_datadev/api/pipeline.py`

在 `execute_rich` / `run_all` 流程中插入新阶段：

```text
1. Parser          → ParsedDeveloperSpec（含 dataset_type）
2. SourceManifest   → 源数据清单
3. SpecEnricher     → 增强后 ParsedDeveloperSpec
4. LabelExtractor   → list[LabelRuleProposal]（仅当存在未解析派生输出列时）
5. LabelRuleValidator → 验证报告
6. Promotion        → 增强后 ParsedDeveloperSpec（含 label_rules）
7. Builder          → SqlBuildPlan（含 CaseWhenStep）
8. Compiler         → SQL
9. Execute          → 结果
```

### 4.2 LabelExtractor 触发条件

**不**使用 `dataset_type == LABEL_TABLE and compute_steps is None`——太粗。

改用**"未解析派生输出列"**检测：

```python
def _find_unresolved_derived_columns(spec: ParsedDeveloperSpec) -> list[str]:
    """找出 output_columns 中既不在源表也不在任何结构化规则中的列。

    已解析条件（任一满足即认为已解析）：
    - 列名存在于任意 input_table 的 columns/key_columns/business_columns 中
    - 列名匹配某个 metric.alias
    - 列名匹配某个 window_metric 的输出
    - 列名匹配某个 compute_step.output_alias / case_when.output_column
    - 列名匹配 spec.label_rules[*].output_column

    Returns:
        未解析的输出列名列表——这些列需要 LLM 提取标签规则。
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

**行为规则**：
- `unresolved` 为空 → 跳过 LabelExtractor（无需 LLM）
- `unresolved` 非空 → 对每个未解析列调用 LLM，独立生成 `LabelRuleProposal`
- 已有结构化标签规则的列 → 跳过 LLM，只运行 Validator
- `dataset_type != LABEL_TABLE` 但存在未解析列 → 产生 `W008` 警告，但仍尝试解析

### 4.3 LabelExtractor

**文件**：`src/tianshu_datadev/labels/label_extractor.py`（新建）

```
职责：从 Markdown body 中提取 CASE WHEN 标签规则
输入：ParsedDeveloperSpec + 未解析列名列表
输出：list[LabelRuleProposal]
LLM：是
```

**Prompt 设计要点**：
- 输入上下文：Markdown body + 相关 output_column 的 name/type/description + 源表可用字段列表
- 输出格式：严格 JSON Schema（`LabelRuleProposal` 结构）
- 要求 LLM 输出**结构化谓词 AST**（`LabelPredicateNode`），不是自由文本
- 每个分支必须附带 `evidence`——引用 Markdown 原文
- 温度设低（0.1~0.2），保证提取一致性

**错误处理**：
- LLM 返回无法解析的 JSON → 生成 `OpenQuestion(blocking=True)`
- LLM 超时 → 重试一次，仍失败则生成 `OpenQuestion(blocking=True)`
- LLM 生成了 `raw_condition` 字符串而非 `LabelPredicateNode` → Validator 拒绝

### 4.4 LabelRuleValidator

**文件**：`src/tianshu_datadev/labels/label_rule_validator.py`（新建）

```
职责：确定性验证 LabelRuleProposal，决定是否可提升为 CaseWhenDecl
输入：LabelRuleProposal + ParsedDeveloperSpec
输出：ValidationReport（pass/fail + 错误详情）
LLM：否——纯确定性逻辑
```

**检查项**（全部通过才可 Promotion）：

| 检查项 | 说明 | 失败行为 |
|--------|------|----------|
| **字段存在性** | `condition` 中引用的所有 `column_ref` 必须在 source_table 已声明列中存在 | 标记 `INVALID_FIELD` |
| **操作符合法性** | `CompareOp` 必须是 `CompareOp` 枚举成员 | 标记 `INVALID_OPERATOR` |
| **类型兼容性** | `TypedLiteral.data_type` 与字段声明的 `data_type` 兼容（number→number, string→string, boolean→boolean） | 标记 `TYPE_MISMATCH` |
| **分支完整性** | 必须有至少 1 个分支；建议有 `else_value`（无则为 W009 警告，不阻断） | 无分支→`EMPTY_BRANCHES`；无 ELSE→`W009` |
| **枚举一致性** | 若 `output_column` 在 `output_spec` 中声明了 `enum` 值，`then_label` 必须在该集合内 | 标记 `ENUM_MISMATCH` |
| **证据存在性** | 每个分支必须有非空 `evidence` | 标记 `MISSING_EVIDENCE`（W010 警告，不阻断） |

**返回值**：

```python
class LabelValidationReport(StrictModel):
    proposal_id: str
    passed: bool                              # 全部检查通过
    checks: list[LabelValidationCheck]        # 逐项检查结果
    errors: list[str] = []                    # 阻断性错误
    warnings: list[str] = []                  # 非阻断警告

class LabelValidationCheck(StrictModel):
    check_name: str                           # FIELD_EXISTS / OPERATOR_VALID / TYPE_COMPATIBLE
                                              # / BRANCH_COMPLETENESS / ENUM_CONSISTENCY / EVIDENCE_PRESENT
    passed: bool
    detail: str = ""
```

**与现有 `validate_label_enums()` 的关系**：

| 验证器 | 位置 | 职责 |
|--------|------|------|
| `LabelRuleValidator` | Promotion 前 | Proposal → CaseWhenDecl——字段/类型/操作符/分支/枚举/证据 |
| `validate_label_enums()` | Compiler 前 | SqlBuildPlan → 防御性复检——检查 CaseWhenStep 结果枚举与 output_columns enum 一致 |

### 4.5 Promotion

**文件**：`src/tianshu_datadev/labels/promotion.py`（新建）

```
职责：将验证通过的 LabelRuleProposal 提升为 CaseWhenDecl，回填到 ParsedDeveloperSpec
输入：LabelRuleProposal + ValidationReport + ParsedDeveloperSpec
输出：增强后的 ParsedDeveloperSpec（含新的 label_rules）
LLM：否——纯确定性
```

**关键操作**：

1. **生成新 Spec**（不原地修改）：
   ```python
   # 基于原 spec 创建新 spec，追加 label_rules
   new_spec = ParsedDeveloperSpec(
       **original_spec.model_dump(),
       label_rules=new_label_rules,
   )
   ```

2. **重新计算 spec_hash**：
   ```python
   new_hash = _normalized_spec_hash(new_spec)
   object.__setattr__(new_spec, "spec_hash", new_hash)
   object.__setattr__(new_spec, "spec_id", f"spec_{new_hash[:12]}")
   ```

3. **记录溯源信息**：
   ```python
   object.__setattr__(new_spec, "parent_spec_hash", original_spec.spec_hash)
   ```

4. **CaseWhenDecl 溯源填充**：
   - `proposal_id` ← `LabelRuleProposal.proposal_id`
   - `promotion_time` ← `datetime.utcnow().isoformat()`
   - `typed_branches` ← 从 `LabelBranchProposal` 转换

**不可复制旧 `spec_hash`**——否则后续 Plan、Contract 和 Review Package 的溯源链断裂。

### 4.6 Builder 改动

**文件**：`src/tianshu_datadev/planning/sql_build_plan.py`

**原则**：Builder 保持统一 IR 驱动，不按 `DatasetType` 分叉代码路径。

**改动点**：

1. **`_build_single_table()` 插入 `CaseWhenStep` 列表**（`sql_build_plan.py:1429-1490`）：

   在 Aggregate 步骤之后、Project 步骤之前插入：

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

2. **新增 `_build_case_when_steps()`**（每个 label_rule 对应一个 `CaseWhenStep`）：

   ```python
   def _build_case_when_steps(self, spec: ParsedDeveloperSpec) -> list[CaseWhenStep]:
       """从 spec.label_rules 构建 CaseWhenStep 列表。

       每个 label_rule（对应一个标签输出列）生成一个独立的 CaseWhenStep。
       使用现有的类型化 WhenBranch（condition 用 Predicate，不用 raw_condition）。
       """
       if not spec.label_rules:
           return []

       steps: list[CaseWhenStep] = []
       for rule in spec.label_rules:
           branches: list[WhenBranch] = []
           for tb in rule.typed_branches:
               predicate = self._predicate_from_label_node(tb.condition)
               branches.append(WhenBranch(
                   condition=predicate,
                   result=SqlLiteral(value=tb.then_label, data_type="string"),
               ))

           else_result = None
           if rule.else_value is not None:
               else_result = SqlLiteral(value=rule.else_value, data_type="string")

           step_id_content = {
               "output_column": rule.output_column,
               "branch_count": len(branches),
           }
           steps.append(CaseWhenStep(
               step_id=SqlBuildPlan.generate_step_id("case_when", step_id_content),
               alias=rule.output_column,
               branches=branches,
               else_result=else_result,
           ))

       return steps
   ```

   `_predicate_from_label_node()` 是 `LabelPredicateNode` AST → Planning `Predicate` AST 的转换函数（一对一映射：`AND`→`LogicalOp.AND`，`COMPARE`→`ComparisonPredicate`，`IS_NULL`→`IsNullPredicate` 等）。

3. **`_build_project_step()` 改动**：

   检测 `output_column` 是否有对应的 `CaseWhenDecl`：
   - **有** → 生成 `ColumnRef` 引用 `CaseWhenStep` 的输出别名（不是源表列）
   - **无** → 保持现有行为（`ColumnRef` 直接引用源表列）

4. **`WhenBranch` 全程使用结构化 `Predicate`**——不使用 `raw_condition`。

**Compiler 保持不变的前提条件**：
- Builder 输出已有 `CaseWhenStep`
- `WhenBranch.condition` 使用结构化 `Predicate`
- `result` / `else_result` 使用 `SqlLiteral`
- 不使用 `raw_condition`

---

## 5. 触发条件与流程控制

### 5.1 LabelExtractor 触发决策树

```
对每个 output_spec.columns 中的列：
  ├─ 列名在源表字段中？
  │   └─ YES → 已解析（物理列）→ 跳过
  ├─ 列名匹配 metric.alias？
  │   └─ YES → 已解析（聚合指标）→ 跳过
  ├─ 列名匹配 compute_steps[*].output_alias？
  │   └─ YES → 已解析（compute_step 产出）→ 跳过
  ├─ 列名匹配 spec.label_rules[*].output_column？
  │   └─ YES → 已解析（已有标签规则）→ 跳过
  └─ NO（所有条件均不匹配）
      └─ → 未解析派生输出列 → 触发 LabelExtractor
```

### 5.2 详细流程

```text
LabelExtractor.trigger_if_needed(spec) → list[unresolved_columns]

if unresolved_columns 为空:
    → 跳过，进入 Builder

elif spec.dataset_type == LABEL_TABLE:
    → 对每个 unresolved_column:
        ├─ 调用 LLM → LabelRuleProposal
        ├─ LabelRuleValidator.validate(proposal)
        │   ├─ PASS → Promotion.promote(proposal) → CaseWhenDecl → 加入 spec.label_rules
        │   └─ FAIL → OpenQuestion(blocking=True) + 记录到 review 报告
        └─ 继续下一个 unresolved_column
    → 进入 Builder

else (非 LABEL_TABLE 但存在未解析列):
    → 产生 W008 警告："检测到未解析派生输出列但 dataset_type 不是 label_table"
    → 仍然尝试 LabelExtractor（宽松模式——失败不阻断）
    → 进入 Builder
```

---

## 6. 迁移策略

### 6.1 分阶段推进

**Phase 1（当前设计实施）**：
1. 定义 `DatasetType` 枚举，`ParsedDeveloperSpec.dataset_type` 默认 `UNSPECIFIED`
2. 实现 `LabelPredicateNode` + `LabelRuleProposal` 模型
3. 实现 `LabelExtractor` + `LabelRuleValidator` + `Promotion`
4. Builder 新增 `_build_case_when_step()`
5. Parser 读取 `spec_dict["type"]` 映射到 `dataset_type`
6. `UNSPECIFIED` 产生 `W007` 迁移警告
7. 更新 Template 2 的 YAML（添加 `type: label_table`、`output_columns[].enum`）

**Phase 2（后续迭代）**：
1. `dataset_type` 升级为必填（移除 `UNSPECIFIED` 默认值）
2. 更新全部内置模板和 fixture
3. 更新黄金样本
4. 将 `dataset_type` 写入 Code Review Package 和 DataTransformContract

### 6.2 向后兼容

- `dataset_type = UNSPECIFIED` 的 Spec 按现有行为处理（不触发 LabelExtractor，不插入 CaseWhenStep）
- 已有 `compute_steps` + `case_when` 的 Spec 不受影响（`CaseWhenDecl` 字段只新增不删除）
- `CaseWhenBranchDecl.when: str` 保留但不用于新链路——LLM 不允许产出字符串模式分支

---

## 7. 验证与测试清单

### 7.1 单元测试

| 测试对象 | 测试内容 |
|----------|----------|
| `DatasetType` 枚举 | 序列化/反序列化；`UNSPECIFIED` 默认值行为 |
| `LabelPredicateNode` | 所有 `node_type` 的构造/验证；递归嵌套（AND of OR of COMPARE）；非法类型组合拒绝 |
| `LabelRuleProposal` | JSON Schema 验证；evidence 空字符串拒绝 |
| `LabelRuleValidator` | 每项检查的 PASS/FAIL；边界条件（空 branches、无 ELSE、枚举不匹配） |
| `Promotion` | spec_hash 重算正确性；溯源字段填充；不修改原 spec |
| `_find_unresolved_derived_columns()` | 物理列/指标/窗口指标/compute_step/label_rule 各场景正确识别 |
| `_build_case_when_step()` | 有 label_rules → 生成 CaseWhenStep；无 label_rules → None；Predicate 结构正确 |
| Parser `type` 映射 | `"label_table"` → `DatasetType.LABEL_TABLE`；未声明 → `UNSPECIFIED` + W007 |

### 7.2 集成测试

| 场景 | 预期行为 |
|------|----------|
| Template 2（label_table，Markdown 有 CASE WHEN） | LabelExtractor 提取 → Validator 通过 → Builder 生成 CaseWhenStep → SQL 含 CASE WHEN → DuckDB 执行成功 |
| label_table 但 Markdown 无 CASE WHEN | LabelExtractor 提取失败 → OpenQuestion(blocking=True) |
| detail_table 有未解析派生列 | W008 警告 + 宽松模式尝试提取 |
| aggregate_table（有 metrics） | 不触发 LabelExtractor；CaseWhenStep 不插入；现有行为不变 |
| 已有 compute_steps + case_when 的 Spec | 不触发 LabelExtractor（已解析）；Builder 按现有逻辑处理 |
| Template 2 + enum 声明的输出列 | Validator 检查 then_label 在 enum 内；不匹配则拒绝 |

### 7.3 回归测试

- 现有 601 个测试全部通过
- 所有已存在模板的 Parse / Plan / Execute 行为不变
- `CaseWhenLabelSpec` 在 Spark 管线中的现有行为不变

---

## 8. 影响范围

### 8.1 新增文件

| 路径 | 职责 |
|------|------|
| `src/tianshu_datadev/labels/__init__.py` | 标签子系统入口 |
| `src/tianshu_datadev/labels/label_extractor.py` | LLM 提取标签规则候选 |
| `src/tianshu_datadev/labels/label_rule_validator.py` | 确定性验证 Proposal |
| `src/tianshu_datadev/labels/promotion.py` | Proposal → CaseWhenDecl 提升 |
| `tests/labels/test_label_extractor.py` | LabelExtractor 单元测试 |
| `tests/labels/test_label_rule_validator.py` | Validator 单元测试 |
| `tests/labels/test_label_e2e.py` | 端到端集成测试 |

### 8.2 修改文件

| 路径 | 改动内容 |
|------|----------|
| `src/tianshu_datadev/developer_spec/models.py` | 新增 `DatasetType`、`LabelPredicateNode`、`LabelTypedLiteral`、`LabelRuleProposal`、`LabelBranchProposal`、`LabelPredicateBranch`；`ParsedDeveloperSpec` 新增 `dataset_type`、`label_rules`、`parent_spec_hash`；`CaseWhenDecl` 新增 `typed_branches`、`proposal_id`、`promotion_time` |
| `src/tianshu_datadev/developer_spec/parser.py` | `parse()` 读取 `spec_dict["type"]` → `dataset_type`；构造 `ParsedDeveloperSpec` 时传入 |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 新增 `_build_case_when_step()`；`_build_single_table()` 插入 CaseWhenStep；`_build_project_step()` 检测 label_rule 列 |
| `src/tianshu_datadev/api/pipeline.py` | 管线编排插入 LabelExtractor → Validator → Promotion |
| `templates/` 目录下 Template 2 YAML | 添加 `type: label_table`；`output_columns` 添加 `enum` 声明 |

---

## 9. 风险与未决项

### 9.1 已识别风险

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| LLM 提取 CASE WHEN 不稳定 | 中 | 低温 + 结构化 Schema + 确定性 Validator 拦截 |
| `LabelPredicateNode` 表达能力不足 | 低 | 当前覆盖 Template 2 全部条件；后续按需扩展（IN、BETWEEN 可后续加入） |
| `_find_unresolved_derived_columns()` 漏判 | 中 | 集成测试覆盖全部已解析类型；Validator 兜底检查 |
| Promotion 后 spec_hash 变化影响下游 | 高 | 严格控制 Promotion 时机；记录 parent_spec_hash 保证溯源 |
| 迁移期 UNSPECIFIED 被静默处理 | 中 | W007 警告不可被静默忽略；日志和前端均展示 |

### 9.2 后续迭代

1. `LabelPredicateNode` 扩展 `IN`、`BETWEEN` 节点（当前需求不需要，但架构预留）
2. `LabelExtractor` 支持多语言 Markdown body（当前仅中文）
3. `LabelRuleValidator` 增加跨列一致性检查（如同一标签表的多个标签列之间无重叠/无遗漏）
4. 前端展示 LabelRuleProposal → CaseWhenDecl 的提升过程（LLM 提取原文 + 验证结果）

---

## 10. 决策记录

| 决策点 | 选项 | 选择 | 理由 |
|--------|------|------|------|
| 标签逻辑输入来源 | A) YAML compute_steps / B) LLM 提取 / C) output_columns 声明 | **B → A**：LLM 提取候选 + 确定性验证后提升为结构化 IR | 避免人工翻译负担，同时不引入 LLM 产物直接执行的风险 |
| type 字段建模 | A) Optional[str] / B) DatasetType 枚举 | **B**：正式枚举，最终设为必填，迁移期用 UNSPECIFIED | 语义清晰，后续扩展安全，Validator 可按类型启用检查 |
| Builder 分流 | A) 按类型三分支 / B) 统一 IR 驱动 | **B**：Builder 按 IR 步骤组合生成计划 | 不重复代码，DatasetType 只用于门禁和验证 |
| LabelExtractor 位置 | A) SpecEnricher 增强 / B) 独立管线阶段 / C) Parser 内 | **B**：独立阶段，Parser → SpecEnricher → LabelExtractor → Validator → Promotion → Builder | 不污染确定性 Parser，不扩大 SpecEnricher 职责 |
| 谓词表达方式 | A) 字符串 when / B) 封闭 AST | **B**：封闭 LabelPredicateNode | 禁止 LLM 输出自由 SQL，关闭注入风险 |
| 触发条件 | A) dataset_type + compute_steps 为空 / B) 未解析派生输出列 | **B**：按输出列逐个检测是否已解析 | 更精确，不漏判也不冗余调用 LLM |
| Builder 标签表达 | A) Project 内嵌 CASE / B) CaseWhenStep / C) 新 Label Builder | **B**：使用现有 CaseWhenStep | 复用已有基础设施，Compiler 无需改动 |
| Validator 层级 | A) 单一 Validator / B) 两层 | **B**：LabelRuleValidator（Promotion 前）+ validate_label_enums（Compiler 前） | Proposal 提升和防御性复检职责分离 |
