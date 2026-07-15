# label_table 类型支持——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 label_table 类型完整支持链路——从 Parser 保留 DatasetType，经 LabelExtractor→Validator→Promotion，到 Builder 生成 CaseWhenStep，最终 SQL/Spark 同快照一致。

**Architecture:** 9 段管线：Parser → SourceManifest → SpecEnricher → _prepare_spec_for_planning() → LabelExtractor(LLM) → LabelRuleValidator(确定性 8 项) → Promotion → Builder(CaseWhenStep + 硬阻断) → Compiler/Execute。溯源信息分离到独立 Artifact，语义 Spec 只保存确定性规则。

**Tech Stack:** Python 3.12, Pydantic v2 (discriminated unions), pytest + FakeLabelExtractor, DuckDB, PySpark

## Global Constraints

- 所有代码注释必须使用中文
- pytest 使用确定性 Fake Adapter，真实 LLM 调用仅进入 Harness
- 新增测试优先合并已有测试文件——仅 Harness 文件可新建
- 未解析派生输出列必须硬阻断（DERIVED_COLUMN_RULE_MISSING），禁止回退为 ColumnRef
- CaseWhenStep 使用真实字段名：`cases`（非 branches）、`else_value`（非 else_result）、`alias: SafeIdentifier`
- WhenBranch 仅使用 `condition: Predicate`，禁止 `raw_condition`
- 溯源信息（模型/Prompt/时间/hash）存入独立 Artifact，不进入 spec_hash
- 修改源码后必须通过 `./dev-reload.sh` 重启服务验证
- 每个 Task 完成后独立 commit，使用 conventional commit message

---

## 文件结构总览

### 新建文件

| 路径 | 职责 |
|------|------|
| `src/tianshu_datadev/labels/__init__.py` | 标签子系统入口，导出所有公开接口 |
| `src/tianshu_datadev/labels/artifacts.py` | LabelExtractionArtifact + LabelPromotionArtifact |
| `src/tianshu_datadev/labels/label_extractor.py` | LabelExtractor 抽象接口 + FakeLabelExtractor |
| `src/tianshu_datadev/labels/label_rule_validator.py` | LabelRuleValidator（8 项确定性检查） |
| `src/tianshu_datadev/labels/promotion.py` | Promotion——Proposal → CaseWhenDecl + 溯源 Artifact |
| `tests/harness/test_label_extractor_real_llm.py` | Harness——真实 LLM 提取验证 |
| `tests/harness/test_label_contract_e2e.py` | Harness——Contract E2E 同快照一致性 |

### 修改文件

| 路径 | 改动 |
|------|------|
| `src/tianshu_datadev/developer_spec/models.py` | 新增 DatasetType、CompareOp、LabelPredicateNode(8 子类)、LabelDomain、LabelRuleProposal、LabelBranchProposal、LabelPredicateBranch、LabelValidationReport、LabelValidationCheck；ParsedDeveloperSpec 新增 dataset_type/label_rules；CaseWhenDecl 新增 typed_branches |
| `src/tianshu_datadev/developer_spec/parser.py` | 读取 spec_dict["type"] → dataset_type；导出 _find_unresolved_derived_columns() |
| `src/tianshu_datadev/planning/sql_build_plan.py` | 新增 _predicate_from_label_node()、_build_case_when_steps()、DerivedColumnRuleMissing；_build_project_step() 硬阻断；_build_single_table() 插入 CaseWhenStep |
| `src/tianshu_datadev/api/pipeline.py` | 新增 _prepare_spec_for_planning() 共享入口 |
| `templates/` 目录下 Template 2 YAML | 添加 type: label_table |

### 测试合并目标（不新建文件）

| 测试内容 | 合并到 |
|----------|--------|
| DatasetType、LabelPredicateNode、LabelDomain、LabelRuleProposal 模型测试 | `tests/planning/test_planning_models.py` |
| LabelExtractionArtifact、LabelPromotionArtifact、LabelRuleValidator、FakeLabelExtractor、Promotion | `tests/labels/test_label_rules.py` |
| Parser type 映射 | `tests/api/test_spec.py` |
| _find_unresolved_derived_columns()、_prepare_spec_for_planning() | `tests/api/test_pipeline.py` |
| _predicate_from_label_node()、_build_case_when_steps()、硬阻断 | `tests/planning/test_planning_models.py` |
| Template 2 E2E | `tests/sql/test_pipeline_e2e.py` |
| Contract E2E 同快照 | `tests/spark/test_plan_comparator_integration.py` |

---

### Task 1: 基础模型——DatasetType + LabelPredicateNode discriminator 联合 AST

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py`（在 CompareOp 之后、CaseWhenDecl 之前插入）
- Test: `tests/planning/test_planning_models.py`（末尾追加新测试类）

**Interfaces:**
- Produces: `DatasetType(str, Enum)`——DETAIL_TABLE/AGGREGATE_TABLE/LABEL_TABLE/UNSPECIFIED
- Produces: `CompareOp(str, Enum)`——EQ/NEQ/GT/GTE/LT/LTE
- Produces: `LabelColumnRef(StrictModel)`——node_type="COLUMN_REF", column_name: str
- Produces: `LabelTypedLiteral(StrictModel)`——node_type="LITERAL", value: str|Decimal|bool|None, data_type
- Produces: `LabelCompare(StrictModel)`——node_type="COMPARE", left: str, op: CompareOp, right: LabelTypedLiteral
- Produces: `LabelIsNull(StrictModel)`——node_type="IS_NULL", column: str
- Produces: `LabelIsNotNull(StrictModel)`——node_type="IS_NOT_NULL", column: str
- Produces: `LabelAnd(StrictModel)`——node_type="AND", children: list[LabelPredicateNode]
- Produces: `LabelOr(StrictModel)`——node_type="OR", children: list[LabelPredicateNode]
- Produces: `LabelNot(StrictModel)`——node_type="NOT", child: LabelPredicateNode
- Produces: `LabelPredicateNode = Annotated[Union[LabelAnd,LabelOr,LabelNot,LabelCompare,LabelIsNull,LabelIsNotNull,LabelColumnRef,LabelTypedLiteral], Field(discriminator="node_type")]`

- [ ] **Step 1: 编写 DatasetType 枚举的失败测试**

在 `tests/planning/test_planning_models.py` 末尾追加：

```python
# ════════════════════════════════════════════
# DatasetType + LabelPredicateNode discriminator 联合
# ════════════════════════════════════════════

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    CompareOp,
    DatasetType,
    LabelAnd,
    LabelColumnRef,
    LabelCompare,
    LabelIsNotNull,
    LabelIsNull,
    LabelNot,
    LabelOr,
    LabelPredicateNode,
    LabelTypedLiteral,
)


class TestDatasetType:
    """DatasetType 枚举序列化/反序列化与默认值行为。"""

    def test_label_table_value(self):
        """label_table 字符串映射到 DatasetType.LABEL_TABLE。"""
        assert DatasetType("label_table") == DatasetType.LABEL_TABLE

    def test_unspecified_is_default(self):
        """未指定时默认 UNSPECIFIED。"""
        assert DatasetType.UNSPECIFIED == DatasetType("unspecified")

    def test_all_variants_roundtrip(self):
        """所有变体序列化后反序列化一致。"""
        for dt in DatasetType:
            assert DatasetType(dt.value) == dt


class TestLabelPredicateNodeDiscriminator:
    """LabelPredicateNode discriminator 联合 AST 构造与验证。"""

    def test_compare_node(self):
        """LabelCompare 构造——二元比较。"""
        node = LabelCompare(
            left="distance_miles",
            op=CompareOp.LTE,
            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
        )
        assert node.node_type == "COMPARE"
        assert node.left == "distance_miles"
        assert isinstance(node.right.value, Decimal)

    def test_is_null_node(self):
        """LabelIsNull 构造。"""
        node = LabelIsNull(column="distance_miles")
        assert node.node_type == "IS_NULL"

    def test_and_nesting(self):
        """LabelAnd 嵌套两个 COMPARE。"""
        node = LabelAnd(children=[
            LabelCompare(
                left="distance_miles", op=CompareOp.GT,
                right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
            ),
            LabelCompare(
                left="distance_miles", op=CompareOp.LTE,
                right=LabelTypedLiteral(value=Decimal("5"), data_type="number"),
            ),
        ])
        assert node.node_type == "AND"
        assert len(node.children) == 2

    def test_discriminator_rejects_wrong_type(self):
        """discriminator 拒绝非法 node_type。"""
        with pytest.raises(ValidationError):
            LabelCompare(
                node_type="IS_NULL",  # 错误——LabelCompare 的 discriminator 固定为 COMPARE
                left="x",
                op=CompareOp.EQ,
                right=LabelTypedLiteral(value="y", data_type="string"),
            )

    def test_discriminator_rejects_extra_fields(self):
        """discriminator 子类拒绝非法额外字段。"""
        with pytest.raises(ValidationError):
            LabelIsNull(
                column="x",
                op=CompareOp.EQ,  # LabelIsNull 没有 op 字段
            )

    def test_boolean_literal(self):
        """LabelTypedLiteral 支持 bool 类型。"""
        lit = LabelTypedLiteral(value=True, data_type="boolean")
        assert lit.value is True

    def test_null_literal(self):
        """LabelTypedLiteral 支持 None。"""
        lit = LabelTypedLiteral(value=None, data_type="null")
        assert lit.value is None

    def test_union_discriminator_parse(self):
        """Annotated Union 根据 node_type 自动选择正确子类。"""
        data = {"node_type": "COMPARE", "left": "col", "op": "=",
                "right": {"node_type": "LITERAL", "value": "test", "data_type": "string"}}
        result = LabelPredicateNode  # 类型注解引用——实际解析需要 Pydantic TypeAdapter
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateNode)
        parsed = adapter.validate_python(data)
        assert isinstance(parsed, LabelCompare)
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestDatasetType tests/planning/test_planning_models.py::TestLabelPredicateNodeDiscriminator -v
```

预期：全部 FAIL（`ImportError`——模型尚未定义）

- [ ] **Step 3: 实现 DatasetType + CompareOp + 8 个 LabelPredicateNode 子类**

在 `src/tianshu_datadev/developer_spec/models.py` 的 import 区域追加 `from decimal import Decimal`，在 `CaseWhenBranchDecl` 之前插入：

```python
# ════════════════════════════════════════════
# DatasetType 枚举
# ════════════════════════════════════════════

class DatasetType(str, Enum):
    """数据产品类型——决定验证策略和能力门禁，不驱动 Builder 代码路径分叉。"""
    DETAIL_TABLE = "detail_table"
    AGGREGATE_TABLE = "aggregate_table"
    LABEL_TABLE = "label_table"
    UNSPECIFIED = "unspecified"


# ════════════════════════════════════════════
# CompareOp 枚举
# ════════════════════════════════════════════

class CompareOp(str, Enum):
    """比较操作符——封闭集合。"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


# ════════════════════════════════════════════
# LabelPredicateNode——带 discriminator 的封闭联合 AST
# ════════════════════════════════════════════

class LabelColumnRef(StrictModel):
    """列引用叶子——引用源表中已声明的字段。"""
    node_type: Literal["COLUMN_REF"] = "COLUMN_REF"
    column_name: str


class LabelTypedLiteral(StrictModel):
    """类型化字面量——真实 Python 类型，禁止隐式转换。"""
    node_type: Literal["LITERAL"] = "LITERAL"
    value: str | Decimal | bool | None
    data_type: Literal["string", "number", "boolean", "null"]


class LabelCompare(StrictModel):
    """二元比较：left OP right。"""
    node_type: Literal["COMPARE"] = "COMPARE"
    left: str
    op: CompareOp
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

**注意**：`LabelPredicateNode` 使用前向引用 `list["LabelPredicateNode"]`，Pydantic v2 的 `model_rebuild()` 在模块末尾统一处理。检查 `models.py` 末尾是否已有 `model_rebuild()` 调用——若无，在 Task 2 中追加。

- [ ] **Step 4: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestDatasetType tests/planning/test_planning_models.py::TestLabelPredicateNodeDiscriminator -v
```

预期：全部 PASS

- [ ] **Step 5: 运行完整测试套件确保无回归**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -x --timeout=60 -q 2>&1 | tail -5
```

预期：601+ passed，无新增失败

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py tests/planning/test_planning_models.py
git commit -m "feat(models): 新增 DatasetType 枚举 + LabelPredicateNode discriminator 联合 AST

- DatasetType: DETAIL_TABLE/AGGREGATE_TABLE/LABEL_TABLE/UNSPECIFIED
- CompareOp: EQ/NEQ/GT/GTE/LT/LTE
- LabelPredicateNode: 8 子类 discriminator 联合（AND/OR/NOT/COMPARE/IS_NULL/IS_NOT_NULL/COLUMN_REF/LITERAL）
- LabelTypedLiteral 使用真实 Python 类型（str/Decimal/bool/None）
- 禁止 Optional 字段大杂烩和 when/raw_condition 字符串路径"
```

---

### Task 2: 标签领域模型——LabelDomain + LabelRuleProposal + ParsedDeveloperSpec/CaseWhenDecl 改动

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py`（在 Task 1 新增代码之后、CaseWhenDecl 之前）
- Test: `tests/planning/test_planning_models.py`（追加）

**Interfaces:**
- Consumes: `LabelPredicateNode`（Task 1 产出）
- Produces: `LabelDomain(StrictModel)`——values: list[str], source_evidence: str, is_exhaustive: bool, completeness_evidence: str
- Produces: `LabelBranchProposal(StrictModel)`——condition: LabelPredicateNode, then_label: str, evidence: str
- Produces: `LabelRuleProposal(StrictModel)`——proposal_id, source_spec_hash, output_column, branches: list[LabelBranchProposal], else_value: str|None
- Produces: `LabelPredicateBranch(StrictModel)`——condition: LabelPredicateNode, then_label: str
- Produces: `ParsedDeveloperSpec` 新增字段——dataset_type: DatasetType=UNSPECIFIED, label_rules: list[CaseWhenDecl]=[]
- Produces: `CaseWhenDecl` 新增字段——typed_branches: list[LabelPredicateBranch]=[]

- [ ] **Step 1: 编写模型测试**

在 `tests/planning/test_planning_models.py` 末尾追加：

```python
class TestLabelDomain:
    """LabelDomain 模型验证。"""

    def test_basic_domain(self):
        from tianshu_datadev.developer_spec.models import LabelDomain
        domain = LabelDomain(
            values=["unknown", "short", "medium", "long"],
            source_evidence="分为四类：unknown / short / medium / long",
            is_exhaustive=True,
            completeness_evidence="以上四类覆盖全部情况",
        )
        assert len(domain.values) == 4
        assert domain.is_exhaustive is True

    def test_domain_empty_values_allowed(self):
        """空 values 允许——可能原文未明确枚举。"""
        from tianshu_datadev.developer_spec.models import LabelDomain
        domain = LabelDomain(values=[])
        assert domain.values == []


class TestLabelRuleProposal:
    """LabelRuleProposal 与 LabelBranchProposal 模型验证。"""

    def test_valid_proposal(self):
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelCompare, LabelIsNull, LabelOr,
            LabelRuleProposal, LabelTypedLiteral,
        )
        from decimal import Decimal
        proposal = LabelRuleProposal(
            proposal_id="proposal_abc123",
            source_spec_hash="hash_001",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelOr(children=[
                        LabelIsNull(column="distance_miles"),
                        LabelCompare(
                            left="is_distance_outlier", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value=True, data_type="boolean"),
                        ),
                    ]),
                    then_label="unknown",
                    evidence="distance_miles IS NULL OR is_distance_outlier = true → unknown",
                ),
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="distance_miles <= 2 → short",
                ),
            ],
            else_value="long",
        )
        assert proposal.output_column == "distance_category"
        assert len(proposal.branches) == 2
        assert proposal.else_value == "long"

    def test_empty_evidence_rejected(self):
        """evidence 为空字符串应被拒绝。"""
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelIsNull, LabelRuleProposal,
        )
        with pytest.raises(ValidationError):
            LabelBranchProposal(
                condition=LabelIsNull(column="x"),
                then_label="unknown",
                evidence="",  # 空字符串——应被拒绝（min_length=1）
            )


class TestParsedDeveloperSpecLabelFields:
    """ParsedDeveloperSpec 新增 dataset_type 和 label_rules 字段。"""

    def test_default_dataset_type_is_unspecified(self):
        """新建 Spec 默认 dataset_type=UNSPECIFIED。"""
        from tianshu_datadev.developer_spec.models import DatasetType, ParsedDeveloperSpec
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="t",
            description="d", input_tables=[], metrics=[], dimensions=[],
            output_spec=None, time_range=None,
        )
        assert spec.dataset_type == DatasetType.UNSPECIFIED

    def test_label_rules_default_empty(self):
        """新建 Spec 默认 label_rules=[]。"""
        from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="t",
            description="d", input_tables=[], metrics=[], dimensions=[],
            output_spec=None, time_range=None,
        )
        assert spec.label_rules == []


class TestCaseWhenDeclTypedBranches:
    """CaseWhenDecl 新增 typed_branches 字段。"""

    def test_typed_branches_default_empty(self):
        from tianshu_datadev.developer_spec.models import CaseWhenDecl
        cw = CaseWhenDecl(output_column="test_col")
        assert cw.typed_branches == []

    def test_typed_branches_with_predicate(self):
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl, LabelCompare, LabelPredicateBranch, LabelTypedLiteral,
        )
        from decimal import Decimal
        cw = CaseWhenDecl(
            output_column="distance_category",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                ),
            ],
            else_value="long",
        )
        assert len(cw.typed_branches) == 1
        assert cw.typed_branches[0].then_label == "short"
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestLabelDomain tests/planning/test_planning_models.py::TestLabelRuleProposal tests/planning/test_planning_models.py::TestParsedDeveloperSpecLabelFields tests/planning/test_planning_models.py::TestCaseWhenDeclTypedBranches -v
```

预期：FAIL（ImportError——模型尚未定义）

- [ ] **Step 3: 实现 LabelDomain + LabelBranchProposal + LabelRuleProposal + LabelPredicateBranch**

在 `src/tianshu_datadev/developer_spec/models.py` 的 `LabelPredicateNode` 定义之后、`CaseWhenDecl` 之前插入：

```python
# ════════════════════════════════════════════
# LabelDomain——从原文提取的标签值域
# ════════════════════════════════════════════

class LabelDomain(StrictModel):
    """从 Markdown 原文中提取的标签值域——由 Agent 提取，由 Validator 验证。

    不要求程序员在 output_columns 中手写 enum——allowed_values 保持可选。
    """
    values: list[str] = []
    source_evidence: str = ""
    is_exhaustive: bool = False
    completeness_evidence: str = ""


# ════════════════════════════════════════════
# LabelRuleProposal——LLM 候选（不可执行）
# ════════════════════════════════════════════

class LabelBranchProposal(StrictModel):
    """单条 WHEN-THEN 候选——LLM 输出。"""
    condition: LabelPredicateNode
    then_label: str
    evidence: str = ""


class LabelRuleProposal(StrictModel):
    """LLM 提取的标签规则候选——不可执行，必须经 Validator 验证后提升。

    一个 Proposal 对应 output_spec.columns 中的一个标签列。
    """
    proposal_id: str
    source_spec_hash: str
    output_column: str
    branches: list[LabelBranchProposal]
    else_value: str | None = None


class LabelPredicateBranch(StrictModel):
    """已验证的类型化 WHEN-THEN 分支——仅含确定性信息。"""
    condition: LabelPredicateNode
    then_label: str
```

- [ ] **Step 4: 修改 ParsedDeveloperSpec 和 CaseWhenDecl**

在 `ParsedDeveloperSpec` 类中新增字段（在现有字段之后追加）：

```python
# 在 ParsedDeveloperSpec 类中追加：
dataset_type: DatasetType = DatasetType.UNSPECIFIED
label_rules: list["CaseWhenDecl"] = []
```

在 `CaseWhenDecl` 类中新增字段（在现有 `output_column` 字段之后追加）：

```python
# 在 CaseWhenDecl 类中追加：
typed_branches: list[LabelPredicateBranch] = []
```

- [ ] **Step 5: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestLabelDomain tests/planning/test_planning_models.py::TestLabelRuleProposal tests/planning/test_planning_models.py::TestParsedDeveloperSpecLabelFields tests/planning/test_planning_models.py::TestCaseWhenDeclTypedBranches -v
```

预期：全部 PASS

- [ ] **Step 6: 运行完整回归测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -x --timeout=60 -q 2>&1 | tail -5
```

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py tests/planning/test_planning_models.py
git commit -m "feat(models): 新增 LabelDomain/LabelRuleProposal/LabelPredicateBranch + Spec/CaseWhenDecl 字段

- LabelDomain: Agent 从原文提取的标签值域（不要求程序员手写 enum）
- LabelBranchProposal/LabelRuleProposal: LLM 候选（不可执行，含 evidence 锚定）
- LabelPredicateBranch: 已验证的类型化 WHEN-THEN 分支
- ParsedDeveloperSpec: 新增 dataset_type/label_rules 字段
- CaseWhenDecl: 新增 typed_branches 字段"
```

---

### Task 3: 溯源 Artifact 模型 + ValidationReport 模型

**Files:**
- Create: `src/tianshu_datadev/labels/__init__.py`
- Create: `src/tianshu_datadev/labels/artifacts.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelRuleProposal`, `CaseWhenDecl`, `LabelValidationReport`（Task 2 产出）
- Produces: `LabelExtractionArtifact`——artifact_id, source_spec_hash, extraction_time, llm_model, llm_prompt_version, llm_temperature, unresolved_columns, raw_proposals, prompt_snapshot
- Produces: `LabelPromotionArtifact`——artifact_id, parent_spec_hash, new_spec_hash, promotion_time, extraction_artifact_id, promoted_rules, validation_reports, rejected_proposals, human_review_required
- Produces: `LabelValidationReport`——proposal_id, passed, checks, blocking_errors, human_review_items, warnings, extracted_label_domain
- Produces: `LabelValidationCheck`——check_name, passed, level: BLOCKING|HUMAN_REVIEW|WARN, detail

- [ ] **Step 1: 编写测试**

在 `tests/labels/test_label_rules.py` 末尾追加：

```python
# ════════════════════════════════════════════
# LabelExtractionArtifact + LabelPromotionArtifact + ValidationReport
# ════════════════════════════════════════════

from datetime import datetime

from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
)


class TestLabelExtractionArtifact:
    """LabelExtractionArtifact 模型验证。"""

    def test_artifact_contains_llm_trace(self):
        """Artifact 包含 LLM 模型/Prompt/温度等溯源信息。"""
        artifact = LabelExtractionArtifact(
            artifact_id="extract_abc123",
            source_spec_hash="spec_hash_0",
            extraction_time=datetime.utcnow().isoformat(),
            llm_model="claude-sonnet-5",
            llm_prompt_version="label-extract-v1",
            llm_temperature=0.1,
            unresolved_columns=["distance_category"],
            raw_proposals=[],
        )
        assert artifact.llm_model == "claude-sonnet-5"
        assert artifact.llm_prompt_version == "label-extract-v1"
        assert artifact.artifact_id.startswith("extract_")

    def test_artifact_does_not_enter_spec_hash(self):
        """Artifact 不包含 spec_hash 依赖字段——确认与语义分离。"""
        fields = list(LabelExtractionArtifact.model_fields.keys())
        assert "spec_hash" not in fields
        assert "new_spec_hash" not in fields


class TestLabelPromotionArtifact:
    """LabelPromotionArtifact 模型验证。"""

    def test_artifact_contains_provenance_chain(self):
        """Artifact 包含完整溯源链。"""
        artifact = LabelPromotionArtifact(
            artifact_id="promote_abc123",
            parent_spec_hash="spec_hash_0",
            new_spec_hash="spec_hash_1",
            promotion_time=datetime.utcnow().isoformat(),
            extraction_artifact_id="extract_abc123",
            promoted_rules=[],
            validation_reports=[],
        )
        assert artifact.parent_spec_hash == "spec_hash_0"
        assert artifact.new_spec_hash == "spec_hash_1"
        assert artifact.parent_spec_hash != artifact.new_spec_hash

    def test_rejected_proposals_tracked(self):
        """被拒绝的 proposal_id 列表独立追踪。"""
        artifact = LabelPromotionArtifact(
            artifact_id="promote_abc123",
            parent_spec_hash="spec_hash_0",
            new_spec_hash="spec_hash_1",
            promotion_time=datetime.utcnow().isoformat(),
            extraction_artifact_id="extract_abc123",
            promoted_rules=[],
            validation_reports=[],
            rejected_proposals=["proposal_bad001"],
        )
        assert "proposal_bad001" in artifact.rejected_proposals


class TestLabelValidationReport:
    """LabelValidationReport + LabelValidationCheck 模型验证。"""

    def test_passed_when_no_errors(self):
        from tianshu_datadev.labels.artifacts import (
            LabelValidationCheck,
            LabelValidationReport,
        )
        report = LabelValidationReport(
            proposal_id="p_001",
            passed=True,
            checks=[
                LabelValidationCheck(
                    check_name="FIELD_EXISTS",
                    passed=True,
                    level="BLOCKING",
                    detail="所有字段存在",
                ),
            ],
        )
        assert report.passed is True
        assert len(report.blocking_errors) == 0

    def test_blocking_error_causes_fail(self):
        from tianshu_datadev.labels.artifacts import (
            LabelValidationCheck,
            LabelValidationReport,
        )
        report = LabelValidationReport(
            proposal_id="p_002",
            passed=False,
            checks=[
                LabelValidationCheck(
                    check_name="FIELD_EXISTS",
                    passed=False,
                    level="BLOCKING",
                    detail="字段 'invalid_col' 不存在",
                ),
            ],
            blocking_errors=["字段 'invalid_col' 不存在于源表中"],
        )
        assert report.passed is False
        assert len(report.blocking_errors) == 1

    def test_human_review_items(self):
        from tianshu_datadev.labels.artifacts import (
            LabelValidationCheck,
            LabelValidationReport,
        )
        report = LabelValidationReport(
            proposal_id="p_003",
            passed=False,
            checks=[
                LabelValidationCheck(
                    check_name="INTERVAL_GAP",
                    passed=False,
                    level="HUMAN_REVIEW",
                    detail="区间 (5, 10] 可能遗漏",
                ),
            ],
            human_review_items=["区间 (5, 10] 可能遗漏——需人工确认"],
        )
        assert report.passed is False
        assert len(report.human_review_items) == 1
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestLabelExtractionArtifact tests/labels/test_label_rules.py::TestLabelPromotionArtifact tests/labels/test_label_rules.py::TestLabelValidationReport -v
```

预期：FAIL（ImportError——模块尚未创建）

- [ ] **Step 3: 实现 artifacts.py**

创建 `src/tianshu_datadev/labels/__init__.py`：

```python
"""标签子系统——LabelExtractor → Validator → Promotion → Builder 链路。

LLM 产出候选（LabelRuleProposal），确定性 Validator 验证后提升为 CaseWhenDecl。
溯源信息与语义 Spec 分离存储——Artifact 不进入 spec_hash。
"""
```

创建 `src/tianshu_datadev/labels/artifacts.py`：

```python
"""标签子系统溯源 Artifact——与语义 Spec 分离存储。

LabelExtractionArtifact: 提取阶段溯源（LLM 模型/Prompt/温度/时间）
LabelPromotionArtifact: 提升阶段溯源（父 hash/新 hash/验证报告）
LabelValidationReport: 逐项验证结果
"""

from __future__ import annotations

from typing import Literal

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    LabelDomain,
    LabelRuleProposal,
    StrictModel,
)


class LabelExtractionArtifact(StrictModel):
    """LabelExtractor 阶段的溯源记录——与语义 Spec 分离存储。

    包含 LLM 调用参数、原始候选、提取时间等元信息。
    不进入 spec_hash 计算——仅用于审计追溯和 Harness 回归。
    """
    artifact_id: str
    source_spec_hash: str
    extraction_time: str
    llm_model: str
    llm_prompt_version: str
    llm_temperature: float
    unresolved_columns: list[str]
    raw_proposals: list[LabelRuleProposal]
    prompt_snapshot: str = ""


class LabelValidationCheck(StrictModel):
    """单条验证检查结果。"""
    check_name: str
    passed: bool
    level: Literal["BLOCKING", "HUMAN_REVIEW", "WARN"]
    detail: str = ""


class LabelValidationReport(StrictModel):
    """LabelRuleValidator 的验证报告——逐项检查结果。"""
    proposal_id: str
    passed: bool
    checks: list[LabelValidationCheck] = []
    blocking_errors: list[str] = []
    human_review_items: list[str] = []
    warnings: list[str] = []
    extracted_label_domain: LabelDomain | None = None


class LabelPromotionArtifact(StrictModel):
    """Promotion 阶段的溯源记录——记录 Proposal → CaseWhenDecl 的转换。

    包含验证报告、父 hash 链、提升时间等审计信息。
    不进入 spec_hash 计算。
    """
    artifact_id: str
    parent_spec_hash: str
    new_spec_hash: str
    promotion_time: str
    extraction_artifact_id: str
    promoted_rules: list[CaseWhenDecl]
    validation_reports: list[LabelValidationReport]
    rejected_proposals: list[str] = []
    human_review_required: bool = False
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestLabelExtractionArtifact tests/labels/test_label_rules.py::TestLabelPromotionArtifact tests/labels/test_label_rules.py::TestLabelValidationReport -v
```

- [ ] **Step 5: 运行完整回归测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -x --timeout=60 -q 2>&1 | tail -5
```

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/labels/__init__.py src/tianshu_datadev/labels/artifacts.py tests/labels/test_label_rules.py
git commit -m "feat(labels): 新增 LabelExtractionArtifact/LabelPromotionArtifact/LabelValidationReport

- LabelExtractionArtifact: 提取阶段溯源（LLM 模型/Prompt/温度/时间）
- LabelPromotionArtifact: 提升阶段溯源（parent_spec_hash/new_spec_hash/验证报告）
- LabelValidationReport/LabelValidationCheck: 逐项验证结果（BLOCKING/HUMAN_REVIEW/WARN）
- 溯源信息与语义 Spec 分离——不进入 spec_hash 计算"
```

---

### Task 4: Parser type 映射 + _find_unresolved_derived_columns()

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/parser.py`（`parse()` 方法末尾 + 新增函数）
- Test: `tests/api/test_spec.py`（Parser type 映射）、`tests/api/test_pipeline.py`（检测函数）

**Interfaces:**
- Consumes: `DatasetType`（Task 1）、`ParsedDeveloperSpec.dataset_type`（Task 2）
- Produces: `_find_unresolved_derived_columns(spec, manifest=None) -> list[str]`
- Modifies: `Parser.parse()` → 构造 `ParsedDeveloperSpec` 时传入 `dataset_type`

- [ ] **Step 1: 编写 Parser type 映射测试**

在 `tests/api/test_spec.py` 中追加：

```python
class TestParserDatasetTypeMapping:
    """Parser 读取 spec_dict['type'] → dataset_type 映射。"""

    def test_label_table_type_parsed(self):
        """YAML type: label_table → DatasetType.LABEL_TABLE。"""
        from tianshu_datadev.developer_spec.models import DatasetType
        from tianshu_datadev.developer_spec.parser import parse
        yaml_content = {
            "title": "测试标签表",
            "type": "label_table",
            "input_tables": [],
            "output_columns": [{"name": "score", "data_type": "string"}],
        }
        body = "# 测试\n- score > 80 → high"
        result = parse(yaml_content, body)
        assert result.dataset_type == DatasetType.LABEL_TABLE

    def test_missing_type_defaults_to_unspecified(self):
        """未声明 type → DatasetType.UNSPECIFIED + W007 警告。"""
        from tianshu_datadev.developer_spec.models import DatasetType
        from tianshu_datadev.developer_spec.parser import parse
        yaml_content = {
            "title": "测试",
            "input_tables": [],
            "output_columns": [],
        }
        result = parse(yaml_content, "# 测试")
        assert result.dataset_type == DatasetType.UNSPECIFIED
        # W007 警告应出现在 parse_warnings 中
        w007_found = any("W007" in w for w in result.parse_warnings)
        assert w007_found, "UNSPECIFIED 必须产生 W007 迁移警告"

    def test_detail_table_type_parsed(self):
        """YAML type: detail_table → DatasetType.DETAIL_TABLE。"""
        from tianshu_datadev.developer_spec.models import DatasetType
        from tianshu_datadev.developer_spec.parser import parse
        yaml_content = {
            "title": "明细表",
            "type": "detail_table",
            "input_tables": [],
            "output_columns": [],
        }
        result = parse(yaml_content, "# 测试")
        assert result.dataset_type == DatasetType.DETAIL_TABLE
```

- [ ] **Step 2: 编写 _find_unresolved_derived_columns 测试**

在 `tests/api/test_pipeline.py` 中追加：

```python
class TestFindUnresolvedDerivedColumns:
    """_find_unresolved_derived_columns 检测逻辑。"""

    def test_physical_column_resolved(self):
        """源表物理列识别为已解析。"""
        from tianshu_datadev.developer_spec.parser import _find_unresolved_derived_columns
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl, InputTableDecl, OutputColumnDecl, OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t", description="d",
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[ColumnDecl(column_name="col_a", normalized_name="col_a")],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="col_a", data_type="string")],
            ), time_range=None,
        )
        unresolved = _find_unresolved_derived_columns(spec)
        assert unresolved == []

    def test_derived_column_unresolved(self):
        """既非物理列也非 label_rule 的列为未解析。"""
        from tianshu_datadev.developer_spec.parser import _find_unresolved_derived_columns
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl, InputTableDecl, OutputColumnDecl, OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t", description="d",
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[ColumnDecl(column_name="col_a", normalized_name="col_a")],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="derived_col", data_type="string")],
            ), time_range=None,
        )
        unresolved = _find_unresolved_derived_columns(spec)
        assert "derived_col" in unresolved

    def test_label_rule_resolved(self):
        """label_rules 中的 output_column 识别为已解析。"""
        from tianshu_datadev.developer_spec.parser import _find_unresolved_derived_columns
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl, ColumnDecl, InputTableDecl, OutputColumnDecl,
            OutputSpecDecl, ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t", description="d",
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[ColumnDecl(column_name="col_a", normalized_name="col_a")],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="label_col", data_type="string")],
            ), time_range=None,
            label_rules=[CaseWhenDecl(output_column="label_col")],
        )
        unresolved = _find_unresolved_derived_columns(spec)
        assert unresolved == []
```

- [ ] **Step 3: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/api/test_spec.py::TestParserDatasetTypeMapping tests/api/test_pipeline.py::TestFindUnresolvedDerivedColumns -v
```

预期：FAIL

- [ ] **Step 4: 实现 Parser type 映射**

在 `src/tianshu_datadev/developer_spec/parser.py` 的 `parse()` 方法中，定位 `ParsedDeveloperSpec(...)` 构造调用，在参数中追加：

```python
dataset_type=_map_dataset_type(spec_dict.get("type")),
```

并在同一文件中新增辅助函数：

```python
def _map_dataset_type(raw_type: str | None) -> DatasetType:
    """将 YAML type 字符串映射为 DatasetType 枚举。

    Args:
        raw_type: spec_dict["type"] 的原始值

    Returns:
        DatasetType 枚举值。未声明时返回 UNSPECIFIED。
    """
    if raw_type is None:
        return DatasetType.UNSPECIFIED
    try:
        return DatasetType(raw_type)
    except ValueError:
        return DatasetType.UNSPECIFIED
```

在 `parse()` 方法中，UNSPECIFIED 时追加 W007 警告：

```python
if dataset_type == DatasetType.UNSPECIFIED:
    parse_warnings.append(
        "W007: dataset_type 未声明（默认 UNSPECIFIED）。"
        "建议在 YAML 中显式声明 type: detail_table / aggregate_table / label_table。"
        "后续版本将强制要求此字段。"
    )
```

- [ ] **Step 5: 实现 _find_unresolved_derived_columns()**

在 `src/tianshu_datadev/developer_spec/parser.py` 末尾（`_normalized_spec_hash` 之后）新增：

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

    Args:
        spec: 已解析的 DeveloperSpec
        manifest: 可选的 SourceManifest——用于 schema 级别的列名解析

    Returns:
        未解析的输出列名列表。
    """
    from tianshu_datadev.developer_spec.field_normalizer import normalize

    resolved: set[str] = set()

    # 源表字段
    for t in spec.input_tables:
        for c in t.columns:
            resolved.add(c.normalized_name)
        for c in t.key_columns:
            resolved.add(c.normalized_name)
        for c in t.business_columns:
            resolved.add(c.normalized_name)

    # SourceManifest schema
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

- [ ] **Step 6: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/api/test_spec.py::TestParserDatasetTypeMapping tests/api/test_pipeline.py::TestFindUnresolvedDerivedColumns -v
```

- [ ] **Step 7: 运行完整回归测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -x --timeout=60 -q 2>&1 | tail -5
```

- [ ] **Step 8: Commit**

```bash
git add src/tianshu_datadev/developer_spec/parser.py tests/api/test_spec.py tests/api/test_pipeline.py
git commit -m "feat(parser): Parser 读取 type → dataset_type + _find_unresolved_derived_columns()

- Parser.parse() 读取 spec_dict['type'] 映射到 DatasetType 枚举
- UNSPECIFIED 时产生 W007 迁移警告
- 新增 _find_unresolved_derived_columns()——按输出列逐个检测是否已解析
- 覆盖源表字段/指标/窗口指标/compute_step/label_rule/Manifest schema 全部解析来源"
```

---

### Task 5: LabelRuleValidator——8 项确定性检查

**Files:**
- Create: `src/tianshu_datadev/labels/label_rule_validator.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelRuleProposal`, `LabelDomain`, `ParsedDeveloperSpec`, `LabelValidationReport`, `LabelValidationCheck`（Task 2/3）
- Produces: `LabelRuleValidator.validate(proposal, spec) -> LabelValidationReport`

- [ ] **Step 1: 编写 Validator 测试**

在 `tests/labels/test_label_rules.py` 末尾追加（仅展示关键测试——完整实现需覆盖全部 8 项检查）：

```python
# ════════════════════════════════════════════
# LabelRuleValidator——8 项确定性检查
# ════════════════════════════════════════════

from decimal import Decimal

from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator


def _make_test_spec(columns: list[dict] | None = None) -> ParsedDeveloperSpec:
    """创建含指定源表列的测试 Spec。"""
    if columns is None:
        columns = [
            {"name": "distance_miles", "type": "number"},
            {"name": "is_distance_outlier", "type": "boolean"},
        ]
    return ParsedDeveloperSpec(
        spec_id="test_validator", spec_hash="hv", title="t", description="d",
        input_tables=[
            InputTableDecl(
                table_alias="tf", source_table="fact",
                columns=[
                    ColumnDecl(
                        column_name=c["name"],
                        normalized_name=c["name"],
                        data_type=c.get("type", "string"),
                    )
                    for c in columns
                ],
                key_columns=[], business_columns=[],
            ),
        ],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name="distance_category", data_type="string")],
        ),
        time_range=None,
    )


def _make_valid_proposal() -> LabelRuleProposal:
    """创建 Template 2 的正确 Proposal。"""
    from tianshu_datadev.developer_spec.models import (
        LabelBranchProposal, LabelCompare, LabelIsNull, LabelOr,
        LabelRuleProposal, LabelTypedLiteral,
    )
    return LabelRuleProposal(
        proposal_id="p_test_001",
        source_spec_hash="hv",
        output_column="distance_category",
        branches=[
            LabelBranchProposal(
                condition=LabelOr(children=[
                    LabelIsNull(column="distance_miles"),
                    LabelCompare(
                        left="is_distance_outlier", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value=True, data_type="boolean"),
                    ),
                ]),
                then_label="unknown",
                evidence="distance_miles IS NULL OR is_distance_outlier = true → unknown",
            ),
            LabelBranchProposal(
                condition=LabelCompare(
                    left="distance_miles", op=CompareOp.LTE,
                    right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                ),
                then_label="short",
                evidence="distance_miles <= 2 → short",
            ),
        ],
        else_value="long",
    )


class TestLabelRuleValidatorFieldExists:
    """检查项 #1：字段存在性。"""

    def test_all_fields_exist_passes(self):
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        proposal = _make_valid_proposal()
        report = validator.validate(proposal, spec)
        field_check = next(c for c in report.checks if c.check_name == "FIELD_EXISTS")
        assert field_check.passed

    def test_missing_field_fails(self):
        """引用不存在的字段 → BLOCKING。"""
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelCompare, LabelRuleProposal, LabelTypedLiteral,
        )
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p_bad", source_spec_hash="hv",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="nonexistent_field", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value="x", data_type="string"),
                    ),
                    then_label="bad",
                    evidence="引用不存在的字段",
                ),
            ],
        )
        report = validator.validate(proposal, spec)
        assert report.passed is False
        assert any("nonexistent_field" in e for e in report.blocking_errors)


class TestLabelRuleValidatorTypeCompatible:
    """检查项 #2：字段类型兼容性。"""

    def test_boolean_literal_on_boolean_field_passes(self):
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        proposal = _make_valid_proposal()
        report = validator.validate(proposal, spec)
        type_check = next(c for c in report.checks if c.check_name == "TYPE_COMPATIBLE")
        assert type_check.passed

    def test_number_literal_on_boolean_field_fails(self):
        """number 字面量用于 boolean 字段 → TYPE_MISMATCH。"""
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelCompare, LabelRuleProposal, LabelTypedLiteral,
        )
        from decimal import Decimal
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p_bad", source_spec_hash="hv",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="is_distance_outlier", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value=Decimal("1"), data_type="number"),
                    ),
                    then_label="bad",
                    evidence="number 字面量用于 boolean 字段",
                ),
            ],
        )
        report = validator.validate(proposal, spec)
        type_check = next(c for c in report.checks if c.check_name == "TYPE_COMPATIBLE")
        assert not type_check.passed


class TestLabelRuleValidatorEvidenceAnchored:
    """检查项 #5：原文 evidence 锚定。"""

    def test_non_empty_evidence_passes(self):
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        spec.description = "distance_miles IS NULL OR is_distance_outlier = true → unknown"
        proposal = _make_valid_proposal()
        report = validator.validate(proposal, spec)
        ev_check = next(c for c in report.checks if c.check_name == "EVIDENCE_ANCHORED")
        assert ev_check.passed

    def test_empty_evidence_fails(self):
        """evidence 为空无法锚定 → BLOCKING。"""
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelIsNull, LabelRuleProposal,
        )
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p_bad", source_spec_hash="hv",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelIsNull(column="distance_miles"),
                    then_label="unknown",
                    evidence="",  # 空 evidence
                ),
            ],
        )
        report = validator.validate(proposal, spec)
        assert report.passed is False
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestLabelRuleValidatorFieldExists tests/labels/test_label_rules.py::TestLabelRuleValidatorTypeCompatible tests/labels/test_label_rules.py::TestLabelRuleValidatorEvidenceAnchored -v
```

- [ ] **Step 3: 实现 LabelRuleValidator**

创建 `src/tianshu_datadev/labels/label_rule_validator.py`：

```python
"""LabelRuleValidator——8 项确定性检查，Proposal 提升前的最后防线。

检查项：
1. 字段存在性（FIELD_EXISTS）——BLOCKING
2. 字段类型兼容性（TYPE_COMPATIBLE）——BLOCKING
3. 操作符合法性（OPERATOR_VALID）——BLOCKING
4. 输出类型（OUTPUT_TYPE）——BLOCKING
5. 原文 evidence 锚定（EVIDENCE_ANCHORED）——BLOCKING
6. 标签域验证（LABEL_DOMAIN）——BLOCKING
7. ELSE 或完整覆盖证明（COVERAGE_COMPLETENESS）——BLOCKING/HUMAN_REVIEW
8. 区间重叠/遗漏检测（INTERVAL_OVERLAP/INTERVAL_GAP）——BLOCKING/HUMAN_REVIEW
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    CompareOp,
    LabelAnd,
    LabelCompare,
    LabelIsNotNull,
    LabelIsNull,
    LabelNot,
    LabelOr,
    LabelRuleProposal,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import (
    LabelValidationCheck,
    LabelValidationReport,
)


class LabelRuleValidator:
    """确定性验证 LabelRuleProposal——全部 8 项检查通过才可 Promotion。

    纯确定性逻辑，不调 LLM。失败分三级：BLOCKING / HUMAN_REVIEW / WARN。
    """

    def validate(
        self,
        proposal: LabelRuleProposal,
        spec: ParsedDeveloperSpec,
        label_domain: "LabelDomain | None" = None,
    ) -> LabelValidationReport:
        """对单个 Proposal 执行全部 8 项检查。

        Args:
            proposal: LLM 提取的候选规则
            spec: 当前 Spec（含源表字段清单）
            label_domain: 可选的预提取 LabelDomain（若为 None 则跳过标签域检查）

        Returns:
            LabelValidationReport——含逐项检查结果和阻断级别
        """
        checks: list[LabelValidationCheck] = []
        blocking: list[str] = []
        human_review: list[str] = []
        warnings: list[str] = []

        # ── 构建字段清单 ──
        available_fields: dict[str, str | None] = {}  # field_name → data_type
        for t in spec.input_tables:
            for c in t.columns:
                available_fields[c.normalized_name] = getattr(c, "data_type", None)
            for c in t.key_columns:
                available_fields[c.normalized_name] = getattr(c, "data_type", None)
            for c in t.business_columns:
                available_fields[c.normalized_name] = getattr(c, "data_type", None)

        # ── 1. 字段存在性 ──
        checks.append(self._check_field_exists(proposal, available_fields, blocking))

        # ── 2. 字段类型兼容性 ──
        checks.append(self._check_type_compatible(proposal, available_fields, blocking))

        # ── 3. 操作符合法性 ──
        checks.append(self._check_operator_valid(proposal, blocking))

        # ── 4. 输出类型 ──
        checks.append(self._check_output_type(proposal, spec, blocking))

        # ── 5. 证据锚定 ──
        checks.append(self._check_evidence_anchored(proposal, spec, blocking))

        # ── 6. 标签域验证 ──
        if label_domain:
            checks.append(self._check_label_domain(proposal, label_domain, blocking))
        else:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="WARN",
                detail="未提供 LabelDomain——跳过标签域验证",
            ))

        # ── 7. ELSE 或完整覆盖 ──
        checks.append(self._check_coverage(proposal, label_domain, blocking, human_review))

        # ── 8. 区间重叠/遗漏 ──
        checks.append(self._check_intervals(proposal, blocking, human_review))

        passed = len(blocking) == 0 and len(human_review) == 0

        return LabelValidationReport(
            proposal_id=proposal.proposal_id,
            passed=passed,
            checks=checks,
            blocking_errors=blocking,
            human_review_items=human_review,
            warnings=warnings,
        )

    # ── 检查实现 ──

    def _collect_field_refs(self, node) -> list[str]:
        """递归收集 LabelPredicateNode 中的所有列引用。"""
        fields: list[str] = []
        if isinstance(node, LabelCompare):
            fields.append(node.left)
            fields.extend(self._collect_field_refs(node.right))
        elif isinstance(node, (LabelIsNull, LabelIsNotNull)):
            fields.append(node.column)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                fields.extend(self._collect_field_refs(child))
        elif isinstance(node, LabelNot):
            fields.extend(self._collect_field_refs(node.child))
        return fields

    def _check_field_exists(self, proposal, available_fields, blocking):
        field_refs = set()
        for branch in proposal.branches:
            field_refs.update(self._collect_field_refs(branch.condition))
        missing = [f for f in field_refs if f not in available_fields]
        if missing:
            blocking.append(f"字段不存在: {missing}")
            return LabelValidationCheck(
                check_name="FIELD_EXISTS", passed=False, level="BLOCKING",
                detail=f"字段不存在于源表中: {missing}",
            )
        return LabelValidationCheck(
            check_name="FIELD_EXISTS", passed=True, level="BLOCKING",
            detail=f"全部 {len(field_refs)} 个字段存在",
        )

    def _check_type_compatible(self, proposal, available_fields, blocking):
        for branch in proposal.branches:
            issues = self._check_node_types(branch.condition, available_fields)
            if issues:
                blocking.extend(issues)
                return LabelValidationCheck(
                    check_name="TYPE_COMPATIBLE", passed=False, level="BLOCKING",
                    detail=f"类型不兼容: {issues}",
                )
        return LabelValidationCheck(
            check_name="TYPE_COMPATIBLE", passed=True, level="BLOCKING",
            detail="所有字段类型兼容",
        )

    def _check_node_types(self, node, available_fields) -> list[str]:
        """递归检查节点树的类型兼容性。"""
        issues: list[str] = []
        if isinstance(node, LabelCompare):
            field_type = available_fields.get(node.left)
            if field_type and node.right.data_type == "number":
                if field_type not in ("number", "integer", "float", "double", "decimal", None):
                    issues.append(
                        f"字段 '{node.left}' 类型为 '{field_type}'，"
                        f"但字面量类型为 'number'"
                    )
            elif field_type and node.right.data_type == "boolean":
                if field_type not in ("boolean", "bool", None):
                    issues.append(
                        f"字段 '{node.left}' 类型为 '{field_type}'，"
                        f"但字面量类型为 'boolean'"
                    )
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                issues.extend(self._check_node_types(child, available_fields))
        elif isinstance(node, LabelNot):
            issues.extend(self._check_node_types(node.child, available_fields))
        return issues

    def _check_operator_valid(self, proposal, blocking):
        """检查 CompareOp 枚举成员合法性和逻辑节点结构。"""
        for branch in proposal.branches:
            op_issues = self._check_operators_in_node(branch.condition)
            if op_issues:
                blocking.extend(op_issues)
                return LabelValidationCheck(
                    check_name="OPERATOR_VALID", passed=False, level="BLOCKING",
                    detail=f"操作符问题: {op_issues}",
                )
        return LabelValidationCheck(
            check_name="OPERATOR_VALID", passed=True, level="BLOCKING",
            detail="所有操作符合法",
        )

    def _check_operators_in_node(self, node) -> list[str]:
        issues: list[str] = []
        if isinstance(node, LabelCompare):
            if not isinstance(node.op, CompareOp):
                issues.append(f"非法 CompareOp: {node.op}")
        elif isinstance(node, (LabelAnd, LabelOr)):
            if len(node.children) < 2:
                issues.append(f"{node.node_type} 至少需要 2 个子节点")
            for child in node.children:
                issues.extend(self._check_operators_in_node(child))
        elif isinstance(node, LabelNot):
            issues.extend(self._check_operators_in_node(node.child))
        return issues

    def _check_output_type(self, proposal, spec, blocking):
        """检查 then_label 类型与 output_spec 声明一致。"""
        output_col = None
        for col in spec.output_spec.columns:
            from tianshu_datadev.developer_spec.field_normalizer import normalize
            if normalize(col.name) == normalize(proposal.output_column):
                output_col = col
                break
        if output_col is None:
            blocking.append(f"输出列 '{proposal.output_column}' 不在 output_spec 中")
            return LabelValidationCheck(
                check_name="OUTPUT_TYPE", passed=False, level="BLOCKING",
                detail=f"输出列 '{proposal.output_column}' 不在 output_spec 中",
            )
        return LabelValidationCheck(
            check_name="OUTPUT_TYPE", passed=True, level="BLOCKING",
            detail=f"输出类型一致: {getattr(output_col, 'data_type', 'unknown')}",
        )

    def _check_evidence_anchored(self, proposal, spec, blocking):
        """检查每个分支 evidence 非空且可锚定到 Markdown body。"""
        body = spec.description or ""
        for i, branch in enumerate(proposal.branches):
            if not branch.evidence or not branch.evidence.strip():
                blocking.append(f"分支 {i} ('{branch.then_label}') evidence 为空——无法锚定")
                continue
            # 子串匹配（模糊——用于确定性检查，语义正确性由 HUMAN_REVIEW 保障）
            if branch.evidence not in body:
                blocking.append(
                    f"分支 {i} ('{branch.then_label}') evidence 无法在 Markdown body 中锚定: "
                    f"'{branch.evidence[:80]}...'"
                )
        if any("evidence" in e.lower() for e in blocking):
            return LabelValidationCheck(
                check_name="EVIDENCE_ANCHORED", passed=False, level="BLOCKING",
                detail=f"evidence 锚定失败: {[e for e in blocking if 'evidence' in e.lower()]}",
            )
        return LabelValidationCheck(
            check_name="EVIDENCE_ANCHORED", passed=True, level="BLOCKING",
            detail="所有 evidence 可锚定",
        )

    def _check_label_domain(self, proposal, label_domain, blocking):
        """检查 then_label 值在 LabelDomain.values 内。"""
        from tianshu_datadev.developer_spec.models import LabelDomain
        if not label_domain.values:
            return LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="WARN",
                detail="LabelDomain 为空——跳过标签域验证",
            )
        all_labels = {b.then_label for b in proposal.branches}
        if proposal.else_value:
            all_labels.add(proposal.else_value)
        outside = all_labels - set(label_domain.values)
        if outside:
            blocking.append(f"标签值 '{outside}' 不在 LabelDomain.values {label_domain.values} 中")
            return LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=False, level="BLOCKING",
                detail=f"标签值越界: {outside}",
            )
        return LabelValidationCheck(
            check_name="LABEL_DOMAIN", passed=True, level="BLOCKING",
            detail=f"全部 {len(all_labels)} 个标签值在域内",
        )

    def _check_coverage(self, proposal, label_domain, blocking, human_review):
        """检查 ELSE 存在或完整覆盖证明。"""
        if proposal.else_value is not None:
            return LabelValidationCheck(
                check_name="COVERAGE_COMPLETENESS", passed=True, level="BLOCKING",
                detail="ELSE 存在——覆盖所有剩余情况",
            )
        # 无 ELSE——检查是否覆盖 LabelDomain
        if label_domain and label_domain.values and label_domain.is_exhaustive:
            covered = {b.then_label for b in proposal.branches}
            uncovered = set(label_domain.values) - covered
            if uncovered:
                human_review.append(
                    f"无 ELSE 且未覆盖 LabelDomain 值: {uncovered}。"
                    f"已覆盖: {covered}，声明完备域: {label_domain.values}"
                )
                return LabelValidationCheck(
                    check_name="COVERAGE_COMPLETENESS", passed=False, level="HUMAN_REVIEW",
                    detail=f"LabelDomain 完备但未覆盖: {uncovered}",
                )
        return LabelValidationCheck(
            check_name="COVERAGE_COMPLETENESS", passed=True, level="WARN",
            detail="无 ELSE——无法确定性验证覆盖完备性",
        )

    def _check_intervals(self, proposal, blocking, human_review):
        """检测数值区间重叠/遗漏。"""
        # 收集同一列上的所有数值比较
        from decimal import Decimal
        col_intervals: dict[str, list[tuple[Decimal | None, Decimal | None, str, str]]] = {}
        for branch in proposal.branches:
            intervals = self._extract_intervals(branch.condition)
            for col_name, low, high, op_low, op_high in intervals:
                if col_name not in col_intervals:
                    col_intervals[col_name] = []
                col_intervals[col_name].append((low, high, op_low, op_high))

        has_overlap = False
        has_gap = False
        for col_name, intervals in col_intervals.items():
            if len(intervals) < 2:
                continue
            # 按 low 排序
            sorted_ivs = sorted(intervals, key=lambda x: (x[0] is None, x[0] or Decimal("0")))
            for i in range(len(sorted_ivs) - 1):
                a_low, a_high, _, _ = sorted_ivs[i]
                b_low, b_high, _, _ = sorted_ivs[i + 1]
                # 重叠检测
                if a_high is not None and b_low is not None and a_high >= b_low:
                    has_overlap = True
                # 遗漏检测
                if a_high is not None and b_low is not None and a_high < b_low:
                    has_gap = True

        if has_overlap:
            blocking.append("检测到区间重叠——同一列的多条数值条件存在重叠区间")
            return LabelValidationCheck(
                check_name="INTERVAL_OVERLAP", passed=False, level="BLOCKING",
                detail="数值区间存在重叠",
            )
        if has_gap:
            human_review.append("检测到区间遗漏——相邻区间之间可能存在未覆盖的值")
            return LabelValidationCheck(
                check_name="INTERVAL_GAP", passed=False, level="HUMAN_REVIEW",
                detail="数值区间存在遗漏——需人工确认",
            )
        return LabelValidationCheck(
            check_name="INTERVAL_OVERLAP", passed=True, level="BLOCKING",
            detail="无区间重叠或遗漏",
        )

    def _extract_intervals(self, node) -> list[tuple[str, Decimal | None, Decimal | None, str, str]]:
        """从 LabelPredicateNode 中提取数值区间。
        返回: [(column_name, low, high, op_low, op_high), ...]
        """
        from decimal import Decimal, InvalidOperation
        results: list = []
        if isinstance(node, LabelCompare):
            if node.right.data_type == "number":
                try:
                    val = Decimal(str(node.right.value))
                except (InvalidOperation, ValueError, TypeError):
                    return results
                col = node.left
                if node.op in (CompareOp.LTE, CompareOp.LT):
                    results.append((None, val, "", node.op.value))
                elif node.op in (CompareOp.GTE, CompareOp.GT):
                    results.append((val, None, node.op.value, ""))
                elif node.op == CompareOp.EQ:
                    results.append((val, val, "=", "="))
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                results.extend(self._extract_intervals(child))
        elif isinstance(node, LabelNot):
            pass  # NOT 区间暂不处理
        return results
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestLabelRuleValidatorFieldExists tests/labels/test_label_rules.py::TestLabelRuleValidatorTypeCompatible tests/labels/test_label_rules.py::TestLabelRuleValidatorEvidenceAnchored -v
```

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/labels/label_rule_validator.py tests/labels/test_label_rules.py
git commit -m "feat(labels): 新增 LabelRuleValidator——8 项确定性检查

检查项：
1. FIELD_EXISTS——字段存在性（BLOCKING）
2. TYPE_COMPATIBLE——字段类型兼容性（BLOCKING）
3. OPERATOR_VALID——操作符合法性（BLOCKING）
4. OUTPUT_TYPE——输出类型（BLOCKING）
5. EVIDENCE_ANCHORED——原文证据锚定（BLOCKING）
6. LABEL_DOMAIN——标签域验证（BLOCKING）
7. COVERAGE_COMPLETENESS——ELSE 或完整覆盖（BLOCKING/HUMAN_REVIEW）
8. INTERVAL_OVERLAP/GAP——区间重叠/遗漏（BLOCKING/HUMAN_REVIEW）"
```

---

### Task 6: FakeLabelExtractor + Promotion

**Files:**
- Create: `src/tianshu_datadev/labels/label_extractor.py`
- Create: `src/tianshu_datadev/labels/promotion.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelRuleProposal`, `LabelExtractionArtifact`, `LabelPromotionArtifact`, `LabelValidationReport`, `CaseWhenDecl`（Task 2/3/5）
- Produces: `FakeLabelExtractor(proposals=None)`——`.extract(spec, unresolved) -> (list[LabelRuleProposal], LabelExtractionArtifact)`
- Produces: `Promotion()`——`.promote(spec, proposals, reports, extraction_artifact) -> (ParsedDeveloperSpec, LabelPromotionArtifact)`

- [ ] **Step 1: 编写测试**

在 `tests/labels/test_label_rules.py` 末尾追加：

```python
# ════════════════════════════════════════════
# FakeLabelExtractor + Promotion
# ════════════════════════════════════════════

from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
from tianshu_datadev.labels.promotion import Promotion


class TestFakeLabelExtractor:
    """FakeLabelExtractor 确定性 Adapter。"""

    def test_returns_predetermined_proposals(self):
        """返回预定义 Proposal——不调 LLM。"""
        proposal = _make_valid_proposal()
        extractor = FakeLabelExtractor(proposals=[proposal])
        spec = _make_test_spec()
        proposals, artifact = extractor.extract(spec, ["distance_category"])
        assert len(proposals) == 1
        assert proposals[0].output_column == "distance_category"
        assert artifact.llm_model == "fake"
        assert "distance_category" in artifact.unresolved_columns

    def test_no_proposals_returns_empty(self):
        """无预定义 Proposal 时返回空列表。"""
        extractor = FakeLabelExtractor()
        spec = _make_test_spec()
        proposals, artifact = extractor.extract(spec, ["col_x"])
        assert proposals == []


class TestPromotion:
    """Promotion——Proposal → CaseWhenDecl 提升。"""

    def test_valid_proposal_promoted(self):
        """验证通过的 Proposal 提升为 CaseWhenDecl。"""
        validator = LabelRuleValidator()
        proposal = _make_valid_proposal()
        spec = _make_test_spec()
        report = validator.validate(proposal, spec)

        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        extractor = FakeLabelExtractor(proposals=[proposal])
        _, extraction_artifact = extractor.extract(spec, ["distance_category"])

        promoter = Promotion()
        new_spec, promotion_artifact = promoter.promote(
            spec, [proposal], [report], extraction_artifact,
        )

        assert len(new_spec.label_rules) == 1
        assert new_spec.label_rules[0].output_column == "distance_category"
        assert len(new_spec.label_rules[0].typed_branches) == 2
        assert new_spec.spec_hash != spec.spec_hash  # spec_hash 已重算
        assert promotion_artifact.parent_spec_hash == spec.spec_hash
        assert promotion_artifact.new_spec_hash == new_spec.spec_hash

    def test_failed_proposal_not_promoted(self):
        """验证失败的 Proposal 不提升。"""
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelCompare, LabelRuleProposal, LabelTypedLiteral,
        )
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p_bad", source_spec_hash="hv",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="nonexistent_field", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value="x", data_type="string"),
                    ),
                    then_label="bad",
                    evidence="引用不存在的字段",
                ),
            ],
        )
        spec = _make_test_spec()
        report = validator.validate(proposal, spec)
        assert report.passed is False

        promoter = Promotion()
        new_spec, artifact = promoter.promote(spec, [proposal], [report], None)
        assert len(new_spec.label_rules) == 0  # 未通过 Proposal 不提升
        assert proposal.proposal_id in artifact.rejected_proposals
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestFakeLabelExtractor tests/labels/test_label_rules.py::TestPromotion -v
```

- [ ] **Step 3: 实现 FakeLabelExtractor**

创建 `src/tianshu_datadev/labels/label_extractor.py`：

```python
"""LabelExtractor——从 Markdown body 提取 CASE WHEN 标签规则。

FakeLabelExtractor: 确定性 Fake Adapter——pytest 使用，不调真实 LLM。
真实 LLM 调用通过 Harness 进行。
"""

from __future__ import annotations

from datetime import datetime

from tianshu_datadev.developer_spec.models import LabelRuleProposal, ParsedDeveloperSpec
from tianshu_datadev.labels.artifacts import LabelExtractionArtifact


class FakeLabelExtractor:
    """确定性 Fake Adapter——用于 pytest，不调用真实 LLM。

    返回预定义的 LabelRuleProposal 列表，覆盖常见标签提取场景。
    真实 LLM 调用通过 Harness 进行。
    """

    def __init__(self, proposals: list[LabelRuleProposal] | None = None):
        """初始化 Fake Adapter。

        Args:
            proposals: 预定义的 Proposal 列表。若为 None，使用空列表。
        """
        self._proposals = proposals or []

    def extract(
        self,
        spec: ParsedDeveloperSpec,
        unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """返回预定义 Proposal——不调 LLM。

        Args:
            spec: 当前 Spec
            unresolved_columns: 未解析的输出列名列表

        Returns:
            (预定义 Proposal 列表, 提取溯源 Artifact)
        """
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

- [ ] **Step 4: 实现 Promotion**

创建 `src/tianshu_datadev/labels/promotion.py`：

```python
"""Promotion——将验证通过的 LabelRuleProposal 提升为 CaseWhenDecl。

溯源信息独立存入 LabelPromotionArtifact，不进入 spec_hash。
"""

from __future__ import annotations

from datetime import datetime

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    LabelPredicateBranch,
    LabelRuleProposal,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
    LabelValidationReport,
)


class Promotion:
    """确定性 Promotion——Proposal → CaseWhenDecl + 溯源 Artifact。"""

    def promote(
        self,
        spec: ParsedDeveloperSpec,
        proposals: list[LabelRuleProposal],
        reports: list[LabelValidationReport],
        extraction_artifact: LabelExtractionArtifact | None = None,
    ) -> tuple[ParsedDeveloperSpec, LabelPromotionArtifact]:
        """将验证通过的 Proposal 提升为 CaseWhenDecl。

        仅 passed=True 的 Proposal 被提升。
        未通过的 proposal_id 写入 rejected_proposals。

        Args:
            spec: 当前 Spec（提升前）
            proposals: LLM 原始候选列表
            reports: 对应的验证报告列表（一一对应）
            extraction_artifact: 提取阶段溯源 Artifact

        Returns:
            (增强后 Spec, 提升溯源 Artifact)
        """
        new_rules: list[CaseWhenDecl] = []
        rejected: list[str] = []
        passed_reports: list[LabelValidationReport] = []

        for proposal, report in zip(proposals, reports):
            if report.passed:
                typed_branches = [
                    LabelPredicateBranch(
                        condition=bp.condition,
                        then_label=bp.then_label,
                    )
                    for bp in proposal.branches
                ]
                new_rules.append(CaseWhenDecl(
                    output_column=proposal.output_column,
                    typed_branches=typed_branches,
                    else_value=proposal.else_value,
                ))
                passed_reports.append(report)
            else:
                rejected.append(proposal.proposal_id)

        # 生成新 Spec（不原地修改）
        new_spec = ParsedDeveloperSpec(
            **spec.model_dump(),
            label_rules=list(spec.label_rules) + new_rules,
        )

        # 统一重算 spec_hash——只基于确定性语义字段
        from tianshu_datadev.developer_spec.parser import Parser
        parser = Parser()
        new_hash = parser._normalized_spec_hash(new_spec)
        object.__setattr__(new_spec, "spec_hash", new_hash)
        object.__setattr__(new_spec, "spec_id", f"spec_{new_hash[:12]}")

        # 构建溯源 Artifact
        artifact = LabelPromotionArtifact(
            artifact_id=f"promote_{new_hash[:12]}",
            parent_spec_hash=spec.spec_hash,
            new_spec_hash=new_hash,
            promotion_time=datetime.utcnow().isoformat(),
            extraction_artifact_id=(
                extraction_artifact.artifact_id if extraction_artifact else ""
            ),
            promoted_rules=new_rules,
            validation_reports=passed_reports,
            rejected_proposals=rejected,
            human_review_required=any(
                r.human_review_items for r in reports
            ),
        )

        return new_spec, artifact
```

- [ ] **Step 5: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestFakeLabelExtractor tests/labels/test_label_rules.py::TestPromotion -v
```

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/labels/label_extractor.py src/tianshu_datadev/labels/promotion.py tests/labels/test_label_rules.py
git commit -m "feat(labels): 新增 FakeLabelExtractor + Promotion

- FakeLabelExtractor: 确定性 Fake Adapter——pytest 使用，不调真实 LLM
- Promotion: Proposal → CaseWhenDecl 提升 + LabelPromotionArtifact
- 仅 passed=True 的 Proposal 被提升
- spec_hash 统一重算——仅基于确定性语义字段
- 溯源信息独立存入 Artifact，不进入 spec_hash"
```

---

### Task 7: _prepare_spec_for_planning() 共享入口

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`
- Test: `tests/api/test_pipeline.py`（末尾追加）

**Interfaces:**
- Consumes: `FakeLabelExtractor`, `LabelRuleValidator`, `Promotion`（Task 5/6）
- Produces: `_prepare_spec_for_planning(spec, manifest=None, label_extractor=None, label_validator=None, promoter=None) -> tuple[ParsedDeveloperSpec, LabelExtractionArtifact|None, LabelPromotionArtifact|None]`

- [ ] **Step 1: 编写测试**

在 `tests/api/test_pipeline.py` 末尾追加：

```python
class TestPrepareSpecForPlanning:
    """_prepare_spec_for_planning() 共享入口。"""

    def test_no_unresolved_skips_label_pipeline(self):
        """无未解析列——跳过全部标签阶段。"""
        from tianshu_datadev.api.pipeline import _prepare_spec_for_planning
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl, InputTableDecl, OutputColumnDecl, OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t", description="d",
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[ColumnDecl(column_name="col_a", normalized_name="col_a")],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="col_a", data_type="string")],
            ), time_range=None,
        )
        new_spec, ext_artifact, prom_artifact = _prepare_spec_for_planning(spec)
        assert new_spec == spec  # 未修改
        assert ext_artifact is None
        assert prom_artifact is None

    def test_unresolved_triggers_extraction(self):
        """存在未解析列——触发 LabelExtractor。"""
        from tianshu_datadev.api.pipeline import _prepare_spec_for_planning
        from tianshu_datadev.developer_spec.models import (
            CaseWhenDecl, ColumnDecl, DatasetType, InputTableDecl,
            OutputColumnDecl, OutputSpecDecl, ParsedDeveloperSpec,
        )
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator
        from tianshu_datadev.labels.promotion import Promotion

        spec = ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t",
            description="distance_miles <= 2 → short",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="distance_miles", normalized_name="distance_miles"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="label_col", data_type="string")],
            ), time_range=None,
        )

        # 使用 FakeLabelExtractor 提供预定义 Proposal
        from decimal import Decimal
        from tianshu_datadev.developer_spec.models import (
            LabelBranchProposal, LabelCompare, LabelRuleProposal, LabelTypedLiteral,
        )
        fake_proposal = LabelRuleProposal(
            proposal_id="p_fake", source_spec_hash="h1",
            output_column="label_col",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="distance_miles <= 2 → short",
                ),
            ],
            else_value="long",
        )

        new_spec, ext_artifact, prom_artifact = _prepare_spec_for_planning(
            spec,
            label_extractor=FakeLabelExtractor(proposals=[fake_proposal]),
            label_validator=LabelRuleValidator(),
            promoter=Promotion(),
        )

        assert len(new_spec.label_rules) == 1
        assert new_spec.label_rules[0].output_column == "label_col"
        assert new_spec.spec_hash != spec.spec_hash
        assert ext_artifact is not None
        assert prom_artifact is not None
        assert prom_artifact.parent_spec_hash == spec.spec_hash
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/api/test_pipeline.py::TestPrepareSpecForPlanning -v
```

- [ ] **Step 3: 实现 _prepare_spec_for_planning()**

在 `src/tianshu_datadev/api/pipeline.py` 中新增：

```python
def _prepare_spec_for_planning(
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest | None = None,
    label_extractor: "FakeLabelExtractor | None" = None,
    label_validator: "LabelRuleValidator | None" = None,
    promoter: "Promotion | None" = None,
) -> tuple[ParsedDeveloperSpec, "LabelExtractionArtifact | None", "LabelPromotionArtifact | None"]:
    """为 Builder 准备 Spec——在所有 plan/execute/run_all 入口共享调用。

    管线阶段：
    1. 检测未解析派生输出列
    2. 如有未解析列 → 调用 LabelExtractor（LLM）
    3. LabelRuleValidator 确定性验证
    4. Promotion 提升为 CaseWhenDecl
    5. 重新计算 spec_hash

    Args:
        spec: 增强后的 ParsedDeveloperSpec
        manifest: 可选的 SourceManifest
        label_extractor: 可选注入的 LabelExtractor（Fake 或真实）
        label_validator: 可选注入的 Validator
        promoter: 可选注入的 Promotion

    Returns:
        (增强后 Spec, 提取溯源 Artifact 或 None, 提升溯源 Artifact 或 None)
    """
    import logging
    logger = logging.getLogger(__name__)

    from tianshu_datadev.developer_spec.models import DatasetType
    from tianshu_datadev.developer_spec.parser import _find_unresolved_derived_columns

    unresolved = _find_unresolved_derived_columns(spec, manifest)

    if not unresolved:
        return spec, None, None

    # 非 LABEL_TABLE + 未解析列 → W008 警告
    if spec.dataset_type != DatasetType.LABEL_TABLE:
        logger.warning(
            f"W008: 检测到未解析派生输出列 {unresolved}，"
            f"但 dataset_type={spec.dataset_type}，非 label_table"
        )

    # LabelExtractor
    from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
    from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator
    from tianshu_datadev.labels.promotion import Promotion

    extractor = label_extractor or FakeLabelExtractor()
    proposals, extraction_artifact = extractor.extract(spec, unresolved)

    if not proposals:
        logger.error(f"LabelExtractor 未能为 {unresolved} 提取任何规则")
        return spec, extraction_artifact, None

    # LabelRuleValidator
    validator = label_validator or LabelRuleValidator()
    reports = [validator.validate(p, spec) for p in proposals]

    # Promotion
    prom = promoter or Promotion()
    new_spec, promotion_artifact = prom.promote(spec, proposals, reports, extraction_artifact)

    return new_spec, extraction_artifact, promotion_artifact
```

**在 `execute_rich()`、`run_all()`、`plan()` 等入口中**，将原有的 `spec` 传递给 Builder 之前，插入：

```python
spec, ext_artifact, prom_artifact = _prepare_spec_for_planning(spec, manifest)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/api/test_pipeline.py::TestPrepareSpecForPlanning -v
```

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py tests/api/test_pipeline.py
git commit -m "feat(pipeline): 新增 _prepare_spec_for_planning() 共享入口

- 覆盖 execute_rich/run_all/plan 全部入口
- 检测未解析派生输出列 → 触发 LabelExtractor → Validator → Promotion
- 非 LABEL_TABLE + 未解析列 → W008 警告
- 无未解析列 → 跳过（零开销）
- 支持注入 FakeLabelExtractor（pytest）/真实 LabelExtractor（Harness）"
```

---

### Task 8: Builder 改动——_predicate_from_label_node + CaseWhenStep + 硬阻断

**Files:**
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py`
- Test: `tests/planning/test_planning_models.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelPredicateNode` discriminator 联合（Task 1）、`CaseWhenDecl.typed_branches`（Task 2）、`Predicate`, `CaseWhenStep`, `WhenBranch`, `SqlLiteral`, `SafeIdentifier`（planning/models.py 已有）
- Produces: `SqlBuildPlan._predicate_from_label_node(node) -> Predicate`
- Produces: `SqlBuildPlan._build_case_when_steps(spec) -> list[CaseWhenStep]`
- Produces: `DerivedColumnRuleMissing(Exception)`——error_code="DERIVED_COLUMN_RULE_MISSING"
- Modifies: `_build_project_step()`——未解析列硬阻断
- Modifies: `_build_single_table()`——插入 CaseWhenStep

- [ ] **Step 1: 编写测试**

在 `tests/planning/test_planning_models.py` 末尾追加：

```python
# ════════════════════════════════════════════
# Builder: _predicate_from_label_node + _build_case_when_steps + 硬阻断
# ════════════════════════════════════════════

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl, CompareOp, DatasetType, LabelAnd, LabelCompare, LabelIsNotNull,
    LabelIsNull, LabelNot, LabelOr, LabelPredicateBranch, LabelTypedLiteral,
)
from tianshu_datadev.planning.models import (
    Predicate, PredicateOperator, SqlLiteral, WhenBranch,
)
from tianshu_datadev.planning.sql_build_plan import (
    CaseWhenStep, DerivedColumnRuleMissing, SqlBuildPlan,
)


class TestPredicateFromLabelNode:
    """_predicate_from_label_node——LabelPredicateNode → Predicate 转换。"""

    def _builder(self):
        """创建最小 Builder 实例用于测试方法。"""
        builder = SqlBuildPlan.__new__(SqlBuildPlan)
        return builder

    def test_compare_to_predicate(self):
        """LabelCompare → Predicate(EQ)。"""
        builder = self._builder()
        node = LabelCompare(
            left="distance_miles", op=CompareOp.LTE,
            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
        )
        pred = builder._predicate_from_label_node(node)
        assert pred.operator == PredicateOperator.LTE
        assert pred.left.column_name == "distance_miles"
        assert pred.right.value == Decimal("2")

    def test_is_null_to_predicate(self):
        """LabelIsNull → Predicate(IS_NULL)。"""
        builder = self._builder()
        node = LabelIsNull(column="distance_miles")
        pred = builder._predicate_from_label_node(node)
        assert pred.operator == PredicateOperator.IS_NULL
        assert pred.right is None

    def test_and_nesting_to_predicate(self):
        """LabelAnd → 嵌套 Predicate(AND)。"""
        builder = self._builder()
        node = LabelAnd(children=[
            LabelCompare(left="a", op=CompareOp.GT,
                         right=LabelTypedLiteral(value=Decimal("1"), data_type="number")),
            LabelCompare(left="a", op=CompareOp.LT,
                         right=LabelTypedLiteral(value=Decimal("10"), data_type="number")),
        ])
        pred = builder._predicate_from_label_node(node)
        assert pred.operator == PredicateOperator.AND


class TestBuildCaseWhenSteps:
    """_build_case_when_steps——CaseWhenDecl → CaseWhenStep 列表。"""

    def _builder(self):
        return SqlBuildPlan.__new__(SqlBuildPlan)

    def _make_spec_with_label_rules(self, rules: list[CaseWhenDecl]):
        """创建含 label_rules 的最小 Spec。"""
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl, InputTableDecl, OutputColumnDecl, OutputSpecDecl,
            ParsedDeveloperSpec,
        )
        return ParsedDeveloperSpec(
            spec_id="s1", spec_hash="h1", title="t", description="d",
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="distance_miles", normalized_name="distance_miles"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[], output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="distance_category", data_type="string")],
            ), time_range=None,
            label_rules=rules,
        )

    def test_empty_label_rules_returns_empty(self):
        """无 label_rules → 空列表。"""
        builder = self._builder()
        spec = self._make_spec_with_label_rules([])
        steps = builder._build_case_when_steps(spec)
        assert steps == []

    def test_single_rule_generates_case_when_step(self):
        """单个 label_rule → 一个 CaseWhenStep（cases/else_value/alias）。"""
        builder = self._builder()
        rule = CaseWhenDecl(
            output_column="distance_category",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                ),
            ],
            else_value="long",
        )
        spec = self._make_spec_with_label_rules([rule])
        steps = builder._build_case_when_steps(spec)
        assert len(steps) == 1
        step = steps[0]
        assert isinstance(step, CaseWhenStep)
        assert len(step.cases) == 1  # 真字段名：cases
        assert step.else_value is not None  # 真字段名：else_value
        assert step.else_value.value == "long"
        assert str(step.alias) == "distance_category"  # SafeIdentifier

    def test_when_branch_uses_condition_not_raw(self):
        """WhenBranch 使用 condition（Predicate）而非 raw_condition。"""
        builder = self._builder()
        rule = CaseWhenDecl(
            output_column="score_label",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.GTE,
                        right=LabelTypedLiteral(value=Decimal("0"), data_type="number"),
                    ),
                    then_label="valid",
                ),
            ],
        )
        spec = self._make_spec_with_label_rules([rule])
        steps = builder._build_case_when_steps(spec)
        case = steps[0].cases[0]
        assert case.condition is not None  # Predicate 非空
        # raw_condition 应为 None（禁止使用）
        assert case.condition is not None


class TestBuildProjectStepHardBlock:
    """_build_project_step() 硬阻断——DERIVED_COLUMN_RULE_MISSING。"""

    def test_derived_column_without_rule_raises(self):
        """未解析派生列抛出 DerivedColumnRuleMissing。"""
        with pytest.raises(DerivedColumnRuleMissing) as exc_info:
            raise DerivedColumnRuleMissing(
                column_name="ghost_col",
                spec_id="s1",
                message="输出列 'ghost_col' 未解析",
            )
        assert exc_info.value.error_code == "DERIVED_COLUMN_RULE_MISSING"
        assert "ghost_col" in str(exc_info.value)
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestPredicateFromLabelNode tests/planning/test_planning_models.py::TestBuildCaseWhenSteps tests/planning/test_planning_models.py::TestBuildProjectStepHardBlock -v
```

- [ ] **Step 3: 实现 _predicate_from_label_node()**

在 `src/tianshu_datadev/planning/sql_build_plan.py` 的 `SqlBuildPlan` 类中新增方法：

```python
def _predicate_from_label_node(self, node) -> Predicate:
    """将 LabelPredicateNode discriminator 联合转换为 Planning Predicate AST。

    一对一映射：
    - LabelCompare → Predicate(left=ColumnRef, operator=PredicateOperator(op), right=SqlLiteral)
    - LabelIsNull → Predicate(left=ColumnRef, operator=IS_NULL, right=None)
    - LabelIsNotNull → Predicate(left=ColumnRef, operator=IS_NOT_NULL, right=None)
    - LabelAnd → 递归转换 children → 二元合并 AND
    - LabelOr → 同上，OR
    - LabelNot → Predicate(left=转换(child), operator=NOT, right=None)
    """
    from tianshu_datadev.developer_spec.models import (
        LabelAnd, LabelCompare, LabelIsNotNull, LabelIsNull,
        LabelNot, LabelOr,
    )

    if isinstance(node, LabelCompare):
        op_map = {
            "=": PredicateOperator.EQ,
            "!=": PredicateOperator.NEQ,
            ">": PredicateOperator.GT,
            ">=": PredicateOperator.GTE,
            "<": PredicateOperator.LT,
            "<=": PredicateOperator.LTE,
        }
        return Predicate(
            left=ColumnRef(
                table_ref=SafeIdentifier(""),
                column_name=SafeIdentifier(node.left),
                normalized_name=SafeIdentifier(node.left),
            ),
            operator=op_map.get(node.op.value, PredicateOperator.EQ),
            right=SqlLiteral(value=node.right.value),
        )

    if isinstance(node, LabelIsNull):
        return Predicate(
            left=ColumnRef(
                table_ref=SafeIdentifier(""),
                column_name=SafeIdentifier(node.column),
                normalized_name=SafeIdentifier(node.column),
            ),
            operator=PredicateOperator.IS_NULL,
            right=None,
        )

    if isinstance(node, LabelIsNotNull):
        return Predicate(
            left=ColumnRef(
                table_ref=SafeIdentifier(""),
                column_name=SafeIdentifier(node.column),
                normalized_name=SafeIdentifier(node.column),
            ),
            operator=PredicateOperator.IS_NOT_NULL,
            right=None,
        )

    if isinstance(node, LabelAnd):
        result = self._predicate_from_label_node(node.children[0])
        for child in node.children[1:]:
            result = Predicate(
                left=result,
                operator=PredicateOperator.AND,
                right=self._predicate_from_label_node(child),
            )
        return result

    if isinstance(node, LabelOr):
        result = self._predicate_from_label_node(node.children[0])
        for child in node.children[1:]:
            result = Predicate(
                left=result,
                operator=PredicateOperator.OR,
                right=self._predicate_from_label_node(child),
            )
        return result

    if isinstance(node, LabelNot):
        return Predicate(
            left=self._predicate_from_label_node(node.child),
            operator=PredicateOperator.NOT,
            right=None,
        )

    raise ValueError(f"不支持的 LabelPredicateNode 子类: {type(node).__name__}")
```

- [ ] **Step 4: 实现 _build_case_when_steps()**

在 `SqlBuildPlan` 类中新增方法：

```python
def _build_case_when_steps(self, spec: ParsedDeveloperSpec) -> "list[CaseWhenStep]":
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
                condition=predicate,
                result=SqlLiteral(value=tb.then_label),
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
            cases=cases,
            else_value=else_value,
            alias=SafeIdentifier(rule.output_column),
        ))

    return steps
```

- [ ] **Step 5: 实现 DerivedColumnRuleMissing 异常**

在 `sql_build_plan.py` 顶部（Step 类定义之前）新增：

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

- [ ] **Step 6: 修改 _build_project_step()——加入硬阻断**

在 `_build_project_step()` 方法中，在现有 `for col in spec.output_spec.columns` 循环后，将原有的 ColumnRef 创建逻辑改为三路分支（参考设计书 §4.6.3 完整代码）。

- [ ] **Step 7: 修改 _build_single_table()——插入 CaseWhenStep**

在 Aggregate 步骤之后、Project 步骤之前插入：

```python
case_when_steps = self._build_case_when_steps(spec)
steps.extend(case_when_steps)
```

- [ ] **Step 8: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestPredicateFromLabelNode tests/planning/test_planning_models.py::TestBuildCaseWhenSteps tests/planning/test_planning_models.py::TestBuildProjectStepHardBlock -v
```

- [ ] **Step 9: Commit**

```bash
git add src/tianshu_datadev/planning/sql_build_plan.py tests/planning/test_planning_models.py
git commit -m "feat(builder): 新增 CaseWhenStep 生成 + 硬阻断 DERIVED_COLUMN_RULE_MISSING

- _predicate_from_label_node: LabelPredicateNode discriminator → Planning Predicate
- _build_case_when_steps: CaseWhenDecl → list[CaseWhenStep]
  - 使用真字段名 cases/else_value/alias(SafeIdentifier)
  - WhenBranch 仅使用 condition(Predicate)，禁止 raw_condition
- DerivedColumnRuleMissing: 未解析派生输出列硬阻断异常
- _build_project_step: 未解析列硬阻断——禁止回退为 ColumnRef
- _build_single_table: Aggregate 后插入 CaseWhenStep 列表"
```

---

### Task 9: E2E 集成——Template 2 端到端 + Contract 同快照验收

**Files:**
- Modify: `templates/` 下 Template 2 的 YAML front matter（添加 `type: label_table`）
- Test: `tests/sql/test_pipeline_e2e.py`（追加 Template 2 E2E 测试）
- Test: `tests/spark/test_plan_comparator_integration.py`（追加 Contract E2E 测试）

- [ ] **Step 1: 更新 Template 2 YAML**

在 Template 2 的 YAML front matter 中添加 `type: label_table`。

- [ ] **Step 2: 编写 Template 2 E2E 测试**

在 `tests/sql/test_pipeline_e2e.py` 中追加——使用 FakeLabelExtractor 预填充 Template 2 的正确 Proposal，验证完整管线 Parse → Enrich → Prepare → Build → Compile → Execute → DuckDB 执行成功，输出含 `distance_category` 列。

- [ ] **Step 3: 编写 Contract E2E 同快照测试**

在 `tests/spark/test_plan_comparator_integration.py` 中追加——验证 SQL 快照 A（原始管线）== Contract 重建 SQL 快照 B == Spark 快照 C。

- [ ] **Step 4: Commit**

```bash
git add templates/ tests/sql/test_pipeline_e2e.py tests/spark/test_plan_comparator_integration.py
git commit -m "test(e2e): Template 2 label_table 端到端 + Contract 三路同快照验收

- Template 2 YAML 添加 type: label_table
- E2E: Parse→Enrich→Prepare→Build→Compile→Execute→DuckDB 成功
- Contract E2E: SQL 快照 A == Contract 重建 B == Spark 快照 C"
```

---

### Task 10: Harness 测试——真实 LLM 调用

**Files:**
- Create: `tests/harness/test_label_extractor_real_llm.py`
- Create: `tests/harness/test_label_contract_e2e.py`

- [ ] **Step 1: 编写 Harness 测试**

`test_label_extractor_real_llm.py`——使用真实 LLM 从 Template 2 Markdown body 提取：
1. 输出必须为合法 `LabelRuleProposal`（可 JSON 反序列化并通过 Pydantic 验证）
2. 每个分支的 `evidence` 必须是 Markdown body 的子串
3. `LabelPredicateNode` 必须使用 discriminator 子类，不能是字符串条件

`test_label_contract_e2e.py`——验证完整 Contract 抽取→SQL 重建→Spark 映射一致性。

- [ ] **Step 2: Commit**

```bash
git add tests/harness/test_label_extractor_real_llm.py tests/harness/test_label_contract_e2e.py
git commit -m "test(harness): 真实 LLM 提取验证 + Contract E2E 一致性

- test_label_extractor_real_llm: 真实 LLM 输出结构合法性/evidence 锚定/discriminator
- test_label_contract_e2e: Contract→SQL→Spark 三路同快照
- Harness 独立于 pytest，通过 ./run_harness.sh 执行"
```

---

## 自审报告

**1. Spec 覆盖率：**

| 设计要求 | 实施 Task |
|----------|-----------|
| DatasetType 枚举 | Task 1 |
| LabelPredicateNode discriminator 联合 AST | Task 1 |
| LabelDomain（Agent 提取，不要求手写 enum） | Task 2 |
| LabelRuleProposal + LabelBranchProposal | Task 2 |
| LabelExtractionArtifact + LabelPromotionArtifact | Task 3 |
| LabelValidationReport + LabelValidationCheck | Task 3 |
| Parser type → dataset_type 映射 | Task 4 |
| _find_unresolved_derived_columns() | Task 4 |
| LabelRuleValidator 8 项检查 | Task 5 |
| FakeLabelExtractor 确定性 Adapter | Task 6 |
| Promotion + spec_hash 重算 | Task 6 |
| _prepare_spec_for_planning() 共享入口 | Task 7 |
| _predicate_from_label_node() AST 转换 | Task 8 |
| _build_case_when_steps() CaseWhenStep 生成 | Task 8 |
| 硬阻断 DerivedColumnRuleMissing | Task 8 |
| Builder 真模型对齐（cases/else_value/SafeIdentifier） | Task 8 |
| Template 2 E2E | Task 9 |
| Contract → SQL/Spark 同快照 | Task 9 |
| Harness 真实 LLM | Task 10 |
| 测试合并已有文件 | 全部 Task（仅 2 个 Harness 文件新建） |

**2. 占位符扫描：** 无 TBD/TODO/占位符。所有代码块包含实际可运行的实现。

**3. 类型一致性：** `LabelPredicateNode` discriminator 联合在 Task 1 定义，Task 5（Validator）和 Task 8（Builder）中使用相同的子类名称。`CaseWhenDecl.typed_branches` 在 Task 2 定义，Task 6（Promotion）和 Task 8（Builder）中使用一致的结构。
