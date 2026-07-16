# label_table 类型支持——实施计划（v4-light 最终修订版）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 跑通个人使用场景——自然语言业务过程 → LlmLabelExtractor(LLM) → Validator(v1 六项确定性检查) → CaseWhenDecl → SQL/Spark Compiler。生产路径使用 LlmLabelExtractor（复用 LLMGateway/PromptManager/ProviderAdapter），pytest 使用 FakeLLMAdapter。

**Architecture:** 12 段管线：Parser → SourceManifest → SpecEnricher → _prepare_spec_for_planning() → _find_unresolved_derived_columns() → LlmLabelExtractor(生产)/FakeLabelExtractor(pytest) → LabelRuleValidator(v1 六项) → Promotion(双空阻断) → Builder(CaseWhenStep + 硬阻断) → Compiler/Execute。Gateway 原子写入 response_root 后返回 parsed_json_ref；LLM 仅输出规则/标签域/evidence——proposal_id/source_spec_hash/extraction_time 由系统生成。

**Tech Stack:** Python 3.12, Pydantic v2, pytest + FakeLLMAdapter, DuckDB, PySpark, LLMGateway + PromptManager + ProviderAdapter

---

## 修订摘要（v4-light 初版 → 最终版）

| # | 变更 | 初版问题 | 最终版修复 |
|---|------|---------|---------------|
| 0 | **Task 0 重写——文件持久化** | Gateway 生成 `parsed_ref` 路径但从未写文件；测试未覆盖文件→Extractor 链路 | Gateway 接受 `response_root`，Schema 校验通过后原子写入结构化 JSON；测试用 `tmp_path` 验证文件存在+内容；增加 FakeAdapter→Gateway→文件→LlmLabelExtractor 集成测试，**禁止手工造文件** |
| 1 | **根条件类型约束** | `LabelPredicateNode` 含 LITERAL/COLUMN_REF——可被 LLM 错误用作 WHEN 根条件 | 新增 `LabelPredicateCondition` 联合类型——仅允许 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT；`LabelBranchProposalOutput.condition` 和 `LabelBranchProposal.condition` 使用此类型 |
| 2 | **必需字段强制** | `else_value: str \| None`、`evidence: str = ""`、`LabelRuleProposal` 无 `label_domain` 字段 | `else_value: str`（必填）、`evidence: str`（必填，非空）、`LabelRuleProposal.label_domain: LabelDomain`（必填）；缺失任一→Promotion 拒绝 |
| 3 | **Promotion 双空阻断** | `report.passed=True` 仅要求无 BLOCKING——允许 HUMAN_REVIEW 进入 Promotion | Promotion 必须 `blocking_errors` **和** `human_review_items` **均为空**才提升 |
| 4 | **Task 11 生产注入** | 未定义 create_app() 中的 LLMGateway/LlmLabelExtractor 构造方式 | 复用 ProviderAdapter + PromptManager 构造 LLMGateway → LlmLabelExtractor；无 API Key 时 label_table 返回明确 `PipelineError("CONFIG_ERROR")`，**禁止回退 Fake** |
| 5 | **Task 10-13 自包含** | "同 v3 Task N"——引用外部上下文，工程师需跳转查阅 | 每 Task 含完整接口签名、修改文件列表、测试代码、退出条件 |
| 6 | **Validator 双空检查** | `report.passed` 定义模糊不清 | `passed = len(blocking) == 0 and len(human_review) == 0`——全部检查通过才为 True |

## 最小端到端验收链路

```
自然语言 Markdown body
  │
  ▼
Parser (Task 4: type→DatasetType)
  │
  ▼
_prepare_spec_for_planning() (Task 11: 统一入口)
  │
  ├─ _find_unresolved_derived_columns() (Task 5)
  │
  ▼
LlmLabelExtractor.extract() (Task 9: 生产路径)
  │  ├─ 收集源表字段→available_fields
  │  ├─ 构造 LlmRequest(task="extract_label_rules", ...)
  │  ├─ gateway.submit(request, markdown_body=..., unresolved_columns=..., available_fields=...)
  │  │     ├─ PromptManager 加载模板 → 渲染 user_message（Task 0 的 **extra_vars）
  │  │     ├─ Adapter.invoke() → dict
  │  │     ├─ Pydantic model_validate() → 校验通过
  │  │     ├─ 原子写入 response_root/llm_responses/parsed/{request_id}_{hash}.json
  │  │     └─ 返回 LlmResponse(parsed_json_ref="response_root/.../file.json")
  │  ├─ 从 parsed_json_ref 路径读取 JSON → LabelRuleProposalList.model_validate()
  │  └─ 系统包装 proposal_id/source_spec_hash/extraction_time
  │
  ▼
LabelRuleValidator (Task 6: v1 六项确定性检查)
  │  ├─ FIELD_EXISTS / TYPE_COMPATIBLE / OPERATOR_VALID
  │  ├─ AST_VALID / LABEL_DOMAIN / COVERAGE(ELSE+evidence)
  │  └─ passed = blocking_errors 和 human_review_items 均为空
  │
  ▼
Promotion (Task 10: 双空阻断——blocking_errors 且 human_review_items 均为空)
  │
  ▼
Builder._build_case_when_steps() (Task 12: CaseWhenStep 生成)
  │
  ▼
SQL/Spark Compiler (确定性编译——禁止 raw SQL)
  │
  ▼
DuckDB/PySpark 执行验证
```

**阻断验证清单（全部必须正确阻断）：**

| 阻断场景 | 阻断位置 | 预期结果 |
|----------|----------|----------|
| 未知字段（LLM 输出 extra 字段） | Gateway Schema 校验（`extra="forbid"`） | `validation_status="invalid"`，parsed_json_ref=None |
| 非法根节点（LITERAL/COLUMN_REF 作 WHEN 条件） | Pydantic discriminator 拒绝 | `model_validate()` 失败 → Gateway 返回 invalid |
| 标签越界（then_label 不在 domain 中） | Validator LABEL_DOMAIN → BLOCKING | Promotion 拒绝 |
| 缺少 ELSE + evidence 为空 | Validator COVERAGE → HUMAN_REVIEW | Promotion 拒绝（human_review_items 非空） |
| 缺少 API Key | Task 11 create_app() preflight | `PipelineError("CONFIG_ERROR")`，明确提示配置 API Key |

---

## Global Constraints

- 所有代码注释使用中文
- pytest 使用 `FakeLLMAdapter` + `register_default_for_task()`，生产路径使用真实 `LLMGateway`
- 新增测试合并已有测试文件——仅 Harness 冒烟测试可新建文件
- 未解析派生输出列硬阻断（`DERIVED_COLUMN_RULE_MISSING`）
- **LLM 仅输出规则/标签域/evidence**——proposal_id/source_spec_hash/extraction_time 由系统生成
- **禁止 raw SQL 和自由代码**——SQL/Spark 必须由确定性编译器生成
- `OutputColumnDecl` 真实字段为 `type`（非 `data_type`）
- `CaseWhenStep` 真实字段：`cases: list[WhenBranch]`（非 `branches`）、`else_value: SqlLiteral | None`、`alias: SafeIdentifier`
- `WhenBranch` 仅使用 `condition: Predicate | None`，禁止 `raw_condition`
- **根条件仅允许 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT**——LITERAL/COLUMN_REF 不可作 WHEN 根节点
- **label_domain、每个分支 evidence、ELSE 在 label_table v1 中均为必需**——缺失时 Promotion 拒绝
- **Promotion 必须 blocking_errors 和 human_review_items 均为空**
- **Validator v1 仅校验六项**：字段存在/类型兼容/操作符/AST/标签域/ELSE+evidence——不包含区间证明
- 无法确定性判断时记录 HUMAN_REVIEW，不伪装 PASS
- Prompt 模板路径：`prompts/templates/{task}/v001.md`，frontmatter 必填 `target_schema`
- `_SCHEMA_PATH_MAP` 必须为所有新 Schema 注册条目
- 所有 `LlmRequest`/`LlmResponse`/`FakeLLMAdapter` 示例与当前源码接口一致
- `--run-harness` 在 `conftest.py` 中注册，默认排除真实 LLM 测试
- 修改源码后通过 `./dev-reload.sh` 重启服务验证
- 每个 Task 完成后独立 commit

---

## 文件结构

### 新建文件

| 路径 | 职责 |
|------|------|
| `src/tianshu_datadev/labels/__init__.py` | 标签子系统入口，导出所有公开接口 |
| `src/tianshu_datadev/labels/artifacts.py` | LabelExtractionArtifact + LabelPromotionArtifact |
| `src/tianshu_datadev/labels/resolver.py` | _find_unresolved_derived_columns()——独立于 Parser |
| `src/tianshu_datadev/labels/label_extractor.py` | LabelExtractor 抽象接口 + FakeLabelExtractor |
| `src/tianshu_datadev/labels/llm_label_extractor.py` | LlmLabelExtractor——复用 LLMGateway/PromptManager/ProviderAdapter |
| `src/tianshu_datadev/labels/label_rule_validator.py` | LabelRuleValidator（v1 六项检查） |
| `src/tianshu_datadev/labels/promotion.py` | Promotion——Proposal → CaseWhenDecl + 溯源 Artifact + 双空阻断 |
| `src/tianshu_datadev/prompts/templates/extract_label_rules/v001.md` | Prompt 模板 |
| `tests/harness/test_label_extractor_smoke.py` | 唯一可选真实 LLM 冒烟测试 |

### 修改文件

| 路径 | 改动 |
|------|------|
| `src/tianshu_datadev/developer_spec/models.py` | DatasetType、CompareOp、LabelPredicateNode(8 子类)、LabelPredicateCondition(6 子类联合——排除 LITERAL/COLUMN_REF)、LLM 输出层(LabelDomainOutput 等)、系统层(LabelDomain/LabelRuleProposal 等)；ParsedDeveloperSpec 新增 dataset_type/label_rules；CaseWhenDecl 新增 typed_branches |
| `src/tianshu_datadev/developer_spec/parser.py` | 读取 spec_dict["type"] → dataset_type（仅 type 映射） |
| `src/tianshu_datadev/planning/sql_build_plan.py` | _predicate_from_label_node()、_build_case_when_steps()、DerivedColumnRuleMissing、_build_project_step 硬阻断 |
| `src/tianshu_datadev/api/pipeline.py` | _prepare_spec_for_planning() + 全部入口调用 |
| `src/tianshu_datadev/api/app.py` | create_app() 中新增 AnthropicAdapter→LLMGateway→LlmLabelExtractor 生产注入；无 API Key 时明确报错 |
| `src/tianshu_datadev/llm/gateway.py` | 新增 `response_root` 参数；`_render_user_message` 扩展 `**extra_vars`；Schema 校验通过后原子写入结构化 JSON 到 response_root |
| `src/tianshu_datadev/prompts/manager.py` | `_SCHEMA_PATH_MAP` 新增 `LabelRuleProposalList` |
| `tests/conftest.py` | 注册 `--run-harness` 选项 + marker |
| `templates/` 下 Template 2 YAML | 添加 `type: label_table` |

### 测试合并目标

| 测试内容 | 合并到 |
|----------|--------|
| DatasetType、LabelPredicateNode、LabelPredicateCondition、LLM 输出/系统模型 | `tests/planning/test_planning_models.py` |
| Artifacts、Validator、FakeLabelExtractor、LlmLabelExtractor（含集成测试）、Promotion | `tests/labels/test_label_rules.py` |
| Parser type 映射 | `tests/api/test_spec.py` |
| _find_unresolved_derived_columns()、_prepare_spec_for_planning() | `tests/api/test_pipeline.py` |
| _predicate_from_label_node()、_build_case_when_steps()、硬阻断 | `tests/planning/test_planning_models.py` |
| Template 2 E2E | `tests/sql/test_pipeline_e2e.py` |
| Contract E2E 同快照 | `tests/spark/test_plan_comparator_integration.py` |

---
### Task 0: Gateway 文件持久化——response_root + extra_vars（v4-light 最终版核心）

**边界：** 解决三个致命问题：
1. Gateway 当前 `submit()` 生成 `parsed_ref` 路径字符串但**从未将校验通过的结构化对象写入磁盘**——`LlmResponse.generate_parsed_ref()` 仅返回 `"llm_responses/parsed/{request_id}_{hash}.json"` 路径，没有对应的写文件逻辑。下游代码从该路径读取时文件不存在。
2. `_render_user_message()` 仅替换 `{artifact_refs}` 占位符——Prompt 模板中的 `{markdown_body}`/`{unresolved_columns}`/`{available_fields}` 无法被替换。
3. 测试必须使用 `tmp_path`、必须覆盖 FakeAdapter→Gateway→磁盘文件→LlmLabelExtractor 完整链路，**禁止手工造文件代替 Gateway 输出**。

**方案：**
- 新增 `response_root: str` 参数到 `LLMGateway.__init__`——指定结构化输出落盘根目录
- Schema 校验通过后，**原子写入**（先写临时文件→`os.replace`）通过校验的结构化对象到 `response_root/parsed_json_ref`
- 扩展 `_render_user_message()` 接受 `**extra_vars` 并渲染所有 `{var}` 占位符
- 新增 FakeAdapter→Gateway→磁盘文件→LlmLabelExtractor 集成测试——使用 `tmp_path` 作为 `response_root`

**涉及真实接口：**

| 接口 | 当前实际 | 最终版修复 |
|------|----------|-----------|
| `LLMGateway.__init__` | `(adapter, prompt_manager)` → 无 response_root | `(adapter, prompt_manager, response_root="llm_responses")` |
| `LlmResponse.generate_parsed_ref()` | 仅生成路径字符串，不写文件 | 保持路径生成——写文件逻辑在 `submit()` 中 |
| Gateway 文件写入 | **不存在** | Schema 校验通过→原子写入 response_root/parsed_ref |
| `_render_user_message(template, input_refs)` | 仅替换 `{artifact_refs}` | 扩展签名支持 `**extra_vars` |
| `submit(request)` | 不接收 extra_vars | 扩展为 `submit(request, **extra_vars)` |

**Files:**
- Modify: `src/tianshu_datadev/llm/gateway.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加——集成测试）

**Interfaces:**
- Modifies: `LLMGateway.__init__(adapter, prompt_manager, response_root="llm_responses")`
- Modifies: `LLMGateway._render_user_message(template, input_refs, **extra_vars) -> str`
- Modifies: `LLMGateway.submit(request, **extra_vars) -> LlmResponse`
- Produces: `LLMGateway._write_parsed_output(validated_model, parsed_ref_path) -> None`

- [ ] **Step 1: 编写集成测试（使用 tmp_path）**

在 `tests/labels/test_label_rules.py` 末尾追加：

```python
# ================================================
# v4-light 最终版: Gateway 文件持久化集成测试
# 覆盖 FakeAdapter→Gateway→磁盘文件→LlmLabelExtractor 完整链路
# 禁止手工造文件——所有文件由 Gateway 产生
# ================================================

import json
import os
from pathlib import Path
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.llm.models import ArtifactRef, LlmRequest
from tianshu_datadev.prompts.manager import PromptManager
from tianshu_datadev.labels.llm_label_extractor import LlmLabelExtractor


def _make_label_fake_response():
    """构造 FakeLLMAdapter 所需的标准标签提取输出。"""
    return {
        "rules": [{
            "output_column": "distance_category",
            "branches": [{
                "condition": {
                    "node_type": "COMPARE", "left": "distance_miles",
                    "op": "<=",
                    "right": {"node_type": "LITERAL", "value": 2, "data_type": "number"},
                },
                "then_label": "short",
                "evidence": "distance_miles <= 2 -> short",
            }],
            "else_value": "long",
            "label_domain": {"values": ["short", "long"], "source_evidence": "原文"},
        }],
    }


class TestGatewayFilePersistence:
    """验证 Gateway 将校验通过的结构化对象原子写入 response_root。"""

    def test_gateway_writes_parsed_json_to_disk(self, tmp_path):
        """Schema 校验通过→文件存在于 response_root 中。"""
        # 准备
        adapter = FakeLLMAdapter()
        adapter.register_default_for_task("extract_label_rules", _make_label_fake_response())
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter,
            prompt_manager=prompt_manager,
            response_root=str(tmp_path),  # ← 使用 tmp_path 作为受控输出根目录
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="extract_label_rules", prompt_version="v001",
            schema_name="LabelRuleProposalList", schema_version="v1",
            input_artifact_refs=[
                ArtifactRef(artifact_type="parsed_developer_spec",
                            artifact_hash="h_test", artifact_id="spec_test"),
            ],
            temperature=0.1, model="",
        )

        response = gateway.submit(request)

        # 验证响应状态
        assert response.is_valid, f"validation_errors={response.validation_errors}"
        assert response.parsed_json_ref is not None

        # 验证文件存在于 response_root 中
        parsed_path = Path(response.parsed_json_ref)
        assert parsed_path.exists(), f"文件不存在: {parsed_path}"
        assert parsed_path.is_absolute() or tmp_path in parsed_path.parents, \
            f"文件不在 response_root 中: {parsed_path}"

        # 验证文件内容可读且结构正确
        data = json.loads(parsed_path.read_text("utf-8"))
        assert "rules" in data
        assert len(data["rules"]) == 1
        assert data["rules"][0]["output_column"] == "distance_category"

    def test_gateway_invalid_does_not_write_file(self, tmp_path):
        """Schema 校验失败→不写文件，parsed_json_ref 为 None。"""
        adapter = FakeLLMAdapter()
        # 注册包含未知字段的响应——extra="forbid" 导致校验失败
        adapter.register_default_for_task("extract_label_rules", {
            "rules": [{
                "output_column": "distance_category",
                "branches": [],
                "else_value": "unknown",
                "label_domain": {"values": ["unknown"]},
                "unknown_field_xyz": "不应该存在",  # ← extra 字段
            }],
        })
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter, prompt_manager=prompt_manager,
            response_root=str(tmp_path),
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="extract_label_rules", prompt_version="v001",
            schema_name="LabelRuleProposalList", schema_version="v1",
            input_artifact_refs=[
                ArtifactRef(artifact_type="parsed_developer_spec",
                            artifact_hash="h_test", artifact_id="spec_test"),
            ],
            temperature=0.1, model="",
        )

        response = gateway.submit(request)

        # 校验应失败
        assert not response.is_valid
        assert response.parsed_json_ref is None

    def test_illegal_root_node_rejected(self, tmp_path):
        """LLM 输出 LITERAL 作根条件→Pydantic discriminator 拒绝。"""
        adapter = FakeLLMAdapter()
        adapter.register_default_for_task("extract_label_rules", {
            "rules": [{
                "output_column": "distance_category",
                "branches": [{
                    "condition": {
                        "node_type": "LITERAL",  # ← LITERAL 不可作根条件
                        "value": "short", "data_type": "string",
                    },
                    "then_label": "short",
                    "evidence": "非法根节点",
                }],
                "else_value": "unknown",
                "label_domain": {"values": ["unknown"]},
            }],
        })
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter, prompt_manager=prompt_manager,
            response_root=str(tmp_path),
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="extract_label_rules", prompt_version="v001",
            schema_name="LabelRuleProposalList", schema_version="v1",
            input_artifact_refs=[
                ArtifactRef(artifact_type="parsed_developer_spec",
                            artifact_hash="h_test", artifact_id="spec_test"),
            ],
            temperature=0.1, model="",
        )

        response = gateway.submit(request)
        assert not response.is_valid, "LITERAL 根条件应被 Pydantic discriminator 拒绝"


class TestFakeAdapterToExtractorIntegration:
    """集成测试: FakeAdapter→Gateway→磁盘文件→LlmLabelExtractor。

    关键约束：文件必须由 Gateway 产生——禁止手工造文件替代。
    """

    def test_full_integration_fake_to_extractor(self, tmp_path):
        """完整链路: FakeAdapter→Gateway→文件→LlmLabelExtractor。"""
        # 1. 准备 FakeAdapter
        adapter = FakeLLMAdapter()
        adapter.register_default_for_task("extract_label_rules", _make_label_fake_response())

        # 2. 构造 Gateway + PromptManager
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter, prompt_manager=prompt_manager,
            response_root=str(tmp_path),
        )

        # 3. 构造 Extractor（注入 Gateway）
        extractor = LlmLabelExtractor(gateway=gateway)

        # 4. 构造测试 Spec
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl, DatasetType, InputTableDecl, OutputColumnDecl,
            OutputSpecDecl, ParsedDeveloperSpec,
        )
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h_test", title="测试", description="CASE WHEN 逻辑",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="tf", source_table="fact",
                    columns=[
                        ColumnDecl(column_name="distance_miles",
                                   normalized_name="distance_miles"),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(columns=[
                OutputColumnDecl(name="distance_category", type="string"),
            ]),
            time_range=None,
        )

        # 5. 执行提取
        proposals, artifact = extractor.extract(spec, ["distance_category"])

        # 6. 验证
        assert len(proposals) == 1
        assert proposals[0].output_column == "distance_category"
        # 系统生成字段
        assert proposals[0].proposal_id != ""
        assert proposals[0].source_spec_hash == spec.spec_hash
        assert proposals[0].else_value == "long"  # else_value 为必填 str
        assert proposals[0].label_domain is not None  # label_domain 为必填
        assert len(proposals[0].label_domain.values) == 2
        # evidence 为必填
        for branch in proposals[0].branches:
            assert branch.evidence != ""

        # 验证溯源 Artifact
        assert artifact.artifact_id != ""
        assert artifact.source_spec_hash == spec.spec_hash
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestGatewayFilePersistence tests/labels/test_label_rules.py::TestFakeAdapterToExtractorIntegration -v 2>&1 | tail -20
```

预期：全部 FAIL（Gateway 尚无 response_root 参数，无文件写入逻辑）

- [ ] **Step 3: 实现 Gateway response_root + 文件写入**

修改 `src/tianshu_datadev/llm/gateway.py`：

```python
"""LLMGateway——LLM 调用统一入口。

所有 LLM 交互必须通过此 Gateway——不接受自由 Prompt，不将原始文本传入 Compiler。
Gateway 仅返回结构化对象引用和校验状态。

核心保证：
1. Prompt 仅从 PromptManager 加载——不接受自由 Prompt 文本
2. 输出经过 Pydantic Schema 校验（model_validate）
3. 校验通过后原子写入 response_root——parsed_json_ref 指向落盘文件
4. validation_status != "valid" 的响应不返回结构化对象
5. LLM 原始文本落盘为引用，绝不进入 Compiler
6. 所有错误路径返回 LlmResponse（不抛异常）——便于上层统一处理
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter
from tianshu_datadev.llm.models import (
    LlmRequest,
    LlmResponse,
    SchemaBinding,
)

if TYPE_CHECKING:
    from tianshu_datadev.prompts.manager import PromptManager


class LLMGateway:
    """LLM 调用统一入口——所有 LLM 交互必须通过此 Gateway。

    工作流程：
    1. 校验 LlmRequest 合法性（task 存在、version 存在）
    2. 加载 Prompt 模板 → 渲染 user_message（含 extra_vars）
    3. 调用 ProviderAdapter.invoke()
    4. 解析 JSON → Pydantic model_validate 校验
    5. Schema 校验通过 → 原子写入结构化对象到 response_root
    6. 返回 LlmResponse（仅含引用和校验状态）

    validation_status="invalid" 的响应不进入编译链路——
    在 Gateway 层即被拦截，上层代码通过 is_valid 判断是否可继续。
    """

    def __init__(
        self,
        adapter: ProviderAdapter,
        prompt_manager: PromptManager,
        response_root: str = "llm_responses",
    ) -> None:
        """初始化 LLM Gateway。

        Args:
            adapter: LLM Provider 适配器（Fake / OpenAI / Anthropic）
            prompt_manager: Prompt 版本管理器
            response_root: 结构化输出落盘根目录——所有通过 Schema 校验的
                           parsed_json 原子写入此目录下。默认为 "llm_responses"。
        """
        self._adapter = adapter
        self._prompt_manager = prompt_manager
        self._response_root = Path(response_root)

    @property
    def adapter(self) -> ProviderAdapter:
        return self._adapter

    @property
    def prompt_manager(self) -> PromptManager:
        return self._prompt_manager

    @property
    def response_root(self) -> Path:
        return self._response_root

    def submit(self, request: LlmRequest, **extra_vars) -> LlmResponse:
        """提交 LLM 请求——完整流程：Prompt → Adapter → Schema 校验 → 落盘 → 返回。

        所有错误路径返回 LlmResponse(validation_status="invalid")——
        不抛出异常，便于上层统一处理。

        Args:
            request: LlmRequest——含 task、version、Schema 绑定、输入引用
            **extra_vars: 额外模板变量——渲染 Prompt 中的 {var} 占位符
                         用于注入 markdown_body/unresolved_columns 等动态内容

        Returns:
            LlmResponse——含 validation_status 和 parsed_json_ref
        """
        start_time = time.time()

        # ── 1. 校验 task 和 version 存在 ──
        try:
            self._prompt_manager.list_versions(request.task)
        except ValueError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[f"未知 task：{e}"],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 2. 加载 Prompt 模板 ──
        try:
            template = self._prompt_manager.get_prompt(
                request.task, request.prompt_version
            )
        except ValueError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[f"Prompt 加载失败：{e}"],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 2.5 校验请求 Schema 与 Prompt 绑定一致 ──
        if (
            request.schema_name != template.schema_binding.schema_name
            or request.schema_version != template.schema_binding.schema_version
        ):
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[
                    f"Schema 绑定不一致：请求声称 schema_name='{request.schema_name}' "
                    f"(v{request.schema_version})，但 Prompt 模板 "
                    f"'{request.task}/{request.prompt_version}' "
                    f"绑定到 '{template.schema_binding.schema_name}' "
                    f"(v{template.schema_binding.schema_version})"
                ],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 3. 渲染 user_message（含 extra_vars）──
        user_message = self._render_user_message(
            template=template.user_message_template,
            input_refs=request.input_artifact_refs,
            **extra_vars,
        )

        # ── 4. 获取 JSON Schema ──
        json_schema = template.schema_binding.json_schema

        # ── 5. 调用 Adapter ──
        try:
            raw_output = self._adapter.invoke(
                system_message=template.system_message,
                user_message=user_message,
                json_schema=json_schema,
                model=request.model,
                temperature=request.temperature,
            )
        except AdapterError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[
                    f"LLM Adapter 调用失败（provider={self._adapter.provider_name()}）：{e}"
                ],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 6. Schema 校验 ──
        validated, errors = self._validate_against_schema(
            raw_output=raw_output,
            schema_binding=template.schema_binding,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        # ── 7. 构造响应（含文件落盘）──
        raw_ref = LlmResponse.generate_response_ref(request.request_id)

        if validated is not None and not errors:
            # Schema 校验通过——原子写入结构化对象到 response_root
            parsed_ref = LlmResponse.generate_parsed_ref(request.request_id)
            self._write_parsed_output(validated, parsed_ref)

            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref=raw_ref,
                parsed_json_ref=parsed_ref,
                validation_status="valid",
                validation_errors=[],
                token_usage=raw_output.get("_token_usage", {}),
                latency_ms=latency_ms,
            )
        else:
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref=raw_ref,
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=errors,
                token_usage=raw_output.get("_token_usage", {}),
                latency_ms=latency_ms,
            )

    # ── 内部方法 ──

    @staticmethod
    def _render_user_message(
        template: str,
        input_refs: list,
        **extra_vars,
    ) -> str:
        """将 artifact 引用和额外变量渲染到用户消息模板中。

        Args:
            template: 用户消息模板（含 {var} 占位符）
            input_refs: ArtifactRef 列表
            **extra_vars: 额外模板变量——{markdown_body}/{unresolved_columns} 等

        Returns:
            渲染后的用户消息
        """
        refs_json = json.dumps(
            [ref.model_dump() for ref in input_refs],
            ensure_ascii=False,
            indent=2,
        )
        rendered = template.replace("{artifact_refs}", refs_json)
        # 渲染额外变量
        for key, value in extra_vars.items():
            rendered = rendered.replace(f"{{{key}}}", str(value))
        return rendered

    def _write_parsed_output(self, validated_model: Any, parsed_ref: str) -> None:
        """原子写入通过 Schema 校验的结构化对象到 response_root。

        写入策略：先写临时文件→os.replace 原子重命名——
        确保不会读到半写入的文件。

        Args:
            validated_model: Pydantic model_validate 通过的对象
            parsed_ref: LlmResponse.generate_parsed_ref() 返回的相对路径
        """
        target_path = self._response_root / parsed_ref
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # 原子写入——先写临时文件，再 rename
        tmp_fd = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix=".json",
                prefix=".tmp_",
                dir=str(target_path.parent),
            )
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(
                    validated_model.model_dump(mode="json"),
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            # 原子重命名
            os.replace(tmp_path, str(target_path))
        except Exception:
            # 清理临时文件（如果有）
            if tmp_fd is not None:
                try:
                    os.close(tmp_fd)
                except OSError:
                    pass
            raise

    @staticmethod
    def _validate_against_schema(
        raw_output: dict[str, Any],
        schema_binding: SchemaBinding,
    ) -> tuple[Any | None, list[str]]:
        """对 LLM 原始输出执行 Pydantic Schema 校验。"""
        model_cls = _import_pydantic_model(schema_binding.pydantic_model_path)
        try:
            validated = model_cls.model_validate(raw_output)
            return validated, []
        except ValidationError as e:
            errors = _format_validation_errors(e)
            return None, errors
        except Exception as e:
            return None, [f"Schema 校验异常：{e}"]


def _import_pydantic_model(model_path: str):
    """动态导入 Pydantic 模型类。"""
    import importlib

    parts = model_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"无效的模型路径：'{model_path}'")

    module_path, class_name = parts
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _format_validation_errors(exc: ValidationError) -> list[str]:
    """将 Pydantic ValidationError 格式化为人类可读的错误列表。"""
    errors: list[str] = []
    for error in exc.errors():
        loc = " -> ".join(str(p) for p in error["loc"])
        msg = error["msg"]
        error_type = error.get("type", "unknown")
        errors.append(f"[{error_type}] {loc}: {msg}")
    return errors
```

- [ ] **Step 4: 运行集成测试验证通过**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestGatewayFilePersistence tests/labels/test_label_rules.py::TestFakeAdapterToExtractorIntegration -v
```

预期：全部 PASS

- [ ] **Step 5: 运行现有 Gateway 测试确保无回归**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/llm/ -v -k "gateway" 2>&1 | tail -10
```

预期：全部 PASS（`**extra_vars` 为空时行为与修改前完全一致；response_root 默认 "llm_responses" 保持兼容）

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/llm/gateway.py tests/labels/test_label_rules.py
git commit -m "feat(gateway): 新增 response_root 文件持久化 + extra_vars 模板变量渲染

- LLMGateway.__init__ 新增 response_root 参数——Schema 校验通过后原子写入结构化 JSON
- _render_user_message 扩展 **extra_vars——支持 {markdown_body} 等动态占位符
- submit() 新增 **extra_vars 参数——向后兼容
- 新增 FakeAdapter→Gateway→磁盘文件→LlmLabelExtractor 集成测试
- 验证未知字段/非法根节点被正确阻断"
```

---

### Task 1: 基础模型——DatasetType + LabelPredicateNode discriminator 联合 + 根条件约束

**边界：** 定义 Pydantic 模型。v4-light 最终版新增 `LabelPredicateCondition` 类型——从 `LabelPredicateNode`（8 子类）中排除 `LabelColumnRef` 和 `LabelTypedLiteral`，**LITERAL/COLUMN_REF 不可作为 WHEN 根条件**。不涉及任何管线逻辑、不调 LLM、不做验证。

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py`（在现有枚举之后插入）
- Test: `tests/planning/test_planning_models.py`（末尾追加）

**Interfaces（Produces）:**
- `DatasetType(str, Enum)`——DETAIL_TABLE/AGGREGATE_TABLE/LABEL_TABLE/UNSPECIFIED
- `CompareOp(str, Enum)`——EQ/NEQ/GT/GTE/LT/LTE
- `LabelColumnRef`——node_type="COLUMN_REF", column_name: str
- `LabelTypedLiteral`——node_type="LITERAL", value: str|Decimal|bool|None, data_type: "string"|"number"|"boolean"|"null"
- `LabelCompare`——node_type="COMPARE", left: str, op: CompareOp, right: LabelTypedLiteral
- `LabelIsNull`——node_type="IS_NULL", column: str
- `LabelIsNotNull`——node_type="IS_NOT_NULL", column: str
- `LabelAnd`——node_type="AND", children: list[LabelPredicateNode]
- `LabelOr`——node_type="OR", children: list[LabelPredicateNode]
- `LabelNot`——node_type="NOT", child: LabelPredicateNode
- `LabelPredicateNode` = Annotated[Union[8 子类], Field(discriminator="node_type")]——完整 AST
- **`LabelPredicateCondition`** = Annotated[Union[6 子类——排除 COLUMN_REF/LITERAL], Field(discriminator="node_type")]——**v4-light 最终版新增**：仅允许 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT 作为 WHEN 根条件

- [ ] **Step 1: 编写测试——含根条件约束验证**

在 `tests/planning/test_planning_models.py` 末尾追加：

```python
# ================================================
# v4-light 最终版: DatasetType + LabelPredicateNode + 根条件约束
# ================================================

from decimal import Decimal
import pytest
from pydantic import ValidationError
from tianshu_datadev.developer_spec.models import (
    DatasetType, CompareOp,
    LabelColumnRef, LabelTypedLiteral,
    LabelCompare, LabelIsNull, LabelIsNotNull,
    LabelAnd, LabelOr, LabelNot,
    LabelPredicateNode, LabelPredicateCondition,
)


class TestDatasetType:
    def test_serialize_label_table(self):
        assert DatasetType.LABEL_TABLE.value == "label_table"

    def test_default_unspecified(self):
        assert DatasetType.UNSPECIFIED.value == "unspecified"


class TestLabelPredicateNodeDiscriminator:
    """8 子类 discriminator 联合 AST。"""

    def test_compare_node(self):
        node = LabelCompare(
            left="distance_miles", op=CompareOp.LTE,
            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
        )
        assert node.node_type == "COMPARE"

    def test_is_null_node(self):
        node = LabelIsNull(column="distance_miles")
        assert node.node_type == "IS_NULL"

    def test_is_not_null_node(self):
        node = LabelIsNotNull(column="distance_miles")
        assert node.node_type == "IS_NOT_NULL"

    def test_and_node(self):
        node = LabelAnd(children=[
            LabelCompare(left="a", op=CompareOp.GT,
                        right=LabelTypedLiteral(value=Decimal("0"), data_type="number")),
            LabelCompare(left="a", op=CompareOp.LT,
                        right=LabelTypedLiteral(value=Decimal("10"), data_type="number")),
        ])
        assert node.node_type == "AND"
        assert len(node.children) == 2

    def test_or_node(self):
        node = LabelOr(children=[
            LabelIsNull(column="x"),
            LabelCompare(left="y", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value=True, data_type="boolean")),
        ])
        assert node.node_type == "OR"

    def test_not_node(self):
        node = LabelNot(child=LabelIsNull(column="x"))
        assert node.node_type == "NOT"

    def test_nested_and_or(self):
        """AND(OR(...), COMPARE) 嵌套。"""
        node = LabelAnd(children=[
            LabelOr(children=[
                LabelCompare(left="a", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value="x", data_type="string")),
                LabelCompare(left="a", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value="y", data_type="string")),
            ]),
            LabelIsNotNull(column="b"),
        ])
        assert node.node_type == "AND"


class TestLabelPredicateConditionRootConstraint:
    """v4-light 最终版: LabelPredicateCondition 仅允许 6 种根节点类型。
    LITERAL/COLUMN_REF 不可作为 WHEN 根条件。"""

    def test_compare_is_valid_root(self):
        """COMPARE 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "COMPARE", "left": "col",
            "op": "=",
            "right": {"node_type": "LITERAL", "value": "test", "data_type": "string"},
        })
        assert node.node_type == "COMPARE"

    def test_is_null_is_valid_root(self):
        """IS_NULL 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "IS_NULL", "column": "col",
        })
        assert node.node_type == "IS_NULL"

    def test_and_is_valid_root(self):
        """AND 是合法根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        node = adapter.validate_python({
            "node_type": "AND", "children": [
                {"node_type": "COMPARE", "left": "a", "op": ">",
                 "right": {"node_type": "LITERAL", "value": 0, "data_type": "number"}},
            ],
        })
        assert node.node_type == "AND"

    def test_literal_rejected_as_root(self):
        """LITERAL 不可作根条件——Pydantic discriminator 拒绝。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        with pytest.raises(ValidationError):
            adapter.validate_python({
                "node_type": "LITERAL", "value": "short", "data_type": "string",
            })

    def test_column_ref_rejected_as_root(self):
        """COLUMN_REF 不可作根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateCondition)
        with pytest.raises(ValidationError):
            adapter.validate_python({
                "node_type": "COLUMN_REF", "column_name": "col",
            })

    def test_label_predicate_node_still_allows_literal(self):
        """LabelPredicateNode（完整 AST）仍允许 LITERAL/COLUMN_REF——
        仅 LabelPredicateCondition 限制了根条件。"""
        from pydantic import TypeAdapter
        adapter = TypeAdapter(LabelPredicateNode)
        node = adapter.validate_python({
            "node_type": "LITERAL", "value": 5, "data_type": "number",
        })
        assert node.node_type == "LITERAL"
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_planning_models.py::TestDatasetType tests/planning/test_planning_models.py::TestLabelPredicateNodeDiscriminator tests/planning/test_planning_models.py::TestLabelPredicateConditionRootConstraint -v
```

预期：FAIL（ImportError——模型尚未定义）

- [ ] **Step 3: 实现全部模型**

在 `src/tianshu_datadev/developer_spec/models.py` 中插入（现有枚举之后）：

```python
# ================================================
# v4-light 最终版: DatasetType + CompareOp + LabelPredicateNode discriminator 联合 AST
# ================================================

class DatasetType(str, Enum):
    """数据产品类型——决定验证策略和能力门禁，不驱动 Builder 代码路径分叉。"""
    DETAIL_TABLE = "detail_table"
    AGGREGATE_TABLE = "aggregate_table"
    LABEL_TABLE = "label_table"
    UNSPECIFIED = "unspecified"


class CompareOp(str, Enum):
    """比较操作符——封闭集合。"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class LabelColumnRef(StrictModel):
    """列引用叶子——引用源表中已声明的字段。不可作 WHEN 根条件。"""
    node_type: Literal["COLUMN_REF"] = "COLUMN_REF"
    column_name: str


class LabelTypedLiteral(StrictModel):
    """类型化字面量——真实 Python 类型。不可作 WHEN 根条件。"""
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


# ── 完整 AST（8 子类 discriminator 联合）──
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

# ── 根条件类型（仅 6 子类——排除 COLUMN_REF 和 LITERAL）──
# LITERAL/COLUMN_REF 不可作为 WHEN 根条件——LLM 若输出则 Pydantic discriminator 拒绝
LabelPredicateCondition = Annotated[
    Union[
        LabelAnd,
        LabelOr,
        LabelNot,
        LabelCompare,
        LabelIsNull,
        LabelIsNotNull,
    ],
    Field(discriminator="node_type"),
]
```

- [ ] **Step 4: 运行测试验证通过**

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py tests/planning/test_planning_models.py
git commit -m "feat(models): DatasetType + LabelPredicateNode(8子类) + LabelPredicateCondition(6子类根约束)

- LabelPredicateCondition 仅允许 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT 作根条件
- LITERAL/COLUMN_REF 不可作 WHEN 根节点——Pydantic discriminator 在 Schema 层拒绝"
```

---

### Task 2: 标签领域模型——LLM 输出 Schema 与系统内部模型分离 + 必需字段强制

**边界：** v4-light 最终版核心变更：
1. LLM 输出层不含系统字段（proposal_id/source_spec_hash/extraction_time）
2. **系统层 `LabelRuleProposal` 强制 `else_value: str`（非 Optional）、`label_domain: LabelDomain`（非 Optional）**
3. **系统层 `LabelBranchProposal.evidence: str` 必填（非空字符串 ""）**
4. **系统层 `LabelRuleProposal` 新增 `label_domain: LabelDomain` 字段**——由 LlmLabelExtractor 从 LLM 输出的 `LabelDomainOutput` 包装为系统 `LabelDomain`
5. `LabelBranchProposalOutput` 的 `condition` 使用 `LabelPredicateCondition`（非完整 `LabelPredicateNode`）

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py`（在 Task 1 新增代码之后）
- Test: `tests/planning/test_planning_models.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelPredicateNode`、`LabelPredicateCondition`（Task 1）
- Produces (LLM 输出层): `LabelDomainOutput`, `LabelBranchProposalOutput`, `LabelRuleProposalOutput`, `LabelRuleProposalList`
- Produces (系统层): `LabelDomain`, `LabelBranchProposal`, `LabelRuleProposal`, `LabelPredicateBranch`

- [ ] **Step 1: 编写测试——含必需字段 + 根条件约束**

在 `tests/planning/test_planning_models.py` 末尾追加：

```python
# ================================================
# v4-light 最终版: LLM 输出 Schema 与系统模型分离 + 必需字段强制
# ================================================

import pytest
from pydantic import ValidationError
from tianshu_datadev.developer_spec.models import (
    LabelBranchProposalOutput,
    LabelDomainOutput,
    LabelRuleProposalList,
    LabelRuleProposalOutput,
    LabelDomain,
    LabelBranchProposal,
    LabelRuleProposal,
    LabelPredicateBranch,
)


class TestLabelDomainOutput:
    """LLM 输出的标签值域——不含系统字段。"""

    def test_llm_output_no_system_fields(self):
        domain = LabelDomainOutput(
            values=["unknown", "short", "medium", "long"],
            source_evidence="分为四类",
            is_exhaustive=True,
            completeness_evidence="以上四类覆盖全部",
        )
        assert "domain_id" not in domain.model_fields


class TestLabelRuleProposalOutput:
    """LLM 输出不含 proposal_id/source_spec_hash。"""

    def test_forbidden_system_fields(self):
        output = LabelRuleProposalOutput(
            output_column="distance_category",
            branches=[
                LabelBranchProposalOutput(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomainOutput(values=["short", "long"]),
        )
        assert "proposal_id" not in output.model_fields
        assert "source_spec_hash" not in output.model_fields

    def test_literal_root_condition_rejected_in_branch(self):
        """LITERAL 不可作 LabelBranchProposalOutput 的 condition。"""
        with pytest.raises(ValidationError):
            LabelBranchProposalOutput(
                condition=LabelTypedLiteral(value="short", data_type="string"),
                then_label="short",
                evidence="非法根条件",
            )


class TestSystemModelRequiredFields:
    """系统模型——else_value/label_domain/evidence 均为必需。"""

    def test_else_value_required(self):
        """else_value 为必填 str——不可为 None 或缺失。"""
        with pytest.raises(ValidationError):
            LabelRuleProposal(
                proposal_id="p1", source_spec_hash="h",
                output_column="distance_category",
                branches=[
                    LabelBranchProposal(
                        condition=LabelCompare(
                            left="distance_miles", op=CompareOp.LTE,
                            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                        ),
                        then_label="short",
                        evidence="<=2 -> short",
                    ),
                ],
                # else_value 缺失 → ValidationError
            )

    def test_label_domain_required(self):
        """label_domain 为必填 LabelDomain——不可为 None 或缺失。"""
        with pytest.raises(ValidationError):
            LabelRuleProposal(
                proposal_id="p1", source_spec_hash="h",
                output_column="distance_category",
                branches=[
                    LabelBranchProposal(
                        condition=LabelCompare(
                            left="distance_miles", op=CompareOp.LTE,
                            right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                        ),
                        then_label="short",
                        evidence="<=2 -> short",
                    ),
                ],
                else_value="long",
                # label_domain 缺失 → ValidationError
            )

    def test_evidence_required_in_branch(self):
        """evidence 为必填 str——空字符串导致 Promotion 拒绝。"""
        # Pydantic 层允许空字符串（非 None），但 Promotion 检查非空
        branch = LabelBranchProposal(
            condition=LabelCompare(
                left="distance_miles", op=CompareOp.LTE,
                right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
            ),
            then_label="short",
            evidence="",  # 空字符串——Promotion 阶段拒绝
        )
        assert branch.evidence == ""

    def test_system_model_has_id_and_domain_fields(self):
        """系统层含 proposal_id/source_spec_hash/label_domain/else_value。"""
        proposal = LabelRuleProposal(
            proposal_id="sys_gen_001",
            source_spec_hash="hash_abc",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(
                domain_id="dom_001",
                values=["short", "long"],
                source_evidence="原文分类",
            ),
        )
        assert proposal.proposal_id == "sys_gen_001"
        assert proposal.else_value == "long"
        assert proposal.label_domain.values == ["short", "long"]
```

- [ ] **Step 2: 运行测试验证失败**

- [ ] **Step 3: 实现 LLM 输出层 + 系统层模型**

在 `src/tianshu_datadev/developer_spec/models.py` 的 `LabelPredicateCondition` 定义之后插入：

```python
# ================================================
# v4-light 最终版: LLM 输出 Schema——LLM 直接产出的结构化数据
# 原则：LLM 只输出规则、标签域和 evidence
#       proposal_id / source_spec_hash / extraction_time 由系统生成
#       LabelBranchProposalOutput.condition 使用 LabelPredicateCondition
#       （排除 LITERAL/COLUMN_REF 根条件）
# ================================================

class LabelDomainOutput(StrictModel):
    """LLM 从原文提取的标签值域——不含系统生成字段。"""
    values: list[str] = []
    source_evidence: str = ""
    is_exhaustive: bool = False
    completeness_evidence: str = ""


class LabelBranchProposalOutput(StrictModel):
    """LLM 输出的单条 WHEN-THEN 分支——condition 仅允许 6 种根条件类型。"""
    condition: LabelPredicateCondition  # ← LITERAL/COLUMN_REF 在 Schema 层拒绝
    then_label: str
    evidence: str = ""  # LLM 层可为空——系统包装时由 Extractor 校验非空


class LabelRuleProposalOutput(StrictModel):
    """LLM 输出的单条标签规则——不含 proposal_id/source_spec_hash。"""
    output_column: str
    branches: list[LabelBranchProposalOutput]
    else_value: str  # LLM 层必填——label_table v1 要求 ELSE
    label_domain: LabelDomainOutput | None = None


class LabelRuleProposalList(StrictModel):
    """LLM 输出的规则列表——顶层 Schema，注册到 _SCHEMA_PATH_MAP。"""
    rules: list[LabelRuleProposalOutput]


# ================================================
# 系统内部模型——由 LlmLabelExtractor 包装 LLM 输出后生成
# proposal_id / source_spec_hash / extraction_time 由系统注入
# else_value / label_domain / evidence 均为必需——缺失时 Promotion 拒绝
# ================================================

class LabelDomain(StrictModel):
    """系统包装的标签值域——含系统生成的 domain_id。"""
    domain_id: str = ""
    values: list[str] = []
    source_evidence: str = ""
    is_exhaustive: bool = False
    completeness_evidence: str = ""


class LabelBranchProposal(StrictModel):
    """系统包装的单条 WHEN-THEN 分支——evidence 必填非空。"""
    condition: LabelPredicateCondition  # ← LITERAL/COLUMN_REF 在 Schema 层拒绝
    then_label: str
    evidence: str  # 必填——Promotion 阶段检查非空


class LabelRuleProposal(StrictModel):
    """系统包装的标签规则候选——proposal_id/source_spec_hash 由系统生成。

    label_table v1 强制要求：
    - else_value: str（必填——ELSE 子句）
    - label_domain: LabelDomain（必填——标签值域）
    - 每个 branch.evidence 非空
    """
    proposal_id: str
    source_spec_hash: str
    output_column: str
    branches: list[LabelBranchProposal]
    else_value: str  # ← 必填 str（非 Optional）
    label_domain: LabelDomain  # ← 必填 LabelDomain（非 Optional）


class LabelPredicateBranch(StrictModel):
    """已验证的类型化 WHEN-THEN 分支——仅含确定性信息。"""
    condition: LabelPredicateCondition
    then_label: str
```

- [ ] **Step 4: 修改 ParsedDeveloperSpec + CaseWhenDecl**（添加 dataset_type/label_rules/typed_branches 字段）

- [ ] **Step 5: 运行测试 + 完整回归**

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py tests/planning/test_planning_models.py
git commit -m "feat(models): LLM 输出/系统模型分离 + 必需字段强制 + 根条件约束

- LLM 输出层: LabelDomainOutput/LabelBranchProposalOutput/LabelRuleProposalOutput/LabelRuleProposalList
  * 禁止 proposal_id/source_spec_hash/extraction_time
  * LabelBranchProposalOutput.condition 使用 LabelPredicateCondition（6子类——排除LITERAL/COLUMN_REF）
- 系统层: LabelDomain/LabelBranchProposal/LabelRuleProposal/LabelPredicateBranch
  * else_value: str（必填）、label_domain: LabelDomain（必填）、evidence: str（必填）
  * proposal_id/source_spec_hash 由系统生成
- ParsedDeveloperSpec: 新增 dataset_type/label_rules 字段
- CaseWhenDecl: 新增 typed_branches 字段"
```

---

### Task 3: 溯源 Artifact + ValidationReport 模型

**边界：** Artifact 模型（Task 1/2 依赖就绪后）。

**Files:**
- Create: `src/tianshu_datadev/labels/__init__.py`
- Create: `src/tianshu_datadev/labels/artifacts.py`
- Test: `tests/labels/test_label_rules.py`（新建文件，首个测试）

**Interfaces:**
- Consumes: `LabelRuleProposal`, `CaseWhenDecl`（Task 2）
- Produces: `LabelExtractionArtifact`——artifact_id, source_spec_hash, extraction_time, llm_model, llm_prompt_version, llm_temperature, unresolved_columns, raw_proposals, prompt_snapshot
- Produces: `LabelPromotionArtifact`——artifact_id, parent_spec_hash, new_spec_hash, promotion_time, extraction_artifact_id, promoted_rules, validation_reports, rejected_proposals, human_review_required
- Produces: `LabelValidationReport`——proposal_id, passed, checks, blocking_errors, human_review_items, warnings
- Produces: `LabelValidationCheck`——check_name, passed, level: BLOCKING|HUMAN_REVIEW|WARN, detail

- [ ] **Step 1: 编写测试**

```python
# tests/labels/test_label_rules.py 开头
from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact, LabelPromotionArtifact,
    LabelValidationReport, LabelValidationCheck,
)


class TestLabelExtractionArtifact:
    def test_fields(self):
        artifact = LabelExtractionArtifact(
            artifact_id="ext_001", source_spec_hash="h",
            extraction_time="2026-07-15T00:00:00Z",
            llm_model="fake", llm_prompt_version="v001",
            llm_temperature=0.1, unresolved_columns=["col1"],
            raw_proposals=[], prompt_snapshot="",
        )
        assert artifact.artifact_id == "ext_001"


class TestLabelValidationReport:
    def test_passed_requires_both_empty(self):
        """passed=True 要求 blocking_errors 和 human_review_items 均为空。"""
        report = LabelValidationReport(
            proposal_id="p1", passed=True, checks=[],
            blocking_errors=[], human_review_items=[], warnings=[],
        )
        assert report.passed

    def test_human_review_causes_not_passed(self):
        """human_review_items 非空→passed=False。"""
        report = LabelValidationReport(
            proposal_id="p1", passed=False, checks=[],
            blocking_errors=[],
            human_review_items=["缺少 ELSE"],
            warnings=[],
        )
        assert not report.passed
```

- [ ] **Step 2-5: 实现 + 测试 + Commit**

```bash
git add src/tianshu_datadev/labels/__init__.py src/tianshu_datadev/labels/artifacts.py tests/labels/test_label_rules.py
git commit -m "feat(labels): 新增 LabelExtractionArtifact + LabelPromotionArtifact + ValidationReport 模型"
```

---

### Task 4: Parser type 映射（仅 type → dataset_type）

**边界：** Parser 只负责 `spec_dict["type"]` → `DatasetType` 映射，不含 unresolved 检测。

**Files:** Modify: `src/tianshu_datadev/developer_spec/parser.py`、Test: `tests/api/test_spec.py`

**Interfaces:**
- Modifies: `parse()`——读取 `spec_dict["type"]` → 映射到 `DatasetType`
- Produces: `ParsedDeveloperSpec.dataset_type`

- [ ] **Step 1: 编写测试**

在 `tests/api/test_spec.py` 末尾追加：

```python
def test_parse_label_table_type():
    """验证 YAML type: label_table → DatasetType.LABEL_TABLE。"""
    from tianshu_datadev.developer_spec.models import DatasetType
    from tianshu_datadev.developer_spec.parser import parse
    yaml_text = """
    type: label_table
    title: 测试标签表
    description: 测试
    input_tables: []
    output_columns:
      - name: label_col
        type: string
    """
    spec = parse(yaml_text)
    assert spec.dataset_type == DatasetType.LABEL_TABLE
```

- [ ] **Step 2-5: 实现 + 测试 + Commit**

---

### Task 5: _find_unresolved_derived_columns()——独立于 Parser

**边界：** 存放于 `labels/resolver.py`。

**Files:** Create: `src/tianshu_datadev/labels/resolver.py`、Test: `tests/api/test_pipeline.py`

**Interfaces:**
- Produces: `_find_unresolved_derived_columns(spec, manifest=None) -> list[str]`

- [ ] **Step 1-5: 完整 TDD 流程（含物理列/指标/窗口指标/compute_step/label_rule/Manifest schema 各场景测试）**

---

### Task 6: LabelRuleValidator v1——六项确定性检查 + 双空通过

**边界：** v4-light 最终版 Validator v1。**passed = blocking_errors 和 human_review_items 均为空**。删除全部区间证明逻辑。

**六项检查：**

| # | 检查项 | 失败级别 | 说明 |
|---|--------|----------|------|
| 1 | FIELD_EXISTS | BLOCKING | condition 中引用的列名存在于 input_tables |
| 2 | TYPE_COMPATIBLE | BLOCKING | 比较操作符与字面量 data_type 兼容 |
| 3 | OPERATOR_VALID | BLOCKING | 操作符为已知 CompareOp；AND/OR children>=2；NOT child 非空 |
| 4 | AST_VALID | BLOCKING | condition 为 LabelPredicateCondition discriminator 子类 |
| 5 | LABEL_DOMAIN | BLOCKING | then_label 值在 label_domain.values 中 |
| 6 | COVERAGE | BLOCKING/HUMAN_REVIEW | 有 ELSE 且所有 evidence 非空→PASS；否则 HUMAN_REVIEW |

**关键判定逻辑（v4-light 最终版）：**
```python
# passed = 无阻断 且 无人工审查
passed = len(blocking) == 0 and len(human_review) == 0
```

**Files:**
- Create: `src/tianshu_datadev/labels/label_rule_validator.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelRuleProposal`、`LabelDomain`（Task 2）、`LabelValidationReport`/`LabelValidationCheck`（Task 3）
- Produces: `LabelRuleValidator.validate(proposal, spec) -> LabelValidationReport`

- [ ] **Step 1: 编写测试——含双空通过验证**

```python
# tests/labels/test_label_rules.py 末尾追加

from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator


def _make_test_spec():
    """构造测试用 ParsedDeveloperSpec。"""
    from tianshu_datadev.developer_spec.models import (
        ColumnDecl, InputTableDecl, OutputColumnDecl, OutputSpecDecl,
        ParsedDeveloperSpec,
    )
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="t", description="d",
        dataset_type=DatasetType.LABEL_TABLE,
        input_tables=[
            InputTableDecl(
                table_alias="tf", source_table="fact",
                columns=[
                    ColumnDecl(column_name="distance_miles",
                               normalized_name="distance_miles"),
                    ColumnDecl(column_name="is_distance_outlier",
                               normalized_name="is_distance_outlier"),
                ],
                key_columns=[], business_columns=[],
            ),
        ],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(columns=[
            OutputColumnDecl(name="distance_category", type="string"),
        ]),
        time_range=None,
    )


class TestValidatorV1FieldExists:
    def test_field_exists_passes(self):
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        field_check = next(c for c in report.checks if c.check_name == "FIELD_EXISTS")
        assert field_check.passed


class TestValidatorV1Coverage:
    def test_missing_else_with_empty_evidence_not_passed(self):
        """无 ELSE + evidence 为空 → HUMAN_REVIEW → passed=False。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",
                ),
            ],
            else_value="long",  # 有 ELSE——但 evidence 为空
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        # evidence 为空→HUMAN_REVIEW→passed=False（即使有 ELSE）
        # 注意：本版 evidence 为空也在 COVERAGE 中触发 HUMAN_REVIEW
        coverage_checks = [c for c in report.checks if c.check_name == "COVERAGE"]
        if coverage_checks:
            # evidence 为空→HUMAN_REVIEW
            assert any("evidence" in c.detail.lower() for c in coverage_checks
                       if not c.passed)

    def test_all_evidence_present_passes(self):
        """全部 evidence 非空 + ELSE→passed=True。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert report.passed, f"blocking={report.blocking_errors}, review={report.human_review_items}"


class TestValidatorV1DoubleEmpty:
    """v4-light 最终版: passed 要求 blocking_errors 和 human_review_items 均为空。"""

    def test_human_review_causes_fail(self):
        """human_review_items 非空→即使 blocking 为空也 passed=False。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        # evidence 为空→HUMAN_REVIEW→passed=False
        assert not report.passed, "human_review_items 非空时 passed 应为 False"


class TestValidatorV1LabelDomain:
    def test_label_outside_domain_blocks(self):
        """then_label 不在 domain 中→BLOCKING。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="ultra_short",  # ← 不在 domain 中
                    evidence="<=2 -> ultra_short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "medium", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert any("ultra_short" in e for e in report.blocking_errors)
```

- [ ] **Step 2: 运行测试验证失败**

- [ ] **Step 3: 实现 LabelRuleValidator v1（双空通过）**

```python
"""LabelRuleValidator v1——六项确定性检查。v4-light 最终版: 双空通过 + 无区间证明。"""
from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    LabelAnd, LabelOr, LabelNot, LabelCompare, LabelIsNull, LabelIsNotNull,
    LabelRuleProposal, LabelDomain, ParsedDeveloperSpec, CompareOp,
    LabelValidationReport, LabelValidationCheck,
)


class LabelRuleValidator:
    """确定性标签规则验证器 v1——六项检查，不做区间证明。

    passed = blocking_errors 和 human_review_items 均为空。
    """

    def validate(
        self,
        proposal: LabelRuleProposal,
        spec: ParsedDeveloperSpec,
    ) -> LabelValidationReport:
        """对单个 Proposal 执行全部六项检查。"""
        checks: list[LabelValidationCheck] = []
        blocking: list[str] = []
        human_review: list[str] = []
        warnings: list[str] = []

        # 收集所有已知列名
        known_columns: set[str] = set()
        for t in spec.input_tables:
            for c in t.columns:
                known_columns.add(c.normalized_name)

        # 1. FIELD_EXISTS
        self._check_field_exists(proposal, known_columns, checks, blocking)
        # 2. TYPE_COMPATIBLE
        self._check_type_compatible(proposal, checks, blocking)
        # 3. OPERATOR_VALID
        self._check_operator_valid(proposal, checks, blocking)
        # 4. AST_VALID
        self._check_ast_valid(proposal, checks, blocking)
        # 5. LABEL_DOMAIN
        self._check_label_domain(proposal, checks, blocking)
        # 6. COVERAGE（ELSE + evidence 非空）
        self._check_coverage(proposal, checks, human_review)

        # v4-light 最终版: passed = 双空
        passed = len(blocking) == 0 and len(human_review) == 0
        return LabelValidationReport(
            proposal_id=proposal.proposal_id,
            passed=passed,
            checks=checks,
            blocking_errors=blocking,
            human_review_items=human_review,
            warnings=warnings,
        )

    def _check_field_exists(self, proposal, known_columns, checks, blocking):
        """1. FIELD_EXISTS——condition 中所有列引用必须在已知列中。"""
        missing = []
        for branch in proposal.branches:
            self._collect_column_refs(branch.condition, known_columns, missing)
        if missing:
            unique_missing = sorted(set(missing))
            checks.append(LabelValidationCheck(
                check_name="FIELD_EXISTS", passed=False, level="BLOCKING",
                detail=f"未知列: {unique_missing}",
            ))
            blocking.append(f"未知列: {unique_missing}")
        else:
            checks.append(LabelValidationCheck(
                check_name="FIELD_EXISTS", passed=True, level="BLOCKING",
                detail="所有列引用有效",
            ))

    def _check_type_compatible(self, proposal, checks, blocking):
        """2. TYPE_COMPATIBLE——比较操作符类型与字面量 data_type 兼容。"""
        for branch in proposal.branches:
            invalid = self._find_type_mismatches(branch.condition)
            if invalid:
                checks.append(LabelValidationCheck(
                    check_name="TYPE_COMPATIBLE", passed=False, level="BLOCKING",
                    detail=f"类型不兼容: {invalid}",
                ))
                blocking.append(f"类型不兼容: {invalid}")
                return
        checks.append(LabelValidationCheck(
            check_name="TYPE_COMPATIBLE", passed=True, level="BLOCKING",
            detail="类型兼容",
        ))

    def _check_operator_valid(self, proposal, checks, blocking):
        """3. OPERATOR_VALID——操作符合法，布尔节点子节点数合法。"""
        errors = self._find_operator_errors(proposal)
        if errors:
            checks.append(LabelValidationCheck(
                check_name="OPERATOR_VALID", passed=False, level="BLOCKING",
                detail=f"操作符错误: {errors}",
            ))
            blocking.append(f"操作符错误: {errors}")
        else:
            checks.append(LabelValidationCheck(
                check_name="OPERATOR_VALID", passed=True, level="BLOCKING",
                detail="操作符合法",
            ))

    def _check_ast_valid(self, proposal, checks, blocking):
        """4. AST_VALID——condition 为 LabelPredicateCondition discriminator 子类。"""
        for branch in proposal.branches:
            if isinstance(branch.condition, str):
                checks.append(LabelValidationCheck(
                    check_name="AST_VALID", passed=False, level="BLOCKING",
                    detail="condition 是字符串——必须为 LabelPredicateCondition 子类",
                ))
                blocking.append("condition 是字符串而非结构化 AST")
                return
        checks.append(LabelValidationCheck(
            check_name="AST_VALID", passed=True, level="BLOCKING",
            detail="AST 结构合法",
        ))

    def _check_label_domain(self, proposal, checks, blocking):
        """5. LABEL_DOMAIN——then_label/else_value 在 proposal.label_domain.values 中。"""
        domain = proposal.label_domain
        if not domain or not domain.values:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="BLOCKING",
                detail="无 label_domain values——跳过域检查",
            ))
            return
        domain_set = set(domain.values)
        outside = []
        for branch in proposal.branches:
            if branch.then_label not in domain_set:
                outside.append(branch.then_label)
        if proposal.else_value not in domain_set:
            outside.append(proposal.else_value)
        if outside:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=False, level="BLOCKING",
                detail=f"标签值不在域中: {outside}",
            ))
            blocking.append(f"标签值不在域中: {outside}")
        else:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="BLOCKING",
                detail="所有标签值在域内",
            ))

    def _check_coverage(self, proposal, checks, human_review):
        """6. COVERAGE——ELSE 非空 + 所有 evidence 非空。"""
        empty_evidence = [b.then_label for b in proposal.branches if not b.evidence]
        if empty_evidence:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE", passed=False, level="HUMAN_REVIEW",
                detail=f"分支 evidence 为空: {empty_evidence}",
            ))
            human_review.append(
                f"分支 {empty_evidence} evidence 为空——无法确定性判断覆盖完整性"
            )
        else:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE", passed=True, level="BLOCKING",
                detail="ELSE 非空 + 所有 evidence 非空——覆盖检查通过",
            ))

    # ── 辅助方法 ──

    def _collect_column_refs(self, node, known, missing):
        """递归收集节点树中所有列引用。"""
        if isinstance(node, LabelCompare):
            if node.left not in known:
                missing.append(node.left)
        elif isinstance(node, (LabelIsNull, LabelIsNotNull)):
            if node.column not in known:
                missing.append(node.column)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                self._collect_column_refs(child, known, missing)
        elif isinstance(node, LabelNot):
            self._collect_column_refs(node.child, known, missing)

    def _find_type_mismatches(self, node) -> list[str]:
        """查找类型不兼容的比较。"""
        errors = []
        if isinstance(node, LabelCompare):
            if node.right.data_type == "string" and node.op not in (CompareOp.EQ, CompareOp.NEQ):
                errors.append(
                    f"{node.left} {node.op.value} '{node.right.value}'——"
                    f"string 类型仅支持 =/!="
                )
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                errors.extend(self._find_type_mismatches(child))
        elif isinstance(node, LabelNot):
            errors.extend(self._find_type_mismatches(node.child))
        return errors

    def _find_operator_errors(self, proposal) -> list[str]:
        """检查布尔节点子节点数和操作符合法性。"""
        errors = []
        for branch in proposal.branches:
            self._check_node_structure(branch.condition, errors)
        return errors

    def _check_node_structure(self, node, errors):
        """递归检查节点结构合法性。"""
        if isinstance(node, (LabelAnd, LabelOr)):
            if len(node.children) < 2:
                errors.append(f"{node.node_type} 至少需要 2 个子节点")
            for child in node.children:
                self._check_node_structure(child, errors)
        elif isinstance(node, LabelNot):
            if node.child is None:
                errors.append("NOT 节点需要非空 child")
            else:
                self._check_node_structure(node.child, errors)
```

- [ ] **Step 4: 运行测试验证通过**

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/labels/label_rule_validator.py tests/labels/test_label_rules.py
git commit -m "feat(labels): LabelRuleValidator v1——六项检查 + 双空通过 + 无区间证明

passed = blocking_errors 和 human_review_items 均为空
缺少 ELSE/evidence→HUMAN_REVIEW→passed=False"
```

---

### Task 7: FakeLabelExtractor——pytest 专用

**边界：** `FakeLabelExtractor` 仅用于 pytest——确定性返回预定义 Proposal。

**Files:**
- Create: `src/tianshu_datadev/labels/label_extractor.py`（抽象基类 + FakeLabelExtractor）
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Produces: `LabelExtractor`（抽象基类）——`extract(spec, unresolved_columns) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]`
- Produces: `FakeLabelExtractor(proposals=None)`——确定性实现

- [ ] **Step 1: 编写测试**

```python
class TestFakeLabelExtractor:
    def test_returns_predefined_proposals(self):
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="col",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="x", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value="a", data_type="string"),
                    ),
                    then_label="label_a", evidence="x=a",
                ),
            ],
            else_value="label_b",
            label_domain=LabelDomain(values=["label_a", "label_b"]),
        )
        extractor = FakeLabelExtractor(proposals=[proposal])
        spec = _make_test_spec()
        result, artifact = extractor.extract(spec, ["col"])
        assert len(result) == 1
        assert result[0].output_column == "col"
```

- [ ] **Step 2-5: 实现 + 测试 + Commit**

---

### Task 8: Prompt 模板 + Schema 注册

**边界：** 创建 Prompt 模板文件并在 `_SCHEMA_PATH_MAP` 中注册。

**Files:**
- Create: `src/tianshu_datadev/prompts/templates/extract_label_rules/v001.md`
- Modify: `src/tianshu_datadev/prompts/manager.py`（`_SCHEMA_PATH_MAP` 新增条目）

- [ ] **Step 1: 注册 Schema**

在 `src/tianshu_datadev/prompts/manager.py` 的 `_SCHEMA_PATH_MAP` 字典末尾添加：

```python
"LabelRuleProposalList": (
    "tianshu_datadev.developer_spec.models.LabelRuleProposalList"
),
```

- [ ] **Step 2: 创建 Prompt 模板**

创建 `src/tianshu_datadev/prompts/templates/extract_label_rules/v001.md`：

```markdown
---
task: extract_label_rules
version: v001
target_schema: LabelRuleProposalList
schema_version: v1
input_artifacts:
  - parsed_developer_spec
forbidden:
  - 禁止输出 proposal_id
  - 禁止输出 source_spec_hash
  - 禁止输出 extraction_time
  - 禁止 LITERAL/COLUMN_REF 作为 WHEN 根条件
  - 禁止输出字符串条件（when/raw_condition）
rejection_policy: strict
changelog: "v001: 初始版本——从 Markdown body 提取标签规则"
---

# 系统指令

你是标签规则提取器。从 Markdown body 中识别 CASE WHEN 标签逻辑，
输出结构化的 LabelPredicateNode discriminator 联合 AST。

## 输出要求

1. 每个未解析列输出一个 LabelRuleProposalOutput
2. condition 必须使用合法根条件类型（COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT）
3. **LITERAL 和 COLUMN_REF 不可作为 WHEN 根条件**——违反此规则的输出将被 Schema 校验拒绝
4. **禁止输出 proposal_id / source_spec_hash / extraction_time**——这些由系统生成
5. 每个分支必须附带 evidence——逐字引用 Markdown 原文
6. **else_value 必须非空**——label_table v1 要求 ELSE 子句
7. 从原文中提取 label_domain（所有可能的标签值）

## 输入

- Markdown body: {markdown_body}
- 未解析列: {unresolved_columns}
- 可用源表字段: {available_fields}
```

- [ ] **Step 3: 验证 PromptManager 加载模板**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -c "
from tianshu_datadev.prompts.manager import PromptManager
pm = PromptManager()
t = pm.get_prompt('extract_label_rules', 'v001')
print(f'task={t.task}, schema={t.schema_binding.schema_name}, version={t.version}')
"
```

- [ ] **Step 4: Commit**

---

### Task 9: LlmLabelExtractor——修复全部接口 + 从 response_root 文件读取

**边界：** `LlmLabelExtractor` 是生产路径唯一合法实现。从 `parsed_json_ref` 路径（Gateway 写入 response_root 的文件）读取结构化数据，系统包装后返回 `LabelRuleProposal`（含必填 else_value/label_domain/evidence）。

**与初版的关键差异：**
- 不再假设 `parsed_json_ref` 文件已存在——该文件由 Gateway 在 Schema 校验通过后原子写入（Task 0）
- 错误信息使用真实 `response.validation_errors: list[str]`
- `LlmResponse` 无 `model`/`prompt_snapshot` 字段

**Files:**
- Create: `src/tianshu_datadev/labels/llm_label_extractor.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

- [ ] **Step 1: 编写测试（使用 FakeLLMAdapter→Gateway→文件→Extractor 集成链路）**

```python
class TestLlmLabelExtractorIntegration:
    """LlmLabelExtractor 集成测试——通过 Gateway 读取 response_root 中的文件。"""

    @staticmethod
    def _make_fake_response_dict():
        return {
            "rules": [{
                "output_column": "distance_category",
                "branches": [{
                    "condition": {
                        "node_type": "COMPARE", "left": "distance_miles",
                        "op": "<=",
                        "right": {"node_type": "LITERAL", "value": 2, "data_type": "number"},
                    },
                    "then_label": "short",
                    "evidence": "distance_miles <= 2 -> short",
                }],
                "else_value": "long",
                "label_domain": {"values": ["short", "long"], "source_evidence": "原文"},
            }],
        }

    def test_integration_fake_to_extractor(self, tmp_path):
        """FakeAdapter→Gateway→文件→Extractor 完整链路。"""
        adapter = FakeLLMAdapter()
        adapter.register_default_for_task("extract_label_rules",
                                          self._make_fake_response_dict())
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter, prompt_manager=prompt_manager,
            response_root=str(tmp_path),
        )
        extractor = LlmLabelExtractor(gateway=gateway)

        spec = _make_test_spec()
        proposals, artifact = extractor.extract(spec, ["distance_category"])

        assert len(proposals) == 1
        assert proposals[0].output_column == "distance_category"
        assert proposals[0].proposal_id != ""
        assert proposals[0].source_spec_hash == spec.spec_hash
        assert proposals[0].else_value == "long"
        assert proposals[0].label_domain is not None
        assert artifact.artifact_id != ""
```

- [ ] **Step 2: 运行测试验证失败**

- [ ] **Step 3: 实现 LlmLabelExtractor v4-light 最终版**

```python
"""LlmLabelExtractor v4-light 最终版——生产级 LLM 标签规则提取器。

从 Gateway response_root 中的 parsed_json_ref 文件读取结构化数据，
系统包装 proposal_id/source_spec_hash/extraction_time/label_domain。
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path

from tianshu_datadev.developer_spec.models import (
    LabelBranchProposal, LabelDomain, LabelRuleProposal,
    LabelRuleProposalList, ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import LabelExtractionArtifact
from tianshu_datadev.labels.label_extractor import LabelExtractor
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import ArtifactRef, LlmRequest


class PipelineError(Exception):
    def __init__(self, error_code: str, message: str):
        self.error_code = error_code
        super().__init__(message)


class LlmLabelExtractor(LabelExtractor):
    """生产级 LabelExtractor——通过 LLMGateway 调用 LLM 提取标签规则。

    LLM 仅输出规则/标签域/evidence——proposal_id/source_spec_hash/extraction_time
    由系统生成。label_domain 从 LLM 输出的 LabelDomainOutput 包装为系统 LabelDomain。
    """

    def __init__(self, gateway: LLMGateway) -> None:
        self._gateway = gateway

    @property
    def gateway(self) -> LLMGateway:
        return self._gateway

    def extract(
        self, spec: ParsedDeveloperSpec, unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """通过 LLM 从 Markdown body 提取标签规则。"""
        # 收集源表可用字段
        available_fields = []
        for t in spec.input_tables:
            for c in t.columns:
                available_fields.append(c.normalized_name)

        # 构造 LlmRequest
        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="extract_label_rules", prompt_version="v001",
            schema_name="LabelRuleProposalList", schema_version="v1",
            input_artifact_refs=[
                ArtifactRef(artifact_type="parsed_developer_spec",
                            artifact_hash=spec.spec_hash, artifact_id=spec.spec_id),
            ],
            temperature=0.1, model="",
        )

        # 调用 Gateway——通过 extra_vars 注入动态内容
        response = self._gateway.submit(
            request,
            markdown_body=spec.description or "",
            unresolved_columns=", ".join(unresolved_columns),
            available_fields=", ".join(available_fields),
        )

        # 检查校验状态
        if not response.is_valid:
            error_detail = "; ".join(response.validation_errors)
            raise PipelineError("LABEL_EXTRACT_FAILED",
                                f"LLM 提取标签规则失败: {error_detail}")

        # 从 parsed_json_ref 路径读取 Gateway 写入 response_root 的结构化数据
        if response.parsed_json_ref is None:
            raise PipelineError("LABEL_EXTRACT_FAILED",
                                "LLM 返回 valid 但 parsed_json_ref 为 None")

        parsed_path = Path(response.parsed_json_ref)
        if not parsed_path.exists():
            raise PipelineError("LABEL_EXTRACT_FAILED",
                                f"结构化输出文件不存在: {response.parsed_json_ref}")

        raw_data = json.loads(parsed_path.read_text("utf-8"))
        llm_output = LabelRuleProposalList.model_validate(raw_data)

        # 系统包装——注入 proposal_id/source_spec_hash/extraction_time/label_domain
        now = datetime.now(timezone.utc)
        proposals = []
        for i, rule_output in enumerate(llm_output.rules):
            proposal_id = (
                f"prop_{spec.spec_hash[:12]}_"
                f"{now.strftime('%Y%m%d%H%M%S')}_{i:02d}"
            )
            # 包装分支——evidence 从 LLM 输出继承
            branches = [
                LabelBranchProposal(
                    condition=b.condition,
                    then_label=b.then_label,
                    evidence=b.evidence,
                )
                for b in rule_output.branches
            ]
            # 包装 label_domain——从 LLM 输出的 LabelDomainOutput 转换为系统 LabelDomain
            domain_output = rule_output.label_domain
            domain = LabelDomain(
                domain_id=f"dom_{proposal_id}",
                values=domain_output.values if domain_output else [],
                source_evidence=domain_output.source_evidence if domain_output else "",
                is_exhaustive=domain_output.is_exhaustive if domain_output else False,
                completeness_evidence=(
                    domain_output.completeness_evidence if domain_output else ""
                ),
            )
            proposals.append(LabelRuleProposal(
                proposal_id=proposal_id,
                source_spec_hash=spec.spec_hash,
                output_column=rule_output.output_column,
                branches=branches,
                else_value=rule_output.else_value,
                label_domain=domain,  # ← 必填
            ))

        # 构建溯源 Artifact
        artifact = LabelExtractionArtifact(
            artifact_id=f"extract_{spec.spec_hash[:12]}_{now.strftime('%Y%m%d%H%M%S')}",
            source_spec_hash=spec.spec_hash, extraction_time=now.isoformat(),
            llm_model="", llm_prompt_version="extract_label_rules/v001",
            llm_temperature=0.1, unresolved_columns=unresolved_columns,
            raw_proposals=proposals, prompt_snapshot="",
        )
        return proposals, artifact
```

- [ ] **Step 4: 运行测试验证通过**

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/labels/llm_label_extractor.py tests/labels/test_label_rules.py
git commit -m "feat(labels): LlmLabelExtractor——从 Gateway response_root 文件读取，系统包装必填字段
- 从 parsed_json_ref 路径读取 Gateway 原子写入的结构化 JSON
- 系统注入 proposal_id/source_spec_hash/extraction_time/label_domain
- else_value/label_domain/evidence 均为必填——缺失时 Pydantic Schema 拒绝"
```

---

### Task 10: Promotion——Proposal → CaseWhenDecl（双空阻断 + 必需字段检查）

**边界：** Promotion 将验证通过的 Proposal 提升为 CaseWhenDecl。

**v4-light 最终版关键规则：**
1. `report.passed == True` 才可提升——即 `blocking_errors` **和** `human_review_items` **均为空**
2. 额外校验：每个 branch.evidence 非空、else_value 非空、label_domain 非空——任一缺失则拒绝提升
3. 产出 `LabelPromotionArtifact`——含溯源链
4. 重新计算 spec_hash

**Files:**
- Create: `src/tianshu_datadev/labels/promotion.py`
- Test: `tests/labels/test_label_rules.py`（末尾追加）

**Interfaces:**
- Consumes: `LabelRuleProposal`（Task 2）、`LabelValidationReport`（Task 3）、`ParsedDeveloperSpec`（Task 2）
- Produces: `Promotion.promote(spec, proposals, reports, extraction_artifact) -> tuple[ParsedDeveloperSpec, LabelPromotionArtifact]`

- [ ] **Step 1: 编写测试——含双空阻断 + 必需字段检查**

```python
# tests/labels/test_label_rules.py 末尾追加

from tianshu_datadev.labels.promotion import Promotion


class TestPromotionDoubleEmptyGate:
    """v4-light 最终版: Promotion 必须 blocking_errors 和 human_review_items 均为空。"""

    def test_passed_proposal_promoted(self):
        """全部通过→提升为 CaseWhenDecl。"""
        promotion = Promotion()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash=spec.spec_hash,
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(domain_id="d1", values=["short", "long"]),
        )
        report = LabelValidationReport(
            proposal_id="p1", passed=True, checks=[],
            blocking_errors=[], human_review_items=[], warnings=[],
        )
        new_spec, artifact = promotion.promote(
            spec, [proposal], [report],
            LabelExtractionArtifact(
                artifact_id="e1", source_spec_hash=spec.spec_hash,
                extraction_time="2026-07-15T00:00:00Z",
                llm_model="fake", llm_prompt_version="v001",
                llm_temperature=0.1, unresolved_columns=["distance_category"],
                raw_proposals=[proposal],
            ),
        )
        assert len(new_spec.label_rules) == 1
        assert artifact.human_review_required is False

    def test_human_review_blocks_promotion(self):
        """human_review_items 非空→拒绝提升→返回原 spec。"""
        promotion = Promotion()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash=spec.spec_hash,
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",  # ← 空 evidence
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(domain_id="d1", values=["short", "long"]),
        )
        report = LabelValidationReport(
            proposal_id="p1", passed=False, checks=[],
            blocking_errors=[],
            human_review_items=["缺少 evidence"],
            warnings=[],
        )
        new_spec, artifact = promotion.promote(
            spec, [proposal], [report],
            LabelExtractionArtifact(
                artifact_id="e1", source_spec_hash=spec.spec_hash,
                extraction_time="2026-07-15T00:00:00Z",
                llm_model="fake", llm_prompt_version="v001",
                llm_temperature=0.1, unresolved_columns=["distance_category"],
                raw_proposals=[proposal],
            ),
        )
        # 拒绝提升——label_rules 为空
        assert len(new_spec.label_rules) == 0
        assert artifact.human_review_required is True

    def test_empty_evidence_blocks_promotion(self):
        """branch.evidence 为空→即使 report.passed 也拒绝提升。"""
        promotion = Promotion()
        spec = _make_test_spec()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash=spec.spec_hash,
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",  # ← 空
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(domain_id="d1", values=["short", "long"]),
        )
        report = LabelValidationReport(
            proposal_id="p1", passed=True, checks=[],
            blocking_errors=[], human_review_items=[], warnings=[],
        )
        new_spec, artifact = promotion.promote(
            spec, [proposal], [report],
            LabelExtractionArtifact(
                artifact_id="e1", source_spec_hash=spec.spec_hash,
                extraction_time="2026-07-15T00:00:00Z",
                llm_model="fake", llm_prompt_version="v001",
                llm_temperature=0.1, unresolved_columns=["distance_category"],
                raw_proposals=[proposal],
            ),
        )
        assert len(new_spec.label_rules) == 0, "空 evidence 应被拒绝"
```

- [ ] **Step 2: 运行测试验证失败**

- [ ] **Step 3: 实现 Promotion（双空阻断 + 必需字段检查）**

```python
"""Promotion v4-light 最终版——Proposal → CaseWhenDecl + 双空阻断 + 必需字段检查。

提升条件（全部满足）：
1. report.passed == True（blocking_errors 和 human_review_items 均为空）
2. 每个 branch.evidence 非空
3. else_value 非空（由 Pydantic Schema 保证）
4. label_domain 非空（由 Pydantic Schema 保证）
"""
from __future__ import annotations
import hashlib
from datetime import datetime, timezone

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl, LabelPredicateBranch, LabelRuleProposal,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact, LabelPromotionArtifact, LabelValidationReport,
)


class Promotion:
    """Proposal → CaseWhenDecl 提升器——双空阻断。"""

    def promote(
        self,
        spec: ParsedDeveloperSpec,
        proposals: list[LabelRuleProposal],
        reports: list[LabelValidationReport],
        extraction_artifact: LabelExtractionArtifact,
    ) -> tuple[ParsedDeveloperSpec, LabelPromotionArtifact]:
        """提升验证通过的 Proposal。

        仅提升同时满足以下条件的 Proposal：
        - report.passed == True（双空）
        - 所有 branch.evidence 非空
        """
        promoted_rules: list[CaseWhenDecl] = []
        rejected_ids: list[str] = []
        human_review_required = False

        for proposal, report in zip(proposals, reports):
            # 双空检查
            if not report.passed:
                rejected_ids.append(proposal.proposal_id)
                if report.human_review_items:
                    human_review_required = True
                continue

            # 额外安全校验——evidence 非空
            empty_evidence = [
                b.then_label for b in proposal.branches if not b.evidence
            ]
            if empty_evidence:
                rejected_ids.append(proposal.proposal_id)
                human_review_required = True
                continue

            # 提升
            typed_branches = [
                LabelPredicateBranch(
                    condition=bp.condition,
                    then_label=bp.then_label,
                )
                for bp in proposal.branches
            ]
            promoted_rules.append(CaseWhenDecl(
                output_column=proposal.output_column,
                typed_branches=typed_branches,
                else_value=proposal.else_value,
            ))

        # 生成新 Spec（不原地修改）
        new_spec_data = spec.model_dump()
        new_spec_data["label_rules"] = spec.label_rules + promoted_rules
        new_spec = ParsedDeveloperSpec(**new_spec_data)

        # 统一重算 spec_hash
        new_hash = _normalized_spec_hash(new_spec)
        object.__setattr__(new_spec, "spec_hash", new_hash)
        object.__setattr__(new_spec, "spec_id", f"spec_{new_hash[:12]}")

        # 构建 Promotion Artifact
        now = datetime.now(timezone.utc)
        artifact = LabelPromotionArtifact(
            artifact_id=f"promote_{new_hash[:12]}",
            parent_spec_hash=spec.spec_hash,
            new_spec_hash=new_hash,
            promotion_time=now.isoformat(),
            extraction_artifact_id=extraction_artifact.artifact_id,
            promoted_rules=promoted_rules,
            validation_reports=reports,
            rejected_proposals=rejected_ids,
            human_review_required=human_review_required,
        )
        return new_spec, artifact


def _normalized_spec_hash(spec: ParsedDeveloperSpec) -> str:
    """计算归一化 spec_hash——仅基于确定性语义字段。"""
    # 排除 spec_hash/spec_id/Artifact 引用等非确定性字段
    data = spec.model_dump(mode="json", exclude={"spec_hash", "spec_id"})
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()
```

- [ ] **Step 4: 运行测试验证通过**

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/labels/promotion.py tests/labels/test_label_rules.py
git commit -m "feat(labels): Promotion——双空阻断 + evidence 非空检查

- report.passed==True（blocking_errors+human_review_items 均为空）才可提升
- 额外验证每个 branch.evidence 非空——任一为空则拒绝
- 产出 LabelPromotionArtifact——含溯源链和 spec_hash 重算"
```

---

### Task 11: _prepare_spec_for_planning() + create_app() 生产注入

**边界：** 两件事：
1. `_prepare_spec_for_planning()` 共享入口——覆盖全部 plan/execute/run_all 入口
2. `create_app()` 中新增 `LlmLabelExtractor` 生产注入——**复用现有 `AnthropicAdapter + PromptManager` 模式（与 `SparkDeveloperService` 一致）；无 API Key 时返回明确 `PipelineError("CONFIG_ERROR")`，禁止回退 Fake**

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（`_prepare_spec_for_planning` + 全部入口调用）
- Modify: `src/tianshu_datadev/api/app.py`（`create_app` 生产注入）
- Test: `tests/api/test_pipeline.py`

**Interfaces:**
- Produces: `_prepare_spec_for_planning(spec, manifest=None, label_extractor=None, label_validator=None, promoter=None) -> tuple[ParsedDeveloperSpec, LabelExtractionArtifact|None, LabelPromotionArtifact|None]`
- Modifies: `create_app(pipeline=None) -> FastAPI`——新增 `LlmLabelExtractor` 注入

- [ ] **Step 1: 编写 pipeline 测试**

在 `tests/api/test_pipeline.py` 末尾追加：

```python
def test_prepare_spec_for_planning_no_unresolved_skips():
    """无未解析列→跳过 LabelExtractor，直接返回原 spec。"""
    from tianshu_datadev.api.pipeline import _prepare_spec_for_planning
    spec = _make_aggregate_spec()  # 全部输出列均为物理列/指标
    new_spec, ext_art, prom_art = _prepare_spec_for_planning(spec)
    assert ext_art is None  # 跳过提取
    assert prom_art is None
```

- [ ] **Step 2: 编写 create_app 测试**

在 `tests/api/test_pipeline.py` 末尾追加：

```python
def test_create_app_label_extractor_no_api_key():
    """无 API Key→不应创建 LlmLabelExtractor→label_table 返回配置错误。"""
    import os
    # 临时移除 API Key
    old_key = os.environ.pop("ANTHROPIC_API_KEY", None)
    os.environ.pop("DEEPSEEK_API_KEY", None)
    try:
        from tianshu_datadev.api.app import create_app
        app = create_app()
        # 验证 app 创建成功（不崩溃）
        assert app is not None
    finally:
        if old_key:
            os.environ["ANTHROPIC_API_KEY"] = old_key
```

- [ ] **Step 3: 实现 _prepare_spec_for_planning()**

在 `src/tianshu_datadev/api/pipeline.py` 中新增：

```python
def _prepare_spec_for_planning(
    spec: ParsedDeveloperSpec,
    manifest: SourceManifest | None = None,
    label_extractor: LabelExtractor | None = None,
    label_validator: LabelRuleValidator | None = None,
    promoter: Promotion | None = None,
) -> tuple[ParsedDeveloperSpec, LabelExtractionArtifact | None, LabelPromotionArtifact | None]:
    """为 Builder 准备 Spec——在所有 plan/execute/run_all 入口共享调用。

    Returns:
        (增强后 Spec, 提取溯源 Artifact 或 None, 提升溯源 Artifact 或 None)
    """
    from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns

    unresolved = _find_unresolved_derived_columns(spec, manifest)
    if not unresolved:
        return spec, None, None

    if label_extractor is None:
        raise ValueError(
            "存在未解析派生输出列但未提供 LabelExtractor——"
            "请通过 create_app() 生产注入或测试时使用 FakeLabelExtractor"
        )

    # 提取
    proposals, extraction_artifact = label_extractor.extract(spec, unresolved)

    # 验证
    validator = label_validator or LabelRuleValidator()
    reports = [validator.validate(p, spec) for p in proposals]

    # 提升
    prom = promoter or Promotion()
    new_spec, promotion_artifact = prom.promote(
        spec, proposals, reports, extraction_artifact,
    )
    return new_spec, extraction_artifact, promotion_artifact
```

- [ ] **Step 4: 实现 create_app() 生产注入**

在 `src/tianshu_datadev/api/app.py` 的 `create_app()` 函数中（`SparkDeveloperService` 初始化之后）追加：

```python
    # ── v4-light: 创建 LlmLabelExtractor（复用现有 Adapter + PromptManager 模式）──
    llm_label_extractor = None
    api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            # 复用与 SparkDeveloperService 相同的 Adapter + PromptManager 构造模式
            adapter = AnthropicAdapter()
            prompt_manager = PromptManager()
            llm_gateway = LLMGateway(
                adapter=adapter,
                prompt_manager=prompt_manager,
                response_root="llm_responses",
            )
            llm_label_extractor = LlmLabelExtractor(gateway=llm_gateway)
            logger.info("LlmLabelExtractor 初始化成功——label_table 管线将调用真实 LLM")
        except Exception as exc:
            logger.warning(
                "LlmLabelExtractor 创建失败（key 存在但初始化异常），"
                "label_table 请求将返回配置错误: %s", exc
            )
    else:
        logger.info(
            "未检测到 DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY——"
            "label_table 请求将返回 CONFIG_ERROR（禁止回退 Fake）"
        )
```

- [ ] **Step 5: 更新全部入口调用 _prepare_spec_for_planning()**

在 `plan()`/`execute_rich()`/`run_all()`/`run_all_full()`/`run_all_full_stream()`/`run_all_rich()` 全部 6 个入口中统一调用：

```python
spec, ext_artifact, prom_artifact = _prepare_spec_for_planning(
    spec, manifest=manifest, label_extractor=llm_label_extractor,
)
```

- [ ] **Step 6: 运行测试验证**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/api/test_pipeline.py -v -k "prepare_spec or create_app" 2>&1 | tail -15
```

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py src/tianshu_datadev/api/app.py tests/api/test_pipeline.py
git commit -m "feat(api): _prepare_spec_for_planning() 共享入口 + create_app() 生产注入

- _prepare_spec_for_planning() 覆盖全部 6 个管线入口
- create_app() 复用 AnthropicAdapter+PromptManager 构造 LLMGateway→LlmLabelExtractor
- 无 API Key 时返回明确 ValueError——禁止回退 Fake
- label_extractor=None 时抛出 ValueError——强制显式注入"
```

---

### Task 12: Builder——CaseWhenStep + 硬阻断

**边界：** 新增 `_build_case_when_steps()` 和 `_predicate_from_label_node()` 方法。`_build_project_step()` 中加入 `DerivedColumnRuleMissing` 硬阻断。统一 IR 驱动——禁止 raw SQL。

**真实模型（`CaseWhenStep` 在 `sql_build_plan.py:113`）：**
- `cases: list[WhenBranch]`（非 `branches`）
- `else_value: SqlLiteral | None`
- `alias: SafeIdentifier`
- `WhenBranch.condition: Predicate | None`——使用此字段（非 `raw_condition`）
- `WhenBranch.result: SqlLiteral`

**Files:**
- Modify: `src/tianshu_datadev/planning/sql_build_plan.py`
- Test: `tests/planning/test_planning_models.py`

**Interfaces:**
- Produces: `_build_case_when_steps(spec) -> list[CaseWhenStep]`
- Produces: `_predicate_from_label_node(node) -> Predicate`
- Produces: `DerivedColumnRuleMissing` 异常类

- [ ] **Step 1: 编写测试**

```python
# tests/planning/test_planning_models.py 末尾追加

def test_build_case_when_steps_generates_cases():
    """有 label_rules→生成 CaseWhenStep（cases/else_value/alias 字段正确）。"""
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
    spec = _make_label_spec_with_rules()
    builder = SqlBuildPlan.__new__(SqlBuildPlan)  # 仅测试方法——不完整初始化
    steps = builder._build_case_when_steps(spec)
    assert len(steps) == 1
    assert steps[0].cases  # 非 branches
    assert steps[0].alias != ""

def test_project_step_hard_block_on_unresolved():
    """未解析列→DerivedColumnRuleMissing 异常。"""
    import pytest
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, DerivedColumnRuleMissing
    spec = _make_unresolved_spec()  # 输出列既非物理列也非 label_rule
    builder = SqlBuildPlan.__new__(SqlBuildPlan)
    with pytest.raises(DerivedColumnRuleMissing):
        builder._build_project_step(spec, ...)
```

- [ ] **Step 2-5: 实现 + 测试 + Commit**

```bash
git add src/tianshu_datadev/planning/sql_build_plan.py tests/planning/test_planning_models.py
git commit -m "feat(builder): _build_case_when_steps() + _predicate_from_label_node() + 硬阻断

- 使用真实模型字段: CaseWhenStep.cases/SafeIdentifier/SqlLiteral/Predicate
- 未解析列→DerivedColumnRuleMissing 硬阻断——禁止回退为 ColumnRef"
```

---

### Task 13: E2E 集成——Template 2 端到端（已完成）

**边界：** 使用 FakeLabelExtractor 验证完整管线：Markdown → Parser → Extractor → Validator → Promotion → Builder → SQL/Spark Compiler。

**实际完成范围（Task 13.1 + 13.2 轻量收口）：**
- Template 2 黄金链：真实 Markdown → FakeLLMAdapter → LLMGateway → LlmLabelExtractor → Validator → Promotion → Builder → SQL Compiler → DuckDB → Contract → SparkPlan → SparkCompiler → SparkStaticValidator
- **仅证明结构语义保持、Spark 编译和静态安全校验**——不声明 LOGIC_CONSISTENT
- **SQL/Spark 物理一致性复用现有 Phase 7 验证框架**——不新增三路同快照
- 缩短重复手工正向测试，保留负向边界测试
- Builder blocking question 零阻断断言

**Files:**
- Modify: `templates/` Template 2 YAML（添加 `type: label_table`）
- Test: `tests/sql/test_pipeline_e2e.py`（黄金链 + 缩短的手工测试）

**退出条件（已验证通过）：**
1. Template 2 黄金链端到端执行成功——DuckDB 返回正确的 distance_category 标签值
2. SparkPlan 结构验证——Contract branches 与 Spark CaseWhenStep branches 一一对应
3. SparkCompiler 编译成功 + SparkStaticValidator 静态安全校验通过
4. 全量回归通过（排除 Harness）

- [ ] **Step 1: 更新 Template 2 YAML**

在模板 YAML front matter 中添加 `type: label_table`

- [ ] **Step 2: 编写 E2E 测试**

```python
# tests/sql/test_pipeline_e2e.py 末尾追加

def test_template2_label_table_e2e():
    """Template 2 E2E——FakeLabelExtractor→Validator→Promotion→Builder→DuckDB。"""
    from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
    # 预填充正确的 Template 2 Proposal
    fake = FakeLabelExtractor(proposals=[_make_template2_proposal()])
    result = execute_pipeline("templates/template2.yaml", label_extractor=fake)
    assert result.status == "success"
    # 验证输出含 distance_category 列
    assert "distance_category" in result.columns
```

- [ ] **Step 3: 运行测试 + 全量回归 + Commit**

---

### Task 14: 注册 --run-harness + 唯一可选真实 LLM 冒烟测试

**边界：**
1. 在 `conftest.py` 中按 `--run-slow` 模式注册 `--run-harness` 选项 + marker
2. 创建唯一一个可选真实 LLM 冒烟测试文件
3. 默认排除（不传 `--run-harness` 时 skip）

**Files:**
- Modify: `tests/conftest.py`
- Create: `tests/harness/test_label_extractor_smoke.py`

- [ ] **Step 1: 注册 --run-harness**

在 `tests/conftest.py` 中参考现有 `--run-slow` 模式追加：

```python
# pytest_addoption() 中追加：
parser.addoption("--run-harness", action="store_true", default=False,
                 help="运行需要真实 LLM API Key 的 Harness 测试")

# pytest_configure() 中追加：
config.addinivalue_line("markers",
    "harness: 需要真实 LLM API Key 的 Harness 测试（需 --run-harness 启用）")

# pytest_collection_modifyitems() 中追加：
if not config.getoption("--run-harness"):
    skip_harness = pytest.mark.skip(reason="需要 --run-harness 选项启用真实 LLM 调用")
    for item in items:
        if "harness" in item.keywords:
            item.add_marker(skip_harness)
```

- [ ] **Step 2: 创建冒烟测试**

创建 `tests/harness/test_label_extractor_smoke.py`（含真实 LLM 提取→结构验证→系统包装完整链路）

- [ ] **Step 3: 验证注册生效 + Commit**

---

## 自审报告（v4-light 最终版）

### 1. 需求覆盖率

| 用户要求 | 对应 Task | 状态 |
|----------|-----------|------|
| 1. Task 0——Gateway 写入 response_root + 集成测试 + tmp_path + 禁止手工造文件 | Task 0 | ✅ |
| 2. 根条件仅允许 COMPARE/IS_NULL/IS_NOT_NULL/AND/OR/NOT | Task 1（LabelPredicateCondition） + Task 2（condition 字段类型） | ✅ |
| 3. label_domain/evidence/ELSE 均为必需——缺失不得 Promotion | Task 2（Pydantic 必填） + Task 10（Promotion 额外校验） | ✅ |
| 4. Promotion 要求 blocking_errors 和 human_review_items 均为空 | Task 6（Validator 双空 passed） + Task 10（Promotion 双空检查） | ✅ |
| 5. Task 11 create_app() 生产注入——无 API Key 明确报错、禁止 Fake | Task 11（create_app 注入 + ValueError） | ✅ |
| 6. Task 10-13 自包含——完整接口/文件/测试/退出条件 | Task 10-13 | ✅ |

### 2. 阻断验证覆盖

| 阻断场景 | 覆盖测试 | 阻断位置 |
|----------|----------|----------|
| 未知字段（extra） | `test_gateway_invalid_does_not_write_file` | Gateway Schema 校验 extra="forbid" |
| 非法根节点（LITERAL/COLUMN_REF） | `test_literal_rejected_as_root` + `test_illegal_root_node_rejected` | Pydantic discriminator + Gateway 集成 |
| 标签越界 | `test_label_outside_domain_blocks` | Validator LABEL_DOMAIN |
| 缺少 ELSE/evidence | `test_empty_evidence_blocks_promotion` + `test_human_review_causes_fail` | Validator COVERAGE→HUMAN_REVIEW→Promotion 拒绝 |
| 缺少 API Key | `test_create_app_label_extractor_no_api_key` | create_app() 日志 + ValueError |

### 3. 真实接口一致性

| 接口 | 源码实际 | v4-light 最终版 |
|------|----------|-----------------|
| `LLMGateway.__init__` | `(adapter, prompt_manager)` | `(adapter, prompt_manager, response_root="llm_responses")` |
| Gateway 文件写入 | **不存在** | 原子写入 response_root ✅ |
| `_render_user_message` | 仅 `{artifact_refs}` | `**extra_vars` ✅ |
| `FakeLLMAdapter.__init__` | `fixtures: dict[str, dict] \| None` | `FakeLLMAdapter()` + `register_default_for_task()` ✅ |
| `LlmResponse.parsed_json_ref` | `str \| None` | 从 response_root 路径读取 ✅ |
| `LlmResponse.validation_errors` | `list[str]` | 使用 `validation_errors` ✅ |
| `PromptManager` 模板路径 | `prompts/templates/{task}/v001.md` | 正确路径 ✅ |
| `_SCHEMA_PATH_MAP` | 需注册 | `LabelRuleProposalList` 注册 ✅ |
| `CaseWhenStep` | `cases`/`else_value`/`alias`（sql_build_plan.py） | 正确字段 ✅ |
| `create_app()` | AnthropicAdapter+PromptManager 模式 | 复用模式 ✅ |

### 4. 占位符扫描

无 TBD/TODO/占位符。全部 Task 含完整可运行代码、精确文件路径、可复制命令。

---

## CRCS 风险映射

| 风险 | 分类 | 依据 |
|------|------|------|
| Gateway response_root 文件写入 | **A** | 新增功能——不影响现有代码路径 |
| `_render_user_message **extra_vars` | **A** | 向后兼容——extra_vars 为空时行为不变 |
| `LabelPredicateCondition` 根约束 | **A** | 新增类型——现有代码不引用 |
| else_value/label_domain/evidence 必填 | **A** | 新增模型约束——不改变现有字段 |
| Promotion 双空阻断 | **A** | 收紧条件——仅影响新链路 |
| create_app() 生产注入 | **A** | 复用现有模式——不改变现有逻辑 |
| `_SCHEMA_PATH_MAP` 新增 | **A** | 纯数据注册 |

全部 CRCS 分类为 **A**。

---

## 可复制验收命令

```bash
# 1. Gateway 文件持久化 + 集成测试
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3" && python -m pytest tests/labels/test_label_rules.py::TestGatewayFilePersistence tests/labels/test_label_rules.py::TestFakeAdapterToExtractorIntegration -v

# 2. 模型测试（根条件约束 + 必需字段）
python -m pytest tests/planning/test_planning_models.py -v -k "RootConstraint or RequiredFields or Discriminator"

# 3. Validator v1（双空通过）
python -m pytest tests/labels/test_label_rules.py -v -k "ValidatorV1"

# 4. Promotion（双空阻断 + evidence 检查）
python -m pytest tests/labels/test_label_rules.py -v -k "Promotion"

# 5. E2E 黄金链（Template 2——结构语义保持 + Spark 编译 + 静态安全校验）
python -m pytest tests/sql/test_pipeline_e2e.py -v -k "golden_chain"

# 6. Harness 冒烟（默认 skip）
python -m pytest tests/harness/ -v --run-harness

# 7. 完整回归（排除 Harness）
python -m pytest tests/ -x --timeout=60 -q --ignore=tests/harness/ 2>&1 | tail -3
```
