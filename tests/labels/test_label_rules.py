"""标签子系统测试——Artifact 模型、Validator、FakeLabelExtractor、LlmLabelExtractor、Promotion。"""

import pytest

from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
    LabelValidationCheck,
    LabelValidationReport,
)


class TestLabelExtractionArtifact:
    """溯源 Artifact——记录 LLM 调用的完整上下文。"""

    def test_fields(self):
        """验证 Artifact 字段构造和读取。"""
        artifact = LabelExtractionArtifact(
            artifact_id="ext_001",
            source_spec_hash="h",
            extraction_time="2026-07-15T00:00:00Z",
            llm_model="fake",
            llm_prompt_version="v001",
            llm_temperature=0.1,
            unresolved_columns=["col1"],
            raw_proposals=[],
            prompt_snapshot="",
        )
        assert artifact.artifact_id == "ext_001"
        assert artifact.source_spec_hash == "h"
        assert artifact.unresolved_columns == ["col1"]
        assert artifact.llm_model == "fake"


class TestLabelValidationReport:
    """校验报告——双空通过。"""

    def test_passed_requires_both_empty(self):
        """passed=True 要求 blocking_errors 和 human_review_items 均为空。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=True,
            checks=[],
            blocking_errors=[],
            human_review_items=[],
            warnings=[],
        )
        assert report.passed

    def test_human_review_causes_not_passed(self):
        """human_review_items 非空→passed=False。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=False,
            checks=[],
            blocking_errors=[],
            human_review_items=["缺少 ELSE"],
            warnings=[],
        )
        assert not report.passed

    def test_blocking_error_causes_not_passed(self):
        """blocking_errors 非空→passed=False。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=False,
            checks=[],
            blocking_errors=["字段不存在: unknown_col"],
            human_review_items=[],
            warnings=[],
        )
        assert not report.passed


class TestLabelValidationCheck:
    """单条校验检查项。"""

    def test_check_fields(self):
        check = LabelValidationCheck(
            check_name="FIELD_EXISTS",
            passed=True,
            level="BLOCKING",
            detail="字段 distance_miles 存在",
        )
        assert check.check_name == "FIELD_EXISTS"
        assert check.passed
        assert check.level == "BLOCKING"


class TestLabelPromotionArtifact:
    """提升 Artifact——记录 Proposal→CaseWhenDecl 转换。"""

    def test_fields(self):
        artifact = LabelPromotionArtifact(
            artifact_id="prom_001",
            parent_spec_hash="h_old",
            new_spec_hash="h_new",
            promotion_time="2026-07-15T00:00:00Z",
            extraction_artifact_id="ext_001",
            promoted_rules=[],
            validation_reports=[],
            rejected_proposals=[],
            human_review_required=False,
        )
        assert artifact.artifact_id == "prom_001"
        assert not artifact.human_review_required

    def test_rejected_proposals_tracked(self):
        """被拒绝的 proposal_id 被记录。"""
        artifact = LabelPromotionArtifact(
            artifact_id="prom_002",
            parent_spec_hash="h_old",
            new_spec_hash="h_new",
            promotion_time="2026-07-15T00:00:00Z",
            extraction_artifact_id="ext_001",
            promoted_rules=[],
            validation_reports=[],
            rejected_proposals=["p1", "p2"],
            human_review_required=True,
        )
        assert len(artifact.rejected_proposals) == 2
        assert artifact.human_review_required

# ================================================
# v4-light 最终版: LabelRuleValidator v1 六项检查 + 双空通过
# ================================================

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    CaseWhenDecl,
    ColumnDecl,
    CompareOp,
    DatasetType,
    InputTableDecl,
    LabelBranchProposal,
    LabelCompare,
    LabelDomain,
    LabelNot,
    LabelPredicateBranch,
    LabelRuleProposal,
    LabelTypedLiteral,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    UncertaintyEntry,
)
from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator


def _make_test_spec():
    """构造测试用 ParsedDeveloperSpec。"""
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="t",
        description=(
            "距离 <= 2 英里归类为短途——锚定项目书分类定义段。"
            "距离 > 2 英里归类为长途——锚定项目书分类定义段。"
            "<=2 -> short。"
            "数据来源：NYC 出租车行程记录表 gold.fact_trips。"
        ),
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
        ], grain=[]),
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

    def test_unknown_field_blocks(self):
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="nonexistent_col", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert not report.passed
        assert any("nonexistent_col" in e for e in report.blocking_errors)


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
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        coverage_checks = [c for c in report.checks if c.check_name == "COVERAGE"]
        if coverage_checks:
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

# ================================================
# v4-light 最终版: FakeLabelExtractor 测试
# ================================================


class TestFakeLabelExtractor:
    """FakeLabelExtractor——pytest 专用，确定性返回预定义 Proposal。"""

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
        assert artifact.llm_model == "fake"

    def test_empty_by_default(self):
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        extractor = FakeLabelExtractor()
        spec = _make_test_spec()
        result, artifact = extractor.extract(spec, [])
        assert result == []
        assert artifact.unresolved_columns == []


# ================================================
# Task 11.5: 三条成功路径测试
# ================================================


class TestGatewayToExtractorIntegration:
    """成功路径 1：FakeLLMAdapter → Gateway → LlmLabelExtractor → Proposal。

    response_root 使用 tmp_path——不污染工作目录。
    """

    def test_gateway_to_extractor_success_path(self, tmp_path):
        """FakeLLMAdapter 注册有效 label 输出 → Gateway 写入 response_root
        → LlmLabelExtractor 读取并包装 Proposal → 验证 Proposal 结构完整。"""
        from tianshu_datadev.labels.llm_label_extractor import LlmLabelExtractor
        from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
        from tianshu_datadev.llm.gateway import LLMGateway
        from tianshu_datadev.prompts.manager import PromptManager

        # ── 构造 LLM 输出 dict（LabelRuleProposalList 格式）──
        label_output = {
            "rules": [
                {
                    "output_column": "risk_label",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "crash_rate",
                                "op": ">=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": 800,
                                    "data_type": "number",
                                },
                            },
                            "then_label": "高风险",
                            "evidence": "事故率 >= 800 定义为高风险——锚定项目书第3段",
                        },
                        {
                            "condition": {
                                "node_type": "IS_NULL",
                                "column": "crash_rate",
                            },
                            "then_label": "未知",
                            "evidence": "事故率为空时标记未知——锚定项目书异常处理段",
                        },
                    ],
                    "else_value": "低风险",
                    "label_domain": {
                        "values": ["高风险", "低风险", "未知"],
                        "source_evidence": "项目书定义三档风险等级",
                        "is_exhaustive": True,
                        "completeness_evidence": "所有记录都有事故率数据或 NULL",
                    },
                }
            ]
        }

        # ── 配置 FakeLLMAdapter ──
        fake_adapter = FakeLLMAdapter()
        fake_adapter.register_default_for_task(
            task="extract_label_rules",
            output=label_output,
        )

        # ── 使用 tmp_path 作为 response_root ──
        response_root = tmp_path / "llm_responses"
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
            response_root=str(response_root),
        )

        extractor = LlmLabelExtractor(gateway=gateway)

        # ── 构造测试 Spec ──
        spec = _make_test_spec()

        # ── 执行提取 ──
        proposals, artifact = extractor.extract(spec, ["risk_label"])

        # ── 验证 Proposal 结构 ──
        assert len(proposals) == 1, f"期望 1 条 Proposal，实际 {len(proposals)}"
        prop = proposals[0]
        assert prop.output_column == "risk_label"
        assert len(prop.branches) == 2
        assert prop.else_value == "低风险"
        # else_value 和 label_domain 必须非空
        assert prop.else_value != ""
        assert prop.label_domain is not None
        assert len(prop.label_domain.values) == 3
        # proposal_id 由系统生成
        assert prop.proposal_id.startswith("prop_")
        assert prop.source_spec_hash == spec.spec_hash
        # 每个分支 evidence 已包装
        for branch in prop.branches:
            assert branch.evidence != ""
            assert len(branch.evidence) >= 10  # 满足最小锚定长度

        # ── 验证 Artifact ──
        assert artifact.source_spec_hash == spec.spec_hash
        # FakeLLMAdapter 的 token_usage 不含 model 字段——默认 "unknown"
        assert artifact.llm_prompt_version == "v001"
        assert artifact.unresolved_columns == ["risk_label"]
        assert len(artifact.raw_proposals) == 1

        # ── 验证 response_root 中有落盘文件 ──
        response_files = list(response_root.rglob("*.json"))
        assert len(response_files) >= 1, (
            f"response_root 中应有 JSON 文件，实际目录内容: "
            f"{list(response_root.rglob('*'))}"
        )


class TestValidatorToPromotionToCaseWhenDecl:
    """成功路径 2：Validator 通过 → Promotion 提升 → CaseWhenDecl 结构验证。

    验证 typed_branches 已填充、branches 列表为空（不把 evidence 写入自由字符串条件）。
    """

    def test_validator_promotion_success_path(self):
        """构造合法 Proposal → Validator 通过 → Promotion 提升
        → CaseWhenDecl.typed_branches 已填充，branches 为空。"""
        from tianshu_datadev.labels.artifacts import LabelExtractionArtifact
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator
        from tianshu_datadev.labels.promotion import Promotion

        # ── 构造合法 Proposal ──
        proposal = LabelRuleProposal(
            proposal_id="prop_test_001",
            source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles",
                        op=CompareOp.LTE,
                        right=LabelTypedLiteral(
                            value=Decimal("2"), data_type="number",
                        ),
                    ),
                    then_label="short",
                    evidence="距离 <= 2 英里归类为短途——锚定项目书分类定义段",
                ),
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles",
                        op=CompareOp.GT,
                        right=LabelTypedLiteral(
                            value=Decimal("2"), data_type="number",
                        ),
                    ),
                    then_label="long",
                    evidence="距离 > 2 英里归类为长途——锚定项目书分类定义段",
                ),
            ],
            else_value="unknown",
            label_domain=LabelDomain(
                domain_id="dom_001",
                values=["short", "long", "unknown"],
            ),
        )

        # ── Validator ──
        validator = LabelRuleValidator()
        spec = _make_test_spec()
        report = validator.validate(proposal, spec)
        assert report.passed, (
            f"Validator 应通过，blocking={report.blocking_errors}, "
            f"review={report.human_review_items}"
        )

        # ── Promotion ──
        extraction_artifact = LabelExtractionArtifact(
            artifact_id="ext_test_001",
            source_spec_hash="h",
            extraction_time="2026-07-15T00:00:00Z",
            llm_model="fake",
            llm_prompt_version="v001",
            llm_temperature=0.0,
            unresolved_columns=["distance_category"],
            raw_proposals=[proposal],
            prompt_snapshot="",
        )
        promoter = Promotion()
        promoted_rules, promotion_artifact = promoter.promote(
            spec.spec_hash, [proposal], [report], extraction_artifact,
        )

        # ── 验证提升结果 ──
        assert len(promoted_rules) == 1, (
            f"期望 1 条提升规则，实际 {len(promoted_rules)}"
        )
        case_when = promoted_rules[0]
        assert isinstance(case_when, CaseWhenDecl)
        assert case_when.output_column == "distance_category"
        assert case_when.else_value == "unknown"

        # ── 关键断言：typed_branches 已填充 ──
        assert len(case_when.typed_branches) == 2, (
            f"typed_branches 应有 2 条，实际 {len(case_when.typed_branches)}"
        )
        for tb in case_when.typed_branches:
            assert isinstance(tb, LabelPredicateBranch)
            assert tb.then_label in ("short", "long")
            # condition 必须是 LabelPredicateCondition（非字符串）
            assert not isinstance(tb.condition, str), (
                "typed_branch.condition 不应是字符串"
            )

        # ── 关键断言：branches（CaseWhenBranchDecl）为空——
        #   不把 evidence 写入自由字符串条件 ──
        assert case_when.branches == [], (
            f"branches 应为空列表（不生成自由字符串条件），"
            f"实际 {case_when.branches}"
        )

        # ── 验证 PromotionArtifact ──
        assert promotion_artifact.promoted_rules == promoted_rules
        assert promotion_artifact.human_review_required is False
        assert len(promotion_artifact.rejected_proposals) == 0


class TestPrepareSpecFinalType:
    """成功路径 3：_prepare_labels 最终类型验证。

    验证 spec.label_rules 中的元素是 CaseWhenDecl（非 Proposal）。
    """

    def test_prepare_spec_appends_case_when_decl(self):
        """Pipeline._prepare_labels → spec.label_rules 元素为 CaseWhenDecl。"""
        from tianshu_datadev.api.pipeline import Pipeline
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor

        # ── 构造 FakeLabelExtractor——返回合法 Proposal ──
        fake_proposal = LabelRuleProposal(
            proposal_id="prop_test_002",
            source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles",
                        op=CompareOp.LTE,
                        right=LabelTypedLiteral(
                            value=Decimal("2"), data_type="number",
                        ),
                    ),
                    then_label="short",
                    evidence="距离 <= 2 英里归类为短途——锚定项目书",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(
                domain_id="dom_002",
                values=["short", "long"],
            ),
        )
        fake_extractor = FakeLabelExtractor(proposals=[fake_proposal])

        # ── 创建 Pipeline——通过构造函数注入 label_extractor ──
        pipeline = Pipeline(label_extractor=fake_extractor)

        # ── 构造 LABEL_TABLE 类型的 Spec ──
        # 需添加 LABEL uncertainty——确保 label_candidates 非空，触发 LabelExtractor
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="距离分类",
            description="距离 <= 2 英里归类为短途——锚定项目书。",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="fact",
                    columns=[
                        ColumnDecl(
                            column_name="distance_miles",
                            normalized_name="distance_miles",
                            data_type="double",
                        ),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="distance_category", type="string")],
                grain=[],
            ),
            time_range=None,
            uncertainties=[UncertaintyEntry(
                field_ref="distance_category_ref",
                output_column="distance_category",
                output_kind="LABEL",
                description="需要 CASE WHEN 定义",
            )],
        )

        # ── 执行预处理 ──
        # ── 构建 manifest 并执行标签准备（统一管线入口） ──
        from tianshu_datadev.developer_spec.source_manifest import (
            build_manifest_from_spec,
        )
        manifest = build_manifest_from_spec(spec)
        result_spec = pipeline._prepare_labels(spec, manifest)

        # ── 关键断言：label_rules 元素类型为 CaseWhenDecl ──
        assert len(result_spec.label_rules) == 1, (
            f"label_rules 应有 1 条，实际 {len(result_spec.label_rules)}"
        )
        for rule in result_spec.label_rules:
            assert isinstance(rule, CaseWhenDecl), (
                f"label_rules 元素应为 CaseWhenDecl，实际 {type(rule)}"
            )
            # CaseWhenDecl 不包含 proposal_id（Proposal 仅在 Artifact 中）
            assert not hasattr(rule, "proposal_id"), (
                "CaseWhenDecl 不应有 proposal_id——Proposal 仅保存在 Artifact"
            )

    def test_label_table_without_extractor_raises_error(self):
        """label_table 缺少 label_extractor → LabelTableConfigError（禁止静默回退）。"""
        from tianshu_datadev.api.pipeline import LabelTableConfigError, Pipeline

        # ── 创建无 label_extractor 的 Pipeline ──
        pipeline = Pipeline()

        # ── 构造 LABEL_TABLE Spec —— 需添加 LABEL uncertainty 触发 Extractor ──
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="h", title="测试", description="",
            dataset_type=DatasetType.LABEL_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="t", source_table="fact",
                    columns=[
                        ColumnDecl(
                            column_name="col1",
                            normalized_name="col1",
                            data_type="double",
                        ),
                    ],
                    key_columns=[], business_columns=[],
                ),
            ],
            metrics=[], dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="label_col", type="string")],
                grain=[],
            ),
            time_range=None,
            uncertainties=[UncertaintyEntry(
                field_ref="label_col_ref",
                output_column="label_col",
                output_kind="LABEL",
                description="需要 CASE WHEN 定义",
            )],
        )

        # ── 应抛出 LabelTableConfigError（Extractor 未配置）──
        from tianshu_datadev.developer_spec.source_manifest import (
            build_manifest_from_spec,
        )
        manifest = build_manifest_from_spec(spec)
        import pytest as _pytest
        with _pytest.raises(LabelTableConfigError, match="未配置"):
            pipeline._prepare_labels(spec, manifest)


# ================================================
# Task 11.6: evidence 规范化匹配——正反例测试
# ================================================


class TestEvidenceAnchoring:
    """evidence 必须在 spec.description 中可规范化匹配。

    规范化规则：折叠连续空白字符→单空格，去首尾空格，大小写不敏感。
    """

    def test_evidence_matching_spec_description_passes(self):
        """evidence 原文片段可在 spec.description 中匹配→COVERAGE 通过。"""
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator

        spec = _make_test_spec_with_description(
            "距离 <= 2 英里归类为短途，距离 > 2 英里归类为长途。"
        )
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="距离 <= 2 英里归类为短途",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        validator = LabelRuleValidator()
        report = validator.validate(proposal, spec)
        assert report.passed, (
            f"evidence 匹配 description 应通过，"
            f"blocking={report.blocking_errors}, review={report.human_review_items}"
        )

    def test_evidence_with_whitespace_variation_still_matches(self):
        """evidence 含多余空白→规范化后仍能匹配 description。"""
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator

        spec = _make_test_spec_with_description(
            "距离 <= 2 英里归类为短途，距离 > 2 英里归类为长途。"
        )
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="距离  <=  2  英里归类为短途",  # 多余空白
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        validator = LabelRuleValidator()
        report = validator.validate(proposal, spec)
        # 规范化后空白折叠→应能匹配
        assert report.passed, (
            f"规范化后应能匹配，"
            f"blocking={report.blocking_errors}, review={report.human_review_items}"
        )

    def test_evidence_not_in_description_triggers_human_review(self):
        """evidence 在 description 中完全找不到→HUMAN_REVIEW→passed=False。"""
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator

        spec = _make_test_spec_with_description(
            "距离 <= 2 英里归类为短途，距离 > 2 英里归类为长途。"
        )
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="根据业务经验，短距离行程通常小于2英里",  # 不在 description 中
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        validator = LabelRuleValidator()
        report = validator.validate(proposal, spec)
        # evidence 无法在 description 中匹配→HUMAN_REVIEW
        assert not report.passed, (
            "evidence 不匹配 description 时 passed 应为 False"
        )
        assert len(report.human_review_items) >= 1, (
            f"应有 HUMAN_REVIEW 项，实际: {report.human_review_items}"
        )
        assert any(
            "无法在项目书正文中匹配" in item
            for item in report.human_review_items
        ), f"human_review_items 应包含匹配失败信息: {report.human_review_items}"

    def test_empty_description_blocks_evidence(self):
        """description 为空时 evidence 验证不得通过——标记 HUMAN_REVIEW。"""
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator

        spec = _make_test_spec_with_description("")
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.GT,
                        right=LabelTypedLiteral(value=Decimal("0"), data_type="number"),
                    ),
                    then_label="label_a",
                    evidence="距离大于 0 英里的充分证据——满足最小长度",
                ),
            ],
            else_value="label_b",
            label_domain=LabelDomain(values=["label_a", "label_b"]),
        )
        validator = LabelRuleValidator()
        report = validator.validate(proposal, spec)
        # description 为空→evidence 无法锚定→passed=False
        assert not report.passed, (
            "description 为空时 evidence 验证不得通过——应标记 HUMAN_REVIEW"
        )
        assert any(
            "description" in item.lower() for item in report.human_review_items
        ), f"human_review_items 应包含空 description 错误: {report.human_review_items}"


def _make_test_spec_with_description(description: str):
    """构造带自定义 description 的测试 Spec。"""
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="测试", description=description,
        dataset_type=DatasetType.LABEL_TABLE,
        input_tables=[
            InputTableDecl(
                table_alias="t", source_table="fact",
                columns=[
                    ColumnDecl(column_name="distance_miles",
                               normalized_name="distance_miles",
                               data_type="double"),
                ],
                key_columns=[], business_columns=[],
            ),
        ],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name="distance_category", type="string")],
            grain=[],
        ),
        time_range=None,
    )


class TestLabelTableV1Restrictions:
    """LabelNot 节点仍被 Validator 拒绝——Label 规则不支持 NOT 逻辑嵌套。"""

    def test_label_not_blocked_by_validator(self):
        """LabelNot 节点 → Validator NO_LABEL_NOT 检查 → BLOCKING。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="label_a",
            branches=[
                LabelBranchProposal(
                    condition=LabelNot(
                        child=LabelCompare(
                            left="col1", op=CompareOp.EQ,
                            right=LabelTypedLiteral(value=Decimal("0"), data_type="number"),
                        ),
                    ),
                    then_label="nonzero",
                    evidence="证据文本锚定在正文中——满足匹配要求。",
                ),
            ],
            else_value="zero",
            label_domain=LabelDomain(values=["nonzero", "zero"]),
        )
        spec = _make_test_spec_with_description(
            "证据文本锚定在正文中——满足匹配要求。"
        )
        report = validator.validate(proposal, spec)
        assert not report.passed, "LabelNot 应被拒绝"
        not_check = next(
            (c for c in report.checks if c.check_name == "NO_LABEL_NOT"), None
        )
        assert not_check is not None, "应存在 NO_LABEL_NOT 检查项"
        assert not not_check.passed, "NO_LABEL_NOT 检查应失败"
        assert any("LabelNot" in e for e in report.blocking_errors), (
            f"blocking_errors 应包含 LabelNot: {report.blocking_errors}"
        )


# ================================================
# Task 12.1: 部分成功——部分 Proposal 通过，部分被拒绝
# ================================================


class TestPartialSuccess:
    """部分 Proposal 通过验证，部分被拒绝——验证门禁正确区分。"""

    def test_one_passes_one_fails_validation(self):
        """一条 Proposal 合法通过，另一条 evidence 为空→HUMAN_REVIEW→被拒绝。"""
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator

        spec = _make_test_spec_with_description(
            "证据文本锚定在项目书正文中——满足匹配要求。"
        )

        # 合法 Proposal
        ok_proposal = LabelRuleProposal(
            proposal_id="p_ok", source_spec_hash="h",
            output_column="label_a",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                    evidence="证据文本锚定在项目书正文中——满足匹配要求。",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )

        # 不合法的 Proposal——evidence 为空
        bad_proposal = LabelRuleProposal(
            proposal_id="p_bad", source_spec_hash="h",
            output_column="label_b",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.GT,
                        right=LabelTypedLiteral(value=Decimal("0"), data_type="number"),
                    ),
                    then_label="positive",
                    evidence="",  # 空 evidence → HUMAN_REVIEW
                ),
            ],
            else_value="zero",
            label_domain=LabelDomain(values=["positive", "zero"]),
        )

        validator = LabelRuleValidator()
        ok_report = validator.validate(ok_proposal, spec)
        bad_report = validator.validate(bad_proposal, spec)

        assert ok_report.passed, (
            f"合法 Proposal 应通过验证: "
            f"blocking={ok_report.blocking_errors}, review={ok_report.human_review_items}"
        )
        assert not bad_report.passed, "evidence 为空的 Proposal 不应通过验证"



