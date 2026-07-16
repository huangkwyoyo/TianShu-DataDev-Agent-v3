"""label_table 真实 LLM 冒烟测试——默认跳过，需 --run-harness 启用。

验证链路：
  真实 Template 2 Markdown → Parser → AnthropicAdapter → LLMGateway
  → LlmLabelExtractor → LabelRuleValidator → Promotion

断言：distance_category、4 个分支、ELSE unknown、无 blocking、无 human_review。

仅验证从 Markdown 到结构化 CaseWhenDecl 的完整提取流程——
禁止 Spark 物理执行、快照框架、评测平台或新编排层。
"""

from __future__ import annotations

import os

import pytest

# ── 门控：必须同时满足 --run-harness 和 API Key 可用 ──
_HAS_API_KEY = bool(os.environ.get("DEEPSEEK_API_KEY"))

pytestmark = [
    pytest.mark.harness,
    pytest.mark.skipif(
        not _HAS_API_KEY,
        reason="缺少 DEEPSEEK_API_KEY——真实 LLM 冒烟测试需要 API Key",
    ),
]


class TestLabelExtractorRealLLMSmoke:
    """真实 LLM 冒烟——Template 2 Markdown → LlmLabelExtractor → Validator → Promotion。

    仅此一个测试。默认跳过（需 --run-harness + API Key）。
    """

    def test_template2_real_llm_extract_to_promotion(self, tmp_path):
        """真实 LLM 提取 Template 2 标签规则——全链路冒烟。

        链路：
        1. 读取真实 Template 2 Markdown（TEMPLATES["tpl_label_table"]）
        2. Parser 解析 → ParsedDeveloperSpec（含 dataset_type=LABEL_TABLE）
        3. AnthropicAdapter → LLMGateway → LlmLabelExtractor.extract()
        4. LabelRuleValidator.validate()——六项检查
        5. Promotion.promote()——双空阻断

        断言：
        - output_column == "distance_category"
        - 4 个 WHEN 分支（unknown / short / medium / long）
        - else_value == "unknown"
        - Validator 无 blocking_errors
        - Validator 无 human_review_items
        - Promotion 成功提升（human_review_required=False）
        """
        # ── 导入（延迟——避免模块加载时连接外部服务）──
        from tianshu_datadev.api.templates import TEMPLATES
        from tianshu_datadev.developer_spec.models import DatasetType
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
        from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator
        from tianshu_datadev.labels.llm_label_extractor import (
            LlmLabelExtractor,
            PipelineError,
        )
        from tianshu_datadev.labels.promotion import Promotion
        from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns
        from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter
        from tianshu_datadev.llm.gateway import LLMGateway
        from tianshu_datadev.prompts.manager import PromptManager

        # ── Step 1: 读取真实 Template 2 Markdown ──
        template2 = next(
            t for t in TEMPLATES if t["template_id"] == "tpl_label_table"
        )
        markdown_text = template2["markdown_template"]

        # ── Step 2: Parser 解析 ──
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        assert spec.dataset_type == DatasetType.LABEL_TABLE, (
            f"应为 LABEL_TABLE，实际: {spec.dataset_type}"
        )
        # 覆盖行数为小值——避免 Validator 因 8000 万行触发时间过滤阻断
        spec.input_tables[0].row_count = 100

        # 验证未解析列
        unresolved = _find_unresolved_derived_columns(spec)
        assert "distance_category" in unresolved, (
            f"distance_category 应在未解析列中，实际: {unresolved}"
        )

        # ── Step 3: 构造真实 LLM 链路 ──
        response_root = tmp_path / "llm_responses"
        response_root.mkdir()

        adapter = AnthropicAdapter()
        prompt_manager = PromptManager()
        gateway = LLMGateway(
            adapter=adapter,
            prompt_manager=prompt_manager,
            response_root=str(response_root),
        )
        llm_extractor = LlmLabelExtractor(gateway)

        # ── Step 4: LlmLabelExtractor.extract() —— 调用真实 LLM ──
        try:
            proposals, extraction_artifact = llm_extractor.extract(
                spec, unresolved,
            )
        except PipelineError as e:
            pytest.fail(
                f"真实 LLM 提取失败: {e}——"
                f"请检查 API Key 和网络连接"
            )

        assert len(proposals) >= 1, (
            f"至少应有 1 个 Proposal，实际: {len(proposals)}"
        )
        proposal = proposals[0]

        # ── Step 5: 验证 Proposal 结构 ──
        assert proposal.output_column == "distance_category", (
            f"output_column 应为 distance_category，实际: {proposal.output_column}"
        )
        assert proposal.else_value == "unknown", (
            f"else_value 应为 unknown，实际: {proposal.else_value}"
        )
        assert len(proposal.branches) == 4, (
            f"应有 4 个 WHEN 分支，实际: {len(proposal.branches)}——"
            f"then_labels={[b.then_label for b in proposal.branches]}"
        )

        # 验证四个分支的 then_label
        labels = {b.then_label for b in proposal.branches}
        expected_labels = {"unknown", "short", "medium", "long"}
        missing_labels = expected_labels - labels
        assert not missing_labels, (
            f"缺少标签: {missing_labels}——实际标签: {sorted(labels)}"
        )

        # 验证每个分支 evidence 非空
        for i, branch in enumerate(proposal.branches):
            assert branch.evidence, (
                f"分支 {i}（label={branch.then_label}）evidence 不应为空"
            )

        # 验证 label_domain 非空
        assert proposal.label_domain is not None, "label_domain 不可为 None"
        assert len(proposal.label_domain.values) >= 4, (
            f"label_domain 应至少含 4 个值，"
            f"实际: {len(proposal.label_domain.values)}——"
            f"values={proposal.label_domain.values}"
        )

        # 验证系统生成字段
        assert proposal.proposal_id != "", "proposal_id 应由系统生成"
        assert proposal.source_spec_hash == spec.spec_hash, (
            f"source_spec_hash 应匹配，"
            f"期望: {spec.spec_hash[:12]}...，实际: {proposal.source_spec_hash[:12]}..."
        )

        # ── Step 6: LabelRuleValidator.validate() —— 六项检查 ──
        validator = LabelRuleValidator()
        report = validator.validate(proposal, spec)

        assert report.passed, (
            f"Validator 应通过——"
            f"blocking={report.blocking_errors}, "
            f"human_review={report.human_review_items}"
        )
        assert len(report.blocking_errors) == 0, (
            f"不应有 blocking_errors，实际: {report.blocking_errors}"
        )
        assert len(report.human_review_items) == 0, (
            f"不应有 human_review_items，实际: {report.human_review_items}"
        )

        # ── Step 7: Promotion.promote() —— 双空阻断 ──
        promotion = Promotion()
        promoted_rules, promotion_artifact = promotion.promote(
            spec.spec_hash,
            proposals,
            [report],
            extraction_artifact,
        )

        assert len(promoted_rules) == 1, (
            f"应有 1 个提升的 CaseWhenDecl，实际: {len(promoted_rules)}"
        )
        assert promotion_artifact.human_review_required is False, (
            "Promotion 不应要求人工审查——"
            f"rejected={promotion_artifact.rejected_proposals}"
        )

        case_when = promoted_rules[0]
        assert case_when.output_column == "distance_category", (
            f"CaseWhenDecl output_column 应为 distance_category，"
            f"实际: {case_when.output_column}"
        )
        assert case_when.else_value == "unknown", (
            f"CaseWhenDecl else_value 应为 unknown，"
            f"实际: {case_when.else_value}"
        )
        assert len(case_when.typed_branches) == 4, (
            f"CaseWhenDecl 应有 4 个 typed_branches，"
            f"实际: {len(case_when.typed_branches)}"
        )
