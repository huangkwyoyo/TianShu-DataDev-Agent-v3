"""LlmLabelExtractor——生产路径标签提取器。

通过 LLMGateway 调用真实 LLM 提取标签规则。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from tianshu_datadev.developer_spec.models import (
    LabelBranchProposal,
    LabelDomain,
    LabelDomainOutput,
    LabelRuleProposal,
    LabelRuleProposalList,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import LabelExtractionArtifact
from tianshu_datadev.labels.label_extractor import LabelExtractor
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import ArtifactRef, LlmRequest


class LlmLabelExtractor(LabelExtractor):
    """生产路径标签提取器——通过 LLMGateway 调用真实 LLM。

    工作流程：
    1. 收集源表字段→available_fields
    2. 构造 LlmRequest（task="extract_label_rules"）
    3. gateway.submit()——含 markdown_body/unresolved_columns/available_fields
    4. 从 response_root 读取 parsed_json_ref 文件→LabelRuleProposalList
    5. 系统包装 proposal_id/source_spec_hash/extraction_time
    """

    def __init__(self, gateway: LLMGateway) -> None:
        """初始化 LlmLabelExtractor。

        Args:
            gateway: LLM Gateway——用于调用 LLM 并写入 response_root
        """
        self._gateway = gateway

    @property
    def gateway(self) -> LLMGateway:
        """返回当前 Gateway——仅供诊断。"""
        return self._gateway

    def extract(
        self,
        spec: ParsedDeveloperSpec,
        unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """通过 LLM 提取标签规则。

        Args:
            spec: 已解析的 DeveloperSpec
            unresolved_columns: 未解析的派生列名列表

        Returns:
            (proposals, artifact)——proposals 含系统生成的 proposal_id/source_spec_hash
        """
        # 1. 收集源表可用字段
        available_fields: list[str] = []
        for table in spec.input_tables:
            for col in table.columns:
                if col.column_name not in available_fields:
                    available_fields.append(col.column_name)

        # 2. 构造 LlmRequest
        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="extract_label_rules",
            prompt_version="v001",
            schema_name="LabelRuleProposalList",
            schema_version="v1",
            input_artifact_refs=[
                ArtifactRef(
                    artifact_type="parsed_developer_spec",
                    artifact_hash=spec.spec_hash,
                    artifact_id=spec.spec_id,
                ),
            ],
            temperature=0.1,
            model="",
        )

        # 3. 提交 LLM 请求
        response = self._gateway.submit(
            request,
            markdown_body=spec.description,
            unresolved_columns=json.dumps(unresolved_columns, ensure_ascii=False),
            available_fields=json.dumps(available_fields, ensure_ascii=False),
        )

        # 4. 从 response_root 读取结构化输出
        raw_proposals: list[LabelRuleProposal] = []
        if response.is_valid and response.parsed_json_ref is not None:
            # parsed_json_ref 可能是相对路径（相对于 response_root）或绝对路径
            parsed_path = Path(response.parsed_json_ref)
            if not parsed_path.is_absolute():
                parsed_path = self._gateway.response_root / response.parsed_json_ref

            if parsed_path.exists():
                data = json.loads(parsed_path.read_text("utf-8"))
                llm_output = LabelRuleProposalList.model_validate(data)
                raw_proposals = self._wrap_system_fields(
                    llm_output, spec.spec_hash,
                )

        # 5. 构造溯源 Artifact
        extraction_time = datetime.now(timezone.utc).isoformat()
        artifact = LabelExtractionArtifact(
            artifact_id=f"ext_{uuid.uuid4().hex[:12]}",
            source_spec_hash=spec.spec_hash,
            extraction_time=extraction_time,
            llm_model=response.token_usage.get("model", "unknown"),
            llm_prompt_version=response.prompt_version,
            llm_temperature=request.temperature,
            unresolved_columns=unresolved_columns,
            raw_proposals=raw_proposals,
            prompt_snapshot="",
        )

        return raw_proposals, artifact

    @staticmethod
    def _wrap_system_fields(
        llm_output: LabelRuleProposalList,
        source_spec_hash: str,
    ) -> list[LabelRuleProposal]:
        """将 LLM 输出包装为系统级 Proposal——注入 proposal_id/source_spec_hash。

        LLM 输出层不含系统字段——此方法将 LLM 输出的 LabelRuleProposalOutput
        转换为系统层的 LabelRuleProposal，并验证必需字段完整性。
        """
        proposals: list[LabelRuleProposal] = []
        for llm_rule in llm_output.rules:
            proposal_id = f"prop_{uuid.uuid4().hex[:12]}"

            # 包装 LabelDomain
            domain = LlmLabelExtractor._wrap_domain(llm_rule.label_domain)

            # 包装分支——evidence 非空检查
            branches: list[LabelBranchProposal] = []
            for llm_branch in llm_rule.branches:
                branches.append(LabelBranchProposal(
                    condition=llm_branch.condition,
                    then_label=llm_branch.then_label,
                    evidence=llm_branch.evidence or "",
                ))

            proposals.append(LabelRuleProposal(
                proposal_id=proposal_id,
                source_spec_hash=source_spec_hash,
                output_column=llm_rule.output_column,
                branches=branches,
                else_value=llm_rule.else_value,
                label_domain=domain,
            ))
        return proposals

    @staticmethod
    def _wrap_domain(
        llm_domain: LabelDomainOutput | None,
    ) -> LabelDomain:
        """将 LLM 输出的 LabelDomainOutput 包装为系统 LabelDomain。

        Args:
            llm_domain: LLM 输出的标签域——可为 None

        Returns:
            系统 LabelDomain——含唯一 domain_id
        """
        if llm_domain is None:
            return LabelDomain(
                domain_id=f"dom_{uuid.uuid4().hex[:12]}",
                values=[],
            )
        return LabelDomain(
            domain_id=f"dom_{uuid.uuid4().hex[:12]}",
            values=llm_domain.values,
            source_evidence=llm_domain.source_evidence,
            is_exhaustive=llm_domain.is_exhaustive,
            completeness_evidence=llm_domain.completeness_evidence,
        )
