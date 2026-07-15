"""标签提取器——抽象接口和测试用 Fake 实现。

LabelExtractor 定义标签提取的抽象契约；
FakeLabelExtractor 提供确定性的测试替身——仅用于 pytest。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from tianshu_datadev.developer_spec.models import (
    LabelRuleProposal,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import LabelExtractionArtifact


class LabelExtractor:
    """标签提取器抽象基类——定义 extract() 契约。

    生产实现（LlmLabelExtractor）通过 LLMGateway 调用真实 LLM；
    测试实现（FakeLabelExtractor）返回预定义 Proposal。
    """

    def extract(
        self,
        spec: ParsedDeveloperSpec,
        unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """从 Spec 中提取标签规则。

        Args:
            spec: 已解析的 DeveloperSpec——含源表列名等上下文
            unresolved_columns: _find_unresolved_derived_columns() 返回的派生列名列表

        Returns:
            (proposals, artifact)——proposals 含系统包装字段，artifact 记录溯源信息

        Raises:
            NotImplementedError: 子类必须实现
        """
        raise NotImplementedError


class FakeLabelExtractor(LabelExtractor):
    """测试用 Fake 标签提取器——返回预定义的 Proposal 列表。

    仅用于 pytest——不调用 LLM，确定性返回。
    """

    def __init__(self, proposals: list[LabelRuleProposal] | None = None) -> None:
        """初始化 Fake 提取器。

        Args:
            proposals: 预定义的 Proposal 列表——extract() 直接返回。
                       为 None 时返回空列表。
        """
        self._proposals = proposals or []

    def extract(
        self,
        spec: ParsedDeveloperSpec,
        unresolved_columns: list[str],
    ) -> tuple[list[LabelRuleProposal], LabelExtractionArtifact]:
        """返回预定义的 Proposal 列表 + 溯源 Artifact。

        Args:
            spec: 已解析的 DeveloperSpec（Fake 实现忽略除 spec_hash 外的字段）
            unresolved_columns: 未解析列名列表（记录到 Artifact 中）

        Returns:
            (proposals, artifact)
        """
        artifact = LabelExtractionArtifact(
            artifact_id=f"ext_{uuid.uuid4().hex[:12]}",
            source_spec_hash=spec.spec_hash,
            extraction_time=datetime.now(timezone.utc).isoformat(),
            llm_model="fake",
            llm_prompt_version="v001",
            llm_temperature=0.0,
            unresolved_columns=unresolved_columns,
            raw_proposals=self._proposals,
            prompt_snapshot="",
        )
        return self._proposals, artifact
