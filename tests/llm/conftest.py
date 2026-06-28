"""Phase 4A LLM Gateway 测试 fixtures——Fake Adapter + PromptManager + Gateway。"""

from __future__ import annotations

import pytest

from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import ArtifactRef, LlmRequest
from tianshu_datadev.prompts.manager import PromptManager


@pytest.fixture
def fake_adapter() -> FakeLLMAdapter:
    """创建空的 FakeLLMAdapter——各测试自行注册 fixture。"""
    return FakeLLMAdapter()


@pytest.fixture
def prompt_manager() -> PromptManager:
    """创建指向真实模板目录的 PromptManager。"""
    return PromptManager()


@pytest.fixture
def gateway(fake_adapter: FakeLLMAdapter, prompt_manager: PromptManager) -> LLMGateway:
    """创建配置了 Fake Adapter 的 LLMGateway。"""
    return LLMGateway(adapter=fake_adapter, prompt_manager=prompt_manager)


@pytest.fixture
def sample_artifact_ref() -> ArtifactRef:
    """创建示例 ArtifactRef——用于构造 LlmRequest。"""
    return ArtifactRef(
        artifact_type="developer_spec_yaml",
        artifact_hash="abc123def456",
        artifact_id="spec_20260628_001",
    )


@pytest.fixture
def valid_request(sample_artifact_ref: ArtifactRef) -> LlmRequest:
    """创建最小有效的 LlmRequest——用于测试 Gateway 正常流程。"""
    return LlmRequest(
        request_id=LlmRequest.generate_request_id(),
        task="developer_spec_parser",
        prompt_version="v001",
        schema_name="ParsedDeveloperSpec",
        schema_version="1.0",
        input_artifact_refs=[sample_artifact_ref],
        temperature=0.0,
        model="fake",
    )
