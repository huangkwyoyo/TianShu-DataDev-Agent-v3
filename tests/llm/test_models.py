"""Phase 4A LLM 数据模型测试——LlmRequest / LlmResponse / ArtifactRef / SchemaBinding。

验证：
1. 正常构造
2. extra="forbid" 拒绝未知字段
3. validation_status 正确设置
4. SchemaBinding 序列化/反序列化
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.llm.models import (
    ArtifactRef,
    LlmRequest,
    LlmResponse,
    SchemaBinding,
)


class TestArtifactRef:
    """ArtifactRef 模型测试。"""

    def test_artifact_ref_valid(self) -> None:
        """正常构造 ArtifactRef——三个字段均可正常设置。"""
        ref = ArtifactRef(
            artifact_type="parsed_developer_spec",
            artifact_hash="abc123def456",
            artifact_id="spec_20260628_001",
        )
        assert ref.artifact_type == "parsed_developer_spec"
        assert ref.artifact_hash == "abc123def456"
        assert ref.artifact_id == "spec_20260628_001"

    def test_artifact_ref_rejects_extra_fields(self) -> None:
        """ArtifactRef 拒绝未知字段（extra="forbid"）。"""
        with pytest.raises(ValidationError):
            ArtifactRef(
                artifact_type="test",
                artifact_hash="abc",
                artifact_id="id1",
                unknown_field="should_reject",  # 未知字段
            )


class TestLlmRequest:
    """LlmRequest 模型测试。"""

    def test_llm_request_valid(self) -> None:
        """正常构造 LlmRequest——所有必填字段填充正确。"""
        req = LlmRequest(
            request_id="test_req_001",
            task="parse_developer_spec",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )
        assert req.request_id == "test_req_001"
        assert req.task == "parse_developer_spec"
        assert req.prompt_version == "v001"
        assert req.temperature == 0.0  # 默认值
        assert req.input_artifact_refs == []  # 默认空列表

    def test_llm_request_rejects_extra_fields(self) -> None:
        """LlmRequest 拒绝未知字段（extra="forbid"）。"""
        with pytest.raises(ValidationError):
            LlmRequest(
                request_id="test_req_002",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                model="fake",
                free_text_prompt="绕过 PromptManager",  # 未知字段——应被拒绝
            )

    def test_generate_request_id_unique(self) -> None:
        """generate_request_id() 两次调用生成不同 ID。"""
        id1 = LlmRequest.generate_request_id()
        id2 = LlmRequest.generate_request_id()
        assert id1 != id2
        assert id1.startswith("llm_req_")
        assert id2.startswith("llm_req_")

    def test_llm_request_with_artifact_refs(self) -> None:
        """LlmRequest 可携带多个 ArtifactRef。"""
        refs = [
            ArtifactRef(
                artifact_type="developer_spec_yaml",
                artifact_hash="abc123",
                artifact_id="spec_001",
            ),
            ArtifactRef(
                artifact_type="source_manifest",
                artifact_hash="def456",
                artifact_id="manifest_001",
            ),
        ]
        req = LlmRequest(
            request_id="test_req_003",
            task="parse_developer_spec",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            input_artifact_refs=refs,
            model="fake",
        )
        assert len(req.input_artifact_refs) == 2
        assert req.input_artifact_refs[0].artifact_type == "developer_spec_yaml"


class TestLlmResponse:
    """LlmResponse 模型测试。"""

    def test_llm_response_valid_status(self) -> None:
        """正常构造 LlmResponse——validation_status="valid"。"""
        resp = LlmResponse(
            request_id="test_req_001",
            task="parse_developer_spec",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            raw_response_ref="llm_responses/raw/test.json",
            parsed_json_ref="llm_responses/parsed/test.json",
            validation_status="valid",
            token_usage={"total_tokens": 100},
            latency_ms=500,
        )
        assert resp.is_valid is True
        assert resp.validation_status == "valid"
        assert resp.validation_errors == []
        assert resp.token_usage == {"total_tokens": 100}
        assert resp.latency_ms == 500

    def test_llm_response_invalid_status(self) -> None:
        """LlmResponse——validation_status="invalid" + 错误列表。"""
        resp = LlmResponse(
            request_id="test_req_002",
            task="parse_developer_spec",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            raw_response_ref="llm_responses/raw/test.json",
            parsed_json_ref=None,
            validation_status="invalid",
            validation_errors=[
                "[missing] spec_id: Field required",
                "[extra] unknown_field: Extra inputs are not permitted",
            ],
            token_usage={},
            latency_ms=300,
        )
        assert resp.is_valid is False
        assert resp.validation_status == "invalid"
        assert len(resp.validation_errors) == 2
        assert resp.parsed_json_ref is None

    def test_llm_response_rejects_extra_fields(self) -> None:
        """LlmResponse 拒绝未知字段。"""
        with pytest.raises(ValidationError):
            LlmResponse(
                request_id="test",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                raw_response_ref="ref",
                validation_status="invalid",
                raw_text="SELECT * FROM users",  # 企图携带 SQL 原始文本
            )

    def test_generate_response_ref(self) -> None:
        """generate_response_ref() 生成有效引用路径——相对于 response_root。"""
        ref = LlmResponse.generate_response_ref("test_req_001")
        assert ref.startswith("raw/")
        assert "test_req_001" in ref
        assert ref.endswith(".json")

    def test_generate_parsed_ref(self) -> None:
        """generate_parsed_ref() 生成有效引用路径——相对于 response_root。"""
        ref = LlmResponse.generate_parsed_ref("test_req_001")
        assert ref.startswith("parsed/")
        assert "test_req_001" in ref
        assert ref.endswith(".json")


class TestLlmResponseInvariants:
    """LlmResponse 跨字段一致性校验——模型级硬约束。"""

    def test_valid_requires_parsed_ref(self) -> None:
        """validation_status="valid" 但 parsed_json_ref=None → ValidationError。"""
        with pytest.raises(ValidationError, match="parsed_json_ref"):
            LlmResponse(
                request_id="test_req",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                raw_response_ref="ref",
                parsed_json_ref=None,  # valid 状态必须有引用
                validation_status="valid",
            )

    def test_valid_requires_no_validation_errors(self) -> None:
        """validation_status="valid" 但 validation_errors 非空 → ValidationError。"""
        with pytest.raises(ValidationError, match="validation_errors"):
            LlmResponse(
                request_id="test_req",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                raw_response_ref="ref",
                parsed_json_ref="llm_responses/parsed/test.json",
                validation_status="valid",
                validation_errors=["some error"],  # valid 状态不应有错误
            )

    def test_invalid_forbids_parsed_ref(self) -> None:
        """validation_status="invalid" 但 parsed_json_ref 非空 → ValidationError。"""
        with pytest.raises(ValidationError, match="parsed_json_ref"):
            LlmResponse(
                request_id="test_req",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                raw_response_ref="ref",
                parsed_json_ref="llm_responses/parsed/test.json",  # invalid 状态禁止引用
                validation_status="invalid",
            )

    def test_rejects_non_literal_status(self) -> None:
        """validation_status 使用非 Literal 值 → ValidationError（Pydantic 类型校验）。"""
        with pytest.raises(ValidationError):
            LlmResponse(
                request_id="test_req",
                task="test",
                prompt_version="v001",
                schema_name="Test",
                schema_version="1.0",
                raw_response_ref="ref",
                validation_status="maybe",  # 不是 "valid" 或 "invalid"
            )


class TestSchemaBinding:
    """SchemaBinding 模型测试。"""

    def test_schema_binding_valid(self) -> None:
        """正常构造 SchemaBinding。"""
        binding = SchemaBinding(
            task="developer_spec_parser",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            pydantic_model_path="tianshu_datadev.developer_spec.models.ParsedDeveloperSpec",
            json_schema={"type": "object", "properties": {}},
        )
        assert binding.task == "developer_spec_parser"
        assert binding.schema_name == "ParsedDeveloperSpec"

    def test_schema_binding_roundtrip(self) -> None:
        """SchemaBinding 序列化后反序列化一致。"""
        binding = SchemaBinding(
            task="test_task",
            schema_name="TestSchema",
            schema_version="2.0",
            pydantic_model_path="some.module.TestSchema",
            json_schema={"type": "object", "properties": {"name": {"type": "string"}}},
        )
        # 序列化
        data = binding.model_dump()
        # 反序列化
        restored = SchemaBinding(**data)
        assert restored.task == binding.task
        assert restored.schema_name == binding.schema_name
        assert restored.schema_version == binding.schema_version
        assert restored.pydantic_model_path == binding.pydantic_model_path
        assert restored.json_schema == binding.json_schema
