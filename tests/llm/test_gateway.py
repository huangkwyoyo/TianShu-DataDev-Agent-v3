"""LLMGateway 测试——正常流程 + extra 拒绝 + Schema 不匹配 + 未知 task。

验证：
1. 正常流程：Prompt → Adapter → Schema 校验 → valid
2. extra 字段 → validation_status="invalid"
3. Schema 不匹配 → validation_status="invalid"
4. 未知 task → validation_status="invalid"
"""

from __future__ import annotations

from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import ArtifactRef, LlmRequest
from tianshu_datadev.prompts.manager import PromptManager

# ════════════════════════════════════════════
# 最小有效 ParsedDeveloperSpec——用于 Gateway 正常流程测试
# ════════════════════════════════════════════

def _make_valid_parsed_spec() -> dict:
    """构造一个最小有效的 ParsedDeveloperSpec dict——
    通过 Pydantic model_validate 校验。
    """
    return {
        "spec_id": "",
        "spec_hash": "",
        "title": "日活用户统计",
        "description": "统计每日活跃用户数",
        "input_tables": [
            {
                "table_alias": "logs",
                "source_table": "dw.user_action_logs",
                "role": "fact",
                "columns": [
                    {
                        "column_name": "user_id",
                        "normalized_name": "user_id",
                        "data_type": "BIGINT",
                    },
                    {
                        "column_name": "action_date",
                        "normalized_name": "action_date",
                        "data_type": "DATE",
                    },
                ],
                "partition_field": "dt",
                "time_field": "action_date",
            }
        ],
        "metrics": [
            {
                "metric_name": "dau",
                "aggregation": "COUNT_DISTINCT",
                "input_column": "user_id",
                "alias": "dau",
            }
        ],
        "dimensions": [
            {
                "dimension_name": "日期",
                "column_ref": "action_date",
            }
        ],
        "output_spec": {
            "columns": ["action_date", "dau"],
            "grain": ["action_date"],
        },
        "open_questions": [],
        "parse_warnings": [],
    }


def _make_extra_fields_spec() -> dict:
    """构造含 extra 字段的 ParsedDeveloperSpec——extra="forbid" 应拒绝。"""
    valid = _make_valid_parsed_spec()
    valid["raw_sql"] = "SELECT * FROM users"  # SQL 注入企图
    return valid


def _make_schema_mismatch_spec() -> dict:
    """构造字段类型错误的 dict——aggregation 不是枚举值。"""
    valid = _make_valid_parsed_spec()
    valid["metrics"][0]["aggregation"] = "FREE_FORM_SQL"  # 不在 AggregationType 枚举中
    return valid


class TestGatewayValidFlow:
    """Gateway 正常流程测试。"""

    def test_gateway_submit_valid(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """正常流程：注册有效 fixture → Gateway submit → validation_status="valid"。"""
        # 注册 task 级别的默认 fixture
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_valid_parsed_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            input_artifact_refs=[
                ArtifactRef(
                    artifact_type="developer_spec_yaml",
                    artifact_hash="abc123",
                    artifact_id="spec_001",
                )
            ],
            temperature=0.0,
            model="fake",
        )

        response = gateway.submit(request)

        assert response.validation_status == "valid", (
            f"期望 valid，实际 {response.validation_status}——"
            f"错误：{response.validation_errors}"
        )
        assert response.is_valid is True
        assert response.parsed_json_ref is not None
        assert response.raw_response_ref != ""
        assert response.task == "developer_spec_parser"
        assert response.prompt_version == "v001"
        # 响应中的 schema_name/schema_version 应来自 Prompt 模板绑定，
        # 而非请求中的声称值（防御 schema 元数据不一致）
        assert response.schema_name == "ParsedDeveloperSpec"
        assert response.schema_version == "1.0"

    def test_gateway_tracks_prompt_version(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """LlmResponse 中 prompt_version 可追踪——验证响应包含版本信息。"""
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_valid_parsed_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.prompt_version == "v001"
        assert response.schema_version == "1.0"
        # request_id 在响应中保持一致
        assert response.request_id == request.request_id


class TestGatewayRejection:
    """Gateway 拒绝路径测试。"""

    def test_gateway_rejects_extra_fields(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """extra 字段（raw_sql）→ validation_status="invalid"。"""
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_extra_fields_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid", (
            f"extra 字段应被拒绝，实际状态={response.validation_status}"
        )
        assert response.is_valid is False
        assert response.parsed_json_ref is None
        # 错误信息应包含 extra 相关提示
        assert any(
            "extra" in err.lower() or "unknown" in err.lower()
            for err in response.validation_errors
        ), f"错误应包含 extra/unknown 提示：{response.validation_errors}"

    def test_gateway_rejects_schema_mismatch(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """字段类型不匹配（aggregation 非枚举值）→ validation_status="invalid"。"""
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_schema_mismatch_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid", (
            f"Schema 不匹配应被拒绝，实际状态={response.validation_status}"
        )
        assert response.is_valid is False
        assert response.parsed_json_ref is None
        assert len(response.validation_errors) > 0

    def test_gateway_rejects_unknown_task(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """未知 task → validation_status="invalid"。"""
        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="nonexistent_task",
            prompt_version="v001",
            schema_name="FakeSchema",
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid"
        assert "未知 task" in response.validation_errors[0]
        assert response.parsed_json_ref is None

    def test_gateway_rejects_unknown_prompt_version(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """已知 task 但未知 version → validation_status="invalid"。"""
        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v999",  # 不存在的版本
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid"
        assert any(
            "v999" in err or "未知" in err for err in response.validation_errors
        ), f"错误应包含版本提示：{response.validation_errors}"
        assert response.parsed_json_ref is None

    def test_gateway_rejects_schema_name_mismatch(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """请求 schema_name 与 Prompt 模板绑定不一致 → validation_status="invalid"。

        复现场景：task=developer_spec_parser（绑定 ParsedDeveloperSpec），
        但请求声称 schema_name=SqlBuildPlan。
        """
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_valid_parsed_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="SqlBuildPlan",  # 错误的 Schema 名称
            schema_version="1.0",
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid", (
            f"schema_name 不匹配应被拒绝，实际状态={response.validation_status}"
        )
        assert response.is_valid is False
        assert response.parsed_json_ref is None
        # 错误消息应包含双方 Schema 名称
        assert any(
            "SqlBuildPlan" in err and "ParsedDeveloperSpec" in err
            for err in response.validation_errors
        ), f"错误应包含请求声称值和模板绑定值：{response.validation_errors}"
        # 应包含"不一致"关键词
        assert any(
            "不一致" in err for err in response.validation_errors
        ), f"错误应说明 Schema 绑定不一致：{response.validation_errors}"

    def test_gateway_rejects_schema_version_mismatch(
        self,
        fake_adapter: FakeLLMAdapter,
        prompt_manager: PromptManager,
    ) -> None:
        """请求 schema_version 与 Prompt 模板绑定不一致 → validation_status="invalid"。"""
        fake_adapter.register_default_for_task(
            task="developer_spec_parser",
            output=_make_valid_parsed_spec(),
        )

        gateway = LLMGateway(
            adapter=fake_adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="999.0",  # 错误的版本号
            model="fake",
        )

        response = gateway.submit(request)
        assert response.validation_status == "invalid", (
            f"schema_version 不匹配应被拒绝，实际状态={response.validation_status}"
        )
        assert response.is_valid is False
        assert response.parsed_json_ref is None
        # 错误消息应包含版本号信息
        assert any(
            "999.0" in err for err in response.validation_errors
        ), f"错误应包含请求的版本号：{response.validation_errors}"


class TestGatewayAdapterError:
    """Gateway 处理 AdapterError 的测试。"""

    def test_gateway_handles_adapter_error(
        self,
        prompt_manager: PromptManager,
    ) -> None:
        """Adapter 抛出 AdapterError → Gateway 返回 invalid 而非传播异常。"""
        # 不注册任何 fixture——FakeLLMAdapter 会抛出 AdapterError
        adapter = FakeLLMAdapter()

        gateway = LLMGateway(
            adapter=adapter,
            prompt_manager=prompt_manager,
        )

        request = LlmRequest(
            request_id=LlmRequest.generate_request_id(),
            task="developer_spec_parser",
            prompt_version="v001",
            schema_name="ParsedDeveloperSpec",
            schema_version="1.0",
            model="fake",
        )

        # 不应抛出异常——Gateway 内部捕获 AdapterError
        response = gateway.submit(request)
        assert response.validation_status == "invalid"
        assert any(
            "adapter" in err.lower() or "未找到" in err
            for err in response.validation_errors
        ), f"错误应说明 Adapter 问题：{response.validation_errors}"
        assert response.parsed_json_ref is None
