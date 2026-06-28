"""FakeLLMAdapter 测试——结构化输出 + extra 拒绝 + 未知输入拒绝。

验证：
1. 注册→返回正确（输出副本隔离）
2. extra 字段可通过 fixture 注入——测试 Gateway 拒绝路径
3. 未注册输入抛出 AdapterError
4. 多 task 各自独立注册
5. provider_name() == "fake"
"""

from __future__ import annotations

import pytest

from tianshu_datadev.llm.adapters.base import AdapterError
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter


class TestFakeAdapterBasic:
    """FakeLLMAdapter 基础功能测试。"""

    def test_fake_adapter_returns_registered_fixture(self) -> None:
        """注册 fixture → invoke 返回正确的输出副本。"""
        adapter = FakeLLMAdapter()
        adapter.register_fixture(
            task="parse_developer_spec",
            fixture_key="valid_minimal",
            output={"spec_id": "test_001", "title": "测试项目书"},
        )

        result = adapter.invoke(
            system_message="test system",
            user_message="fixture_key: valid_minimal",
            json_schema={},
            model="fake",
            temperature=0.0,
        )

        assert result["spec_id"] == "test_001"
        assert result["title"] == "测试项目书"

    def test_fake_adapter_returns_copy_not_reference(self) -> None:
        """invoke 返回的是 deepcopy——修改返回值不影响注册的 fixture。"""
        adapter = FakeLLMAdapter()
        adapter.register_fixture(
            task="test_task",
            fixture_key="mutable_test",
            output={"data": [1, 2, 3]},
        )

        result1 = adapter.invoke(
            system_message="",
            user_message="fixture_key: mutable_test",
            json_schema={},
            model="fake",
            temperature=0.0,
        )
        # 修改返回值
        result1["data"].append(999)

        # 再次获取——应不受影响
        result2 = adapter.invoke(
            system_message="",
            user_message="fixture_key: mutable_test",
            json_schema={},
            model="fake",
            temperature=0.0,
        )

        assert result2["data"] == [1, 2, 3]  # 未被修改

    def test_fake_adapter_raises_on_unknown_input(self) -> None:
        """未注册的 fixture_key → AdapterError。"""
        adapter = FakeLLMAdapter()

        with pytest.raises(AdapterError, match="未找到 fixture"):
            adapter.invoke(
                system_message="",
                user_message="fixture_key: nonexistent_key",
                json_schema={},
                model="fake",
                temperature=0.0,
            )

    def test_fake_adapter_raises_on_missing_fixture_key(self) -> None:
        """user_message 不含 fixture_key → AdapterError。"""
        adapter = FakeLLMAdapter()

        with pytest.raises(AdapterError):
            adapter.invoke(
                system_message="",
                user_message="这条消息不含 fixture_key 标记",
                json_schema={},
                model="fake",
                temperature=0.0,
            )

    def test_fake_adapter_provider_name(self) -> None:
        """provider_name() 返回 'fake'。"""
        adapter = FakeLLMAdapter()
        assert adapter.provider_name() == "fake"

    def test_fake_adapter_multiple_tasks_independent(self) -> None:
        """不同 task 使用不同 fixture_key——各自独立注册和查找。"""
        adapter = FakeLLMAdapter()

        adapter.register_fixture(
            task="task_a",
            fixture_key="fixture_a",
            output={"result": "A"},
        )
        adapter.register_fixture(
            task="task_b",
            fixture_key="fixture_b",
            output={"result": "B"},
        )

        result_a = adapter.invoke(
            system_message="",
            user_message="fixture_key: fixture_a",
            json_schema={},
            model="fake",
            temperature=0.0,
        )
        result_b = adapter.invoke(
            system_message="",
            user_message="fixture_key: fixture_b",
            json_schema={},
            model="fake",
            temperature=0.0,
        )

        assert result_a["result"] == "A"
        assert result_b["result"] == "B"

    def test_fake_adapter_fixture_with_extra_fields(self) -> None:
        """注册含 extra 字段的 fixture——Fake Adapter 不校验，留给 Gateway。"""
        adapter = FakeLLMAdapter()
        adapter.register_fixture(
            task="test_task",
            fixture_key="extra_fields",
            output={
                "spec_id": "test",
                "title": "正常字段",
                "unknown_extra_field": "此字段将被 Gateway 拒绝",  # extra 字段
            },
        )

        # Fake Adapter 直接返回——不执行 Schema 校验
        result = adapter.invoke(
            system_message="",
            user_message="fixture_key: extra_fields",
            json_schema={},
            model="fake",
            temperature=0.0,
        )

        # 含 extra 字段——但 Adapter 不校验
        assert "unknown_extra_field" in result
        assert result["unknown_extra_field"] == "此字段将被 Gateway 拒绝"

    def test_fake_adapter_constructor_with_fixtures(self) -> None:
        """通过构造函数传入 fixtures——可正常使用。"""
        adapter = FakeLLMAdapter(
            fixtures={
                "pre_registered": {"key": "value"},
            }
        )

        result = adapter.invoke(
            system_message="",
            user_message="fixture_key: pre_registered",
            json_schema={},
            model="fake",
            temperature=0.0,
        )

        assert result["key"] == "value"
