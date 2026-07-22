"""FakeAdapter——简单测试适配器，返回预设响应。

与 FakeLLMAdapter 的区别：
- FakeLLMAdapter：支持多 Fixture 注册，通过 system_message 匹配 task
- FakeAdapter：直接返回 response 字典，不依赖消息内容匹配

集成测试用 FakeAdapter，无需 fixture_key 嵌入。
"""

from __future__ import annotations

import copy
from collections.abc import Mapping

from tianshu_datadev.llm.adapters.base import ProviderAdapter


class FakeAdapter(ProviderAdapter):
    """确定性 Fake 适配器——每次 invoke() 返回预设的 response dict。

    用于集成测试，不需要 fixture 注册或消息匹配。
    每次返回 deepcopy，防止测试间状态泄漏。
    """

    def __init__(self, response: Mapping) -> None:
        """初始化 FakeAdapter。

        Args:
            response: LLM 应返回的预设 dict——每次 invoke() 返回其深拷贝
        """
        self._response = copy.deepcopy(dict(response))

    def invoke(
        self,
        system_message: str,
        user_message: str,
        json_schema: dict,
        model: str,
        temperature: float,
    ) -> dict:
        """返回预设响应（深拷贝）。

        Args:
            system_message: 系统指令（Fake 模式下忽略）
            user_message: 用户消息（Fake 模式下忽略）
            json_schema: JSON Schema（Fake 模式下忽略）
            model: 模型标识（Fake 模式下忽略）
            temperature: LLM 温度（Fake 模式下忽略）

        Returns:
            预设 response 的深拷贝
        """
        return copy.deepcopy(self._response)

    def provider_name(self) -> str:
        """Fake 适配器的 Provider 名称。"""
        return "fake"
