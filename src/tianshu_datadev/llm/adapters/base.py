"""LLM Provider 抽象适配器接口——支持 Fake、OpenAI、Anthropic 等多种后端。

所有 Provider 必须实现 ProviderAdapter.invoke()——
返回 LLM 输出的原始 dict（未经 Schema 校验），
校验由 Gateway 的 Pydantic model_validate 完成。
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class AdapterError(Exception):
    """LLM Adapter 层错误——网络故障、认证失败、超时、未知输入等。

    与 ValidationError（Schema 层）明确区分：
    - AdapterError → LLM 调用本身失败（不可恢复，或可重试）
    - ValidationError → LLM 调用成功但输出不合 Schema（可重试或拒绝）
    """

    def __init__(
        self,
        message: str,
        provider: str = "",
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.status_code = status_code


class ProviderAdapter(ABC):
    """LLM Provider 抽象接口——所有 LLM 后端必须实现此接口。

    设计原则：
    - invoke() 只负责调用 LLM 并返回原始 JSON dict
    - 不执行任何 Schema 校验——校验由 Gateway 统一完成
    - 不支持自由 Prompt——Prompt 由 PromptManager 管理
    - 不支持自由 SQL 生成——LLM 只输出结构化对象

    实现类：
    - FakeLLMAdapter：确定性测试适配器（pytest）
    - OpenAiAdapter（Phase 7）：OpenAI 兼容 API
    - AnthropicAdapter（Phase 7）：Anthropic API
    """

    @abstractmethod
    def invoke(
        self,
        system_message: str,
        user_message: str,
        json_schema: dict,
        model: str,
        temperature: float,
    ) -> dict:
        """调用 LLM Provider，返回未经 Schema 校验的 JSON dict。

        Args:
            system_message: 系统指令（Prompt 模板的 system 部分）
            user_message: 用户消息（已渲染 artifact 引用的模板）
            json_schema: 目标输出的 JSON Schema（传给 LLM 做 function calling）
            model: 目标模型标识
            temperature: LLM 温度参数

        Returns:
            LLM 返回的 JSON dict——未经任何 Schema 校验

        Raises:
            AdapterError: LLM 调用失败（网络、认证、超时等）
        """
        ...

    @abstractmethod
    def provider_name(self) -> str:
        """返回 Provider 名称标识。

        Returns:
            "fake" | "openai" | "anthropic"
        """
        ...
