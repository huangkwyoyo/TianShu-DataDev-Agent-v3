"""LLM Provider 适配器包——抽象接口 + Fake 实现。

公开导出：
- ProviderAdapter（ABC）——所有 Provider 的抽象基类
- AdapterError——LLM 调用层异常
- FakeLLMAdapter——确定性测试适配器
"""

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter

__all__ = [
    "AdapterError",
    "FakeLLMAdapter",
    "ProviderAdapter",
]
