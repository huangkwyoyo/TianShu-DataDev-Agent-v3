"""Phase 4A LLM Gateway——统一 LLM 调用入口 + Prompt 版本管理。

本包提供：
- LLMGateway：LLM 调用统一入口——Prompt → Adapter → Schema 校验 → 引用
- ProviderAdapter（ABC）：LLM Provider 抽象接口
- FakeLLMAdapter：确定性测试适配器（pytest）
- LlmRequest / LlmResponse / SchemaBinding / ArtifactRef：核心数据模型

设计约束（来自 AGENTS.md §2）：
- LLM 只输出严格的 Pydantic 结构化对象——绝不生成 SQL 文本
- Gateway 只返回结构化对象引用和校验状态
- validation_status="invalid" 的响应不进入编译链路
- 真实 LLM 调用不进入 pytest 必需路径
"""

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.llm.gateway import LLMGateway
from tianshu_datadev.llm.models import (
    ArtifactRef,
    LlmRequest,
    LlmResponse,
    PromptVersion,
    SchemaBinding,
)

__all__ = [
    "AdapterError",
    "ArtifactRef",
    "FakeLLMAdapter",
    "LLMGateway",
    "LlmRequest",
    "LlmResponse",
    "PromptVersion",
    "ProviderAdapter",
    "SchemaBinding",
]
