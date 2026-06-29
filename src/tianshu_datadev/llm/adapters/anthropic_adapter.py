"""Anthropic/DeepSeek Adapter——通过 Anthropic Messages API 调用 LLM。

支持 DeepSeek（base_url=https://api.deepseek.com/anthropic）及标准 Anthropic API。
所有调用通过 httpx 同步请求实现——无需额外安装 anthropic SDK。

设计原则：
- invoke() 返回未经 Schema 校验的原始 JSON dict
- Schema 校验由 Gateway 的 _validate_against_schema() 统一完成
- API key 通过环境变量 DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY 获取
"""

from __future__ import annotations

import json
import os

import httpx

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter


class AnthropicAdapter(ProviderAdapter):
    """Anthropic Messages API 适配器——兼容 DeepSeek Anthropic 端点。

    配置优先级：
    1. 构造参数 api_key > 环境变量 DEEPSEEK_API_KEY > ANTHROPIC_API_KEY
    2. 构造参数 base_url > 环境变量 DEEPSEEK_BASE_URL > 默认 DeepSeek 端点
    3. 构造参数 model > 环境变量 DEEPSEEK_MODEL > 默认 deepseek-v4-pro

    用法：
        adapter = AnthropicAdapter()  # 从环境变量自动读取配置
        adapter = AnthropicAdapter(
            api_key="sk-xxx",
            base_url="https://api.deepseek.com/anthropic",
            model="deepseek-v4-pro",
        )
    """

    # 默认 DeepSeek 配置
    _DEFAULT_BASE_URL = "https://api.deepseek.com/anthropic"
    _DEFAULT_MODEL = "deepseek-v4-pro"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout: float = 120.0,
        max_tokens: int = 8192,
    ) -> None:
        """初始化 Anthropic Adapter。

        Args:
            api_key: API 密钥——若为 None，从环境变量读取
            base_url: API 基础 URL——若为 None，从环境变量或默认值读取
            model: 默认模型标识——若为 None，从环境变量或默认值读取
            timeout: HTTP 请求超时（秒）
            max_tokens: 最大输出 token 数
        """
        self._api_key = (
            api_key
            or os.environ.get("DEEPSEEK_API_KEY")
            or os.environ.get("ANTHROPIC_API_KEY")
            or ""
        )
        self._base_url = (
            base_url
            or os.environ.get("DEEPSEEK_BASE_URL")
            or self._DEFAULT_BASE_URL
        ).rstrip("/")
        self._model = (
            model
            or os.environ.get("DEEPSEEK_MODEL")
            or self._DEFAULT_MODEL
        )
        self._timeout = timeout
        self._max_tokens = max_tokens

    def invoke(
        self,
        system_message: str,
        user_message: str,
        json_schema: dict,
        model: str,
        temperature: float,
    ) -> dict:
        """调用 Anthropic Messages API，返回未经 Schema 校验的 JSON dict。

        将 JSON Schema 嵌入 system_message——指导 LLM 输出符合 Schema 的 JSON。
        解析响应中的 JSON 文本块并返回。

        Args:
            system_message: 系统指令（已含模板 system 部分）
            user_message: 用户消息
            json_schema: 目标输出的 JSON Schema
            model: 目标模型标识——若为空字符串，使用默认模型
            temperature: LLM 温度参数

        Returns:
            LLM 返回的 JSON dict——未经任何 Schema 校验

        Raises:
            AdapterError: API 调用失败（网络、认证、超时等）
        """
        if not self._api_key:
            raise AdapterError(
                "AnthropicAdapter API key 未配置——请设置环境变量 "
                "DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY，"
                "或通过构造参数 api_key 传入",
                provider=self.provider_name(),
            )

        # ── 构建带 Schema 约束的 system_message ──
        schema_json = json.dumps(json_schema, ensure_ascii=False, indent=2)
        full_system = (
            f"{system_message}\n\n"
            f"## 输出 Schema 约束\n\n"
            f"你必须输出严格符合以下 JSON Schema 的 JSON 对象：\n\n"
            f"```json\n{schema_json}\n```\n\n"
            f"重要规则：\n"
            f"1. 输出纯 JSON——不要包裹在 Markdown 代码块中\n"
            f"2. 不要添加任何解释文字\n"
            f"3. 所有必填字段必须存在\n"
            f"4. 枚举字段必须使用 Schema 中定义的值\n"
            f"5. 不要添加 Schema 中未定义的额外字段"
        )

        target_model = model or self._model

        # ── 构建 Anthropic Messages API 请求体 ──
        request_body = {
            "model": target_model,
            "max_tokens": self._max_tokens,
            "temperature": temperature,
            "system": full_system,
            "messages": [
                {"role": "user", "content": user_message},
            ],
        }

        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        # ── 发送请求 ──
        try:
            response = httpx.post(
                f"{self._base_url}/v1/messages",
                headers=headers,
                json=request_body,
                timeout=self._timeout,
            )
        except httpx.TimeoutException as e:
            raise AdapterError(
                f"LLM 调用超时（{self._timeout}s）：{e}",
                provider=self.provider_name(),
            ) from e
        except httpx.NetworkError as e:
            raise AdapterError(
                f"LLM 网络错误：{e}",
                provider=self.provider_name(),
            ) from e

        # ── 处理 HTTP 错误 ──
        if response.status_code != 200:
            error_detail = ""
            try:
                error_body = response.json()
                error_detail = error_body.get("error", {}).get("message", str(error_body))
            except Exception:
                error_detail = response.text[:500]
            raise AdapterError(
                f"LLM API 返回错误（status={response.status_code}）：{error_detail}",
                provider=self.provider_name(),
                status_code=response.status_code,
            )

        # ── 解析响应 ──
        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            raise AdapterError(
                f"LLM 响应不是合法 JSON：{e}——原始响应前 500 字符：{response.text[:500]}",
                provider=self.provider_name(),
            ) from e

        # ── 提取文本内容（Anthropic Content Blocks 格式）──
        content_blocks = response_data.get("content", [])
        if not content_blocks:
            raise AdapterError(
                "LLM 响应中 content 为空——模型未返回任何内容",
                provider=self.provider_name(),
            )

        # 合并所有 text 类型的内容块
        text_parts: list[str] = []
        for block in content_blocks:
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                # 部分模型可能返回 tool_use 格式——提取 input 字段
                tool_input = block.get("input", {})
                if isinstance(tool_input, dict) and tool_input:
                    return self._attach_token_usage(tool_input, response_data)

        raw_text = "\n".join(text_parts).strip()

        if not raw_text:
            raise AdapterError(
                "LLM 响应中无文本内容——content 块不含 text 类型",
                provider=self.provider_name(),
            )

        # ── 提取 JSON ──
        parsed_json = self._extract_json(raw_text)

        # ── 附加 token 用量 ──
        return self._attach_token_usage(parsed_json, response_data)

    def provider_name(self) -> str:
        """返回 Provider 名称标识。"""
        return "anthropic"

    # ── 内部方法 ──

    def _extract_json(self, raw_text: str) -> dict:
        """从 LLM 原始文本中提取 JSON 对象。

        处理常见的 LLM 输出格式：
        1. 纯 JSON 文本
        2. Markdown 代码块包裹（```json ... ``` 或 ``` ... ```）
        3. 文本中嵌入的 JSON 对象

        Args:
            raw_text: LLM 原始输出文本

        Returns:
            解析后的 JSON dict

        Raises:
            AdapterError: 无法提取或解析 JSON
        """
        # 策略 1：Markdown 代码块中的 JSON
        if "```json" in raw_text:
            # 找到 ```json 和对应的 ```
            start = raw_text.index("```json") + 7
            end = raw_text.index("```", start)
            json_str = raw_text[start:end].strip()
        elif "```" in raw_text:
            # 找到 ``` 和对应的 ```
            start = raw_text.index("```") + 3
            end = raw_text.index("```", start)
            json_str = raw_text[start:end].strip()
        else:
            json_str = raw_text.strip()

        # 尝试查找 JSON 对象的起止边界（处理文本前后有非 JSON 内容的情况）
        if not json_str.startswith("{"):
            brace_start = json_str.find("{")
            if brace_start >= 0:
                brace_end = json_str.rfind("}")
                if brace_end > brace_start:
                    json_str = json_str[brace_start:brace_end + 1]

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise AdapterError(
                f"LLM 输出不是合法 JSON：{e}——"
                f"原始输出前 800 字符：{raw_text[:800]}",
                provider=self.provider_name(),
            ) from e

    @staticmethod
    def _attach_token_usage(parsed: dict, response_data: dict) -> dict:
        """将 Anthropic usage 信息附加到返回 dict 的 _token_usage 字段。

        Args:
            parsed: 解析后的 JSON dict
            response_data: Anthropic API 原始响应

        Returns:
            附加了 _token_usage 的 dict
        """
        usage = response_data.get("usage", {})
        if usage:
            parsed["_token_usage"] = {
                "prompt_tokens": usage.get("input_tokens", 0),
                "completion_tokens": usage.get("output_tokens", 0),
                "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            }
        return parsed
