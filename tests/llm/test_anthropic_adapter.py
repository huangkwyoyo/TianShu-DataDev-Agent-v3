"""AnthropicAdapter 测试——Provider 接口实现、JSON 提取、错误处理。

验证：
1. 实现 ProviderAdapter 接口（provider_name、invoke 签名）
2. JSON 提取——纯 JSON / Markdown 代码块 / 嵌入文本
3. API key 缺失时抛出 AdapterError
4. Token usage 附加
"""

from __future__ import annotations

import json

import pytest

from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter
from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter


class TestAnthropicAdapterInterface:
    """验证 AnthropicAdapter 正确实现 ProviderAdapter 接口。"""

    def test_is_provider_adapter(self):
        """AnthropicAdapter 是 ProviderAdapter 的子类。"""
        assert issubclass(AnthropicAdapter, ProviderAdapter)

    def test_provider_name(self):
        """provider_name 返回 "anthropic"。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        assert adapter.provider_name() == "anthropic"

    def test_default_base_url_is_deepseek(self):
        """默认 base_url 指向 DeepSeek Anthropic 端点。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        assert "deepseek.com" in adapter._base_url
        assert "anthropic" in adapter._base_url

    def test_default_model_is_v4_pro(self):
        """默认模型为 deepseek-v4-pro。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        assert adapter._model == "deepseek-v4-pro"


class TestAnthropicAdapterJSONExtraction:
    """验证 _extract_json 的各种输入格式。"""

    def test_extract_pure_json(self):
        """纯 JSON 文本——直接解析。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        result = adapter._extract_json('{"key": "value", "num": 42}')
        assert result == {"key": "value", "num": 42}

    def test_extract_markdown_json_block(self):
        """Markdown ```json ... ``` 代码块。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        raw = 'Some text\n```json\n{"name": "test", "count": 5}\n```\nMore text'
        result = adapter._extract_json(raw)
        assert result == {"name": "test", "count": 5}

    def test_extract_markdown_code_block(self):
        """Markdown ``` ... ``` 代码块（无 json 标签）。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        raw = 'Output:\n```\n{"status": "ok"}\n```\nDone.'
        result = adapter._extract_json(raw)
        assert result == {"status": "ok"}

    def test_extract_embedded_json(self):
        """文本中嵌入的 JSON 对象——按 {} 边界提取。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        raw = 'Here is the result: {"data": [1, 2, 3]} End of output.'
        result = adapter._extract_json(raw)
        assert result == {"data": [1, 2, 3]}

    def test_extract_nested_json(self):
        """嵌套 JSON 对象——正确解析嵌套结构。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        data = {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], "total": 2}
        raw = json.dumps(data)
        result = adapter._extract_json(raw)
        assert result == data

    def test_extract_invalid_json_raises(self):
        """非法 JSON 抛出 AdapterError。"""
        adapter = AnthropicAdapter(api_key="sk-test")
        with pytest.raises(AdapterError, match="不是合法 JSON"):
            adapter._extract_json("This is not JSON at all")


class TestAnthropicAdapterTokenUsage:
    """验证 _attach_token_usage 正确附加 token 信息。"""

    def test_attach_usage_to_empty_dict(self):
        """空 dict 附加 usage。"""
        result = AnthropicAdapter._attach_token_usage(
            {}, {"usage": {"input_tokens": 100, "output_tokens": 50}}
        )
        assert result["_token_usage"] == {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
        }

    def test_attach_usage_preserves_original(self):
        """附加 usage 不覆盖原有字段。"""
        original = {"key": "value"}
        result = AnthropicAdapter._attach_token_usage(
            original,
            {"usage": {"input_tokens": 10, "output_tokens": 20}},
        )
        assert result["key"] == "value"
        assert result["_token_usage"]["total_tokens"] == 30

    def test_no_usage_when_missing(self):
        """无 usage 字段时不附加 _token_usage。"""
        result = AnthropicAdapter._attach_token_usage({"key": "val"}, {})
        assert "_token_usage" not in result


class TestAnthropicAdapterErrorHandling:
    """验证错误处理路径。"""

    def test_missing_api_key_raises_on_invoke(self, monkeypatch):
        """API key 为空时 invoke() 抛出 AdapterError——隔离本机环境变量。

        使用 monkeypatch.delenv 清除 DEEPSEEK_API_KEY，
        避免本机已配置的 key 干扰测试。构造参数 api_key="" 为 falsy，
        AnthropicAdapter.__init__ 将回退到环境变量——需确保其为空。
        """
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        adapter = AnthropicAdapter(api_key="")
        with pytest.raises(AdapterError, match="API key"):
            adapter.invoke(
                system_message="test",
                user_message="test",
                json_schema={},
                model="",
                temperature=0.0,
            )
