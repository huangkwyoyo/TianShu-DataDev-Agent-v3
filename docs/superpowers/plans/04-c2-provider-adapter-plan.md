# C2 ProviderAdapter 接入实施方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标：** 完成 LLM ProviderAdapter 接入，使 `SparkDeveloperService.annotate()` 可调用真实 LLM 产出 `AnnotatedSparkPlan`，并通过 `AnnotationValidator` 校验。

**基线：** C1 已点亮（11/11 真实 Spark 通过），Developer mock 测试 10/10 全绿，`llm_call` 注入接口已就绪。

---

## 全局约束

- **允许**：定义 ProviderAdapter 基类/协议（纯结构，不含 LLM 调用）
- **允许**：实现 Anthropic Claude ProviderAdapter（需 API key 由调用方注入，不允许硬编码）
- **允许**：完善 `SparkDeveloperService` 的错误处理和重试逻辑
- **禁止**：自由 LLM 代码生成——Prompt 构造必须走 `SparkDeveloperService._build_prompt()`
- **禁止**：在 ProviderAdapter 中引入新的 Prompt 模板或绕过 `AnnotationValidator`
- **禁止**：修改 SQL/Spark 安全边界、Schema/Memory/Prompt 机制
- **C3/C4**：只保持登记，不在本轮实现

---

## 能力清单

### ✅ 已就绪

| 能力 | 证据 |
|------|------|
| `SparkDeveloperService` 接口 | `llm_call: Callable[[SparkPlan], AnnotatedSparkPlan]` 注入模式 |
| `AnnotationValidator` | 3 规则校验（数量 / step_id 合法 / 无重复），测试覆盖 |
| `_build_prompt()` 安全 | 6 个 Prompt 安全测试全绿——不含 SQL/DeveloperSpec/markdown |
| Mock 测试 | 10/10 全绿——构造/标注/校验/异常路径全覆盖 |
| Orchestrator 集成 | 无 llm_call 时 DEVELOPER 阶段标记 SKIPPED |

### ⚠️ 本轮要做

| 项目 | 类型 | 说明 |
|------|------|------|
| ProviderAdapter 基类 | 接口定义 | Protocol/ABC——定义 `__call__` 签名 + 配置模型 |
| AnthropicAdapter | 实现 | 封装 `anthropic.Anthropic` → StructuredOutput `AnnotatedSparkPlan` |
| 重试 + 错误处理 | 加固 | LLM 调用失败 → 重试 1 次 → 仍失败 → HUMAN_REVIEW |
| 集成测试 | 测试 | mock LLM 响应（不含真实 API key）验证全链路 |

### ❌ 本轮不做

| 项目 | 原因 |
|------|------|
| OpenAI / vLLM Adapter | 按需扩展——基类接口就绪后可按相同模式添加 |
| 真实 API key 硬编码 | 安全红线——API key 由环境变量或调用方注入 |
| C3 Comparator 实现 | 需 SQL pipeline 先就绪 |
| C4 Harness 样本 | 需业务方提供 |

---

## 架构分析

### 当前数据流

```
SparkPlan → SparkDeveloperService.annotate()
              ├── self._llm_call(spark_plan)    ← 当前是 mock callable
              └── self._validator.validate()     ← 确定性校验
           → AnnotatedSparkPlan
```

### 目标数据流

```
SparkPlan → SparkDeveloperService.annotate()
              ├── self._build_prompt(spark_plan)           ← 现有方法
              ├── self._adapter(prompt, schema)             ← 新增：ProviderAdapter
              │     ├── AnthropicAdapter.__call__()
              │     │     ├── anthropic.messages.create()   ← StructuredOutput
              │     │     └── parse → AnnotatedSparkPlan
              │     └── 失败 → 重试 1 次 → HUMAN_REVIEW
              └── self._validator.validate()
           → AnnotatedSparkPlan
```

### ProviderAdapter 设计

**核心决策：ProviderAdapter 接收已构造好的 Prompt + Schema，返回 AnnotatedSparkPlan。不对 Prompt 做任何修改。**

```python
# 基类——Protocol 定义
class ProviderAdapter(Protocol):
    """LLM Provider 适配器协议——接收构造好的 Prompt，返回 AnnotatedSparkPlan。"""
    def __call__(self, prompt: str, schema: type[AnnotatedSparkPlan]) -> AnnotatedSparkPlan: ...

# Anthropic 实现
class AnthropicAdapter:
    """Anthropic Claude StructuredOutput 适配器。"""
    def __init__(self, api_key: str, model: str = "claude-sonnet-5"): ...
    def __call__(self, prompt: str, schema: type[AnnotatedSparkPlan]) -> AnnotatedSparkPlan: ...
```

**为什么 ProviderAdapter 接收 Prompt 而非 SparkPlan：**
1. Prompt 构造逻辑已在 `SparkDeveloperService._build_prompt()` 中集中管理
2. ProviderAdapter 不应知道 SparkPlan 的内部结构——单一职责
3. 更换 Provider 时 Prompt 无需改变——解耦

**为什么保持 Callable 模式：**
1. 现有 `llm_call: Callable[[SparkPlan], AnnotatedSparkPlan]` 接口已工作——mock 测试全绿
2. ProviderAdapter 是 `(prompt, schema) → AnnotatedSparkPlan`——外层由 Developer 封装为 `(SparkPlan) → AnnotatedSparkPlan`
3. 不破坏现有接口——向后兼容

---

## Task 1: ProviderAdapter 基类定义

**文件：**
- 创建：`src/tianshu_datadev/spark/provider_adapter.py`

**说明：** 定义 ProviderAdapter 协议 + 配置模型 + 错误类型。纯结构，不含 LLM 调用。

### Step 1: 写入 ProviderAdapter 基类

```python
"""Phase 8 ProviderAdapter——LLM Provider 抽象适配层。

定义 ProviderAdapter 协议、配置模型和错误类型。
具体实现（Anthropic/OpenAI/本地模型）按需扩展。
"""

from __future__ import annotations

from typing import Protocol

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.annotations import AnnotatedSparkPlan


# ════════════════════════════════════════════
# ProviderConfig——LLM Provider 配置
# ════════════════════════════════════════════


class ProviderConfig(StrictModel):
    """LLM Provider 配置——由调用方注入，不硬编码凭据。"""

    provider: str = "anthropic"          # anthropic / openai / vllm
    model: str = "claude-sonnet-5"       # 模型 ID
    api_key: str = ""                     # API key——由环境变量或调用方注入
    base_url: str = ""                    # 自定义 endpoint（vLLM/Ollama 使用）
    max_tokens: int = 4096
    timeout_seconds: float = 60.0


# ════════════════════════════════════════════
# ProviderAdapter——抽象协议
# ════════════════════════════════════════════


class ProviderAdapter(Protocol):
    """LLM Provider 适配器协议。

    接收已构造好的 Prompt + 输出 Schema，返回 AnnotatedSparkPlan。
    不对 Prompt 做任何修改——Prompt 构造由 SparkDeveloperService 负责。

    使用方式：
        adapter: ProviderAdapter = AnthropicAdapter(config)
        result = adapter(prompt, AnnotatedSparkPlan)
    """

    def __call__(
        self,
        prompt: str,
        output_schema: type[AnnotatedSparkPlan],
    ) -> AnnotatedSparkPlan:
        """调用 LLM 并返回结构化标注结果。

        Args:
            prompt: 已构造好的 Prompt 字符串（由 SparkDeveloperService._build_prompt() 产出）
            output_schema: 输出的 Pydantic 模型类（AnnotatedSparkPlan）

        Returns:
            通过 StructuredOutput 产出的 AnnotatedSparkPlan

        Raises:
            ProviderError: LLM 调用失败时抛出
        """
        ...


# ════════════════════════════════════════════
# ProviderError——LLM 调用异常
# ════════════════════════════════════════════


class ProviderError(Exception):
    """LLM Provider 调用异常——含重试建议。"""

    def __init__(
        self,
        message: str,
        retryable: bool = True,
        provider: str = "",
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.provider = provider
        self.status_code = status_code
```

### Step 2: 验证

```bash
python -c "from tianshu_datadev.spark.provider_adapter import ProviderAdapter, ProviderConfig, ProviderError; print('OK')"
```

---

## Task 2: AnthropicAdapter 实现

**文件：**
- 创建：`src/tianshu_datadev/spark/adapter_anthropic.py`

**说明：** 实现 Anthropic Claude StructuredOutput 适配器。使用 `anthropic` SDK 的 `messages.create()` + `tools` 模式。

### Step 1: 写入 AnthropicAdapter

```python
"""Anthropic Claude ProviderAdapter——StructuredOutput 实现。

使用 anthropic SDK 的 messages.create() + tools 模式产出 AnnotatedSparkPlan。
"""

from __future__ import annotations

import json
import logging

from tianshu_datadev.spark.annotations import AnnotatedSparkPlan
from tianshu_datadev.spark.provider_adapter import (
    ProviderAdapter,
    ProviderConfig,
    ProviderError,
)

logger = logging.getLogger(__name__)


class AnthropicAdapter:
    """Anthropic Claude StructuredOutput 适配器。

    使用 anthropic SDK 调用 Claude，通过 tools 模式强制产出结构化 JSON，
    解析为 AnnotatedSparkPlan 后返回。

    使用方式：
        config = ProviderConfig(
            provider="anthropic",
            model="claude-sonnet-5",
            api_key=os.environ["ANTHROPIC_API_KEY"],
        )
        adapter = AnthropicAdapter(config)
        result = adapter(prompt, AnnotatedSparkPlan)
    """

    def __init__(self, config: ProviderConfig) -> None:
        """初始化 Anthropic 适配器。

        Args:
            config: ProviderConfig——含 api_key、model 等配置

        Raises:
            ProviderError: api_key 为空时抛出
        """
        if not config.api_key:
            raise ProviderError(
                "Anthropic API key 为空——请设置 ANTHROPIC_API_KEY 环境变量",
                retryable=False,
                provider="anthropic",
            )
        self._config = config
        # 延迟导入——避免未安装 anthropic 时阻塞模块加载
        self._client: object | None = None

    @property
    def _ensure_client(self) -> object:
        """延迟初始化 anthropic client——首次调用时才 import。"""
        if self._client is None:
            try:
                import anthropic  # noqa: F811
                self._client = anthropic.Anthropic(api_key=self._config.api_key)
            except ImportError as exc:
                raise ProviderError(
                    "anthropic SDK 未安装——运行 pip install anthropic",
                    retryable=False,
                    provider="anthropic",
                ) from exc
        return self._client

    def __call__(
        self,
        prompt: str,
        output_schema: type[AnnotatedSparkPlan],
    ) -> AnnotatedSparkPlan:
        """调用 Anthropic Claude 产出结构化标注。

        流程：
        1. 构造 Anthropic message（system + user prompt）
        2. 调用 messages.create() with tools 强制 StructuredOutput
        3. 解析 tool_use 中的 JSON → AnnotatedSparkPlan
        4. 校验返回对象结构合法（Pydantic 自动校验）

        Args:
            prompt: 已构造好的 Prompt 字符串
            output_schema: AnnotatedSparkPlan 类（用于 JSON Schema 生成）

        Returns:
            通过校验的 AnnotatedSparkPlan

        Raises:
            ProviderError: API 调用失败 / 响应解析失败
        """
        client = self._ensure_client

        # 生成 AnnotatedSparkPlan 的 JSON Schema
        json_schema = output_schema.model_json_schema()

        try:
            message = client.messages.create(
                model=self._config.model,
                max_tokens=self._config.max_tokens,
                system="你是一个 Spark 数据处理管线的语义标注器。请严格按照输出的 JSON Schema 返回结构化结果。",
                messages=[{"role": "user", "content": prompt}],
                tools=[
                    {
                        "name": "output_annotated_plan",
                        "description": "返回标注后的 AnnotatedSparkPlan 结构化结果",
                        "input_schema": json_schema,
                    }
                ],
                tool_choice={"type": "tool", "name": "output_annotated_plan"},
            )
        except Exception as exc:
            raise ProviderError(
                f"Anthropic API 调用失败: {exc}",
                retryable=True,
                provider="anthropic",
            ) from exc

        # 解析 tool_use 中的 JSON
        try:
            tool_use = next(
                block for block in message.content
                if getattr(block, "type", None) == "tool_use"
                and getattr(block, "name", None) == "output_annotated_plan"
            )
            result = output_schema.model_validate(tool_use.input)
        except (StopIteration, Exception) as exc:
            raise ProviderError(
                f"Anthropic 响应解析失败: {exc}",
                retryable=False,
                provider="anthropic",
            ) from exc

        return result
```

### Step 2: 验证

```bash
python -c "
from tianshu_datadev.spark.provider_adapter import ProviderConfig
from tianshu_datadev.spark.adapter_anthropic import AnthropicAdapter
config = ProviderConfig(api_key='test-key', model='claude-sonnet-5')
adapter = AnthropicAdapter(config)
print('AnthropicAdapter 构造成功')
"
```

---

## Task 3: SparkDeveloperService 集成 + 重试逻辑

**文件：**
- 修改：`src/tianshu_datadev/spark/developer.py`——增加 `from_prompt_adapter()` 工厂方法

**说明：** 不改现有 `__init__` 签名——新增类方法将 ProviderAdapter 封装为 `llm_call` 注入模式。

### Step 1: 在 SparkDeveloperService 中添加 `from_prompt_adapter()` 方法

修改 `developer.py`，在 `__init__` 之后添加工厂方法：

```python
@classmethod
def from_provider_adapter(
    cls,
    adapter: "ProviderAdapter",
    max_llm_retries: int = 1,
) -> "SparkDeveloperService":
    """从 ProviderAdapter 创建 SparkDeveloperService 实例。

    将 ProviderAdapter(prompt, schema) 封装为 llm_call(spark_plan) 签名，
    使其兼容现有注入模式。内部处理重试逻辑。

    Args:
        adapter: ProviderAdapter 实例（如 AnthropicAdapter(config)）
        max_llm_retries: LLM 调用失败时最大重试次数（默认 1 次）

    Returns:
        SparkDeveloperService——llm_call 已注入为适配器封装函数
    """
    def _adapter_llm_call(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
        """将 ProviderAdapter 封装为 llm_call 签名。

        内部处理：
        1. Prompt 构造（_build_prompt）
        2. LLM 调用（adapter）
        3. 重试逻辑（最多 max_llm_retries 次）
        """
        prompt = cls._build_prompt_static(spark_plan)
        last_error: Exception | None = None

        for attempt in range(max_llm_retries + 1):
            try:
                return adapter(prompt, AnnotatedSparkPlan)
            except Exception as exc:
                last_error = exc
                if attempt < max_llm_retries:
                    # 只对可重试错误重试
                    retryable = getattr(exc, "retryable", True)
                    if not retryable:
                        raise
                    logger.warning(
                        f"LLM 调用失败（第 {attempt + 1}/{max_llm_retries + 1} 次）"
                        f"，重试中: {exc}"
                    )
                    continue
                raise

        # 不应到达此处
        raise last_error  # type: ignore[misc]

    # 创建临时实例（绕过 __init__ 的 None 检查）
    instance = object.__new__(cls)
    instance._llm_call = _adapter_llm_call
    instance._validator = cls()._validator if hasattr(cls, '_validator') else AnnotationValidator()
    return instance

@staticmethod
def _build_prompt_static(spark_plan: SparkPlan) -> str:
    """_build_prompt 的静态版本——供 from_provider_adapter 使用。"""
    svc = SparkDeveloperService(llm_call=lambda _: None)  # 仅用于访问实例方法
    return svc._build_prompt(spark_plan)
```

**更好的替代方案**（避免 hack）：

直接将 `_build_prompt` 改为静态方法，`__init__` 改为可选 llm_call（保留 ValueError 但提供工厂方法绕过）：

```python
@classmethod
def from_provider_adapter(
    cls,
    adapter: "ProviderAdapter",
    max_llm_retries: int = 1,
) -> "SparkDeveloperService":
    """从 ProviderAdapter 创建 SparkDeveloperService 实例。"""
    import logging
    _logger = logging.getLogger(__name__)

    def _adapter_llm_call(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
        prompt = SparkDeveloperService._build_prompt_static(spark_plan)
        last_error: Exception | None = None

        for attempt in range(max_llm_retries + 1):
            try:
                return adapter(prompt, AnnotatedSparkPlan)
            except Exception as exc:
                last_error = exc
                if attempt < max_llm_retries:
                    retryable = getattr(exc, "retryable", True)
                    if not retryable:
                        raise
                    _logger.warning(
                        f"LLM 调用失败（第 {attempt + 1}/{max_llm_retries + 1} 次），"
                        f"重试中: {exc}"
                    )
                    continue
                raise

        raise last_error  # type: ignore[misc]

    return cls(llm_call=_adapter_llm_call)
```

同时将 `_build_prompt` 改为 `@staticmethod`（它只读 `spark_plan`，不读 `self`）：

```python
@staticmethod
def _build_prompt(spark_plan: SparkPlan) -> str:
    """...(方法体不变)"""
```

### Step 2: 验证

```bash
python -m pytest tests/spark/test_spark_developer.py -v --tb=short
# 预期：10 passed——现有 mock 测试全绿，向后兼容
```

---

## Task 4: ProviderAdapter 集成测试

**文件：**
- 修改：`tests/spark/test_spark_developer.py`——新增 `TestProviderAdapterIntegration` 类

**说明：** 不调真实 LLM——使用 mock ProviderAdapter（返回确定性 AnnotatedSparkPlan）验证集成路径。

### Step 1: 新增测试类

```python
class TestProviderAdapterIntegration:
    """ProviderAdapter → SparkDeveloperService 集成路径。"""

    def test_from_provider_adapter_creates_service(self):
        """from_provider_adapter() 创建可用实例。"""
        from tianshu_datadev.spark.adapter_anthropic import AnthropicAdapter
        from tianshu_datadev.spark.provider_adapter import ProviderConfig

        config = ProviderConfig(api_key="test-key", model="claude-sonnet-5")
        adapter = AnthropicAdapter(config)
        svc = SparkDeveloperService.from_provider_adapter(adapter)
        assert svc is not None

    def test_adapter_integration_with_mock_llm(self):
        """ProviderAdapter 注入后 annotate() 正常产出——模拟真实 LLM 路径。"""
        class MockAdapter:
            """模拟 ProviderAdapter——返回确定性 AnnotatedSparkPlan。"""
            def __call__(self, prompt: str, output_schema: type) -> AnnotatedSparkPlan:
                plan = _make_simple_plan()
                return _mock_llm_annotate(plan)  # 复用已有 mock

        adapter = MockAdapter()
        svc = SparkDeveloperService.from_provider_adapter(adapter)
        plan = _make_simple_plan()
        result = svc.annotate(plan)

        assert isinstance(result, AnnotatedSparkPlan)
        assert len(result.annotations) == len(plan.steps)

    def test_adapter_retry_on_failure(self):
        """LLM 调用首次失败后重试——重试成功则正常返回。"""
        call_count = [0]

        class RetryAdapter:
            def __call__(self, prompt: str, output_schema: type) -> AnnotatedSparkPlan:
                call_count[0] += 1
                if call_count[0] < 2:
                    raise RuntimeError("模拟临时故障")
                plan = _make_simple_plan()
                return _mock_llm_annotate(plan)

        adapter = RetryAdapter()
        svc = SparkDeveloperService.from_provider_adapter(adapter, max_llm_retries=1)
        plan = _make_simple_plan()
        result = svc.annotate(plan)

        assert call_count[0] == 2  # 首次失败 + 1 次重试成功
        assert isinstance(result, AnnotatedSparkPlan)

    def test_adapter_exhausts_retries_raises(self):
        """重试耗尽后仍然失败——抛出异常。"""
        class AlwaysFailAdapter:
            def __call__(self, prompt: str, output_schema: type) -> AnnotatedSparkPlan:
                raise RuntimeError("模拟永久故障")

        adapter = AlwaysFailAdapter()
        svc = SparkDeveloperService.from_provider_adapter(adapter, max_llm_retries=1)
        plan = _make_simple_plan()

        with pytest.raises(RuntimeError, match="模拟永久故障"):
            svc.annotate(plan)

    def test_adapter_non_retryable_error_raises_immediately(self):
        """不可重试错误立即抛出——不浪费重试次数。"""
        from tianshu_datadev.spark.provider_adapter import ProviderError

        class NonRetryableAdapter:
            def __call__(self, prompt: str, output_schema: type) -> AnnotatedSparkPlan:
                raise ProviderError("API key 无效", retryable=False, provider="test")

        adapter = NonRetryableAdapter()
        svc = SparkDeveloperService.from_provider_adapter(adapter, max_llm_retries=2)
        plan = _make_simple_plan()

        with pytest.raises(ProviderError, match="API key 无效"):
            svc.annotate(plan)
```

### Step 2: 验证

```bash
# ProviderAdapter 集成测试（mock 路径，不调真实 LLM）
python -m pytest tests/spark/test_spark_developer.py -v --tb=short

# 全量回归
python -m pytest tests/spark/ tests/artifacts/ -q

# Lint
python -m ruff check src/tianshu_datadev/spark/ tests/spark/
```

---

## 验收命令（本轮完成后）

```bash
# 1. C1 环境验证（已点亮，每次回归可重复执行）
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[1]').getOrCreate(); print('SPARK_OK', spark.version); spark.stop()"

# 2. ProviderAdapter 结构验证
python -c "from tianshu_datadev.spark.provider_adapter import ProviderAdapter, ProviderConfig, ProviderError; print('ProviderAdapter 定义 OK')"
python -c "from tianshu_datadev.spark.adapter_anthropic import AnthropicAdapter; print('AnthropicAdapter 可导入')"

# 3. Developer 全量测试（mock + 集成）
python -m pytest tests/spark/test_spark_developer.py -v --tb=short

# 4. Orchestrator 测试
python -m pytest tests/spark/test_orchestrator.py -v --tb=short

# 5. 物理验证（C1 已点亮）
python -m pytest tests/spark/test_physical_verifier.py -v --run-slow --tb=short

# 6. 全量回归
python -m pytest tests/spark/ tests/artifacts/ -q

# 7. Lint
python -m ruff check src/tianshu_datadev/spark/ tests/spark/ docs/risks/

# 8. Git diff
git diff --check
```

---

## 退出标准

- [ ] `src/tianshu_datadev/spark/provider_adapter.py` 已创建——ProviderAdapter 协议 + ProviderConfig + ProviderError
- [ ] `src/tianshu_datadev/spark/adapter_anthropic.py` 已创建——AnthropicAdapter 实现（不含真实 API key）
- [ ] `SparkDeveloperService.from_provider_adapter()` 已添加——封装 + 重试逻辑
- [ ] ProviderAdapter 集成测试 5/5 通过（mock 路径）
- [ ] 现有 Developer mock 测试 10/10 全绿（向后兼容）
- [ ] 全量回归无退化
- [ ] ruff 零告警

### 通过后是否可进入 C3/C4？

**C2 完成后的状态：**
- ✅ C1 已点亮（11/11 真实 Spark 物理验证通过）
- ✅ C2 接入方案已制定并基本实现（ProviderAdapter 结构 + AnthropicAdapter 骨架 + 集成路径）
- ⚠️ C2 真实 LLM 调试验证需要 API key（由业务方提供时执行 `test_spark_developer.py` 含真实 LLM 的标记测试）

**C3 的前置条件是 SQL pipeline 的 SqlBuildPlan 产出能力——不在本项目的 Spark 路径范围内。**
**C4 的前置条件是业务方提供至少 5 个业务样本（每维度 1 个）。**

进入 C3/C4 前需确认：
1. C2 真实 LLM 验证是否已完成（API key 已配置 + `annotate()` 产出合法标注）
2. SQL pipeline 是否已就绪（供 C3 Comparator 使用）
3. 业务样本是否已准备（供 C4 Harness 使用）

---

## A/B/C 分类汇总

| 分类 | 内容 | 处置 |
|------|------|------|
| **A（本轮实现）** | ProviderAdapter 基类定义 | Task 1——纯协议，不含 LLM 调用 |
| **A（本轮实现）** | AnthropicAdapter 实现 | Task 2——含 LLM 调用逻辑但凭据外部注入 |
| **A（本轮实现）** | SparkDeveloperService 集成 + 重试 | Task 3——`from_provider_adapter()` 工厂方法 |
| **A（本轮实现）** | ProviderAdapter 集成测试 | Task 4——mock 路径，不调真实 LLM |
| **B（待环境就绪）** | 真实 LLM 验证 | 需 API key——在 `@pytest.mark.llm` 标记测试中执行 |
| **C（按需扩展）** | OpenAI / vLLM Adapter | 基类接口就绪后可按 AnthropicAdapter 模式添加 |

---

## 非技术人员解释

**C2 做了什么？**

Spark 管线上有一个"AI 大脑"（Developer），它读取数据加工步骤后给每一步打上语义标签——"这一步在做什么？读数据还是过滤？有没有可疑的地方？"

之前这个大脑只接了"模拟器"（mock），返回预设的标签——能跑通，但不是真的 AI。

现在要做的是给这个大脑接上真正的"电源"——比如 Anthropic 的 Claude 模型。关键是：

1. **接口留好了**：大脑的电源接口是标准化的（`llm_call` 参数），插什么电源都行
2. **安全闸在**：无论 AI 返回什么，都会经过"安检"（`AnnotationValidator`）——数量对不对、ID 合不合法、有没有重复——不合格的直接拒绝
3. **断电能重连**：AI 调用失败了会自动重试一次，还不行就标记"需人工审查"，不阻塞其他机器继续跑
4. **不把钥匙写死**：AI 的 API key 由使用方通过环境变量注入，代码里不硬编码

**为什么不一锅端做完 C3 和 C4？**

- C3（双线对比）：需要隔壁 SQL 车间先完工——SQL 车间不归我们管，完工了直接对接即可
- C4（5 维度质检）：需要业务方提供真实的"产品样本"——没有样本就没法做质检，这和代码无关
