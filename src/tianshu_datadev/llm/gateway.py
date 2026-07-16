"""LLMGateway——LLM 调用统一入口。

所有 LLM 交互必须通过此 Gateway——不接受自由 Prompt，不将原始文本传入 Compiler。
Gateway 仅返回结构化对象引用和校验状态。

核心保证：
1. Prompt 仅从 PromptManager 加载——不接受自由 Prompt 文本
2. 输出经过 Pydantic Schema 校验（model_validate）
3. 校验通过后原子写入 response_root——parsed_json_ref 指向落盘文件
4. validation_status != "valid" 的响应不返回结构化对象
5. LLM 原始文本落盘为引用，绝不进入 Compiler
6. 所有错误路径返回 LlmResponse（不抛异常）——便于上层统一处理
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter
from tianshu_datadev.llm.models import (
    LlmRequest,
    LlmResponse,
    SchemaBinding,
)

if TYPE_CHECKING:
    # 仅用于类型注解——运行时不需要实际类。
    # 放在 TYPE_CHECKING 中打破 llm.gateway ↔ prompts.manager 循环导入。
    from tianshu_datadev.prompts.manager import PromptManager


class LLMGateway:
    """LLM 调用统一入口——所有 LLM 交互必须通过此 Gateway。

    工作流程：
    1. 校验 LlmRequest 合法性（task 存在、version 存在）
    2. 加载 Prompt 模板 → 渲染 user_message（含 extra_vars）
    3. 调用 ProviderAdapter.invoke()
    4. 解析 JSON → Pydantic model_validate 校验
    5. Schema 校验通过 → 原子写入结构化对象到 response_root
    6. 返回 LlmResponse（仅含引用和校验状态）

    validation_status="invalid" 的响应不进入编译链路——
    在 Gateway 层即被拦截，上层代码通过 is_valid 判断是否可继续。
    """

    def __init__(
        self,
        adapter: ProviderAdapter,
        prompt_manager: PromptManager,
        response_root: str = "llm_responses",
    ) -> None:
        """初始化 LLM Gateway。

        Args:
            adapter: LLM Provider 适配器（Fake / OpenAI / Anthropic）
            prompt_manager: Prompt 版本管理器
            response_root: 结构化输出落盘根目录——所有通过 Schema 校验的
                           parsed_json 原子写入此目录下。默认为 "llm_responses"。
        """
        self._adapter = adapter
        self._prompt_manager = prompt_manager
        self._response_root = Path(response_root)

    @property
    def adapter(self) -> ProviderAdapter:
        """返回当前 Adapter——仅供诊断。"""
        return self._adapter

    @property
    def prompt_manager(self) -> PromptManager:
        """返回当前 PromptManager——仅供诊断。"""
        return self._prompt_manager

    @property
    def response_root(self) -> Path:
        """返回当前响应输出根目录。"""
        return self._response_root

    def submit(self, request: LlmRequest, **extra_vars) -> LlmResponse:
        """提交 LLM 请求——完整流程：Prompt → Adapter → Schema 校验 → 落盘 → 返回。

        所有错误路径返回 LlmResponse(validation_status="invalid")——
        不抛出异常，便于上层统一处理。

        Args:
            request: LlmRequest——含 task、version、Schema 绑定、输入引用
            **extra_vars: 额外模板变量——渲染 Prompt 中的 {var} 占位符
                         用于注入 markdown_body/unresolved_columns 等动态内容

        Returns:
            LlmResponse——含 validation_status 和 parsed_json_ref
        """
        start_time = time.time()

        # ── 1. 校验 task 和 version 存在 ──
        try:
            self._prompt_manager.list_versions(request.task)
        except ValueError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[f"未知 task：{e}"],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 2. 加载 Prompt 模板 ──
        try:
            template = self._prompt_manager.get_prompt(
                request.task, request.prompt_version
            )
        except ValueError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[f"Prompt 加载失败：{e}"],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 2.5 校验请求 Schema 与 Prompt 绑定一致 ──
        # 请求中声明的 schema_name/schema_version 必须与 Prompt 模板绑定的
        # Schema 严格一致——不一致说明调用方传入了错误的元数据，应拒绝。
        if (
            request.schema_name != template.schema_binding.schema_name
            or request.schema_version != template.schema_binding.schema_version
        ):
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=request.schema_name,
                schema_version=request.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[
                    f"Schema 绑定不一致：请求声称 schema_name='{request.schema_name}' "
                    f"(v{request.schema_version})，但 Prompt 模板 "
                    f"'{request.task}/{request.prompt_version}' "
                    f"绑定到 '{template.schema_binding.schema_name}' "
                    f"(v{template.schema_binding.schema_version})"
                ],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 3. 渲染 user_message（含 extra_vars）──
        user_message = self._render_user_message(
            template=template.user_message_template,
            input_refs=request.input_artifact_refs,
            **extra_vars,
        )

        # ── 4. 获取 JSON Schema ──
        json_schema = template.schema_binding.json_schema

        # ── 5. 调用 Adapter ──
        try:
            raw_output = self._adapter.invoke(
                system_message=template.system_message,
                user_message=user_message,
                json_schema=json_schema,
                model=request.model,
                temperature=request.temperature,
            )
        except AdapterError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref="",
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=[
                    f"LLM Adapter 调用失败（provider={self._adapter.provider_name()}）：{e}"
                ],
                token_usage={},
                latency_ms=latency_ms,
            )

        # ── 6. Schema 校验 ──
        # Adapter 层会注入 _token_usage 元数据字段——Schema 校验前需剥离，
        # 避免 StrictModel（extra="forbid"）因额外字段而拒绝校验。
        # _token_usage 的值在构造 LlmResponse 时单独提取（见下方 raw_output.get("_token_usage")）。
        _token_usage = raw_output.pop("_token_usage", {})
        validated, errors = self._validate_against_schema(
            raw_output=raw_output,
            schema_binding=template.schema_binding,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        # ── 7. 构造响应 ──
        raw_ref = LlmResponse.generate_response_ref(request.request_id)

        if validated is not None and not errors:
            # Schema 校验通过——原子写入结构化对象到 response_root
            parsed_ref = LlmResponse.generate_parsed_ref(request.request_id)
            self._write_parsed_output(validated, parsed_ref)
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref=raw_ref,
                parsed_json_ref=parsed_ref,
                validation_status="valid",
                validation_errors=[],
                token_usage=_token_usage,
                latency_ms=latency_ms,
            )
        else:
            return LlmResponse(
                request_id=request.request_id,
                task=request.task,
                prompt_version=request.prompt_version,
                schema_name=template.schema_binding.schema_name,
                schema_version=template.schema_binding.schema_version,
                raw_response_ref=raw_ref,
                parsed_json_ref=None,
                validation_status="invalid",
                validation_errors=errors,
                token_usage=_token_usage,
                latency_ms=latency_ms,
            )

    # ── 内部方法 ──

    @staticmethod
    def _render_user_message(
        template: str,
        input_refs: list,
        **extra_vars,
    ) -> str:
        """将 artifact 引用和额外变量渲染到用户消息模板中。

        Args:
            template: 用户消息模板（含 {var} 占位符）
            input_refs: ArtifactRef 列表
            **extra_vars: 额外模板变量——{markdown_body}/{unresolved_columns} 等

        Returns:
            渲染后的用户消息
        """
        # 将 ArtifactRef 列表序列化为 JSON 片段
        refs_json = json.dumps(
            [ref.model_dump() for ref in input_refs],
            ensure_ascii=False,
            indent=2,
        )

        # 替换模板中的 artifact 引用占位符
        rendered = template.replace("{artifact_refs}", refs_json)

        # 渲染额外变量
        for key, value in extra_vars.items():
            rendered = rendered.replace(f"{{{key}}}", str(value))

        return rendered

    def _write_parsed_output(self, validated_model: Any, parsed_ref: str) -> None:
        """原子写入通过 Schema 校验的结构化对象到 response_root。

        写入策略：先写临时文件→os.replace 原子重命名——
        确保不会读到半写入的文件。

        Args:
            validated_model: Pydantic model_validate 通过的对象
            parsed_ref: LlmResponse.generate_parsed_ref() 返回的相对路径
        """
        target_path = self._response_root / parsed_ref
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # 原子写入——先写临时文件，再 rename
        tmp_fd = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix=".json",
                prefix=".tmp_",
                dir=str(target_path.parent),
            )
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(
                    validated_model.model_dump(mode="json"),
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            # 原子重命名
            os.replace(tmp_path, str(target_path))
        except Exception:
            # 清理临时文件（如果有）
            if tmp_fd is not None:
                try:
                    os.close(tmp_fd)
                except OSError:
                    pass
            raise

    @staticmethod
    def _validate_against_schema(
        raw_output: dict[str, Any],
        schema_binding: SchemaBinding,
    ) -> tuple[Any | None, list[str]]:
        """对 LLM 原始输出执行 Pydantic Schema 校验。

        使用 schema_binding 中的模型路径动态导入 Pydantic 类，
        调用 model_validate() 执行校验。

        校验覆盖：
        1. 非法 JSON → 已在 Adapter 层处理（Adapter 返回 dict 即表示 JSON 合法）
        2. extra="forbid" → 未知字段导致 ValidationError
        3. 字段类型不匹配 → ValidationError
        4. 缺少必填字段 → ValidationError

        Args:
            raw_output: LLM 返回的原始 dict
            schema_binding: 目标 Schema 绑定

        Returns:
            (validated_model, errors)——model 为 None 表示校验失败
        """
        model_cls = _import_pydantic_model(schema_binding.pydantic_model_path)

        try:
            validated = model_cls.model_validate(raw_output)
            return validated, []
        except ValidationError as e:
            errors = _format_validation_errors(e)
            return None, errors
        except Exception as e:
            return None, [f"Schema 校验异常：{e}"]


def _import_pydantic_model(model_path: str):
    """动态导入 Pydantic 模型类。

    Args:
        model_path: 完整导入路径

    Returns:
        Pydantic 模型类

    Raises:
        ImportError: 模块或类不存在
    """
    import importlib

    parts = model_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"无效的模型路径：'{model_path}'")

    module_path, class_name = parts
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _format_validation_errors(exc: ValidationError) -> list[str]:
    """将 Pydantic ValidationError 格式化为人类可读的错误列表。

    Args:
        exc: Pydantic ValidationError

    Returns:
        格式化的错误消息列表
    """
    errors: list[str] = []
    for error in exc.errors():
        loc = " -> ".join(str(p) for p in error["loc"])
        msg = error["msg"]
        error_type = error.get("type", "unknown")
        errors.append(f"[{error_type}] {loc}: {msg}")
    return errors
