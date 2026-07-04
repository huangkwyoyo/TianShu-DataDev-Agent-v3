"""Phase 8 SparkDeveloperService——LLM 语义标注 + StructuredOutput。

对 SparkPlan 做逐 step 语义标注（意图/操作描述/疑点标记）。
不读取 DeveloperSpec / SqlBuildPlan / SQL 文本。

Prompt 通过 PromptManager 加载版本化模板——
系统指令和用户消息模板均在 prompts/templates/spark_annotator/ 中管理。

安全边界：
- LLM 调用通过注入的 callable 执行（测试中用 mock 替代）
- 产出经过 AnnotationValidator 确定性校验
- Prompt 中不出现 DeveloperSpec / SqlBuildPlan / SQL 文本引用
- 复用既有 llm.adapters.base.ProviderAdapter + PromptManager——不维护独立 LLM 入口
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from tianshu_datadev.spark.annotations import (
    AnnotatedSparkPlan,
    AnnotationValidator,
)
from tianshu_datadev.spark.models import SparkPlan

if TYPE_CHECKING:
    from tianshu_datadev.llm.adapters.base import ProviderAdapter
    from tianshu_datadev.prompts.manager import PromptManager

logger = logging.getLogger(__name__)


class SparkDeveloperService:
    """LLM 语义标注服务——对 SparkPlan 做逐 step 语义标注。

    职责边界：
    - 读取 SparkPlan 的结构化字段构造 Prompt
    - 调用 LLM（通过注入的 callable）产出 AnnotatedSparkPlan
    - 通过 AnnotationValidator 校验产出
    - 不读取 DeveloperSpec / SqlBuildPlan / SQL 文本

    使用方式：
        # 生产环境——注入真实 LLM callable
        svc = SparkDeveloperService(llm_call=my_llm_adapter)

        # 测试环境——注入 mock
        svc = SparkDeveloperService(llm_call=mock_annotate_function)

        # 从既有基础设施创建（推荐）
        svc = SparkDeveloperService.from_provider_adapter(adapter, prompt_manager)

        annotated = svc.annotate(spark_plan)
    """

    def __init__(
        self,
        llm_call: Callable[[SparkPlan], AnnotatedSparkPlan] | None = None,
    ) -> None:
        """初始化标注服务。

        Args:
            llm_call: LLM 调用函数——签名为 (SparkPlan) -> AnnotatedSparkPlan。
                      None 时抛出 ValueError——防止静默空实现。

        Raises:
            ValueError: llm_call 为 None
        """
        if llm_call is None:
            raise ValueError(
                "SparkDeveloperService 需要注入 llm_call 参数——"
                "签名为 (SparkPlan) -> AnnotatedSparkPlan。"
                "测试中用 mock 替代，生产中用 from_provider_adapter() 创建。"
            )
        self._llm_call = llm_call
        self._validator = AnnotationValidator()

    @classmethod
    def from_provider_adapter(
        cls,
        adapter: "ProviderAdapter",
        prompt_manager: "PromptManager",
        max_llm_retries: int = 1,
    ) -> "SparkDeveloperService":
        """从既有 ProviderAdapter + PromptManager 创建实例。

        复用 llm.adapters.base.ProviderAdapter 接口——
        Prompt 从 PromptManager 加载版本化模板，不再硬编码。

        流程：
        1. 从 PromptManager 加载 spark_annotator 模板
        2. 渲染用户消息（注入 SparkPlan JSON）
        3. adapter.invoke(system_message, user_message, json_schema, model, temperature)
        4. Pydantic model_validate 校验
        5. 失败时重试（仅 retryable 错误）

        Args:
            adapter: 既有 ProviderAdapter 实例（如 AnthropicAdapter()）
            prompt_manager: PromptManager 实例——加载版本化 Prompt 模板
            max_llm_retries: LLM 调用失败时最大重试次数（默认 1 次）

        Returns:
            SparkDeveloperService——llm_call 已注入为适配器封装函数
        """
        def _adapter_llm_call(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            """将既有 ProviderAdapter 封装为 llm_call 签名。

            内部处理：
            1. Prompt 加载与渲染（PromptManager + SparkPlan JSON）
            2. LLM 调用（adapter.invoke()）
            3. Schema 校验（model_validate）
            4. 重试逻辑（仅 AdapterError 可重试）
            """
            # 加载版本化 Prompt 模板
            template = prompt_manager.get_prompt("spark_annotator", "v001")

            # 渲染用户消息——将 SparkPlan 结构化为 JSON 注入模板
            spark_plan_json = json.dumps(
                spark_plan.model_dump(mode="json", exclude_none=True),
                ensure_ascii=False,
                indent=2,
            )
            user_message = template.user_message_template.replace(
                "{spark_plan_json}", spark_plan_json
            )

            # 附加显式 step_id 指引——防止 LLM 从原始 JSON 推断错误 ID
            # （JSON 中 step_type 是枚举值 "read"，但 step_id 格式要求类名 "SparkReadStep_0"）
            step_id_lines = ["", "## Step ID 映射（必须严格使用以下 step_id）", ""]
            for i, step in enumerate(spark_plan.steps):
                step_type = type(step).__name__
                step_id = f"{step_type}_{i}"
                step_id_lines.append(f"Step {i} → step_id: {step_id}")
            step_id_lines.append("")
            step_id_lines.append("重要：annotations 中每个元素的 step_id 必须与上述映射一致——")
            step_id_lines.append("使用上面列出的 step_id 值，不要自己推断或使用 JSON 中的 step_type 值。")
            user_message += "\n".join(step_id_lines)

            # 生成 JSON Schema（用于 LLM StructuredOutput 约束）
            json_schema = AnnotatedSparkPlan.model_json_schema()

            # 调用 LLM（含重试逻辑）
            last_error: Exception | None = None
            for attempt in range(max_llm_retries + 1):
                try:
                    raw_output = adapter.invoke(
                        system_message=template.system_message,
                        user_message=user_message,
                        json_schema=json_schema,
                        model="",        # 使用 adapter 默认模型
                        temperature=0.0,  # 确定性输出
                    )
                    # 移除 adapter 附加的 _token_usage（不属于 Schema 字段）
                    raw_output.pop("_token_usage", None)
                    return AnnotatedSparkPlan.model_validate(raw_output)
                except Exception as exc:
                    last_error = exc
                    # AdapterError 可重试——网络/超时/服务端错误
                    from tianshu_datadev.llm.adapters.base import AdapterError
                    if isinstance(exc, AdapterError) and attempt < max_llm_retries:
                        logger.warning(
                            "LLM 调用失败（第 %d/%d 次），重试中: %s",
                            attempt + 1,
                            max_llm_retries + 1,
                            exc,
                        )
                        continue
                    # 校验错误 / 其他异常——不重试
                    raise

            # 不应到达此处——最后一次尝试失败时已在循环中 raise
            raise last_error  # type: ignore[misc]

        return cls(llm_call=_adapter_llm_call)

    def annotate(self, spark_plan: SparkPlan) -> AnnotatedSparkPlan:
        """对 SparkPlan 做语义标注。

        流程：
        1. 基于 SparkPlan 结构化字段构造 Prompt（不含 SQL 文本）
        2. 调用 LLM 产出 AnnotatedSparkPlan
        3. AnnotationValidator 校验产出
        4. 返回通过校验的 AnnotatedSparkPlan

        Args:
            spark_plan: mapper.py 产出的 SparkPlan

        Returns:
            AnnotatedSparkPlan——通过 AnnotationValidator 校验的合法标注

        Raises:
            ValueError: LLM 产出未通过 AnnotationValidator 校验
        """
        # Step 1：调用 LLM（生产：StructuredOutput，测试：mock）
        annotated = self._llm_call(spark_plan)

        # Step 2：确定性校验
        valid_step_ids = {
            f"{type(s).__name__}_{i}"
            for i, s in enumerate(spark_plan.steps)
        }
        validation_result = self._validator.validate(
            annotated=annotated,
            expected_step_count=len(spark_plan.steps),
            valid_step_ids=valid_step_ids,
        )

        if not validation_result.is_valid:
            raise ValueError(
                f"LLM 标注未通过 AnnotationValidator 校验："
                f"{'; '.join(validation_result.errors)}"
            )

        return annotated

    @classmethod
    def _build_prompt(
        cls,
        spark_plan: SparkPlan,
        prompt_manager: "PromptManager | None" = None,
    ) -> str:
        """基于 SparkPlan 构造语义标注 Prompt（仅用于测试/诊断）。

        当 prompt_manager 提供时：从版本化模板加载并渲染
        当 prompt_manager 为 None 时：使用内置 Prompt（向后兼容测试）

        绝对不包含：
        - SQL 关键字（SELECT/FROM/WHERE 等）
        - DeveloperSpec / SqlBuildPlan / SQL 文本引用
        - Markdown 代码块

        Args:
            spark_plan: 待标注的 SparkPlan
            prompt_manager: PromptManager 实例——为 None 时使用内置模板

        Returns:
            纯文本 Prompt 字符串（system + user 合并）
        """
        if prompt_manager is not None:
            # 从版本化模板加载
            template = prompt_manager.get_prompt("spark_annotator", "v001")
            spark_plan_json = json.dumps(
                spark_plan.model_dump(mode="json", exclude_none=True),
                ensure_ascii=False,
                indent=2,
            )
            user_message = template.user_message_template.replace(
                "{spark_plan_json}", spark_plan_json
            )
            return f"{template.system_message}\n\n{user_message}"
        else:
            # 内置 Prompt（无 PromptManager 时的回退——仅测试使用）
            return cls._build_prompt_builtin(spark_plan)

    @staticmethod
    def _build_prompt_builtin(spark_plan: SparkPlan) -> str:
        """内置 Prompt 构造——不依赖 PromptManager（仅测试回退）。

        与 prompts/templates/spark_annotator/v001.md 语义一致——
        当 PromptManager 不可用时使用。
        """
        lines: list[str] = []
        lines.append("你是一个 Spark 数据处理管线的语义标注器。")
        lines.append("你的任务是对以下 SparkPlan 的每个 step 进行语义标注。")
        lines.append("")
        lines.append("SparkPlan 元数据：")
        lines.append(f"  plan_id: {spark_plan.plan_id}")
        lines.append(f"  version: {spark_plan.version}")
        lines.append(f"  source_phase: {spark_plan.source_phase}")
        lines.append(f"  步骤总数: {len(spark_plan.steps)}")
        lines.append("")

        for i, step in enumerate(spark_plan.steps):
            step_type = type(step).__name__
            step_id = f"{step_type}_{i}"
            step_data = step.model_dump(mode="json", exclude_none=True)
            # 移除 step_type 常量（已在类名中体现）
            step_data.pop("step_type", None)

            lines.append(f"Step {i} (step_id: {step_id}):")
            # 列出关键字段（不包含 SQL 文本）
            for key, val in step_data.items():
                if val is not None and val != "" and val != []:
                    lines.append(f"  {key}: {val}")
            lines.append("")

        lines.append("对每个 Step，输出以下标注：")
        lines.append("- step_id: 必须使用上面 Step 行中标注的 step_id 值（如 SparkReadStep_0）")
        lines.append("- intent: SOURCE/CLEAN/RELATE/SUMMARIZE/LABEL/RANK/SHAPE 之一")
        lines.append("- intent_detail: 中文业务意图描述（不超过 120 字）")
        lines.append("- operation_summary: 中文操作简述")
        lines.append("")

        return "\n".join(lines)
