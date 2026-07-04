"""Phase 8 SparkDeveloperService——LLM 语义标注 + StructuredOutput。

对 SparkPlan 做逐 step 语义标注（意图/操作描述/疑点标记）。
不读取 DeveloperSpec / SqlBuildPlan / SQL 文本。

Prompt 构造仅基于 SparkPlan 的结构化字段（step_type、列名、操作符等），
不含 SQL 关键字或自由文本。

安全边界：
- LLM 调用通过注入的 callable 执行（测试中用 mock 替代）
- 产出经过 AnnotationValidator 确定性校验
- Prompt 中不出现 DeveloperSpec / SqlBuildPlan / SQL 文本引用
"""

from __future__ import annotations

from collections.abc import Callable

from tianshu_datadev.spark.annotations import (
    AnnotatedSparkPlan,
    AnnotationValidator,
)
from tianshu_datadev.spark.models import SparkPlan


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
                "测试中用 mock 替代，生产中用 ProviderAdapter 封装。"
            )
        self._llm_call = llm_call
        self._validator = AnnotationValidator()

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

    def _build_prompt(self, spark_plan: SparkPlan) -> str:
        """基于 SparkPlan 结构化字段构造语义标注 Prompt。

        绝对不包含：
        - SQL 关键字（SELECT/FROM/WHERE 等）
        - DeveloperSpec / SqlBuildPlan / SQL 文本引用
        - Markdown 代码块

        仅包含：
        - SparkPlan 元数据（plan_id、version、source_phase）
        - 每个 step 的结构化字段（step_type、列名、操作符等）

        Args:
            spark_plan: 待标注的 SparkPlan

        Returns:
            纯文本 Prompt 字符串
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
            step_data = step.model_dump(mode="json", exclude_none=True)
            # 移除 step_type 常量（已在类名中体现）
            step_data.pop("step_type", None)

            lines.append(f"Step {i} ({step_type}):")
            # 列出关键字段（不包含 SQL 文本）
            for key, val in step_data.items():
                if val is not None and val != "" and val != []:
                    lines.append(f"  {key}: {val}")
            lines.append("")

        lines.append("对每个 Step，输出以下标注：")
        lines.append("- step_id: 格式为 {StepType}_{索引}")
        lines.append("- intent: SOURCE/CLEAN/RELATE/SUMMARIZE/LABEL/RANK/SHAPE 之一")
        lines.append("- intent_detail: 中文业务意图描述（不超过 120 字）")
        lines.append("- operation_summary: 中文操作简述")
        lines.append("")

        return "\n".join(lines)
