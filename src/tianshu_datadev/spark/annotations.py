"""Phase 6 Spark 语义标注模型——SparkDeveloper 输出 + AnnotationValidator 校验。

SparkDeveloper（LLM）只做语义标注，不增删改 SparkPlan step。
所有标注经过 AnnotationValidator 确定性校验后才进入编译链路。

Provenance 边界：
- plan_id 和 baseline_plan_hash 由 Python 代码确定性计算——不由 LLM 填写
- LLM 返回的 provenance 字段会被 annotate() 强制覆盖

Warning contract：
- 仅 4 种合法 category——超出范围的 warning 被确定性过滤并记录日志
- warning 引用的 step_id 必须存在于 baseline 中
- 未知 REVIEW 不会触发 human_review_suggested
"""

from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from typing import Literal

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════
# Warning category 封闭集合——仅此 4 种合法
# ════════════════════════════════════════════

# 合法 warning category——Prompt 模板必须与以下值保持同步
ALLOWED_WARNING_CATEGORIES: frozenset[str] = frozenset({
    "step_count_anomaly",     # step 数量异常（0 个或超过 20 个）
    "missing_cleaning_step",  # 缺少常见清洗步骤（如无 Filter 直接 Project）
    "window_missing_partition",  # 窗口函数缺少 PARTITION BY
    "nonstandard_alias",      # 别名命名不规范
})

# ════════════════════════════════════════════
# StepIntent 枚举——步骤意图分类
# ════════════════════════════════════════════


class StepIntent(str, Enum):
    """步骤意图分类——LLM 标注时选择，用于人审展示。"""

    SOURCE = "source"        # 数据读取
    CLEAN = "clean"          # 数据清洗/过滤
    RELATE = "relate"        # 表关联
    SUMMARIZE = "summarize"  # 聚合汇总
    LABEL = "label"          # 分类打标 (CASE WHEN)
    RANK = "rank"            # 窗口排名
    SHAPE = "shape"          # 投影/排序/截断（最终整形）


# ════════════════════════════════════════════
# StepAnnotation——单步语义标注
# ════════════════════════════════════════════


class StepAnnotation(StrictModel):
    """单个 step 的语义标注——不修改 SparkPlan step 的任何字段。

    step_id 为主键（对应 baseline.steps[i] 的编译器生成 ID），
    step_index 仅为展示字段。
    """

    step_id: str                     # 主键——对应编译器生成的 step ID
    step_index: int                  # 展示字段——由构建时自动填充
    step_type: str                   # 冗余校验字段（与 SparkStepType 值一致）
    intent: StepIntent               # 意图分类
    intent_detail: str = ""          # 中文业务意图描述（≤120 字）
    operation_summary: str = ""      # 中文操作简述
    downstream_step_ids: list[str] = Field(default_factory=list)  # 下游消费者 step_id
    review_flags: list[str] = Field(default_factory=list)         # 疑点标签


# ════════════════════════════════════════════
# AnnotationWarning——语义疑点（不进执行路径）
# ════════════════════════════════════════════


class AnnotationWarning(StrictModel):
    """SparkDeveloper 发现的语义疑点——只能进入 Review/Repair/Harness。

    禁止：直接修改 SparkPlan、Compiler 输出、Comparator 结论。
    """

    warning_id: str
    step_id: str | None = None       # 关联的 step_id，可为 None（全局疑点）
    severity: Literal["INFO", "WARN", "REVIEW"] = "WARN"
    category: str                    # "semantic_mismatch" / "missing_filter" / "ambiguous_join" / ...
    description: str
    suggestion: str | None = None


# ════════════════════════════════════════════
# AnnotatedSparkPlan——baseline + 标注层
# ════════════════════════════════════════════


class AnnotatedSparkPlan(StrictModel):
    """标注后的 SparkPlan——baseline SparkPlan + 标注层。

    约束：
    1. annotations 数量 == baseline.steps 数量（一一对应）
    2. 删除全部 annotations 后，Compiler 产出等价代码
    3. annotations 不参与 SparkPlan.compute_plan_hash()
    """

    plan_id: str
    baseline_plan_hash: str          # baseline SparkPlan 的 hash（compute_plan_hash）
    annotations: list[StepAnnotation] = Field(default_factory=list)
    warnings: list[AnnotationWarning] = Field(default_factory=list)
    annotator_version: str = "v1"
    annotation_hash: str = ""        # 标注层确定性 hash（由 compute_annotation_hash 填充）


# ════════════════════════════════════════════
# AnnotationValidator——确定性标注校验
# ════════════════════════════════════════════


class AnnotationValidationResult(StrictModel):
    """AnnotationValidator 的校验结果。"""

    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    human_review_suggested: bool = False  # REVIEW 级别 warning 标记


class AnnotationValidator:
    """确定性标注校验器——检查 LLM 标注是否违反边界约束。

    规则：
    - annotation 数量 != steps 数量 → VALIDATION_ERROR（阻断编译）
    - step_id 不在 baseline 中 → VALIDATION_ERROR
    - step_id 重复 → VALIDATION_ERROR
    - REVIEW 级别 warning（已知 category）→ 标记 HumanReviewSuggested（不阻断）
    - 未知 category 的 warning → 确定性过滤 + 记录日志
    - warning 引用的 step_id 不存在 → 移除该 warning + 记录日志
    """

    def validate(
        self,
        annotated: AnnotatedSparkPlan,
        expected_step_count: int,
        valid_step_ids: set[str],
    ) -> AnnotationValidationResult:
        """校验标注是否合法。

        Args:
            annotated: 待校验的标注计划
            expected_step_count: baseline SparkPlan 的 steps 数量
            valid_step_ids: 编译器生成的合法 step_id 集合

        Returns:
            AnnotationValidationResult——is_valid=False 时阻断编译
        """
        errors: list[str] = []
        human_review_suggested = False

        # 规则 1：annotation 数量必须与 steps 数量一致
        if len(annotated.annotations) != expected_step_count:
            errors.append(
                f"标注数量 ({len(annotated.annotations)}) 与 steps 数量 "
                f"({expected_step_count}) 不一致"
            )

        # 规则 2 & 3：step_id 必须在 baseline 中且不重复
        seen_ids: set[str] = set()
        for ann in annotated.annotations:
            if ann.step_id not in valid_step_ids:
                errors.append(
                    f"step_id '{ann.step_id}' 不在 baseline SparkPlan 中"
                )
            if ann.step_id in seen_ids:
                errors.append(f"step_id '{ann.step_id}' 重复")
            seen_ids.add(ann.step_id)

        # 规则 4：确定性过滤 warning——未知 category、无效 step_id
        self._validate_warnings(annotated, valid_step_ids)

        # 规则 5：REVIEW 级别 warning（已知 category）→ HumanReviewSuggested
        for w in annotated.warnings:
            if w.severity == "REVIEW":
                human_review_suggested = True
                break

        return AnnotationValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            human_review_suggested=human_review_suggested,
        )

    @staticmethod
    def _validate_warnings(
        annotated: AnnotatedSparkPlan,
        valid_step_ids: set[str],
    ) -> None:
        """确定性过滤 warning——就地修改 annotated.warnings。

        过滤规则：
        1. 未知 category → 移除 + warning 日志（防止 LLM 越权告警）
        2. step_id 非 None 但不在 valid_step_ids 中 → 移除 + warning 日志

        这些是就地修改——LLM 输出的 warning 不是可执行指令，
        过滤后不影响标注的正确性。
        """
        filtered: list = []
        for w in annotated.warnings:
            # 规则 1：未知 category → 过滤
            if w.category not in ALLOWED_WARNING_CATEGORIES:
                logger.warning(
                    "AnnotationValidator: 过滤未知 warning category=%r "
                    "（warning_id=%s, severity=%s）——不在合法集合 %s 中",
                    w.category, w.warning_id, w.severity,
                    sorted(ALLOWED_WARNING_CATEGORIES),
                )
                continue

            # 规则 2：step_id 存在但在 baseline 中不存在 → 过滤
            if w.step_id is not None and w.step_id not in valid_step_ids:
                logger.warning(
                    "AnnotationValidator: 过滤无效 step_id=%r 的 warning "
                    "（warning_id=%s, category=%s）",
                    w.step_id, w.warning_id, w.category,
                )
                continue

            filtered.append(w)

        # 就地替换——warnings 是非执行性提示，过滤不抛异常
        annotated.warnings = filtered


# ════════════════════════════════════════════
# annotation_hash 计算
# ════════════════════════════════════════════


def compute_annotation_hash(annotated: AnnotatedSparkPlan) -> str:
    """计算标注层确定性 SHA-256。

    包含：annotations(按 step_id 排序)、warnings(按 warning_id 排序)、
          annotator_version、baseline_plan_hash
    不包含：时间戳、step_index（展示字段）、baseline 内部结构

    Args:
        annotated: 标注后的 SparkPlan

    Returns:
        64 字符十六进制 SHA-256
    """
    data: dict = {
        "plan_id": annotated.plan_id,
        "annotator_version": annotated.annotator_version,
        "baseline_plan_hash": annotated.baseline_plan_hash,
        "annotations": sorted(
            [
                a.model_dump(exclude={"step_index"})
                for a in annotated.annotations
            ],
            key=lambda a: a["step_id"],
        ),
        "warnings": sorted(
            [w.model_dump() for w in annotated.warnings],
            key=lambda w: w["warning_id"],
        ),
    }
    content = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()
