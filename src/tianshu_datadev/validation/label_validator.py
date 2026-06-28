"""LabelValidator——CASE WHEN 标签枚举值校验。

Phase 3B 标签门禁——在 CASE WHEN 表达式进入 Compiler 之前执行：
1. 标签枚举值必须来自 DeveloperSpec / SourceManifest 声明
2. 未声明的枚举值被拒绝
3. else_value 不校验（默认值可以是未声明值）
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    OpenQuestion,
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.planning.sql_build_plan import (
    CaseWhenStep,
    SqlBuildPlan,
)


def validate_label_enums(
    plan: SqlBuildPlan,
    spec: ParsedDeveloperSpec | None = None,
    manifest: SourceManifest | None = None,
) -> list[OpenQuestion]:
    """校验 CaseWhenStep 的所有标签枚举值是否在声明范围内。

    查找策略：
    1. 从 spec.input_tables → columns → enum_values 收集所有声明枚举值
    2. 从 manifest.tables → columns → enum_values 收集补充枚举值
    3. 将 CaseWhenStep 中每个 WhenBranch.result.value 与声明值比对
    4. 未声明的值生成 blocking OpenQuestion

    Args:
        plan: 待校验的 SqlBuildPlan
        spec: 已解析的 DeveloperSpec（提供枚举值声明）
        manifest: 事实源（提供补充枚举值）

    Returns:
        OpenQuestion 列表——空列表表示全部通过
    """
    questions: list[OpenQuestion] = []

    # 收集所有 CaseWhenStep
    case_steps: list[CaseWhenStep] = []
    for step in plan.steps:
        if isinstance(step, CaseWhenStep):
            case_steps.append(step)

    if not case_steps:
        return questions

    # 收集所有已声明的枚举值（按字段名索引）
    declared_enums: dict[str, set[str]] = _collect_declared_enums(spec, manifest)

    # 无任何声明枚举值——无法校验，静默通过
    if not declared_enums:
        return questions

    for cstep in case_steps:
        for i, branch in enumerate(cstep.cases):
            result_value = branch.result.value
            if result_value is None:
                continue  # NULL 字面量不校验

            # 将值转为字符串进行比较
            result_str = str(result_value)

            # 在所有声明的枚举值中搜索
            if not _is_enum_declared(result_str, declared_enums):
                # 尝试找到最相关的字段名（从 condition 中推断）
                field_hint = _extract_field_from_predicate(branch.condition)
                questions.append(
                    OpenQuestion(
                        question_id=(
                            f"label_enum_undeclared_"
                            f"{cstep.step_id}_{i}"
                        ),
                        source="LabelValidator",
                        field_ref=field_hint,
                        description=(
                            f"CASE WHEN 分支 {i}（别名 '{cstep.alias}'）的 "
                            f"结果值 '{result_str}' 未在 DeveloperSpec 或 "
                            f"SourceManifest 的枚举值声明中找到。"
                            f"推断字段: {field_hint or '未知'}。"
                            f"已声明的枚举值: "
                            f"{_format_declared_enums(declared_enums)}"
                        ),
                        blocking=True,
                    )
                )

    return questions


def _collect_declared_enums(
    spec: ParsedDeveloperSpec | None,
    manifest: SourceManifest | None,
) -> dict[str, set[str]]:
    """收集所有已声明的枚举值。

    从两个来源收集：
    - DeveloperSpec：input_tables → columns → enum_values
    - SourceManifest：tables → columns → enum_values

    Returns:
        {normalized_field_name: {enum_value, ...}} 的映射
    """
    declared: dict[str, set[str]] = {}

    # 从 ParsedDeveloperSpec 收集
    if spec:
        for table in spec.input_tables:
            for col in table.columns:
                if col.enum_values:
                    key = col.normalized_name
                    if key not in declared:
                        declared[key] = set()
                    declared[key].update(col.enum_values)

    # 从 SourceManifest 收集
    if manifest:
        for table in manifest.tables:
            for col in table.columns:
                if col.enum_values:
                    key = col.normalized_name
                    if key not in declared:
                        declared[key] = set()
                    declared[key].update(col.enum_values)

    return declared


def _is_enum_declared(value: str, declared_enums: dict[str, set[str]]) -> bool:
    """检查值是否在任何已声明的枚举值集合中。

    Args:
        value: 待检查的枚举值
        declared_enums: 已声明枚举值映射

    Returns:
        True 如果值已声明
    """
    for enum_values in declared_enums.values():
        if value in enum_values:
            return True
    return False


def _extract_field_from_predicate(pred) -> str | None:
    """从 Predicate 的 left 侧提取字段名——用于错误消息提示。

    Args:
        pred: CASE WHEN 条件谓词

    Returns:
        推断的字段名，无法提取时返回 None
    """
    left = getattr(pred, "left", None)
    if left is None:
        return None
    col_name = getattr(left, "column_name", None)
    if col_name:
        return col_name
    # 递归检查嵌套 Predicate
    if hasattr(left, "operator"):
        return _extract_field_from_predicate(left)
    return None


def _format_declared_enums(declared_enums: dict[str, set[str]]) -> str:
    """格式化已声明的枚举值——用于错误消息。

    Args:
        declared_enums: 枚举值映射

    Returns:
        可读的枚举值描述字符串
    """
    if not declared_enums:
        return "（无）"

    parts: list[str] = []
    for field, values in sorted(declared_enums.items()):
        vals_str = ", ".join(sorted(values))
        parts.append(f"{field}: [{vals_str}]")
    return "; ".join(parts[:5])  # 最多展示 5 个字段
