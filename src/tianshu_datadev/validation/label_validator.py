"""LabelValidator——CASE WHEN 标签枚举值校验。

Phase 3B 标签门禁——在 CASE WHEN 表达式进入 Compiler 之前执行：
1. 标签枚举值必须来自 DeveloperSpec / SourceManifest 声明（blocking）
2. Phase 3B.1 升级：支持 EnumProfiler 自动检测值（按 tier 分层行为）
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
from tianshu_datadev.profiling.models import (
    EnumConfidenceTier,
    EnumProfile,
)


def validate_label_enums(
    plan: SqlBuildPlan,
    spec: ParsedDeveloperSpec | None = None,
    manifest: SourceManifest | None = None,
    profiles: list[EnumProfile] | None = None,
) -> list[OpenQuestion]:
    """校验 CaseWhenStep 的所有标签枚举值是否在声明范围内。

    查找策略（优先级递减）：
    1. 从 spec.input_tables → columns → enum_values 收集声明枚举值（等效 CERTAIN）
    2. 从 manifest.tables → columns → enum_values 收集补充枚举值（等效 CERTAIN）
    3. 从 profiles（EnumProfiler 输出）收集自动检测值——按 tier 分层行为：
       - CERTAIN → blocking（Flag 必达条件，与手动声明等效）
       - HIGH    → 非 blocking（WARN——不阻断流水线）
       - MEDIUM  → 非 blocking（info——供人工审查参考）
       - LOW     → 跳过

    Args:
        plan: 待校验的 SqlBuildPlan
        spec: 已解析的 DeveloperSpec（提供枚举值声明）
        manifest: 事实源（提供补充枚举值）
        profiles: EnumProfiler 检测结果（Phase 3B.1——可选，提供自动检测值）

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

    # 收集已声明的枚举值（按字段名索引）
    declared_enums: dict[str, set[str]] = _collect_declared_enums(spec, manifest)

    # 收集自动检测的枚举值（按字段名索引，含 tier）
    detected_enums: dict[str, tuple[set[str], EnumConfidenceTier]] = {}
    if profiles:
        detected_enums = _collect_detected_enums(profiles)

    # 合并所有可用的枚举值源
    merged_enums = _merge_enum_sources(declared_enums, detected_enums)

    # 无任何声明且无自动检测值——无法校验，静默通过
    if not merged_enums:
        return questions

    for cstep in case_steps:
        for i, branch in enumerate(cstep.cases):
            result_value = branch.result.value
            if result_value is None:
                continue  # NULL 字面量不校验

            result_str = str(result_value)
            field_hint = _extract_field_from_predicate(branch.condition)

            # 在声明的枚举值中搜索
            if _is_enum_declared(result_str, declared_enums):
                continue  # 已声明——通过

            # 在自动检测的枚举值中搜索
            detected_info = _find_in_detected(result_str, detected_enums)
            if detected_info is not None:
                detected_tier = detected_info
                if detected_tier == EnumConfidenceTier.CERTAIN:
                    continue  # CERTAIN 级别自动检测——通过（等效声明）
                elif detected_tier == EnumConfidenceTier.HIGH:
                    questions.append(
                        OpenQuestion(
                            question_id=(
                                f"label_enum_autodetect_warn_"
                                f"{cstep.step_id}_{i}"
                            ),
                            source="LabelValidator",
                            field_ref=field_hint,
                            description=(
                                f"CASE WHEN 分支 {i}（别名 '{cstep.alias}'）的 "
                                f"结果值 '{result_str}' 未在 DeveloperSpec 中声明，"
                                f"但被 EnumProfiler 检测到（置信度: HIGH）。"
                                f"推断字段: {field_hint or '未知'}。"
                                f"检测到的枚举值: "
                                f"{_format_detected_values(detected_enums, field_hint)}"
                            ),
                            blocking=False,  # WARN——不阻断流水线
                        )
                    )
                elif detected_tier == EnumConfidenceTier.MEDIUM:
                    questions.append(
                        OpenQuestion(
                            question_id=(
                                f"label_enum_autodetect_info_"
                                f"{cstep.step_id}_{i}"
                            ),
                            source="LabelValidator",
                            field_ref=field_hint,
                            description=(
                                f"CASE WHEN 分支 {i}（别名 '{cstep.alias}'）的 "
                                f"结果值 '{result_str}' 未在 DeveloperSpec 中声明，"
                                f"EnumProfiler 检测到但置信度较低（MEDIUM），"
                                f"建议人工审查。"
                                f"推断字段: {field_hint or '未知'}。"
                            ),
                            blocking=False,  # info——仅提示
                        )
                    )
                else:
                    # LOW tier——静默跳过
                    continue
            else:
                # 既不在声明中也不在自动检测中——blocking
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


def _collect_detected_enums(
    profiles: list[EnumProfile],
) -> dict[str, tuple[set[str], EnumConfidenceTier]]:
    """从 EnumProfiler 输出中提取枚举值映射。

    仅提取 tier ≥ MEDIUM 的 profile——LOW 和 NOT_ENUM 不参与校验。
    按 normalized_name 索引——多个 profile 可能指向同一字段的不同表引用，
    取 tier 最高的那个。

    Args:
        profiles: EnumProfiler.profile() 返回的 profiles 列表

    Returns:
        {normalized_name: (detected_values_set, tier)} 的映射
    """
    detected: dict[str, tuple[set[str], EnumConfidenceTier]] = {}

    for p in profiles:
        if p.tier in (EnumConfidenceTier.LOW, EnumConfidenceTier.NOT_ENUM):
            continue  # 低置信度和非枚举不参与

        key = p.normalized_name

        # 同一字段取 tier 更高的
        if key in detected:
            existing_tier = detected[key][1]
            if _tier_rank(p.tier) <= _tier_rank(existing_tier):
                continue

        detected[key] = (set(p.detected_values), p.tier)

    return detected


def _tier_rank(tier: EnumConfidenceTier) -> int:
    """返回 tier 的数值排序——数字越小越可信。"""
    _rank = {
        EnumConfidenceTier.CERTAIN: 0,
        EnumConfidenceTier.HIGH: 1,
        EnumConfidenceTier.MEDIUM: 2,
        EnumConfidenceTier.LOW: 3,
        EnumConfidenceTier.NOT_ENUM: 4,
    }
    return _rank.get(tier, 99)


def _merge_enum_sources(
    declared: dict[str, set[str]],
    detected: dict[str, tuple[set[str], EnumConfidenceTier]],
) -> dict[str, set[str]]:
    """合并手动声明和自动检测的枚举值。

    自动检测值不覆盖手动声明——手动声明始终优先。
    合并后的集合用于唯一查找——tier 信息保留在 detected 中。

    Args:
        declared: 手动声明的枚举值 {normalized_name: {values}}
        detected: 自动检测的枚举值 {normalized_name: ({values}, tier)}

    Returns:
        合并后的 {normalized_name: {all_values}}
    """
    merged: dict[str, set[str]] = {}

    # 手动声明优先
    for name, values in declared.items():
        merged[name] = set(values)

    # 自动检测补充（不覆盖）
    for name, (values, _tier) in detected.items():
        if name not in merged:
            merged[name] = set(values)

    return merged


def _find_in_detected(
    value: str,
    detected_enums: dict[str, tuple[set[str], EnumConfidenceTier]],
) -> EnumConfidenceTier | None:
    """在自动检测的枚举值中查找指定值。

    Args:
        value: 待查找的 CASE WHEN 结果值
        detected_enums: 自动检测枚举值映射

    Returns:
        值所属的最高 tier，未找到返回 None
    """
    best_tier: EnumConfidenceTier | None = None

    for _name, (values, tier) in detected_enums.items():
        if value in values:
            if best_tier is None or _tier_rank(tier) < _tier_rank(best_tier):
                best_tier = tier

    return best_tier


def _format_detected_values(
    detected_enums: dict[str, tuple[set[str], EnumConfidenceTier]],
    field_hint: str | None,
) -> str:
    """格式化自动检测的枚举值——用于 OpenQuestion 描述。

    Args:
        detected_enums: 自动检测枚举值映射
        field_hint: 推断字段名（用于精确匹配）

    Returns:
        可读的枚举值描述字符串
    """
    if field_hint and field_hint in detected_enums:
        values, tier = detected_enums[field_hint]
        return f"[{', '.join(sorted(values))}] (tier={tier.value})"

    # 无精确匹配——汇总所有检测值
    parts: list[str] = []
    for name, (values, tier) in sorted(detected_enums.items()):
        vals_str = ", ".join(sorted(values))
        parts.append(f"{name}: [{vals_str}] (tier={tier.value})")
    return "; ".join(parts[:3])


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
