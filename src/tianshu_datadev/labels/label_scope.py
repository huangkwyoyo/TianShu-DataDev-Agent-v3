"""label_table v1 作用域校验——单表、非聚合、禁止 LabelNot。

由 Pipeline 和 SqlBuildPlanBuilder 共同调用，统一保证 label_table v1 三项约束。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec


class LabelScopeError(Exception):
    """label_table v1 作用域约束违反——单表、非聚合、禁止 LabelNot。

    Pipeline 捕获后重新包装为 LabelTableConfigError；
    SqlBuildPlanBuilder 捕获后重新包装为 DerivedColumnRuleMissingError。
    """


def validate_label_table_v1_scope(spec: "ParsedDeveloperSpec") -> None:
    """校验 label_table v1 作用域约束——单表、非聚合、禁止 LabelNot。

    三项检查：
    1. 单表——label_table v1 仅支持单表输入
    2. 非聚合——label_table v1 不支持聚合指标
    3. 禁止 LabelNot——防御性兜底（Validator 应已拦截，此处为双重保险）

    Args:
        spec: 已解析的 DeveloperSpec

    Raises:
        LabelScopeError: 任一约束违反时
    """
    errors: list[str] = []

    # 1. 单表——label_table v1 仅支持单表输入
    if len(spec.input_tables) != 1:
        errors.append(
            f"label_table v1 仅支持单表——当前 spec 包含 {len(spec.input_tables)} 张表: "
            f"{[t.table_alias for t in spec.input_tables]}"
        )

    # 2. 非聚合——label_table v1 不支持聚合指标
    if spec.metrics:
        errors.append(
            f"label_table v1 不支持聚合——当前 spec 包含 {len(spec.metrics)} 个指标: "
            f"{[m.metric_name for m in spec.metrics]}"
        )

    # 3. 禁止 LabelNot——防御性兜底，检查 label_rules 中的 Predicate 树
    not_found = _find_not_operator_in_rules(spec)
    if not_found:
        errors.append(
            f"label_table v1 暂不支持 LabelNot——以下规则包含 NOT 操作符: {not_found}"
        )

    if errors:
        raise LabelScopeError("；".join(errors))


def _find_not_operator_in_rules(spec: "ParsedDeveloperSpec") -> list[str]:
    """递归检查 label_rules 的 Predicate 树中是否包含 NOT 操作符。

    Pipeline 阶段 label_rules 通常为空（未 promotion），此检查为 no-op；
    Builder 阶段 label_rules 已包含 promoted 的 CaseWhenDecl，可有效兜底。
    """
    from tianshu_datadev.planning.models import Predicate, PredicateOperator

    def _recurse_predicate(node) -> bool:
        """递归搜索 Predicate 树——匹配 NOT 或嵌套 AND/OR/NOT。"""
        if not isinstance(node, Predicate):
            return False
        if node.operator == PredicateOperator.NOT:
            return True
        found = _recurse_predicate(node.left)
        if found:
            return True
        if isinstance(node.right, Predicate):
            return _recurse_predicate(node.right)
        return False

    found: list[str] = []
    for rule in spec.label_rules:
        for branch in rule.typed_branches:
            if branch.condition is not None and _recurse_predicate(branch.condition):
                found.append(rule.output_column)
                break  # 每条规则只报一次
    return found
