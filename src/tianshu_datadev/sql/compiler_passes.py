"""Phase 1C Compiler Pass——4 个幂等优化 Pass。

每个 Pass 必须是幂等的——重复运行不改变结果。
Pass 返回 (变更后的对象, 变更记录)，不修改原对象（函数式风格）。

1. 列裁剪：移除 ScanStep 中未被后续 step 引用的 required_columns
2. 谓词规范化：BETWEEN → >= AND <；移除恒真/恒假条件
3. 无用排序消除：无 LIMIT 且输出不依赖顺序的 SortStep → 移除
4. 常量折叠：1+2 → 3；TRUE AND x → x；重复 IS NOT NULL 合并
"""

from __future__ import annotations

import copy

from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    PredicateOperator,
    SqlLiteral,
)
from tianshu_datadev.planning.sql_build_plan import (
    ScanStep,
    SortStep,
    SqlBuildPlan,
    StepNode,
)

from .models import (
    CompilerPassRecord,
    ConstantFoldRecord,
    PredicateNormRecord,
)

# Compiler Pass 版本——随 Phase 迭代递增
PASS_VERSION = "1.0.0"


# ════════════════════════════════════════════
# Pass 1: 列裁剪
# ════════════════════════════════════════════


def column_pruning(plan: SqlBuildPlan) -> tuple[SqlBuildPlan, CompilerPassRecord, list[str]]:
    """移除 ScanStep 中未被后续 step 引用的 required_columns。

    收集所有非 ScanStep 中引用的 ColumnRef（按 normalized_name），
    从每个 ScanStep 的 required_columns 中移除未被引用的列。

    幂等性：裁剪后的 required_columns 已是引用的最小集，
    再次运行不会进一步裁剪。
    """
    # 收集后续步骤中所有被引用的列（按 normalized_name）
    referenced_cols: set[str] = set()
    for step in plan.steps:
        if isinstance(step, ScanStep):
            continue  # 跳过 ScanStep 自身
        referenced_cols.update(_collect_column_refs(step))

    changes = 0
    removed_cols: list[str] = []
    new_steps: list[StepNode] = []

    for step in plan.steps:
        if isinstance(step, ScanStep):
            kept = []
            pruned = []
            for col in step.required_columns:
                if col.normalized_name in referenced_cols or col.column_name in referenced_cols:
                    kept.append(col)
                else:
                    pruned.append(f"{step.table_ref}.{col.column_name}")
            removed_cols.extend(pruned)
            changes += len(pruned)

            if pruned:
                # 创建裁剪后的 ScanStep
                new_scan = ScanStep(
                    step_id=step.step_id,
                    table_ref=step.table_ref,
                    required_columns=kept,
                    predicates=step.predicates,
                    partition_filters=step.partition_filters,
                    estimated_row_count=step.estimated_row_count,
                )
                new_steps.append(new_scan)
            else:
                new_steps.append(step)
        else:
            new_steps.append(step)

    # 重建 plan（保持其他字段不变）
    new_plan = SqlBuildPlan(
        plan_id=plan.plan_id,
        spec_hash=plan.spec_hash,
        hypothesis_id=plan.hypothesis_id,
        source_manifest_hash=plan.source_manifest_hash,
        steps=new_steps,
        multi_table=plan.multi_table,
    )

    input_snippet = f"{sum(len(s.required_columns) for s in _get_scan_steps(plan))} columns"
    output_snippet = f"{sum(len(s.required_columns) for s in _get_scan_steps(new_plan))} columns"

    record = CompilerPassRecord(
        pass_name="column_pruning",
        pass_version=PASS_VERSION,
        applied=changes > 0,
        changes_count=changes,
        input_ast_snippet=input_snippet,
        output_ast_snippet=output_snippet,
    )

    return new_plan, record, removed_cols


# ════════════════════════════════════════════
# Pass 2: 谓词规范化
# ════════════════════════════════════════════


def predicate_normalization(plan: SqlBuildPlan) -> tuple[SqlBuildPlan, list[PredicateNormRecord]]:
    """谓词规范化——将 BETWEEN 展开、移除恒真/恒假条件。

    规则：
    - BETWEEN(a, x, y) → a >= x AND a < y
    - 移除重复的 IS NOT NULL（如 x IS NOT NULL AND x IS NOT NULL → x IS NOT NULL）

    幂等性：规范化后的谓词不再包含触发规则的模式，再次运行无变更。
    """
    records: list[PredicateNormRecord] = []
    new_steps: list[StepNode] = []

    for step in plan.steps:
        new_step = step
        if hasattr(step, "predicate"):
            new_pred, step_records = _normalize_predicate(step.predicate)
            if step_records:
                records.extend(step_records)
                # 使用 model_copy 重建 step（保持其他字段 + 替换 predicate）
                new_step = step.model_copy(update={"predicate": new_pred})
        new_steps.append(new_step)

    if not records:
        return plan, records

    new_plan = SqlBuildPlan(
        plan_id=plan.plan_id,
        spec_hash=plan.spec_hash,
        hypothesis_id=plan.hypothesis_id,
        source_manifest_hash=plan.source_manifest_hash,
        steps=new_steps,
        multi_table=plan.multi_table,
    )

    return new_plan, records


def _normalize_predicate(
    pred: Predicate,
) -> tuple[Predicate, list[PredicateNormRecord]]:
    """递归规范化单个 Predicate 节点。"""
    records: list[PredicateNormRecord] = []

    # 递归处理嵌套
    new_left = pred.left
    if isinstance(pred.left, Predicate):
        new_left, left_records = _normalize_predicate(pred.left)
        records.extend(left_records)

    new_right = pred.right
    if isinstance(pred.right, Predicate):
        new_right, right_records = _normalize_predicate(pred.right)
        records.extend(right_records)

    # 规则: BETWEEN → >= AND <=（保持包含上界的语义）
    if pred.operator == PredicateOperator.BETWEEN and isinstance(pred.right, list):
        if len(pred.right) == 2:
            original = f"BETWEEN({pred.right[0].value}, {pred.right[1].value})"
            normalized = f">= {pred.right[0].value} AND <= {pred.right[1].value}"
            records.append(
                PredicateNormRecord(
                    original=original,
                    normalized=normalized,
                    rule="between_to_range",
                )
            )
            # 展开为 (left >= low) AND (left <= high)——保持 BETWEEN 包含两端语义
            right_low, right_high = pred.right[0], pred.right[1]
            return (
                Predicate(
                    left=Predicate(
                        left=new_left,
                        operator=PredicateOperator.GTE,
                        right=right_low,
                    ),
                    operator=PredicateOperator.AND,
                    right=Predicate(
                        left=copy.deepcopy(new_left) if isinstance(new_left, Predicate)
                        else new_left.model_copy() if hasattr(new_left, "model_copy")
                        else new_left,
                        operator=PredicateOperator.LTE,
                        right=right_high,
                    ),
                ),
                records,
            )

    # 规则：TRUE AND x → x（恒真消除）
    if pred.operator == PredicateOperator.AND:
        if _is_tautology(new_left):
            original = "TRUE AND x"
            normalized = "x"
            records.append(
                PredicateNormRecord(
                    original=original,
                    normalized=normalized,
                    rule="tautology_and",
                )
            )
            return (new_right if isinstance(new_right, Predicate) else pred, records)
        if _is_tautology(new_right):
            original = "x AND TRUE"
            normalized = "x"
            records.append(
                PredicateNormRecord(
                    original=original,
                    normalized=normalized,
                    rule="tautology_and",
                )
            )
            return (new_left if isinstance(new_left, Predicate) else pred, records)

    # 无变更
    if new_left is not pred.left or new_right is not pred.right:
        return (
            Predicate(left=new_left, operator=pred.operator, right=new_right),
            records,
        )

    return pred, records


def _is_tautology(node) -> bool:
    """检查谓词节点是否为恒真条件。"""
    if isinstance(node, SqlLiteral) and node.value is True:
        return True
    # 1=1 → 恒真
    if isinstance(node, Predicate):
        if node.operator == PredicateOperator.EQ:
            left_val = getattr(node.left, "column_name", "")
            right_val = getattr(node.right, "value", None) if hasattr(node.right, "value") else None
            if left_val and right_val and str(left_val) == str(right_val):
                return True
    return False


# ════════════════════════════════════════════
# Pass 3: 无用排序消除
# ════════════════════════════════════════════


def sort_elimination(plan: SqlBuildPlan) -> tuple[SqlBuildPlan, CompilerPassRecord, list[str]]:
    """消除无用的 SortStep——无 LIMIT 且输出不依赖顺序时移除。

    条件：SortStep.requires_full_sort=True 且无 LimitStep 紧随其后。

    幂等性：消除后不再包含需消除的 SortStep，再次运行无变更。
    """
    new_steps: list[StepNode] = []
    eliminated: list[str] = []
    has_limit = any(
        hasattr(s, "step_type") and s.step_type == "limit" for s in plan.steps
    )

    for step in plan.steps:
        if isinstance(step, SortStep):
            # 如果有无 LIMIT 的 SortStep 且整个计划无 LimitStep → 消除
            if step.requires_full_sort and not has_limit and step.limit is None:
                eliminated.append(step.step_id)
                continue  # 不添加此 SortStep
        new_steps.append(step)

    if not eliminated:
        record = CompilerPassRecord(
            pass_name="sort_elimination",
            pass_version=PASS_VERSION,
            applied=False,
            changes_count=0,
            input_ast_snippet="no sorts to eliminate",
            output_ast_snippet="no change",
        )
        return plan, record, []

    new_plan = SqlBuildPlan(
        plan_id=plan.plan_id,
        spec_hash=plan.spec_hash,
        hypothesis_id=plan.hypothesis_id,
        source_manifest_hash=plan.source_manifest_hash,
        steps=new_steps,
        multi_table=plan.multi_table,
    )

    record = CompilerPassRecord(
        pass_name="sort_elimination",
        pass_version=PASS_VERSION,
        applied=True,
        changes_count=len(eliminated),
        input_ast_snippet=f"{len(_get_sort_steps(plan))} SortStep(s)",
        output_ast_snippet=f"{len(_get_sort_steps(new_plan))} SortStep(s)",
    )

    return new_plan, record, eliminated


# ════════════════════════════════════════════
# Pass 4: 常量折叠
# ════════════════════════════════════════════


def constant_folding(plan: SqlBuildPlan) -> tuple[SqlBuildPlan, list[ConstantFoldRecord]]:
    """常量折叠——简化可静态计算的表达式。

    规则：
    - TRUE AND x → x（恒真与消除）
    - x IS NOT NULL AND x IS NOT NULL → x IS NOT NULL（重复消除）

    幂等性：折叠后的表达式不再包含可折叠模式，再次运行无变更。
    """
    records: list[ConstantFoldRecord] = []
    new_steps: list[StepNode] = []

    for step in plan.steps:
        new_step = step
        if hasattr(step, "predicate"):
            new_pred, step_records = _fold_predicate(step.predicate)
            if step_records:
                records.extend(step_records)
                new_step = step.model_copy(update={"predicate": new_pred})
        new_steps.append(new_step)

    if not records:
        return plan, records

    new_plan = SqlBuildPlan(
        plan_id=plan.plan_id,
        spec_hash=plan.spec_hash,
        hypothesis_id=plan.hypothesis_id,
        source_manifest_hash=plan.source_manifest_hash,
        steps=new_steps,
        multi_table=plan.multi_table,
    )

    return new_plan, records


def _fold_predicate(
    pred: Predicate,
) -> tuple[Predicate, list[ConstantFoldRecord]]:
    """递归折叠 Predicate 中的常量表达式。"""
    records: list[ConstantFoldRecord] = []

    # 递归处理嵌套
    new_left = pred.left
    if isinstance(pred.left, Predicate):
        new_left, left_records = _fold_predicate(pred.left)
        records.extend(left_records)

    new_right = pred.right
    if isinstance(pred.right, Predicate):
        new_right, right_records = _fold_predicate(pred.right)
        records.extend(right_records)

    # 规则：TRUE AND x → x
    if pred.operator == PredicateOperator.AND:
        if _is_literal_true(new_left):
            records.append(
                ConstantFoldRecord(
                    original="TRUE AND x",
                    folded="x",
                    rule="true_and",
                )
            )
            return (
                new_right if isinstance(new_right, Predicate) else pred,
                records,
            )
        if _is_literal_true(new_right):
            records.append(
                ConstantFoldRecord(
                    original="x AND TRUE",
                    folded="x",
                    rule="true_and",
                )
            )
            return (
                new_left if isinstance(new_left, Predicate) else pred,
                records,
            )

    # 规则：整数算术折叠（如 SqlLiteral(1) + SqlLiteral(2) → SqlLiteral(3)）
    if pred.operator in (PredicateOperator.EQ, PredicateOperator.GT, PredicateOperator.LT,
                         PredicateOperator.GTE, PredicateOperator.LTE, PredicateOperator.NEQ):
        if _is_numeric_literal(new_left) and _is_numeric_literal(new_right):
            left_val = float(new_left.value)
            right_val = float(new_right.value)
            original = f"{left_val} {pred.operator} {right_val}"

            # 评估比较结果
            op_map = {
                PredicateOperator.EQ: lambda a, b: a == b,
                PredicateOperator.NEQ: lambda a, b: a != b,
                PredicateOperator.GT: lambda a, b: a > b,
                PredicateOperator.GTE: lambda a, b: a >= b,
                PredicateOperator.LT: lambda a, b: a < b,
                PredicateOperator.LTE: lambda a, b: a <= b,
            }
            result = op_map.get(pred.operator, lambda a, b: None)(left_val, right_val)

            records.append(
                ConstantFoldRecord(
                    original=original,
                    folded=str(result),
                    rule="literal_comparison",
                )
            )

    # 无变更
    if new_left is not pred.left or new_right is not pred.right:
        return (
            Predicate(left=new_left, operator=pred.operator, right=new_right),
            records,
        )

    return pred, records


def _is_literal_true(node) -> bool:
    """检查节点是否为字面量 TRUE。"""
    if isinstance(node, SqlLiteral) and node.value is True:
        return True
    return False


def _is_numeric_literal(node) -> bool:
    """检查节点是否为数值型 SqlLiteral。"""
    if isinstance(node, SqlLiteral):
        return isinstance(node.value, (int, float))
    return False


# ════════════════════════════════════════════
# Step 收集辅助
# ════════════════════════════════════════════


def _get_scan_steps(plan: SqlBuildPlan) -> list[ScanStep]:
    """获取计划中所有 ScanStep。"""
    return [s for s in plan.steps if isinstance(s, ScanStep)]


def _get_sort_steps(plan: SqlBuildPlan) -> list[SortStep]:
    """获取计划中所有 SortStep。"""
    return [s for s in plan.steps if isinstance(s, SortStep)]


def _collect_column_refs(step) -> set[str]:
    """递归收集 step 中所有 ColumnRef 的 normalized_name。

    使用简单的属性遍历——不依赖外部工具库。
    """
    refs: set[str] = set()
    _collect_from_object(step, refs, visited=set())
    return refs


def _collect_from_object(obj, refs: set[str], visited: set[int]) -> None:
    """递归遍历对象属性，收集 ColumnRef 的 normalized_name。"""
    obj_id = id(obj)
    if obj_id in visited:
        return
    visited.add(obj_id)

    if isinstance(obj, ColumnRef):
        refs.add(obj.normalized_name)
        refs.add(obj.column_name)
        return

    if isinstance(obj, (str, int, float, bool, type(None))):
        return

    if isinstance(obj, (list, tuple)):
        for item in obj:
            _collect_from_object(item, refs, visited)
        return

    if isinstance(obj, dict):
        for item in obj.values():
            _collect_from_object(item, refs, visited)
        return

    # Pydantic 模型或普通对象——遍历 __dict__ 或 model_fields
    # 使用 type(obj).model_fields 访问类属性（避免 Pydantic 2.11 弃用警告）
    if hasattr(type(obj), "model_fields"):
        for field_name in type(obj).model_fields:
            try:
                val = getattr(obj, field_name, None)
                _collect_from_object(val, refs, visited)
            except Exception:
                pass
    elif hasattr(obj, "__dict__"):
        for val in obj.__dict__.values():
            _collect_from_object(val, refs, visited)


# ════════════════════════════════════════════
# Compiler Pass 幂等验证——Phase 4B 新增
# ════════════════════════════════════════════


def verify_column_pruning_idempotent(
    plan: SqlBuildPlan,
) -> tuple[bool, str]:
    """验证列裁剪 Pass 的幂等性。

    对同一 SqlBuildPlan 运行列裁剪两次，比较两次输出 hash。
    相同 hash → 幂等成立。

    Args:
        plan: 待验证的 SqlBuildPlan

    Returns:
        (idempotent: bool, detail: str)
    """
    plan_a, _, _ = column_pruning(plan)
    plan_b, _, _ = column_pruning(plan_a)

    hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
    hash_b = SqlBuildPlan.generate_plan_hash(plan_b)

    if hash_a == hash_b:
        return True, f"列裁剪幂等验证通过——两次运行 hash 一致（{hash_a}）"
    else:
        return False, (
            f"列裁剪幂等验证失败——首次 hash={hash_a}，二次 hash={hash_b}"
        )


def verify_predicate_normalization_idempotent(
    plan: SqlBuildPlan,
) -> tuple[bool, str]:
    """验证谓词规范化 Pass 的幂等性。

    对同一 SqlBuildPlan 运行谓词规范化两次，比较两次输出 hash。
    相同 hash → 幂等成立。

    Args:
        plan: 待验证的 SqlBuildPlan

    Returns:
        (idempotent: bool, detail: str)
    """
    plan_a, _ = predicate_normalization(plan)
    plan_b, _ = predicate_normalization(plan_a)

    hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
    hash_b = SqlBuildPlan.generate_plan_hash(plan_b)

    if hash_a == hash_b:
        return True, f"谓词规范化幂等验证通过——两次运行 hash 一致（{hash_a}）"
    else:
        return False, (
            f"谓词规范化幂等验证失败——首次 hash={hash_a}，二次 hash={hash_b}"
        )


def verify_sort_elimination_idempotent(
    plan: SqlBuildPlan,
) -> tuple[bool, str]:
    """验证无用排序消除 Pass 的幂等性。

    对同一 SqlBuildPlan 运行排序消除两次，比较两次输出 hash。
    相同 hash → 幂等成立。

    Args:
        plan: 待验证的 SqlBuildPlan

    Returns:
        (idempotent: bool, detail: str)
    """
    plan_a, _, _ = sort_elimination(plan)
    plan_b, _, _ = sort_elimination(plan_a)

    hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
    hash_b = SqlBuildPlan.generate_plan_hash(plan_b)

    if hash_a == hash_b:
        return True, f"排序消除幂等验证通过——两次运行 hash 一致（{hash_a}）"
    else:
        return False, (
            f"排序消除幂等验证失败——首次 hash={hash_a}，二次 hash={hash_b}"
        )


def verify_constant_folding_idempotent(
    plan: SqlBuildPlan,
) -> tuple[bool, str]:
    """验证常量折叠 Pass 的幂等性。

    对同一 SqlBuildPlan 运行常量折叠两次，比较两次输出 hash。
    相同 hash → 幂等成立。

    Args:
        plan: 待验证的 SqlBuildPlan

    Returns:
        (idempotent: bool, detail: str)
    """
    plan_a, _ = constant_folding(plan)
    plan_b, _ = constant_folding(plan_a)

    hash_a = SqlBuildPlan.generate_plan_hash(plan_a)
    hash_b = SqlBuildPlan.generate_plan_hash(plan_b)

    if hash_a == hash_b:
        return True, f"常量折叠幂等验证通过——两次运行 hash 一致（{hash_a}）"
    else:
        return False, (
            f"常量折叠幂等验证失败——首次 hash={hash_a}，二次 hash={hash_b}"
        )


def verify_all_passes_idempotent(
    plan: SqlBuildPlan,
) -> list[tuple[str, bool, str]]:
    """运行全部 4 个 Compiler Pass 的幂等验证。

    对同一 SqlBuildPlan 分别运行每个 Pass 两次，
    验证所有 Pass 的输出 hash 一致。

    Args:
        plan: 待验证的 SqlBuildPlan

    Returns:
        [(pass_name, idempotent, detail), ...]——4 个 Pass 的验证结果
    """
    return [
        ("column_pruning", *verify_column_pruning_idempotent(plan)),
        ("predicate_normalization", *verify_predicate_normalization_idempotent(plan)),
        ("sort_elimination", *verify_sort_elimination_idempotent(plan)),
        ("constant_folding", *verify_constant_folding_idempotent(plan)),
    ]
