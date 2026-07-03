"""Phase 7B PlanComparator——SQL Plan ↔ Spark Plan 逻辑链路对比器。

封装 Phase 5 plan_equivalence.py 的 9 条对比规则和 compare_plans() 入口。
只读取 SqlBuildPlan 结构化 artifact——不读取 SQL 文本。
默认覆盖 8 类 step：scan/filter/project/sort/limit/aggregate/join/case_when。
window/subquery 仍标记 NOT_COVERED。

状态语义（精确区分）：
- NOT_EXECUTED：整个对比流程尚未执行
- NOT_COVERED：存在本 Phase 尚未启用对比的 step 类型（后续 Phase 会覆盖）
- LOGIC_UNSUPPORTED：对比规则不支持该 step 类型（如 subquery 尚无等价规则）
- LOGIC_EQUIVALENT / LOGIC_MISMATCH：已执行对比的结论
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Any

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.planning.sql_build_plan import (
    SqlBuildPlan,
    StepNode,
)
from tianshu_datadev.spark.annotations import AnnotationWarning
from tianshu_datadev.spark.models import SparkPlan
from tianshu_datadev.spark.plan_equivalence import (
    EquivalenceVerdict,
    PlanEquivalenceResult,
    StepEquivalenceResult,
    compare_plans,
)

# ════════════════════════════════════════════
# ComparisonStatus——逻辑对比状态枚举
# ════════════════════════════════════════════


class ComparisonStatus(str, Enum):
    """逻辑链路对比状态——精确描述，禁止泛化 PASS。

    状态层级：
    - NOT_EXECUTED：整个对比尚未执行（顶层入口未调用或全部 step 无对比结果）
    - NOT_COVERED：存在本 Phase 未覆盖对比的 step 类型（如 join 在 Phase 6B 才覆盖）
      已覆盖部分的对比结果有效，但整体结论需注明未覆盖范围
    - LOGIC_UNSUPPORTED：对比规则不支持该 step 类型（如 subquery 尚无等价规则）
      与 NOT_COVERED 的关键区别：NOT_COVERED 的 step 后续 Phase 会覆盖；
      LOGIC_UNSUPPORTED 的 step 需要先设计对比规则
    - LOGIC_EQUIVALENT：所有 step 的 SQL ↔ Spark 结构等价
    - LOGIC_MISMATCH：存在结构不等价的 step
    """

    LOGIC_EQUIVALENT = "LOGIC_EQUIVALENT"          # SQL ↔ Spark 结构完全等价
    LOGIC_MISMATCH = "LOGIC_MISMATCH"              # 结构不等价
    LOGIC_UNSUPPORTED = "LOGIC_UNSUPPORTED"        # 对比规则不支持（如 subquery）
    NOT_COVERED = "NOT_COVERED"                    # 本 Phase 未覆盖（后续 Phase 会覆盖）
    NOT_EXECUTED = "NOT_EXECUTED"                  # 尚未执行对比


# ════════════════════════════════════════════
# PlanComparisonReport——逻辑对比报告
# ════════════════════════════════════════════


class PlanComparisonReport(StrictModel):
    """SQL Plan ↔ Spark Plan 逻辑对比报告。

    annotation_warnings 携带但不影响 verdict（AnnotationWarning 传播规则）。
    uncovered_step_types 记录 NOT_COVERED 的 step 类型——本 Phase 未覆盖，后续 Phase 会覆盖。
    unsupported_types 记录 LOGIC_UNSUPPORTED 的 step 类型——尚无对比规则。
    """

    report_id: str                                    # 报告唯一标识
    contract_hash: str                                # 来源 Contract hash
    sql_plan_hash: str                                # SQL 侧 plan hash
    spark_plan_hash: str                              # Spark 侧 plan hash
    status: ComparisonStatus                          # 对比结论状态
    step_results: list[StepEquivalenceResult] = Field(
        default_factory=list,
        description="逐 step 类型的对比结果",
    )
    unsupported_types: list[str] = Field(
        default_factory=list,
        description="对比规则不支持对比的 step 类型（无等价规则）",
    )
    uncovered_step_types: list[str] = Field(
        default_factory=list,
        description="本 Phase 尚未覆盖对比的 step 类型（后续 Phase 会覆盖，标记 NOT_COVERED）",
    )
    annotation_warnings: list[AnnotationWarning] = Field(
        default_factory=list,
        description="携带但不影响 verdict 的语义疑点",
    )


# ════════════════════════════════════════════
# PlanComparator
# ════════════════════════════════════════════


class PlanComparator:
    """SQL Plan ↔ Spark Plan 逻辑链路对比器。

    封装 Phase 5 plan_equivalence.py 的 compare_plans() 入口和 9 条对比规则。
    只读取 SqlBuildPlan 结构化 artifact——绝不读取 SQL 文本。

    状态输出规则：
    - 全部 step 在 enabled_step_types 内且等价 → LOGIC_EQUIVALENT
    - 全部 step 在 enabled_step_types 内但有不等价 → LOGIC_MISMATCH
    - 存在不在 enabled_step_types 内的 step → NOT_COVERED（已覆盖部分结果有效）
    - 存在对比规则不支持的 step → LOGIC_UNSUPPORTED

    使用方式：
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
    """

    # Phase 7B 启用的 step 类型（8 种：6A 5 种 + 6B 3 种）
    _PHASE_7B_ENABLED_TYPES: set[str] = {
        "scan",
        "filter",
        "project",
        "sort",
        "limit",
        "aggregate",    # Phase 6B
        "join",         # Phase 6B
        "case_when",    # Phase 6B
    }

    # 需要标记为 NOT_COVERED 的 step 类型（Phase 6C/未来）
    _NOT_YET_COVERED_TYPES: set[str] = {
        "window",       # Phase 6C
        "subquery",     # 尚未设计等价对比规则
    }

    # Step 类型名 → 规范化类型名的映射（SQL 侧和 Spark 侧使用不同命名）
    _TYPE_NORMALIZE_MAP: dict[str, str] = {
        "read": "scan",  # Spark read ↔ SQL scan
    }

    def __init__(self, enabled_step_types: set[str] | None = None) -> None:
        """初始化 PlanComparator。

        Args:
            enabled_step_types: 启用逻辑对比的 step 类型集合。
                                None 时使用 Phase 7B 默认的 8 种类型。
        """
        self._enabled_types = (
            enabled_step_types
            if enabled_step_types is not None
            else self._PHASE_7B_ENABLED_TYPES.copy()
        )

    def compare(
        self,
        sql_plan: SqlBuildPlan,
        spark_plan: SparkPlan,
        annotations: list | None = None,     # noqa: ARG002 保留接口，Phase 8 消费
        warnings: list[AnnotationWarning] | None = None,
        enabled_step_types: set[str] | None = None,
    ) -> PlanComparisonReport:
        """执行逻辑链路对比。

        不在 enabled_step_types 内的类型 → NOT_COVERED（后续 Phase 会覆盖）。
        对比规则不支持的 step 类型 → LOGIC_UNSUPPORTED。
        全部在 enabled_step_types 内的类型 → 执行实际等价对比。

        Args:
            sql_plan: SQL 侧的 SqlBuildPlan（结构化 artifact，非 SQL 文本）
            spark_plan: Spark 侧的 SparkPlan
            annotations: 语义标注列表（Phase 8 消费，Phase 7 仅穿传）
            warnings: AnnotationWarning 列表（携带但不影响 verdict）
            enabled_step_types: 覆盖此实例默认的启用类型

        Returns:
            PlanComparisonReport——完整对比报告
        """
        effective_enabled = enabled_step_types or self._enabled_types

        # Step 1：提取结构化 step 数据——不读取 SQL 文本
        sql_steps_data = self._extract_sql_step_data(sql_plan)
        spark_steps_data = self._extract_spark_step_data(spark_plan)

        # Step 2：计算 hash
        sql_plan_hash = SqlBuildPlan.generate_plan_hash(sql_plan)
        spark_plan_hash = SparkPlan.compute_plan_hash(spark_plan)

        # Step 3：分类 step——已覆盖 vs 未覆盖
        covered_sql: list[dict[str, Any]] = []
        covered_spark: list[dict[str, Any]] = []
        uncovered_types: set[str] = set()

        for s in sql_steps_data:
            stype = self._normalize_type(s.get("step_type", ""))
            if stype in effective_enabled:
                covered_sql.append(s)
            else:
                uncovered_types.add(stype)

        for s in spark_steps_data:
            stype = self._normalize_type(s.get("step_type", ""))
            if stype in effective_enabled:
                covered_spark.append(s)
            else:
                uncovered_types.add(stype)

        # Step 4：执行已覆盖类型的结构等价对比
        equivalence_result: PlanEquivalenceResult
        if covered_sql or covered_spark:
            equivalence_result = compare_plans(
                sql_steps=covered_sql,
                spark_steps=covered_spark,
                sql_plan_hash=sql_plan_hash,
                spark_plan_hash=spark_plan_hash,
            )
        else:
            # 无可对比的 step——全部 NOT_EXECUTED
            equivalence_result = PlanEquivalenceResult(
                sql_plan_hash=sql_plan_hash,
                spark_plan_hash=spark_plan_hash,
                step_results=[],
                overall_verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
            )

        # Step 5：对未覆盖类型补充 NOT_EXECUTED 结果
        for utype in sorted(uncovered_types):
            already_covered = any(
                r.step_type == utype for r in equivalence_result.step_results
            )
            if not already_covered:
                equivalence_result.step_results.append(
                    StepEquivalenceResult(
                        step_type=utype,
                        verdict=EquivalenceVerdict.UNSUPPORTED_COMPARISON,
                        sql_count=self._count_type(sql_steps_data, utype),
                        spark_count=self._count_type(spark_steps_data, utype),
                        detail=f"Phase 7A 未覆盖 {utype} 类型的逻辑对比",
                    )
                )

        # Step 6：映射 verdict → ComparisonStatus
        status = self._map_status(
            equivalence_result.overall_verdict,
            has_uncovered=len(uncovered_types) > 0,
        )

        # Step 7：生成 report_id
        report_id = self._generate_report_id(
            contract_hash=sql_plan.spec_hash,
            sql_plan_hash=sql_plan_hash,
            spark_plan_hash=spark_plan_hash,
        )

        return PlanComparisonReport(
            report_id=report_id,
            contract_hash=sql_plan.spec_hash,
            sql_plan_hash=sql_plan_hash,
            spark_plan_hash=spark_plan_hash,
            status=status,
            step_results=equivalence_result.step_results,
            unsupported_types=equivalence_result.unsupported_types,
            uncovered_step_types=sorted(uncovered_types),
            annotation_warnings=list(warnings or []),
        )

    # ── 内部方法 ──

    @staticmethod
    def _extract_sql_step_data(sql_plan: SqlBuildPlan) -> list[dict[str, Any]]:
        """从 SqlBuildPlan 提取结构化 step 数据——只读 artifact，不读 SQL 文本。

        对每种 step 类型做扁平化转换，使字段名与 plan_equivalence.py 的
        单步对比函数期望的扁平格式一致。

        使用 mode='json' 确保枚举序列化为字符串值（如 "GT" 而非 PredicateOperator.GT）。
        """
        steps: list[dict[str, Any]] = []
        for step in sql_plan.steps:
            step_dict = step.model_dump(mode="json", exclude_none=True)
            # 扁平化：将嵌套字段提升到顶层，使 plan_equivalence 对比函数可消费
            step_dict = PlanComparator._normalize_step_dict(step_dict)
            # 递归处理子查询中的 step
            PlanComparator._flatten_steps(step, steps)
            steps.append(step_dict)
        return steps

    @staticmethod
    def _flatten_steps(
        step: StepNode,
        accumulator: list[dict[str, Any]],
    ) -> None:
        """递归提取子查询中的嵌套 step——结构化展开，不读 SQL 文本。"""
        step_dict = step.model_dump(exclude_none=True)
        if step_dict.get("step_type") == "subquery":
            inner_plan_data = step_dict.get("inner_plan")
            if inner_plan_data and isinstance(inner_plan_data, dict):
                inner_steps = inner_plan_data.get("steps", [])
                for inner_step in inner_steps:
                    accumulator.append(inner_step)

    @staticmethod
    def _normalize_step_dict(step_dict: dict[str, Any]) -> dict[str, Any]:
        """将 SqlBuildPlan step 的 model_dump 扁平化为 plan_equivalence 兼容格式。

        SqlBuildPlan 使用嵌套 Pydantic 模型（如 FilterStep.predicate 包含
        left/operator/right），但 plan_equivalence 的对比函数期望这些字段在
        step dict 的顶层。此方法做无损耗扁平化——不丢失任何字段。

        转换规则：
        - filter: predicate.* → 顶层（left/operator/right）
        - project: AliasExpr.expression.column_name → 顶层
        - join: join_keys (ColumnRef 对) → left_table_ref/left_key/right_key
        - aggregate: group_keys ColumnRef → 字符串，metrics aggregation → function
        - case_when: cases → labels，else_value SqlLiteral → default_value 字符串
        """
        step_type = step_dict.get("step_type", "")

        if step_type == "filter":
            return PlanComparator._flatten_filter_step(step_dict)
        if step_type == "project":
            return PlanComparator._flatten_project_step(step_dict)
        if step_type == "join":
            return PlanComparator._flatten_join_step(step_dict)
        if step_type == "aggregate":
            return PlanComparator._flatten_aggregate_step(step_dict)
        if step_type == "case_when":
            return PlanComparator._flatten_case_when_step(step_dict)
        # scan / sort / limit / window 的对比字段已在顶层，无需额外扁平化
        return step_dict

    @staticmethod
    def _flatten_filter_step(step_dict: dict[str, Any]) -> dict[str, Any]:
        """扁平化 FilterStep——将 predicate 内的字段提升到顶层。

        Predicate 模型：left (ColumnRef), operator, right (ColumnRef|SqlLiteral)
        扁平化后 left/right 为字符串，与 plan_equivalence 的 normalize_field_name 兼容。
        """
        predicate = step_dict.pop("predicate", {})
        if not predicate:
            return step_dict

        # 扁平化 left
        left_val = predicate.get("left", "")
        if isinstance(left_val, dict):
            # ColumnRef → "table_ref.column_name"
            left_val = PlanComparator._column_ref_to_string(left_val)

        # 扁平化 right
        right_val = predicate.get("right", "")
        if isinstance(right_val, dict):
            # ColumnRef 或 SqlLiteral
            right_val = PlanComparator._column_ref_to_string(right_val)
        elif right_val is None:
            right_val = ""

        # 扁平化 operator 保持不变
        operator_val = predicate.get("operator", "")

        result = dict(step_dict)
        result["left"] = str(left_val)
        result["operator"] = str(operator_val)
        result["right"] = str(right_val)
        return result

    @staticmethod
    def _flatten_project_step(step_dict: dict[str, Any]) -> dict[str, Any]:
        """扁平化 ProjectStep——将 AliasExpr.expression.column_name 提升到顶层。

        ProjectStep 的 columns 是 AliasExpr 列表，每个 AliasExpr 有：
        - expression: ColumnRef（column_name 嵌套在此）
        - alias: SafeIdentifier

        compare_project_steps 期望 columns 中每个元素有 column_name 和 alias。
        """
        raw_columns = step_dict.get("columns", [])
        if not raw_columns:
            return step_dict

        flattened_columns = []
        for col in raw_columns:
            if not isinstance(col, dict):
                flattened_columns.append(col)
                continue
            # 提取 column_name——可能在顶层或嵌套在 expression 中
            column_name = col.get("column_name", "")
            if not column_name and "expression" in col:
                expr = col["expression"]
                if isinstance(expr, dict):
                    column_name = expr.get("column_name", "")
            alias = col.get("alias", "")
            flattened_columns.append({
                "column_name": column_name,
                "alias": alias,
            })

        result = dict(step_dict)
        result["columns"] = flattened_columns
        return result

    @staticmethod
    def _flatten_join_step(step_dict: dict[str, Any]) -> dict[str, Any]:
        """扁平化 JoinStep——从 join_keys 提取 left_table_ref / left_key / right_key。

        SQL JoinStep 模型：join_keys 为 (ColumnRef, ColumnRef) 对列表，
        每个 ColumnRef 含 table_ref / column_name / normalized_name。
        对比函数期望 top-level 的 left_table_ref, right_table_ref, left_key, right_key。
        right_table_ref 已在顶层（SafeIdentifier → 字符串）。
        """
        join_keys = step_dict.pop("join_keys", [])
        result = dict(step_dict)

        if join_keys and len(join_keys) > 0:
            first_key = join_keys[0]
            # join_keys 中的每个元素是 [left_col, right_col] 列表
            if isinstance(first_key, list) and len(first_key) >= 2:
                left_col, right_col = first_key[0], first_key[1]
                if isinstance(left_col, dict):
                    result["left_table_ref"] = left_col.get("table_ref", "")
                    result["left_key"] = (
                        left_col.get("normalized_name")
                        or left_col.get("column_name", "")
                    )
                if isinstance(right_col, dict):
                    result["right_key"] = (
                        right_col.get("normalized_name")
                        or right_col.get("column_name", "")
                    )

        # 防御性默认值——确保对比函数访问时不抛 KeyError
        if "left_table_ref" not in result:
            result["left_table_ref"] = ""
        if "left_key" not in result:
            result["left_key"] = ""
        if "right_key" not in result:
            result["right_key"] = ""

        return result

    @staticmethod
    def _flatten_aggregate_step(step_dict: dict[str, Any]) -> dict[str, Any]:
        """扁平化 AggregateStep——group_keys ColumnRef → 字符串，metrics aggregation → function。

        SQL AggregateStep 模型：
        - group_keys: list[ColumnRef] → 需转为字符串列表
        - metrics: list[AggregateSpec]，其中函数名字段为 "aggregation" → 需重命名为 "function"
          （Spark 侧 SparkAggregateSpec 使用 "function" 字段名）
        """
        result = dict(step_dict)

        # 扁平化 group_keys: ColumnRef dict → 字符串
        raw_groups = result.get("group_keys", [])
        if raw_groups:
            flat_groups: list[str] = []
            for g in raw_groups:
                if isinstance(g, dict):
                    flat_groups.append(
                        str(g.get("normalized_name") or g.get("column_name", ""))
                    )
                else:
                    flat_groups.append(str(g))
            result["group_keys"] = flat_groups

        # 扁平化 metrics: aggregation → function（SQL/Spark 侧命名统一）
        raw_metrics = result.get("metrics", [])
        if raw_metrics:
            flat_metrics: list[dict[str, Any]] = []
            for m in raw_metrics:
                if isinstance(m, dict):
                    flat_m = dict(m)
                    if "aggregation" in flat_m and "function" not in flat_m:
                        flat_m["function"] = flat_m.pop("aggregation")
                    flat_metrics.append(flat_m)
                else:
                    flat_metrics.append(m)
            result["metrics"] = flat_metrics

        return result

    @staticmethod
    def _flatten_case_when_step(step_dict: dict[str, Any]) -> dict[str, Any]:
        """扁平化 CaseWhenStep——cases → labels，else_value SqlLiteral → default_value 字符串。

        SQL CaseWhenStep 模型：
        - cases: list[WhenBranch]，每个含 result: SqlLiteral → 提取 value 为 labels
        - else_value: SqlLiteral | None → 提取 value 为 default_value
        - alias: SafeIdentifier（对比函数不消费，保留不丢失）
        """
        result = dict(step_dict)

        # cases → labels: 提取每个 WhenBranch 的 result.value
        raw_cases = result.pop("cases", [])
        labels: list[str] = []
        for c in raw_cases:
            if isinstance(c, dict):
                res = c.get("result", {})
                if isinstance(res, dict):
                    labels.append(str(res.get("value", "")))
                else:
                    labels.append(str(res))
        result["labels"] = labels

        # else_value SqlLiteral → default_value 字符串
        else_val = result.pop("else_value", None)
        if else_val is not None:
            if isinstance(else_val, dict):
                result["default_value"] = str(else_val.get("value", ""))
            else:
                result["default_value"] = str(else_val)
        else:
            result["default_value"] = ""

        return result

    @staticmethod
    def _column_ref_to_string(col_ref: dict[str, Any]) -> str:
        """将 ColumnRef dict 转为字符串——"table_ref.column_name" 格式。

        也兼容 SqlLiteral dict（value 字段）。
        """
        # SqlLiteral: {"value": ...}
        if "value" in col_ref and "table_ref" not in col_ref:
            return str(col_ref["value"])
        # ColumnRef: {"table_ref": "...", "column_name": "...", ...}
        table = col_ref.get("table_ref", "")
        column = col_ref.get("column_name", "")
        if table:
            return f"{table}.{column}"
        return str(column)

    @staticmethod
    def _extract_spark_step_data(spark_plan: SparkPlan) -> list[dict[str, Any]]:
        """从 SparkPlan 提取结构化 step 数据。

        使用 mode='json' 确保 SparkStepType 等枚举序列化为字符串值。
        """
        return [
            step.model_dump(mode="json", exclude_none=True)
            for step in spark_plan.steps
        ]

    def _normalize_type(self, step_type: str) -> str:
        """将 step 类型名归一化（read → scan 等）。"""
        return self._TYPE_NORMALIZE_MAP.get(step_type, step_type)

    @staticmethod
    def _count_type(
        steps: list[dict[str, Any]],
        step_type: str,
    ) -> int:
        """统计指定类型的 step 数量。"""
        count = 0
        for s in steps:
            stype = s.get("step_type", "")
            if hasattr(stype, "value"):
                stype = stype.value
            if stype == step_type:
                count += 1
        return count

    @staticmethod
    def _map_status(
        overall_verdict: EquivalenceVerdict,
        has_uncovered: bool = False,
    ) -> ComparisonStatus:
        """将 EquivalenceVerdict 映射为 ComparisonStatus。

        语义区分：
        - has_uncovered=True → NOT_COVERED：本 Phase 未覆盖，后续 Phase 会覆盖
          已覆盖部分的对比结果仍有效（在 step_results 中可查）
        - UNSUPPORTED_COMPARISON → LOGIC_UNSUPPORTED：对比规则不支持
        - EQUIVALENT / NOT_EQUIVALENT → 直接映射
        - 其他不可达路径 → NOT_EXECUTED（防御性兜底）
        """
        if has_uncovered:
            # 存在未覆盖类型——已覆盖部分对比结果有效，整体标注 NOT_COVERED
            return ComparisonStatus.NOT_COVERED
        if overall_verdict == EquivalenceVerdict.EQUIVALENT:
            return ComparisonStatus.LOGIC_EQUIVALENT
        elif overall_verdict == EquivalenceVerdict.NOT_EQUIVALENT:
            return ComparisonStatus.LOGIC_MISMATCH
        elif overall_verdict == EquivalenceVerdict.UNSUPPORTED_COMPARISON:
            return ComparisonStatus.LOGIC_UNSUPPORTED
        # 防御性兜底——仅在所有分支都无法匹配时返回
        return ComparisonStatus.NOT_EXECUTED

    @staticmethod
    def _generate_report_id(
        contract_hash: str,
        sql_plan_hash: str,
        spark_plan_hash: str,
    ) -> str:
        """生成确定性对比报告 ID。"""
        payload = {
            "contract_hash": contract_hash,
            "sql_plan_hash": sql_plan_hash,
            "spark_plan_hash": spark_plan_hash,
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"compare_{hash_hex}"
