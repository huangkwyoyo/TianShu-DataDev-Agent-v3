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
from tianshu_datadev.planning.sql_program import SqlProgram
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

        # Step 1.5：规范化 BETWEEN 右值——SQL 侧和 Spark 侧序列化格式不同，
        # 需在进入 compare_plans 前统一为规范形式 [v1,v2]
        self._normalize_between_rights(sql_steps_data)
        self._normalize_between_rights(spark_steps_data)

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
                check_order=True,  # 单 SqlBuildPlan 路径：启用顺序检查
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

    def compare_program(
        self,
        sql_program: SqlProgram,
        spark_plan: SparkPlan,
        annotations: list | None = None,     # noqa: ARG002 保留接口，Phase 8 消费
        warnings: list[AnnotationWarning] | None = None,
        enabled_step_types: set[str] | None = None,
        target_grain: list[str] | None = None,  # 新增：目标粒度——用于过滤 DAG 中间粒度 aggregate
    ) -> PlanComparisonReport:
        """多语句 SqlProgram ↔ SparkPlan 逻辑对比入口。

        将所有 SqlStatement 的 SqlBuildPlan steps 扁平化为单一步骤列表，
        过滤 _temp_ 表 scan（内部管道——Spark 侧无对应），然后委托给核心
        compare_plans() 引擎执行等价对比。

        这是 Case06 等 ComputeSteps 路径的唯一对比入口。

        Args:
            sql_program: 多语句 SqlProgram（含 N 个 SqlStatement）
            spark_plan: Spark 侧的 SparkPlan
            annotations: 语义标注列表（Phase 8 消费，Phase 7 仅穿传）
            warnings: AnnotationWarning 列表（携带但不影响 verdict）
            enabled_step_types: 覆盖此实例默认的启用类型

        Returns:
            PlanComparisonReport——完整对比报告
        """
        effective_enabled = enabled_step_types or self._enabled_types

        # Step 1：扁平化 SqlProgram 所有 statement 的 step——过滤 _temp_* scan
        sql_steps_data = self._flatten_sql_program_steps(sql_program)
        spark_steps_data = self._extract_spark_step_data(spark_plan)

        # Step 1.3：DAG 归一化——合并多个 aggregate/project step
        # 使 SQL DAG 的多语句结构与 Mapper 从平铺 Contract 生成的
        # 单 aggregate/单 project 结构对齐——消除拓扑不对称
        sql_steps_data = self._normalize_dag_steps(sql_steps_data, target_grain=target_grain)

        # Step 1.5：规范化 BETWEEN 右值——SQL 侧和 Spark 侧序列化格式不同
        self._normalize_between_rights(sql_steps_data)
        self._normalize_between_rights(spark_steps_data)

        # Step 2：计算 hash
        sql_plan_hash = self._compute_sql_program_hash(sql_program)
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
                check_order=False,  # SqlProgram 路径：DAG 扁平化后顺序无意义
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
            contract_hash=sql_program.spec_id,
            sql_plan_hash=sql_plan_hash,
            spark_plan_hash=spark_plan_hash,
        )

        return PlanComparisonReport(
            report_id=report_id,
            contract_hash=sql_program.spec_id,
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
    def _flatten_sql_program_steps(
        sql_program: SqlProgram,
    ) -> list[dict[str, Any]]:
        """从 SqlProgram 扁平化所有 step 数据。

        规则：
        1. 按拓扑顺序遍历所有 SqlStatement.plan.steps
        2. 跳过 scan step 中 table_ref 以 _temp_ 开头的（DAG 内部管道）
        3. 保留所有源表 scan 和所有语义 step（filter/aggregate/join/project/…）
        4. 对每个 step 做与单 Plan 路径一致的扁平化（_normalize_step_dict）
        5. 递归提取子查询中的嵌套 step（_flatten_steps）
        """
        all_steps: list[dict[str, Any]] = []

        # 按拓扑顺序遍历——确保步骤顺序确定性
        order = (
            sql_program.topological_order
            if sql_program.topological_order
            else [s.statement_id for s in sql_program.statements]
        )
        statement_by_id = {s.statement_id: s for s in sql_program.statements}

        for stmt_id in order:
            stmt = statement_by_id.get(stmt_id)
            if stmt is None:
                continue
            for step in stmt.plan.steps:
                step_dict = step.model_dump(mode="json", exclude_none=True)
                # 扁平化——与单 Plan 路径一致的归一化
                step_dict = PlanComparator._normalize_step_dict(step_dict)
                # 递归提取子查询中的嵌套 step
                PlanComparator._flatten_steps(step, all_steps)

                # 过滤 _temp_* scan：DAG 内部管道——Spark 侧通过变量传递 DataFrame，
                # 不存在临时表概念，这些 scan 不应参与对比
                step_type = step_dict.get("step_type", "")
                if step_type == "scan":
                    table_ref = step_dict.get("table_ref", "")
                    if isinstance(table_ref, str) and table_ref.startswith("_temp_"):
                        continue  # 跳过 _temp_ 中间表 scan

                # 过滤 _temp_* join：DAG 内部管道 join——_temp_ 表之间的
                # 关联是 DAG 实现细节，Spark 侧 Mapper 从 Contract 生成的
                # SparkPlan 不包含这些中间表 join
                if step_type == "join":
                    lt = step_dict.get("left_table_ref", "")
                    rt = step_dict.get("right_table_ref", "")
                    if (isinstance(lt, str) and lt.startswith("_temp_")) or \
                       (isinstance(rt, str) and rt.startswith("_temp_")):
                        continue  # 跳过 _temp_ 中间表 join

                all_steps.append(step_dict)

        return all_steps

    @staticmethod
    def _normalize_dag_steps(
        sql_steps: list[dict[str, Any]],
        target_grain: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """将 DAG 扁平化产生的多个同类型 step 合并为单一步骤。

        合并规则：
        1. aggregate：按 group_keys 签名分组合并——同粒度合并，不同粒度独立
        2. project：合并所有 columns（去重按 alias）
        3. 其他类型（scan/filter/join/case_when/sort/limit）：保持原样
        4. 若提供 target_grain，只保留 group_keys 签名匹配的 aggregate 组

        此归一化使 SQL DAG 的多语句结构与 Mapper 从平铺 Contract
        生成的单 aggregate/单 project 结构对齐。

        Args:
            sql_steps: _flatten_sql_program_steps() 产出的扁平化步骤
            target_grain: 可选——目标粒度（如 ["borough"]），
                          非空时只保留匹配的 aggregate 组

        Returns:
            归一化后的步骤列表
        """
        result: list[dict[str, Any]] = []
        proj_columns: list[dict[str, Any]] = []
        seen_proj_aliases: set[str] = set()
        has_project = False

        # 收集所有 project steps——仅当 target_grain 非空时只保留最后一个（FINAL 输出）
        all_project_steps: list[list[dict[str, Any]]] = []

        # aggregate 按 group_keys 签名分组合并——禁止跨粒度硬合并
        agg_groups: dict[tuple, dict] = {}
        for step in sql_steps:
            stype = step.get("step_type", "")
            if stype == "aggregate":
                gk_tuple = tuple(sorted(step.get("group_keys", [])))
                if gk_tuple not in agg_groups:
                    agg_groups[gk_tuple] = {
                        "group_keys": list(gk_tuple),
                        "metrics": [],
                        "seen_aliases": set(),
                    }
                for m in step.get("metrics", []):
                    alias = m.get("alias", "")
                    if alias not in agg_groups[gk_tuple]["seen_aliases"]:
                        agg_groups[gk_tuple]["seen_aliases"].add(alias)
                        agg_groups[gk_tuple]["metrics"].append(m)
            elif stype == "project":
                has_project = True
                if target_grain is not None:
                    # target_grain 路径——收集所有 project 步，后续只保留最后一个
                    all_project_steps.append(step.get("columns", []) or [])
                else:
                    # 单 Plan 路径——合并所有 project 步的列（旧行为）
                    for col in step.get("columns", []):
                        alias = col.get("alias", "")
                        if alias not in seen_proj_aliases:
                            seen_proj_aliases.add(alias)
                            proj_columns.append(col)
            else:
                result.append(step)

        # B2：target_grain 过滤——保留匹配的 aggregate
        if target_grain is not None:
            target_set = set(target_grain)
            # 检查是否有 aggregate 的 grain 精确匹配 target_grain
            has_exact_match = any(set(gk) == target_set for gk in agg_groups)
            if has_exact_match:
                # 精确匹配模式：只保留 grain 完全等于 target_set 的 aggregate
                agg_groups = {
                    gk: data for gk, data in agg_groups.items()
                    if set(gk) == target_set
                }
            elif agg_groups:
                # 无精确匹配——所有 aggregate 的 grain 都是 target_set 的子集
                #（如 ["borough"] 和 ["violation_county"] 都是 {"borough","violation_county"} 的子集）。
                # 此时合并所有 aggregate 为 1 个，与 Mapper 从 Contract 平铺
                # aggregation/grouping_keys 生成 1 个 SparkAggregateStep 的行为对称。
                all_group_keys: list[str] = []
                seen_gk: set[str] = set()
                all_metrics: list[dict[str, Any]] = []
                seen_metric_aliases: set[str] = set()
                for gk_tuple, agg_data in agg_groups.items():
                    for gk in agg_data["group_keys"]:
                        if gk not in seen_gk:
                            seen_gk.add(gk)
                            all_group_keys.append(gk)
                    for m in agg_data["metrics"]:
                        alias = m.get("alias", "")
                        if alias not in seen_metric_aliases:
                            seen_metric_aliases.add(alias)
                            all_metrics.append(m)
                agg_groups = {
                    tuple(sorted(all_group_keys)): {
                        "group_keys": all_group_keys,
                        "metrics": all_metrics,
                        "seen_aliases": seen_metric_aliases,
                    }
                }

        # 将分组后的 aggregate 按原始出现顺序插入 result
        # 修复：多个 grain 组计算同一 insert_pos 时顺序反转的 bug
        if agg_groups:
            # 记录每个 grain 在原始 sql_steps 中的首次出现位置
            grain_first_pos: dict[tuple, int] = {}
            for idx, step in enumerate(sql_steps):
                if step.get("step_type") == "aggregate":
                    gk = tuple(sorted(step.get("group_keys", [])))
                    if gk not in grain_first_pos:
                        grain_first_pos[gk] = idx

            # 按首次出现位置排序
            sorted_groups = sorted(
                agg_groups.items(),
                key=lambda item: grain_first_pos.get(item[0], 9999),
            )

            # 找到插入位置（最后一个 scan/filter/join/read 之后）
            insert_pos = 0
            for i, s in enumerate(result):
                if s.get("step_type") in ("scan", "filter", "join", "read"):
                    insert_pos = i + 1

            # 按顺序一次性插入所有 aggregate，每个后续 aggregate 插入位置递增
            for gk_tuple, agg_data in sorted_groups:
                merged_agg = {
                    "step_type": "aggregate",
                    "group_keys": agg_data["group_keys"],
                    "metrics": agg_data["metrics"],
                }
                result.insert(insert_pos, merged_agg)
                insert_pos += 1

        # 将合并后的 project 追加到末尾
        if has_project and all_project_steps:
            # target_grain 路径：只保留最后一个 project step 的列（FINAL 输出）
            # 中间 project step 中的临时列（如 total_fare、violation_county）不是最终输出，
            # 但为衍生列补全 column_name（如 crash_per_million_trips 的 column_name
            # 在中间 step 中为空，最终 step 中有值）
            last_project_cols = all_project_steps[-1]
            merged_cols: list[dict[str, Any]] = []
            col_name_fallback: dict[str, str] = {}
            for cols_list in all_project_steps[:-1]:
                for col in cols_list:
                    alias = col.get("alias", "")
                    cname = col.get("column_name", "")
                    if alias and cname and alias not in col_name_fallback:
                        col_name_fallback[alias] = cname
            for col in last_project_cols:
                entry = dict(col)
                if not entry.get("column_name", "") and entry.get("alias", "") in col_name_fallback:
                    entry["column_name"] = col_name_fallback[entry["alias"]]
                merged_cols.append(entry)
            result.append({
                "step_type": "project",
                "columns": merged_cols,
            })
        elif has_project:
            # 单 Plan 路径（target_grain is None）：合并所有 project 步的列（旧行为）
            result.append({
                "step_type": "project",
                "columns": proj_columns,
            })

        return result

    @staticmethod
    def _compute_sql_program_hash(sql_program: SqlProgram) -> str:
        """计算 SqlProgram 的确定性 hash——基于程序结构而非单一 plan。

        与 SqlBuildPlan.generate_plan_hash() 对应，但覆盖多语句 DAG 的
        完整结构：program_id + statement_ids + topological_order。
        """
        program_data = {
            "program_id": sql_program.program_id,
            "statement_ids": [s.statement_id for s in sql_program.statements],
            "topological_order": sql_program.topological_order,
        }
        content = json.dumps(program_data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

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

        BETWEEN 谓词的 right 是一个 SqlLiteral 列表，需逐元素提取 value
        字段生成规范字符串——避免 SQL 侧 dict 列表和 Spark 侧 Python repr
        字符串之间的表示形式差异导致误判 NOT_EQUIVALENT。
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
        elif isinstance(right_val, list):
            # BETWEEN 谓词：right 是 SqlLiteral 列表，逐元素提取 value
            right_val = PlanComparator._normalize_between_list(right_val)

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
    def _normalize_between_list(items: list[Any]) -> str:
        """将 BETWEEN 右值列表规范化为 [v1,v2] 形式。

        SQL 侧 model_dump(mode='json') 将 SqlLiteral 列表序列化为
        [{'value': '...', 'is_sql_expr': false}, ...] 格式。
        此方法逐元素提取 value 字段，生成确定性规范字符串，
        使 SQL 侧与 Spark 侧（Python repr）可正确比较。
        """
        values: list[str] = []
        for item in items:
            if isinstance(item, dict):
                values.append(str(item.get("value", "")))
            else:
                values.append(str(item))
        return "[" + ",".join(values) + "]"

    @staticmethod
    def _normalize_between_right_string(right_str: str) -> str:
        """将 Spark 侧的 BETWEEN 右值 Python repr 字符串规范化为 [v1,v2]。

        Spark 侧 Mapper 将 ContractPredicate.right（已是 SqlLiteral 列表的
        Python repr 字符串）直传给 SparkFilterStep.right。
        此方法从 repr 字符串中提取 value='...' 部分，生成与
        _normalize_between_list 一致的规范形式。
        """
        import re

        right_str = right_str.strip()
        if not (right_str.startswith("[") and right_str.endswith("]")):
            return right_str

        # 从 "SqlLiteral(value='...', ...)" 或 "{'value': '...', ...}" 中提取 value
        values = re.findall(r"value['\"]?\s*[:=]\s*['\"]?([^'\",}\s)]+)", right_str)
        if values:
            return "[" + ",".join(values) + "]"
        return right_str

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
    def _normalize_between_rights(steps_data: list[dict[str, Any]]) -> None:
        """原地规范化所有 filter step 的 BETWEEN 右值。

        SQL 侧的 BETWEEN 右值在 _flatten_filter_step 中已由
        _normalize_between_list 处理（list → 规范字符串）。
        此方法处理 Spark 侧——其 right 字段是 Python repr 字符串，
        需提取 value 后重写为规范形式。

        对非 filter 或非 BETWEEN 的 step 无操作。
        """
        for s in steps_data:
            stype = s.get("step_type", "")
            if stype != "filter":
                continue
            operator = str(s.get("operator", "")).upper()
            if operator != "BETWEEN":
                continue
            right_val = s.get("right", "")
            if isinstance(right_val, list):
                # SQL 侧已在 _flatten_filter_step 处理过，此处防御
                s["right"] = PlanComparator._normalize_between_list(right_val)
            elif isinstance(right_val, str):
                s["right"] = PlanComparator._normalize_between_right_string(right_val)

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
