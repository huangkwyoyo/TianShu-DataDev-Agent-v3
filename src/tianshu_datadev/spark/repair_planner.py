"""Phase 7C RepairPlanner——验证失败后的故障分类与修复路由。

RepairPlanner 从 PhysicalVerificationReport 推断根因类别，产出 RepairAction。
不做任何自动修改——只建议修复方向和目标文件，由 Orchestrator 决定是否执行。

RepairAction 5 种分类：
- MAPPER_BUG：Contract → SparkPlan 映射阶段丢失/错误信息
- COMPILER_BUG：SparkPlan → PySpark DSL 编译阶段渲染/逻辑错误
- VALIDATOR_GAP：上游校验缺失（如未检查排序键存在性）
- SNAPSHOT_ISSUE：快照数据异常（文件损坏、格式错误）
- BUSINESS_SEMANTIC：业务语义歧义——无法自动判定，需人工介入

路由规则：
- MAPPER_BUG → mapper.py
- COMPILER_BUG → compiler.py
- VALIDATOR_GAP → validator.py
- SNAPSHOT_ISSUE → snapshot.py
- BUSINESS_SEMANTIC → HUMAN_REVIEW（不自动修改任何文件）

返工上限：MAX_RETRY = 2——超出后强制 HUMAN_REVIEW。
"""

from __future__ import annotations

from enum import Enum

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.physical_verifier import PhysicalVerificationReport, PhysicalVerificationStatus

# ════════════════════════════════════════════
# RepairActionType——5 种故障分类
# ════════════════════════════════════════════


class RepairActionType(str, Enum):
    """修复动作类型——精确指向故障根因所在的阶段。

    MAPPER_BUG：Contract → SparkPlan 映射阶段丢失/错误信息
    COMPILER_BUG：SparkPlan → PySpark DSL 编译阶段渲染/逻辑错误
    VALIDATOR_GAP：上游校验缺失——应在上游拦截但未拦截
    SNAPSHOT_ISSUE：快照数据异常——文件损坏、格式错误、数据不一致
    BUSINESS_SEMANTIC：业务语义歧义——无法自动判定正确行为
    """

    MAPPER_BUG = "MAPPER_BUG"
    COMPILER_BUG = "COMPILER_BUG"
    VALIDATOR_GAP = "VALIDATOR_GAP"
    SNAPSHOT_ISSUE = "SNAPSHOT_ISSUE"
    BUSINESS_SEMANTIC = "BUSINESS_SEMANTIC"


# ════════════════════════════════════════════
# RepairAction——修复动作模型
# ════════════════════════════════════════════


class RepairAction(StrictModel):
    """修复动作——描述故障分类、目标文件和修复建议。

    不包含任何自动修改逻辑——仅描述问题和建议方向。
    Orchestrator 根据此建议决定是否执行修复。
    """

    action_type: RepairActionType
    description: str         # 故障描述——人类可读
    target_file: str         # 目标文件（如 "mapper.py"）或 "HUMAN_REVIEW"
    suggested_fix: str       # 修复建议——人类可读
    retry_count: int = 0     # 当前返工轮次


# ════════════════════════════════════════════
# RepairPlanner
# ════════════════════════════════════════════


class RepairPlanner:
    """验证失败故障分类器——从 PhysicalVerificationReport 推断 RepaiAction。

    分类逻辑（按优先级排序）：
    1. retry_count >= MAX_RETRY → BUSINESS_SEMANTIC（强制人工介入）
    2. UNSUPPORTED_SEMANTICS → BUSINESS_SEMANTIC
    3. CANONICALIZATION_NEEDED → VALIDATOR_GAP
    4. EXECUTION_ERROR：
       - DuckDB 失败（无 Spark 结果或 Spark 成功）→ SNAPSHOT_ISSUE
       - Spark 失败（DuckDB 成功）→ COMPILER_BUG
    5. RESULT_MISMATCH：
       - schema_match=False → MAPPER_BUG（schema 不对齐 → 映射问题）
       - schema_match=True → COMPILER_BUG（值差异 → 编译逻辑问题）
    6. 其他未覆盖状态 → BUSINESS_SEMANTIC

    使用方式：
        planner = RepairPlanner()
        action = planner.plan(report, retry_count=0)
        target = planner.route(action)  # "mapper.py" 或 "HUMAN_REVIEW"
    """

    # 最多允许 2 轮自动返工——超出后强制人工介入
    MAX_RETRY = 2

    # 分类 → 目标文件路由表
    _ROUTING_TABLE: dict[RepairActionType, str] = {
        RepairActionType.MAPPER_BUG: "mapper.py",
        RepairActionType.COMPILER_BUG: "compiler.py",
        RepairActionType.VALIDATOR_GAP: "validator.py",
        RepairActionType.SNAPSHOT_ISSUE: "snapshot.py",
        RepairActionType.BUSINESS_SEMANTIC: "HUMAN_REVIEW",
    }

    def plan(
        self,
        report: PhysicalVerificationReport,
        retry_count: int = 0,
    ) -> RepairAction:
        """从验证报告推断修复动作。

        Args:
            report: PhysicalVerifier.verify() 产出的完整验证报告
            retry_count: 当前返工轮次（0 表示首次）

        Returns:
            RepairAction——含分类、目标文件和修复建议
        """
        # 优先级 1：返工上限检查
        if retry_count >= self.MAX_RETRY:
            return RepairAction(
                action_type=RepairActionType.BUSINESS_SEMANTIC,
                description=(
                    f"已达返工上限（{self.MAX_RETRY} 轮），无法自动修复。"
                    f"上次状态：{report.status.value}。{report.error_message}"
                ),
                target_file="HUMAN_REVIEW",
                suggested_fix="需数据工程师人工分析根因并决定修复方向",
                retry_count=retry_count,
            )

        # 优先级 2：不支持的类型 → 人工判断
        if report.status == PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS:
            uncovered = report.uncovered_step_types
            return RepairAction(
                action_type=RepairActionType.BUSINESS_SEMANTIC,
                description=(
                    f"包含不支持的 step 类型 {uncovered}——"
                    f"无法自动进行物理验证。{report.error_message}"
                ),
                target_file="HUMAN_REVIEW",
                suggested_fix=(
                    f"确认是否需要支持 {uncovered} 类型的物理验证，"
                    f"若是则需扩展 PhysicalVerifier 和对比规则"
                ),
                retry_count=retry_count,
            )

        # 优先级 3：缺失排序键 → 上游校验缺失
        if report.status == PhysicalVerificationStatus.CANONICALIZATION_NEEDED:
            return RepairAction(
                action_type=RepairActionType.VALIDATOR_GAP,
                description=(
                    f"结果规范化失败——缺少排序键。{report.error_message}"
                ),
                target_file="validator.py",
                suggested_fix=(
                    "在 SparkStaticValidator 中增加排序键存在性检查，"
                    "确保 Comparator 调用前已提供 order_keys"
                ),
                retry_count=retry_count,
            )

        # 优先级 4：执行错误
        if report.status == PhysicalVerificationStatus.EXECUTION_ERROR:
            return self._classify_execution_error(report, retry_count)

        # 优先级 5：结果不一致
        if report.status == PhysicalVerificationStatus.RESULT_MISMATCH:
            return self._classify_mismatch(report, retry_count)

        # 优先级 6：其他未覆盖状态 → 人工判断
        return RepairAction(
            action_type=RepairActionType.BUSINESS_SEMANTIC,
            description=(
                f"无法自动分类的验证状态：{report.status.value}。"
                f"{report.error_message}"
            ),
            target_file="HUMAN_REVIEW",
            suggested_fix="需数据工程师人工分析验证报告并决定下一步",
            retry_count=retry_count,
        )

    def route(self, action: RepairAction) -> str:
        """路由修复动作到目标文件。

        返回目标文件名或 "HUMAN_REVIEW"。

        Args:
            action: plan() 产出的 RepairAction

        Returns:
            目标文件名字符串
        """
        return self._ROUTING_TABLE.get(action.action_type, "HUMAN_REVIEW")

    # ── 内部分类辅助 ──

    @staticmethod
    def _classify_execution_error(
        report: PhysicalVerificationReport,
        retry_count: int,
    ) -> RepairAction:
        """分类 EXECUTION_ERROR——区分快照问题 vs 编译问题。"""
        duckdb_failed = (
            report.duckdb_result is None
            or not report.duckdb_result.success
        )
        spark_failed = (
            report.spark_result is None
            or not report.spark_result.success
        )

        # DuckDB 失败（且 Spark 也失败或未执行）→ 快照数据问题
        if duckdb_failed:
            duckdb_error = (
                report.duckdb_result.error_message
                if report.duckdb_result
                else report.error_message
            )
            return RepairAction(
                action_type=RepairActionType.SNAPSHOT_ISSUE,
                description=(
                    f"DuckDB（基准引擎）执行失败——快照数据可能异常。"
                    f"错误：{duckdb_error[:200]}"
                ),
                target_file="snapshot.py",
                suggested_fix=(
                    "检查快照 Parquet 文件完整性，"
                    "确认 SnapshotBuilder 产出的数据可被 DuckDB 正常读取"
                ),
                retry_count=retry_count,
            )

        # DuckDB 成功但 Spark 失败 → 编译产物问题
        if spark_failed:
            spark_error = (
                report.spark_result.error_message
                if report.spark_result
                else report.error_message
            )
            return RepairAction(
                action_type=RepairActionType.COMPILER_BUG,
                description=(
                    f"DuckDB 执行成功但 Spark 执行失败——"
                    f"编译产物可能在运行时出错。错误：{spark_error[:200]}"
                ),
                target_file="compiler.py",
                suggested_fix=(
                    "检查 compiler.py 生成的 PySpark DSL 代码，"
                    "确认窗口函数调用、列引用和 WindowSpec 语法正确"
                ),
                retry_count=retry_count,
            )

        # 双引擎都失败 → 无法自动判定
        return RepairAction(
            action_type=RepairActionType.BUSINESS_SEMANTIC,
            description=(
                f"双引擎执行均失败——无法自动判定根因。"
                f"错误：{report.error_message[:200]}"
            ),
            target_file="HUMAN_REVIEW",
            suggested_fix="需人工检查执行环境（DuckDB + PySpark）和快照数据",
            retry_count=retry_count,
        )

    @staticmethod
    def _classify_mismatch(
        report: PhysicalVerificationReport,
        retry_count: int,
    ) -> RepairAction:
        """分类 RESULT_MISMATCH——区分映射问题 vs 编译问题。"""
        # Schema 不匹配 → 阶段太早——映射阶段就丢了列/改了结构
        if not report.schema_match:
            return RepairAction(
                action_type=RepairActionType.MAPPER_BUG,
                description=(
                    f"双引擎结果 schema 不一致——"
                    f"可能是 mapper 阶段列信息映射错误。"
                    f"差异数：{len(report.diffs)}"
                ),
                target_file="mapper.py",
                suggested_fix=(
                    "检查 mapper.py 中对应 step 类型的映射逻辑，"
                    "确认 Contract 字段到 SparkPlan 字段的传递无丢失/变形"
                ),
                retry_count=retry_count,
            )

        # Schema 匹配但值不一致 → 编译阶段逻辑错误
        diff_summary = ""
        if report.diffs:
            first = report.diffs[0]
            diff_summary = (
                f"首个差异：第 {first.row_index} 行 {first.column} 列，"
                f"DuckDB={first.duckdb_value[:50]}，"
                f"Spark={first.spark_value[:50]}"
            )
        return RepairAction(
            action_type=RepairActionType.COMPILER_BUG,
            description=(
                f"双引擎结果 schema 一致但值不一致——"
                f"可能是 compiler 渲染逻辑错误。{diff_summary}"
            ),
            target_file="compiler.py",
            suggested_fix=(
                "检查 compiler.py 中对应 step 类型的编译方法，"
                "确认窗口帧边界、聚合表达式和列引用的渲染正确"
            ),
            retry_count=retry_count,
        )
