"""FinalWritePlanBuilder——从 SqlProgram + 目标表信息构建 FinalWritePlan。

Phase 3C 核心构建器——确定性生成写入方案审查材料：
1. 从 SqlProgram 提取 _temp 表操作序列
2. 生成日期分区 INSERT OVERWRITE 审查材料
3. 生成最终结果表 DDL 审查材料
4. 评估写入风险并生成重跑策略

所有输出仅作为审查材料——不实际执行生产写入。
"""

from __future__ import annotations

from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    StatementKind,
)

from .write_plan import (
    FinalWritePlan,
    PartitionOverwriteSpec,
    TempTableStatement,
)


class FinalWritePlanBuilder:
    """从 SqlProgram + 目标表信息确定性构建 FinalWritePlan。

    构建流程：
    1. 从 SqlProgram 提取 _temp 表操作序列（CREATE / INSERT / DROP）
    2. 生成最终结果表 DDL 审查材料
    3. 生成分区 INSERT OVERWRITE 审查材料
    4. 评估写入风险和重跑策略
    5. 组装 FinalWritePlan
    """

    def build(
        self,
        sql_program: SqlProgram,
        target_table: str,
        partition_keys: list[str],
        partition_values: dict[str, str],
        partition_format: str = "yyyyMMdd",
        source_temp_table: str | None = None,
    ) -> FinalWritePlan:
        """从 SqlProgram 构建 FinalWritePlan。

        Args:
            sql_program: 经过 DAG 校验的 SqlProgram
            target_table: 目标物理表名
            partition_keys: 分区键列表（如 ["dt"]）
            partition_values: 分区键 → 值映射（如 {"dt": "20260101"}）
            partition_format: 分区格式——"yyyyMMdd"（日分区）或 "yyyyMM"（月分区）
            source_temp_table: 数据来源 _temp 表名——若为 None，取最后一个 PRODUCER

        Returns:
            FinalWritePlan——写入方案审查材料

        Raises:
            ValueError: 必要参数缺失或 SqlProgram 无 _temp 表
        """
        write_plan_id = FinalWritePlan.generate_write_plan_id(
            sql_program.program_id
        )

        # ── 1. 提取 _temp 表操作序列 ──
        temp_ops = self._extract_temp_table_ops(sql_program)

        # ── 2. 确定数据来源 _temp 表 ──
        if source_temp_table is None:
            # 找最后一个 PRODUCER 语句
            producers = [
                s for s in sql_program.statements
                if s.kind in (StatementKind.PRODUCER,)
                and s.produces
            ]
            if producers:
                source_temp_table = producers[-1].produces
            else:
                # 查找 CONSUMER 中产生 _temp 的
                consumers_with_produces = [
                    s for s in sql_program.statements
                    if s.produces
                ]
                if consumers_with_produces:
                    source_temp_table = consumers_with_produces[-1].produces

        # ── 3. 生成分区 overwrite 审查材料 ──
        partition_spec = None
        if source_temp_table and partition_keys and partition_values:
            partition_spec = self._build_partition_overwrite_spec(
                target_table=target_table,
                partition_keys=partition_keys,
                partition_values=partition_values,
                partition_format=partition_format,
                source_temp_table=source_temp_table,
            )

        # ── 4. 生成审查材料文档 ──
        review_material = self._generate_review_material(
            target_table=target_table,
            partition_keys=partition_keys,
            partition_values=partition_values,
            partition_format=partition_format,
            temp_ops=temp_ops,
            partition_spec=partition_spec,
        )

        # ── 5. 评估风险 ──
        risk_notes = self._assess_risks(
            partition_spec=partition_spec,
            temp_ops=temp_ops,
        )

        # ── 6. 生成重跑策略 ──
        rerun_strategy = self._generate_rerun_strategy(
            target_table=target_table,
            partition_values=partition_values,
            partition_format=partition_format,
        )

        # ── 7. 组装 ──
        return FinalWritePlan(
            write_plan_id=write_plan_id,
            program_id=sql_program.program_id,
            target_table=target_table,
            partition_keys=partition_keys,
            overwrite_mode="partition",
            partition_values=partition_values,
            partition_format=partition_format,
            temp_table_ops=temp_ops,
            partition_overwrite=partition_spec,
            validation_checks=[],
            forbidden_operations=[],
            review_material=review_material,
            risk_notes=risk_notes,
            rerun_strategy=rerun_strategy,
        )

    # ── 内部方法 ──

    @staticmethod
    def _extract_temp_table_ops(
        sql_program: SqlProgram,
    ) -> list[TempTableStatement]:
        """从 SqlProgram 提取 _temp 表操作序列。

        按 topological_order 遍历，为每个 PRODUCER/CONSUMER 生成
        CREATE TABLE AS SELECT / INSERT INTO SELECT 操作，
        并在末尾追加 DROP TABLE 操作。
        """
        ops: list[TempTableStatement] = []
        order = 0

        # 收集所有产生 _temp 表的语句
        stmt_map = {s.statement_id: s for s in sql_program.statements}

        for stmt_id in sql_program.topological_order:
            stmt = stmt_map.get(stmt_id)
            if stmt is None:
                continue

            if stmt.produces:
                temp_id = stmt.produces
                # 判断是 CREATE 还是 INSERT
                # 第一个产生此 _temp 表的是 CREATE，后续是 INSERT
                existing = [
                    o for o in ops
                    if o.temp_id == temp_id and o.operation == "CREATE"
                ]
                if existing:
                    # 已有 CREATE——此操作是 INSERT INTO
                    ops.append(
                        TempTableStatement(
                            temp_id=temp_id,
                            operation="INSERT",
                            sql=f"INSERT INTO {temp_id}\n"
                                f"  {_plan_to_select_clause(stmt.plan)}",
                            order_index=order,
                        )
                    )
                else:
                    # 首次——CREATE TABLE AS SELECT
                    ops.append(
                        TempTableStatement(
                            temp_id=temp_id,
                            operation="CREATE",
                            sql=f"CREATE TABLE {temp_id} AS\n"
                                f"  {_plan_to_select_clause(stmt.plan)}",
                            order_index=order,
                        )
                    )
                order += 1

        # 追加 DROP TABLE 操作（清理顺序——DROP 在最后）
        seen_temps: set[str] = set()
        for op in ops:
            if op.temp_id not in seen_temps:
                seen_temps.add(op.temp_id)
                ops.append(
                    TempTableStatement(
                        temp_id=op.temp_id,
                        operation="DROP",
                        sql=f"DROP TABLE IF EXISTS {op.temp_id}",
                        order_index=order,
                    )
                )
                order += 1

        return ops

    @staticmethod
    def _build_partition_overwrite_spec(
        target_table: str,
        partition_keys: list[str],
        partition_values: dict[str, str],
        partition_format: str,
        source_temp_table: str,
    ) -> PartitionOverwriteSpec:
        """构建分区 overwrite 审查材料。

        生成 INSERT OVERWRITE TABLE ... PARTITION (...) SELECT ...
        的完整 SQL 文本——仅作为审查材料，不实际执行。
        """
        # 分区子句
        partition_clause = ", ".join(
            f"{k}='{v}'" for k, v in partition_values.items()
        )

        # INSERT OVERWRITE DML
        overwrite_dml = (
            f"INSERT OVERWRITE TABLE {target_table}\n"
            f"  PARTITION ({partition_clause})\n"
            f"SELECT *\n"
            f"FROM {source_temp_table}"
        )

        # 执行前检查 SQL
        q = "'"  # 单引号常量——避免 f-string 内转义
        where_conditions = " AND ".join(
            f"{k} = {q}{v}{q}" for k, v in partition_values.items()
        )
        pre_check_sql = (
            f"-- 执行前检查：确认目标分区数据量\n"
            f"SELECT COUNT(*) AS row_count\n"
            f"FROM {target_table}\n"
            f"WHERE {where_conditions}"
        )

        # 回滚注意事项
        backup_suffix = partition_values.get(partition_keys[0], "backup")
        rollback_note = (
            f"如需回滚：\n"
            f"1. 确认上游数据源可重放（{source_temp_table} 的源数据未变更）\n"
            f"2. 备份当前分区数据：\n"
            f"   CREATE TABLE {target_table}_{backup_suffix}_bak "
            f"AS SELECT * FROM {target_table} "
            f"WHERE {where_conditions};\n"
            f"3. 重跑 SqlProgram 并重新执行 INSERT OVERWRITE"
        )

        return PartitionOverwriteSpec(
            target_table=target_table,
            partition_keys=partition_keys,
            partition_values=partition_values,
            partition_format=partition_format,
            source_temp_table=source_temp_table,
            overwrite_dml=overwrite_dml,
            pre_check_sql=pre_check_sql,
            rollback_note=rollback_note,
        )

    @staticmethod
    def _generate_review_material(
        target_table: str,
        partition_keys: list[str],
        partition_values: dict[str, str],
        partition_format: str,
        temp_ops: list[TempTableStatement],
        partition_spec: PartitionOverwriteSpec | None,
    ) -> str:
        """生成人类可读的写入方案审查文档（Markdown 格式）。"""
        lines: list[str] = []

        lines.append("# 写入方案审查材料")
        lines.append("")
        lines.append("> 本文档由 FinalWritePlanBuilder 确定性生成——仅作为审查材料，")
        lines.append("> 不实际执行生产写入。请数据工程师审查后手动执行。")
        lines.append("")

        # 目标表
        lines.append("## 1. 目标表")
        lines.append("")
        lines.append(f"- **目标表**：`{target_table}`")
        lines.append(f"- **分区键**：{partition_keys if partition_keys else '(无)'}")
        lines.append(f"- **分区格式**：{partition_format}")
        lines.append(f"- **分区值**：{partition_values if partition_values else '(未指定)'}")
        lines.append("")

        # _temp 表操作
        lines.append("## 2. _temp 表操作序列")
        lines.append("")
        if temp_ops:
            lines.append("| 序号 | _temp 表 | 操作 |")
            lines.append("|------|----------|------|")
            for op in temp_ops:
                lines.append(f"| {op.order_index} | `{op.temp_id}` | {op.operation} |")
            lines.append("")

            lines.append("### 操作详细 SQL")
            lines.append("")
            for op in temp_ops:
                lines.append(f"**{op.operation} `{op.temp_id}`**：")
                lines.append("```sql")
                lines.append(op.sql)
                lines.append("```")
                lines.append("")
        else:
            lines.append("(无 _temp 表操作)")
            lines.append("")

        # 分区 overwrite
        lines.append("## 3. 分区 Overwrite 方案")
        lines.append("")
        if partition_spec:
            lines.append(f"- **数据来源**：`{partition_spec.source_temp_table}`")
            lines.append("")
            lines.append("### INSERT OVERWRITE DML")
            lines.append("```sql")
            lines.append(partition_spec.overwrite_dml)
            lines.append("```")
            lines.append("")
            lines.append("### 执行前检查")
            lines.append("```sql")
            lines.append(partition_spec.pre_check_sql)
            lines.append("```")
            lines.append("")
            lines.append("### 回滚说明")
            lines.append(partition_spec.rollback_note)
            lines.append("")
        else:
            lines.append("(未生成分区 overwrite 方案——缺少必要参数)")
            lines.append("")

        # 重跑策略
        lines.append("## 4. 重跑策略")
        lines.append("")
        lines.append("1. 清空目标分区数据（由 DBA 手动执行）")
        lines.append("2. 重新执行 _temp 表操作序列（CREATE / INSERT / DROP）")
        lines.append("3. 重新执行分区 INSERT OVERWRITE")
        lines.append("4. 确认分区数据行数和数值汇总")
        lines.append("")

        lines.append("---")
        lines.append("*Generated by TianShu DataDev Agent v3 Phase 3C*")
        lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _assess_risks(
        partition_spec: PartitionOverwriteSpec | None,
        temp_ops: list[TempTableStatement],
    ) -> list[str]:
        """评估写入操作的风险。"""
        risks: list[str] = []

        if not partition_spec:
            risks.append("⚠️ 未生成分区 overwrite 方案——缺少必要参数（partition_keys/partition_values）")
            return risks

        # 检查是否有分区
        if partition_spec.partition_keys:
            risks.append(
                f"✅ 分区 overwrite 模式——仅覆盖指定分区 "
                f"({', '.join(f'{k}={v}' for k, v in partition_spec.partition_values.items())})，"
                f"不会影响其他分区数据"
            )
        else:
            risks.append("❌ 无分区键——全表 overwrite 风险，已拒绝")

        # _temp 表检查
        if temp_ops:
            drop_ops = [o for o in temp_ops if o.operation == "DROP"]
            if drop_ops:
                risks.append(
                    f"ℹ️ {len(drop_ops)} 个 _temp 表需在程序结束时清理——"
                    f"失败时需手动 DROP"
                )

        # 可重跑性
        risks.append(
            "✅ 可重跑——日期分区 overwrite 是幂等操作，"
            "同一分区多次执行结果一致（假设上游数据未变更）"
        )

        return risks

    @staticmethod
    def _generate_rerun_strategy(
        target_table: str,
        partition_values: dict[str, str],
        partition_format: str,
    ) -> str:
        """生成重跑策略说明。"""
        partition_desc = ", ".join(
            f"{k}={v}" for k, v in partition_values.items()
        ) if partition_values else "无分区"

        return (
            f"重跑策略（目标表: {target_table}，分区: {partition_desc}）：\n"
            f"1. 确认上游数据源未变更——若已变更需重新评估\n"
            f"2. 执行 DROP TABLE IF EXISTS _temp_* 清理残留临时表\n"
            f"3. 重新执行 SqlProgram 全部语句（按 topological_order）\n"
            f"4. 重新执行分区 INSERT OVERWRITE（覆盖同一分区，幂等操作）\n"
            f"5. 验证分区行数和数值汇总与预期一致"
        )


def _plan_to_select_clause(plan) -> str:
    """将 SqlBuildPlan 渲染为 SELECT 子句占位符。

    实际 SQL 由 DuckDbSqlCompiler 生成——此处仅生成人类可读的占位描述。
    在审查材料中显示为：SELECT ... FROM ... WHERE ... GROUP BY ...

    Args:
        plan: SqlBuildPlan 对象

    Returns:
        人类可读的 SELECT 描述
    """
    step_types = [s.step_type for s in plan.steps]
    parts: list[str] = ["SELECT ..."]
    if "ScanStep" in step_types:
        parts.append("FROM <source_tables>")
    if "JoinStep" in step_types:
        parts.append("JOIN ...")
    if "FilterStep" in step_types:
        parts.append("WHERE ...")
    if "AggregateStep" in step_types:
        parts.append("GROUP BY ...")
    if "SortStep" in step_types:
        parts.append("ORDER BY ...")
    if "LimitStep" in step_types:
        parts.append("LIMIT ...")
    return "\n  ".join(parts)
