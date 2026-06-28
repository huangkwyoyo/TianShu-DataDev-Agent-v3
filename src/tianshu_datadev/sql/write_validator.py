"""WriteValidator——写入方案安全校验器。

Phase 3C 安全门禁——在 FinalWritePlan 进入审查包之前执行以下检查：
1. 仅允许日期分区 overwrite（PARTITION 模式）
2. 禁止全表 overwrite（无 PARTITION 子句）
3. 禁止无分区字段的 overwrite
4. 禁止 UPDATE / DELETE / MERGE 语句
5. 禁止非 overwrite 的 INSERT INTO
6. _temp 表操作仅允许 CREATE / INSERT / DROP
7. 分区值格式校验（yyyyMMdd / yyyyMM）
8. 分区字段存在性校验
"""

from __future__ import annotations

import re

from tianshu_datadev.planning.sql_program import SqlProgram

from .write_plan import (
    FORBIDDEN_PRODUCTION_OPS,
    VALID_TEMP_OPS,
    FinalWritePlan,
    WriteValidationCheck,
)

# ════════════════════════════════════════════
# WV-009: INSERT OVERWRITE DML 形状正则
# ════════════════════════════════════════════

# 仅允许精确形状：INSERT OVERWRITE TABLE <table> ... PARTITION (...) ... SELECT ... FROM <table>
# 使用宽松的结构化检查——验证三个关键组件按序出现，而非固定空白格式
_INSERT_OVERWRITE_PARTITION_RE = re.compile(
    r"^INSERT\s+OVERWRITE\s+TABLE\s+\S+"
    r".*"
    r"PARTITION\s*\([^)]+\)"
    r".*"
    r"SELECT\s+"
    r".*"
    r"FROM\s+\S+\s*$",
    re.IGNORECASE | re.DOTALL,
)


class WriteValidator:
    """写入方案校验器——拒绝不安全的写入操作。

    所有校验规则均为硬门禁（REJECT），不通过则阻断 FinalWritePlan 生成。
    不区分 WARN 级别——写入安全无"警告"空间。
    """

    def validate(
        self,
        write_plan: FinalWritePlan,
        sql_program: SqlProgram | None = None,
    ) -> FinalWritePlan:
        """校验 FinalWritePlan 的完整合法性。

        执行全部 9 项校验，任一不通过则记录在 validation_checks 中。
        forbidden_operations 非空表示发现被禁止的操作。

        Args:
            write_plan: 待校验的 FinalWritePlan
            sql_program: 可选的 SqlProgram——用于校验 _temp 表引用的有效性

        Returns:
            更新后的 FinalWritePlan（含 validation_checks 和 forbidden_operations）
        """
        checks: list[WriteValidationCheck] = []
        forbidden: list[str] = []

        # ── 1. 分区 overwrite 模式检查 ──
        if write_plan.overwrite_mode != "partition":
            checks.append(
                WriteValidationCheck(
                    check_id="WV-001",
                    check_type="overwrite_mode",
                    passed=False,
                    detail=(
                        f"不支持的 overwrite 模式：'{write_plan.overwrite_mode}'——"
                        f"仅允许 'partition' 模式（日期分区 overwrite）"
                    ),
                )
            )
            forbidden.append(f"overwrite_mode={write_plan.overwrite_mode}")
        else:
            checks.append(
                WriteValidationCheck(
                    check_id="WV-001",
                    check_type="overwrite_mode",
                    passed=True,
                    detail="overwrite_mode='partition'——仅允许分区 overwrite",
                )
            )

        # ── 2. 分区键非空检查 ──
        if not write_plan.partition_keys:
            checks.append(
                WriteValidationCheck(
                    check_id="WV-002",
                    check_type="no_partition_keys",
                    passed=False,
                    detail="缺少分区键——禁止无分区 overwrite（全表覆盖）",
                )
            )
            forbidden.append("全表 overwrite（无分区键）")
        else:
            checks.append(
                WriteValidationCheck(
                    check_id="WV-002",
                    check_type="no_partition_keys",
                    passed=True,
                    detail=f"分区键已声明：{write_plan.partition_keys}",
                )
            )

        # ── 3. 分区值非空检查 ──
        if not write_plan.partition_values:
            checks.append(
                WriteValidationCheck(
                    check_id="WV-003",
                    check_type="no_partition_values",
                    passed=False,
                    detail="缺少分区值——禁止无分区 overwrite（必须指定具体分区）",
                )
            )
            forbidden.append("无分区 overwrite（缺少 partition_values）")
        else:
            checks.append(
                WriteValidationCheck(
                    check_id="WV-003",
                    check_type="no_partition_values",
                    passed=True,
                    detail=f"分区值已指定：{write_plan.partition_values}",
                )
            )

        # ── 4. 分区格式校验 ──
        if write_plan.partition_format and write_plan.partition_values:
            from .write_plan import validate_partition_format

            format_checks = validate_partition_format(
                write_plan.partition_values,
                write_plan.partition_format,
            )
            checks.extend(format_checks)
            # 检查是否有格式不匹配
            for fc in format_checks:
                if not fc.passed:
                    forbidden.append(f"分区格式不匹配：{fc.detail}")

        # ── 5. 禁止生产 DML 操作检查（UPDATE / DELETE / MERGE）──

        # 扫描所有 _temp 操作——确保不含禁止操作
        for op in write_plan.temp_table_ops:
            if op.operation.upper() in FORBIDDEN_PRODUCTION_OPS:
                checks.append(
                    WriteValidationCheck(
                        check_id="WV-005",
                        check_type="forbidden_production_op",
                        passed=False,
                        detail=(
                            f"_temp 表操作 '{op.operation}' 在 "
                            f"'{op.temp_id}' 中被拒绝——"
                            f"不允许 UPDATE / DELETE / MERGE"
                        ),
                    )
                )
                forbidden.append(f"FORBIDDEN: {op.operation} on {op.temp_id}")

        # ── 6. _temp 操作合法性检查 ──
        for op in write_plan.temp_table_ops:
            if op.operation not in VALID_TEMP_OPS:
                checks.append(
                    WriteValidationCheck(
                        check_id="WV-006",
                        check_type="invalid_temp_op",
                        passed=False,
                        detail=(
                            f"_temp 表操作 '{op.operation}' 无效——"
                            f"仅允许 {sorted(VALID_TEMP_OPS)}"
                        ),
                    )
                )
                forbidden.append(f"INVALID_TEMP_OP: {op.operation}")

        # ── 7. 分区 overwrite 规格中的 DML 检查 ──
        if write_plan.partition_overwrite:
            dml = write_plan.partition_overwrite.overwrite_dml.upper()
            for forbidden_op in FORBIDDEN_PRODUCTION_OPS:
                if forbidden_op in dml:
                    checks.append(
                        WriteValidationCheck(
                            check_id="WV-007",
                            check_type="forbidden_dml_in_partition_spec",
                            passed=False,
                            detail=(
                                f"分区 overwrite DML 中包含禁止操作 '{forbidden_op}'——"
                                f"仅允许 INSERT OVERWRITE"
                            ),
                        )
                    )
                    forbidden.append(
                        f"FORBIDDEN in partition DML: {forbidden_op}"
                    )
                    break  # 一个禁止操作已足够标记失败

        # ── 8. 全表 overwrite 检测 ──
        if write_plan.partition_overwrite:
            dml_upper = write_plan.partition_overwrite.overwrite_dml.upper()
            # 仅允许 INSERT OVERWRITE TABLE xxx PARTITION (...)
            if "PARTITION" not in dml_upper and "OVERWRITE" in dml_upper:
                checks.append(
                    WriteValidationCheck(
                        check_id="WV-008",
                        check_type="full_table_overwrite",
                        passed=False,
                        detail=(
                            "分区 overwrite DML 缺少 PARTITION 子句——"
                            "禁止全表 overwrite"
                        ),
                    )
                )
                forbidden.append("全表 overwrite（缺少 PARTITION 子句）")

        # ── 9. DML 形状正则校验（WV-009）──
        if write_plan.partition_overwrite:
            dml = write_plan.partition_overwrite.overwrite_dml
            if not _INSERT_OVERWRITE_PARTITION_RE.match(dml.strip()):
                checks.append(
                    WriteValidationCheck(
                        check_id="WV-009",
                        check_type="dml_shape_mismatch",
                        passed=False,
                        detail=(
                            "分区 overwrite DML 形状不匹配——"
                            "必须为 INSERT OVERWRITE TABLE ... "
                            "PARTITION (...) SELECT ... FROM ..."
                        ),
                    )
                )
                forbidden.append("DML 形状不匹配 INSERT OVERWRITE")

        # ── 汇总 ──
        write_plan.validation_checks = checks
        write_plan.forbidden_operations = forbidden

        return write_plan

    @staticmethod
    def is_approved(write_plan: FinalWritePlan) -> bool:
        """判断写入方案是否通过全部校验。

        所有 validation_checks.passed 必须为 True，
        且 forbidden_operations 必须为空。
        """
        if write_plan.forbidden_operations:
            return False
        return all(c.passed for c in write_plan.validation_checks)
