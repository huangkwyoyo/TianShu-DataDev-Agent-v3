"""FinalWritePlan——受控最终表写入审查材料。

Phase 3C 核心产物——从 SqlProgram + 目标表信息生成日期分区 overwrite 方案。
仅作为审查材料输出，不实际执行生产写入。

约束：
- 仅允许日期分区 overwrite（PARTITION 模式）
- 禁止全表 overwrite、无分区 overwrite
- 禁止 UPDATE / DELETE / MERGE
- 禁止 INSERT INTO（非 overwrite）
- _temp 表仅允许 CREATE TABLE AS SELECT / INSERT INTO SELECT / DROP TABLE
"""

from __future__ import annotations

import hashlib
import re

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# 写入操作枚举
# ════════════════════════════════════════════


class TempTableOp(str):
    """_temp 表合法操作——仅 CREATE / INSERT / DROP。

    使用裸 str 类型以确保序列化兼容，但仅 3 个合法值。
    """


# _temp 表合法操作白名单
VALID_TEMP_OPS: frozenset[str] = frozenset({"CREATE", "INSERT", "DROP"})

# 生产表禁止操作——任何包含这些关键词的 DML 均被拒绝
FORBIDDEN_PRODUCTION_OPS: frozenset[str] = frozenset({
    "UPDATE", "DELETE", "MERGE", "TRUNCATE",
})


# ════════════════════════════════════════════
# 写入方案模型
# ════════════════════════════════════════════


class WriteValidationCheck(StrictModel):
    """单条写入方案校验结果——记录一次校验是否通过及原因。"""

    check_id: str  # 唯一标识（如 "WV-001"）
    check_type: str  # 校验类型（如 "partition_format" / "no_full_overwrite"）
    passed: bool  # True 表示通过校验
    detail: str  # 人类可读的校验详情


class TempTableStatement(StrictModel):
    """_temp 表操作语句——CREATE / INSERT / DROP 的具体 SQL 文本。

    仅作为审查材料展示，不直接执行。
    """

    temp_id: str  # _temp 表名
    operation: str  # "CREATE" | "INSERT" | "DROP"
    sql: str  # 渲染后的 SQL 文本（DuckDB 方言）
    order_index: int  # 在 _temp 操作序列中的序号（0-based）


class PartitionOverwriteSpec(StrictModel):
    """分区 overwrite 规格——描述最终日期分区写入的 DML 审查材料。"""

    target_table: str  # 目标表名（物理名）
    partition_keys: list[str]  # 分区键列表（如 ["dt"]）
    partition_values: dict[str, str]  # 分区键 → 值（如 {"dt": "20260101"}）
    partition_format: str  # 分区格式："yyyyMMdd" | "yyyyMM"
    source_temp_table: str  # 数据来源 _temp 表名
    overwrite_dml: str  # INSERT OVERWRITE ... PARTITION (...) 审查材料 SQL
    pre_check_sql: str  # 执行前检查 SQL（如分区是否存在）
    rollback_note: str  # 回滚注意事项（人类可读）


class FinalWritePlan(StrictModel):
    """受控最终表写入方案——Phase 3C 核心产物。

    从 SqlProgram + 目标表信息生成，包含：
    - _temp 表操作序列（CREATE / INSERT / DROP）
    - 最终日期分区 overwrite 方案
    - 写入校验结果
    - 风险评估和重跑策略

    此方案仅作为审查材料输出——不实际执行生产写入。
    人工审查通过后，由 DBA 或数据工程师手动执行分区 overwrite。
    """

    write_plan_id: str  # 确定性 ID
    program_id: str  # 对应 SqlProgram.program_id
    target_table: str  # 目标物理表名
    partition_keys: list[str] = []  # 分区键列表
    overwrite_mode: str = "partition"  # 固定为 "partition"——仅允许分区 overwrite
    partition_values: dict[str, str] = {}  # 分区键 → 值
    partition_format: str = ""  # "yyyyMMdd" | "yyyyMM"
    temp_table_ops: list[TempTableStatement] = []  # _temp 表操作序列
    partition_overwrite: PartitionOverwriteSpec | None = None  # 分区 overwrite 规格
    validation_checks: list[WriteValidationCheck] = []  # 所有校验结果
    forbidden_operations: list[str] = []  # 被拒绝的操作列表（空 = 无违规）
    review_material: str = ""  # 供人工审查的写入方案说明（Markdown）
    risk_notes: list[str] = []  # 风险注意事项
    rerun_strategy: str = ""  # 重跑策略说明

    @staticmethod
    def generate_write_plan_id(program_id: str) -> str:
        """基于 program_id 的确定性 write_plan ID。"""
        hash_hex = hashlib.sha256(
            f"write_plan:{program_id}".encode()
        ).hexdigest()[:12]
        return f"wp_{hash_hex}"


# ════════════════════════════════════════════
# 分区格式校验正则
# ════════════════════════════════════════════

# yyyyMMdd——8 位数字
_PARTITION_FORMAT_YYYYMMDD = re.compile(r"^\d{8}$")

# yyyyMM——6 位数字
_PARTITION_FORMAT_YYYYMM = re.compile(r"^\d{6}$")


def validate_partition_format(
    partition_values: dict[str, str],
    partition_format: str,
) -> list[WriteValidationCheck]:
    """校验分区值格式是否符合声明的分区格式。

    Args:
        partition_values: 分区键 → 值映射
        partition_format: "yyyyMMdd" 或 "yyyyMM"

    Returns:
        WriteValidationCheck 列表（passed=False 表示格式不匹配）
    """
    checks: list[WriteValidationCheck] = []

    if partition_format not in ("yyyyMMdd", "yyyyMM"):
        checks.append(
            WriteValidationCheck(
                check_id="WV-PART-FORMAT-001",
                check_type="partition_format",
                passed=False,
                detail=(
                    f"不支持的分区格式：'{partition_format}'——"
                    f"仅允许 'yyyyMMdd' 和 'yyyyMM'"
                ),
            )
        )
        return checks

    pattern = (
        _PARTITION_FORMAT_YYYYMMDD
        if partition_format == "yyyyMMdd"
        else _PARTITION_FORMAT_YYYYMM
    )

    for key, value in partition_values.items():
        if not pattern.match(value):
            help_msg = (
                "需要 8 位数字（如 20260101）"
                if partition_format == "yyyyMMdd"
                else "需要 6 位数字（如 202601）"
            )
            checks.append(
                WriteValidationCheck(
                    check_id=f"WV-PART-FORMAT-{len(checks)+2:03d}",
                    check_type="partition_format",
                    passed=False,
                    detail=(
                        f"分区键 '{key}' 的值 '{value}' 不符合格式 "
                        f"'{partition_format}'——{help_msg}"
                    ),
                )
            )
        else:
            checks.append(
                WriteValidationCheck(
                    check_id=f"WV-PART-FORMAT-{len(checks)+2:03d}",
                    check_type="partition_format",
                    passed=True,
                    detail=f"分区键 '{key}' 的值 '{value}' 符合格式 '{partition_format}'",
                )
            )

    return checks
