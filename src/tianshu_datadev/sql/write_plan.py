"""FinalWritePlan——受控最终表写入审查材料。

Phase 3C 核心产物——从 SqlProgram + 目标表信息生成日期分区 overwrite 方案。
仅作为审查材料输出，不实际执行生产写入。

约束：
- 仅允许日期分区 overwrite（PARTITION 模式）
- 禁止全表 overwrite、无分区 overwrite
- 禁止 UPDATE / DELETE / MERGE
- 禁止 INSERT INTO（非 overwrite）
- _temp 表仅允许 CREATE TABLE AS SELECT / INSERT INTO SELECT / DROP TABLE

Phase 3C 安全加固：
- PartitionOverwriteSpec.overwrite_dml / pre_check_sql / rollback_note 均为 @computed_field，
  由结构化字段确定性渲染——不接受外部构造，杜绝 SQL 文本注入
- 所有 target_table / source_temp_table / temp_id 使用 SafePhysicalTableName 约束
- TempTableStatement.sql 受 @model_validator 校验——拒绝分号、禁用操作、operation 不一致
- PartitionOverwriteSpec 内部渲染全部经过 _render_sql_string_literal() 转义
"""

from __future__ import annotations

import hashlib
import re

from pydantic import computed_field, model_validator

from tianshu_datadev.developer_spec.models import (
    SafePhysicalTableName,
    StrictModel,
    _render_sql_string_literal,
)
from tianshu_datadev.planning.models import _SQL_ID_RE

# ════════════════════════════════════════════
# 写入操作枚举
# ════════════════════════════════════════════

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

    Phase 3C 安全加固：temp_id 受 SafePhysicalTableName 约束，
    sql 字段受 @model_validator 校验——拒绝分号、禁用操作、operation 不一致。
    """

    temp_id: SafePhysicalTableName
    operation: str  # "CREATE" | "INSERT" | "DROP"
    sql: str  # 渲染后的 SQL 文本（DuckDB 方言）
    order_index: int  # 在 _temp 操作序列中的序号（0-based）

    @model_validator(mode="after")
    def _validate_sql_safety(self) -> "TempTableStatement":
        """校验 sql 文本的安全性——拒绝多语句、禁用操作、关键词不匹配。

        此校验在 Pydantic 构造后自动执行——任何试图构造含恶意 SQL 的
        TempTableStatement 都会收到 ValidationError。
        """
        sql_upper = self.sql.strip().upper()

        # 1. 完全拒绝分号——审查材料不需要多语句
        if ";" in self.sql:
            raise ValueError(
                f"_temp 表 '{self.temp_id}' 的 SQL 文本包含分号——"
                f"审查材料禁止多语句"
            )

        # 2. 扫描禁止操作词（UPDATE / DELETE / MERGE / TRUNCATE）
        for forbidden_op in FORBIDDEN_PRODUCTION_OPS:
            # 用词边界检查——避免匹配到列名中的子串
            # 简单策略：在 SQL 上界中搜索独立出现的操作词
            if _word_in_sql(forbidden_op, sql_upper):
                raise ValueError(
                    f"_temp 表 '{self.temp_id}' 的 SQL 文本包含禁止操作 "
                    f"'{forbidden_op}'"
                )

        # 3. SQL 关键词必须与 operation 标签一致
        op_keyword_map = {
            "CREATE": "CREATE TABLE",
            "INSERT": "INSERT INTO",
            "DROP": "DROP TABLE",
        }
        expected_prefix = op_keyword_map.get(self.operation)
        if expected_prefix and not sql_upper.startswith(expected_prefix):
            raise ValueError(
                f"_temp 表 '{self.temp_id}' 的 operation="
                f"'{self.operation}' 但 SQL 文本不以 "
                f"'{expected_prefix}' 开头"
            )

        return self


def _word_in_sql(word: str, sql_upper: str) -> bool:
    """检查禁止操作词是否以完整 SQL 标识符形式出现在 SQL 文本中。

    避免将列名中的子串（如 update_count 中的 UPDATE）
    误判为禁止操作。

    Args:
        word: 禁止操作词（大写）
        sql_upper: SQL 文本（大写）

    Returns:
        True 表示 SQL 文本中包含该禁止操作词
    """
    # 在 SQL 文本中搜索，确保前后是词边界（非字母数字下划线）
    # 使用正则确保禁止词是一个完整的 SQL 关键字
    pattern = re.compile(rf"\b{re.escape(word)}\b")
    return bool(pattern.search(sql_upper))


class PartitionOverwriteSpec(StrictModel):
    """分区 overwrite 规格——SQL 文本均为计算属性，不接受外部构造。

    Phase 3C 安全加固核心设计：
    - overwrite_dml / pre_check_sql / rollback_note 均为 @computed_field，
      由 target_table + partition_keys + partition_values + source_temp_table
      确定性渲染——任何人无法通过构造函数注入恶意 SQL 文本
    - 所有表名字段使用 SafePhysicalTableName——构造时拒绝分号/引号等非法字符
    - 分区值渲染全部经过 _render_sql_string_literal() 转义
    """

    target_table: SafePhysicalTableName  # 目标物理表名
    partition_keys: list[str]  # 分区键列表（如 ["dt"]）——@model_validator 校验元素
    partition_values: dict[str, str]  # 分区键 → 值——key 受 @model_validator 校验
    partition_format: str  # 分区格式："yyyyMMdd" | "yyyyMM"
    source_temp_table: SafePhysicalTableName  # 数据来源 _temp 表名

    @model_validator(mode="after")
    def _validate_partition_keys_safe(self) -> "PartitionOverwriteSpec":
        """校验分区键和分区值的 key 均为合法 SQL 标识符。

        分区键在渲染时作为 SQL 标识符原样嵌入 PARTITION (key=...) 子句——
        若不校验，攻击者可通过 partition_values 的 key 注入 SQL 片段：

            partition_values={
                "dt) SELECT * FROM t; DROP TABLE prod; --": "20260101"
            }
            → PARTITION (dt) SELECT * FROM t; DROP TABLE prod; --='20260101')

        此校验在 Schema 层拒绝所有非 SQL 标识符的键名。
        """
        # 1. 校验 partition_keys 每个元素为合法 SQL 标识符
        for key in self.partition_keys:
            if not _SQL_ID_RE.match(key):
                raise ValueError(
                    f"分区键 '{key}' 不是合法 SQL 标识符——"
                    f"必须匹配 {_SQL_ID_RE.pattern}"
                    f"（字母/下划线开头，仅含字母数字下划线）"
                )

        # 2. 校验 partition_values 每个 key 为合法 SQL 标识符
        for key in self.partition_values:
            if not _SQL_ID_RE.match(key):
                raise ValueError(
                    f"分区值键 '{key}' 不是合法 SQL 标识符——"
                    f"必须匹配 {_SQL_ID_RE.pattern}"
                    f"（字母/下划线开头，仅含字母数字下划线）"
                )

        # 3. 强制 partition_values 的 key 集合与 partition_keys 一致
        values_keys = set(self.partition_values.keys())
        declared_keys = set(self.partition_keys)
        if values_keys != declared_keys:
            missing = declared_keys - values_keys
            extra = values_keys - declared_keys
            msg_parts = []
            if missing:
                msg_parts.append(f"partition_values 缺少键：{sorted(missing)}")
            if extra:
                msg_parts.append(f"partition_values 含未声明的键：{sorted(extra)}")
            raise ValueError(
                f"分区键声明与分区值不一致——{'; '.join(msg_parts)}"
            )

        return self

    @computed_field
    @property
    def overwrite_dml(self) -> str:
        """从结构化字段确定性渲染 INSERT OVERWRITE DML。

        分区值通过 _render_sql_string_literal() 转义——
        即使分区值含单引号，也不会终结 SQL 字符串字面量。
        表名由 SafePhysicalTableName 保证不含分号/引号等非法字符。
        """
        partition_clause = ", ".join(
            f"{k}={_render_sql_string_literal(v)}"
            for k, v in self.partition_values.items()
        )
        return (
            f"INSERT OVERWRITE TABLE {self.target_table}\n"
            f"  PARTITION ({partition_clause})\n"
            f"SELECT *\n"
            f"FROM {self.source_temp_table}"
        )

    @computed_field
    @property
    def pre_check_sql(self) -> str:
        """从结构化字段确定性渲染执行前检查 SQL。

        用于在执行 overwrite 前检查目标分区当前数据量。
        """
        where_conditions = " AND ".join(
            f"{k} = {_render_sql_string_literal(v)}"
            for k, v in self.partition_values.items()
        )
        return (
            f"-- 执行前检查：确认目标分区数据量\n"
            f"SELECT COUNT(*) AS row_count\n"
            f"FROM {self.target_table}\n"
            f"WHERE {where_conditions}"
        )

    @computed_field
    @property
    def rollback_note(self) -> str:
        """从结构化字段确定性渲染回滚说明。

        包含备份 SQL 参考——仅供 DBA 审查，不自动执行。
        """
        where_conditions = " AND ".join(
            f"{k} = {_render_sql_string_literal(v)}"
            for k, v in self.partition_values.items()
        )
        # 用第一个分区值作为备份表后缀
        backup_suffix = (
            list(self.partition_values.values())[0]
            if self.partition_values else "backup"
        )
        return (
            f"如需回滚：\n"
            f"1. 确认上游数据源可重放（{self.source_temp_table} 的源数据未变更）\n"
            f"2. 备份当前分区数据：\n"
            f"   CREATE TABLE {self.target_table}_{backup_suffix}_bak "
            f"AS SELECT * FROM {self.target_table} "
            f"WHERE {where_conditions};\n"
            f"3. 重跑 SqlProgram 并重新执行 INSERT OVERWRITE"
        )


class FinalWritePlan(StrictModel):
    """受控最终表写入方案——Phase 3C 核心产物。

    从 SqlProgram + 目标表信息生成，包含：
    - _temp 表操作序列（CREATE / INSERT / DROP）
    - 最终日期分区 overwrite 方案
    - 写入校验结果
    - 风险评估和重跑策略

    此方案仅作为审查材料输出——不实际执行生产写入。
    人工审查通过后，由 DBA 或数据工程师手动执行分区 overwrite。

    Phase 3C 安全加固：target_table 使用 SafePhysicalTableName——
    物理表名注入在 Schema 层即被拒绝。
    """

    write_plan_id: str  # 确定性 ID
    program_id: str  # 对应 SqlProgram.program_id
    target_table: SafePhysicalTableName  # 目标物理表名——Schema 层拒绝注入
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
