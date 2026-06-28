"""TempTableSpec——_temp 中间表规格与生命周期校验。

_temp 表是 SqlProgram 中语句间传递中间结果的临时表，生命周期为：
    CREATE（生产者执行时创建）→ READ（消费者读取）→ DROP（程序结束时清理，无论成功或失败）

约束：
- _temp 表命名必须使用 _temp_ 前缀
- _temp 标识符仅允许字母数字下划线，字母开头（防止 SQL 注入）
- _temp 标识符长度 ≤ 64 字符
- cleanup_after 当前仅支持 "program_end"
- _temp 表不得跨越 SqlProgram 边界
"""

from __future__ import annotations

import re

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

from .models import ColumnRef

# _temp 表名前缀常量——所有临时表必须以该前缀开头
_TEMP_PREFIX = "_temp_"

# _temp 标识符 allowlist 正则——仅允许字母开头 + 字母数字下划线，防止 SQL 注入
# DuckDB 未加引号的标识符必须以字母或 _ 开头，这里进一步限制为字母开头
_TEMP_ID_PATTERN = re.compile(r"^_temp_[A-Za-z][A-Za-z0-9_]{0,58}$")
#                                                      ^^^^^ 前缀5字符 + 最多59字符 = 总计 ≤ 64

# 当前支持的清理时机
VALID_CLEANUP_AFTER = frozenset({"program_end"})


class TempTableSpec(StrictModel):
    """_temp 中间表规格——描述一个临时表的产生、消费和清理策略。

    生命周期：
    - produced_by 生产者语句执行时 CREATE
    - consumed_by 消费者语句 READ
    - cleanup_after 指定清理时机（当前仅 program_end）
    """

    temp_id: str = Field(
        pattern=r"^_temp_[A-Za-z][A-Za-z0-9_]{0,58}$",
        max_length=64,
        description="_temp 表名：_temp_ 前缀 + 字母开头 + 字母数字下划线，最多 64 字符",
    )
    produced_by: str  # 生产者 statement_id
    consumed_by: list[str]  # 消费者 statement_id 列表
    column_defs: list[ColumnRef]  # 中间表列定义（避免与 BaseModel.schema 冲突）
    cleanup_after: str = "program_end"  # 清理时机（当前仅支持 program_end）


def validate_temp_table_naming(temp_id: str) -> None:
    """校验 _temp 表名的合法性——前缀 + 字符 allowlist + 长度上限。

    校验规则：
    1. 必须以 _temp_ 前缀开头
    2. 仅允许字母数字下划线，且前缀后第一个字符必须为字母
    3. 总长度不得超过 64 字符

    这是 _temp 标识符进入 SQL 渲染链路的唯一安全门禁。

    Args:
        temp_id: 待校验的临时表名

    Raises:
        ValueError: 表名不符合 allowlist 规范
    """
    if not temp_id.startswith(_TEMP_PREFIX):
        raise ValueError(
            f"_temp 表名必须以 '{_TEMP_PREFIX}' 开头，"
            f"实际值：'{temp_id}'"
        )

    if not _TEMP_ID_PATTERN.match(temp_id):
        raise ValueError(
            f"_temp 表名包含非法字符或格式不正确：'{temp_id}'。"
            f"仅允许 _temp_ 前缀 + 字母开头 + 字母数字下划线，"
            f"总长度不超过 64 字符"
        )


def validate_temp_table_refs(
    temp_tables: list[TempTableSpec],
    statement_ids: set[str],
) -> list[str]:
    """校验 _temp 表的 produced_by / consumed_by 引用有效 statement_id。

    Args:
        temp_tables: _temp 表声明列表
        statement_ids: SqlProgram 中所有有效的 statement_id 集合

    Returns:
        错误信息列表（空列表表示全部合法）
    """
    errors: list[str] = []

    for tt in temp_tables:
        # 检查 producer 引用
        if tt.produced_by not in statement_ids:
            errors.append(
                f"TempTable '{tt.temp_id}' 的 produced_by '{tt.produced_by}' "
                f"引用了不存在的 statement_id"
            )

        # 检查 consumer 引用
        for consumer_id in tt.consumed_by:
            if consumer_id not in statement_ids:
                errors.append(
                    f"TempTable '{tt.temp_id}' 的 consumed_by '{consumer_id}' "
                    f"引用了不存在的 statement_id"
                )

        # 检查 cleanup_after 合法值
        if tt.cleanup_after not in VALID_CLEANUP_AFTER:
            errors.append(
                f"TempTable '{tt.temp_id}' 的 cleanup_after 值 '{tt.cleanup_after}' "
                f"无效——仅支持 {sorted(VALID_CLEANUP_AFTER)}"
            )

        # 检查命名规范
        try:
            validate_temp_table_naming(tt.temp_id)
        except ValueError as e:
            errors.append(str(e))

    return errors


def validate_consumer_is_declared(
    temp_tables: list[TempTableSpec],
    statement_id: str,
    temp_id: str,
) -> bool:
    """检查某个 statement 是否有权读取指定的 _temp 表。

    只有在该 _temp 表的 consumed_by 列表中声明的消费者才有权读取。
    生产者（produced_by）本身也有权读取自己产生的表。

    Args:
        temp_tables: _temp 表声明列表
        statement_id: 尝试读取 _temp 表的语句 ID
        temp_id: 被读取的 _temp 表名

    Returns:
        True 如果该 statement 有权读取
    """
    for tt in temp_tables:
        if tt.temp_id == temp_id:
            # 生产者有权读取自己产生的表
            if tt.produced_by == statement_id:
                return True
            # 声明的消费者有权读取
            if statement_id in tt.consumed_by:
                return True
            return False

    # 未找到匹配的 temp_id——不是声明的 _temp 表
    return False
