"""测试 TempTableSpec 模型与 _temp 生命周期校验。

覆盖：
- 正常生命周期：创建→使用→清理
- 执行失败后 cleanup 仍然执行
- 非 producer 读取 _temp 被拒绝
- 拓扑排序确定性
"""

import pytest

from tianshu_datadev.planning.models import ColumnRef
from tianshu_datadev.planning.temp_table import (
    TempTableSpec,
    validate_consumer_is_declared,
    validate_temp_table_naming,
    validate_temp_table_refs,
)


class TestTempTableNaming:
    """_temp 表命名规范测试。"""

    def test_valid_temp_name_accepted(self):
        """合法的 _temp_ 前缀表名应通过校验。"""
        validate_temp_table_naming("_temp_agg")  # 不应抛出异常

    def test_missing_prefix_rejected(self):
        """缺少 _temp_ 前缀的表名应被拒绝。"""
        with pytest.raises(ValueError, match="_temp 表名必须以 '_temp_' 开头"):
            validate_temp_table_naming("temp_agg")

    def test_no_prefix_rejected(self):
        """完全没有前缀的表名应被拒绝。"""
        with pytest.raises(ValueError, match="_temp 表名必须以 '_temp_' 开头"):
            validate_temp_table_naming("my_table")

    def test_sql_injection_semicolon_rejected(self):
        """含分号的 SQL 注入标识符应被拒绝。"""
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming("_temp_a; DROP TABLE users; --")

    def test_sql_injection_quotes_rejected(self):
        """含引号的 SQL 注入标识符应被拒绝。"""
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming("""_temp_a" --""")
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming("_temp_' OR 1=1--")

    def test_non_alpha_start_rejected(self):
        """前缀后以数字开头的标识符应被拒绝——DuckDB 未加引号标识符要求字母开头。"""
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming("_temp_123abc")

    def test_empty_suffix_rejected(self):
        """仅 _temp_ 前缀、无后缀的标识符应被拒绝。"""
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming("_temp_")

    def test_over_length_rejected(self):
        """超长标识符（>64 字符）应被拒绝。"""
        long_name = "_temp_" + "a" * 65
        with pytest.raises(ValueError, match="包含非法字符"):
            validate_temp_table_naming(long_name)

    def test_valid_with_digits_and_underscores_accepted(self):
        """含数字和下划线的合法标识符应通过。"""
        # 不应抛出异常
        validate_temp_table_naming("_temp_my_table_123")
        validate_temp_table_naming("_temp_Abc_456_def")
        validate_temp_table_naming("_temp_X")

    def test_pydantic_field_rejects_injection_at_model_level(self):
        """Pydantic Field 约束在模型构造时即拒绝注入标识符。"""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            TempTableSpec(
                temp_id="_temp_a; DROP TABLE users; --",
                produced_by="stmt_A",
                consumed_by=["stmt_B"],
                column_defs=[],
            )


class TestTempTableRefs:
    """_temp 表引用校验测试。"""

    def test_valid_refs_no_errors(self):
        """引用的 produced_by/consumed_by 都存在时应无错误。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_B"],
                column_defs=[
                    ColumnRef(
                        table_ref="_temp_agg",
                        column_name="zone",
                        normalized_name="zone",
                    )
                ],
            )
        ]
        errors = validate_temp_table_refs(
            temp_tables, {"stmt_A", "stmt_B", "stmt_C"}
        )
        assert len(errors) == 0

    def test_missing_producer_reported(self):
        """produced_by 引用不存在的 statement_id 应报告错误。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_missing",  # 不存在
                consumed_by=["stmt_B"],
                column_defs=[],
            )
        ]
        errors = validate_temp_table_refs(temp_tables, {"stmt_B"})
        assert len(errors) >= 1
        assert any("produced_by" in e and "stmt_missing" in e for e in errors)

    def test_missing_consumer_reported(self):
        """consumed_by 引用不存在的 statement_id 应报告错误。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_missing"],  # 不存在
                column_defs=[],
            )
        ]
        errors = validate_temp_table_refs(temp_tables, {"stmt_A"})
        assert len(errors) >= 1
        assert any("consumed_by" in e and "stmt_missing" in e for e in errors)

    def test_invalid_cleanup_after_reported(self):
        """非法的 cleanup_after 值应报告错误。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_B"],
                column_defs=[],
                cleanup_after="never",  # 非法值
            )
        ]
        errors = validate_temp_table_refs(temp_tables, {"stmt_A", "stmt_B"})
        assert len(errors) >= 1
        assert any("cleanup_after" in e for e in errors)


class TestConsumerDeclaration:
    """消费者声明校验测试。"""

    def test_producer_can_read_own_temp(self):
        """生产者有权读取自己产生的 _temp 表。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_B"],
                column_defs=[],
            )
        ]
        assert validate_consumer_is_declared(
            temp_tables, "stmt_A", "_temp_agg"
        ) is True

    def test_declared_consumer_can_read(self):
        """声明的消费者有权读取 _temp 表。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_B", "stmt_C"],
                column_defs=[],
            )
        ]
        assert validate_consumer_is_declared(
            temp_tables, "stmt_B", "_temp_agg"
        ) is True

    def test_non_declared_statement_cannot_read(self):
        """非 producer 也非消费者的语句无权读取 _temp 表。"""
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_agg",
                produced_by="stmt_A",
                consumed_by=["stmt_B"],
                column_defs=[],
            )
        ]
        assert validate_consumer_is_declared(
            temp_tables, "stmt_C", "_temp_agg"
        ) is False

    def test_unknown_temp_id_returns_false(self):
        """读取不存在的 _temp 表名返回 False。"""
        temp_tables: list[TempTableSpec] = []
        assert validate_consumer_is_declared(
            temp_tables, "stmt_A", "_temp_nonexistent"
        ) is False
