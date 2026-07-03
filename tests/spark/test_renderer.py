"""Phase 6 SparkCodeRenderer 安全测试——含恶意输入拒绝。"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkJoinType,
    SparkSortDirection,
    SparkWindowFunction,
)
from tianshu_datadev.spark.renderer import RenderError, SparkCodeRenderer


class TestValidateIdentifier:
    """标识符安全校验测试。"""

    def test_valid_identifier(self):
        """合法标识符通过校验。"""
        assert SparkCodeRenderer.validate_identifier("od") == "od"
        assert SparkCodeRenderer.validate_identifier("order_detail") == "order_detail"
        assert SparkCodeRenderer.validate_identifier("_temp") == "_temp"
        assert SparkCodeRenderer.validate_identifier("df1") == "df1"

    def test_invalid_identifier_raises(self):
        """非法标识符抛出 RenderError。"""
        with pytest.raises(RenderError):
            SparkCodeRenderer.validate_identifier("1df")  # 数字开头
        with pytest.raises(RenderError):
            SparkCodeRenderer.validate_identifier("od; DROP TABLE")  # 含空格
        with pytest.raises(RenderError):
            SparkCodeRenderer.validate_identifier("od--")  # 含特殊字符
        with pytest.raises(RenderError):
            SparkCodeRenderer.validate_identifier("")  # 空字符串

    def test_sql_injection_rejected(self):
        """SQL 注入字符串被拒绝。"""
        malicious = [
            "od; DROP TABLE users",
            "od' OR '1'='1",
            "od--",
            "od/**/",
            "od\nSELECT",
        ]
        for m in malicious:
            with pytest.raises(RenderError, match="非法标识符"):
                SparkCodeRenderer.validate_identifier(m)


class TestRenderColumn:
    """列名渲染测试。"""

    def test_render_simple_column(self):
        assert SparkCodeRenderer.render_column("user_id") == 'F.col("user_id")'

    def test_render_column_with_underscore(self):
        assert SparkCodeRenderer.render_column("order_amount") == 'F.col("order_amount")'

    def test_render_column_rejects_injection(self):
        with pytest.raises(RenderError):
            SparkCodeRenderer.render_column('user_id"); inject(')


class TestRenderLiteral:
    """字面量渲染测试。"""

    def test_render_string(self):
        assert SparkCodeRenderer.render_literal("paid") == "'paid'"

    def test_render_string_with_quote(self):
        assert SparkCodeRenderer.render_literal("it's") == "'it\\'s'"

    def test_render_int(self):
        assert SparkCodeRenderer.render_literal(42) == "42"

    def test_render_float(self):
        assert SparkCodeRenderer.render_literal(3.14) == "3.14"

    def test_render_bool(self):
        assert SparkCodeRenderer.render_literal(True) == "True"
        assert SparkCodeRenderer.render_literal(False) == "False"


class TestRenderAggFunction:
    """聚合函数名渲染测试。"""

    def test_count(self):
        assert SparkCodeRenderer.render_agg_function(SparkAggFunction.COUNT) == "F.count"

    def test_count_distinct(self):
        assert SparkCodeRenderer.render_agg_function(SparkAggFunction.COUNT_DISTINCT) == "F.countDistinct"

    def test_sum(self):
        assert SparkCodeRenderer.render_agg_function(SparkAggFunction.SUM) == "F.sum"

    def test_all_functions_renderable(self):
        """所有聚合函数枚举值均可渲染。"""
        for func in SparkAggFunction:
            result = SparkCodeRenderer.render_agg_function(func)
            assert result.startswith("F.")


class TestRenderWindowFunction:
    """窗口函数名渲染测试。"""

    def test_row_number(self):
        assert SparkCodeRenderer.render_window_function(SparkWindowFunction.ROW_NUMBER) == "F.row_number"

    def test_all_window_functions(self):
        """所有窗口函数枚举值均可渲染。"""
        for func in SparkWindowFunction:
            result = SparkCodeRenderer.render_window_function(func)
            assert result.startswith("F.")


class TestRenderJoinType:
    """Join 类型渲染测试。"""

    def test_inner(self):
        assert SparkCodeRenderer.render_join_type(SparkJoinType.INNER) == '"inner"'

    def test_left(self):
        assert SparkCodeRenderer.render_join_type(SparkJoinType.LEFT) == '"left"'


class TestRenderSortDirection:
    """排序方向渲染测试。"""

    def test_asc(self):
        assert SparkCodeRenderer.render_sort_direction(SparkSortDirection.ASC) == "F.asc"

    def test_desc(self):
        assert SparkCodeRenderer.render_sort_direction(SparkSortDirection.DESC) == "F.desc"


class TestRenderOperator:
    """操作符渲染测试。"""

    def test_comparison_operators(self):
        assert SparkCodeRenderer.render_operator("GT") == ">"
        assert SparkCodeRenderer.render_operator("GTE") == ">="
        assert SparkCodeRenderer.render_operator("LT") == "<"
        assert SparkCodeRenderer.render_operator("LTE") == "<="
        assert SparkCodeRenderer.render_operator("EQ") == "=="
        assert SparkCodeRenderer.render_operator("NEQ") == "!="

    def test_logical_operators(self):
        assert SparkCodeRenderer.render_operator("AND") == "&"
        assert SparkCodeRenderer.render_operator("OR") == "|"
        assert SparkCodeRenderer.render_operator("NOT") == "~"

    def test_case_insensitive(self):
        assert SparkCodeRenderer.render_operator("gt") == ">"
        assert SparkCodeRenderer.render_operator("eq") == "=="

    def test_invalid_operator_raises(self):
        with pytest.raises(RenderError):
            SparkCodeRenderer.render_operator("INVALID_OP")

    def test_unary_operator_detection(self):
        assert SparkCodeRenderer.is_unary_operator("IS_NULL") is True
        assert SparkCodeRenderer.is_unary_operator("IS_NOT_NULL") is True
        assert SparkCodeRenderer.is_unary_operator("EQ") is False


class TestRenderComment:
    """注释行渲染测试。"""

    def test_simple_comment(self):
        result = SparkCodeRenderer.render_comment_line("Step: test")
        assert result == "# Step: test"

    def test_comment_cleans_newlines(self):
        """注释注入换行被清洗。"""
        result = SparkCodeRenderer.render_comment_line("Step:\nDROP TABLE")
        assert "\n" not in result
        assert "DROP TABLE" in result

    def test_comment_cleans_sql_injection(self):
        """SQL 注释注入被清洗。"""
        result = SparkCodeRenderer.render_comment_line("test -- DROP TABLE")
        assert "--" not in result
        assert "——" in result


class TestRenderImports:
    """导入块渲染测试。"""

    def test_imports_contain_required(self):
        result = SparkCodeRenderer.render_imports()
        assert "from pyspark.sql import DataFrame" in result
        assert "from pyspark.sql import functions as F" in result


class TestRenderFunctionSignature:
    """函数签名渲染测试。"""

    def test_signature_format(self):
        result = SparkCodeRenderer.render_function_signature()
        assert "def transform(" in result
        assert "inputs: Mapping[str, DataFrame]" in result
        assert "-> DataFrame:" in result
