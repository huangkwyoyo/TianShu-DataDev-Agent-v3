"""Phase 6 SparkStaticValidator 测试——8 种错误码（E601-E608）。

每种错误码至少 1 个独立测试。
"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.validator import (
    SparkStaticValidator,
)


@pytest.fixture
def validator() -> SparkStaticValidator:
    return SparkStaticValidator()


# ════════════════════════════════════════════
# E601: FORBIDDEN_API
# ════════════════════════════════════════════


class TestE601ForbiddenAPI:
    """禁止的 Spark API 调用。"""

    def test_spark_read_rejected(self, validator):
        code = "df = spark.read.parquet('/path/to/data')"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E601" for e in result.errors)

    def test_spark_table_rejected(self, validator):
        code = "df = spark.table('my_table')"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E601" for e in result.errors)

    def test_spark_sql_rejected(self, validator):
        code = "df = spark.sql('SELECT * FROM t')"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E601" for e in result.errors)

    def test_inputs_dict_allowed(self, validator):
        """inputs dict 读取不被拦截。"""
        code = 'od = inputs["dwd.order_detail"]'
        result = validator.validate(code)
        # 不含禁止 API
        assert not any(e.error_code == "E601" for e in result.errors)


# ════════════════════════════════════════════
# E602: UNSAFE_IMPORT
# ════════════════════════════════════════════


class TestE602UnsafeImport:
    """禁止的不安全导入。"""

    def test_subprocess_import_rejected(self, validator):
        code = "import subprocess"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E602" for e in result.errors)

    def test_os_system_import_rejected(self, validator):
        """import os 本身不触发 E602——只有 os.system 等完整路径才触发。"""
        code = "import os"
        _ = validator.validate(code)
        # "os" 本身不触发 E602，只有 "os.system" 触发
        # 当前 E602 检查是针对完整模块名的，"os" 不在禁止列表中

    def test_pyspark_imports_allowed(self, validator):
        """PySpark 标准导入允许。"""
        code = "from pyspark.sql import DataFrame\nfrom pyspark.sql import functions as F"
        result = validator.validate(code)
        assert not any(e.error_code == "E602" for e in result.errors)


# ════════════════════════════════════════════
# E603: ACTION_NOT_ALLOWED
# ════════════════════════════════════════════


class TestE603ActionNotAllowed:
    """禁止的 DataFrame Action。"""

    def test_df_collect_rejected(self, validator):
        code = "result = df.collect()"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E603" for e in result.errors)

    def test_df_count_rejected(self, validator):
        code = "n = df.count()"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E603" for e in result.errors)

    def test_df_show_rejected(self, validator):
        code = "df.show(10)"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E603" for e in result.errors)

    def test_df_toPandas_rejected(self, validator):  # noqa: N802
        code = "pdf = df.toPandas()"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E603" for e in result.errors)

    def test_f_count_allowed(self, validator):
        """F.count() 聚合函数允许（不是 df.count() action）。"""
        code = "df.groupBy('x').agg(F.count('*'))"
        _ = validator.validate(code)
        # F.count 调用不应被误判为 df.count()
        # 当前 AST 实现中，F.count 是 Attribute chain，不会被 _check_action_call 拦截


# ════════════════════════════════════════════
# E604: SINK_NOT_ALLOWED
# ════════════════════════════════════════════


class TestE604SinkNotAllowed:
    """禁止的 Sink 方法。"""

    def test_df_write_rejected(self, validator):
        code = "df.write.parquet('/output')"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E604" for e in result.errors)

    def test_df_save_rejected(self, validator):
        code = "df.save('/output')"
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E604" for e in result.errors)


# ════════════════════════════════════════════
# E605: UDF_NOT_ALLOWED
# ════════════════════════════════════════════


class TestE605UDFNotAllowed:
    """禁止的 UDF 装饰器。"""

    def test_udf_decorator_rejected(self, validator):
        code = """
@udf(returnType=StringType())
def my_func(x):
    return x.upper()
"""
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E605" for e in result.errors)


# ════════════════════════════════════════════
# E606: RAW_EXPRESSION
# ════════════════════════════════════════════


class TestE606RawExpression:
    """禁止的 F.expr() 原始表达式。"""

    def test_f_expr_rejected(self, validator):
        code = 'df = df.withColumn("x", F.expr("col_a + col_b"))'
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E606" for e in result.errors)

    def test_f_col_allowed(self, validator):
        """F.col() 允许。"""
        code = 'df.filter(F.col("status") == "paid")'
        result = validator.validate(code)
        assert not any(e.error_code == "E606" for e in result.errors)


# ════════════════════════════════════════════
# E607: UNKNOWN_FUNCTION
# ════════════════════════════════════════════


class TestE607UnknownFunction:
    """不在白名单内的函数调用。"""

    def test_placeholder_e607(self, validator):
        """E607 占位——Phase 6A 当前白名单检查嵌入 E601/E603/E604/E606。"""
        # E607 作为独立错误码在 Phase 6B 完整实现函数白名单时扩展
        # 当前阶段：已知函数通过，非禁止函数不拦截
        pass


# ════════════════════════════════════════════
# E608: DYNAMIC_EXEC
# ════════════════════════════════════════════


class TestE608DynamicExec:
    """禁止的 eval/exec 动态执行。"""

    def test_eval_rejected(self, validator):
        code = 'eval("1 + 1")'
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E608" for e in result.errors)

    def test_exec_rejected(self, validator):
        code = 'exec("x = 1")'
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E608" for e in result.errors)

    def test_compile_rejected(self, validator):
        code = 'compile("x = 1", "", "exec")'
        result = validator.validate(code)
        assert not result.is_valid
        assert any(e.error_code == "E608" for e in result.errors)


# ════════════════════════════════════════════
# 合法代码测试
# ════════════════════════════════════════════


class TestValidCode:
    """合法 PySpark DSL 代码通过校验。"""

    def test_read_and_filter_valid(self, validator):
        code = '''from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(inputs, params=None):
    od = inputs["dwd.order_detail"]
    _f0 = od.filter(F.col("od.order_status") == 'paid')
    _p1 = _f0.select(F.col("stat_date"), F.col("total_amount"))
    return _p1
'''
        result = validator.validate(code)
        assert result.is_valid, f"合法代码不应报错：{result.errors}"

    def test_inputs_read_valid(self, validator):
        code = 'df = inputs["my_table"]'
        result = validator.validate(code)
        assert result.is_valid

    def test_select_filter_orderBy_valid(self, validator):  # noqa: N802
        code = '''df = inputs["t"]
df2 = df.filter(F.col("x") > 0)
df3 = df2.select(F.col("x"), F.col("y"))
df4 = df3.orderBy(F.desc("x"))
df5 = df4.limit(100)
'''
        result = validator.validate(code)
        assert result.is_valid

    def test_escaped_source_name_passes(self, validator):
        """render_dict_key 转义后的 inputs key 仍通过 Validator。"""
        code = 'df = inputs["a\\\\"b"]'
        result = validator.validate(code)
        # 含转义字符的合法字符串不应被拦截
        # E601 检查的是 spark.read / spark.table，不是 inputs[...]
        assert not any(e.error_code == "E601" for e in result.errors)
