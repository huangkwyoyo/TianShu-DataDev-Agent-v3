"""Phase 6 SparkCodeRenderer 安全测试——含恶意输入拒绝。"""

from __future__ import annotations

import pytest

from tianshu_datadev.artifacts.models import CaseWhenCondition
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


class TestRenderCommentText:
    """注释文本清洗——不添加 # 前缀，净化控制字符和注入。"""

    def test_normal_text(self):
        """正常文本原样返回。"""
        result = SparkCodeRenderer.render_comment_text("数据读取")
        assert result == "数据读取"

    def test_newline_replaced_with_space(self):
        """换行替换为空格——防止注释逃逸为裸代码行。"""
        result = SparkCodeRenderer.render_comment_text("hello\nimport os\n# bad")
        assert "\n" not in result
        assert "import os" in result  # 内容保留但换行变空格

    def test_carriage_return_replaced(self):
        """回车替换为空格。"""
        result = SparkCodeRenderer.render_comment_text("a\rb")
        assert "\r" not in result

    def test_tab_replaced_with_space(self):
        """制表符替换为空格。"""
        result = SparkCodeRenderer.render_comment_text("a\tb")
        assert "\t" not in result

    def test_null_byte_removed(self):
        """空字节被丢弃。"""
        result = SparkCodeRenderer.render_comment_text("a\x00b")
        assert "\x00" not in result
        assert "ab" in result

    def test_escape_sequence_removed(self):
        """ESC 控制字符被丢弃。"""
        result = SparkCodeRenderer.render_comment_text("a\x1bb")
        assert "\x1b" not in result

    def test_del_removed(self):
        """DEL (0x7F) 被丢弃。"""
        result = SparkCodeRenderer.render_comment_text("a\x7fb")
        assert "\x7f" not in result

    def test_sql_comment_injection_prevented(self):
        """SQL 注释 -- 被替换为 ——。"""
        result = SparkCodeRenderer.render_comment_text("test -- DROP TABLE")
        assert "--" not in result
        assert "——" in result

    def test_multiline_injection_produces_single_line(self):
        """多行注入产出单行——不会在注释块中产生裸代码行。"""
        result = SparkCodeRenderer.render_comment_text(
            '从 inputs["dwd.\nimport os\n# order_detail"] 读取数据'
        )
        assert "\n" not in result
        # 验证 "import os" 不再作为独立行存在
        lines = result.split("\n")
        assert len(lines) == 1


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


# ════════════════════════════════════════════
# 安全补丁：render_dict_key / render_filter_right
# ════════════════════════════════════════════


class TestRenderDictKey:
    """字典键安全渲染——转义双引号、反斜杠、控制字符。"""

    def test_normal_key(self):
        """普通键原样返回，用双引号包围。"""
        result = SparkCodeRenderer.render_dict_key("dwd.order_detail")
        assert result == '"dwd.order_detail"'

    def test_key_with_double_quotes(self):
        """含双引号的键——双引号被转义。"""
        result = SparkCodeRenderer.render_dict_key('a"b')
        assert result == r'"a\"b"'

    def test_key_with_newline_rejected(self):
        """含换行的键——换行被转义为 \\n 字面量。"""
        result = SparkCodeRenderer.render_dict_key("a\nb")
        assert "\\n" in result
        assert "\n" not in result

    def test_key_with_backslash(self):
        """含反斜杠的键——反斜杠被转义。"""
        result = SparkCodeRenderer.render_dict_key("a\\b")
        assert result == r'"a\\b"'

    def test_key_with_carriage_return(self):
        """含回车的键——被转义。"""
        result = SparkCodeRenderer.render_dict_key("a\rb")
        assert "\\r" in result
        assert "\r" not in result

    def test_key_with_null_byte(self):
        """含 NUL (0x00) 的键——转义为 \\x00。"""
        result = SparkCodeRenderer.render_dict_key("a\x00b")
        assert "\\x00" in result
        assert "\x00" not in result

    def test_key_with_escape_char(self):
        """含 ESC (0x1B) 的键——转义为 \\x1b。"""
        result = SparkCodeRenderer.render_dict_key("a\x1bb")
        assert "\\x1b" in result
        assert "\x1b" not in result

    def test_key_with_del_char(self):
        """含 DEL (0x7F) 的键——转义为 \\x7f。"""
        result = SparkCodeRenderer.render_dict_key("a\x7fb")
        assert "\\x7f" in result

    def test_key_with_vt_char(self):
        """含垂直制表符 (0x0B) 的键——转义为 \\x0b。"""
        result = SparkCodeRenderer.render_dict_key("a\x0bb")
        assert "\\x0b" in result

    def test_key_with_form_feed(self):
        """含换页符 (0x0C) 的键——转义为 \\x0c。"""
        result = SparkCodeRenderer.render_dict_key("a\x0cb")
        assert "\\x0c" in result

    def test_all_control_chars_escaped(self):
        """所有 ASCII 控制字符 (0x00-0x1F, 0x7F) 均被转义——输出不含原始控制字符。"""
        # 构造含全部控制字符的键
        controls = "".join(chr(i) for i in list(range(0x00, 0x20)) + [0x7F])
        key = f"x{controls}y"
        result = SparkCodeRenderer.render_dict_key(key)
        # 输出中不应含任何原始控制字符（引号包围的字符串内全是可打印字符）
        inner = result[1:-1]  # 去掉外双引号
        for ch in inner:
            cp = ord(ch)
            assert cp >= 0x20 and cp != 0x7F, (
                f"控制字符 U+{cp:04X} 未被转义：{repr(result)}"
            )
        # 验证有效控制字符被转义
        assert "\\n" in result or "\\x0a" in result  # 换行被转义
        assert "\\x00" in result  # NUL 被转义
        assert "\\x7f" in result  # DEL 被转义


class TestRenderFilterRight:
    """过滤右值安全渲染——列引用委托 render_column，字面量安全校验。"""

    def test_column_ref_rendered_as_fcol(self):
        """含点号且无引号——识别为列引用。"""
        result = SparkCodeRenderer.render_filter_right("od.status")
        assert result == 'F.col("od.status")'

    def test_safe_literal_passthrough(self):
        """安全的预格式化字面量——校验通过后原样返回。"""
        result = SparkCodeRenderer.render_filter_right("'paid'")
        assert result == "'paid'"

    def test_numeric_literal_passthrough(self):
        """数值字面量——校验通过后原样返回。"""
        result = SparkCodeRenderer.render_filter_right("100")
        assert result == "100"

    def test_malicious_exec_rejected(self):
        """含 exec() 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("exec('rm -rf /')")

    def test_malicious_eval_rejected(self):
        """含 eval() 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("eval('__import__(\"os\")')")

    def test_malicious_spark_read_rejected(self):
        """含 spark.read 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("spark.read.parquet('/etc/passwd')")

    def test_malicious_spark_table_rejected(self):
        """含 spark.table 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("spark.table('secret')")

    def test_malicious_spark_sql_rejected(self):
        """含 spark.sql 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("spark.sql('DROP TABLE')")

    def test_malicious_import_rejected(self):
        """含 import 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("import os")

    def test_malicious_subprocess_rejected(self):
        """含 subprocess 的右值——抛出 RenderError。"""
        with pytest.raises(RenderError, match="危险模式"):
            SparkCodeRenderer.render_filter_right("subprocess.run('ls')")

    def test_unpaired_quotes_rejected(self):
        """引号不配对——抛出 RenderError。"""
        with pytest.raises(RenderError, match="引号不配对"):
            SparkCodeRenderer.render_filter_right("'paid")

    def test_newline_injection_rejected(self):
        """含换行控制字符——抛出 RenderError。"""
        with pytest.raises(RenderError, match="控制字符"):
            SparkCodeRenderer.render_filter_right("'paid'\n# injected")

    def test_in_list_safe(self):
        """IN 列表字面量——安全校验通过。"""
        result = SparkCodeRenderer.render_filter_right("[1, 2, 3]")
        assert result == "[1, 2, 3]"


# ════════════════════════════════════════════
# Phase 6B 新增：render_join_key 安全测试
# ════════════════════════════════════════════


class TestRenderJoinKey:
    """Join 键引用安全渲染——df["col"] 格式。"""

    def test_valid_join_key(self):
        """合法 join 键正常渲染。"""
        result = SparkCodeRenderer.render_join_key("od", "user_id")
        assert result == 'od["user_id"]'

    def test_malicious_alias_rejected(self):
        """恶意 DataFrame 别名——抛出 RenderError。"""
        with pytest.raises(RenderError, match="非法标识符"):
            SparkCodeRenderer.render_join_key("od; DROP TABLE", "user_id")

    def test_malicious_column_rejected(self):
        """恶意列名——抛出 RenderError。"""
        with pytest.raises(RenderError, match="非法标识符"):
            SparkCodeRenderer.render_join_key("od", "user_id; DROP TABLE")

    def test_column_with_quotes_rejected(self):
        """含引号的列名——被标识符校验拦截（引号违反安全标识符正则）。"""
        with pytest.raises(RenderError):
            SparkCodeRenderer.render_join_key("od", 'user_id"')


# ════════════════════════════════════════════
# Phase 6B 新增：Compiler 层恶意输入回归测试
# ════════════════════════════════════════════


class TestMaliciousInputPhase6B:
    """Phase 6B aggregate/join/case_when 恶意输入安全测试。

    通过 Compiler 编译时触发 Renderer 安全校验——验证恶意输入在编译器层被拦截。
    """

    def test_aggregate_malicious_group_key_rejected(self):
        """aggregate group key 含 SQL 注入——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkAggregateStep(
                    input_alias="od",
                    group_keys=['region; DROP TABLE'],  # 恶意 key
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias="cnt",
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="非法标识符"):
            compiler.compile(plan)

    def test_aggregate_malicious_metric_alias_rejected(self):
        """aggregate metric alias 含恶意字符——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkAggregateStep(
                    input_alias="od",
                    group_keys=["region"],
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias='cnt; DROP TABLE',  # 恶意 alias
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="非法标识符"):
            compiler.compile(plan)

    def test_join_malicious_left_alias_rejected(self):
        """join left_alias 含恶意字符——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkJoinStep,
            SparkJoinType,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkReadStep(alias="up", source_name="t2", input_key="t2"),
                SparkJoinStep(
                    left_alias="od; import os",  # 恶意 alias
                    right_alias="up",
                    left_key="user_id",
                    right_key="user_id",
                    join_type=SparkJoinType.INNER,
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="非法标识符"):
            compiler.compile(plan)

    def test_join_malicious_key_rejected(self):
        """join key 含恶意字符——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkJoinStep,
            SparkJoinType,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkReadStep(alias="up", source_name="t2", input_key="t2"),
                SparkJoinStep(
                    left_alias="od",
                    right_alias="up",
                    left_key='user_id"; exec(',  # 恶意 key
                    right_key="user_id",
                    join_type=SparkJoinType.INNER,
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError):
            compiler.compile(plan)

    def test_case_when_malicious_condition_column_rejected(self):
        """case_when condition.normalized_name 含恶意字符——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkCaseWhenStep(
                    input_alias="od",
                    output_alias="label",
                    branches=[
                        SparkCaseWhenBranch(
                            label="bad",
                            condition=CaseWhenCondition(
                                operator="EQ",
                                normalized_name="status; DROP TABLE",  # 恶意列名
                                value="active",
                            ),
                        ),
                    ],
                    else_value="other",
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="非法标识符"):
            compiler.compile(plan)

    def test_case_when_malicious_output_alias_rejected(self):
        """case_when output_alias 含恶意字符——编译时抛出 RenderError。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
            SparkPlan,
            SparkReadStep,
        )

        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[
                SparkReadStep(alias="od", source_name="t", input_key="t"),
                SparkCaseWhenStep(
                    input_alias="od",
                    output_alias='label"; exec(',  # 恶意 output_alias
                    branches=[
                        SparkCaseWhenBranch(
                            label="ok",
                            condition=CaseWhenCondition(
                                operator="EQ",
                                normalized_name="status",
                                value="active",
                            ),
                        ),
                    ],
                    else_value="other",
                ),
            ],
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError):
            compiler.compile(plan)


# ════════════════════════════════════════════
# Phase 6C：窗口帧边界渲染测试
# ════════════════════════════════════════════


class TestRenderFrameBoundary:
    """render_frame_boundary 单元测试——覆盖三种符号值 + 整数字面量 + 非法输入拒绝。"""

    def test_unbounded_preceding_renders_camel_case(self):
        """unbounded_preceding → Window.unboundedPreceding（camelCase，非 UPPER_SNAKE_CASE）。"""
        result = SparkCodeRenderer.render_frame_boundary("unbounded_preceding")
        assert result == "Window.unboundedPreceding"

    def test_unbounded_following_renders_camel_case(self):
        """unbounded_following → Window.unboundedFollowing。"""
        result = SparkCodeRenderer.render_frame_boundary("unbounded_following")
        assert result == "Window.unboundedFollowing"

    def test_current_row_renders_camel_case(self):
        """current_row → Window.currentRow。"""
        result = SparkCodeRenderer.render_frame_boundary("current_row")
        assert result == "Window.currentRow"

    def test_case_insensitive_and_whitespace_tolerant(self):
        """大小写不敏感 + 前后空白容忍。"""
        result1 = SparkCodeRenderer.render_frame_boundary("  Unbounded_Preceding  ")
        assert result1 == "Window.unboundedPreceding"
        assert SparkCodeRenderer.render_frame_boundary("CURRENT_ROW") == "Window.currentRow"

    def test_non_negative_integer_passthrough(self):
        """非负整数字面量原样返回。"""
        assert SparkCodeRenderer.render_frame_boundary("0") == "0"
        assert SparkCodeRenderer.render_frame_boundary("3") == "3"
        assert SparkCodeRenderer.render_frame_boundary(" 10 ") == "10"

    def test_invalid_boundary_raises(self):
        """非白名单符号且非数字 → RenderError。"""
        with pytest.raises(RenderError, match="非法的窗口帧边界值"):
            SparkCodeRenderer.render_frame_boundary("invalid")
        with pytest.raises(RenderError, match="非法的窗口帧边界值"):
            SparkCodeRenderer.render_frame_boundary("preceding")
        with pytest.raises(RenderError, match="非法的窗口帧边界值"):
            SparkCodeRenderer.render_frame_boundary("-1")  # 负数不是非负整数

    def test_negative_integer_rejected(self):
        """负整数字面量被拒绝——digits only 检查不通过（含负号）。"""
        with pytest.raises(RenderError, match="非法的窗口帧边界值"):
            SparkCodeRenderer.render_frame_boundary("-5")

    def test_float_rejected(self):
        """浮点数字面量被拒绝。"""
        with pytest.raises(RenderError, match="非法的窗口帧边界值"):
            SparkCodeRenderer.render_frame_boundary("1.5")


class TestRenderFrameType:
    """render_frame_type 单元测试——rows/range 映射 + 非法输入拒绝。"""

    def test_rows_renders_rows_between(self):
        """rows → rowsBetween。"""
        assert SparkCodeRenderer.render_frame_type("rows") == "rowsBetween"

    def test_range_renders_range_between(self):
        """range → rangeBetween。"""
        assert SparkCodeRenderer.render_frame_type("range") == "rangeBetween"

    def test_case_insensitive_and_whitespace_tolerant(self):
        """大小写不敏感 + 前后空白容忍。"""
        assert SparkCodeRenderer.render_frame_type("  ROWS  ") == "rowsBetween"
        assert SparkCodeRenderer.render_frame_type("Range") == "rangeBetween"

    def test_invalid_frame_type_raises(self):
        """非 rows/range → RenderError。"""
        with pytest.raises(RenderError, match="非法的窗口帧类型"):
            SparkCodeRenderer.render_frame_type("groups")
        with pytest.raises(RenderError, match="非法的窗口帧类型"):
            SparkCodeRenderer.render_frame_type("")


# ════════════════════════════════════════════
# Phase 6C：窗口帧边界编译器集成测试补充
# ════════════════════════════════════════════


class TestWindowFrameBoundaryIntegration:
    """通过 Compiler 编译验证 render_frame_boundary 输出的正确 PySpark API 名称。"""

    def test_compiled_output_uses_camel_case_frame_constants(self):
        """编译产物中含 Window.unboundedPreceding / Window.currentRow，而非大写形式。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import (
            SparkPlan,
            SparkReadStep,
            SparkWindowExpr,
            SparkWindowFunction,
            SparkWindowStep,
        )

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="running_total",
                    input_column="amount",
                    partition_by=["region"],
                    order_by=["order_date"],
                ),
            ],
        )
        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-6",
            source_contract_hash="hash",
            steps=[SparkReadStep(alias="df", source_name="t", input_key="t"), step],
        )
        result = SparkCompiler().compile(plan)

        # 正确：camelCase
        assert "Window.unboundedPreceding" in result.raw_pyspark
        assert "Window.currentRow" in result.raw_pyspark
        # 错误形式不应出现
        assert "Window.UNBOUNDED_PRECEDING" not in result.raw_pyspark
        assert "Window.CURRENT_ROW" not in result.raw_pyspark
