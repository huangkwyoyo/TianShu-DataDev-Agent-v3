"""SQL 标识符注入安全测试——验证 SafeIdentifier 在 Schema 层拒绝恶意输入。

Phase 3B 安全加固——所有进入 SQL 渲染的 str 字段改用 SafeIdentifier 约束类型。
Pydantic AfterValidator 在构造 IR 对象时即拒绝非法字符，
构成 Schema 层第一道防线（Validator 层为第二道）。

覆盖：
- 所有 SafeIdentifier 字段的恶意值拒绝（10+ 注入模式）——参数化合并为 1 个测试
- 所有 SafeIdentifier 字段的合法值通过——参数化合并为 1 个测试
- 边界情况：空字符串、下划线开头、数字内嵌
- 集成验证：合法 plan 通过 Compiler 生成安全 SQL
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    AggregateSpec,
    AliasExpr,
    ColumnRef,
    FrameBoundary,
    FrameBoundaryKind,
    JoinType,
    NullOrder,
    Predicate,
    PredicateOperator,
    SortSpec,
    SqlLiteral,
    WhenBranch,
    WindowExpr,
    WindowFrame,
    WindowFrameType,
    WindowFunction,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    JoinStep,
    ScanStep,
    SqlBuildPlan,
    WindowStep,
)

# ════════════════════════════════════════════
# 恶意输入样本——覆盖 SQL 注入常见向量
# ════════════════════════════════════════════

MALICIOUS_SAMPLES: list[tuple[str, str]] = [
    ("x; DROP TABLE users; --", "分号+多语句注入"),
    ("x' OR '1'='1", "单引号布尔注入"),
    ('x" OR "1"="1', "双引号布尔注入"),
    ("x`); DELETE FROM sensitive; --", "反引号逃逸+DELETE"),
    ("x UNION SELECT * FROM passwords", "UNION 注入"),
    ("x ASC); DROP TABLE users; --", "闭合括号+DROP"),
    ("1 OR 1=1", "空格注入（数字开头也是非法）"),
    ("x--\nDELETE FROM users", "注释逃逸+换行注入"),
    ("x/**/OR/**/1=1", "块注释注入"),
    ("x; %00DROP TABLE", "空字节注入（%00 含特殊字符）"),
    ("a b", "含空格"),
    ("a,b", "含逗号"),
    ("a=b", "含等号"),
    ("a;b", "含分号"),
    ("a)b", "含括号"),
    ("a]b", "含方括号"),
    ("a\nb", "含换行符"),
    ("a\tb", "含制表符"),
]

VALID_SAMPLES: list[str] = [
    "id", "user_name", "table1", "_temp_agg", "dept_name",
    "salary_2024", "rn", "rank_val", "prev_salary", "cum_sum",
    "avg_amt", "cnt", "tf", "t1", "label", "score_level",
    "_internal", "column_123", "A", "a1234567890", "normalized_field_name",
]

# ════════════════════════════════════════════
# 参数化目标——所有使用 SafeIdentifier 的模型字段
# ════════════════════════════════════════════

# (标签, 构造工厂(注入值), 被注入字段名)
MALICIOUS_FIELD_TARGETS: list[tuple[str, callable, str]] = [
    ("ColumnRef.table_ref",
     lambda v: ColumnRef(table_ref=v, column_name="id", normalized_name="id"),
     "table_ref"),
    ("ColumnRef.column_name",
     lambda v: ColumnRef(table_ref="t", column_name=v, normalized_name="id"),
     "column_name"),
    ("ColumnRef.normalized_name",
     lambda v: ColumnRef(table_ref="t", column_name="id", normalized_name=v),
     "normalized_name"),
    ("SortSpec.column",
     lambda v: SortSpec(column=v),
     "column"),
    ("WindowExpr.alias",
     lambda v: WindowExpr(function=WindowFunction.ROW_NUMBER, alias=v),
     "alias"),
    ("AliasExpr.alias",
     lambda v: AliasExpr(
         expression=ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
         alias=v,
     ),
     "alias"),
    ("AggregateSpec.alias",
     lambda v: AggregateSpec(aggregation="SUM", input_column="amt", alias=v),
     "alias"),
    ("AggregateSpec.input_column",
     lambda v: AggregateSpec(aggregation="SUM", input_column=v, alias="total"),
     "input_column"),
    ("ScanStep.table_ref",
     lambda v: ScanStep(
         step_id="s1", table_ref=v,
         required_columns=[ColumnRef(table_ref=v, column_name="id", normalized_name="id")],
     ),
     "table_ref"),
    ("JoinStep.right_table_ref",
     lambda v: JoinStep(
         step_id="j1", right_table_ref=v, join_type=JoinType.INNER,
         join_keys=[(ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                     ColumnRef(table_ref=v, column_name="id", normalized_name="id"))],
         relationship_ref="rel_1",
     ),
     "right_table_ref"),
    ("CaseWhenStep.alias",
     lambda v: CaseWhenStep(
         step_id="case_1",
         cases=[WhenBranch(
             condition=Predicate(
                 left=ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                 operator=PredicateOperator.GTE,
                 right=SqlLiteral(value=90),
             ),
             result=SqlLiteral(value="优秀"),
         )],
         alias=v,
     ),
     "alias"),
]

# (标签, 构造工厂(合法值), 取值函数)
VALID_FIELD_TARGETS: list[tuple[str, callable, callable]] = [
    ("ColumnRef.table_ref",
     lambda v: ColumnRef(table_ref=v, column_name="id", normalized_name="id"),
     lambda m: m.table_ref),
    ("ColumnRef.column_name",
     lambda v: ColumnRef(table_ref="t", column_name=v, normalized_name=v),
     lambda m: m.column_name),
    ("SortSpec.column",
     lambda v: SortSpec(column=v),
     lambda m: m.column),
    ("WindowExpr.alias",
     lambda v: WindowExpr(function=WindowFunction.ROW_NUMBER, alias=v),
     lambda m: m.alias),
    ("AliasExpr.alias",
     lambda v: AliasExpr(
         expression=ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
         alias=v,
     ),
     lambda m: m.alias),
    ("AggregateSpec.alias",
     lambda v: AggregateSpec(aggregation="SUM", input_column="amt", alias=v),
     lambda m: m.alias),
    ("AggregateSpec.input_column",
     lambda v: AggregateSpec(aggregation="SUM", input_column=v, alias="total"),
     lambda m: m.input_column),
    ("ScanStep.table_ref",
     lambda v: ScanStep(
         step_id="s1", table_ref=v,
         required_columns=[ColumnRef(table_ref=v, column_name="id", normalized_name="id")],
     ),
     lambda m: m.table_ref),
    ("JoinStep.right_table_ref",
     lambda v: JoinStep(
         step_id="j1", right_table_ref=v, join_type=JoinType.INNER,
         join_keys=[(ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                     ColumnRef(table_ref=v, column_name="id", normalized_name="id"))],
         relationship_ref="rel_1",
     ),
     lambda m: m.right_table_ref),
    ("CaseWhenStep.alias",
     lambda v: CaseWhenStep(
         step_id="case_1",
         cases=[WhenBranch(
             condition=Predicate(
                 left=ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                 operator=PredicateOperator.GTE,
                 right=SqlLiteral(value=90),
             ),
             result=SqlLiteral(value="优秀"),
         )],
         alias=v,
     ),
     lambda m: m.alias),
]


# ════════════════════════════════════════════
# 统一参数化——所有 SafeIdentifier 字段恶意值拒绝
# ════════════════════════════════════════════


class TestAllSafeIdentifierFieldsRejectMalicious:
    """所有 SafeIdentifier 字段统一拒绝恶意输入——跨 11 个模型字段、18 种注入模式。"""

    @pytest.mark.parametrize("label,factory,_field", MALICIOUS_FIELD_TARGETS, ids=lambda x: x if isinstance(x, str) else "")
    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_field_rejects_malicious(self, label: str, factory, _field: str, malicious: str, desc: str):
        """任一 SafeIdentifier 字段拒绝任一恶意值——抛出 ValidationError。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            factory(malicious)


class TestAllSafeIdentifierFieldsAcceptValid:
    """所有 SafeIdentifier 字段统一接受合法值——跨 10 个模型字段、21 种合法值。"""

    @pytest.mark.parametrize("label,factory,getter", VALID_FIELD_TARGETS, ids=lambda x: x if isinstance(x, str) else "")
    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_field_accepts_valid(self, label: str, factory, getter, valid: str):
        """任一 SafeIdentifier 字段接受合法值——构造成功且值正确存回。"""
        model = factory(valid)
        assert getter(model) == valid


# ════════════════════════════════════════════
# 保留的独立测试——需要特定构造逻辑的边界情况
# ════════════════════════════════════════════


class TestSortSpecEdgeCases:
    """SortSpec 的额外边界。"""

    def test_sort_spec_with_null_order(self):
        """SortSpec 含合法 column + null_order 正常工作。"""
        spec = SortSpec(column="score", direction="DESC", null_order=NullOrder.FIRST)
        assert spec.column == "score"
        assert spec.null_order == NullOrder.FIRST


class TestWindowExprEdgeCases:
    """WindowExpr 完整构造的集成检查。"""

    def test_full_window_expr_with_safe_identifiers(self):
        """完整 WindowExpr 含 partition_by + order_by + frame——全部 SafeIdentifier 防护。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
            order_by=[SortSpec(column="salary", direction="DESC")],
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="cum_sum",
        )
        assert wexpr.alias == "cum_sum"


class TestAggregateSpecEnum:
    """AggregateSpec.aggregation 枚举值校验——Phase 3B 安全加固最后一道防线。"""

    def test_aggregation_rejects_malicious_function_name(self):
        """aggregation 拒绝不在 AggregationType 白名单中的函数名——SQL 注入尝试。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="COUNT); DROP TABLE users; --", input_column="id", alias="x")

    def test_aggregation_rejects_unknown_function(self):
        """aggregation 拒绝任意未注册的聚合函数名。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="MEDIAN", input_column="salary", alias="med_salary")

    def test_aggregation_rejects_lowercase_variant(self):
        """aggregation 拒绝小写变体——必须精确匹配枚举值。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="sum", input_column="amt", alias="total")

    def test_aggregation_rejects_empty_string(self):
        """aggregation 拒绝空字符串。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="", input_column="amt", alias="total")

    @pytest.mark.parametrize("valid_func", ["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"])
    def test_aggregation_accepts_all_valid_functions(self, valid_func: str):
        """aggregation 接受全部 6 种合法聚合函数。"""
        spec = AggregateSpec(aggregation=valid_func, input_column="amt", alias="result")
        assert spec.aggregation == valid_func

    def test_aggregation_enum_member_also_accepted(self):
        """aggregation 也接受 AggregationType 枚举成员。"""
        from tianshu_datadev.developer_spec.models import AggregationType
        spec = AggregateSpec(aggregation=AggregationType.AVG, input_column="score", alias="avg_score")
        assert spec.aggregation == "AVG"


class TestCaseWhenStepEdgeCases:
    """CaseWhenStep 的额外边界。"""

    def test_empty_alias_allowed(self):
        """空 alias = 无别名——合法（CaseWhenStep 默认值）。"""
        step = CaseWhenStep(step_id="case_no_alias", cases=[], alias="")
        assert step.alias == ""


# ════════════════════════════════════════════
# 集成测试——合法 plan 经 SafeIdentifier 后正确编译
# ════════════════════════════════════════════


class TestSafeIdentifierIntegration:
    """端到端验证：SafeIdentifier 不阻挡合法 plan 的编译和渲染。"""

    def test_case_when_plan_compiles_safely(self):
        """含 CaseWhenStep 的 plan——SafeIdentifier 字段全部合法，正常编译。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_safe",
            spec_hash="safe_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1", table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_labels",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=90),
                            ),
                            result=SqlLiteral(value="优秀"),
                        )
                    ],
                    else_value=SqlLiteral(value="及格"),
                    alias="score_label",
                ),
            ],
        )
        sql = DuckDbSqlCompiler().compile(plan).sql
        assert "score_label" in sql
        assert ";" not in sql
        assert "DROP" not in sql

    def test_window_plan_compiles_safely(self):
        """含 WindowStep 的 plan——SafeIdentifier 字段全部合法，正常编译。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_window_safe",
            spec_hash="safe_hash_002",
            steps=[
                ScanStep(
                    step_id="scan_1", table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="tf", column_name="salary", normalized_name="salary"),
                    ],
                ),
                WindowStep(
                    step_id="win_1",
                    window_exprs=[
                        WindowExpr(
                            function=WindowFunction.ROW_NUMBER,
                            partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                            order_by=[SortSpec(column="salary", direction="DESC")],
                            alias="rn",
                        ),
                        WindowExpr(
                            function=WindowFunction.SUM_OVER,
                            input=ColumnRef(table_ref="tf", column_name="salary", normalized_name="salary"),
                            partition_by=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                            alias="dept_total",
                        ),
                    ],
                ),
            ],
        )
        sql = DuckDbSqlCompiler().compile(plan).sql
        assert "rn" in sql
        assert "dept_total" in sql
        assert "ROW_NUMBER" in sql
        assert ");" not in sql

    def test_aggregate_plan_compiles_safely(self):
        """含 AggregateStep 的 plan——SafeIdentifier alias 正常编译。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_agg_safe",
            spec_hash="safe_hash_003",
            steps=[
                ScanStep(
                    step_id="scan_1", table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                    metrics=[
                        AggregateSpec(aggregation="SUM", input_column="amt", alias="total_amt"),
                        AggregateSpec(aggregation="COUNT", alias="row_count"),
                    ],
                ),
            ],
        )
        sql = DuckDbSqlCompiler().compile(plan).sql
        assert "total_amt" in sql
        assert "row_count" in sql

    def test_compiler_determinism_with_safe_identifiers(self):
        """SafeIdentifier 不影响编译确定性——相同 plan 两次编译相同 SQL + hash。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_det",
            spec_hash="det_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1", table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_labels",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=90),
                            ),
                            result=SqlLiteral(value="优秀"),
                        )
                    ],
                    else_value=SqlLiteral(value="及格"),
                    alias="score_label",
                ),
            ],
        )
        compiler = DuckDbSqlCompiler()
        result1 = compiler.compile(plan)
        result2 = compiler.compile(plan)
        assert result1.sql == result2.sql
        assert result1.sql_sha256 == result2.sql_sha256

    def test_aggregation_enum_prevents_injection_in_pipeline(self):
        """端到端：AggregationType 枚举在 Schema 层拒注入，合法 plan 全链路通过。"""
        from tianshu_datadev.developer_spec.models import SourceManifest
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
        from tianshu_datadev.sql.validator import SqlBuildPlanValidator

        plan = SqlBuildPlan(
            plan_id="plan_test_agg_enum",
            spec_hash="agg_enum_001",
            steps=[
                ScanStep(
                    step_id="scan_1", table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")],
                    metrics=[
                        AggregateSpec(aggregation="SUM", input_column="amt", alias="total_amt"),
                        AggregateSpec(aggregation="COUNT", alias="row_count"),
                        AggregateSpec(aggregation="AVG", input_column="amt", alias="avg_amt"),
                        AggregateSpec(aggregation="MAX", input_column="amt", alias="max_amt"),
                        AggregateSpec(aggregation="MIN", input_column="amt", alias="min_amt"),
                        AggregateSpec(aggregation="COUNT_DISTINCT", input_column="dept", alias="distinct_dept"),
                    ],
                ),
            ],
        )
        manifest = SourceManifest(manifest_id="test_manifest", spec_hash="agg_enum_001", tables=[])
        validator = SqlBuildPlanValidator()
        validator.validate(plan, manifest)
        sql = DuckDbSqlCompiler().compile(plan).sql
        assert "SUM(amt)" in sql
        assert "COUNT(*)" in sql
        assert "COUNT(DISTINCT dept)" in sql
        assert "DROP" not in sql


# ════════════════════════════════════════════
# SafeIdentifier 类型本身的行为
# ════════════════════════════════════════════


class TestSafeIdentifierTypeBehavior:
    """SafeIdentifier 类型本身的边界行为。"""

    def test_empty_string_is_valid(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        assert _validate_sql_identifier("") == ""

    def test_leading_underscore_valid(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        assert _validate_sql_identifier("_temp_agg") == "_temp_agg"

    def test_number_leading_rejected(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("123abc")

    @pytest.mark.parametrize("char", ["@", "#", "$", "%", "&", "*", "!", "?", "/", "\\", "|"])
    def test_special_chars_rejected(self, char: str):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier(f"col{char}name")

    def test_chinese_chars_rejected(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("标签")

    def test_hyphen_rejected(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("user-name")

    def test_dot_rejected(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("tf.score")

    def test_long_identifier_valid(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        assert _validate_sql_identifier("a" + "b" * 200) == "a" + "b" * 200

    def test_single_char_valid(self):
        from tianshu_datadev.planning.models import _validate_sql_identifier
        assert _validate_sql_identifier("x") == "x"
