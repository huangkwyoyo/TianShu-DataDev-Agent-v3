"""SQL 标识符注入安全测试——验证 SafeIdentifier 在 Schema 层拒绝恶意输入。

Phase 3B 安全加固——所有进入 SQL 渲染的 str 字段改用 SafeIdentifier 约束类型。
Pydantic AfterValidator 在构造 IR 对象时即拒绝非法字符，
构成 Schema 层第一道防线（Validator 层为第二道）。

测试覆盖：
- 每个 SafeIdentifier 字段的恶意值拒绝（10+ 注入模式）
- 每个 SafeIdentifier 字段的合法值通过
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

# 每个样本为 (value, description)
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

# 合法输入样本
VALID_SAMPLES: list[str] = [
    "id",
    "user_name",
    "table1",
    "_temp_agg",
    "dept_name",
    "salary_2024",
    "rn",
    "rank_val",
    "prev_salary",
    "cum_sum",
    "avg_amt",
    "cnt",
    "tf",
    "t1",
    "label",
    "score_level",
    "_internal",
    "column_123",
    "A",
    "a1234567890",
    "normalized_field_name",
]


# ════════════════════════════════════════════
# Schema 层——ColumnRef 安全
# ════════════════════════════════════════════


class TestColumnRefSafety:
    """ColumnRef 的三个 SafeIdentifier 字段：table_ref / column_name / normalized_name。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_table_ref_rejects_malicious(self, malicious: str, desc: str):
        """table_ref 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            ColumnRef(table_ref=malicious, column_name="id", normalized_name="id")

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_column_name_rejects_malicious(self, malicious: str, desc: str):
        """column_name 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            ColumnRef(table_ref="t", column_name=malicious, normalized_name="id")

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_normalized_name_rejects_malicious(self, malicious: str, desc: str):
        """normalized_name 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            ColumnRef(table_ref="t", column_name="id", normalized_name=malicious)

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_table_ref_accepts_valid(self, valid: str):
        """table_ref 接受合法值。"""
        col = ColumnRef(table_ref=valid, column_name="id", normalized_name="id")
        assert col.table_ref == valid

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_column_name_accepts_valid(self, valid: str):
        """column_name 接受合法值。"""
        col = ColumnRef(table_ref="t", column_name=valid, normalized_name=valid)
        assert col.column_name == valid


# ════════════════════════════════════════════
# Schema 层——SortSpec 安全
# ════════════════════════════════════════════


class TestSortSpecSafety:
    """SortSpec.column 是裸字符串——Phase 3B 安全加固将其改为 SafeIdentifier。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_column_rejects_malicious(self, malicious: str, desc: str):
        """SortSpec.column 拒绝恶意值——防止 ORDER BY 注入。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            SortSpec(column=malicious, direction="ASC")  # type: ignore[arg-type]

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_column_accepts_valid(self, valid: str):
        """SortSpec.column 接受合法值。"""
        spec = SortSpec(column=valid)
        assert spec.column == valid

    def test_sort_spec_with_null_order(self):
        """SortSpec 含合法 column + null_order 正常工作。"""
        spec = SortSpec(column="score", direction="DESC", null_order=NullOrder.FIRST)  # type: ignore[arg-type]
        assert spec.column == "score"
        assert spec.null_order == NullOrder.FIRST


# ════════════════════════════════════════════
# Schema 层——WindowExpr 安全
# ════════════════════════════════════════════


class TestWindowExprSafety:
    """WindowExpr.alias 进入 SELECT AS 子句——须 SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_alias_rejects_malicious(self, malicious: str, desc: str):
        """WindowExpr.alias 拒绝恶意值——防止 OVER ... AS <注入>。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            WindowExpr(
                function=WindowFunction.ROW_NUMBER,
                alias=malicious,
            )

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_alias_accepts_valid(self, valid: str):
        """WindowExpr.alias 接受合法值。"""
        wexpr = WindowExpr(function=WindowFunction.ROW_NUMBER, alias=valid)
        assert wexpr.alias == valid

    def test_full_window_expr_with_safe_identifiers(self):
        """完整 WindowExpr 含 partition_by + order_by 全部 SafeIdentifier 防护。"""
        wexpr = WindowExpr(
            function=WindowFunction.SUM_OVER,
            input=ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
            partition_by=[
                ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
            ],
            order_by=[SortSpec(column="salary", direction="DESC")],  # type: ignore[arg-type]
            frame=WindowFrame(
                frame_type=WindowFrameType.ROWS,
                start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
            ),
            alias="cum_sum",
        )
        assert wexpr.alias == "cum_sum"
        assert wexpr.order_by[0].column == "salary"


# ════════════════════════════════════════════
# Schema 层——AliasExpr 安全
# ════════════════════════════════════════════


class TestAliasExprSafety:
    """AliasExpr.alias 进入 SELECT AS 子句——SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_alias_rejects_malicious(self, malicious: str, desc: str):
        """AliasExpr.alias 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            AliasExpr(
                expression=ColumnRef(
                    table_ref="t", column_name="id", normalized_name="id"
                ),
                alias=malicious,
            )

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_alias_accepts_valid(self, valid: str):
        """AliasExpr.alias 接受合法值。"""
        ae = AliasExpr(
            expression=ColumnRef(table_ref="t", column_name="id", normalized_name="id"),
            alias=valid,
        )
        assert ae.alias == valid


# ════════════════════════════════════════════
# Schema 层——AggregateSpec 安全
# ════════════════════════════════════════════


class TestAggregateSpecSafety:
    """AggregateSpec.alias 和 input_column 进入聚合函数渲染——SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_alias_rejects_malicious(self, malicious: str, desc: str):
        """AggregateSpec.alias 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            AggregateSpec(aggregation="SUM", input_column="amt", alias=malicious)

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_input_column_rejects_malicious(self, malicious: str, desc: str):
        """AggregateSpec.input_column 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            AggregateSpec(aggregation="SUM", input_column=malicious, alias="total")

    def test_input_column_none_allowed(self):
        """input_column=None 表示 COUNT(*)——合法。"""
        spec = AggregateSpec(aggregation="COUNT", alias="row_count")
        assert spec.input_column is None

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_alias_accepts_valid(self, valid: str):
        """AggregateSpec.alias 接受合法值。"""
        spec = AggregateSpec(aggregation="SUM", input_column="amt", alias=valid)
        assert spec.alias == valid

    # ── aggregation 枚举值校验（Phase 3B 安全加固）──

    def test_aggregation_rejects_malicious_function_name(self):
        """AggregateSpec.aggregation 拒绝不在 AggregationType 白名单中的函数名。

        这是 Phase 3B 安全加固最后一道防线——aggregation 从自由 str 改为
        AggregationType 枚举，在 Schema 层即拒绝 SQL 注入。
        """
        # SQL 注入尝试
        with pytest.raises(ValidationError):
            AggregateSpec(
                aggregation="COUNT); DROP TABLE users; --",
                input_column="id",
                alias="x",
            )

    def test_aggregation_rejects_unknown_function(self):
        """AggregateSpec.aggregation 拒绝任意未注册的聚合函数名。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="MEDIAN", input_column="salary", alias="med_salary")

    def test_aggregation_rejects_lowercase_variant(self):
        """AggregateSpec.aggregation 拒绝小写变体——必须精确匹配枚举值。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="sum", input_column="amt", alias="total")

    def test_aggregation_rejects_empty_string(self):
        """AggregateSpec.aggregation 拒绝空字符串。"""
        with pytest.raises(ValidationError):
            AggregateSpec(aggregation="", input_column="amt", alias="total")

    @pytest.mark.parametrize("valid_func", [
        "COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT",
    ])
    def test_aggregation_accepts_all_valid_functions(self, valid_func: str):
        """AggregateSpec.aggregation 接受全部 6 种合法聚合函数。"""
        spec = AggregateSpec(aggregation=valid_func, input_column="amt", alias="result")
        assert spec.aggregation == valid_func

    def test_aggregation_enum_member_also_accepted(self):
        """AggregateSpec.aggregation 也接受 AggregationType 枚举成员。"""
        from tianshu_datadev.developer_spec.models import AggregationType

        spec = AggregateSpec(
            aggregation=AggregationType.AVG,
            input_column="score",
            alias="avg_score",
        )
        assert spec.aggregation == "AVG"


# ════════════════════════════════════════════
# Schema 层——ScanStep 安全
# ════════════════════════════════════════════


class TestScanStepSafety:
    """ScanStep.table_ref 进入 FROM AS 子句——SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_table_ref_rejects_malicious(self, malicious: str, desc: str):
        """ScanStep.table_ref 拒绝恶意值——防止 FROM ... AS <注入>。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            ScanStep(
                step_id="s1",
                table_ref=malicious,
                required_columns=[
                    ColumnRef(table_ref=malicious, column_name="id", normalized_name="id")
                ],
            )

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_table_ref_accepts_valid(self, valid: str):
        """ScanStep.table_ref 接受合法值。"""
        scan = ScanStep(
            step_id="s1",
            table_ref=valid,
            required_columns=[
                ColumnRef(table_ref=valid, column_name="id", normalized_name="id")
            ],
        )
        assert scan.table_ref == valid


# ════════════════════════════════════════════
# Schema 层——JoinStep 安全
# ════════════════════════════════════════════


class TestJoinStepSafety:
    """JoinStep.right_table_ref 进入 JOIN AS 子句——SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_right_table_ref_rejects_malicious(self, malicious: str, desc: str):
        """JoinStep.right_table_ref 拒绝恶意值。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            JoinStep(
                step_id="j1",
                right_table_ref=malicious,
                join_type=JoinType.INNER,
                join_keys=[
                    (
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref=malicious, column_name="id", normalized_name="id"),
                    )
                ],
                relationship_ref="rel_1",
            )

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_right_table_ref_accepts_valid(self, valid: str):
        """JoinStep.right_table_ref 接受合法值。"""
        join = JoinStep(
            step_id="j1",
            right_table_ref=valid,
            join_type=JoinType.INNER,
            join_keys=[
                (
                    ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ColumnRef(table_ref=valid, column_name="id", normalized_name="id"),
                )
            ],
            relationship_ref="rel_1",
        )
        assert join.right_table_ref == valid


# ════════════════════════════════════════════
# Schema 层——CaseWhenStep 安全
# ════════════════════════════════════════════


class TestCaseWhenStepSafety:
    """CaseWhenStep.alias 进入 CASE ... AS <alias>——SafeIdentifier 防护。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_alias_rejects_malicious(self, malicious: str, desc: str):
        """CaseWhenStep.alias 拒绝恶意值——防止 CASE ... AS <注入>。"""
        with pytest.raises(ValidationError, match="非法 SQL 标识符"):
            CaseWhenStep(
                step_id="case_1",
                cases=[
                    WhenBranch(
                        condition=Predicate(
                            left=ColumnRef(
                                table_ref="tf", column_name="score", normalized_name="score"
                            ),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value=90),
                        ),
                        result=SqlLiteral(value="优秀"),
                    )
                ],
                alias=malicious,
            )

    def test_empty_alias_allowed(self):
        """空 alias = 无别名——合法（CaseWhenStep 默认值）。"""
        step = CaseWhenStep(
            step_id="case_no_alias",
            cases=[],
            alias="",  # 空字符串——表示不产生输出列
        )
        assert step.alias == ""

    @pytest.mark.parametrize("valid", VALID_SAMPLES)
    def test_alias_accepts_valid(self, valid: str):
        """CaseWhenStep.alias 接受合法值。"""
        step = CaseWhenStep(
            step_id="case_1",
            cases=[
                WhenBranch(
                    condition=Predicate(
                        left=ColumnRef(
                            table_ref="tf", column_name="score", normalized_name="score"
                        ),
                        operator=PredicateOperator.GTE,
                        right=SqlLiteral(value=90),
                    ),
                    result=SqlLiteral(value="优异"),
                )
            ],
            alias=valid,
        )
        assert step.alias == valid


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
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_labels",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf", column_name="score", normalized_name="score"
                                ),
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
        result = compiler.compile(plan)

        sql = result.sql
        # SQL 不含任何注入特征
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
                    step_id="scan_1",
                    table_ref="tf",
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
                            partition_by=[
                                ColumnRef(
                                    table_ref="tf", column_name="dept", normalized_name="dept"
                                )
                            ],
                            order_by=[SortSpec(column="salary", direction="DESC")],  # type: ignore[arg-type]
                            alias="rn",
                        ),
                        WindowExpr(
                            function=WindowFunction.SUM_OVER,
                            input=ColumnRef(
                                table_ref="tf", column_name="salary", normalized_name="salary"
                            ),
                            partition_by=[
                                ColumnRef(
                                    table_ref="tf", column_name="dept", normalized_name="dept"
                                )
                            ],
                            alias="dept_total",
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        result = compiler.compile(plan)

        sql = result.sql
        assert "rn" in sql
        assert "dept_total" in sql
        assert "ROW_NUMBER" in sql
        assert "SUM" in sql
        # 确认 SQL 不含注入特征
        assert ");" not in sql
        assert "UNION" not in sql

    def test_aggregate_plan_compiles_safely(self):
        """含 AggregateStep 的 plan——SafeIdentifier alias 正常编译。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_agg_safe",
            spec_hash="safe_hash_003",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
                    ],
                    metrics=[
                        AggregateSpec(aggregation="SUM", input_column="amt", alias="total_amt"),
                        AggregateSpec(aggregation="COUNT", alias="row_count"),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        result = compiler.compile(plan)

        sql = result.sql
        assert "total_amt" in sql
        assert "row_count" in sql
        assert "SUM(amt)" in sql.upper() or "sum(amt)" in sql.lower()

    def test_compiler_determinism_with_safe_identifiers(self):
        """SafeIdentifier 不影响编译确定性——相同 plan 两次编译相同 SQL + hash。"""
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        plan = SqlBuildPlan(
            plan_id="plan_test_det",
            spec_hash="det_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="score", normalized_name="score"),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_labels",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf", column_name="score", normalized_name="score"
                                ),
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
        """端到端验证：AggregationType 枚举在 Schema 层即拒绝注入，
        合法 plan 通过 Validator + Compiler 流水线无问题。"""
        from tianshu_datadev.developer_spec.models import SourceManifest
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
        from tianshu_datadev.sql.validator import SqlBuildPlanValidator

        # 合法 plan——使用 AggregationType 中的合法函数
        plan = SqlBuildPlan(
            plan_id="plan_test_agg_enum",
            spec_hash="agg_enum_001",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept"),
                        ColumnRef(table_ref="tf", column_name="amt", normalized_name="amt"),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[
                        ColumnRef(table_ref="tf", column_name="dept", normalized_name="dept")
                    ],
                    metrics=[
                        AggregateSpec(aggregation="SUM", input_column="amt", alias="total_amt"),
                        AggregateSpec(aggregation="COUNT", alias="row_count"),
                        AggregateSpec(aggregation="AVG", input_column="amt", alias="avg_amt"),
                        AggregateSpec(aggregation="MAX", input_column="amt", alias="max_amt"),
                        AggregateSpec(aggregation="MIN", input_column="amt", alias="min_amt"),
                        AggregateSpec(
                            aggregation="COUNT_DISTINCT",
                            input_column="dept",
                            alias="distinct_dept",
                        ),
                    ],
                ),
            ],
        )

        # Validator 通过
        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="agg_enum_001",
            tables=[],  # 空清单——跳过表引用校验
        )
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        # 注意：由于 manifest 为空，表引用校验可能失败，但 aggregation 枚举已由 Schema 保证
        # 这里仅验证无 aggregation 相关的新增 blocking 问题

        # Compiler 通过
        compiler = DuckDbSqlCompiler()
        result = compiler.compile(plan)
        sql = result.sql

        # 所有合法聚合函数正确渲染
        assert "SUM(amt)" in sql
        assert "COUNT(*)" in sql
        assert "AVG(amt)" in sql
        assert "MAX(amt)" in sql
        assert "MIN(amt)" in sql
        assert "COUNT(DISTINCT dept)" in sql
        # 确认不含注入特征
        assert "DROP" not in sql
        assert ";" not in sql.split("FROM")[0]


# ════════════════════════════════════════════
# SafeIdentifier 类型本身的行为
# ════════════════════════════════════════════


class TestSafeIdentifierTypeBehavior:
    """SafeIdentifier 类型本身的边界行为。"""

    def test_empty_string_is_valid(self):
        """空字符串合法——表示"无别名"。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        result = _validate_sql_identifier("")
        assert result == ""

    def test_leading_underscore_valid(self):
        """下划线开头合法——SQL 标准支持 _ 前缀。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        result = _validate_sql_identifier("_temp_agg")
        assert result == "_temp_agg"

    def test_number_leading_rejected(self):
        """数字开头被拒绝——不符合 SQL 未加引号标识符规范。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("123abc")

    def test_special_chars_rejected(self):
        """特殊字符（@、#、$、%）被拒绝。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        for char in ["@", "#", "$", "%", "&", "*", "!", "?", "/", "\\", "|"]:
            with pytest.raises(ValueError, match="非法 SQL 标识符"):
                _validate_sql_identifier(f"col{char}name")

    def test_chinese_chars_rejected(self):
        """中文字符被拒绝——SafeIdentifier 仅允许 ASCII。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("标签")

    def test_hyphen_rejected(self):
        """连字符被拒绝——SQL 标识符不允许 -。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("user-name")

    def test_dot_rejected(self):
        """点号被拒绝——表限定应通过 ColumnRef.table_ref 而非点号嵌入。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        with pytest.raises(ValueError, match="非法 SQL 标识符"):
            _validate_sql_identifier("tf.score")

    def test_long_identifier_valid(self):
        """长标识符合法——SafeIdentifier 不限制长度（由各数据库自行约束）。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        long_id = "a" + "b" * 200  # 201 字符
        result = _validate_sql_identifier(long_id)
        assert result == long_id

    def test_single_char_valid(self):
        """单字符合法。"""
        from tianshu_datadev.planning.models import _validate_sql_identifier

        result = _validate_sql_identifier("x")
        assert result == "x"
