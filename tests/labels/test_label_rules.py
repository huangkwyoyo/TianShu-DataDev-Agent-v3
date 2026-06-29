"""标签枚举完整测试——Phase 3B CaseWhenStep 枚举值覆盖。

覆盖：
- 标签枚举完整覆盖——所有值在 DeveloperSpec 声明中通过
- 标签枚举值越界拒绝——未声明值产生 blocking OpenQuestion
- else_value 不校验——默认值可以是未声明值
- 空 cases 不报错
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    ColumnDecl,
    InputTableDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    PredicateOperator,
    SqlLiteral,
    WhenBranch,
)
from tianshu_datadev.planning.sql_build_plan import (
    CaseWhenStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.validation.label_validator import validate_label_enums

# ── 辅助函数 ──


def _make_spec_with_enums(
    enum_values: list[str],
    field_name: str = "score",
) -> ParsedDeveloperSpec:
    """创建含指定枚举值声明的 DeveloperSpec。"""
    return ParsedDeveloperSpec(
        spec_id="test_labels",
        spec_hash="label_hash_001",
        title="标签枚举测试",
        description="用于验证 CASE WHEN 枚举值校验",
        input_tables=[
            InputTableDecl(
                table_alias="tf",
                source_table="test_fact",
                columns=[
                    ColumnDecl(
                        column_name=field_name,
                        normalized_name=field_name,
                        enum_values=enum_values,
                    )
                ],
            )
        ],
        metrics=[],
        dimensions=[],
        output_spec=OutputSpecDecl(columns=["label"], grain=[]),
    )


def _make_case_when_plan(
    results: list[str],
    else_value: str | None = None,
    field_name: str = "score",
) -> SqlBuildPlan:
    """创建含单个 CaseWhenStep 的 SqlBuildPlan。

    Args:
        results: 每个 WhenBranch 的 result 值
        else_value: ELSE 默认值
        field_name: 条件中的字段名
    """
    cases: list[WhenBranch] = []
    for i, result_val in enumerate(results):
        threshold = 90 - i * 10  # 逐个递减
        cases.append(
            WhenBranch(
                condition=Predicate(
                    left=ColumnRef(
                        table_ref="tf",
                        column_name=field_name,
                        normalized_name=field_name,
                    ),
                    operator=PredicateOperator.GTE,
                    right=SqlLiteral(value=threshold),
                ),
                result=SqlLiteral(value=result_val),
            )
        )

    return SqlBuildPlan(
        plan_id="test_case_plan",
        spec_hash="label_hash_001",
        steps=[
            ScanStep(
                step_id="scan_1",
                table_ref="tf",
                required_columns=[
                    ColumnRef(
                        table_ref="tf",
                        column_name=field_name,
                        normalized_name=field_name,
                    ),
                ],
            ),
            CaseWhenStep(
                step_id="case_labels",
                cases=cases,
                else_value=SqlLiteral(value=else_value) if else_value else None,
                alias="label",
            ),
        ],
    )


# ════════════════════════════════════════════
# 测试
# ════════════════════════════════════════════


class TestLabelEnumCoverage:
    """标签枚举完整覆盖测试。"""

    def test_all_enum_values_declared_passes(self):
        """所有 CASE WHEN 结果值都在声明枚举中——应通过。"""
        spec = _make_spec_with_enums(["优秀", "良好", "及格", "不及格"])
        plan = _make_case_when_plan(["优秀", "良好", "及格"], else_value="不及格")
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 0, f"全部声明值应通过，实际: {questions}"

    def test_single_enum_value_declared_passes(self):
        """单个枚举值在声明中——应通过。"""
        spec = _make_spec_with_enums(["A", "B", "C", "D"])
        plan = _make_case_when_plan(["A"], else_value="D")
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 0, f"声明值应通过: {questions}"

    def test_numeric_enum_values_passes(self):
        """数值枚举值在声明中——应通过。"""
        spec = _make_spec_with_enums(["1", "2", "3", "4"])
        plan = _make_case_when_plan(["1", "3"], else_value="4")
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 0


class TestLabelEnumRejection:
    """标签枚举值越界拒绝测试。"""

    def test_undeclared_enum_value_rejected(self):
        """未声明的枚举值被拒绝。"""
        spec = _make_spec_with_enums(["A", "B", "C"])
        plan = _make_case_when_plan(["A", "Z"])  # Z 未声明
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) >= 1, f"未声明值应被拒绝: {questions}"
        undeclared_qs = [
            q for q in questions if "Z" in q.description
        ]
        assert len(undeclared_qs) >= 1, f"应报告 Z 未声明: {questions}"
        for q in questions:
            assert q.blocking is True, f"枚举越界应为 blocking: {q.question_id}"

    def test_all_values_undeclared_rejected(self):
        """所有值都未声明——全部拒绝。"""
        spec = _make_spec_with_enums(["X", "Y"])
        plan = _make_case_when_plan(["A", "B", "C"], else_value="D")
        questions = validate_label_enums(plan, spec=spec)
        # A, B, C 都未在 [X, Y] 中
        assert len(questions) == 3, (
            f"3 个未声明值应产生 3 个问题，实际: {len(questions)}"
        )

    def test_partial_undeclared_first_branch_rejected(self):
        """第一个分支枚举值未声明——单独拒绝。"""
        spec = _make_spec_with_enums(["良好", "及格"])
        plan = _make_case_when_plan(["优秀", "良好"])  # "优秀" 未声明
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 1, f"仅 1 个未声明: {questions}"
        assert "优秀" in questions[0].description


class TestLabelEnumEdgeCases:
    """标签枚举边界情况测试。"""

    def test_else_value_not_checked(self):
        """ELSE 默认值不校验——可以为未声明值。"""
        spec = _make_spec_with_enums(["A", "B"])
        plan = _make_case_when_plan(["A"], else_value="未分类")
        questions = validate_label_enums(plan, spec=spec)
        # "A" 已声明，"未分类" 是 else_value——不校验
        assert len(questions) == 0, f"ELSE 值不校验: {questions}"

    def test_empty_cases_no_errors(self):
        """空的 CASE WHEN（无分支）不产生错误。"""
        spec = _make_spec_with_enums(["A"])
        plan = SqlBuildPlan(
            plan_id="empty_case",
            spec_hash="label_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="score",
                            normalized_name="score",
                        ),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_empty",
                    cases=[],
                    alias="empty_label",
                ),
            ],
        )
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 0

    def test_no_spec_no_manifest_passes(self):
        """无 spec 也无 manifest 时——静默通过（无法校验）。"""
        plan = _make_case_when_plan(["任意值"])
        questions = validate_label_enums(plan, spec=None, manifest=None)
        # 无声明枚举值可对照——通过但不安全
        assert len(questions) == 0

    def test_null_literal_value_not_checked(self):
        """NULL 字面量不校验。"""
        spec = _make_spec_with_enums(["A"])
        plan = SqlBuildPlan(
            plan_id="null_case",
            spec_hash="label_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="score",
                            normalized_name="score",
                        ),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_null",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="score",
                                    normalized_name="score",
                                ),
                                operator=PredicateOperator.IS_NULL,
                            ),
                            result=SqlLiteral(value=None),
                        )
                    ],
                    alias="null_label",
                ),
            ],
        )
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 0

    def test_multiple_case_when_steps_all_checked(self):
        """多个 CaseWhenStep 全部校验。"""
        spec = _make_spec_with_enums(["A", "B"], field_name="score")
        plan = SqlBuildPlan(
            plan_id="multi_case",
            spec_hash="label_hash_001",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="score",
                            normalized_name="score",
                        ),
                    ],
                ),
                CaseWhenStep(
                    step_id="case_1",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="score",
                                    normalized_name="score",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=90),
                            ),
                            result=SqlLiteral(value="A"),  # 合法
                        )
                    ],
                    alias="grade",
                ),
                CaseWhenStep(
                    step_id="case_2",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="tf",
                                    column_name="score",
                                    normalized_name="score",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=80),
                            ),
                            result=SqlLiteral(value="Z"),  # 非法！
                        )
                    ],
                    alias="level",
                ),
            ],
        )
        questions = validate_label_enums(plan, spec=spec)
        assert len(questions) == 1
        assert "Z" in questions[0].description
        assert questions[0].blocking is True


# ════════════════════════════════════════════
# Phase 3B.1：EnumProfiler 测试
# ════════════════════════════════════════════


import pytest

from tianshu_datadev.profiling.enum_profiler import ColumnSample, EnumProfiler
from tianshu_datadev.profiling.models import (
    EnumConfidenceTier,
    EnumFieldClass,
    EnumProfile,
)


class TestEnumProfilerFlag:
    """Flag（标志位）检测——二值判定 + 字段名信号。"""

    @pytest.mark.parametrize(
        "values,field_name,expected_tier,expected_class",
        [
            (["0", "1", "1", "0"], "is_valid", EnumConfidenceTier.CERTAIN, EnumFieldClass.FLAG),
            (["Y", "N", "Y"], "pass_yn", EnumConfidenceTier.CERTAIN, EnumFieldClass.FLAG),
            (["是", "否", "是"], "是否有效", EnumConfidenceTier.CERTAIN, EnumFieldClass.FLAG),
            # 字段名无信号 → 降级
            (["0", "1", "0", "1"], "score", EnumConfidenceTier.LOW, EnumFieldClass.FLAG),
            # distinct=3 → 跳过
            (["0", "1", "2"], "is_valid", EnumConfidenceTier.NOT_ENUM, None),
        ],
    )
    def test_flag_detection(self, values, field_name, expected_tier, expected_class):
        """Flag 检测覆盖：CERTAIN/LOW/NOT_ENUM 三层。"""
        sample = ColumnSample(
            table_ref="tf", column_name=field_name,
            normalized_name=field_name, values=values,
        )
        profile = EnumProfiler().profile([sample]).profiles[0]
        assert profile.tier == expected_tier
        assert profile.field_class == expected_class


class TestEnumProfilerStatus:
    """Status（状态码）检测——模式匹配 + 字段名 + 词典。"""

    @pytest.mark.parametrize(
        "values,field_name,expected_tier,expected_class",
        [
            (["Approved", "Pending"], "order_status", EnumConfidenceTier.HIGH, EnumFieldClass.STATUS),
            (
                ["Pending", "In Progress", "In-Progress"], "task_state",
                EnumConfidenceTier.HIGH, EnumFieldClass.STATUS,
            ),
            # distinct > 30 → 跳过
            ([f"S{i}" for i in range(35)], "status_field", EnumConfidenceTier.NOT_ENUM, None),
        ],
    )
    def test_status_detection(self, values, field_name, expected_tier, expected_class):
        sample = ColumnSample(
            table_ref="tf", column_name=field_name,
            normalized_name=field_name, values=values,
        )
        profile = EnumProfiler().profile([sample]).profiles[0]
        assert profile.tier == expected_tier
        assert profile.field_class == expected_class


class TestEnumProfilerCodeAndExclusions:
    """Code 检测 + 特殊排除（年份/月份/金额）。"""

    @pytest.mark.parametrize(
        "values,field_name,expected_tier,expected_class",
        [
            (["1", "2", "3"], "payment_type", EnumConfidenceTier.HIGH, EnumFieldClass.CODE),
            (["PDR", "ADR", "OTH"], "fee_code", EnumConfidenceTier.HIGH, EnumFieldClass.CODE),
            # 年份/月份/金额 → 排除
            (["2020", "2021", "2022"], "fiscal_year", EnumConfidenceTier.NOT_ENUM, None),
            (["1", "6", "12"], "birth_month", EnumConfidenceTier.NOT_ENUM, None),
            (["100", "200", "500"], "item_amount", EnumConfidenceTier.NOT_ENUM, None),
            # 无字段名信号 → LOW
            (["10", "20", "30"], "unknown_field", EnumConfidenceTier.LOW, EnumFieldClass.CODE),
        ],
    )
    def test_code_and_exclusions(self, values, field_name, expected_tier, expected_class):
        sample = ColumnSample(
            table_ref="tf", column_name=field_name,
            normalized_name=field_name, values=values,
        )
        profile = EnumProfiler().profile([sample]).profiles[0]
        assert profile.tier == expected_tier
        assert profile.field_class == expected_class


class TestLabelEnumWithAutoDetect:
    """LabelValidator 集成自动检测——分层行为。"""

    def test_certain_blocks_undeclared(self):
        """CERTAIN 自动检测——值不在检测集中仍 blocking。"""
        from tianshu_datadev.validation.label_validator import validate_label_enums

        spec = _make_spec_with_enums(["A"])
        plan = _make_case_when_plan(["Z"])
        profiles = [
            EnumProfile(
                table_ref="tf", column_name="score", normalized_name="score",
                field_class=EnumFieldClass.FLAG,
                detected_values=["0", "1"], distinct_count=2, total_sampled=100,
                tier=EnumConfidenceTier.CERTAIN, pattern_match_ratio=1.0,
                signals=["field_name:flag"],
            )
        ]
        questions = validate_label_enums(plan, spec=spec, profiles=profiles)
        # Z 不在声明的 [A] 也不在检测的 [0, 1] → blocking
        assert len(questions) == 1
        assert questions[0].blocking is True

    def test_high_tier_warns_non_blocking(self):
        """HIGH 自动检测——值在检测集中但未声明 → WARN。"""
        from tianshu_datadev.validation.label_validator import validate_label_enums

        spec = _make_spec_with_enums(["A"], field_name="status")
        plan = _make_case_when_plan(["Approved"], field_name="status")
        profiles = [
            EnumProfile(
                table_ref="tf", column_name="status", normalized_name="status",
                field_class=EnumFieldClass.STATUS,
                detected_values=["Approved", "Pending"], distinct_count=2, total_sampled=200,
                tier=EnumConfidenceTier.HIGH, pattern_match_ratio=0.95,
                signals=["field_name:status"],
            )
        ]
        questions = validate_label_enums(plan, spec=spec, profiles=profiles)
        warn_qs = [q for q in questions if not q.blocking]
        assert len(warn_qs) >= 1, f"应产生 WARN: {questions}"

    def test_medium_tier_info_only(self):
        """MEDIUM 自动检测 → info 不阻断。"""
        from tianshu_datadev.validation.label_validator import validate_label_enums

        spec = _make_spec_with_enums(["A"], field_name="status")
        plan = _make_case_when_plan(["Approved"], field_name="status")
        profiles = [
            EnumProfile(
                table_ref="tf", column_name="status", normalized_name="status",
                field_class=EnumFieldClass.STATUS,
                detected_values=["Approved", "Pending"], distinct_count=2, total_sampled=200,
                tier=EnumConfidenceTier.MEDIUM, pattern_match_ratio=0.85,
                signals=[],
            )
        ]
        questions = validate_label_enums(plan, spec=spec, profiles=profiles)
        assert all(not q.blocking for q in questions)

    def test_low_tier_skipped_blocks_undeclared(self):
        """LOW 自动检测被跳过 → 值不在声明中 → blocking。"""
        from tianshu_datadev.validation.label_validator import validate_label_enums

        spec = _make_spec_with_enums(["A"], field_name="status")
        plan = _make_case_when_plan(["Approved"], field_name="status")
        profiles = [
            EnumProfile(
                table_ref="tf", column_name="status", normalized_name="status",
                field_class=EnumFieldClass.STATUS,
                detected_values=["Approved", "Pending"], distinct_count=2, total_sampled=200,
                tier=EnumConfidenceTier.LOW, pattern_match_ratio=0.7,
                signals=[],
            )
        ]
        questions = validate_label_enums(plan, spec=spec, profiles=profiles)
        assert len(questions) == 1
        assert questions[0].blocking is True  # LOW → 跳过 → 回退到 blocking
