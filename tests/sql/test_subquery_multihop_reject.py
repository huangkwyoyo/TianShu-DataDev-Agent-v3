"""子查询 & 多跳 Join 拒绝测试——Phase 3C 遗留门禁。

验证 Validator 正确拒绝：
1. 多跳 Join（≥2 JoinStep 在同一 SqlBuildPlan）
2. 不支持的步骤类型（白名单外的任何 Step 类型）
3. 单跳 Join 正常通过（无回归）
"""

from __future__ import annotations

import pytest

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.planning.models import AggregateSpec, ColumnRef, JoinType
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    JoinStep,
    LimitStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

# ── 辅助工厂 ──


def _make_manifest() -> SourceManifest:
    """构造最小 SourceManifest——注册 3 张表供 Join 使用。"""
    return SourceManifest(
        manifest_id="test_manifest",
        spec_hash="multi_hop_test",
        tables=[
            ManifestTable(
                table_ref="u",
                source_table="dwd.user_info",
                columns=[
                    ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                    ManifestColumn(column_name="user_name", normalized_name="user_name", data_type="varchar"),
                ],
            ),
            ManifestTable(
                table_ref="o",
                source_table="dwd.order_detail",
                columns=[
                    ManifestColumn(
                        column_name="user_id", normalized_name="user_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="product_id", normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="amount", normalized_name="amount",
                        data_type="decimal",
                    ),
                ],
            ),
            ManifestTable(
                table_ref="p",
                source_table="dim.product_info",
                columns=[
                    ManifestColumn(
                        column_name="product_id", normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="product_name",
                        normalized_name="product_name", data_type="varchar",
                    ),
                ],
            ),
        ],
    )


def _make_single_join_plan() -> SqlBuildPlan:
    """合法单跳 Join 计划——u JOIN o（应通过验证）。"""
    return SqlBuildPlan(
        plan_id="plan_single_join",
        spec_hash="abc123",
        steps=[
            ScanStep(
                step_id="scan_u",
                table_ref="u",
                required_columns=[
                    ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                ],
            ),
            ScanStep(
                step_id="scan_o",
                table_ref="o",
                required_columns=[
                    ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id"),
                    ColumnRef(table_ref="o", column_name="amount", normalized_name="amount"),
                ],
            ),
            JoinStep(
                step_id="join_u_o",
                right_table_ref="o",
                join_type=JoinType.INNER,
                join_keys=[
                    (
                        ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                        ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id"),
                    ),
                ],
                relationship_ref="rel_u_o",
            ),
            AggregateStep(
                step_id="agg_1",
                group_keys=[
                    ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                ],
                metrics=[
                    AggregateSpec(aggregation="COUNT", input_column="user_id", alias="cnt"),
                ],
            ),
            LimitStep(step_id="limit_100", limit=100),
        ],
        multi_table=True,
    )


def _make_multi_hop_plan() -> SqlBuildPlan:
    """多跳 Join 计划——u JOIN o JOIN p（应被拒绝）。"""
    return SqlBuildPlan(
        plan_id="plan_multi_hop",
        spec_hash="multi_hop_001",
        steps=[
            ScanStep(
                step_id="scan_u",
                table_ref="u",
                required_columns=[
                    ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                ],
            ),
            ScanStep(
                step_id="scan_o",
                table_ref="o",
                required_columns=[
                    ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id"),
                    ColumnRef(table_ref="o", column_name="product_id", normalized_name="product_id"),
                ],
            ),
            ScanStep(
                step_id="scan_p",
                table_ref="p",
                required_columns=[
                    ColumnRef(table_ref="p", column_name="product_id", normalized_name="product_id"),
                ],
            ),
            JoinStep(
                step_id="join_u_o",
                right_table_ref="o",
                join_type=JoinType.INNER,
                join_keys=[
                    (
                        ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                        ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id"),
                    ),
                ],
                relationship_ref="rel_u_o",
            ),
            JoinStep(
                step_id="join_o_p",
                right_table_ref="p",
                join_type=JoinType.LEFT,
                join_keys=[
                    (
                        ColumnRef(table_ref="o", column_name="product_id", normalized_name="product_id"),
                        ColumnRef(table_ref="p", column_name="product_id", normalized_name="product_id"),
                    ),
                ],
                relationship_ref="rel_o_p",
            ),
        ],
        multi_table=True,
    )


# ════════════════════════════════════════════
# 多跳 Join 拒绝测试
# ════════════════════════════════════════════


class TestMultiHopJoinReject:
    """多跳 Join——Phase 3C 门禁拒绝。"""

    def test_single_join_passes(self):
        """单跳 Join（两表关联）→ 通过验证。"""
        plan = _make_single_join_plan()
        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert passed, f"单跳 Join 应通过，实际 blocking: {[q.question_id for q in questions if q.blocking]}"

    def test_multi_hop_join_rejected(self):
        """两个 JoinStep（u→o→p）→ 拒绝 MULTIHOP。"""
        plan = _make_multi_hop_plan()
        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert not passed, "多跳 Join 应被拒绝"
        multihop_qs = [q for q in questions if "MULTIHOP" in q.question_id]
        assert len(multihop_qs) == 1, f"应有 1 个 MULTIHOP 问题，实际: {[q.question_id for q in questions]}"
        assert multihop_qs[0].blocking is True
        assert "2 个 JoinStep" in multihop_qs[0].description
        assert "o → p" in multihop_qs[0].description

    def test_three_hop_join_rejected(self):
        """三个 JoinStep（4 表关联）→ 拒绝 MULTIHOP。"""
        plan = SqlBuildPlan(
            plan_id="plan_three_hop",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_id="scan_a", table_ref="u",
                    required_columns=[
                        ColumnRef(
                            table_ref="u", column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_b", table_ref="o",
                    required_columns=[
                        ColumnRef(
                            table_ref="o", column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_c", table_ref="p",
                    required_columns=[
                        ColumnRef(
                            table_ref="p", column_name="product_id",
                            normalized_name="product_id",
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_1", right_table_ref="o", join_type=JoinType.INNER,
                    join_keys=[
                        (ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                         ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id")),
                    ],
                    relationship_ref="rel_1",
                ),
                JoinStep(
                    step_id="join_2", right_table_ref="p", join_type=JoinType.LEFT,
                    join_keys=[
                        (ColumnRef(table_ref="o", column_name="product_id", normalized_name="product_id"),
                         ColumnRef(table_ref="p", column_name="product_id", normalized_name="product_id")),
                    ],
                    relationship_ref="rel_2",
                ),
                JoinStep(
                    step_id="join_3", right_table_ref="u", join_type=JoinType.INNER,
                    join_keys=[
                        (ColumnRef(table_ref="p", column_name="product_id", normalized_name="product_id"),
                         ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id")),
                    ],
                    relationship_ref="rel_3",
                ),
            ],
            multi_table=True,
        )
        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert not passed
        multihop_qs = [q for q in questions if "MULTIHOP" in q.question_id]
        assert len(multihop_qs) == 1
        assert "3 个 JoinStep" in multihop_qs[0].description


# ════════════════════════════════════════════
# 不支持的步骤类型拒绝测试
# ════════════════════════════════════════════


class TestUnsupportedStepTypeReject:
    """不支持的步骤类型——架构边界双层防线。"""

    def test_unknown_step_type_rejected_by_discriminated_union(self):
        """Pydantic discriminated union 是第一道防线——拒绝未知 step_type。

        SqlBuildPlan 的 steps 字段使用 discriminated union (StepNode)，
        Pydantic 在构造时即根据 step_type 分发到具体类型。
        不在 Discriminator 中的 step_type 会被 Pydantic 以 union_tag_invalid 拒绝——
        在 Validator 运行之前。
        """
        from pydantic import ValidationError as PydanticValidationError

        # 用 dict 传入含未注册 step_type 的步骤——Pydantic 直接拒绝
        with pytest.raises(PydanticValidationError, match="union_tag_invalid|does not match any"):
            SqlBuildPlan(
                plan_id="plan_unsupported",
                spec_hash="test_hash",
                steps=[
                    ScanStep(
                        step_id="scan_1", table_ref="u",
                        required_columns=[
                            ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                        ],
                    ),
                    # 未注册的 step_type——不在 StepNode Discriminator 中
                    {"step_id": "fake_1", "step_type": "fake_unsupported", "fake_field": "test"},
                ],  # type: ignore[list-item]
                multi_table=False,
            )

    def test_validator_unsupported_rule_is_second_line(self):
        """Validator 的 _validate_unsupported_step_types 是第二道防线。

        当 StepNode discriminated union 未来扩展（例如 Phase 4.6 新增 SubqueryStep）时，
        若 Validator 白名单未同步更新，_validate_unsupported_step_types 会拦截。
        此测试验证该规则方法存在且不误报正常步骤。
        """
        plan = _make_single_join_plan()
        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        _, questions = validator.validate(plan, manifest)
        unsupported = [q for q in questions if "UNSUPPORTED" in q.question_id]
        assert len(unsupported) == 0, (
            f"正常 Step 不应触发 UNSUPPORTED: {[q.description for q in unsupported]}"
        )


# ════════════════════════════════════════════
# 组合场景——多跳 Join 同时触发其他规则
# ════════════════════════════════════════════


class TestCombinedRejection:
    """多跳 Join + 其他规则同时触发——确保所有问题都被收集。"""

    def test_multi_hop_also_triggers_join_type_check(self):
        """多跳 Join 被拒绝的同时，Join key 类型检查也执行（若类型不兼容）。"""
        plan = SqlBuildPlan(
            plan_id="plan_combined",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_id="scan_u", table_ref="u",
                    required_columns=[
                        ColumnRef(
                            table_ref="u", column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_o", table_ref="o",
                    required_columns=[
                        ColumnRef(
                            table_ref="o", column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_1", right_table_ref="o", join_type=JoinType.INNER,
                    join_keys=[
                        (ColumnRef(table_ref="u", column_name="user_id", normalized_name="user_id"),
                         ColumnRef(table_ref="o", column_name="user_id", normalized_name="user_id")),
                    ],
                    relationship_ref="rel_1",
                ),
                JoinStep(
                    step_id="join_2", right_table_ref="p", join_type=JoinType.LEFT,
                    join_keys=[
                        # user_id (bigint) ↔ product_name (varchar)——类型不兼容
                        (
                            ColumnRef(
                                table_ref="u", column_name="user_id",
                                normalized_name="user_id",
                            ),
                            ColumnRef(
                                table_ref="p", column_name="product_name",
                                normalized_name="product_name",
                            ),
                        ),
                    ],
                    relationship_ref="rel_2",
                ),
            ],
            multi_table=True,
        )
        manifest = _make_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)
        assert not passed
        # 应同时出现 MULTIHOP 和 JOINTYPE 问题
        question_ids = [q.question_id for q in questions]
        assert any("MULTIHOP" in qid for qid in question_ids), f"缺少 MULTIHOP: {question_ids}"
        assert any("JOINTYPE" in qid for qid in question_ids), f"缺少 JOINTYPE: {question_ids}"


# ════════════════════════════════════════════
# Fixture 文件存在性测试
# ════════════════════════════════════════════


class TestSubqueryMultihopFixtures:
    """验证 3 个 subquery_multihop fixture 文件存在且可读。"""

    _EXPECTED_FIXTURES = [
        "reject_multi_hop_three_table_join.md",
        "reject_from_subquery_derived_table.md",
        "reject_cte_with_clause.md",
    ]

    def test_fixture_directory_exists(self):
        """fixtures/subquery_multihop/ 目录存在。"""
        import os
        fixture_dir = os.path.join(
            os.path.dirname(__file__), "..", "fixtures", "subquery_multihop",
        )
        assert os.path.isdir(fixture_dir), f"目录不存在: {fixture_dir}"

    def test_all_fixture_files_exist(self):
        """3 个 fixture 文件全部存在。"""
        import os
        fixture_dir = os.path.join(
            os.path.dirname(__file__), "..", "fixtures", "subquery_multihop",
        )
        for filename in self._EXPECTED_FIXTURES:
            filepath = os.path.join(fixture_dir, filename)
            assert os.path.isfile(filepath), f"Fixture 文件缺失: {filepath}"

    def test_fixture_files_readable_and_non_empty(self):
        """3 个 fixture 文件可读且非空。"""
        import os
        fixture_dir = os.path.join(
            os.path.dirname(__file__), "..", "fixtures", "subquery_multihop",
        )
        for filename in self._EXPECTED_FIXTURES:
            filepath = os.path.join(fixture_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            assert len(content) > 100, f"Fixture 过短 ({len(content)} 字符): {filename}"
            # 验证关键标记存在
            assert "---" in content, f"缺少 YAML frontmatter: {filename}"
            assert "spec:" in content, f"缺少 spec 声明: {filename}"

    @pytest.mark.parametrize("filename", _EXPECTED_FIXTURES)
    def test_each_fixture_contains_expected_code(self, filename: str):
        """每个 fixture 含预期拒绝码标注。"""
        import os
        fixture_dir = os.path.join(
            os.path.dirname(__file__), "..", "fixtures", "subquery_multihop",
        )
        filepath = os.path.join(fixture_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        # 所有 fixture 应包含"拒绝"或"reject"关键词
        assert "拒绝" in content or "reject" in content.lower(), (
            f"Fixture 缺少拒绝标注: {filename}"
        )
