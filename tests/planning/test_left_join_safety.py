"""LEFT JOIN 右表唯一性安全门禁测试。

覆盖：
- 右表 primary_key 覆盖 join key → 不阻断
- 右表 unique_keys 覆盖 join key → 不阻断
- 无唯一性证据 → blocking OpenQuestion
- unique_keys 不覆盖 join key → blocking OpenQuestion
- INNER JOIN 不触发该门禁
- SQL compiler 不出现去重子查询包裹
"""

import os

from tianshu_datadev.developer_spec.models import (
    FieldSource,
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import ColumnRef, JoinType
from tianshu_datadev.planning.relationship_hypothesis import JoinEvidenceLevel
from tianshu_datadev.planning.relationship_planner import (
    FakeRelationshipPlanner,
    RelationshipPlanner,
)
from tianshu_datadev.planning.relationship_validator import RelationshipValidator
from tianshu_datadev.planning.sql_build_plan import (
    JoinStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

# ── 辅助 ──


def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _make_manifest(
    table_ref: str,
    source_table: str = "",
    unique_keys: list[list[str]] | None = None,
    primary_key: list[str] | None = None,
) -> SourceManifest:
    """构建含 unique_keys 的最小 SourceManifest。"""
    table = ManifestTable(
        table_ref=table_ref,
        source_table=source_table,  # type: ignore[arg-type]
        columns=[
            ManifestColumn(
                column_name="id",
                normalized_name="id",
                data_type="bigint",
                nullable=False,
                source=FieldSource.DEVELOPER_SPEC,
            ),
            ManifestColumn(
                column_name="name",
                normalized_name="name",
                data_type="varchar",
                nullable=True,
                source=FieldSource.DEVELOPER_SPEC,
            ),
        ],
        primary_key=primary_key,
        unique_keys=unique_keys,
    )
    return SourceManifest(
        manifest_id="test_manifest_001",
        spec_hash="abc123def456",
        tables=[table],
    )


# ════════════════════════════════════════════
# RelationshipValidator.check_left_join_safety 单元测试
# ════════════════════════════════════════════


class TestCheckLeftJoinSafety:
    """check_left_join_safety() 方法单元测试。"""

    def test_null_unique_keys_unsafe(self):
        """unique_keys=None → unsafe。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=None,
            right_join_key="user_id",
        )
        assert is_safe is False
        assert desc is not None
        assert "无唯一性保证" in desc

    def test_empty_unique_keys_unsafe(self):
        """unique_keys=[] → unsafe。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[],
            right_join_key="user_id",
        )
        assert is_safe is False

    def test_exact_single_key_match_safe(self):
        """单列唯一键精确匹配 → safe。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[["user_id"]],
            right_join_key="user_id",
        )
        assert is_safe is True
        assert desc is None

    def test_case_insensitive_match(self):
        """大小写不敏感匹配 → safe。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[["User_ID"]],
            right_join_key="user_id",
        )
        assert is_safe is True

    def test_composite_key_not_supported_phase1(self):
        """Phase 1 不支持复合键——单列 join key 不匹配复合唯一键。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[["user_id", "order_date"]],
            right_join_key="user_id",
        )
        # 复合键中 len != 1，不匹配
        assert is_safe is False

    def test_no_coverage_unsafe(self):
        """有 unique_keys 但不覆盖当前联结键 → unsafe。"""
        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[["order_id"], ["product_id"]],
            right_join_key="user_id",
        )
        assert is_safe is False
        assert desc is not None
        assert "user_id" in desc


# ════════════════════════════════════════════
# FakeRelationshipPlanner 集成测试
# ════════════════════════════════════════════


class TestLeftJoinSafetyGate:
    """LEFT JOIN 安全门禁集成——FakeRelationshipPlanner + Manifest。"""

    def test_left_join_with_primary_key_passes(self):
        """右表 primary_key 覆盖 join key → 不阻断。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        # 修改 joins 为 LEFT JOIN
        from tianshu_datadev.developer_spec.models import JoinTypeEnum
        spec.joins[0].join_type = JoinTypeEnum("LEFT")

        # 构建 manifest：右表 td 的 primary_key 覆盖 join key dim_id
        manifest = _make_manifest(
            table_ref="td",
            source_table="dim.test_dim",
            primary_key=["dim_id"],
            unique_keys=[["dim_id"]],
        )
        # 左表 tf 也需要在 manifest 中
        tf_table = ManifestTable(
            table_ref="tf",
            source_table="dwd.test_fact",  # type: ignore[arg-type]
            columns=[
                ManifestColumn(
                    column_name="id",
                    normalized_name="id",
                    data_type="bigint",
                    nullable=False,
                    source=FieldSource.DEVELOPER_SPEC,
                ),
            ],
        )
        manifest.tables.append(tf_table)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec, manifest)

        # 不应有安全门禁阻断
        safety_questions = [q for q in questions if "Q-JOIN-SAFETY" in q.question_id]
        assert len(safety_questions) == 0, f"不应阻断，但生成了 {safety_questions}"
        # 候选应通过
        assert len(hypothesis.candidates) == 1

    def test_left_join_with_unique_keys_passes(self):
        """右表 unique_keys（非 primary_key）覆盖 join key → 不阻断。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        from tianshu_datadev.developer_spec.models import JoinTypeEnum
        spec.joins[0].join_type = JoinTypeEnum("LEFT")

        # 构建 manifest：primary_key 为空，但 unique_keys 包含 dim_id
        manifest = _make_manifest(
            table_ref="td",
            source_table="dim.test_dim",
            unique_keys=[["dim_id"]],
        )
        tf_table = ManifestTable(
            table_ref="tf",
            source_table="dwd.test_fact",  # type: ignore[arg-type]
            columns=[
                ManifestColumn(
                    column_name="id",
                    normalized_name="id",
                    data_type="bigint",
                    nullable=False,
                    source=FieldSource.DEVELOPER_SPEC,
                ),
            ],
        )
        manifest.tables.append(tf_table)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec, manifest)

        safety_questions = [q for q in questions if "Q-JOIN-SAFETY" in q.question_id]
        assert len(safety_questions) == 0
        assert len(hypothesis.candidates) == 1

    def test_left_join_no_unique_keys_blocks(self):
        """无 manifest（无 unique_keys）→ blocking OpenQuestion。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        from tianshu_datadev.developer_spec.models import JoinTypeEnum
        spec.joins[0].join_type = JoinTypeEnum("LEFT")

        # 不传 manifest → 无 unique_keys 信息
        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec, manifest=None)

        # 应有安全门禁阻断
        safety_questions = [q for q in questions if "Q-JOIN-SAFETY" in q.question_id]
        assert len(safety_questions) == 1, f"应有 1 个安全阻断，实际 {len(safety_questions)}"
        assert safety_questions[0].blocking is True
        assert "无唯一性保证" in safety_questions[0].description
        # 候选不应加入（被阻断）
        assert len(hypothesis.candidates) == 0

    def test_left_join_unique_keys_no_coverage_blocks(self):
        """有 unique_keys 但不覆盖联结键 → blocking OpenQuestion。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        from tianshu_datadev.developer_spec.models import JoinTypeEnum
        spec.joins[0].join_type = JoinTypeEnum("LEFT")

        # unique_keys 只有 order_id，不包含联结键 dim_id
        manifest = _make_manifest(
            table_ref="td",
            source_table="dim.test_dim",
            unique_keys=[["order_id"]],
        )
        tf_table = ManifestTable(
            table_ref="tf",
            source_table="dwd.test_fact",  # type: ignore[arg-type]
            columns=[
                ManifestColumn(
                    column_name="id",
                    normalized_name="id",
                    data_type="bigint",
                    nullable=False,
                    source=FieldSource.DEVELOPER_SPEC,
                ),
            ],
        )
        manifest.tables.append(tf_table)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec, manifest)

        safety_questions = [q for q in questions if "Q-JOIN-SAFETY" in q.question_id]
        assert len(safety_questions) == 1
        assert safety_questions[0].blocking is True
        assert "不被任何唯一键覆盖" in safety_questions[0].description

    def test_inner_join_skips_safety_gate(self):
        """INNER JOIN 不触发 LEFT JOIN 安全门禁。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        # 保持 INNER JOIN（fixture 默认）
        from tianshu_datadev.developer_spec.models import JoinTypeEnum
        spec.joins[0].join_type = JoinTypeEnum("INNER")

        # 不传 manifest
        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec, manifest=None)

        # 不应有安全门禁阻断（INNER JOIN 不触发）
        safety_questions = [q for q in questions if "Q-JOIN-SAFETY" in q.question_id]
        assert len(safety_questions) == 0
        # 候选应通过
        assert len(hypothesis.candidates) == 1
        assert hypothesis.candidates[0].evidence is not None
        assert hypothesis.candidates[0].evidence.level == JoinEvidenceLevel.STRONG


# ════════════════════════════════════════════
# RelationshipPlanner 委托路径——验证 _fake 桥接
# ════════════════════════════════════════════


class TestRelationshipPlannerDelegateGate:
    """验证 RelationshipPlanner(adapter=None) 通过 _fake 正确委托安全门禁。

    这类测试防止 _build_unique_keys_lookup / _check_left_join_safety_gate
    被错误地以 self._method() 而非 self._fake._method() 调用的回归。
    """

    def test_delegate_build_unique_keys_lookup(self):
        """_fake._build_unique_keys_lookup 可从 RelationshipPlanner 访问。"""
        manifest = _make_manifest(
            table_ref="t1",
            source_table="test.t1",
            primary_key=["id"],
            unique_keys=[["id"], ["code"]],
        )
        t2 = ManifestTable(
            table_ref="t2",
            source_table="test.t2",  # type: ignore[arg-type]
            columns=[
                ManifestColumn(
                    column_name="name",
                    normalized_name="name",
                    data_type="varchar",
                    nullable=True,
                    source=FieldSource.DEVELOPER_SPEC,
                ),
            ],
        )
        manifest.tables.append(t2)

        planner = RelationshipPlanner(adapter=None)
        lookup = planner._fake._build_unique_keys_lookup(manifest)

        assert "t1" in lookup
        # _build_unique_keys_lookup 返回的是 unique_keys 列表本身（分组），不做合并
        assert ["id"] in lookup["t1"]
        assert ["code"] in lookup["t1"]
        # 未声明 unique_keys 的表不会出现在 lookup 中
        assert "t2" not in lookup

    def test_delegate_check_left_join_safety_blocks(self):
        """_fake._check_left_join_safety_gate 返回 blocking OpenQuestion（无覆盖）。"""
        planner = RelationshipPlanner(adapter=None)

        from tianshu_datadev.planning.relationship_hypothesis import (
            JoinCandidate,
            JoinEvidenceLevel,
            RelationshipEvidence,
        )

        candidate = JoinCandidate(
            candidate_id="jc_test_delegate",
            left_table="tf",
            right_table="td",
            left_key="dim_id",
            right_key="dim_id",
            left_key_normalized="dim_id",
            right_key_normalized="dim_id",
            join_type=JoinType.LEFT,
            evidence=RelationshipEvidence(
                evidence_id="ev_test",
                level=JoinEvidenceLevel.STRONG,
                action="AUTO_ADOPT",
                left_table="tf",
                right_table="td",
                left_key_raw="dim_id",
                right_key_raw="dim_id",
                left_key_normalized="dim_id",
                right_key_normalized="dim_id",
                evidence_checks=[],
                detail="test",
            ),
        )
        candidate.evidence.generate_evidence_chain_yaml()

        # 右表无 unique_keys → 应阻断
        table_unique_keys = {"td": []}
        question = planner._fake._check_left_join_safety_gate(candidate, table_unique_keys)
        assert question is not None
        assert question.blocking is True
        assert "Q-JOIN-SAFETY" in question.question_id
        assert "无唯一性保证" in question.description

    def test_delegate_check_left_join_safety_passes(self):
        """_fake._check_left_join_safety_gate 返回 None（有覆盖）。"""
        planner = RelationshipPlanner(adapter=None)

        from tianshu_datadev.planning.relationship_hypothesis import (
            JoinCandidate,
            JoinEvidenceLevel,
            RelationshipEvidence,
        )

        candidate = JoinCandidate(
            candidate_id="jc_test_delegate_safe",
            left_table="tf",
            right_table="td",
            left_key="dim_id",
            right_key="dim_id",
            left_key_normalized="dim_id",
            right_key_normalized="dim_id",
            join_type=JoinType.LEFT,
            evidence=RelationshipEvidence(
                evidence_id="ev_test_safe",
                level=JoinEvidenceLevel.STRONG,
                action="AUTO_ADOPT",
                left_table="tf",
                right_table="td",
                left_key_raw="dim_id",
                right_key_raw="dim_id",
                left_key_normalized="dim_id",
                right_key_normalized="dim_id",
                evidence_checks=[],
                detail="test",
            ),
        )
        candidate.evidence.generate_evidence_chain_yaml()

        # 右表 unique_keys 覆盖 dim_id → 应通过
        table_unique_keys = {"td": [["dim_id"]]}
        question = planner._fake._check_left_join_safety_gate(candidate, table_unique_keys)
        assert question is None

    def test_delegate_inner_join_skips(self):
        """_fake._check_left_join_safety_gate 对 INNER JOIN 返回 None。"""
        planner = RelationshipPlanner(adapter=None)

        from tianshu_datadev.planning.relationship_hypothesis import (
            JoinCandidate,
            JoinEvidenceLevel,
            RelationshipEvidence,
        )

        candidate = JoinCandidate(
            candidate_id="jc_test_delegate_inner",
            left_table="tf",
            right_table="td",
            left_key="dim_id",
            right_key="dim_id",
            left_key_normalized="dim_id",
            right_key_normalized="dim_id",
            join_type=JoinType.INNER,
            evidence=RelationshipEvidence(
                evidence_id="ev_test_inner",
                level=JoinEvidenceLevel.STRONG,
                action="AUTO_ADOPT",
                left_table="tf",
                right_table="td",
                left_key_raw="dim_id",
                right_key_raw="dim_id",
                left_key_normalized="dim_id",
                right_key_normalized="dim_id",
                evidence_checks=[],
                detail="test",
            ),
        )
        candidate.evidence.generate_evidence_chain_yaml()

        # INNER JOIN 不触发门禁
        table_unique_keys = {"td": []}
        question = planner._fake._check_left_join_safety_gate(candidate, table_unique_keys)
        assert question is None


# ════════════════════════════════════════════
# SQL Compiler——确认无静默去重包裹
# ════════════════════════════════════════════


class TestSqlCompilerNoSilentDedup:
    """验证 SQL compiler 不会静默为 LEFT JOIN 加去重子查询。"""

    def test_left_join_no_distinct_subquery(self):
        """LEFT JOIN 渲染不应包含 SELECT DISTINCT 或去重子查询包裹。"""
        plan = SqlBuildPlan(
            plan_id="test_no_dedup",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dim_id", normalized_name="dim_id"),
                    ],
                ),
                ScanStep(
                    step_id="scan_td",
                    table_ref="td",
                    required_columns=[
                        ColumnRef(table_ref="td", column_name="dim_id", normalized_name="dim_id"),
                    ],
                ),
                JoinStep(
                    step_id="join_1",
                    right_table_ref="td",
                    relationship_ref="jc_test123",
                    join_type=JoinType.LEFT,
                    join_keys=[
                        (
                            ColumnRef(table_ref="tf", column_name="dim_id", normalized_name="dim_id"),
                            ColumnRef(table_ref="td", column_name="dim_id", normalized_name="dim_id"),
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        sql = compiler.compile(plan)

        # 不应出现去重子查询包裹
        assert "DISTINCT" not in sql.sql.upper(), (
            f"SQL 不应包含 DISTINCT，实际输出:\n{sql.sql}"
        )

    def test_left_join_is_direct(self):
        """LEFT JOIN 渲染应该是直接的 LEFT JOIN ... ON ... 格式。"""
        plan = SqlBuildPlan(
            plan_id="test_direct_join",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_tf",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(table_ref="tf", column_name="dim_id", normalized_name="dim_id"),
                    ],
                ),
                ScanStep(
                    step_id="scan_td",
                    table_ref="td",
                    required_columns=[
                        ColumnRef(table_ref="td", column_name="dim_id", normalized_name="dim_id"),
                    ],
                ),
                JoinStep(
                    step_id="join_1",
                    right_table_ref="td",
                    relationship_ref="jc_test456",
                    join_type=JoinType.LEFT,
                    join_keys=[
                        (
                            ColumnRef(table_ref="tf", column_name="dim_id", normalized_name="dim_id"),
                            ColumnRef(table_ref="td", column_name="dim_id", normalized_name="dim_id"),
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()
        sql = compiler.compile(plan)

        # 应有 LEFT JOIN 关键字
        assert "LEFT JOIN" in sql.sql.upper(), (
            f"SQL 应包含 LEFT JOIN，实际输出:\n{sql.sql}"
        )
