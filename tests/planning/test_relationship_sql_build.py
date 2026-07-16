"""测试 FakeRelationshipPlanner / RelationshipPlanner + SqlBuildPlanBuilder 集成 + 确定性。"""


from tests._test_utils import read_fixture
from tianshu_datadev.developer_spec.models import (
    FieldSource,
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.cross_validator import cross_validate
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipEvidence,
    RelationshipHypothesis,
)
from tianshu_datadev.planning.relationship_planner import (
    FakeRelationshipPlanner,
    RelationshipPlanner,
)
from tianshu_datadev.planning.sql_build_plan import (
    SqlBuildPlanBuilder,
)

# ── 辅助 ──


# ════════════════════════════════════════════
# 单表计划结构
# ════════════════════════════════════════════


class TestSingleTablePlan:
    """单表 SqlBuildPlan 结构正确性。"""

    def test_single_table_plan_structure(self):
        """单表 golden fixture → Scan → (Aggregate) → Project。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        builder = SqlBuildPlanBuilder()
        plan, questions = builder.build(spec)

        assert len(plan.steps) >= 1
        # 至少有一个 ScanStep
        scan_steps = [s for s in plan.steps if s.step_type == "scan"]
        assert len(scan_steps) == 1
        assert scan_steps[0].table_ref == "tf"

        # 不应有 JoinStep（单表）
        join_steps = [s for s in plan.steps if s.step_type == "join"]
        assert len(join_steps) == 0

        # 有 ProjectStep
        proj_steps = [s for s in plan.steps if s.step_type == "project"]
        assert len(proj_steps) == 1

        # 无 OpenQuestion
        assert len(questions) == 0

    def test_single_table_with_metrics_has_aggregate(self):
        """单表含指标 → 应有 AggregateStep。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        agg_steps = [s for s in plan.steps if s.step_type == "aggregate"]
        assert len(agg_steps) == 1
        # 应该有 group_keys 和 metrics
        agg = agg_steps[0]
        assert len(agg.metrics) > 0


# ════════════════════════════════════════════
# 显式 Join → STRONG JoinCandidate → JoinStep
# ════════════════════════════════════════════


class TestExplicitJoinIntegration:
    """显式 Join 声明完整流程——Planner + Builder 集成。"""

    def test_explicit_join_to_strong_candidate(self):
        """显式 Join fixture → FakeRelationshipPlanner → STRONG JoinCandidate。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec)

        # 应有 1 个候选
        assert len(hypothesis.candidates) == 1
        candidate = hypothesis.candidates[0]

        # 应为 STRONG
        assert candidate.evidence is not None
        assert candidate.evidence.level == JoinEvidenceLevel.STRONG
        assert candidate.left_table == "tf"
        assert candidate.right_table == "td"

        # 证据链 YAML 已生成
        assert candidate.evidence.evidence_chain_yaml != ""
        assert "STRONG" in candidate.evidence.evidence_chain_yaml

    def test_explicit_join_produces_join_step(self):
        """显式 Join → SqlBuildPlan 应包含 JoinStep。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 应有 JoinStep
        join_steps = [s for s in plan.steps if s.step_type == "join"]
        assert len(join_steps) == 1, f"期望 1 个 JoinStep，实际 {len(join_steps)}"
        join_step = join_steps[0]
        assert join_step.right_table_ref == "td"
        assert join_step.relationship_ref == hypothesis.candidates[0].candidate_id

    def test_no_joins_empty_candidates(self):
        """无 Join 声明 → 空 candidates。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_explicit_joins.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec)

        assert len(hypothesis.candidates) == 0
        # 无 Join 声明不应有问题
        assert len(questions) == 0

    def test_multi_table_flag(self):
        """多表 spec → multi_table=True。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        assert hypothesis.multi_table is True

        # 单表应 multi_table=False
        text_single = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec_single = parser.parse(text_single)
        hyp_single, _ = planner.plan(spec_single)
        assert hyp_single.multi_table is False


# ════════════════════════════════════════════
# 确定性
# ════════════════════════════════════════════


class TestFakePlannerDeterminism:
    """Fake Planner/Build 确定性——相同输入 → 相同输出。"""

    def test_planner_determinism(self):
        """相同 spec 两次 plan → 相同 hypothesis_id + 相同候选数。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hyp1, q1 = planner.plan(spec)
        hyp2, q2 = planner.plan(spec)

        assert hyp1.hypothesis_id == hyp2.hypothesis_id
        assert len(hyp1.candidates) == len(hyp2.candidates)
        assert len(q1) == len(q2)

    def test_builder_determinism(self):
        """相同 spec + hypothesis 两次 build → 相同 plan_id + 相同 step 数。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan1, _ = builder.build(spec, hypothesis)
        plan2, _ = builder.build(spec, hypothesis)

        assert plan1.plan_id == plan2.plan_id
        assert len(plan1.steps) == len(plan2.steps)
        for i, (s1, s2) in enumerate(zip(plan1.steps, plan2.steps)):
            assert s1.step_type == s2.step_type, f"Step {i} 类型不一致"

    def test_evidence_chain_yaml_generated(self):
        """JoinCandidate 证据链 YAML 包含完整字段。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        candidate = hypothesis.candidates[0]
        yaml_text = candidate.evidence.evidence_chain_yaml

        # 验证 YAML 包含关键信息
        assert "left_table:" in yaml_text
        assert "right_table:" in yaml_text
        assert "evidence_checks:" in yaml_text
        assert "field_name_match" in yaml_text
        assert "developer_declared" in yaml_text
        assert "STRONG" in yaml_text


# ════════════════════════════════════════════
# RelationshipPlanner LLM 骨架测试（Phase 4E）
# ════════════════════════════════════════════


def _build_test_manifest(
    tables_data: list[dict], spec_hash: str = "test_hash_1234"
) -> SourceManifest:
    """构建测试用的 SourceManifest——基于 tables_data 字典列表。

    Args:
        tables_data: [{"table_ref": "tf", "source_table": "dwd.fact",
                       "columns": [("col", "bigint"), ...]}, ...]
        spec_hash: 关联的 spec hash

    Returns:
        SourceManifest 实例
    """
    tables: list[ManifestTable] = []
    for td in tables_data:
        cols: list[ManifestColumn] = []
        for col_name, col_type in td.get("columns", []):
            cols.append(
                ManifestColumn(
                    column_name=col_name,
                    normalized_name=col_name.lower(),
                    data_type=col_type,
                    nullable=True,
                    source=FieldSource.DEVELOPER_SPEC,
                )
            )
        tables.append(
            ManifestTable(
                table_ref=td["table_ref"],
                source_table=td.get("source_table", f"test.{td['table_ref']}"),
                columns=cols,
            )
        )
    return SourceManifest(
        manifest_id="test_manifest",
        spec_hash=spec_hash,
        tables=tables,
    )


class TestRelationshipPlannerDegradation:
    """退化路径——adapter=None → 完全复刻 Fake 行为。"""

    def test_degradation_explicit_join(self):
        """无 LLM client → 显式 Join 与 Fake 一致。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        fake = FakeRelationshipPlanner()
        rp = RelationshipPlanner(adapter=None)

        hyp_fake, q_fake = fake.plan(spec)
        hyp_rp, q_rp = rp.plan(spec)

        assert hyp_fake.hypothesis_id == hyp_rp.hypothesis_id
        assert len(hyp_fake.candidates) == len(hyp_rp.candidates)
        assert len(q_fake) == len(q_rp)

    def test_degradation_no_joins(self):
        """无 LLM client + 无 Join → 与 Fake 一致。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_explicit_joins.md")
        spec = parser.parse(text)

        fake = FakeRelationshipPlanner()
        rp = RelationshipPlanner(adapter=None)

        hyp_fake, q_fake = fake.plan(spec)
        hyp_rp, q_rp = rp.plan(spec)

        assert len(hyp_rp.candidates) == len(hyp_fake.candidates)
        assert len(q_rp) == len(q_fake)


class TestRelationshipPlannerContextBuilder:
    """_build_context——从 SourceManifest 构建 LLM 输入。"""

    def test_context_contains_table_schemas(self):
        """context 必须包含所有表的结构信息。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        manifest = _build_test_manifest(
            [
                {
                    "table_ref": "tf",
                    "source_table": "dwd.fact",
                    "columns": [("id", "bigint"), ("amount", "decimal"), ("dim_id", "bigint")],
                },
                {
                    "table_ref": "td",
                    "source_table": "dim.dim",
                    "columns": [("dim_id", "bigint"), ("dim_name", "varchar")],
                },
            ],
            spec.spec_hash,
        )

        rp = RelationshipPlanner(adapter=None)
        ctx = rp._build_context(spec, manifest)

        assert "table_schemas" in ctx
        assert len(ctx["table_schemas"]) == 2
        # 验证每个表有 table_ref 和 columns
        tf_schema = next(s for s in ctx["table_schemas"] if s["table_ref"] == "tf")
        assert len(tf_schema["columns"]) == 3
        assert any(c["column_name"] == "dim_id" for c in tf_schema["columns"])

    def test_context_contains_existing_joins(self):
        """context 必须包含程序员的显式声明（H4 保护）。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        manifest = _build_test_manifest(
            [
                {"table_ref": "tf", "columns": [("dim_id", "bigint")]},
                {"table_ref": "td", "columns": [("dim_id", "bigint")]},
            ],
            spec.spec_hash,
        )

        rp = RelationshipPlanner(adapter=None)
        ctx = rp._build_context(spec, manifest)

        assert "existing_joins" in ctx
        assert len(ctx["existing_joins"]) == 1
        assert ctx["existing_joins"][0]["left_key"] == "dim_id"

    def test_context_contains_title_and_description(self):
        """context 必须包含业务描述（LLM 语义推断的依据）。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_explicit_joins.md")
        spec = parser.parse(text)

        manifest = _build_test_manifest(
            [
                {"table_ref": "tf", "columns": [("id", "bigint")]},
                {"table_ref": "td", "columns": [("dim_id", "bigint")]},
            ],
            spec.spec_hash,
        )

        rp = RelationshipPlanner(adapter=None)
        ctx = rp._build_context(spec, manifest)

        assert "business_description" in ctx
        assert "spec_title" in ctx


# ════════════════════════════════════════════
# 交叉验证测试——CV1-CV4 四条规则
# ════════════════════════════════════════════


class TestCrossValidator:
    """交叉验证层——指标推断 vs Join 推断一致性检查。"""

    def _build_hypothesis(self, candidates, spec_hash="test_hash_123456"):
        """构建测试用 RelationshipHypothesis。"""
        return RelationshipHypothesis(
            hypothesis_id=f"hyp_{spec_hash[:12]}",
            spec_hash=spec_hash,
            candidates=candidates,
            multi_table=len(candidates) > 0,
        )

    def _build_candidate(
        self,
        left_table,
        right_table,
        left_key,
        right_key,
        level,
        evidence_checks=None,
    ):
        """构建测试用 JoinCandidate，含 evidence。"""
        from tianshu_datadev.planning.models import JoinType

        candidate = JoinCandidate(
            candidate_id=f"jc_{left_table}_{right_table}",
            left_table=left_table,
            right_table=right_table,
            left_key=left_key,
            right_key=right_key,
            left_key_normalized=left_key.lower(),
            right_key_normalized=right_key.lower(),
            join_type=JoinType.INNER,
        )
        action = "AUTO_ADOPT" if level == JoinEvidenceLevel.STRONG else "HUMAN_CONFIRM"
        if level == JoinEvidenceLevel.WEAK:
            action = "REJECT_BLOCKING"
        evidence = RelationshipEvidence(
            evidence_id=f"ev_{candidate.candidate_id}",
            level=level,
            action=action,
            left_table=left_table,
            right_table=right_table,
            left_key_raw=left_key,
            right_key_raw=right_key,
            left_key_normalized=left_key.lower(),
            right_key_normalized=right_key.lower(),
            evidence_checks=evidence_checks or [],
            detail="Test evidence",
        )
        object.__setattr__(candidate, "evidence", evidence)
        return candidate

    def _make_spec_with_metrics(self, metrics, grain=None):
        """解析 fixture 并覆盖指标和粒度，返回 (spec, manifest)。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)
        # 覆盖指标
        spec.metrics = metrics
        # 覆盖粒度
        if grain is not None:
            spec.output_spec.grain = grain
        # 移除显式 Join 声明——交叉验证主要针对 LLM 推断的 Join
        spec.joins = None
        # 构建 manifest
        manifest = _build_test_manifest(
            [
                {"table_ref": "tf", "columns": [
                    ("id", "bigint"), ("dim_id", "bigint"), ("amount", "decimal"),
                    ("stat_date", "date"),
                ]},
                {"table_ref": "td", "columns": [
                    ("dim_id", "bigint"), ("dim_name", "varchar"),
                    ("stat_date", "date"), ("status", "varchar"),
                ]},
            ],
            spec.spec_hash,
        )
        return spec, manifest

    # ── CV1: 列可达性 ──

    def test_cv1_all_columns_reachable_passes(self):
        """所有列在可达表中 → 无问题。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics([
            MetricDecl(
                metric_name="total_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="total_amount",
            ),
        ])
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.STRONG)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv1_qs = [q for q in questions if "CV1" in q.description]
        assert len(cv1_qs) == 0

    def test_cv1_column_in_unreachable_table_with_weak_join(self):
        """列在非事实表中，JOIN 仅为 WEAK → 不可达 → CV1 问题。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics([
            MetricDecl(
                metric_name="test_metric", aggregation=AggregationType.SUM,
                input_column="dim_name", alias="test_metric",  # dim_name 仅在 td 表中
            ),
        ])
        # WEAK JOIN → td 不可达
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.WEAK)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv1_qs = [q for q in questions if "CV1" in q.description]
        assert len(cv1_qs) > 0, f"期望 CV1 检测到 dim_name 不可达，实际: {questions}"
        assert not cv1_qs[0].blocking  # CV1 为 non-blocking

    # ── CV2: Join 必要性 ──

    def test_cv2_unnecessary_join_flagged(self):
        """JOIN 右表无列被指标使用 → CV2 冗余标记。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics([
            MetricDecl(
                metric_name="total_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="total_amount",  # amount 仅在 tf 表中
            ),
        ])
        # JOIN 连接 td 但 td 的列未被使用
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.MEDIUM)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv2_qs = [q for q in questions if "CV2" in q.description]
        assert len(cv2_qs) > 0, f"期望 CV2 标记冗余 JOIN，实际: {questions}"
        assert not cv2_qs[0].blocking  # CV2 为 non-blocking

    def test_cv2_explicit_join_not_flagged(self):
        """程序员显式声明的 JOIN 不标记为冗余（CV2 豁免）。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics([
            MetricDecl(
                metric_name="total_amount", aggregation=AggregationType.SUM,
                input_column="amount", alias="total_amount",
            ),
        ])
        # 显式声明的 JOIN（evidence_checks 含 "developer_declared: FOUND"）
        candidate = self._build_candidate(
            "tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.STRONG,
            evidence_checks=["developer_declared: FOUND"],
        )
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv2_qs = [q for q in questions if "CV2" in q.description]
        assert len(cv2_qs) == 0, f"显式声明 JOIN 不应被 CV2 标记，实际: {cv2_qs}"

    # ── CV3: 跨表列歧义 ──

    def test_cv3_same_column_with_join_no_ambiguity(self):
        """同名列在两表中且有确认 JOIN → 无歧义（CV3 不触发）。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics([
            MetricDecl(
                metric_name="status_count", aggregation=AggregationType.COUNT,
                input_column="status", alias="status_count",  # status 在两表中都出现
            ),
        ])
        # STRONG JOIN → tf↔td 连通，status 在两可达表中都有，但 JOIN 已确认 → 无歧义
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.STRONG)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv3_qs = [q for q in questions if "CV3" in q.description]
        assert len(cv3_qs) == 0, f"确认 JOIN 应消除歧义，实际: {cv3_qs}"

    # ── CV4: 粒度一致性 ──

    def test_cv4_grain_column_reachable_passes(self):
        """grain 列在可达表中 → 无 CV4 问题。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics(
            [
                MetricDecl(
                    metric_name="total", aggregation=AggregationType.SUM,
                    input_column="amount", alias="total",
                ),
            ],
            grain=["stat_date"],  # stat_date 在 tf 表中
        )
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.STRONG)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv4_qs = [q for q in questions if "CV4" in q.description]
        assert len(cv4_qs) == 0

    def test_cv4_grain_column_unreachable_blocking(self):
        """grain 列仅存在于不可达表 → CV4 blocking。"""
        from tianshu_datadev.developer_spec.models import AggregationType, MetricDecl

        spec, manifest = self._make_spec_with_metrics(
            [
                MetricDecl(
                    metric_name="total", aggregation=AggregationType.SUM,
                    input_column="amount", alias="total",
                ),
            ],
            grain=["dim_name"],  # dim_name 仅在 td 表中
        )
        # WEAK JOIN → td 不可达
        candidate = self._build_candidate("tf", "td", "dim_id", "dim_id", JoinEvidenceLevel.WEAK)
        hypothesis = self._build_hypothesis([candidate], spec.spec_hash)

        questions = cross_validate(spec, hypothesis, manifest)
        cv4_qs = [q for q in questions if "CV4" in q.description]
        assert len(cv4_qs) > 0, f"期望 CV4 检测到 dim_name 不可达，实际: {questions}"
        assert cv4_qs[0].blocking  # CV4 为 blocking

    # ── 边界条件 ──

    def test_null_hypothesis_returns_empty(self):
        """hypothesis=None → 跳过所有检查，返回空列表。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)
        manifest = _build_test_manifest(
            [{"table_ref": "tf", "columns": [("id", "bigint")]}],
            spec.spec_hash,
        )
        questions = cross_validate(spec, None, manifest)
        assert questions == []

    def test_empty_candidates_returns_empty(self):
        """无 JOIN 候选 → 跳过检查，返回空列表。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)
        manifest = _build_test_manifest(
            [{"table_ref": "tf", "columns": [("id", "bigint")]}],
            spec.spec_hash,
        )
        hypothesis = self._build_hypothesis([], spec.spec_hash)
        questions = cross_validate(spec, hypothesis, manifest)
        assert questions == []


class TestRelationshipPlannerResponseParser:
    """_parse_llm_response——LLM JSON 解析 + H1 字段名校验 + 容错。"""

    def _basic_manifest(self) -> SourceManifest:
        """构建两个标准表的 manifest——列各不相同。"""
        return _build_test_manifest(
            [
                {
                    "table_ref": "tf",
                    "columns": [("id", "bigint"), ("order_id", "bigint"), ("amount", "decimal")],
                },
                {
                    "table_ref": "td",
                    "columns": [("id", "bigint"), ("order_id", "bigint"), ("name", "varchar")],
                },
            ]
        )

    def test_parse_valid_high_confidence(self):
        """合法 JSON + 列名存在 + high confidence → 保留。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf",
                    "right_table": "td",
                    "left_key": "order_id",
                    "right_key": "order_id",
                    "join_type": "INNER",
                    "confidence": "high",
                    "reasoning": "两表都有 order_id 字段",
                }
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 1
        assert result[0]["left_key"] == "order_id"
        assert result[0]["confidence"] == "high"
        assert result[0]["join_type"] == "INNER"

    def test_parse_multiple_candidates(self):
        """多个合法候选 → 全部保留。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf", "right_table": "td",
                    "left_key": "order_id", "right_key": "order_id",
                    "join_type": "INNER", "confidence": "high", "reasoning": "同名列",
                },
                {
                    "left_table": "tf", "right_table": "td",
                    "left_key": "id", "right_key": "id",
                    "join_type": "LEFT", "confidence": "medium", "reasoning": "同名列",
                },
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 2

    def test_parse_invalid_field_name_discarded(self):
        """H1 违反——字段名不在 manifest 中 → 丢弃该候选。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf",
                    "right_table": "td",
                    "left_key": "order_id",
                    "right_key": "nonexistent_field",  # 不存在于 td
                    "join_type": "INNER",
                    "confidence": "high",
                    "reasoning": "猜测",
                }
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 0

    def test_parse_invalid_table_alias_discarded(self):
        """表别名无效 → 丢弃。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf",
                    "right_table": "nonexistent_table",
                    "left_key": "order_id",
                    "right_key": "order_id",
                    "join_type": "INNER",
                    "confidence": "high",
                    "reasoning": "猜测",
                }
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 0

    def test_parse_empty_inferred_joins(self):
        """空数组 → 0 候选，不抛异常。"""
        rp = RelationshipPlanner(adapter=None)
        result = rp._parse_llm_response({"inferred_joins": []}, self._basic_manifest())
        assert len(result) == 0
        assert isinstance(result, list)

    def test_parse_missing_inferred_joins_key(self):
        """缺少 inferred_joins 键 → 0 候选。"""
        rp = RelationshipPlanner(adapter=None)
        result = rp._parse_llm_response({}, self._basic_manifest())
        assert len(result) == 0

    def test_parse_self_join_rejected(self):
        """同一表自 Join → 丢弃（留给 Builder 自引用检测处理）。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf", "right_table": "tf",
                    "left_key": "id", "right_key": "order_id",
                    "join_type": "INNER", "confidence": "low", "reasoning": "自引用",
                }
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 0

    def test_parse_invalid_confidence_defaults_to_medium(self):
        """非法 confidence → 容错默认 medium。"""
        rp = RelationshipPlanner(adapter=None)
        raw = {
            "inferred_joins": [
                {
                    "left_table": "tf", "right_table": "td",
                    "left_key": "order_id", "right_key": "order_id",
                    "join_type": "INNER",
                    "confidence": "certain",  # 非法值
                    "reasoning": "猜测",
                }
            ]
        }
        result = rp._parse_llm_response(raw, self._basic_manifest())
        assert len(result) == 1
        assert result[0]["confidence"] == "medium"
