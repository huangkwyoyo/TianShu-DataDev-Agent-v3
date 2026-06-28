"""测试 FakeRelationshipPlanner + SqlBuildPlanBuilder 集成 + 确定性。"""

import os

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinEvidenceLevel,
)
from tianshu_datadev.planning.relationship_planner import FakeRelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import (
    SqlBuildPlanBuilder,
)

# ── 辅助 ──


def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


# ════════════════════════════════════════════
# 单表计划结构
# ════════════════════════════════════════════


class TestSingleTablePlan:
    """单表 SqlBuildPlan 结构正确性。"""

    def test_single_table_plan_structure(self):
        """单表 golden fixture → Scan → (Aggregate) → Project。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_no_time_range.md")
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
        text = _read_fixture("fixtures/golden/golden_no_time_range.md")
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
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
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
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
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
        text = _read_fixture("fixtures/golden/golden_no_explicit_joins.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, questions = planner.plan(spec)

        assert len(hypothesis.candidates) == 0
        # 无 Join 声明不应有问题
        assert len(questions) == 0

    def test_multi_table_flag(self):
        """多表 spec → multi_table=True。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
        spec = parser.parse(text)

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        assert hypothesis.multi_table is True

        # 单表应 multi_table=False
        text_single = _read_fixture("fixtures/golden/golden_no_time_range.md")
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
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
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
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
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
        text = _read_fixture("fixtures/relationship/explicit_join_spec.md")
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
