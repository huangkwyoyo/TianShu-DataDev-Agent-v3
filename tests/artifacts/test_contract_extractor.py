"""测试 DataTransformContractExtractor——确定性抽取 DataTransformContract-lite。

覆盖：
- 单表 SqlBuildPlan → Contract 字段完整
- 两表 Join → Contract.join_relationships 含证据链
- 相同 plan → 相同 contract + 相同 hash
- Contract 不包含 SQL 代码字段
"""

import os

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.relationship_planner import FakeRelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

# ── 辅助 ──


def _read_fixture(path: str) -> str:
    """读取测试 fixture 文件。"""
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_spec(fixture_path: str):
    """解析 fixture 文件为 ParsedDeveloperSpec。"""
    parser = DeveloperSpecParser()
    text = _read_fixture(fixture_path)
    return parser.parse(text)


# ════════════════════════════════════════════
# Contract 抽取测试
# ════════════════════════════════════════════


class TestContractExtractorSingleTable:
    """单表 SqlBuildPlan → DataTransformContract-lite 抽取。"""

    def test_extract_from_single_table_plan(self):
        """单表 plan 抽取——Contract 字段完整。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        extractor = DataTransformContractExtractor()
        contract = extractor.extract(plan)

        # 基本字段
        assert contract.version == "lite"
        assert contract.source_phase == "phase-2"
        assert contract.source_sqlbuildplan_hash != ""

        # 输入表
        assert len(contract.input_tables) >= 1
        table_refs = {t.table_ref for t in contract.input_tables}
        assert "tf" in table_refs  # golden fixture 的表别名

        # 输入列
        assert len(contract.input_columns) > 0

        # 聚合（golden_no_time_range 有指标声明）
        if spec.metrics:
            assert len(contract.aggregations) > 0
            assert len(contract.grouping_keys) > 0

        # 输出列
        assert len(contract.output_columns) > 0

        # contract_id 格式
        assert contract.contract_id.startswith("dtc_lite_")

    def test_deterministic_same_hash(self):
        """相同 plan → 相同 contract + 相同 hash。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        extractor = DataTransformContractExtractor()
        contract1 = extractor.extract(plan)
        contract2 = extractor.extract(plan)

        # Contract 字段内容一致
        assert contract1.source_sqlbuildplan_hash == contract2.source_sqlbuildplan_hash
        assert contract1.input_tables == contract2.input_tables
        assert contract1.input_columns == contract2.input_columns
        assert contract1.aggregations == contract2.aggregations
        assert contract1.grouping_keys == contract2.grouping_keys
        assert contract1.output_columns == contract2.output_columns

        # Hash 一致
        h1 = DataTransformContractExtractor.compute_contract_hash(contract1)
        h2 = DataTransformContractExtractor.compute_contract_hash(contract2)
        assert h1 == h2

    def test_no_sql_code_in_contract(self):
        """Contract 不包含 SQL 代码字段。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")
        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        extractor = DataTransformContractExtractor()
        contract = extractor.extract(plan)

        data = contract.model_dump()
        # 确认不包含任何 SQL 代码字段
        assert "sql" not in data
        assert "raw_sql" not in data
        assert "sql_text" not in data
        assert "compiled_sql" not in data
        # 确认是 lite 版本
        assert data["version"] == "lite"
        assert data["source_phase"] == "phase-2"


class TestContractExtractorJoin:
    """两表 Join → DataTransformContract-lite 抽取。"""

    def test_extract_from_join_plan(self):
        """两表 Join plan 抽取——Contract.join_relationships 含证据链。"""
        spec = _parse_spec("fixtures/relationship/explicit_join_spec.md")

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 构建 evidence_map
        evidence_map = {}
        for candidate in hypothesis.candidates:
            if candidate.evidence:
                evidence_map[candidate.candidate_id] = candidate.evidence

        extractor = DataTransformContractExtractor()
        contract = extractor.extract(plan, evidence_map)

        # Join 关系
        assert len(contract.join_relationships) >= 1
        join_rel = contract.join_relationships[0]

        # 基本字段
        assert join_rel.join_id != ""
        assert join_rel.left_table != ""
        assert join_rel.right_table != ""
        assert join_rel.left_key != ""
        assert join_rel.right_key != ""
        assert join_rel.join_type in ("INNER", "LEFT", "RIGHT", "FULL")

        # 证据链
        if join_rel.evidence_chain:
            assert "level" in join_rel.evidence_chain
            # 证据等级应在 STRONG 或 MEDIUM（WEAK/NONE 不进 Contract）
            assert join_rel.level in ("STRONG", "MEDIUM")

    def test_join_contract_deterministic(self):
        """带 Join 的 plan——抽取确定性。"""
        spec = _parse_spec("fixtures/relationship/explicit_join_spec.md")

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        evidence_map = {}
        for candidate in hypothesis.candidates:
            if candidate.evidence:
                evidence_map[candidate.candidate_id] = candidate.evidence

        extractor = DataTransformContractExtractor()
        contract1 = extractor.extract(plan, evidence_map)
        contract2 = extractor.extract(plan, evidence_map)

        # Hash 一致
        h1 = DataTransformContractExtractor.compute_contract_hash(contract1)
        h2 = DataTransformContractExtractor.compute_contract_hash(contract2)
        assert h1 == h2

        # join_relationships 一致
        assert len(contract1.join_relationships) == len(contract2.join_relationships)
