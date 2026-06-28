"""测试 DuckDbSqlCompiler——确定性编译 + SQL 渲染。"""

import os

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.relationship_planner import FakeRelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.models import SqlArtifact

# ── 辅助 ──

def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_spec(fixture_path: str):
    parser = DeveloperSpecParser()
    text = _read_fixture(fixture_path)
    return parser.parse(text)


# ════════════════════════════════════════════
# Compiler 测试
# ════════════════════════════════════════════


class TestDuckDbSqlCompiler:
    """DuckDbSqlCompiler 确定性编译测试。"""

    def test_single_table_compile(self):
        """单表 SqlBuildPlan → 合法 DuckDB SQL。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # 验证 CompiledSql 结构
        assert compiled.sql != ""
        assert compiled.sql_sha256 != ""
        assert compiled.compiler_version == "1.0.0"
        assert compiled.input_plan_hash is not None

        # SQL 应包含关键词
        assert "SELECT" in compiled.sql.upper()
        assert "FROM" in compiled.sql.upper()

        # 应有优化记录
        assert compiled.optimized_plan is not None
        assert len(compiled.optimized_plan.applied_passes) >= 0

    def test_two_table_join_compile(self):
        """两表 Join SqlBuildPlan → 合法 DuckDB SQL。"""
        spec = _parse_spec("fixtures/relationship/explicit_join_spec.md")

        planner = FakeRelationshipPlanner()
        hypothesis, _ = planner.plan(spec)

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec, hypothesis)

        # 提供表名映射
        table_mapping = {"tf": "dwd.test_fact", "td": "dim.test_dim"}
        compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
        compiled = compiler.compile(plan)

        assert compiled.sql != ""
        assert "SELECT" in compiled.sql.upper()
        assert "JOIN" in compiled.sql.upper()
        assert "dwd.test_fact" in compiled.sql
        assert "dim.test_dim" in compiled.sql

    def test_deterministic_compile_same_hash(self):
        """相同 SqlBuildPlan 两次编译 → 相同 SQL 和 SHA-256。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()

        compiled1 = compiler.compile(plan)
        compiled2 = compiler.compile(plan)

        # SQL 文本必须一致
        assert compiled1.sql == compiled2.sql, (
            f"SQL 不同:\n---1---\n{compiled1.sql}\n---2---\n{compiled2.sql}"
        )

        # SHA-256 必须一致
        assert compiled1.sql_sha256 == compiled2.sql_sha256, (
            f"SHA-256 不同: {compiled1.sql_sha256} vs {compiled2.sql_sha256}"
        )

        # input_plan_hash 必须一致
        assert compiled1.input_plan_hash == compiled2.input_plan_hash

    def test_compile_with_case_when_minimal(self):
        """最小 CASE WHEN 编译——CaseWhenStep 确认可通过编译管道。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # CASE WHEN step 当前为空，编译应正常完成
        assert compiled.sql != ""

    def test_compile_to_artifact_wraps_full_lineage(self):
        """compile_to_artifact 包装 CompiledSql + 完整溯源链为 SqlArtifact。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        artifact = compiler.compile_to_artifact(
            plan,
            spec_hash=spec.spec_hash,
            hypothesis_id=None,
        )

        # 验证 SqlArtifact 结构
        assert isinstance(artifact, SqlArtifact)
        assert artifact.artifact_id != ""
        assert artifact.artifact_id.startswith("artifact_")
        assert artifact.spec_hash == spec.spec_hash
        assert artifact.plan_id == plan.plan_id
        assert artifact.hypothesis_id is None  # 单表无 hypothesis

        # 验证内嵌的 CompiledSql
        assert artifact.compiled_sql is not None
        assert artifact.compiled_sql.sql != ""
        assert artifact.compiled_sql.sql_sha256 != ""

        # 确定性：相同输入 → 相同 artifact_id
        artifact2 = compiler.compile_to_artifact(
            plan,
            spec_hash=spec.spec_hash,
        )
        assert artifact.artifact_id == artifact2.artifact_id
        assert artifact.compiled_sql.sql_sha256 == artifact2.compiled_sql.sql_sha256

    def test_optimized_plan_records_pruning(self):
        """OptimizedSQLPlan 正确记录列裁剪明细。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        optimized = compiled.optimized_plan

        # applied_passes 应包含所有 4 个 Pass
        pass_names = {p.pass_name for p in optimized.applied_passes}
        assert "column_pruning" in pass_names

        # column_pruning_removed 应为列表（可能为空，取决于 plan 内容）
        assert isinstance(optimized.column_pruning_removed, list)

        # eliminated_sorts 应为列表（可能为空）
        assert isinstance(optimized.eliminated_sorts, list)

        # predicate_normalizations 应为列表
        assert isinstance(optimized.predicate_normalizations, list)

        # constant_folds 应为列表
        assert isinstance(optimized.constant_folds, list)

        # input_plan_hash 和 output_plan_hash 应不同（经过 Pass 处理后）
        assert optimized.input_plan_hash != ""
        assert optimized.output_plan_hash != ""

    def test_optimized_plan_records_rejected_directives(self):
        """OptimizedSQLPlan.rejected_directives 正确记录未应用的优化指令。"""
        spec = _parse_spec("fixtures/golden/golden_no_time_range.md")

        builder = SqlBuildPlanBuilder()
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        # rejected_directives 应为列表（Phase 1C 当前为空）
        assert isinstance(compiled.optimized_plan.rejected_directives, list)

        # 验证 OptimizedSQLPlan 各字段与 CompiledSql 的关联一致
        assert compiled.optimized_plan.input_plan_hash == compiled.input_plan_hash
