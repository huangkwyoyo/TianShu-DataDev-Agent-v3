"""测试 SqlProgram 模型、DAG 校验与拓扑排序。

覆盖：
- 两步聚合 DAG、多表串联 DAG、扇出扇入 DAG
- 直接/间接循环依赖拒绝
- 缺失依赖引用拒绝
- 拓扑排序与 Kahn 结果一致性
"""

import os
from functools import lru_cache

import pytest

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import ColumnRef
from tianshu_datadev.planning.sql_build_plan import ScanStep, SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
    topological_sort,
    validate_program_dag,
)
from tianshu_datadev.planning.temp_table import TempTableSpec

# ── 辅助函数 ──


@lru_cache(maxsize=1)
def _cached_base_plan() -> SqlBuildPlan:
    """缓存基础 SqlBuildPlan——避免每次测试都重新解析 fixture。"""
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "golden", "golden_no_time_range.md"
    )
    with open(fixture_path, "r", encoding="utf-8") as f:
        spec_text = f.read()
    parser = DeveloperSpecParser()
    spec = parser.parse(spec_text)
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)
    return plan


def _make_statement(
    statement_id: str,
    kind: StatementKind = StatementKind.STANDALONE,
    depends_on: list[str] | None = None,
    produces: str | None = None,
) -> SqlStatement:
    """创建 SqlStatement——基于缓存的 SqlBuildPlan 克隆。"""
    base = _cached_base_plan()
    # 深层克隆 plan 并设置 statement_id 作为 plan_id——避免共享 steps 列表
    plan = base.model_copy(deep=True, update={"plan_id": statement_id})
    return SqlStatement(
        statement_id=statement_id,
        plan=plan,
        kind=kind,
        depends_on=depends_on or [],
        produces=produces,
    )


def _make_temp_table(
    temp_id: str,
    produced_by: str,
    consumed_by: list[str],
) -> TempTableSpec:
    """创建 TempTableSpec 辅助函数。"""
    return TempTableSpec(
        temp_id=temp_id,
        produced_by=produced_by,
        consumed_by=consumed_by,
        column_defs=[],
    )


def _make_statement_with_temp_refs(
    statement_id: str,
    temp_refs: list[str],
    kind: StatementKind = StatementKind.CONSUMER,
    depends_on: list[str] | None = None,
    produces: str | None = None,
) -> SqlStatement:
    """创建包含 _temp_* 表引用的 SqlStatement——用于消费者授权测试。

    在缓存的 base plan 基础上，注入对指定 _temp_* 表的 ScanStep 引用。
    这模拟了 CONSUMER/FINAL 语句实际读取上游 _temp 表的场景。

    Args:
        statement_id: 语句 ID
        temp_refs: 要注入的 _temp_* 表引用列表（如 ["_temp_a"]）
        kind: 语句类型
        depends_on: 依赖列表
        produces: 产生的 _temp 表名

    Returns:
        SqlStatement——其 plan.steps 包含对 _temp_* 表的引用
    """
    base = _cached_base_plan()
    plan = base.model_copy(deep=True, update={"plan_id": statement_id})
    # 在 plan.steps 头部插入 _temp_* 表的 ScanStep
    for temp_ref in temp_refs:
        fake_scan = ScanStep(
            step_id=f"{statement_id}_scan_{temp_ref}",
            table_ref=temp_ref,
            required_columns=[
                ColumnRef(
                    table_ref=temp_ref,
                    column_name="zone",
                    normalized_name="zone",
                )
            ],
        )
        plan.steps.insert(0, fake_scan)
    return SqlStatement(
        statement_id=statement_id,
        plan=plan,
        kind=kind,
        depends_on=depends_on or [],
        produces=produces,
    )


# ════════════════════════════════════════════
# 拓扑排序测试
# ════════════════════════════════════════════


class TestTopologicalSort:
    """Kahn 算法拓扑排序测试。"""

    def test_single_node_no_deps(self):
        """单节点无依赖——排序结果为该节点。"""
        stmt = _make_statement("stmt_A")
        result = topological_sort([stmt])
        assert result == ["stmt_A"]

    def test_linear_chain_three_nodes(self):
        """A→B→C 线性链——排序按依赖顺序。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        c = _make_statement("stmt_C", kind=StatementKind.FINAL, depends_on=["stmt_B"])
        result = topological_sort([a, b, c])
        assert result == ["stmt_A", "stmt_B", "stmt_C"]

    def test_fan_out(self):
        """A 扇出到 B、C——B 和 C 同级按字典序排列。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        c = _make_statement("stmt_C", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        result = topological_sort([a, b, c])
        assert result[0] == "stmt_A"
        # B 和 C 在同级，字典序 B < C
        assert result[1] == "stmt_B"
        assert result[2] == "stmt_C"

    def test_fan_in(self):
        """A、B 扇入到 C——C 依赖 A 和 B。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.PRODUCER, produces="_temp_b")
        c = _make_statement("stmt_C", kind=StatementKind.FINAL, depends_on=["stmt_A", "stmt_B"])
        result = topological_sort([c, a, b])
        assert result[0] == "stmt_A"
        assert result[1] == "stmt_B"
        assert result[2] == "stmt_C"

    def test_diamond_dag(self):
        """菱形 DAG：A→B→D, A→C→D。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        c = _make_statement("stmt_C", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        d = _make_statement("stmt_D", kind=StatementKind.FINAL, depends_on=["stmt_B", "stmt_C"])
        result = topological_sort([a, b, c, d])
        assert result[0] == "stmt_A"
        assert result[1] == "stmt_B"
        assert result[2] == "stmt_C"
        assert result[3] == "stmt_D"

    def test_deterministic_result(self):
        """相同 DAG 两次拓扑排序——结果必须一致。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        c = _make_statement("stmt_C", kind=StatementKind.CONSUMER, depends_on=["stmt_A"])
        d = _make_statement("stmt_D", kind=StatementKind.FINAL, depends_on=["stmt_B", "stmt_C"])

        result1 = topological_sort([a, b, c, d])
        result2 = topological_sort([a, b, c, d])
        assert result1 == result2


class TestCircularDependencyRejection:
    """循环依赖拒绝测试。"""

    def test_direct_cycle_rejected(self):
        """A→B→A 直接循环——应抛出 CIRCULAR_DEPENDENCY。"""
        a = _make_statement("stmt_A", depends_on=["stmt_B"])
        b = _make_statement("stmt_B", depends_on=["stmt_A"])
        with pytest.raises(ValueError, match="CIRCULAR_DEPENDENCY"):
            topological_sort([a, b])

    def test_indirect_cycle_rejected(self):
        """A→B→C→A 间接循环——应抛出 CIRCULAR_DEPENDENCY。"""
        a = _make_statement("stmt_A", depends_on=["stmt_C"])
        b = _make_statement("stmt_B", depends_on=["stmt_A"])
        c = _make_statement("stmt_C", depends_on=["stmt_B"])
        with pytest.raises(ValueError, match="CIRCULAR_DEPENDENCY"):
            topological_sort([a, b, c])

    def test_self_loop_rejected(self):
        """自己依赖自己——应抛出 CIRCULAR_DEPENDENCY。"""
        a = _make_statement("stmt_A", depends_on=["stmt_A"])
        with pytest.raises(ValueError, match="CIRCULAR_DEPENDENCY"):
            topological_sort([a])


# ════════════════════════════════════════════
# DAG 校验测试
# ════════════════════════════════════════════


class TestValidateProgramDag:
    """SqlProgram DAG 校验测试。"""

    def test_valid_dag_passes(self):
        """合法 DAG 通过校验——PRODUCER→FINAL，含 temp_tables 声明。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.FINAL, depends_on=["stmt_A"])
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=[
                _make_temp_table("_temp_a", "stmt_A", ["stmt_B"]),
            ],
            topological_order=["stmt_A", "stmt_B"],
            final_output="stmt_B",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) == 0

    def test_missing_dependency_reported(self):
        """depends_on 引用不存在的 statement_id——报告 MISSING_DEPENDENCY。"""
        a = _make_statement("stmt_A")
        b = _make_statement("stmt_B", depends_on=["stmt_nonexistent"])
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            topological_order=["stmt_A", "stmt_B"],
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) >= 1
        assert any("MISSING_DEPENDENCY" in q.description for q in blocking)

    def test_circular_dag_reported(self):
        """循环 DAG 校验应报告 CIRCULAR_DEPENDENCY。"""
        a = _make_statement("stmt_A", depends_on=["stmt_B"])
        b = _make_statement("stmt_B", depends_on=["stmt_A"])
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            topological_order=["stmt_A", "stmt_B"],
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) >= 1
        assert any("CIRCULAR_DEPENDENCY" in q.description for q in blocking)

    def test_unmatched_topological_order_reported(self):
        """topological_order 与实际 Kahn 结果不一致——应报告。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.FINAL, depends_on=["stmt_A"])
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=[
                _make_temp_table("_temp_a", "stmt_A", ["stmt_B"]),
            ],
            topological_order=["stmt_B", "stmt_A"],  # 故意写反
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert any("topological_order" in q.description.lower() for q in blocking)

    def test_final_output_missing_reported(self):
        """final_output 引用不存在的 statement_id——应报告。"""
        a = _make_statement("stmt_A")
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a],
            topological_order=["stmt_A"],
            final_output="stmt_nonexistent",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) >= 1
        assert any("final_output" in q.description for q in blocking)

    def test_undeclared_temp_table_reported(self):
        """produces 的 _temp 表未在 temp_tables 中声明——应报告。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.FINAL, depends_on=["stmt_A"])
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=[],  # 没有声明 _temp_a
            topological_order=["stmt_A", "stmt_B"],
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert any("未在 temp_tables 中声明" in q.description for q in blocking)

    def test_empty_statements_reported(self):
        """statements 为空——应报告阻塞问题。"""
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[],
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) >= 1

    def test_producer_mismatch_reported(self):
        """statement.produces 与 TempTableSpec.produced_by 不一致——应报告。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement("stmt_B", kind=StatementKind.FINAL, depends_on=["stmt_A"])
        temp_tables = [
            TempTableSpec(
                temp_id="_temp_a",
                produced_by="stmt_B",  # 声明 stmt_B 是生产者，但实际是 stmt_A
                consumed_by=[],
                column_defs=[],
            )
        ]
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=temp_tables,
            topological_order=["stmt_A", "stmt_B"],
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert any("produced_by" in q.description.lower() for q in blocking)

    def test_unauthorized_consumer_rejected(self):
        """非声明消费者引用 _temp 表——应报告阻塞问题。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement_with_temp_refs(
            "stmt_B", temp_refs=["_temp_a"],
            kind=StatementKind.CONSUMER, depends_on=["stmt_A"],
        )
        # stmt_C 引用了 _temp_a，但 consumed_by 只有 stmt_B
        c = _make_statement_with_temp_refs(
            "stmt_C", temp_refs=["_temp_a"],
            kind=StatementKind.FINAL, depends_on=["stmt_B"],
        )
        temp_tables = [_make_temp_table("_temp_a", "stmt_A", ["stmt_B"])]
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b, c],
            temp_tables=temp_tables,
            topological_order=topological_sort([a, b, c]),
            final_output="stmt_C",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert any("unauthorized_consumer" in q.question_id for q in blocking), (
            f"应拦截非声明消费者，实际 blocking={len(blocking)}: "
            f"{[q.question_id for q in blocking]}"
        )

    def test_declared_consumer_with_temp_ref_passes(self):
        """声明消费者引用 _temp 表——应通过校验。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement_with_temp_refs(
            "stmt_B", temp_refs=["_temp_a"],
            kind=StatementKind.CONSUMER, depends_on=["stmt_A"],
        )
        temp_tables = [_make_temp_table("_temp_a", "stmt_A", ["stmt_B"])]
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=temp_tables,
            topological_order=topological_sort([a, b]),
            final_output="stmt_B",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) == 0, (
            f"声明消费者应通过，实际 blocking={len(blocking)}: "
            f"{[q.description for q in blocking]}"
        )

    def test_producer_reads_own_temp_passes(self):
        """生产者引用自己产生的 _temp 表——应通过校验。"""
        a = _make_statement_with_temp_refs(
            "stmt_A", temp_refs=["_temp_a"],
            kind=StatementKind.PRODUCER, produces="_temp_a",
        )
        temp_tables = [_make_temp_table("_temp_a", "stmt_A", [])]
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a],
            temp_tables=temp_tables,
            topological_order=topological_sort([a]),
            final_output="stmt_A",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) == 0, (
            f"生产者自引用应通过，实际 blocking={len(blocking)}: "
            f"{[q.description for q in blocking]}"
        )

    def test_consumer_without_producer_dep_rejected(self):
        """消费者被 consumed_by 授权但 depends_on 不含生产者——应报告阻塞问题。

        验证：仅 consumed_by 授权不足以保证执行顺序，
        必须通过 depends_on 链确保 producer 先于 consumer。
        """
        a = _make_statement(
            "zzz_producer", kind=StatementKind.PRODUCER, produces="_temp_a"
        )
        b = _make_statement_with_temp_refs(
            "aaa_consumer", temp_refs=["_temp_a"],
            kind=StatementKind.CONSUMER,
            depends_on=[],  # ← 没有依赖 zzz_producer
        )
        temp_tables = [
            _make_temp_table("_temp_a", "zzz_producer", ["aaa_consumer"])
        ]
        # 手动指定 topological_order 以绕过 topological_sort 的字典序
        # （两个入度为 0 的节点按字典序排列，consumer 会排在 producer 前面）
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b],
            temp_tables=temp_tables,
            topological_order=topological_sort([a, b]),
            final_output="aaa_consumer",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert any(
            "missing_producer_dep" in q.question_id for q in blocking
        ), (
            f"应拦截缺少生产者依赖的消费者，实际 blocking={len(blocking)}: "
            f"{[q.question_id for q in blocking]}"
        )

    def test_consumer_with_indirect_producer_dep_passes(self):
        """消费者通过中间节点间接依赖生产者——应通过校验。

        DAG: zzz_producer → stmt_B → aaa_consumer
        aaa_consumer 读 _temp_a，通过 stmt_B 间接依赖 zzz_producer。
        验证可达性校验允许传递路径，不强制直接依赖。
        """
        a = _make_statement(
            "zzz_producer", kind=StatementKind.PRODUCER, produces="_temp_a"
        )
        b = _make_statement(
            "stmt_B", kind=StatementKind.CONSUMER, depends_on=["zzz_producer"]
        )
        c = _make_statement_with_temp_refs(
            "aaa_consumer", temp_refs=["_temp_a"],
            kind=StatementKind.FINAL,
            depends_on=["stmt_B"],  # 间接依赖——通过 stmt_B → zzz_producer
        )
        temp_tables = [
            _make_temp_table("_temp_a", "zzz_producer", ["aaa_consumer"])
        ]
        program = SqlProgram(
            program_id="program_test",
            spec_id="test_spec",
            statements=[a, b, c],
            temp_tables=temp_tables,
            topological_order=topological_sort([a, b, c]),
            final_output="aaa_consumer",
        )
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        assert len(blocking) == 0, (
            f"间接依赖应通过校验，实际 blocking={len(blocking)}: "
            f"{[q.description for q in blocking]}"
        )


class TestSqlProgramBuilder:
    """SqlProgramBuilder 构建测试。"""

    def test_build_two_step_aggregation(self):
        """两步聚合：PRODUCER → FINAL。"""
        a = _make_statement("stmt_agg", kind=StatementKind.PRODUCER, produces="_temp_agg")
        b = _make_statement("stmt_output", kind=StatementKind.FINAL, depends_on=["stmt_agg"])
        temp_tables = [_make_temp_table("_temp_agg", "stmt_agg", ["stmt_output"])]
        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[a, b],
            temp_tables=temp_tables,
            spec_hash="test_hash",
            final_output="stmt_output",
        )
        assert program.program_id == "program_test_hash"
        assert program.topological_order == ["stmt_agg", "stmt_output"]
        assert program.final_output == "stmt_output"

    def test_build_multi_table_chaining(self):
        """多表串联 DAG：A→B→C。"""
        a = _make_statement("stmt_A", kind=StatementKind.PRODUCER, produces="_temp_a")
        b = _make_statement(
            "stmt_B", kind=StatementKind.CONSUMER,
            depends_on=["stmt_A"], produces="_temp_b"
        )
        c = _make_statement("stmt_C", kind=StatementKind.FINAL, depends_on=["stmt_B"])
        temp_tables = [
            _make_temp_table("_temp_a", "stmt_A", ["stmt_B"]),
            _make_temp_table("_temp_b", "stmt_B", ["stmt_C"]),
        ]
        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[a, b, c],
            temp_tables=temp_tables,
            spec_hash="chain_hash",
            final_output="stmt_C",
        )
        assert program.topological_order == ["stmt_A", "stmt_B", "stmt_C"]
        assert program.final_output == "stmt_C"
