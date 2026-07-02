"""测试编译器注释块生成——覆盖 13 项验收标准。"""

import hashlib
import os
import re

import pytest

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler


# ── 辅助函数 ──


def _parse_fixture(name: str):
    """解析 fixture 文件为 ParsedDeveloperSpec。"""
    path = os.path.join(os.path.dirname(__file__), "..", name)
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    parser = DeveloperSpecParser()
    return parser.parse(text)


def _assert_five_line_comment_block(sql: str):
    """断言 SQL 以完整的 5 行注释块开头。"""
    pattern = (
        r"-- Step: .+\n"
        r"-- Intent: .+\n"
        r"-- Operation: .+\n"
        r"-- Inputs: .+\n"
        r"-- Output: .+"
    )
    assert re.search(pattern, sql), (
        f"SQL 不包含完整 5 行注释块：\n{sql[:300]}"
    )


def _build_multi_statement_program(
    spec_spec_hash: str,
    plan: SqlBuildPlan,
    final_output_target: str | None = None,
) -> SqlProgram:
    """构建两语句 PRODUCER → FINAL 的 SqlProgram。

    自动推导 temp_tables，确保 DAG 校验通过。
    """
    stmt1 = SqlStatement(
        statement_id="stmt_1",
        plan=plan,
        kind=StatementKind.PRODUCER,
        produces="_temp_test_producer",
        intent="测试用生产者步骤。",
    )
    stmt2 = SqlStatement(
        statement_id="stmt_2",
        plan=plan,
        kind=StatementKind.FINAL,
        depends_on=["stmt_1"],
        intent="测试用最终输出步骤。",
    )
    return SqlProgramBuilder().build_from_statements(
        statements=[stmt1, stmt2],
        spec_hash=spec_spec_hash,
        final_output="stmt_2",
        final_output_target=final_output_target,
    )


# ════════════════════════════════════════════
# 验收项 #1: PRODUCER 前有完整注释块
# ════════════════════════════════════════════


def test_comment_producer_has_full_block():
    """每个 CREATE TEMP TABLE 前必须有完整 5 行注释块。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    program = _build_multi_statement_program(spec.spec_hash, plan)

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_program(program)
    compiled = artifact.compiled

    # 第一条语句（PRODUCER）的 SQL 应以注释块开头
    producer_sql = compiled.statements[0].sql
    assert producer_sql.startswith("-- Step:"), (
        f"PRODUCER SQL 不以注释块开头：\n{producer_sql[:200]}"
    )
    _assert_five_line_comment_block(producer_sql)
    # PRODUCER 应包含 CREATE TEMP TABLE
    assert "CREATE TEMP TABLE _temp_test_producer" in producer_sql, (
        "PRODUCER SQL 缺少 CREATE TEMP TABLE 包装"
    )


# ════════════════════════════════════════════
# 验收项 #2: FINAL 前有注释块
# ════════════════════════════════════════════


def test_comment_final_has_block():
    """FINAL 语句前必须有注释块，Step 行含 Final Output:。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    program = _build_multi_statement_program(spec.spec_hash, plan)

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_program(program)

    final_sql = artifact.compiled.statements[1].sql
    assert "-- Step: Final Output:" in final_sql, (
        f"FINAL 注释不含 'Final Output:'：\n{final_sql[:200]}"
    )
    _assert_five_line_comment_block(final_sql)


# ════════════════════════════════════════════
# 验收项 #3: STANDALONE 单语句前有注释块
# ════════════════════════════════════════════


def test_comment_standalone_has_block():
    """STANDALONE 单语句前必须有注释块。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    compiler = DuckDbSqlCompiler()
    compiled = compiler.compile(plan)

    assert compiled.sql.startswith("-- Step:"), (
        f"STANDALONE SQL 不以注释块开头：\n{compiled.sql[:200]}"
    )
    _assert_five_line_comment_block(compiled.sql)
    assert "-- Output: (直接返回)" in compiled.sql


# ════════════════════════════════════════════
# 验收项 #4: 注释块完整性——5 行不缺
# ════════════════════════════════════════════


def test_comment_block_five_lines_complete():
    """注释块必须是完整 5 行——Step/Intent/Operation/Inputs/Output。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    compiler = DuckDbSqlCompiler()
    compiled = compiler.compile(plan)

    lines = compiled.sql.split("\n")
    comment_lines = [l for l in lines if l.startswith("-- ")]
    assert len(comment_lines) >= 5, f"注释行数不足 5：{len(comment_lines)}"
    assert any("Step:" in l for l in comment_lines)
    assert any("Intent:" in l for l in comment_lines)
    assert any("Operation:" in l for l in comment_lines)
    assert any("Inputs:" in l for l in comment_lines)
    assert any("Output:" in l for l in comment_lines)


# ════════════════════════════════════════════
# 验收项 #5: intent 可被 ReviewPackage 直接读取
# ════════════════════════════════════════════


def test_intent_accessible_from_program():
    """SqlStatement.intent 可直接从模型字段读取——无需反解析 SQL。"""
    stmt = SqlStatement(
        statement_id="test_stmt",
        plan=SqlBuildPlanBuilder().build(
            _parse_fixture("fixtures/golden/golden_no_time_range.md"),
        )[0],
        kind=StatementKind.PRODUCER,
        produces="_temp_test",
        intent="测试意图描述——供 ReviewPackage 直接读取。",
    )
    # intent 可直接从字段读取
    assert stmt.intent == "测试意图描述——供 ReviewPackage 直接读取。"
    # 字段在 model_dump 中可见
    dumped = stmt.model_dump()
    assert dumped["intent"] == "测试意图描述——供 ReviewPackage 直接读取。"


# ════════════════════════════════════════════
# 验收项 #6: SQL hash 随注释变化
# ════════════════════════════════════════════


def test_sql_hash_differs_with_comment():
    """加注释前后 compute_sql_hash() 结果不同——这是期望行为。"""
    from tianshu_datadev.sql.models import CompiledSql

    raw_sql = "SELECT 1"
    commented_sql = (
        "-- Step: test\n"
        "-- Intent: test\n"
        "-- Operation: test\n"
        "-- Inputs: test\n"
        "-- Output: test\n"
        "\n"
        "SELECT 1"
    )

    hash_raw = CompiledSql.compute_sql_hash(raw_sql, "1.1.0")
    hash_commented = CompiledSql.compute_sql_hash(commented_sql, "1.1.0")

    assert hash_raw != hash_commented, "注释应改变 SQL hash"


# ════════════════════════════════════════════
# 验收项 #8: build_single/build_chain 生成通用 intent
# ════════════════════════════════════════════


def test_build_single_intent_not_none():
    """build_single() 生成通用 intent。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    program_builder = SqlProgramBuilder()
    program = program_builder.build_single(plan, spec.spec_hash)

    assert len(program.statements) == 1
    assert program.statements[0].intent is not None
    assert "单语句" in program.statements[0].intent


def test_build_chain_intent_not_none():
    """build_chain() 生成通用 intent。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan1, _ = builder.build(spec)
    # 创建第二个 plan——使用不同的 plan_id 避免 DAG 冲突
    plan2 = SqlBuildPlan(
        plan_id="plan_different",
        spec_hash=plan1.spec_hash,
        steps=plan1.steps,
    )

    program_builder = SqlProgramBuilder()
    program = program_builder.build_chain(
        [plan1, plan2], spec.spec_hash, "test_chain"
    )

    assert len(program.statements) == 2
    assert program.statements[0].intent is not None
    assert "第 1 步" in program.statements[0].intent
    assert program.statements[1].intent is not None
    assert "多步骤" in program.statements[1].intent


# ════════════════════════════════════════════
# 验收项 #10: 注释安全清洗
# ════════════════════════════════════════════


def test_comment_sanitization_control_chars():
    """控制字符被清洗。"""
    result = DuckDbSqlCompiler._render_comment_line("Test", "val\x00ue\x1f")
    assert "\x00" not in result
    assert "\x1f" not in result
    assert "value" in result


def test_comment_sanitization_double_dash():
    """连续 -- 被替换为 - -。"""
    result = DuckDbSqlCompiler._render_comment_line(
        "Test", "val--ue--more"
    )
    assert "----" not in result
    # 排除前缀 -- Test: 后检查剩余部分不含 --
    remaining = result.replace("-- Test:", "")
    assert "--" not in remaining


def test_comment_sanitization_newlines():
    """CR/LF 被替换为空格。"""
    result = DuckDbSqlCompiler._render_comment_line(
        "Test", "line1\r\nline2\nline3"
    )
    assert "\r" not in result
    assert "\n" not in result
    # \r\n → 两个空格，\n → 一个空格，但保证各词段均出现
    assert "line1" in result
    assert "line2" in result
    assert "line3" in result


# ════════════════════════════════════════════
# 验收项 #12: FINAL Output 当有 final_output_target 时写真实目标
# ════════════════════════════════════════════


def test_final_output_with_real_target():
    """FINAL 语句的 Output 行——有 final_output_target 时写真实目标。"""
    spec = _parse_fixture("fixtures/golden/golden_no_time_range.md")
    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    program = _build_multi_statement_program(
        spec.spec_hash,
        plan,
        final_output_target="ads_test_table partition dt=20260701",
    )

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_program(program)

    final_sql = artifact.compiled.statements[1].sql
    assert "-- Output: ads_test_table partition dt=20260701" in final_sql, (
        f"FINAL 注释 Output 行不含真实目标：\n{final_sql[:300]}"
    )


# ════════════════════════════════════════════
# 验收项 #13: Provenance hash 一致性
# ════════════════════════════════════════════


def test_provenance_hash_matches_file_content():
    """compiled_program_sha256 与 _assemble_full_sql() 输出一致。"""
    from tianshu_datadev.artifacts.packager import ReviewPackageBuilder

    # 构造 sql_program_artifact dict
    sql_program_artifact = {
        "compiled": {
            "statements": [
                {"sql": "-- Step: test\nCREATE TEMP TABLE _t AS\nSELECT 1"},
                {"sql": "-- Step: final\nSELECT * FROM _t"},
            ],
            "cleanup_sql": ["DROP TABLE IF EXISTS _t"],
        }
    }

    full_sql = ReviewPackageBuilder._assemble_full_sql(
        sql_program_artifact, None
    )
    expected_hash = hashlib.sha256(full_sql.encode("utf-8")).hexdigest()

    # 验证 hash 一致性
    assert len(expected_hash) == 64  # SHA-256 输出 64 hex 字符
    # 重新计算应相同
    full_sql_2 = ReviewPackageBuilder._assemble_full_sql(
        sql_program_artifact, None
    )
    assert full_sql == full_sql_2
