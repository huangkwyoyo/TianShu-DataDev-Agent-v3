"""Phase 3C 写入方案测试——覆盖 FinalWritePlan / WriteValidator / FinalWritePlanBuilder。

测试覆盖：
1. FinalWritePlan 日期分区 overwrite 材料生成
2. WriteValidator 拒绝全表 overwrite、无分区 overwrite、UPDATE/DELETE/MERGE
3. FinalWritePlanBuilder 从 SqlProgram 构建写入方案
4. _temp CREATE / INSERT / DROP 合法操作
5. CompilerBackend / DuckDBBackend 接口符合性
"""

from __future__ import annotations

import pytest

from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.planning.temp_table import TempTableSpec
from tianshu_datadev.sql.compiler_backend import (
    CompilerBackend,
    DuckDBBackend,
)
from tianshu_datadev.sql.write_plan import (
    FORBIDDEN_PRODUCTION_OPS,
    VALID_TEMP_OPS,
    FinalWritePlan,
    PartitionOverwriteSpec,
    TempTableStatement,
    validate_partition_format,
)
from tianshu_datadev.sql.write_plan_builder import FinalWritePlanBuilder
from tianshu_datadev.sql.write_validator import WriteValidator

# ════════════════════════════════════════════
# 测试辅助
# ════════════════════════════════════════════

def _make_minimal_sql_program(
    program_id: str = "test_prog",
    temp_produces: str | None = "_temp_result",
) -> SqlProgram:
    """创建含单个 _temp 生产者语句的最小 SqlProgram。"""
    plan = SqlBuildPlan(
        plan_id=f"{program_id}_step1",
        spec_hash="abc123",
        steps=[
            ScanStep(
                step_id="scan_1",
                table_ref="t",
                required_columns=[
                    ColumnRef(
                        table_ref="t",
                        column_name="dt",
                        normalized_name="dt",
                    ),
                    ColumnRef(
                        table_ref="t",
                        column_name="amt",
                        normalized_name="amt",
                    ),
                ],
            ),
            AggregateStep(
                step_id="agg_1",
                group_keys=[
                    ColumnRef(
                        table_ref="t",
                        column_name="dt",
                        normalized_name="dt",
                    ),
                ],
                metrics=[
                    AggregateSpec(
                        aggregation="SUM",
                        input_column="amt",
                        alias="total_amt",
                    ),
                ],
            ),
        ],
    )

    stmt = SqlStatement(
        statement_id=f"{program_id}_step1",
        plan=plan,
        kind=StatementKind.PRODUCER,
        depends_on=[],
        produces=temp_produces,
    )

    # 构建 _temp 表声明（当 produces 不为 None 时需要）——compile_program 要求
    temp_tables = []
    if temp_produces:
        temp_tables.append(
            TempTableSpec(
                temp_id=temp_produces,
                produced_by=stmt.statement_id,
                consumed_by=[],
                column_defs=[
                    ColumnRef(
                        table_ref=temp_produces,
                        column_name="dt",
                        normalized_name="dt",
                    ),
                    ColumnRef(
                        table_ref=temp_produces,
                        column_name="total_amt",
                        normalized_name="total_amt",
                    ),
                ],
                cleanup_after="program_end",
            )
        )

    return SqlProgram(
        program_id=program_id,
        spec_id="spec_abc123",
        statements=[stmt],
        temp_tables=temp_tables,
        topological_order=[stmt.statement_id],
        final_output=stmt.statement_id,
    )


# ════════════════════════════════════════════
# WriteValidationCheck + validate_partition_format
# ════════════════════════════════════════════

class TestValidatePartitionFormat:
    """分区格式校验——yyyyMMdd / yyyyMM。"""

    def test_yyyymmdd_valid(self):
        """合法 yyyyMMdd 格式通过。"""
        checks = validate_partition_format(
            {"dt": "20260101"}, "yyyyMMdd"
        )
        assert all(c.passed for c in checks)

    def test_yyyymmdd_invalid_too_short(self):
        """7 位数字被 yyyyMMdd 拒绝。"""
        checks = validate_partition_format(
            {"dt": "2026010"}, "yyyyMMdd"
        )
        assert not checks[0].passed

    def test_yyyymmdd_invalid_alpha(self):
        """含字母的值被 yyyyMMdd 拒绝。"""
        checks = validate_partition_format(
            {"dt": "2026a101"}, "yyyyMMdd"
        )
        assert not checks[0].passed

    def test_yyyymm_valid(self):
        """合法 yyyyMM 格式通过。"""
        checks = validate_partition_format(
            {"month": "202601"}, "yyyyMM"
        )
        assert all(c.passed for c in checks)

    def test_yyyymm_invalid_too_long(self):
        """8 位数字被 yyyyMM 拒绝。"""
        checks = validate_partition_format(
            {"month": "20260101"}, "yyyyMM"
        )
        assert not checks[0].passed

    def test_unsupported_format_rejected(self):
        """不支持的格式被拒绝。"""
        checks = validate_partition_format(
            {"dt": "2026-01-01"}, "yyyy-MM-dd"
        )
        assert not checks[0].passed
        assert "不支持的分区格式" in checks[0].detail

    def test_multiple_partition_keys(self):
        """多个分区键全部校验。"""
        checks = validate_partition_format(
            {"dt": "20260101", "hr": "20260101"},  # hr 也用了日期格式
            "yyyyMMdd",
        )
        assert len(checks) == 2
        assert all(c.passed for c in checks)

    def test_mixed_valid_invalid(self):
        """混合合法/非法分区值——第一个失败即标记。"""
        checks = validate_partition_format(
            {"dt": "20260101", "hr": "bad"},
            "yyyyMMdd",
        )
        assert checks[0].passed  # dt 通过
        assert not checks[1].passed  # hr 失败


# ════════════════════════════════════════════
# FinalWritePlan 模型
# ════════════════════════════════════════════

class TestFinalWritePlanModel:
    """FinalWritePlan 模型构造和序列化。"""

    def test_minimal_creation(self):
        """最小合法 FinalWritePlan 可正确构造。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test001",
            program_id="prog_abc",
            target_table="ads.result_table",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )
        assert plan.write_plan_id == "wp_test001"
        assert plan.overwrite_mode == "partition"
        assert len(plan.partition_keys) == 1

    def test_generate_write_plan_id_deterministic(self):
        """相同 program_id 生成相同 write_plan_id。"""
        id1 = FinalWritePlan.generate_write_plan_id("prog_abc")
        id2 = FinalWritePlan.generate_write_plan_id("prog_abc")
        assert id1 == id2

    def test_generate_write_plan_id_different(self):
        """不同 program_id 生成不同 write_plan_id。"""
        id1 = FinalWritePlan.generate_write_plan_id("prog_abc")
        id2 = FinalWritePlan.generate_write_plan_id("prog_xyz")
        assert id1 != id2

    def test_forbidden_operations_empty_by_default(self):
        """新创建的方案 forbidden_operations 默认为空。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="t",
        )
        assert plan.forbidden_operations == []

    def test_validation_checks_empty_by_default(self):
        """新创建的方案 validation_checks 默认为空。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="t",
        )
        assert plan.validation_checks == []


# ════════════════════════════════════════════
# TempTableStatement
# ════════════════════════════════════════════

class TestTempTableStatement:
    """_temp 表操作语句。"""

    def test_create_op(self):
        """CREATE TABLE AS SELECT 语句。"""
        op = TempTableStatement(
            temp_id="_temp_agg",
            operation="CREATE",
            sql="CREATE TABLE _temp_agg AS SELECT ...",
            order_index=0,
        )
        assert op.operation == "CREATE"
        assert "CREATE" in VALID_TEMP_OPS

    def test_insert_op(self):
        """INSERT INTO SELECT 语句。"""
        op = TempTableStatement(
            temp_id="_temp_agg",
            operation="INSERT",
            sql="INSERT INTO _temp_agg SELECT ...",
            order_index=1,
        )
        assert op.operation == "INSERT"
        assert "INSERT" in VALID_TEMP_OPS

    def test_drop_op(self):
        """DROP TABLE IF EXISTS 语句。"""
        op = TempTableStatement(
            temp_id="_temp_agg",
            operation="DROP",
            sql="DROP TABLE IF EXISTS _temp_agg",
            order_index=2,
        )
        assert op.operation == "DROP"
        assert "DROP" in VALID_TEMP_OPS


# ════════════════════════════════════════════
# WriteValidator
# ════════════════════════════════════════════

class TestWriteValidator:
    """写入方案校验器。"""

    def test_approves_valid_partition_write(self):
        """合法日期分区 overwrite 方案通过校验。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            partition_overwrite=PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
                overwrite_dml=(
                    "INSERT OVERWRITE TABLE ads.result\n"
                    "  PARTITION (dt='20260101')\n"
                    "SELECT *\n"
                    "FROM _temp_result"
                ),
                pre_check_sql="SELECT COUNT(*) FROM ads.result WHERE dt='20260101'",
                rollback_note="回滚说明",
            ),
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert WriteValidator.is_approved(validated)
        assert validated.forbidden_operations == []

    def test_rejects_full_table_overwrite(self):
        """全表 overwrite（无分区键）被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=[],  # 无分区键
            overwrite_mode="partition",
            partition_values={},
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert len(validated.forbidden_operations) > 0
        assert any("全表" in f for f in validated.forbidden_operations)

    def test_rejects_missing_partition_values(self):
        """无分区值被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={},  # 空分区值
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any(
            "缺少分区值" in c.detail
            for c in validated.validation_checks
            if not c.passed
        )

    def test_rejects_invalid_overwrite_mode(self):
        """非法 overwrite 模式被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="full_table",  # 仅允许 "partition"
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("full_table" in f for f in validated.forbidden_operations)

    def test_rejects_update_in_temp_ops(self):
        """_temp 表中的 UPDATE 操作被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id="_temp_x",
                    operation="UPDATE",  # 禁止
                    sql="UPDATE _temp_x SET ...",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("UPDATE" in f for f in validated.forbidden_operations)

    def test_rejects_delete_in_temp_ops(self):
        """_temp 表中的 DELETE 操作被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id="_temp_x",
                    operation="DELETE",  # 禁止
                    sql="DELETE FROM _temp_x WHERE ...",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("DELETE" in f for f in validated.forbidden_operations)

    def test_rejects_merge_in_temp_ops(self):
        """_temp 表中的 MERGE 操作被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id="_temp_x",
                    operation="MERGE",  # 禁止
                    sql="MERGE INTO _temp_x ...",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("MERGE" in f for f in validated.forbidden_operations)

    def test_rejects_invalid_temp_op(self):
        """非法 _temp 操作被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id="_temp_x",
                    operation="ALTER",  # 不在 VALID_TEMP_OPS 中
                    sql="ALTER TABLE _temp_x ...",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("ALTER" in f for f in validated.forbidden_operations)

    def test_rejects_full_table_overwrite_in_partition_dml(self):
        """分区 DML 中缺少 PARTITION 子句被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            partition_overwrite=PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
                overwrite_dml=(
                    "INSERT OVERWRITE TABLE ads.result\n"  # 缺少 PARTITION
                    "SELECT *\n"
                    "FROM _temp_result"
                ),
                pre_check_sql="",
                rollback_note="",
            ),
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("全表 overwrite" in f for f in validated.forbidden_operations)

    def test_rejects_forbidden_dml_in_partition_spec(self):
        """分区 DML 中包含 DELETE 被拒绝。"""
        plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="ads.result",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            partition_overwrite=PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
                overwrite_dml=(
                    "INSERT OVERWRITE TABLE ads.result\n"
                    "  PARTITION (dt='20260101')\n"
                    "SELECT * FROM _temp_result;\n"
                    "DELETE FROM ads.result WHERE dt='20260101'"  # 禁止
                ),
                pre_check_sql="",
                rollback_note="",
            ),
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert not WriteValidator.is_approved(validated)
        assert any("DELETE" in f for f in validated.forbidden_operations)


# ════════════════════════════════════════════
# FinalWritePlanBuilder
# ════════════════════════════════════════════

class TestFinalWritePlanBuilder:
    """从 SqlProgram 构建 FinalWritePlan。"""

    def test_build_from_sql_program(self):
        """从含 _temp 生产者的 SqlProgram 构建写入方案。"""
        program = _make_minimal_sql_program(
            program_id="test_build",
            temp_produces="_temp_daily_agg",
        )

        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.daily_summary",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        assert plan.program_id == "test_build"
        assert plan.target_table == "ads.daily_summary"
        assert plan.overwrite_mode == "partition"
        assert len(plan.partition_keys) == 1
        assert plan.partition_values == {"dt": "20260101"}

        # 验证 _temp 表操作
        assert len(plan.temp_table_ops) >= 2  # 至少 CREATE + DROP
        create_ops = [o for o in plan.temp_table_ops if o.operation == "CREATE"]
        drop_ops = [o for o in plan.temp_table_ops if o.operation == "DROP"]
        assert len(create_ops) == 1
        assert len(drop_ops) == 1

        # 验证分区 overwrite 规格
        assert plan.partition_overwrite is not None
        assert plan.partition_overwrite.target_table == "ads.daily_summary"
        assert "PARTITION" in plan.partition_overwrite.overwrite_dml
        assert "INSERT OVERWRITE" in plan.partition_overwrite.overwrite_dml

    def test_build_generates_review_material(self):
        """构建的写入方案包含审查材料文档。"""
        program = _make_minimal_sql_program(program_id="test_review")
        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.metrics",
            partition_keys=["month"],
            partition_values={"month": "202601"},
            partition_format="yyyyMM",
        )

        assert plan.review_material
        assert "写入方案审查材料" in plan.review_material
        assert "ads.metrics" in plan.review_material
        assert "INSERT OVERWRITE" in plan.review_material

    def test_build_generates_risk_notes(self):
        """构建的写入方案包含风险提示。"""
        program = _make_minimal_sql_program(program_id="test_risk")
        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.risky",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        assert len(plan.risk_notes) > 0
        assert any("分区 overwrite" in r for r in plan.risk_notes)

    def test_build_generates_rerun_strategy(self):
        """构建的写入方案包含重跑策略。"""
        program = _make_minimal_sql_program(program_id="test_rerun")
        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.rerun_test",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        assert plan.rerun_strategy
        assert "重跑策略" in plan.rerun_strategy
        assert "ads.rerun_test" in plan.rerun_strategy

    def test_build_rollback_note_includes_backup(self):
        """分区 overwrite 规格包含回滚备份说明。"""
        program = _make_minimal_sql_program(program_id="test_backup")
        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.important",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        assert plan.partition_overwrite is not None
        assert "备份" in plan.partition_overwrite.rollback_note
        assert "CREATE TABLE" in plan.partition_overwrite.rollback_note

    def test_build_with_yyyymm_partition_format(self):
        """月分区格式正确生成。"""
        program = _make_minimal_sql_program(program_id="test_monthly")
        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.monthly_summary",
            partition_keys=["mn"],
            partition_values={"mn": "202601"},
            partition_format="yyyyMM",
        )

        assert plan.partition_format == "yyyyMM"
        assert plan.partition_overwrite is not None
        assert "mn='202601'" in plan.partition_overwrite.overwrite_dml

    def test_write_plan_id_deterministic(self):
        """相同输入参数生成相同 write_plan_id。"""
        program1 = _make_minimal_sql_program(program_id="deterministic")
        program2 = _make_minimal_sql_program(program_id="deterministic")
        builder = FinalWritePlanBuilder()

        plan1 = builder.build(
            sql_program=program1,
            target_table="t",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )
        plan2 = builder.build(
            sql_program=program2,
            target_table="t",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        assert plan1.write_plan_id == plan2.write_plan_id


# ════════════════════════════════════════════
# CompilerBackend
# ════════════════════════════════════════════

class TestCompilerBackend:
    """CompilerBackend 抽象接口 + DuckDBBackend 实现。"""

    def test_duckdb_backend_dialect(self):
        """DuckDBBackend 返回 'duckdb' 方言。"""
        backend = DuckDBBackend()
        assert backend.dialect() == "duckdb"

    def test_duckdb_backend_is_compiler_backend(self):
        """DuckDBBackend 是 CompilerBackend 的子类。"""
        backend = DuckDBBackend()
        assert isinstance(backend, CompilerBackend)

    def test_duckdb_backend_compile_single_plan(self):
        """DuckDBBackend 编译单个 SqlBuildPlan——行为与现有 Compiler 一致。"""
        from tianshu_datadev.sql.models import CompiledSql

        plan = SqlBuildPlan(
            plan_id="test_cb",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(
                            table_ref="t",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
        )

        backend = DuckDBBackend(table_mapping={"t": "users"})
        result = backend.compile(plan)

        assert isinstance(result, CompiledSql)
        assert "users AS t" in result.sql

    def test_duckdb_backend_compile_sql_program(self):
        """DuckDBBackend 编译 SqlProgram。"""
        from tianshu_datadev.sql.models import SqlProgramArtifact

        program = _make_minimal_sql_program(program_id="test_cb_prog")

        backend = DuckDBBackend(table_mapping={"t": "test_fact"})
        result = backend.compile(program)

        assert isinstance(result, SqlProgramArtifact)
        assert result.compiled is not None
        assert len(result.compiled.statements) > 0

    def test_duckdb_backend_rejects_unknown_type(self):
        """DuckDBBackend 拒绝非 SqlBuildPlan / SqlProgram 的输入。"""
        backend = DuckDBBackend()
        with pytest.raises(TypeError, match="仅接受"):
            backend.compile("not a plan")


# ════════════════════════════════════════════
# WriteValidator + Builder 集成
# ════════════════════════════════════════════

class TestBuildThenValidate:
    """构建后立即校验的集成流程。"""

    def test_valid_plan_passes_validation(self):
        """合法写入方案构建后通过校验。"""
        program = _make_minimal_sql_program(program_id="test_integration")

        builder = FinalWritePlanBuilder()
        plan = builder.build(
            sql_program=program,
            target_table="ads.validated",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        assert WriteValidator.is_approved(validated)
        assert validated.forbidden_operations == []
        assert all(c.passed for c in validated.validation_checks)

    def test_full_table_build_still_fails_validation(self):
        """即使 Builder 构建了全表 overwrite 方案，Validator 仍拒绝。"""
        program = _make_minimal_sql_program(program_id="test_full_table")

        builder = FinalWritePlanBuilder()
        # 构建时不传分区键——Builder 仍生成方案（不做校验），
        # Validator 负责拒绝
        plan = builder.build(
            sql_program=program,
            target_table="ads.no_partition",
            partition_keys=[],  # 无分区键
            partition_values={},
        )

        validator = WriteValidator()
        validated = validator.validate(plan)

        # Validator 应拒绝无分区键的方案
        assert not WriteValidator.is_approved(validated)


# ════════════════════════════════════════════
# 禁止操作白名单校验
# ════════════════════════════════════════════

class TestForbiddenOperationsList:
    """禁止操作白名单完整性。"""

    def test_update_delete_merge_all_forbidden(self):
        """UPDATE / DELETE / MERGE / TRUNCATE 均在禁止列表中。"""
        assert "UPDATE" in FORBIDDEN_PRODUCTION_OPS
        assert "DELETE" in FORBIDDEN_PRODUCTION_OPS
        assert "MERGE" in FORBIDDEN_PRODUCTION_OPS
        assert "TRUNCATE" in FORBIDDEN_PRODUCTION_OPS

    def test_create_insert_drop_all_valid(self):
        """CREATE / INSERT / DROP 均在合法列表中。"""
        assert "CREATE" in VALID_TEMP_OPS
        assert "INSERT" in VALID_TEMP_OPS
        assert "DROP" in VALID_TEMP_OPS

    def test_alter_not_in_valid_temp_ops(self):
        """ALTER 不在合法 _temp 操作列表中。"""
        assert "ALTER" not in VALID_TEMP_OPS
