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
        """合法日期分区 overwrite 方案通过校验。

        Phase 3C 安全加固后，PartitionOverwriteSpec 仅接受结构化字段——
        overwrite_dml / pre_check_sql / rollback_note 均为 @computed_field，
        由 SafePhysicalTableName 约束的表名 + _render_sql_string_literal()
        转义的分区值确定性渲染。
        """
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
            ),
        )

        # 验证 computed fields 正确生成
        spec = plan.partition_overwrite
        assert spec is not None
        assert "INSERT OVERWRITE" in spec.overwrite_dml
        assert "PARTITION (dt='20260101')" in spec.overwrite_dml
        assert "FROM _temp_result" in spec.overwrite_dml
        assert "SELECT COUNT(*)" in spec.pre_check_sql
        assert "回滚" in spec.rollback_note

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
        """_temp 表中的 UPDATE 操作在 Schema 层即被拒绝。

        Phase 3C 安全加固：TempTableStatement 的 @model_validator
        扫描 sql 文本中的禁止操作词——包含 UPDATE 的 SQL 无法通过构造。
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="UPDATE",
                sql="UPDATE _temp_x SET col=1",
                order_index=0,
            )

    def test_rejects_delete_in_temp_ops(self):
        """_temp 表中的 DELETE 操作在 Schema 层即被拒绝。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="DELETE",
                sql="DELETE FROM _temp_x WHERE col=1",
                order_index=0,
            )

    def test_rejects_merge_in_temp_ops(self):
        """_temp 表中的 MERGE 操作在 Schema 层即被拒绝。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="MERGE",
                sql="MERGE INTO _temp_x USING src ON _temp_x.id=src.id WHEN MATCHED THEN UPDATE SET col=1",
                order_index=0,
            )

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

    def test_overwrite_dml_always_has_partition(self):
        """computed overwrite_dml 始终包含 PARTITION 子句——无法构造无分区的 DML。

        Phase 3C 安全加固后，overwrite_dml 是 @computed_field——
        任何人无法通过构造函数注入缺少 PARTITION 的 DML 文本。
        此测试验证 computed 输出始终符合安全形状。
        """
        spec = PartitionOverwriteSpec(
            target_table="ads.result",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            source_temp_table="_temp_result",
        )
        assert "PARTITION" in spec.overwrite_dml
        assert "INSERT OVERWRITE TABLE ads.result" in spec.overwrite_dml

    def test_overwrite_dml_never_contains_forbidden_ops(self):
        """computed overwrite_dml 永不包含禁止操作词。

        Schema 层保证——恶意 DML 无法通过结构化字段进入审查材料。
        此测试作为回归验证：即使未来 computed 逻辑变更，
        也不应意外引入 UPDATE/DELETE/MERGE/TRUNCATE。
        """
        spec = PartitionOverwriteSpec(
            target_table="ads.result",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            source_temp_table="_temp_result",
        )
        dml_upper = spec.overwrite_dml.upper()
        for forbidden_op in FORBIDDEN_PRODUCTION_OPS:
            assert forbidden_op not in dml_upper, (
                f"overwrite_dml 包含禁止操作 '{forbidden_op}'"
            )


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
# Phase 3C 安全链路回归——问题 1~3 绕过样本
# ════════════════════════════════════════════

class TestSecurityBypassRegression:
    """C 类安全链路回归——验证问题 1~3 的绕过路径已被关闭。

    每条测试对应一个已验证的可复现绕过：
    1. target_table 注入 → SafePhysicalTableName 拒绝
    2. INSERT INTO 替代 INSERT OVERWRITE → @computed_field 保证
    3. _temp sql 文本含分号/禁止操作 → @model_validator 拒绝
    """

    def test_bypass_1_target_table_injection_rejected(self):
        """问题 1 回归：target_table 注入在 Schema 层被拒绝。

        复现样本：
        target_table = "ads.result; DROP TABLE prod; --"
        之前：WriteValidator.is_approved() == True（全线绕过）
        修复后：FinalWritePlan 构造时 SafePhysicalTableName 拒绝分号
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            FinalWritePlan(
                write_plan_id="wp_test",
                program_id="prog_test",
                target_table="ads.result; DROP TABLE prod; --",
                partition_keys=["dt"],
                overwrite_mode="partition",
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
            )

    def test_bypass_1_partition_spec_target_table_injection_rejected(self):
        """问题 1 延伸：PartitionOverwriteSpec.target_table 注入同样被拒绝。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PartitionOverwriteSpec(
                target_table="ads.result; DROP TABLE prod; --",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
            )

    def test_bypass_1_source_temp_table_injection_rejected(self):
        """问题 1 延伸：source_temp_table 受 SafePhysicalTableName 约束。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_x; DROP TABLE prod; --",
            )

    def test_bypass_2_insert_overwrite_guaranteed_by_computed_field(self):
        """问题 2 回归：overwrite_dml 始终以 INSERT OVERWRITE 开头。

        复现样本：
        overwrite_dml = "INSERT INTO ads.result PARTITION (dt='20260101') SELECT ..."
        之前：WriteValidator.is_approved() == True（INSERT INTO 穿透）
        修复后：overwrite_dml 是 @computed_field——由结构化字段确定性渲染，
        必然以 INSERT OVERWRITE 开头，无法构造 INSERT INTO 的 DML。
        同时 WV-009 正则形状校验作为纵深防线。
        """
        spec = PartitionOverwriteSpec(
            target_table="ads.result",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            source_temp_table="_temp_result",
        )
        # computed 输出必然以 INSERT OVERWRITE 开头
        assert spec.overwrite_dml.strip().upper().startswith("INSERT OVERWRITE"), (
            f"overwrite_dml 不以 INSERT OVERWRITE 开头：{spec.overwrite_dml}"
        )
        # WV-009 正则作为纵深——必须匹配
        from tianshu_datadev.sql.write_validator import (
            _INSERT_OVERWRITE_PARTITION_RE,
        )
        assert _INSERT_OVERWRITE_PARTITION_RE.match(
            spec.overwrite_dml.strip()
        ), "overwrite_dml 不匹配 WV-009 正则"

    def test_bypass_3_temp_sql_semicolon_rejected(self):
        """问题 3 回归：_temp sql 含分号在 Schema 层被拒绝。

        复现样本：
        operation = "CREATE"
        sql = "CREATE TABLE _temp_x AS SELECT 1; DELETE FROM ads.result"
        之前：WriteValidator.is_approved() == True（sql 文本不受校验）
        修复后：TempTableStatement 的 @model_validator 拒绝任何分号
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="CREATE",
                sql="CREATE TABLE _temp_x AS SELECT 1; DELETE FROM ads.result",
                order_index=0,
            )

    def test_bypass_3_temp_sql_forbidden_op_rejected(self):
        """问题 3 延伸：_temp sql 含禁止操作词被拒绝——即使 operation 标签合法。

        operation="CREATE" 但 sql 文本内嵌 DELETE——之前可绕过，
        现在 @model_validator 扫描 sql 文本中的禁止操作词。
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="CREATE",
                sql="CREATE TABLE _temp_x AS SELECT * FROM t; DROP TABLE important",
                order_index=0,
            )

    def test_bypass_3_temp_sql_operation_mismatch_rejected(self):
        """问题 3 延伸：sql 文本与 operation 标签不一致被拒绝。

        operation="CREATE" 但 sql 以 INSERT INTO 开头——
        @model_validator 校验关键词一致性。
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TempTableStatement(
                temp_id="_temp_x",
                operation="CREATE",
                sql="INSERT INTO _temp_x SELECT * FROM t",
                order_index=0,
            )

    # ── 问题 4：分区键注入（本次修复）──

    def test_bypass_4_partition_value_key_injection_rejected(self):
        """问题 4 回归：partition_values 的 key 注入被 Schema 层拒绝。

        复现样本：
        partition_values={
            "dt) SELECT * FROM _temp_result; DROP TABLE prod; --": "20260101"
        }
        之前：WriteValidator.is_approved() == True，
             生成的 SQL 含可执行注入。
        修复后：PartitionOverwriteSpec 的 @model_validator 拒绝非标识符 key。
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={
                    "dt) SELECT * FROM _temp_result; DROP TABLE prod; --": "20260101"
                },
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
            )

    def test_bypass_4_partition_key_with_semicolon_rejected(self):
        """问题 4 延伸：partition_keys 元素含分号被拒绝。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt; DROP TABLE prod; --"],
                partition_values={"dt; DROP TABLE prod; --": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
            )

    def test_bypass_4_partition_key_mismatch_rejected(self):
        """问题 4 延伸：partition_values key 与 partition_keys 不一致被拒绝。"""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PartitionOverwriteSpec(
                target_table="ads.result",
                partition_keys=["dt"],
                partition_values={"wrong_key": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_result",
            )

    def test_partition_keys_allow_valid_identifiers(self):
        """合法的 SQL 标识符分区键正常通过。"""
        spec = PartitionOverwriteSpec(
            target_table="ads.result",
            partition_keys=["dt", "country_code"],
            partition_values={"dt": "20260101", "country_code": "CN"},
            partition_format="yyyyMMdd",
            source_temp_table="_temp_result",
        )
        assert "dt='20260101'" in spec.overwrite_dml
        assert "country_code='CN'" in spec.overwrite_dml


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
