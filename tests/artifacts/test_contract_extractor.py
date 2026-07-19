"""测试 DataTransformContractExtractor——确定性抽取 DataTransformContract。

覆盖：
- 单表 SqlBuildPlan → lite 字段完整
- 两表 Join → lite join_relationships 含证据链
- 相同 plan → 相同 lite contract + 相同 hash
- lite 不包含 SQL 代码字段
- SqlProgram → v1 确定性抽取——含全部 5 个 v1 新增字段
- v1 hash 一致性
"""

from tests._test_utils import read_fixture
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.models import (
    DataTransformContractV1,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import (
    AggregateSpec,
    AggregationType,
    AliasExpr,
    ColumnRef,
    Predicate,
    PredicateOperator,
    SortSpec,
    SqlLiteral,
    WhenBranch,
    WindowExpr,
    WindowFunction,
)
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    FilterStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    SqlBuildPlanBuilder,
    WindowStep,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.planning.temp_table import TempTableSpec


def _parse_spec(fixture_path: str):
    """解析 fixture 文件为 ParsedDeveloperSpec。"""
    parser = DeveloperSpecParser()
    text = read_fixture(fixture_path)
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

        planner = RelationshipPlanner()
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

        planner = RelationshipPlanner()
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


# ════════════════════════════════════════════
# DataTransformContract v1 抽取测试
# ════════════════════════════════════════════

# ── v1 测试辅助：构建含多 step 类型的 SqlBuildPlan ──


def _make_base_scan() -> ScanStep:
    """构建最小 ScanStep——供 v1 测试复用。"""
    return ScanStep(
        step_id="scan_t1",
        table_ref="t1",
        required_columns=[
            ColumnRef(
                table_ref="t1",
                column_name="id",
                normalized_name="id",
            ),
            ColumnRef(
                table_ref="t1",
                column_name="amount",
                normalized_name="amount",
            ),
            ColumnRef(
                table_ref="t1",
                column_name="category",
                normalized_name="category",
            ),
        ],
    )


def _make_minimal_plan(
    plan_id: str,
    extra_steps: list | None = None,
) -> SqlBuildPlan:
    """构建最小 SqlBuildPlan——Scan + Aggregate + Project + Sort + Limit。

    Args:
        plan_id: plan 唯一标识
        extra_steps: 额外插入的 step 列表（如 CaseWhenStep / WindowStep），
                     插入在 Aggregate 之后、Project 之前
    """
    steps: list = [
        _make_base_scan(),
        AggregateStep(
            step_id="agg_1",
            group_keys=[
                ColumnRef(
                    table_ref="t1",
                    column_name="category",
                    normalized_name="category",
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column="id",
                    alias="cnt",
                ),
                AggregateSpec(
                    aggregation=AggregationType.SUM,
                    input_column="amount",
                    alias="total",
                ),
            ],
        ),
    ]
    if extra_steps:
        steps.extend(extra_steps)
    steps.extend(
        [
            ProjectStep(
                step_id="proj_1",
                columns=[
                    AliasExpr(
                        expression=ColumnRef(
                            table_ref="t1",
                            column_name="category",
                            normalized_name="category",
                        ),
                        alias="category",
                    ),
                    AliasExpr(
                        expression=ColumnRef(
                            table_ref="t1",
                            column_name="cnt",
                            normalized_name="cnt",
                        ),
                        alias="cnt",
                    ),
                    AliasExpr(
                        expression=ColumnRef(
                            table_ref="t1",
                            column_name="total",
                            normalized_name="total",
                        ),
                        alias="total",
                    ),
                ],
            ),
            SortStep(
                step_id="sort_1",
                order_by=[
                    SortSpec(
                        column="total",
                        direction="DESC",
                    ),
                ],
            ),
            LimitStep(
                step_id="limit_1",
                limit=100,
            ),
        ]
    )
    return SqlBuildPlan(
        plan_id=plan_id,
        spec_hash=f"hash_{plan_id}",
        steps=steps,
    )


def _make_case_when_step() -> CaseWhenStep:
    """构建含 CASE WHEN 标签的步骤。"""
    return CaseWhenStep(
        step_id="case_1",
        cases=[
            WhenBranch(
                condition=Predicate(
                    left=ColumnRef(
                        table_ref="t1",
                        column_name="amount",
                        normalized_name="amount",
                    ),
                    operator=PredicateOperator.GTE,
                    right=SqlLiteral(value=10000),
                ),
                result=SqlLiteral(value="高价值"),
            ),
            WhenBranch(
                condition=Predicate(
                    left=ColumnRef(
                        table_ref="t1",
                        column_name="amount",
                        normalized_name="amount",
                    ),
                    operator=PredicateOperator.GTE,
                    right=SqlLiteral(value=1000),
                ),
                result=SqlLiteral(value="中价值"),
            ),
        ],
        else_value=SqlLiteral(value="低价值"),
    )


def _make_window_step() -> WindowStep:
    """构建含窗口函数的步骤——ROW_NUMBER + LAG（含输入列）。"""
    return WindowStep(
        step_id="win_1",
        window_exprs=[
            WindowExpr(
                function=WindowFunction.ROW_NUMBER,
                partition_by=[
                    ColumnRef(
                        table_ref="t1",
                        column_name="category",
                        normalized_name="category",
                    ),
                ],
                order_by=[
                    SortSpec(column="total", direction="DESC"),
                ],
                alias="rn",
            ),
            WindowExpr(
                function=WindowFunction.LAG,
                input=ColumnRef(
                    table_ref="t1",
                    column_name="total",
                    normalized_name="total",
                ),
                partition_by=[
                    ColumnRef(
                        table_ref="t1",
                        column_name="category",
                        normalized_name="category",
                    ),
                ],
                order_by=[
                    SortSpec(column="order_date", direction="ASC"),
                ],
                alias="prev_total",
            ),
        ],
    )


class TestContractExtractorV1:
    """SqlProgram → DataTransformContract v1 抽取。"""

    def test_extract_v1_all_fields_present(self):
        """从 SqlProgram 抽取 v1——验证所有 5 个 v1 新增字段非空。"""
        # 构建 SqlProgram：两个 statement，含 CASE WHEN + Window + _temp
        plan1 = _make_minimal_plan("plan_s1", extra_steps=[_make_case_when_step()])
        plan2 = _make_minimal_plan("plan_s2", extra_steps=[_make_window_step()])

        temp_spec = TempTableSpec(
            temp_id="_temp_agg_data",
            produced_by="plan_s1",
            consumed_by=["plan_s2"],
            column_defs=[
                ColumnRef(
                    table_ref="_temp_agg_data",
                    column_name="category",
                    normalized_name="category",
                ),
            ],
        )

        stmt1 = SqlStatement(
            statement_id="plan_s1",
            plan=plan1,
            kind=StatementKind.PRODUCER,
            depends_on=[],
            produces="_temp_agg_data",
        )
        stmt2 = SqlStatement(
            statement_id="plan_s2",
            plan=plan2,
            kind=StatementKind.FINAL,
            depends_on=["plan_s1"],
            produces=None,
        )

        sql_program = SqlProgram(
            program_id="program_test_v1",
            spec_id="spec_hash_001",
            statements=[stmt1, stmt2],
            temp_tables=[temp_spec],
            topological_order=["plan_s1", "plan_s2"],
            final_output="plan_s2",
        )

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        # ── 基本字段 ──
        assert contract.version == "v1"
        assert contract.source_phase == "phase-3"
        assert contract.source_sqlprogram_hash == "program_test_v1"
        assert contract.contract_id.startswith("dtc_v1_")

        # ── lite 等价字段 ──
        assert len(contract.input_tables) >= 1
        assert len(contract.input_columns) > 0
        assert len(contract.aggregations) >= 2  # COUNT + SUM 两个语句各有两个
        assert len(contract.output_columns) >= 3  # 每个语句 3 个输出列
        assert contract.sort_spec is not None
        assert contract.limit_spec is not None

        # ── v1 新增字段 1: step_dag ──
        assert "plan_s1" in contract.step_dag
        assert "plan_s2" in contract.step_dag
        assert contract.step_dag["plan_s1"] == []
        assert contract.step_dag["plan_s2"] == ["plan_s1"]

        # ── v1 新增字段 2: temp_tables ──
        assert len(contract.temp_tables) == 1
        assert contract.temp_tables[0]["temp_id"] == "_temp_agg_data"
        assert contract.temp_tables[0]["produced_by"] == "plan_s1"
        assert contract.temp_tables[0]["consumed_by"] == ["plan_s2"]

        # ── v1 新增字段 3: case_when_labels ──
        assert len(contract.case_when_labels) >= 1
        cw = contract.case_when_labels[0]
        assert cw.statement_id == "plan_s1"
        assert cw.branch_count == 2
        assert "高价值" in cw.labels
        assert "中价值" in cw.labels
        assert cw.else_label == "低价值"

        # ── v1 新增字段 4: window_specs ──
        assert len(contract.window_specs) >= 2
        ws = contract.window_specs[0]
        assert ws.statement_id == "plan_s2"
        assert ws.function == "ROW_NUMBER"
        assert ws.alias == "rn"
        assert ws.input_column is None  # 排名函数无输入列
        assert "category" in ws.partition_by
        assert "total DESC" in ws.order_by

        # LAG 窗口函数的 input_column 必须被正确提取
        ws2 = contract.window_specs[1]
        assert ws2.function == "LAG"
        assert ws2.alias == "prev_total"
        assert ws2.input_column == "total", (
            f"LAG 窗口函数应提取 input_column='total'，实际为 {ws2.input_column!r}"
        )

        # ── v1 新增字段 5: write_spec ──
        # 无 FinalWritePlan 时应为 None
        assert contract.write_spec is None

    def test_extract_v1_deterministic_hash(self):
        """相同 SqlProgram → 相同 v1 Contract → 相同 hash。"""
        plan = _make_minimal_plan("plan_standalone")

        stmt = SqlStatement(
            statement_id="plan_standalone",
            plan=plan,
            kind=StatementKind.STANDALONE,
        )

        sql_program = SqlProgram(
            program_id="program_det_test",
            spec_id="spec_hash_det",
            statements=[stmt],
            topological_order=["plan_standalone"],
            final_output="plan_standalone",
        )

        extractor = DataTransformContractExtractor()
        contract1 = extractor.extract_v1(sql_program)
        contract2 = extractor.extract_v1(sql_program)

        # 字段一致性
        assert contract1.step_dag == contract2.step_dag
        assert contract1.temp_tables == contract2.temp_tables
        assert contract1.case_when_labels == contract2.case_when_labels
        assert contract1.window_specs == contract2.window_specs
        assert contract1.input_tables == contract2.input_tables
        assert contract1.aggregations == contract2.aggregations

        # Hash 一致性
        h1 = DataTransformContractV1.compute_contract_hash(contract1)
        h2 = DataTransformContractV1.compute_contract_hash(contract2)
        assert h1 == h2

    def test_extract_v1_with_write_spec(self):
        """v1 Contract 集成 FinalWritePlan——write_spec 字段正确序列化。"""
        from tianshu_datadev.sql.write_plan import (
            FinalWritePlan,
            PartitionOverwriteSpec,
            WriteValidationCheck,
        )

        plan = _make_minimal_plan("plan_final_stmt")

        stmt = SqlStatement(
            statement_id="plan_final_stmt",
            plan=plan,
            kind=StatementKind.FINAL,
        )

        sql_program = SqlProgram(
            program_id="program_write",
            spec_id="spec_hash_write",
            statements=[stmt],
            topological_order=["plan_final_stmt"],
            final_output="plan_final_stmt",
        )

        # 构造 FinalWritePlan
        write_plan = FinalWritePlan(
            write_plan_id=FinalWritePlan.generate_write_plan_id("program_write"),
            program_id="program_write",
            target_table="ads.test_output",
            partition_keys=["dt"],
            overwrite_mode="partition",
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            partition_overwrite=PartitionOverwriteSpec(
                target_table="ads.test_output",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                source_temp_table="_temp_final",
            ),
            validation_checks=[
                WriteValidationCheck(
                    check_id="WV-001",
                    check_type="partition_format",
                    passed=True,
                    detail="日期分区格式正确：yyyyMMdd → 20260101",
                ),
            ],
            forbidden_operations=[],
            review_material="审查通过——分区 overwrite 方案符合规范",
        )

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program, write_plan)

        # write_spec 应正确序列化
        assert contract.write_spec is not None
        assert contract.write_spec["target_table"] == "ads.test_output"
        assert contract.write_spec["overwrite_mode"] == "partition"
        assert len(contract.write_spec["validation_checks"]) == 1
        assert contract.write_spec["validation_checks"][0]["check_id"] == "WV-001"

    def test_extract_v1_no_sql_code(self):
        """v1 Contract 不包含 SQL 代码字段。"""
        plan = _make_minimal_plan("plan_nosql")

        stmt = SqlStatement(
            statement_id="plan_nosql",
            plan=plan,
            kind=StatementKind.STANDALONE,
        )

        sql_program = SqlProgram(
            program_id="program_nosql",
            spec_id="spec_hash_nosql",
            statements=[stmt],
            topological_order=["plan_nosql"],
            final_output="plan_nosql",
        )

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        data = contract.model_dump()
        assert "sql" not in data
        assert "raw_sql" not in data
        assert "sql_text" not in data
        assert "compiled_sql" not in data
        assert data["version"] == "v1"
        assert data["source_phase"] == "phase-3"

    def test_extract_v1_empty_program_rejected(self):
        """空 SqlProgram 应抛出 ValueError。"""
        import pytest

        sql_program = SqlProgram(
            program_id="program_empty",
            spec_id="spec_hash_empty",
            statements=[],
        )

        extractor = DataTransformContractExtractor()
        with pytest.raises(ValueError, match="不含任何 statement"):
            extractor.extract_v1(sql_program)

    def test_extract_v1_preserves_declared_output_grain(self):
        """明细表的 DeveloperSpec 粒度必须进入 Contract，不能只保留聚合键。"""
        from tianshu_datadev.planning.program_factory import build_sql_program

        plan = _make_minimal_plan("plan_detail_grain")
        program = build_sql_program(plan, "spec_detail_grain")

        contract = DataTransformContractExtractor().extract_v1(
            program,
            output_grain=["trip_id"],
        )

        assert contract.output_grain == ["trip_id"]

    def test_predicate_to_case_when_rejects_column_ref_right(self):
        """二元比较右侧是 ColumnRef（列-列比较）→ ValueError，不静默字符串化。"""
        import pytest

        # 构造一个右侧为 ColumnRef 的 Predicate（列-列比较：amount > other_amount）
        pred = Predicate(
            left=ColumnRef(
                table_ref="t1",
                column_name="amount",
                normalized_name="amount",
            ),
            operator=PredicateOperator.GT,
            right=ColumnRef(
                table_ref="t1",
                column_name="other_amount",
                normalized_name="other_amount",
            ),
        )

        with pytest.raises(ValueError, match="列-列比较"):
            DataTransformContractExtractor._predicate_to_case_when_condition(pred)

    def test_extract_literal_value_rejects_non_sql_literal(self):
        """_extract_literal_value 对非 SqlLiteral 抛 ValueError，不返回 str()。"""
        import pytest

        col = ColumnRef(table_ref="t1", column_name="x", normalized_name="x")

        with pytest.raises(ValueError, match="右侧仅支持 SqlLiteral"):
            DataTransformContractExtractor._extract_literal_value(col)

    def test_predicate_to_case_when_rejects_predicate_left(self):
        """二元比较左侧是嵌套 Predicate → ValueError，不把 repr 字符串写入列名。"""
        import pytest

        # 攻击路径：二元比较（EQ）的 left 不是 ColumnRef，而是嵌套 Predicate
        # _extract_column_ref 必须拒绝并抛出 ValueError
        pred = Predicate(
            left=Predicate(
                left=ColumnRef(
                    table_ref="t1",
                    column_name="amount",
                    normalized_name="amount",
                ),
                operator=PredicateOperator.GT,
                right=SqlLiteral(value=50),
            ),
            operator=PredicateOperator.EQ,
            right=SqlLiteral(value=100),
        )

        with pytest.raises(ValueError, match="左侧仅支持 ColumnRef"):
            DataTransformContractExtractor._predicate_to_case_when_condition(pred)

    def test_extract_column_ref_rejects_non_column_ref(self):
        """_extract_column_ref 对非 ColumnRef 抛 ValueError，不返回 str()。"""
        import pytest

        pred = Predicate(
            left=ColumnRef(table_ref="t1", column_name="x", normalized_name="x"),
            operator=PredicateOperator.EQ,
            right=SqlLiteral(value=1),
        )

        with pytest.raises(ValueError, match="左侧仅支持 ColumnRef"):
            DataTransformContractExtractor._extract_column_ref(pred)

    def test_contract_input_tables_excludes_temp(self):
        """Contract V1 的 input_tables 不应包含 _temp_* 中间表——DAG 内部管道。"""
        from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
        from tianshu_datadev.planning.models import AliasExpr, ColumnRef
        from tianshu_datadev.planning.sql_build_plan import (
            ProjectStep,
            ScanStep,
            SqlBuildPlan,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # 构造含 _temp_* scan 的 2 语句 SqlProgram
        stmt_0_plan = SqlBuildPlan(
            plan_id="plan_0",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_type="scan",
                    step_id="scan_fc",
                    table_ref="fc",
                    required_columns=[
                        ColumnRef(table_ref="fc", column_name="borough", normalized_name="borough"),
                    ],
                ),
                ProjectStep(
                    step_type="project",
                    step_id="proj_0",
                    columns=[
                        AliasExpr(
                            expression=ColumnRef(
                                table_ref="", column_name="borough", normalized_name="borough"
                            ),
                            alias="borough",
                        ),
                    ],
                ),
            ],
        )
        stmt_1_plan = SqlBuildPlan(
            plan_id="plan_1",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_type="scan",
                    step_id="scan_temp",
                    table_ref="_temp_abc123_crash_boro_agg",
                    required_columns=[
                        ColumnRef(
                            table_ref="_temp_abc123_crash_boro_agg",
                            column_name="borough",
                            normalized_name="borough",
                        ),
                    ],
                ),
                ProjectStep(
                    step_type="project",
                    step_id="proj_1",
                    columns=[
                        AliasExpr(
                            expression=ColumnRef(
                                table_ref="", column_name="borough", normalized_name="borough"
                            ),
                            alias="borough",
                        ),
                    ],
                ),
            ],
        )
        sql_program = SqlProgram(
            program_id="test_prog",
            spec_id="test_spec",
            statements=[
                SqlStatement(statement_id="stmt_0", plan=stmt_0_plan, kind=StatementKind.PRODUCER),
                SqlStatement(
                    statement_id="stmt_1",
                    plan=stmt_1_plan,
                    kind=StatementKind.CONSUMER,
                    depends_on=["stmt_0"],
                ),
            ],
            topological_order=["stmt_0", "stmt_1"],
        )

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        input_refs = {t.table_ref for t in contract.input_tables}
        assert "fc" in input_refs, "源表 fc 应出现在 input_tables"
        assert not any(ref.startswith("_temp_") for ref in input_refs), (
            f"_temp_* 表不应进入 Contract input_tables，实际={input_refs}"
        )

    def test_temp_to_external_join_restores_source_lineage(self):
        """临时结果关联外部角色表时，Contract 必须保留 Join 和列来源。"""
        from tianshu_datadev.artifacts.contract_extractor import (
            DataTransformContractExtractor,
        )
        from tianshu_datadev.planning.models import AliasExpr, ColumnRef, JoinType
        from tianshu_datadev.planning.sql_build_plan import JoinStep, ProjectStep

        lineage = {
            ("_temp_trip_pickup", "dropoff_location_id"): ColumnRef(
                table_ref="ft",
                column_name="dropoff_location_id",
                normalized_name="dropoff_location_id",
            ),
            ("_temp_trip_pickup", "pickup_zone_name"): ColumnRef(
                table_ref="tz_pu",
                column_name="zone_name",
                normalized_name="zone_name",
            ),
        }
        join = JoinStep(
            step_id="join_dropoff",
            right_table_ref="tz_do",
            join_type=JoinType.LEFT,
            join_keys=[(
                ColumnRef(
                    table_ref="_temp_trip_pickup",
                    column_name="dropoff_location_id",
                    normalized_name="dropoff_location_id",
                ),
                ColumnRef(
                    table_ref="tz_do",
                    column_name="location_id",
                    normalized_name="location_id",
                ),
            )],
            relationship_ref="join_dropoff_relation",
        )

        extracted_join = DataTransformContractExtractor._extract_join(
            join,
            {},
            lineage,
        )
        assert extracted_join is not None
        assert extracted_join.left_table == "ft"
        assert extracted_join.right_table == "tz_do"

        project = ProjectStep(
            step_id="project_zones",
            columns=[
                AliasExpr(
                    expression=ColumnRef(
                        table_ref="_temp_trip_pickup",
                        column_name="pickup_zone_name",
                        normalized_name="pickup_zone_name",
                    ),
                    alias="pickup_zone_name",
                ),
                AliasExpr(
                    expression=ColumnRef(
                        table_ref="tz_do",
                        column_name="zone_name",
                        normalized_name="zone_name",
                    ),
                    alias="dropoff_zone_name",
                ),
            ],
        )
        outputs = DataTransformContractExtractor._extract_project(project, lineage)
        assert [item.source_table_ref for item in outputs] == ["tz_pu", "tz_do"]


# ════════════════════════════════════════════
# Phase 3C Step 2——Pipeline 集成测试
# ════════════════════════════════════════════


class TestPipelineStep2:
    """Pipeline._build_sql_program() + 条件选择 v1/lite。"""

    def test_build_sql_program_single_statement(self):
        """_build_sql_program() 从单 plan 构建 STANDALONE SqlProgram。"""
        from tianshu_datadev.planning.program_factory import build_sql_program

        plan = _make_minimal_plan("plan_step2_test")
        sql_program = build_sql_program(plan, "spec_hash_step2")

        # 基本字段
        assert sql_program.program_id.startswith("program_")
        assert sql_program.spec_id == "spec_hash_step2"
        assert sql_program.final_output == "plan_step2_test"

        # 单语句
        assert len(sql_program.statements) == 1
        stmt = sql_program.statements[0]
        assert stmt.statement_id == "plan_step2_test"
        assert stmt.kind == StatementKind.STANDALONE
        assert stmt.depends_on == []
        assert stmt.produces is None
        assert stmt.plan == plan

        # 拓扑排序——单节点
        assert sql_program.topological_order == ["plan_step2_test"]

        # temp_tables 为空
        assert sql_program.temp_tables == []

    def test_conditional_v1_when_multi_statement(self):
        """多语句 SqlProgram（>1 statement）→ extract_v1()。"""
        # 构建两个 plan
        plan1 = _make_minimal_plan("plan_s1", extra_steps=[_make_case_when_step()])
        plan2 = _make_minimal_plan("plan_s2", extra_steps=[_make_window_step()])

        temp_spec = TempTableSpec(
            temp_id="_temp_mid",
            produced_by="plan_s1",
            consumed_by=["plan_s2"],
            column_defs=[
                ColumnRef(
                    table_ref="_temp_mid",
                    column_name="category",
                    normalized_name="category",
                ),
            ],
        )

        stmt1 = SqlStatement(
            statement_id="plan_s1",
            plan=plan1,
            kind=StatementKind.PRODUCER,
            produces="_temp_mid",
        )
        stmt2 = SqlStatement(
            statement_id="plan_s2",
            plan=plan2,
            kind=StatementKind.FINAL,
            depends_on=["plan_s1"],
        )

        sql_program = SqlProgram(
            program_id="program_multi",
            spec_id="spec_hash_multi",
            statements=[stmt1, stmt2],
            temp_tables=[temp_spec],
            topological_order=["plan_s1", "plan_s2"],
            final_output="plan_s2",
        )

        # 条件：> 1 statement → extract_v1()
        extractor = DataTransformContractExtractor()
        if len(sql_program.statements) > 1:
            contract = extractor.extract_v1(sql_program)
        else:
            contract = extractor.extract(plan1)

        # 验证走 v1 路径
        assert contract.version == "v1"
        assert contract.source_phase == "phase-3"
        assert contract.source_sqlprogram_hash == "program_multi"
        assert len(contract.step_dag) == 2
        assert len(contract.temp_tables) == 1
        assert len(contract.case_when_labels) >= 1
        assert len(contract.window_specs) >= 1

    def test_conditional_lite_when_single_statement(self):
        """单语句 SqlProgram（==1 statement）→ extract()（lite）。"""
        plan = _make_minimal_plan("plan_single")

        stmt = SqlStatement(
            statement_id="plan_single",
            plan=plan,
            kind=StatementKind.STANDALONE,
        )

        sql_program = SqlProgram(
            program_id="program_single",
            spec_id="spec_hash_single",
            statements=[stmt],
            topological_order=["plan_single"],
            final_output="plan_single",
        )

        # 条件：== 1 statement → extract()（lite）
        extractor = DataTransformContractExtractor()
        if len(sql_program.statements) > 1:
            contract = extractor.extract_v1(sql_program)
        else:
            contract = extractor.extract(plan)

        # 验证走 lite 路径
        assert contract.version == "lite"
        assert contract.source_phase == "phase-2"
        assert contract.source_sqlbuildplan_hash != ""
        # lite 不含 v1 专属的 DAG/temp_tables/write_spec 字段
        data = contract.model_dump()
        assert "step_dag" not in data
        assert "temp_tables" not in data
        assert "write_spec" not in data
        # Phase 3B 修复：lite 现在包含 case_when_labels/window_specs——
        # 否则 adapt_lite_to_v1 硬编码 [] 会静默丢弃 CASE WHEN/Window 信息
        assert "case_when_labels" in data
        assert "window_specs" in data


# ════════════════════════════════════════════
# 回归测试——单语句生产路径统一走 v1（CaseWhen/Window 不再丢失）
# ════════════════════════════════════════════


class TestSingleStatementV1Regression:
    """回归：pipeline 生产路径统一 build_sql_program() + extract_v1()。

    历史缺陷：单语句走 lite extract()，CaseWhenStep/WindowStep 被静默丢弃，
    adapt_lite_to_v1() 硬编码 case_when_labels=[]，导致 Spark 侧缺失
    CASE WHEN 步骤，逻辑对比报 case_when 不等价 + 步骤顺序不等价。
    """

    @staticmethod
    def _make_case_when_full_chain_plan() -> SqlBuildPlan:
        """构建模板 2 形态的单语句 plan：scan → filter ×2 → case_when → project → sort。

        表别名用 trips（非 tN/fN 模式）——Mapper 别名校验禁止旧式代码变量名。
        """
        return SqlBuildPlan(
            plan_id="plan_cw_chain",
            spec_hash="hash_cw_chain",
            steps=[
                ScanStep(
                    step_id="scan_trips",
                    table_ref="trips",
                    required_columns=[
                        ColumnRef(
                            table_ref="trips",
                            column_name="amount",
                            normalized_name="amount",
                        ),
                        ColumnRef(
                            table_ref="trips",
                            column_name="category",
                            normalized_name="category",
                        ),
                    ],
                ),
                FilterStep(
                    step_id="filter_1",
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref="trips",
                            column_name="amount",
                            normalized_name="amount",
                        ),
                        operator=PredicateOperator.GT,
                        right=SqlLiteral(value=0),
                    ),
                ),
                FilterStep(
                    step_id="filter_2",
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref="trips",
                            column_name="amount",
                            normalized_name="amount",
                        ),
                        operator=PredicateOperator.LT,
                        right=SqlLiteral(value=100000),
                    ),
                ),
                CaseWhenStep(
                    step_id="case_1",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="trips",
                                    column_name="amount",
                                    normalized_name="amount",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=10000),
                            ),
                            result=SqlLiteral(value="高价值"),
                        ),
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="trips",
                                    column_name="amount",
                                    normalized_name="amount",
                                ),
                                operator=PredicateOperator.GTE,
                                right=SqlLiteral(value=1000),
                            ),
                            result=SqlLiteral(value="中价值"),
                        ),
                    ],
                    else_value=SqlLiteral(value="低价值"),
                    alias="value_level",
                ),
                ProjectStep(
                    step_id="proj_1",
                    columns=[
                        AliasExpr(
                            expression=ColumnRef(
                                table_ref="trips",
                                column_name="category",
                                normalized_name="category",
                            ),
                            alias="category",
                        ),
                        AliasExpr(
                            expression=ColumnRef(
                                table_ref="trips",
                                column_name="value_level",
                                normalized_name="value_level",
                            ),
                            alias="value_level",
                        ),
                    ],
                ),
                SortStep(
                    step_id="sort_1",
                    order_by=[SortSpec(column="category", direction="ASC")],
                ),
            ],
        )

    def test_case_when_full_chain_step_sequence_identical(self):
        """CaseWhen 全链路：plan → build_sql_program → extract_v1 → Mapper → SparkPlan，
        规范化步骤序列必须完全一致（不只是数量相同）。"""
        from tianshu_datadev.planning.program_factory import build_sql_program
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan

        plan = self._make_case_when_full_chain_plan()

        # ── 生产路径：单 plan 包装为 SqlProgram → v1 抽取 ──
        sql_program = build_sql_program(plan, "spec_hash_cw_chain")
        assert len(sql_program.statements) == 1

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        # v1 路径必须捕获 CaseWhenStep（历史缺陷：lite 路径静默丢弃）
        assert contract.version == "v1"
        assert len(contract.case_when_labels) == 1
        cw = contract.case_when_labels[0]
        assert cw.labels == ["高价值", "中价值"]
        assert cw.else_label == "低价值"
        assert cw.output_alias == "value_level"
        # 结构化分支条件必须存在——Spark 编译依赖
        assert len(cw.branches) == 2
        assert all(b.condition is not None for b in cw.branches)

        # Phase 3B 修复：output_columns 必须包含 CaseWhenStep 的派生列
        # （历史缺陷：ProjectStep 只含基础列，CaseWhen 输出列被遗漏）
        output_col_names = {oc.column_name for oc in contract.output_columns}
        assert "value_level" in output_col_names, (
            f"output_columns 缺少 CaseWhen 派生列 value_level，"
            f"当前列：{output_col_names}"
        )

        # ── Contract → SparkPlan 映射 ──
        result = map_contract_to_spark_plan(contract)
        assert result.success, f"映射失败：gaps={result.gaps}, unsupported={result.unsupported}"
        spark_plan = result.spark_plan
        assert spark_plan is not None

        # ── 核心断言：规范化步骤序列完全一致（read → scan 归一化后逐位对比） ──
        expected_sequence = ["scan", "filter", "filter", "case_when", "project", "sort"]
        sql_sequence = [s.step_type for s in plan.steps]
        spark_sequence = [
            "scan" if s.step_type == "read" else s.step_type
            for s in spark_plan.steps
        ]
        assert sql_sequence == expected_sequence
        assert spark_sequence == expected_sequence, (
            f"SQL/Spark 步骤序列不一致：SQL={sql_sequence}, Spark={spark_sequence}"
        )

        # Spark 侧 case_when 步骤的分支与 ELSE 完整传递
        spark_cw = [s for s in spark_plan.steps if s.step_type == "case_when"][0]
        assert len(spark_cw.branches) == 2
        assert all(b.condition is not None for b in spark_cw.branches)
        assert spark_cw.else_value == "低价值"

    def test_case_when_lite_path_preserves_labels_and_output_columns(self):
        """Phase 3B 修复：lite 路径 extract() 必须提取 case_when_labels 并将
        CaseWhenStep 输出列合并到 output_columns。

        历史缺陷：lite extract() 跳过 CaseWhenStep，导致：
        1. case_when_labels=[] → adapt_lite_to_v1 硬编码 [] → Mapper 不生成 CaseWhenStep
        2. output_columns 只含 ProjectStep 基础列 → PySpark select() 缺派生列
        """
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1

        plan = self._make_case_when_full_chain_plan()

        extractor = DataTransformContractExtractor()
        lite = extractor.extract(plan)

        # lite 必须捕获 CaseWhenStep
        assert lite.version == "lite"
        assert len(lite.case_when_labels) == 1, (
            f"lite 路径应提取 case_when_labels，但得到 {lite.case_when_labels}"
        )
        cw = lite.case_when_labels[0]
        assert cw.output_alias == "value_level"

        # output_columns 必须包含 CaseWhen 派生列
        output_col_names = {oc.column_name for oc in lite.output_columns}
        assert "value_level" in output_col_names, (
            f"lite output_columns 缺少 CaseWhen 派生列 value_level，"
            f"当前列：{output_col_names}"
        )

        # adapt_lite_to_v1 必须透传 case_when_labels
        v1 = adapt_lite_to_v1(lite)
        assert len(v1.case_when_labels) == 1
        assert v1.case_when_labels[0].output_alias == "value_level"
        # output_columns 在 v1 中也应包含派生列
        v1_output_col_names = {oc.column_name for oc in v1.output_columns}
        assert "value_level" in v1_output_col_names, (
            f"v1 output_columns 缺少 CaseWhen 派生列 value_level，"
            f"当前列：{v1_output_col_names}"
        )

    def test_case_when_output_columns_not_duplicated(self):
        """CaseWhen 派生列不应在 output_columns 中重复——去重逻辑验证。"""
        plan = self._make_case_when_full_chain_plan()

        extractor = DataTransformContractExtractor()
        lite = extractor.extract(plan)

        # value_level 只出现一次
        value_level_count = sum(
            1 for oc in lite.output_columns
            if oc.column_name == "value_level"
        )
        assert value_level_count == 1, (
            f"value_level 在 output_columns 中出现 {value_level_count} 次，应精确 1 次"
        )

    def test_window_step_single_statement_v1_path(self):
        """WindowStep 单语句：build_sql_program + extract_v1 必须捕获 window_specs。"""
        from tianshu_datadev.planning.program_factory import build_sql_program

        plan = _make_minimal_plan("plan_win_single", extra_steps=[_make_window_step()])
        sql_program = build_sql_program(plan, "spec_hash_win_single")
        assert len(sql_program.statements) == 1

        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program)

        # v1 路径必须捕获 WindowStep（历史缺陷：lite 路径静默丢弃）
        assert contract.version == "v1"
        assert len(contract.window_specs) == 2
        functions = {ws.function for ws in contract.window_specs}
        assert functions == {"ROW_NUMBER", "LAG"}

    def test_adapt_lite_to_v1_idempotent_passthrough(self):
        """adapt_lite_to_v1 对 V1 输入幂等透传——不得清空 case_when_labels/window_specs。

        历史风险：生产路径统一产出 V1 后，下游遗留的 adapt_lite_to_v1 调用点
        若在 V1 上重建默认字段，会重新引入 CaseWhen 丢失缺陷。
        """
        from tianshu_datadev.planning.program_factory import build_sql_program
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1

        plan = self._make_case_when_full_chain_plan()
        sql_program = build_sql_program(plan, "spec_hash_adapter")

        extractor = DataTransformContractExtractor()
        v1_contract = extractor.extract_v1(sql_program)
        assert len(v1_contract.case_when_labels) == 1

        adapted = adapt_lite_to_v1(v1_contract)
        # 同一对象透传——V1 字段原样保留
        assert adapted is v1_contract
        assert len(adapted.case_when_labels) == 1
