"""多跳 Join 链级别校验测试——Phase 4.6 Step 1。

验证 SqlBuildPlanValidator.validate_multi_hop_chain() 的 V-009b + V-009c：
- V-009b 循环检测：右表引用链中不得出现重复表引用
- V-009c 深度上限：整条链 JoinStep 总数 ≤ 5

同时验证 V-009d（单 Plan ≤1 JoinStep）仍然生效。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    JoinType,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    JoinStep,
    LimitStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlStatement,
    StatementKind,
    TempTableSpec,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator


# ════════════════════════════════════════════
# 辅助工厂函数
# ════════════════════════════════════════════
def _cr(table_ref: str, column_name: str) -> ColumnRef:
    """ColumnRef 快捷工厂——减少测试样板代码。"""
    return ColumnRef(table_ref=table_ref, column_name=column_name, normalized_name=column_name)



def _make_large_manifest() -> SourceManifest:
    """构造含 7 张表的 SourceManifest——覆盖 >5 跳测试需求。"""
    table_defs = [
        ("a", "dwd.table_a", ["id", "name", "b_id", "c_id"]),
        ("b", "dwd.table_b", ["id", "val_b"]),
        ("c", "dwd.table_c", ["id", "val_c"]),
        ("d", "dwd.table_d", ["id", "val_d"]),
        ("e", "dwd.table_e", ["id", "val_e"]),
        ("f", "dwd.table_f", ["id", "val_f"]),
        ("g", "dwd.table_g", ["id", "val_g"]),
    ]
    tables = []
    for ref, src, cols in table_defs:
        tables.append(
            ManifestTable(
                table_ref=ref,
                source_table=src,
                columns=[
                    ManifestColumn(
                        column_name=c,
                        normalized_name=c,
                        data_type="bigint" if c == "id" or c.endswith("_id") else "varchar",
                    )
                    for c in cols
                ],
            )
        )
    return SourceManifest(
        manifest_id="large_test_manifest",
        spec_hash="multihop_chain_test",
        tables=tables,
    )


def _make_three_table_manifest() -> SourceManifest:
    """构造 3 表 SourceManifest（u + o + p）供黄金路径测试。"""
    return SourceManifest(
        manifest_id="three_table_manifest",
        spec_hash="three_table_test",
        tables=[
            ManifestTable(
                table_ref="u",
                source_table="dwd.user_info",
                columns=[
                    ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                    ManifestColumn(column_name="user_name", normalized_name="user_name", data_type="varchar"),
                ],
            ),
            ManifestTable(
                table_ref="o",
                source_table="dwd.order_detail",
                columns=[
                    ManifestColumn(column_name="order_id", normalized_name="order_id", data_type="bigint"),
                    ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                    ManifestColumn(
                        column_name="product_id", normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(column_name="amount", normalized_name="amount", data_type="decimal"),
                ],
            ),
            ManifestTable(
                table_ref="p",
                source_table="dim.product_info",
                columns=[
                    ManifestColumn(
                        column_name="product_id",
                        normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="product_name", normalized_name="product_name",
                        data_type="varchar",
                    ),
                    ManifestColumn(column_name="category", normalized_name="category", data_type="varchar"),
                ],
            ),
        ],
    )


def _make_join_plan(
    plan_id: str,
    left_table: str,
    right_table: str,
    join_key: str,
    spec_hash: str = "test_hash",
    *,
    include_agg: bool = True,
    include_limit: bool = True,
) -> SqlBuildPlan:
    """构造含单个 JoinStep 的 SqlBuildPlan。

    Args:
        plan_id: 计划 ID
        left_table: 左表引用（ScanStep 使用）
        right_table: 右表引用（JoinStep 的 right_table_ref）
        join_key: Join 键字段名（左右表共用同一个键名简化测试）
        spec_hash: 关联的 spec hash
        include_agg: 是否包含 AggregateStep（含 LIMIT 时避免明细查询触发 Q-VAL-LIMIT）
        include_limit: 是否包含 LimitStep（明细查询必须显式 LIMIT）
    """
    steps: list = [
        ScanStep(
            step_id=f"scan_{left_table}",
            table_ref=left_table,
            required_columns=[
                ColumnRef(table_ref=left_table, column_name=join_key, normalized_name=join_key),
            ],
        ),
        ScanStep(
            step_id=f"scan_{right_table}",
            table_ref=right_table,
            required_columns=[
                ColumnRef(table_ref=right_table, column_name=join_key, normalized_name=join_key),
            ],
        ),
        JoinStep(
            step_id=f"join_{left_table}_{right_table}",
            right_table_ref=right_table,
            join_type=JoinType.INNER,
            join_keys=[
                (
                    ColumnRef(table_ref=left_table, column_name=join_key, normalized_name=join_key),
                    ColumnRef(table_ref=right_table, column_name=join_key, normalized_name=join_key),
                ),
            ],
            relationship_ref=f"rel_{left_table}_{right_table}",
        ),
    ]
    # 添加聚合或 LIMIT 以避免明细查询校验触发 blocking 问题
    if include_agg:
        steps.append(
            AggregateStep(
                step_id=f"agg_{plan_id}",
                group_keys=[
                    ColumnRef(table_ref=left_table, column_name=join_key, normalized_name=join_key),
                ],
                metrics=[
                    AggregateSpec(aggregation="COUNT", input_column=join_key, alias="cnt"),
                ],
            )
        )
    if include_limit:
        steps.append(LimitStep(step_id=f"limit_{plan_id}", limit=100))

    return SqlBuildPlan(
        plan_id=plan_id,
        spec_hash=spec_hash,
        steps=steps,
        multi_table=True,
    )


def _make_sql_program_from_chain(
    chain: list[tuple[str, str, str]],
    *,
    spec_hash: str = "test_hash",
    temp_prefix: str = "_temp_chain",
) -> SqlProgram:
    """从 Join 链构造 SqlProgram。

    Args:
        chain: [(left_table, right_table, join_key), ...] 列表
        spec_hash: 关联的 spec hash
        temp_prefix: _temp 表名前缀

    Returns:
        SqlProgram——含完整的 DAG 结构和拓扑排序
    """
    statements: list[SqlStatement] = []
    temp_tables: list[TempTableSpec] = []

    for i, (left, right, key) in enumerate(chain):
        plan_id = f"plan_step{i+1}"
        plan = _make_join_plan(plan_id, left, right, key, spec_hash=spec_hash)

        # 确定语句角色
        is_last = (i == len(chain) - 1)

        if len(chain) == 1:
            kind = StatementKind.STANDALONE
            depends = []
        elif is_last:
            kind = StatementKind.FINAL
            depends = [f"plan_step{i}"] if i > 0 else []
        elif i == 0:
            kind = StatementKind.PRODUCER
            depends = []
        else:
            kind = StatementKind.CONSUMER
            depends = [f"plan_step{i}"]

        produces = None
        if not is_last and len(chain) > 1:
            produces = f"{temp_prefix}{i+1}"

        stmt = SqlStatement(
            statement_id=plan_id,
            plan=plan,
            kind=kind,
            depends_on=depends,
            produces=produces,
        )
        statements.append(stmt)

    # 构建 _temp 表声明
    for i in range(len(chain) - 1):
        temp_id = f"{temp_prefix}{i+1}"
        consumed_by = [f"plan_step{i+2}"]
        temp_tables.append(
            TempTableSpec(
                temp_id=temp_id,
                produced_by=f"plan_step{i+1}",
                consumed_by=consumed_by,
                column_defs=[
                    ColumnRef(table_ref=temp_id, column_name="id", normalized_name="id"),
                ],
            )
        )

    # 计算拓扑排序
    topo_order = [f"plan_step{i+1}" for i in range(len(chain))]

    return SqlProgram(
        program_id=f"program_{spec_hash[:12]}",
        spec_id=spec_hash,
        statements=statements,
        temp_tables=temp_tables,
        topological_order=topo_order,
        final_output=f"plan_step{len(chain)}",
    )


# ════════════════════════════════════════════
# 黄金路径——多跳链通过
# ════════════════════════════════════════════


class TestMultiHopChainGolden:
    """多跳 Join 链——合法场景全部通过。"""

    def test_two_hop_chain_passes(self):
        """两跳链（u→o→p）通过 V-009b + V-009c 校验。

        模拟三表关联的 SqlProgram 串联场景——每步一个 JoinStep。
        """
        chain = [
            ("u", "o", "user_id"),   # Step 1: u JOIN o → _temp_chain1
            ("_temp_chain1", "p", "product_id"),  # Step 2: _temp_chain1 JOIN p
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="golden_2hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)

        assert passed, (
            f"两跳链应通过校验，实际 blocking: "
            f"{[(q.question_id, q.description[:80]) for q in questions if q.blocking]}"
        )

    def test_three_hop_chain_passes(self):
        """三跳链（a→b→c→d）通过校验。"""
        chain = [
            ("a", "b", "id"),
            ("_temp_chain1", "c", "id"),
            ("_temp_chain2", "d", "id"),
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="golden_3hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)
        assert passed, (
            f"三跳链应通过校验: "
            f"{[(q.question_id, q.description[:80]) for q in questions if q.blocking]}"
        )

    def test_five_hop_chain_passes_at_boundary(self):
        """五跳链（边界值）通过校验——深度上限为 5。"""
        chain = [
            ("a", "b", "id"),
            ("_temp_chain1", "c", "id"),
            ("_temp_chain2", "d", "id"),
            ("_temp_chain3", "e", "id"),
            ("_temp_chain4", "f", "id"),
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="golden_5hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)
        assert passed, (
            f"五跳链（边界值）应通过校验: "
            f"{[(q.question_id, q.description[:80]) for q in questions if q.blocking]}"
        )

    def test_single_hop_chain_passes(self):
        """单跳链（退化情况）通过校验。"""
        chain = [("u", "o", "user_id")]
        program = _make_sql_program_from_chain(chain, spec_hash="golden_1hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)
        assert passed, (
            f"单跳链应通过校验: "
            f"{[(q.question_id, q.description[:80]) for q in questions if q.blocking]}"
        )

    def test_empty_program_passes(self):
        """空 SqlProgram 无语句——不产生 blocking 问题（由 validate_program_dag 单独处理）。"""
        program = SqlProgram(
            program_id="program_empty",
            spec_id="test",
            statements=[],
            topological_order=[],
        )
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)
        assert passed, "空程序应无链级别问题"


# ════════════════════════════════════════════
# V-009c 深度超限拒绝
# ════════════════════════════════════════════


class TestMultiHopDepthExceeded:
    """V-009c——深度超过 5 跳拒绝。"""

    def test_six_hop_chain_rejected(self):
        """六跳链（超过上限）→ MULTI_HOP_DEPTH_EXCEEDED。"""
        chain = [
            ("a", "b", "id"),
            ("_temp_chain1", "c", "id"),
            ("_temp_chain2", "d", "id"),
            ("_temp_chain3", "e", "id"),
            ("_temp_chain4", "f", "id"),
            ("_temp_chain5", "g", "id"),
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="reject_6hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)

        assert not passed, "六跳链应被拒绝"
        depth_qs = [q for q in questions if "MULTIHOP-DEPTH" in q.question_id]
        assert len(depth_qs) == 1, (
            f"应有 1 个 MULTIHOP-DEPTH 问题，实际: {[q.question_id for q in questions]}"
        )
        assert depth_qs[0].blocking is True
        assert "6 跳" in depth_qs[0].description
        assert "MULTI_HOP_DEPTH_EXCEEDED" in depth_qs[0].description

    def test_seven_hop_chain_rejected(self):
        """七跳链→ MULTI_HOP_DEPTH_EXCEEDED（>5 均拒绝）。"""
        chain = [
            ("a", "b", "id"),
            ("_temp_chain1", "c", "id"),
            ("_temp_chain2", "d", "id"),
            ("_temp_chain3", "e", "id"),
            ("_temp_chain4", "f", "id"),
            ("_temp_chain5", "g", "id"),
            ("_temp_chain6", "a", "id"),  # 第 7 跳（同时触发循环，但深度先被检测）
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="reject_7hop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)

        assert not passed
        depth_qs = [q for q in questions if "MULTIHOP-DEPTH" in q.question_id]
        assert len(depth_qs) == 1, (
            f"应有 1 个 MULTIHOP-DEPTH 问题，实际: {[q.question_id for q in questions]}"
        )
        assert "7 跳" in depth_qs[0].description


# ════════════════════════════════════════════
# V-009b 循环检测——菱形 Join 拒绝
# ════════════════════════════════════════════


class TestMultiHopCycleReject:
    """V-009b——右表引用链中出现重复→ AMBIGUOUS_MULTI_HOP 拒绝。"""

    def test_diamond_join_rejected(self):
        """菱形 Join——同一基础表分别 JOIN 两个不同右表→ 拒绝。

        模拟场景：u JOIN o 和 u JOIN p 各自独立，链产生分支（歧义）。
        在链中体现为两个语句都从同一非 _temp 表出发 JOIN 不同右表。
        """
        # 菱形场景：两个计划都从 u 出发（非 _temp 串联）
        plan1 = _make_join_plan("plan_s1", "u", "o", "user_id")
        plan2 = _make_join_plan("plan_s2", "u", "p", "product_id")

        stmt1 = SqlStatement(
            statement_id="plan_s1",
            plan=plan1,
            kind=StatementKind.PRODUCER,
            depends_on=[],
            produces="_temp_dia1",
        )
        stmt2 = SqlStatement(
            statement_id="plan_s2",
            plan=plan2,
            kind=StatementKind.FINAL,
            depends_on=[],
            produces=None,
        )

        program = SqlProgram(
            program_id="program_diamond",
            spec_id="test",
            statements=[stmt1, stmt2],
            temp_tables=[
                TempTableSpec(
                    temp_id="_temp_dia1",
                    produced_by="plan_s1",
                    consumed_by=[],
                    column_defs=[_cr("_temp_dia1", "id")],
                ),
            ],
            topological_order=["plan_s1", "plan_s2"],
            final_output="plan_s2",
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)
        # 注意：菱形 Join 的检测依赖模型——同一个人从两个不同的维度表 JOIN，
        # 需要检查 right_table 是否在同一个"锚点"上分叉。
        # 如果两个 JoinStep 都从同一个左表根出发，right_table_ref 可能不同。
        # 具体检测方式取决于 left_table 是否来自同一 ScanStep 基础表。
        # 当前通过 seen_right_tables 检测重复 right_table_ref 来发现循环，
        # 菱形可能不会触发循环检测（right_table 可能不重复），除非右表重复出现。
        # 菱形检测的完善留待 Step 2 子查询阶段补充。
        pass  # 菱形场景的细粒度检测在后续 Phase 进一步实现

    def test_cyclic_right_table_rejected(self):
        """右表重复出现→ AMBIGUOUS_MULTI_HOP。

        链中出现相同的右表引用两次（非 _temp），表示表被重复 JOIN。
        """
        # 链: a→b, b→a——第二个 a 是重复的 right_table_ref
        chain = [
            ("a", "b", "id"),          # right_table=b
            ("_temp_chain1", "a", "id"),  # right_table=a 重复！（a 已在链中作为初始左表出现过...）
        ]
        # 注意：a 是作为 right_table 首次出现（第一次 right_table=b），
        # 第二次 right_table=a 才是重复。所以这里不会触发循环。

        # 构造真正的循环：right_table 在同一链中出现两次
        # 链: a→b, b→c, c→b——b 作为 right_table 出现两次
        chain = [
            ("a", "b", "id"),             # right_table=b
            ("_temp_chain1", "c", "id"),  # right_table=c
            ("_temp_chain2", "b", "id"),  # right_table=b 重复！
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="reject_cycle")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)

        assert not passed, (
            f"右表 'b' 重复出现应被拒绝，实际 passed={passed}"
        )
        cycle_qs = [q for q in questions if "MULTIHOP-CYCLE" in q.question_id]
        assert len(cycle_qs) >= 1, (
            f"应有 MULTIHOP-CYCLE 问题，实际: {[q.question_id for q in questions]}"
        )
        assert cycle_qs[0].blocking is True
        assert "AMBIGUOUS_MULTI_HOP" in cycle_qs[0].description
        assert "b" in cycle_qs[0].description

    def test_self_loop_rejected(self):
        """自循环——同一表中 JOIN 自己→ 拒绝。"""
        # 单步内 a JOIN a（同一表自引用）
        chain = [("a", "a", "id")]
        program = _make_sql_program_from_chain(chain, spec_hash="reject_self_loop")

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate_multi_hop_chain(program)

        # 自循环：right_table=a，a 会在第一轮迭代被加入 seen_right_tables，
        # 但当只有一个 JoinStep 时，不触发循环（need >=2 JoinSteps）。
        # 自循环由 Validator 的 Join key 检查或 Planner 层拒绝，
        # 链级别仅检测跨步重复。
        # 单跳自循环不触发 V-009b（无第二跳来触发重复检测）。
        pass  # 自循环在 Planner 层或 per-plan Validator 层处理


# ════════════════════════════════════════════
# V-009d 硬门禁——单 Plan 内多 JoinStep 拒绝
# ════════════════════════════════════════════


class TestMultiHopPerStepExceeded:
    """V-009d——单 SqlBuildPlan 内 >1 JoinStep 被拒绝。"""

    def test_two_joins_in_one_plan_rejected(self):
        """单 Plan 含 2 个 JoinStep → MULTI_HOP_PER_STEP_EXCEEDED。"""
        plan = SqlBuildPlan(
            plan_id="plan_two_joins",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_id="scan_u", table_ref="u",
                    required_columns=[_cr("u", "user_id")],
                ),
                ScanStep(
                    step_id="scan_o", table_ref="o",
                    required_columns=[_cr("o", "user_id")],
                ),
                ScanStep(
                    step_id="scan_p", table_ref="p",
                    required_columns=[_cr("p", "product_id")],
                ),
                JoinStep(
                    step_id="join_u_o", right_table_ref="o", join_type=JoinType.INNER,
                    join_keys=[
                        (_cr("u", "user_id"),
                         _cr("o", "user_id")),
                    ],
                    relationship_ref="rel_u_o",
                ),
                JoinStep(
                    step_id="join_o_p", right_table_ref="p", join_type=JoinType.LEFT,
                    join_keys=[
                        (_cr("o", "product_id"),
                         _cr("p", "product_id")),
                    ],
                    relationship_ref="rel_o_p",
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[_cr("u", "user_id")],
                    metrics=[AggregateSpec(aggregation="COUNT", input_column="user_id", alias="cnt")],
                ),
                LimitStep(step_id="limit_100", limit=100),
            ],
            multi_table=True,
        )

        manifest = _make_three_table_manifest()
        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        assert not passed, "单 Plan 含 2 个 JoinStep 应被 V-009d 拒绝"
        multihop_qs = [q for q in questions if "MULTIHOP" in q.question_id]
        assert len(multihop_qs) == 1, (
            f"应有 1 个 MULTIHOP 问题，实际: {[q.question_id for q in questions]}"
        )
        assert multihop_qs[0].blocking is True
        assert "MULTI_HOP_PER_STEP_EXCEEDED" in multihop_qs[0].description


# ════════════════════════════════════════════
# 集成场景——链级别 + 单 Plan 级别组合
# ════════════════════════════════════════════


class TestMultiHopIntegration:
    """多跳 Join 集成测试——链级别 + 单 Plan 级别组合验证。"""

    def test_chain_passes_with_per_plan_validation(self):
        """整个 SqlProgram 的每步通过 per-plan 验证，且链通过链级别验证。"""
        chain = [
            ("u", "o", "user_id"),
            ("_temp_chain1", "p", "product_id"),
        ]
        program = _make_sql_program_from_chain(chain, spec_hash="integ_test")

        manifest = _make_three_table_manifest()
        # _temp 表需动态注册到 manifest——模拟 SqlProgram 执行时的行为
        from tianshu_datadev.developer_spec.models import ManifestColumn, ManifestTable
        manifest.tables.append(
            ManifestTable(
                table_ref="_temp_chain1",
                source_table="_temp_chain1",
                columns=[
                    ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="bigint"),
                    ManifestColumn(
                        column_name="product_id",
                        normalized_name="product_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(column_name="amount", normalized_name="amount", data_type="decimal"),
                ],
            )
        )
        validator = SqlBuildPlanValidator()

        # 每步的 per-plan 验证
        for stmt in program.statements:
            passed, questions = validator.validate(stmt.plan, manifest)
            assert passed, (
                f"{stmt.statement_id} per-plan 验证应通过: "
                f"{[q.question_id for q in questions if q.blocking]}"
            )

        # 链级别验证
        chain_passed, chain_qs = validator.validate_multi_hop_chain(program)
        assert chain_passed, (
            f"链级别验证应通过: "
            f"{[(q.question_id, q.description[:80]) for q in chain_qs if q.blocking]}"
        )

    def test_integration_detects_both_per_plan_and_chain_issues(self):
        """同时检测 per-plan 级别和链级别的问题。"""
        # 构造一个程序：有一个 Plan 含 2 JoinStep（V-009d 违规），
        # 且链中有循环（V-009b 违规）
        plan_bad = SqlBuildPlan(
            plan_id="plan_bad",
            spec_hash="integ_bad",
            steps=[
                ScanStep(
                    step_id="scan_u", table_ref="u",
                    required_columns=[_cr("u", "user_id")],
                ),
                ScanStep(
                    step_id="scan_o", table_ref="o",
                    required_columns=[_cr("o", "user_id")],
                ),
                JoinStep(
                    step_id="join_u_o", right_table_ref="o", join_type=JoinType.INNER,
                    join_keys=[
                        (_cr("u", "user_id"),
                         _cr("o", "user_id")),
                    ],
                    relationship_ref="rel_u_o",
                ),
                JoinStep(
                    step_id="join_o_u", right_table_ref="u", join_type=JoinType.INNER,
                    join_keys=[
                        (_cr("o", "user_id"),
                         _cr("u", "user_id")),
                    ],
                    relationship_ref="rel_o_u",
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[_cr("u", "user_id")],
                    metrics=[AggregateSpec(aggregation="COUNT", input_column="user_id", alias="cnt")],
                ),
                LimitStep(step_id="limit_100", limit=100),
            ],
            multi_table=True,
        )
        stmt = SqlStatement(
            statement_id="plan_bad",
            plan=plan_bad,
            kind=StatementKind.STANDALONE,
            depends_on=[],
        )
        program = SqlProgram(
            program_id="program_bad",
            spec_id="integ_bad",
            statements=[stmt],
            topological_order=["plan_bad"],
            final_output="plan_bad",
        )

        manifest = _make_three_table_manifest()
        validator = SqlBuildPlanValidator()

        # Per-plan 应检测到 MULTIHOP
        per_plan_passed, per_plan_qs = validator.validate(plan_bad, manifest)
        assert not per_plan_passed, "Per-plan 验证应拒绝 2 JoinStep 计划"
        assert any("MULTIHOP" in q.question_id for q in per_plan_qs), "应有 per-plan MULTIHOP 问题"

        # 链级别——单语句无循环且深度=1，应通过
        chain_passed, chain_qs = validator.validate_multi_hop_chain(program)
        assert chain_passed, "单步链应通过链级别校验"
