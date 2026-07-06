"""Phase 5 SparkPlan IR 测试——模型、映射器、PlanEquivalence 规则。

覆盖：
- SparkPlan 9 种 step 类型的创建和序列化
- DataTransformContractV1 → SparkPlan 确定性映射（golden/reject）
- PlanEquivalence 9 条对比规则
- 确定性 hash
"""

from __future__ import annotations

import pytest

from tianshu_datadev.artifacts.models import (
    CaseWhenBranchSpec,
    CaseWhenCondition,
    CaseWhenLabelSpec,
    ContractAggregation,
    ContractInputTable,
    ContractJoin,
    ContractLimit,
    ContractOutputColumn,
    ContractPredicate,
    ContractSort,
    DataTransformContractV1,
    WindowSpecSummary,
)
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkCaseWhenBranch,
    SparkCaseWhenStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkJoinType,
    SparkLimitStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkStepType,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
)
from tianshu_datadev.spark.plan_equivalence import (
    EquivalenceVerdict,
    compare_aggregate_steps,
    compare_case_when_steps,
    compare_filter_steps,
    compare_join_steps,
    compare_limit_steps,
    compare_plans,
    compare_project_steps,
    compare_scan_steps,
    compare_sort_steps,
    compare_window_steps,
    normalize_field_name,
)

# ════════════════════════════════════════════
# 测试 fixture——最小合法 DataTransformContractV1
# ════════════════════════════════════════════


def _make_minimal_contract() -> DataTransformContractV1:
    """构造一个覆盖全部 9 种 step 类型的最小合法 Contract。"""
    program_id = "prog_test_001"
    contract_id = DataTransformContractV1.generate_contract_id(program_id)

    return DataTransformContractV1(
        contract_id=contract_id,
        version="v1",
        source_phase="phase-3",
        source_sqlprogram_hash=program_id,
        input_tables=[
            ContractInputTable(
                table_ref="od",
                source_table="dwd.order_detail",
                estimated_row_count=50000000,
            ),
            ContractInputTable(
                table_ref="ri",
                source_table="dim.region_info",
                estimated_row_count=500,
            ),
        ],
        input_columns=[],
        join_relationships=[
            ContractJoin(
                join_id="join_od_ri_region",
                left_table="od",
                right_table="ri",
                left_key="region_code",
                right_key="region_code",
                join_type="LEFT",
                evidence_chain={
                    "level": "STRONG",
                    "action": "ACCEPT",
                    "left_field": {"raw": "region_code", "normalized": "region_code"},
                    "right_field": {"raw": "region_code", "normalized": "region_code"},
                    "evidence_checks": {"exact_name_match": True, "type_match": True, "unique_match": True},
                },
                level="STRONG",
            ),
        ],
        filters=[
            ContractPredicate(operator="EQ", left="od.order_status", right="'paid'"),
        ],
        aggregations=[
            ContractAggregation(function="COUNT_DISTINCT", input_column="od.user_id", alias="active_users"),
            ContractAggregation(function="SUM", input_column="od.order_amount", alias="total_amount"),
            ContractAggregation(function="COUNT", input_column=None, alias="order_count"),
        ],
        grouping_keys=["stat_date", "region_code"],
        output_columns=[
            ContractOutputColumn(column_name="stat_date", alias="stat_date"),
            ContractOutputColumn(column_name="region_code", alias="region_code"),
            ContractOutputColumn(column_name="region_name", alias="region_name", data_type="string"),
            ContractOutputColumn(column_name="active_users", alias="active_users", data_type="bigint"),
            ContractOutputColumn(column_name="total_amount", alias="total_amount", data_type="decimal(18,2)"),
        ],
        output_grain=["stat_date", "region_code"],
        sort_spec=[
            ContractSort(column="total_amount", direction="DESC"),
        ],
        limit_spec=ContractLimit(limit=100),
        business_keys=["region_code"],
        step_dag={"stmt_main": []},
        temp_tables=[],
        case_when_labels=[
            CaseWhenLabelSpec(
                statement_id="stmt_label",
                output_alias="value_level",
                branch_count=3,
                labels=["high", "mid", "low"],
                else_label="unknown",
                branches=[
                    CaseWhenBranchSpec(
                        label="high",
                        condition=CaseWhenCondition(
                            operator="GT", normalized_name="amount", value=100,
                        ),
                    ),
                    CaseWhenBranchSpec(
                        label="mid",
                        condition=CaseWhenCondition(
                            operator="AND",
                            left=CaseWhenCondition(
                                operator="GT", normalized_name="amount", value=50,
                            ),
                            right=CaseWhenCondition(
                                operator="LTE", normalized_name="amount", value=100,
                            ),
                        ),
                    ),
                    CaseWhenBranchSpec(
                        label="low",
                        condition=CaseWhenCondition(
                            operator="LTE", normalized_name="amount", value=50,
                        ),
                    ),
                ],
            ),
        ],
        window_specs=[
            WindowSpecSummary(
                statement_id="stmt_rank",
                function="ROW_NUMBER",
                alias="amount_rank",
                partition_by=["stat_date"],
                order_by=["total_amount"],
            ),
        ],
        write_spec={
            "type": "partition_overwrite",
            "target_table": "ads.user_region_daily",
            "partition_field": "stat_date",
            "partition_format": "yyyyMMdd",
        },
    )


# ════════════════════════════════════════════
# 模型测试
# ════════════════════════════════════════════


class TestSparkPlanModels:
    """SparkPlan IR 模型创建和严格性测试。"""

    def test_read_step_creation(self):
        """ReadStep 基本创建——Phase 0 迁移后使用 source_name + input_key。"""
        step = SparkReadStep(
            alias="od",
            source_name="dwd.order_detail",
            input_key="od",
            required_columns=["order_status", "user_id", "order_amount"],
            estimated_row_count=50000000,
        )
        assert step.step_type == SparkStepType.READ
        assert step.alias == "od"
        assert step.source_name == "dwd.order_detail"
        assert step.input_key == "od"
        assert step.required_columns == ["order_status", "user_id", "order_amount"]
        assert step.estimated_row_count == 50000000

    def test_filter_step_creation(self):
        """FilterStep 基本创建。"""
        step = SparkFilterStep(
            input_alias="od",
            operator="EQ",
            left="od.order_status",
            right="'paid'",
        )
        assert step.step_type == SparkStepType.FILTER

    def test_join_step_creation(self):
        """JoinStep 基本创建。"""
        step = SparkJoinStep(
            left_alias="od",
            right_alias="ri",
            left_key="region_code",
            right_key="region_code",
            join_type=SparkJoinType.LEFT,
            evidence_chain={"level": "STRONG"},
        )
        assert step.step_type == SparkStepType.JOIN
        assert step.join_type == SparkJoinType.LEFT

    def test_aggregate_step_creation(self):
        """AggregateStep 基本创建。"""
        step = SparkAggregateStep(
            input_alias="od",
            group_keys=["stat_date", "region_code"],
            metrics=[
                SparkAggregateSpec(
                    function=SparkAggFunction.COUNT_DISTINCT,
                    input_column="od.user_id",
                    alias="active_users",
                ),
                SparkAggregateSpec(
                    function=SparkAggFunction.SUM,
                    input_column="od.order_amount",
                    alias="total_amount",
                ),
            ],
        )
        assert step.step_type == SparkStepType.AGGREGATE
        assert len(step.metrics) == 2

    def test_project_step_creation(self):
        """ProjectStep 基本创建。"""
        step = SparkProjectStep(
            input_alias="od",
            columns=[
                SparkProjectColumn(column_name="stat_date", alias="stat_date"),
                SparkProjectColumn(column_name="total_amount", alias="total_amount"),
            ],
        )
        assert step.step_type == SparkStepType.PROJECT
        assert len(step.columns) == 2

    def test_case_when_step_creation(self):
        """CaseWhenStep 基本创建。"""
        step = SparkCaseWhenStep(
            input_alias="",
            output_alias="value_level",
            branches=[
                SparkCaseWhenBranch(label="high"),
                SparkCaseWhenBranch(label="mid"),
                SparkCaseWhenBranch(label="low"),
            ],
            else_value="unknown",
        )
        assert step.step_type == SparkStepType.CASE_WHEN
        assert len(step.branches) == 3

    def test_window_step_creation(self):
        """WindowStep 基本创建。"""
        step = SparkWindowStep(
            input_alias="",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.ROW_NUMBER,
                    alias="amount_rank",
                    partition_by=["stat_date"],
                    order_by=["total_amount"],
                ),
            ],
        )
        assert step.step_type == SparkStepType.WINDOW
        assert len(step.expressions) == 1

    def test_sort_step_creation(self):
        """SortStep 基本创建。"""
        step = SparkSortStep(
            input_alias="",
            order_by=[
                SparkSortSpec(column="total_amount", direction=SparkSortDirection.DESC),
            ],
        )
        assert step.step_type == SparkStepType.SORT

    def test_limit_step_creation(self):
        """LimitStep 基本创建。"""
        step = SparkLimitStep(
            input_alias="",
            limit=100,
        )
        assert step.step_type == SparkStepType.LIMIT
        assert step.offset is None

    def test_spark_plan_creation_and_hash(self):
        """SparkPlan 创建和确定性 hash。"""
        steps: list = [
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="ri", source_name="dim.region_info", input_key="ri"),
            SparkLimitStep(input_alias="", limit=100),
        ]

        plan1 = SparkPlan(
            plan_id=SparkPlan.generate_plan_id("abc123"),
            version="v1",
            source_phase="phase-5",
            source_contract_hash="abc123",
            steps=steps,
        )

        plan2 = SparkPlan(
            plan_id=SparkPlan.generate_plan_id("abc123"),
            version="v1",
            source_phase="phase-5",
            source_contract_hash="abc123",
            steps=list(steps),  # 相同内容，新列表
        )

        assert plan1.plan_id == plan2.plan_id
        assert SparkPlan.compute_plan_hash(plan1) == SparkPlan.compute_plan_hash(plan2)

    def test_spark_plan_extra_field_rejected(self):
        """SparkPlan 拒绝 extra 字段。"""
        with pytest.raises(Exception):
            SparkPlan(
                plan_id="test",
                version="v1",
                source_phase="phase-5",
                source_contract_hash="abc123",
                steps=[],
                not_a_field="should_reject",  # type: ignore[call-arg]
            )

    def test_spark_plan_write_mode(self):
        """SparkPlan write_mode 默认 None。"""
        plan = SparkPlan(
            plan_id="test",
            version="v1",
            source_phase="phase-5",
            source_contract_hash="abc123",
            steps=[],
        )
        assert plan.write_mode is None


# ════════════════════════════════════════════
# 映射器测试
# ════════════════════════════════════════════


class TestSparkPlanMapper:
    """Contract → SparkPlan 确定性映射测试。"""

    def test_full_contract_mapping_success(self):
        """完整 Contract（全部 9 种 step）映射成功。"""
        contract = _make_minimal_contract()
        result = map_contract_to_spark_plan(contract)

        assert result.success is True
        assert result.spark_plan is not None
        assert len(result.unsupported) == 0

        plan = result.spark_plan
        # 验证 step 类型覆盖
        step_types = {type(s).__name__ for s in plan.steps}
        assert "SparkReadStep" in step_types  # input_tables
        assert "SparkFilterStep" in step_types  # filters
        assert "SparkJoinStep" in step_types  # join_relationships
        assert "SparkAggregateStep" in step_types  # aggregations
        assert "SparkProjectStep" in step_types  # output_columns
        assert "SparkCaseWhenStep" in step_types  # case_when_labels
        assert "SparkWindowStep" in step_types  # window_specs
        assert "SparkSortStep" in step_types  # sort_spec
        assert "SparkLimitStep" in step_types  # limit_spec

    def test_mapping_is_deterministic(self):
        """相同 Contract 两次映射产生相同 SparkPlan hash。"""
        contract = _make_minimal_contract()
        result1 = map_contract_to_spark_plan(contract)
        result2 = map_contract_to_spark_plan(contract)

        assert result1.spark_plan is not None
        assert result2.spark_plan is not None
        h1 = SparkPlan.compute_plan_hash(result1.spark_plan)
        h2 = SparkPlan.compute_plan_hash(result2.spark_plan)
        assert h1 == h2

    def test_empty_input_tables_fails(self):
        """空 input_tables 映射失败。"""
        contract = _make_minimal_contract()
        contract.input_tables = []

        result = map_contract_to_spark_plan(contract)
        assert result.success is False
        assert result.spark_plan is None
        assert any("input_tables" in g.contract_field for g in result.gaps)

    def test_empty_output_columns_fails(self):
        """空 output_columns 映射失败。"""
        contract = _make_minimal_contract()
        contract.output_columns = []

        result = map_contract_to_spark_plan(contract)
        assert result.success is False
        assert result.spark_plan is None

    def test_unsupported_agg_function(self):
        """不支持的聚合函数产生 UnsupportedPattern。"""
        contract = _make_minimal_contract()
        contract.aggregations = [
            ContractAggregation(function="MEDIAN", input_column="od.order_amount", alias="median_amt"),
        ]

        result = map_contract_to_spark_plan(contract)
        assert result.success is False
        assert len(result.unsupported) >= 1
        assert "MEDIAN" in result.unsupported[0].reason

    def test_unsupported_window_function(self):
        """不支持的窗口函数产生 UnsupportedPattern。"""
        contract = _make_minimal_contract()
        contract.window_specs = [
            WindowSpecSummary(
                statement_id="stmt_bad",
                function="CUME_DIST",  # CUME_DIST 不在 9 种白名单中
                alias="cume_rank",
                partition_by=["stat_date"],
                order_by=["total_amount"],
            ),
        ]

        result = map_contract_to_spark_plan(contract)
        assert result.success is False
        assert len(result.unsupported) >= 1

    def test_no_aggregation_is_ok(self):
        """无聚合的 Contract（明细查询）映射成功。"""
        contract = _make_minimal_contract()
        contract.aggregations = []
        contract.grouping_keys = []

        result = map_contract_to_spark_plan(contract)
        assert result.success is True
        assert result.spark_plan is not None

    def test_no_join_is_ok(self):
        """无 Join 的 Contract（单表）映射成功。"""
        contract = _make_minimal_contract()
        contract.join_relationships = []

        result = map_contract_to_spark_plan(contract)
        assert result.success is True

    def test_write_spec_partition_overwrite(self):
        """包含 partition_overwrite write_spec 时 write_mode 正确。"""
        contract = _make_minimal_contract()
        result = map_contract_to_spark_plan(contract)
        assert result.spark_plan is not None
        assert result.spark_plan.write_mode == "overwrite_partition"

    def test_write_spec_none(self):
        """无 write_spec 时 write_mode 为 None。"""
        contract = _make_minimal_contract()
        contract.write_spec = None
        result = map_contract_to_spark_plan(contract)
        assert result.spark_plan is not None
        assert result.spark_plan.write_mode is None

    def test_window_input_column_passthrough(self):
        """WindowSpecSummary.input_column → SparkWindowExpr.input_column 完整传递。"""
        contract = _make_minimal_contract()
        # 构造含 input_column 的窗口函数——LAG(amount) 和 NTILE(4)
        contract.window_specs = [
            WindowSpecSummary(
                statement_id="stmt_lag",
                function="LAG",
                alias="prev_amount",
                input_column="amount",
                partition_by=["stat_date"],
                order_by=["order_date"],
            ),
            WindowSpecSummary(
                statement_id="stmt_ntile",
                function="NTILE",
                alias="bucket",
                input_column="4",
                partition_by=[],
                order_by=["total_amount"],
            ),
        ]

        result = map_contract_to_spark_plan(contract)
        assert result.success is True, f"映射应成功，但失败：{result.gaps}"
        plan = result.spark_plan
        assert plan is not None

        # 找到 WindowStep
        window_steps = [s for s in plan.steps if isinstance(s, SparkWindowStep)]
        assert len(window_steps) == 1, f"应有 1 个 WindowStep，实际 {len(window_steps)} 个"
        ws = window_steps[0]
        assert len(ws.expressions) == 2

        # LAG 的 input_column 应传递到 SparkWindowExpr
        lag_expr = ws.expressions[0]
        assert lag_expr.function == SparkWindowFunction.LAG
        assert lag_expr.input_column == "amount", (
            f"LAG input_column 应传递 'amount'，实际为 {lag_expr.input_column!r}"
        )

        # NTILE 的 input_column 应传递到 SparkWindowExpr
        ntile_expr = ws.expressions[1]
        assert ntile_expr.function == SparkWindowFunction.NTILE
        assert ntile_expr.input_column == "4", (
            f"NTILE input_column 应传递 '4'，实际为 {ntile_expr.input_column!r}"
        )

    def test_input_alias_chain_populated_for_linear_steps(self):
        """简单线性 Contract（read → filter → project → sort → limit）的
        input_alias 依赖链被正确填充——R3 收口验证。"""
        contract = _make_minimal_contract()
        # 简化为单表单过滤器场景——去掉 join/aggregate/window/case_when + 仅保留一个输入表
        contract.input_tables = [
            ContractInputTable(
                table_ref="od",
                source_table="dwd.order_detail",
                estimated_row_count=50000000,
            ),
        ]
        contract.join_relationships = []
        contract.aggregations = []
        contract.grouping_keys = []
        contract.window_specs = []
        contract.case_when_labels = []
        contract.output_columns = [
            ContractOutputColumn(column_name="order_id", alias="order_id"),
            ContractOutputColumn(column_name="amount", alias="amount"),
        ]
        contract.sort_spec = [ContractSort(column="amount", direction="DESC")]
        contract.limit_spec = ContractLimit(limit=10)

        result = map_contract_to_spark_plan(contract)
        assert result.success is True, f"映射应成功：{result.gaps}"
        plan = result.spark_plan
        assert plan is not None

        # 验证步骤顺序：ReadStep → FilterStep → ProjectStep → SortStep → LimitStep
        step_types = [type(s).__name__ for s in plan.steps]
        assert "SparkReadStep" in step_types
        assert "SparkFilterStep" in step_types
        assert "SparkProjectStep" in step_types
        assert "SparkSortStep" in step_types
        assert "SparkLimitStep" in step_types

        # 核心验证：每个非首步的 input_alias 非空且指向正确的编译器输出别名
        for i, step in enumerate(plan.steps):
            if isinstance(step, SparkReadStep):
                # ReadStep 为数据源，无 input_alias
                assert step.alias == "od"
            elif isinstance(step, SparkFilterStep):
                # FilterStep 的 input_alias 由 _extract_table_alias 设置为 "od"
                assert step.input_alias == "od", (
                    f"FilterStep[{i}] input_alias 应为 'od'，实际为 {step.input_alias!r}"
                )
            elif isinstance(step, SparkProjectStep):
                # 前一步是 FilterStep(index)，编译器输出为 _f{index}
                assert step.input_alias != "", (
                    f"ProjectStep[{i}] input_alias 不应为空（R3 修复验证）"
                )
                assert step.input_alias.startswith("_f"), (
                    f"ProjectStep[{i}] input_alias 应指向 FilterStep 输出，"
                    f"实际为 {step.input_alias!r}"
                )
            elif isinstance(step, SparkSortStep):
                assert step.input_alias != "", (
                    f"SortStep[{i}] input_alias 不应为空（R3 修复验证）"
                )
                assert step.input_alias.startswith("_p"), (
                    f"SortStep[{i}] input_alias 应指向 ProjectStep 输出，"
                    f"实际为 {step.input_alias!r}"
                )
            elif isinstance(step, SparkLimitStep):
                assert step.input_alias != "", (
                    f"LimitStep[{i}] input_alias 不应为空（R3 修复验证）"
                )
                assert step.input_alias.startswith("_s"), (
                    f"LimitStep[{i}] input_alias 应指向 SortStep 输出，"
                    f"实际为 {step.input_alias!r}"
                )

    def test_input_alias_chain_full_contract_no_empty_aliases(self):
        """完整 Contract（9 种 step）映射后，所有需要 input_alias 的步骤
        均非空——R3 收口全量验证。"""
        contract = _make_minimal_contract()
        result = map_contract_to_spark_plan(contract)
        assert result.success is True, f"映射应成功：{result.gaps}"
        plan = result.spark_plan
        assert plan is not None

        # 遍历所有步骤，验证需要 input_alias 的步骤均非空
        for i, step in enumerate(plan.steps):
            if isinstance(step, SparkReadStep):
                continue  # 数据源，无 input_alias
            if isinstance(step, SparkJoinStep):
                continue  # 使用 left_alias/right_alias
            # 其余步骤类型均需非空 input_alias
            assert step.input_alias != "", (
                f"{type(step).__name__}[{i}] input_alias 为空——"
                f"R3 依赖链填充未覆盖此步骤类型"
            )


# ════════════════════════════════════════════
# PlanEquivalence 测试
# ════════════════════════════════════════════


class TestNormalizeFieldName:
    """字段名归一化测试。"""

    def test_strip_table_alias(self):
        assert normalize_field_name("od.user_id") == "user_id"

    def test_lowercase(self):
        assert normalize_field_name("UserID") == "userid"

    def test_no_alias(self):
        assert normalize_field_name("region_code") == "region_code"


class TestPlanEquivalence:
    """PlanEquivalence 规则测试。"""

    def test_scan_equivalent(self):
        """相同的输入表——等价。"""
        sql_scans = [
            {"table_ref": "od"},
            {"table_ref": "ri"},
        ]
        spark_reads = [
            {"alias": "od"},
            {"alias": "ri"},
        ]
        result = compare_scan_steps(sql_scans, spark_reads)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_scan_not_equivalent_count(self):
        """输入表数量不一致——不等价。"""
        result = compare_scan_steps([{"table_ref": "od"}], [{"alias": "od"}, {"alias": "ri"}])
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_filter_equivalent(self):
        """相同过滤条件——等价。"""
        sql_filters = [{"left": "od.order_status", "operator": "EQ", "right": "'paid'"}]
        spark_filters = [{"left": "od.order_status", "operator": "EQ", "right": "'paid'"}]
        result = compare_filter_steps(sql_filters, spark_filters)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_filter_not_equivalent(self):
        """不同过滤条件——不等价。"""
        result = compare_filter_steps(
            [{"left": "od.order_status", "operator": "EQ", "right": "'paid'"}],
            [{"left": "od.order_status", "operator": "EQ", "right": "'unpaid'"}],
        )
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_join_equivalent(self):
        """相同 Join 规格——等价。"""
        sql_joins = [{
            "left_table_ref": "od", "right_table_ref": "ri",
            "left_key": "region_code", "right_key": "region_code",
            "join_type": "LEFT",
        }]
        spark_joins = [{
            "left_alias": "od", "right_alias": "ri",
            "left_key": "region_code", "right_key": "region_code",
            "join_type": "left",
        }]
        result = compare_join_steps(sql_joins, spark_joins)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_join_not_equivalent_type(self):
        """Join 类型不一致——不等价。"""
        sql_join = {
            "left_table_ref": "a", "right_table_ref": "b",
            "left_key": "k", "right_key": "k", "join_type": "INNER",
        }
        spark_join = {
            "left_alias": "a", "right_alias": "b",
            "left_key": "k", "right_key": "k", "join_type": "LEFT",
        }
        result = compare_join_steps([sql_join], [spark_join])
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_aggregate_equivalent(self):
        """相同聚合规格——等价。"""
        sql_aggs = [{
            "group_keys": ["stat_date", "region_code"],
            "metrics": [
                {"function": "COUNT_DISTINCT", "input_column": "od.user_id", "alias": "active_users"},
                {"function": "SUM", "input_column": "od.order_amount", "alias": "total_amount"},
            ],
        }]
        spark_aggs = [{
            "group_keys": ["region_code", "stat_date"],  # 不同顺序
            "metrics": [
                {"function": "SUM", "input_column": "od.order_amount", "alias": "total_amount"},
                {"function": "COUNT_DISTINCT", "input_column": "od.user_id", "alias": "active_users"},
            ],
        }]
        result = compare_aggregate_steps(sql_aggs, spark_aggs)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_aggregate_not_equivalent_groups(self):
        """分组键不一致——不等价。"""
        result = compare_aggregate_steps(
            [{"group_keys": ["stat_date"], "metrics": []}],
            [{"group_keys": ["region_code"], "metrics": []}],
        )
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_project_equivalent(self):
        """相同投影——等价。"""
        sql_projects = [{
            "columns": [
                {"column_name": "stat_date", "alias": "stat_date"},
                {"column_name": "total_amount", "alias": "total_amount"},
            ],
        }]
        spark_projects = [{
            "columns": [
                {"column_name": "total_amount", "alias": "total_amount"},
                {"column_name": "stat_date", "alias": "stat_date"},
            ],
        }]
        result = compare_project_steps(sql_projects, spark_projects)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_project_not_equivalent(self):
        """投影不一致——不等价。"""
        result = compare_project_steps(
            [{"columns": [{"column_name": "a", "alias": "a"}]}],
            [{"columns": [{"column_name": "b", "alias": "b"}]}],
        )
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_case_when_equivalent(self):
        """相同 CASE WHEN——等价。"""
        sql_cw = [{"labels": ["high", "mid", "low"], "default_value": "unknown"}]
        spark_cw = [{
            "branches": [{"label": "mid"}, {"label": "high"}, {"label": "low"}],
            "else_value": "unknown",
        }]
        result = compare_case_when_steps(sql_cw, spark_cw)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_case_when_not_equivalent_labels(self):
        """CASE WHEN 标签不一致——不等价。"""
        result = compare_case_when_steps(
            [{"labels": ["high", "mid"], "default_value": None}],
            [{"branches": [{"label": "high"}, {"label": "low"}], "else_value": None}],
        )
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_window_equivalent(self):
        """相同窗口函数——等价。"""
        win_expr = {
            "function": "ROW_NUMBER", "alias": "rn",
            "partition_by": ["stat_date"], "order_by": ["total_amount"],
        }
        sql_windows = [{"window_exprs": [win_expr]}]
        spark_windows = [{"expressions": [win_expr]}]
        result = compare_window_steps(sql_windows, spark_windows)
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_window_not_equivalent(self):
        """窗口函数不一致——不等价。"""
        sql_win = {
            "window_exprs": [
                {"function": "ROW_NUMBER", "alias": "rn",
                 "partition_by": ["a"], "order_by": ["b"]},
            ],
        }
        spark_win = {
            "window_exprs": [
                {"function": "RANK", "alias": "rn",
                 "partition_by": ["a"], "order_by": ["b"]},
            ],
        }
        result = compare_window_steps([sql_win], [spark_win])
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_sort_equivalent(self):
        """相同排序——等价。"""
        result = compare_sort_steps(
            [{"order_by": [{"column": "total_amount", "direction": "DESC"}]}],
            [{"order_by": [{"column": "total_amount", "direction": "desc"}]}],
        )
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_limit_equivalent(self):
        """相同 LIMIT——等价。"""
        result = compare_limit_steps(
            [{"limit": 100, "offset": None}],
            [{"limit": 100, "offset": None}],
        )
        assert result.verdict == EquivalenceVerdict.EQUIVALENT

    def test_limit_not_equivalent(self):
        """LIMIT 不一致——不等价。"""
        result = compare_limit_steps(
            [{"limit": 100, "offset": None}],
            [{"limit": 50, "offset": None}],
        )
        assert result.verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_full_plan_equivalent(self):
        """完整 Plan 对比——等价。"""
        sql_steps = [
            {"type": "scan", "table_ref": "od"},
            {"type": "scan", "table_ref": "ri"},
        ]
        spark_steps = [
            {"step_type": "read", "alias": "od"},
            {"step_type": "read", "alias": "ri"},
        ]
        result = compare_plans(sql_steps, spark_steps, "hash_sql", "hash_spark")
        assert result.overall_verdict == EquivalenceVerdict.EQUIVALENT

    def test_full_plan_not_equivalent_extra_step(self):
        """完整 Plan 对比——Spark 侧多了 step，不等价。"""
        sql_steps = [{"type": "scan", "table_ref": "od"}]
        spark_steps = [
            {"step_type": "read", "alias": "od"},
            {"step_type": "read", "alias": "ri"},
        ]
        result = compare_plans(sql_steps, spark_steps)
        assert result.overall_verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_empty_both_sides(self):
        """两侧都空——等价。"""
        result = compare_plans([], [])
        assert result.overall_verdict == EquivalenceVerdict.EQUIVALENT
