"""_alias_resolver 测试——tN/fN 序号别名 + 集成链验证。

覆盖：
- Read 按 input_key 字典序分配 tN
- 连续 Filter→f1→f2→f3
- Filter→Join→Aggregate 仅出现 tN/fN
- 双分支 Join 输入引用正确
- source input 顺序变化后 tN 映射仍确定
- 同一 Plan 多次解析编译 hash 完全一致
- Mapper→resolver→Compiler 集成测试
- ast.parse 验证变量先定义后引用
- 不含 filtered_filtered 等语义别名
"""

from __future__ import annotations

import ast

import pytest

from tianshu_datadev.spark._alias_resolver import (  # noqa: I001
    AliasResolutionError,
    assign_source_aliases,
    resolve_codegen_aliases,
)
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkJoinType,
    SparkPlan,
    SparkReadStep,
)

# ════════════════════════════════════════════
# 辅助
# ════════════════════════════════════════════


def _make_plan(*steps: object) -> SparkPlan:
    """用给定的 step 列表构造 SparkPlan。"""
    steps_list = list(steps)
    return SparkPlan(
        plan_id="test_plan",
        version="v1",
        source_phase="test",
        source_contract_hash="hash_01",
        source_contract_version="v1",
        steps=steps_list,
    )


def _collect_df_assignments(code: str) -> list[str]:
    """用 ast.parse 收集 transform 函数体中所有 DataFrame 赋值目标变量名。

    仅收集顶层 ast.Assign 的目标 Name 节点——不深入嵌套表达式。
    返回收集到的变量名列表，调用方断言非空以防测试空通过。
    """
    tree = ast.parse(code)
    func_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "transform":
            func_def = node
            break
    if func_def is None:
        return []

    df_vars: list[str] = []
    for stmt in func_def.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    df_vars.append(target.id)
    return df_vars


# ════════════════════════════════════════════
# assign_source_aliases 测试
# ════════════════════════════════════════════


class TestAssignSourceAliases:
    """assign_source_aliases()——Read 节点按 input_key 字典序分配 tN。"""

    def test_single_read_gets_t1(self):
        steps = [SparkReadStep(alias="od", source_name="orders", input_key="od")]
        result = assign_source_aliases(steps)
        assert result == {"od": "t1"}

    def test_two_reads_sorted_by_input_key(self):
        """按 input_key 字典序——dim_users < fact_orders → t1, t2。"""
        steps = [
            SparkReadStep(alias="rb", source_name="fact_orders", input_key="fact_orders"),
            SparkReadStep(alias="ra", source_name="dim_users", input_key="dim_users"),
        ]
        result = assign_source_aliases(steps)
        assert result["ra"] == "t1"  # dim_users < fact_orders
        assert result["rb"] == "t2"

    def test_input_key_order_change_deterministic(self):
        """source input 顺序变化后 tN 映射仍确定——取决于 input_key 字典序，与 steps 顺序无关。"""
        steps_a = [
            SparkReadStep(alias="r1", source_name="b_table", input_key="b"),
            SparkReadStep(alias="r2", source_name="a_table", input_key="a"),
        ]
        steps_b = [
            SparkReadStep(alias="r2", source_name="a_table", input_key="a"),
            SparkReadStep(alias="r1", source_name="b_table", input_key="b"),
        ]
        assert assign_source_aliases(steps_a) == assign_source_aliases(steps_b)

    def test_duplicate_input_key_raises(self):
        """重复 input_key → ValueError。"""
        steps = [
            SparkReadStep(alias="r1", source_name="a", input_key="same"),
            SparkReadStep(alias="r2", source_name="b", input_key="same"),
        ]
        with pytest.raises(ValueError, match="重复的 input_key"):
            assign_source_aliases(steps)

    def test_non_read_steps_ignored(self):
        """非 Read 步骤不影响 tN 分配。"""
        steps = [
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
        ]
        result = assign_source_aliases(steps)
        assert result == {"od": "t1"}


# ════════════════════════════════════════════
# AliasResolutionError——严格校验
# ════════════════════════════════════════════


class TestAliasResolutionErrors:
    """Resolver 严格校验——依赖缺失、重复 alias、空 Plan 必须报错。"""

    def test_empty_plan_raises(self):
        """空 Plan → AliasResolutionError。"""
        plan = _make_plan()
        with pytest.raises(AliasResolutionError, match="空 Plan"):
            resolve_codegen_aliases(plan)

    def test_first_step_not_read_raises(self):
        """首个步骤不是 ReadStep 且无 input_alias → AliasResolutionError。"""
        plan = _make_plan(
            SparkFilterStep(input_alias="", operator="GT", left="x", right="0"),
        )
        with pytest.raises(AliasResolutionError, match="首个步骤必须是 ReadStep"):
            resolve_codegen_aliases(plan)

    def test_missing_dependency_in_filter_raises(self):
        """Filter 的 input_alias 在 latest 中不存在 → AliasResolutionError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="nonexistent", operator="GT", left="x", right="0"),
        )
        with pytest.raises(AliasResolutionError, match="input_alias='nonexistent' 未解析"):
            resolve_codegen_aliases(plan)

    def test_missing_left_alias_in_join_raises(self):
        """Join 左表别名未解析 → AliasResolutionError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkJoinStep(
                left_alias="missing_left", right_alias="od",
                left_key="id", right_key="id",
                join_type=SparkJoinType.INNER,
            ),
        )
        with pytest.raises(AliasResolutionError, match="左表别名 'missing_left' 未解析"):
            resolve_codegen_aliases(plan)

    def test_missing_right_alias_in_join_raises(self):
        """Join 右表别名未解析 → AliasResolutionError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkJoinStep(
                left_alias="od", right_alias="missing_right",
                left_key="id", right_key="id",
                join_type=SparkJoinType.INNER,
            ),
        )
        with pytest.raises(AliasResolutionError, match="右表别名 'missing_right' 未解析"):
            resolve_codegen_aliases(plan)

    def test_duplicate_read_alias_raises(self):
        """两个 Read 使用相同 alias → AliasResolutionError。"""
        steps = [
            SparkReadStep(alias="same", source_name="a_table", input_key="a"),
            SparkReadStep(alias="same", source_name="b_table", input_key="b"),
        ]
        with pytest.raises(AliasResolutionError, match="重复的 Read alias: 'same'"):
            assign_source_aliases(steps)

    def test_duplicate_read_alias_and_input_key_both_checked(self):
        """重复 alias + 重复 input_key —— 任一检查先触发均拦截。"""
        steps = [
            SparkReadStep(alias="same", source_name="a_table", input_key="same_key"),
            SparkReadStep(alias="same", source_name="b_table", input_key="same_key"),
        ]
        # input_key 检查在前——率先触发
        with pytest.raises(AliasResolutionError, match="重复的 input_key"):
            assign_source_aliases(steps)


# ════════════════════════════════════════════
# resolve_codegen_aliases 测试
# ════════════════════════════════════════════


class TestResolveCodegenAliases:
    """resolve_codegen_aliases()——tN/fN 序号别名分配。"""

    def test_read_then_filter_produces_t1_f1(self):
        """Read→Filter → t1→f1。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
        )
        resolved = resolve_codegen_aliases(plan)
        assert resolved.steps[0].output_var == "t1"
        assert resolved.steps[1].output_var == "f1"
        assert resolved.steps[1].input_vars == ("t1",)

    def test_two_consecutive_filters_produce_f1_f2(self):
        """两个连续 Filter：t1→f1→f2。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GTE", left="od.pickup_at", right="'2026-01-01'"),
            SparkFilterStep(input_alias="od", operator="LT", left="od.pickup_at", right="'2026-04-01'"),
        )
        resolved = resolve_codegen_aliases(plan)
        assert resolved.steps[0].output_var == "t1"
        assert resolved.steps[1].output_var == "f1"
        assert resolved.steps[1].input_vars == ("t1",)
        assert resolved.steps[2].output_var == "f2"
        # 第二个 Filter 的输入应为第一个 Filter 的输出 f1
        assert resolved.steps[2].input_vars == ("f1",)

    def test_three_consecutive_filters_produce_f1_f2_f3(self):
        """三个连续 Filter：t1→f1→f2→f3。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.a", right="0"),
            SparkFilterStep(input_alias="od", operator="LT", left="od.b", right="10"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.c", right="'x'"),
        )
        resolved = resolve_codegen_aliases(plan)
        assert resolved.steps[0].output_var == "t1"
        assert resolved.steps[1].output_var == "f1"
        assert resolved.steps[2].output_var == "f2"
        assert resolved.steps[3].output_var == "f3"
        assert resolved.output_var == "f3"

    def test_join_input_vars_correct(self):
        """双分支 Join——左右 input_vars 正确。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkReadStep(alias="up", source_name="users", input_key="up"),
            SparkJoinStep(
                left_alias="od", right_alias="up",
                left_key="user_id", right_key="id",
                join_type=SparkJoinType.LEFT,
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        join_step = resolved.steps[2]
        assert join_step.input_vars == ("t1", "t2")  # od < up 按 input_key
        assert join_step.output_var == "f1"

    def test_filter_join_aggregate_no_semantic_aliases(self):
        """Filter→Join→Aggregate——所有输出变量均为 tN/fN，不含语义后缀。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkReadStep(alias="ri", source_name="regions", input_key="ri"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
            SparkJoinStep(
                left_alias="od", right_alias="ri",
                left_key="region_id", right_key="id",
                join_type=SparkJoinType.INNER,
            ),
            SparkAggregateStep(
                input_alias="od",
                group_keys=["region_id"],
                metrics=[SparkAggregateSpec(
                    function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
                )],
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        all_vars = {s.output_var for s in resolved.steps}
        # 所有变量名仅含 tN/fN
        for v in all_vars:
            assert v.startswith("t") or v.startswith("f"), f"非预期变量名: {v!r}"
        # 不含语义后缀
        for s in resolved.steps:
            assert "_filtered" not in s.output_var
            assert "_with_" not in s.output_var

    def test_hash_determinism(self):
        """同一 Plan 多次解析编译 hash 完全一致。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
        )
        result1 = SparkCompiler().compile(plan)
        result2 = SparkCompiler().compile(plan)
        assert result1.raw_hash == result2.raw_hash
        assert result1.raw_pyspark == result2.raw_pyspark


# ════════════════════════════════════════════
# 旧式别名拒绝——回归测试（B 类设计一致性修复）
# ════════════════════════════════════════════


class TestOldStyleAliasRejection:
    """旧式语义别名必须在 resolver 层被严格拒绝——不得模糊回退。"""

    def test_aggregate_with_old_style_alias_raises(self):
        """真实失败形态：Aggregate 的 input_alias 为旧式派生名 → AliasResolutionError。

        Read(ft) + Read(tz) → Filter → Filter → Join → Aggregate
        Aggregate.input_alias='ft_filtered_filtered_with_tz' 无法在 latest 中解析。
        """
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_table", input_key="ft"),
            SparkReadStep(alias="tz", source_name="timezone", input_key="tz"),
            SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="0"),
            SparkFilterStep(input_alias="ft", operator="LT", left="ft.amount", right="100"),
            SparkJoinStep(
                left_alias="ft", right_alias="tz",
                left_key="tz_id", right_key="id",
                join_type=SparkJoinType.LEFT,
            ),
            # 旧式语义别名——Mapper 从 Contract 列名前缀推断产生的错误值
            SparkAggregateStep(
                input_alias="ft_filtered_filtered_with_tz",
                group_keys=["region_id"],
                metrics=[SparkAggregateSpec(
                    function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
                )],
            ),
        )
        with pytest.raises(AliasResolutionError, match="input_alias='ft_filtered_filtered_with_tz' 未解析"):
            resolve_codegen_aliases(plan)

    def test_correct_alias_same_topology_compiles(self):
        """相同拓扑但 Aggregate 使用正确的稳定 lineage key → 编译成功。

        证明问题出在 input_alias 的值，而非拓扑结构。
        """
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_table", input_key="ft"),
            SparkReadStep(alias="tz", source_name="timezone", input_key="tz"),
            SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="0"),
            SparkFilterStep(input_alias="ft", operator="LT", left="ft.amount", right="100"),
            SparkJoinStep(
                left_alias="ft", right_alias="tz",
                left_key="tz_id", right_key="id",
                join_type=SparkJoinType.LEFT,
            ),
            # 使用 Join 的左表别名作为 lineage key——正确的依赖标识
            SparkAggregateStep(
                input_alias="ft",
                group_keys=["region_id"],
                metrics=[SparkAggregateSpec(
                    function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
                )],
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        compiled = SparkCompiler().compile(plan)

        # 编译成功，不含旧式别名
        assert "_filtered" not in compiled.raw_pyspark
        assert "filtered_filtered" not in compiled.raw_pyspark
        # Aggregate 的输入来自 Join 的输出（f3），而非原始 Read（t1）
        agg_step = resolved.steps[5]
        assert agg_step.input_vars[0].startswith("f"), (
            f"Aggregate 输入应为 Join 的输出 fN，实际为 {agg_step.input_vars[0]}"
        )


# ════════════════════════════════════════════
# 集成测试——Mapper→resolver→Compiler
# ════════════════════════════════════════════


class TestIntegrationMapperToCompiler:
    """Mapper→resolver→Compiler 全链路集成测试。"""

    def test_mapped_plan_compiles_with_ordinal_aliases(self):
        """Mapper 产出 → resolver 解析 → Compiler 编译——仅含 tN/fN。

        使用 minimal contract 通过 map_contract_to_spark_plan 走完整 Mapper 路径。
        """
        from tianshu_datadev.artifacts.models import (
            ContractAggregation,
            ContractInputTable,
            ContractJoin,
            ContractOutputColumn,
            ContractPredicate,
            DataTransformContractV1,
        )

        program_id = "prog_test_int_001"
        contract = DataTransformContractV1(
            contract_id=DataTransformContractV1.generate_contract_id(program_id),
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(
                    table_ref="od", source_table="dwd.order_detail",
                ),
                ContractInputTable(
                    table_ref="up", source_table="dim.user_profile",
                ),
            ],
            input_columns=[],
            filters=[
                ContractPredicate(operator="GT", left="od.amount", right="0"),
            ],
            join_relationships=[
                ContractJoin(
                    join_id="join_1",
                    left_table="od", right_table="up",
                    left_key="user_id", right_key="user_id",
                    join_type="INNER",
                    level="STRONG",
                ),
            ],
            aggregations=[
                ContractAggregation(function="COUNT", input_column=None, alias="cnt"),
            ],
            grouping_keys=["name"],
            output_columns=[
                ContractOutputColumn(column_name="name", alias="name"),
                ContractOutputColumn(column_name="cnt", alias="cnt"),
            ],
            output_grain=["name"],
            sort_spec=[],
            limit_spec=None,
            case_when_labels=[],
            window_specs=[],
        )

        result = map_contract_to_spark_plan(contract)
        assert result.success is True, f"映射失败：{result.gaps}"
        plan = result.spark_plan
        assert plan is not None

        compiler = SparkCompiler()
        compiled = compiler.compile(plan)
        code = compiled.raw_pyspark

        # 仅含 tN/fN 变量名
        assert "t1 = inputs[" in code
        assert "t2 = inputs[" in code
        assert "f" in code
        # 不含语义别名
        assert "_filtered" not in code
        assert "_with_" not in code
        assert "_output" not in code
        assert "_selected" not in code
        assert "_sorted" not in code
        # 不含 filtered_filtered 模式
        assert "filtered_filtered" not in code

    def test_contract_with_old_style_column_prefix_causes_resolver_failure(self):
        """Contract 聚合列带旧式前缀 → Mapper 不再推断旧式 alias → 使用稳定 lineage key。

        这是本次 B 类修复的精确复现：
        Contract 的 input_column="ft_filtered.filtered_with_tz" 是 SqlBuildPlan
        中间表派生名。修复前 Mapper 从列名前缀提取 "ft_filtered" 作为 input_alias，
        导致新 resolver 无法解析。修复后 Mapper 不再从列名推断，由
        _chain_input_aliases 用稳定 lineage key（如 "ft"）填充。
        """
        from tianshu_datadev.artifacts.models import (
            ContractAggregation,
            ContractInputTable,
            ContractJoin,
            ContractOutputColumn,
            ContractPredicate,
            DataTransformContractV1,
        )

        program_id = "prog_test_old_alias_001"
        contract = DataTransformContractV1(
            contract_id=DataTransformContractV1.generate_contract_id(program_id),
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(table_ref="ft", source_table="fact_table"),
                ContractInputTable(table_ref="tz", source_table="dim_timezone"),
            ],
            input_columns=[],
            filters=[
                ContractPredicate(operator="GT", left="ft.amount", right="0"),
                ContractPredicate(operator="LT", left="ft.amount", right="100"),
            ],
            join_relationships=[
                ContractJoin(
                    join_id="join_1",
                    left_table="ft", right_table="tz",
                    left_key="tz_id", right_key="id",
                    join_type="LEFT",
                    level="STRONG",
                ),
            ],
            aggregations=[
                # 关键：input_column 携带 SqlBuildPlan 旧式派生表前缀
                ContractAggregation(
                    function="COUNT",
                    input_column="ft_filtered.filtered_with_tz",
                    alias="cnt",
                ),
            ],
            grouping_keys=["region_id"],
            output_columns=[
                ContractOutputColumn(column_name="region_id", alias="region_id"),
                ContractOutputColumn(column_name="cnt", alias="cnt"),
            ],
            output_grain=["region_id"],
            sort_spec=[],
            limit_spec=None,
            case_when_labels=[],
            window_specs=[],
        )

        result = map_contract_to_spark_plan(contract)
        # 修复后：Aggregate 的 input_alias 不再从列名前缀推断，
        # 而是由 _chain_input_aliases 用稳定 lineage key 填充（此处应为 "ft"）
        assert result.success is True, f"映射失败：{result.gaps}"
        plan = result.spark_plan
        assert plan is not None

        # 验证 Aggregate 使用稳定 lineage key 而非旧式派生别名
        agg_steps = [s for s in plan.steps if isinstance(s, SparkAggregateStep)]
        assert len(agg_steps) == 1
        assert agg_steps[0].input_alias == "ft", (
            f"Aggregate input_alias 应为稳定 lineage key 'ft'，"
            f"实际为 {agg_steps[0].input_alias!r}"
        )

        # 编译应成功
        compiler = SparkCompiler()
        compiled = compiler.compile(plan)

        # DataFrame 变量名只有 tN/fN——用 ast.parse 收集，确保非空
        df_vars = _collect_df_assignments(compiled.raw_pyspark)
        assert len(df_vars) >= 1, "应至少收集到 1 个 DataFrame 赋值变量"
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}"
            )
        # return 语句指向 fN
        assert "return f" in compiled.raw_pyspark, (
            f"return 应指向 fN，实际代码: {compiled.raw_pyspark[-80:]}"
        )
        # 关键：input_alias 不再包含 _filtered（验证旧式别名未进入依赖字段）
        assert agg_steps[0].input_alias == "ft", (
            f"Aggregate input_alias 应为 'ft'，实际为 {agg_steps[0].input_alias!r}"
        )

    def test_contract_with_filter_old_style_prefix_blocked(self):
        """Contract Filter 的 left 带旧式前缀 → Mapper 别名校验阻断。

        Filter 的 left="ft_filtered_filtered_with_tz.some_col" 提取出的
        input_alias 不在已知 Read alias 集合中，应由 _validate_step_aliases
        检测并以 BLOCKING gap 拒绝，不得猜测或自动修正。
        """
        from tianshu_datadev.artifacts.models import (
            ContractAggregation,
            ContractInputTable,
            ContractJoin,
            ContractOutputColumn,
            ContractPredicate,
            DataTransformContractV1,
        )

        program_id = "prog_test_old_alias_002"
        contract = DataTransformContractV1(
            contract_id=DataTransformContractV1.generate_contract_id(program_id),
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(table_ref="ft", source_table="fact_table"),
                ContractInputTable(table_ref="tz", source_table="dim_timezone"),
            ],
            input_columns=[],
            filters=[
                # Filter 的 left 带旧式前缀——_extract_table_alias 会错误提取
                ContractPredicate(
                    operator="GT",
                    left="ft_filtered_filtered_with_tz.some_col",
                    right="0",
                ),
            ],
            join_relationships=[
                ContractJoin(
                    join_id="join_1",
                    left_table="ft", right_table="tz",
                    left_key="tz_id", right_key="id",
                    join_type="LEFT",
                    level="STRONG",
                ),
            ],
            aggregations=[
                ContractAggregation(
                    function="COUNT", input_column=None, alias="cnt",
                ),
            ],
            grouping_keys=["region_id"],
            output_columns=[
                ContractOutputColumn(column_name="region_id", alias="region_id"),
            ],
            output_grain=["region_id"],
            sort_spec=[],
            limit_spec=None,
            case_when_labels=[],
            window_specs=[],
        )

        result = map_contract_to_spark_plan(contract)
        # 修复后：Mapper 应拒绝含旧式别名的 Contract，返回 BLOCKING gap
        assert result.success is False, (
            f"应拒绝含旧式前缀的 Filter alias，但 result.success={result.success}"
        )
        blocking_gaps = [g for g in result.gaps if g.severity == "BLOCKING"]
        assert len(blocking_gaps) >= 1, f"应有 BLOCKING gap，实际 gaps: {result.gaps}"
        assert any("ft_filtered_filtered_with_tz" in g.missing_info for g in blocking_gaps), (
            f"gap 应提及无法解析的 alias 'ft_filtered_filtered_with_tz'，"
            f"实际: {blocking_gaps[0].missing_info if blocking_gaps else 'N/A'}"
        )

    def test_compiled_code_passes_ast_parse(self):
        """编译产物通过 ast.parse——所有变量先定义后引用。

        按函数体语句顺序逐条检查：每条赋值语句的 RHS 中引用的变量
        必须在之前已定义（assigned 集合中）。避免全局集合模式的漏检——
        即 f2 = f1... 出现在 f1 = ... 之前时也能被捕获。
        """
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkReadStep(alias="up", source_name="users", input_key="up"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
            SparkFilterStep(input_alias="od", operator="LT", left="od.amount", right="100"),
            SparkJoinStep(
                left_alias="od", right_alias="up",
                left_key="user_id", right_key="id",
                join_type=SparkJoinType.LEFT,
            ),
        )
        compiler = SparkCompiler()
        compiled = compiler.compile(plan)

        # Python ast 解析——通过即无语法错误
        tree = ast.parse(compiled.raw_pyspark)

        # 找到 def transform(...) 函数定义
        func_def = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "transform":
                func_def = node
                break
        assert func_def is not None, "编译产物中未找到 transform 函数定义"

        # 内置/形参——不视为未定义
        builtins = {"inputs", "params", "F", "Window", "print", "len",
                     "range", "str", "int", "float", "bool", "dict", "list",
                     "DataFrame", "Mapping", "None"}

        assigned: set[str] = set()

        # 按函数体语句顺序逐条检查——先查 RHS 引用，再登记 LHS
        for stmt in func_def.body:
            if isinstance(stmt, ast.Assign):
                # 收集 RHS 中所有的 Name.Load 引用
                rhs_names: set[str] = set()
                for node in ast.walk(stmt.value):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        rhs_names.add(node.id)

                # 检查引用是否已在之前定义
                undefined = rhs_names - assigned - builtins
                assert not undefined, (
                    f"语句引用了未定义变量 {undefined}——"
                    f"变量必须先定义后引用"
                )

                # 登记 LHS 定义的变量
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        assigned.add(target.id)

    def test_return_points_to_output_node(self):
        """return 语句指向 output_var。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
        )
        resolved = resolve_codegen_aliases(plan)
        compiled = SparkCompiler().compile(plan)

        expected_return = f"return {resolved.output_var}"
        assert expected_return in compiled.raw_pyspark, (
            f"期望 return {resolved.output_var}，代码中未找到"
        )

    def test_project_window_sort_limit_all_use_current_lineage(self):
        """Project/Window/Sort/Limit 统一使用当前 lineage——Mapper 产出可编译。

        Read(od) → Filter → Window → Project → Sort → Limit
        所有单输入步骤的 input_alias 均由 _chain_input_aliases 填充，
        使用当前 lineage key（Read alias 或前驱步骤所追踪的 key）。
        """
        from tianshu_datadev.artifacts.models import (
            ContractAggregation,
            ContractInputTable,
            ContractOutputColumn,
            ContractSort,
            DataTransformContractV1,
            WindowSpecSummary,
        )
        from tianshu_datadev.artifacts.models import (
            ContractLimit as ContractLimitModel,
        )

        program_id = "prog_test_lineage_001"
        contract = DataTransformContractV1(
            contract_id=DataTransformContractV1.generate_contract_id(program_id),
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(table_ref="od", source_table="dwd.order_detail"),
            ],
            input_columns=[],
            filters=[],
            join_relationships=[],
            aggregations=[
                ContractAggregation(function="COUNT", input_column=None, alias="cnt"),
            ],
            grouping_keys=["name"],
            output_columns=[
                ContractOutputColumn(column_name="name", alias="name"),
                ContractOutputColumn(column_name="cnt", alias="cnt"),
            ],
            output_grain=["name"],
            sort_spec=[ContractSort(column="name", direction="ASC")],
            limit_spec=ContractLimitModel(limit=100),
            case_when_labels=[],
            window_specs=[
                WindowSpecSummary(
                    statement_id="stmt_1",
                    function="ROW_NUMBER",
                    alias="rn",
                    input_column=None,
                    partition_by=["name"],
                    order_by=["cnt"],
                ),
            ],
        )

        result = map_contract_to_spark_plan(contract)
        assert result.success is True, f"映射失败：{result.gaps}"
        plan = result.spark_plan

        # 编译应成功
        compiler = SparkCompiler()
        compiled = compiler.compile(plan)
        code = compiled.raw_pyspark

        # DataFrame 变量名只有 tN/fN——用 ast.parse 收集，确保非空
        df_vars = _collect_df_assignments(code)
        assert len(df_vars) >= 1, "应至少收集到 1 个 DataFrame 赋值变量"
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}"
            )
        # 不含旧式别名作为左值
        assert "_filtered =" not in code
        assert "_with_" not in code

    def test_tn_fn_code_var_in_alias_blocked(self):
        """tN/fN 代码变量名出现在 input_alias 中 → 校验阻断。

        确保 Mapper 不会将代码生成变量名泄漏到 SparkPlan 的依赖字段。
        """
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            # t1 是代码变量名，不应出现在 input_alias 中
            SparkFilterStep(input_alias="t1", operator="GT", left="od.amount", right="0"),
        )
        # 直接构造的 plan 绕过了 Mapper 校验，但 resolver 仍应能解析
        # （因为 t1 恰好在 latest 中作为 od 的输出变量）
        # 真正的防线在 Mapper 的 _validate_step_aliases
        from tianshu_datadev.spark.mapper import _validate_step_aliases
        errors = _validate_step_aliases(list(plan.steps), {"od"})
        assert len(errors) >= 1, f"应检测到 t1 代码变量名，实际 errors: {errors}"
        assert any("tN/fN" in e for e in errors), (
            f"错误应提及 tN/fN 禁止模式，实际: {errors}"
        )


# ════════════════════════════════════════════
# 业务别名回归测试——合法别名不得被误伤
# ════════════════════════════════════════════


class TestBusinessAliasRegression:
    """合法业务别名（含下划线）必须正确映射和编译——不被旧式后缀检查误伤。"""

    def test_read_alias_orders_filtered_compiles(self):
        """Read alias='orders_filtered' 必须映射和编译成功。

        合法 lineage key 的唯一事实源是已声明的 Read alias 集合——
        如果 alias 属于 read_aliases，即使名称含 _filtered 也必须合法。
        """
        plan = _make_plan(
            SparkReadStep(alias="orders_filtered", source_name="orders", input_key="orders_filtered"),
            SparkFilterStep(
                input_alias="orders_filtered", operator="GT",
                left="orders_filtered.amount", right="0",
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        compiled = SparkCompiler().compile(plan)

        # 编译成功，DataFrame 变量为 tN/fN
        df_vars = _collect_df_assignments(compiled.raw_pyspark)
        assert len(df_vars) >= 1, "应至少收集到 1 个 DataFrame 赋值变量"
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}"
            )
        # input_vars 正确引用 t1
        assert resolved.steps[1].input_vars == ("t1",)

    def test_read_alias_fact_with_tax_compiles(self):
        """Read alias='fact_with_tax' 必须映射和编译成功。

        含 _with_ 的合法业务别名不得被误伤——唯一事实源是 Read alias 集合。
        """
        plan = _make_plan(
            SparkReadStep(alias="fact_with_tax", source_name="fact_table", input_key="fact_with_tax"),
            SparkFilterStep(
                input_alias="fact_with_tax", operator="GT",
                left="fact_with_tax.amount", right="0",
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        compiled = SparkCompiler().compile(plan)

        df_vars = _collect_df_assignments(compiled.raw_pyspark)
        assert len(df_vars) >= 1, "应至少收集到 1 个 DataFrame 赋值变量"
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}"
            )
        assert resolved.steps[1].input_vars == ("t1",)

    def test_input_alias_ft_filtered_not_in_read_aliases_blocked(self):
        """input_alias='ft_filtered' 且它不属于 Read aliases 时必须被阻断。

        ft_filtered 不在 read_aliases={ft, tz} 中 → 返回 BLOCKING gap。
        不得尝试解析、拆分或兼容旧式派生别名。
        """
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_table", input_key="ft"),
            SparkReadStep(alias="tz", source_name="timezone", input_key="tz"),
            SparkFilterStep(
                input_alias="ft_filtered", operator="GT",
                left="ft.amount", right="0",
            ),
        )
        from tianshu_datadev.spark.mapper import _validate_step_aliases
        errors = _validate_step_aliases(list(plan.steps), {"ft", "tz"})
        assert len(errors) >= 1, (
            f"应检测到不在 Read alias 集合中的 'ft_filtered'，实际 errors: {errors}"
        )
        assert any("ft_filtered" in e for e in errors), (
            f"错误应提及未知 alias 'ft_filtered'，实际: {errors}"
        )

    def test_input_alias_t1_and_f2_blocked(self):
        """input_alias='t1' 和 'f2' 必须被阻断——tN/fN 不得进入 SparkPlan 依赖字段。

        分别测试 t1（匹配 tN）和 f2（匹配 fN），确保两种模式均被覆盖。
        """
        from tianshu_datadev.spark.mapper import _validate_step_aliases

        # 测试 t1
        plan_t1 = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="t1", operator="GT", left="od.amount", right="0"),
        )
        errors_t1 = _validate_step_aliases(list(plan_t1.steps), {"od"})
        assert len(errors_t1) >= 1, f"t1 应被阻断，实际 errors: {errors_t1}"
        assert any("tN/fN" in e for e in errors_t1), (
            f"错误应提及 tN/fN 禁止模式，实际: {errors_t1}"
        )

        # 测试 f2
        plan_f2 = _make_plan(
            SparkReadStep(alias="od", source_name="orders", input_key="od"),
            SparkFilterStep(input_alias="f2", operator="GT", left="od.amount", right="0"),
        )
        errors_f2 = _validate_step_aliases(list(plan_f2.steps), {"od"})
        assert len(errors_f2) >= 1, f"f2 应被阻断，实际 errors: {errors_f2}"
        assert any("tN/fN" in e for e in errors_f2), (
            f"错误应提及 tN/fN 禁止模式，实际: {errors_f2}"
        )

    def test_ast_collects_real_variables_and_return(self):
        """ast.parse 实际收集到 t1/t2/f1...，并验证最终 return 指向最后一个关系变量。

        使用真实链路 Read(ft,tz) → Filter → Join → Aggregate，
        确保收集结果非空且 return 指向 resolved.output_var。
        """
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_table", input_key="ft"),
            SparkReadStep(alias="tz", source_name="timezone", input_key="tz"),
            SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="0"),
            SparkJoinStep(
                left_alias="ft", right_alias="tz",
                left_key="tz_id", right_key="id",
                join_type=SparkJoinType.LEFT,
            ),
            SparkAggregateStep(
                input_alias="ft",
                group_keys=["tz_id"],
                metrics=[SparkAggregateSpec(
                    function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
                )],
            ),
        )
        resolved = resolve_codegen_aliases(plan)
        compiled = SparkCompiler().compile(plan)

        df_vars = _collect_df_assignments(compiled.raw_pyspark)
        # Read(ft,tz) → Filter → Join → Aggregate：至少 t1,t2,f1,f2,f3
        assert len(df_vars) >= 4, (
            f"应至少收集到 4 个 DataFrame 变量（t1,t2,f1,f2...），实际: {df_vars}"
        )

        # 所有变量为 tN/fN
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}"
            )

        # return 指向最后一个关系变量（resolved.output_var）
        assert f"return {resolved.output_var}" in compiled.raw_pyspark, (
            f"return 应指向 {resolved.output_var}，"
            f"实际代码末段: {compiled.raw_pyspark[-100:]}"
        )

    def test_mapper_validates_orders_filtered_as_legal_alias(self):
        """Mapper 别名校验——Read alias='orders_filtered' 通过 _validate_step_aliases。

        直接验证：_validate_step_aliases 不拒绝属于 read_aliases 的合法业务别名，
        即使其名称含旧式后缀子串。
        """
        plan = _make_plan(
            SparkReadStep(alias="orders_filtered", source_name="orders", input_key="orders_filtered"),
            SparkReadStep(alias="fact_with_tax", source_name="fact", input_key="fact_with_tax"),
            SparkFilterStep(
                input_alias="orders_filtered", operator="GT",
                left="orders_filtered.amount", right="0",
            ),
            SparkJoinStep(
                left_alias="orders_filtered", right_alias="fact_with_tax",
                left_key="id", right_key="id",
                join_type=SparkJoinType.INNER,
            ),
        )
        from tianshu_datadev.spark.mapper import _validate_step_aliases
        errors = _validate_step_aliases(
            list(plan.steps), {"orders_filtered", "fact_with_tax"},
        )
        assert len(errors) == 0, (
            f"合法业务别名不应被拒绝，实际 errors: {errors}"
        )
