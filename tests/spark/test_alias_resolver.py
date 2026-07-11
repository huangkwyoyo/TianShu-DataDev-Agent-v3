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
