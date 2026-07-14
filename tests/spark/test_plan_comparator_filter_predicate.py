"""PlanComparator 测试——Filter 谓词等价性 + 右侧归一化。

从 test_plan_comparator.py 拆分（Phase 6.2 Comparator 文件拆分）。
公共构建器见 tests/spark/plan_comparator_fixtures.py。
"""

from __future__ import annotations

from tests.spark.plan_comparator_fixtures import (
    _make_spark_filter_step,
    _make_spark_plan,
    _make_spark_read_step,
    _make_sql_filter_step,
    _make_sql_plan,
    _make_sql_scan_step,
)
from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    PredicateOperator,
    SqlLiteral,
)
from tianshu_datadev.planning.sql_build_plan import (
    FilterStep,
    ScanStep,
)
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkReadStep,
    SparkStepType,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.spark.plan_equivalence import EquivalenceVerdict


class TestPlanComparatorFilterEquivalence:
    """Filter 逻辑等价性对比。"""

    def test_filter_equivalent(self):
        """相同过滤条件 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_filter_step(),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                _make_spark_filter_step(),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_not_equivalent(self):
        """不同过滤操作符 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_filter_step(),
            ]
        )
        # Spark 侧用 EQ 而非 GT
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkFilterStep(
                    step_type=SparkStepType.FILTER,
                    input_alias="od",
                    operator="EQ",
                    left="amount",
                    right="threshold",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_filter_between_equivalent_different_literal_formats(self):
        """BETWEEN 右值列表表示形式不同但值相同 → LOGIC_EQUIVALENT。

        SQL 侧 right 是 SqlLiteral 对象列表（model_dump 后为 dict 列表），
        Spark 侧 right 是 Python repr 字符串（Mapper 直传 ContractPredicate.right）。
        两种形式在语义上等价——Comparator 应归一化后判定为等价。
        """
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        # SQL 侧：BETWEEN 右值为 SqlLiteral 对象列表
        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft",
                column_name="pickup_date_key",
                normalized_name="pickup_date_key",
            ),
            operator=PredicateOperator.BETWEEN,
            right=[
                SqlLiteral(value="20260101", is_sql_expr=False),
                SqlLiteral(value="20260331", is_sql_expr=False),
            ],
        )
        sql_filter = FilterStep(
            step_type="filter",
            step_id="step_filter_between",
            predicate=sql_predicate,
        )

        # Spark 侧：BETWEEN 右值为 Python repr 字符串（模拟 Mapper 产出）
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="BETWEEN",
            left="ft.pickup_date_key",
            right="[SqlLiteral(value='20260101', is_sql_expr=False),"
            " SqlLiteral(value='20260331', is_sql_expr=False)]",
        )

        sql_plan = _make_sql_plan(
            [
                ScanStep(
                    step_type="scan",
                    step_id="scan_ft",
                    table_ref="ft",
                    required_columns=[
                        ColumnRef(
                            table_ref="ft",
                            column_name="pickup_date_key",
                            normalized_name="pickup_date_key",
                        ),
                    ],
                ),
                sql_filter,
            ]
        )
        spark_plan = _make_spark_plan(
            [
                SparkReadStep(
                    step_type=SparkStepType.READ,
                    alias="ft",
                    source_name="fact_trips",
                    input_key="fact_trips_key",
                ),
                spark_filter,
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 关键断言：BETWEEN 右值只是表示形式不同（dict vs SqlLiteral repr），
        # 值相同 → 应判定为 LOGIC_EQUIVALENT
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"BETWEEN 右值不同表示形式应归一化后等价，"
            f"实际 status={report.status}，"
            f"filter_result={[(r.step_type, r.verdict.value, r.detail[:100]) for r in report.step_results]}"
        )

    def test_filter_in_equivalent(self):
        """IN 操作符双向等价——多元素列表排序后规范字符串一致。"""
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft", column_name="status",
                normalized_name="status",
            ),
            operator=PredicateOperator.IN,
            right=[
                SqlLiteral(value="paid", is_sql_expr=False),
                SqlLiteral(value="shipped", is_sql_expr=False),
            ],
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_in",
            predicate=sql_predicate,
        )
        # Spark 侧 IN 右值为 Python repr 字符串（模拟 Mapper 产出）
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="IN",
            left="ft.status",
            right="[SqlLiteral(value='shipped', is_sql_expr=False),"
            " SqlLiteral(value='paid', is_sql_expr=False)]",
        )
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft", table_ref="ft",
                required_columns=[
                    ColumnRef(table_ref="ft", column_name="status",
                              normalized_name="status"),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="ft",
                source_name="fact_table", input_key="fact_table_key",
                required_columns=["status"],
            ),
            spark_filter,
        ])
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        # IN 列表排序后应等价——[paid,shipped] vs [shipped,paid] → 排序后同为 [paid,shipped]
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_not_in_equivalent(self):
        """NOT_IN 操作符双向等价——否定语义 + 列表排序。"""
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft", column_name="status",
                normalized_name="status",
            ),
            operator=PredicateOperator.NOT_IN,
            right=[
                SqlLiteral(value="cancelled", is_sql_expr=False),
                SqlLiteral(value="returned", is_sql_expr=False),
            ],
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_not_in",
            predicate=sql_predicate,
        )
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="NOT_IN",
            left="ft.status",
            right="[SqlLiteral(value='returned', is_sql_expr=False),"
            " SqlLiteral(value='cancelled', is_sql_expr=False)]",
        )
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft", table_ref="ft",
                required_columns=[
                    ColumnRef(table_ref="ft", column_name="status",
                              normalized_name="status"),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="ft",
                source_name="fact_table", input_key="fact_table_key",
                required_columns=["status"],
            ),
            spark_filter,
        ])
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_is_null_equivalent(self):
        """IS_NULL 单目操作符——right=None → <NULL> 渲染与 Spark 侧一致。"""
        from tianshu_datadev.planning.models import Predicate

        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft", column_name="remark",
                normalized_name="remark",
            ),
            operator=PredicateOperator.IS_NULL,
            right=None,
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_is_null",
            predicate=sql_predicate,
        )
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="IS_NULL",
            left="ft.remark",
            right="<NULL>",
        )
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft", table_ref="ft",
                required_columns=[
                    ColumnRef(table_ref="ft", column_name="remark",
                              normalized_name="remark"),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="ft",
                source_name="fact_table", input_key="fact_table_key",
                required_columns=["remark"],
            ),
            spark_filter,
        ])
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_is_not_null_equivalent(self):
        """IS_NOT_NULL 否定单目操作符——右侧均映射为 <NULL> 后等价。"""
        from tianshu_datadev.planning.models import Predicate

        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft", column_name="remark",
                normalized_name="remark",
            ),
            operator=PredicateOperator.IS_NOT_NULL,
            right=None,
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_is_not_null",
            predicate=sql_predicate,
        )
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="IS_NOT_NULL",
            left="ft.remark",
            right="<NULL>",
        )
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft", table_ref="ft",
                required_columns=[
                    ColumnRef(table_ref="ft", column_name="remark",
                              normalized_name="remark"),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="ft",
                source_name="fact_table", input_key="fact_table_key",
                required_columns=["remark"],
            ),
            spark_filter,
        ])
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_like_equivalent(self):
        """LIKE 操作符双向等价——字符串模式匹配保留原样。"""
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft", column_name="name",
                normalized_name="name",
            ),
            operator=PredicateOperator.LIKE,
            right=SqlLiteral(value="%pattern%", is_sql_expr=False),
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_like",
            predicate=sql_predicate,
        )
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="LIKE",
            left="ft.name",
            right="%pattern%",
        )
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft", table_ref="ft",
                required_columns=[
                    ColumnRef(table_ref="ft", column_name="name",
                              normalized_name="name"),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="ft",
                source_name="fact_table", input_key="fact_table_key",
                required_columns=["name"],
            ),
            spark_filter,
        ])
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_nested_predicate_tree_rendered_and_compared(self):
        """嵌套 AND/OR 谓词 → 通过 PREDICATE_TREE 正确渲染并对比。"""
        # 构造嵌套谓词：OR( AND(a > 1, b < 10), EQ(c, 0) )
        # 即 WHERE (a > 1 AND b < 10) OR c = 0
        nested_pred = Predicate(
            left=Predicate(
                left=Predicate(
                    left=ColumnRef(table_ref="t", column_name="a", normalized_name="a"),
                    operator=PredicateOperator.GT,
                    right=SqlLiteral(value="1"),
                ),
                operator=PredicateOperator.AND,
                right=Predicate(
                    left=ColumnRef(table_ref="t", column_name="b", normalized_name="b"),
                    operator=PredicateOperator.LT,
                    right=SqlLiteral(value="10"),
                ),
            ),
            operator=PredicateOperator.OR,
            right=Predicate(
                left=ColumnRef(table_ref="t", column_name="c", normalized_name="c"),
                operator=PredicateOperator.EQ,
                right=SqlLiteral(value="0"),
            ),
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            FilterStep(
                step_type="filter",
                step_id="step_filter_nested",
                predicate=nested_pred,
            ),
        ])
        # 提取 SQL 侧 step 数据，验证扁平化后 nested predicate 正确渲染
        sql_steps = PlanComparator._extract_sql_step_data(sql_plan)

        # 验证 SQL 侧 filter step 的 left 不是空字符串（缺陷 2 根因）
        sql_filters = [s for s in sql_steps if s.get("step_type") == "filter"]
        assert len(sql_filters) == 1
        # 嵌套谓词扁平化后 left 应为规范字符串，非空
        assert sql_filters[0].get("left", "") != ""
        # operator 应为 PREDICATE_TREE
        assert sql_filters[0].get("operator", "") == "PREDICATE_TREE"
        # right 应为空（PREDICATE_TREE 模式下右值无意义）
        assert sql_filters[0].get("right", "") == ""

    def test_filter_nested_predicate_right_side(self):
        """右侧为嵌套 Predicate tree → 不崩溃，正确递归渲染并对比。

        Predicate.right 可以是嵌套 Predicate（模型允许 right: Predicate）。
        _flatten_filter_step 应正确检测 _is_predicate_tree 并调用
        _render_predicate_tree 递归渲染，而非走 _column_ref_to_string 产生空结果。
        """
        # 构造谓词：EQ(amount, GT(threshold, 100))
        # left 为 ColumnRef，right 为嵌套 Predicate tree
        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="od", column_name="amount",
                normalized_name="amount",
            ),
            operator=PredicateOperator.EQ,
            right=Predicate(
                left=ColumnRef(
                    table_ref="od", column_name="threshold",
                    normalized_name="threshold",
                ),
                operator=PredicateOperator.GT,
                right=SqlLiteral(value=100),
            ),
        )
        sql_filter = FilterStep(
            step_type="filter", step_id="step_filter_nested_right",
            predicate=sql_predicate,
        )
        # Spark 侧：right 为嵌套 tree 渲染后的规范字符串
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="od",
            operator="EQ",
            left="od.amount",
            right="(threshold GT 100)",
        )
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            spark_filter,
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 关键断言：不崩溃，filter step 有有效 verdict
        filter_results = [r for r in report.step_results if r.step_type == "filter"]
        assert len(filter_results) == 1
        # nested_predicate_right 渲染后应与 Spark 侧匹配
        assert filter_results[0].verdict == EquivalenceVerdict.EQUIVALENT, (
            f"嵌套 Predicate right 渲染应与 Spark 侧匹配，"
            f"实际 verdict={filter_results[0].verdict.value}, "
            f"detail={filter_results[0].detail[:200]}"
        )

    def test_not_predicate_rendered_correctly(self):
        """NOT 谓词 → 渲染结果包含 NOT 标记。"""
        not_pred = Predicate(
            left=Predicate(
                left=ColumnRef(table_ref="t", column_name="a", normalized_name="a"),
                operator=PredicateOperator.GT,
                right=SqlLiteral(value="1"),
            ),
            operator=PredicateOperator.NOT,
            right=None,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            FilterStep(
                step_type="filter",
                step_id="step_filter_not",
                predicate=not_pred,
            ),
        ])

        from tianshu_datadev.spark.plan_comparator import PlanComparator
        sql_steps = PlanComparator._extract_sql_step_data(sql_plan)

        sql_filters = [s for s in sql_steps if s.get("step_type") == "filter"]
        assert len(sql_filters) == 1
        rendered_left = sql_filters[0].get("left", "")
        # NOT 标记不应丢失
        assert "NOT" in rendered_left.upper()
        assert sql_filters[0].get("operator", "") == "PREDICATE_TREE"

    def test_between_list_preserves_order_in_predicate_tree(self):
        """BETWEEN 右值列表不可交换——[1,10] 和 [10,1] 应渲染为不同字符串。

        IN/NOT_IN 列表可交换（语义等效），BETWEEN [low, high] 不可交换——
        BETWEEN 10 AND 1 在 SQL 中恒为空集，排序后错误地等价于 BETWEEN 1 AND 10。
        """
        from tianshu_datadev.spark.plan_comparator import PlanComparator

        # 构造嵌套谓词：AND(col > 0, BETWEEN(col, [1, 10]))
        between_low_high = Predicate(
            left=ColumnRef(table_ref="t", column_name="a", normalized_name="a"),
            operator=PredicateOperator.GT,
            right=SqlLiteral(value="0"),
        )
        between_10_1 = Predicate(
            left=ColumnRef(table_ref="t", column_name="a", normalized_name="a"),
            operator=PredicateOperator.BETWEEN,
            right=[SqlLiteral(value="10"), SqlLiteral(value="1")],
        )
        nested_between_reversed = Predicate(
            left=between_low_high,
            operator=PredicateOperator.AND,
            right=between_10_1,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            FilterStep(
                step_type="filter",
                step_id="step_filter_between",
                predicate=nested_between_reversed,
            ),
        ])
        sql_steps = PlanComparator._extract_sql_step_data(sql_plan)
        sql_filters = [s for s in sql_steps if s.get("step_type") == "filter"]
        assert len(sql_filters) == 1
        rendered = sql_filters[0].get("left", "")

        # BETWEEN 保序：[10,1] 不应被排序成 [1,10]
        assert "[10,1]" in rendered, (
            f"BETWEEN [10,1] 应保序不排序，实际渲染：{rendered}"
        )

    def test_between_normalization_with_datetime_spaces(self):
        """BETWEEN 右值含空格时间戳——正则应完整提取不截断。

        回归验证 IM-02：捕获组移除 \\s 后，datetime 值如
        "2026-01-01 00:00:00" 应被完整提取，而非在空格处截断为 "2026-01-01"。
        """
        from tianshu_datadev.spark.plan_comparator import PlanComparator

        # 构造含空格时间戳的 Spark 侧 repr 字符串
        right_str = (
            "[SqlLiteral(value='2026-01-01 00:00:00', is_sql_expr=False),"
            " SqlLiteral(value='2026-01-31 23:59:59', is_sql_expr=False)]"
        )
        result = PlanComparator._normalize_between_right_string(right_str)
        # 应完整提取含空格的时间戳
        assert "2026-01-01 00:00:00" in result, (
            f"时间戳含空格值应完整提取，实际：{result}"
        )
        assert "2026-01-31 23:59:59" in result, (
            f"时间戳含空格值应完整提取，实际：{result}"
        )

    def test_between_datetime_full_comparison(self):
        """端到端验证含空格时间戳的 BETWEEN filter 双向等价。

        SQL 侧 right 为 SqlLiteral 列表，Spark 侧 right 为 Python repr 字符串——
        两边的 BETWEEN 右值都含带空格的时间戳，正则提取后应被正确归一化。
        """
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        # SQL 侧：BETWEEN 右值为 SqlLiteral 对象列表（含空格时间戳）
        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft",
                column_name="pickup_datetime",
                normalized_name="pickup_datetime",
            ),
            operator=PredicateOperator.BETWEEN,
            right=[
                SqlLiteral(value="2026-01-01 00:00:00", is_sql_expr=False),
                SqlLiteral(value="2026-01-31 23:59:59", is_sql_expr=False),
            ],
        )
        sql_filter = FilterStep(
            step_type="filter",
            step_id="step_filter_between_dt",
            predicate=sql_predicate,
        )

        # Spark 侧：BETWEEN 右值为 Python repr 字符串（模拟 Mapper 产出）
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="BETWEEN",
            left="ft.pickup_datetime",
            right="[SqlLiteral(value='2026-01-01 00:00:00', is_sql_expr=False),"
            " SqlLiteral(value='2026-01-31 23:59:59', is_sql_expr=False)]",
        )

        sql_plan = _make_sql_plan(
            [
                ScanStep(
                    step_type="scan",
                    step_id="scan_ft",
                    table_ref="ft",
                    required_columns=[
                        ColumnRef(
                            table_ref="ft",
                            column_name="pickup_datetime",
                            normalized_name="pickup_datetime",
                        ),
                    ],
                ),
                sql_filter,
            ]
        )
        spark_plan = _make_spark_plan(
            [
                SparkReadStep(
                    step_type=SparkStepType.READ,
                    alias="ft",
                    source_name="fact_table",
                    input_key="fact_table_key",
                    required_columns=["pickup_datetime"],
                ),
                spark_filter,
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

class TestFilterRightPredicateTreeAux:
    """_flatten_filter_step dict 层辅助测试（非验收路径）。

    直接测试 _flatten_filter_step 对右侧嵌套 Predicate tree 的 dict 输入的处理，
    不经过 PlanComparator.compare() 全管道。
    """

    def test_right_is_predicate_tree_rendered(self):
        """right 为嵌套 Predicate tree dict → 递归渲染为规范字符串。"""
        step_dict = {
            "step_type": "filter",
            "step_id": "step_test",
            "predicate": {
                "left": {
                    "table_ref": "od",
                    "column_name": "amount",
                    "normalized_name": "amount",
                },
                "operator": "EQ",
                "right": {
                    "left": {
                        "table_ref": "od",
                        "column_name": "threshold",
                        "normalized_name": "threshold",
                    },
                    "operator": "GT",
                    "right": {"value": 100},
                },
            },
        }
        result = PlanComparator._flatten_filter_step(step_dict)

        assert result["left"] == "od.amount"
        assert result["operator"] == "EQ"
        # right 应为递归渲染后的规范字符串，而非空（原 bug 表现）
        assert result["right"] == "(threshold GT 100)", (
            f"右侧嵌套 Predicate tree 应递归渲染为规范字符串，"
            f"实际 right={result['right']!r}"
        )

    def test_right_is_column_ref_unchanged(self):
        """right 为 ColumnRef dict → 走原 _column_ref_to_string 路径（回归）。"""
        step_dict = {
            "step_type": "filter",
            "step_id": "step_test",
            "predicate": {
                "left": {
                    "table_ref": "od",
                    "column_name": "amount",
                    "normalized_name": "amount",
                },
                "operator": "GT",
                "right": {
                    "table_ref": "od",
                    "column_name": "threshold",
                    "normalized_name": "threshold",
                },
            },
        }
        result = PlanComparator._flatten_filter_step(step_dict)

        # 普通 ColumnRef → 原路径，right 应为 "od.threshold"
        assert result["right"] == "od.threshold", (
            f"ColumnRef right 应走原 _column_ref_to_string 路径，"
            f"实际 right={result['right']!r}"
        )

    def test_right_is_predicate_tree_left_is_predicate_tree(self):
        """left 和 right 均为嵌套 Predicate tree → left 分支短路（原逻辑不变）。"""
        step_dict = {
            "step_type": "filter",
            "step_id": "step_test",
            "predicate": {
                "left": {
                    "left": {
                        "table_ref": "t",
                        "column_name": "a",
                        "normalized_name": "a",
                    },
                    "operator": "GT",
                    "right": {"value": "1"},
                },
                "operator": "AND",
                "right": {
                    "left": {
                        "table_ref": "t",
                        "column_name": "b",
                        "normalized_name": "b",
                    },
                    "operator": "LT",
                    "right": {"value": "10"},
                },
            },
        }
        result = PlanComparator._flatten_filter_step(step_dict)

        # left 是 predicate tree → left 分支短路，operator 为 PREDICATE_TREE
        assert result["operator"] == "PREDICATE_TREE"
        assert result["left"] != ""
        # right 应为空（PREDICATE_TREE 模式下无意义）
        assert result["right"] == ""

class TestNormalizeFilterRights:
    """_normalize_filter_rights 单测——BETWEEN/IN/IS_NULL 右值规范化。

    覆盖三种场景：
    1. BETWEEN/IN/NOT_IN——列表/字符串 right 统一为 [v1,v2,...]
    2. IS_NULL/IS_NOT_NULL——right 统一为 <NULL> 占位符
    3. 非 filter step 或非特殊操作符——保持原样
    """

    # ── BETWEEN：列表 right（SQL 侧）─────────────────────────────

    def test_between_list_right_normalized(self):
        """BETWEEN + 列表 right → 提取 value 保序 [v1,v2]。
        SQL 侧 SqlLiteral 列表序列化为 [{'value': '...'}, ...]。"""
        steps = [
            {"step_type": "filter", "operator": "BETWEEN",
             "right": [{"value": "10"}, {"value": "20"}]},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[10,20]"

    def test_between_list_right_preserves_order(self):
        """BETWEEN right 保序——[low, high] 不可交换。"""
        steps = [
            {"step_type": "filter", "operator": "BETWEEN",
             "right": [{"value": "20"}, {"value": "10"}]},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[20,10]", "BETWEEN 应保序"

    # ── BETWEEN：字符串 right（Spark 侧）────────────────────────

    def test_between_string_right_normalized(self):
        """BETWEEN + Spark repr 字符串 right → 提取 value 为 [v1,v2]。"""
        steps = [
            {"step_type": "filter", "operator": "BETWEEN",
             "right": "[SqlLiteral(value='100', ...), SqlLiteral(value='500', ...)]"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[100,500]"

    def test_between_string_right_with_spaces(self):
        """BETWEEN 字符串 right 值含空格（如 datetime）→ 完整提取。"""
        steps = [
            {"step_type": "filter", "operator": "BETWEEN",
             "right": "[SqlLiteral(value='2026-01-01 00:00:00', ...), "
                      "SqlLiteral(value='2026-12-31 23:59:59', ...)]"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[2026-01-01 00:00:00,2026-12-31 23:59:59]"

    # ── IN：列表 right ─────────────────────────────────────────

    def test_in_list_right_sorted(self):
        """IN + 列表 right → 提取 value 并排序。"""
        steps = [
            {"step_type": "filter", "operator": "IN",
             "right": [{"value": "c"}, {"value": "a"}, {"value": "b"}]},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[a,b,c]"

    def test_not_in_list_right_sorted(self):
        """NOT_IN + 列表 right → 提取 value 并排序。"""
        steps = [
            {"step_type": "filter", "operator": "NOT_IN",
             "right": [{"value": "z"}, {"value": "x"}, {"value": "y"}]},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[x,y,z]"

    # ── IN：字符串 right（Spark 侧）─────────────────────────────

    def test_in_string_right_extracted_and_sorted(self):
        """IN + 字符串 right（Spark repr）→ 提取后排序。"""
        steps = [
            {"step_type": "filter", "operator": "IN",
             "right": "[SqlLiteral(value='b', ...), "
                      "SqlLiteral(value='a', ...), SqlLiteral(value='c', ...)]"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "[a,b,c]"

    # ── IS_NULL / IS_NOT_NULL ──────────────────────────────────

    def test_is_null_right_becomes_null_placeholder(self):
        """IS_NULL → right 统一为 <NULL>。"""
        steps = [
            {"step_type": "filter", "operator": "IS_NULL", "right": ""},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "<NULL>"

    def test_is_not_null_right_becomes_null_placeholder(self):
        """IS_NOT_NULL → right 统一为 <NULL>（Spark 侧可能有任意值）。"""
        steps = [
            {"step_type": "filter", "operator": "IS_NOT_NULL",
             "right": "whatever_spark_outputs"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "<NULL>"

    def test_is_null_none_right_becomes_null_placeholder(self):
        """IS_NULL + right=None（SQL 侧 flatten 后为 ""）→ <NULL>。"""
        steps = [
            {"step_type": "filter", "operator": "IS_NULL", "right": None},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "<NULL>"

    # ── 非 filter step / 非特殊操作符 ──────────────────────────

    def test_non_filter_step_untouched(self):
        """非 filter step 不修改。"""
        steps = [
            {"step_type": "scan", "right": "original"},
            {"step_type": "project", "right": "original"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "original"
        assert steps[1]["right"] == "original"

    def test_unknown_operator_untouched(self):
        """非简单比较/非列表/非单目操作符——不修改 right。

        AND/OR/NOT/PREDICATE_TREE 等复合操作符 right 值保持不变。
        """
        steps = [
            {"step_type": "filter", "operator": "AND", "right": ""},
            {"step_type": "filter", "operator": "OR", "right": ""},
            {"step_type": "filter", "operator": "PREDICATE_TREE", "right": ""},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == ""
        assert steps[1]["right"] == ""
        assert steps[2]["right"] == ""

    # ── 字符串字面量引号剥离（EQ/NEQ/GT/GTE/LT/LTE/LIKE）────────

    def test_eq_quoted_string_stripped(self):
        """EQ + 'PAID' → right 剥引号后为 PAID。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": "'PAID'"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "PAID"

    def test_gte_quoted_date_stripped(self):
        """GTE + '2025-01-01' → right 剥引号后为 2025-01-01。"""
        steps = [
            {"step_type": "filter", "operator": "GTE", "right": "'2025-01-01'"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "2025-01-01"

    def test_lte_quoted_datetime_stripped(self):
        """LTE + '2025-01-31 23:59:59' → right 剥引号后为裸值。"""
        steps = [
            {"step_type": "filter", "operator": "LTE",
             "right": "'2025-01-31 23:59:59'"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "2025-01-31 23:59:59"

    def test_like_unquoted_pattern_unchanged(self):
        """LIKE + %test%（无引号）→ 保持不变。"""
        steps = [
            {"step_type": "filter", "operator": "LIKE", "right": "%test%"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "%test%"

    def test_eq_unquoted_number_unchanged(self):
        """EQ + 100（无引号数字）→ 保持不变。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": "100"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "100"

    def test_eq_double_quoted_string_stripped(self):
        """EQ + \"value\"（双引号包裹）→ 剥去双引号。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": '"value"'},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "value"

    def test_eq_single_quote_only_start_unchanged(self):
        """仅首引号无尾引号 → 保持不变（防御畸形值）。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": "'incomplete"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "'incomplete"

    def test_eq_mixed_quotes_unchanged(self):
        """首尾引号不匹配（\"...'）→ 保持不变。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": "\"value'"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == "\"value'"

    def test_eq_empty_string_unchanged(self):
        """空字符串 right → 不修改。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": ""},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] == ""

    def test_eq_none_right_unchanged(self):
        """right=None → 不修改。"""
        steps = [
            {"step_type": "filter", "operator": "EQ", "right": None},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0]["right"] is None

    def test_ne_gt_lt_also_stripped(self):
        """NEQ/GT/LT 同样剥引号——覆盖全部简单比较操作符。"""
        for op in ("NEQ", "GT", "LT"):
            steps = [
                {"step_type": "filter", "operator": op, "right": "'val'"},
            ]
            PlanComparator._normalize_filter_rights(steps)
            assert steps[0]["right"] == "val", f"{op} 应剥引号"

    def test_empty_steps_list(self):
        """空列表不抛异常。"""
        steps: list = []
        PlanComparator._normalize_filter_rights(steps)
        assert steps == []

    def test_mixed_steps(self):
        """混合 step 类型：仅 filter 被处理。"""
        steps = [
            {"step_type": "scan"},
            {"step_type": "filter", "operator": "BETWEEN",
             "right": [{"value": "1"}, {"value": "5"}]},
            {"step_type": "filter", "operator": "IS_NULL", "right": ""},
            {"step_type": "sort"},
        ]
        PlanComparator._normalize_filter_rights(steps)
        assert steps[0].get("right") is None  # scan 未修改
        assert steps[1]["right"] == "[1,5]"
        assert steps[2]["right"] == "<NULL>"
        assert steps[3].get("right") is None  # sort 未修改
