"""Phase 6 SparkCompiler 测试——5 种 step（scan/filter/project/sort/limit）编译。

6B/6C step 类型使用 skip/xfail 占位。
"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.compiler import SparkCompileResult, SparkCompiler
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkLimitStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
)


def _make_plan(*steps) -> SparkPlan:
    """构建测试用 SparkPlan。"""
    return SparkPlan(
        plan_id="test_plan",
        version="v1",
        source_phase="phase-6",
        source_contract_hash="test_hash",
        steps=list(steps),
    )


# ════════════════════════════════════════════
# Phase 6A 测试——5 种 step
# ════════════════════════════════════════════


class TestCompileRead:
    """ReadStep 编译测试。"""

    def test_single_read(self):
        """单个 ReadStep 编译。"""
        step = SparkReadStep(
            alias="od", source_name="dwd.order_detail", input_key="od",
        )
        plan = _make_plan(step)
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'od = inputs["dwd.order_detail"]' in result.raw_pyspark
        assert "def transform(" in result.raw_pyspark
        assert len(result.step_ids) == 1

    def test_multiple_reads(self):
        """多个 ReadStep 编译。"""
        steps = [
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="ri", source_name="dim.region_info", input_key="ri"),
        ]
        plan = _make_plan(*steps)
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'od = inputs["dwd.order_detail"]' in result.raw_pyspark
        assert 'ri = inputs["dim.region_info"]' in result.raw_pyspark
        assert len(result.step_ids) == 2

    def test_read_no_spark_read(self):
        """ReadStep 不生成 spark.read.parquet()。"""
        step = SparkReadStep(
            alias="od", source_name="dwd.order_detail", input_key="od",
        )
        plan = _make_plan(step)
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "spark.read" not in result.raw_pyspark
        assert "spark.table" not in result.raw_pyspark


class TestCompileFilter:
    """FilterStep 编译测试。"""

    def test_equality_filter(self):
        """EQ 过滤条件编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.order_status", right="'paid'"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "od = inputs[" in result.raw_pyspark
        assert ".filter(" in result.raw_pyspark
        assert 'F.col("od.order_status")' in result.raw_pyspark
        assert "==" in result.raw_pyspark
        assert "'paid'" in result.raw_pyspark

    def test_comparison_filter(self):
        """GT 过滤条件编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.order_amount", right="100"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ">" in result.raw_pyspark

    def test_filter_chaining(self):
        """多个 FilterStep 链式编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkFilterStep(input_alias="_f1", operator="GT", left="od.amount", right="0"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert result.raw_pyspark.count(".filter(") == 2


class TestCompileProject:
    """ProjectStep 编译测试。"""

    def test_simple_project(self):
        """基本列投影编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkProjectStep(
                input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="stat_date", alias="stat_date"),
                    SparkProjectColumn(column_name="total_amount", alias="total_amount"),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".select(" in result.raw_pyspark
        assert 'F.col("stat_date")' in result.raw_pyspark
        assert 'F.col("total_amount")' in result.raw_pyspark

    def test_project_with_alias(self):
        """带别名投影编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkProjectStep(
                input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="order_amount", alias="amount"),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert '.alias("amount")' in result.raw_pyspark


class TestCompileSort:
    """SortStep 编译测试。"""

    def test_sort_asc(self):
        """升序排序编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkSortStep(
                input_alias="od",
                order_by=[
                    SparkSortSpec(column="stat_date", direction=SparkSortDirection.ASC),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".orderBy(" in result.raw_pyspark
        assert 'F.asc("stat_date")' in result.raw_pyspark

    def test_sort_desc(self):
        """降序排序编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkSortStep(
                input_alias="od",
                order_by=[
                    SparkSortSpec(column="total_amount", direction=SparkSortDirection.DESC),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'F.desc("total_amount")' in result.raw_pyspark

    def test_sort_multiple_keys(self):
        """多键排序编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkSortStep(
                input_alias="od",
                order_by=[
                    SparkSortSpec(column="stat_date", direction=SparkSortDirection.DESC),
                    SparkSortSpec(column="region_code", direction=SparkSortDirection.ASC),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'F.desc("stat_date")' in result.raw_pyspark
        assert 'F.asc("region_code")' in result.raw_pyspark


class TestCompileLimit:
    """LimitStep 编译测试。"""

    def test_limit(self):
        """行限制编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkLimitStep(input_alias="od", limit=100),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".limit(100)" in result.raw_pyspark

    def test_limit_after_sort(self):
        """排序后限制——TOP N 模式。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkSortStep(
                input_alias="od",
                order_by=[
                    SparkSortSpec(column="total_amount", direction=SparkSortDirection.DESC),
                ],
            ),
            SparkLimitStep(input_alias="_s1", limit=10),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".orderBy(" in result.raw_pyspark
        assert ".limit(10)" in result.raw_pyspark


# ════════════════════════════════════════════
# 编译确定性测试
# ════════════════════════════════════════════


class TestCompileDeterminism:
    """编译确定性——相同输入 → 相同输出。"""

    def test_same_plan_same_output(self):
        """相同 SparkPlan 两次编译产生相同代码 hash。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
        )
        compiler = SparkCompiler()
        r1 = compiler.compile(plan)
        r2 = compiler.compile(plan)
        assert r1.raw_hash == r2.raw_hash
        assert r1.raw_pyspark == r2.raw_pyspark

    def test_raw_and_annotated_same_body(self):
        """raw 和 annotated 的执行代码相同（去除注释后）。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        # 去除注释行
        raw_lines = [l for l in result.raw_pyspark.split("\n") if not l.strip().startswith("#")]
        ann_lines = [l for l in result.annotated_pyspark.split("\n") if not l.strip().startswith("#")]
        assert raw_lines == ann_lines


# ════════════════════════════════════════════
# Phase 6B/6C skip/xfail 占位
# ════════════════════════════════════════════


class TestPhase6BUnsupported:
    """Phase 6B step 类型——占位测试。"""

    @pytest.mark.skip(reason="Phase 6B")
    def test_aggregate_skip(self):
        pass

    @pytest.mark.skip(reason="Phase 6B")
    def test_join_skip(self):
        pass

    @pytest.mark.skip(reason="Phase 6B")
    def test_case_when_skip(self):
        pass


class TestPhase6CUnsupported:
    """Phase 6C step 类型——占位测试。"""

    @pytest.mark.skip(reason="Phase 6C")
    def test_window_skip(self):
        pass
