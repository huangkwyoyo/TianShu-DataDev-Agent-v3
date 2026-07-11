"""Phase 6 SparkCompiler 测试——9 种 step 编译（6A 5 种 + 6B 3 种 + 6C 1 种）。"""

from __future__ import annotations

import re

import pytest

from tianshu_datadev.artifacts.models import CaseWhenCondition
from tianshu_datadev.spark.annotations import StepAnnotation, StepIntent
from tianshu_datadev.spark.compiler import SparkCompiler
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
)
from tianshu_datadev.spark.renderer import RenderError


def _make_plan(*steps) -> SparkPlan:
    """构建测试用 SparkPlan——无 Read 时自动前置默认 ReadStep。

    保证所有步骤的 input_alias 都能在 latest 中解析。
    使用 alias="df" 与多数 Window 测试的参数名一致。
    """
    steps_list = list(steps)
    has_read = any(isinstance(s, SparkReadStep) for s in steps_list)
    if not has_read:
        steps_list.insert(
            0,
            SparkReadStep(alias="df", source_name="default_table", input_key="default"),
        )
    return SparkPlan(
        plan_id="test_plan",
        version="v1",
        source_phase="phase-6",
        source_contract_hash="test_hash",
        steps=steps_list,
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

        assert 't1 = inputs["dwd.order_detail"]' in result.raw_pyspark
        assert "def transform(" in result.raw_pyspark
        assert len(result.step_ids) == 1

    def test_multiple_reads(self):
        """多个 ReadStep 编译——tN 按 input_key 字典序分配。"""
        steps = [
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="ri", source_name="dim.region_info", input_key="ri"),
        ]
        plan = _make_plan(*steps)
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        # t1/t2 按 input_key 字典序——"od" < "ri"
        assert 't1 = inputs["dwd.order_detail"]' in result.raw_pyspark
        assert 't2 = inputs["dim.region_info"]' in result.raw_pyspark
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

        assert "t1 = inputs[" in result.raw_pyspark
        assert ".filter(" in result.raw_pyspark
        # F.col() 只接受纯列名，filter 上下文剥离了表前缀 "od."
        assert 'F.col("order_status")' in result.raw_pyspark
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
        """多个 FilterStep 链式编译——resolver 通过 latest 映射串联。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
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
            SparkLimitStep(input_alias="od", limit=10),
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
        raw_lines = [
            line for line in result.raw_pyspark.split("\n")
            if not line.strip().startswith("#")
        ]
        ann_lines = [
            line for line in result.annotated_pyspark.split("\n")
            if not line.strip().startswith("#")
        ]
        assert raw_lines == ann_lines


# ════════════════════════════════════════════
# 安全补丁：恶意输入回归测试
# ════════════════════════════════════════════


class TestMaliciousInput:
    """恶意输入在编译器/Renderer 层被安全处理——拒绝或转义。"""

    def test_source_name_with_quotes_escaped(self):
        """含双引号的 source_name——双引号被转义，输出合法 Python。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name='a"b', input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        # 输出中双引号被转义，不是裸双引号
        assert 'inputs[' in result.raw_pyspark
        assert 'a\\"b' in result.raw_pyspark

    def test_source_name_with_newline_escaped(self):
        """含换行的 source_name——换行被转义为 \\n。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="a\nb", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        # 换行被转义，代码仍在单行内
        assert "\\n" in result.raw_pyspark
        # 生成的代码不跨行
        read_line = [line for line in result.raw_pyspark.split("\n") if "inputs[" in line][0]
        assert read_line.count("inputs[") == 1

    def test_source_name_with_backslash_escaped(self):
        """含反斜杠的 source_name——反斜杠被转义。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="a\\b", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        assert "a\\\\b" in result.raw_pyspark

    def test_filter_right_exec_rejected(self):
        """filter right 含 exec()——编译时抛出 RenderError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status",
                           right="exec('rm -rf /')"),
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="危险模式"):
            compiler.compile(plan)

    def test_filter_right_spark_read_rejected(self):
        """filter right 含 spark.read——编译时抛出 RenderError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status",
                           right="spark.read.parquet('/tmp/evil')"),
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="危险模式"):
            compiler.compile(plan)

    def test_filter_right_import_rejected(self):
        """filter right 含 import——编译时抛出 RenderError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status",
                           right="import os"),
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="危险模式"):
            compiler.compile(plan)

    def test_filter_right_unpaired_quotes_rejected(self):
        """filter right 引号不配对——编译时抛出 RenderError。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status",
                           right="'paid"),
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="引号不配对"):
            compiler.compile(plan)

    # ── 注释注入测试 ──

    def test_source_name_newline_not_break_comment(self):
        """source_name 含换行——注释中换行被清洗，不会产生裸代码行。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.\nimport os\n# order_detail",
                         input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        # 核心不变式：去注释后 annotated == raw
        def _strip(code):
            return "\n".join(
                line for line in code.split("\n") if not line.lstrip().startswith("#")
            )
        assert _strip(result.annotated_pyspark) == result.raw_pyspark, (
            "含换行的 source_name 导致注释注入——去注释后 annotated 与 raw 不一致"
        )
        # 额外验证：注释行中不含原始换行（已被 render_comment_text 清洗）
        for line in result.annotated_pyspark.split("\n"):
            if line.lstrip().startswith("#"):
                assert "\n" not in line

    def test_annotated_no_bare_code_from_malicious_input(self):
        """含恶意换行+eval 的 input 不产生裸代码行——防御纵深验证。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.\neval('bad')\n# order_detail",
                         input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        # 核心不变式：去注释后 annotated == raw
        def _strip(code):
            return "\n".join(
                line for line in code.split("\n") if not line.lstrip().startswith("#")
            )
        assert _strip(result.annotated_pyspark) == result.raw_pyspark, (
            "恶意 source_name 导致注释注入"
        )
        # eval('bad') 出现在注释文本和 raw 的转义字符串中——两者均安全
        # 关键：eval( 作为 Python 代码的唯一实例在 raw 中也存在（字符串字面量内）
        raw_code_lines = [
            line for line in result.raw_pyspark.split("\n")
            if not line.lstrip().startswith("#") and "eval(" in line
        ]
        ann_non_comment_eval_lines = [
            line for line in result.annotated_pyspark.split("\n")
            if not line.lstrip().startswith("#") and "eval(" in line
        ]
        assert len(raw_code_lines) == len(ann_non_comment_eval_lines), (
            f"eval( 在非注释代码行中数量不一致："
            f"raw={len(raw_code_lines)}, annotated={len(ann_non_comment_eval_lines)}"
        )

    def test_annotated_minus_comments_equals_raw_with_malicious_source(self):
        """即使 source_name 含特殊字符，去注释后 annotated 仍与 raw 一致。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.\r\nimport\x00os\x1b# order_detail",
                         input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        # 去注释后应完全一致
        def _strip_comments(code: str) -> str:
            return "\n".join(
                line for line in code.split("\n")
                if not line.lstrip().startswith("#")
            )
        assert _strip_comments(result.annotated_pyspark) == result.raw_pyspark, (
            "恶意 source_name 导致 annotated 与 raw 不一致——注释注入风险"
        )


# ════════════════════════════════════════════
# Phase 8B 测试——LLM 语义标注注入
# ════════════════════════════════════════════


class TestAnnotationInjection:
    """LLM 语义标注注入 `_enhance_comment_with_annotation` 的验证测试。

    Phase 8C：注释简化为 Step 行 + 一句自然语言业务描述（intent_detail）。
    不再产生 Intent/Operation/Inputs/Output 结构化行。

    核心检查点：
    - intent_detail 作为自然语言注释追加在 Step 行后
    - 所有 LLM 文本经 render_comment_text 清洗（换行被移除）
    - annotation=None 时不报错
    - annotation 中含恶意换行时注释仍为单行
    """

    def _make_comment(self) -> str:
        """生成一个 Step 行注释作为测试输入。"""
        return "# Step: SparkReadStep_0（索引 1/6）"

    def _make_annotation(self, intent_detail="读取行程事实表") -> StepAnnotation:
        return StepAnnotation(
            step_id="SparkReadStep_0",
            step_index=0,
            step_type="SparkReadStep",
            intent=StepIntent.SOURCE,
            intent_detail=intent_detail,
            operation_summary="从 ft 读取数据",
        )

    def test_intent_detail_appended(self):
        """intent_detail 作为自然语言注释追加在 Step 行之后。"""
        compiler = SparkCompiler()
        ann = self._make_annotation(intent_detail="读取出租车行程事实数据表")
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        lines = result.split("\n")
        assert lines[0] == "# Step: SparkReadStep_0（索引 1/6）"
        assert lines[1] == "# 读取出租车行程事实数据表"

    def test_no_structured_lines(self):
        """增强后不含 Intent/Operation/Inputs/Output 结构化行。"""
        compiler = SparkCompiler()
        ann = self._make_annotation()
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        assert "# Intent:" not in result
        assert "# Operation:" not in result
        assert "# Inputs:" not in result
        assert "# Output:" not in result

    def test_malicious_newline_in_intent_detail_sanitized(self):
        """intent_detail 中含恶意换行——注释为单行（不产生裸代码）。"""
        compiler = SparkCompiler()
        ann = self._make_annotation(
            intent_detail="正常描述\neval('bad')\n# 注入",
        )
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        # 注释部分应被 render_comment_text 清洗掉换行，只保留 2 行（Step + 描述）
        lines = result.split("\n")
        assert len(lines) == 2, f"应为 2 行（Step + 清洗后的单行注释），实际 {len(lines)} 行"
        assert "正常描述" in result

    # ── 集成测试：E2E compile() 通过 annotation 参数 ──

    def _make_full_plan(self):
        """构建一个 3-step 计划（read + filter + project）用于集成测试。"""
        return _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkFilterStep(input_alias="ft", operator="GT",
                           left="ft.distance", right="10"),
            SparkProjectStep(input_alias="ft", columns=[
                SparkProjectColumn(column_name="trip_id", alias="trip_id"),
                SparkProjectColumn(column_name="distance", alias="distance"),
            ]),
        )

    def _make_annotations(self):
        """构建 3 个与 full_plan 步骤匹配的 StepAnnotation。"""
        return [
            StepAnnotation(
                step_id="SparkReadStep_0", step_index=0,
                step_type="SparkReadStep", intent=StepIntent.SOURCE,
                intent_detail="读取出租车行程事实数据表 ft",
                operation_summary="从 fact_trips 读取原始数据",
            ),
            StepAnnotation(
                step_id="SparkFilterStep_1", step_index=1,
                step_type="SparkFilterStep", intent=StepIntent.CLEAN,
                intent_detail="过滤距离大于 10 的行程记录",
                operation_summary="按 distance > 10 过滤",
            ),
            StepAnnotation(
                step_id="SparkProjectStep_2", step_index=2,
                step_type="SparkProjectStep", intent=StepIntent.SHAPE,
                intent_detail="选取 trip_id 和 distance 两个输出字段",
                operation_summary="投影保留 trip_id 和 distance",
            ),
        ]

    def test_compile_with_annotations_all_steps(self):
        """传入 annotations 时所有 step 的注释块包含 intent_detail。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        anns = self._make_annotations()
        result = compiler.compile(plan, annotations=anns)
        # 每个 step 的注释块应为 2 行：Step + intent_detail
        for ann in anns:
            step_comment = self._extract_step_comment(result.annotated_pyspark, ann.step_id)
            assert step_comment is not None, f"未找到 {ann.step_id} 的注释块"
            assert f"# {ann.intent_detail}" in step_comment, (
                f"{ann.step_id} 缺少 intent_detail 注释"
            )
            # 不应包含旧的结构化行
            assert "# Intent:" not in step_comment, f"{ann.step_id} 不应有 Intent 行"
            assert "# Operation:" not in step_comment, f"{ann.step_id} 不应有 Operation 行"

    def _extract_step_comment(self, code: str, step_id: str) -> str | None:
        """从编译代码中提取指定 step_id 的注释块。"""
        lines = code.split("\n")
        in_target = False
        comment_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(f"# Step: {step_id}"):
                in_target = True
                comment_lines.append(stripped)
            elif in_target:
                if stripped.startswith("#"):
                    comment_lines.append(stripped)
                else:
                    break
        return "\n".join(comment_lines) if comment_lines else None

    def test_compile_without_annotations_fallback(self):
        """annotations=None 时仅生成 Step 行，不报错。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        result = compiler.compile(plan, annotations=None)
        # 只含 Step 行，不补业务注释（无 annotation 就没有 intent_detail）
        assert "# Step:" in result.annotated_pyspark
        assert "# Business:" not in result.annotated_pyspark
        # 也不应含有旧的结构化行
        assert "# Intent:" not in result.annotated_pyspark

    def test_annotated_pyspark_injection_verified(self):
        """annotations 注入后 _verify_no_comment_injection 仍通过。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        anns = self._make_annotations()
        result = compiler.compile(plan, annotations=anns)
        # _verify_no_comment_injection 在 compile() 内部已调用
        # 这里验证 raw_hash 正确（raw 不受影响）
        raw_only = compiler.compile(plan, annotations=None)
        assert result.raw_hash == raw_only.raw_hash, "annotation 不应改变 raw_hash"

    def test_annotated_pyspark_contains_all_annotation_intents(self):
        """每个 annotation 的 intent_detail 出现在 annotated_pyspark 中。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        anns = self._make_annotations()
        result = compiler.compile(plan, annotations=anns)
        code = result.annotated_pyspark
        for ann in anns:
            assert ann.intent_detail in code, f"intent_detail {ann.intent_detail} 未出现"


# ════════════════════════════════════════════
# Phase 6B 测试——aggregate / join / case_when
# ════════════════════════════════════════════


class TestCompileAggregate:
    """AggregateStep 编译测试。"""

    def test_simple_aggregate_with_group_keys(self):
        """基本 groupBy + 聚合编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkAggregateStep(
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                    SparkAggregateSpec(
                        function=SparkAggFunction.SUM,
                        input_column="amount",
                        alias="total_amount",
                    ),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".groupBy(" in result.raw_pyspark
        assert ".agg(" in result.raw_pyspark
        assert 'F.col("region")' in result.raw_pyspark
        assert "F.count(F.lit(1))" in result.raw_pyspark
        assert 'F.sum(F.col("amount"))' in result.raw_pyspark
        assert '.alias("cnt")' in result.raw_pyspark
        assert '.alias("total_amount")' in result.raw_pyspark

    def test_aggregate_with_multiple_group_keys(self):
        """多 groupBy 键编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkAggregateStep(
                input_alias="od",
                group_keys=["region", "category"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'F.col("region")' in result.raw_pyspark
        assert 'F.col("category")' in result.raw_pyspark

    def test_aggregate_without_group_keys(self):
        """无 groupBy 键——全局聚合。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkAggregateStep(
                input_alias="od",
                group_keys=[],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.MAX,
                        input_column="amount",
                        alias="max_amount",
                    ),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".groupBy(" not in result.raw_pyspark
        assert ".agg(" in result.raw_pyspark
        assert 'F.max(F.col("amount"))' in result.raw_pyspark

    def test_aggregate_count_distinct(self):
        """COUNT_DISTINCT 编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkAggregateStep(
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT_DISTINCT,
                        input_column="user_id",
                        alias="unique_users",
                    ),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "F.countDistinct" in result.raw_pyspark
        assert 'F.col("user_id")' in result.raw_pyspark

    def test_aggregate_multiple_metrics(self):
        """多个聚合指标编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkAggregateStep(
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
                    ),
                    SparkAggregateSpec(
                        function=SparkAggFunction.SUM, input_column="amount", alias="total",
                    ),
                    SparkAggregateSpec(
                        function=SparkAggFunction.AVG, input_column="amount", alias="avg_amt",
                    ),
                    SparkAggregateSpec(
                        function=SparkAggFunction.MIN, input_column="amount", alias="min_amt",
                    ),
                    SparkAggregateSpec(
                        function=SparkAggFunction.MAX, input_column="amount", alias="max_amt",
                    ),
                ],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert result.raw_pyspark.count(".alias(") == 5
        assert "F.count(F.lit(1))" in result.raw_pyspark
        assert "F.sum" in result.raw_pyspark
        assert "F.avg" in result.raw_pyspark
        assert "F.min" in result.raw_pyspark
        assert "F.max" in result.raw_pyspark


class TestCompileJoin:
    """JoinStep 编译测试。"""

    def test_inner_join(self):
        """INNER JOIN 编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="up", source_name="dim.user_profile", input_key="up"),
            SparkJoinStep(
                left_alias="od",
                right_alias="up",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".join(" in result.raw_pyspark
        assert 't1["user_id"]' in result.raw_pyspark
        assert 't2["user_id"]' in result.raw_pyspark
        assert 'how="inner"' in result.raw_pyspark

    def test_left_join(self):
        """LEFT JOIN 编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="up", source_name="dim.user_profile", input_key="up"),
            SparkJoinStep(
                left_alias="od",
                right_alias="up",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.LEFT,
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'how="left"' in result.raw_pyspark

    def test_join_with_different_keys(self):
        """不同 Join 键名编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkReadStep(alias="ri", source_name="dim.region_info", input_key="ri"),
            SparkJoinStep(
                left_alias="od",
                right_alias="ri",
                left_key="region_code",
                right_key="code",
                join_type=SparkJoinType.INNER,
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 't1["region_code"]' in result.raw_pyspark
        assert 't2["code"]' in result.raw_pyspark

    def test_join_after_filter(self):
        """Filter 后 Join——resolver 通过 latest 映射串联。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="GT", left="od.amount", right="0"),
            SparkReadStep(alias="up", source_name="dim.user_profile", input_key="up"),
            SparkJoinStep(
                left_alias="od",
                right_alias="up",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".filter(" in result.raw_pyspark
        assert ".join(" in result.raw_pyspark


class TestCompileCaseWhen:
    """CaseWhenStep 编译测试——使用结构化 condition（Phase 10）。"""

    def test_simple_case_when(self):
        """单分支 CASE WHEN 编译——EQ 条件渲染。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkCaseWhenStep(
                input_alias="od",
                output_alias="status_label",
                branches=[
                    SparkCaseWhenBranch(
                        label="normal",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="status",
                            value="paid",
                        ),
                    ),
                ],
                else_value="other",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".withColumn(" in result.raw_pyspark
        assert "F.when(" in result.raw_pyspark
        assert ".otherwise(" in result.raw_pyspark
        assert 'F.col("status")' in result.raw_pyspark
        assert "== F.lit('paid')" in result.raw_pyspark
        assert "F.lit('normal')" in result.raw_pyspark

    def test_case_when_multiple_branches(self):
        """多分支 CASE WHEN——链式 when 编译。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkCaseWhenStep(
                input_alias="od",
                output_alias="priority_label",
                branches=[
                    SparkCaseWhenBranch(
                        label="high",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="priority",
                            value="1",
                        ),
                    ),
                    SparkCaseWhenBranch(
                        label="medium",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="priority",
                            value="2",
                        ),
                    ),
                    SparkCaseWhenBranch(
                        label="low",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="priority",
                            value="3",
                        ),
                    ),
                ],
                else_value="unknown",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert result.raw_pyspark.count("F.when(") == 3
        assert result.raw_pyspark.count(".otherwise(") == 3
        assert "F.lit('high')" in result.raw_pyspark
        assert "F.lit('medium')" in result.raw_pyspark
        assert "F.lit('low')" in result.raw_pyspark

    def test_case_when_else_none(self):
        """无 else_value——默认为 F.lit(None)。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkCaseWhenStep(
                input_alias="od",
                output_alias="label",
                branches=[
                    SparkCaseWhenBranch(
                        label="valid",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="status",
                            value="active",
                        ),
                    ),
                ],
                else_value=None,
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "F.lit(None)" in result.raw_pyspark


# ════════════════════════════════════════════
# Phase 10 结构化条件测试——CASE WHEN Predicate AST 渲染
# ════════════════════════════════════════════


class TestCompileCaseWhenStructuredConditions:
    """Phase 10 结构化条件编译测试——验证 CaseWhenCondition → PySpark Column API。"""

    def test_is_null_condition(self):
        """IS_NULL → F.col("distance_miles").isNull()。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="unknown",
                        condition=CaseWhenCondition(
                            operator="IS_NULL",
                            normalized_name="distance_miles",
                        ),
                    ),
                ],
                else_value="valid",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert 'F.col("distance_miles").isNull()' in result.raw_pyspark
        assert "F.when(" in result.raw_pyspark

    def test_eq_true_condition(self):
        """EQ True → F.col(...) == F.lit(True)，非 F.lit('true')。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="outlier",
                        condition=CaseWhenCondition(
                            operator="EQ",
                            normalized_name="is_distance_outlier",
                            value=True,
                        ),
                    ),
                ],
                else_value="normal",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "F.lit(True)" in result.raw_pyspark
        assert "F.lit('true')" not in result.raw_pyspark
        assert "F.lit('True')" not in result.raw_pyspark

    def test_lte_numeric_condition(self):
        """LTE 2 → F.col(...) <= F.lit(2)，非 F.lit('2')。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="short",
                        condition=CaseWhenCondition(
                            operator="LTE",
                            normalized_name="distance_miles",
                            value=2,
                        ),
                    ),
                ],
                else_value="long",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "F.lit(2)" in result.raw_pyspark
        assert "F.lit('2')" not in result.raw_pyspark

    def test_gt_numeric_condition(self):
        """GT 10 → F.col(...) > F.lit(10)。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="long",
                        condition=CaseWhenCondition(
                            operator="GT",
                            normalized_name="distance_miles",
                            value=10,
                        ),
                    ),
                ],
                else_value="short",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "F.lit(10)" in result.raw_pyspark
        assert "F.col(\"distance_miles\") > F.lit(10)" in result.raw_pyspark

    def test_and_condition(self):
        """AND(LTE 2, GT 0) → (F.col(...) <= F.lit(2)) & (F.col(...) > F.lit(0))。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="short",
                        condition=CaseWhenCondition(
                            operator="AND",
                            left=CaseWhenCondition(
                                operator="LTE",
                                normalized_name="distance_miles",
                                value=2,
                            ),
                            right=CaseWhenCondition(
                                operator="GT",
                                normalized_name="distance_miles",
                                value=0,
                            ),
                        ),
                    ),
                ],
                else_value="other",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "&" in result.raw_pyspark
        assert "F.lit(2)" in result.raw_pyspark
        assert "F.lit(0)" in result.raw_pyspark

    def test_or_with_is_null(self):
        """OR(IS_NULL x, EQ y True) → (F.col("x").isNull()) | (F.col("y") == F.lit(True))。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    SparkCaseWhenBranch(
                        label="unknown",
                        condition=CaseWhenCondition(
                            operator="OR",
                            left=CaseWhenCondition(
                                operator="IS_NULL",
                                normalized_name="distance_miles",
                            ),
                            right=CaseWhenCondition(
                                operator="EQ",
                                normalized_name="is_distance_outlier",
                                value=True,
                            ),
                        ),
                    ),
                ],
                else_value="valid",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert ".isNull()" in result.raw_pyspark
        assert "|" in result.raw_pyspark
        assert "F.lit(True)" in result.raw_pyspark

    def test_case_02_full_chain(self):
        """Case 02 完整 4 分支：IS_NULL OR =true → unknown; LTE 2 → short;
        AND(GT 2, LTE 10) → medium; GT 10 → long。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="distance_category",
                branches=[
                    # 分支 1：distance_miles IS NULL OR is_distance_outlier = true → unknown
                    SparkCaseWhenBranch(
                        label="unknown",
                        condition=CaseWhenCondition(
                            operator="OR",
                            left=CaseWhenCondition(
                                operator="IS_NULL",
                                normalized_name="distance_miles",
                            ),
                            right=CaseWhenCondition(
                                operator="EQ",
                                normalized_name="is_distance_outlier",
                                value=True,
                            ),
                        ),
                    ),
                    # 分支 2：distance_miles <= 2 → short
                    SparkCaseWhenBranch(
                        label="short",
                        condition=CaseWhenCondition(
                            operator="LTE",
                            normalized_name="distance_miles",
                            value=2,
                        ),
                    ),
                    # 分支 3：distance_miles > 2 AND distance_miles <= 10 → medium
                    SparkCaseWhenBranch(
                        label="medium",
                        condition=CaseWhenCondition(
                            operator="AND",
                            left=CaseWhenCondition(
                                operator="GT",
                                normalized_name="distance_miles",
                                value=2,
                            ),
                            right=CaseWhenCondition(
                                operator="LTE",
                                normalized_name="distance_miles",
                                value=10,
                            ),
                        ),
                    ),
                    # 分支 4：distance_miles > 10 → long
                    SparkCaseWhenBranch(
                        label="long",
                        condition=CaseWhenCondition(
                            operator="GT",
                            normalized_name="distance_miles",
                            value=10,
                        ),
                    ),
                ],
                else_value="unknown",
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        # 验证 4 个 F.when 调用
        assert result.raw_pyspark.count("F.when(") == 4
        # 验证所有标签出现
        assert "F.lit('unknown')" in result.raw_pyspark
        assert "F.lit('short')" in result.raw_pyspark
        assert "F.lit('medium')" in result.raw_pyspark
        assert "F.lit('long')" in result.raw_pyspark
        # 验证类型保真：数字和布尔
        assert "F.lit(2)" in result.raw_pyspark
        assert "F.lit(10)" in result.raw_pyspark
        assert "F.lit(True)" in result.raw_pyspark
        # 验证 IS_NULL 渲染
        assert ".isNull()" in result.raw_pyspark
        # 验证逻辑操作符
        assert " & " in result.raw_pyspark or "&" in result.raw_pyspark
        assert " | " in result.raw_pyspark or "|" in result.raw_pyspark

    def test_labels_only_branch_raises(self):
        """condition=None 的分支 → RenderError，不生成空条件。"""
        plan = _make_plan(
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkCaseWhenStep(
                input_alias="ft",
                output_alias="label",
                branches=[
                    SparkCaseWhenBranch(label="bad"),  # 无 condition
                ],
                else_value="other",
            ),
        )
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="缺少结构化 condition"):
            compiler.compile(plan)

    def test_unsupported_operator_raises(self):
        """不支持的操作符（如 IN）→ RenderError。"""
        with pytest.raises(RenderError, match="不支持条件操作符"):
            compiler = SparkCompiler()
            compiler._render_case_when_condition(
                CaseWhenCondition(operator="IN", normalized_name="x", value="1")
            )

    def test_and_missing_left_raises(self):
        """AND 缺少 left 子树 → RenderError，不抛 AttributeError。"""
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="缺少 left 或 right 子树"):
            compiler._render_case_when_condition(
                CaseWhenCondition(
                    operator="AND",
                    right=CaseWhenCondition(
                        operator="EQ", normalized_name="x", value=1,
                    ),
                ),
            )

    def test_and_missing_right_raises(self):
        """AND 缺少 right 子树 → RenderError，不抛 AttributeError。"""
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="缺少 left 或 right 子树"):
            compiler._render_case_when_condition(
                CaseWhenCondition(
                    operator="AND",
                    left=CaseWhenCondition(
                        operator="EQ", normalized_name="x", value=1,
                    ),
                ),
            )

    def test_or_missing_both_raises(self):
        """OR 缺少左右子树 → RenderError，不抛 AttributeError。"""
        compiler = SparkCompiler()
        with pytest.raises(RenderError, match="缺少 left 或 right 子树"):
            compiler._render_case_when_condition(
                CaseWhenCondition(operator="OR"),
            )


# ════════════════════════════════════════════
# Phase 6C 测试——window 编译 + 帧边界
# ════════════════════════════════════════════


class TestCompileWindow:
    """Phase 6C 窗口函数编译测试——含帧边界渲染。"""

    # ── 排名窗口函数 ──

    def test_row_number_basic(self):
        """ROW_NUMBER 基本编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.ROW_NUMBER,
                    alias="row_num",
                    order_by=["amount"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "row_num" in result.raw_pyspark
        assert "F.row_number()" in result.raw_pyspark
        assert "Window.orderBy" in result.raw_pyspark
        # ROW_NUMBER 不使用帧边界
        assert "rowsBetween" not in result.raw_pyspark

    def test_rank_with_partition(self):
        """RANK + partitionBy 编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.RANK,
                    alias="rank_val",
                    partition_by=["region"],
                    order_by=["amount"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.rank()" in result.raw_pyspark
        assert "Window.partitionBy" in result.raw_pyspark
        assert "rank_val" in result.raw_pyspark

    def test_dense_rank(self):
        """DENSE_RANK 编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.DENSE_RANK,
                    alias="dense_r",
                    order_by=["score"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.dense_rank()" in result.raw_pyspark
        assert "dense_r" in result.raw_pyspark

    def test_ntile_with_input_column(self):
        """NTILE 使用 input_column 作为分桶数参数。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.NTILE,
                    alias="bucket",
                    input_column="4",
                    order_by=["amount"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.ntile(4)" in result.raw_pyspark
        assert "bucket" in result.raw_pyspark

    def test_ntile_without_input_column_raises(self):
        """NTILE 缺少 input_column 时抛出编译错误——不用占位值掩盖缺失语义。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.NTILE,
                    alias="bucket",
                    order_by=["amount"],
                ),
            ],
        )
        plan = _make_plan(step)

        with pytest.raises(ValueError, match="NTILE"):
            SparkCompiler().compile(plan)

    # ── 偏移窗口函数 ──

    def test_lag_with_column(self):
        """LAG 带列名编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.LAG,
                    alias="prev_amount",
                    input_column="amount",
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.lag" in result.raw_pyspark
        assert "prev_amount" in result.raw_pyspark

    def test_lead_with_column(self):
        """LEAD 带列名编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.LEAD,
                    alias="next_amount",
                    input_column="amount",
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.lead" in result.raw_pyspark
        assert "next_amount" in result.raw_pyspark

    def test_lag_without_column_raises(self):
        """LAG 缺少 input_column 时抛出编译错误——严禁用 F.lit(1) 占位掩盖缺失语义。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.LAG,
                    alias="prev_amount",
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)

        with pytest.raises(ValueError, match="LAG"):
            SparkCompiler().compile(plan)

    def test_lead_without_column_raises(self):
        """LEAD 缺少 input_column 时抛出编译错误——严禁用 F.lit(1) 占位掩盖缺失语义。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.LEAD,
                    alias="next_amount",
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)

        with pytest.raises(ValueError, match="LEAD"):
            SparkCompiler().compile(plan)

    # ── 聚合窗口函数 + 帧边界 ──

    def test_sum_over(self):
        """SUM_OVER + 默认帧边界。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="running_total",
                    input_column="amount",
                    partition_by=["region"],
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.sum" in result.raw_pyspark
        assert "running_total" in result.raw_pyspark
        # 聚合窗口函数应有帧边界
        assert "rowsBetween" in result.raw_pyspark
        assert "Window.unboundedPreceding" in result.raw_pyspark
        assert "Window.currentRow" in result.raw_pyspark

    def test_count_over(self):
        """COUNT_OVER 编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.COUNT_OVER,
                    alias="cnt",
                    input_column="order_id",
                    partition_by=["region"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.count" in result.raw_pyspark
        assert "rowsBetween" in result.raw_pyspark

    def test_avg_over(self):
        """AVG_OVER 编译。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.AVG_OVER,
                    alias="avg_val",
                    input_column="score",
                    order_by=["order_date"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "F.avg" in result.raw_pyspark

    # ── 自定义帧边界 ──

    def test_custom_frame_range(self):
        """自定义 RANGE 帧边界。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="range_sum",
                    input_column="amount",
                    order_by=["order_date"],
                    frame_type="range",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        assert "rangeBetween" in result.raw_pyspark
        assert "Window.unboundedPreceding" in result.raw_pyspark

    def test_custom_frame_rows_between(self):
        """自定义 ROWS 帧边界——3 PRECEDING AND 3 FOLLOWING。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.AVG_OVER,
                    alias="moving_avg",
                    input_column="amount",
                    order_by=["order_date"],
                    frame_type="rows",
                    frame_start="3",
                    frame_end="3",
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        # 自定义整数帧边界
        assert "rowsBetween(3, 3)" in result.raw_pyspark

    # ── 多表达式窗口 ──

    def test_multiple_expressions(self):
        """单步骤包含多个窗口表达式——链式 withColumn。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.ROW_NUMBER,
                    alias="row_num",
                    partition_by=["region"],
                    order_by=["amount"],
                ),
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total",
                    input_column="amount",
                    partition_by=["region"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        # 两个 withColumn 链式调用
        assert result.raw_pyspark.count(".withColumn") == 2
        assert "F.row_number()" in result.raw_pyspark
        assert "F.sum" in result.raw_pyspark

    # ── 空表达式 ──

    def test_empty_expressions(self):
        """空表达式列表——生成占位注释。"""
        from tianshu_datadev.spark.models import SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        # 空表达式应生成直通赋值 f1 = t1（Read 前置 → t1）
        assert "f1 = t1" in result.raw_pyspark
        assert "# WINDOW" not in result.raw_pyspark
        assert "# Step:" in result.annotated_pyspark

    # ── 注释格式 ──

    def test_window_comment_format(self):
        """窗口函数编译产物含 5 行注释。"""
        from tianshu_datadev.spark.models import SparkWindowExpr, SparkWindowFunction, SparkWindowStep

        step = SparkWindowStep(
            input_alias="df",
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.ROW_NUMBER,
                    alias="rn",
                    partition_by=["dept"],
                    order_by=["salary"],
                ),
            ],
        )
        plan = _make_plan(step)
        result = SparkCompiler().compile(plan)

        annotated = result.annotated_pyspark
        assert "# Step:" in annotated
        # Phase 8C：不再产生 Intent/Operation/Inputs/Output 结构化行
        assert "# Intent:" not in annotated
        assert "# Operation:" not in annotated
        assert "# Inputs:" not in annotated
        assert "# Output:" not in annotated


# ════════════════════════════════════════════
# 注释块格式测试（从 test_spark_compiler_comment.py 合并）
# ════════════════════════════════════════════


class TestCommentFormat:
    """注释格式测试——Phase 8C 简化为 Step 行 + 可选业务注释。"""

    def test_comment_has_one_line(self):
        """每个步骤无 annotation 时只生成 Step 行。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        step_lines = [
            line for line in result.annotated_pyspark.split("\n")
            if line.strip().startswith("# Step:")
        ]
        assert len(step_lines) == 1

    def test_comment_step_present(self):
        """Step 行始终出现在注释中。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "# Step:" in result.annotated_pyspark
        # Phase 8C：不再产生 Intent/Operation/Inputs/Output
        assert "# Intent:" not in result.annotated_pyspark
        assert "# Operation:" not in result.annotated_pyspark
        assert "# Inputs:" not in result.annotated_pyspark
        assert "# Output:" not in result.annotated_pyspark

    def test_comment_missing_from_raw(self):
        """raw_pyspark 不含注释。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "# Step:" not in result.raw_pyspark

    def test_comment_index_format(self):
        """注释包含索引信息（索引 N/总数）。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "索引 1/1" in result.annotated_pyspark


class TestCommentNoSQL:
    """注释中不含 SQL 文本。"""

    def test_no_sql_keywords_in_comment(self):
        """注释中不含 SELECT/FROM/WHERE/JOIN 等 SQL 关键字。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkProjectStep(
                input_alias="od",
                columns=[SparkProjectColumn(column_name="amount", alias="amount")],
            ),
            SparkSortStep(
                input_alias="od",
                order_by=[SparkSortSpec(column="amount", direction=SparkSortDirection.DESC)],
            ),
            SparkLimitStep(input_alias="od", limit=100),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        comment_lines = [
            line for line in result.annotated_pyspark.split("\n")
            if line.strip().startswith("#")
        ]

        sql_pattern = re.compile(
            r"\b(SELECT|FROM|WHERE|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING|UNION|INSERT|UPDATE|DELETE)\b",
            re.IGNORECASE,
        )

        for line in comment_lines:
            content = line.strip()[2:] if line.strip().startswith("# ") else line.strip()[1:]
            match = sql_pattern.search(content)
            if match:
                pass
            assert sql_pattern.search(content) is None, (
                f"注释行含 SQL 关键字：{line.strip()}"
            )


class TestAnnotationsRemovable:
    """删除注释后执行代码等价测试。"""

    def test_raw_equals_annotated_minus_comments(self):
        """raw_pyspark 与 annotated_pyspark 去注释后完全一致。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkProjectStep(
                input_alias="od",
                columns=[SparkProjectColumn(column_name="status", alias="status")],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        def _strip_comments(code: str) -> str:
            lines = [
                line for line in code.split("\n")
                if not line.strip().startswith("#")
            ]
            return "\n".join(lines)

        raw_stripped = _strip_comments(result.raw_pyspark)
        ann_stripped = _strip_comments(result.annotated_pyspark)

        assert raw_stripped == ann_stripped
