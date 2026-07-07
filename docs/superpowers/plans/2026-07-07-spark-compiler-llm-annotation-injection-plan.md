# Spark Compiler LLM 注释注入 Phase 8B 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 DEVELOPER 阶段 LLM 产出的 `StepAnnotation`（intent / intent_detail / operation_summary）注入 SparkCompiler 生成的 standalone PySpark 脚本中，作为每步 transform 函数的业务注释和输出字段的静态说明。

**架构：** 通过新增 `_enhance_comment_with_annotation()` helper，在 compile() 主循环内对 `_compile_xxx()` 返回的已有结构性 comment 进行后处理增强——用 LLM 文本替换 Intent/Operation 行、追加 Business 行，保留原 Inputs/Output 行。所有 LLM 文本必须经过 `render_comment_text()` 单行清洗。`_build_comment_block()` 不新增参数，保持原有 5 行结构性注释职责。

**Tech Stack:** Python 3.11, re (新增 import), typing, StepAnnotation (src/tianshu_datadev/spark/annotations.py)

## Global Constraints

- LLM 文本只能出现在 `#` 注释行中，**不可进入任何可执行 Python 语句**
- **所有 LLM 来源文本必须经过 `renderer.render_comment_text()` 单行清洗**（去换行、转义引号、移除控制字符）
- `_verify_no_comment_injection()` 不得削弱——去注释后 `annotated_pyspark` 必须等于 `raw_pyspark`
- `raw_pyspark` 不变（`raw_hash` 不受影响）
- `annotation_result=None`（DEVELOPER 未执行）时走原逻辑，不报错
- `_build_comment_block()` 保持原职责——只做 5 行结构性注释生成，不新增 annotation 参数
- 所有新增 LLM 字段的拼接和清洗统一在 `_enhance_comment_with_annotation()` helper 中完成

---
### Task 1: 测试先行——为 `_enhance_comment_with_annotation` 编写单元测试

**Files:**
- Test: `tests/spark/test_spark_compiler.py`

**Interfaces:**
- Consumes: `SparkCompiler._enhance_comment_with_annotation(self, comment, annotation) → str`
- Produces: 测试验证 `_enhance_comment_with_annotation` 的 6 个行为点

- [ ] **Step 1: 在 `TestMaliciousInput` 类后新增 `TestAnnotationInjection` 测试类**

```python
# ════════════════════════════════════════════
# Phase 8B 测试——LLM 语义标注注入
# ════════════════════════════════════════════


class TestAnnotationInjection:
    """LLM 语义标注注入 `_enhance_comment_with_annotation` 的验证测试。

    核心检查点：
    - Intent/Operation 行被 LLM 文本替换
    - Business 行追加在 Output 行之后
    - Inputs/Output 行被保留（不清空）
    - 所有 LLM 文本经 render_comment_text 清洗（换行被移除）
    - annotation=None 时不报错
    - annotation 中含恶意换行时 Business 行仍为单行
    """

    def _make_comment(self) -> str:
        """生成一个标准 5 行结构性注释作为测试输入。"""
        return (
            "# Step: SparkReadStep_0（索引 1/6）\n"
            "# Intent: source\n"
            "# Operation: 读取数据\n"
            "# Inputs: ft\n"
            "# Output: od"
        )

    def _make_annotation(self, intent_detail="读取行程事实表", operation_summary="从 ft 读取数据") -> StepAnnotation:
        return StepAnnotation(
            step_id="SparkReadStep_0",
            step_index=0,
            step_type="SparkReadStep",
            intent=StepIntent.SOURCE,
            intent_detail=intent_detail,
            operation_summary=operation_summary,
        )

    def test_intent_replaced(self):
        """Intent 行被 annotation.intent 替换。"""
        compiler = SparkCompiler()
        ann = self._make_annotation()
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        expected_intent = f"# Intent: {ann.intent.value}"
        assert expected_intent in result, f"Intent 行应被替换为 {expected_intent!r}"

    def test_operation_replaced(self):
        """Operation 行被 annotation.operation_summary 替换。"""
        compiler = SparkCompiler()
        ann = self._make_annotation(operation_summary="从 ft 事实表读取行程数据")
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        assert "# Operation: 从 ft 事实表读取行程数据" in result

    def test_business_appended_after_output(self):
        """Business 行追加在 Output 行之后。"""
        compiler = SparkCompiler()
        ann = self._make_annotation(intent_detail="读取出租车行程事实数据表")
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        lines = result.split("\n")
        # 找 Output 行和 Business 行的索引
        output_idx = next(i for i, l in enumerate(lines) if l.startswith("# Output:"))
        business_idx = next(i for i, l in enumerate(lines) if l.startswith("# Business:"))
        assert business_idx == output_idx + 1, (
            f"Business 行应在 Output 行之后：Output={output_idx}, Business={business_idx}"
        )
        assert "# Business: 读取出租车行程事实数据表" in result

    def test_inputs_output_preserved(self):
        """Inputs/Output 行内容不被清空。"""
        compiler = SparkCompiler()
        ann = self._make_annotation()
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        assert "# Inputs: ft" in result, "Inputs 行应被保留"
        assert "# Output: od" in result, "Output 行应被保留"

    def test_operation_summary_empty_falls_back(self):
        """operation_summary 为空时不替换 Operation 行（保留结构性描述）。"""
        compiler = SparkCompiler()
        # operation_summary="" 的 annotation
        ann = StepAnnotation(
            step_id="SparkReadStep_0", step_index=0,
            step_type="SparkReadStep", intent=StepIntent.SOURCE,
            intent_detail="读取数据", operation_summary="",
        )
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        # Operation 行应保留原值（不做替换）
        assert "# Operation: 读取数据" in result

    def test_malicious_newline_in_intent_detail_sanitized(self):
        """intent_detail 中含恶意换行——Business 注释为单行（不产生裸代码）。"""
        compiler = SparkCompiler()
        ann = self._make_annotation(
            intent_detail="正常描述\neval('bad')\n# 注入",
        )
        comment = self._make_comment()
        result = compiler._enhance_comment_with_annotation(comment, ann)
        # 所有 Business 行应该在 # 注释中，不在可执行代码中
        business_lines = [l for l in result.split("\n") if l.lstrip().startswith("# Business:")]
        assert len(business_lines) == 1, "Business 应为单行注释"
        # 验证没有裸代码行：去注释后应与原 raw 一致（在 compile() 的 _verify_no_comment_injection 验证）
        # 至少 Business 行不包含换行产生的多行
        assert "\n\n" not in result, "Business 行注入不应产生空行"
```

- [ ] **Step 2: 运行测试，验证失败**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/test_spark_compiler.py::TestAnnotationInjection -v
```

Expected: 所有 6 个测试 FAIL（`_enhance_comment_with_annotation` 尚未实现）

- [ ] **Step 3: import `StepAnnotation` 和 `StepIntent` 在测试文件头部**

在 `tests/spark/test_spark_compiler.py` 的现有 imports 中添加：

```python
from tianshu_datadev.spark.annotations import StepAnnotation, StepIntent
```

- [ ] **Step 4: 提交**

```bash
git add tests/spark/test_spark_compiler.py
git commit -m "test(spark): Phase 8B _enhance_comment_with_annotation 单元测试（红）"
```

---
### Task 2: 实现 `_enhance_comment_with_annotation` helper

**Files:**
- Modify: `src/tianshu_datadev/spark/compiler.py`

**Interfaces:**
- Consumes: `comment: str`（`_compile_xxx` 返回的 5 行结构性注释）、`annotation: StepAnnotation`
- Produces: `str`——增强后的注释（替换的 Intent/Operation 行 + 追加的 Business 行）

- [ ] **Step 1: 新增 `import re`**

在 `compiler.py` 第 10 行附近，`import hashlib` 之后添加：

```python
import re
```

- [ ] **Step 2: 在 `_build_comment_block` 方法后（第 792 行之后）添加 `_enhance_comment_with_annotation`**

```python
    def _enhance_comment_with_annotation(
        self,
        comment: str,
        annotation: StepAnnotation,
    ) -> str:
        """在已有结构性注释基础上增强——替换 Intent/Operation 文本，追加 Business 行。

        不清空 Inputs/Output——它们由 _compile_xxx 生成，包含真实的输入输出列信息。
        所有 LLM 来源文本必须经过 self.renderer.render_comment_text() 清洗。
        职责分工：
        - _build_comment_block():  结构性 5 行注释（Step / Intent / Operation / Inputs / Output）
        - _enhance_comment_with_annotation(): LLM 语义增强（替换 Intent/Operation，追加 Business）

        Args:
            comment: _compile_xxx 返回的原结构性注释
            annotation: StepAnnotation（LLM 语义标注）

        Returns:
            增强后的注释字符串
        """
        r = self.renderer

        # 替换 Intent 行内容（保留 # Intent: 行前缀）
        intent_text = (
            annotation.intent.value
            if hasattr(annotation.intent, "value")
            else str(annotation.intent)
        )
        comment = re.sub(
            r'^# Intent: .*$',
            f'# Intent: {r.render_comment_text(intent_text)}',
            comment,
            count=1,
            flags=re.MULTILINE,
        )

        # 替换 Operation 行内容（仅当 operation_summary 非空时）
        if annotation.operation_summary:
            comment = re.sub(
                r'^# Operation: .*$',
                f'# Operation: {r.render_comment_text(annotation.operation_summary)}',
                comment,
                count=1,
                flags=re.MULTILINE,
            )

        # 在 Output 行后追加 Business 行
        business_text = annotation.intent_detail
        comment = re.sub(
            r'^(# Output: .*)$',
            rf'\1\n# Business: {r.render_comment_text(business_text)}',
            comment,
            count=1,
            flags=re.MULTILINE,
        )

        return comment
```

在文件头部或 `_enhance_comment_with_annotation` 前添加类型导入：

```python
from tianshu_datadev.spark.annotations import StepAnnotation
```

> **注意：** 类型导入放在文件顶部现有的 `from tianshu_datadev.spark.renderer import ...` 之后。

- [ ] **Step 3: 运行测试验证通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/test_spark_compiler.py::TestAnnotationInjection -v
```

Expected: 所有 6 个测试 PASS（测试与实现对应）

- [ ] **Step 4: 验证已有测试不受影响**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/test_spark_compiler.py -v
```

Expected: 全部 PASS（含原有注释注入安全测试）

- [ ] **Step 5: 提交**

```bash
git add src/tianshu_datadev/spark/compiler.py tests/spark/test_spark_compiler.py
git commit -m "feat(spark): 实现 _enhance_comment_with_annotation helper——LLM 语义替换 + Business 追加 + render_comment_text 清洗"
```

---
### Task 3: 在 `compile()` 主循环中接入 annotation 后处理

**Files:**
- Modify: `src/tianshu_datadev/spark/compiler.py`

**Interfaces:**
- Consumes: `compile(plan, annotations)` 的 `annotations` 参数（已有，但未使用）
- Produces: `SparkCompileResult.annotated_pyspark`——含 LLM 业务注释的代码

- [ ] **Step 1: 修改 `compile()` 方法——构建 ann_map + 循环内后处理**

在 `compile()` 方法中，在 for 循环之前添加 ann_map 构建，循环内添加后处理逻辑：

```python
    def compile(
        self,
        plan: SparkPlan,
        annotations: list | None = None,
    ) -> SparkCompileResult:
        """编译 SparkPlan 为 PySpark DSL 代码。

        Args:
            plan: mapper.py 产出的 SparkPlan
            annotations: StepAnnotation 列表（可选——由 DEVELOPER 阶段产出）

        Returns:
            SparkCompileResult——含 raw + annotated 两个版本
        """
        state = _CompileState()

        # ── Phase 8B: 构建 step_id → StepAnnotation 映射 ──
        # （StepAnnotation 类型已在文件顶部 import，见 Task 2）
        ann_map: dict[str, "StepAnnotation"] = {}
        if annotations is not None:
            for a in annotations:
                if hasattr(a, "step_id") and a.step_id:
                    ann_map[a.step_id] = a

        # 渲染导入和函数签名
        imports = self.renderer.render_imports()
        signature = self.renderer.render_function_signature()
        state.raw_lines.append(imports)
        state.raw_lines.append("")
        state.raw_lines.append("")
        state.annotated_lines.append(imports)
        state.annotated_lines.append("")
        state.annotated_lines.append("")

        for i, step in enumerate(plan.steps):
            step_type = type(step).__name__
            step_id = state.next_step_id(step_type)

            # 分发到具体的编译方法
            if isinstance(step, SparkReadStep):
                raw, comment = self._compile_read(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkFilterStep):
                raw, comment = self._compile_filter(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkProjectStep):
                raw, comment = self._compile_project(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkSortStep):
                raw, comment = self._compile_sort(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkLimitStep):
                raw, comment = self._compile_limit(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkJoinStep):
                raw, comment = self._compile_join(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkAggregateStep):
                raw, comment = self._compile_aggregate(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkCaseWhenStep):
                raw, comment = self._compile_case_when(step, step_id, i, len(plan.steps))
            elif isinstance(step, SparkWindowStep):
                raw, comment = self._compile_window(step, step_id, i, len(plan.steps))
            else:
                raw, comment = self._compile_unsupported(step, step_id, "unknown")

            # ── Phase 8B: 有 LLM annotation 时增强 comment ──
            annotation = ann_map.get(step_id)
            if annotation is not None:
                comment = self._enhance_comment_with_annotation(comment, annotation)

            state.add_step(step_id, raw, comment)

        # 组装函数体
        body_raw = "\n".join(f"    {line}" for line in state.raw_lines[3:])
        body_annotated = "\n".join(f"    {line}" for line in state.annotated_lines[3:])

        raw_pyspark = (
            f"{imports}\n\n\n"
            f"{signature}\n"
            f"{body_raw}\n"
        )
        annotated_pyspark = (
            f"{imports}\n\n\n"
            f"{signature}\n"
            f"{body_annotated}\n"
        )

        raw_hash = hashlib.sha256(raw_pyspark.encode()).hexdigest()

        # 防御纵深：验证注释不含裸代码注入（去注释后应与 raw 一致）
        self._verify_no_comment_injection(raw_pyspark, annotated_pyspark)

        return SparkCompileResult(
            raw_pyspark=raw_pyspark,
            annotated_pyspark=annotated_pyspark,
            raw_hash=raw_hash,
            step_ids=state.step_ids,
        )
```

> **diff 格式说明：** 仅修改 3 处：(1) 方法 docstring 增加 annotations 参数说明；(2) for 循环前增加 ann_map 构建（8 行）；(3) 每个 step 编译后增加 annotation 后处理（4 行）。其余代码不变。

- [ ] **Step 2: 运行全部 compiler 测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/test_spark_compiler.py -v
```

Expected: 全部 PASS（含 TestAnnotationInjection 和原有安全测试）

- [ ] **Step 3: 验证 annotations=None 时不会报错**

添加一个快速验证测试（可与 Step 2 合并执行）：

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run python -c "
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.models import SparkReadStep, SparkPlan
# 调用 compile 时不传 annotations——应为 None
plan = SparkPlan(steps=[SparkReadStep(alias='t', source_name='test', input_key='t')])
c = SparkCompiler()
r = c.compile(plan)
print('annotations=None: OK, raw_hash=', r.raw_hash[:8])
"
```

Expected: 无异常输出，raw_hash 正常

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/spark/compiler.py
git commit -m "feat(spark): compile() 主循环接入 _enhance_comment_with_annotation 后处理"
```

---
### Task 4: pipeline.py `_do_spark_compile` 传参 + 使用 annotated_pyspark + 输出注释

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`

**Interfaces:**
- Consumes: `context.annotation_result: AnnotatedSparkPlan | None`（Phase 8A 已有）
- Produces: `context.standalone_pyspark`——含 LLM 业务注释的独立可执行脚本

- [ ] **Step 1: 修改 `_do_spark_compile`——传参 + 换输出 + 追加注释**

```python
    def _do_spark_compile(self, context: SparkStageContext) -> None:
        """执行 COMPILER 阶段——SparkPlan → PySpark DSL。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import SparkReadStep

        compiler = SparkCompiler()

        # ── Phase 8B: 传入 DEVELOPER 阶段的 LLM 语义标注 ──
        step_annotations = None
        if context.annotation_result is not None:
            step_annotations = context.annotation_result.annotations

        result = compiler.compile(context.spark_plan, annotations=step_annotations)
        context.compile_result = result
        context.stage_results["COMPILER"] = "SUCCESS"

        # ── 生成独立可执行脚本（wrapper 格式，含 SparkSession 引导）──
        # ── Phase 8B: 使用 annotated_pyspark（含 LLM 业务注释）──
        annotated_pyspark = result.annotated_pyspark
        # 提取所有 ReadStep 的 source_name
        input_names: list[str] = []
        for step in context.spark_plan.steps:
            if isinstance(step, SparkReadStep):
                input_names.append(step.source_name)

        # 构建 wrapper 脚本
        wrapper_lines: list[str] = []
        wrapper_lines.append("from pyspark.sql import SparkSession")
        wrapper_lines.append("from pyspark.sql.functions import *")
        wrapper_lines.append("")
        wrapper_lines.append("")
        wrapper_lines.append("# 以下 transform 函数由编译器自动生成")
        wrapper_lines.append("# 数据源需根据实际路径修改")
        wrapper_lines.append("")
        # 嵌入 annotated_pyspark（含 LLM 业务注释的 transform 函数）
        for line in annotated_pyspark.split("\n"):
            wrapper_lines.append(line)
        wrapper_lines.append("")
        wrapper_lines.append("")
        wrapper_lines.append('if __name__ == "__main__":')
        wrapper_lines.append('    spark = SparkSession.builder.appName("tianshu_datadev") \\')
        wrapper_lines.append('        .master("local[*]") \\')
        wrapper_lines.append('        .config("spark.sql.shuffle.partitions", "4") \\')
        wrapper_lines.append('        .getOrCreate()')
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 1. 加载数据")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    inputs = {")
        for i, name in enumerate(input_names):
            comma = "," if i < len(input_names) - 1 else ""
            wrapper_lines.append(f'        "{name}": spark.read.csv("data/{name}.csv", header=True){comma}')
        wrapper_lines.append("    }")
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 2. 执行转换")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    result = transform(inputs)")
        wrapper_lines.append("")
        # ── Phase 8B: 追加静态字段解读注释（仅注释块，不进可执行代码）──
        if context.annotation_result and context.annotation_result.annotations:
            last_ann = context.annotation_result.annotations[-1]
            safe_detail = compiler.renderer.render_comment_text(last_ann.intent_detail)
            wrapper_lines.append(f"    # 输出字段说明: {safe_detail}")
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 3. 输出结果")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append('    print("=== 结果概要 ===")')
        wrapper_lines.append("    result.printSchema()")
        wrapper_lines.append('    print(f"行数: {result.count()}")')
        wrapper_lines.append("    result.show(20, truncate=False)")
        wrapper_lines.append("")
        wrapper_lines.append('    print("=== 执行完毕 ===")')
        wrapper_lines.append("    spark.stop()")

        context.standalone_pyspark = "\n".join(wrapper_lines)
```

> **diff 实质改动：** (1) 提取 `step_annotations` 传参（+4 行）；(2) `raw_pyspark` → `annotated_pyspark` 用于 wrapper 嵌入（改 1 行）；(3) `# 输出字段说明:` 注释追加（+6 行，含 `render_comment_text` 清洗）

- [ ] **Step 2: 验证 `compiler.renderer` 可访问**

`compiler` 实例在 `__init__` 中已有 `self.renderer` 属性（见 `compiler.py:100-101`），是 `SparkCodeRenderer` 实例，有 `render_comment_text()` 方法。不需要额外改动。

- [ ] **Step 3: 验证已有测试不受影响**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/ -v
```

Expected: 全部 PASS（含 compiler、orchestrator、renderer 测试）

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat(api): _do_spark_compile 传入 LLM 标注 + annotated_pyspark + 静态字段注释"
```

---
### Task 5: 集成验证——Standalone 脚本注释完整性

**Files:**
- Test: `tests/spark/test_spark_compiler.py`（追加集成测试）

- [ ] **Step 1: 添加 `TestAnnotationInjection` 集成测试——standalone 脚本检查**

在 Task 1 的 `TestAnnotationInjection` 类尾部追加以下方法：

```python
    # ── 集成测试：E2E compile() 通过 annotation 参数 ──

    def _make_full_plan(self):
        """构建一个 3-step 计划（read + filter + project）用于集成测试。"""
        from tianshu_datadev.spark.models import (
            SparkFilterStep, SparkProjectStep,
        )
        return SparkPlan(steps=[
            SparkReadStep(alias="ft", source_name="fact_trips", input_key="ft"),
            SparkFilterStep(input_alias="ft", operator="GT",
                           left="ft.distance", right="10"),
            SparkProjectStep(input_alias="ft", columns=["trip_id", "distance"]),
        ])

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
        """传入 annotations 时所有 step 的注释块都包含 Business 行。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        anns = self._make_annotations()
        result = compiler.compile(plan, annotations=anns)
        # 每个 step 的注释块应包含 Business: 行
        for ann in anns:
            step_comment = self._extract_step_comment(result.annotated_pyspark, ann.step_id)
            assert step_comment is not None, f"未找到 {ann.step_id} 的注释块"
            assert f"# Business: {ann.intent_detail}" in step_comment, (
                f"{ann.step_id} 缺少 Business 行"
            )
            assert "# Inputs:" in step_comment, f"{ann.step_id} 的 Inputs 行被清空"
            assert "# Output:" in step_comment, f"{ann.step_id} 的 Output 行被清空"

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
        """annotations=None 时走原逻辑，不报错。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        result = compiler.compile(plan, annotations=None)
        # 不应包含 Business 行
        assert "# Business:" not in result.annotated_pyspark, "无 annotation 时不应有 Business 行"
        # 原有 5 行结构仍存在
        assert "# Intent:" in result.annotated_pyspark
        assert "# Operation:" in result.annotated_pyspark

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
        """每个 annotation 的 intent/intent_detail/operation_summary 出现在 annotated_pyspark 中。"""
        compiler = SparkCompiler()
        plan = self._make_full_plan()
        anns = self._make_annotations()
        result = compiler.compile(plan, annotations=anns)
        code = result.annotated_pyspark
        for ann in anns:
            assert ann.intent.value in code, f"intent {ann.intent.value} 未出现"
            if ann.operation_summary:
                assert ann.operation_summary in code, f"operation_summary {ann.operation_summary} 未出现"
            assert ann.intent_detail in code, f"intent_detail {ann.intent_detail} 未出现"
```

- [ ] **Step 2: 运行所有 compiler 测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/test_spark_compiler.py -v
```

Expected: 全部 PASS（含原有 + 新增 10 个测试）

- [ ] **Step 3: 运行全量 spark 测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/ -v
```

Expected: PASS（orchestrator / renderer 测试不受影响）

- [ ] **Step 4: 提交**

```bash
git add tests/spark/test_spark_compiler.py
git commit -m "test(spark): Phase 8B 集成测试——E2E annotation 注入 + _verify_no_comment_injection + raw_hash 不变"
```

---
### Task 6: 后端 API 烟雾测试（仅验证服务启动和编译链路无 500）

**Files:**
- Run: 后端启动 + Run All（使用已部署的 tpl_aggregation 模板）

- [ ] **Step 1: 启动后端服务**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run uvicorn src.tianshu_datadev.api.app:create_app --factory --host 0.0.0.0 --port 8000 --log-level info
```

Expected: 服务启动无报错

- [ ] **Step 2: 触发一次全流程（COMPILER + DEVELOPER）**

通过 API 调用 Run All（tpl_aggregation 模板），验证：
- COMPILER 阶段返回的 standalone_pyspark 包含 `# Business:` 注释行
- 注释内容为真实业务语义（非结构性描述）
- `# 输出字段说明:` 注释已添加且为单行
- `# Output:` / `# Inputs:` 行内容未被清空

如无法执行 E2E API 调用，则 Step 1-2 可跳过，由后续阶段验证。

- [ ] **Step 3: 全量测试通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && uv run pytest tests/spark/ -v
```

Expected: 所有测试 PASS（当前基线：601 passed / 11 skipped）

- [ ] **Step 4: 最终提交（如 E2E 有额外修复）**

```bash
git add -A && git commit -m "chore: Phase 8B 集成验证——LLM 语义标注成功注入 standalone 脚本"
```

---

## 验收检查清单

设计文档验收项与本计划的映射：

| # | 验收项 | 对应任务 | 验证方式 |
|---|--------|---------|---------|
| 1 | `transform()` 每步注释含 Business 业务语义，且经过 `render_comment_text` 清洗 | Task 2 + Task 5 | `test_compile_with_annotations_all_steps` |
| 2 | `_enhance_comment_with_annotation` 不破坏 Inputs/Output | Task 2 | `test_inputs_output_preserved` |
| 3 | `__main__` 有静态 `# 输出字段说明:` 注释，经 `render_comment_text` 清洗 | Task 4 | API 返回脚本检查 |
| 4 | `annotation_count == len(spark_plan.steps)`；annotation_result=None 时不要求 | Task 3 | `test_compile_with_annotations_all_steps`（100% 命中） |
| 5 | `annotation_result=None` 时不报错 | Task 3 | `test_compile_without_annotations_fallback` |
| 6 | `raw_hash` 不变 | Task 3 + Task 5 | `test_annotated_pyspark_injection_verified` |
| 7 | `_verify_no_comment_injection()` 通过 | Task 3 | compile() 内部调用 + 恶意输入回归测试 |
| 8 | 全量测试通过 | Task 5 | `pytest tests/spark/ -v` |
