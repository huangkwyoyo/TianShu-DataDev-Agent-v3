"""Phase 6 SparkCompiler——SparkPlan → PySpark DSL 确定性代码生成。

参照 SQL Compiler 的 _compile_core() + 注释渲染分层架构。
不访问：DeveloperSpec、SqlBuildPlan、SQL 文本、LLM。
所有代码片段通过 SparkCodeRenderer 生成——禁止直接 f-string 拼接。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from tianshu_datadev.developer_spec.models import MetricFilterDecl
from tianshu_datadev.spark._alias_resolver import (
    AliasResolutionError,
    ResolvedStep,
    resolve_codegen_aliases,
)
from tianshu_datadev.spark.annotations import StepAnnotation
from tianshu_datadev.spark.models import (
    SparkAggregateStep,
    SparkArithmeticExpression,
    SparkCaseWhenStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkLimitStep,
    SparkPlan,
    SparkProjectStep,
    SparkReadStep,
    SparkSortStep,
    SparkWindowStep,
)
from tianshu_datadev.spark.renderer import RenderError, SparkCodeRenderer

# ════════════════════════════════════════════
# 编译结果
# ════════════════════════════════════════════


@dataclass(frozen=True)
class SparkCompileResult:
    """SparkCompiler 的编译产出。

    raw_pyspark 是执行版本（Validator/执行/hash 以此为准），
    annotated_pyspark 是含注释的展示版本（仅供人审）。
    """

    raw_pyspark: str          # 无注释的纯 PySpark DSL 代码
    annotated_pyspark: str    # 带结构化注释的代码
    raw_hash: str             # raw_pyspark 的 SHA-256
    step_ids: list[str] = field(default_factory=list)  # 编译器生成的 step_id 列表


# ════════════════════════════════════════════
# 编译器内部状态
# ════════════════════════════════════════════


@dataclass
class _CompileState:
    """编译过程中的可变状态——仅在 compile() 调用内使用。"""

    raw_lines: list[str] = field(default_factory=list)
    annotated_lines: list[str] = field(default_factory=list)
    comment_lines: list[str] = field(default_factory=list)
    step_ids: list[str] = field(default_factory=list)
    step_counter: int = 0
    join_result_vars: set[str] = field(default_factory=set)
    """来自 JOIN 步骤的 DataFrame 变量名——其列名含 source_alias 前缀，需限定列引用。"""

    def next_step_id(self, step_type: str) -> str:
        """生成下一个 step_id。"""
        sid = f"{step_type}_{self.step_counter}"
        self.step_counter += 1
        return sid

    def add_step(self, step_id: str, raw_code: str, comment_block: str) -> None:
        """添加一个编译好的步骤。"""
        if comment_block:
            self.comment_lines.append(comment_block)
        self.raw_lines.append(raw_code)
        self.annotated_lines.append(raw_code)
        self.step_ids.append(step_id)


# ════════════════════════════════════════════
# SparkCompiler
# ════════════════════════════════════════════


class SparkCompiler:
    """确定性 PySpark DSL 编译器——SparkPlan → PySpark 代码。

    生成代码固定入口：
        def transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame:

    Phase 6A 支持 5 种 step：scan/filter/project/sort/limit。
    Phase 6B 扩展 3 种：aggregate/join/case_when。
    Phase 6C 扩展 1 种：window。
    """

    COMPILER_VERSION = "1.0.0"

    def __init__(self, renderer: SparkCodeRenderer | None = None):
        """初始化编译器。

        Args:
            renderer: 代码渲染器，默认创建新实例
        """
        self.renderer = renderer or SparkCodeRenderer()

    # ── 公共入口 ──

    def compile(
        self,
        plan: SparkPlan,
        annotations: list | None = None,
    ) -> SparkCompileResult:
        """编译 SparkPlan 为 PySpark DSL 代码。

        Args:
            plan: mapper.py 产出的 SparkPlan
            annotations: StepAnnotation 列表（可选，Phase 8B: 由 DEVELOPER 阶段产出）

        Returns:
            SparkCompileResult——含 raw + annotated 两个版本
        """
        if plan.branches:
            return self._compile_multistage(plan, annotations)

        # ── 单一入口：解析所有代码生成变量名 ──
        # 注意：主步骤的解析已移至分支编译之后，以便感知分支输出变量
        state = _CompileState()

        # 渲染导入和函数签名
        imports = self.renderer.render_imports()
        signature = self.renderer.render_function_signature()
        state.raw_lines.append(imports)
        state.raw_lines.append("")
        state.raw_lines.append("")
        state.annotated_lines.append(imports)
        state.annotated_lines.append("")
        state.annotated_lines.append("")

        # ── Phase 8B: 构建 step_id → StepAnnotation 映射 ──
        ann_map: dict[str, "StepAnnotation"] = {}
        if annotations is not None:
            for a in annotations:
                if hasattr(a, "step_id") and a.step_id:
                    ann_map[a.step_id] = a

        # ── 新增：编译所有分支（branches）──
        # 每个分支产生独立 DataFrame 变量——最终变量名 = 分支名
        # 先编译分支，再编译主 steps，主步骤可通过 JoinStep.alias 引用分支输出
        branch_outputs: dict[str, str] = {}
        if plan.branches:
            branch_var_counter = 0
            for branch_name, branch_steps in plan.branches.items():
                if not branch_steps:
                    continue

                # 分支头部注释——仅 annotated 输出包含，raw 输出不包含
                state.annotated_lines.append(f"# ── 分支: {branch_name} ──")

                # 追踪该分支内的别名→变量名映射
                _branch_latest: dict[str, str] = {}
                _branch_prev_output: str | None = None

                for j, step in enumerate(branch_steps):
                    step_type = type(step).__name__
                    step_id = state.next_step_id(step_type)
                    is_last = (j == len(branch_steps) - 1)

                    if isinstance(step, SparkReadStep):
                        branch_var_counter += 1
                        out_var = branch_name if is_last else f"_br_{branch_name}_{branch_var_counter}"
                        _branch_latest[step.alias] = out_var
                        _branch_prev_output = out_var
                        _resolved = ResolvedStep(step=step, input_vars=(), output_var=out_var)
                        raw, comment = self._compile_read(_resolved, step_id, j, len(branch_steps))

                    elif isinstance(step, SparkJoinStep):
                        left_var = _branch_latest.get(step.left_alias)
                        if left_var is None:
                            left_var = branch_outputs.get(step.left_alias)
                        if left_var is None:
                            raise AliasResolutionError(
                                f"分支 {branch_name!r} Join 步骤左表别名 {step.left_alias!r} 未解析"
                            )
                        right_var = _branch_latest.get(step.right_alias)
                        if right_var is None:
                            right_var = branch_outputs.get(step.right_alias)
                        if right_var is None:
                            raise AliasResolutionError(
                                f"分支 {branch_name!r} Join 步骤右表别名 {step.right_alias!r} 未解析"
                            )
                        branch_var_counter += 1
                        out_var = branch_name if is_last else f"_br_{branch_name}_{branch_var_counter}"
                        _branch_latest[step.left_alias] = out_var
                        _branch_prev_output = out_var
                        _resolved = ResolvedStep(
                            step=step,
                            input_vars=(left_var, right_var),
                            output_var=out_var,
                        )
                        raw, comment = self._compile_join(
                            _resolved, step_id, j, len(branch_steps),
                            join_result_vars=None,
                        )

                    else:
                        # 单输入步骤——Filter/Project/Sort/Limit/Aggregate/CaseWhen/Window
                        input_key = getattr(step, "input_alias", "") or ""
                        input_var: str | None = None
                        if input_key:
                            input_var = _branch_latest.get(input_key)
                            if input_var is None:
                                input_var = branch_outputs.get(input_key)
                        elif _branch_prev_output is not None:
                            input_var = _branch_prev_output
                        if input_var is None:
                            raise AliasResolutionError(
                                f"分支 {branch_name!r} 步骤 {j} "
                                f"{step_type} 的 input_alias={input_key!r} 未解析"
                            )
                        branch_var_counter += 1
                        out_var = branch_name if is_last else f"_br_{branch_name}_{branch_var_counter}"
                        if input_key:
                            _branch_latest[input_key] = out_var
                        _branch_prev_output = out_var
                        _resolved = ResolvedStep(step=step, input_vars=(input_var,), output_var=out_var)

                        if isinstance(step, SparkFilterStep):
                            raw, comment = self._compile_filter(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkProjectStep):
                            raw, comment = self._compile_project(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkSortStep):
                            raw, comment = self._compile_sort(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkLimitStep):
                            raw, comment = self._compile_limit(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkAggregateStep):
                            raw, comment = self._compile_aggregate(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkCaseWhenStep):
                            raw, comment = self._compile_case_when(_resolved, step_id, j, len(branch_steps))
                        elif isinstance(step, SparkWindowStep):
                            raw, comment = self._compile_window(_resolved, step_id, j, len(branch_steps))
                        else:
                            raw, comment = self._compile_unsupported(step, step_id, "unknown")

                    # 使用 add_step 统一管理（与主步骤一致：comment 进 comment_lines，raw 进两列表）
                    state.add_step(step_id, raw, comment)

                # 注册分支输出——分支名 → 分支输出变量名
                branch_alias = self.renderer.validate_identifier(
                    branch_name,
                    "SparkPlan.branches key",
                )
                alias_step_id = state.next_step_id("SparkBranchAlias")
                state.add_step(
                    alias_step_id,
                    f'{branch_name} = {branch_name}.alias("{branch_alias}")',
                    "",
                )
                branch_outputs[branch_name] = branch_name

        # ── 解析主步骤所有代码生成变量名（感知分支输出）──
        resolved_plan = resolve_codegen_aliases(plan, branch_outputs)

        # ── 预扫描 JOIN 步骤——收集其输出变量，用于后续 JOIN 条件中的列名消歧 ──
        join_result_vars: set[str] = set()
        for resolved in resolved_plan.steps:
            if isinstance(resolved.step, SparkJoinStep):
                join_result_vars.add(resolved.output_var)

        for i, resolved in enumerate(resolved_plan.steps):
            step = resolved.step
            step_type = type(step).__name__
            step_id = state.next_step_id(step_type)

            # 分发到具体的编译方法——传入 ResolvedStep（含 input_vars + output_var）
            if isinstance(step, SparkReadStep):
                raw, comment = self._compile_read(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkFilterStep):
                raw, comment = self._compile_filter(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkProjectStep):
                raw, comment = self._compile_project(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkSortStep):
                raw, comment = self._compile_sort(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkLimitStep):
                raw, comment = self._compile_limit(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkJoinStep):
                raw, comment = self._compile_join(
                    resolved, step_id, i, len(resolved_plan.steps),
                    join_result_vars=join_result_vars,
                )
            elif isinstance(step, SparkAggregateStep):
                raw, comment = self._compile_aggregate(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkCaseWhenStep):
                raw, comment = self._compile_case_when(resolved, step_id, i, len(resolved_plan.steps))
            elif isinstance(step, SparkWindowStep):
                raw, comment = self._compile_window(resolved, step_id, i, len(resolved_plan.steps))
            else:
                raw, comment = self._compile_unsupported(step, step_id, "unknown")

            # ── Phase 8B: 有 LLM annotation 时增强 comment ──
            annotation = ann_map.get(step_id)
            if annotation is not None:
                comment = self._enhance_comment_with_annotation(comment, annotation)

            state.add_step(step_id, raw, comment)

        # ── 返回值：resolver 已确定最终输出变量 ──
        last_var = resolved_plan.output_var

        # 组装函数体
        body_raw = "\n".join(f"    {line}" for line in state.raw_lines[3:])
        if state.comment_lines:
            indented_comments = "\n".join(
                "\n".join(f"    {line}" for line in block.split("\n"))
                for block in state.comment_lines
            )
            code = "\n".join(f"    {line}" for line in state.annotated_lines[3:])
            body_annotated = f"{indented_comments}\n{code}"
        else:
            body_annotated = "\n".join(f"    {line}" for line in state.annotated_lines[3:])

        if last_var:
            body_raw += f"\n    return {last_var}"
            body_annotated += f"\n    return {last_var}"

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

        self._verify_no_comment_injection(raw_pyspark, annotated_pyspark)

        return SparkCompileResult(
            raw_pyspark=raw_pyspark,
            annotated_pyspark=annotated_pyspark,
            raw_hash=raw_hash,
            step_ids=state.step_ids,
        )

    def compile_raw(self, plan: SparkPlan) -> SparkCompileResult:
        """编译无标注版本的代码（用于验证 annotation 不影响执行代码）。

        Args:
            plan: mapper.py 产出的 SparkPlan

        Returns:
            SparkCompileResult——raw_pyspark 与 compile(plan, annotations).raw_pyspark 相同
        """
        return self.compile(plan, annotations=None)

    def _compile_multistage(
        self,
        plan: SparkPlan,
        annotations: list | None,
    ) -> SparkCompileResult:
        """把多语句 DAG 编译为多个私有转换函数和一个公开编排入口。"""
        imports = self.renderer.render_imports()
        ann_map = {
            annotation.step_id: annotation
            for annotation in (annotations or [])
            if getattr(annotation, "step_id", "")
        }
        raw_blocks: list[str] = [imports]
        annotated_blocks: list[str] = [imports]
        step_ids: list[str] = []
        stage_outputs: dict[str, str] = {}
        stage_calls: list[tuple[str, str, list[str]]] = []

        for stage_index, (branch_name, branch_steps) in enumerate(
            plan.branches.items(),
            start=1,
        ):
            if not branch_steps:
                continue
            stage_var = f"s{stage_index}"
            function_name = f"_transform_{stage_var}"
            dependencies = self._stage_dependencies(
                branch_steps,
                stage_outputs,
            )
            raw, annotated, branch_step_ids = self._compile_stage_function(
                function_name=function_name,
                stage_label=stage_var,
                steps=branch_steps,
                external_outputs=stage_outputs,
                dependencies=dependencies,
                output_alias=stage_var,
                annotations=ann_map,
                main_stage=False,
            )
            raw_blocks.append(raw)
            annotated_blocks.append(annotated)
            step_ids.extend(branch_step_ids)
            stage_outputs[branch_name] = stage_var
            stage_calls.append((stage_var, function_name, dependencies))

        final_stage_index = len(stage_calls) + 1
        final_stage_var = f"s{final_stage_index}"
        final_function_name = f"_transform_{final_stage_var}"
        final_dependencies = self._stage_dependencies(
            plan.steps,
            stage_outputs,
        )
        raw, annotated, final_step_ids = self._compile_stage_function(
            function_name=final_function_name,
            stage_label=final_stage_var,
            steps=plan.steps,
            external_outputs=stage_outputs,
            dependencies=final_dependencies,
            output_alias=None,
            annotations=ann_map,
            main_stage=True,
        )
        raw_blocks.append(raw)
        annotated_blocks.append(annotated)
        step_ids.extend(final_step_ids)
        stage_calls.append(
            (final_stage_var, final_function_name, final_dependencies)
        )

        signature = self.renderer.render_function_signature()
        raw_orchestrator = [signature]
        annotated_orchestrator = [
            signature,
            "    # 按 SqlProgram DAG 顺序执行业务阶段",
        ]
        for stage_var, function_name, dependencies in stage_calls:
            args = ["inputs", *dependencies, "params=params"]
            call = f"    {stage_var} = {function_name}({', '.join(args)})"
            raw_orchestrator.append(call)
            annotated_orchestrator.append(call)
        raw_orchestrator.append(f"    return {final_stage_var}")
        annotated_orchestrator.append(f"    return {final_stage_var}")
        raw_blocks.append("\n".join(raw_orchestrator))
        annotated_blocks.append("\n".join(annotated_orchestrator))

        raw_pyspark = "\n\n\n".join(raw_blocks) + "\n"
        annotated_pyspark = "\n\n\n".join(annotated_blocks) + "\n"
        self._verify_no_comment_injection(raw_pyspark, annotated_pyspark)
        return SparkCompileResult(
            raw_pyspark=raw_pyspark,
            annotated_pyspark=annotated_pyspark,
            raw_hash=hashlib.sha256(raw_pyspark.encode()).hexdigest(),
            step_ids=step_ids,
        )

    def _compile_stage_function(
        self,
        *,
        function_name: str,
        stage_label: str,
        steps: list,
        external_outputs: dict[str, str],
        dependencies: list[str],
        output_alias: str | None,
        annotations: dict[str, StepAnnotation],
        main_stage: bool,
    ) -> tuple[str, str, list[str]]:
        """编译一个业务阶段；阶段内部统一使用 tN/fN。"""
        resolved_plan = resolve_codegen_aliases(steps, external_outputs)
        params = ["inputs: Mapping[str, DataFrame]"]
        params.extend(f"{name}: DataFrame" for name in dependencies)
        params.append("params: dict | None = None")
        signature = (
            f"def {function_name}(\n"
            + "".join(f"    {param},\n" for param in params)
            + ") -> DataFrame:"
        )
        raw_lines = [signature]
        annotated_lines = [
            signature,
            f"    # 业务阶段 {stage_label}",
            f"    {self._stage_goal_comment(steps, main_stage=main_stage)}",
        ]
        join_result_vars = {
            resolved.output_var
            for resolved in resolved_plan.steps
            if isinstance(resolved.step, SparkJoinStep)
        }
        step_ids: list[str] = []

        for index, resolved in enumerate(resolved_plan.steps):
            render_step = self._remap_external_aliases(
                resolved.step,
                external_outputs,
            )
            render_resolved = ResolvedStep(
                step=render_step,
                input_vars=resolved.input_vars,
                output_var=resolved.output_var,
            )
            step = render_step
            step_id = (
                f"{type(step).__name__}_{index}"
                if main_stage
                else f"{type(step).__name__}_{stage_label}_{index}"
            )
            raw, _ = self._compile_resolved_step(
                render_resolved,
                step_id,
                index,
                len(resolved_plan.steps),
                join_result_vars,
            )
            comment = (
                f"{self._build_comment_block(step_id, index, len(resolved_plan.steps))}\n"
                f"{self._readable_step_comment(render_resolved)}"
            )
            annotation = annotations.get(step_id)
            if annotation is not None:
                comment = self._enhance_comment_with_annotation(
                    comment,
                    annotation,
                )
            raw_lines.append(f"    {raw}")
            annotated_lines.extend(
                f"    {line}" for line in comment.splitlines()
            )
            annotated_lines.append(f"    {raw}")
            step_ids.append(step_id)

        return_expr = resolved_plan.output_var
        if output_alias is not None:
            safe_alias = self.renderer.validate_identifier(
                output_alias,
                "SparkPlan.branches key",
            )
            return_expr = f'{return_expr}.alias("{safe_alias}")'
        raw_lines.append(f"    return {return_expr}")
        annotated_lines.append(f"    return {return_expr}")
        return "\n".join(raw_lines), "\n".join(annotated_lines), step_ids

    def _stage_goal_comment(self, steps: list, *, main_stage: bool) -> str:
        """根据封闭 SparkStep 结构生成一行阶段业务目标。"""
        source_names = list(dict.fromkeys(
            step.source_name
            for step in steps
            if isinstance(step, SparkReadStep)
        ))
        operation_names: list[str] = []
        operation_map = (
            (SparkFilterStep, "过滤"),
            (SparkJoinStep, "关联"),
            (SparkAggregateStep, "聚合"),
            (SparkCaseWhenStep, "业务标签"),
            (SparkWindowStep, "窗口计算"),
            (SparkSortStep, "排序"),
            (SparkLimitStep, "行数限制"),
            (SparkProjectStep, "字段选择"),
        )
        for step in steps:
            for step_type, operation_name in operation_map:
                if isinstance(step, step_type):
                    if operation_name not in operation_names:
                        operation_names.append(operation_name)
                    break

        output_columns: list[str] = []
        for step in reversed(steps):
            if isinstance(step, SparkProjectStep):
                output_columns = [
                    column.alias or column.column_name
                    for column in step.columns
                ]
                break

        if source_names:
            source_text = f"读取 {'、'.join(source_names)}"
        else:
            source_text = "合并上游阶段"
        operation_text = (
            f"，完成{'、'.join(operation_names)}"
            if operation_names
            else ""
        )
        output_text = (
            f"，产出 {'、'.join(output_columns)}"
            if output_columns
            else ""
        )
        purpose = "作为最终结果" if main_stage else "供后续阶段使用"
        detail = f"业务目标：{source_text}{operation_text}{output_text}，{purpose}。"
        return self.renderer.render_comment_line(detail)

    @staticmethod
    def _stage_dependencies(
        steps: list,
        stage_outputs: dict[str, str],
    ) -> list[str]:
        """按 sN 顺序返回当前阶段实际引用的上游阶段。"""
        used: set[str] = set()
        for step in steps:
            if isinstance(step, SparkJoinStep):
                aliases = (step.left_alias, step.right_alias)
            else:
                aliases = (getattr(step, "input_alias", ""),)
            for alias in aliases:
                stage_var = stage_outputs.get(alias)
                if stage_var is not None:
                    used.add(stage_var)
        return sorted(used, key=lambda name: int(name[1:]))

    def _remap_external_aliases(
        self,
        step,
        external_outputs: dict[str, str],
    ):
        """仅在代码生成视图中把内部临时表别名替换为 sN。"""
        if not external_outputs:
            return step

        def remap(value):
            if isinstance(value, str):
                for source, target in external_outputs.items():
                    if value == source:
                        return target
                    if value.startswith(f"{source}."):
                        return f"{target}{value[len(source):]}"
                return value
            if isinstance(value, list):
                return [remap(item) for item in value]
            if isinstance(value, dict):
                return {key: remap(item) for key, item in value.items()}
            return value

        payload = remap(step.model_dump(mode="python"))
        return type(step).model_validate(payload)

    def _compile_resolved_step(
        self,
        resolved: ResolvedStep,
        step_id: str,
        index: int,
        total: int,
        join_result_vars: set[str],
    ) -> tuple[str, str]:
        """统一分发已解析步骤，避免多阶段编译器维护第二套规则。"""
        step = resolved.step
        if isinstance(step, SparkReadStep):
            return self._compile_read(resolved, step_id, index, total)
        if isinstance(step, SparkFilterStep):
            return self._compile_filter(resolved, step_id, index, total)
        if isinstance(step, SparkProjectStep):
            return self._compile_project(resolved, step_id, index, total)
        if isinstance(step, SparkSortStep):
            return self._compile_sort(resolved, step_id, index, total)
        if isinstance(step, SparkLimitStep):
            return self._compile_limit(resolved, step_id, index, total)
        if isinstance(step, SparkJoinStep):
            return self._compile_join(
                resolved,
                step_id,
                index,
                total,
                join_result_vars=join_result_vars,
            )
        if isinstance(step, SparkAggregateStep):
            return self._compile_aggregate(resolved, step_id, index, total)
        if isinstance(step, SparkCaseWhenStep):
            return self._compile_case_when(resolved, step_id, index, total)
        if isinstance(step, SparkWindowStep):
            return self._compile_window(resolved, step_id, index, total)
        return self._compile_unsupported(step, step_id, "unknown")

    def _readable_step_comment(self, resolved: ResolvedStep) -> str:
        """生成贴近代码的确定性操作注释，不暴露内部 step_id。"""
        step = resolved.step
        output = resolved.output_var
        if isinstance(step, SparkReadStep):
            detail = f"读取输入 {step.source_name} -> {output}"
        elif isinstance(step, SparkFilterStep):
            detail = (
                f"尽早过滤 {step.left} {step.operator} "
                f"{step.right} -> {output}"
            )
        elif isinstance(step, SparkJoinStep):
            detail = (
                f"{step.join_type.value} JOIN: "
                f"{step.left_key} = {step.right_key} -> {output}"
            )
        elif isinstance(step, SparkAggregateStep):
            metrics = ", ".join(metric.alias for metric in step.metrics)
            groups = ", ".join(step.group_keys) or "全局"
            detail = f"按 {groups} 聚合 {metrics} -> {output}"
        elif isinstance(step, SparkProjectStep):
            columns = ", ".join(
                column.alias or column.column_name
                for column in step.columns
            )
            detail = f"选择业务列 {columns} -> {output}"
        elif isinstance(step, SparkCaseWhenStep):
            detail = f"生成业务标签 {step.output_alias} -> {output}"
        elif isinstance(step, SparkWindowStep):
            expressions = ", ".join(
                expression.alias for expression in step.expressions
            )
            detail = f"计算窗口列 {expressions} -> {output}"
        elif isinstance(step, SparkSortStep):
            columns = ", ".join(spec.column for spec in step.order_by)
            detail = f"按 {columns} 排序 -> {output}"
        elif isinstance(step, SparkLimitStep):
            detail = f"限制为前 {step.limit} 行 -> {output}"
        else:
            detail = f"执行 {type(step).__name__} -> {output}"
        return f"# {self.renderer.render_comment_text(detail)}"

    # ── Step 编译方法 ──

    def _compile_read(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 ReadStep → tN = inputs["{source_name}"]。"""
        step = resolved.step
        out_alias = resolved.output_var  # resolver 已分配 tN
        key_str = self.renderer.render_dict_key(step.source_name)
        source_alias = self.renderer.validate_identifier(
            step.alias, "ReadStep.alias",
        )
        raw = f'{out_alias} = inputs[{key_str}].alias("{source_alias}")'

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="数据读取",
            operation=f'从 inputs["{step.source_name}"] 读取数据 → {out_alias}',
            inputs=step.source_name,
            output=out_alias,
        )
        return raw, comment

    def _compile_filter(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 FilterStep → fN = {input}.filter(...)。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var
        op = step.operator.upper()
        pure_column = step.left.split(".", 1)[-1] if "." in step.left else step.left
        col_ref = self.renderer.render_column(pure_column)

        if self.renderer.is_unary_operator(op):
            # IS_NULL / IS_NOT_NULL
            if op == "IS_NULL":
                cond = f"{col_ref}.isNull()"
            else:  # IS_NOT_NULL
                cond = f"{col_ref}.isNotNull()"
        elif op == "IN":
            right_str = self.renderer.render_filter_right(step.right)
            cond = f"{col_ref}.isin({right_str})"
        elif op == "NOT_IN":
            right_str = self.renderer.render_filter_right(step.right)
            cond = f"~{col_ref}.isin({right_str})"
        elif op == "BETWEEN":
            right_str = self.renderer.render_filter_right(step.right)
            # Phase 8C: BETWEEN 需要两个独立参数，不是列表
            # right_str 格式为 "['v1', 'v2']"，提取两个值
            vals = re.findall(r"'([^']*)'", right_str)
            if len(vals) == 2:
                cond = f"{col_ref}.between('{vals[0]}', '{vals[1]}')"
            else:
                cond = f"{col_ref}.between({right_str})"
        elif op == "LIKE":
            right_str = self.renderer.render_filter_right(step.right)
            cond = f"{col_ref}.like({right_str})"
        else:
            py_op = self.renderer.render_operator(op)
            right_str = self.renderer.render_filter_right(step.right)
            cond = f"{col_ref} {py_op} {right_str}"

        raw = f"{out_alias} = {input_alias}.filter({cond})"

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="数据过滤",
            operation=f"对 {input_alias} 应用过滤条件：{step.left} {op} {step.right}",
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _compile_project(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 ProjectStep → fN = {input}.select(...)。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var

        col_strs: list[str] = []
        for col in step.columns:
            if col.arithmetic_expression is not None:
                alias = self.renderer.validate_identifier(
                    col.alias, "ProjectStep.alias",
                )
                expression = self._render_arithmetic_expression(
                    col.arithmetic_expression
                )
                col_strs.append(f'({expression}).alias("{alias}")')
                continue
            if col.ratio_expr is not None:
                ratio = col.ratio_expr
                numerator = self.renderer.validate_identifier(
                    ratio.numerator_alias,
                    "ProjectStep.ratio_expr.numerator_alias",
                )
                denominator = self.renderer.validate_identifier(
                    ratio.denominator_alias,
                    "ProjectStep.ratio_expr.denominator_alias",
                )
                alias = self.renderer.validate_identifier(
                    col.alias, "ProjectStep.alias",
                )
                denominator_ref = f'F.col("{denominator}")'
                value = (
                    f'(F.col("{numerator}").cast("double") / '
                    f'{denominator_ref}.cast("double"))'
                )
                if ratio.multiplier == 100:
                    value = f"({value} * F.lit(100))"
                col_strs.append(
                    "F.when("
                    f"{denominator_ref}.isNull() | ({denominator_ref} == F.lit(0)), "
                    'F.lit(None).cast("double")'
                    f').otherwise({value}).alias("{alias}")'
                )
                continue
            col_name = self.renderer.validate_identifier(
                col.column_name, "ProjectStep.column_name"
            )
            qualified_name = (
                f"{col.source_alias}.{col_name}"
                if col.source_alias
                else col_name
            )
            col_expr = self.renderer.render_column(qualified_name)
            if col.alias and col.alias != col.column_name:
                alias = self.renderer.validate_identifier(
                    col.alias, "ProjectStep.alias"
                )
                col_strs.append(f'{col_expr}.alias("{alias}")')
            else:
                col_strs.append(col_expr)

        cols_joined = ", ".join(col_strs)
        raw = f"{out_alias} = {input_alias}.select({cols_joined})"

        col_names = [c.column_name for c in step.columns]
        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="列投影",
            operation=f"从 {input_alias} 选取列：{', '.join(col_names)}",
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _render_arithmetic_expression(
        self,
        expression: SparkArithmeticExpression,
    ) -> str:
        """递归渲染封闭算术 AST，不接受原始 PySpark 表达式。"""
        if expression.kind == "column":
            column_name = self.renderer.validate_identifier(
                expression.column_name or "",
                "SparkArithmeticExpression.column_name",
            )
            return f'F.col("{column_name}")'
        if expression.kind == "literal":
            if expression.value is None:
                raise RenderError("算术字面量不得为空")
            return f"F.lit({expression.value!r})"
        if expression.kind == "null_if_zero":
            if expression.left is None:
                raise RenderError("NULLIF 表达式缺少输入")
            value = self._render_arithmetic_expression(expression.left)
            return (
                f"F.when(({value}).isNull() | ({value} == F.lit(0)), "
                f"F.lit(None)).otherwise({value})"
            )
        if (
            expression.kind != "binary"
            or expression.left is None
            or expression.right is None
        ):
            raise RenderError("二元算术表达式缺少左右操作数")
        operators = {
            "ADD": "+",
            "SUBTRACT": "-",
            "MULTIPLY": "*",
            "DIVIDE": "/",
        }
        operator = operators.get(expression.operator or "")
        if operator is None:
            raise RenderError(
                f"不支持的算术操作符: {expression.operator!r}"
            )
        left = self._render_arithmetic_expression(expression.left)
        right = self._render_arithmetic_expression(expression.right)
        return f"({left} {operator} {right})"

    def _compile_sort(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 SortStep → fN = {input}.orderBy(*[...])。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var

        sort_strs: list[str] = []
        for spec in step.order_by:
            col_name = self.renderer.validate_identifier(
                spec.column, "SortStep.column"
            )
            direction_fn = self.renderer.render_sort_direction(spec.direction)
            sort_strs.append(f'{direction_fn}("{col_name}")')

        sorts_joined = ", ".join(sort_strs)
        raw = f"{out_alias} = {input_alias}.orderBy({sorts_joined})"

        sort_desc = ", ".join(
            f"{s.column} {s.direction.value}" for s in step.order_by
        )
        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="排序",
            operation=f"对 {input_alias} 排序：{sort_desc}",
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _compile_limit(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 LimitStep → fN = {input}.limit({n})。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var

        raw = f"{out_alias} = {input_alias}.limit({step.limit})"

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="行限制",
            operation=f"对 {input_alias} 取前 {step.limit} 行",
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _compile_join(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
        join_result_vars: set[str] | None = None,
    ) -> tuple[str, str]:
        """编译 JoinStep → fN = {left}.join({right}, on=..., how=...)。"""
        step = resolved.step
        left = resolved.input_vars[0]
        right = resolved.input_vars[1]
        out_alias = resolved.output_var

        # 使用 table.column 限定名消除 JOIN 结果中的同名列歧义。
        # 当左/右 DataFrame 来自前序 JOIN 时，其列名按 PySpark 规则以
        # source_alias.column_name 形式存在，直接使用原始字段名会导致
        # AmbiguousReference（如 cp.crash_date_key vs fc.crash_date_key）。
        # 对单源 DataFrame 仍使用原始列名。
        _join_vars = join_result_vars or set()
        left_col = (
            f"{step.left_alias}.{step.left_key}"
            if left in _join_vars
            else step.left_key
        )
        right_col = (
            f"{step.right_alias}.{step.right_key}"
            if right in _join_vars
            else step.right_key
        )
        how = self.renderer.render_join_type(step.join_type)
        if step.left_key == step.right_key:
            if step.left_key == 'borough':
                # borough 大小写归一化：改用表达式 JOIN 而非 using-column JOIN——
                # crash_detail.borough 全大写 vs taxi_zone.borough 首字母大写
                left_key_ref = self.renderer.render_join_key(left, left_col)
                right_key_ref = self.renderer.render_join_key(right, right_col)
                condition = (
                    f"F.upper({left_key_ref}) == F.upper({right_key_ref})"
                )
                raw = (
                    f"{out_alias} = {left}.join("
                    f"{right}, on={condition}, how={how})"
                )
            else:
                # using-column Join 会把同名联结键合并为一个输出列，避免后续
                # 聚合或投影以裸列名引用时触发 Spark AMBIGUOUS_REFERENCE。
                join_key = self.renderer.validate_identifier(
                    step.left_key, "JoinStep.join_key",
                )
                raw = (
                    f'{out_alias} = {left}.join('
                    f'{right}, on="{join_key}", how={how})'
                )
        else:
            left_key_ref = self.renderer.render_join_key(left, left_col)
            right_key_ref = self.renderer.render_join_key(right, right_col)
            # borough 大小写归一化——处理左右键不同名但涉及 borough 的场景
            if step.left_key == 'borough' or step.right_key == 'borough':
                left_key_ref = f"F.upper({left_key_ref})"
                right_key_ref = f"F.upper({right_key_ref})"
            condition = f"{left_key_ref} == {right_key_ref}"
            raw = f"{out_alias} = {left}.join({right}, on={condition}, how={how})"

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="表连接",
            operation=f"{left} JOIN {right} ON {step.left_key} = {step.right_key}（{step.join_type.value}）",
            inputs=f"{left}, {right}",
            output=out_alias,
        )
        return raw, comment

    def _compile_aggregate(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 AggregateStep → fN = {input}.groupBy(...).agg(...)。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var

        # 受控派生键在聚合表达式内部计算，不增加额外 SparkPlan 节点。
        aggregate_input = input_alias
        for derived in step.derived_group_keys:
            output_col = self.renderer.validate_identifier(
                derived.output_column, "DerivedGroupKey.output_column"
            )
            source_col = self.renderer.render_column(derived.source_column)
            if derived.date_part == "HOUR":
                aggregate_input += (
                    f'.withColumn("{output_col}", F.hour({source_col}))'
                )

        # Group key 列引用——构建列表以支持追加 time_transforms
        group_col_parts: list[str] = [
            self.renderer.render_column(k) for k in step.group_keys
        ]

        # time_transforms（v3.1 新增）
        for tt in step.time_transforms:
            func = tt.time_function  # "hour"
            src = f'F.col("{tt.source_table}.{tt.source_column}")'
            group_col_parts.append(f'F.{func}({src}).alias("{tt.alias}")')

        group_cols = ", ".join(group_col_parts)

        # 聚合指标表达式
        agg_parts: list[str] = []
        for m in step.metrics:
            fn_name = self.renderer.render_agg_function(m.function)
            if m.input_column:
                col_ref = self.renderer.render_column(m.input_column)
                inner = col_ref
                # SUM 对非数值列（如 boolean）自动 cast 为 double，
                # 匹配 DuckDB 隐式类型转换行为（Spark 拒绝 SUM(boolean)）。
                # 仅在无 FILTER 时触发——有 FILTER 时 F.when() 已推导类型。
                if fn_name in ("F.sum",) and not m.filter:
                    inner = f"{inner}.cast('double')"
                # AVG 对 DECIMAL 列精度受限——Spark 返回 DecimalType(38,6)
                # 仅 6 位小数，而 DuckDB 隐式转 DOUBLE 后返回全精度 ~15 位。
                # 统一 cast 为 double 消除跨引擎 ~1e-7 AVG 差异。
                if fn_name in ("F.avg",):
                    inner = f"{inner}.cast('double')"
            else:
                # COUNT(*) → F.lit(1)
                inner = "F.lit(1)"
            # 条件聚合 FILTER——F.when(condition, inner) 包装
            if m.filter:
                cond = self._render_metric_filter_spark(m.filter)
                inner = f"F.when({cond}, {inner})"
            agg_expr = f"{fn_name}({inner})"
            alias = self.renderer.validate_identifier(
                m.alias, "AggregateSpec.alias"
            )
            agg_parts.append(f'{agg_expr}.alias("{alias}")')

        # time_transforms 别名——在 agg 中引用，确保输出列存在
        for tt in step.time_transforms:
            agg_parts.append(f'F.col("{tt.alias}")')

        agg_str = ", ".join(agg_parts)

        if group_cols:
            raw = f"{out_alias} = {aggregate_input}.groupBy({group_cols}).agg({agg_str})"
        else:
            raw = f"{out_alias} = {aggregate_input}.agg({agg_str})"

        metrics_desc = ", ".join(
            f"{m.function.value}({m.input_column or '*'}) AS {m.alias}"
            for m in step.metrics
        )
        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="数据聚合",
            operation=f"对 {input_alias} 按 {step.group_keys or '(全局)'} 分组，计算 {metrics_desc}",
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _render_metric_filter_spark(self, filter_decl: MetricFilterDecl) -> str:
        """渲染 MetricFilterDecl 为 PySpark 条件表达式——用于 F.when()。

        操作符映射与 SQL compiler _render_metric_filter 语义一致：
          eq -> ==, neq -> !=, gt -> >, gte -> >=, lt -> <, lte -> <=,
          in -> .isin(...), is_null -> .isNull(), is_not_null -> .isNotNull()
        """
        col = f'F.col("{filter_decl.column}")'
        op = filter_decl.operator
        val = filter_decl.value

        if op == "eq":
            # 转义值中的双引号，防止生成的 PySpark 代码语法错误
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} == F.lit("{val_escaped}")'
        elif op == "neq":
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} != F.lit("{val_escaped}")'
        elif op == "gt":
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} > F.lit("{val_escaped}")'
        elif op == "gte":
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} >= F.lit("{val_escaped}")'
        elif op == "lt":
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} < F.lit("{val_escaped}")'
        elif op == "lte":
            val_escaped = str(val).replace('"', '\\"')
            return f'{col} <= F.lit("{val_escaped}")'
        elif op == "in":
            # value 是逗号分隔的字符串，拆分为列表
            items = [f'F.lit("{v.strip()}")' for v in val.split(",")]
            return f'{col}.isin({", ".join(items)})'
        elif op == "is_null":
            return f'{col}.isNull()'
        elif op == "is_not_null":
            return f'{col}.isNotNull()'
        else:
            raise ValueError(f"不支持的 MetricFilterDecl 操作符: {op!r}")

    def _compile_case_when(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 CaseWhenStep → fN = {input}.withColumn(col, F.when(...).otherwise(...))。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var
        output_col = self.renderer.validate_identifier(
            step.output_alias, "CaseWhenStep.output_alias"
        )

        # 构建 otherwise 链——从最内层开始
        if step.else_value is not None:
            else_lit = self.renderer.render_literal(step.else_value)
            chain = f"F.lit({else_lit})"
        else:
            chain = "F.lit(None)"

        # 第一步：检查 condition=None 的分支（labels-only 路径——阻断）
        for b in step.branches:
            if b.condition is None:
                raise RenderError(
                    f"CaseWhenStep 分支 label='{b.label}' 缺少结构化 condition，"
                    f"labels-only 路径不能进入可执行 compiler。"
                    f"请确保 Contract 提取时已填充 CaseWhenBranchSpec.branches"
                )

        # 过滤 COMPLEX_RAW 分支——这些是复杂布尔表达式（如 A OR B），
        # 无法结构化为 PySpark DSL，但 COMPARATOR 已验证 SparkPlan 侧
        # 保留了这些条件且逻辑等价。编译产物仅用于静态分析/安全校验，
        # 不用于实际执行（PHYSICAL_VERIFIER 默认 SKIPPED）。
        branches_to_compile = [
            b for b in step.branches
            if b.condition.operator != "COMPLEX_RAW"
        ]

        if not branches_to_compile:
            # 全为 COMPLEX_RAW——直通赋值，编译不产出可执行 PySpark
            raw = f"{out_alias} = {input_alias}"
            comment = self._build_comment_block(
                step_id=step_id, index=index, total=total,
                intent="条件分支（COMPLEX_RAW——跳过编译）",
                operation=(
                    f"对 {input_alias} 新增列 {output_col}："
                    f"复杂表达式跳过编译，仅保留直通赋值"
                ),
                inputs=input_alias,
                output=out_alias,
            )
            return raw, comment

        # 倒序遍历分支——构建 F.when(cond, val).otherwise(inner)
        for branch in reversed(branches_to_compile):
            # 缺条件→阻断，不平替为空条件
            if branch.condition is None:
                raise RenderError(
                    f"CaseWhenStep 分支 label='{branch.label}' 缺少结构化 condition，"
                    f"labels-only 路径不能进入可执行 compiler。"
                    f"请确保 Contract 提取时已填充 CaseWhenBranchSpec.branches"
                )
            label_lit = self.renderer.render_literal(branch.label)
            cond = self._render_case_when_condition(branch.condition)
            chain = f"F.when({cond}, F.lit({label_lit})).otherwise({chain})"

        raw = f'{out_alias} = {input_alias}.withColumn("{output_col}", {chain})'

        branches_desc = ", ".join(
            f"WHEN {b.condition.operator}"
            + (f" {b.condition.normalized_name}" if b.condition.normalized_name else "")
            + (f" {b.condition.value}" if b.condition.value is not None else "")
            + f" THEN {b.label}"
            if b.condition is not None
            else f"WHEN ? THEN {b.label}"
            for b in step.branches
        )
        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="条件分支",
            operation=(
                f"对 {input_alias} 新增列 {output_col}："
                f"{branches_desc} ELSE {step.else_value or 'NULL'}"
            ),
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _render_case_when_condition(self, condition) -> str:
        """将 CaseWhenCondition AST 渲染为 PySpark Column API 表达式。

        使用 renderer.render_column / render_literal / render_operator 做安全渲染。
        render_literal 已正确处理 int/float/bool/str 类型保真。

        Args:
            condition: CaseWhenCondition 实例

        Returns:
            PySpark Column API 表达式字符串（如 '(F.col("a").isNull()) | (F.col("b") == F.lit(True))'）

        Raises:
            RenderError: 遇到不支持的操作符
        """
        op = condition.operator

        # 一元：IS_NULL / IS_NOT_NULL
        if op == "IS_NULL":
            col = self.renderer.render_column(
                condition.normalized_name or condition.table_ref
            )
            return f"{col}.isNull()"
        if op == "IS_NOT_NULL":
            col = self.renderer.render_column(
                condition.normalized_name or condition.table_ref
            )
            return f"{col}.isNotNull()"

        # 二元比较
        if op in ("EQ", "NEQ", "GT", "GTE", "LT", "LTE"):
            col = self.renderer.render_column(
                condition.normalized_name or condition.table_ref
            )
            if condition.date_part == "HOUR":
                col = f"F.hour({col})"
            py_op = self.renderer.render_operator(op)
            val = self.renderer.render_literal(condition.value)
            return f"{col} {py_op} F.lit({val})"

        # 逻辑组合——left/right 必须非空，否则是畸形 AST
        if op in ("AND", "OR"):
            if condition.left is None or condition.right is None:
                raise RenderError(
                    f"CaseWhenCondition operator='{op}' 缺少 left 或 right 子树，"
                    f"AND/OR 要求左右子树均非空"
                )
            left = self._render_case_when_condition(condition.left)
            right = self._render_case_when_condition(condition.right)
            return f"({left}) & ({right})" if op == "AND" else f"({left}) | ({right})"

        raise RenderError(f"Spark CASE WHEN 不支持条件操作符: {op}")

    def _compile_window(
        self, resolved: ResolvedStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 WindowStep → fN = {input}.withColumn(alias, fn.over(windowSpec))。"""
        step = resolved.step
        input_alias = resolved.input_vars[0]
        out_alias = resolved.output_var

        if not step.expressions:
            # 空表达式列表——直通赋值（无操作）
            raw = f"{out_alias} = {input_alias}"
            comment = self._build_comment_block(
                step_id=step_id, index=index, total=total,
                intent="窗口函数（空）",
                operation=f"对 {input_alias} 未指定任何窗口表达式，直通传递",
                inputs=input_alias,
                output=out_alias,
            )
            return raw, comment

        # 构建 withColumn 链
        chain = input_alias
        expr_descs: list[str] = []

        for expr in step.expressions:
            alias = self.renderer.validate_identifier(expr.alias, "WindowExpr.alias")

            # 渲染窗口函数调用
            fn_call = self._render_window_fn_call(expr)

            # 渲染 WindowSpec
            window_spec = self._render_window_spec(expr)

            chain = f"{chain}.withColumn(\"{alias}\", {fn_call}.over({window_spec}))"

            col_info = expr.input_column or ""
            expr_descs.append(f"{expr.function.value}({col_info}) AS {expr.alias}")

        raw = f"{out_alias} = {chain}"

        # 从第一个表达式提取 partition/order 信息（同一步骤的表达式共享同一窗口）
        first = step.expressions[0]
        partition_info = ", ".join(first.partition_by) if first.partition_by else "(全局)"
        order_info = ", ".join(first.order_by) if first.order_by else "(无排序)"

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="窗口函数",
            operation=(
                f"对 {input_alias} 应用窗口函数：{'; '.join(expr_descs)} "
                f"PARTITION BY [{partition_info}] ORDER BY [{order_info}]"
            ),
            inputs=input_alias,
            output=out_alias,
        )
        return raw, comment

    def _render_window_fn_call(self, expr) -> str:
        """渲染单个窗口函数调用——委托 renderer 生成函数名 + 参数。

        排名函数（ROW_NUMBER/RANK/DENSE_RANK）无参数。
        NTILE 需要整数参数（默认 1）。
        LAG/LEAD 需要列名。
        聚合窗口函数（SUM_OVER/AVG_OVER/COUNT_OVER）需要列名。
        """
        from tianshu_datadev.spark.models import SparkWindowFunction

        fn_name = self.renderer.render_window_function(expr.function)

        # 排名函数——无参数
        if expr.function in (
            SparkWindowFunction.ROW_NUMBER,
            SparkWindowFunction.RANK,
            SparkWindowFunction.DENSE_RANK,
        ):
            return f"{fn_name}()"

        # NTILE——需要分桶数参数，来自 input_column（必须为正整数字符串）
        if expr.function == SparkWindowFunction.NTILE:
            if expr.input_column and expr.input_column.strip().isdigit():
                return f"{fn_name}({expr.input_column.strip()})"
            raise ValueError(
                f"NTILE 窗口函数必须指定有效的分桶数（input_column），"
                f"当前值为 {expr.input_column!r}，不允许用默认值掩盖缺失语义"
            )

        # LAG / LEAD——必须指定 input_column，严禁占位值
        if expr.function in (SparkWindowFunction.LAG, SparkWindowFunction.LEAD):
            if expr.input_column:
                col_ref = self.renderer.render_column(expr.input_column)
                return f"{fn_name}({col_ref})"
            raise ValueError(
                f"{expr.function.value} 窗口函数必须指定 input_column，"
                f"不允许使用 F.lit(1) 占位掩盖缺失语义"
            )

        # 聚合窗口函数——SUM_OVER / AVG_OVER / COUNT_OVER
        if expr.input_column:
            col_ref = self.renderer.render_column(expr.input_column)
            return f"{fn_name}({col_ref})"
        # COUNT_OVER 无列名时使用 F.lit(1)（等价于 COUNT(*)）
        return f"{fn_name}(F.lit(1))"

    def _render_window_spec(self, expr) -> str:
        """渲染 WindowSpec——partitionBy + orderBy + 帧边界。

        帧边界使用 render_frame_boundary / render_frame_type 做白名单校验。
        默认帧（unbounded_preceding → current_row, rows）仅在使用
        聚合窗口函数时渲染，排名函数省略帧边界。
        """
        from tianshu_datadev.spark.models import SparkWindowFunction

        parts: list[str] = []

        # partitionBy
        if expr.partition_by:
            partition_cols = ", ".join(
                self.renderer.render_column(c) for c in expr.partition_by
            )
            parts.append(f"Window.partitionBy({partition_cols})")

        # orderBy
        if expr.order_by:
            rendered_order_cols: list[str] = []
            for item in expr.order_by:
                order_parts = item.rsplit(maxsplit=1)
                if len(order_parts) == 2 and order_parts[1].upper() in {"ASC", "DESC"}:
                    column_expr = self.renderer.render_column(order_parts[0])
                    rendered_order_cols.append(
                        f"{column_expr}.{order_parts[1].lower()}()"
                    )
                else:
                    rendered_order_cols.append(self.renderer.render_column(item))
            order_cols = ", ".join(rendered_order_cols)
            # 仅当 partitionBy 在前时才省略 Window. 前缀
            if expr.partition_by:
                parts.append(f"orderBy({order_cols})")
            else:
                parts.append(f"Window.orderBy({order_cols})")

        # 帧边界——聚合窗口函数才渲染（排名函数使用隐式默认帧即可）
        is_aggregate_window = expr.function in (
            SparkWindowFunction.SUM_OVER,
            SparkWindowFunction.AVG_OVER,
            SparkWindowFunction.COUNT_OVER,
        )
        # 检查是否为非默认帧配置
        has_custom_frame = (
            expr.frame_start != "unbounded_preceding"
            or expr.frame_end != "current_row"
            or expr.frame_type != "rows"
        )

        if is_aggregate_window or has_custom_frame:
            frame_start = self.renderer.render_frame_boundary(expr.frame_start)
            frame_end = self.renderer.render_frame_boundary(expr.frame_end)
            frame_fn = self.renderer.render_frame_type(expr.frame_type)
            parts.append(f"{frame_fn}({frame_start}, {frame_end})")

        if not parts:
            return "Window()"

        # 链式拼接：Window.partitionBy(...).orderBy(...).rowsBetween(...)
        result = parts[0]
        for p in parts[1:]:
            result += f".{p}"
        return result

    def _compile_unsupported(
        self, step, step_id: str, reason: str,
    ) -> tuple[str, str]:
        """编译不支持/未实现的 step 类型——生成占位符注释。"""
        step_type = type(step).__name__
        raw = f"# UNSUPPORTED: {step_type} — {reason}"
        comment = f"# Step: {step_id}\n# {step_type} 编译尚未实现（{reason}）"
        return raw, comment

    # ── 注释块生成 ──

    def _build_comment_block(
        self,
        step_id: str,
        index: int,
        total: int,
        intent: str = "",
        operation: str = "",
        inputs: str = "",
        output: str = "",
    ) -> str:
        """构建 Step 头部注释行。

        Phase 8C：简化为仅含 Step 行，结构化的 Intent/Operation/Inputs/Output
        不再产生。具体的业务过程注释由 LLM annotation（intent_detail）注入。
        """
        return f"# Step: {step_id}（索引 {index + 1}/{total}）"

    def _enhance_comment_with_annotation(
        self,
        comment: str,
        annotation: StepAnnotation,
    ) -> str:
        """在 Step 头部注释后附加一句自然语言业务注释（intent_detail）。

        原结构化注释（Intent/Operation/Inputs/Output）不再产生，
        Phase 8C 统一使用 intent_detail 作为每步的唯一业务过程描述。
        所有 LLM 来源文本通过 self.renderer.render_comment_text() 清洗。
        """
        r = self.renderer
        detail = r.render_comment_text(annotation.intent_detail)
        return f"{comment}\n# {detail}"


    @staticmethod
    def _verify_no_comment_injection(raw: str, annotated: str) -> None:
        """防御纵深：验证去注释后的 annotated_pyspark 与 raw_pyspark 一致。

        若注释中含未清洗的换行，会导致 annotated 中出现裸代码行，
        此时去注释后与 raw 不一致——抛出 RenderError 阻断。
        """
        from tianshu_datadev.spark.renderer import RenderError

        def _strip_comments(code: str) -> str:
            return "\n".join(
                line for line in code.split("\n")
                if not line.lstrip().startswith("#")
            )

        if _strip_comments(annotated) != raw:
            raise RenderError(
                "annotated_pyspark 安全验证失败——去注释后与 raw_pyspark 不一致，"
                "可能存在注释注入产生的裸代码行"
            )
