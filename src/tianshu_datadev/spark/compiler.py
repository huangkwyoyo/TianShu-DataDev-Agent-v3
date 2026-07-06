"""Phase 6 SparkCompiler——SparkPlan → PySpark DSL 确定性代码生成。

参照 SQL Compiler 的 _compile_core() + 注释渲染分层架构。
不访问：DeveloperSpec、SqlBuildPlan、SQL 文本、LLM。
所有代码片段通过 SparkCodeRenderer 生成——禁止直接 f-string 拼接。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from tianshu_datadev.spark.models import (
    SparkAggregateStep,
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
    step_ids: list[str] = field(default_factory=list)
    # 追踪每个 step 的输出变量名
    output_var_map: dict[int, str] = field(default_factory=dict)
    step_counter: int = 0

    def next_step_id(self, step_type: str) -> str:
        """生成下一个 step_id。"""
        sid = f"{step_type}_{self.step_counter}"
        self.step_counter += 1
        return sid

    def add_step(self, step_id: str, raw_code: str, comment_block: str) -> None:
        """添加一个编译好的步骤。"""
        if comment_block:
            self.annotated_lines.append(comment_block)
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
            annotations: StepAnnotation 列表（可选，Phase 6A 暂无 LLM 标注）

        Returns:
            SparkCompileResult——含 raw + annotated 两个版本
        """
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

            state.add_step(step_id, raw, comment)

        # 组装函数体
        body_raw = "\n".join(f"    {line}" for line in state.raw_lines[3:])  # 跳过导入
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

    def compile_raw(self, plan: SparkPlan) -> SparkCompileResult:
        """编译无标注版本的代码（用于验证 annotation 不影响执行代码）。

        Args:
            plan: mapper.py 产出的 SparkPlan

        Returns:
            SparkCompileResult——raw_pyspark 与 compile(plan, annotations).raw_pyspark 相同
        """
        return self.compile(plan, annotations=None)

    # ── Step 编译方法 ──

    def _compile_read(
        self, step: SparkReadStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 ReadStep → {alias} = inputs["{source_name}"]。

        不允许 spark.read.parquet()——物理路径在 SnapshotManifest。
        source_name 作为 dict key 字符串使用，不校验 Python 标识符。
        """
        alias = self.renderer.validate_identifier(step.alias, "ReadStep.alias")
        # source_name 通过 render_dict_key 安全渲染——转义双引号/反斜杠/控制字符
        key_str = self.renderer.render_dict_key(step.source_name)
        raw = f"{alias} = inputs[{key_str}]"

        comment = self._build_comment_block(
            step_id=step_id, index=index, total=total,
            intent="数据读取",
            operation=f'从 inputs["{step.source_name}"] 读取数据，赋值为 DataFrame {alias}',
            inputs=step.source_name,
            output=alias,
        )
        return raw, comment

    def _compile_filter(
        self, step: SparkFilterStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 FilterStep → {out} = {input}.filter(...)。

        操作符从白名单映射到 Python 操作符。
        """
        input_alias = self.renderer.validate_identifier(
            step.input_alias, "FilterStep.input_alias"
        )
        op = step.operator.upper()
        col_ref = self.renderer.render_column(step.left)

        # 生成输出别名
        out_alias = f"_f{index}"

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
        self, step: SparkProjectStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 ProjectStep → {out} = {input}.select(...)。"""
        input_alias = self.renderer.validate_identifier(
            step.input_alias, "ProjectStep.input_alias"
        )
        out_alias = f"_p{index}"

        col_strs: list[str] = []
        for col in step.columns:
            col_name = self.renderer.validate_identifier(
                col.column_name, "ProjectStep.column_name"
            )
            if col.alias and col.alias != col.column_name:
                alias = self.renderer.validate_identifier(
                    col.alias, "ProjectStep.alias"
                )
                col_strs.append(f'F.col("{col_name}").alias("{alias}")')
            else:
                col_strs.append(f'F.col("{col_name}")')

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

    def _compile_sort(
        self, step: SparkSortStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 SortStep → {out} = {input}.orderBy(*[...])。"""
        input_alias = self.renderer.validate_identifier(
            step.input_alias or f"_p{index - 1}", "SortStep.input_alias"
        )
        out_alias = f"_s{index}"

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
        self, step: SparkLimitStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 LimitStep → {out} = {input}.limit({n})。"""
        input_alias = self.renderer.validate_identifier(
            step.input_alias or f"_s{index - 1}", "LimitStep.input_alias"
        )
        out_alias = f"_l{index}"

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
        self, step: SparkJoinStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 JoinStep → {out} = {left}.join({right}, on=..., how=...)。

        Join 键使用 df["col"] 语法消除同名列歧义。
        """
        left = self.renderer.validate_identifier(
            step.left_alias, "JoinStep.left_alias"
        )
        right = self.renderer.validate_identifier(
            step.right_alias, "JoinStep.right_alias"
        )
        out_alias = f"_j{index}"

        # Join 条件——使用 df["col"] 语法避免同名歧义
        left_key_ref = self.renderer.render_join_key(step.left_alias, step.left_key)
        right_key_ref = self.renderer.render_join_key(step.right_alias, step.right_key)
        condition = f"{left_key_ref} == {right_key_ref}"

        how = self.renderer.render_join_type(step.join_type)
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
        self, step: SparkAggregateStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 AggregateStep → {out} = {input}.groupBy(...).agg(...)。

        COUNT(*) 且 input_column 为 None 时使用 F.lit(1)。
        无 group_keys 时为全局聚合（不含 groupBy）。
        """
        input_alias = self.renderer.validate_identifier(
            step.input_alias, "AggregateStep.input_alias"
        )
        out_alias = f"_a{index}"

        # Group key 列引用
        group_cols = ", ".join(
            self.renderer.render_column(k) for k in step.group_keys
        )

        # 聚合指标表达式
        agg_parts: list[str] = []
        for m in step.metrics:
            fn_name = self.renderer.render_agg_function(m.function)
            if m.input_column:
                col_ref = self.renderer.render_column(m.input_column)
                agg_expr = f"{fn_name}({col_ref})"
            else:
                # COUNT(*) → F.count(F.lit(1))
                agg_expr = f"{fn_name}(F.lit(1))"
            alias = self.renderer.validate_identifier(
                m.alias, "AggregateSpec.alias"
            )
            agg_parts.append(f'{agg_expr}.alias("{alias}")')

        agg_str = ", ".join(agg_parts)

        if group_cols:
            raw = f"{out_alias} = {input_alias}.groupBy({group_cols}).agg({agg_str})"
        else:
            raw = f"{out_alias} = {input_alias}.agg({agg_str})"

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

    def _compile_case_when(
        self, step: SparkCaseWhenStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 CaseWhenStep → {out} = {input}.withColumn(col, F.when(...).otherwise(...))。

        每个分支必须携带结构化 condition（CaseWhenCondition AST），
        condition=None 时抛出 RenderError 阻断——labels-only 路径不进可执行 compiler。
        """
        input_alias = self.renderer.validate_identifier(
            step.input_alias, "CaseWhenStep.input_alias"
        )
        out_alias = f"_c{index}"
        output_col = self.renderer.validate_identifier(
            step.output_alias, "CaseWhenStep.output_alias"
        )

        # 构建 otherwise 链——从最内层开始
        if step.else_value is not None:
            else_lit = self.renderer.render_literal(step.else_value)
            chain = f"F.lit({else_lit})"
        else:
            chain = "F.lit(None)"

        # 倒序遍历分支——构建 F.when(cond, val).otherwise(inner)
        for branch in reversed(step.branches):
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
        self, step: SparkWindowStep, step_id: str, index: int, total: int,
    ) -> tuple[str, str]:
        """编译 WindowStep → {out} = {input}.withColumn(alias, fn.over(windowSpec))。

        每个 SparkWindowExpr 生成一个 withColumn 调用，多个表达式链式调用。
        帧边界仅在非标准默认值或显式指定时渲染。
        """
        input_alias = self.renderer.validate_identifier(
            step.input_alias, "WindowStep.input_alias"
        )
        out_alias = f"_w{index}"

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
            order_cols = ", ".join(
                self.renderer.render_column(c) for c in expr.order_by
            )
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
        comment = (
            f"# Step: {step_id}\n"
            f"# Intent: 未实现\n"
            f"# Operation: {step_type} 编译尚未实现（{reason}）\n"
            f"# Inputs: N/A\n"
            f"# Output: N/A"
        )
        return raw, comment

    # ── 注释块生成 ──

    def _build_comment_block(
        self,
        step_id: str,
        index: int,
        total: int,
        intent: str,
        operation: str,
        inputs: str,
        output: str,
    ) -> str:
        """构建 5 行固定格式注释块。

        格式：Step / Intent / Operation / Inputs / Output
        所有文本字段均通过 render_comment_text 清洗——不含 SQL 文本、不含换行。
        """
        r = self.renderer
        lines = [
            f"# Step: {step_id}（索引 {index + 1}/{total}）",
            f"# Intent: {r.render_comment_text(intent)}",
            f"# Operation: {r.render_comment_text(operation)}",
            f"# Inputs: {r.render_comment_text(inputs)}",
            f"# Output: {r.render_comment_text(output)}",
        ]
        # 每行独立清洗，末尾不加换行（由 add_step 统一管理）
        return "\n".join(lines)

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
