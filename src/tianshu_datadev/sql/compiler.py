"""DuckDbSqlCompiler——确定性 DuckDB SQL 编译器。

只接受通过 Validator 的 SqlBuildPlan，不接受自由 SQL 片段。
编译流程：
    1. 接收 SqlBuildPlan + 可选表名映射
    2. 运行 4 个 Compiler Pass（列裁剪 → 谓词规范化 → 无用排序消除 → 常量折叠）
    3. 确定性渲染每个 Step 为 SQL 片段
    4. 组装完整 DuckDB SQL
    5. 计算确定性 sql_sha256
    6. 输出 CompiledSql（含 OptimizedSQLPlan 记录）

相同 SqlBuildPlan + 相同 compiler_version → 相同 SQL + 相同 SHA-256。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from tianshu_datadev.developer_spec.models import AggregationType
from tianshu_datadev.planning.models import (
    Predicate,
    PredicateOperator,
    SqlLiteral,
    WindowExpr,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    SubqueryStep,
    WindowStep,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlStatement,
    StatementKind,
)

from .compiler_passes import (
    column_pruning,
    constant_folding,
    predicate_normalization,
    sort_elimination,
)
from .models import (
    CompiledSql,
    CompilerPassRecord,
    ConstantFoldRecord,
    OptimizedSQLPlan,
    PredicateNormRecord,
    ProgramCompiledSql,
    SqlArtifact,
    SqlProgramArtifact,
)

COMPILER_VERSION = "1.1.0"


@dataclass(frozen=True)
class CoreCompileResult:
    """_compile_core() 的返回值——纯编译产物，不含注释和 CREATE TEMP TABLE 包装。

    将编译核心与注释渲染解耦：compile() 和 compile_program()
    各自在 CoreCompileResult 之上添加自己的注释策略。
    """

    raw_sql: str                      # 优化后的裸 SQL（无注释，无 CREATE TEMP TABLE 包装）
    optimized_plan: SqlBuildPlan      # 优化后的 SqlBuildPlan（供注释渲染使用）
    optimized_sql_plan: OptimizedSQLPlan  # 优化 Pass 记录（供调试/审计）
    input_plan_hash: str              # 优化前 SqlBuildPlan 的 hash


class DuckDbSqlCompiler:
    """确定性 DuckDB SQL 编译器。

    编译流程（10 步）：
    1. 列裁剪 Pass
    2. 谓词规范化 Pass
    3. 无用排序消除 Pass
    4. 常量折叠 Pass
    5. 收集 Step 子句（FROM / JOIN / WHERE / GROUP BY / SELECT / ORDER BY / LIMIT）
    6. 渲染 Predicate 为 SQL 表达式
    7. 组装完整 SQL
    8. 计算确定性 SHA-256
    9. 构建 OptimizedSQLPlan 记录
    10. 输出 CompiledSql
    """

    def __init__(self, table_mapping: dict[str, str] | None = None):
        """初始化编译器。

        Args:
            table_mapping: table_ref（别名）→ source_table（物理表名）的映射。
                          如果为 None，则使用 table_ref 作为物理表名（用于单表/测试场景）。

        Raises:
            ValueError: table_mapping 的 key 或 value 包含非法 SQL 字符
        """
        if table_mapping:
            self._validate_table_mapping(table_mapping)
        self._table_mapping = table_mapping or {}

    def _compile_core(self, plan: SqlBuildPlan) -> CoreCompileResult:
        """纯编译核心——运行 Pass + 渲染 SQL，不添加注释。

        将当前 compile() 中的校验 → Pass → 渲染逻辑抽取到此方法。
        compile() 和 compile_program() 后续重构为调用
        _compile_core() 后再添加各自的注释策略。

        Args:
            plan: 经 Validator 验证通过的 SqlBuildPlan

        Returns:
            CoreCompileResult——含 raw_sql、优化后 plan、Pass 记录、input_plan_hash

        Raises:
            ValueError: plan.steps 为空或 step 缺少 step_id
        """
        if not plan.steps:
            raise ValueError("SqlBuildPlan.steps 为空——无法编译")

        # 最小安全网：确认所有 step 都有 step_id
        for step in plan.steps:
            if not step.step_id:
                raise ValueError(
                    f"Step 类型 {step.step_type} 缺少 step_id——"
                    f"SqlBuildPlan 可能未经过 Validator 验证"
                )

        # 计算输入 plan hash
        input_plan_hash = SqlBuildPlan.generate_plan_hash(plan)

        # ── Compiler Pass 阶段 ──
        pass_records: list[CompilerPassRecord] = []
        norm_records: list[PredicateNormRecord] = []
        fold_records: list[ConstantFoldRecord] = []
        pruned_cols: list[str] = []
        eliminated_sorts: list[str] = []

        # Pass 1: 列裁剪
        plan, prune_record, pruned_cols = column_pruning(plan)
        pass_records.append(prune_record)

        # Pass 2: 谓词规范化
        plan, norm_records = predicate_normalization(plan)
        if norm_records:
            pass_records.append(
                CompilerPassRecord(
                    pass_name="predicate_normalization",
                    pass_version="1.0.0",
                    applied=True,
                    changes_count=len(norm_records),
                    input_ast_snippet="see predicate_normalizations",
                    output_ast_snippet=f"{len(norm_records)} changes",
                )
            )

        # Pass 3: 无用排序消除
        plan, sort_record, eliminated_sorts = sort_elimination(plan)
        pass_records.append(sort_record)

        # Pass 4: 常量折叠
        plan, fold_records = constant_folding(plan)
        if fold_records:
            pass_records.append(
                CompilerPassRecord(
                    pass_name="constant_folding",
                    pass_version="1.0.0",
                    applied=True,
                    changes_count=len(fold_records),
                    input_ast_snippet="see constant_folds",
                    output_ast_snippet=f"{len(fold_records)} folds",
                )
            )

        # 计算优化后 plan hash
        output_plan_hash = SqlBuildPlan.generate_plan_hash(plan)

        # ── SQL 渲染阶段 ──
        raw_sql = self._render_sql(plan)

        # ── 构建 OptimizedSQLPlan ──
        optimized_sql_plan = OptimizedSQLPlan(
            input_plan_hash=input_plan_hash,
            output_plan_hash=output_plan_hash,
            applied_passes=pass_records,
            rejected_directives=[],
            column_pruning_removed=pruned_cols,
            predicate_normalizations=norm_records,
            eliminated_sorts=eliminated_sorts,
            constant_folds=fold_records,
        )

        return CoreCompileResult(
            raw_sql=raw_sql,
            optimized_plan=plan,
            optimized_sql_plan=optimized_sql_plan,
            input_plan_hash=input_plan_hash,
        )

    def compile(self, plan: SqlBuildPlan) -> CompiledSql:
        """编译 SqlBuildPlan 为 CompiledSql——单语句 STANDALONE 注释。

        Args:
            plan: 经 Validator 验证通过的 SqlBuildPlan

        Returns:
            CompiledSql——含 SQL 文本（以注释块开头）、SHA-256、优化记录

        Raises:
            ValueError: plan.steps 为空
        """
        core = self._compile_core(plan)

        # ── 生成 STANDALONE 注释 ──
        comment = self._render_standalone_comment(plan, core.optimized_plan)
        final_sql = f"{comment}\n\n{core.raw_sql}"

        # ── 确定性 hash（基于最终 SQL，含注释） ──
        sql_sha256 = CompiledSql.compute_sql_hash(final_sql, COMPILER_VERSION)

        return CompiledSql(
            sql=final_sql,
            sql_sha256=sql_sha256,
            optimized_plan=core.optimized_sql_plan,
            compiler_version=COMPILER_VERSION,
            input_plan_hash=core.input_plan_hash,
        )

    def compile_to_artifact(
        self,
        plan: SqlBuildPlan,
        spec_hash: str,
        hypothesis_id: str | None = None,
    ) -> SqlArtifact:
        """编译并包装为完整 SqlArtifact——含完整溯源链。

        与 compile() 的区别：compile() 输出 CompiledSql（纯编译产物），
        compile_to_artifact() 输出 SqlArtifact（绑定 spec/plan/hypothesis 溯源）。

        Args:
            plan: 经 Validator 验证通过的 SqlBuildPlan
            spec_hash: 对应 ParsedDeveloperSpec.spec_hash——溯源到输入
            hypothesis_id: 对应 RelationshipHypothesis.hypothesis_id（单表时为 None）

        Returns:
            SqlArtifact——含 CompiledSql + 完整溯源 ID 链
        """
        compiled = self.compile(plan)
        artifact_id = SqlArtifact.generate_artifact_id(
            plan.plan_id, COMPILER_VERSION
        )
        return SqlArtifact(
            artifact_id=artifact_id,
            compiled_sql=compiled,
            spec_hash=spec_hash,
            plan_id=plan.plan_id,
            hypothesis_id=hypothesis_id,
        )

    # ── SQL 渲染 ──

    def _render_sql(self, plan: SqlBuildPlan) -> str:
        """将 SqlBuildPlan 渲染为完整的 DuckDB SQL 字符串。

        当 FilterStep 引用窗口函数别名时，自动切换为子查询包裹模式：
        SELECT * FROM (
          SELECT ..., ROW_NUMBER() OVER(...) AS rn FROM ... GROUP BY ...
        ) _sub WHERE _sub.rn <= N

        Phase 3B 新增：CaseWhenStep 和 WindowStep 渲染到 SELECT 子句。
        Phase 5 新增：窗口过滤子查询包裹——支持全部 8 种窗口函数。
        """
        # ── 收集窗口函数别名 ──
        window_aliases: set[str] = self._collect_window_aliases(plan)

        # ── 检测是否有 FilterStep 引用窗口别名 ──
        needs_window_wrap = False
        if window_aliases:
            for step in plan.steps:
                if isinstance(step, FilterStep):
                    if self._predicate_references_any(
                        step.predicate, window_aliases
                    ):
                        needs_window_wrap = True
                        break

        if needs_window_wrap:
            return self._render_window_wrapped_sql(plan, window_aliases)
        else:
            return self._render_flat_sql(plan)

    def _collect_window_aliases(self, plan: SqlBuildPlan) -> set[str]:
        """收集计划中所有 WindowStep 产出的别名。"""
        aliases: set[str] = set()
        for step in plan.steps:
            if isinstance(step, WindowStep):
                for wexpr in step.window_exprs:
                    if wexpr.alias:
                        aliases.add(str(wexpr.alias))
        return aliases

    def _predicate_references_any(
        self, pred: Predicate, aliases: set[str]
    ) -> bool:
        """递归检查 Predicate 树是否引用了给定别名集合中的任何列。

        检查 ColumnRef.column_name 是否在 aliases 中，
        对嵌套 Predicate（AND/OR/NOT）递归遍历。

        Args:
            pred: 待检查的谓词树
            aliases: 窗口函数别名集合

        Returns:
            True 如果谓词引用了至少一个给定别名
        """
        # 检查 left 侧
        col_name = getattr(pred.left, "column_name", None)
        if col_name and col_name in aliases:
            return True
        if isinstance(pred.left, Predicate):
            if self._predicate_references_any(pred.left, aliases):
                return True

        # 检查 right 侧
        if pred.right is not None:
            if isinstance(pred.right, Predicate):
                if self._predicate_references_any(pred.right, aliases):
                    return True
            elif not isinstance(pred.right, list):
                col_name = getattr(pred.right, "column_name", None)
                if col_name and col_name in aliases:
                    return True

        return False

    def _render_window_wrapped_sql(
        self, plan: SqlBuildPlan, window_aliases: set[str]
    ) -> str:
        """窗口过滤子查询包裹渲染——全部 8 种窗口函数通用。

        将计划拆分为内层（到最后一个 WindowStep 为止）和外层：
        内层：SELECT agg_cols, window_cols FROM ... GROUP BY ...
        外层：SELECT * FROM (内层) AS _sub WHERE post_window_filters ORDER BY ... LIMIT ...

        不引入 CTE，使用 FROM 子句派生表。

        Args:
            plan: 含 WindowStep + 引用窗口列的 FilterStep 的 SqlBuildPlan
            window_aliases: WindowStep 产出的别名集合

        Returns:
            包裹后的完整 SQL 字符串
        """
        from tianshu_datadev.planning.sql_build_plan import (
            FilterStep,
            LimitStep,
            SortStep,
            WindowStep,
        )

        # ── 找到最后一个 WindowStep 的索引作为拆分点 ──
        split_idx = 0
        for i, step in enumerate(plan.steps):
            if isinstance(step, WindowStep):
                split_idx = i

        inner_steps = plan.steps[: split_idx + 1]
        outer_steps = plan.steps[split_idx + 1:]

        # ── 渲染内层 SQL ──
        inner_plan = plan.model_copy(update={"steps": list(inner_steps)})
        inner_sql = self._render_flat_sql(inner_plan)

        # ── 渲染外层 ──
        outer_where: list[str] = []
        outer_order_by: list[str] = []
        outer_limit: str = ""

        for step in outer_steps:
            if isinstance(step, FilterStep):
                # 重写 predicate——列引用加上 _sub. 前缀
                where_sql = self._render_predicate_with_prefix(
                    step.predicate, "_sub"
                )
                if where_sql:
                    outer_where.append(where_sql)
            elif isinstance(step, SortStep):
                for s in step.order_by:
                    direction = (
                        s.direction.value
                        if hasattr(s.direction, "value")
                        else str(s.direction)
                    )
                    outer_order_by.append(f"_sub.{s.column} {direction}")
            elif isinstance(step, LimitStep):
                if step.offset is not None:
                    outer_limit = (
                        f"LIMIT {step.limit} OFFSET {step.offset}"
                    )
                else:
                    outer_limit = f"LIMIT {step.limit}"
            # ProjectStep 在外层不再处理——外层统一用 SELECT *

        # ── 组装外层 SQL ──
        outer_parts: list[str] = []
        outer_parts.append("SELECT *")
        outer_parts.append(f"FROM (\n{inner_sql}\n) AS _sub")

        if outer_where:
            outer_parts.append(f"WHERE\n  {' AND '.join(outer_where)}")

        if outer_order_by:
            outer_parts.append(
                f"ORDER BY\n  {', '.join(outer_order_by)}"
            )

        if outer_limit:
            outer_parts.append(outer_limit)

        return "\n".join(outer_parts)

    def _render_predicate_with_prefix(
        self, pred: Predicate, prefix: str
    ) -> str:
        """渲染 Predicate 并将所有 ColumnRef 的 table_ref 替换为给定前缀。

        用于子查询包裹场景——内层列在外部需要通过 _sub. 前缀引用。

        Args:
            pred: 待渲染的 Predicate
            prefix: 表前缀（如 "_sub"）

        Returns:
            SQL 表达式字符串
        """
        left_str = self._render_predicate_operand_with_prefix(
            pred.left, prefix
        )
        right_str = self._render_predicate_operand_with_prefix(
            pred.right, prefix
        )

        op = pred.operator
        op_str = self._operator_to_sql(op)

        if op in (PredicateOperator.AND, PredicateOperator.OR):
            return f"({left_str} {op_str} {right_str})"

        if op == PredicateOperator.NOT:
            return f"NOT ({left_str})"

        if op in (PredicateOperator.IS_NULL, PredicateOperator.IS_NOT_NULL):
            return f"{left_str} {op_str}"

        if op == PredicateOperator.IN:
            if isinstance(pred.right, list):
                values = ", ".join(
                    self._render_literal(v) for v in pred.right
                )
                return f"{left_str} IN ({values})"
            return f"{left_str} IN ({right_str})"

        if op == PredicateOperator.NOT_IN:
            if isinstance(pred.right, list):
                values = ", ".join(
                    self._render_literal(v) for v in pred.right
                )
                return f"{left_str} NOT IN ({values})"
            return f"{left_str} NOT IN ({right_str})"

        if op == PredicateOperator.BETWEEN:
            if isinstance(pred.right, list) and len(pred.right) == 2:
                low = self._render_literal(pred.right[0])
                high = self._render_literal(pred.right[1])
                return f"{left_str} BETWEEN {low} AND {high}"

        if op == PredicateOperator.LIKE:
            return f"{left_str} LIKE {right_str}"

        return f"{left_str} {op_str} {right_str}"

    def _render_predicate_operand_with_prefix(
        self, operand, prefix: str
    ) -> str:
        """渲染 Predicate 操作数——ColumnRef 加前缀，嵌套 Predicate 递归处理。

        Args:
            operand: ColumnRef / Predicate / SqlLiteral / None
            prefix: 表前缀

        Returns:
            SQL 表达式字符串
        """
        if operand is None:
            return "NULL"
        if isinstance(operand, Predicate):
            return self._render_predicate_with_prefix(operand, prefix)
        if isinstance(operand, SqlLiteral):
            return self._render_literal(operand)
        if hasattr(operand, "column_name"):
            col = operand.column_name
            return f"{prefix}.{col}"
        return str(operand)

    def _render_flat_sql(self, plan: SqlBuildPlan) -> str:
        """标准扁平 SQL 渲染——不含窗口过滤子查询包裹。"""
        # 收集各子句
        select_cols: list[str] = []
        from_parts: list[str] = []
        join_parts: list[str] = []
        where_parts: list[str] = []
        group_by_parts: list[str] = []
        having_clause: str = ""
        order_by_parts: list[str] = []
        limit_clause: str = ""

        has_aggregation = False  # 追踪是否已处理 AggregateStep
        # 记录所有被 JOIN 的右表——后面组装 FROM 时排除，避免重复引用
        joined_tables: set[str] = set()
        # CASE WHEN 列和窗口函数列（Phase 3B）
        case_when_cols: list[str] = []
        window_cols: list[str] = []

        for step in plan.steps:
            if isinstance(step, ScanStep):
                table_name = self._resolve_table(step.table_ref)
                from_parts.append(f"{table_name} AS {step.table_ref}")
                # 从 required_columns 提取初版 select（后续 ProjectStep 覆盖）
                if not select_cols:
                    select_cols = [
                        f"{step.table_ref}.{c.column_name}"
                        for c in step.required_columns
                    ]

            elif isinstance(step, FilterStep):
                where_sql = self._render_predicate(step.predicate)
                if where_sql:
                    where_parts.append(where_sql)

            elif isinstance(step, JoinStep):
                # 记录右表——后续 FROM 组装时排除，避免 FROM a, b JOIN b 的重复引用
                joined_tables.add(str(step.right_table_ref))
                join_clause = self._render_join(step)
                if join_clause:
                    join_parts.append(join_clause)

            elif isinstance(step, AggregateStep):
                # 聚合函数渲染
                agg_cols = self._render_aggregate(step)
                if agg_cols:
                    select_cols = agg_cols  # 聚合覆盖 select
                    has_aggregation = True  # 标记已聚合，后续 ProjectStep 不覆盖
                # GROUP BY
                for gk in step.group_keys:
                    if gk.table_ref:
                        group_by_parts.append(f"{gk.table_ref}.{gk.column_name}")
                    else:
                        group_by_parts.append(gk.column_name)
                # HAVING
                if step.having:
                    having_clause = self._render_predicate(step.having)

            elif isinstance(step, ProjectStep):
                if has_aggregation:
                    # 聚合后投影：保留聚合表达式，仅按 ProjectStep 定义的列顺序重排
                    reordered = self._reorder_aggregation_cols(step, select_cols)
                    if reordered:
                        select_cols = reordered
                else:
                    proj_cols = self._render_project(step)
                    if proj_cols:
                        select_cols = proj_cols

            elif isinstance(step, CaseWhenStep):
                # Phase 3B：渲染 CASE WHEN 表达式
                case_sql = self._render_case_when(step)
                if case_sql:
                    case_when_cols.append(case_sql)

            elif isinstance(step, WindowStep):
                # Phase 3B：渲染窗口函数表达式
                for wexpr in step.window_exprs:
                    win_sql = self._render_window_expr(wexpr)
                    if win_sql:
                        window_cols.append(win_sql)

            elif isinstance(step, SortStep):
                for s in step.order_by:
                    direction = s.direction.value if hasattr(s.direction, "value") else str(s.direction)
                    order_by_parts.append(f"{s.column} {direction}")

            elif isinstance(step, LimitStep):
                if step.offset is not None:
                    limit_clause = f"LIMIT {step.limit} OFFSET {step.offset}"
                else:
                    limit_clause = f"LIMIT {step.limit}"

            elif isinstance(step, SubqueryStep):
                # Phase 4.6 Step 2：递归渲染子查询
                sub_sql = self._render_subquery_step(step)
                if sub_sql:
                    from_parts.append(sub_sql)

        # ── 合并 SELECT 列：基础列 + CASE WHEN + 窗口函数 ──
        all_select_cols = select_cols + case_when_cols + window_cols

        # ── 组装 SQL ──
        sql_parts: list[str] = []

        # SELECT
        if all_select_cols:
            sql_parts.append(f"SELECT\n  {', '.join(all_select_cols)}")
        else:
            sql_parts.append("SELECT *")

        # FROM——排除已在 JOIN 子句中出现的右表，避免 FROM a, b JOIN b 的重复引用
        if from_parts:
            if joined_tables:
                from_parts_filtered = [
                    fp for fp in from_parts
                    if not any(fp.endswith(f" AS {jt}") for jt in joined_tables)
                ]
            else:
                from_parts_filtered = from_parts
            if from_parts_filtered:
                sql_parts.append(f"FROM\n  {' , '.join(from_parts_filtered)}")
            else:
                sql_parts.append("FROM (VALUES (1)) AS _empty")  # 所有表都被 JOIN 覆盖时的 fallback
        else:
            sql_parts.append("FROM (VALUES (1)) AS _empty")  # fallback

        # JOIN
        for jp in join_parts:
            sql_parts.append(jp)

        # WHERE
        if where_parts:
            sql_parts.append(f"WHERE\n  {' AND '.join(where_parts)}")

        # GROUP BY
        if group_by_parts:
            sql_parts.append(f"GROUP BY\n  {', '.join(group_by_parts)}")

        # HAVING
        if having_clause:
            sql_parts.append(f"HAVING\n  {having_clause}")

        # ORDER BY
        if order_by_parts:
            sql_parts.append(f"ORDER BY\n  {', '.join(order_by_parts)}")

        # LIMIT
        if limit_clause:
            sql_parts.append(limit_clause)

        return "\n".join(sql_parts)

    # ── 子句渲染 ──

    def _render_subquery_step(self, step: SubqueryStep) -> str:
        """V-010 兼容渲染——递归编译内层 SqlBuildPlan 为派生表。

        渲染为 (SELECT ... ) AS alias 形式，不使用 CTE（WITH ... AS）。
        内层计划通过递归调用 _render_sql() 生成完整 SQL 片段。

        Args:
            step: SubqueryStep——含嵌套 SqlBuildPlan

        Returns:
            派生表 SQL 片段，如 "(SELECT o.product_id, SUM(...) FROM ...) AS order_agg"
        """
        inner_sql = self._render_sql(step.inner_plan)
        # 确保内层 SQL 有实质内容
        if not inner_sql.strip():
            raise ValueError(
                f"子查询 '{step.alias}' 的内层计划渲染为空——"
                f"inner_plan.steps 可能为空"
            )
        return f"(\n{inner_sql}\n) AS {step.alias}"

    def _render_join(self, step: JoinStep) -> str:
        """渲染 JoinStep 为 JOIN 子句。

        CROSS JOIN 无 ON 子句——join_keys 为空列表。
        """
        join_type = step.join_type.value if hasattr(step.join_type, "value") else str(step.join_type)
        right_table = self._resolve_table(step.right_table_ref)

        # CROSS JOIN——无等值条件
        if join_type == "CROSS" or not step.join_keys:
            return f"CROSS JOIN\n  {right_table} AS {step.right_table_ref}"

        # 渲染 join keys
        on_parts: list[str] = []
        for left_key, right_key in step.join_keys:
            on_parts.append(
                f"{left_key.table_ref}.{left_key.column_name} = "
                f"{step.right_table_ref}.{right_key.column_name}"
            )

        on_clause = " AND ".join(on_parts)
        return f"{join_type} JOIN\n  {right_table} AS {step.right_table_ref}\n  ON {on_clause}"

    def _render_aggregate(self, step: AggregateStep) -> list[str]:
        """渲染聚合步骤为 SELECT 列列表。

        Phase 4D 扩展：
        - distinct=True → SUM(DISTINCT col) 等去重聚合
        - input_expression → 多字段表达式（如 "quantity * unit_price"）
        - filter → FILTER (WHERE ...) 条件聚合
        """
        cols: list[str] = []

        # GROUP BY 列
        for gk in step.group_keys:
            if gk.table_ref:
                cols.append(f"{gk.table_ref}.{gk.column_name}")
            else:
                cols.append(gk.column_name)

        # 聚合指标
        for m in step.metrics:
            # ── 列前缀——自引用/多表场景消除歧义 ──
            col_prefix = f"{m.source_table}." if m.source_table else ""

            # 确定聚合输入：
            #   优先级：input_expression > input_column > "*"
            if m.input_expression:
                # 多字段表达式——如 "quantity * unit_price"（不加前缀，已是完整表达式）
                input_part = m.input_expression
            elif m.input_column:
                if m.distinct and m.aggregation != AggregationType.COUNT_DISTINCT:
                    # SUM(DISTINCT col) 等去重聚合（COUNT_DISTINCT 已独立处理）
                    input_part = f"DISTINCT {col_prefix}{m.input_column}"
                else:
                    input_part = f"{col_prefix}{m.input_column}"
            else:
                input_part = "*"

            # 渲染聚合函数
            agg_func = m.aggregation
            if agg_func == AggregationType.COUNT_DISTINCT:
                if m.input_column:
                    agg_expr = f"COUNT(DISTINCT {col_prefix}{m.input_column})"
                else:
                    agg_expr = "COUNT(DISTINCT *)"
            else:
                agg_expr = f"{agg_func.value}({input_part})"

            # FILTER 子句——条件聚合（如 FILTER (WHERE status = 'STANDARD')）
            if m.filter:
                filter_sql = self._render_metric_filter(m.filter)
                if filter_sql:
                    agg_expr = f"{agg_expr} FILTER (WHERE {filter_sql})"

            cols.append(f"{agg_expr} AS {m.alias}")

        return cols

    def _render_metric_filter(self, filter_decl) -> str:
        """渲染 MetricFilterDecl 为 SQL WHERE 片段——用于 FILTER 子句。

        操作符映射：
          eq → =, neq → !=, gt → >, gte → >=, lt → <, lte → <=,
          in → IN, is_null → IS NULL, is_not_null → IS NOT NULL

        Args:
            filter_decl: MetricFilterDecl 实例

        Returns:
            SQL WHERE 片段（不含 WHERE 关键字），如 "status = 'STANDARD'"
        """
        op_map = {
            "eq": "=", "neq": "!=",
            "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
            "in": "IN",
            "is_null": "IS NULL", "is_not_null": "IS NOT NULL",
        }
        sql_op = op_map.get(filter_decl.operator, "=")
        col = filter_decl.column

        if filter_decl.operator in ("is_null", "is_not_null"):
            return f"{col} {sql_op}"
        elif filter_decl.operator == "in":
            # value 应为逗号分隔的列表字符串
            return f"{col} IN ({filter_decl.value})"
        else:
            # 值加单引号——编译器无法在编译时确定列类型，统一加引号
            escaped = str(filter_decl.value).replace("'", "''")
            return f"{col} {sql_op} '{escaped}'"

    def _render_project(self, step: ProjectStep) -> list[str]:
        """渲染投影步骤为 SELECT 列列表。

        Phase 3B 扩展：支持 WindowExpr 在 AliasExpr.expression 中。
        """
        cols: list[str] = []
        for ae in step.columns:
            expr = ae.expression
            # Phase 3B：窗口表达式
            if isinstance(expr, WindowExpr):
                col_expr = self._render_window_expr(expr)
                if col_expr:
                    cols.append(col_expr)
            elif hasattr(expr, "column_name"):
                col_expr = expr.column_name
                # 有 table_ref 时添加表前缀——消除 Join 后列歧义
                if hasattr(expr, "table_ref") and expr.table_ref:
                    col_expr = f"{expr.table_ref}.{col_expr}"
                if ae.alias and ae.alias != expr.column_name:
                    cols.append(f"{col_expr} AS {ae.alias}")
                else:
                    cols.append(col_expr)
            else:
                col_expr = str(expr)
                if ae.alias and ae.alias != col_expr:
                    cols.append(f"{col_expr} AS {ae.alias}")
                else:
                    cols.append(col_expr)
        return cols

    def _reorder_aggregation_cols(
        self, step: ProjectStep, agg_select_cols: list[str]
    ) -> list[str]:
        """聚合后投影：按 ProjectStep 定义的输出列顺序重排聚合表达式。

        AggregateStep 已生成正确的聚合 SELECT 表达式（如 "COUNT(id) AS pv"），
        此方法仅按 ProjectStep.columns 的 alias 顺序重排，不覆盖表达式本体。

        匹配规则：
        - 聚合列格式为 "FUNC(...) AS alias" → 按 alias 匹配
        - GROUP BY 列格式为 "table.col" → 按 col 名匹配

        Args:
            step: ProjectStep，定义最终输出列顺序
            agg_select_cols: AggregateStep 生成的 SELECT 列列表

        Returns:
            重排后的 SELECT 列列表
        """
        reordered: list[str] = []
        remaining = list(agg_select_cols)  # 可修改的副本，用于已匹配去重

        for ae in step.columns:
            target_alias = str(ae.alias) if ae.alias else ""
            matched = None
            for i, col_expr in enumerate(remaining):
                # 匹配聚合指标：格式 "FUNC(...) AS <alias>"
                if target_alias and col_expr.endswith(f" AS {target_alias}"):
                    matched = remaining.pop(i)
                    break
                # 匹配 GROUP BY 列：格式 "table_ref.column_name"
                if "." in col_expr:
                    col_name = col_expr.rsplit(".", 1)[-1]
                    if col_name == target_alias:
                        matched = remaining.pop(i)
                        break
            if matched is not None:
                reordered.append(matched)
            else:
                # 列不在聚合结果中——可能是 WindowExpr 或计算列，回退到 _render_project 单列渲染
                reordered.append(self._render_single_project_alias(ae))

        return reordered

    def _render_single_project_alias(self, ae) -> str:
        """渲染单个 AliasExpr 为 SQL 列表达式——用于 _reorder_aggregation_cols 的回退路径。"""
        expr = ae.expression
        if isinstance(expr, WindowExpr):
            return self._render_window_expr(expr)
        elif hasattr(expr, "column_name"):
            col_expr = expr.column_name
            if ae.alias and ae.alias != col_expr:
                return f"{col_expr} AS {ae.alias}"
            return col_expr
        else:
            col_expr = str(expr)
            if ae.alias and ae.alias != col_expr:
                return f"{col_expr} AS {ae.alias}"
            return col_expr

    # ── Phase 3B：CASE WHEN 渲染 ──

    def _render_case_when(self, step: CaseWhenStep) -> str:
        """渲染 CaseWhenStep 为 CASE WHEN SQL 表达式。

        输出格式：
            CASE
              WHEN <condition> THEN <result>
              [WHEN <condition> THEN <result> ...]
              [ELSE <else_value>]
            END AS <alias>

        Args:
            step: CASE WHEN 步骤

        Returns:
            完整的 CASE WHEN ... END AS alias SQL 表达式
        """
        if not step.cases:
            return ""

        parts: list[str] = ["CASE"]

        for branch in step.cases:
            cond_sql = self._render_predicate(branch.condition)
            result_sql = self._render_literal(branch.result)
            parts.append(f"  WHEN {cond_sql} THEN {result_sql}")

        if step.else_value is not None:
            else_sql = self._render_literal(step.else_value)
            parts.append(f"  ELSE {else_sql}")

        parts.append("END")

        case_expr = "\n".join(parts)
        if step.alias:
            return f"{case_expr} AS {step.alias}"
        return case_expr

    # ── Phase 3B：窗口函数渲染 ──

    def _render_window_expr(self, wexpr: WindowExpr) -> str:
        """渲染 WindowExpr 为带 OVER 子句的窗口函数 SQL 表达式。

        输出格式：
            <FUNCTION>(<input>) OVER (
              [PARTITION BY <cols>]
              [ORDER BY <cols>]
              [<frame>]
            ) AS <alias>

        支持 9 种白名单窗口函数：
        - ROW_NUMBER / RANK / DENSE_RANK：无参数
        - NTILE：整数参数（桶数）
        - LAG / LEAD：单参数（列引用）
        - SUM_OVER / AVG_OVER / COUNT_OVER：单参数（列引用）

        Args:
            wexpr: 窗口函数表达式

        Returns:
            完整的窗口函数 SQL 表达式（含 OVER 子句）
        """
        # 函数名映射：WindowFunction 枚举 → SQL 函数名
        func_name_map = {
            "ROW_NUMBER": "ROW_NUMBER()",
            "RANK": "RANK()",
            "DENSE_RANK": "DENSE_RANK()",
            "NTILE": "NTILE",
            "LAG": "LAG",
            "LEAD": "LEAD",
            "SUM_OVER": "SUM",
            "AVG_OVER": "AVG",
            "COUNT_OVER": "COUNT",
        }

        func_value = wexpr.function.value if hasattr(wexpr.function, "value") else str(wexpr.function)
        sql_func = func_name_map.get(func_value, func_value)

        # 构建函数调用
        if func_value in ("ROW_NUMBER", "RANK", "DENSE_RANK"):
            func_call = sql_func  # 无参数
        else:
            # 有参数的窗口函数
            if wexpr.input is not None:
                input_str = self._render_window_input(wexpr.input)
                func_call = f"{sql_func}({input_str})"
            else:
                func_call = f"{sql_func}()"

        # OVER 子句
        over_parts: list[str] = []

        if wexpr.partition_by:
            partition_cols = [
                f"{c.table_ref}.{c.column_name}" if c.table_ref else c.column_name
                for c in wexpr.partition_by
            ]
            over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")

        if wexpr.order_by:
            order_cols = [
                f"{s.column} {s.direction.value if hasattr(s.direction, 'value') else str(s.direction)}"
                for s in wexpr.order_by
            ]
            over_parts.append(f"ORDER BY {', '.join(order_cols)}")

        if wexpr.frame is not None:
            frame_sql = self._render_window_frame(wexpr.frame)
            if frame_sql:
                over_parts.append(frame_sql)

        if over_parts:
            over_clause = "\n  ".join(over_parts)
            result = f"{func_call} OVER (\n  {over_clause}\n)"
        else:
            result = f"{func_call} OVER ()"

        if wexpr.alias:
            return f"{result} AS {wexpr.alias}"
        return result

    def _render_window_input(self, win_input) -> str:
        """渲染窗口函数的输入参数。

        Args:
            win_input: ColumnRef 或 SqlLiteral

        Returns:
            SQL 输入表达式
        """
        if hasattr(win_input, "table_ref") and hasattr(win_input, "column_name"):
            # ColumnRef
            if win_input.table_ref:
                return f"{win_input.table_ref}.{win_input.column_name}"
            return win_input.column_name
        # SqlLiteral
        return self._render_literal(win_input)

    def _render_window_frame(self, frame) -> str:
        """渲染 WindowFrame 为 SQL 窗口帧子句。

        输出格式：ROWS BETWEEN <start> AND <end>
                  RANGE BETWEEN <start> AND <end>

        Args:
            frame: WindowFrame 对象

        Returns:
            窗口帧 SQL 字符串
        """
        frame_type = frame.frame_type.value if hasattr(frame.frame_type, "value") else str(frame.frame_type)
        start_str = self._render_frame_boundary(frame.start)
        end_str = self._render_frame_boundary(frame.end)
        return f"{frame_type} BETWEEN {start_str} AND {end_str}"

    def _render_frame_boundary(self, boundary) -> str:
        """渲染 FrameBoundary 为 SQL 边界表达式。

        Args:
            boundary: FrameBoundary 对象

        Returns:
            SQL 边界字符串（如 UNBOUNDED PRECEDING、CURRENT ROW、3 PRECEDING）
        """
        kind = boundary.kind
        kind_value = kind.value if hasattr(kind, "value") else str(kind)

        kind_map = {
            "CURRENT_ROW": "CURRENT ROW",
            "UNBOUNDED_PRECEDING": "UNBOUNDED PRECEDING",
            "UNBOUNDED_FOLLOWING": "UNBOUNDED FOLLOWING",
            "N_PRECEDING": f"{boundary.offset} PRECEDING",
            "N_FOLLOWING": f"{boundary.offset} FOLLOWING",
        }
        return kind_map.get(kind_value, str(kind_value))

    # ── Predicate 渲染 ──

    def _render_predicate(self, pred: Predicate) -> str:
        """递归渲染 Predicate 为 DuckDB SQL 表达式。

        示例：
        - Predicate(ColumnRef("t","amt"), GT, Literal(100)) → "t.amt > 100"
        - Predicate(Predicate(...), AND, Predicate(...)) → "(...) AND (...)"
        - Predicate(ColumnRef("t","status"), IN, [Literal("a"), Literal("b")]) → "t.status IN ('a', 'b')"
        """
        left_str = self._render_predicate_operand(pred.left)
        right_str = self._render_predicate_operand(pred.right)

        op = pred.operator
        op_str = self._operator_to_sql(op)

        if op in (PredicateOperator.AND, PredicateOperator.OR):
            return f"({left_str} {op_str} {right_str})"

        if op == PredicateOperator.NOT:
            return f"NOT ({left_str})"

        if op in (PredicateOperator.IS_NULL, PredicateOperator.IS_NOT_NULL):
            return f"{left_str} {op_str}"

        if op == PredicateOperator.IN:
            if isinstance(pred.right, list):
                values = ", ".join(self._render_literal(v) for v in pred.right)
                return f"{left_str} IN ({values})"
            return f"{left_str} IN ({right_str})"

        if op == PredicateOperator.NOT_IN:
            if isinstance(pred.right, list):
                values = ", ".join(self._render_literal(v) for v in pred.right)
                return f"{left_str} NOT IN ({values})"
            return f"{left_str} NOT IN ({right_str})"

        if op == PredicateOperator.BETWEEN:
            if isinstance(pred.right, list) and len(pred.right) == 2:
                low = self._render_literal(pred.right[0])
                high = self._render_literal(pred.right[1])
                return f"{left_str} BETWEEN {low} AND {high}"

        if op == PredicateOperator.LIKE:
            return f"{left_str} LIKE {right_str}"

        # 默认二元操作
        return f"{left_str} {op_str} {right_str}"

    def _render_predicate_operand(self, operand) -> str:
        """渲染 Predicate 的操作数。"""
        if operand is None:
            return "NULL"
        if isinstance(operand, Predicate):
            return self._render_predicate(operand)
        if isinstance(operand, SqlLiteral):
            return self._render_literal(operand)
        # ColumnRef
        if hasattr(operand, "table_ref") and hasattr(operand, "column_name"):
            table = operand.table_ref
            col = operand.column_name
            if table:
                return f"{table}.{col}"
            return col
        return str(operand)

    @staticmethod
    def _render_literal(lit: SqlLiteral) -> str:
        """渲染 SqlLiteral 为 SQL 字面量。

        is_sql_expr=True 时 value 为可信 SQL 表达式（如 CURRENT_DATE - INTERVAL 30 DAY），
        直接渲染不加引号——仅 Builder 确定性代码可设置此标志。
        """
        if lit.is_sql_expr:
            return str(lit.value)
        v = lit.value
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, str):
            # 转义单引号
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        return str(v)

    @staticmethod
    def _operator_to_sql(op: PredicateOperator) -> str:
        """将 PredicateOperator 映射到 SQL 操作符字符串。"""
        mapping = {
            PredicateOperator.EQ: "=",
            PredicateOperator.NEQ: "!=",
            PredicateOperator.GT: ">",
            PredicateOperator.GTE: ">=",
            PredicateOperator.LT: "<",
            PredicateOperator.LTE: "<=",
            PredicateOperator.AND: "AND",
            PredicateOperator.OR: "OR",
            PredicateOperator.NOT: "NOT",
            PredicateOperator.IN: "IN",
            PredicateOperator.NOT_IN: "NOT IN",
            PredicateOperator.BETWEEN: "BETWEEN",
            PredicateOperator.IS_NULL: "IS NULL",
            PredicateOperator.IS_NOT_NULL: "IS NOT NULL",
            PredicateOperator.LIKE: "LIKE",
        }
        return mapping.get(op, str(op.value))

    # ── 注释安全清洗 ──

    @staticmethod
    def _render_comment_line(label: str, value: str) -> str:
        """安全渲染单行注释——清洗控制字符、换行、注释破坏序列。

        规则：
        1. 替换 CR/LF 为空格
        2. 移除 C0 控制字符（0x00-0x1F，除 \\t 外）
        3. 连续 "--" 替换为 "- -"（防止注释提前终止）
        4. 首尾空白 trim
        5. 统一前缀 "-- {label}: "
        """
        cleaned = str(value)
        cleaned = cleaned.replace("\r", " ").replace("\n", " ")
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
        cleaned = re.sub(r"--+", "- -", cleaned)
        cleaned = cleaned.strip()
        return f"-- {label}: {cleaned}"

    # ── 注释块渲染 ──

    def _render_statement_comment(
        self,
        stmt: SqlStatement,
        program: SqlProgram,
        optimized_plan: SqlBuildPlan,
    ) -> str:
        """从 intent + 优化后 plan 生成 5 行注释块。

        Args:
            stmt: 当前语句（含 intent）
            program: 所属 SqlProgram（取 final_output_target）
            optimized_plan: 优化后的 SqlBuildPlan（保证注释与最终 SQL 一致）

        Returns:
            完整的 5 行注释块字符串
        """
        # ── Step 标签 ──
        if stmt.kind == StatementKind.STANDALONE:
            step_label = f"Standalone Query: {stmt.plan.plan_id[:8]}"
        elif stmt.kind == StatementKind.FINAL:
            step_label = f"Final Output: {program.final_output_target or 'result'}"
        else:
            step_label = stmt.produces or stmt.statement_id

        # ── Intent ──
        intent = stmt.intent or "（无描述）"

        # ── Operation ──
        operation = self._derive_operation_description(optimized_plan)

        # ── Inputs ──
        inputs = self._derive_input_tables(optimized_plan)

        # ── Output ──
        if stmt.kind == StatementKind.FINAL:
            output_str = program.final_output_target or "(最终结果集)"
        elif stmt.kind == StatementKind.STANDALONE:
            output_str = "(直接返回)"
        else:
            output_str = stmt.produces or "(中间结果)"

        lines = [
            self._render_comment_line("Step", step_label),
            self._render_comment_line("Intent", intent),
            self._render_comment_line("Operation", operation),
            self._render_comment_line("Inputs", inputs),
            self._render_comment_line("Output", output_str),
        ]
        return "\n".join(lines)

    def _render_standalone_comment(
        self, plan: SqlBuildPlan, optimized_plan: SqlBuildPlan,
    ) -> str:
        """为 compile() 单语句生成 STANDALONE 注释块。

        与 _render_statement_comment() 不同——compile() 没有 SqlStatement 和
        SqlProgram 上下文。此方法内部构造 transient 对象驱动渲染，不暴露到 artifact。

        Args:
            plan: 原始 SqlBuildPlan（取 plan_id）
            optimized_plan: 优化后的 SqlBuildPlan

        Returns:
            完整 5 行注释块字符串
        """
        transient_stmt = SqlStatement(
            statement_id=plan.plan_id,
            plan=optimized_plan,
            kind=StatementKind.STANDALONE,
            intent="单语句直接生成目标查询结果。",
        )
        transient_program = SqlProgram(
            program_id="",
            spec_id="",
            statements=[transient_stmt],
        )
        return self._render_statement_comment(
            transient_stmt, transient_program, optimized_plan,
        )

    # ── 表名解析 ──

    @staticmethod
    def _validate_table_mapping(mapping: dict[str, str]) -> None:
        """校验 table_mapping 的 key 和 value 均不含 SQL 注入字符。

        这是 Compiler 层防线——在 _resolve_table() 将物理表名拼入 SQL 之前，
        确保 mapping 中的所有字符串均已通过 allowlist 校验。

        校验规则（与 SafePhysicalTableName 保持一致）：
        - key（table_ref 别名）：必须符合 SafeIdentifier 规范
        - value（物理表名）：必须符合 SafePhysicalTableName 规范
          （支持 schema.table 限定名格式）

        Args:
            mapping: table_ref → physical_table_name 映射

        Raises:
            ValueError: 任一 key 或 value 包含非法字符
        """
        from tianshu_datadev.developer_spec.models import (
            _validate_physical_table_name,
        )
        from tianshu_datadev.planning.models import _SQL_ID_RE

        for key, value in mapping.items():
            # 校验 key（table_ref 别名）——必须符合 SQL 标识符规范
            if not key:
                raise ValueError("table_mapping 的 key 不能为空字符串")
            if not _SQL_ID_RE.match(key):
                raise ValueError(
                    f"table_mapping 的 key（表别名）包含非法字符：'{key}'——"
                    f"必须匹配 {_SQL_ID_RE.pattern}"
                )

            # 校验 value（物理表名）——必须符合物理表名 allowlist
            _validate_physical_table_name(value)

    def _resolve_table(self, table_ref: str) -> str:
        """将 table_ref（别名）解析为物理表名。

        自引用场景（P2-6）：table_ref 可能以 _self_left / _self_right 结尾，
        此时去除后缀后用原始别名查表映射，使两个别名指向同一物理表。
        """
        # 自引用别名回退：去除 _self_left / _self_right 后缀后查映射
        for suffix in ("_self_right", "_self_left"):
            if table_ref.endswith(suffix):
                base = table_ref[: -len(suffix)]
                return self._table_mapping.get(base, base)
        return self._table_mapping.get(table_ref, table_ref)

    # ── Operation 描述 ──

    def _describe_single_step(self, step) -> str | None:
        """单 step → 中文短语——用于 Operation 描述。

        基于优化后 plan.steps，跳过信息量低的 step（如仅含单列的 ProjectStep）。
        """
        if isinstance(step, ScanStep):
            n = len(step.required_columns) if step.required_columns else 0
            table = step.table_ref
            return f"从 {table} 扫描 {n} 个字段" if n else f"从 {table} 扫描"
        elif isinstance(step, FilterStep):
            # 取 predicate 的简要描述
            pred_desc = self._render_predicate(step.predicate)
            # 限制长度，避免注释过长
            if len(pred_desc) > 60:
                pred_desc = pred_desc[:57] + "..."
            return f"过滤条件：{pred_desc}"
        elif isinstance(step, JoinStep):
            keys = ", ".join(
                f"{lk.column_name}={rk.column_name}"
                for lk, rk in step.join_keys
            )
            return f"与 {step.right_table_ref} 按 {keys} 关联"
        elif isinstance(step, AggregateStep):
            keys = ", ".join(gk.column_name for gk in step.group_keys)
            n_metrics = len(step.metrics)
            return f"按 {keys} 分组，聚合 {n_metrics} 个指标"
        elif isinstance(step, WindowStep):
            aliases = ", ".join(
                wexpr.alias for wexpr in step.window_exprs if wexpr.alias
            )
            return f"计算窗口函数：{aliases}" if aliases else "计算窗口函数"
        elif isinstance(step, ProjectStep):
            n = len(step.columns)
            return f"输出 {n} 列" if n > 1 else None  # 单列投影信息量低，跳过
        elif isinstance(step, SortStep):
            cols = ", ".join(s.column for s in step.order_by)
            return f"按 {cols} 排序"
        elif isinstance(step, LimitStep):
            return f"限制 {step.limit} 行"
        elif isinstance(step, CaseWhenStep):
            return f"计算 {step.alias} 分类标签" if step.alias else "计算分类标签"
        return None

    def _derive_operation_description(self, plan: SqlBuildPlan) -> str:
        """从优化后的 plan.steps 生成中文操作描述短语串。

        按 step 顺序提取，连成逗号分隔的一句话。跳过信息量低的 step。
        """
        parts: list[str] = []
        for step in plan.steps:
            desc = self._describe_single_step(step)
            if desc:
                parts.append(desc)
        if not parts:
            return "（无操作描述）"
        return "，".join(parts) + "。"

    def _derive_input_tables(self, plan: SqlBuildPlan) -> str:
        """从 plan.steps 提取输入表名（ScanStep + JoinStep 去重）。

        仅提取非 _temp_ 前缀的原始输入表。
        """
        tables: list[str] = []
        seen: set[str] = set()
        for step in plan.steps:
            if isinstance(step, ScanStep):
                ref = step.table_ref
                if ref not in seen and not ref.startswith("_temp_"):
                    tables.append(ref)
                    seen.add(ref)
            elif isinstance(step, JoinStep):
                ref = step.right_table_ref
                if ref not in seen and not ref.startswith("_temp_"):
                    tables.append(ref)
                    seen.add(ref)
        return ", ".join(tables) if tables else "（无输入表）"

    # ── 多语句编译（Phase 3A） ──

    def compile_program(self, program: SqlProgram) -> SqlProgramArtifact:
        """编译 SqlProgram 为多语句 SQL 产物。

        流程：
        1. 校验 DAG 合法性
        2. 按 topological_order 依次编译每个语句
        3. 为 PRODUCER 语句包装 CREATE TEMP TABLE
        4. 为 CONSUMER 语句注入 _temp 表引用
        5. 生成 cleanup DROP TABLE 语句列表
        6. 组装 SqlProgramArtifact

        Args:
            program: 经过 DAG 校验的 SqlProgram

        Returns:
            SqlProgramArtifact——含每个语句的 CompiledSql + cleanup SQL

        Raises:
            ValueError: DAG 校验失败
        """
        from tianshu_datadev.planning.sql_program import validate_program_dag

        # 1. DAG 校验
        questions = validate_program_dag(program)
        blocking = [q for q in questions if q.blocking]
        if blocking:
            error_msgs = "; ".join(q.description for q in blocking)
            raise ValueError(f"SqlProgram DAG 校验失败：{error_msgs}")

        if not program.topological_order:
            raise ValueError("SqlProgram.topological_order 为空——无法确定编译顺序")

        # 2. 构建 statement_id → SqlStatement 的索引
        stmt_map: dict[str, SqlStatement] = {
            s.statement_id: s for s in program.statements
        }

        # 3. 收集语句索引
        compiled_statements: list[CompiledSql] = []
        cleanup_sqls: list[str] = []

        # 4. 按拓扑序编译每个语句
        for stmt_id in program.topological_order:
            stmt = stmt_map.get(stmt_id)
            if stmt is None:
                raise ValueError(
                    f"topological_order 中引用了不存在的 statement_id：'{stmt_id}'"
                )

            # 构建此语句的 table_mapping
            # 基础映射来自 compiler 实例的 _table_mapping
            stmt_table_mapping: dict[str, str] = dict(self._table_mapping)

            # 注入上游 _temp 表映射——CONSUMER 需要能引用上游 PRODUCER 创建的 _temp 表
            # _temp 表在 DuckDB 中以 temp_id 自身作为物理表名（CREATE TEMP TABLE 时使用 temp_id）
            for dep_id in stmt.depends_on:
                dep_stmt = stmt_map.get(dep_id)
                if dep_stmt and dep_stmt.produces:
                    temp_id = dep_stmt.produces
                    # _temp 表名直接作为物理引用（因为在 CREATE TEMP TABLE 时使用此名称）
                    stmt_table_mapping[temp_id] = temp_id

            # 如果当前语句自己产生 _temp 表，其自身 ScanStep 引用的数据源可能是 CSV 表
            # 也可能是上游 _temp 表（已在上面处理）

            # 创建临时编译器实例用于编译此语句
            stmt_compiler = DuckDbSqlCompiler(table_mapping=stmt_table_mapping)
            core = stmt_compiler._compile_core(stmt.plan)

            # 根据语句类型包装 SQL 并 prepend 注释
            if stmt.kind == StatementKind.PRODUCER and stmt.produces:
                # 先包装 CREATE TEMP TABLE
                wrapped_sql = (
                    f"CREATE TEMP TABLE {stmt.produces} AS\n{core.raw_sql}"
                )
                # 再 prepend 上下文注释
                comment = self._render_statement_comment(
                    stmt, program, core.optimized_plan,
                )
                final_sql = f"{comment}\n\n{wrapped_sql}"
                compiled = CompiledSql(
                    sql=final_sql,
                    sql_sha256=CompiledSql.compute_sql_hash(final_sql, COMPILER_VERSION),
                    optimized_plan=core.optimized_sql_plan,
                    compiler_version=COMPILER_VERSION,
                    input_plan_hash=core.input_plan_hash,
                )
                cleanup_sqls.append(f"DROP TABLE IF EXISTS {stmt.produces}")

            elif stmt.kind == StatementKind.CONSUMER:
                if stmt.produces:
                    wrapped_sql = (
                        f"CREATE TEMP TABLE {stmt.produces} AS\n{core.raw_sql}"
                    )
                    comment = self._render_statement_comment(
                        stmt, program, core.optimized_plan,
                    )
                    final_sql = f"{comment}\n\n{wrapped_sql}"
                    compiled = CompiledSql(
                        sql=final_sql,
                        sql_sha256=CompiledSql.compute_sql_hash(final_sql, COMPILER_VERSION),
                        optimized_plan=core.optimized_sql_plan,
                        compiler_version=COMPILER_VERSION,
                        input_plan_hash=core.input_plan_hash,
                    )
                    cleanup_sqls.append(f"DROP TABLE IF EXISTS {stmt.produces}")
                else:
                    # CONSUMER 不产生 _temp——直接 prepend 注释
                    comment = self._render_statement_comment(
                        stmt, program, core.optimized_plan,
                    )
                    final_sql = f"{comment}\n\n{core.raw_sql}"
                    compiled = CompiledSql(
                        sql=final_sql,
                        sql_sha256=CompiledSql.compute_sql_hash(final_sql, COMPILER_VERSION),
                        optimized_plan=core.optimized_sql_plan,
                        compiler_version=COMPILER_VERSION,
                        input_plan_hash=core.input_plan_hash,
                    )

            else:
                # FINAL / STANDALONE——prepend 注释，无 CREATE TEMP TABLE 包装
                comment = self._render_statement_comment(
                    stmt, program, core.optimized_plan,
                )
                final_sql = f"{comment}\n\n{core.raw_sql}"
                compiled = CompiledSql(
                    sql=final_sql,
                    sql_sha256=CompiledSql.compute_sql_hash(final_sql, COMPILER_VERSION),
                    optimized_plan=core.optimized_sql_plan,
                    compiler_version=COMPILER_VERSION,
                    input_plan_hash=core.input_plan_hash,
                )

            compiled_statements.append(compiled)

        # 5. 组装产物
        program_compiled = ProgramCompiledSql(
            program_id=program.program_id,
            statements=compiled_statements,
            cleanup_sql=cleanup_sqls,
            statement_order=list(program.topological_order),
        )

        artifact_id = SqlProgramArtifact.generate_artifact_id(
            program.program_id, COMPILER_VERSION
        )

        return SqlProgramArtifact(
            artifact_id=artifact_id,
            program_id=program.program_id,
            compiled=program_compiled,
            spec_id=program.spec_id,
            compiler_version=COMPILER_VERSION,
        )


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════
# column_pruning() 和 sort_elimination() 已直接返回裁剪列表和消除列表，
# 不再需要 _extract_* 辅助函数。
