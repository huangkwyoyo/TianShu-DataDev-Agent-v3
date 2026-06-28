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

from tianshu_datadev.planning.models import (
    Predicate,
    PredicateOperator,
    SqlLiteral,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
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

COMPILER_VERSION = "1.0.0"


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
        """
        self._table_mapping = table_mapping or {}

    def compile(self, plan: SqlBuildPlan) -> CompiledSql:
        """编译 SqlBuildPlan 为 CompiledSql。

        Args:
            plan: 经 Validator 验证通过的 SqlBuildPlan

        Returns:
            CompiledSql——含 SQL 文本、SHA-256、优化记录

        Raises:
            ValueError: plan.steps 为空
        """
        if not plan.steps:
            raise ValueError("SqlBuildPlan.steps 为空——无法编译")

        # 最小安全网：确认所有 step 都有 step_id（Validator 通过的基本条件）
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
        sql = self._render_sql(plan)

        # ── 确定性 hash ──
        sql_sha256 = CompiledSql.compute_sql_hash(sql, COMPILER_VERSION)

        # ── 构建 OptimizedSQLPlan ──
        optimized_plan = OptimizedSQLPlan(
            input_plan_hash=input_plan_hash,
            output_plan_hash=output_plan_hash,
            applied_passes=pass_records,
            rejected_directives=[],
            column_pruning_removed=pruned_cols,
            predicate_normalizations=norm_records,
            eliminated_sorts=eliminated_sorts,
            constant_folds=fold_records,
        )

        return CompiledSql(
            sql=sql,
            sql_sha256=sql_sha256,
            optimized_plan=optimized_plan,
            compiler_version=COMPILER_VERSION,
            input_plan_hash=input_plan_hash,
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

        渲染顺序：
        SELECT <project_cols>
        FROM <scan_table>
        [JOIN <right_table> ON <keys>]
        [WHERE <filters>]
        [GROUP BY <group_keys>]
        [HAVING <having>]
        [ORDER BY <sort>]
        [LIMIT <n>]
        """
        # 收集各子句
        select_cols: list[str] = []
        from_parts: list[str] = []
        join_parts: list[str] = []
        where_parts: list[str] = []
        group_by_parts: list[str] = []
        having_clause: str = ""
        order_by_parts: list[str] = []
        limit_clause: str = ""

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
                join_clause = self._render_join(step)
                if join_clause:
                    join_parts.append(join_clause)

            elif isinstance(step, AggregateStep):
                # 聚合函数渲染
                agg_cols = self._render_aggregate(step)
                if agg_cols:
                    select_cols = agg_cols  # 聚合覆盖 select
                # GROUP BY
                for gk in step.group_keys:
                    group_by_parts.append(f"{gk.table_ref}.{gk.column_name}")
                # HAVING
                if step.having:
                    having_clause = self._render_predicate(step.having)

            elif isinstance(step, ProjectStep):
                proj_cols = self._render_project(step)
                if proj_cols:
                    select_cols = proj_cols

            elif isinstance(step, SortStep):
                for s in step.order_by:
                    direction = s.direction.value if hasattr(s.direction, "value") else str(s.direction)
                    order_by_parts.append(f"{s.column} {direction}")

            elif isinstance(step, LimitStep):
                if step.offset is not None:
                    limit_clause = f"LIMIT {step.limit} OFFSET {step.offset}"
                else:
                    limit_clause = f"LIMIT {step.limit}"

        # ── 组装 SQL ──
        sql_parts: list[str] = []

        # SELECT
        if select_cols:
            sql_parts.append(f"SELECT\n  {', '.join(select_cols)}")
        else:
            sql_parts.append("SELECT *")

        # FROM
        if from_parts:
            sql_parts.append(f"FROM\n  {' , '.join(from_parts)}")  # 注意：DuckDB 支持逗号分隔多表
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

    def _render_join(self, step: JoinStep) -> str:
        """渲染 JoinStep 为 JOIN 子句。"""
        join_type = step.join_type.value if hasattr(step.join_type, "value") else str(step.join_type)
        right_table = self._resolve_table(step.right_table_ref)

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
        """渲染聚合步骤为 SELECT 列列表。"""
        cols: list[str] = []

        # GROUP BY 列
        for gk in step.group_keys:
            cols.append(f"{gk.table_ref}.{gk.column_name}")

        # 聚合指标
        for m in step.metrics:
            # 处理聚合函数
            agg_func = m.aggregation if isinstance(m.aggregation, str) else m.aggregation.value
            if agg_func.upper() == "COUNT_DISTINCT":
                if m.input_column:
                    cols.append(f"COUNT(DISTINCT {m.input_column}) AS {m.alias}")
                else:
                    cols.append(f"COUNT(DISTINCT *) AS {m.alias}")
            else:
                if m.input_column:
                    cols.append(f"{agg_func.upper()}({m.input_column}) AS {m.alias}")
                else:
                    # COUNT(*) 的情况
                    cols.append(f"{agg_func.upper()}(*) AS {m.alias}")

        return cols

    def _render_project(self, step: ProjectStep) -> list[str]:
        """渲染投影步骤为 SELECT 列列表。"""
        cols: list[str] = []
        for ae in step.columns:
            # AliasExpr: expression(ColumnRef) + alias
            col_expr = (
                ae.expression.column_name
                if hasattr(ae.expression, "column_name")
                else str(ae.expression)
            )
            if ae.alias and ae.alias != col_expr:
                cols.append(f"{col_expr} AS {ae.alias}")
            else:
                cols.append(col_expr)
        return cols

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
        """渲染 SqlLiteral 为 SQL 字面量。"""
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

    # ── 表名解析 ──

    def _resolve_table(self, table_ref: str) -> str:
        """将 table_ref（别名）解析为物理表名。"""
        return self._table_mapping.get(table_ref, table_ref)

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
            compiled = stmt_compiler.compile(stmt.plan)

            # 根据语句类型包装 SQL
            if stmt.kind == StatementKind.PRODUCER and stmt.produces:
                # 生产者：CREATE TEMP TABLE {temp_id} AS {compiled_sql}
                wrapped_sql = (
                    f"CREATE TEMP TABLE {stmt.produces} AS\n{compiled.sql}"
                )
                # 重新计算 hash（SQL 内容已变）
                wrapped_sql_sha256 = CompiledSql.compute_sql_hash(
                    wrapped_sql, COMPILER_VERSION
                )
                compiled = CompiledSql(
                    sql=wrapped_sql,
                    sql_sha256=wrapped_sql_sha256,
                    optimized_plan=compiled.optimized_plan,
                    compiler_version=compiled.compiler_version,
                    input_plan_hash=compiled.input_plan_hash,
                )
                # 记录需要清理的 _temp 表
                cleanup_sqls.append(f"DROP TABLE IF EXISTS {stmt.produces}")

            elif stmt.kind == StatementKind.CONSUMER:
                # 消费者：直接使用编译结果（_temp 表引用已解析）
                # 如果消费者也产生 _temp 表
                if stmt.produces:
                    wrapped_sql = (
                        f"CREATE TEMP TABLE {stmt.produces} AS\n{compiled.sql}"
                    )
                    wrapped_sql_sha256 = CompiledSql.compute_sql_hash(
                        wrapped_sql, COMPILER_VERSION
                    )
                    compiled = CompiledSql(
                        sql=wrapped_sql,
                        sql_sha256=wrapped_sql_sha256,
                        optimized_plan=compiled.optimized_plan,
                        compiler_version=compiled.compiler_version,
                        input_plan_hash=compiled.input_plan_hash,
                    )
                    cleanup_sqls.append(f"DROP TABLE IF EXISTS {stmt.produces}")

            # FINAL / STANDALONE：直接使用编译结果，无包装

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
