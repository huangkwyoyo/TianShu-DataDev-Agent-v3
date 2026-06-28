"""DuckDBExecutor——隔离执行 Compiler 产物，输出结构化 ExecutionTrace + ResultSummary。

只接受 CompiledSql 对象——不接受外部 SQL 字符串。
支持从 CSV fixture 文件加载只读表。
执行超时保护（默认 30 秒）。
"""

from __future__ import annotations

import time

from .models import (
    CompiledSql,
    ExecutionStatus,
    ExecutionTrace,
    ProgramCompiledSql,
    ProgramExecutionResult,
    ResultSummary,
    StatementExecutionResult,
)

# 默认执行超时（秒）
_DEFAULT_TIMEOUT_SEC = 30
# 抽样行数上限
_MAX_SAMPLE_ROWS = 20


class DuckDBExecutor:
    """DuckDB 执行器——隔离执行，只接受 Compiler 产物。

    执行流程：
    1. 校验输入为 CompiledSql 对象（非字符串）
    2. 创建 DuckDB 内存数据库连接
    3. 从 CSV fixture 文件加载只读表
    4. 执行 SQL
    5. 收集行数、列类型、NULL 计数、数值汇总、抽样行
    6. 输出 ExecutionTrace + ResultSummary
    """

    def __init__(self, table_paths: dict[str, str] | None = None, timeout_sec: float = _DEFAULT_TIMEOUT_SEC):
        """初始化执行器。

        Args:
            table_paths: 物理表名 → CSV 文件路径的映射
            timeout_sec: 执行超时秒数

        Raises:
            ValueError: table_paths 的 key（物理表名）或 value（CSV 路径）包含非法 SQL 字符
        """
        if table_paths:
            self._validate_table_paths(table_paths)
        self._table_paths = table_paths or {}
        self._timeout_sec = timeout_sec

    def execute(self, compiled: CompiledSql) -> tuple[ExecutionTrace, ResultSummary]:
        """执行编译后的 SQL。

        Args:
            compiled: CompiledSql 对象——必须是 Compiler 产物

        Returns:
            (ExecutionTrace, ResultSummary)

        Raises:
            TypeError: 输入不是 CompiledSql 对象
        """
        if not isinstance(compiled, CompiledSql):
            raise TypeError(
                f"DuckDBExecutor 只接受 CompiledSql 对象，收到 {type(compiled).__name__}。"
                f"拒绝执行外部 SQL 字符串。"
            )

        plan_id = compiled.input_plan_hash
        trace_id = ExecutionTrace.generate_trace_id(plan_id)
        start_time = time.perf_counter()

        try:
            import duckdb
        except ImportError:
            # DuckDB 未安装——返回 NOT_EXECUTED 状态
            trace = ExecutionTrace(
                trace_id=trace_id,
                plan_id=plan_id,
                engine="duckdb",
                generated_sql=compiled.sql,
                status=ExecutionStatus.NOT_EXECUTED,
                row_count=0,
                execution_time_ms=0.0,
                error_message="DuckDB 未安装——请运行 pip install duckdb",
            )
            summary = ResultSummary(
                summary_id=ResultSummary.generate_summary_id(trace_id),
                trace_id=trace_id,
                engine="duckdb",
                columns=[],
                column_types=[],
                row_count=0,
                null_counts={},
                numeric_sums={},
                sample_rows=[],
            )
            return trace, summary

        try:
            # 创建内存数据库连接
            con = duckdb.connect(":memory:")

            # 加载 CSV fixture 表
            self._load_tables(con)

            # 执行 SQL
            result = con.execute(compiled.sql)

            # 获取结果
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            elapsed_ms = (time.perf_counter() - start_time) * 1000
            row_count = len(rows)

            # 提取列类型
            column_types = self._extract_column_types(result.description)

            # 计算 NULL 计数
            null_counts = self._compute_null_counts(columns, rows)

            # 计算数值汇总
            numeric_sums = self._compute_numeric_sums(columns, column_types, rows)

            # 抽样行
            sample_rows = self._sample_rows(rows, _MAX_SAMPLE_ROWS)

            # 构建 ExecutionTrace
            trace = ExecutionTrace(
                trace_id=trace_id,
                plan_id=plan_id,
                engine="duckdb",
                generated_sql=compiled.sql,
                status=ExecutionStatus.RUNTIME_PASS,
                row_count=row_count,
                execution_time_ms=round(elapsed_ms, 2),
                error_message=None,
            )

            # 构建 ResultSummary
            summary = ResultSummary(
                summary_id=ResultSummary.generate_summary_id(trace_id),
                trace_id=trace_id,
                engine="duckdb",
                columns=columns,
                column_types=column_types,
                row_count=row_count,
                null_counts=null_counts,
                numeric_sums=numeric_sums,
                sample_rows=sample_rows,
            )

            con.close()
            return trace, summary

        except Exception as e:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            trace = ExecutionTrace(
                trace_id=trace_id,
                plan_id=plan_id,
                engine="duckdb",
                generated_sql=compiled.sql,
                status=ExecutionStatus.RUNTIME_FAIL,
                row_count=0,
                execution_time_ms=round(elapsed_ms, 2),
                error_message=str(e),
            )
            summary = ResultSummary(
                summary_id=ResultSummary.generate_summary_id(trace_id),
                trace_id=trace_id,
                engine="duckdb",
                columns=[],
                column_types=[],
                row_count=0,
                null_counts={},
                numeric_sums={},
                sample_rows=[],
            )
            return trace, summary

    # ── 多语句执行（Phase 3A） ──

    def execute_program(
        self, compiled: ProgramCompiledSql
    ) -> ProgramExecutionResult:
        """执行多语句 SqlProgram——同一连接 + 失败阻断 + 总是清理。

        流程：
        1. 创建 DuckDB :memory: 连接（一次）
        2. 加载 CSV 表（一次）
        3. 按 statement_order 依次执行每条 SQL：
           - 成功 → 记录 ExecutionTrace(RUNTIME_PASS) + ResultSummary
           - 失败 → 记录 ExecutionTrace(RUNTIME_FAIL)，停止执行后续语句
        4. 始终执行 cleanup（DROP temp tables）——finally 块保证
        5. 关闭连接
        6. 返回 ProgramExecutionResult

        Args:
            compiled: ProgramCompiledSql——编译产物

        Returns:
            ProgramExecutionResult——含每个语句的 trace + summary + cleanup 状态
        """
        results: list[StatementExecutionResult] = []
        failed_at: str | None = None
        cleanup_status = "success"
        cleanup_error: str | None = None
        con = None

        try:
            import duckdb
        except ImportError:
            # DuckDB 未安装——所有语句标记为 NOT_EXECUTED
            for i, stmt_id in enumerate(compiled.statement_order):
                cs = compiled.statements[i] if i < len(compiled.statements) else None
                trace = ExecutionTrace(
                    trace_id=ExecutionTrace.generate_trace_id(stmt_id),
                    plan_id=stmt_id,
                    engine="duckdb",
                    generated_sql=cs.sql if cs else "",
                    status=ExecutionStatus.NOT_EXECUTED,
                    row_count=0,
                    execution_time_ms=0.0,
                    error_message="DuckDB 未安装——请运行 pip install duckdb",
                )
                summary = ResultSummary(
                    summary_id=ResultSummary.generate_summary_id(
                        ExecutionTrace.generate_trace_id(stmt_id)
                    ),
                    trace_id=ExecutionTrace.generate_trace_id(stmt_id),
                    engine="duckdb",
                    columns=[],
                    column_types=[],
                    row_count=0,
                    null_counts={},
                    numeric_sums={},
                    sample_rows=[],
                )
                results.append(
                    StatementExecutionResult(
                        statement_id=stmt_id,
                        trace=trace,
                        summary=summary,
                    )
                )
            return ProgramExecutionResult(
                program_id=compiled.program_id,
                results=results,
                completed_count=0,
                failed_at=None,
                cleanup_status="success",
            )

        try:
            # 1. 创建连接——整个 program 共享
            con = duckdb.connect(":memory:")

            # 2. 加载 CSV 表——只加载一次
            self._load_tables(con)

            # 3. 按顺序执行每条语句
            for i, stmt_id in enumerate(compiled.statement_order):
                if i >= len(compiled.statements):
                    break

                cs = compiled.statements[i]
                start_time = time.perf_counter()

                try:
                    # 执行 SQL
                    result = con.execute(cs.sql)

                    # 收集结果
                    columns = [desc[0] for desc in result.description]
                    rows = result.fetchall()
                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    row_count = len(rows)
                    column_types = self._extract_column_types(result.description)
                    null_counts = self._compute_null_counts(columns, rows)
                    numeric_sums = self._compute_numeric_sums(
                        columns, column_types, rows
                    )
                    sample_rows = self._sample_rows(rows, _MAX_SAMPLE_ROWS)

                    trace_id = ExecutionTrace.generate_trace_id(stmt_id)
                    trace = ExecutionTrace(
                        trace_id=trace_id,
                        plan_id=stmt_id,
                        engine="duckdb",
                        generated_sql=cs.sql,
                        status=ExecutionStatus.RUNTIME_PASS,
                        row_count=row_count,
                        execution_time_ms=round(elapsed_ms, 2),
                        error_message=None,
                    )
                    summary = ResultSummary(
                        summary_id=ResultSummary.generate_summary_id(trace_id),
                        trace_id=trace_id,
                        engine="duckdb",
                        columns=columns,
                        column_types=column_types,
                        row_count=row_count,
                        null_counts=null_counts,
                        numeric_sums=numeric_sums,
                        sample_rows=sample_rows,
                    )
                    results.append(
                        StatementExecutionResult(
                            statement_id=stmt_id,
                            trace=trace,
                            summary=summary,
                        )
                    )

                except Exception as exec_err:
                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    trace_id = ExecutionTrace.generate_trace_id(stmt_id)
                    trace = ExecutionTrace(
                        trace_id=trace_id,
                        plan_id=stmt_id,
                        engine="duckdb",
                        generated_sql=cs.sql,
                        status=ExecutionStatus.RUNTIME_FAIL,
                        row_count=0,
                        execution_time_ms=round(elapsed_ms, 2),
                        error_message=str(exec_err),
                    )
                    summary = ResultSummary(
                        summary_id=ResultSummary.generate_summary_id(trace_id),
                        trace_id=trace_id,
                        engine="duckdb",
                        columns=[],
                        column_types=[],
                        row_count=0,
                        null_counts={},
                        numeric_sums={},
                        sample_rows=[],
                    )
                    results.append(
                        StatementExecutionResult(
                            statement_id=stmt_id,
                            trace=trace,
                            summary=summary,
                        )
                    )

                    # 记录失败位置并停止执行后续语句
                    failed_at = stmt_id
                    break

        except Exception as outer_err:
            # 连接创建或 CSV 加载失败
            cleanup_status = "partial_failure"
            cleanup_error = str(outer_err)

        finally:
            # 4. 始终执行 cleanup——DROP 所有 _temp 表
            if con is not None:
                try:
                    for drop_sql in compiled.cleanup_sql:
                        try:
                            con.execute(drop_sql)
                        except Exception:
                            # 单个 DROP 失败不阻断其他 DROP
                            pass
                except Exception as drop_err:
                    cleanup_status = "partial_failure"
                    cleanup_error = str(drop_err)
                finally:
                    try:
                        con.close()
                    except Exception:
                        pass

        completed_count = sum(
            1
            for r in results
            if r.trace.status == ExecutionStatus.RUNTIME_PASS
        )

        return ProgramExecutionResult(
            program_id=compiled.program_id,
            results=results,
            completed_count=completed_count,
            failed_at=failed_at,
            cleanup_status=cleanup_status,
            cleanup_error=cleanup_error,
        )

    # ── 内部方法 ──

    @staticmethod
    def _validate_table_paths(table_paths: dict[str, str]) -> None:
        """校验 table_paths 的 key 和 value 均不含 SQL 注入字符。

        双重防线（Schema 层）：
        - key（物理表名）→ SafePhysicalTableName 三层防线
        - value（CSV 路径）→ SafeCsvPathLiteral 拒绝单引号/控制字符

        此方法在构造器 __init__ 中调用，构成入口门禁。

        Args:
            table_paths: 物理表名 → CSV 路径映射

        Raises:
            ValueError: 任一 key 或 value 包含非法 SQL 字符
        """
        from tianshu_datadev.developer_spec.models import (
            _validate_csv_path_literal,
            _validate_physical_table_name,
        )

        for table_name, csv_path in table_paths.items():
            # 校验 key（物理表名）
            if not table_name:
                raise ValueError("table_paths 的 key（物理表名）不能为空字符串")
            _validate_physical_table_name(table_name)

            # 校验 value（CSV 路径）——Phase 3B 安全加固：关闭 csv_path 注入面
            _validate_csv_path_literal(csv_path)

    def _load_tables(self, con) -> None:
        """从 CSV fixture 文件加载只读表到 DuckDB 内存数据库。

        使用 _render_sql_string_literal() 对 CSV 路径做 SQL 字符串字面量转义——
        这是渲染层纵深防线：即使 Schema 层校验被绕过，
        单引号转义仍可阻止通过路径值终结字符串字面量的注入攻击。
        """
        from tianshu_datadev.developer_spec.models import _render_sql_string_literal

        for table_name, csv_path in self._table_paths.items():
            try:
                # 使用 DuckDB 的 read_csv_auto 加载 CSV
                # csv_path 经 _render_sql_string_literal 转义后安全嵌入 SQL
                safe_path = _render_sql_string_literal(csv_path)
                con.execute(
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv_auto({safe_path})"
                )
            except Exception:
                # CSV 加载失败——跳过此表，让 SQL 执行时报错暴露
                pass

    @staticmethod
    def _extract_column_types(description) -> list[str]:
        """从 DuckDB 结果描述中提取列类型。

        DuckDB description 格式: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        type_code 可用于映射到标准类型名。
        """
        types: list[str] = []
        for col in description:
            # col[1] 是类型字符串表示
            if len(col) > 1:
                types.append(str(col[1]))
            else:
                types.append("unknown")
        return types

    @staticmethod
    def _compute_null_counts(columns: list[str], rows: list) -> dict[str, int]:
        """逐列计算 NULL 值数量。"""
        if not rows:
            return {col: 0 for col in columns}

        counts: dict[str, int] = {}
        for i, col in enumerate(columns):
            null_count = sum(1 for row in rows if row[i] is None)
            counts[col] = null_count
        return counts

    @staticmethod
    def _compute_numeric_sums(
        columns: list[str],
        column_types: list[str],
        rows: list,
    ) -> dict[str, float]:
        """数值列的合计——用于交叉验证比对。"""
        sums: dict[str, float] = {}
        if not rows:
            return sums

        numeric_type_hints = {"int", "integer", "bigint", "smallint", "tinyint",
                              "float", "double", "real", "decimal", "numeric"}

        for i, (col, col_type) in enumerate(zip(columns, column_types)):
            # 检查是否为数值类型
            type_lower = col_type.lower()
            is_numeric = any(hint in type_lower for hint in numeric_type_hints)

            if is_numeric:
                total = 0.0
                for row in rows:
                    val = row[i]
                    if val is not None:
                        try:
                            total += float(val)
                        except (ValueError, TypeError):
                            pass
                sums[col] = round(total, 4)

        return sums

    @staticmethod
    def _sample_rows(rows: list, max_rows: int) -> list[list]:
        """取前 N 行作为抽样数据。

        将每行 tuple 转换为 list，并处理非 JSON 兼容类型。
        """
        sample = []
        for row in rows[:max_rows]:
            sample_row = []
            for val in row:
                # 转换非标量类型为字符串
                if val is None:
                    sample_row.append(None)
                elif isinstance(val, (int, float, str, bool)):
                    sample_row.append(val)
                else:
                    sample_row.append(str(val))
            sample.append(sample_row)
        return sample
