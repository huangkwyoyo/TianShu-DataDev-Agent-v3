"""DuckDBExecutor——隔离执行 Compiler 产物，输出结构化 ExecutionTrace + ResultSummary。

只接受 CompiledSql 对象——不接受外部 SQL 字符串。
支持从 CSV fixture 文件加载只读表。
执行超时保护（默认 30 秒）。
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from .models import (
    CompiledSql,
    ExecutionStatus,
    ExecutionTrace,
    ProgramCompiledSql,
    ProgramExecutionResult,
    ResultSummary,
    StatementExecutionResult,
)
from .process_guard import ProcessGuard, ProcessGuardError

# 默认执行超时（秒）
_DEFAULT_TIMEOUT_SEC = 30
# 抽样行数上限
_MAX_SAMPLE_ROWS = 20
# 默认结果行数上限——超过此值返回 RESULT_TOO_LARGE
_DEFAULT_MAX_RESULT_ROWS = 10_000
# 默认内存上限
_DEFAULT_MEMORY_LIMIT = "2GB"
# 默认线程数
_DEFAULT_THREADS = 2
# Worker 进程硬内存上限必须高于 DuckDB buffer limit，给解释器和原生算子留余量
_DEFAULT_PROCESS_MEMORY_LIMIT_MB = 2560
# 防止 DuckDB 溢写占满个人电脑磁盘
_DEFAULT_MAX_TEMP_DIRECTORY_SIZE = "4GB"
_RESOURCE_SIZE_RE = re.compile(r"^[1-9]\d*(?:\.\d+)?(?:KB|MB|GB|TB)$", re.IGNORECASE)
_MAX_WORKER_OUTPUT_BYTES = 2 * 1024 * 1024


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

    def __init__(
        self,
        table_paths: dict[str, str] | None = None,
        timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
        duckdb_path: str | None = None,
        memory_limit: str = _DEFAULT_MEMORY_LIMIT,
        threads: int = _DEFAULT_THREADS,
        temp_directory: str | None = None,
        max_result_rows: int = _DEFAULT_MAX_RESULT_ROWS,
        max_temp_directory_size: str = _DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
        process_memory_limit_mb: int = _DEFAULT_PROCESS_MEMORY_LIMIT_MB,
        _worker_mode: bool = False,
    ):
        """初始化执行器。

        Args:
            table_paths: 物理表名 → CSV 文件路径的映射
            timeout_sec: 执行超时秒数——超时后通过 connection.interrupt() 中断查询
            duckdb_path: 外部 DuckDB 数据库文件路径——ATTACH 后自动创建同名 schema
                         和 VIEW，使编译产生的两段表名（如 gold.fact_trips）可直接查询
            memory_limit: DuckDB 内存上限（如 "2GB"、"512MB"）——防止内存耗尽
            threads: DuckDB 工作线程数——限制 CPU 占用
            temp_directory: DuckDB 溢出临时目录——None 则使用系统默认临时目录
            max_result_rows: 结果行数上限——超过时返回 RESULT_TOO_LARGE，防止无界 fetchall
            max_temp_directory_size: DuckDB 临时溢写目录大小上限
            process_memory_limit_mb: Worker 进程硬内存上限（MB）

        Raises:
            ValueError: table_paths 的 key（物理表名）或 value（CSV 路径）包含非法 SQL 字符
        """
        if table_paths:
            self._validate_table_paths(table_paths)
        self._validate_resource_config(
            timeout_sec=timeout_sec,
            memory_limit=memory_limit,
            threads=threads,
            max_result_rows=max_result_rows,
            max_temp_directory_size=max_temp_directory_size,
            process_memory_limit_mb=process_memory_limit_mb,
        )
        self._table_paths = table_paths or {}
        self._timeout_sec = timeout_sec
        self._duckdb_path = duckdb_path
        self._memory_limit = memory_limit
        self._threads = threads
        self._temp_directory = str(
            Path(temp_directory or Path(tempfile.gettempdir()) / "tianshu_duckdb").resolve()
        )
        Path(self._temp_directory).mkdir(parents=True, exist_ok=True)
        self._max_result_rows = max_result_rows
        self._max_temp_directory_size = max_temp_directory_size
        self._process_memory_limit_mb = process_memory_limit_mb
        self._worker_mode = _worker_mode

    def execute(self, compiled: CompiledSql) -> tuple[ExecutionTrace, ResultSummary]:
        """在受控 Worker 中执行单条编译 SQL。"""
        if self._worker_mode:
            return self._execute_local(compiled)
        return self._execute_isolated_single(compiled)

    def materialize_snapshot(
        self,
        *,
        output_dir: str,
        contract_hash: str,
        source_tables: list[str],
        joins: list[dict[str, str]],
        table_aliases: dict[str, str],
        sampling: dict,
        table_role_aliases: dict[str, list[str]] | None = None,
    ):
        """在受硬限制 Worker 中从 DuckDB 源库构建快照。"""
        from tianshu_datadev.spark.snapshot import (
            SamplingSpec,
            SnapshotBuilder,
            SnapshotManifest,
        )

        if not self._duckdb_path:
            raise ValueError("构建仓库快照必须提供 duckdb_path")
        if self._worker_mode:
            return SnapshotBuilder(output_dir=output_dir).materialize_warehouse_tables(
                contract_hash=contract_hash,
                source_tables=source_tables,
                duckdb_path=self._duckdb_path,
                joins=joins,
                table_aliases=table_aliases,
                table_role_aliases=table_role_aliases,
                sampling=SamplingSpec.model_validate(sampling),
                memory_limit=self._memory_limit,
                threads=self._threads,
                max_temp_directory_size=self._max_temp_directory_size,
            )

        payload, _failure_kind, error, _elapsed_ms = self._run_worker({
            "mode": "snapshot",
            "output_dir": output_dir,
            "contract_hash": contract_hash,
            "source_tables": source_tables,
            "joins": joins,
            "table_aliases": table_aliases,
            "table_role_aliases": table_role_aliases or {},
            "sampling": sampling,
        })
        if payload is None:
            raise RuntimeError(error)
        return SnapshotManifest.model_validate(payload)

    def _execute_local(self, compiled: CompiledSql) -> tuple[ExecutionTrace, ResultSummary]:
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

        timer = None
        con = None
        try:
            # 创建内存数据库连接
            con = duckdb.connect(":memory:")

            # 设置资源保护参数——内存上限、线程数、溢出目录
            self._configure_connection(con)

            # 加载 CSV fixture 表
            self._load_tables(con)

            # ATTACH 外部 DuckDB 数据库（如 NYC 数据仓库）
            self._attach_database(con)

            # 启动超时定时器——超时后通过 connection.interrupt() 中断查询
            timer = self._start_timeout_timer(con)

            # 执行 SQL
            result = con.execute(compiled.sql)

            # 获取结果——使用 fetchmany 防止无界内存增长
            columns = [desc[0] for desc in result.description]
            overflow, rows = self._fetch_with_limit(result, columns)

            # 查询和结果读取均完成后才能取消超时保护
            if timer is not None:
                timer.cancel()
                timer = None

            elapsed_ms = (time.perf_counter() - start_time) * 1000

            if overflow:
                # 结果行数超过上限——返回明确错误状态，不计算假 row_count
                trace = ExecutionTrace(
                    trace_id=trace_id,
                    plan_id=plan_id,
                    engine="duckdb",
                    generated_sql=compiled.sql,
                    status=ExecutionStatus.RESULT_TOO_LARGE,
                    row_count=0,
                    execution_time_ms=round(elapsed_ms, 2),
                    error_message=(
                        f"结果行数超过 max_result_rows={self._max_result_rows} 上限，"
                        "请缩小受控快照或查询范围；系统不会自动修改业务 SQL"
                    ),
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

            return trace, summary

        except Exception as e:
            # 取消超时定时器（如果还在运行）
            if timer is not None:
                timer.cancel()

            elapsed_ms = (time.perf_counter() - start_time) * 1000

            # 判断是否为超时中断
            if _is_interrupt_exception(e):
                trace = ExecutionTrace(
                    trace_id=trace_id,
                    plan_id=plan_id,
                    engine="duckdb",
                    generated_sql=compiled.sql,
                    status=ExecutionStatus.TIMEOUT,
                    row_count=0,
                    execution_time_ms=round(elapsed_ms, 2),
                    error_message=(
                        f"查询超时（{self._timeout_sec}s），"
                        f"已被 connection.interrupt() 中断"
                    ),
                )
            else:
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

        finally:
            # 确保定时器被清理
            if timer is not None:
                timer.cancel()
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass

    # ── CDP digest 执行（Task 5） ──

    def execute_with_cdp(
        self,
        compiled: CompiledSql,
        spec,
        snapshot_id: str,
    ):
        """在受控 Worker 中执行 DuckDB CDP 摘要。"""
        if self._worker_mode:
            return self._execute_with_cdp_local(compiled, spec, snapshot_id)
        return self._execute_isolated_cdp(compiled, spec, snapshot_id)

    def _execute_with_cdp_local(
        self,
        compiled: CompiledSql,
        spec,
        snapshot_id: str,
    ):
        """执行 SQL 并在引擎内部计算 CDP digest——不将 output_rows 传回 Python。

        Args:
            compiled: CompiledSql 对象——Comiler 产物
            spec: CreDigestSpec——CDP 摘要规范
            snapshot_id: 快照 ID（用于溯源）

        Returns:
            DigestExecutionEnvelope——含 full_digest 和 row_count，不含 samples
        """
        from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder
        from tianshu_datadev.spark.cdp_spec import (
            DigestExecutionEnvelope,
            EngineDigestSummary,
            compute_digest_spec_hash,
        )

        builder = DuckdbCdpBuilder()
        spec_hash_hex = compute_digest_spec_hash(spec).hex()
        # spec_hash_hex 作为参数传入 build_query——仅通过 f-string 格式化
        cdp_query = builder.build_query(
            compiled.sql, spec, spec_hash_hex=spec_hash_hex
        )

        try:
            import duckdb

            con = duckdb.connect(":memory:")
            # 与现有 execute() 一致——加载 CSV fixture 和外部数据库
            self._load_tables(con)
            self._attach_database(con)

            result = con.execute(cdp_query).fetchone()
            full_digest = str(result[0])
            row_count = int(result[1])

            return DigestExecutionEnvelope(
                execution_status="SUCCESS",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="duckdb",
                summary=EngineDigestSummary(
                    row_count=row_count,
                    full_digest=full_digest,
                    samples=[],
                ),
            )
        except Exception as e:
            return DigestExecutionEnvelope(
                execution_status="FAILED",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="duckdb",
                error=str(e),
            )

    # ── 多语句执行（Phase 3A） ──

    def execute_program(
        self, compiled: ProgramCompiledSql
    ) -> ProgramExecutionResult:
        """在受控 Worker 中执行多语句程序。"""
        if self._worker_mode:
            return self._execute_program_local(compiled)
        return self._execute_isolated_program(compiled)

    def _execute_program_local(
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

            # 设置资源保护参数——内存上限、线程数、溢出目录
            self._configure_connection(con)

            # 2. 加载 CSV 表——只加载一次
            self._load_tables(con)

            # 2b. ATTACH 外部 DuckDB 数据库
            self._attach_database(con)

            # 3. 按顺序执行每条语句——每条语句自带超时保护
            for i, stmt_id in enumerate(compiled.statement_order):
                if i >= len(compiled.statements):
                    break

                cs = compiled.statements[i]
                start_time = time.perf_counter()
                timer = None

                try:
                    # 启动超时定时器——每条语句独立超时
                    timer = self._start_timeout_timer(con)

                    # 执行 SQL
                    result = con.execute(cs.sql)

                    # 收集结果——使用 fetchmany 防止无界内存增长
                    columns = [desc[0] for desc in result.description]
                    overflow, rows = self._fetch_with_limit(result, columns)

                    # 查询和结果读取均完成后才能取消超时保护
                    if timer is not None:
                        timer.cancel()
                        timer = None
                    elapsed_ms = (time.perf_counter() - start_time) * 1000

                    if overflow:
                        # 结果行数超过上限
                        trace_id = ExecutionTrace.generate_trace_id(stmt_id)
                        trace = ExecutionTrace(
                            trace_id=trace_id,
                            plan_id=stmt_id,
                            engine="duckdb",
                            generated_sql=cs.sql,
                            status=ExecutionStatus.RESULT_TOO_LARGE,
                            row_count=0,
                            execution_time_ms=round(elapsed_ms, 2),
                            error_message=(
                                f"结果行数超过 max_result_rows={self._max_result_rows} 上限，"
                                "请缩小受控快照或查询范围；系统不会自动修改业务 SQL"
                            ),
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
                        # 结果过大也阻断后续语句——避免级联问题
                        failed_at = stmt_id
                        break

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
                    # 取消超时定时器
                    if timer is not None:
                        timer.cancel()

                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    trace_id = ExecutionTrace.generate_trace_id(stmt_id)

                    # 判断是否为超时中断
                    if _is_interrupt_exception(exec_err):
                        trace = ExecutionTrace(
                            trace_id=trace_id,
                            plan_id=stmt_id,
                            engine="duckdb",
                            generated_sql=cs.sql,
                            status=ExecutionStatus.TIMEOUT,
                            row_count=0,
                            execution_time_ms=round(elapsed_ms, 2),
                            error_message=(
                                f"查询超时（{self._timeout_sec}s），"
                                f"已被 connection.interrupt() 中断"
                            ),
                        )
                    else:
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

    # ── 隔离执行边界 ──

    def _execute_isolated_single(
        self, compiled: CompiledSql,
    ) -> tuple[ExecutionTrace, ResultSummary]:
        if not isinstance(compiled, CompiledSql):
            raise TypeError(
                f"DuckDBExecutor 只接受 CompiledSql 对象，收到 {type(compiled).__name__}"
            )
        payload, failure_kind, error, elapsed_ms = self._run_worker({
            "mode": "single",
            "compiled": compiled.model_dump(mode="json"),
        })
        if payload is None:
            return self._build_single_worker_failure(
                compiled, failure_kind, error, elapsed_ms,
            )
        return (
            ExecutionTrace.model_validate(payload["trace"]),
            ResultSummary.model_validate(payload["summary"]),
        )

    def _execute_isolated_program(
        self, compiled: ProgramCompiledSql,
    ) -> ProgramExecutionResult:
        if not isinstance(compiled, ProgramCompiledSql):
            raise TypeError(
                "DuckDBExecutor 只接受 ProgramCompiledSql 对象，"
                f"收到 {type(compiled).__name__}"
            )
        payload, failure_kind, error, elapsed_ms = self._run_worker({
            "mode": "program",
            "compiled": compiled.model_dump(mode="json"),
        })
        if payload is not None:
            return ProgramExecutionResult.model_validate(payload)

        first_id = compiled.statement_order[0] if compiled.statement_order else compiled.program_id
        first_sql = compiled.statements[0].sql if compiled.statements else ""
        status = (
            ExecutionStatus.TIMEOUT
            if failure_kind == "timeout"
            else ExecutionStatus.RUNTIME_FAIL
        )
        trace_id = ExecutionTrace.generate_trace_id(first_id)
        trace = ExecutionTrace(
            trace_id=trace_id,
            plan_id=first_id,
            engine="duckdb",
            generated_sql=first_sql,
            status=status,
            row_count=0,
            execution_time_ms=round(elapsed_ms, 2),
            error_message=error,
        )
        summary = self._empty_summary(trace_id)
        return ProgramExecutionResult(
            program_id=compiled.program_id,
            results=[
                StatementExecutionResult(
                    statement_id=first_id,
                    trace=trace,
                    summary=summary,
                )
            ],
            completed_count=0,
            failed_at=first_id,
            cleanup_status="partial_failure",
            cleanup_error=error,
        )

    def _execute_isolated_cdp(self, compiled, spec, snapshot_id: str):
        from tianshu_datadev.spark.cdp_spec import DigestExecutionEnvelope

        payload, failure_kind, error, _elapsed_ms = self._run_worker({
            "mode": "cdp",
            "compiled": compiled.model_dump(mode="json"),
            "spec": spec.model_dump(mode="json"),
            "snapshot_id": snapshot_id,
        })
        if payload is not None:
            return DigestExecutionEnvelope.model_validate(payload)
        return DigestExecutionEnvelope(
            execution_status="TIMEOUT" if failure_kind == "timeout" else "FAILED",
            snapshot_id=snapshot_id,
            digest_spec_hash="",
            protocol_version="cdp-v1",
            engine_version="duckdb",
            error=error,
        )

    def _run_worker(
        self, request: dict,
    ) -> tuple[dict | None, str, str, float]:
        """启动受硬限制的 Worker，并只接收有界 JSON。"""
        request["config"] = self._worker_config()
        request_json = json.dumps(request, ensure_ascii=False)
        guard = ProcessGuard(self._process_memory_limit_mb * 1024 * 1024)
        proc = None
        start = time.perf_counter()

        try:
            preexec_fn = guard.prepare()
            env = self._build_worker_env()
            src_dir = str(Path(__file__).resolve().parents[2])
            old_pythonpath = os.environ.get("PYTHONPATH", "")
            env["PYTHONPATH"] = os.pathsep.join(
                value for value in (src_dir, old_pythonpath) if value
            )
            env["PYTHONIOENCODING"] = "utf-8"
            popen_kwargs = {
                "args": [sys.executable, "-m", "tianshu_datadev.sql.executor_worker"],
                "stdin": subprocess.PIPE,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
                "env": env,
                "cwd": self._temp_directory,
                "start_new_session": os.name != "nt",
            }
            if preexec_fn is not None:
                popen_kwargs["preexec_fn"] = preexec_fn
            if os.name == "nt":
                popen_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            proc = subprocess.Popen(**popen_kwargs)
            guard.attach(proc)
            hard_timeout = max(float(self._timeout_sec) + 5.0, 5.0)
            stdout, stderr = proc.communicate(input=request_json, timeout=hard_timeout)
            elapsed_ms = (time.perf_counter() - start) * 1000

            if len(stdout.encode("utf-8")) > _MAX_WORKER_OUTPUT_BYTES:
                return None, "failed", "SQL Worker 输出超过 2MB 上限", elapsed_ms
            try:
                response = json.loads(stdout)
            except json.JSONDecodeError:
                detail = stderr.strip()[-1000:] or f"退出码 {proc.returncode}"
                return None, "failed", f"SQL Worker 返回无效响应：{detail}", elapsed_ms
            if proc.returncode != 0 or not response.get("ok"):
                detail = response.get("error") or stderr.strip()[-1000:]
                return None, "failed", f"SQL Worker 执行失败：{detail}", elapsed_ms
            return response["payload"], "", "", elapsed_ms
        except subprocess.TimeoutExpired:
            if proc is not None:
                guard.terminate(proc)
                try:
                    proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            elapsed_ms = (time.perf_counter() - start) * 1000
            return None, "timeout", f"SQL Worker 超时（{self._timeout_sec}s）", elapsed_ms
        except (OSError, ProcessGuardError) as exc:
            if proc is not None:
                guard.terminate(proc)
            elapsed_ms = (time.perf_counter() - start) * 1000
            return None, "failed", f"SQL Worker 隔离失败：{exc}", elapsed_ms
        finally:
            guard.close()

    def _worker_config(self) -> dict:
        """生成 Worker 配置，不允许子进程再次创建 Worker。"""
        return {
            "table_paths": self._table_paths,
            "timeout_sec": self._timeout_sec,
            "duckdb_path": self._duckdb_path,
            "memory_limit": self._memory_limit,
            "threads": self._threads,
            "temp_directory": self._temp_directory,
            "max_result_rows": self._max_result_rows,
            "max_temp_directory_size": self._max_temp_directory_size,
            "process_memory_limit_mb": self._process_memory_limit_mb,
        }

    @staticmethod
    def _build_worker_env() -> dict[str, str]:
        """只向 Worker 传递解释器和本地运行必需的环境变量。"""
        allowed = {
            "PATH",
            "PATHEXT",
            "SYSTEMROOT",
            "WINDIR",
            "TEMP",
            "TMP",
            "VIRTUAL_ENV",
            "CONDA_PREFIX",
            "PYTHONHOME",
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
        }
        return {
            key: value
            for key, value in os.environ.items()
            if key.upper() in allowed
        }

    def _build_single_worker_failure(
        self,
        compiled: CompiledSql,
        failure_kind: str,
        error: str,
        elapsed_ms: float,
    ) -> tuple[ExecutionTrace, ResultSummary]:
        status = (
            ExecutionStatus.TIMEOUT
            if failure_kind == "timeout"
            else ExecutionStatus.RUNTIME_FAIL
        )
        trace_id = ExecutionTrace.generate_trace_id(compiled.input_plan_hash)
        trace = ExecutionTrace(
            trace_id=trace_id,
            plan_id=compiled.input_plan_hash,
            engine="duckdb",
            generated_sql=compiled.sql,
            status=status,
            row_count=0,
            execution_time_ms=round(elapsed_ms, 2),
            error_message=error,
        )
        return trace, self._empty_summary(trace_id)

    @staticmethod
    def _empty_summary(trace_id: str) -> ResultSummary:
        return ResultSummary(
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

    @staticmethod
    def _validate_resource_config(
        *,
        timeout_sec: float,
        memory_limit: str,
        threads: int,
        max_result_rows: int,
        max_temp_directory_size: str,
        process_memory_limit_mb: int,
    ) -> None:
        """拒绝无效资源参数，避免保护配置静默失效。"""
        if timeout_sec <= 0:
            raise ValueError("timeout_sec 必须大于 0")
        if not _RESOURCE_SIZE_RE.fullmatch(memory_limit):
            raise ValueError("memory_limit 必须使用正数加 KB/MB/GB/TB 单位")
        if not _RESOURCE_SIZE_RE.fullmatch(max_temp_directory_size):
            raise ValueError("max_temp_directory_size 必须使用正数加 KB/MB/GB/TB 单位")
        if isinstance(threads, bool) or not 1 <= threads <= 64:
            raise ValueError("threads 必须在 1 到 64 之间")
        if isinstance(max_result_rows, bool) or max_result_rows <= 0:
            raise ValueError("max_result_rows 必须大于 0")
        if isinstance(process_memory_limit_mb, bool) or process_memory_limit_mb < 256:
            raise ValueError("process_memory_limit_mb 不能小于 256MB")

    def _load_tables(self, con) -> None:
        """从 CSV 或 Parquet 快照加载只读表到 DuckDB 内存数据库。

        使用 _render_sql_string_literal() 对 CSV 路径做 SQL 字符串字面量转义——
        这是渲染层纵深防线：即使 Schema 层校验被绕过，
        单引号转义仍可阻止通过路径值终结字符串字面量的注入攻击。
        """
        from tianshu_datadev.developer_spec.models import _render_sql_string_literal

        for table_name, source_path in self._table_paths.items():
            try:
                parts = table_name.split(".")
                if len(parts) == 2:
                    con.execute(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}")
                safe_path = _render_sql_string_literal(source_path)
                reader = (
                    "read_parquet"
                    if Path(source_path).suffix.lower() == ".parquet"
                    else "read_csv_auto"
                )
                con.execute(
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM {reader}({safe_path})"
                )
            except Exception:
                # CSV 加载失败——跳过此表，让 SQL 执行时报错暴露
                pass

    def _attach_database(self, con) -> None:
        """ATTACH 外部 DuckDB 数据库并创建 schema + VIEW 桥接。

        编译产生的 SQL 使用两段表名（如 gold.fact_trips），但 ATTACH 的数据库
        表名需要三段（catalog.schema.table）。此方法在默认 catalog 中创建同名
        schema 和 VIEW，使两段表名可直接解析到外部数据库表。

        注意：information_schema 在 system catalog 下，不能用 {alias}.information_schema
        查询——需用全局 information_schema + table_catalog 过滤。
        """
        if not self._duckdb_path:
            return

        try:
            # 使用固定别名 ATTACH 外部数据库
            alias = "_ext_db"
            con.execute(f"ATTACH '{self._duckdb_path}' AS {alias} (READ_ONLY)")

            # 发现所有用户 schema（排除系统 schema——通过 table_catalog 过滤）
            schemas = con.execute(f"""
                SELECT DISTINCT table_schema
                FROM information_schema.tables
                WHERE table_catalog = '{alias}'
                  AND table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchall()

            for (schema_name,) in schemas:
                # 在默认 catalog 创建同名 schema
                try:
                    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                except Exception:
                    continue

                # 为该 schema 下的每张表创建 VIEW
                tables = con.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_catalog = '{alias}'
                      AND table_schema = '{schema_name}'
                      AND table_type = 'BASE TABLE'
                """).fetchall()

                for (table_name,) in tables:
                    try:
                        con.execute(
                            f"CREATE OR REPLACE VIEW {schema_name}.{table_name} AS "
                            f"SELECT * FROM {alias}.{schema_name}.{table_name}"
                        )
                    except Exception:
                        # VIEW 创建失败——可能已存在或权限问题，跳过
                        pass

        except Exception:
            # ATTACH 失败——数据库文件不存在或损坏，静默跳过
            # SQL 执行时会因表缺失而报明确错误
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

    # ── 资源保护方法 ──

    def _configure_connection(self, con) -> None:
        """设置 DuckDB 连接资源保护参数——内存上限、线程数、溢出临时目录。"""
        from tianshu_datadev.developer_spec.models import _render_sql_string_literal

        safe_temp_directory = _render_sql_string_literal(self._temp_directory)
        con.execute(f"SET memory_limit = '{self._memory_limit}'")
        con.execute(f"SET threads = {self._threads}")
        con.execute(f"SET temp_directory = {safe_temp_directory}")
        con.execute(
            f"SET max_temp_directory_size = '{self._max_temp_directory_size}'"
        )

    def _start_timeout_timer(self, con) -> threading.Timer | None:
        """启动超时定时器——超时后通过 connection.interrupt() 中断查询。

        Returns:
            threading.Timer 实例——调用方需在查询完成后 cancel()。
            若 timeout_sec <= 0，返回 None（不设超时）。
        """
        if self._timeout_sec <= 0:
            return None

        def _on_timeout():
            try:
                con.interrupt()
            except Exception:
                pass

        timer = threading.Timer(self._timeout_sec, _on_timeout)
        timer.daemon = True
        timer.start()
        return timer

    def _fetch_with_limit(self, result, columns: list[str]) -> tuple[bool, list]:
        """使用 fetchmany 限制结果行数——防止无界 fetchall 撑爆内存。

        Args:
            result: DuckDB 查询结果对象
            columns: 输出列名列表（仅用于空结果时跳过 fetch）

        Returns:
            (overflow, rows)——
            overflow=True 表示结果超过 max_result_rows 上限，rows 被丢弃
            overflow=False 时 rows 为实际获取的行
        """
        if result.description is None or len(columns) == 0:
            # 非 SELECT 语句（如 CREATE TABLE、INSERT）——无结果集
            return False, []

        rows = result.fetchmany(self._max_result_rows + 1)
        if len(rows) > self._max_result_rows:
            return True, []
        return False, rows


# ════════════════════════════════════════════
# 模块级辅助函数
# ════════════════════════════════════════════


def _is_interrupt_exception(exc: Exception) -> bool:
    """判断异常是否由 DuckDB connection.interrupt() 触发。

    不同 DuckDB 版本抛出的异常类型不同：
    - duckdb>=1.0: duckdb.duckdb.InterruptException
    - 旧版本: duckdb.duckdb.IOException 或 RuntimeError
    - 也可能是 duckdb.InternalException

    使用异常类名子串匹配而非 isinstance，避免因版本差异遗漏。
    """
    exc_name = type(exc).__name__
    exc_module = type(exc).__module__ or ""
    exc_str = str(exc).lower()

    # 类名精确匹配
    if exc_name in ("InterruptException", "QueryInterruptedException"):
        return True

    # duckdb 模块下的异常 + 消息含 interrupt/timeout 关键词
    if "duckdb" in exc_module.lower():
        if any(kw in exc_str for kw in ("interrupt", "timeout", "cancel")):
            return True

    return False
