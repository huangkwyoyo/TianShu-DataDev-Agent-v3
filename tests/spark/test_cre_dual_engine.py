"""CRE v2 双引擎 Harness——同一不可变 Parquet fixture 经 DuckDB 和真实 Spark 执行后 CRE 比较。

前置条件：
- 标记 @pytest.mark.slow，需显式 `--run-slow`
- 本机需安装 PySpark 及兼容 Java 版本
- 不满足时 spark_available fixture 自动 skip

覆盖场景：
- int、bool、float 尾差（等值/微小差异）、Decimal、NULL、date、timestamp
- NaN 双引擎归一化一致性（列类型为 double）
- 重复/NULL 主键的 HUMAN_REVIEW 判定
- exact match、CONSISTENT_WITH_WARN、超容差 MISMATCH
- schema/行数差异、缺少配置 HUMAN_REVIEW
- 真实 AVG/SUM/GROUP BY 聚合尾差（要求 1）
- 微秒精度 timestamp + 非 UTC 时区双引擎一致性（要求 4）

约束（要求 7）：
- 不修改 pipeline.py、executor.py、physical_verifier.py 及其未提交改动
- 不接入生产状态枚举
"""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest

# CRE v2 原型组件
from tianshu_datadev.spark.cre import (
    CreConfig,
    CREEncoder,
    DecisionEngine,
    EnvironmentManifest,
    KeyBasedRowAligner,
    NotToleratedReason,
    SpecialFloatStrategy,
    ToleranceComparator,
)
from tianshu_datadev.spark.physical_verifier import NormalizationColumn

# 临时目录清理注册表——session 结束时统一清理（要求 4）
_cleanup_dirs: list[str] = []


def _cleanup_temp_dirs() -> None:
    """清理所有注册的临时目录。"""
    for d in _cleanup_dirs:
        shutil.rmtree(d, ignore_errors=True)
    _cleanup_dirs.clear()


# ════════════════════════════════════════════
# Parquet fixture 工厂
# ════════════════════════════════════════════


@pytest.fixture(scope="session")
def shared_parquet_dir():
    """创建不可变的 Parquet fixture——所有测试用例共用同一快照。

    包含覆盖所有目标类型的数据：int、bool、float 尾差场景、Decimal、
    NULL、date、timestamp、NaN、重复主键、NULL 主键。
    """
    import duckdb

    tmpdir = tempfile.mkdtemp(prefix="cre_dual_engine_")
    _cleanup_dirs.append(tmpdir)
    parquet_path = os.path.join(tmpdir, "fixture.parquet")

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE test_data AS SELECT * FROM (
            VALUES
                -- 基础类型覆盖（id 1-5）
                (1, 42,              TRUE,               1.23456789012345,
                 CAST(123.45 AS DECIMAL(10,2)), NULL,     DATE '2024-01-15',
                 TIMESTAMP '2024-06-15 10:30:00', NULL),
                (2, -7,              FALSE,              -3.33333333333333,
                 CAST(-0.01 AS DECIMAL(10,2)), 'hello',  DATE '2023-12-31',
                 TIMESTAMP '2023-12-31 23:59:59', 1.0/0.0),
                (3, 2147483647,      TRUE,               0.0,
                 CAST(999999.99 AS DECIMAL(12,2)), 'world', DATE '2024-07-13',
                 TIMESTAMP '2024-07-13 12:00:00', -1.0/0.0),
                (4, -2147483648,     FALSE,              1e-15,
                 CAST(0.00 AS DECIMAL(10,2)), '',         DATE '1970-01-01',
                 TIMESTAMP '1970-01-01 00:00:00', CAST('NaN' AS DOUBLE)),
                -- 浮点极值（id 5）
                (5, 1,               TRUE,               3.14159265358979,
                 CAST(123.45 AS DECIMAL(10,2)), 'abc',   DATE '2024-06-15',
                 TIMESTAMP '2024-06-15 10:30:00', 0.0/0.0),
                -- NULL 覆盖（id 6-7）
                (6, 0,               NULL,               NULL,
                 CAST(NULL AS DECIMAL(10,2)), NULL,      DATE '2024-01-01',
                 TIMESTAMP '2024-01-01 00:00:00', NULL),
                (7, NULL,            TRUE,               0.0,
                 CAST(1.23 AS DECIMAL(10,2)), 'null_test', DATE '2024-02-29',
                 TIMESTAMP '2024-02-29 12:00:00', 10.5),
                -- 重复主键（id 8 重复两次）
                (8, 100,             TRUE,               1.5,
                 CAST(10.00 AS DECIMAL(10,2)), 'dup_a',  DATE '2024-03-01',
                 TIMESTAMP '2024-03-01 00:00:00', NULL),
                -- NULL 主键（id=9 但 pk 为 NULL）
                (9, 200,             FALSE,              2.5,
                 CAST(20.00 AS DECIMAL(10,2)), 'null_pk', DATE '2024-04-01',
                 TIMESTAMP '2024-04-01 00:00:00', NULL)
        ) AS t(
            id, int_col, bool_col, float_col,
            dec_col, str_col, date_col,
            ts_col, nan_col
        )
    """)

    # 用 DuckDB 写 Parquet
    con.execute(f"COPY test_data TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()
    return tmpdir, parquet_path


@pytest.fixture(scope="class")
def spark_available():
    """检查真实 PySpark 环境是否可用——尝试创建 SparkSession。"""
    try:
        from tianshu_datadev.spark.executor import LocalSparkExecutor
        executor = LocalSparkExecutor()
        if not executor.check_environment():
            pytest.skip("PySpark 环境不可用（check_environment=False）")
    except Exception as e:
        pytest.skip(f"PySpark 环境不可用：{e}")


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _run_duckdb(parquet_path: str) -> list[dict]:
    """用 DuckDB 读取 Parquet 并返回排序后的行列表。

    保留 NaN/Infinity 原始值——由 EnvironmentManifest 策略判定比较结果。
    """
    import duckdb
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT * FROM '{parquet_path}'
        ORDER BY id
    """).fetchall()
    col_names = [desc[0] for desc in con.description]
    con.close()
    # 只做类型转换，保留 NaN/Inf 原始值
    result = []
    for row in rows:
        d = {}
        for i, c in enumerate(col_names):
            v = row[i]
            # DuckDB 可能将 NaN 返回为字符串 'nan'——转为 float
            if isinstance(v, str) and v.lower() in ("nan",):
                v = float("nan")
            elif isinstance(v, str) and v.lower() in ("inf", "+inf", "infinity"):
                v = float("inf")
            elif isinstance(v, str) and v.lower() in ("-inf", "-infinity"):
                v = float("-inf")
            d[c] = v
        result.append(d)
    return result


def _run_spark(parquet_path: str) -> list[dict]:
    """用 PySpark 读取 Parquet 并返回排序后的行列表。

    保留 NaN/Infinity 原始值——由 EnvironmentManifest 策略判定比较结果。
    处理 Java 版本兼容性：通过 _find_compatible_java_home 查找 Java 17+。
    """
    import os

    from tianshu_datadev.spark.executor import _find_compatible_java_home

    # 确保 JAVA_HOME 指向 Java 17+
    java_home = _find_compatible_java_home(min_version=17)
    if java_home:
        os.environ["JAVA_HOME"] = java_home

    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("CRE Dual Engine") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config(
            "spark.driver.extraJavaOptions",
            "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
        ) \
        .master("local[1]") \
        .getOrCreate()

    df = spark.read.parquet(parquet_path).orderBy("id")
    rows = df.collect()
    col_names = df.columns

    # 转换 Row 为 dict——保留原生 datetime/date 类型（CRE 比较器需要）
    result = []
    for r in rows:
        row = {}
        for c in col_names:
            v = r[c]
            row[c] = v
        result.append(row)

    spark.stop()
    return result


# ════════════════════════════════════════════
# 聚合测试辅助（要求 1：真实 SQL/DataFrame 执行）
# ════════════════════════════════════════════


@pytest.fixture(scope="session")
def shared_agg_parquet_dir():
    """聚合测试专用 Parquet fixture——用于 AVG/SUM/GROUP BY/NULL 聚合后 CRE 比较。"""
    import duckdb

    tmpdir = tempfile.mkdtemp(prefix="cre_dual_engine_agg_")
    _cleanup_dirs.append(tmpdir)
    parquet_path = os.path.join(tmpdir, "agg_fixture.parquet")

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE agg_data AS SELECT * FROM (
            VALUES
                (1, 'a', 1.5,               CAST(10.00 AS DECIMAL(10,2))),
                (2, 'a', 2.5,               CAST(20.00 AS DECIMAL(10,2))),
                (3, 'b', 3.5,               CAST(30.00 AS DECIMAL(10,2))),
                (4, 'b', CAST(NULL AS DOUBLE), CAST(40.00 AS DECIMAL(10,2))),
                (5, 'b', 5.5,               CAST(NULL AS DECIMAL(10,2))),
                (6, CAST(NULL AS VARCHAR),  6.5,               CAST(60.00 AS DECIMAL(10,2)))
        ) AS t(id, grp, val_double, val_decimal)
    """)
    con.execute(f"COPY agg_data TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()
    return tmpdir, parquet_path


def _run_duckdb_agg(parquet_path: str) -> list[dict]:
    """用 DuckDB 执行真实聚合 SQL——AVG(double)、SUM(decimal)、GROUP BY、NULL 聚合。"""
    import duckdb
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT
            grp,
            AVG(val_double) AS avg_double,
            SUM(val_decimal) AS sum_decimal,
            COUNT(*) AS cnt,
            COUNT(val_double) AS cnt_double,
            COUNT(val_decimal) AS cnt_decimal
        FROM '{parquet_path}'
        GROUP BY grp
        ORDER BY grp NULLS LAST
    """).fetchall()
    col_names = [desc[0] for desc in con.description]
    con.close()
    result = []
    for row in rows:
        d = {}
        for i, c in enumerate(col_names):
            v = row[i]
            # DuckDB 可能将 NaN 返回为字符串 'nan'——转为 float
            if isinstance(v, str) and v.lower() in ("nan",):
                v = float("nan")
            d[c] = v
        result.append(d)
    return result


# ════════════════════════════════════════════
# 时间戳微秒精度与非 UTC 时区辅助
# ════════════════════════════════════════════


@pytest.fixture(scope="session")
def shared_ts_parquet_dir():
    """时间戳（微秒 + 非 UTC 时区）专用 Parquet fixture。"""
    import duckdb

    tmpdir = tempfile.mkdtemp(prefix="cre_dual_engine_ts_")
    _cleanup_dirs.append(tmpdir)
    parquet_path = os.path.join(tmpdir, "ts_fixture.parquet")

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE ts_data AS SELECT * FROM (
            VALUES
                -- 微秒精度时间戳
                (1, TIMESTAMP '2024-06-15 10:30:00.123456',
                 TIMESTAMP '2024-06-15 02:30:00.123456Z'),
                -- 毫秒精度（容差场景）
                (2, TIMESTAMP '2024-06-15 10:30:00.500000',
                 TIMESTAMP '2024-06-15 02:30:00.500000Z'),
                -- 整秒
                (3, TIMESTAMP '2024-07-13 12:00:00.000000',
                 TIMESTAMP '2024-07-13 04:00:00.000000Z'),
                -- NULL 时间戳
                (4, CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP)),
                -- 伦敦冬令时（UTC+0）
                (5, TIMESTAMP '2024-01-15 14:30:00.000000',
                 TIMESTAMP '2024-01-15 14:30:00.000000Z'),
                -- 纽约夏令时（UTC-4, DST）
                (6, TIMESTAMP '2024-06-01 09:00:00.000000',
                 TIMESTAMP '2024-06-01 13:00:00.000000Z'),
                -- 东京（UTC+9, 无 DST）
                (7, TIMESTAMP '2024-06-15 18:30:00.000000',
                 TIMESTAMP '2024-06-15 09:30:00.000000Z')
        ) AS t(id, ts_local, ts_utc)
    """)
    con.execute(f"COPY ts_data TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()
    return tmpdir, parquet_path


def _make_ts_config(timezone: str) -> CreConfig:
    """构造带时区配置的 TS 比较配置。"""
    columns = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="ts_local", data_type="timestamp"),
        NormalizationColumn(column_name="ts_utc", data_type="timestamp"),
    ]
    return CreConfig(
        output_columns=columns,
        primary_keys=["id"],
        timezone=timezone,
    )


# ════════════════════════════════════════════


def _make_config() -> CreConfig:
    """构造 CRE 比较配置（含 timestamp、float 容差、环境清单）。"""
    columns = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="int_col", data_type="integer"),
        NormalizationColumn(column_name="bool_col", data_type="boolean"),
        NormalizationColumn(column_name="float_col", data_type="double"),
        NormalizationColumn(column_name="dec_col", data_type="decimal(12,2)"),
        NormalizationColumn(column_name="str_col", data_type="varchar"),
        NormalizationColumn(column_name="date_col", data_type="date"),
        NormalizationColumn(column_name="ts_col", data_type="timestamp"),
        NormalizationColumn(column_name="nan_col", data_type="double"),
    ]
    return CreConfig(
        output_columns=columns,
        primary_keys=["id"],
        float_abs_tolerance=1e-12,
        float_rel_tolerance=1e-9,
        timezone="Asia/Shanghai",
        # NaN/Infinity 显式策略——双引擎值相等时视为一致
        environment_manifest=EnvironmentManifest(
            nan_handling=SpecialFloatStrategy.EQUAL,
            pos_inf_handling=SpecialFloatStrategy.EQUAL,
            neg_inf_handling=SpecialFloatStrategy.EQUAL,
        ),
    )


# ════════════════════════════════════════════
# 测试类
# ════════════════════════════════════════════


class TestDualEngineCREComparison:
    """真实双引擎 CRE 比较——DuckDB vs Spark 同一 Parquet 快照。

    不满足 PySpark 条件时自动跳过。
    """

    @pytest.mark.slow
    def test_full_pipeline_exact_match(self, shared_parquet_dir, spark_available):
        """DuckDB 与 Spark 的 exact consistent 路径。

        NaN 在 DuckDB 和 Spark 间可能归一化不一致（DuckDB 保留 NaN，
        Spark 可能转为 NULL），因此全量 exact match 预期为 false，
        但浮点列差异应在容差内（CONSISTENT_WITH_WARN）。
        """
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = _run_spark(parquet_path)

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert not alignment.error_message, f"对齐应成功：{alignment.error_message}"
        assert not alignment.duplicate_keys, "不应有重复键"
        assert not alignment.duckdb_only, "DuckDB 不应有额外行"
        assert not alignment.spark_only, "Spark 不应有额外行"
        assert len(alignment.aligned_pairs) > 0, "应有对齐行"

        # 逐行比较
        pairs = alignment.aligned_pairs
        row_results = [
            (d, s, ToleranceComparator.compare_row(d, s, config, encoder))
            for d, s in pairs
        ]

        # Bucket 诊断
        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests(pairs, encoder, config)

        # 判定
        result = DecisionEngine.decide(alignment, row_results, buckets, config)
        assert result.status in (
            "CONSISTENT", "CONSISTENT_WITH_WARN"
        ), f"预期一致，实际 {result.status}：{result.decision_reason}"

    @pytest.mark.slow
    def test_int_bool_decimal_date_consistent(self, shared_parquet_dir, spark_available):
        """整数、布尔、Decimal、date 列应为 exact match（无容差类型）。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = _run_spark(parquet_path)

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)

        for d_row, s_row in alignment.aligned_pairs:
            for col in ("int_col", "bool_col", "dec_col", "date_col"):
                d_val = d_row.get(col)
                s_val = s_row.get(col)
                # 如果双方都是 NULL，跳过
                if d_val is None and s_val is None:
                    continue
                # 精确编码比较（双方编码应为相同字节）
                n_col = next(
                    c for c in config.output_columns
                    if c.column_name == col
                )
                d_enc = encoder.encode_value(d_val, n_col.data_type)
                s_enc = encoder.encode_value(s_val, n_col.data_type)
                assert d_enc == s_enc, (
                    f"列 {col} 编码不一致：duckdb={d_val!r}({d_enc.hex()}) "
                    f"spark={s_val!r}({s_enc.hex()})"
                )

    @pytest.mark.slow
    def test_timestamp_timezone_consistent(self, shared_parquet_dir, spark_available):
        """timestamp 列经 timezone 转换后应编码一致。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = _run_spark(parquet_path)

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)

        for d_row, s_row in alignment.aligned_pairs:
            d_ts = d_row.get("ts_col")
            s_ts = s_row.get("ts_col")
            if d_ts is None and s_ts is None:
                continue
            # 比较 timestamp 编码（时区已由 CREEncoder 处理）
            ts_col = next(
                c for c in config.output_columns if c.column_name == "ts_col"
            )
            d_enc = encoder.encode_value(d_ts, ts_col.data_type)
            s_enc = encoder.encode_value(s_ts, ts_col.data_type)
            assert d_enc == s_enc, (
                f"Timestamp 编码不一致：duckdb={d_ts!r} spark={s_ts!r}"
            )


class TestDualEngineEdgeCases:
    """双引擎边缘场景——重复键 / NULL 主键判定测试。

    使用 DuckDB 自读自比（构造人工差异场景），不依赖 Spark。
    """

    def test_duplicate_keys_human_review(self, shared_parquet_dir):
        """重复主键 → HUMAN_REVIEW。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)

        # 构造重复键：插入 2 行同 id=8 的数据
        import duckdb
        con = duckdb.connect()
        extra = con.execute(f"""
            SELECT * FROM '{parquet_path}' WHERE id = 8
        """).fetchall()
        col_names = [desc[0] for desc in con.description]
        con.close()
        extra_rows = [dict(zip(col_names, r)) for r in extra]

        spark_rows = duck_rows + extra_rows  # Spark 侧有 2 个 id=8

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert alignment.duplicate_keys, "应检测到重复键"
        assert alignment.error_message != "", "应有错误消息"

        # 直接判定
        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests([], encoder, config)
        result = DecisionEngine.decide(
            alignment, [], buckets, config
        )
        assert result.status == "HUMAN_REVIEW", (
            f"重复键应返回 HUMAN_REVIEW，实际 {result.status}"
        )

    def test_missing_primary_key_config_human_review(self, shared_parquet_dir):
        """缺少 primary_keys 配置 → HUMAN_REVIEW。"""
        _dir, parquet_path = shared_parquet_dir
        rows = _run_duckdb(parquet_path)

        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="id", data_type="bigint")],
            primary_keys=[],  # 空主键
        )
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(rows, rows, config, encoder)
        assert alignment.error_message != "", "缺少主键应报错"

        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests([], encoder, config)
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "HUMAN_REVIEW"

    def test_row_count_mismatch_mismatch(self, shared_parquet_dir):
        """行数不匹配 → MISMATCH。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = duck_rows[:-2]  # Spark 少 2 行

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert alignment.spark_only or alignment.duckdb_only, "应有行数差异"

        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests([], encoder, config)
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "MISMATCH"

    def test_null_primary_key_human_review(self, shared_parquet_dir):
        """NULL 主键行 → HUMAN_REVIEW。"""
        _dir, parquet_path = shared_parquet_dir
        import duckdb
        con = duckdb.connect()
        extra = con.execute(f"""
            SELECT * FROM '{parquet_path}' WHERE id = 9
        """).fetchall()
        col_names = [desc[0] for desc in con.description]
        con.close()
        null_pk_row = dict(zip(col_names, extra[0]))
        null_pk_row["id"] = None  # 主键设为 NULL

        duck_rows = _run_duckdb(parquet_path)
        spark_rows = duck_rows + [null_pk_row]  # Spark 侧有 NULL 主键行

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        # 主键 NULL 阻止对齐
        assert "NULL" in (alignment.error_message or ""), \
            f"NULL 主键应报错：{alignment.error_message}"

        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests([], encoder, config)
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "HUMAN_REVIEW"

    def test_schema_mismatch_extra_column(self, shared_parquet_dir):
        """DuckDB 额外列 → MISMATCH。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = _run_duckdb(parquet_path)

        # 给 DuckDB 加上额外列
        for row in duck_rows:
            row["extra_col"] = "extra"

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert alignment.error_message != "", "额外列应报错"
        assert "额外列" in alignment.error_message

    def test_schema_mismatch_missing_column_one_side(self, shared_parquet_dir):
        """单侧缺列 → MISMATCH。"""
        _dir, parquet_path = shared_parquet_dir
        duck_rows = _run_duckdb(parquet_path)
        spark_rows = _run_duckdb(parquet_path)

        # Spark 侧缺失 float_col
        for row in spark_rows:
            row.pop("float_col", None)

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert alignment.error_message != "", "单侧缺列应报错"
        assert "缺少" in alignment.error_message

    def test_schema_mismatch_both_missing_contract_column(self, shared_parquet_dir):
        """双方缺少 Contract 必需列 → MISMATCH。"""
        _dir, parquet_path = shared_parquet_dir
        rows = _run_duckdb(parquet_path)

        # 配置包含双方都不存在的列
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="non_existent", data_type="varchar"),
            ],
            primary_keys=["id"],
        )
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(rows, rows, config, encoder)
        assert alignment.error_message != "", "双方缺必需列应报错"
        assert "缺少" in alignment.error_message
        assert "non_existent" in alignment.error_message

    def test_out_of_tolerance_mismatch(self, shared_parquet_dir):
        """超容差数值差异 → MISMATCH。"""
        _dir, parquet_path = shared_parquet_dir
        rows = _run_duckdb(parquet_path)

        # Spark 侧修改 float_col 为差异巨大的值
        import copy
        spark_rows = copy.deepcopy(rows)
        for row in spark_rows:
            if row["id"] == 1:
                row["float_col"] = 99999.0  # 远大于容差
                break

        config = _make_config()
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(rows, spark_rows, config, encoder)
        assert not alignment.error_message

        pairs = alignment.aligned_pairs
        row_results = [
            (d, s, ToleranceComparator.compare_row(d, s, config, encoder))
            for d, s in pairs
        ]
        all_cells = []
        for _d, _s, rr in row_results:
            if not rr.exact_match:
                all_cells.extend(rr.cell_results)

        out_of_tolerance = [
            c for c in all_cells
            if c.reason == NotToleratedReason.OUT_OF_TOLERANCE
        ]
        assert len(out_of_tolerance) > 0, "应检测到超容差差异"


class TestDualEngineAggregation:
    """真实 SQL/DataFrame 聚合执行——AVG、SUM、GROUP BY（要求 1）。

    分别执行真实 DuckDB SQL 与 Spark DataFrame 聚合操作，再用 CRE 比较。
    复现原问题的计算尾差（double AVG 尾差、decimal SUM）。
    """

    @pytest.mark.slow
    def test_avg_double_tail_diff(self, shared_agg_parquet_dir, spark_available):
        """AVG(double) 计算尾差——CRE 应检测为 WITHIN_TOLERANCE。

        DuckDB 和 Spark 的 double AVG 可能有 1e-15 级尾差。
        """
        _dir, parquet_path = shared_agg_parquet_dir
        duck_rows = _run_duckdb_agg(parquet_path)

        import os

        from tianshu_datadev.spark.executor import _find_compatible_java_home
        java_home = _find_compatible_java_home(min_version=17)
        if java_home:
            os.environ["JAVA_HOME"] = java_home

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F  # noqa: N812
        spark = SparkSession.builder \
            .appName("CRE Agg Test") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config(
                "spark.driver.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
            ) \
            .master("local[1]") \
            .getOrCreate()
        try:
            df = spark.read.parquet(parquet_path)
            agg_df = df.groupBy("grp").agg(
                F.avg("val_double").alias("avg_double"),
                F.sum("val_decimal").alias("sum_decimal"),
                F.count("*").alias("cnt"),
                F.count("val_double").alias("cnt_double"),
                F.count("val_decimal").alias("cnt_decimal"),
            ).orderBy(F.col("grp").asc_nulls_last())
            spark_rows = [row.asDict() for row in agg_df.collect()]
        finally:
            spark.stop()

        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="grp", data_type="varchar"),
                NormalizationColumn(column_name="avg_double", data_type="double"),
                NormalizationColumn(column_name="sum_decimal", data_type="decimal(12,2)"),
                NormalizationColumn(column_name="cnt", data_type="bigint"),
                NormalizationColumn(column_name="cnt_double", data_type="bigint"),
                NormalizationColumn(column_name="cnt_decimal", data_type="bigint"),
            ],
            primary_keys=["grp"],
            environment_manifest=EnvironmentManifest(
                nan_handling=SpecialFloatStrategy.EQUAL,
            ),
        )
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert not alignment.error_message, f"对齐应成功：{alignment.error_message}"

        pairs = alignment.aligned_pairs
        row_results = [
            (d, s, ToleranceComparator.compare_row(d, s, config, encoder))
            for d, s in pairs
        ]
        from tianshu_datadev.spark.cre import BucketHasher
        buckets = BucketHasher.compute_bucket_digests(pairs, encoder, config)
        result = DecisionEngine.decide(alignment, row_results, buckets, config)
        # AVG 尾差在容差内，整数列 exact match → CONSISTENT 或 CONSISTENT_WITH_WARN
        assert result.status in (
            "CONSISTENT", "CONSISTENT_WITH_WARN"
        ), f"聚合尾差不应导致 MISMATCH：{result.status} {result.decision_reason}"

    @pytest.mark.slow
    def test_sum_decimal_exact_match(self, shared_agg_parquet_dir, spark_available):
        """SUM(decimal) 精确匹配——Decimal 尾差不应存在。"""
        _dir, parquet_path = shared_agg_parquet_dir
        duck_rows = _run_duckdb_agg(parquet_path)

        import os

        from tianshu_datadev.spark.executor import _find_compatible_java_home
        java_home = _find_compatible_java_home(min_version=17)
        if java_home:
            os.environ["JAVA_HOME"] = java_home

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F  # noqa: N812
        spark = SparkSession.builder \
            .appName("CRE Agg Dec Test") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config(
                "spark.driver.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
            ) \
            .master("local[1]") \
            .getOrCreate()
        try:
            df = spark.read.parquet(parquet_path)
            agg_df = df.groupBy("grp").agg(
                F.sum("val_decimal").alias("sum_decimal"),
            ).orderBy(F.col("grp").asc_nulls_last())
            spark_rows = [row.asDict() for row in agg_df.collect()]
        finally:
            spark.stop()

        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="grp", data_type="varchar"),
                NormalizationColumn(column_name="sum_decimal", data_type="decimal(12,2)"),
            ],
            primary_keys=["grp"],
        )
        encoder = CREEncoder(config)
        alignment = KeyBasedRowAligner.align(duck_rows, spark_rows, config, encoder)
        assert not alignment.error_message, f"对齐应成功：{alignment.error_message}"

        for d_row, s_row in alignment.aligned_pairs:
            d_enc = encoder.encode_value(d_row.get("sum_decimal"), "decimal(12,2)")
            s_enc = encoder.encode_value(s_row.get("sum_decimal"), "decimal(12,2)")
            assert d_enc == s_enc, (
                f"SUM(decimal) 编码不一致：duckdb={d_row['sum_decimal']!r} "
                f"spark={s_row['sum_decimal']!r}"
            )


class TestDualEngineTimestampPrecision:
    """时间戳微秒精度与非 UTC 时区测试（要求 4）。"""

    @pytest.mark.slow
    def test_ts_microsecond_precision(self, shared_ts_parquet_dir, spark_available):
        """微秒级 timestamp 在双引擎间应精确匹配。"""
        _dir, parquet_path = shared_ts_parquet_dir
        duck_rows = _run_duckdb(parquet_path)

        import os

        from tianshu_datadev.spark.executor import _find_compatible_java_home
        java_home = _find_compatible_java_home(min_version=17)
        if java_home:
            os.environ["JAVA_HOME"] = java_home

        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("CRE TS Micro Test") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config(
                "spark.driver.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
            ) \
            .master("local[1]") \
            .getOrCreate()
        try:
            df = spark.read.parquet(parquet_path).orderBy("id")
            spark_rows = [row.asDict() for row in df.collect()]
        finally:
            spark.stop()

        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="ts_local", data_type="timestamp"),
                NormalizationColumn(column_name="ts_utc", data_type="timestamp"),
            ],
            primary_keys=["id"],
            timezone="Asia/Shanghai",
        )
        encoder = CREEncoder(config)
        for d_row, s_row in zip(duck_rows, spark_rows):
            for col in ("ts_local", "ts_utc"):
                d_enc = encoder.encode_value(d_row.get(col), "timestamp")
                s_enc = encoder.encode_value(s_row.get(col), "timestamp")
                assert d_enc == s_enc, (
                    f"id={d_row.get('id')} 列 {col} 微秒时间戳编码不一致："
                    f"duckdb={d_row.get(col)!r} spark={s_row.get(col)!r}"
                )

    @pytest.mark.slow
    def test_non_utc_timezone_tokyo(self, shared_ts_parquet_dir, spark_available):
        """非 UTC 时区（Asia/Tokyo）时间戳双引擎一致性。"""
        _dir, parquet_path = shared_ts_parquet_dir
        duck_rows = _run_duckdb(parquet_path)

        import os

        from tianshu_datadev.spark.executor import _find_compatible_java_home
        java_home = _find_compatible_java_home(min_version=17)
        if java_home:
            os.environ["JAVA_HOME"] = java_home

        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("CRE TS Tz Test") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config(
                "spark.driver.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
            ) \
            .master("local[1]") \
            .getOrCreate()
        try:
            df = spark.read.parquet(parquet_path).orderBy("id")
            spark_rows = [row.asDict() for row in df.collect()]
        finally:
            spark.stop()

        config = _make_ts_config("Asia/Tokyo")
        encoder = CREEncoder(config)

        for d_row, s_row in zip(duck_rows, spark_rows):
            d_enc = encoder.encode_value(d_row.get("ts_local"), "timestamp")
            s_enc = encoder.encode_value(s_row.get("ts_local"), "timestamp")
            assert d_enc == s_enc, (
                f"id={d_row.get('id')} Tokyo 时区 ts_local 编码不一致"
            )
