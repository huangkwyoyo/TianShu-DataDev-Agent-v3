"""Spark 测试共享 fixture——Pipeline + Parquet 临时目录等。"""

import os
import shutil
import tempfile

import pytest

from tianshu_datadev.api.pipeline import Pipeline


@pytest.fixture
def pipeline():
    """创建带临时目录的 Pipeline——测试结束后自动清理。"""
    tmpdir = tempfile.mkdtemp(prefix="tianshu_spark_")
    yield Pipeline(base_output_dir=tmpdir)
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def temp_parquet_dir():
    """创建含测试 Parquet 文件的临时目录——DuckDB 真实执行使用。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tmpdir = tempfile.mkdtemp(prefix="tianshu_physver_")

    # 创建测试数据并写入 Parquet
    table = pa.table({
        "order_id": ["1", "2", "3"],
        "amount": [100, 200, 150],
        "region": ["east", "west", "east"],
    })
    pq.write_table(table, os.path.join(tmpdir, "order_info.parquet"))

    yield tmpdir

    # 清理
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)
