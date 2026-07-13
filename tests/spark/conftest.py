"""Spark 测试共享 fixture——Parquet 临时目录等。"""

import os
import tempfile

import pytest


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
