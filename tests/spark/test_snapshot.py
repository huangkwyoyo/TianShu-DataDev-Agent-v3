"""Phase 7A Snapshot 模块测试——SnapshotBuilder + SnapshotManifest 完整性。

覆盖：
- SnapshotSourceProvider 白名单校验
- SnapshotBuilder.build() 生成确定性 SnapshotManifest
- LOCAL_FIXTURE：实际 CSV → Parquet 写入（PyArrow，不启 DuckDB/Spark）
- snapshot_id 确定性（相同输入 → 相同 ID）
- 完整性校验（Manifest 结构 + 文件存在性 + SHA-256 校验）
- 非 LOCAL_FIXTURE：占位清单（row_count=0）
- 非白名单数据源拒绝
- 不含 spark.read / DuckDB / SparkSession 调用
"""

from __future__ import annotations

import os
import shutil

import pytest

from tests._test_utils import read_fixture
from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.spark.snapshot import (
    SamplingSpec,
    SnapshotBuilder,
    SnapshotFile,
    SnapshotIntegrityError,
    SnapshotManifest,
    SnapshotSourceNotAllowlistedError,
    SnapshotSourceProvider,
    SnapshotSourceType,
)


def _make_local_fixture_builder(tmpdir: str):
    """在 tmpdir 下创建 fact_trips_sample.csv 和对应 provider，返回 (provider, builder)。

    供别名校验和侧车索引测试使用——创建 2 行样例 CSV 后构造 LOCAL_FIXTURE provider。
    tmpdir 为绝对路径字符串（使用 tempfile.mkdtemp 创建，避免 pytest tmp_path 权限冲突）。
    """
    # 写 2 行样例 CSV
    csv_content = "trip_id,fare\n1,10.5\n2,20.0\n"
    csv_path = os.path.join(tmpdir, "fact_trips_sample.csv")
    with open(csv_path, "w", encoding="utf-8") as fp:
        fp.write(csv_content)

    provider = SnapshotSourceProvider(
        provider_id="test_provider_001",
        source_type=SnapshotSourceType.LOCAL_FIXTURE,
        connection_alias="test",
        allowlisted_tables=["fact_trips_sample"],
        base_path=tmpdir,
        description="测试用本地 fixture",
    )
    builder = SnapshotBuilder(output_dir=os.path.join(tmpdir, "snap"))
    return provider, builder


# ════════════════════════════════════════════
# Fixtures
# ════════════════════════════════════════════


@pytest.fixture
def local_fixture_provider():
    """本地 fixture 数据源提供方——白名单含 3 张测试表。"""
    return SnapshotSourceProvider(
        provider_id="fixture_provider_001",
        source_type=SnapshotSourceType.LOCAL_FIXTURE,
        connection_alias="local_test",
        allowlisted_tables=[
            "order_info",
            "user_profile",
            "product_catalog",
        ],
        base_path="tests/fixtures/",
        description="本地测试 fixture 数据集",
    )


@pytest.fixture
def dev_warehouse_provider():
    """开发环境数据仓库提供方。"""
    return SnapshotSourceProvider(
        provider_id="dev_wh_001",
        source_type=SnapshotSourceType.DEV_WAREHOUSE,
        connection_alias="dev_warehouse_readonly",
        allowlisted_tables=[
            "dw.dim_date",
            "dw.fact_orders",
        ],
        base_path="/data/dev_warehouse/",
        description="开发环境只读数据仓库",
    )


@pytest.fixture
def snapshot_builder():
    """SnapshotBuilder 实例——每次测试使用独立临时目录，自动清理。

    使用 tempfile.mkdtemp 而非 pytest tmp_path/tmpdir，
    避免 Windows 上 pytest-asyncio 与 tmpdir 的权限冲突。
    """
    import shutil
    import tempfile

    tmpdir = tempfile.mkdtemp(prefix="tianshu_snap_")
    builder = SnapshotBuilder(output_dir=tmpdir)
    yield builder
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def sample_files():
    """示例快照文件列表。"""
    return [
        SnapshotFile(
            source_name="order_info",
            file_path="/snap/snap_abc123/order_info.parquet",
            format="parquet",
            row_count=1000,
            file_sha256="a1b2c3d4e5f6",
        ),
        SnapshotFile(
            source_name="user_profile",
            file_path="/snap/snap_abc123/user_profile.parquet",
            format="parquet",
            row_count=500,
            file_sha256="f6e5d4c3b2a1",
        ),
    ]


# ════════════════════════════════════════════
# SnapshotSourceProvider 白名单测试
# ════════════════════════════════════════════


class TestSnapshotSourceProvider:
    """SnapshotSourceProvider 模型基本测试。"""

    def test_create_local_fixture_provider(self, local_fixture_provider):
        """创建本地 fixture 数据源提供方。"""
        assert local_fixture_provider.provider_id == "fixture_provider_001"
        assert local_fixture_provider.source_type == SnapshotSourceType.LOCAL_FIXTURE
        assert len(local_fixture_provider.allowlisted_tables) == 3

    def test_allowlisted_tables_exact_match_only(self, local_fixture_provider):
        """白名单只支持精确匹配——无通配符。"""
        allowed = set(local_fixture_provider.allowlisted_tables)
        assert "order_info" in allowed
        # 验证无通配符
        for table in allowed:
            assert "*" not in table
            assert "?" not in table

    def test_source_type_enum_values(self):
        """SnapshotSourceType 枚举值正确。"""
        assert SnapshotSourceType.LOCAL_FIXTURE.value == "local_fixture"
        assert SnapshotSourceType.DEV_WAREHOUSE.value == "dev_warehouse"
        assert SnapshotSourceType.TEST_DATASET.value == "test_dataset"


# ════════════════════════════════════════════
# SamplingSpec 测试
# ════════════════════════════════════════════


class TestSamplingSpec:
    """采样策略模型测试。"""

    def test_default_full_mode(self):
        """默认采样模式为 full。"""
        spec = SamplingSpec()
        assert spec.mode == "full"
        assert spec.limit is None
        assert spec.seed is None

    def test_random_with_seed(self):
        """随机采样 + 固定种子。"""
        spec = SamplingSpec(mode="random", limit=1000, seed=42)
        assert spec.mode == "random"
        assert spec.limit == 1000
        assert spec.seed == 42

    def test_stratified_sampling(self):
        """分层采样带 strata_keys。"""
        spec = SamplingSpec(
            mode="stratified",
            limit=500,
            seed=123,
            strata_keys=["region", "category"],
            anchor_keys=["order_date"],
        )
        assert spec.mode == "stratified"
        assert spec.strata_keys == ["region", "category"]
        assert spec.anchor_keys == ["order_date"]


# ════════════════════════════════════════════
# SnapshotBuilder 测试
# ════════════════════════════════════════════


class TestSnapshotBuilderBuild:
    """SnapshotBuilder.build() 核心测试。"""

    def test_build_with_local_fixture(
        self, snapshot_builder, local_fixture_provider,
    ):
        """从本地 fixture 数据源构建快照清单。"""
        manifest = snapshot_builder.build(
            contract_hash="abc123def456",
            source_tables=["order_info", "user_profile"],
            provider=local_fixture_provider,
        )

        assert isinstance(manifest, SnapshotManifest)
        assert manifest.contract_hash == "abc123def456"
        assert manifest.source_provider_id == "fixture_provider_001"
        assert manifest.source_type == "local_fixture"
        assert manifest.snapshot_id.startswith("snap_")
        assert len(manifest.files) == 2
        assert manifest.deidentification == "none"

    def test_build_creates_deterministic_snapshot_id(
        self, snapshot_builder, local_fixture_provider,
    ):
        """相同输入产出相同 snapshot_id（确定性）。"""
        manifest1 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )
        manifest2 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        assert manifest1.snapshot_id == manifest2.snapshot_id

    def test_build_different_contract_different_id(
        self, snapshot_builder, local_fixture_provider,
    ):
        """不同 Contract 产出不同 snapshot_id。"""
        manifest1 = snapshot_builder.build(
            contract_hash="contract_a",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )
        manifest2 = snapshot_builder.build(
            contract_hash="contract_b",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        assert manifest1.snapshot_id != manifest2.snapshot_id

    def test_build_different_tables_different_id(
        self, snapshot_builder, local_fixture_provider,
    ):
        """不同表选择产出不同 snapshot_id。"""
        manifest1 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )
        manifest2 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["user_profile"],
            provider=local_fixture_provider,
        )

        assert manifest1.snapshot_id != manifest2.snapshot_id

    def test_build_single_table(self, snapshot_builder, local_fixture_provider):
        """单表快照构建。"""
        manifest = snapshot_builder.build(
            contract_hash="single_table_test",
            source_tables=["product_catalog"],
            provider=local_fixture_provider,
        )

        assert len(manifest.files) == 1
        assert manifest.files[0].source_name == "product_catalog"
        assert manifest.files[0].format == "parquet"

    def test_build_empty_source_tables(
        self, snapshot_builder, local_fixture_provider,
    ):
        """空表列表构建（虽然罕见但合法）。"""
        manifest = snapshot_builder.build(
            contract_hash="empty_test",
            source_tables=[],
            provider=local_fixture_provider,
        )

        assert len(manifest.files) == 0
        assert manifest.snapshot_id.startswith("snap_")

    def test_build_with_sampling_spec(
        self, snapshot_builder, local_fixture_provider,
    ):
        """自定义采样策略影响 snapshot_id。"""
        spec1 = SamplingSpec(mode="full")
        spec2 = SamplingSpec(mode="head", limit=100)

        manifest1 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["order_info"],
            provider=local_fixture_provider,
            sampling=spec1,
        )
        manifest2 = snapshot_builder.build(
            contract_hash="abc123",
            source_tables=["order_info"],
            provider=local_fixture_provider,
            sampling=spec2,
        )

        # 不同采样策略 → 不同 snapshot_id
        assert manifest1.snapshot_id != manifest2.snapshot_id

    def test_build_files_sorted_by_source_name(
        self, snapshot_builder, local_fixture_provider,
    ):
        """文件列表按 source_name 排序。"""
        manifest = snapshot_builder.build(
            contract_hash="sort_test",
            source_tables=["user_profile", "order_info", "product_catalog"],
            provider=local_fixture_provider,
        )

        names = [f.source_name for f in manifest.files]
        assert names == sorted(names)

    def test_build_local_fixture_creates_parquet_files(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE 类型——build() 实际写入 Parquet 文件到磁盘。"""
        manifest = snapshot_builder.build(
            contract_hash="parquet_create_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 验证 Parquet 文件实际存在
        assert len(manifest.files) == 1
        parquet_path = manifest.files[0].file_path
        assert os.path.isfile(parquet_path), f"Parquet 文件未创建: {parquet_path}"

        # 验证 row_count > 0（非占位）
        assert manifest.files[0].row_count > 0, (
            f"LOCAL_FIXTURE 快照应有真实行数，当前 row_count={manifest.files[0].row_count}"
        )

        # 验证 file_sha256 非空
        assert manifest.files[0].file_sha256, (
            "LOCAL_FIXTURE 快照应有真实 SHA-256"
        )
        assert len(manifest.files[0].file_sha256) == 64  # SHA-256 → 64 hex chars

    def test_build_local_fixture_matches_source_rows(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE 快照行数与源 CSV 一致。"""
        import pyarrow.csv as pacsv

        manifest = snapshot_builder.build(
            contract_hash="row_count_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 对比源 CSV 行数
        csv_path = os.path.join(local_fixture_provider.base_path, "order_info.csv")
        source_table = pacsv.read_csv(csv_path)
        assert manifest.files[0].row_count == source_table.num_rows, (
            f"快照行数 {manifest.files[0].row_count} 与源 CSV 行数 {source_table.num_rows} 不一致"
        )

    def test_build_local_fixture_with_head_sampling(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE + head 采样——仅取前 N 行。"""
        spec = SamplingSpec(mode="head", limit=2)
        manifest = snapshot_builder.build(
            contract_hash="head_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
            sampling=spec,
        )

        assert manifest.files[0].row_count == 2, (
            f"head(2) 应有 2 行，实际 {manifest.files[0].row_count}"
        )


class TestSnapshotBuilderWhitelist:
    """白名单安全门禁测试。"""

    def test_allowlisted_table_succeeds(
        self, snapshot_builder, local_fixture_provider,
    ):
        """白名单内的表允许访问。"""
        # 不应抛出异常
        manifest = snapshot_builder.build(
            contract_hash="test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )
        assert manifest is not None

    def test_non_allowlisted_table_rejected(
        self, snapshot_builder, local_fixture_provider,
    ):
        """非白名单表被拒绝。"""
        with pytest.raises(SnapshotSourceNotAllowlistedError) as exc_info:
            snapshot_builder.build(
                contract_hash="test",
                source_tables=["secret_production_table"],
                provider=local_fixture_provider,
            )

        assert "secret_production_table" in str(exc_info.value)
        assert exc_info.value.source_name == "secret_production_table"
        assert exc_info.value.provider_id == "fixture_provider_001"

    def test_partial_allowlist_failure(
        self, snapshot_builder, local_fixture_provider,
    ):
        """部分表在白名单内——第一个非白名单表就拒绝（fail-fast）。"""
        with pytest.raises(SnapshotSourceNotAllowlistedError) as exc_info:
            snapshot_builder.build(
                contract_hash="test",
                source_tables=["unknown_table", "order_info"],
                provider=local_fixture_provider,
            )

        assert "unknown_table" in str(exc_info.value)

    def test_dev_warehouse_allowlist(
        self, snapshot_builder, dev_warehouse_provider,
    ):
        """开发环境数据源白名单。"""
        manifest = snapshot_builder.build(
            contract_hash="dev_test",
            source_tables=["dw.dim_date"],
            provider=dev_warehouse_provider,
        )

        assert manifest.source_type == "dev_warehouse"
        assert len(manifest.files) == 1


class TestSnapshotBuilderIntegrity:
    """快照完整性校验测试。"""

    def test_verify_integrity_valid_manifest(
        self, snapshot_builder, local_fixture_provider,
    ):
        """完整清单通过完整性校验。"""
        manifest = snapshot_builder.build(
            contract_hash="integrity_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        assert snapshot_builder.verify_integrity(manifest) is True

    def test_verify_integrity_empty_files_raises(self, snapshot_builder):
        """空文件列表触发完整性错误。"""
        manifest = SnapshotManifest(
            snapshot_id="snap_empty",
            contract_hash="test",
            files=[],
            snapshot_sha256="",
        )

        with pytest.raises(SnapshotIntegrityError) as exc_info:
            snapshot_builder.verify_integrity(manifest)

        assert "无文件记录" in str(exc_info.value)

    def test_verify_integrity_missing_hash_raises(self, snapshot_builder):
        """缺失完整性 hash 触发错误。"""
        manifest = SnapshotManifest(
            snapshot_id="snap_nohash",
            contract_hash="test",
            files=[
                SnapshotFile(
                    source_name="t1",
                    file_path="/tmp/t1.parquet",
                ),
            ],
            snapshot_sha256="",
        )

        with pytest.raises(SnapshotIntegrityError) as exc_info:
            snapshot_builder.verify_integrity(manifest)

        assert "缺少完整性 hash" in str(exc_info.value)

    def test_verify_integrity_hash_mismatch_raises(
        self, snapshot_builder, sample_files,
    ):
        """hash 不一致触发完整性错误。"""
        manifest = SnapshotManifest(
            snapshot_id="snap_bad",
            contract_hash="test",
            files=sample_files,
            snapshot_sha256="bad_hash_value",
        )

        with pytest.raises(SnapshotIntegrityError) as exc_info:
            snapshot_builder.verify_integrity(manifest)

        assert "不一致" in str(exc_info.value)

    def test_manifest_snapshot_sha256_matches_files(
        self, snapshot_builder, local_fixture_provider,
    ):
        """Manifest 的 snapshot_sha256 与文件列表一致。"""
        manifest = snapshot_builder.build(
            contract_hash="hash_test",
            source_tables=["order_info", "user_profile"],
            provider=local_fixture_provider,
        )

        # 重新计算 hash 应与 manifest 中的一致
        expected_hash = SnapshotBuilder._compute_snapshot_hash(manifest.files)
        assert expected_hash == manifest.snapshot_sha256

    def test_verify_integrity_local_fixture_checks_files(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE——verify_integrity 检查文件存在性和 SHA-256。"""
        manifest = snapshot_builder.build(
            contract_hash="integrity_files_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 文件存在时校验通过
        assert snapshot_builder.verify_integrity(manifest) is True

    def test_verify_integrity_local_fixture_missing_file_raises(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE——文件缺失时 verify_integrity 报错。"""
        manifest = snapshot_builder.build(
            contract_hash="missing_file_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 删除实际 Parquet 文件
        for sf in manifest.files:
            if os.path.isfile(sf.file_path):
                os.remove(sf.file_path)

        with pytest.raises(SnapshotIntegrityError) as exc_info:
            snapshot_builder.verify_integrity(manifest)

        assert "文件缺失" in str(exc_info.value)

    def test_verify_integrity_local_fixture_bad_hash_raises(
        self, snapshot_builder, local_fixture_provider,
    ):
        """LOCAL_FIXTURE——SHA-256 不匹配时报错。"""
        manifest = snapshot_builder.build(
            contract_hash="bad_hash_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 篡改 file_sha256
        manifest.files[0].file_sha256 = "a" * 64

        with pytest.raises(SnapshotIntegrityError) as exc_info:
            snapshot_builder.verify_integrity(manifest)

        assert "hash 不一致" in str(exc_info.value)

    def test_verify_integrity_dev_warehouse_skips_file_check(
        self, snapshot_builder, dev_warehouse_provider,
    ):
        """非 LOCAL_FIXTURE 类型——verify_integrity 仅检查 Manifest 结构。"""
        manifest = snapshot_builder.build(
            contract_hash="dev_skip_test",
            source_tables=["dw.dim_date"],
            provider=dev_warehouse_provider,
        )

        # 非 LOCAL_FIXTURE 不检查文件存在性（文件尚未实际写入）
        assert snapshot_builder.verify_integrity(manifest) is True

    def test_dev_warehouse_manifest_has_placeholder_values(
        self, snapshot_builder, dev_warehouse_provider,
    ):
        """非 LOCAL_FIXTURE——Manifest 使用占位值（row_count=0, file_sha256=""）。"""
        manifest = snapshot_builder.build(
            contract_hash="placeholder_test",
            source_tables=["dw.dim_date"],
            provider=dev_warehouse_provider,
        )

        assert manifest.files[0].row_count == 0, (
            "非 LOCAL_FIXTURE 应为占位值 row_count=0"
        )
        assert manifest.files[0].file_sha256 == "", (
            "非 LOCAL_FIXTURE 应为占位值 file_sha256=''"
        )


# ════════════════════════════════════════════
# SnapshotManifest 测试
# ════════════════════════════════════════════


class TestSnapshotManifest:
    """SnapshotManifest 模型测试。"""

    def test_manifest_contains_provenance_chain(
        self, snapshot_builder, local_fixture_provider,
    ):
        """Manifest 包含完整溯源链。"""
        manifest = snapshot_builder.build(
            contract_hash="chain_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        # 溯源字段全部非空
        assert manifest.contract_hash
        assert manifest.snapshot_id
        assert manifest.source_provider_id
        assert manifest.source_type
        assert manifest.snapshot_sha256

    def test_snapshot_id_no_timestamp_participation(
        self, snapshot_builder, local_fixture_provider,
    ):
        """snapshot_id 不依赖时间戳——created_at 为空。"""
        manifest = snapshot_builder.build(
            contract_hash="time_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        assert manifest.created_at == ""
        # snapshot_id 应该基于纯业务字段
        assert manifest.snapshot_id.startswith("snap_")

    def test_manifest_model_dump_roundtrip(
        self, snapshot_builder, local_fixture_provider,
    ):
        """Manifest 序列化-反序列化往返。"""
        manifest = snapshot_builder.build(
            contract_hash="roundtrip_test",
            source_tables=["order_info"],
            provider=local_fixture_provider,
        )

        data = manifest.model_dump()
        restored = SnapshotManifest(**data)

        assert restored.snapshot_id == manifest.snapshot_id
        assert restored.contract_hash == manifest.contract_hash
        assert len(restored.files) == len(manifest.files)

    def test_deidentification_field(self):
        """脱敏字段默认值为 none。"""
        manifest = SnapshotManifest(
            snapshot_id="snap_test",
            contract_hash="test",
        )
        assert manifest.deidentification == "none"


# ════════════════════════════════════════════
# 反转辅助测试（Task 3）
# ════════════════════════════════════════════


def test_reverse_table_mapping_to_aliases():
    """{别名: 物理} 反转为 {物理: 别名}——纯函数测试。"""
    from tianshu_datadev.api.pipeline import _aliases_from_table_mapping
    assert _aliases_from_table_mapping({"ft": "fact_trips_sample", "tz": "dim_taxi_zone"}) == {
        "fact_trips_sample": "ft",
        "dim_taxi_zone": "tz",
    }
    assert _aliases_from_table_mapping(None) == {}
    assert _aliases_from_table_mapping({}) == {}


# ════════════════════════════════════════════
# SnapshotBuilder 别名 + 侧车索引测试（Task 1）
# ════════════════════════════════════════════


class TestSnapshotBuilderAliases:
    """SnapshotBuilder.build() 别名校验 + _inputs_index 侧车写入。"""

    def test_build_source_name_uses_alias_file_keeps_physical(self):
        """table_aliases 提供时——SnapshotFile.source_name 用别名，磁盘文件名保持物理名。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_alias_")
        try:
            provider, builder = _make_local_fixture_builder(tmpdir)
            manifest = builder.build(
                contract_hash="c_hash",
                source_tables=["fact_trips_sample"],
                provider=provider,
                table_aliases={"fact_trips_sample": "ft"},
            )
            f = next(x for x in manifest.files if x.file_path.endswith(".parquet"))
            # source_name 是别名
            assert f.source_name == "ft"
            # 磁盘文件名仍是物理名
            assert f.file_path.endswith("fact_trips_sample.parquet")
            assert os.path.isfile(f.file_path)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_build_writes_inputs_index(self):
        """写入 _inputs_index.json——{别名: 物理文件名}。"""
        import json
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_index_")
        try:
            provider, builder = _make_local_fixture_builder(tmpdir)
            manifest = builder.build(
                contract_hash="c_hash",
                source_tables=["fact_trips_sample"],
                provider=provider,
                table_aliases={"fact_trips_sample": "ft"},
            )
            index_path = os.path.join(manifest.snapshot_dir, "_inputs_index.json")
            assert os.path.isfile(index_path)
            with open(index_path, encoding="utf-8") as fp:
                index = json.load(fp)
            assert index == {"ft": "fact_trips_sample.parquet"}
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_build_without_aliases_keeps_physical_source_name(self):
        """未提供 table_aliases——source_name 回退物理名（向后兼容）。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_noalias_")
        try:
            provider, builder = _make_local_fixture_builder(tmpdir)
            manifest = builder.build(
                contract_hash="c_hash",
                source_tables=["fact_trips_sample"],
                provider=provider,
            )
            f = next(x for x in manifest.files if x.file_path.endswith(".parquet"))
            assert f.source_name == "fact_trips_sample"
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_dev_warehouse_writes_inputs_index(self):
        """非 LOCAL_FIXTURE（DEV_WAREHOUSE）也写 _inputs_index.json。

        回归：SnapshotBuilder 非 LOCAL_FIXTURE 分支遗漏 _write_inputs_index 调用，
        导致 executor prologue 无法按别名装载 inputs → KeyError。
        """
        import json
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_dw_index_")
        try:
            from tianshu_datadev.spark.snapshot import (
                SnapshotBuilder,
                SnapshotSourceProvider,
                SnapshotSourceType,
            )
            builder = SnapshotBuilder(output_dir=tmpdir)
            provider = SnapshotSourceProvider(
                provider_id="dev_wh_001",
                source_type=SnapshotSourceType.DEV_WAREHOUSE,
                connection_alias="dev_warehouse_readonly",
                allowlisted_tables=["dw.dim_date", "dw.fact_orders"],
                base_path="/data/dev_warehouse/",
                description="开发环境只读数据仓库",
            )
            manifest = builder.build(
                contract_hash="dw_test",
                source_tables=["dw.dim_date", "dw.fact_orders"],
                provider=provider,
                table_aliases={"dw.dim_date": "dd", "dw.fact_orders": "fo"},
            )
            index_path = os.path.join(manifest.snapshot_dir, "_inputs_index.json")
            assert os.path.isfile(index_path), (
                f"DEV_WAREHOUSE 路径应写 _inputs_index.json——路径={index_path}"
            )
            with open(index_path, encoding="utf-8") as fp:
                index = json.load(fp)
            assert index == {
                "dd": "dw.dim_date.parquet",
                "fo": "dw.fact_orders.parquet",
            }
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════
# Phase 9B-P0: Pipeline + SnapshotBuilder 集成测试
# ════════════════════════════════════════════


class TestSnapshotPipelineIntegration:
    """Pipeline.run_all() + SnapshotBuilder 端到端集成测试。

    验证：
    1. 无 SnapshotBuilder 时 Pipeline 行为不变（向后兼容）
    2. 有 SnapshotBuilder + Provider 时 run_all() 产出 SnapshotManifest
    3. export_artifacts() 能导出 snapshot_manifest
    4. Snapshot 失败不阻断 run_all() 主流程
    5. 白名单外 table 被过滤，不传给 SnapshotBuilder
    """

    # ── 辅助方法 ──

    @staticmethod
    def _tests_dir() -> str:
        """返回 tests/ 目录的绝对路径。"""
        return os.path.dirname(os.path.dirname(__file__))

    # ── 测试方法 ──

    def test_run_all_without_snapshot_builder_still_works(self):
        """无 SnapshotBuilder 时 run_all() 行为不变——向后兼容。"""
        pipeline = Pipeline()  # 不注入 SnapshotBuilder
        md = read_fixture("fixtures/golden/golden_passing.md")

        # 需提供 test_fact 的 table_paths 以保证 golden spec SQL 可执行
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        assert result["request_id"], "无 SnapshotBuilder 时 run_all 应正常返回 request_id"
        assert result["package_id"], "无 SnapshotBuilder 时应正常生成 package_id"
        # export_artifacts 应正常返回（snapshot_manifest 为 None）
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.snapshot_manifest is None, (
            "未注入 SnapshotBuilder 时 snapshot_manifest 应为 None"
        )

    def test_run_all_with_snapshot_builder_produces_manifest(self, local_fixture_provider):
        """注入 SnapshotBuilder + LOCAL_FIXTURE Provider 时 run_all() 产出 SnapshotManifest。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_snap_int_")
        snapshot_builder = SnapshotBuilder(output_dir=tmpdir)

        # 注入 SnapshotBuilder + Provider——使用 order_info fixture
        pipeline = Pipeline(
            snapshot_builder=snapshot_builder,
            snapshot_provider=local_fixture_provider,
        )
        md = read_fixture("fixtures/golden/golden_passing.md")

        # table_paths 中需同时包含：
        #   - test_fact：golden spec SQL 执行所需
        #   - order_info：快照白名单内表，SnapshotBuilder 可处理
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
            "order_info": os.path.join(fixture_dir, "fixtures", "order_info.csv"),
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        assert result["request_id"], "run_all 应正常返回 request_id"
        assert result["package_id"], "run_all 应正常生成 package_id"

        # 通过 export_artifacts 验证 SnapshotManifest 存在
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.snapshot_manifest is not None, (
            "注入 SnapshotBuilder + Provider 后应产出 SnapshotManifest"
        )
        manifest = bundle.snapshot_manifest
        assert manifest.snapshot_id.startswith("snap_"), (
            f"snapshot_id 应以 'snap_' 开头，实际: {manifest.snapshot_id}"
        )
        assert len(manifest.files) == 1, (
            f"应生成 1 个快照文件，实际: {len(manifest.files)}"
        )
        assert manifest.files[0].source_name == "order_info"
        assert manifest.files[0].row_count > 0, "快照文件应有实际行数"
        assert manifest.files[0].file_sha256, "快照文件应有 SHA-256"
        assert manifest.snapshot_sha256, "快照清单应有整体完整性 hash"

        # 清理临时目录
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_export_artifacts_includes_snapshot_manifest(self, local_fixture_provider):
        """export_artifacts() 能导出 snapshot_manifest——供下游 Orchestrator/Harness 消费。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_snap_exp_")
        snapshot_builder = SnapshotBuilder(output_dir=tmpdir)

        pipeline = Pipeline(
            snapshot_builder=snapshot_builder,
            snapshot_provider=local_fixture_provider,
        )
        md = read_fixture("fixtures/golden/golden_passing.md")
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
            "order_info": os.path.join(fixture_dir, "fixtures", "order_info.csv"),
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        # 验证 bundle 中除 snapshot_manifest 外的字段也完整
        assert bundle.sql_build_plan is not None, "sql_build_plan 不应为空"
        assert bundle.data_transform_contract is not None, "contract 不应为空"
        assert bundle.snapshot_manifest is not None, "snapshot_manifest 不应为空"

        # 验证 snapshot_manifest 与 contract 的 hash 关联
        from tianshu_datadev.artifacts.models import (
            DataTransformContractLite,
            DataTransformContractV1,
        )
        contract = bundle.data_transform_contract
        if isinstance(contract, DataTransformContractV1):
            contract_hash = DataTransformContractV1.compute_contract_hash(contract)
        else:
            contract_hash = DataTransformContractLite.compute_contract_hash(contract)
        assert bundle.snapshot_manifest.contract_hash == contract_hash, (
            "snapshot_manifest.contract_hash 应与 contract 的 compute_contract_hash() 一致"
        )

        # 清理临时目录
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_snapshot_failure_does_not_block_run_all(self, local_fixture_provider):
        """Snapshot 构建失败不阻断 run_all() 主流程——优雅降级。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_snap_fail_")
        # 构造一个会失败的 provider——白名单空（source_tables 过滤后为空，但不抛异常）
        # 真正的失败场景：provider 白名单不含 table_paths 中的表 → source_tables 为空 → 跳过 snapshot
        pipeline = Pipeline(
            snapshot_builder=SnapshotBuilder(output_dir=tmpdir),
            snapshot_provider=local_fixture_provider,
        )
        md = read_fixture("fixtures/golden/golden_passing.md")
        # 传入白名单外的表——会被过滤掉，source_tables 为空，跳过 snapshot
        # 同时需提供 test_fact 保证 golden spec SQL 可正常执行
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
            "unknown_table": "/nonexistent/path.csv",
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        assert result["request_id"], "snapshot 失败不应阻断 run_all"
        assert result["package_id"], "snapshot 失败不应阻断 package 生成"

        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        # source_tables 过滤后为空 → 不调用 build → snapshot_manifest 为 None
        assert bundle.snapshot_manifest is None, (
            "白名单外 table 被过滤后 snapshot_manifest 应为 None"
        )

        # 清理临时目录
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_backward_compatible_no_snapshot_params(self):
        """Pipeline() 无 snapshot 参数时完全向后兼容——已有测试不受影响。"""
        # 不传任何 snapshot 参数
        pipeline = Pipeline()
        assert pipeline._snapshot_builder is None
        assert pipeline._snapshot_provider is None

    # ── Phase 9B-P1: provenance.yml 显式 snapshot_manifest_hash 断言 ──

    def test_provenance_yml_contains_snapshot_manifest_hash(self, local_fixture_provider):
        """provenance.yml 中 snapshot_manifest_hash 非空且值正确——验收标准 #3 显式覆盖。"""
        import re
        import tempfile

        from tianshu_datadev.artifacts.provenance import compute_json_hash

        # 使用独立临时目录作为 Pipeline 的输出根目录——便于定位 provenance.yml
        out_dir = tempfile.mkdtemp(prefix="tianshu_prov_")
        snap_dir = tempfile.mkdtemp(prefix="tianshu_snap_")
        snapshot_builder = SnapshotBuilder(output_dir=snap_dir)

        pipeline = Pipeline(
            base_output_dir=out_dir,
            snapshot_builder=snapshot_builder,
            snapshot_provider=local_fixture_provider,
        )
        md = read_fixture("fixtures/golden/golden_passing.md")
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
            "order_info": os.path.join(fixture_dir, "fixtures", "order_info.csv"),
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        request_id = result["request_id"]

        # 获取实际的 snapshot_manifest——用于验证 hash 正确性
        bundle = pipeline.export_artifacts(request_id)
        assert bundle is not None
        assert bundle.snapshot_manifest is not None, (
            "注入 SnapshotBuilder + Provider 后 snapshot_manifest 不应为空"
        )
        manifest_dict = bundle.snapshot_manifest.model_dump()

        # 读取生成的 provenance.yml
        prov_path = os.path.join(out_dir, request_id, "provenance.yml")
        assert os.path.isfile(prov_path), (
            f"provenance.yml 未生成于预期路径: {prov_path}"
        )
        with open(prov_path, "r", encoding="utf-8") as f:
            prov_content = f.read()

        # 提取 snapshot_manifest_hash——应为 64 位 hex 字符串
        match = re.search(r"snapshot_manifest_hash:\s*\"([0-9a-f]+)\"", prov_content)
        assert match is not None, (
            f"provenance.yml 中未找到 snapshot_manifest_hash 字段\n"
            f"文件内容: {prov_content[:500]}"
        )
        snapshot_hash = match.group(1)
        assert len(snapshot_hash) == 64, (
            f"snapshot_manifest_hash 应为 64 位 hex，实际长度: {len(snapshot_hash)}"
        )

        # ★ 显式正确性断言——provenance.yml 中的 hash 必须与 compute_json_hash 一致
        expected_hash = compute_json_hash(manifest_dict)
        assert snapshot_hash == expected_hash, (
            f"snapshot_manifest_hash 不正确\n"
            f"  provenance.yml 中: {snapshot_hash}\n"
            f"  compute_json_hash: {expected_hash}"
        )

        # 清理
        shutil.rmtree(out_dir, ignore_errors=True)
        shutil.rmtree(snap_dir, ignore_errors=True)

    def test_snapshot_manifest_hash_empty_when_no_snapshot_integration(self):
        """不注入 SnapshotBuilder 时 provenance.yml 中 snapshot_manifest_hash 必须为空——生产默认路径。"""
        import tempfile

        out_dir = tempfile.mkdtemp(prefix="tianshu_prov_nosnap_")

        # 不注入 SnapshotBuilder——模拟生产默认路径
        pipeline = Pipeline(base_output_dir=out_dir)
        md = read_fixture("fixtures/golden/golden_passing.md")
        fixture_dir = self._tests_dir()
        table_paths = {
            "test_fact": os.path.join(fixture_dir, "fixtures", "sql", "test_fact.csv"),
        }

        result = pipeline.run_all(md, table_paths=table_paths)
        request_id = result["request_id"]

        # 确认 snapshot_manifest 为 None
        bundle = pipeline.export_artifacts(request_id)
        assert bundle is not None
        assert bundle.snapshot_manifest is None, (
            "未注入 SnapshotBuilder 时 snapshot_manifest 应为 None"
        )

        # 读取 provenance.yml 并验证 hash 为空
        prov_path = os.path.join(out_dir, request_id, "provenance.yml")
        assert os.path.isfile(prov_path), (
            f"provenance.yml 未生成于预期路径: {prov_path}"
        )
        with open(prov_path, "r", encoding="utf-8") as f:
            prov_content = f.read()

        # 显式验证——无快照时字段存在但值为空
        assert 'snapshot_manifest_hash: ""' in prov_content, (
            "未注入 SnapshotBuilder 时 snapshot_manifest_hash 应为空字符串\n"
            f"实际内容: {prov_content[:500]}"
        )

        # 清理
        shutil.rmtree(out_dir, ignore_errors=True)
