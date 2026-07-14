"""Phase 7A Snapshot Builder——不可变数据快照的创建与管理。

Snapshot Builder 从 SnapshotSourceProvider 白名单数据源生成 Parquet 快照，
供 DuckDB（基准 SQL）和本地 Spark（PySpark DSL）双引擎读取。

安全边界：
- 只能读 SnapshotSourceProvider 白名单内的数据源
- 禁止从 Contract 字段直接推导生产连接
- Manifest 记录完整数据源追溯链
- 快照目录生成后为只读
- snapshot_id 确定性生成，不依赖时间戳

Phase 7A 范围：
- LOCAL_FIXTURE 类型：从本地 CSV/JSON fixture 实际写入 Parquet 文件
  （使用 PyArrow，不启动 DuckDB/Spark）
- 非 LOCAL_FIXTURE 类型：生成占位清单（Phase 7B+ 实现实际写入）
"""

from __future__ import annotations

import hashlib
import json
import os
from enum import Enum
from typing import Literal

import pyarrow as pa
import pyarrow.csv as _pacsv  # noqa: F401  # pyright: ignore[reportPrivateImportUsage]
import pyarrow.json as _pajson  # noqa: F401  # pyright: ignore[reportPrivateImportUsage]
import pyarrow.parquet as pq
from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# 快照数据源配置
# ════════════════════════════════════════════


class SnapshotSourceType(str, Enum):
    """快照数据源类型白名单——Snapshot Builder 只能读取此清单内的数据源。"""

    LOCAL_FIXTURE = "local_fixture"    # 本地测试 fixture（tests/fixtures/）
    DEV_WAREHOUSE = "dev_warehouse"    # 开发环境数据仓库
    TEST_DATASET = "test_dataset"      # 测试数据集


class SnapshotSourceProvider(StrictModel):
    """快照数据源配置——Snapshot Builder 只能读取此清单内的数据源。

    白名单机制：
    - allowlisted_tables 为完全限定表名精确匹配
    - 禁止通配符、正则、模糊匹配
    - 禁止从 Contract 推导生产连接
    """

    provider_id: str                           # 数据源唯一标识
    source_type: SnapshotSourceType            # 数据源类型
    connection_alias: str                      # 受控连接别名（不是真实凭据）
    allowlisted_tables: list[str] = Field(     # 完全限定表名，精确匹配
        default_factory=list,
        description="完全限定表名精确匹配，禁止通配/正则/模糊",
    )
    base_path: str = ""                        # 数据源根路径（本地 fixture 时为目录路径）
    description: str = ""                      # 数据源用途描述


class SamplingSpec(StrictModel):
    """采样策略——确保同一 Contract 可回归复现。

    确定性采样条件：
    - seed 固定 → random/stratified 模式结果可复现
    - anchor_keys 提供稳定排序基准
    """

    mode: Literal["full", "random", "stratified", "head"] = "full"
    limit: int | None = None              # 采样行数上限
    seed: int | None = None               # 随机种子（保证可复现）
    strata_keys: list[str] = Field(default_factory=list)     # 分层采样键
    anchor_keys: list[str] = Field(default_factory=list)     # 稳定排序锚点键


# ════════════════════════════════════════════
# 快照文件与清单
# ════════════════════════════════════════════


class SnapshotFile(StrictModel):
    """快照中的单个文件——记录来源、格式、行数及完整性校验值。"""

    source_name: str                       # 对应 inputs dict 的 key
    file_path: str                         # 快照目录内的相对路径
    format: str = "parquet"                # 文件格式
    row_count: int = 0                     # 记录行数
    file_sha256: str = ""                  # 文件内容 SHA-256


class SnapshotManifest(StrictModel):
    """不可变快照清单。

    snapshot_id 确定性生成——同一 Contract + 同一数据源 → 同一 snapshot_id。
    created_at 仅元数据，不参与 snapshot_id 计算。
    """

    snapshot_id: str                       # 确定性快照 ID（sha256 派生）
    contract_hash: str                     # 来源 Contract 的 hash
    created_at: str = ""                   # ISO 8601，仅元数据，不参与 snapshot_id
    snapshot_dir: str = ""                 # 快照目录绝对路径
    files: list[SnapshotFile] = Field(default_factory=list)
    snapshot_sha256: str = ""              # 快照整体完整性 hash
    source_provider_id: str = ""           # 数据源提供方 ID
    source_type: str = ""                  # "local_fixture" / "dev_warehouse" / "test_dataset"
    sampling_spec: SamplingSpec = Field(default_factory=SamplingSpec)
    deidentification: str = "none"          # "none" / "masked_pii" / "synthetic"


# 快照目录内的 inputs 索引侧车文件名——executor prologue 据此按别名装载 inputs
_INPUTS_INDEX_FILENAME = "_inputs_index.json"


# ════════════════════════════════════════════
# SnapshotBuilder
# ════════════════════════════════════════════


class SnapshotIntegrityError(Exception):
    """快照完整性校验失败。"""

    def __init__(self, message: str, missing_files: list[str] | None = None):
        super().__init__(message)
        self.missing_files = missing_files or []


class SnapshotSourceNotAllowlistedError(Exception):
    """数据源不在 SnapshotSourceProvider 白名单中。"""

    def __init__(self, source_name: str, provider_id: str):
        super().__init__(
            f"数据源 '{source_name}' 不在 SnapshotSourceProvider "
            f"'{provider_id}' 的白名单中"
        )
        self.source_name = source_name
        self.provider_id = provider_id


class SnapshotBuilder:
    """从 Contract 生成不可变 Parquet 快照。

    安全边界：
    - 只能读 SnapshotSourceProvider 白名单内的数据源
    - 禁止从 Contract 字段直接推导生产连接
    - Manifest 记录完整数据源追溯链
    - 快照目录生成后为只读
    - DuckDB 和 Spark 都从同一目录读取

    Phase 7A 范围：
    - LOCAL_FIXTURE 类型：实际读取 CSV/JSON fixture → 写入 Parquet → 完整性校验
      （使用 PyArrow，不启动 DuckDB/Spark）
    - 非 LOCAL_FIXTURE 类型：生成占位清单（row_count=0，Phase 7B 实现实际写入）
    """

    # ── 环境指纹因子（snapshot_id 的稳定组成部分）──
    _ENV_FINGERPRINT = "tianshu-datadev-v3"

    # ── 支持的 fixture 文件格式 ──
    _FIXTURE_EXTENSIONS = (".csv", ".json", ".jsonl")

    def __init__(self, output_dir: str) -> None:
        """初始化 SnapshotBuilder。

        Args:
            output_dir: 快照输出根目录
        """
        self._output_dir = output_dir

    # ── 公共 API ──

    def build(
        self,
        contract_hash: str,
        source_tables: list[str],
        provider: SnapshotSourceProvider,
        sampling: SamplingSpec | None = None,
        table_aliases: dict[str, str] | None = None,
    ) -> SnapshotManifest:
        """构建快照——从白名单数据源生成不可变 Parquet 快照。

        LOCAL_FIXTURE 类型（Phase 7A）：
        - 从 provider.base_path 读取 CSV/JSON fixture 文件
        - 使用 PyArrow 写入 Parquet（不启动 DuckDB/Spark）
        - 计算真实 row_count 和 file_sha256

        非 LOCAL_FIXTURE 类型（Phase 7B+）：
        - 生成占位清单（row_count=0，file_sha256=""）
        - 实际数据写入推迟到后续 Phase

        Args:
            contract_hash: 来源 Contract 的 SHA-256
            source_tables: 需要快照的表名列表（对应 inputs dict 的 key）
            provider: 数据源提供方配置（必须在白名单内）
            sampling: 采样策略（默认 full）
            table_aliases: 物理表名 → 别名（inputs dict 的 key）映射。
                           提供时 SnapshotFile.source_name 置为别名，磁盘文件仍用物理名。

        Returns:
            SnapshotManifest——包含完整溯源链

        Raises:
            SnapshotSourceNotAllowlistedError: 数据源不在白名单中
            FileNotFoundError: fixture 文件不存在
        """
        # 安全门禁 1：验证数据源白名单
        self._validate_source_tables(source_tables, provider)

        # 确定采样策略
        spec = sampling or SamplingSpec()

        # 生成确定性 snapshot_id
        snapshot_id = self._generate_snapshot_id(
            contract_hash=contract_hash,
            source_tables=source_tables,
            provider_id=provider.provider_id,
            sampling_spec=spec,
        )

        # 构建快照目录路径
        snapshot_dir = os.path.join(self._output_dir, snapshot_id)

        # 根据数据源类型决定写入策略
        if provider.source_type == SnapshotSourceType.LOCAL_FIXTURE:
            # Phase 7A：实际从本地 CSV/JSON fixture 写入 Parquet
            files = self._materialize_local_fixtures(
                source_tables=source_tables,
                provider=provider,
                snapshot_dir=snapshot_dir,
                sampling_spec=spec,
                table_aliases=table_aliases,
            )
            # 写 inputs 索引侧车——executor prologue 按别名装载 inputs
            self._write_inputs_index(snapshot_dir, files)
        else:
            # 非 LOCAL_FIXTURE：占位清单（Phase 7B+ 实现实际写入）
            # 仍需创建目录和写入 _inputs_index.json——确保 executor prologue
            # 能按别名装载 inputs（即使 parquet 文件尚不存在，索引提供正确映射）
            files: list[SnapshotFile] = []
            _aliases = table_aliases or {}
            os.makedirs(snapshot_dir, exist_ok=True)
            for table_name in sorted(source_tables):
                file_path = os.path.join(snapshot_dir, f"{table_name}.parquet")
                files.append(SnapshotFile(
                    source_name=_aliases.get(table_name, table_name),
                    file_path=file_path,
                    format="parquet",
                    row_count=0,           # Phase 7B 填充
                    file_sha256="",        # Phase 7B 填充
                ))
            # 写 inputs 索引侧车——executor prologue 按别名装载 inputs
            # 即使 parquet 文件尚未写入，索引提供正确的 别名→物理文件名 映射
            self._write_inputs_index(snapshot_dir, files)

        # 计算快照整体 hash
        snapshot_sha256 = self._compute_snapshot_hash(files)

        manifest = SnapshotManifest(
            snapshot_id=snapshot_id,
            contract_hash=contract_hash,
            created_at="",             # 不参与 hash——仅 Phase 7B 写入时填充
            snapshot_dir=snapshot_dir,
            files=files,
            snapshot_sha256=snapshot_sha256,
            source_provider_id=provider.provider_id,
            source_type=provider.source_type.value,
            sampling_spec=spec,
            deidentification="none",
        )

        return manifest

    def verify_integrity(self, manifest: SnapshotManifest) -> bool:
        """校验快照完整性——检查 Manifest 结构 + 文件存在性 + SHA-256。

        LOCAL_FIXTURE 类型（Phase 7A）：
        - 检查 Manifest 结构完整性（文件列表 + hash）
        - 检查每个 Parquet 文件实际存在于磁盘
        - 校验每个文件的 SHA-256 与 Manifest 记录一致

        非 LOCAL_FIXTURE 类型：
        - 仅检查 Manifest 结构完整性（文件尚未实际写入）

        Args:
            manifest: 待校验的快照清单

        Returns:
            True 表示完整性校验通过

        Raises:
            SnapshotIntegrityError: 完整性校验失败（含缺失文件列表）
        """
        # Step 1：结构完整性校验（所有类型均适用）
        if not manifest.files:
            raise SnapshotIntegrityError(
                "快照清单中无文件记录",
                missing_files=[],
            )

        if not manifest.snapshot_sha256:
            raise SnapshotIntegrityError(
                "快照清单缺少完整性 hash",
                missing_files=[],
            )

        expected_hash = self._compute_snapshot_hash(manifest.files)
        if expected_hash != manifest.snapshot_sha256:
            raise SnapshotIntegrityError(
                f"快照完整性 hash 不一致："
                f"期望 {expected_hash}，实际 {manifest.snapshot_sha256}",
            )

        # Step 2：LOCAL_FIXTURE 类型检查实际文件存在性和内容完整性
        if manifest.source_type == SnapshotSourceType.LOCAL_FIXTURE.value:
            missing_files: list[str] = []
            hash_mismatches: list[str] = []

            for sf in manifest.files:
                # 检查文件是否存在
                if not os.path.isfile(sf.file_path):
                    missing_files.append(sf.file_path)
                    continue

                # 检查文件 SHA-256 是否匹配
                if sf.file_sha256:
                    actual_sha = self._compute_file_sha256(sf.file_path)
                    if actual_sha != sf.file_sha256:
                        hash_mismatches.append(
                            f"{sf.source_name}: 期望 {sf.file_sha256[:16]}...，"
                            f"实际 {actual_sha[:16]}..."
                        )

            if missing_files:
                raise SnapshotIntegrityError(
                    f"快照文件缺失 ({len(missing_files)} 个)：{missing_files}",
                    missing_files=missing_files,
                )

            if hash_mismatches:
                raise SnapshotIntegrityError(
                    f"快照文件 hash 不一致 ({len(hash_mismatches)} 个)：{hash_mismatches}",
                )

        return True

    # ── 内部方法 ──

    def _validate_source_tables(
        self,
        source_tables: list[str],
        provider: SnapshotSourceProvider,
    ) -> None:
        """安全门禁：验证所有请求的数据源都在白名单内。

        精确匹配——不支持通配符或正则。
        """
        allowlisted = set(provider.allowlisted_tables)
        for table in source_tables:
            if table not in allowlisted:
                raise SnapshotSourceNotAllowlistedError(
                    source_name=table,
                    provider_id=provider.provider_id,
                )

    @staticmethod
    def _generate_snapshot_id(
        contract_hash: str,
        source_tables: list[str],
        provider_id: str,
        sampling_spec: SamplingSpec,
    ) -> str:
        """生成确定性 snapshot_id。

        输入因子（排序确保确定性）：
        - contract_hash
        - 排序后的 source_tables 列表
        - provider_id
        - sampling_spec 的 model_dump（排序键）
        - 环境指纹

        created_at 不参与——时间戳不能成为确定性 ID 的一部分。
        """
        sampling_data = sampling_spec.model_dump(exclude_none=True)
        payload = {
            "contract_hash": contract_hash,
            "source_tables": sorted(source_tables),
            "provider_id": provider_id,
            "sampling_spec": sampling_data,
            "env": SnapshotBuilder._ENV_FINGERPRINT,
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()
        return f"snap_{hash_hex[:16]}"

    @staticmethod
    def _compute_snapshot_hash(files: list[SnapshotFile]) -> str:
        """基于文件列表计算快照整体完整性 hash。

        对文件列表按 source_name 排序后聚合计算。
        """
        sorted_files = sorted(files, key=lambda f: f.source_name)
        payload = {
            "files": [
                {
                    "source_name": f.source_name,
                    "format": f.format,
                    "file_sha256": f.file_sha256,
                }
                for f in sorted_files
            ],
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def _compute_file_sha256(file_path: str) -> str:
        """计算单个文件的 SHA-256 哈希值。

        Args:
            file_path: 文件绝对路径

        Returns:
            十六进制 SHA-256 字符串
        """
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            # 分块读取以支持大文件
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()

    @staticmethod
    def _find_fixture_file(table_name: str, base_path: str) -> str:
        """在 base_path 下查找 fixture 文件——按扩展名优先级尝试。

        优先级：.csv > .json > .jsonl

        Args:
            table_name: 表名（不含扩展名）
            base_path: 数据源根路径

        Returns:
            匹配的 fixture 文件绝对路径

        Raises:
            FileNotFoundError: 未找到任何匹配的 fixture 文件
        """
        for ext in SnapshotBuilder._FIXTURE_EXTENSIONS:
            candidate = os.path.join(base_path, f"{table_name}{ext}")
            if os.path.isfile(candidate):
                return candidate

        raise FileNotFoundError(
            f"在 '{base_path}' 中未找到表 '{table_name}' 的 fixture 文件"
            f"（尝试扩展名: {SnapshotBuilder._FIXTURE_EXTENSIONS}）"
        )

    @staticmethod
    def _read_fixture_to_table(file_path: str) -> pa.Table:
        """读取 fixture 文件为 PyArrow Table。

        根据扩展名自动选择读取器：
        - .csv → pyarrow.csv.read_csv
        - .json / .jsonl → pyarrow.json.read_json

        Args:
            file_path: fixture 文件路径

        Returns:
            PyArrow Table

        Raises:
            ValueError: 不支持的文件格式
        """
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            # read_options 自动推断类型；ParseOptions 处理常见 CSV 方言
            return _pacsv.read_csv(  # pyright: ignore[reportPrivateImportUsage]
                file_path,
                read_options=_pacsv.ReadOptions(),  # pyright: ignore[reportPrivateImportUsage]
                parse_options=_pacsv.ParseOptions(),  # pyright: ignore[reportPrivateImportUsage]
            )
        elif ext in (".json", ".jsonl"):
            return _pajson.read_json(file_path)  # pyright: ignore[reportPrivateImportUsage]
        else:
            raise ValueError(f"不支持的 fixture 文件格式: {ext}（支持 .csv / .json / .jsonl）")

    @staticmethod
    def _apply_sampling(
        table: pa.Table,
        sampling_spec: SamplingSpec,
    ) -> pa.Table:
        """对 PyArrow Table 应用采样策略。

        Phase 7A 支持的采样模式：
        - full: 全量（默认，不做裁剪）
        - head: 取前 N 行
        - random / stratified: Phase 7B+ 实现，Phase 7A 退化为 full

        Args:
            table: 源数据表
            sampling_spec: 采样策略

        Returns:
            采样后的 PyArrow Table
        """
        mode = sampling_spec.mode

        if mode == "head" and sampling_spec.limit is not None:
            return table.slice(0, sampling_spec.limit)

        # full / random / stratified → Phase 7A 均返回全量
        # random 和 stratified 的确定性采样在 Phase 7B 实现
        return table

    def _materialize_local_fixtures(
        self,
        source_tables: list[str],
        provider: SnapshotSourceProvider,
        snapshot_dir: str,
        sampling_spec: SamplingSpec,
        table_aliases: dict[str, str] | None = None,
    ) -> list[SnapshotFile]:
        """从本地 fixture 文件写入 Parquet 快照——Phase 7A 核心实现。

        流程：
        1. 在 provider.base_path 中查找 fixture 文件（.csv / .json）
        2. 用 PyArrow 读取为 Table
        3. 应用采样策略
        4. 写入 Parquet 到 snapshot_dir
        5. 计算 row_count 和 file_sha256

        不启动 DuckDB/Spark——全程使用 PyArrow。

        Args:
            source_tables: 需要快照的表名列表
            provider: 数据源提供方（source_type 必须为 LOCAL_FIXTURE）
            snapshot_dir: 快照输出目录
            sampling_spec: 采样策略
            table_aliases: 物理表名 → 别名映射。提供时 source_name 用别名。

        Returns:
            SnapshotFile 列表——含真实 row_count 和 file_sha256

        Raises:
            FileNotFoundError: fixture 文件不存在
        """
        os.makedirs(snapshot_dir, exist_ok=True)
        _aliases = table_aliases or {}

        files: list[SnapshotFile] = []
        for table_name in sorted(source_tables):
            # 查找 fixture 文件
            fixture_path = self._find_fixture_file(table_name, provider.base_path)

            # 读取 fixture → PyArrow Table
            table = self._read_fixture_to_table(fixture_path)

            # 应用采样
            table = self._apply_sampling(table, sampling_spec)

            # 磁盘文件名保持物理名——DuckDB 视图注册依赖物理文件名
            parquet_path = os.path.join(snapshot_dir, f"{table_name}.parquet")
            pq.write_table(table, parquet_path)

            # 计算元数据
            row_count = table.num_rows
            file_sha256 = self._compute_file_sha256(parquet_path)

            files.append(SnapshotFile(
                # source_name 用别名（inputs dict 的 key）——无别名时回退物理名
                source_name=_aliases.get(table_name, table_name),
                file_path=parquet_path,
                format="parquet",
                row_count=row_count,
                file_sha256=file_sha256,
            ))

        return files

    @staticmethod
    def _write_inputs_index(snapshot_dir: str, files: list[SnapshotFile]) -> None:
        """写 inputs 索引侧车——记录 {source_name(别名): 物理文件名}。

        executor prologue 读取此索引，按别名装载 Parquet 为 inputs dict。
        与 DuckDB 的 glob-by-stem 视图注册互不干扰（索引为 .json，非 .parquet）。
        """
        index = {f.source_name: os.path.basename(f.file_path) for f in files}
        index_path = os.path.join(snapshot_dir, _INPUTS_INDEX_FILENAME)
        with open(index_path, "w", encoding="utf-8") as fp:
            json.dump(index, fp, sort_keys=True, ensure_ascii=False)
