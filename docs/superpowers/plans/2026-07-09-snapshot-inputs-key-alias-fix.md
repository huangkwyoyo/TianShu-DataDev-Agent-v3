# 快照 inputs key 别名对齐修复实施计划（方案 A）

> **For agentic workers:** REQUIRED SUB-SKILL: 用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务执行本计划。步骤用 checkbox（`- [ ]`）跟踪。

**Goal:** 修复快照 Parquet 文件命名（物理名）与 PySpark 代码 `inputs[...]` key（别名）不一致导致的 KeyError 崩溃——让快照的 `SnapshotFile.source_name` 承载别名（inputs key），executor prologue 通过 manifest 侧车索引按别名装载 inputs。

**Architecture:** 补全 SnapshotManifest 既有的 `source_name`（"对应 inputs dict 的 key"）契约。Parquet 文件在磁盘上**保持物理名不变**（DuckDB `_register_parquet_views` 按物理文件名建视图，零影响）；`SnapshotFile.source_name` 改为别名；快照目录写入侧车索引 `_inputs_index.json`（`{别名: 物理文件名}`）；executor prologue 优先读该索引装载 `inputs`，无索引时回退旧的 glob-by-stem 行为（向后兼容）。Contract 保持物理名无关（不动 `contract_extractor`）。

**Tech Stack:** Python 3.11+、PyArrow（快照写盘）、PySpark（子进程执行）、pytest。

## Global Constraints

- 所有代码注释、docstring 使用**中文**，解释"为什么"而非"是什么"。
- **Parquet 文件磁盘名必须保持物理名**（`{physical_table}.parquet`）——DuckDB 视图注册依赖它，不得改为别名文件名。
- **不修改** `src/tianshu_datadev/artifacts/contract_extractor.py`——Contract 的 `source_table=step.table_ref` 语义保持不变，Contract 保持物理名无关与确定性 hash。
- `snapshot_id` 只依赖 `contract_hash + source_tables(物理) + provider + sampling`，本次改动**不得**改变 `snapshot_id` 生成逻辑（确定性保留）。`snapshot_sha256` 因 `source_name` 变化而变化属预期——如有冻结该值的测试需同步更新。
- executor prologue 改动必须**向后兼容**：无 `_inputs_index.json` 时回退到 glob-by-stem，旧快照/旧测试不受影响。
- `table_aliases` 缺失或某物理表无对应别名时，`source_name` 回退为物理名（不崩溃，维持现状行为）。
- 源码修改后验证前，若需重启服务走 `./dev-reload.sh`（本计划以 pytest 为主，通常无需重启）。

---

## File Structure

- `src/tianshu_datadev/spark/snapshot.py`（修改）——`SnapshotBuilder.build` / `_materialize_local_fixtures` 增加 `table_aliases` 参数；`SnapshotFile.source_name` 置为别名；新增 `_write_inputs_index` 写侧车索引。
- `src/tianshu_datadev/spark/executor.py`（修改）——`_SPARK_PROLOGUE_TEMPLATE` 优先读 `_inputs_index.json` 装载 inputs，回退 glob。
- `src/tianshu_datadev/api/pipeline.py`（修改）——两处 `SnapshotBuilder.build` 调用点（约 1305、1576 行）传入从 `table_mapping` 反推的 `table_aliases`。
- `tests/spark/test_snapshot.py`（修改）——source_name=别名、文件保物理名、索引写入、向后兼容的单元测试。
- `tests/spark/test_spark_executor.py`（修改）——prologue 注入索引读取逻辑的断言 + 真实 Spark 装载测试（有 pyspark 守卫）。
- `tests/spark/test_physical_verifier.py`（修改）——E2E 回归：别名 source_name + `inputs["别名"]` 全链路不再 KeyError。

---

## Task 1：SnapshotBuilder 让 source_name 承载别名 + 写侧车索引

**Files:**
- Modify: `src/tianshu_datadev/spark/snapshot.py`（`build` ~174-260、`_materialize_local_fixtures` ~535-564、新增 `_write_inputs_index`）
- Test: `tests/spark/test_snapshot.py`

**Interfaces:**
- Consumes: 无（新增可选参数，调用方 Task 3 传入）
- Produces:
  - `SnapshotBuilder.build(contract_hash, source_tables, provider, sampling=None, table_aliases: dict[str, str] | None = None)` —— `table_aliases` 为 `{物理表名: 别名}`
  - `SnapshotBuilder._materialize_local_fixtures(source_tables, provider, snapshot_dir, sampling_spec, table_aliases: dict[str, str] | None = None)`
  - `SnapshotBuilder._write_inputs_index(snapshot_dir: str, files: list[SnapshotFile]) -> None` —— 写 `_inputs_index.json` = `{source_name: basename(file_path)}`
  - 侧车文件名常量 `_INPUTS_INDEX_FILENAME = "_inputs_index.json"`（模块级）

- [ ] **Step 1: 写失败测试——source_name=别名，文件保物理名**

在 `tests/spark/test_snapshot.py` 新增（复用文件已有的 provider/fixture 构造辅助；若无则参照现有 LOCAL_FIXTURE 测试的 provider 构造方式）：

```python
def test_build_source_name_uses_alias_file_keeps_physical(tmp_path):
    """table_aliases 提供时——SnapshotFile.source_name 用别名，磁盘文件名保持物理名。"""
    # 构造 LOCAL_FIXTURE provider（base_path 下放 fact_trips_sample.csv）
    provider, builder = _make_local_fixture_builder(tmp_path)  # 见 Step 3 辅助
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
    import os
    assert os.path.isfile(f.file_path)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m pytest tests/spark/test_snapshot.py::test_build_source_name_uses_alias_file_keeps_physical -v`
Expected: FAIL——`build()` 无 `table_aliases` 参数（TypeError）或 `source_name == "fact_trips_sample"`。

- [ ] **Step 3: 实现——build/materialize 增加 table_aliases**

在 `snapshot.py` 模块级（靠近其他常量）新增：

```python
# 快照目录内的 inputs 索引侧车文件名——executor prologue 据此按别名装载 inputs
_INPUTS_INDEX_FILENAME = "_inputs_index.json"
```

修改 `build` 签名与两个分支（约 174-242 行）：

```python
    def build(
        self,
        contract_hash: str,
        source_tables: list[str],
        provider: SnapshotSourceProvider,
        sampling: SamplingSpec | None = None,
        table_aliases: dict[str, str] | None = None,
    ) -> SnapshotManifest:
```

在 docstring 的 Args 补一行：

```
            table_aliases: 物理表名 → 别名（inputs dict 的 key）映射。
                           提供时 SnapshotFile.source_name 置为别名，磁盘文件仍用物理名。
```

LOCAL_FIXTURE 分支传入 `table_aliases`：

```python
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
```

占位分支（非 LOCAL_FIXTURE）同样让 source_name 用别名：

```python
        else:
            # 非 LOCAL_FIXTURE：占位清单（Phase 7B+ 实现实际写入）
            files: list[SnapshotFile] = []
            _aliases = table_aliases or {}
            for table_name in sorted(source_tables):
                file_path = os.path.join(snapshot_dir, f"{table_name}.parquet")
                files.append(SnapshotFile(
                    source_name=_aliases.get(table_name, table_name),
                    file_path=file_path,
                    format="parquet",
                    row_count=0,
                    file_sha256="",
                ))
```

修改 `_materialize_local_fixtures`（约 535-564 行）签名与 source_name：

```python
    def _materialize_local_fixtures(
        self,
        source_tables: list[str],
        provider: SnapshotSourceProvider,
        snapshot_dir: str,
        sampling_spec: SamplingSpec,
        table_aliases: dict[str, str] | None = None,
    ) -> list[SnapshotFile]:
```

循环内文件名保持物理名，source_name 用别名：

```python
        os.makedirs(snapshot_dir, exist_ok=True)
        _aliases = table_aliases or {}

        files: list[SnapshotFile] = []
        for table_name in sorted(source_tables):
            fixture_path = self._find_fixture_file(table_name, provider.base_path)
            table = self._read_fixture_to_table(fixture_path)
            table = self._apply_sampling(table, sampling_spec)

            # 磁盘文件名保持物理名——DuckDB 视图注册依赖物理文件名
            parquet_path = os.path.join(snapshot_dir, f"{table_name}.parquet")
            pq.write_table(table, parquet_path)

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
```

新增 `_write_inputs_index` 方法（放在 `_materialize_local_fixtures` 之后）：

```python
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
```

> `json` 已在 `snapshot.py` 顶部导入（`_compute_snapshot_hash` 使用了 `json.dumps`），无需新增 import。若辅助 `_make_local_fixture_builder` 尚不存在，在测试文件顶部新增：读取现有 LOCAL_FIXTURE 测试的 provider/builder 构造，抽出一个 `_make_local_fixture_builder(tmp_path)`，在 `tmp_path` 下写 `fact_trips_sample.csv`（含 2 行样例），返回 `(provider, SnapshotBuilder(output_dir=str(tmp_path/"snap")))`。

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m pytest tests/spark/test_snapshot.py::test_build_source_name_uses_alias_file_keeps_physical -v`
Expected: PASS

- [ ] **Step 5: 写并运行索引写入 + 向后兼容测试**

新增两个测试：

```python
def test_build_writes_inputs_index(tmp_path):
    """写入 _inputs_index.json——{别名: 物理文件名}。"""
    import json, os
    provider, builder = _make_local_fixture_builder(tmp_path)
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


def test_build_without_aliases_keeps_physical_source_name(tmp_path):
    """未提供 table_aliases——source_name 回退物理名（向后兼容）。"""
    provider, builder = _make_local_fixture_builder(tmp_path)
    manifest = builder.build(
        contract_hash="c_hash",
        source_tables=["fact_trips_sample"],
        provider=provider,
    )
    f = next(x for x in manifest.files if x.file_path.endswith(".parquet"))
    assert f.source_name == "fact_trips_sample"
```

Run: `python -m pytest tests/spark/test_snapshot.py -v -k "inputs_index or source_name or physical"`
Expected: 全部 PASS

- [ ] **Step 6: 回归 + 修正冻结 hash 测试（如有）**

Run: `python -m pytest tests/spark/test_snapshot.py -q`
若有断言固定 `snapshot_sha256` 字面值的测试因 source_name 变化失败——用别名场景重算期望值并更新（这是预期变化，非缺陷）。用 `grep -rn "snapshot_sha256" tests/` 定位。
Expected: 全绿。

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/spark/snapshot.py tests/spark/test_snapshot.py
git commit -m "fix(snapshot): source_name 承载别名 + 写 _inputs_index 侧车，parquet 保持物理名"
```

---

## Task 2：Executor prologue 优先按 _inputs_index.json 装载 inputs

**Files:**
- Modify: `src/tianshu_datadev/spark/executor.py`（`_SPARK_PROLOGUE_TEMPLATE` ~131-148）
- Test: `tests/spark/test_spark_executor.py`

**Interfaces:**
- Consumes: 快照目录内的 `_inputs_index.json`（Task 1 产出）
- Produces: prologue 装载后 `inputs` 的 key 为别名；无索引时回退 glob-by-stem（key 为文件名 stem）

- [ ] **Step 1: 写失败测试——prologue 含索引读取分支**

`_SPARK_PROLOGUE_TEMPLATE` 是模块级字符串常量，可直接对其内容断言（不需真实 Spark）：

```python
from tianshu_datadev.spark.executor import _SPARK_PROLOGUE_TEMPLATE


def test_prologue_reads_inputs_index_before_glob():
    """prologue 优先读 _inputs_index.json，回退 glob——两条路径都在模板里。"""
    tpl = _SPARK_PROLOGUE_TEMPLATE
    # 索引优先分支
    assert "_inputs_index.json" in tpl
    # 回退分支保留
    assert "*.parquet" in tpl
    # 索引读取用 json
    assert "json" in tpl
```

- [ ] **Step 2: 运行确认失败**

Run: `python -m pytest tests/spark/test_spark_executor.py::test_prologue_reads_inputs_index_before_glob -v`
Expected: FAIL——模板不含 `_inputs_index.json`。

- [ ] **Step 3: 实现——改写 prologue 模板**

将 `executor.py` 的 `_SPARK_PROLOGUE_TEMPLATE`（131-148 行）替换为：

```python
_SPARK_PROLOGUE_TEMPLATE = '''# ── Executor 注入：Spark 初始化 + 数据加载 ──
import os as _tianshu_os, glob as _tianshu_glob, json as _tianshu_json
from pyspark.sql import SparkSession as _TianShuSpark, functions as F
_tianshu_builder = _TianShuSpark.builder
_tianshu_builder = _tianshu_builder.appName("tianshu_executor")
_tianshu_builder = _tianshu_builder.master("local[1]")
_tianshu_builder = _tianshu_builder.config("spark.ui.enabled", "false")
_tianshu_builder = _tianshu_builder.config("spark.sql.adaptive.enabled", "false")
_tianshu_spark = _tianshu_builder.getOrCreate()
# 构造 inputs 字典——优先读快照侧车索引（key=别名），无索引时回退按文件名 stem
inputs: dict = {}
_data_dir = _tianshu_os.environ.get("SPARK_DATA_DIR", "")
if _data_dir and _tianshu_os.path.isdir(_data_dir):
    _index_path = _tianshu_os.path.join(_data_dir, "_inputs_index.json")
    if _tianshu_os.path.isfile(_index_path):
        # 索引路径：{别名: 物理文件名}——按别名装载，与 PySpark 代码 inputs[别名] 对齐
        with open(_index_path, "r", encoding="utf-8") as _idx_f:
            _index = _tianshu_json.load(_idx_f)
        for _key, _fname in _index.items():
            inputs[_key] = _tianshu_spark.read.parquet(
                _tianshu_os.path.join(_data_dir, _fname)
            )
    else:
        # 回退路径：无索引的旧快照——按文件名 stem 做 key（向后兼容）
        _files = sorted(_tianshu_glob.glob(_tianshu_os.path.join(_data_dir, "*.parquet")))
        for _f in _files:
            _name = _tianshu_os.path.splitext(_tianshu_os.path.basename(_f))[0]
            inputs[_name] = _tianshu_spark.read.parquet(_f)
'''
```

- [ ] **Step 4: 运行确认通过**

Run: `python -m pytest tests/spark/test_spark_executor.py::test_prologue_reads_inputs_index_before_glob -v`
Expected: PASS

- [ ] **Step 5: 写真实 Spark 装载测试（有 pyspark 守卫）**

参照 `tests/spark/test_physical_verifier.py` 现有真实 Spark 测试的 skip 守卫方式（`pytest.importorskip("pyspark")` 或项目既有的 marker），新增：

```python
def test_execute_loads_inputs_by_alias_from_index(tmp_path):
    """真实子进程：写 parquet + _inputs_index.json，inputs['ft'] 可解析，无 KeyError。"""
    import pytest
    pytest.importorskip("pyspark")
    import json, os
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tianshu_datadev.spark.executor import LocalSparkExecutor

    data_dir = tmp_path / "snap"
    data_dir.mkdir()
    # 磁盘物理名 fact_trips_sample.parquet
    pq.write_table(
        pa.table({"amount": [1, 2, 3]}),
        str(data_dir / "fact_trips_sample.parquet"),
    )
    # 索引把别名 ft 指向物理文件
    (data_dir / "_inputs_index.json").write_text(
        json.dumps({"ft": "fact_trips_sample.parquet"}), encoding="utf-8"
    )

    executor = LocalSparkExecutor()
    # 代码用别名 ft——修复前会 KeyError
    result = executor.execute("result_df = inputs['ft']", data_dir=str(data_dir))
    assert result.status.name == "SUCCESS", result.error_message
```

Run: `python -m pytest tests/spark/test_spark_executor.py::test_execute_loads_inputs_by_alias_from_index -v`
Expected: PASS（无 pyspark 环境则 SKIP）

- [ ] **Step 6: 回归 + Commit**

Run: `python -m pytest tests/spark/test_spark_executor.py -q`
Expected: 全绿（或既有 skip）。

```bash
git add src/tianshu_datadev/spark/executor.py tests/spark/test_spark_executor.py
git commit -m "fix(executor): prologue 优先读 _inputs_index 按别名装载 inputs，回退 glob"
```

---

## Task 3：pipeline 两处 build 调用点传入 table_aliases

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（约 1299-1309 与 1570-1580 两处 `build` 调用）
- Test: `tests/spark/test_snapshot.py` 或 `tests/api/`（对 pipeline 反推映射的单元测试）

**Interfaces:**
- Consumes: 方法作用域内的 `table_mapping`（`{别名: 物理表名}`，两处调用点均在作用域内——1204/1214、1438/1448 已使用）；Task 1 的 `build(..., table_aliases=...)`
- Produces: 传给 build 的 `table_aliases = {物理表名: 别名}`（table_mapping 反转）

- [ ] **Step 1: 写失败测试——反转映射正确**

反转逻辑很小，抽为模块级纯函数便于测试。在 `tests/spark/test_snapshot.py` 新增：

```python
def test_reverse_table_mapping_to_aliases():
    """{别名: 物理} 反转为 {物理: 别名}。"""
    from tianshu_datadev.api.pipeline import _aliases_from_table_mapping
    assert _aliases_from_table_mapping({"ft": "fact_trips_sample", "tz": "dim_taxi_zone"}) == {
        "fact_trips_sample": "ft",
        "dim_taxi_zone": "tz",
    }
    assert _aliases_from_table_mapping(None) == {}
    assert _aliases_from_table_mapping({}) == {}
```

- [ ] **Step 2: 运行确认失败**

Run: `python -m pytest tests/spark/test_snapshot.py::test_reverse_table_mapping_to_aliases -v`
Expected: FAIL——`_aliases_from_table_mapping` 不存在（ImportError）。

- [ ] **Step 3: 实现——新增反转辅助 + 两处调用点传参**

在 `pipeline.py` 靠近 `_auto_table_mapping`（94 行）处新增：

```python
def _aliases_from_table_mapping(table_mapping: dict[str, str] | None) -> dict[str, str]:
    """把 {别名: 物理表名} 反转为 {物理表名: 别名}——供 SnapshotBuilder.build 的
    table_aliases 使用，让快照 source_name（inputs key）与 PySpark 代码的别名对齐。

    多别名映射到同一物理表时后者覆盖前者（正常 1:1，不预期冲突）。
    """
    if not table_mapping:
        return {}
    return {physical: alias for alias, physical in table_mapping.items()}
```

第一处调用点（约 1305 行），在 `build(...)` 增加 `table_aliases`：

```python
                        if source_tables:
                            snapshot_manifest = self._snapshot_builder.build(
                                contract_hash=contract_hash,
                                source_tables=source_tables,
                                provider=self._snapshot_provider,
                                table_aliases=_aliases_from_table_mapping(table_mapping),
                            )
```

第二处调用点（约 1576 行）同样：

```python
                    if source_tables:
                        snapshot_manifest = self._snapshot_builder.build(
                            contract_hash=contract_hash,
                            source_tables=source_tables,
                            provider=self._snapshot_provider,
                            table_aliases=_aliases_from_table_mapping(table_mapping),
                        )
```

- [ ] **Step 4: 运行确认通过**

Run: `python -m pytest tests/spark/test_snapshot.py::test_reverse_table_mapping_to_aliases -v`
Expected: PASS

- [ ] **Step 5: 回归 + Commit**

Run: `python -m pytest tests/spark/ tests/api/ -q`
Expected: 全绿（或既有 skip）。

```bash
git add src/tianshu_datadev/api/pipeline.py tests/spark/test_snapshot.py
git commit -m "fix(pipeline): build 传入 table_aliases——快照 source_name 对齐别名"
```

---

## Task 4：E2E 回归——inputs[别名] 全链路不再 KeyError

**Files:**
- Test: `tests/spark/test_physical_verifier.py`（新增 E2E 回归，有 pyspark 守卫）

**Interfaces:**
- Consumes: Task 1-3 的完整链路（SnapshotBuilder 写别名 source_name + 索引 → executor 按别名装载）

- [ ] **Step 1: 写 E2E 回归测试**

参照 `test_physical_verifier.py` 现有 `temp_parquet_dir` fixture 与真实 Spark 测试守卫，新增：模拟快照目录含物理名 parquet + `_inputs_index.json`（别名 ft），PySpark 代码用 `inputs["ft"]`，走 `_execute_spark`（或 `LocalSparkExecutor.execute`）断言 SUCCESS。

```python
def test_spark_inputs_alias_resolves_end_to_end(tmp_path):
    """回归：快照物理名 + 索引别名 ft，PySpark inputs['ft'] 全链路解析，无 KeyError。"""
    import pytest
    pytest.importorskip("pyspark")
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tianshu_datadev.spark.executor import LocalSparkExecutor

    snap = tmp_path / "snap"
    snap.mkdir()
    pq.write_table(pa.table({"amount": [10, 20]}), str(snap / "fact_trips_sample.parquet"))
    (snap / "_inputs_index.json").write_text(
        json.dumps({"ft": "fact_trips_sample.parquet"}), encoding="utf-8"
    )

    code = "result_df = inputs['ft'].groupBy().sum('amount')"
    result = LocalSparkExecutor().execute(code, data_dir=str(snap))
    assert result.status.name == "SUCCESS", result.error_message
```

- [ ] **Step 2: 运行确认通过**

Run: `python -m pytest tests/spark/test_physical_verifier.py::test_spark_inputs_alias_resolves_end_to_end -v`
Expected: PASS（无 pyspark 则 SKIP）

- [ ] **Step 3: 全量回归 + Commit**

Run: `python -m pytest tests/spark/ -q`
Expected: 全绿（或既有 skip）。

```bash
git add tests/spark/test_physical_verifier.py
git commit -m "test(verifier): E2E 回归——inputs[别名] 全链路解析不再 KeyError"
```

---

## 验证（全局验收）

```bash
# 全量回归
python -m pytest tests/spark/ tests/api/ -q

# ruff 零新增告警（改动文件）
python -m ruff check \
  src/tianshu_datadev/spark/snapshot.py \
  src/tianshu_datadev/spark/executor.py \
  src/tianshu_datadev/api/pipeline.py \
  tests/spark/test_snapshot.py \
  tests/spark/test_spark_executor.py \
  tests/spark/test_physical_verifier.py

# DuckDB 侧未受影响——确认物理名文件仍在，视图注册依赖不变
grep -n "view_name = filename.replace" src/tianshu_datadev/spark/physical_verifier.py

git diff --check
```

**验收要点：**
1. `SnapshotFile.source_name` == 别名（有 table_aliases 时），磁盘文件名仍为物理名。
2. `_inputs_index.json` 正确写入 `{别名: 物理文件名}`。
3. executor prologue 有索引走别名装载、无索引回退 glob。
4. `inputs["ft"]` 全链路解析不再 KeyError（有 pyspark 环境验证通过，否则 SKIP）。
5. DuckDB `_register_parquet_views` 零改动，物理名视图注册路径不受影响。
6. `snapshot_id` 生成逻辑未变（确定性保留）。
