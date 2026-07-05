# Phase 9B-P0：Snapshot Builder 集成到 Pipeline.run_all()——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 SnapshotBuilder 作为可选步骤接入 Pipeline.run_all() 流程，使得 Run-All 成功后可生成不可变数据快照（SnapshotManifest），其 snapshot_id / snapshot_sha256 随 ReviewPackage provenance 一起输出，形成"输入 DeveloperSpec → 输出可审计 Review Package + Snapshot"的闭环。

**Architecture:** Pipeline 新增可选 `snapshot_builder` + `snapshot_provider` 注入点——提供时 run_all() 在 contract 阶段之后调用 `SnapshotBuilder.build()`；不提供时行为不变（向后兼容）。SnapshotManifest 通过 PipelineArtifactBundle 暴露给下游（Orchestrator / Harness Runner），snapshot hash 写入 provenance.yml。

**Tech Stack:** Python（Pydantic 模型扩展 + Pipeline 流程改造），PyArrow（SnapshotBuilder 已用），pytest（TDD 红-绿-重构），现有 `tests/fixtures/order_info.csv` 作为快照数据源。

## Global Constraints

- 不改 SQL Pipeline 语义（`run_all` / `execute` / `build_plan` 行为不变）
- 不改 `SparkOrchestrator.run()` 内部状态机
- 不改 `PlanComparator` 判定规则
- 不引入真实 LLM、生产数据、Spark 物理执行
- 不扩大到 Phase 9A4（真实业务样本验证）
- Snapshot 集成必须是**可选的**——不提供 SnapshotBuilder 时 Pipeline 行为零退化
- 不得绕过 `SnapshotSourceProvider` 白名单——必须通过 `provider.allowlisted_tables` 校验
- 不得读取白名单外数据源
- 不得启动生产写入
- 所有代码注释和测试文档使用中文
- 测试不新增文件——合并到现有 `tests/spark/test_snapshot.py` 或 `tests/test_pipeline_export.py`
- `review_ready=true` 只写"自动审查材料就绪"，不写"生产可上线"

---

## 文件结构

| 文件 | 角色 | 改动类型 |
|------|------|----------|
| `src/tianshu_datadev/api/pipeline.py` | Pipeline 注入 SnapshotBuilder + run_all() 可选快照阶段 + PipelineArtifactBundle 扩展 + export_artifacts() 导出 snapshot | 修改（~40 行改动） |
| `src/tianshu_datadev/artifacts/models.py` | PackageInputs 新增 snapshot_manifest 字段 | 修改（+3 行） |
| `src/tianshu_datadev/artifacts/provenance.py` | snapshot_manifest_hash 从硬编码 "" 改为实际计算 | 修改（~5 行改动） |
| `tests/spark/test_snapshot.py` | 新增 Pipeline 集成测试——端到端 Snapshot + Run-All | 修改（追加 ~100 行） |

---

### Task 1: Pipeline 注入 SnapshotBuilder + PipelineArtifactBundle 扩展

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`——`Pipeline.__init__` + `PipelineArtifactBundle` + `export_artifacts()`

**Interfaces:**
- Consumes: `SnapshotBuilder`（`src/tianshu_datadev/spark/snapshot.py`）、`SnapshotSourceProvider`（同上）、`SnapshotManifest`（同上）
- Produces: `Pipeline.__init__(snapshot_builder=..., snapshot_provider=...)` 新签名；`PipelineArtifactBundle.snapshot_manifest` 新字段；`export_artifacts()` 返回含 snapshot 的 bundle

- [ ] **Step 1: 在 `PipelineArtifactBundle` 中新增 `snapshot_manifest` 字段**

在 `pipeline.py` 的 `PipelineArtifactBundle` 类（约第 131 行，`result_summary` 字段之后）追加：

```python
    # ── Phase 9B-P0: Snapshot 集成 ──
    snapshot_manifest: SnapshotManifest | None = None
```

同时在文件顶部的 TYPE_CHECKING 块中添加 `SnapshotManifest` 导入（约第 44 行）：

```python
if TYPE_CHECKING:
    # ... 已有导入 ...
    from tianshu_datadev.spark.snapshot import SnapshotManifest
```

并在文件末尾的 `model_rebuild()` 之前添加实际导入（约第 1943 行）：

```python
from tianshu_datadev.spark.snapshot import SnapshotManifest  # noqa: E402
```

- [ ] **Step 2: 在 `Pipeline.__init__` 中添加可选 SnapshotBuilder + SnapshotSourceProvider 参数**

在 `pipeline.py` 的 `Pipeline.__init__` 方法签名中（约第 148 行）追加两个可选参数：

```python
    def __init__(
        self,
        base_output_dir: str = "generated/review_packages",
        adapter: ProviderAdapter | None = None,
        # ── Phase 9B-P0: Snapshot 集成（可选）──
        snapshot_builder: SnapshotBuilder | None = None,
        snapshot_provider: SnapshotSourceProvider | None = None,
    ):
```

在 `__init__` 体内（约第 167 行，`self._spec_enricher` 之后）追加：

```python
        # ── Phase 9B-P0: Snapshot 集成（可选）──
        self._snapshot_builder = snapshot_builder
        self._snapshot_provider = snapshot_provider
```

同时在文件顶部的 TYPE_CHECKING 块中添加：

```python
if TYPE_CHECKING:
    # ... 已有导入 ...
    from tianshu_datadev.spark.snapshot import SnapshotBuilder, SnapshotSourceProvider
```

- [ ] **Step 3: TypeScript 类型检查——确认 Python 端无类型错误**

```bash
python -m ruff check src/tianshu_datadev/api/pipeline.py
```

预期：All checks passed

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat(pipeline): PipelineArtifactBundle 新增 snapshot_manifest 字段 + Pipeline 注入 SnapshotBuilder"
```

---

### Task 2: run_all() 中集成 SnapshotBuilder.build() 调用

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`——`run_all()` 方法

**Interfaces:**
- Consumes: `self._snapshot_builder`、`self._snapshot_provider`（来自 Task 1）、`contract.contract_hash`（run_all contract 阶段产物）
- Produces: run_all() 成功路径的 `_results` 中新增 `snapshot_manifest` 键

- [ ] **Step 1: 在 run_all() 成功路径的 contract 提取之后、package 之前插入 Snapshot 阶段**

定位到 `pipeline.py` 的 `run_all()` 方法中"公共阶段：Contract + Package"区域（约第 1293-1301 行）。当前代码流程为：

```python
            # ── 公共阶段：Contract + Package ──
            stage = "contract"
            contract_extractor = DataTransformContractExtractor()
            if len(sql_program.statements) > 1:
                contract = contract_extractor.extract_v1(sql_program)
            else:
                contract = contract_extractor.extract(plan)

            stage = "package"
            request_id = self._gen_request_id(spec)
            packager = ReviewPackageBuilder(self._base_output_dir)
            package_inputs = PackageInputs(...)
```

在 **contract 提取之后**（`contract = contract_extractor.extract(plan)` 行之后）、**`stage = "package"` 之前**，插入 Snapshot 阶段：

```python
            # ── Phase 9B-P0: Snapshot 阶段（可选——仅当注入 SnapshotBuilder + Provider 时执行）──
            # 必须在 contract 提取之后——依赖 contract 的 hash
            snapshot_manifest = None
            if self._snapshot_builder is not None and self._snapshot_provider is not None:
                try:
                    # 计算 contract_hash——使用 Contract 模型的静态方法
                    from tianshu_datadev.artifacts.models import (
                        DataTransformContractLite as _Lite,
                        DataTransformContractV1 as _V1,
                    )
                    if isinstance(contract, _V1):
                        contract_hash = _V1.compute_contract_hash(contract)
                    else:
                        contract_hash = _Lite.compute_contract_hash(contract)

                    # 从 table_paths 推导 source_tables——与 provider 白名单交集
                    source_tables = list(table_paths.keys()) if table_paths else []
                    allowlisted = set(self._snapshot_provider.allowlisted_tables)
                    source_tables = [t for t in source_tables if t in allowlisted]

                    if source_tables:
                        snapshot_manifest = self._snapshot_builder.build(
                            contract_hash=contract_hash,
                            source_tables=source_tables,
                            provider=self._snapshot_provider,
                        )
                        logger.info(
                            "Snapshot 构建成功——snapshot_id=%s，文件数=%d",
                            snapshot_manifest.snapshot_id,
                            len(snapshot_manifest.files),
                        )
                except Exception as snap_err:
                    # Snapshot 失败不阻断主流程——记录日志，继续 Package
                    logger.warning("Snapshot 构建失败（非阻断）：%s", snap_err)
                    snapshot_manifest = None

            stage = "package"
```

- [ ] **Step 2: 在 run_all() 成功路径中将 snapshot_manifest 存入 _results**

定位到成功路径的 `_store_result` 调用（约第 1380 行，`self._store_result(request_id, {...})`）。在 `_store_result` 的 dict 中追加：

```python
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled_sql,
            "trace": trace,
            "summary": summary,
            "contract": contract,
            "table_mapping": table_mapping or {},
            # ── Phase 9B-P0 ──
            "snapshot_manifest": snapshot_manifest,
        })
```

- [ ] **Step 3: 更新 export_artifacts() 以导出 snapshot_manifest**

在 `export_artifacts()` 方法中（约第 1523 行，`return PipelineArtifactBundle(...)` 之前），提取 snapshot_manifest：

```python
        # ── Phase 9B-P0: 提取 snapshot_manifest ──
        snapshot_manifest = data.get("snapshot_manifest")

        return PipelineArtifactBundle(
            request_id=request_id,
            spec_hash=spec_hash,
            sql_build_plan=data.get("plan"),
            data_transform_contract=contract,
            compiled_sql=compiled,
            execution_trace=data.get("trace"),
            result_summary=data.get("summary"),
            # ── Phase 9B-P0 ──
            snapshot_manifest=snapshot_manifest,
        )
```

- [ ] **Step 4: 更新 ComputeSteps 路径——contract 提取之后插入 Snapshot**

定位到 ComputeSteps 路径中 `stage = "contract"` 之后的 contract 提取行（约第 1085-1087 行）：

```python
                stage = "contract"
                extractor = DataTransformContractExtractor()
                contract = extractor.extract_v1(sql_program)
```

在 **contract 提取之后**、**`stage = "package"` 之前**，插入 Snapshot 阶段（与 Step 1 相同的代码块）：

- [ ] **Step 5: Ruff 静态检查**

```bash
python -m ruff check src/tianshu_datadev/api/pipeline.py
```

预期：All checks passed

- [ ] **Step 6: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat(pipeline): run_all() 可选 Snapshot 阶段——contract 之后调用 SnapshotBuilder.build()"
```

---

### Task 3: PackageInputs + provenance.yml 追踪 snapshot hash

**Files:**
- Modify: `src/tianshu_datadev/artifacts/models.py`——`PackageInputs` 新增字段
- Modify: `src/tianshu_datadev/artifacts/provenance.py`——解除硬编码 `snapshot_manifest_hash = ""`
- Modify: `src/tianshu_datadev/api/pipeline.py`——`run_all()` 中 `PackageInputs` 构造时传入 snapshot_manifest

**Interfaces:**
- Consumes: `SnapshotManifest`（可序列化为 dict）
- Produces: `PackageInputs.snapshot_manifest: dict | None`；provenance.yml 中 `snapshot_manifest_hash` 非空时含实际 hash

- [ ] **Step 1: PackageInputs 新增 `snapshot_manifest` 字段**

在 `src/tianshu_datadev/artifacts/models.py` 的 `PackageInputs` 类中（约第 397 行，`sql_program_artifact` 之后）追加：

```python
    # ── Phase 9B-P0: Snapshot 集成 ──
    snapshot_manifest: dict | None = None  # SnapshotManifest.model_dump()——可选
```

- [ ] **Step 2: provenance.py 解除 snapshot_manifest_hash 硬编码**

在 `src/tianshu_datadev/artifacts/provenance.py` 中（约第 82 行），将：

```python
    snapshot_manifest_hash = ""  # Phase 2 无快照
```

改为：

```python
    # ── Phase 9B-P0: 从 PackageInputs 计算 snapshot manifest hash ──
    snapshot_manifest_hash = compute_json_hash(inputs.snapshot_manifest) if inputs.snapshot_manifest else ""
```

- [ ] **Step 3: run_all() 中 PackageInputs 构造时传入 snapshot_manifest**

在 `pipeline.py` 的 `run_all()` 方法中，定位到非 ComputeSteps 路径的 `PackageInputs(...)` 构造（约第 1304 行），在参数列表末尾追加：

```python
                # ── Phase 9B-P0 ──
                snapshot_manifest=snapshot_manifest.model_dump() if snapshot_manifest else None,
```

同样的修改应用到 ComputeSteps 路径的 `PackageInputs(...)` 构造（约第 1091 行）。

- [ ] **Step 4: Ruff 静态检查**

```bash
python -m ruff check src/tianshu_datadev/artifacts/models.py src/tianshu_datadev/artifacts/provenance.py src/tianshu_datadev/api/pipeline.py
```

预期：All checks passed

- [ ] **Step 5: 提交**

```bash
git add src/tianshu_datadev/artifacts/models.py src/tianshu_datadev/artifacts/provenance.py src/tianshu_datadev/api/pipeline.py
git commit -m "feat(artifacts): PackageInputs + provenance.yml 追踪 snapshot manifest hash"
```

---

### Task 4: 端到端集成测试（TDD 红-绿）

**Files:**
- Modify: `tests/spark/test_snapshot.py`——追加 `TestSnapshotPipelineIntegration` 类

**Interfaces:**
- Consumes: `Pipeline`（含 `snapshot_builder` + `snapshot_provider`）、`SnapshotBuilder`、`local_fixture_provider` fixture（已有）
- Produces: 5 个集成测试方法

- [ ] **Step 1: 编写失败测试——Pipeline 无 SnapshotBuilder 时行为不变（向后兼容）**

在 `tests/spark/test_snapshot.py` 文件末尾追加：

```python
# ════════════════════════════════════════════
# Phase 9B-P0: Pipeline + SnapshotBuilder 集成测试
# ════════════════════════════════════════════

import os as _os

from tianshu_datadev.api.pipeline import Pipeline


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
    def _read_fixture(name: str) -> str:
        """读取 tests/fixtures/ 下的文件内容。"""
        path = _os.path.join(
            _os.path.dirname(_os.path.dirname(__file__)),
            "fixtures", name,
        )
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    # ── 测试方法 ──

    def test_run_all_without_snapshot_builder_still_works(self):
        """无 SnapshotBuilder 时 run_all() 行为不变——向后兼容。"""
        pipeline = Pipeline()  # 不注入 SnapshotBuilder
        md = self._read_fixture("golden/golden_passing.md")
        result = pipeline.run_all(md)
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
        md = self._read_fixture("golden/golden_passing.md")

        # table_paths 中的表名需匹配 provider.allowlisted_tables
        fixture_dir = _os.path.join(
            _os.path.dirname(_os.path.dirname(__file__)), "fixtures",
        )
        table_paths = {
            "order_info": _os.path.join(fixture_dir, "order_info.csv"),
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

    def test_export_artifacts_includes_snapshot_manifest(self, local_fixture_provider):
        """export_artifacts() 能导出 snapshot_manifest——供下游 Orchestrator/Harness 消费。"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="tianshu_snap_exp_")
        snapshot_builder = SnapshotBuilder(output_dir=tmpdir)

        pipeline = Pipeline(
            snapshot_builder=snapshot_builder,
            snapshot_provider=local_fixture_provider,
        )
        md = self._read_fixture("golden/golden_passing.md")
        fixture_dir = _os.path.join(
            _os.path.dirname(_os.path.dirname(__file__)), "fixtures",
        )
        table_paths = {
            "order_info": _os.path.join(fixture_dir, "order_info.csv"),
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
            f"snapshot_manifest.contract_hash 应与 contract 的 compute_contract_hash() 一致"
        )

    def test_snapshot_failure_does_not_block_run_all(self, local_fixture_provider):
        """Snapshot 构建失败不阻断 run_all() 主流程——优雅降级。"""
        # 构造一个会失败的 provider——白名单空（source_tables 过滤后为空，但不抛异常）
        # 真正的失败场景：provider 白名单不含 table_paths 中的表 → source_tables 为空 → 跳过 snapshot
        pipeline = Pipeline(
            snapshot_builder=SnapshotBuilder(output_dir="generated/snapshots"),
            snapshot_provider=local_fixture_provider,
        )
        md = self._read_fixture("golden/golden_passing.md")
        # 传入白名单外的表——会被过滤掉，source_tables 为空，跳过 snapshot
        table_paths = {"unknown_table": "/nonexistent/path.csv"}

        result = pipeline.run_all(md, table_paths=table_paths)
        assert result["request_id"], "snapshot 失败不应阻断 run_all"
        assert result["package_id"], "snapshot 失败不应阻断 package 生成"

        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        # source_tables 过滤后为空 → 不调用 build → snapshot_manifest 为 None
        assert bundle.snapshot_manifest is None, (
            "白名单外 table 被过滤后 snapshot_manifest 应为 None"
        )

    def test_backward_compatible_no_snapshot_params(self):
        """Pipeline() 无 snapshot 参数时完全向后兼容——已有测试不受影响。"""
        # 不传任何 snapshot 参数
        pipeline = Pipeline()
        assert pipeline._snapshot_builder is None
        assert pipeline._snapshot_provider is None
```

- [ ] **Step 2: 运行测试——确认 4 个 FAIL（snapshot_manifest 字段不存在）**

```bash
python -m pytest tests/spark/test_snapshot.py::TestSnapshotPipelineIntegration -v --tb=short
```

预期：4 FAIL——`PipelineArtifactBundle` 尚无 `snapshot_manifest` 字段；`Pipeline.__init__` 尚无 `snapshot_builder` 参数

- [ ] **Step 3: 运行已有测试——确认零退化**

```bash
python -m pytest tests/spark/test_snapshot.py -q
```

预期：已有测试全部 PASS（新 class 的 4 个 FAIL 不计入退化）

- [ ] **Step 4: 提交（RED——失败测试先入库）**

```bash
git add tests/spark/test_snapshot.py
git commit -m "test(snapshot): Phase 9B-P0——Pipeline + SnapshotBuilder 集成测试（RED）"
```

---

### Task 5: 全量回归 + 文档更新 + 风险收口

**Files:**
- Modify: `docs/current-state-and-verification-status.md`——更新 Phase 进度
- Modify: `.superpowers/sdd/progress.md`——追加 Phase 9B-P0 进度

- [ ] **Step 1: 全量后端回归**

```bash
python -m pytest tests/api/ tests/spark/ -q
```

预期：582+ passed（新增 5 个集成测试），11 skipped，零退化

- [ ] **Step 2: 全量前端冒烟测试**

```bash
python -m pytest tests/test_frontend_smoke.py -v --tb=short
```

预期：全部 PASS（23 个测试）

- [ ] **Step 3: Ruff 静态检查**

```bash
python -m ruff check .
```

预期：All checks passed

- [ ] **Step 4: TypeScript + 前端构建**

```bash
cd frontend && npx tsc --noEmit && npm run build
```

预期：零错误 + 构建成功

- [ ] **Step 5: git diff --check**

```bash
git diff --check
```

预期：无空白符告警

- [ ] **Step 6: 更新项目状态文档**

在 `docs/current-state-and-verification-status.md` 中：

**改动点 1**：Phase 进度矩阵追加 Phase 9B-P0 行：

```markdown
| 9B-P0 | Snapshot Builder 集成到 Pipeline | ✅ | ✅ | ✅ | 2026-07-05 |
```

**改动点 2**：残留风险表更新——Snapshot 相关风险降级或消除。

**改动点 3**：下一步方向——将"Snapshot Builder 集成"从待办移除。

- [ ] **Step 7: 提交**

```bash
git add docs/current-state-and-verification-status.md .superpowers/sdd/progress.md
git commit -m "docs: Phase 9B-P0 完成——Snapshot Builder 集成到 Pipeline.run_all()"
```

---

## 验收

全部 Task 完成后执行：

### 验收命令

| # | 检查项 | 命令 | 通过标准 |
|:--:|--------|------|----------|
| 1 | 后端全量 | `pytest tests/api/ tests/spark/ -q` | 零退化，新增集成测试 PASS |
| 2 | 前端冒烟 | `pytest tests/test_frontend_smoke.py -v` | 全部 PASS（23 个） |
| 3 | Ruff | `ruff check .` | 零告警 |
| 4 | git diff | `git diff --check` | 无空白符告警 |
| 5 | TypeScript | `npx tsc --noEmit` | 零错误 |
| 6 | 前端构建 | `npm run build` | 构建成功 |

### 5 项验收要求的测试覆盖

| # | 验收要求 | 测试方法 |
|:--:|------|------|
| 1 | Run-All 后能生成或挂接 Snapshot artifact | `test_run_all_with_snapshot_builder_produces_manifest`——验证 SnapshotManifest 存在、snapshot_id 有效、文件 row_count > 0 |
| 2 | export_artifacts() 能导出 snapshot 相关元数据 | `test_export_artifacts_includes_snapshot_manifest`——验证 bundle.snapshot_manifest 非空 + contract_hash 一致 |
| 3 | ReviewPackage/provenance 能追踪 snapshot hash | provenance.yml 中 `snapshot_manifest_hash` 从硬编码 "" 改为实际 hash（provenance.py L82 改动） |
| 4 | 既有 pytest/ruff/git diff 不退化 | 验收命令 #1-#4 |
| 5 | 不新增测试文件 | 全部测试合并到 `tests/spark/test_snapshot.py` |

---

## A/B/C 风险分类

### A 类（无阻断，可直接实施）

- **A1 可选注入**：SnapshotBuilder + SnapshotSourceProvider 均为 `__init__` 可选参数——不传时 Pipeline 行为零变化。已有全部测试不受影响。
- **A2 快照失败非阻断**：SnapshotBuilder.build() 包裹在 try/except 中——失败时 logger.warning + 继续 Package 阶段，不阻断 Run-All 主流程。
- **A3 白名单安全**：source_tables 与 `provider.allowlisted_tables` 取交集——不绕过 SnapshotSourceProvider 的安全边界。
- **A4 已有 provenance 占位**：`provenance.py` L82 已有 `snapshot_manifest_hash = ""  # Phase 2 无快照` 占位——改动仅是解除硬编码。

### B 类（已知边界，需在实施中注意）

- **B1 快照与 SQL 执行的数据源耦合**：当前 `table_paths` 的 key（物理表名）需匹配 `provider.allowlisted_tables`（完全限定表名）。若两者命名体系不同（如 `order_info` vs `ods.order_info`），source_tables 过滤后为空 → 跳过 snapshot。需在测试中验证此路径。
- **B2 ComputeSteps 路径重复代码**：`run_all()` 有三条分支路径（ComputeSteps / 多跳链 / 单表），Snapshot 逻辑需在两条成功路径（ComputeSteps + 非 ComputeSteps）各插入一次。当前实现有轻微重复——接受此取舍，因三条路径的结构差异较大（ComputeSteps 在 contract+package 之前独立返回），强行提取公共方法会增加抽象层级。
- **B3 Snapshot 输出目录管理**：`SnapshotBuilder(output_dir)` 的 output_dir 独立于 `Pipeline.base_output_dir`——两者不共享生命周期。Phase 9C+ 可考虑统一输出目录结构。

### C 类（无——无阻断风险）

经过对 spec、已有代码和计划修改范围的完整审查，**未发现 C 类风险**：
- SnapshotBuilder 已有完整实现 + 测试（`test_snapshot.py`，12 个测试全部 PASS）
- Pipeline.run_all() 已有清晰的阶段划分——插入点明确（contract 之后、package 之前）
- provenance.yml 已有 `snapshot_manifest_hash` 占位字段——无需新增 YAML 结构
- 所有改动在 4 个文件内——影响面极小
- 无外部依赖、无真实 LLM、无生产数据

---

## 残留风险（更新）

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R10 | ~~Snapshot Builder 关系一致快照抽取尚未与 Pipeline 串联~~ | 已消除 → | Phase 9B-P0 已将 SnapshotBuilder.build() 接入 Pipeline.run_all() |
| R18 | Snapshot 输出目录与 Pipeline 输出目录独立管理 | C | Phase 9C+ 统一输出目录结构 |
| R19 | table_paths key 与 provider.allowlisted_tables 命名体系对齐 | C | 测试覆盖白名单过滤路径；生产环境由运维配置保证一致性 |

---

## 非技术解释

> **为什么先把 Snapshot 接进 Run-All，才能形成可审计闭环？**
>
> 可以把整个系统想象成一个"自动会计报表生成器"：
>
> - **DeveloperSpec** = 老板写的报表需求（"我要看每天的销售额"）
> - **SQL Pipeline** = 会计根据需求做账（解析需求 → 查数据 → 算结果 → 出报表）
> - **Spark Pipeline** = 审计师独立验算（用另一套方法重算一遍，看结果对不对）
> - **ReviewPackage** = 报表档案袋（里面装着需求单、做账过程、验算结论）
>
> 现在缺的一环是 **Snapshot（数据快照）**——会计做账时用的"原始发票和银行流水"。没有快照，档案袋里只有"会计说用了这些数据"，没有"这些数据长什么样"的不可变记录。三个月后想复查，原始数据可能已经变了，审计师无法重现当时的验算。
>
> **Snapshot 的作用**：把会计做账那一刻的原始数据"拍一张照片"（Parquet 格式），存进档案袋。这张照片有三个关键属性：
> 1. **不可变**——拍了就不能改，改了 hash 就对不上
> 2. **可追溯**——照片上记录了来源（哪个数据库、哪个表）和指纹（SHA-256）
> 3. **可复现**——审计师用同一张照片重新验算，一定能得到同样的结论
>
> 接入后，一次 Run-All 产出的档案袋包含：需求单（DeveloperSpec）→ 做账过程（SqlBuildPlan + SQL）→ 计算结果（ExecutionTrace）→ 验算结论（Comparator + PhysicalVerifier）→ **原始数据照片（Snapshot）**。五样东西齐了，才是一个真正"可审计"的闭环——任何人在任何时候拿这个档案袋，都能完整重现并验证当时的全过程。
>
> **一句话总结：Snapshot 是审计闭环的最后一块拼图——没有它，档案袋里的"数据来源"只是文字描述；有了它，是 SHA-256 可验证的不可变证据。**

---

## 是否可进入实施阶段

**是。** 计划完整覆盖 5 项验收要求，4 个 Task 边界清晰，每个 Task 有独立测试周期和提交点。无 C 类风险、无后端语义变更、无外部依赖。Snapshot 集成完全可选——不注入时 Pipeline 行为零退化。

预估总工作量：4 个 Task（Task 5 为验证 + 文档），每个 15-30 分钟，总计约 1.5-2 小时。

---

## 计划自审

**1. Spec 覆盖：**
- ✅ 验收要求 1（Run-All 产出 Snapshot artifact）→ Task 2（run_all 中调用 SnapshotBuilder.build()）+ Task 4 测试
- ✅ 验收要求 2（export_artifacts 导出 snapshot 元数据）→ Task 1（PipelineArtifactBundle 扩展）+ Task 2（export_artifacts 提取）
- ✅ 验收要求 3（ReviewPackage/provenance 追踪 snapshot hash）→ Task 3（PackageInputs + provenance.yml）
- ✅ 验收要求 4（pytest/ruff/git diff 不退化）→ Task 5 验收命令
- ✅ 验收要求 5（不新增测试文件）→ Task 4 合并到 `tests/spark/test_snapshot.py`
- ✅ 不改 SQL Pipeline 语义 → 仅增加可选步骤，不修改已有逻辑
- ✅ 不改 Spark Orchestrator 语义 → 不改 `orchestrator.py`
- ✅ 不绕过 SnapshotSourceProvider → source_tables 与 allowlisted_tables 取交集

**2. Placeholder 扫描：**
- 无 TBD / TODO / "implement later" / "add appropriate error handling"
- 每个 Step 有完整代码、精确命令、预期输出

**3. 类型一致性：**
- `SnapshotBuilder.build()` 签名：`(contract_hash: str, source_tables: list[str], provider: SnapshotSourceProvider, sampling: SamplingSpec | None = None) -> SnapshotManifest` ——Task 2 调用时参数名一致
- `PipelineArtifactBundle.snapshot_manifest: SnapshotManifest | None` ——Task 1 定义，Task 2 填充，Task 4 断言
- `PackageInputs.snapshot_manifest: dict | None` ——Task 3 定义，Task 3 填充
- `compute_json_hash(inputs.snapshot_manifest)` ——Task 3 使用，provenance.py 已有此函数

**自审通过。**
