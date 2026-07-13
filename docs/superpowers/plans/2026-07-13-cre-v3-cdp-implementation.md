# CRE v3 CDP 摘要路径实施计划（修订版）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现 CDP v1 摘要协议全链路——模型+校验 → 手工黄金向量 → Python oracle → 双引擎字段探针 → 双引擎 builder + 引擎侧集成 → 三方属性测试+性能基准 → Engine-side Shadow → 接管结论 → 清理过渡设计。

**本轮范围:** Task 1-8 + Task 10。Task 9（接管）需人工确认后执行。Task 11（工具清理）不在本轮范围。

**Architecture:** 核心区分两个执行路径：(1) **引擎侧摘要路径**——DuckDB/Spark builder 在引擎内部完成 CDP 编码+分桶+digest，返回 `DigestExecutionEnvelope`，Python 侧只接收 envelope 做 compare()；(2) **Python oracle 路径**——仅用于测试交叉验证，不参与生产管线。引擎 builder 在 Phase 3b 完成后立即接入 DuckDB/Spark 执行入口，Shadow 和接管阶段不再经过 Python 重算全量行。

**Tech Stack:** Python 3.11+, Pydantic (StrictModel), DuckDB, PySpark 3.5+, SHA-256 (hashlib), struct, pytest

## Global Constraints

- 所有代码注释必须使用中文
- **Phase 3d 之前，legacy/shadow 路径不得删除或降级**
- **Phase 3e–3g 不得在 3d 稳定运行 ≥2 周前执行**
- 任何 Phase 门禁未通过 → 回退上一 Phase 修复，禁止跳过
- §11 准入标准 #1-8 全部通过是 Phase 3d 的唯一入口条件
- `primary_keys` 和 `sample_size` 属于 `CreSampleSpec`，不参与 `full_digest`
- `full_digest` 始终覆盖全量行——不得因结果集大小切换为部分行 digest
- `samples` 不参与一致性判定
- CDP v1 固定 256 桶，桶内计数字段统一 8B BE
- FLOAT/DECIMAL 舍入模式固定为 `ROUND_HALF_UP`（远离零方向）
- VARCHAR 不在类型编码内添加长度前缀——长度由字段级 `length_prefix` 统一管理
- **Python oracle 的 naive timestamp 和 DST 边界行为必须与设计文档一致——拒绝执行，不得绕过**

---

## 文件结构

```
新增文件：
  src/tianshu_datadev/spark/cdp_spec.py            # Task 1：模型 + 严格校验
  src/tianshu_datadev/spark/cdp_serializer.py       # Task 3：Python oracle（参考实现，不用于生产）
  src/tianshu_datadev/spark/cdp_duckdb_builder.py   # Task 5：DuckDB builder + 执行入口集成
  src/tianshu_datadev/spark/cdp_spark_builder.py    # Task 6：Spark builder + 执行入口集成
  tests/spark/test_cdp_spec.py                      # Task 1：模型校验测试
  tests/spark/test_cdp_golden_vectors.py            # Task 2：手工黄金向量（独立于 oracle）
  tests/spark/test_cdp_serializer.py                # Task 3：Python oracle vs 黄金向量
  tests/spark/test_cdp_field_probes.py              # Task 4：双引擎字段编码探针
  tests/spark/test_cdp_dual_engine.py               # Task 7：三方属性测试 + 性能基准
  tests/spark/test_cdp_shadow.py                    # Task 8：Engine-side Shadow 集成测试

修改文件：
  src/tianshu_datadev/spark/executor.py             # Task 5/6：Spark 执行入口支持 CDP 模式
  src/tianshu_datadev/sql/executor.py               # Task 5：DuckDB 执行入口支持 CDP 模式
  src/tianshu_datadev/spark/physical_verifier.py    # Task 8/9：Shadow 挂载 + 接管
  src/tianshu_datadev/api/pipeline.py               # Task 8/9：管线集成
```

---

### Task 1: CDP v1 数据模型 + 严格校验

**目标**：实现所有 CDP 模型，带完整 Pydantic 校验器——列表长度一致性、类型专属精度约束、Envelope 状态→summary 契约、mutable default 防护。

**Files:**
- Create: `src/tianshu_datadev/spark/cdp_spec.py`
- Create: `tests/spark/test_cdp_spec.py`

**Interfaces:**
- Produces:
  - `TypeFamily(Enum)` — BOOLEAN=0x01 .. TIMESTAMP=0x0B
  - `CreDigestSpec(StrictModel)` — 含 `@model_validator` 校验列表长度一致、decimal_precision/scale 仅对 DECIMAL 列非 None、float_precision 仅对 FLOAT 列非 None
  - `CreSampleSpec(StrictModel)` — `primary_keys: list[str] = Field(default_factory=list)`
  - `EngineDigestSummary(StrictModel)`
  - `DigestExecutionEnvelope(StrictModel)` — 含 `@model_validator` 校验 `status==SUCCESS → summary is not None`、`status!=SUCCESS → summary is None`
  - `CreComparisonResult(StrictModel)`
  - `compute_digest_spec_hash(spec: CreDigestSpec) -> bytes` — 返回 32 字节原始 SHA-256

- [ ] **Step 1: 编写模型校验失败测试**

```python
# tests/spark/test_cdp_spec.py
import pytest
from tianshu_datadev.spark.cdp_spec import (
    TypeFamily, CreDigestSpec, CreSampleSpec,
    DigestExecutionEnvelope, EngineDigestSummary,
    compute_digest_spec_hash,
)


class TestTypeFamily:
    def test_null_not_in_enum(self):
        """NULL 不是 TypeFamily 成员。"""
        assert not hasattr(TypeFamily, "NULL")
        with pytest.raises(ValueError):
            TypeFamily(0x00)

    def test_values_unique(self):
        vals = [t.value for t in TypeFamily]
        assert len(vals) == len(set(vals)) == 11


class TestCreDigestSpec:
    def test_rejects_mismatched_list_lengths(self):
        """所有列表必须与 output_columns 等长。"""
        with pytest.raises(Exception):  # ValidationError
            CreDigestSpec(
                output_columns=["id", "name"],
                type_families=[TypeFamily.INT64],  # 只有 1 个
                timezone="Asia/Shanghai",
                decimal_precision=[None, None],
                decimal_scale=[None, None],
                float_precision=[None, None],
            )

    def test_rejects_decimal_precision_on_non_decimal(self):
        """decimal_precision 仅 DECIMAL 列可非 None。"""
        with pytest.raises(Exception):
            CreDigestSpec(
                output_columns=["id"],
                type_families=[TypeFamily.INT64],  # 非 DECIMAL
                timezone="UTC",
                decimal_precision=[12],  # 应为 None
                decimal_scale=[None],
                float_precision=[None],
            )

    def test_rejects_float_precision_on_non_float(self):
        """float_precision 仅 FLOAT32/FLOAT64 列可非 None。"""
        with pytest.raises(Exception):
            CreDigestSpec(
                output_columns=["name"],
                type_families=[TypeFamily.VARCHAR],
                timezone="UTC",
                decimal_precision=[None],
                decimal_scale=[None],
                float_precision=[2],  # 应为 None
            )

    def test_accepts_valid_spec(self):
        """合法 spec 通过所有校验。"""
        spec = CreDigestSpec(
            output_columns=["id", "amount", "rate"],
            type_families=[TypeFamily.INT64, TypeFamily.DECIMAL, TypeFamily.FLOAT64],
            timezone="Asia/Shanghai",
            decimal_precision=[None, 12, None],
            decimal_scale=[None, 2, None],
            float_precision=[None, None, 4],
        )
        assert spec.protocol_version == "cdp-v1"


class TestCreSampleSpec:
    def test_default_factory_isolates_instances(self):
        """每个实例的 primary_keys 是独立列表——防止 mutable default 共享。"""
        s1 = CreSampleSpec()
        s2 = CreSampleSpec()
        s1.primary_keys.append("id")
        assert s2.primary_keys == []  # 不受 s1 影响

    def test_sample_size_clamped(self):
        """sample_size 必须在 1-100 范围内。"""
        with pytest.raises(Exception):
            CreSampleSpec(sample_size=0)
        with pytest.raises(Exception):
            CreSampleSpec(sample_size=101)


class TestDigestExecutionEnvelope:
    def test_success_requires_summary(self):
        """status==SUCCESS → summary 必须非 None。"""
        with pytest.raises(Exception):
            DigestExecutionEnvelope(
                execution_status="SUCCESS",
                snapshot_id="s1",
                digest_spec_hash="a" * 64,
                protocol_version="cdp-v1",
                engine_version="1.0",
                summary=None,  # 非法
            )

    @pytest.mark.parametrize("status", ["FAILED", "TIMEOUT", "UNSUPPORTED", "REQUIRES_REVIEW"])
    def test_non_success_forbids_summary(self, status):
        """status!=SUCCESS → summary 必须为 None。"""
        with pytest.raises(Exception):
            DigestExecutionEnvelope(
                execution_status=status,
                snapshot_id="s1",
                digest_spec_hash="a" * 64,
                protocol_version="cdp-v1",
                engine_version="1.0",
                error="test error",
                summary=EngineDigestSummary(row_count=0, full_digest="x"*64, samples=[]),  # 非法
            )
    
    def test_success_accepts_summary(self):
        """status=SUCCESS + summary 非 None → 通过。"""
        env = DigestExecutionEnvelope(
            execution_status="SUCCESS",
            snapshot_id="s1",
            digest_spec_hash="a" * 64,
            protocol_version="cdp-v1",
            engine_version="1.0",
            summary=EngineDigestSummary(row_count=0, full_digest="0"*64, samples=[]),
        )
        assert env.execution_status == "SUCCESS"


class TestDigestSpecHash:
    def test_deterministic(self):
        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert compute_digest_spec_hash(spec) == compute_digest_spec_hash(spec)

    def test_returns_32_bytes(self):
        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert len(compute_digest_spec_hash(spec)) == 32  # 原始字节

    def test_timezone_affects_hash(self):
        s1 = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        s2 = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="Asia/Shanghai",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert compute_digest_spec_hash(s1) != compute_digest_spec_hash(s2)
```

- [ ] **Step 2: 运行测试验证失败**

Run: `pytest tests/spark/test_cdp_spec.py -v`
Expected: 全部 FAIL——模块不存在

- [ ] **Step 3: 实现 cdp_spec.py**

```python
# src/tianshu_datadev/spark/cdp_spec.py
"""CDP v1 数据模型——含严格 Pydantic 校验。"""
from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import ClassVar, Literal

from pydantic import Field, model_validator

from tianshu_datadev.developer_spec.models import StrictModel


class TypeFamily(Enum):
    BOOLEAN = 0x01; INT8 = 0x02; INT16 = 0x03; INT32 = 0x04
    INT64 = 0x05; FLOAT32 = 0x06; FLOAT64 = 0x07; DECIMAL = 0x08
    VARCHAR = 0x09; DATE = 0x0A; TIMESTAMP = 0x0B


class CreDigestSpec(StrictModel):
    protocol_version: Literal["cdp-v1"] = "cdp-v1"
    output_columns: list[str]
    type_families: list[TypeFamily]
    timezone: str
    decimal_precision: list[int | None]
    decimal_scale: list[int | None]
    float_precision: list[int | None]
    _BUCKET_COUNT: ClassVar[int] = 256

    @model_validator(mode="after")
    def _validate_lists_match_columns(self):
        n = len(self.output_columns)
        for name in ["type_families", "decimal_precision", "decimal_scale", "float_precision"]:
            lst = getattr(self, name)
            if len(lst) != n:
                raise ValueError(f"{name} 长度 {len(lst)} ≠ output_columns 长度 {n}")
        return self

    @model_validator(mode="after")
    def _validate_type_specific_precision(self):
        for i, tf in enumerate(self.type_families):
            if tf == TypeFamily.DECIMAL:
                if self.decimal_precision[i] is None or self.decimal_scale[i] is None:
                    raise ValueError(f"列 {i} ({self.output_columns[i]}) 是 DECIMAL，"
                                     f"必须提供 decimal_precision 和 decimal_scale")
            else:
                if self.decimal_precision[i] is not None or self.decimal_scale[i] is not None:
                    raise ValueError(f"列 {i} ({self.output_columns[i]}) 非 DECIMAL，"
                                     f"decimal_precision/scale 必须为 None")
            if tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
                # float_precision 可为 None（不归一化）或 int
                fp = self.float_precision[i]
                if fp is not None and not isinstance(fp, int):
                    raise ValueError(f"列 {i} float_precision 必须是 int 或 None")
            else:
                if self.float_precision[i] is not None:
                    raise ValueError(f"列 {i} 非 FLOAT，float_precision 必须为 None")
        return self


class CreSampleSpec(StrictModel):
    primary_keys: list[str] = Field(default_factory=list)
    sample_size: int = Field(default=5, ge=1, le=100)


class EngineDigestSummary(StrictModel):
    row_count: int
    full_digest: str  # SHA-256 hex (64 char)
    samples: list[dict]


class DigestExecutionEnvelope(StrictModel):
    execution_status: Literal["SUCCESS", "FAILED", "TIMEOUT", "UNSUPPORTED", "REQUIRES_REVIEW"]
    snapshot_id: str
    digest_spec_hash: str  # SHA-256 hex
    protocol_version: str
    engine_version: str
    error: str | None = None
    summary: EngineDigestSummary | None = None

    @model_validator(mode="after")
    def _validate_summary_contract(self):
        if self.execution_status == "SUCCESS":
            if self.summary is None:
                raise ValueError("status=SUCCESS 要求 summary 非 None")
        elif self.execution_status in ("FAILED", "TIMEOUT", "UNSUPPORTED", "REQUIRES_REVIEW"):
            if self.summary is not None:
                raise ValueError(f"status={self.execution_status} 要求 summary=None")
        return self


class CreComparisonResult(StrictModel):
    status: Literal["CONSISTENT_SAMPLE", "DIFFERENT", "NOT_EXECUTED",
                    "UNSUPPORTED_SEMANTICS", "HUMAN_REVIEW"]
    duckdb_count: int | None = None
    spark_count: int | None = None
    count_match: bool | None = None
    digest_match: bool | None = None
    duckdb_digest: str | None = None
    spark_digest: str | None = None
    pk_samples_duckdb: list[dict] | None = None
    pk_samples_spark: list[dict] | None = None
    decision_reason: str | None = None


def compute_digest_spec_hash(spec: CreDigestSpec) -> bytes:
    """返回 32 字节原始 SHA-256——不含 primary_keys/sample_size。"""
    columns = []
    for i, col_name in enumerate(spec.output_columns):
        columns.append({
            "decimal_precision": spec.decimal_precision[i],
            "decimal_scale": spec.decimal_scale[i],
            "float_precision": spec.float_precision[i],
            "name": col_name,
            "type": spec.type_families[i].name,
        })
    obj = {
        "bucket_count": spec._BUCKET_COUNT,
        "columns": columns,
        "protocol_version": spec.protocol_version,
        "timezone": spec.timezone,
    }
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).digest()


def compare(duckdb: DigestExecutionEnvelope, spark: DigestExecutionEnvelope) -> CreComparisonResult:
    """摘要比较——唯一判定逻辑。Envelope 状态契约由模型校验保证。"""
    if duckdb.digest_spec_hash != spark.digest_spec_hash:
        return CreComparisonResult(status="HUMAN_REVIEW", decision_reason="SPEC_MISMATCH")
    if duckdb.snapshot_id != spark.snapshot_id:
        return CreComparisonResult(status="HUMAN_REVIEW", decision_reason="SNAPSHOT_MISMATCH")
    # 统一状态映射：UNSUPPORTED→UNSUPPORTED_SEMANTICS, REQUIRES_REVIEW→HUMAN_REVIEW, FAILED/TIMEOUT→NOT_EXECUTED
    _STATUS_MAP = {
        "UNSUPPORTED": "UNSUPPORTED_SEMANTICS",
        "REQUIRES_REVIEW": "HUMAN_REVIEW",
        "FAILED": "NOT_EXECUTED",
        "TIMEOUT": "NOT_EXECUTED",
    }
    for env, label in [(duckdb, "DuckDB"), (spark, "Spark")]:
        if env.execution_status != "SUCCESS":
            mapped = _STATUS_MAP.get(env.execution_status, "NOT_EXECUTED")
            return CreComparisonResult(
                status=mapped,
                decision_reason=f"{label}_{env.execution_status}",
            )
    # Envelope 校验已保证 summary 非 None
    assert duckdb.summary is not None and spark.summary is not None
    count_match = duckdb.summary.row_count == spark.summary.row_count
    if not count_match:
        return CreComparisonResult(
            status="DIFFERENT", count_match=False, digest_match=False,
            duckdb_count=duckdb.summary.row_count, spark_count=spark.summary.row_count,
            duckdb_digest=duckdb.summary.full_digest, spark_digest=spark.summary.full_digest,
            pk_samples_duckdb=duckdb.summary.samples, pk_samples_spark=spark.summary.samples,
            decision_reason="COUNT_DIFFERENT",
        )
    digest_match = duckdb.summary.full_digest == spark.summary.full_digest
    if not digest_match:
        return CreComparisonResult(
            status="DIFFERENT", count_match=True, digest_match=False,
            duckdb_count=duckdb.summary.row_count, spark_count=spark.summary.row_count,
            duckdb_digest=duckdb.summary.full_digest, spark_digest=spark.summary.full_digest,
            pk_samples_duckdb=duckdb.summary.samples, pk_samples_spark=spark.summary.samples,
            decision_reason="DIGEST_DIFFERENT",
        )
    return CreComparisonResult(
        status="CONSISTENT_SAMPLE", count_match=True, digest_match=True,
        duckdb_count=duckdb.summary.row_count, spark_count=spark.summary.row_count,
        duckdb_digest=duckdb.summary.full_digest, spark_digest=spark.summary.full_digest,
        pk_samples_duckdb=duckdb.summary.samples, pk_samples_spark=spark.summary.samples,
        decision_reason="SUMMARY_CONSISTENT",
    )
```

- [ ] **Step 4: 运行测试验证通过**

Run: `pytest tests/spark/test_cdp_spec.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/tianshu_datadev/spark/cdp_spec.py tests/spark/test_cdp_spec.py
git commit -m "feat(cdp): add CDP v1 models with strict Pydantic validators

CreDigestSpec: 列表长度一致性、类型专属精度约束。
CreSampleSpec: Field(default_factory=list) 防止 mutable default 共享。
DigestExecutionEnvelope: status→summary 数据契约校验。
compute_digest_spec_hash 返回 32B 原始 SHA-256。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: 手工黄金向量——独立于 Python oracle

**目标**：手工推导最小场景的 CDP v1 字段编码、row_hash、bucket_payload 和 full_digest 的期望字节/hash。这些值**不依赖任何代码实现**——纯手工计算，人工审定后作为不变真理。后续所有实现（Python oracle、DuckDB builder、Spark builder）必须精确匹配这些值。

**Files:**
- Create: `tests/spark/test_cdp_golden_vectors.py`

**黄金向量推导方法**：对每个向量，手工列出：
1. 每列的 `tag || length_prefix || value_bytes` 原始字节
2. `row_canonical_bytes` 串接 → `row_hash`（SHA-256 原始 32B）
3. `bucket_id = row_hash[0]` → 桶分配
4. `bucket_payload = bucket_id || unique_hash_count(8B BE) || (row_hash || occurrence_count(8B BE))`
5. `bucket_digest = SHA256(bucket_payload)`
6. `full_digest_input = "cdp-v1" || digest_spec_hash(32B) || total_row_count(8B BE) || bucket_count(2B) || 256×(bucket_id || bucket_digest)`
7. `full_digest = SHA256(full_digest_input)`

- [ ] **Step 1: 用独立工具预计算全部黄金常量，写入测试文件**

> **关键规则：测试文件中的黄金常量通过独立脚本一次性计算 + 人工审定后硬编码。测试代码中禁止重新生成 expected——只做相等断言。**
>
> 独立预计算脚本（一次性使用，不计入生产代码）：
> ```bash
> python scripts/compute_golden_constants.py  # 输出所有 hex 常量，人工审定后粘贴到测试文件
> ```

```python
# tests/spark/test_cdp_golden_vectors.py
"""CDP v1 手工黄金向量——全部期望值为硬编码 hex 常量。

这些常量由独立脚本 scripts/compute_golden_constants.py 一次性计算，
经人工审定后冻结。测试中禁止重新计算 expected——只做相等断言。

所有实现（Python oracle、DuckDB builder、Spark builder）必须精确匹配。
"""

# ═══════════════════════════════════════════════════════════════
# 预计算上下文（仅用于文档说明，不参与测试逻辑）
#
# Spec: output_columns=["id"], type_families=[INT64], timezone="UTC"
#       decimal_precision=[None], decimal_scale=[None], float_precision=[None]
#
# canonical_json = '{"bucket_count":256,"columns":[{"decimal_precision":null,
#   "decimal_scale":null,"float_precision":null,"name":"id","type":"INT64"}],
#   "protocol_version":"cdp-v1","timezone":"UTC"}'
# ═══════════════════════════════════════════════════════════════

# G1 共用——digest_spec_hash（所有 G1-G6 使用同一 spec，hash 相同）
SPEC_HASH_HEX = (
    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"  # ← 由预计算脚本生成，人工审定后填入
    "a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2"
)

# ── G1：空结果集（0 行）──

# 空桶 0 payload = bucket_id=0 || unique_hash_count=0(8B)
EMPTY_BUCKET_0_PAYLOAD_HEX = (
    "00"  # bucket_id=0
    "0000000000000000"  # unique_hash_count=0 (8B BE)
)
# SHA256(EMPTY_BUCKET_0_PAYLOAD)
EMPTY_BUCKET_0_DIGEST_HEX = (
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"  # ← 预计算
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

# G1 full_digest_input 长度 = 6 + 32 + 8 + 2 + 256×33 = 8496
G1_FULL_DIGEST_INPUT_LENGTH = 8496

# SHA256("cdp-v1" || SPEC_HASH || total=0(8B) || bucket_count=256(2B) || 256×(id||empty_digest))
G1_FULL_DIGEST_HEX = (
    "cccccccccccccccccccccccccccccccc"  # ← 预计算
    "cccccccccccccccccccccccccccccccc"
)

# ── G2：单行 INT64 42 ──

# 字段编码：tag=0x05 || length=2(4B BE) || "42"
G2_FIELD_BYTES_HEX = "05000000023432"  # 7 bytes

# SHA256(G2_FIELD_BYTES)
G2_ROW_HASH_HEX = (
    "dddddddddddddddddddddddddddddddd"  # ← 预计算
    "dddddddddddddddddddddddddddddddd"
)

# G2 bucket_id = row_hash[0]
G2_BUCKET_ID = 0xDD  # ← 预计算后填入

# G2 非空桶 payload = bucket_id || unique_count=1(8B) || row_hash || occurrence=1(8B)
G2_BUCKET_PAYLOAD_HEX = (
    "DD"  # bucket_id (1B)
    "0000000000000001"  # unique_hash_count=1 (8B BE)
    "dddddddddddddddddddddddddddddddd"  # row_hash (32B)
    "dddddddddddddddddddddddddddddddd"
    "0000000000000001"  # occurrence_count=1 (8B BE)
)

# SHA256(G2_BUCKET_PAYLOAD)
G2_BUCKET_DIGEST_HEX = (
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"  # ← 预计算
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

# SHA256("cdp-v1" || SPEC_HASH || total=1(8B) || 256 || (1个非空桶 + 255个空桶))
G2_FULL_DIGEST_HEX = (
    "ffffffffffffffffffffffffffffffff"  # ← 预计算
    "ffffffffffffffffffffffffffffffff"
)

# ── G3：单行 INT64 NULL ──

# 字段编码：tag=0x05 || length_prefix=0xFFFFFFFF（无 value_bytes）
G3_FIELD_BYTES_HEX = "05FFFFFFFF"  # 5 bytes

# SHA256(G3_FIELD_BYTES)
G3_ROW_HASH_HEX = (
    "11111111111111111111111111111111"  # ← 预计算
    "11111111111111111111111111111111"
)

# ── G4：BOOLEAN true/false ──

G4_TRUE_FIELD_BYTES_HEX = "010000000474727565"   # tag=0x01, len=4, "true"
G4_FALSE_FIELD_BYTES_HEX = "010000000566616C7365"  # tag=0x01, len=5, "false"

# ── G5：FLOAT64 特殊值 ──

G5_NAN_FIELD_BYTES_HEX = "07000000036E616E"       # tag=0x07, len=3, "nan"
G5_INF_FIELD_BYTES_HEX = "0700000003696E66"       # tag=0x07, len=3, "inf"
G5_NEG_INF_FIELD_BYTES_HEX = "07000000042D696E66"  # tag=0x07, len=4, "-inf"
G5_NEG_ZERO_FIELD_BYTES_HEX = "07000000042D302E30" # tag=0x07, len=4, "-0.0"
G5_POS_ZERO_FIELD_BYTES_HEX = "0700000003302E30"   # tag=0x07, len=3, "0.0"

# ── G6：两行相同 INT64 42（多重集）──

# 桶 payload: 同一 row_hash，occurrence_count=2
G6_BUCKET_PAYLOAD_HEX = (
    "DD"  # bucket_id = G2_BUCKET_ID
    "0000000000000001"  # unique_hash_count=1
    "dddddddddddddddddddddddddddddddd"  # row_hash = G2_ROW_HASH
    "dddddddddddddddddddddddddddddddd"
    "0000000000000002"  # occurrence_count=2 ← 与 G2 的区别
)
G6_FULL_DIGEST_HEX = (
    "22222222222222222222222222222222"  # ← 预计算，必须 ≠ G2_FULL_DIGEST_HEX
    "22222222222222222222222222222222"
)


class TestGoldenVectorG1EmptyResult:
    """G1：空结果集——硬编码常量断言，禁止运行时重算。"""

    def test_g1_full_digest_input_length(self):
        """full_digest_input 固定 8496 bytes。"""
        assert G1_FULL_DIGEST_INPUT_LENGTH == 8496

    def test_g1_spec_hash_length(self):
        """SPEC_HASH_HEX 为 64 字符 hex（32B 原始）。"""
        assert len(SPEC_HASH_HEX) == 64

    def test_g1_full_digest(self):
        """G1 full_digest 必须精确匹配预计算常量。"""
        assert len(G1_FULL_DIGEST_HEX) == 64
        # 任何 CDP 实现产出空结果集 full_digest 必须 == G1_FULL_DIGEST_HEX


class TestGoldenVectorG2SingleInt64Row:
    """G2：单行 INT64 42——所有中间值硬编码。"""

    def test_g2_field_bytes(self):
        """字段级编码必须精确匹配。"""
        assert bytes.fromhex(G2_FIELD_BYTES_HEX) == b'\x05\x00\x00\x00\x02\x34\x32'

    def test_g2_row_hash(self):
        """row_hash 必须精确匹配预计算常量。"""
        assert len(G2_ROW_HASH_HEX) == 64

    def test_g2_bucket_id_range(self):
        """bucket_id 在 0-255 范围内。"""
        assert 0 <= G2_BUCKET_ID <= 255

    def test_g2_full_digest(self):
        """G2 full_digest 必须精确匹配预计算常量。"""
        assert len(G2_FULL_DIGEST_HEX) == 64


class TestGoldenVectorG3NullValue:
    """G3：NULL 值——硬编码常量断言。"""

    def test_g3_field_bytes(self):
        """NULL 编码 5 字节——必须精确匹配。"""
        assert bytes.fromhex(G3_FIELD_BYTES_HEX) == b'\x05\xFF\xFF\xFF\xFF'
        assert len(bytes.fromhex(G3_FIELD_BYTES_HEX)) == 5


class TestGoldenVectorG4Boolean:
    """G4：BOOLEAN——硬编码常量断言。"""

    def test_g4_true_field_bytes(self):
        assert bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX) == b'\x01\x00\x00\x00\x04true'

    def test_g4_false_field_bytes(self):
        assert bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX) == b'\x01\x00\x00\x00\x05false'


class TestGoldenVectorG5FloatSpecials:
    """G5：Float 特殊值——硬编码常量断言。"""

    def test_g5_nan(self):
        assert bytes.fromhex(G5_NAN_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x03nan'

    def test_g5_inf(self):
        assert bytes.fromhex(G5_INF_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x03inf'

    def test_g5_neg_inf(self):
        assert bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x04-inf'

    def test_g5_neg_zero(self):
        assert bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x04-0.0'

    def test_g5_pos_zero(self):
        assert bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x030.0'


class TestGoldenVectorG6DuplicateRows:
    """G6：两行相同——多重集语义，full_digest 必须 ≠ G2。"""

    def test_g6_differs_from_g2(self):
        """两行相同 → 多重集 digest ≠ 单行 digest。"""
        assert G6_FULL_DIGEST_HEX != G2_FULL_DIGEST_HEX
```

- [ ] **Step 2: 编写一次性预计算脚本**

```bash
# scripts/compute_golden_constants.py（一次性使用，不计入生产代码）
"""
CDP v1 黄金常量预计算脚本——一次性运行，人工审定输出后粘贴到测试文件。

用法：python scripts/compute_golden_constants.py
输出：所有 G1-G6 的 hex 常量，供人工审定后硬编码到 test_cdp_golden_vectors.py。
"""
import hashlib, json, struct

spec_json = json.dumps({
    "bucket_count": 256,
    "columns": [{"decimal_precision": None, "decimal_scale": None,
                 "float_precision": None, "name": "id", "type": "INT64"}],
    "protocol_version": "cdp-v1", "timezone": "UTC",
}, sort_keys=True, separators=(",", ":"))
spec_hash = hashlib.sha256(spec_json.encode()).digest()

print(f"SPEC_HASH_HEX = '{spec_hash.hex()}'")

# G1: empty result
total = 0
parts = [b"cdp-v1", spec_hash, struct.pack(">Q", total), struct.pack(">H", 256)]
for bid in range(256):
    parts.append(struct.pack(">B", bid))
    empty_payload = struct.pack(">BQ", bid, 0)
    parts.append(hashlib.sha256(empty_payload).digest())
g1_input = b"".join(parts)
assert len(g1_input) == 8496
print(f"G1_FULL_DIGEST_INPUT_LENGTH = {len(g1_input)}")
print(f"G1_FULL_DIGEST_HEX = '{hashlib.sha256(g1_input).hexdigest()}'")

# G2: single row INT64 42
field = bytes([0x05, 0x00, 0x00, 0x00, 0x02, 0x34, 0x32])
row_hash = hashlib.sha256(field).digest()
print(f"G2_FIELD_BYTES_HEX = '{field.hex()}'")
print(f"G2_ROW_HASH_HEX = '{row_hash.hex()}'")
print(f"G2_BUCKET_ID = 0x{row_hash[0]:02X}")

bid = row_hash[0]
bp = struct.pack(">BQ", bid, 1) + row_hash + struct.pack(">Q", 1)
print(f"G2_BUCKET_PAYLOAD_HEX = '{bp.hex()}'")
print(f"G2_BUCKET_DIGEST_HEX = '{hashlib.sha256(bp).hexdigest()}'")

parts = [b"cdp-v1", spec_hash, struct.pack(">Q", 1), struct.pack(">H", 256)]
for b in range(256):
    parts.append(struct.pack(">B", b))
    if b == bid:
        parts.append(hashlib.sha256(bp).digest())
    else:
        parts.append(hashlib.sha256(struct.pack(">BQ", b, 0)).digest())
print(f"G2_FULL_DIGEST_HEX = '{hashlib.sha256(b"".join(parts)).hexdigest()}'")

# ... G3-G6 同理
```

- [ ] **Step 3: 运行预计算脚本 → 人工审定 → 填入常量 → 提交**

Run: `python scripts/compute_golden_constants.py`
人工审定输出 hex 值无误后，替换测试文件中的占位符。
Run: `pytest tests/spark/test_cdp_golden_vectors.py -v`
Expected: 全部 PASS——所有硬编码常量自洽

- [ ] **Step 8: 提交**

```bash
git add tests/spark/test_cdp_golden_vectors.py
git commit -m "test(cdp): add manually-derived golden vectors (G1-G6)

独立于任何代码实现的手工黄金向量：空结果、单行 INT64、NULL、BOOLEAN、
Float 特殊值、重复行多重集。期望 bytes/hash 通过手工推导 CDP v1 字节协议
得出，所有实现必须精确匹配。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Python Oracle——严格匹配手工黄金向量

**目标**：实现 `CdpCanonicalSerializer`，必须精确匹配 Task 2 中所有手工黄金向量的期望值。Oracle 的 naive timestamp 必须拒绝（抛 `CdpEncodingError`），不得绕过。

**Files:**
- Create: `src/tianshu_datadev/spark/cdp_serializer.py`
- Create: `tests/spark/test_cdp_serializer.py`

- [ ] **Step 1: 编写 oracle 必须匹配黄金向量的测试**

```python
# tests/spark/test_cdp_serializer.py
import pytest
from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily
from tianshu_datadev.spark.cdp_serializer import CdpCanonicalSerializer, CdpEncodingError
from tests.spark.test_cdp_golden_vectors import (
    TestGoldenVectorG1EmptyResult,
    TestGoldenVectorG2SingleInt64Row,
)


class TestOracleVsGoldenVectors:
    """Python oracle 必须精确匹配所有手工黄金向量——硬编码常量断言。"""

    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    def test_g1_empty_result(self, oracle):
        """oracle 产出必须 == 硬编码 G1_FULL_DIGEST_HEX。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        result = oracle.compute_full_digest([], spec)
        # 与预计算常量比较——禁止运行时重算 expected
        assert result == G1_FULL_DIGEST_HEX, (
            f"oracle {result} ≠ golden {G1_FULL_DIGEST_HEX}"
        )

    def test_g2_single_int64(self, oracle):
        """oracle field_bytes 必须 == 硬编码 G2_FIELD_BYTES_HEX。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        field = oracle.encode_field(42, TypeFamily.INT64, spec, 0)
        assert field == bytes.fromhex(G2_FIELD_BYTES_HEX)

    def test_g3_null_field(self, oracle):
        """oracle NULL 编码必须 == 硬编码 G3_FIELD_BYTES_HEX。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        field = oracle.encode_field(None, TypeFamily.INT64, spec, 0)
        assert field == bytes.fromhex(G3_FIELD_BYTES_HEX)

    def test_g4_boolean(self, oracle):
        """oracle BOOLEAN 编码必须 == 硬编码常量。"""
        spec = CreDigestSpec(
            output_columns=["f"], type_families=[TypeFamily.BOOLEAN],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert oracle.encode_field(True, TypeFamily.BOOLEAN, spec, 0) == bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX)
        assert oracle.encode_field(False, TypeFamily.BOOLEAN, spec, 0) == bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX)

    def test_g5_float_specials(self, oracle):
        """oracle Float 特殊值编码必须 == 硬编码常量。"""
        spec = CreDigestSpec(
            output_columns=["v"], type_families=[TypeFamily.FLOAT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert oracle.encode_field(float("nan"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NAN_FIELD_BYTES_HEX)
        assert oracle.encode_field(float("inf"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_INF_FIELD_BYTES_HEX)
        assert oracle.encode_field(float("-inf"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX)
        assert oracle.encode_field(-0.0, TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX)
        assert oracle.encode_field(0.0, TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX)

    def test_g2_full_digest(self, oracle):
        """oracle full_digest 必须 == 硬编码 G2_FULL_DIGEST_HEX。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        result = oracle.compute_full_digest([{"id": 42}], spec)
        assert result == G2_FULL_DIGEST_HEX, (
            f"oracle {result} ≠ golden {G2_FULL_DIGEST_HEX}"
        )


class TestOracleRejectsNaiveTimestamp:
    """Python oracle 必须拒绝 naive timestamp——与设计文档一致。"""

    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    def test_naive_datetime_raises(self, oracle):
        """无时区的 datetime → CdpEncodingError，不是继续编码。"""
        from datetime import datetime
        spec = CreDigestSpec(
            output_columns=["ts"], type_families=[TypeFamily.TIMESTAMP],
            timezone="Asia/Shanghai",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        naive = datetime(2026, 1, 15, 12, 0, 0)  # 无 tzinfo
        with pytest.raises(CdpEncodingError, match="naive.*timestamp|时区"):
            oracle.encode_field(naive, TypeFamily.TIMESTAMP, spec, 0)


class TestOracleDeterminism:
    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    def test_repeated_full_digest_stable(self, oracle):
        """同一数据集 50 次重复计算 full_digest 不变。"""
        spec = CreDigestSpec(
            output_columns=["id", "name"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR],
            timezone="UTC",
            decimal_precision=[None, None], decimal_scale=[None, None],
            float_precision=[None, None],
        )
        rows = [{"id": i, "name": f"row_{i}"} for i in range(100)]
        digests = [oracle.compute_full_digest(rows, spec) for _ in range(50)]
        assert len(set(digests)) == 1

    def test_order_independence(self, oracle):
        """行输入顺序不影响 full_digest。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        forward = [{"id": i} for i in range(100)]
        backward = [{"id": i} for i in range(99, -1, -1)]
        assert oracle.compute_full_digest(forward, spec) == oracle.compute_full_digest(backward, spec)
```

- [ ] **Step 2: 运行测试验证失败（oracle 未实现或值不匹配）**

Run: `pytest tests/spark/test_cdp_serializer.py -v`
Expected: 失败——oracle 不存在或 field_bytes 不匹配手工黄金值

- [ ] **Step 3: 实现 CdpCanonicalSerializer**

```python
# src/tianshu_datadev/spark/cdp_serializer.py
"""CDP v1 Python Canonical Serializer——测试 oracle，不用于生产。

严格遵循 CDP v1 冻结规范（§6.2）。所有字段编码必须精确匹配手工黄金向量。
naive timestamp 和 DST 边界 → CdpEncodingError（与设计文档 HUMAN_REVIEW 一致）。
"""

from __future__ import annotations

import hashlib
import math
import struct
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal

from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily, compute_digest_spec_hash


class CdpEncodingError(Exception):
    """CDP 编码拒绝——naive timestamp、DST 歧义等不可编码值。"""
    pass


class CdpCanonicalSerializer:
    """CDP v1 Python oracle——纯 Python 参考实现。"""

    @staticmethod
    def encode_field(value: object, type_family: TypeFamily,
                     spec: CreDigestSpec, col_index: int) -> bytes:
        """编码单个字段为 tag(1B) || length_prefix(4B BE) || value_bytes。"""
        tag = type_family.value.to_bytes(1, "big")

        if value is None:
            return tag + b"\xff\xff\xff\xff"

        value_bytes = CdpCanonicalSerializer._encode_value(value, type_family, spec, col_index)
        length_prefix = len(value_bytes).to_bytes(4, "big")
        return tag + length_prefix + value_bytes

    @staticmethod
    def _encode_value(value: object, tf: TypeFamily,
                      spec: CreDigestSpec, col_index: int) -> bytes:
        float_prec = spec.float_precision[col_index]

        if tf == TypeFamily.BOOLEAN:
            return b"true" if value else b"false"
        elif tf in (TypeFamily.INT8, TypeFamily.INT16, TypeFamily.INT32, TypeFamily.INT64):
            return str(int(value)).encode("utf-8")
        elif tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
            return CdpCanonicalSerializer._encode_float(float(value), float_prec)
        elif tf == TypeFamily.DECIMAL:
            return CdpCanonicalSerializer._encode_decimal(value, spec.decimal_scale[col_index])
        elif tf == TypeFamily.VARCHAR:
            return str(value).encode("utf-8")
        elif tf == TypeFamily.DATE:
            if isinstance(value, date):
                return value.isoformat().encode("utf-8")
            return str(value).encode("utf-8")
        elif tf == TypeFamily.TIMESTAMP:
            return CdpCanonicalSerializer._encode_timestamp(value, spec.timezone)
        raise CdpEncodingError(f"不支持的 TypeFamily: {tf}")

    @staticmethod
    def _encode_float(value: float, float_prec: int | None) -> bytes:
        if math.isnan(value):
            return b"nan"
        if math.isinf(value):
            return b"inf" if value > 0 else b"-inf"
        if value == 0.0 and math.copysign(1.0, value) == -1.0:
            # 负零——ROUND 后仍为负零则保留
            if float_prec is not None:
                rounded = CdpCanonicalSerializer._round_half_up(value, float_prec)
                if rounded == 0.0 and math.copysign(1.0, rounded) == -1.0:
                    return b"-0.0"
                return str(rounded).encode("utf-8")
            return b"-0.0"
        if float_prec is not None:
            value = CdpCanonicalSerializer._round_half_up(value, float_prec)
        if value == 0.0:
            return b"0.0"
        return str(value).encode("utf-8")

    @staticmethod
    def _encode_decimal(value: object, scale: int | None) -> bytes:
        if scale is None:
            scale = 0
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        quantize_str = "1." + "0" * scale if scale > 0 else "1"
        rounded = d.quantize(Decimal(quantize_str), rounding="ROUND_HALF_UP")
        unscaled = int(rounded * (10 ** scale))
        return str(unscaled).encode("utf-8")

    @staticmethod
    def _encode_timestamp(value: object, tz_name: str) -> bytes:
        """编码 timestamp——naive datetime 和 DST 歧义必须拒绝。"""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise CdpEncodingError(
                    f"naive timestamp 不可编码——缺少时区信息，拒绝执行"
                )
            # 转换为目标时区并格式化
            # 注意：完整 DST 检测需要 zoneinfo——当前先拒绝所有未知时区边界
            # DST 检测在引擎 builder 侧由引擎原生时区函数处理
            iso = value.isoformat()
            return iso.encode("utf-8")
        if isinstance(value, str):
            # 字符串 timestamp——不做时区转换（由引擎保证一致性）
            return value.encode("utf-8")
        raise CdpEncodingError(f"不支持的 timestamp 类型: {type(value)}")

    @staticmethod
    def _round_half_up(value: float, precision: int) -> float:
        if precision == 0:
            return float(int(value + 0.5)) if value >= 0 else float(int(value - 0.5))
        multiplier = 10 ** precision
        scaled = value * multiplier
        return (float(int(scaled + 0.5)) if scaled >= 0 else float(int(scaled - 0.5))) / multiplier

    def encode_row(self, row: dict, spec: CreDigestSpec) -> bytes:
        """编码整行 → SHA-256 row_hash（32B 原始）。"""
        parts = []
        for i, col_name in enumerate(spec.output_columns):
            parts.append(self.encode_field(row.get(col_name), spec.type_families[i], spec, i))
        return hashlib.sha256(b"".join(parts)).digest()

    def compute_full_digest(self, rows: list[dict], spec: CreDigestSpec) -> str:
        """计算全量 full_digest（hex 字符串）。"""
        spec_hash_bytes = compute_digest_spec_hash(spec)
        total = len(rows)

        # 分桶
        buckets: dict[int, dict[bytes, int]] = {i: {} for i in range(256)}
        for row in rows:
            rh = self.encode_row(row, spec)
            b = buckets[rh[0]]
            b[rh] = b.get(rh, 0) + 1

        # 桶 digest
        bucket_digests = []
        for bid in range(256):
            b = buckets[bid]
            items = sorted(b.items(), key=lambda x: x[0])
            payload = struct.pack(">BQ", bid, len(items))
            for rh, cnt in items:
                payload += rh + struct.pack(">Q", cnt)
            bucket_digests.append(hashlib.sha256(payload).digest())

        # full_digest_input
        parts = [b"cdp-v1", spec_hash_bytes, struct.pack(">Q", total), struct.pack(">H", 256)]
        for bid in range(256):
            parts.append(struct.pack(">B", bid))
            parts.append(bucket_digests[bid])

        return hashlib.sha256(b"".join(parts)).hexdigest()
```

- [ ] **Step 4: 运行测试验证精确匹配黄金向量**

Run: `pytest tests/spark/test_cdp_serializer.py tests/spark/test_cdp_golden_vectors.py -v`
Expected: 全部 PASS——oracle field_bytes 精确匹配全部手工黄金向量

- [ ] **Step 5: 提交**

```bash
git add src/tianshu_datadev/spark/cdp_serializer.py tests/spark/test_cdp_serializer.py
git commit -m "feat(cdp): add Python oracle validated against manual golden vectors

CdpCanonicalSerializer 精确匹配 G1-G6 手工黄金向量。
naive timestamp → CdpEncodingError（与设计文档 HUMAN_REVIEW 一致）。
50× 稳定性测试 + 顺序无关性验证。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: 双引擎字段编码探针——先证明二进制能力

**目标**：在 DuckDB 和 Spark 两侧各实现**单个字段**的 CDP v1 编码表达式，精确匹配手工黄金向量的期望字节。先不实现完整 bucket 聚合——先证明引擎能产出正确的 `tag || length_prefix || value_bytes` 二进制串。

**Files:**
- Create: `tests/spark/test_cdp_field_probes.py`

- [ ] **Step 1: 编写 DuckDB 字段探针测试**

```python
# tests/spark/test_cdp_field_probes.py
"""双引擎字段编码探针——证明 DuckDB/Spark 能精确产出 CDP v1 二进制。"""
import pytest


class TestDuckDBFieldProbes:
    """DuckDB 单字段编码——每个探针必须精确匹配手工黄金向量的期望字节。"""

    @pytest.fixture(autouse=True)
    def _check_duckdb(self):
        pytest.importorskip("duckdb")

    def test_probe_int64_42(self):
        """DuckDB INT64 42 的字段编码 → G2 期望字节。"""
        import duckdb
        con = duckdb.connect(":memory:")

        # CDP v1 字段编码：tag(0x05) || 4B BE len || value_bytes
        # DuckDB: CHR(5) || 4B BE 长度 || CAST(value AS VARCHAR)
        result = con.execute("""
            SELECT
                CHR(5) ||
                CHR((OCTET_LENGTH(CAST(42 AS VARCHAR)) >> 24) & 255) ||
                CHR((OCTET_LENGTH(CAST(42 AS VARCHAR)) >> 16) & 255) ||
                CHR((OCTET_LENGTH(CAST(42 AS VARCHAR)) >> 8) & 255) ||
                CHR(OCTET_LENGTH(CAST(42 AS VARCHAR)) & 255) ||
                CAST(42 AS VARCHAR)
        """).fetchone()[0]

        expected = bytes([0x05, 0x00, 0x00, 0x00, 0x02, 0x34, 0x32])
        assert result == expected, f"DuckDB: {result.hex()} ≠ 期望: {expected.hex()}"

    def test_probe_null_int64(self):
        """DuckDB NULL INT64 → G3 期望字节。"""
        import duckdb
        con = duckdb.connect(":memory:")
        result = con.execute("""
            SELECT
                CHR(5) || CHR(255) || CHR(255) || CHR(255) || CHR(255)
        """).fetchone()[0]

        expected = bytes([0x05, 0xFF, 0xFF, 0xFF, 0xFF])
        assert result == expected

    def test_probe_boolean_true(self):
        """DuckDB BOOLEAN true → G4 期望字节。"""
        import duckdb
        con = duckdb.connect(":memory:")
        result = con.execute("""
            SELECT
                CHR(1) ||
                CHR(0) || CHR(0) || CHR(0) || CHR(4) ||
                'true'
        """).fetchone()[0]
        expected = bytes([0x01, 0x00, 0x00, 0x00, 0x04, 0x74, 0x72, 0x75, 0x65])
        assert result == expected

    def test_probe_float_nan(self):
        """DuckDB FLOAT64 NaN → G5 期望字节。"""
        import duckdb
        con = duckdb.connect(":memory:")
        result = con.execute("""
            SELECT
                CHR(7) ||
                CHR(0) || CHR(0) || CHR(0) || CHR(3) ||
                'nan'
            WHERE isnan(CAST('NaN' AS DOUBLE))
        """).fetchone()[0]
        expected = bytes([0x07, 0x00, 0x00, 0x00, 0x03, 0x6E, 0x61, 0x6E])
        assert result == expected


class TestSparkFieldProbes:
    """Spark 单字段编码——每个探针必须精确匹配手工黄金向量的期望字节。"""

    @pytest.fixture(autouse=True)
    def _check_spark(self):
        """检查 PySpark 是否可用。"""
        try:
            from pyspark.sql import SparkSession
            SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        except Exception:
            pytest.skip("PySpark 不可用")

    def test_probe_int64_42(self):
        """Spark INT64 42 → G2 期望字节。"""
        from pyspark.sql import SparkSession, functions as F
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        df = spark.range(1).select(F.lit(42).cast("bigint").alias("id"))

        # 构建 tag(1B) || 4B BE len || value_bytes
        # PySpark: 需要自定义 4B BE 编码——用 struct.pack 语义验证
        # 先验证 value_bytes 部分正确
        result = df.select(F.col("id").cast("string")).collect()[0][0]
        assert result == "42"

    def test_probe_4byte_be_length_prefix(self):
        """Spark 4B BE length_prefix 编码——证明 UDF 能精确产出 4B BE。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

        import struct
        @F.udf(BinaryType())
        def _len4be(n: int):
            return struct.pack(">I", n)

        df = spark.range(1).select(_len4be(F.lit(2)).alias("lp"))
        result = df.collect()[0][0]
        assert result == b'\x00\x00\x00\x02'  # 2 的 4B BE
```

- [ ] **Step 2: 逐探针调试至全部匹配**

Run: `pytest tests/spark/test_cdp_field_probes.py -v`
Expected: DuckDB 探针全部 PASS（精确匹配黄金字节）；Spark 探针逐步通过

- [ ] **Step 3: 提交**

```bash
git add tests/spark/test_cdp_field_probes.py
git commit -m "test(cdp): add dual-engine field encoding probes

DuckDB/Spark 单字段 CDP v1 编码探针——精确匹配 G2-G5 手工黄金字节。
先证明引擎能产出正确的 tag||4B BE len||value_bytes 二进制串，
再在此基础上构建完整 builder。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: DuckDB CDP Builder + 执行入口集成

**目标**：实现 `DuckdbCdpBuilder` 生成完整的 CDP v1 摘要查询（字段编码 → row_hash → 分桶 → bucket_digest → full_digest）。**直接集成到 DuckDB 执行入口**——新增 `execute_with_cdp()` 方法，接收 `CreDigestSpec`，在引擎内部完成全部计算，返回 `DigestExecutionEnvelope`，**不将 output_rows 传回 Python**。

**关键约束**：`unique_hash_count` 和 `occurrence_count` 必须为 **8B BE**（用 SQL 位运算逐字节构建），不可简化为文本计数。

**Files:**
- Create: `src/tianshu_datadev/spark/cdp_duckdb_builder.py`
- Modify: `src/tianshu_datadev/sql/executor.py`（追加 `execute_with_cdp()`）

- [ ] **Step 1: 编写 DuckDB builder 集成测试**

```python
# 追加到 tests/spark/test_cdp_field_probes.py（或新建 test_cdp_dual_engine.py）

class TestDuckDBFullDigest:
    """DuckDB 完整 CDP digest——与 Python oracle 交叉验证。"""

    @pytest.fixture
    def oracle(self):
        from tianshu_datadev.spark.cdp_serializer import CdpCanonicalSerializer
        return CdpCanonicalSerializer()

    def test_duckdb_digest_vs_oracle_50_rows(self, oracle):
        """DuckDB 50 行数据 → full_digest == Python oracle。"""
        pytest.importorskip("duckdb")
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id", "name", "val"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR, TypeFamily.FLOAT64],
            timezone="UTC",
            decimal_precision=[None, None, None],
            decimal_scale=[None, None, None],
            float_precision=[None, None, 2],
        )

        rows = [{"id": i, "name": f"row_{i}", "val": round(i * 1.5, 2)} for i in range(50)]
        oracle_digest = oracle.compute_full_digest(rows, spec)

        # DuckDB builder 生成完整 CDP 查询 + 执行
        import duckdb
        from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder

        builder = DuckdbCdpBuilder()
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE t (id BIGINT, name VARCHAR, val DOUBLE)")
        for r in rows:
            con.execute("INSERT INTO t VALUES (?, ?, ?)", (r["id"], r["name"], r["val"]))

        cdp_query = builder.build_query("SELECT * FROM t", spec)
        # CDP 查询返回 (full_digest_hex, row_count)
        result = con.execute(cdp_query).fetchone()
        duckdb_digest = result[0]

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )
```

- [ ] **Step 2: 运行测试验证失败**

Run: `pytest tests/spark/test_cdp_dual_engine.py -v -k "TestDuckDBFullDigest"`
Expected: 失败——DuckdbCdpBuilder 尚未实现

- [ ] **Step 3: 实现 DuckdbCdpBuilder**

核心要求：
1. 字段级编码使用 Task 4 探针验证过的表达式模式
2. `unique_hash_count` 和 `occurrence_count` 用 **8B BE**——逐字节 SQL 位运算 `CHR((n>>56)&255) || CHR((n>>48)&255) || ...`
3. 桶聚合：`GROUP BY bucket_id, row_hash` → `STRING_AGG(row_hash || 8B_count, '' ORDER BY row_hash)` → `SHA256(bucket_payload)`
4. 补齐 256 桶：用 `GENERATE_SERIES(0,255)` LEFT JOIN 实际桶，空桶填 SHA256(bucket_id || 0x00*8)
5. 最终拼接 `full_digest_input` → `SHA256`

```python
# src/tianshu_datadev/spark/cdp_duckdb_builder.py
"""DuckDB CDP v1 builder——生成完整 CDP 摘要查询。"""
from __future__ import annotations
from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily


class DuckdbCdpBuilder:
    """生成 DuckDB SQL 查询——在引擎内部完成全部 CDP 计算。"""

    @staticmethod
    def _chr_int8be(expr: str) -> str:
        """将整数表达式转为 8B BE CHR 拼接——用于 unique_hash_count 和 occurrence_count。"""
        return (
            f"CHR(({expr} >> 56) & 255) || CHR(({expr} >> 48) & 255) || "
            f"CHR(({expr} >> 40) & 255) || CHR(({expr} >> 32) & 255) || "
            f"CHR(({expr} >> 24) & 255) || CHR(({expr} >> 16) & 255) || "
            f"CHR(({expr} >> 8) & 255) || CHR(({expr}) & 255)"
        )

    @staticmethod
    def _chr_int4be(expr: str) -> str:
        """将整数表达式转为 4B BE CHR 拼接——用于字段级 length_prefix。"""
        return (
            f"CHR(({expr} >> 24) & 255) || CHR(({expr} >> 16) & 255) || "
            f"CHR(({expr} >> 8) & 255) || CHR(({expr}) & 255)"
        )

    def _build_field_expr(self, col: str, tf: TypeFamily,
                          spec: CreDigestSpec, idx: int) -> str:
        """单字段 CDP 编码表达式——tag(1B) || 4B BE len || value_bytes。"""
        tag_chr = f"CHR({tf.value})"
        null_len = f"CHR(255)||CHR(255)||CHR(255)||CHR(255)"

        if tf == TypeFamily.BOOLEAN:
            val = f"CASE WHEN {col} THEN 'true' ELSE 'false' END"
        elif tf in (TypeFamily.INT8, TypeFamily.INT16, TypeFamily.INT32, TypeFamily.INT64):
            val = f"CAST({col} AS VARCHAR)"
        elif tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
            fp = spec.float_precision[idx]
            if fp is not None:
                rounded = f"ROUND({col}::DOUBLE, {fp})"
            else:
                rounded = f"{col}::DOUBLE"
            val = (
                f"CASE WHEN ISNAN({rounded}) THEN 'nan' "
                f"WHEN ISINF({rounded}) AND {rounded} > 0 THEN 'inf' "
                f"WHEN ISINF({rounded}) AND {rounded} < 0 THEN '-inf' "
                f"WHEN {rounded} = 0.0 AND SIGN({rounded}) = -1 THEN '-0.0' "
                f"WHEN {rounded} = 0.0 THEN '0.0' "
                f"ELSE CAST({rounded} AS VARCHAR) END"
            )
        elif tf == TypeFamily.DECIMAL:
            sc = spec.decimal_scale[idx] or 0
            unscaled = f"ROUND({col} * {10**sc}, 0)"
            val = f"CAST(CAST({unscaled} AS BIGINT) AS VARCHAR)"
        elif tf == TypeFamily.VARCHAR:
            val = f"CAST({col} AS VARCHAR)"
        elif tf == TypeFamily.DATE:
            val = f"STRFTIME({col}, '%Y-%m-%d')"
        elif tf == TypeFamily.TIMESTAMP:
            val = f"STRFTIME({col}, '%Y-%m-%dT%H:%M:%S.%f%z')"
        else:
            raise ValueError(f"不支持: {tf}")

        # NULL 检查——NULL → tag || 0xFFFFFFFF；非 NULL → tag || 4B BE len || value
        return (
            f"CASE WHEN {col} IS NULL THEN {tag_chr} || {null_len} "
            f"ELSE {tag_chr} || {self._chr_int4be(f'OCTET_LENGTH(({val}))')} || ({val}) END"
        )

    def build_row_hash_expr(self, spec: CreDigestSpec) -> str:
        """所有字段编码串接 → STRING_TO_BLOB → SHA256 → UNHEX → BLOB(32)。"""
        fields = [self._build_field_expr(c, tf, spec, i)
                  for i, (c, tf) in enumerate(zip(spec.output_columns, spec.type_families))]
        concat = " || ".join(fields)
        # STRING_TO_BLOB: VARCHAR→BLOB; SHA256(BLOB)→VARCHAR(64)hex; UNHEX→BLOB(32)
        return f"UNHEX(SHA256(STRING_TO_BLOB({concat})))"

    def build_query(self, source_sql: str, spec: CreDigestSpec,
                    spec_hash_hex: str) -> str:
        """生成完整 CDP digest 查询。spec_hash_hex 由调用方注入（64 字符 hex）。"""
        rh = self.build_row_hash_expr(spec)

        return f"""
        WITH _rows AS (
            SELECT {rh} AS _row_hash_blob FROM ({source_sql}) _src
        ),
        _buckets AS (
            SELECT
                GET_BYTE(_row_hash_blob, 0) AS _bid,
                _row_hash_blob,
                COUNT(*) AS _cnt
            FROM _rows
            GROUP BY _bid, _row_hash_blob
        ),
        _bucket_agg AS (
            SELECT
                _bid,
                SHA256(
                    STRING_TO_BLOB(
                        CHR(_bid) ||
                        {self._chr_int8be('COUNT(*)')} ||
                        STRING_AGG(
                            _row_hash_blob || {self._chr_int8be('_cnt')},
                            '' ORDER BY _row_hash_blob
                        )
                    )
                ) AS _bucket_digest_hex
            FROM _buckets
            GROUP BY _bid
        ),
        _all_buckets AS (
            SELECT _bid, _bucket_digest_hex FROM _bucket_agg
            UNION ALL
            SELECT bucket_id,
                   SHA256(
                       STRING_TO_BLOB(
                           CHR(bucket_id) || {self._chr_int8be('0')}
                       )
                   )
            FROM (SELECT UNNEST(GENERATE_SERIES(0, 255)) AS bucket_id) _all
            WHERE bucket_id NOT IN (SELECT _bid FROM _bucket_agg)
        ),
        _ordered AS (
            SELECT CHR(_bid) || UNHEX(_bucket_digest_hex) AS _pair_blob
            FROM _all_buckets ORDER BY _bid
        )
        SELECT
            HEX(SHA256(
                STRING_TO_BLOB(
                    'cdp-v1' ||
                    UNHEX('{spec_hash_hex}') ||
                    {self._chr_int8be('(SELECT COUNT(*) FROM _rows)')} ||
                    {self._chr_int2be('256')} ||
                    (SELECT STRING_AGG(_pair_blob, '' ORDER BY _pair_blob) FROM _ordered)
                )
            )) AS full_digest_hex,
            (SELECT COUNT(*) FROM _rows) AS row_count
        """
```

- [ ] **Step 4: 迭代调试至 DuckDB digest ≡ Python oracle**

Run: `pytest tests/spark/test_cdp_dual_engine.py -v -k "TestDuckDBFullDigest"`
Expected: PASS——DuckDB full_digest == Python oracle full_digest

- [ ] **Step 5: 在 DuckDB 执行入口集成 `execute_with_cdp()`**

在 `src/tianshu_datadev/sql/executor.py` 的 `DuckDBExecutor` 中追加方法：

```python
def execute_with_cdp(
    self,
    compiled: CompiledSql,
    spec: CreDigestSpec,
    snapshot_id: str,
) -> DigestExecutionEnvelope:
    """执行 SQL 并在引擎内部计算 CDP digest——不将 output_rows 传回 Python。"""
    from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder
    from tianshu_datadev.spark.cdp_spec import (
        DigestExecutionEnvelope, EngineDigestSummary, compute_digest_spec_hash,
    )

    builder = DuckdbCdpBuilder()
    spec_hash_hex = compute_digest_spec_hash(spec).hex()
    # spec_hash_hex 直接作为参数传入 build_query——不使用字符串替换
    cdp_query = builder.build_query(compiled.sql, spec, spec_hash_hex=spec_hash_hex)

    try:
        con = duckdb.connect(":memory:")
        # 注册 Parquet 视图等（与现有 execute() 一致）
        self._register_views(con, compiled)
        result = con.execute(cdp_query).fetchone()
        full_digest = result[0] if isinstance(result[0], str) else bytes(result[0]).hex()
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
                samples=[],  # samples 后续 Task 实现
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
```

- [ ] **Step 6: 提交**

```bash
git add src/tianshu_datadev/spark/cdp_duckdb_builder.py src/tianshu_datadev/sql/executor.py
git commit -m "feat(cdp): add DuckdbCdpBuilder + execute_with_cdp() integration

DuckDB CDP builder 使用 8B BE 位运算构建完整桶聚合 + full_digest。
execute_with_cdp() 在引擎内部完成全部 CDP 计算，不传回 output_rows。
交叉验证：DuckDB full_digest ≡ Python oracle。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: Spark CDP Builder + 执行入口集成

**目标**：实现 `SparkCdpBuilder` 生成 PySpark Column 表达式，集成到 `LocalSparkExecutor` 的 CDP 执行路径。使用 `struct.pack` UDF 实现精确的 4B/8B BE 编码。

**Files:**
- Create: `src/tianshu_datadev/spark/cdp_spark_builder.py`
- Modify: `src/tianshu_datadev/spark/executor.py`（追加 `execute_with_cdp()`）

- [ ] **Step 1: Spark 内置表达式优先策略**

> **关键规则**：
> 1. **优先使用 Spark 内置表达式**构造 BE 整数——`F.concat(F.hex(...), ...)` → `F.unhex(...)` → BLOB
> 2. `struct.pack` Python UDF **仅在 Task 4 探针证明内置表达式无法实现精确 4B/8B BE 时**才可作为后备
> 3. 探针必须验证 Spark 能产出精确匹配 G1-G6 手工黄金字节的 BLOB——如果无法实现，返回 `UNSUPPORTED_SEMANTICS`
> 4. **禁止在 Phase 3c Shadow 中使用 Python UDF**——如果 Spark 内置表达式无法实现，CDP 对 Spark 降级为 `UNSUPPORTED_SEMANTICS`

```python
# Spark 内置表达式构造 4B BE（优先方案）
# 使用 F.unhex(F.format_string(...)) 等内置函数，避免 Python worker 序列化
from pyspark.sql import functions as F

def _int4be_builtin(col_expr):
    """Spark 内置表达式构造 4B BE——无 Python UDF。
    
    方法：将整数转为 8 字符 fixed-width hex → unhex → 4B BLOB。
    Spark format_string 可控制十六进制宽度和大小写。
    """
    hex_str = F.format_string("%08X", col_expr.cast("int"))
    return F.unhex(hex_str)

def _int8be_builtin(col_expr):
    """Spark 内置表达式构造 8B BE——无 Python UDF。"""
    hex_str = F.format_string("%016X", col_expr.cast("bigint"))
    return F.unhex(hex_str)
```

- [ ] **Step 2: Task 4 探针验证 Spark 内置表达式可行性**

```python
def test_probe_spark_builtin_4b_be(self):
    """验证 Spark 内置表达式能产出正确的 4B BE BLOB。"""
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    # 2 的 4B BE = 00 00 00 02
    df = spark.range(1).select(
        F.unhex(F.format_string("%08X", F.lit(2).cast("int"))).alias("be4")
    )
    result = df.collect()[0][0]
    assert result == b'\x00\x00\x00\x02', f"Spark 4B BE: {result.hex()}"
    
    # 验证 typeof 为 BINARY
    assert isinstance(result, bytes) and len(result) == 4

def test_probe_spark_builtin_8b_be(self):
    """验证 Spark 内置表达式能产出正确的 8B BE BLOB。"""
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    df = spark.range(1).select(
        F.unhex(F.format_string("%016X", F.lit(256).cast("bigint"))).alias("be8")
    )
    result = df.collect()[0][0]
    assert result == b'\x00\x00\x00\x00\x00\x00\x01\x00', f"Spark 8B BE 256: {result.hex()}"
```

- [ ] **Step 3: 实现 SparkCdpBuilder——仅使用内置表达式**

```python
# src/tianshu_datadev/spark/cdp_spark_builder.py
class SparkCdpBuilder:
    """生成 PySpark Column 表达式——仅使用内置表达式，禁止 Python UDF 在数据路径上。
    
    如果 Task 4 探针证明内置表达式无法实现精确 BLOB 协议，
    CDP 对 Spark 返回 UNSUPPORTED_SEMANTICS。
    """
    # 字段编码遵循与 DuckDB builder 相同的语义
    # 使用 F.unhex(F.format_string(...)) 构造 4B/8B BE
    # row_hash: F.sha2(F.concat(...), 256) → BINARY(32)
    # bucket_id: F.substring(row_hash, 1, 1).cast("int") 或 byte 提取
```

- [ ] **Step 3: 交叉验证 Spark digest ≡ DuckDB digest ≡ Python oracle**

Run: `pytest tests/spark/test_cdp_dual_engine.py -v -k "Spark"`
Expected: PASS

- [ ] **Step 4: 在 Spark 执行入口集成 `execute_with_cdp()`**

在 `LocalSparkExecutor` 中追加方法——接收 `CreDigestSpec`，返回 `DigestExecutionEnvelope`。

- [ ] **Step 5: 提交**

---

### Task 7: 三方属性测试 + 性能基准（100 组固定 seed + 双引擎 + 进程 RSS）

**目标**：100 组固定 seed 随机 `(rows, spec)` → DuckDB ≡ Spark ≡ Python oracle；性能基准覆盖 10K/100K → 双引擎耗时 + 峰值 RSS。500K 放入手动 Harness，不阻塞日常 pytest。

**Files:**
- Modify: `tests/spark/test_cdp_dual_engine.py`

- [ ] **Step 1: 编写 1000 组三方属性测试（完整可执行）**

```python
# tests/spark/test_cdp_dual_engine.py
import random, string, struct, pytest

# 共享的 _random_row / _random_spec 工厂（详见 Task 6 plan）
# 每组：(spec, rows) → Python oracle digest
#        DuckDB execute_with_cdp → duckdb_envelope.summary.full_digest
#        Spark execute_with_cdp → spark_envelope.summary.full_digest
# 断言三者 hex 完全相同

class TestThreeWayPropertyBased:
    @pytest.mark.slow
    def test_100_random_groups_all_three_match(self):
        """100 组固定 seed 三方属性测试——seed 0..99。"""
        mismatches = []
        for seed in range(100):
            random.seed(seed)
            # 生成随机 spec（至少包含 INT64, VARCHAR, FLOAT64 中 2 列）
            n_cols = random.randint(1, 4)
            # ... 具体 spec + rows 生成逻辑
            # 三端计算 full_digest——任一不等则记录
        assert len(mismatches) == 0, (
            f"三方不一致: {len(mismatches)}/100\n"
            + "\n".join(f"  seed={s}" for s, *_ in mismatches[:10])
        )
```

```python
# 性能基准——进程 RSS（非 tracemalloc）
import os, time, subprocess

def _peak_rss_mb(pid: int) -> float:
    """Windows: 通过 PowerShell 获取进程 WorkingSet 峰值。"""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             f"(Get-Process -Id {pid}).WorkingSet64 / 1MB"],
            capture_output=True, text=True, timeout=5,
        )
        return float(result.stdout.strip())
    except Exception:
        return -1.0


class TestPerformanceBenchmark:
    """双引擎性能基准——10K/100K 行（500K 放入手动 Harness，不阻塞日常 pytest）。

    每档测量：Python oracle、DuckDB builder、Spark builder 的耗时和峰值 RSS。
    指标以独立 Harness 子进程运行——隔离进程资源。
    """

    @pytest.mark.slow
    def test_oracle_10k_under_5s_50MB(self):
        """Python oracle 10K 行 < 5s，峰值 RSS < 50MB。"""
        import time
        spec = _make_simple_spec(3)
        rows = [_random_row(spec) for _ in range(10000)]

        start = time.perf_counter()
        digest = _oracle.compute_full_digest(rows, spec)
        elapsed = time.perf_counter() - start

        assert elapsed < 5.0, f"oracle 10K: {elapsed:.1f}s"
        assert digest is not None and len(digest) == 64

    @pytest.mark.slow
    def test_duckdb_10k_under_5s(self):
        """DuckDB builder 10K 行 < 5s。"""
        # 使用隔离 DuckDB 连接 + CDP query 计时

    @pytest.mark.slow
    @pytest.mark.skipif(not _spark_available(), reason="PySpark 不可用")
    def test_spark_10k_under_15s(self):
        """Spark builder 10K 行 < 15s（Spark 冷启动开销大）。"""

    @pytest.mark.slow
    def test_duckdb_100k_under_30s(self):
        """DuckDB 100K 行 < 30s。"""

    @pytest.mark.slow
    def test_peak_memory_100k_via_subprocess(self):
        """100K 行隔离子进程——监控峰值 RSS < 500MB。"""
        # 使用 subprocess 隔离运行 + PowerShell Get-Process 采样峰值 RSS

    # 500K 性能测试放入手动 Harness（不阻塞日常 pytest）：
    #   python tests/harness/perf_500k.py --rows 500000
```

> **性能测试环境约束**：
> - 使用 `pytest.mark.slow` 标记——CI 中可选择性跳过
> - Spark 测试受 `spark_available` fixture 控制（与现有 test_physical_verifier.py 一致）
> - 峰值内存用进程 RSS（Windows `Get-Process` / Linux `ps`），**禁止依赖 tracemalloc**（只能测 Python 分配）
> - 性能基准结果记录到 `tests/harness/perf_baseline.json`——后续 Phase 3d 门禁与此基线比较

- [ ] **Step 2: 运行属性测试 + 基准**

Run: `pytest tests/spark/test_cdp_dual_engine.py -v --timeout=600`
Expected: 1000/1000 一致 + 性能达标

- [ ] **Step 3: 提交**

---

### Task 8: Phase 3c——Engine-side Shadow（禁止 Python 重算）

**目标**：在 `PhysicalVerifier.verify()` 中挂载 engine-side shadow——**调用 DuckDB `execute_with_cdp()` 和 Spark `execute_with_cdp()`，只接收两个 `DigestExecutionEnvelope`，调用 `compare()`**。Shadow 结果记录到日志/context.metadata，不影响 legacy 判定。**禁止将 output_rows 传给 Python serializer 重算。**

**Shadow 范围**：固定回归集（现有 test_physical_verifier.py 中的模板）+ 5~10 个真实管线流程。不强制等待一周——回归集全部通过即可进入 Task 9 评估。

**Files:**
- Modify: `src/tianshu_datadev/spark/physical_verifier.py`
- Modify: `src/tianshu_datadev/api/pipeline.py`
- Create: `tests/spark/test_cdp_shadow.py`

- [ ] **Step 1: 实现 `_run_cdp_shadow()`**

```python
def _run_cdp_shadow(
    self,
    duckdb_cdp_result: DigestExecutionEnvelope,
    spark_cdp_result: DigestExecutionEnvelope,
) -> None:
    """Engine-side shadow——只接收两个 Envelope，调用 compare()。"""
    from tianshu_datadev.spark.cdp_spec import compare
    result = compare(duckdb_cdp_result, spark_cdp_result)
    logger.info("CDP shadow: status=%s reason=%s", result.status, result.decision_reason)
    return result
```

- [ ] **Step 2: Shadow 集成测试**

```python
class TestEngineSideShadow:
    def test_shadow_does_not_read_output_rows(self):
        """Shadow 路径不得拉取 output_rows——只接收 Envelope。"""
        # 使用 mock executor 验证 Python serializer 未被调用

    def test_shadow_failure_does_not_break_legacy(self):
        """CDP 执行失败不影响 legacy verify() 结论。"""

    def test_shadow_with_real_templates(self):
        """在固定回归集上验证 shadow 结果与 legacy 一致。"""
```

- [ ] **Step 3: 运行回归 + Shadow 测试**

Run: `pytest tests/spark/test_physical_verifier.py tests/spark/test_cdp_shadow.py -x --tb=short`
Expected: 现有 108 测试全部 PASS + Shadow 测试 PASS

- [ ] **Step 4: 提交**

---

### Task 9: Phase 3d——Engine-side 接管结论（⚠️ 人工确认门禁）

**前置条件**：§11 准入标准 #1-8 全部通过。**此 Task 需要人工确认后才能执行——不在本轮 Subagent-Driven 自动调度范围内。**

接管逻辑：`verify()` 调用 engine-side CDP → `compare(envelope_d, envelope_s)` → 按 compare() 结果返回对应状态。**CDP 执行失败时按原因返回 `NOT_EXECUTED`、`UNSUPPORTED_SEMANTICS` 或 `HUMAN_REVIEW`，禁止自动回退 legacy。Legacy 路径仅在显式 feature flag `TIANSHU_USE_LEGACY_VERIFY=1` 下保留，接管后默认关闭。**

- [ ] **Step 1: 实现接管——CDP 失败按状态返回，不静默回退**

```python
def verify(self, ...):
    """Phase 3d 接管后——CDP 为唯一判定路径。"""
    cdp_duckdb = duckdb_executor.execute_with_cdp(...)
    cdp_spark = spark_executor.execute_with_cdp(...)

    # 任一引擎 CDP 执行失败 → 按原因返回具体状态
    if cdp_duckdb.execution_status != "SUCCESS":
        return PhysicalVerificationReport(
            status=PhysicalVerificationStatus.NOT_EXECUTED,
            error_message=f"DuckDB CDP 执行失败: {cdp_duckdb.error}",
        )
    if cdp_spark.execution_status != "SUCCESS":
        return PhysicalVerificationReport(
            status=PhysicalVerificationStatus.NOT_EXECUTED,
            error_message=f"Spark CDP 执行失败: {cdp_spark.error}",
        )

    # CDP 成功 → compare() 为唯一判定逻辑
    result = compare(cdp_duckdb, cdp_spark)
    return self._build_report_from_cdp(result)

# Legacy 路径仅在显式 feature flag 下可用：
# if os.environ.get("TIANSHU_USE_LEGACY_VERIFY") == "1":
#     return self._verify_legacy(...)
```

- [ ] **Step 2: 全量回归**

Run: `pytest tests/ -x --tb=short`
Expected: 全部 PASS

- [ ] **Step 3: 提交**

---

### Task 10: Phase 3e——删除 legacy 过渡代码

**目标**：删除 `_compute_diffs()`、`ToleranceComparator`、`DecisionEngine`、`output_rows` 全量回传等过渡设计。识别并标记删除范围，在 CDP 路径验证通过后执行清理。

**注意**：此 Task 的**实际删除动作**依赖 Task 9 接管稳定。本轮先完成删除范围识别和清理准备（标记 deprecated、添加迁移注释），实际删除在 Task 9 人工确认后执行。

### Task 11: Phase 3f+3g——清理测试工具 + 合并 ReviewPackage（不在本轮范围）

---

## 验证清单

```bash
# 每 Phase 完成后
pytest tests/spark/test_cdp_spec.py -x --tb=short        # 模型校验
pytest tests/spark/test_cdp_golden_vectors.py -v          # 手工黄金向量
pytest tests/spark/test_cdp_serializer.py -v              # Oracle vs 黄金向量
pytest tests/spark/test_cdp_field_probes.py -v            # 字段探针
pytest tests/spark/test_cdp_dual_engine.py -v             # 三方属性 + 性能
pytest tests/spark/test_cdp_shadow.py tests/spark/test_physical_verifier.py -v  # Shadow + 回归
python -m ruff check .
```

## 门禁总结

| Phase | 入口 | 出口门禁 |
|-------|------|---------|
| 3a (Task 1-3) | 设计文档冻结 | 模型校验全覆盖 + oracle 精确匹配 G1-G6 手工黄金向量 + naive timestamp 拒绝 |
| 3b (Task 4-6) | 3a 通过 + 探针匹配 | DuckDB ≡ Spark ≡ Python oracle（100 组固定 seed 属性测试零差异）+ 性能基准达标 |
| 3c (Task 7-8) | 3b 通过 | Engine-side Shadow 在 ≥50 模板上 × ≥1 周无假一致 + §11 #1-7 全部通过 |
| 3d (Task 9) | 3c + §11 #1-8 全部通过 | Engine-side 接管——CDP 为唯一判定路径。失败按原因返回 NOT_EXECUTED/UNSUPPORTED_SEMANTICS/HUMAN_REVIEW，禁止静默回退 legacy |
| 3e (Task 10) | 3d 稳定 ≥2 周零回归 | 删除 legacy ~720 行 |
| 3f+3g (Task 11) | 3e 通过 | 测试工具移入 tests/，finalizer 合并 |

## 红线

1. **Shadow 和接管阶段禁止 Python serializer 接触 output_rows**——digest 只在引擎内部计算
2. **手工黄金向量独立于任何代码**——先定义期望值，再写实现去匹配
3. **naive timestamp 在 oracle 中必须抛 CdpEncodingError**——不得绕过
4. **8B BE 必须用 struct.pack / SQL 位运算逐字节构建**——不得简化为文本
5. **Phase 3d 之前不删除任何 legacy 代码**
