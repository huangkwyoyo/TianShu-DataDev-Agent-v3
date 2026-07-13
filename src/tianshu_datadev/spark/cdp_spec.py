"""CDP v1 数据模型——含严格 Pydantic 校验。"""
from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import ClassVar, Literal

from pydantic import Field, model_validator

from tianshu_datadev.developer_spec.models import StrictModel


class TypeFamily(Enum):
    BOOLEAN = 0x01
    INT8 = 0x02
    INT16 = 0x03
    INT32 = 0x04
    INT64 = 0x05
    FLOAT32 = 0x06
    FLOAT64 = 0x07
    DECIMAL = 0x08
    VARCHAR = 0x09
    DATE = 0x0A
    TIMESTAMP = 0x0B


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
    # 统一状态映射：UNSUPPORTED→UNSUPPORTED_SEMANTICS, REQUIRES_REVIEW→HUMAN_REVIEW
    _status_map = {
        "UNSUPPORTED": "UNSUPPORTED_SEMANTICS",
        "REQUIRES_REVIEW": "HUMAN_REVIEW",
        "FAILED": "NOT_EXECUTED",
        "TIMEOUT": "NOT_EXECUTED",
    }
    for env, label in [(duckdb, "DuckDB"), (spark, "Spark")]:
        if env.execution_status != "SUCCESS":
            mapped = _status_map.get(env.execution_status, "NOT_EXECUTED")
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
