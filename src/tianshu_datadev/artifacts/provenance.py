"""provenance.yml 生成器——记录 Code Review Package 的完整溯源链。

字段覆盖：所有输入 artifact 的 hash、编译器/验证器版本、返工轮次、时间戳和环境指纹。
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys

from .models import PackageInputs

# 已知版本常量——Phase 2 硬编码，Phase 4 后从配置读取
COMPILER_VERSION = "1.1.0"
VALIDATOR_VERSION = "1.0.0"
PROMPT_VERSION = "phase-2-fake"


def generate_provenance(
    inputs: PackageInputs,
    timestamp: str | None = None,
) -> tuple[str, str]:
    """生成 provenance.yml 文本及其 SHA-256。

    Args:
        inputs: 组装 Code Review Package 所需的全部输入
        timestamp: ISO 时间戳字符串——为 None 时使用空字符串（确定性默认值），
                  调用方应显式传入 datetime.now(timezone.utc).isoformat() 以获取真实时间戳

    Returns:
        (provenance_yml_content, provenance_sha256)
    """
    now = timestamp if timestamp is not None else ""

    # 从序列化 dict 中提取 hash 值
    spec_hash = _safe_get(inputs.parsed_spec, "spec_hash", "")
    source_manifest_id = _safe_get(inputs.source_manifest, "manifest_id", "")
    hypothesis_id = _safe_get(inputs.hypothesis, "hypothesis_id", "") if inputs.hypothesis else ""
    plan_id = _safe_get(inputs.sql_build_plan, "plan_id", "")
    artifact_id = _safe_get(inputs.sql_artifact, "artifact_id", "")
    artifact_sql_sha256 = ""
    if inputs.sql_artifact and "compiled_sql" in inputs.sql_artifact:
        artifact_sql_sha256 = _safe_get(
            inputs.sql_artifact["compiled_sql"], "sql_sha256", ""
        )

    # ── 多语句 hash（新增） ──
    compiled_program_sha256 = ""
    statement_sql_sha256_entries: list[dict] = []
    if inputs.sql_program_artifact:
        from tianshu_datadev.artifacts.packager import ReviewPackageBuilder

        full_sql = ReviewPackageBuilder._assemble_full_sql(
            inputs.sql_program_artifact, inputs.sql_artifact
        )
        compiled_program_sha256 = hashlib.sha256(
            full_sql.encode("utf-8")
        ).hexdigest()
        # 逐语句 hash
        compiled = inputs.sql_program_artifact.get("compiled", {})
        for cs in compiled.get("statements", []):
            stmt_sql = cs.get("sql", "")
            stmt_hash = hashlib.sha256(stmt_sql.encode("utf-8")).hexdigest()
            statement_sql_sha256_entries.append({
                "sql_sha256": stmt_hash,
            })

    contract_id = _safe_get(inputs.data_transform_contract, "contract_id", "")
    trace_id = _safe_get(inputs.execution_trace, "trace_id", "") if inputs.execution_trace else ""

    # 计算各原始对象的 hash
    parsed_spec_hash = compute_json_hash(inputs.parsed_spec)
    source_manifest_hash = compute_json_hash(inputs.source_manifest)
    hypothesis_hash = compute_json_hash(inputs.hypothesis) if inputs.hypothesis else ""
    sql_build_plan_hash = compute_json_hash(inputs.sql_build_plan)
    # sql_artifact 的完整性由 compiled_sql_sha256 保证——不需要额外 hash
    data_transform_contract_hash = compute_json_hash(inputs.data_transform_contract)
    execution_trace_hash = compute_json_hash(inputs.execution_trace) if inputs.execution_trace else ""
    result_summary_hash = compute_json_hash(inputs.result_summary) if inputs.result_summary else ""
    # ── Phase 9B-P0: 从 PackageInputs 计算 snapshot manifest hash ──
    snapshot_manifest_hash = compute_json_hash(inputs.snapshot_manifest) if inputs.snapshot_manifest else ""

    # 构建环境指纹
    env_fingerprint = _build_environment_fingerprint()

    yml = f"""# ── Code Review Package 溯源 ──
# 生成时间: {now}
# 本文件记录所有 artifact 的版本和 hash 追溯信息

request_id: "{inputs.request_id}"
spec_hash: "{spec_hash}"
parsed_spec_hash: "{parsed_spec_hash}"
source_manifest_hash: "{source_manifest_hash}"
relationship_hypothesis_hash: "{hypothesis_hash}"
sql_build_plan_hash: "{sql_build_plan_hash}"
compiled_sql_sha256: "{artifact_sql_sha256}"
# ── 多语句 hash（单语句时为空） ──
compiled_program_sha256: "{compiled_program_sha256}"
statement_sql_sha256: {json.dumps(statement_sql_sha256_entries, ensure_ascii=False)}
optimized_plan_hash: "{sql_build_plan_hash}"
data_transform_contract_hash: "{data_transform_contract_hash}"
snapshot_manifest_hash: "{snapshot_manifest_hash}"
execution_trace_hash: "{execution_trace_hash}"
result_summary_hash: "{result_summary_hash}"

# ── 版本信息 ──
model_id: "{_safe_get(inputs.sql_artifact, 'model_id', 'phase-2-fake')}"
prompt_version: "{PROMPT_VERSION}"
compiler_version: "{COMPILER_VERSION}"
validator_version: "{VALIDATOR_VERSION}"

# ── 执行信息 ──
retry_count: {inputs.retry_count}
timestamp: "{now}"
environment_fingerprint: "{env_fingerprint}"

# ── Artifact ID 映射 ──
artifact_ids:
  source_manifest: "{source_manifest_id}"
  hypothesis: "{hypothesis_id}"
  sql_build_plan: "{plan_id}"
  sql_artifact: "{artifact_id}"
  data_transform_contract: "{contract_id}"
  execution_trace: "{trace_id}"
"""

    # 计算 provenance.yml 自身的 SHA-256
    provenance_sha256 = hashlib.sha256(yml.encode("utf-8")).hexdigest()

    return yml, provenance_sha256


def compute_json_hash(data: dict | None) -> str:
    """计算 JSON 可序列化数据的 SHA-256——canonical JSON 序列化后取 SHA-256。

    公开函数，供 ReviewPackageManifest 和 provenance.yml 共同使用，
    确保 manifest 与 provenance 的 hash 值一致。
    """
    if data is None:
        return ""
    import json

    content = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _safe_get(data: dict | None, key: str, default: str = "") -> str:
    """安全地从 dict 中获取字符串值。"""
    if data is None:
        return default
    if not key:  # key 为空时返回整个 dict 的 hash
        return compute_json_hash(data)
    val = data.get(key, default)
    return str(val) if val is not None else default


def _build_environment_fingerprint() -> str:
    """构建环境指纹——记录运行时环境特征。"""
    py_ver = sys.version.split()[0] if hasattr(sys, "version") else "unknown"
    os_name = platform.system() if hasattr(platform, "system") else "unknown"
    # 只记录主版本号，不记录完整路径或用户名（隐私考虑）
    return f"python={py_ver};os={os_name}"
