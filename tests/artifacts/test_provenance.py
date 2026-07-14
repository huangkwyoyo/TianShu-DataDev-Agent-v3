"""测试 provenance.yml 生成器。

覆盖：
- provenance.yml 字段完整
- 返工轮次记录
"""

import os

from tianshu_datadev.artifacts.models import PackageInputs
from tianshu_datadev.artifacts.provenance import generate_provenance
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tests._test_utils import read_fixture


# ── 辅助 ──


def read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _build_minimal_inputs(request_id: str = "test_req", retry_count: int = 0) -> PackageInputs:
    """构建最小合法 PackageInputs——单表 golden fixture 全链路。"""
    spec_text = read_fixture("fixtures/golden/golden_no_time_range.md")
    parser = DeveloperSpecParser()
    spec = parser.parse(spec_text)

    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_to_artifact(plan, spec_hash=spec.spec_hash)

    return PackageInputs(
        request_id=request_id,
        original_spec_md=spec_text,
        parsed_spec=spec.model_dump(),
        source_manifest={
            "manifest_id": f"manifest_{spec.spec_hash[:12]}",
            "spec_hash": spec.spec_hash,
            "tables": [],
            "conflicts": [],
            "anomalies": [],
        },
        hypothesis=None,
        sql_build_plan=plan.model_dump(),
        sql_artifact=artifact.model_dump(),
        execution_trace=None,
        result_summary=None,
        data_transform_contract={
            "contract_id": "dtc_lite_test",
            "version": "lite",
            "source_phase": "phase-2",
            "source_sqlbuildplan_hash": SqlBuildPlan.generate_plan_hash(plan),
            "input_tables": [],
            "input_columns": [],
            "join_relationships": [],
            "filters": [],
            "aggregations": [],
            "grouping_keys": [],
            "output_columns": [],
            "output_grain": [],
            "business_keys": [],
            "semantic_policy_ref": "",
        },
        retry_count=retry_count,
    )


# ════════════════════════════════════════════
# Provenance 测试
# ════════════════════════════════════════════


class TestProvenance:
    """provenance.yml 生成器测试。"""

    def test_provenance_fields_complete(self):
        """provenance.yml 必须包含所有核心字段。"""
        inputs = _build_minimal_inputs()
        yml, sha256 = generate_provenance(inputs)

        # 验证 SHA-256 非空
        assert sha256 != ""
        assert len(sha256) == 64  # SHA-256 hex 长度

        # 验证必需字段
        required_fields = [
            "request_id:",
            "spec_hash:",
            "parsed_spec_hash:",
            "source_manifest_hash:",
            "sql_build_plan_hash:",
            "compiled_sql_sha256:",
            "compiled_program_sha256:",
            "statement_sql_sha256:",
            "optimized_plan_hash:",
            "data_transform_contract_hash:",
            "execution_trace_hash:",
            "snapshot_manifest_hash:",
            "compiler_version:",
            "validator_version:",
            "retry_count:",
            "timestamp:",
            "environment_fingerprint:",
        ]
        for field in required_fields:
            assert field in yml, f"provenance.yml 缺少字段: {field}"

        # 验证版本（与 provenance.py 中的常量一致）
        assert "compiler_version: \"1.1.0\"" in yml
        assert "validator_version: \"1.0.0\"" in yml

    def test_provenance_records_retry_count(self):
        """provenance.yml 必须记录返工轮次。"""
        # retry_count=0
        inputs0 = _build_minimal_inputs(retry_count=0)
        yml0, _ = generate_provenance(inputs0)
        assert "retry_count: 0" in yml0

        # retry_count=2
        inputs2 = _build_minimal_inputs(request_id="test_req_2", retry_count=2)
        yml2, _ = generate_provenance(inputs2)
        assert "retry_count: 2" in yml2

    def test_provenance_deterministic(self):
        """相同输入 → 相同 provenance.yml + 相同 hash。"""
        inputs = _build_minimal_inputs()
        yml1, sha1 = generate_provenance(inputs)
        yml2, sha2 = generate_provenance(inputs)

        # YAML 内容一致
        assert yml1 == yml2
        # SHA-256 一致
        assert sha1 == sha2

    def test_provenance_contains_artifact_ids(self):
        """provenance.yml 必须包含 artifact ID 映射。"""
        inputs = _build_minimal_inputs()
        yml, _ = generate_provenance(inputs)

        assert "artifact_ids:" in yml
        assert "source_manifest:" in yml
        assert "sql_build_plan:" in yml
        assert "sql_artifact:" in yml
        assert "data_transform_contract:" in yml

    def test_snapshot_manifest_hash_empty_when_none(self):
        """无快照时 snapshot_manifest_hash 必须为空字符串——不传 snapshot_manifest 时不应有 hash。"""
        inputs = _build_minimal_inputs()
        # _build_minimal_inputs() 不设 snapshot_manifest——默认 None
        yml, _ = generate_provenance(inputs)

        # 验证字段存在且值为空（provenance.yml 中应写 snapshot_manifest_hash: ""）
        assert 'snapshot_manifest_hash: ""' in yml, (
            "无快照时 snapshot_manifest_hash 应为空字符串，"
            "不应有任何非空 hash 值"
        )

    def test_snapshot_manifest_hash_deterministic(self):
        """相同 snapshot_manifest → 相同 hash——溯源链要求确定性。"""
        # 构造一个模拟的 snapshot_manifest dict
        snapshot_dict = {
            "snapshot_id": "snap_test_det_001",
            "contract_hash": "abc123",
            "files": [
                {
                    "source_name": "order_info",
                    "file_path": "/tmp/snap/order_info.parquet",
                    "format": "parquet",
                    "row_count": 100,
                    "file_sha256": "a" * 64,
                },
            ],
            "snapshot_sha256": "b" * 64,
        }

        inputs1 = PackageInputs(
            request_id="det_001",
            original_spec_md="test",
            parsed_spec={"spec_hash": "abc"},
            source_manifest={"manifest_id": "m1"},
            sql_build_plan={"plan_id": "p1"},
            sql_artifact={"artifact_id": "a1", "compiled_sql": {"sql_sha256": "c" * 64}},
            data_transform_contract={"contract_id": "dtc_1"},
            snapshot_manifest=snapshot_dict,
        )
        inputs2 = PackageInputs(
            request_id="det_001",
            original_spec_md="test",
            parsed_spec={"spec_hash": "abc"},
            source_manifest={"manifest_id": "m1"},
            sql_build_plan={"plan_id": "p1"},
            sql_artifact={"artifact_id": "a1", "compiled_sql": {"sql_sha256": "c" * 64}},
            data_transform_contract={"contract_id": "dtc_1"},
            snapshot_manifest=dict(snapshot_dict),  # 独立拷贝——内容相同
        )

        yml1, sha1 = generate_provenance(inputs1)
        yml2, sha2 = generate_provenance(inputs2)

        # YAML 内容必须完全一致
        assert yml1 == yml2, "相同输入 → provenance.yml 内容应完全一致"
        # SHA-256 必须一致
        assert sha1 == sha2, "相同输入 → provenance SHA-256 应一致"

        # 显式验证 hash 非空——有 snapshot 时不应为空
        import re
        match = re.search(r'snapshot_manifest_hash:\s*"([0-9a-f]*)"', yml1)
        assert match is not None, "provenance.yml 中必须有 snapshot_manifest_hash 字段"
        hash_val = match.group(1)
        assert len(hash_val) == 64, (
            f"snapshot_manifest_hash 应为 64 位 hex，实际: {len(hash_val)}"
        )
        assert hash_val != "", "有 snapshot 时 hash 不应为空"
