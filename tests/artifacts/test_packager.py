"""测试 ReviewPackageBuilder——Code Review Package 组装器。

覆盖：
- 目录结构完整
- 相同输入生成稳定 artifact hash
- 非法输入生成拒绝报告
- 不保存完整结果集
"""

import hashlib
import json
import os
import tempfile

import pytest

from tianshu_datadev.artifacts.models import PackageInputs, ReviewPackageManifest
from tianshu_datadev.artifacts.packager import ReviewPackageBuilder
from tianshu_datadev.artifacts.provenance import compute_json_hash
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tests._test_utils import read_fixture


# ── 辅助 ──


def read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _build_single_table_inputs(request_id: str = "test_req_pkg") -> PackageInputs:
    """构建单表的完整 PackageInputs。"""
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
            "tables": [
                {
                    "table_ref": "tf",
                    "source_table": "test_fact",
                    "columns": [],
                }
            ],
            "conflicts": [],
            "anomalies": [],
        },
        hypothesis=None,
        sql_build_plan=plan.model_dump(),
        sql_artifact=artifact.model_dump(),
        execution_trace={
            "trace_id": "trace_test",
            "plan_id": plan.plan_id,
            "engine": "duckdb",
            "generated_sql": artifact.compiled_sql.sql,
            "status": "RUNTIME_PASS",
            "row_count": 100,
            "execution_time_ms": 15.5,
        },
        result_summary={
            "summary_id": "summary_test",
            "trace_id": "trace_test",
            "engine": "duckdb",
            "columns": ["zone", "total_amount"],
            "column_types": ["varchar", "double"],
            "row_count": 100,
            "null_counts": {},
            "numeric_sums": {},
        },
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
        open_questions=[
            {
                "question_id": "q001",
                "source": "parser",
                "description": "时间范围未声明——请确认是否需要限制数据时间范围",
                "blocking": False,
                "resolution": None,
            },
        ],
        validation_questions=[],
        perf_results=[],
        retry_count=0,
    )


def _verify_file(path: str) -> tuple[bool, str]:
    """验证文件存在且非空。返回 (存在, SHA-256)。"""
    if not os.path.isfile(path):
        return False, ""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return len(content) > 0, sha


# ════════════════════════════════════════════
# Packager 测试
# ════════════════════════════════════════════


class TestReviewPackageBuilder:
    """Code Review Package 组装器测试。"""

    def test_package_directory_structure_complete(self):
        """生成的目录必须包含所有预期文件。"""
        inputs = _build_single_table_inputs()
        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(tempfile.gettempdir(), "test_pkg_struct")
        )
        manifest = builder.build(inputs)

        package_dir = os.path.join(builder._base_dir, inputs.request_id)

        # 验证所有预期文件存在
        expected_files = [
            "developer_spec/raw.md",
            "developer_spec/parsed.json",
            "developer_spec/open_questions.md",
            "planning/relationship_hypotheses.md",
            "planning/sql_build_plan.json",
            "planning/field_lineage.md",
            "contracts/data_transform_contract.json",
            "sql/main.sql",
            "validation/source_validation.md",
            "validation/join_validation.md",
            "validation/enum_checks.md",
            "validation/execution_trace.json",
            "feedback/review_feedback.schema.json",
            "provenance.yml",
            "review.md",
        ]

        for rel_path in expected_files:
            abs_path = os.path.join(package_dir, rel_path)
            exists, sha = _verify_file(abs_path)
            assert exists, f"缺少文件: {rel_path}"
            assert sha != "", f"文件为空: {rel_path}"

        # 验证 manifest 记录了所有文件
        manifest_paths = {a.path for a in manifest.artifacts}
        for rel_path in expected_files:
            assert rel_path in manifest_paths, (
                f"Manifest 未记录文件: {rel_path}"
            )

        # 清理
        import shutil
        if os.path.isdir(package_dir):
            shutil.rmtree(package_dir)

    def test_artifact_hash_reproducible(self):
        """相同输入 → 相同 artifact hash。"""
        inputs = _build_single_table_inputs()

        dir1 = os.path.join(tempfile.gettempdir(), "test_pkg_hash_1")
        dir2 = os.path.join(tempfile.gettempdir(), "test_pkg_hash_2")

        try:
            fixed_ts = "2026-06-28T00:00:00.000000+00:00"

            builder1 = ReviewPackageBuilder(base_output_dir=dir1)
            builder1.set_fixed_timestamp(fixed_ts)
            manifest1 = builder1.build(inputs)

            builder2 = ReviewPackageBuilder(base_output_dir=dir2)
            builder2.set_fixed_timestamp(fixed_ts)
            manifest2 = builder2.build(inputs)

            # 相同 artifact 数量
            assert len(manifest1.artifacts) == len(manifest2.artifacts)

            # 相同路径 → 相同 hash
            hash_map1 = {a.path: a.sha256 for a in manifest1.artifacts}
            hash_map2 = {a.path: a.sha256 for a in manifest2.artifacts}

            for path, sha1 in hash_map1.items():
                sha2 = hash_map2.get(path)
                assert sha2 is not None, f"Manifest2 缺少: {path}"
                assert sha1 == sha2, (
                    f"文件 '{path}' hash 不一致: {sha1} vs {sha2}"
                )

        finally:
            import shutil
            for d in [dir1, dir2]:
                if os.path.isdir(d):
                    shutil.rmtree(d)

    def test_no_complete_result_set_saved(self):
        """不保存完整结果集——ExecutionTrace 只存 row_count。"""
        inputs = _build_single_table_inputs()

        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(tempfile.gettempdir(), "test_pkg_no_result")
        )
        _manifest = builder.build(inputs)

        package_dir = os.path.join(builder._base_dir, inputs.request_id)

        # 验证 execution_trace.json 不含完整数据行
        trace_path = os.path.join(package_dir, "validation/execution_trace.json")
        with open(trace_path, "r", encoding="utf-8") as f:
            trace_data = json.load(f)

        # 只检查 row_count（数值），不检查实际数据行
        assert "row_count" in trace_data
        assert isinstance(trace_data["row_count"], int)
        # 确保不含完整结果集（不应有 "rows"、"data"、"results" 等字段）
        assert "rows" not in trace_data
        assert "data" not in trace_data
        assert "results" not in trace_data

        # 清理
        import shutil
        if os.path.isdir(package_dir):
            shutil.rmtree(package_dir)

    def test_feedback_schema_valid_json_schema(self):
        """生成的 review_feedback.schema.json 必须是合法 JSON Schema。"""
        inputs = _build_single_table_inputs()

        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(tempfile.gettempdir(), "test_pkg_schema")
        )
        _manifest = builder.build(inputs)

        package_dir = os.path.join(builder._base_dir, inputs.request_id)
        schema_path = os.path.join(package_dir, "feedback/review_feedback.schema.json")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        # 验证 JSON Schema 结构
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["additionalProperties"] is False
        assert "target" in schema["properties"]
        assert "enum" in schema["properties"]["target"]

        # target 合法值必须在 schema enum 中
        from tianshu_datadev.artifacts.models import VALID_REVIEW_TARGETS
        assert set(schema["properties"]["target"]["enum"]) == set(VALID_REVIEW_TARGETS)

        # 所有必填字段
        required = schema.get("required", [])
        expected_required = [
            "request_id", "review_package_id", "developer_spec_hash",
            "source_manifest_hash", "sql_build_plan_hash", "sql_artifact_hash",
            "target", "finding_type", "comment", "suggested_resolution",
        ]
        for field in expected_required:
            assert field in required, f"JSON Schema 缺少 required 字段: {field}"

        # 清理
        import shutil
        if os.path.isdir(package_dir):
            shutil.rmtree(package_dir)

    def test_package_manifest_self_consistent(self):
        """Manifest 自身的一致性——package_id 确定性 + retry_count 正确。"""
        inputs = _build_single_table_inputs()

        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(tempfile.gettempdir(), "test_pkg_manifest")
        )
        manifest = builder.build(inputs)

        # package_id 确定性
        pkg_id_1 = ReviewPackageManifest.generate_package_id("test_req_pkg")
        pkg_id_2 = ReviewPackageManifest.generate_package_id("test_req_pkg")
        assert pkg_id_1 == pkg_id_2

        # retry_count
        assert manifest.retry_count == 0

        # 清理
        import shutil
        package_dir = os.path.join(builder._base_dir, inputs.request_id)
        if os.path.isdir(package_dir):
            shutil.rmtree(package_dir)

    def test_manifest_hash_fields_are_sha256(self):
        """Manifest 的 *_hash 字段必须是内容 SHA-256，不能是 ID。

        packager.py:760-766 原先将 manifest_id / plan_id / artifact_id /
        contract_id 填入 *_hash 字段。修复后应使用 compute_json_hash 计算
        canonical JSON SHA-256。
        """
        inputs = _build_single_table_inputs()

        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(tempfile.gettempdir(), "test_pkg_hash")
        )
        manifest = builder.build(inputs)

        # 1. SHA-256 格式校验
        hex64 = set("0123456789abcdef")

        # spec_hash 特殊——Parser 使用截断 SHA-256（16 字符），非完整 64 字符
        assert len(manifest.spec_hash) == 16, (
            f"spec_hash 应为 16 字符截断 SHA-256，实际：'{manifest.spec_hash}'"
        )
        assert all(c in hex64 for c in manifest.spec_hash)

        # 其余 5 个 hash 字段必须是完整 64 字符 SHA-256
        for field_name in [
            "source_manifest_hash",
            "sql_build_plan_hash",
            "sql_artifact_hash",
            "data_transform_contract_hash",
            "provenance_hash",
        ]:
            val = getattr(manifest, field_name)
            assert len(val) == 64, (
                f"manifest.{field_name} 应为 64 字符 SHA-256，实际长度 {len(val)}：'{val}'"
            )
            assert all(c in hex64 for c in val), (
                f"manifest.{field_name} 含非十六进制字符：'{val}'"
            )

        # 2. 各 hash 必须与 provenance.py 的 compute_json_hash 一致
        assert manifest.source_manifest_hash == compute_json_hash(
            inputs.source_manifest
        ), "manifest.source_manifest_hash 与 compute_json_hash(source_manifest) 不一致"

        assert manifest.sql_build_plan_hash == compute_json_hash(
            inputs.sql_build_plan
        ), "manifest.sql_build_plan_hash 与 compute_json_hash(sql_build_plan) 不一致"

        assert manifest.sql_artifact_hash == compute_json_hash(
            inputs.sql_artifact
        ), "manifest.sql_artifact_hash 与 compute_json_hash(sql_artifact) 不一致"

        assert manifest.data_transform_contract_hash == compute_json_hash(
            inputs.data_transform_contract
        ), (
            "manifest.data_transform_contract_hash 与 "
            "compute_json_hash(data_transform_contract) 不一致"
        )

        # 3. spec_hash 特殊——Parser 已计算 SHA-256，直接校验格式
        assert manifest.spec_hash == inputs.parsed_spec.get(
            "spec_hash", ""
        ), "manifest.spec_hash 应来自 parsed_spec.spec_hash"

        # 清理
        import shutil
        package_dir = os.path.join(builder._base_dir, inputs.request_id)
        if os.path.isdir(package_dir):
            shutil.rmtree(package_dir)

    def test_validate_contract_plan_hash_consistency(self):
        """Contract 的 source_sqlbuildplan_hash 必须与 Plan 的 hash 一致。

        修复前 _validate_inputs 有注释无代码，错配的 contract 可无声进入包。
        修复后不一致时 build() 应抛出 ValueError。
        """
        inputs = _build_single_table_inputs()

        # 1. 合法输入——build 成功
        builder = ReviewPackageBuilder(
            base_output_dir=os.path.join(
                tempfile.gettempdir(), "test_pkg_contract_ok"
            )
        )
        manifest = builder.build(inputs)
        assert manifest is not None

        # 清理
        import shutil
        ok_dir = os.path.join(builder._base_dir, inputs.request_id)
        if os.path.isdir(ok_dir):
            shutil.rmtree(ok_dir)

        # 2. 篡改 contract.source_sqlbuildplan_hash → ValueError
        inputs.data_transform_contract["source_sqlbuildplan_hash"] = (
            "deadbeefdeadbeef"  # 故意写错的 hash
        )
        builder2 = ReviewPackageBuilder(
            base_output_dir=os.path.join(
                tempfile.gettempdir(), "test_pkg_contract_bad"
            )
        )
        with pytest.raises(ValueError, match="source_sqlbuildplan_hash"):
            builder2.build(inputs)

        # 清理
        bad_dir = os.path.join(builder2._base_dir, inputs.request_id)
        if os.path.isdir(bad_dir):
            shutil.rmtree(bad_dir)

        # 3. contract 无 source_sqlbuildplan_hash（空字符串）→ 不校验，不报错
        inputs.data_transform_contract["source_sqlbuildplan_hash"] = ""
        builder3 = ReviewPackageBuilder(
            base_output_dir=os.path.join(
                tempfile.gettempdir(), "test_pkg_contract_empty"
            )
        )
        manifest3 = builder3.build(inputs)
        assert manifest3 is not None

        # 清理
        empty_dir = os.path.join(builder3._base_dir, inputs.request_id)
        if os.path.isdir(empty_dir):
            shutil.rmtree(empty_dir)
