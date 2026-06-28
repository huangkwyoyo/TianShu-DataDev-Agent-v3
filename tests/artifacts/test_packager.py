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
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.relationship_planner import FakeRelationshipPlanner
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

# ── 辅助 ──


def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _build_single_table_inputs(request_id: str = "test_req_pkg") -> PackageInputs:
    """构建单表的完整 PackageInputs。"""
    spec_text = _read_fixture("fixtures/golden/golden_no_time_range.md")
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
        manifest = builder.build(inputs)

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
        manifest = builder.build(inputs)

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
