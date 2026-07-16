"""CRE Finalizer E2E 测试——验证 ReviewPackageFinalizer + CreHarnessRunner。

Point 5 覆盖：
- finalize 前后所有旧 artifact 内容/hash 不变
- CRE/provenance/Manifest hash 一致
- 篡改被拒绝
- 多语句 SQL 不变
- finalize 失败可见
- 重复 finalize 幂等
- golden 已知差异零假阴性
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile

from tests._test_utils import read_fixture
from tianshu_datadev.artifacts.finalizer import (
    ReviewPackageFinalizer,
)
from tianshu_datadev.artifacts.models import PackageInputs, ReviewPackageManifest
from tianshu_datadev.artifacts.packager import ReviewPackageBuilder
from tianshu_datadev.cre_models import (
    CreShadowReport,
    CreShadowStatus,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.harness import (
    CreHarnessRunner,
    GoldenRegistry,
)
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def read_fixture(path: str) -> str:
    """读取测试 fixture 文件。"""
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _build_test_package(
    request_id: str = "test_cre_e2e",
    base_dir: str | None = None,
    include_cre: bool = False,
    cre_status: CreShadowStatus = CreShadowStatus.CONSISTENT,
) -> tuple[ReviewPackageBuilder, ReviewPackageManifest, str]:
    """构建测试用 Review Package——返回 (builder, manifest, package_dir)。

    Args:
        request_id: 请求 ID
        base_dir: 输出根目录（None 使用临时目录）
        include_cre: 是否在构建时包含 CRE shadow 报告
        cre_status: CRE 状态（include_cre=True 时生效）
    """
    spec_text = read_fixture("fixtures/golden/golden_no_time_range.md")
    parser = DeveloperSpecParser()
    spec = parser.parse(spec_text)

    plan_builder = SqlBuildPlanBuilder()
    plan, _ = plan_builder.build(spec)

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_to_artifact(plan, spec_hash=spec.spec_hash)

    output_dir = base_dir or tempfile.mkdtemp(prefix="tianshu_e2e_")

    cre_report = None
    if include_cre:
        diag_available = cre_status not in (
            CreShadowStatus.NOT_EXECUTED, CreShadowStatus.ERROR,
        )
        cre_report = CreShadowReport(
            diagnostic_available=diag_available,
            contract_hash="test_contract_hash",
            cre_status=cre_status,
            mapped_status="RESULT_CONSISTENT",
            legacy_status="RESULT_CONSISTENT",
            status_consistent=True,
            total_rows=100,
            exact_match_rows=100,
        )

    inputs = PackageInputs(
        request_id=request_id,
        original_spec_md=spec_text,
        parsed_spec=spec.model_dump(),
        source_manifest={
            "manifest_id": f"manifest_{spec.spec_hash[:12]}",
            "spec_hash": spec.spec_hash,
            "tables": [{
                "table_ref": "tf",
                "source_table": "test_fact",
                "columns": [],
            }],
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
            "sample_rows": [],
        },
        data_transform_contract={
            "contract_id": "contract_test",
            "spec_hash": spec.spec_hash,
            "source_sqlbuildplan_hash": SqlBuildPlan.generate_plan_hash(plan),
            "output_columns": [
                {"alias": "zone", "column_name": "zone", "data_type": "varchar"},
                {"alias": "total_amount", "column_name": "total_amount", "data_type": "double"},
            ],
        },
        open_questions=[],
        validation_questions=[],
        perf_results=[],
        retry_count=0,
        cre_shadow_report=cre_report,
    )

    packager = ReviewPackageBuilder(output_dir)
    manifest = packager.build(inputs)
    package_dir = os.path.join(output_dir, request_id)
    return packager, manifest, package_dir


def _make_cre_report(
    contract_hash: str = "test_contract_hash",
    cre_status: CreShadowStatus = CreShadowStatus.CONSISTENT,
    legacy_status: str = "RESULT_CONSISTENT",
    status_consistent: bool = True,
    diagnostic_available: bool = True,
) -> CreShadowReport:
    """构建测试用 CreShadowReport。"""
    return CreShadowReport(
        diagnostic_available=diagnostic_available,
        contract_hash=contract_hash,
        cre_status=cre_status,
        mapped_status="RESULT_CONSISTENT" if cre_status in (
            CreShadowStatus.CONSISTENT, CreShadowStatus.CONSISTENT_WITH_WARN,
        ) else "RESULT_MISMATCH",
        legacy_status=legacy_status,
        status_consistent=status_consistent,
        total_rows=100,
        exact_match_rows=95,
        tolerance_match_rows=5,
    )


# ════════════════════════════════════════════
# E2E 测试：ReviewPackageFinalizer
# ════════════════════════════════════════════


class TestFinalizerE2E:
    """ReviewPackageFinalizer 端到端测试。"""

    def test_finalize_preserves_all_existing_artifacts(self):
        """finalize 前后所有旧 artifact 内容/hash 不变。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_preserve_")
        _, manifest_before, pkg_dir = _build_test_package(
            request_id="test_preserve", base_dir=tmpdir,
        )

        # 记录 finalize 前所有 artifact 的路径和哈希
        hashes_before: dict[str, str] = {}
        for ref in manifest_before.artifacts:
            fp = os.path.join(pkg_dir, ref.path)
            sha = hashlib.sha256()
            with open(fp, "rb") as f:
                sha.update(f.read())
            hashes_before[ref.path] = sha.hexdigest()

        # 执行 finalize
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize("test_preserve", cre_report)

        assert result.success, f"Finalize 应成功：{result.errors}"
        assert result.artifacts_after == result.artifacts_before + 1, (
            f"artifact 应增加 1（CRE shadow），"
            f"实际：{result.artifacts_before} → {result.artifacts_after}"
        )

        # 验证旧 artifact 哈希全部不变（provenance.yml 和 manifest.json 除外——Finalizer 会更新它们）
        _finalizer_updated = {"provenance.yml", "manifest.json"}
        for ref in manifest_before.artifacts:
            if ref.path in _finalizer_updated:
                continue  # Finalizer 会更新这些——预期改变
            fp = os.path.join(pkg_dir, ref.path)
            sha = hashlib.sha256()
            with open(fp, "rb") as f:
                sha.update(f.read())
            assert sha.hexdigest() == hashes_before[ref.path], (
                f"finalize 不应改变已有 artifact：{ref.path}"
            )

        # 验证 CRE shadow 报告文件存在
        cre_path = os.path.join(pkg_dir, "validation", "cre_shadow_report.json")
        assert os.path.isfile(cre_path), "CRE shadow 报告文件应存在"

    def test_cre_provenance_manifest_hash_consistent(self):
        """CRE/provenance/Manifest hash 三者一致。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_consistent_")
        _, manifest_before, pkg_dir = _build_test_package(
            request_id="test_consistent", base_dir=tmpdir,
        )

        # 执行 finalize
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report(contract_hash="hash_consistency_test")
        result = finalizer.finalize("test_consistent", cre_report)
        assert result.success

        # 读取更新后的 manifest.json
        with open(os.path.join(pkg_dir, "manifest.json"), "r", encoding="utf-8") as f:
            manifest_dict = json.load(f)
        updated_manifest = ReviewPackageManifest.model_validate(manifest_dict)

        # 验证 manifest 中的 cre_shadow_report_hash
        assert updated_manifest.cre_shadow_report_hash != "", (
            "manifest 应包含 cre_shadow_report_hash"
        )
        assert updated_manifest.cre_shadow_report_hash == result.cre_shadow_report_hash, (
            "manifest 与 result 的 cre_shadow_report_hash 应一致"
        )

        # 验证 provenance.yml 中的 cre_shadow_report_hash
        with open(os.path.join(pkg_dir, "provenance.yml"), "r", encoding="utf-8") as f:
            provenance_text = f.read()
        assert f'cre_shadow_report_hash: "{result.cre_shadow_report_hash}"' in provenance_text, (
            "provenance.yml 应包含正确的 cre_shadow_report_hash"
        )

        # 验证 CRE 报告文件哈希
        cre_path = os.path.join(pkg_dir, "validation", "cre_shadow_report.json")
        cre_file_hash = hashlib.sha256()
        with open(cre_path, "rb") as f:
            cre_file_hash.update(f.read())
        assert cre_file_hash.hexdigest() == result.cre_shadow_report_hash, (
            "磁盘上的 CRE 文件哈希应与 manifest 声明一致"
        )

    def test_tampered_manifest_rejected(self):
        """Manifest 被篡改后 finalize 拒绝。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_tamper_m_")
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_tamper_m", base_dir=tmpdir,
        )

        # 篡改 manifest.json——修改 request_id（不改变 package_id 的输入）
        manifest_path = os.path.join(pkg_dir, "manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest_dict = json.load(f)
        # 保存原始 request_id 以便检查
        orig_request_id = manifest_dict["request_id"]
        manifest_dict["request_id"] = "TAMPERED_REQUEST_ID"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_dict, f, ensure_ascii=False, indent=2)

        # 执行 finalize——应被拒绝（request_id 不匹配）
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize(orig_request_id, cre_report)

        assert not result.success, "篡改 manifest 应被拒绝"
        assert result.audit_status == "INCOMPLETE", (
            f"篡改应导致 audit_status=INCOMPLETE，实际={result.audit_status}"
        )
        assert any("request_id 不匹配" in e for e in result.errors), (
            f"错误应提到 request_id 不匹配：{result.errors}"
        )

    def test_tampered_artifact_rejected(self):
        """Artifact 文件被篡改后 finalize 拒绝。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_tamper_a_")
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_tamper_a", base_dir=tmpdir,
        )

        # 篡改一个 artifact 文件——修改 developer_spec/raw.md
        raw_path = os.path.join(pkg_dir, "developer_spec", "raw.md")
        with open(raw_path, "a", encoding="utf-8") as f:
            f.write("\n# TAMPERED CONTENT\n")

        # 执行 finalize——应被拒绝
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize("test_tamper_a", cre_report)

        assert not result.success, "篡改 artifact 应被拒绝"
        assert result.audit_status == "INCOMPLETE"
        assert any("哈希验证失败" in e or "哈希不匹配" in e for e in result.errors), (
            f"错误应提到哈希验证失败：{result.errors}"
        )

    def test_finalize_failure_visible(self):
        """Finalize 失败时 audit_status=INCOMPLETE，错误在结果中可见。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_failvis_")

        # 不构建 package，直接 finalize 不存在的目录
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize("nonexistent_request", cre_report)

        assert not result.success
        assert result.audit_status == "INCOMPLETE", (
            f"失败应 audit_status=INCOMPLETE，实际={result.audit_status}"
        )
        assert len(result.errors) > 0, "失败应有错误信息"
        assert result.cre_shadow_report_hash == "", "失败时不应有 CRE hash"

    def test_repeated_finalize_idempotent(self):
        """重复 finalize 幂等——相同输入 → 相同输出。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_idem_")
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_idempotent", base_dir=tmpdir,
        )

        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report(contract_hash="idempotent_test")

        # 第一次 finalize
        result1 = finalizer.finalize("test_idempotent", cre_report)
        assert result1.success

        # 第二次 finalize——完全相同输入
        result2 = finalizer.finalize("test_idempotent", cre_report)
        assert result2.success, f"幂等 finalize 应成功：{result2.errors}"

        # 验证结果一致
        assert result2.cre_shadow_report_hash == result1.cre_shadow_report_hash, (
            "幂等 finalize 的 CRE hash 应一致"
        )
        assert result2.artifacts_after == result1.artifacts_after, (
            "幂等 finalize 不应增加 artifact 计数"
        )

        # 验证磁盘上的 manifest 未改变
        with open(os.path.join(pkg_dir, "manifest.json"), "r", encoding="utf-8") as f:
            manifest_dict = json.load(f)
        assert manifest_dict["cre_shadow_report_hash"] == result1.cre_shadow_report_hash

    def test_multi_statement_sql_unchanged(self):
        """多语句 SQL 在 finalize 前后不变。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_multisql_")
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_multisql", base_dir=tmpdir,
        )

        # 读取 finalize 前 SQL
        sql_path = os.path.join(pkg_dir, "sql", "main.sql")
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_before = f.read()

        # 执行 finalize
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize("test_multisql", cre_report)
        assert result.success

        # 验证 SQL 文件不变
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_after = f.read()
        assert sql_after == sql_before, "finalize 不应改变 SQL artifact"

    def test_request_id_mismatch_rejected(self):
        """request_id 不匹配被拒绝——包的 request_id 与传入不一致。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_reqid_")
        real_id = "real_req"
        # 用 request_id=real_id 构建
        _, _manifest, pkg_dir = _build_test_package(
            request_id=real_id, base_dir=tmpdir,
        )

        # 用错误的 request_id 调用 finalize——目录不存在
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report()
        result = finalizer.finalize("fake_req", cre_report)

        assert not result.success, (
            f"不存在的 request_id 应被拒绝：{result.errors}"
        )
        assert any(
            "目录不存在" in e or "request_id 不匹配" in e
            for e in result.errors
        ), f"错误应提到目录不存在或 request_id 不匹配：{result.errors}"


# ════════════════════════════════════════════
# E2E 测试：CreHarnessRunner
# ════════════════════════════════════════════


class TestHarnessRunnerE2E:
    """CreHarnessRunner 端到端测试。"""

    def test_harness_runner_scan_empty_dir(self):
        """Harness runner 扫描空目录——返回空聚合。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_empty_")
        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        assert report.total_samples == 0
        assert not report.passes_admission, "空目录不应通过准入"

    def test_harness_runner_processes_package_with_cre(self):
        """Harness runner 正确处理包含 CRE 报告的 Package。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_pkg_")
        # 构建带 CRE 报告的 Package
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_harness_pkg", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.CONSISTENT,
        )

        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        assert report.total_samples == 1, f"应有 1 个样本，实际={report.total_samples}"
        assert report.executable_total == 1, "应有 1 个可执行样本"
        assert report.cre_consistent_count == 1, "CRE 状态应为 CONSISTENT"

    def test_harness_runner_detects_tampered_package(self):
        """Harness runner 检测被篡改的 Package。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_tamper_")
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_harness_tamper", base_dir=tmpdir, include_cre=True,
        )

        # 篡改 manifest.json
        manifest_path = os.path.join(pkg_dir, "manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 修改 artifact 哈希但不改 manifest hash（模拟不一致篡改）
        if data.get("artifacts"):
            data["artifacts"][0]["sha256"] = "deadbeef" * 8
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        # 篡改后 manifest 模型校验可能失败，或哈希验证失败
        # 这两种情况都应导致样本数为 0 或所有样本 diagnostic_available=False
        executable = [s for s in report.samples if s.diagnostic_available]
        assert len(executable) == 0, (
            f"篡改后的 Package 应无可执行样本，实际={len(executable)}"
        )

    def test_golden_registry_zero_false_negative(self):
        """Golden 已知差异零假阴性——CRE 正确检出 golden MISMATCH。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_golden_fn_")

        # 构建 Package 包含 MISMATCH 的 CRE 报告
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_golden_fn", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.MISMATCH,
        )

        # 创建 golden registry——声明期望 MISMATCH
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {
            "version": "1.0.0",
            "entries": [{
                "contract_hash": "test_contract_hash",
                "scenario_id": "test_golden_fn",
                "golden_label": "MISMATCH",
            }],
        }
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        registry = GoldenRegistry(registry_path)
        runner = CreHarnessRunner(tmpdir, registry)
        report = runner.run()

        # 验证 golden 统计
        assert report.golden_total == 1, "应有 1 个 golden 样本"
        assert report.total_known_differences == 1, "应有 1 个已知差异"
        assert report.false_negative_count == 0, (
            f"已知差异应零假阴性，实际={report.false_negative_count}"
        )
        assert report.false_negative_rate == 0.0, "假阴性率应为 0"

    def test_golden_false_negative_detected(self):
        """Golden 假阴性被正确检测——golden=MISMATCH 但 CRE=CONSISTENT。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_golden_fn2_")

        # 构建 Package 包含 CONSISTENT 的 CRE 报告（但 golden 期望 MISMATCH）
        _, _manifest, pkg_dir = _build_test_package(
            request_id="test_golden_fn2", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.CONSISTENT,
        )

        # 创建 golden registry——期望 MISMATCH，但 CRE 判 CONSISTENT
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {
            "version": "1.0.0",
            "entries": [{
                "contract_hash": "test_contract_hash",
                "scenario_id": "test_golden_fn2",
                "golden_label": "MISMATCH",
            }],
        }
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        registry = GoldenRegistry(registry_path)
        runner = CreHarnessRunner(tmpdir, registry)
        report = runner.run()

        assert report.total_known_differences == 1
        assert report.false_negative_count == 1, (
            f"应为 1 个假阴性（golden=MISMATCH 但 CRE=CONSISTENT），"
            f"实际={report.false_negative_count}"
        )

    def test_harness_aggregation_immutable(self):
        """Harness 聚合报告不可变——相同输入两次 run() 返回相同指标。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_imm_")
        _build_test_package(
            request_id="test_immutable", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.CONSISTENT,
        )

        runner = CreHarnessRunner(tmpdir)
        report1 = runner.run()
        report2 = runner.run()

        # 验证两次 run 结果一致
        assert report2.total_samples == report1.total_samples
        assert report2.executable_total == report1.executable_total
        assert report2.executable_consistency_rate == report1.executable_consistency_rate
        assert report2.cre_legacy_conflict_count == report1.cre_legacy_conflict_count

    def test_harness_runner_skips_packages_without_manifest(self):
        """Harness runner 跳过无 manifest.json 的目录。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_nomf_")
        # 创建一个目录但没有 manifest.json
        empty_dir = os.path.join(tmpdir, "empty_package")
        os.makedirs(empty_dir, exist_ok=True)

        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        assert report.total_samples == 0, "无 manifest 的目录应被跳过"

    def test_harness_runner_human_review_tracking(self):
        """Harness runner 正确追踪 HUMAN_REVIEW 样本。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_hr_")
        _build_test_package(
            request_id="test_hr", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.HUMAN_REVIEW,
        )

        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        assert report.cre_human_review_count == 1, (
            f"HUMAN_REVIEW 样本数应为 1，实际={report.cre_human_review_count}"
        )

    def test_harness_runner_not_executed_tracking(self):
        """Harness runner 正确追踪 NOT_EXECUTED 样本。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_harness_ne_")
        _build_test_package(
            request_id="test_ne", base_dir=tmpdir, include_cre=True,
            cre_status=CreShadowStatus.NOT_EXECUTED,
        )

        runner = CreHarnessRunner(tmpdir)
        report = runner.run()

        assert report.not_executed_count == 1, (
            f"NOT_EXECUTED 样本数应为 1，实际={report.not_executed_count}"
        )

    def test_full_e2e_build_finalize_harness(self):
        """全链路 E2E：构建 Package → Finalize → Harness 扫描。

        验证完整流程：Packager 构建 → Finalizer 追加 CRE → Harness 验证哈希并聚合。
        """
        tmpdir = tempfile.mkdtemp(prefix="tianshu_full_e2e_")

        # Step 1：构建 Package（不带 CRE）
        packager, manifest_before, pkg_dir = _build_test_package(
            request_id="full_e2e", base_dir=tmpdir, include_cre=False,
        )

        # Step 2：Finalize——追加 CRE 报告
        finalizer = ReviewPackageFinalizer(tmpdir)
        cre_report = _make_cre_report(contract_hash="full_e2e_contract")
        result = finalizer.finalize("full_e2e", cre_report)
        assert result.success, f"Finalize 应成功：{result.errors}"

        # Step 3：验证 CRE 文件存在
        cre_path = os.path.join(pkg_dir, "validation", "cre_shadow_report.json")
        assert os.path.isfile(cre_path)

        # Step 4：创建 golden registry
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {
            "version": "1.0.0",
            "entries": [{
                "contract_hash": "full_e2e_contract",
                "scenario_id": "full_e2e",
                "golden_label": "CONSISTENT",
            }],
        }
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        # Step 5：Harness 扫描
        registry = GoldenRegistry(registry_path)
        runner = CreHarnessRunner(tmpdir, registry)
        harness_report = runner.run()

        # Step 6：验证 Harness 结果
        assert harness_report.total_samples == 1
        assert harness_report.executable_total == 1
        assert harness_report.executable_consistency_rate == 1.0, (
            f"可执行样本一致率应为 100%，实际={harness_report.executable_consistency_rate}"
        )
        assert harness_report.golden_total == 1
        assert harness_report.false_negative_count == 0
        assert harness_report.cre_legacy_conflict_count == 0

        # 准入检查
        # 注意：total_known_differences=0（golden_label=CONSISTENT），所以 passes_admission=False
        # 这符合预期——需要有 golden MISMATCH 样本才能验证判别能力
        # 一致率 100% + 零假阴性 + 零冲突
        assert harness_report.executable_consistency_rate >= 1.0
        assert harness_report.false_negative_rate <= 0.0
        assert harness_report.cre_legacy_conflict_count == 0


# ════════════════════════════════════════════
# E2E 测试：GoldenRegistry
# ════════════════════════════════════════════


class TestGoldenRegistry:
    """GoldenRegistry 单元测试。"""

    def test_load_valid_registry(self):
        """加载有效 golden registry。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_registry_")
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {
            "version": "1.0.0",
            "entries": [
                {
                    "contract_hash": "abc123",
                    "scenario_id": "case_01",
                    "golden_label": "CONSISTENT",
                },
                {
                    "contract_hash": "def456",
                    "scenario_id": "case_02",
                    "golden_label": "MISMATCH",
                },
            ],
        }
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        registry = GoldenRegistry(registry_path)
        assert registry.version == "1.0.0"
        assert registry.entry_count == 2

        entry1 = registry.lookup("abc123")
        assert entry1 is not None
        assert entry1.golden_label == CreShadowStatus.CONSISTENT

        entry2 = registry.lookup("def456")
        assert entry2 is not None
        assert entry2.golden_label == CreShadowStatus.MISMATCH

        assert registry.lookup("nonexistent") is None

    def test_load_empty_registry(self):
        """加载空 golden registry。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_registry_e_")
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {"version": "1.0.0", "entries": []}
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        registry = GoldenRegistry(registry_path)
        assert registry.entry_count == 0

    def test_load_missing_registry(self):
        """加载不存在的 golden registry——不抛异常。"""
        registry = GoldenRegistry("/nonexistent/path/golden_registry.json")
        assert registry.entry_count == 0

    def test_skip_invalid_golden_label(self):
        """跳过无效 golden_label 的条目。"""
        tmpdir = tempfile.mkdtemp(prefix="tianshu_registry_inv_")
        registry_path = os.path.join(tmpdir, "golden_registry.json")
        registry_data = {
            "version": "1.0.0",
            "entries": [
                {
                    "contract_hash": "valid",
                    "scenario_id": "case_ok",
                    "golden_label": "CONSISTENT",
                },
                {
                    "contract_hash": "invalid",
                    "scenario_id": "case_bad",
                    "golden_label": "NOT_A_REAL_STATUS",
                },
            ],
        }
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry_data, f)

        registry = GoldenRegistry(registry_path)
        assert registry.entry_count == 1, (
            f"无效 golden_label 的条目应跳过，实际={registry.entry_count}"
        )
        assert registry.lookup("valid") is not None
        assert registry.lookup("invalid") is None
