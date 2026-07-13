"""ReviewPackageFinalizer——原子化追加 CRE shadow 报告到已有 Review Package。

与旧的 _finalize_cre_shadow_package 方案的关键区别：
- 旧方案：从 _results 零散字段重建整个 Package（依赖内部缓存，脆弱）
- 新方案：打开磁盘上已有的 Package，验证全部 artifact 哈希，原子追加 CRE 报告

流程：
1. 定位已有 Package 目录（generated/review_packages/{request_id}/）
2. 读取 manifest.json，验证 request_id/package_id 身份
3. 逐文件验证 Manifest 中全部 artifact 的 SHA-256
4. 写入 validation/cre_shadow_report.json（原子写入——先写临时文件再 rename）
5. 更新 provenance.yml（追加 cre_shadow_report_hash）
6. 更新 manifest.json（追加 ArtifactRef + cre_shadow_report_hash）
7. 返回结构化 FinalizerResult
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile

from pydantic import Field

from tianshu_datadev.cre_models import CreShadowReport
from tianshu_datadev.developer_spec.models import StrictModel

from .models import ArtifactRef, ReviewPackageManifest

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════
# 结构化结果模型
# ════════════════════════════════════════════


class FinalizerResult(StrictModel):
    """ReviewPackageFinalizer 结构化返回结果。

    success=False 时，legacy 结论不变，但 CRE 必须标记
    diagnostic_available=False、audit_status=INCOMPLETE，
    并在 API/阶段报告中可见——禁止只写 warning。
    """

    success: bool
    package_id: str = ""
    request_id: str = ""
    cre_shadow_report_hash: str = ""
    # 变更前后 artifact 计数（用于验证未丢失旧 artifact）
    artifacts_before: int = 0
    artifacts_after: int = 0
    # 错误详情——失败时填充
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    # 已验证的哈希映射（path → sha256）——审计追踪
    verified_hashes: dict[str, str] = Field(default_factory=dict)
    # 审计状态——失败时 = INCOMPLETE
    audit_status: str = "COMPLETE"


# ════════════════════════════════════════════
# ReviewPackageFinalizer
# ════════════════════════════════════════════


class ReviewPackageFinalizer:
    """原子化追加 CRE shadow 报告到已有 Review Package。

    验证既有 Manifest 中全部 artifact 哈希及 request/package 身份后，
    以原子方式追加 validation/cre_shadow_report.json，
    更新 provenance、ArtifactRef、cre_shadow_report_hash 和 Manifest。

    不覆盖或丢失原 SQL、DeveloperSpec、Contract、trace、summary。
    覆盖单语句和多语句路径。

    Usage:
        finalizer = ReviewPackageFinalizer("generated/review_packages")
        result = finalizer.finalize("req_abc123", cre_shadow_report)
        if not result.success:
            # CRE 标记 diagnostic_available=False，阶段报告可见
            ...
    """

    # CRE shadow 报告在 Package 中的固定路径
    CRE_SHADOW_PATH = "validation/cre_shadow_report.json"

    def __init__(self, base_output_dir: str = "generated/review_packages"):
        """初始化 Finalizer。

        Args:
            base_output_dir: Review Package 输出根目录（与 ReviewPackageBuilder 一致）
        """
        self._base_dir = base_output_dir

    # ── 公共 API ──

    def finalize(
        self,
        request_id: str,
        cre_shadow_report: CreShadowReport,
    ) -> FinalizerResult:
        """原子追加 CRE shadow 报告到指定 request 的 Review Package。

        验证 → 写入 → 更新清单 → 返回结构化结果。
        任何步骤失败立即返回 FinalizerResult(success=False)。

        Args:
            request_id: Pipeline 请求 ID（与 Package 目录名一致）
            cre_shadow_report: CRE shadow 诊断报告（严格 Pydantic 模型）

        Returns:
            FinalizerResult——success=True 表示原子追加完成
        """
        result = FinalizerResult(request_id=request_id, success=False)

        try:
            # Step 1：定位并验证 Package 目录存在
            package_dir = os.path.join(self._base_dir, request_id)
            if not os.path.isdir(package_dir):
                return self._fail(
                    result,
                    f"Package 目录不存在：{package_dir}——"
                    "请先执行 SQL 管线构建 Review Package",
                )

            # Step 2：读取并反序列化 manifest.json
            manifest_path = os.path.join(package_dir, "manifest.json")
            if not os.path.isfile(manifest_path):
                return self._fail(
                    result,
                    f"manifest.json 不存在：{manifest_path}——"
                    "Package 可能由旧版 packager 构建，缺少 manifest 持久化",
                )

            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest_dict = json.load(f)
            except json.JSONDecodeError as e:
                return self._fail(result, f"manifest.json JSON 解析失败：{e}")

            try:
                manifest = ReviewPackageManifest.model_validate(manifest_dict)
            except Exception as e:
                return self._fail(result, f"manifest.json 模型校验失败：{e}")

            result.package_id = manifest.package_id
            result.artifacts_before = len(manifest.artifacts)

            # Step 3：验证 request_id/package_id 身份一致
            if manifest.request_id != request_id:
                return self._fail(
                    result,
                    f"request_id 不匹配：manifest 中为 '{manifest.request_id}'，"
                    f"传入为 '{request_id}'",
                )

            expected_package_id = ReviewPackageManifest.generate_package_id(request_id)
            if manifest.package_id != expected_package_id:
                return self._fail(
                    result,
                    f"package_id 不匹配：manifest 中为 '{manifest.package_id}'，"
                    f"期望为 '{expected_package_id}'（从 request_id 推导）",
                )

            # Step 4：逐文件验证 Manifest 中全部 artifact 的 SHA-256
            hash_errors = self._verify_all_artifact_hashes(
                package_dir, manifest.artifacts,
            )
            if hash_errors:
                result.verified_hashes = {
                    a.path: a.sha256 for a in manifest.artifacts
                }
                return self._fail(
                    result,
                    "Artifact 哈希验证失败——Package 可能已被篡改：\n- "
                    + "\n- ".join(hash_errors),
                )

            # 记录已验证的哈希（审计追踪）
            result.verified_hashes = {
                a.path: a.sha256 for a in manifest.artifacts
            }

            # Step 5：检查 CRE shadow 报告是否已存在（幂等）
            cre_path = os.path.join(package_dir, self.CRE_SHADOW_PATH)
            cre_json, cre_sha256 = self._serialize_cre_report(cre_shadow_report)

            if os.path.isfile(cre_path):
                # 比对已有文件哈希——相同则幂等返回
                existing_hash = self._compute_file_sha256(cre_path)
                if existing_hash == cre_sha256:
                    logger.info(
                        "CRE shadow 报告已存在且哈希一致——幂等跳过：%s", cre_path,
                    )
                    result.success = True
                    result.cre_shadow_report_hash = cre_sha256
                    result.artifacts_after = result.artifacts_before
                    return result
                else:
                    result.warnings.append(
                        "CRE shadow 报告已存在但哈希不一致——将覆盖更新"
                    )

            # Step 6：原子写入 CRE shadow 报告（先写临时文件，再 rename）
            self._atomic_write(cre_path, cre_json)

            # Step 7：更新 provenance.yml——追加 cre_shadow_report_hash
            provenance_path = os.path.join(package_dir, "provenance.yml")
            new_provenance_hash = self._update_provenance_with_cre_hash(
                provenance_path, cre_sha256,
            )

            # Step 8：更新 manifest.json——追加 ArtifactRef + cre_shadow_report_hash
            cre_artifact_ref = ArtifactRef(
                path=self.CRE_SHADOW_PATH, sha256=cre_sha256,
            )
            manifest.artifacts.append(cre_artifact_ref)
            manifest.cre_shadow_report_hash = cre_sha256
            # 更新 provenance_hash（因 provenance.yml 已变更）
            manifest.provenance_hash = new_provenance_hash
            # 同步更新 provenance.yml 的 ArtifactRef（hash 已变更）
            for i, ref in enumerate(manifest.artifacts):
                if ref.path == "provenance.yml":
                    manifest.artifacts[i] = ArtifactRef(
                        path="provenance.yml", sha256=new_provenance_hash,
                    )
                    break

            result.artifacts_after = len(manifest.artifacts)

            # 原子写入更新后的 manifest.json
            new_manifest_json = json.dumps(
                manifest.model_dump(), ensure_ascii=False, indent=2, default=str,
            )
            self._atomic_write(manifest_path, new_manifest_json)

            # Step 9：全部成功
            result.success = True
            result.cre_shadow_report_hash = cre_sha256
            result.audit_status = "COMPLETE"
            logger.info(
                "CRE shadow finalize 成功：request_id=%s, package_id=%s, "
                "cre_hash=%s, artifacts: %d → %d",
                request_id, manifest.package_id, cre_sha256,
                result.artifacts_before, result.artifacts_after,
            )

        except Exception as e:
            return self._fail(result, f"Finalizer 异常：{e}")

        return result

    # ── 哈希验证 ──

    @staticmethod
    def _verify_all_artifact_hashes(
        package_dir: str, artifacts: list[ArtifactRef],
    ) -> list[str]:
        """验证 Manifest 中全部 artifact 的 SHA-256 与实际文件一致。

        Args:
            package_dir: Package 根目录
            artifacts: Manifest 中声明的 artifact 引用列表

        Returns:
            错误描述列表——空列表表示全部验证通过
        """
        errors: list[str] = []
        for ref in artifacts:
            file_path = os.path.join(package_dir, ref.path)
            if not os.path.isfile(file_path):
                errors.append(f"缺失：{ref.path}（Manifest 声明但文件不存在）")
                continue
            actual_hash = ReviewPackageFinalizer._compute_file_sha256(file_path)
            if actual_hash != ref.sha256:
                errors.append(
                    f"哈希不匹配：{ref.path}——"
                    f"期望={ref.sha256[:16]}...，实际={actual_hash[:16]}..."
                )
        return errors

    @staticmethod
    def _compute_file_sha256(file_path: str) -> str:
        """计算文件的 SHA-256 哈希。"""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    # ── CRE 报告序列化 ──

    @staticmethod
    def _serialize_cre_report(report: CreShadowReport) -> tuple[str, str]:
        """序列化 CRE shadow 报告为 JSON 字符串并计算 SHA-256。

        Returns:
            (json_string, sha256_hex)
        """
        cre_dict = report.model_dump()
        cre_json = json.dumps(cre_dict, ensure_ascii=False, indent=2, default=str)
        cre_sha256 = hashlib.sha256(cre_json.encode("utf-8")).hexdigest()
        return cre_json, cre_sha256

    # ── 原子写入 ──

    @staticmethod
    def _atomic_write(target_path: str, content: str) -> None:
        """原子写入——先写临时文件再 os.replace（同卷原子 rename）。

        确保：要么旧文件完整，要么新文件完整——不会出现半写状态。
        """
        tmp_fd, tmp_path = tempfile.mkstemp(
            suffix=".tmp",
            prefix=os.path.basename(target_path) + ".",
            dir=os.path.dirname(target_path),
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="\n") as f:
                f.write(content)
            os.replace(tmp_path, target_path)  # 同卷原子操作
        except Exception:
            # 清理临时文件
            if os.path.isfile(tmp_path):
                os.unlink(tmp_path)
            raise

    # ── Provenance 更新 ──

    @staticmethod
    def _update_provenance_with_cre_hash(
        provenance_path: str, cre_sha256: str,
    ) -> str:
        """更新 provenance.yml——追加/更新 cre_shadow_report_hash 行。

        保留原有所有内容，仅在末尾追加 CRE hash 行（如已存在则替换）。

        Returns:
            更新后 provenance.yml 的 SHA-256
        """
        if not os.path.isfile(provenance_path):
            raise FileNotFoundError(f"provenance.yml 不存在：{provenance_path}")

        with open(provenance_path, "r", encoding="utf-8") as f:
            content = f.read()

        cre_line = f'cre_shadow_report_hash: "{cre_sha256}"'

        # 如果已存在 cre_shadow_report_hash 行——原地替换
        if "\ncre_shadow_report_hash:" in content:
            import re
            content = re.sub(
                r'^cre_shadow_report_hash:.*$',
                cre_line,
                content,
                flags=re.MULTILINE,
            )
        else:
            # 追加到 provenance.yml 末尾（在文件最后一个非空行后）
            content = content.rstrip("\n") + "\n" + cre_line + "\n"

        # 原子写入更新后的 provenance.yml
        ReviewPackageFinalizer._atomic_write(provenance_path, content)

        new_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return new_hash

    # ── 内部工具 ──

    @staticmethod
    def _fail(result: FinalizerResult, error_msg: str) -> FinalizerResult:
        """填充失败结果——标记 audit_status=INCOMPLETE。"""
        result.success = False
        result.errors.append(error_msg)
        result.audit_status = "INCOMPLETE"
        logger.error("CRE shadow finalize 失败：%s", error_msg)
        return result
