"""CreHarnessRunner——跨请求 CRE Harness 执行器。

扫描 Review Package、验证 Manifest 哈希、严格反序列化 CRE 报告，
与 harness/datasets/regression/ 中版本化 golden 注册表关联，
自动计算一致率、假阴性、冲突、WARN、HUMAN_REVIEW、NOT_EXECUTED。

核心原则：
- 禁止外部手工注入统计值——所有指标由 aggregate() 内部计算
- 输出带输入包哈希的不可变聚合报告
- 聚合幂等——相同输入 → 相同输出
"""

from __future__ import annotations

import json
import logging
import os

from tianshu_datadev.artifacts.models import ReviewPackageManifest
from tianshu_datadev.cre_models import (
    CreAhsMetrics,
    CreHarnessAggregation,
    CreShadowReport,
    CreShadowStatus,
)

logger = logging.getLogger(__name__)


class GoldenRegistryEntry:
    """单个 golden 注册表条目——携带预期 CRE 状态标签。"""

    def __init__(
        self,
        contract_hash: str,
        scenario_id: str,
        golden_label: CreShadowStatus,
    ):
        self.contract_hash = contract_hash
        self.scenario_id = scenario_id
        self.golden_label = golden_label


class GoldenRegistry:
    """版本化 golden 注册表——从 JSON 文件加载。"""

    def __init__(self, registry_path: str):
        self._path = registry_path
        self._entries: list[GoldenRegistryEntry] = []
        self._version: str = "0.0.0"
        self._by_contract: dict[str, GoldenRegistryEntry] = {}
        self._load()

    @property
    def version(self) -> str:
        return self._version

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def lookup(self, contract_hash: str) -> GoldenRegistryEntry | None:
        """按 contract_hash 查找 golden 标签。"""
        return self._by_contract.get(contract_hash)

    def _load(self) -> None:
        """从 JSON 文件加载 golden 注册表。"""
        if not os.path.isfile(self._path):
            logger.warning(
                "Golden registry 文件不存在：%s——所有样本将视为非 golden",
                self._path,
            )
            return

        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._version = data.get("version", "0.0.0")

        for entry_data in data.get("entries", []):
            label_str = entry_data.get("golden_label", "NOT_EXECUTED")
            try:
                golden_label = CreShadowStatus(label_str)
            except ValueError:
                logger.warning(
                    "Golden registry 中无效的 golden_label '%s'——跳过",
                    label_str,
                )
                continue

            entry = GoldenRegistryEntry(
                contract_hash=entry_data.get("contract_hash", ""),
                scenario_id=entry_data.get("scenario_id", ""),
                golden_label=golden_label,
            )
            self._entries.append(entry)
            if entry.contract_hash:
                self._by_contract[entry.contract_hash] = entry


# ══════════════════════════════════════════════════════════════════════
# CreHarnessRunner
# ══════════════════════════════════════════════════════════════════════


class CreHarnessRunner:
    """跨请求 CRE Harness——扫描 Review Package、验证哈希、聚合指标。

    流程：
    1. 扫描 generated/review_packages/ 下所有子目录
    2. 对每个 Package：验证 Manifest 哈希 → 反序列化 CRE 报告 → 关联 golden 注册表
    3. 自动计算一致率、假阴性、冲突、WARN、HUMAN_REVIEW、NOT_EXECUTED
    4. 输出带输入包哈希的不可变聚合报告
    """

    def __init__(
        self,
        base_output_dir: str = "generated/review_packages",
        golden_registry: GoldenRegistry | None = None,
    ):
        """初始化 Harness Runner。

        Args:
            base_output_dir: Review Package 输出根目录
            golden_registry: Golden 注册表——为 None 时所有样本视为非 golden
        """
        self._base_dir = base_output_dir
        self._registry = golden_registry

    def run(self) -> CreHarnessAggregation:
        """扫描所有 Review Package，执行完整 Harness 验证。

        Returns:
            CreHarnessAggregation——含全部指标和准入判定
        """
        metrics_list: list[CreAhsMetrics] = []

        if not os.path.isdir(self._base_dir):
            logger.warning("Package 根目录不存在：%s", self._base_dir)
            aggregation = CreHarnessAggregation()
            aggregation.aggregate(metrics_list)
            return aggregation

        for entry in sorted(os.listdir(self._base_dir)):
            package_dir = os.path.join(self._base_dir, entry)
            if not os.path.isdir(package_dir):
                continue
            metrics = self._process_package(package_dir)
            if metrics is not None:
                metrics_list.append(metrics)

        aggregation = CreHarnessAggregation()
        aggregation.aggregate(metrics_list)
        logger.info(
            "CRE Harness 完成：%d 个样本，一致率=%.1f%%，"
            "假阴性=%d，冲突=%d，准入=%s",
            aggregation.total_samples,
            aggregation.executable_consistency_rate * 100,
            aggregation.false_negative_count,
            aggregation.cre_legacy_conflict_count,
            "通过" if aggregation.passes_admission else "不通过",
        )
        return aggregation

    def _process_package(self, package_dir: str) -> CreAhsMetrics | None:
        """处理单个 Review Package——验证 + 反序列化 + golden 关联。

        Returns:
            CreAhsMetrics——验证失败时返回 diagnostic_available=False 的指标
        """
        package_name = os.path.basename(package_dir)

        # 读取 manifest.json
        manifest_path = os.path.join(package_dir, "manifest.json")
        if not os.path.isfile(manifest_path):
            logger.warning("[%s] manifest.json 不存在——跳过", package_name)
            return None

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_dict = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("[%s] manifest.json 读取失败：%s", package_name, e)
            return None

        try:
            manifest = ReviewPackageManifest.model_validate(manifest_dict)
        except Exception as e:
            logger.warning(
                "[%s] manifest.json 模型校验失败：%s", package_name, e,
            )
            return None

        # 验证 Manifest 中全部 artifact 哈希
        hash_ok = self._verify_manifest_hashes(package_dir, manifest)
        if not hash_ok:
            return CreAhsMetrics(
                contract_hash=manifest.cre_shadow_report_hash or "UNKNOWN",
                scenario_id=package_name,
                cre_status=CreShadowStatus.ERROR,
                diagnostic_available=False,
                error_message="Manifest 哈希验证失败——Package 可能已被篡改",
            )

        # 读取 CRE shadow 报告
        cre_path = os.path.join(
            package_dir, "validation", "cre_shadow_report.json",
        )
        if not os.path.isfile(cre_path):
            logger.info(
                "[%s] CRE shadow 报告不存在——可能是旧 Package 无 CRE 诊断",
                package_name,
            )
            return CreAhsMetrics(
                contract_hash=manifest.cre_shadow_report_hash or "",
                scenario_id=package_name,
                cre_status=CreShadowStatus.NOT_EXECUTED,
                diagnostic_available=False,
                error_message="CRE shadow 报告不存在",
            )

        try:
            with open(cre_path, "r", encoding="utf-8") as f:
                cre_dict = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            return CreAhsMetrics(
                contract_hash=manifest.cre_shadow_report_hash or "",
                scenario_id=package_name,
                cre_status=CreShadowStatus.ERROR,
                diagnostic_available=False,
                error_message=f"CRE shadow 报告读取失败：{e}",
            )

        # 严格反序列化 CreShadowReport
        try:
            cre_report = CreShadowReport.model_validate(cre_dict)
        except Exception as e:
            return CreAhsMetrics(
                contract_hash=manifest.cre_shadow_report_hash or "",
                scenario_id=package_name,
                cre_status=CreShadowStatus.ERROR,
                diagnostic_available=False,
                error_message=f"CreShadowReport 反序列化失败：{e}",
            )

        # 关联 golden 注册表
        is_golden = False
        golden_label = None
        if self._registry is not None:
            reg_entry = self._registry.lookup(cre_report.contract_hash)
            if reg_entry is not None:
                is_golden = True
                golden_label = reg_entry.golden_label

        # 构建 CreAhsMetrics（所有字段从 CreShadowReport 和 golden 注册表计算）
        warn_rows = (
            cre_report.affected_row_count if cre_report.has_warnings else 0
        )
        return CreAhsMetrics(
            contract_hash=cre_report.contract_hash,
            scenario_id=package_name,
            cre_status=cre_report.cre_status,
            legacy_status=cre_report.legacy_status,
            status_consistent=cre_report.status_consistent,
            is_golden=is_golden,
            golden_label=golden_label,
            diagnostic_available=cre_report.diagnostic_available,
            total_rows=cre_report.total_rows,
            exact_match_rows=cre_report.exact_match_rows,
            tolerance_match_rows=cre_report.tolerance_match_rows,
            affected_row_count=cre_report.affected_row_count,
            has_warnings=cre_report.has_warnings,
            warn_affected_row_count=warn_rows,
            decision_reason=cre_report.decision_reason,
            error_message=cre_report.error_message,
        )

    @staticmethod
    def _verify_manifest_hashes(
        package_dir: str, manifest: ReviewPackageManifest,
    ) -> bool:
        """验证 Manifest 中全部 artifact 的 SHA-256 与实际文件一致。"""
        import hashlib as hl

        for ref in manifest.artifacts:
            file_path = os.path.join(package_dir, ref.path)
            if not os.path.isfile(file_path):
                logger.warning(
                    "Harness 哈希验证：%s 缺失", ref.path,
                )
                return False
            sha256 = hl.sha256()
            try:
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(65536), b""):
                        sha256.update(chunk)
            except OSError as e:
                logger.warning(
                    "Harness 哈希验证：%s 读取失败——%s", ref.path, e,
                )
                return False
            if sha256.hexdigest() != ref.sha256:
                logger.warning(
                    "Harness 哈希验证：%s 哈希不匹配", ref.path,
                )
                return False
        return True


# ══════════════════════════════════════════════════════════════════════
# 工厂函数
# ══════════════════════════════════════════════════════════════════════


def create_harness_runner(
    base_output_dir: str = "generated/review_packages",
    golden_registry_path: str = "harness/datasets/regression/golden_registry.json",
) -> CreHarnessRunner:
    """创建 Harness Runner 的便捷工厂——自动加载 golden 注册表。"""
    registry = (
        GoldenRegistry(golden_registry_path)
        if os.path.isfile(golden_registry_path)
        else None
    )
    return CreHarnessRunner(base_output_dir, registry)
