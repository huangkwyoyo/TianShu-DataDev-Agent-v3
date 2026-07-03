"""Phase 7A 统一验证报告——Phase 7 逻辑+物理链路的总览报告。

Phase 7A 范围：
- 逻辑链路结果（LOGIC_EQUIVALENT / LOGIC_MISMATCH / LOGIC_UNSUPPORTED / NOT_EXECUTED）
- 物理链路始终标记 NOT_EXECUTED（Phase 7B 才启动物理引擎）
- overall_status 精确反映双链路实际状态
- 禁止使用 "PASS" / "Go" / "No-Go" 等泛化状态名
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparisonReport,
)

# ════════════════════════════════════════════
# VerificationOverallStatus——统一验证状态
# ════════════════════════════════════════════


class VerificationOverallStatus(str, Enum):
    """Phase 7 统一验证报告的整体状态。

    禁止使用泛化名——每个状态精确描述双链路实际结论。
    """

    ALL_CONSISTENT = "ALL_CONSISTENT"
    """逻辑等价 + 物理一致——可直接进入 REVIEW_READY（Phase 7B+）。"""

    LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED = "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"
    """逻辑等价但物理链路未执行——Phase 7A 的期望终态。"""

    LOGIC_CONSISTENT_PHYSICAL_MISMATCH = "LOGIC_CONSISTENT_PHYSICAL_MISMATCH"
    """逻辑等价但物理结果不一致——需要 REPAIR_NEEDED（Phase 7B+）。"""

    LOGIC_MISMATCH = "LOGIC_MISMATCH"
    """逻辑链路不等价——应停止并进入 REPAIR_NEEDED。"""

    LOGIC_UNSUPPORTED = "LOGIC_UNSUPPORTED"
    """存在不支持对比的 step 类型——标记 HUMAN_REVIEW。"""

    NOT_EXECUTED = "NOT_EXECUTED"
    """逻辑链路未执行——物理链路自动 NOT_EXECUTED。"""

    REPAIR_NEEDED = "REPAIR_NEEDED"
    """需要返工修复（逻辑或物理不一致）。"""

    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"
    """需要人工审查——不可自动判定。"""


# ════════════════════════════════════════════
# PhysicalVerificationStatus（预留——Phase 7B 实现）
# ════════════════════════════════════════════


class PhysicalVerificationStatus(str, Enum):
    """物理链路验证状态——Phase 7B 启用，Phase 7A 始终 NOT_EXECUTED。"""

    RESULT_CONSISTENT = "RESULT_CONSISTENT"         # 双引擎结果一致
    RESULT_MISMATCH = "RESULT_MISMATCH"             # 结果不一致
    EXECUTION_FAILED = "EXECUTION_FAILED"           # 执行失败
    NOT_EXECUTED = "NOT_EXECUTED"                   # 尚未执行物理验证
    HUMAN_REVIEW = "HUMAN_REVIEW"                    # 需要人工审查


# ════════════════════════════════════════════
# UnifiedVerificationReport
# ════════════════════════════════════════════


class PhysicalVerificationReport(StrictModel):
    """物理验证报告——Phase 7B 实现，Phase 7A 仅占位。"""

    report_id: str = ""
    status: PhysicalVerificationStatus = PhysicalVerificationStatus.NOT_EXECUTED
    reason: str = "物理链路尚未执行——Phase 7B 实现"


class UnifiedVerificationReport(StrictModel):
    """Phase 7 统一验证报告——Phase 8 编排层消费。

    包含逻辑链路和物理链路的完整结果。
    overall_status 基于状态流转规则自动推导。

    Phase 7A 范围：
    - logic_status 可为 LOGIC_EQUIVALENT / LOGIC_MISMATCH / LOGIC_UNSUPPORTED / NOT_EXECUTED
    - physical_status 始终 NOT_EXECUTED
    - overall_status 为 LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED（逻辑等价）或 LOGIC_MISMATCH/NOT_EXECUTED
    """

    report_id: str                                    # 报告唯一标识
    contract_hash: str                                # 来源 Contract hash
    snapshot_id: str | None = None                    # 关联快照 ID（Phase 7A 可选）
    logic_status: ComparisonStatus                    # 逻辑链路对比状态
    logic_detail: PlanComparisonReport | None = None  # 逻辑对比详情
    physical_status: PhysicalVerificationStatus = Field(
        default=PhysicalVerificationStatus.NOT_EXECUTED,
        description="物理链路验证状态——Phase 7A 始终 NOT_EXECUTED",
    )
    physical_detail: PhysicalVerificationReport | None = Field(
        default=None,
        description="物理验证详情——Phase 7B 实现",
    )
    overall_status: VerificationOverallStatus = Field(
        default=VerificationOverallStatus.NOT_EXECUTED,
        description="整体验证状态——基于逻辑+物理状态自动推导",
    )
    requires_human_review: bool = False               # 是否需要人工审查
    repair_attempts_remaining: int = 2                # 剩余返工次数（上限 2 轮）

    @staticmethod
    def generate_report_id(contract_hash: str, logic_report_id: str) -> str:
        """生成确定性报告 ID。

        Args:
            contract_hash: 来源 Contract hash
            logic_report_id: 逻辑对比报告 ID

        Returns:
            "vrpt_{hash前12位}" 格式的报告 ID
        """
        payload = {
            "contract_hash": contract_hash,
            "logic_report_id": logic_report_id,
            "phase": "7A",
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"vrpt_{hash_hex}"

    @staticmethod
    def derive_overall_status(
        logic_status: ComparisonStatus,
        physical_status: PhysicalVerificationStatus,
    ) -> VerificationOverallStatus:
        """根据状态流转规则推导整体验证状态。

        规则：
        - 逻辑链路 NOT_EXECUTED → 整体 NOT_EXECUTED
        - 逻辑链路 NOT_COVERED → 整体 LOGIC_UNSUPPORTED（当前无法完整验证，需后续 Phase 覆盖）
        - 逻辑链路 LOGIC_MISMATCH → 整体 LOGIC_MISMATCH
        - 逻辑链路 LOGIC_UNSUPPORTED → 整体 LOGIC_UNSUPPORTED
        - 逻辑链路 LOGIC_EQUIVALENT + 物理 NOT_EXECUTED → LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
        - 逻辑链路 LOGIC_EQUIVALENT + 物理 RESULT_CONSISTENT → ALL_CONSISTENT
        - 逻辑链路 LOGIC_EQUIVALENT + 物理 RESULT_MISMATCH → LOGIC_CONSISTENT_PHYSICAL_MISMATCH
        - 逻辑链路 LOGIC_EQUIVALENT + 物理 HUMAN_REVIEW → HUMAN_REVIEW_REQUIRED

        Args:
            logic_status: 逻辑链路对比状态
            physical_status: 物理链路验证状态

        Returns:
            推导出的整体验证状态
        """
        # 逻辑链路未执行
        if logic_status == ComparisonStatus.NOT_EXECUTED:
            return VerificationOverallStatus.NOT_EXECUTED

        # 逻辑链路不等价
        if logic_status == ComparisonStatus.LOGIC_MISMATCH:
            return VerificationOverallStatus.LOGIC_MISMATCH

        # 逻辑链路不支持 / 本 Phase 未覆盖
        if logic_status in (ComparisonStatus.LOGIC_UNSUPPORTED, ComparisonStatus.NOT_COVERED):
            return VerificationOverallStatus.LOGIC_UNSUPPORTED

        # 逻辑链路等价——检查物理链路状态
        if logic_status == ComparisonStatus.LOGIC_EQUIVALENT:
            if physical_status == PhysicalVerificationStatus.NOT_EXECUTED:
                return VerificationOverallStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
            elif physical_status == PhysicalVerificationStatus.RESULT_CONSISTENT:
                return VerificationOverallStatus.ALL_CONSISTENT
            elif physical_status == PhysicalVerificationStatus.RESULT_MISMATCH:
                return VerificationOverallStatus.LOGIC_CONSISTENT_PHYSICAL_MISMATCH
            elif physical_status == PhysicalVerificationStatus.HUMAN_REVIEW:
                return VerificationOverallStatus.HUMAN_REVIEW_REQUIRED

        return VerificationOverallStatus.NOT_EXECUTED


# ════════════════════════════════════════════
# 工厂函数——从 PlanComparator 结果构建统一报告
# ════════════════════════════════════════════


def build_verification_report(
    contract_hash: str,
    logic_report: PlanComparisonReport,
    snapshot_id: str | None = None,
) -> UnifiedVerificationReport:
    """从逻辑对比结果构建 Phase 7A 统一验证报告。

    物理链路自动标记 NOT_EXECUTED——Phase 7B 才启动物理引擎。

    Args:
        contract_hash: 来源 Contract hash
        logic_report: PlanComparator 产出的逻辑对比报告
        snapshot_id: 关联快照 ID（可选）

    Returns:
        UnifiedVerificationReport——physical_status 为 NOT_EXECUTED
    """
    report_id = UnifiedVerificationReport.generate_report_id(
        contract_hash, logic_report.report_id,
    )

    physical_status = PhysicalVerificationStatus.NOT_EXECUTED
    physical_detail = PhysicalVerificationReport(
        report_id=f"{report_id}_physical",
        status=physical_status,
        reason="物理链路尚未执行——Phase 7B 实现",
    )

    overall_status = UnifiedVerificationReport.derive_overall_status(
        logic_status=logic_report.status,
        physical_status=physical_status,
    )

    requires_human_review = (
        logic_report.status == ComparisonStatus.LOGIC_MISMATCH
        or logic_report.status == ComparisonStatus.LOGIC_UNSUPPORTED
        or logic_report.status == ComparisonStatus.NOT_COVERED
    )

    return UnifiedVerificationReport(
        report_id=report_id,
        contract_hash=contract_hash,
        snapshot_id=snapshot_id,
        logic_status=logic_report.status,
        logic_detail=logic_report,
        physical_status=physical_status,
        physical_detail=physical_detail,
        overall_status=overall_status,
        requires_human_review=requires_human_review,
        repair_attempts_remaining=2,
    )
