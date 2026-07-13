"""CRE 共享模型——无循环依赖的中立模块。

本模块位于 tianshu_datadev 根包级别（而非 spark/ 子包内），
完全独立于 spark/、artifacts/、api/ 等任何子包，从任何模块导入均不触发循环依赖。

包含 PhysicalVerifier、CRE 编码系统、PackageInputs 三方共用的模型：
- NormalizationColumn（列定义——消除 cre_encoding↔physical_verifier 循环依赖）
- EnvironmentManifest（引擎环境差异声明）
- CreShadowReport / CreShadowStatus（shadow 诊断报告）
- CreConfig（CRE 编码配置）
- CreAhsMetrics / CreHarnessAggregation（跨请求 Harness 聚合器）

仅依赖：tianshu_datadev.developer_spec.models.StrictModel

Usage:
    from tianshu_datadev.cre_models import (
        CreShadowReport, EnvironmentManifest, NormalizationColumn, ...
    )
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# NormalizationColumn——列定义（从中立模块导出，消除循环依赖）
# ════════════════════════════════════════════


class NormalizationColumn(StrictModel):
    """Contract output_columns 中的单列定义——用于规范化配置和 CRE 编码。

    原位置：physical_verifier.py。
    移至 cre_models.py 后，cre_encoding.py 不再需要导入 physical_verifier，
    解决了 CreShadowReport ↔ NormalizationColumn 的循环依赖。
    """

    column_name: str              # 列名
    data_type: str | None = None  # 数据类型（如 "double"、"decimal(18,2)"）


# ════════════════════════════════════════════
# 容差与差异模型
# ════════════════════════════════════════════


class ToleratedFieldDetail(StrictModel):
    """单个字段的容差内差异详情。

    affected_cell_count : 该列有容差内差异的单元格数（每行最多 1 个）
    total_rows          : 总比较行数（用于计算 affected_ratio）
    affected_ratio      : affected_cell_count / total_rows（每列每行最多 1 个单元格，≤100%）
    """
    column: str = ""
    data_type: str = ""
    rule_applied: str = ""
    affected_cell_count: int = 0
    total_rows: int = 0
    affected_ratio: float = 0.0
    max_abs_error: float | None = None
    max_rel_error: float | None = None
    samples: list[dict[str, Any]] = Field(default_factory=list)


class ToleratedDifferenceWarning(StrictModel):
    """容差内差异警告——全部差异在容差内时生成。

    tolerated_ratio      : 基于受影响唯一行数的比例（≤100%）
    affected_row_count   : 至少有一个容差内差异的唯—行数
    affected_cell_count  : 容差内单元格总数（可大于行数，因一行多列有差异）
    """
    action: str = "PASS_WITH_WARN"
    tolerated_ratio: float = 0.0
    tolerated_ratio_threshold: float = 0.05
    affected_row_count: int = 0
    affected_cell_count: int = 0
    field_details: list[ToleratedFieldDetail] = Field(default_factory=list)
    total_comparison_rows: int = 0
    total_out_of_tolerance: int = 0
    recommended_next_step: str = ""


# ════════════════════════════════════════════
# EnvironmentManifest——引擎间环境差异声明
# ════════════════════════════════════════════


class SpecialFloatStrategy(str, Enum):
    """特殊浮点值比较策略。

    EQUAL         : NaN==NaN, +Inf==+Inf, -Inf==-Inf
    MISMATCH      : 遇到特殊浮点值即视为差异
    HUMAN_REVIEW  : 无法自动判定（保守）
    UNKNOWN       : 未声明策略——行为同 HUMAN_REVIEW（保守回退），但标记为未配置
    """
    EQUAL = "EQUAL"
    MISMATCH = "MISMATCH"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    UNKNOWN = "UNKNOWN"


class DecimalStrategy(str, Enum):
    """Decimal 比较策略——双引擎 Decimal 精度差异处理。

    EXACT    : 逐字节精确比较（要求双引擎 Decimal 编码完全一致）
    QUANTIZE : 按 Contract scale 量化后比较（容忍尾数差异）
    UNKNOWN  : 未声明策略——禁止自动判定，回退 HUMAN_REVIEW
    """
    EXACT = "EXACT"
    QUANTIZE = "QUANTIZE"
    UNKNOWN = "UNKNOWN"


class NullStrategy(str, Enum):
    """NULL 值比较策略——双引擎 NULL 语义差异处理。

    EQUAL    : NULL == NULL（与 SQL 三值逻辑一致）
    MISMATCH : NULL 与其他值比较时始终标记为差异
    UNKNOWN  : 未声明策略——禁止自动判定，回退 HUMAN_REVIEW
    """
    EQUAL = "EQUAL"
    MISMATCH = "MISMATCH"
    UNKNOWN = "UNKNOWN"


class EnvironmentManifest(StrictModel):
    """环境清单——显式声明双引擎环境差异配置。

    Pipeline 必须显式构造并传入 PhysicalVerifier/CRE，
    禁止 CRE 内部使用默认值猜测环境配置。

    字段说明：
    - duckdb_version / spark_version : 引擎版本标识（可检测——用于审计追溯）
    - timezone : 时区标识（如 "Asia/Shanghai"）——从 Contract 提取
    - ansi_sql : 是否启用 ANSI SQL 模式——UNKNOWN 表示无法证明（禁止猜测）
    - case_sensitive_compare : 字符串比较是否区分大小写——UNKNOWN 表示无法证明
    - decimal_strategy : Decimal 精度差异处理策略——UNKNOWN 表示无法证明
    - null_strategy : NULL 值语义策略——UNKNOWN 表示无法证明
    - nan_handling / pos_inf_handling / neg_inf_handling : 特殊浮点值策略——UNKNOWN 表示无法证明
    """
    # 引擎版本——可实际检测
    duckdb_version: str = ""
    spark_version: str = ""
    # 时区——从 Contract 提取（无法提取时为空）
    timezone: str = ""
    # SQL 方言配置——无法自动检测的字段默认为 UNKNOWN
    ansi_sql: bool | None = None                      # None = 无法证明
    case_sensitive_compare: bool | None = None         # None = 无法证明
    # 数值策略——默认 UNKNOWN：强制调用方显式声明，禁止依赖默认猜测
    decimal_strategy: DecimalStrategy = DecimalStrategy.UNKNOWN
    # NULL 策略——默认 UNKNOWN：强制调用方显式声明
    null_strategy: NullStrategy = NullStrategy.UNKNOWN
    # 特殊浮点值策略——默认 UNKNOWN：强制调用方显式声明
    nan_handling: SpecialFloatStrategy = SpecialFloatStrategy.UNKNOWN
    pos_inf_handling: SpecialFloatStrategy = SpecialFloatStrategy.UNKNOWN
    neg_inf_handling: SpecialFloatStrategy = SpecialFloatStrategy.UNKNOWN

    @property
    def has_any_unknown_strategy(self) -> bool:
        """是否有任何关键策略字段为 UNKNOWN——触发 HUMAN_REVIEW 的必要条件。"""
        return (
            self.decimal_strategy == DecimalStrategy.UNKNOWN
            or self.null_strategy == NullStrategy.UNKNOWN
            or self.nan_handling == SpecialFloatStrategy.UNKNOWN
            or self.pos_inf_handling == SpecialFloatStrategy.UNKNOWN
            or self.neg_inf_handling == SpecialFloatStrategy.UNKNOWN
        )


# ════════════════════════════════════════════
# CreShadowReport——CRE shadow 诊断报告（严格模型）
# ════════════════════════════════════════════


class CreShadowStatus(str, Enum):
    """CRE shadow 诊断状态枚举。"""
    CONSISTENT = "CONSISTENT"
    CONSISTENT_WITH_WARN = "CONSISTENT_WITH_WARN"
    MISMATCH = "MISMATCH"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    NOT_EXECUTED = "NOT_EXECUTED"
    ERROR = "ERROR"


class CreShadowWarning(StrictModel):
    """CRE shadow 警告条目——严格模型，无 dict 逃生口。"""
    action: str = ""
    tolerated_ratio: float = 0.0
    affected_row_count: int = 0
    affected_cell_count: int = 0
    total_comparison_rows: int = 0
    field_details: list[ToleratedFieldDetail] = Field(default_factory=list)


class CreShadowReport(StrictModel):
    """CRE shadow 诊断报告——严格 Pydantic 模型，extra="forbid"。

    包含完整的 CRE 诊断结果，用于与 legacy status 对比。
    所有字段均为必需，调用方必须显式填充。
    """
    # 诊断可用性
    diagnostic_available: bool = False
    # 溯源标识
    contract_hash: str = ""
    snapshot_id: str = ""
    # CRE 状态（封闭枚举——禁止任意字符串）
    cre_status: CreShadowStatus = CreShadowStatus.NOT_EXECUTED
    mapped_status: str = ""                      # 映射后的 legacy 状态
    # 对照
    legacy_status: str = ""                      # 生产 verify() 的原始状态
    status_consistent: bool = False              # CRE 与 legacy 结论是否一致
    human_review_recommended: bool = False       # 是否建议人工审查
    # 警告
    has_warnings: bool = False
    warnings: list[CreShadowWarning] = Field(default_factory=list)
    # 统计
    total_rows: int = 0
    exact_match_rows: int = 0
    tolerance_match_rows: int = 0
    affected_row_count: int = 0
    mismatched_bucket_count: int = 0
    # 判定原因
    decision_reason: str = ""
    # 错误信息（NOT_EXECUTED / ERROR 时的详细原因）
    error_message: str = ""


# ════════════════════════════════════════════
# CreConfig——CRE 原型配置
# ════════════════════════════════════════════


class CreConfig(StrictModel):
    """CRE 原型配置——从 NormalizationConfig + 主键配置合成。

    output_columns : Contract output_columns（列顺序决定 CRE 编码列序）
    primary_keys   : 归一化后的权威主键列名（用于行对齐和分桶）
    float_abs_tol  : math.isclose abs_tol（仅 float/double）
    float_rel_tol  : math.isclose rel_tol（仅 float/double）
    num_buckets    : 诊断分桶数（2^n 推荐）
    timezone       : 时区标识，空字符串表示不支持 timestamp
    """
    output_columns: list[NormalizationColumn] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    float_abs_tolerance: float = 1e-12
    float_rel_tolerance: float = 1e-9
    num_buckets: int = 64
    timezone: str = ""
    environment_manifest: EnvironmentManifest | None = None


# ════════════════════════════════════════════
# CRE Harness 聚合器——跨请求指标聚合
# ════════════════════════════════════════════


class CreAhsMetrics(StrictModel):
    """单个 AHS（Abstract Harness Sample）的 CRE 指标快照。

    用于跨请求聚合——每个 Contract+场景作为一个独立样本。

    golden_label 和 is_golden 字段驱动零容忍 Harness 验证：
    - is_golden=True 的样本携带已知预期（golden_label），用于计算假阴性率和 CRE/legacy 冲突
    - 禁止外部手工赋统计值——所有指标由 CreHarnessAggregation.aggregate() 内部计算
    """

    # 样本标识
    contract_hash: str = ""
    scenario_id: str = ""
    # CRE 状态
    cre_status: CreShadowStatus = CreShadowStatus.NOT_EXECUTED
    legacy_status: str = ""
    status_consistent: bool = False
    # ── Golden 样本标签（跨请求 Harness 零容忍验证）──
    is_golden: bool = False                     # 是否携带已知预期标签
    golden_label: CreShadowStatus | None = None  # 预期 CRE 状态（仅 is_golden=True 时有效）
    # 可执行性
    diagnostic_available: bool = False
    # 行数统计
    total_rows: int = 0
    exact_match_rows: int = 0
    tolerance_match_rows: int = 0
    affected_row_count: int = 0
    # WARN 信息（仅诊断）
    has_warnings: bool = False
    warn_affected_row_count: int = 0
    # 判定原因
    decision_reason: str = ""
    error_message: str = ""


class CreHarnessAggregation(StrictModel):
    """CRE Harness 跨请求聚合报告——全部指标由 aggregate() 内部计算，禁止外部手工赋值。

    计算规则（零容忍准入标准）：
    - executable_consistency_rate = executable_consistent / executable_total（必须 = 100%）
    - false_negative_rate = false_negatives / total_known_differences（必须 = 0%）
    - cre_legacy_conflict_count：mapped=CONSISTENT 但 legacy=MISMATCH 的冲突数（必须 = 0）
    - not_executed_ratio = not_executed / total_samples（单独计算覆盖率，不稀释一致率）
    - warn_rate = warn_count / executable_total（仅诊断，不作为门槛）
    - 零已知差异样本 → 准入失败（无法验证 Harness 判别能力）

    aggregate() 是幂等的——每次调用从零开始重算，相同输入 → 相同输出。
    """

    # ── 总体统计 ──
    total_samples: int = 0
    executable_total: int = 0                     # diagnostic_available=True 的样本数
    not_executed_count: int = 0                   # NOT_EXECUTED 样本数（单独统计）
    error_count: int = 0                          # ERROR 样本数

    # ── 状态分布 ──
    cre_consistent_count: int = 0                 # CRE=CONSISTENT
    cre_consistent_warn_count: int = 0            # CRE=CONSISTENT_WITH_WARN
    cre_mismatch_count: int = 0                   # CRE=MISMATCH
    cre_human_review_count: int = 0               # CRE=HUMAN_REVIEW

    # ── Golden 样本统计 ──
    golden_total: int = 0                         # is_golden=True 的样本数
    golden_consistent_count: int = 0              # golden 样本中 CRE↔legacy 一致的样本数

    # ── 零容忍指标（由 aggregate() 内部计算，禁止外部赋值）──
    # 可执行样本状态一致率
    executable_consistent_count: int = 0          # 可执行且状态一致（CRE↔legacy）
    executable_consistency_rate: float = 0.0      # = executable_consistent / executable_total

    # 零假阴性——golden 样本的已知差异全部检出
    total_known_differences: int = 0              # golden_label=MISMATCH 的 golden 样本数
    false_negative_count: int = 0                 # golden=MISMATCH 但 CRE 判定 CONSISTENT
    false_negative_rate: float = 0.0              # 必须 = 0%

    # CRE/legacy 冲突——mapped=CONSISTENT 但 legacy=MISMATCH
    cre_legacy_conflict_count: int = 0            # 必须 = 0

    # NOT_EXECUTED 覆盖率（独立统计）
    not_executed_ratio: float = 0.0

    # WARN 率（仅诊断）
    warn_count: int = 0
    warn_rate: float = 0.0

    # ── 详细样本列表 ──
    samples: list[CreAhsMetrics] = Field(default_factory=list)

    # ── 准入判定 ──
    @property
    def passes_admission(self) -> bool:
        """CRE 准入标准判定——零容忍（全部必须满足）。

        - 至少一个已知差异样本（total_known_differences > 0）——否则无法验证 Harness 判别能力
        - 可执行样本状态一致率 = 100%
        - 零假阴性率 = 0%
        - CRE/legacy 冲突 = 0
        """
        if self.executable_total == 0:
            return False  # 无可执行样本——无法判定
        if self.total_known_differences == 0:
            return False  # 零已知差异样本——无法验证 Harness 判别能力
        return (
            self.executable_consistency_rate >= 1.0
            and self.false_negative_rate <= 0.0
            and self.cre_legacy_conflict_count == 0
        )

    def aggregate(self, metrics_list: list[CreAhsMetrics]) -> None:
        """从 AHS 指标列表计算聚合报告——幂等，禁止外部手工赋统计值。

        每次调用从零开始重算所有指标：
        1. 统计基本状态分布和一致率
        2. 从 golden 样本自动计算假阴性（golden=MISMATCH 但 CRE=CONSISTENT）
        3. 从所有样本自动计算 CRE/legacy 冲突（mapped=CONSISTENT 但 legacy=MISMATCH）
        4. 零已知差异样本 → passes_admission = False

        由 Harness 层调用——不依赖运行时 Memory。
        相同输入 → 相同输出（幂等）。
        """
        # ── 幂等：所有计数器从零开始 ──
        self.samples = list(metrics_list)
        self.total_samples = len(metrics_list)
        self.executable_total = 0
        self.not_executed_count = 0
        self.error_count = 0
        self.cre_consistent_count = 0
        self.cre_consistent_warn_count = 0
        self.cre_mismatch_count = 0
        self.cre_human_review_count = 0
        self.golden_total = 0
        self.golden_consistent_count = 0
        self.executable_consistent_count = 0
        self.total_known_differences = 0
        self.false_negative_count = 0
        self.cre_legacy_conflict_count = 0
        self.warn_count = 0
        self.executable_consistency_rate = 0.0
        self.false_negative_rate = 0.0
        self.not_executed_ratio = 0.0
        self.warn_rate = 0.0

        # ── CRE → legacy 映射表（与 PhysicalVerifier._CRE_TO_LEGACY_MAP 保持一致）──
        _cre_to_legacy = {
            CreShadowStatus.CONSISTENT: "RESULT_CONSISTENT",
            CreShadowStatus.CONSISTENT_WITH_WARN: "RESULT_CONSISTENT",
            CreShadowStatus.MISMATCH: "RESULT_MISMATCH",
            CreShadowStatus.HUMAN_REVIEW: "HUMAN_REVIEW",
        }

        for m in metrics_list:
            # ── Golden 样本统计 ──
            if m.is_golden:
                self.golden_total += 1
                if m.status_consistent:
                    self.golden_consistent_count += 1
                # 已知差异：golden_label=MISMATCH 的样本
                if m.golden_label == CreShadowStatus.MISMATCH:
                    self.total_known_differences += 1
                    # 假阴性：已知应有差异，但 CRE 判定 CONSISTENT（含 WARN）
                    if m.cre_status in (CreShadowStatus.CONSISTENT, CreShadowStatus.CONSISTENT_WITH_WARN):
                        self.false_negative_count += 1

            if not m.diagnostic_available:
                self.not_executed_count += 1
                continue

            self.executable_total += 1

            # 状态分布
            if m.cre_status == CreShadowStatus.CONSISTENT:
                self.cre_consistent_count += 1
            elif m.cre_status == CreShadowStatus.CONSISTENT_WITH_WARN:
                self.cre_consistent_warn_count += 1
            elif m.cre_status == CreShadowStatus.MISMATCH:
                self.cre_mismatch_count += 1
            elif m.cre_status == CreShadowStatus.HUMAN_REVIEW:
                self.cre_human_review_count += 1
            elif m.cre_status == CreShadowStatus.ERROR:
                self.error_count += 1

            # 一致率
            if m.status_consistent:
                self.executable_consistent_count += 1

            # CRE/legacy 冲突：mapped=CONSISTENT 但 legacy=MISMATCH
            mapped = _cre_to_legacy.get(m.cre_status, "")
            if mapped == "RESULT_CONSISTENT" and m.legacy_status == "RESULT_MISMATCH":
                self.cre_legacy_conflict_count += 1

            # WARN 跟踪（仅诊断）
            if m.has_warnings:
                self.warn_count += 1

        # 计算比率
        if self.executable_total > 0:
            self.executable_consistency_rate = (
                self.executable_consistent_count / self.executable_total
            )
            self.warn_rate = self.warn_count / self.executable_total

        if self.total_samples > 0:
            self.not_executed_ratio = self.not_executed_count / self.total_samples

        if self.total_known_differences > 0:
            self.false_negative_rate = (
                self.false_negative_count / self.total_known_differences
            )
