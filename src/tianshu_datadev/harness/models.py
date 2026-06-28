"""Phase 4C 评测数据模型——安全评测与语义评测。

所有模型继承 StrictModel（extra="forbid"），遵循现有模式。
评测器不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# 安全评测模型
# ════════════════════════════════════════════


class AttackVector(str, Enum):
    """六种攻击向量——对应 Phase 4C 安全评测范围。"""

    PROMPT_INJECTION = "PROMPT_INJECTION"  # 攻击向量1：Prompt 注入
    SQL_INJECTION = "SQL_INJECTION"  # 攻击向量2：SQL 注入
    SCHEMA_EXTRA = "SCHEMA_EXTRA"  # 攻击向量3：Schema extra 突破
    UNDECLARED_REF = "UNDECLARED_REF"  # 攻击向量4：未声明引用
    JOIN_ERROR_INFERENCE = "JOIN_ERROR_INFERENCE"  # 攻击向量5：Join 错误推理
    WRITE_PRIVILEGE = "WRITE_PRIVILEGE"  # 攻击向量6：写入越权


class SecurityCase(StrictModel):
    """单个安全攻击测试用例——确定性的输入 + 预期行为。

    每个 case 对应一种攻击向量的一个具体注入场景。
    expected_rejection_pattern 用于匹配实际拒绝码/异常消息中的关键词，
    确保拒绝是可追溯的（不依赖 LLM "自行判断"）。
    """

    case_id: str  # "SEC-PI-001"——唯一标识
    attack_vector: AttackVector  # 攻击向量分类
    description: str  # 人类可读的攻击场景描述
    expected_protection_layer: str  # 预期拦截层："schema" | "validator" | "render" | "write_validator"
    expected_rejection_pattern: str  # 预期拒绝码/消息的关键词（用于 traceable rejection）
    payload: dict = {}  # 攻击载荷参数——驱动 evaluator 构造具体攻击输入


class SecurityCaseResult(StrictModel):
    """单个安全测试的执行结果——含拦截层和拒绝详情。

    每个结果必须记录拒绝的 code/path/message，确保可追溯。
    """

    case_id: str  # 对应 SecurityCase.case_id
    attack_vector: AttackVector  # 攻击向量分类
    passed: bool  # True=系统正确拦截了此攻击
    detection_layer: str | None = None  # 实际拦截层（schema/validator/render/write_validator）
    rejection_detail: str = ""  # 实际拒绝详情——question_id 或异常消息（即 rejection code + message）
    trace: str = ""  # 执行追踪描述——含 path（如 rule_id、field_ref）


class SecurityEvalReport(StrictModel):
    """安全评测聚合报告——覆盖全部 6 种攻击向量。

    汇总每个攻击向量的逐条结果，标记未拦截的攻击（blocking_issues）。
    """

    eval_id: str  # 评测唯一标识——确定性生成
    timestamp: str  # ISO 时间戳
    summary: str  # "6/6 vectors blocked" 等汇总结果
    vector_coverage: dict[str, bool]  # 每种攻击向量是否被覆盖（AttackVector.value → bool）
    results: list[SecurityCaseResult] = []  # 所有用例的逐条结果
    blocking_issues: list[str] = []  # 未拦截的攻击列表（case_id + 简要描述）

    @staticmethod
    def generate_eval_id() -> str:
        """生成确定性评测 ID——基于时间戳的 SHA-256 前 12 位。"""
        now = datetime.now(timezone.utc).isoformat()
        return f"sec_eval_{hashlib.sha256(now.encode()).hexdigest()[:12]}"


# ════════════════════════════════════════════
# 语义评测模型
# ════════════════════════════════════════════


class SemanticErrorType(str, Enum):
    """五类语义错误——对应 Phase 4C 语义评测范围。"""

    WRONG_FIELD = "WRONG_FIELD"  # 错字段——聚合输入列与声明不符
    WRONG_GRAIN = "WRONG_GRAIN"  # 错粒度——分组键与声明不符
    WRONG_AGGREGATION = "WRONG_AGGREGATION"  # 错聚合——聚合函数与声明不符
    WRONG_ENUM = "WRONG_ENUM"  # 错枚举——CASE WHEN 输出未声明枚举值
    WRONG_JOIN = "WRONG_JOIN"  # 错 Join——Join key 与声明不符


class SemanticCase(StrictModel):
    """单个语义错误测试用例——声明 vs 实际输出的差异化检测验证。

    注入方式是"声明正确但构造故意错误的 SqlBuildPlan"，
    验证系统能否检测到差异。
    """

    case_id: str  # "SEM-WF-001"——唯一标识
    error_type: SemanticErrorType  # 语义错误分类
    description: str  # 人类可读的错误场景描述
    expected_detection_layer: str  # 预期检测层："validator" | "label_validator" | "perf_validator"
    expected_rejection_pattern: str  # 预期拒绝码/消息的关键词


class SemanticCaseResult(StrictModel):
    """单个语义测试的执行结果——含检测层和拒绝详情。"""

    case_id: str  # 对应 SemanticCase.case_id
    error_type: SemanticErrorType  # 语义错误分类
    passed: bool  # True=系统正确检测到此语义错误
    detection_layer: str | None = None  # 实际检测层
    rejection_detail: str = ""  # 拦截详情或未检测原因
    trace: str = ""  # 执行追踪描述


class SemanticEvalReport(StrictModel):
    """语义评测聚合报告——覆盖全部 5 类语义错误。

    汇总每类错误的逐条结果，标记未检测到的错误（undetected_errors）。
    known_gaps 记录系统当前确实无法检测的错误类型——与"检测失败"区分，
    表示系统能力边界而非测试未通过。
    """

    eval_id: str  # 评测唯一标识——确定性生成
    timestamp: str  # ISO 时间戳
    summary: str  # "3/5 errors detectable (2 known gaps: WRONG_GRAIN, WRONG_AGGREGATION)" 等汇总结果
    error_type_coverage: dict[str, bool]  # 每种错误类型是否被覆盖。known_gap 类型为 False
    results: list[SemanticCaseResult] = []  # 所有用例的逐条结果
    undetected_errors: list[str] = []  # 未被检测到的语义错误列表
    known_gaps: list[str] = []  # 已知能力缺口——系统当前无规则检测的 SemanticErrorType.value 列表

    @staticmethod
    def generate_eval_id() -> str:
        """生成确定性评测 ID——基于时间戳的 SHA-256 前 12 位。"""
        now = datetime.now(timezone.utc).isoformat()
        return f"sem_eval_{hashlib.sha256(now.encode()).hexdigest()[:12]}"
