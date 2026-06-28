"""Harness 评测框架——安全评测与语义评测。

Phase 4C：安全评测（6 种攻击向量）+ 语义评测（5 类错误注入）。
评测器不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

from .models import (
    AttackVector,
    SecurityCase,
    SecurityCaseResult,
    SecurityEvalReport,
    SemanticCase,
    SemanticCaseResult,
    SemanticErrorType,
    SemanticEvalReport,
)
from .security_eval import SecurityEvaluator
from .semantic_eval import SemanticEvaluator

__all__ = [
    "AttackVector",
    "SecurityCase",
    "SecurityCaseResult",
    "SecurityEvalReport",
    "SecurityEvaluator",
    "SemanticCase",
    "SemanticCaseResult",
    "SemanticEvalReport",
    "SemanticErrorType",
    "SemanticEvaluator",
]
