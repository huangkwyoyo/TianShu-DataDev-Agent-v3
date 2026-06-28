"""Harness 评测框架——安全评测、语义评测与七维门禁。

Phase 4C：安全评测（6 种攻击向量）+ 语义评测（5 类错误注入）。
Phase 4D：七维门禁 + SQL-first v1.0 验收框架。
评测器不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

from .dataset_loader import DatasetLoader
from .eval_runner import HarnessRunner
from .metrics import HarnessMetricsEngine
from .models import (
    AttackVector,
    DatasetCategory,
    DimensionResult,
    HarnessCase,
    HarnessReport,
    HarnessVerdict,
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
    # Phase 4C
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
    # Phase 4D
    "DatasetCategory",
    "DatasetLoader",
    "DimensionResult",
    "HarnessCase",
    "HarnessMetricsEngine",
    "HarnessReport",
    "HarnessRunner",
    "HarnessVerdict",
]
