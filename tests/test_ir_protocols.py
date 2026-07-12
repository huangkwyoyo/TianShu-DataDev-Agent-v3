"""IR Protocol 兼容层最小契约测试。

ir.protocols 已完全废弃——生产代码零引用（2026-07-11 确认）。
本文件只保留兼容契约验证，不测试枚举值、Protocol 形状或字段存在性。
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import sys
import warnings

# ═══════════════════════════════════════════════════════════════════════
# 契约 1：导入 ir.protocols 必须发出 DeprecationWarning
# ═══════════════════════════════════════════════════════════════════════


def test_import_triggers_deprecation_warning():
    """导入 ir.protocols 时发出 DeprecationWarning——告知调用方迁移。

    该模块在模块级 (line 36) 通过 warnings.warn 发出 DeprecationWarning。
    如未来移除警告逻辑（=正式删除模块），此测试应同步删除。
    """
    # 清除之前的导入缓存，确保每次都能抓到 warning
    _clear_ir_cache()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        importlib.import_module("tianshu_datadev.ir.protocols")
        deprecation_warnings = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "ir.protocols is deprecated" in str(x.message)
        ]
        assert len(deprecation_warnings) >= 1, (
            "导入 ir.protocols 应发出 DeprecationWarning——"
            "如该模块已正式删除，请同步删除本测试"
        )


# ═══════════════════════════════════════════════════════════════════════
# 契约 2：生产代码不得导入 ir.protocols
# ═══════════════════════════════════════════════════════════════════════


# ir/ 自身文件——这些可以导入
_IR_SELF_FILES = {
    "ir/__init__.py",
    "ir/protocols.py",
}


def _collect_src_files() -> list[pathlib.Path]:
    """收集 src/ 下所有 .py 文件。"""
    src_root = pathlib.Path(__file__).resolve().parents[1] / "src"
    return sorted(
        p for p in src_root.rglob("*.py")
        if p.is_file() and "__pycache__" not in str(p)
    )


def _has_ir_protocols_import(file_path: pathlib.Path) -> str | None:
    """检查文件是否导入 ir.protocols——返回匹配行或 None。"""
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if "ir.protocols" in module:
                return f"from {module} import ..."
        if isinstance(node, ast.Import):
            for alias in node.names:
                if "ir.protocols" in alias.name:
                    return f"import {alias.name}"
    return None


def test_no_production_code_imports_ir_protocols():
    """验证 src/ 下没有任何生产代码导入已废弃的 ir.protocols。

    唯一例外：ir/__init__.py（兼容导出层）和 ir/protocols.py 自身。
    如新增合法使用方，需在 AGENTS.md 中记录并通过审查。
    """
    violations: list[str] = []
    for f in _collect_src_files():
        rel = str(f.relative_to(pathlib.Path(__file__).resolve().parents[1] / "src"))
        # 跳过 ir 包自身
        if rel in _IR_SELF_FILES:
            continue
        # 跳过测试文件（tests/ 外的测试辅助？不，只扫描 src/）
        line = _has_ir_protocols_import(f)
        if line is not None:
            violations.append(f"src/{rel}: {line}")

    assert not violations, (
        f"发现 {len(violations)} 处生产代码导入已废弃的 ir.protocols：\n"
        + "\n".join(violations)
        + "\n\n请迁移到对应的严格 Pydantic 模型。"
    )


# ═══════════════════════════════════════════════════════════════════════
# 契约 3：兼容导出——顶层 ir 包仍导出旧 Protocol 名称
# ═══════════════════════════════════════════════════════════════════════

# 顶层 ir/__init__.py 承诺导出的符号集合（2026-07-11 快照）
_EXPECTED_EXPORTS = {
    "CrossValidationResult",
    "MergedResult",
    "RepairDirective",
    "RepairTarget",
    "RequestStatus",
    "RequirementIR",
    "SparkCodeArtifact",
    "SQLPlan",
    "StepStatus",
    "SubIntent",
    "TransformationContract",
    "TransformParams",
}


def test_top_level_ir_exports_are_stable():
    """验证 ir/__init__.py 的 __all__ 导出集合未意外变更。

    如果此测试失败，说明有人新增或删除了兼容导出——需更新 _EXPECTED_EXPORTS
    快照并在 AGENTS.md 中记录变更原因。
    """
    _clear_ir_cache()
    mod = importlib.import_module("tianshu_datadev.ir")
    actual = set(getattr(mod, "__all__", []))
    assert actual == _EXPECTED_EXPORTS, (
        f"ir/__init__.py 的 __all__ 导出集合已变更：\n"
        f"  新增: {actual - _EXPECTED_EXPORTS}\n"
        f"  删除: {_EXPECTED_EXPORTS - actual}\n"
        f"请更新此测试的 _EXPECTED_EXPORTS 快照并记录变更原因。"
    )


# ═══════════════════════════════════════════════════════════════════════
# 辅助
# ═══════════════════════════════════════════════════════════════════════


def _clear_ir_cache() -> None:
    """清除 ir 包及其子模块的导入缓存。"""
    for key in list(sys.modules.keys()):
        if key == "tianshu_datadev.ir" or key.startswith("tianshu_datadev.ir."):
            del sys.modules[key]
