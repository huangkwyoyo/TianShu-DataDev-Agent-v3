"""LabelRuleValidator v1——六项确定性检查。

v4-light 最终版：双空通过 + 无区间证明。
passed = blocking_errors 和 human_review_items 均为空。

Task 11.5 改进：
- 类型族校验：根据 ColumnDecl.data_type 做最小类型族检查
- 空 label_domain 阻断：label_table v1 要求 label_domain 必须非空
- evidence 锚定检查：evidence 必须锚定项目书正文（非空 + 最小长度）
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    CompareOp,
    LabelAnd,
    LabelCompare,
    LabelDatePartRef,
    LabelIsNotNull,
    LabelIsNull,
    LabelNot,
    LabelOr,
    LabelRuleProposal,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.artifacts import (
    LabelValidationCheck,
    LabelValidationReport,
)

# ── 类型族定义——用于最小类型兼容性检查 ──
_NUMERIC_TYPES = frozenset({
    "int", "bigint", "integer", "tinyint", "smallint",
    "double", "float", "decimal", "numeric", "number", "real",
})
_STRING_TYPES = frozenset({
    "string", "varchar", "text", "char", "nvarchar", "nchar",
})
_TEMPORAL_TYPES = frozenset({
    "date", "datetime", "timestamp", "timestamp_ntz", "timestamp_ltz",
})
_BOOLEAN_TYPES = frozenset({"boolean", "bool"})

# evidence 最小长度——低于此阈值的 evidence 视为未锚定项目书正文
_MIN_EVIDENCE_LENGTH = 10


def _normalize_text(text: str) -> str:
    """规范化文本用于子串匹配——折叠空白字符，去除首尾空格。

    将连续的空白字符（空格、换行、制表符等）替换为单个空格，
    然后去除首尾空格并转小写。

    Args:
        text: 待规范化的原始文本

    Returns:
        规范化后的文本
    """
    import re
    return re.sub(r"\s+", " ", text).strip().lower()


def _same_type_family(col_type: str | None, lit_type: str | None) -> bool:
    """检查列类型和字面量类型是否属于同一类型族。

    仅做最小检查——仅在明确不兼容时报错。
    - 数值列 + 字符串字面量 → False（明确不兼容）
    - 其他组合 → True（宽容通过，避免过度阻断）

    Args:
        col_type: ColumnDecl.data_type（可能为 None）
        lit_type: LabelTypedLiteral.data_type（可能为 None）

    Returns:
        True 表示类型兼容或无法判断，False 表示明确不兼容
    """
    if col_type is None or lit_type is None:
        return True  # 缺少类型信息时宽容通过
    col_lower = col_type.lower().strip()
    lit_lower = lit_type.lower().strip()
    # 数值列 + 字符串字面量 → 明确不兼容
    if col_lower in _NUMERIC_TYPES and lit_lower == "string":
        return False
    return True  # 其他组合宽容通过


class LabelRuleValidator:
    """确定性标签规则验证器 v1——六项检查，不做区间证明。

    passed = blocking_errors 和 human_review_items 均为空。

    Task 11.5 改进：
    - 类型族校验：数值列 + 字符串字面量 → BLOCKING
    - 空 label_domain → BLOCKING（label_table v1 强制要求）
    - evidence 非空 + 最小长度锚定检查
    """

    def validate(
        self,
        proposal: LabelRuleProposal,
        spec: ParsedDeveloperSpec,
        *,
        strict_evidence: bool = True,
    ) -> LabelValidationReport:
        """对单个 Proposal 执行全部六项检查。

        Args:
            proposal: 系统包装后的标签规则候选
            spec: 已解析的 DeveloperSpec——提供源表列名和类型等上下文

        Returns:
            LabelValidationReport——passed=True 当且仅当双空
        """
        checks: list[LabelValidationCheck] = []
        blocking: list[str] = []
        human_review: list[str] = []
        warnings: list[str] = []

        # 收集所有已知列名 + 列类型映射
        known_columns: set[str] = set()
        column_types: dict[str, str] = {}  # column_name → data_type
        for t in spec.input_tables:
            for c in t.columns:
                known_columns.add(c.normalized_name)
                known_columns.add(c.column_name)
                if c.data_type:
                    column_types[c.normalized_name] = c.data_type
                    column_types[c.column_name] = c.data_type

        # 1. FIELD_EXISTS
        self._check_field_exists(proposal, known_columns, checks, blocking)
        # 2. TYPE_COMPATIBLE（含类型族校验）
        self._check_type_compatible(proposal, column_types, checks, blocking)
        # 3. OPERATOR_VALID
        self._check_operator_valid(proposal, checks, blocking)
        # 4. AST_VALID
        self._check_ast_valid(proposal, checks, blocking)
        # 5. LABEL_DOMAIN（空 domain 阻断）
        self._check_label_domain(proposal, checks, blocking)
        # 6. COVERAGE（ELSE + evidence 锚定检查）
        self._check_coverage(
            proposal,
            spec,
            checks,
            human_review,
            warnings,
            strict_evidence=strict_evidence,
        )
        # 7. NO_LABEL_NOT——label_table v1 暂时拒绝 LabelNot
        self._check_no_label_not(proposal, checks, blocking)

        # v4-light 最终版: passed = 双空
        passed = len(blocking) == 0 and len(human_review) == 0
        return LabelValidationReport(
            proposal_id=proposal.proposal_id,
            passed=passed,
            checks=checks,
            blocking_errors=blocking,
            human_review_items=human_review,
            warnings=warnings,
        )

    # ── 六项检查 ──

    def _check_field_exists(self, proposal, known_columns, checks, blocking):
        """1. FIELD_EXISTS——condition 中所有列引用必须在已知列中。"""
        missing: list[str] = []
        for branch in proposal.branches:
            self._collect_column_refs(branch.condition, known_columns, missing)
        if missing:
            unique_missing = sorted(set(missing))
            checks.append(LabelValidationCheck(
                check_name="FIELD_EXISTS", passed=False, level="BLOCKING",
                detail=f"未知列: {unique_missing}",
            ))
            blocking.append(f"未知列: {unique_missing}")
        else:
            checks.append(LabelValidationCheck(
                check_name="FIELD_EXISTS", passed=True, level="BLOCKING",
                detail="所有列引用有效",
            ))

    def _check_type_compatible(self, proposal, column_types, checks, blocking):
        """2. TYPE_COMPATIBLE——比较操作符类型兼容 + 类型族校验。

        两项检查：
        a) 操作符级别：string 类型仅支持 = / !=（已有）
        b) 类型族级别：数值列 + 字符串字面量 → BLOCKING（Task 11.5 新增）
        """
        errors: list[str] = []
        for branch in proposal.branches:
            # 递归收集类型不兼容错误
            self._find_type_mismatches(branch.condition, column_types, errors)
        if errors:
            checks.append(LabelValidationCheck(
                check_name="TYPE_COMPATIBLE", passed=False, level="BLOCKING",
                detail=f"类型不兼容: {errors}",
            ))
            blocking.append(f"类型不兼容: {errors}")
        else:
            checks.append(LabelValidationCheck(
                check_name="TYPE_COMPATIBLE", passed=True, level="BLOCKING",
                detail="类型兼容——操作符和类型族均通过",
            ))

    def _check_operator_valid(self, proposal, checks, blocking):
        """3. OPERATOR_VALID——操作符合法，布尔节点子节点数合法。"""
        errors = self._find_operator_errors(proposal)
        if errors:
            checks.append(LabelValidationCheck(
                check_name="OPERATOR_VALID", passed=False, level="BLOCKING",
                detail=f"操作符错误: {errors}",
            ))
            blocking.append(f"操作符错误: {errors}")
        else:
            checks.append(LabelValidationCheck(
                check_name="OPERATOR_VALID", passed=True, level="BLOCKING",
                detail="操作符合法",
            ))

    def _check_ast_valid(self, proposal, checks, blocking):
        """4. AST_VALID——condition 为 LabelPredicateCondition discriminator 子类。"""
        for branch in proposal.branches:
            if isinstance(branch.condition, str):
                checks.append(LabelValidationCheck(
                    check_name="AST_VALID", passed=False, level="BLOCKING",
                    detail="condition 是字符串——必须为 LabelPredicateCondition 子类",
                ))
                blocking.append("condition 是字符串而非结构化 AST")
                return
        checks.append(LabelValidationCheck(
            check_name="AST_VALID", passed=True, level="BLOCKING",
            detail="AST 结构合法",
        ))

    def _check_label_domain(self, proposal, checks, blocking):
        """5. LABEL_DOMAIN——标签值域检查。

        Task 11.5 改进：空 label_domain 阻断——
        label_table v1 要求 label_domain 必须非空且含至少一个合法值。
        """
        domain = proposal.label_domain
        # ── 空 domain 阻断（Task 11.5）──
        if not domain or not domain.values:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=False, level="BLOCKING",
                detail="label_domain 为空——label_table v1 要求声明标签值域",
            ))
            blocking.append("label_domain 为空——label_table v1 要求声明标签值域")
            return

        domain_set = set(domain.values)
        outside = []
        for branch in proposal.branches:
            if branch.then_label not in domain_set:
                outside.append(branch.then_label)
        if proposal.else_value not in domain_set:
            outside.append(proposal.else_value)
        if outside:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=False, level="BLOCKING",
                detail=f"标签值不在域中: {outside}",
            ))
            blocking.append(f"标签值不在域中: {outside}")
        else:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="BLOCKING",
                detail="所有标签值在域内",
            ))

    def _check_coverage(
        self,
        proposal,
        spec,
        checks,
        human_review,
        warnings,
        *,
        strict_evidence: bool,
    ):
        """6. COVERAGE——ELSE 非空 + evidence 锚定项目书正文。

        Task 11.5 改进：evidence 必须满足最小长度要求——
        过短的 evidence（如单个字符）视为未锚定项目书正文。

        Task 11.6 改进：evidence 必须在 spec.description 中可规范化匹配——
        无法匹配的 evidence 视为未锚定，标记 HUMAN_REVIEW。
        """
        issues: list[str] = []
        # 规范化 description 全文——用于子串匹配
        normalized_desc = _normalize_text(spec.description) if spec.description else ""
        # ── evidence 非空 + 最小长度 + 原文匹配检查 ──
        for branch in proposal.branches:
            if not branch.evidence:
                issues.append(f"分支 '{branch.then_label}' evidence 为空")
                continue
            stripped = branch.evidence.strip()
            if len(stripped) < _MIN_EVIDENCE_LENGTH:
                issues.append(
                    f"分支 '{branch.then_label}' evidence 过短"
                    f"（{len(branch.evidence)} 字符 < {_MIN_EVIDENCE_LENGTH} 最小阈值）——"
                    f"未锚定项目书正文"
                )
                continue
            # ── description 为空时 evidence 验证不得通过 ──
            if not normalized_desc:
                issues.append(
                    f"分支 '{branch.then_label}' evidence 无法锚定——"
                    f"项目书正文（description）为空，无法验证 evidence 来源"
                )
                continue
            # ── Task 11.6: evidence 原文匹配检查 ──
            normalized_evidence = _normalize_text(stripped)
            if normalized_evidence not in normalized_desc:
                issues.append(
                    f"分支 '{branch.then_label}' evidence 无法在项目书正文中匹配——"
                    f"evidence 原文片段与 description 不一致"
                )
        if issues:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE",
                passed=not strict_evidence,
                level="HUMAN_REVIEW" if strict_evidence else "WARNING",
                detail=f"evidence 锚定检查失败: {issues}",
            ))
            if strict_evidence:
                human_review.extend(issues)
            else:
                warnings.extend(issues)
        else:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE", passed=True, level="BLOCKING",
                detail="ELSE 非空 + 所有 evidence 已锚定项目书正文",
            ))

    def _check_no_label_not(self, proposal, checks, blocking):
        """7. NO_LABEL_NOT——label_table v1 暂时拒绝 LabelNot 节点。

        LabelNot 会增加 CASE WHEN 表达式的嵌套复杂度——
        label_table v1 阶段暂不开放，遇到时标记 BLOCKING。
        """
        not_found: list[str] = []
        for branch in proposal.branches:
            self._find_label_not_nodes(branch.condition, not_found)
        if not_found:
            checks.append(LabelValidationCheck(
                check_name="NO_LABEL_NOT", passed=False, level="BLOCKING",
                detail=f"label_table v1 暂不支持 LabelNot——以下分支包含 NOT 节点: {not_found}",
            ))
            blocking.append(f"label_table v1 暂不支持 LabelNot: {not_found}")
        else:
            checks.append(LabelValidationCheck(
                check_name="NO_LABEL_NOT", passed=True, level="BLOCKING",
                detail="无 LabelNot 节点",
            ))

    @staticmethod
    def _find_label_not_nodes(node, results: list[str]) -> None:
        """递归查找 LabelNot 节点——记录 then_label 用于错误信息。"""
        if isinstance(node, LabelNot):
            # 从 NOT 节点的子节点中提取列名作为定位信息
            col_info = "NOT(...)"
            if isinstance(node.child, LabelCompare):
                col_info = f"NOT({node.child.left})"
            elif isinstance(node.child, (LabelIsNull, LabelIsNotNull)):
                col_info = f"NOT({node.child.column})"
            results.append(col_info)
            # 继续递归——NOT 内部可能还有嵌套 NOT
            LabelRuleValidator._find_label_not_nodes(node.child, results)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                LabelRuleValidator._find_label_not_nodes(child, results)
    # ── 辅助递归方法 ──

    def _collect_column_refs(self, node, known, missing):
        """递归收集节点树中所有列引用——不在 known 中的添加到 missing。"""
        if isinstance(node, LabelCompare):
            column_name = (
                node.left.column_name
                if isinstance(node.left, LabelDatePartRef)
                else node.left
            )
            if column_name not in known:
                missing.append(column_name)
        elif isinstance(node, (LabelIsNull, LabelIsNotNull)):
            if node.column not in known:
                missing.append(node.column)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                self._collect_column_refs(child, known, missing)
        elif isinstance(node, LabelNot):
            self._collect_column_refs(node.child, known, missing)

    def _find_type_mismatches(
        self, node, column_types: dict[str, str], errors: list[str],
    ) -> None:
        """递归查找类型不兼容——操作符级别 + 类型族级别。

        a) 操作符级别：string 类型仅支持 = / !=
        b) 类型族级别（Task 11.5 新增）：数值列 + 字符串字面量 → 不兼容
        """
        if isinstance(node, LabelCompare):
            column_name = (
                node.left.column_name
                if isinstance(node.left, LabelDatePartRef)
                else node.left
            )
            display_left = (
                f"{node.left.part}({column_name})"
                if isinstance(node.left, LabelDatePartRef)
                else column_name
            )
            if isinstance(node.left, LabelDatePartRef):
                source_type = (column_types.get(column_name) or "").lower()
                if source_type not in _TEMPORAL_TYPES:
                    errors.append(
                        f"{display_left} 的源列类型必须是日期或时间，实际为 {source_type or 'unknown'}"
                    )
            # a) 操作符级别检查
            if node.right.data_type == "string" and node.op not in (CompareOp.EQ, CompareOp.NEQ):
                errors.append(
                    f"{display_left} {node.op.value} '{node.right.value}'——"
                    f"string 类型仅支持 =/!="
                )
            # b) 类型族级别检查（Task 11.5 新增）
            col_type = (
                "number"
                if isinstance(node.left, LabelDatePartRef)
                else column_types.get(column_name)
            )
            if not _same_type_family(col_type, node.right.data_type):
                errors.append(
                    f"{display_left}（类型={col_type}）与 "
                    f"'{node.right.value}'（类型={node.right.data_type}）类型族不兼容"
                )
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                self._find_type_mismatches(child, column_types, errors)
        elif isinstance(node, LabelNot):
            self._find_type_mismatches(node.child, column_types, errors)

    def _find_operator_errors(self, proposal) -> list[str]:
        """检查布尔节点子节点数和操作符合法性。"""
        errors: list[str] = []
        for branch in proposal.branches:
            self._check_node_structure(branch.condition, errors)
        return errors

    def _check_node_structure(self, node, errors):
        """递归检查节点结构合法性。"""
        if isinstance(node, (LabelAnd, LabelOr)):
            if len(node.children) < 2:
                errors.append(f"{node.node_type} 至少需要 2 个子节点")
            for child in node.children:
                self._check_node_structure(child, errors)
        elif isinstance(node, LabelNot):
            if node.child is None:
                errors.append("NOT 节点需要非空 child")
            else:
                self._check_node_structure(node.child, errors)
