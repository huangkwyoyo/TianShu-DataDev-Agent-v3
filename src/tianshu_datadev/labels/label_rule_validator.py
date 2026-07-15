"""LabelRuleValidator v1——六项确定性检查。

v4-light 最终版：双空通过 + 无区间证明。
passed = blocking_errors 和 human_review_items 均为空。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    CompareOp,
    LabelAnd,
    LabelCompare,
    LabelDomain,
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


class LabelRuleValidator:
    """确定性标签规则验证器 v1——六项检查，不做区间证明。

    passed = blocking_errors 和 human_review_items 均为空。
    """

    def validate(
        self,
        proposal: LabelRuleProposal,
        spec: ParsedDeveloperSpec,
    ) -> LabelValidationReport:
        """对单个 Proposal 执行全部六项检查。

        Args:
            proposal: 系统包装后的标签规则候选
            spec: 已解析的 DeveloperSpec——提供源表列名等上下文

        Returns:
            LabelValidationReport——passed=True 当且仅当双空
        """
        checks: list[LabelValidationCheck] = []
        blocking: list[str] = []
        human_review: list[str] = []
        warnings: list[str] = []

        # 收集所有已知列名
        known_columns: set[str] = set()
        for t in spec.input_tables:
            for c in t.columns:
                known_columns.add(c.normalized_name)
                known_columns.add(c.column_name)

        # 1. FIELD_EXISTS
        self._check_field_exists(proposal, known_columns, checks, blocking)
        # 2. TYPE_COMPATIBLE
        self._check_type_compatible(proposal, checks, blocking)
        # 3. OPERATOR_VALID
        self._check_operator_valid(proposal, checks, blocking)
        # 4. AST_VALID
        self._check_ast_valid(proposal, checks, blocking)
        # 5. LABEL_DOMAIN
        self._check_label_domain(proposal, checks, blocking)
        # 6. COVERAGE（ELSE + evidence 非空）
        self._check_coverage(proposal, checks, human_review)

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

    def _check_type_compatible(self, proposal, checks, blocking):
        """2. TYPE_COMPATIBLE——比较操作符类型与字面量 data_type 兼容。"""
        for branch in proposal.branches:
            invalid = self._find_type_mismatches(branch.condition)
            if invalid:
                checks.append(LabelValidationCheck(
                    check_name="TYPE_COMPATIBLE", passed=False, level="BLOCKING",
                    detail=f"类型不兼容: {invalid}",
                ))
                blocking.append(f"类型不兼容: {invalid}")
                return
        checks.append(LabelValidationCheck(
            check_name="TYPE_COMPATIBLE", passed=True, level="BLOCKING",
            detail="类型兼容",
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
        """5. LABEL_DOMAIN——then_label/else_value 在 proposal.label_domain.values 中。"""
        domain = proposal.label_domain
        if not domain or not domain.values:
            checks.append(LabelValidationCheck(
                check_name="LABEL_DOMAIN", passed=True, level="BLOCKING",
                detail="无 label_domain values——跳过域检查",
            ))
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

    def _check_coverage(self, proposal, checks, human_review):
        """6. COVERAGE——ELSE 非空 + 所有 evidence 非空。"""
        empty_evidence = [b.then_label for b in proposal.branches if not b.evidence]
        if empty_evidence:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE", passed=False, level="HUMAN_REVIEW",
                detail=f"分支 evidence 为空: {empty_evidence}",
            ))
            human_review.append(
                f"分支 {empty_evidence} evidence 为空——无法确定性判断覆盖完整性"
            )
        else:
            checks.append(LabelValidationCheck(
                check_name="COVERAGE", passed=True, level="BLOCKING",
                detail="ELSE 非空 + 所有 evidence 非空——覆盖检查通过",
            ))

    # ── 辅助递归方法 ──

    def _collect_column_refs(self, node, known, missing):
        """递归收集节点树中所有列引用——不在 known 中的添加到 missing。"""
        if isinstance(node, LabelCompare):
            if node.left not in known:
                missing.append(node.left)
        elif isinstance(node, (LabelIsNull, LabelIsNotNull)):
            if node.column not in known:
                missing.append(node.column)
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                self._collect_column_refs(child, known, missing)
        elif isinstance(node, LabelNot):
            self._collect_column_refs(node.child, known, missing)

    @staticmethod
    def _find_type_mismatches(node) -> list[str]:
        """查找类型不兼容的比较——string 类型仅支持 = / !=。"""
        errors: list[str] = []
        if isinstance(node, LabelCompare):
            if node.right.data_type == "string" and node.op not in (CompareOp.EQ, CompareOp.NEQ):
                errors.append(
                    f"{node.left} {node.op.value} '{node.right.value}'——"
                    f"string 类型仅支持 =/!="
                )
        elif isinstance(node, (LabelAnd, LabelOr)):
            for child in node.children:
                errors.extend(LabelRuleValidator._find_type_mismatches(child))
        elif isinstance(node, LabelNot):
            errors.extend(LabelRuleValidator._find_type_mismatches(node.child))
        return errors

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
