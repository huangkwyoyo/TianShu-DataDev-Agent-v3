"""ProposalValidator——确定性的 Proposal 正确性校验，不调 LLM。

V1-V13 检查项全覆盖：
- 列存在性（V1-V2）
- 时间函数白名单（V3）
- 指标别名/维度名称唯一性（V4-V6）
- CASE WHEN 结构完整性（V7-V8）
- 条件引用正确性（V9）
- LabelNot 拒绝（V10b）
- 冲突检测（V11）
- 指标完整性（V12）
- 输出列映射覆盖（V13）
"""

from tianshu_datadev.developer_spec.models import (
    OpenQuestion,
    ParsedDeveloperSpec,
    RequirementProposal,
    SourceManifest,
)


class ProposalValidator:
    """确定性校验——不调 LLM，不做语义推断。"""

    def validate(
        self,
        proposal: RequirementProposal,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[bool, list[OpenQuestion]]:
        """校验 Proposal 的正确性。

        Args:
            proposal: 待校验的 Proposal
            spec: 解析后的 DeveloperSpec
            manifest: SourceManifest

        Returns:
            (valid, questions) 元组——valid=False 表示存在阻断级问题
        """
        questions: list[OpenQuestion] = []
        valid = True

        # ── 收集参考数据集 ──

        # manifest 中所有有效列名
        all_columns: set[str] = set()
        for table in manifest.tables:
            for col in table.columns:
                all_columns.add(col.column_name)

        # spec input_tables 中所有列名
        spec_columns: set[str] = set()
        for table in spec.input_tables:
            for col in table.columns:
                spec_columns.add(col.column_name)

        # proposal 中所有已声明的名称（可用于条件引用验证）
        available_names: set[str] = set()
        available_names.update(spec_columns)
        for d in spec.dimensions:
            available_names.add(d.dimension_name)
        for dd in spec.derived_dimensions:
            available_names.add(dd.dimension_name)
        for d in proposal.dimensions:
            available_names.add(d.dimension_name)
        for dd in proposal.derived_dimensions:
            available_names.add(dd.dimension_name)
        for m in proposal.metrics:
            available_names.add(m.alias)

        # ════════════════════════════════════════════
        # V1: dimension column_ref 存在于 SourceManifest
        # ════════════════════════════════════════════
        for d in proposal.dimensions:
            if d.column_ref not in all_columns:
                questions.append(OpenQuestion(
                    question_id="V1",
                    source="proposal_validator",
                    field_ref=f"dimensions.{d.dimension_name}.column_ref",
                    description=f"列 '{d.column_ref}' 不在 SourceManifest 中",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V2: derived_dimension source_column 存在于 SourceManifest
        # ════════════════════════════════════════════
        for dd in proposal.derived_dimensions:
            if dd.source_column not in all_columns:
                questions.append(OpenQuestion(
                    question_id="V2",
                    source="proposal_validator",
                    field_ref=f"derived_dimensions.{dd.dimension_name}.source_column",
                    description=f"源列 '{dd.source_column}' 不在 SourceManifest 中",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V3: time_function 白名单（仅 HOUR）
        # ════════════════════════════════════════════
        for dd in proposal.derived_dimensions:
            if dd.time_function not in {"HOUR"}:
                questions.append(OpenQuestion(
                    question_id="V3",
                    source="proposal_validator",
                    field_ref=f"derived_dimensions.{dd.dimension_name}.time_function",
                    description=f"时间函数 '{dd.time_function}' 不在白名单中（仅 HOUR）",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V4: metric alias 非空
        # ════════════════════════════════════════════
        for m in proposal.metrics:
            if not m.alias:
                questions.append(OpenQuestion(
                    question_id="V4",
                    source="proposal_validator",
                    field_ref=f"metrics.{m.metric_name}.alias",
                    description=f"指标 '{m.metric_name}' 的 alias 为空",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V5: metric alias 在 proposal 内唯一
        # ════════════════════════════════════════════
        seen_aliases: set[str] = set()
        for m in proposal.metrics:
            if m.alias in seen_aliases:
                questions.append(OpenQuestion(
                    question_id="V5",
                    source="proposal_validator",
                    field_ref=f"metrics.{m.metric_name}.alias",
                    description=f"指标别名 '{m.alias}' 重复",
                    blocking=True,
                ))
                valid = False
            seen_aliases.add(m.alias)

        # ════════════════════════════════════════════
        # V6: derived_dimension dimension_name 在 proposal 内唯一
        # ════════════════════════════════════════════
        seen_dd_names: set[str] = set()
        for dd in proposal.derived_dimensions:
            if dd.dimension_name in seen_dd_names:
                questions.append(OpenQuestion(
                    question_id="V6",
                    source="proposal_validator",
                    field_ref=f"derived_dimensions.{dd.dimension_name}",
                    description=f"派生维度名称 '{dd.dimension_name}' 重复",
                    blocking=True,
                ))
                valid = False
            seen_dd_names.add(dd.dimension_name)

        # ════════════════════════════════════════════
        # V7: CASE WHEN branches 非空
        # ════════════════════════════════════════════
        for rule in proposal.case_when_rules:
            if not rule.branches:
                questions.append(OpenQuestion(
                    question_id="V7",
                    source="proposal_validator",
                    field_ref=f"case_when_rules.{rule.output_column}.branches",
                    description=f"CASE WHEN '{rule.output_column}' 分支列表为空",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V8: CASE WHEN else_value 非空
        # ════════════════════════════════════════════
        for rule in proposal.case_when_rules:
            if not rule.else_value:
                questions.append(OpenQuestion(
                    question_id="V8",
                    source="proposal_validator",
                    field_ref=f"case_when_rules.{rule.output_column}.else_value",
                    description=f"CASE WHEN '{rule.output_column}' 缺少 ELSE 默认值",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V9: CASE WHEN condition 中的列引用在 spec/proposal 中可解析
        # ════════════════════════════════════════════
        for rule in proposal.case_when_rules:
            for i, branch in enumerate(rule.branches):
                refs = self._extract_column_refs(branch.condition)
                for col_ref in refs:
                    if col_ref not in available_names:
                        questions.append(OpenQuestion(
                            question_id="V9",
                            source="proposal_validator",
                            field_ref=(
                                f"case_when_rules.{rule.output_column}.branches[{i}]"
                            ),
                            description=(
                                f"CASE WHEN 条件引用了未知名称 '{col_ref}'"
                            ),
                            blocking=True,
                        ))
                        valid = False

        # ════════════════════════════════════════════
        # V10b: CASE WHEN 条件不含 NOT 节点（LabelNot 拒绝）
        # ════════════════════════════════════════════
        for rule in proposal.case_when_rules:
            for i, branch in enumerate(rule.branches):
                if self._contains_not_node(branch.condition):
                    questions.append(OpenQuestion(
                        question_id="V10b",
                        source="proposal_validator",
                        field_ref=(
                            f"case_when_rules.{rule.output_column}.branches[{i}]"
                        ),
                        description="CASE WHEN 条件含 NOT 节点——MVP 不支持",
                        blocking=True,
                    ))
                    valid = False

        # ════════════════════════════════════════════
        # V11: 与程序员手写字段冲突检测
        # ════════════════════════════════════════════
        declared_dim_names = {d.dimension_name for d in spec.dimensions}
        for d in proposal.dimensions:
            if d.dimension_name in declared_dim_names:
                questions.append(OpenQuestion(
                    question_id="V11",
                    source="proposal_validator",
                    field_ref=f"dimensions.{d.dimension_name}",
                    description=f"维度 '{d.dimension_name}' 与程序员手写声明冲突",
                    blocking=True,
                ))
                valid = False

        # ════════════════════════════════════════════
        # V12: 至少有一个 metric 定义
        # ════════════════════════════════════════════
        if not proposal.metrics:
            questions.append(OpenQuestion(
                question_id="V12",
                source="proposal_validator",
                field_ref="metrics",
                description="Proposal 中未定义任何指标",
                blocking=True,
            ))
            valid = False

        # ════════════════════════════════════════════
        # V13: 所有输出列有映射
        # ════════════════════════════════════════════
        output_col_names = {col.name for col in spec.output_spec.columns}
        mapped_names: set[str] = set()
        for d in proposal.dimensions:
            mapped_names.add(d.dimension_name)
        for dd in proposal.derived_dimensions:
            mapped_names.add(dd.dimension_name)
        for m in proposal.metrics:
            mapped_names.add(m.alias)
        for rule in proposal.case_when_rules:
            mapped_names.add(rule.output_column)
        unmapped = output_col_names - mapped_names
        if unmapped:
            questions.append(OpenQuestion(
                question_id="V13",
                source="proposal_validator",
                field_ref="output_spec.columns",
                description=f"输出列无映射：{', '.join(sorted(unmapped))}",
                blocking=True,
            ))
            valid = False

        return valid, questions

    # ── 工具方法 ──

    @staticmethod
    def _contains_not_node(condition: dict) -> bool:
        """递归检查条件树是否含 NOT（LabelNot）节点。"""
        if isinstance(condition, dict):
            if condition.get("node_type") == "NOT":
                return True
            # 检查 children/child 路径
            for child_key in ("children", "child"):
                child = condition.get(child_key)
                if isinstance(child, list):
                    for c in child:
                        if ProposalValidator._contains_not_node(c):
                            return True
                elif isinstance(child, dict):
                    if ProposalValidator._contains_not_node(child):
                        return True
            # 检查 left/right 路径（AND/OR/COMPARE 的子树）
            for key in ("left", "right"):
                child = condition.get(key)
                if isinstance(child, dict):
                    if ProposalValidator._contains_not_node(child):
                        return True
        return False

    @staticmethod
    def _extract_column_refs(condition: dict) -> list[str]:
        """递归提取条件树中所有列引用。"""
        refs: list[str] = []
        if isinstance(condition, dict):
            node_type = condition.get("node_type")
            # COLUMN_REF 节点
            if node_type == "COLUMN_REF":
                col_name = condition.get("column_name")
                if isinstance(col_name, str):
                    refs.append(col_name)
            # COMPARE 节点的 left 是列名字符串
            elif node_type == "COMPARE":
                left = condition.get("left")
                if isinstance(left, str):
                    refs.append(left)
            # IS_NULL / IS_NOT_NULL 节点的 column 字段
            elif node_type in ("IS_NULL", "IS_NOT_NULL"):
                col = condition.get("column")
                if isinstance(col, str):
                    refs.append(col)
            # 递归遍历子节点
            for key in ("children", "child", "left", "right"):
                child = condition.get(key)
                if isinstance(child, list):
                    for c in child:
                        refs.extend(ProposalValidator._extract_column_refs(c))
                elif isinstance(child, dict):
                    refs.extend(ProposalValidator._extract_column_refs(child))
        return refs
