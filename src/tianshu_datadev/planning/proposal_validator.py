"""ProposalValidator——确定性的 Proposal 正确性校验，不调 LLM。

V1-V11 检查项全覆盖：
- 列存在性（V1-V2）
- 时间函数白名单（V3）
- 指标别名/维度名称唯一性（V4-V6）
- CASE WHEN 结构完整性（V7-V8）
- 条件引用正确性（V9）
- LabelNot 拒绝（V10b）
- 冲突检测（V11）

注意：输出列完整性（原 V12/V13）不在 Validator 职责范围——
由 Pipeline._find_unresolved_derived_columns() 在 Planner + SpecEnricher 均完成后统一阻断。
"""

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    OpenQuestion,
    ParsedDeveloperSpec,
    RatioProposal,
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
        # 输出列名也纳入合法引用范围——CASE WHEN post_aggregate 条件
        # 引用聚合输出列（如 killed_person_count）是正确语义，不依赖 Planner
        # 是否生成匹配的 MetricDecl。真正的字段级校验由 SqlBuildPlanValidator 负责。
        for col in spec.output_spec.columns:
            available_names.add(col.name)
        for d in spec.dimensions:
            available_names.add(d.dimension_name)
        for dd in spec.derived_dimensions:
            available_names.add(dd.dimension_name)
        for m in spec.metrics:
            available_names.add(m.alias)
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


class RatioProposalValidator:
    """比率候选的确定性校验器——只验证封闭字段与已知依赖。"""

    _NUMERIC_TYPES = {
        "tinyint", "smallint", "int", "integer", "bigint",
        "float", "double", "real", "decimal", "numeric",
        "long", "short",
    }
    _NUMERIC_WINDOWS = {
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
        "SUM", "SUM_OVER", "AVG", "AVG_OVER", "COUNT", "COUNT_OVER",
    }

    def validate(
        self,
        proposal: RatioProposal,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[bool, list[OpenQuestion]]:
        """验证依赖、计算阶段、分母类型和输出冲突。"""
        questions: list[OpenQuestion] = []
        output_names = {column.name for column in spec.output_spec.columns}
        metric_map = {metric.alias: metric for metric in spec.metrics}
        window_map = {
            metric.alias: metric for metric in spec.inferred_window_metrics
        }
        ratio_aliases = {ratio.output_alias for ratio in spec.ratio_metrics}
        post_aggregate_names = set(metric_map) | set(window_map) | ratio_aliases

        def reject(code: str, field: str, description: str) -> None:
            questions.append(OpenQuestion(
                question_id=f"RATIO-{code}-{proposal.output_alias}",
                source="ratio_proposal_validator",
                field_ref=field,
                description=description,
                blocking=True,
            ))

        if proposal.output_alias not in output_names:
            reject(
                "OUTPUT",
                f"ratio_metrics.{proposal.output_alias}",
                f"比率输出 '{proposal.output_alias}' 不在 output_spec.columns 中",
            )

        occupied = (
            set(metric_map)
            | set(window_map)
            | ratio_aliases
            | {dimension.dimension_name for dimension in spec.dimensions}
            | {dimension.dimension_name for dimension in spec.derived_dimensions}
        )
        if proposal.output_alias in occupied:
            reject(
                "CONFLICT",
                f"ratio_metrics.{proposal.output_alias}",
                f"比率输出别名 '{proposal.output_alias}' 与已有声明冲突",
            )

        for role, dependency in (
            ("numerator_alias", proposal.numerator_alias),
            ("denominator_alias", proposal.denominator_alias),
        ):
            if dependency not in post_aggregate_names:
                reject(
                    "DEPENDENCY",
                    f"ratio_metrics.{proposal.output_alias}.{role}",
                    (
                        f"比率依赖 '{dependency}' 不存在，或不是聚合/窗口后的数值输出；"
                        "RatioExpr 只能在 post_aggregate 阶段引用已定义别名"
                    ),
                )

        if (
            proposal.denominator_alias in post_aggregate_names
            and not self._is_numeric_output(
                proposal.denominator_alias, metric_map, window_map, ratio_aliases, manifest
            )
        ):
            reject(
                "DENOMINATOR_TYPE",
                f"ratio_metrics.{proposal.output_alias}.denominator_alias",
                (
                    f"分母 '{proposal.denominator_alias}' 的类型不能确定为数值型，"
                    "禁止生成除法"
                ),
            )

        if proposal.confidence == "low":
            reject(
                "CONFIDENCE",
                f"ratio_metrics.{proposal.output_alias}",
                f"比率 '{proposal.output_alias}' 置信度为 low，需人工确认",
            )

        return not questions, questions

    def _is_numeric_output(
        self,
        alias: str,
        metric_map: dict,
        window_map: dict,
        ratio_aliases: set[str],
        manifest: SourceManifest,
    ) -> bool:
        """根据聚合/窗口声明和 Manifest 确定输出是否可作为分母。"""
        if alias in ratio_aliases:
            return True

        metric = metric_map.get(alias)
        if metric is not None:
            if metric.aggregation in {
                AggregationType.COUNT,
                AggregationType.COUNT_DISTINCT,
            }:
                return True
            return self._column_is_numeric(metric.input_column, manifest)

        window = window_map.get(alias)
        if window is None:
            return False
        if str(window.window_function).upper() in self._NUMERIC_WINDOWS:
            return True
        return self._column_is_numeric(window.input_column, manifest)

    def _column_is_numeric(
        self,
        column_name: str | None,
        manifest: SourceManifest,
    ) -> bool:
        """仅依据 SourceManifest 的显式类型判断数值列。"""
        if not column_name:
            return False
        for table in manifest.tables:
            for column in table.columns:
                if column.column_name != column_name:
                    continue
                data_type = column.data_type.lower().split("(", 1)[0].strip()
                return data_type in self._NUMERIC_TYPES
        return False
