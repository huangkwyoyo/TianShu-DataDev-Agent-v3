"""DataTransformContractExtractor——从 SqlBuildPlan 确定性抽取 DataTransformContract-lite。

抽取是确定性的——相同 SqlBuildPlan 产生相同 DataTransformContract 和相同 hash。
不包含 SQL 代码字段，不依赖 SqlProgram，不依赖 LLM。
"""

from __future__ import annotations

from tianshu_datadev.planning.relationship_hypothesis import RelationshipEvidence
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)

from .models import (
    ContractAggregation,
    ContractColumn,
    ContractInputTable,
    ContractJoin,
    ContractLimit,
    ContractOutputColumn,
    ContractPredicate,
    ContractSort,
    DataTransformContractLite,
)


class DataTransformContractExtractor:
    """从 SqlBuildPlan 确定性抽取 DataTransformContract-lite。

    抽取策略：
    - 输入表/列 → 从 ScanStep 提取
    - 过滤条件 → 从 FilterStep 提取（Predicate → 人类可读描述）
    - Join 关系 → 从 JoinStep 提取（含 evidence_chain）
    - 聚合/分组 → 从 AggregateStep 提取
    - 输出列 → 从 ProjectStep 提取
    - 排序/行限制 → 从 SortStep/LimitStep 提取

    相同 SqlBuildPlan → 相同 DataTransformContractLite → 相同 hash。
    """

    def extract(
        self,
        plan: SqlBuildPlan,
        evidence_map: dict[str, RelationshipEvidence] | None = None,
    ) -> DataTransformContractLite:
        """从 SqlBuildPlan 确定性抽取 DataTransformContract-lite。

        Args:
            plan: 已验证的 SqlBuildPlan
            evidence_map: Join candidate_id → RelationshipEvidence 的映射，
                         用于填充 join_relationships 中的 evidence_chain。
                         若为 None，Join 的 evidence_chain 为空。

        Returns:
            DataTransformContractLite——不包含 SQL 代码字段
        """
        evidence_map = evidence_map or {}

        # 计算 source plan hash
        plan_hash = SqlBuildPlan.generate_plan_hash(plan)

        # 收集各步骤信息
        input_tables: list[ContractInputTable] = []
        input_columns: list[ContractColumn] = []
        join_relationships: list[ContractJoin] = []
        filters: list[ContractPredicate] = []
        aggregations: list[ContractAggregation] = []
        grouping_keys: list[str] = []
        output_columns: list[ContractOutputColumn] = []
        sort_spec: list[ContractSort] | None = None
        limit_spec: ContractLimit | None = None
        business_keys: list[str] = []

        # 用于跟踪已添加的表（去重）
        seen_tables: set[str] = set()
        # 用于跟踪已添加的列（去重——按 (table_ref, normalized_name) 去重）
        seen_columns: set[tuple[str, str]] = set()

        for step in plan.steps:
            if isinstance(step, ScanStep):
                self._extract_scan(
                    step, input_tables, input_columns, seen_tables, seen_columns
                )

            elif isinstance(step, FilterStep):
                filters.append(self._extract_filter(step))

            elif isinstance(step, JoinStep):
                join_rel = self._extract_join(step, evidence_map)
                if join_rel:
                    join_relationships.append(join_rel)

            elif isinstance(step, AggregateStep):
                aggs, groups, biz_keys = self._extract_aggregate(step)
                aggregations.extend(aggs)
                grouping_keys.extend(groups)
                business_keys.extend(biz_keys)

            elif isinstance(step, ProjectStep):
                output_columns = self._extract_project(step)

            elif isinstance(step, SortStep):
                sort_spec = self._extract_sort(step)

            elif isinstance(step, LimitStep):
                limit_spec = self._extract_limit(step)

        # 生成确定性 contract ID
        contract_id = DataTransformContractLite.generate_contract_id(plan_hash)

        contract = DataTransformContractLite(
            contract_id=contract_id,
            version="lite",
            source_phase="phase-2",
            source_sqlbuildplan_hash=plan_hash,
            input_tables=input_tables,
            input_columns=input_columns,
            join_relationships=join_relationships,
            filters=filters,
            aggregations=aggregations,
            grouping_keys=grouping_keys,
            output_columns=output_columns,
            output_grain=grouping_keys,  # 输出粒度 = 分组键
            sort_spec=sort_spec,
            limit_spec=limit_spec,
            business_keys=business_keys,
            semantic_policy_ref="",
        )

        return contract

    # ── Step 抽取辅助 ──

    @staticmethod
    def _extract_scan(
        step: ScanStep,
        input_tables: list[ContractInputTable],
        input_columns: list[ContractColumn],
        seen_tables: set[str],
        seen_columns: set[tuple[str, str]],
    ) -> None:
        """从 ScanStep 提取输入表和列。"""
        # 表（去重）
        if step.table_ref not in seen_tables:
            seen_tables.add(step.table_ref)
            input_tables.append(
                ContractInputTable(
                    table_ref=step.table_ref,
                    source_table=step.table_ref,  # ScanStep 中 table_ref 即表标识
                    estimated_row_count=step.estimated_row_count,
                )
            )

        # 列（去重）
        for col in step.required_columns:
            key = (col.table_ref or step.table_ref, col.normalized_name)
            if key not in seen_columns:
                seen_columns.add(key)
                # 尝试从 ColumnRef 推断类型——Phase 2 中类型信息有限，
                # 从 normalized_name 无法推断时填 "unknown"
                input_columns.append(
                    ContractColumn(
                        column_name=col.column_name,
                        normalized_name=col.normalized_name,
                        data_type="unknown",
                        table_ref=col.table_ref or step.table_ref,
                    )
                )

    @staticmethod
    def _extract_filter(step: FilterStep) -> ContractPredicate:
        """从 FilterStep 提取结构化过滤条件——不含自由文本表达式。

        人类可读的表达式渲染由 review.md 层负责，
        Contract 仅保留 left/operator/right 结构化三元组。
        """
        pred = step.predicate
        left_str = DataTransformContractExtractor._render_operand(pred.left)
        right_str = DataTransformContractExtractor._render_operand(pred.right) if pred.right else ""
        op_str = pred.operator.value if hasattr(pred.operator, "value") else str(pred.operator)

        return ContractPredicate(
            operator=op_str,
            left=left_str,
            right=right_str,
        )

    @staticmethod
    def _extract_join(
        step: JoinStep,
        evidence_map: dict[str, RelationshipEvidence],
    ) -> ContractJoin | None:
        """从 JoinStep 提取 Join 关系（含证据链）。"""
        if not step.join_keys:
            return None

        # 取第一对 join key（Phase 1B 仅支持单 key Join）
        left_key, right_key = step.join_keys[0]

        # 构建 evidence_chain
        evidence_chain: dict = {}
        if step.relationship_ref in evidence_map:
            ev = evidence_map[step.relationship_ref]
            evidence_chain = {
                "evidence_id": ev.evidence_id,
                "level": ev.level.value,
                "action": ev.action.value,
                "left_field": {
                    "raw": ev.left_key_raw,
                    "normalized": ev.left_key_normalized,
                },
                "right_field": {
                    "raw": ev.right_key_raw,
                    "normalized": ev.right_key_normalized,
                },
                "evidence_checks": ev.evidence_checks,
                "detail": ev.detail,
            }

        return ContractJoin(
            join_id=step.relationship_ref,
            left_table=left_key.table_ref,
            right_table=right_key.table_ref or step.right_table_ref,
            left_key=left_key.column_name,
            right_key=right_key.column_name,
            join_type=step.join_type.value if hasattr(step.join_type, "value") else str(step.join_type),
            evidence_chain=evidence_chain,
            level=evidence_chain.get("level", "MEDIUM"),
        )

    @staticmethod
    def _extract_aggregate(
        step: AggregateStep,
    ) -> tuple[list[ContractAggregation], list[str], list[str]]:
        """从 AggregateStep 提取聚合、分组键和业务键。"""
        aggs: list[ContractAggregation] = []
        groups: list[str] = []
        biz_keys: list[str] = []

        # 聚合指标
        for m in step.metrics:
            aggs.append(
                ContractAggregation(
                    function=m.aggregation if isinstance(m.aggregation, str) else m.aggregation,
                    input_column=m.input_column,
                    alias=m.alias,
                )
            )

        # 分组键（归一化名）
        for gk in step.group_keys:
            groups.append(gk.normalized_name)
            biz_keys.append(gk.normalized_name)

        return aggs, groups, biz_keys

    @staticmethod
    def _extract_project(step: ProjectStep) -> list[ContractOutputColumn]:
        """从 ProjectStep 提取输出列。"""
        cols: list[ContractOutputColumn] = []
        for ae in step.columns:
            col_name = (
                ae.expression.column_name
                if hasattr(ae.expression, "column_name")
                else str(ae.expression)
            )
            cols.append(
                ContractOutputColumn(
                    column_name=col_name,
                    alias=ae.alias,
                    data_type="unknown",
                )
            )
        return cols

    @staticmethod
    def _extract_sort(step: SortStep) -> list[ContractSort]:
        """从 SortStep 提取排序规格。"""
        sorts: list[ContractSort] = []
        for s in step.order_by:
            direction = s.direction.value if hasattr(s.direction, "value") else str(s.direction)
            sorts.append(
                ContractSort(
                    column=s.column,
                    direction=direction,
                )
            )
        return sorts

    @staticmethod
    def _extract_limit(step: LimitStep) -> ContractLimit:
        """从 LimitStep 提取行限制。"""
        return ContractLimit(
            limit=step.limit,
            offset=step.offset,
        )

    # ── Predicate 操作数渲染 ──

    @staticmethod
    def _render_operand(operand) -> str:
        """将 Predicate 的操作数渲染为人类可读字符串。"""
        if operand is None:
            return "None"
        # ColumnRef
        if hasattr(operand, "table_ref") and hasattr(operand, "column_name"):
            table = operand.table_ref
            col = operand.column_name
            if table:
                return f"{table}.{col}"
            return col
        # SqlLiteral
        if hasattr(operand, "value"):
            v = operand.value
            if v is None:
                return "NULL"
            if isinstance(v, str):
                return f"'{v}'"
            return str(v)
        # 嵌套 Predicate——递归渲染
        if hasattr(operand, "left") and hasattr(operand, "operator"):
            return DataTransformContractExtractor._render_operand(operand)
        return str(operand)

    # ── 静态工具方法 ──

    @staticmethod
    def compute_contract_hash(contract: DataTransformContractLite) -> str:
        """计算 contract 的确定性 SHA-256。"""
        return DataTransformContractLite.compute_contract_hash(contract)
