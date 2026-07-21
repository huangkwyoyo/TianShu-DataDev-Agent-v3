"""DataTransformContractExtractor——从 SqlBuildPlan 确定性抽取 DataTransformContract-lite。

抽取是确定性的——相同 SqlBuildPlan 产生相同 DataTransformContract 和相同 hash。
不包含 SQL 代码字段，不依赖 SqlProgram，不依赖 LLM。
"""

from __future__ import annotations

from tianshu_datadev.planning.models import ColumnRef, DerivedGroupKey, TimeTransformExpr
from tianshu_datadev.planning.relationship_hypothesis import RelationshipEvidence
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    WindowStep,
)
from tianshu_datadev.planning.sql_program import SqlProgram
from tianshu_datadev.sql.write_plan import FinalWritePlan

from .models import (
    CaseWhenBranchSpec,
    CaseWhenCondition,
    CaseWhenLabelSpec,
    ContractAggregation,
    ContractColumn,
    ContractInputTable,
    ContractJoin,
    ContractLimit,
    ContractOutputColumn,
    ContractPredicate,
    ContractSort,
    ContractTimeTransform,
    DataTransformContractLite,
    DataTransformContractV1,
    WindowSpecSummary,
)


class DataTransformContractExtractor:
    """从 SqlBuildPlan/SqlProgram 确定性抽取 DataTransformContract。

    extract():    SqlBuildPlan → DataTransformContractLite（Phase 2）
    extract_v1(): SqlProgram  → DataTransformContractV1（Phase 3 Exit）

    lite 抽取策略：
    - 输入表/列 → 从 ScanStep 提取
    - 过滤条件 → 从 FilterStep 提取（Predicate → 人类可读描述）
    - Join 关系 → 从 JoinStep 提取（含 evidence_chain）
    - 聚合/分组 → 从 AggregateStep 提取
    - 输出列 → 从 ProjectStep 提取
    - 排序/行限制 → 从 SortStep/LimitStep 提取

    v1 在 lite 基础上聚合 SqlProgram 全部 statements：
    - step_dag → 从 SqlProgram.statements[].depends_on 派生
    - temp_tables → 从 SqlProgram.temp_tables
    - case_when_labels → 从所有 CaseWhenStep 聚合
    - window_specs → 从所有 WindowStep 聚合
    - write_spec → 可选 FinalWritePlan

    相同输入 → 相同 Contract → 相同 hash。
    """

    def extract(
        self,
        plan: SqlBuildPlan,
        evidence_map: dict[str, RelationshipEvidence] | None = None,
        output_grain: list[str] | None = None,
    ) -> DataTransformContractLite:
        """从 SqlBuildPlan 确定性抽取 DataTransformContract-lite。

        Args:
            plan: 已验证的 SqlBuildPlan
            evidence_map: Join candidate_id → RelationshipEvidence 的映射，
                         用于填充 join_relationships 中的 evidence_chain。
                         若为 None，Join 的 evidence_chain 为空。
            output_grain: DeveloperSpec 已声明的输出粒度；未提供时回退聚合分组键。

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
        time_transforms: list[ContractTimeTransform] = []
        # ── Phase 3B 字段——lite 路径也需要提取以通过 adapt_lite_to_v1 传递 ──
        case_when_labels: list[CaseWhenLabelSpec] = []
        window_specs: list[WindowSpecSummary] = []

        # 用于跟踪已添加的表（去重）
        seen_tables: set[str] = set()
        # 用于跟踪已添加的列（去重——按 (table_ref, normalized_name) 去重）
        seen_columns: set[tuple[str, str]] = set()
        window_seen = False

        for step in plan.steps:
            if isinstance(step, ScanStep):
                self._extract_scan(
                    step, input_tables, input_columns, seen_tables, seen_columns
                )

            elif isinstance(step, FilterStep):
                filters.append(self._extract_filter(
                    step,
                    phase="post_window" if window_seen else "pre_transform",
                ))

            elif isinstance(step, JoinStep):
                join_rel = self._extract_join(step, evidence_map)
                if join_rel:
                    join_relationships.append(join_rel)

            elif isinstance(step, AggregateStep):
                aggs, groups, biz_keys, time_transforms = self._extract_aggregate(step)
                aggregations.extend(aggs)
                grouping_keys.extend(groups)
                business_keys.extend(biz_keys)

            elif isinstance(step, ProjectStep):
                output_columns = self._extract_project(step)

            elif isinstance(step, SortStep):
                sort_spec = self._extract_sort(step)

            elif isinstance(step, LimitStep):
                limit_spec = self._extract_limit(step)

            elif isinstance(step, CaseWhenStep):
                # Phase 3B：lite 路径必须提取 CASE WHEN，否则
                # adapt_lite_to_v1 会硬编码 case_when_labels=[] 静默丢弃。
                case_when_labels.extend(
                    self._extract_case_when_v1(step, "main")
                )

            elif isinstance(step, WindowStep):
                # Phase 3B：lite 路径必须提取 Window 规格
                window_specs.extend(
                    self._extract_window_v1(step, "main")
                )
                window_seen = True

        # ── 将 CASE WHEN / Window 输出列合并到 output_columns ──
        # ProjectStep 仅包含基础 SELECT 列，不包含 CaseWhenStep/WindowStep
        # 生成的派生列。SQL 编译器会合并它们（compiler.py:639），
        # 但 Contract 提取器必须显式追加，否则 PySpark select() 缺列。
        output_columns = self._merge_derived_output_columns(
            output_columns, case_when_labels, window_specs,
        )
        if aggregations:
            output_columns = self._clear_post_aggregate_qualifiers(output_columns)

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
            output_grain=(
                list(output_grain) if output_grain is not None else grouping_keys
            ),
            sort_spec=sort_spec,
            limit_spec=limit_spec,
            business_keys=business_keys,
            time_transforms=time_transforms,
            semantic_policy_ref="",
            case_when_labels=case_when_labels,
            window_specs=window_specs,
        )

        return contract

    # ── 派生列合并辅助 ──

    @staticmethod
    def _merge_derived_output_columns(
        output_columns: list[ContractOutputColumn],
        case_when_labels: list[CaseWhenLabelSpec],
        window_specs: list[WindowSpecSummary],
    ) -> list[ContractOutputColumn]:
        """将 CASE WHEN / Window 派生列追加到 output_columns。

        ProjectStep 仅包含基础 SELECT 列——CaseWhenStep 和 WindowStep
        的输出列不会出现在 ProjectStep.columns 中。SQL 编译器在末尾合并
        它们（compiler.py:639），但 Contract 提取器必须显式追加，
        否则 PySpark 的 .select() 会丢失这些列。

        去重逻辑：按 column_name 去重，保留已存在的列（ProjectStep 优先）。

        Args:
            output_columns: 从 ProjectStep 提取的输出列
            case_when_labels: 从所有 CaseWhenStep 提取的标签规格
            window_specs: 从所有 WindowStep 提取的窗口规格

        Returns:
            合并后的 output_columns（包含派生列）
        """
        existing_names = {oc.column_name for oc in output_columns}
        result = list(output_columns)

        for cw in case_when_labels:
            if cw.output_alias and cw.output_alias not in existing_names:
                result.append(ContractOutputColumn(
                    column_name=cw.output_alias,
                    alias=cw.output_alias,
                    data_type="unknown",
                ))
                existing_names.add(cw.output_alias)

        for ws in window_specs:
            if ws.alias and ws.alias not in existing_names:
                result.append(ContractOutputColumn(
                    column_name=ws.alias,
                    alias=ws.alias,
                    data_type="unknown",
                ))
                existing_names.add(ws.alias)

        return result

    @staticmethod
    def _clear_post_aggregate_qualifiers(
        output_columns: list[ContractOutputColumn],
    ) -> list[ContractOutputColumn]:
        """聚合结果列已脱离源表命名空间，最终投影不得保留表限定符。"""
        return [
            column.model_copy(update={"source_table_ref": ""})
            for column in output_columns
        ]

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
    def _extract_filter(
        step: FilterStep,
        phase: str = "pre_transform",
    ) -> ContractPredicate:
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
            phase=phase,
        )

    @staticmethod
    def _extract_join(
        step: JoinStep,
        evidence_map: dict[str, RelationshipEvidence],
        temp_column_lineage: dict[tuple[str, str], ColumnRef] | None = None,
    ) -> ContractJoin | None:
        """从 JoinStep 提取 Join 关系（含证据链）。"""
        if not step.join_keys:
            return None

        # 取第一对 join key（Phase 1B 仅支持单 key Join）
        left_key, right_key = step.join_keys[0]
        left_key = DataTransformContractExtractor._resolve_column_lineage(
            left_key,
            temp_column_lineage or {},
        )
        right_key = DataTransformContractExtractor._resolve_column_lineage(
            right_key,
            temp_column_lineage or {},
        )

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
    ) -> tuple[list[ContractAggregation], list[str], list[str], list[ContractTimeTransform]]:
        """从 AggregateStep 提取聚合、分组键、业务键和时间变换。"""
        aggs: list[ContractAggregation] = []
        groups: list[str] = []
        biz_keys: list[str] = []
        time_transforms: list[ContractTimeTransform] = []

        # 聚合指标
        for m in step.metrics:
            aggs.append(
                ContractAggregation(
                    function=m.aggregation if isinstance(m.aggregation, str) else m.aggregation,
                    input_column=m.input_column,
                    alias=m.alias,
                )
            )

        # 分组键：DerivedGroupKey → time_transform + alias key
        # ColumnRef → normalized_name + business_key
        for gk in step.group_keys:
            if isinstance(gk, DerivedGroupKey):
                groups.append(gk.alias)
                time_transforms.append(
                    ContractTimeTransform(
                        source_column=gk.expr.source_column,
                        source_table=gk.expr.source_table,
                        time_function=gk.expr.time_function,
                        alias=gk.alias,
                    )
                )
            elif isinstance(gk, ColumnRef):
                groups.append(gk.normalized_name)
                biz_keys.append(gk.normalized_name)

        return aggs, groups, biz_keys, time_transforms

    @staticmethod
    def _extract_project(
        step: ProjectStep,
        temp_column_lineage: dict[tuple[str, str], ColumnRef] | None = None,
    ) -> list[ContractOutputColumn]:
        """从 ProjectStep 提取输出列。"""
        cols: list[ContractOutputColumn] = []
        for ae in step.columns:
            expression = ae.expression
            if isinstance(expression, ColumnRef):
                expression = DataTransformContractExtractor._resolve_column_lineage(
                    expression,
                    temp_column_lineage or {},
                )
                col_name = expression.column_name
                source_table_ref = expression.table_ref
            else:
                col_name = str(expression)
                source_table_ref = ""
            cols.append(
                ContractOutputColumn(
                    column_name=col_name,
                    alias=ae.alias,
                    data_type="unknown",
                    source_table_ref=source_table_ref,
                )
            )
        return cols

    @staticmethod
    def _resolve_column_lineage(
        column: ColumnRef,
        temp_column_lineage: dict[tuple[str, str], ColumnRef],
    ) -> ColumnRef:
        """沿结构化 ProjectStep 血缘解析临时表列，禁止读取 SQL 文本。"""
        current = column
        visiting: set[tuple[str, str]] = set()
        while current.table_ref.startswith("_temp_"):
            key = (current.table_ref, current.column_name)
            if key in visiting:
                raise ValueError(f"临时表列血缘存在循环：{key}")
            visiting.add(key)
            upstream = temp_column_lineage.get(key)
            if upstream is None:
                break
            current = upstream
        return current

    @staticmethod
    def _record_temp_column_lineage(
        temp_table: str,
        plan: SqlBuildPlan,
        temp_column_lineage: dict[tuple[str, str], ColumnRef],
    ) -> None:
        """记录生产语句 ProjectStep 输出列到原始 ColumnRef 的映射。"""
        project_steps = [
            step for step in plan.steps if isinstance(step, ProjectStep)
        ]
        if not project_steps:
            return
        for alias_expr in project_steps[-1].columns:
            if not isinstance(alias_expr.expression, ColumnRef):
                continue
            output_name = alias_expr.alias or alias_expr.expression.column_name
            temp_column_lineage[(temp_table, output_name)] = (
                DataTransformContractExtractor._resolve_column_lineage(
                    alias_expr.expression,
                    temp_column_lineage,
                )
            )

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
        # TimeTransformExpr（v3.1 新增）
        if hasattr(operand, "time_function") and hasattr(operand, "source_column"):
            return f"{operand.time_function}({operand.source_table}.{operand.source_column})"
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

    # ── v1 抽取：SqlProgram → DataTransformContractV1 ──

    def extract_v1(
        self,
        sql_program: SqlProgram,
        write_plan: FinalWritePlan | None = None,
        evidence_map: dict[str, RelationshipEvidence] | None = None,
        output_grain: list[str] | None = None,
    ) -> DataTransformContractV1:
        """从 SqlProgram 确定性抽取 DataTransformContract v1。

        聚合 SqlProgram 中全部 statement 的 lite 字段，
        并新增 step_dag、temp_tables、case_when_labels、window_specs、write_spec。

        Args:
            sql_program: 经过 DAG 校验的 SqlProgram
            write_plan: 可选的 FinalWritePlan——若提供则写入 write_spec 字段
            evidence_map: Join candidate_id → RelationshipEvidence 的映射，
                         用于填充 join_relationships 中的 evidence_chain。
                         若为 None，Join 的 evidence_chain 为空。
            output_grain: DeveloperSpec 已声明的输出粒度；未提供时回退聚合分组键。

        Returns:
            DataTransformContractV1——不包含 SQL 代码字段

        Raises:
            ValueError: SqlProgram 不含任何 statement
        """
        if not sql_program.statements:
            raise ValueError("SqlProgram 不含任何 statement，无法抽取 Contract v1")

        evidence_map = evidence_map or {}

        program_id = sql_program.program_id

        # ── 聚合所有 statement 的 lite 等价字段 ──
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
        time_transforms: list[ContractTimeTransform] = []

        seen_tables: set[str] = set()
        seen_columns: set[tuple[str, str]] = set()
        temp_column_lineage: dict[tuple[str, str], ColumnRef] = {}

        # ── v1 新增字段收集 ──
        step_dag: dict[str, list[str]] = {}
        case_when_labels: list[CaseWhenLabelSpec] = []
        window_specs: list[WindowSpecSummary] = []

        # 遍历所有 statement，按优先级聚合：
        # - 最终 statement（FINAL/STANDALONE）的 sort/limit 优先
        # - 聚合时 group_keys 取并集
        for stmt in sql_program.statements:
            plan = stmt.plan
            sid = stmt.statement_id
            window_seen = False

            # step_dag 条目
            step_dag[sid] = list(stmt.depends_on)

            # 遍历 plan 的所有 step
            for step in plan.steps:
                if isinstance(step, ScanStep):
                    # _temp_* 表是 DAG 内部管道——不进入 Contract
                    if step.table_ref.startswith("_temp_"):
                        continue
                    self._extract_scan(
                        step, input_tables, input_columns, seen_tables, seen_columns,
                    )
                elif isinstance(step, FilterStep):
                    filters.append(self._extract_filter(
                        step,
                        phase="post_window" if window_seen else "pre_transform",
                    ))
                elif isinstance(step, JoinStep):
                    # 两个临时关系之间的 Join 属于 DAG 内部编排；临时结果与外部表
                    # 的 Join 仍是业务语义，必须还原结构化列血缘后进入 Contract。
                    if step.join_keys and any(
                        k[0].table_ref.startswith("_temp_")
                        and k[1].table_ref.startswith("_temp_")
                        for k in step.join_keys
                    ):
                        continue
                    join_rel = self._extract_join(
                        step,
                        evidence_map,
                        temp_column_lineage,
                    )
                    if join_rel:
                        join_relationships.append(join_rel)
                elif isinstance(step, AggregateStep):
                    aggs, groups, biz_keys, time_transforms = self._extract_aggregate(step)
                    aggregations.extend(aggs)
                    grouping_keys.extend(groups)
                    business_keys.extend(biz_keys)
                elif isinstance(step, ProjectStep):
                    output_columns = self._extract_project(
                        step,
                        temp_column_lineage,
                    )
                elif isinstance(step, SortStep):
                    sort_spec = self._extract_sort(step)
                elif isinstance(step, LimitStep):
                    limit_spec = self._extract_limit(step)
                elif isinstance(step, CaseWhenStep):
                    case_when_labels.extend(
                        self._extract_case_when_v1(step, sid)
                    )
                elif isinstance(step, WindowStep):
                    window_specs.extend(
                        self._extract_window_v1(step, sid)
                    )
                    window_seen = True

            if stmt.produces:
                self._record_temp_column_lineage(
                    stmt.produces,
                    plan,
                    temp_column_lineage,
                )

        # ── 将 CASE WHEN / Window 派生列合并到 output_columns ──
        # 与 lite 路径相同：ProjectStep 不含 CaseWhenStep/WindowStep 生成的派生列，
        # SQL 编译器合并它们，Contract 提取器也必须显式追加。
        output_columns = self._merge_derived_output_columns(
            output_columns, case_when_labels, window_specs,
        )
        if aggregations:
            output_columns = self._clear_post_aggregate_qualifiers(output_columns)

        # ── temp_tables 序列化 ──
        temp_tables: list[dict] = [
            tt.model_dump() for tt in sql_program.temp_tables
        ]

        # ── 生成确定性 contract ID ──
        contract_id = DataTransformContractV1.generate_contract_id(program_id)

        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=input_tables,
            input_columns=input_columns,
            join_relationships=join_relationships,
            filters=filters,
            aggregations=aggregations,
            grouping_keys=grouping_keys,
            output_columns=output_columns,
            output_grain=(
                list(output_grain) if output_grain is not None else grouping_keys
            ),
            sort_spec=sort_spec,
            limit_spec=limit_spec,
            business_keys=business_keys,
            time_transforms=time_transforms,
            semantic_policy_ref="",
            step_dag=step_dag,
            temp_tables=temp_tables,
            case_when_labels=case_when_labels,
            window_specs=window_specs,
            write_spec=write_plan.model_dump() if write_plan else None,
        )

        return contract

    # ── v1 专用 Step 抽取辅助 ──

    @staticmethod
    def _extract_case_when_v1(
        step: CaseWhenStep, statement_id: str,
    ) -> list[CaseWhenLabelSpec]:
        """从 CaseWhenStep 提取 CASE WHEN 标签规格。

        一个 CaseWhenStep 产生一个标签列，同时提取 labels（展示兼容）和
        branches（结构化条件）两个平行列表。

        Args:
            step: CaseWhenStep 实例
            statement_id: 所属语句 ID

        Returns:
            CaseWhenLabelSpec 列表（当前每个 step 对应一个 spec）

        Raises:
            ValueError: 遇到不支持的 PredicateOperator（IN/BETWEEN/LIKE 等）
        """
        labels: list[str] = []
        else_label: str | None = None

        for branch in step.cases:
            result = branch.result
            if hasattr(result, "value"):
                labels.append(str(result.value))

        if step.else_value is not None and hasattr(step.else_value, "value"):
            else_label = str(step.else_value.value)

        # 提取结构化条件分支——与 labels 平行
        branches_spec: list[CaseWhenBranchSpec] = []
        for branch in step.cases:
            if branch.condition is not None:
                # 结构化条件分支（简单比较，如 status = 'VIP'）
                cond = DataTransformContractExtractor._predicate_to_case_when_condition(
                    branch.condition,
                )
            else:
                # raw_condition 模式——复杂布尔表达式（如 A OR B / A AND B）
                # 无法分解为 CaseWhenCondition 叶子/逻辑节点。
                # 创建占位节点承载原始表达式前 200 字符，
                # Spark compiler 遇到 COMPLEX_RAW 时会抛出 RenderError 阻断——
                # 这是预期行为：复杂表达式当前不走结构化 contract 路径。
                raw_text = (
                    branch.raw_condition.sql_fragment[:200]
                    if branch.raw_condition is not None and hasattr(branch.raw_condition, "sql_fragment")
                    else ""
                )
                cond = CaseWhenCondition(
                    operator="COMPLEX_RAW",
                    normalized_name=raw_text,
                )
            label = str(branch.result.value) if hasattr(branch.result, "value") else ""
            branches_spec.append(CaseWhenBranchSpec(label=label, condition=cond))

        return [
            CaseWhenLabelSpec(
                statement_id=statement_id,
                output_alias=step.alias,  # 业务列名，非 step_id
                branch_count=len(step.cases),
                labels=labels,
                else_label=else_label,
                branches=branches_spec,
            )
        ]

    # ── Predicate → CaseWhenCondition 转换辅助 ──

    @staticmethod
    def _extract_column_ref(col) -> tuple[str, str]:
        """从 ColumnRef 提取 (table_ref, normalized_name)。

        仅接受有 table_ref 和 normalized_name 属性的对象（ColumnRef）。
        非列引用类型（如嵌套 Predicate）说明上游构造了非法 CASE WHEN 条件，
        必须拒绝而非静默字符串化——与 _extract_literal_value 的类型守卫对称。

        Raises:
            ValueError: col 不是 ColumnRef（缺少 table_ref 或 normalized_name 属性）
        """
        if hasattr(col, "table_ref") and hasattr(col, "normalized_name"):
            return col.table_ref, col.normalized_name
        raise ValueError(
            f"CASE WHEN 左侧仅支持 ColumnRef（列引用），"
            f"收到 {type(col).__name__}。嵌套表达式/子查询不支持"
        )

    @staticmethod
    def _extract_literal_value(lit) -> str | int | float | bool | None:
        """从 SqlLiteral 提取原始类型的值——保留 int/float/bool，不转字符串。

        仅接受有 value 属性的对象（SqlLiteral）。非字面量类型（如 ColumnRef）
        说明上游 Predicate 的右侧不是常量，CASE WHEN 当前不支持列-列比较，
        必须拒绝而非静默字符串化。

        Raises:
            ValueError: lit 不是 SqlLiteral（无 value 属性）
        """
        if hasattr(lit, "value"):
            return lit.value  # 保留原始类型：int、float、bool、str、None
        raise ValueError(
            f"CASE WHEN 右侧仅支持 SqlLiteral（字面量），"
            f"收到 {type(lit).__name__}。列-列比较等复杂表达式不支持"
        )

    @staticmethod
    def _predicate_to_case_when_condition(
        pred,
        derived_expr_map: dict | None = None,
    ) -> CaseWhenCondition:
        """将 Predicate AST 递归转换为 CaseWhenCondition 扁平表示。

        支持：EQ/NEQ/GT/GTE/LT/LTE/IS_NULL/IS_NOT_NULL/AND/OR。
        不支持 IN/BETWEEN/LIKE/NOT——遇到即抛 ValueError 阻断。

        Args:
            pred: planning.models.Predicate 实例
            derived_expr_map: {alias: TimeTransformExpr} 映射，用于将
                             TimeTransformExpr 操作数反查为 alias 列名

        Returns:
            CaseWhenCondition——扁平序列化表示

        Raises:
            ValueError: 遇到不支持的 PredicateOperator
        """
        op = pred.operator.value if hasattr(pred.operator, "value") else str(pred.operator)

        # 解析 left 操作数：TimeTransformExpr → alias（v3.1 新增）
        left_operand = pred.left
        if isinstance(left_operand, TimeTransformExpr):
            derived_expr_map = derived_expr_map or {}
            # 构建 TimeTransformExpr → alias 反向映射
            expr_to_alias: dict[tuple, str] = {}
            for alias, expr in derived_expr_map.items():
                if hasattr(expr, "source_table") and hasattr(expr, "source_column"):
                    k = (str(expr.source_table), str(expr.source_column), expr.time_function)
                    expr_to_alias[k] = alias
            tte_key = (
                str(left_operand.source_table),
                str(left_operand.source_column),
                left_operand.time_function,
            )
            alias = expr_to_alias.get(tte_key)
            if alias is None:
                raise ValueError(
                    f"TimeTransformExpr({tte_key}) 在 derived_expr_map 中无对应 alias"
                )
            # 用 alias 合成 ColumnRef 使下游逻辑不改动
            left_operand = ColumnRef(
                table_ref=left_operand.source_table,
                column_name=alias,
                normalized_name=alias,
            )

        # 一元操作符
        if op in ("IS_NULL", "IS_NOT_NULL"):
            table, name = DataTransformContractExtractor._extract_column_ref(left_operand)
            return CaseWhenCondition(
                operator=op, table_ref=table, normalized_name=name,
            )

        # 二元比较——右侧必须是 SqlLiteral（字面量），不支持列-列比较
        if op in ("EQ", "NEQ", "GT", "GTE", "LT", "LTE"):
            table, name = DataTransformContractExtractor._extract_column_ref(left_operand)
            if not hasattr(pred.right, "value"):
                raise ValueError(
                    f"CASE WHEN 条件 operator='{op}' 右侧必须是 SqlLiteral（字面量），"
                    f"收到 {type(pred.right).__name__}。列-列比较不支持"
                )
            val = DataTransformContractExtractor._extract_literal_value(pred.right)
            return CaseWhenCondition(
                operator=op, table_ref=table, normalized_name=name, value=val,
            )

        # 逻辑组合
        if op in ("AND", "OR"):
            left_c = DataTransformContractExtractor._predicate_to_case_when_condition(
                pred.left,
                derived_expr_map,
            )
            right_c = DataTransformContractExtractor._predicate_to_case_when_condition(
                pred.right,
                derived_expr_map,
            )
            return CaseWhenCondition(operator=op, left=left_c, right=right_c)

        # 不支持的操作符——阻断，不平替
        raise ValueError(
            f"Contract CASE WHEN 不支持 PredicateOperator.{op}，"
            f"需扩展 _predicate_to_case_when_condition 或由上游拒绝该条件"
        )

    @staticmethod
    def _extract_window_v1(
        step: WindowStep, statement_id: str,
    ) -> list[WindowSpecSummary]:
        """从 WindowStep 提取窗口函数规格摘要。

        每个 WindowExpr 生成一个 WindowSpecSummary。

        Args:
            step: WindowStep 实例
            statement_id: 所属语句 ID

        Returns:
            WindowSpecSummary 列表
        """
        specs: list[WindowSpecSummary] = []
        for wexpr in step.window_exprs:
            func = wexpr.function.value if hasattr(wexpr.function, "value") else str(wexpr.function)
            alias = wexpr.alias

            # 分区键——归一化名
            partition_by = [
                cr.normalized_name
                if hasattr(cr, "normalized_name")
                else str(cr)
                for cr in wexpr.partition_by
            ]

            # 排序键必须保留方向，否则 SQL DESC 会在 Spark 中退化为 ASC。
            order_by = [
                (
                    f"{s.column} "
                    f"{s.direction.value if hasattr(s.direction, 'value') else s.direction}"
                )
                if hasattr(s, "column") else str(s)
                for s in wexpr.order_by
            ]

            # 提取输入列/参数——从 WindowExpr.input 提取
            # ColumnRef → 列名字符串；SqlLiteral → str(value)（NTILE 桶数）
            # None → None（排名函数无需参数）
            input_column: str | None = None
            if wexpr.input is not None:
                if hasattr(wexpr.input, "column_name"):
                    # ColumnRef——LAG/LEAD/SUM_OVER/AVG_OVER/COUNT_OVER
                    input_column = wexpr.input.column_name
                elif hasattr(wexpr.input, "value"):
                    # SqlLiteral——NTILE(n)
                    input_column = str(wexpr.input.value)

            specs.append(
                WindowSpecSummary(
                    statement_id=statement_id,
                    function=func,
                    alias=alias,
                    input_column=input_column,
                    partition_by=partition_by,
                    order_by=order_by,
                )
            )
        return specs

    # ── 静态工具方法 ──

    @staticmethod
    def compute_contract_hash(contract: DataTransformContractLite) -> str:
        """计算 contract 的确定性 SHA-256。"""
        return DataTransformContractLite.compute_contract_hash(contract)
