"""SqlProgram——多语句 SQL 程序 DAG 模型 + 拓扑排序 + 校验。

SqlProgram 将多个 SqlBuildPlan 按 DAG 依赖关系编排为多语句执行单元，
通过 _temp 中间表传递数据。不实现 CTEPlan——SqlProgram + _temp 覆盖所有 CTE 用例。

核心组件：
- StatementKind：语句在 DAG 中的角色枚举
- SqlStatement：单个语句——封装 SqlBuildPlan + DAG 元数据
- SqlProgram：多语句程序——DAG + _temp 表 + 拓扑排序
- topological_sort()：Kahn 算法确定性拓扑排序
- validate_program_dag()：DAG 合法性校验（循环检测 + 缺失依赖 + _temp 引用）
- SqlProgramBuilder：确定性构建器
"""

from __future__ import annotations

import heapq
import logging
from enum import Enum

from tianshu_datadev.developer_spec.models import (
    OpenQuestion,
    ParsedDeveloperSpec,
    StrictModel,
)

from .sql_build_plan import (
    ColumnRef,
    JoinStep,
    ScanStep,
    SqlBuildPlan,
)
from .temp_table import (
    TempTableSpec,
    make_temp_name,
    validate_consumer_is_declared,
    validate_temp_table_naming,
    validate_temp_table_refs,
)

logger = logging.getLogger(__name__)


def _derive_temp_tables_from_statements(
    statements: list[SqlStatement],
) -> list[TempTableSpec]:
    """从 statements 的 produces / ScanStep._temp_ 引用自动推导 TempTableSpec 列表。

    当调用方未显式传入 temp_tables 时，此函数根据：
    - PRODUCER 语句的 produces 字段 → TempTableSpec.produced_by
    - 各语句 ScanStep 中的 _temp_ 表引用 → TempTableSpec.consumed_by

    自动构建完整的 temp_tables，确保 DAG 校验的一致性检查能通过。
    """
    # 收集生产者：{temp_id: produced_by}
    producer_map: dict[str, str] = {}
    for stmt in statements:
        if stmt.produces:
            producer_map[stmt.produces] = stmt.statement_id

    # 收集消费者：{temp_id: [consumed_by_statement_ids]}
    consumer_map: dict[str, list[str]] = {tid: [] for tid in producer_map}
    for stmt in statements:
        temp_refs = _collect_temp_refs_from_plan(stmt.plan)
        for temp_id in temp_refs:
            if temp_id not in consumer_map:
                consumer_map[temp_id] = []
            if stmt.statement_id not in consumer_map[temp_id]:
                consumer_map[temp_id].append(stmt.statement_id)

    # 构建 TempTableSpec 列表——仅包含有生产者的 _temp 表
    # 同时构建 statement_id → plan 映射，用于提取产出列
    stmt_plan_map: dict[str, SqlBuildPlan] = {
        s.statement_id: s.plan for s in statements
    }

    temp_tables: list[TempTableSpec] = []
    for temp_id, produced_by in producer_map.items():
        # 从生产者 Plan 的最后一个 ProjectStep 提取产出列
        col_defs = _extract_output_columns(stmt_plan_map.get(produced_by))
        temp_tables.append(TempTableSpec(
            temp_id=temp_id,
            produced_by=produced_by,
            consumed_by=consumer_map.get(temp_id, []),
            column_defs=col_defs,
        ))

    return temp_tables


def _extract_output_columns(plan: SqlBuildPlan | None) -> list[ColumnRef]:
    """从 SqlBuildPlan 的最后一个 ProjectStep 提取产出列。

    ProjectStep.columns 是 AliasExpr 列表——提取 alias 作为 ColumnRef.column_name，
    确保 TempTableSpec 的 column_defs 类型正确。

    若 plan 为 None 或无 ProjectStep，返回空列表。
    """
    if plan is None:
        return []

    from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer

    _normalizer = FieldNormalizer()

    for step in reversed(plan.steps):
        if step.step_type == "project" and hasattr(step, "columns"):
            col_defs: list[ColumnRef] = []
            for col_expr in step.columns:
                # AliasExpr 有 alias 属性；普通 ColumnRef 直接使用
                if hasattr(col_expr, "alias") and col_expr.alias:
                    col_name = col_expr.alias
                elif hasattr(col_expr, "column_name"):
                    col_name = col_expr.column_name
                else:
                    continue
                col_defs.append(ColumnRef(
                    table_ref="",
                    column_name=col_name,
                    normalized_name=_normalizer.normalize(col_name),
                ))
            return col_defs
    return []


# ════════════════════════════════════════════
# StatementKind 枚举
# ════════════════════════════════════════════


class StatementKind(str, Enum):
    """语句在 SqlProgram DAG 中的角色。

    - PRODUCER：产生 _temp 中间表供下游消费（如中间聚合）
    - CONSUMER：读取上游 _temp 表，可能也产生新的 _temp
    - FINAL：最终输出 SELECT，不产生中间表
    - STANDALONE：单语句程序（无依赖、无中间表）
    """

    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"
    FINAL = "FINAL"
    STANDALONE = "STANDALONE"


# ════════════════════════════════════════════
# SqlStatement——单个语句
# ════════════════════════════════════════════


class SqlStatement(StrictModel):
    """SqlProgram 中的单个语句——封装 SqlBuildPlan + DAG 元数据。

    statement_id 等同于对应 SqlBuildPlan.plan_id，确保溯源一致性。
    depends_on 声明此语句依赖的上游语句，构成 DAG 边。
    produces 声明此语句产生的 _temp 表名（PRODUCER 时非空）。
    """

    statement_id: str  # 等同于对应 SqlBuildPlan.plan_id
    plan: SqlBuildPlan  # 对应的 SqlBuildPlan
    kind: StatementKind  # DAG 角色
    depends_on: list[str] = []  # 依赖的 statement_id 列表
    produces: str | None = None  # 产生的 _temp 表名
    intent: str | None = None  # Builder 填写的业务意图描述——供注释渲染和 ReviewPackage 使用


# ════════════════════════════════════════════
# SqlProgram——多语句程序
# ════════════════════════════════════════════


class SqlProgram(StrictModel):
    """多语句 SQL 程序——DAG 编排多个 SqlBuildPlan。

    steps 中的 SqlBuildPlan 通过 _temp 中间表传递数据，
    执行顺序由 topological_order 确定（Kahn 算法 + 字典序打破平局）。
    """

    program_id: str  # program_{spec_hash[:12]}
    spec_id: str  # 对应 ParsedDeveloperSpec.spec_hash
    statements: list[SqlStatement]  # 语句列表
    temp_tables: list[TempTableSpec] = []  # _temp 中间表声明
    topological_order: list[str] = []  # 确定性拓扑排序结果
    final_output: str | None = None  # 最终输出的 statement_id
    final_output_target: str | None = None  # FINAL 的真实输出目标（表名+分区等）
    # final_output_target 仅 build_from_compute_steps 填写

    @staticmethod
    def generate_program_id(spec_hash: str) -> str:
        """基于 spec_hash 的确定性 program_id。"""
        return f"program_{spec_hash[:12]}"


# ════════════════════════════════════════════
# Kahn 拓扑排序（确定性）
# ════════════════════════════════════════════


def topological_sort(statements: list[SqlStatement]) -> list[str]:
    """Kahn 算法拓扑排序——同级节点按 statement_id 字典序打破平局。

    算法：
    1. 计算每个节点入度（depends_on 边数）
    2. 入度为 0 的节点入最小堆（按 statement_id 字典序排序）
    3. 弹出堆顶节点，将其从所有依赖它的节点入度中减 1
    4. 新入度为 0 的节点入堆
    5. 重复直到堆空

    Args:
        statements: 待排序的语句列表

    Returns:
        statement_id 的拓扑排序列表

    Raises:
        ValueError: 存在循环依赖（CIRCULAR_DEPENDENCY）时抛出
    """
    # 构建 statement_id → 索引的映射
    id_to_idx: dict[str, int] = {s.statement_id: i for i, s in enumerate(statements)}

    # 计算每个节点的入度
    in_degree: dict[str, int] = {s.statement_id: 0 for s in statements}
    # 构建依赖图：谁依赖我 → 我依赖谁的反向图
    dependents: dict[str, list[str]] = {s.statement_id: [] for s in statements}

    for stmt in statements:
        for dep_id in stmt.depends_on:
            if dep_id not in id_to_idx:
                # 缺失依赖——由 validate_program_dag 负责报告，
                # 此处仅跳过以避免后续计算异常
                continue
            in_degree[stmt.statement_id] += 1
            dependents[dep_id].append(stmt.statement_id)

    # 使用最小堆确保字典序打破平局
    heap: list[str] = []
    for sid, deg in in_degree.items():
        if deg == 0:
            heapq.heappush(heap, sid)

    result: list[str] = []

    while heap:
        # 弹出字典序最小的节点
        current = heapq.heappop(heap)
        result.append(current)

        # 移除出边——减少依赖者的入度
        for dependent in dependents.get(current, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                heapq.heappush(heap, dependent)

    # 若结果长度不等于语句总数，说明存在循环
    if len(result) != len(statements):
        # 找出未排序的节点（参与循环的节点）
        remaining = set(s.statement_id for s in statements) - set(result)
        raise ValueError(
            f"CIRCULAR_DEPENDENCY：DAG 存在循环依赖，"
            f"未排序节点：{sorted(remaining)}"
        )

    return result


# ════════════════════════════════════════════
# _temp 引用收集（辅助函数）
# ════════════════════════════════════════════


def _collect_temp_refs_from_plan(plan: SqlBuildPlan) -> set[str]:
    """从 SqlBuildPlan 的 steps 中收集所有 _temp_ 开头的表引用。

    覆盖 ScanStep.table_ref 和 JoinStep.right_table_ref。
    仅收集以 _temp_ 前缀开头的引用——CSV 表引用忽略。

    Args:
        plan: 待扫描的 SqlBuildPlan

    Returns:
        以 _temp_ 开头的表引用集合（可能为空）
    """
    temp_refs: set[str] = set()
    for step in plan.steps:
        if isinstance(step, ScanStep):
            if step.table_ref.startswith("_temp_"):
                temp_refs.add(step.table_ref)
        elif isinstance(step, JoinStep):
            if step.right_table_ref.startswith("_temp_"):
                temp_refs.add(step.right_table_ref)
    return temp_refs


def _is_reachable(
    graph: dict[str, set[str]],
    source: str,
    target: str,
) -> bool:
    """BFS 检查 source → target 在依赖图中是否存在路径。

    图的方向：graph[node] = {依赖 node 的所有节点}（反向边集）。
    从 source 出发 BFS，检查是否能到达 target。

    Args:
        graph: 反向依赖图——key 被 value 集合中的节点所依赖
        source: 起始节点（生产者）
        target: 目标节点（消费者）

    Returns:
        True 如果 source 可达 target
    """
    if source == target:
        return True
    visited: set[str] = set()
    queue: list[str] = [source]
    while queue:
        node = queue.pop(0)
        if node == target:
            return True
        if node in visited:
            continue
        visited.add(node)
        for neighbor in graph.get(node, set()):
            if neighbor not in visited:
                queue.append(neighbor)
    return False


# ════════════════════════════════════════════
# DAG 校验
# ════════════════════════════════════════════


def validate_program_dag(program: SqlProgram) -> list[OpenQuestion]:
    """校验 SqlProgram 的 DAG 合法性。

    检查项：
    1. MISSING_DEPENDENCY：depends_on 引用的 statement_id 不存在
    2. CIRCULAR_DEPENDENCY：DAG 存在循环
    3. _temp 引用合法性：consumed_by / produced_by 引用有效 statement_id
    4. _temp 命名规范：temp_id 以 _temp_ 开头
    4.5. _temp 消费者授权：实际引用 _temp 表的语句必须在 consumed_by 中声明
    4.6. _temp 消费者到生产者可达性：读取 _temp 的语句必须通过 depends_on 链可达生产者
    5. final_output 引用有效性
    6. 拓扑排序一致性：topological_order 与 Kahn 结果匹配

    Args:
        program: 待校验的 SqlProgram

    Returns:
        OpenQuestion 列表——空列表表示全部校验通过
    """
    questions: list[OpenQuestion] = []
    statement_ids = {s.statement_id for s in program.statements}

    if not program.statements:
        questions.append(
            OpenQuestion(
                question_id="prog_empty_steps",
                source="SqlProgram.Validator",
                description="SqlProgram 的 statements 为空——至少需要一个语句",
                blocking=True,
            )
        )
        return questions

    # ── 1. 缺失依赖检测 ──
    for stmt in program.statements:
        for dep_id in stmt.depends_on:
            if dep_id not in statement_ids:
                questions.append(
                    OpenQuestion(
                        question_id=f"prog_missing_dep_{stmt.statement_id}_{dep_id}",
                        source="SqlProgram.Validator",
                        description=(
                            f"MISSING_DEPENDENCY：语句 '{stmt.statement_id}' "
                            f"依赖了不存在的语句 '{dep_id}'"
                        ),
                        blocking=True,
                        # resolution 留空——需人工裁决
                    )
                )

    # ── 2. 循环依赖检测 ──
    try:
        computed_order = topological_sort(program.statements)
    except ValueError as e:
        questions.append(
            OpenQuestion(
                question_id="prog_circular",
                source="SqlProgram.Validator",
                description=str(e),
                blocking=True,
            )
        )
        computed_order = []  # 后续校验无法继续

    # ── 3. _temp 表引用校验 ──
    temp_errors = validate_temp_table_refs(program.temp_tables, statement_ids)
    for i, err in enumerate(temp_errors):
        questions.append(
            OpenQuestion(
                question_id=f"prog_temp_ref_{i}",
                source="SqlProgram.Validator",
                description=err,
                blocking=True,
            )
        )

    # ── 4. produces 字段与 TempTableSpec 一致性校验 ──
    # 收集所有声明产生的 _temp 表
    declared_temp_ids = {tt.temp_id for tt in program.temp_tables}

    for stmt in program.statements:
        if stmt.produces:
            # 检查命名规范
            try:
                validate_temp_table_naming(stmt.produces)
            except ValueError as e:
                questions.append(
                    OpenQuestion(
                        question_id=f"prog_produces_naming_{stmt.statement_id}",
                        source="SqlProgram.Validator",
                        description=str(e),
                        blocking=True,
                    )
                )

            # 检查 produces 必须在 temp_tables 中有声明
            if stmt.produces not in declared_temp_ids:
                questions.append(
                    OpenQuestion(
                        question_id=f"prog_produces_undeclared_{stmt.statement_id}",
                        source="SqlProgram.Validator",
                        description=(
                            f"语句 '{stmt.statement_id}' 声明 produces='{stmt.produces}'，"
                            f"但该 _temp 表未在 temp_tables 中声明"
                        ),
                        blocking=True,
                    )
                )

            # 检查 produces 的生产者一致性
            for tt in program.temp_tables:
                if tt.temp_id == stmt.produces and tt.produced_by != stmt.statement_id:
                    questions.append(
                        OpenQuestion(
                            question_id=f"prog_producer_mismatch_{stmt.statement_id}",
                            source="SqlProgram.Validator",
                            description=(
                                f"语句 '{stmt.statement_id}' 声明 produces='{stmt.produces}'，"
                                f"但 TempTableSpec 中 produced_by='{tt.produced_by}'"
                            ),
                            blocking=True,
                        )
                    )

    # ── 4.5 _temp 消费者授权校验：实际引用 _temp 表的语句必须在 consumed_by 中声明 ──
    for stmt in program.statements:
        temp_refs = _collect_temp_refs_from_plan(stmt.plan)
        for temp_id in temp_refs:
            if not validate_consumer_is_declared(
                program.temp_tables, stmt.statement_id, temp_id
            ):
                questions.append(
                    OpenQuestion(
                        question_id=f"prog_unauthorized_consumer_{stmt.statement_id}_{temp_id}",
                        source="SqlProgram.Validator",
                        description=(
                            f"语句 '{stmt.statement_id}' 的 SqlBuildPlan 引用了 _temp 表 "
                            f"'{temp_id}'，但该语句既不是此表的生产者，"
                            f"也未在其 TempTableSpec.consumed_by 中声明"
                        ),
                        blocking=True,
                    )
                )

    # ── 4.6 _temp 消费者到生产者的 DAG 可达性强制校验 ──
    # 构建反向依赖图：从被依赖节点 → 依赖它的节点
    reverse_graph: dict[str, set[str]] = {
        s.statement_id: set() for s in program.statements
    }
    for stmt in program.statements:
        for dep_id in stmt.depends_on:
            if dep_id in reverse_graph:
                reverse_graph[dep_id].add(stmt.statement_id)

    for stmt in program.statements:
        temp_refs = _collect_temp_refs_from_plan(stmt.plan)
        for temp_id in temp_refs:
            # 找到对应的 TempTableSpec
            for tt in program.temp_tables:
                if tt.temp_id == temp_id and tt.produced_by != stmt.statement_id:
                    # 此 statement 是消费者（非生产者自读）
                    if not _is_reachable(
                        reverse_graph, tt.produced_by, stmt.statement_id
                    ):
                        questions.append(
                            OpenQuestion(
                                question_id=(
                                    "prog_missing_producer_dep_"
                                    f"{stmt.statement_id}_{temp_id}"
                                ),
                                source="SqlProgram.Validator",
                                description=(
                                    f"语句 '{stmt.statement_id}' 读取 _temp 表 "
                                    f"'{temp_id}'，但生产者 '{tt.produced_by}' "
                                    f"在 DAG 中不可达——执行顺序无法保证 "
                                    f"producer 先于 consumer。"
                                    f"请确保 depends_on 链中存在从 "
                                    f"'{tt.produced_by}' 到 "
                                    f"'{stmt.statement_id}' 的路径"
                                ),
                                blocking=True,
                            )
                        )

    # ── 5. final_output 引用校验 ──
    if program.final_output and program.final_output not in statement_ids:
        questions.append(
            OpenQuestion(
                question_id="prog_final_output_missing",
                source="SqlProgram.Validator",
                description=(
                    f"final_output '{program.final_output}' 引用了不存在的 statement_id"
                ),
                blocking=True,
            )
        )

    # ── 6. 拓扑排序一致性校验 ──
    if computed_order and program.topological_order:
        if program.topological_order != computed_order:
            questions.append(
                OpenQuestion(
                    question_id="prog_topo_mismatch",
                    source="SqlProgram.Validator",
                    description=(
                        f"topological_order 与 Kahn 计算结果不一致："
                        f"声明={program.topological_order}，"
                        f"计算={computed_order}"
                    ),
                    blocking=True,
                )
            )

    return questions


# ════════════════════════════════════════════
# SqlProgramBuilder（确定性）
# ════════════════════════════════════════════


class SqlProgramBuilder:
    """Phase 3A 确定性 SqlProgram 构建器。

    构建策略：
    - build_single：单语句直接输出——STANDALONE 语句
    - build_chain：多跳 Join 链——PRODUCER → ... → FINAL
    - build_from_compute_steps：ComputeSteps DAG——PRODUCER/FINAL 按步骤依赖
    - build_from_statements：通用方法——接受预构建的 SqlStatement 列表

    所有构建方法最终委托至 build_from_statements。
    """

    def build_single(self, plan: SqlBuildPlan, spec_hash: str) -> SqlProgram:
        """从单个 SqlBuildPlan 构建最小 SqlProgram——单语句直接输出。

        Args:
            plan: 单个 SqlBuildPlan
            spec_hash: 对应 spec 的 hash

        Returns:
            SqlProgram——含单一 STANDALONE 语句
        """
        stmt = SqlStatement(
            statement_id=plan.plan_id,
            plan=plan,
            kind=StatementKind.STANDALONE,
            intent="单语句直接生成目标查询结果。",
        )
        return self.build_from_statements(
            statements=[stmt],
            spec_hash=spec_hash,
            final_output=plan.plan_id,
        )

    def build_chain(
        self,
        plans: list[SqlBuildPlan],
        spec_hash: str,
        chain_id: str,
    ) -> SqlProgram:
        """从线性计划链构建多语句 SqlProgram——PRODUCER → ... → FINAL。

        链中每个步骤依赖前一步骤的 _temp 表输出。
        中间步骤为 PRODUCER，最后一个为 FINAL。

        Args:
            plans: 按执行顺序排列的 SqlBuildPlan 列表
            spec_hash: 对应 spec 的 hash
            chain_id: 执行链 ID（8 字符 hex）

        Returns:
            SqlProgram——含链式依赖的 PRODUCER/.../FINAL 语句
        """
        statements: list[SqlStatement] = []
        temp_tables: list[TempTableSpec] = []

        for idx, plan in enumerate(plans):
            is_final = (idx == len(plans) - 1)

            # ── 填写通用 intent ──
            if is_final:
                intent = "生成多步骤处理链的最终结果。"
            else:
                intent = f"生成第 {idx + 1} 步中间结果，供下一步处理使用。"

            # ── 推导 DAG 依赖 ──
            depends_on: list[str] = []
            if idx > 0:
                depends_on.append(plans[idx - 1].plan_id)

            # ── 中间步骤产生 _temp 表 ──
            produces: str | None = None
            if not is_final:
                produces = make_temp_name(chain_id, str(idx))

            kind = StatementKind.FINAL if is_final else StatementKind.PRODUCER

            stmt = SqlStatement(
                statement_id=plan.plan_id,
                plan=plan,
                kind=kind,
                depends_on=depends_on,
                produces=produces,
                intent=intent,
            )
            statements.append(stmt)

        # ── 构建 TempTableSpec 列表 ──
        for idx, plan in enumerate(plans):
            if idx < len(plans) - 1:
                temp_id = make_temp_name(chain_id, str(idx))
                consumed_by = [plans[idx + 1].plan_id]
                col_defs = _extract_output_columns(plan)
                temp_tables.append(TempTableSpec(
                    temp_id=temp_id,
                    produced_by=plan.plan_id,
                    consumed_by=consumed_by,
                    column_defs=col_defs,
                ))

        final_output = plans[-1].plan_id if plans else None
        return self.build_from_statements(
            statements=statements,
            temp_tables=temp_tables,
            spec_hash=spec_hash,
            final_output=final_output,
        )

    def build_from_compute_steps(
        self,
        plans: list[SqlBuildPlan],
        spec: ParsedDeveloperSpec,
        chain_id: str,
    ) -> SqlProgram:
        """从 ComputeSteps 的 Plan 链构建多语句 SqlProgram——DAG 编排。

        每个 ComputeStep 对应一个 SqlBuildPlan。中间步骤输出 _temp 表，
        最终步骤使用 spec.output_spec。DAG 依赖从 ComputeStep.source 推导。

        Args:
            plans: 按原始声明顺序排列的 SqlBuildPlan 列表（与 spec.compute_steps 顺序一致）
            spec: 已解析的 DeveloperSpec（compute_steps 必须非空）
            chain_id: 执行链 ID（8 字符 hex）

        Returns:
            SqlProgram——含 PRODUCER/FINAL 语句，intent 和 final_output_target 已填写
        """
        steps = spec.compute_steps
        if not steps:
            raise ValueError("compute_steps 为空")

        statements: list[SqlStatement] = []
        temp_tables: list[TempTableSpec] = []

        # ── DAG 消费者分析：不被任何下游步骤消费的 step 才是 FINAL ──
        consumed: set[str] = set()
        step_names = {cs.step_name for cs in steps}
        for cs in steps:
            src_list = cs.source if isinstance(cs.source, list) else [cs.source]
            for src in src_list:
                if src != "input" and src in step_names:
                    consumed.add(src)
        for idx, (cs, plan) in enumerate(zip(steps, plans)):
            is_final = cs.step_name not in consumed

            # ── 填写 intent ──
            if is_final:
                intent = "本步骤用于生成项目书声明的最终输出结果。"
            else:
                # 查找此步骤的下游消费者
                consumers: list[str] = []
                for other_cs, _ in zip(steps, plans):
                    other_src = (
                        other_cs.source
                        if isinstance(other_cs.source, list)
                        else [other_cs.source]
                    )
                    if cs.step_name in other_src:
                        consumers.append(other_cs.step_name)
                if consumers:
                    intent = (
                        f"生成{cs.step_name}中间结果，"
                        f"供后续{', '.join(consumers)}使用。"
                        f"下游消费者：{', '.join(consumers)}"
                    )
                else:
                    intent = f"生成{cs.step_name}中间结果，供下游使用。"

            # ── 推导 DAG 依赖（从 ComputeStep.source） ──
            depends_on: list[str] = []
            src_list = (
                cs.source if isinstance(cs.source, list) else [cs.source]
            )
            for src in src_list:
                if src != "input":
                    # 找到 src 对应的 plan_id
                    for other_cs, other_plan in zip(steps, plans):
                        if other_cs.step_name == src:
                            if other_plan.plan_id not in depends_on:
                                depends_on.append(other_plan.plan_id)
                            break

            # ── 中间步骤产生 _temp 表 ──
            produces: str | None = None
            if not is_final:
                produces = make_temp_name(chain_id, cs.step_name)

            kind = StatementKind.FINAL if is_final else StatementKind.PRODUCER

            stmt = SqlStatement(
                statement_id=plan.plan_id,
                plan=plan,
                kind=kind,
                depends_on=depends_on,
                produces=produces,
                intent=intent,
            )
            statements.append(stmt)

        # ── 构建 TempTableSpec 列表 ──
        # 只对被下游消费的中间步骤创建 _temp 表；不被消费的 FINAL 步骤不需要
        for idx, (cs, plan) in enumerate(zip(steps, plans)):
            if cs.step_name in consumed:
                temp_id = make_temp_name(chain_id, cs.step_name)
                # 收集消费者
                consumed_by: list[str] = []
                for other_cs, other_plan in zip(steps, plans):
                    other_src = (
                        other_cs.source
                        if isinstance(other_cs.source, list)
                        else [other_cs.source]
                    )
                    if cs.step_name in other_src:
                        consumed_by.append(other_plan.plan_id)
                col_defs = _extract_output_columns(plan)
                temp_tables.append(TempTableSpec(
                    temp_id=temp_id,
                    produced_by=plan.plan_id,
                    consumed_by=consumed_by,
                    column_defs=col_defs,
                ))

        # ── 从 spec.output_spec 派生 final_output_target ──
        final_output_target: str | None = None
        output_spec = getattr(spec, "output_spec", None)
        if output_spec:
            table_name = getattr(output_spec, "table_name", None) or ""
            partition = getattr(output_spec, "partition_spec", None)
            if table_name:
                final_output_target = table_name
                if partition and hasattr(partition, "dt"):
                    final_output_target = f"{table_name} partition dt={partition.dt}"

        # final_output 选择
        final_stmts = [s for s in statements if s.kind == StatementKind.FINAL]
        if len(final_stmts) == 1:
            # 单一 FINAL → 直接使用
            final_output = final_stmts[0].statement_id
        elif len(final_stmts) > 1:
            # 多 sink DAG：无显式 output_step 声明，选择首个 FINAL 作为最佳推测
            # TODO: 引入 OutputSpecDecl.output_step 后改用显式声明
            logger.warning(
                "存在 %d 个 FINAL 语句，多 sink DAG 无显式 output_step；"
                "默认选择首个 FINAL(%s)。",
                len(final_stmts), final_stmts[0].statement_id,
            )
            final_output = final_stmts[0].statement_id
        else:
            final_output = statements[-1].statement_id
        return self.build_from_statements(
            statements=statements,
            temp_tables=temp_tables,
            spec_hash=spec.spec_hash,
            final_output=final_output,
            final_output_target=final_output_target,
        )

    def build_from_statements(
        self,
        statements: list[SqlStatement],
        temp_tables: list[TempTableSpec] | None = None,
        spec_hash: str = "",
        final_output: str | None = None,
        final_output_target: str | None = None,
    ) -> SqlProgram:
        """从预构建的语句列表创建 SqlProgram。

        Args:
            statements: SqlStatement 列表
            temp_tables: _temp 中间表声明
            spec_hash: 对应 spec 的 hash
            final_output: 最终输出的 statement_id
            final_output_target: FINAL 的真实输出目标（表名+分区等）

        Returns:
            SqlProgram——含计算后的 topological_order
        """
        program_id = SqlProgram.generate_program_id(spec_hash) if spec_hash else "program_test"

        # 自动生成 temp_tables——从 statements 的 produces / ScanStep._temp_ 引用推导
        if not temp_tables:
            temp_tables = _derive_temp_tables_from_statements(statements)

        # 计算拓扑排序
        try:
            order = topological_sort(statements)
        except ValueError:
            order = []  # 调用方负责校验

        return SqlProgram(
            program_id=program_id,
            spec_id=spec_hash,
            statements=statements,
            temp_tables=temp_tables or [],
            topological_order=order,
            final_output=final_output,
            final_output_target=final_output_target,
        )
