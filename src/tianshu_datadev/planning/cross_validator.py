"""交叉验证层——确定性校验 SpecEnricher 推断指标 vs RelationshipPlanner 推断 Join。

位于两个 LLM 组件之后、Builder 之前。纯确定性规则，不调用 LLM，不修改任何中间产物。

四条检查规则：
  CV1 列可达性——指标引用列是否在可达表中（通过 JOIN 或直接本表）
  CV2 Join 必要性——每个 JOIN 是否至少有一个指标使用其右表列
  CV3 跨表列歧义——同名列在多表中出现且无 JOIN 覆盖时标记歧义
  CV4 粒度一致性——target_grain 列是否在可达表中存在
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    OpenQuestion,
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.planning.relationship_hypothesis import (
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipHypothesis,
)


def _compute_reachable_tables(
    fact_table: str,
    hypothesis: RelationshipHypothesis,
    table_cols: dict[str, set[str]],
) -> set[str]:
    """从事实表出发，通过已确认 JOIN（STRONG/MEDIUM）做 BFS 计算可达表集合。

    Args:
        fact_table: 事实表的 table_alias
        hypothesis: Join 推测结果
        table_cols: table_ref → 该表所有列名的映射

    Returns:
        所有可达的 table_ref 集合（至少包含 fact_table 自身）
    """
    reachable: set[str] = {fact_table}
    # 构建邻接表——仅含 STRONG/MEDIUM 证据等级
    graph: dict[str, set[str]] = {}
    for c in hypothesis.candidates:
        if c.evidence is None:
            continue
        if c.evidence.level not in (JoinEvidenceLevel.STRONG, JoinEvidenceLevel.MEDIUM):
            continue
        # 确保左右表都在 table_cols 中——防御性检查
        if c.left_table not in table_cols or c.right_table not in table_cols:
            continue
        graph.setdefault(c.left_table, set()).add(c.right_table)
        graph.setdefault(c.right_table, set()).add(c.left_table)

    # BFS
    if fact_table in graph:
        queue = [fact_table]
        while queue:
            current = queue.pop(0)
            for neighbor in graph.get(current, set()):
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    queue.append(neighbor)

    return reachable


def cross_validate(
    spec: ParsedDeveloperSpec,
    hypothesis: RelationshipHypothesis | None,
    manifest: SourceManifest,
) -> list[OpenQuestion]:
    """交叉验证 SpecEnricher 推断指标 vs RelationshipPlanner 推断 Join。

    只读操作——不修改任何输入。产出 OpenQuestion 列表，汇入 Pipeline 人审列表。

    Args:
        spec: 已 enrich 的 DeveloperSpec（所有指标已合并到 spec.metrics）
        hypothesis: Join 推测结果（None 时跳过所有检查）
        manifest: 源数据清单

    Returns:
        OpenQuestion 列表——source="cross_validation"，空列表表示全部通过
    """
    if hypothesis is None or not hypothesis.candidates:
        return []

    # ── 构建查找结构 ──
    # 列名 → 拥有该列的表集合
    col_to_tables: dict[str, set[str]] = {}
    # table_ref → 该表所有列名集合
    table_cols: dict[str, set[str]] = {}
    for table in manifest.tables:
        cols = {col.column_name for col in table.columns}
        table_cols[table.table_ref] = cols
        for col in table.columns:
            if col.column_name not in col_to_tables:
                col_to_tables[col.column_name] = set()
            col_to_tables[col.column_name].add(table.table_ref)

    # 事实表——spec 的第一张输入表
    fact_table = spec.input_tables[0].table_alias if spec.input_tables else ""
    if not fact_table:
        return []

    # 计算可达表集合
    reachable = _compute_reachable_tables(fact_table, hypothesis, table_cols)

    questions: list[OpenQuestion] = []

    # ── 收集所有需要检查的列引用 ──
    # (列名, 使用场景描述) 用于生成清晰的 OpenQuestion
    col_refs: list[tuple[str, str]] = []

    # 从指标中收集
    for m in spec.metrics:
        if m.input_column:
            col_refs.append((m.input_column, f"指标 {m.alias} 的 input_column={m.input_column}"))

    # 从窗口指标中收集
    for wm in getattr(spec, "inferred_window_metrics", []) or []:
        if wm.input_column:
            col_refs.append((wm.input_column, f"窗口指标 {wm.alias} 的 input_column={wm.input_column}"))

    # ── CV1: 列可达性 ──
    _cv1_check(col_refs, col_to_tables, reachable, fact_table, questions)

    # ── CV2: Join 必要性 ──
    used_columns: set[str] = {col for col, _ in col_refs}
    _cv2_check(hypothesis.candidates, used_columns, table_cols, reachable, questions)

    # ── CV3: 跨表列歧义 ──
    _cv3_check(col_refs, col_to_tables, reachable, hypothesis.candidates, questions)

    # ── CV4: 粒度一致性 ──
    _cv4_check(spec, col_to_tables, reachable, fact_table, questions)

    return questions


def _cv1_check(
    col_refs: list[tuple[str, str]],
    col_to_tables: dict[str, set[str]],
    reachable: set[str],
    fact_table: str,
    questions: list[OpenQuestion],
) -> None:
    """CV1: 列可达性——每个指标引用的列必须存在于至少一个可达表中。

    如果列仅存在于不可达表 → 生成 OpenQuestion(non-blocking)。

    Args:
        col_refs: (列名, 描述) 列表
        col_to_tables: 列名 → 拥有该列的表集合
        reachable: 可达表集合
        fact_table: 事实表别名
        questions: 累积 OpenQuestion 的列表
    """
    for col_name, context_desc in col_refs:
        tables_with_col = col_to_tables.get(col_name, set())
        if not tables_with_col:
            # 列不在任何 manifest 表中 → 由 Validator 负责，此处不重复检查
            continue

        # 如果至少有一个拥有该列的表是可达的 → OK
        if tables_with_col & reachable:
            continue

        # 列仅存在于不可达表中
        unreachable = tables_with_col - reachable
        reachable_str = ", ".join(sorted(reachable))
        unreachable_str = ", ".join(sorted(unreachable))
        questions.append(OpenQuestion(
            question_id=f"Q-XV-CV1-{col_name}",
            source="cross_validation",
            field_ref=col_name,
            description=(
                f"[CV1 列可达性] {context_desc}——"
                f"列 {col_name} 仅存在于不可达表 [{unreachable_str}]，"
                f"当前可达表为 [{reachable_str}]。"
                f"需确认 Join 关系以访问该列。"
            ),
            blocking=False,
        ))


def _cv2_check(
    candidates: list[JoinCandidate],
    used_columns: set[str],
    table_cols: dict[str, set[str]],
    reachable: set[str],
    questions: list[OpenQuestion],
) -> None:
    """CV2: Join 必要性——每个 JOIN 候选是否至少有一个指标使用其右表中的列。

    如果 JOIN 两侧表的列均未被任何指标引用 → 可能冗余。

    注意：仅检查 LLM 推断的 JOIN（evidence 不含 developer_declared 检查项），
    程序员显式声明的 JOIN 不标记为冗余。

    Args:
        candidates: Join 候选列表
        used_columns: 所有指标引用的列名集合
        table_cols: table_ref → 列名集合
        reachable: 可达表集合
        questions: 累积 OpenQuestion 的列表
    """
    for c in candidates:
        # 跳过程序员显式声明的 JOIN——它们不需要"必要性"证明
        if c.evidence and c.evidence.evidence_checks:
            if any("developer_declared: FOUND" in check for check in c.evidence.evidence_checks):
                continue

        # 获取右表中被使用的列
        right_cols = table_cols.get(c.right_table, set())
        left_cols = table_cols.get(c.left_table, set())

        # 右表独有列（不在左表中）——这些是需要 JOIN 才能访问的列
        right_only_cols = right_cols - left_cols

        # 如果右表独有列中没有任何一个被指标使用 → 可能冗余
        if not (right_only_cols & used_columns):
            questions.append(OpenQuestion(
                question_id=f"Q-XV-CV2-{c.candidate_id}",
                source="cross_validation",
                field_ref=f"{c.left_table}.{c.left_key}={c.right_table}.{c.right_key}",
                description=(
                    f"[CV2 Join 必要性] Join {c.left_table}.{c.left_key} = "
                    f"{c.right_table}.{c.right_key}——"
                    f"右表 {c.right_table} 中没有列被当前指标引用，"
                    f"该 Join 可能不必要，请确认是否需要保留。"
                ),
                blocking=False,
            ))


def _cv3_check(
    col_refs: list[tuple[str, str]],
    col_to_tables: dict[str, set[str]],
    reachable: set[str],
    candidates: list[JoinCandidate],
    questions: list[OpenQuestion],
) -> None:
    """CV3: 跨表列歧义——同名列出在多表中但无 JOIN 覆盖时标记歧义。

    例如：tf 和 td 都有 status 列，某指标引用了 status，
    但 tf↔td 的 JOIN 未被确认 → 无法确定 status 属于哪张表。

    Args:
        col_refs: (列名, 描述) 列表
        col_to_tables: 列名 → 拥有该列的表集合
        reachable: 可达表集合
        candidates: Join 候选列表
        questions: 累积 OpenQuestion 的列表
    """
    # 构建已确认 JOIN 的表对集合
    confirmed_pairs: set[tuple[str, str]] = set()
    for c in candidates:
        if c.evidence and c.evidence.level in (JoinEvidenceLevel.STRONG, JoinEvidenceLevel.MEDIUM):
            confirmed_pairs.add((c.left_table, c.right_table))
            confirmed_pairs.add((c.right_table, c.left_table))

    already_reported: set[str] = set()  # 去重——同一列只报告一次

    for col_name, context_desc in col_refs:
        if col_name in already_reported:
            continue
        tables_with_col = col_to_tables.get(col_name, set())
        if len(tables_with_col) <= 1:
            continue  # 列只在一张表中 → 无歧义

        # 列在多个表中 → 检查这些表对中是否有至少一对被 JOIN 覆盖
        # 简化：检查是否所有拥有该列的可达表对之间都有 JOIN
        reachable_with_col = tables_with_col & reachable
        if len(reachable_with_col) <= 1:
            continue  # 只有一个可达表有该列 → 无歧义

        # 检查可达表之间是否有 JOIN 覆盖
        reachable_list = sorted(reachable_with_col)
        has_ambiguity = False
        for i in range(len(reachable_list)):
            for j in range(i + 1, len(reachable_list)):
                if (reachable_list[i], reachable_list[j]) not in confirmed_pairs:
                    has_ambiguity = True
                    break
            if has_ambiguity:
                break

        if has_ambiguity:
            already_reported.add(col_name)
            questions.append(OpenQuestion(
                question_id=f"Q-XV-CV3-{col_name}",
                source="cross_validation",
                field_ref=col_name,
                description=(
                    f"[CV3 跨表列歧义] {context_desc}——"
                    f"列 {col_name} 存在于多个可达表 [{', '.join(reachable_list)}]，"
                    f"但这些表之间缺少已确认的 Join 关系，"
                    f"无法确定该列属于哪张表，需人工指定。"
                ),
                blocking=True,
            ))


def _cv4_check(
    spec: ParsedDeveloperSpec,
    col_to_tables: dict[str, set[str]],
    reachable: set[str],
    fact_table: str,
    questions: list[OpenQuestion],
) -> None:
    """CV4: 粒度一致性——target_grain 中的所有列必须在可达表中存在。

    grain 是 GROUP BY 的键——如果 grain 列不可达，整个查询的维度就错了。

    Args:
        spec: 已 enrich 的 DeveloperSpec
        col_to_tables: 列名 → 拥有该列的表集合
        reachable: 可达表集合
        fact_table: 事实表别名
        questions: 累积 OpenQuestion 的列表
    """
    grain = spec.output_spec.grain
    if not grain:
        return

    for grain_col in grain:
        tables_with_col = col_to_tables.get(grain_col, set())
        if not tables_with_col:
            # grain 列不在任何 manifest 表中 → 由 Validator 负责
            continue

        if tables_with_col & reachable:
            continue  # 至少有一个可达表包含该 grain 列

        questions.append(OpenQuestion(
            question_id=f"Q-XV-CV4-{grain_col}",
            source="cross_validation",
            field_ref=grain_col,
            description=(
                f"[CV4 粒度一致性] target_grain 列 {grain_col} "
                f"仅存在于不可达表 [{', '.join(sorted(tables_with_col))}]，"
                f"当前可达表为 [{', '.join(sorted(reachable))}]。"
                f"粒度列不可达将导致 GROUP BY 无法执行——"
                f"需确认 Join 关系或修改 grain 定义。"
            ),
            blocking=True,
        ))
