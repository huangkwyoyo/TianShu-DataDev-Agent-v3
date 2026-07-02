"""review.md 生成器——生成人类可读的代码审查文档。

面向不熟悉系统内部实现的数据工程师。
内容不含 SqlBuildPlan 内部结构、Compiler Pass 细节或 LLM 实现细节。
"""

from __future__ import annotations

from .models import HumanReviewItem, PackageInputs


def generate_review_md(
    inputs: PackageInputs,
    review_items: list[HumanReviewItem] | None = None,
) -> str:
    """生成人类可读的 review.md。

    Args:
        inputs: 组装 Code Review Package 所需的全部输入
        review_items: 人工审查清单项（由调用方根据 OpenQuestions 和 PerfResults 构建）

    Returns:
        review.md 的 Markdown 内容
    """
    review_items = review_items or []

    # 从序列化 dict 中提取关键信息
    spec = inputs.parsed_spec
    sql_artifact = inputs.sql_artifact
    contract_dict = inputs.data_transform_contract
    trace = inputs.execution_trace

    # ── 标题 ──
    spec_title = spec.get("title", "未命名项目")
    spec_hash = spec.get("spec_hash", "")
    spec_desc = spec.get("description", "")

    # ── 输入表 ──
    input_tables = spec.get("input_tables", [])

    # ── Join 关系 ──
    join_rels = contract_dict.get("join_relationships", [])

    # ── SQL ──
    compiled_sql = ""
    if sql_artifact and "compiled_sql" in sql_artifact:
        compiled_sql = sql_artifact["compiled_sql"].get("sql", "")

    # ── 执行摘要 ──
    exec_status = "未执行"
    exec_row_count = 0
    exec_time_ms = 0.0
    if trace:
        exec_status = trace.get("status", "未执行")
        exec_row_count = trace.get("row_count", 0)
        exec_time_ms = trace.get("execution_time_ms", 0.0)

    # ── 构建 Markdown ──
    lines: list[str] = []

    # 1. 项目目标
    lines.append(f"# SQL Code Review — `{inputs.request_id}`")
    lines.append("")
    lines.append("> 本文档面向数据工程师进行代码审查。" + \
                 "不熟悉系统内部实现者应可独立理解本文档内容。")
    lines.append("")
    lines.append("## 1. 项目目标")
    lines.append("")
    lines.append(f"**项目名称**：{spec_title}")
    lines.append("")
    if spec_desc:
        lines.append(f"**需求描述**：{spec_desc[:500]}")
        lines.append("")
    lines.append(f"**DeveloperSpec hash**：`{spec_hash}`")
    lines.append(f"**返工轮次**：{inputs.retry_count}")
    lines.append("")

    # 2. 数据结构化理解
    lines.append("## 2. 数据结构化理解")
    lines.append("")
    lines.append("### 2.1 输入表")
    lines.append("")
    lines.append("| 表别名 | 物理表 | 估算行数 |")
    lines.append("|--------|--------|----------|")
    for t in input_tables:
        alias = t.get("table_alias", "")
        source = t.get("source_table", "")
        rows = t.get("row_count", "")
        row_str = f"{rows:,}" if isinstance(rows, int) else str(rows) if rows else "未知"
        lines.append(f"| {alias} | {source} | {row_str} |")
    lines.append("")

    # 字段清单
    lines.append("### 2.2 字段清单")
    lines.append("")
    for t in input_tables:
        alias = t.get("table_alias", "")
        lines.append(f"**{alias}**：")
        columns = t.get("columns", [])
        key_cols = t.get("key_columns", [])
        biz_cols = t.get("business_columns", [])
        all_cols = list(columns) + list(key_cols) + list(biz_cols)

        if all_cols:
            for c in all_cols:
                col_name = c.get("column_name", "")
                col_type = c.get("data_type", "") or "未声明"
                nullable = c.get("nullable", "")
                nullable_str = "可空" if nullable else ("非空" if nullable is False else "未知")
                lines.append(f"  - `{col_name}` (类型: {col_type}, {nullable_str})")
        else:
            lines.append("  - (未声明具体字段)")
        lines.append("")

    # 过滤条件（从 Contract 结构化字段渲染人类可读表达式）
    lines.append("### 2.3 过滤条件")
    lines.append("")
    filters = contract_dict.get("filters", [])
    if filters:
        for f in filters:
            display = _render_predicate_display(f)
            lines.append(f"- `{display}`")
    else:
        lines.append("(无显式过滤条件)")
    lines.append("")

    # 3. Join 证据链
    lines.append("## 3. Join 证据链")
    lines.append("")
    if join_rels:
        for jr in join_rels:
            jid = jr.get("join_id", "")
            lt = jr.get("left_table", "")
            rt = jr.get("right_table", "")
            lk = jr.get("left_key", "")
            rk = jr.get("right_key", "")
            jt = jr.get("join_type", "INNER")
            level = jr.get("level", "未知")
            evidence = jr.get("evidence_chain", {})

            lines.append(f"### Join: `{lt}` ↔ `{rt}`")
            lines.append("")
            lines.append(f"- **Join ID**：`{jid}`")
            lines.append(f"- **类型**：{jt}")
            lines.append(f"- **关联键**：`{lt}.{lk}` = `{rt}.{rk}`")
            lines.append(f"- **证据等级**：{level}")
            lines.append("")

            if evidence:
                checks = evidence.get("evidence_checks", [])
                detail = evidence.get("detail", "")
                if checks:
                    lines.append("**证据检查**：")
                    for chk in checks:
                        lines.append(f"  - {chk}")
                    lines.append("")
                if detail:
                    lines.append(f"**评级理由**：{detail}")
                    lines.append("")
    else:
        lines.append("本项目为单表查询，无 Join 关系。")
        lines.append("")

    # 3.5 处理步骤说明（从 SqlProgram.statements[].intent 读取）
    sql_program = inputs.sql_program
    if sql_program:
        statements = sql_program.get("statements", [])
        if len(statements) > 1:
            lines.append("## 3.5 处理步骤说明")
            lines.append("")
            for stmt in statements:
                sid = stmt.get("statement_id", "")
                intent = stmt.get("intent", "")
                kind = stmt.get("kind", "")
                produces = stmt.get("produces", "")
                if intent:
                    kind_label = {
                        "PRODUCER": "中间步骤",
                        "CONSUMER": "中间步骤",
                        "FINAL": "最终输出",
                        "STANDALONE": "单步查询",
                    }.get(kind, kind)
                    produce_info = f" → `{produces}`" if produces else ""
                    lines.append(
                        f"- **[{kind_label}]** `{sid}`{produce_info}：{intent}"
                    )
            lines.append("")

    # 4. SQL
    lines.append("## 4. SQL（编译产物）")
    lines.append("")
    if compiled_sql:
        lines.append("```sql")
        lines.append(compiled_sql)
        lines.append("```")
    else:
        lines.append("(SQL 编译产物缺失)")
    lines.append("")

    # 5. 执行摘要
    lines.append("## 5. 执行摘要")
    lines.append("")
    lines.append(f"- **执行状态**：{exec_status}")
    lines.append(f"- **返回行数**：{exec_row_count:,}")
    lines.append(f"- **执行耗时**：{exec_time_ms:.1f} ms")
    lines.append("")

    if inputs.result_summary:
        col_names = inputs.result_summary.get("columns", [])
        if col_names:
            lines.append(f"**输出列**：{', '.join(str(c) for c in col_names)}")
            lines.append("")

    # 6. 人工审查清单
    lines.append("## 6. 人工审查清单")
    lines.append("")
    if review_items:
        for item in review_items:
            severity_mark = {
                "blocking": "🔴 阻断",
                "warning": "🟡 警告",
                "info": "🔵 信息",
            }.get(item.severity, "⚪")

            artifact_ref = f" → `{item.related_artifact}`" if item.related_artifact else ""
            lines.append(
                f"- [{severity_mark}] **{item.category}**：" +
                f"{item.description}{artifact_ref}"
            )
    else:
        lines.append("(无待审查项)")
    lines.append("")

    # 7. 开放问题
    lines.append("## 7. 开放问题")
    lines.append("")
    open_questions = inputs.open_questions + inputs.validation_questions
    if open_questions:
        for q in open_questions:
            blocking = "🔴 阻断" if q.get("blocking", False) else "🟡 非阻断"
            source = q.get("source", "未知")
            desc = q.get("description", "")
            resolution = q.get("resolution", {})
            lines.append(f"- [{blocking}] [{source}] {desc}")
            if resolution:
                answer = resolution.get("answer", "")
                if answer:
                    lines.append(f"  - 已裁决：{answer}")
        lines.append("")
    else:
        lines.append("(无开放问题)")
        lines.append("")

    # 8. 性能建议
    lines.append("## 8. 性能建议")
    lines.append("")
    perf_results = inputs.perf_results
    perf_warns = [r for r in perf_results if not r.get("passed", True)]
    if perf_warns:
        for r in perf_warns:
            lines.append(f"- **{r.get('rule_id', '')}**：{r.get('message', '')}")
    else:
        lines.append("无性能警告。")
    lines.append("")

    # ── 页脚 ──
    lines.append("---")
    lines.append("")
    lines.append("*Generated by TianShu DataDev Agent v3 Phase 2 — " +
                 f"retry_count={inputs.retry_count}*")
    lines.append("")

    return "\n".join(lines)


def _render_predicate_display(pred: dict) -> str:
    """从 ContractPredicate 的结构化字段渲染人类可读的谓词表达式。

    此函数仅用于 review.md 等显示层 artifact，
    不得被 Phase 5 Spark 编译器直接消费——Spark 应使用 left/operator/right 结构化三元组。

    Args:
        pred: ContractPredicate 序列化 dict，含 left/operator/right 字段

    Returns:
        人类可读的表达式字符串，如 "tf.amount > 0" 或 "td.status = 'active'"
    """
    left = pred.get("left", "")
    op = pred.get("operator", "")
    right = pred.get("right", "")
    if right:
        return f"{left} {op} {right}"
    return f"{left} {op}"
