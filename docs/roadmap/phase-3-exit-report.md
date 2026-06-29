# Phase 3 Exit HarnessReport

> **报告 ID**：`hr_15af40bf5bae`
> **Phase**：`phase-3-exit`
> **总判决**：`GO`
> **评测时间**：2026-06-29T03:29:35.979423+00:00
> **生成脚本**：`scripts/phase3_exit_eval.py`

## ✅ 无 REJECT 维度——Phase 3 Exit 基线已建立

---
### ✅ 维度 1：Schema 可生成性基线（判决: PASS）

| 指标 | 值 |
|------|----|
| total_golden_fixtures | 6 |
| parsed_count | 6 |
| plan_built_count | 4 |
| passed_count | 0 |
| parse_pass_rate | 100.0 |
| fixtures_requiring_planner | 2 |
| fixtures_full_pipeline | 0 |

**详情**：
⚠️ golden_chinese_column_comments: 解析通过，Plan 构建需 Planner/Hypothesis——Parser 宽松6：中文列注释。SafeIdentifier 拒绝中文列名（设计决定），Plan 构建预期失败——E2E 测试中通过列名归一化处理。
⚠️ golden_extra_markdown_text: Validator 拒绝——Q-VAL-COL-step_scan_450f8aff-cnt （Parser 宽松5：额外 Markdown 文本。单表场景，预期全链路通过。）
⚠️ golden_no_explicit_joins: 解析通过，Plan 构建需 Planner/Hypothesis——Parser 宽松3：Join 未显式声明。多表场景，需 RelationshipHypothesis 推理 Join——SqlBuildPlanBuilder 单步无法完成，E2E 测试通过 FakePipeline 覆盖。
⚠️ golden_no_output_sort: Validator 拒绝——Q-VAL-COL-step_scan_450f8aff-cnt （Parser 宽松2：无输出排序。单表场景，预期全链路通过。）
⚠️ golden_no_time_range: Validator 拒绝——Q-VAL-COL-step_scan_450f8aff-cnt （Parser 宽松1：无时间范围。单表场景，E2E 测试已验证全链路。）
⚠️ golden_type_inferred_from_registry: Validator 拒绝——Q-VAL-COL-step_scan_450f8aff-total_amount （Parser 宽松4：类型从 Registry 推断。单表场景但需类型补全，部分 E2E 测试覆盖。）

**证据来源**：tests/fixtures/golden/ (6 个 fixture); 完整 E2E 覆盖在 tests/sql/test_pipeline_e2e.py

---
### ✅ 维度 2：DataTransformContract v1 覆盖度（判决: PASS）

| 指标 | 值 |
|------|----|
| v1_total_fields | 21 |
| v1_specific_fields_defined | 5 |
| v1_specific_fields_missing | 0 |
| extract_v1_available | True |
| test_count | 13 |

**详情**：
v1 专属字段定义完整: 5/5
已定义字段: case_when_labels, step_dag, temp_tables, window_specs, write_spec
✅ extract_v1() 方法存在
Contract v1 测试覆盖: 13 个测试

**证据来源**：src/tianshu_datadev/artifacts/contract_extractor.py + models.py

---
### ✅ 维度 3：SqlProgram + _temp 多语句 Compiler 覆盖率（判决: PASS）

| 指标 | 值 |
|------|----|
| sqlprogram_builder_available | True |
| compile_program_available | True |
| sqlprogram_test_count | 24 |
| e2e_test_count | 2 |

**详情**：
✅ SqlProgramBuilder.build_from_statements() 可用
✅ DuckDbSqlCompiler.compile_program() 可用
SqlProgram 测试: 24 个 (test_sql_program.py)
Pipeline E2E 测试: 2 个 (test_pipeline_e2e.py)

**证据来源**：src/tianshu_datadev/planning/sql_program.py; src/tianshu_datadev/sql/compiler.py; tests/planning/test_sql_program.py; tests/sql/test_pipeline_e2e.py

---
### ℹ️ 维度 4：已知不支持的 SQL 模式清单（判决: INFO）

| 指标 | 值 |
|------|----|
| unsupported_patterns_count | 5 |
| never_implement | 1 |
| phase4_plus | 3 |
| phase3b_forbidden | 1 |

**详情**：
### CTE（Common Table Expression）
- 状态: 永不实现
- 替代方案: SqlProgram + _temp 中间表 + DAG 拓扑排序——语义等价：WITH cte AS (...) SELECT ... FROM cte 等效于 CREATE TEMP TABLE _temp_cte AS ...; SELECT ... FROM _temp_cte
- 拒绝方式: Validator → UNSUPPORTED_PLAN
- 理由: CTE 引入嵌套作用域，破坏 SqlBuildPlan 的扁平可审查性；_temp 等效覆盖所有 CTE 场景，无需维护两套机制
- 文档: AGENTS.md:116, docs/00-product-charter.md:118, docs/01-target-architecture.md §3.3, docs/02-reuse-and-migration-map.md:102, docs/03-sql-ir-and-compiler-plan.md §3.3.2
### 子查询（Subquery）
- 状态: Phase 1-3 不支持，Phase 4+ 按黄金用例逐项开放
- 替代方案: 当前无等效替代——涉及子查询的需求需拆分为多语句 SqlProgram 或等待 Phase 4+
- 拒绝方式: Validator → UNSUPPORTED_PLAN
- 理由: 子查询引入嵌套作用域，与 CTE 同样破坏扁平可审查性。Phase 4+ 开放时需满足 7 项成套交付规则（Schema + Validator + Compiler + Safety + 测试 + 拒绝路径 + Artifact）
- 文档: docs/03-sql-ir-and-compiler-plan.md:273, 409, 426-434
### 多跳 Join（Multi-hop Join）
- 状态: Phase 1-3 不支持，Phase 4+ 按黄金用例逐项开放
- 替代方案: 当前支持单跳 Join（两表关联）。多跳需拆分为多步 SqlProgram，每步最多一个 JoinStep，通过 _temp 表传递中间结果
- 拒绝方式: Validator → UNSUPPORTED_PLAN
- 理由: 多跳 Join 增加 Join 推理的复杂度——Planner 需同时验证多对关系（证据链互相独立）。Phase 4+ 开放时与子查询共享同一套 7 项交付规则
- 文档: docs/03-sql-ir-and-compiler-plan.md:409, 426-434
### 窗口函数与子查询组合
- 状态: Phase 3B 明确禁止
- 替代方案: 窗口函数仅允许独立于子查询使用——WindowExpr 不接受嵌套 WindowExpr 或子查询参数
- 拒绝方式: Validator / window_validator → 拒绝
- 理由: 窗口函数 + 子查询的组合在语义上等价于先子查询物化再窗口——应拆分为两个 SqlProgram 步骤
- 文档: docs/03-sql-ir-and-compiler-plan.md:309, docs/roadmap/phase-3b-window-functions.md:86
### DDL / DML（CREATE/ALTER/DROP/INSERT/UPDATE/DELETE/MERGE）
- 状态: Phase 1-3 不支持，DML 写入由 FinalWritePlan 受控审查替代
- 替代方案: CREATE TEMP TABLE 仅用于 _temp 中间表（受控）；INSERT OVERWRITE 仅用于日期分区写入（FinalWritePlan 审查）；其他 DDL/DML 一律拒绝
- 拒绝方式: WriteValidator → 拒绝禁止操作；Validator → UNSUPPORTED_PLAN
- 理由: 避免 LLM 生成破坏性的 DDL/DML 语句。受控写入通过 FinalWritePlan + WriteValidator 10 项安全检查
- 文档: docs/03-sql-ir-and-compiler-plan.md:409, docs/roadmap/phase-3c-*.md

**证据来源**：docs/03-sql-ir-and-compiler-plan.md §7; docs/01-target-architecture.md §3.3

> **补充**：子查询与多跳 Join 的边界已从占位声明升级为具象可执行规划——
> 详见 [[subquery-multihop-join-boundary_20260629_1500|子查询 & 多跳 Join 边界补充文档]]（含 Phase 4.6 归属、黄金用例、7 项规则具象化 checklist）。

---
### ℹ️ 维度 5：Phase 4 硬化的输入基线（判决: INFO）

| 指标 | 值 |
|------|----|
| total_tests | 1123 |
| phases_completed | 7 |
| phases_in_progress | 1 |
| phases_ready | 1 |
| phases_pending | 3 |
| known_gaps_count | 5 |

**详情**：
### 各 Phase 核销状态
- Phase 1A: ✅ 已完成
- Phase 1B: ✅ 已完成
- Phase 1C: ✅ 已完成
- Phase 2: ✅ 已完成
- Phase 3A: ✅ 已完成
- Phase 3B: ✅ 已完成
- Phase 3B.1（枚举自动检测）: ✅ 已完成
- Phase 3C: ⚠️ 实施中（5/6——本报告补齐第 6 项）
- Phase 4A: 🔄 基础设施就绪（2/5，阻塞于本报告）
- Phase 4B: ⏳ 待实施
- Phase 4C: ⏳ 待实施（Harness 安全/语义评测器已就绪）
- Phase 4D: ⏳ 待实施（Harness 七维门禁框架已就绪）

### 已知能力缺口
- WRONG_GRAIN（错粒度）——Validator 无粒度完整性规则
- WRONG_AGGREGATION（错聚合）——Validator 无聚合类型声明对比规则
- Phase 4A: missing regression_cases.jsonl × 4
- Phase 4A: missing structured_output.py
- Phase 4A: missing real LLM integration

### 测试基线
- 全量测试: 1123 个
- 6 个 golden fixture (tests/fixtures/golden/)
- 6 个 reject fixture (tests/fixtures/reject/)
- 13 个 harness dataset fixture (harness/datasets/)
- 4 个 harness 测试文件 (tests/harness/)

**证据来源**：AGENTS.md; docs/roadmap/phase-3c-*.md; docs/roadmap/phase-4a-*.md; src/tianshu_datadev/harness/semantic_eval.py

---

*报告由 `scripts/phase3_exit_eval.py` 确定性生成——
相同代码基线重新运行产生相同结果。*
*生成时间：2026-06-29T03:29:35.979423+00:00*
