# 9A2 Progress Ledger
Started: 2026-07-05T11:01:53+08:00
## 9A2 Progress
Task 1: complete (mark contract_to_sql_steps deprecated)
Task 2: complete (test_comparator_with_real_sql_pipeline_plan)
Task 3: complete (test_d4_with_real_sql_pipeline_plan)
Task 4: complete (risk doc update)
Final: 610 passed, 11 skipped, ruff clean

## 9A3 Progress
Started: 2026-07-05
Task 1: complete (adapt_lite_to_v1 + 3 tests)
Task 2: complete (HarnessRunner auto-drive upgrade)
Task 3: complete (test updates + Lite/V1收口)
Task 4: complete (docs sync + full verification)
Final: 617 passed, 11 skipped, ruff clean
## 9A5 Progress
Started: 2026-07-05
Task 1: complete (SparkReviewPackage + SparkReviewBuilder enhancement)
Task 2: complete (REVIEW_READY e2e integration tests)
Task 3: complete (documentation updates)
Final: 629 passed, 11 skipped, ruff clean

## Spark Frontend Integration Progress
Started: 2026-07-05
Base commit: 10d51ca
Task 1: complete (SparkVerifyRequest/Response/StageItem models)
Task 2: complete (POST /api/spark/verify endpoint)
Task 3: complete (4 endpoint tests)
Task 4: complete (SparkVerifyResponse type + sparkVerify() client)
Task 5: complete (PipelineStageIndicator title prop + Spark CN mapping)
Task 6: complete (App.tsx Spark verify button + 2nd indicator)
Final: 582 passed, 11 skipped (api + spark subset), ruff clean, tsc zero errors, build success
A-class fix: .gitignore frontend/dist/ (git diff --check now clean)

## Phase 9B Progress
Started: 2026-07-05
Base commit: 0610012
Task 1: complete (commits 0610012..b397274, review clean)
  Minor: test_spark_verify_catch_sets_error_for_display L143 死代码——第二个条件永远为 false（空格匹配）
Task 2: complete (commits b397274..49e4120, review clean)
  Minor: clearError 中移除 pipelineStages: [] 未在需求中记录（合理配套变更）
Task 3: complete (验收 + 文档更新)
Final: 582 passed / 11 skipped (api/spark 子集), frontend smoke 23 passed all, ruff/tsc/build/git diff clean
R11: 已消除（前端源码级回归测试覆盖按钮/指示灯/错误路径）
R15: 已消除（SQL 指示灯成功态 green dot + 8 阶段全部 ✅）

## Phase 9B-P0 Progress
Started: 2026-07-05
Base commit: 27ad156
Task 1: complete (commits 27ad156..3685226, review clean)
Task 2: complete (commits 3685226..da71d42, review clean)
  Minor: ComputeSteps + 公共路径 Snapshot 代码块重复（B2 已知取舍）
Task 3: complete (commits da71d42..20f876a, review clean)
Task 4: complete (commits 20f876a..70c5dfc..48c5a41, fix round for tmpdir/import/hardcoded-path)
Task 5: complete (全量回归 + 文档更新)
Final: 587 passed / 11 skipped, frontend smoke 23 passed, ruff/tsc/build/git diff clean
R10: 已消除（Snapshot Builder 已集成到 Pipeline.run_all()，可选注入+build+provenance hash 全链路覆盖）

## Phase 9B-P1 Progress
Started: 2026-07-05
Base commit: 7d34731
Task 1: complete (commits 7d34731..e250d3c)
  provenance.yml snapshot_manifest_hash 显式断言——验收标准 #3 直接覆盖
Final: 588 passed / 11 skipped (api/spark 全量), frontend smoke 23 passed, ruff/tsc/build/git diff clean

## Phase 9C Progress
Started: 2026-07-05
Base commit: e250d3c
Task 1: complete (commit 21cccd4)
  - 安装 @playwright/test + Chromium 浏览器
  - playwright.config.ts: webServer 启动 FastAPI + Vite
  - fixtures/developer-specs.ts: 模板名称 + 手工 Spec（两类分离）
  - helpers/api.ts: waitForBackend + waitForExecutionComplete
Final: tsc zero errors, playwright config validated

## Phase 9C Progress
Started: 2026-07-05
Base commit: e250d3c
Task 1: complete (commits e250d3c..21cccd4, review clean)
Task 2: complete (commits 21cccd4..fadad6b, review clean)
Task 3: complete (commits fadad6b..bf7d199, review clean)
Task 4: complete (commits bf7d199..757d08c..5d45b24, fix round for waitForExecutionComplete dot-ok/dot-error distinction)
  Minor: table_paths 环境缺失——spec-editor 成功路径 + spark-verify 成功路径标记为 test.skip()
  Minor: waitForExecutionComplete 早期返回边缘情况
Task 5: complete (全量回归验收 + 文档更新)
  Final: 588 passed / 11 skipped (api/spark 全量), frontend smoke 23 passed, Playwright E2E 4 passed / 2 skipped (table_paths 跳过), ruff/tsc/build/git diff clean
  R11: 已消除（Playwright E2E 补充——源码级 + E2E 双重覆盖）
  R16: 新增残留风险——table_paths 环境缺失
Task 5: complete (commits 5d45b24..3bb52b2, review clean)
Final: 588 passed / 11 skipped, frontend smoke 23 passed, E2E 4 passed / 2 skipped, ruff/tsc/build/git diff clean
R11: 已消除（源码级 + Playwright E2E 双重覆盖）
R16: 新增——table_paths 环境缺失，2 个 E2E 测试标记 test.skip()

## Phase 9C-R16 Progress
Started: 2026-07-05
Base commit: 3bb52b2
Task: complete (R16 消除——table_paths 自动发现 + PipelineStageIndicator dot-ok 修复)
  - Pipeline.default_table_paths 回退参数 + 7 处方法适配
  - create_app() CSV fixture 自动发现
  - playwright.config.ts 修正模块路径（main→app factory）
  - PipelineStageIndicator: skipped 阶段视为非失败完成态
  - spec-editor + spark-verify 两个 test.skip() 移除
  - Backend: 588 passed / 11 skipped (zero regression)
  - E2E: 6 passed / 0 skipped
  - Frontend smoke: 23 passed
  - ruff/tsc/build/git diff: clean

## Phase 9C-R16b Progress (边界硬化)
Started: 2026-07-05
Base commit: 676b812
Task: complete (table_paths 边界硬化——None/{} 语义区分 + E2E 模式开关)
  - Pipeline._resolve_table_paths() 辅助方法：None 回退到默认值，{} 保持空字典
  - 7 处 DuckDBExecutor 构造统一改为 self._resolve_table_paths(table_paths)
  - create_app() 仅在 TIANSHU_E2E_MODE=true 时调用 _discover_csv_fixtures()
  - playwright.config.ts: Python 一行命令在进程内设置环境变量
  - 新增 tests/api/test_pipeline_table_paths_boundary.py（6 个边界测试）
  - Backend: 594 passed / 11 skipped (+6 新边界测试，零退化)
  - E2E: 6 passed / 0 skipped
  - ruff/tsc/build/git diff: clean

## Phase 9C Final Review
Final review: CLEAN (0 Critical, 0 Important, 0 Minor)
Non-blocking observations:
- frontend/test-results/ 建议加入 .gitignore
- error-display.spec.ts 测试名略有歧义（实际测试 PipelineStageIndicator 而非 ErrorDisplay）

## Phase 9B-P1 补全 Progress (provenance.yml 显式断言)
Started: 2026-07-05
Base commit: de562d9
Task: complete (snapshot_manifest_hash 显式断言——补全测试覆盖矩阵)
  - tests/artifacts/test_provenance.py:
    - required_fields 列表追加 snapshot_manifest_hash
    - 新增 test_snapshot_manifest_hash_empty_when_none（无快照→空字符串）
    - 新增 test_snapshot_manifest_hash_deterministic（同输入→同 hash）
  - tests/spark/test_snapshot.py:
    - 加固 test_provenance_yml_contains_snapshot_manifest_hash（补上 compute_json_hash 正确性断言）
    - 新增 test_snapshot_manifest_hash_empty_when_no_snapshot_integration（生产路径无快照→空 hash）
  - 测试基线: 601 passed / 11 skipped (+3 新测试，零退化)
  - ruff/tsc/build/git diff: clean

## Phase 9A4-NYC Progress (真实业务样本——NYC 案例 01)
Started: 2026-07-05
Base commit: b6bb5f2
Task: complete (NYC 按行程来源每日聚合——SQL 全链路 + Spark 双链验证)
  - 数据: nyc_transport.duckdb gold.fact_trips 分层抽样 → tests/fixtures/nyc/fact_trips_sample.csv (2549 rows)
  - DeveloperSpec: tests/fixtures/nyc/nyc_trip_source_daily.md (5 metrics × 2 dims × time_range)
  - SQL 管线: Parser → Builder → Validator → Compiler → DuckDB Executor 全链路通过 (265 output rows)
  - 结果一致性: Pipeline 聚合值与直接 DuckDB 查询完全一致 (trip_count=2549, total_revenue=10762.87)
  - Spark 双链: Contract → adapt_lite_to_v1 → Orchestrator → PlanComparator 逻辑等价
  - 测试: tests/api/test_nyc_business_case.py (11 个测试——7 SQL + 3 Contract + 1 Spark)
  - 测试基线: 659 passed / 11 skipped (+11 新测试，零退化)
  - ruff/tsc/build/git diff: clean

## Phase 9A4-NYC-01 Comparator 状态收口 Progress
Started: 2026-07-05
Base commit: 1f0e9dc
Task: complete (Comparator 状态收口——双重根因修复 + 显式断言)
  - RC1: BETWEEN 右值规范化——_flatten_filter_step 新增 list 处理 +
    _normalize_between_list / _normalize_between_right_string 两个 helper +
    compare() 中统一调用 _normalize_between_rights
  - RC2: derive_overall_status() 新增 comparator_report.status 检查——
    LOGIC_MISMATCH → REPAIR_NEEDED，LOGIC_UNSUPPORTED/NOT_COVERED → HUMAN_REVIEW_REQUIRED
  - 测试: tests/spark/test_plan_comparator.py +1 (test_filter_between_equivalent_different_literal_formats)
          tests/spark/test_orchestrator.py +1 (test_derive_overall_status_checks_comparator_report_not_just_stage_result)
          tests/api/test_nyc_business_case.py 加固 (显式断言 comparator_report.status + overall_status 一致性)
  - 测试基线: 661 passed / 11 skipped (+2 新测试，零退化)
  - ruff/git diff: clean
  - NYC 案例 01 Spark 双链路逻辑等价: 已点亮 (comparator_report.status=LOGIC_EQUIVALENT)

## R8 Progress
Started: 2026-07-05
Base commit: 5d5553a
Task 1: complete (commits 5d5553a..af6be79)
Task 2: complete (commits af6be79..11d32bc)
Task 3: complete (commits 11d32bc..2cadc8f)
Task 4: complete (verification: 850 passed / 11 skipped, ruff clean, gate OK)
Task 5: complete (commits 2cadc8f..b4300d5)
Final: 850 passed, 11 skipped, ruff clean, gate OK

## Phase 10 Case06 DAG Progress
Started: 2026-07-05
Base commit: b4300d5
Task 1: complete (commits b4300d5..af3dd48, review clean — spec ✅ quality Approved)
Task 2: complete (无需改动——Contract 提取已在 compute_steps 分支完整实现: pipeline.py L1116-1118 extract_v1 + L1201 _store_result + L1633 export_artifacts)
