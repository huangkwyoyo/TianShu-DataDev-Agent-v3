# 项目当前状态与验证进度 — TianShu DataDev Agent v3

> 文档版本：2026-07-05 | 最后更新：2026-07-05 R8 真实 LLM 验证完成——8/8 通过，100% pass rate，31,517 tokens
> 本文是项目当前实施状态的**唯一权威文档**。各 Phase 设计文档（docs/00-09、docs/roadmap/）描述的是目标设计，实际建成状态以本文为准。

## 1. Phase 进度矩阵

| Phase | 名称 | 设计 | 实现 | 测试 | 备注 |
|:-----:|------|:---:|:---:|:---:|------|
| 0.5 | DeveloperSpec-first 架构校正 | ✅ | ✅ | ✅ | 文档迁移 + 路线图统一 |
| 1A/1B/1C | SQL 管线（输入→推理→编译） | ✅ | ✅ | ✅ | SQL-first v1.0 基础 |
| 2 | Code Review Package v1 | ✅ | ✅ | ✅ | |
| 3A/3B/3C | SqlProgram + 窗口 + 写入 | ✅ | ✅ | ✅ | |
| 4A-4D | SQL-first v1.0 硬化 | ✅ | ✅ | ✅ | LLM Gateway + Harness 七维 |
| 4.5 | 内部交互验证口 | ✅ | ✅ | ✅ | CLI + REST API |
| 4.6 | 复杂 SQL 渐进开放 | ✅ | ✅ | ✅ | 多跳 Join + FROM 子查询 |
| 5 | Spark-ready 契约 | ✅ | ✅ | ✅ | DataTransformContract + SparkPlan IR |
| 6A | scan/filter/project/sort/limit | ✅ | ✅ | ✅ | 编译 + Validator 全错误码 |
| 6B | aggregate/join/case_when | ✅ | ✅ | ✅ | 编译扩展 |
| 6C | window + 帧边界 | ✅ | ✅ | ✅ | 含 RepairPlanner |
| 7A | 逻辑链路 + Snapshot | ✅ | ✅ | ✅ | PlanComparator 9 种 step |
| 7B | 物理链路——双引擎验证 | ✅ | ✅ | ✅ | 11/11 真实 Spark 通过 |
| 7C | 物理链路扩展 + 安全加固 | ✅ | ✅ | ✅ | 窗口双引擎 + SQL 加固 |
| 8 | 编排硬化 + Harness | ✅ | ✅ | ✅ | Orchestrator + Review Package + 5 维度 |
| 9A | 生产级串联升级 | ✅ | ✅ | ✅ | 9A1-9A3 + 9A5 完成，9A4 NYC 01/06 完成，Case 06 B 类遗留 |
| 9B | 前端回归 + 可观测性 | ✅ | ✅ | ✅ | R11/R15 消除，2026-07-05 |
| 9B-P0 | Snapshot Builder 集成到 Pipeline | ✅ | ✅ | ✅ | R10 消除，可选注入+全链路覆盖，2026-07-05 |
| 9C | DOM E2E 交互测试 | ✅ | ✅ | ✅ | 6/6 Playwright 测试通过，2026-07-05 |
| 9C-R16 | table_paths 环境配置补齐 | ✅ | ✅ | ✅ | R16 消除，CSV fixture 自动发现，2026-07-05 |
| 9C-R16b | table_paths 边界硬化 | ✅ | ✅ | ✅ | None/{} 语义区分 + E2E 模式开关，2026-07-05 |
| 9B-P1 | provenance.yml 显式断言 | ✅ | ✅ | ✅ | snapshot_manifest_hash 测试覆盖矩阵补全，2026-07-05 |
| 9A4-NYC | 真实业务样本——NYC 案例 01-05 | ✅ | ✅ | 🟡 | Case 01-04 SQL+Spark 双链 LOGIC_EQUIVALENT，Case 05 Comparator NOT_COVERED（窗口函数），2026-07-05 |
| 10-Case06 | SqlProgram 多语句 DAG——NYC Case 06 | ✅ | 🟡 | 🟡 | 跨域融合 7 步 DAG，_temp_* 串联，比率计算/CASE WHEN 待后续 Phase，Spark Comparator 框架就绪（3 测试/1 xfail） |

**当前测试基线**：847 passed / 11 skipped / 4 xfailed（api/spark/artifacts/harness 全量后端）+ 23 passed（前端冒烟全量）+ 6 passed / 0 skipped（Playwright E2E），ruff/tsc/build 零告警

## 2. C1-C4 业务集成验证

| 编号 | 内容 | 风险等级 | 状态 | 证据 |
|:----:|------|:--------:|:----:|------|
| C1 | 真实 Spark 物理验证 | 已消除 | ✅ 11/11 通过 | PySpark 4.1.2，DuckDB ↔ PySpark 一致性 100% |
| C2 | LLM 基础设施架构收口 | 已消除 | ✅ 收口完成 | 重复文件已删除，18/18 测试全绿，DeepSeek 3/3 验证 |
| C3 | Comparator 真实逻辑对比 | 已消除 | ✅ 桥接+集成 | 30/30 测试全绿，Orchestrator COMPARATOR 集成 |
| C4 | Harness 5 维度评测 | 已消除 | ✅ 全 5 维度 | D1/D2/D3/D4/D5 共 31/31 测试全绿 |

**D4 重要说明（2026-07-05 更新）**：D4 LOGIC_EQUIVALENCE 已从**桥接级**升级到**生产级**（Phase 9A1-9A3 + 9A5）。当前使用 Pipeline.run_all() → export_artifacts() → adapt_lite_to_v1() → Orchestrator.run() → PlanComparator → ReviewBuilder → REVIEW_READY 判定的全链路闭环。`contract_to_sql_steps()` 保留为向后兼容路径（deprecated）。

## 3. 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R5 | ~~桥接函数替代完整 SQL Pipeline~~ | 已消除 | Phase 9A1-9A3 + 9A5 已升级为真实 Pipeline 全链路 |
| R6 | ~~Harness Runner 为结果聚合器~~ | 已消除 | Phase 9A3 已升级为自动评测驱动器 |
| R7 | 真实业务样本——NYC 案例 01-05 已完成（Case 01-04 LOGIC_EQUIVALENT，Case 05 NOT_COVERED），Case 06 Comparator 框架就绪（SqlProgram 多语句对比 + TestNYCCase06SparkDualChain），严格断言因 plan 拓扑非对称 xfail，剩余 1 个场景待接入 | B | Case 06 比率计算/CASE WHEN + plan 拓扑对齐待后续 Phase |
| R8 | ~~LLM 生产环境验证~~ | 已消除 | 2026-07-05 真实 LLM 验证 8/8 通过，100% pass rate，31,517 tokens，269.7s，DeepSeek v4-pro，报告：`llm_reports/verify_20260705.json`（已脱敏） |
| R9 | Case 05 Spark Comparator 窗口函数 NOT_COVERED——仅排除 LOGIC_MISMATCH，未证明等价 | C | 窗口函数（ROW_NUMBER）Comparator 覆盖待完善 |
| R10 | ~~Snapshot Builder 未集成到 REVIEW_READY 流程~~ | 已消除 | Phase 9B-P0 已将 SnapshotBuilder.build() 接入 Pipeline.run_all()，snapshot hash 写入 provenance.yml |
| R11 | ~~前端无自动化测试框架~~ | 已消除 | Phase 9B 源码级 + Phase 9C Playwright E2E |
| R15 | ~~SQL 成功态 pipeline_stages 为空~~ | 已消除 | handleRunAll 成功路径注入全成功阶段——SQL 指示灯始终可见 |
| R16 | ~~Playwright E2E 缺少 table_paths 配置~~ | 已消除 | Phase 9C-R16 + R16b：CSV fixture 自动发现（E2E 模式）+ None/{}} 语义区分 + 边界硬化 |

## 4. 当前架构全景

```
DeveloperSpec (.md 项目书)
    │
    ├─ SQL 管线（确定性，生产可用）
    │   Pipeline.run_all() → Parser → SourceManifest → SqlBuildPlan → Compiler → DuckDB
    │       │
    │       └─ export_artifacts() → PipelineArtifactBundle
    │           ├─ sql_build_plan (真实 SqlBuildPlan)
    │           └─ data_transform_contract
    │               │
    │               └─ adapt_lite_to_v1() → DataTransformContractV1
    │
    └─ Spark 管线（确定性，生产级验证）
        DataTransformContractV1 → Mapper → SparkPlan → Compiler → Validator
                                        │                      │
                                        └── PlanComparator ────┘  ← 双管线逻辑对比
                                             PhysicalVerifier      ← 双引擎物理对比
                                             Orchestrator          ← 6 阶段编排
                                             Harness 5 维度        ← 评测框架
                                                  │
                                                  └─ SparkReviewBuilder.build()
                                                         │
                                                         └─ SparkReviewPackage
                                                            ├─ provenance (完整溯源链)
                                                            ├─ stage_results (6 阶段结果)
                                                            ├─ comparator_status (对比器状态)
                                                            └─ review_ready ★ REVIEW_READY 判定
```

## 5. 下一步方向（Phase 10+）

1. **真实业务样本端到端验证**——NYC 案例 01-05 已完成（Case 01-04 LOGIC_EQUIVALENT，Case 05 NOT_COVERED），Case 06 A 类完成 + B 类遗留
2. **Case 05 Comparator 升级**——窗口函数（ROW_NUMBER）Comparator 从 NOT_COVERED 升级到严格等价判定
3. **Case 06 Spark Comparator 接入**——多语句 DAG 从未走双链验证，需补齐
4. **生产环境 LLM 验证**——R8 脚本就绪，待 API key 配置后执行：`TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py --output llm_reports/verify_$(date +%Y%m%d).json`
5. **Case 06 遗留工作**（C 类→后续 Phase）：
   - `compute_ratios` 步骤的比率计算（crash_per_million_trips = total_crashes / total_trip_count * 1e6）
   - `risk_label` 步骤的 CASE WHEN 输出支持
   - `violation_county` 代码映射的方案通用化（当前硬编码 NYC 5 个代码）
6. **Case 06 遗留工作**（B 类→需解锁）：
   - Builder 对多语句 DAG 的 CaseWhenStep 生成支持
   - 派生表达式（比率计算）的 compute_step 渲染

## 6. 关键文档索引

| 文档 | 用途 |
|------|------|
| `docs/00-product-charter.md` | 产品愿景和验收标准 |
| `docs/01-target-architecture.md` | 目标架构（设计参考） |
| `docs/04-spark-multi-agent-plan.md` | Phase 6 Spark DSL 设计 |
| `docs/05-cross-validation-and-repair-plan.md` | Phase 7 双链验证设计 |
| `docs/06-langgraph-orchestration-plan.md` | Phase 8 编排设计 |
| `docs/09-test-strategy.md` | 测试策略 |
| `docs/risks/phase-6-8-known-risks.md` | C1-C4 风险详细登记 |
| `docs/roadmap/` | 各 Phase 实施路线图 |
| `docs/superpowers/specs/` | Phase 6-8 完整设计+实施计划 |
