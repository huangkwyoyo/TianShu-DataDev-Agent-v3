# 项目当前状态与验证进度 — TianShu DataDev Agent v3

> 文档版本：2026-07-13 CRE 物理验证可用 | 最后更新：2026-07-13 CRE shadow 最终验收通过——物理验证可用，CRE 保持 shadow，legacy 继续负责最终状态判定
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
| 9A | 生产级串联升级 | ✅ | ✅ | ✅ | 9A1-9A3 + 9A5 完成，9A4 NYC 01-06 全量完成 |
| 9B | 前端回归 + 可观测性 | ✅ | ✅ | ✅ | R11/R15 消除，2026-07-05 |
| 9B-P0 | Snapshot Builder 集成到 Pipeline | ✅ | ✅ | ✅ | R10 消除，可选注入+全链路覆盖，2026-07-05 |
| 9C | DOM E2E 交互测试 | ✅ | ✅ | ✅ | 6/6 Playwright 测试通过，2026-07-05 |
| 9C-R16 | table_paths 环境配置补齐 | ✅ | ✅ | ✅ | R16 消除，CSV fixture 自动发现，2026-07-05 |
| 9C-R16b | table_paths 边界硬化 | ✅ | ✅ | ✅ | None/{} 语义区分 + E2E 模式开关，2026-07-05 |
| 9B-P1 | provenance.yml 显式断言 | ✅ | ✅ | ✅ | snapshot_manifest_hash 测试覆盖矩阵补全，2026-07-05 |
| 9A4-NYC | 真实业务样本——NYC 案例 01-05 | ✅ | ✅ | 🟡 | Case 01-04 SQL+Spark 双链 LOGIC_EQUIVALENT，Case 05 Comparator NOT_COVERED（窗口函数），2026-07-05 |
| 10-Case06 | SqlProgram 多语句 DAG——NYC Case 06 | ✅ | ✅ | ✅ | **2026-07-06 闭环**："三层剥离"（_temp_* scan/join 过滤 + grain-aware aggregate 合并 + target_grain 透传），1 xfail 转正（test_spark_orchestrator_logic_equivalence → LOGIC_EQUIVALENT），659 passed / 11 skipped（spark+artifacts+api） |
| 10-ContentAlign | Spark Comparator 内容级对齐 | ✅ | ✅ | ✅ | **2026-07-06 完成**：8 commits，PlanComparator.compare_program() 三层剥离 + Orchestrator target_grain 透传 + Contract _temp_ 守卫，详见 `docs/superpowers/specs/2026-07-06-spark-comparator-closure-and-risks.md` |
| CRE Phase 2 | CRE shadow 最终准入硬化 | ✅ | ✅ | ✅ | **2026-07-13 物理验证可用**：三条 Pipeline 证据通过——RESULT_CONSISTENT（一致）、CONSISTENT_WITH_WARN（浮点容差）、RESULT_MISMATCH（真实差异）。CRE 保持 shadow，legacy 继续负责最终状态判定。不切换生产门禁。详见 `docs/CRE_v2_设计文档_20260713_1745.md` |

**当前测试基线**：2601 passed / 24 skipped / 2 xfailed / 13 预存失败（NYC Case 03/04 DuckDB 扩展依赖 + harness gate 缺真实人工数据），ruff/tsc/build 零告警。

CRE 相关测试基线：
- CRE 核心（test_cre / test_cre_dual_engine / test_cre_shadow_pipeline / test_cre_finalizer）：125 passed / 7 skipped
- Physical Verifier（test_physical_verifier，含 CRE shadow 集成）：191 passed / 11 skipped
- artifacts 层（含 finalizer E2E）：全部通过

**三条 Pipeline 验收证据（2026-07-13）**：

| 证据 | 测试 | 路径 | 结果 |
|:----:|------|------|:----:|
| 证据 1：一致 | `TestPhysicalVerifierWithMock::test_result_consistent` | `verifier.verify()` 全链路 → CRE shadow | **RESULT_CONSISTENT** — DuckDB/Spark 完全一致 |
| 证据 2：浮点容差 WARN | `TestPhysicalVerifierShadow::test_shadow_warn_maps_to_consistent` | `_shadow_cre_diagnose()` → DecisionEngine | **CONSISTENT_WITH_WARN** — 浮点 1e-11 级差异在容差内，WARN 但不阻断 |
| 证据 3：真实差异 | `TestPhysicalVerifierWithMock::test_result_mismatch` | `verifier.verify()` 全链路 → CRE shadow | **RESULT_MISMATCH** — Spark 返回不同值，正确检出，原因清晰 |

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
| R7 | ~~真实业务样本——NYC 案例 01-06 全部完成。Case 06 Spark Comparator LOGIC_EQUIVALENT 仍 xfail~~ | 已消除 | **2026-07-06 消除**：Spark Comparator 内容级对齐完成，"三层剥离"使 Case 06 双链达到 LOGIC_EQUIVALENT，xfail 转正 |
| R-CA-1 | `target_grain` 过滤是 Case 06 特化——基于"非目标粒度=内部实现"假设，不能误解为通用业务真理。多输出粒度场景需扩展为 `target_grains` | **C** | **架构风险**——当前逻辑对 Case 06 正确，但不得推广。详见 `docs/superpowers/specs/2026-07-06-spark-comparator-closure-and-risks.md` |
| R-CA-3 | Builder 缺 join——Case 06 Step 4 的 join step 在 builder 输出中缺失，新 case 可能暴露 | **中高（B）** | **B 类设计修复项**——独立排查 builder join 生成逻辑，记忆文件：[[rc-a-3-builder-join-bug]] |
| R8 | ~~LLM 生产环境验证~~ | 已消除 | 2026-07-05 真实 LLM 验证 8/8 通过，100% pass rate，31,517 tokens，269.7s，DeepSeek v4-pro，报告：`llm_reports/verify_20260705.json`（已脱敏） |
| R9 | Case 05 Spark Comparator 窗口函数 NOT_COVERED——仅排除 LOGIC_MISMATCH，未证明等价 | C | 窗口函数（ROW_NUMBER）Comparator 覆盖待完善 |
| R10 | ~~Snapshot Builder 未集成到 REVIEW_READY 流程~~ | 已消除 | Phase 9B-P0 已将 SnapshotBuilder.build() 接入 Pipeline.run_all()，snapshot hash 写入 provenance.yml |
| R11 | ~~前端无自动化测试框架~~ | 已消除 | Phase 9B 源码级 + Phase 9C Playwright E2E |
| R15 | ~~SQL 成功态 pipeline_stages 为空~~ | 已消除 | handleRunAll 成功路径注入全成功阶段——SQL 指示灯始终可见 |
| R16 | ~~Playwright E2E 缺少 table_paths 配置~~ | 已消除 | Phase 9C-R16 + R16b：CSV fixture 自动发现（E2E 模式）+ None/{} 语义区分 + 边界硬化 |
| R-CRE-Golden | Golden Registry 为空——`passes_admission` 要求至少一个 golden MISMATCH 样本验证 Harness 判别能力 | 低（非阻断） | 后续 Phase 业务方填充已知差异样本 |
| R-CRE-Null | `null_strategy` 始终 UNKNOWN——无法从 Contract 或执行环境证明 NULL 语义一致性 | 低（非阻断） | 仅使相关语义进入 HUMAN_REVIEW，不误伤不涉及该策略的场景 |
| R-CRE-Finalizer | ReviewPackageFinalizer 为审计附属能力——写入失败只影响审计完整性，不改变 legacy 比较结论 | 低（非阻断） | 已实现并测试，明确为非阻断附属能力 |

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
                                        ├── PlanComparator.compare()        ← 单 plan 路径（SqlBuildPlan）
                                        ├── PlanComparator.compare_program() ← 多语句 DAG 路径（SqlProgram）
                                        │       └─ 三层剥离：_temp_* 过滤 + grain-aware merge + target_grain 过滤
                                        ├── PhysicalVerifier                ← 双引擎物理对比
                                        ├── Orchestrator                    ← 6 阶段编排
                                        └── Harness 5 维度                  ← 评测框架
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

1. ~~**Case 06 Spark 双链 LOGIC_EQUIVALENT**~~ → **✅ 已完成（2026-07-06）**
2. ~~**CRE shadow 最终准入硬化**~~ → **✅ 已完成（2026-07-13）——物理验证可用，CRE 保持 shadow**
3. **CRE 门禁切换（非阻断后续事项）**——以下三项为已知缺口，记录为后续 Phase 工作，不阻塞当前物理验证上线：
   - **Golden Registry 为空**：`harness/datasets/regression/golden_registry.json` 无已知差异样本，`passes_admission` 要求 `total_known_differences > 0`（至少一个 golden MISMATCH 样本）才能验证 Harness 判别能力。需业务方注册已知差异样本后门禁切换前提才算完全满足。
   - **NULL strategy 始终 UNKNOWN**：无法从 Contract 或执行环境证明 NULL 语义一致性，仅使相关语义进入 HUMAN_REVIEW，不误伤不涉及该策略的精确一致场景。
   - **门禁切换需 Owner 批准**：设计文档 v2.3 中的接入前提（可执行样本一致率 100%、零假阴性、CRE/legacy 冲突 0）已全部满足，但正式接管生产门禁需显式批准。
4. **Case 05 Comparator 升级**——窗口函数（ROW_NUMBER）Comparator 从 NOT_COVERED 升级到严格等价判定
5. **Builder join 缺陷修复**（R-CA-3，中高）——Case 06 Step 4 的 join 在 builder 输出中缺失，新 case 可能暴露。B 类设计修复项，下一轮迭代优先处理
6. **target_grain 扩展为 target_grains**（R-CA-1，C）——当前单粒度过滤是 Case 06 特化，多输出粒度场景需重构
7. **`_temp_` 前缀检测统一**（R-CA-2，B）——提取共享 `_is_temp_table()` 谓词，消除 plan_comparator/contract_extractor 两处检测逻辑不一致
8. **生产环境 LLM 验证**——R8 脚本就绪，待 API key 配置后执行：`TIANSHU_RUN_REAL_LLM=1 python scripts/real_llm_regression.py --output llm_reports/verify_$(date +%Y%m%d).json`
9. **Case 06 遗留工作**（C 类→后续 Phase）：
   - `violation_county` 代码映射的方案通用化（当前硬编码 NYC 5 个代码）
   - `test_temp_tables_cleaned_after_execution` 真正的 temp 表清理验证（需 Pipeline 暴露 cleanup_status）

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
