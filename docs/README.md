# 文档索引 — TianShu DataDev Agent v3

> **唯一入口**：本文帮助快速定位所需文档。**所有文档的当前实施状态以 `current-state-and-verification-status.md` 为准**，各设计文档描述的是目标设计，实际建成状态可能不同。

---

## 快速入口

| 你想找什么 | 读这个 |
|-----------|--------|
| 项目当前做到哪一步了？ | [`current-state-and-verification-status.md`](current-state-and-verification-status.md) |
| Agent 必须遵守什么规则？ | [`../AGENTS.md`](../AGENTS.md)（项目宪法） |
| label_table 类型怎么实现的？ | [`superpowers/specs/2026-07-15-label-table-design.md`](superpowers/specs/2026-07-15-label-table-design.md) |
| CASE WHEN 为什么不对比 condition？ | [`case_when条件对比边界说明_20260717_0908.md`](case_when条件对比边界说明_20260717_0908.md) |
| 产品定位和目标？ | [`00-product-charter.md`](00-product-charter.md) |
| 架构设计？ | [`01-target-architecture.md`](01-target-architecture.md) |
| 测试策略？ | [`09-test-strategy.md`](09-test-strategy.md) |
| Pipeline 实现细节？ | [`pipeline_主链路详解_20260702_2140.md`](pipeline_主链路详解_20260702_2140.md) |
| CRE 双引擎验证？ | [`CRE_v2_设计文档_20260713_1745.md`](CRE_v2_设计文档_20260713_1745.md) |
| DeveloperSpec 怎么写？ | [`examples/`](examples/) |
| 工程术语表？ | [`datadev_engineering_glossary_20260629_1600.md`](datadev_engineering_glossary_20260629_1600.md) |
| ComputeSteps Builder 双能力扩展？ | [`superpowers/specs/2026-07-24-compute-steps-builder-extension-design.md`](superpowers/specs/2026-07-24-compute-steps-builder-extension-design.md) |
| RequirementPlanner 如何工作？ | [`superpowers/specs/2026-07-21-requirement-planner-design.md`](superpowers/specs/2026-07-21-requirement-planner-design.md) |
| Label Table 统一管线设计？ | [`superpowers/specs/2026-07-22-label-table-unified-pipeline-design.md`](superpowers/specs/2026-07-22-label-table-unified-pipeline-design.md) |
| SparkPlan 多分支 DAG 设计？ | [`superpowers/specs/2026-07-10-sparkplan-v2-design.md`](superpowers/specs/2026-07-10-sparkplan-v2-design.md) |
| 覆盖率、优化 Pass、安全门禁？ | [`superpowers/specs/2026-07-08-comparator-gap-fix-design.md`](superpowers/specs/2026-07-08-comparator-gap-fix-design.md) + [`superpowers/specs/2026-07-09-left-join-safety-gate-v2-design.md`](superpowers/specs/2026-07-09-left-join-safety-gate-v2-design.md) |

---

## 文档分类

### 📌 权威状态（1 份——唯一事实源）

| 文档 | 说明 |
|------|------|
| `current-state-and-verification-status.md` | **当前实施状态的唯一权威文档**。Phase 进度、测试基线、残留风险、架构全景 |

### ⚖️ 宪法（1 份——不可违反）

| 文档 | 说明 |
|------|------|
| `../AGENTS.md` | 项目宪法。所有 Agent、LLM 角色和自动化工具必须遵守 |

### 🏛️ 架构与设计

| 文档 | 说明 |
|------|------|
| `00-product-charter.md` | 产品宪章——愿景、AssuranceLevel、验收标准 |
| `01-target-architecture.md` | 目标架构——原则、数据流、组件关系 |
| `02-reuse-and-migration-map.md` | 复用与迁移地图——现有项目审计 |
| `03-sql-ir-and-compiler-plan.md` | SQL IR 与编译器计划 |
| `07-harness-and-memory-plan.md` | Harness + Memory 边界 |
| `08-frontend-workbench-plan.md` | 内部交互验证口设计 |
| `09-test-strategy.md` | 测试策略——预算、方法、基线 |

### 🔧 实现细节

| 文档 | 说明 |
|------|------|
| `pipeline_主链路详解_20260702_2140.md` | SQL 管线 Stage 1-7 内部实现（仅 SQL 部分） |
| `datadev_engineering_glossary_20260629_1600.md` | 工程术语表 |

### 🔬 特性设计（Specs）

| 文档 | 说明 |
|------|------|
| `superpowers/specs/` | 各特性的完整设计文档。Spark-first v2.0、CRE、label_table v1、监控等详见目录索引 |

### 📋 实施计划（Plans）

| 文档 | 说明 |
|------|------|
| `superpowers/plans/README.md` | 方案书索引与执行链路 |

### 🗂️ 设计取舍与边界

| 文档 | 说明 |
|------|------|
| `case_when条件对比边界说明_20260717_0908.md` | CASE WHEN condition UNSUPPORTED 的设计取舍 |
| `CRE_v2_设计文档_20260713_1745.md` | CRE v2 双引擎编码比较体系 |
| `CRE_v3_设计文档_20260713_2000.md` | CRE v3 CDP 工程化 |
| `diagnostic-monitor-analysis_20260715_1430.md` | 诊断监控方案分析 |
| `superpowers/specs/2026-07-15-label-table-design.md` | label_table v1 类型完整管线设计（Parser → Extractor → Validator → Promotion → Builder → Compiler） |
| `superpowers/specs/2026-07-21-requirement-planner-design.md` | RequirementPlanner v3.1 设计——TimeTransformExpr + UncertaintyEntry 路由 |
| `superpowers/specs/2026-07-22-label-table-unified-pipeline-design.md` | Label Table 统一管线——所有 dataset_type 走统一管线，删除 label_scope.py |
| `superpowers/specs/2026-07-24-compute-steps-builder-extension-design.md` | ComputeSteps Builder 双能力扩展——case_when+metrics 共存 + 混合源 Join + 两跳桥接 JOIN |
| `superpowers/specs/2026-07-10-sparkplan-v2-design.md` | SparkPlan 多分支 DAG——branches + 编译器支持，typed_branches 替换 SqlRawExpression |
| `superpowers/specs/2026-07-06-spark-comparator-closure-and-risks.md` | Spark Comparator 内容级对齐闭环报告与残留风险 |
| `superpowers/specs/2026-07-10-full-pipeline-monitoring-design.md` | 全链路监控设计 |
| `superpowers/specs/2026-07-09-left-join-safety-gate-v2-design.md` | LEFT JOIN 安全门禁 v2 设计 |
| `superpowers/specs/2026-07-09-step-alias-human-friendly-design.md` | SparkPlan step alias 人性化命名设计 |
| `superpowers/specs/2026-07-06-spark-stage-independent-and-llm-traces-design.md` | Spark 阶段独立执行 + LLM 追踪面板设计 |
| `superpowers/specs/2026-07-07-spark-compiler-llm-annotation-injection-design.md` | Spark Compiler LLM 标注注入设计 |

### 📎 示例

| 文档 | 说明 |
|------|------|
| `examples/developer-spec-01-aggregate-table.md` | 汇总表示例 |
| `examples/developer-spec-02-label-table.md` | 标签表示例 |
| `examples/developer-spec-03-multi-step.md` | 多步骤加工示例 |

### 🗄️ 已归档

以下文档描述的阶段/计划已全部完成，保留作为历史参考：

| 文档 | 归档原因 |
|------|----------|
| `04-spark-multi-agent-plan.md` | Phase 6 设计，已全部实现 |
| `05-cross-validation-and-repair-plan.md` | Phase 7 设计，已全部实现 |
| `06-langgraph-orchestration-plan.md` | Phase 8 设计，已全部实现 |
| `llm_response_fixture_plan_20260701.md` | LLM 响应 Fixture 计划已实现 |
| `spec_enricher_validation_gap_fix_plan_20260701.md` | SpecEnricher 验证缺口已修复 |
| `spec_schema_dag_extension_plan_20260701.md` | Schema DAG 扩展已实现 |
| `企业落地场景与业界分析_20260626_1500.md` | 历史企业场景分析 |
| `roadmap/`（全部 phase-*.md 文件） | Phase 0-8 路线图设计文档，全部 Phase 已完成 |
| `risks/phase-6-8-known-risks.md` | Phase 6-8 历史风险登记（已复核），当前残留风险仅 Case05-Comparator |
| `superpowers/plans/2026-07-05-phase-9*.md` | Phase 9B/9C 实施计划，已完成 |
| `superpowers/plans/2026-07-05-r8-llm-production-verification.md` | R8 真实验证计划，已完成 |
| `superpowers/plans/2026-07-15-label-table-implementation.md` | label_table 实施计划，已完成 |
| `superpowers/plans/2026-07-21-requirement-planner-implementation.md` | RequirementPlanner 实施计划，已完成 |
| `superpowers/plans/2026-07-22-label-table-unified-pipeline.md` | Label Table 统一管线实施计划，已完成 |
| `superpowers/plans/2026-07-24-compute-steps-builder-extension.md` | ComputeSteps Builder 扩展实施计划，已完成 |

---

## 文档状态约定

| 标记 | 含义 |
|:----:|------|
| ✅ | 设计/实现/测试均已完成 |
| 🟡 | 功能完成，部分场景待增强 |
| 📋 | 计划中或部分完成 |
| 🗄️ | 已归档（历史参考） |

## 交叉引用

- `docs/roadmap/`：各 Phase 实施路线图（Phase 0-8 全部完成，历史参考）
- `docs/superpowers/specs/`：各特性完整设计文档（含 Phase 9A-9C、label_table v1、RequirementPlanner v3.1、ComputeSteps Builder 扩展、SparkPlan v2、CRE v2-v3 等）
- `docs/superpowers/plans/`：实施计划方案书
- `docs/risks/phase-6-8-known-risks.md`：**Phase 6-8 历史风险与验收证据**（已复核），当前风险以 `current-state-and-verification-status.md` §3、§3.5 为准
- `docs/current-state-and-verification-status.md`：**项目当前状态的唯一权威文档**
