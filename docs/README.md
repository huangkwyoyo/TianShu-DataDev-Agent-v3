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
- `docs/superpowers/`：完整设计（specs）+ 实施计划（plans）
- `docs/risks/phase-6-8-known-risks.md`：**Phase 6-8 历史风险与验收证据**（已冻结），当前风险以 `current-state-and-verification-status.md` §3、§3.5 为准
