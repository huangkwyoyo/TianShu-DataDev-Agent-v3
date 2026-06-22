# Phase 0.5 Architecture Contract Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在进入 Phase 1 前统一 SQL IR、PySpark 生成、同源快照、交叉验证、LangGraph State、Harness 与 Memory 的规划契约。

**Architecture:** SQL 分支只接收类型化计划并由 Python 编译；Spark 分支生成受控纯转换函数；两个执行引擎读取同一个关系一致快照，由确定性 Comparator 产生验证结论。LangGraph 只保存状态和 artifact 引用，不承载业务规则或数据集。

**Tech Stack:** Markdown、Python 类型契约设计、DuckDB、PySpark、LangGraph、Parquet。

---

### Task 1: 校正核心架构契约

**Files:**
- Modify: `docs/00-product-charter.md`
- Modify: `docs/01-target-architecture.md`
- Modify: `docs/03-sql-ir-and-compiler-plan.md`

- [x] 定义开发审查级与 AssuranceLevel，禁止用通用 PASS 表示生产就绪。
- [x] 将 SQLPlan 中的字符串表达式替换为类型化表达式 AST 规划。
- [x] 增加 TransformationContract 与 MergePlan 边界。

### Task 2: 校正 Spark 与双引擎验证契约

**Files:**
- Modify: `docs/04-spark-multi-agent-plan.md`
- Modify: `docs/05-cross-validation-and-repair-plan.md`

- [x] 固定 `transform(inputs, params) -> DataFrame` 纯函数入口。
- [x] 明确 Developer、Reviewer、Tester 的独立输入输出及代码再验证路径。
- [x] 定义关系一致快照、语义规范化、精确验证状态和返工边界。

### Task 3: 校正编排、Harness 与测试路线

**Files:**
- Modify: `docs/06-langgraph-orchestration-plan.md`
- Modify: `docs/07-harness-and-memory-plan.md`
- Modify: `docs/09-test-strategy.md`
- Modify: `docs/roadmap/phase-1-sql-vertical-slice.md`
- Modify: `docs/roadmap/phase-2-spark-multi-agent.md`
- Modify: `docs/roadmap/phase-3-dual-engine-validation.md`
- Modify: `docs/roadmap/phase-4-repair-loop.md`
- Modify: `docs/08-frontend-workbench-plan.md`
- Modify: `docs/roadmap/phase-0-bootstrap.md`
- Modify: `docs/roadmap/phase-5-frontend.md`
- Modify: `docs/roadmap/phase-6-harness-memory.md`
- Modify: `docs/roadmap/phase-7-llm-hardening.md`

- [x] Graph State 只保存 artifact 引用、哈希、状态和摘要。
- [x] Harness 与 pytest 分离，Memory 暂不参与运行时决策。
- [x] 统一 Phase 0-7 的交付物、依赖和测试预算。

### Task 4: 同步项目规则并验证

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`

- [x] 同步 Phase 0.5 的硬边界和当前状态。
- [x] 运行 `python -m pytest tests -q`，预期现有测试通过。
- [x] 运行 `python -m ruff check .`，记录现有代码质量结果；本轮不修改 Python 文件。
- [x] 运行 `git diff --check` 和文档关键词扫描，确认无冲突表述。
