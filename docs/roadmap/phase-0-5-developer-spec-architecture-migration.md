# Phase 0.5：DeveloperSpec-first 文档迁移与路线图统一

> 状态：✅ 已完成（2026-06-26）
> 下一阶段：Phase 1A — DeveloperSpec Parser + SourceManifest

## 执行前必须阅读

1. `AGENTS.md` — 项目宪法
2. `docs/00-product-charter.md` — 产品宪章
3. `docs/01-target-architecture.md` — 目标架构
4. `docs/03-sql-ir-and-compiler-plan.md` — SQL IR 和编译器计划
5. 主规划书：`DataDev_Agent_项目规划书_DeveloperSpec-first_SQL-first到Spark-first_20260625.md`

## 只允许修改

- `README.md`
- `AGENTS.md`
- `docs/00` 至 `docs/09`
- `docs/roadmap/**`

## 禁止修改

- `src/**`
- `tests/**`
- 任何 Phase 1 及后续代码实现

## 新增模型

本阶段不新增代码模型。文档层新增以下概念定义：

- `DeveloperSpec`：程序员编写的 Markdown + YAML-like metadata block 项目书
- `ParsedDeveloperSpec`：Parser 确定性解析输出
- `OpenQuestion`：Parser/推理无法确定的问题（含 `blocking: bool`）
- `SourceConflict`：DeveloperSpec 声明 vs SchemaRegistry 物理事实冲突
- `SourceManifest`：表字段事实追踪（三来源标记）
- `RelationshipHypothesis`：Join 推理候选 + 证据链 + 证据等级
- `SqlBuildPlan`：8 step 类型化单语句 SQL 构建计划
- `SqlProgram`：多语句 DAG + _temp 中间表
- `DataTransformContract`：三级递进（lite → v1 → Phase 5 消费）

## artifact schema

不适用（本阶段不产生运行时 artifact）。

## 必须新增的测试

不适用（本阶段不修改代码，不新增测试）。

## 必须运行的检查

```bash
rg -n "RequirementIR|SubIntent|Fact Catalog Adapter|planning_table" README.md AGENTS.md docs/
rg -n "raw_sql|where_sql|join_on: str|expression: str" README.md AGENTS.md docs/
rg -n "DeveloperSpec|ParsedDeveloperSpec|SourceManifest|RelationshipHypothesis|SqlBuildPlan|DataTransformContract" README.md AGENTS.md docs/
rg -n "phase-1-2|phase-1-5|phase-2-spark|phase-3-dual|phase-4-repair|phase-5-frontend|phase-6-harness|phase-7-llm" docs/roadmap/
ls docs/roadmap/ | grep -E "phase-1-2|phase-1-5|phase-2-spark|phase-3-dual|phase-4-repair|phase-5-frontend|phase-6-harness|phase-7-llm"
git diff --check
```

## B/C 暂停条件

- 发现新的架构边界争议（如 Spark 侧输入源选择、Join 推理权责分配）
- 发现宪法章节（AGENTS.md §2-§10）中需修改的硬边界
- 旧术语清除时发现需修改代码才能解决的遗留引用

## 退出条件

1. `README.md`、`AGENTS.md`、`docs/00`、`docs/01`、`docs/03`、`docs/roadmap` 全部对齐 DeveloperSpec-first
2. Phase 1 不再要求 Fact Catalog Adapter 或 RequirementIR / SubIntent 主链路
3. 四组全局 rg 检查通过——旧路线术语零残留或全部在迁移/替换上下文
4. `docs/roadmap/` 根目录只有新命名规范的 Phase 文件
5. `git diff --check` 通过
6. G1-G5 五道门禁全部满足

---

> Phase 0.5 完成 | 2026-06-26 | 下一阶段：Phase 1A
