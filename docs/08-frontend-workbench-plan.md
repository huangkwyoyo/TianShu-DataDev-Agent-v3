# 内部交互验证口计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0.5 DeveloperSpec-first 架构校正版

## 1. 定位

Phase 4.5 建设的内部交互验证口是开发团队使用的轻量工具，用于输入 DeveloperSpec 项目书 → 走完整 SQL-first 链路 → 输出 Code Review Package。它是验证工具，不是生产执行入口，也不是面向业务人员的自然语言问数界面。

Phase 1-4 通过 CLI/API 验证核心能力；Phase 4.5 在此之上增加简单的 Web 界面。

## 2. 核心工作流

1. **输入 DeveloperSpec**：编辑器加载空白模板或选择预设模板（单表聚合、两表 Join、多表 Join + 聚合、窗口 TopN、CASE 标签分类等）。
2. **结构化解析预览**：查看 ParsedDeveloperSpec 的结构化视图（表、字段、指标、维度、Join 声明）。
3. **SourceManifest 匹配**：查看字段来源标记（developer_spec / schema_registry / snapshot_profile）和 SOURCE_CONFLICT 条目。
4. **OpenQuestion 面板**：查看 blocking（红色）和非 blocking（黄色）问题，程序员可在面板中确认、拒绝或修改。
5. **查看 SqlBuildPlan / SqlProgram**：查看类型化 SQL 构建计划（8 step 结构）或 SqlProgram DAG 依赖图。
6. **查看 SQL 编译产物和 ExecutionTrace**：查看 Compiler 生成的 SQL、OptimizedSQLPlan 优化链、DuckDB 执行结果和 ResultSummary。
7. **查看 Code Review Package**：预览审查包目录结构和文件内容。
8. **下载 Code Review Package**：下载完整审查包。

## 3. 状态展示

UI 必须展示精确状态：`DRAFT`、`STATIC_VALIDATED`、`EXECUTION_PASS`、`LOGIC_EQUIVALENT`、`RESULT_CONSISTENT`、`RESULT_MISMATCH`、`UNSUPPORTED_SEMANTICS`、`NOT_EXECUTED`、`REVIEW_READY` 和 `HUMAN_REVIEW`。

禁止只用绿色 PASS/红色 FAIL 掩盖未执行、样本一致和人工审查的差别。每个状态提供简短边界说明。

## 4. 页面

- **DeveloperSpec Editor**：Markdown + YAML-like 项目书编辑器、模板加载按钮、语法高亮。
- **Parse Preview**：ParsedDeveloperSpec 结构化视图（表/字段/指标/维度）、SourceManifest 匹配结果、SOURCE_CONFLICT 警告。
- **OpenQuestion Panel**：blocking（红色）和非 blocking（黄色）问题列表，可交互确认/修改。
- **SqlBuildPlan View**：8 step 类型化计划的可视化展示（scan → filter → join → aggregate → project → sort → limit）。
- **SqlProgram DAG View**：多语句 DAG 依赖图可视化（Phase 3A+）。
- **SQL Artifacts**：编译产物 SQL、OptimizedSQLPlan、ExecutionTrace、ResultSummary。
- **Code Review Package**：审查包目录结构预览和下载。

## 5. 人工操作边界

**允许**：选择模板、编辑 DeveloperSpec、提交解析、确认/拒绝 OpenQuestion、查看 artifact、下载审查包。

**禁止**：在线绕过 Validator 执行代码、直接修改 Comparator 结论、批准上线、配置生产连接和触发生产任务、作为业务人员自然语言问数入口。

## 6. 数据与安全

- API 返回 artifact 摘要和按需分页内容，不返回完整 DataFrame。
- 差异样本默认脱敏并限制行数。
- 前端不保存 LLM API Key 和数据源凭据。
- 所有人工动作产生审计 artifact，不直接篡改历史 State。

## 7. 验收

1. DeveloperSpec 编辑器可用：输入项目书 → 模板加载 → 解析预览 → OpenQuestion 面板。
2. 能区分所有精确验证状态。
3. 能追溯代码、事实源（SourceManifest/SchemaRegistry）、快照、环境和返工版本。
4. `HUMAN_REVIEW` 可以暂停和恢复。
5. 下载包与后端 artifact manifest 一致。
6. UI 不存在生产部署和在线任意代码执行入口。
7. 不做面向业务人员的自然语言问数主入口。

---

> Phase 0.5 DeveloperSpec-first 校正 | 2026-06-26 | Phase 4.5 实施依据
