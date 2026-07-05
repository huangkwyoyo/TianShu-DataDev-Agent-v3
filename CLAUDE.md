# CLAUDE.md — TianShu DataDev Agent v3

## 项目状态快速入口

**了解项目当前状态，首先阅读 `docs/current-state-and-verification-status.md`**——包含 Phase 进度矩阵、C1-C4 验证状态、当前测试基线（601 passed / 11 skipped）、残留风险、下一步方向。

## 代码规范

- **所有代码注释必须使用中文**（包括函数注释、变量说明、行内注释、文档字符串等）
- 注释应简洁明了，解释"为什么"而非"是什么"
- 函数/类使用简短的中文 docstring 说明用途

## CodeGraph 使用策略

**前置条件：仅当 `.codegraph/` 目录存在且 `codegraph_explore` MCP 工具可用时才启用。**
`.codegraph/` 在 `.gitignore` 中，不会被 git 追踪。未安装 CodeGraph 的 PC 上该目录不存在，
此时**完全跳过本节**，直接使用 Grep/Glob/Read 传统工具链，不影响任何工作流。

满足前置条件时，按以下策略使用：

- **优先使用 `codegraph_explore`**：理解代码结构、查找符号定义、分析调用链和影响范围时，先尝试 CodeGraph。一次调用通常能替代多轮 grep + Read。
- **不可用则回退**：如果 CodeGraph 返回空结果、daemon 未运行、或索引明显过时，直接使用 Grep/Glob/Read，不要阻塞工作流。
- **锁冲突处理**：如遇 `file lock held by another process` 错误，运行 `codegraph unlock` 清除残留锁，然后 `codegraph sync` 刷新索引。
- **索引同步**：大规模代码变更（>10 个文件）后，运行 `codegraph sync` 手动刷新，避免 auto-sync 延迟影响查询准确性。

**CodeGraph 的核心价值**：Blast Radius 分析（修改前知道谁依赖目标符号）和调用者统计（含测试覆盖标记），这两项是传统 grep 无法替代的。

## 外接知识文档路径

项目的外部知识积累存放在 Obsidian Vault 中，按优先级依次尝试以下路径：

1. `C:\Users\62414\Nutstore\1\Obsidian Vault\Ai Learning\Data Dev Agent知识积累`（主路径）
2. `C:\Users\Karvi_h\Nutstore\1\Obsidian Vault\Ai Learning\Data Dev Agent知识积累`（备用路径）

> 使用规则：按上述顺序尝试，第一个存在且可读的路径即为当前会话的有效外接知识路径。

### 使用规则

1. **知识查找优先级**：当需要查找 Data Dev Agent 相关的设计知识、架构分析、场景分析、实施经验等文档时，应同时检索项目内 `docs/` 目录和上述外接知识路径。
2. **输出知识文档时**：若输出内容引用或补充了外接知识库中的已有文档，应在文中标注来源路径。
3. **知识文档输出位置**：新产生的知识文档默认输出到项目 `docs/` 目录；若明确属于外部积累性质的内容，输出到上述外接路径。
4. **只读原则**：外接知识路径由 Obsidian 管理，除明确要求外，不对其进行批量修改或删除。
5. **路径识别**：凡涉及知识文档的读写、搜索、引用操作，自动将上述路径纳入候选范围，无需每次手动指定。

### 外接知识库当前内容概览

| 文档 | 说明 |
|------|------|
| `DataDev_Agent_项目规划书_DeveloperSpec-first_SQL-first到Spark-first_20260625.md` | 项目总体规划书 |
| `DataDev_Agent_Phase实施Prompt手册_DeveloperSpec-first_SQL-first到Spark-first_20260625.md` | Phase 实施 Prompt 手册 |
| `新架构详解_逻辑链路与物理链路_20260626_1730.md` | 新架构逻辑/物理链路详解 |
| `Spark DSL正确性保障_逻辑链路与物理链路_20260626_0829.md` | Spark DSL 正确性保障分析 |
| `SQL_Harness门禁设计_七维度评测框架_20260625_1430.md` | SQL Harness 七维评测框架 |
| `六个企业场景深度分析_20260626_1700.md` | 六大企业落地场景分析 |
| `规划合理性评估与技术架构分析_20260626_0930.md` | 规划与技术架构评估 |
| `Code Review分类体系CRCS_20260616_2300.md` | Code Review 分类体系 |
| `Fact Catalog概念与实现分析_20260624_1200.md` | Fact Catalog 概念与实现 |
| `一次性可写Sandbox_20260619_2307.md` | 一次性可写 Sandbox 设计 |
| `过时项目规划书.md` | 历史规划（已过时） |
| `过时实施文档.md` | 历史实施文档（已过时） |
