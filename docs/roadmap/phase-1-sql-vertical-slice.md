# Phase 1：SQL 垂直切片

## 目标

实现端到端的单项目书、单 SubIntent 从需求到 SQL 执行和结果摘要的完整链路。证明基础架构可行：RequirementIR 输入 -> SubIntent 提取 -> SQLPlan 生成（确定性编译，非 LLM）-> DuckDB 只读执行 -> ExecutionTrace + ResultSummary 输出。

## 输入

- **RequirementIR**：结构化项目书，包含需求描述、关联表列表、过滤条件和聚合意图
- **SubIntent**：从 RequirementIR 中提取的单条子意图（select、filter、group_by、order_by、limit）
- **TianShu contracts/ 事实源**：表名、字段名、类型、主外键关系、JOIN 路径，来自 `contracts/` 目录

## 输出

| 产出 | 说明 |
|------|------|
| **确定性 SQL 编译器** | Python 拼接生成 SQL，不含 LLM 调用；表名、字段、JOIN 全部源自 contracts |
| **DuckDB 只读执行器** | 以只读模式连接 DuckDB，执行 SQL 并返回结果集 |
| **ExecutionTrace** | 记录编译过程、执行计划、行数、耗时等元信息 |
| **ResultSummary** | 对执行结果的摘要统计（行数、列数、空值率、数值分布等） |
| **集成测试套件** | 覆盖典型 SubIntent 的编译-执行-摘要全流程 |

## 模块职责

- **sql_compiler/**：接收 SubIntent，根据 contracts 中的元数据确定性地拼接 SQL，支持 SELECT、WHERE、GROUP BY、ORDER BY、LIMIT
  - 参考 TianShu-Text2SQL-Agent 中 `sql_gen.py` 的拼接算法，但不复制代码，根据 contracts Protocol 重构
- **duckdb_executor/**：以只读模式（`access_mode=READ_ONLY`）连接 DuckDB，执行 SQL，返回 `duckdb.DuckDBPyRelation`
- **execution_tracer/**：记录编译和执行全过程的 Trace 信息（编译耗时、执行耗时、影响行数、物理计划概览）
- **result_summarizer/**：对执行结果进行摘要统计

## 明确不做什么

- 不涉及多 SubIntent 的合并或排序
- 不涉及 LLM 调用（SQL 编译是确定性的）
- 不涉及 Spark 或 PySpark
- 不涉及前端展示
- 不涉及 Repair Loop 或差异分析
- 不涉及数据写入（DuckDB 只读模式）

## 契约

- **输入契约**：`SubIntent` dataclass 必须包含 `select_columns`, `filter_conditions`, `group_by_columns`, `order_by_columns`, `limit`, `source_table` 等字段
- **输出契约**：`ExecutionTrace` 和 `ResultSummary` 按 `contracts/` 中定义的结构输出
- **表名/字段校验**：编译器必须校验所有引用的表和字段存在于 contracts 元数据中，不存在时抛出 `SchemaNotFoundError`

## 风险

| 风险 | 缓解措施 |
|------|----------|
| contracts 元数据与实际 DuckDB 表结构不一致 | 编译时做双重校验：contracts 元数据 + DuckDB `information_schema` |
| 复杂 JOIN 路径的确定性问题 | 先从单表场景开始，逐步扩展到 contracts 中预定义的 JOIN 路径 |
| DuckDB 版本兼容性 | 锁定 DuckDB >= 0.10.0，在 CI 中固定版本 |
| 拼接 SQL 的注入风险 | SubIntent 字段类型化（非原始字符串拼接），使用参数化查询 |

## 验收标准

1. [ ] 单表 SELECT + WHERE + LIMIT 场景编译、执行、摘要全流程通过
2. [ ] 支持 GROUP BY + HAVING + ORDER BY 场景
3. [ ] 支持 contracts 预定义的 2 表 JOIN 场景
4. [ ] 所有引用的表和字段经过 contracts 校验，不存在的表和字段抛出明确错误
5. [ ] DuckDB 执行器在只读模式下运行，写操作被拒绝
6. [ ] ExecutionTrace 包含编译耗时、执行耗时、行数
7. [ ] ResultSummary 包含行数、列名列表、空值率
8. [ ] 集成测试覆盖率 >= 80%（SubIntent -> ResultSummary）

## 测试边界

- **测试范围**：SubIntent 编译 -> SQL 执行 -> Trace + Summary 全流程
- **不测试**：多 SubIntent 协调、LLM 调用、前端展示、Repair Loop
- **隔离要求**：使用独立的 DuckDB 测试数据库，不依赖真实生产数据
- **异常测试**：表不存在、字段不存在、非法类型、空 SubIntent 等异常场景

## 与其他阶段的依赖

- **依赖 Phase 0**：依赖 `contracts/` 中定义的 SubIntent、ExecutionTrace、ResultSummary 等数据结构
- **被 Phase 3 依赖**：Phase 3 双引擎验证依赖本阶段的 SQL 执行器
- **与 Phase 2 并行**：Phase 2 Spark 多智能体阶段与本阶段可独立开发

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
