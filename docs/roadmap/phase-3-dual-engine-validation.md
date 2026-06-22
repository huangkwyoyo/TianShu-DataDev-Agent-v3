# Phase 3：双引擎结果验证

## 目标

实现 SQL 和 Spark 两种引擎对同一份数据快照执行后的结果进行确定性交叉验证。核心思想：同源数据、分路执行、逐维比较。验证 SQL 编译器（Phase 1）和 Spark 代码生成器（Phase 2）的输出在语义上是否等价。

## 输入

- **SQL 执行结果**（来自 Phase 1）：DuckDB 执行 SQL 后的 `ExecutionTrace` + `ResultSummary`
- **Spark 执行结果**（来自 Phase 2）：PySpark 执行 DataFrame DSL 后的 `ExecutionTrace` + `ResultSummary`
- **同源 Parquet 快照**：从 TianShu 开发库抽样生成的 Parquet 文件 + schema.json + manifest.yml + SHA-256

## 输出

| 产出 | 说明 |
|------|------|
| **Snapshot Builder** | 从 TianShu 开发库抽取样本数据，转换为 Parquet 格式，附带 schema.json、manifest.yml 和 SHA-256 校验文件 |
| **CrossValidator** | 9 个确定性比较维度，逐一对比 SQL 和 Spark 的执行结果 |
| **ValidationReport** | 详细报告，包含各维度的比较结果、差异明细、通过/失败判定 |
| **Snapshot 仓库** | 按版本管理的快照集合，支持回滚和复现 |

## 模块职责

- **snapshot_builder/**：从 TianShu 开发库 SQL 表中按规则抽样，转为 Parquet 格式，输出：
  - `data.parquet`：列式存储的样本数据
  - `schema.json`：Parquet 文件的 Schema 定义
  - `manifest.yml`：快照元数据（抽样时间、来源表名、行数、列数、抽样策略）
  - `sha256.txt`：以上所有文件的 SHA-256 校验和
- **cross_validator/**：接收 SQL 结果和 Spark 结果，在 9 个维度进行确定性比较（参考 legacy 项目 `cross_validation.py` 的比较逻辑）：
  1. **行数一致性**：SQL 和 Spark 的输出行数是否相同
  2. **列名一致性**：列名集合和顺序是否相同
  3. **Schema 一致性**：每列的数据类型是否兼容
  4. **主键唯一性**：主键列的值集合是否一致
  5. **数值精度**：数值列的求和、均值是否在容差范围内
  6. **空值分布**：各列空值率是否一致
  7. **字符串集合**：字符串列的取值集合是否一致
  8. **排序一致性**：ORDER BY 场景下排序结果是否一致
  9. **哈希摘要**：全表内容的哈希摘要是否一致
- **validation_reporter/**：将 CrossValidator 的比较结果格式化为 ValidationReport，包含：
  - 各维度的通过/失败/跳过状态
  - 差异的详细数据（最多展示前 20 条差异行）
  - 总体判定（所有维度通过则 PASS，否则 FAIL）

## 明确不做什么

- 不生成 SQL 或 Spark 代码（由 Phase 1/2 负责）
- 不涉及 Repair Loop 或自动修复（在 Phase 4）
- 不涉及前端展示
- 不涉及生产数据的写入或修改
- 不覆盖所有数据源，仅限抽样快照

## 契约

- **快照契约**：Parquet 文件必须附带 schema.json、manifest.yml、SHA-256；manifest.yml 采用固定字段结构
- **验证输入契约**：SQL 和 Spark 的 `ExecutionTrace` + `ResultSummary` 必须遵循 `contracts/` 中定义的结构
- **比较容差**：数值比较的容差在 `CrossValidator` 初始化时指定，默认为 1e-6

## 风险

| 风险 | 缓解措施 |
|------|----------|
| SQL 和 Spark 的 NULL 处理语义不同 | 在 9 个比较维度中显式对比空值分布（维度 6） |
| 浮点数精度差异导致误判 | 数值比较使用相对容差（rtol）而非绝对相等 |
| 快照数据过大影响 CI 速度 | 快照行数控制在 1000 行以内，单独缓存 |
| 时区/日期格式不一致 | schema.json 明确时间戳的时区设定，比较时统一转换 |
| Parquet 版本兼容性 | 锁定 pyarrow >= 14.0，parquet 格式版本固定 |

## 验收标准

1. [ ] Snapshot Builder 能从指定 SQL 表抽取样本并生成完整的 Parquet + schema + manifest + SHA-256
2. [ ] 9 个比较维度全部实现，每个维度有独立的比较函数
3. [ ] 在 SQL 和 Spark 结果一致的场景下全部 9 个维度通过
4. [ ] 在故意制造差异的场景下，每个维度能正确检测并报告差异
5. [ ] ValidationReport 清晰区分 PASS/FAIL 及差异明细
6. [ ] SQL 和 Spark 读取同一份 Parquet 快照，确保"同源"约束
7. [ ] 快照的 SHA-256 校验在读取时自动验证

## 测试边界

- **测试范围**：Snapshot Builder 生成、CrossValidator 各维度比较、ValidationReport 格式
- **不测试**：SQL 编译正确性、Spark 代码生成质量、Repair Loop
- **隔离要求**：使用独立的小型测试数据集（CSV/Pandas DataFrame -> Parquet）
- **异常测试**：快照文件损坏、SHA 不匹配、Schema 不兼容、两个结果行数差异巨大

## 与其他阶段的依赖

- **依赖 Phase 1**：依赖 SQL 执行器输出 ExecutionTrace + ResultSummary
- **依赖 Phase 2**：依赖 SparkDeveloper 输出和 Spark 执行结果
- **被 Phase 4 依赖**：Phase 4 Repair Loop 依赖本阶段的 ValidationReport 来确定是否需要修复

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
