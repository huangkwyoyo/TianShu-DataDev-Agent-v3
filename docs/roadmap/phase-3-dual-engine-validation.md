# Phase 3：关系一致快照与双引擎验证

## 目标

构建同源、不可变、可复现的关系快照，让DuckDB与Spark读取同一批数据，并用确定性Normalizer和Comparator产生精确一致性状态。

## 输入

- TransformationContract和Fact Catalog引用。
- SQL/Spark artifacts。
- 开发库只读连接或版本化fixture。
- SemanticCompatibilityPolicy与EnvironmentManifest。

## 交付物

1. 锚点键驱动的Relational Snapshot Builder。
2. Parquet、Schema、关系键、抽取计划和SHA-256 manifest。
3. DuckDB/Spark Snapshot Adapters。
4. ResultNormalizer和规范化类型系统。
5. DeterministicComparator与ComparisonReport。
6. MergePlan Validator和多SubIntent受控合并基础。

## 比较状态

- `NOT_EXECUTED`
- `RUNTIME_PASS`
- `DIFFERENT`
- `UNSUPPORTED_SEMANTICS`
- `CONSISTENT_SAMPLE`
- `REVIEW_READY`
- `HUMAN_REVIEW`

本阶段不使用泛化`PASS`。

## 禁止

- 每张表独立LIMIT抽样。
- 一个分支重新生成快照。
- LLM参与结果判定。
- 把样本一致称为业务正确或生产性能通过。
- 自动修复差异。

## 验收

1. 多表快照保留声明的键关系和重复分布。
2. 两个引擎校验同一snapshot_id和hash。
3. NULL、NaN、Decimal、时间、字符串、重复行和Join基数有明确策略。
4. 任一引擎未执行时不能产生`CONSISTENT_SAMPLE`。
5. 故意注入的过滤、Join、聚合和NULL差异可被定位。
6. 累计pytest目标70-90个。

## 下一阶段依赖

Phase 4只读取结构化ComparisonReport、trace摘要和artifact引用进行差异诊断与路由。

---

> Phase 0.5 校正 | 2026-06-22
