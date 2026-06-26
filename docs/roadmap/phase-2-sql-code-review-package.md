# Phase 2：SQL Code Review Package v1 + DataTransformContract-lite

> 状态：待实施
> 前置依赖：Phase 1C 退出条件全部满足

## 执行前必须阅读

1. `AGENTS.md` §8 — Data Contracts（DataTransformContract 三级递进）
2. `docs/01-target-architecture.md` §4 — DataTransformContract 三级递进
3. `docs/01-target-architecture.md` §8 — Code Review Package 目录结构
4. `docs/03-sql-ir-and-compiler-plan.md` §9 — DataTransformContract 抽取
5. `docs/09-test-strategy.md` §7 Phase 2

## 只允许修改

- `src/tianshu_datadev/artifacts/` — 新建模块
  - `packager.py`：从 SqlBuildPlan + CompilerOutput + ExecutionTrace + SourceManifest 组装 Code Review Package
  - `contract_extractor.py`：从已验证 SqlBuildPlan 确定性抽取 DataTransformContract-lite
  - `provenance.py`：provenance.yml 生成器
  - `review_md.py`：review.md 生成器
- `tests/` — 新增 test_packager.py / test_contract_extractor.py

## 禁止修改

- `src/tianshu_datadev/planning/` — Phase 1B 已验证
- `src/tianshu_datadev/sql/compiler.py` — Phase 1C 已验证
- `src/tianshu_datadev/spark/` — Phase 5 前不碰

## 新增模型

### Code Review Package 目录结构

```text
generated/review_packages/{request_id}/
├── developer_spec/
│   ├── original_spec.md               # 原始 DeveloperSpec 副本
│   └── parsed_spec.json               # ParsedDeveloperSpec
├── source_manifest/
│   ├── source_manifest.json           # SourceManifest
│   └── conflicts.json                 # SOURCE_CONFLICT 条目
├── plans/
│   ├── relationship_hypothesis.json   # RelationshipHypothesis + 证据链
│   └── sql_build_plan.json            # SqlBuildPlan
├── contracts/
│   └── data_transform_contract.json   # DataTransformContract-lite
├── sql/
│   ├── compiled.sql                   # 编译产物 SQL
│   └── optimized_plan.json            # OptimizedSQLPlan
├── traces/
│   ├── execution_trace.json           # ExecutionTrace
│   └── result_summary.json            # ResultSummary
├── lineage/
│   └── source_refs.yml
├── provenance.yml
└── review.md
```

### provenance.yml 字段

```yaml
request_id: str
spec_hash: str                          # DeveloperSpec SHA-256
parsed_spec_hash: str
source_manifest_hash: str
relationship_hypothesis_hash: str
sql_build_plan_hash: str
compiled_sql_sha256: str
optimized_plan_hash: str
data_transform_contract_hash: str
snapshot_manifest_hash: str
execution_trace_hash: str
model_id: str
prompt_version: str
compiler_version: str
validator_version: str
retry_count: int
timestamp: str
environment_fingerprint: str
```

### DataTransformContract-lite

从单个 SqlBuildPlan 确定性抽取：

```python
class DataTransformContractLite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contract_id: str
    level: str = "lite"                 # Phase 2 固定为 lite
    source_sqlbuildplan_hash: str
    input_tables: list[ContractInputTable]
    input_columns: list[ContractColumn]
    join_relationships: list[ContractJoin]
    filters: list[ContractPredicate]
    aggregations: list[ContractAggregation]
    grouping_keys: list[str]
    output_columns: list[ContractOutputColumn]
    output_grain: list[str]
    sort_spec: list[ContractSort] | None
    limit_spec: ContractLimit | None
    business_keys: list[str]
    semantic_policy_ref: str
```

抽取是确定性的——相同 SqlBuildPlan 产生相同 DataTransformContract 和相同哈希。

### 证据链模板（Join 证据链）

每个 Join 关系在审查包中附带完整证据链（参见 `docs/01-target-architecture.md` §2.4）：

```yaml
# contracts/data_transform_contract.json 中每个 join_relationship 包含：
join_relationships:
  - join_id: "join_001"
    left_table: "orders"
    right_table: "customers"
    left_key: "cust_id"
    right_key: "customer_id"
    join_type: "INNER"
    evidence_chain:
      left_field:
        raw: "cust_id"
        normalized: "customer_id"
        source: "developer_spec"
      right_field:
        raw: "customer_id"
        normalized: "customer_id"
        source: "schema_registry"
      evidence:
        - type: "field_name_match"
          result: "MATCH"
        - type: "type_compatibility"
          result: "MATCH"
        - type: "foreign_key"
          result: "FOUND"
      level: "STRONG"
      action: "AUTO_ADOPT"
```

## artifact schema

- Code Review Package 目录（含全部子 artifact）
- `provenance.yml`
- `review.md`（可被不熟悉系统的数据工程师读懂）
- `DataTransformContract-lite` JSON

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| Code Review Package | 4 | 目录结构完整、artifact hash 可复现、非法输入生成拒绝报告不生成不完整审查包 |
| DataTransformContract-lite | 4 | 从 SqlBuildPlan 确定性抽取、不包含 SQL 代码字段、不依赖 SqlProgram、hash 一致性 |
| provenance.yml | 2 | 所有版本/hash 记录完整、返工轮次记录 |
| review.md 可读性 | 1 | 生成内容不含代码实现细节 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "packager or contract_extractor or provenance or review"
python -m ruff check src/tianshu_datadev/artifacts/
git diff --check
```

## B/C 暂停条件

- review.md 可读性标准无法达成一致（需确定目标读者和可读性度量）
- DataTransformContract-lite 字段遗漏——发现 SqlBuildPlan 中有但 Contract 中无的关键信息
- provenance.yml 记录粒度争议——需权衡可追溯性与文件大小

## 退出条件

1. Code Review Package 目录结构完整，artifact hash 可复现
2. DataTransformContract-lite 从 SqlBuildPlan 确定性抽取（不依赖 Phase 3A SqlProgram）
3. provenance.yml 记录所有模型版本和输入 hash
4. review.md 可被不熟悉系统的数据工程师读懂
5. 非法输入生成拒绝报告，不生成不完整审查包
6. Phase 1A/1B/1C 测试保持通过

---

> Phase 2 | 待实施 | 前置：Phase 1C 退出
