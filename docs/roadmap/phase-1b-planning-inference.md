# Phase 1B：RelationshipHypothesis + SqlBuildPlan

> 状态：待实施
> 前置依赖：Phase 1A 退出条件全部满足

## 执行前必须阅读

1. `AGENTS.md` §2 — Join 推理三层分工 + WEAK/NONE 硬门禁
2. `docs/01-target-architecture.md` §2.2 — RelationshipHypothesis 三层分工与证据等级硬门禁
3. `docs/01-target-architecture.md` §2.3 — 字段名归一化规则
4. `docs/01-target-architecture.md` §2.4 — 证据链路模板
5. `docs/01-target-architecture.md` §3.1 — SqlBuildPlan 最小 step 范围
6. `docs/03-sql-ir-and-compiler-plan.md` §3.2 — RelationshipHypothesis + SqlBuildPlan 完整类型定义
7. `docs/09-test-strategy.md` §7 Phase 1B

## 只允许修改

- `src/tianshu_datadev/planning/` — 新建模块
  - `relationship_hypothesis.py`：Join 候选推理 + 证据定级 Validator
  - `sql_build_plan.py`：SqlBuildPlan Planner + 8 step Schema
  - `evidence.py`：EvidenceItem / EvidenceLevel / EvidenceChain
- `src/tianshu_datadev/developer_spec/field_normalizer.py` — 补充常见别名字典
- `tests/` — 新增 test_relationship.py / test_sql_build_plan.py / test_evidence.py

## 禁止修改

- `src/tianshu_datadev/sql/` — 下一阶段
- `src/tianshu_datadev/spark/` — Phase 5 前不碰
- `src/tianshu_datadev/developer_spec/parser.py` — Phase 1A 已验证

## 新增模型（Pydantic `extra="forbid"`）

### RelationshipHypothesis

```python
class RelationshipHypothesis(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hypothesis_id: str
    spec_id: str
    source_manifest_hash: str
    join_candidates: list[JoinCandidate]

class JoinCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    candidate_id: str
    left_table: str
    right_table: str
    left_key: ColumnRef
    right_key: ColumnRef
    join_type: JoinType
    cardinality_hint: str | None       # "1:1" | "1:N" | "N:M" | None
    evidence: list[EvidenceItem]       # LLM 不填，由 Validator 确定性填入
    level: EvidenceLevel | None        # LLM 不填，由 Validator 确定性填入
    action: EvidenceAction | None      # LLM 不填，由 Validator 确定性填入

class EvidenceLevel(str, Enum):
    STRONG = "STRONG"
    MEDIUM = "MEDIUM"
    WEAK = "WEAK"
    NONE = "NONE"

class EvidenceAction(str, Enum):
    AUTO_ADOPT = "AUTO_ADOPT"          # STRONG → 自动采纳
    HUMAN_CONFIRM = "HUMAN_CONFIRM"    # MEDIUM → 采纳但进入 open_questions
    REJECT_BLOCKING = "REJECT_BLOCKING"  # WEAK → 拒绝，进入 open_questions(blocking=true)
    REJECT_SILENT = "REJECT_SILENT"    # NONE → 拒绝，仅记录 evidence_log

class EvidenceItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    evidence_type: str                 # field_name_match | field_name_similarity | type_compatibility
                                       # | foreign_key | unique_index | developer_declared | column_statistics
    result: str                        # MATCH | MISMATCH | FOUND | NOT_FOUND
    detail: str
```

### 证据等级判定规则（确定性 Validator）

| 等级 | 条件（AND） | 行为 |
|------|------------|------|
| `STRONG` | ① DeveloperSpec 显式声明 Join + 关联键 **或** SchemaRegistry 外键约束存在；② 字段名归一化后完全匹配；③ 双方类型兼容 | AUTO_ADOPT |
| `MEDIUM` | ① 字段名归一化后匹配 + 类型兼容；② 至少一方有唯一索引或快照采样显示高去重率；③ 但无显式声明且无外键约束 | HUMAN_CONFIRM |
| `WEAK` | ① 仅字段名相似（编辑距离 1-2 或常见别名匹配）；② 类型兼容；③ 无任何约束、索引或声明佐证 | REJECT_BLOCKING |
| `NONE` | 字段名不匹配、无约束、无声明——无任何证据 | REJECT_SILENT |

**硬门禁**：WEAK/NONE 在任何情况下不得进入 SqlBuildPlan 的 JoinSpec。Validator 未拦截视为 Bug。

### 字段名归一化规则

1. **大小写统一**：全部转为小写
2. **驼峰转下划线**：`userId` → `user_id`，`OrderID` → `order_id`
3. **常见别名字典**（版本化）：`cust_id` ↔ `customer_id`、`amt` ↔ `amount`、`dt` ↔ `date`、`qty` ↔ `quantity`、`desc` ↔ `description`
4. **去表名前缀**：可配置去除 `{table_alias}_` 前缀
5. **去特殊字符**：去除非字母数字，多下划线合并为一个

归一化后的字段名用于匹配，SqlBuildPlan 中保留原始字段名。

### 证据链模板

参见 `docs/01-target-architecture.md` §2.4。每个 Join 候选必须输出完整 YAML 格式证据链，包含：
- left/right 字段详情（raw、normalized、source、type、nullable）
- 逐条 evidence（evidence_type、result、detail）
- 最终 level 和 action

### SqlBuildPlan 8 Step Pydantic 骨架

```python
class SqlBuildPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: str
    spec_id: str
    hypothesis_id: str
    source_manifest_hash: str
    steps: list[StepNode]               # 至少一个 step

# StepNode = ScanStep | FilterStep | JoinStep | AggregateStep | ProjectStep | CaseWhenStep | SortStep | LimitStep
# 完整类型定义见 docs/03-sql-ir-and-compiler-plan.md §3.2.2

class Predicate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    left: ColumnRef | Predicate         # Predicate 支持嵌套（AND/OR/NOT）
    operator: str                       # EQ/NEQ/GT/GTE/LT/LTE/IN/NOT_IN/IS_NULL/IS_NOT_NULL/AND/OR/NOT/BETWEEN/LIKE
    right: ColumnRef | Literal | list[Literal] | None

class ColumnRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table_ref: str
    column_name: str                    # 原始字段名
    normalized_name: str                # 归一化字段名
```

## artifact schema

- `RelationshipHypothesis` JSON（含 join_candidates、evidence、level、action）
- `SqlBuildPlan` JSON（含 steps、完整 ColumnRef/Predicate AST）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| 证据等级判定 | 8 | STRONG 外键、STRONG 显式声明、MEDIUM 字段匹配+索引、WEAK 仅别名相似、NONE 无证据、类型不兼容拒绝、字段名归一化后匹配 |
| WEAK/NONE 硬门禁 | 4 | WEAK Join 被拦截、NONE Join 被拦截、STRONG 通过、MEDIUM 通过但标记 HUMAN_CONFIRM |
| 字段名归一化 | 5 | 驼峰转换、别名替换、前缀去除、大小写统一、复合场景 |
| SqlBuildPlan Schema | 5 | 合法 8 step、extra 字段拒绝、缺失必填字段、非法 step 类型、ScanStep 空 required_columns |
| 禁止字段 | 4 | raw_sql 被拒绝、where_sql 被拒绝、join_on: str 被拒绝、expression: str 被拒绝 |
| Fake Planner 确定性 | 1 | 相同 SourceManifest 两次生成相同 SqlBuildPlan |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "relationship or sql_build_plan or evidence or field_norm"
python -m ruff check src/tianshu_datadev/planning/
git diff --check
```

## B/C 暂停条件

- 证据等级判定规则无法覆盖某种常见 Join 场景（需新增证据类型）
- 字段名归一化字典与 TianShu 实际命名规范冲突
- SqlBuildPlan step 范围需要扩展（如子查询、UNION）→ 这些必须延后到后续 Phase

## 退出条件

1. 证据等级判定规则：STRONG/MEDIUM 自动采纳，WEAK/NONE 被 Validator 拦截
2. WEAK/NONE Join 在任何情况下不得进入 SqlBuildPlan 的 JoinSpec
3. 字段名归一化 5 条规则全部正确
4. 每个 Join 候选输出完整证据链（7 类证据逐条勾选）
5. SqlBuildPlan 8 step Schema——extra 字段拒绝
6. `raw_sql`、`where_sql`、`join_on: str`、`expression: str` 字段不存在或被拒绝
7. Fake Planner 确定性：相同 SourceManifest 两次生成相同 SqlBuildPlan
8. Phase 1A 测试保持通过

---

> Phase 1B | 待实施 | 前置：Phase 1A 退出
