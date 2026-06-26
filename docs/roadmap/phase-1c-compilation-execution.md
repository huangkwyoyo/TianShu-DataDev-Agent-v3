# Phase 1C：Validator + Compiler + DuckDB Executor

> 状态：待实施
> 前置依赖：Phase 1B 退出条件全部满足

## 执行前必须阅读

1. `AGENTS.md` §2 — 性能门禁由确定性 PerfValidator 执行
2. `docs/03-sql-ir-and-compiler-plan.md` §6 — SQL Compiler 完整编译流程
3. `docs/03-sql-ir-and-compiler-plan.md` §6.1 — Compiler Pass 优化管道
4. `docs/03-sql-ir-and-compiler-plan.md` §6.2 — 为何 SQL 侧不设独立 LLM Performance Reviewer
5. `docs/01-target-architecture.md` §8 — Code Review Package 目录结构（Compiler 产物部分）
6. `docs/09-test-strategy.md` §7 Phase 1C

## 只允许修改

- `src/tianshu_datadev/sql/` — 新建模块
  - `validator.py`：SQL Validator（事实源校验 + Join 证据门禁 + 语义校验）
  - `compiler.py`：确定性 Compiler（输入 SqlBuildPlan → 输出 DuckDB SQL + OptimizedSQLPlan）
  - `perf_validator.py`：PerfValidator（REJECT 阻断 / WARN 记录）
  - `compiler_passes.py`：4 个 Compiler Pass（列裁剪、谓词规范化、无用排序消除、常量折叠）
  - `executor.py`：DuckDB Executor（隔离执行 + ExecutionTrace + ResultSummary）
- `tests/` — 新增 test_validator.py / test_compiler.py / test_perf.py / test_executor.py

## 禁止修改

- `src/tianshu_datadev/planning/` — Phase 1B 已验证
- `src/tianshu_datadev/spark/` — Phase 5 前不碰
- SqlBuildPlan Schema 结构——只消费，不修改

## 新增模型

### Compiler 输入输出

**输入**：`SqlBuildPlan`（已验证，所有 table_ref/column_ref/relationship_ref 已绑定）

**输出**：
```python
class CompilerOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sql: str                            # 编译产物 DuckDB SQL
    sql_sha256: str
    optimized_plan: OptimizedSQLPlan
    compiler_version: str
    input_plan_hash: str

class OptimizedSQLPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input_plan_hash: str
    output_plan_hash: str
    applied_passes: list[CompilerPassRecord]
    rejected_directives: list[str]      # 未应用的优化指令及理由
    column_pruning_removed: list[str]   # 被裁剪的列
    predicate_normalizations: list[PredicateNormRecord]
    eliminated_sorts: list[str]         # 被消除的无用排序
    constant_folds: list[ConstantFoldRecord]

class CompilerPassRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pass_name: str                      # column_pruning | predicate_normalization | sort_elimination | constant_folding
    pass_version: str
    applied: bool
    changes_count: int
    input_ast_snippet: str
    output_ast_snippet: str
```

### PerfContract 规则注册表（8 条 PERF 规则）

| 规则 ID | 规则 | 级别 | 条件 |
|---------|------|------|------|
| `PERF-001` | 无 LIMIT 的全量扫描且估算行数 > 10M | REJECT | 阻断编译 |
| `PERF-002` | Join 键双方类型不一致（如 int ↔ varchar） | REJECT | 阻断编译 |
| `PERF-003` | 窗口函数违反白名单（Phase 3B 生效，Phase 1C no-op） | REJECT | 当前 no-op |
| `PERF-004` | 分区过滤键未在 WHERE 中出现且表为分区表 | REJECT | 阻断编译 |
| `PERF-005` | 无 LIMIT 的排序且 `estimated_input_rows` > 1M | WARN | 记录不阻断 |
| `PERF-006` | 聚合前行数 > 10M 且 group_keys > 5 | WARN | 记录不阻断 |
| `PERF-007` | `SELECT *`（required_columns 为空或等于全表列） | WARN | 记录不阻断 |
| `PERF-008` | Join 前可预聚合但 `pre_aggregation_allowed=False` | WARN | 记录不阻断 |

### 4 个 Compiler Pass

1. **列裁剪**：移除 ScanStep 中未被后续 step 引用的 `required_columns`
2. **谓词规范化**：`BETWEEN` → `>= AND <`；`DATE() =` → 范围表达式；`strftime` → 等价范围；移除恒真/恒假
3. **无用排序消除**：无 LIMIT 且输出不依赖顺序的 SortStep → 移除
4. **常量折叠**：`1+2` → `3`；`TRUE AND x` → `x`；重复 `IS NOT NULL` → 合并

每个 Pass 必须是幂等的——重复运行不改变结果。

## 旧代码迁移策略

- `src/tianshu_datadev/ir/protocols.py`：标记 `# DEPRECATED: Phase 0.5`，保留文件但所有 Protocol 类加 `deprecated` 注释
- 如果 Phase 1C 退出时旧 Protocol 已无任何引用：删除文件
- 如果仍有测试引用旧 Protocol：迁移测试到新 Pydantic 模型，然后删除

## artifact schema

- `CompilerOutput` JSON（含 sql、sql_sha256、optimized_plan）
- `OptimizedSQLPlan` JSON（含 applied_passes、rejected_directives）
- `ExecutionTrace` JSON（含执行状态、耗时、行数、错误信息）
- `ResultSummary` JSON（含列摘要、NULL 计数、数值汇总）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| SQL Validator | 6 | 未声明表/字段拒绝、Join key 类型不一致拒绝、时间字段无过滤拒绝、WEAK Join 门禁、合法输入通过、空 steps 拒绝 |
| Compiler 确定性 | 2 | 相同 SqlBuildPlan 两次编译相同 SQL 和 SHA-256 |
| PerfValidator REJECT | 3 | PERF-001/002/004 阻断编译 |
| PerfValidator WARN | 3 | PERF-005/007/008 记录不阻断 |
| PERF-003 no-op | 1 | 窗口函数规则注册但 no-op |
| Compiler Pass 幂等 | 4 | 列裁剪、谓词规范化、无用排序消除、常量折叠各 1 个 |
| OptimizedSQLPlan | 2 | 优化链正确记录、rejected_directives 正确记录 |
| DuckDB Executor | 3 | 合法 SQL 执行成功、非法 SQL 报 ExecutionTrace（非崩溃）、ResultSummary 正确 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "validator or compiler or perf or executor or optimizer"
python -m ruff check src/tianshu_datadev/sql/
git diff --check
```

## B/C 暂停条件

- PerfContract 规则阈值（如 10M 行）需要基于实际 TianShu 表规模校准
- Compiler Pass 的优化行为与 DuckDB 自身优化器冲突（如常量折叠导致执行计划退化）
- 发现 SqlBuildPlan 无法表达的合法 SQL 模式——需判断是否扩展 step 类型还是返回 UNSUPPORTED

## 退出条件

1. SQL Validator 正确拒绝非法表/字段/Join key 类型/时间字段无过滤
2. Compiler 确定性：相同 SqlBuildPlan 两次编译产生相同 SQL 和 SHA-256
3. PerfContract REJECT 规则（PERF-001/002/004）违反后阻断
4. PerfContract WARN 规则违反后记录不阻断
5. 4 个 Compiler Pass 全部幂等
6. OptimizedSQLPlan 正确记录优化链和 rejected_directives
7. DuckDB Executor 正确执行 Compiler 产物并输出 ExecutionTrace + ResultSummary
8. Phase 1A/1B 测试保持通过
9. 旧 Protocol 迁移策略已执行

---

> Phase 1C | 待实施 | 前置：Phase 1B 退出
