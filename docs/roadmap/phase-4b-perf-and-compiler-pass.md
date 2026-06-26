# Phase 4B：PerfValidator + Compiler Pass

> 状态：待实施
> 前置依赖：Phase 4A 退出（真实 LLM 结构化输出可测量）

## 执行前必须阅读

1. `AGENTS.md` §2 — 性能门禁由确定性 PerfValidator 执行
2. `docs/03-sql-ir-and-compiler-plan.md` §6.1 — Compiler Pass 优化管道
3. `docs/03-sql-ir-and-compiler-plan.md` §6.2 — 为何 SQL 侧不设独立 LLM Performance Reviewer
4. Phase 1C 中 PerfContract 的初始实现（8 条规则 + 4 个 Compiler Pass）

## 只允许修改

- `src/tianshu_datadev/sql/` — 扩展
  - `perf_validator.py`：扩展至 15 条 PERF 规则
  - `compiler_passes.py`：验证 4 个 Compiler Pass 在真实 LLM 生成的 SqlBuildPlan 上幂等
  - `explain_feedback.py`：EXPLAIN 执行计划反馈解析
- `tests/` — 新增 test_perf_15_rules.py / test_compiler_pass_real.py

## 禁止修改

- LLM Gateway / Prompt 版本管理——Phase 4A 已验证
- SqlBuildPlan Schema——只消费，不修改
- 不得修改 Compiler Pass 的业务语义（只能优化执行效率）

## 新增模型

### 15 条 PERF 规则

| 编号 | 规则 | 处理 |
|------|------|------|
| PERF-001 | 默认禁止 `SELECT *`，必须只选择业务需要字段 | REJECT |
| PERF-002 | 查询大事实表时必须添加时间范围过滤 | REJECT / WARN |
| PERF-003 | 时间过滤使用 `>= start AND < end`，禁止 WHERE 左侧套函数 | REJECT |
| PERF-004 | 优先使用已给出的汇总表/DWS 表，不能默认扫描 fact 明细 | WARN |
| PERF-005 | 大表 Join 大表前必须先过滤、再按业务粒度聚合 | WARN / REJECT |
| PERF-006 | Join key 类型必须一致，禁止 Join 条件临时 CAST | REJECT |
| PERF-007 | Join key 必须有业务含义证据，维表 Join 前检查唯一性 | WARN |
| PERF-008 | 明细查询必须带 LIMIT，离线生成结果表除外 | REJECT |
| PERF-009 | 禁止无理由 `DISTINCT *` | REJECT |
| PERF-010 | 禁止无理由 `CROSS JOIN` | REJECT |
| PERF-011 | `ORDER BY` 只允许最终展示层或窗口必要位置 | WARN / REJECT |
| PERF-012 | 窗口函数前必须尽可能缩小数据范围 | WARN |
| PERF-013 | 高频指标建议沉淀为汇总表 | WARN |
| PERF-014 | 复杂 SQL 允许拆分 `_temp` 中间表验证中间行数 | WARN |
| PERF-015 | 慢 SQL 必须基于真实执行计划优化 | PERF_FEEDBACK |

### Compiler Pass 幂等验证

4 个 Pass（列裁剪、谓词规范化、无用排序消除、常量折叠）必须通过幂等测试：相同 SqlBuildPlan 经任意次数运行产生相同输出。

### EXPLAIN 反馈

```python
class ExplainFeedback(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_hash: str
    explain_output: str                # EXPLAIN / EXPLAIN ANALYZE 原始输出
    flagged_operations: list[str]      # 被标记的操作（全表扫描、笛卡尔积等）
    suggested_optimizations: list[str] # 建议的优化方向
```

## artifact schema

- 扩展后的 `PerfValidationResult` JSON（15 条规则的分流结果）
- `ExplainFeedback` JSON
- `OptimizedSQLPlan` JSON（含 compiler_pass_version）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| 15 条 PERF 规则 | 8 | REJECT 项阻断、WARN 项记录、PERF_FEEDBACK 输出、规则阈值边界 |
| Compiler Pass 幂等 | 4 | 列裁剪/谓词规范化/无用排序消除/常量折叠 各 2 次运行结果一致 |
| EXPLAIN 反馈 | 2 | 全表扫描被标记、优化建议正确生成 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "perf_15 or compiler_pass or explain"
python -m ruff check src/tianshu_datadev/sql/
git diff --check
```

## B/C 暂停条件

- PERF 规则的 REJECT/WARN 阈值与实际 TianShu 表规模冲突（误报率高）
- Compiler Pass 的优化行为在真实 LLM 生成的 SqlBuildPlan 上出现非幂等
- EXPLAIN 输出格式因 DuckDB 版本差异导致解析失败

## 退出条件（4B → 4C 门禁）

1. 15 条 PERF 规则全部实现
2. REJECT / WARN / PERF_FEEDBACK 分流正确
3. Compiler Pass 在真实 LLM 生成的 SqlBuildPlan 上幂等
4. 慢 SQL 可生成执行计划反馈
5. Phase 1A-4A 测试保持通过

---

> Phase 4B | 待实施 | 前置：Phase 4A 退出 | 下一阶段：Phase 4C
