# Phase 4.6：复杂 SQL 模式渐进开放（多跳 Join + 子查询）

> 状态：**规划就绪**
> 前置依赖：Phase 3 Exit 满足全部退出条件 ✅ | Phase 4.5（内部交互验证口）✅
> 关联文档：[[subquery-multihop-join-boundary_20260629_1500]] | [[03-sql-ir-and-compiler-plan]] §7.3 | [[AGENTS.md]] §2

---

## Step 1（高优先级）— 多跳 Join（预估 ~150 行）

### 执行前必须阅读

1. `AGENTS.md` §2 — SQL Generation Boundary（Join 推理三层分工 + WEAK/NONE 硬门禁）
2. `docs/03-sql-ir-and-compiler-plan.md` §3.1 — SqlBuildPlan step 类型定义
3. `docs/roadmap/subquery-multihop-join-boundary_20260629_1500.md` §3、§5、§6
4. `docs/09-test-strategy.md` §7

### 只允许修改

- `src/tianshu_datadev/sql/validator.py` — 新增 `MULTI_HOP_JOIN_CHECK` 规则：每条 JoinStep 的 evidence_level ≥ MEDIUM；右表引用无重复；跳数 ≤ 5
- `tests/` — 新增测试文件 + golden/reject fixture

### 禁止修改

- `src/tianshu_datadev/planning/sql_build_plan.py` — 无 Schema 改动，现有 JoinStep 已支持多表串联
- `src/tianshu_datadev/sql/compiler.py` — 现有多步 SqlProgram 编译已支持
- `src/tianshu_datadev/artifacts/` — Contract 层不变
- `src/tianshu_datadev/developer_spec/` — Parser 层不变

### 新增模型

无 Schema 层面的新增——Step 1 仅在 Validator 层新增一条硬规则。

#### Validator 规则：MULTI_HOP_JOIN_CHECK (V-009)

| 子规则 | 检查内容 | 拒绝行为 |
|--------|---------|---------|
| V-009a | 每步 JoinStep 的 `relationship_ref` 独立定级 ≥ MEDIUM | 任意 WEAK/NONE → `PLAN_REJECTED` + `JOIN_EVIDENCE_TOO_WEAK` |
| V-009b | 右表引用链无循环（表 A → B → A） | 检测循环 → `AMBIGUOUS_MULTI_HOP` |
| V-009c | 多跳深度 ≤ 5（连续 JoinStep 数量） | 超过 5 → `MULTI_HOP_DEPTH_EXCEEDED` |
| V-009d | 同一 SqlBuildPlan 内仅允许一张 JoinStep（多跳拆入多步 SqlProgram） | 单步内多 JoinStep → `MULTI_HOP_PER_STEP_EXCEEDED` |

### Safety Validation 新增

`WriteValidator` 增加 _temp 表名冲突检查和中间步骤数上限（≤ 5 步），已有 WV-006 覆盖 _temp 操作白名单。

### 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| 多跳 Join 黄金路径 | 1 | 三表关联（u → o → p），evidence 全部 ≥ MEDIUM |
| 菱形 Join 拒绝 | 1 | 左表同时 JOIN 右表 1 和右表 2，Validator 拒绝 |
| 超过 5 跳拒绝 | 1 | 6 连续 JoinStep → MULTI_HOP_DEPTH_EXCEEDED |
| 单步内多 JoinStep 拒绝 | 1 | 同一 SqlBuildPlan 含 2 个 JoinStep → MULTI_HOP_PER_STEP_EXCEEDED |
| 确定性 hash | 1 | 相同 DAG 两次执行相同拓扑顺序 + 相同 SQL |

### 验收命令

```bash
python -m pytest tests/ -q -k "multihop or multi_hop"
python -m ruff check src/tianshu_datadev/sql/
git diff --check
```

---

## Step 2（中优先级）— 子查询（预估 ~360 行）

### 执行前必须阅读

1. Step 1 全部阅读材料
2. `docs/03-sql-ir-and-compiler-plan.md` §3.4 — 子查询边界声明
3. `docs/roadmap/subquery-multihop-join-boundary_20260629_1500.md` §4、§5、§6

### 只允许修改

- `src/tianshu_datadev/planning/sql_build_plan.py` — 新增 `SubqueryStep(StrictModel)` 类
- `src/tianshu_datadev/planning/models.py` — 可选：若子查询声明需要新模型
- `src/tianshu_datadev/sql/validator.py` — 新增 4 条子查询专用规则
- `src/tianshu_datadev/sql/compiler.py` — 新增 `_render_subquery_step()` 递归方法
- `src/tianshu_datadev/planning/sql_program.py` — 可选扩展 SqlProgram 支持子查询引用
- `tests/` — 新增测试文件 + golden/reject fixture

### 禁止修改

- `src/tianshu_datadev/developer_spec/` — Parser 层不变（子查询在 Planner 层组合）
- `src/tianshu_datadev/artifacts/` — Contract 层不变（v1 字段已含子查询位置）
- `src/tianshu_datadev/spark/` — Phase 5 前不碰

### 新增模型

```python
class SubqueryStep(StrictModel):
    """子查询步骤——在 FROM 子句中嵌入完整的 SqlBuildPlan。

    仅支持 FROM 子句中的派生表子查询，不支持：
    - WHERE 中的关联子查询
    - SELECT 列表中的标量子查询
    - 超过 2 层嵌套

    递归引用 via annotations（`from __future__ import annotations` + ForwardRef）。
    """

    step_type: str = "subquery"
    alias: str  # 派生表别名（如 `order_agg`）
    inner_plan: SqlBuildPlan  # 嵌套的完整 SqlBuildPlan
    depth: int = 1  # 嵌套深度（从 1 开始计数，Validator 限制 ≤ 2）
```

#### Validator 规则

| 规则 ID | 检查内容 | 拒绝行为 |
|---------|---------|---------|
| V-010a SUBQUERY_DEPTH_CHECK | 嵌套层数 ≤ 2 | 超 2 → `SUBQUERY_NESTING_TOO_DEEP` |
| V-010b SUBQUERY_COLUMN_LEAK_CHECK | 内层输出列必须全部被外层引用或显式丢弃 | 未引用列 → `SUBQUERY_COLUMN_LEAK` + WARN |
| V-010c SUBQUERY_FACT_SOURCE_CHECK | 递归校验内层 SqlBuildPlan 的事实源一致性 | 不一致 → `SOURCE_CONFLICT` |
| V-010d SUBQUERY_WINDOW_FORBIDDEN | 内层不含 WindowStep（Phase 3B 规则继承） | Window 在内层 → `SUBQUERY_WINDOW_FORBIDDEN` |
| V-010e SUBQUERY_JOIN_FORBIDDEN | 内层仅单表（复杂关联应拆为多步 SqlProgram） | 内层含 JoinStep → `SUBQUERY_JOIN_FORBIDDEN` |

### 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| FROM 子查询黄金路径 | 1 | 派生表 JOIN 维表，全部通过 |
| 嵌套超 2 层拒绝 | 1 | 3 层嵌套 → SUBQUERY_NESTING_TOO_DEEP |
| 子查询内窗口拒绝 | 1 | WindowStep 嵌套在 SubqueryStep 内部 |
| 子查询内 Join 拒绝 | 1 | JoinStep 出现在子查询内层 |
| 确定性 hash | 1 | 相同子查询两次编译相同 SQL + hash |
| 空子查询拒绝 | 1 | 无内层 plan → ValueError |

### 验收命令

```bash
python -m pytest tests/ -q -k "subquery or derived_table"
python -m ruff check src/tianshu_datadev/planning/ src/tianshu_datadev/sql/
git diff --check
```

---

## 全局约束（两步骤共享）

### 架构约束（6 条硬约束，来自 AGENTS.md 和产品宪章）

| 约束 | 来源 | 影响 |
|------|------|------|
| 禁止自由 SQL 片段 | AGENTS.md:18 | 子查询必须通过嵌套 SqlBuildPlan 表达，不使用 `subquery_sql: str` 逃生口 |
| 禁止 CTE 嵌套 | AGENTS.md:116 | 子查询编译时不得生成 `WITH ... AS (...)`——使用 `(...)` 或 _temp 物化 |
| 禁止 WEAK/NONE Join 进入计划 | AGENTS.md:23 | 多跳 Join 每步 evidence_level ≥ MEDIUM——一个 WEAK 阻断整链 |
| LLM 不决定验证通过 | AGENTS.md §5 | 子查询深度/Join 跳数上限由 Validator 硬编码检查 |
| SQL 修复只能生成新 SqlBuildPlan | AGENTS.md:20 | 子查询或 Join 链有问题的返工入口是重新生成 SqlBuildPlan |
| 窗口+子查询组合仍禁止 | Phase 3B 退出条件 | 子查询开放后 WindowStep 仍不得嵌套在 SubqueryStep 内部 |

### B/C 暂停条件

| 条件 | 触发时行为 |
|------|-----------|
| 多跳 Join 证据链互相依赖（非独立定级） | 暂停，讨论 Planner 证据层设计 |
| 子查询物化策略与 _temp 方案冲突 | 暂停，评估物化 vs 内联的性能与安全差异 |
| 任一测试在 DuckDB 上行为与预期不符 | 暂停，验证 DuckDB 子查询语法限制 |
| 发现 7 项交付规则未覆盖的合法场景边界 | 暂停，补充缺失规则 |

### 退出条件（Step 1 + Step 2 全部满足）

1. ✅ **多跳 3 表 Join 端到端通过** — 三表关联（u→o→p）通过 Parser → Planner → Validator → Compiler → Executor
2. ✅ **菱形 Join、超过 5 跳、单步内多 JoinStep 被 Validator 拒绝**
3. ✅ **WEAK/NONE Join 多跳阻断** — 任意一步证据不足阻断整条链路
4. ✅ **FROM 子查询端到端通过** — 派生表子查询通过完整链路
5. ✅ **嵌套超 2 层子查询被拒绝**
6. ✅ **子查询内窗口函数被拒绝**
7. ✅ **相同 SqlBuildPlan 两次编译一致（含子查询/多跳场景）**
8. ✅ **Phase 1A-4.5 测试保持通过（无回归）**

---

> Phase 4.6 | 规划就绪 | Step 1 前置：Phase 3 Exit + Phase 4.5 | Step 2 前置：Step 1 退出
