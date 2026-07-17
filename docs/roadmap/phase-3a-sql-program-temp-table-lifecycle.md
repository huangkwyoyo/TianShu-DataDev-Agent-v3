# Phase 3A：SqlProgram + _temp 中间表生命周期

> 状态：**已完成 ✅**（2026-06-29 核销）
> 前置依赖：Phase 2 退出条件全部满足 ✅

## 执行前必须阅读

1. `AGENTS.md` §8 — SqlProgram 多语句合并使用 DAG 依赖和确定性拓扑排序
2. `docs/01-target-architecture.md` §3.2 — SqlProgram + _temp + DAG + 拓扑排序
3. `docs/01-target-architecture.md` §3.3 — 不实现 CTEPlan
4. `docs/01-target-architecture.md` §5 — SqlProgram DAG 依赖与拓扑排序
5. `docs/03-sql-ir-and-compiler-plan.md` §3.3 — SqlProgram + Compiler 完整定义
6. `docs/09-test-strategy.md` §7 Phase 3A

## 只允许修改

- `src/tianshu_datadev/planning/` — 扩展
  - `sql_program.py`：SqlProgram Builder + DAG 校验 + 拓扑排序
  - `temp_table.py`：TempTableSpec + _temp 生命周期管理
- `src/tianshu_datadev/sql/` — 扩展
  - `compiler.py`：扩展支持 SqlProgram 多语句编译
  - `executor.py`：扩展支持多语句执行 + _temp 清理
- `tests/` — 新增 test_sql_program.py / test_temp_table.py

## 禁止修改

- `src/tianshu_datadev/developer_spec/` — Phase 1A 已验证
- SqlBuildPlan 单语句 Schema——只消费，不修改
- `src/tianshu_datadev/spark/` — Phase 5 前不碰

## 新增模型

### SqlProgram

```python
class SqlProgram(BaseModel):
    model_config = ConfigDict(extra="forbid")
    program_id: str
    spec_id: str
    steps: list[SqlBuildPlan]           # 有序执行单元列表
    dag: dict[str, list[str]]           # step_id → 依赖的 step_id 列表
    temp_tables: list[TempTableSpec]    # _temp 中间表声明
    topological_order: list[str]        # 确定性拓扑排序结果（Kahn 算法 + 字典序打破平局）
    final_output: str | None            # 最终输出 step_id（None 表示以写入结束）

class TempTableSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    temp_id: str                        # 如 "_temp_aggregated_orders"
    produced_by: str                    # 生产者 step_id
    consumed_by: list[str]              # 消费者 step_id 列表
    schema: list[ColumnRef]
    cleanup_after: str = "program_end"
```

### _temp 中间表生命周期

```
CREATE:    producer step 执行时创建 _temp_* 表
READ:      consumer step(s) 读取 _temp_* 表
CLEANUP:   程序执行完毕后（成功或失败），所有 _temp_* 表在 cleanup 阶段 DROP
```

边界约束：
- _temp 表不得跨越 SqlProgram 边界——不同 SqlProgram 之间通过 DataTransformContract 传递规格
- _temp 表命名必须使用 `_temp_` 前缀
- cleanup 必须在程序结束时执行（无论成功或失败）——Executor 负责

### DAG 依赖与拓扑排序

- 每个 step 的 `depends_on` 只能引用同一 SqlProgram 内的其他 step_id
- 循环依赖被 Validator 拒绝（错误码 `CIRCULAR_DEPENDENCY`）
- 拓扑排序使用 Kahn 算法，同级节点按 step_id 字典序打破平局——确定性保证
- 缺失的依赖引用（引用了不存在的 step_id）被拒绝（错误码 `MISSING_DEPENDENCY`）

## artifact schema

- `SqlProgram` JSON（含 steps、dag、temp_tables、topological_order）
- 多语句 `CompilerOutput` JSON（每个 step 一条 SQL）
- 多语句 `ExecutionTrace` JSON（每个 step 的执行状态 + cleanup 状态）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| SqlProgram DAG | 4 | 两步聚合、多表串联、扇出扇入、合法 DAG 拓扑排序 |
| 循环依赖拒绝 | 2 | 直接循环 A→B→A、间接循环 A→B→C→A |
| _temp 生命周期 | 3 | 创建-使用-清理正常流程、执行失败后 cleanup 仍然执行、非 producer 读取 _temp 被拒绝 |
| 拓扑排序确定性 | 1 | 相同 DAG 两次拓扑排序结果一致 |
| 多语句执行 | 2 | 全成功、中间失败阻断后续+cleanup |
| 缺失依赖拒绝 | 1 | 引用不存在的 step_id |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "sql_program or temp_table or dag or topological"
python -m ruff check src/tianshu_datadev/planning/sql_program.py
git diff --check
```

## B/C 暂停条件

- DAG 依赖模型无法表达实际业务中的某种合法多步模式
- _temp 表命名规范与实际 SQL 环境的表命名限制冲突
- 拓扑排序的字典序打破平局策略在某种情况下产生非确定性

## 退出条件（核销结果）

| # | 条件 | 状态 | 核销依据 |
|---|------|------|---------|
| 1 | SqlProgram 多语句 DAG 依赖正确——两步聚合、多表串联、扇出扇入 | ✅ | `sql_program.py`：SqlProgram/Builder/DAG 校验/Kahn 拓扑排序；`test_sql_program.py` 28 测试 |
| 2 | 循环依赖被拒绝 | ✅ | `validate_program_dag()` 拒绝 CIRCULAR_DEPENDENCY；`test_topological.py` 含循环拒绝测试 |
| 3 | _temp 中间表生命周期：创建、使用、清理（失败时也清理） | ✅ | `temp_table.py`：TempTableSpec + 命名/引用校验；Executor 失败路径 cleanup |
| 4 | 拓扑排序确定性 | ✅ | Kahn 算法 + step_id 字典序打破平局；`topological_sort()` 确定性保证 |
| 5 | 多语句 Executor：失败语句阻断后续，cleanup 正确执行 | ✅ | `execute_program()` 含 failure cleanup；`compile_program()` 多语句编译 |
| 6 | Phase 1A-2 测试保持通过 | ✅ | 全量 1105 测试通过（49 个 Phase 3A 相关） |

### 代码文件清单

| 文件 | 行数 | 说明 |
|------|------|------|
| `planning/sql_program.py` | ~350 | SqlProgram/SqlStatement/StatementKind/Builder/DAG 校验/拓扑排序 |
| `planning/temp_table.py` | ~190 | TempTableSpec/命名校验/引用校验/消费者授权 |
| `sql/compiler.py` | ~850 | compile_program() 多语句编译 |
| `sql/executor.py` | ~250 | execute_program() + _temp cleanup |

### 测试覆盖

- `tests/planning/test_sql_program.py` — 24 测试（DAG/循环/拓扑/缺失依赖）
- `tests/planning/test_temp_table.py` — 4 测试（生命周期/cleanup/引用校验）
- `tests/sql/test_pipeline_e2e.py` — 4 测试（多语句执行）

---

> Phase 3A | **已完成** | 49 测试通过 | 全量 1105 测试通过
