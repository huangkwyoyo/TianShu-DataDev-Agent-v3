# Phase 3B：CaseWhenStep + WindowExpr（标签与窗口函数）

> 状态：待实施
> 前置依赖：Phase 3A 退出条件全部满足

## 执行前必须阅读

1. `docs/03-sql-ir-and-compiler-plan.md` §3.1 — SqlBuildPlan CaseWhenStep 定义
2. `docs/03-sql-ir-and-compiler-plan.md` §3.5 — WindowExpr / WindowSpec 完整类型定义
3. `docs/09-test-strategy.md` §7 Phase 3B

## 只允许修改

- `src/tianshu_datadev/planning/` — 扩展
  - SqlBuildPlan step 类型：CaseWhenStep、WindowStep（或 WindowExpr 嵌入现有 step）
- `src/tianshu_datadev/sql/` — 扩展
  - `compiler.py`：支持 CASE WHEN 渲染、窗口函数 OVER 子句渲染
  - `validator.py`：CASE 标签枚举检查、窗口函数白名单检查、窗口位置检查
- `tests/` — 新增 test_case_when.py / test_window.py

## 禁止修改

- SqlBuildPlan 已有的 6 个 step（Scan/Filter/Join/Aggregate/Project/Sort/Limit）——只扩展，不修改
- `src/tianshu_datadev/spark/` — Phase 5 前不碰

## 新增模型

### CaseWhenStep

```python
class CaseWhenStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step_type: str = "case_when"
    cases: list[WhenBranch]
    else_value: Literal | None
    alias: str

class WhenBranch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    condition: Predicate
    result: Literal
```

**标签枚举覆盖检查**：所有 CASE WHEN 结果值必须在 DeveloperSpec 声明的枚举值列表中。未声明枚举值被 Validator 拒绝。

### WindowExpr（8 种白名单函数）

```python
class WindowFunction(str, Enum):
    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    LAG = "LAG"
    LEAD = "LEAD"
    SUM_OVER = "SUM_OVER"
    AVG_OVER = "AVG_OVER"
    COUNT_OVER = "COUNT_OVER"

class WindowExpr(BaseModel):
    model_config = ConfigDict(extra="forbid")
    function: WindowFunction
    input: ColumnRef | Literal | None
    partition_by: list[ColumnRef]
    order_by: list[SortSpec]
    frame: WindowFrame | None
    alias: str

class WindowFrame(BaseModel):
    model_config = ConfigDict(extra="forbid")
    frame_type: str                     # ROWS | RANGE
    start: FrameBoundary
    end: FrameBoundary

class FrameBoundary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str                           # CURRENT_ROW | UNBOUNDED_PRECEDING | UNBOUNDED_FOLLOWING
                                        # | N_PRECEDING | N_FOLLOWING
    offset: int | None
```

**禁止事项**：
- 禁止任意窗口函数名（不在白名单 8 种的拒绝）
- 禁止嵌套窗口函数
- 禁止窗口函数出现在 WHERE 子句
- 禁止窗口函数内自由表达式
- 禁止窗口函数与子查询组合

## artifact schema

- 扩展后的 `SqlBuildPlan` JSON（含 CaseWhenStep / WindowExpr）
- 扩展后的 `CompilerOutput` JSON（含 CASE WHEN / OVER 渲染的 SQL）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| CaseWhenStep | 4 | 合法 CASE WHEN、枚举值在 DeveloperSpec 声明中通过、未声明枚举值拒绝、else_value 合法 |
| 窗口函数白名单 | 5 | 8 种函数各至少 1 个通过、非法函数名拒绝 |
| 窗口函数拒绝路径 | 4 | 嵌套窗口拒绝、WHERE 中窗口拒绝、自由表达式拒绝、非法 FrameBoundary 拒绝 |
| WindowFrame | 2 | 合法 ROWS frame、合法 RANGE frame |
| Compiler 确定性 | 1 | 相同 SqlBuildPlan（含 CASE/Window）两次编译相同 SQL 和 hash |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "case_when or window or frame"
python -m ruff check src/tianshu_datadev/planning/ src/tianshu_datadev/sql/
git diff --check
```

## B/C 暂停条件

- 窗口函数白名单需要扩展（新增函数超出 8 种）——必须按成套规则交付
- CASE WHEN 标签枚举检查的边界（如动态标签、聚合标签）需要明确处理策略
- WindowFrame RANGE vs ROWS 语义差异在 DuckDB 中的行为需要验证

## 退出条件

1. CaseWhenStep 标签枚举覆盖检查——枚举值不在 DeveloperSpec 声明中被拒绝
2. WindowExpr 白名单 8 种函数通过，非法函数被拒绝
3. WindowFrame 非法参数被拒绝
4. 窗口函数嵌套被拒绝
5. 窗口函数非法位置（WHERE 子句）被拒绝
6. 相同 SqlBuildPlan（含 CASE/Window）两次编译 SQL 和 hash 一致
7. Phase 1A-3A 测试保持通过

---

> Phase 3B | 待实施 | 前置：Phase 3A 退出
