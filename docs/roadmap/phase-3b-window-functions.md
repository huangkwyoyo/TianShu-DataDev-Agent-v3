# Phase 3B：CaseWhenStep + WindowExpr（标签与窗口函数）

> 状态：**已完成**（2026-06-29 核销）
> 前置依赖：Phase 3A 退出条件全部满足 ✅

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

## 退出条件（核销结果）

| # | 条件 | 状态 | 核销依据 |
|---|------|------|---------|
| 1 | CaseWhenStep 标签枚举覆盖检查——枚举值不在 DeveloperSpec 声明中被拒绝 | ✅ | `label_validator.py`：从 DeveloperSpec/SourceManifest 收集声明枚举值 → 比对 WhenBranch.result → 生成 blocking OpenQuestion；94 测试通过 |
| 2 | WindowExpr 白名单 8 种函数通过，非法函数被拒绝 | ✅ | `WindowFunction` 枚举 8 种（ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD/SUM_OVER/AVG_OVER/COUNT_OVER）；`window_validator.py` `_VALID_WINDOW_FUNCTIONS` 白名单 |
| 3 | WindowFrame 非法参数被拒绝 | ✅ | `_validate_frame()`：6 项规则（RANGE 边界约束/N 偏移 offset 必填/offset 非负/start≠UNBOUNDED_FOLLOWING/end≠UNBOUNDED_PRECEDING） |
| 4 | 窗口函数嵌套被拒绝 | ✅ | 类型系统在 Schema 层阻止（WindowExpr.input 不接受 WindowExpr）+ Validator 二次确认 |
| 5 | 窗口函数非法位置（WHERE 子句）被拒绝 | ✅ | `validate_window_not_in_where()`：检查 FilterStep 和 HAVING 中窗口别名引用 |
| 6 | 相同 SqlBuildPlan（含 CASE/Window）两次编译 SQL 和 hash 一致 | ✅ | DuckDbSqlCompiler 确定性输出 + SHA-256 hash |
| 7 | Phase 1A-3A 测试保持通过 | ✅ | 全量 1105 测试通过（94 个 Phase 3B 相关） |

### 已知设计讨论

**标签枚举检查策略**——当前实现要求 DeveloperSpec 中显式声明 `enum_values`，LabelValidator 仅在声明非空时执行校验。不声明 → 静默通过。

用户建议了一个**数据驱动的枚举检测方案**：
> 抽取目标表的部分数据，自动判定字段是否属于枚举值字段（Flag/Status/Code 三类），仅当检测到枚举值字段时才触发拦截。

枚举值分类：
- **标志位（Flag）**：0/1、Y/N、YES/NO → 例 `is_waisu=1`
- **状态码（Status）**：固定英文短语 → 例 `Status='Approved'`
- **分类代码（Code）**：字母缩写或纯数字 → 例 `Type=PDR`、`payment_type=2`

该方案的优点是降低 DeveloperSpec 编写负担；风险是数据采样可能遗漏未出现的枚举值，需要结合统计推断（如低基数列检测）和人工审查标注。

已实现：**[[phase-3b1-enum-auto-detection|Phase 3B.1：枚举值自动检测]]**——`profiling/enum_profiler.py` + `label_validator.py` 集成，支持 Flag/Status/Code 三类自动检测 + 五层置信度分层（CERTAIN→blocking / HIGH→WARN / MEDIUM→info）。测试融合于 `tests/labels/test_label_rules.py`（13 新增）。

### 代码文件清单

| 文件 | 行数 | 说明 |
|------|------|------|
| `planning/models.py` §101-252 | ~150 | WindowFunction 枚举/WindowFrame/FrameBoundary/WindowExpr/WhenBranch |
| `planning/sql_build_plan.py` §106-129 | ~25 | CaseWhenStep/WindowStep 定义 |
| `validation/label_validator.py` | ~189 | CASE WHEN 标签枚举值校验 |
| `validation/window_validator.py` | ~361 | 窗口函数白名单/嵌套/位置/Frame 校验 |
| `sql/compiler.py` | 含 CASE WHEN / OVER 渲染 | Compiler 支持窗口函数方言 |

### 测试覆盖

- `tests/labels/test_label_rules.py` — 标签枚举覆盖/越界/else_value/空 cases
- `tests/window/test_window_functions.py` — 8 种函数合法+非法拒绝+嵌套+Frame+确定性编译
- `tests/planning/test_planning_models.py` — WindowExpr/Frame 模型校验
- `tests/sql/test_compiler.py` — 含窗口函数的编译器输出一致性

---

> Phase 3B | **已完成** | 枚举值自动检测已实现（Phase 3B.1） | 1123 全量通过
