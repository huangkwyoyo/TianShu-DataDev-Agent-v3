# SpecEnricher LLM 输出校验缺口修复计划

## 一、CRCS 分类

### 窗口函数白名单校验：B 类 — DESIGN-REVIEW

| 判定维度 | 结论 |
|----------|------|
| 是否纯机械改动？ | 否——JSON Schema 与实际 WindowFunction 枚举存在命名不一致，需决定修复策略 |
| 是否有多种可行方案？ | 是——3 种方案（改 JSON Schema / 加映射层 / 两者都做） |
| 是否影响 LLM 契约？ | 是——修改 JSON Schema 意味着 Prompt 模板变更 |

### expression 安全校验：B 类 — DESIGN-REVIEW

| 判定维度 | 结论 |
|----------|------|
| 是否纯机械改动？ | 否——涉及安全防线设计，需决定校验位置和策略 |
| 是否影响安全边界？ | 不改变现有防线，但填补 LLM 路径的防线空洞 |
| 是否有多种可行方案？ | 是——3 种方案（字符黑名单 / 安全类型 / 下游校验） |

---

## 二、可复现的事实与证据

### 2.1 JSON Schema 与实际枚举的命名不一致（新发现）

```python
# 文件 1：prompts 中的 JSON Schema（告诉 LLM 输出什么）
# src/tianshu_datadev/planning/spec_enricher.py 第 1010 行
_METRIC_JSON_SCHEMA:
  window_function.enum: ["ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG", "LAG", "LEAD"]
                         ↑ 7 个值，SUM/AVG 无 _OVER 后缀，缺 NTILE、COUNT_OVER

# 文件 2：WindowFunction 枚举（系统实际接受的值）
# src/tianshu_datadev/planning/models.py 第 102-116 行
class WindowFunction(str, Enum):
    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    NTILE = "NTILE"          # ← JSON Schema 中没有
    LAG = "LAG"
    LEAD = "LEAD"
    SUM_OVER = "SUM_OVER"    # ← JSON Schema 中是 "SUM"
    AVG_OVER = "AVG_OVER"    # ← JSON Schema 中是 "AVG"
    COUNT_OVER = "COUNT_OVER" # ← JSON Schema 中没有

# 文件 3：WindowValidator 白名单（执行层校验）
# src/tianshu_datadev/validation/window_validator.py 第 27-37 行
_VALID_WINDOW_FUNCTIONS: frozenset[WindowFunction] = frozenset({
    WindowFunction.ROW_NUMBER,    # ✅ 三端一致
    WindowFunction.RANK,          # ✅ 三端一致
    WindowFunction.DENSE_RANK,    # ✅ 三端一致
    WindowFunction.NTILE,         # ❌ JSON Schema 缺失
    WindowFunction.LAG,           # ✅ 三端一致
    WindowFunction.LEAD,          # ✅ 三端一致
    WindowFunction.SUM_OVER,      # ❌ JSON Schema 写的是 "SUM"
    WindowFunction.AVG_OVER,      # ❌ JSON Schema 写的是 "AVG"
    WindowFunction.COUNT_OVER,    # ❌ JSON Schema 缺失
})
```

**结论**：即使 LLM 严格遵循 JSON Schema 输出 `"SUM"`，下游 Builder/Validator 使用 `WindowFunction.SUM_OVER`，也会不匹配。**JSON Schema 本身就是错的。**

### 2.2 expression 在 Parser 路径被拦截但在 LLM 路径不受限

```python
# Parser 路径（确定性，安全 ✅）
# src/tianshu_datadev/developer_spec/parser.py 第 130-133 行
_FORBIDDEN_SQL_FIELDS = frozenset({
    "raw_sql", "where_sql", "join_on", "expression",  # ← expression 在此
    "aggregation_expr", "having_sql",
})
# → YAML 中写 "expression: xxx" → ParseError(E007) ✅

# LLM Enricher 路径（安全 ❌）
# src/tianshu_datadev/planning/spec_enricher.py 第 1241-1250 行
inferred_computed.append(
    InferredComputedMetric(
        expression=item.get("expression", ""),  # ← 无任何校验！
        ...
    )
)
# → LLM 输出 expression: "1; DROP TABLE users; --" → 透传 ❌
```

### 2.3 _parse_llm_response 的校验不对称

```python
# 指标解析——有校验（H2 聚合函数枚举）
for item in raw.get("inferred_metrics", []):
    try:
        agg = AggregationType(item["aggregation"])  # ✅ 枚举校验
    except (KeyError, ValueError):
        continue  # 非法聚合函数 → 丢弃

# 窗口指标解析——无校验
for item in raw.get("inferred_window_metrics", []):
    inferred_window.append(
        InferredWindowMetric(
            window_function=item.get("window_function", ""),  # ❌ 完全透传
            ...
        )
    )

# 计算指标解析——无校验
for item in raw.get("inferred_computed_metrics", []):
    inferred_computed.append(
        InferredComputedMetric(
            expression=item.get("expression", ""),  # ❌ 完全透传
            ...
        )
    )
```

### 2.4 测试基线

```
1471 passed, 0 failed, 1 warning
test_spec_enricher_llm.py: 12 passed  （新增）
test_relationship_planner_llm.py: 12 passed  （新增）
```

---

## 三、正确行为

### 3.1 window_function 校验

```
输入：LLM JSON 中的 window_function 字符串
行为：
  ✅ "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD"        → 直接通过
  ✅ "SUM"       → 自动映射为 "SUM_OVER"   （JSON Schema 兼容）
  ✅ "AVG"       → 自动映射为 "AVG_OVER"   （JSON Schema 兼容）
  ✅ "NTILE"     → 直接通过                   （补充 JSON Schema）
  ✅ "COUNT_OVER" → 直接通过                  （补充 JSON Schema）
  ❌ "PERCENT_RANK", "MEDIAN", 空字符串, 任意非法值  → 静默丢弃该项
```

### 3.2 expression 安全校验

```
输入：LLM JSON 中的 expression 字符串
行为：
  ✅ "paid_count / total_count"       → 通过（合法算术表达式）
  ✅ "quantity * unit_price"          → 通过
  ✅ "" （空字符串）                   → 通过（允许不填）
  ❌ "1; DROP TABLE users; --"        → 丢弃该项（含 SQL 注入字符）
  ❌ "x' OR '1'='1"                   → 丢弃该项（含 SQL 注入字符）
  ❌ "x`; DELETE FROM orders`"        → 丢弃该项（含反引号）
```

**校验策略**：expression 中禁止出现 SQL 特殊字符（`;`, `'`, `"`, `` ` ``, `--`, `/*`）。这与 `_PHYSICAL_TABLE_NAME_FORBIDDEN` 的设计理念一致——Schema 层拦截非法字符。

---

## 四、不能改变的边界

| 边界 | 约束 | 本次遵守 |
|------|------|---------|
| WindowFunction 枚举 | `WindowFunction(str, Enum)` 值不修改 | 只在 `_parse_llm_response` 内部做名称映射，不改枚举定义 |
| `_METRIC_JSON_SCHEMA` | JSON Schema 结构不变 | 只修正 `window_function.enum` 值，不改结构 |
| Parser 的 `_FORBIDDEN_SQL_FIELDS` | 保持仅作用于 YAML 解析 | 在 Enricher 中独立实现 expression 校验，不耦合 Parser |
| 现有测试 | 1471 个测试保持通过 | 不修改已有测试，新增测试覆盖新校验路径 |
| 静默丢弃策略 | 非法项静默丢弃（不抛异常） | 新增校验遵循相同策略：非法窗口函数/expression → 丢弃该项 |
| Pydantic 模型 | `InferredWindowMetric` / `InferredComputedMetric` 模型不修改 | 校验在解析函数中完成，模型字段类型保持不变 |
| Prompt 模板 | `_METRIC_INFERENCE_SYSTEM_PROMPT` 正文不修改 | 只修改 JSON Schema 的 enum 列表 |

---

## 五、本轮允许做到哪一步

### 5.1 做

```
1. 修正 _METRIC_JSON_SCHEMA 的 window_function.enum
   - "SUM" → "SUM_OVER"
   - "AVG" → "AVG_OVER"
   - 新增 "NTILE", "COUNT_OVER"
   理由：JSON Schema 是 LLM 的契约——契约本身错了，LLM 输出就不可能对

2. 在 SpecEnricher._parse_llm_response() 新增 window_function 白名单校验
   - 合法值（直接匹配 WindowFunction 枚举）→ 通过
   - "SUM" / "AVG"（JSON Schema 旧值兼容）→ 自动映射为 "SUM_OVER" / "AVG_OVER"
   - 非法值 → 静默丢弃该项

3. 在 SpecEnricher._parse_llm_response() 新增 expression 安全校验
   - 禁止字符集合：; ' " ` -- /*
   - 含禁止字符 → 静默丢弃该项

4. 新增测试
   - test_spec_enricher_llm.py 新增 ~8 个测试
   - 覆盖：合法窗口函数通过、非法窗口函数丢弃、"SUM"/"AVG" 映射、expression SQL 注入拒绝、合法 expression 通过

5. 更新 llm_responses/enricher/mixed_valid_invalid.json fixture
   - 将 window_function 的 PERCENT_RANK → 预期被丢弃
```

### 5.2 不做

| 不做 | 理由 |
|------|------|
| 不修改 WindowFunction 枚举 | 枚举值被 Builder/Validator/Compiler 共同使用，修改影响面太大 |
| 不修改 InferredWindowMetric 模型 | 模型字段 window_function: str 保持不变——白名单校验在解析函数层完成 |
| 不修改 Parser 的 _FORBIDDEN_SQL_FIELDS | 职责独立——Parser 管 YAML，Enricher 管 LLM 输出 |
| 不给 computed metric 加 depends_on 校验 | 需要跨表引用检查，涉及 SourceManifest——超出本轮范围 |
| 不修改 relationship_planner | 该函数已有完善校验（H1/H2） |

---

## 六、验收标准

### 6.1 自动化

```bash
# 1. 新测试通过
pytest tests/planning/test_spec_enricher_llm.py -v  # 预期 20+ passed

# 2. 全量回归
pytest --tb=short -q  # 预期 1479+ passed, 0 failed

# 3. 窗口函数映射验证
# - test_parse_window_function_maps_sum_to_sum_over
# - test_parse_window_function_maps_avg_to_avg_over
# - test_parse_window_function_rejects_invalid

# 4. expression 安全验证
# - test_parse_rejects_sql_injection_in_expression
# - test_parse_allows_legitimate_expression
# - test_parse_allows_empty_expression
```

### 6.2 验收清单

```
☐ _METRIC_JSON_SCHEMA 的 window_function.enum 与 WindowFunction 枚举一致
☐ _parse_llm_response 对非法 window_function 静默丢弃
☐ _parse_llm_response 对 "SUM" / "AVG" 自动映射为 "SUM_OVER" / "AVG_OVER"
☐ _parse_llm_response 对含 SQL 注入字符的 expression 静默丢弃
☐ 新增 8+ 测试全部通过
☐ 1471 个现有测试零退化
☐ 无新增 ruff/mypy 告警
```

---

## 七、对不懂 AI 的人的解释

### 背景

我们的系统有两个"入口"让数据进来：

1. **程序员手写需求书**（Markdown/YAML 格式）→ 系统用严格规则解析，遇到"自由 SQL 文本"直接拒绝
2. **LLM 智能推断**（JSON 格式）→ LLM 阅读需求书描述后，自动补充"你可能还需要这些指标/计算"

第 2 条路是方便——程序员不用写全所有细节，系统帮你猜。但 LLM 的输出**不可信**——它可能编造不存在的函数名、写出危险的 SQL 代码。

### 发现的问题

检查代码后发现，第 2 条路的"安检"有两个漏洞：

**漏洞 1：窗口函数名不对**

LLM 被告知可以输出 `"SUM"`、`"AVG"` 这样的窗口函数名。但系统实际用的是 `"SUM_OVER"`、`"AVG_OVER"`。这个"说明书"本身就写错了——LLM 照着说明书写对了，但系统不认。这就像告诉快递员"送到 3 号楼"，但公司门牌号是"3-OVER 号楼"——快递送不到。

同时，系统支持 `NTILE` 和 `COUNT_OVER` 这两种窗口函数，但说明书里漏写了。LLM 不知道该功能存在，永远不会建议用它们。

**漏洞 2：expression 字段无安检**

LLM 可以输出 `"expression": "a / b"` 来表示"a 除以 b"。但如果 LLM 幻觉发作，输出了 `"expression": "1; DROP TABLE orders; --"`（一个删表攻击），系统不会拦截——直接透传给下游处理。同一个字段如果程序员手写在需求书 YAML 里，系统会直接拒绝（"禁止自由 SQL 文本"）——但 LLM 产出的却不管。

### 这次修什么

1. **修说明书**（JSON Schema）：把窗口函数的名称改成系统实际使用的名字，补上遗漏的功能
2. **加安检**（`_parse_llm_response` 函数）：对 LLM 输出的窗口函数名做白名单检查，对 expression 中的 SQL 注入攻击字符做拦截

### 影响

- **对现有功能**：零影响。改的是 LLM 推断路径的校验逻辑，当前 LLM 未开启
- **对将来的 LLM**：LLM 输出的窗口函数名更准确（说明书对了），expression 中的危险内容会被拦截（安检生效了）
- **对安全**：补上了 LLM 路径中一个与 Parser 路径不对称的防线空洞

---

## 八、实施计划

### 8.1 修改文件清单

```
修改文件（3 个）:
├── src/tianshu_datadev/planning/spec_enricher.py
│   ├── _METRIC_JSON_SCHEMA: 修正 window_function.enum（4 处变更）
│   └── _parse_llm_response(): 新增 2 段校验逻辑（~20 行）
├── tests/planning/test_spec_enricher_llm.py
│   └── 新增 ~8 个测试
└── tests/fixtures/llm_responses/enricher/mixed_valid_invalid.json
    └── 调整 window_function 测试值以反映新的白名单行为

未修改文件（0 个删除）:
（无——本轮不删除任何文件）
```

### 8.2 代码变更预览

```python
# spec_enricher.py — _METRIC_JSON_SCHEMA 修正（第 1009 行）
# 变更前：
"enum": ["ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG", "LAG", "LEAD"],
# 变更后：
"enum": ["ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
         "LAG", "LEAD", "SUM_OVER", "AVG_OVER", "COUNT_OVER"],

# spec_enricher.py — _parse_llm_response() 新增校验（第 1227 行之前）
# 新增：窗口函数名白名单校验 + 旧名映射
_VALID_WINDOW_FUNCTIONS = frozenset({
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
    "LAG", "LEAD", "SUM_OVER", "AVG_OVER", "COUNT_OVER",
})
_WINDOW_FUNCTION_ALIASES = {"SUM": "SUM_OVER", "AVG": "AVG_OVER"}

# 在解析 window_metrics 的循环中新增：
wf = item.get("window_function", "")
if wf in _WINDOW_FUNCTION_ALIASES:
    wf = _WINDOW_FUNCTION_ALIASES[wf]
if wf not in _VALID_WINDOW_FUNCTIONS:
    continue  # 非法窗口函数 → 静默丢弃

# spec_enricher.py — _parse_llm_response() 新增校验（第 1241 行之前）
# 新增：expression 安全校验
_FORBIDDEN_EXPRESSION_CHARS = frozenset({";", "'", '"', "`"})
_FORBIDDEN_EXPRESSION_PATTERNS = ("--", "/*")

# 在解析 computed_metrics 的循环中新增：
expr = item.get("expression", "")
if any(c in expr for c in _FORBIDDEN_EXPRESSION_CHARS):
    continue  # 含 SQL 注入字符 → 丢弃
if any(p in expr for p in _FORBIDDEN_EXPRESSION_PATTERNS):
    continue  # 含 SQL 注释标记 → 丢弃
```

### 8.3 执行顺序

```
Phase A: 修正 JSON Schema（1 行变更）
    ↓
Phase B: 新增 window_function 白名单校验（~15 行）
    ↓
Phase C: 新增 expression 安全校验（~10 行）
    ↓
Phase D: 新增 8 个测试
    ↓
Phase E: 更新 mixed_valid_invalid.json fixture
    ↓
Phase F: 全量回归（1471 → 1479+）
```

### 8.4 Git 提交策略

```
Commit 1: fix(spec_enricher): 修正 window_function JSON Schema enum 与实际枚举一致
  - 1 file changed: spec_enricher.py

Commit 2: fix(spec_enricher): 新增 window_function 白名单校验 + expression 安全校验
  - 1 file changed: spec_enricher.py

Commit 3: test(spec_enricher): 新增校验路径的 fixture 测试
  - 2 files changed: test_spec_enricher_llm.py + mixed_valid_invalid.json
```

### 8.5 可观测性设计

- 每个校验规则有独立测试：`test_parse_window_function_rejects_PER_PERCENT_RANK`
- 测试失败时 pytest 输出精确到行号的断言差异
- 新增校验逻辑中预留了集合/常量定义——便于后续审计和 grep 追溯
- 每个 commit message 遵循 Conventional Commits 规范

---

> **文档元信息**
> - 创建时间：2026-07-01
> - CRCS 分类：B 类 — DESIGN-REVIEW
> - 关联文档：[[llm_response_fixture_plan_20260701]]
> - 依赖文件：`src/tianshu_datadev/planning/spec_enricher.py` (L942-1052, L1172-1262)
> - 影响范围：仅 SpecEnricher LLM 路径，不涉及 Parser / Builder / Validator
