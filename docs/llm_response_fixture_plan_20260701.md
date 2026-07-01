# LLM 输出解析器 Fixture 补充计划

## 一、CRCS 分类

**B 类 — DESIGN-REVIEW（需设计确认）**

| 判定维度 | 结论 |
|----------|------|
| 是否纯机械改动？ | 否——涉及新 fixture 格式设计、测试架构决策、Mock 策略选择 |
| 是否影响架构边界？ | 不触及 SQL Generation Boundary / Validation Boundary / SafeIdentifier |
| 是否有多种可行方案？ | 是——fixture 格式（JSON 文件 vs Python dict）、测试粒度（单函数 vs 集成）、组织方式（独立文件 vs 合并） |
| 是否需要非 AI 人员理解？ | 是——fixture 文件是"LLM 输出的标准答案"，产品/测试需要能读懂和补充 |

不属于 A 类（非纯机械改动），也不属于 C 类（不改变安全红线），归入 **B 类**。

---

## 二、可复现的事实与证据

### 2.1 两条零测试的解析函数

```python
# 文件：src/tianshu_datadev/planning/relationship_planner.py

class RelationshipPlanner:
    def _parse_llm_response(self, raw: dict, manifest: SourceManifest) -> list[dict]:
        """解析 LLM JSON → 校验 H1/H2 → 合法候选 dict 列表"""
        # 行 526-585，60 行，6 项校验规则
        # 当前测试覆盖：0 个独立测试
        # 间接覆盖：无——FakeRelationshipPlanner 不调用此函数
```

```python
# 文件：src/tianshu_datadev/planning/spec_enricher.py

class SpecEnricher:
    def _parse_llm_response(self, raw: dict, spec: ParsedDeveloperSpec) -> EnrichedSpec:
        """解析 LLM JSON → 校验 H1-H5 → EnrichedSpec"""
        # 行 1172-1262，91 行，5 项校验规则，3 个子解析器
        # 当前测试覆盖：0 个独立测试
        # 间接覆盖：无——FakeSpecEnricher 走规则路径，不调用此函数
```

### 2.2 为什么间接覆盖不存在

Pipeline 当前 `llm_client=None`（fallback 模式），两个 Planner/Enricher 都走 Fake 实现（纯规则），**从不调用 `_parse_llm_response()`**。这意味着：

- 即使 `_parse_llm_response()` 有严重 bug，当前 1447 个测试也无法发现
- 当 LLM 开启时（`llm_client` 注入），这两条函数是唯一的防线——但防线本身未经测试

### 2.3 静默丢弃——最危险的容错策略

两个解析函数都采用"非法项静默丢弃，不抛异常"：

```python
# relationship_planner.py:554-555
if left_key not in left_cols or right_key not in right_cols:
    continue  # 非法字段名 → 丢弃（不抛异常、不记录日志）
```

```python
# spec_enricher.py:1199-1201
try:
    agg = AggregationType(item["aggregation"])
except (KeyError, ValueError):
    continue  # H2：非法聚合函数，丢弃
```

如果校验逻辑有 bug（如 `table_columns` 构建错误导致所有 key 都找不到），**不会报错**——所有 LLM 产出被静默丢弃，最终输出空列表，用户看到的是"系统没有推断出任何 Join 关系"，而不是"系统出错了"。

### 2.4 已定义但未测试的校验规则

**RelationshipPlanner（7 条硬约束，6 条在解析器中校验）：**

| 约束 | 代码位置 | 校验逻辑 | 有测试？ |
|------|----------|----------|----------|
| H1 键名必须在 manifest 中存在 | L552-555 | `left_key not in left_cols → continue` | ❌ |
| H1 表别名必须有效 | L558-559 | `left_table not in table_columns → continue` | ❌ |
| H1 禁止自引用 Join | L562-563 | `left_table == right_table → continue` | ❌ |
| H2 join_type 合法枚举 | L566-568 | 非法值降级为 INNER | ❌ |
| confidence 合法枚举 | L571-573 | 非法值降级为 medium | ❌ |
| 空列表处理 | L545 | `raw.get("inferred_joins", [])` | ❌ |

**SpecEnricher（8 条硬约束，5 条在解析器中校验）：**

| 约束 | 代码位置 | 校验逻辑 | 有测试？ |
|------|----------|----------|----------|
| H2 聚合函数合法枚举 | L1198-1201 | 非法值 → continue | ❌ |
| H3 filter.column 必须存在 | L1204-1212 | 非法 filter → 丢弃 filter 保留指标 | ❌ |
| H3 filter 字段缺失容错 | L1211-1212 | `except Exception: pass` | ❌ |
| 窗口指标解析 | L1227-1238 | 缺少字段时静默使用默认值 | ❌ |
| 计算指标解析 | L1241-1250 | 同上 | ❌ |

### 2.5 测试基线

- 当前 1447 个测试全部通过，0 失败
- `tests/planning/` 下无 `test_relationship_planner.py` 或 `test_spec_enricher_llm.py`
- `tests/harness/test_spec_enricher.py` 只测 Fake 路径（规则推断），不涉及 `_parse_llm_response()`

### 2.6 额外发现：JSON Schema 与系统枚举命名不一致（本轮执行中暴露）

在执行 Fixture 补充过程中，进一步追踪代码发现：**给 LLM 的 JSON Schema 本身就写错了**——LLM 被要求输出的窗口函数名，与系统实际接受的不一致。

#### 三端对比

```
JSON Schema（spec_enricher.py:1010）  →  告诉 LLM 可以输出什么
WindowFunction 枚举（models.py:108-116） →  系统实际接受什么
WindowValidator 白名单（window_validator.py:27-37） →  执行层校验什么
```

| 序号 | JSON Schema 写的 | WindowFunction 枚举 | WindowValidator 白名单 | 状态 |
|------|-----------------|---------------------|----------------------|------|
| 1 | `ROW_NUMBER` | `ROW_NUMBER` | `ROW_NUMBER` | ✅ 三端一致 |
| 2 | `RANK` | `RANK` | `RANK` | ✅ 三端一致 |
| 3 | `DENSE_RANK` | `DENSE_RANK` | `DENSE_RANK` | ✅ 三端一致 |
| 4 | （缺失） | `NTILE` | `NTILE` | ❌ LLM 不知道可用 |
| 5 | `LAG` | `LAG` | `LAG` | ✅ 三端一致 |
| 6 | `LEAD` | `LEAD` | `LEAD` | ✅ 三端一致 |
| 7 | **`SUM`** | **`SUM_OVER`** | **`SUM_OVER`** | ❌ **名字错了** |
| 8 | **`AVG`** | **`AVG_OVER`** | **`AVG_OVER`** | ❌ **名字错了** |
| 9 | （缺失） | `COUNT_OVER` | `COUNT_OVER` | ❌ LLM 不知道可用 |

**9 个窗口函数中 4 个有问题（44%）。**

#### 影响

1. **`SUM`/`AVG` 名称不匹配**：LLM 严格遵守 JSON Schema 输出 `"SUM"`，但系统代码只认 `"SUM_OVER"`。即使 LLM 输出完全正确，也会因为名字对不上而失败。
2. **`NTILE`/`COUNT_OVER` 遗漏**：系统支持这两种窗口函数，但 LLM 的说明书里没写——LLM 永远不会推荐用户使用它们，能力被隐藏了。

#### 修复

此问题在 `spec_enricher_validation_gap_fix_plan_20260701.md` 中单独规划并修复：
- 修正 JSON Schema `window_function.enum`：`SUM→SUM_OVER`, `AVG→AVG_OVER`, 补 `NTILE`/`COUNT_OVER`
- 新增旧名兼容映射：`{"SUM": "SUM_OVER", "AVG": "AVG_OVER"}`
- 新增窗口函数白名单校验（9 种合法值，非法静默丢弃）

---

## 三、正确行为应当是什么

### 3.1 RelationshipPlanner._parse_llm_response()

```
输入：LLM 返回的 JSON dict + SourceManifest
输出：list[dict]（校验通过的 Join 候选）

正确行为：
✅ 合法候选（字段存在 + join_type 合法 + 表别名有效 + 非自引用）→ 全量保留
✅ H1 违规（字段名/表别名不存在）→ 静默丢弃该项（不抛异常）
✅ H1 违规（自引用 left_table==right_table）→ 静默丢弃该项
✅ H2 违规（join_type 不在枚举中）→ 降级为 "INNER"，不丢弃
✅ confidence 非法 → 降级为 "medium"，不丢弃
✅ 空 inferred_joins=[] → 返回 []
✅ 缺少 inferred_joins 键 → 返回 []
✅ reasoning 字段缺失 → 返回 ""（默认值）
```

### 3.2 SpecEnricher._parse_llm_response()

```
输入：LLM 返回的 JSON dict + ParsedDeveloperSpec
输出：EnrichedSpec（含 inferred_metrics / inferred_window_metrics / inferred_computed_metrics）

正确行为：
✅ 合法指标（aggregation 枚举正确）→ 全部解析为 MetricDecl
✅ H2 违规（aggregation 非法）→ 静默丢弃该项
✅ H3 违规（filter.column 不存在于 schema）→ 丢弃 filter，保留指标
✅ H3 违规（filter 结构不完整）→ 丢弃 filter，保留指标
✅ input_column=None → 正常保留（对应 COUNT(*)）
✅ distinct=false（默认）→ 正常
✅ 窗口指标缺少 partition_by/order_by → 使用默认空列表
✅ 计算指标缺少 depends_on → 使用默认空列表
✅ 三个列表全部为空 → 返回含空列表的 EnrichedSpec
✅ raw 缺少某键 → 对应列表为空
```

---

## 四、哪些边界不能改变

| 边界 | 约束 | 本次如何遵守 |
|------|------|-------------|
| 函数签名 | `_parse_llm_response(raw: dict, manifest/spec)` 不变 | 测试只调用公开函数，不测私有实现细节 |
| JSON Schema 契约 | `_RELATIONSHIP_JSON_SCHEMA` / `_METRIC_JSON_SCHEMA` 不变 | Fixture 内容严格匹配已定义的 JSON Schema 结构 |
| Pydantic 模型 | StrictModel extra="forbid" 不变 | 不修改任何模型定义 |
| 现有测试 | 1447 个测试保持通过 | 新增测试文件，不修改已有测试 |
| LLM 调用路径 | `_llm_plan()` / `_llm_enrich()` 不变 | 只测 `_parse_llm_response()`，不测 LLM 调用 |
| 容错策略 | "静默丢弃非法项"不变 | 测试验证丢弃行为，不改变策略 |
| Fixture 目录 | `tests/fixtures/` 是唯一 fixture 根目录 | 新增 `tests/fixtures/llm_responses/`，不动其他目录 |
| 根目录 `fixtures/` | 不归集、不删除 | 本轮不处理，保持现状 |

---

## 五、本轮允许做到哪一步

### 5.1 做

```
新增 Fixture 文件（tests/fixtures/llm_responses/）:
  ├── relationship/
  │   ├── normal.json              # 2 个合法 JOIN
  │   ├── field_not_found.json     # left_key 不存在
  │   ├── invalid_join_type.json   # join_type="CROSS"
  │   ├── self_join.json           # left_table==right_table
  │   ├── table_alias_invalid.json # left_table 不存在
  │   ├── empty_list.json          # inferred_joins=[]
  │   ├── missing_key.json         # 无 inferred_joins 键
  │   └── mixed_valid_invalid.json # 2 合法 + 2 非法
  └── enricher/
      ├── normal.json              # 3 种推断齐全
      ├── invalid_aggregation.json # aggregation="MEDIAN"
      ├── invalid_filter.json      # filter 不合法
      ├── window_metrics.json      # 窗口指标
      ├── computed_metrics.json    # 计算指标
      ├── empty_all.json           # 全部空列表
      └── mixed_valid_invalid.json # 2 合法 + 2 非法

新增测试文件:
  ├── tests/planning/test_relationship_planner_llm.py  # ~15 测试
  └── tests/planning/test_spec_enricher_llm.py          # ~15 测试

辅助代码:
  └── 每个测试文件内定义 _build_mock_manifest() 工厂函数
      ——构造最小合法 SourceManifest，无需外部文件依赖
```

### 5.2 不做

| 不做 | 理由 |
|------|------|
| 不修改 `_parse_llm_response()` 函数本身 | 本轮只补测试，不改生产代码 |
| 不测 `_llm_plan()` / `_llm_enrich()` | 那需要 Mock AnthropicAdapter，属于集成测试 |
| 不测 LLM Prompt 模板 | 由 `tests/prompts/test_prompt_manager.py` 覆盖 |
| 不测 ComputeSteps LLM 输出 | 该功能尚未实现（LLM 推断留到下一轮） |
| 不归集 `fixtures/` 根目录 | 见边界约束 |
| 不修改 Pipeline 的 llm_client 注入 | 保持 fallback 模式不变 |

---

## 六、用什么方式验收

### 6.1 自动化验收

```bash
# 1. 新增测试全部通过
pytest tests/planning/test_relationship_planner_llm.py -v
pytest tests/planning/test_spec_enricher_llm.py -v

# 2. 全量回归零退化
pytest --tb=short -q  # 预期：1447+ → 1477+ passed, 0 failed
```

### 6.2 覆盖率验收

| 函数 | 当前覆盖 | 目标覆盖 |
|------|----------|----------|
| `RelationshipPlanner._parse_llm_response()` | 0% | 100% 分支覆盖 |
| `SpecEnricher._parse_llm_response()` | 0% | 100% 分支覆盖 |

验收标准：
```
☐ RelationshipPlanner._parse_llm_response() 6 项校验规则每项至少 1 个测试
☐ SpecEnricher._parse_llm_response() 5 项校验规则每项至少 1 个测试
☐ 每个 fixture 文件是合法 JSON（json.loads 可解析）
☐ 每个 fixture 内容与对应 JSON Schema 结构一致
☐ 1447 个现有测试零退化
☐ 新增 30+ 测试全部通过
☐ 无新增 ruff/mypy 告警
```

### 6.3 回归保障

- 所有 fixture 文件是**纯数据**（JSON），不含任何 Python 逻辑——不会因代码重构而失效
- 测试文件通过 `_build_mock_manifest()` 内联构造依赖——不依赖外部文件或数据库
- 每次测试独立构造 manifest，测试间无共享状态
- Fixture 文件路径使用 `os.path.join(os.path.dirname(__file__), "..", "fixtures/llm_responses/")` 模式——与现有测试保持一致

### 6.4 可观测性

- 每个测试名明确描述场景：`test_parse_rejects_field_not_in_manifest`、`test_parse_downgrades_invalid_join_type`
- 失败时 pytest 输出包含 fixture 文件名和具体断言差异
- Fixture 文件名自描述场景（`field_not_found.json`、`invalid_aggregation.json`）

---

## 七、对不读 AI 的人解释

### 为什么需要这个？

我们这个系统有一个设计原则：**LLM 负责"理解需求"，但代码负责"验证和生成 SQL"**。

具体流程是：
1. 用户写一份需求书（Markdown 格式）
2. 系统解析需求书，得到结构化的数据（表名、字段、聚合方式）
3. 如果需求书里没写 JOIN 关系，系统调用 LLM 去**推测**表之间应该怎么关联
4. LLM 返回一段 JSON，里面是它推测的关联关系
5. **系统用自己的代码去解析和校验这段 JSON，过滤掉不合法的部分**
6. 校验通过的部分进入 SQL 生成流程

第 5 步是安全防线的关键——**LLM 的输出不可信**，必须经过严格校验才能使用。

### 现在的问题

第 5 步的"解析和校验代码"（两个函数，加起来 150 行）**没有任何测试**。

这意味着：
- 如果校验代码有 bug（比如把合法 JOIN 误判为非法），LLM 推断的结果会被静默丢弃——用户看到"系统没找到关联关系"，但不知道是代码 bug
- 如果校验代码放过了非法数据（比如引用了不存在的字段），后续 SQL 生成会报错——但报错信息与根因脱节，难以排查
- 当前 1447 个测试全部通过，但**没有一条测试跑过这两个函数**——因为 LLM 调用还没开启

### Fixture 是什么？

Fixture 就是"标准答案"——我们预先准备好 LLM 的典型输出（JSON 文件），然后验证解析器能不能正确吃掉这些输出、产出预期的数据结构。

类比：
- 你要测试一个"英文翻译成中文"的程序
- Fixture 就是"事先准备好的英文句子 + 标准中文翻译"
- 测试就是"把英文句子喂给程序，看输出是否等于标准翻译"
- 你不需要每次都调真正的翻译 API——你用提前准备好的输入输出对来验证程序逻辑

### 这次要做什么？

1. 准备 15 个 JSON 文件（fixture），模拟 LLM 可能返回的各种情况——正常的、异常的、边界情况
2. 写 30 个测试，用这些 fixture 去测那两个解析函数
3. **不改任何生产代码**——只补测试

### 风险是什么？

**零风险。** 只新增文件（fixture JSON + 测试 Python），不动任何已有代码。1447 个现有测试保持通过。

---

## 八、实施方案

### 8.1 文件清单

```
新增文件（17 个）:
├── tests/fixtures/llm_responses/
│   ├── relationship/
│   │   ├── normal.json                 # 2 个合法 JOIN 候选
│   │   ├── field_not_found.json        # left_key 不在 manifest 中
│   │   ├── invalid_join_type.json      # join_type="CROSS"
│   │   ├── self_join.json              # left_table == right_table
│   │   ├── table_alias_invalid.json    # left_table 不在 manifest 中
│   │   ├── empty_list.json             # inferred_joins: []
│   │   ├── missing_key.json            # 无 inferred_joins 键
│   │   └── mixed_valid_invalid.json    # 2 合法 + 2 非法
│   └── enricher/
│       ├── normal.json                 # 3 种推断齐全
│       ├── invalid_aggregation.json    # aggregation="MEDIAN"
│       ├── invalid_filter.json         # filter 字段不合法
│       ├── window_metrics.json         # 仅窗口指标
│       ├── computed_metrics.json       # 仅计算指标
│       ├── empty_all.json              # 全部空列表
│       └── mixed_valid_invalid.json    # 2 合法 + 2 非法
├── tests/planning/test_relationship_planner_llm.py   # ~15 测试
└── tests/planning/test_spec_enricher_llm.py          # ~15 测试

修改文件（0 个）:
（无——本轮只新增，不修改任何已有文件）
```

### 8.2 测试结构设计

```python
# tests/planning/test_relationship_planner_llm.py

import json
import os
import pytest
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.developer_spec.models import SourceManifest, SourceTable, ColumnDef

# ── 辅助工厂函数 ──

def _build_mock_manifest() -> SourceManifest:
    """构造最小合法 SourceManifest——用于测试 _parse_llm_response。

    包含 2 张表：orders(user_id, amount, order_date), users(id, name)
    """
    ...

def _read_fixture(name: str) -> dict:
    """读取 llm_responses/relationship/ 下的 JSON fixture。"""
    path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "llm_responses", "relationship", name
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)

# ── 测试类 ──

class TestRelationshipPlannerParseLLM:
    """验证 _parse_llm_response 对各种 LLM JSON 输出的处理。"""

    def test_parse_normal_joins(self): ...
    def test_parse_rejects_field_not_in_manifest(self): ...
    def test_parse_downgrades_invalid_join_type(self): ...
    def test_parse_rejects_self_join(self): ...
    def test_parse_rejects_invalid_table_alias(self): ...
    def test_parse_empty_list(self): ...
    def test_parse_missing_key(self): ...
    def test_parse_mixed_valid_invalid(self): ...
    def test_parse_confidence_default(self): ...
    def test_parse_missing_reasoning_defaults_to_empty(self): ...
```

```python
# tests/planning/test_spec_enricher_llm.py

# 同样结构——_build_mock_spec() 工厂 + _read_fixture() + 测试类

class TestSpecEnricherParseLLM:
    """验证 _parse_llm_response 对各种 LLM JSON 输出的处理。"""

    def test_parse_normal_all_three_types(self): ...
    def test_parse_rejects_invalid_aggregation(self): ...
    def test_parse_drops_invalid_filter_keeps_metric(self): ...
    def test_parse_window_metrics(self): ...
    def test_parse_computed_metrics(self): ...
    def test_parse_empty_all_lists(self): ...
    def test_parse_mixed_valid_invalid(self): ...
    def test_parse_missing_input_column_allowed(self): ...
    def test_parse_enrichment_metadata(self): ...
```

### 8.3 Fixture JSON 示例

**`tests/fixtures/llm_responses/relationship/normal.json`：**
```json
{
  "inferred_joins": [
    {
      "left_table": "orders",
      "right_table": "users",
      "left_key": "user_id",
      "right_key": "id",
      "join_type": "INNER",
      "confidence": "high",
      "reasoning": "orders.user_id 是 users.id 的外键"
    },
    {
      "left_table": "orders",
      "right_table": "products",
      "left_key": "product_id",
      "right_key": "id",
      "join_type": "LEFT",
      "confidence": "medium",
      "reasoning": "订单可能无产品关联"
    }
  ]
}
```

**`tests/fixtures/llm_responses/enricher/normal.json`：**
```json
{
  "inferred_metrics": [
    {
      "metric_name": "total_amount",
      "aggregation": "SUM",
      "input_column": "amount",
      "alias": "total_amount",
      "filter": null,
      "input_expression": null,
      "distinct": false,
      "confidence": "high",
      "reasoning": "业务描述提到'总金额'"
    }
  ],
  "inferred_window_metrics": [
    {
      "metric_name": "amount_rank",
      "window_function": "RANK",
      "input_column": "amount",
      "partition_by": ["user_id"],
      "order_by": ["amount DESC"],
      "alias": "amount_rank",
      "confidence": "medium",
      "reasoning": "业务描述提到'排名'"
    }
  ],
  "inferred_computed_metrics": [
    {
      "metric_name": "conversion_rate",
      "expression": "paid_count / total_count",
      "depends_on": ["paid_count", "total_count"],
      "alias": "conversion_rate",
      "confidence": "low",
      "reasoning": "业务描述提到'转化率'"
    }
  ]
}
```

### 8.4 执行顺序

```
Phase 1: 创建 fixture 目录 + 编写 15 个 JSON 文件
    ↓
Phase 2: 编写 test_relationship_planner_llm.py（~15 测试）
    ↓  （每写 3-4 个测试就跑一次，确认通过）
Phase 3: 编写 test_spec_enricher_llm.py（~15 测试）
    ↓  （同上，渐进验证）
Phase 4: 全量回归——确认 1447 个现有测试零退化
    ↓
Phase 5: 覆盖率确认——检查两个 _parse_llm_response 的分支覆盖
```

### 8.5 可追溯设计

- **Git 原子提交**：Fixtures（数据）和 Tests（代码）分两个 commit
  - Commit 1: `test: add LLM response fixtures (15 JSON files)`
  - Commit 2: `test: add _parse_llm_response fixture tests (30 tests)`
- **Fixture 不可变性**：JSON 文件写入后不再修改——后续只新增，不覆盖
- **测试名自文档化**：`test_parse_rejects_field_not_in_manifest` → 无需读代码即知测什么

---

> **文档元信息**
> - 创建时间：2026-07-01
> - CRCS 分类：B 类 — DESIGN-REVIEW
> - 关联术语：[[datadev_engineering_glossary_20260629_1600]] §52-54（LLM Gateway / Prompt Manager / SpecEnricher）
> - 依赖文档：[[spec_schema_dag_extension_plan_20260701]]
> - 后续文档：[[spec_enricher_validation_gap_fix_plan_20260701]]（执行过程中发现的 JSON Schema 命名不一致问题及修复）
