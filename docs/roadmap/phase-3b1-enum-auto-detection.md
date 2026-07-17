# Phase 3B.1：枚举值自动检测

> 状态：**已完成 ✅**（2026-06-29）
> 前置依赖：Phase 3B 退出条件全部满足 ✅
> 关联：Phase 3B LabelValidator 升级

## 动机

当前 `label_validator.py` 要求开发者在 DeveloperSpec 中**显式声明 `enum_values`** 才会触发 CASE WHEN 标签枚举校验。未声明 → 静默通过 → 标签值错误无法被拦截。

实际场景中，大量目标表字段虽然没有显式枚举声明，但事实上是枚举值字段（Flag/Status/Code 三类）。这些字段的 CASE WHEN 标签越界错误应该被自动检测并拦截，而非依赖开发者自觉声明。

## 核心思路

> 从目标表采样数据 → 自动判定字段是否属于枚举值字段 → 仅当检测到枚举值字段时触发 CASE WHEN 标签越界拦截

## 枚举值分类

```
枚举值
├── 标志位（Flag）    — 0/1、Y/N、YES/NO    例：is_waisu=1
├── 状态码（Status）   — 固定英文短语          例：Status='Approved'
└── 分类代码（Code）   — 字母缩写或纯数字       例：Type=PDR、payment_type=2
```

### 检测启发式

三类枚举的确定性有本质差异——不能用单一阈值统一。

#### Flag（标志位）——确定性最强

Flag 检测是**二值判定**：要么是 Flag，要么不是。不适用连续置信度。

| 必达条件 | 反例（不触发） |
|---------|--------------|
| distinct ∈ {1, 2} | distinct ≥ 3 → 跳过 |
| 全部值 ∈ {0, 1} 或 {Y, N} 或 {YES, NO} 或 {True, False} 或 {是, 否} | 出现第三类值 → 跳过 |
| 字段名匹配 `is_*` `has_*` `flag_*` `*_flag` `*_yn` | 字段名无信号 + distinct=2 → 降级为 LOW（无法确认为 Flag） |

> Flag 不设置信度——满足必达条件即为 `CERTAIN`，任一条件不满足即为 `NOT_FLAG`。

#### Status（状态码）——中等确定性，需多条信号交叉验证

Status 检测容易误判：普通文本列也可能首字母大写、distinct 不大。

| 信号 | 权重 | 说明 |
|------|------|------|
| distinct ≤ 30 且 distinct/total ≤ 0.1 | 必要 | 基数比过高一定不是 Status |
| ≥90% 值匹配 `[A-Z][a-zA-Z]*( [A-Z][a-zA-Z]*)*`（首字母大写，可空格分隔多词） | 高 | 放宽原 `[A-Z][a-z]+`，支持 "Pending Review" "In-Progress" |
| 字段名匹配 `*_status` `*_state` `status_*` `*_phase` `*_stage` | 高 | 语义线索 |
| 值列表中出现典型状态词：Approved/Pending/Active/Inactive/Complete/Draft/Closed/Open/Done | 中 | 词典命中加分 |
| 值平均长度 4-20 字符 | 低 | 排除过长文本 |

**常见误判场景**：
- 姓名字段（distinct 多但模式匹配）→ 无字段名信号 → 降级
- 城市名（distinct 多、模式匹配但不含状态词）→ 词典不命中 → 降级
- 部门名（可能全部首字母大写）→ distinct/total 比值过高 → 降级

#### Code（分类代码）——最易误判，需最强的交叉验证

Code 最容易与以下字段混淆：年份、月份、小整数 ID、金额。

| 信号 | 权重 | 说明 |
|------|------|------|
| distinct ≥ 3 且 distinct ≤ 50 且 distinct/total ≤ 0.05 | 必要 | 比 Status 基数比更严格 |
| ≥90% 值匹配纯数字（2-6 位）或大写字母缩写（2-6 位不含元音组合） | 高 | `PDR` `A001` `12` → 匹配；`Hello` `Name` → 不匹配 |
| 字段名匹配 `*_type` `*_code` `*_category` `*_class` `*_kind` `*_level` `type_*` `code_*` | 高 | 最强信号 |
| 所有值为统一长度（±1） | 中 | 年份也是统一长度——不能单独用 |
| 值不含前导零（`001` 视为含前导零） | 低 | 排除 ID 编码 |

**常见误判场景**（必须排除）：
- 年份列：2020-2025 → 全部纯数字、统一长度、distinct=6，但字段名通常含 `year`/`yr`/`年度` → 字段名白名单排除
- 月份列：1-12 → 全部纯数字，但字段名含 `month`/`mon`/`月` → 排除
- 金额/数量小整数：100/200/500 → 字段名含 `amount`/`price`/`qty`/`count`/`金额`/`数量` → 排除
- 不参与 CASE WHEN 的字段 → 字段名不在任何 WHEN 条件中出现 → 跳过

### 置信度模型：分层而非单点浮点数

取代单一 `confidence: float`，改用 **信号积分制 + 分层**。

```
每个字段的检测流程：

1. 低基数列筛选（distinct/total ≤ 阈值，distinct ≤ 绝对上限）
   ↓ 通过
2. 模式匹配：计算 pattern_match_ratio（匹配值数 / distinct）
   ↓
3. 字段名信号：检查列名是否命中语义词典
   ↓
4. 特殊排除：年份/月份/金额/数量白名单排除
   ↓
5. 分层判定：
   ┌─────────────┬──────────────────────────────────────┐
   │ CERTAIN     │ Flag 必达条件全部满足                    │
   │             │ 或 DeveloperSpec 显式声明               │
   ├─────────────┼──────────────────────────────────────┤
   │ HIGH        │ pattern_match_ratio ≥ 0.9              │
   │             │ + 字段名信号命中                         │
   │             │ + 未被特殊排除                           │
   ├─────────────┼──────────────────────────────────────┤
   │ MEDIUM      │ pattern_match_ratio ≥ 0.8              │
   │             │ + (字段名信号命中 或 词典命中)              │
   │             │ + 未被特殊排除                           │
   ├─────────────┼──────────────────────────────────────┤
   │ LOW         │ pattern_match_ratio ≥ 0.6              │
   │             │ 或 字段名信号命中但 pattern < 0.8        │
   ├─────────────┼──────────────────────────────────────┤
   │ NOT_ENUM    │ 未通过低基数列筛选 或 被特殊排除          │
   │             │ 或 pattern_match_ratio < 0.6            │
   └─────────────┴──────────────────────────────────────┘

各层在 LabelValidator 中的行为：
  CERTAIN → 生成 blocking OpenQuestion（与手动声明等效）
  HIGH    → 生成 WARN OpenQuestion（不阻断流水线）
  MEDIUM  → 生成 info OpenQuestion（仅记录在 review.md 供人工审查）
  LOW     → 不生成问题，但写入 EnumProfile 日志
  NOT_ENUM → 不参与后续检测
```

### 阈值参数（可配置，均附推导依据）

| 参数 | 默认值 | 推导依据 |
|------|--------|---------|
| `max_distinct_ratio_status` | 0.1 | 1000 行表中超过 100 个 distinct 值不太可能是状态 |
| `max_distinct_ratio_code` | 0.05 | 分类代码的基数比应更低——1000 行中超过 50 种分类需怀疑 |
| `max_flag_distinct` | 2 | Flag 定义——超过 2 种值不是标志位 |
| `max_status_distinct` | 30 | 状态值通常 ≤ 10，30 是宽松上限（留空间给历史遗留值） |
| `max_code_distinct` | 50 | 分类代码通常 ≤ 30，50 是宽松上限 |
| `sample_size` | 10000 | 覆盖大部分日表（单日 < 1000 万行的 distinct 近似） |
| `pattern_match_ratio_high` | 0.9 | 90% 值匹配模式——允许少量脏数据 |
| `pattern_match_ratio_medium` | 0.8 | 80% 值匹配——需字段名信号加成 |
| `pattern_match_ratio_low` | 0.6 | 低于 60% 视为噪声——不触发检测 |

## 数据流

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
│ Executor      │────▶│ EnumProfiler     │────▶│ EnumRegistry      │
│ (已执行查询)   │     │ 采样 + 分类检测   │     │ {field: {values}} │
└──────────────┘     └─────────────────┘     └──────────────────┘
                                                      │
                                                      ▼
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
│ CaseWhenStep  │────▶│ LabelValidator   │────▶│ OpenQuestion[]    │
│ (IR 标签步骤) │     │ 合并声明+自动检测  │     │ blocking if越界   │
└──────────────┘     └─────────────────┘     └──────────────────┘
```

## 新增/修改文件

| 文件 | 操作 | 说明 |
|------|------|------|
| `src/tianshu_datadev/profiling/__init__.py` | 新建 | profiling 模块 |
| `src/tianshu_datadev/profiling/enum_profiler.py` | 新建 | EnumProfiler——采样 + 分类检测 |
| `src/tianshu_datadev/profiling/models.py` | 新建 | EnumProfile / EnumFieldClass / EnumDetectionResult |
| `src/tianshu_datadev/validation/label_validator.py` | 修改 | `_collect_declared_enums()` → 合并手动声明 + 自动检测 |
| `tests/profiling/` | 新建 | test_enum_profiler.py |

## 新增模型

```python
class EnumConfidenceTier(str, Enum):
    """枚举检测置信度分层——替代单点浮点数。"""
    CERTAIN = "certain"    # Flag 必达条件全满足，或 DeveloperSpec 显式声明
    HIGH = "high"          # 模式匹配 ≥ 0.9 + 字段名信号命中 + 未被排除
    MEDIUM = "medium"      # 模式匹配 ≥ 0.8 +（字段名信号 或 词典命中）
    LOW = "low"            # 模式匹配 ≥ 0.6，或字段名信号命中但模式 < 0.8
    NOT_ENUM = "not_enum"  # 未通过筛选 或 被特殊排除

class EnumFieldClass(str, Enum):
    """枚举值字段分类。"""
    FLAG = "flag"        # 0/1、Y/N、YES/NO
    STATUS = "status"    # 固定英文短语
    CODE = "code"        # 字母缩写或纯数字

class EnumProfile(StrictModel):
    """单个字段的枚举值检测结果。"""
    table_ref: str
    column_name: str
    normalized_name: str
    field_class: EnumFieldClass | None        # None 表示 NOT_ENUM
    detected_values: list[str]                 # 检测到的所有枚举值
    distinct_count: int                        # distinct 数
    total_sampled: int                         # 采样行数
    tier: EnumConfidenceTier                   # 分层置信度（替代 float）
    pattern_match_ratio: float                 # 模式匹配率 0-1（诊断用，非决策用）
    signals: list[str]                         # 命中的信号列表（如 ["field_name:status", "dict:hit:Approved"]）
    exclusions: list[str]                      # 触发的排除规则（空列表表示未被排除）

class EnumDetectionResult(StrictModel):
    """一次 profiling 的完整结果。"""
    profiles: list[EnumProfile]
    sampled_tables: list[str]                  # 实际采样了哪些表
    sample_timestamp: str                      # 采样时间戳
```

## 集成点

`label_validator.py` 当前逻辑：

```python
# 当前
declared_enums = _collect_declared_enums(spec, manifest)
if not declared_enums:
    return questions  # ← 静默通过——升级点
```

升级后：

```python
# 升级后
declared_enums = _collect_declared_enums(spec, manifest)
profiles = enum_profiler.profile(tables, plan)    # 采样 + 分层
detected_enums = _to_enum_map(profiles)            # 仅提取 tier ≥ MEDIUM
merged_enums = _merge_enum_sources(declared_enums, detected_enums)
# tier=CERTAIN → blocking；tier=HIGH → WARN；tier=MEDIUM → info（不阻断）
```

## 合并策略（按 tier 分层行为）

| 来源 | tier | CASE WHEN 越界时的行为 |
|------|------|----------------------|
| DeveloperSpec `enum_values` | —（等效 CERTAIN） | **blocking** OpenQuestion |
| SourceManifest `enum_values` | —（等效 CERTAIN） | **blocking** OpenQuestion |
| 自动检测 + tier=CERTAIN | Flag 必达条件全满足 | **blocking** OpenQuestion |
| 自动检测 + tier=HIGH | 模式 ≥0.9 + 字段名命中 | **WARN** OpenQuestion（不阻断） |
| 自动检测 + tier=MEDIUM | 模式 ≥0.8 + 部分信号 | **info**（仅写入 review.md） |
| 自动检测 + tier=LOW | 信号弱 | **跳过**——不参与校验 |
| 自动检测 + tier=NOT_ENUM | 未通过筛选 | **跳过** |

> 关键设计决策：**只有 CERTAIN 级别的自动检测才能阻断流水线**。这是对"置信度是否过高"的直接回应——让弱的信号只做提示，不做拦截。自动检测值**不覆盖**手动声明值。手动声明为空的字段仍以手动声明为准（开发者可能明确表示"非枚举"）。

## 边界约束

- **不触发自动检测的场景**（tier=NOT_ENUM）：
  - 目标表不存在（尚未建表）
  - 目标表为空（无数据可采样）
  - 采样查询超时（默认 30s）
  - 未通过低基数列筛选（distinct/total > 阈值）
  - 命中特殊排除规则（年份/月份/金额/数量字段）
- **降级场景**：
  - pattern_match_ratio 在边界附近 + 无字段名信号 → MEDIUM
  - 值模式混合（同时符合两个类别特征）→ 取匹配率更高的类别，降一级 tier
  - 采样数据量不足（< 100 行）→ 最高 MEDIUM
- **安全约束**：
  - 采样查询仅执行 `SELECT DISTINCT` + `LIMIT`——不触发全表扫描
  - 读权限与 Executor 共享（dry_run 隔离环境）
  - profiling 结果不写入 SourceManifest（只读参考）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| Flag 检测 | 4 | 0/1 CERTAIN、Y/N CERTAIN、distinct=3 → NOT_ENUM、字段名无信号+distinct=2 → 降级 |
| Status 检测 | 4 | 标准英文短语 HIGH、多词空格分隔、带连字符、distinct 超 30 → NOT_ENUM |
| Code 检测 | 4 | 纯数字 Code HIGH、字母缩写 Code HIGH、年份列 → 排除、金额列 → 排除 |
| 分层判定 | 4 | pattern≥0.9+字段名信号 → HIGH、pattern≥0.8+无信号 → MEDIUM、pattern<0.6 → NOT_ENUM、Flag CERTAIN |
| 边界条件 | 4 | 空表跳过、全 NULL 列、单值列、采样 <100 行 → 最高 MEDIUM |
| 合并策略 | 3 | CERTAIN 自动检测 → blocking、HIGH → WARN、MEDIUM → info（不阻断） |
| 排除规则 | 2 | 字段名含 year/month → NOT_ENUM、字段名含 amount/price → NOT_ENUM |

## 必须运行的检查

```bash
python -m pytest tests/profiling/ tests/labels/ -q -k "enum"
python -m ruff check src/tianshu_datadev/profiling/ src/tianshu_datadev/validation/
git diff --check
```

## B/C 暂停条件

- 自动检测误判率过高（Flag/Status/Code 分类与实际业务含义矛盾）→ 需要人工标注介入
- 目标表采样查询在生产环境中性能不可接受 → 需要 snapshot 预计算
- 合并策略（手动 vs 自动优先级）与团队 Code Review 流程冲突

## 退出条件

1. Flag/Status/Code 三类检测 + 排除规则正确（25 个测试场景）
2. 分层判定：CERTAIN/HIGH/MEDIUM/LOW/NOT_ENUM 五层边界准确
3. 合并策略：CERTAIN→blocking, HIGH→WARN, MEDIUM→info——各层行为正确
4. 特殊排除：年份/月份/金额/数量字段不被误判为 Code
5. 空表/全 NULL/单值/采样不足边界正确处理
6. Phase 1A-3B 测试保持通过

---

> Phase 3B.1 | **已完成** | 测试融合于 tests/labels/test_label_rules.py（13 新增） | 1123 全量通过
