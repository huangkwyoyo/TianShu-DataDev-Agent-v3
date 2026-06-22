# 交叉验证和修复计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

通过确定性比较 SQL 和 PySpark 两路执行结果，验证代码正确性。所有 PASS 判定由机器完成，LLM 仅参与差异解释环节。

## 2. 九个确定性比较维度

Comparator 从 9 个维度比较 SQL 和 PySpark 的执行结果：

| # | 维度 | 说明 | 比较方式 |
|---|------|------|----------|
| 1 | 列名 | 两结果的列名集合是否一致 | 集合比较 |
| 2 | 数据类型 | 每列的数据类型是否一致 | Schema 比对 |
| 3 | 行数 | 总行数是否一致 | 精确数值比较 |
| 4 | 业务键集合 | 根据 Key 列比较 ID 集合 | 集合差集 |
| 5 | 规范化排序行 | 按 Key 排序后逐行比较 | 逐行精确比较 |
| 6 | 空值数量 | 每列空值数量是否一致 | 数值比较 |
| 7 | 数值列汇总 | SUM / AVG 聚合值是否一致 | 容差比较 |
| 8 | 容差 | 允许浮点误差（如 1e-6） | 相对误差 |
| 9 | 内容摘要哈希 | 整体内容的 SHA-256 摘要 | 哈希值比较 |

### 2.1 Comparator 契约

```python
@dataclass
class ComparisonResult:
    """9 个维度的比较结果"""
    column_names_match: bool
    data_types_match: bool
    row_count_match: bool
    key_set_match: bool
    sorted_rows_match: bool
    null_counts_match: bool
    numeric_summary_match: bool
    tolerance_passed: bool
    content_hash_match: bool
    details: dict                     # 每个维度的详细信息
    overall_pass: bool                # 所有维度都通过 → True
```

**核心约束**：`overall_pass` 仅由 Comparator 的确定性逻辑判定，LLM 无权修改。

## 3. 同源 Parquet 快照机制

### 3.1 快照构建流程

```
Snapshot Builder
  │
  ├── 读取 TianShu 表数据
  ├── 写入 Parquet 文件
  ├── 生成 schema.json（字段名 + 类型 + 描述）
  ├── 生成 manifest.yml（来源表、时间戳、版本）
  └── 计算 SHA-256 校验和
```

### 3.2 快照不可变性

- 在一个推理周期内，快照一旦生成不再修改
- SQL 和 PySpark 使用同一快照路径
- 快照的 manifest.yml 记录在 Code Review Package 中

### 3.3 快照目录结构

```
snapshots/{session_id}/
├── data/
│   ├── table_a.parquet
│   └── table_b.parquet
├── schema.json
├── manifest.yml
└── checksum.sha256
```

## 4. RepairDirective 五个目标

当交叉验证失败时，RepairPlanner（LLM）生成 RepairDirective：

```python
@dataclass
class RepairDirective:
    """修复指令"""
    target: str                       # 修复目标：SQL_PLAN / SPARK_CODE / BOTH / REQUIREMENT / HUMAN_REVIEW
    description: str                  # 修复描述
    changes: list[dict]               # 具体变更
    rationale: str                    # 变更理由
```

| 目标值 | 含义 | 触发条件 |
|--------|------|----------|
| SQL_PLAN | 修改 SQLPlan | SQL 分支结果有明显错误 |
| SPARK_CODE | 修改 PySpark 代码 | Spark 分支结果有明显错误 |
| BOTH | 同时修改两者 | 需要协调修改 |
| REQUIREMENT | 回退修改需求 | 需求理解错误 |
| HUMAN_REVIEW | 无法自动修复 | 多次返工仍不通过，需人工介入 |

## 5. PASS 判定规则

```
只有 Deterministic Comparator 可以说 PASS
LLM DifferenceAnalyst 只能说 "我发现了这些差异，可能的原因是..."
```

**流程**：
1. Comparator 判定 9 个维度，输出 `overall_pass`
2. 如果 PASS → 直接进入 Code Review Package
3. 如果 FAIL → 调用 DifferenceAnalyst（LLM）做差异诊断
4. DifferenceAnalyst 输出诊断文本（非判定）
5. RepairPlanner 根据诊断生成 RepairDirective
6. 返回双分支重新执行
7. 最多 2 轮自动返工
8. 第 3 轮仍 FAIL → 自动标记 HUMAN_REVIEW

## 6. 返工上限

| 轮次 | 动作 |
|------|------|
| 第 1 次 FAIL | 正常返工，写入 Run Memory |
| 第 2 次 FAIL | 正常返工，写入 Run Memory |
| 第 3 次 FAIL | 标记 HUMAN_REVIEW，差异诊断和修复记录一并打包 |

## 7. 不做什么

- LLM 不决定 PASS/FAIL
- Comparator 不做模糊匹配（除非浮点容差）
- 不比较执行时间或性能
- 不比较中间结果（只比较最终输出）

## 8. 测试边界

| 测试类型 | 覆盖内容 |
|----------|----------|
| 完全一致 | 两路结果完全相同 → PASS |
| 列名不一致 | 列名不同 → FAIL + 明确差异 |
| 行数不一致 | 行数不同 → FAIL + 具体差值 |
| 浮点误差 | 在容差范围内的差异 → PASS |
| 空值不一致 | 空值数量不同 → FAIL |
| 哈希一致性 | 重复验证相同结果 → PASS |

## 9. 风险

| 风险 | 缓解 |
|------|------|
| 数据源不一致导致误判 | 同源快照保证数据一致 |
| 浮点误差积累 | 容差机制 + 合理阈值 |
| 大结果集比较性能 | 支持采样比较策略 |
| LLM 诊断误导修复方向 | 返工上限 + HUMAN_REVIEW 兜底 |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
