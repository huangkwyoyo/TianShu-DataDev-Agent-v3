# 物理验证诊断监控引擎——可行性、意义与边界分析

> 分析日期：2026-07-15
> 分析依据：`/superpowers:brainstorming` 指令，针对之前讨论的 `PhysVerDiagnosticEngine` 方案

---

## 一、项目现有监控基础设施

### 1.1 `monitor/` 模块（已成熟）

```python
src/tianshu_datadev/monitor/
├── __init__.py          # 导出 RunLogCollector, StageContext, get_collector
├── models.py            # StageEvent, HttpEvent, BrowserEvent, ResourceSample
├── collector.py         # RunLogCollector（队列+单线程写入）、StageContext（上下文管理器）
├── renderer.py          # LogRenderer（事件格式化）
├── sanitizer.py         # Sanitizer（敏感信息清洗）
├── rotation.py          # 按 run_id 分组轮转，保留最近 50 组
├── lifespan.py          # FastAPI lifespan 集成
├── middleware.py        # HTTP 中间件
└── resource_sampler.py  # 系统资源采样
```

特点：
- **无 TIANSHU_RUN_ID 时零开销**（`NullCollector`）
- **线程安全**——queue.Queue + 单消费者线程写 JSONL
- **自动轮转**——`rotation.cleanup()` 保留最近 50 组，保护当前 run
- **PII 清洗**——`Sanitizer` 排除所有敏感字段

### 1.2 Pipeline 阶段监控

`pipeline.py` 已有约 **50+ 处** `collector.stage()` 调用，覆盖：

```
sql_parser → sql_enricher → sql_builder → sql_validator →
sql_compiler → sql_executor → contract_extractor →
snapshot_builder → packager
```

每个阶段记录：`started` → `completed/failed`，含 `duration_ms`、`row_count`、`error_message`。

### 1.3 物理验证器已有诊断功能

`physical_verifier.py` 中已有：

| 组件 | 位置 | 说明 |
|------|------|------|
| **诊断 JSON 保存** | L578-608 | 无条件写入 `_diag_physver_pyspark.py`、`_diag_physver_sql.txt`、`_diag_physver_spark_rows.json`、`_diag_physver_duckdb_rows.json` |
| **CDP Shadow** | L717-733 | `_run_cdp_shadow()`——双引擎 CDP 摘要对比，shadow 模式 |
| **CRE Shadow** | L856-867 | `_shadow_cre_diagnose()`——基于 Contract primary_keys 的行级比较，shadow 模式 |

---

## 二、当前诊断代码的问题

### 问题 1：硬编码绝对路径

```python
# L594 — 在别人的机器上直接崩溃或写错位置
_diag_base = r"D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3\_diag_physver"
```

项目目录在不同计算机上路径不同，此路径不可移植。

### 问题 2：无条件写入

每轮验证无论结果一致与否都写文件。Spark 行数据 JSON 可能很大（数万行）。

### 问题 3：Decimal 序列化静默失败

```python
# L603-605 — Decimal 类型不是 JSON 可序列化的
json.dump(duckdb_result.output_rows, _f)  # 抛 TypeError → 被 L607 的 except 吞掉
# 结果：duckdb_rows.json 只有 97 字节的截断文件
```

### 问题 4：无轮转清理

`_diag_physver_*` 文件每次运行覆盖，但如果多个并发运行则互相覆盖。也不属于任何日志轮转策略。

### 问题 5：未与 monitor 系统集成

诊断文件写到了项目根目录，而非 `logs/monitor/`，不属于监控系统的管理范围。

---

## 三、方案可行性评估

### 3.1 技术可行性：✅ 可行

**理由**：
- 双引擎行数据已经在 `verify()` 方法的 `duckdb_result.output_rows` 和 `spark_result.output_rows` 中——无需重新执行 SQL/Spark
- CRE Shadow 已经实现了行级对齐和比较（`KeyBasedRowAligner`、`ToleranceComparator`），可作为分析引擎的基础
- 监控系统已有完整的文件写入、轮转、PII 清洗管道
- 当前诊断代码本身已经是 shadow 模式（try/except 包裹），增加结构化分析是增量式增强而非从零开始

### 3.2 典型场景覆盖

| 差异模式 | 检测可行性 | 已具备的基础 |
|----------|-----------|-------------|
| **时区边界偏移**（72vs63 案例） | ✅ 高——分 group_keys 后按日期列看一方多出行 | JSON 行数据已有 date_key 列 |
| **浮点精度差异** | ✅ 高——CRE ToleranceComparator 已有 | CRE shadow 已实现 |
| **NULL 处理差异** | ✅ 中——对比列值，一方 NULL 一方有值 | 行对齐后可直接逐列对比 |
| **类型转换差异** | ✅ 中——同排序列对比值表现形式 | 需要 Schema 信息 |
| **缺少/多余行** | ✅ 高——排序键对齐后 count 不等 | CRE 的 KeyBasedRowAligner 已有 |

### 3.3 与已有能力的重叠

```
CRE Shadow 已有          诊断引擎新增
─────────────────        ────────────────
行级对齐（KeyBasedRowAligner）   ✗ 无需重复
容差比较（ToleranceComparator）   ✗ 无需重复
分桶摘要（BucketHasher）         ✗ 无需重复
                              ✓ 差异模式分类（时区/精度/NULL/类型）
                              ✓ 人类可读摘要报告（summary.md）
                              ✓ 结构化 analysis.json 输出
                              ✓ 与 monitor 模块的轮转集成
```

核心增量不是"重新对比"，而是**对对比结果做根因分类**。

---

## 四、性能影响评估

### 4.1 当前代码（改造前）的性能开销

| 操作 | 开销 | 触发条件 |
|------|------|---------|
| 写 `_diag_physver_pyspark.py` | ~1KB I/O | **每次验证** |
| 写 `_diag_physver_sql.txt` | ~1KB I/O | **每次验证** |
| 写 `_diag_physver_spark_rows.json` | ~9KB I/O（72行时） | **每次验证** |
| 写 `_diag_physver_duckdb_rows.json` | ~100B I/O（失败截断） | **每次验证** |

**当前代码已在每次验证中执行 I/O**，但 DuckDB 行 JSON 写入因 Decimal bug 静默失败。

### 4.2 改造后预估开销

| 操作 | 开销 | 条件 |
|------|------|------|
| 写 pyspark/sql 文件 | 同上 | 仅 RESULT_MISMATCH |
| 写双引擎行 JSON | 同上（Decimal bug 修复后正常） | 仅 RESULT_MISMATCH |
| 差异分析（模式分类） | O(N) 内存计算，N = 行数 | 仅 RESULT_MISMATCH |
| 写 analysis.json | 数 KB 级别 | 仅 RESULT_MISMATCH |
| 写 summary.md | 数 KB 级别 | 仅 RESULT_MISMATCH |

**结论**：改造后性能优于现状——因为当前是无条件写入，改造后是**仅当 RESULT_MISMATCH 时触发**。正常通过验证（绝大多数情况）的性能开销为零（或仅限于替换当前无条件写入的那几行）。

### 4.3 对比 CRE Shadow 的开销

CRE Shadow 已在每次验证中运行完整行对齐和比较，其开销远大于诊断引擎的格式化和分类。诊断引擎在 CRE Shadow 之上添加的是轻量级的模式匹配逻辑。

---

## 五、架构边界风险评估

### 5.1 CRCS v2.0 分类

**→ A类（AUTO-FIX）**

| 检查项 | 结论 |
|--------|------|
| 是否改变验证逻辑/结果判定？ | ❌ 不改变——shadow 模式，异常安全 |
| 是否影响 Code Generation 边界？ | ❌ 不影响 |
| 是否影响 Validator → Executor 边界？ | ❌ 不影响——验证完成后处理已产出数据 |
| 是否改变防线 2 检查项？ | ❌ 不影响 |
| 是否绕过人审闸门？ | ❌ 不影响 |
| 是否修改 Prompt/Schema/Memory？ | ❌ 不影响 |

### 5.2 边界安全设计原则

1. **Shadow 隔离**——所有异常被 try/except 捕获，不传播到主逻辑
2. **只读输入**——只读取 `duckdb_result.output_rows` 和 `spark_result.output_rows`，不修改
3. **零侵入**——不改变验证状态、报告内容、返回值
4. **现有先例**——CDP Shadow（L717-733）和 CRE Shadow（L856-867）已验证此模式安全

### 5.3 文件结构边界

```
当前结构（问题）         建议结构（改造后）
─────────────────       ────────────────
_diag_physver_*.json    取消——不再用项目根临时文件
（项目根，不在 .gitignore 中）

无专门目录              logs/monitor/diagnostics/ 下
                        按 run_id 分组轮转
                        ✓ 已在 .gitignore 中
```

---

## 六、方案建议

### 6.1 核心改动

1. **修改 `physical_verifier.py` L578-608**
   - 将硬编码路径改为与 monitor 集成的路径
   - 修复 Decimal 序列化（`json.dump(..., default=str)` 或 `default=decimal_to_float`）
   - 改为仅 RESULT_MISMATCH 时写文件

2. **创建 `diagnostic_collector.py`（或并入现有 collector）**
   - 封装诊断文件的写入逻辑
   - 与 `monitor/collector.py` 共用 paths 和 rotation

3. **差异模式分类（增量）**
   - 分析双引擎日期列的 distinct 值分布
   - 分析 NULL 值分布差异
   - 输出结构化的 `analysis.json`

### 6.2 不做的事（YAGNI）

- ❌ 不创建独立的 `PhysVerDiagnosticEngine` 类——功能合并到现有 shadow 诊断代码中
- ❌ 不创建 `diagnostics/` 新目录——复用 `logs/monitor/` 路径和轮转
- ❌ 不做全自动根因定位——只做差异模式分类，不自动修复
- ❌ 不做重复行检测——重复行应该由更上层的 Contract 去重逻辑保证

### 6.3 代码量估算（修正后）

| 模块 | 行数 | 说明 |
|------|------|------|
| 当前诊断代码改造（路径修正 + 条件化 + Decimal 修复） | ~30 行修改 | 修改现有 578-608 |
| 差异分类逻辑（简单模式匹配） | ~80 行 | 新建函数 |
| 结构化 report 输出 | ~50 行 | 2 个 JSON schema |
| 测试 | ~60 行 | 针对分类函数的单测 |
| **总计** | **~220 行** | 其中新增 ~130 行 |

### 6.4 实施风险等级

**低风险**——主要风险来自：
1. 文件路径硬编码 → 已识别，修复方案明确
2. Decimal 序列化 → 已识别，修复方案明确
3. 与现有 CRE Shadow 的功能划分 → 需设计时明确边界

---

## 七、总结

| 维度 | 评估 |
|------|------|
| **可行性** | ✅ 可行——现有数据已在内存中，无需额外引擎执行 |
| **有意义** | ✅ 中等——时区差异分类已验证有价值，但整体模式分类是增量改进 |
| **性能影响** | ✅ 正面——改造后从无条件 I/O 变为仅 RESULT_MISMATCH 时 I/O |
| **边界安全** | ✅ A类——shadow 模式，不改变主逻辑，已有 CDP/CRE Shadow 先例 |
| **代码量** | ~130 行新增，低风险 |
| **紧急程度** | 低——当前已有 CRE Shadow 做行级对比，诊断引擎只是让结果更可读 |
