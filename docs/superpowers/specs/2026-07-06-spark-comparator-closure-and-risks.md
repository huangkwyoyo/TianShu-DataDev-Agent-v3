# Spark Comparator 内容级对齐——闭环报告与剩余风险

> 日期：2026-07-06 | 状态：闭环确认，合并就绪

## 一、闭环结论

Case 06（NYC 区域安全合规画像）双管线闭环已达成：

```
SQL SqlProgram/SqlBuildPlan ──► PlanComparator.compare_program() ──► LOGIC_EQUIVALENT
Spark Contract → Mapper → SparkPlan ──┘
```

**核心测试基线**：659 passed, 11 skipped, 0 failed（spark + artifacts + api），ruff clean。

## 二、架构边界——已守住

本轮修复未跨越以下架构红线：

| 边界 | 状态 | 说明 |
|------|:--:|------|
| Contract 不承载 `_temp_*` 内部实现细节 | ✅ | `extract_v1` 在 ScanStep 分支过滤 `_temp_` 前缀表，DAG 管道信息不泄漏到下游 |
| Spark Mapper 不模拟 SQL 临时表 DAG | ✅ | Mapper 仅消费 Contract 的业务语义（grouping_keys、join_relationships），不感知 `_temp_` |
| `compare()` 单 plan 路径零退化 | ✅ | 所有 `target_grain` 相关逻辑均 gated 在 `is not None` 后；`_normalize_dag_steps(target_grain=None)` 保留 grain-aware 合并行为 |
| 不修改 SqlProgram IR Schema | ✅ | 所有变更在提取器/比较器/编排器/编译器层 |
| 不引入新 step 类型 | ✅ | COMPLEX_RAW 是 `CaseWhenCondition.operator` 的哨兵值，非新 step |

## 三、Comparator 修复方向

修复集中在 `compare_program()` 多语句路径，策略为"**三层剥离 + 粒度对齐**"：

1. **_temp_\* 内部步骤剥离**（`_flatten_sql_program_steps`）：过滤 `_temp_*` scan + `_temp_*` join——这些是 DAG 管道实现细节，Spark 侧无对应物
2. **Grain-aware aggregate 合并**（`_normalize_dag_steps`）：按 `group_keys` 签名分组合并——同粒度合并 metrics，不同粒度独立保留
3. **target_grain 过滤**（`_normalize_dag_steps` + `compare_program` + Orchestrator）：从 Contract.grouping_keys 提取目标粒度，过滤非业务粒度的中间 aggregate

**单 plan 路径**（`compare()`）完全未触及。

## 四、剩余风险——非合并阻塞项

以下风险已确认存在，但不阻塞当前分支合并。需在后续迭代中处置。

### R-CA-1：target_grain 过滤不是通用业务真理

**等级**：C（架构风险）

**说明**：`target_grain` 过滤基于一个隐含假设——DAG 中所有非目标粒度的 aggregate 都是内部实现细节。这对 Case 06 成立（`[violation_county]` 是中间步骤，`[borough]` 是最终输出），但**不能推广为通用规则**：

- 多输出粒度场景（一个 DAG 产出多张不同粒度的结果表）会被错误裁剪
- `set(gk) == target_set` 的精确匹配 + 子集回退合并策略是 Case 06 特化的，未经过多粒度场景验证
- 未来若 Contract 支持 `target_grains: list[list[str]]`（多输出粒度），当前单粒度逻辑需重构

**处置**：后续 Phase 将 `target_grain: list[str]` 扩展为 `target_grains: list[list[str]]`，或在 `_normalize_dag_steps` 的 docstring 中明确标注当前适用边界。

**⚠️ 关键约束**：任何人在阅读此代码时，**不得将 target_grain 的单粒度过滤逻辑理解为通用业务真理**。它是对 Case 06 场景的正确特化，不是对任意 DAG 的通用解。

### R-CA-2：`_temp_` 前缀依赖

**等级**：B（设计需确认）

**说明**：`_temp_` 前缀是 DAG 内部管道的充分非必要条件。若未来临时表命名规则变化（如改为 `__pipeline__` 前缀），三处过滤逻辑需同步更新：
- `_flatten_sql_program_steps`（plan_comparator.py）—— scan + join 过滤
- `extract_v1`（contract_extractor.py）—— scan + join 过滤

**处置**：测试覆盖了 `_temp_` 前缀过滤行为，改名会触发测试失败。建议未来提取共享的 `_is_temp_table(name: str) -> bool` 谓词函数。

### R-CA-3：Builder 缺 join（中高）

**等级**：**中高**（2026-07-06 升级）

**说明**：Case 06 Step 4（all_three_join）的 join step 在 builder 输出中缺失。这是 builder 的 join 生成逻辑缺陷，非 Comparator 问题。当前 Case 06 不影响对齐，但新 case 中可能暴露导致业务语义不完整。

**处置**：**B 类设计修复项**——独立排查 builder 的 join 生成逻辑，建立单独的修复任务。不阻塞本 Phase 合并，但应在下一轮迭代中优先处理。

### R-CA-4：target_grain 子集合并假设

**等级**：Minor（未来注意事项）

**说明**：当无 aggregate 精确匹配 `target_grain` 时，代码回退为合并所有 aggregate 组。这假设所有 aggregate 粒度都是 `target_grain` 的子集。如果存在无关粒度（如 `target_grain=["borough"]` 但 DAG 中有 `["department"]` 粒度的聚合），会被错误合并。

**处置**：当前对 Case 06 成立。未来若引入无关中间聚合，需在合并前增加粒度相关性校验。

## 五、后续行动项

| 优先级 | 行动 | 类型 |
|:--:|------|:--:|
| P0 | 合并当前分支到 main | 立即 |
| P1 | 排查 builder join 生成缺陷（R-CA-3） | B 类设计修复 |
| P2 | target_grain 扩展为 target_grains（R-CA-1） | 后续 Phase |
| P3 | 提取共享 `_is_temp_table()` 谓词（R-CA-2） | 重构 |
| P4 | target_grain 子集合并增加粒度相关性校验（R-CA-4） | 增强 |

## 六、设计文档同步

本报告是对 `2026-07-06-spark-comparator-content-alignment-design.md` 的闭环补充。设计文档中的 Step 0→A→C→B1→B2→D 已全部执行完毕，残留风险表（第八章）已更新。
