# Phase 4D：Harness 七维门禁 + SQL-first v1.0 HarnessVerdict 门禁

> 状态：已实施（Phase 4D 补全——D4/D5/D7 stub 消除，全部 7 维真实判决）✅
> 前置依赖：Phase 4C 退出（攻击向量和语义错误注入全部拦截）✅

## 执行前必须阅读

1. `AGENTS.md` §9 — Harness 和 Memory（Harness 不得成为产品运行时依赖）
2. `docs/07-harness-and-memory-plan.md` — Harness 七维门禁定义
3. Phase 4A/4B/4C 全部评测报告

## 只允许修改

- `src/tianshu_datadev/harness/` — 完成 Harness Framework
  - `eval_runner.py`：全量评测执行器
  - `metrics.py`：七维门禁指标计算 + HarnessReport 生成
  - `dataset_loader.py`：golden/rejection/attack/performance/regression 数据集加载
- `harness/datasets/` — 完善五个数据集目录
- `tests/` — 新增 test_harness_gate.py

## 禁止修改

- 被测系统的任何核心逻辑——Harness 只评测，不修改
- Harness 不得成为产品运行时依赖（`src/tianshu_datadev/` 不得 import harness）
- 不得降低七维门禁的 REJECT 标准来"通过"评测

## 新增模型

### Harness 七维度门禁

| 维度 | REJECT（阻断 Phase 4 退出） | WARN / INFO |
|------|---------------------------|-------------|
| 1. 结构化约束力 | ParsedDeveloperSpec / RelationshipHypothesis / SqlBuildPlan 一次通过率 < 95%；extra 字段拒绝率 < 100% | 高频错误进入 Prompt 回归 |
| 2. Join 推理质量（零容忍） | ① 高风险错误 Join 漏报率 > 0（STRONG/MEDIUM 等级的 Join 在人工审查中确认错误即为漏报）；② **WEAK/NONE 等级的 Join 被错误采纳进入 SqlBuildPlan = REJECT**（Validator 未拦截视为 Bug）；③ 每个 Join 必须展示完整证据链，只给结论不给证据 = REJECT | MEDIUM Join 默认进入人工确认面板；低置信 Join 人工接受率进入跟踪 |
| 3. 语义正确性 | 错字段、错粒度、错聚合、错枚举全部拦截 | 合法输入误拒绝率可接受 |
| 4. 编译与执行 | 编译成功率 < 99%；执行成功率 < 95%；编译确定性 < 100% | 执行反馈进入审查包 |
| 5. 产品可用性 | review.md 人工接受率 < 阈值 | WARN 可读性经人工确认 |
| 6. 安全边界 | 注入、越权、写入越权任一漏报 | 无 |
| 7. 运行稳健性 | 连续执行无显著衰减；异常进入 HUMAN_REVIEW | 记录 token、延迟、成本 |

> **Join 推理质量是 Phase 4 退出时 7 个维度中唯一的"零容忍"维度**：第 2 维度的三条 REJECT 项有一条不通过，Phase 4 不得退出。

### 三条 REJECT 指标（维度 2）可测量定义

| # | 指标 | 测量方法 | 阈值 |
|---|------|----------|------|
| 1 | 漏报率 | 在 golden 数据集中标注所有应被推理出的 Join，统计 Planner 未输出的比例 | **= 0**（任何漏报 = REJECT） |
| 2 | WEAK/NONE 被采纳 | 扫描所有 SqlBuildPlan 的 JoinStep，回溯其 relationship_ref 在 RelationshipHypothesis 中的 level | **= 0**（任何采纳 = REJECT，Validator 未拦截视为 Bug） |
| 3 | 缺证据链 | 检查每个 Join 候选的 evidence 列表是否非空且包含至少 3 类证据的逐条结果 | **= 0**（任何缺失 = REJECT） |

### 评测数据集目录

```text
harness/datasets/
├── golden/                        # 黄金 DeveloperSpec → 预期 SqlBuildPlan
├── rejection/                     # 应被拒绝的非法输入
├── attack/                        # 六种攻击向量（Phase 4C 产出）
├── performance/                   # 15 条 PERF 规则边界
└── regression/                    # 回归用例（Phase 3 Exit + 4A/4B/4C 全部已知错误）
```

### 评测用例格式

```json
{
  "case_id": "join_fact_dim_001",
  "category": "single_join",
  "developer_spec_path": "datasets/sql_harness/specs/join_fact_dim_001.md",
  "expected": {
    "must_accept": true,
    "required_output_columns": ["user_id", "pay_amount"],
    "required_join_keys": [["orders.user_id", "users.user_id"]],
    "required_warnings": [],
    "forbidden_patterns": ["SELECT *", "CROSS JOIN"]
  },
  "attack": {
    "type": null,
    "expected_rejection_code": null
  },
  "human_review": {
    "requires_review": true,
    "review_focus": ["join_evidence", "grain"]
  }
}
```

### HarnessReport

```python
class HarnessReport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    report_id: str
    phase: str = "phase-4-exit"
    dimensions: list[DimensionResult]
    overall_verdict: str              # GO | NO_GO
    rejected_dimensions: list[str]    # REJECT 的维度列表
    warn_items: list[str]
    evaluated_at: str
    dataset_counts: dict[str, int]    # 每个数据集的评测案例数

class DimensionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dimension: int
    name: str
    verdict: str                      # PASS | REJECT | WARN
    metrics: dict[str, float]         # 可测量指标
    evidence: str                     # 证据文件引用
```

## artifact schema

- `HarnessReport` JSON（七维度逐项结果 + HarnessVerdict = GO / NO_GO）
- 评测结果数据集（每个 case 的详细结果）
- SQL-first v1.0 验收报告（供人工审查）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| 七维门禁 | 7 | 每个维度至少 1 个验证其 REJECT 条件正确触发的用例 |
| Join 零容忍 | 3 | 漏报=0、WEAK 被采纳=REJECT、缺证据链=REJECT（使用故意注入错误的 fixture） |
| HarnessReport 生成 | 2 | HarnessVerdict 正确、REJECT 维度报告完整 |
| 人工接受率 | 1 | 评测流程可执行、结果可追溯到 case_id |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "harness_gate or harness_report"
python -m ruff check src/tianshu_datadev/harness/
git diff --check
```

## B/C 暂停条件

- 七维门禁的 REJECT 阈值需要基于真实数据校准——在获得 30-50 个真实 LLM 样本前，阈值是占位值
- Join 推理质量的"零容忍"标准在真实 LLM 行为下无法达到——需评估是 Schema/Validator 不足还是 LLM 能力上限
- 人工接受率评测需要至少 3 名数据工程师参与——人力资源安排可能成为阻塞

## 退出条件（4D → Phase 5 门禁）

1. ✅ **Harness 七维度全部执行**——D1-D7 均有真实判决逻辑（D4/D5/D7 已从 stub 补全）
2. ✅ **REJECT 项全部通过**——D4 编译/执行/确定性阈值、D5 接受率/审查人数阈值、D7 异常/漂移阈值均已实现
3. ✅ **WARN 项进入审查包**——无数据时诚实返回 WARN + [stub]，不静默放行
4. ⚠️ **人工接受率评测**——阈值已实现（70%/3人），真实数据需 3+ 名数据工程师参与（见 B/C 暂停条件）
5. ✅ **SQL-first v1.0 验收报告**——HarnessReport 完整生成，含 7 维度 + dataset_counts + extra_reports
6. ⚠️ **HarnessVerdict = GO**——框架就绪，需填充真实 Compiler/人工审查/多次运行数据后产出最终 GO 判决

### Phase 4D 补全说明（2026-06-29）

原 3 个 stub 维度（D4/D5/D7）已在本轮补全：

**D4（编译与执行）**：
- 新增 `HarnessRunner.run_compiler_checks()`——集成 `DuckDbSqlCompiler` + `DuckDBExecutor`（Phase 4B 已就绪）
- `compute_dimension_4()` 支持真实 `compile_results` dict 输入
- 三重 REJECT 阈值：compile_success_rate < 99%、execute_success_rate < 95%、compile_determinism < 100%
- 无数据时返回 WARN（[stub]），有数据时执行真实判决

**D5（产品可用性）**：
- `compute_dimension_5()` 支持完整 `review_results` 输入（含 `reviewer_count`）
- 双重 REJECT 条件：审查人数 < 3（样本偏差风险）、human_acceptance_rate < 70%
- 阈值待 30-50 个真实样本后校准（见 B/C 暂停条件）

**D7（运行稳健性）**：
- `compute_dimension_7()` 实现多运行退化检测
- token 漂移检测：后半段均值 vs 前半段均值 > 50% → REJECT
- 延迟漂移检测：后半段均值 vs 前半段均值 > 100%（2x）→ REJECT
- 异常运行排除在趋势计算外，需 >= 3 次正常运行才触发趋势检测

### 测试覆盖

- `tests/harness/test_harness_gate.py` — 54 测试（38 原有 + 16 新增）
  - `TestDimension4CompileAndExecute` — 6 测试（stub/编译率/执行率/确定性/PASS/零plan）
  - `TestDimension7OperationalRobustness` — 7 测试（stub/空/异常/token漂移/延迟漂移/PASS/排除异常/少次跳过）
  - `TestHumanAcceptanceFlow` — 3 新增（接受率REJECT/审查人数REJECT/PASS）
- 全量 harness 测试：99 passed
- 全量项目测试（排除 CLI）：1203 passed

---

> Phase 4D | 已实施 + 补全 ✅ | D4/D5/D7 stub 消除，全部 7 维真实判决 | 下一阶段：Phase 5 或 Phase 4.5 Internal Workbench
