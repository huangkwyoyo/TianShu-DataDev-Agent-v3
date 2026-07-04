# 业务集成执行第一轮 —— C1 点亮 + C2 接入方案启动

> **日期：** 2026-07-04 | **状态：** ✅ 已完成
> **来源：** `02-business-integration-prep.md` 中定义的 C1/C2 验收路径
> **下一文档：** `04-c2-provider-adapter-plan.md`（C2 详细实施方案，待执行）

**目标：** 在真实 PySpark 环境中点亮全部 11 个双引擎物理验证用例（C1），并制定 LLM ProviderAdapter 接入实施方案（C2）。

**基线：** 521 passed, 11 skipped, ruff clean, R3 已修复, C1-C4 验收路径矩阵已定义。

---

## 全局约束

- **允许**：C1 真实 Spark 点亮——`pytest --run-slow` 全部 11 个用例
- **允许**：更新 `docs/risks/phase-6-8-known-risks.md`，C1 从"未点亮"更新为"已点亮"
- **允许**：处理 PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON 可复现性问题
- **允许**：制定 C2 ProviderAdapter 接入实施方案（独立方案书）
- **禁止**：接入真实 LLM（C2 只制定方案）
- **禁止**：接入生产数据、生产库
- **禁止**：绕过 Validator / Comparator / Executor
- **禁止**：改变 SQL/Spark 安全边界、Schema/Memory/Prompt 机制
- **C3 / C4**：只保持登记，不在本轮实现

---

## 能力清单

### ✅ 本轮之前已完成

| 能力 | 证据 |
|------|------|
| R3 Mapper input_alias 修复 | `test_real_contract_e2e_mapper_compiler_validator` PASSED |
| Orchestrator 骨架 E2E | 28 个 orchestrator 测试全绿 |
| C1-C4 验收路径矩阵 | `business-integration-prep.md` §2.1-2.6 |
| 全量回归 | 521 passed, 11 skipped |

### ✅ 本轮完成

| 项目 | 类型 | 证据 |
|------|------|------|
| C1 环境验证 | A-执行 | PySpark 4.1.2 + Python 3.12.10 + Java OpenJDK |
| C1 物理验证点亮 | A-执行 | 69/69 passed（含 11/11 `TestRealSparkExecution`） |
| PYSPARK_PYTHON 可复现性 | A-执行 | 警告原因 + 修复方式已记录 |
| C2 ProviderAdapter 方案 | A-方案 | `c2-provider-adapter-plan.md`——4 个 Task + 验收路径 |
| 风险文档同步 | A-文档 | C1→已消除，C2→方案已定，风险矩阵更新 |

### ❌ 本轮不做

| 项目 | 原因 |
|------|------|
| C2 方案实现（Task 1-4） | 本轮只制定方案——实现见 `c2-provider-adapter-plan.md` |
| C2 真实 LLM 验证 | 需 API key——由业务方提供时执行 |
| C3 Comparator 实现 | 需 SQL pipeline 先就绪 |
| C4 Harness 样本 | 需业务方提供 |

---

## 执行记录

### Task 1: C1 环境验证 + 物理验证点亮

#### Step 1: 环境检查

```bash
python -c "from pyspark.sql import SparkSession; spark = ...; print('SPARK_OK', spark.version); spark.stop()"
```

**结果：**

| 组件 | 版本 | 状态 |
|------|------|------|
| PySpark | 4.1.2 | ✅ ≥3.3.0 |
| Python | 3.12.10 | ✅ ≥3.10 |
| Java | OpenJDK 64-Bit (build 25.442-b08) | ✅ ≥8 |
| SparkSession | `local[1]` 模式 | ✅ 创建/销毁正常 |

#### Step 2: PYSPARK_PYTHON 可复现性

**问题：** PySpark 启动时查找 `python3` 未找到（Windows 上仅有 `python`），输出警告但不影响功能。

**修复：**

```
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python
```

或在命令行中内联：`PYSPARK_PYTHON=python PYSPARK_DRIVER_PYTHON=python pytest ...`

#### Step 3: 物理验证点亮

```bash
python -m pytest tests/spark/test_physical_verifier.py -v --run-slow --tb=short
```

**结果：69/69 passed**

| 分组 | 用例数 | 结果 |
|------|--------|------|
| TestResultCanonicalizer | 8 | ✅ 全绿 |
| TestDuckDBExecution | 3 | ✅ 全绿 |
| TestDuckDBSecurity | 24 | ✅ 全绿 |
| TestPhysicalVerifierWithMock | 7 | ✅ 全绿 |
| TestWindowPhysicalVerification | 3 | ✅ 全绿 |
| TestPhysicalVerificationReport | 5 | ✅ 全绿 |
| **TestRealSparkExecution** | **11** | ✅ **全绿（首次点亮！）** |

**11 个双引擎对比参数：**

| # | 用例 | DuckDB ↔ PySpark |
|---|------|:---:|
| 1 | scan | ✅ 一致 |
| 2 | filter | ✅ 一致 |
| 3 | project | ✅ 一致 |
| 4 | sort | ✅ 一致 |
| 5 | limit | ✅ 一致 |
| 6 | aggregate | ✅ 一致 |
| 7 | join | ✅ 一致 |
| 8 | case_when | ✅ 一致 |
| 9 | window_row_number | ✅ 一致 |
| 10 | window_sum_over | ✅ 一致 |
| 11 | window_rank | ✅ 一致 |

### Task 2: C2 ProviderAdapter 接入方案制定

输出文件：`docs/superpowers/plans/04-c2-provider-adapter-plan.md`

方案包含 4 个 Task：

| Task | 内容 | 文件 |
|------|------|------|
| Task 1 | ProviderAdapter 基类定义（协议 + 配置 + 错误类型） | `provider_adapter.py`（新建） |
| Task 2 | AnthropicAdapter 实现（StructuredOutput） | `adapter_anthropic.py`（新建） |
| Task 3 | SparkDeveloperService 集成 + 重试逻辑 | `developer.py`（修改） |
| Task 4 | ProviderAdapter 集成测试（mock 路径） | `test_spark_developer.py`（修改） |

### Task 3: 风险文档同步

修改文件：`docs/risks/phase-6-8-known-risks.md`

- **C1**：风险等级从"C-环境依赖"更新为"已消除"，记录 PySpark 4.1.2 / 11/11 通过 / PYSPARK_PYTHON 可复现性
- **C2**：风险等级从"C-延期"更新为"C-延期（方案已定）"，链接到 C2 ProviderAdapter 方案文档
- **风险矩阵**：C1 行更新为"已消除"，C2 行更新为"方案已定，待实现"

---

## 验收证据（全量汇总）

```
✅ C1 环境：SPARK_OK 4.1.2 (PySpark 4.1.2, Python 3.12.10, Java OpenJDK)
✅ C1 物理验证：69/69 passed
   - TestRealSparkExecution 11/11：scan/filter/project/sort/limit/aggregate/join/
     case_when/window_row_number/window_sum_over/window_rank 全部一致
✅ Orchestrator：28/28 passed
✅ Mapper：13/13 passed
✅ 全量回归：521 passed, 11 skipped，零退化
✅ Ruff：All checks passed!
✅ Git diff --check：clean（仅 CRLF 警告）
```

**验收命令（可复现）：**

```bash
# 1. C1 环境验证
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('env_check').master('local[1]').getOrCreate(); print('SPARK_OK', spark.version); spark.stop()"

# 2. C1 物理验证
python -m pytest tests/spark/test_physical_verifier.py -v --run-slow --tb=short

# 3. Orchestrator + Mapper
python -m pytest tests/spark/test_orchestrator.py tests/spark/test_spark_plan.py::TestSparkPlanMapper -v --tb=short

# 4. 全量回归
python -m pytest tests/spark/ tests/artifacts/ -q

# 5. Lint
python -m ruff check src/tianshu_datadev/spark/ tests/spark/ docs/risks/

# 6. Git diff
git diff --check
```

---

## 退出标准

- [x] C1 环境验证通过——PySpark 4.1.2 + Python 3.12.10 + Java ≥8
- [x] C1 物理验证 11/11 全部 PASS——双引擎（DuckDB ↔ PySpark）一致性 100%
- [x] PYSPARK_PYTHON 可复现性问题已记录并修复
- [x] C2 ProviderAdapter 接入方案已输出——4 个 Task + 验收路径
- [x] 风险文档 C1/C2 状态已同步
- [x] 全量回归 521 passed, 11 skipped，零退化
- [x] ruff 零告警

---

## 是否可继续 C2？

**是。** 满足以下全部条件：

1. ✅ C1 已完全点亮——11/11 真实 Spark 双引擎物理验证通过
2. ✅ C2 接入方案已制定——`c2-provider-adapter-plan.md` 含 4 个 Task + 完整验收路径
3. ⚠️ C2 实现（Task 1-4）是纯代码工作——ProviderAdapter 基类 + AnthropicAdapter + 集成测试，不依赖外部环境
4. ⚠️ C2 真实 LLM 验证需要 API key——由业务方提供时执行 `@pytest.mark.llm` 标记测试

**建议下一轮执行顺序：**
1. C2 Task 1-4 实现（ProviderAdapter 基类 → AnthropicAdapter → 集成 → 测试）
2. C2 mock 路径全通后，等待 API key 点亮真实 LLM 验证
3. C3/C4 继续等待——C3 需 SQL pipeline 就绪，C4 需业务样本

---

## A/B/C 分类汇总

| 分类 | 内容 | 处置 |
|------|------|------|
| **A（本轮完成）** | C1 真实 Spark 物理验证点亮 | 11/11 通过，Spark 4.1.2，双引擎一致性 100% |
| **A（本轮完成）** | C2 ProviderAdapter 接入方案制定 | 输出 `c2-provider-adapter-plan.md`（4 个 Task） |
| **A（本轮完成）** | 风险文档 C1/C2 状态同步 | `phase-6-8-known-risks.md` 已更新 |
| **A（本轮完成）** | PYSPARK_PYTHON 可复现性处理 | 警告原因 + 修复方式已记录 |
| **A（本轮完成）** | 本轮执行方案书 | 本文档 |
| **B（下一轮）** | C2 ProviderAdapter 实现 | Task 1-4——纯代码，不依赖外部环境 |
| **B（待 API key）** | C2 真实 LLM 验证 | 需 Anthropic API key |
| **C（不实现）** | C3 Comparator 真实链路 | 阻塞于 SQL pipeline |
| **C（不实现）** | C4 Harness 业务样本 | 阻塞于业务方 |

---

## 修改范围

| 文件 | 操作 | 说明 |
|------|------|------|
| `docs/risks/phase-6-8-known-risks.md` | 修改 | C1 → 已点亮，C2 → 方案已定，风险矩阵更新 |
| `docs/superpowers/plans/04-c2-provider-adapter-plan.md` | **新建** | C2 ProviderAdapter 详细实施方案（待执行） |
| `docs/superpowers/plans/03-business-integration-round1.md` | **新建** | 本文档——本轮执行方案书 |

**本轮无代码修改**（仅文档层面）——C1 点亮是环境验证，C2 方案是规划输出。

---

## 残留风险

| 风险 | 等级 | 说明 |
|------|------|------|
| C2 方案未实现 | B-待执行 | ProviderAdapter 4 个 Task 已定义，需下一轮执行 |
| C2 真实 LLM 验证 | C-环境依赖 | 需 Anthropic API key（由业务方提供） |
| C3 Comparator | 骨架级 | 阻塞于 SQL pipeline 就绪状态——不在本项目范围内 |
| C4 Harness 样本 | C-延期 | 阻塞于业务方提供样本 |

---

## 非技术人员解释

**这轮做了什么？**

上轮的"通电指南"画好了每台机器的接线图。这轮就是真的去通电。

**第一台机器（C1）已通电并跑完全程。** 之前这台双引擎对比机的 11 个测试全标着"跳过——没电"。现在接上电（PySpark），11 个全亮绿灯。这意味着：从一份数据加工合同出发，系统能自动生成 PySpark 代码，在真实 Spark 引擎上跑出结果，并且和另一套独立的验证引擎（DuckDB）逐行对比——11 种数据处理操作（读数据、过滤、投影、排序、截断、聚合、关联、分类打标、三种窗口排名）全部一致。这是整个 Phase 6-8 的一个里程碑——证明了系统生成的代码不仅语法正确，执行结果也是正确的。

**第二台机器（C2）的接线图画好了。** AI 大脑的接口已经有了，现在画好了"电源适配器"的设计图——包含适配器规格（基类）、Claude 专属插头（AnthropicAdapter）、断电重连机制（重试逻辑）。下一次来就是按图纸把适配器装上去——纯代码活，不需要额外材料。装好后只要插上 API key 就能用。

**第三台（C3）和第四台（C4）还是老样子，等隔壁车间完工和业务方送样本。**

**现在整个项目的状态：**
- 整条流水线的"骨架"（零件 → 组装 → 质检）已经跑通
- 最难的"双引擎验证"也证明是可靠的（11/11 全部一致）
- 只差一个 AI 大脑接上真正的电源——图纸已画好，下次来装

---

## 后续更新（2026-07-04）

### C2 已完成（2026-07-04）

本文档 §Task 2 中制定的 C2 ProviderAdapter 接入方案已被**方案 A（完全合并）**替代：

- **原方案**（`04-c2-provider-adapter-plan.md`）：新建 `spark/provider_adapter.py` + `spark/adapter_anthropic.py`
- **实际执行**（`05-c2-llm-boundary-consolidation.md`）：删除重复文件，复用既有 `llm/adapters/base.py::ProviderAdapter` + `PromptManager`
- **原因**：C2 初始实现引入了与既有 LLM 基础设施 ~80% 重复的 Spark 专用 Adapter——属 C 类架构违规，需合并而非新建
- **完成状态**：
  - ✅ 重复文件已删除：`spark/provider_adapter.py`、`spark/adapter_anthropic.py`
  - ✅ 版本化 Prompt 模板：`prompts/templates/spark_annotator/v001.md`
  - ✅ 循环导入修复：`llm/gateway.py` → `TYPE_CHECKING` 延迟导入
  - ✅ 结构化路径 18/18 测试全绿（mock 可回归）
  - ⚠️ 真实 LLM 开发环境一次性验证通过（DeepSeek `deepseek-v4-pro` 3/3）——生产环境持续验证待 API key

### 原 §"是否可继续 C2？" 更新

- ~~C2 Task 1-4 实现~~ → **已完成**（通过方案 A 完全合并）
- ~~C2 mock 路径全通后，等待 API key~~ → **mock 路径已全通**，真实 LLM 开发环境验证已通过
- **当前状态**：C2 全部阻塞项已消除，可进入 C3/C4 评估

### 原 §残留风险 更新

| 风险 | 原状态 | 当前状态 |
|------|--------|---------|
| C2 方案未实现 | B-待执行 | ✅ 已完成（通过方案 A） |
| C2 真实 LLM 验证 | C-环境依赖 | ⚠️ 开发环境已验证，生产环境待 API key |
