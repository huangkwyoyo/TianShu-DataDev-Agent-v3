# 业务集成前置准备 实施方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> **状态：** ✅ 已完成（2026-07-04）。本方案中定义的 C1/C2 验收路径已在业务集成执行第一轮中执行并产出结果，详见 `business-integration-round1.md`。

**目标：** 完成 R3 状态同步（A 类小修）+ 制定 C1-C4 验收路径与前置条件清单，使项目具备进入业务集成执行阶段的所有信息前提。

**架构：** 本轮不写新功能代码——Task 1 做文档级别的 R3 已修复同步，Task 2 输出一个结构化的"业务集成验收路径矩阵"，覆盖目标环境、数据源、Spark 点亮、LLM 接入、Comparator 串联、Harness 样本 6 条路径。每条路径定义：前置条件、验证命令、通过/失败判定、阻塞项。

**基线：** 521 passed, 11 skipped, ruff clean, R3 已修复。

---

## 全局约束

- 不接入真实 LLM（C2 只定义验收路径）
- 不点亮真实 Spark slow tests（C1 只定义验收路径）
- 不引入真实生产数据（数据源只定义样本格式和接入方式）
- 不改变 SQL/Spark 安全边界
- 不改 Schema/Memory/Prompt 机制
- 不绕过 Validator、Executor、Comparator
- 不把 11 个 skipped Spark 用例宣称为已通过
- C1-C4 只能规划、登记和定义验收，不得直接实现
- 修改范围仅限：`docs/risks/phase-6-8-known-risks.md`、`tests/spark/test_orchestrator.py`（过期注释清理）、`docs/superpowers/plans/`（新方案文档）

---

## 能力清单（诚实声明）

### ✅ 本轮之前已完成

| 能力 | 证据 |
|------|------|
| Mapper → Compiler → Validator 真实串联 | `test_real_contract_e2e_mapper_compiler_validator` PASSED |
| R3 input_alias 依赖链填充 | `test_input_alias_chain_populated_for_linear_steps` PASSED |
| Orchestrator 骨架 E2E | 28 个 orchestrator 测试全绿 |
| 全量回归 | 521 passed, 11 skipped |
| 风险登记 | `docs/risks/phase-6-8-known-risks.md`（R3 需更新为"已修复"） |

### ⚠️ 本轮要做

| 项目 | 类型 | 说明 |
|------|------|------|
| R3 状态同步 | A 类小修 | 更新 risk doc + 清理测试过期注释 |
| 业务集成验收路径矩阵 | 方案输出 | C1-C4 每条路径的前置条件 + 验收命令 |

### ❌ 本轮不做

| 项目 | 原因 |
|------|------|
| 真实 Spark 点亮 | C 类环境依赖——仅定义验收路径 |
| LLM ProviderAdapter 接入 | C 类延期——仅定义选型标准 |
| Comparator 真实对比实现 | 骨架级——需 SQL pipeline 先就绪 |
| Harness 真实样本填充 | C 类延期——仅定义样本格式 |

---

## Task 1: A 类小修——R3 状态同步 + 过期注释清理

**文件：**
- 修改：`docs/risks/phase-6-8-known-risks.md`（更新 R3 状态为"已修复"）
- 修改：`tests/spark/test_orchestrator.py`（清理过期的 R3 gap 注释）

### Step 1: 更新风险文档中 R3 状态

**位置**：`docs/risks/phase-6-8-known-risks.md` 第 69-84 行（R3 条目）

将 R3 的 `风险等级：A（阻塞 mapper→compiler 真实串联）` 改为 `风险等级：已消除（2026-07-04 修复）`，并在"处置建议"后追加修复摘要。

修改内容：

```markdown
## R3: Mapper ProjectStep input_alias 空值 Gap（已修复 ✅）

- **风险等级**：已消除（2026-07-04 修复）
- **发现阶段**：Phase 8 全局验收 Task 2
- **修复阶段**：R3 收口（业务集成前置准备）
- **影响范围**：`src/tianshu_datadev/spark/mapper.py`
- **问题描述**：
  - Mapper 对 `ProjectStep`、`CaseWhenStep`、`SortStep`、`LimitStep` 产出空的 `input_alias`
  - 导致 Compiler 的 `SparkCodeRenderer.validate_identifier()` 拒绝（正则 `^[a-zA-Z_][a-zA-Z0-9_]*$` 不匹配空字符串）
- **修复方案**：
  - 新增 `_chain_input_aliases(steps)` ——遍历已排序步骤列表，为每个步骤的 `input_alias` 填入前驱步骤的编译器输出别名
  - 新增 `_get_step_output_alias(step, index)` ——返回编译器将赋予给定步骤的输出变量名（与 compiler.py out_alias 命名规则严格一致）
  - 在 `map_contract_to_spark_plan()` 中，SparkPlan 构造前调用 `_chain_input_aliases(steps)`
- **验收证据**：
  - `test_input_alias_chain_populated_for_linear_steps` PASSED（简单线性 Contract）
  - `test_input_alias_chain_full_contract_no_empty_aliases` PASSED（完整 9 种 step）
  - `test_real_contract_e2e_mapper_compiler_validator` PASSED（真实 Contract 全链路）
  - 全量回归 521 passed, 11 skipped，零退化
- **状态**：✅ 已修复并回归通过
```

同时更新风险矩阵（第 100-107 行）：

```markdown
| 编号 | 等级 | 阻塞骨架验收？ | 阻塞业务集成？ | 处置时机 |
|------|------|:---:|:---:|------|
| C1 | C-环境依赖 | 否 | 是 | 业务集成前点亮 |
| C2 | C-延期 | 否 | 是 | 业务集成前接入 |
| C3 | 骨架级 | 否 | 否 | 随 SQL 链路验收 |
| C4 | C-延期 | 否 | 是 | 业务集成前准备样本 |
| R3 | 已消除 | — | — | 2026-07-04 已修复 |
| R4 | 已消除 | — | — | 2026-07-04 已修复 |
```

### Step 2: 清理 test_orchestrator.py 过期 R3 注释

**位置 1**：`tests/spark/test_orchestrator.py` 第 257-261 行

修改前：
```python
# 已知限制：Mapper 产出的 ProjectStep/CaseWhenStep/SortStep/LimitStep
# 的 input_alias 为空字符串，导致 Compiler 拒绝（非法标识符）。
# 该 gap 已登记为 R3，需在后续 Phase 修复 mapper 的依赖链赋值逻辑。
# 本轮 E2E 测试跳过 Mapper，直接构造 SparkPlan → Compiler → Validator。
```

修改后：
```python
# R3（Mapper input_alias 空值）已于 2026-07-04 修复——_chain_input_aliases()
# 在 mapper 组装步骤后自动填充线性依赖链。真实 Contract E2E 测试见
# test_real_contract_e2e_mapper_compiler_validator。
```

**位置 2**：`tests/spark/test_orchestrator.py` 第 311-316 行（`TestOrchestratorSkeletonE2E` 类 docstring）

修改前：
```python
class TestOrchestratorSkeletonE2E:
    """骨架级端到端——Orchestrator 真实调用 compiler → validator。

    注意：Mapper 因 input_alias gap（R3）暂不参与真实链路。
    Comparator 和 PhysicalVerifier 因依赖外部条件标记 SKIPPED。
    """
```

修改后：
```python
class TestOrchestratorSkeletonE2E:
    """骨架级端到端——Orchestrator 真实调用 mapper → compiler → validator。

    R3（Mapper input_alias 空值）已修复——真实 Contract 可经 mapper 全链路流转。
    Comparator 和 PhysicalVerifier 因依赖外部条件标记 SKIPPED。
    """
```

### Step 3: 验证

```bash
python -m pytest tests/spark/test_orchestrator.py -v --tb=short
# 预期：28 passed

python -m pytest tests/spark/ tests/artifacts/ -q
# 预期：521 passed, 11 skipped

python -m ruff check src/tianshu_datadev/spark/ tests/spark/ docs/risks/
# 预期：All checks passed!

git diff --check
# 预期：clean（仅 CRLF 警告）
```

---

## Task 2: 业务集成验收路径矩阵（方案输出）

**文件：**
- 写入：`docs/superpowers/plans/2026-07-04-business-integration-prep.md`（本文件）

**说明**：Task 2 不写代码——它定义 C1-C4 每条路径的前置条件、验收命令和通过标准，供业务集成执行阶段直接使用。

---

### 2.1 目标环境定义

**Spark 运行时最低要求：**

| 组件 | 最低版本 | 验证方法 |
|------|---------|---------|
| PySpark | ≥3.3.0 | `pyspark --version` 或 `python -c "import pyspark; print(pyspark.__version__)"` |
| Java | ≥8 (OpenJDK/JDK) | `java -version` |
| Python | ≥3.10 | `python --version` |
| OS | Windows / Linux / macOS | `pyspark` 在各平台均可运行 |

**环境验证命令（一键检查）：**

```bash
python -c "
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('env_check').master('local[1]').getOrCreate()
df = spark.createDataFrame([(1,)], ['a'])
df.show()
spark.stop()
print(f'PySpark {pyspark.__version__} OK')
"
```

**通过标准**：无 ImportError / Java 未安装 / SparkSession 创建失败。

**当前项目状态**：`pyspark>=3.4.0` 已在 `pyproject.toml` 可选依赖中声明。未安装时，`pytest --run-slow` 自动 skip 全部 11 个 Spark 用例——当前行为无需改变。

---

### 2.2 数据源定义

**业务样本数据来源（不引入生产数据）：**

| 来源 | 格式 | 接入方式 | 责任人 |
|------|------|---------|--------|
| 手工构造 DataFrame | PySpark 内存 DataFrame | 测试 fixture 中 `spark.createDataFrame(...)` | 开发者 |
| CSV/Parquet 样例文件 | `.csv` / `.parquet`（≤1000 行） | 放入 `tests/spark/samples/` 目录，通过 `spark.read` fixture 加载 | 业务方/开发者 |
| DuckDB 内存表 | DuckDB 内存表 | 现有 PhysicalVerifier 已支持——用于逻辑对比基准 | 已就绪 |

**样本最低要求：**

- 每维度至少 1 个样本，每个样本覆盖至少 1 种 step 类型
- 样本不包含真实 PII（个人身份信息）、真实商业数据
- 每个样本标注：覆盖的 step 类型、预期行数、关键字段

**SnapShotSourceProvider 约束**：物理数据源只能从 SnapshotSourceProvider 显式配置的 dev/test/fixture 源读取。本轮不修改 SnapshotSourceProvider 白名单。

---

### 2.3 C1 验收路径：真实 Spark 物理验证点亮

**当前状态**：`tests/spark/test_physical_verifier.py::TestRealSparkExecution` 含 11 个参数化用例（scan / filter / project / sort / limit / aggregate / join / case_when / window_row_number / window_sum_over / window_rank），全部标记 `@pytest.mark.slow`，在无 PySpark 环境时自动 skip。

**前置条件：**

1. ✅ 目标环境已安装 PySpark + Java（见 2.1 验证命令）
2. ✅ DuckDB 侧安全校验 + 结果对比已全绿（已就绪）
3. ⬜ `PhysicalVerifier._execute_spark()` 方法可正常创建 SparkSession 并执行 PySpark 代码
4. ⬜ `PhysicalVerifier._compare_results(duckdb_result, spark_result)` 的对比逻辑覆盖数值精度容差（decimal 类型）

**验收路径（业务集成执行阶段执行，本轮不执行）：**

```bash
# Step 1：确认环境
python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').getOrCreate().stop(); print('OK')"

# Step 2：点亮所有 11 个 Spark 用例
python -m pytest tests/spark/test_physical_verifier.py -v --run-slow --tb=long

# Step 3：验证覆盖——11 个参数化用例应全部 PASS
python -m pytest tests/spark/test_physical_verifier.py -v --run-slow -q
```

**通过标准：**

| 判定 | 条件 |
|------|------|
| ✅ 通过 | 11/11 Spark 用例 PASS，DuckDB 对比结果数值一致（精度 1e-6） |
| ⚠️ 部分通过 | ≥9/11 PASS，其余为已知精度差异（decimal/window frame） |
| ❌ 阻塞 | <9/11 PASS 或 SparkSession 创建失败 |

**阻塞项**：若 `pyspark` 在目标环境中无法安装或 `spark-submit` 无法执行，则 C1 升级为 A 类（环境阻塞），需在业务集成前解决。

**注意**：`@pytest.mark.slow` 的行为是本轮设计的一部分——它在无 Spark 环境时自动 skip，保证了 CI 和本地开发的流畅性。点亮时只需 `--run-slow`。

---

### 2.4 C2 验收路径：LLM ProviderAdapter 接入

**当前状态**：
- `SparkDeveloperService.annotate(spark_plan)` 接口已定义
- `AnnotationValidator` 校验逻辑已就绪
- Prompt 安全构造已验证——不含 SQL 文本、不含 DeveloperSpec 引用、不含 markdown 代码块
- `llm_call` 使用 mock callable 注入——测试全覆盖

**前置条件：**

1. ⬜ 选择 ProviderAdapter（OpenAI / Anthropic / 本地模型——取决于业务方 LLM 基础设施）
2. ⬜ 实现 `SparkDeveloperService.annotate()` 的真实 LLM 调用路径：
   - `ProviderAdapter.complete(prompt, schema=AnnotatedSparkPlan)` → StructuredOutput
   - 调用结果经 `AnnotationValidator.validate()` 校验
   - 校验失败 → 重试一次 → 仍失败 → HUMAN_REVIEW
3. ⬜ `annotation_hash` = 标注后 SparkPlan 的 hash（与 baseline 的 step 结构一致，仅 annotation 字段不同）
4. ⬜ `compile_raw(baseline) == compile_raw(annotated.baseline)`（删除 annotation 后执行代码完全等价）

**验收路径（业务集成执行阶段执行，本轮不执行）：**

```bash
# Step 1：ProviderAdapter 连通性检查
python -c "
from tianshu_datadev.spark.developer import SparkDeveloperService
# 使用真实 ProviderAdapter 创建 Developer
svc = SparkDeveloperService(provider=YourProviderAdapter(api_key='...'))
print('ProviderAdapter OK')
"

# Step 2：Developer 真实 LLM 标注测试
python -m pytest tests/spark/test_spark_developer.py -v --tb=long

# Step 3：验证 AnnotatedSparkPlan 校验通过
python -m pytest tests/spark/test_annotations.py -v --tb=long

# Step 4：端到端——含 Developer 的 Orchestrator 全链路
python -m pytest tests/spark/test_orchestrator.py -v --tb=long
```

**通过标准：**

| 判定 | 条件 |
|------|------|
| ✅ 通过 | `annotate()` 返回有效 AnnotatedSparkPlan，AnnotationValidator 拒绝率 <5%，compile_raw baseline==annotated 等价性成立 |
| ⚠️ 部分通过 | LLM 标注可用但 AnnotationValidator 拒绝率 >5%（提示 Prompt 需要调优） |
| ❌ 阻塞 | ProviderAdapter 无法连接 / `annotate()` 持续失败 / 超过重试上限仍无有效输出 |

**选型建议（非强制性，供业务集成时参考）：**

| Provider | 优势 | 劣势 |
|----------|------|------|
| Anthropic Claude | StructuredOutput 原生支持，Prompt 安全 | 需 API key + 网络 |
| OpenAI GPT-4 | StructuredOutput 成熟 | 需 API key + 网络 |
| 本地 vLLM/Ollama | 无网络依赖，数据不外传 | 需运维 LLM 实例，StructuredOutput 需额外适配 |

**阻塞项**：若无可用 LLM Provider（网络不可达 / API key 未配置 / 本地模型未部署），Developer 阶段保持 SKIPPED——不影响其他阶段执行。此行为与当前 Orchestrator 设计一致。

---

### 2.5 C3 验收路径：Comparator 真实链路

**当前状态**：
- `PlanComparator.compare(spark_plan, sql_plan)` 接口已完整实现——9 种 step 对比规则已就绪
- `compare()` 需要 `SqlBuildPlan` 作为输入（由 SQL pipeline 产出）
- Spark pipeline 当前不产生 SqlBuildPlan
- Orchestrator 中 COMPARATOR 阶段标记 SKIPPED

**前置条件：**

1. ⬜ SQL pipeline 可产出 SqlBuildPlan（需确认 SQL pipeline 的就绪状态——不在本项目范围内）
2. ⬜ 同一 DataTransformContractV1 可同时输入 SQL pipeline 和 Spark pipeline
3. ⬜ `PlanComparator.compare()` 的调用路径：从 Contract 出发 → SQL pipeline 产出 SqlBuildPlan → Spark pipeline 产出 SparkPlan → 两者送入 compare()

**验收路径（业务集成执行阶段执行，本轮不执行）：**

```bash
# Step 1：验证 Comparator 9 种 step 对比规则（已有测试）
python -m pytest tests/spark/test_plan_comparator.py -v --tb=long

# Step 2：真实 SQL ↔ Spark 逻辑对比（需 SQL pipeline 就绪）
python -m pytest tests/spark/test_comparator_e2e.py -v --tb=long
# 或通过 orchestrator 执行——需为 Comparator 提供 SqlBuildPlan
```

**通过标准：**

| 判定 | 条件 |
|------|------|
| ✅ 通过 | 9 种 step 对比规则全部等价（同一 Contract 的 SQL 和 Spark 产出语义一致） |
| ⚠️ 部分通过 | 部分 step 类型对比不等价（如窗口函数帧边界语义差异），已记录为已知差异 |
| ❌ 阻塞 | 对比逻辑存在结构性差异（SQL 和 Spark 对同一 Contract 的解释不同） |

**阻塞项**：C3 的最大阻塞项是 SQL pipeline 的 SqlBuildPlan 产出能力。若 SQL pipeline 未就绪，Comparator 持续 SKIPPED——此为已知骨架级 gap，不影响 Spark pipeline 独立验证。

**当前处置**：COMPARATOR 在 Orchestrator 中标记 SKIPPED，附带说明原因。此行为在业务集成执行阶段之前无需改变。

---

### 2.6 C4 验收路径：Harness 业务样本

**当前状态**：
- 5 维度评测框架已定义：`SPARK_CONTRACT_FIDELITY` / `SPARK_COMPILATION_DETERMINISM` / `SPARK_VALIDATOR_COVERAGE` / `SPARK_LOGIC_EQUIVALENCE` / `SPARK_PHYSICAL_CONSISTENCY`
- `SparkHarnessRunner.evaluate()` 当前是结果聚合器——统计预置 `case.passed` 布尔值
- `SparkEvalCase` 模型已定义——含 `case_id` / `dimension` / `description` / `expected_behavior` / `passed` / `actual_result`
- 无业务样本集

**前置条件：**

1. ⬜ 每维度至少 1 个业务样本，每个样本覆盖至少 1 种 step 类型
2. ⬜ 样本来源：手工构造或 CSV/Parquet 样例文件（见 2.2 数据源定义）
3. ⬜ Harness Runner 能够对每个样本执行：真实编译 → 真实 Validator → 真实对比

**样本集模板（供业务集成时填充）：**

```python
# 每维度至少 1 个样本，以下为模板
SAMPLE_SUITE: list[dict] = [
    # D1: CONTRACT_FIDELITY——验证 Contract 字段完整映射到 SparkPlan
    {
        "case_id": "D1_001",
        "dimension": SparkEvalDimension.SPARK_CONTRACT_FIDELITY,
        "description": "Contract(input_tables=[od], output_columns=[order_id, amount]) → SparkPlan 所有字段一对一映射",
        "expected_behavior": "SparkPlan 包含 ReadStep(od) + ProjectStep(2 列)，无遗漏无多余",
        "contract_fixture": "minimal_read_project",
    },
    # D2: COMPILATION_DETERMINISM——同一 plan 多次编译产出相同 hash
    {
        "case_id": "D2_001",
        "dimension": SparkEvalDimension.SPARK_COMPILATION_DETERMINISM,
        "description": "同一 SparkPlan（含 scan+filter+project+sort+limit）3 次编译 raw_hash 相同",
        "expected_behavior": "3 次 raw_hash 全等",
        "contract_fixture": "acceptance_r3_e2e",
    },
    # D3: VALIDATOR_COVERAGE——Validator 正确拒绝恶意代码
    {
        "case_id": "D3_001",
        "dimension": SparkEvalDimension.SPARK_VALIDATOR_COVERAGE,
        "description": "含 spark.read 的恶意代码被 Validator 拒绝（E601）",
        "expected_behavior": "is_valid=False，error_code=E601",
        "malicious_payload": 'df = spark.read.parquet("/etc/passwd")',
    },
    # D4: LOGIC_EQUIVALENCE——SQL ↔ Spark 逻辑对比（依赖 C3）
    {
        "case_id": "D4_001",
        "dimension": SparkEvalDimension.SPARK_LOGIC_EQUIVALENCE,
        "description": "同一 Contract 的 SQL 和 Spark 产出语义等价",
        "expected_behavior": "PlanComparator 报告 9 种 step 全部等价",
        "contract_fixture": "full_9_step",
    },
    # D5: PHYSICAL_CONSISTENCY——双引擎物理结果一致（依赖 C1）
    {
        "case_id": "D5_001",
        "dimension": SparkEvalDimension.SPARK_PHYSICAL_CONSISTENCY,
        "description": "scan+filter+project 的 DuckDB 和 Spark 结果完全一致",
        "expected_behavior": "DuckDB 和 Spark 结果行数相同、数值差异 <1e-6",
        "contract_fixture": "acceptance_r3_e2e",
    },
]
```

**验收路径（业务集成执行阶段执行，本轮不执行）：**

```bash
# Step 1：验证 Harness 框架（已有测试）
python -m pytest tests/spark/test_spark_eval.py -v --tb=long

# Step 2：填充 5 个业务样本后执行真实评测
python -m pytest tests/spark/test_harness_real_samples.py -v --tb=long
```

**通过标准：**

| 判定 | 条件 |
|------|------|
| ✅ 通过 | 5/5 维度各有 ≥1 个样本，全部 PASS |
| ⚠️ 部分通过 | ≥4/5 维度通过，其余为已知依赖缺口（C1/C3 未点亮导致 D4/D5 无法执行——标记 SKIPPED 并登记原因） |
| ❌ 阻塞 | <4/5 维度有样本或样本定义错误 |

**阻塞项**：业务样本的具体内容需业务方提供。若暂无业务方输入，使用 2.2 数据源中定义的样例数据作为占位样本——不阻塞进入业务集成执行阶段。

---

## 验收命令

```bash
# 1. Orchestrator 测试（含 R3 收口 E2E）
python -m pytest tests/spark/test_orchestrator.py -v --tb=short

# 2. 全量回归
python -m pytest tests/spark/ tests/artifacts/ -q

# 3. Lint
python -m ruff check src/tianshu_datadev/spark/ tests/spark/ docs/risks/

# 4. Git diff
git diff --check
```

**预期结果：**
- `test_orchestrator.py`：28 passed（无变化——注释修改不影响测试逻辑）
- 全量回归：521 passed, 11 skipped（无退化）
- ruff：零告警
- git diff：clean

---

## 退出标准

- [ ] R3 风险状态已更新为"已修复"（docs/risks/phase-6-8-known-risks.md）
- [ ] test_orchestrator.py 过期 R3 注释已清理
- [ ] 全量回归 521 passed, 11 skipped
- [ ] ruff 零告警
- [ ] 业务集成验收路径矩阵已写入本方案文档（2.1-2.6 节）
- [ ] C1-C4 每条路径的前置条件 + 验收命令 + 通过/失败判定明确

### 通过后是否可进入业务集成执行阶段？

**是。** 满足以下全部条件时允许进入业务集成执行阶段：

1. ✅ R3 已修复——真实 Contract → Mapper → Compiler → Validator 全链路可跑通
2. ✅ C1-C4 验收路径已明确——每个风险的验收命令、通过标准、阻塞项已文档化
3. ✅ 目标环境定义已到位——PySpark + Java 版本要求和验证命令已就绪
4. ⚠️ 进入前需确认：目标环境是否已安装 PySpark（C1 的前置条件）、LLM Provider 是否可选型（C2 的前置条件）

**不建议**跳过业务集成前置准备直接进入生产上线。应先完成：
- 业务集成执行阶段（按 2.1-2.6 路径依次点亮 C1-C4）
- 然后根据验收结果决定是否可进入生产试点

---

## A/B/C 分类汇总

| 分类 | 内容 | 处置 |
|------|------|------|
| **A（本轮修复）** | R3 风险状态同步 + 过期注释清理 | ✅ 已完成 |
| **B（本轮方案）** | C1-C4 验收路径矩阵制定 | ✅ 已完成——本方案 §2.1-2.6 |
| **C（已执行）** | C1 真实 Spark 点亮 | ✅ 2026-07-04 已点亮——11/11 通过，见 `business-integration-round1.md` |
| **C（已启动）** | C2 LLM ProviderAdapter 接入 | 📋 方案已制定——见 `c2-provider-adapter-plan.md`，4 个 Task 待实现 |
| **C（不实现）** | C3 Comparator 真实链路 | 依赖 SQL pipeline 就绪 |
| **C（不实现）** | C4 Harness 业务样本 | 依赖业务方提供样本 |

---

## 执行状态（2026-07-04 更新）

> 本章节记录本方案中定义的各验收路径的实际执行情况。

| 路径 | 原定义 | 当前状态 | 执行方案书 |
|------|--------|---------|-----------|
| C1 (§2.3) | 定义验收路径 | **✅ 已点亮** | `business-integration-round1.md` |
| C2 (§2.4) | 定义验收路径 | **📋 已制定详细方案** | `c2-provider-adapter-plan.md` |
| C3 (§2.5) | 定义验收路径 | 等待 SQL pipeline | — |
| C4 (§2.6) | 定义验收路径 | 等待业务样本 | — |

**下一轮建议：** 执行 `c2-provider-adapter-plan.md` 中的 Task 1-4（纯代码，不依赖外部环境）。

---

## 非技术人员解释

**这轮做了什么？**

上轮修好了一根传动轴（R3），让原料能从头跑到尾。这轮做了两件小事：

1. **更新了维修记录**：把"这根轴有问题"的标签换成了"已修好"，清理了车间里过期的警示牌（代码注释），让后面来的师傅不会看到旧警告以为轴还是坏的。

2. **画了一张"通电指南"**：流水线上还有三台机器没通电（Spark 环境、AI 大脑、双线对比），现在把每台机器需要什么电源、怎么测试、通电后按什么标准验收都写清楚了。这不是去通电——是告诉下一轮的师傅"电闸在哪、线怎么接、灯亮了算成功"。

**为什么现在不做通电？**

因为通电需要去机房（配 Spark 环境）、买电池（搞 LLM API key）、等隔壁车间完工（SQL 链路）。这些不是代码问题，是环境和资源问题。现在的情况是：机器位置留好了、接口留好了、测试插头留好了——只差把电源线插上。

**下一轮做什么？**

按"通电指南"逐台通电：先检查机房有没有 Spark → 有就点亮 11 个测试 → 再确认用哪家 AI → 接上标注功能 → 等隔壁 SQL 车间完工 → 联调双线对比 → 最后用 5 个真实产品跑一次完整质检。
