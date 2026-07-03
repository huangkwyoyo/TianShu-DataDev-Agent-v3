# Spark-first Phase 6-8 实施计划

> 状态：待执行 | 日期：2026-07-03
> 前置：设计文档 `2026-07-03-spark-first-phase-6-8-design.md` 已通过 CRCS 审查
> 基线代码：Phase 5 完成，45 个测试通过，`src/tianshu_datadev/spark/` 含 models.py / mapper.py / plan_equivalence.py

---

## 实施顺序总览

```
Phase 0（前置迁移）── SparkReadStep 模型迁移 + mapper 适配 + 回归
        │
Phase 6A ── scan/filter/project/sort/limit 编译 + Validator + 注释
        │
Phase 7A ── 逻辑链路（5 种）+ Snapshot 最小快照
        │
Phase 6B ── aggregate/join/case_when 编译扩展
        │
Phase 7B ── 物理链路最小验证（双引擎执行）
        │
Phase 6C ── window 编译 + 帧边界
        │
Phase 7C ── 物理链路扩展（窗口双引擎验证）
        │
Phase 8  ── 编排 + Review Package + Harness
```

每个 Phase 有明确的退出条件，下一 Phase 不可在未满足退出条件时启动。

---

## Phase 0：SparkReadStep 模型迁移（前置）

### 目标

将 `SparkReadStep` 从 Phase 5 的 `source_path` + `format` 模型迁移至 Phase 6 设计要求的 `source_name` + `input_key` + `required_columns` 模型。物理路径不再存放于 `SparkPlan`，而是由 `SnapshotManifest` 管理。

### 当前状态

```python
# models.py:89 — 当前模型
class SparkReadStep(StrictModel):
    step_type: SparkStepType = SparkStepType.READ
    alias: str
    source_path: str          # ← 移除
    format: str = "parquet"   # ← 移除
    estimated_row_count: int | None = None
```

### 目标状态

```python
# models.py — 目标模型
class SparkReadStep(StrictModel):
    step_type: SparkStepType = SparkStepType.READ
    alias: str                        # DataFrame 变量名
    source_name: str                  # inputs dict 的 key（替换 source_path）
    input_key: str                    # 对应 ContractInputTable 的唯一标识
    required_columns: list[str] = Field(default_factory=list)  # 需要的列
    estimated_row_count: int | None = None
```

### 允许修改文件

| 文件 | 修改内容 |
|------|---------|
| `src/tianshu_datadev/spark/models.py` | `SparkReadStep` 字段重构 |
| `src/tianshu_datadev/spark/mapper.py` | `_map_input_tables()` 适配新字段 |
| `tests/spark/test_spark_plan.py` | 更新 `SparkReadStep` 构造和断言 |

### 禁止事项

- ❌ 不新增文件（这是迁移，不是新功能）
- ❌ 不修改 `SparkPlan.compute_plan_hash()`（hash 输入需同步更新，但算法不变）
- ❌ 不修改 `plan_equivalence.py` 中的 Read step 对比逻辑
- ❌ 不移除 `__init__.py` 中的 `SparkReadStep` 导出

### 验收命令

```bash
# 1. 类型检查（零错误）
uv run pyright src/tianshu_datadev/spark/models.py src/tianshu_datadev/spark/mapper.py

# 2. 现有 45 个测试全部通过（零失败）
uv run pytest tests/spark/test_spark_plan.py -v --tb=short

# 3. 未发现 source_path 或 format 残留
uv run python -c "
import ast, sys
from pathlib import Path
spark_dir = Path('src/tianshu_datadev/spark')
for f in spark_dir.glob('*.py'):
    tree = ast.parse(f.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in ('source_path',):
            print(f'RESIDUAL source_path in {f}:{node.lineno}')
            sys.exit(1)
print('OK: 无 source_path 残留')
"
```

### 残留扫描

```bash
# 项目全局 source_path 引用扫描——只应在文档中出现，不在 spark/ 代码中
rg "source_path" src/tianshu_datadev/spark/ --no-filename
# 预期输出：（空）
```

### 回归测试

- `tests/spark/test_spark_plan.py` 全部 45 个测试通过
- 特别验证：`test_read_step_creation`、`test_roundtrip_read_step`（如有）、`test_mapper_full_pipeline` 中涉及 ReadStep 的断言

### 退出条件

- [ ] `SparkReadStep` 不再有 `source_path`、`format` 字段
- [ ] `_map_input_tables()` 产出新字段（`source_name`、`input_key`）
- [ ] `source_path` 在 `src/tianshu_datadev/spark/` 下零残留
- [ ] 45 个测试全部绿
- [ ] `pyright` 零错误
- [ ] 设计文档 `SparkReadStep` 描述与代码一致

---

## Phase 6A：基础编译闭环（scan/filter/project/sort/limit）

### 目标

实现 5 种基础 step 类型的完整闭环：Compiler 代码生成 → Renderer 安全渲染 → 5 行结构化注释 → Static Validator AST 校验。建立 Phase 6 的核心架构，后续 6B/6C 只扩展 step 类型。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/annotations.py` | **新增** | StepAnnotation、AnnotationWarning、AnnotatedSparkPlan、AnnotationValidator、StepIntent 枚举 |
| `src/tianshu_datadev/spark/renderer.py` | **新增** | SparkCodeRenderer + 全部 render_* 方法 + SafeIdentifier 校验 |
| `src/tianshu_datadev/spark/compiler.py` | **新增** | SparkCompiler + SparkCompileResult（5 种 step 编译） |
| `src/tianshu_datadev/spark/validator.py` | **新增** | SparkStaticValidator + 8 种错误码（E601-E608）定义 |
| `tests/spark/test_renderer.py` | **新增** | Renderer 安全测试（含恶意输入拒绝） |
| `tests/spark/test_spark_compiler.py` | **新增** | 5 种 step 编译测试 |
| `tests/spark/test_spark_compiler_comment.py` | **新增** | 注释块格式测试 |
| `tests/spark/test_spark_validator.py` | **新增** | Validator 8 种错误码测试 |
| `tests/spark/test_annotations.py` | **新增** | 标注模型 + AnnotationValidator 测试 |

### 禁止事项

- ❌ 不新建 `developer.py`（SparkDeveloperService 属于 Phase 6B+）
- ❌ 不实现 aggregate/join/case_when/window 编译（用 skip/xfail 占位）
- ❌ 不访问 LLM / ProviderAdapter
- ❌ 不修改 `mapper.py`、`models.py`（Phase 0 已完成迁移）
- ❌ Validator 不接入 ExecutionSafetyProbe（接口预留即可）

### 验收命令

```bash
# Renderer 安全测试（含恶意输入拒绝）
uv run pytest tests/spark/test_renderer.py -v --tb=short

# Compiler 5 种 step 编译测试
uv run pytest tests/spark/test_spark_compiler.py -v --tb=short

# 注释块格式测试
uv run pytest tests/spark/test_spark_compiler_comment.py -v --tb=short

# Validator 8 种错误码测试
uv run pytest tests/spark/test_spark_validator.py -v --tb=short

# 标注模型测试
uv run pytest tests/spark/test_annotations.py -v --tb=short

# 全部 Spark 测试（含已有 45 个回归）
uv run pytest tests/spark/ -v --tb=short

# 类型检查
uv run pyright src/tianshu_datadev/spark/

# ruff 检查
uv run ruff check src/tianshu_datadev/spark/
```

### 残留扫描

```bash
# 检查 Compiler 无直接字符串拼接（必须过 Renderer）
rg "f\"F\." src/tianshu_datadev/spark/compiler.py
# 预期：零匹配（所有 F.xxx 必须通过 Renderer 生成）

# 检查注释格式不含 SQL 文本
rg -i "select\b|where\b|from\b|join\b" src/tianshu_datadev/spark/compiler.py --no-filename
# 预期：只出现在字符串内的变量名/字段名，不在注释模板中

# 检查无 spark.read / spark.table 出现在代码生成路径
rg "spark\.(read|table)" src/tianshu_datadev/spark/compiler.py src/tianshu_datadev/spark/renderer.py
# 预期：零匹配
```

### 回归测试

- `tests/spark/test_spark_plan.py` 45 个测试全绿
- 不可有任何已有测试退化

### 退出条件

- [ ] 5 种 step 编译产出合法 PySpark DSL（通过 Validator AST 检查）
- [ ] Compiler 所有代码片段通过 Renderer（禁止裸 f-string）
- [ ] Renderer 恶意输入测试通过（SQL 注入、标识符越界、非法字面量）
- [ ] Validator 8 种错误码每种至少 1 个测试
- [ ] 注释 5 行格式测试（Step/Intent/Operation/Inputs/Output）通过
- [ ] 删除 annotation 后 raw 代码与 annotated 代码功能等价（compile_raw == compile_annotated 去除注释）
- [ ] 6B/6C step 类型有 skip/xfail 占位，标记 `reason="Phase 6B"` / `reason="Phase 6C"`
- [ ] pyright 零错误、ruff 零告警
- [ ] 已有测试零退化

---

## Phase 7A：逻辑链路 + Snapshot 最小快照

### 目标

完成 5 种 step 的逻辑链路对比（SQL Plan ↔ Spark Plan），建立 Snapshot Builder 最小闭环（单表本地 fixture → Parquet 快照 → 完整性校验）。物理链路暂不执行，标记 NOT_EXECUTED。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/snapshot.py` | **新增** | SnapshotSourceProvider、SamplingSpec、SnapshotFile、SnapshotManifest、SnapshotBuilder |
| `src/tianshu_datadev/spark/plan_comparator.py` | **新增** | PlanComparator + ComparisonStatus + PlanComparisonReport（封装 plan_equivalence） |
| `src/tianshu_datadev/spark/verification_report.py` | **新增** | UnifiedVerificationReport + VerificationOverallStatus |
| `tests/spark/test_snapshot.py` | **新增** | Snapshot 构建 + Manifest 完整性测试 |
| `tests/spark/test_plan_comparator.py` | **新增** | 5 种 step 逻辑对比测试（非 5 种的标记 NOT_EXECUTED） |

### 禁止事项

- ❌ 不执行任何物理引擎代码（DuckDB/Spark 均不启动）
- ❌ 不创建 `physical_verifier.py`、`executor.py`、`repair_planner.py`
- ❌ Snapshot Builder 不访问任何不在 `SnapshotSourceProvider` 白名单的数据源
- ❌ 不直接读取 SQL 文本（只读 SqlBuildPlan 结构化 artifact）
- ❌ 不修改 `plan_equivalence.py`（已有，只封装调用）

### 验收命令

```bash
# Snapshot 测试
uv run pytest tests/spark/test_snapshot.py -v --tb=short

# 逻辑对比测试
uv run pytest tests/spark/test_plan_comparator.py -v --tb=short

# 全部 Spark 测试
uv run pytest tests/spark/ -v --tb=short

# 类型检查
uv run pyright src/tianshu_datadev/spark/
```

### 残留扫描

```bash
# Snapshot Builder 不使用 spark.read
rg "spark\.read" src/tianshu_datadev/spark/snapshot.py
# 预期：零匹配

# PlanComparator 不直接引用 SQL 文本字符串
rg "sql_text|sql_code|sql_str" src/tianshu_datadev/spark/plan_comparator.py
# 预期：零匹配

# 无 "PASS" 泛化状态
rg "\"PASS\"|PASS\b" src/tianshu_datadev/spark/plan_comparator.py src/tianshu_datadev/spark/verification_report.py
# 预期：零匹配（必须用 LOGIC_EQUIVALENT 等具体状态）
```

### 回归测试

- Phase 6A 全部测试通过
- `tests/spark/test_spark_plan.py` 45 个测试通过

### 退出条件

- [ ] `SnapshotBuilder` 能基于 `SnapshotSourceProvider` 白名单生成本地 fixture Parquet 快照
- [ ] `SnapshotManifest` 包含完整溯源链（snapshot_id 确定性，不含时间戳参与 hash）
- [ ] `PlanComparator` 封装 `plan_equivalence.py`，5 种 step 对比产出 `PlanComparisonReport`
- [ ] 非 5 种 step 类型 → `NOT_EXECUTED`（不在对比范围）
- [ ] `UnifiedVerificationReport` 正确标记 `LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED`
- [ ] 无 "PASS" / "Go" / "No-Go" 状态名
- [ ] pyright 零错误
- [ ] 已有测试零退化

---

## Phase 6B：中等编译扩展（aggregate/join/case_when）

### 目标

在 6A 架构上扩展 Compiler/Renderer/Validator 支持 aggregate、join、case_when 三种 step 类型。将 6B 的 skip/xfail 占位转为实际测试。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/compiler.py` | **修改** | 增加 3 种 step 编译方法 |
| `src/tianshu_datadev/spark/renderer.py` | **修改** | 增加 join/agg/case_when 渲染方法 |
| `tests/spark/test_spark_compiler.py` | **修改** | 6B skip/xfail → 实际断言 |
| `tests/spark/test_renderer.py` | **修改** | 追加 3 种 step 的恶意输入测试 |

### 禁止事项

- ❌ 不修改 `annotations.py` 模型定义（模型在 6A 已完整）
- ❌ 不修改 `validator.py` 错误码体系（6A 已完整定义 8 种）
- ❌ 不新增文件
- ❌ 不实现 window 编译

### 验收命令

```bash
uv run pytest tests/spark/test_spark_compiler.py -v --tb=short -k "aggregate or join or case_when"
uv run pytest tests/spark/test_renderer.py -v --tb=short
uv run pytest tests/spark/ -v --tb=short
uv run pyright src/tianshu_datadev/spark/
```

### 退出条件

- [ ] 8 种 step（6A 5 种 + 6B 3 种）全部编译产出合法 PySpark DSL
- [ ] Renderer 覆盖 8 种 step 的渲染（含安全拒绝）
- [ ] 6B 的 skip/xfail 全部移除
- [ ] 6C 的 skip/xfail 保留（window 仍标记 `reason="Phase 6C"`）
- [ ] 已有测试零退化

---

## Phase 7B：物理链路最小验证

### 目标

在 7A 逻辑链路基础上，打通物理链路：同一份 Snapshot → DuckDB 执行 SQL → 本地 Spark 执行 PySpark DSL → ResultCanonicalizer 规范化 → 对比结果。范围限定 6A+6B 的 8 种 step。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/physical_verifier.py` | **新增** | PhysicalVerifier + ResultCanonicalizer + PhysicalVerificationStatus |
| `src/tianshu_datadev/spark/executor.py` | **新增** | LocalSparkExecutor（子进程隔离 + 安全边界） |
| `tests/spark/test_physical_verifier.py` | **新增** | 双引擎执行 + 结果对比测试 |
| `tests/spark/test_spark_executor.py` | **新增** | 执行器安全边界测试 |

### 禁止事项

- ❌ 不创建 `repair_planner.py`（RepairPlanner 只在有实际修复需求时创建）
- ❌ `exec(pyspark_code)` 不在主进程执行——必须子进程隔离
- ❌ 不在测试中硬编码生产数据源路径
- ❌ 物理验证失败不自动泛化为 "PASS"
- ❌ 未覆盖 step（window）→ `NOT_EXECUTED`

### 验收命令

```bash
uv run pytest tests/spark/test_physical_verifier.py -v --tb=short
uv run pytest tests/spark/test_spark_executor.py -v --tb=short
uv run pytest tests/spark/ -v --tb=short
uv run pyright src/tianshu_datadev/spark/
```

### 安全扫描

```bash
# executor.py 必须在 subprocess 中执行
rg "subprocess|preexec_fn|tempfile.mkdtemp" src/tianshu_datadev/spark/executor.py
# 预期：3 处全部命中

# executor.py 不得有裸 exec() 在主进程
rg -n "^\s*exec\(" src/tianshu_datadev/spark/executor.py
# 预期：exec 调用出现在子进程函数的字符串参数中
```

### 退出条件

- [ ] `SnapshotBuilder` → 同一 Parquet 目录供 DuckDB 和 Spark 分别读取
- [ ] DuckDB 执行 SQL 成功，产出结果
- [ ] 本地 Spark 执行 PySpark DSL 成功（通过子进程隔离）
- [ ] `ResultCanonicalizer` 排序/去重/NULL/NaN/Decimal 策略生效
- [ ] 无排序键的测试 case → `UNSUPPORTED_SEMANTICS` → `HUMAN_REVIEW`
- [ ] 双引擎结果一致 → `RESULT_CONSISTENT`
- [ ] Window step → `NOT_EXECUTED`
- [ ] 已有测试零退化

---

## Phase 6C：窗口编译（window）

### 目标

扩展 Compiler/Renderer 支持 window step 类型（含帧边界校验）。6C 是 Phase 6 的最后一块。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/compiler.py` | **修改** | 增加 window step 编译（含 WindowSpec 帧边界） |
| `src/tianshu_datadev/spark/renderer.py` | **修改** | 增加窗口函数/WindowSpec 渲染方法 |
| `tests/spark/test_spark_compiler.py` | **修改** | 6C skip/xfail → 实际断言 |

### 禁止事项

- ❌ 不新增文件
- ❌ 不修改 annotations.py / validator.py

### 验收命令

```bash
uv run pytest tests/spark/test_spark_compiler.py -v --tb=short -k "window"
uv run pytest tests/spark/ -v --tb=short
uv run pyright src/tianshu_datadev/spark/
```

### 退出条件

- [ ] 9 种 step 全部编译产出合法 PySpark DSL
- [ ] 窗口帧边界（ROWS/RANGE BETWEEN）正确渲染
- [ ] 所有 skip/xfail 移除
- [ ] Phase 6 全量测试通过（6A + 6B + 6C）

---

## Phase 7C：物理链路扩展 + RepairPlanner

### 目标

补全 window 物理验证，实现 RepairPlanner（只输出 RepairAction，不修改 Plan），完成 Phase 7 全部交付。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/repair_planner.py` | **新增** | RepairPlanner + RepairAction |
| `src/tianshu_datadev/spark/physical_verifier.py` | **修改** | 扩展 window 验证 |
| `tests/spark/test_repair_planner.py` | **新增** | RepairPlanner 测试 |
| `tests/spark/test_physical_verifier.py` | **修改** | 追加 window 双引擎测试 |

### 禁止事项

- ❌ RepairPlanner 不直接修改 SparkPlan / Contract / SqlBuildPlan / PySpark 代码
- ❌ RepairAction 不自动触发执行（只建议，由 Orchestrator 决定）
- ❌ 返工计数不超过 2 轮

### 验收命令

```bash
uv run pytest tests/spark/test_repair_planner.py -v --tb=short
uv run pytest tests/spark/test_physical_verifier.py -v --tb=short -k "window"
uv run pytest tests/spark/ -v --tb=short
uv run pyright src/tianshu_datadev/spark/
```

### 退出条件

- [ ] 9 种 step 全量物理验证通过（或正确标记 NOT_EXECUTED/HUMAN_REVIEW）
- [ ] RepairPlanner 输出 5 种 RepairAction 分类（MAPPER_BUG/COMPILER_BUG/VALIDATOR_GAP/SNAPSHOT_ISSUE/BUSINESS_SEMANTIC）
- [ ] BUSINESS_SEMANTIC → HUMAN_REVIEW（不自动修改）
- [ ] MAPPER_BUG → 路由回 mapper.py（测试中 mock 验证路由正确）
- [ ] Phase 7 全量测试通过

---

## Phase 8：编排硬化 + Review Package + Harness

### 目标

完成 Spark-first 路径的完整编排、统一交付物（SparkReviewPackage）、Harness 5 维度评测。这是 Phase 6-8 的最后一个 Phase。

### 允许修改文件

| 文件 | 操作 | 内容 |
|------|------|------|
| `src/tianshu_datadev/spark/orchestrator.py` | **新增** | SparkOrchestrator + SparkPipelineState |
| `src/tianshu_datadev/spark/review_package.py` | **新增** | SparkReviewPackage + CrossReference + SparkProvenance |
| `src/tianshu_datadev/spark/review_builder.py` | **新增** | SparkReviewBuilder |
| `src/tianshu_datadev/spark/developer.py` | **新增** | SparkDeveloperService（LLM 封装 + StructuredOutput） |
| `src/tianshu_datadev/harness/spark_eval.py` | **新增** | Spark 5 维度评测 |
| `tests/spark/test_orchestrator.py` | **新增** | 编排测试 |
| `tests/spark/test_review_package.py` | **新增** | Review Package 测试 |
| `tests/spark/test_spark_developer.py` | **新增** | prompt regression 测试 |
| `tests/spark/test_spark_e2e.py` | **新增** | 端到端集成测试 |

### 禁止事项

- ❌ Orchestrator 不直接访问 ProviderAdapter / LLM Client
- ❌ Orchestrator 不直接构造 Prompt
- ❌ Orchestrator 不解析 LLM 自由文本输出
- ❌ CrossReference 不含 SQL 文本字符串（只放 sql_artifact_id / sql_step_id）
- ❌ SparkDeveloper 不读取 DeveloperSpec / SqlBuildPlan / SQL 文本
- ❌ 返工最多 2 轮 → 自动进入 HUMAN_REVIEW
- ❌ AnnotationWarning 不触发自动返工

### 验收命令

```bash
# 编排测试
uv run pytest tests/spark/test_orchestrator.py -v --tb=short

# Review Package 测试
uv run pytest tests/spark/test_review_package.py -v --tb=short

# Developer 测试
uv run pytest tests/spark/test_spark_developer.py -v --tb=short

# Harness 评测
uv run pytest tests/spark/test_spark_e2e.py -v --tb=short

# 全部测试
uv run pytest tests/ -v --tb=short

# 类型检查
uv run pyright src/tianshu_datadev/

# ruff 检查
uv run ruff check src/tianshu_datadev/
```

### 残留扫描

```bash
# CrossReference 不含 SQL 文本
rg "sql_text|sql_code|SELECT|FROM\b|WHERE\b" src/tianshu_datadev/spark/review_package.py
# 预期：只出现在类名/方法名/类型注解，不在字符串值中

# Orchestrator 不直接调 LLM
rg "provider_adapter|llm_client|anthropic|openai|langchain" src/tianshu_datadev/spark/orchestrator.py
# 预期：零匹配

# Review Package 不含 SQL 文本
rg "SELECT.*FROM|WHERE.*=" src/tianshu_datadev/spark/review_package.py
# 预期：零匹配
```

### 退出条件

- [ ] `SparkOrchestrator` 正确编排 6A→7A→6B→7B→6C→7C 全链路
- [ ] `SparkDeveloperService` 产出带 StructuredOutput 的标注（含 AnnotationValidator 校验）
- [ ] `SparkReviewPackage` 含完整 provenance hash 链（contract_hash → spark_plan_hash → annotation_hash → compiled_code_sha256 → snapshot_id）
- [ ] `CrossReference` 使用 sql_artifact_id/sql_step_id 引用（不含 SQL 文本）
- [ ] Harness 5 维度评测可执行（SPARK_CONTRACT_FIDELITY / SPARK_COMPILATION_DETERMINISM / SPARK_VALIDATOR_COVERAGE / SPARK_LOGIC_EQUIVALENCE / SPARK_PHYSICAL_CONSISTENCY）
- [ ] Pipeline 集成：4 种全局状态正确产出
- [ ] E2E 测试覆盖完整链路（Contract → mapper → Developer → Compiler → Validator → Comparator → PhysicalVerifier → Review Package）
- [ ] 已有测试零退化

---

## Phase 间依赖与风险

```
Phase 0 ──► Phase 6A ──► Phase 7A ──► Phase 6B ──► Phase 7B ──► Phase 6C ──► Phase 7C ──► Phase 8
  │            │            │            │            │            │            │            │
  │            └── 不可跳过 ──┘            │            └── 不可跳过 ──┘            │            │
  │                        │                           │                           │            │
  └── 必须先完成 ──────────┴───────────────────────────┴───────────────────────────┴────────────┘
```

**风险点**：

| 风险 | 阶段 | 缓解 |
|------|------|------|
| 本地 Spark 环境不兼容 | 7B | Phase 7B 前先验证本地 `pyspark` 可启动 |
| LLM StructuredOutput 不稳定 | 6A（developer 部分推迟到 8） | Developer 推迟到 Phase 8；6A-7C 全确定性组件 |
| 测试数据需本地 fixture | 7A | Phase 7A 使用 `local_fixture` 源类型，从 `tests/fixtures/` 生成 |
| `source_path` 残留未清理 | 0 | 残留扫描脚本自动化 |
| skip/xfail 未正确标记 phase | 6A | 验收时 grep 检查所有 skip/xfail reason 字段 |

---

## 全局验收检查清单（Phase 8 结束后执行）

- [ ] `spark.read` 在 `src/tianshu_datadev/spark/` 下零出现
- [ ] SQL 文本在 Spark 代码注释中零出现
- [ ] `SparkPlan` 只在 `mapper.py` 中构造（由 `map_contract_to_spark_plan()` 返回）
- [ ] `RepairPlanner` 不直接修改任何 Plan 对象
- [ ] 所有未覆盖能力标记 `NOT_EXECUTED` 或 `HUMAN_REVIEW`
- [ ] 无 "PASS" / "Go" / "No-Go" 状态名
- [ ] `pyright` 零错误、`ruff` 零告警
- [ ] 全部测试通过（`uv run pytest tests/ -v`）
- [ ] 设计文档与代码一致（5 条硬边界逐条扫描验证）

---

> 实施计划完成。每个 Phase 必须在退出条件全部满足后才能进入下一 Phase。
> 关联设计文档：`docs/superpowers/specs/2026-07-03-spark-first-phase-6-8-design.md`
