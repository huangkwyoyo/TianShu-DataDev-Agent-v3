# Phase 3C：受控写入审查材料 + CompilerBackend 接口

> 状态：待实施
> 前置依赖：Phase 3B 退出条件全部满足

## 执行前必须阅读

1. `AGENTS.md` §8 — _temp 中间表和最终日期分区写入方案只作为受控审查材料
2. `docs/01-target-architecture.md` §4 — DataTransformContract v1（Phase 3 Exit 交付）
3. `docs/09-test-strategy.md` §7 Phase 3C

## 只允许修改

- `src/tianshu_datadev/sql/` — 扩展
  - `compiler_backend.py`：CompilerBackend 抽象接口（占位）
  - `write_validator.py`：写入方案校验（分区 overwrite 审查）
- `src/tianshu_datadev/artifacts/` — 扩展
  - `contract_extractor.py`：扩展支持 DataTransformContract v1 抽取（含 SqlProgram、CASE、窗口、写入方案）
- `tests/` — 新增 test_write_plan.py / test_compiler_backend.py

## 禁止修改

- SqlBuildPlan / SqlProgram 核心 Schema——只消费，不修改
- `src/tianshu_datadev/spark/` — Phase 5 前不碰

## 新增模型

### FinalWritePlan

```python
class FinalWritePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    write_plan_id: str
    program_id: str
    target_table: str
    partition_keys: list[str]           # 分区键列表
    overwrite_mode: str                 # "partition"——仅允许分区 overwrite
    partition_values: dict[str, str]    # 分区键 → 值
    validation_checks: list[WriteValidationCheck]
    forbidden_operations: list[str]     # 被拒绝的操作列表
    review_material: str                # 供人工审查的写入方案说明

class WriteValidationCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")
    check_id: str
    check_type: str
    passed: bool
    detail: str
```

**写入方案约束**：
- 只允许日期分区 overwrite——作为审查材料输出，不实际执行写入
- 全表 overwrite → 拒绝
- 无分区 overwrite → 拒绝
- UPDATE / DELETE / MERGE → 拒绝
- INSERT INTO（非 overwrite）→ 拒绝

### CompilerBackend 接口（占位）

```python
class CompilerBackend(ABC):
    """SQL 编译器后端抽象接口——Phase 3C 占位，Phase 5+ 实现 Spark SQL 后端"""

    @abstractmethod
    def compile(self, plan: SqlBuildPlan | SqlProgram) -> CompilerOutput:
        """将 SqlBuildPlan / SqlProgram 编译为目标 SQL 方言"""
        ...

    @abstractmethod
    def dialect(self) -> str:
        """返回 SQL 方言标识：'duckdb' | 'spark_sql'"""
        ...
```

Phase 3C 仅定义接口并实现 DuckDB 后端的重构（将现有 Compiler 逻辑封装为 DuckDBBackend）。Spark SQL 后端在 Phase 5 实现。

### DataTransformContract v1

从 SqlProgram 确定性抽取，相比 lite 新增：

```python
class DataTransformContractV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contract_id: str
    level: str = "v1"
    source_sqlprogram_hash: str
    # ... lite 全部字段 ...
    step_dag: dict[str, list[str]]      # 多步依赖图（从 SqlProgram.dag）
    temp_tables: list[TempTableSpec]    # _temp 中间表规格
    case_when_labels: list[CaseWhenLabelSpec]   # CASE 标签规则
    window_specs: list[WindowSpec]      # 窗口函数规格
    write_spec: FinalWritePlan | None   # 写入方案（Phase 3C 新增）
```

## artifact schema

- `FinalWritePlan` JSON（含分区 overwrite 方案 + 审查材料）
- `DataTransformContract v1` JSON
- 重构后的 `CompilerOutput` JSON（CompilerBackend 架构）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| FinalWritePlan | 4 | 日期分区 overwrite 正确生成、全表 overwrite 拒绝、无分区 overwrite 拒绝、UPDATE/DELETE/MERGE 拒绝 |
| DataTransformContract v1 | 3 | 从 SqlProgram 确定性抽取、包含全部 v1 新增字段、hash 一致性 |
| CompilerBackend | 2 | DuckDBBackend 实现 dialect() 返回 'duckdb'、编译行为与重构前一致 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "write_plan or contract_v1 or compiler_backend"
python -m ruff check src/tianshu_datadev/sql/ src/tianshu_datadev/artifacts/
git diff --check
```

## B/C 暂停条件

- 写入方案的审查材料格式与目标团队 Code Review 流程不匹配
- CompilerBackend 接口需支持 DuckDB 不支持的 SQL 方言特性——接口需调整
- DataTransformContract v1 字段过多需要拆分

## 退出条件

1. FinalWritePlan 日期分区 overwrite 方案正确生成
2. 全表 overwrite、无分区 overwrite、UPDATE/DELETE/MERGE 被拒绝
3. CompilerBackend 抽象接口占位就绪，DuckDBBackend 实现正确
4. DataTransformContract v1 从 SqlProgram 确定性抽取
5. Phase 1A-3B 测试保持通过

### Phase 3 Exit：HarnessReport(phase="phase-3-exit")

Phase 3C 退出时生成 HarnessReport：
- SQL-first v1.0 的 Schema 可生成性基线
- DataTransformContract v1 覆盖度
- SqlProgram + _temp 多语句场景的 Compiler 覆盖率
- 已知不支持的 SQL 模式清单（CTE、子查询、多跳 Join）
- Phase 4 硬化的输入基线

---

> Phase 3C | 待实施 | 前置：Phase 3B 退出
