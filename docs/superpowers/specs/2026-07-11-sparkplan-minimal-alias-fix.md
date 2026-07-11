# SparkPlan 最小别名修复（Spark-only）

> 消除 mapper/compiler/test 三套别名状态机冗余，将别名解析统一到单一 `_alias_resolver`。
> **范围：仅 Spark Compiler。SQL Compiler 不在本次范围内。**

## 1. 问题与边界

### 1.1 原问题

v1 别名系统存在三套独立的状态机副本：

| 位置 | 状态 | 问题 |
|------|------|------|
| `_alias_generator.generate_step_alias()` | 语义别名规则（`_filtered`、`_with_`、`_output`） | 别名不可预测，依赖碰撞检测和截断 |
| `compiler._CompileState.latest_alias` | 运行时追踪每个 alias 的最新输出变量 | 从 `raw.split(" = ")` 反向解析变量名 |
| `mapper._chain_input_aliases()` | 重复 compiler 的别名解析逻辑来预测变量名 | 预测错误 → NameError（如 `ft_filtered_filtered_with_tz`） |

### 1.2 修复范围

- **新增** `_alias_resolver.py`——单一别名解析层
- **修改** `compiler.py`——删除 `generate_step_alias`/`latest_alias`/`used_aliases`/`last_project_idx`
- **修改** `mapper.py`——删除别名预测逻辑，仅保留依赖链补全
- **删除** `_alias_generator.py`——已无生产调用者，死代码移除

### 1.3 不变边界

- SparkPlan、DataTransformContract、Predicate、Compiler API 不变
- SQL/Spark 业务语义和 Comparator 规则不变
- **SQL Compiler 不在此次范围内**——别名统一仅针对 Spark 侧
- v1 Compiler 已删除——`_alias_generator` 不再保留

---

## 2. 单一 alias resolver

### 2.1 文件：`src/tianshu_datadev/spark/_alias_resolver.py`

```python
@dataclass(frozen=True)
class ResolvedStep:
    step: SparkStep            # 原始 step 引用
    input_vars: tuple[str, ...]  # 输入 DataFrame 变量名
    output_var: str            # 输出 DataFrame 变量名（tN 或 fN）

@dataclass(frozen=True)
class ResolvedPlan:
    steps: tuple[ResolvedStep, ...]
    output_var: str            # 最终 return 的变量名
```

### 2.2 别名规则

| 节点类型 | 别名 | 排序依据 |
|---------|------|---------|
| ReadOpV2 | `t1`, `t2`, `t3`... | `input_key` 字典序 |
| 非 Read | `f1`, `f2`, `f3`... | 执行顺序 |

### 2.3 两个入口函数

**`assign_source_aliases(steps) → dict[str, str]`**：为 Read 节点按 `input_key` 字典序分配 tN，重复 `input_key` → `ValueError`。

**`resolve_codegen_aliases(plan) → ResolvedPlan`**：单循环遍历 steps，通过 `latest` 字典追踪每个 alias 的最新输出变量，为每个 step 分配 `input_vars` 和 `output_var`。

**依赖解析策略**：
- **非空 `input_alias`**：必须在 `latest` 中存在，否则抛 `AliasResolutionError`——这是严格校验，防止静默生成未定义变量。
- **空 `input_alias` + 有前序步骤**：使用 `prev_output`——这是**线性链兼容策略**，不是静默回退。`_chain_input_aliases()` 的目标是将空字段补全为正确的链式 key，resolver 保留此 fallback 仅用于防御未补全的线性链（如手工构造的测试 Plan），并非替代 Mapper 的职责。
- **空 `input_alias` + 无前序步骤**：抛 `AliasResolutionError`——首个步骤必须是 ReadStep。

---

## 3. Compiler 改造

### 3.1 `_CompileState` 简化

删除字段：`output_var_map`、`latest_alias`、`used_aliases`、`last_project_idx`

### 3.2 `compile()` 改造

```python
def compile(self, plan, annotations=None):
    resolved_plan = resolve_codegen_aliases(plan)  # ← 单一入口
    # ... 分发 _compile_* 方法时传入 ResolvedStep ...
    last_var = resolved_plan.output_var  # ← 不再反向解析
```

### 3.3 九个 `_compile_*` 方法

签名统一为 `(self, resolved: ResolvedStep, step_id, index, total)`：
- `step = resolved.step`——原始 step
- `input_alias = resolved.input_vars[0]`——已解析的输入变量名
- `out_alias = resolved.output_var`——已分配的输出变量名

删除项：
- `generate_step_alias()` 调用——所有方法
- `raw.split(" = ")` 反向解析——`compile()` 循环末尾
- `latest_alias` 更新——`compile()` 循环末尾
- `resolved_input_alias`/`resolved_left`/`resolved_right` 参数——所有方法
- `is_last_project` 逻辑——`_compile_project()`

---

## 4. Mapper 改造

### 4.1 `_chain_input_aliases()` 简化

删除 `generate_step_alias` 导入和 `_get_step_output_alias()` 函数。

新逻辑：仅补全空的 `input_alias` 字段为 `prev_key`——确保 DAG 依赖链完整，不预测代码变量名。

```python
def _chain_input_aliases(steps):
    prev_key = None
    for step in steps:
        if isinstance(step, SparkReadStep):
            prev_key = step.alias
        elif isinstance(step, SparkJoinStep):
            prev_key = step.left_alias
        else:
            if empty_input_alias:
                step.input_alias = prev_key
            prev_key = step.input_alias or prev_key
```

---

## 5. 测试

### 5.1 新增：`tests/spark/test_alias_resolver.py`（22 tests）

| 测试组 | 覆盖 |
|--------|------|
| `TestAssignSourceAliases`（5） | 单/双 Read、字典序确定性、重复 `input_key` 拒绝、非 Read 忽略 |
| `TestAliasResolutionErrors`（8） | 空 Plan、首步非 Read、缺失 `input_alias`、Join 左右别名缺失、重复 Read alias |
| `TestResolveCodegenAliases`（6） | t1→f1、f1→f2→f3、Join 双输入、Filter→Join→Aggregate 无语义别名、hash 确定性 |
| `TestIntegrationMapperToCompiler`（3） | Mapper 全链路→仅含 tN/fN、`ast.parse` 逐语句顺序验证、return 指向 output_var |

### 5.2 更新：现有测试

- `test_spark_compiler.py`：别名断言从语义（`od = inputs[`）改为序号（`t1 = inputs[`）；`_make_plan` 无 Read 时自动前置默认 ReadStep；E741 已修复
- `test_spark_plan.py`：删除对 `_filtered`/`_output`/`_sorted` 后缀的断言
- `test_orchestrator.py`：v1 风格别名（`_f0`/`_p2`）改为正确的链式依赖 key
- `test_renderer.py`：恶意 `left_alias` 测试改为期望 `AliasResolutionError`
- `test_spark_eval.py` / `test_spark_developer.py`：v1 风格别名全量修正
- `test_alias_generator.py`：**已删除**——连同 `_alias_generator.py`（无生产调用者）

### 5.3 验证命令

```bash
pytest tests/spark/ -x --tb=short   # 660 passed, 11 skipped
python -m ruff check .               # 修改文件全部 clean
git diff --check                    # 无空白告警
```

---

## 6. 回滚方案

1. `git revert` 本次提交
2. `_alias_resolver.py` 和 `test_alias_resolver.py` 删除
3. `compiler.py`、`mapper.py` 恢复 `generate_step_alias` 调用
4. 测试文件恢复语义别名断言
5. 恢复 `_alias_generator.py` 和 `test_alias_generator.py`

**公共 API（`SparkCompiler.compile()`、`map_contract_to_spark_plan()`）和 SparkPlan v1 模型未修改**——回滚不影响外部调用方。但 Compiler 内部实现已从 `generate_step_alias` 改为 `resolve_codegen_aliases`，Mapper 内部 `_chain_input_aliases` 已简化。
