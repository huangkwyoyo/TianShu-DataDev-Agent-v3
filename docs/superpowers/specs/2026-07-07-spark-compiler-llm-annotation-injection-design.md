# Spark Compiler LLM 注释注入设计文档

> Phase 8B — 将 LLM 语义标注（intent_detail / operation_summary）注入 SparkCompiler 产出的 standalone PySpark 脚本中

## 1. 问题描述

- [x] DEVELOPER 阶段已成功注入 SparkDeveloperService，产出 6 个步骤的语义标注
- [x] 编译阶段 `_do_spark_compile()` 生成的 standalone PySpark 脚本不含 LLM 业务注释
- [x] 根因：`SparkCompiler.compile()` 已有 `annotations` 形参但从未传入；standalone 脚本使用 `raw_pyspark` 而非 `annotated_pyspark`

## 2. 方案概览

**方案 A：编译器集成法**（推荐）

| 改动点 | 文件 | 说明 |
|--------|------|------|
| ① 传参 | `pipeline.py` → `_do_spark_compile()` | 将 `context.annotation_result` 传入 `compiler.compile()` |
| ② 增强注释 | `compiler.py` → `_build_comment_block()` | 有 LLM annotation 时追加 Business 行 |
| ③ 换输出 | `pipeline.py` → `_do_spark_compile()` | standalone 改为使用 `result.annotated_pyspark` |
| ④ 输出注释 | `pipeline.py` → standalone wrapper | `__main__` 中 `# 输出字段说明:` 静态注释（不进可执行代码） |

## 3. 数据流

```
COMPILER 阶段入口 (_do_spark_compile)
  │
  ├─ context.spark_plan               ← SparkPlan（6 steps）
  ├─ context.annotation_result        ← AnnotatedSparkPlan（6 annotations）── NEW
  │
  ▼
  compiler.compile(plan, annotations=step_annotations)  ← annotations 参数
  │
  ├─ for i, step in enumerate(plan.steps):
  │     step_id = state.next_step_id(step_type)           ← "SparkReadStep_0"
  │     annotation = ann_map.get(step_id)                 ← 按 step_id 匹配
  │     raw, comment = _compile_xxx(step, step_id, ...)   ← 原签名不变
  │     if annotation: comment = _build_comment_block(...) ← 后处理增强
  │
  ▼
  result.raw_pyspark         ← 不变（hash 一致性保障）
  result.annotated_pyspark   ← 含 LLM 业务注释 ── NEW
  │
  ▼
  standalone wrapper
  ├── 注解部分用 annotated_pyspark
  └── 追加 # 输出字段说明: ... 静态注释（不进可执行代码）
```

## 4. 改动详情

### 4.1 `pipeline.py` — `_do_spark_compile()`

代码位置：约第 2589 行

改动一：调用 `compiler.compile()` 时传入 annotations

```python
# 从 annotation_result 提取 annotations 列表
step_annotations = None
if context.annotation_result is not None:
    step_annotations = context.annotation_result.annotations

# 传入 compiler
result = compiler.compile(context.spark_plan, annotations=step_annotations)
```

改动二：standalone 脚本用 `annotated_pyspark`

```python
annotated = result.annotated_pyspark  # 含 LLM 业务注释
for line in annotated.split('\n'):
    wrapper_lines.append(line)
```

改动三：wrapper 末尾追加静态字段解读注释

⚠ **安全约束：LLM 文本不得进入可执行 Python 语句（`print(...)` / `exec()` / `eval()`）。
LLM 内容仅可出现在 `#` 注释中，依赖 `_verify_no_comment_injection()` 防线。**

```python
# 在 print("=== 结果概要 ===") 前插入静态字段注释
# LLM 文本只进注释块，不进可执行代码
if context.annotation_result:
    last_ann = context.annotation_result.annotations[-1]
    wrapper_lines.append(f'    # 输出字段说明: {last_ann.intent_detail}')
```

### 4.2 `compiler.py` — 注释增强（`_build_comment_block`）

**原则：保留 5 行固定结构，有 LLM annotation 时替换 Intent 和 Operation 行，追加 Business 行。**

```python
def _build_comment_block(
    self,
    step_id: str,
    index: int,
    total: int,
    intent: str = "",
    operation: str = "",
    inputs: str = "",
    output: str = "",
    *,
    # ── Phase 8B: LLM 语义标注注入（可选）──
    annotation: StepAnnotation | None = None,
) -> str:
    """构建步骤注释块——5 行固定格式。

    有 LLM annotation 时：
    - Intent 行用 annotation.intent
    - Operation 行用 annotation.operation_summary
    - 追加 Business 行（intent_detail）
    - Step / Inputs / Output 行保持不变
    """
    if annotation is not None:
        lines = [
            f"# Step: {step_id}（索引 {index + 1}/{total}）",
            f"# Intent: {annotation.intent if hasattr(annotation.intent, 'value') else annotation.intent}",
            f"# Operation: {annotation.operation_summary or operation}",
            f"# Inputs: {inputs}",
            f"# Output: {output}",
            f"# Business: {annotation.intent_detail}",
        ]
        return "\n".join(lines)

    # 原结构性注释逻辑（5 行）
    ...
```

### 4.3 compiler `compile()` 主循环

**策略：不改 `_compile_xxx()` 签名。compile() 拿到 `raw, comment` 后，在循环内统一增强 comment。**

```python
def compile(self, plan: SparkPlan, annotations: list | None = None) -> SparkCompileResult:
    """..."""
    state = _CompileState()
    # 构建 step_id → StepAnnotation 的查找映射
    ann_map: dict[str, StepAnnotation] = {}
    if annotations:
        for a in annotations:
            if hasattr(a, 'step_id') and a.step_id:
                ann_map[a.step_id] = a

    for i, step in enumerate(plan.steps):
        step_type = type(step).__name__
        step_id = state.next_step_id(step_type)    # "SparkReadStep_0"
        annotation = ann_map.get(step_id)           # 按 step_id 匹配 LLM 标注

        # 调用原编译方法（签名不变，不传 annotation）
        if isinstance(step, SparkReadStep):
            raw, comment = self._compile_read(step, step_id, i, len(plan.steps))
        elif ...:
            ...

        # 后处理：有 LLM annotation 时增强 comment（Phase 8B）
        if annotation is not None:
            comment = self._build_comment_block(
                step_id=step_id, index=i, total=len(plan.steps),
                intent=step_type,
                operation="",  # 将由 annotation 填充
                inputs="",
                output="",
                annotation=annotation,
            )
        ...
```

## 5. 边界 & 约束

**改动范围：**
- `pipeline.py`: +~35 行（传参 + standalone 改 annotated_pyspark + 输出注释）
- `compiler.py`: +~30 行（`_build_comment_block` 增强 + compile 主循环后处理）

**不修改：**
- `developer.py` / `annotations.py` / `SparkOrchestrator` / `routes.py`
- Prompt 模板
- 前端面板
- 所有 `_compile_xxx()` 方法签名

**关键约束：**
- `raw_pyspark` 不变（`raw_hash` 不受影响）
- `_verify_no_comment_injection()` 通过——注释格式不含裸代码，**不得削弱该函数**
- LLM 文本只能出现在 `# ` 注释行中，**不可进入任何可执行 Python 语句**
- `annotation_result=None`（DEVELOPER 未执行）时走原逻辑，不报错

## 6. 数据流图（代码级）

```
SparkStageContext
├── spark_plan: SparkPlan
│   └── steps: [SparkReadStep, SparkFilterStep, ...]   ← 6 steps
├── annotation_result: AnnotatedSparkPlan
│   └── annotations: [StepAnnotation, ...]              ← 6 annotations
│       └── step_id: str  → match → next_step_id() 输出的 step_id
└── compile_result: SparkCompileResult
    ├── raw_pyspark         → standalone 脚本（无注释）← 不变
    └── annotated_pyspark   → standalone 脚本（含 LLM 注释）← 本阶段目标
```

## 7. 验收清单

| # | 验收项 | 验证方式 |
|---|--------|---------|
| 1 | `transform()` 每步注释块包含 `intent_detail` 业务语义 | API 返回 standalone 脚本检查 |
| 2 | 注释含 `intent_detail` 业务语义（非纯结构性描述） | 检查注释含真实业务文字 |
| 3 | `__main__` 中 print/show 前有静态 `# 输出字段说明:` 注释 | 脚本含注释行，不含可执行 print |
| 4 | `annotation_count == len(spark_plan.steps)`；annotation_result=None 时不要求 | 全量步骤命中 |
| 5 | `annotation_result=None` 时不报错 | pytest 验证 |
| 6 | `raw_hash` 不变 | 单元测试验证 |
| 7 | `_verify_no_comment_injection()` 通过 | 编译过程不抛出 |
| 8 | 全量测试通过 | `pytest tests/spark/ -v` |

## 8. 风险 & 回退

- **风险：** `_build_comment_block` 输出格式变更可能导致 `_verify_no_comment_injection` 误报
  → 处理原则：**不得削弱 `_verify_no_comment_injection()`**。优先修正注释渲染方式：
    1. 所有 LLM 文本必须经过 `renderer.render_comment_text()` 单行清洗（去换行、转义引号）
    2. 只有在证明 `_verify_no_comment_injection()` 自身存在明确 bug，并有恶意注入回归测试验证时，才能调整该函数
- **风险：** step_id 匹配失败（compiler 内部 step_id 生成逻辑与 annotation 的 step_id 不一致）
  → 兜底：匹配失败时静默降级为结构性注释，不阻断编译
