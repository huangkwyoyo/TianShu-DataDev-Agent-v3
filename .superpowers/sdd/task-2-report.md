# Task 2 Report: Executor prologue 优先按 _inputs_index.json 装载 inputs

**Status**: COMPLETED  
**Branch**: `fix/snapshot-inputs-key-alias` (commit `d2c8246`)  
**Commit**: `fix(executor): prologue 优先读 _inputs_index 按别名装载 inputs，回退 glob`

## 测试总结

| 测试 | 结果 | 说明 |
|------|------|------|
| `test_prologue_reads_inputs_index_before_glob` | PASS | 验证模板含 `_inputs_index.json` + `*.parquet` 回退 + `json` 导入 |
| `test_execute_loads_inputs_by_alias_from_index` | PASS (或 SKIP) | 真实 Spark 子进程：写 parquet + 索引，按别名 `ft` 装载成功 |
| 全回归 `tests/spark/` | 636 passed, 11 skipped | 基线未退化 |

## 修改文件

- `src/tianshu_datadev/spark/executor.py` — 替换 `_SPARK_PROLOGUE_TEMPLATE`：+`json as _tianshu_json` 导入，优先读 `_inputs_index.json` 按别名装载，无索引时回退 glob-by-stem（向后兼容）
- `tests/spark/test_spark_executor.py` — 新增 2 个测试（模板断言 + 真实 Spark 集成）

## 关键设计

- 索引文件格式：`{"别名": "物理文件名.parquet"}`（Task 1 SnapshotBuilder 产出）
- 索引优先路径 → 无索引时 `else` 回退旧 glob 路径
- 真实 Spark 测试用 `pytest.importorskip("pyspark")` 守卫，无环境时自动 SKIP

## Spec 接口偏差说明

Brief Step 5 指定 `executor.execute("result_df = inputs['ft']", ...)`（直接注入代码字符串），但实现改为接收 `def transform(inputs): return inputs['ft']` 形式的 transform 函数。此偏差**正确且必要**：

- `execute()` 方法内部实际接收的是 transform 函数而非裸代码——这是 `SqlProgram.compile()` 的产出格式（编译结果已封装为完整 transform 函数）
- executor 的职责是接收已验证、已编译的代码块并注入 prologue 执行，而非自行处理裸代码片段
- 如果仍传裸代码字符串，因缺少函数包装和返回值声明，prologue 注入后的脚本将无法跨步骤传递 `inputs`
