# Batch 5 实现报告：Spark 管线监控埋点注入

## 修改摘要

### 修改的文件

1. **`src/tianshu_datadev/spark/orchestrator.py`**
   - `run()`: 添加 `collector=None` 参数（默认 NullCollector），用 `with collector.stage("spark_verify", state.contract_hash)` 包裹全链路
   - `_execute_stage()`: 添加 `collector` 和 `parent_stage_run_id` 参数，6 个阶段分别用 `with collector.stage()` 包裹
   - 阶段失败注入路径：用异常触发 StageContext 记录 "failed" 状态

2. **`src/tianshu_datadev/api/pipeline.py`**
   - `run_spark_stage()`: 添加 `collector = get_collector()` + `with collector.stage(stage_node, request_id)` 包裹阶段执行

3. **`tests/monitor/test_spark_integration.py`**（新建）
   - MockCollector + MockStageContext 模式（参考 test_pipeline_integration.py）
   - 8 个测试用例覆盖全链路、parent_stage_run_id、单阶段调用、artifact_path、失败记录、retry、NullCollector

### 节点映射

| 阶段 | node 名 | parent |
|------|---------|--------|
| run() 全链路 | `spark_verify` | null |
| MAPPER | `spark_mapper` | spark_verify.stage_run_id |
| DEVELOPER | `spark_developer` | spark_verify.stage_run_id |
| COMPILER | `spark_compiler` | spark_verify.stage_run_id |
| VALIDATOR | `spark_validator` | spark_verify.stage_run_id |
| COMPARATOR | `spark_comparator` | spark_verify.stage_run_id |
| PHYSICAL_VERIFIER | `spark_physical_verifier` | spark_verify.stage_run_id |

## 测试结果

### 新建测试（8 passed）
```
tests/monitor/test_spark_integration.py ........                          [100%]
```

1. `test_spark_verify_records_all_six_sub_stages` — 全链路 verify 记录 6 子节点 + spark_verify
2. `test_sub_stages_have_parent_stage_run_id` — 子节点 parent = spark_verify.stage_run_id
3. `test_run_spark_stage_records_single_stage` — 单阶段调用 parent=null
4. `test_spark_compile_records_artifact_path` — COMPILER 成功后记录 artifact_path
5. `test_spark_physical_verify_records_row_count` — SKIPPED 阶段无 row_count/artifact_path
6. `test_spark_failure_records_stage_failed` — 失败注入记录 failed + error_type
7. `test_spark_retry_is_new_stage_run_id` — retry>0 产生新 stage_run_id
8. `test_null_collector_spark_stage_noop` — NullCollector 模式下行为不变

### 回归测试（2276 passed, 11 skipped）
```
pytest tests/ -q --ignore=tests/api/test_nyc_business_case.py ...
2276 passed, 11 skipped, 18 warnings
```

### 现有 Spark 测试（661 passed, 11 skipped）
```
pytest tests/spark/ -q
661 passed, 11 skipped
```

## Lint 结果

- `ruff check src/tianshu_datadev/spark/orchestrator.py` — All checks passed!
- `ruff check tests/monitor/test_spark_integration.py` — Clean
- `ruff check src/tianshu_datadev/api/pipeline.py` — 仅含预存 lint 错误（长行/F541/F821），非本次修改引入

## 关键设计决策

- **`_run_*` 方法不修改**：埋点包裹在 `_execute_stage` 层，`_run_*` 内部异常由自身 try/except 处理，不传播到 StageContext
- **阶段失败注入用异常触发 failed 记录**：`RuntimeError` 经 `try/except RuntimeError: pass` 吞掉，确保 StageContext.__exit__ 能记录 "failed"
- **NullCollector 默认值**：`collector=None` → `NullCollector()`，确保所有现有调用方无需修改
- **`artifact_request_id` 使用 `state.contract_hash`**：与任务要求一致

## 剩余风险

- 无
