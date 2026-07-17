# 项目当前状态与验证进度 — TianShu DataDev Agent v3

> 文档版本：2026-07-17 label_table v1 完成 + 测试统计口径修正 + 运行环境说明
> 最后更新：2026-07-17
> 本文是项目当前实施状态的**唯一权威文档**。各 Phase 设计文档（docs/00-09、docs/roadmap/）描述的是目标设计，实际建成状态以本文为准。

## 1. Phase 进度矩阵

| Phase | 名称 | 设计 | 实现 | 测试 | 备注 |
|:-----:|------|:---:|:---:|:---:|------|
| 0.5 | DeveloperSpec-first 架构校正 | ✅ | ✅ | ✅ | 文档迁移 + 路线图统一 |
| 1A/1B/1C | SQL 管线（输入→推理→编译） | ✅ | ✅ | ✅ | SQL-first v1.0 基础 |
| 2 | Code Review Package v1 | ✅ | ✅ | ✅ | |
| 3A/3B/3C | SqlProgram + 窗口 + 写入 | ✅ | ✅ | ✅ | |
| 4A-4D | SQL-first v1.0 硬化 | ✅ | ✅ | ✅ | LLM Gateway + Harness 七维 |
| 4.5 | 内部交互验证口 | ✅ | ✅ | ✅ | CLI + REST API |
| 4.6 | 复杂 SQL 渐进开放 | ✅ | ✅ | ✅ | 多跳 Join + FROM 子查询 |
| 5 | Spark-ready 契约 | ✅ | ✅ | ✅ | DataTransformContract + SparkPlan IR |
| 6A | scan/filter/project/sort/limit | ✅ | ✅ | ✅ | 编译 + Validator 全错误码 |
| 6B | aggregate/join/case_when | ✅ | ✅ | ✅ | 编译扩展 |
| 6C | window + 帧边界 | ✅ | ✅ | ✅ | 含 RepairPlanner |
| 7A | 逻辑链路 + Snapshot | ✅ | ✅ | ✅ | PlanComparator 9 种 step |
| 7B | 物理链路——双引擎验证 | ✅ | ✅ | ✅ | 11/11 真实 Spark 通过 |
| 7C | 物理链路扩展 + 安全加固 | ✅ | ✅ | ✅ | 窗口双引擎 + SQL 加固 |
| 8 | 编排硬化 + Harness | ✅ | ✅ | ✅ | Orchestrator + Review Package + 5 维度 |
| 9A | 生产级串联升级 | ✅ | ✅ | ✅ | 9A1-9A3 + 9A5 完成，9A4 NYC 01-06 全量完成 |
| 9B | 前端回归 + 可观测性 | ✅ | ✅ | ✅ | R11/R15 消除，2026-07-05 |
| 9B-P0 | Snapshot Builder 集成到 Pipeline | ✅ | ✅ | ✅ | R10 消除，可选注入+全链路覆盖，2026-07-05 |
| 9C | DOM E2E 交互测试 | ✅ | ✅ | ✅ | 6/6 Playwright 测试通过，2026-07-05 |
| 9C-R16 | table_paths 环境配置补齐 | ✅ | ✅ | ✅ | R16 消除，CSV fixture 自动发现，2026-07-05 |
| 9C-R16b | table_paths 边界硬化 | ✅ | ✅ | ✅ | None/{} 语义区分 + E2E 模式开关，2026-07-05 |
| 9B-P1 | provenance.yml 显式断言 | ✅ | ✅ | ✅ | snapshot_manifest_hash 测试覆盖矩阵补全，2026-07-05 |
| 9A4-NYC | 真实业务样本——NYC 案例 01-06 | ✅ | ✅ | 🟡 | Case 01-06 SQL+Spark 双链 LOGIC_EQUIVALENT；Case 05 窗口函数 NOT_COVERED |
| 10-Case06 | SqlProgram 多语句 DAG——NYC Case 06 | ✅ | ✅ | ✅ | **2026-07-06 闭环**："三层剥离" |
| 10-ContentAlign | Spark Comparator 内容级对齐 | ✅ | ✅ | ✅ | **2026-07-06 完成**：8 commits |
| CRE Phase 2 | CRE shadow 最终准入硬化 | ✅ | ✅ | ✅ | **2026-07-13 物理验证可用** |
| label_table v1 | 标签表类型完整管线 | ✅ | ✅ | ✅ | **2026-07-16 完成**：Parser → Extractor → Validator → Promotion → Builder(CaseWhenStep) → Compiler。8 commits，90 个测试全绿。详见 `docs/superpowers/specs/2026-07-15-label-table-design.md` |

### 测试基线（2026-07-17 采集）

**采集口径**：
- `pytest --collect-only`：**2818 tests collected**
- 全量执行需要 `--run-slow` + PySpark 环境（SparkSession 启动约 30-60s，部分测试有 180s 超时）
- 非 Spark/非 Harness 子集：**1629 passed / 6 skipped / 2 xfailed**（50s）
- ruff/tsc/build：零告警

**Spark 测试状态**（需 `--run-slow`）：
- PySpark 4.1.2 已安装在系统 Python（D:\Program Files\Python312），.venv 不包含
- 服务启动脚本 `dev-reload.sh` 已退出 .venv，直接使用系统 Python（详见 §7）

**CRE 测试基线**（不变）：
- CRE 核心：125 passed / 7 skipped
- Physical Verifier（含 CRE shadow 集成）：191 passed / 11 skipped
- artifacts 层（含 finalizer E2E）：全部通过

**三条 Pipeline 验收证据（2026-07-13）**：

| 证据 | 测试 | 路径 | 结果 |
|:----:|------|------|:----:|
| 证据 1：一致 | `TestPhysicalVerifierWithMock::test_result_consistent` | `verifier.verify()` 全链路 → CRE shadow | **RESULT_CONSISTENT** |
| 证据 2：浮点容差 WARN | `TestPhysicalVerifierShadow::test_shadow_warn_maps_to_consistent` | `_shadow_cre_diagnose()` → DecisionEngine | **CONSISTENT_WITH_WARN** |
| 证据 3：真实差异 | `TestPhysicalVerifierWithMock::test_result_mismatch` | `verifier.verify()` 全链路 → CRE shadow | **RESULT_MISMATCH** |

## 2. 业务集成验证

### C1-C4（已消除）

| 编号 | 内容 | 风险等级 | 状态 | 证据 |
|:----:|------|:--------:|:----:|------|
| C1 | 真实 Spark 物理验证 | 已消除 | ✅ 11/11 通过 | PySpark 4.1.2，DuckDB ↔ PySpark 一致性 100% |
| C2 | LLM 基础设施架构收口 | 已消除 | ✅ 收口完成 | 重复文件已删除，18/18 测试全绿，DeepSeek 3/3 验证 |
| C3 | Comparator 真实逻辑对比 | 已消除 | ✅ 桥接+集成 | 30/30 测试全绿，Orchestrator COMPARATOR 集成 |
| C4 | Harness 5 维度评测 | 已消除 | ✅ 全 5 维度 | D1-D5 共 31/31 测试全绿 |

### label_table v1

| 维度 | 状态 | 证据 |
|------|:----:|------|
| Parser → DatasetType 映射 | ✅ | `DatasetType.LABEL_TABLE`，YAML `type:` 字段支持 |
| 标签领域模型 | ✅ | LLM 输出层（`LabelDomainOutput`）与系统层（`LabelDomain`）分离，8 种 LabelPredicateNode，6 种 LabelPredicateCondition 根约束 |
| Gateway 文件持久化 | ✅ | Schema 校验通过后原子写入 `response_root`，FakeAdapter → Gateway → Extractor 集成测试 |
| LlmLabelExtractor | ✅ | 从 `response_root` 文件读取 LLM JSON 输出，复用 AnthropicAdapter + PromptManager |
| FakeLabelExtractor | ✅ | pytest 确定性输出，覆盖 ALL/COLUMN_REF/MIXED 三类标签域 |
| LabelRuleValidator v1 | ✅ | 六项检查：FIELD_EXISTS、TYPE_COMPATIBLE、OPERATOR_VALID、AST_VALID、LABEL_DOMAIN、COVERAGE |
| Promotion 双空阻断 | ✅ | `blocking_errors` 和 `human_review_items` 均为空才通过；evidence 非空强制 |
| Builder CaseWhenStep | ✅ | `_validate_label_rule_set()` 集合门禁 + `DerivedColumnRuleMissing` 硬阻断 |
| API Key 安全 | ✅ | 无 Key 时返回明确 CONFIG_ERROR，禁止回退 Fake |
| 回归测试 | ✅ | 90 个测试全绿（models/validator/promotion/extractor/integration） |

## 3. 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R5 | ~~桥接函数替代完整 SQL Pipeline~~ | 已消除 | Phase 9A1-9A3 + 9A5 已升级 |
| R6 | ~~Harness Runner 为结果聚合器~~ | 已消除 | Phase 9A3 已升级 |
| R7 | ~~Case 06 Spark Comparator xfail~~ | 已消除 | 2026-07-06 三层剥离完成 |
| R8 | ~~LLM 生产环境验证~~ | 已消除 | 2026-07-05 真实 LLM 8/8 通过 |
| R9 | Case 05 Window 规范化差异——Spark 的 ROW_NUMBER 窗口帧边界默认行为与 DuckDB 存在规范化差异，非代码 bug | **C（保守阻断）** | 非逻辑等价问题，属引擎行为差异。保守阻断，需人工确认语义等价后再解除阻断 |
| R10 | ~~Snapshot Builder 未集成~~ | 已消除 | Phase 9B-P0 |
| R11 | ~~前端无自动化测试~~ | 已消除 | Phase 9B + 9C |
| R-CRE-Golden | Golden Registry 为空 | 低（非阻断） | 后续 Phase 填充 |
| R-CRE-Null | `null_strategy` 始终 UNKNOWN | 低（非阻断） | 仅进入 HUMAN_REVIEW |
| R-CRE-Finalizer | Finalizer 写入失败不影响比较结论 | 低（非阻断） | 已实现 |
| R-LT-1 | CASE WHEN condition 静态等价不支持——`compare_case_when_steps` 标记 UNSUPPORTED，condition（谓词条件）的语义等价对比未实现 | **B** | **设计取舍**：condition 语义对比可复用 filter Predicate 递归逻辑，但表别名归一化、等价变换误判等使性价比不高。当前状态为 **CONSISTENT_SAMPLE**（结构骨架 labels/else_value/alias 已验证，condition 待人审）。**按需建设，非当前优先级**。详见 `docs/case_when条件对比边界说明_20260717_0908.md` |
| R-LT-2 | API Key 是环境前置条件，非架构风险——无 Key 时 label_table 请求返回 CONFIG_ERROR，SparkDeveloperService 标记 SKIPPED | **环境** | 仅影响需要 LLM 的功能子集；pytest 使用 FakeAdapter，不依赖 Key |
| R-LT-3 | condition 中可能包含 ColumnRef 表别名——CaseWhenStep 的 WHEN condition 是结构化谓词树（LabelPredicateNode），依赖表别名的 ColumnRef 在跨源场景需要额外归一化 | **B** | 当前单表场景无此问题；多表 label_table 被 `validate_label_table_v1_scope` 阻断 |

## 3.5 能力边界（已知非风险局限）

以下事项是已知的设计边界或架构局限，非待修复风险，当前不实施：

| 编号 | 说明 | 处置 |
|:----:|------|------|
| R-CA-1 | `target_grain` 过滤是 Case 06 特化——DAG 单粒度过滤是 Case 06 的正确特化，不是对任意 DAG 的通用解 | 当前不实施。多输出粒度场景需扩展为 `target_grains: list[list[str]]`。详见 `docs/superpowers/specs/2026-07-06-spark-comparator-closure-and-risks.md` |

## 4. 当前架构全景

```
DeveloperSpec (.md 项目书)
    │
    ├─ label_table 受控补全分支（v1）
    │   Parser 识别 type: label_table → DatasetType.LABEL_TABLE
    │   → LlmLabelExtractor（LLM 提取标签规则，Gateway 文件持久化）
    │      └─ LabelRuleValidator v1（6 项检查：字段/类型/操作符/AST/LABEL_DOMAIN/COVERAGE）
    │   → Promotion（双空阻断：blocking_errors + human_review_items 均为空）
    │   → Builder 追加 CaseWhenStep（DerivedColumnRuleMissing 硬阻断）
    │   → 进入下游 SQL/Spark 管线
    │
    ├─ SQL 管线（确定性，生产可用）
    │   Pipeline.run_all() → Parser → SourceManifest → SqlBuildPlan(含CaseWhenStep) → Compiler → DuckDB
    │       │
    │       └─ export_artifacts() → PipelineArtifactBundle
    │           ├─ sql_build_plan（真实 SqlBuildPlan）
    │           └─ data_transform_contract
    │               │
    │               └─ adapt_lite_to_v1() → DataTransformContractV1
    │
    └─ Spark 管线（确定性，生产级验证）
        DataTransformContractV1 → Mapper → SparkPlan → Compiler → Validator
                                        │                      │
                                        ├── PlanComparator.compare()        ← 单 plan 路径（SqlBuildPlan）
                                        ├── PlanComparator.compare_program() ← 多语句 DAG 路径（SqlProgram）
                                        │       └─ 三层剥离：_temp_* 过滤 + grain-aware merge + target_grain 过滤
                                        ├── PhysicalVerifier                ← 双引擎物理对比
                                        ├── Orchestrator                    ← 6 阶段编排
                                        └── Harness 5 维度                  ← 评测框架
                                                  │
                                                  └─ SparkReviewBuilder.build()
                                                         │
                                                         └─ SparkReviewPackage
                                                            ├─ provenance（完整溯源链）
                                                            ├─ stage_results（6 阶段结果）
                                                            ├─ comparator_status（对比器状态）
                                                            └─ review_ready ★ REVIEW_READY 判定
```

## 5. 下一步方向

1. ~~Case 06 Spark 双链 LOGIC_EQUIVALENT~~ → **✅ 已完成（2026-07-06）**
2. ~~CRE shadow 最终准入硬化~~ → **✅ 已完成（2026-07-13）**
3. **CRE 门禁切换（非阻断后续事项）**：
   - Golden Registry 为空——需业务方注册已知差异样本
   - NULL strategy 始终 UNKNOWN——仅进入 HUMAN_REVIEW
   - 门禁切换需 Owner 批准
4. **Case 05 Window 规范化差异**——Spark 与 DuckDB 的 ROW_NUMBER 窗口帧边界默认行为存在规范化差异，非代码 bug。**C 类保守阻断**：需人工确认语义等价后再解除阻断
5. **CASE WHEN condition 等价比较**——当前设计为 UNSUPPORTED，**按需建设，非当前优先级**。condition 是业务语义核心，人工审核是当前通道
6. **`_temp_` 前缀检测统一**（R-CA-2，**低优先级维护债**）
7. **生产环境 LLM 验证**——R8 脚本就绪，待 API key 配置后执行

## 6. 关键文档索引

| 文档 | 用途 |
|------|------|
| `AGENTS.md` | 项目宪法——所有 Agent、LLM 角色和自动化工具必须遵守 |
| `docs/README.md` | 文档分类索引与唯一入口 |
| `docs/00-product-charter.md` | 产品愿景和验收标准 |
| `docs/01-target-architecture.md` | 目标架构（设计参考） |
| `docs/09-test-strategy.md` | 测试策略 |
| `docs/pipeline_主链路详解_20260702_2140.md` | SQL 管线 Stage 1-7 内部实现细节 |
| `docs/CRE_v2_设计文档_20260713_1745.md` | CRE v2 双引擎编码比较体系 |
| `docs/CRE_v3_设计文档_20260713_2000.md` | CRE v3 CDP 工程化设计 |
| `docs/case_when条件对比边界说明_20260717_0908.md` | CASE WHEN condition UNSUPPORTED 取舍记录 |
| `docs/datadev_engineering_glossary_20260629_1600.md` | 工程术语表 |
| `docs/superpowers/specs/2026-07-15-label-table-design.md` | label_table v1 完整设计 |
| `docs/superpowers/specs/` | Spark-first Phase 6-8 完整设计 |
| `docs/superpowers/plans/README.md` | 方案书索引与执行链路 |
| `docs/examples/` | DeveloperSpec 示例（汇总表/标签表/多步骤加工） |

## 7. 运行环境说明

| 项目 | 说明 |
|------|------|
| Python | 系统 Python `D:\Program Files\Python312`（3.12.10） |
| 虚拟环境 | `.venv/` 由 uv 管理（Python 3.11.15），不含 pyspark |
| 服务启动 | `./dev-reload.sh` 已退出 .venv，固定使用系统 Python 路径；`scripts/dev_reload.py` 内 `sys.executable` 继承此路径 |
| 依赖完整性 | uvicorn、fastapi、pydantic、duckdb 等在系统 Python 中均可用 |
| PySpark | 4.1.2 安装在系统 Python site-packages；Java 17 可用；`JAVA_HOME` 指向 JDK 8（不影响 PySpark 4.x 在 Java 17 上运行） |
