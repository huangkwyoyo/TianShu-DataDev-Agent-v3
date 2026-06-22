# Phase 2：Spark 多智能体代码生成

## 目标

引入三个 AI 角色（SparkDeveloper、SparkReviewer、SparkTester）协作生成 PySpark DataFrame DSL 代码和对应的测试代码。三个角色使用同一底层模型但拥有独立的 System Prompt 和结构化输出 Schema，形成可控的代码生成流水线。

## 输入

- **SubIntent**：与 Phase 1 相同的 SubIntent 结构，作为 Spark 代码生成的业务输入
- **Spark Static Validator 规则**：AST 级别安全分析的检查规则列表

## 输出

| 产出 | 说明 |
|------|------|
| **SparkDeveloper 响应** | 生成 PySpark DataFrame DSL 代码（含 import、DataFrame 转换链、输出） |
| **SparkReviewer 响应** | 对 Developer 输出的审查意见，包含正确性、性能、安全性评分 |
| **SparkTester 响应** | 生成对应的 PySpark 单元测试代码（基于 pytest + assert_frame_equal） |
| **Spark Static Validator 报告** | AST 安全分析报告，标记 12 类安全问题（参考 legacy 项目的防御思路） |
| **三方协作流水线** | Developer -> Reviewer（通过/驳回）-> Tester 的顺序执行链路 |

## 模块职责

- **spark_developer/**：定义 SparkDeveloper 角色的 System Prompt 和输出 Schema，封装 LLM 调用，输出 PySpark DataFrame DSL 代码
- **spark_reviewer/**：定义 SparkReviewer 角色的 System Prompt 和输出 Schema，对 Developer 代码进行审查评分，输出 Pass/Revise 决定及具体改进意见
- **spark_tester/**：定义 SparkTester 角色的 System Prompt 和输出 Schema，根据 Developer 最终代码生成 pytest 测试代码
- **spark_static_validator/**：AST 级别安全分析，不使用简单子串匹配。参考 legacy 项目 `spark_safety.py` 的 12 层防御思路，覆盖：
  1. 动态表达式执行（`expr()`、`regexp_replace` 等）
  2. UDF 注册（`udf`、`pandas_udf`）
  3. 文件系统操作（`write`、`save`、`overwrite`）
  4. SQL 内嵌（`spark.sql` 调用）
  5. 反射调用（`getattr`、`invoke`）
  6. 网络请求（`urllib`、`requests`）
  7. 子进程调用（`subprocess`、`os.system`）
  8. 文件读取（`read`、`load`、`text`）
  9. 循环/递归（`for`、`while`、递归函数定义）
  10. 动态导入（`__import__`、`importlib`）
  11. 变量覆盖（内置函数名重用）
  12. 非白名单方法调用
- **pipeline_orchestrator/**：编排三个角色的调用顺序，处理 Reviewer 驳回时的重试逻辑

## 明确不做什么

- 不生成 SQL 代码（与 Phase 1 职责分离）
- 不执行 Spark 代码（执行和验证在 Phase 3）
- 不涉及 DuckDB 或 Cross-validation
- 不涉及 Repair Loop 或差异分析
- 不涉及前端展示
- 不接入真实 LLM（本阶段用 Mock LLM 开发，Phase 7 接入真实 LLM）

## 契约

- **角色输出 Schema**：三个角色各有独立的 Pydantic/JsonSchema 输出模型，在 `contracts/` 中定义
- **Validator 输入/输出**：输入为 AST 树（`ast.AST`），输出为 `SafetyReport` dataclass（含风险级别、风险描述、代码位置）
- **流水线契约**：Developer 输出 -> Reviewer 输入；Reviewer 通过后 -> Tester 输入

## 风险

| 风险 | 缓解措施 |
|------|----------|
| 角色 Prompt 边界模糊导致角色混淆 | 独立 System Prompt + Schema 强约束，角色名在 Prompt 和接口中显式区分 |
| Reviewer 过于严格或宽松 | Reviewer Schema 中设置评分卡（Checklist 形式），减少主观判断空间 |
| AST Validator 漏报/误报 | 参考 legacy 项目已验证的 12 层防御规则，加回归测试套件 |
| Mock LLM 与真实 LLM 行为差异 | Mock 返回预设的"完美"代码，真实 LLM 接入在 Phase 7 独立验证 |
| 代码生成质量不稳定 | Reviewer 提供结构化反馈，允许单轮重试（超过则降级到 SQL 路径） |

## 验收标准

1. [ ] SparkDeveloper 能根据 SubIntent 生成语法正确、风格规范的 PySpark DataFrame DSL 代码
2. [ ] SparkReviewer 能识别常见的正确性错误和性能问题
3. [ ] SparkTester 能为 Developer 代码生成对应的 pytest 测试代码
4. [ ] 三方流水线在 Mock LLM 模式下能完整运行 Developer -> Reviewer -> Tester
5. [ ] Spark Static Validator 在 AST 级别正确识别全部 12 类安全问题，回归测试通过
6. [ ] Reviewer 驳回时支持单次重试（Developer 重新生成）
7. [ ] 所有角色输出符合 `contracts/` 中定义的 Schema

## 测试边界

- **测试范围**：三方 Prompt 效果测试、AST Validator 规则测试、流水线编排测试
- **不测试**：真实 LLM 调用、Spark 执行结果、前端展示
- **隔离要求**：使用 Mock LLM（`unittest.mock` 或自定义 stub），不依赖真实 LLM API
- **异常测试**：Developer 输出空代码、Reviewer Schema 解析失败、Tester 生成错误格式代码

## 与其他阶段的依赖

- **依赖 Phase 0**：依赖 `contracts/` 中定义的 SubIntent 和角色输出 Schema
- **被 Phase 3 依赖**：Phase 3 双引擎验证依赖本阶段的 Spark 代码和测试代码
- **与 Phase 1 并行**：本阶段与 Phase 1 SQL 垂直切片可独立开发，但需协商 SubIntent 的契约一致性

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
