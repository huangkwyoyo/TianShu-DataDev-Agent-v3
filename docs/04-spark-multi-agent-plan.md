# Spark 多 Agent 计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

设计 PySpark 分支的三个 LLM 角色隔离结构和 Spark Static Validator，确保 Spark 代码的安全性、正确性和可测试性。

## 2. 三角色隔离

### 2.1 SparkDeveloper（开发者）

**职责**：将 SubIntent 转换为 PySpark 代码

| 项目 | 内容 |
|------|------|
| 输入 | SubIntent（结构化） |
| 输出 | PySpark DSL 代码（字符串） |
| 约束 | 使用 PySpark DataFrame API，不使用 Spark SQL |
| Prompt 模式 | 给定 SubIntent → 生成 DataFrame 链式调用 |
| Schema 约束 | 输出必须包含有效的 PySpark 代码块 |

**不做的**：
- 不检查代码安全性（交给 Reviewer）
- 不生成测试代码（交给 Tester）
- 不执行代码

### 2.2 SparkReviewer（审查者）

**职责**：从安全和正确性角度审查 PySpark 代码

| 项目 | 内容 |
|------|------|
| 输入 | SparkDeveloper 输出的代码 + SubIntent |
| 输出 | 审查意见列表 |
| 检查维度 | JOIN 正确性、聚合逻辑、空值处理、数据类型匹配、性能风险 |

**检查重点**：
1. JOIN 条件是否完整（避免 Cartesian product）
2. GROUP BY 列是否与 SELECT 非聚合列一致
3. 空值处理是否明确（fillna / dropna）
4. 字段类型转换是否合理（cast）
5. 是否有不必要的 Shuffle 操作
6. 是否有危险 API 调用（如 `collect()` 不加 limit）

### 2.3 SparkTester（测试者）

**职责**：为 PySpark 代码生成测试规格和 Python 测试代码

| 项目 | 内容 |
|------|------|
| 输入 | PySpark 代码 + SubIntent |
| 输出 | 测试规格 + pytest 测试代码 |
| 测试框架 | pytest + chispa（DataFrame 断言） |

**输出规范**：
- 测试用例至少覆盖正常路径和一条边界路径
- 测试数据基于同源 Parquet 快照的子样本
- 测试代码可直接运行（不含外部依赖）

## 3. 隔离模式

三个角色使用 **同模型不同 Prompt 和 Schema** 的隔离方式：

```
SparkDeveloper:  Prompt A + Schema A → PySpark 代码
SparkReviewer:   Prompt B + Schema B → 审查意见
SparkTester:     Prompt C + Schema C → 测试代码
```

- 使用同一 LLM 模型（如 Claude / GPT）
- 每个角色的 Prompt 不同，输出 Schema 不同
- 上下文窗口不共享（每个角色是独立 LLM 调用）
- Reviewer 可以看到 Developer 的输出，但 Tester 不直接依赖 Developer 输出

## 4. Spark Static Validator

### 4.1 职责

对生成的 PySpark 代码进行静态安全分析，作为 LLM 审查的补充。

### 4.2 检查规则

| 规则 | 说明 | 严重程度 |
|------|------|----------|
| AST 白名单 | 只允许 DataFrame API 和安全的 Python 内置函数 | ERROR |
| 禁止模块导入 | 不允许 import os、import sys、import subprocess 等 | ERROR |
| 入口点强制 | 代码必须有一个明确的入口函数 | ERROR |
| 禁止 collect() | 禁止调用 collect() 方法（不含 limit） | WARNING |
| 禁止写文件 | 不允许 write / save 操作 | ERROR |
| 禁止 eval/exec | 不允许动态执行代码 | ERROR |

### 4.3 实现方式

```python
class SparkStaticValidator:
    """PySpark 代码静态安全分析器"""

    def validate(self, code: str) -> ValidationResult:
        """
        解析代码为 AST，逐节点检查。
        返回：通过/不通过 + 违规列表
        """
```

- 使用 Python `ast` 模块解析代码
- 不执行代码
- 所有规则可配置（启用/禁用、严重程度）

## 5. 参考 Legacy 的 12 层防御

Legacy 项目实现了 12 层 Spark 安全检查，包括：

| Layer | 描述 | v3 策略 |
|-------|------|---------|
| 1-3 | 关键字/正则/注入检测 | 不需要——代码由 LLM 生成，非用户输入 |
| 4-5 | DDL/DML 检测 | 纳入 Static Validator |
| 6-7 | 资源限制/死循环检测 | 执行时由运行环境控制 |
| 8-9 | 权限/数据敏感检测 | 不属于 v3 范围 |
| 10-12 | 代码风格/性能建议 | Reviewer 的职责 |

**v3 简化方案**：
- Static Validator：关注安全红线（禁止导入、禁止写文件、AST 白名单）
- SparkReviewer：关注代码正确性和性能
- SparkTester：关注可测试性
- 运行时限制：由执行引擎控制（超时、内存上限）

## 6. 测试边界

| 测试类型 | 覆盖内容 |
|----------|----------|
| Validator 正常 | 合法 PySpark 代码通过检查 |
| Validator 异常 | 包含禁止模块导入、禁止写文件等 |
| Validator 边界 | 嵌套函数、装饰器、lambda |
| 角色 Prompt | 各角色 Prompt 能稳定生成预期格式 |
| 代码执行 | 生成的代码能在本地 PySpark 中执行 |

## 7. 风险

| 风险 | 缓解 |
|------|------|
| LLM 生成不安全代码 | Static Validator 硬检查 + Reviewer 兜底 |
| 角色间信息不一致 | 所有角色使用同一 SubIntent 作为源头 |
| 测试过度/不足 | Tester 有明确的输出规格约束 |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
