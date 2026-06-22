# TianShu DataDev Agent v3

AI 辅助数据开发工具。接收项目书，生成达到 **"开发审查级"** 的 PySpark 代码——即代码质量足以提交给程序员进行 code review 和上线决策。

## 定位

- **核心目标**：生成开发审查级 PySpark 代码——质量足以进入 code review，而非仅作原型参考
- **SQL 是验证手段**：DuckDB SQL 用于交叉验证 PySpark 代码的正确性，而非最终交付物
- **最终产物是 Code Review Package**：PySpark 代码 + 测试 + 验证报告 + 审查摘要
- **不上线，不写生产库**
- **人是最终代码审查者和上线决策者**

## 架构概要

```
项目书 → Requirement Analyzer → SubIntent Decomposer
                                   ├── SQLPlan → Python编译器 → DuckDB执行
                                   └── SparkDeveloper → Reviewer → Tester → Spark执行
                                   ↓
                              同源样本交叉验证 → LLM差异诊断 → 返工/HUMAN_REVIEW
                                   ↓
                              Code Review Package → 人工审查
```

## 项目状态

- **当前阶段**：Phase 0 — 项目骨架搭建
- **下一阶段**：Phase 1 — 单项目书 SQL 纵向切片

## 开发

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
python -m pytest tests -q
```

## 许可证

MIT
