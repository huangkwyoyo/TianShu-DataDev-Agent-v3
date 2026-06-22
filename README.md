# TianShu DataDev Agent v3

AI辅助数据开发工具：接收数据开发项目书，生成SQL、PySpark、测试和验证材料，最终输出供程序员审查的Code Review Package。

最终产物是代码，不是生产数据。系统不自动上线、不写生产库。

## 当前状态

- 当前阶段：**Phase 0.5 — 架构契约校正**。
- Phase 0脚手架已完成。
- Phase 0.5只校正规划、边界和路线图，不修改Python IR实现。
- 下一阶段：Phase 1类型化SQL纵向切片。

## 目标流程

```text
项目书
→ RequirementIR确认
→ SubIntent拆分
→ TransformationContract
→ 关系一致Parquet快照
    ├─ SQLPlan → Python编译SQL → DuckDB执行
    └─ SparkDeveloper → Validator → Reviewer
         → Developer修订 → Tester → Spark执行
→ 结果规范化与确定性交叉验证
→ 差异诊断与最多2轮返工
→ REVIEW_READY / HUMAN_REVIEW
→ Code Review Package
```

## 关键边界

### SQL

- LLM不生成SQL文本或SQL片段。
- LLM只输出严格类型化SQLPlan。
- Python编译器确定性生成SQL。
- SQL修复必须修改SQLPlan后重新编译。

### PySpark

PySpark只能以受控纯转换函数形式生成：

```python
def transform(inputs: Mapping[str, DataFrame], params: TransformParams) -> DataFrame:
    ...
```

代码只读取注入的inputs，禁止自行读取数据、Action、写入、UDF、网络、文件系统和动态执行。Reviewer只输出修订指令，最终修订仍由Developer完成；测试代码同样需要安全校验和隔离执行。

### 验证

SQL与Spark读取同一个关系一致冻结快照。确定性Comparator可以产生`CONSISTENT_SAMPLE`，但该状态只说明样本一致，不代表业务绝对正确、全量性能合格或获准上线。

### LangGraph

LangGraph只负责编排、分支、checkpoint、重试和人工中断。业务逻辑是普通Python服务；Graph State只保存artifact引用、哈希、状态和摘要。

## 规划文档

核心事实源：

- `docs/00-product-charter.md`
- `docs/01-target-architecture.md`
- `docs/03-sql-ir-and-compiler-plan.md`
- `docs/04-spark-multi-agent-plan.md`
- `docs/05-cross-validation-and-repair-plan.md`
- `docs/06-langgraph-orchestration-plan.md`
- `docs/07-harness-and-memory-plan.md`
- `docs/09-test-strategy.md`
- `docs/roadmap/phase-1-sql-vertical-slice.md`至`phase-4-repair-loop.md`

## 目录

```text
src/tianshu_datadev/
├── ir/              # 严格IR和artifact契约
├── sql/             # SQL规划解析和确定性编译
├── spark/           # Spark角色、静态校验和代码artifact
├── execution/       # 快照、DuckDB和Spark隔离执行
├── validation/      # 规范化、Comparator和MergePlan
├── orchestration/   # LangGraph薄编排层
├── artifacts/       # Code Review Package
└── llm/             # LLM Gateway、角色Prompt和Schema
```

## 开发命令

```powershell
pip install -e ".[dev]"
python -m pytest tests -q
python -m ruff check .
```

## 已知Phase 0质量状态

- pytest：22个用例。
- 测试数量已超过Phase 0原预算；Phase 1前应合并低价值Protocol反射测试。
- Phase 0.5不修改Python文件，已有ruff问题将在独立A类小修中处理。
