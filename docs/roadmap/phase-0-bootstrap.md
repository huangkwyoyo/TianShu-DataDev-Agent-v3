# Phase 0 / 0.5：项目启动与架构契约校正

## Phase 0已完成

- 创建项目骨架、核心文档、Protocol探索模型和22个基础测试。
- 完成TianShu、Text2SQL Agent和legacy Data Dev Agent复用审计。
- 明确新项目不复制旧项目实现，不继承发布和物化体系。

Phase 0产物是探索基线，不是最终运行时契约。`src/tianshu_datadev/ir/protocols.py`中的自由字符串和宽泛Protocol不得直接进入Phase 1实现。

## Phase 0.5目标

在进入Phase 1前统一以下架构事实：

1. SQLPlan必须使用类型化表达式AST，禁止LLM间接生成SQL片段。
2. LLM结构化输出使用严格Pydantic/JSON Schema，而不是仅靠Protocol。
3. PySpark固定为`transform(inputs, params) -> DataFrame`纯转换函数。
4. 多表样本使用关系一致快照，不按表独立LIMIT。
5. 验证采用精确状态，`CONSISTENT_SAMPLE`不等于生产正确。
6. Graph State只保存artifact引用、哈希、状态和摘要。
7. Domain Knowledge由TianShu Fact Catalog提供，不建设可写Domain Memory。
8. Phase 1-7路线按类型化SQL、Phase 1.2性能契约、Phase 1.5开窗函数、Spark、验证、返工、前端、Harness、真实LLM依次推进。

## 本阶段不做

- 不修改Python IR实现。
- 不实现SQL编译、Spark生成、执行器或LangGraph。
- 不接真实LLM、数据库或前端。
- 不新增针对文档措辞的pytest。

## 验收

- 核心规划和Phase 1-7路线无相互冲突。
- AGENTS和README与目标架构一致。
- 现有22个测试保持通过。
- ruff现有问题被明确记录并留给独立A类小修。
- Phase 1能依据文档制定独立实现计划。

---

> Phase 0完成；Phase 0.5文档校正 | 2026-06-22
