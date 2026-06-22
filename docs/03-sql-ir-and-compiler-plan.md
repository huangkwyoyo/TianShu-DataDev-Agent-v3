# SQL IR 和编译器计划 — TianShu DataDev Agent v3

> 文档版本：Phase 0 初稿

## 1. 目标

设计三层 IR 结构和 Python 确定性编译器，确保 SQL 由编译器生成而非 LLM 直接输出，杜绝非法 SQL 进入执行层。

## 2. 三层 IR 结构

### 2.1 RequirementIR（需求层）

从项目书解析得到的结构化需求表示。

```python
@dataclass
class RequirementIR:
    """项目书解析后的结构化需求"""
    raw_text: str                     # 原始项目书文本
    data_source: list[str]            # 涉及的数据源（表名）
    analysis_goals: list[str]         # 分析目标列表
    output_requirements: dict         # 输出格式要求
    constraints: list[str]            # 约束条件
    business_context: str | None      # 业务上下文说明
```

**不包含**：LLM 评分、Token 统计、模型版本等运行时元数据。

### 2.2 SubIntent（子意图层）

原子可执行的子任务。

```python
@dataclass
class SubIntent:
    """原子可执行的子意图"""
    id: str                           # 唯一标识
    description: str                  # 自然语言描述
    target_tables: list[str]          # 目标表
    required_fields: list[str]        # 所需字段
    filter_conditions: list[dict]     # 过滤条件
    aggregation: dict | None          # 聚合定义
    join_relations: list[dict] | None # Join 关系
    ordering: list[dict] | None       # 排序要求
    limit: int | None                 # 行数限制
```

**不包含**：执行计划、SQL 片段、LLM 中间产物。

### 2.3 SQLPlan（SQL 计划层）

编译器输入，包含生成 SQL 所需的所有结构化信息。

```python
@dataclass
class SQLPlan:
    """SQL 编译器输入计划"""
    sub_intent_id: str                # 对应的 SubIntent ID
    source_tables: list[str]          # FROM 子句中的表
    join_clauses: list[JoinClause]    # JOIN 定义
    selected_columns: list[ColumnDef] # SELECT 列
    where_conditions: list[Condition] # WHERE 条件
    group_by: list[str] | None         # GROUP BY 字段
    having: list[Condition] | None     # HAVING 条件
    order_by: list[SortSpec] | None    # ORDER BY
    limit: int | None                  # LIMIT
    output_alias: str | None           # 输出别名
```

```python
@dataclass
class JoinClause:
    """JOIN 子句定义"""
    join_type: str                    # INNER / LEFT / RIGHT / FULL
    left_table: str
    right_table: str
    left_key: str
    right_key: str

@dataclass
class ColumnDef:
    """列定义"""
    table: str
    column: str
    alias: str | None
    aggregation: str | None            # SUM / COUNT / AVG / MIN / MAX / None

@dataclass
class Condition:
    """条件定义"""
    field: str                         # table.column 格式
    operator: str                      # =, !=, >, <, >=, <=, IN, LIKE, IS NULL, IS NOT NULL
    value: Any | None                  # 值（可为 None）
    logical_op: str = "AND"            # AND / OR

@dataclass
class SortSpec:
    """排序定义"""
    field: str
    direction: str = "ASC"             # ASC / DESC
```

**字段契约**：SQLPlan 仅包含生成 SQL 所需的结构化信息。不包含：
- LLM 评分或置信度
- 替代方案
- Token 统计
- 执行预估
- 安全风险评估

## 3. Python 确定性编译器设计

### 3.1 编译器签名

```python
def compile_sql(sql_plan: SQLPlan) -> str:
    """
    将 SQLPlan 编译为 DuckDB 兼容的 SQL 字符串。

    输入：严格类型化的 SQLPlan
    输出：有效的 DuckDB SQL 字符串
    约束：同一输入始终产生同一输出（确定性）
    """
```

### 3.2 编译器规则

1. 表名、字段名直接来自 TianShu `contracts/*.yml`，不修改、不猜测
2. JOIN 条件中的左右键必须存在于对应表的 Contract 定义中
3. 聚合函数只使用 DuckDB 支持的标准函数
4. 不生成任何 DDL 语句
5. 不生成 INSERT / UPDATE / DELETE
6. 输出格式化为可读的 SQL（关键字大写、缩进一致）
7. 编译器不执行任何安全检查——安全由输入契约保证

### 3.3 确定性保证

- 相同 SQLPlan → 相同 SQL 字符串（100% 确定）
- 编译器是无状态的纯函数
- 不使用随机数或当前时间等不确定因素
- 输出不依赖 LLM

## 4. 硬约束

| 约束 | 说明 |
|------|------|
| LLM 不生成 SQL 字符串 | LLM 只输出结构化的 SQLPlan，SQL 由编译器生成 |
| 表名来自事实源 | 所有表名必须存在于 TianShu 的 contracts 中 |
| 字段名来自事实源 | 所有字段名必须在对应表的 contracts 中有定义 |
| JOIN 关系来自事实源 | JOIN 条件中的字段必须在 contracts 中有关联关系 |
| 不执行外部数据 | 编译器不读取外部文件、不调用网络 API |

## 5. 不需要的旧项目安全检查

Legacy 项目有 6 层 SQL 安全检查：
1. 关键字黑名单 → v3 编译器不会生成危险语句
2. 正则表达式过滤 → v3 编译器生成的是结构化 SQL
3. SQL 注入检测 → v3 SQL 中无用户输入拼接
4. DDL 检测 → v3 编译器不生成 DDL
5. 多语句检测 → v3 编译器生成单条 SELECT
6. 权限评估 → v3 不涉及权限

**v3 方案**：编译器仅做输入验证（SQLPlan 字段合法性），不做运行时安全检查。

## 6. 编译器测试边界

| 测试类型 | 覆盖内容 |
|----------|----------|
| 正常路径 | 各种标准 SELECT、JOIN、GROUP BY、HAVING、ORDER BY、LIMIT |
| 边界情况 | 空 SELECT、空 WHERE、NULL 条件、空 Join 列表 |
| 异常输入 | 字段名不存在于 contracts、表名不存在、非法操作符 |
| 确定性 | 两次相同输入 → 两次相同输出 |

---

> Phase 0 初稿 | 2026-06-22 | 待后续阶段细化
