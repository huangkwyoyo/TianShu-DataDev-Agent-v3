# Phase 1A：DeveloperSpec Parser + SourceManifest

> 状态：待实施
> 前置依赖：Phase 0.5 文档迁移完成

## 执行前必须阅读

1. `AGENTS.md` §2 — SQL Generation Boundary
2. `docs/01-target-architecture.md` §2.1 — SourceManifest 与 SchemaRegistry 冲突策略
3. `docs/03-sql-ir-and-compiler-plan.md` §3.1 — ParsedDeveloperSpec 结构
4. `docs/03-sql-ir-and-compiler-plan.md` §5 — SourceManifest 事实源解析
5. `docs/09-test-strategy.md` §7 Phase 1A

## 只允许修改

- `src/tianshu_datadev/developer_spec/` — 新建模块
  - `parser.py`：确定性 Markdown + YAML-like Parser
  - `models.py`：ParsedDeveloperSpec / OpenQuestion / SourceConflict / ParseWarning Pydantic 模型
  - `source_manifest.py`：SourceManifest 构建器
  - `field_normalizer.py`：字段名归一化（大小写统一、驼峰转下划线、常见别名字典）
- `tests/` — 新增 test_parser.py / test_source_manifest.py / test_field_normalizer.py
- `src/tianshu_datadev/ir/protocols.py` — 标记 deprecated（不删除，仅加注释）

## 禁止修改

- `src/tianshu_datadev/planning/` — 下一阶段
- `src/tianshu_datadev/sql/` — 下一阶段
- `src/tianshu_datadev/spark/` — Phase 5 前不碰
- 任何现有测试逻辑（只新增，不修改现有 22 个）

## 新增模型（Pydantic `extra="forbid"`）

### ParsedDeveloperSpec

```python
class ParsedDeveloperSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    spec_id: str
    spec_hash: str                          # normalized_spec_hash
    title: str
    description: str
    input_tables: list[InputTableDecl]
    metrics: list[MetricDecl]
    dimensions: list[DimensionDecl]
    joins: list[JoinDecl] | None
    time_range: TimeRangeDecl | None
    output_spec: OutputSpecDecl
    open_questions: list[OpenQuestion]
    parse_warnings: list[ParseWarning]

class InputTableDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table_alias: str
    source_table: str
    columns: list[ColumnDecl]
    filters: list[FilterDecl]

class ColumnDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column_name: str                        # 原始字段名
    normalized_name: str                    # 归一化字段名
    data_type: str | None
    enum_values: list[str] | None
    nullable: bool | None
    unique: bool | None

class MetricDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metric_name: str
    aggregation: str                        # COUNT/SUM/AVG/MIN/MAX/COUNT_DISTINCT
    input_column: str | None
    alias: str

class DimensionDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dimension_name: str
    column_ref: str

class JoinDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: str                          # INNER/LEFT/RIGHT/FULL

class TimeRangeDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column_ref: str
    start: str
    end: str
    inclusive: bool = True

class OutputSpecDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    columns: list[str]
    grain: list[str]
    sort: list[SortDecl] | None
    limit: int | None
```

### OpenQuestion / SourceConflict / ParseWarning

```python
class OpenQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question_id: str
    source: str                             # "parser" | "source_manifest" | "relationship"
    field_ref: str | None
    description: str
    blocking: bool                          # True = 阻断后续流程
    resolution: HumanResolution | None

class SourceConflict(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field_ref: str
    table_ref: str
    developer_spec_value: str
    schema_registry_value: str
    conflict_type: str                      # TYPE_MISMATCH | ENUM_MISMATCH | UNIQUENESS_MISMATCH | MISSING_IN_REGISTRY

class ParseWarning(BaseModel):
    model_config = ConfigDict(extra="forbid")
    warning_id: str
    field_ref: str | None
    message: str
    severity: str                           # LOW | MEDIUM
```

### SourceManifest

```python
class SourceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    manifest_id: str
    spec_hash: str
    tables: list[ManifestTable]
    conflicts: list[SourceConflict]
    anomalies: list[SourceAnomaly]

class ManifestTable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table_ref: str
    source_table: str
    columns: list[ManifestColumn]
    primary_key: list[str] | None
    foreign_keys: list[ForeignKeyRef] | None
    estimated_row_count: int | None

class ManifestColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column_name: str
    normalized_name: str
    data_type: str
    nullable: bool
    unique: bool | None
    enum_values: list[str] | None
    source: FieldSource                     # developer_spec | schema_registry | snapshot_profile

class FieldSource(str, Enum):
    DEVELOPER_SPEC = "developer_spec"
    SCHEMA_REGISTRY = "schema_registry"
    SNAPSHOT_PROFILE = "snapshot_profile"

class SourceAnomaly(BaseModel):
    model_config = ConfigDict(extra="forbid")
    anomaly_id: str
    table_ref: str
    column_ref: str | None
    description: str
    anomaly_type: str                       # MISSING_IN_REGISTRY | TYPE_CONFLICT | UNEXPECTED_NULL
```

### Parser 错误码表

| 错误码 | 含义 | blocking |
|--------|------|----------|
| `E001` | YAML metadata block 解析失败 | 是 |
| `E002` | 必填字段缺失（如 input_tables 为空） | 是 |
| `E003` | 表别名与物理表名映射不明确 | 是 |
| `E004` | 未声明字段被引用（如指标引用了不存在的列） | 是 |
| `W001` | 字段类型未声明，需从 SchemaRegistry 补充 | 否 |
| `W002` | 时间范围未指定，将使用全量数据 | 否 |
| `W003` | Join 声明存在但关联键类型未指定 | 否 |
| `W004` | 输出排序声明但未指定方向，默认 ASC | 否 |

### Parser 允许宽松 6 项

| # | 允许场景 | golden fixture |
|----|----------|----------------|
| 1 | 字段类型未声明——Parser 不拒绝，由 SourceManifest 从 SchemaRegistry 补充 | `golden_type_inferred_from_registry` |
| 2 | 时间范围未指定——Parser 生成 W002 警告，不阻断 | `golden_no_time_range` |
| 3 | Join 未显式声明——Parser 不要求，留给 RelationshipHypothesis 推理 | `golden_no_explicit_joins` |
| 4 | 输出排序未声明——Parser 不拒绝 | `golden_no_output_sort` |
| 5 | Markdown 正文中有额外非结构化说明——Parser 保留在 description 中，不拒绝 | `golden_extra_markdown_text` |
| 6 | 字段注释中存在中文——归一化正常处理 | `golden_chinese_column_comments` |

### Parser 禁止宽松 7 项

| # | 禁止场景 | 拒绝 fixture |
|----|----------|--------------|
| 1 | YAML metadata block 不存在或完全无法解析 | `reject_missing_metadata` |
| 2 | `input_tables` 为空数组 | `reject_empty_input_tables` |
| 3 | 指标引用了不在任何 input_table 中的字段 | `reject_metric_refs_missing_column` |
| 4 | 两个表使用相同别名 | `reject_duplicate_table_alias` |
| 5 | Join 声明引用了不存在的表别名 | `reject_join_refs_missing_table` |
| 6 | 输出列列表为空 | `reject_empty_output_columns` |
| 7 | `raw_sql`、`where_sql`、`expression: str` 字段出现在任何声明中 | `reject_free_sql_field` |

## artifact schema

- `ParsedDeveloperSpec` JSON（含 open_questions 和 parse_warnings）
- `SourceManifest` JSON（含 conflicts 和 anomalies）
- `normalized_spec_hash` 记录在 provenance 中

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| golden fixture | 6 | 每项允许宽松场景 1 个 |
| 拒绝 fixture | 7 | 每项禁止宽松场景 1 个 |
| Schema 严格性 | 4 | extra 字段拒绝、必填字段缺失、枚举非法值、类型错误 |
| SourceManifest 冲突 | 3 | SOURCE_CONFLICT 输出双方值、SchemaRegistry 不静默覆盖、SOURCE_ANOMALY |
| 字段归一化 | 4 | 大小写统一、驼峰转下划线、别名替换、去特殊字符 |
| hash 确定性 | 1 | 相同输入两次解析 hash 一致 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "parser or source_manifest or field_normalizer"
python -m ruff check src/tianshu_datadev/developer_spec/
git diff --check
```

## B/C 暂停条件

- Parser 遇到无法分类的 DeveloperSpec 写法模式（需新增允许/禁止项）
- SchemaRegistry 集成方式需要选择（git submodule / REST API / 本地文件）
- 字段名归一化字典的初始词条范围存在争议

## 退出条件

1. 6 个 golden fixture 全部通过
2. 7 个拒绝 fixture 全部正确拒绝
3. ParsedDeveloperSpec / OpenQuestion / SourceConflict / SourceManifest 严格 Schema——extra 字段拒绝
4. `normalized_spec_hash` 确定性验证通过
5. SourceManifest 字段来源标记正确（developer_spec / schema_registry / snapshot_profile）
6. SOURCE_CONFLICT 正确输出双方值，SchemaRegistry 不可静默覆盖
7. REQUIRED 字段缺失生成 OpenQuestion(blocking=true)
8. 现有 22 个测试保持通过

---

> Phase 1A | 待实施 | 前置：Phase 0.5 完成
