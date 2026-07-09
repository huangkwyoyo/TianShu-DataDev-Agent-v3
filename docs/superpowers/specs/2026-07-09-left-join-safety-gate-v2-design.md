# LEFT JOIN 右表唯一性安全门禁 V2——dim 表提示增强设计

> 状态：设计完成 | 日期：2026-07-09

## 背景

Phase 1 LEFT JOIN 安全门禁已实现。核心逻辑：LEFT JOIN 右表 join key 无唯一性证据（primary_key / unique_keys / SchemaRegistry）时，生成 blocking OpenQuestion 阻断流程，防止静默笛卡尔积。

实际使用中暴露问题：dim 维度表（如 `silver.taxi_zone`，265 行，key_column 为 `location_id`）作为 LEFT JOIN 右表时被阻断。用户在 DeveloperSpec 中未给 key_column 写 `unique: true`，而系统缺少手段区分"真正危险的 LEFT JOIN"和"明显安全的维度表关联"。

本设计（V2）的核心改进：**不因 dim 表自动放行，但生成更精准、可操作的阻断提示，引导用户用最小的标注代价解决阻断。** 同时补齐 V1 遗留的四个架构缺口：YAML `unique_keys` 语法、Registry 主键链路、lookup 类型重命名、字段归一化。

## 设计原则

1. **门禁不放水**：只有 `primary_key` / `unique_keys` / SchemaRegistry unique index 能放行——与 Phase 1 一致
2. **dim 表只改善文案**：role=dim + join_key ∈ key_column_names 时，阻断消息更具体、可操作
3. **LLM 不碰门禁决策**：LLM 仅在 Phase 2+ 生成修复建议文案，不参与通过/阻断判断
4. **统一归一化**：所有列名比较统一经过 `FieldNormalizer.normalize()`
5. **公共合并函数**：unique_keys 的合并/去重逻辑集中在一个纯函数中

## 第一节：ManifestTable 模型扩展

### `developer_spec/models.py` — ManifestTable

```python
class ManifestTable(StrictModel):
    table_ref: str
    source_table: SafePhysicalTableName
    columns: list[ManifestColumn] = []
    primary_key: list[str] | None = None
    foreign_keys: list[ForeignKeyRef] | None = None
    unique_keys: list[list[str]] | None = None
    estimated_row_count: int | None = None
    # ── 新增 ──
    role: str | None = None
    """表角色——"fact" | "dim" | None。从 InputTableDecl.role 透传。"""
    key_column_names: list[str] = []
    """key_columns 的归一化列名集合。由 SourceManifestBuilder 从
    InputTableDecl.key_columns 提取，经 FieldNormalizer.normalize() 处理。
    用于 LEFT JOIN 安全门禁判断 join key 是否属于维度键。"""
```

### `developer_spec/models.py` — InputTableDecl

```python
class InputTableDecl(StrictModel):
    # ... 现有字段不变 ...
    # ── 新增 ──
    unique_keys: list[list[str]] | None = None
    """开发者显式声明的唯一键集合。每个元素是一组列名，表示这组列在表中值唯一。
    示例：unique_keys: [["location_id"], ["zone_name", "borough"]]
    由 _parse_input_tables 从 YAML source_tables[*].unique_keys 解析。"""
```

## 第二节：SourceManifestBuilder 透传 + Registry 主键收口

### 公共合并函数（新增，`source_manifest.py` 内）

```python
def _normalize_unique_keys_list(keys: list[list[str]] | None) -> list[list[str]]:
    """将 unique_keys 列表归一化：列名小写、组内排序、组间去重。

    Args:
        keys: 原始 unique_keys 列表，None 等价于 []

    Returns:
        归一化后的唯一键组列表，每个键组内列名已小写且排序，组间无重复
    """
    if not keys:
        return []
    merged: set[tuple[str, ...]] = set()
    for kg in keys:
        if kg:
            merged.add(tuple(sorted(k.lower() for k in kg)))
    return [list(g) for g in merged]


def _merge_unique_keys_from_sources(
    *sources: list[list[str]] | None,
) -> list[list[str]]:
    """合并多个来源的 unique_keys，经归一化去重后返回。

    各来源等同对待，统一 set 去重。来源优先级由调用方控制传参顺序。
    """
    all_keys: list[list[str]] = []
    for src in sources:
        if src:
            all_keys.extend(src)
    return _normalize_unique_keys_list(all_keys) if all_keys else []
```

### `_build_manifest_tables` 变更

```python
def _build_manifest_tables(self, spec: ParsedDeveloperSpec) -> list[ManifestTable]:
    tables: list[ManifestTable] = []
    for input_t in spec.input_tables:
        # ... 现有 columns 构建逻辑不变 ...

        # 提取主键
        primary_key = [c.column_name for c in input_t.key_columns if c.unique]

        # 提取 key_column_names（归一化）
        key_column_names = [self._normalizer.normalize(c.column_name)
                           for c in input_t.key_columns]

        # 合并 unique_keys ——三来源合并
        pk_as_list = [primary_key] if primary_key else None
        unique_keys = _merge_unique_keys_from_sources(
            pk_as_list,              # 来源 1：primary_key
            input_t.unique_keys,     # 来源 2：YAML 显式声明
        )

        tables.append(ManifestTable(
            table_ref=input_t.table_alias,
            source_table=input_t.source_table,
            columns=columns,
            primary_key=primary_key if primary_key else None,
            unique_keys=unique_keys if unique_keys else None,
            estimated_row_count=input_t.row_count,
            role=input_t.role,                    # ── 新增
            key_column_names=key_column_names,    # ── 新增
        ))
    return tables
```

### `_supplement_from_registry` 补主键链路

```python
# 从 SchemaRegistry 补充 unique_keys（合并而非覆盖）
reg_unique_keys = registry_meta.get("unique_keys")
reg_pk = registry_meta.get("primary_key")
reg_pk_as_list = [reg_pk] if reg_pk and isinstance(reg_pk, list) and len(reg_pk) > 0 else None

# 合并到现有 unique_keys
reg_merged = _merge_unique_keys_from_sources(reg_unique_keys, reg_pk_as_list)
if reg_merged:
    existing = table.unique_keys or []
    all_merged = _merge_unique_keys_from_sources(existing, reg_merged)
    if all_merged:
        object.__setattr__(table, "unique_keys", all_merged)
```

### `build_manifest_from_spec` 同步变更

与 `_build_manifest_tables` 一致：透传 role/key_column_names，调用 `_merge_unique_keys_from_sources`。

## 第三节：lookup 类型重命名 + 富结构

### `planning/relationship_validator.py` — TableKeyInfo 数据类

```python
from dataclasses import dataclass, field

@dataclass
class TableKeyInfo:
    """LEFT JOIN 安全门禁所需的表唯一性元数据。

    由 _build_unique_keys_lookup 从 SourceManifest 构建，
    传给 check_left_join_safety 做唯一性判断。
    """
    unique_keys: list[list[str]] = field(default_factory=list)
    """已声明的唯一键组（经 _normalize_unique_keys_list 处理）。
    [] 表示"已查询但无声明"，区别于 None 表示"未查询"。"""
    role: str | None = None
    """表角色——"fact" | "dim" | None。"""
    key_column_names: list[str] = field(default_factory=list)
    """key_columns 列名（已归一化小写），用于判断 join key 是否属于维度键。"""
```

### `planning/relationship_planner.py` — `_build_unique_keys_lookup` 重命名 + 签名变更

```python
@staticmethod
def _build_unique_keys_lookup(
    manifest: SourceManifest | None,
) -> dict[str, TableKeyInfo]:
    """从 SourceManifest 构建 {table_ref: TableKeyInfo} 查询表。"""
    if manifest is None:
        return {}
    lookup: dict[str, TableKeyInfo] = {}
    for table in manifest.tables:
        lookup[table.table_ref] = TableKeyInfo(
            unique_keys=table.unique_keys or [],
            role=table.role,
            key_column_names=table.key_column_names,
        )
    return lookup
```

### `FakeRelationshipPlanner` 调用链变更

```python
# plan() 中
table_key_info = self._build_unique_keys_lookup(manifest)
# ...
open_q = self._rate_and_decide(candidate, table_key_info)

# _rate_and_decide() 签名
def _rate_and_decide(
    self,
    candidate: JoinCandidate,
    table_key_info: dict[str, TableKeyInfo] | None = None,  # 重命名
) -> OpenQuestion | None:

# _check_left_join_safety_gate() 签名
def _check_left_join_safety_gate(
    self,
    candidate: JoinCandidate,
    table_key_info: dict[str, TableKeyInfo] | None,
) -> OpenQuestion | None:
    if candidate.join_type != JoinType.LEFT:
        return None
    info = table_key_info.get(candidate.right_table) if table_key_info else None
    is_safe, desc = self._validator.check_left_join_safety(
        right_table_unique_keys=info.unique_keys if info else None,
        right_join_key=candidate.right_key,
        right_table_key_info=info,
    )
    # ...
```

### `RelationshipPlanner` 调用链同步

`_llm_plan()` 和 `_rate_and_decide_llm()` 中：
- `self._fake._build_unique_keys_lookup(manifest)` 返回类型改为 `dict[str, TableKeyInfo]`
- 传递参数名从 `table_unique_keys` 改为 `table_key_info`

## 第四节：Validator 增强——统一 normalizer + 文案分层

### `check_left_join_safety` 完整签名

```python
def check_left_join_safety(
    self,
    right_table_unique_keys: list[list[str]] | None,
    right_join_key: str,
    right_table_key_info: TableKeyInfo | None = None,
) -> tuple[bool, str | None]:
    """检查 LEFT JOIN 右表联结键是否有唯一性保证。

    Args:
        right_table_unique_keys: 右表的 unique_keys 列表。None 表示未查询。
        right_join_key: 右表联结键的原始字段名。
        right_table_key_info: 右表的完整 KeyInfo（含 role + key_column_names），
                              用于生成更精准的阻断提示。None 时退化为 V1 行为。

    Returns:
        (is_safe, description)。safe 时 description 为 None。
    """
```

### 归一化策略

`unique_keys` 和 `key_column_names` 在 Builder 阶段经 `_normalize_unique_keys_list` / `FieldNormalizer.normalize()` 处理为小写。Validator 中直接使用 `.lower()` 比较——两边已对齐，无需 Validator 持有 normalizer 实例。

```python
def check_left_join_safety(self, ...):
    # 归一化 join key——与 unique_keys / key_column_names 的预归一化一致
    right_key_lower = right_join_key.lower()

    # 无任何唯一性声明 → unsafe
    if not right_table_unique_keys:
        return self._build_unsafe_result_no_unique_keys(
            right_join_key, right_table_key_info
        )

    # 检查是否有唯一键组覆盖 join key
    for key_group in right_table_unique_keys:
        if right_key_lower in key_group and len(key_group) == 1:
            return (True, None)

    # 有声明但不覆盖 → unsafe
    return self._build_unsafe_result_no_coverage(
        right_join_key, right_table_unique_keys
    )
```

### 三种阻断文案层级

```python
def _build_unsafe_result_no_unique_keys(
    self,
    right_join_key: str,
    key_info: TableKeyInfo | None,
) -> tuple[bool, str]:
    """构建"无唯一性声明"的阻断结果，dim 表生成增强文案。"""
    # 场景 2：dim 表 + join_key 属于 key_column_names → 增强文案
    if (
        key_info is not None
        and key_info.role == "dim"
        and right_join_key.lower() in key_info.key_column_names
    ):
        return (
            False,
            f"dim 表的 key_column '{right_join_key}' 未声明唯一性。"
            f"请在 DeveloperSpec 中为该列添加 'unique: true'，"
            f"或在 source_tables 对应条目中声明 "
            f"unique_keys: [['{right_join_key}']]。"
            f"若该键确实有重复值，请提供去重策略说明。",
        )
    # 场景 1：通用阻断
    return (
        False,
        f"LEFT JOIN 右表 '{right_join_key}' 无唯一性保证："
        f"右表未声明 primary_key 且 ManifestTable.unique_keys 为空。"
        f"若该键有重复值，将导致静默笛卡尔积、左表度量值膨胀。"
        f"请在 DeveloperSpec 中为右表声明 unique_keys，或提供去重策略说明。",
    )
```

注意：`right_join_key.lower()` 用于与预归一化的 `key_column_names` 比较。`unique_keys` 和 `key_column_names` 均由 Builder 在写入时归一化（小写），Validator 侧统一使用 `.lower()` 即可保持一致性。

## 第五节：Parser 校验

### `_parse_input_tables` 新增 unique_keys 解析

YAML 语法：

```yaml
source_tables:
  - name: silver.taxi_zone
    alias: tz
    unique_keys:
      - [location_id]           # 单列唯一键
      - [zone_name, borough]    # 复合唯一键
```

解析逻辑：

```python
def _parse_input_tables(self, raw_tables: list[dict]) -> list[InputTableDecl]:
    for raw in raw_tables:
        # ... 现有解析 ...
        # ── 新增：解析 unique_keys ──
        raw_unique_keys = raw.get("unique_keys")
        unique_keys = None
        if raw_unique_keys is not None:
            if not isinstance(raw_unique_keys, list):
                parse_warnings.append(ParseWarning(
                    code="W005",
                    message=f"表 '{alias}' 的 unique_keys 必须是列表，"
                            f"收到 {type(raw_unique_keys).__name__}——已忽略",
                ))
            else:
                validated = []
                for i, kg in enumerate(raw_unique_keys):
                    if not isinstance(kg, list) or len(kg) == 0:
                        parse_warnings.append(ParseWarning(
                            code="W005",
                            message=f"表 '{alias}' 的 unique_keys[{i}] 必须是非空列表——已跳过",
                        ))
                    else:
                        # 校验列名存在于 key_columns/business_columns 中
                        all_col_names = {c.name for c in key_columns} | {c.name for c in biz_columns}
                        unknown = [k for k in kg if k not in all_col_names]
                        if unknown:
                            parse_warnings.append(ParseWarning(
                                code="W006",
                                message=f"表 '{alias}' 的 unique_keys[{i}] 引用了未知列："
                                        f"{unknown}——保留但请核实",
                            ))
                        validated.append(kg)
                if validated:
                    unique_keys = validated
        # ...
        InputTableDecl(..., unique_keys=unique_keys)
```

校验规则：
1. `unique_keys` 值必须是 `list`，否则 `W005` 警告 + 忽略
2. 内层元素必须是非空 `list`，否则 `W005` 警告 + 跳过该组
3. 引用的列名必须在 `key_columns` 或 `business_columns` 中存在，否则 `W006` 警告 + 保留（不静默丢弃）

## 测试矩阵

共 8 个测试（V1 的 17 个保留，新增 8 个聚焦 V2 变更）：

| # | 文件 | 测试 | 场景 | 预期 |
|---|------|------|------|------|
| 1 | `test_left_join_safety.py` | `test_dim_key_without_unique_blocks` | role=dim, key_column_names=["location_id"], join="location_id", unique_keys=[] | blocking=True, 文案含 "unique: true" 和 "unique_keys: [['location_id']]" |
| 2 | `test_left_join_safety.py` | `test_dim_key_with_unique_keys_passes` | role=dim, unique_keys=[["location_id"]] | 通过 |
| 3 | `test_left_join_safety.py` | `test_fact_no_unique_blocks` | role=fact, unique_keys=[] | blocking=True, 文案不含 "key_column" 措辞 |
| 4 | `test_left_join_safety.py` | `test_key_column_names_normalized` | key_column 在 YAML 中声明为 "Location_ID"，key_column_names 存 ["location_id"]，join_key="location_id" | 匹配成功（阻断文案含增强提示） |
| 5 | `test_left_join_safety.py` | `test_table_key_info_default_factory` | TableKeyInfo() 默认构造，unique_keys=[]，role=None，key_column_names=[] | 不抛异常，安全门禁正常阻断 |
| 6 | `test_source_manifest.py` | `test_merge_unique_keys_from_registry_pk` | Registry 返回 primary_key=["loc_id"] | manifest.unique_keys 含 ["loc_id"] |
| 7 | `test_source_manifest.py` | `test_builder_transmits_role_and_key_columns` | InputTableDecl.role="dim", key_columns=[ColumnDecl("Location_ID")] | ManifestTable.role="dim", key_column_names=["location_id"] |
| 8 | `test_parser.py` | `test_parser_rejects_invalid_unique_keys` | YAML unique_keys: "not_a_list" | ParseWarning W005 |

## 变更文件清单

| 文件 | 变更类型 | 内容 |
|------|----------|------|
| `developer_spec/models.py` | 修改 | `ManifestTable` 加 `role` + `key_column_names`；`InputTableDecl` 加 `unique_keys` |
| `developer_spec/source_manifest.py` | 修改 | 新增 `_normalize_unique_keys_list` + `_merge_unique_keys_from_sources`；`_build_manifest_tables` 透传 role/key_column_names + 合并 unique_keys；`_supplement_from_registry` 补 registry PK；`build_manifest_from_spec` 同步 |
| `developer_spec/parser.py` | 修改 | `_parse_input_tables` 解析 `unique_keys` + 校验 W005/W006 |
| `planning/relationship_validator.py` | 修改 | 新增 `TableKeyInfo` dataclass（含 default_factory）；`check_left_join_safety` 签名加 `right_table_key_info` 参数；新增 `_build_unsafe_result_no_unique_keys` 分层文案方法 |
| `planning/relationship_planner.py` | 修改 | `_build_unique_keys_lookup` 返回类型改为 `dict[str, TableKeyInfo]`；`_check_left_join_safety_gate` 透传 `TableKeyInfo`；`_llm_plan` 参数名更新 |
| `tests/planning/test_left_join_safety.py` | 修改 | 新增 5 个 V2 测试（见矩阵 #1-5） |
| `tests/unit/test_source_manifest.py` 或新建 | 修改/新建 | 新增 2 个测试（见矩阵 #6-7） |
| `tests/unit/test_parser.py` | 修改 | 新增 1 个测试（见矩阵 #8） |

## 不涉及

- Compiler——承诺不变
- DeduplicateStep / AggregateStep——Phase 2
- LLM 推断唯一性——Phase 2+
- FakeRelationshipPlanner 已覆盖的 V1 测试——保留不动
