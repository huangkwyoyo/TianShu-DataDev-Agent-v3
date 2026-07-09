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
4. **统一归一化**：所有列名比较统一经过 `FieldNormalizer.normalize()`。Builder 侧归一化后写入 `unique_keys` 和 `key_column_names`；Validator 侧接收已归一化的 `right_join_key_normalized`，不再自行 lower()
5. **公共合并函数**：unique_keys 的合并/去重逻辑集中在 `SourceManifestBuilder` 实例方法中，复用 `self._normalizer`
6. **不破坏事实源顺序**：unique_keys 组内列名顺序保留原始声明顺序（第一组首次出现的顺序），仅归一化大小写。去重用 `tuple(normalized_names)` 做 set key，返回值还原为原始顺序的归一化列表

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
    key_column_names_normalized: list[str] = Field(default_factory=list)
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

### 公共合并函数（新增，`SourceManifestBuilder` 实例方法）

合并逻辑放在 `SourceManifestBuilder` 上以复用 `self._normalizer`，避免外部调用 `.lower()`。

```python
def _normalize_unique_keys_list(
    self, keys: list[list[str]] | None
) -> list[list[str]]:
    """将 unique_keys 列表归一化：列名经 FieldNormalizer 处理、组间去重。

    原则：
    - 每个键组内保留原始声明顺序，仅归一化每个列名的大小写/分隔符
    - 组间去重用 tuple(normalized_names) 做 set key
    - 不改变组内列的顺序——unique_keys 是事实源

    Args:
        keys: 原始 unique_keys 列表，None 等价于 []

    Returns:
        归一化后的唯一键组列表，组间无重复，组内顺序保留首次声明。
    """
    if not keys:
        return []
    seen: set[tuple[str, ...]] = set()
    result: list[list[str]] = []
    for kg in keys:
        if not kg:
            continue
        normalized = [self._normalizer.normalize(k) for k in kg]
        key_tuple = tuple(normalized)
        if key_tuple not in seen:
            seen.add(key_tuple)
            result.append(normalized)
    return result


def _merge_unique_keys_from_sources(
    self, *sources: list[list[str]] | None,
) -> list[list[str]]:
    """合并多个来源的 unique_keys，经归一化去重后返回。

    各来源等同对待，统一 set 去重。来源优先级由调用方控制传参顺序。
    """
    all_keys: list[list[str]] = []
    for src in sources:
        if src:
            all_keys.extend(src)
    return self._normalize_unique_keys_list(all_keys) if all_keys else []
```

### `_build_manifest_tables` 变更

```python
def _build_manifest_tables(self, spec: ParsedDeveloperSpec) -> list[ManifestTable]:
    tables: list[ManifestTable] = []
    for input_t in spec.input_tables:
        # ... 现有 columns 构建逻辑不变 ...

        # 提取主键
        primary_key = [c.column_name for c in input_t.key_columns if c.unique]

        # 提取 key_column_names_normalized
        key_column_names_normalized = [
            self._normalizer.normalize(c.column_name)
            for c in input_t.key_columns
        ]

        # 合并 unique_keys ——三来源合并
        pk_as_list = [primary_key] if primary_key else None
        unique_keys = self._merge_unique_keys_from_sources(
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
            role=input_t.role,                                  # ── 新增
            key_column_names_normalized=key_column_names_normalized,  # ── 新增
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
reg_merged = self._merge_unique_keys_from_sources(reg_unique_keys, reg_pk_as_list)
if reg_merged:
    existing = table.unique_keys or []
    all_merged = self._merge_unique_keys_from_sources(existing, reg_merged)
    if all_merged:
        object.__setattr__(table, "unique_keys", all_merged)
```

### `build_manifest_from_spec` 同步变更

与 `_build_manifest_tables` 一致：透传 role/key_column_names_normalized，调用 `_merge_unique_keys_from_sources`。该函数是模块级函数，需要创建局部 `SourceManifestBuilder` 实例（或提取为独立函数接收 normalizer 参数）以复用归一化逻辑。

## 第三节：lookup 类型重命名 + 富结构

### `planning/relationship_validator.py` — JoinSafetyTableInfo 数据类

```python
from dataclasses import dataclass, field

@dataclass
class JoinSafetyTableInfo:
    """LEFT JOIN 安全门禁所需的表唯一性元数据。

    由 _build_join_safety_info 从 SourceManifest 构建，
    传给 check_left_join_safety 做唯一性判断。
    """
    unique_keys: list[list[str]] = field(default_factory=list)
    """已声明的唯一键组（经 _normalize_unique_keys_list 处理）。
    [] 表示"已查询但无声明"，区别于 None 表示"未查询"。"""
    role: str | None = None
    """表角色——"fact" | "dim" | None。"""
    key_column_names_normalized: list[str] = field(default_factory=list)
    """key_columns 列名（已归一化），用于判断 join key 是否属于维度键。"""
```

### `planning/relationship_planner.py` — `_build_join_safety_info` 重命名 + 签名变更

```python
@staticmethod
def _build_join_safety_info(
    manifest: SourceManifest | None,
) -> dict[str, JoinSafetyTableInfo]:
    """从 SourceManifest 构建 {table_ref: JoinSafetyTableInfo} 查询表。"""
    if manifest is None:
        return {}
    lookup: dict[str, JoinSafetyTableInfo] = {}
    for table in manifest.tables:
        lookup[table.table_ref] = JoinSafetyTableInfo(
            unique_keys=table.unique_keys or [],
            role=table.role,
            key_column_names_normalized=table.key_column_names_normalized,
        )
    return lookup
```

### `FakeRelationshipPlanner` 调用链变更

```python
# plan() 中
join_safety_info = self._build_join_safety_info(manifest)
# ...
open_q = self._rate_and_decide(candidate, join_safety_info)

# _rate_and_decide() 签名
def _rate_and_decide(
    self,
    candidate: JoinCandidate,
    join_safety_info: dict[str, JoinSafetyTableInfo] | None = None,
) -> OpenQuestion | None:

# _check_left_join_safety_gate() 签名
def _check_left_join_safety_gate(
    self,
    candidate: JoinCandidate,
    join_safety_info: dict[str, JoinSafetyTableInfo] | None,
) -> OpenQuestion | None:
    if candidate.join_type != JoinType.LEFT:
        return None
    info = join_safety_info.get(candidate.right_table) if join_safety_info else None
    # 归一化 join key——与 unique_keys / key_column_names_normalized 的预归一化一致
    right_key_normalized = self._normalizer.normalize(candidate.right_key)
    is_safe, desc = self._validator.check_left_join_safety(
        right_table_unique_keys=info.unique_keys if info else None,
        right_join_key_normalized=right_key_normalized,
        right_join_safety_info=info,
    )
    # ...
```

### `RelationshipPlanner` 调用链同步

`_llm_plan()` 和 `_rate_and_decide_llm()` 中：
- `self._fake._build_join_safety_info(manifest)` 返回类型为 `dict[str, JoinSafetyTableInfo]`
- 传递参数名从 `table_unique_keys` 改为 `join_safety_info`

## 第四节：Validator 增强——统一 normalizer + 文案分层

### `check_left_join_safety` 完整签名

```python
def check_left_join_safety(
    self,
    right_table_unique_keys: list[list[str]] | None,
    right_join_key_normalized: str,
    right_join_safety_info: JoinSafetyTableInfo | None = None,
) -> tuple[bool, str | None]:
    """检查 LEFT JOIN 右表联结键是否有唯一性保证。

    Args:
        right_table_unique_keys: 右表的 unique_keys 列表（已归一化）。None 表示未查询。
        right_join_key_normalized: 右表联结键——已由调用方经 FieldNormalizer 归一化传入。
        right_join_safety_info: 右表的完整安全信息（含 role + key_column_names_normalized），
                                用于生成更精准的阻断提示。None 时退化为 V1 行为。

    Returns:
        (is_safe, description)。safe 时 description 为 None。
    """
```

### 归一化策略

`unique_keys` 和 `key_column_names_normalized` 在 Builder 阶段经 `_normalize_unique_keys_list` / `FieldNormalizer.normalize()` 处理。`right_join_key_normalized` 由 Planner 调用方在传入前用 `FieldNormalizer.normalize()` 归一化。Validator 内部不做任何 `.lower()` ——接收即假定已归一化。

```python
def check_left_join_safety(self, ...):
    # 无任何唯一性声明 → unsafe
    if not right_table_unique_keys:
        return self._build_unsafe_result_no_unique_keys(
            right_join_key_normalized, right_join_safety_info
        )

    # 检查是否有唯一键组覆盖 join key
    for key_group in right_table_unique_keys:
        if right_join_key_normalized in key_group and len(key_group) == 1:
            return (True, None)

    # 有声明但不覆盖 → unsafe
    return self._build_unsafe_result_no_coverage(
        right_join_key_normalized, right_table_unique_keys
    )
```

### 三种阻断文案层级

```python
def _build_unsafe_result_no_unique_keys(
    self,
    right_join_key_normalized: str,
    safety_info: JoinSafetyTableInfo | None,
) -> tuple[bool, str]:
    """构建"无唯一性声明"的阻断结果，dim 表生成增强文案。"""
    # 场景 2：dim 表 + join_key 属于 key_column_names_normalized → 增强文案
    if (
        safety_info is not None
        and safety_info.role == "dim"
        and right_join_key_normalized in safety_info.key_column_names_normalized
    ):
        return (
            False,
            f"dim 表的 key_column '{right_join_key_normalized}' 未声明唯一性。"
            f"请在 DeveloperSpec 中为该列添加 'unique: true'，"
            f"或在 source_tables 对应条目中声明 "
            f"unique_keys: [['{right_join_key_normalized}']]。"
            f"若该键确实有重复值，请提供去重策略说明。",
        )
    # 场景 1：通用阻断
    return (
        False,
        f"LEFT JOIN 右表 '{right_join_key_normalized}' 无唯一性保证："
        f"右表未声明 primary_key 且 ManifestTable.unique_keys 为空。"
        f"若该键有重复值，将导致静默笛卡尔积、左表度量值膨胀。"
        f"请在 DeveloperSpec 中为右表声明 unique_keys，或提供去重策略说明。",
    )
```

注意：`unique_keys`、`key_column_names_normalized` 和 `right_join_key_normalized` 三者在进入 `check_left_join_safety` 前均已归一化。归一化边界在 Builder（写入侧）和 Planner 调用方——Validator 不做二次归一化。

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
                        # 使用 FieldNormalizer 归一化比较——处理大小写/分隔符差异
                        all_col_names_normalized = {
                            self._normalizer.normalize(c.column_name)
                            for c in key_columns + biz_columns
                        }
                        unknown = [
                            k for k in kg
                            if self._normalizer.normalize(k) not in all_col_names_normalized
                        ]
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

共 10 个测试（V1 的 17 个保留，新增 10 个聚焦 V2 变更）：

| # | 文件 | 测试 | 场景 | 预期 |
|---|------|------|------|------|
| 1 | `test_left_join_safety.py` | `test_dim_key_without_unique_blocks` | role=dim, key_column_names_normalized=["location_id"], join_key_normalized="location_id", unique_keys=[] | blocking=True, 文案含 "unique: true" 和 "unique_keys: [['location_id']]" |
| 2 | `test_left_join_safety.py` | `test_dim_key_with_unique_keys_passes` | role=dim, unique_keys=[["location_id"]] | 通过 |
| 3 | `test_left_join_safety.py` | `test_fact_no_unique_blocks` | role=fact, unique_keys=[] | blocking=True, 文案不含 "key_column" 措辞 |
| 4 | `test_left_join_safety.py` | `test_field_normalizer_handles_special_chars` | key_column 声明为 "Location ID"（含空格），经 FieldNormalizer 归一化为 "location_id"，join_key_normalized="location_id" | 匹配成功。证明 .lower() 不够——必须走 FieldNormalizer |
| 5 | `test_left_join_safety.py` | `test_join_safety_info_default_factory` | JoinSafetyTableInfo() 默认构造，unique_keys=[]，role=None，key_column_names_normalized=[] | 不抛异常，安全门禁正常阻断 |
| 6 | `test_source_manifest.py` | `test_merge_unique_keys_from_registry_pk` | Registry 返回 primary_key=["loc_id"] | manifest.unique_keys 含 ["loc_id"] |
| 7 | `test_source_manifest.py` | `test_builder_transmits_role_and_key_columns` | InputTableDecl(role="dim", key_columns=[ColumnDecl("Location_ID")]) | ManifestTable.role="dim", key_column_names_normalized=["location_id"] |
| 8 | `test_source_manifest.py` | `test_unique_keys_preserves_original_order` | YAML unique_keys: [["zone_name", "borough"]] | ManifestTable.unique_keys=[["zone_name", "borough"]]——保留原始顺序，不排序 |
| 9 | `test_parser.py` | `test_parser_rejects_invalid_unique_keys` | YAML unique_keys: "not_a_list" | ParseWarning W005 |
| 10 | `test_parser.py` | `test_parser_accepts_valid_unique_keys` | YAML unique_keys: [[location_id], [zone_name, borough]] | InputTableDecl.unique_keys = [["location_id"], ["zone_name", "borough"]] |

## 变更文件清单

| 文件 | 变更类型 | 内容 |
|------|----------|------|
| `developer_spec/models.py` | 修改 | `ManifestTable` 加 `role` + `key_column_names_normalized`（`Field(default_factory=list)`）；`InputTableDecl` 加 `unique_keys` |
| `developer_spec/source_manifest.py` | 修改 | 新增 `_normalize_unique_keys_list` + `_merge_unique_keys_from_sources`（均为 `SourceManifestBuilder` 实例方法）；`_build_manifest_tables` 透传 role/key_column_names_normalized + 合并 unique_keys；`_supplement_from_registry` 补 registry PK 链路；`build_manifest_from_spec` 同步 |
| `developer_spec/parser.py` | 修改 | `_parse_input_tables` 解析 `unique_keys` + FieldNormalizer 校验 W005/W006 |
| `planning/relationship_validator.py` | 修改 | 新增 `JoinSafetyTableInfo` dataclass（`default_factory=list`）；`check_left_join_safety` 参数改为 `right_join_key_normalized` + `right_join_safety_info`；新增 `_build_unsafe_result_no_unique_keys` 分层文案 |
| `planning/relationship_planner.py` | 修改 | `_build_unique_keys_lookup` 重命名为 `_build_join_safety_info`，返回 `dict[str, JoinSafetyTableInfo]`；`_check_left_join_safety_gate` 用 `FieldNormalizer` 归一化 join key 后传入 Validator；参数名从 `table_unique_keys` 统一为 `join_safety_info` |
| `tests/planning/test_left_join_safety.py` | 修改 | 新增 5 个 V2 测试（见矩阵 #1-5） |
| `tests/unit/test_source_manifest.py` 或新建 | 修改/新建 | 新增 3 个测试（见矩阵 #6-8） |
| `tests/unit/test_parser.py` | 修改 | 新增 2 个测试（见矩阵 #9-10） |

## 不涉及

- Compiler——承诺不变
- DeduplicateStep / AggregateStep——Phase 2
- LLM 推断唯一性——Phase 2+
- FakeRelationshipPlanner 已覆盖的 V1 测试——保留不动
- 真实 SchemaRegistry 连接——测试 #6 使用 mock registry 返回 primary_key，不要求真实外部连接
- FakeRelationshipPlanner 已覆盖的 V1 测试——保留不动
