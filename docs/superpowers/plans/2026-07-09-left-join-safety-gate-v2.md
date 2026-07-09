# LEFT JOIN 安全门禁 V2——dim 表提示增强实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 LEFT JOIN 安全门禁 V2：dim 表增强阻断文案 + unique_keys 语法支持 + Registry 主键链路 + JoinSafetyTableInfo 重命名 + FieldNormalizer 统一归一化

**Architecture:** 归一化边界收口到 Builder（写入侧）和 Planner（传参前），Validator 接收预归一化值。`_normalize_unique_keys_list` / `_merge_unique_keys_from_sources` 为模块级纯函数，显式接收 normalizer 参数，SourceManifestBuilder 和 build_manifest_from_spec 共用。

**Tech Stack:** Python 3.12+, Pydantic (StrictModel), pytest, FieldNormalizer

## Global Constraints

- 所有注释和文档字符串使用中文
- unique_keys 组内列名保留原始声明顺序（不排序），仅归一化大小写/分隔符
- Parser W006 后跳过该 key group（不保留为唯一性证据，防止误放行）
- Pydantic 可变默认值必须使用 `Field(default_factory=list)`
- 合并函数 `_normalize_unique_keys_list` / `_merge_unique_keys_from_sources` 为模块级纯函数，显式接收 `normalizer: FieldNormalizer`
- Pydantic `Field(default_factory=list)` 用于所有 list 类型默认值
- 17 个 V1 测试保留不动，10 个新测试追加到对应文件末尾（同名测试类）

---

### Task 1: ManifestTable + InputTableDecl 模型扩展

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/models.py:689-704` (ManifestTable)
- Modify: `src/tianshu_datadev/developer_spec/models.py:411-425` (InputTableDecl)

**Interfaces:**
- Produces: `ManifestTable.role: str | None`, `ManifestTable.key_column_names_normalized: list[str]`, `InputTableDecl.unique_keys: list[list[str]] | None`

- [ ] **Step 1: ManifestTable 添加 role + key_column_names_normalized**

在 `ManifestTable` 类中 `estimated_row_count` 字段之前添加两个新字段：

```python
class ManifestTable(StrictModel):
    """SourceManifest 中的表条目。"""

    table_ref: str
    source_table: SafePhysicalTableName
    columns: list[ManifestColumn] = []
    primary_key: list[str] | None = None
    foreign_keys: list[ForeignKeyRef] | None = None
    unique_keys: list[list[str]] | None = None
    """已知唯一键集合——每个元素是一组列名的列表，表示这组列在表中值唯一。

    primary_key 自动视为唯一键之一，构建时由 SourceManifestBuilder 同步写入。
    SchemaRegistry 可补充额外的唯一索引信息。
    用于 LEFT JOIN 右表唯一性安全门禁——无覆盖 join key 的唯一键时阻断。
    """
    # ── V2 新增 ──
    role: str | None = None
    """表角色——"fact" | "dim" | None。从 InputTableDecl.role 透传。
    用于 LEFT JOIN 安全门禁生成更精准的阻断提示。"""
    key_column_names_normalized: list[str] = Field(default_factory=list)
    """key_columns 的归一化列名集合。由 SourceManifestBuilder 从
    InputTableDecl.key_columns 提取，经 FieldNormalizer.normalize() 处理。
    用于 LEFT JOIN 安全门禁判断 join key 是否属于维度键。"""
    estimated_row_count: int | None = None
```

注意：需要确认 `Field` 已从 `pydantic` import。检查文件头部是否有 `from pydantic import Field`。

- [ ] **Step 2: InputTableDecl 添加 unique_keys**

在 `InputTableDecl` 类中 `business_columns` 字段之后添加：

```python
class InputTableDecl(StrictModel):
    """源表声明——包含别名、物理表名、列、过滤、角色等全部声明信息。"""

    table_alias: str
    source_table: SafePhysicalTableName
    row_count: int | None = None
    raw_row_count: str | None = None
    role: str | None = None  # "fact" | "dim" | None
    description: str | None = None
    columns: list[ColumnDecl] = []
    filters: list[FilterDecl] = []
    partition_field: str | None = None
    time_field: str | None = None
    key_columns: list[ColumnDecl] = []
    business_columns: list[ColumnDecl] = []
    # ── V2 新增 ──
    unique_keys: list[list[str]] | None = None
    """开发者显式声明的唯一键集合。每个元素是一组列名，表示这组列在表中值唯一。
    示例：unique_keys: [["location_id"], ["zone_name", "borough"]]
    由 _parse_input_tables 从 YAML source_tables[*].unique_keys 解析。"""
```

- [ ] **Step 3: 运行现有测试确认模型兼容**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/test_source_manifest.py tests/test_parser.py tests/planning/test_left_join_safety.py -v --tb=short -x
```

预期：所有已有测试通过（新增字段为可选且带默认值，不破坏现有构造）。

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/developer_spec/models.py
git commit -m "feat(models): ManifestTable 添加 role/key_column_names_normalized，InputTableDecl 添加 unique_keys"
```

---

### Task 2: 模块级合并函数——_normalize_unique_keys_list + _merge_unique_keys_from_sources

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/source_manifest.py:1-31` (imports 区域)

**Interfaces:**
- Produces: `_normalize_unique_keys_list(keys: list[list[str]] | None, normalizer: FieldNormalizer) -> list[list[str]]`
- Produces: `_merge_unique_keys_from_sources(*sources: list[list[str]] | None, normalizer: FieldNormalizer) -> list[list[str]]`

- [ ] **Step 1: 在 source_manifest.py 中添加两个模块级函数**

在文件末尾（`build_manifest_from_spec` 函数之后）添加。这些是纯函数，放在 `SourceManifestBuilder` 类之外。

```python
# ════════════════════════════════════════════
# 模块级 unique_keys 合并工具函数（V2 新增）
# ════════════════════════════════════════════


def _normalize_unique_keys_list(
    keys: list[list[str]] | None,
    normalizer: FieldNormalizer,
) -> list[list[str]]:
    """将 unique_keys 列表归一化：列名经 FieldNormalizer 处理、组间去重。

    原则：
    - 每个键组内保留原始声明顺序，仅归一化每个列名的大小写/分隔符
    - 组间去重用 tuple(normalized_names) 做 set key
    - 不改变组内列的顺序——unique_keys 是事实源
    - 模块级纯函数，SourceManifestBuilder 和 build_manifest_from_spec 共用

    Args:
        keys: 原始 unique_keys 列表，None 等价于 []
        normalizer: FieldNormalizer 实例，由调用方传入

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
        normalized = [normalizer.normalize(k) for k in kg]
        key_tuple = tuple(normalized)
        if key_tuple not in seen:
            seen.add(key_tuple)
            result.append(normalized)
    return result


def _merge_unique_keys_from_sources(
    *sources: list[list[str]] | None,
    normalizer: FieldNormalizer,
) -> list[list[str]]:
    """合并多个来源的 unique_keys，经归一化去重后返回。

    各来源等同对待，统一 set 去重。来源优先级由调用方控制传参顺序。
    模块级纯函数，SourceManifestBuilder 和 build_manifest_from_spec 共用。

    Args:
        *sources: 多个 unique_keys 来源，None 等价于 []
        normalizer: FieldNormalizer 实例

    Returns:
        去重后的归一化唯一键组列表。
    """
    all_keys: list[list[str]] = []
    for src in sources:
        if src:
            all_keys.extend(src)
    return _normalize_unique_keys_list(all_keys, normalizer) if all_keys else []
```

- [ ] **Step 2: 运行测试确认模块导入正常**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -c "from tianshu_datadev.developer_spec.source_manifest import _normalize_unique_keys_list, _merge_unique_keys_from_sources; print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add src/tianshu_datadev/developer_spec/source_manifest.py
git commit -m "feat(source_manifest): 新增模块级 _normalize_unique_keys_list + _merge_unique_keys_from_sources 纯函数"
```

---

### Task 3: SourceManifestBuilder + build_manifest_from_spec 更新

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/source_manifest.py:151-222` (_build_manifest_tables)
- Modify: `src/tianshu_datadev/developer_spec/source_manifest.py:224-285` (_supplement_from_registry)
- Modify: `src/tianshu_datadev/developer_spec/source_manifest.py:472-551` (build_manifest_from_spec)

**Interfaces:**
- Consumes: `_normalize_unique_keys_list`, `_merge_unique_keys_from_sources` (from Task 2)
- Consumes: `ManifestTable.role`, `ManifestTable.key_column_names_normalized`, `InputTableDecl.unique_keys` (from Task 1)

- [ ] **Step 1: 更新 _build_manifest_tables——透传 role/key_column_names_normalized + 合并 unique_keys**

替换 `_build_manifest_tables` 方法中构建 ManifestTable 的部分（约第 204-222 行）：

```python
            # 提取主键（来自 key_columns 中标记 unique=True 的字段）
            primary_key = [c.column_name for c in input_t.key_columns if c.unique]

            # 提取 key_column_names_normalized——所有 key_columns 列名的归一化形式
            key_column_names_normalized = [
                self._normalizer.normalize(c.column_name)
                for c in input_t.key_columns
            ]

            # 合并 unique_keys ——调用模块级纯函数
            pk_as_list = [primary_key] if primary_key else None
            unique_keys = _merge_unique_keys_from_sources(
                pk_as_list,              # 来源 1：primary_key
                input_t.unique_keys,     # 来源 2：YAML 显式声明
                normalizer=self._normalizer,
            )

            tables.append(ManifestTable(
                table_ref=input_t.table_alias,
                source_table=input_t.source_table,
                columns=columns,
                primary_key=primary_key if primary_key else None,
                foreign_keys=None,
                unique_keys=unique_keys if unique_keys else None,
                estimated_row_count=input_t.row_count,
                role=input_t.role,                                  # ── V2 新增
                key_column_names_normalized=key_column_names_normalized,  # ── V2 新增
            ))
```

- [ ] **Step 2: 更新 _supplement_from_registry——补 Registry PK 链路 + 使用合并函数**

替换 `_supplement_from_registry` 中 unique_keys 合并部分（约第 276-285 行）：

```python
            # 从 SchemaRegistry 补充 unique_keys（合并而非覆盖）—— V2：用模块级纯函数
            reg_unique_keys = registry_meta.get("unique_keys")
            reg_pk = registry_meta.get("primary_key")
            reg_pk_as_list = [reg_pk] if reg_pk and isinstance(reg_pk, list) and len(reg_pk) > 0 else None

            # 合并 registry 侧的 unique_keys + primary_key
            reg_merged = _merge_unique_keys_from_sources(
                reg_unique_keys, reg_pk_as_list, normalizer=self._normalizer,
            )
            if reg_merged:
                existing = table.unique_keys or []
                all_merged = _merge_unique_keys_from_sources(
                    existing, reg_merged, normalizer=self._normalizer,
                )
                if all_merged:
                    object.__setattr__(table, "unique_keys", all_merged)
```

注意：需要删除旧的 `tuple(sorted(g))` 逻辑（第 280-283 行），替换为上述合并函数调用。

- [ ] **Step 3: 更新 build_manifest_from_spec——透传新字段 + 合并 unique_keys**

替换 `build_manifest_from_spec` 函数中构建 ManifestTable 的部分（约第 529-546 行）：

```python
        # 提取主键（来自 key_columns 中标记 unique=True 的字段）
        primary_key = [c.column_name for c in t.key_columns if c.unique]

        # 提取 key_column_names_normalized
        key_column_names_normalized = [
            FieldNormalizer().normalize(c.column_name)
            for c in t.key_columns
        ]

        # 合并 unique_keys ——调用模块级纯函数
        pk_as_list = [primary_key] if primary_key else None
        unique_keys = _merge_unique_keys_from_sources(
            pk_as_list,
            t.unique_keys,
            normalizer=FieldNormalizer(),
        )

        tables.append(
            ManifestTable(
                table_ref=t.table_alias,
                source_table=t.source_table,
                columns=cols,
                primary_key=primary_key if primary_key else None,
                unique_keys=unique_keys if unique_keys else None,
                estimated_row_count=t.row_count,
                role=t.role,                                      # ── V2 新增
                key_column_names_normalized=key_column_names_normalized,  # ── V2 新增
            )
        )
```

- [ ] **Step 4: 运行现有测试确认不退化**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/test_source_manifest.py tests/planning/test_left_join_safety.py -v --tb=short
```

预期：所有已有测试通过。

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/developer_spec/source_manifest.py
git commit -m "feat(source_manifest): _build_manifest_tables 透传 role/key_column_names_normalized + 合并 unique_keys；_supplement_from_registry 补 Registry PK 链路；build_manifest_from_spec 同步"
```

---

### Task 4: Parser——unique_keys 解析 + W005/W006 校验

**Files:**
- Modify: `src/tianshu_datadev/developer_spec/parser.py:360-417` (_parse_input_tables)

**Interfaces:**
- Consumes: `InputTableDecl.unique_keys` (from Task 1)
- Consumes: `self._normalizer` (parser instance field)

- [ ] **Step 1: 修改 _parse_input_tables 签名——添加 parse_warnings 参数**

`parse_warnings` 在 `parse()` 中是局部变量（第 238 行），需要作为参数传入 `_parse_input_tables`。

修改方法签名（第 360 行）：

```python
    def _parse_input_tables(
        self, raw_tables: list[dict], parse_warnings: list[ParseWarning] | None = None
    ) -> list[InputTableDecl]:
```

在 `parse()` 调用处（第 220 行）更新：

```python
        input_tables = self._parse_input_tables(
            spec_dict.get("source_tables", []), parse_warnings=parse_warnings
        )
```

注意：调用时 `parse_warnings` 在第 238 行才初始化，所以需要把 `_parse_input_tables` 调用移到 `parse_warnings` 初始化之后，或者先初始化空列表。查看当前代码顺序：

- 第 220 行: `input_tables = self._parse_input_tables(...)`——在 parse_warnings 初始化（第 238 行）之前
- 第 238 行: `parse_warnings: list[ParseWarning] = []`

因此需要将 `_parse_input_tables` 调用移到第 238 行之后。但 input_tables 被后续 `_parse_metrics`、`_parse_joins` 等使用（第 221-224 行），所以需要调整顺序：先初始化 `parse_warnings = []`，再调用各子解析器。

调整后的顺序（替换第 217-241 行）：

```python
        # 5. 初始化 warnings 容器（子解析器需要追加警告）
        parse_warnings: list[ParseWarning] = []

        # 6. 解析各子部分
        input_tables = self._parse_input_tables(
            spec_dict.get("source_tables", []), parse_warnings=parse_warnings
        )
        metrics = self._parse_metrics(spec_dict.get("metrics", []), input_tables)
        dimensions = self._parse_dimensions(spec_dict.get("dimensions", []))
        compute_steps = self._parse_compute_steps(spec_dict.get("compute_steps"), input_tables)
        joins = self._parse_joins(spec_dict.get("joins"), input_tables, compute_steps)
        time_range = self._parse_time_range(spec_dict.get("time_range"))
        output_spec = self._parse_output_spec(spec_dict)

        # 7. 提取标题
        title = self._extract_title(md_body) or spec_dict.get("summary", "Untitled")

        # 8. 组装描述
        summary = spec_dict.get("summary", "")
        description_parts = [p for p in [summary, md_body] if p]
        description = "\n\n".join(description_parts)

        # 9. 执行允许/禁止检查
        open_questions: list[OpenQuestion] = []
        self._validate_seven_rejections(spec_dict, input_tables, metrics, joins, output_spec)
        parse_warnings.extend(self._validate_six_allowances(spec_dict, input_tables, joins, time_range))
```

- [ ] **Step 2: 在 _parse_input_tables 中添加 unique_keys 解析逻辑**

在 `_parse_input_tables` 方法中，在构建 `InputTableDecl(...)` 之前（约第 398-401 行之间），插入 unique_keys 解析：

```python
            # ── V2 新增：解析 unique_keys ──
            raw_unique_keys = raw.get("unique_keys")
            unique_keys = None
            if raw_unique_keys is not None:
                if not isinstance(raw_unique_keys, list):
                    if parse_warnings is not None:
                        parse_warnings.append(ParseWarning(
                            code="W005",
                            message=f"表 '{alias}' 的 unique_keys 必须是列表，"
                                    f"收到 {type(raw_unique_keys).__name__}——已忽略",
                            field_ref=f"{alias}.unique_keys",
                        ))
                else:
                    validated = []
                    for i, kg in enumerate(raw_unique_keys):
                        if not isinstance(kg, list) or len(kg) == 0:
                            if parse_warnings is not None:
                                parse_warnings.append(ParseWarning(
                                    code="W005",
                                    message=f"表 '{alias}' 的 unique_keys[{i}] 必须是非空列表——已跳过",
                                    field_ref=f"{alias}.unique_keys[{i}]",
                                ))
                            continue
                        # 校验列名存在于 key_columns/business_columns 中
                        # 直接用原始 dict 提取列名，避免重复 _parse_columns 调用
                        all_col_names = {
                            c.get("name", "") for c in key_cols + biz_cols
                            if isinstance(c, dict)
                        }
                        all_col_names_normalized = {
                            self._normalizer.normalize(n) for n in all_col_names
                        }
                        unknown = [
                            k for k in kg
                            if self._normalizer.normalize(k) not in all_col_names_normalized
                        ]
                        if unknown:
                            if parse_warnings is not None:
                                parse_warnings.append(ParseWarning(
                                    code="W006",
                                    message=f"表 '{alias}' 的 unique_keys[{i}] 引用了未知列："
                                            f"{unknown}——已跳过，不保留为唯一性证据",
                                    field_ref=f"{alias}.unique_keys[{i}]",
                                ))
                            continue  # 跳过该组，不保留——防止误放行
                        validated.append(kg)
                    if validated:
                        unique_keys = validated
```

- [ ] **Step 2: 更新 InputTableDecl 构造调用——传入 unique_keys**

在 `_parse_input_tables` 末尾的 `InputTableDecl(...)` 构造中添加 `unique_keys=unique_keys`：

```python
            tables.append(InputTableDecl(
                table_alias=alias,
                source_table=raw.get("name", ""),
                row_count=row_count,
                raw_row_count=raw_row_count,
                role=raw.get("role"),
                description=raw.get("description"),
                columns=columns,
                filters=filters,
                partition_field=raw.get("partition_field"),
                time_field=raw.get("time_field"),
                key_columns=self._parse_columns(key_cols, f"table {alias} key_columns"),
                business_columns=self._parse_columns(biz_cols, f"table {alias} business_columns"),
                unique_keys=unique_keys,  # ── V2 新增
            ))
```

- [ ] **Step 3: 运行现有解析器测试确认兼容**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/test_parser.py -v --tb=short
```

预期：所有已有测试通过。

- [ ] **Step 4: Commit**

```bash
git add src/tianshu_datadev/developer_spec/parser.py
git commit -m "feat(parser): _parse_input_tables 解析 unique_keys + W005/W006 校验，W006 后跳过该组"
```

---

### Task 5: JoinSafetyTableInfo dataclass + Validator 签名变更 + 分层文案

**Files:**
- Modify: `src/tianshu_datadev/planning/relationship_validator.py:1-10` (imports)
- Modify: `src/tianshu_datadev/planning/relationship_validator.py:114-167` (check_left_join_safety + 新辅助方法)

**Interfaces:**
- Produces: `JoinSafetyTableInfo` dataclass
- Produces: `check_left_join_safety(right_table_unique_keys, right_join_key_normalized, right_join_safety_info=None)` — 新签名
- Produces: `_build_unsafe_result_no_unique_keys(right_join_key_normalized, safety_info)` — 分层文案辅助方法

- [ ] **Step 1: 在文件头部添加 dataclasses import + JoinSafetyTableInfo 定义**

在 `relationship_validator.py` 文件顶部添加 import，并在 `RelationshipValidator` 类之前定义数据类：

```python
"""RelationshipValidator——确定性证据评级器。

LLM/Fake 只能提候选——等级由 Validator 确定性计算。
四级级联规则：STRONG → MEDIUM → WEAK → NONE，匹配即终止。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .relationship_hypothesis import EvidenceAction, JoinEvidenceLevel


@dataclass
class JoinSafetyTableInfo:
    """LEFT JOIN 安全门禁所需的表唯一性元数据。

    由 _build_join_safety_info 从 SourceManifest 构建，
    传给 check_left_join_safety 做唯一性判断。
    """
    unique_keys: list[list[str]] = field(default_factory=list)
    """已声明的唯一键组（已归一化）。[] 表示"已查询但无声明"，区别于 None 表示"未查询"。"""
    role: str | None = None
    """表角色——"fact" | "dim" | None。"""
    key_column_names_normalized: list[str] = field(default_factory=list)
    """key_columns 列名（已归一化），用于判断 join key 是否属于维度键。"""


class RelationshipValidator:
    # ... 现有 rate / generate_detail 方法不变 ...
```

- [ ] **Step 2: 替换 check_left_join_safety——新签名 + 预归一化语义**

完全替换现有的 `check_left_join_safety` 方法（第 116-167 行）：

```python
    # ── LEFT JOIN 唯一性安全门禁（V2：预归一化 + JoinSafetyTableInfo + 分层文案）──

    def check_left_join_safety(
        self,
        right_table_unique_keys: list[list[str]] | None,
        right_join_key_normalized: str,
        right_join_safety_info: JoinSafetyTableInfo | None = None,
    ) -> tuple[bool, str | None]:
        """检查 LEFT JOIN 右表联结键是否有唯一性保证。

        右表联结键不唯一时，LEFT JOIN 会产生静默笛卡尔积——
        左表行被复制、度量值膨胀。此方法是安全门禁：
        无唯一性证据时返回 unsafe，由调用方生成 blocking OpenQuestion。

        Phase 1 仅支持单列联结键。复合键去重延后到 Phase 2。

        V2 变更：
        - right_join_key_normalized 已由调用方经 FieldNormalizer 归一化传入
        - unique_keys 已由 Builder 预归一化
        - Validator 内部不做任何 .lower() ——接收即假定已归一化
        - right_join_safety_info 提供 role/key_column_names 用于增强阻断文案

        Args:
            right_table_unique_keys: 右表的 unique_keys 列表（已归一化）。None 表示未查询。
            right_join_key_normalized: 右表联结键——已由调用方经 FieldNormalizer 归一化传入。
            right_join_safety_info: 右表的完整安全信息（含 role + key_column_names_normalized），
                                    用于生成更精准的阻断提示。None 时退化为 V1 行为。

        Returns:
            (is_safe, description) 二元组。
            is_safe=True 时 description 为 None。
            is_safe=False 时 description 为阻断理由。
        """
        # 无任何唯一性声明 → unsafe
        if not right_table_unique_keys:
            return self._build_unsafe_result_no_unique_keys(
                right_join_key_normalized, right_join_safety_info
            )

        # 检查是否有唯一键组覆盖 join key（Phase 1 仅支持单列键）
        for key_group in right_table_unique_keys:
            if right_join_key_normalized in key_group and len(key_group) == 1:
                return (True, None)

        # 有唯一键声明但不覆盖当前联结键 → unsafe
        declared = "; ".join(", ".join(g) for g in right_table_unique_keys)
        return (
            False,
            f"LEFT JOIN 右表联结键 '{right_join_key_normalized}' 不被任何唯一键覆盖。"
            f"右表已声明唯一键：[{declared}]，"
            f"均不包含 '{right_join_key_normalized}'。"
            f"若该键有重复值，将导致静默笛卡尔积。"
            f"请确认联结键选择正确，或为 '{right_join_key_normalized}' 声明唯一性。",
        )
```

- [ ] **Step 3: 添加 _build_unsafe_result_no_unique_keys 辅助方法**

在 `check_left_join_safety` 方法之后添加：

```python
    def _build_unsafe_result_no_unique_keys(
        self,
        right_join_key_normalized: str,
        safety_info: JoinSafetyTableInfo | None,
    ) -> tuple[bool, str]:
        """构建"无唯一性声明"的阻断结果——dim 表生成增强文案。

        策略：
        - role=dim 且 join_key ∈ key_column_names_normalized → 增强文案（提示声明 unique: true 或 unique_keys）
        - 其他 → 通用阻断文案
        """
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
        # 场景 1：通用阻断——无任何唯一性声明
        return (
            False,
            f"LEFT JOIN 右表 '{right_join_key_normalized}' 无唯一性保证："
            f"右表未声明 primary_key 且 unique_keys 为空。"
            f"若该键有重复值，将导致静默笛卡尔积、左表度量值膨胀。"
            f"请在 DeveloperSpec 中为右表声明 unique_keys，或提供去重策略说明。",
        )
```

- [ ] **Step 4: 确认 V1 测试参数名需更新（实际修改在 Task 6 执行）**

Validator 签名变更：`right_join_key` → `right_join_key_normalized`。V1 测试中所有 `check_left_join_safety(right_join_key=...)` 调用需要改为 `right_join_key_normalized=...`。此修改在 Task 6 Step 8 中统一执行，避免跨 task 冲突。

- [ ] **Step 5: Commit**

```bash
git add src/tianshu_datadev/planning/relationship_validator.py
git commit -m "feat(validator): 新增 JoinSafetyTableInfo dataclass，check_left_join_safety 改为预归一化签名 + _build_unsafe_result_no_unique_keys 分层文案"
```

---

### Task 6: relationship_planner.py——调用链全面更新

**Files:**
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:1-28` (imports)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:55-99` (plan 方法)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:124-180` (_rate_and_decide)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:246-285` (_check_left_join_safety_gate)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:321-341` (_build_unique_keys_lookup → _build_join_safety_info)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:677-758` (_llm_plan)
- Modify: `src/tianshu_datadev/planning/relationship_planner.py:808-928` (_rate_and_decide_llm)

**Interfaces:**
- Consumes: `JoinSafetyTableInfo` (from Task 5)
- Consumes: `check_left_join_safety(right_table_unique_keys, right_join_key_normalized, right_join_safety_info)` (from Task 5)

- [ ] **Step 1: 更新 imports——添加 JoinSafetyTableInfo**

在 `relationship_planner.py` 顶部 import 中添加：

```python
from .relationship_validator import JoinSafetyTableInfo, RelationshipValidator
```

（当前 import 只有 `RelationshipValidator`，需要追加 `JoinSafetyTableInfo`）

- [ ] **Step 2: 重命名 _build_unique_keys_lookup → _build_join_safety_info，返回 dict[str, JoinSafetyTableInfo]**

完全替换 FakeRelationshipPlanner 中的方法（第 321-341 行）：

```python
    @staticmethod
    def _build_join_safety_info(
        manifest: SourceManifest | None,
    ) -> dict[str, JoinSafetyTableInfo]:
        """从 SourceManifest 构建 {table_ref: JoinSafetyTableInfo} 查询表。

        供 LEFT JOIN 唯一性安全门禁使用。
        无论 unique_keys 是否有值，所有表都写入 lookup——[] 表示"已查询但无声明"。

        Args:
            manifest: 源数据清单，None 时返回空 dict

        Returns:
            {table_ref: JoinSafetyTableInfo} 映射
        """
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

- [ ] **Step 3: 更新 plan() 方法——变量名改为 join_safety_info**

在 `FakeRelationshipPlanner.plan()` 中（第 75-84 行）：

```python
        # 构建 JoinSafetyInfo 查询表（供 LEFT JOIN 安全门禁使用）
        join_safety_info = self._build_join_safety_info(manifest)

        # 从显式声明提取候选
        if spec.joins:
            for join_decl in spec.joins:
                candidate = self._build_candidate(join_decl, spec)
                open_q = self._rate_and_decide(candidate, join_safety_info)
```

- [ ] **Step 4: 更新 _rate_and_decide()——签名 + 传递 join_safety_info**

将 `_rate_and_decide` 的参数名从 `table_unique_keys` 改为 `join_safety_info`，类型改为 `dict[str, JoinSafetyTableInfo] | None`。在 STRONG 分支调用处更新：

```python
    def _rate_and_decide(
        self,
        candidate: JoinCandidate,
        join_safety_info: dict[str, JoinSafetyTableInfo] | None = None,
    ) -> OpenQuestion | None:
        # ... 中间逻辑不变 ...

        # STRONG → LEFT JOIN 唯一性安全门禁
        if level == JoinEvidenceLevel.STRONG:
            return self._check_left_join_safety_gate(candidate, join_safety_info)

        return None
```

注意：方法签名中的参数名从 `table_unique_keys` 改为 `join_safety_info`，内部引用也需同步。

- [ ] **Step 5: 更新 _check_left_join_safety_gate()——FieldNormalizer 归一化 join key + 新 Validator 签名**

完全替换 `_check_left_join_safety_gate` 方法（第 246-285 行）：

```python
    def _check_left_join_safety_gate(
        self,
        candidate: JoinCandidate,
        join_safety_info: dict[str, JoinSafetyTableInfo] | None,
    ) -> OpenQuestion | None:
        """STRONG 通过后，对 LEFT JOIN 做右表联结键唯一性检查。

        只有 LEFT JOIN 需要此门禁——INNER/RIGHT/FULL 不触发。
        无唯一性证据时返回 blocking OpenQuestion。

        V2 变更：用 FieldNormalizer 归一化 join key 后传入 Validator，
        与 unique_keys / key_column_names_normalized 的预归一化一致。

        Args:
            candidate: 已通过 STRONG 评级的 Join 候选
            join_safety_info: {table_ref: JoinSafetyTableInfo} 查询表

        Returns:
            OpenQuestion（不安全时）或 None（安全通过）。
        """
        if candidate.join_type != JoinType.LEFT:
            return None

        # 查询右表的 JoinSafetyTableInfo
        info = join_safety_info.get(candidate.right_table) if join_safety_info else None

        # 归一化 join key——与 unique_keys / key_column_names_normalized 的预归一化一致
        right_key_normalized = self._normalizer.normalize(candidate.right_key)

        is_safe, desc = self._validator.check_left_join_safety(
            right_table_unique_keys=info.unique_keys if info else None,
            right_join_key_normalized=right_key_normalized,
            right_join_safety_info=info,
        )

        if not is_safe:
            return OpenQuestion(
                question_id=f"Q-JOIN-SAFETY-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.right_table}.{candidate.right_key}",
                description=desc,
                blocking=True,
            )

        return None
```

- [ ] **Step 6: 更新 _llm_plan()——变量名同步**

在 `RelationshipPlanner._llm_plan()` 中（第 694-695 行）：

```python
        # 构建 JoinSafetyInfo 查询表（供 LEFT JOIN 安全门禁使用）
        join_safety_info = self._fake._build_join_safety_info(manifest)
```

后续引用 `table_unique_keys` 的地方全部改为 `join_safety_info`（第 702、734、926 行）。

- [ ] **Step 7: 更新 _rate_and_decide_llm()——签名 + 传递**

将 `_rate_and_decide_llm` 的参数名从 `table_unique_keys` 改为 `join_safety_info`，类型改为 `dict[str, JoinSafetyTableInfo] | None`。STRONG 分支调用处（第 925-926 行）：

```python
        # STRONG → LEFT JOIN 唯一性安全门禁
        if level == JoinEvidenceLevel.STRONG:
            return self._fake._check_left_join_safety_gate(candidate, join_safety_info)
```

- [ ] **Step 8: 更新 V1 测试中调用 check_left_join_safety 的参数名**

V1 测试中调用 `check_left_join_safety(right_join_key=...)` 需要改为 `right_join_key_normalized=...`：

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && grep -n "right_join_key=" tests/planning/test_left_join_safety.py
```

逐个将 `right_join_key=` 替换为 `right_join_key_normalized=`。

- [ ] **Step 9: 运行现有测试确认全链路不退化**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_left_join_safety.py -v --tb=short
```

预期：所有 17 个 V1 测试通过。

- [ ] **Step 10: Commit**

```bash
git add src/tianshu_datadev/planning/relationship_planner.py tests/planning/test_left_join_safety.py
git commit -m "feat(planner): _build_join_safety_info 重命名 + _check_left_join_safety_gate 用 FieldNormalizer 归一化 + 全链路 join_safety_info 传递"
```

---

### Task 7: 测试——Validator V2 新用例（5 个）

**Files:**
- Modify: `tests/planning/test_left_join_safety.py` (追加到文件末尾)

**Interfaces:**
- Consumes: `JoinSafetyTableInfo`, `check_left_join_safety` (from Task 5)

- [ ] **Step 1: 在 test_left_join_safety.py 末尾添加 V2 测试类**

在文件末尾追加 `TestLeftJoinSafetyV2` 测试类，包含 5 个测试。

需要新增的 import（在文件顶部添加）：

```python
from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo
```

测试类代码：

```python
# ════════════════════════════════════════════
# V2 测试——dim 表增强文案 + JoinSafetyTableInfo + FieldNormalizer
# ════════════════════════════════════════════


class TestLeftJoinSafetyV2:
    """LEFT JOIN 安全门禁 V2 新功能测试。"""

    def test_dim_key_without_unique_blocks(self):
        """role=dim + join_key ∈ key_column_names → blocking，文案含 unique: true 和建议。"""
        from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo

        validator = RelationshipValidator()
        safety_info = JoinSafetyTableInfo(
            unique_keys=[],
            role="dim",
            key_column_names_normalized=["location_id"],
        )
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[],
            right_join_key_normalized="location_id",
            right_join_safety_info=safety_info,
        )
        assert is_safe is False
        assert desc is not None
        assert "unique: true" in desc
        assert "unique_keys: [['location_id']]" in desc
        assert "key_column" in desc

    def test_dim_key_with_unique_keys_passes(self):
        """role=dim 但 unique_keys 已声明 → 通过。"""
        from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo

        validator = RelationshipValidator()
        safety_info = JoinSafetyTableInfo(
            unique_keys=[["location_id"]],
            role="dim",
            key_column_names_normalized=["location_id"],
        )
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[["location_id"]],
            right_join_key_normalized="location_id",
            right_join_safety_info=safety_info,
        )
        assert is_safe is True
        assert desc is None

    def test_fact_no_unique_blocks(self):
        """role=fact + 无 unique_keys → blocking，文案不含 key_column 措辞。"""
        from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo

        validator = RelationshipValidator()
        safety_info = JoinSafetyTableInfo(
            unique_keys=[],
            role="fact",
            key_column_names_normalized=["order_id"],
        )
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[],
            right_join_key_normalized="order_id",
            right_join_safety_info=safety_info,
        )
        assert is_safe is False
        assert desc is not None
        # fact 表不应出现 dim 表专属的 key_column 增强文案
        assert "key_column" not in desc

    def test_field_normalizer_handles_special_chars(self):
        """key_column 声明为 "Location ID"（含空格），经 FieldNormalizer 归一化后匹配成功。
        
        证明 .lower() 不够——必须走 FieldNormalizer（处理空格/分隔符）。
        """
        from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo

        validator = RelationshipValidator()
        # key_column_names_normalized 应该已经由 Builder 侧的 FieldNormalizer 处理过
        # "Location ID" → FieldNormalizer.normalize() → "location_id"
        safety_info = JoinSafetyTableInfo(
            unique_keys=[],
            role="dim",
            key_column_names_normalized=["location_id"],
        )
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=[],
            right_join_key_normalized="location_id",
            right_join_safety_info=safety_info,
        )
        # 应匹配——因为 "location_id" == "location_id"
        assert is_safe is False  # 阻断（无 unique_keys）
        assert desc is not None
        # 但文案应该是 dim 增强版（因为 join_key 匹配了 key_column_names_normalized）
        assert "key_column" in desc

    def test_join_safety_info_default_factory(self):
        """JoinSafetyTableInfo() 默认构造——各字段默认值正确，安全门禁正常阻断。"""
        from tianshu_datadev.planning.relationship_validator import JoinSafetyTableInfo

        info = JoinSafetyTableInfo()
        assert info.unique_keys == []
        assert info.role is None
        assert info.key_column_names_normalized == []

        validator = RelationshipValidator()
        is_safe, desc = validator.check_left_join_safety(
            right_table_unique_keys=info.unique_keys if info else None,
            right_join_key_normalized="some_key",
            right_join_safety_info=info,
        )
        # 空 unique_keys → unsafe，role=None → 走通用文案而非 dim 增强
        assert is_safe is False
        assert desc is not None
        assert "key_column" not in desc  # role 不是 dim
```

- [ ] **Step 2: 运行 V2 测试——预期全部通过**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_left_join_safety.py::TestLeftJoinSafetyV2 -v --tb=long
```

预期：5/5 pass。

- [ ] **Step 3: 确认 V1 测试无退化**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/planning/test_left_join_safety.py -v --tb=short
```

预期：17（V1）+ 5（V2）= 22 passed。

- [ ] **Step 4: Commit**

```bash
git add tests/planning/test_left_join_safety.py
git commit -m "test: LEFT JOIN 安全门禁 V2 测试——dim 增强文案 + FieldNormalizer + default_factory 共 5 个"
```

---

### Task 8: 测试——SourceManifest + Parser 新用例（5 个）

**Files:**
- Modify: `tests/test_source_manifest.py` (追加 3 个测试)
- Modify: `tests/test_parser.py` (追加 2 个测试)

- [ ] **Step 1: 在 test_source_manifest.py 末尾添加 3 个测试**

```python
# ════════════════════════════════════════════
# V2 测试——unique_keys 合并 + Registry PK + role/ key_column_names 透传
# ════════════════════════════════════════════


class TestUniqueKeysMergeV2:
    """V2 unique_keys 合并逻辑测试。"""

    def test_merge_unique_keys_from_registry_pk(self):
        """Registry 返回 primary_key=["loc_id"] → manifest.unique_keys 含 ["loc_id"]。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.source_manifest import (
            SourceManifestBuilder,
            _merge_unique_keys_from_sources,
        )

        normalizer = FieldNormalizer()
        # 测试模块级合并函数：Registry primary_key 应合并到 unique_keys
        result = _merge_unique_keys_from_sources(
            [["loc_id"]],   # Registry primary_key 作为 unique_keys 来源
            None,            # Registry unique_keys
            normalizer=normalizer,
        )
        assert ["loc_id"] in result

    def test_builder_transmits_role_and_key_columns(self):
        """InputTableDecl(role="dim", key_columns=[ColumnDecl("Location_ID")])
        → ManifestTable.role="dim", key_column_names_normalized=["location_id"]。
        """
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.source_manifest import SourceManifestBuilder
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            InputTableDecl,
            ParsedDeveloperSpec,
            SafePhysicalTableName,
        )

        normalizer = FieldNormalizer()
        builder = SourceManifestBuilder(normalizer=normalizer)

        # 构建一个最小 InputTableDecl
        spec = ParsedDeveloperSpec(
            spec_id="test_v2",
            spec_hash="abc123",
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="tz",
                    source_table=SafePhysicalTableName("silver.taxi_zone"),
                    role="dim",
                    key_columns=[
                        ColumnDecl(
                            column_name="Location ID",
                            normalized_name=normalizer.normalize("Location ID"),
                            data_type="bigint",
                        ),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec={"columns": []},  # type: ignore[arg-type]
        )

        manifest, _ = builder.build(spec)
        table = manifest.tables[0]
        assert table.role == "dim"
        assert "location_id" in table.key_column_names_normalized

    def test_unique_keys_preserves_original_order(self):
        """unique_keys: [["zone_name", "borough"]] → 保留原始顺序，不排序。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.source_manifest import _normalize_unique_keys_list

        normalizer = FieldNormalizer()
        result = _normalize_unique_keys_list(
            [["zone_name", "borough"]], normalizer=normalizer
        )
        assert result == [["zone_name", "borough"]]
        # 不应被排序为 [["borough", "zone_name"]]
        assert result != [["borough", "zone_name"]]
```

- [ ] **Step 2: 在 test_parser.py 末尾添加 2 个测试**

```python
# ════════════════════════════════════════════
# V2 测试——unique_keys 解析 W005/W006
# ════════════════════════════════════════════


class TestUniqueKeysParserV2:
    """V2 parser unique_keys 解析测试。"""

    def test_parser_rejects_invalid_unique_keys(self):
        """YAML unique_keys: "not_a_list" → ParseWarning W005。"""
        parser = DeveloperSpecParser()
        text = """\
spec_id: test_w005
title: 测试 W005
description: 测试

input_tables:
  - name: test.table1
    alias: t1
    key_columns:
      - name: id
        type: bigint
    unique_keys: not_a_list

metrics:
  - name: cnt
    sql: COUNT(*)

output:
  columns: []
"""
        spec = parser.parse(text)
        w005_warnings = [w for w in spec.parse_warnings if w.code == "W005"]
        assert len(w005_warnings) >= 1, f"应有 W005 警告，实际 warnings: {spec.parse_warnings}"

    def test_parser_accepts_valid_unique_keys(self):
        """YAML unique_keys: [[location_id], [zone_name, borough]]
        → InputTableDecl.unique_keys 正确解析。
        """
        parser = DeveloperSpecParser()
        text = """\
spec_id: test_valid_uk
title: 测试合法 unique_keys
description: 测试

input_tables:
  - name: silver.taxi_zone
    alias: tz
    key_columns:
      - name: location_id
        type: bigint
      - name: zone_name
        type: varchar
      - name: borough
        type: varchar
    unique_keys:
      - [location_id]
      - [zone_name, borough]

metrics:
  - name: cnt
    sql: COUNT(*)

output:
  columns: []
"""
        spec = parser.parse(text)
        table = spec.input_tables[0]
        assert table.unique_keys == [["location_id"], ["zone_name", "borough"]]
```

- [ ] **Step 3: 运行新增测试**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/test_source_manifest.py::TestUniqueKeysMergeV2 tests/test_parser.py::TestUniqueKeysParserV2 -v --tb=long
```

预期：5/5 pass。

- [ ] **Step 4: 全量回归**

```bash
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -v --tb=short
```

预期：全部通过（现有基线 + 10 个 V2 新测试）。

- [ ] **Step 5: Commit**

```bash
git add tests/test_source_manifest.py tests/test_parser.py
git commit -m "test: SourceManifest + Parser V2 测试——Registry PK 合并 + role 透传 + 顺序保留 + W005/W006"
```

---

## 验证

```bash
# 全量回归
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m pytest tests/ -v --tb=short

# ruff 检查
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && python -m ruff check src/ tests/

# 确认 git diff 覆盖所有 8 个文件
cd "D:\Program Files\gitvscode\TianShu-DataDev-Agent-v3" && git diff --stat main
```
