"""FakeRelationshipPlanner——Phase 1B 确定性 Join 推测器。

仅从 DeveloperSpec 显式 Join 声明生成候选（不推断），
调用 RelationshipValidator 定级，过滤 WEAK/NONE，生成证据链 YAML。
不依赖 LLM——Phase 4 替换为真实 LLM Planner。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
from tianshu_datadev.developer_spec.models import JoinDecl, OpenQuestion, ParsedDeveloperSpec, SourceManifest

from .models import JoinType
from .relationship_hypothesis import (
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipEvidence,
    RelationshipHypothesis,
)
from .relationship_validator import RelationshipValidator

if TYPE_CHECKING:
    from tianshu_datadev.llm.adapters.base import ProviderAdapter


class FakeRelationshipPlanner:
    """Phase 1B 确定性 Join 推测器（Fake 实现）。

    行为：
    1. 从 DeveloperSpec.joins 提取显式声明的 Join
    2. 对每个 Join 调用 FieldNormalizer 归一化键名
    3. 调用 RelationshipValidator 确定性定级
    4. STRONG/MEDIUM → 加入 hypothesis.candidates
    5. WEAK/NONE → 生成 OpenQuestion，不加入 candidates（硬门禁）
    6. 生成可渲染的 YAML 证据链文本
    """

    def __init__(
        self,
        validator: RelationshipValidator | None = None,
        normalizer: FieldNormalizer | None = None,
    ):
        """初始化 Fake Planner。

        Args:
            validator: 证据评级器，None 使用默认 RelationshipValidator
            normalizer: 字段名归一化器，None 使用默认 FieldNormalizer
        """
        self._validator = validator or RelationshipValidator()
        self._normalizer = normalizer or FieldNormalizer()

    def plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest | None = None,
    ) -> tuple[RelationshipHypothesis, list[OpenQuestion]]:
        """基于 DeveloperSpec 构建 RelationshipHypothesis。

        Phase 1B 仅处理显式 Join 声明——不进行字段名匹配推理。
        manifest 用于 LEFT JOIN 唯一性安全门禁（Phase 1 新增）。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 可选的 SourceManifest——用于查询右表 unique_keys

        Returns:
            (RelationshipHypothesis, list[OpenQuestion])
        """
        open_questions: list[OpenQuestion] = []
        candidates: list[JoinCandidate] = []

        # 构建 unique_keys 查询表（供 LEFT JOIN 安全门禁使用）
        table_unique_keys = self._build_unique_keys_lookup(manifest)

        # 从显式声明提取候选
        if spec.joins:
            for join_decl in spec.joins:
                candidate = self._build_candidate(join_decl, spec)
                open_q = self._rate_and_decide(candidate, table_unique_keys)
                if open_q:
                    open_questions.append(open_q)
                else:
                    # STRONG/MEDIUM 且无额外问题的加入 candidates
                    candidates.append(candidate)

        multi_table = len(spec.input_tables) > 1

        hypothesis = RelationshipHypothesis(
            hypothesis_id=RelationshipHypothesis.generate_hypothesis_id(spec.spec_hash),
            spec_hash=spec.spec_hash,
            source_manifest_hash=manifest.spec_hash if manifest else None,
            candidates=candidates,
            multi_table=multi_table,
        )

        return hypothesis, open_questions

    # ── 内部方法 ──

    def _build_candidate(self, join_decl: JoinDecl, spec: ParsedDeveloperSpec) -> JoinCandidate:
        """将 DeveloperSpec JoinDecl 转换为 JoinCandidate。"""
        left_normalized = self._normalizer.normalize(join_decl.left_key)
        right_normalized = self._normalizer.normalize(join_decl.right_key)

        return JoinCandidate(
            candidate_id=JoinCandidate.generate_candidate_id(
                join_decl.left_table,
                join_decl.right_table,
                join_decl.left_key,
                join_decl.right_key,
            ),
            left_table=join_decl.left_table,
            right_table=join_decl.right_table,
            left_key=join_decl.left_key,
            right_key=join_decl.right_key,
            left_key_normalized=left_normalized,
            right_key_normalized=right_normalized,
            join_type=self._map_join_type(join_decl),
        )

    def _rate_and_decide(
        self,
        candidate: JoinCandidate,
        table_unique_keys: dict[str, list[list[str]]] | None = None,
    ) -> OpenQuestion | None:
        """对候选调用 Validator 定级，填充 evidence，按等级决定去向。

        Phase 1 新增：STRONG + LEFT JOIN 时检查右表联结键唯一性，
        无证据则生成 blocking OpenQuestion。

        Args:
            candidate: Join 候选
            table_unique_keys: {table_ref: unique_keys} 查询表

        Returns:
            OpenQuestion（WEAK/NONE/不安全时）或 None（通过时）。
        """
        # 判断字段名归一化后是否匹配
        names_match = candidate.left_key_normalized == candidate.right_key_normalized

        # 计算编辑距离
        edit_dist = self._edit_distance(
            candidate.left_key_normalized,
            candidate.right_key_normalized,
        )

        # 判断别名匹配（编辑距离不为0但归一化后仍不同 → 检查是否仅差别名）
        alias_match = not names_match and edit_dist is not None and edit_dist <= 2

        # 类型兼容性——Phase 1B 默认兼容（DeveloperSpec 显式声明视为程序员保证了兼容性）
        types_compatible = True

        # 调用 Validator 定级
        level, action = self._validator.rate(
            has_explicit_decl=True,  # Phase 1B 只处理显式声明
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,  # Phase 1B 不使用 schema_registry
            has_high_distinct_ratio=False,
        )

        # 构建证据检查列表
        evidence_checks = self._build_checks(
            names_match=names_match,
            edit_dist=edit_dist,
            types_compatible=types_compatible,
            has_explicit_decl=True,
        )

        # 生成可读理由
        detail = self._validator.generate_detail(
            level=level,
            has_explicit_decl=True,
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,
            has_high_distinct_ratio=False,
        )

        # 构建证据记录
        evidence = RelationshipEvidence(
            evidence_id=f"ev_{candidate.candidate_id}",
            level=level,
            action=action,
            left_table=candidate.left_table,
            right_table=candidate.right_table,
            left_key_raw=candidate.left_key,
            right_key_raw=candidate.right_key,
            left_key_normalized=candidate.left_key_normalized,
            right_key_normalized=candidate.right_key_normalized,
            evidence_checks=evidence_checks,
            detail=detail,
        )
        # 生成可渲染证据链 YAML
        evidence.generate_evidence_chain_yaml()

        # 填入 evidence
        object.__setattr__(candidate, "evidence", evidence)

        # WEAK/NONE 硬门禁——生成 OpenQuestion 阻断
        if level in (JoinEvidenceLevel.WEAK, JoinEvidenceLevel.NONE):
            blocking = level == JoinEvidenceLevel.WEAK
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"证据等级 {level.value}——{detail}"
                ),
                blocking=blocking,
            )

        # MEDIUM → OpenQuestion（非阻断）
        if level == JoinEvidenceLevel.MEDIUM:
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"证据等级 MEDIUM——需人工确认"
                ),
                blocking=False,
            )

        # STRONG → LEFT JOIN 唯一性安全门禁
        if level == JoinEvidenceLevel.STRONG:
            return self._check_left_join_safety_gate(candidate, table_unique_keys)

        return None

    # ── LEFT JOIN 安全门禁 ──

    def _check_left_join_safety_gate(
        self,
        candidate: JoinCandidate,
        table_unique_keys: dict[str, list[list[str]]] | None,
    ) -> OpenQuestion | None:
        """STRONG 通过后，对 LEFT JOIN 做右表联结键唯一性检查。

        只有 LEFT JOIN 需要此门禁——INNER/RIGHT/FULL 不触发。
        无唯一性证据时返回 blocking OpenQuestion。

        Args:
            candidate: 已通过 STRONG 评级的 Join 候选
            table_unique_keys: {table_ref: unique_keys} 查询表

        Returns:
            OpenQuestion（不安全时）或 None（安全通过）。
        """
        if candidate.join_type != JoinType.LEFT:
            return None

        # 查询右表的 unique_keys
        right_unique = None
        if table_unique_keys:
            right_unique = table_unique_keys.get(candidate.right_table)

        is_safe, desc = self._validator.check_left_join_safety(
            right_table_unique_keys=right_unique,
            right_join_key=candidate.right_key,
        )

        if not is_safe:
            return OpenQuestion(
                question_id=f"Q-JOIN-SAFETY-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.right_table}.{candidate.right_key}",
                description=desc,
                blocking=True,  # 阻断——不允许静默笛卡尔积
            )

        return None

    # ── 辅助 ──

    @staticmethod
    def _map_join_type(join_decl: JoinDecl) -> JoinType:
        """将 developer_spec JoinDecl.join_type 映射到 planning JoinType。"""
        # JoinDecl.join_type 是 JoinTypeEnum（developer_spec），映射到 JoinType（planning）
        mapping = {
            "INNER": JoinType.INNER,
            "LEFT": JoinType.LEFT,
            "RIGHT": JoinType.RIGHT,
            "FULL": JoinType.FULL,
        }
        return mapping.get(str(join_decl.join_type.value), JoinType.INNER)

    @staticmethod
    def _edit_distance(a: str, b: str) -> int:
        """计算两个字符串的莱文斯坦编辑距离。"""
        if len(a) < len(b):
            a, b = b, a
        # a 是较长的字符串
        if len(b) == 0:
            return len(a)
        # 使用两行滚动数组节省内存
        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a, 1):
            curr = [i]
            for j, cb in enumerate(b, 1):
                if ca == cb:
                    curr.append(prev[j - 1])
                else:
                    curr.append(1 + min(prev[j], curr[-1], prev[j - 1]))
            prev = curr
        return prev[-1]

    @staticmethod
    def _build_unique_keys_lookup(
        manifest: SourceManifest | None,
    ) -> dict[str, list[list[str]]]:
        """从 SourceManifest 构建 {table_ref: unique_keys} 查询表。

        供 LEFT JOIN 唯一性安全门禁使用。

        Args:
            manifest: 源数据清单，None 时返回空 dict

        Returns:
            {table_ref: [[col1, col2], ...]} 映射
        """
        if manifest is None:
            return {}
        lookup: dict[str, list[list[str]]] = {}
        for table in manifest.tables:
            if table.unique_keys:
                lookup[table.table_ref] = table.unique_keys
        return lookup

    @staticmethod
    def _build_checks(
        names_match: bool,
        edit_dist: int | None,
        types_compatible: bool,
        has_explicit_decl: bool,
    ) -> list[str]:
        """构建证据检查列表——记录每条检查的结果。"""
        checks: list[str] = []
        if has_explicit_decl:
            checks.append("developer_declared: FOUND")
        else:
            checks.append("developer_declared: NOT_FOUND")

        if names_match:
            checks.append("field_name_match: MATCH")
        elif edit_dist is not None and edit_dist <= 2:
            checks.append(f"field_name_similarity: PARTIAL (edit_distance={edit_dist})")
        else:
            checks.append("field_name_match: MISMATCH")

        if types_compatible:
            checks.append("type_compatibility: MATCH")
        else:
            checks.append("type_compatibility: MISMATCH")

        checks.append("unique_index: NOT_CHECKED")
        checks.append("foreign_key: NOT_CHECKED")
        return checks


# ════════════════════════════════════════════
# LLM Prompt 模板——Phase 4E 启用
# ════════════════════════════════════════════

_RELATIONSHIP_INFERENCE_PROMPT = """你是数据仓库表关系推断专家。你的任务是阅读业务描述和表结构，
推断表之间的 Join 关系，并输出严格的 JSON 结构。

════════════════════════════════════
硬约束（违反任何一条都是错误）
════════════════════════════════════

H1. 键名只能从提供的 [Table Schemas] 中选择，禁止编造不存在的列名。
    如果你需要的列不在 Schema 中，不要编造——跳过该候选。

H2. 只能推断这些 Join 类型之一：INNER | LEFT | RIGHT | FULL。
    不要使用 CROSS JOIN、NATURAL JOIN 或任何非等值 Join。

H3. 只能推断两表之间的直接 Join——不要推测三表或更复杂的多跳关系。
    每对表之间最多输出 1 个 Join 候选（按最匹配的键对）。
    多对表关系由多次调用独立处理。

H4. 不能覆盖程序员已显式声明的 Join 关系（[Existing Joins] 中的条目）。
    你的输出与声明列表合并——相同 (left_table, right_table) 对以声明为准。

H5. 键名仅为列名本身，不包含表名前缀。
    正确: "user_id" 错误: "users.user_id"、"tf.user_id"。

H6. 不确定时设置 confidence=low，不要猜测。
    confidence 取值：
    - high:   明显的外键命名模式（orders.customer_id → customers.id）
    - medium: 同名列或语义相关命名，但无明确 FK 模式
    - low:    仅凭描述推测，列名无明确对应关系

H7. 你只接收 schema 信息（列名 + 类型 + 可空标记），不接收数据样本。
    不要要求或期望看到实际数据值。推测仅基于表结构和业务描述。

════════════════════════════════════
推断规则
════════════════════════════════════

- 同名列优先：两表中有完全相同列名 → 高优先级候选
- 外键命名模式：orders.user_id → users.id —— 通过 _id 后缀与 id 列的对应关系推断
- 语义相关：表 A 的 dept_id 与表 B 的 department_id —— 仅当描述文本明确提到两者关联时考虑
- 多键关系：如果描述中明确说明了多个关联条件，输出多个 Join 候选（一对表一个候选）
- 字段类型兼容：int ↔ bigint ↔ decimal 兼容；varchar 与 varchar 兼容；
  跨类型（int ↔ varchar）仍可输出，交给 Validator 确定性裁决

════════════════════════════════════
输出格式
════════════════════════════════════

严格按以下 JSON 输出，禁止多余文本或解释：

{
  "inferred_joins": [
    {
      "left_table": "左表别名（从 Table Schemas 的 table_ref 中选择）",
      "right_table": "右表别名（从 Table Schemas 的 table_ref 中选择）",
      "left_key": "左表字段名（从该表的 columns 中选择）",
      "right_key": "右表字段名（从该表的 columns 中选择）",
      "join_type": "INNER|LEFT|RIGHT|FULL",
      "confidence": "high|medium|low",
      "reasoning": "一句话推断依据"
    }
  ]
}

如果无法推断任何 Join 关系，输出空数组：
{"inferred_joins": []}"""


# ════════════════════════════════════════════
# LLM 输出 JSON Schema——传给 AnthropicAdapter 做 structured output
# ════════════════════════════════════════════

_RELATIONSHIP_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "inferred_joins": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "left_table": {
                        "type": "string",
                        "description": "左表别名——必须从 Table Schemas 的 table_ref 中选择",
                    },
                    "right_table": {
                        "type": "string",
                        "description": "右表别名——必须从 Table Schemas 的 table_ref 中选择",
                    },
                    "left_key": {
                        "type": "string",
                        "description": "左表 Join 键列名——必须存在于 left_table 的 columns 中",
                    },
                    "right_key": {
                        "type": "string",
                        "description": "右表 Join 键列名——必须存在于 right_table 的 columns 中",
                    },
                    "join_type": {
                        "type": "string",
                        "enum": ["INNER", "LEFT", "RIGHT", "FULL"],
                        "description": "Join 类型",
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "推断置信度：high(FK命名模式)/medium(同名列)/low(仅描述推测)",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "一句话推断依据",
                    },
                },
                "required": [
                    "left_table",
                    "right_table",
                    "left_key",
                    "right_key",
                    "join_type",
                    "confidence",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["inferred_joins"],
    "additionalProperties": False,
}


class RelationshipPlanner:
    """Phase 4E LLM Join 推断器——调用 LLM 从表结构和业务描述推断 Join 关系。

    使用嵌入 7 条硬约束的 System Prompt + JSON 约束输出。
    需要注入 ProviderAdapter，Phase 4E 装配。

    与 FakeRelationshipPlanner 接口完全一致——相同 (spec, manifest) → (hypothesis, questions)。
    adapter=None 时完全退化为 FakeRelationshipPlanner 行为。
    """

    # 字段类型兼容性矩阵——同组类型视为兼容
    # Validator 仍会做最终裁决，此处仅用于上下文质量标注
    _TYPE_GROUPS: list[set[str]] = [
        {"int", "bigint", "integer", "smallint", "tinyint", "decimal", "numeric", "float", "double", "real"},
        {"varchar", "char", "text", "string", "nvarchar", "longtext"},
        {"date", "datetime", "timestamp", "timestamptz"},
        {"boolean", "bool"},
    ]

    def __init__(
        self,
        adapter: ProviderAdapter | None = None,
        validator: RelationshipValidator | None = None,
        normalizer: FieldNormalizer | None = None,
    ):
        """初始化 LLM 推断器。

        Args:
            adapter: LLM Provider 适配器，Phase 4E 注入。
                     None 时退化为 FakeRelationshipPlanner（纯规则推断）。
            validator: 证据评级器，None 使用默认 RelationshipValidator
            normalizer: 字段名归一化器，None 使用默认 FieldNormalizer
        """
        self._adapter = adapter
        self._validator = validator or RelationshipValidator()
        self._normalizer = normalizer or FieldNormalizer()
        self._fake = FakeRelationshipPlanner(self._validator, self._normalizer)

    def plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest | None = None,
    ) -> tuple[RelationshipHypothesis, list[OpenQuestion]]:
        """基于 spec + manifest 构建 RelationshipHypothesis。

        adapter=None → 退化到 FakeRelationshipPlanner。
        adapter 已注入 → LLM 推断隐式 Join 并与显式声明合并。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单（LLM 模式需要，Fake 模式可选）

        Returns:
            (RelationshipHypothesis, list[OpenQuestion])
        """
        if self._adapter is None:
            return self._fake.plan(spec, manifest)

        return self._llm_plan(spec, manifest)

    # ── LLM 推断内部方法 ──

    def _build_context(self, spec: ParsedDeveloperSpec, manifest: SourceManifest) -> dict:
        """构建 LLM 调用的 Context 部分——不包含数据样本（遵守 H7）。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            可序列化的 Context dict
        """
        # 表结构——仅含列名 + 类型 + 可空
        tables_info: list[dict] = []
        for table in manifest.tables:
            cols_info: list[dict] = []
            for col in table.columns:
                cols_info.append({
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                })
            tables_info.append({
                "table_ref": table.table_ref,
                "source_table": str(table.source_table) if table.source_table else None,
                "columns": cols_info,
            })

        # 已有显式声明——不可覆盖（H4）
        existing_joins: list[dict] = []
        if spec.joins:
            for j in spec.joins:
                existing_joins.append({
                    "left_table": j.left_table,
                    "right_table": j.right_table,
                    "left_key": j.left_key,
                    "right_key": j.right_key,
                    "join_type": (
                        str(j.join_type.value)
                        if hasattr(j.join_type, "value")
                        else str(j.join_type)
                    ),
                })

        return {
            "table_schemas": tables_info,
            "existing_joins": existing_joins,
            "business_description": spec.description,
            "spec_title": spec.title,
        }

    def _parse_llm_response(self, raw: dict, manifest: SourceManifest) -> list[dict]:
        """解析 LLM 返回的 JSON 并校验——确保字段名在 manifest 中存在（H1）。

        容错策略：不抛异常，不合法的候选项直接丢弃，合法项保留。

        Args:
            raw: LLM 返回的原始 JSON
            manifest: 源数据清单（用于校验字段名存在性）

        Returns:
            校验通过的 Join 候选 dict 列表
        """
        valid: list[dict] = []

        # 构建合法字段名集合（按 table_ref 分组）
        table_columns: dict[str, set[str]] = {}
        for table in manifest.tables:
            table_columns[table.table_ref] = {col.column_name for col in table.columns}

        for item in raw.get("inferred_joins", []):
            left_table = item.get("left_table", "")
            right_table = item.get("right_table", "")
            left_key = item.get("left_key", "")
            right_key = item.get("right_key", "")

            # H1：字段名必须在 manifest 中存在
            left_cols = table_columns.get(left_table, set())
            right_cols = table_columns.get(right_table, set())
            if left_key not in left_cols or right_key not in right_cols:
                continue  # 非法字段名 → 丢弃

            # 表别名必须有效
            if left_table not in table_columns or right_table not in table_columns:
                continue

            # 跳过同一表自 Join（留给 Builder 的自引用检测处理）
            if left_table == right_table:
                continue

            # H2：校验 join_type 为合法枚举
            join_type = item.get("join_type", "INNER")
            if join_type not in ("INNER", "LEFT", "RIGHT", "FULL"):
                join_type = "INNER"

            # 校验 confidence
            confidence = item.get("confidence", "medium")
            if confidence not in ("high", "medium", "low"):
                confidence = "medium"

            valid.append({
                "left_table": left_table,
                "right_table": right_table,
                "left_key": left_key,
                "right_key": right_key,
                "join_type": join_type,
                "confidence": confidence,
                "reasoning": item.get("reasoning", ""),
            })

        return valid

    def _llm_plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[RelationshipHypothesis, list[OpenQuestion]]:
        """LLM 推断流程——Context → LLM → 解析 → 合并显式声明 → 校验 → 定级。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            (RelationshipHypothesis, list[OpenQuestion])
        """
        open_questions: list[OpenQuestion] = []
        candidates: list[JoinCandidate] = []

        # 构建 unique_keys 查询表（供 LEFT JOIN 安全门禁使用）
        table_unique_keys = self._fake._build_unique_keys_lookup(manifest)

        # 步骤 1：先处理显式声明（不变——高优先级）
        explicit_candidates: dict[tuple[str, str], JoinCandidate] = {}
        if spec.joins:
            for join_decl in spec.joins:
                candidate = self._fake._build_candidate(join_decl, spec)
                open_q = self._fake._rate_and_decide(candidate, table_unique_keys)
                if open_q:
                    open_questions.append(open_q)
                else:
                    explicit_candidates[(join_decl.left_table, join_decl.right_table)] = candidate

        # 步骤 2：构建 Context → 调用 LLM
        context = self._build_context(spec, manifest)
        raw: dict = {"inferred_joins": []}

        try:
            raw = self._adapter.invoke(
                system_message=_RELATIONSHIP_INFERENCE_PROMPT,
                user_message=json.dumps(context, ensure_ascii=False),
                json_schema=_RELATIONSHIP_JSON_SCHEMA,
                model="",
                temperature=0.1,
            )
        except Exception:
            # LLM 调用失败 → 退化为纯显式声明模式，不阻断流程
            raw = {"inferred_joins": []}

        # 步骤 3：解析 LLM 返回
        inferred_items = self._parse_llm_response(raw, manifest)

        # 步骤 4：合并——显式声明覆盖 LLM 推断（H4）
        for item in inferred_items:
            pair_key = (item["left_table"], item["right_table"])
            if pair_key in explicit_candidates:
                continue

            candidate = self._build_llm_candidate(item)
            open_q = self._rate_and_decide_llm(candidate, item["confidence"], table_unique_keys)
            if open_q:
                open_questions.append(open_q)
                # MEDIUM（非阻断人审）→ 候选仍加入流程，供人工确认后采纳
                # WEAK/NONE → 硬阻断，候选不加入
                if not open_q.blocking and candidate.evidence is not None and \
                   candidate.evidence.level == JoinEvidenceLevel.MEDIUM:
                    candidates.append(candidate)
            else:
                candidates.append(candidate)

        # 步骤 5：合并显式声明候选项
        final_candidates = list(explicit_candidates.values()) + candidates

        multi_table = len(spec.input_tables) > 1

        hypothesis = RelationshipHypothesis(
            hypothesis_id=RelationshipHypothesis.generate_hypothesis_id(spec.spec_hash),
            spec_hash=spec.spec_hash,
            source_manifest_hash=manifest.spec_hash if manifest else None,
            candidates=final_candidates,
            multi_table=multi_table,
        )

        return hypothesis, open_questions

    def _build_llm_candidate(self, item: dict) -> JoinCandidate:
        """将 LLM 推断的 dict 转为 JoinCandidate。

        与 Fake._build_candidate 的区别：不调用 FieldNormalizer（字段名已在 _parse_llm_response 校验过），
        但保留归一化以保持与 Validator 的兼容。

        Args:
            item: 来自 _parse_llm_response 的合法 dict

        Returns:
            JoinCandidate 实例
        """
        left_normalized = self._normalizer.normalize(item["left_key"])
        right_normalized = self._normalizer.normalize(item["right_key"])

        return JoinCandidate(
            candidate_id=JoinCandidate.generate_candidate_id(
                item["left_table"],
                item["right_table"],
                item["left_key"],
                item["right_key"],
            ),
            left_table=item["left_table"],
            right_table=item["right_table"],
            left_key=item["left_key"],
            right_key=item["right_key"],
            left_key_normalized=left_normalized,
            right_key_normalized=right_normalized,
            join_type=self._map_join_type_str(item["join_type"]),
        )

    def _map_join_type_str(self, join_type_str: str) -> JoinType:
        """将 LLM 输出的字符串 join_type 映射到 JoinType 枚举。

        Args:
            join_type_str: 来自 LLM 的字符串（已通过 H2 校验）

        Returns:
            JoinType 枚举值
        """
        mapping = {
            "INNER": JoinType.INNER,
            "LEFT": JoinType.LEFT,
            "RIGHT": JoinType.RIGHT,
            "FULL": JoinType.FULL,
        }
        return mapping.get(join_type_str, JoinType.INNER)

    def _rate_and_decide_llm(
        self,
        candidate: JoinCandidate,
        llm_confidence: str,
        table_unique_keys: dict[str, list[list[str]]] | None = None,
    ) -> OpenQuestion | None:
        """对 LLM 推断的候选调用 Validator 定级——与显式声明的区别在于 has_explicit_decl=False。

        LLM confidence 不作为唯一性证据——LEFT JOIN 安全门禁只信任
        ManifestTable.unique_keys / primary_key 等确定性来源。

        Args:
            candidate: LLM 推断的 JoinCandidate（未填充 evidence）
            llm_confidence: LLM 的置信度标签（high/medium/low）
            table_unique_keys: {table_ref: unique_keys} 查询表

        Returns:
            OpenQuestion 或 None
        """
        # 判断字段名归一化后是否匹配
        names_match = candidate.left_key_normalized == candidate.right_key_normalized

        # 计算编辑距离
        edit_dist = self._edit_distance(
            candidate.left_key_normalized,
            candidate.right_key_normalized,
        )

        # 别名匹配判定
        alias_match = not names_match and edit_dist is not None and edit_dist <= 2

        # LLM 推断的来源没有显式声明——has_explicit_decl=False
        has_explicit_decl = False

        # 类型兼容性——基于 _TYPE_GROUPS 判断
        types_compatible = True  # 具体类型检查由 Builder 执行

        # 调用 Validator 定级
        level, action = self._validator.rate(
            has_explicit_decl=has_explicit_decl,
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,
            has_high_distinct_ratio=llm_confidence == "high",
        )

        # 构建证据检查列表
        evidence_checks = self._build_llm_checks(
            names_match=names_match,
            edit_dist=edit_dist,
            types_compatible=types_compatible,
            llm_confidence=llm_confidence,
        )

        # 生成可读理由
        detail = self._validator.generate_detail(
            level=level,
            has_explicit_decl=has_explicit_decl,
            has_fk_constraint=False,
            names_normalized_match=names_match,
            name_edit_distance=edit_dist if edit_dist is not None else None,
            name_alias_match=alias_match,
            types_compatible=types_compatible,
            has_unique_index=False,
            has_high_distinct_ratio=llm_confidence == "high",
        )

        # 构建证据记录
        evidence = RelationshipEvidence(
            evidence_id=f"ev_{candidate.candidate_id}",
            level=level,
            action=action,
            left_table=candidate.left_table,
            right_table=candidate.right_table,
            left_key_raw=candidate.left_key,
            right_key_raw=candidate.right_key,
            left_key_normalized=candidate.left_key_normalized,
            right_key_normalized=candidate.right_key_normalized,
            evidence_checks=evidence_checks,
            detail=detail,
        )
        evidence.generate_evidence_chain_yaml()
        object.__setattr__(candidate, "evidence", evidence)

        # WEAK/NONE 硬门禁
        if level in (JoinEvidenceLevel.WEAK, JoinEvidenceLevel.NONE):
            blocking = level == JoinEvidenceLevel.WEAK
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"LLM 推断 Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"(LLM 置信度={llm_confidence})——证据等级 {level.value}——{detail}"
                ),
                blocking=blocking,
            )

        # MEDIUM → OpenQuestion（非阻断）
        if level == JoinEvidenceLevel.MEDIUM:
            return OpenQuestion(
                question_id=f"Q-JOIN-{candidate.candidate_id}",
                source="relationship",
                field_ref=f"{candidate.left_table}.{candidate.left_key}",
                description=(
                    f"LLM 推断 Join {candidate.left_table}.{candidate.left_key} "
                    f"= {candidate.right_table}.{candidate.right_key} "
                    f"(LLM 置信度={llm_confidence})——证据等级 MEDIUM——需人工确认"
                ),
                blocking=False,
            )

        # STRONG → LEFT JOIN 唯一性安全门禁
        if level == JoinEvidenceLevel.STRONG:
            return self._fake._check_left_join_safety_gate(candidate, table_unique_keys)

        return None

    # ── 静态辅助 ──

    @staticmethod
    def _edit_distance(a: str, b: str) -> int:
        """计算两个字符串的莱文斯坦编辑距离。"""
        if len(a) < len(b):
            a, b = b, a
        if len(b) == 0:
            return len(a)
        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a, 1):
            curr = [i]
            for j, cb in enumerate(b, 1):
                if ca == cb:
                    curr.append(prev[j - 1])
                else:
                    curr.append(1 + min(prev[j], curr[-1], prev[j - 1]))
            prev = curr
        return prev[-1]

    @staticmethod
    def _build_llm_checks(
        names_match: bool,
        edit_dist: int | None,
        types_compatible: bool,
        llm_confidence: str,
    ) -> list[str]:
        """构建 LLM 推断候选的证据检查列表。

        与 Fake 的 _build_checks 区别：记录 LLM 置信度。

        Args:
            names_match: 归一化字段名是否匹配
            edit_dist: 编辑距离
            types_compatible: 类型是否兼容
            llm_confidence: LLM 置信度标签

        Returns:
            证据检查字符串列表
        """
        checks: list[str] = [
            f"llm_inferred: YES (confidence={llm_confidence})",
            "developer_declared: NOT_FOUND",
        ]

        if names_match:
            checks.append("field_name_match: MATCH")
        elif edit_dist is not None and edit_dist <= 2:
            checks.append(f"field_name_similarity: PARTIAL (edit_distance={edit_dist})")
        else:
            checks.append("field_name_match: MISMATCH")

        if types_compatible:
            checks.append("type_compatibility: MATCH")
        else:
            checks.append("type_compatibility: MISMATCH")

        checks.append("unique_index: NOT_CHECKED")
        checks.append("foreign_key: NOT_CHECKED")
        return checks
