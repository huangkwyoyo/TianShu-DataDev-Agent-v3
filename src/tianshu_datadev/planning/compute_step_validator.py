"""ComputeStepValidator——Builder 前确定性门禁。

五项检查（全部确定性，不调 LLM）：
a. 符号解析——Join 键在对应的上游输出/SourceManifest 中存在
b. 类型兼容——left_key 与 right_key 字段类型兼容（UNKNOWN 类型阻断）
c. Join 基数安全——单列 Join 仅由完全匹配的单列唯一键组放行
d. 显式 JoinDecl——合流步骤必须有显式 JoinDecl
e. evaluation_phase 已确定

不返回 BuildReadyStep——仅返回 list[OpenQuestion]。
由 Pipeline（api/pipeline.py）在 Builder 前调用。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.models import OpenQuestion

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ComputeStep
    from tianshu_datadev.developer_spec.source_manifest import SourceManifest
    from tianshu_datadev.planning.field_normalizer import FieldNormalizer
    from tianshu_datadev.planning.step_output_schema import StepOutputSchema


# ── 类型兼容矩阵 ──
_TYPE_COMPAT: dict[tuple[str, str], bool] = {
    ("bigint", "integer"): True, ("integer", "bigint"): True,
    ("bigint", "bigint"): True, ("integer", "integer"): True,
    ("varchar", "varchar"): True, ("varchar", "text"): True,
    ("text", "varchar"): True,
    ("decimal", "double"): True, ("double", "decimal"): True,
    ("decimal", "decimal"): True, ("double", "double"): True,
    ("timestamp", "timestamp"): True, ("boolean", "boolean"): True,
}


def _normalize_type(type_str: str | None) -> str | None:
    """归一化字段类型——去参数部分并小写。None 返回 None（UNKNOWN）。"""
    if type_str is None:
        return None
    return type_str.lower().split("(")[0].strip()


def _types_compatible(left_type: str | None, right_type: str | None) -> bool:
    """检查两个字段类型是否兼容。UNKNOWN 类型永远不兼容。"""
    if left_type is None or right_type is None:
        return False  # UNKNOWN 类型阻断
    ln = _normalize_type(left_type)
    rn = _normalize_type(right_type)
    if ln is None or rn is None:
        return False
    if ln == rn:
        return True
    return _TYPE_COMPAT.get((ln, rn), False)


class ComputeStepValidator:
    """ComputeStep 确定性校验器。

    由 Pipeline（api/pipeline.py）在 Builder 前调用。
    Builder 不持有 Validator 实例、不引用 manifest。
    """

    def __init__(self, normalizer: FieldNormalizer, spec_hash: str = ""):
        self._normalizer = normalizer
        self._spec_hash = spec_hash

    def _qid(self, step_name: str, check: str, field: str) -> str:
        """生成确定性 question_id——禁止 UUID。"""
        return f"cs:{self._spec_hash}:{step_name}:{check}:{field}"

    def validate(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
    ) -> list[OpenQuestion]:
        """对单个 ComputeStep 执行全部校验。

        Args:
            cs: 待校验的 ComputeStep
            step_schemas: {step_name: StepOutputSchema}——上游步骤的输出 schema
            manifest: 源数据清单（None 时跳过 manifest 相关校验）

        Returns:
            OpenQuestion 列表——空列表 = 全部通过
        """
        errors: list[OpenQuestion] = []

        # ── 校验 a+b+c：Join 符号解析 + 类型兼容 + 基数安全 ──
        if cs.joins:
            self._validate_joins(cs, step_schemas, manifest, errors)

        # ── 校验 d：合流必须显式 JoinDecl ──
        if isinstance(cs.source, list) and len(cs.source) > 1:
            if not cs.joins:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "no_join_decl", "joins"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins",
                    description=(
                        f"合流步骤 '{cs.step_name}' source={cs.source}，"
                        f"未声明 joins——禁止共同列猜键和隐式 CROSS JOIN"
                    ),
                    blocking=True,
                ))

        # ── 校验 e：evaluation_phase 已确定 ──
        if cs.case_when and cs.case_when.branches:
            if cs.case_when.evaluation_phase is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "eval_phase_none", "case_when"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.case_when.evaluation_phase",
                    description=(
                        f"compute_step '{cs.step_name}' case_when "
                        f"evaluation_phase 未确定——SpecEnricher 未成功判定"
                    ),
                    blocking=True,
                ))

        return errors

    def _validate_joins(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
        errors: list[OpenQuestion],
    ) -> None:
        """校验所有 Join 声明——符号+类型+基数。

        source=[a,b] 时：分别解析 a 和 b 的输出 schema——
        左键在 left_schema.columns 中查找，右键在 right_schema.columns 中查找。
        """
        # 构建 SourceManifest 查找表
        manifest_cols: dict[str, dict[str, str]] = {}  # {table_ref: {norm_name: type}}
        manifest_unique_groups: dict[str, list[list[str]]] = {}
        if manifest:
            for table in manifest.tables:
                col_map = {}
                for col in table.columns:
                    col_map[self._normalizer.normalize(col.column_name)] = col.column_type
                manifest_cols[table.table_ref] = col_map
                manifest_unique_groups[table.table_ref] = [
                    [self._normalizer.normalize(k) for k in key_group]
                    for key_group in (table.unique_keys or [])
                ]

        for jd in cs.joins:
            left_norm = self._normalizer.normalize(jd.left_key)
            right_norm = self._normalizer.normalize(jd.right_key)

            # ── 解析左侧 schema：从 step_schemas 中找到 left_table 对应的 schema ──
            left_schema: StepOutputSchema | None = step_schemas.get(jd.left_table)

            # ── 校验 a：左键在左侧 schema 中存在 ──
            if left_schema is not None and left_norm not in left_schema.columns:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "left_key_missing", jd.left_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.left_key",
                    description=(
                        f"Join 左键 '{jd.left_key}' 不在上游步骤 "
                        f"'{jd.left_table}' 的输出中。"
                        f"可用列：{sorted(left_schema.columns.keys())}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 a：右键在 manifest 中存在 ──
            right_cols = manifest_cols.get(jd.right_table, {}) if manifest else {}
            if manifest and jd.right_table in manifest_cols and right_norm not in right_cols:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "right_key_missing", jd.right_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.right_key",
                    description=(
                        f"Join 右键 '{jd.right_key}' 不在表 "
                        f"'{jd.right_table}' 中。"
                        f"已声明列：{sorted(right_cols.keys())}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 b：类型兼容——左侧类型来自 schema，右侧来自 manifest ──
            left_type: str | None = None
            if left_schema is not None:
                left_type = left_schema.columns.get(left_norm)
            right_type: str | None = None
            if right_cols:
                right_type = right_cols.get(right_norm)

            # UNKNOWN 类型阻断
            if left_type is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "left_type_unknown", jd.left_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.left_key",
                    description=(
                        f"Join 左键 '{jd.left_key}' 类型为 UNKNOWN——"
                        f"无法从源表或上游推导类型，禁止参与 Join"
                    ),
                    blocking=True,
                ))
                continue

            if right_type is None:
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "right_type_unknown", jd.right_key),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins.right_key",
                    description=(
                        f"Join 右键 '{jd.right_table}.{jd.right_key}' 类型为 UNKNOWN——"
                        f"无法从 SourceManifest 推导类型，禁止参与 Join"
                    ),
                    blocking=True,
                ))
                continue

            if not _types_compatible(left_type, right_type):
                errors.append(OpenQuestion(
                    question_id=self._qid(cs.step_name, "type_incompat",
                                          f"{jd.left_key}:{jd.right_key}"),
                    source="compute_step_validator",
                    field_ref=f"compute_steps.{cs.step_name}.joins",
                    description=(
                        f"Join 键类型不兼容——"
                        f"左键 '{jd.left_key}' 类型={left_type}，"
                        f"右键 '{jd.right_table}.{jd.right_key}' 类型={right_type}"
                    ),
                    blocking=True,
                ))
                continue

            # ── 校验 c：基数安全——单列 Join 仅由完全匹配的单列唯一键组放行 ──
            # 右侧唯一性来自 manifest
            right_key_groups = manifest_unique_groups.get(jd.right_table, [])
            right_key_unique = any(
                len(key_group) == 1 and key_group[0] == right_norm
                for key_group in right_key_groups
            )

            if not right_key_unique:
                declared_keys = "; ".join(
                    ",".join(g) for g in right_key_groups
                ) if right_key_groups else "(无)"
                # 同时检查右侧来源如果是上游步骤，其 unique_keys 是否覆盖此键
                right_source_schema = step_schemas.get(jd.right_table)
                if right_source_schema:
                    right_key_unique = any(
                        len(key_group) == 1 and key_group[0] == right_norm
                        for key_group in right_source_schema.unique_keys
                    )

                if not right_key_unique:
                    errors.append(OpenQuestion(
                        question_id=self._qid(cs.step_name, "cardinality", jd.right_key),
                        source="compute_step_validator",
                        field_ref=f"compute_steps.{cs.step_name}.joins",
                        description=(
                            f"Join 右表 '{jd.right_table}' 的键 "
                            f"'{jd.right_key}' 无单列唯一键保证——"
                            f"右表已声明唯一键组：[{declared_keys}]，"
                            f"禁止拆散复合唯一键。"
                        ),
                        blocking=True,
                    ))

    def compute_output_schema(
        self,
        cs: ComputeStep,
        step_schemas: dict[str, StepOutputSchema],
        manifest: SourceManifest | None,
    ) -> StepOutputSchema:
        """校验通过后计算此步骤的 StepOutputSchema——供下游步骤校验使用。"""
        from tianshu_datadev.planning.step_output_schema import compute_output_schema

        return compute_output_schema(
            cs, step_schemas, manifest, self._normalizer,
        )
