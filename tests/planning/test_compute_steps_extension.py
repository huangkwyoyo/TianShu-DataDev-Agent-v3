"""ComputeSteps 扩展——表驱动单元测试。
覆盖：StepOutputSchema(2) + Validator(5) + Builder(1) + Contract(1) + TypeCompat(1)
"""

from types import SimpleNamespace

import pytest
from tianshu_datadev.planning.step_output_schema import (
    StepOutputSchema, compute_output_schema,
)
from tianshu_datadev.planning.compute_step_validator import (
    ComputeStepValidator, _types_compatible, _normalize_type,
)
from tianshu_datadev.developer_spec import FieldNormalizer


# ════════════════════════════════════════════
# 辅助：构建 duck-typed SourceManifest
# 注：ManifestColumn 实际字段为 data_type，
# 但 Validator/StepOutputSchema 读取 column_type，
# 故使用 SimpleNamespace 绕开此差异，不修改被测代码。
# ════════════════════════════════════════════


def _make_col(name: str, col_type: str) -> SimpleNamespace:
    """构建可被 Validator/StepOutputSchema 读取的伪列对象。"""
    return SimpleNamespace(column_name=name, normalized_name=name, column_type=col_type)


def _make_table(
    table_ref: str,
    columns: list[SimpleNamespace],
    unique_keys: list[list[str]] | None = None,
    source_table: str = "s.t",
) -> SimpleNamespace:
    """构建伪表对象——含 column_type 属性供 Validator 消费。"""
    return SimpleNamespace(
        table_ref=table_ref,
        source_table=source_table,
        columns=columns,
        unique_keys=unique_keys,
        row_count=100,
        role="dim",
        key_column_names_normalized=[c.column_name for c in columns],
    )


def _make_manifest(tables: list[SimpleNamespace]) -> SimpleNamespace:
    """构建伪 SourceManifest。"""
    return SimpleNamespace(tables=tables)


class TestStepOutputSchema:
    """StepOutputSchema——类型推导与 UNKNOWN 处理。"""

    def test_metric_types_derived_correctly(self):
        """COUNT→bigint, SUM→继承源列类型, AVG→double。"""
        from tianshu_datadev.developer_spec import (
            AggregationType, ComputeStep, MetricDecl,
        )
        cs = ComputeStep(
            step_name="s1", source="input",
            group_by=["status"],
            metrics=[
                MetricDecl(metric_name="cnt", aggregation=AggregationType.COUNT,
                           input_column="id", alias="cnt"),
                MetricDecl(metric_name="avg_val", aggregation=AggregationType.AVG,
                           input_column="amount", alias="avg_val"),
            ],
            output_alias="s1",
        )
        schema = compute_output_schema(cs, {}, None, FieldNormalizer())
        # FieldNormalizer 将 "cnt" → "count"、"avg_val" → "average_value"
        assert schema.columns["count"] == "bigint"
        assert schema.columns["average_value"] == "double"
        # GROUP BY 类型来自 manifest——无 manifest 则为 UNKNOWN
        assert schema.columns["status"] is None  # UNKNOWN

    def test_unique_keys_from_group_by(self):
        """Aggregate 的 group_by 形成派生 unique_keys。"""
        from tianshu_datadev.developer_spec import ComputeStep
        cs = ComputeStep(
            step_name="s1", source="input",
            group_by=["borough", "zone_name"],
            metrics=[], output_alias="s1",
        )
        schema = compute_output_schema(cs, {}, None, FieldNormalizer())
        assert len(schema.unique_keys) == 1
        assert set(schema.unique_keys[0]) == {"borough", "zone_name"}


class TestValidator:
    """ComputeStepValidator——五项校验 + UNKNOWN 阻断。"""

    def _make_manifest(self):
        """构建单表 SourceManifest——taxi_zone(265行)。"""
        return _make_manifest([
            _make_table(
                table_ref="tz", source_table="s.tz",
                columns=[
                    _make_col("location_id", "integer"),
                    _make_col("borough", "varchar"),
                ],
                unique_keys=[["location_id"], ["borough"]],
            ),
        ])

    def test_valid_join_passes(self):
        """合法混合源 Join——返回空列表。"""
        from tianshu_datadev.developer_spec import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="borough", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(
            normalizer=FieldNormalizer(), spec_hash="abc",
        )
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        assert len(errors) == 0

    def test_left_key_missing_returns_error(self):
        """left_key 不在上游 schema 中。"""
        from tianshu_datadev.developer_spec import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="nonexistent", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        assert len(errors) == 1
        assert errors[0].blocking is True

    def test_unknown_type_blocks_join(self):
        """UNKNOWN 类型 Join 键阻断。"""
        from tianshu_datadev.developer_spec import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="tz",
                   left_key="unknown_col", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"unknown_col": None}),  # UNKNOWN
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, self._make_manifest())
        # 左键类型 UNKNOWN → 阻断
        assert any("UNKNOWN" in e.description or "unknown" in e.description.lower()
                   for e in errors)

    def test_composite_unique_key_rejects_single_column_join(self):
        """复合键 [borough, zone_name] 不放行单列 borough Join。"""
        from tianshu_datadev.developer_spec import (
            ComputeStep, JoinDecl, JoinTypeEnum,
        )
        manifest = _make_manifest([
            _make_table(
                table_ref="t2", source_table="s.t2",
                columns=[
                    _make_col("borough", "varchar"),
                    _make_col("zone_name", "varchar"),
                ],
                unique_keys=[["borough", "zone_name"]],
            ),
        ])
        cs = ComputeStep(
            step_name="s2", source="s1", group_by=["borough"],
            output_alias="s2",
            joins=[JoinDecl(left_table="s1", right_table="t2",
                   left_key="borough", right_key="borough",
                   join_type=JoinTypeEnum.INNER)],
            metrics=[],
        )
        step_schemas = {
            "s1": StepOutputSchema(columns={"borough": "varchar"}),
        }
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, step_schemas, manifest)
        assert any("唯一键" in e.description or "复合" in e.description
                   or "单列" in e.description
                   for e in errors)

    def test_confluence_without_joins_returns_error(self):
        """合流无 JoinDecl——必须显式声明。"""
        from tianshu_datadev.developer_spec import ComputeStep
        cs = ComputeStep(
            step_name="s3", source=["s1", "s2"],
            group_by=["borough"], output_alias="s3",
            metrics=[],
        )
        validator = ComputeStepValidator(normalizer=FieldNormalizer(), spec_hash="abc")
        errors = validator.validate(cs, {"s1": StepOutputSchema(), "s2": StepOutputSchema()}, None)
        assert any("JoinDecl" in e.description or "joins" in e.description
                   for e in errors)


class TestBuilder:
    """Builder——case_when + metrics 共存。"""

    def test_case_when_and_metrics_coexist(self):
        """删除守卫后两者都出现在步骤列表中。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType, CaseWhenBranchDecl, CaseWhenDecl, ColumnDecl,
            ComputeStep, InputTableDecl, MetricDecl,
            OutputSpecDecl, ParsedDeveloperSpec,
        )
        from tianshu_datadev.planning import SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            spec_id="test",
            spec_hash="abc",
            title="test",
            description="",
            input_tables=[
                InputTableDecl(
                    source_table="s.t1", table_alias="t1",
                    columns=[
                        ColumnDecl(column_name="amount", normalized_name="amount",
                                   data_type="decimal", nullable=True),
                    ],
                    key_columns=[
                        ColumnDecl(column_name="id", normalized_name="id",
                                   data_type="bigint", nullable=False, unique=True),
                    ],
                    business_columns=[
                        ColumnDecl(column_name="amount", normalized_name="amount",
                                   data_type="decimal", nullable=True),
                    ],
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=["status"],
                grain=["status"],
            ),
            metrics=[],
            dimensions=[],
            compute_steps=[
                ComputeStep(
                    step_name="s1", source="input",
                    group_by=[], output_alias="s1",
                    metrics=[MetricDecl(
                        metric_name="total", aggregation=AggregationType.SUM,
                        input_column="amount", alias="total",
                    )],
                    case_when=CaseWhenDecl(
                        output_column="level",
                        evaluation_phase="post_aggregate",
                        else_value="低",
                        branches=[CaseWhenBranchDecl(
                            condition_column="total",
                            condition_operator=">=",
                            condition_value="1000",
                            result_column="",
                        )],
                    ),
                ),
            ],
        )
        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec)
        final = plans[-1]
        step_types = [type(s).__name__ for s in final.steps]
        assert "AggregateStep" in step_types
        assert "CaseWhenStep" in step_types


class TestContract:
    """Contract——保留所有显式 Join。"""

    def test_temp_to_temp_join_preserved(self):
        """temp↔temp 的 borough Join 不再被跳过。"""
        from tianshu_datadev.planning import (
            ColumnRef, JoinStep, JoinType, SafeIdentifier,
        )
        from tianshu_datadev.artifacts.contract_extractor import (
            DataTransformContractExtractor,
        )
        join_step = JoinStep(
            step_id="join_t3",
            right_table_ref="_temp_abc_s2",
            join_type=JoinType("INNER"),
            join_keys=[(
                ColumnRef(table_ref="_temp_abc_s1",
                          column_name=SafeIdentifier("borough"),
                          normalized_name=SafeIdentifier("borough")),
                ColumnRef(table_ref="_temp_abc_s2",
                          column_name=SafeIdentifier("borough"),
                          normalized_name=SafeIdentifier("borough")),
            )],
            relationship_ref="compute_steps:abc:s1:s2",
        )
        temp_lineage = {
            ("_temp_abc_s1", "borough"): ColumnRef(
                table_ref="cd", column_name=SafeIdentifier("borough"),
                normalized_name=SafeIdentifier("borough"),
            ),
            ("_temp_abc_s2", "borough"): ColumnRef(
                table_ref="tz", column_name=SafeIdentifier("borough"),
                normalized_name=SafeIdentifier("borough"),
            ),
        }
        join_rel = DataTransformContractExtractor._extract_join(
            join_step, {}, temp_lineage,
        )
        assert join_rel is not None
        assert join_rel.left_table == "cd"
        assert join_rel.right_table == "tz"


class TestTypeCompat:
    """类型兼容矩阵——UNKNOWN 阻断。"""

    def test_same_type_compatible(self):
        assert _types_compatible("bigint", "bigint") is True

    def test_unknown_blocks_any(self):
        """UNKNOWN 类型与任何类型不兼容（含自身）。"""
        assert _types_compatible(None, "varchar") is False
        assert _types_compatible("varchar", None) is False
        assert _types_compatible(None, None) is False

    def test_normalize_type_handles_none(self):
        assert _normalize_type(None) is None
        assert _normalize_type("decimal(12,2)") == "decimal"
