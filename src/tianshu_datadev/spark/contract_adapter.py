"""Contract 适配层——DataTransformContractLite → DataTransformContractV1 确定性升级。

Phase 9A3 第一优先级：收口 Lite/V1 入口适配。
Pipeline.run_all() 单表路径产出 DataTransformContractLite（extract(plan)），
而 Mapper（map_contract_to_spark_plan）仅接受 DataTransformContractV1。
本模块提供无损确定性适配——将 Lite 的所有公共字段复制到 V1，
V1 独有字段（step_dag/temp_tables/case_when_labels/window_specs/write_spec）填入合理默认值。

适配是纯函数：相同 Lite → 相同 V1 → 相同 contract_id。
"""

from __future__ import annotations

from tianshu_datadev.artifacts.models import (
    DataTransformContractLite,
    DataTransformContractV1,
)


def adapt_lite_to_v1(lite: DataTransformContractLite) -> DataTransformContractV1:
    """将 DataTransformContractLite 确定性升级为 DataTransformContractV1。

    Lite 和 V1 共享 14 个业务字段（input_tables / filters / aggregations 等）。
    适配直接复制这些字段，并为 V1 独有的 5 个字段填入安全默认值：
    - step_dag: {}（单语句无 DAG）
    - temp_tables: []（无临时表）
    - case_when_labels: []（Lite 不包含 CASE WHEN 标签）
    - window_specs: []（Lite 不包含窗口函数规格）
    - write_spec: None（无受控写入方案）

    contract_id 使用 DataTransformContractV1.generate_contract_id() 生成——
    基于 source_sqlprogram_hash（即 Lite 的 source_sqlbuildplan_hash），
    保证确定性。

    Args:
        lite: DataTransformContractLite 实例——来自 Pipeline.export_artifacts() 的
              data_transform_contract 字段。

    Returns:
        DataTransformContractV1——可直接传入 Mapper（map_contract_to_spark_plan）和
        Orchestrator（run(contract=...)）。

    不变式：
        adapt_lite_to_v1(lite).{field} == lite.{field}  # 对所有公共字段
        adapt_lite_to_v1(lite).version == "v1"
        adapt_lite_to_v1(lite).source_phase == "phase-3"
    """
    # 用 Lite 的 source_sqlbuildplan_hash 作为 V1 的 source_sqlprogram_hash
    program_id = lite.source_sqlbuildplan_hash
    contract_id = DataTransformContractV1.generate_contract_id(program_id)

    return DataTransformContractV1(
        contract_id=contract_id,
        version="v1",
        source_phase="phase-3",
        source_sqlprogram_hash=program_id,
        # ── 公共字段：直接从 Lite 复制 ──
        input_tables=lite.input_tables,
        input_columns=lite.input_columns,
        join_relationships=lite.join_relationships,
        filters=lite.filters,
        aggregations=lite.aggregations,
        grouping_keys=lite.grouping_keys,
        output_columns=lite.output_columns,
        output_grain=lite.output_grain,
        sort_spec=lite.sort_spec,
        limit_spec=lite.limit_spec,
        business_keys=lite.business_keys,
        semantic_policy_ref=lite.semantic_policy_ref,
        # ── V1 独有字段：安全默认值 ──
        step_dag={},
        temp_tables=[],
        case_when_labels=[],
        window_specs=[],
        write_spec=None,
    )
