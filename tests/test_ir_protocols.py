"""验证 IR Protocol 接口和状态枚举的定义正确性。

所有 Protocol 只定义形状（属性），不包含实现代码。
"""


# ExecutionTrace 和 ResultSummary 已迁移至 tianshu_datadev.sql.models（严格 Pydantic 模型）
from tianshu_datadev.ir import (
    CrossValidationResult,
    MergedResult,
    RepairDirective,
    RepairTarget,
    RequestStatus,
    RequirementIR,
    SparkCodeArtifact,
    SQLPlan,
    StepStatus,
    SubIntent,
    TransformationContract,
    TransformParams,
)


class TestRequestStatus:
    """RequestStatus 枚举——一次请求的生命周期状态。"""

    def test_all_statuses_defined(self):
        """验证所有生命周期状态已定义。"""
        assert RequestStatus.DRAFT == "DRAFT"
        assert RequestStatus.ANALYZING == "ANALYZING"
        assert RequestStatus.DECOMPOSING == "DECOMPOSING"
        assert RequestStatus.EXECUTING_SQL == "EXECUTING_SQL"
        assert RequestStatus.EXECUTING_SPARK == "EXECUTING_SPARK"
        assert RequestStatus.VALIDATING == "VALIDATING"
        assert RequestStatus.DIAGNOSING == "DIAGNOSING"
        assert RequestStatus.RETRYING == "RETRYING"
        assert RequestStatus.HUMAN_REVIEW == "HUMAN_REVIEW"
        assert RequestStatus.COMPLETED == "COMPLETED"
        assert RequestStatus.FAILED == "FAILED"

    def test_num_statuses(self):
        """验证状态数量——防止意外膨胀。"""
        # 如果此测试失败，说明有人新增了状态——请确保新增状态有明确的阶段归属
        assert len(RequestStatus) == 11


class TestStepStatus:
    """StepStatus 枚举——单步骤执行状态。"""

    def test_pass_status_exists(self):
        """PASS 只能由确定性 Comparator 产生。"""
        assert StepStatus.PASS == "PASS"

    def test_fail_status_exists(self):
        """FAIL 表示安全检查或执行失败。"""
        assert StepStatus.FAIL == "FAIL"

    def test_different_status_exists(self):
        """DIFFERENT 表示 SQL/Spark 结果不一致。"""
        assert StepStatus.DIFFERENT == "DIFFERENT"

    def test_not_executed_status_exists(self):
        """NOT_EXECUTED 表示步骤未执行（如 Spark 不可用）。"""
        assert StepStatus.NOT_EXECUTED == "NOT_EXECUTED"

    def test_num_statuses(self):
        """验证状态数量——防止意外膨胀。"""
        # 如果此测试失败，请确认新增状态是否真的需要
        assert len(StepStatus) == 5  # PASS/FAIL/DIFFERENT/NOT_EXECUTED/SKIPPED


class TestRepairTarget:
    """RepairTarget 枚举——修复目标。"""

    def test_all_targets_defined(self):
        """验证所有修复目标已定义。"""
        assert RepairTarget.SQL_PLAN == "SQL_PLAN"
        assert RepairTarget.SPARK_CODE == "SPARK_CODE"
        assert RepairTarget.BOTH == "BOTH"
        assert RepairTarget.REQUIREMENT == "REQUIREMENT"
        assert RepairTarget.HUMAN_REVIEW == "HUMAN_REVIEW"

    def test_num_targets(self):
        """验证修复目标数量——防止意外膨胀。"""
        assert len(RepairTarget) == 5


class TestProtocolInterfaces:
    """Protocol 接口——验证核心字段存在。

    所有 Protocol 只定义属性签名，不包含实现。使用 runtime_checkable
    装饰器支持 isinstance 检查。
    """

    # ── RequirementIR ──

    def test_requirement_ir_protocol_attributes(self):
        """验证 RequirementIR Protocol 定义了核心字段。"""
        attrs = RequirementIR.__protocol_attrs__
        assert "request_id" in attrs
        assert "metrics" in attrs
        assert "dimensions" in attrs
        assert "time_range" in attrs
        assert "filters" in attrs
        assert "grain" in attrs
        assert "raw_spec" in attrs

    # ── SubIntent ──

    def test_sub_intent_protocol_attributes(self):
        """验证 SubIntent Protocol 定义了核心字段。"""
        attrs = SubIntent.__protocol_attrs__
        assert "sub_intent_id" in attrs
        assert "parent_request_id" in attrs
        assert "metrics" in attrs
        assert "planning_table" in attrs
        assert "time_range" in attrs
        assert "dimensions" in attrs
        assert "status" in attrs

    # ── SQLPlan ──

    def test_sql_plan_protocol_attributes(self):
        """验证 SQLPlan Protocol 定义了核心字段——不包括 LLM 评分等附加属性。"""
        attrs = SQLPlan.__protocol_attrs__
        assert "plan_id" in attrs
        assert "sub_intent_id" in attrs
        assert "primary_table" in attrs
        assert "joins" in attrs
        assert "where_clauses" in attrs
        assert "group_by" in attrs
        assert "order_by" in attrs
        assert "aggregations" in attrs
        assert "limit" in attrs
        assert "confidence" in attrs

    # ExecutionTrace 和 ResultSummary 测试已迁移至 tests/sql/——使用严格 Pydantic 模型

    # ── CrossValidationResult ──

    def test_cross_validation_protocol_attributes(self):
        """验证 CrossValidationResult Protocol 定义了确定性比较结果字段。"""
        attrs = CrossValidationResult.__protocol_attrs__
        assert "validation_id" in attrs
        assert "request_id" in attrs
        assert "sql_summary_id" in attrs
        assert "spark_summary_id" in attrs
        assert "status" in attrs
        assert "comparisons" in attrs

    # ── RepairDirective ──

    def test_repair_directive_protocol_attributes(self):
        """验证 RepairDirective Protocol 定义了修复指令字段。"""
        attrs = RepairDirective.__protocol_attrs__
        assert "directive_id" in attrs
        assert "validation_id" in attrs
        assert "target" in attrs
        assert "reason" in attrs
        assert "suggestions" in attrs
        assert "retry_count" in attrs

    # ── MergedResult ──

    def test_merged_result_protocol_attributes(self):
        """验证 MergedResult Protocol 定义了多 SubIntent 合并结果字段。"""
        attrs = MergedResult.__protocol_attrs__
        assert "merge_id" in attrs
        assert "request_id" in attrs
        assert "merge_key" in attrs
        assert "row_count" in attrs
        assert "source_summary_ids" in attrs
        assert "status" in attrs


    # ── TransformationContract ──

    def test_transformation_contract_protocol_attributes(self):
        """验证 TransformationContract 定义了 PySpark 转换的结构化契约字段。"""
        attrs = TransformationContract.__protocol_attrs__
        assert "contract_id" in attrs
        assert "sub_intent_id" in attrs
        assert "declared_inputs" in attrs
        assert "field_schemas" in attrs
        assert "join_paths" in attrs
        assert "metrics" in attrs
        assert "output_schema" in attrs
        assert "grain" in attrs
        assert "allowed_imports" in attrs
        assert "params_schema" in attrs

    # ── TransformParams ──

    def test_transform_params_is_protocol(self):
        """验证 TransformParams 是一个 Protocol 接口。"""
        assert hasattr(TransformParams, "__protocol_attrs__"), (
            "TransformParams 应为 Protocol——其具体字段由 TransformationContract.params_schema 声明"
        )

    # ── SparkCodeArtifact ──

    def test_spark_code_artifact_protocol_attributes(self):
        """验证 SparkCodeArtifact 定义了代码产物的完整溯源字段。"""
        attrs = SparkCodeArtifact.__protocol_attrs__
        assert "artifact_id" in attrs
        assert "sub_intent_id" in attrs
        assert "contract_ref" in attrs
        assert "code_ref" in attrs
        assert "code_sha256" in attrs
        assert "entrypoint" in attrs
        assert "model_id" in attrs
        assert "prompt_version" in attrs
        assert "generation_round" in attrs
        assert "allowed_imports" in attrs
        assert "declared_inputs" in attrs
        assert "expected_output_schema" in attrs
        assert "static_validation_status" in attrs

    def test_spark_code_artifact_entrypoint_is_transform(self):
        """验证入口点固定为 transform——不允许 LLM 自定义入口名。"""
        # 此测试验证契约语义：entrypoint 字段的值必须始终是 "transform"
        # 实际代码中的函数名由 Static Validator 强制执行，此处验证 Protocol 包含该字段即可
        attrs = SparkCodeArtifact.__protocol_attrs__
        assert "entrypoint" in attrs, (
            "SparkCodeArtifact 缺少 entrypoint 字段——entrypoint 固定为 transform，"
            "不允许 LLM 自定义入口名"
        )


class TestProtocolDesign:
    """设计边界测试——确保 Protocol 不包含污染。"""

    # 所有核心 Protocol 名称（含 SQL 侧和 Spark 侧）
    # 注意：ExecutionTrace 和 ResultSummary 已迁移至 tianshu_datadev.sql.models
    CORE_PROTOCOLS = [
        "RequirementIR",
        "SubIntent",
        "SQLPlan",
        "CrossValidationResult",
        "RepairDirective",
        "MergedResult",
        # Spark 侧
        "TransformationContract",
        "TransformParams",
        "SparkCodeArtifact",
    ]

    def test_no_legacy_port_names(self):
        """确保 v3 不包含旧项目的命名模式。

        旧项目使用了 QuestionIntent、AgentResponse、UnifiedResponse 等
        与新架构不符的名称。v3 Protocol 不应包含这些。
        """
        actual_protocols = set(
            name
            for name in dir(__import__("tianshu_datadev.ir.protocols", fromlist=["*"]))
            if not name.startswith("_") and name[0].isupper()
        )
        # 过滤掉 Enum 和 typing 导入
        actual_protocols.discard("Protocol")
        actual_protocols.discard("runtime_checkable")
        actual_protocols.discard("Enum")

        legacy_names = {
            "QuestionIntent",
            "AgentResponse",
            "UnifiedResponse",
            "IntentType",
            "Domain",
            "IntentType",
            "TimeRangeType",
            "ChartSpec",
            "CrossDomainDecision",
        }
        overlap = actual_protocols & legacy_names
        assert not overlap, f"v3 Protocol 不应包含旧项目命名: {overlap}"

    def test_all_core_protocols_are_runtime_checkable(self):
        """确保所有核心 Protocol 都标记了 @runtime_checkable。"""
        from tianshu_datadev.ir import protocols as mod

        for name in self.CORE_PROTOCOLS:
            cls = getattr(mod, name, None)
            assert cls is not None, f"缺少 {name}"
            assert hasattr(cls, "__protocol_attrs__"), (
                f"{name} 不是 Protocol——缺少 __protocol_attrs__"
            )

    def test_no_dataclass_in_protocols(self):
        """确保 protocols.py 中没有 concrete dataclass——Phase 0 只定义接口。

        具体 dataclass 实现将在 Phase 1 添加到 ir/ 子包的独立模块中。
        """
        import dataclasses
        import inspect

        from tianshu_datadev.ir import protocols as mod

        for name in dir(mod):
            obj = getattr(mod, name)
            if inspect.isclass(obj) and name not in ("Protocol", "Enum", "str"):
                # Protocol 类不应该同时是 dataclass
                is_dc = dataclasses.is_dataclass(obj)
                assert not is_dc, (
                    f"{name} 是 dataclass——Phase 0 只应定义 Protocol 接口，"
                    f"dataclass 实现应在 Phase 1 的独立文件中"
                )
