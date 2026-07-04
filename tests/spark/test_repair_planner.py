"""Phase 7C RepairPlanner 测试——故障分类 + 路由验证 + 返工上限。

覆盖：
- RepairAction 5 种分类模型结构
- RepairPlanner 从 PhysicalVerificationReport 推断修复动作
- 路由规则：MAPPER_BUG → mapper.py，BUSINESS_SEMANTIC → HUMAN_REVIEW
- 返工上限：>= MAX_RETRY 时强制 HUMAN_REVIEW
"""

from __future__ import annotations

from tianshu_datadev.spark.physical_verifier import (
    DiffDetail,
    EngineExecutionResult,
    PhysicalVerificationReport,
    PhysicalVerificationStatus,
)
from tianshu_datadev.spark.repair_planner import RepairAction, RepairActionType, RepairPlanner

# ════════════════════════════════════════════
# RepairAction 模型结构测试
# ════════════════════════════════════════════


class TestRepairAction:
    """RepairAction 模型结构 + RepairActionType 枚举测试。"""

    def test_all_five_action_types_exist(self):
        """5 种 RepairActionType 全部存在。"""
        expected = {
            "MAPPER_BUG",
            "COMPILER_BUG",
            "VALIDATOR_GAP",
            "SNAPSHOT_ISSUE",
            "BUSINESS_SEMANTIC",
        }
        actual = {t.value for t in RepairActionType}
        assert actual == expected

    def test_repair_action_creation(self):
        """RepairAction 基本构造——所有字段可设置。"""
        action = RepairAction(
            action_type=RepairActionType.MAPPER_BUG,
            description="窗口函数 input_column 未传递——mapper 断层",
            target_file="mapper.py",
            suggested_fix="在 _map_windows() 中补全 input_column 传递",
            retry_count=1,
        )
        assert action.action_type == RepairActionType.MAPPER_BUG
        assert action.target_file == "mapper.py"
        assert action.retry_count == 1

    def test_repair_action_default_retry_zero(self):
        """默认 retry_count 为 0。"""
        action = RepairAction(
            action_type=RepairActionType.COMPILER_BUG,
            description="编译错误",
            target_file="compiler.py",
            suggested_fix="修正渲染逻辑",
        )
        assert action.retry_count == 0


# ════════════════════════════════════════════
# RepairPlanner 分类测试
# ════════════════════════════════════════════


class TestRepairPlannerClassification:
    """RepairPlanner 从 PhysicalVerificationReport 推断 RepairAction 类型。"""

    def test_unsupported_semantics_maps_to_business_semantic(self):
        """UNSUPPORTED_SEMANTICS → BUSINESS_SEMANTIC（人工判断）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_001",
            contract_hash="hash_abc",
            snapshot_id="snap_001",
            status=PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS,
            uncovered_step_types=["subquery"],
            error_message="不支持的 step 类型：['subquery']",
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.BUSINESS_SEMANTIC
        assert action.target_file == "HUMAN_REVIEW"
        assert "subquery" in action.description.lower()

    def test_result_mismatch_with_schema_match_maps_to_compiler_bug(self):
        """结果不一致但 schema 匹配 → COMPILER_BUG（编译逻辑错误）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_002",
            contract_hash="hash_abc",
            snapshot_id="snap_002",
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=True, raw_row_count=3,
            ),
            diffs=[
                DiffDetail(
                    row_index=0, column="amount",
                    duckdb_value="100", spark_value="999",
                    description="值不一致",
                ),
            ],
            row_count_match=True,
            schema_match=True,
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.COMPILER_BUG
        assert "compiler" in action.target_file.lower()

    def test_result_mismatch_without_schema_match_maps_to_mapper_bug(self):
        """结果不一致且 schema 不匹配 → MAPPER_BUG（映射阶段列信息丢失）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_003",
            contract_hash="hash_abc",
            snapshot_id="snap_003",
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=True, raw_row_count=2,
            ),
            diffs=[
                DiffDetail(
                    row_index=2, column="(整行)",
                    duckdb_value="{'amount': 150}",
                    spark_value="(缺失)",
                    description="Spark 侧缺少第 3 行",
                ),
            ],
            row_count_match=False,
            schema_match=False,
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.MAPPER_BUG
        assert "mapper" in action.target_file.lower()

    def test_execution_error_duckdb_maps_to_snapshot_issue(self):
        """DuckDB 执行失败 → SNAPSHOT_ISSUE（快照数据问题）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_004",
            contract_hash="hash_abc",
            snapshot_id="snap_004",
            status=PhysicalVerificationStatus.EXECUTION_ERROR,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=False,
                error_message="DuckDB 执行失败：IOError: Parquet file corrupted",
            ),
            error_message="DuckDB 执行失败：IOError: Parquet file corrupted",
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.SNAPSHOT_ISSUE
        assert "snapshot" in action.target_file.lower()

    def test_execution_error_spark_maps_to_compiler_bug(self):
        """仅 Spark 执行失败 → COMPILER_BUG（编译产物有误）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_005",
            contract_hash="hash_abc",
            snapshot_id="snap_005",
            status=PhysicalVerificationStatus.EXECUTION_ERROR,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=False,
                error_message="NameError: name 'F' is not defined",
            ),
            error_message="Spark 执行失败：NameError: name 'F' is not defined",
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.COMPILER_BUG
        assert "compiler" in action.target_file.lower()

    def test_canonicalization_needed_maps_to_validator_gap(self):
        """缺少排序键 → VALIDATOR_GAP（上游应校验排序键存在性）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_006",
            contract_hash="hash_abc",
            snapshot_id="snap_006",
            status=PhysicalVerificationStatus.CANONICALIZATION_NEEDED,
            error_message="缺少排序键（order_keys）且 deduplicate=False",
        )
        action = planner.plan(report)
        assert action.action_type == RepairActionType.VALIDATOR_GAP
        assert "validator" in action.target_file.lower()


# ════════════════════════════════════════════
# RepairPlanner 路由测试
# ════════════════════════════════════════════


class TestRepairPlannerRouting:
    """RepairPlanner 路由目标验证——BUSINESS_SEMANTIC → HUMAN_REVIEW，MAPPER_BUG → mapper.py。"""

    def test_business_semantic_routes_to_human_review(self):
        """BUSINESS_SEMANTIC → HUMAN_REVIEW——不自动修改文件。"""
        planner = RepairPlanner()
        action = RepairAction(
            action_type=RepairActionType.BUSINESS_SEMANTIC,
            description="窗口语义歧义——无法自动判定",
            target_file="HUMAN_REVIEW",
            suggested_fix="需数据工程师确认窗口函数的业务含义",
        )
        result = planner.route(action)
        assert result == "HUMAN_REVIEW"
        assert "mapper" not in result.lower()
        assert "compiler" not in result.lower()

    def test_mapper_bug_routes_to_mapper(self):
        """MAPPER_BUG → mapper.py——路由回 mapper 修复。"""
        planner = RepairPlanner()
        action = RepairAction(
            action_type=RepairActionType.MAPPER_BUG,
            description="input_column 映射缺失",
            target_file="mapper.py",
            suggested_fix="补全 _map_windows 的 input_column 传递",
        )
        result = planner.route(action)
        assert result == "mapper.py"

    def test_compiler_bug_routes_to_compiler(self):
        """COMPILER_BUG → compiler.py——路由回 compiler 修复。"""
        planner = RepairPlanner()
        action = RepairAction(
            action_type=RepairActionType.COMPILER_BUG,
            description="窗口帧边界渲染错误",
            target_file="compiler.py",
            suggested_fix="修正 render_frame_boundary 映射",
        )
        result = planner.route(action)
        assert result == "compiler.py"

    def test_validator_gap_routes_to_validator(self):
        """VALIDATOR_GAP → validator.py——路由回 validator 补校验。"""
        planner = RepairPlanner()
        action = RepairAction(
            action_type=RepairActionType.VALIDATOR_GAP,
            description="缺少排序键校验",
            target_file="validator.py",
            suggested_fix="增加排序键存在性检查",
        )
        result = planner.route(action)
        assert result == "validator.py"

    def test_snapshot_issue_routes_to_snapshot(self):
        """SNAPSHOT_ISSUE → snapshot.py——路由回 snapshot 重建。"""
        planner = RepairPlanner()
        action = RepairAction(
            action_type=RepairActionType.SNAPSHOT_ISSUE,
            description="快照 Parquet 文件损坏",
            target_file="snapshot.py",
            suggested_fix="重新生成快照",
        )
        result = planner.route(action)
        assert result == "snapshot.py"


# ════════════════════════════════════════════
# RepairPlanner 返工上限测试
# ════════════════════════════════════════════


class TestRepairPlannerRetryLimit:
    """返工最多 2 轮——>= MAX_RETRY 时强制 HUMAN_REVIEW。"""

    def test_retry_under_limit_proceeds_normally(self):
        """retry_count < MAX_RETRY → 正常分类。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_007",
            contract_hash="hash_abc",
            snapshot_id="snap_007",
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=True, raw_row_count=3,
            ),
            diffs=[],
            row_count_match=True,
            schema_match=True,
        )
        action = planner.plan(report, retry_count=0)
        # retry_count=0 < 2 → 允许自动返工
        assert action.action_type != RepairActionType.BUSINESS_SEMANTIC

    def test_retry_at_limit_forces_human_review(self):
        """retry_count >= MAX_RETRY → BUSINESS_SEMANTIC（强制人工介入）。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_008",
            contract_hash="hash_abc",
            snapshot_id="snap_008",
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=True, raw_row_count=3,
            ),
            diffs=[],
            row_count_match=True,
            schema_match=True,
        )
        action = planner.plan(report, retry_count=2)
        assert action.action_type == RepairActionType.BUSINESS_SEMANTIC
        assert action.target_file == "HUMAN_REVIEW"
        assert "2 轮" in action.description or "返工" in action.description

    def test_retry_exceeds_limit_forces_human_review(self):
        """retry_count > MAX_RETRY → BUSINESS_SEMANTIC。"""
        planner = RepairPlanner()
        report = PhysicalVerificationReport(
            report_id="physver_test_009",
            contract_hash="hash_abc",
            snapshot_id="snap_009",
            status=PhysicalVerificationStatus.RESULT_MISMATCH,
            duckdb_result=EngineExecutionResult(
                engine="duckdb", success=True, raw_row_count=3,
            ),
            spark_result=EngineExecutionResult(
                engine="spark", success=True, raw_row_count=3,
            ),
            diffs=[],
            row_count_match=True,
            schema_match=True,
        )
        action = planner.plan(report, retry_count=3)
        assert action.action_type == RepairActionType.BUSINESS_SEMANTIC
        assert action.target_file == "HUMAN_REVIEW"
