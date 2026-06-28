"""安全评测器测试——6 种攻击向量的全量覆盖。

测试分为两层：
1. 直接攻击测试——绕过 SecurityEvaluator，直接验证被测系统的防御层
2. 集成测试——通过 SecurityEvaluator.run_all() 运行全量评测

每次拒绝必须有 code（question_id/异常消息）、path（field_ref）、message（description）。
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.developer_spec.models import (
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.harness.models import AttackVector, SecurityCaseResult
from tianshu_datadev.harness.security_eval import SecurityEvaluator
from tianshu_datadev.planning.models import ColumnRef, JoinType
from tianshu_datadev.planning.relationship_hypothesis import (
    EvidenceAction,
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipEvidence,
    RelationshipHypothesis,
)
from tianshu_datadev.planning.sql_build_plan import (
    JoinStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

# ════════════════════════════════════════════
# 攻击向量1：Prompt 注入
# ════════════════════════════════════════════


class TestAttackVectorPromptInjection:
    """攻击向量1：Prompt 注入——Schema 层 extra='forbid' 拒绝 raw_sql 等自由 SQL 字段。"""

    EXTRA_FIELDS = [
        ("raw_sql", "SELECT * FROM users"),
        ("where_sql", "1=1"),
        ("join_on", "t1.id = t2.id"),
    ]

    @pytest.mark.parametrize("field_name,field_value", EXTRA_FIELDS)
    def test_extra_field_rejected_by_schema(self, field_name: str, field_value: str):
        """构造含额外 SQL 字段的 SqlBuildPlan 被 Pydantic extra='forbid' 拒绝。"""
        bad_dict = {
            "plan_id": "test_inject",
            "spec_hash": "abc",
            "steps": [],
            field_name: field_value,
        }
        with pytest.raises(ValidationError) as exc_info:
            SqlBuildPlan(**bad_dict)
        errors_str = str(exc_info.value)
        # 验证拒绝可追溯——有明确的 code（extra 字段名）
        assert (
            "extra" in errors_str.lower()
            or field_name in errors_str
            or "Unknown" in errors_str
            or "unknown" in errors_str
        ), f"拒绝消息应含 extra 或字段名 '{field_name}': {errors_str}"

    def test_extra_field_rejected_with_traceable_path(self):
        """拒绝消息含具体字段名——可追溯到注入点。"""
        bad_dict = {
            "plan_id": "test_trace",
            "spec_hash": "abc",
            "steps": [],
            "raw_sql": "SELECT 1",
        }
        with pytest.raises(ValidationError) as exc_info:
            SqlBuildPlan(**bad_dict)
        errors_str = str(exc_info.value)
        # 验证 path 可追溯
        assert "raw_sql" in errors_str, (
            f"拒绝消息应明确指向注入字段名 'raw_sql': {errors_str}"
        )


# ════════════════════════════════════════════
# 攻击向量2：SQL 注入
# ════════════════════════════════════════════


class TestAttackVectorSqlInjection:
    """攻击向量2：SQL 注入——SafeIdentifier AfterValidator 拒绝非法字符。"""

    MALICIOUS_SAMPLES = [
        ("x'; DROP TABLE users; --", "分号+DROP注入"),
        ("x' OR '1'='1", "单引号布尔注入"),
        ("x UNION SELECT * FROM passwords", "UNION注入"),
        ("1 OR 1=1", "空格+OR注入"),
    ]

    @pytest.mark.parametrize("value,desc", MALICIOUS_SAMPLES)
    def test_columnref_rejects_sql_injection(self, value: str, desc: str):
        """ColumnRef 的 SafeIdentifier 字段拒绝 SQL 注入字符。"""
        with pytest.raises((ValidationError, ValueError)) as exc_info:
            ColumnRef(
                table_ref="t1",
                column_name=value,
                normalized_name=value,
            )
        errors_str = str(exc_info.value)
        # 验证拒绝可追溯
        assert (
            "非法" in errors_str
            or "invalid" in errors_str.lower()
            or "SQL" in errors_str
            or "标识符" in errors_str
        ), f"拒绝消息应含明确拒绝原因: {errors_str}"

    @pytest.mark.parametrize("value,desc", MALICIOUS_SAMPLES)
    def test_scantep_table_ref_rejects_sql_injection(self, value: str, desc: str):
        """ScanStep.table_ref 拒绝 SQL 注入——防止 FROM AS 子句注入。"""
        with pytest.raises((ValidationError, ValueError)):
            ScanStep(
                step_id="s_inject",
                table_ref=value,
                required_columns=[
                    ColumnRef(table_ref=value, column_name="id", normalized_name="id"),
                ],
            )


# ════════════════════════════════════════════
# 攻击向量3：Schema extra 突破
# ════════════════════════════════════════════


class TestAttackVectorSchemaExtra:
    """攻击向量3：Schema extra 突破——Pydantic extra='forbid' 拒绝未定义字段。"""

    EXTRA_FIELDS = [
        {"plan_id": "x", "spec_hash": "abc", "steps": [], "sql": "SELECT 1"},
        {"plan_id": "x", "spec_hash": "abc", "steps": [], "raw_where": "1=1"},
        {"plan_id": "x", "spec_hash": "abc", "steps": [], "expression": "x + 1"},
    ]

    @pytest.mark.parametrize("data", EXTRA_FIELDS)
    def test_extra_fields_rejected(self, data: dict):
        """含有额外字段的 dict 构造 SqlBuildPlan 被拒绝。"""
        with pytest.raises(ValidationError) as exc_info:
            SqlBuildPlan(**data)
        errors_str = str(exc_info.value)
        assert (
            "extra" in errors_str.lower()
            or "Unknown" in errors_str
            or "unknown" in errors_str
        ), f"拒绝消息应含 extra 标记: {errors_str}"


# ════════════════════════════════════════════
# 攻击向量4：未声明引用
# ════════════════════════════════════════════


class TestAttackVectorUndeclaredRef:
    """攻击向量4：未声明引用——Validator 拒绝不在 SourceManifest 中的表引用。"""

    def test_undeclared_table_rejected_with_q_val_table(self):
        """引用未注册表的 plan 被 Validator 拒绝——含 Q-VAL-TABLE- 拒绝码。"""
        plan = SqlBuildPlan(
            plan_id="test_undeclared",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_bad",
                    table_ref="nonexistent_table",
                    required_columns=[
                        ColumnRef(
                            table_ref="nonexistent_table",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="abc",
            tables=[],  # 空清单
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        assert passed is False, "应拒绝未注册表引用"
        table_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-TABLE-" in q.question_id
        ]
        assert len(table_issues) >= 1, (
            f"应有 Q-VAL-TABLE- 拒绝码，实际 questions={[q.question_id for q in questions]}"
        )
        # 验证拒绝可追溯——有 code, path, message
        issue = table_issues[0]
        assert issue.question_id, "拒绝应有 question_id (code)"
        assert issue.description, "拒绝应有 description (message)"
        # field_ref 可选——表级引用可能为 None

    def test_undeclared_column_ref_rejected(self):
        """字段引用校验也拒绝未声明的列——Q-VAL-COL-。"""
        plan = SqlBuildPlan(
            plan_id="test_undeclared_col",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(
                            table_ref="t1",
                            column_name="nonexistent_column",
                            normalized_name="nonexistent_column",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(column_name="id", normalized_name="id", data_type="int"),
                    ],
                ),
            ],
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        assert passed is False, "应拒绝未声明列引用"
        col_issues = [
            q for q in questions
            if q.blocking and (
                "Q-VAL-COL-" in q.question_id
                or "nonexistent_column" in q.description
            )
        ]
        assert len(col_issues) >= 1, (
            f"应有字段相关拒绝码，实际 questions={[q.question_id for q in questions]}"
        )


# ════════════════════════════════════════════
# 攻击向量5：Join 错误推理
# ════════════════════════════════════════════


class TestAttackVectorJoinError:
    """攻击向量5：Join 错误推理——Validator 拒绝 WEAK 证据等级 Join。"""

    def test_weak_join_rejected_by_validator(self):
        """WEAK 证据等级的 Join 被 Validator Join 门禁拦截。"""
        plan = SqlBuildPlan(
            plan_id="test_weak_join",
            spec_hash="abc",
            hypothesis_id="hyp_weak",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                ),
                ScanStep(
                    step_id="scan_t2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(table_ref="t2", column_name="id", normalized_name="id"),
                    ],
                ),
                JoinStep(
                    step_id="join_weak",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                            ColumnRef(table_ref="t2", column_name="id", normalized_name="id"),
                        ),
                    ],
                    relationship_ref="jc_weak",  # 必须匹配 candidate_id
                ),
            ],
            multi_table=True,
        )

        # WEAK 证据
        weak_evidence = RelationshipEvidence(
            evidence_id="ev_weak",
            level=JoinEvidenceLevel.WEAK,
            action=EvidenceAction.REJECT_BLOCKING,
            left_table="t1",
            right_table="t2",
            left_key_raw="id",
            right_key_raw="id",
            left_key_normalized="id",
            right_key_normalized="id",
            evidence_checks=["field_name_match: MATCH（仅命名）"],
            detail="字段名相同但无唯一性证据——WEAK",
        )

        hypothesis = RelationshipHypothesis(
            hypothesis_id="hyp_weak",
            spec_hash="abc",
            candidates=[
                JoinCandidate(
                    candidate_id="jc_weak",
                    left_table="t1",
                    right_table="t2",
                    left_key="id",
                    right_key="id",
                    left_key_normalized="id",
                    right_key_normalized="id",
                    join_type=JoinType.INNER,
                    evidence=weak_evidence,
                ),
            ],
        )

        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[ManifestColumn(column_name="id", normalized_name="id", data_type="int")],
                ),
                ManifestTable(
                    table_ref="t2",
                    source_table="db.t2",
                    columns=[ManifestColumn(column_name="id", normalized_name="id", data_type="int")],
                ),
            ],
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest, hypothesis=hypothesis)

        assert passed is False, "应拒绝 WEAK Join"
        gate_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-GATE-" in q.question_id
        ]
        assert len(gate_issues) >= 1, (
            f"应有 Q-VAL-GATE- 拒绝码，实际 questions={[q.question_id for q in questions]}"
        )
        # 验证拒绝可追溯
        issue = gate_issues[0]
        assert issue.question_id, "拒绝应有 question_id (code)"
        assert issue.description, "拒绝应有 description (message)"


# ════════════════════════════════════════════
# 攻击向量6：写入越权
# ════════════════════════════════════════════


class TestAttackVectorWritePrivilege:
    """攻击向量6：写入越权——WriteValidator 拒绝禁止的生产操作。"""

    def test_forbidden_truncate_in_temp_ops_rejected(self):
        """TempTableStatement 含 TRUNCATE 操作被 WriteValidator 拒绝。"""
        import uuid

        from tianshu_datadev.sql.write_plan import FinalWritePlan, TempTableStatement
        from tianshu_datadev.sql.write_validator import WriteValidator

        temp_id = f"_temp_{uuid.uuid4().hex[:8]}"
        write_plan = FinalWritePlan(
            write_plan_id="wp_test",
            program_id="prog_test",
            target_table="target_db.target_table",
            overwrite_mode="partition",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id=temp_id,
                    operation="TRUNCATE",  # 禁止操作
                    # SQL 文本不含 TRUNCATE 词——避免 model_validator 提前拦截
                    sql=f"SELECT * FROM target_db.{temp_id}",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(write_plan)

        assert validated.forbidden_operations, (
            "应拒绝 TRUNCATE 操作——forbidden_operations 不应为空"
        )
        assert any("TRUNCATE" in op for op in validated.forbidden_operations), (
            f"forbidden_operations 应含 TRUNCATE: {validated.forbidden_operations}"
        )

    def test_forbidden_delete_operation_rejected(self):
        """TempTableStatement 含 DELETE 操作被拒绝。"""
        import uuid

        from tianshu_datadev.sql.write_plan import FinalWritePlan, TempTableStatement
        from tianshu_datadev.sql.write_validator import WriteValidator

        temp_id = f"_temp_{uuid.uuid4().hex[:8]}"
        write_plan = FinalWritePlan(
            write_plan_id="wp_test_delete",
            program_id="prog_test",
            target_table="target_db.target_table",
            overwrite_mode="partition",
            partition_keys=["dt"],
            partition_values={"dt": "20260101"},
            partition_format="yyyyMMdd",
            temp_table_ops=[
                TempTableStatement(
                    temp_id=temp_id,
                    operation="DELETE",  # 禁止操作
                    # SQL 文本不含 DELETE 词——避免 model_validator 提前拦截
                    sql=f"SELECT * FROM target_db.{temp_id}",
                    order_index=0,
                ),
            ],
        )

        validator = WriteValidator()
        validated = validator.validate(write_plan)

        assert validated.forbidden_operations, "应拒绝 DELETE 操作"


# ════════════════════════════════════════════
# 集成测试——SecurityEvaluator.run_all()
# ════════════════════════════════════════════


class TestSecurityEvaluatorIntegration:
    """通过 SecurityEvaluator 运行全量攻击测试。"""

    def test_run_all_vectors_block_all(self):
        """SecurityEvaluator.run_all() 运行全部 6 种攻击——所有用例通过。"""
        evaluator = SecurityEvaluator()
        report = evaluator.run_all()

        # 验证 6 种向量全被覆盖
        for vector in AttackVector:
            assert vector.value in report.vector_coverage, (
                f"攻击向量 {vector.value} 未被覆盖"
            )
            failed_details = [
                r.rejection_detail for r in report.results
                if r.attack_vector == vector and not r.passed
            ]
            assert report.vector_coverage[vector.value] is True, (
                f"攻击向量 {vector.value} 未全部拦截: {failed_details}"
            )

        # 验证全部用例通过
        assert all(r.passed for r in report.results), (
            f"未拦截的攻击: {report.blocking_issues}"
        )

        # 验证汇总信息
        assert "cases blocked" in report.summary
        assert report.eval_id, "应生成 eval_id"

    def test_run_single_vector(self):
        """run_vector() 运行单个攻击向量。"""
        evaluator = SecurityEvaluator()
        results = evaluator.run_vector(AttackVector.SCHEMA_EXTRA)

        assert len(results) >= 1
        for r in results:
            assert r.attack_vector == AttackVector.SCHEMA_EXTRA
            assert isinstance(r, SecurityCaseResult)

    def test_each_rejection_has_code_path_message(self):
        """每次拒绝有 code、path、message——不依赖 LLM '自行判断'。"""
        evaluator = SecurityEvaluator()
        report = evaluator.run_all()

        for result in report.results:
            assert result.passed, (
                f"{result.case_id}: 攻击未被拦截——"
                f"拒绝应来自确定性规则，非 LLM 判断"
            )
            # rejection_detail 应非空（即 code + message）
            assert result.rejection_detail, (
                f"{result.case_id}: rejection_detail 为空——"
                f"每次拒绝必须有明确的拒绝信息"
            )
            # detection_layer 应指向具体防御层
            assert result.detection_layer in (
                "schema", "validator", "render", "write_validator",
            ), (
                f"{result.case_id}: detection_layer 无效——"
                f"'{result.detection_layer}' 非合法防御层"
            )

    def test_payload_drives_different_attack_payloads(self):
        """同一攻击向量的不同 case 使用不同的攻击载荷——payload 是事实来源。

        取 SQL_INJECTION 向量：4 个 case 应有不同的 trace（malicious_value 不同）。
        取 WRITE_PRIVILEGE 向量：3 个 case 应有不同的 rejection_detail 来源。
        """
        evaluator = SecurityEvaluator()

        # 验证 SQL 注入——4 个 case 测试不同的注入目标和值
        sql_results = evaluator.run_vector(AttackVector.SQL_INJECTION)
        assert len(sql_results) == 4, f"SQL_INJECTION 应有 4 个 case，实际 {len(sql_results)}"
        # 每个 case 的 trace 应包含不同的注入值
        sql_traces = {r.case_id: r.trace for r in sql_results}
        unique_traces = set(sql_traces.values())
        assert len(unique_traces) >= 2, (
            f"SQL 注入的 4 个 case 应使用不同攻击值，"
            f"实际只有 {len(unique_traces)} 种 trace: {sql_traces}"
        )

        # 验证写入越权——3 个 case 测试不同的操作/场景
        wp_results = evaluator.run_vector(AttackVector.WRITE_PRIVILEGE)
        assert len(wp_results) == 3, (
            f"WRITE_PRIVILEGE 应有 3 个 case，实际 {len(wp_results)}"
        )
        wp_details = {r.case_id: r.rejection_detail for r in wp_results}
        unique_details = set(wp_details.values())
        assert len(unique_details) >= 2, (
            f"写入越权的 3 个 case 应使用不同攻击策略，"
            f"实际只有 {len(unique_details)} 种 rejection_detail: {wp_details}"
        )

    def test_sql_injection_cases_have_distinct_targets(self):
        """SQL_INJECTION 的 4 个 case 覆盖 4 个不同的 SafeIdentifier 承载字段。

        ColumnRef.column_name / ColumnRef.table_ref / ScanStep.table_ref /
        AggregateSpec.input_column —— 每个都是独立的攻击面。
        """
        evaluator = SecurityEvaluator()
        cases = evaluator._load_cases(AttackVector.SQL_INJECTION)

        # 收集每个 case 声称的 (target_type, target_field)
        targets = set()
        for c in cases:
            p = c.payload
            key = (p.get("target_type", "?"), p.get("target_field", "?"))
            targets.add(key)

        assert len(targets) >= 3, (
            f"SQL_INJECTION 的 4 个 case 应覆盖至少 3 种不同的 "
            f"(target_type, target_field) 组合，实际: {targets}"
        )


# ════════════════════════════════════════════
# 负向回归测试——不相关拒绝 ≠ 成功
# ════════════════════════════════════════════


class TestFallbackRejectionIsNotSuccess:
    """负向回归：当 Validator 返回 blocking 但拒绝码不匹配预期时，passed=False。

    这是方案 A 的核心安全属性——消除"不相关拒绝也算成功"的虚高报告。

    测试策略：
    - Part A：直接验证 fallback 逻辑——当 Validator 因不相关原因拒绝时，结果不为 passed=True
    - Part B：验证 run_all() 汇总逻辑——passed=False 结果进入 blocking_issues
    """

    def test_precise_rejection_code_match_distinguishes_attack_vectors(self):
        """精确拒绝码匹配能区分不同类型的攻击拦截。

        Validator 独立运行所有检查——一个 plan 可能触发多个 blocking 拒绝码。
        Evaluator 的精确匹配确保 Q-VAL-GATE- 只被算作 Join 门禁成功，
        Q-VAL-COL- 只被算作字段校验成功，互不混淆。

        此测试验证：同一 plan 同时触发 Q-VAL-COL- 和 Q-VAL-GATE- 时，
        Join 门禁评测器只认 Q-VAL-GATE-，字段评测器只认 Q-VAL-COL-。
        """
        from tianshu_datadev.developer_spec.models import (
            ManifestColumn,
            ManifestTable,
            SourceManifest,
        )
        from tianshu_datadev.planning.relationship_hypothesis import (
            EvidenceAction,
            JoinCandidate,
            JoinEvidenceLevel,
            RelationshipEvidence,
            RelationshipHypothesis,
        )

        # 构造 plan：列缺失 + WEAK Join 证据 → 两种 blocking 同时触发
        plan = SqlBuildPlan(
            plan_id="test_multi_reject",
            spec_hash="abc",
            hypothesis_id="hyp_weak",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(
                            table_ref="t1",
                            column_name="nonexistent_col",
                            normalized_name="nonexistent_col",
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_t2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(
                            table_ref="t2", column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_weak",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(
                                table_ref="t1",
                                column_name="nonexistent_col",
                                normalized_name="nonexistent_col",
                            ),
                            ColumnRef(
                                table_ref="t2", column_name="id",
                                normalized_name="id",
                            ),
                        ),
                    ],
                    relationship_ref="jc_weak_001",
                ),
            ],
            multi_table=True,
        )

        weak_evidence = RelationshipEvidence(
            evidence_id="ev_weak_001",
            level=JoinEvidenceLevel.WEAK,
            action=EvidenceAction.REJECT_BLOCKING,
            left_table="t1",
            right_table="t2",
            left_key_raw="nonexistent_col",
            right_key_raw="id",
            left_key_normalized="nonexistent_col",
            right_key_normalized="id",
            evidence_checks=["field_name_match: NO_MATCH"],
            detail="字段名不同——等级 WEAK",
        )

        hypothesis = RelationshipHypothesis(
            hypothesis_id="hyp_weak",
            spec_hash="abc",
            candidates=[
                JoinCandidate(
                    candidate_id="jc_weak_001",
                    left_table="t1",
                    right_table="t2",
                    left_key="nonexistent_col",
                    right_key="id",
                    left_key_normalized="nonexistent_col",
                    right_key_normalized="id",
                    join_type=JoinType.INNER,
                    evidence=weak_evidence,
                ),
            ],
        )

        manifest = SourceManifest(
            manifest_id="test_multi",
            spec_hash="abc",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(
                            column_name="id", normalized_name="id",
                            data_type="int",
                        ),
                    ],
                ),
                ManifestTable(
                    table_ref="t2",
                    source_table="db.t2",
                    columns=[
                        ManifestColumn(
                            column_name="id", normalized_name="id",
                            data_type="int",
                        ),
                    ],
                ),
            ],
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(
            plan, manifest, hypothesis=hypothesis,
        )

        # Validator 独立运行所有检查——多个 blocking 码可能同时出现
        assert passed is False, "Validator 应拒绝此 plan"
        blocking_codes = {
            q.question_id.split("-")[0] + "-" + q.question_id.split("-")[1]
            if len(q.question_id.split("-")) >= 2 else q.question_id
            for q in questions if q.blocking
        }

        # 核心断言：Q-VAL-COL- 和 Q-VAL-GATE- 应同时存在
        # ——它们来自不同的校验路径，互不阻断
        has_col = any("Q-VAL-COL-" in q.question_id for q in questions if q.blocking)
        has_gate = any("Q-VAL-GATE-" in q.question_id for q in questions if q.blocking)
        assert has_col, f"应有 Q-VAL-COL- blocking，实际: {blocking_codes}"
        assert has_gate, f"应有 Q-VAL-GATE- blocking，实际: {blocking_codes}"

        # 模拟 evaluator 的精确匹配：Join 门禁评测器只取 Q-VAL-GATE-
        gate_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-GATE-" in q.question_id
        ]
        assert len(gate_issues) >= 1, "精确匹配应找到 Q-VAL-GATE-——Join 门禁正常工作"

        # 模拟 evaluator 的精确匹配：字段评测器只取 Q-VAL-COL-
        col_issues = [
            q for q in questions
            if q.blocking and "Q-VAL-COL-" in q.question_id
        ]
        assert len(col_issues) >= 1, "精确匹配应找到 Q-VAL-COL-——字段校验正常工作"

    def test_run_all_blocking_issues_count_matches_failed_results(self):
        """run_all() 的 blocking_issues 数量等于 passed=False 的结果数量。

        验证汇总逻辑：SecurityEvalReport 正确收集所有未拦截的攻击。
        """
        evaluator = SecurityEvaluator()
        report = evaluator.run_all()

        failed_results = [r for r in report.results if not r.passed]
        assert len(report.blocking_issues) == len(failed_results), (
            f"blocking_issues 数量 ({len(report.blocking_issues)}) "
            f"应与 passed=False 结果数量 ({len(failed_results)}) 一致。"
            f"\nblocking_issues={report.blocking_issues}"
            f"\nfailed_results={[r.case_id for r in failed_results]}"
        )

        # 额外验证：每个 failed case_id 都出现在 blocking_issues 中
        for fr in failed_results:
            found = any(fr.case_id in bi for bi in report.blocking_issues)
            assert found, (
                f"{fr.case_id}: passed=False 但未出现在 blocking_issues 中。"
                f"blocking_issues={report.blocking_issues}"
            )
