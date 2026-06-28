"""SecurityEvaluator——Phase 4C 安全评测器。

对 6 种攻击向量进行确定性评测：
1. Prompt 注入——Schema 层 extra="forbid" 拒绝 raw_sql 字段
2. SQL 注入——SafeIdentifier AfterValidator 拒绝非法字符
3. Schema extra 突破——Pydantic extra="forbid" 拒绝未定义字段
4. 未声明引用——Validator 拒绝未注册表引用
5. Join 错误推理——Validator 硬门禁拦截 WEAK Join
6. 写入越权——WriteValidator 拒绝禁止操作

评测器不修改被测系统——只读取、验证、报告。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from tianshu_datadev.developer_spec.models import SourceManifest
from tianshu_datadev.planning.models import (
    ColumnRef,
    JoinType,
)
from tianshu_datadev.planning.relationship_hypothesis import (
    EvidenceAction,
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipHypothesis,
)
from tianshu_datadev.planning.sql_build_plan import (
    JoinStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator
from tianshu_datadev.sql.write_plan import (
    FinalWritePlan,
    TempTableStatement,
)
from tianshu_datadev.sql.write_validator import WriteValidator

from .models import (
    AttackVector,
    SecurityCase,
    SecurityCaseResult,
    SecurityEvalReport,
)

# ════════════════════════════════════════════
# 攻击数据集路径
# ════════════════════════════════════════════

# 默认攻击数据集目录——相对于 haraness 包的路径
_DEFAULT_ATTACK_DIR = Path(__file__).resolve().parent.parent.parent.parent / "harness" / "datasets" / "attack"

# 文件名 → AttackVector 映射
_VECTOR_FILE_MAP: dict[str, AttackVector] = {
    "prompt_injection.json": AttackVector.PROMPT_INJECTION,
    "sql_injection.json": AttackVector.SQL_INJECTION,
    "schema_extra.json": AttackVector.SCHEMA_EXTRA,
    "undeclared_ref.json": AttackVector.UNDECLARED_REF,
    "join_error.json": AttackVector.JOIN_ERROR_INFERENCE,
    "write_privilege.json": AttackVector.WRITE_PRIVILEGE,
}


class SecurityEvaluator:
    """安全评测器——确定性评估 6 种攻击向量的防御效果。

    评测流程（确定性的，不调用真实 LLM）：
    1. 加载攻击数据集（从 haraness/datasets/attack/ 的 JSON fixture）
    2. 为每个攻击用例构造攻击载荷
    3. 注入被测系统（Schema → Validator → Compiler → WriteValidator）
    4. 检测拦截层并记录结果
    5. 生成 SecurityEvalReport
    """

    def __init__(self, attack_dataset_dir: str | None = None):
        """初始化安全评测器。

        Args:
            attack_dataset_dir: 攻击数据集目录路径。
                                默认为 haraness/datasets/attack/。
        """
        self._attack_dir = Path(attack_dataset_dir) if attack_dataset_dir else _DEFAULT_ATTACK_DIR
        # 缓存已加载的用例
        self._cases_cache: dict[AttackVector, list[SecurityCase]] = {}

    # ── 公开接口 ──

    def run_all(self, vectors: list[AttackVector] | None = None) -> SecurityEvalReport:
        """运行全部（或指定子集）攻击向量的评测。

        Args:
            vectors: 要评测的攻击向量列表。None 表示全部 6 种。

        Returns:
            SecurityEvalReport——含每个攻击用例的逐条结果。
        """
        if vectors is None:
            vectors = list(AttackVector)

        all_results: list[SecurityCaseResult] = []
        vector_coverage: dict[str, bool] = {}

        for vector in vectors:
            try:
                case_results = self.run_vector(vector)
                all_results.extend(case_results)
                # 覆盖状态：该向量所有用例都通过才算覆盖
                vector_coverage[vector.value] = all(
                    r.passed for r in case_results
                ) if case_results else False
            except Exception as e:
                # 评测器自身的异常不应阻止其他向量的评测
                vector_coverage[vector.value] = False
                all_results.append(
                    SecurityCaseResult(
                        case_id=f"SEC-ERROR-{vector.value}",
                        attack_vector=vector,
                        passed=False,
                        detection_layer=None,
                        rejection_detail=f"评测器异常：{e}",
                        trace=str(e),
                    )
                )

        # 汇总
        blocked_count = sum(1 for r in all_results if r.passed)
        total_count = len(all_results)
        blocking_issues = [
            f"{r.case_id}: {r.rejection_detail}"
            for r in all_results if not r.passed
        ]

        return SecurityEvalReport(
            eval_id=SecurityEvalReport.generate_eval_id(),
            timestamp=datetime.now(timezone.utc).isoformat(),
            summary=f"{blocked_count}/{total_count} cases blocked",
            vector_coverage=vector_coverage,
            results=all_results,
            blocking_issues=blocking_issues,
        )

    def run_vector(self, vector: AttackVector) -> list[SecurityCaseResult]:
        """运行单个攻击向量的全部用例。"""
        cases = self._load_cases(vector)
        return [self.evaluate_case(case) for case in cases]

    def evaluate_case(self, case: SecurityCase) -> SecurityCaseResult:
        """评测单个攻击用例——按攻击向量分派到对应的 _eval_* 方法。

        每种攻击向量有独立的注入策略和验证方式。
        """
        dispatcher = {
            AttackVector.PROMPT_INJECTION: self._eval_prompt_injection,
            AttackVector.SQL_INJECTION: self._eval_sql_injection,
            AttackVector.SCHEMA_EXTRA: self._eval_schema_extra,
            AttackVector.UNDECLARED_REF: self._eval_undeclared_ref,
            AttackVector.JOIN_ERROR_INFERENCE: self._eval_join_error_inference,
            AttackVector.WRITE_PRIVILEGE: self._eval_write_privilege,
        }
        handler = dispatcher.get(case.attack_vector)
        if handler is None:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=f"未知攻击向量：{case.attack_vector}",
                trace="无对应的评测方法",
            )
        return handler(case)

    # ── 攻击向量1：Prompt 注入 ──

    def _eval_prompt_injection(self, case: SecurityCase) -> SecurityCaseResult:
        """检测 Prompt 注入——构造含 raw_sql/where_sql/join_on 额外字段的 dict，
        验证 Pydantic extra="forbid" 拒绝。

        注入方式：LLM 被 Prompt 注入后输出含自由 SQL 文本字段的 SqlBuildPlan，
        Schema 层应在构造时即拒绝。
        """
        # 构造含额外字段的 SqlBuildPlan dict
        # extra_field 从 payload 读取——fixture 是攻击参数的唯一事实来源
        payload = case.payload
        extra_field = payload.get("extra_field", case.expected_rejection_pattern)
        injection_value = payload.get("injection_value", "SELECT * FROM users")
        bad_dict: dict[str, Any] = {
            "plan_id": "test_prompt_injection",
            "spec_hash": "abc123",
            "steps": [],
            extra_field: injection_value,  # 注入的自由 SQL
        }

        try:
            SqlBuildPlan(**bad_dict)
            # 如果构造成功——说明 Schema 层未拒绝
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=f"Schema 层未拒绝 extra 字段 '{extra_field}'——extra='forbid' 可能未生效",
                trace=f"SqlBuildPlan({extra_field}=...) 构造成功，预期应抛出 ValidationError",
            )
        except ValidationError as e:
            errors_str = str(e)
            # 检查是否因 extra 字段被拒绝
            if "extra" in errors_str.lower() or extra_field in errors_str:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="schema",
                    rejection_detail=f"Schema 层拒绝 extra 字段 '{extra_field}'",
                    trace=f"ValidationError: {errors_str[:200]}",
                )
            # 其他 ValidationError（如字段缺失）——不是因 extra 字段被拒绝
            # 拒绝原因未命中预期，不算攻击被成功拦截
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="schema",
                rejection_detail=(
                    f"Schema 层拒绝但未命中预期原因——"
                    f"预期因 extra 字段 '{extra_field}' 被拒，"
                    f"实际: {errors_str[:150]}"
                ),
                trace=f"ValidationError: {errors_str[:200]}",
            )
        except Exception as e:
            # 评测器自身异常不应被解释为安全拦截成功
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=f"评测器内部异常（非安全拦截）：{type(e).__name__}：{e}",
                trace=str(e)[:200],
            )

    # ── 攻击向量2：SQL 注入 ──

    def _eval_sql_injection(self, case: SecurityCase) -> SecurityCaseResult:
        """检测 SQL 注入——根据 payload 中的 target_type 和 target_field，
        构造对应的 SafeIdentifier 承载对象，验证 AfterValidator 拒绝。

        注入方式：字段名/表名/过滤值中含 `'; DROP TABLE--` 等 SQL 注入向量，
        SafeIdentifier 的 AfterValidator 应在 Schema 层拒绝。

        payload 字段：
        - malicious_value: 注入字符串
        - target_type: 承载 SafeIdentifier 的类型（"ColumnRef"|"ScanStep"|"AggregateSpec"）
        - target_field: 注入目标字段名（"column_name"|"table_ref"|"input_column"）
        """
        payload = case.payload
        malicious_value = payload.get(
            "malicious_value", "x'; DROP TABLE users; --"
        )
        target_type = payload.get("target_type", "ColumnRef")
        target_field = payload.get("target_field", "column_name")

        try:
            if target_type == "ColumnRef":
                # ColumnRef 的 table_ref / column_name / normalized_name 均为 SafeIdentifier
                kwargs: dict[str, Any] = {
                    "table_ref": "t1",
                    "column_name": "id",
                    "normalized_name": "id",
                }
                kwargs[target_field] = malicious_value
                # 如果注入目标是 normalized_name，同步调整 column_name 使其合法
                if target_field == "normalized_name":
                    kwargs["column_name"] = "id"
                ColumnRef(**kwargs)
            elif target_type == "ScanStep":
                # ScanStep.table_ref 是 SafeIdentifier
                if target_field == "table_ref":
                    ScanStep(
                        step_id="s_inject",
                        table_ref=malicious_value,
                        required_columns=[
                            ColumnRef(
                                table_ref="t1",
                                column_name="id",
                                normalized_name="id",
                            ),
                        ],
                    )
                else:
                    # 未知 target_field——回退到 ColumnRef 测试
                    ColumnRef(
                        table_ref="t1",
                        column_name=malicious_value,
                        normalized_name="safe_col",
                    )
            elif target_type == "AggregateSpec":
                # AggregateSpec.input_column 是 SafeIdentifier | None
                from tianshu_datadev.planning.models import AggregateSpec, AggregationType

                if target_field == "input_column":
                    AggregateSpec(
                        aggregation=AggregationType.SUM,
                        input_column=malicious_value,
                        alias="safe_alias",
                    )
                else:
                    # 回退
                    ColumnRef(
                        table_ref="t1",
                        column_name=malicious_value,
                        normalized_name="safe_col",
                    )
            else:
                # 未知 target_type——回退到 ColumnRef.column_name
                ColumnRef(
                    table_ref="t1",
                    column_name=malicious_value,
                    normalized_name="safe_col",
                )

            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=(
                    f"SafeIdentifier 未拒绝 {target_type}.{target_field}='{malicious_value}'——"
                    f"该字段可能缺少 AfterValidator"
                ),
                trace=(
                    f"{target_type}({target_field}='{malicious_value}') 构造成功，"
                    f"预期应抛出 ValidationError 或 ValueError"
                ),
            )
        except ValidationError as e:
            errors_str = str(e)
            if "非法 SQL 标识符" in errors_str or "非法" in errors_str:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="schema",
                    rejection_detail=(
                        f"SafeIdentifier 拒绝 {target_type}.{target_field} "
                        f"含 SQL 注入字符"
                    ),
                    trace=f"ValidationError: {errors_str[:200]}",
                )
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="schema",
                rejection_detail=(
                    "Schema 层拒绝但未命中预期原因——"
                    "预期 SafeIdentifier AfterValidator 拒绝 SQL 注入字符，"
                    f"实际: {errors_str[:150]}"
                ),
                trace=f"ValidationError: {errors_str[:200]}",
            )
            # AfterValidator 抛出 ValueError——也是合法拦截
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=True,
                detection_layer="schema",
                rejection_detail="SafeIdentifier AfterValidator 拒绝",
                trace=f"ValueError: {str(e)[:200]}",
            )

    # ── 攻击向量3：Schema extra 突破 ──

    def _eval_schema_extra(self, case: SecurityCase) -> SecurityCaseResult:
        """检测 Schema extra 突破——构造含未定义字段的模型 dict，
        验证 Pydantic extra="forbid" 拒绝。

        注入方式：LLM 试图在 SqlBuildPlan 中嵌入 sql、expression、raw_where
        等自由 SQL 字段绕过 Compiler。
        """
        payload = case.payload
        extra_field = payload.get("extra_field", case.expected_rejection_pattern)
        injection_value = payload.get("injection_value", "injected_value")
        bad_dict: dict[str, Any] = {
            "plan_id": "test_schema_extra",
            "spec_hash": "abc123",
            "steps": [],
            extra_field: injection_value,
        }

        try:
            SqlBuildPlan(**bad_dict)
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=f"Schema 层未拒绝 extra 字段 '{extra_field}'",
                trace=f"SqlBuildPlan({extra_field}=...) 构造成功，预期应抛出 ValidationError",
            )
        except ValidationError as e:
            errors_str = str(e)
            if "extra" in errors_str.lower() or extra_field in errors_str:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="schema",
                    rejection_detail=f"Schema extra='forbid' 拒绝 '{extra_field}'",
                    trace=f"ValidationError: {errors_str[:200]}",
                )
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="schema",
                rejection_detail=(
                    "Schema 层拒绝但未命中预期原因——"
                    f"预期因 extra 字段 '{extra_field}' 被拒，"
                    f"实际: {errors_str[:150]}"
                ),
                trace=f"ValidationError: {errors_str[:200]}",
            )

    # ── 攻击向量4：未声明引用 ──

    def _eval_undeclared_ref(self, case: SecurityCase) -> SecurityCaseResult:
        """检测未声明引用——构造引用未注册表的 SqlBuildPlan，
        验证 Validator 拒绝。

        注入方式：指标引用不在 SourceManifest 中的表，
        Validator._validate_table_refs() 应产生 Q-VAL-TABLE- 拒绝码。
        """
        # 从 payload 读取攻击参数——fixture 是攻击参数的唯一事实来源
        payload = case.payload
        undeclared_table = payload.get("table_ref", "nonexistent_table")

        # 构造一个引用未注册表的 SqlBuildPlan
        plan = SqlBuildPlan(
            plan_id="test_undeclared_ref",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_undeclared",
                    table_ref=undeclared_table,  # 不在 manifest 中
                    required_columns=[
                        ColumnRef(
                            table_ref=undeclared_table,
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )

        # 构造一个仅含合法表的 SourceManifest——不含 nonexistent_table
        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="abc123",
            tables=[],  # 空清单——所有引用都算未注册
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        if not passed:
            # 找到 Q-VAL-TABLE- 相关的拒绝
            table_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-TABLE-" in q.question_id
            ]
            if table_issues:
                issue = table_issues[0]
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=f"Validator 拒绝未注册表引用：{issue.question_id}",
                    trace=f"question_id={issue.question_id}, "
                          f"field_ref={issue.field_ref}, "
                          f"description={issue.description[:200]}",
                )
            # 有其他 blocking 问题但没有精确匹配 Q-VAL-TABLE-
            # 拒绝码未命中预期——不能算"未声明引用攻击被拦截"
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="validator",
                rejection_detail=(
                    f"Validator 拒绝但拒绝码未命中预期——"
                    f"预期 Q-VAL-TABLE-*，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )
        else:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail="Validator 未拒绝未注册表引用——表引用校验可能缺失",
                trace="plan 含 nonexistent_table，manifest 为空，但 Validator 返回 passed=True",
            )

    # ── 攻击向量5：Join 错误推理 ──

    def _eval_join_error_inference(self, case: SecurityCase) -> SecurityCaseResult:
        """检测 Join 错误推理——根据 payload.scenario 分派：
        - "weak_evidence": WEAK 证据等级的 Join candidate → Q-VAL-GATE-
        - "type_mismatch": Join key 类型不兼容（int vs varchar）→ Q-VAL-JOINTYPE-

        注入方式：Planner 输出错误推理的 Join，Validator 门禁应在编译前拦截。
        """
        payload = case.payload
        scenario = payload.get("scenario", "weak_evidence")

        if scenario == "type_mismatch":
            return self._eval_join_type_mismatch(case, payload)
        else:
            return self._eval_join_weak_evidence(case)

    def _eval_join_weak_evidence(self, case: SecurityCase) -> SecurityCaseResult:
        """Join 门禁——WEAK 证据等级拦截。"""
        plan = SqlBuildPlan(
            plan_id="test_join_error",
            spec_hash="abc123",
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
                    relationship_ref="jc_weak_001",  # 必须匹配 candidate_id
                ),
            ],
            multi_table=True,
        )

        # 构造一个 WEAK 等级的 RelationshipHypothesis
        from tianshu_datadev.planning.relationship_hypothesis import (
            RelationshipEvidence,
        )

        weak_evidence = RelationshipEvidence(
            evidence_id="ev_weak_001",
            level=JoinEvidenceLevel.WEAK,  # WEAK——应被门禁拦截
            action=EvidenceAction.REJECT_BLOCKING,
            left_table="t1",
            right_table="t2",
            left_key_raw="id",
            right_key_raw="id",
            left_key_normalized="id",
            right_key_normalized="id",
            evidence_checks=["field_name_match: MATCH（仅命名）"],
            detail="字段名相同但无唯一性证据，仅基于命名猜测——等级 WEAK",
        )

        hypothesis = RelationshipHypothesis(
            hypothesis_id="hyp_weak",
            spec_hash="abc123",
            candidates=[
                JoinCandidate(
                    candidate_id="jc_weak_001",
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

        # 构造 manifest——包含两个表
        from tianshu_datadev.developer_spec.models import ManifestColumn, ManifestTable

        manifest = SourceManifest(
            manifest_id="test_manifest",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(column_name="id", normalized_name="id", data_type="int"),
                    ],
                ),
                ManifestTable(
                    table_ref="t2",
                    source_table="db.t2",
                    columns=[
                        ManifestColumn(column_name="id", normalized_name="id", data_type="int"),
                    ],
                ),
            ],
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest, hypothesis=hypothesis)

        if not passed:
            gate_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-GATE-" in q.question_id
            ]
            if gate_issues:
                issue = gate_issues[0]
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=f"Validator Join 门禁拒绝 WEAK Join：{issue.question_id}",
                    trace=f"question_id={issue.question_id}, "
                          f"description={issue.description[:200]}",
                )
            # 其他 blocking 问题但未命中 Q-VAL-GATE-——不能算 Join 门禁成功
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="validator",
                rejection_detail=(
                    f"Validator 拒绝但拒绝码未命中预期——"
                    f"预期 Q-VAL-GATE-*，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )
        else:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail="Validator 未拒绝 WEAK Join——Join 门禁可能缺失",
                trace="plan 含 WEAK 证据等级 Join，但 Validator 返回 passed=True",
            )

    def _eval_join_type_mismatch(
        self, case: SecurityCase, payload: dict
    ) -> SecurityCaseResult:
        """Join key 类型不兼容——int vs varchar 应被 Q-VAL-JOINTYPE- 拒绝。"""
        left_col = payload.get("left_column", "user_id")
        right_col = payload.get("right_column", "order_desc")
        left_type = payload.get("left_type", "int")
        right_type = payload.get("right_type", "varchar")

        plan = SqlBuildPlan(
            plan_id="test_join_type_mismatch",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_t1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(
                            table_ref="t1", column_name=left_col,
                            normalized_name=left_col,
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_t2",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(
                            table_ref="t2", column_name=right_col,
                            normalized_name=right_col,
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_type_mismatch",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(
                                table_ref="t1", column_name=left_col,
                                normalized_name=left_col,
                            ),
                            ColumnRef(
                                table_ref="t2", column_name=right_col,
                                normalized_name=right_col,
                            ),
                        ),
                    ],
                    relationship_ref="rel_type_mismatch",
                ),
            ],
            multi_table=True,
        )

        from tianshu_datadev.developer_spec.models import ManifestColumn, ManifestTable

        manifest = SourceManifest(
            manifest_id="test_manifest_jt",
            spec_hash="abc123",
            tables=[
                ManifestTable(
                    table_ref="t1",
                    source_table="db.t1",
                    columns=[
                        ManifestColumn(
                            column_name=left_col, normalized_name=left_col,
                            data_type=left_type,
                        ),
                    ],
                ),
                ManifestTable(
                    table_ref="t2",
                    source_table="db.t2",
                    columns=[
                        ManifestColumn(
                            column_name=right_col, normalized_name=right_col,
                            data_type=right_type,
                        ),
                    ],
                ),
            ],
        )

        validator = SqlBuildPlanValidator()
        passed, questions = validator.validate(plan, manifest)

        if not passed:
            jointype_issues = [
                q for q in questions
                if q.blocking and "Q-VAL-JOINTYPE-" in q.question_id
            ]
            if jointype_issues:
                issue = jointype_issues[0]
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="validator",
                    rejection_detail=(
                        f"Validator 拒绝类型不兼容 Join key "
                        f"（{left_col}:{left_type} vs {right_col}:{right_type}）："
                        f"{issue.question_id}"
                    ),
                    trace=f"question_id={issue.question_id}, "
                          f"description={issue.description[:200]}",
                )
            # 拒绝码未命中 Q-VAL-JOINTYPE-——不能算"Join 类型不兼容已识别"
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer="validator",
                rejection_detail=(
                    f"Validator 拒绝但拒绝码未命中预期——"
                    f"预期 Q-VAL-JOINTYPE-*，"
                    f"实际: {questions[0].question_id}"
                ),
                trace=f"question_id={questions[0].question_id}, "
                      f"description={questions[0].description[:200]}",
            )
        else:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=False,
                detection_layer=None,
                rejection_detail=(
                    f"Validator 未拒绝类型不兼容 Join key "
                    f"（{left_col}:{left_type} vs {right_col}:{right_type}）——"
                    f"Join key 类型校验可能缺失"
                ),
                trace=(
                    f"plan 含 {left_type} JOIN {right_type}，"
                    f"但 Validator 返回 passed=True"
                ),
            )

    # ── 攻击向量6：写入越权 ──

    def _eval_write_privilege(self, case: SecurityCase) -> SecurityCaseResult:
        """检测写入越权——根据 payload.test_strategy 分派：
        - "temp_table_op": 构造含禁止操作的 TempTableStatement → WV-005
        - "no_partition": 构造无分区的全表 overwrite → WV-001/WV-002

        注入方式：Agent 被注入写入指令，WriteValidator 应拒绝。
        """
        payload = case.payload
        test_strategy = payload.get("test_strategy", "temp_table_op")

        if test_strategy == "no_partition":
            return self._eval_write_no_partition(case, payload)
        else:
            return self._eval_write_temp_table_op(case, payload)

    def _eval_write_temp_table_op(
        self, case: SecurityCase, payload: dict
    ) -> SecurityCaseResult:
        """TempTableStatement 含禁止操作——WV-005 检查 op.operation。"""
        import uuid

        operation = payload.get("operation", "TRUNCATE")
        sql_text = payload.get("sql", "SELECT * FROM target_db._temp_xxx")

        try:
            temp_id = f"_temp_{uuid.uuid4().hex[:8]}"
            write_plan = FinalWritePlan(
                write_plan_id="wp_test_privilege",
                program_id="prog_test",
                target_table="target_db.target_table",
                overwrite_mode="partition",
                partition_keys=["dt"],
                partition_values={"dt": "20260101"},
                partition_format="yyyyMMdd",
                temp_table_ops=[
                    TempTableStatement(
                        temp_id=temp_id,
                        operation=operation,  # 从 payload 读取
                        # SQL 文本不含禁止词——避免 model_validator 提前拦截
                        sql=sql_text,
                        order_index=0,
                    ),
                ],
            )

            # 运行 WriteValidator
            write_validator = WriteValidator()
            validated = write_validator.validate(write_plan)

            if validated.forbidden_operations:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="write_validator",
                    rejection_detail=(
                        f"WriteValidator 拒绝禁止操作 '{operation}'："
                        f"{validated.forbidden_operations}"
                    ),
                    trace=f"forbidden_operations={validated.forbidden_operations}",
                )
            else:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=False,
                    detection_layer=None,
                    rejection_detail=(
                        f"WriteValidator 未拒绝 '{operation}' 操作——"
                        f"写入越权门禁可能缺失"
                    ),
                    trace=(
                        f"temp_table_ops 含 {operation}，"
                        f"但 forbidden_operations 为空"
                    ),
                )
        except ValidationError as e:
            # Schema 层拒绝——也是合法拦截
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=True,
                detection_layer="schema",
                rejection_detail=(
                    f"Schema 层在构造阶段拒绝 '{operation}' 操作"
                ),
                trace=f"ValidationError: {str(e)[:200]}",
            )
        except ValueError as e:
            # model_validator 拒绝——合法拦截
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=True,
                detection_layer="schema",
                rejection_detail=(
                    f"Schema 层拒绝 '{operation}' 操作（ValueError）"
                ),
                trace=f"ValueError: {str(e)[:200]}",
            )

    def _eval_write_no_partition(
        self, case: SecurityCase, payload: dict
    ) -> SecurityCaseResult:
        """全表 overwrite（无分区）——WV-001/WV-002 应拒绝。"""
        import uuid

        overwrite_mode = payload.get("overwrite_mode", "full")
        partition_keys = payload.get("partition_keys", [])
        partition_values = payload.get("partition_values", {})

        try:
            temp_id = f"_temp_{uuid.uuid4().hex[:8]}"
            write_plan = FinalWritePlan(
                write_plan_id="wp_test_no_partition",
                program_id="prog_test",
                target_table="target_db.target_table",
                overwrite_mode=overwrite_mode,  # 非 "partition" → WV-001
                partition_keys=partition_keys,  # 空 → WV-002
                partition_values=partition_values,  # 空 → WV-003
                partition_format="yyyyMMdd",
                temp_table_ops=[
                    TempTableStatement(
                        temp_id=temp_id,
                        operation="CREATE",
                        sql=f"CREATE TABLE target_db.{temp_id} AS SELECT * FROM source",
                        order_index=0,
                    ),
                    TempTableStatement(
                        temp_id=temp_id,
                        operation="INSERT",
                        sql=f"INSERT INTO target_db.target_table SELECT * FROM target_db.{temp_id}",
                        order_index=1,
                    ),
                ],
            )

            write_validator = WriteValidator()
            validated = write_validator.validate(write_plan)

            if validated.forbidden_operations:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=True,
                    detection_layer="write_validator",
                    rejection_detail=(
                        f"WriteValidator 拒绝全表 overwrite："
                        f"{validated.forbidden_operations}"
                    ),
                    trace=f"forbidden_operations={validated.forbidden_operations}",
                )
            else:
                return SecurityCaseResult(
                    case_id=case.case_id,
                    attack_vector=case.attack_vector,
                    passed=False,
                    detection_layer=None,
                    rejection_detail=(
                        "WriteValidator 未拒绝全表 overwrite（无分区）——"
                        "WV-001/WV-002 门禁可能缺失"
                    ),
                    trace=(
                        f"overwrite_mode={overwrite_mode}, "
                        f"partition_keys={partition_keys}，"
                        f"但 forbidden_operations 为空"
                    ),
                )
        except ValidationError as e:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=True,
                detection_layer="schema",
                rejection_detail="Schema 层拒绝全表 overwrite 构造",
                trace=f"ValidationError: {str(e)[:200]}",
            )
        except ValueError as e:
            return SecurityCaseResult(
                case_id=case.case_id,
                attack_vector=case.attack_vector,
                passed=True,
                detection_layer="schema",
                rejection_detail="Schema 层拒绝全表 overwrite（ValueError）",
                trace=f"ValueError: {str(e)[:200]}",
            )

    # ── 数据集加载 ──

    def _load_cases(self, vector: AttackVector) -> list[SecurityCase]:
        """加载指定攻击向量的测试用例（含缓存）。

        从 haraness/datasets/attack/<vector_file>.json 加载。
        """
        if vector in self._cases_cache:
            return self._cases_cache[vector]

        # 找到对应的文件名
        filename = None
        for fname, vec in _VECTOR_FILE_MAP.items():
            if vec == vector:
                filename = fname
                break

        if filename is None:
            raise ValueError(f"未知攻击向量：{vector}——无对应的 fixture 文件")

        filepath = self._attack_dir / filename
        if not filepath.is_file():
            raise FileNotFoundError(f"攻击数据集文件不存在：{filepath}")

        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)

        cases = [SecurityCase(**case_data) for case_data in data]
        self._cases_cache[vector] = cases
        return cases
