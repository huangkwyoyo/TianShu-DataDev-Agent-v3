"""ReviewPackageBuilder——从全部 artifact 组装 Code Review Package 目录结构。

组装流程：
1. 验证所有输入 artifact 的 hash 一致性
2. 创建目录结构（developer_spec / planning / contracts / sql / validation / feedback）
3. 写入各 artifact 文件，计算 SHA-256
4. 生成 provenance.yml
5. 生成 review.md
6. 生成 ReviewFeedback JSON Schema
7. 生成 ReviewPackageManifest

安全约束：不保存完整结果集——ExecutionTrace 只存 row_count，
ResultSummary 只存 sample_rows（前 20 行）。
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone

from .models import (
    VALID_REVIEW_TARGETS,
    ArtifactRef,
    HumanReviewItem,
    PackageInputs,
    ReviewPackageManifest,
)
from .provenance import generate_provenance
from .review_md import generate_review_md


class ReviewPackageBuilder:
    """从 SqlBuildPlan + CompiledSql + ExecutionTrace + SourceManifest 组装 Code Review Package。

    所有生成的文件均计算 SHA-256，记录到 ReviewPackageManifest 中。
    相同输入 → 相同目录结构 + 相同 hash。
    """

    def __init__(self, base_output_dir: str = "generated/review_packages"):
        """初始化构建器。

        Args:
            base_output_dir: 输出根目录（默认为 generated/review_packages）
        """
        self._base_dir = base_output_dir
        self._fixed_timestamp: str | None = None  # 测试用——固定时间戳

    def set_fixed_timestamp(self, timestamp: str) -> None:
        """设置固定时间戳——用于确定性测试。

        Args:
            timestamp: ISO 格式时间戳字符串
        """
        self._fixed_timestamp = timestamp

    def build(self, inputs: PackageInputs) -> ReviewPackageManifest:
        """组装完整 Code Review Package。

        Args:
            inputs: 组装所需的全部输入 artifact

        Returns:
            ReviewPackageManifest——含所有文件路径和 SHA-256

        Raises:
            ValueError: 输入验证失败（hash 不一致等）
        """
        # 1. 验证输入 hash 一致性
        self._validate_inputs(inputs)

        # 2. 确定输出目录
        package_dir = os.path.join(self._base_dir, inputs.request_id)

        # 3. 创建目录结构
        dirs = [
            os.path.join(package_dir, "developer_spec"),
            os.path.join(package_dir, "planning"),
            os.path.join(package_dir, "contracts"),
            os.path.join(package_dir, "sql"),
            os.path.join(package_dir, "validation"),
            os.path.join(package_dir, "feedback"),
        ]
        for d in dirs:
            os.makedirs(d, exist_ok=True)

        # 4. 构建人工审查清单
        review_items = self._build_review_items(inputs)

        # 5. 写入各 artifact 文件
        artifacts: list[ArtifactRef] = []

        # 5.1 developer_spec/
        artifacts.extend(self._write_developer_spec(package_dir, inputs))

        # 5.2 planning/
        artifacts.extend(self._write_planning(package_dir, inputs))

        # 5.3 contracts/
        artifacts.extend(self._write_contracts(package_dir, inputs))

        # 5.4 sql/
        artifacts.extend(self._write_sql(package_dir, inputs))

        # 5.5 validation/
        artifacts.extend(self._write_validation(package_dir, inputs))

        # 5.6 feedback/
        artifacts.extend(self._write_feedback_schema(package_dir))

        # 5.7 provenance.yml
        provenance_path = os.path.join(package_dir, "provenance.yml")
        provenance_yml, provenance_sha256 = generate_provenance(
            inputs, timestamp=self._fixed_timestamp
        )
        self._write_file(provenance_path, provenance_yml)
        artifacts.append(
            ArtifactRef(path="provenance.yml", sha256=provenance_sha256)
        )

        # 5.8 review.md
        review_path = os.path.join(package_dir, "review.md")
        review_content = generate_review_md(inputs, review_items)
        review_sha256 = hashlib.sha256(review_content.encode("utf-8")).hexdigest()
        self._write_file(review_path, review_content)
        artifacts.append(
            ArtifactRef(path="review.md", sha256=review_sha256)
        )

        # 6. 生成 ReviewPackageManifest
        manifest = self._build_manifest(inputs, artifacts, provenance_sha256)

        return manifest

    # ── 私有：目录写入 ──

    @staticmethod
    def _write_file(path: str, content: str) -> None:
        """写入文件——自动处理编码和换行符。"""
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)

    @staticmethod
    def _to_json(data, path: str) -> str:
        """将数据序列化为 JSON 文件，返回内容的 SHA-256。"""
        json_str = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        ReviewPackageBuilder._write_file(path, json_str)
        return hashlib.sha256(json_str.encode("utf-8")).hexdigest()

    @staticmethod
    def _artifact(path: str, sha256: str) -> ArtifactRef:
        """创建 ArtifactRef。"""
        return ArtifactRef(path=path, sha256=sha256)

    # ── 子目录写入 ──

    def _write_developer_spec(
        self, package_dir: str, inputs: PackageInputs
    ) -> list[ArtifactRef]:
        """写入 developer_spec/ 目录。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "developer_spec")

        # raw.md——原始 DeveloperSpec
        raw_path = os.path.join(subdir, "raw.md")
        raw_content = inputs.original_spec_md
        self._write_file(raw_path, raw_content)
        raw_sha = hashlib.sha256(raw_content.encode("utf-8")).hexdigest()
        artifacts.append(self._artifact("developer_spec/raw.md", raw_sha))

        # parsed.json——ParsedDeveloperSpec
        parsed_path = os.path.join(subdir, "parsed.json")
        parsed_sha = self._to_json(inputs.parsed_spec, parsed_path)
        artifacts.append(self._artifact("developer_spec/parsed.json", parsed_sha))

        # open_questions.md——开放问题清单
        oq_path = os.path.join(subdir, "open_questions.md")
        oq_content = self._render_open_questions_md(inputs)
        self._write_file(oq_path, oq_content)
        oq_sha = hashlib.sha256(oq_content.encode("utf-8")).hexdigest()
        artifacts.append(self._artifact("developer_spec/open_questions.md", oq_sha))

        return artifacts

    def _write_planning(
        self, package_dir: str, inputs: PackageInputs
    ) -> list[ArtifactRef]:
        """写入 planning/ 目录。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "planning")

        # relationship_hypotheses.md——Join 假设 + 证据链
        hyp_path = os.path.join(subdir, "relationship_hypotheses.md")
        hyp_content = self._render_hypotheses_md(inputs)
        self._write_file(hyp_path, hyp_content)
        hyp_sha = hashlib.sha256(hyp_content.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("planning/relationship_hypotheses.md", hyp_sha)
        )

        # sql_build_plan.json——SqlBuildPlan
        plan_path = os.path.join(subdir, "sql_build_plan.json")
        plan_sha = self._to_json(inputs.sql_build_plan, plan_path)
        artifacts.append(
            self._artifact("planning/sql_build_plan.json", plan_sha)
        )

        # field_lineage.md——字段溯源
        lineage_path = os.path.join(subdir, "field_lineage.md")
        lineage_content = self._render_field_lineage_md(inputs)
        self._write_file(lineage_path, lineage_content)
        lineage_sha = hashlib.sha256(lineage_content.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("planning/field_lineage.md", lineage_sha)
        )

        return artifacts

    def _write_contracts(
        self, package_dir: str, inputs: PackageInputs
    ) -> list[ArtifactRef]:
        """写入 contracts/ 目录。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "contracts")

        # data_transform_contract.json
        contract_path = os.path.join(subdir, "data_transform_contract.json")
        contract_sha = self._to_json(inputs.data_transform_contract, contract_path)
        artifacts.append(
            self._artifact("contracts/data_transform_contract.json", contract_sha)
        )

        return artifacts

    def _write_sql(
        self, package_dir: str, inputs: PackageInputs
    ) -> list[ArtifactRef]:
        """写入 sql/ 目录。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "sql")

        # main.sql——编译产物 SQL
        sql_content = ""
        if inputs.sql_artifact and "compiled_sql" in inputs.sql_artifact:
            sql_content = inputs.sql_artifact["compiled_sql"].get("sql", "")

        sql_path = os.path.join(subdir, "main.sql")
        self._write_file(sql_path, sql_content)
        sql_sha = hashlib.sha256(sql_content.encode("utf-8")).hexdigest()
        artifacts.append(self._artifact("sql/main.sql", sql_sha))

        return artifacts

    def _write_validation(
        self, package_dir: str, inputs: PackageInputs
    ) -> list[ArtifactRef]:
        """写入 validation/ 目录。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "validation")

        # source_validation.md——SourceManifest 校验结果
        sv_path = os.path.join(subdir, "source_validation.md")
        sv_content = self._render_source_validation_md(inputs)
        self._write_file(sv_path, sv_content)
        sv_sha = hashlib.sha256(sv_content.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("validation/source_validation.md", sv_sha)
        )

        # join_validation.md——Join 证据校验
        jv_path = os.path.join(subdir, "join_validation.md")
        jv_content = self._render_join_validation_md(inputs)
        self._write_file(jv_path, jv_content)
        jv_sha = hashlib.sha256(jv_content.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("validation/join_validation.md", jv_sha)
        )

        # enum_checks.md——枚举值检查
        enum_path = os.path.join(subdir, "enum_checks.md")
        enum_content = self._render_enum_checks_md(inputs)
        self._write_file(enum_path, enum_content)
        enum_sha = hashlib.sha256(enum_content.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("validation/enum_checks.md", enum_sha)
        )

        # execution_trace.json
        if inputs.execution_trace:
            trace_path = os.path.join(subdir, "execution_trace.json")
            trace_sha = self._to_json(inputs.execution_trace, trace_path)
            artifacts.append(
                self._artifact("validation/execution_trace.json", trace_sha)
            )

        return artifacts

    def _write_feedback_schema(self, package_dir: str) -> list[ArtifactRef]:
        """写入 feedback/review_feedback.schema.json。"""
        artifacts: list[ArtifactRef] = []
        subdir = os.path.join(package_dir, "feedback")

        schema = self._build_feedback_json_schema()
        schema_path = os.path.join(subdir, "review_feedback.schema.json")
        schema_json = json.dumps(schema, ensure_ascii=False, indent=2)
        self._write_file(schema_path, schema_json)
        schema_sha = hashlib.sha256(schema_json.encode("utf-8")).hexdigest()
        artifacts.append(
            self._artifact("feedback/review_feedback.schema.json", schema_sha)
        )

        return artifacts

    # ── 输入验证 ──

    @staticmethod
    def _validate_inputs(inputs: PackageInputs) -> None:
        """验证输入 artifact 之间的 hash 一致性。

        Raises:
            ValueError: hash 不一致
        """
        errors: list[str] = []

        # 验证 spec_hash 一致
        spec_hash = inputs.parsed_spec.get("spec_hash", "")
        plan_spec_hash = inputs.sql_build_plan.get("spec_hash", "")
        if spec_hash and plan_spec_hash and spec_hash != plan_spec_hash:
            errors.append(
                f"parsed spec_hash ({spec_hash}) != "
                f"sql_build_plan spec_hash ({plan_spec_hash})"
            )

        # 验证 hypothesis 与 plan 的关联
        if inputs.hypothesis:
            hyp_id = inputs.hypothesis.get("hypothesis_id", "")
            plan_hyp_id = inputs.sql_build_plan.get("hypothesis_id", "")
            if hyp_id and plan_hyp_id and hyp_id != plan_hyp_id:
                errors.append(
                    f"hypothesis_id ({hyp_id}) != "
                    f"sql_build_plan hypothesis_id ({plan_hyp_id})"
                )

        # 验证 contract 的 source_sqlbuildplan_hash 与 plan 一致
        # （contract 是在 packager 外部生成的，此处仅做一致性提醒）
        if errors:
            raise ValueError("输入 artifact hash 不一致:\n- " + "\n- ".join(errors))

    # ── 人工审查清单构建 ──

    @staticmethod
    def _build_review_items(inputs: PackageInputs) -> list[HumanReviewItem]:
        """从 OpenQuestions 和 PerfResults 构建人工审查清单。"""
        items: list[HumanReviewItem] = []
        item_idx = 0

        # 从 Parser/SourceManifest 的 OpenQuestions 构建
        for q in inputs.open_questions:
            blocking = q.get("blocking", False)
            items.append(
                HumanReviewItem(
                    item_id=f"hr_{item_idx:03d}",
                    category="open_question",
                    description=q.get("description", "未命名问题"),
                    severity="blocking" if blocking else "warning",
                    related_artifact="developer_spec/parsed.json",
                )
            )
            item_idx += 1

        # 从 Validator 的 OpenQuestions 构建
        for q in inputs.validation_questions:
            blocking = q.get("blocking", False)
            items.append(
                HumanReviewItem(
                    item_id=f"hr_{item_idx:03d}",
                    category=q.get("source", "open_question"),
                    description=q.get("description", "未命名问题"),
                    severity="blocking" if blocking else "warning",
                    related_artifact="planning/sql_build_plan.json",
                )
            )
            item_idx += 1

        # 从 PerfResults 构建
        for r in inputs.perf_results:
            passed = r.get("passed", True)
            if not passed:
                level = r.get("level", "WARN")
                items.append(
                    HumanReviewItem(
                        item_id=f"hr_{item_idx:03d}",
                        category="performance",
                        description=r.get("message", ""),
                        severity="blocking" if level == "REJECT" else "warning",
                        related_artifact="validation/execution_trace.json",
                    )
                )
                item_idx += 1

        # 如果有多表 Join 但证据等级为 MEDIUM → 加入审查清单
        join_rels = inputs.data_transform_contract.get("join_relationships", [])
        for jr in join_rels:
            level = jr.get("level", "")
            if level == "MEDIUM":
                items.append(
                    HumanReviewItem(
                        item_id=f"hr_{item_idx:03d}",
                        category="join_evidence",
                        description=(
                            f"Join 关系 '{jr.get('left_table', '')}' ↔ "
                            f"'{jr.get('right_table', '')}' 证据等级为 MEDIUM——"
                            f"建议人工确认关联键是否正确"
                        ),
                        severity="warning",
                        related_artifact="planning/relationship_hypotheses.md",
                    )
                )
                item_idx += 1

        # 如果缺少时间过滤 → 加入审查清单
        filters = inputs.data_transform_contract.get("filters", [])
        has_time_filter = any(
            "date" in str(f.get("left", "")).lower() or
            "dt" in str(f.get("left", "")).lower() or
            "time" in str(f.get("left", "")).lower()
            for f in filters
        )
        if not has_time_filter:
            items.append(
                HumanReviewItem(
                    item_id=f"hr_{item_idx:03d}",
                    category="time_filter",
                    description="未检测到明确的时间范围过滤条件——请确认是否需要限制数据时间范围",
                    severity="info",
                    related_artifact="planning/sql_build_plan.json",
                )
            )
            item_idx += 1

        return items

    # ── Markdown 渲染 ──

    @staticmethod
    def _render_open_questions_md(inputs: PackageInputs) -> str:
        """渲染开放问题清单 Markdown。"""
        lines = ["# 开放问题清单", ""]
        questions = inputs.open_questions + inputs.validation_questions

        if not questions:
            lines.append("(无开放问题)")
            return "\n".join(lines)

        for q in questions:
            blocking = "🔴 阻断" if q.get("blocking", False) else "🟡 非阻断"
            source = q.get("source", "未知")
            desc = q.get("description", "")
            lines.append(f"## [{blocking}] [{source}] {desc}")
            lines.append("")

            resolution = q.get("resolution")
            if resolution:
                answer = resolution.get("answer", "")
                resolved_by = resolution.get("resolved_by", "")
                if answer:
                    lines.append(f"**裁决**：{answer}")
                if resolved_by:
                    lines.append(f"**裁决人**：{resolved_by}")
                lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _render_hypotheses_md(inputs: PackageInputs) -> str:
        """渲染 Join 假设 Markdown。"""
        lines = ["# Join 关系假设与证据链", ""]

        if not inputs.hypothesis:
            lines.append("本项目为单表查询，无 Join 关系假设。")
            return "\n".join(lines)

        candidates = inputs.hypothesis.get("candidates", [])
        if not candidates:
            lines.append("(无 Join 候选)")
            return "\n".join(lines)

        for c in candidates:
            cid = c.get("candidate_id", "")
            lt = c.get("left_table", "")
            rt = c.get("right_table", "")
            lk = c.get("left_key", "")
            rk = c.get("right_key", "")
            evidence = c.get("evidence", {})

            lines.append(f"## Join 候选 `{cid}`")
            lines.append("")
            lines.append(f"- **左表**：{lt}")
            lines.append(f"- **右表**：{rt}")
            lines.append(f"- **关联键**：`{lt}.{lk}` = `{rt}.{rk}`")
            lines.append("")

            if evidence:
                lines.append(f"- **证据等级**：{evidence.get('level', '未知')}")
                lines.append(f"- **动作**：{evidence.get('action', '未知')}")
                lines.append(f"- **评级理由**：{evidence.get('detail', '')}")
                lines.append("")

                checks = evidence.get("evidence_checks", [])
                if checks:
                    lines.append("**证据检查项**：")
                    for chk in checks:
                        lines.append(f"  - {chk}")
                    lines.append("")

                # 输出 evidence_chain_yaml
                chain_yaml = evidence.get("evidence_chain_yaml", "")
                if chain_yaml:
                    lines.append("**完整证据链**：")
                    lines.append("")
                    lines.append("```yaml")
                    lines.append(chain_yaml)
                    lines.append("```")
                    lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _render_field_lineage_md(inputs: PackageInputs) -> str:
        """渲染字段溯源 Markdown。"""
        lines = ["# 字段溯源", ""]

        manifest = inputs.source_manifest
        tables = manifest.get("tables", [])

        if not tables:
            lines.append("(无字段溯源信息)")
            return "\n".join(lines)

        for t in tables:
            table_ref = t.get("table_ref", "")
            source_table = t.get("source_table", "")
            lines.append(f"## 表 `{table_ref}` → `{source_table}`")
            lines.append("")
            lines.append("| 字段名 | 归一化名 | 类型 | 来源 |")
            lines.append("|--------|----------|------|------|")

            for col in t.get("columns", []):
                col_name = col.get("column_name", "")
                norm_name = col.get("normalized_name", "")
                data_type = col.get("data_type", "")
                source = col.get("source", "未知")
                lines.append(f"| {col_name} | {norm_name} | {data_type} | {source} |")

            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _render_source_validation_md(inputs: PackageInputs) -> str:
        """渲染 SourceManifest 校验结果 Markdown。"""
        lines = ["# SourceManifest 校验结果", ""]

        manifest = inputs.source_manifest
        conflicts = manifest.get("conflicts", [])
        anomalies = manifest.get("anomalies", [])

        lines.append("## 冲突记录")
        lines.append("")
        if conflicts:
            for c in conflicts:
                field_ref = c.get("field_ref", "")
                conflict_type = c.get("conflict_type", "")
                dev_val = c.get("developer_spec_value", "")
                sr_val = c.get("schema_registry_value", "")
                lines.append(f"- **{field_ref}** [{conflict_type}]")
                lines.append(f"  - DeveloperSpec 值：`{dev_val}`")
                lines.append(f"  - SchemaRegistry 值：`{sr_val}`")
                lines.append("")
        else:
            lines.append("(无冲突)")
            lines.append("")

        lines.append("## 异常记录")
        lines.append("")
        if anomalies:
            for a in anomalies:
                aid = a.get("anomaly_id", "")
                desc = a.get("description", "")
                lines.append(f"- **{aid}**：{desc}")
                lines.append("")
        else:
            lines.append("(无异常)")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _render_join_validation_md(inputs: PackageInputs) -> str:
        """渲染 Join 证据校验结果 Markdown。"""
        lines = ["# Join 证据校验", ""]

        contract = inputs.data_transform_contract
        join_rels = contract.get("join_relationships", [])

        if not join_rels:
            lines.append("本项目为单表查询，无 Join 关系。")
            return "\n".join(lines)

        for jr in join_rels:
            jid = jr.get("join_id", "")
            lt = jr.get("left_table", "")
            rt = jr.get("right_table", "")
            level = jr.get("level", "未知")
            evidence = jr.get("evidence_chain", {})

            lines.append(f"## Join `{jid}`：`{lt}` ↔ `{rt}`")
            lines.append("")
            lines.append(f"- **证据等级**：{level}")
            lines.append("")

            if evidence:
                lines.append("### 证据链")
                lines.append("")
                lines.append("```json")
                lines.append(json.dumps(evidence, ensure_ascii=False, indent=2))
                lines.append("```")
                lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _render_enum_checks_md(inputs: PackageInputs) -> str:
        """渲染枚举值检查结果 Markdown。"""
        lines = ["# 枚举值检查", ""]

        # Phase 2：CaseWhenStep 尚未开放，枚举检查仅为占位
        # 检查 spec 中声明的 enum_values
        spec = inputs.parsed_spec
        input_tables = spec.get("input_tables", [])

        has_enums = False
        for t in input_tables:
            for c_list in [t.get("columns", []), t.get("key_columns", []),
                           t.get("business_columns", [])]:
                for c in c_list:
                    enum_vals = c.get("enum_values")
                    if enum_vals:
                        has_enums = True
                        col_name = c.get("column_name", "")
                        lines.append(f"## 字段 `{col_name}` 声明枚举值")
                        lines.append("")
                        lines.append(f"声明值：{', '.join(enum_vals)}")
                        lines.append("")
                        lines.append("> Phase 2：枚举值检查为占位——" +
                                     "CaseWhenStep 和枚举门禁在 Phase 3B 开放。")
                        lines.append("")

        if not has_enums:
            lines.append("(无枚举值声明——Phase 2 枚举检查为占位)")
            lines.append("")

        return "\n".join(lines)

    # ── JSON Schema 构建 ──

    @staticmethod
    def _build_feedback_json_schema() -> dict:
        """构建 ReviewFeedback JSON Schema（用于人工审查反馈的 schema 验证）。"""
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "ReviewFeedback",
            "description": (
                "结构化 Review 反馈——人工审查不通过时的返工输入。"
                "target 是机器路由主字段，finding_type 是细分原因。"
            ),
            "type": "object",
            "properties": {
                "request_id": {
                    "type": "string",
                    "description": "请求唯一标识",
                },
                "review_package_id": {
                    "type": "string",
                    "description": "审查包 ID",
                },
                "developer_spec_hash": {
                    "type": "string",
                    "description": "DeveloperSpec SHA-256",
                },
                "source_manifest_hash": {
                    "type": "string",
                    "description": "SourceManifest SHA-256",
                },
                "sql_build_plan_hash": {
                    "type": "string",
                    "description": "SqlBuildPlan SHA-256",
                },
                "sql_artifact_hash": {
                    "type": "string",
                    "description": "SqlArtifact SHA-256",
                },
                "target": {
                    "type": "string",
                    "enum": sorted(VALID_REVIEW_TARGETS),
                    "description": (
                        "机器路由主字段：REQUIREMENT → 修改 DeveloperSpec；"
                        "SQL_PLAN → 生成新 SqlBuildPlan；"
                        "COMPILER_BUG → 修 Compiler；"
                        "SOURCE_FACT → 更新 SourceManifest；"
                        "HUMAN_REVIEW → 停止自动返工"
                    ),
                },
                "finding_type": {
                    "type": "string",
                    "description": "细分原因（不参与路由）",
                },
                "comment": {
                    "type": "string",
                    "description": "人类可读的审查意见",
                },
                "suggested_resolution": {
                    "type": "string",
                    "description": "建议的解决方案",
                },
            },
            "required": [
                "request_id",
                "review_package_id",
                "developer_spec_hash",
                "source_manifest_hash",
                "sql_build_plan_hash",
                "sql_artifact_hash",
                "target",
                "finding_type",
                "comment",
                "suggested_resolution",
            ],
            "additionalProperties": False,
        }

    # ── Manifest 构建 ──

    def _build_manifest(
        self,
        inputs: PackageInputs,
        artifacts: list[ArtifactRef],
        provenance_sha256: str,
    ) -> ReviewPackageManifest:
        """构建 ReviewPackageManifest。"""
        now = self._fixed_timestamp if self._fixed_timestamp else datetime.now(timezone.utc).isoformat()
        package_id = ReviewPackageManifest.generate_package_id(inputs.request_id)

        return ReviewPackageManifest(
            request_id=inputs.request_id,
            package_id=package_id,
            created_at=now,
            artifacts=artifacts,
            spec_hash=inputs.parsed_spec.get("spec_hash", ""),
            source_manifest_hash=inputs.source_manifest.get("manifest_id", ""),
            sql_build_plan_hash=inputs.sql_build_plan.get("plan_id", ""),
            sql_artifact_hash=inputs.sql_artifact.get("artifact_id", ""),
            data_transform_contract_hash=inputs.data_transform_contract.get(
                "contract_id", ""
            ),
            provenance_hash=provenance_sha256,
            retry_count=inputs.retry_count,
        )
