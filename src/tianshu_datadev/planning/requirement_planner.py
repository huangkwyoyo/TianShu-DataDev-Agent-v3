"""RequirementPlanner——从自然语言业务描述生成结构化声明。

使用 LLM 推断维度、派生维度、指标和 CASE WHEN 规则。
输出 RequirementPlannerOutput——经 Validator → Promotion 后写入 Spec。
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from tianshu_datadev.developer_spec.models import (
    CaseWhenBranch,
    CaseWhenRule,
    DerivedDimensionDecl,
    DimensionDecl,
    MetricDecl,
    RequirementPlannerOutput,
    UncertaintyEntry,
)

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec, SourceManifest
    from tianshu_datadev.llm.adapters.base import ProviderAdapter

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════
# System Prompt
# ════════════════════════════════════════════

_REQUIREMENT_PLANNER_SYSTEM_PROMPT = """\
你是数据开发规格分析 Agent。阅读程序员提供的业务描述和源表 Schema，
输出结构化的维度、派生维度、指标和 CASE WHEN 规则。

════════════════════════════════════════
硬约束
════════════════════════════════════════

H1. 列名只能从以下来源中选择，禁止编造：
    - [Table Schemas] 的 columns.column_name：源表物理列
    - [output_columns]：输出列名列表
    - [existing_declarations] 的 metrics.alias：已声明的指标别名
    CASE WHEN post_aggregate 条件引用聚合结果列（如 crash_count）是正确语义——
    聚合结果列名在 output_columns 或 existing_declarations.metrics 中。

H2. 聚合函数只能是：COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT
    input_column 必须是裸列名（如 collision_id），不要带表别名前缀（如 cp.collision_id）。
    列名从 [Table Schemas] 的 columns.column_name 中选择。

H3. 时间函数只能是：HOUR
    不要使用 DAY、MONTH、YEAR、DAY_OF_WEEK、DATE_TRUNC、EXTRACT 等。

H4. CASE WHEN 条件必须使用类型化 Predicate 树。
    禁止输出 when/then 字符串模式。
    条件只能使用 COMPARE / IS_NULL / IS_NOT_NULL / AND / OR。
    不要使用 NOT 节点——用反向比较操作符（!=、IS_NULL vs IS_NOT_NULL）代替。
    THEN 值为纯字符串字面量。

H4 补充——有序 CASE WHEN 通过分支优先级避免 NOT 节点：

正确示例——风险等级标注（分支由高到低排列，无需 NOT）：
  CASE WHEN crash_count >= 30 OR total_killed >= 2 THEN "高"
       WHEN crash_count >= 10 THEN "中"
       WHEN crash_count >= 1 THEN "低"
       ELSE "无数据"

  关键：第一个分支拦截了所有"高风险"记录，
  后续分支自动排除了已满足的条件——不需要 NOT。

H5. 不确定时写入 uncertainties。每条包含：
    - field_ref: 诊断标识（自由文本，仅用于日志）
    - output_column: 输出列名（路由主键——必须填写，否则按 UNKNOWN 处理）
    - description: 为什么不确定
    - candidates: 可能的解析方案（可为空列表）
    - output_kind: 该列的业务性质——LABEL | METRIC | DERIVED_DIMENSION | UNKNOWN
      判断依据（互斥——每列只能匹配一条）：
      · LABEL: 条件分支产出的有限值分类（如 risk_level, peak_type, 安全等级）
        特征：输出依赖 WHEN/THEN 逻辑，不能仅用确定性函数从单一源列推导
      · METRIC: 聚合或聚合后数值计算（如 avg_xxx, total_xxx, rate）
      · DERIVED_DIMENSION: 对源字段做直接确定性变换（如 HOUR(pickup_at) → pickup_hour）
        特征：单输入 + 确定性函数 → 单输出，无分支
      · UNKNOWN: 完全无法判断——系统将阻断并请求人工裁决

H6. 不要覆盖 [Existing Declarations] 中程序员已手写的字段。

H8. 窗口函数、比率指标、跨粒度依赖不在你的处理范围——
    不给这些字段生成任何输出，也不生成 uncertainty。

H9. 遇到白名单外聚合函数（如 MODE、MEDIAN、STDDEV 等）时：
    不要尝试构造 MetricDecl——Schema 会拒绝。
    输出一条 uncertainty：
      - output_kind=METRIC
      - output_column=目标列名
      - description 说明"聚合函数 MODE 不在白名单 COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT 中"
    MODE 由最终 unresolved 检查确定性路由到人工审核——此处仅记录原因。"""

# ════════════════════════════════════════════
# JSON Schema（v3.1——predicate_root 不含 NOT）
# ════════════════════════════════════════════

_REQUIREMENT_PLANNER_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string"},
                    "column_ref": {"type": "string"},
                    "source_table": {"type": "string"},
                },
                "required": ["dimension_name", "column_ref", "source_table"],
                "additionalProperties": False,
            },
        },
        "derived_dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension_name": {"type": "string"},
                    "source_column": {"type": "string"},
                    "source_table": {"type": "string"},
                    "time_function": {"type": "string", "enum": ["HOUR"]},
                },
                "required": ["dimension_name", "source_column", "source_table", "time_function"],
                "additionalProperties": False,
            },
        },
        "metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string"},
                    "aggregation": {
                        "type": "string",
                        "enum": ["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"],
                    },
                    "input_column": {"type": "string"},
                    "alias": {"type": "string"},
                },
                "required": ["metric_name", "aggregation", "alias"],
                "additionalProperties": False,
            },
        },
        "case_when_rules": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "output_column": {"type": "string"},
                    "branches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "condition": {"$ref": "#/$defs/predicate_root"},
                                "then_value": {"type": "string"},
                            },
                            "required": ["condition", "then_value"],
                            "additionalProperties": False,
                        },
                    },
                    "else_value": {"type": "string"},
                },
                "required": ["output_column", "branches", "else_value"],
                "additionalProperties": False,
            },
        },
        "uncertainties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field_ref": {"type": "string"},
                    "output_column": {"type": ["string", "null"]},
                    "output_kind": {
                        "type": "string",
                        "enum": ["LABEL", "METRIC", "DERIVED_DIMENSION", "UNKNOWN"]
                    },
                    "description": {"type": "string"},
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["field_ref", "output_column", "output_kind", "description"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["dimensions", "derived_dimensions", "metrics", "case_when_rules", "uncertainties"],
    "additionalProperties": False,
    "$defs": {
        "literal": {
            "type": "object",
            "properties": {
                "node_type": {"const": "LITERAL"},
                "value": {},
                "data_type": {
                    "type": "string",
                    "enum": ["string", "number", "boolean", "null"],
                },
            },
            "required": ["node_type", "value", "data_type"],
            "additionalProperties": False,
        },
        "predicate_leaf": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "COMPARE"},
                        "left": {"type": "string"},
                        "op": {
                            "type": "string",
                            "enum": ["=", "!=", ">", ">=", "<", "<=", "IN", "NOT_IN"],
                        },
                        "right": {"$ref": "#/$defs/literal"},
                    },
                    "required": ["node_type", "left", "op", "right"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "IS_NULL"},
                        "column": {"type": "string"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "IS_NOT_NULL"},
                        "column": {"type": "string"},
                    },
                    "required": ["node_type", "column"],
                    "additionalProperties": False,
                },
            ],
        },
        "predicate_root": {
            "oneOf": [
                {"$ref": "#/$defs/predicate_leaf"},
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "AND"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/predicate_root"},
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {
                        "node_type": {"const": "OR"},
                        "children": {
                            "type": "array",
                            "minItems": 2,
                            "items": {"$ref": "#/$defs/predicate_root"},
                        },
                    },
                    "required": ["node_type", "children"],
                    "additionalProperties": False,
                },
            ],
        },
    },
}


class RequirementPlanningError(Exception):
    """RequirementPlanner 调用失败——禁止静默回退。

    仅当 Adapter 技术失败时抛出：
      - error_type="llm_call_failed": adapter.invoke() 抛异常

    合法空输出（LLM 成功返回但所有列表为空）不抛异常——
    继续传递给 SpecEnricher 处理。
    """

    def __init__(
        self,
        error_type: str,
        message: str,
    ):
        super().__init__(f"[{error_type}] {message}")
        self.error_type = error_type
        self.message = message


class RequirementPlanner:
    """从自然语言业务描述生成结构化声明。

    使用 LLM（通过 ProviderAdapter）推断：
    - dimensions: 基础维度
    - derived_dimensions: 派生维度（仅有 HOUR 时间函数）
    - metrics: 基础指标
    - case_when_rules: 类型化 CASE WHEN 规则
    - uncertainties: 不确定项

    adapter.invoke() 失败时抛出 RequirementPlanningError——
    不再静默返回空 Output，由管线层处理阶段失败。
    """

    def __init__(self, adapter: ProviderAdapter | None = None):
        """初始化 RequirementPlanner。

        Args:
            adapter: LLM Provider 适配器。None 时 plan() 返回全空输出。
        """
        self._adapter = adapter

    def plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> RequirementPlannerOutput:
        """执行 LLM 推断，返回结构化声明。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            RequirementPlannerOutput——含推断结果
        """
        if self._adapter is None:
            return RequirementPlannerOutput()

        # 构建上下文
        context = self._build_context(spec, manifest)

        try:
            raw = self._adapter.invoke(
                system_message=_REQUIREMENT_PLANNER_SYSTEM_PROMPT,
                user_message=json.dumps(context, ensure_ascii=False),
                json_schema=_REQUIREMENT_PLANNER_JSON_SCHEMA,
                model="",
                temperature=0.1,
            )
        except Exception as e:
            logger.error("RequirementPlanner LLM 调用失败：%s", e)
            raise RequirementPlanningError(
                error_type="llm_call_failed",
                message=f"LLM 调用异常：{e}",
            ) from e

        # 合法空输出正常返回——交给 SpecEnricher
        return self._parse_response(raw)

    def _build_context(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> dict:
        """构建 LLM 调用的 Context 部分。"""
        # 源表 Schema
        tables_info: list[dict] = []
        for table in manifest.tables:
            cols_info = [
                {
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                }
                for col in table.columns
            ]
            tables_info.append({
                "table_ref": table.table_ref,
                "source_table": str(table.source_table) if table.source_table else None,
                "columns": cols_info,
            })

        # 已有声明——不可覆盖（H6）
        existing_declarations: dict = {
            "dimensions": [
                {"dimension_name": d.dimension_name, "column_ref": d.column_ref}
                for d in spec.dimensions
            ],
            "metrics": [
                {"metric_name": m.metric_name, "alias": m.alias}
                for m in spec.metrics
            ],
        }

        return {
            "table_schemas": tables_info,
            "existing_declarations": existing_declarations,
            "output_columns": [c.name for c in spec.output_spec.columns],
            "business_description": spec.description,
            "spec_title": spec.title,
        }

    def _parse_response(self, raw: dict) -> RequirementPlannerOutput:
        """解析 LLM 返回的 JSON 为 RequirementPlannerOutput。"""
        try:
            dimensions = [
                DimensionDecl(**d)
                for d in raw.get("dimensions", [])
            ]
        except Exception as e:
            logger.warning("解析 dimensions 失败：%s", e)
            dimensions = []

        try:
            derived_dimensions = [
                DerivedDimensionDecl(**dd)
                for dd in raw.get("derived_dimensions", [])
            ]
        except Exception as e:
            logger.warning("解析 derived_dimensions 失败：%s", e)
            derived_dimensions = []

        try:
            metrics = [
                MetricDecl(**m)
                for m in raw.get("metrics", [])
            ]
        except Exception as e:
            logger.warning("解析 metrics 失败：%s", e)
            metrics = []

        # 初始化 uncertainties——CASE WHEN 逐规则解析可能追加解析失败的 UncertaintyEntry
        uncertainties: list[UncertaintyEntry] = []

        # CASE WHEN 逐规则解析——单条失败不静默清空整个列表，
        # 而是生成 UncertaintyEntry 记录解析错误，由 Validator 转为阻断级 OpenQuestion。
        case_when_rules = []
        for i, rule in enumerate(raw.get("case_when_rules", [])):
            try:
                branches = [
                    CaseWhenBranch(**b)
                    for b in rule.get("branches", [])
                ]
                case_when_rules.append(CaseWhenRule(
                    output_column=rule["output_column"],
                    branches=branches,
                    else_value=rule.get("else_value", ""),
                ))
            except Exception as e:
                logger.warning(
                    "解析 case_when_rules[%d] 失败：%s", i, e,
                )
                # 生成 UncertaintyEntry 标记解析失败——
                # 下游 ProposalValidator 将其转为阻断 OpenQuestion
                output_col = rule.get("output_column", "<unknown>")
                uncertainties.append(UncertaintyEntry(
                    field_ref=f"case_when_rules.parse_error.{output_col}",
                    output_column=output_col if output_col != "<unknown>" else None,
                    output_kind="LABEL",
                    description=f"CASE WHEN 规则 '{output_col}' 解析失败：{e}",
                ))

        # LLM 返回的 uncertainties 追加到已有列表（已有列表可能含 CASE WHEN 解析错误）
        try:
            uncertainties.extend([
                UncertaintyEntry(**u)
                for u in raw.get("uncertainties", [])
            ])
        except Exception as e:
            logger.warning("解析 uncertainties 失败：%s", e)

        return RequirementPlannerOutput(
            dimensions=dimensions,
            derived_dimensions=derived_dimensions,
            metrics=metrics,
            case_when_rules=case_when_rules,
            uncertainties=uncertainties,
        )
