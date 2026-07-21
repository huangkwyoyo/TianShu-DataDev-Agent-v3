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

H1. 列名只能从 [Table Schemas] 中选择，禁止编造。

H2. 聚合函数只能是：COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT

H3. 时间函数只能是：HOUR
    不要使用 DAY、MONTH、YEAR、DAY_OF_WEEK、DATE_TRUNC、EXTRACT 等。

H4. CASE WHEN 条件必须使用类型化 Predicate 树。
    禁止输出 when/then 字符串模式。
    条件只能使用 COMPARE / IS_NULL / IS_NOT_NULL / AND / OR。
    不要使用 NOT 节点——用反向比较操作符（!=、IS_NULL vs IS_NOT_NULL）代替。
    THEN 值为纯字符串字面量。

H5. 不确定时写入 uncertainties。只写 field_ref + description + candidates。
    不写 category——阻断级别由系统确定性规则决定。

H6. 不要覆盖 [Existing Declarations] 中程序员已手写的字段。

H7. label_table 类型不在你的职责范围——返回全空输出。

H8. 窗口函数、比率指标、跨粒度依赖不在你的职责范围——
    不给这些字段生成任何输出，也不生成 uncertainty。"""

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
                    "description": {"type": "string"},
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["field_ref", "description"],
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


class RequirementPlanner:
    """从自然语言业务描述生成结构化声明。

    使用 LLM（通过 ProviderAdapter）推断：
    - dimensions: 基础维度
    - derived_dimensions: 派生维度（仅有 HOUR 时间函数）
    - metrics: 基础指标
    - case_when_rules: 类型化 CASE WHEN 规则
    - uncertainties: 不确定项

    LLM 调用失败时返回全空 RequirementPlannerOutput——不阻断管线。
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
            logger.warning("RequirementPlanner LLM 调用失败：%s", e)
            return RequirementPlannerOutput()

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

        try:
            case_when_rules = []
            for rule in raw.get("case_when_rules", []):
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
            logger.warning("解析 case_when_rules 失败：%s", e)
            case_when_rules = []

        try:
            uncertainties = [
                UncertaintyEntry(**u)
                for u in raw.get("uncertainties", [])
            ]
        except Exception as e:
            logger.warning("解析 uncertainties 失败：%s", e)
            uncertainties = []

        return RequirementPlannerOutput(
            dimensions=dimensions,
            derived_dimensions=derived_dimensions,
            metrics=metrics,
            case_when_rules=case_when_rules,
            uncertainties=uncertainties,
        )
