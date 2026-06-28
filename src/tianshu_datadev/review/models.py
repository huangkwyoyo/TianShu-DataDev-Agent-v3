"""Review Package 数据模型——标签规则、窗口表达式和字段血缘。

Phase 3B 审查包输出——从 SqlBuildPlan 确定性抽取的可审查格式。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import StrictModel


class LabelRuleEntry(StrictModel):
    """单条 CASE WHEN 标签规则——条件 + 结果 + 默认值。"""

    step_id: str  # 对应 CaseWhenStep.step_id
    alias: str  # 输出列别名
    branches: list[str]  # "WHEN <cond> THEN <result>" 的人类可读描述
    else_value: str  # ELSE 默认值（人类可读）
    enum_values_declared: list[str]  # 已声明的枚举值（来自 DeveloperSpec）


class WindowExprEntry(StrictModel):
    """单条窗口函数表达式——函数 + 分区 + 排序 + 帧。"""

    step_id: str  # 对应 WindowStep.step_id
    function: str  # 窗口函数名
    input_column: str  # 输入列（排名函数为 ""）
    partition_by: list[str]  # PARTITION BY 列名列表
    order_by: list[str]  # ORDER BY 列名 + 方向列表
    frame: str  # 窗口帧的人类可读描述（无 frame 时为 ""）
    alias: str  # 输出别名


class FieldLineageEntry(StrictModel):
    """单条字段血缘——从源表字段到最终输出的转换链。"""

    source_table: str  # 源表引用
    source_column: str  # 源字段名
    transformations: list[str]  # 转换步骤（如 "COUNT()"、"CASE WHEN"、"ROW_NUMBER()"）
    output_alias: str  # 最终输出别名


class ReviewReport(StrictModel):
    """Phase 3B 审查报告——综合标签、窗口、血缘信息。"""

    plan_id: str  # 对应 SqlBuildPlan.plan_id
    label_rules: list[LabelRuleEntry] = []  # CASE WHEN 标签规则
    window_exprs: list[WindowExprEntry] = []  # 窗口函数表达式
    field_lineage: list[FieldLineageEntry] = []  # 字段血缘
    source_tables: list[str] = []  # 引用的源表
    output_columns: list[str] = []  # 最终输出列名
