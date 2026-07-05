"""确定性 Markdown + YAML-like DeveloperSpec 解析器。

输入：包含 ```markdown fenced code block 的 Markdown 文本，block 内包含 YAML front matter。
输出：严格校验的 ParsedDeveloperSpec。

解析策略：
  1. 从输入文本中查找 ```markdown ... ``` fenced code block
  2. 在 block 内提取 --- ... --- YAML front matter
  3. 解析 YAML → 根据 spec: 键提取结构化数据 → Pydantic 模型
  4. 一次性递归检测禁止的自由 SQL 字段（替代各子解析器中 9 处重复检查）
  5. 执行 6 项允许宽松 + 7 项禁止宽松检查（不含自由 SQL 字段）
  6. 执行字段名归一化
  7. 计算 normalized_spec_hash（排除 open_questions、parse_warnings、description）
"""

from __future__ import annotations

import hashlib
import json
import re
import warnings
from typing import Any

import yaml

from tianshu_datadev.sql.expression_guard import validate_input_expression

from .field_normalizer import FieldNormalizer
from .models import (
    AggregationType,
    CaseWhenBranchDecl,
    CaseWhenDecl,
    ColumnDecl,
    ComputeStep,
    ComputeStepExpression,
    DimensionDecl,
    FilterDecl,
    InferredComputedMetric,
    InferredWindowMetric,
    InputTableDecl,
    JoinDecl,
    JoinTypeEnum,
    LegacyDescriptionDSLWarning,
    MetricDecl,
    MetricFilterDecl,
    MetricVariant,
    OpenQuestion,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    ParseWarning,
    SortDecl,
    SortDirection,
    TimeRangeDecl,
    WarningSeverity,
)


class ParseErrorCode:
    """解析错误码——对应 Phase 1A 错误码表。"""

    E001_YAML_PARSE_FAILED = "E001"  # YAML metadata block 解析失败
    E002_MISSING_REQUIRED_FIELD = "E002"  # 必填字段缺失（如 input_tables 为空）
    E003_AMBIGUOUS_TABLE_ALIAS = "E003"  # 表别名与物理表名映射不明确
    E004_UNDECLARED_FIELD_REF = "E004"  # 未声明字段被引用
    E005_DUPLICATE_TABLE_ALIAS = "E005"  # 两个表使用相同别名
    E006_EMPTY_OUTPUT_COLUMNS = "E006"  # 输出列列表为空
    E007_FREE_SQL_FIELD = "E007"  # raw_sql/where_sql/expression: str 字段出现
    E008_UNSAFE_EXPRESSION = "E008"  # input_expression 含禁止字符/模式——SQL 注入风险


class ParseError(Exception):
    """解析失败时抛出的异常——包含错误码、消息和可选的字段引用。"""

    def __init__(self, error_code: str, message: str, field_ref: str | None = None):
        self.error_code = error_code
        self.message = message
        self.field_ref = field_ref
        super().__init__(f"[{error_code}] {message}")


# ── row_count 中文量级规范化 ──

# 中文量级单位 → 乘数
_MAGNITUDE_MULTIPLIERS: dict[str, int] = {
    "万": 10_000,
    "百万": 1_000_000,
    "千万": 10_000_000,
    "亿": 100_000_000,
}

# 匹配模式：可选 ~ 前缀 + 数字（支持浮点）+ 可选空格 + 中文单位
_ROW_COUNT_PATTERN = re.compile(
    r"^~?\s*(\d+(?:\.\d+)?)\s*(万|亿|千万|百万)?\s*$"
)


def _normalize_row_count(raw: str) -> tuple[int | None, str | None]:
    """将中文量级 row_count 字符串规范化为整数。

    Args:
        raw: 原始字符串，如 "~5000万"、"~2亿"、"500"、"~1.5千万"

    Returns:
        (int_value, raw_string) —— int_value 为规范化的整数，raw_string 为原始值（用于追溯）
        无法解析时返回 (None, raw)。
    """
    if not raw or not isinstance(raw, str):
        return None, None

    raw_stripped = raw.strip()
    match = _ROW_COUNT_PATTERN.match(raw_stripped)
    if not match:
        # 尝试直接解析为整数
        try:
            value = int(raw_stripped.replace("~", "").strip())
            return value, raw_stripped
        except ValueError:
            return None, raw_stripped

    number_str = match.group(1)
    unit = match.group(2)

    number = float(number_str)
    if unit and unit in _MAGNITUDE_MULTIPLIERS:
        number *= _MAGNITUDE_MULTIPLIERS[unit]

    return int(number), raw_stripped


def _parse_optional_hint(hint_value: Any, model_cls: type):
    """将 YAML 中的结构化 hint dict 转换为对应的 Pydantic 模型。

    None / 空 dict → None，非法数据 → None（不阻断解析，由 validator 兜底）。

    Args:
        hint_value: YAML 中的 hint 值（dict 或 None）
        model_cls: 目标 Pydantic 模型类（MetricDecl / InferredComputedMetric / InferredWindowMetric）

    Returns:
        模型实例或 None
    """
    if hint_value is None:
        return None
    if isinstance(hint_value, model_cls):
        return hint_value
    if isinstance(hint_value, dict):
        # 空 dict → None
        if not hint_value:
            return None
        try:
            return model_cls(**hint_value)
        except Exception:
            return None
    return None


class DeveloperSpecParser:
    """确定性 Markdown + YAML-like DeveloperSpec 解析器。

    用法:
        parser = DeveloperSpecParser()
        spec = parser.parse(markdown_text)
        # spec 是已验证的 ParsedDeveloperSpec
    """

    # 禁止出现在任何声明中的自由 SQL 字段名
    _FORBIDDEN_SQL_FIELDS = frozenset({
        "raw_sql", "where_sql", "join_on", "expression",
        "aggregation_expr", "having_sql",
    })

    # 允许的 filter operator 值
    _VALID_FILTER_OPERATORS = frozenset({
        "=", "!=", ">", "<", ">=", "<=", "IN", "BETWEEN",
        "IS_NULL", "IS_NOT_NULL",
    })

    def __init__(self, normalizer: FieldNormalizer | None = None):
        """初始化解析器。

        Args:
            normalizer: 字段名归一化器，None 时使用默认配置
        """
        self._normalizer = normalizer or FieldNormalizer()
        self._question_counter = 0
        self._warning_counter = 0

    # ── 主入口 ──

    def parse(self, markdown_text: str) -> ParsedDeveloperSpec:
        """解析 Markdown 文本为 ParsedDeveloperSpec。

        Args:
            markdown_text: 包含 ```markdown fenced block 的完整文本

        Returns:
            验证通过的 ParsedDeveloperSpec

        Raises:
            ParseError: 遇到禁止宽松场景或无法解析的输入
        """
        self._question_counter = 0
        self._warning_counter = 0

        # 1. 提取 fenced code block
        fenced_text = self._extract_fenced_block(markdown_text)

        # 2. 提取 YAML front matter + Markdown 正文
        yaml_dict, md_body = self._extract_yaml_front_matter(fenced_text)

        # 3. 提取 spec: 子字典
        spec_dict = yaml_dict.get("spec", yaml_dict)

        # 4. 一次性递归检测禁止的自由 SQL 字段（替代各子解析器中 9 处重复检查）
        self._check_all_forbidden_fields(spec_dict)

        # 5. 解析各子部分（_check_all_forbidden_fields 已在上一步扫描全局，
        # 各子解析器中不再需要重复调用 _check_forbidden_sql_fields）
        # 注意：compute_steps 必须在 joins 之前解析——join 可能引用 step_name
        input_tables = self._parse_input_tables(spec_dict.get("source_tables", []))
        metrics = self._parse_metrics(spec_dict.get("metrics", []), input_tables)
        dimensions = self._parse_dimensions(spec_dict.get("dimensions", []))
        compute_steps = self._parse_compute_steps(spec_dict.get("compute_steps"), input_tables)
        joins = self._parse_joins(spec_dict.get("joins"), input_tables, compute_steps)
        time_range = self._parse_time_range(spec_dict.get("time_range"))
        output_spec = self._parse_output_spec(spec_dict)

        # 6. 提取标题
        title = self._extract_title(md_body) or spec_dict.get("summary", "Untitled")

        # 7. 组装描述
        summary = spec_dict.get("summary", "")
        description_parts = [p for p in [summary, md_body] if p]
        description = "\n\n".join(description_parts)

        # 8. 执行允许/禁止检查
        open_questions: list[OpenQuestion] = []
        parse_warnings: list[ParseWarning] = []

        self._validate_seven_rejections(spec_dict, input_tables, metrics, joins, output_spec)
        parse_warnings.extend(self._validate_six_allowances(spec_dict, input_tables, joins, time_range))

        # 9. 构建 ParsedDeveloperSpec（spec_id/spec_hash 先占位，步骤 10 计算后回填）
        spec = ParsedDeveloperSpec(
            spec_id="",
            spec_hash="",  # 先占位，计算 hash 后再填入
            title=title,
            description=description,
            input_tables=input_tables,
            metrics=metrics,
            dimensions=dimensions,
            joins=joins,
            time_range=time_range,
            output_spec=output_spec,
            compute_steps=compute_steps,
            open_questions=open_questions,
            parse_warnings=parse_warnings,
        )

        # 10. 计算 normalized_spec_hash 并回填 spec_hash / spec_id
        # spec_id 从 spec_hash 截取前 12 位派生，保证区分度与 spec_hash 一致
        spec_hash = self._normalized_spec_hash(spec)
        spec_id = f"spec_{spec_hash[:12]}"
        # 使用 object.__setattr__ 绕过 frozen 检查
        object.__setattr__(spec, "spec_hash", spec_hash)
        object.__setattr__(spec, "spec_id", spec_id)

        return spec

    # ── Fenced block 提取 ──

    def _extract_fenced_block(self, text: str) -> str:
        """从输入文本中提取 ```markdown ... ``` fenced code block 的内容。

        只匹配第一个 ```markdown（或 ```md）block。
        支持 block 前有可选的空白字符。
        """
        # 匹配 ```markdown 或 ```md 开头的 fenced block
        pattern = re.compile(r"```(?:markdown|md)\s*\r?\n(.*?)```", re.DOTALL)
        match = pattern.search(text)
        if not match:
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                "未找到 ```markdown fenced code block——输入必须包含一个 ```markdown 代码块",
            )
        return match.group(1)

    def _extract_yaml_front_matter(self, fenced_text: str) -> tuple[dict[str, Any], str]:
        """从 fenced block 内容中提取 YAML front matter。

        YAML front matter 由一对 --- 分隔符包围：
            ---
            spec:
              ...
            ---
            正文 Markdown

        Returns:
            (parsed_yaml_dict, markdown_body)
        """
        # 查找第一个 ---（必须在文本开头或紧跟换行符）
        lines = fenced_text.split("\n")
        # 找到 --- 分隔符的位置
        first_dash = None
        second_dash = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped == "---":
                if first_dash is None:
                    first_dash = i
                elif second_dash is None:
                    second_dash = i
                    break

        if first_dash is None:
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                "fenced block 内未找到 YAML front matter 起始分隔符 '---'",
            )

        if second_dash is None:
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                "fenced block 内未找到 YAML front matter 结束分隔符 '---'",
            )

        # 提取 YAML 部分
        yaml_text = "\n".join(lines[first_dash + 1 : second_dash])

        try:
            yaml_dict = yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                f"YAML 解析失败: {e}",
            ) from e

        if not isinstance(yaml_dict, dict):
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                "YAML front matter 解析结果不是字典——请检查 YAML 格式",
            )

        # 提取 Markdown 正文（第二个 --- 之后的内容）
        md_body = "\n".join(lines[second_dash + 1 :]).strip()

        return yaml_dict, md_body

    def _extract_title(self, md_body: str) -> str | None:
        """从 Markdown 正文中提取第一个 # 标题作为 spec 标题。"""
        if not md_body:
            return None
        match = re.match(r"^#\s+(.+)$", md_body.strip(), re.MULTILINE)
        if match:
            return match.group(1).strip()
        return None

    # ── 子解析器 ──

    def _parse_input_tables(self, raw_tables: list[dict]) -> list[InputTableDecl]:
        """解析 source_tables 列表为 InputTableDecl 列表。

        同时执行 7 项禁止检查：
          - 空 input_tables → E002
          - 重复别名 → E005
          - 自由 SQL 字段 → 已在步骤 4 的 _check_all_forbidden_fields 中完成
        """
        if not raw_tables:
            raise ParseError(
                ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                "input_tables 不能为空——必须声明至少一个源表",
            )

        tables: list[InputTableDecl] = []
        seen_aliases: set[str] = set()

        for raw in raw_tables:
            alias = raw.get("alias", raw.get("name", ""))
            if alias in seen_aliases:
                raise ParseError(
                    ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS,
                    f"表别名 '{alias}' 重复——每个源表必须使用不同的别名",
                    field_ref=alias,
                )
            seen_aliases.add(alias)

            # 合并 key_columns + business_columns → columns
            key_cols = raw.get("key_columns", []) or []
            biz_cols = raw.get("business_columns", []) or []
            flat_cols = raw.get("columns", []) or []
            all_raw_cols = list(key_cols) + list(biz_cols) + list(flat_cols)

            columns = self._parse_columns(all_raw_cols, f"table {alias}")

            # 解析 table 级 filters
            filters = self._parse_filters(raw.get("filters", []) or [], alias)

            # row_count 规范化
            raw_row = raw.get("row_count")
            row_count, raw_row_count = _normalize_row_count(str(raw_row)) if raw_row else (None, None)

            tables.append(InputTableDecl(
                table_alias=alias,
                source_table=raw.get("name", ""),
                row_count=row_count,
                raw_row_count=raw_row_count,
                role=raw.get("role"),
                description=raw.get("description"),
                columns=columns,
                filters=filters,
                partition_field=raw.get("partition_field"),
                time_field=raw.get("time_field"),
                key_columns=self._parse_columns(key_cols, f"table {alias} key_columns"),
                business_columns=self._parse_columns(biz_cols, f"table {alias} business_columns"),
            ))

        return tables

    def _parse_columns(self, raw_columns: list[dict], context: str) -> list[ColumnDecl]:
        """解析列定义列表——执行字段名归一化。

        Args:
            raw_columns: YAML 中的列定义列表
            context: 上下文描述（用于错误消息）
        """
        columns: list[ColumnDecl] = []
        for raw in raw_columns:
            if not isinstance(raw, dict):
                continue

            col_name = raw.get("name", "")
            normalized = self._normalizer.normalize(col_name)
            columns.append(ColumnDecl(
                column_name=col_name,
                normalized_name=normalized,
                data_type=raw.get("type"),
                enum_values=raw.get("enum"),
                nullable=raw.get("nullable"),
                unique=raw.get("unique"),
                description=raw.get("description"),
            ))
        return columns

    def _parse_filters(self, raw_filters: list[dict], table_alias: str) -> list[FilterDecl]:
        """解析表级过滤声明。"""
        filters: list[FilterDecl] = []
        for raw in raw_filters:
            if not isinstance(raw, dict):
                continue
            operator = raw.get("operator", "=")
            if operator not in self._VALID_FILTER_OPERATORS:
                raise ParseError(
                    ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                    f"不支持的过滤操作符 '{operator}'——允许: {sorted(self._VALID_FILTER_OPERATORS)}",
                    field_ref=raw.get("column_ref"),
                )
            filters.append(FilterDecl(
                column_ref=raw.get("column_ref", ""),
                operator=operator,
                value=raw.get("value"),
            ))
        return filters

    def _parse_metrics(
        self, raw_metrics: list[dict], tables: list[InputTableDecl]
    ) -> list[MetricDecl]:
        """解析 metrics 列表。

        7 项禁止检查：指标 input_column 引用的字段必须在某个 input_table 中存在。
        """
        if not raw_metrics:
            return []

        # 构建所有已声明字段的集合（按 normalized_name）
        declared_cols: set[str] = set()
        for t in tables:
            for c in t.columns:
                declared_cols.add(c.normalized_name)
            for c in t.key_columns:
                declared_cols.add(c.normalized_name)
            for c in t.business_columns:
                declared_cols.add(c.normalized_name)

        metrics: list[MetricDecl] = []
        for raw in raw_metrics:
            if not isinstance(raw, dict):
                continue

            input_col = raw.get("input_column")
            # COUNT(*) 允许 input_column 为 None
            if input_col is not None:
                normalized_input = self._normalizer.normalize(input_col)
                if normalized_input not in declared_cols:
                    raise ParseError(
                        ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                        f"指标 '{raw.get('metric_name')}' 引用了未声明的字段 '{input_col}'——"
                        f"该字段不在任何已声明的 input_table 中",
                        field_ref=input_col,
                    )

            # 校验 aggregation 值
            agg_str = raw.get("aggregation", "COUNT")
            try:
                aggregation = AggregationType(agg_str)
            except ValueError:
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    f"指标 '{raw.get('metric_name')}' 使用了不支持的聚合函数 '{agg_str}'——"
                    f"允许: {[a.value for a in AggregationType]}",
                    field_ref=raw.get("metric_name"),
                ) from None

            # Phase 4D：解析 filter / input_expression / distinct
            raw_filter = raw.get("filter")
            metric_filter = None
            if raw_filter and isinstance(raw_filter, dict):
                metric_filter = MetricFilterDecl(
                    column=raw_filter.get("column", ""),
                    operator=raw_filter.get("operator", "eq"),
                    value=str(raw_filter.get("value", "")),
                )

            # ── Phase 4D 安全校验：input_expression 入站过滤 + 编译器层检查 ──
            raw_input_expr = raw.get("input_expression")
            if raw_input_expr:
                # 入站层：禁止字符 + 禁止模式
                is_valid, err_msg = validate_input_expression(raw_input_expr, mode="strict")
                if not is_valid:
                    raise ParseError(
                        ParseErrorCode.E008_UNSAFE_EXPRESSION,
                        err_msg,
                        field_ref=f"metrics.{raw.get('metric_name', '')}.input_expression",
                    ) from None
                # 编译器层：白名单正则 + SQL 关键字拒绝
                is_valid_c, err_msg_c = validate_input_expression(raw_input_expr, mode="compiler")
                if not is_valid_c:
                    raise ParseError(
                        ParseErrorCode.E008_UNSAFE_EXPRESSION,
                        err_msg_c,
                        field_ref=f"metrics.{raw.get('metric_name', '')}.input_expression",
                    ) from None
                # 存储 strip 后的值——expression_guard 内部 strip 了，此处同步确保外部引用一致
                raw_input_expr = raw_input_expr.strip()

            metrics.append(MetricDecl(
                metric_name=raw.get("metric_name", ""),
                aggregation=aggregation,
                input_column=input_col,
                alias=raw.get("alias", raw.get("metric_name", "")),
                # ── Phase 5：多条件变体 ──
                variants=self._parse_metric_variants(raw.get("variants", [])),
                # ── Phase 4D：filter / input_expression / distinct ──
                filter=metric_filter,
                input_expression=raw_input_expr,
                distinct=raw.get("distinct", False),
            ))

        return metrics

    def _parse_metric_variants(
        self, raw_variants: list[dict] | None,
    ) -> list[MetricVariant] | None:
        """解析 MetricDecl.variants 列表——每个 variant 有独立的 filter + alias。

        YAML 格式：
            variants:
              - variant_name: paying_users
                filter:
                  column: status
                  operator: eq
                  value: paying
                alias: paying_users

        Args:
            raw_variants: 原始 variants 列表（可为 None 或空列表）

        Returns:
            解析后的 MetricVariant 列表，None 表示无 variants
        """
        if not raw_variants:
            return None
        variants: list[MetricVariant] = []
        for rv in raw_variants:
            if not isinstance(rv, dict):
                continue
            # 解析 filter
            raw_filter = rv.get("filter")
            filter_decl = None
            if raw_filter and isinstance(raw_filter, dict):
                filter_decl = MetricFilterDecl(
                    column=raw_filter.get("column", ""),
                    operator=raw_filter.get("operator", "eq"),
                    value=str(raw_filter.get("value", "")),
                )
            variants.append(MetricVariant(
                variant_name=rv.get("variant_name", ""),
                filter=filter_decl,
                alias=rv.get("alias", rv.get("variant_name", "")),
            ))
        return variants if variants else None

    def _parse_dimensions(self, raw_dimensions: list[dict]) -> list[DimensionDecl]:
        """解析 dimensions 列表。"""
        if not raw_dimensions:
            return []
        dimensions: list[DimensionDecl] = []
        for raw in raw_dimensions:
            if not isinstance(raw, dict):
                continue
            dimensions.append(DimensionDecl(
                dimension_name=raw.get("dimension_name", ""),
                column_ref=raw.get("column_ref", ""),
            ))
        return dimensions

    def _parse_joins(
        self, raw_joins: list[dict] | None, tables: list[InputTableDecl],
        compute_steps: list[ComputeStep] | None = None,
    ) -> list[JoinDecl] | None:
        """解析 joins 列表。

        None → 允许宽松（留空由 RelationshipHypothesis 推理）。
        7 项禁止检查：引用不存在的表别名或 step_name → E005。

        当 compute_steps 非空时，step_name 也作为合法的表引用——
        用于分支合流步骤的 Join 声明（两个上游 _temp 表的 Join 键）。
        """
        if raw_joins is None:
            return None
        if not raw_joins:
            return []

        valid_aliases = {t.table_alias for t in tables}
        # 当 compute_steps 存在时，step_name 也是合法的 Join 引用
        if compute_steps:
            for cs in compute_steps:
                valid_aliases.add(cs.step_name)
        joins: list[JoinDecl] = []
        for raw in raw_joins:
            if not isinstance(raw, dict):
                continue

            left = raw.get("left_table", "")
            right = raw.get("right_table", "")

            # 检查引用的表别名或 step_name 是否存在
            for side, alias in [("left", left), ("right", right)]:
                if alias and alias not in valid_aliases:
                    raise ParseError(
                        ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS,
                        f"Join {side}_table '{alias}' 不在已声明的 input_tables "
                        f"或 compute_steps 中——"
                        f"已声明的别名: {sorted(valid_aliases)}",
                        field_ref=alias,
                    )

            # 解析 join_type
            join_type_str = raw.get("join_type", "INNER").upper()
            try:
                join_type = JoinTypeEnum(join_type_str)
            except ValueError:
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    f"不支持的 Join 类型 '{join_type_str}'——允许: {[j.value for j in JoinTypeEnum]}",
                    field_ref=f"{left}-{right}",
                ) from None

            joins.append(JoinDecl(
                left_table=left,
                right_table=right,
                left_key=raw.get("left_key", ""),
                right_key=raw.get("right_key", ""),
                join_type=join_type,
            ))

        return joins

    def _parse_time_range(self, raw: dict | None) -> TimeRangeDecl | None:
        """解析 time_range 声明——None 时允许宽松。

        Phase 5 新增业务日历字段：calendar_type / relative_range / fiscal_year。
        relative_range 与 start/end 互斥——Parser 层校验。
        """
        if raw is None:
            return None
        if not isinstance(raw, dict):
            return None

        calendar_type = str(raw.get("calendar_type", "calendar"))
        relative_range = raw.get("relative_range")
        fiscal_year = raw.get("fiscal_year")

        # 校验 calendar_type 取值
        if calendar_type not in ("calendar", "fiscal_jul", "fiscal_apr"):
            calendar_type = "calendar"

        # 校验 relative_range 取值
        if relative_range is not None:
            relative_range = str(relative_range)
            if relative_range not in ("last_7d", "last_30d", "last_90d", "mtd", "ytd"):
                relative_range = None

        # fiscal_year 类型校验
        if fiscal_year is not None:
            try:
                fiscal_year = int(fiscal_year)
            except (ValueError, TypeError):
                fiscal_year = None

        return TimeRangeDecl(
            column_ref=raw.get("column_ref", ""),
            start=str(raw.get("start", "")),
            end=str(raw.get("end", "")),
            inclusive=raw.get("inclusive", True),
            calendar_type=calendar_type,
            relative_range=relative_range,
            fiscal_year=fiscal_year,
        )

    def _parse_output_spec(self, spec_dict: dict) -> OutputSpecDecl:
        """解析输出规格。

        7 项禁止检查：output_columns 为空 → E006。
        """
        # 从 output_columns 提取列声明列表（name + type + description + 结构化 hint）
        from tianshu_datadev.developer_spec.models import OutputColumnDecl
        raw_output_cols = spec_dict.get("output_columns", []) or []
        columns: list[OutputColumnDecl] = []
        for col in raw_output_cols:
            if isinstance(col, dict):
                name = col.get("name", "")
                if not name:
                    continue
                # 解析结构化 hint 字段（推荐方式）
                metric_hint = _parse_optional_hint(col.get("metric_hint"), MetricDecl)
                computed_hint = _parse_optional_hint(col.get("computed_hint"), InferredComputedMetric)
                window_hint = _parse_optional_hint(col.get("window_hint"), InferredWindowMetric)
                user_description = col.get("user_description")

                # 旧 description 格式兼容：无结构化 hint 但有 description → 警告
                old_description = col.get("description")
                if old_description and not any([metric_hint, computed_hint, window_hint]):
                    warnings.warn(
                        f"列 '{name}' 使用旧 description DSL 格式 "
                        f"（\"{old_description[:80]}{'...' if len(old_description) > 80 else ''}\"），"
                        f"请迁移到 metric_hint / computed_hint / window_hint 结构化字段",
                        LegacyDescriptionDSLWarning,
                        stacklevel=2,
                    )

                columns.append(OutputColumnDecl(
                    name=name,
                    type=col.get("type", "varchar"),
                    description=old_description,
                    metric_hint=metric_hint,
                    computed_hint=computed_hint,
                    window_hint=window_hint,
                    user_description=user_description,
                ))
            elif isinstance(col, str):
                # 向后兼容：纯字符串列名
                columns.append(OutputColumnDecl(name=col))

        if not columns:
            raise ParseError(
                ParseErrorCode.E006_EMPTY_OUTPUT_COLUMNS,
                "output_columns 不能为空——必须声明至少一个输出列",
            )

        # grain 来自 target_grain
        grain = spec_dict.get("target_grain", []) or []

        # 解析 sort
        raw_sort = spec_dict.get("sort")
        sort: list[SortDecl] | None = None
        if raw_sort:
            sort = []
            for s in raw_sort:
                if isinstance(s, dict):
                    direction_str = s.get("direction", "ASC").upper()
                    try:
                        direction = SortDirection(direction_str)
                    except ValueError:
                        direction = SortDirection.ASC
                    sort.append(SortDecl(
                        column=s.get("column", ""),
                        direction=direction,
                    ))

        # limit
        limit = spec_dict.get("limit")

        return OutputSpecDecl(
            columns=columns,
            grain=grain,
            sort=sort,
            limit=limit,
        )

    # ── 6 项允许宽松 + 7 项禁止宽松 ──

    def _validate_six_allowances(
        self,
        spec_dict: dict,
        input_tables: list[InputTableDecl],
        joins: list[JoinDecl] | None,
        time_range: TimeRangeDecl | None,
    ) -> list[ParseWarning]:
        """执行 6 项允许宽松检查——生成 ParseWarning 而非阻断。

        1. 字段类型未声明 → W001
        2. 时间范围未指定 → W002
        3. Join 未显式声明 → W003（但不由 Parser 生成，留给 RelationshipHypothesis）
        4. 输出排序未声明 → W004
        5. Markdown 正文中有额外非结构化说明 → 不生成警告（保留在 description 中）
        6. 字段注释中存在中文 → 不生成警告（归一化正常处理）
        """
        warnings: list[ParseWarning] = []

        # 1. 字段类型未声明
        for t in input_tables:
            for c in t.columns:
                if c.data_type is None:
                    warnings.append(self._make_warning(
                        "W001",
                        f"字段 '{t.table_alias}.{c.column_name}' 类型未声明，需从 SchemaRegistry 补充",
                        field_ref=f"{t.table_alias}.{c.column_name}",
                    ))

        # 2. 时间范围未指定
        has_time_field = any(t.time_field for t in input_tables)
        has_time_range = time_range is not None
        if has_time_field and not has_time_range:
            warnings.append(self._make_warning(
                "W002",
                "源表有时间字段但未指定时间范围，将使用全量数据",
            ))

        # 3. Join 未显式声明 —— 不生成警告（留给 RelationshipHypothesis 推理）

        # 4. 输出排序未声明 —— 检查 output_spec（已在 _parse_output_spec 中处理 sort=None）
        output_sort = spec_dict.get("sort")
        if not output_sort:
            warnings.append(self._make_warning(
                "W004",
                "输出排序未声明，默认不保证顺序",
            ))

        return warnings

    def _parse_compute_steps(
        self, raw_steps: list[dict] | None, input_tables: list[InputTableDecl]
    ) -> list[ComputeStep] | None:
        """解析 compute_steps 列表——可选的分布计算声明。

        校验：
        - step_name 在同一个 Spec 内必须唯一
        - source 为字符串时：必须是 "input" 或已存在的 step_name
        - source 为列表时：每个元素必须是 "input" 或已存在的 step_name
        - 不允许自引用（source 列表中不能包含自己的 step_name）
        - 不允许循环引用（Kahn 拓扑排序检测）
        - group_by / metrics 引用字段必须在 source 上下文中存在

        Args:
            raw_steps: YAML 中的 compute_steps 列表，None 或空列表返回 None
            input_tables: 已解析的源表列表（用于 "input" source 的字段校验）

        Returns:
            ComputeStep 列表，空列表或 None 表示走原路径
        """
        if not raw_steps:
            return None

        # 构建所有已声明字段的集合（用于 "input" source 字段校验）
        declared_cols: set[str] = set()
        for t in input_tables:
            for c in t.columns:
                declared_cols.add(c.normalized_name)
            for c in t.key_columns:
                declared_cols.add(c.normalized_name)
            for c in t.business_columns:
                declared_cols.add(c.normalized_name)

        # 第一步检查：收集所有 step_name，校验唯一性
        seen_names: set[str] = set()
        for raw in raw_steps:
            if not isinstance(raw, dict):
                continue
            name = raw.get("step_name", "")
            if not name:
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    "compute_steps 中每个步骤必须声明 step_name",
                )
            if name in seen_names:
                raise ParseError(
                    ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS,
                    f"compute_steps 中 step_name '{name}' 重复——每个步骤名必须唯一",
                    field_ref=name,
                )
            seen_names.add(name)

        # 第二步：解析每个步骤（先收集基本字段，再校验 source 引用）
        steps: list[ComputeStep] = []
        step_names: set[str] = set()

        for raw in raw_steps:
            if not isinstance(raw, dict):
                continue

            step_name = raw.get("step_name", "")
            source_raw = raw.get("source", "input")
            group_by: list[str] = raw.get("group_by", [])
            output_alias = raw.get("output_alias", "")

            if not output_alias:
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    f"compute_step '{step_name}' 必须声明 output_alias",
                    field_ref=step_name,
                )

            # 标准化 source——处理 list 和 scalar 两种形式
            if isinstance(source_raw, list):
                sources: list[str] = []
                for s in source_raw:
                    if not isinstance(s, str) or not s:
                        raise ParseError(
                            ParseErrorCode.E001_YAML_PARSE_FAILED,
                            f"compute_step '{step_name}' 的 source 列表元素必须是"
                            f"非空字符串",
                            field_ref=step_name,
                        )
                    sources.append(s)
                if len(sources) == 0:
                    raise ParseError(
                        ParseErrorCode.E001_YAML_PARSE_FAILED,
                        f"compute_step '{step_name}' 的 source 列表不能为空",
                        field_ref=step_name,
                    )
                if len(set(sources)) != len(sources):
                    raise ParseError(
                        ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                        f"compute_step '{step_name}' 的 source 列表中存在重复引用",
                        field_ref=step_name,
                    )
                # 校验每个 source 引用
                for s in sources:
                    if s != "input" and s not in step_names:
                        raise ParseError(
                            ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                            f"compute_step '{step_name}' 的 source '{s}' 无效——"
                            f"必须是 'input' 或已声明的 step_name"
                            f"（当前已声明：{sorted(step_names)}）",
                            field_ref=step_name,
                        )
                    if s == step_name:
                        raise ParseError(
                            ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                            f"compute_step '{step_name}' 的 source 不能引用自己——"
                            f"自引用形成非法循环",
                            field_ref=step_name,
                        )
            else:
                # 标量 source——校验引用
                source_str = str(source_raw)
                if source_str != "input" and source_str not in step_names:
                    raise ParseError(
                        ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                        f"compute_step '{step_name}' 的 source '{source_str}' 无效——"
                        f"必须是 'input' 或已声明的 step_name"
                        f"（当前已声明：{sorted(step_names)}）",
                        field_ref=step_name,
                    )
                if source_str == step_name:
                    raise ParseError(
                        ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                        f"compute_step '{step_name}' 的 source 不能引用自己——"
                        f"自引用形成非法循环",
                        field_ref=step_name,
                    )

            # 解析此步骤的 metrics
            raw_metrics = raw.get("metrics", [])
            step_metrics: list[MetricDecl] = []
            for rm in raw_metrics:
                if not isinstance(rm, dict):
                    continue
                input_col = rm.get("input_column")
                # 对于 source="input" 或 source 列表含 "input"，校验字段在 input_tables 中存在
                _has_input_source = (
                    source_raw == "input"
                    or (isinstance(source_raw, list) and "input" in source_raw)
                )
                if _has_input_source and input_col is not None:
                    normalized_input = self._normalizer.normalize(input_col)
                    if normalized_input not in declared_cols:
                        raise ParseError(
                            ParseErrorCode.E004_UNDECLARED_FIELD_REF,
                            f"compute_step '{step_name}' 的指标 '{rm.get('metric_name')}' "
                            f"引用了未声明的字段 '{input_col}'",
                            field_ref=input_col,
                        )

                agg_str = rm.get("aggregation", "COUNT")
                try:
                    aggregation = AggregationType(agg_str)
                except ValueError:
                    raise ParseError(
                        ParseErrorCode.E001_YAML_PARSE_FAILED,
                        f"compute_step '{step_name}' 的指标 '{rm.get('metric_name')}' "
                        f"使用了不支持的聚合函数 '{agg_str}'",
                        field_ref=rm.get("metric_name"),
                    ) from None

                step_metrics.append(MetricDecl(
                    metric_name=rm.get("metric_name", ""),
                    aggregation=aggregation,
                    input_column=input_col,
                    alias=rm.get("alias", rm.get("metric_name", "")),
                    # ── Phase 5：多条件变体 ──
                    variants=self._parse_metric_variants(rm.get("variants", [])),
                ))

            # 解析此步骤的源表 Join 声明（当 source="input" 且需多表 Join 时）
            raw_joins = raw.get("joins", [])
            step_joins: list[JoinDecl] = []
            if raw_joins:
                for rj in raw_joins:
                    if not isinstance(rj, dict):
                        continue
                    join_type_str = str(rj.get("join_type", "INNER")).upper()
                    try:
                        join_type = JoinTypeEnum(join_type_str)
                    except ValueError:
                        raise ParseError(
                            ParseErrorCode.E001_YAML_PARSE_FAILED,
                            f"compute_step '{step_name}' 的 Join 声明中不支持的 Join 类型 "
                            f"'{join_type_str}'——允许: {[j.value for j in JoinTypeEnum]}",
                            field_ref=step_name,
                        ) from None
                    step_joins.append(JoinDecl(
                        left_table=rj.get("left_table", ""),
                        right_table=rj.get("right_table", ""),
                        left_key=rj.get("left_key", ""),
                        right_key=rj.get("right_key", ""),
                        join_type=join_type,
                    ))

            steps.append(ComputeStep(
                step_name=step_name,
                source=source_raw,  # Pydantic validator 会归一化单元素列表
                group_by=group_by,
                metrics=step_metrics,
                output_alias=output_alias,
                joins=step_joins if step_joins else None,
                case_when=self._parse_case_when_raw(step_name, raw),
                expressions=self._parse_expressions_raw(step_name, raw),
            ))
            step_names.add(step_name)

        # 第三步：检测循环引用（Kahn 拓扑排序——支持多源 DAG）
        name_to_idx = {s.step_name: i for i, s in enumerate(steps)}
        in_degree: dict[str, int] = {s.step_name: 0 for s in steps}
        dependents: dict[str, list[str]] = {s.step_name: [] for s in steps}

        for s in steps:
            # 归一化 source 为列表以统一处理
            src_list = s.source if isinstance(s.source, list) else [s.source]
            for src in src_list:
                if src != "input" and src in name_to_idx:
                    in_degree[s.step_name] += 1
                    dependents[src].append(s.step_name)

        # Kahn 算法
        import heapq
        heap = [n for n, d in in_degree.items() if d == 0]
        heapq.heapify(heap)
        sorted_count = 0
        while heap:
            current = heapq.heappop(heap)
            sorted_count += 1
            for dep in dependents.get(current, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    heapq.heappush(heap, dep)

        if sorted_count != len(steps):
            # 循环检测：Kahn 算法结束后入度仍 > 0 的节点参与循环
            cyclic_nodes = sorted(n for n, d in in_degree.items() if d > 0)
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                f"compute_steps 存在循环引用——以下步骤无法排序：{cyclic_nodes}",
            )

        return steps if steps else None

    def _parse_case_when_raw(
        self, step_name: str, raw_step: dict
    ) -> CaseWhenDecl | None:
        """从原始 YAML 字典解析 CaseWhenDecl——支持字符串模式和类型化模式。"""
        raw_cw = raw_step.get("case_when")
        if raw_cw is None:
            return None
        if not isinstance(raw_cw, dict):
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                f"compute_step '{step_name}' 的 case_when 必须是字典",
            )

        raw_branches = raw_cw.get("branches", [])
        if not isinstance(raw_branches, list) or len(raw_branches) == 0:
            raise ParseError(
                ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                f"compute_step '{step_name}' 的 case_when.branches 必须是非空列表",
            )

        branches: list[CaseWhenBranchDecl] = []
        for bi, rb in enumerate(raw_branches):
            if isinstance(rb, dict):
                # 字符串模式：when/then 字段
                if "when" in rb and "then" in rb:
                    branches.append(CaseWhenBranchDecl(when=rb["when"], then=rb["then"]))
                # 类型化模式：condition_column/condition_operator/condition_value/result_column
                elif "condition_column" in rb:
                    branches.append(CaseWhenBranchDecl(
                        condition_column=rb.get("condition_column", ""),
                        condition_operator=rb.get("condition_operator", "="),
                        condition_value=str(rb.get("condition_value", "")),
                        result_column=rb.get("result_column", ""),
                    ))
                else:
                    raise ParseError(
                        ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                        f"compute_step '{step_name}' 的 case_when 分支[{bi}] "
                        f"需提供 when/then（字符串模式）或 condition_column/...  （类型化模式）",
                    )
            else:
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    f"compute_step '{step_name}' 的 case_when 分支[{bi}] 必须是字典",
                )

        return CaseWhenDecl(
            branches=branches,
            else_value=raw_cw.get("else_value") or raw_cw.get("else_label"),
            output_column=raw_cw.get("output_column", ""),
        )

    def _parse_expressions_raw(
        self, step_name: str, raw_step: dict
    ) -> list[ComputeStepExpression]:
        """从原始 YAML 字典解析 ComputeStepExpression 列表。"""
        raw_exprs = raw_step.get("expressions", [])
        if not raw_exprs:
            return []
        if not isinstance(raw_exprs, list):
            raise ParseError(
                ParseErrorCode.E001_YAML_PARSE_FAILED,
                f"compute_step '{step_name}' 的 expressions 必须是列表",
            )

        exprs: list[ComputeStepExpression] = []
        for ei, re in enumerate(raw_exprs):
            if not isinstance(re, dict):
                raise ParseError(
                    ParseErrorCode.E001_YAML_PARSE_FAILED,
                    f"compute_step '{step_name}' 的 expressions[{ei}] 必须是字典",
                )
            exprs.append(ComputeStepExpression(
                name=re.get("name", ""),
                expression=re.get("expression", ""),
                type=re.get("type", "double"),
            ))
        return exprs

    def _validate_seven_rejections(
        self,
        spec_dict: dict,
        input_tables: list[InputTableDecl],
        metrics: list[MetricDecl],
        joins: list[JoinDecl] | None,
        output_spec: OutputSpecDecl,
    ) -> None:
        """执行 7 项禁止宽松检查——抛出 ParseError。

        1. YAML metadata block 不存在或无法解析
           → 已在 _extract_fenced_block / _extract_yaml_front_matter 中检查
        2. input_tables 为空 → 已在 _parse_input_tables 中检查
        3. 指标引用未声明字段 → 已在 _parse_metrics 中检查
        4. 重复别名 → 已在 _parse_input_tables 中检查
        5. Join 引用不存在表 → 已在 _parse_joins 中检查
        6. 输出列为空 → 已在 _parse_output_spec 中检查
        7. 自由 SQL 字段 → 已在步骤 4 的 _check_all_forbidden_fields 中一次性完成

        此方法做整合检查——验证所有 rejection 都有对应的检测路径。
        不再调用 _check_forbidden_sql_fields，该检测已在 parse() 步骤 4 递归完成。
        """

    # ── 辅助方法 ──

    def _check_forbidden_sql_fields(self, raw: dict, context: str) -> None:
        """检查字典中是否出现了禁止的自由 SQL 字段名。

        compute_steps[*].expressions[*] 中的 expression 字段是合法例外——
        它属于 ComputeStepExpression 类型化模型，不是自由 SQL 逃逸字段。
        """
        # 检测是否在合法 expressions 上下文中（compute_steps[N].expressions[M]）
        _is_expression_item = ".expressions[" in context
        for key in raw:
            if key in self._FORBIDDEN_SQL_FIELDS:
                # expression 字段在 ComputeStepExpression 中是合法类型化字段
                if key == "expression" and _is_expression_item:
                    continue
                raise ParseError(
                    ParseErrorCode.E007_FREE_SQL_FIELD,
                    f"在 '{context}' 中发现禁止字段 '{key}'——"
                    f"所有 SQL 表达式必须通过类型化 IR 表达，不允许自由文本字段",
                    field_ref=key,
                )
            # 检查字段值是否包含 "expression: str" 这类模式
            if key == "type" and isinstance(raw[key], str) and raw[key] == "expression":
                raise ParseError(
                    ParseErrorCode.E007_FREE_SQL_FIELD,
                    f"在 '{context}' 中发现 expression 类型字段——不允许使用自由表达式",
                    field_ref=key,
                )

    def _check_all_forbidden_fields(self, data: Any, context: str = "spec") -> None:
        """递归遍历整个 spec 数据结构，一次性检测所有禁止字段。

        在进入任何子解析器之前调用——确保所有嵌套层级都已扫描，
        各子解析器不再需要单独检查。
        """
        if isinstance(data, dict):
            self._check_forbidden_sql_fields(data, context)
            for key, value in data.items():
                self._check_all_forbidden_fields(value, f"{context}.{key}")
        elif isinstance(data, list):
            for i, item in enumerate(data):
                self._check_all_forbidden_fields(item, f"{context}[{i}]")

    def _make_warning(
        self, warning_id: str, message: str, field_ref: str | None = None
    ) -> ParseWarning:
        """生成一个 ParseWarning 实例。"""
        self._warning_counter += 1
        severity = WarningSeverity.LOW
        # W001（类型缺失）和 W002（时间范围缺失）为 MEDIUM
        if warning_id in ("W001", "W002"):
            severity = WarningSeverity.MEDIUM
        return ParseWarning(
            warning_id=f"{warning_id}-{self._warning_counter:03d}",
            field_ref=field_ref,
            message=message,
            severity=severity,
        )

    def _normalized_spec_hash(self, spec: ParsedDeveloperSpec) -> str:
        """计算 normalized_spec_hash。

        排除 open_questions、parse_warnings 和 description 字段——
        这些是解析过程的副作用产物或自由文本，不应影响规范标识。

        使用 sort_keys=True 保证字段顺序无关。
        """
        # 使用 Pydantic 的 model_dump 序列化
        data = spec.model_dump(
            exclude={"open_questions", "parse_warnings", "description"},
            exclude_none=False,
        )
        # 同时排除 spec_hash 自身（避免循环依赖）
        data.pop("spec_hash", None)

        serialized = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
