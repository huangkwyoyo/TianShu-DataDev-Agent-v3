"""确定性 Markdown + YAML-like DeveloperSpec 解析器。

输入：包含 ```markdown fenced code block 的 Markdown 文本，block 内包含 YAML front matter。
输出：严格校验的 ParsedDeveloperSpec。

解析策略：
  1. 从输入文本中查找 ```markdown ... ``` fenced code block
  2. 在 block 内提取 --- ... --- YAML front matter
  3. 解析 YAML → 根据 spec: 键提取结构化数据 → Pydantic 模型
  4. 执行 6 项允许宽松 + 7 项禁止宽松检查
  5. 执行字段名归一化
  6. 计算 normalized_spec_hash（排除 open_questions、parse_warnings、description）
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any

import yaml

from .field_normalizer import FieldNormalizer
from .models import (
    AggregationType,
    ColumnDecl,
    DimensionDecl,
    FilterDecl,
    InputTableDecl,
    JoinDecl,
    JoinTypeEnum,
    MetricDecl,
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

        # 4. 解析各子部分
        input_tables = self._parse_input_tables(spec_dict.get("source_tables", []))
        metrics = self._parse_metrics(spec_dict.get("metrics", []), input_tables)
        dimensions = self._parse_dimensions(spec_dict.get("dimensions", []))
        joins = self._parse_joins(spec_dict.get("joins"), input_tables)
        time_range = self._parse_time_range(spec_dict.get("time_range"))
        output_spec = self._parse_output_spec(spec_dict)

        # 5. 提取标题
        title = self._extract_title(md_body) or spec_dict.get("summary", "Untitled")

        # 6. 组装描述
        summary = spec_dict.get("summary", "")
        description_parts = [p for p in [summary, md_body] if p]
        description = "\n\n".join(description_parts)

        # 7. 执行允许/禁止检查
        open_questions: list[OpenQuestion] = []
        parse_warnings: list[ParseWarning] = []

        self._validate_seven_rejections(spec_dict, input_tables, metrics, joins, output_spec)
        parse_warnings.extend(self._validate_six_allowances(spec_dict, input_tables, joins, time_range))

        # 8. 构建 ParsedDeveloperSpec
        spec_id = self._build_spec_id(input_tables)
        spec = ParsedDeveloperSpec(
            spec_id=spec_id,
            spec_hash="",  # 先占位，计算 hash 后再填入
            title=title,
            description=description,
            input_tables=input_tables,
            metrics=metrics,
            dimensions=dimensions,
            joins=joins,
            time_range=time_range,
            output_spec=output_spec,
            open_questions=open_questions,
            parse_warnings=parse_warnings,
        )

        # 9. 计算并回填 normalized_spec_hash
        spec_hash = self._normalized_spec_hash(spec)
        # 使用 object.__setattr__ 绕过 frozen 检查
        object.__setattr__(spec, "spec_hash", spec_hash)

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
          - raw_sql 等自由 SQL 字段 → E007
        """
        if not raw_tables:
            raise ParseError(
                ParseErrorCode.E002_MISSING_REQUIRED_FIELD,
                "input_tables 不能为空——必须声明至少一个源表",
            )

        tables: list[InputTableDecl] = []
        seen_aliases: set[str] = set()

        for raw in raw_tables:
            # 检查自由 SQL 字段
            self._check_forbidden_sql_fields(raw, f"source_table {raw.get('name', '?')}")

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
            # 检查自由 SQL 字段
            self._check_forbidden_sql_fields(raw, f"{context}.{raw.get('name', '?')}")

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
            self._check_forbidden_sql_fields(raw, f"filter on {table_alias}")
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
            self._check_forbidden_sql_fields(raw, f"metric {raw.get('metric_name', '?')}")

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

            metrics.append(MetricDecl(
                metric_name=raw.get("metric_name", ""),
                aggregation=aggregation,
                input_column=input_col,
                alias=raw.get("alias", raw.get("metric_name", "")),
            ))

        return metrics

    def _parse_dimensions(self, raw_dimensions: list[dict]) -> list[DimensionDecl]:
        """解析 dimensions 列表。"""
        if not raw_dimensions:
            return []
        dimensions: list[DimensionDecl] = []
        for raw in raw_dimensions:
            if not isinstance(raw, dict):
                continue
            self._check_forbidden_sql_fields(raw, f"dimension {raw.get('dimension_name', '?')}")
            dimensions.append(DimensionDecl(
                dimension_name=raw.get("dimension_name", ""),
                column_ref=raw.get("column_ref", ""),
            ))
        return dimensions

    def _parse_joins(
        self, raw_joins: list[dict] | None, tables: list[InputTableDecl]
    ) -> list[JoinDecl] | None:
        """解析 joins 列表。

        None → 允许宽松（留空由 RelationshipHypothesis 推理）。
        7 项禁止检查：引用不存在的表别名 → E005。
        """
        if raw_joins is None:
            return None
        if not raw_joins:
            return []

        valid_aliases = {t.table_alias for t in tables}
        joins: list[JoinDecl] = []
        for raw in raw_joins:
            if not isinstance(raw, dict):
                continue
            ctx = f"join {raw.get('left_table', '?')}-{raw.get('right_table', '?')}"
            self._check_forbidden_sql_fields(raw, ctx)

            left = raw.get("left_table", "")
            right = raw.get("right_table", "")

            # 检查引用的表别名是否存在
            for side, alias in [("left", left), ("right", right)]:
                if alias and alias not in valid_aliases:
                    raise ParseError(
                        ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS,
                        f"Join {side}_table '{alias}' 不在已声明的 input_tables 中——"
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
        """解析 time_range 声明——None 时允许宽松。"""
        if raw is None:
            return None
        if not isinstance(raw, dict):
            return None
        self._check_forbidden_sql_fields(raw, "time_range")
        return TimeRangeDecl(
            column_ref=raw.get("column_ref", ""),
            start=str(raw.get("start", "")),
            end=str(raw.get("end", "")),
            inclusive=raw.get("inclusive", True),
        )

    def _parse_output_spec(self, spec_dict: dict) -> OutputSpecDecl:
        """解析输出规格。

        7 项禁止检查：output_columns 为空 → E006。
        """
        # 从 output_columns 提取列名列表
        raw_output_cols = spec_dict.get("output_columns", []) or []
        columns: list[str] = []
        for col in raw_output_cols:
            if isinstance(col, dict):
                # 检查每个输出列是否包含禁止的 SQL 字段
                self._check_forbidden_sql_fields(col, f"output_column {col.get('name', '?')}")
                name = col.get("name", "")
                if name:
                    columns.append(name)
            elif isinstance(col, str):
                columns.append(col)

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
        7. 自由 SQL 字段 → 在各子解析器的 _check_forbidden_sql_fields 中检查

        此方法做整合检查——验证所有 rejection 都有对应的检测路径。
        """
        # 额外检查：spec 顶层字典中也不应存在自由 SQL 字段
        self._check_forbidden_sql_fields(spec_dict, "spec")

    # ── 辅助方法 ──

    def _check_forbidden_sql_fields(self, raw: dict, context: str) -> None:
        """检查字典中是否出现了禁止的自由 SQL 字段名。"""
        for key in raw:
            if key in self._FORBIDDEN_SQL_FIELDS:
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

    def _build_spec_id(self, tables: list[InputTableDecl]) -> str:
        """生成确定性 spec_id。

        基于源表名排序后的 SHA-256 前 12 位 hex。
        无表时使用 UUID4。
        """
        if not tables:
            return f"spec_{uuid.uuid4().hex[:12]}"
        sorted_names = sorted(t.source_table for t in tables)
        digest = hashlib.sha256("|".join(sorted_names).encode()).hexdigest()
        return f"spec_{digest[:12]}"

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
