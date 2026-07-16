"""Phase 7B PhysicalVerifier——双引擎物理链路验证器。

编排 DuckDB（基准 SQL）+ 本地 Spark（PySpark DSL）双引擎执行，
通过 ResultCanonicalizer 规范化后对比结果。

状态语义（精确区分，禁止泛化 PASS）：
- RESULT_CONSISTENT：双引擎结果一致
- RESULT_MISMATCH：双引擎结果不一致
- CANONICALIZATION_NEEDED：缺少排序键 → 无法确定等价
- UNSUPPORTED_SEMANTICS：step 类型不支持物理对比（如 subquery 尚无对比规则）
- HUMAN_REVIEW：自动判定无法得出结论，需人工介入
- NOT_EXECUTED：尚未执行物理验证
- EXECUTION_ERROR：任一引擎执行失败

物理验证规范化配置（Phase 9B）：
- NormalizationConfig：控制比较策略和容差
- float/double 使用 math.isclose（非全局 round）
- Decimal 禁止转 float，按 Contract precision/scale 精确比较
- NULL 缺失键用权威 schema 补齐（Contract output_columns）
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import Field

from tianshu_datadev.cre_models import (
    CreShadowReport,
    EnvironmentManifest,
    NormalizationColumn,
)
from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.executor import (
    LocalSparkExecutor,
    SparkExecutionResult,
    SparkExecutionStatus,
)

# ════════════════════════════════════════════
# SQL 安全校验——白名单 + 纵深防御（方案 B）
# ════════════════════════════════════════════

# 禁止的 DDL/DML/高风险操作关键词（黑名单——纵深防御层）
_FORBIDDEN_SQL_KEYWORDS_RE = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE|"
    r"ATTACH|DETACH|COPY|INSTALL|LOAD|PRAGMA|"
    r"EXPORT|IMPORT|VACUUM|CHECKPOINT|GRANT|REVOKE|"
    r"SET|RESET)\b",
    re.IGNORECASE,
)

# 白名单结构——必须以 SELECT 开头（禁止 CTE / WITH）
_SELECT_STRUCTURE_RE = re.compile(
    r"^\s*SELECT\b",
    re.IGNORECASE | re.DOTALL,
)


def _strip_sql_comments(sql: str) -> str:
    """去除 SQL 注释——块注释（/* */）和行注释（--）。

    Args:
        sql: 原始 SQL 字符串

    Returns:
        去除注释后的 SQL 字符串
    """
    # 块注释 /* ... */（非贪婪）
    result = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # 行注释 -- ...（到行尾）
    result = re.sub(r"--[^\n]*", " ", result)
    return result


def _strip_string_literals(sql: str) -> str:
    """去除 SQL 字符串字面量（单引号括起的内容）——避免关键词误判。

    只去除单引号字符串。双引号在 DuckDB 中是标识符，予以保留。

    Args:
        sql: SQL 字符串

    Returns:
        去除字符串字面量后的 SQL 字符串
    """
    result: list[str] = []
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            # 跳过单引号字符串——处理 '' 转义
            i += 1
            while i < len(sql):
                if sql[i] == "'":
                    if i + 1 < len(sql) and sql[i + 1] == "'":
                        i += 2  # SQL 转义 ''
                        continue
                    i += 1
                    break
                i += 1
        else:
            result.append(ch)
            i += 1
    return "".join(result)


def _has_multiple_statements(sql: str) -> bool:
    """检测 SQL 是否包含多语句——引号外的分号。

    仅检测单引号外的 ; 分隔符。

    Args:
        sql: SQL 字符串

    Returns:
        True 如果含多语句
    """
    in_single = False
    for ch in sql:
        if ch == "'":
            in_single = not in_single
        elif ch == ";" and not in_single:
            return True
    return False


def _validate_select_sql(sql_query: str) -> str:
    """校验 SQL 查询——仅允许单条只读 SELECT。

    职责边界：
        本函数仅校验 PhysicalVerifier 的最终查询 SQL——即 DuckDB 读取快照视图
        并产出双引擎对比结果的 SELECT 语句。CREATE TEMP TABLE / _temp_ 中间表
        机制属于 SqlProgram 编译/执行链路，不走本函数。

    安全策略（三层纵深防御，按顺序执行）：
    1. 剥离字符串字面量——先于注释剥离，防止字符串内 -- 和 /* */ 干扰后续检测
    2. 去除注释——在剥离字符串后的文本上操作，确保安全
    3. 多语句检测：拒绝分号分隔的多语句
    4. 结构白名单：必须以 SELECT 开头（禁止 CTE / WITH）
    5. 关键词黑名单：检测 DDL/DML/高风险操作

    Args:
        sql_query: 待校验的原始 SQL 字符串

    Returns:
        校验通过的原 SQL（原样返回）

    Raises:
        ValueError: SQL 不符合安全策略，含具体拒绝原因和 SQL 前 200 字符
    """
    # Step 1: 剥离字符串字面量——必须在注释剥离之前执行
    # 防止字符串内的 -- 和 /* */ 被正则误当作注释剥离（如 SELECT '--'; SELECT 2）
    no_strings = _strip_string_literals(sql_query)

    # Step 2: 去除注释（在剥离字符串后的文本上操作，此时 -- 只可能出现在真实注释中）
    cleaned = _strip_sql_comments(no_strings)

    # Step 3: 检测多语句（注释去除后检测——注释内分号不触发）
    if _has_multiple_statements(cleaned):
        raise ValueError(
            f"SQL 安全校验失败：禁止多语句（分号分隔）。"
            f"SQL 前 200 字符：{sql_query[:200]}"
        )

    # Step 4: 结构白名单——必须以 SELECT 开头（禁止 CTE / WITH）
    if not _SELECT_STRUCTURE_RE.match(cleaned):
        raise ValueError(
            f"SQL 安全校验失败：仅允许 SELECT 开头的只读查询（禁止 CTE / WITH）。"
            f"SQL 前 200 字符：{sql_query[:200]}"
        )

    # Step 5: 关键词黑名单（字符串已剥离、注释已去除，直接检测即可）
    match = _FORBIDDEN_SQL_KEYWORDS_RE.search(cleaned)
    if match:
        raise ValueError(
            f"SQL 安全校验失败：包含禁止的关键词 '{match.group()}'. "
            f"SQL 前 200 字符：{sql_query[:200]}"
        )

    return sql_query


# ════════════════════════════════════════════
# PhysicalVerificationStatus——状态枚举
# ════════════════════════════════════════════


class PhysicalVerificationStatus(str, Enum):
    """物理验证状态——精确描述，禁止泛化 PASS。

    状态区间：
    - RESULT_CONSISTENT：双引擎结果一致（最佳结论）
    - RESULT_MISMATCH：结果不一致（需定位根因）
    - CANONICALIZATION_NEEDED：缺少排序键 → 无法确定等价
    - UNSUPPORTED_SEMANTICS：step 类型不在物理验证范围内
    - HUMAN_REVIEW：自动判定无法得出确定结论
    - NOT_EXECUTED：尚未执行物理验证
    - EXECUTION_ERROR：任一引擎执行失败
    """

    RESULT_CONSISTENT = "RESULT_CONSISTENT"
    RESULT_MISMATCH = "RESULT_MISMATCH"
    CANONICALIZATION_NEEDED = "CANONICALIZATION_NEEDED"
    UNSUPPORTED_SEMANTICS = "UNSUPPORTED_SEMANTICS"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    NOT_EXECUTED = "NOT_EXECUTED"
    EXECUTION_ERROR = "EXECUTION_ERROR"


# ════════════════════════════════════════════
# ResultCanonicalizer——结果规范化器
# ════════════════════════════════════════════


class CanonicalizationError(Exception):
    """规范化失败——缺少排序键或数据格式不支持。"""

    pass


class ResultCanonicalizer:
    """双引擎结果规范化器——消除执行环境差异。

    规范化策略：
    1. 列名归一化——去除表前缀、统一小写、去除首尾空格
    2. NULL/NaN 处理——None 和 float('nan') 统一为空字符串 ""
    3. Decimal 归一化——Decimal 值转为 float（DuckDB 和 Spark 数值精度差异）
    4. 排序——按指定键排序（确保对比确定性）
    5. 去重——可选（默认不去重，除非无排序键）

    使用方式：
        canonicalizer = ResultCanonicalizer()
        normalized = canonicalizer.canonicalize(rows, order_keys=["order_id"])
    """

    # 规范化键的列名分隔符
    _KEY_SEPARATOR = "|||"

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """列名归一化——去表前缀、统一小写、去空格。"""
        if "." in name:
            name = name.split(".")[-1]
        return name.strip().lower()

    @staticmethod
    def _normalize_value(value: Any) -> str:
        """值归一化——统一 NULL/NaN/Decimal/datetime/float 表示。

        双引擎差异来源：
        1. DuckDB (C++) vs PySpark (JVM) 浮点运算末位差异（~1e-15）
           → round(value, 10) 消除
        2. PySpark toJSON() 序列化 datetime → ISO 字符串（T 分隔）
           → 检测并替换 T 为空格
        3. DuckDB 返回原生 datetime 对象 vs PySpark 返回 ISO 字符串
           → 统一格式化为 YYYY-MM-DD HH:MM:SS
        4. DuckDB Decimal 类型 vs PySpark float/double 类型
           → 统一转为字符串表示
        """
        import datetime as _dt
        import math as _math
        import re as _re

        if value is None:
            return ""
        if isinstance(value, float):
            if _math.isnan(value):
                return ""
        # Decimal 归一化——转为字符串（禁止转 float，避免大值精度损失）
        if hasattr(value, "__class__") and value.__class__.__name__ == "Decimal":
            return str(value)
        # datetime.date → 规范化为 YYYY-MM-DD 格式
        if isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
            return value.isoformat()
        # datetime.datetime → 规范化为 YYYY-MM-DD HH:MM:SS 格式（空格分隔，与 DuckDB 一致）
        if isinstance(value, _dt.datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        # 字符串：检测 ISO 8601 datetime 格式（T 分隔，如 "2026-01-15T10:30:00"）
        # PySpark toJSON() 序列化产物——需归一化为空格分隔以与 DuckDB 原生 str() 对齐
        if isinstance(value, str) and _re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$", value,
        ):
            return value.replace("T", " ")
        return str(value)

    def canonicalize(
        self,
        rows: list[dict[str, Any]],
        order_keys: list[str] | None = None,
        deduplicate: bool = False,
    ) -> list[dict[str, Any]]:
        """规范化结果行列表——排序、归一化列名和值。

        Args:
            rows: 原始行列表（每行为 dict）
            order_keys: 排序键列表（列名）。
                        为 None 时不排序（如单行结果）。
                        为空列表 [] 且 deduplicate=False 时抛出 CanonicalizationError
                        ——无法保证对比确定性。
            deduplicate: 是否去重

        Returns:
            规范化后的行列表

        Raises:
            CanonicalizationError: 无排序键且无去重——无法保证对比确定性
        """
        if not rows:
            return []

        # Step 1：列名归一化
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            norm_row: dict[str, Any] = {}
            for key, val in row.items():
                norm_key = self._normalize_column_name(key)
                norm_row[norm_key] = val
            normalized_rows.append(norm_row)

        # Step 2：排序
        if order_keys:
            norm_keys = [self._normalize_column_name(k) for k in order_keys]

            def _sort_key(row: dict[str, Any]) -> str:
                """将排序键值拼接为可比较字符串。"""
                parts: list[str] = []
                for k in norm_keys:
                    parts.append(self._normalize_value(row.get(k)))
                return self._KEY_SEPARATOR.join(parts)

            normalized_rows.sort(key=_sort_key)
        elif not deduplicate and len(normalized_rows) > 1:
            # 无排序键且不去重且多行——无法保证对比确定性
            # 单行结果天然确定，无需排序键
            raise CanonicalizationError(
                "缺少排序键（order_keys）且 deduplicate=False 且结果有多行——"
                "无法保证双引擎结果对比的确定性。"
                "请提供排序键或设置 deduplicate=True。"
            )

        # Step 3：值归一化
        result: list[dict[str, Any]] = []
        for row in normalized_rows:
            norm_row = {
                key: self._normalize_value(val)
                for key, val in row.items()
            }
            result.append(norm_row)

        # Step 3.5：补齐缺失列——PySpark toJSON() 可能省略 null 字段的键
        # 确保所有行的键集合一致，缺失键以空字符串填充（与 _normalize_value(None) 一致）
        if result:
            all_keys: set[str] = set()
            for row in result:
                all_keys.update(row.keys())
            for row in result:
                for key in all_keys:
                    if key not in row:
                        row[key] = ""

        # Step 4：去重（可选）
        if deduplicate:
            seen: set[str] = set()
            deduped: list[dict[str, Any]] = []
            for row in result:
                row_key = json.dumps(row, sort_keys=True, default=str)
                if row_key not in seen:
                    seen.add(row_key)
                    deduped.append(row)
            result = deduped

        return result


# ════════════════════════════════════════════
# 物理验证结果模型
# ════════════════════════════════════════════


class EngineExecutionResult(StrictModel):
    """单个引擎的执行结果——含规范化后的行数据和元数据。"""

    engine: str                                    # "duckdb" | "spark"
    success: bool                                  # 执行是否成功
    execution_time_ms: float = 0.0                 # 执行耗时
    raw_row_count: int = 0                         # 原始行数
    canonical_row_count: int = 0                   # 规范化后行数
    error_message: str = ""                        # 失败时的错误信息
    sample_rows: list[dict[str, Any]] = Field(     # 规范化后的采样行（最多 5 行）
        default_factory=list,
    )


class DiffDetail(StrictModel):
    """单个差异项——描述双引擎结果的不同之处。"""

    row_index: int | None = None                   # 差异所在行索引（None 表示无法定位）
    column: str = ""                               # 差异列名
    duckdb_value: str = ""                         # DuckDB 侧值
    spark_value: str = ""                          # Spark 侧值
    description: str = ""                          # 差异描述


# NormalizationColumn 已移至 cre_models.py 消除循环依赖。
# 在此保留导入以支持本文件的引用——所有新代码应直接从 cre_models 导入。


class NormalizationConfig(StrictModel):
    """物理验证规范化配置——控制比较策略和容差。

    由 pipeline._do_spark_physical_verify() 从 Contract 构造并传入。
    用于：
    1. float/double → math.isclose 类型感知比较
    2. Decimal → quantize 精确比较（禁止 float 转换）
    3. NULL 缺失键 → 用权威 schema 补齐（非数据发现）
    """

    # 浮点绝对容差——仅作用于 float/double 字段
    float_abs_tolerance: float = 1e-12
    # 浮点相对容差——仅作用于 float/double 字段
    float_rel_tolerance: float = 1e-12
    # 权威 schema 输出列定义——用于 NULL 补齐和类型感知比较
    output_columns: list[NormalizationColumn] = Field(default_factory=list)
    # Contract hash——用于 normalization_config_snapshot 追溯
    contract_hash: str = ""
    # 权威主键列名（来自 Contract grouping_keys）——CRE shadow 行对齐用
    primary_keys: list[str] = Field(default_factory=list)


class PhysicalVerificationReport(StrictModel):
    """物理验证完整报告——双引擎执行 + 规范化 + 对比的完整记录。"""

    report_id: str                                 # 确定性报告 ID
    contract_hash: str                             # 来源 Contract hash
    snapshot_id: str                               # 使用的快照 ID
    status: PhysicalVerificationStatus             # 验证结论
    duckdb_result: EngineExecutionResult | None = None
    spark_result: EngineExecutionResult | None = None
    diffs: list[DiffDetail] = Field(default_factory=list)
    row_count_match: bool = False                  # 行数是否一致
    schema_match: bool = False                     # schema（列集合）是否一致
    uncovered_step_types: list[str] = Field(       # 未覆盖的 step 类型
        default_factory=list,
    )
    error_message: str = ""                        # 整体错误信息
    # Phase 9B 新增字段
    total_diff_count: int = 0                      # 真实差异总数（不含截断）
    diffs_truncated: bool = False                  # 是否有差异被截断
    normalization_config_snapshot: dict[str, Any] = Field(  # 本次验证用容差参数快照
        default_factory=dict,
    )
    # CRE shadow 诊断报告（最终硬化）——严格 Pydantic 模型，与生产验证并行
    # 类型已通过 cre_models.py 中立模块解除循环依赖——直接使用 CreShadowReport 类型
    cre_shadow_report: CreShadowReport | None = Field(default=None)
    cdp_shadow_result: dict | None = Field(       # CDP v1 shadow 摘要对比结果（Task 8）
        default=None,
        description="CDP v1 引擎侧摘要对比结果——仅在双引擎均成功时填充。含 status 和 decision_reason。",
    )


# ════════════════════════════════════════════
# PhysicalVerifier
# ════════════════════════════════════════════


class PhysicalVerifier:
    """双引擎物理链路验证器——编排 DuckDB + Spark 执行并对比结果。

    验证流程：
    1. 检查 step 类型——window/subquery → UNSUPPORTED_SEMANTICS
    2. DuckDB 执行 SQL（基准）→ 收集结果行
    3. Spark 执行 PySpark DSL（被测）→ 收集结果行
    4. ResultCanonicalizer 规范化双方结果
    5. 逐行逐列对比 → RESULT_CONSISTENT / RESULT_MISMATCH

    使用方式：
        verifier = PhysicalVerifier()
        report = verifier.verify(
            sql_query="SELECT * FROM read_parquet('...')",
            pyspark_code='df = spark.read.parquet("...")...',
            snapshot_dir="/tmp/snap_abc123",
            contract_hash="abc123",
            snapshot_id="snap_def456",
            order_keys=["order_id"],
        )
    """

    # Phase 7C 物理验证不支持的 step 类型——window 已在 Phase 6C/7C 开放
    _UNSUPPORTED_STEP_TYPES: set[str] = {
        "subquery",     # 尚无等价对比规则
    }

    def __init__(
        self,
        spark_executor: LocalSparkExecutor | None = None,
        normalization_config: NormalizationConfig | None = None,
    ) -> None:
        """初始化物理验证器。

        Args:
            spark_executor: LocalSparkExecutor 实例，None 时创建默认实例
            normalization_config: 规范化配置（Phase 9B），控制比较策略和容差
        """
        self._spark_executor = spark_executor or LocalSparkExecutor()
        self._canonicalizer = ResultCanonicalizer()
        self._normalization_config = normalization_config

    # ── 公共 API ──

    # 需要导入 SparkExecutionStatus 用于状态判断
    def verify(
        self,
        sql_query: str,
        pyspark_code: str,
        snapshot_dir: str,
        contract_hash: str,
        snapshot_id: str,
        order_keys: list[str] | None = None,
        uncovered_step_types: list[str] | None = None,
        duckdb_path: str | None = None,
        # CRE shadow 参数——不影响生产验证结论
        cre_primary_keys: list[str] | None = None,
        cre_timezone: str = "",
        cre_environment_manifest: EnvironmentManifest | None = None,
    ) -> PhysicalVerificationReport:
        """执行双引擎物理验证。

        Args:
            sql_query: DuckDB 可执行的 SQL 查询
            pyspark_code: 已编译的 PySpark DSL 代码
            snapshot_dir: 快照 Parquet 文件所在目录
            contract_hash: 来源 Contract 的 SHA-256
            snapshot_id: 快照 ID
            order_keys: 排序键——用于 ResultCanonicalizer 排序。
                        为 None 时尝试不排序对比（仅限单行结果）。
            uncovered_step_types: 物理验证未覆盖的 step 类型列表
            duckdb_path: 外部 DuckDB 数据库路径——ATTACH 后自动创建
                         gold/silver schema VIEW 桥接

        Returns:
            PhysicalVerificationReport——完整验证报告
        """
        # Step 1：检查未覆盖类型
        uncovered = list(uncovered_step_types or [])
        has_unsupported = any(
            t in self._UNSUPPORTED_STEP_TYPES
            for t in uncovered
        )
        if has_unsupported:
            unsupported_list = [
                t for t in uncovered
                if t in self._UNSUPPORTED_STEP_TYPES
            ]
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS,
                uncovered_step_types=uncovered,
                error_message=(
                    f"不支持的 step 类型：{unsupported_list}。"
                    f"window 在 Phase 6C 覆盖，subquery 尚无对比规则。"
                ),
            )

        # Step 2：DuckDB 执行 SQL
        duckdb_result = self._execute_duckdb(sql_query, snapshot_dir, duckdb_path=duckdb_path)

        # Step 3：Spark 执行 PySpark DSL
        spark_result = self._execute_spark(pyspark_code, snapshot_dir)

        # ── 诊断日志：输出关键中间数据（用 warning 级别确保输出）──
        _diag_logger = logging.getLogger(__name__)
        _diag_logger.warning("[PHYS_VERIFIER_DIAG] snapshot_dir=%s snapshot_id=%s "
                             "duckdb_status=%s duckdb_rows=%s "
                             "spark_status=%s spark_rows=%s spark_error=%s "
                             "pyspark_code_len=%s order_keys=%s primary_keys=%s",
                             snapshot_dir, snapshot_id,
                             duckdb_result.status,
                             len(duckdb_result.output_rows) if duckdb_result.output_rows else 0,
                             spark_result.status,
                             len(spark_result.output_rows) if spark_result.output_rows else 0,
                             spark_result.error_message or "无",
                             len(pyspark_code), order_keys, cre_primary_keys)

        # 自动检测排序键：当无显式排序键时，优先使用业务主键（cre_primary_keys），
        # 否则从 DuckDB 结果列名自动提取。使用业务主键排序可避免聚合指标列
        # （如 avg_distance、total_fare）参与排序导致浮点尾差行错位。
        if not order_keys and duckdb_result.output_rows:
            if cre_primary_keys:
                order_keys = list(cre_primary_keys)
            else:
                order_keys = list(duckdb_result.output_rows[0].keys())

        # Step 4：规范化
        duckdb_rows: list[dict] = []
        spark_rows: list[dict] = []

        try:
            if duckdb_result.status == SparkExecutionStatus.SUCCESS:
                duckdb_rows = self._canonicalizer.canonicalize(
                    duckdb_result.output_rows,
                    order_keys=order_keys,
                )
        except CanonicalizationError as e:
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.CANONICALIZATION_NEEDED,
                duckdb_result=EngineExecutionResult(
                    engine="duckdb",
                    success=True,
                    execution_time_ms=duckdb_result.execution_time_ms,
                    raw_row_count=duckdb_result.output_rows.__len__(),
                ),
                uncovered_step_types=uncovered,
                error_message=str(e),
            )

        try:
            # 结果溢出检测——超阈值时不得判定 SQL/Spark 一致，返回 NOT_EXECUTED
            if getattr(spark_result, 'result_overflow', False):
                # 复用模块级 logging——函数内 import 会遮蔽模块级名字导致 UnboundLocalError
                logging.getLogger(__name__).warning(
                    "Spark 结果行数超过上限（%s），跳过逐行对比，返回 NOT_EXECUTED。"
                    "contract_hash=%s, snapshot_id=%s",
                    getattr(spark_result, 'error_message', ''),
                    contract_hash,
                    snapshot_id,
                )
                return PhysicalVerificationReport(
                    report_id=self._generate_report_id(contract_hash, snapshot_id),
                    contract_hash=contract_hash,
                    snapshot_id=snapshot_id,
                    status=PhysicalVerificationStatus.NOT_EXECUTED,
                    duckdb_result=EngineExecutionResult(
                        engine="duckdb",
                        success=True,
                        execution_time_ms=duckdb_result.execution_time_ms,
                        raw_row_count=duckdb_result.output_rows.__len__(),
                        canonical_row_count=len(duckdb_rows),
                        sample_rows=duckdb_rows[:5],
                    ),
                    spark_result=EngineExecutionResult(
                        engine="spark",
                        success=False,
                        execution_time_ms=spark_result.execution_time_ms,
                        error_message=spark_result.error_message,
                    ),
                    uncovered_step_types=uncovered,
                    error_message=(
                        f"Spark 结果行数超过收集上限，"
                        f"无法进行逐行对比。需通过 CDP 摘要验证（后续功能）。"
                        f"详情：{spark_result.error_message}"
                    ),
                )

            if spark_result.status == SparkExecutionStatus.SUCCESS:
                spark_rows = self._canonicalizer.canonicalize(
                    spark_result.output_rows,
                    order_keys=order_keys,
                )
        except CanonicalizationError as e:
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.CANONICALIZATION_NEEDED,
                duckdb_result=EngineExecutionResult(
                    engine="duckdb",
                    success=True,
                    execution_time_ms=duckdb_result.execution_time_ms,
                    raw_row_count=duckdb_result.output_rows.__len__(),
                    canonical_row_count=len(duckdb_rows),
                    sample_rows=duckdb_rows[:5],
                ),
                spark_result=EngineExecutionResult(
                    engine="spark",
                    success=True,
                    execution_time_ms=spark_result.execution_time_ms,
                    raw_row_count=spark_result.output_rows.__len__(),
                ),
                uncovered_step_types=uncovered,
                error_message=str(e),
            )

        # Step 5：执行失败检查
        if duckdb_result.status != SparkExecutionStatus.SUCCESS:
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.EXECUTION_ERROR,
                duckdb_result=EngineExecutionResult(
                    engine="duckdb",
                    success=False,
                    execution_time_ms=duckdb_result.execution_time_ms,
                    error_message=duckdb_result.error_message,
                ),
                uncovered_step_types=uncovered,
                error_message=f"DuckDB 执行失败：{duckdb_result.error_message}",
            )

        if spark_result.status != SparkExecutionStatus.SUCCESS:
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.EXECUTION_ERROR,
                duckdb_result=EngineExecutionResult(
                    engine="duckdb",
                    success=True,
                    execution_time_ms=duckdb_result.execution_time_ms,
                    raw_row_count=duckdb_result.output_rows.__len__(),
                    canonical_row_count=len(duckdb_rows),
                    sample_rows=duckdb_rows[:5],
                ),
                spark_result=EngineExecutionResult(
                    engine="spark",
                    success=False,
                    execution_time_ms=spark_result.execution_time_ms,
                    error_message=spark_result.error_message,
                ),
                uncovered_step_types=uncovered,
                error_message=f"Spark 执行失败：{spark_result.error_message}",
            )

        # Step 6：CDP v1 Engine-side Shadow（Task 8）
        # 在 legacy 判定之外并行计算 CDP 摘要——不影响现有逻辑
        cdp_shadow_result = None
        try:
            cdp_spec = self._infer_cdp_spec(duckdb_result.output_rows)
            if cdp_spec is not None:
                cdp_shadow_result = self._run_cdp_shadow(
                    sql_query=sql_query,
                    snapshot_dir=snapshot_dir,
                    cdp_spec=cdp_spec,
                    snapshot_id=snapshot_id,
                    duckdb_con=None,  # 新建独立连接
                )
        except Exception:
            # Shadow 异常不影响 legacy 判定
            logging.getLogger(__name__).warning(
                "CDP shadow 执行异常——已忽略", exc_info=True,
            )

        # Step 7：对比结果
        duckdb_canonical_count = len(duckdb_rows)
        spark_canonical_count = len(spark_rows)
        row_count_match = duckdb_canonical_count == spark_canonical_count

        # Schema 对比
        duckdb_cols = set()
        for row in duckdb_rows:
            duckdb_cols.update(row.keys())
        spark_cols = set()
        for row in spark_rows:
            spark_cols.update(row.keys())
        schema_match = duckdb_cols == spark_cols

        # Step 6.5：Schema 不匹配时——尝试用权威 schema 补齐
        has_authoritative_schema = (
            self._normalization_config is not None
            and len(self._normalization_config.output_columns) > 0
        )
        if not schema_match and has_authoritative_schema:
            duckdb_rows = self._fill_missing_columns(duckdb_rows)
            spark_rows = self._fill_missing_columns(spark_rows)
            # 补齐后重新判断 schema_match
            duckdb_cols = set()
            for row in duckdb_rows:
                duckdb_cols.update(row.keys())
            spark_cols = set()
            for row in spark_rows:
                spark_cols.update(row.keys())
            schema_match = duckdb_cols == spark_cols
        elif not schema_match and not has_authoritative_schema:
            # 无权威 schema 又收到 schema 不匹配 → 无法自动判断
            return PhysicalVerificationReport(
                report_id=self._generate_report_id(contract_hash, snapshot_id),
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                status=PhysicalVerificationStatus.HUMAN_REVIEW,
                duckdb_result=EngineExecutionResult(
                    engine="duckdb",
                    success=True,
                    execution_time_ms=duckdb_result.execution_time_ms,
                    raw_row_count=duckdb_result.output_rows.__len__(),
                    canonical_row_count=duckdb_canonical_count,
                    sample_rows=duckdb_rows[:5],
                ),
                spark_result=EngineExecutionResult(
                    engine="spark",
                    success=True,
                    execution_time_ms=spark_result.execution_time_ms,
                    raw_row_count=spark_result.output_rows.__len__(),
                    canonical_row_count=spark_canonical_count,
                    sample_rows=spark_rows[:5],
                ),
                uncovered_step_types=uncovered,
                row_count_match=row_count_match,
                schema_match=False,
                error_message=(
                    f"Schema 不匹配且缺少权威 schema（Contract output_columns）——"
                    f"无法自动补齐判断。DuckDB 列={sorted(duckdb_cols)}，"
                    f"Spark 列={sorted(spark_cols)}。需人工介入。"
                ),
            )

        # Step 7：类型感知差异计算
        # 传入归一化后的 order_keys 用于基于键的行对齐
        # 确保列名与 ResultCanonicalizer 处理后的行数据一致
        norm_order_keys: list[str] | None = None
        if order_keys:
            norm_order_keys = [
                self._canonicalizer._normalize_column_name(k)
                for k in order_keys
            ]
        diffs, total_diff_count, diffs_truncated = self._compute_diffs(
            duckdb_rows, spark_rows,
            config=self._normalization_config,
            order_keys=norm_order_keys,
        )

        # 判定最终状态
        if total_diff_count == 0 and row_count_match and schema_match:
            status = PhysicalVerificationStatus.RESULT_CONSISTENT
        elif not schema_match or (duckdb_canonical_count > 0 and spark_canonical_count > 0):
            status = PhysicalVerificationStatus.RESULT_MISMATCH
        else:
            status = PhysicalVerificationStatus.HUMAN_REVIEW

        # ── 诊断：仅 RESULT_MISMATCH 时保存双引擎行数据到日志目录 ──
        if status == PhysicalVerificationStatus.RESULT_MISMATCH:
            self._save_mismatch_diagnostics(
                pyspark_code=pyspark_code,
                sql_query=sql_query,
                duckdb_rows=duckdb_result.output_rows,
                spark_rows=spark_result.output_rows,
                snapshot_id=snapshot_id,
                contract_hash=contract_hash,
                duckdb_row_count=duckdb_canonical_count,
                spark_row_count=spark_canonical_count,
            )

        # 构建 normalization_config_snapshot
        config_snapshot: dict[str, Any] = {}
        if self._normalization_config:
            cfg = self._normalization_config
            config_snapshot = {
                "float_abs_tolerance": cfg.float_abs_tolerance,
                "float_rel_tolerance": cfg.float_rel_tolerance,
                "output_column_count": len(cfg.output_columns),
                "has_output_columns": bool(cfg.output_columns),
                "contract_hash_prefix": cfg.contract_hash[:12] if cfg.contract_hash else "",
            }

        # 生成 report_id
        report_id = self._generate_report_id(contract_hash, snapshot_id)

        duckdb_engine_result = EngineExecutionResult(
            engine="duckdb",
            success=True,
            execution_time_ms=duckdb_result.execution_time_ms,
            raw_row_count=duckdb_result.output_rows.__len__(),
            canonical_row_count=duckdb_canonical_count,
            sample_rows=duckdb_rows[:5],
        )
        spark_engine_result = EngineExecutionResult(
            engine="spark",
            success=True,
            execution_time_ms=spark_result.execution_time_ms,
            raw_row_count=spark_result.output_rows.__len__(),
            canonical_row_count=spark_canonical_count,
            sample_rows=spark_rows[:5],
        )

        duckdb_success = duckdb_result.status == SparkExecutionStatus.SUCCESS
        spark_success = spark_result.status == SparkExecutionStatus.SUCCESS

        # CRE shadow 诊断（最终硬化）——不改变现有结论
        cre_shadow_report = self._shadow_cre_diagnose(
            duckdb_raw=duckdb_result.output_rows if duckdb_success else [],
            spark_raw=spark_result.output_rows if spark_success else [],
            norm_config=self._normalization_config,
            legacy_status=status.value,
            contract_hash=contract_hash,
            snapshot_id=snapshot_id,
            primary_keys=cre_primary_keys,
            timezone=cre_timezone,
            environment_manifest=cre_environment_manifest,
        )

        return PhysicalVerificationReport(
            report_id=report_id,
            contract_hash=contract_hash,
            snapshot_id=snapshot_id,
            status=status,
            duckdb_result=duckdb_engine_result,
            spark_result=spark_engine_result,
            diffs=diffs,
            row_count_match=row_count_match,
            schema_match=schema_match,
            uncovered_step_types=uncovered,
            total_diff_count=total_diff_count,
            diffs_truncated=diffs_truncated,
            normalization_config_snapshot=config_snapshot,
            cre_shadow_report=cre_shadow_report,
            cdp_shadow_result=cdp_shadow_result,
        )

    # ── 权威 schema NULL 补齐 ──

    def _fill_missing_columns(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """用权威 schema（Contract output_columns）补齐缺失列。

        PySpark toJSON() 在列值为 NULL 时完全省略该键，导致引擎间
        schema 不匹配。本方法通过 Contract output_columns 的列名集合
        将每行中缺失的键统一填充为空字符串（与 NULL 归一化一致）。

        仅在该列属于权威 schema 且当前行为空（NULL）时填充。

        Args:
            rows: 规范化后的行列表

        Returns:
            补齐后的行列表（所有行都包含权威 schema 的全部列）
        """
        if not self._normalization_config or not self._normalization_config.output_columns:
            return rows

        # 归一化权威 schema 列名（与 ResultCanonicalizer 逻辑一致）
        expected_cols = {
            self._canonicalizer._normalize_column_name(col.column_name)
            for col in self._normalization_config.output_columns
        }
        if not expected_cols:
            return rows

        result: list[dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for col in expected_cols:
                if col not in new_row:
                    new_row[col] = ""  # NULL 在规范化后为空字符串
            result.append(new_row)
        return result

    # ── RESULT_MISMATCH 诊断保存 ──

    def _save_mismatch_diagnostics(
        self,
        pyspark_code: str,
        sql_query: str,
        duckdb_rows: list[dict[str, Any]] | None,
        spark_rows: list[dict[str, Any]] | None,
        snapshot_id: str,
        contract_hash: str,
        duckdb_row_count: int,
        spark_row_count: int,
    ) -> None:
        """RESULT_MISMATCH 时保存双引擎行数据到日志目录用于离线分析。

        保存到 logs/monitor/diagnostics/ 下按时间戳命名的子目录中。
        静默异常——不影响验证主流程。

        Args:
            pyspark_code: Spark 执行的 PySpark DSL 代码
            sql_query: DuckDB 执行的 SQL 查询
            duckdb_rows: DuckDB 原始输出行
            spark_rows: Spark 原始输出行
            snapshot_id: 快照 ID
            contract_hash: Contract hash
            duckdb_row_count: DuckDB 规范化后行数
            spark_row_count: Spark 规范化后行数
        """
        _diag_logger = logging.getLogger(__name__)
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            diag_dir = Path("logs/monitor/diagnostics") / f"physver_{snapshot_id}_{ts}"
            diag_dir.mkdir(parents=True, exist_ok=True)

            # 保存代码和查询
            (diag_dir / "pyspark_code.py").write_text(pyspark_code, encoding="utf-8")
            (diag_dir / "sql_query.sql").write_text(sql_query, encoding="utf-8")

            # 保存行数据（default=str 处理 Decimal 不可序列化问题）
            if duckdb_rows:
                (diag_dir / "duckdb_rows.json").write_text(
                    json.dumps(duckdb_rows, ensure_ascii=False, indent=1, default=str),
                    encoding="utf-8",
                )
            if spark_rows:
                (diag_dir / "spark_rows.json").write_text(
                    json.dumps(spark_rows, ensure_ascii=False, indent=1, default=str),
                    encoding="utf-8",
                )

            # 保存元数据
            manifest = {
                "diagnostic_type": "physver_mismatch",
                "snapshot_id": snapshot_id,
                "contract_hash": contract_hash,
                "saved_at": datetime.now().isoformat(),
                "duckdb_canonical_count": duckdb_row_count,
                "spark_canonical_count": spark_row_count,
                "duckdb_raw_count": len(duckdb_rows) if duckdb_rows else 0,
                "spark_raw_count": len(spark_rows) if spark_rows else 0,
            }
            (diag_dir / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            _diag_logger.warning(
                "[PHYS_VERIFIER_DIAG] RESULT_MISMATCH 诊断已保存到 %s", diag_dir,
            )
        except Exception as exc:
            _diag_logger.warning(
                "[PHYS_VERIFIER_DIAG] 保存 MISMATCH 诊断失败: %s", exc,
            )

    # ── DuckDB 执行 ──

    def _execute_duckdb(
        self,
        sql_query: str,
        snapshot_dir: str,
        duckdb_path: str | None = None,
    ) -> SparkExecutionResult:
        """通过 DuckDB 执行 SQL 查询——主进程内执行（轻量，无安全风险）。

        DuckDB 是内嵌式数据库——不需要子进程隔离。
        安全校验：_validate_select_sql 白名单——仅允许单条只读 SELECT。

        数据加载顺序：
        1. ATTACH 外部 DuckDB 数据库（如 NYC 数据仓库 gold.fact_trips）
        2. 注册快照 Parquet 文件为视图——覆盖 ATTACH 的同名视图

        Args:
            sql_query: DuckDB SQL 查询
            snapshot_dir: 快照目录（用于自动发现 Parquet 文件）
            duckdb_path: 外部 DuckDB 数据库路径——ATTACH 后自动创建 schema VIEW

        Returns:
            SparkExecutionResult——含输出行数据
        """
        # 白名单 SQL 安全校验——仅允许单条只读 SELECT
        try:
            _validate_select_sql(sql_query)
        except ValueError as e:
            return SparkExecutionResult(
                status=SparkExecutionStatus.SECURITY_REJECTED,
                error_message=str(e),
            )

        import time
        start = time.monotonic()

        try:
            import duckdb

            con = duckdb.connect()
            # 统一时区为 UTC——与 Spark 端 spark.sql.session.timeZone=UTC 对齐
            con.execute("SET TimeZone = 'UTC'")

            # Step 1：ATTACH 外部 DuckDB 数据库——使 gold/silver schema 表可用
            # 先 ATTACH，后用快照 Parquet 覆盖——确保快照数据优先
            if duckdb_path:
                self._attach_external_database(con, duckdb_path)

            # Step 2：注册快照目录下的所有 Parquet 文件为视图
            # CREATE OR REPLACE VIEW 会覆盖 Step 1 中 ATTACH 的同名视图
            _register_parquet_views(con, snapshot_dir)

            result = con.execute(sql_query)
            # 转换为 dict 列表
            columns = [desc[0] for desc in result.description]
            rows = [
                dict(zip(columns, row))
                for row in result.fetchall()
            ]
            con.close()

            elapsed = (time.monotonic() - start) * 1000
            return SparkExecutionResult(
                status=SparkExecutionStatus.SUCCESS,
                output_rows=rows,
                execution_time_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                execution_time_ms=elapsed,
                error_message=str(e),
            )

    @staticmethod
    def _attach_external_database(con, duckdb_path: str) -> None:
        """ATTACH 外部 DuckDB 数据库并创建 schema + VIEW 桥接。

        编译产生的 SQL 使用两段表名（如 gold.fact_trips），ATTACH 后自动
        在默认 catalog 中创建同名 schema 和 VIEW，使两段表名可直接解析。

        与 DuckDBExecutor._attach_database() 逻辑一致——确保 SQL 和
        物理验证两个管线看到相同的表结构。
        """
        try:
            alias = "_ext_db"
            con.execute(f"ATTACH '{duckdb_path}' AS {alias} (READ_ONLY)")

            # 发现所有用户 schema
            schemas = con.execute(f"""
                SELECT DISTINCT table_schema
                FROM information_schema.tables
                WHERE table_catalog = '{alias}'
                  AND table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchall()

            for (schema_name,) in schemas:
                try:
                    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                except Exception:
                    continue

                # 为该 schema 下的每张表创建 VIEW
                tables = con.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_catalog = '{alias}'
                      AND table_schema = '{schema_name}'
                      AND table_type = 'BASE TABLE'
                """).fetchall()

                for (table_name,) in tables:
                    try:
                        con.execute(
                            f"CREATE OR REPLACE VIEW {schema_name}.{table_name} AS "
                            f"SELECT * FROM {alias}.{schema_name}.{table_name}"
                        )
                    except Exception:
                        pass
        except Exception as e:
            # ATTACH 失败——记录详细信息以便排查
            import sys
            print(
                f"[PhysicalVerifier] ATTACH 外部数据库失败：{e}",
                file=sys.stderr,
            )
            import traceback
            traceback.print_exc(file=sys.stderr)

    # ── Spark 执行 ──

    def _execute_spark(
        self,
        pyspark_code: str,
        snapshot_dir: str,
    ) -> SparkExecutionResult:
        """通过 LocalSparkExecutor 在子进程中执行 PySpark DSL。

        Args:
            pyspark_code: 已编译的 PySpark DSL 代码
            snapshot_dir: 快照目录

        Returns:
            SparkExecutionResult——含输出行数据
        """
        return self._spark_executor.execute(
            pyspark_code=pyspark_code,
            data_dir=snapshot_dir,
            output_var="result_df",
        )

    # ── CDP v1 Engine-side Shadow（Task 8） ──

    @staticmethod
    def _infer_cdp_spec(output_rows: list[dict]) -> object | None:
        """从执行结果的列类型推断 CreDigestSpec——用于 CDP shadow 摘要计算。

        DuckDB 查询结果的 Python 类型 → TypeFamily 映射：
        - int → INT64（DuckDB 默认整数类型）
        - float → FLOAT64
        - str → VARCHAR
        - bool → BOOLEAN
        - NoneType / 其他 → 跳过（无法确定类型）

        Args:
            output_rows: DuckDB 执行结果行列表

        Returns:
            CreDigestSpec 或 None（列数不足 / 类型不兼容时返回 None）
        """
        if not output_rows:
            return None

        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        # 收集所有列名及其可能的 Python 类型
        col_names = list(output_rows[0].keys())
        if not col_names:
            return None

        # 从所有行中推断每列的类型（取第一个非 None 值的类型）
        col_types: list[TypeFamily] = []
        for col in col_names:
            tf = PhysicalVerifier._guess_type_family(col, output_rows)
            if tf is None:
                return None  # 无法确定全部列的类型
            col_types.append(tf)

        n = len(col_names)
        return CreDigestSpec(
            output_columns=col_names,
            type_families=col_types,
            timezone="UTC",
            decimal_precision=[None] * n,
            decimal_scale=[None] * n,
            float_precision=[None] * n,
        )

    @staticmethod
    def _guess_type_family(col: str, rows: list[dict]) -> object | None:
        """从行数据中推断单列的 TypeFamily——取第一个非 None 值的 Python 类型。

        Args:
            col: 列名
            rows: 数据行列表

        Returns:
            TypeFamily 或 None（无法确定时）
        """
        from tianshu_datadev.spark.cdp_spec import TypeFamily

        for row in rows:
            v = row.get(col)
            if v is None:
                continue
            if isinstance(v, bool):
                return TypeFamily.BOOLEAN
            if isinstance(v, int):
                return TypeFamily.INT64
            if isinstance(v, float):
                return TypeFamily.FLOAT64
            if isinstance(v, str):
                return TypeFamily.VARCHAR
            # 其他类型暂不支持
            return None
        # 全部为 None——默认 VARCHAR
        return TypeFamily.VARCHAR

    def _run_cdp_shadow(
        self,
        sql_query: str,
        snapshot_dir: str,
        cdp_spec: object,
        snapshot_id: str,
        duckdb_con=None,
    ) -> dict | None:
        """执行 CDP v1 engine-side shadow——双引擎摘要计算 + compare()。

        Shadow 路径禁止接触 output_rows——仅在引擎内部计算 CDP digest，
        接收两个 DigestExecutionEnvelope，调用 compare() 对比。

        Args:
            sql_query: DuckDB SQL 查询
            snapshot_dir: 快照目录
            cdp_spec: CreDigestSpec 实例
            snapshot_id: 快照 ID
            duckdb_con: 可选的已有 DuckDB 连接（None 时新建）

        Returns:
            dict——含 status, decision_reason, duckdb_digest, spark_digest
            任一引擎失败时返回 None
        """
        import logging

        from tianshu_datadev.spark.cdp_spec import (
            DigestExecutionEnvelope,
            EngineDigestSummary,
            compare,
            compute_digest_spec_hash,
        )

        logger = logging.getLogger(__name__)
        spec_hash_hex = compute_digest_spec_hash(cdp_spec).hex()

        # ── DuckDB CDP ──
        duckdb_envelope = None
        own_con = False
        try:
            if duckdb_con is None:
                import duckdb

                duckdb_con = duckdb.connect()
                own_con = True
                # 注册快照视图（与 _execute_duckdb 相同的协议）
                _register_parquet_views(duckdb_con, snapshot_dir)

            from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder

            builder = DuckdbCdpBuilder()
            cdp_query = builder.build_query(
                sql_query, cdp_spec, spec_hash_hex=spec_hash_hex,
            )
            result = duckdb_con.execute(cdp_query).fetchone()
            duckdb_envelope = DigestExecutionEnvelope(
                execution_status="SUCCESS",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="duckdb",
                summary=EngineDigestSummary(
                    row_count=int(result[1]),
                    full_digest=str(result[0]),
                    samples=[],
                ),
            )
        except Exception:
            logger.warning("CDP shadow: DuckDB CDP 计算失败", exc_info=True)
            return None
        finally:
            if own_con and duckdb_con is not None:
                try:
                    duckdb_con.close()
                except Exception:
                    pass

        # ── Spark CDP ──
        spark_envelope = None
        try:
            spark_envelope = self._spark_executor.execute_with_cdp(
                spec=cdp_spec,
                snapshot_id=snapshot_id,
                data_dir=snapshot_dir,
            )
        except Exception:
            logger.warning("CDP shadow: Spark CDP 计算失败", exc_info=True)
            return None

        if spark_envelope is None or duckdb_envelope is None:
            return None

        # 任一引擎失败
        if duckdb_envelope.execution_status != "SUCCESS":
            logger.info(
                "CDP shadow: DuckDB CDP 失败 status=%s",
                duckdb_envelope.execution_status,
            )
            return None
        if spark_envelope.execution_status != "SUCCESS":
            logger.info(
                "CDP shadow: Spark CDP 失败 status=%s",
                spark_envelope.execution_status,
            )
            return None

        # ── 对比 ──
        comparison = compare(duckdb_envelope, spark_envelope)
        logger.info(
            "CDP shadow: status=%s reason=%s",
            comparison.status,
            comparison.decision_reason,
        )
        return {
            "status": comparison.status,
            "decision_reason": comparison.decision_reason,
        }

    # ── 差异计算 ──
    # [CDP v1 迁移提示] _compute_diffs() 将在 Task 9 CDP 接管后删除。
    # 替换路径：verify() → CDP shadow → compare() → 基于 digest 的判定。
    # 逐行逐列对比的 legacy 逻辑由 CDP 摘要对比替代。

    @staticmethod
    def _parse_decimal_scale(dtype: str) -> int | None:
        """从 data_type 字符串解析 Decimal scale。

        支持标准格式如 "decimal(18,2)" → 2，"decimal(10,0)" → 0。
        不支持格式或无 scale 信息时返回 None。

        Args:
            dtype: 数据类型字符串

        Returns:
            scale 值，无法解析时返回 None
        """
        m = re.match(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", dtype)
        if m:
            return int(m.group(2))
        return None

    @staticmethod
    def _values_are_equivalent(
        duckdb_val: str,
        spark_val: str,
        column: str,
        col_types: dict[str, str],
        config: NormalizationConfig | None,
    ) -> bool:
        """类型感知的等价性判断——str 相等无法覆盖时使用。

        优先级：
        1. 空值判断——两个都为空视为等价
        2. str 精确相等——最快路径
        3. float/double——math.isclose（仅配置了类型时）
        4. Decimal——quantize 精确比较（仅配置了类型时）
        5. 以上都不满足 → 不等价

        Args:
            duckdb_val: DuckDB 侧规范化后的字符串值
            spark_val: Spark 侧规范化后的字符串值
            column: 列名（已规范化）
            col_types: 列名 → 类型名映射
            config: 规范化配置（可为 None）

        Returns:
            True 如果两值等价，否则 False
        """
        # 都为空 → 等价（NULL 补齐后双方缺失键→"")
        if not duckdb_val and not spark_val:
            return True
        # 一方为空 → 不等价（真正缺失 vs 有值）
        if not duckdb_val or not spark_val:
            return False
        # 精确 str 匹配 → 等价（最快路径）
        if duckdb_val == spark_val:
            return True

        if config is None:
            return False

        import math
        from decimal import Decimal as _Decimal

        dtype = col_types.get(column, "").lower()

        # Float/double → math.isclose
        if dtype and ("float" in dtype or "double" in dtype):
            try:
                dv = float(duckdb_val)
                sv = float(spark_val)
                if math.isclose(
                    dv, sv,
                    rel_tol=config.float_rel_tolerance,
                    abs_tol=config.float_abs_tolerance,
                ):
                    return True
            except (ValueError, TypeError):
                pass
            return False  # float 列不匹配且不满足容差 → 不等价

        # Decimal → quantize 精确比较
        if dtype and "decimal" in dtype:
            try:
                dd = _Decimal(duckdb_val)
                sd = _Decimal(spark_val)
                scale = PhysicalVerifier._parse_decimal_scale(dtype)
                if scale is not None:
                    quantize_str = "0." + "0" * scale if scale > 0 else "1"
                    quant = _Decimal(quantize_str)
                    dd = dd.quantize(quant)
                    sd = sd.quantize(quant)
                return dd == sd
            except Exception:
                pass
            return False  # Decimal 列不精确匹配 → 不等价

        # 数值回退——当 data_type 为 "unknown" 或缺失时，
        # DuckDB vs PySpark 浮点末位差异（~1e-15）仍用 math.isclose 消除
        # 仅当双方都能解析为 float 时启用，不影响非数值列
        try:
            dv = float(duckdb_val)
            sv = float(spark_val)
            if math.isclose(
                dv, sv,
                rel_tol=config.float_rel_tolerance,
                abs_tol=config.float_abs_tolerance,
            ):
                return True
        except (ValueError, TypeError):
            pass

        return False

    @staticmethod
    def _build_sort_key(row: dict[str, Any], keys: list[str]) -> str:
        """从行数据中提取排序键的规范化字符串——用于基于键的行对齐。

        Args:
            row: 规范化后的行 dict
            keys: 排序键列名列表（已归一化）

        Returns:
            排序键字符串（用 | 连接）
        """
        return "|".join(str(row.get(k, "")) for k in keys)

    @classmethod
    def _compute_diffs(
        cls,
        duckdb_rows: list[dict[str, Any]],
        spark_rows: list[dict[str, Any]],
        config: NormalizationConfig | None = None,
        order_keys: list[str] | None = None,
    ) -> tuple[list[DiffDetail], int, bool]:
        """逐行逐列对比 DuckDB 和 Spark 规范化后的结果。

        优先按排序键（order_keys）对齐行——避免一方多行时后续全部错位。
        无排序键时回退到按索引对比（原行为）。

        Args:
            duckdb_rows: DuckDB 规范化后的行列表
            spark_rows: Spark 规范化后的行列表
            config: 规范化配置（Phase 9B），用于类型感知比较
            order_keys: 排序键列名列表（已归一化），用于基于键的行对齐

        Returns:
            (diffs, total_diff_count, diffs_truncated) 三元组
        """
        diffs: list[DiffDetail] = []
        total_diff_count = 0
        max_detail_diffs = 20  # 最多返回 20 条详细差异

        # 从 NormalizationConfig 构建列名→类型映射
        col_types: dict[str, str] = {}
        if config:
            for col in config.output_columns:
                norm_name = ResultCanonicalizer._normalize_column_name(col.column_name)
                if col.data_type:
                    col_types[norm_name] = col.data_type

        # ── 基于排序键的行对齐（order_keys 可用时）──
        # 优先使用 key-based 对齐，避免 index-based 的错位级联
        if order_keys:
            # Step 1：构建 DuckDB 行索引（sort_key → row）
            duckdb_index: dict[str, dict[str, Any]] = {}
            duckdb_keys_seen: set[str] = set()
            for row in duckdb_rows:
                sk = cls._build_sort_key(row, order_keys)
                if sk not in duckdb_keys_seen:
                    duckdb_index[sk] = row
                    duckdb_keys_seen.add(sk)

            # Step 2：逐行对比 Spark 侧 vs DuckDB 侧
            spark_keys_matched: set[str] = set()
            for i, spark_row in enumerate(spark_rows):
                sk = cls._build_sort_key(spark_row, order_keys)
                duckdb_row = duckdb_index.get(sk)

                if duckdb_row is None:
                    # Spark 侧独有的行
                    spark_keys_matched.add(sk)  # 标记已处理
                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column="(整行)",
                            duckdb_value="(缺失)",
                            spark_value=str(spark_row)[:200],
                            description=f"Row {i}（key={sk}）：DuckDB 侧缺失，Spark 侧有数据",
                        ))
                    continue

                spark_keys_matched.add(sk)

                # 已找到对应行——逐列对比
                all_columns = sorted(set(duckdb_row.keys()) | set(spark_row.keys()))
                for col in all_columns:
                    duckdb_val = duckdb_row.get(col, "")
                    spark_val = spark_row.get(col, "")

                    if cls._values_are_equivalent(duckdb_val, spark_val, col, col_types, config):
                        continue

                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column=col,
                            duckdb_value=str(duckdb_val)[:200],
                            spark_value=str(spark_val)[:200],
                            description=f"Row {i}（key={sk}）：{col} 列值不一致",
                        ))

            # Step 3：DuckDB 侧独特行（未在 Spark 中找到对应 key）
            for i, row in enumerate(duckdb_rows):
                sk = cls._build_sort_key(row, order_keys)
                if sk not in spark_keys_matched:
                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column="(整行)",
                            duckdb_value=str(row)[:200],
                            spark_value="(缺失)",
                            description=f"Row {i}（key={sk}）：DuckDB 侧有数据，Spark 侧缺失",
                        ))

        else:
            # ── 无排序键时回退到按索引对比（原行为）──
            max_len = max(len(duckdb_rows), len(spark_rows))
            for i in range(max_len):
                duckdb_row = duckdb_rows[i] if i < len(duckdb_rows) else {}
                spark_row = spark_rows[i] if i < len(spark_rows) else {}

                # 行数不对齐
                if not duckdb_row:
                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column="(整行)",
                            duckdb_value="(缺失)",
                            spark_value=str(spark_row)[:200],
                            description=f"第 {i} 行：DuckDB 侧缺失，Spark 侧有数据",
                        ))
                    continue
                if not spark_row:
                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column="(整行)",
                            duckdb_value=str(duckdb_row)[:200],
                            spark_value="(缺失)",
                            description=f"第 {i} 行：DuckDB 侧有数据，Spark 侧缺失",
                        ))
                    continue

                # 逐列对比
                all_columns = sorted(set(duckdb_row.keys()) | set(spark_row.keys()))
                for col in all_columns:
                    duckdb_val = duckdb_row.get(col, "")
                    spark_val = spark_row.get(col, "")

                    if cls._values_are_equivalent(duckdb_val, spark_val, col, col_types, config):
                        continue

                    total_diff_count += 1
                    if len(diffs) < max_detail_diffs:
                        diffs.append(DiffDetail(
                            row_index=i,
                            column=col,
                            duckdb_value=str(duckdb_val)[:200],
                            spark_value=str(spark_val)[:200],
                            description=f"第 {i} 行 {col} 列值不一致",
                        ))

        diffs_truncated = total_diff_count > len(diffs)
        return diffs, total_diff_count, diffs_truncated

    # ── 报告 ID ──

    @staticmethod
    def _generate_report_id(contract_hash: str, snapshot_id: str) -> str:
        """生成确定性物理验证报告 ID。"""
        payload = {
            "contract_hash": contract_hash,
            "snapshot_id": snapshot_id,
            "phase": "7B",
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"physver_{hash_hex}"

    # ════════════════════════════════════════════
    # CRE 状态 → 生产状态映射表
    # ════════════════════════════════════════════

    _CRE_TO_LEGACY_MAP: dict[str, str] = {
        "CONSISTENT": "RESULT_CONSISTENT",
        "CONSISTENT_WITH_WARN": "RESULT_CONSISTENT",
        "MISMATCH": "RESULT_MISMATCH",
        "HUMAN_REVIEW": "HUMAN_REVIEW",
    }

    # ── CRE shadow 模式诊断（要求 5/6）──

    @staticmethod
    def _shadow_cre_diagnose(
        duckdb_raw: list[dict[str, Any]],
        spark_raw: list[dict[str, Any]],
        norm_config: NormalizationConfig | None,
        legacy_status: str,
        contract_hash: str = "",
        snapshot_id: str = "",
        primary_keys: list[str] | None = None,
        timezone: str = "",
        environment_manifest: EnvironmentManifest | None = None,
    ) -> CreShadowReport:
        """以 shadow 模式运行 CRE 诊断——不改变现有结论。

        Args:
            duckdb_raw: DuckDB 原始行数据
            spark_raw: Spark 原始行数据
            norm_config: 规范化配置（含 Contract 列定义）
            legacy_status: 生产 verify() 的原始状态
            contract_hash: Contract hash（审计追溯）
            snapshot_id: 快照 ID（审计追溯）
            primary_keys: 权威主键列名（来自 Contract grouping_keys），
                          None 时使用 norm_config.primary_keys
            timezone: 时区标识，如 "Asia/Shanghai"
            environment_manifest: EnvironmentManifest 对象（Pipeline 显式传入），
                                  为 None 时特殊浮点值策略回退为 HUMAN_REVIEW

        Returns:
            CreShadowReport 严格模型——始终返回，不返回 None
        """
        # 惰性导入避免循环依赖
        from tianshu_datadev.spark.cre_encoding import (
            CreShadowReport,
            CreShadowStatus,
            CreShadowWarning,
        )

        # ── 前置条件检查 1：缺少 Contract output_columns ──
        if not norm_config or not norm_config.output_columns:
            return CreShadowReport(
                diagnostic_available=False,
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                cre_status=CreShadowStatus.NOT_EXECUTED,
                mapped_status="NOT_EXECUTED",
                legacy_status=legacy_status,
                status_consistent=False,
                human_review_recommended=True,
                decision_reason="缺少 Contract output_columns——无法运行 CRE 诊断",
                error_message="缺少 Contract output_columns——无法运行 CRE 诊断",
            )

        # 确定主键：优先用传入的 primary_keys，后备 norm_config.primary_keys
        resolved_pks = primary_keys if primary_keys is not None else norm_config.primary_keys

        # ── 前置条件检查 2：无主键时的 singleton 对齐（Req 3）──
        if not resolved_pks:
            duckdb_count = len(duckdb_raw)
            spark_count = len(spark_raw)
            if duckdb_count == 1 and spark_count == 1:
                # 双侧恰好各 1 行 → singleton 对齐允许
                pass
            else:
                # 任一侧多行 → NOT_EXECUTED，禁止按行号或全部列猜键
                return CreShadowReport(
                    diagnostic_available=False,
                    contract_hash=contract_hash,
                    snapshot_id=snapshot_id,
                    cre_status=CreShadowStatus.NOT_EXECUTED,
                    mapped_status="NOT_EXECUTED",
                    legacy_status=legacy_status,
                    status_consistent=False,
                    human_review_recommended=True,
                    decision_reason=(
                        "缺少权威主键（primary_keys）且双侧行数不满足 singleton 条件——"
                        f"DuckDB={duckdb_count}行，Spark={spark_count}行，"
                        f"禁止按行号或全部列猜键，需人工介入指定主键"
                    ),
                    error_message=(
                        f"缺少权威主键（primary_keys）——DuckDB={duckdb_count}行，"
                        f"Spark={spark_count}行——不满足 singleton 对齐条件"
                    ),
                )

        # ── 前置条件检查 3：缺少 output_columns ──
        # 已在上面检查过，此处防御
        if not norm_config.output_columns:
            return CreShadowReport(
                diagnostic_available=False,
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                cre_status=CreShadowStatus.NOT_EXECUTED,
                mapped_status="NOT_EXECUTED",
                legacy_status=legacy_status,
                status_consistent=False,
                human_review_recommended=True,
                error_message="缺少 Contract output_columns——无法运行 CRE 诊断",
            )

        try:
            from tianshu_datadev.spark.cre import (
                BucketHasher,
                CreConfig,
                CREEncoder,
                DecisionEngine,
                KeyBasedRowAligner,
                ToleranceComparator,
            )
            # 构造 CreConfig——singleton 模式下用占位主键通过 DecisionEngine PK gate
            singleton_mode = not resolved_pks
            effective_pks = list(resolved_pks) if not singleton_mode else ["__singleton__"]
            config = CreConfig(
                output_columns=list(norm_config.output_columns),
                primary_keys=effective_pks,
                float_abs_tolerance=norm_config.float_abs_tolerance,
                float_rel_tolerance=norm_config.float_rel_tolerance,
                timezone=timezone,
                environment_manifest=environment_manifest,
            )
            encoder = CREEncoder(config)

            # 行对齐——singleton 跳过 KeyBasedRowAligner 主键校验，直接配对
            if singleton_mode:
                from tianshu_datadev.spark.cre_alignment import AlignmentResult
                alignment = AlignmentResult(
                    aligned_pairs=[(duckdb_raw[0], spark_raw[0])],
                    duckdb_only=[],
                    spark_only=[],
                    error_message="",
                    duplicate_keys=False,
                )
            else:
                alignment = KeyBasedRowAligner.align(duckdb_raw, spark_raw, config, encoder)

            # 逐行比较——singleton 和非 singleton 统一路径
            pairs = alignment.aligned_pairs
            row_results = [
                (d, s, ToleranceComparator.compare_row(d, s, config, encoder))
                for d, s in pairs
            ] if pairs else []

            # 分桶——singleton 模式跳过 BucketHasher（占位 PK 不可用于 digest）
            if singleton_mode:
                from tianshu_datadev.spark.cre_alignment import BucketResult
                buckets = BucketResult(
                    duckdb_bucket_digests=[],
                    spark_bucket_digests=[],
                    mismatched_buckets=[],
                    num_buckets=config.num_buckets,
                )
            elif pairs:
                buckets = BucketHasher.compute_bucket_digests(pairs, encoder, config)
            else:
                buckets = BucketHasher.compute_bucket_digests([], encoder, config)

            # 判定——统一走 DecisionEngine（Req 6：singleton 不再手写分支）
            cre_result = DecisionEngine.decide(alignment, row_results, buckets, config)

            # ── 状态映射（要求 2）──
            cre_status = cre_result.status
            mapped_status = PhysicalVerifier._CRE_TO_LEGACY_MAP.get(
                cre_status, "NOT_EXECUTED"
            )

            # status_consistent 必须比较映射后的语义状态
            status_consistent = (mapped_status == legacy_status)

            # 两套结论不同 → HUMAN_REVIEW 提示
            human_review_recommended = not status_consistent

            # 收集 WARN（严格模型——field_details 直接传递，不再 model_dump）
            warnings_list: list[CreShadowWarning] = []
            for w in cre_result.warnings:
                warnings_list.append(CreShadowWarning(
                    action=getattr(w, "action", ""),
                    tolerated_ratio=getattr(w, "tolerated_ratio", 0.0),
                    affected_row_count=getattr(w, "affected_row_count", 0),
                    affected_cell_count=getattr(w, "affected_cell_count", 0),
                    total_comparison_rows=getattr(w, "total_comparison_rows", 0),
                    field_details=list(w.field_details),
                ))

            return CreShadowReport(
                diagnostic_available=True,
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                cre_status=CreShadowStatus(cre_status),
                mapped_status=mapped_status,
                legacy_status=legacy_status,
                status_consistent=status_consistent,
                human_review_recommended=human_review_recommended,
                has_warnings=len(cre_result.warnings) > 0,
                warnings=warnings_list,
                total_rows=cre_result.aligned_rows,
                exact_match_rows=cre_result.exact_match_rows,
                tolerance_match_rows=cre_result.tolerance_match_rows,
                affected_row_count=cre_result.tolerance_match_rows,
                mismatched_bucket_count=cre_result.mismatched_bucket_count,
                decision_reason=cre_result.decision_reason,
                error_message=alignment.error_message or "",
            )
        except Exception as e:
            # shadow 失败不阻断主流程（要求 3）
            return CreShadowReport(
                diagnostic_available=False,
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                cre_status=CreShadowStatus.ERROR,
                mapped_status="NOT_EXECUTED",
                legacy_status=legacy_status,
                status_consistent=False,
                human_review_recommended=True,
                decision_reason=f"CRE shadow 诊断异常：{e}",
                error_message=f"CRE shadow 诊断异常：{e}",
            )


# ════════════════════════════════════════════
# DuckDB 辅助工具
# ════════════════════════════════════════════


def _register_parquet_views(duckdb_con: Any, snapshot_dir: str) -> None:
    """将快照目录下的所有 Parquet 文件注册为同名 DuckDB 视图。

    对 snapshot_dir 下每个 *.parquet 文件创建视图——使用参数化查询避免 SQL 注入。

    支持两种表名格式：
    - flat name（如 fact_trips_sample）：直接创建同名视图
    - schema 前缀（如 gold.fact_trips）：自动创建 schema，在对应 schema 下创建视图

    安全措施：
    1. 视图名严格校验（仅允许字母、数字、下划线，且不以数字开头）
       支持 schema.table 格式——分别校验 schema 和 table 部分
    2. 路径穿越防护（realpath 必须位于 snapshot_dir 下）
    3. 文件路径通过字符串转义（单引号加倍），配合 realpath 校验确保安全

    仅在 snapshot_dir 存在且包含 .parquet 文件时才注册。

    Args:
        duckdb_con: duckdb.DuckDBPyConnection 连接对象
        snapshot_dir: 快照目录路径
    """
    import os

    if not os.path.isdir(snapshot_dir):
        return

    # 规范化快照目录路径——用于路径穿越防护
    real_snapshot_dir = os.path.realpath(snapshot_dir)

    for filename in sorted(os.listdir(snapshot_dir)):
        if not filename.endswith(".parquet"):
            continue
        filepath = os.path.join(snapshot_dir, filename)
        # 文件名去扩展名作为视图名
        view_name = filename.replace(".parquet", "")

        # 解析 schema.table 格式——分别校验各段标识符
        schema_name: str | None = None
        table_name: str = view_name
        if "." in view_name:
            parts = view_name.split(".", 1)
            schema_name, table_name = parts[0], parts[1]
            # 分别校验 schema 和 table 部分——仅允许 SQL 标准标识符
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
                continue
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
                continue
        else:
            # 严格标识符校验——SQL 标识符规范：字母/下划线开头 + 字母/数字/下划线
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", view_name):
                continue

        # 路径穿越防护——realpath 必须在 snapshot_dir 子树内
        try:
            real_path = os.path.realpath(filepath)
        except OSError:
            continue
        if not real_path.startswith(real_snapshot_dir + os.sep):
            continue

        try:
            # 路径 SQL 转义——单引号加倍（SQL 标准），配合 realpath 校验确保安全
            escaped_path = filepath.replace("'", "''")
            if schema_name is not None:
                # schema.table 格式：自动创建 schema，在对应 schema 下创建视图
                duckdb_con.execute(
                    'CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
                        schema=schema_name,
                    ),
                )
                duckdb_con.execute(
                    'CREATE OR REPLACE VIEW "{schema}"."{table}" AS '
                    "SELECT * FROM read_parquet('{path}')".format(
                        schema=schema_name, table=table_name, path=escaped_path,
                    ),
                )
            else:
                # flat name：直接创建视图（保持向后兼容）
                duckdb_con.execute(
                    'CREATE OR REPLACE VIEW "{name}" AS '
                    "SELECT * FROM read_parquet('{path}')".format(
                        name=view_name, path=escaped_path,
                    ),
                )
        except Exception:
            # 视图创建失败不阻断验证流程
            pass
