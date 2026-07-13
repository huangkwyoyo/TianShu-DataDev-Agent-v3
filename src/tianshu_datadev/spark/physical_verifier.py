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
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from typing import Any

from pydantic import Field

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
            # 浮点精度归一化——DuckDB C++ 引擎与 PySpark JVM 引擎
            # 对同一聚合运算（AVG/SUM）可能产生末位差异（~1e-15），
            # 四舍五入到 10 位小数消除此差异。
            # 例：DuckDB 3.9369690851405856 vs Spark 3.9369690851405883
            #     → round(..., 10) → 3.9369690851（一致）
            return str(round(value, 10))
        # Decimal 归一化——转为 float 后再 round，与 PySpark float 路径对齐
        # DuckDB DECIMAL(12,2) 的 str() 保留尾随零（如 '1266.70'），
        # 而 PySpark double 的 str() 不保留（如 '1266.7'）——两者需统一。
        # float 转换引入的精度损失与 PySpark JVM double 一致，round 后等价。
        if hasattr(value, "__class__") and value.__class__.__name__ == "Decimal":
            return str(round(float(value), 10))
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
    cdp_shadow_result: dict | None = Field(       # CDP shadow 摘要对比结果（Task 8）
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
    ) -> None:
        """初始化物理验证器。

        Args:
            spark_executor: LocalSparkExecutor 实例，None 时创建默认实例
        """
        self._spark_executor = spark_executor or LocalSparkExecutor()
        self._canonicalizer = ResultCanonicalizer()

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

        # 自动检测排序键：当无显式排序键时，从 DuckDB 结果列名自动提取
        # 确保双引擎结果按相同键排序，实现确定性行级对比
        if not order_keys and duckdb_result.output_rows:
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
            import logging
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

        # 逐行逐列对比
        diffs = self._compute_diffs(duckdb_rows, spark_rows)

        # 判定最终状态
        if not diffs and row_count_match and schema_match:
            status = PhysicalVerificationStatus.RESULT_CONSISTENT
        elif not schema_match or (duckdb_canonical_count > 0 and spark_canonical_count > 0):
            status = PhysicalVerificationStatus.RESULT_MISMATCH
        else:
            status = PhysicalVerificationStatus.HUMAN_REVIEW

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
            cdp_shadow_result=cdp_shadow_result,
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
    def _compute_diffs(
        duckdb_rows: list[dict[str, Any]],
        spark_rows: list[dict[str, Any]],
    ) -> list[DiffDetail]:
        """逐行逐列对比 DuckDB 和 Spark 规范化后的结果。

        按行索引逐一对比——两方对齐后比较每列值。
        最多返回 20 个差异项（防止超大输出）。

        Args:
            duckdb_rows: DuckDB 规范化后的行列表
            spark_rows: Spark 规范化后的行列表

        Returns:
            差异项列表
        """
        diffs: list[DiffDetail] = []
        max_len = max(len(duckdb_rows), len(spark_rows))

        for i in range(max_len):
            if len(diffs) >= 20:
                break

            duckdb_row = duckdb_rows[i] if i < len(duckdb_rows) else {}
            spark_row = spark_rows[i] if i < len(spark_rows) else {}

            # 行数不对齐
            if not duckdb_row:
                diffs.append(DiffDetail(
                    row_index=i,
                    column="(整行)",
                    duckdb_value="(缺失)",
                    spark_value=str(spark_row)[:200],
                    description=f"第 {i} 行：DuckDB 侧缺失，Spark 侧有数据",
                ))
                continue
            if not spark_row:
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
                duckdb_val = duckdb_row.get(col)
                spark_val = spark_row.get(col)
                if duckdb_val != spark_val:
                    diffs.append(DiffDetail(
                        row_index=i,
                        column=col,
                        duckdb_value=str(duckdb_val)[:200],
                        spark_value=str(spark_val)[:200],
                        description=f"第 {i} 行 {col} 列值不一致",
                    ))
                    if len(diffs) >= 20:
                        break

        return diffs

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
