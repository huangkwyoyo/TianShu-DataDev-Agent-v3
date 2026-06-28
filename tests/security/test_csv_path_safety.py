"""CSV 路径注入安全测试——验证 SafeCsvPathLiteral 在 Schema 层拒绝恶意输入。

Phase 3B 安全加固——table_paths 的 value（CSV 文件路径）作为 SQL 字符串字面量
直接拼入 read_csv_auto('...')。任何含单引号的路径值可终结字符串字面量并注入
任意 SQL 语句。

SafeCsvPathLiteral（Schema 层） + _render_sql_string_literal（渲染层）构成双重防线。

测试覆盖：
- 恶意 CSV 路径在 DuckDBExecutor 构造期被拒绝（单引号/换行/回车/空字节）
- 合法路径通过（Unix / Windows 绝对路径、相对路径）
- 边界情况：空字符串、仅反斜杠
- 集成验证：强行绕过构造器将恶意值塞入 _table_paths 后，转义层阻止 rogue 表创建
- _render_sql_string_literal 单元测试
"""

from __future__ import annotations

import pytest

from tianshu_datadev.developer_spec.models import (
    _render_sql_string_literal,
    _validate_csv_path_literal,
)
from tianshu_datadev.sql.executor import DuckDBExecutor

# ════════════════════════════════════════════
# 恶意输入样本
# ════════════════════════════════════════════

MALICIOUS_CSV_PATHS: list[tuple[str, str]] = [
    # ── 单引号注入（复现原始漏洞）──
    (
        "safe.csv'); CREATE TABLE injected AS SELECT 42 AS x; --",
        "单引号逃逸 + CREATE TABLE 注入",
    ),
    (
        "data.csv'); DROP TABLE users; --",
        "单引号逃逸 + DROP TABLE 注入",
    ),
    (
        "x'); INSERT INTO secret VALUES (1, 'stolen'); --",
        "单引号逃逸 + INSERT 注入",
    ),
    (
        "x' OR '1'='1",
        "单引号布尔注入",
    ),

    # ── 控制字符 ──
    (
        "data.csv\n'); DROP TABLE users; --",
        "换行符 + 注入",
    ),
    (
        "data.csv\r'); DROP TABLE users; --",
        "回车符 + 注入",
    ),
    (
        "data.csv\x00'); DROP TABLE users; --",
        "空字节 + 注入",
    ),
]

# 合法路径样本
VALID_CSV_PATHS: list[tuple[str, str]] = [
    ("data/users.csv", "相对 Unix 路径"),
    ("/absolute/path/to/data.csv", "绝对 Unix 路径"),
    ("C:\\Users\\data\\file.csv", "Windows 绝对路径（反斜杠）"),
    ("D:\\Program Files\\data\\source.csv", "含空格的 Windows 路径"),
    ("../fixtures/sql/test_fact.csv", "相对路径含上级目录"),
    ("file.csv", "仅文件名"),
    ("data_2024.csv", "含下划线和数字"),
    ("a" * 200 + ".csv", "长路径名"),
]


# ════════════════════════════════════════════
# _validate_csv_path_literal 单元测试
# ════════════════════════════════════════════

class TestValidateCsvPathLiteral:
    """SafeCsvPathLiteral 底层校验函数——单元测试。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_CSV_PATHS)
    def test_reject_malicious_path(self, malicious: str, desc: str):
        """恶意 CSV 路径被校验函数拒绝。"""
        with pytest.raises(ValueError, match="CSV 路径"):
            _validate_csv_path_literal(malicious)

    @pytest.mark.parametrize("valid,desc", VALID_CSV_PATHS)
    def test_accept_valid_path(self, valid: str, desc: str):
        """合法 CSV 路径通过校验（不抛异常）。"""
        result = _validate_csv_path_literal(valid)
        assert result == valid

    def test_reject_empty_string(self):
        """空字符串被拒绝。"""
        with pytest.raises(ValueError, match="不能为空"):
            _validate_csv_path_literal("")

    def test_reject_only_single_quote(self):
        """纯单引号被拒绝。"""
        with pytest.raises(ValueError, match="CSV 路径"):
            _validate_csv_path_literal("'")


# ════════════════════════════════════════════
# _render_sql_string_literal 单元测试
# ════════════════════════════════════════════

class TestRenderSqlStringLiteral:
    """SQL 字符串字面量渲染——转义层单元测试。"""

    def test_wraps_with_single_quotes(self):
        """普通路径被单引号包裹。"""
        result = _render_sql_string_literal("data/users.csv")
        assert result == "'data/users.csv'"

    def test_escapes_embedded_single_quote(self):
        """路径中的单引号被转义为双单引号。"""
        result = _render_sql_string_literal("O'Brien/data.csv")
        assert result == "'O''Brien/data.csv'"

    def test_windows_path_preserved(self):
        """Windows 反斜杠路径保持不变。"""
        result = _render_sql_string_literal("C:\\Users\\data.csv")
        assert result == "'C:\\Users\\data.csv'"

    def test_escapes_injection_payload(self):
        """注入 payload 中的单引号被转义——SQL 语义被摧毁。"""
        result = _render_sql_string_literal(
            "safe.csv'); CREATE TABLE injected AS SELECT 42 AS x; --"
        )
        # 转义后所有 ' 变成 ''，攻击无法逃逸字符串
        assert "''" in result
        assert result.startswith("'")
        assert result.endswith("'")
        # 确认注入关键词被包裹在字符串内（无法执行）
        assert "CREATE TABLE" not in result.split("'")[0].upper()

    def test_empty_string_wrapped(self):
        """空字符串也被正确包裹。"""
        result = _render_sql_string_literal("")
        assert result == "''"


# ════════════════════════════════════════════
# DuckDBExecutor 构造期拒绝测试
# ════════════════════════════════════════════

class TestExecutorRejectsMaliciousCsvPath:
    """DuckDBExecutor.__init__ 在构造期拒绝恶意 CSV 路径。"""

    @pytest.mark.parametrize("malicious,desc", MALICIOUS_CSV_PATHS)
    def test_reject_malicious_csv_path_at_construction(
        self, malicious: str, desc: str
    ):
        """恶意 CSV 路径在构造器中被拒绝——Schema 层防线。"""
        with pytest.raises(ValueError, match="CSV 路径"):
            DuckDBExecutor(table_paths={"safe_table": malicious})

    @pytest.mark.parametrize("valid,desc", VALID_CSV_PATHS)
    def test_accept_valid_csv_path_at_construction(
        self, valid: str, desc: str
    ):
        """合法 CSV 路径在构造器中通过。"""
        executor = DuckDBExecutor(table_paths={"safe_table": valid})
        assert executor._table_paths == {"safe_table": valid}

    def test_reject_empty_csv_path_at_construction(self):
        """空 CSV 路径在构造器中被拒绝。"""
        with pytest.raises(ValueError, match="不能为空"):
            DuckDBExecutor(table_paths={"safe_table": ""})


# ════════════════════════════════════════════
# 集成测试——转义层纵深防线
# ════════════════════════════════════════════

class TestEscapingLayerDefense:
    """转义层纵深防线——即使绕过构造器校验，转义层仍阻止注入执行。

    这些测试通过直接修改 _table_paths 私有属性（绕过 __init__ 校验），
    验证 _load_tables() 中的 _render_sql_string_literal 转义
    能独立阻止 SQL 注入。
    """

    def test_bypassed_validation_still_blocked_by_escaping(self):
        """绕过构造器校验后，转义层将注入 payload 安全化为普通字符串。

        攻击 payload：
          safe.csv'); CREATE TABLE injected AS SELECT 42 AS x; --

        转义后变为 DuckDB 字符串字面量：
          'safe.csv''); CREATE TABLE injected AS SELECT 42 AS x; --'

        DuckDB 在 read_csv_auto 中读取路径时会因文件不存在而报错，
        CREATE TABLE injected 语句永远不会执行。
        """
        import duckdb

        malicious_path = (
            "safe.csv'); CREATE TABLE injected AS SELECT 42 AS x; --"
        )

        # 绕过构造器校验——直接赋值私有属性
        executor = DuckDBExecutor()
        executor._table_paths = {"safe_table": malicious_path}

        con = duckdb.connect(":memory:")

        # _load_tables 内部使用 _render_sql_string_literal 转义
        # 恶意路径被安全化为 read_csv_auto('safe.csv''); ...')
        # DuckDB 尝试打开名为 "safe.csv''); CREATE TABLE..." 的文件（失败）
        executor._load_tables(con)

        # 验证 injected 表未被创建
        try:
            result = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='injected'"
            ).fetchall()
            all_tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert len(result) == 0, (
                f"转义层防线失守！injected 表被成功创建。"
                f"所有表: {all_tables}"
            )
        finally:
            con.close()

    def test_escaped_path_does_not_create_rogue_table(self):
        """合法路径 + 注入后缀被转义后不会创建额外表。"""
        import duckdb

        # 构造一个看起来像真实文件路径的注入
        malicious_path = (
            "/data/safe.csv'); CREATE TABLE rogue AS SELECT 'pwned' AS x; --"
        )

        executor = DuckDBExecutor()
        executor._table_paths = {"t": malicious_path}
        con = duckdb.connect(":memory:")
        executor._load_tables(con)

        # rogue 表不应存在
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "rogue" not in table_names, (
            f"转义层防线失守！rogue 表被创建。现有表: {table_names}"
        )
        con.close()

    def test_escaped_newline_blocked(self):
        """含换行符的路径被转义后不会产生第二条语句。"""
        import duckdb

        malicious_path = "data.csv\nCREATE TABLE sneaky AS SELECT 1"

        executor = DuckDBExecutor()
        executor._table_paths = {"t": malicious_path}
        con = duckdb.connect(":memory:")
        executor._load_tables(con)

        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "sneaky" not in table_names, (
            f"转义层防线失守！sneaky 表被创建。现有表: {table_names}"
        )
        con.close()


# ════════════════════════════════════════════
# 端到端集成——正常场景不受影响
# ════════════════════════════════════════════

class TestEndToEndNormalPath:
    """正常 CSV 加载场景——验证安全加固不影响正常功能。"""

    def test_load_real_csv_fixture(self):
        """真实 CSV fixture 正常加载——安全加固无副作用。"""
        import os

        import duckdb

        # 使用项目真实的 CSV fixture
        fixtures_dir = os.path.join(
            os.path.dirname(__file__), "..", "fixtures", "sql"
        )
        csv_path = os.path.abspath(
            os.path.join(fixtures_dir, "test_fact.csv")
        )

        if not os.path.exists(csv_path):
            pytest.skip(f"CSV fixture 不存在: {csv_path}")

        executor = DuckDBExecutor(
            table_paths={"test_fact": csv_path}
        )
        con = duckdb.connect(":memory:")
        executor._load_tables(con)

        # 验证表被正常创建
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "test_fact" in table_names, (
            f"正常 CSV fixture 加载失败。现有表: {table_names}"
        )
        con.close()
