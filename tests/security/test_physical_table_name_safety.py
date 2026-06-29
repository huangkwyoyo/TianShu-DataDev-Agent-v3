"""物理表名注入安全测试——验证 SafePhysicalTableName 在 Schema 层拒绝恶意输入。

Phase 3B 安全加固——schema.source_table / table_mapping / table_paths 等
所有表示物理表名的 str 字段改用 SafePhysicalTableName 约束类型，
在 Pydantic Schema 层即拒绝非法字符。

测试覆盖：
- 每个 SafePhysicalTableName 字段的恶意值拒绝（18+ 注入模式）
- 每个 SafePhysicalTableName 字段的合法值通过（含 Unicode / schema.table）
- 边界情况：空字符串、Unicode 表名、点号分隔、数字开头
- Compiler 集成：table_mapping 恶意值拒绝
- Executor 集成：table_paths 恶意值拒绝
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.developer_spec.models import (
    InputTableDecl,
    ManifestTable,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor

# ════════════════════════════════════════════
# 恶意输入样本——覆盖物理表名注入常见向量
# ════════════════════════════════════════════

MALICIOUS_SAMPLES: list[tuple[str, str]] = [
    # ── 语句终止 / 多语句注入 ──
    ("x; DROP TABLE users; --", "分号+多语句注入"),
    ("x; DELETE FROM sensitive; --", "分号+DELETE"),
    ("x; %00DROP TABLE", "空字节注入（%00 含特殊字符）"),

    # ── 引号逃逸 ──
    ("x' OR '1'='1", "单引号布尔注入"),
    ('x" OR "1"="1', "双引号布尔注入"),
    ("x`); DELETE FROM sensitive; --", "反引号逃逸+DELETE"),

    # ── UNION 注入 ──
    ("x UNION SELECT * FROM passwords", "UNION 注入"),
    ("x UNION ALL SELECT 1,2,3", "UNION ALL 注入"),

    # ── 注释逃逸 ──
    ("x--\nDELETE FROM users", "行注释逃逸+换行注入"),
    ("x/**/OR/**/1=1", "块注释注入"),

    # ── 空格/空白 ──
    ("a b", "含空格"),
    ("a\tb", "含制表符"),
    ("a\nb", "含换行符"),
    ("a\rb", "含回车符"),

    # ── 特殊字符 ──
    ("a,b", "含逗号"),
    ("a=b", "含等号"),
    ("a(b", "含左括号"),
    ("a)b", "含右括号"),
]

# 合法输入样本——含简单名、限定名、Unicode 名
VALID_SAMPLES: list[tuple[str, str]] = [
    ("user_info", "简单英文表名"),
    ("sales_data_2024", "含数字和下划线"),
    ("public.users", "schema.table 限定名"),
    ("dwd.用户行为表", "含中文字符的限定名（真实数仓场景）"),
    ("_temp_agg", "下划线开头"),
    ("table1", "字母数字组合"),
    ("db.schema.table", "禁用——仅支持两级限定名（暂不测试）"),
]

# 仅简单表名和两级限定名
VALID_SIMPLE_AND_QUALIFIED: list[str] = [
    "user_info",
    "sales_data_2024",
    "public.users",
    "dwd.用户行为表",
    "_temp_agg",
    "table1",
    "订单表",
    "CUSTOMERS",
    "order_items",
    "schema_public.用户订单表_2024",
]


# ════════════════════════════════════════════
# source_table 字段统一测试（InputTableDecl + ManifestTable）
# ════════════════════════════════════════════

_SOURCE_TABLE_MODELS = [
    ("InputTableDecl", InputTableDecl, lambda v: {"table_alias": "t", "source_table": v}),
    ("ManifestTable", ManifestTable, lambda v: {"table_ref": "t", "source_table": v}),
]


class TestSourceTableField:
    """InputTableDecl 和 ManifestTable 的 source_table 字段使用相同的 SafePhysicalTableName 约束。"""

    @pytest.mark.parametrize("label,model_cls,kwargs_fn", _SOURCE_TABLE_MODELS)
    @pytest.mark.parametrize("malicious,desc", MALICIOUS_SAMPLES)
    def test_reject_malicious_source_table(self, label: str, model_cls, kwargs_fn, malicious: str, desc: str):
        """恶意物理表名在 {InputTableDecl|ManifestTable} 构造时被拒绝。"""
        with pytest.raises(ValidationError, match="source_table"):
            model_cls(**kwargs_fn(malicious))

    @pytest.mark.parametrize("label,model_cls,kwargs_fn", _SOURCE_TABLE_MODELS)
    @pytest.mark.parametrize("valid", VALID_SIMPLE_AND_QUALIFIED)
    def test_accept_valid_source_table(self, label: str, model_cls, kwargs_fn, valid: str):
        """合法物理表名在 {InputTableDecl|ManifestTable} 构造时通过。"""
        table = model_cls(**kwargs_fn(valid))
        assert table.source_table == valid

    def test_reject_empty_source_table(self):
        """空字符串物理表名被拒绝——两个模型均拒绝。"""
        for label, model_cls, kwargs_fn in _SOURCE_TABLE_MODELS:
            with pytest.raises(ValidationError, match="source_table"):
                model_cls(**kwargs_fn(""))


# ════════════════════════════════════════════
# DuckDbSqlCompiler table_mapping 校验测试
# ════════════════════════════════════════════

class TestCompilerTableMapping:
    """Compiler.__init__ 校验 table_mapping 的 key 和 value。"""

    def test_reject_malicious_mapping_value(self):
        """table_mapping 的 value（物理表名）含注入字符时被拒绝。"""
        with pytest.raises(ValueError, match="物理表名"):
            DuckDbSqlCompiler(table_mapping={"t": "x; DROP TABLE users; --"})

    def test_reject_malicious_mapping_key(self):
        """table_mapping 的 key（表别名）含非法字符时被拒绝。"""
        with pytest.raises(ValueError, match="table_mapping"):
            DuckDbSqlCompiler(table_mapping={"x y": "users"})

    def test_reject_empty_mapping_key(self):
        """table_mapping 的 key 为空字符串时被拒绝。"""
        with pytest.raises(ValueError, match="不能为空"):
            DuckDbSqlCompiler(table_mapping={"": "users"})

    def test_reject_empty_mapping_value(self):
        """table_mapping 的 value 为空字符串时被拒绝。"""
        with pytest.raises(ValueError, match="物理表名不能为空"):
            DuckDbSqlCompiler(table_mapping={"t": ""})

    def test_accept_valid_mapping(self):
        """合法 table_mapping 通过校验。"""
        compiler = DuckDbSqlCompiler(table_mapping={
            "t": "public.user_info",
            "o": "dwd.订单表",
        })
        assert compiler._resolve_table("t") == "public.user_info"
        assert compiler._resolve_table("o") == "dwd.订单表"

    def test_none_mapping_accepted(self):
        """table_mapping=None 时通过（默认行为——table_ref 自身作为物理表名）。"""
        compiler = DuckDbSqlCompiler(table_mapping=None)
        assert compiler._resolve_table("t") == "t"

    def test_empty_dict_mapping_accepted(self):
        """table_mapping={} 时通过。"""
        compiler = DuckDbSqlCompiler(table_mapping={})
        assert compiler._resolve_table("t") == "t"

    # ── 18 种注入向量逐一覆盖 ──

    @pytest.mark.parametrize("malicious,desc", [
        ("x; DROP TABLE users; --", "分号+DROP"),
        ("x' OR '1'='1", "单引号注入"),
        ('x" OR "1"="1', "双引号注入"),
        ("x`; DELETE FROM x; --", "反引号注入"),
        ("x UNION SELECT * FROM pw", "UNION注入"),
        ("x--\nDELETE FROM users", "注释+换行"),
        ("x/**/OR/**/1=1", "块注释"),
        ("a b", "空格"),
        ("a\tb", "制表符"),
        ("a\nb", "换行符"),
        ("a,b", "逗号"),
        ("a=b", "等号"),
        ("a(b", "左括号"),
        ("a)b", "右括号"),
        ("a[b", "左方括号"),
        ("a]b", "右方括号"),
        ("a/b", "斜杠"),
        ("a\\b", "反斜杠"),
    ])
    def test_reject_all_injection_patterns_in_mapping_value(
        self, malicious: str, desc: str
    ):
        """覆盖 18 种注入模式——table_mapping value 逐一拒绝。"""
        with pytest.raises(ValueError):
            DuckDbSqlCompiler(table_mapping={"t": malicious})


# ════════════════════════════════════════════
# DuckDBExecutor table_paths 校验测试
# ════════════════════════════════════════════

class TestExecutorTablePaths:
    """Executor.__init__ 校验 table_paths 的 key（物理表名）。"""

    def test_reject_malicious_table_name_in_paths(self):
        """table_paths 的 key 含注入字符时被拒绝。"""
        with pytest.raises(ValueError, match="物理表名"):
            DuckDBExecutor(table_paths={"x; DROP TABLE users; --": "data/users.csv"})

    def test_reject_empty_table_name_in_paths(self):
        """table_paths 的 key 为空字符串时被拒绝。"""
        with pytest.raises(ValueError, match="不能为空"):
            DuckDBExecutor(table_paths={"": "data/users.csv"})

    def test_accept_valid_table_names_in_paths(self):
        """合法 table_paths 通过校验。"""
        executor = DuckDBExecutor(table_paths={
            "public.user_info": "data/users.csv",
            "dwd.订单表": "data/orders.csv",
        })
        assert len(executor._table_paths) == 2

    def test_none_table_paths_accepted(self):
        """table_paths=None 时通过。"""
        executor = DuckDBExecutor(table_paths=None)
        assert executor._table_paths == {}


# ════════════════════════════════════════════
# SafePhysicalTableName 边界情况
# ════════════════════════════════════════════

class TestSafePhysicalTableNameEdgeCases:
    """SafePhysicalTableName 边界情况——Unicode / 特殊格式。"""

    @pytest.mark.parametrize("valid,desc", [
        ("t", "单字符"),
        ("_private", "下划线开头"),
        ("a" * 64, "64 字符长表名"),
        ("public." + "a" * 50, "长限定名"),
        ("订单表", "纯中文表名"),
        ("схема.таблица", "Cyrillic 限定名"),
        ("schema.테이블", "Hangul 限定名"),
    ])
    def test_accept_edge_case_valid_names(self, valid: str, desc: str):
        """边界合法物理表名通过校验。"""
        # 通过 InputTableDecl 间接测试 SafePhysicalTableName
        table = InputTableDecl(table_alias="t", source_table=valid)
        assert table.source_table == valid

    @pytest.mark.parametrize("invalid,desc", [
        ("", "空字符串"),
        (" ", "纯空格"),
        (".", "纯点号"),
        ("a..b", "双点号"),
        (".leading", "点号开头"),
        ("trailing.", "点号结尾"),
    ])
    def test_reject_edge_case_invalid_names(self, invalid: str, desc: str):
        """边界非法物理表名被拒绝。"""
        with pytest.raises((ValidationError, ValueError)):
            InputTableDecl(table_alias="t", source_table=invalid)

    def test_digit_start_rejected_by_regex(self):
        """数字开头的表名被 allowlist 正则拒绝（未加引号 SQL 标识符不允许数字开头）。"""
        with pytest.raises(ValidationError, match="source_table"):
            InputTableDecl(table_alias="t", source_table="123_table")


# ════════════════════════════════════════════
# 集成测试——Compiler + Executor 端到端安全
# ════════════════════════════════════════════

class TestEndToEndTableNameSafety:
    """端到端验证——合法物理表名经 Compiler → Executor 生成安全 SQL。"""

    def test_compiler_emits_safe_table_name(self):
        """Compiler 使用合法物理表名渲染 SQL——无注入风险。"""
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
        )

        # 构造含合法物理表名的 SqlBuildPlan
        plan = SqlBuildPlan(
            plan_id="test_safe_table",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t",
                    required_columns=[
                        ColumnRef(
                            table_ref="t",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
        )

        # 使用合法 table_mapping
        compiler = DuckDbSqlCompiler(
            table_mapping={"t": "public.user_info"}
        )
        result = compiler.compile(plan)

        # SQL 中应包含合法的物理表名——无注入片段
        assert "public.user_info AS t" in result.sql
        assert "DROP" not in result.sql
        assert "DELETE" not in result.sql
        assert ";" not in result.sql  # 单语句 SQL 不含分号

    def test_compiler_with_unicode_table_name(self):
        """Compiler 渲染含 Unicode 表名的 SQL——端到端确定性。

        列名（SafeIdentifier）仅支持 ASCII，物理表名（SafePhysicalTableName）
        支持 Unicode。SQL 中被渲染的是物理表名而非列名本身。
        """
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
        )

        plan = SqlBuildPlan(
            plan_id="test_unicode_table",
            spec_hash="abc456",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="ua",
                    required_columns=[
                        ColumnRef(
                            table_ref="ua",
                            column_name="user_id",  # SafeIdentifier 仅支持 ASCII
                            normalized_name="user_id",
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler(
            table_mapping={"ua": "dwd.用户行为表"}
        )
        result = compiler.compile(plan)

        # 确认 SQL 包含中文物理表名且无注入片段
        assert "dwd.用户行为表 AS ua" in result.sql
        assert result.sql_sha256  # 确定性 hash 正常生成

    def test_none_mapping_uses_table_ref_directly(self):
        """table_mapping=None 时使用 table_ref 自身——SafeIdentifier 已保护。"""
        from tianshu_datadev.planning.models import ColumnRef
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
            SqlBuildPlan,
        )

        plan = SqlBuildPlan(
            plan_id="test_no_mapping",
            spec_hash="abc789",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="users",
                    required_columns=[
                        ColumnRef(
                            table_ref="users",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
        )

        compiler = DuckDbSqlCompiler()  # 无 table_mapping
        result = compiler.compile(plan)

        # table_ref 已是 SafeIdentifier——不会包含注入字符
        assert "users AS users" in result.sql
