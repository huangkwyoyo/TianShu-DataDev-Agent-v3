"""RelationshipPlanner._parse_llm_response() Fixture 测试。

验证 LLM JSON 输出 → Join 候选 dict 列表的解析和校验逻辑。
覆盖全部 6 项校验规则：H1(字段存在/表别名有效/禁止自引用)、H2(join_type/confidence 降级)、空列表/缺键容错。

所有测试使用 JSON fixture 文件模拟 LLM 输出，不依赖网络或 API Key。
"""

from __future__ import annotations

import json
import os

import pytest

from tianshu_datadev.developer_spec.models import (
    FieldSource,
    ManifestColumn,
    ManifestTable,
    SourceManifest,
)
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _read_fixture(name: str) -> dict:
    """读取 llm_responses/relationship/ 下的 JSON fixture。"""
    path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "llm_responses", "relationship", name,
    )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_mock_manifest() -> SourceManifest:
    """构造最小合法 SourceManifest——含 3 张表，与 fixture 数据对应。

    orders: user_id, amount, product_id, order_date, id, status
    users:  id, name, email
    products: id, product_name, price
    """
    return SourceManifest(
        manifest_id="test_manifest",
        spec_hash="abc123",
        tables=[
            ManifestTable(
                table_ref="orders",
                source_table="test.orders",
                columns=[
                    ManifestColumn(column_name="id", normalized_name="id", data_type="int", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="user_id", normalized_name="user_id", data_type="int", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="amount", normalized_name="amount", data_type="decimal", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="product_id", normalized_name="product_id", data_type="int", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="order_date", normalized_name="order_date", data_type="date", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="status", normalized_name="status", data_type="varchar", source=FieldSource.DEVELOPER_SPEC),
                ],
            ),
            ManifestTable(
                table_ref="users",
                source_table="test.users",
                columns=[
                    ManifestColumn(column_name="id", normalized_name="id", data_type="int", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="name", normalized_name="name", data_type="varchar", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="email", normalized_name="email", data_type="varchar", source=FieldSource.DEVELOPER_SPEC),
                ],
            ),
            ManifestTable(
                table_ref="products",
                source_table="test.products",
                columns=[
                    ManifestColumn(column_name="id", normalized_name="id", data_type="int", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="product_name", normalized_name="product_name", data_type="varchar", source=FieldSource.DEVELOPER_SPEC),
                    ManifestColumn(column_name="price", normalized_name="price", data_type="decimal", source=FieldSource.DEVELOPER_SPEC),
                ],
            ),
        ],
    )


# ════════════════════════════════════════════
# 测试类
# ════════════════════════════════════════════


class TestRelationshipPlannerParseLLM:
    """验证 _parse_llm_response 对各种 LLM JSON 输出的处理。"""

    # ── 正常路径 ──

    def test_parse_normal_joins_all_retained(self):
        """合法 Join 候选——全部保留。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("normal.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 2
        assert result[0]["left_table"] == "orders"
        assert result[0]["right_table"] == "users"
        assert result[0]["left_key"] == "user_id"
        assert result[0]["right_key"] == "id"
        assert result[0]["join_type"] == "INNER"
        assert result[0]["confidence"] == "high"
        assert result[1]["left_table"] == "orders"
        assert result[1]["right_table"] == "products"
        assert result[1]["join_type"] == "LEFT"

    # ── H1：字段名不存在 → 丢弃 ──

    def test_parse_rejects_field_not_in_manifest(self):
        """left_key 不在 manifest 中——静默丢弃。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("field_not_found.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 0, "非法字段名应被丢弃"

    # ── H1：表别名无效 → 丢弃 ──

    def test_parse_rejects_invalid_table_alias(self):
        """left_table 不在 manifest 中——静默丢弃。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("table_alias_invalid.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 0, "不存在的表别名应被丢弃"

    # ── H1：自引用 Join → 丢弃 ──

    def test_parse_rejects_self_join(self):
        """left_table == right_table——静默丢弃。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("self_join.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 0, "自引用 Join 应被丢弃"

    # ── H2：非法 join_type → 降级为 INNER ──

    def test_parse_downgrades_invalid_join_type(self):
        """join_type="CROSS" 不在枚举中——降级为 INNER，不丢弃。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("invalid_join_type.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 1, "非法 join_type 应降级而非丢弃"
        assert result[0]["join_type"] == "INNER"
        # 其他字段应保留
        assert result[0]["left_table"] == "orders"
        assert result[0]["left_key"] == "user_id"

    # ── 空列表 ──

    def test_parse_empty_list_returns_empty(self):
        """inferred_joins=[] → 返回空列表。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("empty_list.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert result == []

    # ── 缺少键 ──

    def test_parse_missing_key_returns_empty(self):
        """JSON 不含 inferred_joins 键——返回空列表。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("missing_key.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert result == []

    # ── 混合合法/非法 ──

    def test_parse_mixed_valid_invalid(self):
        """2 合法 + 2 非法——只保留合法项。"""
        planner = RelationshipPlanner()
        raw = _read_fixture("mixed_valid_invalid.json")
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 2, "应只保留 2 个合法候选"
        # 验证保留下来的都是合法项
        for item in result:
            assert item["left_table"] in ("orders",)
            assert item["right_table"] in ("users", "products")

    # ── confidence 降级 ──

    def test_parse_downgrades_invalid_confidence(self):
        """confidence 不在枚举中——降级为 medium。"""
        planner = RelationshipPlanner()
        raw = {
            "inferred_joins": [
                {
                    "left_table": "orders",
                    "right_table": "users",
                    "left_key": "user_id",
                    "right_key": "id",
                    "join_type": "INNER",
                    "confidence": "super_high",
                    "reasoning": "非法 confidence 值",
                }
            ]
        }
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 1
        assert result[0]["confidence"] == "medium"

    # ── reasoning 缺失默认值 ──

    def test_parse_missing_reasoning_defaults_to_empty(self):
        """reasoning 字段缺失——默认空字符串。"""
        planner = RelationshipPlanner()
        raw = {
            "inferred_joins": [
                {
                    "left_table": "orders",
                    "right_table": "users",
                    "left_key": "user_id",
                    "right_key": "id",
                    "join_type": "INNER",
                    "confidence": "high",
                }
            ]
        }
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 1
        assert result[0]["reasoning"] == ""

    # ── right_key 不存在 → 丢弃 ──

    def test_parse_rejects_invalid_right_key(self):
        """right_key 不在 manifest 中——静默丢弃。"""
        planner = RelationshipPlanner()
        raw = {
            "inferred_joins": [
                {
                    "left_table": "orders",
                    "right_table": "users",
                    "left_key": "user_id",
                    "right_key": "nonexistent",
                    "join_type": "INNER",
                    "confidence": "high",
                    "reasoning": "right_key 不存在",
                }
            ]
        }
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 0

    # ── right_table 无效 → 丢弃 ──

    def test_parse_rejects_invalid_right_table(self):
        """right_table 不在 manifest 中——静默丢弃。"""
        planner = RelationshipPlanner()
        raw = {
            "inferred_joins": [
                {
                    "left_table": "orders",
                    "right_table": "ghost",
                    "left_key": "user_id",
                    "right_key": "id",
                    "join_type": "INNER",
                    "confidence": "high",
                    "reasoning": "right_table 不存在",
                }
            ]
        }
        manifest = _build_mock_manifest()

        result = planner._parse_llm_response(raw, manifest)

        assert len(result) == 0
