"""攻击数据集 fixture 验证测试——确保 JSON fixture 格式正确、所有 case 可加载。

Phase 4C：验证 haraness/datasets/attack/ 下所有 JSON fixture 文件
结构合法、字段完整、attack_vector 与文件名一致。
"""

from __future__ import annotations

import json
from pathlib import Path

from tianshu_datadev.harness.models import AttackVector, SecurityCase

# 攻击数据集目录——相对于项目根目录
_ATTACK_DATASET_DIR = Path(__file__).resolve().parent.parent.parent / "harness" / "datasets" / "attack"

# 预期的文件→AttackVector 映射
_EXPECTED_FILES: dict[str, AttackVector] = {
    "prompt_injection.json": AttackVector.PROMPT_INJECTION,
    "sql_injection.json": AttackVector.SQL_INJECTION,
    "schema_extra.json": AttackVector.SCHEMA_EXTRA,
    "undeclared_ref.json": AttackVector.UNDECLARED_REF,
    "join_error.json": AttackVector.JOIN_ERROR_INFERENCE,
    "write_privilege.json": AttackVector.WRITE_PRIVILEGE,
}

# SecurityCase 必需字段
_REQUIRED_FIELDS = {
    "case_id", "attack_vector", "description",
    "expected_protection_layer", "expected_rejection_pattern",
}

# 合法的 protection_layer 值
_VALID_PROTECTION_LAYERS = {"schema", "validator", "render", "write_validator"}


class TestAttackDatasetFixtures:
    """验证攻击数据集 JSON fixture 的完整性和可加载性。"""

    def test_all_fixture_files_exist(self):
        """6 个攻击向量的 JSON fixture 文件全部存在。"""
        for filename, vector in _EXPECTED_FILES.items():
            filepath = _ATTACK_DATASET_DIR / filename
            assert filepath.is_file(), (
                f"缺少攻击数据集 fixture：{filepath}（{vector.value}）"
            )

    def test_all_fixtures_loadable(self):
        """所有 JSON fixture 文件可解析为 SecurityCase 列表。"""
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            assert isinstance(data, list), (
                f"{filename} 应为 JSON 数组"
            )
            assert len(data) >= 1, (
                f"{filename} 至少应有 1 个测试用例"
            )
            for case_data in data:
                # 验证可通过 SecurityCase 构造
                case = SecurityCase(**case_data)
                assert case.attack_vector is not None

    def test_all_vectors_have_cases(self):
        """每种攻击向量至少 1 个用例。"""
        covered = set()
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                case = SecurityCase(**case_data)
                covered.add(case.attack_vector)
        for vector in AttackVector:
            assert vector in covered, (
                f"攻击向量 {vector.value} 缺少测试用例"
            )

    def test_attack_vector_matches_filename(self):
        """每个 fixture 中的 attack_vector 与文件名对应。"""
        for filename, expected_vector in _EXPECTED_FILES.items():
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                case = SecurityCase(**case_data)
                assert case.attack_vector == expected_vector, (
                    f"{filename} 中 case {case.case_id} 的 attack_vector="
                    f"{case.attack_vector.value} 与预期 {expected_vector.value} 不符"
                )

    def test_required_fields_complete(self):
        """每个 case 包含全部必需字段。"""
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                for field in _REQUIRED_FIELDS:
                    assert field in case_data, (
                        f"{filename} 中 case {case_data.get('case_id', '?')} "
                        f"缺少字段 '{field}'"
                    )

    def test_case_ids_are_unique(self):
        """所有 case 的 case_id 全局唯一。"""
        seen = set()
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                case = SecurityCase(**case_data)
                assert case.case_id not in seen, (
                    f"重复的 case_id：{case.case_id}（出现在 {filename}）"
                )
                seen.add(case.case_id)

    def test_protection_layer_is_valid(self):
        """expected_protection_layer 必须是已知值。"""
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                case = SecurityCase(**case_data)
                assert case.expected_protection_layer in _VALID_PROTECTION_LAYERS, (
                    f"{case.case_id}: 未知 protection_layer "
                    f"'{case.expected_protection_layer}'——"
                    f"须为 {_VALID_PROTECTION_LAYERS} 之一"
                )

    def test_payload_present_for_all_cases(self):
        """每个 case 的 payload 非空且含可操作的攻击参数。

        payload 是 evaluator 构造攻击载荷的唯一事实来源——
        空 payload 意味着 evaluator 将使用硬编码默认值，导致 fixture 描述与实际测试脱节。
        """
        for filename in _EXPECTED_FILES:
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for case_data in data:
                case = SecurityCase(**case_data)
                assert isinstance(case.payload, dict), (
                    f"{case.case_id}: payload 必须是 dict，实际为 "
                    f"{type(case.payload).__name__}"
                )
                assert len(case.payload) >= 1, (
                    f"{case.case_id}: payload 不得为空——"
                    f"evaluator 必须从 payload 读取攻击参数，禁止硬编码默认值"
                )

    # 每种攻击向量的 payload 必需 key 约定
    _PAYLOAD_REQUIRED_KEYS: dict[str, set[str]] = {
        "PROMPT_INJECTION": {"extra_field"},
        "SQL_INJECTION": {"malicious_value", "target_type", "target_field"},
        "SCHEMA_EXTRA": {"extra_field"},
        "UNDECLARED_REF": {"table_ref"},
        "JOIN_ERROR_INFERENCE": {"scenario"},
        "WRITE_PRIVILEGE": {"test_strategy"},
    }

    def test_payload_keys_match_vector(self):
        """每种攻击向量的 payload 包含约定的必需 key。

        此测试确保 fixture 的 payload 与 evaluator 的读取逻辑一致——
        如果新增 attack vector 或修改 payload 约定，需同步更新此映射。
        """
        for filename, expected_vector in _EXPECTED_FILES.items():
            filepath = _ATTACK_DATASET_DIR / filename
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            required_keys = self._PAYLOAD_REQUIRED_KEYS.get(
                expected_vector.value, set()
            )
            for case_data in data:
                case = SecurityCase(**case_data)
                payload = case.payload
                missing = required_keys - set(payload.keys())
                assert not missing, (
                    f"{case.case_id} ({expected_vector.value}): "
                    f"payload 缺少必需 key: {missing}——"
                    f"约定必需 key 为 {required_keys}"
                )
