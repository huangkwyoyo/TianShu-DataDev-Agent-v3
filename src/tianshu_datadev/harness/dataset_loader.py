"""DatasetLoader——统一的 5 类数据集加载器。

从 harness/datasets/{golden,rejection,attack,performance,regression}/ 子目录加载 JSON fixture。
每个类别有独立的子目录和 JSON 格式约定。
"""

from __future__ import annotations

import json
from pathlib import Path

from .models import DatasetCategory, HarnessCase

# 默认数据集根目录——相对于项目根
_DEFAULT_DATASET_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "harness" / "datasets"
)

# 类别 → 子目录名映射
_CATEGORY_DIR_MAP: dict[DatasetCategory, str] = {
    DatasetCategory.GOLDEN: "golden",
    DatasetCategory.REJECTION: "rejection",
    DatasetCategory.ATTACK: "attack",
    DatasetCategory.PERFORMANCE: "performance",
    DatasetCategory.REGRESSION: "regression",
}


class DatasetLoader:
    """统一的数据集加载器——按类别加载 JSON fixture。

    支持按单类别加载或全量加载。
    不解析或验证预期内容——仅转为 HarnessCase 列表。
    """

    def __init__(self, base_dir: str | None = None):
        """初始化加载器。

        Args:
            base_dir: 数据集根目录。默认为 harness/datasets/。
        """
        self._base_dir = Path(base_dir) if base_dir else _DEFAULT_DATASET_DIR
        self._cache: dict[DatasetCategory, list[HarnessCase]] = {}

    def load_all(self) -> dict[DatasetCategory, list[HarnessCase]]:
        """加载全部 5 个数据集分类。

        Returns:
            dict[DatasetCategory, list[HarnessCase]]——每类数据集对应的用例列表。
        """
        result: dict[DatasetCategory, list[HarnessCase]] = {}
        for category in DatasetCategory:
            result[category] = self.load_category(category)
        return result

    def load_category(self, category: DatasetCategory) -> list[HarnessCase]:
        """加载指定分类的全部用例（含缓存）。

        遍历子目录下的所有 *.json 文件，跳过 __init__ 文件。
        自动为缺少 category 字段的用例填入当前分类值。

        Args:
            category: 要加载的数据集分类。

        Returns:
            list[HarnessCase]——该分类的全部用例。目录不存在时返回空列表。
        """
        if category in self._cache:
            return self._cache[category]

        subdir = self._get_subdir(category)
        if not subdir.is_dir():
            self._cache[category] = []
            return []

        cases: list[HarnessCase] = []
        for filepath in sorted(subdir.glob("*.json")):
            if filepath.stem == "__init__":
                continue
            file_cases = self._load_json_file(filepath, category)
            cases.extend(file_cases)

        self._cache[category] = cases
        return cases

    def _get_subdir(self, category: DatasetCategory) -> Path:
        """获取分类对应的子目录路径。"""
        dirname = _CATEGORY_DIR_MAP[category]
        return self._base_dir / dirname

    def _load_json_file(
        self, filepath: Path, category: DatasetCategory,
    ) -> list[HarnessCase]:
        """加载单个 JSON fixture 文件为 HarnessCase 列表。

        支持 JSON 数组和单个对象两种格式。
        """
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return [
                self._parse_case(item, category, filepath.name) for item in data
            ]
        elif isinstance(data, dict):
            return [self._parse_case(data, category, filepath.name)]
        else:
            raise ValueError(
                f"数据集文件 {filepath} 格式无效——"
                f"必须是 JSON 数组或对象，实际为 {type(data).__name__}"
            )

    # HarnessCase 允许的字段——attack/ 目录的 JSON 含 SecurityCase 特有字段
    # （attack_vector、expected_protection_layer、expected_rejection_pattern、
    # payload），需在构造前过滤
    _HARNESS_CASE_FIELDS = frozenset({
        "case_id", "category", "description",
        "developer_spec", "expected", "attack", "human_review",
    })

    def _parse_case(
        self, data: dict, category: DatasetCategory, filename: str,
    ) -> HarnessCase:
        """将 JSON dict 解析为 HarnessCase，自动填入 category。

        attack/ 目录的 JSON fixture 使用 SecurityCase 格式（含 attack_vector、
        payload 等字段），这些字段对 HarnessCase 而言是 extra 字段。
        此处过滤掉非 HarnessCase 字段，确保 StrictModel(extra="forbid") 通过。
        """
        if "category" not in data:
            data["category"] = category.value

        # 过滤仅保留 HarnessCase 定义的字段——attack/ JSON 含额外字段
        filtered = {
            k: v for k, v in data.items()
            if k in self._HARNESS_CASE_FIELDS
        }
        return HarnessCase(**filtered)
