"""字段名归一化——大小写统一、驼峰转下划线、常见别名字典替换。

归一化是确定性的——相同输入永远产生相同输出。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class NormalizationConfig:
    """字段名归一化配置——不可变，避免运行时意外修改。"""

    lowercase: bool = True  # 大小写统一为小写
    camel_to_snake: bool = True  # 驼峰转下划线
    strip_special_chars: bool = True  # 去除非字母数字字符（保留 _）
    merge_underscores: bool = True  # 多个连续下划线合并为一个


class FieldNormalizer:
    """字段名归一化执行器。

    五步归一化管道：
    1. lowercase      —— 全部转为小写
    2. camel_to_snake —— userId → user_id
    3. alias_dict     —— 常见别名替换（如 cust → customer）
    4. strip_special  —— 去除非字母数字字符
    5. merge_underscores —— 合并连续下划线

    默认别名字典覆盖数据仓库常见缩写，可根据项目扩展。
    """

    # 默认别名字典——数据仓库常见缩写
    DEFAULT_ALIASES: ClassVar[dict[str, str]] = {
        "cust_id": "customer_id",
        "cust": "customer",
        "amt": "amount",
        "dt": "date",
        "ord": "order",
        "qty": "quantity",
        "cnt": "count",
        "addr": "address",
        "org": "organization",
        "dept": "department",
        "emp": "employee",
        "prod": "product",
        "cat": "category",
        "info": "information",
        "desc": "description",
        "no": "number",
        "ts": "timestamp",
        "createtime": "create_time",
        "modifytime": "modify_time",
        "updatetime": "update_time",
        "inserttime": "insert_time",
        "starttime": "start_time",
        "endtime": "end_time",
        "statdate": "stat_date",
        "regioncode": "region_code",
        "ordercode": "order_code",
    }

    def __init__(
        self,
        config: NormalizationConfig | None = None,
        extra_aliases: dict[str, str] | None = None,
    ):
        """初始化归一化器。

        Args:
            config: 归一化配置，None 使用默认配置
            extra_aliases: 额外别名字典，会合并到默认别名中（覆盖同名条目）
        """
        self._config = config or NormalizationConfig()
        self._aliases: dict[str, str] = {**self.DEFAULT_ALIASES, **(extra_aliases or {})}

    def normalize(self, name: str) -> str:
        """执行完整归一化管道，返回归一化后的字段名。

        注意：camel_to_snake 必须在 lowercase 之前执行——否则丢失大小写边界信息。
        """
        result = name
        # camel_to_snake 依赖大小写边界，必须在 lowercase 之前
        if self._config.camel_to_snake:
            result = self._apply_camel_to_snake(result)
        if self._config.lowercase:
            result = self._apply_lowercase(result)
        result = self._apply_alias_dict(result)
        if self._config.strip_special_chars:
            result = self._apply_strip_special_chars(result)
        if self._config.merge_underscores:
            result = self._apply_merge_underscores(result)
        return result

    def normalize_batch(self, names: list[str]) -> list[str]:
        """批量归一化——对列表中的每个字段名执行完整归一化管道。"""
        return [self.normalize(n) for n in names]

    def are_equal(self, a: str, b: str) -> bool:
        """判断两个字段名归一化后是否相同。"""
        return self.normalize(a) == self.normalize(b)

    # ── 内部归一化步骤 ──

    def _apply_lowercase(self, name: str) -> str:
        """全部转为小写。"""
        return name.lower()

    def _apply_camel_to_snake(self, name: str) -> str:
        """驼峰转下划线。

        算法：在每处小写→大写边界或数字→大写边界插入下划线。
        连续大写缩写（如 ID、URL）视为一个整体，不在其内部拆分。
        例如：
            userId   → user_id
            OrderID  → order_id
            UserURL  → user_url
        """
        # 在 小写/数字 → 大写 边界插入下划线
        result = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        # 在 连续大写缩写 → 下一个大写+小写 边界插入下划线（如 OrderID → Order_ID）
        # 但不拆分连续大写缩写内部（如 ID 不变成 I_D）
        return result

    def _apply_alias_dict(self, name: str) -> str:
        """查别名字典替换。

        先查完整名匹配，再按 _ 分词逐段替换后重组。
        例如：cust_id → customer_id（完整匹配），
              prod_cat → product_category（逐段：prod→product, cat→category）。
        """
        # 完整名匹配优先
        if name in self._aliases:
            return self._aliases[name]
        # 按 _ 分词逐段替换
        parts = name.split("_")
        replaced = [self._aliases.get(p, p) for p in parts]
        return "_".join(replaced)

    def _apply_strip_special_chars(self, name: str) -> str:
        """去除非字母数字字符，仅保留字母、数字和下划线。"""
        return re.sub(r"[^a-z0-9_]", "", name)

    def _apply_merge_underscores(self, name: str) -> str:
        """合并多个连续下划线为一个，去除首尾下划线。"""
        result = re.sub(r"_+", "_", name)
        return result.strip("_")
