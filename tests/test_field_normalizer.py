"""测试 FieldNormalizer 的归一化规则。"""


from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer


class TestLowercase:
    """大小写统一测试。"""

    def test_uppercase_to_lower(self):
        normalizer = FieldNormalizer()
        assert normalizer.normalize("USERID") == "userid"

    def test_mixed_case(self):
        normalizer = FieldNormalizer()
        assert normalizer.normalize("UserName") == "user_name"


class TestCamelToSnake:
    """驼峰转下划线测试。"""

    def test_simple_camel(self):
        normalizer = FieldNormalizer()
        assert normalizer.normalize("userId") == "user_id"

    def test_consecutive_uppercase_abbreviation(self):
        """连续大写缩写（如 ID）视为一个整体，不拆分内部。"""
        normalizer = FieldNormalizer()
        result = normalizer.normalize("OrderID")
        assert "order_id" in result

    def test_multiple_camel_words(self):
        normalizer = FieldNormalizer()
        result = normalizer.normalize("userLoginCount")
        assert result == "user_login_count"


class TestAliasDict:
    """别名字典替换测试。"""

    def test_exact_match(self):
        """完整名匹配优先——cust_id → customer_id。"""
        normalizer = FieldNormalizer()
        assert normalizer.normalize("cust_id") == "customer_id"

    def test_partial_match(self):
        """按 _ 分词逐段替换——prod_cat → product_category。"""
        normalizer = FieldNormalizer()
        assert normalizer.normalize("prod_cat") == "product_category"

    def test_amt_alias(self):
        """常见缩写——amt → amount。"""
        normalizer = FieldNormalizer()
        assert normalizer.normalize("amt") == "amount"


class TestStripSpecialChars:
    """特殊字符去除测试。"""

    def test_chinese_chars_removed(self):
        """中文字符被去除，保留字母数字和下划线。"""
        normalizer = FieldNormalizer()
        result = normalizer.normalize("用户ID")
        assert "id" in result

    def test_hyphen_removed(self):
        """连字符被去除。"""
        normalizer = FieldNormalizer()
        result = normalizer.normalize("user-id")
        assert "-" not in result
        assert "user" in result


class TestFullNormalization:
    """完整归一化管道测试。"""

    def test_pipeline_integration(self):
        """所有步骤串联——驼峰 + 别名 + 特殊字符。"""
        normalizer = FieldNormalizer()
        # "CustID" → camel "Cust_ID" → lowercase "cust_id" → alias "customer_id"
        result = normalizer.normalize("CustID")
        assert result == "customer_id"  # cust→customer

    def test_are_equal(self):
        """归一化后比较——两个不同写法同一字段。"""
        normalizer = FieldNormalizer()
        assert normalizer.are_equal("UserID", "user_id")

    def test_normalize_batch(self):
        """批量归一化。"""
        normalizer = FieldNormalizer()
        result = normalizer.normalize_batch(["UserID", "OrderAmt", "CustID"])
        assert len(result) == 3
        assert result[0] == "user_id"
        assert result[1] == "order_amount"
        assert result[2] == "customer_id"
