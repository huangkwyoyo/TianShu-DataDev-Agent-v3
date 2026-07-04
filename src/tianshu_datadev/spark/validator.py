"""Phase 6 SparkStaticValidator——PySpark DSL AST 硬门禁。

基于 AST call-chain 分类（不是简单的字符串匹配），区分：
- F.count(...) → 允许（聚合函数）
- df.count()  → 禁止（DataFrame action）
- Window.orderBy(...) → 允许
"""

from __future__ import annotations

import ast
from typing import ClassVar

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# 校验错误模型
# ════════════════════════════════════════════


class SparkValidationError(StrictModel):
    """Static Validator 发现的单个校验错误。"""

    error_code: str          # "E601" / "E602" / ...
    line_number: int
    category: str            # "FORBIDDEN_API" / "UNSAFE_IMPORT" / ...
    detail: str
    suggestion: str | None = None


class SparkValidationResult(StrictModel):
    """Static Validator 的校验结果。"""

    is_valid: bool
    errors: list[SparkValidationError] = Field(default_factory=list)
    validated_code_hash: str | None = None


# ════════════════════════════════════════════
# 禁止 API 清单
# ════════════════════════════════════════════

# DataFrame Action 方法——禁止调用（会触发实际计算）
_FORBIDDEN_ACTIONS: set[str] = {
    "collect", "count", "first", "head", "take", "show",
    "toPandas", "toDF", "cache", "persist", "unpersist",
    "foreach", "foreachPartition",
}

# DataFrame Sink 方法——禁止写入
_FORBIDDEN_SINKS: set[str] = {
    "write", "writeStream", "save", "saveAsTable",
    "insertInto", "insertIntoJDBC",
}

# 禁止的属性和方法组合
_FORBIDDEN_SPARK_METHODS: set[str] = {
    "spark.read", "spark.table", "spark.sql", "spark.range",
    "spark.createDataFrame", "spark.catalog",
}

# 禁止的函数调用
_FORBIDDEN_FUNCTIONS: set[str] = {
    "eval", "exec", "compile", "__import__",
}

# 禁止的导入模块
_FORBIDDEN_IMPORTS: dict[str, str] = {
    "subprocess": "E602",
    "os.system": "E602",
    "shlex": "E602",
    "socket": "E602",
    "requests": "E602",
    "urllib": "E602",
}

# PySpark 白名单函数前缀（F.xxx, Window.xxx, fn.xxx）
_ALLOWED_FUNCTION_PREFIXES: set[str] = {
    "F.", "fn.", "Window.",
}

# 允许的 DataFrame 方法链（transformations 只构建逻辑计划）
_ALLOWED_DF_METHODS: set[str] = {
    "select", "filter", "where", "join", "groupBy", "agg",
    "orderBy", "sort", "limit", "withColumn", "withColumnRenamed",
    "drop", "distinct", "dropDuplicates", "dropna", "fill", "fillna",
    "alias", "crossJoin", "union", "unionAll", "unionByName",
    "sample", "randomSplit", "repartition", "coalesce",
    "selectExpr", "transform", "hint",
}


# ════════════════════════════════════════════
# SparkStaticValidator
# ════════════════════════════════════════════


class SparkStaticValidator:
    """PySpark DSL 静态校验器——AST 白名单 + 语义约束。

    基于 AST call-chain 分类（不是简单的字符串匹配）。
    预留 ExecutionSafetyProbe 接口（Phase 7 接入）。
    """

    # 错误码类别
    CATEGORY: ClassVar[dict[str, str]] = {
        "E601": "FORBIDDEN_API",
        "E602": "UNSAFE_IMPORT",
        "E603": "ACTION_NOT_ALLOWED",
        "E604": "SINK_NOT_ALLOWED",
        "E605": "UDF_NOT_ALLOWED",
        "E606": "RAW_EXPRESSION",
        "E607": "UNKNOWN_FUNCTION",
        "E608": "DYNAMIC_EXEC",
    }

    # ── 绑定分析：追踪 PySpark functions 导入别名 + 赋值污染检测 ──

    def _find_functions_aliases(self, tree: ast.AST) -> tuple[set[str], set[str]]:
        """第一遍 AST 扫描：找到 PySpark functions 的导入别名和被污染的变量名。

        三通道污染检测——任何对 functions 别名的写操作都会污染它：
        1. 名称重绑定：F = df（ast.Assign → ast.Name 目标）
        2. 属性覆盖：F.count = df.count（ast.Assign → ast.Attribute 目标）
        3. 增量赋值：F.count += 1（ast.AugAssign → ast.Attribute 目标）
        4. 属性删除：del F.count（ast.Delete → ast.Attribute 目标）

        Compiler 生成的代码从不修改 functions 别名或其属性，
        因此正常路径不受影响。

        Args:
            tree: 已解析的 AST 模块

        Returns:
            (clean_aliases, tainted): clean_aliases 是确认未污染的 functions 别名，
            tainted 是被污染的别名（不用于当前判断，保留供调试/日志）。
        """
        aliases: set[str] = set()      # from pyspark.sql import functions as X
        tainted: set[str] = set()      # 被任何写操作污染的别名

        for node in ast.walk(tree):
            # 检测 from pyspark.sql import functions as X
            if isinstance(node, ast.ImportFrom):
                if node.module == "pyspark.sql":
                    for alias in node.names:
                        if alias.name == "functions":
                            asname = alias.asname or alias.name
                            aliases.add(asname)

            # 检测 import pyspark.sql.functions as X（备选形式）
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pyspark.sql.functions":
                        asname = alias.asname or alias.name.split(".")[-1]
                        aliases.add(asname)

            # 通道 1：名称重绑定——F = df
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in aliases:
                        tainted.add(target.id)
                    # 通道 2：属性覆盖——F.count = df.count
                    elif isinstance(target, ast.Attribute):
                        root = self._get_attr_root(target)
                        if root is not None and root in aliases:
                            tainted.add(root)

            # 通道 3：增量赋值——F.count += 1
            if isinstance(node, ast.AugAssign):
                if isinstance(node.target, ast.Attribute):
                    root = self._get_attr_root(node.target)
                    if root is not None and root in aliases:
                        tainted.add(root)

            # 通道 4：属性删除——del F.count
            if isinstance(node, ast.Delete):
                for target in node.targets:
                    if isinstance(target, ast.Attribute):
                        root = self._get_attr_root(target)
                        if root is not None and root in aliases:
                            tainted.add(root)

        clean = aliases - tainted
        return clean, tainted

    @staticmethod
    def _get_attr_root(node: ast.expr) -> str | None:
        """从属性链提取根名称——F.count → 'F', df.write.parquet → 'df'。

        沿 ast.Attribute.value 链向下走到 ast.Name 节点，返回其 id。
        若链的末端不是 ast.Name（如 f().x），返回 None。

        Args:
            node: AST 表达式节点（应为 ast.Attribute）

        Returns:
            根名称字符串，无法提取时返回 None
        """
        current = node
        while isinstance(current, ast.Attribute):
            current = current.value
        if isinstance(current, ast.Name):
            return current.id
        return None

    def validate(self, code: str) -> SparkValidationResult:
        """对 PySpark DSL 代码执行静态安全校验。

        Args:
            code: PySpark DSL 源代码字符串

        Returns:
            SparkValidationResult——is_valid=False 时阻断执行
        """
        errors: list[SparkValidationError] = []

        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            errors.append(SparkValidationError(
                error_code="E608",
                line_number=e.lineno or 0,
                category="SYNTAX_ERROR",
                detail=f"代码语法错误：{e.msg}",
                suggestion="检查编译产物是否正确",
            ))
            return SparkValidationResult(is_valid=False, errors=errors)

        # 第一遍：建立 functions 别名绑定知识
        clean_aliases, _ = self._find_functions_aliases(tree)

        # 逐条检查
        for node in ast.walk(tree):
            # E602：禁止的导入
            errors.extend(self._check_import(node))

            # E608：动态执行
            errors.extend(self._check_dynamic_exec(node))

            # E601/E603/E604/E605/E606/E607：调用链检查
            errors.extend(self._check_call(node, clean_aliases))

            # E605：UDF 装饰器
            errors.extend(self._check_udf_decorator(node))

            # E606：F.expr() 调用
            errors.extend(self._check_raw_expression(node))

        return SparkValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
        )

    # ── E602：禁止的导入 ──

    def _check_import(self, node: ast.AST) -> list[SparkValidationError]:
        """检查禁止的模块导入。"""
        errors: list[SparkValidationError] = []

        if isinstance(node, ast.Import):
            for alias in node.names:
                for forbidden, code in _FORBIDDEN_IMPORTS.items():
                    if alias.name == forbidden or alias.name.startswith(forbidden + "."):
                        errors.append(SparkValidationError(
                            error_code=code,
                            line_number=node.lineno,
                            category="UNSAFE_IMPORT",
                            detail=f"禁止导入模块：{alias.name}",
                            suggestion="移除不安全的导入",
                        ))

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                for forbidden, code in _FORBIDDEN_IMPORTS.items():
                    if node.module == forbidden or node.module.startswith(forbidden + "."):
                        errors.append(SparkValidationError(
                            error_code=code,
                            line_number=node.lineno,
                            category="UNSAFE_IMPORT",
                            detail=f"禁止导入模块：{node.module}",
                            suggestion="移除不安全的导入",
                        ))

        return errors

    # ── E608：动态执行 ──

    def _check_dynamic_exec(self, node: ast.AST) -> list[SparkValidationError]:
        """检查 eval/exec 等动态执行调用。"""
        errors: list[SparkValidationError] = []

        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            if func_name in _FORBIDDEN_FUNCTIONS:
                errors.append(SparkValidationError(
                    error_code="E608",
                    line_number=node.lineno,
                    category="DYNAMIC_EXEC",
                    detail=f"禁止调用 {func_name}()——动态代码执行不安全",
                    suggestion="移除 eval/exec 调用，使用确定性代码生成",
                ))

        return errors

    # ── 调用链检查 ──

    def _check_call(self, node: ast.AST, clean_aliases: set[str]) -> list[SparkValidationError]:
        """检查函数调用和方法调用是否符合白名单。"""
        errors: list[SparkValidationError] = []

        if not isinstance(node, ast.Call):
            return errors

        # E601：spark.read / spark.table 等
        err = self._check_spark_api_call(node)
        if err:
            errors.append(err)

        # E603：DataFrame Action 方法（df.count(), df.collect() 等）
        err = self._check_action_call(node, clean_aliases)
        if err:
            errors.append(err)

        # E604：DataFrame Sink 方法（df.write.parquet() 等）
        err = self._check_sink_call(node)
        if err:
            errors.append(err)

        return errors

    def _check_spark_api_call(self, node: ast.Call) -> SparkValidationError | None:
        """检查 spark.xxx() 禁止方法调用——使用前缀匹配。"""
        func_name = self._get_func_name(node.func)
        for forbidden in _FORBIDDEN_SPARK_METHODS:
            # 前缀匹配：spark.read.parquet 匹配 spark.read
            if func_name == forbidden or func_name.startswith(forbidden + "."):
                return SparkValidationError(
                    error_code="E601",
                    line_number=node.lineno,
                    category="FORBIDDEN_API",
                    detail=f"禁止调用 {func_name}()——使用 inputs dict 获取数据",
                    suggestion='使用 inputs["source_name"] 替代 spark.read/table',
                )
        return None

    def _check_action_call(self, node: ast.Call, clean_aliases: set[str]) -> SparkValidationError | None:
        """检查 DataFrame.action() 调用。

        白名单策略：仅当接收者名称经绑定分析确认为未污染的 PySpark functions
        导入别名时才放行。仅凭名字匹配（如 id=="F"）是不安全的——用户可通过
        `F = df` 重赋值绕过。clean_aliases 由 _find_functions_aliases() 在
        第一遍扫描中建立，已排除被赋值污染的变量。
        """
        if not isinstance(node.func, ast.Attribute):
            return None

        # 白名单：确认未污染的 PySpark functions 别名——安全放行
        if isinstance(node.func.value, ast.Name) and node.func.value.id in clean_aliases:
            return None

        method_name = node.func.attr
        if method_name in _FORBIDDEN_ACTIONS:
            return SparkValidationError(
                error_code="E603",
                line_number=node.lineno,
                category="ACTION_NOT_ALLOWED",
                detail=f"禁止调用 DataFrame Action：.{method_name}()",
                suggestion="DataFrame action 在受控执行器中由框架调用，不在用户代码中",
            )
        return None

    def _check_sink_call(self, node: ast.Call) -> SparkValidationError | None:
        """检查 df.write / df.save 等 sink 调用——检查完整属性链。"""
        if not isinstance(node.func, ast.Attribute):
            return None

        # 提取完整属性链（如 df.write.parquet → ["df", "write", "parquet"]）
        chain = self._get_attr_chain(node.func)
        # 检查链中是否包含禁止的 sink 方法
        for method in chain:
            if method in _FORBIDDEN_SINKS:
                return SparkValidationError(
                    error_code="E604",
                    line_number=node.lineno,
                    category="SINK_NOT_ALLOWED",
                    detail=f"禁止调用 Sink 方法：.{method}（完整调用链：{'.'.join(chain)}）",
                    suggestion="数据写入由框架控制，不在用户生成的 DSL 代码中",
                )
        return None

    # ── E605：UDF 装饰器 ──

    def _check_udf_decorator(self, node: ast.AST) -> list[SparkValidationError]:
        """检查 @udf 装饰器。"""
        errors: list[SparkValidationError] = []

        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                dec_name = self._get_func_name(decorator)
                if dec_name in ("udf", "pandas_udf"):
                    errors.append(SparkValidationError(
                        error_code="E605",
                        line_number=node.lineno,
                        category="UDF_NOT_ALLOWED",
                        detail=f"禁止使用 @{dec_name} 装饰器——UDF 不受白名单控制",
                        suggestion="使用内置 PySpark 函数（F.xxx）替代 UDF",
                    ))

        return errors

    # ── E606：F.expr() 原始表达式 ──

    def _check_raw_expression(self, node: ast.AST) -> list[SparkValidationError]:
        """检查 F.expr(...) 原始表达式调用。"""
        errors: list[SparkValidationError] = []

        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            if func_name == "F.expr":
                errors.append(SparkValidationError(
                    error_code="E606",
                    line_number=node.lineno,
                    category="RAW_EXPRESSION",
                    detail="禁止使用 F.expr()——原始表达式绕过类型安全检查",
                    suggestion="使用类型化的 F.col() / F.when() 等替代 F.expr()",
                ))

        return errors

    # ── 辅助方法 ──

    @staticmethod
    def _get_attr_chain(node: ast.expr) -> list[str]:
        """提取 AST 属性链——如 df.write.parquet → ['df', 'write', 'parquet']。

        Args:
            node: AST 表达式节点

        Returns:
            属性名列表（从最外层到最内层）
        """
        chain: list[str] = []
        current = node
        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            chain.append(current.id)
        # 反转——从根到叶
        chain.reverse()
        return chain

    @staticmethod
    def _get_func_name(node: ast.expr) -> str:
        """从 AST 节点提取完整函数名。

        例：ast.Attribute(ast.Name("F"), "count") → "F.count"
            ast.Name("eval") → "eval"
            ast.Attribute(ast.Attribute(ast.Name("df"), "write"), "parquet") → "df.write.parquet"
        """
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = SparkStaticValidator._get_func_name(node.value)
            return f"{base}.{node.attr}"
        if isinstance(node, ast.Call):
            return SparkStaticValidator._get_func_name(node.func)
        return ""
