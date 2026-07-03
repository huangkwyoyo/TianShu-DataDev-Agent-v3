"""Phase 7B LocalSparkExecutor 安全边界测试。

覆盖：
- 安全扫描拒绝危险模式（spark.read/os.system/eval/exec 等）
- 子进程隔离（exec 调用不发生在主进程）
- 环境检查
- 超时控制
- 临时文件清理
- 输出解析
"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.executor import (
    LocalSparkExecutor,
    SparkExecutionStatus,
    _inject_output_collector,
    _parse_output_rows,
    scan_pyspark_code,
)

# ════════════════════════════════════════════
# 安全扫描测试
# ════════════════════════════════════════════


class TestSecurityScan:
    """安全扫描——拒绝危险模式。"""

    def test_rejects_spark_read(self):
        """拒绝 spark.read——禁止动态读取数据源。"""
        code = 'df = spark.read.parquet("/etc/passwd")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("spark.read" in v for v in violations)

    def test_rejects_spark_write(self):
        """拒绝 spark.write——禁止写入。"""
        code = 'spark.write.parquet("/tmp/output")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("spark.write" in v for v in violations)

    def test_rejects_spark_sql(self):
        """拒绝 spark.sql——禁止执行裸 SQL。"""
        code = 'df = spark.sql("SELECT * FROM secret_table")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("spark.sql" in v for v in violations)

    def test_rejects_spark_table(self):
        """拒绝 spark.table——禁止直接查表。"""
        code = 'df = spark.table("production.orders")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("spark.table" in v for v in violations)

    def test_rejects_os_system(self):
        """拒绝 os.system——禁止 OS 命令执行。"""
        code = 'os.system("rm -rf /")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("os.system" in v for v in violations)

    def test_rejects_subprocess(self):
        """拒绝 subprocess——禁止子进程嵌套。"""
        code = 'import subprocess; subprocess.run(["malicious"])'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("subprocess" in v for v in violations)

    def test_rejects_eval(self):
        """拒绝 eval——禁止动态求值。"""
        code = 'eval("__import__(\'os\').system(\'ls\')")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("eval" in v for v in violations)

    def test_rejects_exec(self):
        """拒绝 exec——禁止动态执行。"""
        code = 'exec("import os; os.system(\'ls\')")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("exec" in v for v in violations)

    def test_rejects_import_dynamic(self):
        """拒绝 __import__——禁止动态导入。"""
        code = 'm = __import__("os"); m.system("ls")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("__import__" in v for v in violations)

    def test_allows_safe_code(self):
        """安全 DSL 代码——通过扫描（不含 spark.read / eval / exec 等模式）。"""
        safe_code = 'result_df = input_df.filter(F.col("amount") > 100)'
        violations = scan_pyspark_code(safe_code)
        assert len(violations) == 0

    def test_rejects_multiple_violations(self):
        """多个违规模式——全部报告。"""
        code = '''
spark.read.parquet("/tmp")
os.system("echo hacked")
eval("1+1")
'''
        violations = scan_pyspark_code(code)
        assert len(violations) >= 3

    def test_rejects_urllib(self):
        """拒绝 urllib——禁止网络访问绕过。"""
        code = 'import urllib.request; urllib.request.urlopen("http://evil")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("urllib" in v for v in violations)

    def test_rejects_httpx(self):
        """拒绝 httpx——禁止 HTTP 客户端。"""
        code = 'import httpx; httpx.get("http://evil")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("HTTP" in v for v in violations)

    def test_rejects_pathlib_write(self):
        """拒绝 pathlib.Path().write_text——禁止文件写入绕过。"""
        code = 'import pathlib; pathlib.Path("/tmp/evil").write_text("pwned")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("pathlib" in v for v in violations)

    def test_rejects_io_open_write(self):
        """拒绝 io.open with write——禁止文件写入绕过。"""
        code = 'import io; io.open("/tmp/evil", "w").write("pwned")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("io.open" in v for v in violations)

    def test_rejects_ftp(self):
        """拒绝 ftplib——禁止 FTP 网络访问。"""
        code = 'from ftplib import FTP; FTP("evil.com")'
        violations = scan_pyspark_code(code)
        assert len(violations) > 0
        assert any("FTP" in v for v in violations)


# ════════════════════════════════════════════
# 输出解析测试
# ════════════════════════════════════════════


class TestOutputParsing:
    """stdout 输出解析——从注入标记中提取 JSON 行。"""

    def test_parse_simple_output(self):
        """解析包含标记和 JSON 行的 stdout。"""
        stdout = (
            "some log line\n"
            "===SPARK_EXECUTOR_OUTPUT_START===\n"
            '{"order_id": "1", "amount": 100}\n'
            '{"order_id": "2", "amount": 200}\n'
            "===SPARK_EXECUTOR_OUTPUT_END===\n"
            "more logs\n"
        )
        rows, cleaned = _parse_output_rows(stdout)

        assert len(rows) == 2
        assert rows[0] == {"order_id": "1", "amount": 100}
        assert rows[1] == {"order_id": "2", "amount": 200}
        # 清理后不含标记行
        assert "SPARK_EXECUTOR_OUTPUT_START" not in cleaned

    def test_parse_empty_output(self):
        """空输出——返回空列表。"""
        stdout = (
            "===SPARK_EXECUTOR_OUTPUT_START===\n"
            "===SPARK_EXECUTOR_OUTPUT_END===\n"
        )
        rows, _ = _parse_output_rows(stdout)
        assert len(rows) == 0

    def test_parse_no_markers(self):
        """无标记——无 JSON 行。"""
        stdout = "some output without markers"
        rows, _ = _parse_output_rows(stdout)
        assert len(rows) == 0

    def test_parse_malformed_json(self):
        """畸形 JSON 行——跳过，不影响后续。"""
        stdout = (
            "===SPARK_EXECUTOR_OUTPUT_START===\n"
            "not valid json\n"
            '{"valid": "row"}\n'
            "===SPARK_EXECUTOR_OUTPUT_END===\n"
        )
        rows, _ = _parse_output_rows(stdout)
        assert len(rows) == 1
        assert rows[0] == {"valid": "row"}


# ════════════════════════════════════════════
# 执行器行为测试
# ════════════════════════════════════════════


class TestExecutorBehavior:
    """LocalSparkExecutor 行为测试——安全拒绝、环境检查。"""

    def test_security_rejection_returns_rejected_status(self):
        """安全扫描拒绝 → SECURITY_REJECTED 状态。"""
        executor = LocalSparkExecutor()
        malicious_code = 'os.system("echo hacked")'
        result = executor.execute(malicious_code)

        assert result.status == SparkExecutionStatus.SECURITY_REJECTED
        assert "os.system" in result.error_message

    def test_check_environment(self):
        """check_environment 不抛异常（环境可能不可用）。"""
        executor = LocalSparkExecutor()
        # 不抛异常——返回 True 或 False
        available = executor.check_environment()
        assert isinstance(available, bool)

    def test_execute_with_nonexistent_python(self):
        """不存在的 Python 解释器 → ENVIRONMENT_ERROR。"""
        executor = LocalSparkExecutor(python_cmd="nonexistent_python_xyz")
        safe_code = 'result_df = input_df.filter(F.col("x") > 0)'
        result = executor.execute(safe_code)

        assert result.status == SparkExecutionStatus.ENVIRONMENT_ERROR

    def test_executor_no_exec_in_main_process(self):
        """验证 executor.py 中无裸 exec() 调用在主进程。"""
        import os

        # 读取 executor 源码
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "src", "tianshu_datadev", "spark", "executor.py",
        )
        src_path = os.path.normpath(src_path)

        with open(src_path, encoding="utf-8") as f:
            source = f.read()

        # exec() 只能出现在安全扫描列表中（作为被拒绝的模式），
        # 或在注释/字符串中，不能作为语句
        lines = source.split("\n")
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            # 跳过注释行和空行
            if stripped.startswith("#") or not stripped:
                continue
            # exec( 作为语句出现（不在字符串中）
            # 安全扫描的拒绝列表中含 exec 模式，这是字符串内的——跳过
            if "exec" in stripped.lower() and "\\beval" not in stripped:
                # 检查是否作为函数调用——真正的 exec() 调用
                if stripped.startswith("exec("):
                    pytest.fail(
                        f"executor.py 第 {i} 行包含裸 exec() 调用：{stripped}"
                    )

    def test_subprocess_isolation_used(self):
        """验证 executor 使用 subprocess（非主进程 exec）。"""
        import inspect

        from tianshu_datadev.spark.executor import LocalSparkExecutor

        source = inspect.getsource(LocalSparkExecutor.execute)
        # execute 方法中应包含 subprocess 相关调用
        assert "subprocess" in source or "Popen" in source

    def test_build_sandbox_env_only_whitelisted_keys(self):
        """白名单环境变量——不包含 HOME/USERPROFILE 等敏感变量。"""
        executor = LocalSparkExecutor()
        env = executor._build_sandbox_env(data_dir="/tmp/test_snap")
        # 白名单变量应在结果中（如果当前环境有的话）
        # 敏感变量绝不应在结果中
        for key in env:
            assert key not in ("HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH",
                               "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                               "GITHUB_TOKEN", "DOCKER_HOST", "KUBECONFIG",
                               "AZURE_STORAGE_KEY", "DB_PASSWORD", "SECRET_KEY")

    def test_build_sandbox_env_includes_data_dir(self):
        """SPARK_DATA_DIR 在白名单中——传入时一定包含。"""
        executor = LocalSparkExecutor()
        env = executor._build_sandbox_env(data_dir="/custom/data/path")
        assert env["SPARK_DATA_DIR"] == "/custom/data/path"

    def test_inject_resource_limits_adds_prologue(self):
        """资源限制注入——Unix 侧注入 import resource + setrlimit 调用。"""
        executor = LocalSparkExecutor()
        code = "result_df = input_df.filter(F.col('x') > 0)"
        injected = executor._inject_resource_limits(code)
        assert "import resource as _exec_resource" in injected
        assert "RLIMIT_CPU" in injected
        assert "RLIMIT_AS" in injected
        assert code in injected  # 原始代码保留

    def test_check_output_size_no_truncation(self):
        """输出未超限——不截断。"""
        stdout, stderr, truncated = LocalSparkExecutor._check_output_size(
            "small output", "small error",
        )
        assert stdout == "small output"
        assert stderr == "small error"
        assert not truncated

    def test_check_output_size_truncation(self):
        """输出超 10 MB——截断并标记。"""
        # 构造超大输出——重复字符
        big_chunk = "x" * (5 * 1024 * 1024)  # 5 MB
        stdout = big_chunk + big_chunk + big_chunk  # 15 MB
        stderr = "error"
        _, _, truncated = LocalSparkExecutor._check_output_size(stdout, stderr)
        assert truncated

    def test_resource_limits_injected_before_collector(self):
        """资源限制在输出收集器之前注入——确保子进程先设限再执行。"""
        code = "result_df = input_df"
        executor = LocalSparkExecutor()
        # 先注入资源限制，再注入输出收集器（与 execute() 方法顺序一致）
        limited = executor._inject_resource_limits(code)
        complete = _inject_output_collector(limited, "result_df")
        rlimit_pos = complete.index("RLIMIT_CPU")
        start_marker_pos = complete.index("===SPARK_EXECUTOR_OUTPUT_START===")
        assert rlimit_pos < start_marker_pos, (
            "资源限制应在输出收集器之前注入"
        )
