"""Phase 7B LocalSparkExecutor——PySpark DSL 子进程隔离执行器。

安全边界（C 类加固）：
- 所有 PySpark 代码在独立子进程中执行（subprocess），主进程绝不执行裸 exec()
- 执行前对代码做安全扫描——拒绝 spark.read/spark.write/os.system/subprocess/eval/exec
- 使用 tempfile 写入代码文件，执行后清理
- 超时控制——防止无限循环/挂起
- 环境变量白名单——不继承主进程敏感环境变量
- 输出大小上限 10 MB——防止 stdout/stderr 撑爆内存
- CPU/内存限制——Unix resource.setrlimit + Windows Job Object
- 工作目录隔离——子进程 cwd 指向独立临时目录
- 网络隔离——强化静态扫描 + 平台能力门控
"""

from __future__ import annotations

import ctypes
import logging
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum

# ════════════════════════════════════════════
# 执行结果
# ════════════════════════════════════════════


class SparkExecutionStatus(str, Enum):
    """PySpark 执行状态——精确描述执行结果。"""

    SUCCESS = "SUCCESS"              # 执行成功，输出合法
    TIMEOUT = "TIMEOUT"              # 执行超时
    RUNTIME_ERROR = "RUNTIME_ERROR"  # 运行时异常（PySpark 报错）
    SECURITY_REJECTED = "SECURITY_REJECTED"  # 安全扫描拒绝
    ENVIRONMENT_ERROR = "ENVIRONMENT_ERROR"  # PySpark 环境不可用
    OUTPUT_TRUNCATED = "OUTPUT_TRUNCATED"    # 输出超 10 MB 上限被截断
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"  # CPU/内存超限被 OS 杀死


@dataclass
class SparkExecutionResult:
    """单次 PySpark 执行的结果——包含 stdout/stderr/耗时/退出码。"""

    status: SparkExecutionStatus
    stdout: str = ""
    stderr: str = ""
    return_code: int = -1
    execution_time_ms: float = 0.0
    # 解析后的 DataFrame 输出（行为 dict 列表，由调用方解析）
    output_rows: list[dict] = field(default_factory=list)
    error_message: str = ""
    # 资源使用记录（可观测性）
    resource_usage: dict = field(default_factory=lambda: {
        "stdout_bytes": 0,
        "stderr_bytes": 0,
        "output_truncated": False,
        "env_keys_used": [],
    })


# ════════════════════════════════════════════
# 安全扫描器
# ════════════════════════════════════════════


# 禁止出现在 PySpark 代码中的危险模式
_FORBIDDEN_PATTERNS: list[tuple[str, str]] = [
    # (正则模式, 描述)
    # ── 数据源保护 ──
    (r"spark\.read", "spark.read——禁止动态读取数据源"),
    (r"spark\.write", "spark.write——禁止写入（本执行器只读快照目录）"),
    (r"spark\.table", "spark.table——禁止直接查表"),
    (r"spark\.sql\s*\(", "spark.sql——禁止执行裸 SQL"),
    # ── 命令执行 ──
    (r"os\.system\s*\(", "os.system——禁止 OS 命令执行"),
    (r"subprocess", "subprocess——禁止子进程嵌套"),
    (r"__import__\s*\(", "__import__——禁止动态导入"),
    (r"\beval\s*\(", "eval——禁止动态求值"),
    (r"\bexec\s*\(", "exec——禁止动态执行（子进程内也不允许）"),
    # ── 文件写入——覆盖多路径变体 ──
    (r"\bopen\s*\(.*[,].*['\"]w", "open with write mode——禁止写文件"),
    (r"pathlib\.Path\([^)]*\)\.write_(bytes|text)\s*\(", "pathlib write——禁止写文件"),
    (r"\bio\.open\s*\(.*[,].*['\"]w", "io.open with write mode——禁止写文件"),
    (r"builtins\.open\s*\(.*[,].*['\"]w", "builtins.open with write mode——禁止写文件"),
    # ── 网络访问——覆盖绕过变体 ──
    (r"requests?\.", "HTTP 请求——禁止网络访问"),
    (r"socket\.", "socket——禁止网络访问"),
    (r"urllib\.", "urllib——禁止网络访问"),
    (r"urllib3", "urllib3——禁止网络访问"),
    (r"httpx?\b", "HTTP 客户端——禁止网络访问"),
    (r"ftplib", "FTP——禁止网络访问"),
    (r"smtplib", "SMTP——禁止网络访问"),
    (r"telnetlib", "Telnet——禁止网络访问"),
]

# ── 资源限制常量 ──

# 输出总大小上限（stdout + stderr）
_MAX_OUTPUT_BYTES = 10 * 1024 * 1024  # 10 MB

# CPU 时间上限（秒）
_CPU_LIMIT_SECONDS = 60

# 内存上限（字节）
_MEMORY_LIMIT_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB

# ── 环境变量白名单 ──

# 子进程只暴露这些环境变量——禁止 token/secret/凭据泄露
_ALLOWED_ENV_VARS: frozenset[str] = frozenset({
    "SPARK_DATA_DIR",
    "PYTHONPATH",
    "PATH",
    "SYSTEMROOT",          # Windows 必需
    "TEMP",
    "TMP",
    "JAVA_HOME",
    "SPARK_HOME",
    "HADOOP_HOME",         # Windows 必需——winutils.exe 所在目录
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
})

# ── Spark 初始化注入代码——注入到子进程脚本开头，在资源限制之后执行 ──

_SPARK_PROLOGUE_TEMPLATE = '''# ── Executor 注入：Spark 初始化 + 数据加载 ──
import os as _tianshu_os, glob as _tianshu_glob, json as _tianshu_json
from pyspark.sql import SparkSession as _TianShuSpark, functions as F
_tianshu_builder = _TianShuSpark.builder
_tianshu_builder = _tianshu_builder.appName("tianshu_executor")
_tianshu_builder = _tianshu_builder.master("local[1]")
_tianshu_builder = _tianshu_builder.config("spark.ui.enabled", "false")
_tianshu_builder = _tianshu_builder.config("spark.sql.adaptive.enabled", "false")
_tianshu_spark = _tianshu_builder.getOrCreate()
# 构造 inputs 字典——优先读快照侧车索引（key=别名），无索引时回退按文件名 stem
inputs: dict = {}
_data_dir = _tianshu_os.environ.get("SPARK_DATA_DIR", "")
if _data_dir and _tianshu_os.path.isdir(_data_dir):
    _index_path = _tianshu_os.path.join(_data_dir, "_inputs_index.json")
    if _tianshu_os.path.isfile(_index_path):
        # 索引路径：{别名: 物理文件名}——按别名装载，与 PySpark 代码 inputs[别名] 对齐
        with open(_index_path, "r", encoding="utf-8") as _idx_f:
            _index = _tianshu_json.load(_idx_f)
        for _key, _fname in _index.items():
            inputs[_key] = _tianshu_spark.read.parquet(
                _tianshu_os.path.join(_data_dir, _fname)
            )
    else:
        # 回退路径：无索引的旧快照——按文件名 stem 做 key（向后兼容）
        _files = sorted(_tianshu_glob.glob(_tianshu_os.path.join(_data_dir, "*.parquet")))
        for _f in _files:
            _name = _tianshu_os.path.splitext(_tianshu_os.path.basename(_f))[0]
            inputs[_name] = _tianshu_spark.read.parquet(_f)
'''

# ── 资源限制注入代码（Unix 路径——注入到子进程脚本开头） ──

_RESOURCE_LIMIT_PROLOGUE = '''# ── Executor 注入：资源限制 ──
import platform as _exec_platform
if _exec_platform.system() != "Windows":
    try:
        import resource as _exec_resource
        _exec_resource.setrlimit(_exec_resource.RLIMIT_CPU, ({cpu_limit}, {cpu_limit}))
        _exec_resource.setrlimit(_exec_resource.RLIMIT_AS, ({mem_limit}, {mem_limit}))
    except (ImportError, ValueError, OSError):
        pass  # 资源限制设置失败不影响执行——由调用方检测
'''


def scan_pyspark_code(code: str) -> list[str]:
    """安全扫描 PySpark 代码——返回发现的违规模式描述列表。

    空列表表示扫描通过。

    Args:
        code: 待扫描的 PySpark 代码字符串

    Returns:
        违规模式描述列表
    """
    violations: list[str] = []
    for pattern, description in _FORBIDDEN_PATTERNS:
        if re.search(pattern, code):
            violations.append(description)
    return violations


# ════════════════════════════════════════════
# 执行结果解析
# ════════════════════════════════════════════


# stdout 输出分隔标记——executor 在代码末尾注入，标记 DataFrame 输出的起止
_OUTPUT_START_MARKER = "===SPARK_EXECUTOR_OUTPUT_START==="
_OUTPUT_END_MARKER = "===SPARK_EXECUTOR_OUTPUT_END==="

# 注入到 PySpark 代码末尾的输出收集片段
# 将最终 DataFrame 转为 JSON 行格式输出到 stdout
_OUTPUT_COLLECTOR_TEMPLATE = """
# ── Executor 注入：结果收集 ──
import json, sys as _exec_sys

# 调用编译器产出的 transform 函数——传入 executor prologue 构造的 inputs 字典
result_df = transform(inputs)
_rows = result_df.toJSON().collect()
_exec_sys.stdout.write("{start_marker}\\n")
for _row_json in _rows:
    _exec_sys.stdout.write(_row_json + "\\n")
_exec_sys.stdout.write("{end_marker}\\n")
_exec_sys.stdout.flush()
"""


def _inject_output_collector(code: str, output_var: str = "result_df") -> str:
    """在 PySpark 代码末尾注入输出收集器——调用 transform(inputs) 并序列化结果为 JSON 行。

    Args:
        code: 原始 PySpark 代码
        output_var: 保留参数，向后兼容（新模板固定使用 result_df）

    Returns:
        注入后的代码
    """
    collector = _OUTPUT_COLLECTOR_TEMPLATE.format(
        start_marker=_OUTPUT_START_MARKER,
        end_marker=_OUTPUT_END_MARKER,
    )
    return code.rstrip() + "\n" + collector


def _parse_output_rows(stdout: str) -> tuple[list[dict], str]:
    """从 stdout 中解析 DataFrame JSON 行输出。

    返回 (输出行列表, 清洗后的 stdout——移除标记行和 JSON 行)。

    Args:
        stdout: 子进程 stdout 原始输出

    Returns:
        (output_rows, cleaned_stdout)
    """
    import json

    lines = stdout.split("\n")
    rows: list[dict] = []
    cleaned_lines: list[str] = []
    in_output = False

    for line in lines:
        if line.strip() == _OUTPUT_START_MARKER:
            in_output = True
            continue
        if line.strip() == _OUTPUT_END_MARKER:
            in_output = False
            continue
        if in_output:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        else:
            # 非输出区域的文本保留在清洗后的 stdout 中
            cleaned_lines.append(line)

    return rows, "\n".join(cleaned_lines)


# ════════════════════════════════════════════
# LocalSparkExecutor
# ════════════════════════════════════════════


class LocalSparkExecutor:
    """本地 PySpark 子进程隔离执行器。

    安全保证：
    1. 所有 PySpark 代码在独立子进程中执行（subprocess.Popen）
    2. 代码写入 tempfile，由子进程 Python 解释器读取执行
    3. 执行前安全扫描——拒绝危险模式
    4. 超时 kill——防止挂起
    5. 主进程绝不调用 exec(code)

    使用方式：
        executor = LocalSparkExecutor()
        result = executor.execute(pyspark_code, data_dir="/tmp/snap_abc123")
        if result.status == SparkExecutionStatus.SUCCESS:
            for row in result.output_rows:
                print(row)
    """

    # 默认超时（秒）
    DEFAULT_TIMEOUT_SECONDS = 120

    # 查找 PySpark 可执行环境
    _PYTHON_CMD = os.environ.get("PYSPARK_PYTHON", "python")

    def __init__(
        self,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        python_cmd: str | None = None,
    ) -> None:
        """初始化执行器。

        Args:
            timeout_seconds: 子进程执行超时（秒）
            python_cmd: Python 解释器路径，None 时使用环境变量或默认 "python"
        """
        self._timeout = timeout_seconds
        self._python_cmd = python_cmd or self._PYTHON_CMD

    # ── 沙箱构建 ──

    @staticmethod
    def _build_sandbox_env(data_dir: str | None) -> dict[str, str]:
        """构建白名单环境变量——只暴露必要变量，防止凭据泄露。

        Args:
            data_dir: 数据目录路径，注入为 SPARK_DATA_DIR

        Returns:
            白名单过滤后的环境变量 dict
        """
        sandbox: dict[str, str] = {}
        for key in _ALLOWED_ENV_VARS:
            value = os.environ.get(key)
            if value is not None:
                sandbox[key] = value
        if data_dir:
            sandbox["SPARK_DATA_DIR"] = data_dir
        return sandbox

    @staticmethod
    def _create_windows_job_object() -> int | None:
        """创建 Windows Job Object 并设置 CPU/内存限制。

        仅在 Windows 平台调用。非 Windows 平台返回 None。

        Returns:
            Job Object 句柄（Windows），或 None（非 Windows / 创建失败）
        """
        if platform.system() != "Windows":
            return None

        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

            # ── 类型定义（命名遵循 Windows SDK 规范） ──
            class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):  # noqa: N801
                _fields_ = [
                    ("BasicLimitInformation", ctypes.c_uint64 * 8),  # JOBOBJECT_BASIC_LIMIT_INFORMATION
                    ("IoInfo", ctypes.c_void_p * 2),                  # IO_COUNTERS
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            # 简化结构——只设置关键字段
            class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):  # noqa: N801
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_uint64),
                    ("PerJobUserTimeLimit", ctypes.c_uint64),
                    ("LimitFlags", ctypes.c_uint32),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", ctypes.c_uint32),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", ctypes.c_uint32),
                    ("SchedulingClass", ctypes.c_uint32),
                ]

            JobObjectExtendedLimitInformation = 9  # noqa: N806
            JOB_OBJECT_LIMIT_PROCESS_TIME = 0x00000002  # noqa: N806
            JOB_OBJECT_LIMIT_JOB_TIME = 0x00000004  # noqa: N806
            JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100  # noqa: N806
            JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200  # noqa: N806

            # ── 创建 Job Object ──
            job_handle = kernel32.CreateJobObjectW(None, None)
            if not job_handle:
                return None

            # ── 设置限制 ──
            basic_limits = JOBOBJECT_BASIC_LIMIT_INFORMATION()
            basic_limits.PerProcessUserTimeLimit = _CPU_LIMIT_SECONDS * 10_000_000  # 100ns 单位
            basic_limits.PerJobUserTimeLimit = _CPU_LIMIT_SECONDS * 10_000_000
            basic_limits.LimitFlags = (
                JOB_OBJECT_LIMIT_PROCESS_TIME
                | JOB_OBJECT_LIMIT_JOB_TIME
            )

            extended_limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            # 将 basic limits 复制到 extended 结构的前 8 个 uint64
            ctypes.memmove(
                ctypes.addressof(extended_limits),
                ctypes.addressof(basic_limits),
                ctypes.sizeof(JOBOBJECT_BASIC_LIMIT_INFORMATION),
            )
            extended_limits.ProcessMemoryLimit = _MEMORY_LIMIT_BYTES
            extended_limits.JobMemoryLimit = _MEMORY_LIMIT_BYTES
            extended_limits.BasicLimitInformation[2] |= (
                JOB_OBJECT_LIMIT_PROCESS_MEMORY | JOB_OBJECT_LIMIT_JOB_MEMORY
            )

            result = kernel32.SetInformationJobObject(
                ctypes.c_void_p(job_handle),
                JobObjectExtendedLimitInformation,
                ctypes.byref(extended_limits),
                ctypes.sizeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
            )
            if not result:
                kernel32.CloseHandle(ctypes.c_void_p(job_handle))
                return None

            return job_handle

        except OSError:
            return None

    @staticmethod
    def _assign_to_job_object(job_handle: int, pid: int) -> bool:
        """将子进程分配到 Windows Job Object（施加资源限制）。

        Args:
            job_handle: CreateJobObjectW 返回的句柄
            pid: 子进程 PID

        Returns:
            True 表示分配成功
        """
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

            PROCESS_SET_QUOTA = 0x0100  # noqa: N806
            PROCESS_TERMINATE = 0x0001  # noqa: N806
            child_handle = kernel32.OpenProcess(
                PROCESS_SET_QUOTA | PROCESS_TERMINATE, False, pid
            )
            if not child_handle:
                return False

            result = kernel32.AssignProcessToJobObject(
                ctypes.c_void_p(job_handle), ctypes.c_void_p(child_handle)
            )
            kernel32.CloseHandle(ctypes.c_void_p(child_handle))
            return bool(result)
        except OSError:
            return False

    @staticmethod
    def _close_job_object(job_handle: int) -> None:
        """关闭 Windows Job Object 句柄。"""
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle(ctypes.c_void_p(job_handle))
        except OSError:
            pass

    @staticmethod
    def _inject_resource_limits(code: str) -> str:
        """在代码顶部注入资源限制代码（Unix 路径）。

        Windows 通过 Job Object 施加限制，不走注入路径。

        Args:
            code: 原始 PySpark DSL 代码

        Returns:
            注入资源限制代码后的代码
        """
        prologue = _RESOURCE_LIMIT_PROLOGUE.format(
            cpu_limit=_CPU_LIMIT_SECONDS,
            mem_limit=_MEMORY_LIMIT_BYTES,
        )
        return prologue + "\n" + code

    @staticmethod
    def _inject_spark_prologue(code: str) -> str:
        """在代码顶部注入 SparkSession 初始化和快照数据加载代码。

        注入内容：
        - 创建 local[1] 模式的 SparkSession（关闭 UI 和自适应查询）
        - 导出 F（pyspark.sql.functions）供 DSL 代码使用
        - 从 SPARK_DATA_DIR 读取快照数据为 inputs 字典：
          优先读 _inputs_index.json 按别名装载，无索引时回退 glob-by-stem

        Args:
            code: 原始 PySpark DSL 代码（编译器产出的 transform 函数）

        Returns:
            注入 Spark prologue 后的代码
        """
        return _SPARK_PROLOGUE_TEMPLATE + "\n" + code

    @staticmethod
    def _check_output_size(stdout: str, stderr: str) -> tuple[str, str, bool]:
        """检查 stdout/stderr 总大小，超限截断。

        Args:
            stdout: 原始 stdout 字符串
            stderr: 原始 stderr 字符串

        Returns:
            (stdout, stderr, truncated)——truncated=True 表示发生了截断
        """
        total = len(stdout.encode("utf-8")) + len(stderr.encode("utf-8"))
        if total <= _MAX_OUTPUT_BYTES:
            return stdout, stderr, False

        # 按比例截断，优先保留 stdout
        stdout_ratio = 0.9
        max_stdout = int(_MAX_OUTPUT_BYTES * stdout_ratio)
        max_stderr = _MAX_OUTPUT_BYTES - max_stdout

        stdout_bytes = stdout.encode("utf-8")
        if len(stdout_bytes) > max_stdout:
            # 从开头截断（不保留尾部——输出正常在开头）
            stdout = stdout_bytes[:max_stdout].decode("utf-8", errors="replace")
            stdout += "\n[输出截断——超出 10 MB 上限]"

        stderr_bytes = stderr.encode("utf-8")
        if len(stderr_bytes) > max_stderr:
            stderr = stderr_bytes[:max_stderr].decode("utf-8", errors="replace")
            stderr += "\n[输出截断——超出 10 MB 上限]"

        return stdout, stderr, True

    @staticmethod
    def _cleanup_workdir(workdir: str) -> None:
        """安全清理临时工作目录。"""
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except OSError:
            pass

    # ── 公共 API ──

    def execute(
        self,
        pyspark_code: str,
        data_dir: str | None = None,
        output_var: str = "result_df",
    ) -> SparkExecutionResult:
        """在子进程中执行 PySpark DSL 代码。

        Args:
            pyspark_code: 待执行的 PySpark DSL 代码字符串
            data_dir: 数据目录路径（快照 Parquet 文件所在目录），
                      注入为 SPARK_DATA_DIR 环境变量供代码引用
            output_var: 最终 DataFrame 变量名，用于输出收集

        Returns:
            SparkExecutionResult——含执行状态、输出、耗时、资源使用
        """
        # Step 1：安全扫描
        violations = scan_pyspark_code(pyspark_code)
        if violations:
            return SparkExecutionResult(
                status=SparkExecutionStatus.SECURITY_REJECTED,
                error_message="安全扫描拒绝——发现以下违规模式：\n" + "\n".join(
                    f"  - {v}" for v in violations
                ),
            )

        # Step 2：注入 Spark prologue → 资源限制 → 输出收集器
        # 注入顺序（从底到顶）：输出收集器 → Spark prologue → 资源限制
        # 最终脚本结构：资源限制 → Spark 初始化 → 用户代码 → 输出收集器
        instrumented_code = _inject_output_collector(pyspark_code, output_var)
        instrumented_code = self._inject_spark_prologue(instrumented_code)
        instrumented_code = self._inject_resource_limits(instrumented_code)

        # Step 3：创建隔离工作目录 + 写入临时文件
        tmp_path = ""
        workdir = ""
        try:
            workdir = tempfile.mkdtemp(prefix="tianshu_sandbox_")
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".py",
                prefix="tianshu_spark_",
                dir=workdir,
                delete=False,
                encoding="utf-8",
            )
            tmp.write(instrumented_code)
            tmp.flush()
            tmp_path = tmp.name
        except OSError as e:
            self._cleanup_workdir(workdir)
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                error_message=f"无法创建临时文件或工作目录：{e}",
            )

        # Step 4：构建沙箱环境变量（白名单）
        sandbox_env = self._build_sandbox_env(data_dir)
        env_keys_used = sorted(sandbox_env.keys())

        # Step 5：设置 Windows Job Object 资源限制
        job_handle = self._create_windows_job_object()

        # Step 6：子进程执行
        start_time = time.monotonic()
        proc = None  # 类型检查哨兵——Popen 可能因异常未初始化
        try:
            proc = subprocess.Popen(
                [self._python_cmd, tmp_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,                    # 文本模式——自动解码
                cwd=workdir,                  # 隔离工作目录
                env=sandbox_env,              # 白名单环境变量
                # Windows 不传 preexec_fn（POSIX only），无安全问题
            )

            # Windows：将子进程分配到 Job Object
            if job_handle is not None and proc.pid is not None:
                self._assign_to_job_object(job_handle, proc.pid)

            stdout, stderr = proc.communicate(timeout=self._timeout)
            return_code = proc.returncode
            elapsed_ms = (time.monotonic() - start_time) * 1000

        except subprocess.TimeoutExpired:
            if proc is not None:
                proc.kill()
                proc.wait()
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.TIMEOUT,
                execution_time_ms=elapsed_ms,
                error_message=f"执行超时（{self._timeout}s）",
                resource_usage={"env_keys_used": env_keys_used},
            )
        except FileNotFoundError:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                execution_time_ms=elapsed_ms,
                error_message=(
                    f"Python 解释器不可用：'{self._python_cmd}'。"
                    f"请确认 PySpark 环境已安装或设置 PYSPARK_PYTHON 环境变量。"
                ),
                resource_usage={"env_keys_used": env_keys_used},
            )
        except OSError as e:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                execution_time_ms=elapsed_ms,
                error_message=f"子进程启动失败：{e}",
                resource_usage={"env_keys_used": env_keys_used},
            )

        # Step 7：清理临时文件和 Job Object
        self._cleanup_tmp(tmp_path)
        self._cleanup_workdir(workdir)
        if job_handle is not None:
            self._close_job_object(job_handle)

        # Step 8：输出大小检查——截断超限输出
        stdout, stderr, was_truncated = self._check_output_size(stdout, stderr)
        stdout_bytes = len(stdout.encode("utf-8"))
        stderr_bytes = len(stderr.encode("utf-8"))

        # Step 9：解析输出
        output_rows, cleaned_stdout = _parse_output_rows(stdout)

        # Step 10：判断执行结果
        # 资源耗尽检测——Unix signal 或 Windows Job Object kill
        if return_code in (-9, -15, 0xC000013A):  # SIGKILL, SIGTERM, STATUS_STACK_BUFFER_OVERRUN
            return SparkExecutionResult(
                status=SparkExecutionStatus.RESOURCE_EXHAUSTED,
                stdout=cleaned_stdout,
                stderr=stderr,
                return_code=return_code,
                execution_time_ms=elapsed_ms,
                error_message=f"资源耗尽（退出码 {return_code}）——CPU/内存超限被 OS 终止",
                resource_usage={
                    "stdout_bytes": stdout_bytes,
                    "stderr_bytes": stderr_bytes,
                    "output_truncated": was_truncated,
                    "env_keys_used": env_keys_used,
                },
            )
        if was_truncated:
            return SparkExecutionResult(
                status=SparkExecutionStatus.OUTPUT_TRUNCATED,
                stdout=cleaned_stdout,
                stderr=stderr,
                return_code=return_code,
                execution_time_ms=elapsed_ms,
                output_rows=output_rows,
                error_message=f"输出超过 {_MAX_OUTPUT_BYTES // (1024 * 1024)} MB 上限，已截断",
                resource_usage={
                    "stdout_bytes": stdout_bytes,
                    "stderr_bytes": stderr_bytes,
                    "output_truncated": True,
                    "env_keys_used": env_keys_used,
                },
            )
        if return_code != 0:
            # stderr 摘要写入后端日志——上限 5000 字符，超出时附加截断标记
            _stderr_total = len(stderr)
            _stderr_log_limit = 5000
            if _stderr_total > _stderr_log_limit:
                _stderr_summary = stderr[:_stderr_log_limit] + (
                    f"\n... [stderr truncated: {_stderr_log_limit}/{_stderr_total} chars]"
                )
            else:
                _stderr_summary = stderr
            logging.getLogger(__name__).error(
                "PySpark 执行失败（退出码 %d）：\n%s",
                return_code, _stderr_summary,
            )
            # error_message 返回前端——截断 2000 字符，避免 UI 过长
            _msg_limit = 2000
            _error_msg = stderr[:_msg_limit]
            if _stderr_total > _msg_limit:
                _error_msg += f"\n... [stderr truncated: {_msg_limit}/{_stderr_total} chars]"
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                stdout=cleaned_stdout,
                stderr=stderr,
                return_code=return_code,
                execution_time_ms=elapsed_ms,
                error_message=f"PySpark 执行失败（退出码 {return_code}）：{_error_msg}",
                resource_usage={
                    "stdout_bytes": stdout_bytes,
                    "stderr_bytes": stderr_bytes,
                    "output_truncated": False,
                    "env_keys_used": env_keys_used,
                },
            )

        return SparkExecutionResult(
            status=SparkExecutionStatus.SUCCESS,
            stdout=cleaned_stdout,
            stderr=stderr,
            return_code=return_code,
            execution_time_ms=elapsed_ms,
            output_rows=output_rows,
            resource_usage={
                "stdout_bytes": stdout_bytes,
                "stderr_bytes": stderr_bytes,
                "output_truncated": False,
                "env_keys_used": env_keys_used,
            },
        )

    # ── CDP digest 执行（Task 6） ──

    def execute_with_cdp(
        self,
        spec,
        snapshot_id: str,
        data_dir: str | None = None,
    ) -> "DigestExecutionEnvelope":
        """在子进程中计算 CDP v1 full_digest。

        使用 SparkCdpBuilder 生成摘要计算脚本，通过子进程隔离执行，
        与 execute() 使用相同的沙箱模型。

        Args:
            spec: CreDigestSpec —— CDP 摘要规范
            snapshot_id: 快照 ID（用于溯源）
            data_dir: 数据目录（Parquet 快照文件所在目录）

        Returns:
            DigestExecutionEnvelope —— 含 full_digest 和 row_count
        """
        from tianshu_datadev.spark.cdp_spark_builder import SparkCdpBuilder
        from tianshu_datadev.spark.cdp_spec import (
            DigestExecutionEnvelope,
            EngineDigestSummary,
            compute_digest_spec_hash,
        )

        spec_hash_hex = compute_digest_spec_hash(spec).hex()
        builder = SparkCdpBuilder()
        cdp_code = builder.build_digest_script(
            spec, spec_hash_hex, snapshot_id,
        )

        # 安全扫描——扫描 builder 生成的代码（不含 prologue，与 execute() 一致）
        violations = scan_pyspark_code(cdp_code)
        if violations:
            return DigestExecutionEnvelope(
                execution_status="FAILED",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="spark",
                error="安全扫描拒绝：" + "; ".join(violations),
            )

        # 注入 Spark prologue + 资源限制
        full_code = self._inject_spark_prologue(cdp_code)
        full_code = self._inject_resource_limits(full_code)

        # 子进程执行——复用 execute() 的沙箱逻辑
        result = self._run_cdp_subprocess(full_code, data_dir)

        if result.status != SparkExecutionStatus.SUCCESS:
            return DigestExecutionEnvelope(
                execution_status="FAILED",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="spark",
                error=f"子进程执行失败 ({result.status}): {result.error_message}",
            )

        # 解析 stdout 中的 JSON 结果
        try:
            import json

            for line in result.stdout.split("\n"):
                line = line.strip()
                if line.startswith("{"):
                    data = json.loads(line)
                    full_digest = str(data["full_digest"])
                    row_count = int(data["row_count"])
                    return DigestExecutionEnvelope(
                        execution_status="SUCCESS",
                        snapshot_id=snapshot_id,
                        digest_spec_hash=spec_hash_hex,
                        protocol_version="cdp-v1",
                        engine_version="spark",
                        summary=EngineDigestSummary(
                            row_count=row_count,
                            full_digest=full_digest,
                            samples=[],
                        ),
                    )
            # 未找到 JSON 行
            return DigestExecutionEnvelope(
                execution_status="FAILED",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="spark",
                error=f"stdout 中未找到 JSON 结果行:\n{result.stdout[:2000]}",
            )
        except Exception as e:
            return DigestExecutionEnvelope(
                execution_status="FAILED",
                snapshot_id=snapshot_id,
                digest_spec_hash=spec_hash_hex,
                protocol_version="cdp-v1",
                engine_version="spark",
                error=f"结果解析失败: {e}",
            )

    def _run_cdp_subprocess(
        self,
        code: str,
        data_dir: str | None,
    ) -> SparkExecutionResult:
        """执行 CDP 子进程——与 execute() 共享的沙箱逻辑。

        此方法从 execute() 抽取了子进程创建/管理/清理的核心流程，
        但不注入 output collector（CDP 脚本自带输出逻辑）。
        """
        # 创建隔离工作目录 + 写入临时文件
        tmp_path = ""
        workdir = ""
        try:
            workdir = tempfile.mkdtemp(prefix="tianshu_cdp_")
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".py",
                prefix="tianshu_cdp_",
                dir=workdir,
                delete=False,
                encoding="utf-8",
            )
            tmp.write(code)
            tmp.flush()
            tmp_path = tmp.name
        except OSError as e:
            self._cleanup_workdir(workdir)
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                error_message=f"无法创建临时文件或工作目录：{e}",
            )

        # 构建沙箱环境变量（白名单）
        sandbox_env = self._build_sandbox_env(data_dir)
        env_keys_used = sorted(sandbox_env.keys())

        # 设置 Windows Job Object 资源限制
        job_handle = self._create_windows_job_object()

        # 子进程执行
        start_time = time.monotonic()
        proc = None
        try:
            proc = subprocess.Popen(
                [self._python_cmd, tmp_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=workdir,
                env=sandbox_env,
            )

            if job_handle is not None and proc.pid is not None:
                self._assign_to_job_object(job_handle, proc.pid)

            stdout, stderr = proc.communicate(timeout=self._timeout)
            return_code = proc.returncode
            elapsed_ms = (time.monotonic() - start_time) * 1000

        except subprocess.TimeoutExpired:
            if proc is not None:
                proc.kill()
                proc.wait()
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.TIMEOUT,
                execution_time_ms=elapsed_ms,
                error_message=f"CDP 执行超时（{self._timeout}s）",
                resource_usage={"env_keys_used": env_keys_used},
            )
        except FileNotFoundError:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                execution_time_ms=elapsed_ms,
                error_message=(
                    f"Python 解释器不可用：'{self._python_cmd}'。"
                ),
                resource_usage={"env_keys_used": env_keys_used},
            )
        except OSError as e:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._cleanup_tmp(tmp_path)
            self._cleanup_workdir(workdir)
            self._close_job_object(job_handle) if job_handle else None
            return SparkExecutionResult(
                status=SparkExecutionStatus.ENVIRONMENT_ERROR,
                execution_time_ms=elapsed_ms,
                error_message=f"子进程启动失败：{e}",
                resource_usage={"env_keys_used": env_keys_used},
            )

        # 清理
        self._cleanup_tmp(tmp_path)
        self._cleanup_workdir(workdir)
        if job_handle is not None:
            self._close_job_object(job_handle)

        # 输出大小检查
        stdout, stderr, was_truncated = self._check_output_size(stdout, stderr)

        # 判断执行结果
        if return_code != 0:
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                stdout=stdout,
                stderr=stderr,
                return_code=return_code,
                execution_time_ms=elapsed_ms,
                error_message=f"CDP 执行失败（退出码 {return_code}）",
            )
        if was_truncated:
            return SparkExecutionResult(
                status=SparkExecutionStatus.OUTPUT_TRUNCATED,
                stdout=stdout,
                stderr=stderr,
                return_code=return_code,
                execution_time_ms=elapsed_ms,
                error_message="CDP 输出被截断",
            )

        return SparkExecutionResult(
            status=SparkExecutionStatus.SUCCESS,
            stdout=stdout,
            stderr=stderr,
            return_code=return_code,
            execution_time_ms=elapsed_ms,
            resource_usage={"env_keys_used": env_keys_used},
        )

    # ── 内部方法 ──

    @staticmethod
    def _cleanup_tmp(file_path: str) -> None:
        """安全清理临时文件——忽略文件不存在等错误。"""
        try:
            os.unlink(file_path)
        except OSError:
            pass

    def check_environment(self) -> bool:
        """检查 PySpark 执行环境是否可用。

        通过子进程实际创建 SparkSession 来验证——"import pyspark" 仅验证
        Python 包存在，不能发现 Java 版本不兼容（如 Java 8 vs PySpark 需 Java 17+）。

        Returns:
            True 表示环境可用（SparkSession 成功启动并关闭）
        """
        # 用最简 SparkSession 启动验证：local[1] 模式，关闭 UI
        _check_code = (
            "from pyspark.sql import SparkSession; "
            "s = SparkSession.builder.appName('tianshu_check')"
            ".master('local[1]').config('spark.ui.enabled','false')"
            ".getOrCreate(); "
            "s.stop()"
        )
        try:
            proc = subprocess.run(
                [self._python_cmd, "-c", _check_code],
                capture_output=True,
                text=True,
                timeout=60,  # 冷启动 PySpark 可能需 30-60s
            )
            return proc.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            return False
