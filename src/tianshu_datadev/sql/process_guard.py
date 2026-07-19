"""SQL Worker 的跨平台进程资源限制。"""

from __future__ import annotations

import ctypes
import os
import platform
import signal
import subprocess
from collections.abc import Callable


class ProcessGuardError(RuntimeError):
    """无法建立硬资源边界时抛出，禁止降级为无保护执行。"""


class ProcessGuard:
    """为单个 Worker 设置内存硬上限，并负责终止整个进程树。"""

    def __init__(self, memory_limit_bytes: int) -> None:
        if memory_limit_bytes <= 0:
            raise ValueError("进程内存上限必须大于 0")
        self._memory_limit_bytes = memory_limit_bytes
        self._job_handle: int | None = None

    def prepare(self) -> Callable[[], None] | None:
        """在启动子进程前建立平台资源边界。"""
        system = platform.system()
        if system == "Windows":
            self._job_handle = self._create_windows_job_object()
            if self._job_handle is None:
                raise ProcessGuardError("无法创建 Windows Job Object，拒绝无保护执行")
            return None
        if system in {"Linux", "Darwin"}:
            return self._build_posix_preexec()
        raise ProcessGuardError(f"当前平台不支持 SQL Worker 硬资源限制：{system}")

    def attach(self, proc: subprocess.Popen[str]) -> None:
        """将已启动的 Worker 纳入硬资源边界。"""
        if platform.system() != "Windows":
            return
        if self._job_handle is None or not self._assign_to_windows_job(proc.pid):
            self.terminate(proc)
            raise ProcessGuardError("无法将 SQL Worker 加入 Windows Job Object")

    def terminate(self, proc: subprocess.Popen[str]) -> None:
        """终止 Worker 及其子进程。"""
        if proc.poll() is not None:
            return
        try:
            if platform.system() == "Windows":
                proc.kill()
            else:
                os.killpg(proc.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            try:
                proc.kill()
            except OSError:
                pass

    def close(self) -> None:
        """关闭 Job Object；KILL_ON_JOB_CLOSE 会清理残留进程。"""
        if self._job_handle is None:
            return
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel32.CloseHandle(ctypes.c_void_p(self._job_handle))
        finally:
            self._job_handle = None

    def _build_posix_preexec(self) -> Callable[[], None]:
        memory_limit_bytes = self._memory_limit_bytes

        def _set_limits() -> None:
            import resource

            resource.setrlimit(
                resource.RLIMIT_AS,
                (memory_limit_bytes, memory_limit_bytes),
            )

        return _set_limits

    def _create_windows_job_object(self) -> int | None:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):  # noqa: N801
            _fields_ = [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):  # noqa: N801
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", ctypes.c_uint32),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", ctypes.c_uint32),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", ctypes.c_uint32),
                ("SchedulingClass", ctypes.c_uint32),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):  # noqa: N801
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        create_job = kernel32.CreateJobObjectW
        create_job.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
        create_job.restype = ctypes.c_void_p
        set_info = kernel32.SetInformationJobObject
        set_info.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        set_info.restype = ctypes.c_int

        handle = create_job(None, None)
        if not handle:
            return None

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = (
            0x00000100  # JOB_OBJECT_LIMIT_PROCESS_MEMORY
            | 0x00000200  # JOB_OBJECT_LIMIT_JOB_MEMORY
            | 0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        info.ProcessMemoryLimit = self._memory_limit_bytes
        info.JobMemoryLimit = self._memory_limit_bytes
        if not set_info(handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
            kernel32.CloseHandle(ctypes.c_void_p(handle))
            return None
        return int(handle)

    def _assign_to_windows_job(self, pid: int) -> bool:
        assert self._job_handle is not None
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
        open_process.restype = ctypes.c_void_p
        assign = kernel32.AssignProcessToJobObject
        assign.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        assign.restype = ctypes.c_int
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]

        process_handle = open_process(0x0100 | 0x0001, False, pid)
        if not process_handle:
            return False
        try:
            return bool(
                assign(
                    ctypes.c_void_p(self._job_handle),
                    ctypes.c_void_p(process_handle),
                )
            )
        finally:
            close_handle(ctypes.c_void_p(process_handle))
