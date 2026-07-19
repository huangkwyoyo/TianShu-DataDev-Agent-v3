"""SQL Worker 进程硬内存边界测试。"""

from __future__ import annotations

import os
import subprocess
import sys

from tianshu_datadev.sql.process_guard import ProcessGuard


def test_process_guard_allows_small_worker():
    """正常小进程可以在硬边界内完成。"""
    guard = ProcessGuard(256 * 1024 * 1024)
    preexec_fn = guard.prepare()
    proc = subprocess.Popen(
        [sys.executable, "-c", "print('ok')"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=preexec_fn,
        start_new_session=os.name != "nt",
    )
    try:
        guard.attach(proc)
        stdout, _stderr = proc.communicate(timeout=10)
        assert proc.returncode == 0
        assert stdout.strip() == "ok"
    finally:
        guard.terminate(proc)
        guard.close()


def test_process_guard_stops_memory_exhaustion():
    """超过硬内存上限的 Worker 不能继续运行。"""
    guard = ProcessGuard(256 * 1024 * 1024)
    preexec_fn = guard.prepare()
    code = "payload = bytearray(512 * 1024 * 1024); print(len(payload))"
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=preexec_fn,
        start_new_session=os.name != "nt",
    )
    try:
        guard.attach(proc)
        proc.communicate(timeout=15)
        assert proc.returncode != 0
    finally:
        guard.terminate(proc)
        guard.close()
