"""临时目录管理器——统一收口到 D 盘，自动清理超限旧文件。

启动时调用 ensure_temp_dir() 完成：
1. 创建 D:/ProgramData/Temp 目录
2. 设置 TMPDIR 环境变量 + 覆盖 tempfile.tempdir 缓存
3. 若目录总大小超过上限（默认 10GB），按修改时间从旧到新删除，直至低于上限
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile as _tf
from pathlib import Path

logger = logging.getLogger(__name__)

# tianshu 运行时产生的临时文件前缀——仅清理匹配这些前缀的目录/文件
_TIANSHU_TEMP_PREFIXES = (
    "tianshu_",
    "snap_",
    ".tmp_",
)

# 默认上限：10GB
_DEFAULT_MAX_SIZE_GB = 10


def _get_dir_size(path: Path) -> int:
    """递归计算目录或文件的总大小（字节）。"""
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    try:
        for entry in path.rglob("*"):
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except OSError:
                continue
    except OSError:
        return 0
    return total


def _cleanup_if_needed(
    temp_dir: Path,
    max_size_gb: int = _DEFAULT_MAX_SIZE_GB,
) -> int:
    """若 temp_dir 下 tianshu 相关文件总大小超过上限，按 mtime 从旧到新删除。

    Args:
        temp_dir: 临时目录路径
        max_size_gb: 大小上限（GB）

    Returns:
        删除的条目数
    """
    if not temp_dir.exists():
        return 0

    max_bytes = max_size_gb * 1024 * 1024 * 1024

    # 收集所有匹配前缀的条目及其大小和 mtime
    entries: list[tuple[Path, int, float]] = []  # (path, size, mtime)
    total_size = 0

    try:
        for entry in temp_dir.iterdir():
            name = entry.name
            if not any(name.startswith(p) for p in _TIANSHU_TEMP_PREFIXES):
                continue
            size = _get_dir_size(entry)
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                mtime = 0.0
            entries.append((entry, size, mtime))
            total_size += size
    except OSError:
        return 0

    if total_size <= max_bytes:
        return 0

    # 按 mtime 从旧到新排序——优先删除最旧的
    entries.sort(key=lambda x: x[2])

    deleted = 0
    for entry, size, _mtime in entries:
        if total_size <= max_bytes:
            break
        try:
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
            total_size -= size
            deleted += 1
            logger.info("临时目录清理: %s (%.1f MB)", entry.name, size / 1024 / 1024)
        except OSError:
            continue

    if deleted > 0:
        logger.info(
            "临时目录清理完成: 删除 %d 个条目，释放 %.1f GB",
            deleted,
            (max_bytes - max(total_size, 0)) / 1024 / 1024 / 1024,
        )

    return deleted


def ensure_temp_dir(
    temp_path: str = "D:/ProgramData/Temp",
    max_size_gb: int = _DEFAULT_MAX_SIZE_GB,
) -> None:
    """确保临时目录存在，设置环境变量，自动清理超限旧文件。

    调用方（app.py、cli/main.py、conftest.py）应在入口处调用此函数，
    确保所有后续 tempfile 调用使用正确的临时目录。

    Args:
        temp_path: 临时目录路径
        max_size_gb: 大小上限（GB），超过后按 mtime 从旧到新自动清理
    """
    temp_dir = Path(temp_path)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # 注入环境变量——Python tempfile 模块在首次导入时读取 TMPDIR
    os.environ["TMPDIR"] = str(temp_dir)
    _tf.tempdir = str(temp_dir)

    # 自动清理超限旧文件
    _cleanup_if_needed(temp_dir, max_size_gb)
