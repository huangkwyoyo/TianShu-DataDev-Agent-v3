"""日志轮转——按 run_id 分组清理旧日志，保留最近 N 组。

策略：
- 每个日志文件按前缀 tianshu_run_{run_id}_ 或 tianshu_run_{run_id}. 匹配分组
- 按 run_id 字符串排序，保留最后 keep_groups 组
- 当前 current_run_id 的日志组绝对不删
"""

import re
from pathlib import Path

# 匹配日志文件名的正则：tianshu_run_{run_id} 后跟 _ 或 . 或文件结束
# 捕获 run_id 部分（不含前后缀分隔符）
_LOG_FILE_PATTERN: re.Pattern[str] = re.compile(
    r"^tianshu_run_(.+?)(?:[._].*)?$"
)


def _parse_run_id(filename: str) -> str | None:
    """从日志文件名中提取 run_id。

    Args:
        filename: 文件名（不含路径）。

    Returns:
        run_id 字符串，不匹配时返回 None。
    """
    m = _LOG_FILE_PATTERN.match(filename)
    return m.group(1) if m else None


def cleanup(log_dir: Path, current_run_id: str, keep_groups: int = 50) -> int:
    """清理旧日志组，保留最近 keep_groups 组。

    按 run_id 分组（文件前缀 tianshu_run_{run_id}_ 或 tianshu_run_{run_id}.），
    按 run_id 字符串排序，保留最后 keep_groups 组。
    当前 current_run_id 的日志组绝对不删。

    Args:
        log_dir: 日志目录路径。
        current_run_id: 当前运行 run_id，其日志组绝对不删。
        keep_groups: 保留的组数，默认 50。

    Returns:
        删除的组数。
    """
    if not log_dir.is_dir():
        return 0

    # 收集所有日志文件及其 run_id
    run_id_to_files: dict[str, list[Path]] = {}
    for fpath in log_dir.iterdir():
        if not fpath.is_file():
            continue
        rid = _parse_run_id(fpath.name)
        if rid:
            run_id_to_files.setdefault(rid, []).append(fpath)

    if not run_id_to_files:
        return 0

    # 按 run_id 排序
    sorted_run_ids = sorted(run_id_to_files.keys())

    # 保留最后 keep_groups 组
    if len(sorted_run_ids) <= keep_groups:
        return 0

    # 要删除的 run_id 集合（保护 current_run_id）
    to_delete = set(sorted_run_ids[:-keep_groups])
    to_delete.discard(current_run_id)

    # 执行删除
    deleted_group_count = 0
    for rid in to_delete:
        for fpath in run_id_to_files[rid]:
            fpath.unlink(missing_ok=True)
        deleted_group_count += 1

    return deleted_group_count
