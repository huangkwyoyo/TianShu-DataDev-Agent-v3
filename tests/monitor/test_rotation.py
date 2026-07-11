"""测试日志轮转 cleanup——分组保留、保护当前 run_id、边界情况。"""

import tempfile
from pathlib import Path

from tianshu_datadev.monitor.rotation import cleanup


def _create_log_files(dir: Path, run_ids: list[str], suffix: str = ".log"):
    """辅助：在 dir 下为每个 run_id 创建模拟日志文件。"""
    for rid in run_ids:
        filepath = dir / f"tianshu_run_{rid}{suffix}"
        filepath.write_text(f"模拟日志内容 for {rid}")


class TestCleanup:
    """cleanup 核心功能测试。"""

    def test_cleanup_keeps_recent_50_groups(self):
        """创建 60 组模拟日志目录，cleanup 后剩 50 组。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            # 创建 60 组日志，run_id 按数字排序
            run_ids = [f"run-{i:04d}" for i in range(60)]
            _create_log_files(log_dir, run_ids)

            # 清理，保留 50 组
            deleted = cleanup(log_dir, current_run_id="dummy-last")

            # 应该删除了 10 组（最早的 10 组）
            assert deleted == 10
            remaining = sorted(log_dir.iterdir())
            # 检查剩余文件数量
            assert len(remaining) == 50
            # 最旧的 run_id 应该是 run-0010（第 11 个，索引从 0 开始是第 10 个）
            first_remaining = remaining[0].stem.replace("tianshu_run_", "")
            assert first_remaining == "run-0010"

    def test_cleanup_protects_current_run_id(self):
        """当前 run_id 的日志组不被删除。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            # 创建 60 组，其中 run-0005 设为 current_run_id
            run_ids = [f"run-{i:04d}" for i in range(60)]
            _create_log_files(log_dir, run_ids)

            deleted = cleanup(log_dir, current_run_id="run-0005", keep_groups=50)

            # 虽然 run-0005 在最早的 10 组中，但它不应被删除
            assert deleted < 60
            assert (log_dir / "tianshu_run_run-0005.log").exists()

    def test_cleanup_handles_empty_dir(self):
        """空目录不报错。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            # 空目录，cleanup 应正常返回 0
            deleted = cleanup(log_dir, current_run_id="run-001")
            assert deleted == 0

    def test_cleanup_handles_missing_dir(self):
        """目录不存在不报错。"""
        with tempfile.TemporaryDirectory() as tmp:
            missing_dir = Path(tmp) / "nonexistent"
            # 目录不存在，cleanup 应正常返回 0
            deleted = cleanup(missing_dir, current_run_id="run-001")
            assert deleted == 0

    def test_cleanup_stable_with_collision_names(self):
        """碰撞文件名（后缀 random hex）不影响清理逻辑。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            # 创建 60 组标准日志
            run_ids = [f"run-{i:04d}" for i in range(60)]
            _create_log_files(log_dir, run_ids)
            # 额外创建一些带有 random hex 后缀的文件（碰撞）
            extra_ids = [f"run-{i:04d}" for i in range(5, 10)]
            for rid in extra_ids:
                filepath = log_dir / f"tianshu_run_{rid}.a1b2c3d4.log"
                filepath.write_text(f"碰撞日志 for {rid}")

            deleted = cleanup(log_dir, current_run_id="dummy", keep_groups=50)

            # 清理逻辑应该稳定处理，不报错
            assert deleted >= 10  # 至少删除了 10 组
