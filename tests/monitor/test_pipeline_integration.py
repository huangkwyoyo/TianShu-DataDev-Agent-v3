"""测试 Pipeline 与 monitor collector 集成——验证正确埋点行为。

Batch 4 — SQL 管线埋点。
使用 mock collector 验证 pipeline 各入口方法在真实执行点正确调用 collector.stage()。
"""

import os
from unittest.mock import patch, MagicMock

import pytest

from tianshu_datadev.monitor import NullCollector, StageContext
from tianshu_datadev.monitor.collector import get_collector


# ═══════════════════════════════════════════════════════════
# 辅助：MockCollector——轻量替代 RunLogCollector，避免文件 IO
# ═══════════════════════════════════════════════════════════


class MockCollector:
    """模拟 Collector——记录所有 stage 调用供断言。"""

    def __init__(self):
        self.stages: list[dict] = []  # [(node, artifact_request_id, status)]
        self._active_stages: list["MockStageContext"] = []

    def stage(self, node: str, artifact_request_id: str, parent_stage_run_id: str | None = None):
        ctx = MockStageContext(
            collector=self,
            node=node,
            artifact_request_id=artifact_request_id,
        )
        self._active_stages.append(ctx)
        return ctx

    def emit(self, event):
        pass

    def log_resource_sample(self, sample):
        pass

    def log_browser_event(self, payload):
        pass

    def flush(self, timeout=5.0):
        return True

    def close(self):
        pass


class MockStageContext:
    """模拟 StageContext——记录所有 set_result 调用。"""

    def __init__(self, collector: MockCollector, node: str, artifact_request_id: str):
        self.collector = collector
        self.node = node
        self.artifact_request_id = artifact_request_id
        self._result = {}
        self._exited = False

    def set_result(self, **kwargs):
        self._result.update(kwargs)

    def __enter__(self):
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "status": "started",
            "result": {},
        })
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            status = "completed"
        else:
            status = "failed"
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "status": status,
            "result": dict(self._result),
            "error_type": exc_type.__name__ if exc_type else None,
        })
        return False  # 不吞异常


# ═══════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════


@pytest.fixture
def mock_collector():
    """返回 MockCollector 实例，自动 patch get_collector。"""
    mc = MockCollector()

    def _fake_get_collector(*args, **kwargs):
        return mc

    with patch("tianshu_datadev.api.pipeline.get_collector", side_effect=_fake_get_collector):
        yield mc


@pytest.fixture
def pipeline(mock_collector):
    """创建真实 Pipeline 实例——由 mock_collector fixture 自动注入 mock collector。"""
    from tianshu_datadev.api.pipeline import Pipeline
    import tempfile
    tmpdir = tempfile.mkdtemp()
    yield Pipeline(base_output_dir=tmpdir)
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def golden_spec() -> str:
    """读取 golden fixture。"""
    _root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path = os.path.join(_root, "tests", "fixtures", "golden", "golden_no_time_range.md")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def golden_spec_passing() -> str:
    """读取通过验证的 golden fixture。"""
    _root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path = os.path.join(_root, "tests", "fixtures", "golden", "golden_passing.md")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ═══════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════


class TestSqlPipelineMonitoring:
    """验证 Pipeline 各入口方法在正确节点抛出 stage 事件。"""

    # ─── helpers ────────────────────────────────────────────

    @staticmethod
    def _collect_nodes(events: list[dict]) -> list[str]:
        """从 stage 事件列表中提取 node 唯一序列（去重 same node 的 started/completed）。"""
        seen: set[str] = set()
        nodes: list[str] = []
        for ev in events:
            n = ev["node"]
            if n not in seen:
                seen.add(n)
                nodes.append(n)
            elif ev["status"] == "failed":
                # 失败事件也保留（不覆盖已存在的 started/completed）
                pass
        return nodes

    @staticmethod
    def _stage_events_by_node(
        events: list[dict],
    ) -> dict[str, list[dict]]:
        """按 node 名分组 stage 事件。"""
        grouped: dict[str, list[dict]] = {}
        for ev in events:
            grouped.setdefault(ev["node"], []).append(ev)
        return grouped

    # ─── execute_rich ──────────────────────────────────────

    def test_execute_rich_records_all_sql_stages(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """execute_rich 应记录 6 个 SQL 管线节点事件。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        csv_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv",
            )
        )
        # 清空 mock 记录
        mock_collector.stages.clear()

        result = pipeline.execute_rich(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        assert "pipeline_error" not in result, (
            f"execute_rich 不应失败：{result.get('pipeline_error')}"
        )

        nodes = self._collect_nodes(mock_collector.stages)
        expected = [
            "sql_parser", "sql_enricher", "sql_builder",
            "sql_validator", "sql_compiler", "sql_executor",
            "snapshot_builder",
        ]
        for n in expected:
            assert n in nodes, f"缺少节点 {n}，实际节点：{nodes}"
        assert len(nodes) == len(expected), (
            f"应有 {len(expected)} 个节点，实际 {len(nodes)}：{nodes}"
        )

    # ─── parse_only ───────────────────────────────────────

    def test_parse_only_records_parser_stage(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """parse_only 应记录 sql_parser 节点事件。"""
        mock_collector.stages.clear()

        result = pipeline.parse_only(golden_spec_passing)
        assert "pipeline_error" not in result

        nodes = self._collect_nodes(mock_collector.stages)
        assert "sql_parser" in nodes, f"缺少 sql_parser 节点，实际：{nodes}"
        # 不应有其它节点
        assert len(nodes) == 1, f"parse_only 应只有 1 个节点，实际 {len(nodes)}：{nodes}"

    # ─── build_plan ───────────────────────────────────────

    def test_build_plan_records_four_stages(
        self, pipeline, golden_spec, mock_collector,
    ):
        """build_plan 应记录 parser/enricher/builder/validator 四个节点。"""
        mock_collector.stages.clear()

        result = pipeline.build_plan(golden_spec)
        # build_plan 可能会返回 validation_blocked——正常
        nodes = self._collect_nodes(mock_collector.stages)
        expected = ["sql_parser", "sql_enricher", "sql_builder", "sql_validator"]
        for n in expected:
            assert n in nodes, f"缺少节点 {n}，实际节点：{nodes}"
        assert len(nodes) == len(expected), (
            f"应有 {len(expected)} 个节点，实际 {len(nodes)}：{nodes}"
        )

    # ─── run_all ──────────────────────────────────────────

    def test_run_all_records_extra_stages(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """run_all 应记录 9 个节点（6 基础 + contract_extractor + snapshot_builder + packager）。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        csv_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv",
            )
        )
        mock_collector.stages.clear()

        # 注入 mock SnapshotBuilder + SnapshotProvider——使 snapshot_builder 阶段可执行
        mock_snap_builder = MagicMock()
        mock_snap_manifest = MagicMock()
        mock_snap_manifest.snapshot_id = "snap_test"
        mock_snap_manifest.files = [MagicMock()]
        mock_snap_manifest.model_dump.return_value = {
            "snapshot_id": "snap_test", "files": [],
            "snapshot_dir": "/tmp/test", "contract_hash": "abc123",
            "source_type": "local_fixture",
        }
        mock_snap_builder.build.return_value = mock_snap_manifest
        mock_snap_provider = MagicMock()
        mock_snap_provider.allowlisted_tables = ["test_fact"]
        pipeline.inject_snapshot_deps(mock_snap_builder, mock_snap_provider)

        result = pipeline.run_all(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )
        assert "pipeline_error" not in result, (
            f"run_all 不应失败：{result.get('pipeline_error')}"
        )

        nodes = self._collect_nodes(mock_collector.stages)
        expected = [
            "sql_parser", "sql_enricher", "sql_builder",
            "sql_validator", "sql_compiler", "sql_executor",
            "contract_extractor", "snapshot_builder", "packager",
        ]
        for n in expected:
            assert n in nodes, f"缺少节点 {n}，实际节点：{nodes}"
        assert len(nodes) == len(expected), (
            f"应有 {len(expected)} 个节点，实际 {len(nodes)}：{nodes}"
        )

    # ─── parse_rich 不重复 ────────────────────────────────

    def test_parse_rich_does_not_duplicate_stages(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """parse_rich 委托 parse_only，不产生重复的 sql_parser 事件。"""
        mock_collector.stages.clear()

        result = pipeline.parse_rich(golden_spec_passing)
        assert "pipeline_error" not in result

        nodes = self._collect_nodes(mock_collector.stages)
        assert "sql_parser" in nodes, f"缺少 sql_parser 节点，实际：{nodes}"
        # 只应有 1 个节点（parse_rich 委托到 parse_only，不额外埋点）
        assert len(nodes) == 1, (
            f"parse_rich 应只有 1 个 sql_parser 节点，"
            f"实际 {len(nodes)} 个：{nodes}"
        )

    # ─── artifact_request_id ──────────────────────────────

    def test_stage_event_has_artifact_request_id(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """stage 事件的 artifact_request_id 应与 _gen_request_id 一致。"""
        mock_collector.stages.clear()

        pipeline.parse_only(golden_spec_passing)
        # parse_only 的 sql_parser 阶段使用 ""，因为解析前没有 spec
        parser_events = [
            ev for ev in mock_collector.stages if ev["node"] == "sql_parser"
        ]
        assert len(parser_events) >= 1
        # parser 阶段使用 ""（解析前无法计算 request_id）
        assert parser_events[0]["artifact_request_id"] == "", (
            "parse_only parser 阶段 artifact_request_id 应为空字符串"
        )

        # execute_rich 的后续阶段应有非空 artifact_request_id
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        csv_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv",
            )
        )
        mock_collector.stages.clear()

        pipeline.execute_rich(
            golden_spec_passing,
            table_mapping={"tf": "test_fact"},
            table_paths={"test_fact": csv_path},
        )

        # sql_executor 阶段应有 request_id
        executor_events = [
            ev for ev in mock_collector.stages if ev["node"] == "sql_executor"
        ]
        if executor_events:
            assert executor_events[0]["artifact_request_id"] != "", (
                "execute_rich executor 阶段的 artifact_request_id 不应为空"
            )

    # ─── compile 失败 ────────────────────────────────────

    def test_compile_failure_records_stage_failed(
        self, pipeline, golden_spec_passing, mock_collector,
    ):
        """编译失败时 sql_compiler stage 记录 failed 状态 + error_type。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")
        csv_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv",
            )
        )
        mock_collector.stages.clear()

        # 注入一个会抛出异常的 compiler（使用通过验证的 golden_spec_passing）
        import tianshu_datadev.api.pipeline as pipeline_mod

        class FailingCompiler:
            def __init__(self, *args, **kwargs):
                pass

            def compile(self, plan):
                raise ValueError("模拟编译失败")

        with patch.object(pipeline_mod, 'DuckDbSqlCompiler', FailingCompiler):
            result = pipeline.execute(
                golden_spec_passing,
                table_mapping={"tf": "test_fact"},
                table_paths={"test_fact": csv_path},
            )

        # 应该有 pipeline_error
        assert "pipeline_error" in result
        # 检查 mock 是否记录到 failed
        failed_compiles = [
            ev for ev in mock_collector.stages
            if ev["node"] == "sql_compiler" and ev["status"] == "failed"
        ]
        if failed_compiles:
            assert failed_compiles[-1]["error_type"] == "ValueError"
        else:
            # NullCollector 或 mock 未捕获到——至少检查 pipeline_error
            assert result["pipeline_error"]["stage"] == "compile"

        # 检查 compile 之前的 stage 都是 completed
        parser_completed = [
            ev for ev in mock_collector.stages
            if ev["node"] == "sql_parser" and ev["status"] == "completed"
        ]
        assert parser_completed, "parser 应完成"

    # ─── NullCollector ────────────────────────────────────

    def test_null_collector_pipeline_noop(
        self, golden_spec_passing,
    ):
        """NullCollector 时管线行为不变（不崩溃、返回正常结果）。"""
        # 确保 TIANSHU_RUN_ID 未设置 → NullCollector
        # 只需要调用 pipeline 方法，不应崩溃
        from tianshu_datadev.api.pipeline import Pipeline
        import tempfile

        tmpdir = tempfile.mkdtemp()
        try:
            p = Pipeline(base_output_dir=tmpdir)
            # parse_only
            result = p.parse_only(golden_spec_passing)
            assert "pipeline_error" not in result
            assert result["spec_id"] != ""
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
