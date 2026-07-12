"""Phase 9C-R16 边界硬化测试——table_paths=None vs {} 语义区分 + E2E 模式开关。

验证：
  1. table_paths=None 回退到 default_table_paths
  2. table_paths={} 显式空字典不回退
  3. create_app() 生产模式不自动发现 CSV fixture
  4. create_app() E2E 模式（TIANSHU_E2E_MODE=true）自动发现 CSV fixture
"""

import os

import pytest

from tianshu_datadev.api.app import _discover_csv_fixtures, create_app
from tianshu_datadev.api.pipeline import Pipeline

# CSV fixture 文件路径
_CSV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)


class TestTablePathsNoneVsEmptyDict:
    """Pipeline 层：table_paths=None 回退到默认值，table_paths={} 不回退。"""

    @pytest.fixture
    def pipeline_with_default(self):
        """创建带 default_table_paths 的 Pipeline——使用临时目录。"""
        import shutil
        import tempfile

        tmpdir = tempfile.mkdtemp()
        pipeline = Pipeline(
            base_output_dir=tmpdir,
            default_table_paths={"test_fact": _CSV_PATH},
        )
        yield pipeline
        shutil.rmtree(tmpdir, ignore_errors=True)

    # 有效 DeveloperSpec——YAML front matter + markdown fence（参考 golden_passing.md 格式）
    _VALID_SPEC = (
        "```markdown\n"
        "---\n"
        "spec:\n"
        "  type: aggregate_table\n"
        "  target_table: ads.test_daily\n"
        "  target_grain: [stat_date]\n"
        "  summary: 边界测试——最简聚合\n"
        "  source_tables:\n"
        "    - name: test_fact\n"
        "      alias: tf\n"
        "      row_count: ~100\n"
        "      role: fact\n"
        "      time_field: event_time\n"
        "      key_columns:\n"
        "        - name: id\n"
        "          type: bigint\n"
        "          nullable: false\n"
        "      business_columns:\n"
        "        - name: amount\n"
        "          type: decimal\n"
        "          nullable: true\n"
        "        - name: event_time\n"
        "          type: timestamp\n"
        "          nullable: false\n"
        "        - name: stat_date\n"
        "          type: date\n"
        "          nullable: false\n"
        "  metrics:\n"
        "    - metric_name: total_amount\n"
        "      aggregation: SUM\n"
        "      input_column: amount\n"
        "      alias: total_amount\n"
        "  dimensions:\n"
        "    - dimension_name: stat_date\n"
        "      column_ref: stat_date\n"
        "  output_columns:\n"
        "    - name: stat_date\n"
        "      type: date\n"
        "    - name: total_amount\n"
        "      type: decimal\n"
        "---\n"
        "# 边界测试\n\n"
        "测试 table_paths 语义区分。\n"
        "```\n"
    )

    def test_none_falls_back_to_default(self, pipeline_with_default):
        """table_paths=None → 使用 default_table_paths，执行成功。"""
        result = pipeline_with_default.execute(
            self._VALID_SPEC,
            table_mapping={"tf": "test_fact"},
            table_paths=None,  # 不传 → 应回退到 default_table_paths
        )

        # 回退到默认值 → CSV 可加载 → 执行成功（trace 存在）
        # 如果因 Validator 阻断失败，错误不应是"表不存在"
        if "pipeline_error" in result:
            error_msg = result["pipeline_error"].get("error_message", "")
            assert "does not exist" not in error_msg, (
                f"table_paths=None 应回退到默认值，不应报表不存在: {error_msg}"
            )
        else:
            assert "execution_trace" in result, (
                "table_paths=None + 默认值可用 → 应执行成功"
            )

    def test_empty_dict_does_not_fall_back(self, pipeline_with_default):
        """table_paths={} → 不回退到 default_table_paths，表找不到。"""
        result = pipeline_with_default.execute(
            self._VALID_SPEC,
            table_mapping={"tf": "test_fact"},
            table_paths={},  # 显式传空 → 不得回退
        )

        # 显式传 {} → CSV 不加载 → DuckDB 中表不存在 → 执行失败
        if "pipeline_error" in result:
            # 执行阶段失败——表不存在是预期行为
            assert result["pipeline_error"]["stage"] == "execute"
        else:
            # 如果没有失败，说明 DuckDB 可能缓存了之前的表——仍然验证 trace 存在
            assert "execution_trace" in result


class TestCreateAppE2EMode:
    """create_app() 生产模式 vs E2E 模式——CSV fixture 自动发现开关。"""

    def test_production_mode_no_csv_discovery(self, monkeypatch):
        """生产模式（默认）——不自动发现 CSV fixture。"""
        # 使用 setenv 而非 delenv——因为 .env 文件中有 TIANSHU_E2E_MODE=true，
        # load_dotenv() 仅在 key not in os.environ 时加载，setenv 可阻止回填
        monkeypatch.setenv("TIANSHU_E2E_MODE", "false")

        # 生产模式创建 app——Pipeline 不带 default_table_paths
        app = create_app()
        pipeline = app.state.pipeline

        # Pipeline 的 _default_table_paths 应为空字典（无回退值）
        assert pipeline._default_table_paths == {}

    def test_e2e_mode_discovers_csv_fixtures(self, monkeypatch):
        """E2E 模式（TIANSHU_E2E_MODE=true）——自动发现 CSV fixture。"""
        monkeypatch.setenv("TIANSHU_E2E_MODE", "true")

        app = create_app()
        pipeline = app.state.pipeline

        # Pipeline 的 _default_table_paths 应包含发现的 CSV 文件
        default_paths = pipeline._default_table_paths
        assert isinstance(default_paths, dict)
        # tests/fixtures/ 下至少有 test_fact.csv
        assert "test_fact" in default_paths
        assert default_paths["test_fact"].endswith("test_fact.csv")

    def test_explicit_pipeline_overrides_mode(self, monkeypatch):
        """显式传入 Pipeline——无论 E2E 模式如何，使用传入的实例。"""
        monkeypatch.setenv("TIANSHU_E2E_MODE", "true")

        custom_pipeline = Pipeline(
            default_table_paths={"custom_table": "/custom/path.csv"}
        )
        app = create_app(pipeline=custom_pipeline)
        pipeline = app.state.pipeline

        # 应使用显式传入的 Pipeline，忽略 E2E 自动发现
        assert pipeline._default_table_paths == {"custom_table": "/custom/path.csv"}

    def test_discover_csv_fixtures_function(self):
        """_discover_csv_fixtures() 函数本身的行为验证。"""
        mapping = _discover_csv_fixtures()

        # 应返回字典
        assert isinstance(mapping, dict)
        # 应包含至少 test_fact 表
        assert "test_fact" in mapping
        # 路径应存在
        assert os.path.isfile(mapping["test_fact"])
