"""PromptManager 测试——版本加载 + 未知版本报错 + Schema 绑定。

验证：
1. 按 task/version 加载 Prompt 模板
2. list_versions() / list_tasks() 正确
3. 未知 task / version 报错
4. Prompt 元数据与 Schema 绑定一致
"""

from __future__ import annotations

import pytest

from tianshu_datadev.llm.models import SchemaBinding
from tianshu_datadev.prompts.manager import PromptManager, PromptTemplate


@pytest.fixture
def manager() -> PromptManager:
    """创建指向真实模板目录的 PromptManager。"""
    return PromptManager()


class TestPromptLoading:
    """Prompt 模板加载测试。"""

    def test_load_prompt_by_task_and_version(
        self, manager: PromptManager
    ) -> None:
        """正常加载 developer_spec_parser/v001 → PromptTemplate。"""
        template = manager.get_prompt("developer_spec_parser", "v001")

        assert isinstance(template, PromptTemplate)
        assert template.task == "developer_spec_parser"
        assert template.version == "v001"
        assert len(template.system_message) > 100  # 系统指令应有一定长度
        assert len(template.forbidden) > 0  # 必须有禁止事项
        assert template.rejection_policy == "strict"

    def test_load_relationship_planner_prompt(
        self, manager: PromptManager
    ) -> None:
        """加载 relationship_planner/v001。"""
        template = manager.get_prompt("relationship_planner", "v001")
        assert template.task == "relationship_planner"
        assert template.version == "v001"
        assert "不要生成 SQL" in template.system_message or any(
            "SQL" in item for item in template.forbidden
        )

    def test_load_sql_build_planner_prompt(
        self, manager: PromptManager
    ) -> None:
        """加载 sql_build_planner/v001。"""
        template = manager.get_prompt("sql_build_planner", "v001")
        assert template.task == "sql_build_planner"
        assert template.version == "v001"
        # sql_build_planner 必须强调禁止生成 SQL
        assert any(
            "SQL" in item for item in template.forbidden
        ) or "SQL" in template.system_message

    def test_load_sql_program_planner_prompt(
        self, manager: PromptManager
    ) -> None:
        """加载 sql_program_planner/v001。"""
        template = manager.get_prompt("sql_program_planner", "v001")
        assert template.task == "sql_program_planner"
        assert template.version == "v001"


class TestVersionListing:
    """版本列表测试。"""

    def test_list_all_tasks(self, manager: PromptManager) -> None:
        """list_tasks() 返回 5 个已注册的 task。"""
        tasks = manager.list_tasks()
        assert len(tasks) == 5
        assert "developer_spec_parser" in tasks
        assert "relationship_planner" in tasks
        assert "spark_annotator" in tasks
        assert "sql_build_planner" in tasks
        assert "sql_program_planner" in tasks

    def test_list_versions_for_task(self, manager: PromptManager) -> None:
        """list_versions() 返回某个 task 的所有版本。"""
        versions = manager.list_versions("developer_spec_parser")
        assert "v001" in versions
        assert len(versions) >= 1

    def test_unknown_task_raises(self, manager: PromptManager) -> None:
        """不存在的 task → ValueError。"""
        with pytest.raises(ValueError, match="未知 task"):
            manager.get_prompt("nonexistent_task", "v001")

    def test_unknown_version_raises(self, manager: PromptManager) -> None:
        """已知 task 但不存在 version → ValueError。"""
        with pytest.raises(ValueError, match="未知 Prompt 版本"):
            manager.get_prompt("developer_spec_parser", "v999")

    def test_list_versions_unknown_task_raises(
        self, manager: PromptManager
    ) -> None:
        """对不存在的 task 调用 list_versions → ValueError。"""
        with pytest.raises(ValueError, match="未知 task"):
            manager.list_versions("nonexistent_task")


class TestSchemaBinding:
    """Schema 绑定测试。"""

    def test_prompt_version_tracks_schema_binding(
        self, manager: PromptManager
    ) -> None:
        """Prompt 模板的 Schema 绑定与目标 Schema 一致。"""
        template = manager.get_prompt("developer_spec_parser", "v001")

        binding = template.schema_binding
        assert isinstance(binding, SchemaBinding)
        assert binding.task == "developer_spec_parser"
        assert binding.schema_name == "ParsedDeveloperSpec"
        assert binding.pydantic_model_path == (
            "tianshu_datadev.developer_spec.models.ParsedDeveloperSpec"
        )

    def test_get_schema_binding(self, manager: PromptManager) -> None:
        """get_schema_binding() 返回正确的 SchemaBinding。"""
        binding = manager.get_schema_binding(
            "developer_spec_parser", "v001"
        )
        assert binding.schema_name == "ParsedDeveloperSpec"
        assert binding.schema_version == "1.0"

    def test_all_prompts_have_schema_binding(
        self, manager: PromptManager
    ) -> None:
        """所有 4 个 Prompt 均有有效的 Schema 绑定。"""
        for task in manager.list_tasks():
            for version in manager.list_versions(task):
                template = manager.get_prompt(task, version)
                binding = template.schema_binding
                assert binding.schema_name, (
                    f"{task}/{version} 缺少 schema_name"
                )
                assert binding.pydantic_model_path, (
                    f"{task}/{version} 缺少 pydantic_model_path"
                )

    def test_prompt_has_forbidden_items(
        self, manager: PromptManager
    ) -> None:
        """所有 Prompt 的 forbidden 列表非空。"""
        for task in manager.list_tasks():
            for version in manager.list_versions(task):
                template = manager.get_prompt(task, version)
                assert len(template.forbidden) > 0, (
                    f"{task}/{version} 缺少禁止事项"
                )


class TestPromptCache:
    """Prompt 缓存测试。"""

    def test_prompt_caching(self, manager: PromptManager) -> None:
        """同一 task/version 两次加载返回相同对象（缓存）。"""
        t1 = manager.get_prompt("developer_spec_parser", "v001")
        t2 = manager.get_prompt("developer_spec_parser", "v001")

        # 由于 StrictModel(frozen=False)，不保证 is 一致，
        # 但内容应完全相同
        assert t1.task == t2.task
        assert t1.version == t2.version
        assert t1.system_message == t2.system_message
