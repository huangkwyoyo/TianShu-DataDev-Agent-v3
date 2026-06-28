"""PromptManager——Prompt 版本管理器。

按 task/version 加载和管理 Prompt 模板，每个模板对应一个 Markdown 文件。
文件头部使用 YAML frontmatter 记录元数据（目标 Schema、禁止事项、变更说明等），
正文为系统指令 + 用户消息模板。

设计原则：
- Prompt 只能从版本化模板加载——不接受自由 Prompt 文本
- 每次 LLM 调用可追溯到具体的 Prompt 版本和 Schema 绑定
- 未知 task / version 立即报错——不 fallback
"""

from __future__ import annotations

from pathlib import Path

import yaml

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.llm.models import PromptVersion, SchemaBinding

# ════════════════════════════════════════════
# Schema 名称 → Pydantic 模型路径映射
# ════════════════════════════════════════════

_SCHEMA_PATH_MAP: dict[str, str] = {
    "ParsedDeveloperSpec": (
        "tianshu_datadev.developer_spec.models.ParsedDeveloperSpec"
    ),
    "SourceManifest": (
        "tianshu_datadev.developer_spec.models.SourceManifest"
    ),
    "RelationshipHypothesis": (
        "tianshu_datadev.planning.relationship_hypothesis.RelationshipHypothesis"
    ),
    "SqlBuildPlan": "tianshu_datadev.planning.sql_build_plan.SqlBuildPlan",
    "SqlProgram": "tianshu_datadev.planning.sql_program.SqlProgram",
}


class PromptTemplate(StrictModel):
    """Prompt 模板——包含系统指令、Schema 绑定、禁止事项。

    每个 Prompt 版本是一个独立的 Markdown 文件，
    由 PromptManager 加载为 PromptTemplate 对象。
    """

    task: str                          # 任务标识
    version: str                       # 版本号
    system_message: str                # 系统指令（Markdown 正文）
    user_message_template: str         # 用户消息模板（含 {var} 占位符）
    schema_binding: SchemaBinding      # 目标 Schema 绑定
    forbidden: list[str] = []          # 禁止事项列表
    examples: list[dict] = []          # few-shot 示例
    rejection_policy: str = "strict"   # 拒绝策略


class PromptManager:
    """Prompt 版本管理器——按 task/version 加载 Prompt 模板。

    模板文件结构：
        prompts/templates/
          {task}/
            v001.md
            v002.md

    每个 .md 文件以 YAML frontmatter 开头（--- 分隔），
    包含 task / version / target_schema / forbidden / changelog 等元数据，
    正文为系统指令。

    使用方式：
        manager = PromptManager()
        template = manager.get_prompt("developer_spec_parser", "v001")
        # template.system_message → 系统指令
        # template.forbidden → 禁止事项
        # template.schema_binding → Schema 绑定
    """

    # 默认模板根目录——相对于包安装位置
    _DEFAULT_TEMPLATES_ROOT = "src/tianshu_datadev/prompts/templates"

    def __init__(self, templates_root: str | None = None) -> None:
        """初始化 Prompt 管理器。

        Args:
            templates_root: 模板根目录路径——
                           若为 None，使用默认路径
        """
        if templates_root is None:
            # 尝试从当前工作目录定位
            cwd = Path.cwd()
            candidate = cwd / self._DEFAULT_TEMPLATES_ROOT
            if candidate.is_dir():
                templates_root = str(candidate)
            else:
                # fallback：从本文件位置反向定位
                this_dir = Path(__file__).resolve().parent
                templates_root = str(this_dir / "templates")

        self._templates_root = Path(templates_root)
        if not self._templates_root.is_dir():
            raise ValueError(
                f"Prompt 模板根目录不存在：{self._templates_root}"
            )

        # 缓存已加载的 Prompt——key 为 "{task}:{version}"
        self._cache: dict[str, PromptTemplate] = {}

    @property
    def templates_root(self) -> str:
        """返回模板根目录路径。"""
        return str(self._templates_root)

    # ── 公开方法 ──

    def get_prompt(self, task: str, version: str) -> PromptTemplate:
        """按 task + version 加载 Prompt 模板。

        Args:
            task: 任务标识（如 "developer_spec_parser"）
            version: 版本号（如 "v001"）

        Returns:
            PromptTemplate——包含系统指令、Schema 绑定、禁止事项

        Raises:
            ValueError: task 不存在、version 不存在或模板文件损坏
        """
        cache_key = f"{task}:{version}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        template = self._load_template(task, version)
        self._cache[cache_key] = template
        return template

    def list_versions(self, task: str) -> list[str]:
        """列出某 task 的所有可用版本。

        Args:
            task: 任务标识

        Returns:
            版本号列表（按字母序排列，如 ["v001", "v002"]）

        Raises:
            ValueError: task 目录不存在
        """
        task_dir = self._templates_root / task
        if not task_dir.is_dir():
            raise ValueError(f"未知 task：'{task}'——模板目录不存在：{task_dir}")

        versions: list[str] = []
        for entry in sorted(task_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".md":
                versions.append(entry.stem)  # "v001" from "v001.md"

        return sorted(versions)

    def list_tasks(self) -> list[str]:
        """列出所有已注册的 task。

        Returns:
            task 名称列表（按字母序排列）
        """
        tasks: list[str] = []
        for entry in sorted(self._templates_root.iterdir()):
            if entry.is_dir():
                tasks.append(entry.name)
        return sorted(tasks)

    def get_schema_binding(self, task: str, version: str) -> SchemaBinding:
        """获取某 task/version 的 Schema 绑定。

        Args:
            task: 任务标识
            version: 版本号

        Returns:
            SchemaBinding——任务到 Pydantic Schema 的绑定信息
        """
        template = self.get_prompt(task, version)
        return template.schema_binding

    # ── 内部方法 ──

    def _load_template(self, task: str, version: str) -> PromptTemplate:
        """从文件系统加载单个 Prompt 模板。

        解析 Markdown 文件的 YAML frontmatter + 正文，
        构造 PromptTemplate 对象。

        Args:
            task: 任务标识
            version: 版本号

        Returns:
            PromptTemplate

        Raises:
            ValueError: 文件不存在、YAML 解析失败、必填字段缺失
        """
        # 验证 task 目录存在
        task_dir = self._templates_root / task
        if not task_dir.is_dir():
            raise ValueError(
                f"未知 task：'{task}'——目录不存在：{task_dir}"
            )

        # 验证模板文件存在
        template_path = task_dir / f"{version}.md"
        if not template_path.is_file():
            available = self.list_versions(task)
            raise ValueError(
                f"未知 Prompt 版本：task='{task}' version='{version}'——"
                f"可用版本：{available}"
            )

        # 读取文件内容
        raw_content = template_path.read_text(encoding="utf-8")

        # 解析 YAML frontmatter
        frontmatter, body = self._parse_frontmatter(raw_content, str(template_path))

        # 验证必填字段
        required_fields = ["task", "version", "target_schema"]
        for field in required_fields:
            if field not in frontmatter:
                raise ValueError(
                    f"Prompt 模板 '{template_path}' 的 YAML frontmatter "
                    f"缺少必填字段：'{field}'"
                )

        # 验证 frontmatter 中的 task/version 与路径一致
        fm_task = frontmatter["task"]
        fm_version = frontmatter["version"]
        if fm_task != task:
            raise ValueError(
                f"Prompt 模板 '{template_path}' frontmatter task='{fm_task}' "
                f"与路径 task='{task}' 不一致"
            )
        if fm_version != version:
            raise ValueError(
                f"Prompt 模板 '{template_path}' frontmatter version='{fm_version}' "
                f"与文件名 version='{version}' 不一致"
            )

        # 构造 PromptVersion 元数据
        prompt_version = PromptVersion(
            task=fm_task,
            version=fm_version,
            target_schema=frontmatter["target_schema"],
            input_artifacts=frontmatter.get("input_artifacts", []),
            forbidden=frontmatter.get("forbidden", []),
            examples_count=frontmatter.get("examples_count", 0),
            rejection_policy=frontmatter.get("rejection_policy", "strict"),
            changelog=frontmatter.get("changelog", ""),
        )

        # 构造 SchemaBinding——从目标 Schema 的 Pydantic 模型生成 JSON Schema
        schema_binding = self._build_schema_binding(
            task=fm_task,
            target_schema=frontmatter["target_schema"],
            schema_version=frontmatter.get("schema_version", "1.0"),
        )

        # 构造 PromptTemplate
        return PromptTemplate(
            task=fm_task,
            version=fm_version,
            system_message=body.strip(),
            user_message_template=body.strip(),  # 默认 user_message 与 system 相同
            schema_binding=schema_binding,
            forbidden=prompt_version.forbidden,
            examples=[],  # 示例从 body 中提取（暂不实现）
            rejection_policy=prompt_version.rejection_policy,
        )

    @staticmethod
    def _parse_frontmatter(
        raw_content: str, path_hint: str
    ) -> tuple[dict, str]:
        """解析 Markdown 文件的 YAML frontmatter。

        格式：
            ---
            key: value
            ...
            ---
            Markdown 正文...

        Args:
            raw_content: 文件原始内容
            path_hint: 文件路径（用于错误消息）

        Returns:
            (frontmatter_dict, body_text)

        Raises:
            ValueError: frontmatter 格式不正确
        """
        lines = raw_content.splitlines()

        # 检查是否以 --- 开头
        if not lines or lines[0].strip() != "---":
            raise ValueError(
                f"Prompt 模板 '{path_hint}' 必须以 YAML frontmatter（---）开头"
            )

        # 查找结束的 ---
        end_idx = None
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                end_idx = i
                break

        if end_idx is None:
            raise ValueError(
                f"Prompt 模板 '{path_hint}' 的 YAML frontmatter 缺少结束标记 '---'"
            )

        # 提取 YAML 部分
        yaml_lines = lines[1:end_idx]
        yaml_text = "\n".join(yaml_lines)

        try:
            frontmatter = yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise ValueError(
                f"Prompt 模板 '{path_hint}' YAML frontmatter 解析失败：{e}"
            ) from e

        if not isinstance(frontmatter, dict):
            raise ValueError(
                f"Prompt 模板 '{path_hint}' 的 YAML frontmatter 必须是映射（dict）"
            )

        # 提取正文（end_idx 之后的内容）
        body_lines = lines[end_idx + 1:]
        body = "\n".join(body_lines)

        return frontmatter, body

    @staticmethod
    def _build_schema_binding(
        task: str,
        target_schema: str,
        schema_version: str,
    ) -> SchemaBinding:
        """根据目标 Schema 名称构造 SchemaBinding。

        通过 target_schema 名称映射到实际的 Pydantic 模型类路径，
        并调用 model_json_schema() 生成 JSON Schema。

        Args:
            task: 任务标识
            target_schema: 目标 Pydantic 模型类名
            schema_version: Schema 版本号

        Returns:
            SchemaBinding——含 pydantic_model_path 和 json_schema
        """
        model_path = _SCHEMA_PATH_MAP.get(target_schema)
        if model_path is None:
            raise ValueError(
                f"未知的目标 Schema：'{target_schema}'——"
                f"已知 Schema：{sorted(_SCHEMA_PATH_MAP.keys())}"
            )

        # 动态导入 Pydantic 模型并生成 JSON Schema
        try:
            model_cls = _import_pydantic_model(model_path)
            json_schema = model_cls.model_json_schema()
        except Exception as e:
            # 导入失败时不阻断——记录空 Schema（真实 LLM 调用时再处理）
            json_schema = {
                "_error": f"无法生成 JSON Schema：{e}",
                "_model_path": model_path,
            }

        return SchemaBinding(
            task=task,
            schema_name=target_schema,
            schema_version=schema_version,
            pydantic_model_path=model_path,
            json_schema=json_schema,
        )


def _import_pydantic_model(model_path: str):
    """动态导入 Pydantic 模型类。

    Args:
        model_path: 完整导入路径（如 "tianshu_datadev.developer_spec.models.ParsedDeveloperSpec"）

    Returns:
        Pydantic 模型类

    Raises:
        ImportError: 模块或类不存在
    """
    parts = model_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"无效的模型路径：'{model_path}'——应为 'module.ClassName'")

    module_path, class_name = parts

    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)
