"""Prompt 模板和版本管理——Phase 4A 核心模块。

本包提供：
- PromptManager：按 task/version 加载 Prompt 模板
- PromptTemplate：单次 Prompt 调用的完整模板（系统指令 + Schema 绑定 + 禁止事项）

所有 Prompt 模板存储在 `templates/{task}/v{NNN}.md`，
使用 YAML frontmatter 记录元数据，正文为系统指令。

设计原则：
1. Prompt 只能从版本化管理模板加载——不接受自由 Prompt 文本
2. 每个 Prompt 版本绑定到特定 Pydantic Schema——Gateway 用此校验 LLM 输出
3. 未知 task / version 立即报错——不 fallback
"""

from tianshu_datadev.prompts.manager import PromptManager, PromptTemplate

__all__ = [
    "PromptManager",
    "PromptTemplate",
]
