"""FakeLLMAdapter——确定性 Fake 适配器，供 pytest 使用。

通过 register_fixture() 注册已知的输入→输出映射，
每次调用返回输出副本（防止测试间状态泄漏）。

查找策略（按优先级）：
1. user_message 中包含 "fixture_key: {key}" → 按 key 查找
2. 注册了 task 级别的默认 fixture → 返回默认 fixture
3. 否则 → 抛出 AdapterError

未注册的输入抛出 AdapterError——模拟真实 LLM 不可用的场景。
真实 LLM 调用不进入 pytest 必需路径。
"""

from __future__ import annotations

import copy

from tianshu_datadev.llm.adapters.base import AdapterError, ProviderAdapter


class FakeLLMAdapter(ProviderAdapter):
    """确定性 Fake 适配器——用于 pytest，不需要真实 API key。

    工作方式：
    1. 测试通过 register_fixture(task, fixture_key, output) 注册已知输出
    2. 或通过 register_default_for_task(task, output) 注册 task 级默认输出
    3. invoke() 查找 fixture——找到则返回输出副本
    4. 找不到 → 抛出 AdapterError

    特性：
    - 输出副本隔离：每次 invoke() 返回 deepcopy，防止测试间状态泄漏
    - 多 task 独立：不同 task 的 fixture 互不干扰
    - 支持 extra 字段注入：可用于测试 Gateway 的 extra="forbid" 拒绝路径
    - 支持 task 默认 fixture：Gateway 测试无需在 Prompt 模板中嵌入 fixture_key
    """

    def __init__(self, fixtures: dict[str, dict] | None = None) -> None:
        """初始化 Fake Adapter。

        Args:
            fixtures: 可选的初始 fixture 映射——
                      key 格式为 fixture_key（全局唯一），
                      也支持 "{task}:{fixture_key}" 格式，
                      value 为要返回的 dict 输出
        """
        self._fixtures: dict[str, dict] = {}
        self._task_defaults: dict[str, dict] = {}
        if fixtures:
            for key, value in fixtures.items():
                self._fixtures[key] = copy.deepcopy(value)

    def register_fixture(
        self,
        task: str,
        fixture_key: str,
        output: dict,
    ) -> None:
        """注册一个已知的输入→输出映射。

        Args:
            task: 任务标识（如 "parse_developer_spec"）
            fixture_key: 此 fixture 的唯一标识（如 "valid_minimal" / "extra_field"）
            output: 此输入对应的 LLM 输出 dict
        """
        key = f"{task}:{fixture_key}"
        self._fixtures[key] = copy.deepcopy(output)

    def register_default_for_task(
        self,
        task: str,
        output: dict,
    ) -> None:
        """注册某 task 的默认 fixture——当 invoke() 找不到显式 fixture_key 时返回。

        这是 Gateway 集成测试的主要入口——Gateway 不修改 Prompt 模板添加 fixture_key，
        而是通过 Adapter 的 task 默认注册来匹配。

        Args:
            task: 任务标识（如 "developer_spec_parser"）
            output: 当 LLM 被调用此 task 时返回的 dict 输出
        """
        self._task_defaults[task] = copy.deepcopy(output)

    def invoke(
        self,
        system_message: str,
        user_message: str,
        json_schema: dict,
        model: str,
        temperature: float,
    ) -> dict:
        """查找并返回注册的 fixture 输出。

        查找顺序：
        1. 从 user_message 中提取 fixture_key → 查找 "{task}:{fixture_key}"
        2. 查找 fixture_key（全局 key，向后兼容）
        3. 查找 task 级别的默认 fixture
        4. 以上均无 → AdapterError

        Args:
            system_message: 系统指令（Fake 模式下用于提取 task 信息）
            user_message: 用户消息——可含 "fixture_key: {key}" 行
            json_schema: JSON Schema（Fake 模式下忽略）
            model: 模型标识（Fake 模式下忽略）
            temperature: LLM 温度（Fake 模式下忽略）

        Returns:
            注册的 fixture 输出副本

        Raises:
            AdapterError: 未找到对应的 fixture
        """
        # ── 策略 1：从 user_message 中提取显式 fixture_key ──
        fixture_key = None
        for line in user_message.splitlines():
            stripped = line.strip()
            if stripped.startswith("fixture_key:"):
                fixture_key = stripped.split(":", 1)[1].strip()
                break

        if fixture_key:
            # 尝试 "{task}:{fixture_key}" 格式——需要从 fixture_key 推断 task
            # 先检查所有 task 前缀的组合
            for key, value in self._fixtures.items():
                if key.endswith(f":{fixture_key}"):
                    return copy.deepcopy(value)
            # 再尝试全局 key
            if fixture_key in self._fixtures:
                return copy.deepcopy(self._fixtures[fixture_key])

        # ── 策略 2：从 system_message 推断 task，查找默认 fixture ──
        # system_message 来自 Prompt 模板——通常标题行包含 task 提示
        # 遍历所有注册的 task 默认值，匹配 system_message
        for task, default_output in self._task_defaults.items():
            # 尝试多种匹配方式——task 名对应的模板目录名
            task_variants = [
                task,
                task.replace("_", " "),
                task.replace("_", "-"),
            ]
            for variant in task_variants:
                if variant.lower() in system_message.lower():
                    return copy.deepcopy(default_output)

        # ── 策略 3：如果只有一个 task 默认值，直接返回 ──
        if len(self._task_defaults) == 1:
            return copy.deepcopy(next(iter(self._task_defaults.values())))

        raise AdapterError(
            f"FakeLLMAdapter 未找到 fixture（fixture_key={fixture_key!r}，"
            f"已注册 task 默认值：{sorted(self._task_defaults.keys())}）——"
            f"请先调用 register_fixture() 或 register_default_for_task() 注册",
            provider=self.provider_name(),
        )

    def provider_name(self) -> str:
        """Fake 适配器的 Provider 名称。"""
        return "fake"
