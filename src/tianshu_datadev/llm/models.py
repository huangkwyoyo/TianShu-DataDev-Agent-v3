"""Phase 4A LLM Gateway 数据模型——LlmRequest / LlmResponse / SchemaBinding 等。

所有模型继承 StrictModel（extra="forbid"），确保 LLM 输出不引入未声明的字段。
Gateway 仅返回结构化对象引用和校验状态——LLM 原始文本绝不进入 Compiler。
"""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Literal

from pydantic import model_validator

from tianshu_datadev.developer_spec.models import StrictModel


class ArtifactRef(StrictModel):
    """结构化 artifact 引用——不传递完整 artifact 内容，仅通过哈希引用。

    设计原则：LLM 不应直接访问原始 artifact 内容——
    Gateway 通过引用管理 artifact 的传递和版本追踪。
    """

    artifact_type: str  # "parsed_developer_spec" | "source_manifest" | "sql_build_plan" | ...
    artifact_hash: str  # 内容哈希（SHA256 前 12 位）
    artifact_id: str    # 人类可读的 artifact 标识


class LlmRequest(StrictModel):
    """LLM Gateway 请求——所有字段由系统确定性填充，LLM 不可自由构造。

    每次 LLM 调用必须携带完整的 Prompt 版本、Schema 绑定和输入引用——
    确保调用可复现、可审计、可回归。
    """

    request_id: str                    # 请求唯一标识
    # 合法值："parse_developer_spec" | "plan_relationship" | "plan_sql_build" | "plan_sql_program"
    task: str
    prompt_version: str                # Prompt 版本号（如 "v001"）
    schema_name: str                   # 目标 Pydantic Schema 名称
    schema_version: str                # Schema 版本号
    input_artifact_refs: list[ArtifactRef] = []  # 输入 artifact 引用列表
    temperature: float = 0.0           # LLM 温度——默认 0 = 确定性输出
    model: str = ""                    # 目标模型标识

    @staticmethod
    def generate_request_id() -> str:
        """生成唯一请求 ID——基于 UUID4 + 微秒时间戳。"""
        ts = str(time.time()).replace(".", "_")
        short_uuid = uuid.uuid4().hex[:8]
        return f"llm_req_{ts}_{short_uuid}"


class LlmResponse(StrictModel):
    """LLM Gateway 响应——仅包含引用和校验状态，不含 LLM 原始文本。

    核心保证：
    - raw_response_ref 仅用于审查日志——不进入编译链路
    - parsed_json_ref 指向通过 Schema 校验的结构化对象
    - validation_status="invalid" 时 parsed_json_ref 为 None
    - token_usage / latency_ms 用于成本追踪和性能监控
    """

    request_id: str
    task: str
    prompt_version: str
    schema_name: str
    schema_version: str
    raw_response_ref: str              # LLM 原始响应落盘引用（仅用于审查，不进入编译链路）
    parsed_json_ref: str | None = None # 通过 Schema 校验的结构化输出落盘引用
    validation_status: Literal["valid", "invalid"]  # 仅允许这两个值
    validation_errors: list[str] = []  # 校验错误详情列表
    token_usage: dict[str, int] = {}   # {"prompt_tokens": N, "completion_tokens": N, "total_tokens": N}
    latency_ms: int = 0                # 总调用延迟（毫秒）

    @property
    def is_valid(self) -> bool:
        """便捷方法——判断校验是否通过。"""
        return self.validation_status == "valid"

    @model_validator(mode="after")
    def _check_invariants(self) -> "LlmResponse":
        """跨字段一致性校验——确保 valid/invalid 状态与附属字段一致。

        规则：
        - validation_status="valid" → parsed_json_ref 非空，validation_errors 为空
        - validation_status="invalid" → parsed_json_ref 为 None
        这些是硬约束——任何代码路径构造 LlmResponse 时都必须遵守，
        不依赖调用方的纪律。
        """
        if self.validation_status == "valid":
            if self.parsed_json_ref is None:
                raise ValueError(
                    "validation_status='valid' 要求 parsed_json_ref 非空"
                )
            if self.validation_errors:
                raise ValueError(
                    f"validation_status='valid' 要求 validation_errors 为空，"
                    f"当前有 {len(self.validation_errors)} 条错误"
                )
        elif self.validation_status == "invalid":
            if self.parsed_json_ref is not None:
                raise ValueError(
                    "validation_status='invalid' 要求 parsed_json_ref 为 None"
                )
        return self

    @staticmethod
    def generate_response_ref(request_id: str) -> str:
        """生成原始响应落盘引用路径——相对于 response_root。"""
        hash_hex = hashlib.sha256(
            f"raw_response:{request_id}".encode()
        ).hexdigest()[:12]
        return f"raw/{request_id}_{hash_hex}.json"

    @staticmethod
    def generate_parsed_ref(request_id: str) -> str:
        """生成结构化输出落盘引用路径——相对于 response_root。"""
        hash_hex = hashlib.sha256(
            f"parsed_output:{request_id}".encode()
        ).hexdigest()[:12]
        return f"parsed/{request_id}_{hash_hex}.json"


class LlmTraceNode(StrictModel):
    """单个 LLM 节点调用诊断元数据。

    仅含诊断信息——不含 prompt 原文、raw response、业务数据。
    不可进入 IR、不可影响路由、不可参与 REVIEW_READY 判定。
    """
    node_name: str
    # 合法值：
    #   "requirement_planner" | "spec_enricher" | "relationship_planner" |
    #   "label_extractor" | "spark_developer"
    model: str              # 实际模型标识（Fake 时为 "fake"）
    token_usage: dict[str, int] = {}
    # {"prompt_tokens": N, "completion_tokens": N, "total_tokens": N}
    latency_ms: int = 0     # 总延迟（毫秒）
    status: str = "skipped"
    # "valid" | "invalid" | "skipped" | "error"
    error_type: str | None = None
    # 失败时的 AdapterError 类型字符串


class SchemaBinding(StrictModel):
    """任务到 Pydantic Schema 的绑定——Gateway 用此校验 LLM 输出的 JSON。

    pydantic_model_path 指向实际的 Pydantic 模型类——
    Gateway 通过 importlib 加载该类并调用 model_validate() 执行校验。
    json_schema 是从 Pydantic 模型生成的 JSON Schema dict——
    传递给 LLM Provider 用于 function calling / structured output。
    """

    task: str                          # 任务标识
    schema_name: str                   # Pydantic 模型类名（如 "ParsedDeveloperSpec"）
    schema_version: str                # Schema 版本号
    # 完整导入路径（如 "tianshu_datadev.developer_spec.models.ParsedDeveloperSpec"）
    pydantic_model_path: str
    json_schema: dict = {}             # 从 Pydantic 模型生成的 JSON Schema（传给 LLM）


class PromptVersion(StrictModel):
    """Prompt 版本元数据——记录目标 Schema、输入 artifact、禁止事项和变更说明。

    每次 Prompt 升级必须更新此模型中的版本信息——
    确保每次 LLM 调用可追溯到具体的 Prompt 版本和 Schema 绑定。
    """

    task: str                          # 任务标识
    version: str                       # 版本号（如 "v001"）
    target_schema: str                 # 目标 Pydantic 模型名称
    input_artifacts: list[str] = []    # 所需输入 artifact 类型列表
    forbidden: list[str] = []          # 明确禁止 LLM 产出的内容
    examples_count: int = 0            # 附带的 few-shot 示例数量
    rejection_policy: str = "strict"   # 拒绝策略——"strict"：非法 JSON / extra / Schema 不匹配一律拒绝
    changelog: str = ""                # 版本变更记录
