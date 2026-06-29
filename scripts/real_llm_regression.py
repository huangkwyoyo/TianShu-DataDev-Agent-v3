"""真实 LLM 回归验证——使用 AnthropicAdapter 对 4 个任务的 Prompt 模板进行端到端验证。

用法：
    python scripts/real_llm_regression.py

前提：
    - .env 中已配置 DEEPSEEK_API_KEY
    - 网络可访问 api.deepseek.com

输出：
    - 每个用例的 validation_status / token_usage / latency_ms
    - 总体通过率统计
    - 失败用例的详细错误信息
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# 确保项目根目录在 sys.path 中
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "src"))

from tianshu_datadev.config import load_dotenv
from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter
from tianshu_datadev.llm.gateway import _format_validation_errors, _import_pydantic_model
from tianshu_datadev.prompts.manager import PromptManager

# ════════════════════════════════════════════
# 样本输入数据——覆盖 4 个任务 × 2-3 个场景
# ════════════════════════════════════════════

# ── 任务 1：DeveloperSpec Parser ──

DEV_SPEC_DAU_YAML = """\
title: 日活跃用户数 (DAU)
description: 计算每日活跃用户数量，按日期统计去重 user_id
input_tables:
  - table_alias: logs
    source_table: dw.user_behavior_logs
    role: fact
    description: 用户行为日志表
    columns:
      - column_name: user_id
        data_type: bigint
        description: 用户ID
      - column_name: action_date
        data_type: date
        description: 行为日期
      - column_name: action_type
        data_type: varchar
        description: 行为类型
    partition_field: dt
    time_field: action_date
    key_columns: [user_id]
metrics:
  - name: dau
    aggregation: COUNT_DISTINCT
    field: user_id
    alias: dau
    description: 日活跃用户数
dimensions:
  - name: action_date
    field: action_date
    type: date
    description: 行为日期
time_range:
  field: action_date
  start: "2025-01-01"
  end: "2025-01-31"
output_spec:
  format: table
  sort_by: [action_date]
  limit: 1000"""

DEV_SPEC_DAU_MANIFEST = {
    "tables": [
        {
            "table_alias": "logs",
            "source_table": "dw.user_behavior_logs",
            "row_count": 50000000,
            "columns": [
                {"column_name": "user_id", "data_type": "bigint", "nullable": False},
                {"column_name": "action_date", "data_type": "date", "nullable": False},
                {"column_name": "action_type", "data_type": "varchar", "nullable": True},
            ],
            "partition_field": "dt",
            "time_field": "action_date",
            "key_columns": ["user_id"],
        }
    ]
}

DEV_SPEC_SALES_YAML = """\
title: 用户消费汇总
description: 按用户汇总订单金额，关联用户信息表获取用户名称
input_tables:
  - table_alias: orders
    source_table: dw.orders
    role: fact
    description: 订单表
    columns:
      - column_name: user_id
        data_type: bigint
      - column_name: amount
        data_type: decimal
      - column_name: order_time
        data_type: timestamp
    partition_field: dt
    time_field: order_time
    key_columns: [user_id, order_id]
  - table_alias: users
    source_table: dim.users
    role: dimension
    description: 用户维度表
    columns:
      - column_name: user_id
        data_type: bigint
      - column_name: user_name
        data_type: varchar
      - column_name: city
        data_type: varchar
    key_columns: [user_id]
joins:
  - left_table: orders
    right_table: users
    left_key: user_id
    right_key: user_id
    join_type: INNER
    description: 关联用户信息
metrics:
  - name: total_amount
    aggregation: SUM
    field: amount
    alias: total_amount
    description: 用户消费总额
dimensions:
  - name: user_name
    field: user_name
    type: string
  - name: city
    field: city
    type: string
time_range:
  field: order_time
  start: "2025-06-01"
  end: "2025-06-30"
output_spec:
  format: table
  sort_by: [total_amount]
  sort_direction: DESC
  limit: 100"""

DEV_SPEC_SALES_MANIFEST = {
    "tables": [
        {
            "table_alias": "orders",
            "source_table": "dw.orders",
            "row_count": 200000000,
            "columns": [
                {"column_name": "user_id", "data_type": "bigint", "nullable": False},
                {"column_name": "amount", "data_type": "decimal(18,2)", "nullable": False},
                {"column_name": "order_time", "data_type": "timestamp", "nullable": False},
                {"column_name": "order_id", "data_type": "bigint", "nullable": False},
            ],
            "partition_field": "dt",
            "time_field": "order_time",
            "key_columns": ["user_id", "order_id"],
        },
        {
            "table_alias": "users",
            "source_table": "dim.users",
            "row_count": 5000000,
            "columns": [
                {"column_name": "user_id", "data_type": "bigint", "nullable": False},
                {"column_name": "user_name", "data_type": "varchar", "nullable": False},
                {"column_name": "city", "data_type": "varchar", "nullable": True},
            ],
            "key_columns": ["user_id"],
        },
    ]
}

# ── 上下文输入——JSON 格式的 ParsedDeveloperSpec（供下游任务使用）──

# DAU 场景的 ParsedDeveloperSpec（模拟 LLM 已经解析完成的结果）
PARSED_SPEC_DAU = {
    "spec_id": "spec_dau_001",
    "spec_hash": "abc123dau001",
    "title": "日活跃用户数 (DAU)",
    "description": "计算每日活跃用户数量，按日期统计去重 user_id",
    "input_tables": [
        {
            "table_alias": "logs",
            "source_table": "dw.user_behavior_logs",
            "row_count": 50000000,
            "role": "fact",
            "description": "用户行为日志表",
            "columns": [
                {"column_name": "user_id", "data_type": "bigint", "description": "用户ID"},
                {"column_name": "action_date", "data_type": "date", "description": "行为日期"},
                {"column_name": "action_type", "data_type": "varchar", "description": "行为类型"},
            ],
            "partition_field": "dt",
            "time_field": "action_date",
            "key_columns": ["user_id"],
        }
    ],
    "metrics": [
        {
            "name": "dau",
            "aggregation": "COUNT_DISTINCT",
            "field": "user_id",
            "alias": "dau",
            "description": "日活跃用户数",
        }
    ],
    "dimensions": [
        {"name": "action_date", "field": "action_date", "type": "date", "description": "行为日期"}
    ],
    "time_range": {"field": "action_date", "start": "2025-01-01", "end": "2025-01-31"},
    "output_spec": {"format": "table", "sort_by": ["action_date"], "limit": 1000},
}

# 销售场景的 ParsedDeveloperSpec
PARSED_SPEC_SALES = {
    "spec_id": "spec_sales_002",
    "spec_hash": "xyz789sales02",
    "title": "用户消费汇总",
    "description": "按用户汇总订单金额，关联用户信息表获取用户名称",
    "input_tables": [
        {
            "table_alias": "orders",
            "source_table": "dw.orders",
            "row_count": 200000000,
            "role": "fact",
            "description": "订单表",
            "columns": [
                {"column_name": "user_id", "data_type": "bigint"},
                {"column_name": "amount", "data_type": "decimal"},
                {"column_name": "order_time", "data_type": "timestamp"},
                {"column_name": "order_id", "data_type": "bigint"},
            ],
            "partition_field": "dt",
            "time_field": "order_time",
            "key_columns": ["user_id", "order_id"],
        },
        {
            "table_alias": "users",
            "source_table": "dim.users",
            "row_count": 5000000,
            "role": "dimension",
            "description": "用户维度表",
            "columns": [
                {"column_name": "user_id", "data_type": "bigint"},
                {"column_name": "user_name", "data_type": "varchar"},
                {"column_name": "city", "data_type": "varchar"},
            ],
            "key_columns": ["user_id"],
        },
    ],
    "joins": [
        {
            "left_table": "orders",
            "right_table": "users",
            "left_key": "user_id",
            "right_key": "user_id",
            "join_type": "INNER",
            "description": "关联用户信息",
        }
    ],
    "metrics": [
        {
            "name": "total_amount",
            "aggregation": "SUM",
            "field": "amount",
            "alias": "total_amount",
            "description": "用户消费总额",
        }
    ],
    "dimensions": [
        {"name": "user_name", "field": "user_name", "type": "string"},
        {"name": "city", "field": "city", "type": "string"},
    ],
    "time_range": {"field": "order_time", "start": "2025-06-01", "end": "2025-06-30"},
    "output_spec": {"format": "table", "sort_by": ["total_amount"], "limit": 100},
}

# ── RelationshipHypothesis 样本（供 SqlBuildPlanner 使用）──

REL_HYP_DAU = {
    "hypothesis_id": "hyp_dau_single",
    "spec_hash": "abc123dau001",
    "source_manifest_hash": "man_dau_001",
    "candidates": [],
    "multi_table": False,
}

REL_HYP_SALES = {
    "hypothesis_id": "hyp_sales_join",
    "spec_hash": "xyz789sales02",
    "source_manifest_hash": "man_sales_001",
    "candidates": [
        {
            "candidate_id": "jc_sales_user",
            "left_table": "orders",
            "right_table": "users",
            "left_key": "user_id",
            "right_key": "user_id",
            "left_key_normalized": "user_id",
            "right_key_normalized": "user_id",
            "join_type": "INNER",
            "evidence": {
                "evidence_id": "ev_fk_001",
                "level": "STRONG",
                "action": "AUTO_ADOPT",
                "left_table": "orders",
                "right_table": "users",
                "left_key_raw": "user_id",
                "right_key_raw": "user_id",
                "left_key_normalized": "user_id",
                "right_key_normalized": "user_id",
                "evidence_checks": [
                    "field_name_match: MATCH",
                    "type_match: MATCH (bigint=bigint)",
                    "fk_constraint: MATCH (orders.user_id→users.user_id)",
                ],
                "detail": "显式外键约束+字段名精确匹配+类型兼容→STRONG自动采纳",
            },
        }
    ],
    "multi_table": True,
}

# ── SqlBuildPlan 样本（供 SqlProgram Planner 使用）──

SQL_BUILD_PLAN_DAU = {
    "plan_id": "plan_dau_001",
    "spec_hash": "abc123dau001",
    "steps": [
        {
            "step_type": "scan",
            "step_id": "scan_logs",
            "table_ref": "logs",
            "required_columns": [
                {"table_ref": "logs", "column_name": "user_id", "normalized_name": "user_id"},
                {"table_ref": "logs", "column_name": "action_date", "normalized_name": "action_date"},
            ],
        },
        {
            "step_type": "aggregate",
            "step_id": "agg_1",
            "group_keys": [
                {"table_ref": "logs", "column_name": "action_date", "normalized_name": "action_date"}
            ],
            "metrics": [
                {"aggregation": "COUNT_DISTINCT", "input_column": "user_id", "alias": "dau"}
            ],
        },
        {"step_type": "limit", "step_id": "limit_1000", "limit": 1000},
    ],
    "multi_table": False,
}

SQL_BUILD_PLAN_SALES = {
    "plan_id": "plan_sales_002",
    "spec_hash": "xyz789sales02",
    "steps": [
        {
            "step_type": "scan",
            "step_id": "scan_orders",
            "table_ref": "orders",
            "required_columns": [
                {"table_ref": "orders", "column_name": "user_id", "normalized_name": "user_id"},
                {"table_ref": "orders", "column_name": "amount", "normalized_name": "amount"},
            ],
        },
        {
            "step_type": "scan",
            "step_id": "scan_users",
            "table_ref": "users",
            "required_columns": [
                {"table_ref": "users", "column_name": "user_id", "normalized_name": "user_id"},
                {"table_ref": "users", "column_name": "user_name", "normalized_name": "user_name"},
            ],
        },
        {
            "step_type": "join",
            "step_id": "join_orders_users",
            "right_table_ref": "users",
            "join_type": "INNER",
            "join_keys": [
                [
                    {"table_ref": "orders", "column_name": "user_id", "normalized_name": "user_id"},
                    {"table_ref": "users", "column_name": "user_id", "normalized_name": "user_id"},
                ]
            ],
            "relationship_ref": "jc_sales_user",
        },
        {
            "step_type": "aggregate",
            "step_id": "agg_1",
            "group_keys": [
                {"table_ref": "users", "column_name": "user_name", "normalized_name": "user_name"}
            ],
            "metrics": [
                {"aggregation": "SUM", "input_column": "amount", "alias": "total_amount"}
            ],
        },
        {"step_type": "limit", "step_id": "limit_100", "limit": 100},
    ],
    "multi_table": True,
}


# ════════════════════════════════════════════
# 回归用例定义——按 task 组织
# ════════════════════════════════════════════

REGRESSION_CASES: dict[str, list[dict]] = {
    "developer_spec_parser": [
        {
            "case_id": "real_dev_spec_dau",
            "description": "真实 LLM 解析 DAU DeveloperSpec YAML",
            "system_message_override": None,  # 使用 Prompt 模板的 body
            "user_message": (
                "请解析以下 DeveloperSpec YAML 声明和 SourceManifest，输出严格的 ParsedDeveloperSpec JSON。\n\n"
                "## DeveloperSpec YAML\n\n```yaml\n"
                + DEV_SPEC_DAU_YAML
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_DAU_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
        {
            "case_id": "real_dev_spec_sales",
            "description": "真实 LLM 解析销售汇总 DeveloperSpec YAML（含 Join 声明）",
            "system_message_override": None,
            "user_message": (
                "请解析以下 DeveloperSpec YAML 声明和 SourceManifest，输出严格的 ParsedDeveloperSpec JSON。\n\n"
                "## DeveloperSpec YAML\n\n```yaml\n"
                + DEV_SPEC_SALES_YAML
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_SALES_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
    ],
    "relationship_planner": [
        {
            "case_id": "real_rel_single_table",
            "description": "真实 LLM 推断单表场景——无 Join 候选",
            "system_message_override": None,
            "user_message": (
                "请基于以下 ParsedDeveloperSpec 和 SourceManifest 推理 Join 关系候选。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_DAU, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_DAU_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
        {
            "case_id": "real_rel_two_table_fk",
            "description": "真实 LLM 推断两表 Join——外键 + 命名匹配",
            "system_message_override": None,
            "user_message": (
                "请基于以下 ParsedDeveloperSpec 和 SourceManifest 推理 Join 关系候选。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_SALES, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_SALES_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
    ],
    "sql_build_planner": [
        {
            "case_id": "real_plan_dau_single",
            "description": "真实 LLM 生成 DAU 单表 SqlBuildPlan",
            "system_message_override": None,
            "user_message": (
                "请基于以下输入生成严格的 SqlBuildPlan JSON。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_DAU, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_DAU_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## RelationshipHypothesis\n\n```json\n"
                + json.dumps(REL_HYP_DAU, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
        {
            "case_id": "real_plan_sales_join",
            "description": "真实 LLM 生成销售汇总两表 Join SqlBuildPlan",
            "system_message_override": None,
            "user_message": (
                "请基于以下输入生成严格的 SqlBuildPlan JSON。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_SALES, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_SALES_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## RelationshipHypothesis\n\n```json\n"
                + json.dumps(REL_HYP_SALES, ensure_ascii=False, indent=2)
                + "\n```"
            ),
        },
    ],
    "sql_program_planner": [
        {
            "case_id": "real_prog_dau_standalone",
            "description": "真实 LLM 编排 DAU 单步 SqlProgram（STANDALONE）",
            "system_message_override": None,
            "user_message": (
                "请基于以下输入编排 SqlProgram JSON。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_DAU, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_DAU_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## RelationshipHypothesis\n\n```json\n"
                + json.dumps(REL_HYP_DAU, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SqlBuildPlans\n\n```json\n["
                + json.dumps(SQL_BUILD_PLAN_DAU, ensure_ascii=False, indent=2)
                + "]\n```"
            ),
        },
        {
            "case_id": "real_prog_sales_standalone",
            "description": "真实 LLM 编排销售汇总单步 SqlProgram（STANDALONE）",
            "system_message_override": None,
            "user_message": (
                "请基于以下输入编排 SqlProgram JSON。\n\n"
                "## ParsedDeveloperSpec\n\n```json\n"
                + json.dumps(PARSED_SPEC_SALES, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SourceManifest\n\n```json\n"
                + json.dumps(DEV_SPEC_SALES_MANIFEST, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## RelationshipHypothesis\n\n```json\n"
                + json.dumps(REL_HYP_SALES, ensure_ascii=False, indent=2)
                + "\n```\n\n"
                "## SqlBuildPlans\n\n```json\n["
                + json.dumps(SQL_BUILD_PLAN_SALES, ensure_ascii=False, indent=2)
                + "]\n```"
            ),
        },
    ],
}


# ════════════════════════════════════════════
# 主回归执行逻辑
# ════════════════════════════════════════════

def run_real_llm_regression(
    model: str = "",
    temperature: float = 0.0,
    verbose: bool = True,
) -> dict:
    """执行真实 LLM 回归验证——覆盖全部 4 个任务的 Prompt 模板。

    Args:
        model: LLM 模型标识——空字符串表示使用默认模型 (deepseek-v4-pro)
        temperature: LLM 温度
        verbose: 是否输出详细日志

    Returns:
        汇总 dict——含 total/passed/failed/errors 及每用例明细
    """
    # ── 初始化 ──
    load_dotenv()
    adapter = AnthropicAdapter(model=model or None)
    prompt_manager = PromptManager()

    results: list[dict] = []
    total_start = time.time()

    for task_name, cases in REGRESSION_CASES.items():
        _log(f"\n{'='*60}", verbose)
        _log(f"  Task: {task_name}", verbose)
        _log(f"{'='*60}", verbose)

        # 加载 Prompt 模板——获取 system_message 和 Schema 绑定
        template = prompt_manager.get_prompt(task_name, "v001")
        schema_binding = template.schema_binding

        for case in cases:
            case_start = time.time()
            system_msg = case.get("system_message_override") or template.system_message
            user_msg = case["user_message"]

            _log(f"\n  [{case['case_id']}] {case['description']}", verbose)

            try:
                # 调用真实 LLM
                raw_output = adapter.invoke(
                    system_message=system_msg,
                    user_message=user_msg,
                    json_schema=schema_binding.json_schema,
                    model=model,
                    temperature=temperature,
                )

                # 提取 token 用量（必须在剥离 _ 前缀字段前提取）
                token_usage = raw_output.get("_token_usage", {})

                # Schema 校验（内部剥离 _ 前缀字段）
                validated, errors = _validate_against_schema_fast(
                    raw_output=raw_output,
                    schema_binding=schema_binding,
                )

                latency_ms = int((time.time() - case_start) * 1000)

                if validated is not None and not errors:
                    _log(f"    [PASS] ({latency_ms}ms, {token_usage.get('total_tokens', '?')} tokens)", verbose)
                    results.append({
                        "case_id": case["case_id"],
                        "task": task_name,
                        "status": "passed",
                        "latency_ms": latency_ms,
                        "token_usage": token_usage,
                    })
                else:
                    _log(f"    [FAIL] Schema validation failed ({latency_ms}ms)", verbose)
                    for err in errors[:5]:  # 前 5 条错误
                        _log(f"       {err}", verbose)
                    results.append({
                        "case_id": case["case_id"],
                        "task": task_name,
                        "status": "failed",
                        "latency_ms": latency_ms,
                        "token_usage": token_usage,
                        "errors": errors,
                    })

            except Exception as e:
                latency_ms = int((time.time() - case_start) * 1000)
                _log(f"    [ERROR] {type(e).__name__}: {e}", verbose)
                results.append({
                    "case_id": case["case_id"],
                    "task": task_name,
                    "status": "error",
                    "latency_ms": latency_ms,
                    "error": f"{type(e).__name__}: {e}",
                })

    # ── 汇总 ──
    total_elapsed = time.time() - total_start
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "passed")
    failed = sum(1 for r in results if r["status"] == "failed")
    errors = sum(1 for r in results if r["status"] == "error")
    total_tokens = sum(r.get("token_usage", {}).get("total_tokens", 0) for r in results)

    summary = {
        "total": total,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "pass_rate": passed / total if total > 0 else 0.0,
        "total_tokens": total_tokens,
        "total_latency_ms": int(total_elapsed * 1000),
        "results": results,
    }

    _log(f"\n{'='*60}", verbose)
    _log(f"  回归汇总", verbose)
    _log(f"{'='*60}", verbose)
    _log(f"  总计: {total} 用例", verbose)
    _log(f"  通过: {passed} [PASS]", verbose)
    _log(f"  失败: {failed} [FAIL]", verbose)
    _log(f"  异常: {errors} [ERROR]", verbose)
    _log(f"  通过率: {summary['pass_rate']:.1%}", verbose)
    _log(f"  总 Token: {total_tokens:,}", verbose)
    _log(f"  总耗时: {total_elapsed:.1f}s", verbose)

    # 按 task 统计通过率
    for task_name in REGRESSION_CASES:
        task_results = [r for r in results if r["task"] == task_name]
        task_passed = sum(1 for r in task_results if r["status"] == "passed")
        _log(f"  {task_name}: {task_passed}/{len(task_results)}", verbose)

    return summary


def _validate_against_schema_fast(raw_output: dict, schema_binding) -> tuple:
    """Schema 校验的轻量封装——校验前剥离 _token_usage 内部字段。"""
    from pydantic import ValidationError

    # 剥离内部字段——_token_usage 被 Pydantic extra="forbid" 拒绝
    clean_output = {k: v for k, v in raw_output.items() if not k.startswith("_")}

    model_cls = _import_pydantic_model(schema_binding.pydantic_model_path)
    try:
        validated = model_cls.model_validate(clean_output)
        return validated, []
    except ValidationError as e:
        errors = _format_validation_errors(e)
        return None, errors
    except Exception as e:
        return None, [f"Schema 校验异常：{e}"]


def _log(msg: str, verbose: bool) -> None:
    """条件日志输出。"""
    if verbose:
        print(msg, flush=True)


# ════════════════════════════════════════════
# CLI 入口
# ════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="真实 LLM 回归验证——验证 4 个 Prompt 模板的结构化输出约束力"
    )
    parser.add_argument(
        "--model",
        default="",
        help="LLM 模型标识（默认：deepseek-v4-pro）",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="LLM 温度（默认：0.0 = 确定性输出）",
    )
    parser.add_argument(
        "--task",
        default="",
        help="仅运行指定 task（如 developer_spec_parser）——留空运行全部",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="以 JSON 格式输出结果",
    )
    args = parser.parse_args()

    # 如果指定了 --task，过滤用例
    if args.task:
        if args.task not in REGRESSION_CASES:
            print(f"未知 task：'{args.task}'——已知：{sorted(REGRESSION_CASES.keys())}")
            sys.exit(1)
        original = dict(REGRESSION_CASES)
        REGRESSION_CASES.clear()
        REGRESSION_CASES[args.task] = original[args.task]

    summary = run_real_llm_regression(
        model=args.model,
        temperature=args.temperature,
        verbose=not args.output_json,
    )

    if args.output_json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    # 退出码：全部通过 → 0，否则 → 1
    sys.exit(0 if summary["failed"] == 0 and summary["errors"] == 0 else 1)
