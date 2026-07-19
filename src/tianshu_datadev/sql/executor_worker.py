"""DuckDB 隔离 Worker，只通过 stdin/stdout 交换结构化 JSON。"""

from __future__ import annotations

import json
import sys

from tianshu_datadev.spark.cdp_spec import CreDigestSpec
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import CompiledSql, ProgramCompiledSql


def _build_executor(config: dict) -> DuckDBExecutor:
    """从父进程已校验的配置创建 Worker 内执行器。"""
    return DuckDBExecutor(**config, _worker_mode=True)


def main() -> int:
    """执行单个请求并输出唯一 JSON 响应。"""
    try:
        request = json.loads(sys.stdin.read())
        mode = request["mode"]
        executor = _build_executor(request["config"])

        if mode == "single":
            trace, summary = executor.execute(
                CompiledSql.model_validate(request["compiled"]),
            )
            payload = {
                "trace": trace.model_dump(mode="json"),
                "summary": summary.model_dump(mode="json"),
            }
        elif mode == "program":
            result = executor.execute_program(
                ProgramCompiledSql.model_validate(request["compiled"]),
            )
            payload = result.model_dump(mode="json")
        elif mode == "cdp":
            envelope = executor.execute_with_cdp(
                CompiledSql.model_validate(request["compiled"]),
                CreDigestSpec.model_validate(request["spec"]),
                request["snapshot_id"],
            )
            payload = envelope.model_dump(mode="json")
        elif mode == "snapshot":
            manifest = executor.materialize_snapshot(
                output_dir=request["output_dir"],
                contract_hash=request["contract_hash"],
                source_tables=request["source_tables"],
                joins=request["joins"],
                table_aliases=request["table_aliases"],
                table_role_aliases=request.get("table_role_aliases"),
                sampling=request["sampling"],
            )
            payload = manifest.model_dump(mode="json")
        else:
            raise ValueError(f"未知 Worker 模式：{mode}")

        sys.stdout.write(json.dumps({"ok": True, "payload": payload}, ensure_ascii=False))
        return 0
    except Exception as exc:
        sys.stdout.write(
            json.dumps(
                {"ok": False, "error": f"{type(exc).__name__}: {exc}"},
                ensure_ascii=False,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
