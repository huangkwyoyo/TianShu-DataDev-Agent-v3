"""TianShu DataDev Agent CLI——内部命令行交互入口。

子命令:
    tianshu parse <file>          解析 DeveloperSpec 文件
    tianshu run <file>            全流程执行（含打包）
    tianshu package <request_id>  获取 ReviewPackage 信息

所有命令输出 JSON 格式：成功 → stdout，失败 → stderr。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tianshu_datadev.api.pipeline import Pipeline


def _read_file(filepath: str) -> str:
    """读取文本文件内容——文件不存在或编码错误时系统性报错。

    Args:
        filepath: 文件路径

    Returns:
        文件内容字符串

    Raises:
        SystemExit: 文件不可读时退出
    """
    path = Path(filepath)
    if not path.exists():
        _fail("FILE_NOT_FOUND", f"文件不存在: {filepath}", "file")
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        _fail("FILE_ENCODING_ERROR", f"文件编码错误（需要 UTF-8）: {filepath}", "file")


def _parse_table_paths(args: argparse.Namespace) -> dict[str, str] | None:
    """从命令行参数解析 table_paths 映射。

    支持两种来源:
      --table-path KEY=VALUE  单条映射（可多次指定）
      --table-paths FILE      JSON 文件 {"table_name": "/path/to.csv"}

    Args:
        args: argparse 解析后的命名空间

    Returns:
        table_paths dict，无映射时返回 None
    """
    table_paths: dict[str, str] = {}

    # 从 --table-path 参数解析
    for entry in getattr(args, "table_path", []) or []:
        if "=" in entry:
            key, value = entry.split("=", 1)
            table_paths[key.strip()] = value.strip()
        else:
            _fail("ARG_ERROR", f"无效的 --table-path 格式（需要 KEY=VALUE）: {entry}", "table_path")

    # 从 --table-paths JSON 文件解析
    json_file = getattr(args, "table_paths_file", None)
    if json_file:
        try:
            loaded = json.loads(Path(json_file).read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                table_paths.update(loaded)
            else:
                _fail("ARG_ERROR", f"--table-paths 文件内容必须是 JSON 对象: {json_file}", "table_paths_file")
        except json.JSONDecodeError as e:
            _fail("ARG_ERROR", f"--table-paths JSON 解析失败: {e}", "table_paths_file")

    return table_paths if table_paths else None


def _output_json(data: dict) -> None:
    """输出 JSON 到 stdout——成功路径。"""
    print(json.dumps(data, ensure_ascii=False, default=str))


def _fail(error_code: str, message: str, field_ref: str | None = None) -> None:
    """输出错误 JSON 到 stderr 并退出——失败路径。

    Args:
        error_code: 错误码
        message: 错误描述
        field_ref: 字段引用（可选）
    """
    error = {
        "command": _current_command,
        "status": "error",
        "error": {"error_code": error_code, "message": message, "field_ref": field_ref},
    }
    print(json.dumps(error, ensure_ascii=False, default=str), file=sys.stderr)
    sys.exit(1)


# 当前执行的子命令名（用于错误输出）
_current_command: str = ""


def handle_parse(args: argparse.Namespace) -> None:
    """处理 parse 子命令——解析 DeveloperSpec 文件为结构化摘要。

    输出 SpecParseResponse JSON 到 stdout。
    """
    global _current_command
    _current_command = "parse"

    markdown_text = _read_file(args.file)
    pipeline = Pipeline()
    result = pipeline.parse_only(markdown_text)
    if "pipeline_error" in result:
        pe = result["pipeline_error"]
        _fail(f"PIPELINE_{pe['stage'].upper()}_ERROR", pe["error_message"])
    _output_json({"command": "parse", "status": "success", "result": result})


def handle_run(args: argparse.Namespace) -> None:
    """处理 run 子命令——全流程执行+打包。

    输出 RunAllResponse JSON 到 stdout。
    """
    global _current_command
    _current_command = "run"

    markdown_text = _read_file(args.file)
    table_paths = _parse_table_paths(args)
    pipeline = Pipeline()
    result = pipeline.run_all(markdown_text, table_paths=table_paths)
    if "pipeline_error" in result:
        pe = result["pipeline_error"]
        _fail(f"PIPELINE_{pe['stage'].upper()}_ERROR", pe["error_message"])
    _output_json({"command": "run", "status": "success", "result": result})


def handle_package(args: argparse.Namespace) -> None:
    """处理 package 子命令——获取 ReviewPackage 信息。

    输出 PackageResponse JSON 到 stdout。
    """
    global _current_command
    _current_command = "package"

    pipeline = Pipeline()
    result = pipeline.get_package(args.request_id)
    if result is None:
        _fail("NOT_FOUND", f"request_id '{args.request_id}' 对应的 package 不存在", "request_id")
    _output_json({"command": "package", "status": "success", "result": result})


def main(argv: list[str] | None = None) -> None:
    """CLI 主入口——解析参数并分发子命令。

    Args:
        argv: 命令行参数列表（默认 sys.argv[1:]）
    """
    # 运行时临时目录收口到 D 盘（上限 10GB，超过自动清理旧文件）
    from tianshu_datadev.temp_manager import ensure_temp_dir
    ensure_temp_dir()

    parser = argparse.ArgumentParser(
        prog="tianshu",
        description="TianShu DataDev Agent — 内部交互验证 CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="可用子命令")

    # ── tianshu parse ──
    parse_parser = subparsers.add_parser("parse", help="解析 DeveloperSpec 文件")
    parse_parser.add_argument("file", help="DeveloperSpec Markdown 文件路径")
    parse_parser.set_defaults(handler=handle_parse)

    # ── tianshu run ──
    run_parser = subparsers.add_parser("run", help="全流程执行（含打包）")
    run_parser.add_argument("file", help="DeveloperSpec Markdown 文件路径")
    run_parser.add_argument("--table-path", action="append", default=[], metavar="KEY=VALUE",
                            help="单表路径映射（可多次指定）")
    run_parser.add_argument("--table-paths", dest="table_paths_file", default=None, metavar="FILE",
                            help="JSON 文件路径，格式 {\"table_name\": \"/path/to.csv\"}")
    run_parser.set_defaults(handler=handle_run)

    # ── tianshu package ──
    pkg_parser = subparsers.add_parser("package", help="获取 ReviewPackage 信息")
    pkg_parser.add_argument("request_id", help="请求唯一标识")
    pkg_parser.set_defaults(handler=handle_package)

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.handler(args)


if __name__ == "__main__":
    main()
