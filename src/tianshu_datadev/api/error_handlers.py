"""FastAPI 异常处理器——ParseError/ValidationError → 结构化 ErrorDetail 响应。

所有错误返回统一格式的 ErrorDetail JSON，不泄露内部堆栈。
"""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from tianshu_datadev.developer_spec.parser import ParseError


async def parse_error_handler(request: Request, exc: ParseError) -> JSONResponse:
    """处理 ParseError——返回 422 + 结构化错误码。

    ParseError 来自 DeveloperSpecParser，包含明确的 error_code 和 message。
    """
    return JSONResponse(
        status_code=422,
        content={
            "error_code": exc.error_code,
            "message": exc.message,
            "field_ref": exc.field_ref,
        },
    )


async def validation_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
    """处理 Pydantic ValidationError——返回 422 + 字段级错误描述。

    将 Pydantic 的验证错误转换为统一 ErrorDetail 格式。
    多个错误时取第一个错误的详情。
    """
    errors = exc.errors()
    if errors:
        first = errors[0]
        field_ref = ".".join(str(loc) for loc in first.get("loc", []))
        message = first.get("msg", str(exc))
    else:
        field_ref = None
        message = str(exc)

    return JSONResponse(
        status_code=422,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": message,
            "field_ref": field_ref,
        },
    )


async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """处理 ValueError——返回 422 + 结构化错误。

    Builder/Compiler 的 ValueError 统一转换为 ErrorDetail。
    """
    return JSONResponse(
        status_code=422,
        content={
            "error_code": "VALUE_ERROR",
            "message": str(exc),
            "field_ref": None,
        },
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """处理未知异常——返回 500 + 通用错误。

    不泄露内部堆栈信息。
    """
    return JSONResponse(
        status_code=500,
        content={
            "error_code": "INTERNAL_ERROR",
            "message": "内部处理错误",
            "field_ref": None,
        },
    )


def register_error_handlers(app) -> None:
    """在 FastAPI app 上注册所有异常处理器。

    Args:
        app: FastAPI 应用实例
    """
    app.add_exception_handler(ParseError, parse_error_handler)
    app.add_exception_handler(ValidationError, validation_error_handler)
    app.add_exception_handler(ValueError, value_error_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
