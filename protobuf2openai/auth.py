from __future__ import annotations

import os
from typing import Optional
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse


class BearerTokenAuth:
    """Bearer Token 认证中间件"""

    def __init__(self, expected_token: Optional[str] = None):
        """
        初始化认证中间件

        Args:
            expected_token: 预期的Bearer token，如果为None则从环境变量读取
        """
        self.expected_token = expected_token or os.getenv("API_TOKEN")

        # 如果没有设置token，强制要求设置
        if not self.expected_token:
            print("❌ 错误: 未设置 API_TOKEN 环境变量，API将被锁定")
            print("   请在 .env 文件中设置: API_TOKEN=001")
            print("   或设置环境变量: export API_TOKEN=001")
            self.expected_token = None  # 强制为None，确保认证失败

    def authenticate(self, authorization: Optional[str]) -> bool:
        """
        验证Bearer token

        Args:
            authorization: Authorization头的值

        Returns:
            bool: 验证是否通过
        """
        # 如果没有设置预期的token，拒绝所有请求
        if not self.expected_token:
            return False

        if not authorization:
            return False

        # 检查是否是Bearer token格式
        if not authorization.startswith("Bearer "):
            return False

        token = authorization[7:]  # 移除 "Bearer " 前缀
        return token == self.expected_token

    def get_auth_error_response(self) -> JSONResponse:
        """获取认证失败的响应"""
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "error": {
                    "message": "Invalid API key provided",
                    "type": "authentication_error",
                    "code": "invalid_api_key"
                }
            },
            headers={"WWW-Authenticate": "Bearer"}
        )


# 全局认证实例
auth = BearerTokenAuth()


async def authenticate_request(request: Request) -> None:
    """
    FastAPI中间件函数 - 验证请求的认证

    支持两种认证方式:
      - OpenAI:    Authorization: Bearer xxx
      - Anthropic: x-api-key: xxx

    Args:
        request: FastAPI请求对象

    Raises:
        HTTPException: 认证失败时抛出
    """
    # 优先检查 Authorization: Bearer xxx (OpenAI 格式)
    authorization = request.headers.get("authorization") or request.headers.get("Authorization")

    if not authorization:
        # 回退到 x-api-key (Anthropic 格式)
        api_key = request.headers.get("x-api-key")
        if api_key:
            authorization = f"Bearer {api_key}"

    # 验证token
    if not auth.authenticate(authorization):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key provided",
            headers={"WWW-Authenticate": "Bearer"}
        )


def require_auth(func):
    """
    装饰器：为路由函数添加认证要求

    使用示例：
    @router.post("/v1/chat/completions")
    @require_auth
    async def chat_completions(...):
        ...
    """
    async def wrapper(*args, **kwargs):
        # 获取request对象（通常是第一个参数或在kwargs中）
        request = None
        if args and hasattr(args[0], 'headers'):
            request = args[0]
        elif 'request' in kwargs:
            request = kwargs['request']

        if request:
            await authenticate_request(request)

        return await func(*args, **kwargs)

    return wrapper


async def verify_admin_token(request: Request) -> None:
    """
    FastAPI Depends 依赖：校验 API_TOKEN，用于保护管理接口。

    使用示例：
        @app.get("/api/accounts", dependencies=[Depends(verify_admin_token)])
        async def list_accounts(): ...
    """
    authorization = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth.authenticate(authorization):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin API requires valid API_TOKEN",
            headers={"WWW-Authenticate": "Bearer"},
        )