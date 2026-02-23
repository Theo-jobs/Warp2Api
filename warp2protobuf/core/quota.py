#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp 额度查询模块

通过 GraphQL GetRequestLimitInfo 查询精确获取账号的剩余 AI 请求额度。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from ..config.settings import (
    CLIENT_VERSION,
    OS_CATEGORY,
    OS_NAME,
    OS_VERSION,
    TLS_VERIFY,
    proxy_for_url,
)
from .logging import logger

# GraphQL 端点（优先走 rustls proxy 防 TLS 指纹限流）
_RUSTLS_PROXY_PORT = "28887"
_GQL_URL_PROXY = f"http://127.0.0.1:{_RUSTLS_PROXY_PORT}/graphql/v2?op=GetRequestLimitInfo"
_GQL_URL_DIRECT = "https://app.warp.dev/graphql/v2?op=GetRequestLimitInfo"

_QUERY = (
    "query GetRequestLimitInfo($requestContext: RequestContext!) {\n"
    "  user(requestContext: $requestContext) {\n"
    "    __typename\n"
    "    ... on UserOutput {\n"
    "      user {\n"
    "        requestLimitInfo {\n"
    "          requestLimit\n"
    "          requestsUsedSinceLastRefresh\n"
    "        }\n"
    "      }\n"
    "    }\n"
    "    ... on UserFacingError {\n"
    "      error { __typename message }\n"
    "    }\n"
    "  }\n"
    "}\n"
)


def _gql_headers(access_token: str) -> Dict[str, str]:
    return {
        "content-type": "application/json",
        "accept-encoding": "gzip, br",
        "authorization": f"Bearer {access_token}",
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }


def _build_body() -> Dict[str, Any]:
    return {
        "operationName": "GetRequestLimitInfo",
        "query": _QUERY,
        "variables": {
            "requestContext": {
                "clientContext": {"version": CLIENT_VERSION},
                "osContext": {
                    "category": OS_CATEGORY,
                    "linuxKernelVersion": None,
                    "name": OS_NAME,
                    "version": OS_VERSION,
                },
            },
        },
    }


async def get_request_limit_info(access_token: str) -> Dict[str, Any]:
    """调用 Warp GraphQL GetRequestLimitInfo 获取精确额度信息。

    使用 Account Pro v3.1.0 的 user(requestContext) query。

    Args:
        access_token: 有效的 Warp access_token (Bearer JWT)

    Returns:
        dict with keys:
            request_limit: int — 总额度
            used: int — 已用额度
            remaining: int — 剩余额度
            raw: dict — 原始 GraphQL 响应

    Raises:
        RuntimeError: GraphQL 请求失败或返回错误
    """
    headers = _gql_headers(access_token)
    body = _build_body()

    # 策略：
    #   1) 本地 rustls proxy（127.0.0.1:28887）
    #   2) 直连 Warp（若配置了 WARP_PROXY_URL，则先尝试经代理）
    #   3) 直连 Warp（不走代理，兜底）
    direct_proxy = proxy_for_url(_GQL_URL_DIRECT)
    endpoint_attempts: list[tuple[str, str, str | None]] = [
        ("proxy", _GQL_URL_PROXY, None),
    ]
    if direct_proxy:
        endpoint_attempts.append(("direct-via-proxy", _GQL_URL_DIRECT, direct_proxy))
        endpoint_attempts.append(("direct-no-proxy", _GQL_URL_DIRECT, None))
    else:
        endpoint_attempts.append(("direct", _GQL_URL_DIRECT, None))

    resp: Optional[httpx.Response] = None
    last_error: Optional[str] = None

    for route_label, url, route_proxy in endpoint_attempts:
        try:
            client_kwargs: dict[str, Any] = {
                "timeout": httpx.Timeout(15.0),
                "verify": TLS_VERIFY,
                "trust_env": False,
            }
            if route_proxy:
                client_kwargs["proxy"] = route_proxy

            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.post(url, headers=headers, json=body)

            if resp.status_code == 200:
                break

            last_error = f"{route_label}: HTTP {resp.status_code}: {(resp.text or '')[:200]}"
            logger.warning("[Quota] %s (%s) → %s", url[:50], route_label, last_error)
        except Exception as e:
            error_text = str(e)[:200]
            last_error = f"{route_label}: {error_text}"
            logger.warning("[Quota] %s (%s) → %s", url[:50], route_label, error_text)

    if resp is None or resp.status_code != 200:
        raise RuntimeError(f"GetRequestLimitInfo failed: {last_error}")

    try:
        data = resp.json()
    except ValueError as e:
        preview = (resp.text or "")[:200]
        raise RuntimeError(f"GetRequestLimitInfo invalid JSON: {preview}") from e

    if not isinstance(data, dict):
        raise RuntimeError(f"GetRequestLimitInfo invalid response type: {type(data).__name__}")

    gql_errors = data.get("errors")
    if gql_errors:
        raise RuntimeError(f"GetRequestLimitInfo GraphQL errors: {str(gql_errors)[:300]}")

    data_node = data.get("data")
    if not isinstance(data_node, dict) or not data_node:
        raise RuntimeError(f"GetRequestLimitInfo missing or empty data: {str(data)[:300]}")

    user_result = data_node.get("user")
    if not isinstance(user_result, dict) or not user_result:
        raise RuntimeError(f"GetRequestLimitInfo missing or invalid data.user: {str(data_node)[:300]}")

    typename = user_result.get("__typename", "")

    if typename == "UserFacingError":
        error_msg = user_result.get("error", {}).get("message", str(user_result)[:200])
        raise RuntimeError(f"GetRequestLimitInfo error: {error_msg}")

    if typename != "UserOutput":
        raise RuntimeError(f"GetRequestLimitInfo unexpected typename: {typename or 'empty'}")

    limit_info = user_result.get("user", {}).get("requestLimitInfo")
    if not isinstance(limit_info, dict):
        raise RuntimeError("GetRequestLimitInfo missing requestLimitInfo")

    if "requestLimit" not in limit_info or "requestsUsedSinceLastRefresh" not in limit_info:
        raise RuntimeError("GetRequestLimitInfo incomplete requestLimitInfo")

    try:
        request_limit = int(limit_info["requestLimit"])
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            "GetRequestLimitInfo invalid requestLimit value: "
            f"{limit_info.get('requestLimit')!r}"
        ) from e

    try:
        used = int(limit_info["requestsUsedSinceLastRefresh"])
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            "GetRequestLimitInfo invalid requestsUsedSinceLastRefresh value: "
            f"{limit_info.get('requestsUsedSinceLastRefresh')!r}"
        ) from e

    logger.info(
        "[Quota] limit=%d used=%d remaining=%d",
        request_limit, used, request_limit - used,
    )

    return {
        "request_limit": request_limit,
        "used": used,
        "remaining": request_limit - used,
        "raw": limit_info,
    }
