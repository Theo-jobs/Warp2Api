#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp 账号注册模块

正确流程：
1. CreateAnonymousUser (Warp GraphQL) → 在 Warp 后端创建用户 + 获取 Firebase custom token
2. signInWithCustomToken (Firebase) → 换取 refreshToken
3. accounts:update (Firebase) → 绑定 email/password（可选，使账号可用邮箱登录）

每个新账号自动获得 ~300 次 AI 请求额度。
"""
from __future__ import annotations

import random
import string
from dataclasses import dataclass
from typing import Optional

import httpx

from ..config.settings import (
    CLIENT_ID,
    CLIENT_VERSION,
    OS_CATEGORY,
    OS_NAME,
    OS_VERSION,
)
from .logging import logger

_FIREBASE_API_KEY = "AIzaSyBdy3O3S9hrdayLJxJ7mriBR4qgUaUygAs"
# GraphQL 走 rustls proxy 避免 TLS 指纹限流
_ANON_GQL_URL_DIRECT = "https://app.warp.dev/graphql/v2?op=CreateAnonymousUser"
_ANON_GQL_URL_PROXY = "http://127.0.0.1:28887/graphql/v2?op=CreateAnonymousUser"
_SIGNIN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={_FIREBASE_API_KEY}"
_UPDATE_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:update?key={_FIREBASE_API_KEY}"

_EMAIL_DOMAINS = [
    "icloud.com", "icloud.com", "icloud.com",
    "aol.com", "aol.com",
    "yahoo.com", "yahoo.com",
    "protonmail.com", "protonmail.com",
    "mail.ru", "hotmail.com", "outlook.com",
]

@dataclass(frozen=True)
class RegisteredAccount:
    """注册成功的账号信息"""
    email: str
    local_id: str
    id_token: str
    refresh_token: str


def _random_email() -> str:
    length = random.randint(6, 14)
    username = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return f"{username}@{random.choice(_EMAIL_DOMAINS)}"


def _random_password() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + "!@#$%", k=random.randint(12, 16)))


def _warp_headers() -> dict:
    return {
        "content-type": "application/json",
        "accept-encoding": "gzip, br",
        "x-warp-client-id": CLIENT_ID,
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }


async def _step1_create_anonymous_user() -> str:
    """Step 1: Warp GraphQL CreateAnonymousUser → idToken (走 proxy 防限流)"""
    query = (
        "mutation CreateAnonymousUser($input: CreateAnonymousUserInput!, $requestContext: RequestContext!) {\n"
        "  createAnonymousUser(input: $input, requestContext: $requestContext) {\n"
        "    __typename\n"
        "    ... on CreateAnonymousUserOutput { expiresAt anonymousUserType firebaseUid idToken isInviteValid }\n"
        "    ... on UserFacingError { error { __typename message } }\n"
        "  }\n"
        "}\n"
    )
    variables = {
        "input": {
            "anonymousUserType": "NATIVE_CLIENT_ANONYMOUS_USER_FEATURE_GATED",
            "expirationType": "NO_EXPIRATION",
            "referralCode": None,
        },
        "requestContext": {
            "clientContext": {"version": CLIENT_VERSION},
            "osContext": {
                "category": OS_CATEGORY,
                "linuxKernelVersion": None,
                "name": OS_NAME,
                "version": OS_VERSION,
            },
        },
    }
    body = {"query": query, "variables": variables, "operationName": "CreateAnonymousUser"}

    # 优先走 rustls proxy，失败则直连
    for url in [_ANON_GQL_URL_PROXY, _ANON_GQL_URL_DIRECT]:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                resp = await client.post(url, headers=_warp_headers(), json=body)
            if resp.status_code == 200:
                break
            logger.warning("[Register] Step1 %s → HTTP %d", url[:40], resp.status_code)
        except Exception as e:
            logger.warning("[Register] Step1 %s → %s", url[:40], str(e)[:100])
    else:
        raise RuntimeError(f"CreateAnonymousUser failed on all endpoints: HTTP {resp.status_code}")

    data = resp.json()
    result = data.get("data", {}).get("createAnonymousUser", {})
    id_token = result.get("idToken")
    if not id_token:
        error_msg = result.get("error", {}).get("message", str(data)[:200])
        raise RuntimeError(f"CreateAnonymousUser no idToken: {error_msg}")

    logger.info("[Register] Step1 OK: uid=%s", result.get("firebaseUid", "?")[:12])
    return id_token


async def _step2_signin_custom_token(id_token: str) -> tuple[str, str, str]:
    """Step 2: signInWithCustomToken → (session_id_token, refresh_token, local_id)"""
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "accept-encoding": "gzip, br",
        "x-warp-client-id": CLIENT_ID,
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }
    form = {"returnSecureToken": "true", "token": id_token}

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        resp = await client.post(_SIGNIN_URL, headers=headers, data=form)

    if resp.status_code != 200:
        raise RuntimeError(f"signInWithCustomToken HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    session_token = data.get("idToken", "")
    refresh_token = data.get("refreshToken", "")
    local_id = data.get("localId", "")

    # localId 可能为空，从 idToken JWT 解析 sub 作为 fallback
    if not local_id and session_token:
        import base64, json as _json
        try:
            parts = session_token.split(".")
            if len(parts) == 3:
                payload_b64 = parts[1]
                padding = 4 - len(payload_b64) % 4
                if padding != 4:
                    payload_b64 += "=" * padding
                jwt_payload = _json.loads(base64.urlsafe_b64decode(payload_b64))
                local_id = jwt_payload.get("sub", "") or jwt_payload.get("user_id", "")
        except Exception:
            pass

    if not refresh_token:
        raise RuntimeError("signInWithCustomToken missing refreshToken")

    logger.info("[Register] Step2 OK: localId=%s", local_id[:12])
    return session_token, refresh_token, local_id


async def _step3_bind_email(session_token: str, email: str, password: str) -> tuple[str, str]:
    """Step 3: accounts:update → 绑定 email/password，返回 (new_id_token, new_refresh_token)"""
    payload = {
        "idToken": session_token,
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        resp = await client.post(_UPDATE_URL, json=payload)

    if resp.status_code != 200:
        logger.warning("[Register] Step3 bind email failed: HTTP %d %s", resp.status_code, resp.text[:200])
        return "", ""

    data = resp.json()
    logger.info("[Register] Step3 OK: email=%s bound", email)
    return data.get("idToken", ""), data.get("refreshToken", "")


async def register_account(
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> RegisteredAccount:
    """
    注册一个新的 Warp 账号

    流程：CreateAnonymousUser → signInWithCustomToken → 绑定 email/password

    Args:
        email: 邮箱地址，为空则随机生成
        password: 密码，为空则随机生成

    Returns:
        RegisteredAccount 包含完整凭证
    """
    email = email or _random_email()
    password = password or _random_password()

    # Step 1: 在 Warp 创建匿名用户
    custom_token = await _step1_create_anonymous_user()

    # Step 2: 换取 Firebase session
    session_token, refresh_token, local_id = await _step2_signin_custom_token(custom_token)

    # Step 3: 绑定 email/password
    new_id_token, new_refresh = await _step3_bind_email(session_token, email, password)

    # 优先使用绑定后的 token
    final_id_token = new_id_token or session_token
    final_refresh = new_refresh or refresh_token

    logger.info("[Register] Account ready: email=%s local_id=%s", email, local_id[:12])

    return RegisteredAccount(
        email=email,
        local_id=local_id,
        id_token=final_id_token,
        refresh_token=final_refresh,
    )


async def batch_register(
    count: int = 5,
    delay_s: float = 2.0,
    default_quota: int = 300,
) -> list[dict]:
    """
    批量注册 Warp 账号

    Args:
        count: 注册数量
        delay_s: 每次注册间隔（秒），防限流
        default_quota: 默认额度

    Returns:
        注册结果列表
    """
    import asyncio

    results: list[dict] = []

    for i in range(count):
        try:
            acc = await register_account()
            results.append({
                "success": True,
                "account": {
                    "email": acc.email,
                    "local_id": acc.local_id,
                    "id_token": acc.id_token,
                    "refresh_token": acc.refresh_token,
                    "total_limit": default_quota,
                    "used_limit": 0,
                },
                "error": None,
            })
            logger.info("[Register] Batch %d/%d OK: %s", i + 1, count, acc.email)
        except Exception as e:
            error_msg = str(e)[:200]
            results.append({"success": False, "account": None, "error": error_msg})
            logger.warning("[Register] Batch %d/%d FAIL: %s", i + 1, count, error_msg)

        if i < count - 1:
            await asyncio.sleep(delay_s)

    return results
