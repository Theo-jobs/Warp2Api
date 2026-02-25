#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp 账号注册模块

正确流程：
1. CreateAnonymousUser (Warp GraphQL) → 在 Warp 后端创建用户 + 获取 Firebase custom token
2. signInWithCustomToken (Firebase) → 换取 refreshToken
3. accounts:update (Firebase) → 绑定 email/password（可选，默认跳过）
4. GetOrCreateUser (Warp GraphQL) → 在 Warp 后端激活用户（关键！缺少此步会 400）
5. proxy/token (Warp) → 用 refreshToken 换取 access_token（用于 AI 请求）

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
    FIREBASE_API_KEY,
    OS_CATEGORY,
    OS_NAME,
    OS_VERSION,
    PROXY_URL,
    REFRESH_URL,
    TLS_VERIFY,
    proxy_for_url,
)
from .logging import logger

# GraphQL 走 rustls proxy 避免 TLS 指纹限流
_ANON_GQL_URL_DIRECT = "https://app.warp.dev/graphql/v2?op=CreateAnonymousUser"
_ANON_GQL_URL_PROXY = "http://127.0.0.1:28887/graphql/v2?op=CreateAnonymousUser"
_SIGNIN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={FIREBASE_API_KEY}"
_UPDATE_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:update?key={FIREBASE_API_KEY}"
# GetOrCreateUser 走 rustls proxy 激活 Warp 用户（缺少此步新账号 AI 请求会 400）
_GETORCREATE_URL_DIRECT = "https://app.warp.dev/graphql/v2?op=GetOrCreateUser"
_GETORCREATE_URL_PROXY = "http://127.0.0.1:28887/graphql/v2?op=GetOrCreateUser"

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
    access_token: str = ""  # Warp access_token（用于 AI 请求）


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
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }


class _CurlResponse:
    """Minimal response object to match httpx interface for curl fallback."""
    def __init__(self, status_code: int, body: bytes):
        self.status_code = status_code
        self._body = body

    def json(self) -> dict:
        import json as _json
        return _json.loads(self._body)


async def _step1_via_curl(body: dict) -> _CurlResponse:
    """Fallback: 用 curl 走 HTTP 代理发 GraphQL（httpx 与 Stash CONNECT 不兼容）"""
    import asyncio
    import json as _json

    json_str = _json.dumps(body)
    headers = _warp_headers()
    cmd = [
        "curl", "-s", "-w", "\n%{http_code}",
        "-x", PROXY_URL,
        "--connect-timeout", "15",
        "-X", "POST",
        _ANON_GQL_URL_DIRECT,
    ]
    for k, v in headers.items():
        cmd.extend(["-H", f"{k}: {v}"])
    cmd.extend(["-d", json_str])

    if not TLS_VERIFY:
        cmd.append("-k")

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    output = stdout.decode("utf-8", errors="replace").strip()

    # 最后一行是 http_code
    lines = output.rsplit("\n", 1)
    if len(lines) == 2:
        body_str, code_str = lines
    else:
        body_str, code_str = output, "0"

    status_code = int(code_str) if code_str.isdigit() else 0
    logger.info("[Register] Step1 curl → HTTP %d (proxy=%s)", status_code, PROXY_URL)
    return _CurlResponse(status_code, body_str.encode("utf-8"))


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

    # 策略：rustls proxy → 直连 → curl 走 HTTP 代理（httpx 与 Stash CONNECT 不兼容）
    endpoints = [_ANON_GQL_URL_PROXY, _ANON_GQL_URL_DIRECT]

    resp = None
    for url in endpoints:
        try:
            client_kwargs: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
            _proxy = proxy_for_url(url)
            if _proxy:
                client_kwargs["proxy"] = _proxy
            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.post(url, headers=_warp_headers(), json=body)
            if resp.status_code == 200:
                break
            logger.warning("[Register] Step1 %s (proxy=%s) → HTTP %d", url[:40], _proxy or "direct", resp.status_code)
        except Exception as e:
            logger.warning("[Register] Step1 %s → %s", url[:40], str(e)[:100])

    # httpx 全部 429/失败时，尝试 curl 走 HTTP 代理（换 IP 绕限流）
    if (resp is None or resp.status_code != 200) and PROXY_URL:
        logger.info("[Register] Step1 httpx failed, trying curl via proxy %s", PROXY_URL)
        try:
            resp = await _step1_via_curl(body)
        except Exception as e:
            logger.warning("[Register] Step1 curl fallback → %s", str(e)[:150])

    if resp is None or resp.status_code != 200:
        code = resp.status_code if resp is not None else 0
        raise RuntimeError(f"CreateAnonymousUser failed on all endpoints: HTTP {code}")

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

    client_kwargs: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
    _proxy = proxy_for_url(_SIGNIN_URL)
    if _proxy:
        client_kwargs["proxy"] = _proxy
    async with httpx.AsyncClient(**client_kwargs) as client:
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


async def _step3_exchange_access_token(refresh_token: str) -> str:
    """Step 3: proxy/token → 用 refreshToken 换取 Warp access_token（关键步骤！）"""
    payload = f"grant_type=refresh_token&refresh_token={refresh_token}".encode("utf-8")
    headers = {
        "x-warp-client-id": CLIENT_ID,
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
        "content-type": "application/x-www-form-urlencoded",
        "accept": "*/*",
        "accept-encoding": "gzip, br",
        "content-length": str(len(payload)),
    }

    client_kwargs: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
    _proxy = proxy_for_url(REFRESH_URL)
    if _proxy:
        client_kwargs["proxy"] = _proxy

    async with httpx.AsyncClient(**client_kwargs) as client:
        resp = await client.post(REFRESH_URL, headers=headers, content=payload)

    if resp.status_code != 200:
        raise RuntimeError(f"proxy/token exchange failed: HTTP {resp.status_code} {resp.text[:200]}")

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(f"proxy/token response missing access_token: {token_data}")

    logger.info("[Register] Step3 OK: access_token obtained (len=%d)", len(access_token))
    return access_token


async def _step4_bind_email(session_token: str, email: str, password: str) -> tuple[str, str]:
    """Step 4 (可选): accounts:update → 绑定 email/password，返回 (new_id_token, new_refresh_token)"""
    payload = {
        "idToken": session_token,
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }

    try:
        client_kwargs: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
        _proxy = proxy_for_url(_UPDATE_URL)
        if _proxy:
            client_kwargs["proxy"] = _proxy
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.post(_UPDATE_URL, json=payload)

        if resp.status_code != 200:
            logger.warning("[Register] Step3 bind email failed: HTTP %d %s", resp.status_code, resp.text[:200])
            return "", ""

        data = resp.json()
        logger.info("[Register] Step3 OK: email=%s bound", email)
        return data.get("idToken", ""), data.get("refreshToken", "")
    except Exception as e:
        logger.warning("[Register] Step3 bind email error (non-fatal): %s", str(e)[:150])
        return "", ""


_SEND_OOB_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={FIREBASE_API_KEY}"
_TEMPMAIL_GEN_URL = "https://api.tempmail.lol/generate"


async def _step4b_verify_email(id_token: str, email: str, refresh_tok: str) -> tuple[str, str]:
    """Step 4b: 通过临时邮箱完成 email 验证，返回 (verified_id_token, verified_refresh_token)。

    流程：改绑临时邮箱 → 发送验证邮件 → 读取邮件 → 确认验证 → 刷新 token
    如果失败，返回空字符串（非致命，不阻塞注册）。
    """
    import re
    import asyncio
    import time as _time

    try:
        client_kwargs: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
        async with httpx.AsyncClient(**client_kwargs) as client:
            # 1) 创建临时邮箱
            resp = await client.get(_TEMPMAIL_GEN_URL, timeout=15.0)
            resp.raise_for_status()
            mail_data = resp.json()
            temp_email = mail_data["address"]
            mail_token = mail_data["token"]
            logger.info("[Register] Step4b: temp email=%s", temp_email)

            # 2) 改绑邮箱到临时地址
            rebind_payload = {"idToken": id_token, "email": temp_email, "returnSecureToken": True}
            _proxy = proxy_for_url(_UPDATE_URL)
            ckw: dict = {"timeout": httpx.Timeout(30.0), "verify": TLS_VERIFY, "trust_env": False}
            if _proxy:
                ckw["proxy"] = _proxy
            async with httpx.AsyncClient(**ckw) as fc:
                resp = await fc.post(_UPDATE_URL, json=rebind_payload)
            if resp.status_code != 200:
                logger.warning("[Register] Step4b rebind failed: HTTP %d", resp.status_code)
                return "", ""
            rebind_data = resp.json()
            work_token = rebind_data.get("idToken", "") or id_token
            work_refresh = rebind_data.get("refreshToken", "") or refresh_tok

            # 3) 发送验证邮件
            async with httpx.AsyncClient(**ckw) as fc:
                resp = await fc.post(_SEND_OOB_URL, json={"requestType": "VERIFY_EMAIL", "idToken": work_token})
            if resp.status_code != 200:
                logger.warning("[Register] Step4b sendOobCode failed: HTTP %d", resp.status_code)
                return "", ""

            # 4) 轮询收件箱（最多60秒）
            deadline = _time.time() + 60
            msg = None
            inbox_url = f"https://api.tempmail.lol/auth/{mail_token}"
            while _time.time() < deadline:
                await asyncio.sleep(5)
                resp = await client.get(inbox_url, timeout=15.0)
                emails = resp.json().get("email", [])
                if emails:
                    msg = emails[0]
                    break

            if not msg:
                logger.warning("[Register] Step4b: no verification email received (timeout)")
                return "", ""

            # 5) 提取 oobCode
            html = msg.get("html", "") or msg.get("body", "") or msg.get("text", "")
            oob_match = re.search(r'oobCode=([^&"\'>\s]+)', html)
            if not oob_match:
                logger.warning("[Register] Step4b: no oobCode in email body")
                return "", ""
            oob_code = oob_match.group(1)

            # 6) 确认验证
            async with httpx.AsyncClient(**ckw) as fc:
                resp = await fc.post(_UPDATE_URL, json={"oobCode": oob_code})
            if resp.status_code != 200:
                logger.warning("[Register] Step4b confirm failed: HTTP %d", resp.status_code)
                return "", ""

            # 7) 刷新 token
            await asyncio.sleep(2)
            from .auth import refresh_access_token_with_refresh_token
            final_token = await refresh_access_token_with_refresh_token(work_refresh)
            logger.info("[Register] Step4b: email verified OK, temp_email=%s", temp_email)
            return final_token, work_refresh

    except Exception as e:
        logger.warning("[Register] Step4b verify email error (non-fatal): %s", str(e)[:200])
        return "", ""


async def _step5_get_or_create_user(id_token: str) -> tuple[str, bool]:
    """Step 5: GetOrCreateUser GraphQL → 在 Warp 后端激活用户（关键步骤！）

    缺少此步骤，新注册账号的 AI 请求会返回 400。
    参考项目 batch_register.py 中的 _activate_warp_user() 实现。

    Args:
        id_token: Firebase ID Token（绑定 email 后的新 token 或原始 session_token）

    Returns:
        (uid, is_onboarded) — Warp 用户 UID 和 onboard 状态
    """
    import uuid as _uuid

    session_id = str(_uuid.uuid4())

    query = (
        "mutation GetOrCreateUser("
        "$input: GetOrCreateUserInput!, $requestContext: RequestContext!) { "
        "getOrCreateUser(requestContext: $requestContext, input: $input) { "
        "__typename "
        "... on GetOrCreateUserOutput { uid isOnboarded __typename } "
        "... on UserFacingError { error { message __typename } __typename } "
        "} }"
    )
    body = {
        "operationName": "GetOrCreateUser",
        "variables": {
            "input": {"sessionId": session_id},
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
        "query": query,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {id_token}",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.6778.205 Safari/537.36"
        ),
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }

    # 优先走 rustls proxy，失败则直连
    endpoints = [_GETORCREATE_URL_PROXY, _GETORCREATE_URL_DIRECT]
    resp = None
    for url in endpoints:
        try:
            client_kwargs: dict = {
                "timeout": httpx.Timeout(30.0),
                "verify": TLS_VERIFY,
                "trust_env": False,
            }
            _proxy = proxy_for_url(url)
            if _proxy:
                client_kwargs["proxy"] = _proxy
            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                break
            body_preview = (resp.text or "")[:300]
            logger.warning(
                "[Register] Step5 %s → HTTP %d: %s", url[:50], resp.status_code, body_preview
            )
        except Exception as e:
            logger.warning("[Register] Step5 %s → %s", url[:50], str(e)[:100])

    if resp is None or resp.status_code != 200:
        code = resp.status_code if resp is not None else 0
        raise RuntimeError(f"GetOrCreateUser failed on all endpoints: HTTP {code}")

    data = resp.json()
    result = data.get("data", {}).get("getOrCreateUser", {})
    typename = result.get("__typename", "")

    if typename == "GetOrCreateUserOutput":
        uid = result.get("uid", "")
        is_onboarded = result.get("isOnboarded", False)
        logger.info("[Register] Step5 OK: uid=%s onboarded=%s", uid[:12] if uid else "?", is_onboarded)
        return uid, is_onboarded

    # UserFacingError
    error_msg = result.get("error", {}).get("message", str(data)[:200])
    raise RuntimeError(f"GetOrCreateUser error: {error_msg}")


async def register_account(
    email: Optional[str] = None,
    password: Optional[str] = None,
    bind_email: bool = True,
) -> RegisteredAccount:
    """
    注册一个新的 Warp 账号

    流程：
    1. CreateAnonymousUser → 获取 Firebase custom token
    2. signInWithCustomToken → 获取 refreshToken
    3. accounts:update → 绑定 email/password（必须紧跟 step 2，否则 CREDENTIAL_TOO_OLD）
    4. GetOrCreateUser → 在 Warp 后端激活用户（关键！缺少会导致 AI 请求 400）
    5. proxy/token → 用绑定后的 refreshToken 换取 Warp access_token

    注意：Warp AI 请求要求账号有 email（sign_in_provider=password），
    纯匿名账号（sign_in_provider=custom）会被 400 拒绝。

    Args:
        email: 邮箱地址，为空则随机生成
        password: 密码，为空则随机生成
        bind_email: 是否绑定邮箱（默认 True，Warp 要求有 email 才能用 AI）

    Returns:
        RegisteredAccount 包含完整凭证（含 access_token）
    """
    email = email or _random_email()
    password = password or _random_password()

    # Step 1: 在 Warp 创建匿名用户
    custom_token = await _step1_create_anonymous_user()

    # Step 2: 换取 Firebase session
    session_token, refresh_token, local_id = await _step2_signin_custom_token(custom_token)

    # Step 3: 绑定 email/password（必须紧跟 step 2，延迟会导致 CREDENTIAL_TOO_OLD）
    final_refresh = refresh_token
    activation_token = session_token  # 用于 step 4 的 GetOrCreateUser
    if bind_email:
        new_id_token, new_refresh = await _step4_bind_email(session_token, email, password)
        if new_refresh:
            final_refresh = new_refresh
            logger.info("[Register] Step3 email bound: %s", email)
        else:
            logger.warning("[Register] Step3 email bind failed, using original refresh_token")
        # 绑定成功时用新 id_token 激活，否则用原始 session_token
        if new_id_token:
            activation_token = new_id_token

        # Step 3b: 通过临时邮箱验证 email（Warp 要求 email_verified=true）
        verified_token, verified_refresh = await _step4b_verify_email(
            activation_token, email, final_refresh,
        )
        if verified_token:
            activation_token = verified_token
            logger.info("[Register] Step3b email verified via temp mailbox")
        if verified_refresh:
            final_refresh = verified_refresh

    # Step 4: 在 Warp 后端激活用户（关键步骤！缺少此步 AI 请求会 400）
    await _step5_get_or_create_user(activation_token)

    # Step 5: 用 refreshToken 换取 Warp access_token（用绑定后的 refresh_token）
    access_token = await _step3_exchange_access_token(final_refresh)

    logger.info("[Register] Account ready: email=%s local_id=%s has_access_token=%s",
                email, local_id[:12], bool(access_token))

    return RegisteredAccount(
        email=email,
        local_id=local_id,
        id_token=session_token,
        refresh_token=final_refresh,
        access_token=access_token,
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
                    "id_token": acc.access_token or acc.id_token,
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
