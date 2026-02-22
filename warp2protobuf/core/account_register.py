#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp 账号注册模块

通过 Firebase email/password signUp 创建 Warp 账号，
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
    REFRESH_URL,
)
from .logging import logger

# Firebase API key（从 REFRESH_URL 提取）
_FIREBASE_API_KEY = "AIzaSyBdy3O3S9hrdayLJxJ7mriBR4qgUaUygAs"
_SIGNUP_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={_FIREBASE_API_KEY}"

# 邮箱域名池（模拟 Account Pro 的分布）
_EMAIL_DOMAINS = [
    "icloud.com", "icloud.com", "icloud.com",
    "aol.com", "aol.com",
    "yahoo.com", "yahoo.com",
    "protonmail.com", "protonmail.com",
    "mail.ru",
    "hotmail.com",
    "outlook.com",
    "gmail.com",
]

@dataclass(frozen=True)
class RegisteredAccount:
    """注册成功的账号信息"""
    email: str
    password: str
    local_id: str
    id_token: str
    refresh_token: str


def _generate_random_email() -> str:
    """生成随机邮箱地址"""
    length = random.randint(6, 14)
    username = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    domain = random.choice(_EMAIL_DOMAINS)
    return f"{username}@{domain}"


def _generate_random_password() -> str:
    """生成随机密码（12-16位，含大小写+数字+特殊字符）"""
    length = random.randint(12, 16)
    chars = string.ascii_letters + string.digits + "!@#$%"
    return "".join(random.choices(chars, k=length))


def _build_warp_headers() -> dict:
    """构建 Warp 客户端伪装 headers"""
    return {
        "content-type": "application/json",
        "accept-encoding": "gzip, br",
        "x-warp-client-id": CLIENT_ID,
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
    }


async def register_account(
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> RegisteredAccount:
    """
    注册一个新的 Warp 账号（Firebase email/password signUp）

    Args:
        email: 邮箱地址，为空则随机生成
        password: 密码，为空则随机生成

    Returns:
        RegisteredAccount 包含完整凭证

    Raises:
        RuntimeError: 注册失败
    """
    email = email or _generate_random_email()
    password = password or _generate_random_password()

    headers = _build_warp_headers()
    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        resp = await client.post(_SIGNUP_URL, headers=headers, json=payload)

    if resp.status_code != 200:
        error_text = resp.text[:300]
        logger.error("[Register] signUp failed: HTTP %d %s", resp.status_code, error_text)
        raise RuntimeError(f"Firebase signUp failed: HTTP {resp.status_code} {error_text}")

    data = resp.json()
    id_token = data.get("idToken", "")
    refresh_token = data.get("refreshToken", "")
    local_id = data.get("localId", "")

    if not id_token or not refresh_token:
        raise RuntimeError(f"signUp response missing tokens: {list(data.keys())}")

    logger.info("[Register] Account created: email=%s local_id=%s", email, local_id[:12])

    return RegisteredAccount(
        email=email,
        password=password,
        local_id=local_id,
        id_token=id_token,
        refresh_token=refresh_token,
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
        注册结果列表 [{"success": bool, "account": dict|None, "error": str|None}]
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
            results.append({
                "success": False,
                "account": None,
                "error": error_msg,
            })
            logger.warning("[Register] Batch %d/%d FAIL: %s", i + 1, count, error_msg)

        if i < count - 1:
            await asyncio.sleep(delay_s)

    return results
