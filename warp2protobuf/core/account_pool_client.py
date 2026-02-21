#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Account pool service client.

Wraps allocate/release/status APIs for external account-pool-service.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from ..config.settings import (
    ACCOUNT_POOL_ALLOCATE_TIMEOUT,
    ACCOUNT_POOL_BASE_URL,
    ACCOUNT_POOL_RELEASE_TIMEOUT,
)
from .logging import logger


def _mask_secret(value: str, keep: int = 4) -> str:
    """Mask token-like secrets for logging."""
    if not value:
        return "<empty>"
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}...{value[-keep:]}"


class AccountPoolClient:
    """HTTP client for account-pool-service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        allocate_timeout: Optional[float] = None,
        release_timeout: Optional[float] = None,
    ) -> None:
        self.base_url = (base_url or ACCOUNT_POOL_BASE_URL).rstrip("/")
        self.allocate_timeout = (
            float(allocate_timeout)
            if allocate_timeout is not None
            else float(ACCOUNT_POOL_ALLOCATE_TIMEOUT)
        )
        self.release_timeout = (
            float(release_timeout)
            if release_timeout is not None
            else float(ACCOUNT_POOL_RELEASE_TIMEOUT)
        )

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    async def allocate(self, session_id: str, count: int = 1) -> Dict[str, Any]:
        """Allocate account(s) for a request session."""
        payload: Dict[str, Any] = {"session_id": session_id, "count": count}
        url = self._url("/api/accounts/allocate")

        try:
            async with httpx.AsyncClient(timeout=self.allocate_timeout) as client:
                response = await client.post(url, json=payload)
        except Exception as exc:
            raise RuntimeError(f"allocate request failed: {exc}") from exc

        if response.status_code != 200:
            body_preview = (response.text or "")[:300]
            raise RuntimeError(
                f"allocate failed: HTTP {response.status_code} body={body_preview}"
            )

        data: Dict[str, Any] = response.json()
        if not data.get("success"):
            message = str(data.get("message") or "allocate returned success=false")
            raise RuntimeError(message)

        accounts = data.get("accounts") or []
        account_hint = "unknown"
        if isinstance(accounts, list) and accounts:
            first = accounts[0] if isinstance(accounts[0], dict) else {}
            account_hint = str(first.get("email") or first.get("local_id") or "unknown")

        logger.info(
            "[AccountPool] allocate success: session_id=%s count=%s account=%s",
            session_id,
            len(accounts),
            _mask_secret(account_hint, keep=2),
        )
        return data

    async def release(self, session_id: str) -> Dict[str, Any]:
        """Release account(s) bound to a request session."""
        payload = {"session_id": session_id}
        url = self._url("/api/accounts/release")

        try:
            async with httpx.AsyncClient(timeout=self.release_timeout) as client:
                response = await client.post(url, json=payload)
        except Exception as exc:
            raise RuntimeError(f"release request failed: {exc}") from exc

        if response.status_code != 200:
            body_preview = (response.text or "")[:300]
            raise RuntimeError(
                f"release failed: HTTP {response.status_code} body={body_preview}"
            )

        data: Dict[str, Any] = response.json()
        logger.info("[AccountPool] release session_id=%s success=%s", session_id, data.get("success"))
        return data

    async def status(self) -> Dict[str, Any]:
        """Get account pool status."""
        url = self._url("/api/accounts/status")
        try:
            async with httpx.AsyncClient(timeout=self.allocate_timeout) as client:
                response = await client.get(url)
        except Exception as exc:
            raise RuntimeError(f"status request failed: {exc}") from exc

        if response.status_code != 200:
            body_preview = (response.text or "")[:300]
            raise RuntimeError(
                f"status failed: HTTP {response.status_code} body={body_preview}"
            )
        return response.json()
