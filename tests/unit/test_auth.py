#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""warp2protobuf.core.auth 单元测试。"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest

from warp2protobuf.core import auth


def _make_jwt(payload: dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}

    def _b64(data: dict[str, Any]) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{_b64(header)}.{_b64(payload)}.signature"


def test_decode_jwt_payload_valid() -> None:
    token = _make_jwt({"exp": 4_102_444_800, "user_id": "u-1"})

    payload = auth.decode_jwt_payload(token)

    assert payload["exp"] == 4_102_444_800
    assert payload["user_id"] == "u-1"


def test_decode_jwt_payload_invalid_format() -> None:
    payload = auth.decode_jwt_payload("not-a-jwt")
    assert payload == {}


def test_is_token_expired_false_for_future_token() -> None:
    future_exp = int(time.time()) + 3600
    token = _make_jwt({"exp": future_exp})

    assert auth.is_token_expired(token, buffer_minutes=5) is False


def test_is_token_expired_true_for_near_expiry() -> None:
    near_exp = int(time.time()) + 30
    token = _make_jwt({"exp": near_exp})

    assert auth.is_token_expired(token, buffer_minutes=1) is True


def test_get_user_id_reads_user_id(monkeypatch: pytest.MonkeyPatch) -> None:
    token = _make_jwt({"exp": 4_102_444_800, "user_id": "user-123", "sub": "sub-123"})
    monkeypatch.setenv("WARP_JWT", token)

    user_id = auth.get_user_id()

    assert user_id == "user-123"


def test_get_user_id_falls_back_to_sub(monkeypatch: pytest.MonkeyPatch) -> None:
    token = _make_jwt({"exp": 4_102_444_800, "sub": "sub-only"})
    monkeypatch.setenv("WARP_JWT", token)

    user_id = auth.get_user_id()

    assert user_id == "sub-only"


@pytest.mark.asyncio
async def test_refresh_access_token_with_refresh_token_success(httpx_mock: object) -> None:
    httpx_mock.add_response(status_code=200, json={"access_token": "access-abc"})

    token = await auth.refresh_access_token_with_refresh_token("refresh-xyz")

    assert token == "access-abc"


@pytest.mark.asyncio
async def test_refresh_access_token_with_refresh_token_http_error(httpx_mock: object) -> None:
    httpx_mock.add_response(status_code=401, text="unauthorized")

    with pytest.raises(RuntimeError, match="HTTP 401"):
        await auth.refresh_access_token_with_refresh_token("refresh-xyz")


@pytest.mark.asyncio
async def test_refresh_access_token_with_refresh_token_missing_access(httpx_mock: object) -> None:
    httpx_mock.add_response(status_code=200, json={"expires_in": 3600})

    with pytest.raises(RuntimeError, match="missing access_token"):
        await auth.refresh_access_token_with_refresh_token("refresh-xyz")
