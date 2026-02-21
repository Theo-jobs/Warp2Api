#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""protobuf_routes account pool helper 单元测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from warp2protobuf.api import protobuf_routes as routes
from warp2protobuf.core import auth


@pytest.mark.asyncio
async def test_release_pool_session_success() -> None:
    pool_client = SimpleNamespace(release=AsyncMock())

    await routes._release_pool_session(pool_client, "s1")

    pool_client.release.assert_awaited_once_with("s1")


@pytest.mark.asyncio
async def test_release_pool_session_skip_when_missing_client_or_session() -> None:
    pool_client = SimpleNamespace(release=AsyncMock())

    await routes._release_pool_session(None, "s1")
    await routes._release_pool_session(pool_client, None)

    pool_client.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_release_pool_session_swallow_release_error() -> None:
    pool_client = SimpleNamespace(release=AsyncMock(side_effect=RuntimeError("boom")))

    await routes._release_pool_session(pool_client, "s1")

    pool_client.release.assert_awaited_once_with("s1")


@pytest.mark.asyncio
async def test_allocate_access_token_from_pool_success(monkeypatch: pytest.MonkeyPatch) -> None:
    pool_client = SimpleNamespace(
        allocate=AsyncMock(return_value={"accounts": [{"refresh_token": "rt-1"}]})
    )
    release_mock = AsyncMock()
    refresh_mock = AsyncMock(return_value="access-1")

    monkeypatch.setattr(routes, "_release_pool_session", release_mock)
    monkeypatch.setattr(routes, "refresh_access_token_with_refresh_token", refresh_mock)

    token = await routes._allocate_access_token_from_pool(pool_client, "s1")

    assert token == "access-1"
    release_mock.assert_not_awaited()
    refresh_mock.assert_awaited_once_with("rt-1")


@pytest.mark.asyncio
async def test_allocate_access_token_from_pool_empty_accounts_release(monkeypatch: pytest.MonkeyPatch) -> None:
    pool_client = SimpleNamespace(allocate=AsyncMock(return_value={"accounts": []}))
    release_mock = AsyncMock()

    monkeypatch.setattr(routes, "_release_pool_session", release_mock)

    with pytest.raises(RuntimeError, match="no accounts"):
        await routes._allocate_access_token_from_pool(pool_client, "s1")

    release_mock.assert_awaited_once_with(pool_client, "s1")


@pytest.mark.asyncio
async def test_allocate_access_token_from_pool_invalid_account_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_client = SimpleNamespace(allocate=AsyncMock(return_value={"accounts": ["bad"]}))
    release_mock = AsyncMock()

    monkeypatch.setattr(routes, "_release_pool_session", release_mock)

    with pytest.raises(RuntimeError, match="invalid account payload"):
        await routes._allocate_access_token_from_pool(pool_client, "s1")

    release_mock.assert_awaited_once_with(pool_client, "s1")


@pytest.mark.asyncio
async def test_allocate_access_token_from_pool_missing_refresh_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_client = SimpleNamespace(allocate=AsyncMock(return_value={"accounts": [{}]}))
    release_mock = AsyncMock()

    monkeypatch.setattr(routes, "_release_pool_session", release_mock)

    with pytest.raises(RuntimeError, match="missing refresh_token"):
        await routes._allocate_access_token_from_pool(pool_client, "s1")

    release_mock.assert_awaited_once_with(pool_client, "s1")


@pytest.mark.asyncio
async def test_allocate_access_token_from_pool_refresh_failed_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_client = SimpleNamespace(
        allocate=AsyncMock(return_value={"accounts": [{"refresh_token": "rt-1"}]})
    )
    release_mock = AsyncMock()
    refresh_mock = AsyncMock(side_effect=RuntimeError("refresh failed"))

    monkeypatch.setattr(routes, "_release_pool_session", release_mock)
    monkeypatch.setattr(routes, "refresh_access_token_with_refresh_token", refresh_mock)

    with pytest.raises(RuntimeError, match="refresh failed"):
        await routes._allocate_access_token_from_pool(pool_client, "s1")

    release_mock.assert_awaited_once_with(pool_client, "s1")


@pytest.mark.asyncio
async def test_resolve_request_access_token_pool_success(monkeypatch: pytest.MonkeyPatch) -> None:
    pool_client = object()
    allocate_mock = AsyncMock(return_value="pool-token")

    monkeypatch.setattr(routes, "_allocate_access_token_from_pool", allocate_mock)

    token, using_pool = await routes._resolve_request_access_token(pool_client, "s1")

    assert token == "pool-token"
    assert using_pool is True
    allocate_mock.assert_awaited_once_with(pool_client, "s1")


@pytest.mark.asyncio
async def test_resolve_request_access_token_pool_fail_fallback_to_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_client = object()
    allocate_mock = AsyncMock(side_effect=RuntimeError("pool down"))
    env_jwt_mock = AsyncMock(return_value="env-token")

    monkeypatch.setattr(routes, "_allocate_access_token_from_pool", allocate_mock)
    monkeypatch.setattr(routes, "ACCOUNT_POOL_FALLBACK_TO_ENV", True)
    monkeypatch.setattr(auth, "get_valid_jwt", env_jwt_mock)

    token, using_pool = await routes._resolve_request_access_token(pool_client, "s1")

    assert token == "env-token"
    assert using_pool is False
    env_jwt_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_request_access_token_pool_fail_no_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_client = object()
    allocate_mock = AsyncMock(side_effect=RuntimeError("pool down"))

    monkeypatch.setattr(routes, "_allocate_access_token_from_pool", allocate_mock)
    monkeypatch.setattr(routes, "ACCOUNT_POOL_FALLBACK_TO_ENV", False)

    with pytest.raises(RuntimeError, match="fallback disabled"):
        await routes._resolve_request_access_token(pool_client, "s1")
