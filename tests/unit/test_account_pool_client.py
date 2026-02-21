#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AccountPoolClient 单元测试。"""

from __future__ import annotations

import pytest

from warp2protobuf.core.account_pool_client import AccountPoolClient


@pytest.mark.asyncio
async def test_allocate_success(httpx_mock: object) -> None:
    client = AccountPoolClient(base_url="http://pool.test")
    httpx_mock.add_response(
        method="POST",
        url="http://pool.test/api/accounts/allocate",
        status_code=200,
        json={
            "success": True,
            "accounts": [{"email": "u1@example.com", "refresh_token": "rt1"}],
            "message": "ok",
        },
    )

    data = await client.allocate(session_id="s1", count=1)

    assert data["success"] is True
    assert len(data["accounts"]) == 1


@pytest.mark.asyncio
async def test_allocate_http_error(httpx_mock: object) -> None:
    client = AccountPoolClient(base_url="http://pool.test")
    httpx_mock.add_response(
        method="POST",
        url="http://pool.test/api/accounts/allocate",
        status_code=500,
        text="internal error",
    )

    with pytest.raises(RuntimeError, match="allocate failed: HTTP 500"):
        await client.allocate(session_id="s1", count=1)


@pytest.mark.asyncio
async def test_allocate_success_false(httpx_mock: object) -> None:
    client = AccountPoolClient(base_url="http://pool.test")
    httpx_mock.add_response(
        method="POST",
        url="http://pool.test/api/accounts/allocate",
        status_code=200,
        json={"success": False, "message": "pool empty"},
    )

    with pytest.raises(RuntimeError, match="pool empty"):
        await client.allocate(session_id="s1", count=1)


@pytest.mark.asyncio
async def test_release_success(httpx_mock: object) -> None:
    client = AccountPoolClient(base_url="http://pool.test")
    httpx_mock.add_response(
        method="POST",
        url="http://pool.test/api/accounts/release",
        status_code=200,
        json={"success": True, "message": "released"},
    )

    data = await client.release(session_id="s1")

    assert data["success"] is True


@pytest.mark.asyncio
async def test_status_success(httpx_mock: object) -> None:
    client = AccountPoolClient(base_url="http://pool.test")
    httpx_mock.add_response(
        method="GET",
        url="http://pool.test/api/accounts/status",
        status_code=200,
        json={"pool_stats": {"total": 10, "available": 8}},
    )

    data = await client.status()

    assert data["pool_stats"]["total"] == 10
