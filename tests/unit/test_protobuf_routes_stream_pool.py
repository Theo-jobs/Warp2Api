#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""protobuf_routes SSE + account pool 重试行为测试。"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from warp2protobuf.api import protobuf_routes as routes
from warp2protobuf.warp import api_client


def _identity_wrapper(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


def _to_text(chunk: Any) -> str:
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8")
    return str(chunk)


@pytest.mark.asyncio
async def test_stream_sse_switch_account_on_quota_429_then_release_old_and_final(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeWarpSseRequest:
        call_count = 0

        def __init__(self, protobuf_bytes: bytes, access_token: str) -> None:
            self.protobuf_bytes = protobuf_bytes
            self.access_token = access_token

        async def iter_lines(self):
            FakeWarpSseRequest.call_count += 1
            if FakeWarpSseRequest.call_count == 1:
                raise api_client.WarpApiHttpError(429, "No remaining quota")
            yield "data: [DONE]"

    release_mock = AsyncMock()
    resolve_mock = AsyncMock(side_effect=[("token-1", True), ("token-2", True)])

    session_ids = iter(["session-old", "session-new"])

    monkeypatch.setattr(routes, "sanitize_mcp_input_schema_in_packet", _identity_wrapper)
    monkeypatch.setattr(routes, "_encode_smd_inplace", lambda v: v)
    monkeypatch.setattr(routes, "dict_to_protobuf_bytes", lambda _data, _msg_type: b"pb")
    monkeypatch.setattr(routes, "_resolve_request_access_token", resolve_mock)
    monkeypatch.setattr(routes, "_release_pool_session", release_mock)
    monkeypatch.setattr(routes.uuid, "uuid4", lambda: next(session_ids))
    monkeypatch.setattr(routes, "ACCOUNT_POOL_SWITCH_MAX_RETRIES", 2)
    monkeypatch.setattr(api_client, "WarpSseRequest", FakeWarpSseRequest)

    request = routes.EncodeRequest(json_data={"k": "v"})
    response = await routes.send_to_warp_api_stream_sse(request)

    chunks: list[str] = []
    async for chunk in response.body_iterator:
        chunks.append(_to_text(chunk))

    assert any("[DONE]" in chunk for chunk in chunks)
    assert release_mock.await_count == 2
    assert release_mock.await_args_list[0].args[1] == "session-old"
    assert release_mock.await_args_list[1].args[1] == "session-new"


@pytest.mark.asyncio
async def test_stream_sse_non_429_error_release_final_session_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeWarpSseRequest:
        def __init__(self, protobuf_bytes: bytes, access_token: str) -> None:
            self.protobuf_bytes = protobuf_bytes
            self.access_token = access_token

        async def iter_lines(self):
            raise api_client.WarpApiHttpError(500, "upstream broken")
            yield ""

    release_mock = AsyncMock()
    resolve_mock = AsyncMock(return_value=("token-1", True))

    monkeypatch.setattr(routes, "sanitize_mcp_input_schema_in_packet", _identity_wrapper)
    monkeypatch.setattr(routes, "_encode_smd_inplace", lambda v: v)
    monkeypatch.setattr(routes, "dict_to_protobuf_bytes", lambda _data, _msg_type: b"pb")
    monkeypatch.setattr(routes, "_resolve_request_access_token", resolve_mock)
    monkeypatch.setattr(routes, "_release_pool_session", release_mock)
    monkeypatch.setattr(routes.uuid, "uuid4", lambda: "session-only")
    monkeypatch.setattr(routes, "ACCOUNT_POOL_SWITCH_MAX_RETRIES", 2)
    monkeypatch.setattr(api_client, "WarpSseRequest", FakeWarpSseRequest)

    request = routes.EncodeRequest(json_data={"k": "v"})
    response = await routes.send_to_warp_api_stream_sse(request)

    chunks: list[str] = []
    async for chunk in response.body_iterator:
        chunks.append(_to_text(chunk))

    merged = "".join(chunks)
    assert "HTTP 500" in merged
    assert "[DONE]" in merged
    release_mock.assert_awaited_once()
    assert release_mock.await_args.args[1] == "session-only"
