#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""warp2protobuf.warp.api_client 单元测试。"""

from __future__ import annotations

from warp2protobuf.warp.api_client import (
    WarpApiHttpError,
    _get_event_type,
    _parse_payload_bytes,
    is_quota_429,
)


def test_warp_api_http_error_fields() -> None:
    err = WarpApiHttpError(429, "No remaining quota")

    assert err.status_code == 429
    assert err.error_content == "No remaining quota"
    assert "HTTP 429" in str(err)


def test_is_quota_429_matches_known_messages() -> None:
    assert is_quota_429(429, "No remaining quota") is True
    assert is_quota_429(429, "No AI requests remaining today") is True
    assert is_quota_429(429, "other reason") is False
    assert is_quota_429(500, "No remaining quota") is False


def test_get_event_type_recognizes_finished() -> None:
    event_type = _get_event_type({"finished": {}})
    assert event_type == "FINISHED"


def test_get_event_type_recognizes_client_actions() -> None:
    event = {
        "client_actions": {
            "actions": [
                {"append_to_message_content": {}},
                {"tool_call": {}},
            ]
        }
    }

    event_type = _get_event_type(event)

    assert "CLIENT_ACTIONS" in event_type
    assert "APPEND_CONTENT" in event_type
    assert "TOOL_CALL" in event_type


def test_parse_payload_bytes_supports_hex_and_base64() -> None:
    assert _parse_payload_bytes("68656c6c6f") == b"hello"
    assert _parse_payload_bytes("aGVsbG8=") == b"hello"
    assert _parse_payload_bytes("") is None
