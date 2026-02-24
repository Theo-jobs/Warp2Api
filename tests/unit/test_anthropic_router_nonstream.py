#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""protobuf2openai.anthropic_router 非流式响应构建测试。"""

from __future__ import annotations

from protobuf2openai.anthropic_router import (
    _anthropic_nonstream_response_from_bridge,
    _resolve_thinking_model,
    _tool_result_content_to_text,
)


def test_nonstream_response_builds_text_tool_use_usage_and_stop_reason() -> None:
    bridge_resp = {
        "response": "fallback",
        "parsed_events": [
            {
                "parsed_data": {
                    "client_actions": {
                        "actions": [
                            {
                                "append_to_message_content": {
                                    "message": {
                                        "agent_output": {
                                            "text": "hello ",
                                        }
                                    }
                                }
                            },
                            {
                                "add_messages_to_task": {
                                    "messages": [
                                        {
                                            "tool_call": {
                                                "tool_call_id": "toolu_123",
                                                "call_mcp_tool": {
                                                    "name": "read_file",
                                                    "args": {"path": "/tmp/a.txt"},
                                                },
                                            }
                                        }
                                    ]
                                }
                            },
                        ]
                    },
                    "finished": {
                        "token_usage": [
                            {
                                "total_input": 10,
                                "output": 5,
                                "input_cache_read": 2,
                                "input_cache_write": 3,
                            }
                        ],
                        "reason": {"done": {}},
                    },
                }
            }
        ],
    }

    payload = _anthropic_nonstream_response_from_bridge(
        bridge_resp=bridge_resp,
        model="claude-sonnet-4-6-20260219",
    )

    assert payload["type"] == "message"
    assert payload["role"] == "assistant"
    assert payload["model"] == "claude-sonnet-4-6-20260219"
    assert payload["stop_reason"] == "tool_use"

    assert isinstance(payload["content"], list)
    assert payload["content"][0]["type"] == "text"
    assert payload["content"][0]["text"] == "hello"

    tool_block = payload["content"][1]
    assert tool_block["type"] == "tool_use"
    assert tool_block["id"] == "toolu_123"
    assert tool_block["name"] == "read_file"
    assert tool_block["input"] == {"path": "/tmp/a.txt"}

    usage = payload["usage"]
    assert usage["input_tokens"] == 10
    assert usage["output_tokens"] == 5
    assert usage["cache_read_input_tokens"] == 2
    assert usage["cache_creation_input_tokens"] == 3


def test_nonstream_response_uses_fallback_text_when_no_content_blocks() -> None:
    bridge_resp = {
        "response": "plain fallback text",
        "parsed_events": [],
    }

    payload = _anthropic_nonstream_response_from_bridge(
        bridge_resp=bridge_resp,
        model="claude-4-sonnet",
    )

    assert payload["content"] == [{"type": "text", "text": "plain fallback text"}]
    assert payload["stop_reason"] == "end_turn"


def test_nonstream_response_maps_quota_limit_to_end_turn() -> None:
    bridge_resp = {
        "parsed_events": [
            {
                "parsed_data": {
                    "finished": {
                        "reason": {"quota_limit": {}},
                    }
                }
            }
        ]
    }

    payload = _anthropic_nonstream_response_from_bridge(
        bridge_resp=bridge_resp,
        model="claude-4-sonnet",
    )

    assert payload["stop_reason"] == "end_turn"


def test_tool_result_content_to_text_keeps_structured_dict_block() -> None:
    text = _tool_result_content_to_text(
        {
            "type": "json",
            "data": {"a": 1, "b": True},
        }
    )

    assert text == '{"type":"json","data":{"a":1,"b":true}}'


def test_tool_result_content_to_text_keeps_mixed_text_and_structured_blocks() -> None:
    text = _tool_result_content_to_text(
        [
            {"type": "text", "text": "read ok"},
            {
                "type": "json",
                "data": {"path": "/tmp/a.txt", "content": "hello"},
            },
        ]
    )

    assert text == 'read ok\n{"type":"json","data":{"path":"/tmp/a.txt","content":"hello"}}'


def test_resolve_thinking_model_maps_supported_models() -> None:
    sonnet = _resolve_thinking_model("claude-4-5-sonnet-20260219", thinking_enabled=True)
    opus = _resolve_thinking_model("claude-4-5-opus-20260219", thinking_enabled=True)

    assert sonnet == "claude-4-5-sonnet-thinking"
    assert opus == "claude-4-5-opus-thinking"


def test_resolve_thinking_model_keeps_existing_thinking_variant() -> None:
    model = _resolve_thinking_model("claude-4-5-sonnet-thinking", thinking_enabled=True)
    assert model == "claude-4-5-sonnet-thinking"


def test_resolve_thinking_model_unsupported_model_raises() -> None:
    try:
        _resolve_thinking_model("claude-sonnet-4-20250514", thinking_enabled=True)
    except ValueError as exc:
        assert "thinking is not supported" in str(exc)
        return

    raise AssertionError("Expected ValueError for unsupported thinking model")
