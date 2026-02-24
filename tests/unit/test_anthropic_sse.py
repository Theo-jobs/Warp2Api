#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""protobuf2openai.anthropic_sse 单元测试。"""

from __future__ import annotations

import json

from protobuf2openai.anthropic_sse import (
    AnthropicSseState,
    _finalize,
    _ingest_finished_usage,
)


def test_ingest_finished_usage_updates_tokens() -> None:
    state = AnthropicSseState()
    finished = {
        "token_usage": [
            {
                "total_input": 123,
                "output": 45,
            }
        ]
    }

    _ingest_finished_usage(finished, state)

    assert state.input_tokens == 123
    assert state.output_tokens == 45


def _extract_message_delta_stop_reason(event: str) -> str:
    payload_str = event.split("data: ", 1)[1].strip()
    payload = json.loads(payload_str)
    return payload["delta"]["stop_reason"]


def test_finalize_maps_max_token_limit_to_max_tokens() -> None:
    state = AnthropicSseState(has_tool_use=False)
    state.finish_reason = {"max_token_limit": {}}

    events = _finalize(state)

    message_delta_event = next(event for event in events if "event: message_delta" in event)
    assert _extract_message_delta_stop_reason(message_delta_event) == "max_tokens"


def test_finalize_maps_done_with_tool_use_to_tool_use() -> None:
    state = AnthropicSseState(has_tool_use=True)
    state.finish_reason = {"done": {}}

    events = _finalize(state)

    message_delta_event = next(event for event in events if "event: message_delta" in event)
    assert _extract_message_delta_stop_reason(message_delta_event) == "tool_use"


def test_finalize_maps_context_window_exceeded_to_max_tokens() -> None:
    state = AnthropicSseState(has_tool_use=False)
    state.finish_reason = {"context_window_exceeded": {}}

    events = _finalize(state)

    message_delta_event = next(event for event in events if "event: message_delta" in event)
    assert _extract_message_delta_stop_reason(message_delta_event) == "max_tokens"


def test_finalize_maps_internal_error_to_end_turn() -> None:
    state = AnthropicSseState(has_tool_use=False)
    state.finish_reason = {"internal_error": {}}

    events = _finalize(state)

    message_delta_event = next(event for event in events if "event: message_delta" in event)
    assert _extract_message_delta_stop_reason(message_delta_event) == "end_turn"
