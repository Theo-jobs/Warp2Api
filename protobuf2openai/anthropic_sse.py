"""
Anthropic Messages SSE 流式输出
================================
将 Warp bridge 的 protobuf SSE 流转换为 Anthropic Messages API 的 SSE 格式。

参照 sse_transform.py 中 stream_openai_sse() 的 bridge 请求 + protobuf 解析模式，
输出符合 Anthropic Messages Streaming 规范的 SSE 事件。
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, AsyncGenerator

import httpx

from .config import BRIDGE_BASE_URL
from .helpers import _get

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _sse_line(event: str, data: dict | str) -> str:
    """构造一行 SSE 输出。"""
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


def _gen_msg_id() -> str:
    return f"msg_{uuid.uuid4().hex[:24]}"


def _gen_tool_use_id() -> str:
    return f"toolu_{uuid.uuid4().hex[:24]}"


# ---------------------------------------------------------------------------
# 状态跟踪
# ---------------------------------------------------------------------------

@dataclass
class AnthropicSseState:
    """跟踪 Anthropic SSE 流的状态。"""

    model: str = "claude-sonnet-4-20250514"
    message_id: str = ""
    block_index: int = 0
    block_type: str = ""  # "text" | "tool_use" | ""
    current_tool_id: str = ""
    current_tool_name: str = ""
    input_json_buf: str = ""  # tool_use 的 input JSON 累积
    started: bool = False
    has_tool_use: bool = False  # 是否曾经发射过 tool_use block
    input_tokens: int = 0
    output_tokens: int = 0
    stop_sequence: str | None = None
    finish_reason: dict[str, Any] | None = None

    def next_block_index(self) -> int:
        idx = self.block_index
        self.block_index += 1
        return idx


# ---------------------------------------------------------------------------
# Anthropic SSE 事件发射
# ---------------------------------------------------------------------------

def _emit_message_start(state: AnthropicSseState) -> str:
    state.message_id = _gen_msg_id()
    state.started = True
    return _sse_line("message_start", {
        "type": "message_start",
        "message": {
            "id": state.message_id,
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": state.model,
            "stop_reason": None,
            "stop_sequence": None,
            "usage": {
                "input_tokens": state.input_tokens,
                "output_tokens": 0,
            },
        },
    })


def _emit_ping() -> str:
    return _sse_line("ping", {"type": "ping"})


def _open_text_block(state: AnthropicSseState) -> str:
    idx = state.next_block_index()
    state.block_type = "text"
    return _sse_line("content_block_start", {
        "type": "content_block_start",
        "index": idx,
        "content_block": {"type": "text", "text": ""},
    })


def _emit_text_delta(state: AnthropicSseState, text: str) -> str:
    return _sse_line("content_block_delta", {
        "type": "content_block_delta",
        "index": state.block_index - 1,
        "delta": {"type": "text_delta", "text": text},
    })


def _close_text_block(state: AnthropicSseState) -> str:
    state.block_type = ""
    return _sse_line("content_block_stop", {
        "type": "content_block_stop",
        "index": state.block_index - 1,
    })


def _open_tool_use_block(state: AnthropicSseState, tool_id: str, name: str) -> str:
    idx = state.next_block_index()
    state.block_type = "tool_use"
    state.current_tool_id = tool_id
    state.current_tool_name = name
    state.input_json_buf = ""
    state.has_tool_use = True
    return _sse_line("content_block_start", {
        "type": "content_block_start",
        "index": idx,
        "content_block": {"type": "tool_use", "id": tool_id, "name": name, "input": {}},
    })


def _emit_tool_input_delta(state: AnthropicSseState, partial_json: str) -> str:
    return _sse_line("content_block_delta", {
        "type": "content_block_delta",
        "index": state.block_index - 1,
        "delta": {"type": "input_json_delta", "partial_json": partial_json},
    })


def _close_tool_use_block(state: AnthropicSseState) -> str:
    state.block_type = ""
    state.current_tool_id = ""
    state.current_tool_name = ""
    state.input_json_buf = ""
    return _sse_line("content_block_stop", {
        "type": "content_block_stop",
        "index": state.block_index - 1,
    })


def _emit_message_delta(state: AnthropicSseState, stop_reason: str = "end_turn") -> str:
    return _sse_line("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": stop_reason, "stop_sequence": state.stop_sequence},
        "usage": {"output_tokens": state.output_tokens},
    })


def _emit_message_stop() -> str:
    return _sse_line("message_stop", {"type": "message_stop"})


# ---------------------------------------------------------------------------
# 流结束处理
# ---------------------------------------------------------------------------

def _map_finished_stop_reason(state: AnthropicSseState) -> str:
    """Map Warp finished reason into Anthropic stop_reason."""
    reason = state.finish_reason or {}

    if not isinstance(reason, dict):
        return "tool_use" if state.has_tool_use else "end_turn"

    if "max_token_limit" in reason or "maxTokenLimit" in reason:
        return "max_tokens"
    if "quota_limit" in reason or "quotaLimit" in reason:
        return "end_turn"
    if "context_window_exceeded" in reason or "contextWindowExceeded" in reason:
        return "max_tokens"
    if "llm_unavailable" in reason or "llmUnavailable" in reason:
        return "end_turn"
    if "internal_error" in reason or "internalError" in reason:
        return "end_turn"
    if "done" in reason or "other" in reason:
        return "tool_use" if state.has_tool_use else "end_turn"

    if state.has_tool_use:
        return "tool_use"
    return "end_turn"


def _ingest_finished_usage(finished: dict[str, Any], state: AnthropicSseState) -> None:
    """Extract token usage and stop metadata from finished event."""
    token_usage = finished.get("token_usage") or finished.get("tokenUsage") or []
    if isinstance(token_usage, list) and token_usage:
        usage0 = token_usage[0] if isinstance(token_usage[0], dict) else {}
        total_input = usage0.get("total_input") or usage0.get("totalInput")
        output = usage0.get("output")

        try:
            if isinstance(total_input, (int, float)) and int(total_input) > 0:
                state.input_tokens = max(state.input_tokens, int(total_input))
        except Exception:
            pass

        try:
            if isinstance(output, (int, float)) and int(output) > 0:
                state.output_tokens = max(state.output_tokens, int(output))
        except Exception:
            pass

    reason = finished.get("reason")
    if isinstance(reason, dict):
        state.finish_reason = reason


def _finalize(state: AnthropicSseState) -> list[str]:
    """流结束时，关闭所有未关闭的 block 并发送结束事件。"""
    parts: list[str] = []
    if state.block_type == "text":
        parts.append(_close_text_block(state))
    elif state.block_type == "tool_use":
        parts.append(_close_tool_use_block(state))

    stop_reason = _map_finished_stop_reason(state)
    parts.append(_emit_message_delta(state, stop_reason))
    parts.append(_emit_message_stop())
    return parts


# ---------------------------------------------------------------------------
# Protobuf 事件处理（参照 sse_transform.py 的解析逻辑）
# ---------------------------------------------------------------------------

def _process_warp_event(ev: dict, state: AnthropicSseState) -> list[str]:
    """
    解析一个 Warp bridge SSE event（protobuf 格式），
    提取文本和 tool_call，转换为 Anthropic SSE 事件。

    与 sse_transform.py 的解析结构完全对齐：
    - client_actions 是 dict（不是 list）
    - 支持 snake_case / camelCase 双写
    - 文本路径: append_to_message_content.message.agent_output.text
    - 工具路径: add_messages_to_task.messages[].tool_call.call_mcp_tool
    """
    parts: list[str] = []

    parsed = ev.get("parsed_data")
    if not parsed:
        return parts

    # client_actions 是 dict，不是 list
    client_actions = _get(parsed, "client_actions", "clientActions")
    if not isinstance(client_actions, dict):
        return parts

    actions = _get(client_actions, "actions", "Actions") or []
    for action in actions:
        _action_handled = False
        # ---- 文本内容 ----
        append_data = _get(action, "append_to_message_content", "appendToMessageContent")
        if isinstance(append_data, dict):
            _action_handled = True
            message = append_data.get("message", {})
            agent_output = _get(message, "agent_output", "agentOutput") or {}
            text_content = agent_output.get("text", "")
            if text_content:
                # 如果当前有 tool_use block 打开，先关闭
                if state.block_type == "tool_use":
                    parts.append(_close_tool_use_block(state))
                # 如果没有 text block 打开，先打开
                if state.block_type != "text":
                    parts.append(_open_text_block(state))
                parts.append(_emit_text_delta(state, text_content))
                state.output_tokens += max(1, len(text_content) // 4)

        # ---- Tool call ----
        messages_data = _get(action, "add_messages_to_task", "addMessagesToTask")
        if isinstance(messages_data, dict):
            _action_handled = True
            messages = messages_data.get("messages", [])
            for msg in messages:
                tool_call = _get(msg, "tool_call", "toolCall") or {}
                if not isinstance(tool_call, dict) or not tool_call:
                    # 非 tool_call 消息，可能包含 agent_output 文本
                    agent_output = _get(msg, "agent_output", "agentOutput") or {}
                    text_content = agent_output.get("text", "")
                    if text_content:
                        if state.block_type == "tool_use":
                            parts.append(_close_tool_use_block(state))
                        if state.block_type != "text":
                            parts.append(_open_text_block(state))
                        parts.append(_emit_text_delta(state, text_content))
                        state.output_tokens += max(1, len(text_content) // 4)
                    continue

                # --- 提取 tool_call_id ---
                tc_id = (tool_call.get("tool_call_id")
                         or tool_call.get("toolCallId")
                         or _gen_tool_use_id())

                # --- 识别工具类型和参数 ---
                # ToolCall 是 oneof tool，包含 13 种变体（proto task.proto:114-129）
                # 优先检查 call_mcp_tool（有显式 name/args 字段）
                tool_name: str | None = None
                args_str = "{}"

                call_mcp = _get(tool_call, "call_mcp_tool", "callMcpTool") or {}
                if isinstance(call_mcp, dict) and call_mcp.get("name"):
                    tool_name = call_mcp["name"]
                    try:
                        args_str = json.dumps(call_mcp.get("args", {}) or {}, ensure_ascii=False)
                    except Exception:
                        args_str = "{}"
                else:
                    # 通用提取：遍历 tool_call 的所有 key，
                    # 跳过 tool_call_id/toolCallId，第一个 dict 值即为工具
                    _skip = {"tool_call_id", "toolCallId"}
                    for _k, _v in tool_call.items():
                        if _k in _skip:
                            continue
                        if isinstance(_v, dict):
                            tool_name = _k
                            try:
                                args_str = json.dumps(_v, ensure_ascii=False)
                            except Exception:
                                args_str = "{}"
                            break

                if not tool_name:
                    # tool_call 存在但无法识别工具类型，回退为文本
                    agent_output = _get(msg, "agent_output", "agentOutput") or {}
                    text_content = agent_output.get("text", "")
                    if text_content:
                        if state.block_type == "tool_use":
                            parts.append(_close_tool_use_block(state))
                        if state.block_type != "text":
                            parts.append(_open_text_block(state))
                        parts.append(_emit_text_delta(state, text_content))
                        state.output_tokens += max(1, len(text_content) // 4)
                    continue

                logger.debug("[anthropic_sse] tool_call 提取: name=%s tc_id=%s args_len=%d", tool_name, tc_id, len(args_str))

                # 关闭当前打开的 block
                if state.block_type == "text":
                    parts.append(_close_text_block(state))
                elif state.block_type == "tool_use":
                    parts.append(_close_tool_use_block(state))

                # 打开新的 tool_use block
                anthropic_tool_id = _gen_tool_use_id()
                parts.append(_open_tool_use_block(state, anthropic_tool_id, tool_name))

                # 发射 input_json_delta
                if args_str and args_str != "{}":
                    parts.append(_emit_tool_input_delta(state, args_str))
                    state.input_json_buf = args_str

                # 立即关闭 tool_use block（bridge 一次性给出完整 arguments）
                parts.append(_close_tool_use_block(state))

                state.output_tokens += max(1, len(args_str) // 4)

        # ---- update_task_message（完整消息更新，包含响应文本） ----
        # 与 warp2protobuf/warp/response.py 对齐：提取 message.agent_output.text
        update_msg_data = _get(action, "update_task_message", "updateTaskMessage")
        if isinstance(update_msg_data, dict):
            _action_handled = True
            _umsg = update_msg_data.get("message", {})
            if isinstance(_umsg, dict):
                _uagent = _get(_umsg, "agent_output", "agentOutput") or {}
                _utext = _uagent.get("text", "") if isinstance(_uagent, dict) else ""
                if _utext:
                    logger.debug("[anthropic_sse] update_task_message 提取文本 len=%d", len(_utext))
                    if state.block_type == "tool_use":
                        parts.append(_close_tool_use_block(state))
                    if state.block_type != "text":
                        parts.append(_open_text_block(state))
                    parts.append(_emit_text_delta(state, _utext))
                    state.output_tokens += max(1, len(_utext) // 4)

        # ---- create_task（任务创建，可能包含初始消息文本） ----
        # 与 warp2protobuf/warp/response.py 对齐：提取 task.messages[].agent_output.text
        create_task_data = _get(action, "create_task", "createTask")
        if isinstance(create_task_data, dict):
            _action_handled = True
            _ctask = create_task_data.get("task", {})
            if isinstance(_ctask, dict):
                _cmessages = _ctask.get("messages", []) or []
                for _cmsg in _cmessages:
                    if isinstance(_cmsg, dict):
                        _cagent = _get(_cmsg, "agent_output", "agentOutput") or {}
                        _ctext = _cagent.get("text", "") if isinstance(_cagent, dict) else ""
                        if _ctext:
                            logger.debug("[anthropic_sse] create_task 提取文本 len=%d", len(_ctext))
                            if state.block_type == "tool_use":
                                parts.append(_close_tool_use_block(state))
                            if state.block_type != "text":
                                parts.append(_open_text_block(state))
                            parts.append(_emit_text_delta(state, _ctext))
                            state.output_tokens += max(1, len(_ctext) // 4)

        # ---- Warp 内部控制事件（纯状态管理，不含用户文本） ----
        _KNOWN_CONTROL_ACTIONS = {
            "commit_transaction", "commitTransaction",
            "update_task_description", "updateTaskDescription",
            "update_task_status", "updateTaskStatus",
            "update_task_summary", "updateTaskSummary",
            "set_cursor_position", "setCursorPosition",
            "begin_transaction", "beginTransaction",
            "rollback_transaction", "rollbackTransaction",
            "start_new_conversation", "startNewConversation",
        }
        if not _action_handled:
            _action_keys = set(action.keys()) if isinstance(action, dict) else set()
            if _action_keys & _KNOWN_CONTROL_ACTIONS:
                logger.debug("[anthropic_sse] 忽略 Warp 控制事件: keys=%s", list(_action_keys))
            else:
                logger.warning("[anthropic_sse] 未识别的 action 类型，内容可能丢失: keys=%s", list(_action_keys))

    return parts


# ---------------------------------------------------------------------------
# 主函数：stream_anthropic_sse
# ---------------------------------------------------------------------------

async def stream_anthropic_sse(
    packet: dict,
    *,
    model: str = "claude-sonnet-4-20250514",
    access_token: str | None = None,
    estimated_input_tokens: int = 0,
) -> AsyncGenerator[str, None]:
    """
    向 Warp bridge 发送请求，将 protobuf SSE 流转换为 Anthropic Messages SSE 格式。

    参照 sse_transform.py 中 stream_openai_sse() 的 bridge 请求模式。
    """
    state = AnthropicSseState(model=model)
    state.input_tokens = estimated_input_tokens

    # ---- 构造 bridge 请求（与 stream_openai_sse 完全一致：JSON body） ----
    bridge_url = f"{BRIDGE_BASE_URL}/api/warp/send_stream_sse"
    req_body: dict = {
        "json_data": packet,
        "message_type": "warp.multi_agent.v1.Request",
    }
    if access_token:
        req_body["access_token"] = access_token

    logger.info("[anthropic_sse] bridge_url=%s model=%s", bridge_url, model)

    try:
        async with httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(300.0, connect=30.0),
            trust_env=True,
        ) as client:
            async with client.stream(
                "POST", bridge_url,
                headers={"accept": "text/event-stream"},
                json=req_body,
            ) as resp:
                if resp.status_code != 200:
                    body = await resp.aread()
                    logger.error(
                        "[anthropic_sse] bridge returned %s: %s",
                        resp.status_code,
                        body[:500],
                    )
                    error_msg = f"Bridge error: HTTP {resp.status_code}"
                    yield _emit_message_start(state)
                    yield _open_text_block(state)
                    yield _emit_text_delta(state, error_msg)
                    yield _close_text_block(state)
                    yield _emit_message_delta(state, "end_turn")
                    yield _emit_message_stop()
                    return

                # ---- 发射 message_start + ping ----
                yield _emit_message_start(state)
                yield _emit_ping()

                # ---- SSE 流解析（与 stream_openai_sse 一致：aiter_lines） ----
                current = ""
                _event_count = 0
                async for line in resp.aiter_lines():
                    if line.startswith("data:"):
                        payload_str = line[5:].strip()
                        if not payload_str:
                            continue
                        logger.info("[anthropic_sse] 接收到 Protobuf SSE 数据块（len=%d）", len(payload_str))
                        if payload_str == "[DONE]":
                            break
                        current += payload_str
                        continue
                    if (line.strip() == "") and current:
                        try:
                            ev = json.loads(current)
                        except json.JSONDecodeError:
                            logger.warning(
                                "[anthropic_sse] JSON decode error: %s",
                                current[:200],
                            )
                            current = ""
                            continue
                        current = ""
                        _event_count += 1

                        # 记录事件摘要
                        event_data = (ev or {}).get("parsed_data") or {}
                        try:
                            logger.info(
                                "[anthropic_sse] 接收到 Protobuf 事件 #%d (parsed): keys=%s",
                                _event_count,
                                list(event_data.keys()) if isinstance(event_data, dict) else type(event_data).__name__,
                            )
                        except Exception:
                            pass

                        # 提取 init 事件中的 conversation_id / task_id
                        if "init" in event_data:
                            init_data = event_data["init"]
                            _conv_id = init_data.get("conversation_id", "")
                            _task_id = init_data.get("task_id", "")
                            if _conv_id:
                                logger.info("[anthropic_sse] 会话初始化: conversation_id=%s task_id=%s", _conv_id, _task_id)
                                yield f"\x00__CONV_ID__:{_conv_id}"
                            if _task_id:
                                yield f"\x00__TASK_ID__:{_task_id}"
                            continue

                        # 检查错误事件
                        if isinstance(ev, dict) and ev.get("error"):
                            err_msg = str(ev["error"])
                            logger.error("[anthropic_sse] Bridge SSE error: %s", err_msg)
                            if state.block_type != "text":
                                yield _open_text_block(state)
                            yield _emit_text_delta(state, f"Bridge error: {err_msg}")
                            continue

                        # 处理 protobuf 事件
                        parts = _process_warp_event(ev, state)
                        for p in parts:
                            # 记录发射的 SSE 事件类型
                            try:
                                _evt_type = ""
                                if "content_block_delta" in p:
                                    _pdata = json.loads(p.split("data: ", 1)[1].split("\n")[0])
                                    _delta = _pdata.get("delta", {})
                                    if _delta.get("type") == "text_delta":
                                        logger.info("[anthropic_sse] emit: text_delta len=%d", len(_delta.get("text", "")))
                                    elif _delta.get("type") == "input_json_delta":
                                        logger.info("[anthropic_sse] emit: input_json_delta len=%d", len(_delta.get("partial_json", "")))
                                elif "content_block_start" in p:
                                    _pdata = json.loads(p.split("data: ", 1)[1].split("\n")[0])
                                    _block = _pdata.get("content_block", {})
                                    logger.info("[anthropic_sse] emit: block_start type=%s", _block.get("type", "?"))
                                elif "content_block_stop" in p:
                                    logger.info("[anthropic_sse] emit: block_stop index=%d", state.block_index - 1)
                            except Exception:
                                pass
                            yield p

                        # 检查 finished 事件
                        if "finished" in event_data:
                            finished = event_data.get("finished")
                            if isinstance(finished, dict):
                                _ingest_finished_usage(finished, state)
                                _raw_reason = finished.get("reason")
                                if _raw_reason:
                                    logger.info("[anthropic_sse] finished reason: %s", _raw_reason)
                            else:
                                logger.warning("[anthropic_sse] finished 事件非 dict: %s", type(finished).__name__)
                            logger.info("[anthropic_sse] 收到 finished 事件，结束流")
                            break

                # ---- 流结束，finalize ----
                logger.info(
                    "[anthropic_sse] 流结束: events=%d blocks=%d has_tool_use=%s input_tokens=%d output_tokens=%d reason=%s",
                    _event_count,
                    state.block_index,
                    state.has_tool_use,
                    state.input_tokens,
                    state.output_tokens,
                    state.finish_reason,
                )
                # 低 output 告警：可能是流被异常截断
                if state.output_tokens < 50 and _event_count > 5 and not state.has_tool_use:
                    logger.warning(
                        "[anthropic_sse] ⚠ 异常低输出: output_tokens=%d events=%d，可能存在内容丢失或后端截断",
                        state.output_tokens, _event_count,
                    )
                for p in _finalize(state):
                    yield p

                # 内部元数据信号：让外层知道 finish_reason（不是 SSE 事件，以 \x00 开头）
                if state.finish_reason and isinstance(state.finish_reason, dict):
                    if "quota_limit" in state.finish_reason or "quotaLimit" in state.finish_reason:
                        yield "\x00__QUOTA_LIMIT__"

    except httpx.ConnectError as exc:
        logger.error("[anthropic_sse] connect error: %s", exc)
        state_fresh = AnthropicSseState(model=model)
        yield _emit_message_start(state_fresh)
        yield _open_text_block(state_fresh)
        yield _emit_text_delta(state_fresh, f"Connection error: {exc}")
        yield _close_text_block(state_fresh)
        yield _emit_message_delta(state_fresh, "end_turn")
        yield _emit_message_stop()

    except httpx.ReadTimeout as exc:
        logger.error("[anthropic_sse] read timeout: %s", exc)
        # 如果已经开始了，尝试 finalize
        if state.started:
            for p in _finalize(state):
                yield p
        else:
            state_fresh = AnthropicSseState(model=model)
            yield _emit_message_start(state_fresh)
            yield _open_text_block(state_fresh)
            yield _emit_text_delta(state_fresh, "Request timed out")
            yield _close_text_block(state_fresh)
            yield _emit_message_delta(state_fresh, "end_turn")
            yield _emit_message_stop()

    except Exception as exc:
        logger.exception("[anthropic_sse] unexpected error")
        if state.started:
            for p in _finalize(state):
                yield p
        else:
            state_fresh = AnthropicSseState(model=model)
            yield _emit_message_start(state_fresh)
            yield _open_text_block(state_fresh)
            yield _emit_text_delta(state_fresh, f"Internal error: {exc}")
            yield _close_text_block(state_fresh)
            yield _emit_message_delta(state_fresh, "end_turn")
            yield _emit_message_stop()
