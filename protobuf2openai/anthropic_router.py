"""
Anthropic Messages API → Warp bridge router.

Accepts Anthropic-format requests at /v1/messages, converts them to
Warp packet format, and streams back Anthropic-format SSE responses.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from warp2protobuf.config.settings import ACCOUNT_DB_PATH
from warp2protobuf.core.account_selector import AccountSelector
from warp2protobuf.config.models import resolve_model

from .anthropic_models import AnthropicMessagesRequest
from .auth import authenticate_request
from .anthropic_sse import stream_anthropic_sse
from .bridge import initialize_once
from .helpers import normalize_content_to_list, segments_to_text
from .models import ChatMessage
from .packets import attach_user_and_tools_to_inputs, packet_template
from .token_manager import TokenManager

logger = logging.getLogger(__name__)

anthropic_router = APIRouter()


# ---------------------------------------------------------------------------
# Anthropic → Warp message conversion helpers
# ---------------------------------------------------------------------------

def _extract_system_prompt(req: AnthropicMessagesRequest) -> str | None:
    """Extract the system prompt from the Anthropic request."""
    if req.system is None:
        return None
    if isinstance(req.system, str):
        return req.system if req.system.strip() else None
    # system can be a list of AnthropicSystemBlock or dicts
    if isinstance(req.system, list):
        parts: list[str] = []
        for block in req.system:
            if isinstance(block, str):
                parts.append(block)
            elif hasattr(block, "text"):
                # AnthropicSystemBlock pydantic model
                parts.append(block.text)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "\n".join(parts) if parts else None
    return None


def _serialize_content_blocks(content: Any) -> str:
    """Serialize Anthropic content blocks to a plain-text string for Warp."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type", "")
                if btype == "text":
                    parts.append(block.get("text", ""))
                elif btype == "tool_use":
                    # Encode tool_use as a JSON string so Warp can parse it
                    parts.append(json.dumps(block, ensure_ascii=False))
                elif btype == "tool_result":
                    # Flatten tool_result content
                    inner = block.get("content", "")
                    if isinstance(inner, list):
                        for sub in inner:
                            if isinstance(sub, dict) and sub.get("type") == "text":
                                parts.append(sub.get("text", ""))
                            elif isinstance(sub, str):
                                parts.append(sub)
                    elif isinstance(inner, str):
                        parts.append(inner)
                elif btype == "image":
                    parts.append("[image]")
                else:
                    parts.append(json.dumps(block, ensure_ascii=False))
        return "\n".join(parts)
    return str(content)


def _find_last_tool_results(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Find tool_result blocks in the last user message.
    Returns a list of {tool_use_id, content} dicts.
    """
    results: list[dict[str, Any]] = []
    if not messages:
        return results
    last_msg = messages[-1]
    if last_msg.get("role") != "user":
        return results
    content = last_msg.get("content")
    if not isinstance(content, list):
        return results
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_result":
            tool_use_id = block.get("tool_use_id", "")
            inner = block.get("content", "")
            if isinstance(inner, list):
                text_parts = []
                for sub in inner:
                    if isinstance(sub, dict) and sub.get("type") == "text":
                        text_parts.append(sub.get("text", ""))
                    elif isinstance(sub, str):
                        text_parts.append(sub)
                inner = "\n".join(text_parts)
            results.append({"tool_use_id": tool_use_id, "content": str(inner)})
    return results


def _build_warp_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert Anthropic messages to Warp-compatible messages.

    Warp expects simple {role, content} messages. Tool-use and tool-result
    blocks are serialized to text.
    """
    warp_msgs: list[dict[str, Any]] = []

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        if role == "assistant":
            # Check for tool_use blocks inside assistant content
            if isinstance(content, list):
                text_parts: list[str] = []
                tool_calls: list[dict[str, Any]] = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "tool_use":
                            tool_calls.append({
                                "id": block.get("id", ""),
                                "type": "function",
                                "function": {
                                    "name": block.get("name", ""),
                                    "arguments": json.dumps(
                                        block.get("input", {}),
                                        ensure_ascii=False,
                                    ),
                                },
                            })
                    elif isinstance(block, str):
                        text_parts.append(block)

                # If there are tool_calls, emit as OpenAI-style assistant + tool_calls
                if tool_calls:
                    assistant_text = "\n".join(text_parts) if text_parts else ""
                    warp_msgs.append({
                        "role": "assistant",
                        "content": assistant_text,
                        "tool_calls": tool_calls,
                    })
                else:
                    warp_msgs.append({
                        "role": "assistant",
                        "content": "\n".join(text_parts),
                    })
            else:
                warp_msgs.append({
                    "role": "assistant",
                    "content": _serialize_content_blocks(content),
                })

        elif role == "user":
            # Check for tool_result blocks
            if isinstance(content, list):
                tool_results: list[dict[str, Any]] = []
                text_parts_u: list[str] = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "tool_result":
                            tool_results.append(block)
                        elif block.get("type") == "text":
                            text_parts_u.append(block.get("text", ""))
                    elif isinstance(block, str):
                        text_parts_u.append(block)

                # Emit tool results as OpenAI-style "tool" messages
                for tr in tool_results:
                    inner = tr.get("content", "")
                    if isinstance(inner, list):
                        serialized_parts = []
                        for sub in inner:
                            if isinstance(sub, dict) and sub.get("type") == "text":
                                serialized_parts.append(sub.get("text", ""))
                            elif isinstance(sub, str):
                                serialized_parts.append(sub)
                        inner = "\n".join(serialized_parts)
                    warp_msgs.append({
                        "role": "tool",
                        "tool_call_id": tr.get("tool_use_id", ""),
                        "content": str(inner),
                    })

                # Also emit any remaining text as a user message
                if text_parts_u:
                    warp_msgs.append({
                        "role": "user",
                        "content": "\n".join(text_parts_u),
                    })
            else:
                warp_msgs.append({
                    "role": "user",
                    "content": _serialize_content_blocks(content),
                })
        else:
            # Pass through any other role as-is
            warp_msgs.append({
                "role": role,
                "content": _serialize_content_blocks(content),
            })

    return warp_msgs


def _convert_tools_to_openai(anthropic_tools: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """
    Convert Anthropic tool definitions to OpenAI function-calling format
    that Warp understands.
    """
    if not anthropic_tools:
        return []
    openai_tools: list[dict[str, Any]] = []
    for tool in anthropic_tools:
        openai_tools.append({
            "type": "function",
            "function": {
                "name": tool.get("name", ""),
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {}),
            },
        })
    return openai_tools


def _serialize_history_to_text(history: list[ChatMessage]) -> str | None:
    """将多轮对话历史序列化为文本，注入 system prompt（无状态模式）。

    跳过 system 消息和尾部当前输入（与 attach_user_and_tools_to_inputs 对齐）。
    """
    non_system = [m for m in history if m.role != "system"]
    if len(non_system) <= 1:
        return None

    if non_system[-1].role == "tool":
        split_idx = len(non_system)
        while split_idx > 0 and non_system[split_idx - 1].role == "tool":
            split_idx -= 1
        history_msgs = non_system[:split_idx]
    else:
        history_msgs = non_system[:-1]

    if not history_msgs:
        return None

    lines: list[str] = []
    for m in history_msgs:
        text = segments_to_text(normalize_content_to_list(m.content))
        if m.role == "user":
            lines.append(f"User: {text}")
        elif m.role == "assistant":
            if text:
                lines.append(f"Assistant: {text}")
            for tc in (m.tool_calls or []):
                fn = tc.get("function") or {}
                tc_name = fn.get("name", "unknown")
                tc_args = fn.get("arguments", "{}")
                lines.append(f"Assistant: [called tool: {tc_name}({tc_args})]")
        elif m.role == "tool":
            lines.append(f"Tool result ({m.tool_call_id or 'unknown'}): {text[:500]}")

    if not lines:
        return None
    return (
        "[Previous conversation]\n"
        + "\n".join(lines)
        + "\n[End of previous conversation]\n\n"
        + "Continue the conversation naturally based on the above context."
    )


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@anthropic_router.post("/v1/messages")
async def anthropic_messages(request: Request) -> StreamingResponse:
    """Handle Anthropic Messages API requests."""
    # --- Auth ---
    await authenticate_request(request)

    # --- Parse body ---
    body = await request.json()
    try:
        req = AnthropicMessagesRequest(**body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    # --- Token acquisition (mirrors router.py pattern) ---
    selector = AccountSelector(ACCOUNT_DB_PATH)
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)

    account = selector.select_account()
    if not account:
        raise HTTPException(status_code=503, detail="No available account with remaining quota")

    access_token = await token_mgr.get_valid_token(account)

    # 如果当前账号 token 为空，尝试换号（最多 2 次）
    _tried_ids: set[int] = {account["id"]}
    for _retry in range(2):
        if access_token:
            break
        logger.warning("[Anthropic] account_id=%d token 为空，尝试换号 (%d/2)", account["id"], _retry + 1)
        account = selector.select_account()
        if not account or account["id"] in _tried_ids:
            break
        _tried_ids.add(account["id"])
        access_token = await token_mgr.get_valid_token(account)

    # 回退机制：多账号全失败时，走原始单账号 get_valid_jwt()
    _used_fallback = False
    if not access_token:
        try:
            from warp2protobuf.core.auth import get_valid_jwt

            access_token = await get_valid_jwt()
            _used_fallback = True
            logger.info("[Anthropic] Fallback get_valid_jwt() succeeded")
        except Exception as exc:
            logger.warning("[Anthropic] Fallback get_valid_jwt() failed: %s", exc)

    if not access_token:
        raise HTTPException(status_code=503, detail="No valid token available")

    account_id = account["id"] if account else 0

    # 在锁保护下设置 JWT
    async with token_mgr.env_lock:
        os.environ["WARP_JWT"] = access_token

    try:
        initialize_once()
    except Exception as e:
        logger.warning("[Anthropic] initialize_once failed or skipped: %s", e)

    # --- Model mapping (use shared resolve_model, same as OpenAI router) ---
    warp_model = resolve_model(req.model)
    logger.info("Anthropic model '%s' → Warp model '%s'", req.model, warp_model)

    # --- Convert messages ---
    raw_messages = [m if isinstance(m, dict) else m.model_dump() for m in req.messages]
    warp_messages = _build_warp_messages(raw_messages)

    # --- System prompt ---
    final_system = _extract_system_prompt(req)

    # --- Convert tools ---
    raw_tools = None
    if req.tools:
        raw_tools = [t if isinstance(t, dict) else t.model_dump() for t in req.tools]
    openai_tools = _convert_tools_to_openai(raw_tools)

    # --- Build packet (mirrors router.py pattern) ---
    packet = packet_template()

    # Stateless mode
    packet["task_context"] = {}

    # Set model (key is "base", not "base_model")
    packet["settings"]["model_config"]["base"] = warp_model

    # --- Convert warp_messages (dicts) to ChatMessage objects ---
    chat_messages: list[ChatMessage] = []
    for msg in warp_messages:
        chat_messages.append(ChatMessage(
            role=msg["role"],
            content=msg.get("content", ""),
            tool_call_id=msg.get("tool_call_id"),
            tool_calls=msg.get("tool_calls"),
            name=msg.get("name"),
        ))

    # --- Serialize history to text for stateless mode (multi-turn support) ---
    history_text = _serialize_history_to_text(chat_messages)
    if history_text:
        if final_system:
            final_system = final_system + "\n\n" + history_text
        else:
            final_system = history_text

    # --- Attach last message + system prompt to packet inputs ---
    attach_user_and_tools_to_inputs(packet, chat_messages, final_system)

    # --- Set tools via mcp_context (mirrors router.py pattern) ---
    if openai_tools:
        mcp_tools: list[dict[str, Any]] = []
        for t in openai_tools:
            func = t.get("function", {})
            mcp_tools.append({
                "name": func.get("name", ""),
                "description": func.get("description", ""),
                "input_schema": func.get("parameters", {}),
            })
        if mcp_tools:
            packet.setdefault("mcp_context", {}).setdefault("tools", []).extend(mcp_tools)

    logger.info(
        "Anthropic request: model=%s, messages=%d (ChatMessage), tools=%d (mcp_context)",
        req.model,
        len(chat_messages),
        len(openai_tools),
    )

    # --- Stream response (with record_usage, mirrors router.py) ---
    async def _stream_with_usage():
        try:
            async for chunk in stream_anthropic_sse(packet, model=req.model, access_token=access_token):
                yield chunk
            # 流式成功，记录使用
            selector.record_usage(account_id, 1)
        except Exception as exc:
            logger.error("[Anthropic] stream error: %s", exc)
            raise

    return StreamingResponse(
        _stream_with_usage(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
