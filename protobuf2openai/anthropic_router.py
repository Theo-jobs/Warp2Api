"""
Anthropic Messages API → Warp bridge router.

Accepts Anthropic-format requests at /v1/messages, converts them to
Warp packet format, and streams back Anthropic-format SSE responses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import time
import uuid
from typing import Any

import httpx

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from warp2protobuf.config.settings import (
    ACCOUNT_DB_PATH,
    HISTORY_TOOL_RESULT_MAX_CHARS,
)
from warp2protobuf.core.account_selector import AccountSelector
from warp2protobuf.core.db import get_connection
from warp2protobuf.config.models import resolve_model

from .anthropic_models import AnthropicMessagesRequest, AnthropicUsage
from .auth import authenticate_request
from .anthropic_sse import stream_anthropic_sse
from .bridge import initialize_once
from .config import BRIDGE_BASE_URL
from .helpers import normalize_content_to_list, segments_to_text
from .models import ChatMessage
from .packets import attach_user_and_tools_to_inputs, packet_template
from .state import STATE
from .token_manager import TokenManager
from .utils import serialize_history_to_text

# ---- 全局 bridge 请求并发控制 ----
# 限制同时打到 Warp bridge 的请求数，防止多 agent 并发导致 429 雪崩
_BRIDGE_SEMAPHORE = asyncio.Semaphore(5)
# 全局 bridge 429 冷却（秒）
_bridge_429_until: float = 0.0
_bridge_429_lock = asyncio.Lock()
_BRIDGE_429_COOLDOWN = 30  # bridge 429 后冷却 30 秒
_BRIDGE_MAX_WAIT = 90  # 排队最大等待时间（秒），超时才返回 429
_SEMAPHORE_ACQUIRE_TIMEOUT = 60  # semaphore 获取超时（秒），超时返回错误而非空流

from .utils import safe_create_task


def _is_bridge_throttled() -> bool:
    return time.time() < _bridge_429_until


async def _set_bridge_throttled() -> None:
    global _bridge_429_until
    async with _bridge_429_lock:
        _bridge_429_until = time.time() + _BRIDGE_429_COOLDOWN
    logging.getLogger(__name__).warning(
        "[Anthropic] Bridge 429 全局冷却已触发，%ds 内新请求排队等待", _BRIDGE_429_COOLDOWN
    )


async def _wait_for_bridge_ready() -> bool:
    """等待 bridge 冷却结束。返回 True 表示就绪，False 表示超时。"""
    if not _is_bridge_throttled():
        return True
    wait_until = time.time() + _BRIDGE_MAX_WAIT
    remaining = _bridge_429_until - time.time()
    logging.getLogger(__name__).info(
        "[Anthropic] 请求排队等待 bridge 冷却，预计 %.0fs", remaining
    )
    while _is_bridge_throttled():
        if time.time() > wait_until:
            return False
        await asyncio.sleep(2)  # 每 2 秒检查一次
    return True


# SSE chunk 中提取 token 统计的正则
_RE_INPUT_TOKENS = re.compile(r'"input_tokens"\s*:\s*(\d+)')
_RE_OUTPUT_TOKENS = re.compile(r'"output_tokens"\s*:\s*(\d+)')


def _estimate_input_tokens(req: "AnthropicMessagesRequest") -> int:
    """从请求体估算 input tokens（Warp 不返回真实值，4 字符 ≈ 1 token）。"""
    total_chars = 0
    # system prompt
    if req.system:
        if isinstance(req.system, str):
            total_chars += len(req.system)
        elif isinstance(req.system, list):
            for block in req.system:
                text = getattr(block, "text", "") if not isinstance(block, dict) else block.get("text", "")
                total_chars += len(text)
    # messages（AnthropicMessage 是 Pydantic 对象，用属性访问）
    for msg in req.messages:
        content = msg.content if hasattr(msg, "content") else ""
        if isinstance(content, str):
            total_chars += len(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    total_chars += len(block.get("text", ""))
                elif hasattr(block, "text"):
                    total_chars += len(block.text)
    return max(1, total_chars // 4)


def _extract_usage_from_chunk(
    chunk: str, cur_input: int, cur_output: int
) -> tuple[int, int]:
    """从 SSE chunk 文本中提取 input_tokens / output_tokens，返回更新后的累计值。"""
    # message_start 事件包含 input_tokens
    if "message_start" in chunk:
        m = _RE_INPUT_TOKENS.search(chunk)
        if m:
            cur_input = max(cur_input, int(m.group(1)))
    # message_delta 事件包含 output_tokens
    if "message_delta" in chunk:
        m = _RE_OUTPUT_TOKENS.search(chunk)
        if m:
            cur_output = max(cur_output, int(m.group(1)))
    return cur_input, cur_output


def _get_account_by_id_with_tokens(db_path: str, account_id: int) -> dict[str, Any] | None:
    """按账号 ID 获取完整账号信息（含 id_token/refresh_token）。"""
    with get_connection(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id, email, local_id, id_token, refresh_token, api_key,
                status, total_limit, used_limit, use_count, last_used
            FROM accounts
            WHERE id = ?
            """,
            (account_id,),
        )
        row = cursor.fetchone()
    return dict(row) if row else None

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


def _tool_result_content_to_text(inner: Any) -> str:
    """Serialize tool_result content without dropping structured blocks."""
    if isinstance(inner, str):
        return inner

    if isinstance(inner, list):
        normalized_inner: list[dict[str, Any]] = []
        for sub in inner:
            if isinstance(sub, dict):
                normalized_inner.append(sub)
            elif isinstance(sub, str):
                normalized_inner.append({"type": "text", "text": sub})
        return segments_to_text(normalized_inner)

    if isinstance(inner, dict):
        normalized = normalize_content_to_list(inner)
        if normalized:
            return segments_to_text(normalized)
        return segments_to_text([inner])

    return str(inner)


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
                    parts.append(_tool_result_content_to_text(block.get("content", "")))
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
            inner = _tool_result_content_to_text(block.get("content", ""))
            results.append({"tool_use_id": tool_use_id, "content": inner})
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
            # Check for tool_result / text / image blocks
            if isinstance(content, list):
                tool_results: list[dict[str, Any]] = []
                text_parts_u: list[str] = []
                image_segments: list[dict[str, Any]] = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "tool_result":
                            tool_results.append(block)
                        elif block.get("type") == "text":
                            text_parts_u.append(block.get("text", ""))
                        elif block.get("type") == "image":
                            # Convert Anthropic image block to OpenAI image_url format
                            # so downstream extract_images_from_segments() can pick it up
                            source = block.get("source", {})
                            if isinstance(source, dict) and source.get("type") == "base64":
                                media_type = source.get("media_type", "image/png")
                                data = source.get("data", "")
                                if data:
                                    image_segments.append({
                                        "type": "image_url",
                                        "image_url": {
                                            "url": f"data:{media_type};base64,{data}"
                                        },
                                    })
                    elif isinstance(block, str):
                        text_parts_u.append(block)

                # Emit tool results as OpenAI-style "tool" messages
                for tr in tool_results:
                    inner = _tool_result_content_to_text(tr.get("content", ""))
                    warp_msgs.append({
                        "role": "tool",
                        "tool_call_id": tr.get("tool_use_id", ""),
                        "content": inner,
                    })

                # Emit user message; use list format when images are present
                if text_parts_u or image_segments:
                    if image_segments:
                        user_content: list[dict[str, Any]] = []
                        if text_parts_u:
                            user_content.append({"type": "text", "text": "\n".join(text_parts_u)})
                        user_content.extend(image_segments)
                        warp_msgs.append({
                            "role": "user",
                            "content": user_content,
                        })
                    else:
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


def _resolve_thinking_model(model: str, thinking_enabled: bool) -> str:
    """Choose thinking variant model when requested and available.

    - 4.5 系列：切换到独立的 -thinking 变体
    - 4.6 系列：thinking 能力内置于 -high/-max，升级到 -max
    - 其他模型：graceful degradation，忽略 thinking 而非报错
    """
    if not thinking_enabled:
        return model

    lower = model.lower()
    if "thinking" in lower:
        return model

    # 4.5 系列：有独立 thinking 变体
    if lower.startswith("claude-4-5-sonnet"):
        return "claude-4-5-sonnet-thinking"
    if lower.startswith("claude-4-5-opus"):
        return "claude-4-5-opus-thinking"

    # 4.6 系列：thinking 内置于 -high/-max，升级到 -max
    if lower.startswith("claude-4-6-opus"):
        logger.info("[thinking] 4.6 opus requested with thinking, using -max variant")
        return "claude-4-6-opus-max"
    if lower.startswith("claude-4-6-sonnet"):
        logger.info("[thinking] 4.6 sonnet requested with thinking, using -max variant")
        return "claude-4-6-sonnet-max"

    # 其他模型：graceful degradation，不阻断请求
    logger.warning(
        "[thinking] no thinking variant for model '%s', proceeding without thinking",
        model,
    )
    return model


def _anthropic_nonstream_response_from_bridge(
    bridge_resp: dict[str, Any],
    model: str,
) -> tuple[dict[str, Any], bool]:
    """Build Anthropic non-stream response from /api/warp/send_stream result.

    Returns:
        (response_dict, hit_quota_limit) — hit_quota_limit 为 True 表示服务端返回额度耗尽。
    """
    text_parts: list[str] = []
    tool_blocks: list[dict[str, Any]] = []
    stop_reason: str = "end_turn"
    _hit_quota_limit = False

    usage = AnthropicUsage()

    parsed_events = bridge_resp.get("parsed_events")
    if isinstance(parsed_events, list):
        for ev in parsed_events:
            parsed = ev.get("parsed_data") if isinstance(ev, dict) else {}
            if not isinstance(parsed, dict):
                continue

            client_actions = parsed.get("client_actions") or parsed.get("clientActions")
            actions = client_actions.get("actions", []) if isinstance(client_actions, dict) else []

            for action in actions:
                if not isinstance(action, dict):
                    continue

                append_data = action.get("append_to_message_content") or action.get("appendToMessageContent")
                if isinstance(append_data, dict):
                    message = append_data.get("message", {})
                    if isinstance(message, dict):
                        agent_output = message.get("agent_output") or message.get("agentOutput") or {}
                        if isinstance(agent_output, dict):
                            text = agent_output.get("text")
                            if isinstance(text, str) and text:
                                text_parts.append(text)

                add_messages = action.get("add_messages_to_task") or action.get("addMessagesToTask")
                if isinstance(add_messages, dict):
                    for message in add_messages.get("messages", []):
                        if not isinstance(message, dict):
                            continue

                        tool_call = message.get("tool_call") or message.get("toolCall") or {}
                        call_mcp = tool_call.get("call_mcp_tool") or tool_call.get("callMcpTool") or {}
                        if isinstance(call_mcp, dict) and call_mcp.get("name"):
                            args_obj = call_mcp.get("args", {})
                            args_obj = args_obj if isinstance(args_obj, dict) else {}
                            tool_blocks.append({
                                "type": "tool_use",
                                "id": tool_call.get("tool_call_id") or f"toolu_{uuid.uuid4().hex[:24]}",
                                "name": call_mcp.get("name", "unknown"),
                                "input": args_obj,
                            })

            finished = parsed.get("finished")
            if isinstance(finished, dict):
                token_usage = finished.get("token_usage") or finished.get("tokenUsage") or []
                if isinstance(token_usage, list) and token_usage:
                    usage0 = token_usage[0] if isinstance(token_usage[0], dict) else {}
                    total_input = usage0.get("total_input") or usage0.get("totalInput")
                    output = usage0.get("output")
                    input_cache_read = usage0.get("input_cache_read") or usage0.get("inputCacheRead")
                    input_cache_write = usage0.get("input_cache_write") or usage0.get("inputCacheWrite")

                    if isinstance(total_input, (int, float)):
                        usage.input_tokens = max(0, int(total_input))
                    if isinstance(output, (int, float)):
                        usage.output_tokens = max(0, int(output))
                    if isinstance(input_cache_read, (int, float)):
                        usage.cache_read_input_tokens = max(0, int(input_cache_read))
                    if isinstance(input_cache_write, (int, float)):
                        usage.cache_creation_input_tokens = max(0, int(input_cache_write))

                reason = finished.get("reason")
                if isinstance(reason, dict):
                    if "max_token_limit" in reason or "maxTokenLimit" in reason:
                        stop_reason = "max_tokens"
                    elif "context_window_exceeded" in reason or "contextWindowExceeded" in reason:
                        stop_reason = "max_tokens"
                    elif "quota_limit" in reason or "quotaLimit" in reason:
                        stop_reason = "end_turn"
                        _hit_quota_limit = True
                    elif "llm_unavailable" in reason or "llmUnavailable" in reason:
                        stop_reason = "end_turn"
                    elif "internal_error" in reason or "internalError" in reason:
                        stop_reason = "end_turn"
                    elif "done" in reason or "other" in reason:
                        stop_reason = "tool_use" if tool_blocks else "end_turn"
                    else:
                        stop_reason = "tool_use" if tool_blocks else "end_turn"

    text = "".join(text_parts).strip()

    content_blocks: list[dict[str, Any]] = []
    if text:
        content_blocks.append({"type": "text", "text": text})
    content_blocks.extend(tool_blocks)

    if not content_blocks:
        fallback_text = bridge_resp.get("response")
        if isinstance(fallback_text, str) and fallback_text.strip():
            content_blocks.append({"type": "text", "text": fallback_text})

    if tool_blocks and stop_reason == "end_turn":
        stop_reason = "tool_use"

    return {
        "id": f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content_blocks,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage.model_dump(),
    }, _hit_quota_limit


async def _auto_sync_quota_on_empty(token_mgr: "TokenManager") -> None:
    """无可用账号时自动触发额度同步（最多 3 个 used_limit 最高的 available 账号）。

    目的：本地 used_limit 可能虚高，同步真实额度后可能恢复可用。
    """
    try:
        with get_connection(str(token_mgr.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT id, email, id_token, refresh_token
                   FROM accounts
                   WHERE status = 'available'
                     AND refresh_token IS NOT NULL AND refresh_token != ''
                   ORDER BY used_limit DESC
                   LIMIT 3"""
            ).fetchall()
        if not rows:
            return
        from warp2protobuf.core.auth import refresh_access_token_with_refresh_token
        for row in rows:
            try:
                token = row["id_token"]
                if not token or token == "":
                    token = await refresh_access_token_with_refresh_token(row["refresh_token"])
                if token:
                    await token_mgr.sync_account_quota(row["id"], token)
            except Exception as e:
                logger.debug("[AutoSync] account_id=%d failed: %s", row["id"], e)
        logger.info("[AutoSync] Triggered quota sync for %d accounts on 503", len(rows))
    except Exception as e:
        logger.warning("[AutoSync] Failed: %s", e)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Count tokens (stub) — CLI 会调用此端点预估 token 数
# ---------------------------------------------------------------------------

@anthropic_router.post("/v1/messages/count_tokens")
async def count_tokens(request: Request) -> JSONResponse:
    """Stub: 粗略估算 input tokens，避免 CLI Proxy 收到 404。"""
    await authenticate_request(request)
    body = await request.json()

    # 粗略估算：将所有 messages 文本拼接，按 4 字符 ≈ 1 token 估算
    total_chars = 0
    for msg in body.get("messages", []):
        content = msg.get("content", "")
        if isinstance(content, str):
            total_chars += len(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    total_chars += len(block.get("text", ""))
    # system prompt
    system = body.get("system", "")
    if isinstance(system, str):
        total_chars += len(system)
    elif isinstance(system, list):
        for block in system:
            if isinstance(block, dict):
                total_chars += len(block.get("text", ""))

    estimated_tokens = max(1, total_chars // 4)
    return JSONResponse({"input_tokens": estimated_tokens})


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@anthropic_router.post("/v1/messages")
async def anthropic_messages(request: Request) -> Any:
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
    # bridge 冷却中则排队等待（最多 90 秒），超时才返回 429
    if _is_bridge_throttled():
        ready = await _wait_for_bridge_ready()
        if not ready:
            raise HTTPException(
                status_code=429,
                detail="Rate limited, all retries exhausted",
                headers={"Retry-After": "30"},
            )

    selector = AccountSelector(ACCOUNT_DB_PATH)
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)

    account = selector.select_account()
    if not account:
        # 无可用账号 → 自动触发一次快速额度同步（fire-and-forget），可能是本地计数虚高
        if not token_mgr.is_firebase_blocked():
            safe_create_task(_auto_sync_quota_on_empty(token_mgr))
        if token_mgr.is_firebase_blocked():
            remaining = int((TokenManager._firebase_blocked_until - time.time()) / 60)
            raise HTTPException(
                status_code=503,
                detail=f"All account tokens expired (Firebase rate limited), cooldown {remaining} min remaining",
            )
        raise HTTPException(status_code=503, detail="No available account with remaining quota")

    access_token = await token_mgr.get_valid_token(account)

    # 如果当前账号 token 为空，尝试换号（最多 2 次），排除已尝试的账号
    _tried_ids: set[int] = {account["id"]}
    for _retry in range(2):
        if access_token:
            break
        logger.warning("[Anthropic] account_id=%d token 为空，尝试换号 (%d/2)", account["id"], _retry + 1)
        account = selector.select_account(exclude_ids=_tried_ids)
        if not account:
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
        if token_mgr.is_firebase_blocked():
            remaining = int((TokenManager._firebase_blocked_until - time.time()) / 60)
            raise HTTPException(
                status_code=503,
                detail=f"All token refresh attempts failed (Firebase rate limited), cooldown {remaining} min remaining",
            )
        raise HTTPException(status_code=503, detail="No valid token available")

    account_id = account["id"] if account else 0

    try:
        await initialize_once()
    except Exception as e:
        logger.warning("[Anthropic] initialize_once failed or skipped: %s", e)

    # --- Model mapping (use shared resolve_model, same as OpenAI router) ---
    warp_model = resolve_model(req.model)

    # thinking 配置：若请求启用 thinking 且模型存在 thinking 变体，则切换。
    # 当前 protobuf request 不暴露 thinking budget 字段，budget 暂仅用于上层兼容验证。
    thinking_budget_tokens = req.thinking.budget_tokens if req.thinking else None
    if req.thinking and req.thinking.type != "enabled":
        raise HTTPException(status_code=400, detail="Unsupported thinking.type; only 'enabled' is accepted")
    thinking_enabled = bool(req.thinking)
    warp_model = _resolve_thinking_model(warp_model, thinking_enabled)

    logger.info(
        "Anthropic model '%s' → Warp model '%s' (thinking=%s budget_tokens=%s)",
        req.model,
        warp_model,
        thinking_enabled,
        thinking_budget_tokens,
    )

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

    # 有状态对话模式：复用 conversation_id（如果有），每次请求独立 task_id
    _new_task_id = str(uuid.uuid4())
    packet["task_context"] = {"active_task_id": _new_task_id}
    # 注入 conversation_id 到 metadata（如果 warmup 或之前的请求已获取）
    if STATE.conversation_id:
        packet.setdefault("metadata", {})["conversation_id"] = STATE.conversation_id
        logger.debug("[Anthropic] 复用 conversation_id=%s, new task_id=%s", STATE.conversation_id, _new_task_id)

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
    history_text = serialize_history_to_text(chat_messages, max_tool_chars=HISTORY_TOOL_RESULT_MAX_CHARS)
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
        "Anthropic request: model=%s, messages=%d (ChatMessage), tools=%d (mcp_context), stream=%s",
        req.model,
        len(chat_messages),
        len(openai_tools),
        req.stream,
    )

    if req.stream:
        # --- 估算 input tokens（Warp 不返回真实值，用字符数 / 4 近似）---
        _est_input = _estimate_input_tokens(req)

        # --- Stream response (with semaphore + token tracking) ---
        _stream_access_token = access_token
        _stream_account_id = account_id

        async def _stream_with_usage():
            nonlocal _stream_access_token, _stream_account_id
            _input_tokens = 0
            _output_tokens = 0
            _hit_quota = False

            # ---- semaphore 获取（带超时 + 心跳保活） ----
            _acquired = False
            try:
                _acquired = _BRIDGE_SEMAPHORE._value > 0  # 快速检查
                if not _acquired:
                    logger.info("[Anthropic] semaphore 已满，排队等待（account_id=%d）", _stream_account_id)
                    # 等待期间每 8 秒发送 SSE comment 保活
                    _deadline = time.time() + _SEMAPHORE_ACQUIRE_TIMEOUT
                    while not _acquired:
                        try:
                            await asyncio.wait_for(_BRIDGE_SEMAPHORE.acquire(), timeout=8)
                            _acquired = True
                        except asyncio.TimeoutError:
                            if time.time() > _deadline:
                                logger.warning("[Anthropic] semaphore 等待超时 %ds，account_id=%d", _SEMAPHORE_ACQUIRE_TIMEOUT, _stream_account_id)
                                yield 'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"Server busy, please retry later"}}\n\n'
                                return
                            yield ": keepalive\n\n"
                else:
                    await _BRIDGE_SEMAPHORE.acquire()
                    _acquired = True
            except Exception as exc:
                logger.error("[Anthropic] semaphore acquire error: %s", exc)
                yield f'event: error\ndata: {{"type":"error","error":{{"type":"api_error","message":"Internal error: {exc}"}}}}\n\n'
                return

            try:
                _chunk_idx = 0
                _got_429 = False
                _got_auth_error = False
                _buffered: list[str] = []
                _429_DETECT_CHUNKS = 3  # 429/401/403 检测只需前 3 个 chunk
                try:
                    async for chunk in stream_anthropic_sse(packet, model=req.model, access_token=_stream_access_token, estimated_input_tokens=_est_input):
                        # 内部元数据信号（不是 SSE 事件），不 yield 给客户端
                        if chunk.startswith("\x00"):
                            if "__QUOTA_LIMIT__" in chunk:
                                _hit_quota = True
                            elif "__CONV_ID__:" in chunk:
                                _conv_id = chunk.split("__CONV_ID__:", 1)[1]
                                STATE.conversation_id = _conv_id
                            elif "__TASK_ID__:" in chunk:
                                _task_id = chunk.split("__TASK_ID__:", 1)[1]
                                if _task_id:
                                    STATE.baseline_task_id = _task_id
                            continue
                        # 检测前几个 chunk 是否有 429 或 401/403
                        if _chunk_idx < _429_DETECT_CHUNKS:
                            _buffered.append(chunk)
                            if "Bridge error: HTTP 429" in chunk:
                                _got_429 = True
                                break
                            if "Bridge error: HTTP 401" in chunk or "Bridge error: HTTP 403" in chunk:
                                _got_auth_error = True
                                break
                            _chunk_idx += 1
                            continue
                        # flush buffer
                        if _buffered:
                            for buf in _buffered:
                                _input_tokens, _output_tokens = _extract_usage_from_chunk(
                                    buf, _input_tokens, _output_tokens
                                )
                                yield buf
                            _buffered.clear()
                        # 解析 token 统计
                        _input_tokens, _output_tokens = _extract_usage_from_chunk(
                            chunk, _input_tokens, _output_tokens
                        )
                        yield chunk

                    if not _got_429 and not _got_auth_error:
                        # flush 残余 buffer
                        for buf in _buffered:
                            _input_tokens, _output_tokens = _extract_usage_from_chunk(
                                buf, _input_tokens, _output_tokens
                            )
                            yield buf
                        # 用真实 token 数记录使用
                        total_tokens = _input_tokens + _output_tokens
                        selector.record_usage(_stream_account_id, 1)
                        # fire-and-forget: 同步真实额度（不同模型消耗不同 credits）
                        safe_create_task(token_mgr.sync_account_quota(_stream_account_id, _stream_access_token))
                        logger.info(
                            "[Anthropic] stream done: account_id=%d input=%d output=%d total=%d",
                            _stream_account_id, _input_tokens, _output_tokens, total_tokens,
                        )
                        # 额度耗尽 → 标记 exhausted
                        if _hit_quota:
                            logger.warning("[Anthropic] stream quota_limit hit on account_id=%d, marking exhausted", _stream_account_id)
                            token_mgr.mark_account_exhausted(_stream_account_id)
                        return

                    # ---- 401/403 → 标记账号吊销，自动换号重试一次 ----
                    if _got_auth_error:
                        logger.warning(
                            "[Anthropic] stream 401/403 on account_id=%d, marking revoked and retrying with new account",
                            _stream_account_id,
                        )
                        token_mgr.mark_account_revoked(_stream_account_id)
                        # 尝试换号
                        _new_account = selector.select_account(exclude_ids=_tried_ids)
                        if _new_account:
                            _tried_ids.add(_new_account["id"])
                            _new_token = await token_mgr.get_valid_token(_new_account)
                            if _new_token:
                                _stream_account_id = _new_account["id"]
                                _stream_access_token = _new_token
                                _buffered.clear()
                                logger.info("[Anthropic] 401/403 换号重试: new account_id=%d", _stream_account_id)
                                # 用新 token 重试流
                                async for chunk in stream_anthropic_sse(packet, model=req.model, access_token=_stream_access_token, estimated_input_tokens=_est_input):
                                    if chunk.startswith("\x00"):
                                        if "__QUOTA_LIMIT__" in chunk:
                                            _hit_quota = True
                                        elif "__CONV_ID__:" in chunk:
                                            STATE.conversation_id = chunk.split("__CONV_ID__:", 1)[1]
                                        elif "__TASK_ID__:" in chunk:
                                            _tid = chunk.split("__TASK_ID__:", 1)[1]
                                            if _tid:
                                                STATE.baseline_task_id = _tid
                                        continue
                                    _input_tokens, _output_tokens = _extract_usage_from_chunk(
                                        chunk, _input_tokens, _output_tokens
                                    )
                                    yield chunk
                                # 重试成功
                                total_tokens = _input_tokens + _output_tokens
                                selector.record_usage(_stream_account_id, 1)
                                safe_create_task(token_mgr.sync_account_quota(_stream_account_id, _stream_access_token))
                                logger.info(
                                    "[Anthropic] 401/403 retry stream done: account_id=%d input=%d output=%d total=%d",
                                    _stream_account_id, _input_tokens, _output_tokens, total_tokens,
                                )
                                if _hit_quota:
                                    token_mgr.mark_account_exhausted(_stream_account_id)
                                return
                        # 换号失败，返回错误
                        yield 'event: error\ndata: {"type":"error","error":{"type":"authentication_error","message":"API key revoked (401/403), no fallback account available"}}\n\n'
                        return

                    # 429 → 触发全局冷却，直接返回错误给 CC（不内部重试）
                    await _set_bridge_throttled()
                    token_mgr.mark_account_failed(_stream_account_id)
                    logger.warning(
                        "[Anthropic] stream 429 on account_id=%d, returning to client (no internal retry)",
                        _stream_account_id,
                    )
                    yield 'event: error\ndata: {"type":"error","error":{"type":"rate_limit_error","message":"Rate limited, please retry after 30s"}}\n\n'

                except Exception as exc:
                    logger.error("[Anthropic] stream error: %s", exc)
                    raise
            finally:
                if _acquired:
                    _BRIDGE_SEMAPHORE.release()

        return StreamingResponse(
            _stream_with_usage(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # --- Non-stream response ---
    bridge_url = f"{BRIDGE_BASE_URL}/api/warp/send_stream"
    req_body: dict[str, Any] = {
        "json_data": packet,
        "message_type": "warp.multi_agent.v1.Request",
        "access_token": access_token,
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, read=180.0)) as client:
            resp = await client.post(bridge_url, json=req_body)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"bridge_unreachable: {exc}") from exc

    # --- 429 限流：触发冷却，排队等待后重试一次 ---
    if resp.status_code == 429:
        await _set_bridge_throttled()
        token_mgr.mark_account_failed(account_id)
        logger.warning("[Anthropic] non-stream 429 on account_id=%d, waiting for cooldown", account_id)
        ready = await _wait_for_bridge_ready()
        if ready:
            # 冷却结束，重试一次
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, read=180.0)) as client:
                    resp = await client.post(bridge_url, json=req_body)
                logger.info("[Anthropic] non-stream 429 retry result: %d", resp.status_code)
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"bridge_unreachable on retry: {exc}") from exc
        if resp.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail="Rate limited, all retries exhausted",
                headers={"Retry-After": "30"},
            )

    if resp.status_code != 200:
        # --- 401/403 吊销防御：标记账号，换号重试一次 ---
        if resp.status_code in (401, 403):
            logger.warning("[Anthropic] non-stream %d on account_id=%d, marking revoked", resp.status_code, account_id)
            token_mgr.mark_account_revoked(account_id)
            _new_account = selector.select_account(exclude_ids=_tried_ids)
            if _new_account:
                _tried_ids.add(_new_account["id"])
                _new_token = await token_mgr.get_valid_token(_new_account)
                if _new_token:
                    logger.info("[Anthropic] non-stream 401/403 换号重试: new account_id=%d", _new_account["id"])
                    req_body["access_token"] = _new_token
                    try:
                        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, read=180.0)) as client:
                            resp = await client.post(bridge_url, json=req_body)
                        if resp.status_code == 200:
                            account_id = _new_account["id"]
                            logger.info("[Anthropic] non-stream 401/403 retry succeeded with account_id=%d", account_id)
                    except Exception as exc:
                        raise HTTPException(status_code=502, detail=f"bridge_unreachable on auth retry: {exc}") from exc
        if resp.status_code != 200:
            detail = resp.text[:300]
            raise HTTPException(status_code=resp.status_code, detail=f"Bridge error: HTTP {resp.status_code} {detail}")

    try:
        bridge_resp = resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Invalid bridge response: {exc}") from exc

    final_payload, hit_quota_limit = _anthropic_nonstream_response_from_bridge(bridge_resp, req.model)

    # 补充 input_tokens 估算（Warp 不返回真实值）
    usage_dict = final_payload.get("usage", {})
    if not usage_dict.get("input_tokens"):
        usage_dict["input_tokens"] = _estimate_input_tokens(req)
        final_payload["usage"] = usage_dict
    # output_tokens 兜底：用响应文本长度估算
    if not usage_dict.get("output_tokens"):
        _out_chars = sum(len(b.get("text", "")) for b in final_payload.get("content", []) if b.get("type") == "text")
        usage_dict["output_tokens"] = max(1, _out_chars // 4)
        final_payload["usage"] = usage_dict

    # 额度耗尽 → 标记 exhausted
    if hit_quota_limit:
        logger.warning("[Anthropic] non-stream quota_limit hit on account_id=%d, marking exhausted", account_id)
        token_mgr.mark_account_exhausted(account_id)

    # 从非流式响应中提取真实 token 统计
    usage = final_payload.get("usage", {})
    total_tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
    selector.record_usage(account_id, 1)
    # fire-and-forget: 同步真实额度（不同模型消耗不同 credits）
    safe_create_task(token_mgr.sync_account_quota(account_id, access_token))
    logger.info(
        "[Anthropic] non-stream done: account_id=%d input=%d output=%d total=%d",
        account_id, usage.get("input_tokens", 0), usage.get("output_tokens", 0), total_tokens,
    )

    return JSONResponse(content=final_payload)


async def build_streaming_response_for_account(
    req: AnthropicMessagesRequest,
    account_id: int,
) -> StreamingResponse:
    """使用指定账号构建 Anthropic SSE 流响应。"""
    selector = AccountSelector(ACCOUNT_DB_PATH)
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)

    account = _get_account_by_id_with_tokens(ACCOUNT_DB_PATH, account_id)
    if not account:
        raise HTTPException(status_code=404, detail=f"Account #{account_id} not found")

    if account.get("status") != "available":
        raise HTTPException(status_code=400, detail=f"Account #{account_id} is not available")

    access_token = await token_mgr.get_valid_token(account)
    if not access_token:
        raise HTTPException(status_code=503, detail=f"Account #{account_id} has no valid token")

    try:
        await initialize_once()
    except Exception as e:
        logger.warning("[AnthropicTest] initialize_once failed or skipped: %s", e)

    warp_model = resolve_model(req.model)
    logger.info(
        "[AnthropicTest] account_id=%d model='%s' → warp='%s'",
        account_id,
        req.model,
        warp_model,
    )

    raw_messages = [m if isinstance(m, dict) else m.model_dump() for m in req.messages]
    warp_messages = _build_warp_messages(raw_messages)

    final_system = _extract_system_prompt(req)

    raw_tools = None
    if req.tools:
        raw_tools = [t if isinstance(t, dict) else t.model_dump() for t in req.tools]
    openai_tools = _convert_tools_to_openai(raw_tools)

    packet = packet_template()
    packet["task_context"] = {"active_task_id": str(uuid.uuid4())}
    packet["settings"]["model_config"]["base"] = warp_model

    chat_messages: list[ChatMessage] = []
    for msg in warp_messages:
        chat_messages.append(ChatMessage(
            role=msg["role"],
            content=msg.get("content", ""),
            tool_call_id=msg.get("tool_call_id"),
            tool_calls=msg.get("tool_calls"),
            name=msg.get("name"),
        ))

    history_text = serialize_history_to_text(chat_messages, max_tool_chars=HISTORY_TOOL_RESULT_MAX_CHARS)
    if history_text:
        if final_system:
            final_system = final_system + "\n\n" + history_text
        else:
            final_system = history_text

    attach_user_and_tools_to_inputs(packet, chat_messages, final_system)

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
        "[AnthropicTest] request: account_id=%d model=%s messages=%d tools=%d",
        account_id,
        req.model,
        len(chat_messages),
        len(openai_tools),
    )

    async def _stream_with_usage() -> Any:
        try:
            async for chunk in stream_anthropic_sse(packet, model=req.model, access_token=access_token):
                yield chunk
            selector.record_usage(account_id, 1)
        except Exception as exc:
            logger.error("[AnthropicTest] stream error account_id=%d: %s", account_id, exc)
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
