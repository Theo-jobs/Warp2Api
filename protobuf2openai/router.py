from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from .logging import logger

from .models import ChatCompletionsRequest, ChatMessage
from .reorder import reorder_messages_for_anthropic
from .helpers import normalize_content_to_list, segments_to_text
from .packets import packet_template, attach_user_and_tools_to_inputs
from .state import STATE
from .config import BRIDGE_BASE_URL
from .bridge import initialize_once
from .sse_transform import stream_openai_sse
from .auth import authenticate_request
from .token_manager import TokenManager

from warp2protobuf.config.models import resolve_model, get_all_unique_models as _get_all_models
from warp2protobuf.config.settings import (
    ACCOUNT_DB_PATH,
    HISTORY_TOOL_RESULT_MAX_CHARS,
)
from warp2protobuf.core.account_context import AccountContext, get_current_account_info

router = APIRouter()


def _extract_http_status_from_error_text(error_text: str) -> Optional[int]:
    """Extract upstream HTTP status from canonical bridge error text."""
    if not error_text:
        return None
    match = re.search(r"Warp\s+API\s+Error\s*\(HTTP\s+([1-5]\d{2})\)", error_text, re.IGNORECASE)
    if not match:
        return None
    try:
        status_code = int(match.group(1))
    except ValueError:
        return None
    if 100 <= status_code <= 599:
        return status_code
    return None


def _serialize_history_to_text(history: List[ChatMessage]) -> Optional[str]:
    """将多轮对话历史序列化为文本，用于注入 system prompt。

    跳过 system 消息和尾部的当前输入（user 或连续 tool_result + 对应 assistant）。
    """
    non_system = [m for m in history if m.role != "system"]
    if len(non_system) <= 1:
        return None  # 没有历史

    # 计算尾部当前输入的范围（与 attach_user_and_tools_to_inputs 对齐）
    if non_system[-1].role == "tool":
        # 尾部连续 tool_result 全部跳过（它们作为结构化 tool_call_result 输入）
        split_idx = len(non_system)
        while split_idx > 0 and non_system[split_idx - 1].role == "tool":
            split_idx -= 1
        # 注意：保留 assistant(tool_calls) 在历史中！
        # Warp 需要知道是哪个 assistant 调了什么工具
        history_msgs = non_system[:split_idx]
    else:
        # 最后一条 user 是当前输入
        history_msgs = non_system[:-1]

    if not history_msgs:
        return None

    lines: List[str] = []
    for m in history_msgs:
        text = segments_to_text(normalize_content_to_list(m.content))
        if m.role == "user":
            lines.append(f"User: {text}")
        elif m.role == "assistant":
            if text:
                lines.append(f"Assistant: {text}")
            for tc in (m.tool_calls or []):
                fn = (tc.get("function") or {})
                tc_name = fn.get("name", "unknown")
                tc_args = fn.get("arguments", "{}")
                lines.append(f"Assistant: [called tool: {tc_name}({tc_args})]")
        elif m.role == "tool":
            max_chars = max(1, HISTORY_TOOL_RESULT_MAX_CHARS)
            lines.append(
                f"Tool result ({m.tool_call_id or 'unknown'}): {text[:max_chars]}"
            )
    if not lines:
        return None
    return (
        "[Previous conversation]\n"
        + "\n".join(lines)
        + "\n[End of previous conversation]\n\n"
        + "Continue the conversation naturally based on the above context."
    )


@router.get("/")
def root():
    return {"service": "OpenAI Chat Completions (Warp bridge) - Streaming", "status": "ok"}


@router.get("/healthz")
def health_check():
    return {"status": "ok", "service": "OpenAI Chat Completions (Warp bridge) - Streaming"}


@router.get("/v1/models")
def list_models():
    """OpenAI-compatible model listing — 直接返回本地模型目录。"""
    return {"object": "list", "data": _get_all_models()}


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionsRequest, request: Request = None):
    # 认证检查
    if request:
        await authenticate_request(request)

    # 选择账号并获取有效 token
    from warp2protobuf.core.account_selector import AccountSelector
    selector = AccountSelector(ACCOUNT_DB_PATH)
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)

    account = selector.select_account()
    if not account:
        raise HTTPException(503, "No available account with remaining quota")

    # 获取有效 token（过期自动刷新；Firebase 限流时返回已有 token）
    valid_token = await token_mgr.get_valid_token(account)

    # 如果当前账号 token 完全为空，尝试换号（最多 2 次，减少 Firebase 压力）
    _tried_ids = {account["id"]}
    for _retry in range(2):
        if valid_token:
            break
        logger.warning(
            "[OpenAI Compat] account_id=%d token 为空，尝试换号 (%d/2)",
            account["id"], _retry + 1,
        )
        account = selector.select_account()
        if not account or account["id"] in _tried_ids:
            break
        _tried_ids.add(account["id"])
        valid_token = await token_mgr.get_valid_token(account)

    # === 回退机制：多账号全失败时，走原始单账号 get_valid_jwt() ===
    _used_fallback = False
    if not valid_token:
        logger.warning(
            "[OpenAI Compat] 所有账号 token 获取失败，回退到原始 get_valid_jwt() 机制"
        )
        try:
            from warp2protobuf.core.auth import get_valid_jwt
            valid_token = await get_valid_jwt()
            _used_fallback = True
            logger.info("[OpenAI Compat] 原始 get_valid_jwt() 回退成功")
        except Exception as fallback_err:
            logger.error(
                "[OpenAI Compat] 原始 get_valid_jwt() 回退也失败: %s", fallback_err
            )

    if not valid_token:
        raise HTTPException(503, "No account with valid token available (all refresh failed)")

    account_id = account["id"] if account else 0
    if account and not _used_fallback:
        account_info = {
            "id": account["id"],
            "email": account["email"],
            "local_id": account["local_id"],
            "use_count": account["use_count"],
            "remaining_limit": account["total_limit"] - account["used_limit"],
        }
        logger.info(
            "[OpenAI Compat] Using account: id=%d email=%s remaining=%d",
            account_info["id"],
            account_info["email"],
            account_info["remaining_limit"],
        )
    else:
        logger.info("[OpenAI Compat] Using fallback JWT (original mechanism)")

    # initialize_once 已在 startup 中完成，此处无需重复
    try:
        await initialize_once()
    except Exception as e:
        logger.warning(f"[OpenAI Compat] initialize_once failed or skipped: {e}")

    if not req.messages:
        raise HTTPException(400, "messages 不能为空")

    # 1) 生产环境避免打印完整请求体，防止敏感信息泄露
    try:
        logger.info(
            "[OpenAI Compat] 接收到 Chat Completions 请求: model=%s stream=%s messages=%d tools=%d",
            req.model,
            req.stream,
            len(req.messages or []),
            len(req.tools or []),
        )
    except Exception:
        logger.info("[OpenAI Compat] 接收到 Chat Completions 请求（摘要日志失败）")

    # 整理消息
    history: List[ChatMessage] = reorder_messages_for_anthropic(list(req.messages))

    # 2) 仅记录整理后的摘要，避免日志落地完整上下文
    try:
        logger.info(
            "[OpenAI Compat] post-reorder 摘要: messages=%d last_role=%s",
            len(history),
            history[-1].role if history else "none",
        )
    except Exception:
        logger.info("[OpenAI Compat] post-reorder 摘要日志失败")

    system_prompt_text: Optional[str] = None
    try:
        chunks: List[str] = []
        for _m in history:
            if _m.role == "system":
                _txt = segments_to_text(normalize_content_to_list(_m.content))
                if _txt.strip():
                    chunks.append(_txt)
        if chunks:
            system_prompt_text = "\n\n".join(chunks)
    except Exception:
        system_prompt_text = None

    task_id = str(uuid.uuid4())
    packet = packet_template()

    # 调试：记录 Claude Code 发来的 tools 名称
    if req.tools:
        tool_names = [t.function.name for t in req.tools[:10]]
        logger.info("[OpenAI Compat] Client tools (first 10): %s", tool_names)

    # 多轮对话：将历史序列化为文本注入 system prompt（T4 方案）
    # 每次都当新会话处理，空 task_context，完全无状态
    history_text = _serialize_history_to_text(history)
    if history_text:
        if system_prompt_text:
            system_prompt_text = system_prompt_text + "\n\n" + history_text
        else:
            system_prompt_text = history_text

    packet["task_context"] = {}

    packet.setdefault("settings", {}).setdefault("model_config", {})
    resolved = resolve_model(req.model)
    packet["settings"]["model_config"]["base"] = resolved
    logger.info("[OpenAI Compat] 模型映射: %s → %s", req.model, resolved)

    attach_user_and_tools_to_inputs(packet, history, system_prompt_text)

    # 调试日志：记录 input 结构
    inputs_list = packet.get("input", {}).get("user_inputs", {}).get("inputs", [])
    input_types = [list(inp.keys())[0] if inp else "empty" for inp in inputs_list]
    logger.info(
        "[OpenAI Compat] Packet inputs: count=%d types=%s",
        len(inputs_list), input_types,
    )

    if req.tools:
        mcp_tools: List[Dict[str, Any]] = []
        for t in req.tools:
            if t.type != "function" or not t.function:
                continue
            mcp_tools.append({
                "name": t.function.name,
                "description": t.function.description or "",
                "input_schema": t.function.parameters or {},
            })
        if mcp_tools:
            packet.setdefault("mcp_context", {}).setdefault("tools", []).extend(mcp_tools)

    # 3) 仅记录发送包摘要，避免完整 payload 泄露
    try:
        input_count = len((((packet.get("input") or {}).get("user_inputs") or {}).get("inputs") or []))
        image_count = len((((packet.get("input") or {}).get("context") or {}).get("images") or []))
        logger.info(
            "[OpenAI Compat] Protobuf JSON 摘要: input_count=%d image_count=%d has_mcp=%s",
            input_count,
            image_count,
            bool((packet.get("mcp_context") or {}).get("tools")),
        )
    except Exception:
        logger.info("[OpenAI Compat] Protobuf JSON 摘要日志失败")

    created_ts = int(time.time())
    completion_id = str(uuid.uuid4())
    model_id = req.model or "warp-default"

    if req.stream:
        async def _agen():
            try:
                async for chunk in stream_openai_sse(packet, completion_id, created_ts, model_id, access_token=valid_token):
                    yield chunk
                # 流式成功，记录使用
                selector.record_usage(account_id, 1)
            except RuntimeError as e:
                err_msg = str(e)
                if "429" in err_msg:
                    token_mgr.mark_account_failed(account_id)
                    logger.warning("[OpenAI Compat] Stream 429, account_id=%d marked failed", account_id)
                # 将错误作为 SSE 事件发送给客户端
                error_chunk = {
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "created": created_ts,
                    "model": model_id,
                    "choices": [{"index": 0, "delta": {"content": f"\n\n[Error: {err_msg}]"}, "finish_reason": "stop"}],
                }
                yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
                yield "data: [DONE]\n\n"
        return StreamingResponse(_agen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})

    # 非流式请求
    def _post_once(_token: str) -> requests.Response:
        return requests.post(
            f"{BRIDGE_BASE_URL}/api/warp/send_stream",
            json={
                "json_data": packet,
                "message_type": "warp.multi_agent.v1.Request",
                "access_token": _token,
            },
            timeout=(5.0, 180.0),
        )

    try:
        resp = _post_once(valid_token)

        if resp.status_code == 429:
            # 标记当前账号失败，尝试换号重试
            token_mgr.mark_account_failed(account_id)
            logger.warning("[OpenAI Compat] 429 from bridge, switching account...")

            retry_account = selector.select_account()
            if retry_account and retry_account["id"] != account_id:
                retry_token = await token_mgr.get_valid_token(retry_account)
                if retry_token:
                    resp = _post_once(retry_token)
                    account_id = retry_account["id"]

        if resp.status_code != 200:
            raise HTTPException(resp.status_code, f"bridge_error: {resp.text}")
        bridge_resp = resp.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"bridge_unreachable: {e}")

    try:
        STATE.conversation_id = bridge_resp.get("conversation_id") or STATE.conversation_id
        ret_task_id = bridge_resp.get("task_id")
        if isinstance(ret_task_id, str) and ret_task_id:
            STATE.baseline_task_id = ret_task_id
    except Exception:
        pass

    tool_calls: List[Dict[str, Any]] = []
    try:
        parsed_events = bridge_resp.get("parsed_events", []) or []
        for ev in parsed_events:
            evd = ev.get("parsed_data") or ev.get("raw_data") or {}
            client_actions = evd.get("client_actions") or evd.get("clientActions") or {}
            actions = client_actions.get("actions") or client_actions.get("Actions") or []
            for action in actions:
                add_msgs = action.get("add_messages_to_task") or action.get("addMessagesToTask") or {}
                if not isinstance(add_msgs, dict):
                    continue
                for message in add_msgs.get("messages", []) or []:
                    tc = message.get("tool_call") or message.get("toolCall") or {}
                    call_mcp = tc.get("call_mcp_tool") or tc.get("callMcpTool") or {}
                    if isinstance(call_mcp, dict) and call_mcp.get("name"):
                        try:
                            args_obj = call_mcp.get("args", {}) or {}
                            args_str = json.dumps(args_obj, ensure_ascii=False)
                        except Exception:
                            args_str = "{}"
                        tool_calls.append({
                            "id": tc.get("tool_call_id") or str(uuid.uuid4()),
                            "type": "function",
                            "function": {"name": call_mcp.get("name"), "arguments": args_str},
                        })
    except Exception:
        pass

    if tool_calls:
        msg_payload = {"role": "assistant", "content": "", "tool_calls": tool_calls}
        finish_reason = "tool_calls"
    else:
        response_text = bridge_resp.get("response", "")
        status_code = _extract_http_status_from_error_text(response_text)
        if status_code in {401, 403, 429, 500, 502, 503, 504}:
            raise HTTPException(status_code=status_code, detail=response_text)
        msg_payload = {"role": "assistant", "content": response_text}
        finish_reason = "stop"

    final = {
        "id": completion_id,
        "object": "chat.completion",
        "created": created_ts,
        "model": model_id,
        "choices": [{"index": 0, "message": msg_payload, "finish_reason": finish_reason}],
    }

    # 提取 token 消耗并更新账号额度
    try:
        request_cost = bridge_resp.get("request_cost", 0)
        if isinstance(request_cost, (int, float)) and request_cost > 0:
            tokens_used = int(request_cost)
            selector.record_usage(account_id, 1)
            logger.info(
                "[OpenAI Compat] Request cost: %d tokens, account_id=%d",
                tokens_used,
                account_id,
            )
    except Exception as e:
        logger.warning("[OpenAI Compat] Failed to extract token cost: %s", e)

    return final 