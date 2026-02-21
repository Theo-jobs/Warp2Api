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

from warp2protobuf.config.models import resolve_model, get_all_unique_models as _get_all_models
from warp2protobuf.config.settings import ACCOUNT_DB_PATH
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

    跳过 system 消息和最后一条 user/tool 输入（它会作为当前 query 发送）。
    """
    non_system = [m for m in history if m.role != "system"]
    if len(non_system) <= 1:
        return None  # 没有历史

    # 最后一条 user/tool 是当前输入，不放入历史
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
            lines.append(f"Tool result ({m.tool_call_id or 'unknown'}): {text[:500]}")
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

    # 使用账号上下文管理账号分配与使用追踪
    try:
        with AccountContext(ACCOUNT_DB_PATH) as account_ctx:
            # 临时设置当前账号的 JWT token
            original_jwt = os.environ.get("WARP_JWT", "")
            os.environ["WARP_JWT"] = account_ctx.get_id_token()

            try:
                initialize_once()
            except Exception as e:
                logger.warning(f"[OpenAI Compat] initialize_once failed or skipped: {e}")

            if not req.messages:
                raise HTTPException(400, "messages 不能为空")

            # 记录当前使用的账号
            account_info = account_ctx.get_account_info()
            logger.info(
                "[OpenAI Compat] Using account: id=%d email=%s remaining=%d",
                account_info["id"],
                account_info["email"],
                account_info["remaining_limit"],
            )

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

            # ... 继续原有逻辑 ...
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
            async for chunk in stream_openai_sse(packet, completion_id, created_ts, model_id):
                yield chunk
        return StreamingResponse(_agen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})

    def _post_once() -> requests.Response:
        return requests.post(
            f"{BRIDGE_BASE_URL}/api/warp/send_stream",
            json={"json_data": packet, "message_type": "warp.multi_agent.v1.Request"},
            timeout=(5.0, 180.0),
        )

    try:
        resp = _post_once()
        if resp.status_code == 429:
            try:
                r = requests.post(f"{BRIDGE_BASE_URL}/api/auth/refresh", timeout=10.0)
                logger.warning("[OpenAI Compat] Bridge returned 429. Tried JWT refresh -> HTTP %s", getattr(r, 'status_code', 'N/A'))
            except Exception as _e:
                logger.warning("[OpenAI Compat] JWT refresh attempt failed after 429: %s", _e)
            resp = _post_once()
        if resp.status_code != 200:
            raise HTTPException(resp.status_code, f"bridge_error: {resp.text}")
        bridge_resp = resp.json()
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
                    account_ctx.set_tokens_used(tokens_used)
                    logger.info(
                        "[OpenAI Compat] Request cost: %d tokens, account_id=%d",
                        tokens_used,
                        account_info["id"],
                    )
            except Exception as e:
                logger.warning("[OpenAI Compat] Failed to extract token cost: %s", e)

            # 恢复原 JWT
            os.environ["WARP_JWT"] = original_jwt

            return final
    except Exception as e:
        # 确保异常时也恢复 JWT
        if 'original_jwt' in locals():
            os.environ["WARP_JWT"] = original_jwt
        raise 