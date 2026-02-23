#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp API客户端模块

处理与 Warp API 的通信，包括 protobuf 数据发送和 SSE 响应解析。
"""

from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, Optional, AsyncGenerator

import httpx

from ..config.settings import (
    CLIENT_ID,
    CLIENT_VERSION,
    OS_CATEGORY,
    OS_NAME,
    OS_VERSION,
    WARP_URL as CONFIG_WARP_URL,
    TLS_VERIFY,
)
from ..core.auth import get_valid_jwt
from ..core.logging import logger
from ..core.protobuf_utils import protobuf_to_dict


class WarpApiHttpError(RuntimeError):
    """Structured upstream HTTP error from Warp API."""

    def __init__(self, status_code: int, error_content: str):
        super().__init__(f"Warp API Error (HTTP {status_code}): {error_content}")
        self.status_code = status_code
        self.error_content = error_content


class WarpSseRequest:
    """Reusable SSE upstream request context for parsed/raw stream endpoints."""

    def __init__(self, protobuf_bytes: bytes, access_token: str):
        self.protobuf_bytes = protobuf_bytes
        self.access_token = access_token
        self.headers = _build_headers(access_token, protobuf_bytes)
        self.verify_opt = TLS_VERIFY

    async def iter_lines(self) -> AsyncGenerator[str, None]:
        async with httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(60.0),
            verify=self.verify_opt,
            trust_env=False,
        ) as client:
            for attempt in range(2):
                headers = self.headers if attempt == 0 else _build_headers(self.access_token, self.protobuf_bytes)
                async with client.stream(
                    "POST",
                    CONFIG_WARP_URL,
                    headers=headers,
                    content=self.protobuf_bytes,
                ) as response:
                    if response.status_code != 200:
                        error_text = await response.aread()
                        error_content = error_text.decode("utf-8") if error_text else ""

                        # 429 配额耗尽：尝试申请匿名 token 并重试一次
                        if attempt == 0 and is_quota_429(response.status_code, error_content):
                            logger.warning("WARP API 429 (配额用尽)，尝试申请匿名 token 重试…")
                            try:
                                from ..core.auth import acquire_anonymous_access_token
                                new_jwt = await acquire_anonymous_access_token()
                                if new_jwt:
                                    self.access_token = new_jwt
                                    continue
                            except Exception as e:
                                logger.error("匿名 token 申请失败: %s", e)

                        raise WarpApiHttpError(response.status_code, error_content)

                    async for line in response.aiter_lines():
                        yield line
                    return  # 成功完成，退出重试循环


def _get(d: Dict[str, Any], *names: str) -> Any:
    """Return the first matching key value (camelCase/snake_case tolerant)."""
    for name in names:
        if name in d:
            return d[name]
    return None


def _get_event_type(event_data: dict) -> str:
    """Determine the type of SSE event for logging."""
    if "init" in event_data:
        return "INITIALIZATION"

    client_actions = _get(event_data, "client_actions", "clientActions")
    if isinstance(client_actions, dict):
        actions = _get(client_actions, "actions", "Actions") or []
        if not actions:
            return "CLIENT_ACTIONS_EMPTY"

        action_types = []
        for action in actions:
            if _get(action, "create_task", "createTask") is not None:
                action_types.append("CREATE_TASK")
            elif _get(action, "append_to_message_content", "appendToMessageContent") is not None:
                action_types.append("APPEND_CONTENT")
            elif _get(action, "add_messages_to_task", "addMessagesToTask") is not None:
                action_types.append("ADD_MESSAGE")
            elif _get(action, "tool_call", "toolCall") is not None:
                action_types.append("TOOL_CALL")
            elif _get(action, "tool_response", "toolResponse") is not None:
                action_types.append("TOOL_RESPONSE")
            else:
                action_types.append("UNKNOWN_ACTION")

        return f"CLIENT_ACTIONS({', '.join(action_types)})"

    if "finished" in event_data:
        return "FINISHED"

    return "UNKNOWN_EVENT"


def is_quota_429(status_code: int, error_content: str) -> bool:
    """Match existing quota-429 keyword rules (must stay consistent)."""
    if status_code != 429:
        return False
    return ("No remaining quota" in error_content) or (
        "No AI requests remaining" in error_content
    )


def _parse_payload_bytes(data_str: str) -> Optional[bytes]:
    compact = re.sub(r"\s+", "", data_str or "")
    if not compact:
        return None

    if re.fullmatch(r"[0-9a-fA-F]+", compact):
        try:
            return bytes.fromhex(compact)
        except Exception:
            return None

    pad = "=" * ((4 - (len(compact) % 4)) % 4)
    try:
        return base64.urlsafe_b64decode(compact + pad)
    except Exception:
        try:
            return base64.b64decode(compact + pad)
        except Exception:
            return None


def _build_headers(access_token: str, protobuf_bytes: bytes) -> Dict[str, str]:
    return {
        "accept": "text/event-stream",
        "content-type": "application/x-protobuf",
        "x-warp-client-id": CLIENT_ID,
        "x-warp-client-version": CLIENT_VERSION,
        "x-warp-os-category": OS_CATEGORY,
        "x-warp-os-name": OS_NAME,
        "x-warp-os-version": OS_VERSION,
        "authorization": f"Bearer {access_token}",
        "content-length": str(len(protobuf_bytes)),
    }


def _resolve_tls_verify() -> bool:
    insecure_env = os.getenv("WARP_INSECURE_TLS", "").lower()
    if insecure_env in ("1", "true", "yes"):
        logger.warning("TLS verification disabled via WARP_INSECURE_TLS for Warp API client")
        return False
    return True


def _extract_text_from_event(
    event_data: Dict[str, Any],
    current_task_id: Optional[str],
    complete_response: list[str],
) -> Optional[str]:
    """Collect text fragments and update task_id from one parsed event."""
    task_id = current_task_id

    client_actions = _get(event_data, "client_actions", "clientActions")
    if not isinstance(client_actions, dict):
        return task_id

    actions = _get(client_actions, "actions", "Actions") or []
    for action in actions:
        append_data = _get(action, "append_to_message_content", "appendToMessageContent")
        if isinstance(append_data, dict):
            message = append_data.get("message", {})
            agent_output = _get(message, "agent_output", "agentOutput") or {}
            text_content = agent_output.get("text", "")
            if text_content:
                complete_response.append(text_content)

        messages_data = _get(action, "add_messages_to_task", "addMessagesToTask")
        if isinstance(messages_data, dict):
            messages = messages_data.get("messages", [])
            task_id = messages_data.get("task_id", messages_data.get("taskId", task_id))
            for message in messages:
                if _get(message, "agent_output", "agentOutput") is not None:
                    agent_output = _get(message, "agent_output", "agentOutput") or {}
                    text_content = agent_output.get("text", "")
                    if text_content:
                        complete_response.append(text_content)

    return task_id


async def _send_and_parse(
    protobuf_bytes: bytes,
    access_token: Optional[str],
    show_all_events: bool,
    collect_parsed_events: bool,
) -> tuple[str, Optional[str], Optional[str], list[dict[str, Any]]]:
    warp_url = CONFIG_WARP_URL
    verify_opt = TLS_VERIFY

    jwt = access_token or await get_valid_jwt()
    headers = _build_headers(jwt, protobuf_bytes)

    conversation_id: Optional[str] = None
    task_id: Optional[str] = None
    complete_response: list[str] = []
    parsed_events: list[dict[str, Any]] = []
    event_count = 0

    async with httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(60.0),
        verify=verify_opt,
        trust_env=False,
    ) as client:
        for attempt in range(2):
            if attempt > 0:
                headers = _build_headers(jwt, protobuf_bytes)
            async with client.stream("POST", warp_url, headers=headers, content=protobuf_bytes) as response:
                if response.status_code != 200:
                    error_text = await response.aread()
                    error_content = error_text.decode("utf-8") if error_text else ""

                    # 429 配额耗尽：尝试申请匿名 token 并重试一次
                    if attempt == 0 and is_quota_429(response.status_code, error_content):
                        logger.warning("WARP API 429 (配额用尽)，尝试申请匿名 token 重试…")
                        try:
                            from ..core.auth import acquire_anonymous_access_token
                            new_jwt = await acquire_anonymous_access_token()
                            if new_jwt:
                                jwt = new_jwt
                                continue
                        except Exception as e:
                            logger.error("匿名 token 申请失败: %s", e)

                    raise WarpApiHttpError(response.status_code, error_content)

                logger.info(f"✅ 收到HTTP {response.status_code}响应")
                logger.info("开始处理SSE事件流...")

                current_data = ""
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        payload = line[5:].strip()
                        if not payload:
                            continue
                        if payload == "[DONE]":
                            logger.info("收到[DONE]标记，结束处理")
                            break
                        current_data += payload
                        continue

                    if (line.strip() == "") and current_data:
                        raw_bytes = _parse_payload_bytes(current_data)
                        current_data = ""
                        if raw_bytes is None:
                            logger.debug("跳过无法解析的SSE数据块（非hex/base64或不完整）")
                            continue

                        try:
                            event_data = protobuf_to_dict(raw_bytes, "warp.multi_agent.v1.ResponseEvent")
                        except Exception as parse_error:
                            logger.debug(f"解析事件失败，跳过: {str(parse_error)[:100]}")
                            continue

                        event_count += 1
                        event_type = _get_event_type(event_data)

                        if collect_parsed_events:
                            parsed_events.append(
                                {
                                    "event_number": event_count,
                                    "event_type": event_type,
                                    "parsed_data": event_data,
                                }
                            )

                        logger.info(f"🔄 Event #{event_count}: {event_type}")
                        if show_all_events:
                            logger.info(f"   📋 Event data: {str(event_data)}...")

                        if "init" in event_data:
                            init_data = event_data["init"]
                            conversation_id = init_data.get("conversation_id", conversation_id)
                            task_id = init_data.get("task_id", task_id)
                            logger.info(f"会话初始化: {conversation_id}")

                        task_id = _extract_text_from_event(
                            event_data=event_data,
                            current_task_id=task_id,
                            complete_response=complete_response,
                        )
                break  # 成功处理完毕，退出重试循环

    full_response = "".join(complete_response)
    logger.info("=" * 60)
    logger.info("📊 SSE STREAM SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📈 Total Events Processed: {event_count}")
    logger.info(f"🆔 Conversation ID: {conversation_id}")
    logger.info(f"🆔 Task ID: {task_id}")
    logger.info(f"📝 Response Length: {len(full_response)} characters")
    if collect_parsed_events:
        logger.info(f"🎯 Parsed Events Count: {len(parsed_events)}")
    logger.info("=" * 60)

    if full_response:
        logger.info("✅ Stream processing completed successfully")
        return full_response, conversation_id, task_id, parsed_events

    logger.warning("⚠️ No text content received in response")
    return "Warning: No response content received", conversation_id, task_id, parsed_events


async def send_protobuf_to_warp_api(
    protobuf_bytes: bytes,
    show_all_events: bool = True,
    access_token: Optional[str] = None,
) -> tuple[str, Optional[str], Optional[str]]:
    """发送protobuf数据到Warp API并获取响应。"""
    logger.info(f"发送 {len(protobuf_bytes)} 字节到Warp API")
    logger.info(f"数据包前32字节 (hex): {protobuf_bytes[:32].hex()}")
    logger.info(f"发送请求到: {CONFIG_WARP_URL}")

    response_text, conversation_id, task_id, _ = await _send_and_parse(
        protobuf_bytes=protobuf_bytes,
        access_token=access_token,
        show_all_events=show_all_events,
        collect_parsed_events=False,
    )
    return response_text, conversation_id, task_id


async def send_protobuf_to_warp_api_parsed(
    protobuf_bytes: bytes,
    access_token: Optional[str] = None,
) -> tuple[str, Optional[str], Optional[str], list[dict[str, Any]]]:
    """发送protobuf数据到Warp API并获取解析后的SSE事件数据。"""
    logger.info(f"发送 {len(protobuf_bytes)} 字节到Warp API (解析模式)")
    logger.info(f"数据包前32字节 (hex): {protobuf_bytes[:32].hex()}")
    logger.info(f"发送请求到: {CONFIG_WARP_URL}")

    return await _send_and_parse(
        protobuf_bytes=protobuf_bytes,
        access_token=access_token,
        show_all_events=True,
        collect_parsed_events=True,
    )
