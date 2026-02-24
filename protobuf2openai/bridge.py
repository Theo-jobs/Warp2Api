from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Dict, Optional

import httpx
from .logging import logger

from .config import (
    FALLBACK_BRIDGE_URLS,
    WARMUP_INIT_RETRIES,
    WARMUP_INIT_DELAY_S,
    WARMUP_REQUEST_RETRIES,
    WARMUP_REQUEST_DELAY_S,
)
from .packets import packet_template
from .state import STATE, ensure_tool_ids


async def bridge_send_stream(packet: Dict[str, Any], access_token: Optional[str] = None) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for base in FALLBACK_BRIDGE_URLS:
        url = f"{base}/api/warp/send_stream"
        try:
            wrapped_packet = {"json_data": packet, "message_type": "warp.multi_agent.v1.Request"}
            if access_token:
                wrapped_packet["access_token"] = access_token
            try:
                # 日志脱敏：移除 access_token 后再打印
                safe_payload = {k: v for k, v in wrapped_packet.items() if k != "access_token"}
                logger.info("[OpenAI Compat] Bridge request URL: %s", url)
                logger.info("[OpenAI Compat] Bridge request payload: %s", json.dumps(safe_payload, ensure_ascii=False)[:2000])
            except Exception:
                logger.info("[OpenAI Compat] Bridge request payload serialization failed for URL %s", url)
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, read=180.0)) as client:
                r = await client.post(url, json=wrapped_packet)
            if r.status_code == 200:
                try:
                    # 响应体截断到前 500 字符
                    logger.info("[OpenAI Compat] Bridge response (raw text): %s", r.text[:500])
                except Exception:
                    pass
                return r.json()
            else:
                txt = r.text[:500]
                last_exc = Exception(f"bridge_error: HTTP {r.status_code} {txt}")
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise Exception("bridge_unreachable")


_warmup_attempted = False
_warmup_lock = asyncio.Lock()


async def initialize_once() -> None:
    global _warmup_attempted
    async with _warmup_lock:
        if _warmup_attempted or STATE.conversation_id:
            return
        _warmup_attempted = True

        ensure_tool_ids()

        first_task_id = STATE.baseline_task_id or str(uuid.uuid4())
        STATE.baseline_task_id = first_task_id

        health_urls = [f"{base}/healthz" for base in FALLBACK_BRIDGE_URLS]
        last_err: Optional[str] = None
        for _ in range(WARMUP_INIT_RETRIES):
            try:
                ok = False
                last_err = None
                for h in health_urls:
                    try:
                        async with httpx.AsyncClient(timeout=5.0) as client:
                            resp = await client.get(h)
                        if resp.status_code == 200:
                            ok = True
                            break
                        else:
                            last_err = f"HTTP {resp.status_code} at {h}"
                    except Exception as he:
                        last_err = f"{type(he).__name__}: {he} at {h}"
                if ok:
                    break
            except Exception as e:
                last_err = str(e)
            await asyncio.sleep(WARMUP_INIT_DELAY_S)
        else:
            raise RuntimeError(f"Bridge server not ready: {last_err}")

        pkt = packet_template()
        pkt["task_context"]["active_task_id"] = first_task_id
        pkt["input"]["user_inputs"]["inputs"].append({"user_query": {"query": "warmup"}})

        last_exc: Optional[Exception] = None
        for attempt in range(1, WARMUP_REQUEST_RETRIES + 1):
            try:
                resp = await bridge_send_stream(pkt)
                break
            except Exception as e:
                last_exc = e
                logger.warning("[OpenAI Compat] Warmup attempt %d/%d failed: %s", attempt, WARMUP_REQUEST_RETRIES, e)
                if attempt < WARMUP_REQUEST_RETRIES:
                    await asyncio.sleep(WARMUP_REQUEST_DELAY_S)
                else:
                    raise

        STATE.conversation_id = resp.get("conversation_id") or STATE.conversation_id
        ret_task_id = resp.get("task_id")
        if isinstance(ret_task_id, str) and ret_task_id:
            STATE.baseline_task_id = ret_task_id
