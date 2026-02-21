from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Optional

import requests
from .logging import logger

from .config import (
    BRIDGE_BASE_URL,
    FALLBACK_BRIDGE_URLS,
    WARMUP_INIT_RETRIES,
    WARMUP_INIT_DELAY_S,
    WARMUP_REQUEST_RETRIES,
    WARMUP_REQUEST_DELAY_S,
)
from .packets import packet_template
from .state import STATE, ensure_tool_ids


def bridge_send_stream(packet: Dict[str, Any], access_token: Optional[str] = None) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for base in FALLBACK_BRIDGE_URLS:
        url = f"{base}/api/warp/send_stream"
        try:
            wrapped_packet = {"json_data": packet, "message_type": "warp.multi_agent.v1.Request"}
            # 如果 router 传入了 access_token，附加到请求中让 bridge 直接使用
            if access_token:
                wrapped_packet["access_token"] = access_token
            try:
                logger.info("[OpenAI Compat] Bridge request URL: %s", url)
                logger.info("[OpenAI Compat] Bridge request payload: %s", json.dumps(wrapped_packet, ensure_ascii=False))
            except Exception:
                logger.info("[OpenAI Compat] Bridge request payload serialization failed for URL %s", url)
            r = requests.post(url, json=wrapped_packet, timeout=(5.0, 180.0))
            if r.status_code == 200:
                try:
                    logger.info("[OpenAI Compat] Bridge response (raw text): %s", r.text)
                except Exception:
                    pass
                return r.json()
            else:
                txt = r.text
                last_exc = Exception(f"bridge_error: HTTP {r.status_code} {txt}")
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise Exception("bridge_unreachable")


_warmup_attempted = False


def initialize_once() -> None:
    global _warmup_attempted
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
                    resp = requests.get(h, timeout=5.0)
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
        time.sleep(WARMUP_INIT_DELAY_S)
    else:
        raise RuntimeError(f"Bridge server not ready: {last_err}")

    pkt = packet_template()
    pkt["task_context"]["active_task_id"] = first_task_id
    pkt["input"]["user_inputs"]["inputs"].append({"user_query": {"query": "warmup"}})

    last_exc: Optional[Exception] = None
    for attempt in range(1, WARMUP_REQUEST_RETRIES + 1):
        try:
            resp = bridge_send_stream(pkt)
            break
        except Exception as e:
            last_exc = e
            logger.warning(f"[OpenAI Compat] Warmup attempt {attempt}/{WARMUP_REQUEST_RETRIES} failed: {e}")
            if attempt < WARMUP_REQUEST_RETRIES:
                time.sleep(WARMUP_REQUEST_DELAY_S)
            else:
                raise

    STATE.conversation_id = resp.get("conversation_id") or STATE.conversation_id
    ret_task_id = resp.get("task_id")
    if isinstance(ret_task_id, str) and ret_task_id:
        STATE.baseline_task_id = ret_task_id 