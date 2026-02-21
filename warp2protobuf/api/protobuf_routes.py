#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Protobuf编解码API路由

提供纯protobuf数据包编解码服务，包括JWT管理和WebSocket支持。
"""

import asyncio
import base64
import json
import os as _os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..config.models import get_all_unique_models
from ..config.settings import (
    ACCOUNT_ADMIN_ENABLED,
    ACCOUNT_DB_PATH,
    ACCOUNT_POOL_BASE_URL,
    ACCOUNT_POOL_ENABLED,
    ACCOUNT_POOL_FALLBACK_TO_ENV,
    ACCOUNT_POOL_SWITCH_MAX_RETRIES,
    WARP_URL as CONFIG_WARP_URL,
)
from ..core.account_pool_client import AccountPoolClient
from ..core.auth import (
    get_jwt_token,
    is_token_expired,
    refresh_access_token_with_refresh_token,
    refresh_jwt_if_needed,
)
from ..core.logging import logger
from ..core.protobuf_utils import dict_to_protobuf_bytes, protobuf_to_dict
from ..core.schema_sanitizer import sanitize_mcp_input_schema_in_packet
from ..core.server_message_data import decode_server_message_data, encode_server_message_data
from ..core.stream_processor import get_stream_processor, set_websocket_manager


def _encode_smd_inplace(obj: Any) -> Any:
    if isinstance(obj, dict):
        new_d = {}
        for k, v in obj.items():
            if k in ("server_message_data", "serverMessageData") and isinstance(v, dict):
                try:
                    b64 = encode_server_message_data(
                        uuid=v.get("uuid"),
                        seconds=v.get("seconds"),
                        nanos=v.get("nanos"),
                    )
                    new_d[k] = b64
                except Exception:
                    new_d[k] = v
            else:
                new_d[k] = _encode_smd_inplace(v)
        return new_d
    if isinstance(obj, list):
        return [_encode_smd_inplace(x) for x in obj]
    return obj


def _decode_smd_inplace(obj: Any) -> Any:
    if isinstance(obj, dict):
        new_d = {}
        for k, v in obj.items():
            if k in ("server_message_data", "serverMessageData") and isinstance(v, str):
                try:
                    dec = decode_server_message_data(v)
                    new_d[k] = dec
                except Exception:
                    new_d[k] = v
            else:
                new_d[k] = _decode_smd_inplace(v)
        return new_d
    if isinstance(obj, list):
        return [_decode_smd_inplace(x) for x in obj]
    return obj


class EncodeRequest(BaseModel):
    json_data: Optional[Dict[str, Any]] = None
    message_type: str = "warp.multi_agent.v1.Request"

    task_context: Optional[Dict[str, Any]] = None
    input: Optional[Dict[str, Any]] = None
    settings: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    mcp_context: Optional[Dict[str, Any]] = None
    existing_suggestions: Optional[Dict[str, Any]] = None
    client_version: Optional[str] = None
    os_category: Optional[str] = None
    os_name: Optional[str] = None
    os_version: Optional[str] = None

    class Config:
        extra = "allow"

    def get_data(self) -> Dict[str, Any]:
        if self.json_data is not None:
            return self.json_data

        data: Dict[str, Any] = {}
        if self.task_context is not None:
            data["task_context"] = self.task_context
        if self.input is not None:
            data["input"] = self.input
        if self.settings is not None:
            data["settings"] = self.settings
        if self.metadata is not None:
            data["metadata"] = self.metadata
        if self.mcp_context is not None:
            data["mcp_context"] = self.mcp_context
        if self.existing_suggestions is not None:
            data["existing_suggestions"] = self.existing_suggestions
        if self.client_version is not None:
            data["client_version"] = self.client_version
        if self.os_category is not None:
            data["os_category"] = self.os_category
        if self.os_name is not None:
            data["os_name"] = self.os_name
        if self.os_version is not None:
            data["os_version"] = self.os_version

        skip_keys = {
            "json_data",
            "message_type",
            "task_context",
            "input",
            "settings",
            "metadata",
            "mcp_context",
            "existing_suggestions",
            "client_version",
            "os_category",
            "os_name",
            "os_version",
        }
        try:
            for k, v in self.__dict__.items():
                if v is None:
                    continue
                if k in skip_keys:
                    continue
                if k not in data:
                    data[k] = v
        except Exception:
            pass
        return data


class DecodeRequest(BaseModel):
    protobuf_bytes: str
    message_type: str = "warp.multi_agent.v1.Request"


class StreamDecodeRequest(BaseModel):
    protobuf_chunks: List[str]
    message_type: str = "warp.multi_agent.v1.Response"


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.packet_history: List[Dict] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket连接建立，当前连接数: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket连接断开，当前连接数: {len(self.active_connections)}")

    async def broadcast(self, message: Dict):
        if not self.active_connections:
            return

        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"发送WebSocket消息失败: {e}")
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect(conn)

    async def log_packet(self, packet_type: str, data: Dict, size: int):
        packet_info = {
            "timestamp": datetime.now().isoformat(),
            "type": packet_type,
            "size": size,
            "data_preview": str(data)[:200] + "..." if len(str(data)) > 200 else str(data),
            "full_data": data,
        }

        self.packet_history.append(packet_info)
        if len(self.packet_history) > 100:
            self.packet_history = self.packet_history[-100:]

        await self.broadcast({"event": "packet_captured", "packet": packet_info})


manager = ConnectionManager()
set_websocket_manager(manager)

app = FastAPI(title="Warp Protobuf编解码服务器", version="1.0.0")
_cors_origins = [
    x.strip()
    for x in _os.getenv("W2A_CORS_ORIGINS", "http://localhost,http://127.0.0.1").split(",")
    if x.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Warp Protobuf编解码服务器", "version": "1.0.0"}


@app.get("/healthz")
async def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.post("/api/encode")
async def encode_json_to_protobuf(request: EncodeRequest):
    try:
        logger.info(f"收到编码请求，消息类型: {request.message_type}")
        actual_data = request.get_data()
        if not actual_data:
            raise HTTPException(400, "数据包不能为空")
        wrapped = {"json_data": actual_data}
        wrapped = sanitize_mcp_input_schema_in_packet(wrapped)
        actual_data = wrapped.get("json_data", actual_data)
        actual_data = _encode_smd_inplace(actual_data)
        protobuf_bytes = dict_to_protobuf_bytes(actual_data, request.message_type)
        try:
            await manager.log_packet("encode", actual_data, len(protobuf_bytes))
        except Exception as log_error:
            logger.warning(f"数据包记录失败: {log_error}")
        result = {
            "protobuf_bytes": base64.b64encode(protobuf_bytes).decode("utf-8"),
            "size": len(protobuf_bytes),
            "message_type": request.message_type,
        }
        logger.info(f"✅ JSON编码为protobuf成功: {len(protobuf_bytes)} 字节")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ JSON编码失败: {e}")
        raise HTTPException(500, f"编码失败: {str(e)}")


@app.post("/api/decode")
async def decode_protobuf_to_json(request: DecodeRequest):
    try:
        logger.info(f"收到解码请求，消息类型: {request.message_type}")
        if not request.protobuf_bytes or not request.protobuf_bytes.strip():
            raise HTTPException(400, "Protobuf数据不能为空")
        try:
            protobuf_bytes = base64.b64decode(request.protobuf_bytes)
        except Exception as decode_error:
            logger.error(f"Base64解码失败: {decode_error}")
            raise HTTPException(400, f"Base64解码失败: {str(decode_error)}")
        if not protobuf_bytes:
            raise HTTPException(400, "解码后的protobuf数据为空")
        json_data = protobuf_to_dict(protobuf_bytes, request.message_type)
        try:
            await manager.log_packet("decode", json_data, len(protobuf_bytes))
        except Exception as log_error:
            logger.warning(f"数据包记录失败: {log_error}")
        result = {
            "json_data": json_data,
            "size": len(protobuf_bytes),
            "message_type": request.message_type,
        }
        logger.info(f"✅ Protobuf解码为JSON成功: {len(protobuf_bytes)} 字节")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Protobuf解码失败: {e}")
        raise HTTPException(500, f"解码失败: {e}")


@app.post("/api/stream-decode")
async def decode_stream_protobuf(request: StreamDecodeRequest):
    try:
        logger.info(f"收到流式解码请求，数据块数量: {len(request.protobuf_chunks)}")
        results = []
        total_size = 0
        for i, chunk_b64 in enumerate(request.protobuf_chunks):
            try:
                chunk_bytes = base64.b64decode(chunk_b64)
                chunk_json = protobuf_to_dict(chunk_bytes, request.message_type)
                chunk_result = {
                    "chunk_index": i,
                    "json_data": chunk_json,
                    "size": len(chunk_bytes),
                }
                results.append(chunk_result)
                total_size += len(chunk_bytes)
                await manager.log_packet(f"stream_decode_chunk_{i}", chunk_json, len(chunk_bytes))
            except Exception as e:
                logger.warning(f"数据块 {i} 解码失败: {e}")
                results.append({"chunk_index": i, "error": str(e), "size": 0})
        try:
            all_bytes = b"".join([base64.b64decode(chunk) for chunk in request.protobuf_chunks])
            complete_json = protobuf_to_dict(all_bytes, request.message_type)
            await manager.log_packet("stream_decode_complete", complete_json, len(all_bytes))
            complete_result = {"json_data": complete_json, "size": len(all_bytes)}
        except Exception as e:
            complete_result = {"error": f"无法拼接完整消息: {e}", "size": total_size}
        result = {
            "chunks": results,
            "complete": complete_result,
            "total_chunks": len(request.protobuf_chunks),
            "total_size": total_size,
            "message_type": request.message_type,
        }
        logger.info(
            f"✅ 流式protobuf解码完成: {len(request.protobuf_chunks)} 块，总大小 {total_size} 字节"
        )
        return result
    except Exception as e:
        logger.error(f"❌ 流式protobuf解码失败: {e}")
        raise HTTPException(500, f"流式解码失败: {e}")


@app.get("/api/schemas")
async def get_protobuf_schemas():
    try:
        from ..core.protobuf import ALL_MSGS, ensure_proto_runtime, msg_cls

        ensure_proto_runtime()
        schemas = []
        for msg_name in ALL_MSGS:
            try:
                message_class = msg_cls(msg_name)
                descriptor = message_class.DESCRIPTOR
                fields = []
                for field in descriptor.fields:
                    fields.append(
                        {
                            "name": field.name,
                            "type": field.type,
                            "label": getattr(field, "label", None),
                            "number": field.number,
                        }
                    )
                schemas.append(
                    {
                        "name": msg_name,
                        "full_name": descriptor.full_name,
                        "field_count": len(fields),
                        "fields": fields[:10],
                    }
                )
            except Exception as e:
                logger.warning(f"获取schema {msg_name} 信息失败: {e}")
        result = {
            "schemas": schemas,
            "total_count": len(schemas),
            "message": f"找到 {len(schemas)} 个protobuf消息类型",
        }
        logger.info(f"✅ 返回 {len(schemas)} 个protobuf schema")
        return result
    except Exception as e:
        logger.error(f"❌ 获取protobuf schemas失败: {e}")
        raise HTTPException(500, f"获取schemas失败: {e}")


@app.get("/api/auth/status")
async def get_auth_status():
    try:
        jwt_token = get_jwt_token()
        if not jwt_token:
            return {
                "authenticated": False,
                "message": "未找到JWT token",
                "suggestion": "运行 'uv run refresh_jwt.py' 获取token",
            }
        is_expired = is_token_expired(jwt_token)
        result = {
            "authenticated": not is_expired,
            "token_present": True,
            "token_expired": is_expired,
            "message": "Token有效" if not is_expired else "Token已过期",
        }
        if is_expired:
            result["suggestion"] = "运行 'uv run refresh_jwt.py' 刷新token"
        return result
    except Exception as e:
        logger.error(f"❌ 获取认证状态失败: {e}")
        raise HTTPException(500, f"获取认证状态失败: {e}")


@app.post("/api/auth/refresh")
async def refresh_auth_token():
    try:
        success = await refresh_jwt_if_needed()
        if success:
            return {
                "success": True,
                "message": "JWT token刷新成功",
                "timestamp": datetime.now().isoformat(),
            }
        return {
            "success": False,
            "message": "JWT token刷新失败",
            "suggestion": "检查网络连接或手动运行 'uv run refresh_jwt.py'",
        }
    except Exception as e:
        logger.error(f"❌ 刷新JWT token失败: {e}")
        raise HTTPException(500, f"刷新token失败: {e}")


@app.get("/api/auth/user_id")
async def get_user_id_endpoint():
    try:
        from ..core.auth import get_user_id

        user_id = get_user_id()
        if user_id:
            return {"success": True, "user_id": user_id, "message": "User ID获取成功"}
        return {
            "success": False,
            "user_id": "",
            "message": "未找到User ID，可能需要刷新JWT token",
        }
    except Exception as e:
        logger.error(f"❌ 获取User ID失败: {e}")
        raise HTTPException(500, f"获取User ID失败: {e}")


# ============ Account Management API ============


@app.get("/api/accounts")
async def list_accounts(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    keyword: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
):
    """获取账号列表（分页）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(403, "Account management is disabled")

    try:
        from ..core.account_store import AccountStore

        store = AccountStore(ACCOUNT_DB_PATH)
        accounts, total = store.get_accounts(
            page=page,
            page_size=page_size,
            keyword=keyword,
            status=status,
        )

        return {
            "accounts": accounts,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }
    except Exception as e:
        logger.error(f"❌ 获取账号列表失败: {e}")
        raise HTTPException(500, f"获取账号列表失败: {e}")


@app.get("/api/accounts/summary")
async def get_accounts_summary():
    """获取账号汇总统计"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(403, "Account management is disabled")

    try:
        from ..core.account_store import AccountStore

        store = AccountStore(ACCOUNT_DB_PATH)
        summary = store.get_summary()

        return {
            "success": True,
            "summary": summary,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"❌ 获取账号汇总失败: {e}")
        raise HTTPException(500, f"获取账号汇总失败: {e}")


@app.get("/api/accounts/{account_id}")
async def get_account_detail(account_id: int):
    """获取单个账号详情"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(403, "Account management is disabled")

    try:
        from ..core.account_store import AccountStore

        store = AccountStore(ACCOUNT_DB_PATH)
        account = store.get_account_by_id(account_id)

        if not account:
            raise HTTPException(404, "Account not found")

        return {
            "success": True,
            "account": account,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 获取账号详情失败: {e}")
        raise HTTPException(500, f"获取账号详情失败: {e}")


class UpdateLimitRequest(BaseModel):
    total_limit: int
    used_limit: int


@app.patch("/api/accounts/{account_id}/limit")
async def update_account_limit(account_id: int, request: UpdateLimitRequest):
    """更新账号额度"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(403, "Account management is disabled")

    try:
        from ..core.account_store import AccountStore

        store = AccountStore(ACCOUNT_DB_PATH)
        success = store.update_limit(
            account_id=account_id,
            total_limit=request.total_limit,
            used_limit=request.used_limit,
        )

        if not success:
            raise HTTPException(404, "Account not found")

        return {
            "success": True,
            "message": "额度更新成功",
            "timestamp": datetime.now().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 更新账号额度失败: {e}")
        raise HTTPException(500, f"更新账号额度失败: {e}")


# ============ End Account Management API ============


@app.get("/api/packets/history")
async def get_packet_history(limit: int = 50):
    try:
        history = (
            manager.packet_history[-limit:]
            if len(manager.packet_history) > limit
            else manager.packet_history
        )
        return {
            "packets": history,
            "total_count": len(manager.packet_history),
            "returned_count": len(history),
        }
    except Exception as e:
        logger.error(f"❌ 获取数据包历史失败: {e}")
        raise HTTPException(500, f"获取历史记录失败: {e}")


@app.post("/api/warp/send")
async def send_to_warp_api(
    request: EncodeRequest,
    show_all_events: bool = Query(True, description="Show detailed SSE event breakdown"),
):
    try:
        logger.info(f"收到Warp API发送请求，消息类型: {request.message_type}")
        actual_data = request.get_data()
        if not actual_data:
            raise HTTPException(400, "数据包不能为空")
        wrapped = {"json_data": actual_data}
        wrapped = sanitize_mcp_input_schema_in_packet(wrapped)
        actual_data = wrapped.get("json_data", actual_data)
        actual_data = _encode_smd_inplace(actual_data)
        protobuf_bytes = dict_to_protobuf_bytes(actual_data, request.message_type)
        logger.info(f"✅ JSON编码为protobuf成功: {len(protobuf_bytes)} 字节")
        from ..warp.api_client import send_protobuf_to_warp_api

        response_text, conversation_id, task_id = await send_protobuf_to_warp_api(
            protobuf_bytes,
            show_all_events=show_all_events,
        )
        await manager.log_packet("warp_request", actual_data, len(protobuf_bytes))
        await manager.log_packet(
            "warp_response",
            {
                "response": response_text,
                "conversation_id": conversation_id,
                "task_id": task_id,
            },
            len(response_text.encode()),
        )
        result = {
            "response": response_text,
            "conversation_id": conversation_id,
            "task_id": task_id,
            "request_size": len(protobuf_bytes),
            "response_size": len(response_text),
            "message_type": request.message_type,
        }
        logger.info(f"✅ Warp API调用成功，响应长度: {len(response_text)} 字符")
        return result
    except Exception as e:
        import traceback

        error_details = {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "request_info": {
                "message_type": request.message_type,
                "json_size": len(str(actual_data)),
                "has_tools": "mcp_context" in actual_data,
                "has_history": "task_context" in actual_data,
            },
        }
        logger.error(f"❌ Warp API调用失败: {e}")
        logger.error(f"错误详情: {error_details}")
        try:
            await manager.log_packet("warp_error", error_details, 0)
        except Exception as log_error:
            logger.warning(f"记录错误失败: {log_error}")
        raise HTTPException(500, detail=error_details)


async def _release_pool_session(
    pool_client: Optional[AccountPoolClient],
    session_id: Optional[str],
) -> None:
    if not pool_client or not session_id:
        return
    try:
        await pool_client.release(session_id)
    except Exception as exc:
        logger.warning(f"[AccountPool] release failed: session_id={session_id} error={exc}")


async def _allocate_access_token_from_pool(
    pool_client: AccountPoolClient,
    session_id: str,
) -> str:
    try:
        allocated = await pool_client.allocate(session_id=session_id, count=1)
        accounts = allocated.get("accounts") or []
        if not accounts:
            raise RuntimeError("account pool allocate returned no accounts")

        account = accounts[0]
        if not isinstance(account, dict):
            raise RuntimeError("account pool allocate returned invalid account payload")

        refresh_token = str(account.get("refresh_token") or "")
        if not refresh_token:
            raise RuntimeError("allocated account missing refresh_token")

        return await refresh_access_token_with_refresh_token(refresh_token)
    except Exception:
        await _release_pool_session(pool_client, session_id)
        raise


async def _resolve_request_access_token(
    pool_client: Optional[AccountPoolClient],
    session_id: str,
) -> Tuple[str, bool]:
    """Return (access_token, using_pool)."""
    if pool_client:
        try:
            token = await _allocate_access_token_from_pool(pool_client, session_id)
            return token, True
        except Exception as exc:
            if ACCOUNT_POOL_FALLBACK_TO_ENV:
                logger.warning(
                    f"[AccountPool] allocate/refresh failed, fallback to env enabled: {exc}"
                )
            else:
                raise RuntimeError(
                    f"account pool unavailable and fallback disabled: {exc}"
                ) from exc

    from ..core.auth import get_valid_jwt

    token = await get_valid_jwt()
    return token, False


@app.post("/api/warp/send_stream")
async def send_to_warp_api_parsed(request: EncodeRequest):
    from ..warp.api_client import (
        WarpApiHttpError,
        is_quota_429,
        send_protobuf_to_warp_api_parsed,
    )

    pool_client: Optional[AccountPoolClient] = None
    using_pool = False
    session_id: Optional[str] = None

    try:
        logger.info(f"收到Warp API解析发送请求，消息类型: {request.message_type}")
        actual_data = request.get_data()
        if not actual_data:
            raise HTTPException(400, "数据包不能为空")

        wrapped = {"json_data": actual_data}
        wrapped = sanitize_mcp_input_schema_in_packet(wrapped)
        actual_data = wrapped.get("json_data", actual_data)
        actual_data = _encode_smd_inplace(actual_data)
        protobuf_bytes = dict_to_protobuf_bytes(actual_data, request.message_type)
        logger.info(f"✅ JSON编码为protobuf成功: {len(protobuf_bytes)} 字节")

        if ACCOUNT_POOL_ENABLED:
            pool_client = AccountPoolClient(base_url=ACCOUNT_POOL_BASE_URL)

        max_retries = max(0, int(ACCOUNT_POOL_SWITCH_MAX_RETRIES))
        switch_attempt = 0
        session_id = str(uuid.uuid4())
        access_token, using_pool = await _resolve_request_access_token(pool_client, session_id)

        while True:
            try:
                response_text, conversation_id, task_id, parsed_events = await send_protobuf_to_warp_api_parsed(
                    protobuf_bytes,
                    access_token=access_token,
                )
                break
            except WarpApiHttpError as api_error:
                should_switch = (
                    using_pool
                    and is_quota_429(api_error.status_code, api_error.error_content)
                    and switch_attempt < max_retries
                )
                if not should_switch:
                    raise

                logger.warning(
                    f"命中配额429，执行换号重试: {switch_attempt + 1}/{max_retries}"
                )
                await _release_pool_session(pool_client, session_id)
                using_pool = False
                switch_attempt += 1
                session_id = str(uuid.uuid4())
                access_token, using_pool = await _resolve_request_access_token(
                    pool_client,
                    session_id,
                )

        parsed_events = _decode_smd_inplace(parsed_events)
        await manager.log_packet("warp_request_parsed", actual_data, len(protobuf_bytes))
        response_data = {
            "response": response_text,
            "conversation_id": conversation_id,
            "task_id": task_id,
            "parsed_events": parsed_events,
        }
        await manager.log_packet("warp_response_parsed", response_data, len(str(response_data)))

        result = {
            "response": response_text,
            "conversation_id": conversation_id,
            "task_id": task_id,
            "request_size": len(protobuf_bytes),
            "response_size": len(response_text),
            "message_type": request.message_type,
            "parsed_events": parsed_events,
            "events_count": len(parsed_events),
            "events_summary": {},
        }
        if parsed_events:
            event_type_counts: Dict[str, int] = {}
            for event in parsed_events:
                event_type = event.get("event_type", "UNKNOWN")
                event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            result["events_summary"] = event_type_counts

        logger.info(
            f"✅ Warp API解析调用成功，响应长度: {len(response_text)} 字符，事件数量: {len(parsed_events)}"
        )
        return result

    except WarpApiHttpError as api_error:
        logger.error(
            f"❌ Warp API解析调用失败: HTTP {api_error.status_code} {api_error.error_content[:200]}"
        )
        raise HTTPException(status_code=api_error.status_code, detail=api_error.error_content)

    except Exception as e:
        import traceback

        error_details = {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "request_info": {
                "message_type": request.message_type,
                "json_size": len(str(actual_data)) if "actual_data" in locals() else 0,
                "has_tools": "mcp_context" in (actual_data or {}),
                "has_history": "task_context" in (actual_data or {}),
            },
        }
        logger.error(f"❌ Warp API解析调用失败: {e}")
        logger.error(f"错误详情: {error_details}")
        try:
            await manager.log_packet("warp_error_parsed", error_details, 0)
        except Exception as log_error:
            logger.warning(f"记录错误失败: {log_error}")
        raise HTTPException(500, detail=error_details)

    finally:
        if using_pool and session_id:
            await _release_pool_session(pool_client, session_id)


@app.post("/api/warp/send_stream_sse")
async def send_to_warp_api_stream_sse(request: EncodeRequest):
    from fastapi.responses import StreamingResponse

    from ..warp.api_client import (
        WarpApiHttpError,
        WarpSseRequest,
        _get_event_type,
        is_quota_429,
    )

    def _decode_event_for_sse(raw_line_payload: str) -> Optional[Dict[str, Any]]:
        import re as _re

        def _parse_payload_bytes(data_str: str) -> Optional[bytes]:
            s = _re.sub(r"\s+", "", data_str or "")
            if not s:
                return None
            if _re.fullmatch(r"[0-9a-fA-F]+", s or ""):
                try:
                    return bytes.fromhex(s)
                except Exception:
                    return None
            pad = "=" * ((4 - (len(s) % 4)) % 4)
            try:
                import base64 as _b64

                return _b64.urlsafe_b64decode(s + pad)
            except Exception:
                try:
                    return _b64.b64decode(s + pad)
                except Exception:
                    return None

        raw_bytes = _parse_payload_bytes(raw_line_payload)
        if raw_bytes is None:
            return None

        try:
            return protobuf_to_dict(raw_bytes, "warp.multi_agent.v1.ResponseEvent")
        except Exception:
            return None

    try:
        actual_data = request.get_data()
        if not actual_data:
            raise HTTPException(400, "数据包不能为空")

        wrapped = {"json_data": actual_data}
        wrapped = sanitize_mcp_input_schema_in_packet(wrapped)
        actual_data = wrapped.get("json_data", actual_data)
        actual_data = _encode_smd_inplace(actual_data)
        protobuf_bytes = dict_to_protobuf_bytes(actual_data, request.message_type)

        pool_client: Optional[AccountPoolClient] = None
        if ACCOUNT_POOL_ENABLED:
            pool_client = AccountPoolClient(base_url=ACCOUNT_POOL_BASE_URL)

        max_retries = max(0, int(ACCOUNT_POOL_SWITCH_MAX_RETRIES))

        async def _agen():
            session_id = str(uuid.uuid4())
            access_token, using_pool = await _resolve_request_access_token(pool_client, session_id)
            switch_attempt = 0
            event_no = 0

            try:
                while True:
                    try:
                        req = WarpSseRequest(
                            protobuf_bytes=protobuf_bytes,
                            access_token=access_token,
                        )

                        logger.info(f"✅ Warp API SSE连接已建立: {CONFIG_WARP_URL}")
                        logger.info(f"📦 请求字节数: {len(protobuf_bytes)}")

                        current_data = ""
                        async for line in req.iter_lines():
                            if line.startswith("data:"):
                                payload = line[5:].strip()
                                if not payload:
                                    continue
                                if payload == "[DONE]":
                                    break
                                current_data += payload
                                continue

                            if (line.strip() == "") and current_data:
                                event_data = _decode_event_for_sse(current_data)
                                current_data = ""
                                if event_data is None:
                                    continue

                                event_no += 1
                                event_type = _get_event_type(event_data)
                                logger.info(f"🔄 SSE Event #{event_no}: {event_type}")

                                out = {
                                    "event_number": event_no,
                                    "event_type": event_type,
                                    "parsed_data": event_data,
                                }
                                yield f"data: {json.dumps(out, ensure_ascii=False)}\n\n"

                        logger.info("=" * 60)
                        logger.info("📊 SSE STREAM SUMMARY (代理)")
                        logger.info("=" * 60)
                        logger.info(f"📈 Total Events Forwarded: {event_no}")
                        logger.info("=" * 60)
                        yield "data: [DONE]\n\n"
                        return

                    except WarpApiHttpError as api_error:
                        should_switch = (
                            using_pool
                            and is_quota_429(api_error.status_code, api_error.error_content)
                            and switch_attempt < max_retries
                        )
                        if should_switch:
                            logger.warning(
                                f"SSE命中配额429，执行换号重试: {switch_attempt + 1}/{max_retries}"
                            )
                            await _release_pool_session(pool_client, session_id)
                            using_pool = False
                            switch_attempt += 1
                            session_id = str(uuid.uuid4())
                            access_token, using_pool = await _resolve_request_access_token(
                                pool_client,
                                session_id,
                            )
                            continue

                        logger.error(
                            f"Warp API HTTP error {api_error.status_code}: {api_error.error_content[:300]}"
                        )
                        payload = {
                            "error": f"HTTP {api_error.status_code}",
                            "detail": api_error.error_content[:300],
                        }
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    except Exception as stream_exc:
                        logger.error(f"Warp SSE转发流内错误: {stream_exc}")
                        yield f"data: {json.dumps({'error': 'stream error'}, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return
            finally:
                if using_pool:
                    await _release_pool_session(pool_client, session_id)

        return StreamingResponse(
            _agen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        error_details = {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
        }
        logger.error(f"Warp SSE转发端点错误: {e}")
        raise HTTPException(500, detail=error_details)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_json(
            {
                "event": "connected",
                "message": "WebSocket连接已建立",
                "timestamp": datetime.now().isoformat(),
            }
        )
        recent_packets = manager.packet_history[-10:]
        for packet in recent_packets:
            await websocket.send_json({"event": "packet_history", "packet": packet})
        while True:
            data = await websocket.receive_text()
            logger.debug(f"收到WebSocket消息: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket错误: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=28888)
