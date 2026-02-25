from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psutil
from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from .logging import logger

from .config import BRIDGE_BASE_URL, WARMUP_INIT_RETRIES, WARMUP_INIT_DELAY_S
from .bridge import initialize_once
from .router import router
from .anthropic_router import anthropic_router, build_streaming_response_for_account
from .token_manager import TokenManager, _extract_exp_from_jwt
from .auth import verify_admin_token, authenticate_request
from .anthropic_models import AnthropicMessagesRequest

# 导入账号管理模块
from warp2protobuf.config.settings import (
    ACCOUNT_DB_PATH,
    ACCOUNT_ADMIN_ENABLED,
    ACCOUNT_REGISTER_ENABLED,
    ACCOUNT_SELECT_STRATEGY,
)
from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.core.db import get_connection
from warp2protobuf.core.account_selector import (
    AccountSelector,
    VALID_STRATEGIES,
    set_runtime_strategy,
    get_current_strategy,
)

# 记录启动时间
_START_TIME = time.time()


app = FastAPI(title="Warp Bridge API - Anthropic Messages")
# OpenAI 兼容层已禁用 —— 如需恢复，取消下行注释
# app.include_router(router)
app.include_router(anthropic_router)

# 挂载静态文件（GUI）
SCRIPT_DIR = Path(__file__).parent.parent
STATIC_DIR = SCRIPT_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    logger.info("✅ 静态文件服务已启用: /static")


@app.get("/healthz")
async def health_check() -> dict[str, str]:
    """容器健康检查端点。"""
    return {
        "status": "ok",
        "service": "Warp Bridge API - Anthropic Messages",
    }


# GUI 入口
@app.get("/gui")
async def serve_gui():
    """账号管理 GUI 页面"""
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="GUI not found")
    return FileResponse(index_file)


@app.get("/test")
async def serve_test_page():
    """Anthropic 请求测试页面"""
    test_file = STATIC_DIR / "test.html"
    if not test_file.exists():
        raise HTTPException(status_code=404, detail="Test page not found")
    return FileResponse(test_file)


# 账号管理 API
class UpdateLimitRequest(BaseModel):
    total_limit: int
    used_limit: int


class UpdateStatusRequest(BaseModel):
    status: str


class AnthropicTestRequest(BaseModel):
    account_id: int = Field(..., ge=1)
    model: str = "claude-4-6-opus-high"
    prompt: str = Field(..., min_length=1)
    system: str = ""
    max_tokens: int = Field(1024, ge=1, le=8192)


class BatchUpdateStatusRequest(BaseModel):
    account_ids: list[int]
    status: str


def _fetch_account_tokens(db_path: Path, account_id: int) -> dict[str, Any] | None:
    with get_connection(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, id_token, refresh_token, status, api_key FROM accounts WHERE id = ?",
            (account_id,),
        )
        row = cursor.fetchone()

    return dict(row) if row else None


@app.get("/v1/models")
async def list_models_v1(request: Request) -> dict[str, Any]:
    """OpenAI/Anthropic 客户端常用模型列表端点。"""
    await authenticate_request(request)

    from warp2protobuf.config.models import get_all_unique_models

    return {
        "object": "list",
        "data": get_all_unique_models(),
    }


@app.get("/api/models", dependencies=[Depends(verify_admin_token)])
async def list_available_models() -> dict[str, Any]:
    """返回可用模型列表（供测试页下拉选择）。"""
    from warp2protobuf.config.models import get_all_unique_models

    return {
        "success": True,
        "models": get_all_unique_models(),
    }


@app.get("/api/accounts", dependencies=[Depends(verify_admin_token)])
async def list_accounts(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    keyword: str = Query(None),
    status: str = Query(None),
):
    """获取账号列表（分页）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    accounts, total = store.get_accounts(page, page_size, keyword, status)

    return {
        "success": True,
        "accounts": accounts,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@app.get("/api/accounts/summary", dependencies=[Depends(verify_admin_token)])
async def get_accounts_summary():
    """获取账号汇总统计"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    summary = store.get_summary()

    return {
        "success": True,
        "summary": summary,
    }


@app.patch("/api/accounts/{account_id}/limit", dependencies=[Depends(verify_admin_token)])
async def update_account_limit(account_id: int, request: UpdateLimitRequest):
    """更新账号额度"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    success = store.update_limit(account_id, request.total_limit, request.used_limit)

    if not success:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "message": "Limit updated successfully",
    }


@app.patch("/api/accounts/{account_id}/status", dependencies=[Depends(verify_admin_token)])
async def update_account_status(account_id: int, request: UpdateStatusRequest):
    """更新单个账号启用/禁用状态"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    if request.status not in ("available", "disabled"):
        raise HTTPException(status_code=400, detail="status must be 'available' or 'disabled'")

    store = AccountStore(ACCOUNT_DB_PATH)
    success = store.update_status(account_id, request.status)

    if not success:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "account_id": account_id,
        "status": request.status,
    }


@app.post("/api/accounts/batch-status", dependencies=[Depends(verify_admin_token)])
async def batch_update_account_status(request: BatchUpdateStatusRequest):
    """批量更新账号启用/禁用状态"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    if request.status not in ("available", "disabled"):
        raise HTTPException(status_code=400, detail="status must be 'available' or 'disabled'")

    if not request.account_ids:
        raise HTTPException(status_code=400, detail="account_ids must not be empty")

    store = AccountStore(ACCOUNT_DB_PATH)
    updated = store.batch_update_status(request.account_ids, request.status)

    return {
        "success": True,
        "updated": updated,
        "status": request.status,
    }


@app.get("/api/accounts/pool/status", dependencies=[Depends(verify_admin_token)])
async def get_pool_status():
    """获取账号池实时状态"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    status = selector.get_pool_status()

    return {
        "success": True,
        "status": status,
    }


@app.post("/api/accounts/{account_id}/record-usage", dependencies=[Depends(verify_admin_token)])
async def record_account_usage(account_id: int, tokens: int = 0):
    """手动记录账号使用（测试用）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    success = selector.record_usage(account_id, tokens)

    if not success:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "message": f"Recorded {tokens} tokens usage",
    }


@app.post("/api/test/anthropic/messages", dependencies=[Depends(verify_admin_token)])
async def test_anthropic_messages(payload: AnthropicTestRequest, request: Request) -> StreamingResponse:
    """指定账号发送 Anthropic Messages 测试请求（SSE）。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    await authenticate_request(request)

    req = AnthropicMessagesRequest(
        model=payload.model,
        max_tokens=payload.max_tokens,
        stream=True,
        system=payload.system,
        messages=[
            {
                "role": "user",
                "content": payload.prompt,
            }
        ],
    )

    return await build_streaming_response_for_account(req, payload.account_id)


@app.get("/api/accounts/current", dependencies=[Depends(verify_admin_token)])
async def get_current_account():
    """获取最近使用的账号（替代当前账号）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    recent = selector.get_recently_used_accounts(limit=1)

    if not recent:
        return {
            "success": False,
            "message": "No account has been used yet",
            "account": None,
        }

    return {
        "success": True,
        "account": recent[0],
    }


@app.get("/api/accounts/recent", dependencies=[Depends(verify_admin_token)])
async def get_recent_accounts():
    """获取最近使用的账号列表"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    recent = selector.get_recently_used_accounts(limit=10)

    return {
        "success": True,
        "accounts": recent,
        "total": len(recent),
    }


@app.get("/api/accounts/{account_id}", dependencies=[Depends(verify_admin_token)])
async def get_account_detail(account_id: int):
    """获取单个账号详情"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    account = store.get_account_by_id(account_id)

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "account": account,
    }


def _contains_http_status(msg: str, status_code: int) -> bool:
    patterns = (
        fr"\bhttp\s+{status_code}\b",
        fr"\bstatus\s+{status_code}\b",
    )
    return any(re.search(pattern, msg) for pattern in patterns)


def _is_auth_error(err: Exception) -> bool:
    msg = str(err).lower()
    auth_keywords = (
        "unauthorized",
        "invalid token",
        "token invalid",
        "expired token",
        "token expired",
        "unauthenticated",
        "invalid_grant",
        "not authenticated",
    )
    return any(keyword in msg for keyword in auth_keywords) or _contains_http_status(msg, 401)


def _is_transient_error(err: Exception) -> bool:
    if isinstance(err, (httpx.TimeoutException, httpx.TransportError)):
        return True

    msg = str(err).lower()
    transient_keywords = (
        "timeout",
        "timed out",
        "too many requests",
        "rate limit",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "connection reset",
        "temporar",
        "try again",
    )
    if any(keyword in msg for keyword in transient_keywords):
        return True

    return (
        _contains_http_status(msg, 429)
        or _contains_http_status(msg, 502)
        or _contains_http_status(msg, 503)
    )


def _save_refreshed_token(account_id: int, new_token: str) -> None:
    """将刷新后的 token 写回数据库，更新 token_expires_at。"""
    try:
        expires_at = _extract_exp_from_jwt(new_token) or (time.time() + 3600)
        with get_connection(ACCOUNT_DB_PATH) as conn:
            conn.execute(
                "UPDATE accounts SET id_token = ?, token_expires_at = ?, updated_at = ? WHERE id = ?",
                (new_token, expires_at, datetime.now().isoformat(), account_id),
            )
            conn.commit()
        logger.info("[SaveToken] 已保存刷新后的 token: account_id=%d expires_in=60min", account_id)
    except Exception as e:
        logger.warning("[SaveToken] 保存 token 失败: account_id=%d error=%s", account_id, e)


async def _resolve_quota_with_token_strategy(
    id_token: str | None,
    refresh_token: str | None,
    api_key: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    """查询额度，如果触发了 token 刷新则一并返回新 token。

    优先级：
    1. wk-1.xxx API key（永不过期，无需 Firebase）
    2. 未过期的 Firebase id_token
    3. 刷新 Firebase id_token

    Returns:
        (quota_info, refreshed_token) — refreshed_token 为 None 表示未刷新。
    """
    from warp2protobuf.core.auth import refresh_access_token_with_refresh_token, is_token_expired
    from warp2protobuf.core.quota import get_request_limit_info

    token = ""
    refreshed_token: str | None = None
    from_id_token = False

    # wk-1 API key 优先：直接用于额度查询，无需 Firebase
    if api_key and api_key.startswith("wk-"):
        quota = await get_request_limit_info(api_key)
        return quota, None

    # 检查 Firebase 全局限流，避免绕过 TokenManager 直接触发 429
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    firebase_blocked = token_mgr.is_firebase_blocked()

    if id_token and not is_token_expired(id_token, buffer_minutes=0):
        token = id_token
        from_id_token = True
    elif refresh_token:
        if firebase_blocked:
            raise RuntimeError("Firebase 全局限流中，无法刷新 token 查询额度，请稍后重试")
        token = await refresh_access_token_with_refresh_token(refresh_token)
        refreshed_token = token
    else:
        raise RuntimeError("missing valid id_token and refresh_token")

    try:
        quota = await get_request_limit_info(token)
        return quota, refreshed_token
    except Exception as first_error:
        if from_id_token and refresh_token and _is_auth_error(first_error):
            if firebase_blocked:
                raise RuntimeError("Firebase 全局限流中，无法刷新 token 重试额度查询") from first_error
            token = await refresh_access_token_with_refresh_token(refresh_token)
            refreshed_token = token
            quota = await get_request_limit_info(token)
            return quota, refreshed_token
        raise


@app.post("/api/accounts/verify-quota", dependencies=[Depends(verify_admin_token)])
async def batch_verify_quota() -> dict[str, Any]:
    """批量验证所有账号的真实额度（GraphQL GetRequestLimitInfo）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    # 检查 Firebase 全局限流
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    if token_mgr.is_firebase_blocked():
        remaining = int((TokenManager._firebase_blocked_until - time.time()) / 60)
        return {
            "success": False,
            "error": f"Firebase 全局限流中，剩余冷却 {remaining} 分钟，请稍后重试",
        }

    store = AccountStore(ACCOUNT_DB_PATH)
    accounts, total = store.get_accounts(page=1, page_size=500)

    results = {"total": total, "valid": 0, "invalid": 0, "details": []}

    for account in accounts:
        account_id = account["id"]
        email = account["email"]

        # 需要完整 token（放到线程中执行，避免阻塞事件循环）
        row = await asyncio.to_thread(_fetch_account_tokens, store.db_path, account_id)

        if not row:
            results["invalid"] += 1
            results["details"].append({
                "id": account_id,
                "email": email,
                "valid": False,
                "error": "account not found",
            })
            continue

        id_token = row["id_token"]
        refresh_token = row["refresh_token"]
        row_api_key = row.get("api_key", "") or ""

        if not id_token and not refresh_token and not row_api_key.startswith("wk-"):
            results["invalid"] += 1
            results["details"].append({
                "id": account_id,
                "email": email,
                "valid": False,
                "error": "missing id_token and refresh_token",
            })
            continue

        try:
            quota_info, new_token = await _resolve_quota_with_token_strategy(id_token, refresh_token, api_key=row_api_key)
            total_limit = quota_info["request_limit"]
            used_limit = quota_info["used"]
            remaining = quota_info.get("remaining", total_limit - used_limit)

            store.update_limit(account_id, total_limit, used_limit)
            # 额度归零 → exhausted；有额度 + 非正常状态 → 恢复 available
            if remaining <= 0:
                store.update_status(account_id, "exhausted")
            elif account.get("status") in ("exhausted", "token_expired") and remaining > 0:
                store.update_status(account_id, "available")
            # 如果触发了 token 刷新，写回数据库
            if new_token:
                _save_refreshed_token(account_id, new_token)
            results["valid"] += 1
            results["details"].append({
                "id": account_id,
                "email": email,
                "valid": True,
                "error": None,
                "quota": quota_info,
            })
        except Exception as e:
            error_msg = str(e)[:200]
            results["invalid"] += 1
            if _is_auth_error(e):
                store.update_status(account_id, "token_expired")
            elif _is_transient_error(e):
                logger.warning("[VerifyQuota] transient error account_id=%s: %s", account_id, error_msg)
            # 检测 429 → 触发全局限流并中断批量操作
            if "429" in error_msg or "rate" in error_msg.lower():
                token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
                token_mgr.set_firebase_blocked()
                logger.error("[VerifyQuota] 检测到 429 限流，中断批量验证")
                results["details"].append({
                    "id": account_id,
                    "email": email,
                    "valid": False,
                    "error": error_msg,
                })
                break
            results["details"].append({
                "id": account_id,
                "email": email,
                "valid": False,
                "error": error_msg,
            })

        # 避免请求过快被限流
        await asyncio.sleep(1.5)

    logger.info(
        "[VerifyQuota] Batch done: total=%d valid=%d invalid=%d",
        total, results["valid"], results["invalid"],
    )
    return {"success": True, "results": results}


@app.post("/api/accounts/{account_id}/verify-quota", dependencies=[Depends(verify_admin_token)])
async def verify_single_quota(account_id: int) -> dict[str, Any]:
    """验证单个账号的真实额度（GraphQL GetRequestLimitInfo）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)

    # 获取完整 token（放到线程中执行，避免阻塞事件循环）
    row = await asyncio.to_thread(_fetch_account_tokens, store.db_path, account_id)

    if not row:
        raise HTTPException(status_code=404, detail="Account not found")

    email = row["email"]
    id_token = row["id_token"]
    refresh_token = row["refresh_token"]
    row_api_key = row.get("api_key", "") or ""

    if not id_token and not refresh_token and not row_api_key.startswith("wk-"):
        return {
            "success": True,
            "result": {
                "id": account_id,
                "email": email,
                "valid": False,
                "error": "missing id_token and refresh_token",
            },
        }

    try:
        quota_info, new_token = await _resolve_quota_with_token_strategy(id_token, refresh_token, api_key=row_api_key)
        total_limit = quota_info["request_limit"]
        used_limit = quota_info["used"]
        remaining = quota_info.get("remaining", total_limit - used_limit)

        store.update_limit(account_id, total_limit, used_limit)
        # 额度归零 → exhausted；有额度 + 非正常状态 → 恢复 available
        current_status = row.get("status")
        if remaining <= 0:
            store.update_status(account_id, "exhausted")
        elif current_status in ("exhausted", "token_expired") and remaining > 0:
            store.update_status(account_id, "available")
        if new_token:
            _save_refreshed_token(account_id, new_token)

        return {
            "success": True,
            "result": {
                "id": account_id,
                "email": email,
                "valid": True,
                "error": None,
                "quota": quota_info,
            },
        }
    except Exception as e:
        error_msg = str(e)[:200]
        if _is_auth_error(e):
            store.update_status(account_id, "token_expired")
        elif _is_transient_error(e):
            logger.warning("[VerifyQuota] transient error account_id=%s: %s", account_id, error_msg)
        return {
            "success": True,
            "result": {"id": account_id, "email": email, "valid": False, "error": error_msg},
        }


@app.post("/api/accounts/{account_id}/force-refresh", dependencies=[Depends(verify_admin_token)])
async def force_refresh_single(account_id: int) -> dict[str, Any]:
    """强制刷新单个账号 Token（清除 Firebase 限流 + 单账号冷却）。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    row = await asyncio.to_thread(_fetch_account_tokens, store.db_path, account_id)
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")

    email = row["email"]
    refresh_token = row["refresh_token"]
    if not refresh_token:
        return {"success": False, "detail": f"{email}: 无 refresh_token，无法刷新"}

    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    # 清除全局 Firebase 限流 + 该账号的失败冷却
    was_blocked = token_mgr.clear_firebase_block()
    token_mgr._refresh_failures.pop(account_id, None)

    try:
        from warp2protobuf.core.auth import refresh_access_token_with_refresh_token
        new_id_token = await refresh_access_token_with_refresh_token(refresh_token)
        if not new_id_token:
            return {"success": False, "detail": f"{email}: 刷新返回空 token"}

        # 更新数据库
        expires_at = time.time() + 3600
        with get_connection(ACCOUNT_DB_PATH) as conn:
            conn.execute(
                "UPDATE accounts SET id_token = ?, token_expires_at = ?, updated_at = ? WHERE id = ?",
                (new_id_token, expires_at, datetime.now().isoformat(), account_id),
            )
            conn.commit()

        logger.info("[ForceRefresh] 单账号刷新成功: %s (was_firebase_blocked=%s)", email, was_blocked)
        return {
            "success": True,
            "message": f"{email}: Token 刷新成功",
            "was_firebase_blocked": was_blocked,
            "expires_in_min": 60,
        }
    except Exception as e:
        error_msg = str(e)[:200]
        logger.error("[ForceRefresh] 单账号刷新失败: %s — %s", email, error_msg)
        return {"success": False, "detail": f"{email}: {error_msg}"}


class RegisterRequest(BaseModel):
    count: int = 5
    delay_s: float = 5.0
    default_quota: int = 300


class FeatureFlagsResponse(BaseModel):
    account_admin_enabled: bool
    account_register_enabled: bool


@app.delete("/api/accounts/{account_id}", dependencies=[Depends(verify_admin_token)])
async def delete_account(account_id: int) -> dict[str, Any]:
    """删除单个账号。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")
    store = AccountStore(ACCOUNT_DB_PATH)
    deleted = store.delete_account(account_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"success": True, "message": f"账号 #{account_id} 已删除"}


class BatchDeleteRequest(BaseModel):
    ids: list[int]


@app.post("/api/accounts/batch-delete", dependencies=[Depends(verify_admin_token)])
async def batch_delete_accounts(req: BatchDeleteRequest) -> dict[str, Any]:
    """批量删除账号。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")
    if not req.ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")
    store = AccountStore(ACCOUNT_DB_PATH)
    count = store.batch_delete_accounts(req.ids)
    return {"success": True, "message": f"已删除 {count} 个账号", "count": count}


@app.get("/api/config", dependencies=[Depends(verify_admin_token)])
async def get_feature_flags() -> dict[str, Any]:
    """返回前端可见的功能开关状态。"""
    return {
        "success": True,
        "features": {
            **FeatureFlagsResponse(
                account_admin_enabled=ACCOUNT_ADMIN_ENABLED,
                account_register_enabled=ACCOUNT_REGISTER_ENABLED,
            ).model_dump(),
            "account_select_strategy": get_current_strategy(),
            "available_strategies": list(VALID_STRATEGIES),
        },
    }


@app.post("/api/accounts/register", dependencies=[Depends(verify_admin_token)])
async def register_accounts(request: RegisterRequest) -> dict[str, Any]:
    """批量注册新 Warp 账号（Firebase email/password signUp）并自动入库"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    if not ACCOUNT_REGISTER_ENABLED:
        raise HTTPException(
            status_code=403,
            detail="Account registration is temporarily disabled",
        )

    if request.count < 1 or request.count > 50:
        raise HTTPException(status_code=400, detail="count must be 1-50")

    from warp2protobuf.core.account_register import batch_register

    results = await batch_register(
        count=request.count,
        delay_s=request.delay_s,
        default_quota=request.default_quota,
    )

    # 将成功注册的账号写入数据库
    store = AccountStore(ACCOUNT_DB_PATH)
    saved = 0
    for r in results:
        if r["success"] and r["account"]:
            try:
                store.upsert_account(r["account"])
                saved += 1
            except Exception as e:
                logger.warning("[Register] Failed to save account: %s", e)

    total_ok = sum(1 for r in results if r["success"])
    total_fail = sum(1 for r in results if not r["success"])

    logger.info(
        "[Register] Batch register done: requested=%d success=%d fail=%d saved=%d",
        request.count, total_ok, total_fail, saved,
    )

    return {
        "success": True,
        "message": f"Registered {total_ok} accounts, saved {saved} to database",
        "stats": {
            "requested": request.count,
            "success": total_ok,
            "failed": total_fail,
            "saved_to_db": saved,
        },
        "details": [
            {
                "email": r["account"]["email"] if r["account"] else None,
                "success": r["success"],
                "error": r["error"],
            }
            for r in results
        ],
    }


@app.post("/api/tokens/refresh", dependencies=[Depends(verify_admin_token)])
async def manual_token_refresh():
    """手动触发全量 Token 刷新"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    stats = await token_mgr.batch_refresh_all()

    return {
        "success": True,
        "message": "Token refresh completed",
        "stats": stats,
    }


class StrategyRequest(BaseModel):
    strategy: str


@app.put("/api/accounts/strategy", dependencies=[Depends(verify_admin_token)])
async def change_strategy(req: StrategyRequest) -> dict[str, Any]:
    """运行时切换账号选择策略。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")
    try:
        actual = set_runtime_strategy(req.strategy)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "success": True,
        "strategy": actual,
        "available_strategies": list(VALID_STRATEGIES),
    }


@app.post("/api/accounts/reset-usage", dependencies=[Depends(verify_admin_token)])
async def reset_account_usage() -> dict[str, Any]:
    """重置所有账号的用量统计（use_count, used_limit, last_used）。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")
    store = AccountStore(ACCOUNT_DB_PATH)
    count = store.reset_usage()
    return {"success": True, "message": f"已重置 {count} 个账号的用量数据", "count": count}


@app.post("/api/tokens/force-refresh", dependencies=[Depends(verify_admin_token)])
async def force_token_refresh() -> dict[str, Any]:
    """强制刷新 Token（清除 Firebase 限流标记后执行）。"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    # 清除 Firebase 限流标记
    was_blocked = token_mgr.clear_firebase_block()
    token_mgr._refresh_failures.clear()
    logger.info("[ForceRefresh] Firebase 限流标记已清除 (was_blocked=%s)", was_blocked)
    # 执行全量刷新
    stats = await token_mgr.batch_refresh_all()
    return {
        "success": True,
        "message": "强制刷新完成" + (" (已清除 Firebase 限流)" if was_blocked else ""),
        "was_firebase_blocked": was_blocked,
        "stats": stats,
    }


@app.get("/api/system/stats", dependencies=[Depends(verify_admin_token)])
async def get_system_stats() -> dict[str, Any]:
    """获取系统监控信息"""
    try:
        process = psutil.Process(os.getpid())

        # CPU 使用率（需要间隔测量）
        cpu_percent = process.cpu_percent(interval=0.1)

        # 内存信息
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024

        # 系统总内存
        system_memory = psutil.virtual_memory()

        # 运行时间
        uptime_seconds = time.time() - _START_TIME
        uptime_hours = uptime_seconds / 3600

        # 线程数
        num_threads = process.num_threads()

        return {
            "success": True,
            "stats": {
                "process": {
                    "pid": os.getpid(),
                    "cpu_percent": round(cpu_percent, 2),
                    "memory_mb": round(memory_mb, 2),
                    "memory_percent": round(process.memory_percent(), 2),
                    "num_threads": num_threads,
                    "uptime_seconds": round(uptime_seconds, 2),
                    "uptime_hours": round(uptime_hours, 2),
                },
                "system": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": round(system_memory.total / 1024 / 1024 / 1024, 2),
                    "memory_available_gb": round(system_memory.available / 1024 / 1024 / 1024, 2),
                    "memory_percent": round(system_memory.percent, 2),
                }
            }
        }
    except Exception as e:
        logger.exception("[SystemStats] failed to collect system stats")
        return {
            "success": False,
            "error": "failed to collect system stats"
        }


async def _startup_prefresh_tokens(token_mgr: TokenManager) -> None:
    """启动时预刷新前几个过期账号的 token，确保服务就绪时有可用 token。

    只刷新最多 3 个账号，每个间隔 3 秒，避免触发 Firebase 限流。
    """
    from warp2protobuf.core.auth import is_token_expired, refresh_access_token_with_refresh_token

    max_prefresh = 3
    try:
        with get_connection(str(ACCOUNT_DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, email, id_token, refresh_token FROM accounts "
                "WHERE status = 'available' AND refresh_token IS NOT NULL AND refresh_token != '' "
                "LIMIT 20"
            ).fetchall()
    except Exception as e:
        logger.warning("[StartupPrefresh] 读取账号失败: %s", e)
        return

    refreshed = 0
    for row in rows:
        if refreshed >= max_prefresh:
            break
        account = dict(row)
        id_token = account.get("id_token", "")
        # 只刷新已过期的
        if id_token and not is_token_expired(id_token, buffer_minutes=5):
            continue
        refresh_token = account.get("refresh_token", "")
        if not refresh_token:
            continue
        try:
            logger.info(
                "[StartupPrefresh] 预刷新 account_id=%d email=%s",
                account["id"], account.get("email", "?"),
            )
            new_token = await refresh_access_token_with_refresh_token(refresh_token)
            if new_token:
                token_mgr._update_token_in_db(account["id"], new_token)
                refreshed += 1
                logger.info("[StartupPrefresh] account_id=%d 刷新成功", account["id"])
        except Exception as exc:
            err_msg = str(exc)
            logger.warning("[StartupPrefresh] account_id=%d 刷新失败: %s", account["id"], exc)
            if "429" in err_msg or "rate" in err_msg.lower():
                token_mgr.set_firebase_blocked()
                logger.error("[StartupPrefresh] 遇到 429 限流，停止预刷新")
                break
        await asyncio.sleep(3)

    logger.info("[StartupPrefresh] 启动预刷新完成，刷新了 %d 个账号", refreshed)


@app.on_event("startup")
async def _on_startup():
    try:
        logger.info("[Warp Bridge] Server starting. BRIDGE_BASE_URL=%s", BRIDGE_BASE_URL)
        logger.info("[Warp Bridge] Endpoints: POST /v1/messages (Anthropic), /api/* (管理), /gui")
    except Exception:
        pass

    url = f"{BRIDGE_BASE_URL}/healthz"
    retries = WARMUP_INIT_RETRIES
    delay_s = WARMUP_INIT_DELAY_S
    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient(timeout=5.0, trust_env=True) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                logger.info("[Warp Bridge] Bridge server is ready at %s", url)
                break
            else:
                logger.warning("[Warp Bridge] Bridge health at %s -> HTTP %s", url, resp.status_code)
        except Exception as e:
            logger.warning("[Warp Bridge] Bridge health attempt %s/%s failed: %s", attempt, retries, e)
        await asyncio.sleep(delay_s)
    else:
        logger.error("[Warp Bridge] Bridge server not ready at %s", url)

    try:
        await initialize_once()
    except Exception as e:
        logger.warning("[Warp Bridge] Warmup initialize_once on startup failed: %s", e)

    # 批量 Token 刷新已改为手动触发（POST /api/tokens/refresh）
    # 不再启动时自动刷新，避免 Firebase 限流
    logger.info("[Warp Bridge] Token 批量刷新已关闭自动启动，如需刷新请调用 POST /api/tokens/refresh")

    # 启动时预刷新前几个账号的 token，确保服务就绪时有可用 token
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    try:
        await _startup_prefresh_tokens(token_mgr)
    except Exception as e:
        logger.warning("[Warp Bridge] 启动预刷新失败: %s", e)

    # 启动后台预刷新：每 5 分钟检查即将过期的 token，提前 10 分钟刷新
    # 这是轻量级的，只刷新快到期的，不会一次性刷新所有账号
    token_mgr.start_background_refresh()


# 旧的全量定时刷新已废弃，由 TokenManager.start_background_refresh() 替代