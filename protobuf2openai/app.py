from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
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
from .token_manager import TokenManager
from .auth import verify_admin_token, authenticate_request
from .anthropic_models import AnthropicMessagesRequest

# 导入账号管理模块
from warp2protobuf.config.settings import (
    ACCOUNT_DB_PATH,
    ACCOUNT_ADMIN_ENABLED,
    ACCOUNT_REGISTER_ENABLED,
)
from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.core.account_selector import AccountSelector

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
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, id_token, refresh_token FROM accounts WHERE id = ?",
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


async def _resolve_quota_with_token_strategy(
    id_token: str | None,
    refresh_token: str | None,
) -> dict[str, Any]:
    from warp2protobuf.core.auth import refresh_access_token_with_refresh_token, is_token_expired
    from warp2protobuf.core.quota import get_request_limit_info

    token = ""
    from_id_token = False

    if id_token and not is_token_expired(id_token, buffer_minutes=0):
        token = id_token
        from_id_token = True
    elif refresh_token:
        token = await refresh_access_token_with_refresh_token(refresh_token)
    else:
        raise RuntimeError("missing valid id_token and refresh_token")

    try:
        return await get_request_limit_info(token)
    except Exception as first_error:
        if from_id_token and refresh_token and _is_auth_error(first_error):
            refreshed_token = await refresh_access_token_with_refresh_token(refresh_token)
            return await get_request_limit_info(refreshed_token)
        raise


@app.post("/api/accounts/verify-quota", dependencies=[Depends(verify_admin_token)])
async def batch_verify_quota() -> dict[str, Any]:
    """批量验证所有账号的真实额度（GraphQL GetRequestLimitInfo）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

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

        if not id_token and not refresh_token:
            results["invalid"] += 1
            results["details"].append({
                "id": account_id,
                "email": email,
                "valid": False,
                "error": "missing id_token and refresh_token",
            })
            continue

        try:
            quota_info = await _resolve_quota_with_token_strategy(id_token, refresh_token)
            total_limit = quota_info["request_limit"]
            used_limit = quota_info["used"]

            store.update_limit(account_id, total_limit, used_limit)
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
                store.update_status(account_id, "disabled")
            elif _is_transient_error(e):
                logger.warning("[VerifyQuota] transient error account_id=%s: %s", account_id, error_msg)
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

    if not id_token and not refresh_token:
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
        quota_info = await _resolve_quota_with_token_strategy(id_token, refresh_token)
        total_limit = quota_info["request_limit"]
        used_limit = quota_info["used"]

        store.update_limit(account_id, total_limit, used_limit)

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
            store.update_status(account_id, "disabled")
        elif _is_transient_error(e):
            logger.warning("[VerifyQuota] transient error account_id=%s: %s", account_id, error_msg)
        return {
            "success": True,
            "result": {"id": account_id, "email": email, "valid": False, "error": error_msg},
        }


class RegisterRequest(BaseModel):
    count: int = 5
    delay_s: float = 5.0
    default_quota: int = 300


class FeatureFlagsResponse(BaseModel):
    account_admin_enabled: bool
    account_register_enabled: bool


@app.get("/api/config", dependencies=[Depends(verify_admin_token)])
async def get_feature_flags() -> dict[str, Any]:
    """返回前端可见的功能开关状态。"""
    return {
        "success": True,
        "features": FeatureFlagsResponse(
            account_admin_enabled=ACCOUNT_ADMIN_ENABLED,
            account_register_enabled=ACCOUNT_REGISTER_ENABLED,
        ).model_dump(),
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
        await asyncio.to_thread(initialize_once)
    except Exception as e:
        logger.warning("[Warp Bridge] Warmup initialize_once on startup failed: %s", e)

    # 批量 Token 刷新已改为手动触发（POST /api/tokens/refresh）
    # 不再启动时自动刷新，避免 Firebase 限流
    logger.info("[Warp Bridge] Token 批量刷新已关闭自动启动，如需刷新请调用 POST /api/tokens/refresh")

    # 启动后台预刷新：每 5 分钟检查即将过期的 token，提前 10 分钟刷新
    # 这是轻量级的，只刷新快到期的，不会一次性刷新所有账号
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    token_mgr.start_background_refresh()


# 旧的全量定时刷新已废弃，由 TokenManager.start_background_refresh() 替代