"""
账号级 Token 生命周期管理器

解决问题：
1. Firebase id_token 精确 60 分钟过期 → 到期前自动预刷新
2. os.environ["WARP_JWT"] 并发竞态 → asyncio.Lock 隔离
3. 429 后盲目重试 → 全局 Firebase 限流保护
4. 记录 token_expires_at → 精准判断过期，避免重复解码 JWT
"""
from __future__ import annotations

import asyncio
import base64
import json
import random
import sqlite3
import time
from datetime import datetime
from typing import Dict, Optional

from warp2protobuf.core.auth import (
    is_token_expired,
    refresh_access_token_with_refresh_token,
)
from warp2protobuf.core.db import get_connection
from warp2protobuf.core.logging import logger

# Token 有效期常量（Firebase id_token = 60 分钟）
TOKEN_LIFETIME_SECONDS = 3600
# 提前多少秒开始预刷新（缩短到 3 分钟，减少不必要的刷新）
PRE_REFRESH_BUFFER_SECONDS = 180
# 活跃账号窗口（已不再用于预刷新过滤，保留供其他模块参考）
ACTIVE_ACCOUNT_WINDOW_SECONDS = 7200
# Jitter 最大偏移（秒），防止雷群效应
MAX_JITTER_SECONDS = 120
# 按需刷新最大并发数（防止并发请求同时打 Firebase）
MAX_CONCURRENT_REFRESH = 2

from .utils import safe_create_task


class TokenManager:
    """单例 Token 管理器，管理所有账号的 token 生命周期。"""

    _instance: Optional["TokenManager"] = None
    _lock: asyncio.Lock  # 全局锁，保护 os.environ["WARP_JWT"]

    # 全局 Firebase 限流标记（类级别，所有实例共享）
    _firebase_blocked_until: float = 0.0
    _FIREBASE_COOLDOWN = 600  # Firebase 限流后全局冷却 10 分钟

    # 后台预刷新任务引用
    _refresh_task: Optional[asyncio.Task] = None

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._per_account_locks: Dict[int, asyncio.Lock] = {}
        self._refresh_failures: Dict[int, float] = {}
        self._FAILURE_COOLDOWN = 60
        self._ensure_schema()

    def _get_account_lock(self, account_id: int) -> asyncio.Lock:
        """获取 per-account 刷新锁，防止同一账号并发刷新。"""
        if account_id not in self._per_account_locks:
            self._per_account_locks[account_id] = asyncio.Lock()
        return self._per_account_locks[account_id]

    def _ensure_schema(self) -> None:
        """确保 accounts 表有 token_expires_at 列。"""
        try:
            with get_connection(str(self.db_path)) as conn:
                cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
                if "token_expires_at" not in cols:
                    conn.execute("ALTER TABLE accounts ADD COLUMN token_expires_at REAL DEFAULT 0")
                    conn.commit()
                    logger.info("[TokenManager] 已添加 token_expires_at 列")
        except Exception as e:
            logger.warning("[TokenManager] schema 检查失败: %s", e)

    @classmethod
    def get_instance(cls, db_path: str) -> "TokenManager":
        if cls._instance is None:
            cls._instance = cls(db_path)
        return cls._instance

    @classmethod
    def is_firebase_blocked(cls) -> bool:
        """检查 Firebase 是否处于全局限流冷却期。"""
        return time.time() < cls._firebase_blocked_until

    @classmethod
    def set_firebase_blocked(cls) -> None:
        """标记 Firebase 全局限流，所有刷新暂停。"""
        cls._firebase_blocked_until = time.time() + cls._FIREBASE_COOLDOWN
        remaining = cls._FIREBASE_COOLDOWN // 60
        logger.warning(
            "[TokenManager] ⚠️ Firebase 全局限流已触发，%d 分钟内不再尝试任何 token 刷新",
            remaining,
        )

    @classmethod
    def clear_firebase_block(cls) -> bool:
        """清除 Firebase 全局限流标记。返回之前是否处于限流状态。"""
        was_blocked = cls.is_firebase_blocked()
        cls._firebase_blocked_until = 0.0
        return was_blocked

    async def get_valid_token(self, account: Dict) -> Optional[str]:
        """获取账号的有效 access token，过期则自动刷新。

        优先级：
        1. wk-1.xxx API key（永不过期，无需 Firebase）
        2. 未过期的 Firebase id_token
        3. 刷新 Firebase id_token

        Args:
            account: 从 DB 查出的账号字典，需包含 id, id_token, refresh_token, email, api_key

        Returns:
            有效的 token（wk-1 key 或 id_token）；无法获取时返回 None
        """
        account_id = account["id"]

        # ── wk-1.xxx API key 优先：不需要 Firebase JWT ──
        api_key = account.get("api_key", "")
        if api_key and api_key.startswith("wk-"):
            logger.debug(
                "[TokenManager] account_id=%d 使用 wk API key（跳过 Firebase）",
                account_id,
            )
            return api_key

        id_token = account.get("id_token", "")
        refresh_token = account.get("refresh_token", "")

        # token 未过期，直接返回
        if id_token and not is_token_expired(id_token, buffer_minutes=5):
            return id_token

        # === Firebase 全局限流期间：过期 token 不返回，让调用方换号重试 ===
        if self.is_firebase_blocked():
            remaining = int((self._firebase_blocked_until - time.time()) / 60)
            logger.warning(
                "[TokenManager] Firebase 限流中（剩余 %d 分钟），account_id=%d token 已过期，返回 None 让调用方换号",
                remaining,
                account_id,
            )
            return None

        # 检查单账号冷却期
        fail_ts = self._refresh_failures.get(account_id, 0)
        if fail_ts and (time.time() - fail_ts) < self._FAILURE_COOLDOWN:
            # 冷却期内不返回过期 token
            logger.debug(
                "[TokenManager] account_id=%d 在冷却期，token 已过期，返回 None",
                account_id,
            )
            return None

        # token 过期或为空，需要刷新
        if not refresh_token:
            logger.error(
                "[TokenManager] account_id=%d 无 refresh_token，无法刷新",
                account_id,
            )
            return id_token or None  # 返回已有的，哪怕过期

        logger.info(
            "[TokenManager] account_id=%d email=%s token 已过期，正在刷新...",
            account_id,
            account.get("email", "?"),
        )

        try:
            # per-account 锁：同一账号同时只有一个刷新，其余排队
            account_lock = self._get_account_lock(account_id)
            async with account_lock:
                # 拿到锁后再检查一次：可能排队期间别的请求已经刷新了同一账号
                with get_connection(str(self.db_path)) as conn:
                    row = conn.execute(
                        "SELECT id_token FROM accounts WHERE id = ?",
                        (account_id,),
                    ).fetchone()
                if row and row[0]:
                    fresh_token = row[0]
                    if not is_token_expired(fresh_token, buffer_minutes=5):
                        logger.info(
                            "[TokenManager] account_id=%d 排队期间已被其他请求刷新，直接使用",
                            account_id,
                        )
                        return fresh_token

                new_token = await refresh_access_token_with_refresh_token(refresh_token)
                if not new_token:
                    raise RuntimeError("refresh 返回空 token")

                # 回写 DB（在锁内，确保后续排队者能读到新 token）
                self._update_token_in_db(account_id, new_token)
                self._refresh_failures.pop(account_id, None)

                logger.info(
                    "[TokenManager] account_id=%d token 刷新成功",
                    account_id,
                )
                # 异步同步额度（不阻塞请求热路径）
                safe_create_task(self.sync_account_quota(account_id, new_token))
                return new_token

        except Exception as exc:
            err_msg = str(exc)
            logger.error(
                "[TokenManager] account_id=%d token 刷新失败: %s",
                account_id,
                exc,
            )
            self._refresh_failures[account_id] = time.time()

            # 检测 429 → 触发全局 Firebase 限流
            if "429" in err_msg or "rate" in err_msg.lower():
                self.set_firebase_blocked()

            # 刷新失败 → 返回 None，让调用方换号重试，不再返回过期 token
            return None

    def _update_token_in_db(self, account_id: int, new_token: str) -> None:
        """将刷新后的 token 和过期时间回写数据库。"""
        now = datetime.now().isoformat()
        expires_at = _extract_exp_from_jwt(new_token)
        with get_connection(str(self.db_path)) as conn:
            conn.execute(
                """
                UPDATE accounts SET
                    id_token = ?,
                    token_expires_at = ?,
                    last_refresh_time = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (new_token, expires_at, now, now, account_id),
            )
            conn.commit()
        if expires_at > 0:
            remaining = int((expires_at - time.time()) / 60)
            logger.info(
                "[TokenManager] account_id=%d token 已写入 DB，%d 分钟后过期",
                account_id, remaining,
            )

    def mark_account_failed(self, account_id: int) -> None:
        """标记账号 429/失败，进入冷却期。"""
        self._refresh_failures[account_id] = time.time()
        logger.warning(
            "[TokenManager] account_id=%d 标记为失败，冷却 %ds",
            account_id,
            self._FAILURE_COOLDOWN,
        )

    def mark_account_revoked(self, account_id: int) -> None:
        """标记账号 API key 被吊销（401/403），设置 status='token_expired' 从选择池中剔除。"""
        try:
            with get_connection(str(self.db_path)) as conn:
                now = datetime.now().isoformat()
                conn.execute(
                    "UPDATE accounts SET status = 'token_expired', updated_at = ? WHERE id = ?",
                    (now, account_id),
                )
                conn.commit()
            logger.warning(
                "[TokenManager] account_id=%d API key 被吊销(401/403)，已标记为 token_expired",
                account_id,
            )
        except Exception as exc:
            logger.error(
                "[TokenManager] account_id=%d 标记 token_expired 失败: %s",
                account_id, exc,
            )

    def mark_account_exhausted(self, account_id: int) -> None:
        """标记账号额度耗尽，设置 status='exhausted' 从选择池中剔除。"""
        try:
            with get_connection(str(self.db_path)) as conn:
                now = datetime.now().isoformat()
                conn.execute(
                    "UPDATE accounts SET status = 'exhausted', updated_at = ? WHERE id = ?",
                    (now, account_id),
                )
                conn.commit()
            logger.warning(
                "[TokenManager] account_id=%d 额度耗尽，已标记为 exhausted",
                account_id,
            )
        except Exception as exc:
            logger.error(
                "[TokenManager] account_id=%d 标记 exhausted 失败: %s",
                account_id, exc,
            )

    async def sync_account_quota(self, account_id: int, access_token: str) -> None:
        """查询 Warp 服务端真实额度并同步到本地 DB。"""
        try:
            from warp2protobuf.core.quota import get_request_limit_info
            quota = await get_request_limit_info(access_token)
            total = quota.get("request_limit", 0)
            used = quota.get("used", 0)
            remaining = quota.get("remaining", 0)

            with get_connection(str(self.db_path)) as conn:
                now = datetime.now().isoformat()
                conn.execute(
                    """UPDATE accounts SET
                        total_limit = ?, used_limit = ?, last_check = ?, updated_at = ?
                    WHERE id = ?""",
                    (total, used, now, now, account_id),
                )
                conn.commit()

            logger.info(
                "[TokenManager] account_id=%d 额度已同步: total=%d used=%d remaining=%d",
                account_id, total, used, remaining,
            )

            # 额度耗尽则自动标记 exhausted；有额度则恢复 available（从 exhausted/token_expired）
            if remaining <= 0:
                self.mark_account_exhausted(account_id)
            else:
                # 查询成功说明 token 有效，如果之前是 token_expired/exhausted 则恢复
                with get_connection(str(self.db_path)) as conn:
                    row = conn.execute(
                        "SELECT status FROM accounts WHERE id = ?", (account_id,)
                    ).fetchone()
                    if row and row[0] in ("exhausted", "token_expired"):
                        conn.execute(
                            "UPDATE accounts SET status = 'available', updated_at = ? WHERE id = ?",
                            (datetime.now().isoformat(), account_id),
                        )
                        conn.commit()
                        logger.info("[TokenManager] account_id=%d 状态恢复: %s → available", account_id, row[0])

        except Exception as exc:
            # 额度查询失败不影响主流程，仅记录日志
            logger.warning(
                "[TokenManager] account_id=%d 额度同步失败（不影响 token 刷新）: %s",
                account_id, exc,
            )

    @property
    def env_lock(self) -> asyncio.Lock:
        """获取 os.environ 写入锁。"""
        return self._lock

    async def batch_refresh_all(self) -> Dict[str, int]:
        """批量预刷新所有账号的 token（手动触发）。

        遇到 429 立即停止整个批量操作，触发全局限流保护。

        Returns:
            {"total": N, "refreshed": N, "failed": N, "skipped": N}
        """
        stats = {"total": 0, "refreshed": 0, "failed": 0, "skipped": 0}

        # 全局限流期间拒绝批量刷新
        if self.is_firebase_blocked():
            remaining = int((self._firebase_blocked_until - time.time()) / 60)
            logger.warning(
                "[TokenManager] Firebase 全局限流中，批量刷新被拒绝，剩余冷却 %d 分钟",
                remaining,
            )
            return stats

        with get_connection(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, email, id_token, refresh_token, api_key FROM accounts WHERE status = 'available'"
            ).fetchall()

        stats["total"] = len(rows)
        logger.info("[TokenManager] 开始批量刷新 %d 个账号...", len(rows))

        for row in rows:
            # 每次循环检查全局限流
            if self.is_firebase_blocked():
                logger.warning("[TokenManager] 批量刷新中途触发限流，停止剩余账号")
                stats["skipped"] += stats["total"] - stats["refreshed"] - stats["failed"] - stats["skipped"]
                break

            account = dict(row)
            account_id = account["id"]
            id_token = account.get("id_token", "")

            # wk-1 API key 账号不需要 Firebase 刷新
            api_key = account.get("api_key", "") or ""
            if api_key.startswith("wk-"):
                stats["skipped"] += 1
                continue

            # 未过期的跳过
            if id_token and not is_token_expired(id_token, buffer_minutes=10):
                stats["skipped"] += 1
                continue

            refresh_token = account.get("refresh_token", "")
            if not refresh_token:
                stats["failed"] += 1
                continue

            try:
                new_token = await refresh_access_token_with_refresh_token(refresh_token)
                if new_token:
                    self._update_token_in_db(account_id, new_token)
                    stats["refreshed"] += 1
                else:
                    stats["failed"] += 1
            except Exception as exc:
                err_msg = str(exc)
                if "429" in err_msg or "rate" in err_msg.lower():
                    # 429 → 立即停止，触发全局限流
                    self.set_firebase_blocked()
                    logger.error(
                        "[TokenManager] 批量刷新遇到 429，立即停止。已刷新 %d 个",
                        stats["refreshed"],
                    )
                    break
                else:
                    logger.warning(
                        "[TokenManager] 批量刷新 account_id=%d 失败: %s",
                        account_id, exc,
                    )
                    stats["failed"] += 1

            # 每个账号间隔 5 秒
            await asyncio.sleep(5)

        logger.info(
            "[TokenManager] 批量刷新完成: total=%d refreshed=%d failed=%d skipped=%d",
            stats["total"],
            stats["refreshed"],
            stats["failed"],
            stats["skipped"],
        )
        return stats

    # ==================== 后台预刷新 ====================

    def start_background_refresh(self) -> None:
        """启动后台预刷新任务（在 app startup 中调用一次）。"""
        if self._refresh_task is not None and not self._refresh_task.done():
            logger.info("[TokenManager] 后台预刷新任务已在运行")
            return
        self._refresh_task = asyncio.create_task(self._background_refresh_loop())
        self._refresh_task.add_done_callback(self._on_refresh_task_done)
        logger.info(
            "[TokenManager] ✅ 后台预刷新已启动（动态间隔，到期前 %d 分钟刷新，仅刷新活跃账号）",
            PRE_REFRESH_BUFFER_SECONDS // 60,
        )

    def _on_refresh_task_done(self, task: asyncio.Task) -> None:
        """后台刷新任务结束回调：记录崩溃日志 + 自动重启。"""
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            logger.info("[TokenManager] 后台预刷新任务被取消")
            return
        if exc:
            logger.error("[TokenManager] 后台预刷新任务崩溃: %s，5 秒后自动重启", exc)
            # 延迟重启，避免快速循环崩溃
            loop = asyncio.get_running_loop()
            loop.call_later(5, self.start_background_refresh)

    async def _background_refresh_loop(self) -> None:
        """后台循环：动态计算下次检查时间，基于最近即将过期的 token。"""
        default_interval = 300  # 无账号时默认 5 分钟
        while True:
            next_interval = default_interval
            try:
                next_interval = await self._refresh_expiring_tokens() or default_interval
                # 限制在 60s ~ 600s 之间
                next_interval = max(60, min(600, next_interval))
            except Exception as e:
                logger.error("[TokenManager] 后台预刷新异常: %s", e)

            # 定期检查 exhausted 账号是否额度已恢复
            try:
                await self._check_exhausted_accounts()
            except Exception as e:
                logger.error("[TokenManager] exhausted 恢复检查异常: %s", e)

            await asyncio.sleep(next_interval)

    async def _refresh_expiring_tokens(self) -> int:
        """扫描即将过期的账号 token，逐个刷新。

        策略：
        1. 刷新所有 available 且即将过期（3 分钟内）的账号，不限活跃窗口
        2. 每个账号加随机 Jitter（防雷群效应）
        3. 每轮最多 10 个，防止单轮刷新过多

        Returns:
            建议的下次检查间隔（秒）。
        """
        if self.is_firebase_blocked():
            logger.debug("[TokenManager] Firebase 限流中，跳过预刷新")
            return 300

        now = time.time()
        threshold = now + PRE_REFRESH_BUFFER_SECONDS

        with get_connection(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            # 查询所有 available 且即将过期的账号（不再限制活跃窗口）
            rows = conn.execute(
                """
                SELECT id, email, id_token, refresh_token, token_expires_at, last_used, api_key
                FROM accounts
                WHERE status = 'available'
                  AND refresh_token IS NOT NULL
                  AND refresh_token != ''
                  AND (api_key IS NULL OR api_key = '' OR NOT api_key LIKE 'wk-%')
                  AND (
                    (token_expires_at > 0 AND token_expires_at < ?)
                    OR (token_expires_at = 0 AND id_token IS NOT NULL AND id_token != '')
                  )
                ORDER BY token_expires_at ASC
                LIMIT 10
                """,
                (threshold,),
            ).fetchall()

        if not rows:
            return self._calc_next_check_interval()

        # 过滤：token_expires_at=0 的需要额外检查是否真的过期
        # 有 wk-1 API key 的账号跳过 Firebase 刷新
        to_refresh = []
        for row in rows:
            account = dict(row)
            api_key = account.get("api_key", "") or ""
            if api_key.startswith("wk-"):
                continue  # wk-1 key 不需要 Firebase 刷新
            if account["token_expires_at"] > 0:
                to_refresh.append(account)
            else:
                id_token = account.get("id_token", "")
                if id_token and is_token_expired(id_token, buffer_minutes=3):
                    to_refresh.append(account)

        if not to_refresh:
            return self._calc_next_check_interval()

        active_count = len(to_refresh)
        logger.info(
            "[TokenManager] 发现 %d 个账号即将过期，开始预刷新",
            active_count,
        )

        refreshed = 0
        for account in to_refresh:
            if self.is_firebase_blocked():
                break

            account_id = account["id"]
            refresh_token = account.get("refresh_token", "")
            expires_at = account.get("token_expires_at", 0)
            remaining_min = int((expires_at - time.time()) / 60) if expires_at > 0 else -1

            # Jitter：随机延迟 0~120 秒，防止雷群
            jitter = random.uniform(0, MAX_JITTER_SECONDS)
            logger.info(
                "[TokenManager] 预刷新 account_id=%d email=%s (剩余 %d 分钟, jitter %.0fs)",
                account_id, account.get("email", "?"), remaining_min, jitter,
            )
            await asyncio.sleep(jitter)

            try:
                new_token = await refresh_access_token_with_refresh_token(refresh_token)
                if new_token:
                    self._update_token_in_db(account_id, new_token)
                    self._refresh_failures.pop(account_id, None)
                    refreshed += 1
                    # 刷新成功后顺带同步服务端真实额度
                    await self.sync_account_quota(account_id, new_token)
            except Exception as exc:
                err_msg = str(exc)
                logger.warning(
                    "[TokenManager] 预刷新 account_id=%d 失败: %s",
                    account_id, exc,
                )
                if "429" in err_msg or "rate" in err_msg.lower():
                    self.set_firebase_blocked()
                    break

            # 账号间基础间隔 3 秒（jitter 已在前面加过）
            await asyncio.sleep(3)

        if refreshed > 0:
            logger.info("[TokenManager] 预刷新完成，本轮刷新 %d 个", refreshed)

        # ── 僵尸账号清扫：available 但 token 已过期的，标记为 token_expired ──
        # 这样 _check_exhausted_accounts 会自动接管刷新+恢复
        try:
            with get_connection(str(self.db_path)) as conn:
                stale_count = conn.execute(
                    """UPDATE accounts SET status = 'token_expired', updated_at = ?
                       WHERE status = 'available'
                         AND token_expires_at > 0
                         AND token_expires_at < ?
                         AND refresh_token IS NOT NULL AND refresh_token != ''""",
                    (datetime.now().isoformat(), now),
                ).rowcount
                conn.commit()
                if stale_count > 0:
                    logger.info(
                        "[TokenManager] 标记 %d 个僵尸账号为 token_expired（available 但 token 已过期）",
                        stale_count,
                    )
        except Exception as exc:
            logger.debug("[TokenManager] 僵尸账号清扫异常: %s", exc)

        return self._calc_next_check_interval()

    def _calc_next_check_interval(self) -> int:
        """根据最近即将过期的 token 计算下次检查间隔（秒）。"""
        try:
            with get_connection(str(self.db_path)) as conn:
                row = conn.execute(
                    """
                    SELECT MIN(token_expires_at) as nearest
                    FROM accounts
                    WHERE status = 'available'
                      AND token_expires_at > ?
                      AND refresh_token IS NOT NULL AND refresh_token != ''
                    """,
                    (time.time(),),
                ).fetchone()
                if row and row[0]:
                    # 在过期前 PRE_REFRESH_BUFFER 时刷新
                    seconds_until_refresh = row[0] - PRE_REFRESH_BUFFER_SECONDS - time.time()
                    interval = max(60, int(seconds_until_refresh))
                    logger.debug("[TokenManager] 下次预刷新检查: %d 秒后", interval)
                    return interval
        except Exception:
            pass
        return 300  # 默认 5 分钟

    async def _check_exhausted_accounts(self) -> None:
        """渐进式检查 exhausted / token_expired 账号，如果服务端额度已恢复则自动重新启用。

        策略：
        - exhausted（额度耗尽）：不刷新 token（省 Firebase 调用），用现有 token 查额度，token 也过期则跳过
        - token_expired（token 过期）：刷新 token + 查额度
        - 每次后台循环都会调用，但只查 updated_at 距今 ≥15 分钟的（刚耗尽的不查）
        - 每次最多查 10 个（按 updated_at ASC，最久没检查的优先）
        - 查完更新 updated_at，下次循环自然轮到下一批
        """
        if self.is_firebase_blocked():
            return

        min_age = time.time() - 15 * 60  # 至少 15 分钟前标记/检查的才查（原 1 小时 → 15 分钟）

        with get_connection(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT id, email, refresh_token, id_token, token_expires_at, status,
                          total_limit, used_limit, api_key
                   FROM accounts
                   WHERE status IN ('exhausted', 'token_expired')
                     AND refresh_token IS NOT NULL AND refresh_token != ''
                     AND (updated_at IS NULL OR updated_at < ?)
                   ORDER BY updated_at ASC
                   LIMIT 10""",
                (datetime.fromtimestamp(min_age).isoformat(),),
            ).fetchall()

        if not rows:
            return

        logger.info("[TokenManager] 渐进检查 %d 个 exhausted/token_expired 账号", len(rows))
        recovered = 0

        for row in rows:
            if self.is_firebase_blocked():
                break

            account_id = row["id"]
            account_status = row["status"]
            refresh_token = row["refresh_token"]
            id_token = row["id_token"] or ""
            token_expires_at = row["token_expires_at"] or 0

            try:
                total_limit = row["total_limit"] or 0
                used_limit = row["used_limit"] or 0
                quota_fully_used = total_limit > 0 and used_limit >= total_limit

                # wk-1 API key 账号：直接用 api_key 查额度，无需 Firebase
                row_api_key = (row["api_key"] or "") if "api_key" in row.keys() else ""
                if row_api_key.startswith("wk-"):
                    access_token = row_api_key
                elif quota_fully_used:
                    # 额度确实用完（如 300/300）：不刷新 token，用现有 token 查额度即可
                    # 如果 token 也过期了，跳过（等额度重置周期后再查）
                    token_valid = id_token and (token_expires_at == 0 or token_expires_at > time.time())
                    if not token_valid:
                        logger.debug(
                            "[TokenManager] account_id=%d 额度已满 (%d/%d) 且 token 过期，跳过",
                            account_id, used_limit, total_limit,
                        )
                        self._touch_updated_at(account_id)
                        continue
                    access_token = id_token
                else:
                    # 额度未满或 token_expired：刷新 token + 查额度
                    new_token = await refresh_access_token_with_refresh_token(refresh_token)
                    if not new_token:
                        self._touch_updated_at(account_id)
                        continue
                    self._update_token_in_db(account_id, new_token)
                    access_token = new_token

                # 查询服务端真实额度
                from warp2protobuf.core.quota import get_request_limit_info
                quota = await get_request_limit_info(access_token)
                total = quota.get("request_limit", 0)
                used = quota.get("used", 0)
                remaining = quota.get("remaining", 0)

                now_str = datetime.now().isoformat()
                with get_connection(str(self.db_path)) as conn:
                    conn.execute(
                        """UPDATE accounts SET
                            total_limit = ?, used_limit = ?, last_check = ?, updated_at = ?
                        WHERE id = ?""",
                        (total, used, now_str, now_str, account_id),
                    )
                    if remaining > 0:
                        conn.execute(
                            "UPDATE accounts SET status = 'available', updated_at = ? WHERE id = ?",
                            (now_str, account_id),
                        )
                        recovered += 1
                        logger.info(
                            "[TokenManager] account_id=%d email=%s 额度已恢复 (remaining=%d)，重新启用",
                            account_id, row["email"], remaining,
                        )
                    conn.commit()

            except Exception as exc:
                err_msg = str(exc)
                logger.warning(
                    "[TokenManager] account_id=%d (%s) 恢复检查失败: %s",
                    account_id, account_status, exc,
                )
                # 失败也更新 updated_at，避免卡在同一个账号
                self._touch_updated_at(account_id)
                if "429" in err_msg or "rate" in err_msg.lower():
                    self.set_firebase_blocked()
                    break

            await asyncio.sleep(1)  # 缩短间隔（原 3s → 1s），加速恢复检测

        if recovered > 0:
            logger.info("[TokenManager] exhausted 恢复检查完成，恢复 %d 个账号", recovered)

    def _touch_updated_at(self, account_id: int) -> None:
        """仅更新 updated_at，用于推后下次检查时间。"""
        try:
            with get_connection(str(self.db_path)) as conn:
                conn.execute(
                    "UPDATE accounts SET updated_at = ? WHERE id = ?",
                    (datetime.now().isoformat(), account_id),
                )
                conn.commit()
        except Exception:
            pass


def _extract_exp_from_jwt(token: str) -> float:
    """从 JWT 中提取 exp 时间戳。失败返回 0。"""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return 0.0
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return float(payload.get("exp", 0))
    except Exception:
        return 0.0
