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
import sqlite3
import time
from datetime import datetime
from typing import Dict, Optional

from warp2protobuf.core.auth import (
    is_token_expired,
    refresh_access_token_with_refresh_token,
)
from warp2protobuf.core.logging import logger

# Token 有效期常量（Firebase id_token = 60 分钟）
TOKEN_LIFETIME_SECONDS = 3600
# 提前多少秒开始预刷新（提前 10 分钟）
PRE_REFRESH_BUFFER_SECONDS = 600


class TokenManager:
    """单例 Token 管理器，管理所有账号的 token 生命周期。"""

    _instance: Optional["TokenManager"] = None
    _lock: asyncio.Lock  # 全局锁，保护 os.environ["WARP_JWT"]

    # 全局 Firebase 限流标记（类级别，所有实例共享）
    _firebase_blocked_until: float = 0.0
    _FIREBASE_COOLDOWN = 1800  # Firebase 限流后全局冷却 30 分钟

    # 后台预刷新任务引用
    _refresh_task: Optional[asyncio.Task] = None

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._refresh_failures: Dict[int, float] = {}
        self._FAILURE_COOLDOWN = 60
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """确保 accounts 表有 token_expires_at 列。"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
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

    async def get_valid_token(self, account: Dict) -> Optional[str]:
        """获取账号的有效 id_token，过期则自动刷新。

        - Firebase 全局限流期间：仅返回未过期的 token，过期则返回 None（让调用方换号）
        - 单账号冷却期间：仅返回未过期的 token，过期则返回 None
        - 正常情况：过期则刷新

        Args:
            account: 从 DB 查出的账号字典，需包含 id, id_token, refresh_token, email

        Returns:
            有效的 id_token；无法获取有效 token 时返回 None
        """
        account_id = account["id"]
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
            new_token = await refresh_access_token_with_refresh_token(refresh_token)
            if not new_token:
                raise RuntimeError("refresh 返回空 token")

            # 回写 DB
            self._update_token_in_db(account_id, new_token)

            # 清除失败记录
            self._refresh_failures.pop(account_id, None)

            logger.info(
                "[TokenManager] account_id=%d token 刷新成功",
                account_id,
            )
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
        with sqlite3.connect(str(self.db_path)) as conn:
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

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, email, id_token, refresh_token FROM accounts WHERE status = 'available'"
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
        logger.info(
            "[TokenManager] ✅ 后台预刷新已启动（每 5 分钟检查，到期前 %d 分钟刷新）",
            PRE_REFRESH_BUFFER_SECONDS // 60,
        )

    async def _background_refresh_loop(self) -> None:
        """后台循环：每 5 分钟扫描即将过期的 token，逐个刷新。"""
        check_interval = 300  # 5 分钟检查一次
        while True:
            await asyncio.sleep(check_interval)
            try:
                await self._refresh_expiring_tokens()
            except Exception as e:
                logger.error("[TokenManager] 后台预刷新异常: %s", e)

    async def _refresh_expiring_tokens(self) -> None:
        """扫描即将过期的 token（10 分钟内到期），逐个刷新。"""
        if self.is_firebase_blocked():
            logger.debug("[TokenManager] Firebase 限流中，跳过预刷新")
            return

        threshold = time.time() + PRE_REFRESH_BUFFER_SECONDS
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            # 查找 token_expires_at 在阈值内 或 为 0（未记录）且 token 存在的账号
            rows = conn.execute(
                """
                SELECT id, email, id_token, refresh_token, token_expires_at
                FROM accounts
                WHERE status = 'available'
                  AND refresh_token IS NOT NULL
                  AND refresh_token != ''
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
            return

        # 过滤：token_expires_at=0 的需要额外检查是否真的过期
        to_refresh = []
        for row in rows:
            account = dict(row)
            if account["token_expires_at"] > 0:
                # 有记录的过期时间，且在阈值内
                to_refresh.append(account)
            else:
                # 没记录过期时间，用 JWT 解码检查
                id_token = account.get("id_token", "")
                if id_token and is_token_expired(id_token, buffer_minutes=10):
                    to_refresh.append(account)

        if not to_refresh:
            return

        refreshed = 0
        for account in to_refresh:
            if self.is_firebase_blocked():
                break

            account_id = account["id"]
            refresh_token = account.get("refresh_token", "")
            expires_at = account.get("token_expires_at", 0)
            remaining_min = int((expires_at - time.time()) / 60) if expires_at > 0 else -1

            logger.info(
                "[TokenManager] 预刷新 account_id=%d email=%s (剩余 %d 分钟)",
                account_id, account.get("email", "?"), remaining_min,
            )

            try:
                new_token = await refresh_access_token_with_refresh_token(refresh_token)
                if new_token:
                    self._update_token_in_db(account_id, new_token)
                    self._refresh_failures.pop(account_id, None)
                    refreshed += 1
            except Exception as exc:
                err_msg = str(exc)
                logger.warning(
                    "[TokenManager] 预刷新 account_id=%d 失败: %s",
                    account_id, exc,
                )
                if "429" in err_msg or "rate" in err_msg.lower():
                    self.set_firebase_blocked()
                    break

            # 每个间隔 5 秒
            await asyncio.sleep(5)

        if refreshed > 0:
            logger.info("[TokenManager] 预刷新完成，本轮刷新 %d 个", refreshed)


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
