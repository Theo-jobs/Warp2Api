"""
账号级 Token 生命周期管理器

解决问题：
1. Firebase id_token 1小时过期 → 自动用 refresh_token 刷新
2. os.environ["WARP_JWT"] 并发竞态 → asyncio.Lock 隔离
3. 429 后盲目重试 → 自动换号
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime
from typing import Dict, Optional

from warp2protobuf.core.auth import (
    is_token_expired,
    refresh_access_token_with_refresh_token,
)
from warp2protobuf.core.logging import logger


class TokenManager:
    """单例 Token 管理器，管理所有账号的 token 生命周期。"""

    _instance: Optional["TokenManager"] = None
    _lock: asyncio.Lock  # 全局锁，保护 os.environ["WARP_JWT"]

    # 全局 Firebase 限流标记（类级别，所有实例共享）
    _firebase_blocked_until: float = 0.0
    _FIREBASE_COOLDOWN = 1800  # Firebase 限流后全局冷却 30 分钟

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        # 记录最近刷新失败的账号 ID → 失败时间戳，短期内不再重试
        self._refresh_failures: Dict[int, float] = {}
        self._FAILURE_COOLDOWN = 60  # 单账号刷新失败冷却缩短到 60s

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

        - Firebase 全局限流期间：直接返回已有 token（即使过期），不尝试刷新
        - 单账号冷却期间：跳过该账号
        - 正常情况：过期则刷新

        Args:
            account: 从 DB 查出的账号字典，需包含 id, id_token, refresh_token, email

        Returns:
            有效的 id_token；刷新失败时返回已有 token（可能过期）或 None
        """
        account_id = account["id"]
        id_token = account.get("id_token", "")
        refresh_token = account.get("refresh_token", "")

        # token 未过期，直接返回
        if id_token and not is_token_expired(id_token, buffer_minutes=5):
            return id_token

        # === Firebase 全局限流期间：返回已有 token，不刷新 ===
        if self.is_firebase_blocked():
            if id_token:
                logger.debug(
                    "[TokenManager] Firebase 限流中，account_id=%d 返回已有 token（可能过期）",
                    account_id,
                )
                return id_token
            return None

        # 检查单账号冷却期
        fail_ts = self._refresh_failures.get(account_id, 0)
        if fail_ts and (time.time() - fail_ts) < self._FAILURE_COOLDOWN:
            # 冷却期内也返回已有 token
            if id_token:
                return id_token
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

            # 返回已有 token（过期也比 None 好，让 bridge 层尝试处理）
            return id_token or None

    def _update_token_in_db(self, account_id: int, new_token: str) -> None:
        """将刷新后的 token 回写数据库。"""
        now = datetime.now().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                UPDATE accounts SET
                    id_token = ?,
                    last_refresh_time = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (new_token, now, now, account_id),
            )
            conn.commit()

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
