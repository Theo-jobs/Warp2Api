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

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        # 记录最近刷新失败的账号 ID → 失败时间戳，短期内不再重试
        self._refresh_failures: Dict[int, float] = {}
        self._FAILURE_COOLDOWN = 300  # 刷新失败后 5 分钟冷却

    @classmethod
    def get_instance(cls, db_path: str) -> "TokenManager":
        if cls._instance is None:
            cls._instance = cls(db_path)
        return cls._instance

    async def get_valid_token(self, account: Dict) -> Optional[str]:
        """获取账号的有效 id_token，过期则自动刷新。

        Args:
            account: 从 DB 查出的账号字典，需包含 id, id_token, refresh_token, email

        Returns:
            有效的 id_token，刷新失败返回 None
        """
        account_id = account["id"]
        id_token = account.get("id_token", "")
        refresh_token = account.get("refresh_token", "")

        # 检查是否在冷却期
        fail_ts = self._refresh_failures.get(account_id, 0)
        if fail_ts and (time.time() - fail_ts) < self._FAILURE_COOLDOWN:
            logger.warning(
                "[TokenManager] account_id=%d 在刷新冷却期，跳过",
                account_id,
            )
            return None

        # token 未过期，直接返回
        if id_token and not is_token_expired(id_token, buffer_minutes=5):
            return id_token

        # token 过期或为空，需要刷新
        if not refresh_token:
            logger.error(
                "[TokenManager] account_id=%d 无 refresh_token，无法刷新",
                account_id,
            )
            return None

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
            logger.error(
                "[TokenManager] account_id=%d token 刷新失败: %s",
                account_id,
                exc,
            )
            self._refresh_failures[account_id] = time.time()
            return None

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
        """批量预刷新所有账号的 token（启动时或定时调用）。

        Returns:
            {"total": N, "refreshed": N, "failed": N, "skipped": N}
        """
        stats = {"total": 0, "refreshed": 0, "failed": 0, "skipped": 0}

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, email, id_token, refresh_token FROM accounts WHERE status = 'available'"
            ).fetchall()

        stats["total"] = len(rows)
        logger.info("[TokenManager] 开始批量刷新 %d 个账号...", len(rows))

        for row in rows:
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
                logger.warning(
                    "[TokenManager] 批量刷新 account_id=%d 失败: %s",
                    account_id,
                    exc,
                )
                stats["failed"] += 1

            # 每个账号间隔 0.5s，避免被 Firebase 限流
            await asyncio.sleep(0.5)

        logger.info(
            "[TokenManager] 批量刷新完成: total=%d refreshed=%d failed=%d skipped=%d",
            stats["total"],
            stats["refreshed"],
            stats["failed"],
            stats["skipped"],
        )
        return stats
