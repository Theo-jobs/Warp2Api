#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AccountSelector 多策略选择单元测试

覆盖场景：
- 各策略（least_used / round_robin / random / most_quota / priority）
- Token 过期过滤
- token_expires_at=0 放行
- exclude_ids 排除
- 无可用账号返回 None
- 全部 token 过期返回 None
"""
import sqlite3
import time
from typing import Any, Dict, List

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_test_db(tmp_path, accounts: List[Dict[str, Any]]) -> str:
    """创建测试用 SQLite 数据库并插入账号数据。"""
    db_path = str(tmp_path / "test_accounts.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            local_id TEXT NOT NULL DEFAULT '',
            id_token TEXT NOT NULL DEFAULT '',
            refresh_token TEXT NOT NULL DEFAULT '',
            api_key TEXT DEFAULT '',
            status TEXT DEFAULT 'available',
            total_limit INTEGER DEFAULT 0,
            used_limit INTEGER DEFAULT 0,
            last_check TIMESTAMP,
            use_count INTEGER DEFAULT 0,
            last_used TIMESTAMP,
            last_refresh_time TIMESTAMP,
            session_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            token_expires_at REAL DEFAULT 0,
            priority INTEGER DEFAULT 0
        )
    """)
    for acc in accounts:
        conn.execute(
            """INSERT INTO accounts
               (email, local_id, id_token, refresh_token, status,
                total_limit, used_limit, use_count, token_expires_at, priority)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                acc.get("email", ""),
                acc.get("local_id", ""),
                acc.get("id_token", "tok"),
                acc.get("refresh_token", "ref"),
                acc.get("status", "available"),
                acc.get("total_limit", 100),
                acc.get("used_limit", 0),
                acc.get("use_count", 0),
                acc.get("token_expires_at", 0),
                acc.get("priority", 0),
            ),
        )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLeastUsedStrategy:
    """least_used 策略：选择 use_count 最小的账号。"""

    def test_selects_least_used_account(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "use_count": 10, "total_limit": 100},
            {"email": "b@test.com", "use_count": 2, "total_limit": 100},
            {"email": "c@test.com", "use_count": 5, "total_limit": 100},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is not None
        assert result["email"] == "b@test.com"


class TestRoundRobinStrategy:
    """round_robin 策略：按 ID 顺序轮转。"""

    def test_cycles_through_accounts(self, tmp_path) -> None:
        import warp2protobuf.core.account_selector as mod
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100},
            {"email": "b@test.com", "total_limit": 100},
            {"email": "c@test.com", "total_limit": 100},
        ])
        # 重置全局游标
        mod._round_robin_cursor = 0
        selector = AccountSelector(db)

        first = selector.select_account(strategy="round_robin")
        assert first is not None
        assert first["email"] == "a@test.com"

        second = selector.select_account(strategy="round_robin")
        assert second is not None
        assert second["email"] == "b@test.com"

        third = selector.select_account(strategy="round_robin")
        assert third is not None
        assert third["email"] == "c@test.com"

        # 回到第一个（wrap around）
        fourth = selector.select_account(strategy="round_robin")
        assert fourth is not None
        assert fourth["email"] == "a@test.com"


class TestRandomStrategy:
    """random 策略：从候选中随机选择。"""

    def test_returns_valid_account(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100},
            {"email": "b@test.com", "total_limit": 100},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="random")

        assert result is not None
        assert result["email"] in ("a@test.com", "b@test.com")


class TestMostQuotaStrategy:
    """most_quota 策略：选择剩余额度最多的账号。"""

    def test_selects_highest_remaining_quota(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100, "used_limit": 90},
            {"email": "b@test.com", "total_limit": 100, "used_limit": 10},
            {"email": "c@test.com", "total_limit": 100, "used_limit": 50},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="most_quota")

        assert result is not None
        assert result["email"] == "b@test.com"


class TestPriorityStrategy:
    """priority 策略：按 priority DESC 排序。"""

    def test_selects_highest_priority(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100, "priority": 1},
            {"email": "b@test.com", "total_limit": 100, "priority": 10},
            {"email": "c@test.com", "total_limit": 100, "priority": 5},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="priority")

        assert result is not None
        assert result["email"] == "b@test.com"


class TestTokenExpiryFiltering:
    """Token 过期过滤：过期或 5 分钟内过期的账号应被排除。"""

    def test_excludes_expired_token(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        past = time.time() - 600  # 10 分钟前已过期
        db = _create_test_db(tmp_path, [
            {"email": "expired@test.com", "total_limit": 100, "token_expires_at": past},
            {"email": "valid@test.com", "total_limit": 100, "token_expires_at": 0},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is not None
        assert result["email"] == "valid@test.com"

    def test_excludes_token_expiring_within_buffer(self, tmp_path) -> None:
        """token_expires_at 在当前时间 + 5 分钟以内的也应被排除。"""
        from warp2protobuf.core.account_selector import AccountSelector

        almost_expired = time.time() + 60  # 仅剩 1 分钟，在 5 分钟缓冲内
        db = _create_test_db(tmp_path, [
            {"email": "soon@test.com", "total_limit": 100, "token_expires_at": almost_expired},
            {"email": "safe@test.com", "total_limit": 100, "token_expires_at": time.time() + 3600},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is not None
        assert result["email"] == "safe@test.com"

    def test_allows_zero_token_expires_at(self, tmp_path) -> None:
        """token_expires_at=0 表示未记录，应放行。"""
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "unrecorded@test.com", "total_limit": 100, "token_expires_at": 0},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is not None
        assert result["email"] == "unrecorded@test.com"

    def test_all_tokens_expired_returns_none(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        past = time.time() - 600
        db = _create_test_db(tmp_path, [
            {"email": "exp1@test.com", "total_limit": 100, "token_expires_at": past},
            {"email": "exp2@test.com", "total_limit": 100, "token_expires_at": past},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is None


class TestExcludeIds:
    """exclude_ids 参数：排除指定 ID 的账号。"""

    def test_excludes_specified_ids(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100},
            {"email": "b@test.com", "total_limit": 100},
            {"email": "c@test.com", "total_limit": 100},
        ])
        selector = AccountSelector(db)
        # 排除 id=1 和 id=2
        result = selector.select_account(exclude_ids={1, 2}, strategy="least_used")

        assert result is not None
        assert result["email"] == "c@test.com"

    def test_exclude_all_returns_none(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(exclude_ids={1}, strategy="least_used")

        assert result is None


class TestNoAccountsAvailable:
    """无可用账号场景。"""

    def test_empty_db_returns_none(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is None

    def test_all_disabled_returns_none(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "dis@test.com", "total_limit": 100, "status": "disabled"},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is None

    def test_all_quota_exhausted_returns_none(self, tmp_path) -> None:
        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "full@test.com", "total_limit": 100, "used_limit": 100},
        ])
        selector = AccountSelector(db)
        result = selector.select_account(strategy="least_used")

        assert result is None


class TestDefaultStrategyFallback:
    """monkeypatch 覆盖 ACCOUNT_SELECT_STRATEGY 测试默认策略回退。"""

    def test_uses_config_strategy(self, tmp_path, monkeypatch) -> None:
        """通过 monkeypatch 设置 settings 中的策略。"""
        import warp2protobuf.config.settings as settings_mod
        monkeypatch.setattr(settings_mod, "ACCOUNT_SELECT_STRATEGY", "most_quota")

        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100, "used_limit": 90},
            {"email": "b@test.com", "total_limit": 100, "used_limit": 10},
        ])
        selector = AccountSelector(db)
        # 不传 strategy，应使用 settings 中的 most_quota
        result = selector.select_account()

        assert result is not None
        assert result["email"] == "b@test.com"

    def test_invalid_strategy_falls_back_to_least_used(self, tmp_path, monkeypatch) -> None:
        import warp2protobuf.config.settings as settings_mod
        monkeypatch.setattr(settings_mod, "ACCOUNT_SELECT_STRATEGY", "nonexistent_strategy")

        from warp2protobuf.core.account_selector import AccountSelector

        db = _create_test_db(tmp_path, [
            {"email": "a@test.com", "total_limit": 100, "use_count": 5},
            {"email": "b@test.com", "total_limit": 100, "use_count": 1},
        ])
        selector = AccountSelector(db)
        result = selector.select_account()

        assert result is not None
        # 回退到 least_used，应选 use_count 最小的
        assert result["email"] == "b@test.com"
