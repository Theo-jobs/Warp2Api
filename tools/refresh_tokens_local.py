#!/usr/bin/env python3
"""
本地批量刷新账号 Token 脚本

当极空间 IP 被 Firebase 限流时，在本地（未被限流的 IP）运行此脚本，
刷新所有账号的 id_token，然后将 accounts.db 同步到极空间。

用法:
    python tools/refresh_tokens_local.py [--db-path accounts.db] [--interval 3] [--dry-run]

刷新后同步到极空间:
    scp accounts.db user@极空间IP:/path/to/Warp2Api/accounts.db
    # 然后重启容器: docker compose restart warp2api
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
import time
from pathlib import Path

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from warp2protobuf.core.auth import (
    is_token_expired,
    refresh_access_token_with_refresh_token,
    _mask_token,
    decode_jwt_payload,
)


async def refresh_all_tokens(
    db_path: str,
    interval: float = 3.0,
    dry_run: bool = False,
) -> dict:
    """批量刷新所有账号的 id_token。"""
    stats = {"total": 0, "refreshed": 0, "failed": 0, "skipped": 0, "errors": []}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, email, id_token, refresh_token FROM accounts WHERE status = 'available'"
    ).fetchall()

    stats["total"] = len(rows)
    print(f"\n📋 共 {len(rows)} 个可用账号\n")

    for i, row in enumerate(rows, 1):
        account = dict(row)
        account_id = account["id"]
        email = account.get("email", "?")
        id_token = account.get("id_token", "")
        refresh_token = account.get("refresh_token", "")

        # 检查是否需要刷新
        if id_token and not is_token_expired(id_token, buffer_minutes=10):
            print(f"  [{i}/{len(rows)}] ⏭  {email} — token 未过期，跳过")
            stats["skipped"] += 1
            continue

        if not refresh_token:
            print(f"  [{i}/{len(rows)}] ❌ {email} — 无 refresh_token")
            stats["failed"] += 1
            stats["errors"].append(f"{email}: no refresh_token")
            continue

        if dry_run:
            expired_str = "已过期" if (not id_token or is_token_expired(id_token, buffer_minutes=0)) else "即将过期"
            print(f"  [{i}/{len(rows)}] 🔍 {email} — {expired_str}，需要刷新 (dry-run)")
            stats["refreshed"] += 1
            continue

        # 执行刷新
        try:
            print(f"  [{i}/{len(rows)}] 🔄 {email} — 刷新中...", end="", flush=True)
            new_token = await refresh_access_token_with_refresh_token(refresh_token)
            if not new_token:
                raise RuntimeError("返回空 token")

            # 回写 DB（含 token_expires_at）
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            payload = decode_jwt_payload(new_token)
            expires_at = float(payload.get("exp", 0))
            conn.execute(
                "UPDATE accounts SET id_token = ?, token_expires_at = ?, last_refresh_time = ?, updated_at = ? WHERE id = ?",
                (new_token, expires_at, now, now, account_id),
            )
            conn.commit()

            print(f" ✅ 成功 (token: {_mask_token(new_token)})")
            stats["refreshed"] += 1

        except Exception as exc:
            err_msg = str(exc)
            print(f" ❌ 失败: {err_msg[:80]}")
            stats["failed"] += 1
            stats["errors"].append(f"{email}: {err_msg[:100]}")

            if "429" in err_msg:
                print(f"\n  ⚠️  遇到 429 限流！已刷新 {stats['refreshed']} 个。")
                print(f"  建议等待几分钟后重新运行。\n")
                break

        # 间隔
        if i < len(rows):
            await asyncio.sleep(interval)

    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="本地批量刷新账号 Token")
    parser.add_argument(
        "--db-path",
        default=str(PROJECT_ROOT / "accounts.db"),
        help="accounts.db 路径 (默认: 项目根目录/accounts.db)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="每个账号刷新间隔秒数 (默认: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅检查，不实际刷新",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        sys.exit(1)

    print(f"🗄  数据库: {db_path}")
    print(f"⏱  间隔: {args.interval}s")
    if args.dry_run:
        print("🔍 DRY-RUN 模式\n")

    stats = asyncio.run(refresh_all_tokens(str(db_path), args.interval, args.dry_run))

    print(f"\n{'='*50}")
    print(f"📊 刷新结果:")
    print(f"   总计: {stats['total']}")
    print(f"   成功: {stats['refreshed']}")
    print(f"   失败: {stats['failed']}")
    print(f"   跳过: {stats['skipped']}")

    if stats["errors"]:
        print(f"\n❌ 失败详情:")
        for err in stats["errors"][:10]:
            print(f"   - {err}")

    if stats["refreshed"] > 0 and not args.dry_run:
        print(f"\n✅ 刷新完成！")
        print(f"📦 同步到极空间:")
        print(f"   scp {db_path} <极空间用户>@<极空间IP>:<Warp2Api路径>/accounts.db")
        print(f"   然后重启: docker compose restart warp2api")


if __name__ == "__main__":
    main()
