#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量导入账号 JSON 到本地 SQLite 数据库

支持从 JSON 文件导入账号，自动解析 local_id，支持 dry-run 预览。
"""
import argparse
import base64
import json
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.config.settings import SCRIPT_DIR


def parse_local_id_from_id_token(id_token: str) -> str:
    """从 id_token JWT 解析 sub/user_id 作为 local_id"""
    try:
        parts = id_token.split(".")
        if len(parts) != 3:
            return ""
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes.decode("utf-8"))
        return payload.get("sub") or payload.get("user_id") or ""
    except Exception:
        return ""


def validate_account(account: dict) -> tuple[bool, str]:
    """校验账号必填字段"""
    if not account.get("email"):
        return False, "missing email"
    if not account.get("id_token"):
        return False, "missing id_token"
    if not account.get("refresh_token"):
        return False, "missing refresh_token"
    return True, ""


def main():
    parser = argparse.ArgumentParser(description="Import accounts from JSON file")
    parser.add_argument(
        "--json-file",
        default="/Users/chenzhuo/Downloads/300额度_50个.json",
        help="Path to JSON file containing accounts",
    )
    parser.add_argument(
        "--db-path",
        default=str(SCRIPT_DIR / "accounts.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview import without writing to database",
    )
    args = parser.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"❌ JSON file not found: {json_path}", file=sys.stderr)
        sys.exit(1)

    # 读取 JSON
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            accounts = json.load(f)
    except Exception as e:
        print(f"❌ Failed to read JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(accounts, list):
        print("❌ JSON must be an array of accounts", file=sys.stderr)
        sys.exit(1)

    print(f"📦 Loaded {len(accounts)} accounts from {json_path}")

    # 校验与预处理
    valid_accounts = []
    invalid_accounts = []

    for idx, account in enumerate(accounts, start=1):
        is_valid, reason = validate_account(account)
        if not is_valid:
            invalid_accounts.append((idx, reason))
            continue

        # 补充 local_id（如果缺失）
        if not account.get("local_id"):
            local_id = parse_local_id_from_id_token(account["id_token"])
            if local_id:
                account["local_id"] = local_id
            else:
                invalid_accounts.append((idx, "cannot parse local_id from id_token"))
                continue

        valid_accounts.append(account)

    print(f"✅ Valid accounts: {len(valid_accounts)}")
    print(f"❌ Invalid accounts: {len(invalid_accounts)}")

    if invalid_accounts:
        print("\n⚠️  Invalid accounts:")
        for idx, reason in invalid_accounts[:10]:
            print(f"  - Account #{idx}: {reason}")
        if len(invalid_accounts) > 10:
            print(f"  ... and {len(invalid_accounts) - 10} more")

    if not valid_accounts:
        print("\n❌ No valid accounts to import")
        sys.exit(1)

    # Dry-run 预览
    if args.dry_run:
        print("\n🔍 DRY-RUN MODE - Preview first 5 accounts:")
        for account in valid_accounts[:5]:
            print(f"  - {account['email']} | local_id={account['local_id'][:20]}... | total_limit={account.get('total_limit', 0)}")
        print(f"\n✅ Dry-run complete. Use without --dry-run to import.")
        sys.exit(0)

    # 正式导入
    print(f"\n📥 Importing to database: {args.db_path}")
    store = AccountStore(args.db_path)

    imported = 0
    updated = 0
    failed = 0

    for account in valid_accounts:
        try:
            # 检查是否已存在
            import sqlite3
            with sqlite3.connect(args.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM accounts WHERE email = ?", (account["email"],))
                exists = cursor.fetchone() is not None

            store.upsert_account(account)

            if exists:
                updated += 1
            else:
                imported += 1

        except Exception as e:
            print(f"  ❌ Failed to import {account['email']}: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print("📊 IMPORT SUMMARY")
    print("=" * 60)
    print(f"Total accounts in JSON: {len(accounts)}")
    print(f"Valid accounts: {len(valid_accounts)}")
    print(f"Invalid accounts: {len(invalid_accounts)}")
    print(f"Imported (new): {imported}")
    print(f"Updated (existing): {updated}")
    print(f"Failed: {failed}")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
