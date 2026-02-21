#!/usr/bin/env python3
"""从本机 Warp 客户端提取帐号信息，输出为 shell 变量赋值格式。"""
import argparse
import base64
import json
import os
import plistlib
import sys


def _extract(warp_file: str) -> dict:
    if not os.path.exists(warp_file):
        raise FileNotFoundError("warp user file not found")

    with open(warp_file, "rb") as f:
        data = plistlib.load(f)

    refresh_token: str = data.get("refresh_token", "")
    email: str = data.get("email", "")
    local_id: str = data.get("local_id", "")

    # id_token 可能是 dict（含 id_token + expiration_time）或 string
    id_token_raw = data.get("id_token", "")
    id_token_str = ""
    if isinstance(id_token_raw, dict):
        id_token_str = id_token_raw.get("id_token", "")
    elif isinstance(id_token_raw, str):
        id_token_str = id_token_raw

    # 从 JWT 解析 user_id 和 email
    user_id = ""
    if id_token_str:
        try:
            payload = id_token_str.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            user_id = claims.get("sub", "")
            if not email:
                email = claims.get("email", "")
        except Exception:
            pass

    return {
        "refresh_token": refresh_token,
        "account_email": email,
        "user_id": user_id,
        "local_id": local_id,
        "id_token_preview": id_token_str[:50],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Warp account info")
    parser.add_argument("--json", action="store_true", help="Output JSON format")
    parser.add_argument("warp_file", nargs="?", default=os.path.expanduser("~/Library/Application Support/dev.warp.Warp-Stable/dev.warp.Warp-User"))
    args = parser.parse_args()

    try:
        info = _extract(args.warp_file)
    except Exception:
        if args.json:
            print("{}", flush=True)
        else:
            print("ERROR=extract_failed", flush=True)
        sys.exit(1)

    if args.json:
        print(json.dumps(info, ensure_ascii=False), flush=True)
        return

    # backward-compatible shell variable output
    def safe(s: str) -> str:
        return str(s).replace("\\", "\\\\").replace("'", "'\\''")

    print(f"REFRESH_TOKEN='{safe(info.get('refresh_token', ''))}'")
    print(f"ACCOUNT_EMAIL='{safe(info.get('account_email', ''))}'")
    print(f"USER_ID='{safe(info.get('user_id', ''))}'")
    print(f"ID_TOKEN='{safe(info.get('id_token_preview', ''))}'")
    print(f"LOCAL_ID='{safe(info.get('local_id', ''))}'")


if __name__ == "__main__":
    main()
