#!/usr/bin/env python3
"""从本机 Warp 客户端提取帐号信息，输出为 shell 变量赋值格式。"""
import plistlib
import base64
import json
import sys
import os


def main():
    warp_file = os.path.expanduser(
        "~/Library/Application Support/dev.warp.Warp-Stable/dev.warp.Warp-User"
    )
    if len(sys.argv) > 1:
        warp_file = sys.argv[1]

    if not os.path.exists(warp_file):
        print("ERROR=file_not_found", flush=True)
        sys.exit(1)

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

    # 输出 shell 变量（用 heredoc 安全格式）
    def safe(s: str) -> str:
        return str(s).replace("\\", "\\\\").replace("'", "'\\''")

    print(f"REFRESH_TOKEN='{safe(refresh_token)}'")
    print(f"ACCOUNT_EMAIL='{safe(email)}'")
    print(f"USER_ID='{safe(user_id)}'")
    print(f"ID_TOKEN='{safe(id_token_str[:50])}'")  # 截断，仅用于显示
    print(f"LOCAL_ID='{safe(local_id)}'")


if __name__ == "__main__":
    main()
