#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration settings for Warp API server

Contains environment variables, paths, and constants.
"""
import os
import pathlib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return default


# Path configurations
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
PROTO_DIR = SCRIPT_DIR / "proto"
LOGS_DIR = SCRIPT_DIR / "logs"

# API configuration
# When WARP_RUSTLS_PROXY is set (default), route through the local Rust proxy
# which uses rustls for TLS — matching Warp client's TLS fingerprint to bypass 403.
# Set WARP_URL directly to override, or WARP_RUSTLS_PROXY=0 to disable.
_rustls_proxy_port = os.getenv("RUST_PROXY_PORT", "28887")
_rustls_proxy_enabled = os.getenv("WARP_RUSTLS_PROXY", "1").lower() not in ("0", "false", "no")
_direct_warp_url = "https://app.warp.dev/ai/multi-agent"
_proxy_warp_url = f"http://127.0.0.1:{_rustls_proxy_port}/ai/multi-agent"
WARP_URL = os.getenv("WARP_URL", _proxy_warp_url if _rustls_proxy_enabled else _direct_warp_url)

# Environment variables with defaults
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8002"))
WARP_JWT = os.getenv("WARP_JWT")

# Client headers configuration
CLIENT_ID = os.getenv("WARP_CLIENT_ID", "warp-app")
CLIENT_VERSION = os.getenv("WARP_CLIENT_VERSION", "v0.2026.02.11.08.23.stable_01")
OS_CATEGORY = os.getenv("WARP_OS_CATEGORY", "macOS")
OS_NAME = os.getenv("WARP_OS_NAME", "macOS")
OS_VERSION = os.getenv("WARP_OS_VERSION", "26.3")

# Protobuf field names for text detection
TEXT_FIELD_NAMES = ("text", "prompt", "query", "content", "message", "input")
PATH_HINT_BONUS = ("conversation", "query", "input", "user", "request", "delta")

# Response parsing configuration
SYSTEM_STR = {"agent_output.text", "server_message_data", "USER_INITIATED", "agent_output", "text"}

# JWT refresh configuration
REFRESH_TOKEN_B64 = "Z3JhbnRfdHlwZT1yZWZyZXNoX3Rva2VuJnJlZnJlc2hfdG9rZW49QU1mLXZCeFNSbWRodmVHR0JZTTY5cDA1a0RoSW4xaTd3c2NBTEVtQzlmWURScEh6akVSOWRMN2trLWtIUFl3dlk5Uk9rbXk1MHFHVGNJaUpaNEFtODZoUFhrcFZQTDkwSEptQWY1Zlo3UGVqeXBkYmNLNHdzbzhLZjNheGlTV3RJUk9oT2NuOU56R2FTdmw3V3FSTU5PcEhHZ0JyWW40SThrclc1N1I4X3dzOHU3WGNTdzh1MERpTDlIcnBNbTBMdHdzQ2g4MWtfNmJiMkNXT0ViMWxJeDNIV1NCVGVQRldzUQ=="
REFRESH_URL = "https://app.warp.dev/proxy/token?key=AIzaSyBdy3O3S9hrdayLJxJ7mriBR4qgUaUygAs"

# Account pool integration flags
ACCOUNT_POOL_ENABLED = _env_bool("ACCOUNT_POOL_ENABLED", False)
ACCOUNT_POOL_BASE_URL = os.getenv("ACCOUNT_POOL_BASE_URL", "http://account-pool-service:38019")
ACCOUNT_POOL_ALLOCATE_TIMEOUT = float(os.getenv("ACCOUNT_POOL_ALLOCATE_TIMEOUT", "5"))
ACCOUNT_POOL_RELEASE_TIMEOUT = float(os.getenv("ACCOUNT_POOL_RELEASE_TIMEOUT", "5"))
ACCOUNT_POOL_SWITCH_MAX_RETRIES = int(os.getenv("ACCOUNT_POOL_SWITCH_MAX_RETRIES", "2"))
ACCOUNT_POOL_FALLBACK_TO_ENV = _env_bool("ACCOUNT_POOL_FALLBACK_TO_ENV", True)

# HTTP proxy for external requests (registration, token refresh, etc.)
# Set to "" or "none" to disable. Example: "http://127.0.0.1:7890"
_raw_proxy = os.getenv("WARP_PROXY_URL", "")
PROXY_URL: str | None = _raw_proxy if _raw_proxy and _raw_proxy.lower() != "none" else None

# Domains that must bypass HTTP proxy (MITM proxies like Stash can't handle their TLS)
# Comma-separated, e.g. "warp.dev,googleapis.com"
# Set to empty string if Stash has been configured with DIRECT rules for these domains
_raw_no_proxy = os.getenv("WARP_NO_PROXY_DOMAINS", "")
NO_PROXY_DOMAINS: tuple[str, ...] = tuple(
    d.strip() for d in _raw_no_proxy.split(",") if d.strip()
)


def proxy_for_url(url: str) -> str | None:
    """Return PROXY_URL unless the URL's domain is in the no-proxy list or is local."""
    if not PROXY_URL:
        return None
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        # 本地地址不走代理
        if host in ("localhost", "127.0.0.1", "::1") or host.startswith("192.168.") or host.startswith("10."):
            return None
        for domain in NO_PROXY_DOMAINS:
            if host == domain or host.endswith(f".{domain}"):
                return None
    except Exception:
        pass
    return PROXY_URL


# TLS verification — disable when using MITM proxies (Stash/Surge/mitmproxy)
# Set WARP_INSECURE_TLS=1 to skip certificate verification
TLS_VERIFY: bool = not _env_bool("WARP_INSECURE_TLS", False)

# Account database configuration
ACCOUNT_DB_PATH = os.getenv("ACCOUNT_DB_PATH", str(SCRIPT_DIR / "accounts.db"))
ACCOUNT_ADMIN_ENABLED = _env_bool("ACCOUNT_ADMIN_ENABLED", True)
ACCOUNT_REGISTER_ENABLED = _env_bool("ACCOUNT_REGISTER_ENABLED", False)

# History serialization truncation length for tool results
# Used by protobuf2openai routers when converting conversation history into system text.
HISTORY_TOOL_RESULT_MAX_CHARS = min(
    100_000,
    max(1, _env_int("HISTORY_TOOL_RESULT_MAX_CHARS", 100_000)),
)
