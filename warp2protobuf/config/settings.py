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

# Account database configuration
ACCOUNT_DB_PATH = os.getenv("ACCOUNT_DB_PATH", str(SCRIPT_DIR / "accounts.db"))
ACCOUNT_ADMIN_ENABLED = _env_bool("ACCOUNT_ADMIN_ENABLED", True)
