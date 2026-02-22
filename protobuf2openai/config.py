from __future__ import annotations

import os

BRIDGE_BASE_URL = os.getenv("WARP_BRIDGE_URL", "http://127.0.0.1:28888")
FALLBACK_BRIDGE_URLS = [
    BRIDGE_BASE_URL,
    "http://127.0.0.1:28888",
]

WARMUP_INIT_RETRIES = int(os.getenv("WARP_COMPAT_INIT_RETRIES", "10"))
WARMUP_INIT_DELAY_S = float(os.getenv("WARP_COMPAT_INIT_DELAY", "0.5"))
WARMUP_REQUEST_RETRIES = int(os.getenv("WARP_COMPAT_WARMUP_RETRIES", "1"))
WARMUP_REQUEST_DELAY_S = float(os.getenv("WARP_COMPAT_WARMUP_DELAY", "1.5")) 