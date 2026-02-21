#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pytest 全局 fixtures。"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest


@pytest.fixture
def valid_jwt_token() -> str:
    """提供一个结构合法且可解码 payload 的测试 JWT（不用于真实鉴权）。"""
    return (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJleHAiOjQxMDI0NDQ4MDAsInVzZXJfaWQiOiJ1c2VyLTEyMyIsInN1YiI6InN1Yi0xMjMifQ."
        "signature"
    )


@pytest.fixture(autouse=True)
def isolate_warp_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """隔离测试中的关键环境变量，避免污染本机环境。"""
    backup_vars = {
        "WARP_JWT": os.getenv("WARP_JWT"),
        "WARP_REFRESH_TOKEN": os.getenv("WARP_REFRESH_TOKEN"),
        "WARP_INSECURE_TLS": os.getenv("WARP_INSECURE_TLS"),
    }
    monkeypatch.delenv("WARP_JWT", raising=False)
    monkeypatch.delenv("WARP_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("WARP_INSECURE_TLS", raising=False)

    yield

    for key, value in backup_vars.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
