# app/langgraph/utils/cleaner_utils.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Literal, Optional, TypedDict
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────
# 환경 변수 (테스트에서 쉽게 토글)
# ─────────────────────────────────────────────────────────
ENV_ENABLE = os.getenv("PERSIST_ENABLE_CLEANER", "true").lower() == "true"
ENV_MODE: Literal["full", "mask-only", "off"] = os.getenv("PERSIST_CLEANER_MODE", "full").lower()  # full|mask-only|off
ENV_NO_STORE_POLICY: Literal["drop", "redact"] = os.getenv("PERSIST_NO_STORE_POLICY", "redact").lower()  # drop|redact
ENV_MAX_BYTES = int(os.getenv("PERSIST_CLEANER_MAX_BYTES", "4096"))  # 메시지 본문 저장 상한(바이트)

# ─────────────────────────────────────────────────────────
# 타입
# ─────────────────────────────────────────────────────────
class Message(TypedDict, total=False):
    role: Literal["user", "assistant", "tool"]
    content: str
    created_at: str
    meta: Dict[str, Any]

# ─────────────────────────────────────────────────────────
# PII 마스킹(라이트 규칙)
#  - 전화번호, 주민등록번호, 이메일, 간단한 계좌번호 패턴
# ─────────────────────────────────────────────────────────
_PATTERNS = [
    # 휴대전화/전화
    (re.compile(r"\b(01[016789]|02|0[3-9]\d)-?\d{3,4}-?\d{4}\b"), "[전화번호]"),
    # 주민등록번호(간이)
    (re.compile(r"\b\d{6}-?[1-4]\d{6}\b"), "[주민등록번호]"),
    # 이메일
    (re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b"), "[이메일]"),
    # 계좌(아주 간단한 가정: 10~14자리 숫자, 하이픈 포함 가능)
    (re.compile(r"\b\d{2,6}-?\d{2,6}-?\d{2,6}\b"), "[계좌]"),
]

def mask_pii(text: str) -> str:
    if not text:
        return text
    out = text
    for pat, repl in _PATTERNS:
        out = pat.sub(repl, out)
    return out

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _truncate_bytes(s: str, max_bytes: int) -> str:
    if max_bytes <= 0 or not s:
        return s
    b = s.encode("utf-8")
    if len(b) <= max_bytes:
        return s
    # 잘리는 경우 안전하게 디코드되도록 한 글자씩 줄임
    b = b[:max_bytes]
    while True:
        try:
            return b.decode("utf-8", errors="strict") + " …[TRUNCATED]"
        except UnicodeDecodeError:
            b = b[:-1]

def _maybe_redact_content(content: str, mode: Literal["full", "mask-only", "off"], max_bytes: int) -> str:
    if mode == "off":
        return _truncate_bytes(content, max_bytes)
    if mode == "mask-only":
        return _truncate_bytes(mask_pii(content), max_bytes)
    # full: 마스킹 + 소프트 클린(앞뒤 공백 정리)
    cleaned = mask_pii(content.strip())
    return _truncate_bytes(cleaned, max_bytes)

def clean_messages(
    messages: List[Message],
    enable: Optional[bool] = None,
    mode: Literal["full", "mask-only", "off"] = ENV_MODE,
    no_store_policy: Literal["drop", "redact"] = ENV_NO_STORE_POLICY,
    max_bytes: int = ENV_MAX_BYTES,
) -> List[Message]:
    """
    테스트에서 토글 가능:
      - enable=False → 원본 그대로(단, created_at 누락 시 추가만 수행)
      - mode="off"   → 마스킹/클린 비활성(본문 상한만 적용)
      - no_store_policy="drop"|"redact"
    """
    if enable is None:
        enable = ENV_ENABLE

    out: List[Message] = []
    for m in messages or []:
        role = m.get("role") or "user"
        content = m.get("content") or ""
        meta = dict(m.get("meta") or {})
        created_at = m.get("created_at") or _now_iso()

        # no_store 처리
        if meta.get("no_store") is True:
            if no_store_policy == "drop":
                # 저장 자체 생략 (메타만 남기고 싶으면 아래로 변경)
                continue
            elif no_store_policy == "redact":
                content = "[REDACTED(no_store)]"

        if enable:
            content = _maybe_redact_content(content, mode=mode, max_bytes=max_bytes)
        else:
            # cleaner 비적용이어도 크래시 방지 위해 created_at 보강, 길이 상한 적용만
            content = _truncate_bytes(content, max_bytes)

        out.append({
            "role": role,
            "content": content,
            "created_at": created_at,
            "meta": meta,
        })
    return out
