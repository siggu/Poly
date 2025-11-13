# app/langgraph/nodes/session_orchestrator.py
# -*- coding: utf-8 -*-
"""
session_orchestrator.py
- 세션 수명주기 관리 노드
- 기능:
  * 세션 초기화: session_id 확인, started_at/turn_count 준비
  * 매 턴 갱신: last_activity_at 갱신, turn_count 증가
  * 종료 판단:
      - 사용자가 명시적으로 end_session=True 요청한 경우 → 즉시 종료
      - 그 외: idle_timeout / max_turns / max_duration 기준으로 자동 종료
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict, Literal

from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────
# 환경 변수 (기본값 포함)
# ─────────────────────────────────────────────────────────
IDLE_TIMEOUT_SEC = int(os.getenv("SESSION_IDLE_TIMEOUT_SEC", "900"))          # 기본 15분
MAX_TURNS = int(os.getenv("SESSION_MAX_TURNS", "128"))                        # 기본 128턴
MAX_DURATION_SEC = int(os.getenv("SESSION_MAX_DURATION_SEC", str(2 * 3600)))  # 기본 2시간


class Message(TypedDict, total=False):
    role: Literal["user", "assistant", "tool"]
    content: str
    created_at: str
    meta: Dict[str, Any]


class SessionOrchestratorOutput(TypedDict, total=False):
    # 제어
    session_id: str
    end_session: bool

    # 세션 타이밍/지표
    started_at: str
    last_activity_at: str
    turn_count: int

    # 로깅
    messages: List[Message]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None


def _append_tool_log(msgs: List[Message], text: str, meta: Optional[Dict[str, Any]] = None) -> None:
    msgs.append({
        "role": "tool",
        "content": text,
        "created_at": _now_iso(),
        "meta": meta or {},
    })


def orchestrate(state: Dict[str, Any]) -> SessionOrchestratorOutput:
    """
    입력(state)에서 다음 키를 참조(있으면 사용, 없으면 초기화):
      - session_id: str
      - started_at: ISO str
      - last_activity_at: ISO str
      - turn_count: int
      - messages: List[Message]
      - end_session: bool (사용자 요청 플래그; True면 즉시 종료)

    출력:
      - session_id, started_at, last_activity_at, turn_count
      - end_session: bool (종료 판단 결과)
      - messages: tool 로그 1줄 이상 append
    """
    out: SessionOrchestratorOutput = {}
    msgs: List[Message] = list(state.get("messages") or [])

    # 0) 사용자 요청 end_session 플래그 확인
    user_requested_end = bool(state.get("end_session") is True)

    # 1) 세션 ID 확인
    sid = (state.get("session_id") or "").strip()
    if not sid:
        sid = f"sess-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S-%f')}"
        _append_tool_log(msgs, f"[session_orchestrator] session_id generated: {sid}")

    out["session_id"] = sid

    # 2) 시작/활동 타임스탬프 & 턴 카운트
    started_at_iso = state.get("started_at")
    last_activity_iso = state.get("last_activity_at")
    turn_count = int(state.get("turn_count") or 0)

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # 초기화: started_at
    started_dt = _parse_iso(started_at_iso)
    if started_dt is None:
        started_dt = now
        started_at_iso = now_iso
        _append_tool_log(msgs, "[session_orchestrator] started_at initialized")

    # 초기화: last_activity
    last_activity_dt = _parse_iso(last_activity_iso)
    if last_activity_dt is None:
        last_activity_dt = now

    # 3) 매 턴 처리: turn_count 증가 + last_activity 갱신
    turn_count += 1
    last_activity_dt = now
    last_activity_iso = now_iso

    # 4) 종료 판단
    duration = (now - started_dt).total_seconds()
    end_reasons: List[str] = []

    # 4-1) 사용자 요청이 있으면 최우선으로 종료
    if user_requested_end:
        end_reasons.append("user_requested_end_session=True")
    else:
        # 4-2) 자동 종료 규칙
        if turn_count >= MAX_TURNS:
            end_reasons.append(f"max_turns({MAX_TURNS}) reached")
        if duration >= MAX_DURATION_SEC:
            end_reasons.append(f"max_duration({MAX_DURATION_SEC}s) reached")
        # idle_timeout은 외부에서 세션을 깨우는 구조에 따라 추가 구현 가능

    end_session = bool(end_reasons)

    if end_session:
        _append_tool_log(
            msgs,
            "[session_orchestrator] end_session=True",
            {"reasons": end_reasons, "turn_count": turn_count, "duration_sec": int(duration)},
        )
    else:
        _append_tool_log(
            msgs,
            "[session_orchestrator] tick",
            {
                "turn_count": turn_count,
                "since_start_sec": int(duration),
                "max_turns": MAX_TURNS,
                "max_duration_sec": MAX_DURATION_SEC,
                "idle_timeout_sec": IDLE_TIMEOUT_SEC,
            },
        )

    out.update({
        "started_at": started_at_iso,
        "last_activity_at": last_activity_iso,
        "turn_count": turn_count,
        "end_session": end_session,
        "messages": msgs,
    })
    return out
