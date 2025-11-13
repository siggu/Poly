# -*- coding: utf-8 -*-
"""
session_orchestrator.py
- 세션 수명주기 관리 노드
- 기능:
  * 세션 초기화: session_id 확인, started_at/turn_count 준비
  * 매 턴 갱신: last_activity_at 갱신, turn_count 증가
  * 종료 판단: idle_timeout / max_turns / max_duration 기준으로 end_session True 설정
- 구성:
  * .env 로드: SESSION_IDLE_TIMEOUT_SEC / SESSION_MAX_TURNS / SESSION_MAX_DURATION_SEC
  * messages에 tool 로그를 남겨 디버깅 용이
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

# ─────────────────────────────────────────────────────────
# 타입 정의 (서비스 전역 State TypedDict와 호환되도록 최소 키만 사용)
# ─────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # Python 3.11+ 에서 fromisoformat가 Z를 처리
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None

def _append_tool_log(msgs: List[Message], text: str, meta: Optional[Dict[str, Any]] = None) -> None:
    msgs.append({
        "role": "tool",
        "content": text,
        "created_at": _now_iso(),
        "meta": meta or {}
    })

# ─────────────────────────────────────────────────────────
# 메인 노드 함수
#  - LangGraph에서 add_node("session_orchestrator", orchestrate) 형태로 사용
# ─────────────────────────────────────────────────────────
def orchestrate(state: Dict[str, Any]) -> SessionOrchestratorOutput:
    """
    입력(state)에서 다음 키를 참조(있으면 사용, 없으면 초기화):
      - session_id: str
      - started_at: ISO str
      - last_activity_at: ISO str
      - turn_count: int
      - messages: List[Message]

    출력:
      - session_id, started_at, last_activity_at, turn_count
      - end_session: bool (종료 판단 결과)
      - messages: tool 로그 1줄 이상 append
    """
    out: SessionOrchestratorOutput = {}
    msgs: List[Message] = list(state.get("messages") or [])

    # 1) 세션 ID 확인
    sid = (state.get("session_id") or "").strip()
    if not sid:
        # 세션 ID가 없으면 간단히 타임스탬프 기반 ID를 만든다 (실전에서는 외부에서 주입 권장)
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
    idle_deadline = last_activity_dt - timedelta(seconds=IDLE_TIMEOUT_SEC)  # 참고용
    duration = (now - started_dt).total_seconds()
    idle_ok = True  # 이번 턴은 호출 시점이므로 idle 초과는 보통 외부에서 트리거됨

    end_reasons: List[str] = []

    # max_turns
    if turn_count >= MAX_TURNS:
        end_reasons.append(f"max_turns({MAX_TURNS}) reached")

    # max_duration
    if duration >= MAX_DURATION_SEC:
        end_reasons.append(f"max_duration({MAX_DURATION_SEC}s) reached")

    # idle_timeout은 외부 타이머가 세션을 깨우지 않을 때 의미가 있으나,
    # 여기서는 참고용 메타만 남긴다. (필요 시 외부에서 end_session=True로 호출)
    # idle_ok = (now - last_activity_dt).total_seconds() < IDLE_TIMEOUT_SEC

    end_session = bool(end_reasons)
    if end_session:
        _append_tool_log(msgs, "[session_orchestrator] end_session=True", {"reasons": end_reasons})
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

    # 5) 출력 정리
    out.update({
        "started_at": started_at_iso,
        "last_activity_at": last_activity_iso,
        "turn_count": turn_count,
        "end_session": end_session,
        "messages": msgs,
    })
    return out
