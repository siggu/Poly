# app/langgraph/state/ephemeral_context.py
# -*- coding: utf-8 -*-
"""
ephemeral_context.py

LangGraph 전체에서 공유하는 상태(State) 스키마 정의.

- 목적:
  * 세션 동안 인메모리에 유지되는 ephemeral 컨텍스트 구조를 단일 소스로 관리
  * 각 노드(session_orchestrator, query_router, info_extractor,
    policy_retriever_node, answer_llm, persist_pipeline 등)가
    동일한 타입을 바라보도록 함

- 특징:
  * messages 는 Annotated[..., operator.add] 로 append-only reducer 설정
  * DB에 영구 저장되는 것은 persist_pipeline에서만 처리하고,
    여기 State는 "그래프 실행 중" 관리를 담당
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict, Literal

try:
    # Python 3.11+
    from typing import Annotated
except ImportError:  # Python 3.8~3.10
    from typing_extensions import Annotated

import operator


# ─────────────────────────────────────────────────────────
# 기본 단위 타입
# ─────────────────────────────────────────────────────────

class Message(TypedDict, total=False):
    """
    한 턴의 메시지 단위.
    - role: user / assistant / tool
    - content: 본문
    - created_at: ISO8601 문자열 (UTC 권장)
    - meta: 토큰 사용량, no_store 플래그, tool_name 등 부가 정보
    """
    role: Literal["user", "assistant", "tool"]
    content: str
    created_at: str
    meta: Dict[str, Any]


class RagSnippet(TypedDict, total=False):
    """
    RAG로 가져온 정책/문서 스니펫 단위.
    - doc_id: documents.id 또는 외부 문서 식별자
    - source: "policy_db" 등 출처
    - title: 정책/문서 제목
    - snippet: 요약 또는 발췌
    - score: 유사도/랭킹 점수
    """
    doc_id: str
    source: str
    title: Optional[str]
    snippet: str
    score: Optional[float]
    region: Optional[str]
    url: Optional[str]
    requirements: Optional[str]
    benefits: Optional[str]


class PersistResult(TypedDict, total=False):
    """
    persist_pipeline 실행 결과 요약.
    - ok: 전체 성공 여부
    - conversation_id: 저장된 대화 ID (UUID string)
    - counts: {"messages": int, "embeddings": int}
    - warnings: 경고 메시지 리스트
    """
    ok: bool
    conversation_id: Optional[str]
    counts: Dict[str, int]
    warnings: List[str]


# ─────────────────────────────────────────────────────────
# State (그래프 전체에서 공유하는 컨텍스트)
# ─────────────────────────────────────────────────────────

class EphemeralContextState(TypedDict, total=False):
    # ── 세션/제어 ───────────────────────────────────────
    session_id: str
    end_session: bool
    started_at: str
    last_activity_at: str
    turn_count: int

    # ── 대화 컨텍스트 ───────────────────────────────────
    messages: Annotated[List[Message], operator.add]
    rolling_summary: Optional[str]

    # 🔹 user_context_node가 만드는 파생 컨텍스트 (반드시 유지!)
    merged_profile: Dict[str, Any]
    merged_collection: Dict[str, Any]
    profile_summary_text: Optional[str]
    history_text: Optional[str]

    # ── 사용자 프로필/컬렉션 오버레이 ───────────────────
    profile_id: Optional[int]
    ephemeral_profile: Dict[str, Any]
    ephemeral_collection: Dict[str, Any]

    # ── RAG 관련 ────────────────────────────────────────
    retrieval: Dict[str, Any]   # used_rag / rag_snippets / profile_summary_text ...

    # ── 입출력 ─────────────────────────────────────────
    user_input: Optional[str]
    answer: Dict[str, Any]
    user_action: Optional[str]

    # ── Router 결정 값 ──────────────────────────────────
    router: Dict[str, Any]

    # ── Persist 결과 ────────────────────────────────────
    persist_result: PersistResult



# alias 편의를 위해 짧은 이름도 제공
State = EphemeralContextState

__all__ = [
    "Message",
    "RagSnippet",
    "PersistResult",
    "EphemeralContextState",
    "State",
]
