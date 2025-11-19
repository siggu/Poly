# app/api/v1/chat.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional, Dict, Any, List
from uuid import uuid4
from fastapi import APIRouter
from pydantic import BaseModel

# LangGraph 엔진
from app.agents.new_pipeline import build_graph

# LangGraph Runnable 로드 (초기 1회)
graph_app = build_graph()

router = APIRouter()

# ─────────────────────────────────────
# Request / Response Models
# ─────────────────────────────────────


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    user_input: str
    user_action: str = "none"  # none | save | reset_save | reset_drop
    client_meta: Dict[str, Any] = {}


class ChatDebug(BaseModel):
    router_decision: Optional[str] = None
    used_rag: Optional[bool] = None
    policy_ids: List[int] = []


class ChatResponse(BaseModel):
    session_id: str
    answer: str
    session_ended: bool
    save_result: Optional[str]
    debug: ChatDebug


# ─────────────────────────────────────
# /api/chat
# ─────────────────────────────────────
@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    # A) 세션 ID 생성/유지
    session_id = req.session_id or f"sess-{uuid4().hex}"

    # B) LangGraph에 넘길 초기 state
    base_end_session = req.user_action in ("reset_save", "reset_drop")
    init_state: Dict[str, Any] = {
        "session_id": session_id,
        "user_input": req.user_input,
        "user_action": req.user_action,
        "end_session": base_end_session,
        "client_meta": req.client_meta,
    }

    # C) 세션 기반 체크포인트 사용
    config = {"configurable": {"thread_id": session_id}}

    # D) LangGraph 실행
    out_state: Dict[str, Any] = graph_app.invoke(init_state, config=config)

    # ─────────────────────────────────────
    # 답변 텍스트 추출
    # ─────────────────────────────────────
    raw_answer = out_state.get("answer")
    if isinstance(raw_answer, dict):
        answer_text = raw_answer.get("text") or ""
    else:
        answer_text = raw_answer or ""

    # ─────────────────────────────────────
    # 세션 종료 여부
    # ─────────────────────────────────────
    session_ended = bool(
        req.user_action in ("reset_save", "reset_drop")
        or out_state.get("end_session") is True
    )

    # ─────────────────────────────────────
    # persist_pipeline 결과
    # ─────────────────────────────────────
    persist_result = out_state.get("persist_result") or {}
    if persist_result:
        save_result = "ok" if persist_result.get("ok") else "error"
    else:
        save_result = None

    # ─────────────────────────────────────
    # 디버그 정보
    # ─────────────────────────────────────
    retrieval = out_state.get("retrieval") or {}
    rag_snippets = retrieval.get("rag_snippets") or []

    # router_decision (UI friendly)
    if req.user_action == "save":
        router_decision = "save"
    elif req.user_action in ("reset_save", "reset_drop"):
        router_decision = req.user_action
    else:
        router_decision = "normal"

    used_rag = retrieval.get("used_rag")

    policy_ids: List[int] = []
    for doc in rag_snippets:
        doc_id = doc.get("doc_id")
        if isinstance(doc_id, int):
            policy_ids.append(doc_id)

    debug = ChatDebug(
        router_decision=router_decision,
        used_rag=bool(used_rag),
        policy_ids=policy_ids,
    )

    # ─────────────────────────────────────
    # 최종 응답
    # ─────────────────────────────────────
    return ChatResponse(
        session_id=session_id,
        answer=answer_text,
        session_ended=session_ended,
        save_result=save_result,
        debug=debug,
    )
