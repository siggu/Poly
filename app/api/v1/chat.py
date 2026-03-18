# app/api/v1/chat.py
from __future__ import annotations

from typing import Optional, Dict, Any, List
from uuid import uuid4
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import json

import logging

from app.agents.new_pipeline import build_graph

logger = logging.getLogger(__name__)
router = APIRouter()

# ⭐ 전역 캐시 (싱글톤 패턴)
_graph_app = None
_graph_init_error = None  # 초기화 에러 저장


def get_graph_app():
    """LangGraph 인스턴스를 lazy하게 로드 (첫 호출 시 1회만 생성)"""
    global _graph_app, _graph_init_error

    # 이미 초기화 실패한 경우 즉시 에러
    if _graph_init_error:
        raise _graph_init_error

    if _graph_app is None:
        try:
            logger.info("LangGraph 워크플로우 초기화 중...")
            _graph_app = build_graph()
            logger.info("LangGraph 초기화 완료")
        except Exception as e:
            _graph_init_error = HTTPException(
                status_code=503, detail=f"LangGraph 초기화 실패: {str(e)}"
            )
            logger.error("LangGraph 초기화 실패: %s", e)
            raise _graph_init_error

    return _graph_app


# ─────────────────────────────────────
# Request / Response Models
# ─────────────────────────────────────


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    profile_id: Optional[int] = None  # 👈 프로필 ID 추가
    user_input: str
    user_action: str = "none"
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
    """
    채팅 메시지를 처리하고 응답을 반환합니다.

    첫 호출 시 LangGraph 워크플로우를 초기화합니다 (약 1-2초 소요).
    이후 호출은 캐시된 인스턴스를 사용하여 즉시 처리됩니다.
    """
    # A) 세션 ID 생성/유지
    session_id = req.session_id or f"sess-{uuid4().hex}"

    # 🔍 디버깅 로그 추가
    logger.info("chat API: user_action='%s', user_input='%s', session_id=%s", req.user_action, req.user_input[:50] if req.user_input else '(empty)', session_id)

    # B) LangGraph에 넘길 초기 state
    base_end_session = req.user_action in ("reset_save", "reset_drop")
    init_state: Dict[str, Any] = {
        "session_id": session_id,
        "profile_id": req.profile_id,
        "user_input": req.user_input,
        "user_action": req.user_action,
        "end_session": base_end_session,
        "client_meta": req.client_meta,
    }

    # C) 세션 기반 체크포인트 사용
    config = {"configurable": {"thread_id": session_id}}

    # ⭐ D) LangGraph 실행 (lazy loading)
    try:
        graph_app = get_graph_app()
    except HTTPException as e:
        # 초기화 실패 시 사용자에게 명확한 에러 메시지
        raise e

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

    router_decision = (
        "save"
        if req.user_action == "save"
        else (
            req.user_action
            if req.user_action in ("reset_save", "reset_drop")
            else "normal"
        )
    )

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


# ─────────────────────────────────────
# /api/chat/stream (스트리밍)
# ─────────────────────────────────────
@router.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    """
    채팅 메시지를 처리하고 스트리밍 방식으로 응답을 반환합니다.
    Server-Sent Events (SSE) 형식으로 응답을 전송합니다.

    첫 토큰이 0.5-1초 내 도착하여 사용자 체감 대기시간을 최소화합니다.
    """
    from app.langgraph.nodes.llm_answer_creator import run_answer_llm_stream

    # A) 세션 ID 생성/유지
    session_id = req.session_id or f"sess-{uuid4().hex}"

    logger.info("chat/stream API: user_action='%s', user_input='%s', session_id=%s", req.user_action, req.user_input[:50] if req.user_input else '(empty)', session_id)

    # B) LangGraph에 넘길 초기 state (streaming_mode=True 추가)
    base_end_session = req.user_action in ("reset_save", "reset_drop")
    init_state: Dict[str, Any] = {
        "session_id": session_id,
        "profile_id": req.profile_id,
        "user_input": req.user_input,
        "user_action": req.user_action,
        "end_session": base_end_session,
        "client_meta": req.client_meta,
        "streaming_mode": True,  # 스트리밍 모드 활성화
    }

    # C) 세션 기반 체크포인트 사용
    config = {"configurable": {"thread_id": session_id}}

    # D) LangGraph 실행 (answer_llm은 스킵됨)
    try:
        graph_app = get_graph_app()
    except HTTPException as e:
        raise e

    out_state: Dict[str, Any] = graph_app.invoke(init_state, config=config)

    # E) 스트리밍용 컨텍스트 추출
    streaming_ctx = out_state.get("streaming_context") or {}

    input_text = streaming_ctx.get("input_text", "")
    used = streaming_ctx.get("used", "NONE")
    profile_ctx = streaming_ctx.get("profile_ctx")
    collection_ctx = streaming_ctx.get("collection_ctx")
    summary = streaming_ctx.get("summary")
    documents = streaming_ctx.get("documents")

    # F) 디버그 정보 준비
    retrieval = out_state.get("retrieval") or {}
    rag_snippets = retrieval.get("rag_snippets") or []

    router_decision = (
        "save"
        if req.user_action == "save"
        else (
            req.user_action
            if req.user_action in ("reset_save", "reset_drop")
            else "normal"
        )
    )

    used_rag = retrieval.get("used_rag")

    policy_ids: List[int] = []
    for doc in rag_snippets:
        doc_id = doc.get("doc_id")
        if isinstance(doc_id, int):
            policy_ids.append(doc_id)

    # G) 스트리밍 제너레이터 정의
    async def generate():
        # 1) 메타데이터 전송 (세션 정보, 디버그 정보)
        metadata = {
            "type": "metadata",
            "session_id": session_id,
            "debug": {
                "router_decision": router_decision,
                "used_rag": bool(used_rag),
                "policy_ids": policy_ids,
            },
        }
        yield f"data: {json.dumps(metadata, ensure_ascii=False)}\n\n"

        # 2) LLM 스트리밍 시작
        full_text = ""
        try:
            for chunk in run_answer_llm_stream(
                input_text=input_text,
                used=used,
                profile_ctx=profile_ctx,
                collection_ctx=collection_ctx,
                summary=summary,
                documents=documents,
            ):
                full_text += chunk
                chunk_data = {
                    "type": "chunk",
                    "content": chunk,
                }
                yield f"data: {json.dumps(chunk_data, ensure_ascii=False)}\n\n"

        except Exception as e:
            error_data = {
                "type": "error",
                "message": f"스트리밍 중 오류 발생: {str(e)}",
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
            full_text = f"[오류 발생: {str(e)}]"

        # 3) 완료 메시지 전송
        session_ended = bool(
            req.user_action in ("reset_save", "reset_drop")
            or out_state.get("end_session") is True
        )

        persist_result = out_state.get("persist_result") or {}
        save_result = None
        if persist_result:
            save_result = "ok" if persist_result.get("ok") else "error"

        done_data = {
            "type": "done",
            "session_ended": session_ended,
            "save_result": save_result,
            "total_length": len(full_text),
        }
        yield f"data: {json.dumps(done_data, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Nginx 버퍼링 비활성화
        },
    )
