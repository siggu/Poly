"""
LLM 채팅 관련 API 엔드포인트 라우터입니다.
- /api/v1/chat/stream: LLM 응답 스트리밍
"""

from fastapi import APIRouter, Body
from fastapi.responses import StreamingResponse

from ...backend.llm_manager import get_llm_manager
from app.backend.models import LLMRequest, LLMResponse
from typing import List, Dict, Any

# LLMManager 인스턴스 초기화
llm_manager = get_llm_manager()

# APIRouter 인스턴스 생성
router = APIRouter(
    prefix="/chat",
    tags=["Chat"],
)


@router.post("/stream", response_model=None)
async def stream_llm_response(
    history_messages: List[Dict[str, Any]] = Body(...),
    user_message: str = Body(...),
    active_profile: Dict[str, Any] | None = Body(None),
):
    """
    LLM 응답을 스트리밍으로 제공합니다.
    Streamlit 클라이언트의 get_llm_response_stream에서 이 엔드포인트를 호출합니다.
    """

    # LLMManager의 스트리밍 제너레이터 호출
    generator = llm_manager.generate_response_stream(
        history_messages=history_messages,
        user_message=user_message,
        active_profile=active_profile,
    )

    # FastAPI의 StreamingResponse를 사용하여 LLM의 제너레이터를 클라이언트로 전달
    return StreamingResponse(generator, media_type="text/plain")


@router.post("/generate", response_model=LLMResponse)
async def generate_llm_response(
    history_messages: List[Dict[str, Any]] = Body(...),
    user_message: str = Body(...),
    active_profile: Dict[str, Any] | None = Body(None),
):
    """
    LLM 응답을 비스트리밍으로 제공합니다. (선택적 구현)
    """
    response_data = llm_manager.generate_response(
        history_messages=history_messages,
        user_message=user_message,
        active_profile=active_profile,
    )

    return LLMResponse(**response_data)
