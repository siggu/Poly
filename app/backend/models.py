"""
FastAPI 요청 및 응답 데이터 모델 (Pydantic)
"""

from typing import List, Optional
from pydantic import BaseModel, Field


# 대화 메시지 구조
class ChatMessage(BaseModel):
    """사용자 및 어시스턴트 메시지 구조"""

    role: str = Field(..., description="메시지 역할 (user 또는 assistant)")
    text: str = Field(..., description="메시지 내용")
    # Streamlit 페이지에서 사용하는 추가 필드는 Optional로 처리하거나 무시할 수 있습니다.
    id: Optional[str] = None
    timestamp: Optional[float] = None


# LLM 응답 요청 데이터 (Request Body)
class LLMRequest(BaseModel):
    """LLM 응답을 요청하는 데 사용되는 데이터 구조"""

    history_messages: List[ChatMessage] = Field(
        default_factory=list, description="과거 대화 이력 (시스템 프롬프트 제외)"
    )
    user_message: str = Field(..., description="현재 사용자의 새로운 질문")
    active_profile: Optional[dict] = Field(
        None, description="현재 활성화된 사용자 프로필"
    )


# LLM 응답 데이터 (Response Body - 비스트리밍용)
class LLMResponse(BaseModel):
    """LLM 응답의 결과 데이터 구조"""

    content: str = Field(..., description="LLM이 생성한 최종 응답 텍스트")
    # policies: Optional[List[PolicyCard]] = None # 정책 카드 필드 (추후 확장 가능)
