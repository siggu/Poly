"""ChatOpenAI 초기화 및 응답/스트리밍 11.10 수정"""
import os
from typing import List, Dict, Any, Optional

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage


class LLMManager:
    """
    LLM 초기화 및 응답 생성을 담당하는 매니저.
    - Initialization: ChatOpenAI(model="gpt-4o-mini")
    - Response Generation: 대화 메시지 + 프로필 컨텍스트를 바탕으로 답변 생성
    """

    _instance: Optional["LLMManager"] = None

    def __init__(self, model: str = "gpt-4o-mini"):
        # LLM 초기화 (GPT-4o-mini 사용)
        self.llm = ChatOpenAI(model=model)

    @classmethod
    def get_instance(cls) -> "LLMManager":
        if cls._instance is None:
            cls._instance = LLMManager()
        return cls._instance

    def _build_context_system_prompt(self, profile: Optional[Dict[str, Any]]) -> str:
        if not profile:
            base = (
                "당신은 한국의 복지/의료/정책 정보를 친절하고 정확하게 안내하는 어시스턴트입니다. "
                "반말은 피하고, 과도하게 장황하지 않게 명확하고 실용적으로 답변하세요. "
                "가능하면 항목형식(불릿)으로 요점을 먼저 제시하세요. "
                "정책을 추천할 수 있다면 답변 마지막에 정책 정보를 JSON 코드블록으로 첨부하세요."
            )
        else:
            base = (
                "당신은 한국의 복지/의료/정책 정보를 안내하는 어시스턴트입니다. "
                "아래 이용자 프로필을 참고하여 적합한 정보를 제공하세요. "
                "반말은 피하고, 명확하고 실용적으로 답변하세요. "
                "가능하면 항목형식(불릿)으로 요점을 먼저 제시하세요. "
                "정책을 추천할 수 있다면 답변 마지막에 정책 정보를 JSON 코드블록으로 첨부하세요.\n\n"
                f"- 이름: {profile.get('name', '')}\n"
                f"- 성별: {profile.get('gender', '')}\n"
                f"- 거주지: {profile.get('location', '')}\n"
                f"- 건강보험: {profile.get('healthInsurance', '')}\n"
                f"- 소득수준: {profile.get('incomeLevel', '')}\n"
                f"- 기초생활수급: {profile.get('basicLivelihood', '')}\n"
                f"- 장애등급: {profile.get('disabilityLevel', '')}\n"
                f"- 장기요양: {profile.get('longTermCare', '')}\n"
                f"- 임신/출산: {profile.get('pregnancyStatus', '')}\n"
            )
        # JSON 코드블록 안내
        json_hint = (
            "\n\nJSON 코드블록 형식 예시를 따르세요:\n"
            "```json\n"
            "{\n"
            '  "policies": [\n'
            '    {\n'
            '      "id": "1",\n'
            '      "title": "정책 제목",\n'
            '      "description": "정책 설명",\n'
            '      "eligibility": "자격 요건",\n'
            '      "benefits": "혜택 요약",\n'
            '      "applicationUrl": "https://..."\n'
            "    }\n"
            "  ]\n"
            "}\n"
            "```"
        )
        return base + json_hint

    def generate_response(
        self,
        history_messages: List[Dict[str, Any]],
        user_message: str,
        active_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        대화 이력과 현재 사용자 입력, 프로필을 바탕으로 어시스턴트 응답을 생성
        Returns: {"content": str}
        """
        system_prompt = self._build_context_system_prompt(active_profile)
        lc_messages = [SystemMessage(content=system_prompt)]

        # 과거 대화 이력을 모델 포맷으로 변환 (간단 매핑)
        for m in history_messages or []:
            role = m.get("role")
            content = m.get("content", "")
            if not content:
                continue
            if role == "user":
                lc_messages.append(HumanMessage(content=content))
            elif role == "assistant":
                lc_messages.append(AIMessage(content=content))

        # 현재 사용자 입력 추가
        lc_messages.append(HumanMessage(content=user_message))

        # 모델 호출
        ai_msg = self.llm.invoke(lc_messages)
        return {"content": getattr(ai_msg, "content", "") or "응답을 생성하지 못했습니다."}

    def generate_response_stream(
        self,
        history_messages: List[Dict[str, Any]],
        user_message: str,
        active_profile: Optional[Dict[str, Any]] = None,
    ):
        """
        토큰 스트리밍 제너레이터. 텍스트 델타를 yield.
        """
        system_prompt = self._build_context_system_prompt(active_profile)
        lc_messages = [SystemMessage(content=system_prompt)]
        for m in history_messages or []:
            role = m.get("role")
            content = m.get("content", "")
            if not content:
                continue
            if role == "user":
                lc_messages.append(HumanMessage(content=content))
            elif role == "assistant":
                lc_messages.append(AIMessage(content=content))
        lc_messages.append(HumanMessage(content=user_message))

        for chunk in self.llm.stream(lc_messages):
            text = getattr(chunk, "content", None)
            if text:
                yield text


# 편의 함수
def get_llm_manager() -> LLMManager:
    return LLMManager.get_instance()


