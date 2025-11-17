""" 11.13수정
LLM 초기화 및 응답/스트리밍 로직을 담당하는 백엔드 LLM 매니저.
stream_app/src/llm_manager.py에서 백엔드(FastAPI)로 이전된 코드입니다.
"""

import os
from typing import List, Dict, Any, Optional, Iterator

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage

# 환경 변수가 로드되었다고 가정하고, OPENAI_API_KEY를 직접 사용합니다.
# (FastAPI 앱이 시작될 때 .env 파일이 로드될 것이라고 가정합니다.)


class LLMManager:
    """
    LLM 초기화 및 응답 생성을 담당하는 매니저.
    - Initialization: ChatOpenAI(model="gpt-4o-mini")
    - Response Generation: 대화 메시지 + 프로필 컨텍스트를 바탕으로 답변 생성
    """

    # 싱글톤 패턴 유지를 위한 인스턴스 변수
    _instance: Optional["LLMManager"] = None

    def __init__(self, model: str = "gpt-4o-mini"):
        # LLM 초기화 (GPT-4o-mini 사용)
        self.llm = ChatOpenAI(model=model, api_key=os.getenv("OPENAI_API_KEY"))

    @classmethod
    def get_instance(cls) -> "LLMManager":
        """LLMManager의 싱글톤 인스턴스를 반환합니다."""
        if cls._instance is None:
            # 환경 변수가 설정되어 있지 않으면 오류를 발생시킵니다.
            if not os.getenv("OPENAI_API_KEY"):
                print("Error: OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

            cls._instance = LLMManager()
        return cls._instance

    def _build_context_system_prompt(self, profile: Optional[Dict[str, Any]]) -> str:
        """주어진 프로필을 바탕으로 시스템 프롬프트를 구성합니다."""
        if not profile:
            base = """
                당신은 한국의 복지/의료/정책 정보를 친절하고 정확하게 안내하는 어시스턴트입니다.
                반말은 피하고, 과도하게 장황하지 않게 명확하고 실용적으로 답변하세요.
                가능하면 항목형식(불릿)으로 요점을 먼저 제시하세요.
                """
        else:
            # 프로필 정보를 활용하는 더 구체적인 프롬프트
            # 현재는 주석 처리된 프로필 정보는 제외하고 기본 베이스만 유지합니다.
            base = """
                당신은 한국의 복지/의료/정책 정보를 안내하는 어시스턴트입니다.
                아래 이용자 프로필을 참고하여 적합한 정보를 제공하세요. 
                반말은 피하고, 명확하고 실용적으로 답변하세요.
                가능하면 항목형식(불릿)으로 요점을 먼저 제시하세요.
                """
        return base

    def _build_langchain_messages(
        self,
        system_prompt: str,
        history_messages: List[Dict[str, Any]],
        user_message: str,
    ) -> List[Any]:
        """사용자 메시지와 대화 이력을 LangChain 메시지 형식으로 변환합니다."""
        lc_messages = [SystemMessage(content=system_prompt)]

        # 과거 대화 이력을 모델 포맷으로 변환
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
        return lc_messages

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
        lc_messages = self._build_langchain_messages(
            system_prompt, history_messages, user_message
        )

        try:
            # 모델 호출
            ai_msg = self.llm.invoke(lc_messages)
            content = getattr(ai_msg, "content", "") or "응답을 생성하지 못했습니다."
            return {"content": content}
        except Exception as e:
            print(f"LLM generate_response error: {e}")
            return {"content": f"LLM 호출 중 오류가 발생했습니다: {str(e)}"}

    def generate_response_stream(
        self,
        history_messages: List[Dict[str, Any]],
        user_message: str,
        active_profile: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """
        토큰 스트리밍 제너레이터. 텍스트 델타를 yield.
        """
        system_prompt = self._build_context_system_prompt(active_profile)
        lc_messages = self._build_langchain_messages(
            system_prompt, history_messages, user_message
        )

        try:
            for chunk in self.llm.stream(lc_messages):
                text = getattr(chunk, "content", None)
                if text:
                    yield text
        except Exception as e:
            # 스트림 도중 오류가 발생하면 오류 메시지를 yield합니다.
            print(f"LLM generate_response_stream error: {e}")
            yield f"\n\nLLM 스트리밍 중 오류가 발생했습니다: {str(e)}"


# 편의 함수
def get_llm_manager() -> LLMManager:
    """LLMManager의 싱글톤 인스턴스를 가져옵니다."""
    return LLMManager.get_instance()
