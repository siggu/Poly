import streamlit as st
import uuid
import time
from src.state_manger import initialize_session_state
from src.widgets.policy_card import render_policy_card

# 페이지 시작 시 세션 상태 초기화
initialize_session_state()

# Streamlit 페이지 설정
st.set_page_config(
    page_title="정책 추천 챗봇",
    page_icon="💬",
    layout="wide",
)

# React의 suggestedQuestions 대체
SUGGESTED_QUESTIONS = [
    "청년 주거 지원 정책이 궁금해요",
    "취업 지원 프로그램 알려주세요",
    "창업 지원금 신청 방법은?",
    "육아 지원 혜택 찾아주세요",
]


def handle_send_logic(prompt: str):
    """메시지 전송 처리 함수"""
    if not prompt.strip() or st.session_state["is_loading"]:
        return

    # 사용자 메시지 추가
    user_message = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": prompt,
        "timestamp": time.time(),
    }
    st.session_state.messages.append(user_message)
    st.session_state["input"] = ""
    st.session_state["is_loading"] = True

    try:
        st.rerun()
    except Exception:
        st.error("페이지 새로고침 중 오류가 발생했습니다.")


def render_chatbot_page():
    """챗봇 페이지 UI 렌더링"""
    # 채팅 메시지 표시 영역
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.messages:
            if message["role"] == "user":
                st.write(f"👤: {message['content']}")
            else:
                st.write(f"🤖: {message['content']}")
                if "policies" in message:
                    for idx, policy in enumerate(message["policies"]):
                        # 정책 카드 위젯을 사용하여 렌더링
                        render_policy_card(policy)

    # 입력 영역
    st.markdown("---")
    col1, col2 = st.columns([8, 2])
    with col1:
        st.text_input(
            "메시지를 입력하세요...",
            key="user_input",
            on_change=lambda: handle_send_logic(st.session_state.user_input),
        )

    # 추천 질문 영역
    st.markdown("### 추천 질문")
    cols = st.columns(len(SUGGESTED_QUESTIONS))
    for idx, question in enumerate(SUGGESTED_QUESTIONS):
        with cols[idx]:
            if st.button(question, key=f"suggest_{idx}"):
                handle_send_logic(question)


if __name__ == "__main__":
    render_chatbot_page()
