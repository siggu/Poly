"""채팅 렌더링/메시지 전송/정책 카드 파싱 11.13수정"""

import uuid
import time
import streamlit as st
from src.widgets.policy_card import render_policy_card
from src.utils.template_loader import render_template, load_css
from src.backend_service import backend_service


SUGGESTED_QUESTIONS = [
    "청년 주거 지원 정책이 궁금해요",
    "취업 지원 프로그램 알려주세요",
    "창업 지원금 신청 방법은?",
    "육아 지원 혜택 찾아주세요",
]


def _get_auth_token():
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def _extract_policies_from_text(text: str):
    """
    이 함수는 더 이상 사용되지 않습니다. 항상 None을 반환합니다.
    """
    return None


# 챗봇 메세지 응답 화면
def handle_send_message(message: str):
    if not message.strip() or st.session_state.get("is_loading", False):
        return

    user_message = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": message,
        "timestamp": time.time(),
    }
    if "messages" not in st.session_state:
        st.session_state.messages = []
    st.session_state.messages.append(user_message)

    st.session_state["is_loading"] = True

    active_profile = next(
        (p for p in st.session_state.profiles if p.get("isActive", False)), None
    )

    try:
        with st.spinner("답변 생성중..."):
            placeholder = st.empty()
            collected = ""
            for delta in backend_service.get_llm_response_stream(
                history_messages=st.session_state.get("messages", []),
                user_message=message,
                active_profile=active_profile,
            ):
                collected += delta
                placeholder.markdown(collected)

        assistant_message = {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": collected or "응답을 받았습니다.",
            "timestamp": time.time(),
        }

        policies = _extract_policies_from_text(collected)
        if policies:
            assistant_message["policies"] = policies

        st.session_state.messages.append(assistant_message)
    except Exception as e:
        error_message = {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": f"죄송합니다. 오류가 발생했습니다: {e}",
            "timestamp": time.time(),
        }
        st.session_state.messages.append(error_message)

    st.session_state["is_loading"] = False
    st.session_state["clear_user_input"] = True
    st.rerun()


# 챗봇 메인 페이지
def render_chatbot_main():
    load_css("components/chat_messages.css")
    load_css("components/chat_ui.css")

    # 대화 저장 확인 상태 초기화
    if "save_chat_confirmation" not in st.session_state:
        st.session_state.save_chat_confirmation = False

    if st.session_state.get("clear_user_input", False):
        st.session_state["user_input"] = ""
        st.session_state["clear_user_input"] = False

    col_header_left, col_header_right = st.columns([8, 1])
    with col_header_left:
        render_template("components/chat_header.html")
    with col_header_right:
        if st.button("👤", key="btn_my_page", help="마이페이지"):
            st.session_state["show_profile"] = True
            st.rerun()

    render_template("components/chat_title.html")

    if st.session_state.get("messages"):
        for message in st.session_state.messages:
            if message["role"] == "user":
                render_template(
                    "components/chat_message_user.html", content=message["content"]
                )
            elif message["role"] == "assistant":
                render_template(
                    "components/chat_message_assistant.html",
                    content=message["content"],
                )
                if "policies" in message:
                    for policy in message["policies"]:
                        render_policy_card(policy)

    render_template("components/suggested_questions_header.html")

    cols = st.columns(2)
    for idx, question in enumerate(SUGGESTED_QUESTIONS):
        with cols[idx % 2]:
            if st.button(
                question,
                key=f"suggest_{idx}",
                use_container_width=True,
                type="secondary",
            ):
                handle_send_message(question)

    st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)

    # 폼을 사용하여 엔터 키로 메시지 전송
    with st.form(key="chat_input_form", clear_on_submit=True):
        col_input, col_send = st.columns([9, 1])
        with col_input:
            user_input = st.text_input(
                "정책에 대해 질문해주세요...",
                key="user_input",
                label_visibility="collapsed",
            )
        with col_send:
            submitted = st.form_submit_button("✈️", use_container_width=True)

        if submitted and user_input.strip():
            handle_send_message(user_input)

    render_template("components/disclaimer.html")

    # --- 대화 저장 및 초기화 UI ---
    st.markdown("---")
    if st.session_state.save_chat_confirmation:
        st.warning("현재 대화 내용을 저장하시겠습니까? 저장하지 않은 대화는 사라집니다.")
        col1, col2, col3 = st.columns([1.5, 1.5, 1])
        with col1:
            if st.button("💾 저장하고 초기화", use_container_width=True):
                token = _get_auth_token()
                if token:
                    # TODO: 백엔드에 대화 저장 API 호출 (backend_service 사용)
                    # success, msg = backend.save_chat_history(token, st.session_state.messages)
                    # if success:
                    #     st.toast("대화 내용이 저장되었습니다.")
                    # else:
                    #     st.error(f"저장 실패: {msg}")
                    st.toast("대화 내용 저장 기능은 구현 예정입니다.")
                st.session_state.messages = []
                st.session_state.save_chat_confirmation = False
                st.rerun()
        with col2:
            if st.button("🗑️ 저장하지 않고 초기화", use_container_width=True):
                st.session_state.messages = []
                st.session_state.save_chat_confirmation = False
                st.rerun()
        with col3:
            if st.button("취소", use_container_width=True):
                st.session_state.save_chat_confirmation = False
                st.rerun()
    else:
        col_save, col_reset = st.columns(2)
        with col_save:
            if st.button("💾 대화 저장", use_container_width=True):
                token = _get_auth_token()
                if token:
                    # TODO: 백엔드에 대화 저장 API 호출 (backend_service 사용)
                    # success, msg = backend.save_chat_history(token, st.session_state.messages)
                    # if success:
                    #     st.toast("대화 내용이 저장되었습니다.")
                    # else:
                    #     st.error(f"저장 실패: {msg}")
                    st.toast("대화 내용 저장 기능은 구현 예정입니다.")
                else:
                    st.warning("로그인이 필요합니다.")

        with col_reset:
            if st.button("🔄 초기화", use_container_width=True):
                if len(st.session_state.get("messages", [])) > 1:
                    st.session_state.save_chat_confirmation = True
                    st.rerun()
                else:
                    st.toast("초기화할 대화 내용이 없습니다.")
