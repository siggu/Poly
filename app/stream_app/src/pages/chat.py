"""채팅 렌더링/메시지 전송/정책 카드 파싱 11.10수정"""
import uuid
import time
import json
import re
import streamlit as st

from src.widgets.policy_card import render_policy_card
from src.utils.template_loader import load_template, render_template, load_css
from src.llm_manager import get_llm_manager


SUGGESTED_QUESTIONS = [
    "청년 주거 지원 정책이 궁금해요",
    "취업 지원 프로그램 알려주세요",
    "창업 지원금 신청 방법은?",
    "육아 지원 혜택 찾아주세요",
]


def _extract_policies_from_text(text: str):
    try:
        code_blocks = re.findall(r"```json\\s*([\\s\\S]*?)\\s*```", text, re.IGNORECASE)
        for block in code_blocks:
            data = json.loads(block)
            if (
                isinstance(data, dict)
                and "policies" in data
                and isinstance(data["policies"], list)
            ):
                return data["policies"]
    except Exception:
        pass
    return []


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
        llm_manager = get_llm_manager()
        placeholder = st.empty()
        collected = ""
        for delta in llm_manager.generate_response_stream(
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


def render_chatbot_main():
    load_css("components/chat_messages.css")
    load_css("components/chat_ui.css")

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
    col_input, col_send = st.columns([9, 1])
    with col_input:
        user_input = st.text_input(
            "정책에 대해 질문해주세요...",
            key="user_input",
            label_visibility="collapsed",
        )
    with col_send:
        if st.button("✈️", key="btn_send", use_container_width=True):
            if user_input.strip():
                handle_send_message(user_input)

    render_template("components/disclaimer.html")
