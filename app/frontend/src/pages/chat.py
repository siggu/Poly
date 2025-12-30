"""채팅 렌더링/메시지 전송/정책 카드 파싱"""

# app/frontend/src/pages/chat.py
import uuid
import time
import streamlit as st
from src.widgets.policy_card import render_policy_card
from src.utils.template_loader import render_template, load_css
from src.backend_service import backend_service


SUGGESTED_QUESTIONS = [
    "어르신을 위한 지원 정책",
    "영유아를 위한 지원 정책",
    "임산부를 위한 지원 정책",
    "의료비 지원 정책",
]


def _get_auth_token():
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def _extract_policies_from_text(text: str):
    """
    이 함수는 더 이상 사용되지 않습니다. 항상 None을 반환합니다.
    """
    return None


def handle_send_message(message: str):
    """사용자 메시지를 추가하고 즉시 rerun하여 화면에 표시"""
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

    # 로딩 상태 설정 및 즉시 rerun
    st.session_state["is_loading"] = True
    st.session_state["pending_message"] = message
    st.session_state["clear_user_input"] = True
    st.rerun()


def _process_streaming_response():
    """스트리밍 답변 처리 - 채팅 메시지 영역 내부에서 실행"""
    message = st.session_state.get("pending_message")
    if not message:
        st.session_state["is_loading"] = False
        return

    # 🔥 활성 프로필 가져오기 - user_info의 main_profile_id 사용
    token = _get_auth_token()
    profile_id = None

    # user_info에서 main_profile_id를 먼저 확인
    user_info = st.session_state.get("user_info")
    if user_info and "main_profile_id" in user_info:
        profile_id = user_info.get("main_profile_id")
    else:
        # user_info가 없으면 API에서 가져오기
        if token:
            ok, user_profile = backend_service.get_user_profile(token)
            if ok and isinstance(user_profile, dict):
                profile_id = user_profile.get("main_profile_id")
                # session_state에도 저장
                st.session_state["user_info"] = user_profile

    try:
        # 스트리밍 방식으로 답변 생성
        full_answer = ""

        # 커스텀 HTML 스타일로 스트리밍 플레이스홀더 생성
        placeholder = st.empty()

        # 먼저 로딩 인디케이터 표시
        placeholder.markdown(
            """
            <div class="chat-message-assistant">
                <div class="chat-avatar">AI</div>
                <div class="chat-bubble-assistant loading-skeleton">
                    <div class="typing-indicator">
                        <span></span>
                        <span></span>
                        <span></span>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 스트리밍 시작
        for event in backend_service.send_chat_message_stream(
            session_id=st.session_state.get("session_id"),
            token=token,
            user_input=message,
            profile_id=profile_id,
        ):
            event_type = event.get("type")

            if event_type == "metadata":
                # 세션 ID 업데이트
                new_session_id = event.get("session_id")
                if new_session_id:
                    st.session_state["session_id"] = new_session_id

                # 디버그 정보 저장
                if "debug" in event:
                    st.session_state["last_debug"] = event["debug"]

            elif event_type == "chunk":
                # 청크를 받아서 누적 및 실시간 표시 (커스텀 HTML 스타일 유지)
                chunk = event.get("content", "")
                full_answer += chunk

                # 같은 placeholder에서 답변 렌더링 (로딩 인디케이터를 대체)
                placeholder.markdown(
                    f"""
                    <div class="chat-message-assistant">
                        <div class="chat-avatar">AI</div>
                        <div class="chat-bubble-assistant">
                            <p>{full_answer}▌</p>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            elif event_type == "done":
                # 스트리밍 완료
                break

            elif event_type == "error":
                # 오류 발생
                error_msg = event.get("message", "알 수 없는 오류")
                full_answer = f"죄송합니다. 오류가 발생했습니다: {error_msg}"
                break

        # 최종 메시지를 placeholder에 표시 (커서 없이) - 깜빡임 방지
        final_display = full_answer if full_answer else "응답을 받지 못했습니다."
        placeholder.markdown(
            f"""
            <div class="chat-message-assistant">
                <div class="chat-avatar">AI</div>
                <div class="chat-bubble-assistant">
                    <p>{final_display}</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 최종 메시지를 session_state에 저장
        assistant_message = {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": final_display,
            "timestamp": time.time(),
        }

        # 정책 추출
        policies = _extract_policies_from_text(final_display)
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
    st.session_state["pending_message"] = None
    st.rerun()


def render_chatbot_main():
    load_css("components/chat_messages.css")
    load_css("components/chat_ui.css")

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

    # ✅ 채팅 메시지 영역 - 스크롤 가능한 컨테이너
    st.markdown('<div class="chat-container">', unsafe_allow_html=True)

    if st.session_state.get("messages"):
        for idx, message in enumerate(st.session_state.messages):
            if message["role"] == "user":
                # 사용자 메시지
                st.markdown(
                    f"""
                    <div class="chat-message-user">
                        <div class="chat-bubble-user">
                            <p>{message["content"]}</p>
                        </div>
                    </div>
                """,
                    unsafe_allow_html=True,
                )

            elif message["role"] == "assistant":
                # AI 응답 - 메시지 내용을 HTML 안에 직접 포함
                st.markdown(
                    f"""
                    <div class="chat-message-assistant">
                        <div class="chat-avatar">AI</div>
                        <div class="chat-bubble-assistant">
                            <p>{message["content"]}</p>
                        </div>
                """,
                    unsafe_allow_html=True,
                )

                # 정책 카드가 있으면 표시 (말풍선 밖에 표시)
                if "policies" in message:
                    for policy in message["policies"]:
                        render_policy_card(policy)

                # 인터랙션 버튼들
                st.markdown('<div class="message-actions">', unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

                # AI 메시지 종료
                st.markdown("</div>", unsafe_allow_html=True)
                st.markdown('<hr class="message-divider">', unsafe_allow_html=True)

    # 🔥 스트리밍 처리 - 채팅 메시지 영역 내부에서 실행
    if st.session_state.get("is_loading", False):
        _process_streaming_response()

    st.markdown("</div>", unsafe_allow_html=True)

    # 추천 질문 (대화가 없을 때만 표시)
    if not st.session_state.get("messages"):
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

    # 입력창
    with st.form(key="chat_input_form", clear_on_submit=True):
        col_input, col_send = st.columns([9, 1])
        with col_input:
            user_input = st.text_input(
                "정책에 대해 질문해주세요...",
                key="user_input",
                label_visibility="collapsed",
                placeholder="메시지를 입력하세요...",
            )
        with col_send:
            submitted = st.form_submit_button("✈️", use_container_width=True)

        if submitted and user_input.strip():
            handle_send_message(user_input)

    render_template("components/disclaimer.html")

    # --- 대화 저장 및 초기화 UI ---
    st.markdown("---")
    if st.session_state.save_chat_confirmation:
        st.warning(
            "현재 대화 내용을 저장하시겠습니까? 저장하지 않은 대화는 사라집니다."
        )
        col1, col2, col3 = st.columns([1.5, 1.5, 1])
        with col1:
            if st.button("💾 저장하고 초기화", use_container_width=True):
                token = _get_auth_token()
                if token:
                    # 🔥 user_info에서 profile_id 가져오기
                    user_info = st.session_state.get("user_info")
                    profile_id = user_info.get("main_profile_id") if user_info else None

                    # 백엔드에 reset_save 액션 전송 (DB 저장 트리거)
                    try:
                        response = backend_service.send_chat_message(
                            session_id=st.session_state.get("session_id"),
                            token=token,
                            user_input="",  # 빈 메시지
                            user_action="reset_save",  # 저장 후 초기화
                            profile_id=profile_id,
                        )
                        st.toast("✅ 대화 내용이 저장되었습니다.")
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {e}")

                st.session_state.messages = []
                st.session_state.session_id = None  # 세션 ID 초기화
                st.session_state.save_chat_confirmation = False
                st.rerun()
        with col2:
            if st.button("🗑️ 저장하지 않고 초기화", use_container_width=True):
                # 백엔드에 reset_drop 액션 전송 (저장 없이 초기화)
                token = _get_auth_token()
                if token:
                    user_info = st.session_state.get("user_info")
                    profile_id = user_info.get("main_profile_id") if user_info else None

                    try:
                        backend_service.send_chat_message(
                            session_id=st.session_state.get("session_id"),
                            token=token,
                            user_input="",
                            user_action="reset_drop",  # 저장 없이 초기화
                            profile_id=profile_id,
                        )
                    except Exception:
                        pass  # 에러 무시 (어차피 초기화)

                st.session_state.messages = []
                st.session_state.session_id = None  # 세션 ID 초기화
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
                    # 🔥 user_info에서 profile_id 가져오기
                    user_info = st.session_state.get("user_info")
                    profile_id = user_info.get("main_profile_id") if user_info else None

                    # 백엔드에 save 액션 전송 (대화 유지하며 DB 저장)
                    try:
                        response = backend_service.send_chat_message(
                            session_id=st.session_state.get("session_id"),
                            token=token,
                            user_input="",  # 빈 메시지
                            user_action="save",  # 저장만
                            profile_id=profile_id,
                        )
                        st.toast("✅ 대화 내용이 저장되었습니다.")
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {e}")
                else:
                    st.warning("로그인이 필요합니다.")

        with col_reset:
            if st.button("🔄 초기화", use_container_width=True):
                if len(st.session_state.get("messages", [])) > 1:
                    st.session_state.save_chat_confirmation = True
                    st.rerun()
                else:
                    st.toast("초기화할 대화 내용이 없습니다.")
