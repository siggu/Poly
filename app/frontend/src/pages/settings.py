"""설정 페이지 관련 함수들 11.13 수정"""
import uuid
import time
import streamlit as st
from typing import Optional
from ..backend_service import backend_service
from ..utils.session_manager import clear_session


# 설정 setting 초기화
def initialize_settings_state():
    if "settings_modal_open" not in st.session_state:
        st.session_state.settings_modal_open = False
    if "font_size" not in st.session_state:
        st.session_state.font_size = "medium"
    if "notifications" not in st.session_state:
        st.session_state.notifications = {
            "newPolicy": True,
            "deadline": True,
            "updates": False,
        }
    if "show_delete_confirm" not in st.session_state:
        st.session_state.show_delete_confirm = False
    if "show_password_reset" not in st.session_state:
        st.session_state.show_password_reset = False
    if "password_data" not in st.session_state:
        st.session_state.password_data = {"current": "", "new": "", "confirm": ""}
    if "password_error" not in st.session_state:
        st.session_state.password_error = ""


def _get_auth_token() -> Optional[str]:
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def handle_font_size_change(size):
    st.session_state.font_size = size
    st.success(f"글자 크기가 '{size}로 설정되었습니다.")


def update_notification(key):
    st.session_state.notifications[key] = st.session_state[f"switch_{key}"]


def handle_password_reset():
    data = st.session_state.password_data
    if not data["current"] or not data["new"] or not data["confirm"]:
        st.session_state.password_error = "모든 필드를 입력해주세요."
        return
    if data["new"] != data["confirm"]:
        st.session_state.password_error = "새 비밀번호가 일치하지 않습니다."
        return
    if len(data["new"]) < 8:
        st.session_state.password_error = "비밀번호는 8자 이상이어야 합니다."
        return

    token = _get_auth_token()
    if not token:
        st.session_state.password_error = "로그인 정보를 찾을 수 없습니다."
        return

    success, message = backend_service.reset_password(token, data["current"], data["new"])

    if success:
        st.success(f"🔒 {message}")
        st.session_state.show_password_reset = False
        st.session_state.password_data = {"current": "", "new": "", "confirm": ""}
        st.session_state.password_error = ""
    else:
        st.session_state.password_error = message


def reset_password_form():
    st.session_state.show_password_reset = False
    st.session_state.password_data = {"current": "", "new": "", "confirm": ""}
    st.session_state.password_error = ""


def toggle_delete_confirm(value):
    st.session_state.show_delete_confirm = value


def handle_account_delete():
    token = _get_auth_token()
    if not token:
        st.error("계정 정보를 찾을 수 없습니다.")
        st.stop()  # 추가: 오류 발생 시 실행 중단
        return

    success, message = backend_service.delete_user_account(token)
    if success:
        st.success(f"🗑️ {message}")
        st.session_state.settings_modal_open = False
        st.session_state["is_logged_in"] = False
        try:
            clear_session()
        except Exception:
            pass
        st.session_state["user_info"] = {}
        st.session_state["profiles"] = []
        st.session_state["messages"] = [
            {
                "id": str(uuid.uuid4()),
                "role": "assistant",
                "content": "안녕하세요! 정책 추천 챗봇입니다. 나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.",
                "timestamp": time.time(),
            }
        ]
        st.rerun()
    else:
        st.error(f"🗑️ {message}")


def render_settings_modal():
    """설정 모달 렌더링"""
    st.markdown(
        """
        <style>
        .settings-modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    col_title, col_close = st.columns([9, 1])
    with col_title:
        st.markdown("### ⚙️ 설정")
        st.caption("계정 및 알림 설정을 관리합니다.")
    with col_close:
        if st.button("✕", key="btn_close_settings"):
            st.session_state.settings_modal_open = False
            st.rerun()

    st.markdown("---")

    st.markdown("#### 글자 크기 조절")
    col_small, col_medium, col_large = st.columns(3)
    with col_small:
        is_small = st.session_state.font_size == "small"
        if st.button(
            "작게",
            key="font_small",
            type="primary" if is_small else "secondary",
            use_container_width=True,
            on_click=handle_font_size_change,
            args=("small",),
        ):
            pass
    with col_medium:
        is_medium = st.session_state.font_size == "medium"
        if st.button(
            "보통",
            key="font_medium",
            type="primary" if is_medium else "secondary",
            use_container_width=True,
            on_click=handle_font_size_change,
            args=("medium",),
        ):
            pass
    with col_large:
        is_large = st.session_state.font_size == "large"
        if st.button(
            "크게",
            key="font_large",
            type="primary" if is_large else "secondary",
            use_container_width=True,
            on_click=handle_font_size_change,
            args=("large",),
        ):
            pass

    st.markdown("---")

    st.markdown("#### 알림 수신 설정")
    st.checkbox(
        "신규 정책 알림",
        value=st.session_state.notifications.get("newPolicy", True),
        key="switch_newPolicy",
        on_change=lambda: update_notification("newPolicy"),
    )
    st.checkbox(
        "마감 임박 알림",
        value=st.session_state.notifications.get("deadline", True),
        key="switch_deadline",
        on_change=lambda: update_notification("deadline"),
    )
    st.checkbox(
        "정책 업데이트 알림",
        value=st.session_state.notifications.get("updates", False),
        key="switch_updates",
        on_change=lambda: update_notification("updates"),
    )

    st.markdown("---")

    st.markdown("#### 비밀번호 재설정")
    if not st.session_state.show_password_reset:
        st.text_input(
            "비밀번호 변경",
            key="password_change_input",
            placeholder="비밀번호 변경",
            disabled=True,
        )
    else:
        with st.form(key="password_reset_form"):
            st.text_input("현재 비밀번호 *", type="password", key="current-password")
            st.text_input("새 비밀번호 *", type="password", key="new-password")
            st.text_input("새 비밀번호 확인 *", type="password", key="confirm-password")
            st.session_state.password_data["current"] = st.session_state.get(
                "current-password", ""
            )
            st.session_state.password_data["new"] = st.session_state.get(
                "new-password", ""
            )
            st.session_state.password_data["confirm"] = st.session_state.get(
                "confirm-password", ""
            )

            if st.session_state.get("password_error"):
                st.error(f"⚠️ {st.session_state.password_error}")

            col_submit, col_cancel = st.columns(2)
            with col_submit:
                if st.form_submit_button("변경하기", use_container_width=True):
                    handle_password_reset()
            with col_cancel:
                if st.form_submit_button(
                    "취소", on_click=reset_password_form, use_container_width=True
                ):
                    pass

    st.markdown("---")

    st.markdown("#### 회원 탈퇴")
    if not st.session_state.show_delete_confirm:
        if st.button(
            "회원 탈퇴",
            key="delete_button_initial",
            on_click=toggle_delete_confirm,
            args=(True,),
            use_container_width=True,
            type="primary",
        ):
            pass
    else:
        st.warning(
            "⚠️ 회원 탈퇴 시 모든 데이터가 삭제되며 복구할 수 없습니다. 정말로 탈퇴하시겠습니까?"
        )
        col_delete, col_cancel_delete = st.columns(2)
        with col_delete:
            if st.button("탈퇴하기", key="delete_button_confirm", use_container_width=True):
                handle_account_delete()

        with col_cancel_delete:
            if st.button("취소", key="delete_button_cancel", use_container_width=True):
                toggle_delete_confirm(False)
                st.rerun()
