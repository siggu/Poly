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


def _get_auth_token() -> Optional[str]:
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def handle_font_size_change(size):
    st.session_state.font_size = size
    st.success(f"글자 크기가 '{size}로 설정되었습니다.")


def update_notification(key):
    st.session_state.notifications[key] = st.session_state[f"switch_{key}"]


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
