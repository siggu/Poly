"""앱 전역 상태 관리를 위한 session_state 초기화 및 관리 함수들 - 프로필 관리 추가 11.18"""

import streamlit as st
import uuid
import time


def initialize_session_state():
    """
    앱 전역에서 필요한 기본 session_state 키들을 안전하게 초기화
    """
    defaults = {
        "is_logged_in": False,
        "current_profile": {},  # {'id': ..., 'name': ...}
        "show_login_modal": False,
        "messages": [
            {
                "id": str(uuid.uuid4()),
                "role": "assistant",
                "content": """ 안녕하세요! 정책 추천 챗봇입니다. 
            나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.""",
                "timestamp": time.time(),
            }
        ],
        "input": "",
        "is_loading": False,
        "search_query": "",
        "sidebar_search_input": "",
        "save_chat_confirmation": False,  # 대화 저장 확인 UI
        # ========== 프로필 관리 관련 상태 추가 ==========
        "profiles": [],  # 전체 프로필 목록
        "isAddingProfile": False,  # 프로필 추가 중 여부
        "newProfile": {},  # 새 프로필 임시 데이터
        "editingProfileId": None,  # 현재 편집 중인 프로필 ID
        "editingData": {},  # 편집 중인 프로필 데이터
        # ========== 리다이렉션 관련 상태 추가 ==========
        "redirect_to": None,  # 이동할 페이지 (예: "my_page")
        "redirect_action": None,  # 수행할 액션 (예: "add_profile", "edit_profile")
        "redirect_profile_id": None,  # 편집할 프로필 ID (edit 액션용)
        # ========== 계정 관련 상태 추가 ==========
        "show_password_reset": False,  # 비밀번호 재설정 폼 표시 여부
        "show_delete_confirm": False,  # 회원 탈퇴 확인 표시 여부
        "password_data": {
            "current": "",
            "new": "",
            "confirm": "",
        },  # 비밀번호 입력 데이터
        "password_error": "",  # 비밀번호 에러 메시지
        # ========== 마이페이지 모달 관련 ==========
        "show_profile": False,  # 마이페이지 모달 표시 여부
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def reset_chat_session():
    """채팅 세션을 재설정"""
    st.session_state["messages"] = [
        {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": """ 안녕하세요! 정책 추천 챗봇입니다. 
            나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.""",
            "timestamp": time.time(),
        }
    ]
    st.session_state["input"] = ""
    st.session_state["is_loading"] = False


def set_redirect(page: str, action: str = None, profile_id: int = None):
    """
    페이지 리다이렉션을 설정합니다.

    Args:
        page (str): 이동할 페이지 (예: "my_page", "chat", "settings")
        action (str, optional): 수행할 액션 (예: "add_profile", "edit_profile")
        profile_id (int, optional): 편집할 프로필 ID (edit_profile 액션용)

    Examples:
        # 마이페이지로 이동하여 프로필 추가
        set_redirect("my_page", "add_profile")

        # 마이페이지로 이동하여 프로필 편집
        set_redirect("my_page", "edit_profile", profile_id=123)

        # 단순 페이지 이동
        set_redirect("chat")
    """
    st.session_state["redirect_to"] = page
    st.session_state["redirect_action"] = action
    st.session_state["redirect_profile_id"] = profile_id


def clear_redirect():
    """리다이렉션 상태를 초기화합니다."""
    st.session_state["redirect_to"] = None
    st.session_state["redirect_action"] = None
    st.session_state["redirect_profile_id"] = None


def get_redirect_info():
    """
    현재 리다이렉션 정보를 반환합니다.

    Returns:
        tuple: (page, action, profile_id)
    """
    return (
        st.session_state.get("redirect_to"),
        st.session_state.get("redirect_action"),
        st.session_state.get("redirect_profile_id"),
    )


def reset_profile_states():
    """프로필 관련 상태를 초기화합니다."""
    st.session_state["isAddingProfile"] = False
    st.session_state["newProfile"] = {}
    st.session_state["editingProfileId"] = None
    st.session_state["editingData"] = {}


def reset_account_states():
    """계정 관련 상태를 초기화합니다."""
    st.session_state["show_password_reset"] = False
    st.session_state["show_delete_confirm"] = False
    st.session_state["password_data"] = {"current": "", "new": "", "confirm": ""}
    st.session_state["password_error"] = ""
