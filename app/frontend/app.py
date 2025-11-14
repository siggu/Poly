"""의료 혜택 정보 제공 에이전트 챗봇 메인 애플리케이션 파일 11.13 수정"""

import streamlit as st
import requests
from src.state_manger import initialize_session_state
from src.pages.login import (
    initialize_auth_state,
    render_login_tab,
    render_signup_tab,
)
from src.widgets.sidebar import render_sidebar
from src.utils.template_loader import render_template, load_css
from src.utils.session_manager import load_session
from src.backend_service import backend_service

from src.pages.chat import render_chatbot_main
from src.pages.my_page import render_my_page_modal
from src.pages.settings import render_settings_modal

from dotenv import load_dotenv

load_dotenv()


# ==============================================================================
# 0. 전역 설정 및 CSS 주입
# ==============================================================================

st.set_page_config(
    page_title="의료 혜택 정보 제공 에이전트 챗봇",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS 스타일 주입
load_css("custom.css")


# ==============================================================================
# 1. 상태 초기화 (st.session_state)
# ==============================================================================

initialize_session_state()
initialize_auth_state()

if "profiles" not in st.session_state:
    st.session_state.profiles = []
# ================세션 초기화 기본 문법=============================
# # Initialization
# if 'key' not in st.session_state:
#     st.session_state['key'] = 'value'

# # Session State also supports attribute based syntax
# if 'key' not in st.session_state:
#     st.session_state.key = 'value'
# =============================================================


# 마이페이지 / 설정 모달 관련 상태
if "isAddingProfile" not in st.session_state:  # 프로필 추가 열렸는지 확인
    st.session_state.isAddingProfile = False
if "editingProfileId" not in st.session_state:  # 프로필 수정 상태 초기화
    st.session_state.editingProfileId = None
if "newProfile" not in st.session_state:  # 프로필 추가 세션
    st.session_state.newProfile = {}
if "editingData" not in st.session_state:
    st.session_state.editingData = {}

# 사이드바/챗봇 관련 상태
# 대화 내용 검색 필드의 초기값 설정
if "search_query" not in st.session_state:
    st.session_state.search_query = ""


# 사이드바 검색 입력 필드의 초기값 설정.
if "sidebar_search_input" not in st.session_state:
    st.session_state.sidebar_search_input = ""

# ==============================================================================
# 2. 유틸리티 및 핸들러 함수
# ==============================================================================


def handle_logout():
    st.info("👋 로그아웃되었습니다.")
    st.session_state.settings_modal_open = False


# --- Sidebar 핸들러 ---
def handle_search_update():
    st.session_state.search_query = st.session_state.sidebar_search_input


def handle_settings_click():
    st.session_state.settings_modal_open = True


# ==============================================================================
# 3. 컴포넌트 렌더링 함수
# ==============================================================================


# --- A. ErrorMessage 컴포넌트 ---
def render_error_message(error_type: str, message: str, on_action_click=None):
    def get_error_config(type_key):
        if type_key == "no-policy":
            return {
                "title": "정책을 찾을 수 없습니다",
                "action": "다른 정책 검색해보기",
            }
        elif type_key == "llm-error":
            return {"title": "서버 연결 오류", "action": "다시 시도"}
        elif type_key == "inappropriate":
            return {"title": "부적절한 내용", "action": None}
        elif type_key == "unclear":
            return {
                "title": "질문이 명확하지 않습니다",
                "action": "구체적으로 질문하기",
            }
        else:
            return {"title": "오류 발생", "action": "다시 시도"}

    config = get_error_config(error_type)

    st.error(f"**{config['title']}**")
    st.markdown(
        f"<p style='font-size: 14px; color: gray; margin-top: -15px;'>{message}</p>",
        unsafe_allow_html=True,
    )

    if config["action"]:
        st.button(
            f"🔄 {config['action']}",
            key=f"error_action_{error_type}",
            on_click=(
                on_action_click
                # if on_action_click
                # else lambda: st.toast(f"액션 실행: {config['action']}")
                if on_action_click
                else lambda: st.info(f"액션 실행: {config['action']}")
            ),
        )


# ==============================================================================
# 4. 메인 앱 실행 로직 (Application Flow)
# ==============================================================================

# 추천 질문 목록
SUGGESTED_QUESTIONS = [
    "청년 주거 지원 정책이 궁금해요",
    "취업 지원 프로그램 알려주세요",
    "창업 지원금 신청 방법은?",
    "육아 지원 혜택 찾아주세요",
]


def main_app():
    # 사이드바 네비게이션 숨기기
    st.markdown(
        """
        <style>
            [data-testid="stSidebarNav"] {display: none !important;}
            .main-content {
                max-width: 100%;
                padding: 20px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # 저장된 세션이 있으면 복원
    if not st.session_state.get("is_logged_in", False):
        saved_session = load_session()
        if saved_session and saved_session.get("is_logged_in"):
            st.session_state["is_logged_in"] = True
            st.session_state["auth_token"] = saved_session.get("auth_token")

            if st.session_state["auth_token"]:
                # 토큰으로 프로필 정보 복원
                ok, user_info = backend_service.get_user_profile(
                    st.session_state["auth_token"]
                )
                if ok:
                    st.session_state["user_info"] = user_info
                    # TODO: 다중 프로필을 지원하는 경우, 여기서 모든 프로필을 가져오는 API를 호출해야 합니다.
                    # 현재는 기본 프로필만 가져옵니다.
                    st.session_state["profiles"] = [user_info.get("profile")]
                else:
                    # 토큰이 만료되었거나 유효하지 않은 경우
                    st.session_state["is_logged_in"] = False
                    st.session_state["auth_token"] = None

    # 로그인 상태 확인
    if not st.session_state.get("is_logged_in", False):
        # 비로그인 상태: 첫 화면에 로그인/회원가입 모두 표시
        render_landing_page()
    else:
        # 로그인 상태: 사이드바 렌더링
        render_sidebar()
        # 설정 모달과 마이페이지 모달은 동시에 열리지 않도록 처리
        if st.session_state.get("settings_modal_open", False):
            # 설정 모달이 열려있으면 마이페이지 닫기
            st.session_state["show_profile"] = False
            render_settings_modal()
        elif st.session_state.get("show_profile", False):
            # 마이페이지가 열려있으면 설정 모달 닫기
            st.session_state["settings_modal_open"] = False
            render_my_page_modal()
        else:
            # 메인 챗봇 화면 (모달이 열려있지 않을 때만)
            render_chatbot_main()


def render_landing_page():
    """첫 화면: 로그인/회원가입 모두 표시"""
    # CSS 로드
    load_css("components/landing_page.css")

    # 랜딩 페이지 HTML 렌더링
    render_template("landing_page.html")

    # 로그인/회원가입 탭
    login_tab, signup_tab = st.tabs(["로그인", "회원가입"])

    with login_tab:
        render_login_tab()

    with signup_tab:
        render_signup_tab()


if __name__ == "__main__":
    from src.pages.settings import initialize_settings_state

    # 상태 초기화는 앱 실행 초기에 한 번만 수행합니다.
    if "settings_initialized" not in st.session_state:
        initialize_settings_state()
        st.session_state.settings_initialized = True
    main_app()
