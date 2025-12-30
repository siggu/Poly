"""의료 혜택 정보 제공 에이전트 챗봇 메인 애플리케이션 파일 11.13 수정"""

import streamlit as st

# import requests
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
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

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


def apply_font_size_css():
    """글자 크기 설정에 따라 동적 CSS를 적용합니다."""
    font_size = st.session_state.get("font_size", "medium")

    # 글자 크기별 CSS 변수 설정
    font_sizes = {
        "small": {
            "base": "13px",
            "chat": "13px",
            "title": "24px",
            "subtitle": "14px",
        },
        "medium": {
            "base": "15px",
            "chat": "15px",
            "title": "28px",
            "subtitle": "16px",
        },
        "large": {
            "base": "17px",
            "chat": "17px",
            "title": "32px",
            "subtitle": "18px",
        },
    }

    sizes = font_sizes.get(font_size, font_sizes["medium"])

    st.markdown(
        f"""
        <style>
        /* 글자 크기 설정: {font_size} */
        :root {{
            --font-size-base: {sizes["base"]};
            --font-size-chat: {sizes["chat"]};
            --font-size-title: {sizes["title"]};
            --font-size-subtitle: {sizes["subtitle"]};
        }}

        /* 전체 기본 폰트 크기 */
        .stApp, .main, [data-testid="stAppViewContainer"] {{
            font-size: {sizes["base"]} !important;
        }}

        /* 채팅 메시지 */
        .chat-bubble-user p,
        .chat-bubble-assistant p {{
            font-size: {sizes["chat"]} !important;
        }}

        /* 제목 */
        .chat-title-section h1 {{
            font-size: {sizes["title"]} !important;
        }}

        .chat-title-section p {{
            font-size: {sizes["subtitle"]} !important;
        }}

        /* 입력 필드 */
        .stTextInput input {{
            font-size: {sizes["base"]} !important;
        }}

        /* 버튼 */
        .stButton button {{
            font-size: {sizes["base"]} !important;
        }}

        /* 사이드바 */
        [data-testid="stSidebar"] {{
            font-size: {sizes["base"]} !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ==============================================================================
# 1. 상태 초기화 (st.session_state)
# ==============================================================================

initialize_session_state()
initialize_auth_state()

if "profiles" not in st.session_state:
    st.session_state.profiles = []

# 마이페이지 / 설정 모달 관련 상태
if "isAddingProfile" not in st.session_state:
    st.session_state.isAddingProfile = False
if "editingProfileId" not in st.session_state:
    st.session_state.editingProfileId = None
if "newProfile" not in st.session_state:
    st.session_state.newProfile = {}
if "editingData" not in st.session_state:
    st.session_state.editingData = {}

# 사이드바/챗봇 관련 상태
if "search_query" not in st.session_state:
    st.session_state.search_query = ""

# --- ⭐ 프로필 전환 리팩토링: 표준 세션 키 초기화 ---
if "current_profile_id" not in st.session_state:
    st.session_state.current_profile_id = None
# ---

if "sidebar_search_input" not in st.session_state:
    st.session_state.sidebar_search_input = ""


# ==============================================================================
# 2. 유틸리티 및 핸들러 함수
# ==============================================================================


def handle_logout():
    st.info("👋 로그아웃되었습니다.")
    st.session_state.settings_modal_open = False


def handle_search_update():
    st.session_state.search_query = st.session_state.sidebar_search_input


def handle_settings_click():
    st.session_state.settings_modal_open = True


# ==============================================================================
# 3. 컴포넌트 렌더링 함수
# ==============================================================================


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
                if on_action_click
                else lambda: st.info(f"액션 실행: {config['action']}")
            ),
        )


# ==============================================================================
# 4. 메인 앱 실행 로직 (Application Flow)
# ==============================================================================

# --- ⭐ 프로필 전환 리팩토링: `current_profile_id` 기준으로 로드 ---
def load_user_profiles_from_backend(token: str) -> bool:
    """백엔드에서 사용자 정보와 모든 프로필을 로드하고 `current_profile_id`를 설정합니다."""
    import logging

    logger = logging.getLogger(__name__)

    try:
        # 1. 사용자 기본 정보 조회
        ok, user_info = backend_service.get_user_profile(token)
        if not ok:
            # 토큰 만료 시 자동 로그아웃
            if user_info == "TOKEN_EXPIRED":
                logger.warning("⚠️ 토큰이 만료되었습니다. 자동 로그아웃합니다.")
                st.session_state["is_logged_in"] = False
                st.session_state["auth_token"] = None
                st.session_state["user_info"] = None
                st.session_state["profiles"] = []
                st.warning("세션이 만료되었습니다. 다시 로그인해주세요.")
                st.rerun()
            logger.error(f"❌ 사용자 정보 조회 실패: {user_info}")
            return False
        st.session_state["user_info"] = user_info
        logger.info(f"✅ 사용자 정보 로드: {user_info.get('id')}")

        # 2. 모든 프로필 목록 조회
        ok_profiles, all_profiles = backend_service.get_all_profiles(token)
        if ok_profiles and all_profiles:
            st.session_state["profiles"] = all_profiles
            logger.info(f"✅ 프로필 {len(all_profiles)}개 로드 완료")

            # 3. `current_profile_id` 설정 (가장 중요)
            main_profile_id = user_info.get("main_profile_id")
            if main_profile_id:
                st.session_state["current_profile_id"] = int(main_profile_id)
                logger.info(f"✅ 현재 프로필 ID 설정: {main_profile_id}")
            # 메인 프로필이 지정 안된 경우, 첫번째 프로필을 기본값으로 설정
            elif all_profiles:
                first_profile_id = all_profiles[0].get("id")
                st.session_state["current_profile_id"] = int(first_profile_id)
                logger.warning(
                    f"⚠️ main_profile_id가 없어 첫 프로필({first_profile_id})을 활성화합니다."
                )
            else:
                st.session_state["current_profile_id"] = None
        else:
            logger.warning("⚠️ 프로필이 비어있습니다. 빈 리스트로 초기화합니다.")
            st.session_state["profiles"] = []
            st.session_state["current_profile_id"] = None
        return True

    except Exception as e:
        logger.error(f"❌ 프로필 로드 중 오류 발생: {e}")
        st.session_state["profiles"] = []
        st.session_state["current_profile_id"] = None
        return True


# ---


# 11.17 수정: 메인 앱 함수
def main_app():
    """메인 애플리케이션 함수"""
    import logging

    logger = logging.getLogger(__name__)

    # 글자 크기 CSS 적용
    apply_font_size_css()

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

    # ✅ 수정: 저장된 세션 복원 로직 개선
    if not st.session_state.get("is_logged_in", False):
        saved_session = load_session()
        if saved_session and saved_session.get("is_logged_in"):
            saved_token = saved_session.get("auth_token")

            # ✅ 토큰이 있는지 확인
            if saved_token:
                logger.info(f"✅ 저장된 세션에서 토큰 복원: {saved_token[:20]}...")
                st.session_state["is_logged_in"] = True
                st.session_state["auth_token"] = saved_token

                # ✅ 프로필 로드 (실패해도 로그인 상태는 유지)
                try:
                    load_user_profiles_from_backend(saved_token)
                except Exception as e:
                    logger.warning(f"⚠️ 프로필 로드 실패: {e}")
                    st.session_state["profiles"] = []
            else:
                logger.warning("⚠️ 저장된 세션에 토큰이 없습니다.")

    # ✅ 로그인 상태이고 프로필이 비어있으면 다시 로드
    if st.session_state.get("is_logged_in", False):
        token = st.session_state.get("auth_token")

        # ✅ 토큰 존재 여부 로깅
        if not token:
            logger.error("❌ 로그인 상태인데 토큰이 없습니다!")
            logger.error(f"세션 키: {list(st.session_state.keys())}")
        else:
            logger.info(f"✅ 토큰 확인됨: {token[:20]}...")

        # ✅ 프로필이 비어있으면 다시 로드
        if (
            not st.session_state.get("profiles")
            or len(st.session_state["profiles"]) == 0
        ):
            if token:
                logger.info("프로필이 비어있어 다시 로드합니다...")
                load_user_profiles_from_backend(token)
            else:
                logger.error("토큰이 없어 프로필을 로드할 수 없습니다.")

    # 로그인 상태 확인
    if not st.session_state.get("is_logged_in", False):
        # 비로그인 상태: 첫 화면에 로그인/회원가입 모두 표시
        render_landing_page()
    else:
        # 로그인 상태: 사이드바 렌더링
        render_sidebar()

        # 설정 모달과 마이페이지 모달은 동시에 열리지 않도록 처리
        if st.session_state.get("settings_modal_open", False):
            st.session_state["show_profile"] = False
            render_settings_modal()
        elif st.session_state.get("show_profile", False):
            st.session_state["settings_modal_open"] = False
            render_my_page_modal()
        else:
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
