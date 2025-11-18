"""11. 18사이드바 위젯 - 프로필 관리 중심으로 구성"""

import streamlit as st
from src.utils.template_loader import render_template, load_css
from src.backend_service import backend_service
from src.state_manger import set_redirect
from typing import Optional
from datetime import date


# --- 1. 상태 초기화 ---
if "profiles" not in st.session_state:
    st.session_state.profiles = []
if "settings_modal_open" not in st.session_state:
    st.session_state.settings_modal_open = False


# --- 2. 헬퍼 함수 ---
def _get_auth_token() -> Optional[str]:
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def _get_profile_id(profile):
    """프로필 ID를 안전하게 추출합니다."""
    profile_id = profile.get("id") or profile.get("user_id")
    if profile_id is None:
        return None
    return int(profile_id)


def _get_user_main_profile_id() -> Optional[int]:
    """세션 상태에서 사용자의 main_profile_id를 조회합니다."""
    user_info = st.session_state.get("user_info", {})
    if isinstance(user_info, dict):
        main_id = user_info.get("main_profile_id")
        if main_id is not None:
            return int(main_id)
    return None


def _refresh_profiles_from_db():
    """DB에서 최신 프로필 목록을 가져와 세션 상태를 업데이트합니다."""
    token = _get_auth_token()
    if not token:
        return False

    ok, profiles_list = backend_service.get_all_profiles(token)

    # 401 오류 시 자동 로그아웃 처리 ⭐
    if not ok:
        error_msg = str(profiles_list).lower()
        if "401" in error_msg or "unauthorized" in error_msg:
            # 세션 정보 초기화
            st.session_state["is_logged_in"] = False
            st.session_state["auth_token"] = None
            st.session_state["profiles"] = []
            st.session_state["user_info"] = {}
            st.warning("세션이 만료되었습니다. 다시 로그인해주세요.")
        return False

    if not profiles_list:
        st.session_state.profiles = []
        return False

    main_profile_id = _get_user_main_profile_id()

    for p in profiles_list:
        p_id = _get_profile_id(p)
        if p_id is not None:
            p["isActive"] = p_id == main_profile_id
        else:
            p["isActive"] = False

    st.session_state.profiles = profiles_list
    return True


def calculate_age(birth_date):
    """생년월일로부터 나이를 계산합니다."""
    if isinstance(birth_date, date):
        bd = birth_date
    elif isinstance(birth_date, str):
        try:
            bd = date.fromisoformat(birth_date)
        except Exception:
            return None
    else:
        return None

    today = date.today()
    years = today.year - bd.year
    if (today.month, today.day) < (bd.month, bd.day):
        years -= 1
    return years


# --- 3. 핸들러 함수 ---
def handle_add_profile_click():
    """프로필 추가 버튼 클릭 시 마이페이지로 리다이렉션"""
    set_redirect("my_page", "add_profile")
    st.session_state["show_profile"] = True
    st.rerun()


def handle_edit_profile_click(profile_id: int):
    """프로필 수정 버튼 클릭 시 마이페이지로 리다이렉션"""
    set_redirect("my_page", "edit_profile", profile_id)
    st.session_state["show_profile"] = True
    st.rerun()


def handle_profile_select(profile_id: int):
    """프로필 선택 버튼 클릭 시 활성 프로필 변경"""
    if profile_id is None:
        st.error("프로필 ID가 제공되지 않았습니다.")
        return

    token = _get_auth_token()
    if token:
        success, message = backend_service.set_main_profile(token, profile_id)
        if success:
            st.success("활성 프로필이 변경되었습니다.")
            _refresh_profiles_from_db()
            st.rerun()
        else:
            st.error(f"활성 프로필 변경 실패: {message}")


def handle_profile_delete(profile_id: int):
    """프로필 삭제 버튼 클릭 시 삭제 처리"""
    if profile_id is None:
        st.error("삭제할 프로필 ID가 없습니다.")
        return

    if len(st.session_state.profiles) <= 1:
        st.warning("최소한 하나의 프로필은 남겨야 합니다.")
        return

    token = _get_auth_token()
    if token:
        success, message = backend_service.delete_profile(token, profile_id)
        if success:
            st.success("프로필이 삭제되었습니다.")

            # 삭제된 프로필이 활성 프로필인지 확인
            is_active_deleted = any(
                _get_profile_id(p) == profile_id and p.get("isActive")
                for p in st.session_state.profiles
            )

            # 프로필 목록에서 제거
            st.session_state.profiles = [
                p for p in st.session_state.profiles if _get_profile_id(p) != profile_id
            ]

            # 활성 프로필이 삭제된 경우 첫 번째 프로필을 활성화
            if is_active_deleted and st.session_state.profiles:
                new_active_profile_id = _get_profile_id(st.session_state.profiles[0])
                if new_active_profile_id is not None:
                    backend_service.set_main_profile(token, new_active_profile_id)

            _refresh_profiles_from_db()
            st.rerun()
        else:
            st.error(f"프로필 삭제 실패: {message}")


def handle_settings_click():
    """설정 버튼 클릭 시 설정 모달 열기"""
    st.session_state.settings_modal_open = True
    st.rerun()


# --- 4. 사이드바 렌더링 ---
def render_sidebar():
    """좌측 사이드바 렌더링 - 프로필 관리 중심"""
    # CSS 로드
    load_css("components/sidebar.css")

    with st.sidebar:
        # 로고
        render_template("components/sidebar_logo.html")

        st.markdown("---")

        # ========== 프로필 관리 섹션 ==========
        st.markdown("### 프로필 관리")

        # 로그인 여부 확인 (강화) ⭐
        if not st.session_state.get("is_logged_in", False):
            st.info("로그인 후 프로필을 관리할 수 있습니다.")
            # 설정 버튼은 하단에 표시
            st.markdown("---")
            if st.button(
                "⚙️ 설정", key="sidebar_settings_logged_out", use_container_width=True
            ):
                handle_settings_click()
            return  # 여기서 종료 ⭐

        # 토큰 확인 추가 ⭐
        token = _get_auth_token()
        if not token:
            st.warning("인증 토큰이 없습니다. 다시 로그인해주세요.")
            st.caption("세션이 만료되었을 수 있습니다.")
            # 로그인 상태를 False로 변경
            st.session_state["is_logged_in"] = False
            st.markdown("---")
            if st.button(
                "⚙️ 설정", key="sidebar_settings_no_token", use_container_width=True
            ):
                handle_settings_click()
            return  # 여기서 종료 ⭐

        # 프로필 목록 새로고침
        if not st.session_state.get("profiles"):
            success = _refresh_profiles_from_db()
            if not success:
                st.error("프로필을 불러오는데 실패했습니다.")
                st.caption("로그인 상태를 확인해주세요.")
                st.markdown("---")
                if st.button(
                    "⚙️ 설정", key="sidebar_settings_load_fail", use_container_width=True
                ):
                    handle_settings_click()
                return  # 여기서 종료 ⭐

        # 프로필 추가 버튼
        if st.button(
            "➕ 프로필 추가", key="sidebar_add_profile", use_container_width=True
        ):
            handle_add_profile_click()

        st.markdown("")

        # 활성 프로필 표시
        active_profile = next(
            (p for p in st.session_state.profiles if p.get("isActive", False)), None
        )

        if active_profile:
            st.markdown("#### 기본 프로필")

            with st.container():
                col_info, col_edit = st.columns([8, 2])

                with col_info:
                    st.markdown("**활성** ✓")

                    # 이름
                    name = active_profile.get("name", "미입력")
                    st.write(f"**이름:** {name}")

                    # 생년월일 (나이로 표시)
                    birth_date = active_profile.get("birthDate")
                    age = calculate_age(birth_date)
                    birth_display = f"{age}세" if isinstance(age, int) else "미입력"
                    st.write(f"**생년월일:** {birth_display}")

                    # 거주지
                    location = active_profile.get("location", "미입력")
                    st.write(f"**거주지:** {location}")

                with col_edit:
                    profile_id = _get_profile_id(active_profile)
                    if profile_id is not None:
                        if st.button("✏️", key=f"sidebar_edit_{profile_id}"):
                            handle_edit_profile_click(profile_id)

            st.markdown("---")

        # 등록된 프로필 목록
        st.markdown("#### 등록된 프로필")

        if not st.session_state.profiles:
            st.caption("등록된 프로필이 없습니다.")
        else:
            for profile in st.session_state.profiles:
                profile_id = _get_profile_id(profile)
                if profile_id is None:
                    continue

                # 활성 프로필은 위에 이미 표시했으므로 스킵
                if profile.get("isActive", False):
                    continue

                with st.container():
                    cols = st.columns([6, 2, 2])

                    with cols[0]:
                        name = profile.get("name", "무명")
                        location = profile.get("location", "미입력")
                        st.write(f"**{name}** ({location})")

                    with cols[1]:
                        if st.button(
                            "선택",
                            key=f"sidebar_select_{profile_id}",
                            use_container_width=True,
                        ):
                            handle_profile_select(profile_id)

                    with cols[2]:
                        if st.button(
                            "삭제",
                            key=f"sidebar_delete_{profile_id}",
                            use_container_width=True,
                        ):
                            handle_profile_delete(profile_id)

        st.markdown("---")

        # ========== 설정 버튼 (하단 고정) ==========
        if st.button("⚙️ 설정", key="sidebar_settings", use_container_width=True):
            handle_settings_click()


# --- 실행 (테스트용) ---
if __name__ == "__main__":
    render_sidebar()
