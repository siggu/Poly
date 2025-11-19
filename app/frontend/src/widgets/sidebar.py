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


# --- ⭐ 프로필 전환 리팩토링: st.rerun()을 사용하지 않는 콜백 함수로 변경 ---
def handle_profile_select(profile_id: int):
    """
    프로필 선택 콜백 함수.
    백엔드에 주 프로필 변경을 요청하고, 성공 시 세션 상태를 업데이트합니다.
    버튼의 on_click에서 호출되므로 st.rerun()이 필요 없습니다.
    """
    if profile_id is None:
        return

    token = _get_auth_token()
    if token:
        success, message = backend_service.set_main_profile(token, profile_id)
        if success:
            # 세션 상태의 current_profile_id를 직접 업데이트
            st.session_state.current_profile_id = profile_id
            st.toast("✅ 프로필이 전환되었습니다.")
        else:
            st.error(f"활성 프로필 변경 실패: {message}")


# ---


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

            # --- ⭐ 프로필 전환 리팩토링: 삭제 후 로직 변경 ---
            # 삭제된 프로필이 현재 활성 프로필인지 확인
            is_active_deleted = st.session_state.current_profile_id == profile_id

            # 로컬 프로필 목록에서 즉시 제거
            st.session_state.profiles = [
                p for p in st.session_state.profiles if _get_profile_id(p) != profile_id
            ]

            # 활성 프로필이 삭제되었고, 남은 프로필이 있다면
            if is_active_deleted and st.session_state.profiles:
                new_active_profile = st.session_state.profiles[0]
                new_active_profile_id = _get_profile_id(new_active_profile)

                if new_active_profile_id is not None:
                    # 백엔드에 새 활성 프로필 설정 요청
                    ok, _ = backend_service.set_main_profile(
                        token, new_active_profile_id
                    )
                    if ok:
                        # 세션 상태 업데이트
                        st.session_state.current_profile_id = new_active_profile_id
                    else:
                        st.error("새 활성 프로필 설정에 실패했습니다.")
            # 남은 프로필이 없다면
            elif not st.session_state.profiles:
                st.session_state.current_profile_id = None
            # ---
            st.rerun()  # 삭제는 UI 구조가 바뀌므로 rerun 필요
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

        # 프로필 추가 버튼
        if st.button(
            "➕ 프로필 추가", key="sidebar_add_profile", use_container_width=True
        ):
            handle_add_profile_click()

        st.markdown("")

        # --- ⭐ 프로필 전환 리팩토링: `current_profile_id`를 기준으로 활성 프로필 찾기 ---
        # 활성 프로필 표시
        active_profile = next(
            (
                p
                for p in st.session_state.profiles
                if _get_profile_id(p) == st.session_state.get("current_profile_id")
            ),
            None,
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
            # --- ⭐ 프로필 전환 리팩토링: `current_profile_id`와 일치하지 않는 프로필만 표시 ---
            other_profiles = [
                p
                for p in st.session_state.profiles
                if _get_profile_id(p) != st.session_state.get("current_profile_id")
            ]

            if not other_profiles and active_profile:
                st.caption("다른 프로필이 없습니다.")

            for profile in other_profiles:
                profile_id = _get_profile_id(profile)
                if profile_id is None:
                    continue

                with st.container():
                    cols = st.columns([6, 2, 2])

                    with cols[0]:
                        name = profile.get("name", "무명")
                        location = profile.get("location", "미입력")
                        st.write(f"**{name}** ({location})")

                    with cols[1]:
                        # --- ⭐ 프로필 전환 리팩토링: on_click과 args 사용 ---
                        st.button(
                            "선택",
                            key=f"sidebar_select_{profile_id}",
                            on_click=handle_profile_select,
                            args=(profile_id,),
                            use_container_width=True,
                        )
                        # ---

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
