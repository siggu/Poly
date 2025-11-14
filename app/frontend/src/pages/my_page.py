"""마이페이지 관련 함수들 11.13 수정"""

from datetime import date
import streamlit as st
from typing import Optional
from ..backend_service import backend_service
from ..utils.template_loader import load_css


def _parse_birthdate(value):
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except Exception:
            return None
    return None


def calculate_age(birth_date):
    bd = _parse_birthdate(birth_date)
    if not bd:
        return None
    today = date.today()
    years = today.year - bd.year
    if (today.month, today.day) < (bd.month, bd.day):
        years -= 1
    return years


def is_profile_incomplete(profile):
    required_fields = [
        "name",
        "birthDate",
        "gender",
        "location",
        "healthInsurance",
        "incomeLevel",
    ]
    for field in required_fields:
        if not profile.get(field) and profile.get(field) != 0:
            return True
    return False


def handle_profile_switch(profile_id):
    for p in st.session_state.profiles:
        p_id = p.get('id') or p.get('user_id')
        p["isActive"] = p_id == profile_id
    # 영구 저장
    token = _get_auth_token()
    if token:
        success, message = backend_service.set_main_profile(token, profile_id)
        if success:
            st.success("활성 프로필이 변경되었습니다.")
            # DB에서 최신 프로필 목록을 다시 가져와 세션 상태 업데이트 (isActive 반영)
            _refresh_profiles_from_db()
        else:
            st.error(f"활성 프로필 저장 중 오류 발생: {message}")
        st.rerun()


def handle_delete_profile(profile_id):
    if len(st.session_state.profiles) <= 1:
        st.warning("최소한 하나의 프로필은 남겨야 합니다.")
        return  # Do not proceed with deletion if only one profile exists

    token = _get_auth_token()
    if token:
        # DB에서 직접 프로필 삭제
        success, message = backend_service.delete_profile(token, profile_id)
        if success:
            st.success("프로필이 삭제되었습니다.")

            # 삭제된 프로필이 활성 프로필이었다면, 새로운 활성 프로필 설정
            is_active_deleted = any(
                (p.get('id') or p.get('user_id')) == profile_id and p.get("isActive")
                for p in st.session_state.profiles
            )

            # 로컬 세션에서도 삭제
            st.session_state.profiles = [
                p for p in st.session_state.profiles if (p.get('id') or p.get('user_id')) != profile_id
            ]

            if is_active_deleted and st.session_state.profiles:
                # 남은 프로필 중 첫 번째를 새 활성 프로필로 지정
                new_active_profile_id = st.session_state.profiles[0].get('id') or st.session_state.profiles[0].get('user_id')
                success_activate, msg_activate = backend_service.set_main_profile(
                    token, new_active_profile_id
                )
                if not success_activate:
                    st.error(f"새 활성 프로필 설정 중 오류 발생: {msg_activate}")
            elif not st.session_state.profiles:
                # 모든 프로필이 삭제된 경우 main_profile_id를 NULL로 설정
                # 이 로직은 백엔드에서 처리하거나, 별도 API가 필요할 수 있습니다.
                pass

            _refresh_profiles_from_db()  # 삭제 및 활성 프로필 변경 후 프로필 목록 새로고침
        else:
            st.error(f"프로필 삭제 중 오류 발생: {message}")
        st.rerun()


def handle_add_profile(new_profile_data):
    if not new_profile_data.get("name") or not new_profile_data.get("location"):
        st.error("프로필 이름과 거주지는 필수 입력 항목입니다.")
        return

    token = _get_auth_token()
    if token:
        success, response_data = backend_service.add_profile(token, new_profile_data)
        if success:
            st.success("새 프로필이 추가되었습니다.")
            st.session_state.isAddingProfile = False

            # 새로 추가된 프로필을 활성 상태로 설정하고 DB에 반영
            new_profile_id = response_data.get("id")
            if new_profile_id:
                set_main_ok, _ = backend_service.set_main_profile(token, new_profile_id)
                if set_main_ok:
                    _refresh_profiles_from_db()

        else:
            st.error(f"프로필 추가 중 오류 발생: {response_data}")
        st.rerun()


def handle_start_edit(profile):
    st.session_state.editingProfileId = profile["id"]
    st.session_state.editingData = profile.copy()
    st.rerun()


def handle_save_edit(edited_data):
    pid = st.session_state.editingProfileId
    if not edited_data.get("name") or not edited_data.get("location"):
        st.error(
            "프로필 이름과 거주지는 필수 입력 항목입니다. 편집 내용을 확인해주세요."
        )
        return

    token = _get_auth_token()
    if token:
        # isActive 필드는 백엔드에서 관리하므로 제거
        update_payload = edited_data.copy()
        update_payload.pop("isActive", None)
        update_payload.pop("id", None)

        success, message = backend_service.update_user_profile(token, pid, update_payload)
        if success:
            st.session_state.editingProfileId = None
            st.session_state.editingData = {}
            _refresh_profiles_from_db()  # Refresh to ensure consistency
            st.success("프로필이 성공적으로 수정되었습니다.")
        else:
            st.error(f"프로필 수정 중 오류 발생: {message}")
        st.rerun()


def _get_auth_token() -> Optional[str]:
    """세션에서 인증 토큰을 가져옵니다."""
    return st.session_state.get("auth_token")


def handle_cancel_edit():
    st.session_state.editingProfileId = None
    st.session_state.editingData = {}
    st.rerun()


def _get_user_main_profile_id() -> Optional[int]:
    """세션 상태에서 사용자의 main_profile_id를 조회합니다."""
    user_info = st.session_state.get("user_info", {})
    if isinstance(user_info, dict):
        return user_info.get("main_profile_id")
    return None


def _refresh_profiles_from_db():
    """DB에서 최신 프로필 목록을 가져와 세션 상태를 업데이트합니다."""
    token = _get_auth_token()
    if token:
        ok, profiles_list = backend_service.get_all_profiles(token)
        if ok and profiles_list:
            main_profile_id = _get_user_main_profile_id()
            for p in profiles_list:
                p_id = p.get('id') or p.get('user_id')
                p["isActive"] = p_id == main_profile_id
            st.session_state.profiles = profiles_list
            return True
    return False


def render_my_page_modal():
    """마이페이지 모달 렌더링 (프로필 추가 / 편집 기능 포함)"""
    # 마이페이지 모달에 필요한 CSS 파일을 로드합니다.
    load_css("my_page.css")

    col_title, col_close = st.columns([9, 1])
    with col_title:
        st.markdown("### 마이페이지")
        st.caption("프로필 정보와 설정을 관리하세요")
    with col_close:
        if st.button("✕", key="btn_close_my_page"):
            st.session_state["show_profile"] = False
            st.rerun()

    st.markdown("---")

    st.markdown("#### 프로필 관리")
    if not st.session_state.get("isAddingProfile", False):
        # 프로필 목록이 비어있으면 DB에서 로드 시도
        if not st.session_state.profiles:
            _refresh_profiles_from_db()

        if st.button("➕ 프로필 추가", key="btn_add_profile", use_container_width=True):
            st.session_state["isAddingProfile"] = True
            st.session_state["newProfile"] = {}
            st.rerun()

    st.markdown("")

    active_profile = next(
        (p for p in st.session_state.profiles if p.get("isActive", False)), None
    )
    if active_profile and is_profile_incomplete(active_profile):
        st.warning("정확한 추천을 위해 프로필 정보를 완성해주세요.")

    st.markdown("---")

    if st.session_state.get("isAddingProfile", False):
        st.markdown("##### 새 프로필 추가")
        np = st.session_state.get("newProfile", {})
        with st.form("add_profile_form"):
            name = st.text_input(
                "프로필 이름 *", value=np.get("name", ""), key="add_name"
            )
            birth = st.date_input(
                "생년월일",
                value=_parse_birthdate(np.get("birthDate")) or date(2000, 1, 1),
                min_value=date(1920, 1, 1),
                max_value=date.today(),
                key="add_birthdate",
            )
            gender = st.selectbox(
                "성별",
                options=["남성", "여성"],
                index=0 if np.get("gender", "남성") == "남성" else 1,
                key="add_gender",
            )
            location = st.text_input(
                "거주지 *", value=np.get("location", ""), key="add_location"
            )
            health = st.selectbox(
                "건강보험",
                options=["직장", "지역", "피부양", "의료급여"],
                index=0 if np.get("healthInsurance", "직장") == "직장" else 1,
                key="add_health",
            )
            income = st.number_input(
                "소득 수준(숫자)",
                min_value=0.0,
                max_value=100000000.0,
                value=float(np.get("incomeLevel", 0.0)),
                key="add_income",
            )
            basic = st.selectbox(
                "기초생활수급",
                options=["없음", "생계", "의료", "주거", "교육"],
                index=0 if np.get("basicLivelihood", "없음") == "없음" else 0,
                key="add_basic",
            )
            disability = st.selectbox(
                "장애 등급 *",
                options=["미등록", "심한 장애", "심하지 않은 장애"],
                key="add_disability",
            )
            longterm = st.selectbox(
                "장기요양 등급 *",
                options=["NONE", "G1", "G2", "G3", "G4", "G5", "COGNITIVE"],
                key="add_longterm",
            )
            pregnancy = st.selectbox(
                "임신·출산 여부 *",
                options=["없음", "임신중", "출산후12개월이내"],
                key="add_pregnancy",
            )

            col_submit, col_cancel = st.columns([1, 1])
            with col_submit:
                if st.form_submit_button("추가", use_container_width=True):
                    new_profile_data = {
                        "name": name.strip(),
                        "birthDate": birth,
                        "gender": gender,
                        "location": location.strip(),
                        "healthInsurance": health,
                        "incomeLevel": income,
                        "basicLivelihood": basic,
                        "disabilityLevel": (
                            "0"
                            if disability == "미등록"
                            else ("1" if disability == "심한 장애" else "2")
                        ),
                        "longTermCare": longterm,
                        "pregnancyStatus": pregnancy,
                    }
                    if not new_profile_data["name"] or not new_profile_data["location"]:
                        st.error("프로필 이름과 거주지는 필수 입력 항목입니다.")
                    else:
                        handle_add_profile(new_profile_data)
            with col_cancel:
                if st.form_submit_button("취소", use_container_width=True):
                    st.session_state["isAddingProfile"] = False
                    st.session_state["newProfile"] = {}
                    st.rerun()

        st.markdown("---")

    st.markdown("#### 기본 프로필")
    if active_profile:
        col_active, col_edit = st.columns([8, 1])
        with col_active:
            st.markdown("**활성** ✓")
            age = calculate_age(active_profile.get("birthDate"))
            birth_display = f"{age}세" if isinstance(age, int) else "미입력"
            st.write(f"- 이름: {active_profile.get('name', '미입력')}")
            st.write(f"- 생년월일: {birth_display}")
            st.write(f"- 거주지: {active_profile.get('location', '미입력')}")
        with col_edit:
            profile_id = active_profile.get('id') or active_profile.get('user_id')
            if st.button("✏️", key=f"btn_edit_profile_{profile_id}"):
                st.session_state["editingProfileId"] = profile_id
                st.session_state["editingData"] = active_profile.copy()
                st.rerun()
    else:
        st.info("등록된 프로필이 없습니다. 새 프로필이 필요합니다.")

    st.markdown("---")

    if st.session_state.get("editingProfileId"):
        st.markdown("##### 프로필 수정")
        ed = st.session_state.get("editingData", {})
        with st.form("edit_profile_form"):
            name = st.text_input(
                "프로필 이름 *", value=ed.get("name", ""), key="edit_name"
            )
            birth = st.date_input(
                "생년월일",
                value=_parse_birthdate(ed.get("birthDate")) or date(1990, 1, 1),
                min_value=date(1920, 1, 1),
                max_value=date.today(),
                key="edit_birthdate",
            )
            gender = st.selectbox(
                "성별",
                options=["남성", "여성"],
                index=0 if ed.get("gender", "남성") == "남성" else 1,
                key="edit_gender",
            )
            location = st.text_input(
                "거주지 *", value=ed.get("location", ""), key="edit_location"
            )
            health = st.selectbox(
                "건강보험",
                options=["직장", "지역", "피부양", "의료급여"],
                index=0 if ed.get("healthInsurance", "직장") == "직장" else 0,
                key="edit_health",
            )
            income = st.number_input(
                "소득 수준(숫자)",
                min_value=0.0,
                max_value=100000000.0,
                value=float(ed.get("incomeLevel", 0.0)),
                key="edit_income",
            )
            basic = st.selectbox(
                "기초생활수급",
                options=["없음", "생계", "의료", "주거", "교육"],
                index=0 if ed.get("basicLivelihood", "없음") == "없음" else 0,
                key="edit_basic",
            )

            col_save, col_cancel = st.columns([1, 1])
            with col_save:
                if st.form_submit_button("저장", use_container_width=True):
                    edited_data = {
                        "id": st.session_state.editingProfileId,
                        "name": name.strip(),
                        "birthDate": birth,
                        "gender": gender,
                        "location": location.strip(),
                        "healthInsurance": health,
                        "incomeLevel": income,
                        "basicLivelihood": basic,
                        "disabilityLevel": ed.get("disabilityLevel", "0"),
                        "longTermCare": ed.get("longTermCare", "NONE"),
                        "pregnancyStatus": ed.get("pregnancyStatus", "없음"),
                    }
                    if not edited_data["name"] or not edited_data["location"]:
                        st.error("프로필 이름과 거주지는 필수 입력 항목입니다.")
                    else:
                        handle_save_edit(edited_data)
            with col_cancel:
                if st.form_submit_button("취소", use_container_width=True):
                    handle_cancel_edit()

        st.markdown("---")

    st.markdown("#### 등록된 프로필")
    for profile in st.session_state.profiles:
        cols = st.columns([6, 1, 1])
        with cols[0]:
            st.write(
                f"- {profile.get('name', '무명')} ({profile.get('location','미입력')})"
            )
        with cols[1]:
            profile_id = profile.get('id') or profile.get('user_id')
            if st.button("선택", key=f"select_{profile_id}"):
                handle_profile_switch(profile_id)
                # handle_profile_switch already saves to DB and reruns
        with cols[2]:
            profile_id = profile.get('id') or profile.get('user_id')
            if st.button("삭제", key=f"del_{profile_id}"):
                handle_delete_profile(profile_id)
    st.markdown("---")

    # 계정 관련 액션 (비밀번호 변경/회원 탈퇴/로그아웃)
    st.markdown("#### 계정")
    col_pw, col_delete, col_logout = st.columns(3)
    with col_pw:
        if st.button(
            "🔒 비밀번호 재설정", key="btn_reset_pw", use_container_width=True
        ):
            # 설정 모달을 열고 비밀번호 폼 표시
            st.session_state["show_profile"] = False
            st.session_state["settings_modal_open"] = True
            st.session_state["show_password_reset"] = True
            st.rerun()
    with col_delete:
        if st.button("🗑️ 회원 탈퇴", key="btn_delete_account", use_container_width=True):
            # 설정 모달에서 탈퇴 확인을 처리
            st.session_state["show_profile"] = False
            st.session_state["settings_modal_open"] = True
            st.session_state.show_delete_confirm = True
            st.rerun()
    with col_logout:
        if st.button("→ 로그아웃", key="btn_logout", use_container_width=True):
            from src.utils.session_manager import update_login_status as _uls

            _uls(is_logged_in=False)
            st.session_state["is_logged_in"] = False
            st.session_state["show_profile"] = False
            st.success("로그아웃 되었습니다.")
            st.rerun()  # 채팅 내용은 state_manager에서 관리하므로 여기서는 초기화하지 않습니다.
