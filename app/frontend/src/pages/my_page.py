"""마이페이지 관련 함수들 - Part 1: 헬퍼/핸들러 함수 + 리다이렉션 로직"""

from datetime import date
import streamlit as st
from typing import Optional
import logging
from ..backend_service import backend_service
from ..utils.template_loader import load_css
from ..utils.session_manager import clear_session
from src.state_manger import get_redirect_info, clear_redirect, reset_profile_states

# 로거 설정
logger = logging.getLogger(__name__)

# 옵션 정의 (login.py와 완전히 동일하게)
GENDER_OPTIONS = ["남성", "여성"]
HEALTH_INSURANCE_OPTIONS = ["직장", "지역", "피부양", "의료급여"]
BASIC_LIVELIHOOD_OPTIONS = ["없음", "생계", "의료", "주거", "교육"]
DISABILITY_OPTIONS = ["미등록", "심한 장애", "심하지 않은 장애"]

# ✅ 회원가입 폼과 동일한 형식으로 변경
LONGTERM_CARE_DISPLAY_OPTIONS = [
    "해당없음",
    "1등급",
    "2등급",
    "3등급",
    "4등급",
    "5등급",
    "인지지원등급",
]
LONGTERM_CARE_MAP = {
    "해당없음": "NONE",
    "1등급": "G1",
    "2등급": "G2",
    "3등급": "G3",
    "4등급": "G4",
    "5등급": "G5",
    "인지지원등급": "COGNITIVE",
}
# 역매핑 (DB 값 → 화면 표시용)
LONGTERM_CARE_REVERSE_MAP = {v: k for k, v in LONGTERM_CARE_MAP.items()}

PREGNANCY_OPTIONS = ["없음", "임신중", "출산후12개월이내"]

# 장애 등급 매핑
DISABILITY_MAP = {"미등록": "0", "심한 장애": "1", "심하지 않은 장애": "2"}
DISABILITY_REVERSE_MAP = {v: k for k, v in DISABILITY_MAP.items()}


# ========== 헬퍼 함수 ==========
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


def _get_profile_id(profile):
    """프로필 ID를 안전하게 추출합니다. (None 방지)"""
    profile_id = profile.get("id") or profile.get("user_id")
    if profile_id is None:
        st.error(f"프로필 ID를 찾을 수 없습니다: {profile}")
        return None
    return int(profile_id)


def _get_auth_token() -> Optional[str]:
    """세션에서 인증 토큰을 가져옵니다."""
    token = st.session_state.get("auth_token")

    if not token:
        logger.warning("auth_token이 세션에 없습니다.")
        logger.debug(f"현재 세션 상태: {list(st.session_state.keys())}")
        st.error("인증 토큰이 없습니다. 다시 로그인해주세요.")

    return token


def _refresh_profiles_from_db():
    """DB에서 최신 프로필 목록을 가져와 세션 상태를 업데이트합니다."""
    token = _get_auth_token()
    if not token:
        return

    ok, profiles_list = backend_service.get_all_profiles(token)
    if ok:
        st.session_state.profiles = profiles_list if profiles_list else []
    else:
        st.error("프로필 목록을 새로고침하는데 실패했습니다.")


# ---


# ========== 리다이렉션 처리 함수 ⭐ ==========
def handle_redirect_actions():
    """
    사이드바에서 전달된 리다이렉션 액션을 처리합니다.

    이 함수는 render_my_page_modal() 시작 부분에서 호출됩니다.
    """
    redirect_to, redirect_action, redirect_profile_id = get_redirect_info()

    # 리다이렉션이 없으면 아무것도 하지 않음
    if not redirect_to or redirect_to != "my_page":
        return

    # 리다이렉션 액션 처리
    if redirect_action == "add_profile":
        # 프로필 추가 모드로 전환
        st.session_state["isAddingProfile"] = True
        st.session_state["newProfile"] = {}
        logger.info("사이드바에서 프로필 추가 요청 받음")

    elif redirect_action == "edit_profile" and redirect_profile_id:
        # 프로필 편집 모드로 전환
        # 해당 프로필을 찾아서 편집 데이터로 설정
        profile_to_edit = next(
            (
                p
                for p in st.session_state.profiles
                if _get_profile_id(p) == redirect_profile_id
            ),
            None,
        )

        if profile_to_edit:
            st.session_state["editingProfileId"] = redirect_profile_id
            st.session_state["editingData"] = profile_to_edit.copy()
            logger.info(f"사이드바에서 프로필 편집 요청 받음: {redirect_profile_id}")
        else:
            st.error(f"편집할 프로필을 찾을 수 없습니다. (ID: {redirect_profile_id})")

    # 리다이렉션 정보 초기화
    clear_redirect()


# ========== 핸들러 함수 ==========
# --- ⭐ 프로필 전환 리팩토링: `sidebar.py`와 동일한 콜백 함수로 변경 ---
def handle_profile_switch(profile_id: int):
    """
    프로필 선택 콜백 함수.
    백엔드에 주 프로필 변경을 요청하고, 성공 시 세션 상태를 업데이트합니다.
    """
    if profile_id is None:
        return

    token = _get_auth_token()
    if token:
        success, message = backend_service.set_main_profile(token, profile_id)
        if success:
            st.session_state.current_profile_id = profile_id
            st.toast("✅ 프로필이 전환되었습니다.")
        else:
            st.error(f"활성 프로필 변경 실패: {message}")


# ---


def handle_delete_profile(profile_id):
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

            # --- ⭐ 프로필 전환 리팩토링: `sidebar.py`와 동일한 삭제 로직 ---
            is_active_deleted = st.session_state.current_profile_id == profile_id
            st.session_state.profiles = [
                p for p in st.session_state.profiles if _get_profile_id(p) != profile_id
            ]

            if is_active_deleted and st.session_state.profiles:
                new_active_profile_id = _get_profile_id(st.session_state.profiles[0])
                if new_active_profile_id is not None:
                    ok, _ = backend_service.set_main_profile(
                        token, new_active_profile_id
                    )
                    if ok:
                        st.session_state.current_profile_id = new_active_profile_id
                    else:
                        st.error("새 활성 프로필을 설정하는 데 실패했습니다.")
            elif not st.session_state.profiles:
                st.session_state.current_profile_id = None
            # ---
            _refresh_profiles_from_db()  # DB와 동기화
            st.rerun()  # UI 구조 변경으로 rerun 필요
        else:
            st.error(f"프로필 삭제 중 오류 발생: {message}")


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

            new_profile_id = response_data.get("id")

            if new_profile_id is not None:
                set_main_ok, msg = backend_service.set_main_profile(
                    token, new_profile_id
                )
                if set_main_ok:
                    _refresh_profiles_from_db()
                else:
                    st.error(f"새 프로필을 메인으로 설정하는데 실패했습니다: {msg}")
            else:
                st.error("새 프로필 ID를 받지 못했습니다.")
        else:
            st.error(f"프로필 추가 중 오류 발생: {response_data}")
        st.rerun()


def handle_start_edit(profile):
    profile_id = _get_profile_id(profile)
    if profile_id is None:
        st.error("편집할 프로필 ID를 찾을 수 없습니다.")
        return

    st.session_state.editingProfileId = profile_id
    st.session_state.editingData = profile.copy()
    st.rerun()


def handle_save_edit(edited_data):
    pid = st.session_state.editingProfileId

    if pid is None:
        st.error("편집 중인 프로필 ID가 없습니다.")
        return

    if not edited_data.get("name") or not edited_data.get("location"):
        st.error(
            "프로필 이름과 거주지는 필수 입력 항목입니다. 편집 내용을 확인해주세요."
        )
        return

    token = _get_auth_token()
    if token:
        update_payload = edited_data.copy()
        update_payload.pop("isActive", None)
        update_payload.pop("id", None)

        success, message = backend_service.update_user_profile(
            token, pid, update_payload
        )
        if success:
            st.session_state.editingProfileId = None
            st.session_state.editingData = {}
            _refresh_profiles_from_db()
            st.success("프로필이 성공적으로 수정되었습니다.")
        else:
            st.error(f"프로필 수정 중 오류 발생: {message}")
        st.rerun()


def handle_cancel_edit():
    st.session_state.editingProfileId = None
    st.session_state.editingData = {}
    st.rerun()


def handle_password_reset():
    data = st.session_state.password_data
    if not data["current"] or not data["new"] or not data["confirm"]:
        st.session_state.password_error = "모든 필드를 입력해주세요."
        return
    if data["new"] != data["confirm"]:
        st.session_state.password_error = "새 비밀번호가 일치하지 않습니다."
        return
    token = _get_auth_token()
    if not token:
        st.session_state.password_error = "로그인 정보를 찾을 수 없습니다."
        return
    # 백엔드 API 호출
    success, message = backend_service.reset_password(
        token, data["current"], data["new"]
    )
    if success:
        st.success("비밀번호가 성공적으로 변경되었습니다.")
        st.session_state.show_password_reset = False
        st.session_state.password_error = ""
    else:
        st.session_state.password_error = message


"""마이페이지 관련 함수들 - Part 2: UI 렌더링 함수"""


# ========== UI 렌더링 함수 ==========
def render_my_page_modal():
    """마이페이지 모달 렌더링 (프로필 추가 / 편집 기능 포함)"""
    load_css("my_page.css")

    if not st.session_state.get("is_logged_in", False):
        st.error("로그인이 필요합니다.")
        return

    # ⭐ 리다이렉션 처리 (사이드바에서 전달된 액션 처리)
    handle_redirect_actions()

    # 상태 초기화
    if "show_password_reset" not in st.session_state:
        st.session_state.show_password_reset = False
    if "show_delete_confirm" not in st.session_state:
        st.session_state.show_delete_confirm = False
    if "password_data" not in st.session_state:
        st.session_state.password_data = {"current": "", "new": "", "confirm": ""}
    if "password_error" not in st.session_state:
        st.session_state.password_error = ""

    token = _get_auth_token()
    if not token:
        st.error("인증 토큰이 없습니다. 다시 로그인해주세요.")
        logger.error(f"토큰 없음. 세션 키: {list(st.session_state.keys())}")
        return

    if not st.session_state.get("profiles") or len(st.session_state.profiles) == 0:
        success = _refresh_profiles_from_db()
        if not success:
            st.error("프로필을 불러오는데 실패했습니다. 다시 시도해주세요.")
            return

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
        if st.button("➕ 프로필 추가", key="btn_add_profile", use_container_width=True):
            st.session_state["isAddingProfile"] = True
            st.session_state["newProfile"] = {}
            st.rerun()

    st.markdown("")

    # --- ⭐ 프로필 전환 리팩토링: `current_profile_id`를 기준으로 활성 프로필 찾기 ---
    active_profile = next(
        (
            p
            for p in st.session_state.profiles
            if _get_profile_id(p) == st.session_state.get("current_profile_id")
        ),
        None,
    )
    # ---
    if active_profile and is_profile_incomplete(active_profile):
        st.warning("정확한 추천을 위해 프로필 정보를 완성해주세요.")

    st.markdown("---")

    # ========================================================================
    # ✅ 프로필 추가 폼 (회원가입 폼과 동일하게 수정)
    # ========================================================================
    if st.session_state.get("isAddingProfile", False):
        st.markdown("##### 새 프로필 추가")
        np = st.session_state.get("newProfile", {})
        with st.form("add_profile_form"):
            name = st.text_input("프로필 이름 *", value=np.get("name", ""))
            birth = st.date_input(
                "생년월일",
                value=_parse_birthdate(np.get("birthDate")) or date(1990, 1, 1),
                min_value=date(1920, 1, 1),
                max_value=date.today(),
            )

            gender = st.selectbox("성별", options=GENDER_OPTIONS)

            location = st.text_input(
                "거주지 (시군구) *", placeholder="예: 서울시 강남구"
            )

            health = st.selectbox("건강보험 자격 *", options=HEALTH_INSURANCE_OPTIONS)

            # ✅ 회원가입과 동일하게 텍스트 입력
            income = st.text_input(
                "중위소득 대비 소득수준 (%) *",
                placeholder="예: 50, 100, 150",
                help="중위소득 대비 소득 수준을 백분율로 입력하세요",
            )

            basic = st.selectbox(
                "기초생활보장 급여 *", options=BASIC_LIVELIHOOD_OPTIONS
            )

            disability = st.selectbox(
                "장애 등급 *", options=list(DISABILITY_MAP.keys())
            )

            # ✅ 회원가입과 동일하게 한글 표시
            longterm = st.selectbox(
                "장기요양 등급 *", options=LONGTERM_CARE_DISPLAY_OPTIONS
            )

            pregnancy = st.selectbox("임신·출산 여부 *", options=PREGNANCY_OPTIONS)

            col_submit, col_cancel = st.columns([1, 1])

            with col_submit:
                if st.form_submit_button("추가", use_container_width=True):
                    # ✅ 소득 수준 숫자 변환
                    try:
                        income_value = float(income) if income.strip() else None
                    except (ValueError, TypeError):
                        income_value = None

                    new_profile_data = {
                        "name": name.strip(),
                        "birthDate": birth.isoformat(),
                        "gender": gender,
                        "location": location.strip(),
                        "healthInsurance": health,
                        "incomeLevel": income_value,
                        "basicLivelihood": basic,
                        "disabilityLevel": DISABILITY_MAP.get(disability, "0"),
                        "longTermCare": LONGTERM_CARE_MAP.get(longterm, "NONE"),
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
            profile_id = _get_profile_id(active_profile)
            if profile_id is not None and st.button(
                "✏️", key=f"btn_edit_profile_{profile_id}"
            ):
                st.session_state["editingProfileId"] = profile_id
                st.session_state["editingData"] = active_profile.copy()
                st.rerun()
    else:
        st.info("등록된 프로필이 없습니다. 새 프로필이 필요합니다.")

    st.markdown("---")

    # ========================================================================
    # ✅ 프로필 수정 폼 (회원가입 폼과 동일하게 수정)
    # ========================================================================
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
                options=GENDER_OPTIONS,
                index=(
                    GENDER_OPTIONS.index(ed.get("gender", GENDER_OPTIONS[0]))
                    if ed.get("gender") in GENDER_OPTIONS
                    else 0
                ),
                key="edit_gender",
            )
            location = st.text_input(
                "거주지 (시군구) *",
                value=ed.get("location", ""),
                key="edit_location",
                placeholder="예: 서울시 강남구",
            )
            health = st.selectbox(
                "건강보험 자격 *",
                options=HEALTH_INSURANCE_OPTIONS,
                index=(
                    HEALTH_INSURANCE_OPTIONS.index(
                        ed.get("healthInsurance", HEALTH_INSURANCE_OPTIONS[0])
                    )
                    if ed.get("healthInsurance") in HEALTH_INSURANCE_OPTIONS
                    else 0
                ),
                key="edit_health",
            )

            # ✅ 소득 수준 - 회원가입 폼과 동일하게 텍스트 입력으로 변경
            income = st.text_input(
                "중위소득 대비 소득수준 (%) *",
                value=str(ed.get("incomeLevel", "")),
                key="edit_income",
                placeholder="예: 50, 100, 150",
                help="중위소득 대비 소득 수준을 백분율로 입력하세요",
            )

            basic = st.selectbox(
                "기초생활보장 급여 *",
                options=BASIC_LIVELIHOOD_OPTIONS,
                index=(
                    BASIC_LIVELIHOOD_OPTIONS.index(
                        ed.get("basicLivelihood", BASIC_LIVELIHOOD_OPTIONS[0])
                    )
                    if ed.get("basicLivelihood") in BASIC_LIVELIHOOD_OPTIONS
                    else 0
                ),
                key="edit_basic",
            )

            disability_value = ed.get("disabilityLevel", "0")
            disability_display = DISABILITY_REVERSE_MAP.get(disability_value, "미등록")
            disability = st.selectbox(
                "장애 등급 *",
                options=list(DISABILITY_MAP.keys()),
                index=(
                    list(DISABILITY_MAP.keys()).index(disability_display)
                    if disability_display in DISABILITY_MAP
                    else 0
                ),
                key="edit_disability",
            )

            # ✅ 장기요양 등급 - 회원가입 폼과 동일하게 변경
            longterm_value = ed.get("longTermCare", "NONE")
            longterm_display = LONGTERM_CARE_REVERSE_MAP.get(longterm_value, "해당없음")
            longterm = st.selectbox(
                "장기요양 등급 *",
                options=LONGTERM_CARE_DISPLAY_OPTIONS,
                index=(
                    LONGTERM_CARE_DISPLAY_OPTIONS.index(longterm_display)
                    if longterm_display in LONGTERM_CARE_DISPLAY_OPTIONS
                    else 0
                ),
                key="edit_longterm",
            )

            pregnancy_value = ed.get("pregnancyStatus", PREGNANCY_OPTIONS[0])
            pregnancy = st.selectbox(
                "임신·출산 여부 *",
                options=PREGNANCY_OPTIONS,
                index=(
                    PREGNANCY_OPTIONS.index(pregnancy_value)
                    if pregnancy_value in PREGNANCY_OPTIONS
                    else 0
                ),
                key="edit_pregnancy",
            )

            col_save, col_cancel = st.columns([1, 1])
            with col_save:
                if st.form_submit_button("저장", use_container_width=True):
                    # ✅ 소득 수준 숫자 변환
                    try:
                        income_value = float(income) if income else 0.0
                    except (ValueError, TypeError):
                        income_value = 0.0

                    edited_data = {
                        "id": st.session_state.editingProfileId,
                        "name": name.strip(),
                        "birthDate": (
                            birth.isoformat() if isinstance(birth, date) else str(birth)
                        ),
                        "gender": gender,
                        "location": location.strip(),
                        "healthInsurance": health,
                        "incomeLevel": income_value,  # float로 변환된 값
                        "basicLivelihood": basic,
                        "disabilityLevel": DISABILITY_MAP.get(disability, "0"),
                        "longTermCare": LONGTERM_CARE_MAP.get(
                            longterm, "NONE"
                        ),  # ✅ 매핑 적용
                        "pregnancyStatus": pregnancy,
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
        profile_id = _get_profile_id(profile)

        # 현재 활성 프로필은 '선택' 버튼을 비활성화하고 '활성'으로 표시
        is_active = profile_id == st.session_state.get("current_profile_id")

        with cols[0]:
            st.write(
                f"- {profile.get('name', '무명')} ({profile.get('location','미입력')})"
            )
        with cols[1]:
            if profile_id is not None:
                st.button(
                    "선택",
                    key=f"select_{profile_id}",
                    on_click=handle_profile_switch,
                    args=(profile_id,),
                    disabled=is_active,  # 활성 프로필은 비활성화
                )
        with cols[2]:
            profile_id = _get_profile_id(profile)
            if profile_id is not None and st.button("삭제", key=f"del_{profile_id}"):
                handle_delete_profile(profile_id)
    st.markdown("---")

    # 계정 관련 액션
    st.markdown("#### 계정")
    col_pw, col_delete, col_logout = st.columns(3)
    with col_pw:
        if st.button(
            "🔒 비밀번호 재설정", key="btn_reset_pw", use_container_width=True
        ):
            st.session_state["show_password_reset"] = True
            st.rerun()
    with col_delete:
        if st.button("🗑️ 회원 탈퇴", key="btn_delete_account", use_container_width=True):
            st.session_state.show_delete_confirm = True
            st.rerun()
    with col_logout:
        if st.button("→ 로그아웃", key="btn_logout", use_container_width=True):
            clear_session()
            st.session_state["is_logged_in"] = False
            st.session_state["show_profile"] = False
            st.success("로그아웃 되었습니다.")
            st.rerun()

    # 비밀번호 재설정 폼
    if st.session_state.get("show_password_reset"):
        st.markdown("---")
        st.markdown("##### 비밀번호 재설정")
        with st.form(key="password_reset_form_mypage"):
            current_pw = st.text_input("현재 비밀번호 *", type="password")
            new_pw = st.text_input("새 비밀번호 *", type="password")
            confirm_pw = st.text_input("새 비밀번호 확인 *", type="password")

            if st.session_state.get("password_error"):
                st.error(st.session_state.password_error)

            col_submit, col_cancel = st.columns(2)
            with col_submit:
                if st.form_submit_button("변경하기", use_container_width=True):
                    st.session_state.password_data = {
                        "current": current_pw,
                        "new": new_pw,
                        "confirm": confirm_pw,
                    }
                    handle_password_reset()
                    st.rerun()
            with col_cancel:
                if st.form_submit_button("취소", use_container_width=True):
                    st.session_state.show_password_reset = False
                    st.session_state.password_error = ""
                    st.rerun()

    # 회원 탈퇴 확인
    if st.session_state.get("show_delete_confirm"):
        st.markdown("---")
        st.error(
            "정말로 회원 탈퇴를 진행하시겠습니까? 모든 데이터가 영구적으로 삭제됩니다."
        )
        col_confirm, col_cancel_delete = st.columns(2)
        with col_confirm:
            if st.button("예, 탈퇴합니다.", use_container_width=True, type="primary"):
                ok, msg = backend_service.delete_user_account(token)
                if ok:
                    st.success("회원 탈퇴가 완료되었습니다.")
                    clear_session()
                    st.session_state.clear()
                    st.rerun()
                else:
                    st.error(f"회원 탈퇴 실패: {msg}")

        with col_cancel_delete:
            if st.button("아니요, 취소합니다.", use_container_width=True):
                st.session_state.show_delete_confirm = False
                st.rerun()


def render_my_page_button():
    """마이페이지 열기 버튼 렌더링"""
    if st.button("👤 마이페이지", key="open_my_page"):
        st.session_state["show_profile"] = True
        st.rerun()
    st.markdown("---")
