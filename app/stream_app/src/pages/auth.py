"""로그인/회원가입 UI 및 상태 11.10 수정"""


import datetime
from typing import Dict, Any, Tuple
import streamlit as st
from passlib.hash import bcrypt  # ✅ 비밀번호 해시 검증을 위해 추가

# DB 직접 접근 함수 임포트 (상대 경로 사용)
from src.db.database import (
    create_user_and_profile as api_signup_db,
    get_user_by_username as api_get_user_info_db,
    check_user_exists,
    get_user_password_hash,
    get_all_profiles_by_user_id,
)

from src.utils.session_manager import save_session


def api_check_id_availability(user_id: str) -> Tuple[bool, str]:
    """아이디 중복 확인 (DB 조회)"""
    if not user_id or not user_id.strip():
        return False, "아이디를 입력해주세요"
    user_id = user_id.strip()
    # 아이디 형식 검증 (영문, 숫자만 허용, 4-20자)
    import re

    if not re.match(r"^[a-zA-Z0-9]{4,20}$", user_id):
        return False, "아이디는 영문, 숫자 조합 4-20자로 입력해주세요"
    # 예약어 체크
    reserved_ids = ["admin", "root", "system", "guest"]
    if user_id.lower() in reserved_ids:
        return False, "사용할 수 없는 아이디입니다"
    # DB에서 존재 여부 확인
    if check_user_exists(user_id):
        return False, "이미 사용 중인 아이디입니다"
    return True, "사용 가능한 아이디입니다"


GENDER_OPTIONS = ["남성", "여성"]
HEALTH_INSURANCE_OPTIONS = ["직장", "지역", "피부양", "의료급여"]
BASIC_LIVELIHOOD_OPTIONS = ["없음", "생계", "의료", "주거", "교육"]
DISABILITY_OPTIONS = ["미등록", "심한 장애", "심하지 않은 장애"]
LONGTERM_CARE_OPTIONS = ["NONE", "G1", "G2", "G3", "G4", "G5", "COGNITIVE"]
PREGNANCY_OPTIONS = ["없음", "임신중", "출산후12개월이내"]


# ✅ [추가] DB ENUM 값 매핑 딕셔너리
HEALTH_INSURANCE_MAPPING = {
    "직장": "EMPLOYED",
    "지역": "LOCAL",
    "피부양": "DEPENDENT",
    "의료급여": "MEDICAL_AID_1",  # 🚨주의: 1종/2종이 확실히 구분되면 이 부분을 수정해야 합니다.
    # 현재 UI 옵션에 맞춰 '의료급여' -> 'MEDICAL_AID_1'로 임시 매핑합니다.
}

# ✅ [추가] 기초생활보장 급여 매핑 딕셔너리
BASIC_LIVELIHOOD_MAPPING = {
    "없음": "NONE",
    "생계": "LIVELIHOOD",
    "의료": "MEDICAL",
    "주거": "HOUSING",
    "교육": "EDUCATION",
}

# ==============================================================================
# 1. 상태 초기화 함수 (app.py 최상단에서만 호출됨)
# ==============================================================================


def initialize_auth_state():

    defaults = {
        "auth_active_tab": "login",
        "login_data": {"userId": "", "password": ""},
        "auth_error": {"login": "", "signup": ""},
        "signup_form_data": {
            "userId": "",
            "password": "",
            "confirmPassword": "",
            "name": "",
            "gender": GENDER_OPTIONS[0],
            "birthDate": "",
            "location": "",
            "healthInsurance": HEALTH_INSURANCE_OPTIONS[0],
            "incomeLevel": "",
            "basicLivelihood": BASIC_LIVELIHOOD_OPTIONS[0],
        },
        "user_info": {},
        "is_id_available": None,
        "is_checking_id": False,
        "is_logged_in": False,  # ✅ 추가: 로그인 상태 플래그
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ==============================================================================
# 2. 로그인 렌더링
# ==============================================================================


def handle_login(user_id: str, password: str) -> Tuple[bool, str]:
    """사용자 로그인 처리 (DB 직접 조회)"""
    # 1. DB에서 사용자 ID로 비밀번호 해시 조회
    stored_hash = get_user_password_hash(user_id)
    if not stored_hash:
        return False, "아이디 또는 비밀번호가 올바르지 않습니다."

    # 2. passlib.bcrypt를 사용하여 입력된 비밀번호와 저장된 해시 검증
    try:
        is_verified = bcrypt.verify(password, stored_hash)
        if not is_verified:
            return False, "아이디 또는 비밀번호가 올바르지 않습니다."
    except Exception:
        return False, "비밀번호 검증 중 오류가 발생했습니다. 관리자에게 문의하세요."

    return True, "로그인 성공"


def render_login_tab():
    data = st.session_state["login_data"]
    error_msg = st.session_state["auth_error"].get("login", "")

    with st.form("login_form"):
        # ... (로그인 폼 UI 로직은 동일) ...
        st.text_input("아이디", value=data["userId"], key="login_id_input")
        st.text_input(
            "비밀번호", type="password", value=data["password"], key="login_pw_input"
        )
        if error_msg:
            st.error(error_msg)
        submitted = st.form_submit_button("로그인", use_container_width=True)

    if submitted:
        data["userId"] = st.session_state.get("login_id_input", "").strip()
        data["password"] = st.session_state.get("login_pw_input", "")
        if not data["userId"] or not data["password"]:
            st.session_state["auth_error"][
                "login"
            ] = "아이디와 비밀번호를 입력해주세요."
            st.rerun()

        # ✅ [수정] DB 직접 조회 방식으로 로그인 처리
        success, message = handle_login(data["userId"], data["password"])
        if success:
            st.session_state["is_logged_in"] = True
            st.session_state["show_login_modal"] = False
            st.session_state["auth_error"]["login"] = ""

            # DB에서 프로필 정보 조회
            ok, user_info = api_get_user_info_db(data["userId"])  # username으로 조회
            if ok:
                st.session_state["user_info"] = user_info
                st.session_state["user_info"]["main_profile_id"] = user_info.get("main_profile_id")
            else:
                st.session_state["user_info"] = {"userId": data["userId"]}

            # 🚨 [수정] 사용자의 모든 프로필 목록을 DB에서 직접 조회
            user_uuid = st.session_state.user_info.get("id")
            ok_profiles, profiles_list = get_all_profiles_by_user_id(user_uuid)
            if ok_profiles and profiles_list:
                # 첫 번째 프로필을 활성 프로필로 설정
                profiles_list[0]["isActive"] = True
                st.session_state["profiles"] = profiles_list

            save_session(
                st.session_state.user_info.get("id"),  # 세션에는 id(uuid) 저장
                st.session_state.get("user_info", {"userId": data["userId"]}),
            )
        else:
            st.session_state["auth_error"]["login"] = message
        st.rerun()


# ==============================================================================
# 3. 회원가입 핸들러 및 렌더링
# ==============================================================================


# 'HEALTH_INSURANCE_MAPPING' 딕셔너리가 이 함수를 사용하는 파일 상단에 정의되어 있어야 합니다!


def handle_signup_submit(signup_data: Dict[str, Any]):
    if not signup_data.get("userId") or not signup_data.get("password"):
        return False, "필수 정보를 입력해주세요."

    if signup_data.get("password") != signup_data.get("confirmPassword"):
        return False, "비밀번호와 비밀번호 확인이 일치하지 않습니다."

    signup_data["password"] = bcrypt.hash(signup_data["password"].encode("utf-8"))
    # ====================================================
    # ✅ [수정 핵심] ENUM 값 매핑 로직 (DB 전송 직전)
    # ====================================================

    # 1. healthInsurance 매핑
    insurance_kr = signup_data.get("healthInsurance")
    mapped_insurance = HEALTH_INSURANCE_MAPPING.get(insurance_kr)
    if not mapped_insurance:
        # 혹시 모를 에러 방지
        return False, f"내부 오류: 알 수 없는 건강보험 자격 '{insurance_kr}'"
    signup_data["healthInsurance"] = mapped_insurance

    # 2. basicLivelihood 매핑
    livelihood_kr = signup_data.get("basicLivelihood")
    mapped_livelihood = BASIC_LIVELIHOOD_MAPPING.get(livelihood_kr)
    if not mapped_livelihood:
        # 혹시 모를 에러 방지
        return False, f"내부 오류: 알 수 없는 기초생활보장 급여 유형 '{livelihood_kr}'"
    signup_data["basicLivelihood"] = mapped_livelihood

    # 필드명 매핑 (database.py의 create_user_and_profile 함수와 일치시키기 위함)
    signup_data["username"] = signup_data.pop("userId")
    signup_data["residency_sgg_code"] = signup_data.pop("location")
    signup_data["insurance_type"] = signup_data.pop("healthInsurance")
    # ====================================================

    # 🚨 DB 직접 저장 함수 호출
    success, message = api_signup_db(signup_data)

    if success:
        # 회원가입 성공 시, DB에서 방금 생성된 사용자 정보를 다시 조회하여 세션을 설정합니다.
        ok, user_info = api_get_user_info_db(signup_data["username"])
        if not ok:
            return False, "회원가입은 성공했으나, 사용자 정보를 불러오는 데 실패했습니다."

        st.session_state["user_info"] = user_info
        st.session_state["is_logged_in"] = True
        st.session_state["show_login_modal"] = False

        # 사용자의 모든 프로필 목록을 DB에서 조회
        user_uuid = user_info.get("id")
        ok_profiles, profiles_list = get_all_profiles_by_user_id(user_uuid)
        if ok_profiles and profiles_list:
            # 기본 프로필을 활성 상태로 설정
            main_profile_id = user_info.get("main_profile_id")
            for p in profiles_list:
                p["isActive"] = (p["id"] == main_profile_id)
            st.session_state["profiles"] = profiles_list

    return success, message


# ... (앞부분 생략) ...


def render_signup_tab():
    sdata = st.session_state["signup_form_data"]
    err = st.session_state["auth_error"].get("signup", "")

    # 🚨 [수정] 아이디 입력 필드와 중복 확인 버튼을 form 내부로 이동합니다.
    with st.form("signup_form"):
        # ===================================================================
        # ✅ [이동 및 수정] ID 입력 및 중복 확인을 폼 내부로 가져옵니다.
        # ===================================================================
        col_id, col_check = st.columns([7, 3])
        with col_id:
            user_id = st.text_input(
                "아이디 *",
                value=sdata.get("userId", ""),
                key="signup_userid",
                placeholder="아이디를 입력하세요",
            )
        with col_check:
            st.markdown("<br>", unsafe_allow_html=True)
            # st.form 내부에서는 버튼의 key가 폼 제출 시에만 업데이트되므로,
            # 중복 확인 로직은 별도의 폼 제출 로직 없이 처리하는 것이 좋습니다.
            # 여기서는 편의를 위해 버튼 클릭 시 세션 상태를 업데이트하는 기존 로직을 유지하되,
            # 폼 제출 로직과는 별개로 실행되도록 합니다.
            if st.form_submit_button(
                "아이디 중복 확인", key="btn_check_id_inside", use_container_width=True
            ):
                if user_id:
                    is_available, msg = api_check_id_availability(user_id)
                    if is_available:
                        st.session_state["is_id_available"] = True
                        st.success(msg)
                    else:
                        st.session_state["is_id_available"] = False
                        st.error(msg)
                # 중복 확인 버튼을 눌러도 전체 폼 제출로 간주되므로, 이후 제출 버튼 로직이 실행되지 않도록
                # st.session_state.is_checking_id 상태를 활용하여 처리할 수도 있지만,
                # 여기서는 사용자 경험을 위해 중복 확인 후 재실행(rerun)을 피하고
                # 제출 버튼을 다시 누르도록 유도하는 방식을 선택합니다.

        # 폼 내부에 아이디 중복 확인 결과 표시 (선택 사항)
        if st.session_state.get("is_id_available") is False:
            st.error("아이디 중복 확인이 필요하거나, 사용 불가능한 아이디입니다.")
        elif st.session_state.get("is_id_available") is True:
            st.success("사용 가능한 아이디입니다.")
        # ===================================================================

        st.text_input(
            "비밀번호 *",
            type="password",
            value=sdata.get("password", ""),
            key="signup_pw",
            placeholder="8자 이상 입력하세요",
            help="비밀번호는 8자 이상이어야 합니다.",
        )
        st.text_input(
            "비밀번호 확인 *",
            type="password",
            value=sdata.get("confirmPassword", ""),
            key="signup_pw_confirm",
            placeholder="비밀번호를 다시 입력하세요",
        )
        # ... (나머지 폼 필드는 동일) ...
        st.text_input(
            "이름 *",
            value=sdata.get("name", ""),
            key="signup_name",
            placeholder="이름을 입력하세요",
        )

        min_date = datetime.date(1923, 1, 1)
        max_date = datetime.date.today()
        default_date = datetime.date(1990, 1, 1)
        st.date_input(
            "생년월일 *",
            value=default_date,
            min_value=min_date,
            max_value=max_date,
            key="signup_birthdate",
            format="YYYY-MM-DD",
        )

        st.selectbox(
            "성별 *",
            options=GENDER_OPTIONS,
            index=(
                0
                if not sdata.get("gender")
                else GENDER_OPTIONS.index(sdata.get("gender", GENDER_OPTIONS[0]))
            ),
            key="signup_gender",
            placeholder="선택하세요",
        )
        st.text_input(
            "거주지 (시군구) *",
            value=sdata.get("location", ""),
            key="signup_location",
            placeholder="예: 서울시 강남구",
        )
        st.selectbox(
            "건강보험 자격 *",
            options=HEALTH_INSURANCE_OPTIONS,
            key="signup_health",
            placeholder="선택하세요",
        )
        st.text_input(
            "중위소득 대비 소득수준 (%) *",
            value=sdata.get("incomeLevel", ""),
            key="signup_income",
            placeholder="예: 50, 100, 150",
            help="중위소득 대비 소득 수준을 백분율로 입력하세요",
        )
        st.selectbox(
            "기초생활보장 급여 *",
            options=BASIC_LIVELIHOOD_OPTIONS,
            key="signup_basic",
            placeholder="선택하세요",
        )

        disability_map = {"미등록": "0", "심한 장애": "1", "심하지 않은 장애": "2"}
        disability_options = list(disability_map.keys())
        selected_disability = st.selectbox(
            "장애 등급 *",
            options=disability_options,
            key="signup_disability",
            placeholder="선택하세요",
        )

        longterm_map = {
            "해당없음": "NONE",
            "1등급": "G1",
            "2등급": "G2",
            "3등급": "G3",
            "4등급": "G4",
            "5등급": "G5",
            "인지지원등급": "COGNITIVE",
        }
        longterm_options = list(longterm_map.keys())
        selected_longterm = st.selectbox(
            "장기요양 등급 *",
            options=longterm_options,
            key="signup_longterm",
            placeholder="선택하세요",
        )

        pregnancy_options = ["없음", "임신중", "출산후12개월이내"]
        st.selectbox(
            "임신·출산 여부 *",
            options=pregnancy_options,
            key="signup_pregnancy",
            placeholder="선택하세요",
        )

        # 아이디 중복 확인 결과에 따라 재검토 메시지 표시
        if st.session_state.get("is_id_available") is not True:
            # 아이디가 사용 가능하다고 명시적으로 확인되지 않은 경우
            err_msg = "회원가입을 완료하려면 아이디 중복 확인을 완료해주세요."
            if err:
                err_msg = err
            st.error(err_msg)
        elif err:  # 일반적인 다른 오류 메시지
            st.error(err)

        submitted = st.form_submit_button(
            "회원가입", use_container_width=True, type="primary"
        )

        if submitted:
            # 폼 내부에서는 st.session_state에 값이 즉시 반영되므로,
            # 모든 필수 필드가 올바르게 채워졌는지 다시 한번 확인합니다.
            user_id_value = st.session_state.get("signup_userid", "")

            # 1차 유효성 검사 (필수 항목 및 ID 중복 확인)
            if not user_id_value or not st.session_state.signup_pw:
                st.session_state["auth_error"][
                    "signup"
                ] = "아이디와 비밀번호는 필수 정보를 입력해주세요."
                st.rerun()
                return

            if st.session_state.get("is_id_available") is not True:
                st.session_state["auth_error"][
                    "signup"
                ] = "아이디 중복 확인을 완료하고 사용 가능한 아이디를 선택해야 합니다."
                st.rerun()
                return

            # ... (기존의 데이터 수집 및 제출 로직은 동일) ...
            signup_data = {
                "userId": user_id_value,
                "password": st.session_state.signup_pw,
                "confirmPassword": st.session_state.signup_pw_confirm,
                "name": st.session_state.get("signup_name", ""),
                "gender": st.session_state.signup_gender,
                "birthDate": st.session_state.signup_birthdate,
                "location": st.session_state.signup_location,
                "healthInsurance": st.session_state.signup_health,
                "incomeLevel": st.session_state.signup_income,
                "basicLivelihood": st.session_state.signup_basic,
                "disabilityLevel": disability_map.get(selected_disability, "0"),
                "longTermCare": longterm_map.get(selected_longterm, "NONE"),
                "pregnancyStatus": st.session_state.signup_pregnancy,
            }

            # 비밀번호 일치 확인 (필수 항목이므로 여기서 체크)
            if signup_data["password"] != signup_data["confirmPassword"]:
                st.session_state["auth_error"][
                    "signup"
                ] = "비밀번호와 비밀번호 확인이 일치하지 않습니다."
                st.rerun()
                return

            # 비밀번호 길이 확인 (8자 이상)
            if len(signup_data["password"]) < 8:
                st.session_state["auth_error"][
                    "signup"
                ] = "비밀번호는 8자 이상이어야 합니다."
                st.rerun()
                return

            success, message = handle_signup_submit(signup_data)
            if success:
                st.success(message)
                st.rerun()
            else:
                st.session_state["auth_error"]["signup"] = message
                st.rerun()


def render_auth_modal(show_header: bool = True):
    # initialize_auth_state()
    if show_header:
        st.markdown("### SIMPLECIRCLE")
        st.markdown("로그인하거나 새 계정을 만드세요")
    login_tab, signup_tab = st.tabs(["로그인", "회원가입"])
    with login_tab:
        render_login_tab()
    with signup_tab:
        render_signup_tab()
