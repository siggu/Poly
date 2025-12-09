"""로그인/회원가입 UI 및 상태 11.14 테이블 컬럼명에 맞게 수정"""

import datetime
from typing import Dict, Any, Tuple
import streamlit as st

from src.backend_service import backend_service

from src.utils.session_manager import save_session
import re

# ==============================================================================
# 0. 헬퍼 함수: 아이디 중복 확인 API 호출


def api_check_id_availability(user_id: str) -> Tuple[bool, str]:
    """아이디 중복 확인 (DB 조회)"""
    if not user_id or not user_id.strip():
        return False, "아이디를 입력해주세요"
    user_id = user_id.strip()
    # 아이디 형식 검증 (영문, 숫자만 허용, 4-20자)

    if not re.match(r"^[a-zA-Z0-9]{4,20}$", user_id):
        return False, "아이디는 영문, 숫자 조합 4-20자로 입력해주세요"
    # 예약어 체크
    # reserved_ids = ["admin", "root", "system", "guest"]
    # if user_id.lower() in reserved_ids:
    #     return False, "사용할 수 없는 아이디입니다"

    # TODO: 백엔드에 아이디 중복 확인 API를 만들고 호출해야 합니다.
    # 현재는 임시로 True를 반환합니다.
    return True, "사용 가능한 아이디 형식입니다."


GENDER_OPTIONS = ["남성", "여성"]
HEALTH_INSURANCE_OPTIONS = ["직장", "지역", "피부양", "의료급여"]
BASIC_LIVELIHOOD_OPTIONS = ["없음", "생계", "의료", "주거", "교육"]
DISABILITY_OPTIONS = ["미등록", "심한 장애", "심하지 않은 장애"]
LONGTERM_CARE_OPTIONS = ["NONE", "G1", "G2", "G3", "G4", "G5", "COGNITIVE"]
PREGNANCY_OPTIONS = ["없음", "임신중", "출산후12개월이내"]

# ==============================================================================

# # ✅ [추가] DB ENUM 값 매핑 딕셔너리
# HEALTH_INSURANCE_MAPPING = {
#     "직장": "EMPLOYED",
#     "지역": "LOCAL",
#     "피부양": "DEPENDENT",
#     "의료급여": "MEDICAL_AID_1",  # 🚨주의: 1종/2종이 확실히 구분되면 이 부분을 수정해야 합니다.
#     # 현재 UI 옵션에 맞춰 '의료급여' -> 'MEDICAL_AID_1'로 임시 매핑합니다.
# }

# # ✅ [추가] 기초생활보장 급여 매핑 딕셔너리
# BASIC_LIVELIHOOD_MAPPING = {
#     "없음": "NONE",
#     "생계": "LIVELIHOOD",
#     "의료": "MEDICAL",
#     "주거": "HOUSING",
#     "교육": "EDUCATION",
# }

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
            "sex": GENDER_OPTIONS[0],
            "birth_date": "",
            "residency_sgg_code": "",
            "insurance_type": HEALTH_INSURANCE_OPTIONS[0],
            "median_income_ratio": "",
            "basic_benefit_type": BASIC_LIVELIHOOD_OPTIONS[0],
            "disability_grade": DISABILITY_OPTIONS[0],
            "ltci_grade": LONGTERM_CARE_OPTIONS[0],
            "pregnant_or_postpartum12m": PREGNANCY_OPTIONS[0],
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
# 2. 로그인 렌더링11.17 수정
# ==============================================================================


def render_login_tab():
    data = st.session_state["login_data"]
    error_msg = st.session_state["auth_error"].get("login", "")

    with st.form("login_form"):
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

        # API를 통해 로그인 시도
        success, response_data = backend_service.login_user(
            data["userId"], data["password"]
        )

        if success:
            st.session_state["is_logged_in"] = True
            st.session_state["show_login_modal"] = False
            st.session_state["auth_error"]["login"] = ""
            st.session_state["auth_token"] = response_data.get("access_token")

            # 🔥 로그인 성공 후, 사용자 정보와 모든 프로필 가져오기
            token = st.session_state["auth_token"]

            # 1. 사용자 기본 정보 조회
            profile_ok, profile_data = backend_service.get_user_profile(token)
            if profile_ok:
                st.session_state["user_info"] = profile_data

            # 2. 🔥 모든 프로필 목록 조회
            all_profiles_ok, all_profiles = backend_service.get_all_profiles(token)
            if all_profiles_ok and all_profiles:
                # main_profile_id로 활성 프로필 표시
                main_profile_id = (
                    profile_data.get("main_profile_id") if profile_ok else None
                )

                for p in all_profiles:
                    p_id = p.get("id")
                    p["isActive"] = p_id == main_profile_id

                st.session_state["profiles"] = all_profiles
            else:
                # 프로필이 없는 경우 빈 리스트
                st.session_state["profiles"] = []

            # 세션 저장
            save_session(st.session_state.get("user_info", {}), token)
        else:
            st.session_state["auth_error"]["login"] = response_data
        st.rerun()


# ==============================================================================
# 3. 회원가입 핸들러 및 렌더링
# ==============================================================================


# 'HEALTH_INSURANCE_MAPPING' 딕셔너리가 이 함수를 사용하는 파일 상단에 정의되어 있어야 합니다!


def handle_signup_submit(signup_data: Dict[str, Any]):
    if not signup_data.get("username") or not signup_data.get("password"):
        return False, "필수 정보를 입력해주세요."

    if signup_data.get("password") != signup_data.get("confirmPassword"):
        return False, "비밀번호와 비밀번호 확인이 일치하지 않습니다."

    # 백엔드 API 호출
    success, message = backend_service.register_user(signup_data)

    if success:
        # 회원가입 성공 후 바로 로그인 처리
        login_ok, login_data = backend_service.login_user(
            signup_data.get("username"), signup_data.get("password")
        )
        if login_ok:
            st.session_state["is_logged_in"] = True
            st.session_state["auth_token"] = login_data.get("access_token")
            # 프로필 정보 가져오기 등 후속 처리...
        else:
            return False, "회원가입은 성공했으나 자동 로그인에 실패했습니다."

    return success, message


def render_signup_tab():
    sdata = st.session_state["signup_form_data"]
    err = st.session_state["auth_error"].get("signup", "")

    # ✅ 아이디 입력 및 중복 확인을 form 밖으로 이동
    col_id, col_check = st.columns([7, 3])
    with col_id:
        user_id = st.text_input(
            "아이디 *",
            value=sdata.get("userId", ""),
            key="user_id",
            placeholder="아이디를 입력하세요",
        )
    with col_check:
        st.markdown("<br>", unsafe_allow_html=True)
        # form 밖에서는 일반 button 사용
        if st.button(
            "아이디 중복 확인", key="btn_check_id_outside", use_container_width=True
        ):
            if user_id:
                is_available, msg = api_check_id_availability(user_id)
                if is_available:
                    st.session_state["is_id_available"] = True
                    st.success(msg)
                else:
                    st.session_state["is_id_available"] = False
                    st.error(msg)
            else:
                st.warning("아이디를 입력해주세요.")

    # 아이디 중복 확인 결과 표시
    if st.session_state.get("is_id_available") is False:
        st.error("사용 불가능한 아이디입니다.")
    elif st.session_state.get("is_id_available") is True:
        st.success("사용 가능한 아이디입니다.")

    # 회원가입 폼 시작
    with st.form("signup_form"):

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
        st.text_input(
            "이름 *",
            value=sdata.get("name"),
            key="name",
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
            key="birthdate",
            format="YYYY-MM-DD",
        )

        st.selectbox(
            "성별 *",
            options=GENDER_OPTIONS,
            index=(
                0
                if not sdata.get("sex")
                else GENDER_OPTIONS.index(sdata.get("sex", GENDER_OPTIONS[0]))
            ),
            key="sex",
            placeholder="선택하세요",
        )
        st.text_input(
            "거주지 (시군구) *",
            value=sdata.get("residency_sgg_code", ""),
            key="residency_sgg_code",
            placeholder="예: 서울시 강남구",
        )
        st.selectbox(
            "건강보험 자격 *",
            options=HEALTH_INSURANCE_OPTIONS,
            key="insurance_type",
            placeholder="선택하세요",
        )
        st.text_input(
            "중위소득 대비 소득수준 (%) *",
            value=sdata.get("median_income_ratio", ""),
            key="median_income_ratio",
            placeholder="예: 50, 100, 150",
            help="중위소득 대비 소득 수준을 백분율로 입력하세요",
        )
        st.selectbox(
            "기초생활보장 급여 *",
            options=BASIC_LIVELIHOOD_OPTIONS,
            key="basic_benefit_type",
            placeholder="선택하세요",
        )

        disability_map = {"미등록": "0", "심한 장애": "1", "심하지 않은 장애": "2"}
        disability_options = list(disability_map.keys())
        selected_disability = st.selectbox(
            "장애 등급 *",
            options=disability_options,
            key="disability_grade",
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
            key="ltci_grade",
            placeholder="선택하세요",
        )

        pregnancy_options = ["없음", "임신중", "출산후12개월이내"]
        st.selectbox(
            "임신·출산 여부 *",
            options=pregnancy_options,
            key="pregnant_or_postpartum12m",
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
            user_id_value = st.session_state.get("user_id", "")

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
            # 생년월일 유효성 검사 추가
            if not st.session_state.get("birthdate"):
                st.session_state["auth_error"]["signup"] = "생년월일은 필수 정보입니다."
                st.rerun()
                return

            # 중위소득 비율 숫자 변환
            try:
                income_value = (
                    float(st.session_state.median_income_ratio)
                    if st.session_state.median_income_ratio
                    else 0.0
                )
            except (ValueError, TypeError):
                income_value = 0.0

            signup_data = {
                "username": user_id_value,  # 폼에서 가져온 아이디 사용
                "password": st.session_state.signup_pw,
                "confirmPassword": st.session_state.signup_pw_confirm,
                "name": st.session_state.get("name"),
                "birth_date": str(st.session_state.birthdate),
                "sex": st.session_state.get("sex", ""),
                "residency_sgg_code": st.session_state.residency_sgg_code,
                "insurance_type": st.session_state.get("insurance_type", ""),
                "median_income_ratio": income_value,  # float로 변환
                "basic_benefit_type": st.session_state.basic_benefit_type,
                "disability_grade": disability_map.get(selected_disability, "0"),
                "ltci_grade": longterm_map.get(selected_longterm, "NONE"),
                "pregnant_or_postpartum12m": st.session_state.get(
                    "pregnant_or_postpartum12m", ""
                ),
            }

            # 비밀번호 일치 확인 (필수 항목이므로 여기서 체크)
            if signup_data["password"] != signup_data["confirmPassword"]:
                st.session_state["auth_error"][
                    "signup"
                ] = "비밀번호와 비밀번호 확인이 일치하지 않습니다."
                st.rerun()
                return

            # 이름 필드 확인
            if not st.session_state.get("name", "").strip():
                st.session_state["auth_error"]["signup"] = "이름은 필수 정보입니다."
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
