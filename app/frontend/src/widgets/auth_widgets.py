"""인증(로그인/회원가입) 관련 위젯들"""

import streamlit as st
import datetime
from app.stream_app.src.backend_service import (
    api_signup,
    api_check_id_availability,
    api_get_user_info,
)
from src.utils.session_manager import save_session

# 옵션 데이터
GENDER_OPTIONS = ["남성", "여성"]
HEALTH_INSURANCE_OPTIONS = ["직장", "지역", "피부양", "의료급여"]
BASIC_LIVELIHOOD_OPTIONS = ["없음", "생계", "의료", "주거", "교육"]
DISABILITY_OPTIONS = ["미등록", "심한 장애", "심하지 않은 장애"]
LONGTERM_CARE_OPTIONS = ["NONE", "G1", "G2", "G3", "G4", "G5", "COGNITIVE"]
PREGNANCY_OPTIONS = ["없음", "임신중", "출산후12개월이내"]


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
        "user_info": {},  # 사용자 정보 저장용 추가
        "is_id_available": None,
        "is_checking_id": False,
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def render_chatbot_page():
    """챗봇 페이지를 렌더링하는 함수"""
    st.title("정책 추천 챗봇")
    st.write("나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.")

    # 사용자 입력을 받는 부분
    age = st.number_input("나이를 입력하세요:", min_value=0, max_value=120)
    location = st.text_input("거주지를 입력하세요:")
    # interests = st.text_input("관심 분야를 입력하세요:")

    if st.button("추천 받기"):
        # 여기서 정책 추천 로직을 추가할 수 있습니다.
        st.success(f"{age}세, {location}에 사는 분을 위한 맞춤형 정책을 추천합니다.")


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
        # 폼 제출 후 처리 (콜백 없음)
        data["userId"] = st.session_state.get("login_id_input", "").strip()
        data["password"] = st.session_state.get("login_pw_input", "")
        if not data["userId"] or not data["password"]:
            st.session_state["auth_error"][
                "login"
            ] = "아이디와 비밀번호를 입력해주세요."
            st.rerun()

        success, message = api_login(data["userId"], data["password"])
        if success:
            st.session_state["is_logged_in"] = True
            st.session_state["show_login_modal"] = False
            st.session_state["auth_error"]["login"] = ""
            # 사용자 정보/프로필 불러오기
            ok, user_info = api_get_user_info(data["userId"])
            if ok:
                st.session_state["user_info"] = user_info
                # profiles.json 스키마를 Streamlit 내부 프로필 리스트로 매핑
                profile = user_info.get("profile", {}) or {}
                # 세션 프로필을 단일 활성 프로필로 업데이트
                st.session_state["profiles"] = [
                    {
                        "id": data["userId"],
                        "name": user_info.get("profile", {}).get(
                            "name", ""
                        ),  # 없으면 빈값
                        "birthDate": profile.get("birthDate", ""),
                        "gender": profile.get("gender", ""),
                        "location": profile.get("location", ""),
                        "healthInsurance": profile.get("healthInsurance", ""),
                        "incomeLevel": profile.get("incomeLevel", 0),
                        "basicLivelihood": profile.get("basicLivelihood", "없음"),
                        "disabilityLevel": profile.get("disabilityLevel", "0"),
                        "longTermCare": profile.get("longTermCare", "NONE"),
                        "pregnancyStatus": profile.get("pregnancyStatus", "없음"),
                        "isActive": True,
                    }
                ]
            else:
                # 최소한 userId만 저장
                st.session_state["user_info"] = {"userId": data["userId"]}
            # 로그인 세션 저장 (user_info 포함)
            save_session(
                data["userId"],
                st.session_state.get("user_info", {"userId": data["userId"]}),
            )
        else:
            st.session_state["auth_error"]["login"] = message
        st.rerun()


def handle_signup_submit(signup_data):
    """회원가입 처리 및 사용자 정보 저장"""
    if not signup_data.get("userId") or not signup_data.get("password"):
        return False, "필수 정보를 입력해주세요."

    # 회원가입 API 호출
    success, message = api_signup(signup_data["userId"], signup_data)

    if success:
        # 회원가입 성공 시 사용자 정보 저장
        user_info = {
            "userId": signup_data["userId"],
            "name": signup_data.get("name", ""),
            "gender": signup_data.get("gender", ""),
            "birthDate": str(signup_data.get("birthDate", "")),
            "location": signup_data.get("location", ""),
            "healthInsurance": signup_data.get("healthInsurance", ""),
            "incomeLevel": signup_data.get("incomeLevel", ""),
            "basicLivelihood": signup_data.get("basicLivelihood", ""),
        }
        st.session_state["user_info"] = user_info
        st.session_state["is_logged_in"] = True
        st.session_state["show_login_modal"] = False
        # 회원가입 후 자동 로그인 세션 저장
        save_session(signup_data["userId"], user_info)

    return success, message


def render_signup_tab():
    """회원가입 탭 렌더링 (두 번째 사진 참고 - 모든 필드 표시)"""
    sdata = st.session_state["signup_form_data"]
    err = st.session_state["auth_error"].get("signup", "")

    # 아이디 중복 확인 (폼 밖에서 처리)
    col_id, col_check = st.columns([7, 3])
    with col_id:
        user_id = st.text_input(
            "아이디 *",
            value=sdata.get("userId", ""),
            key="signup_userid",
            placeholder="아이디를 입력하세요",
        )
    with col_check:
        st.markdown("<br>", unsafe_allow_html=True)  # 버튼 정렬을 위한 공백
        if st.button("중복 확인", key="btn_check_id", use_container_width=True):
            if user_id:
                is_available, msg = api_check_id_availability(user_id)
                if is_available:
                    st.session_state["is_id_available"] = True
                    st.success(msg)
                else:
                    st.session_state["is_id_available"] = False
                    st.error(msg)

    with st.form("signup_form"):

        # 비밀번호
        st.text_input(
            "비밀번호 *",
            type="password",
            value=sdata.get("password", ""),
            key="signup_pw",
            placeholder="8자 이상 입력하세요",
            help="비밀번호는 8자 이상이어야 합니다.",
        )

        # 비밀번호 확인
        st.text_input(
            "비밀번호 확인 *",
            type="password",
            value=sdata.get("confirmPassword", ""),
            key="signup_pw_confirm",
            placeholder="비밀번호를 다시 입력하세요",
        )

        # 이름
        st.text_input(
            "이름 *",
            value=sdata.get("name", ""),
            key="signup_name",
            placeholder="이름을 입력하세요",
        )

        # 생년월일
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
        # 선택: 나이(생년월일 제공 안할 경우 보조로 사용)
        # st.number_input("나이(선택)", min_value = 0, max_value = 120, key= "signup_age", value = 0)

        # 성별
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

        # 거주지
        st.text_input(
            "거주지 (시군구) *",
            value=sdata.get("location", ""),
            key="signup_location",
            placeholder="예: 서울시 강남구",
        )

        # 건강보험 자격
        st.selectbox(
            "건강보험 자격 *",
            options=HEALTH_INSURANCE_OPTIONS,
            key="signup_health",
            placeholder="선택하세요",
        )

        # 중위소득 대비 소득수준
        st.text_input(
            "중위소득 대비 소득수준 (%) *",
            value=sdata.get("incomeLevel", ""),
            key="signup_income",
            placeholder="예: 50, 100, 150",
            help="중위소득 대비 소득 수준을 백분율로 입력하세요",
        )

        # 기초생활보장 급여
        st.selectbox(
            "기초생활보장 급여 *",
            options=BASIC_LIVELIHOOD_OPTIONS,
            key="signup_basic",
            placeholder="선택하세요",
        )

        # 장애 등급
        disability_map = {"미등록": "0", "심한 장애": "1", "심하지 않은 장애": "2"}
        disability_options = list(disability_map.keys())
        selected_disability = st.selectbox(
            "장애 등급 *",
            options=disability_options,
            key="signup_disability",
            placeholder="선택하세요",
        )

        # 장기요양 등급
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

        # 임신·출산 여부
        pregnancy_options = ["없음", "임신중", "출산후12개월이내"]
        st.selectbox(
            "임신·출산 여부 *",
            options=pregnancy_options,
            key="signup_pregnancy",
            placeholder="선택하세요",
        )

        if err:
            st.error(err)

        submitted = st.form_submit_button(
            "회원가입", use_container_width=True, type="primary"
        )

        if submitted:
            # 아이디는 폼 밖에 있으므로 session_state에서 가져오기
            user_id_value = st.session_state.get("signup_userid", "")

            # 폼 데이터 수집
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

            # 회원가입 처리
            success, message = handle_signup_submit(signup_data)
            if success:
                st.success(message)
                st.rerun()
            else:
                st.session_state["auth_error"]["signup"] = message
                st.rerun()


def render_auth_modal(show_header: bool = True):
    """
    로그인/회원가입 UI를 렌더. show_header=False로 호출하면
    SIMPLECIRCLE 등의 상단 문구(모달 스타일 설명)를 숨김.
    """
    initialize_auth_state()

    if show_header:
        st.markdown("### SIMPLECIRCLE")
        st.markdown("로그인하거나 새 계정을 만드세요")

    login_tab, signup_tab = st.tabs(["로그인", "회원가입"])
    with login_tab:
        render_login_tab()
    with signup_tab:
        render_signup_tab()


# st.tabs는 클릭할 때마다 해당 탭의 컨테이너를 재실행하므로 별도 active_tab 체크 불필요
