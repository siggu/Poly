"""
Streamlit UI와 FastAPI 백엔드 API 간의 통신을 담당하는 서비스 계층입니다.
DB나 LLM 로직을 직접 처리하지 않고, 모두 HTTP 요청을 통해 FastAPI 서버에 위임합니다.
11.13 수정
"""

import os
from typing import List, Dict, Any, Optional, Iterator, Tuple
import requests

# FastAPI 서버의 기본 URL (개발 환경 기준)
# 실제 환경에서는 환경 변수를 통해 관리해야 합니다.
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://localhost:8000")


class BackendService:
    """
    FastAPI 서버와 통신하는 HTTP 클라이언트 역할 수행.
    """

    _instance: Optional["BackendService"] = None

    def __init__(self):
        # HTTP 클라이언트 초기화 (requests 세션을 사용할 수도 있지만 여기서는 간단하게 처리)
        pass

    @classmethod
    def get_instance(cls) -> "BackendService":
        if cls._instance is None:
            cls._instance = BackendService()
        return cls._instance

    def health_check(self) -> Dict[str, Any]:
        """FastAPI 서버의 상태를 확인합니다."""
        url = f"{FASTAPI_BASE_URL}/health"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()  # 4xx, 5xx 에러 시 예외 발생
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "error", "message": f"백엔드 연결 실패: {e}"}

    def send_chat_message(
        self,
        session_id: Optional[str],
        user_input: str,
        token: Optional[str] = None,  # 인증 토큰
        user_action: str = "none",
        profile_id: Optional[int] = None,  # 👈 프로필 ID 추가
    ) -> Dict[str, Any]:
        """
        새로운 통합 /api/chat 엔드포인트로 채팅 메시지를 전송합니다.
        스트리밍을 사용하지 않고 전체 응답을 한 번에 받습니다.
        """
        url = f"{FASTAPI_BASE_URL}/api/v1/chat"

        # profile_id가 제공되지 않은 경우에만 API에서 가져옴
        if profile_id is None and token:
            ok, user_profile = self.get_user_profile(token)
            if ok:
                profile_id = user_profile.get("main_profile_id")

        payload = {
            "session_id": session_id,
            "profile_id": profile_id,  # 👈 요청 payload에 포함
            "user_input": user_input,
            "user_action": user_action,
            "client_meta": {
                "ui_lang": "ko",
                "app_version": "streamlit-v1"
            }
        }
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"채팅 API 요청 중 오류 발생: {e}"
            print(error_msg)
            return {
                "session_id": session_id,
                "answer": f"오류: {error_msg}",
                "session_ended": False,
                "save_result": None,
                "debug": {},
            }

    # ==============================================================================
    # 사용자 인증 및 프로필 API 호출
    # ==============================================================================
    # 11.18 수정: 회원가입 시 빈 문자열 처리를 개선.
    def register_user(self, user_data: Dict[str, Any]) -> Tuple[bool, str]:
        """회원가입 API를 호출합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/register"

        # 11.18 수정: 빈 문자열 값을 None으로 변환하여 백엔드로 전송
        # 이렇게 해야 DB에 NULL로 저장되어 의도치 않은 기본값 설정을 방지할 수 있습니다.
        payload = {}
        for key, value in user_data.items():
            payload[key] = value if value != "" else None

        # 필수 필드는 payload에 다시 한 번 확실하게 할당합니다.
        payload["username"] = user_data.get("username")
        payload["name"] = user_data.get("name")
        payload["password"] = user_data.get("password")

        # median_income_ratio는 0이 유효한 값이므로 빈 문자열일 때만 None으로 처리
        if user_data.get("median_income_ratio") == "":
            payload["median_income_ratio"] = None
        else:
            payload["median_income_ratio"] = user_data.get("median_income_ratio")
        # ===========================================================================
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 201:
                return True, response.json().get("message", "회원가입에 성공했습니다.")
            else:
                error_detail = response.json().get("detail", "알 수 없는 오류")
                return False, f"회원가입 실패: {error_detail}"
        except requests.exceptions.RequestException as e:
            return False, f"백엔드 연결 실패: {e}"

    def login_user(self, username: str, password: str) -> Tuple[bool, Any]:
        """로그인 API를 호출하고 성공 시 토큰을 반환합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/login"
        print(f"DEBUG: Attempting to log in to: {url}") # 디버그용 출력 추가
        payload = {"username": username, "password": password}
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                return (
                    True,
                    response.json(),
                )  # {"access_token": "...", "token_type": "bearer"}
            else:
                error_detail = response.json().get("detail", "로그인 실패")
                return False, error_detail
        except requests.exceptions.RequestException as e:
            return False, f"백엔드 연결 실패: {e}"

    def check_id_availability(self, username: str) -> Tuple[bool, str]:
        """아이디 사용 가능 여부를 확인하는 API를 호출합니다."""
        if not username:
            return False, "아이디를 입력해주세요."

        url = f"{FASTAPI_BASE_URL}/api/v1/user/check-id/{username}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return True, response.json().get("message", "사용 가능한 아이디입니다.")
            else:
                # 409 Conflict (이미 존재) 또는 다른 오류
                error_detail = response.json().get("detail", "이미 사용 중인 아이디입니다.")
                return False, error_detail
        except requests.exceptions.RequestException as e:
            return False, f"백엔드 연결 실패: {e}"

    def get_user_profile(self, token: str) -> Tuple[bool, Any]:
        """인증된 사용자의 프로필 정보를 가져옵니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/profile"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            return False, f"프로필 조회 실패: {e}"

    def get_all_profiles(self, token: str) -> Tuple[bool, Any]:
        """인증된 사용자의 모든 프로필 목록을 가져옵니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/profiles"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            return False, f"전체 프로필 조회 실패: {e}"

    def add_profile(self, token: str, profile_data: Dict[str, Any]) -> Tuple[bool, Any]:
        """새로운 프로필을 추가합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/profile"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.post(
                url, json=profile_data, headers=headers, timeout=10
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            return False, f"프로필 추가 실패: {e}"

    def update_user_profile(
        self, token: str, profile_id: int, update_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """사용자 프로필을 수정합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/profile/{profile_id}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.patch(
                url, json=update_data, headers=headers, timeout=10
            )
            response.raise_for_status()
            return True, response.json().get("message", "성공적으로 수정되었습니다.")
        except requests.exceptions.RequestException as e:
            return False, f"프로필 수정 실패: {e}"

    def delete_profile(self, token: str, profile_id: int) -> Tuple[bool, str]:
        """특정 프로필을 삭제합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/profile/{profile_id}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.delete(url, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json().get("message", "성공적으로 삭제되었습니다.")
        except requests.exceptions.RequestException as e:
            return False, f"프로필 삭제 실패: {e}"

    def set_main_profile(
        self, token: str, profile_id: Optional[int]
    ) -> Tuple[bool, str]:
        """메인 프로필을 변경합니다."""

        # 🔥 profile_id 유효성 검사 추가
        if profile_id is None:
            return False, "프로필 ID가 제공되지 않았습니다."

        if not isinstance(profile_id, int) or profile_id <= 0:
            return False, f"유효하지 않은 프로필 ID입니다: {profile_id}"

        url = f"{FASTAPI_BASE_URL}/api/v1/user/profile/main/{profile_id}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.put(url, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json().get("message", "메인 프로필이 변경되었습니다.")
        except requests.exceptions.RequestException as e:
            return False, f"메인 프로필 변경 실패: {e}"

    def delete_user_account(self, token: str) -> Tuple[bool, str]:
        """사용자 계정을 삭제합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/delete"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.delete(url, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json().get("message", "계정이 삭제되었습니다.")
        except requests.exceptions.RequestException as e:
            return False, f"계정 삭제 실패: {e}"

    def reset_password(
        self, token: str, current_password: str, new_password: str
    ) -> Tuple[bool, str]:
        """비밀번호를 재설정합니다."""
        # 참고: 이 API는 아직 user.py에 구현되지 않았습니다. 추가 구현이 필요합니다.
        url = f"{FASTAPI_BASE_URL}/api/v1/user/password"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"current_password": current_password, "new_password": new_password}
        try:
            response = requests.put(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            return True, response.json().get("message", "비밀번호가 변경되었습니다.")
        except requests.exceptions.RequestException as e:
            return False, f"비밀번호 변경 실패: {e}"

    # 여기에 DB 관련 로직을 호출하는 다른 메서드들을 추가합니다.
    # (예: get_chat_history, save_chat_message 등)


def get_backend_service() -> BackendService:
    """BackendService의 싱글톤 인스턴스를 가져옵니다."""
    return BackendService.get_instance()


# 편의를 위해 전역 인스턴스를 생성하여 바로 호출할 수 있도록 합니다.
backend_service = get_backend_service()
