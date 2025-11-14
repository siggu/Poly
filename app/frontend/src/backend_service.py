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

    def get_llm_response(
        self,
        history_messages: List[Dict[str, Any]],
        user_message: str,
        active_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        LLM 응답을 요청합니다 (일반 비스트리밍 요청).
        FastAPI의 /api/v1/chat/generate 엔드포인트를 호출합니다.
        """
        url = f"{FASTAPI_BASE_URL}/api/v1/chat/generate"
        payload = {
            "history_messages": history_messages,
            "user_message": user_message,
            "active_profile": active_profile,
        }

        try:
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"LLM 응답 요청 중 오류 발생: {e}"
            print(error_msg)
            return {"content": f"오류: {error_msg}"}

    def get_llm_response_stream(
        self,
        history_messages: List[Dict[str, Any]],
        user_message: str,
        active_profile: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """
        LLM 응답 스트리밍을 요청합니다.
        FastAPI의 /api/v1/chat/stream 엔드포인트를 호출합니다.
        """
        url = f"{FASTAPI_BASE_URL}/api/v1/chat/stream"
        payload = {
            "history_messages": history_messages,
            "user_message": user_message,
            "active_profile": active_profile,
        }

        try:
            # stream=True 설정으로 응답이 들어올 때마다 청크 단위로 처리
            with requests.post(url, json=payload, stream=True, timeout=60) as response:
                response.raise_for_status()
                for chunk in response.iter_content(
                    chunk_size=None, decode_unicode=True
                ):
                    if chunk:
                        yield chunk
        except requests.exceptions.RequestException as e:
            error_msg = f"LLM 스트림 요청 중 오류 발생: {e}"
            print(error_msg)
            yield f"\n\n오류: {error_msg}"

    # ==============================================================================
    # 사용자 인증 및 프로필 API 호출
    # ==============================================================================

    def register_user(self, user_data: Dict[str, Any]) -> Tuple[bool, str]:
        """회원가입 API를 호출합니다."""
        url = f"{FASTAPI_BASE_URL}/api/v1/user/register"
        payload = {
            "username": user_data.get("username"),
            "password": user_data.get("password"),
            
        }
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
        payload = {"username": username, "password": password}
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                return True, response.json()  # {"access_token": "...", "token_type": "bearer"}
            else:
                error_detail = response.json().get("detail", "로그인 실패")
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
            response = requests.post(url, json=profile_data, headers=headers, timeout=10)
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
            response = requests.patch(url, json=update_data, headers=headers, timeout=10)
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

    def set_main_profile(self, token: str, profile_id: int) -> Tuple[bool, str]:
        """메인 프로필을 변경합니다."""
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
