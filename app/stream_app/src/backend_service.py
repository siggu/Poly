"""11.12 백엔드 서비스 API 함수들 - 데이터베이스 직접 접근 방식"""

import bcrypt
import time
import logging
import re
from typing import Dict, Any, Tuple, Optional, List


# DB 접근 함수 임포트 (상대 경로 사용)
try:
    from src.db import database

    # database 모듈은 여전히 필요할 수 있음 (예: api_get_profiles)
except ImportError:
    # 순환 import 방지: database 모듈이 없을 경우를 대비
    database = None

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Mock 설정
MOCK_API_DELAY = 0.5


# === 유틸리티 함수 ===
def hash_password(password: str) -> str:
    """비밀번호 해싱 (bcrypt)"""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """비밀번호 검증"""
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# === API 함수 ===
def api_check_id_availability(user_id: str) -> Tuple[bool, str]:
    """
    아이디 중복 확인
    """
    try:
        time.sleep(MOCK_API_DELAY)
        user_id = user_id.strip()

        # 아이디 형식 검증 (영문, 숫자만 허용, 4-20자)
        if not re.match(r"^[a-zA-Z0-9]{4,20}$", user_id):
            return False, "아이디는 영문, 숫자 조합 4-20자로 입력해주세요"

        reserved_ids = ["admin", "root", "system", "guest"]
        if user_id.lower() in reserved_ids:
            return False, "사용할 수 없는 아이디입니다"

        # [수정] DB에서 사용자 존재 여부 확인
        if database.check_user_exists(user_id):
            return False, "이미 사용 중인 아이디입니다"

        return True, "사용 가능한 아이디입니다"

    except Exception as e:
        logger.error(f"아이디 확인 중 오류: {str(e)}")
        return False, "확인 중 오류가 발생했습니다"


def api_signup(user_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    회원가입 - DB에 사용자 및 프로필 정보 저장
    """
    try:
        time.sleep(MOCK_API_DELAY)
        # auth.py의 handle_signup_submit과 유사하게 DB 함수를 직접 호출
        success, message = database.create_user_and_profile(user_data)
        if success:
            logger.info(f"회원가입 완료: {user_data.get('username')}")
        else:
            logger.warning(f"회원가입 실패: {user_data.get('username')} - {message}")
        return success, message
    except Exception as e:
        logger.error(f"회원가입 API 처리 중 오류 발생: {str(e)}")
        return False, "회원가입 처리 중 오류가 발생했습니다."


def api_get_user_info(user_uuid: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    사용자 전체 정보 조회 (Profile). DB 직접 조회 함수를 호출합니다.
    """
    try:
        time.sleep(MOCK_API_DELAY)
        # [수정] database.py의 get_user_and_profile_by_id 함수를 직접 사용합니다.
        success, user_info = database.get_user_and_profile_by_id(user_uuid)
        if success:
            logger.info(f"사용자 정보 조회 성공: {user_uuid}")
            # 기존 형식과 맞추기 위해 profile 키를 추가합니다.
            return True, {
                "userId": user_info.get("username"),
                "main_profile_id": user_info.get("main_profile_id"),
                "profile": user_info,
            }
        else:
            logger.warning(f"사용자 정보 조회 실패: {user_uuid}")
            return False, None
    except Exception as e:
        logger.error(f"사용자 정보 조회 중 오류: {str(e)}")
        return False, None


def api_save_profiles(
    user_id: str, profiles_list: list
) -> Tuple[bool, str, Optional[List[Dict[str, Any]]]]:
    """
    사용자별 다중 프로필 리스트 저장
    """
    try:
        time.sleep(MOCK_API_DELAY)
        if not isinstance(profiles_list, list):
            return False, "프로필 형식이 올바르지 않습니다", None

        # 프로필 정규화(직렬화 가능한 형태로 변환) - (동일)
        def _sanitize_profile(p: Dict[str, Any]) -> Dict[str, Any]:
            q = dict(p) if isinstance(p, dict) else {}
            bd = q.get("birthDate")
            try:
                if hasattr(bd, "isoformat"):
                    q["birthDate"] = bd.isoformat()[:10]
                elif isinstance(bd, str):
                    q["birthDate"] = bd
                else:
                    q["birthDate"] = ""
            except Exception:
                q["birthDate"] = str(bd) if bd is not None else ""
            try:
                q["incomeLevel"] = int(q.get("incomeLevel", 0))
            except Exception:
                pass
            q.setdefault("gender", "")
            q.setdefault("location", "")
            q.setdefault("name", "")  # 이름 필드 추가
            q.setdefault("healthInsurance", "")
            q.setdefault("basicLivelihood", "없음")
            q.setdefault("disabilityLevel", "0")
            q.setdefault("longTermCare", "NONE")
            q.setdefault("pregnancyStatus", "없음")
            q.setdefault("id", "")
            q.setdefault("isActive", False)
            return q

        sanitized = [_sanitize_profile(p) for p in profiles_list]

        # [수정] 프로필 목록을 순회하며 DB에 추가 또는 업데이트
        for profile in sanitized:
            profile_id = profile.get("id")
            # id가 숫자(BIGINT)이면 기존 프로필, 아니면 신규 프로필로 간주
            if isinstance(profile_id, int):
                # 기존 프로필 업데이트
                success = database.update_profile(profile_id, profile)
                if not success:
                    raise Exception(f"프로필 업데이트 실패: id={profile_id}")
            else:
                # 신규 프로필 추가
                success, new_id = database.add_profile(user_id, profile)
                if not success:
                    raise Exception("새 프로필 추가 실패")

        # 모든 변경사항이 DB에 반영된 후, 최신 프로필 목록을 DB에서 다시 가져옵니다.
        ok, updated_profiles_from_db = database.get_all_profiles_by_user_id(user_id)
        if not ok:
            raise Exception("업데이트된 프로필 목록을 가져오는 데 실패했습니다.")

        logger.info(
            f"사용자 프로필 리스트 저장 완료 및 새로고침: {user_id} ({len(updated_profiles_from_db)}개)"
        )
        return True, "프로필이 저장되었습니다", updated_profiles_from_db
    except Exception as e:
        logger.error(f"사용자 프로필 리스트 저장 중 오류: {str(e)}")
        return False, "프로필 저장 중 오류가 발생했습니다.", None


def api_get_all_profiles_by_user_id(
    user_uuid: str,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    특정 사용자의 모든 프로필 목록을 조회합니다.
    """
    try:
        time.sleep(MOCK_API_DELAY)
        success, profiles = database.get_all_profiles_by_user_id(user_uuid)
        if success:
            logger.info(f"모든 프로필 조회 성공: {user_uuid}")
        return success, profiles
    except Exception as e:
        logger.error(f"모든 프로필 조회 중 오류 발생: {str(e)}")
        return False, []


def api_update_user_main_profile_id(
    user_uuid: str, profile_id: Optional[int]
) -> Tuple[bool, str]:
    """사용자의 기본 프로필 ID를 업데이트합니다."""
    try:
        time.sleep(MOCK_API_DELAY)
        success, message = database.update_user_main_profile_id(user_uuid, profile_id)
        if success:
            logger.info(
                f"사용자 기본 프로필 업데이트 성공: {user_uuid} -> {profile_id}"
            )
        return success, message
    except Exception as e:
        logger.error(f"사용자 기본 프로필 업데이트 중 오류 발생: {str(e)}")
        return False, "기본 프로필 업데이트 중 오류가 발생했습니다."


def api_delete_profile(profile_id: int) -> Tuple[bool, str]:
    """
    특정 프로필 ID를 사용하여 프로필을 삭제합니다.
    """
    try:
        time.sleep(MOCK_API_DELAY)
        success = database.delete_profile_by_id(profile_id)
        if success:
            logger.info(f"프로필 삭제 API 성공: profile_id={profile_id}")
            return True, "프로필이 삭제되었습니다."
        return False, "프로필 삭제 중 오류가 발생했습니다."
    except Exception as e:
        logger.error(f"프로필 삭제 API 처리 중 오류 발생: {str(e)}")
        return False, "프로필 삭제 처리 중 오류가 발생했습니다."


def api_update_profile(user_id: str, profile_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Profile 정보 수정 (9개 항목만)
    """
    # 이 함수는 단일 프로필만 가정하므로, 다중 프로필을 지원하는
    # api_save_profiles를 사용하는 것이 더 적합합니다.
    # 하지만 호환성을 위해 남겨두고, 실제로는 update_profile DB 함수를 호출하도록 수정합니다.
    profile_id = profile_data.get("id")
    if not isinstance(profile_id, int):
        return False, "프로필 ID가 유효하지 않습니다."
    success = database.update_profile(profile_id, profile_data)
    return (
        (True, "프로필이 수정되었습니다.")
        if success
        else (False, "프로필 수정 중 오류가 발생했습니다.")
    )


def api_add_collection(
    user_id: str, collection_data: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Collection 정보 추가
    """
    # 이 함수는 파일 기반 시스템의 잔재로 보입니다.
    # 현재 DB 스키마에서는 'collections' 테이블에 직접 추가하는 로직이 필요합니다.
    # 지금은 사용되지 않으므로 경고를 로깅하고 성공을 반환합니다.
    logger.warning("api_add_collection은 더 이상 사용되지 않는 함수입니다.")
    return True, "정보가 추가되었습니다 (동작 없음)."


def api_reset_password(
    user_uuid: str, current_password: str, new_password: str
) -> Tuple[bool, str]:
    """
    비밀번호 재설정
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 1. DB에서 사용자 정보 조회 (UUID 기반)
        ok, user_info = database.get_user_and_profile_by_id(user_uuid)
        if not ok:
            return False, "사용자 정보를 찾을 수 없습니다."

        # 2. 현재 비밀번호 확인
        stored_hash = database.get_user_password_hash(user_info.get("username"))
        if not stored_hash or not verify_password(current_password, stored_hash):
            logger.warning(f"비밀번호 변경 실패 - 현재 비밀번호 불일치: {user_uuid}")
            return False, "현재 비밀번호가 일치하지 않습니다."

        # 3. DB에 새 비밀번호 해시 업데이트
        new_password_hash = hash_password(new_password)
        success, msg = database.update_user_password(user_uuid, new_password_hash)
        logger.info(f"비밀번호 변경 완료: {user_uuid}")
        return success, msg

    except Exception as e:
        logger.error(f"비밀번호 재설정 중 오류 발생: {str(e)}")
        return False, "비밀번호 변경 중 오류가 발생했습니다"


def api_delete_user_account(user_uuid: str) -> Tuple[bool, str]:
    """
    사용자 계정 삭제
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 데이터베이스 함수 직접 호출
        success, message = database.delete_user_account(user_uuid)

        if success:
            logger.info(f"회원 탈퇴 완료: {user_uuid}")
        else:
            logger.error(f"회원 탈퇴 실패: {user_uuid} - {message}")
        return success, message
    except Exception as e:
        logger.error(f"회원 탈퇴 API 처리 중 오류 발생: {str(e)}")
        return False, "회원 탈퇴 처리 중 오류가 발생했습니다."


def api_send_chat_message(
    user_id: str, message: str, user_profile: Optional[Dict] = None
) -> Tuple[bool, Dict]:
    """
    챗봇 메시지 전송 (Mock)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # [수정] user_profile이 없으면 user_id(UUID)로 DB에서 조회
        if not user_profile:
            _, user_profile_from_db = database.get_user_and_profile_by_id(user_id)
            user_profile = user_profile_from_db or {}

        logger.info(f"챗봇 메시지 전송: {user_id} - {message[:50]}")
        return True
    except Exception as e:
        logger.error(f"메시지 전송 중 오류 발생: {str(e)}")
        return False, {"error": "메시지 전송 중 오류가 발생했습니다"}


def api_get_chat_history(user_id: str, limit: int = 10) -> Tuple[bool, list]:
    """
    채팅 내역 조회 (Mock)
    """
    try:
        time.sleep(MOCK_API_DELAY)
        logger.info(f"채팅 내역 조회: {user_id}, limit={limit}")
        # 실제 구현에서는 채팅 내역 DB에서 조회
        return True, []
    except Exception as e:
        logger.error(f"채팅 내역 조회 중 오류 발생: {str(e)}")
        return False, []
