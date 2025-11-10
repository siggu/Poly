# 11.10 회원가입 수정
import json
import bcrypt
import time
import logging
import re
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Mock 설정
MOCK_API_DELAY = 0.5

# 데이터 저장 경로
DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.json"
PROFILES_FILE = DATA_DIR / "profiles.json"
COLLECTIONS_FILE = DATA_DIR / "collections.json"

# 데이터 디렉토리 생성
DATA_DIR.mkdir(exist_ok=True)


# === 유틸리티 함수 ===
def load_json(filepath: Path) -> Dict:
    """JSON 파일 로드"""
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"JSON 파일 손상: {filepath}, 새로 생성합니다")
            return {}
    return {}


def save_json(filepath: Path, data: Dict) -> None:
    """JSON 파일 저장"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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

    Args:
        user_id (str): 확인할 사용자 ID

    Returns:
        Tuple[bool, str]: (사용 가능 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        if not user_id or not user_id.strip():
            return False, "아이디를 입력해주세요"

        user_id = user_id.strip()

        # 아이디 형식 검증 (영문, 숫자만 허용, 4-20자)
        if not re.match(r"^[a-zA-Z0-9]{4,20}$", user_id):
            return False, "아이디는 영문, 숫자 조합 4-20자로 입력해주세요"

        # 예약어 체크
        reserved_ids = ["admin", "root", "system", "guest"]
        if user_id.lower() in reserved_ids:
            return False, "사용할 수 없는 아이디입니다"

        # 중복 체크
        users = load_json(USERS_FILE)
        if user_id in users:
            logger.info(f"ID 중복: {user_id}")
            return False, "이미 사용 중인 아이디입니다"

        logger.info(f"ID 사용 가능: {user_id}")
        return True, "사용 가능한 아이디입니다"

    except Exception as e:
        logger.error(f"아이디 확인 중 오류: {str(e)}")
        return False, "확인 중 오류가 발생했습니다"


def api_login(user_id: str, password: str) -> Tuple[bool, str]:
    """
    로그인

    Args:
        user_id (str): 사용자 ID
        password (str): 비밀번호

    Returns:
        Tuple[bool, str]: (로그인 성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        if not user_id or not password:
            return False, "아이디와 비밀번호를 입력해주세요"

        users = load_json(USERS_FILE)

        if user_id not in users:
            logger.warning(f"존재하지 않는 사용자: {user_id}")
            return False, "아이디 또는 비밀번호가 일치하지 않습니다"

        user = users[user_id]

        if not verify_password(password, user["password"]):
            logger.warning(f"비밀번호 불일치: {user_id}")
            return False, "아이디 또는 비밀번호가 일치하지 않습니다"

        # 마지막 로그인 시간 업데이트
        user["last_login"] = datetime.now().isoformat()
        users[user_id] = user
        save_json(USERS_FILE, users)

        logger.info(f"로그인 성공: {user_id}")
        return True, "로그인 성공"

    except Exception as e:
        logger.error(f"로그인 중 오류: {str(e)}")
        return False, "로그인 처리 중 오류가 발생했습니다"


def api_signup(user_id: str, profile_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    회원가입 - Profile과 Collection 분리 저장

    Args:
        user_id (str): 사용자 ID
        profile_data (Dict[str, Any]): 회원가입 데이터
            - userId, password
            - Profile 9개 항목: birthDate, gender, location, healthInsurance,
            incomeLevel, basicLivelihood, disabilityLevel, longTermCare, pregnancyStatus
            - collectionData (선택): diseases, treatments, specialCases

    Returns:
        Tuple[bool, str]: (가입 성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 1. 필수 데이터 검증
        if not user_id or not profile_data.get("password"):
            return False, "필수 정보가 누락되었습니다"

        # 2. 중복 체크 (이중 검증)
        users = load_json(USERS_FILE)
        if user_id in users:
            logger.warning(f"이미 존재하는 사용자: {user_id}")
            return False, "이미 존재하는 아이디입니다"

        # 3. Users 테이블에 인증 정보 저장
        hashed_password = hash_password(profile_data["password"])

        users[user_id] = {
            "password": hashed_password,
            "created_at": datetime.now().isoformat(),
            "last_login": None,
        }
        save_json(USERS_FILE, users)
        logger.info(f"Users 테이블 저장 완료: {user_id}")

        # 4. Profiles 테이블에 고정 9개 항목 저장
        profiles = load_json(PROFILES_FILE)

        # birthDate 처리 (date 객체를 문자열로 변환)
        birth_date = profile_data.get("birthDate")
        if hasattr(birth_date, "isoformat"):
            birth_date_str = birth_date.isoformat()
        else:
            birth_date_str = str(birth_date)

        profiles[user_id] = {
            "name": profile_data.get("name", ""),
            "birthDate": birth_date_str,
            "gender": profile_data.get("gender", ""),
            "location": profile_data.get("location", ""),
            "healthInsurance": profile_data.get("healthInsurance", ""),
            "incomeLevel": int(profile_data.get("incomeLevel", 0)),
            "basicLivelihood": profile_data.get("basicLivelihood", "없음"),
            "disabilityLevel": profile_data.get("disabilityLevel", "0"),
            "longTermCare": profile_data.get("longTermCare", "NONE"),
            "pregnancyStatus": profile_data.get("pregnancyStatus", "없음"),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        save_json(PROFILES_FILE, profiles)
        logger.info(f"Profiles 테이블 저장 완료: {user_id}")

        # 5. Collections 테이블에 의료 정보 저장 (선택사항)
        collection_data = profile_data.get("collectionData")
        if collection_data and any(collection_data.values()):
            collections = load_json(COLLECTIONS_FILE)

            # 사용자별로 배열 형태로 저장
            if user_id not in collections:
                collections[user_id] = []

            collection_entry = {
                "id": len(collections[user_id]) + 1,
                "diseases": collection_data.get("diseases", ""),
                "treatments": collection_data.get("treatments", ""),
                "specialCases": collection_data.get("specialCases", ""),
                "created_at": datetime.now().isoformat(),
            }

            collections[user_id].append(collection_entry)
            save_json(COLLECTIONS_FILE, collections)
            logger.info(f"Collections 테이블 저장 완료: {user_id}")

        logger.info(f"회원가입 완료: {user_id}")
        logger.debug(f"프로필 데이터: {profiles[user_id]}")

        return True, "회원가입이 완료되었습니다"

    except Exception as e:
        logger.error(f"회원가입 중 오류 발생: {str(e)}", exc_info=True)

        # 롤백 처리 (부분 저장 방지)
        try:
            users = load_json(USERS_FILE)
            profiles = load_json(PROFILES_FILE)
            collections = load_json(COLLECTIONS_FILE)

            if user_id in users:
                del users[user_id]
                save_json(USERS_FILE, users)

            if user_id in profiles:
                del profiles[user_id]
                save_json(PROFILES_FILE, profiles)

            if user_id in collections:
                del collections[user_id]
                save_json(COLLECTIONS_FILE, collections)

            logger.info(f"회원가입 실패로 인한 롤백 완료: {user_id}")
        except Exception as rollback_error:
            logger.error(f"롤백 중 오류 발생: {str(rollback_error)}")

        return False, "회원가입 처리 중 오류가 발생했습니다"


def api_get_user_info(user_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    사용자 전체 정보 조회 (Profile + Collection)

    Args:
        user_id (str): 사용자 ID

    Returns:
        Tuple[bool, Dict]: (성공 여부, 사용자 정보)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        profiles = load_json(PROFILES_FILE)
        collections = load_json(COLLECTIONS_FILE)

        if user_id not in profiles:
            logger.warning(f"사용자 정보 없음: {user_id}")
            return False, {}

        user_info = {
            "userId": user_id,
            "profile": profiles[user_id],
            "collections": collections.get(user_id, []),
        }

        logger.info(f"사용자 정보 조회: {user_id}")
        return True, user_info

    except Exception as e:
        logger.error(f"사용자 정보 조회 중 오류: {str(e)}")
        return False, {}


def api_update_profile(user_id: str, profile_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Profile 정보 수정 (9개 항목만)

    Args:
        user_id (str): 사용자 ID
        profile_data (Dict[str, Any]): 수정할 프로필 정보

    Returns:
        Tuple[bool, str]: (성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        profiles = load_json(PROFILES_FILE)

        if user_id not in profiles:
            return False, "사용자를 찾을 수 없습니다"

        # 기존 데이터 유지하면서 업데이트
        current_profile = profiles[user_id]

        # Profile 9개 항목만 업데이트
        allowed_fields = [
            "name",
            "birthDate",
            "gender",
            "location",
            "healthInsurance",
            "incomeLevel",
            "basicLivelihood",
            "disabilityLevel",
            "longTermCare",
            "pregnancyStatus",
        ]

        for field in allowed_fields:
            if field in profile_data:
                current_profile[field] = profile_data[field]

        current_profile["updated_at"] = datetime.now().isoformat()

        profiles[user_id] = current_profile
        save_json(PROFILES_FILE, profiles)

        logger.info(f"Profile 업데이트 완료: {user_id}")
        return True, "프로필이 수정되었습니다"

    except Exception as e:
        logger.error(f"Profile 수정 중 오류: {str(e)}")
        return False, "프로필 수정 중 오류가 발생했습니다"


def api_add_collection(
    user_id: str, collection_data: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Collection 정보 추가

    Args:
        user_id (str): 사용자 ID
        collection_data (Dict): 의료 정보
            - diseases, treatments, specialCases

    Returns:
        Tuple[bool, str]: (성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 사용자 존재 확인
        profiles = load_json(PROFILES_FILE)
        if user_id not in profiles:
            return False, "사용자를 찾을 수 없습니다"

        collections = load_json(COLLECTIONS_FILE)

        if user_id not in collections:
            collections[user_id] = []

        collection_entry = {
            "id": len(collections[user_id]) + 1,
            "diseases": collection_data.get("diseases", ""),
            "treatments": collection_data.get("treatments", ""),
            "specialCases": collection_data.get("specialCases", ""),
            "created_at": datetime.now().isoformat(),
        }

        collections[user_id].append(collection_entry)
        save_json(COLLECTIONS_FILE, collections)

        logger.info(f"Collection 추가 완료: {user_id}, entry #{collection_entry['id']}")
        return True, "의료 정보가 추가되었습니다"

    except Exception as e:
        logger.error(f"Collection 추가 중 오류: {str(e)}")
        return False, "정보 추가 중 오류가 발생했습니다"


def api_reset_password(
    user_id: str, current_password: str, new_password: str
) -> Tuple[bool, str]:
    """
    비밀번호 재설정

    Args:
        user_id (str): 사용자 ID
        current_password (str): 현재 비밀번호
        new_password (str): 새 비밀번호

    Returns:
        Tuple[bool, str]: (성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        users = load_json(USERS_FILE)

        if user_id not in users:
            return False, "사용자를 찾을 수 없습니다"

        user = users[user_id]

        # 현재 비밀번호 확인
        if not verify_password(current_password, user["password"]):
            logger.warning(f"비밀번호 변경 실패 - 현재 비밀번호 불일치: {user_id}")
            return False, "현재 비밀번호가 일치하지 않습니다"

        # 새 비밀번호 해싱 및 저장
        user["password"] = hash_password(new_password)
        user["password_updated_at"] = datetime.now().isoformat()
        users[user_id] = user
        save_json(USERS_FILE, users)

        logger.info(f"비밀번호 변경 완료: {user_id}")
        return True, "비밀번호가 변경되었습니다"

    except Exception as e:
        logger.error(f"비밀번호 재설정 중 오류 발생: {str(e)}")
        return False, "비밀번호 변경 중 오류가 발생했습니다"


def api_delete_account(user_id: str) -> Tuple[bool, str]:
    """
    회원 탈퇴 (모든 데이터 삭제)

    Args:
        user_id (str): 사용자 ID

    Returns:
        Tuple[bool, str]: (성공 여부, 메시지)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 1. Users 테이블에서 삭제
        users = load_json(USERS_FILE)
        if user_id in users:
            del users[user_id]
            save_json(USERS_FILE, users)

        # 2. Profiles 테이블에서 삭제
        profiles = load_json(PROFILES_FILE)
        if user_id in profiles:
            del profiles[user_id]
            save_json(PROFILES_FILE, profiles)

        # 3. Collections 테이블에서 삭제
        collections = load_json(COLLECTIONS_FILE)
        if user_id in collections:
            del collections[user_id]
            save_json(COLLECTIONS_FILE, collections)

        logger.info(f"회원 탈퇴 완료: {user_id}")
        return True, "회원 탈퇴가 완료되었습니다"

    except Exception as e:
        logger.error(f"회원 탈퇴 중 오류 발생: {str(e)}")
        return False, "회원 탈퇴 처리 중 오류가 발생했습니다"


def api_send_chat_message(
    user_id: str, message: str, user_profile: Optional[Dict] = None
) -> Tuple[bool, Dict]:
    """
    챗봇 메시지 전송 (Mock)

    Args:
        user_id (str): 사용자 ID
        message (str): 사용자 메시지
        user_profile (Dict): 사용자 프로필 정보 (선택)

    Returns:
        Tuple[bool, Dict]: (성공 여부, 응답 데이터)
    """
    try:
        time.sleep(MOCK_API_DELAY)

        # 프로필 정보가 없으면 DB에서 가져오기
        if not user_profile:
            profiles = load_json(PROFILES_FILE)
            user_profile = profiles.get(user_id, {})

        logger.info(f"챗봇 메시지 전송: {user_id} - {message[:50]}")

        return True, {
            "content": "고객님의 조건에 맞는 정책을 찾았습니다.",
            "policies": [
                {
                    "id": "1",
                    "title": "청년 월세 지원",
                    "description": "만 19세~34세 청년의 주거비 부담을 덜어주기 위한 월세 지원 정책입니다.",
                    "eligibility": "만 19~34세, 소득 기준 충족, 서울시 거주",
                    "benefits": "월 최대 20만원 지원 (최대 12개월)",
                    "applicationUrl": "https://example.com/apply",
                    "isEligible": True,
                }
            ],
        }
    except Exception as e:
        logger.error(f"메시지 전송 중 오류 발생: {str(e)}")
        return False, {"error": "메시지 전송 중 오류가 발생했습니다"}


def api_get_chat_history(user_id: str, limit: int = 10) -> Tuple[bool, list]:
    """
    채팅 내역 조회 (Mock)

    Args:
        user_id (str): 사용자 ID
        limit (int): 조회할 내역 수

    Returns:
        Tuple[bool, list]: (성공 여부, 채팅 내역 리스트)
    """
    try:
        time.sleep(MOCK_API_DELAY)
        logger.info(f"채팅 내역 조회: {user_id}, limit={limit}")
        # 실제 구현에서는 채팅 내역 DB에서 조회
        return True, []
    except Exception as e:
        logger.error(f"채팅 내역 조회 중 오류 발생: {str(e)}")
        return False, []
