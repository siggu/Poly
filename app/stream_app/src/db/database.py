"""PostgreSQL 데이터베이스 연결 및 CRUD 함수"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime, date
import logging
import uuid  # users.id에 사용할 고유 ID 생성을 위해 추가

logger = logging.getLogger(__name__)

# DB 연결 정보 (환경변수 또는 하드코딩)
# 🚨 주의: 비밀번호 'test1234'는 실제 배포 시 반드시 환경 변수로 변경해야 합니다.
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "140.238.10.51"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "team02"),
    "user": os.getenv("DB_USER", "test01"),
    "password": os.getenv("DB_PASSWORD", "test1234"),
}


def get_db_connection():
    """PostgreSQL DB 연결 객체를 반환합니다."""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            client_encoding="UTF8",  # 한글 처리를 위한 인코딩 설정
        )
        return conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        return None


def _normalize_birth_date(birth_date: Any) -> Optional[str]:
    """birthDate를 YYYY-MM-DD 문자열로 변환"""
    if birth_date is None:
        return None
    if isinstance(birth_date, date):
        return birth_date.isoformat()
    if isinstance(birth_date, str):
        # 이미 YYYY-MM-DD 형식인지 확인
        if len(birth_date) >= 10:
            return birth_date[:10]
        return birth_date
    return str(birth_date)


def _normalize_insurance_type(insurance_str: str) -> Optional[str]:
    """건강보험 종류를 DB 형식으로 변환"""
    if not insurance_str:
        return None
    # DB enum에 한글 값이 직접 저장되어 있으므로 변환 없이 그대로 반환
    return insurance_str


def _normalize_benefit_type(benefit_str: str) -> str:
    """기초생활보장 급여 종류를 DB 형식으로 변환"""
    if not benefit_str or benefit_str == "없음":
        return "NONE"
    mapping = {
        "생계": "LIVELIHOOD",
        "의료": "MEDICAL",
        "주거": "HOUSING",
        "교육": "EDUCATION",
    }
    return mapping.get(benefit_str, "NONE")


def _normalize_sex(gender: str) -> Optional[str]:
    """성별을 DB 형식으로 변환 (남성->M, 여성->F 등)"""
    if not gender:
        return None
    gender_lower = gender.lower()
    if "남" in gender_lower or "male" in gender_lower or "m" == gender_lower:
        return "M"
    if "여" in gender_lower or "female" in gender_lower or "f" == gender_lower:
        return "F"
    return gender[:1].upper() if gender else None


def _normalize_disability_grade(disability_level: Any) -> Optional[int]:
    """장애 등급을 정수로 변환"""
    if not disability_level or str(disability_level) in ("0", "미등록"):
        return None
    try:
        return int(disability_level)
    except (ValueError, TypeError):
        return None


def _normalize_ltci_grade(long_term_care: str) -> str:
    """장기요양 등급 정규화"""
    if not long_term_care or long_term_care in ("없음", "해당없음", "NONE"):
        return "NONE"
    return long_term_care.upper()


def _normalize_pregnant_status(pregnancy_status: str) -> Optional[bool]:
    """임신/출산 여부를 Boolean으로 변환"""
    if not pregnancy_status:
        return None
    status_lower = pregnancy_status.lower()
    if (
        "임신" in status_lower
        or "출산" in status_lower
        or status_lower in ("true", "t")
    ):
        return True
    return False


def _normalize_income_ratio(income_level: Any) -> Optional[float]:
    """소득 수준을 NUMERIC(5,2)로 변환"""
    if income_level is None:
        return None
    try:
        val = float(income_level)
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def create_user_and_profile(user_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    새로운 사용자의 인증 정보 (users), 기본 프로필 (profiles),
    및 초기 컬렉션 (collections) 정보를 트랜잭션으로 삽입합니다.

    Args:
        user_data: 회원가입 폼 데이터 (username, password, profile, collection 포함)

    Returns:
        (성공 여부, 메시지)
    """
    conn = get_db_connection()
    if not conn:
        return False, "데이터베이스 연결 실패"

    # 폼에서 받은 데이터 분리 및 정규화
    username = user_data.get("username", "").strip()
    password = user_data.get(
        "password", ""
    ).strip()  # 평문 비밀번호 (backend_service에서 해싱 필요)

    if not username or not password:
        return False, "아이디와 비밀번호는 필수 입력 항목입니다."

    # 🚨 주의: 이 로직은 `backend_service.py`에서 호출될 때 비밀번호가 이미 해싱되었다고 가정합니다.
    # 안전을 위해 password_hash로 변수 이름을 변경합니다.
    password_hash = password  # 임시, 실제로는 해시된 값이어야 함

    # users.id는 TEXT 타입이므로 UUID를 사용
    new_user_id = str(uuid.uuid4())

    try:
        with conn.cursor() as cursor:
            # 1. users 테이블 INSERT (인증 정보)
            # users 테이블의 ID는 TEXT(UUID)입니다.
            # main_profile_id는 profiles 테이블이 생성된 후 업데이트할 예정이므로 NULL로 둡니다.
            user_insert_query = """
            INSERT INTO users (id, username, password_hash, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW());
            """
            # 아이디 중복 확인은 이 쿼리의 무결성 제약 조건(UNIQUE INDEX on username)에 의해 처리됩니다.
            cursor.execute(user_insert_query, (new_user_id, username, password_hash))
            logger.info(f"1. users 테이블에 삽입 완료. user_id: {new_user_id}")

            # 2. profiles 테이블 INSERT (기본 프로필)
            # users.id를 profiles.user_id로 사용하고, profiles.id(BIGINT)를 RETURNING으로 받습니다.

            # --- 프로필 데이터 정규화 ---
            birth_date_str = _normalize_birth_date(user_data.get("birthDate"))
            sex = _normalize_sex(user_data.get("gender", ""))
            # 실제 스키마 필드명에 맞게 user_data의 키를 변경
            residency_sgg = user_data.get("residency_sgg", "").strip() or None
            insurance_type = _normalize_insurance_type(
                user_data.get("insurance_type", "")
            )
            median_income = _normalize_income_ratio(user_data.get("median_income"))
            basic_benefit_type = _normalize_benefit_type(
                user_data.get("basic_benefit_type", "없음")
            )
            disability_grade = _normalize_disability_grade(
                user_data.get("disability_grade", "0")
            )
            ltci_grade = _normalize_ltci_grade(user_data.get("ltci_grade", "NONE"))
            pregnant_or_postpartum = _normalize_pregnant_status(
                user_data.get("pregnant_or_postpartum", "없음")
            )

            profile_insert_query = """
            INSERT INTO profiles (
                user_id, birth_date, sex, residency_sgg, insurance_type,
                median_income, basic_benefit_type, disability_grade,
                ltci_grade, pregnant_or_postpartum, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id; 
            """

            profile_data_tuple = (
                new_user_id,
                birth_date_str,
                sex,
                residency_sgg,
                insurance_type,
                median_income,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum,
            )

            cursor.execute(profile_insert_query, profile_data_tuple)
            new_profile_id = cursor.fetchone()[0]  # profiles.id 획득 (BIGINT)
            logger.info(f"2. profiles 테이블에 삽입 완료. profile_id: {new_profile_id}")

            # 3. collections 테이블 INSERT (초기 멀티 프로필 데이터)
            # profiles.id를 collections.profile_id로 사용합니다.

            # 컬렉션 데이터 (예시로 기본값 또는 폼에서 받은 초기 값 사용)
            collection_data = user_data.get(
                "initial_collection",
                {"subject": "기본", "predicate": "상태", "object": "정상"},
            )

            collection_insert_query = """
            INSERT INTO collections (
                profile_id, subject, predicate, object,
                code_system, code, onset_date, end_date,
                negation, confidence, source_id, created_at
            )
            VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, FALSE, 1.0, NULL, NOW());
            """

            # subject, predicate, object 만 사용하고 나머지는 NULL 또는 기본값 사용
            collection_data_tuple = (
                new_profile_id,
                collection_data.get("subject"),
                collection_data.get("predicate"),
                collection_data.get("object"),
            )

            cursor.execute(collection_insert_query, collection_data_tuple)
            logger.info(f"3. collections 테이블에 삽입 완료.")

            # 4. users 테이블의 main_profile_id 업데이트 (옵션)
            # 기본 프로필이 생성되었으므로, users 테이블에 main_profile_id를 연결
            update_user_query = """
            UPDATE users SET main_profile_id = %s, updated_at = NOW()
            WHERE id = %s;
            """
            cursor.execute(update_user_query, (new_profile_id, new_user_id))
            logger.info("4. users 테이블 main_profile_id 업데이트 완료.")

            # ✅ 최종 성공: 모든 쿼리가 성공했으므로 커밋
            conn.commit()
            return True, "회원가입 및 전체 프로필 설정이 성공적으로 완료되었습니다."

    except psycopg2.IntegrityError as e:
        conn.rollback()
        # username unique constraint 위반 시
        if "users_username_key" in str(e):
            return False, "이미 사용 중인 아이디입니다."
        logger.warning(f"프로필 저장 실패 (무결성 오류): {username} - {e}")
        return False, "데이터 무결성 오류로 저장에 실패했습니다."
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"프로필 저장 중 DB 오류: {username} - {e}")
        return False, f"DB 저장 중 오류 발생: {str(e)}"
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"프로필 저장 중 예상치 못한 오류: {username} - {e}")
        return False, f"예상치 못한 오류 발생: {str(e)}"
    finally:
        if conn:
            conn.close()


# --- 기존 함수는 테이블 변경에 따라 수정이 필요합니다. ---


def get_user_by_id(user_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    user_id로 users와 profiles 테이블을 조인하여 사용자 정보를 조회합니다.
    """
    conn = get_db_connection()
    if not conn:
        return False, {"error": "DB 연결 실패"}

    try:
        # profiles 테이블만 조회하는 대신, users 테이블과 JOIN
        query = """
        SELECT 
            u.id AS "userId",
            p.birth_date AS "birthDate",
            p.sex AS "gender",
            p.residency_sgg AS "location", -- 필드명 수정 (residency_sgg_code -> residency_sgg)
            p.insurance_type AS "healthInsurance",
            p.median_income AS "incomeLevel",
            p.basic_benefit_type AS "basicLivelihood",
            p.disability_grade AS "disabilityLevel",
            p.ltci_grade AS "longTermCare",
            p.pregnant_or_postpartum AS "pregnancyStatus",
            u.username
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        WHERE u.id = %s
        """

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (user_id,))
            row = cursor.fetchone()

            if row:
                user_dict = dict(row)
                # 기존 함수 출력 형식과 맞추기 위해 데이터 변환
                result = {
                    "userId": user_dict.get("userId"),
                    "username": user_dict.get("username"),
                    "birthDate": (
                        str(user_dict.get("birthDate", ""))
                        if user_dict.get("birthDate")
                        else ""
                    ),
                    "gender": (
                        "남성"
                        if user_dict.get("gender") == "M"
                        else (
                            "여성"
                            if user_dict.get("gender") == "F"
                            else user_dict.get("gender", "")
                        )
                    ),
                    "location": user_dict.get("location", ""),
                    "healthInsurance": user_dict.get("healthInsurance", ""),
                    "incomeLevel": (
                        float(user_dict.get("incomeLevel", 0.0))
                        if user_dict.get("incomeLevel")
                        else 0.0
                    ),
                    "basicLivelihood": user_dict.get("basicLivelihood", "NONE"),
                    "disabilityLevel": (
                        str(user_dict.get("disabilityLevel", "0"))
                        if user_dict.get("disabilityLevel") is not None
                        else "0"
                    ),
                    "longTermCare": user_dict.get("longTermCare", "NONE"),
                    "pregnancyStatus": (
                        "임신중" if user_dict.get("pregnancyStatus") else "없음"
                    ),
                }
                return True, result
            return False, {"error": "사용자를 찾을 수 없습니다."}

    except psycopg2.Error as e:
        logger.error(f"사용자 조회 중 DB 오류: {user_id} - {e}")
        return False, {"error": f"DB 조회 오류: {str(e)}"}
    except Exception as e:
        logger.error(f"사용자 조회 중 예상치 못한 오류: {user_id} - {e}")
        return False, {"error": f"예상치 못한 오류: {str(e)}"}
    finally:
        if conn:
            conn.close()


def check_user_exists(username: str) -> bool:
    """username이 이미 존재하는지 확인 (users 테이블 기준)"""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        # 조회 테이블을 core_profile에서 users로 변경
        query = "SELECT 1 FROM users WHERE username = %s LIMIT 1"
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"사용자 존재 확인 중 오류: {username} - {e}")
        return False
    finally:
        if conn:
            conn.close()


# 나머지 함수들은 그대로 유지합니다. (get_user_by_id, check_user_exists 등)
# 단, 이 함수들도 user_id 대신 username을 사용하는 경우,
# profiles가 아닌 users 테이블을 기준으로 조회하도록 수정해야 합니다. (위 함수들 수정 완료)
