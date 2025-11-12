"""데이터베이스 CRUD 로직 (Repository)"""

import logging
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Tuple, Optional, List
from .db_core import get_db_connection
from .normalizer import (
    _normalize_birth_date,
    _normalize_insurance_type,
    _normalize_benefit_type,
    _normalize_sex,
    _normalize_disability_grade,
    _normalize_ltci_grade,
    _normalize_pregnant_status,
    _normalize_income_ratio,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# 1. 헬퍼 함수: DB에서 조회된 데이터를 API 응답 형식으로 변환 (아웃바운드)
# --------------------------------------------------


def _transform_db_to_api(user_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    DB에서 조회된 RealDictCursor 결과를 API 응답에 맞게 변환합니다.
    (두 조회 함수 get_user_and_profile_by_id, get_user_by_username의 중복 로직 제거)
    """

    # 젠더 (M/F -> 남성/여성)
    gender_api = ""
    if user_dict.get("gender") == "M":
        gender_api = "남성"
    elif user_dict.get("gender") == "F":
        gender_api = "여성"

    # 임신 상태 (True/False -> 임신중/없음)
    pregnancy_status_api = "임신중" if user_dict.get("pregnancyStatus") else "없음"

    # 생년월일 (date 객체 -> str)
    birth_date_str = (
        str(user_dict.get("birthDate", "")) if user_dict.get("birthDate") else ""
    )

    # 소득 수준 (Decimal -> float)
    income_level_float = (
        float(user_dict.get("incomeLevel", 0.0))
        if user_dict.get("incomeLevel")
        else 0.0
    )

    # 장애 등급 (int/None -> str/0)
    disability_level_str = (
        str(user_dict.get("disabilityLevel", "0"))
        if user_dict.get("disabilityLevel") is not None
        else "0"
    )

    result = {
        "id": user_dict.get("id"),
        "main_profile_id": user_dict.get("main_profile_id"),
        "userId": user_dict.get("username"),
        "name": user_dict.get("name"),
        "birthDate": birth_date_str,
        "gender": gender_api,
        "location": user_dict.get("location", ""),
        "healthInsurance": user_dict.get("healthInsurance", ""),
        "incomeLevel": income_level_float,
        "basicLivelihood": user_dict.get("basicLivelihood", "NONE"),
        "disabilityLevel": disability_level_str,
        "longTermCare": user_dict.get("longTermCare", "NONE"),
        "pregnancyStatus": pregnancy_status_api,
    }

    # main_profile_id가 None이면 제거 (get_user_by_username에서 이 키가 없으므로 유연하게 처리)
    if "main_profile_id" in result and result["main_profile_id"] is None:
        del result["main_profile_id"]

    return result


# --------------------------------------------------
# 2. CRUD 함수들
# --------------------------------------------------


def create_user_and_profile(user_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    새로운 사용자의 인증 정보 (users), 기본 프로필 (profiles),
    및 초기 컬렉션 (collections) 정보를 트랜잭션으로 삽입합니다.
    """
    conn = get_db_connection()
    if not conn:
        return False, "데이터베이스 연결 실패"

    username = user_data.get("username", "").strip()
    password_hash = user_data.get("password", "").strip()

    if not username or not password_hash:
        return False, "아이디와 비밀번호는 필수 입력 항목입니다."

    new_user_id = str(uuid.uuid4())

    try:
        with conn.cursor() as cursor:
            user_insert_query = """
            INSERT INTO users (id, username, password_hash, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW());
            """
            cursor.execute(user_insert_query, (new_user_id, username, password_hash))
            logger.info(f"1. users 테이블에 삽입 완료. user_id: {new_user_id}")

            # ******* normalizer 모듈을 사용하여 데이터 정규화 *******
            birth_date_str = _normalize_birth_date(user_data.get("birthDate"))
            name = user_data.get("name", "").strip() or None
            sex = _normalize_sex(user_data.get("gender", ""))
            residency_sgg_code = user_data.get("residency_sgg_code", "").strip() or None
            insurance_type = _normalize_insurance_type(
                user_data.get("insurance_type", "")
            )
            median_income_ratio = _normalize_income_ratio(user_data.get("incomeLevel"))
            basic_benefit_type = _normalize_benefit_type(
                user_data.get("basicLivelihood", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                user_data.get("disabilityLevel", "0")
            )
            ltci_grade = _normalize_ltci_grade(user_data.get("longTermCare", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                user_data.get("pregnancyStatus", "없음")
            )
            # *******************************************************

            profile_insert_query = """
            INSERT INTO profiles (
                user_id, name, birth_date, sex, residency_sgg_code, insurance_type,
                median_income_ratio, basic_benefit_type, disability_grade,
                ltci_grade, pregnant_or_postpartum12m, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id; 
            """
            profile_data_tuple = (
                new_user_id,
                name,
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
            )
            cursor.execute(profile_insert_query, profile_data_tuple)
            new_profile_id = cursor.fetchone()[0]
            logger.info(f"2. profiles 테이블에 삽입 완료. profile_id: {new_profile_id}")

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
            VALUES (%s, %s, %s, %s, 'NONE', NULL, NULL, NULL, FALSE, 1.0, NULL, NOW());
            """
            collection_data_tuple = (
                new_profile_id,
                collection_data.get("subject"),
                collection_data.get("predicate"),
                collection_data.get("object"),
            )
            cursor.execute(collection_insert_query, collection_data_tuple)
            logger.info("3. collections 테이블에 삽입 완료.")

            update_user_query = "UPDATE users SET main_profile_id = %s, updated_at = NOW() WHERE id = %s;"
            cursor.execute(update_user_query, (new_profile_id, new_user_id))
            logger.info("4. users 테이블 main_profile_id 업데이트 완료.")

            conn.commit()
            return True, "회원가입 및 전체 프로필 설정이 성공적으로 완료되었습니다."

    except psycopg2.IntegrityError as e:
        conn.rollback()
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


def get_user_password_hash(username: str) -> Optional[str]:
    """DB에서 사용자의 비밀번호 해시를 조회합니다."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        query = "SELECT password_hash FROM users WHERE username = %s"
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"비밀번호 해시 조회 중 오류: {username} - {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_user_and_profile_by_id(user_uuid: str) -> Tuple[bool, Dict[str, Any]]:
    """user_uuid로 users와 profiles 테이블을 조인하여 사용자 정보를 조회합니다."""
    conn = get_db_connection()
    if not conn:
        return False, {"error": "DB 연결 실패"}

    try:
        query = """
        SELECT 
            u.id AS "id", u.username AS "username", u.main_profile_id AS "main_profile_id",
            p.birth_date AS "birthDate", p.sex AS "gender", p.residency_sgg_code AS "location", 
            p.insurance_type AS "healthInsurance", p.median_income_ratio AS "incomeLevel",
            p.basic_benefit_type AS "basicLivelihood", p.disability_grade AS "disabilityLevel",
            p.ltci_grade AS "longTermCare", p.pregnant_or_postpartum12m AS "pregnancyStatus"
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        WHERE u.id = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (user_uuid,))
            row = cursor.fetchone()
            if row:
                user_dict = dict(row)
                # **[수정] DB 값 후처리 로직을 헬퍼 함수로 분리**
                result = _transform_db_to_api(user_dict)
                return True, result
            return False, {"error": "사용자를 찾을 수 없습니다."}
    except psycopg2.Error as e:
        logger.error(f"사용자 조회 중 DB 오류: {user_uuid} - {e}")
        return False, {"error": f"DB 조회 오류: {str(e)}"}
    except Exception as e:
        logger.error(f"사용자 조회 중 예상치 못한 오류: {user_uuid} - {e}")
        return False, {"error": f"예상치 못한 오류: {str(e)}"}
    finally:
        if conn:
            conn.close()


def get_user_by_username(username: str) -> Tuple[bool, Dict[str, Any]]:
    """username으로 users와 profiles 테이블을 조인하여 사용자 정보를 조회합니다."""
    conn = get_db_connection()
    if not conn:
        return False, {"error": "DB 연결 실패"}

    try:
        query = """
        SELECT 
            u.id AS "id", u.username AS "username", u.main_profile_id AS "main_profile_id",
            p.birth_date AS "birthDate", p.sex AS "gender", p.residency_sgg_code AS "location", 
            p.insurance_type AS "healthInsurance", p.median_income_ratio AS "incomeLevel",
            p.basic_benefit_type AS "basicLivelihood", p.disability_grade AS "disabilityLevel",
            p.ltci_grade AS "longTermCare", p.pregnant_or_postpartum12m AS "pregnancyStatus"
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        WHERE u.username = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (username,))
            row = cursor.fetchone()
            if row:
                user_dict = dict(row)
                # **[수정] DB 값 후처리 로직을 헬퍼 함수로 분리**
                # main_profile_id는 인증 로직에서 필요 없으므로 transform에서 제거될 수 있도록 처리
                result = _transform_db_to_api(user_dict)
                return True, result
            return False, {"error": "사용자를 찾을 수 없습니다."}
    except psycopg2.Error as e:
        logger.error(f"사용자 조회 중 DB 오류: {username} - {e}")
        return False, {"error": f"DB 조회 오류: {str(e)}"}
    except Exception as e:
        logger.error(f"사용자 조회 중 예상치 못한 오류: {username} - {e}")
        return False, {"error": f"예상치 못한 오류: {str(e)}"}
    finally:
        if conn:
            conn.close()


def update_user_password(user_uuid: str, new_password_hash: str) -> Tuple[bool, str]:
    """사용자의 비밀번호 해시를 업데이트합니다."""
    conn = get_db_connection()
    if not conn:
        return False, "데이터베이스 연결 실패"

    try:
        with conn.cursor() as cursor:
            query = (
                "UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s"
            )
            cursor.execute(query, (new_password_hash, user_uuid))
            if cursor.rowcount == 0:
                return False, "사용자를 찾을 수 없습니다."
            conn.commit()
            logger.info(f"비밀번호 업데이트 성공 (user_uuid: {user_uuid})")
            return True, "비밀번호가 성공적으로 변경되었습니다."
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"비밀번호 업데이트 중 오류 발생 (user_uuid: {user_uuid}) - {e}")
        return False, "비밀번호 변경 중 오류가 발생했습니다."
    finally:
        if conn:
            conn.close()


def update_user_main_profile_id(
    user_uuid: str, profile_id: Optional[int]
) -> Tuple[bool, str]:
    """사용자의 main_profile_id를 업데이트합니다."""
    conn = get_db_connection()
    if not conn:
        return False, "데이터베이스 연결 실패"

    try:
        with conn.cursor() as cursor:
            query = "UPDATE users SET main_profile_id = %s, updated_at = NOW() WHERE id = %s"
            cursor.execute(query, (profile_id, user_uuid))
            if cursor.rowcount == 0:
                return False, "사용자를 찾을 수 없습니다."
            conn.commit()
            logger.info(
                f"main_profile_id 업데이트 성공 (user_uuid: {user_uuid}, profile_id: {profile_id})"
            )
            return True, "기본 프로필이 성공적으로 변경되었습니다."
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(
            f"main_profile_id 업데이트 중 오류 발생 (user_uuid: {user_uuid}) - {e}"
        )
        return False, "기본 프로필 변경 중 오류가 발생했습니다."
    finally:
        if conn:
            conn.close()


def check_user_exists(username: str) -> bool:
    """username이 이미 존재하는지 확인 (users 테이블 기준)"""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        query = "SELECT 1 FROM users WHERE username = %s LIMIT 1"
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            return bool(result)  # None 체크를 위해 bool() 사용
    except Exception as e:
        logger.error(f"사용자 존재 여부 조회 중 오류: {username} - {e}")
        return False
    finally:
        if conn:
            conn.close()


def delete_user_account(user_id: str) -> Tuple[bool, str]:
    """사용자 계정과 관련된 모든 데이터를 삭제합니다 (users, profiles, collections)."""
    conn = get_db_connection()
    if not conn:
        return False, "데이터베이스 연결 실패"

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM collections WHERE profile_id IN (SELECT id FROM profiles WHERE user_id = %s)",
                (user_id,),
            )
            cursor.execute("DELETE FROM profiles WHERE user_id = %s", (user_id,))
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()
            logger.info(f"회원 탈퇴 완료 (user_id: {user_id})")
            return True, "회원 탈퇴가 완료되었습니다."
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"회원 탈퇴 중 오류 발생 (user_id: {user_id}) - {e}")
        return False, "회원 탈퇴 처리 중 오류가 발생했습니다."
    finally:
        if conn:
            conn.close()


def add_profile(
    user_uuid: str, profile_data: Dict[str, Any]
) -> Tuple[bool, Optional[int]]:
    """새로운 프로필을 profiles 테이블에 추가합니다."""
    conn = get_db_connection()
    if not conn:
        return False, None

    try:
        with conn.cursor() as cursor:
            # **[수정] 인바운드 매핑 로직 제거 및 normalizer 함수 사용으로 통일**
            birth_date_str = _normalize_birth_date(profile_data.get("birthDate"))
            name = profile_data.get("name", "").strip() or None
            sex = _normalize_sex(profile_data.get("gender", ""))
            residency_sgg_code = profile_data.get("location", "").strip() or None
            insurance_type = _normalize_insurance_type(
                profile_data.get("healthInsurance", "")
            )
            median_income_ratio = _normalize_income_ratio(
                profile_data.get("incomeLevel")
            )
            basic_benefit_type = _normalize_benefit_type(
                profile_data.get("basicLivelihood", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                profile_data.get("disabilityLevel", "0")
            )
            ltci_grade = _normalize_ltci_grade(profile_data.get("longTermCare", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                profile_data.get("pregnancyStatus", "없음")
            )
            # *******************************************************

            query = """
            INSERT INTO profiles (
                user_id, name, birth_date, sex, residency_sgg_code, insurance_type,
                median_income_ratio, basic_benefit_type, disability_grade,
                ltci_grade, pregnant_or_postpartum12m, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id;
            """
            data_tuple = (
                user_uuid,
                name,
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
            )
            cursor.execute(query, data_tuple)
            new_profile_id = cursor.fetchone()[0]
            conn.commit()
            logger.info(
                f"새 프로필 추가 성공. user_uuid: {user_uuid}, new_profile_id: {new_profile_id}"
            )
            return True, new_profile_id
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"프로필 추가 중 오류 발생: {user_uuid} - {e}")
        return False, None
    finally:
        if conn:
            conn.close()


def update_profile(profile_id: int, profile_data: Dict[str, Any]) -> bool:
    """기존 프로필 정보를 업데이트합니다."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            # **[수정] 인바운드 매핑 로직 제거 및 normalizer 함수 사용으로 통일**
            birth_date_str = _normalize_birth_date(profile_data.get("birthDate"))
            name = profile_data.get("name", "").strip() or None
            sex = _normalize_sex(profile_data.get("gender", ""))
            residency_sgg_code = profile_data.get("location", "").strip() or None
            insurance_type = _normalize_insurance_type(
                profile_data.get("healthInsurance", "")
            )
            median_income_ratio = _normalize_income_ratio(
                profile_data.get("incomeLevel")
            )
            basic_benefit_type = _normalize_benefit_type(
                profile_data.get("basicLivelihood", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                profile_data.get("disabilityLevel", "0")
            )
            ltci_grade = _normalize_ltci_grade(profile_data.get("longTermCare", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                profile_data.get("pregnancyStatus", "없음")
            )
            # *******************************************************

            query = """
            UPDATE profiles SET
                name = %s, birth_date = %s, sex = %s, residency_sgg_code = %s, insurance_type = %s,
                median_income_ratio = %s, basic_benefit_type = %s, disability_grade = %s,
                ltci_grade = %s, pregnant_or_postpartum12m = %s, updated_at = NOW()
            WHERE id = %s;
            """
            data_tuple = (
                name,
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
                profile_id,
            )
            cursor.execute(query, data_tuple)
            conn.commit()
            logger.info(f"프로필 업데이트 성공. profile_id: {profile_id}")
            return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"프로필 업데이트 중 오류 발생: {profile_id} - {e}")
        return False
    finally:
        if conn:
            conn.close()


def delete_profile_by_id(profile_id: int) -> bool:
    """특정 ID의 프로필을 삭제합니다. collections의 관련 데이터도 함께 삭제합니다."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            # 1. 이 프로필을 참조하는 collections 데이터 삭제
            cursor.execute(
                "DELETE FROM collections WHERE profile_id = %s", (profile_id,)
            )

            # 2. 프로필 삭제
            cursor.execute("DELETE FROM profiles WHERE id = %s", (profile_id,))

            conn.commit()
            logger.info(f"프로필 삭제 성공. profile_id: {profile_id}")
            return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"프로필 삭제 중 오류 발생: profile_id={profile_id} - {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_all_profiles_by_user_id(user_uuid: str) -> Tuple[bool, List[Dict[str, Any]]]:
    """특정 사용자의 모든 프로필 목록을 조회합니다."""
    conn = get_db_connection()
    if not conn:
        return False, []

    try:
        query = """
        SELECT 
            p.id, p.user_id, p.name AS "name", p.birth_date AS "birthDate", p.sex AS "gender",
            p.residency_sgg_code AS "location", p.insurance_type AS "healthInsurance",
            p.median_income_ratio AS "incomeLevel", p.basic_benefit_type AS "basicLivelihood",
            p.disability_grade AS "disabilityLevel", p.ltci_grade AS "longTermCare",
            p.pregnant_or_postpartum12m AS "pregnancyStatus"
        FROM profiles p
        WHERE p.user_id = %s;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (user_uuid,))
            profiles = cursor.fetchall()

            result_profiles = []
            for profile in profiles:
                p_dict = dict(profile)

                # **[수정] 목록 조회에서도 후처리 헬퍼 함수를 사용하여 중복 제거**
                # _transform_db_to_api를 프로필 목록 조회에 맞게 간소화하거나,
                # p.id와 p.user_id 키가 포함되도록 수정된 버전을 사용
                # 여기서는 _transform_db_to_api를 사용하여 변환하고, 필요 없는 키는 제거함
                transformed = _transform_db_to_api(p_dict)

                # 프로필 목록 조회에서 불필요한 키는 제거 (main_profile_id, userId, username)
                transformed.pop("main_profile_id", None)
                transformed.pop("userId", None)
                # transformed.pop("username", None)

                result_profiles.append(transformed)

            return True, result_profiles
    except Exception as e:
        logger.error(f"전체 프로필 조회 중 오류 발생: {user_uuid} - {e}")
        return False, []
    finally:
        if conn:
            conn.close()
