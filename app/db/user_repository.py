"""User Repository 모듈: 사용자 및 프로필 관련 DB 작업을 처리합니다. 11.14수정 + schemas 통합"""

import logging
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Tuple, Optional, List
from datetime import date, datetime
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
# 0. 헬퍼 함수: date/datetime 객체를 ISO 문자열로 변환
# --------------------------------------------------


def _serialize_date(value):
    """date 또는 datetime 객체를 ISO 문자열로 변환합니다."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


# --------------------------------------------------
# ❌ 제거: _transform_db_to_api() 함수는 schemas.py의 from_db_dict()로 대체
# --------------------------------------------------
# schemas.py의 UserProfile.from_db_dict()를 사용합니다


# --------------------------------------------------
# 1. CRUD 함수들
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
    password_hash = user_data.get("password_hash", "").strip()

    if not username or not password_hash:
        return False, "아이디와 비밀번호는 필수 입력 항목입니다."

    new_user_id = str(uuid.uuid4())

    try:
        with conn.cursor() as cursor:
            user_insert_query = """
            INSERT INTO users (id, username, password_hash, main_profile_id, created_at, updated_at, id_uuid)
            VALUES (%s::uuid, %s, %s, NULL, NOW(), NOW(), %s::uuid);
            """
            cursor.execute(
                user_insert_query, (new_user_id, username, password_hash, new_user_id)
            )
            logger.info(f"1. users 테이블에 삽입 완료. user_id: {new_user_id}")

            # normalizer 모듈을 사용하여 데이터 정규화
            birth_date_str = _normalize_birth_date(user_data.get("birth_date"))
            name = user_data.get("name", "").strip() or None
            sex = _normalize_sex(user_data.get("sex", ""))
            residency_sgg_code = user_data.get("residency_sgg_code", "").strip() or None
            insurance_type = _normalize_insurance_type(
                user_data.get("insurance_type", "")
            )
            median_income_ratio = _normalize_income_ratio(
                user_data.get("median_income_ratio")
            )
            basic_benefit_type = _normalize_benefit_type(
                user_data.get("basic_benefit_type", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                user_data.get("disability_grade", "0")
            )
            ltci_grade = _normalize_ltci_grade(user_data.get("ltci_grade", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                user_data.get("pregnant_or_postpartum12m", "없음")
            )

            profile_insert_query = """
            INSERT INTO profiles (
                user_id, birth_date, sex, residency_sgg_code, insurance_type,
                median_income_ratio, basic_benefit_type, disability_grade,
                ltci_grade, pregnant_or_postpartum12m, updated_at, name
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            RETURNING id; 
            """
            profile_data_tuple = (
                new_user_id,
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
                name,
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


def get_user_uuid_by_username(username: str) -> Optional[str]:
    """username으로 user_uuid를 조회합니다."""
    conn = get_db_connection()
    if not conn:
        logger.error(f"DB 연결 실패: {username}")
        return None
    try:
        query = "SELECT id FROM users WHERE username = %s"
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result:
                return str(result[0])
            return None
    except Exception as e:
        logger.error(f"user_uuid 조회 중 오류: {username} - {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_user_password_hash(username: str) -> Optional[str]:
    """DB에서 사용자의 비밀번호 해시를 조회합니다."""
    conn = get_db_connection()
    if not conn:
        logger.error(f"DB 연결 실패: {username}")
        return None
    try:
        query = "SELECT password_hash FROM users WHERE username = %s"
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result:
                logger.info(f"비밀번호 해시 조회 성공: {username}")
                return result[0]
            else:
                logger.warning(f"사용자를 찾을 수 없음: {username}")
                return None
    except Exception as e:
        logger.error(f"비밀번호 해시 조회 중 오류: {username} - {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_user_and_profile_by_id(user_uuid: str) -> Tuple[bool, Dict[str, Any]]:
    """
    user_uuid로 users와 profiles 테이블을 조인하여 사용자 정보를 조회합니다.
    ✅ schemas.py의 from_db_dict() 사용 (변환 로직 통일)
    """
    conn = get_db_connection()
    if not conn:
        return False, {"error": "DB 연결 실패"}

    try:
        # DB 컬럼명 그대로 조회
        query = """
        SELECT 
            u.id AS user_id, u.username, u.main_profile_id,
            p.id AS profile_id,
            p.birth_date, p.sex, p.residency_sgg_code, p.insurance_type,
            p.median_income_ratio, p.basic_benefit_type, p.disability_grade,
            p.ltci_grade, p.pregnant_or_postpartum12m, p.name
        FROM users u
        LEFT JOIN profiles p ON u.main_profile_id = p.id
        WHERE u.id = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (user_uuid,))
            row = cursor.fetchone()

            if not row:
                return False, {"error": "사용자를 찾을 수 없습니다."}

            db_data = dict(row)

            # ✅ 기본 사용자 정보
            result = {
                "id": str(db_data.get("user_id")),
                "username": db_data.get("username"),
                "main_profile_id": db_data.get("main_profile_id"),
            }

            # ✅ 프로필이 있으면 schemas.py의 from_db_dict() 사용
            if db_data.get("profile_id"):
                # UserProfile.from_db_dict()를 호출하기 위해 import 필요
                # 하지만 순환 참조를 피하기 위해 여기서는 직접 변환
                result.update(
                    {
                        "name": db_data.get("name"),
                        "birthDate": _serialize_date(db_data.get("birth_date")),
                        "gender": (
                            "남성"
                            if db_data.get("sex") == "M"
                            else "여성" if db_data.get("sex") == "F" else ""
                        ),
                        "location": db_data.get("residency_sgg_code", ""),
                        "healthInsurance": db_data.get("insurance_type", ""),
                        "incomeLevel": (
                            float(db_data.get("median_income_ratio", 0.0))
                            if db_data.get("median_income_ratio")
                            else 0.0
                        ),
                        "basicLivelihood": db_data.get("basic_benefit_type", "NONE"),
                        "disabilityLevel": (
                            str(db_data.get("disability_grade", "0"))
                            if db_data.get("disability_grade") is not None
                            else "0"
                        ),
                        "longTermCare": db_data.get("ltci_grade", "NONE"),
                        "pregnancyStatus": (
                            "임신중"
                            if db_data.get("pregnant_or_postpartum12m")
                            else "없음"
                        ),
                    }
                )

            return True, result

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
            u.id AS user_id, u.username, u.main_profile_id,
            p.id AS profile_id,
            p.birth_date, p.sex, p.residency_sgg_code, p.insurance_type,
            p.median_income_ratio, p.basic_benefit_type, p.disability_grade,
            p.ltci_grade, p.pregnant_or_postpartum12m, p.name
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        WHERE u.username = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (username,))
            row = cursor.fetchone()

            if not row:
                return False, {"error": "사용자를 찾을 수 없습니다."}

            db_data = dict(row)

            result = {
                "id": str(db_data.get("user_id")),
                "username": db_data.get("username"),
                "main_profile_id": db_data.get("main_profile_id"),
            }

            if db_data.get("profile_id"):
                result.update(
                    {
                        "name": db_data.get("name"),
                        "birthDate": _serialize_date(db_data.get("birth_date")),
                        "gender": (
                            "남성"
                            if db_data.get("sex") == "M"
                            else "여성" if db_data.get("sex") == "F" else ""
                        ),
                        "location": db_data.get("residency_sgg_code", ""),
                        "healthInsurance": db_data.get("insurance_type", ""),
                        "incomeLevel": (
                            float(db_data.get("median_income_ratio", 0.0))
                            if db_data.get("median_income_ratio")
                            else 0.0
                        ),
                        "basicLivelihood": db_data.get("basic_benefit_type", "NONE"),
                        "disabilityLevel": (
                            str(db_data.get("disability_grade", "0"))
                            if db_data.get("disability_grade") is not None
                            else "0"
                        ),
                        "longTermCare": db_data.get("ltci_grade", "NONE"),
                        "pregnancyStatus": (
                            "임신중"
                            if db_data.get("pregnant_or_postpartum12m")
                            else "없음"
                        ),
                    }
                )

            return True, result

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
            return bool(result)
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
            # normalizer 함수 사용으로 통일
            birth_date_str = _normalize_birth_date(profile_data.get("birth_date"))
            name = profile_data.get("name", "").strip() or None
            sex = _normalize_sex(profile_data.get("sex", ""))
            residency_sgg_code = (
                profile_data.get("residency_sgg_code", "").strip() or None
            )
            insurance_type = _normalize_insurance_type(
                profile_data.get("insurance_type", "")
            )
            median_income_ratio = _normalize_income_ratio(
                profile_data.get("median_income_ratio")
            )
            basic_benefit_type = _normalize_benefit_type(
                profile_data.get("basic_benefit_type", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                profile_data.get("disability_grade", "0")
            )
            ltci_grade = _normalize_ltci_grade(profile_data.get("ltci_grade", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                profile_data.get("pregnant_or_postpartum12m", "없음")
            )

            query = """
            INSERT INTO profiles (
                user_id, birth_date, sex, residency_sgg_code, insurance_type,
                median_income_ratio, basic_benefit_type, disability_grade,
                ltci_grade, pregnant_or_postpartum12m, updated_at, name
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            RETURNING id;
            """
            data_tuple = (
                user_uuid,
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
                name,
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
            # normalizer 함수 사용으로 통일
            birth_date_str = _normalize_birth_date(profile_data.get("birth_date"))
            sex = _normalize_sex(profile_data.get("sex", ""))
            residency_sgg_code = (
                profile_data.get("residency_sgg_code", "").strip() or None
            )
            insurance_type = _normalize_insurance_type(
                profile_data.get("insurance_type", "")
            )
            median_income_ratio = _normalize_income_ratio(
                profile_data.get("median_income_ratio")
            )
            basic_benefit_type = _normalize_benefit_type(
                profile_data.get("basic_benefit_type", "NONE")
            )
            disability_grade = _normalize_disability_grade(
                profile_data.get("disability_grade", "0")
            )
            ltci_grade = _normalize_ltci_grade(profile_data.get("ltci_grade", "NONE"))
            pregnant_or_postpartum12m = _normalize_pregnant_status(
                profile_data.get("pregnant_or_postpartum12m", "없음")
            )
            name = profile_data.get("name", "").strip() or None

            query = """
            UPDATE profiles SET
                birth_date = %s, sex = %s, residency_sgg_code = %s, insurance_type = %s,
                median_income_ratio = %s, basic_benefit_type = %s, disability_grade = %s,
                ltci_grade = %s, pregnant_or_postpartum12m = %s, updated_at = NOW(), name = %s
            WHERE id = %s;
            """
            data_tuple = (
                birth_date_str,
                sex,
                residency_sgg_code,
                insurance_type,
                median_income_ratio,
                basic_benefit_type,
                disability_grade,
                ltci_grade,
                pregnant_or_postpartum12m,
                name,
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
            cursor.execute(
                "DELETE FROM collections WHERE profile_id = %s", (profile_id,)
            )
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
    """
    특정 사용자의 모든 프로필 목록을 조회합니다.
    ✅ 프론트엔드 필드명으로 변환하여 반환
    """
    conn = get_db_connection()
    if not conn:
        return False, []

    try:
        # DB 컬럼명 그대로 조회
        query = """
        SELECT
            p.id, p.birth_date, p.sex, p.residency_sgg_code, p.insurance_type,
            p.median_income_ratio, p.basic_benefit_type, p.disability_grade,
            p.ltci_grade, p.pregnant_or_postpartum12m, p.user_id, p.name
        FROM profiles p
        WHERE p.user_id = %s
        ORDER BY p.id;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (user_uuid,))
            profiles = cursor.fetchall()

            result_profiles = []
            for profile in profiles:
                db_data = dict(profile)

                # ✅ DB 필드명 → 프론트엔드 필드명 변환
                transformed = {
                    "id": db_data.get("id"),
                    "name": db_data.get("name"),
                    "birthDate": _serialize_date(db_data.get("birth_date")),
                    "gender": (
                        "남성"
                        if db_data.get("sex") == "M"
                        else "여성" if db_data.get("sex") == "F" else ""
                    ),
                    "location": db_data.get("residency_sgg_code", ""),
                    "healthInsurance": db_data.get("insurance_type", ""),
                    "incomeLevel": (
                        float(db_data.get("median_income_ratio", 0.0))
                        if db_data.get("median_income_ratio")
                        else 0.0
                    ),
                    "basicLivelihood": db_data.get("basic_benefit_type", "NONE"),
                    "disabilityLevel": (
                        str(db_data.get("disability_grade", "0"))
                        if db_data.get("disability_grade") is not None
                        else "0"
                    ),
                    "longTermCare": db_data.get("ltci_grade", "NONE"),
                    "pregnancyStatus": (
                        "임신중" if db_data.get("pregnant_or_postpartum12m") else "없음"
                    ),
                }

                result_profiles.append(transformed)

            return True, result_profiles
    except Exception as e:
        logger.error(f"전체 프로필 조회 중 오류 발생: {user_uuid} - {e}")
        return False, []
    finally:
        if conn:
            conn.close()
# --------------------------------------------------
# End of CRUD 함수들
# --------------------------------------------------