# -*- coding: utf-8 -*-
"""
Streamlit 앱에서 직접 사용하는 데이터베이스 접근 계층 (PostgreSQL)
모든 CRUD 및 사용자 인증 관련 DB 로직을 포함합니다.
실제 DB 스키마에 맞게 수정됨.
"""
import psycopg2
import psycopg2.extras
import os
import uuid
from typing import Optional, Dict, List, Tuple, Any
import logging
from contextlib import contextmanager
from dotenv import load_dotenv


# .env 파일에서 환경 변수 로드
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 환경 변수에서 DB 연결 정보 로드
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# 매핑 딕셔너리
GENDER_MAPPING = {
    "남성": "M",
    "여성": "F",
}

HEALTH_INSURANCE_MAPPING = {
    "직장": "EMPLOYED",
    "지역": "LOCAL",
    "피부양": "DEPENDENT",
    "의료급여": "MEDICAL_AID_1",
}

BASIC_LIVELIHOOD_MAPPING = {
    "없음": "NONE",
    "생계": "LIVELIHOOD",
    "의료": "MEDICAL",
    "주거": "HOUSING",
    "교육": "EDUCATION",
}


# ==============================================================================
# 1. DB 연결 및 컨텍스트 관리
# ==============================================================================


@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        yield conn
    except psycopg2.OperationalError as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        yield None  # 연결 실패 시 None 반환
    except Exception as e:
        logger.error(f"데이터베이스 오류: {e}")
        yield None
    finally:
        if conn:
            conn.close()


def get_db():
    """FastAPI 의존성 주입을 위한 DB 세션 생성기"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        yield conn
    finally:
        if conn:
            conn.close()


def initialize_db():
    """
    DB에 'users' 및 'profiles' 테이블이 없으면 생성합니다.
    주의: 실제 운영 DB는 이미 다른 스키마로 생성되어 있으므로 이 함수는 참고용입니다.
    """
    with get_db_connection() as conn:
        if conn is None:
            logger.error("DB 초기화 실패: 연결할 수 없습니다.")
            return

        try:
            with conn.cursor() as cur:
                # 실제 DB 스키마에 맞춘 테이블 생성 (참고용)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        username TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        main_profile_id BIGINT NULL
                    );
                """
                )
                logger.info("Table 'users' checked/created.")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS profiles (
                        id BIGSERIAL PRIMARY KEY,
                        user_id UUID NOT NULL,
                        name TEXT NOT NULL,
                        birth_date DATE,
                        sex TEXT,
                        residency_sgg_code TEXT,
                        insurance_type TEXT,
                        median_income_ratio NUMERIC,
                        basic_benefit_type TEXT,
                        disability_grade SMALLINT,
                        ltci_grade TEXT,
                        pregnant_or_postpartum12m BOOLEAN,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
                    );
                """
                )
                logger.info("Table 'profiles' checked/created.")

                # main_profile_id 외래 키 제약 조건
                try:
                    cur.execute(
                        """
                        ALTER TABLE users 
                        ADD CONSTRAINT fk_main_profile
                        FOREIGN KEY (main_profile_id) REFERENCES profiles (id) 
                        ON DELETE SET NULL;
                    """
                    )
                    logger.info("Foreign key fk_main_profile added to 'users'.")
                except psycopg2.errors.DuplicateObject:
                    pass

                conn.commit()

            logger.info("Database initialization complete.")
        except Exception as e:
            conn.rollback()
            logger.error(f"DB 초기화 중 오류 발생: {e}")


# ==============================================================================
# 2. 사용자 인증 및 계정 관리
# ==============================================================================


def check_user_exists(username: str) -> bool:
    """아이디(username)를 사용하여 사용자 존재 여부를 확인합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"check_user_exists 오류: {e}")
            return False


def get_user_password_hash(username: str) -> Optional[str]:
    """아이디(username)를 사용하여 저장된 비밀번호 해시를 가져옵니다."""
    with get_db_connection() as conn:
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT password_hash FROM users WHERE username = %s", (username,)
                )
                result = cur.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"get_user_password_hash 오류: {e}")
            return None


def get_user_uuid_by_username(username: str) -> Optional[str]:
    """아이디(username)를 사용하여 UUID를 가져옵니다 (로그인 성공 시 사용)."""
    with get_db_connection() as conn:
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE username = %s", (username,))
                result = cur.fetchone()
                return str(result[0]) if result else None
        except Exception as e:
            logger.error(f"get_user_uuid_by_username 오류: {e}")
            return None


def create_user_and_profile(user_data: Dict[str, Any]) -> Tuple[bool, str]:
    """사용자 계정을 생성하고 초기 프로필을 저장합니다."""
    username = user_data.get("username")
    password_hash = user_data.get("password_hash")

    if not (username and password_hash):
        return False, "필수 사용자 정보가 누락되었습니다."

    new_uuid_str = str(uuid.uuid4())

    with get_db_connection() as conn:
        if conn is None:
            return False, "DB 연결 실패."

        try:
            with conn.cursor() as cur:
                # 1. 사용자 생성 (id_uuid 포함)
                cur.execute(
                    "INSERT INTO users (id, username, password_hash, id_uuid) VALUES (%s, %s, %s, %s)",
                    (new_uuid_str, username, password_hash, new_uuid_str),
                )

                # 2. 기본 프로필 생성 - 매핑 적용
                # profile_name = user_data.get("username")
                profile_name = user_data.get("name", "본인")
                birth_date = user_data.get("birth_date")

                # 성별 매핑
                sex = GENDER_MAPPING.get(user_data.get("gender"), "M")

                residency_sgg_code = user_data.get("residency_sgg_code")

                # 건강보험 매핑
                insurance_type = HEALTH_INSURANCE_MAPPING.get(
                    user_data.get("insurance_type"), "EMPLOYED"
                )

                median_income_ratio = float(
                    user_data.get("median_income_ratio", 0) or 0
                )

                # 기초생활보장 매핑
                basic_benefit_type = BASIC_LIVELIHOOD_MAPPING.get(
                    user_data.get("basic_benefit_type", "없음"), "NONE"
                )

                # 장애등급 (숫자)
                disability_grade = (
                    int(user_data.get("disability_grade", 0) or 0)
                    if user_data.get("disability_grade")
                    else None
                )

                # 장기요양 등급 (이미 영문 코드)
                ltci_grade = user_data.get("ltci_grade", "NONE")

                # 임신 여부 (boolean)
                pregnant_or_postpartum12m = (
                    user_data.get("pregnant_or_postpartum12m") == "임신중"
                    or user_data.get("pregnant_or_postpartum12m") == "출산후12개월이내"
                )

                cur.execute(
                    """
                    INSERT INTO profiles (
                        user_id, name, birth_date, sex, residency_sgg_code, 
                        insurance_type, median_income_ratio, basic_benefit_type, 
                        disability_grade, ltci_grade, pregnant_or_postpartum12m
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    RETURNING id;
                """,
                    (
                        new_uuid_str,
                        profile_name,
                        birth_date,
                        sex,
                        residency_sgg_code,
                        insurance_type,
                        median_income_ratio,
                        basic_benefit_type,
                        disability_grade,
                        ltci_grade,
                        pregnant_or_postpartum12m,
                    ),
                )

                # 3. 생성된 프로필 ID 가져오기
                main_profile_id = cur.fetchone()[0]

                # 4. collections 테이블에 초기 데이터 추가 (임신 여부만)
                if pregnant_or_postpartum12m:
                    pregnancy_detail = user_data.get(
                        "pregnant_or_postpartum12m", "임신중"
                    )
                    cur.execute(
                        """
                        INSERT INTO collections (
                            profile_id, subject, predicate, object,
                            code_system, code, onset_date, end_date,
                            negation, confidence, source_id, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            main_profile_id,
                            "user",
                            "PREGNANT_OR_POSTPARTUM12M",
                            pregnancy_detail,
                            "NONE",
                            None,
                            None,
                            None,
                            False,
                            1.0,
                            None,
                        ),
                    )

                # 5. users 테이블 main_profile_id 업데이트
                cur.execute(
                    "UPDATE users SET main_profile_id = %s WHERE id = %s",
                    (main_profile_id, new_uuid_str),
                )

                conn.commit()
                return True, "회원가입 및 프로필 생성이 완료되었습니다."

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            return False, "이미 존재하는 사용자 이름입니다."
        except Exception as e:
            conn.rollback()
            logger.error(f"create_user_and_profile 오류: {e}")
            return False, f"데이터베이스 오류: {e}"


def update_user_password(user_uuid: str, new_password_hash: str) -> Tuple[bool, str]:
    """사용자 비밀번호 해시를 업데이트합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False, "DB 연결 실패."
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET password_hash = %s WHERE id = %s",
                    (new_password_hash, user_uuid),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return False, "사용자를 찾을 수 없습니다."
                conn.commit()
                return True, "비밀번호가 성공적으로 변경되었습니다."
        except Exception as e:
            conn.rollback()
            logger.error(f"update_user_password 오류: {e}")
            return False, "비밀번호 업데이트 중 오류가 발생했습니다."


def delete_user_account(user_uuid: str) -> Tuple[bool, str]:
    """사용자와 관련된 모든 정보를 삭제합니다 (profiles는 CASCADE로 삭제됨)."""
    with get_db_connection() as conn:
        if conn is None:
            return False, "DB 연결 실패."
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = %s", (user_uuid,))
                if cur.rowcount == 0:
                    conn.rollback()
                    return False, "사용자 계정을 찾을 수 없습니다."
                conn.commit()
                return True, "사용자 계정이 성공적으로 삭제되었습니다."
        except Exception as e:
            conn.rollback()
            logger.error(f"delete_user_account 오류: {e}")
            return False, "계정 삭제 중 오류가 발생했습니다."


# ==============================================================================
# 3. 프로필 관리
# ==============================================================================


def _map_profile_row(row: Dict) -> Dict[str, Any]:
    """DB 행을 프론트엔드에서 사용하는 키 이름으로 변환합니다."""
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "birth_date": str(row["birth_date"]) if row.get("birth_date") else None,
        "sex": GENDER_MAPPING.get(row.get("sex"), "M"),
        "residency_sgg_code": row.get("residency_sgg_code"),
        "median_income_ratio": (
            float(row.get("median_income_ratio"))
            if row.get("median_income_ratio")
            else None
        ),
        "insurance_type": HEALTH_INSURANCE_MAPPING.get(row.get("insurance_type"), "EMPLOYED"),
        "basic_benefit_type": BASIC_LIVELIHOOD_MAPPING.get(row.get("basic_benefit_type"), "NONE"),
        "disability_grade": (
            int(row.get("disability_grade"))
            if row.get("disability_grade") is not None
            else None
        ),
        "ltci_grade": row.get("ltci_grade"),
        "pregnant_or_postpartum12m": (
            "임신 또는 출산 후 12개월 이내"
            if row.get("pregnant_or_postpartum12m")
            else "해당 없음"
        ),
    }


def get_user_and_profile_by_id(user_uuid: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """사용자 UUID로 사용자 정보와 메인 프로필 정보를 조회합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False, None
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        u.id, u.username, u.main_profile_id, u.created_at, u.updated_at,
                        p.id as profile_id, p.name, p.birth_date, p.sex,
                        p.residency_sgg_code, p.insurance_type, p.median_income_ratio,
                        p.basic_benefit_type, p.disability_grade, p.ltci_grade,
                        p.pregnant_or_postpartum12m
                    FROM users u
                    LEFT JOIN profiles p ON u.main_profile_id = p.id
                    WHERE u.id = %s;
                    """,
                    (user_uuid,),
                )

                result = cur.fetchone()

                if not result:
                    return False, None

                user_info = dict(result)

                # 메인 프로필 정보 매핑
                profile_info = {}
                if user_info.get("main_profile_id"):
                    profile_info = {
                        "id": user_info.get("profile_id"),
                        "name": user_info.get("name"),
                        "birthDate": (
                            str(user_info.get("birth_date"))
                            if user_info.get("birth_date")
                            else None
                        ),
                        "sex": user_info.get("sex"),
                        "residency_sgg_code": user_info.get("residency_sgg_code"),
                        "median_income_ratio": (
                            float(user_info.get("median_income_ratio"))
                            if user_info.get("median_income_ratio")
                            else None
                        ),
                        "insurance_type": user_info.get("insurance_type"),
                        "basic_benefit_type": user_info.get("basic_benefit_type"),
                        "disability_grade": (
                            int(user_info.get("disability_grade"))
                            if user_info.get("disability_grade") is not None
                            else None
                        ),
                        "ltci_grade": user_info.get("ltci_grade"),
                        "pregnant_or_postpartum12m": (
                            "임신 또는 출산 후 12개월 이내"
                            if user_info.get("pregnant_or_postpartum12m")
                            else "해당 없음"
                        ),
                    }

                # 최종 데이터 구조
                final_data = {
                    "user_uuid": str(user_info["id"]),
                    "username": user_info["username"],
                    "main_profile_id": user_info["main_profile_id"],
                    "created_at": user_info.get("created_at"),
                    "updated_at": user_info.get("updated_at"),
                    **profile_info,
                }
                return True, final_data
        except Exception as e:
            logger.error(f"get_user_and_profile_by_id 오류: {e}")
            return False, None


def get_all_profiles_by_user_id(user_uuid: str) -> Tuple[bool, List[Dict[str, Any]]]:
    """사용자의 모든 프로필 목록을 조회합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False, []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT * FROM profiles WHERE user_id = %s ORDER BY id",
                    (user_uuid,),
                )
                rows = cur.fetchall()

                profiles_list = [_map_profile_row(dict(row)) for row in rows]
                return True, profiles_list
        except Exception as e:
            logger.error(f"get_all_profiles_by_user_id 오류: {e}")
            return False, []


def add_profile(user_uuid: str, profile_data: Dict[str, Any]) -> Tuple[bool, int]:
    """새로운 프로필을 추가합니다. 성공 시 프로필 ID를 반환합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False, 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO profiles (
                        user_id, name, birth_date, sex, residency_sgg_code, 
                        insurance_type, median_income_ratio, basic_benefit_type, 
                        disability_grade, ltci_grade, pregnant_or_postpartum12m
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    RETURNING id;
                """,
                    (
                        user_uuid,
                        profile_data.get("name", "새 프로필"),
                        profile_data.get("birthDate"),
                        profile_data.get("sex"),
                        profile_data.get("residency_sgg_code"),
                        profile_data.get("insurance_type"),
                        (
                            float(profile_data.get("median_income_ratio") or 0)
                            if profile_data.get("median_income_ratio")
                            else None
                        ),
                        profile_data.get("basic_benefit_type"),
                        (
                            int(profile_data.get("disability_grade") or 0)
                            if profile_data.get("disability_grade")
                            else None
                        ),
                        profile_data.get("ltci_grade"),
                        profile_data.get("pregnant_or_postpartum12m")
                        == "임신 또는 출산 후 12개월 이내",
                    ),
                )

                profile_id = cur.fetchone()[0]
                conn.commit()
                return True, profile_id
        except Exception as e:
            conn.rollback()
            logger.error(f"add_profile 오류: {e}")
            return False, 0


def update_profile(profile_id: int, profile_data: Dict[str, Any]) -> bool:
    """기존 프로필 정보를 업데이트합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False
        try:
            set_clauses = []
            values = []

            # 프론트엔드 키를 DB 컬럼에 맞게 변환
            column_map = {
                "name": "name",
                "birthDate": "birth_date",
                "sex": "sex",
                "residency_sgg_code": "residency_sgg_code",
                "median_income_ratio": "median_income_ratio",
                "insurance_type": "insurance_type",
                "basic_benefit_type": "basic_benefit_type",
                "disability_grade": "disability_grade",
                "ltci_grade": "ltci_grade",
                "pregnant_or_postpartum12m": "pregnant_or_postpartum12m",
            }

            for frontend_key, db_column in column_map.items():
                if frontend_key in profile_data:
                    value = profile_data[frontend_key]

                    # 타입 변환
                    if frontend_key == "median_income_ratio":
                        value = float(value) if value is not None else None
                    elif frontend_key == "disability_grade":
                        value = int(value) if value is not None else None
                    elif frontend_key == "pregnant_or_postpartum12m":
                        value = value == "임신 또는 출산 후 12개월 이내"

                    set_clauses.append(f"{db_column} = %s")
                    values.append(value)

            if not set_clauses:
                logger.warning(f"업데이트할 데이터 없음: profile_id={profile_id}")
                return True

            values.append(profile_id)
            sql = f"UPDATE profiles SET {', '.join(set_clauses)} WHERE id = %s"

            with conn.cursor() as cur:
                cur.execute(sql, values)
                if cur.rowcount == 0:
                    conn.rollback()
                    return False
                conn.commit()
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"update_profile 오류: {e}")
            return False


def delete_profile_by_id(profile_id: int) -> bool:
    """프로필 ID를 사용하여 프로필을 삭제합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False
        try:
            with conn.cursor() as cur:
                # main_profile_id가 이 프로필을 가리키고 있으면 NULL로 설정됨 (ON DELETE SET NULL)
                cur.execute("DELETE FROM profiles WHERE id = %s", (profile_id,))

                if cur.rowcount == 0:
                    conn.rollback()
                    return False

                conn.commit()
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"delete_profile_by_id 오류: {e}")
            return False


def update_user_main_profile_id(
    user_uuid: str, profile_id: Optional[int]
) -> Tuple[bool, str]:
    """사용자의 메인 프로필 ID를 업데이트합니다."""
    with get_db_connection() as conn:
        if conn is None:
            return False, "DB 연결 실패."
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET main_profile_id = %s WHERE id = %s",
                    (profile_id, user_uuid),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return False, "사용자를 찾을 수 없거나 업데이트에 실패했습니다."
                conn.commit()
                return True, "기본 프로필 ID가 성공적으로 업데이트되었습니다."
        except Exception as e:
            conn.rollback()
            logger.error(f"update_user_main_profile_id 오류: {e}")
            return False, "기본 프로필 ID 업데이트 중 오류가 발생했습니다."


# ==============================================================================
# 4. 초기 실행 (main)
# ==============================================================================

if __name__ == "__main__":
    initialize_db()
    print("데이터베이스 초기화 완료.")
