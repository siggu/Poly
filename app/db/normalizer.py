"""11.12 데이터베이스 입력을 위한 데이터 정규화 함수들"""

from typing import Any, Optional
from datetime import date
import re  # 날짜 형식을 검사하기 위해 import


# --------------------------------------------------
# 1. 기본 타입 및 포맷 정규화 함수
# --------------------------------------------------


def _normalize_birth_date(birth_date: Any) -> Optional[str]:
    """birthDate를 YYYY-MM-DD 문자열로 변환하고 유효성을 검사합니다."""
    if birth_date is None:
        return None

    if isinstance(birth_date, date):
        return birth_date.isoformat()

    if isinstance(birth_date, str):
        # YYYY-MM-DD 포맷 검증
        match = re.match(r"^\d{4}-\d{2}-\d{2}", birth_date)
        if match:
            return match.group(0)
        # YYYYMMDD 포맷이 들어온 경우 변환
        if len(birth_date) == 8 and birth_date.isdigit():
            return f"{birth_date[:4]}-{birth_date[4:6]}-{birth_date[6:8]}"

    # 유효한 형식의 값이 아니면 None 반환
    return None


def _normalize_sex(gender: str) -> Optional[str]:
    """성별을 DB ENUM 형식으로 변환 (남성->M, 여성->F)"""
    if not gender:
        return None
    gender_lower = gender.lower().strip()

    if "남" in gender_lower or "male" in gender_lower or gender_lower in ("m", "1"):
        return "M"
    if "여" in gender_lower or "female" in gender_lower or gender_lower in ("f", "2"):
        return "F"

    # 유효한 M/F 값이 아니면 None 반환
    return None


def _normalize_disability_grade(disability_level: Any) -> Optional[int]:
    """장애 등급을 정수형으로 변환"""
    if not disability_level or str(disability_level).strip() in ("0", "미등록", "없음"):
        return None
    try:
        return int(str(disability_level).strip())
    except (ValueError, TypeError):
        return None


def _normalize_ltci_grade(long_term_care: str) -> str:
    """장기요양 등급 정규화 (없음 -> NONE)"""
    if not long_term_care or long_term_care.strip().upper() in (
        "없음",
        "해당없음",
        "NONE",
    ):
        return "NONE"
    return long_term_care.upper().strip()


def _normalize_pregnant_status(pregnancy_status: Any) -> Optional[bool]:
    """임신/출산 여부를 Boolean으로 변환"""
    if pregnancy_status is None:
        return None

    if isinstance(pregnancy_status, bool):
        return pregnancy_status

    status_lower = str(pregnancy_status).lower().strip()

    # 긍정 값
    if (
        "임신" in status_lower
        or "출산" in status_lower
        or status_lower in ("true", "t", "1", "yes", "y")
    ):
        return True

    # 그 외의 모든 값 (None, 명시적 부정 값)은 False로 처리합니다.
    return False


def _normalize_income_ratio(income_level: Any) -> Optional[float]:
    """소득 수준을 NUMERIC(5,2)로 변환"""
    if income_level is None:
        return None
    try:
        if isinstance(income_level, str):
            # 쉼표(,) 제거 후 변환
            income_level = income_level.replace(",", "")

        val = float(income_level)
        return round(val, 2)
    except (ValueError, TypeError):
        return None


# --------------------------------------------------
# 2. ENUM 매핑 정규화 함수 (Repository 중복 제거)
# --------------------------------------------------


def _normalize_insurance_type(insurance_str: str) -> Optional[str]:
    """
    건강보험 종류를 DB ENUM 형식으로 변환합니다. (한글 -> ENUM 매핑 포함)
    """
    if not insurance_str:
        return None

    insurance_mapping = {
        "직장": "EMPLOYED",
        "지역": "LOCAL",
        "피부양": "DEPENDENT",
        "의료급여": "MEDICAL_AID_1",
    }

    # 한글 매핑 시도
    mapped_value = insurance_mapping.get(insurance_str)
    if mapped_value:
        return mapped_value

    # 매핑이 안 되면, 입력된 값을 대문자로 변환하여 반환 (이미 ENUM 값일 경우 대비)
    return insurance_str.strip().upper()


def _normalize_benefit_type(benefit_str: str) -> str:
    """
    기초생활보장 급여 종류를 DB ENUM 형식으로 변환합니다. (한글 -> ENUM 매핑 포함)
    """
    livelihood_mapping = {
        "없음": "NONE",
        "생계": "LIVELIHOOD",
        "의료": "MEDICAL",
        "주거": "HOUSING",
        "교육": "EDUCATION",
    }

    if not benefit_str:
        return "NONE"

    # 한글 매핑 시도
    mapped_value = livelihood_mapping.get(benefit_str)
    if mapped_value:
        return mapped_value

    # 매핑이 안 되면, 입력된 값을 대문자로 변환하여 반환
    return benefit_str.strip().upper()
# --------------------------------------------------