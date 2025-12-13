# -*- coding: utf-8 -*-
# app/langgraph/utils/eligibility_filters.py
# -----------------------------------------------------------
# 프로필 기반 정책 후보 필터링 유틸
#   - median_income_ratio (중위소득 비율)
#   - basic_benefit_type / 차상위 여부
#   - disability_grade (장애등급)
#   - 필요하면 점점 규칙 추가
# -----------------------------------------------------------

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
import logging
log = logging.getLogger(__name__)

def _extract_profile_str(profile: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if not profile:
        return None
    v = profile.get(key)
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return None
    return str(v)


# -------------------------------------------------------------------
# 중위소득 조건
# -------------------------------------------------------------------

def _parse_median_income_condition(text: str) -> Optional[Tuple[Optional[float], Optional[float]]]:
    """
    requirements 텍스트에서 "중위소득 XX% 이하/이상/미만/초과" 패턴을 대략적으로 파싱.
    - "기준 중위소득의 80% 이하", "기준중위소득 100% 이하", "중위소득80%이하" 등 최대한 잡아낸다.
    - 반환: (min_ratio, max_ratio)
      예) "중위소득 120% 이하" → (None, 120.0)
          "중위소득 50% 이상"   → (50.0, None)
          "중위소득 50% 이상 120% 이하" → (50.0, 120.0)
    """
    import re

    if not text:
        return None

    # 1) 전처리: 기준/의/공백 변형들을 최대한 "중위소득 {숫자}" 형태로 정규화
    norm = text
    norm = norm.replace("기준 중위소득", "중위소득")
    norm = norm.replace("기준중위소득", "중위소득")
    norm = norm.replace("중위소득의", "중위소득 ")
    norm = norm.replace("중위소득기준", "중위소득 ")
    # 공백 여러 개 → 하나로
    norm = re.sub(r"\s+", " ", norm)

    # 2) 단일 조건 패턴: "중위소득 80% 이하/이상/미만/초과"
    single_pat = re.compile(r"중위소득\s*(\d+)\s*%?\s*(이하|이내|미만|이상|초과)")
    matches = single_pat.findall(norm)

    # 3) 범위 패턴: "중위소득 50~120%", "중위소득 50%~120%" 등
    range_pat = re.compile(r"중위소득\s*(\d+)\s*%?\s*[~\-]\s*(\d+)\s*%?")
    range_match = range_pat.search(norm)

    min_ratio: Optional[float] = None
    max_ratio: Optional[float] = None

    # 범위 패턴 먼저 적용
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        if low <= high:
            min_ratio, max_ratio = low, high
        else:
            min_ratio, max_ratio = high, low

    # 단일 조건들 추가 반영
    for num_str, op in matches:
        try:
            val = float(num_str)
        except Exception:
            continue

        if op in ("이하", "이내", "미만"):
            if max_ratio is None or val < max_ratio:
                max_ratio = val
        elif op in ("이상", "초과"):
            if min_ratio is None or val > min_ratio:
                min_ratio = val

    if min_ratio is None and max_ratio is None:
        return None

    # 디버깅용 출력
    print("[median_parser] Parsed cond:", (min_ratio, max_ratio), "from:", norm[:80])
    return (min_ratio, max_ratio)

def _extract_profile_numeric(profile: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    """
    profile[key]를 숫자로 뽑아온다.
    dict({"value":..., "confidence":...}) 형태도 처리.
    """
    if not profile:
        return None
    v = profile.get(key)
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _is_eligible_by_median_income(profile: Optional[Dict[str, Any]], doc: Dict[str, Any]) -> bool:
    """
    중위소득 기반 필터.

    - profile.median_income_ratio:
        * 1.2(=120%) 처럼 '배수'로 들어올 수도 있고
        * 120 처럼 '퍼센트 값'으로 들어올 수도 있다고 가정.
    - 문서 requirements/title에 명확한 "중위소득 XX% 이하/이상 ..." 조건이 있으면
      범위 밖이면 후보에서 제외.
    """
    raw = _extract_profile_numeric(profile, "median_income_ratio")
    if raw is None:
        return True  # 소득 정보 없으면 필터링 안 함

    # 🔧 단위 통일: 0~10 사이면 '배수'(1.2 등)로 보고 100을 곱해 퍼센트로 변환
    if raw <= 10:
        user_pct = raw * 100.0   # 1.2 → 120.0
    else:
        user_pct = raw           # 이미 %라고 가정 (예: 120)

    req_text = (doc.get("requirements") or "") + " " + (doc.get("title") or "")
    cond = _parse_median_income_condition(req_text)

    if not cond:
        print("[median_filter] NO_COND user_pct=", user_pct, "title=", doc.get("title"))
        return True

    min_r, max_r = cond  # 이 값들은 항상 '퍼센트 숫자'(예: 80, 100, 120)

    print("[median_filter] user_pct=", user_pct, "cond=", cond, "title=", doc.get("title"))

    # 예: "중위소득 50% 이상"인데 사용자는 40%
    if min_r is not None and user_pct < min_r:
        return False

    # 예: "중위소득 120% 이하"인데 사용자는 150%
    if max_r is not None and user_pct > max_r:
        return False

    return True

# -------------------------------------------------------------------
# 기초생활보장 / 차상위
# -------------------------------------------------------------------

def _is_eligible_by_basic_benefit(profile: Optional[Dict[str, Any]], doc: Dict[str, Any]) -> bool:
    """
    기초생활보장 / 차상위 관련 필터 (간단한 휴리스틱).
    - profile.basic_benefit_type: "생계", "의료", "주거", "교육", "기타" 등 (또는 None)
    - 차상위 여부는 profile에 별도 필드가 없을 수 있으니, 여기선 아주 보수적으로만 거름.
    """
    req_text = (doc.get("requirements") or "") + " " + (doc.get("title") or "")
    req_text = req_text.replace(" ", "")

    needs_basic = any(k in req_text for k in ["기초생활보장수급자", "생계급여수급자", "의료급여수급자"])
    needs_chasangwi = "차상위" in req_text

    if not needs_basic and not needs_chasangwi:
        return True

    user_basic = _extract_profile_str(profile, "basic_benefit_type")

    if user_basic is None:
        return True  # 정보 없으면 일단 통과 (너무 공격적으로 거르지 않음)

    # 영어 코드를 한글로 매핑
    BENEFIT_TYPE_MAP = {
        "LIVELIHOOD": "생계급여",
        "MEDICAL": "의료급여",
        "HOUSING": "주거급여",
        "EDUCATION": "교육급여",
        "NONE": "",
    }

    ub = user_basic.replace(" ", "")
    # 영어 코드면 한글로 변환
    if ub.upper() in BENEFIT_TYPE_MAP:
        ub = BENEFIT_TYPE_MAP[ub.upper()]

    if needs_basic and not any(x in ub for x in ["생계", "의료", "기초", "급여"]):
        return False

    if needs_chasangwi and "차상위" not in ub:
        return False

    return True


# -------------------------------------------------------------------
# 장애등급
# -------------------------------------------------------------------

def _is_eligible_by_disability(profile: Optional[Dict[str, Any]], doc: Dict[str, Any]) -> bool:
    """
    장애등급 기반 아주 간단한 필터.
    - "장애 1급~3급", "장애 1급 이상" 등 일부 패턴만 처리.
    """
    user_grade = _extract_profile_numeric(profile, "disability_grade")
    if user_grade is None:
        return True  # 정보 없으면 필터링 안 함

    req_text = (doc.get("requirements") or "") + " " + (doc.get("title") or "")
    req_text = req_text.replace(" ", "")

    range_pat = re.compile(r"장애(\d)급(?:이상)?[~\-](\d)급(?:이하)?")
    m = range_pat.search(req_text)
    min_g: Optional[float] = None
    max_g: Optional[float] = None
    if m:
        g1 = float(m.group(1))
        g2 = float(m.group(2))
        min_g, max_g = (min(g1, g2), max(g1, g2))
    else:
        single_pat = re.compile(r"장애(\d)급(이상|이하)")
        m2 = single_pat.search(req_text)
        if m2:
            g = float(m2.group(1))
            op = m2.group(2)
            if op == "이상":
                min_g = g
            elif op == "이하":
                max_g = g

    if min_g is None and max_g is None:
        return True

    if min_g is not None and user_grade < min_g:
        return False
    if max_g is not None and user_grade > max_g:
        return False
    return True


# -------------------------------------------------------------------
# 외부에서 쓰는 진입점
# -------------------------------------------------------------------

def filter_candidates_by_profile(
    snippets: List[Dict[str, Any]],
    profile: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    프로필 기반으로 RAG 후보(snippets)를 후처리 필터링.
    - 현재 구현:
      * 중위소득 조건
      * 기초생활보장/차상위 조건
      * 장애등급 조건
    """
    if not snippets or not profile:
        return snippets

    filtered: List[Dict[str, Any]] = []
    for doc in snippets:
        if not _is_eligible_by_median_income(profile, doc):
            continue
        if not _is_eligible_by_basic_benefit(profile, doc):
            continue
        if not _is_eligible_by_disability(profile, doc):
            continue
        filtered.append(doc)

    return filtered
