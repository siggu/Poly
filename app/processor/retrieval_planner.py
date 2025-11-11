# retrieval_planner.py
# 목적: "Retrieval Planner" 노드
# - Router 결정(required_rag)에 따라 PROFILE/ COLLECTION/ BOTH/ NONE 중 무엇을 조회할지 결정
# - PROFILE: core_profile에서 1레코드 조회 → 요약 컨텍스트 구성
# - COLLECTION: triples에서 키워드 기반(간단) 검색 → 컨텍스트 구성
# - 결과는 state["retrieval"]에 {used, profile_ctx?, collection_ctx?} 형태로 저장
#
# 특징:
# - LLM 비사용(규칙 기반). 비용/지연 최소화
# - 키워드 검색은 간단한 ILIKE ANY 패턴(벡터스토어 없을 때용). 없으면 최신 N개
# - 삼중 컨텍스트는 (id, predicate, object, code_system, code, onset/end, negation, confidence, created_at)로 요약
#
# 의존:
#   pip install psycopg python-dotenv
#
# 환경:
#   DATABASE_URL=postgresql://user:pass@host:5432/dbname

from __future__ import annotations

import os
import re
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, TypedDict

import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (e.g. postgresql://user:pass@localhost:5432/db)")
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

# ─────────────────────────────────────────────────────────────────────────────
# 간단 키워드 추출(LLM 비사용). 한글/영문/숫자 토큰 중 길이 2+ 만 사용
# ─────────────────────────────────────────────────────────────────────────────
def extract_keywords(text: str, max_k: int = 6) -> List[str]:
    if not text:
        return []
    # 한글/영문/숫자 토큰
    toks = re.findall(r"[가-힣A-Za-z0-9]+", text)
    # 너무 흔한 단어 제거(매우 간단한 stoplist)
    stop = {"그리고","하지만","그리고요","근데","가능한가요","신청","문의","가능","여부","해당","있는","없는","있나요","인가요","혹시"}
    # 정규화
    norm = []
    for t in toks:
        tt = t.lower()
        if (len(tt) >= 2) and (tt not in stop):
            norm.append(tt)
    # 중복 제거 보존
    seen = set()
    out = []
    for w in norm:
        if w not in seen:
            seen.add(w)
            out.append(w)
        if len(out) >= max_k:
            break
    return out

# ─────────────────────────────────────────────────────────────────────────────
# DB 조회: PROFILE
# ─────────────────────────────────────────────────────────────────────────────
PROFILE_SQL = """
SELECT user_id, birth_date, sex, residency_sgg_code, insurance_type,
       median_income_ratio, basic_benefit_type, disability_grade,
       ltci_grade, pregnant_or_postpartum12m, updated_at
FROM core_profile
WHERE user_id = %(user_id)s
LIMIT 1;
"""

def _calc_age(birth_date: Optional[date]) -> Optional[int]:
    if not birth_date:
        return None
    today = date.today()
    y = today.year - birth_date.year
    # 생일 지났는지?
    if (today.month, today.day) < (birth_date.month, birth_date.day):
        y -= 1
    return y

def fetch_profile_context(user_id: str) -> Optional[Dict[str, Any]]:
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(PROFILE_SQL, {"user_id": user_id})
            row = cur.fetchone()
            if not row:
                return None
            (uid, birth_date, sex, sgg, ins, mir, basic, disab, ltci, preg, updated_at) = row
            age = _calc_age(birth_date)
            # 요약 텍스트(룰 엔진 또는 Answer LLM 힌트로 사용)
            summary = []
            if age is not None:
                summary.append(f"연령 {age}세")
            if ins:
                summary.append(f"건보자격 {ins}")
            if mir is not None:
                summary.append(f"중위소득 {float(mir):.1f}%")
            if basic:
                summary.append(f"기초생활보장 {basic}")
            if disab is not None:
                dg = {0:"미등록",1:"심한",2:"심하지않음"}.get(disab, str(disab))
                summary.append(f"장애등급 {dg}")
            if ltci and ltci != "NONE":
                summary.append(f"장기요양 {ltci}")
            if preg is True:
                summary.append("임신/출산 12개월 이내")
            profile_ctx = {
                "user_id": uid,
                "age": age,
                "birth_date": birth_date.isoformat() if birth_date else None,
                "sex": sex,
                "residency_sgg_code": sgg,
                "insurance_type": ins,
                "median_income_ratio": float(mir) if mir is not None else None,
                "basic_benefit_type": basic,
                "disability_grade": disab,
                "ltci_grade": ltci,
                "pregnant_or_postpartum12m": bool(preg) if preg is not None else None,
                "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at),
                "summary": " / ".join(summary) if summary else None,
            }
            return profile_ctx

# ─────────────────────────────────────────────────────────────────────────────
# DB 조회: COLLECTION (키워드 기반 ILIKE ANY)
# ─────────────────────────────────────────────────────────────────────────────
COLLECTION_BY_KEYWORDS_SQL = """
SELECT id, user_id, subject, predicate, object, code_system, code,
       onset_date, end_date, negation, confidence, source_id, created_at
FROM triples
WHERE user_id = %(user_id)s
  AND (
        object ILIKE ANY(%(patterns)s)
        OR predicate = ANY(%(preds)s)
        OR COALESCE(code,'') ILIKE ANY(%(patterns)s)
      )
ORDER BY created_at DESC
LIMIT %(limit)s;
"""

COLLECTION_RECENT_SQL = """
SELECT id, user_id, subject, predicate, object, code_system, code,
       onset_date, end_date, negation, confidence, source_id, created_at
FROM triples
WHERE user_id = %(user_id)s
ORDER BY created_at DESC
LIMIT %(limit)s;
"""

# 자주 쓰는 술어 키워드 매핑(간단)
PRED_KEYWORDS = {
    "암": "HAS_CONDITION",
    "유방암": "HAS_CONDITION",
    "치료": "UNDER_TREATMENT",
    "항암": "UNDER_TREATMENT",
    "투석": "UNDER_TREATMENT",
    "산정특례": "HAS_SANJEONGTEUKRYE",
    "임신": "PREGNANCY_STATUS",
    "난임": "HAS_INFERTILITY",
    "문서": "HAS_DOCUMENT",
    "영수증": "HAS_DOCUMENT",
    "증빙": "HAS_DOCUMENT",
    "재난": "FINANCIAL_SHOCK",
    "실직": "FINANCIAL_SHOCK",
}

def fetch_collection_context(user_id: str, query_text: Optional[str], limit: int = 12) -> List[Dict[str, Any]]:
    keywords = extract_keywords(query_text or "", max_k=6)
    # ILIKE 패턴 배열 만들기
    patterns = [f"%{kw}%" for kw in keywords] if keywords else []
    # 키워드에서 술어 힌트 뽑기
    preds = []
    for kw in keywords:
        pred = PRED_KEYWORDS.get(kw)  # 한글 키워드 우선
        if not pred:
            # 영문/코드성 키워드 간단 휴리스틱
            if re.match(r"^[A-Z]\d{2}(\.\d+)?$", kw.upper()):  # 예: C50.9
                pred = "HAS_CONDITION"
        if pred and pred not in preds:
            preds.append(pred)

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            if patterns or preds:
                cur.execute(
                    COLLECTION_BY_KEYWORDS_SQL,
                    {"user_id": user_id, "patterns": patterns or ["%%"], "preds": preds or [], "limit": limit},
                )
            else:
                cur.execute(COLLECTION_RECENT_SQL, {"user_id": user_id, "limit": limit})
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for (tid, uid, subj, pred, obj, cs, code, onset, end, neg, conf, src, cat) in rows:
        out.append({
            "id": tid,
            "user_id": uid,
            "subject": subj,
            "predicate": pred,
            "object": obj,
            "code_system": cs,
            "code": code,
            "onset_date": onset.isoformat() if isinstance(onset, date) else onset,
            "end_date": end.isoformat() if isinstance(end, date) else end,
            "negation": bool(neg) if neg is not None else False,
            "confidence": float(conf) if conf is not None else None,
            "source_id": src,
            "created_at": cat.isoformat() if isinstance(cat, datetime) else str(cat),
        })
    return out

# ─────────────────────────────────────────────────────────────────────────────
# LangGraph 상태 & 노드
# ─────────────────────────────────────────────────────────────────────────────
class State(TypedDict, total=False):
    user_id: str
    input_text: str
    router: Dict[str, Any]          # {"required_rag": "..."}
    retrieval: Dict[str, Any]       # {"used": "...", "profile_ctx": {...}, "collection_ctx": [...]}

def _decide_required_rag(router: Optional[Dict[str, Any]], input_text: str) -> str:
    # 1순위: 라우터 결정
    if router and isinstance(router, dict):
        val = router.get("required_rag")
        if val in {"PROFILE","COLLECTION","BOTH","NONE"}:
            return val
    # 2순위: 간단 휴리스틱
    text = (input_text or "").lower()
    # 자격/지원/혜택/대상 등 키워드 → BOTH
    if any(k in text for k in ["자격","지원","혜택","대상","되나요","가능","요건","조건"]):
        return "BOTH"
    # 의료/치료/진단 언급 → COLLECTION
    if any(k in text for k in ["진단","치료","항암","투석","암","산정특례","임신","난임","문서","영수증"]):
        return "COLLECTION"
    return "NONE"

def retrieval_planner_node(state: State) -> State:
    """
    입력: state["router"]["required_rag"] (없으면 휴리스틱)
    동작:
      - PROFILE: core_profile 1건 조회 → profile_ctx
      - COLLECTION: triples 키워드 검색 → collection_ctx
      - BOTH: 둘 다 병렬(순차) 조회 → fusion은 Answer에서 처리
      - NONE: 조회 생략
    출력:
      state["retrieval"] = {
         "used": "PROFILE"|"COLLECTION"|"BOTH"|"NONE",
         "profile_ctx": {...}?,
         "collection_ctx": [...]?
      }
    """
    user_id = state.get("user_id") or ""
    input_text = state.get("input_text") or ""
    required = _decide_required_rag(state.get("router"), input_text)

    ret: Dict[str, Any] = {"used": required}

    if required in {"PROFILE","BOTH"} and user_id:
        try:
            pctx = fetch_profile_context(user_id)
        except Exception as e:
            pctx = {"error": f"profile_fetch_failed: {type(e).__name__}", "detail": str(e)}
        ret["profile_ctx"] = pctx

    if required in {"COLLECTION","BOTH"} and user_id:
        try:
            cctx = fetch_collection_context(user_id, input_text, limit=12)
        except Exception as e:
            cctx = [{"error": f"collection_fetch_failed: {type(e).__name__}", "detail": str(e)}]
        ret["collection_ctx"] = cctx

    state["retrieval"] = ret
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 테스트
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 가벼운 수동 테스트
    test_states: List[State] = [
        {
            "user_id": os.getenv("TEST_USER_ID","u_demo_1"),
            "input_text": "재난적의료비 대상인가요? 저는 의료급여2종이에요.",
            "router": {"required_rag": "BOTH"},
        },
        {
            "user_id": os.getenv("TEST_USER_ID","u_demo_1"),
            "input_text": "6월에 유방암 C50.9 진단, 항암 치료 중입니다.",
            "router": {"required_rag": "COLLECTION"},
        },
        {
            "user_id": os.getenv("TEST_USER_ID","u_demo_1"),
            "input_text": "안녕하세요",
            "router": {"required_rag": "NONE"},
        },
    ]

    for s in test_states:
        out = retrieval_planner_node(s)
        print(json.dumps(out.get("retrieval", {}), ensure_ascii=False, indent=2))
        print("-" * 80)
