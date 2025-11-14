# -*- coding: utf-8 -*-
# retrieval_planner.py
# -------------------------------------------------------------------
# 기능:
#   1) ephemeral + DB 프로필/컬렉션 merge
#   2) SentenceTransformer(bge-m3-ko) 임베딩
#   3) pgvector 기반 문서 검색
#   4) state["retrieval"]에 profile_ctx, collection_ctx, rag_snippets 저장
# -------------------------------------------------------------------

from __future__ import annotations
import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

import psycopg
from dotenv import load_dotenv
from langchain_community.embeddings import HuggingFaceEmbeddings

# merge utils
from app.langgraph.utils.merge_utils import merge_profile, merge_collection

# DB fetch utils
from app.dao.db_user_utils import fetch_profile_from_db, fetch_collections_from_db

# HuggingFace embedding
from sentence_transformers import SentenceTransformer

load_dotenv()

<<<<<<< HEAD

# -------------------------------------------------------------------
# DB URL
# -------------------------------------------------------------------
=======
EMBED_MODEL_NAME = "dragonkue/bge-m3-ko"
EMBED_DEVICE = "cpu"
_embedding_model: Optional[HuggingFaceEmbeddings] = None


def _get_embedding_model() -> HuggingFaceEmbeddings:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = HuggingFaceEmbeddings(
            model_name=EMBED_MODEL_NAME,
            model_kwargs={"device": EMBED_DEVICE},
        )
    return _embedding_model


def _embed_query_vector(text: str) -> str:
    """쿼리 텍스트를 PGVector에서 사용하는 문자열 표현의 벡터로 변환"""
    model = _get_embedding_model()
    vector = model.embed_query(text or "")
    return "[" + ",".join(f"{v:.6f}" for v in vector) + "]"

>>>>>>> 36c79b771493dfe171d3d5b7342a9693d8251afa
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not configured")

if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)


# -------------------------------------------------------------------
# HuggingFace Embedding Model
# -------------------------------------------------------------------
_embedding_model = SentenceTransformer("dragonkue/bge-m3-ko")

def _embed_text(text: str) -> List[float]:
    """
    SentenceTransformer encode → list[float]
    """
    return _embedding_model.encode(
        text,
        normalize_embeddings=True
    ).tolist()


# -------------------------------------------------------------------
# DB Connection
# -------------------------------------------------------------------
def _get_conn():
    return psycopg.connect(DB_URL)


# -------------------------------------------------------------------
# Keyword Extraction
# -------------------------------------------------------------------
def extract_keywords(text: str, max_k: int = 8) -> List[str]:
    if not text:
        return []
    tokens = re.findall(r"[가-힣A-Za-z0-9]+", text)
    stop = {"그리고","하지만","근데","가능","문의","신청","여부","있나요","해당"}
    out, seen = [], set()
    for t in tokens:
        t = t.lower()
        if len(t) >= 2 and t not in stop:
            if t not in seen:
                seen.add(t)
                out.append(t)
                if len(out) >= max_k:
                    break
    return out


# -------------------------------------------------------------------
# Hybrid Document Search (Vector only)
# -------------------------------------------------------------------
def _sanitize_region(region_value: Optional[Any]) -> Optional[str]:
    """
    residency_sgg_code 문자열을 정리.
    dict 형태({'value': '강남구'})도 지원.
    """
    if region_value is None:
        return None

    if isinstance(region_value, dict):
        region_value = region_value.get("value")

    if region_value is None:
        return None
<<<<<<< HEAD

    region_str = str(region_value).strip()
    return region_str or None


def _hybrid_search_documents(
    query_text: str,
    merged_profile: Optional[Dict[str, Any]],
    top_k: int = 8,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    오직 query_text 임베딩 기반 pgvector 검색.
    (collection predicate/object 필터 없음)
    """

    keywords = extract_keywords(query_text, max_k=8)

    # region filter
    region_filter = None
    if merged_profile:
        region_filter = _sanitize_region(
            merged_profile.get("residency_sgg_code")
        )
        if region_filter is None:
            print("[retrieval_planner] region_filter empty or missing")

    # embedding
    try:
        qvec = _embed_text(query_text)
    except Exception:
        qvec = None

    if qvec is None:
        return [], keywords

    qvec_str = str(qvec)

    # SQL
    sql = """
        SELECT
            d.id,
            d.title,
            d.requirements,
            d.benefits,
            d.region,
            d.url,
            1 - (e.embedding <=> %(qvec)s::vector) AS similarity
        FROM documents d
        JOIN embeddings e ON d.id = e.doc_id
    """

    params = {"qvec": qvec_str}

    if region_filter:
        sql += " WHERE TRIM(d.region) = %(region)s::text"
        params["region"] = region_filter

    sql += """
        ORDER BY e.embedding <=> %(qvec)s::vector
        LIMIT %(limit)s
    """
    params["limit"] = top_k

    # execute
    rows = []
    with _get_conn() as conn:
=======
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
            (pid, uid, birth_date, sex, sgg, ins, mir, basic, disab, ltci, preg, updated_at) = row
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
                "profile_id": str(pid) if pid is not None else None,
                "user_id": str(uid) if uid is not None else None,
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
# === 컬렉션 SQL: triples → collections, profile_id 사용 ===
COLLECTION_BY_KEYWORDS_SQL = """
SELECT id, profile_id, subject, predicate, object, code_system, code,
       onset_date, end_date, negation, confidence, source_id, created_at
FROM collections
WHERE profile_id = %(profile_id)s
  AND (
        object ILIKE ANY(%(patterns)s)
        OR predicate = ANY(%(preds)s)
        OR COALESCE(code,'') ILIKE ANY(%(patterns)s)
      )
ORDER BY created_at DESC
LIMIT %(limit)s;
"""

COLLECTION_RECENT_SQL = """
SELECT id, profile_id, subject, predicate, object, code_system, code,
       onset_date, end_date, negation, confidence, source_id, created_at
FROM collections
WHERE profile_id = %(profile_id)s
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

SIMILAR_SEARCH_SQL = """
    SELECT
        d.id,
        d.title,
        d.requirements,
        d.benefits,
        d.region,
        d.url,
        1 - (e.embedding <=> %(query_embedding)s::vector) AS similarity
    FROM documents d
    JOIN embeddings e ON d.id = e.doc_id
    WHERE (
        %(residency_sgg_code)s::text IS NULL
        OR d.region = %(residency_sgg_code)s::text
    )
    ORDER BY e.embedding <=> %(query_embedding)s::vector
    LIMIT %(limit)s;
"""


def search_similar_documents(
    query_text: str,
    *,
    residency_sgg_code: Optional[str] = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    임베딩 기반으로 documents 테이블에서 유사한 항목을 검색한다.

    Args:
        query_text: 검색할 자연어 쿼리
        residency_sgg_code: 사용자 거주지 코드(없으면 전체 대상)
        limit: 반환할 최대 문서 수
    """
    query_text = (query_text or "").strip()
    if not query_text or limit <= 0:
        return []

    embedding_str = _embed_query_vector(query_text)
    params = {
        "query_embedding": embedding_str,
        "residency_sgg_code": residency_sgg_code,
        "limit": limit,
    }

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(SIMILAR_SEARCH_SQL, params)
            rows = cur.fetchall()

    docs: List[Dict[str, Any]] = []
    for doc_id, title, requirements, benefits, region, url, similarity in rows:
        docs.append(
            {
                "id": doc_id,
                "title": title,
                "requirements": requirements,
                "benefits": benefits,
                "region": region,
                "url": url,
                "similarity": float(similarity) if similarity is not None else None,
            }
        )

    return docs





# === fetch_collection_context(): profile_id 해석 후 사용 ===
def fetch_collection_context(user_id: str, query_text: Optional[str], limit: int = 12) -> List[Dict[str, Any]]:
    # 먼저 최신 profile_id 확보
    with psycopg.connect(DB_URL) as conn:
>>>>>>> 36c79b771493dfe171d3d5b7342a9693d8251afa
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

<<<<<<< HEAD
    # convert
    results = []
    for r in rows:
        results.append(
            {
                "doc_id": r[0],
                "title": r[1],
                "requirements": r[2],
                "benefits": r[3],
                "region": r[4],
                "url": r[5],
                "similarity": float(r[6]),
            }
        )
=======
    out = []
    for (tid, prof_id, subj, pred, obj, cs, code, onset, end, neg, conf, src, cat) in rows:
        out.append({
            "id": str(tid) if tid is not None else None,
            "profile_id": str(prof_id) if prof_id is not None else None,
            "subject": subj,
            "predicate": pred,
            "object": obj,
            "code_system": cs,
            "code": code,
            "onset_date": onset.isoformat() if isinstance(onset, date) else onset,
            "end_date": end.isoformat() if isinstance(end, date) else end,
            "negation": bool(neg) if neg is not None else False,
            "confidence": float(conf) if conf is not None else None,
            "source_id": str(src) if src is not None else None,
            "created_at": cat.isoformat() if isinstance(cat, datetime) else str(cat),
        })
    return out
>>>>>>> 36c79b771493dfe171d3d5b7342a9693d8251afa

    results.sort(key=lambda x: x["similarity"], reverse=True)

    # rag_snippets
    snippets = []
    for r in results:
        snippets.append(
            {
                "doc_id": r["doc_id"],
                "title": r["title"],
                "requirements": r["requirements"],
                "benefits": r["benefits"],
                "region": r["region"],
                "url": r["url"],
                "score": r["similarity"],
            }
        )

    return snippets, keywords


# -------------------------------------------------------------------
# Required RAG Decider
# -------------------------------------------------------------------
def _decide_required_rag(router: Optional[Dict[str, Any]], text: str) -> str:
    if router and router.get("required_rag") in {"PROFILE","COLLECTION","BOTH","NONE"}:
        return router["required_rag"]

    text = text.lower()

    if any(k in text for k in ["자격","지원","혜택","대상","가능","요건"]):
        return "BOTH"

    if any(k in text for k in ["암","진단","항암","투석","임신"]):
        return "COLLECTION"

    return "NONE"


# -------------------------------------------------------------------
# LangGraph State
# -------------------------------------------------------------------
class State(dict):
    pass


# -------------------------------------------------------------------
# Retrieval Planner Node
# -------------------------------------------------------------------
def retrieval_planner_node(state: State) -> State:
<<<<<<< HEAD
    user_id = state.get("user_id")
    query_text = state.get("input_text") or ""
    router_info = state.get("router", {})

    required = _decide_required_rag(router_info, query_text)

    # --- ephemeral ---
    eph_profile = state.get("ephemeral_profile")
    eph_collection = state.get("ephemeral_collection")

    # --- DB ---
    db_profile = fetch_profile_from_db(user_id)
    db_collection = fetch_collections_from_db(user_id)

    # --- merge ---
    merged_profile = merge_profile(db_profile, eph_profile)
    merged_collection = merge_collection(db_collection, eph_collection)

    # --- document search ---
    rag_docs, keywords = _hybrid_search_documents(
        query_text=query_text,
        merged_profile=merged_profile,
        top_k=8,
    )

    # --- store ---
    state["retrieval"] = {
        "used": required,
        "profile_ctx": merged_profile,
        "collection_ctx": merged_collection,
        "rag_snippets": rag_docs,
        "keywords": keywords,
    }

=======
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

    # 문서 유사도 검색 (사용자 질문 기반)
    if required != "NONE" and input_text:
        residency_sgg_code = None
        profile_ctx = ret.get("profile_ctx")
        if isinstance(profile_ctx, dict) and "error" not in profile_ctx:
            residency_sgg_code = profile_ctx.get("residency_sgg_code")

        try:
            doc_ctx = search_similar_documents(
                input_text,
                residency_sgg_code=residency_sgg_code,
                limit=5,
            )
        except Exception as e:
            doc_ctx = [
                {
                    "error": f"document_search_failed: {type(e).__name__}",
                    "detail": str(e),
                }
            ]

        if doc_ctx:
            ret["document_ctx"] = doc_ctx

    state["retrieval"] = ret
>>>>>>> 36c79b771493dfe171d3d5b7342a9693d8251afa
    return state


# -------------------------------------------------------------------
# Manual Test
# -------------------------------------------------------------------
def _now():
    return datetime.utcnow().isoformat()

if __name__ == "__main__":
<<<<<<< HEAD
    dummy_state: State = {
        "session_id": "sess-test-1",
        "input_text": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
        "router": {"required_rag": "BOTH"},
        "profile_id": 1,  # 실제 DB에 존재하는 profiles.id로 테스트 권장
        "ephemeral_profile": {
            "residency_sgg_code": {"value": "강남구", "confidence": 0.95},
            "pregnant_or_postpartum12m": {"value": True, "confidence": 0.95},
        },

        "ephemeral_collection": {
            "triples": [
                {
                    "subject": "self",
                    "predicate": "HAS_CONDITION",
                    "object": "유방암",
                    "code_system": "KCD7",
                    "code": "C50.9",
                }
            ]
=======
    # 가벼운 수동 테스트
    test_states: List[State] = [
        {
            "user_id": os.getenv("TEST_USER_ID","fbf9f169-7d52-486e-86ca-43310752008d"),
            "input_text": "재난적의료비 대상인가요? 저는 의료급여2종이에요.",
            "router": {"required_rag": "BOTH"},
        },
        {
            "user_id": os.getenv("TEST_USER_ID","fbf9f169-7d52-486e-86ca-43310752008d"),
            "input_text": "6월에 유방암 C50.9 진단, 항암 치료 중입니다.",
            "router": {"required_rag": "COLLECTION"},
        },
        {
            "user_id": os.getenv("TEST_USER_ID","fbf9f169-7d52-486e-86ca-43310752008d"),
            "input_text": "안녕하세요",
            "router": {"required_rag": "NONE"},
>>>>>>> 36c79b771493dfe171d3d5b7342a9693d8251afa
        },

        "messages": [
            {
                "role": "user",
                "content": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
                "created_at": _now(),
                "meta": {},
            }
        ],
    }

    out = retrieval_planner_node(dummy_state)
    print(json.dumps(out["retrieval"], ensure_ascii=False, indent=2, default=str))  