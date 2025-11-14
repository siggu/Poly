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

# merge utils
from app.langgraph.utils.merge_utils import merge_profile, merge_collection

# DB fetch utils
from app.dao.db_user_utils import fetch_profile_from_db, fetch_collections_from_db

# HuggingFace embedding
from sentence_transformers import SentenceTransformer

load_dotenv()


# -------------------------------------------------------------------
# DB URL
# -------------------------------------------------------------------
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
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

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

    return state


# -------------------------------------------------------------------
# Manual Test
# -------------------------------------------------------------------
def _now():
    return datetime.utcnow().isoformat()

if __name__ == "__main__":
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